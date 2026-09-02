"""Full-pipeline tests wiring a real Sender to a real Relay through two
linked FakeClient pairs.

Every other test file exercises the sender or the relay in isolation with
directly injected ACKs. These tests instead let genuine MQTT-shaped
callback traffic flow between two live components on real background
threads, so a regression in how the two halves are meant to interoperate
(topic names, envelope shape, message_id correlation, retry-on-outage) would
surface here even if each component's own unit tests still passed.
"""

from __future__ import annotations

import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fakes import FakeClient, client_factory_for

from reliomq.config import RelayConfig, SenderConfig
from reliomq.protocol import DeliveryEnvelope
from reliomq.relay import Relay
from reliomq.sender import Sender


RELAY_TOPIC = "reliable/pipeline/input"
DELIVERY_ACK_TOPIC = "reliable/pipeline/ack"


class _LinkedBrokerPair:
    """Relay publish() calls between two FakeClient pairs, like a real broker
    delivering to each side's live subscription."""

    def __init__(
        self,
        sender_client: FakeClient,
        relay_source_client: FakeClient,
    ) -> None:
        self.sender_client = sender_client
        self.relay_source_client = relay_source_client
        sender_client.publish_hook = self._relay_source_publish
        relay_source_client.publish_hook = self._relay_source_ack

    def _relay_source_publish(self, call: dict) -> None:
        if call["topic"] != RELAY_TOPIC:
            return
        self.relay_source_client.emit_message(RELAY_TOPIC, call["payload"])

    def _relay_source_ack(self, call: dict) -> None:
        if call["topic"] != DELIVERY_ACK_TOPIC:
            return
        self.sender_client.emit_message(DELIVERY_ACK_TOPIC, call["payload"])


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.outbox_path = Path(self.temporary_directory.name) / "pending.jsonl"

        self.sender_client = FakeClient()
        self.relay_source_client = FakeClient()
        self.relay_dest_client = FakeClient()
        self._link = _LinkedBrokerPair(self.sender_client, self.relay_source_client)

        self.sender = Sender(
            SenderConfig(
                host="source-broker",
                outbox_path=self.outbox_path,
                relay_topic=RELAY_TOPIC,
                delivery_ack_topic=DELIVERY_ACK_TOPIC,
                delivery_ack_timeout=0.05,
                mqtt_puback_timeout=0.05,
                retry_interval=0.03,
            ),
            client_factory=client_factory_for(self.sender_client),
        )
        self.relay = Relay(
            RelayConfig(
                source_host="source-broker",
                destination_host="destination-broker",
                relay_topic=RELAY_TOPIC,
                delivery_ack_topic=DELIVERY_ACK_TOPIC,
                destination_publish_timeout=0.05,
                source_ack_publish_timeout=0.05,
                retry_interval=0.03,
            ),
            source_client_factory=client_factory_for(self.relay_source_client),
            destination_client_factory=client_factory_for(self.relay_dest_client),
        )
        self.addCleanup(self.sender.stop)
        self.addCleanup(self.relay.stop)

    def bring_up_sender(self) -> None:
        self.sender.start()
        self.sender_client.emit_connect()
        self.sender_client.emit_latest_suback((1,))

    def bring_up_relay(self, *, destination_connected: bool) -> None:
        self.relay.start()
        self.relay_source_client.emit_connect()
        self.relay_source_client.emit_latest_suback((1,))
        if destination_connected:
            self.relay_dest_client.emit_connect()

    def test_publish_flows_end_to_end_and_clears_the_durable_store(self) -> None:
        self.bring_up_sender()
        self.bring_up_relay(destination_connected=True)

        message_id = self.sender.publish(
            "factory/machine1/data",
            {"temperature": 25.2, "pressure": 4.1},
            message_id="pipeline-happy-path",
        )

        self.assertTrue(self.sender.wait_for_delivery(message_id, timeout=5.0))
        self.assertEqual(self.sender.pending_count(), 0)

        self.assertEqual(len(self.relay_dest_client.publish_calls), 1)
        destination_call = self.relay_dest_client.publish_calls[0]
        self.assertEqual(destination_call["topic"], "factory/machine1/data")
        self.assertEqual(destination_call["qos"], 1)
        delivered = DeliveryEnvelope.from_bytes(destination_call["payload"])
        self.assertEqual(delivered.message_id, message_id)
        self.assertEqual(delivered.payload, {"temperature": 25.2, "pressure": 4.1})

    def test_destination_outage_retries_and_recovers_without_losing_the_message(
        self,
    ) -> None:
        self.bring_up_sender()
        # The destination starts disconnected: the relay must forward no
        # DeliveryAck, and the sender must keep the durable record and retry.
        self.bring_up_relay(destination_connected=False)

        message_id = self.sender.publish(
            "factory/machine1/data",
            {"temperature": 99.9},
            message_id="pipeline-outage",
        )

        # Give the retry loop a few cycles to prove the message survives an
        # outage instead of disappearing after one failed attempt.
        time.sleep(0.2)
        self.assertEqual(self.sender.pending_count(), 1)
        self.assertEqual(self.relay_dest_client.publish_calls, [])

        def reconnect_destination_soon() -> None:
            time.sleep(0.1)
            self.relay_dest_client.emit_connect()

        threading.Thread(target=reconnect_destination_soon, daemon=True).start()

        self.assertTrue(self.sender.wait_for_delivery(message_id, timeout=5.0))
        self.assertEqual(self.sender.pending_count(), 0)
        self.assertEqual(len(self.relay_dest_client.publish_calls), 1)
        delivered = DeliveryEnvelope.from_bytes(
            self.relay_dest_client.publish_calls[0]["payload"]
        )
        self.assertEqual(delivered.message_id, message_id)

    def test_two_messages_are_delivered_in_fifo_order_through_the_relay(self) -> None:
        self.bring_up_sender()
        self.bring_up_relay(destination_connected=True)

        first = self.sender.publish("factory/a", 1, message_id="pipeline-fifo-1")
        second = self.sender.publish("factory/b", 2, message_id="pipeline-fifo-2")

        self.assertTrue(self.sender.wait_for_delivery(timeout=5.0))
        self.assertEqual(self.sender.pending_count(), 0)

        delivered_ids = [
            DeliveryEnvelope.from_bytes(call["payload"]).message_id
            for call in self.relay_dest_client.publish_calls
        ]
        self.assertEqual(delivered_ids, [first, second])


if __name__ == "__main__":
    unittest.main()

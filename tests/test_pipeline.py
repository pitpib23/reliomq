"""Full-pipeline tests wiring a real ReliablePublisher to a real
ReliableMqttBridge through two linked FakeClient pairs.

Every other test file exercises the publisher or the bridge in isolation
with directly injected ACKs. These tests instead let genuine MQTT-shaped
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

from reliomq.bridge import ReliableMqttBridge
from reliomq.config import BridgeConfig, PublisherConfig
from reliomq.protocol import DeliveryEnvelope
from reliomq.publisher import ReliablePublisher


ENVELOPE_TOPIC = "reliable/pipeline/input"
ACK_TOPIC = "reliable/pipeline/ack"


class _LinkedBrokerPair:
    """Relay publish() calls between two FakeClient pairs, like a real broker
    delivering to each side's live subscription."""

    def __init__(
        self,
        publisher_client: FakeClient,
        bridge_source_client: FakeClient,
    ) -> None:
        self.publisher_client = publisher_client
        self.bridge_source_client = bridge_source_client
        publisher_client.publish_hook = self._relay_source_publish
        bridge_source_client.publish_hook = self._relay_source_ack

    def _relay_source_publish(self, call: dict) -> None:
        if call["topic"] != ENVELOPE_TOPIC:
            return
        self.bridge_source_client.emit_message(ENVELOPE_TOPIC, call["payload"])

    def _relay_source_ack(self, call: dict) -> None:
        if call["topic"] != ACK_TOPIC:
            return
        self.publisher_client.emit_message(ACK_TOPIC, call["payload"])


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.queue_path = Path(self.temporary_directory.name) / "pending.jsonl"

        self.publisher_client = FakeClient()
        self.bridge_source_client = FakeClient()
        self.bridge_dest_client = FakeClient()
        self._link = _LinkedBrokerPair(
            self.publisher_client, self.bridge_source_client
        )

        self.publisher = ReliablePublisher(
            PublisherConfig(
                host="source-broker",
                queue_path=self.queue_path,
                envelope_topic=ENVELOPE_TOPIC,
                ack_topic=ACK_TOPIC,
                ack_timeout=0.05,
                publish_timeout=0.05,
                retry_interval=0.03,
            ),
            client_factory=client_factory_for(self.publisher_client),
        )
        self.bridge = ReliableMqttBridge(
            BridgeConfig(
                source_host="source-broker",
                destination_host="destination-broker",
                envelope_topic=ENVELOPE_TOPIC,
                ack_topic=ACK_TOPIC,
                destination_publish_timeout=0.05,
                source_ack_publish_timeout=0.05,
                retry_interval=0.03,
            ),
            source_client_factory=client_factory_for(self.bridge_source_client),
            destination_client_factory=client_factory_for(self.bridge_dest_client),
        )
        self.addCleanup(self.publisher.stop)
        self.addCleanup(self.bridge.stop)

    def bring_up_publisher(self) -> None:
        self.publisher.start()
        self.publisher_client.emit_connect()
        self.publisher_client.emit_latest_suback((1,))

    def bring_up_bridge(self, *, destination_connected: bool) -> None:
        self.bridge.start()
        self.bridge_source_client.emit_connect()
        self.bridge_source_client.emit_latest_suback((1,))
        if destination_connected:
            self.bridge_dest_client.emit_connect()

    def test_publish_flows_end_to_end_and_clears_the_durable_store(self) -> None:
        self.bring_up_publisher()
        self.bring_up_bridge(destination_connected=True)

        message_id = self.publisher.publish(
            topic="factory/machine1/data",
            payload={"temperature": 25.2, "pressure": 4.1},
            message_id="pipeline-happy-path",
        )

        self.assertTrue(self.publisher.wait_for_delivery(message_id, timeout=5.0))
        self.assertEqual(self.publisher.pending_count(), 0)

        self.assertEqual(len(self.bridge_dest_client.publish_calls), 1)
        destination_call = self.bridge_dest_client.publish_calls[0]
        self.assertEqual(destination_call["topic"], "factory/machine1/data")
        self.assertEqual(destination_call["qos"], 1)
        delivered = DeliveryEnvelope.from_bytes(destination_call["payload"])
        self.assertEqual(delivered.message_id, message_id)
        self.assertEqual(delivered.payload, {"temperature": 25.2, "pressure": 4.1})

    def test_destination_outage_retries_and_recovers_without_losing_the_message(
        self,
    ) -> None:
        self.bring_up_publisher()
        # The destination starts disconnected: the bridge must forward no
        # ACK, and the publisher must keep the durable record and retry.
        self.bring_up_bridge(destination_connected=False)

        message_id = self.publisher.publish(
            topic="factory/machine1/data",
            payload={"temperature": 99.9},
            message_id="pipeline-outage",
        )

        # Give the retry loop a few cycles to prove the message survives an
        # outage instead of disappearing after one failed attempt.
        time.sleep(0.2)
        self.assertEqual(self.publisher.pending_count(), 1)
        self.assertEqual(self.bridge_dest_client.publish_calls, [])

        def reconnect_destination_soon() -> None:
            time.sleep(0.1)
            self.bridge_dest_client.emit_connect()

        threading.Thread(target=reconnect_destination_soon, daemon=True).start()

        self.assertTrue(self.publisher.wait_for_delivery(message_id, timeout=5.0))
        self.assertEqual(self.publisher.pending_count(), 0)
        self.assertEqual(len(self.bridge_dest_client.publish_calls), 1)
        delivered = DeliveryEnvelope.from_bytes(
            self.bridge_dest_client.publish_calls[0]["payload"]
        )
        self.assertEqual(delivered.message_id, message_id)

    def test_two_messages_are_delivered_in_fifo_order_through_the_bridge(self) -> None:
        self.bring_up_publisher()
        self.bring_up_bridge(destination_connected=True)

        first = self.publisher.publish("factory/a", 1, message_id="pipeline-fifo-1")
        second = self.publisher.publish("factory/b", 2, message_id="pipeline-fifo-2")

        self.assertTrue(self.publisher.wait_for_delivery(timeout=5.0))
        self.assertEqual(self.publisher.pending_count(), 0)

        delivered_ids = [
            DeliveryEnvelope.from_bytes(call["payload"]).message_id
            for call in self.bridge_dest_client.publish_calls
        ]
        self.assertEqual(delivered_ids, [first, second])


if __name__ == "__main__":
    unittest.main()

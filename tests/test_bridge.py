from __future__ import annotations

import queue
import unittest

from fakes import FakeClient, FakePublishInfo, client_factory_for
from reliomq.bridge import ReliableMqttBridge
from reliomq.config import BridgeConfig
from reliomq.protocol import Ack, DeliveryEnvelope, MessageEnvelope


def bridge_config(**overrides):
    values = {
        "source_host": "source-broker",
        "destination_host": "destination-broker",
        "data_topic": "reliable/input",
        "ack_topic": "reliable/ack",
        "destination_publish_timeout": 0.01,
        "source_ack_publish_timeout": 0.01,
        "retry_interval": 0.01,
    }
    values.update(overrides)
    return BridgeConfig(**values)


class ReliableMqttBridgeTests(unittest.TestCase):
    def make_bridge(self, **config_overrides):
        source = FakeClient()
        destination = FakeClient()
        bridge = ReliableMqttBridge(
            bridge_config(**config_overrides),
            source_client_factory=client_factory_for(source),
            destination_client_factory=client_factory_for(destination),
        )
        return bridge, source, destination

    @staticmethod
    def make_ready(
        bridge: ReliableMqttBridge,
        source: FakeClient,
        destination: FakeClient,
    ) -> None:
        source.connected = True
        destination.connected = True
        bridge._source_connected.set()
        bridge._source_subscription_ready.set()
        bridge._destination_connected.set()

    def test_remote_forward_failure_or_timeout_sends_no_ack(self) -> None:
        for result in (
            FakePublishInfo(rc=4, published=False),
            FakePublishInfo(rc=0, published=False),
            FakePublishInfo(wait_error=TimeoutError("publish timed out")),
        ):
            with self.subTest(rc=result.rc, published=result.published):
                bridge, source, destination = self.make_bridge()
                self.make_ready(bridge, source, destination)
                destination.publish_results.append(result)

                forwarded = bridge._forward_once(
                    MessageEnvelope(
                        event_id="event-remote-failure",
                        topic="destination/data",
                        payload={"value": 1},
                    )
                )

                self.assertFalse(forwarded)
                self.assertEqual(len(destination.publish_calls), 1)
                self.assertEqual(source.publish_calls, [])

    def test_successful_remote_publish_sends_correlated_confirmed_ack(self) -> None:
        bridge, source, destination = self.make_bridge()
        self.make_ready(bridge, source, destination)
        call_order: list[str] = []
        destination.publish_hook = lambda _call: call_order.append("destination")
        source.publish_hook = lambda _call: call_order.append("ack")
        envelope = MessageEnvelope(
            event_id="event-success",
            topic="factory/machine/data",
            payload={"temperature": 24.5},
        )

        self.assertTrue(bridge._forward_once(envelope))

        self.assertEqual(call_order, ["destination", "ack"])
        destination_call = destination.publish_calls[0]
        self.assertEqual(destination_call["topic"], envelope.topic)
        self.assertEqual(destination_call["qos"], 1)
        self.assertFalse(destination_call["retain"])
        delivered = DeliveryEnvelope.from_bytes(destination_call["payload"])
        self.assertEqual(delivered.event_id, envelope.event_id)
        self.assertEqual(delivered.payload, envelope.payload)

        ack_call = source.publish_calls[0]
        self.assertEqual(ack_call["topic"], bridge.config.ack_topic)
        self.assertEqual(Ack.from_bytes(ack_call["payload"]).event_id, envelope.event_id)

    def test_ack_publish_failure_reports_failure_after_remote_success(self) -> None:
        bridge, source, destination = self.make_bridge()
        self.make_ready(bridge, source, destination)
        source.publish_results.append(FakePublishInfo(rc=0, published=False))
        envelope = MessageEnvelope(
            event_id="event-ack-failure", topic="destination/data", payload=1
        )

        self.assertFalse(bridge._forward_once(envelope))
        self.assertEqual(len(destination.publish_calls), 1)
        self.assertEqual(len(source.publish_calls), 1)

    def test_disconnected_destination_is_not_published_or_acknowledged(self) -> None:
        bridge, source, destination = self.make_bridge()
        source.connected = True
        bridge._source_connected.set()

        self.assertFalse(
            bridge._forward_once(
                MessageEnvelope(
                    event_id="event-offline",
                    topic="destination/data",
                    payload=None,
                )
            )
        )
        self.assertEqual(destination.publish_calls, [])
        self.assertEqual(source.publish_calls, [])

    def test_source_requires_matching_successful_suback_before_intake(self) -> None:
        bridge, source, _destination = self.make_bridge()
        bridge._accepting.set()

        source.emit_connect()

        self.assertFalse(bridge.source_subscription_ready)
        self.assertEqual(source.subscribe_calls[0][:2], (bridge.config.data_topic, 1))
        source.emit_latest_suback((1,))
        self.assertTrue(bridge.source_subscription_ready)

        source.emit_disconnect()
        self.assertFalse(bridge.source_subscription_ready)

    def test_malformed_and_wrong_topic_messages_are_not_queued(self) -> None:
        bridge, source, _destination = self.make_bridge()
        bridge._accepting.set()
        bridge._source_subscription_ready.set()

        source.emit_message("wrong/topic", b"{}")
        source.emit_message(bridge.config.data_topic, b"not-json")

        self.assertEqual(bridge.queued_count, 0)
        self.assertEqual(source.publish_calls, [])

    def test_full_bridge_queue_leaves_new_message_unacknowledged(self) -> None:
        bridge, source, _destination = self.make_bridge(max_queue_size=1)
        bridge._accepting.set()
        bridge._source_subscription_ready.set()
        first = MessageEnvelope(
            event_id="event-one", topic="destination/data", payload=1
        )
        second = MessageEnvelope(
            event_id="event-two", topic="destination/data", payload=2
        )

        source.emit_message(bridge.config.data_topic, first.to_bytes())
        source.emit_message(bridge.config.data_topic, second.to_bytes())

        self.assertEqual(bridge.queued_count, 1)
        self.assertEqual(bridge._tasks.get_nowait(), first)
        with self.assertRaises(queue.Empty):
            bridge._tasks.get_nowait()
        self.assertEqual(source.publish_calls, [])

    def test_duplicate_source_delivery_is_allowed_and_keeps_same_id(self) -> None:
        bridge, source, destination = self.make_bridge()
        self.make_ready(bridge, source, destination)
        envelope = MessageEnvelope(
            event_id="duplicate-event",
            topic="destination/data",
            payload={"value": 7},
        )

        self.assertTrue(bridge._forward_once(envelope))
        self.assertTrue(bridge._forward_once(envelope))

        self.assertEqual(len(destination.publish_calls), 2)
        destination_ids = [
            DeliveryEnvelope.from_bytes(call["payload"]).event_id
            for call in destination.publish_calls
        ]
        self.assertEqual(destination_ids, [envelope.event_id, envelope.event_id])
        self.assertEqual(len(source.publish_calls), 2)


if __name__ == "__main__":
    unittest.main()


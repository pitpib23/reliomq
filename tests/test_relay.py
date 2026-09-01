from __future__ import annotations

import queue
import unittest

from fakes import FakeClient, FakePublishInfo, client_factory_for
from reliomq.config import RelayConfig
from reliomq.protocol import DeliveryAck, DeliveryEnvelope, MessageEnvelope
from reliomq.relay import Relay, ReliableMqttBridge


def relay_config(**overrides):
    values = {
        "source_host": "source-broker",
        "destination_host": "destination-broker",
        "relay_topic": "reliable/input",
        "delivery_ack_topic": "reliable/ack",
        "destination_publish_timeout": 0.01,
        "source_ack_publish_timeout": 0.01,
        "retry_interval": 0.01,
    }
    values.update(overrides)
    return RelayConfig(**values)


class RelayTests(unittest.TestCase):
    def make_relay(self, **config_overrides):
        source = FakeClient()
        destination = FakeClient()
        relay = Relay(
            relay_config(**config_overrides),
            source_client_factory=client_factory_for(source),
            destination_client_factory=client_factory_for(destination),
        )
        return relay, source, destination

    @staticmethod
    def make_ready(
        relay: Relay,
        source: FakeClient,
        destination: FakeClient,
    ) -> None:
        source.connected = True
        destination.connected = True
        relay._source_connected.set()
        relay._source_subscription_ready.set()
        relay._destination_connected.set()

    def test_remote_forward_failure_or_timeout_sends_no_ack(self) -> None:
        for result in (
            FakePublishInfo(rc=4, published=False),
            FakePublishInfo(rc=0, published=False),
            FakePublishInfo(wait_error=TimeoutError("publish timed out")),
        ):
            with self.subTest(rc=result.rc, published=result.published):
                relay, source, destination = self.make_relay()
                self.make_ready(relay, source, destination)
                destination.publish_results.append(result)

                forwarded = relay._forward_once(
                    MessageEnvelope(
                        message_id="event-remote-failure",
                        topic="destination/data",
                        payload={"value": 1},
                    )
                )

                self.assertFalse(forwarded)
                self.assertEqual(len(destination.publish_calls), 1)
                self.assertEqual(source.publish_calls, [])

    def test_successful_remote_publish_sends_correlated_confirmed_ack(self) -> None:
        relay, source, destination = self.make_relay()
        self.make_ready(relay, source, destination)
        call_order: list[str] = []
        destination.publish_hook = lambda _call: call_order.append("destination")
        source.publish_hook = lambda _call: call_order.append("ack")
        envelope = MessageEnvelope(
            message_id="event-success",
            topic="factory/machine/data",
            payload={"temperature": 24.5},
        )

        self.assertTrue(relay._forward_once(envelope))

        self.assertEqual(call_order, ["destination", "ack"])
        destination_call = destination.publish_calls[0]
        self.assertEqual(destination_call["topic"], envelope.topic)
        self.assertEqual(destination_call["qos"], 1)
        self.assertFalse(destination_call["retain"])
        delivered = DeliveryEnvelope.from_bytes(destination_call["payload"])
        self.assertEqual(delivered.message_id, envelope.message_id)
        self.assertEqual(delivered.payload, envelope.payload)

        ack_call = source.publish_calls[0]
        self.assertEqual(ack_call["topic"], relay.config.delivery_ack_topic)
        self.assertEqual(
            DeliveryAck.from_bytes(ack_call["payload"]).message_id, envelope.message_id
        )

    def test_ack_publish_failure_reports_failure_after_remote_success(self) -> None:
        relay, source, destination = self.make_relay()
        self.make_ready(relay, source, destination)
        source.publish_results.append(FakePublishInfo(rc=0, published=False))
        envelope = MessageEnvelope(
            message_id="event-ack-failure", topic="destination/data", payload=1
        )

        self.assertFalse(relay._forward_once(envelope))
        self.assertEqual(len(destination.publish_calls), 1)
        self.assertEqual(len(source.publish_calls), 1)

    def test_disconnected_destination_is_not_published_or_acknowledged(self) -> None:
        relay, source, destination = self.make_relay()
        source.connected = True
        relay._source_connected.set()

        self.assertFalse(
            relay._forward_once(
                MessageEnvelope(
                    message_id="event-offline",
                    topic="destination/data",
                    payload=None,
                )
            )
        )
        self.assertEqual(destination.publish_calls, [])
        self.assertEqual(source.publish_calls, [])

    def test_source_requires_matching_successful_suback_before_intake(self) -> None:
        relay, source, _destination = self.make_relay()
        relay._accepting.set()

        source.emit_connect()

        self.assertFalse(relay.source_subscription_ready)
        self.assertEqual(
            source.subscribe_calls[0][:2], (relay.config.relay_topic, 1)
        )
        source.emit_latest_suback((1,))
        self.assertTrue(relay.source_subscription_ready)

        source.emit_disconnect()
        self.assertFalse(relay.source_subscription_ready)

    def test_malformed_and_wrong_topic_messages_are_not_queued(self) -> None:
        relay, source, _destination = self.make_relay()
        relay._accepting.set()
        relay._source_subscription_ready.set()

        source.emit_message("wrong/topic", b"{}")
        source.emit_message(relay.config.relay_topic, b"not-json")

        self.assertEqual(relay.queued_count, 0)
        self.assertEqual(source.publish_calls, [])

    def test_full_relay_queue_leaves_new_message_unacknowledged(self) -> None:
        relay, source, _destination = self.make_relay(max_queue_size=1)
        relay._accepting.set()
        relay._source_subscription_ready.set()
        first = MessageEnvelope(
            message_id="event-one", topic="destination/data", payload=1
        )
        second = MessageEnvelope(
            message_id="event-two", topic="destination/data", payload=2
        )

        source.emit_message(relay.config.relay_topic, first.to_bytes())
        source.emit_message(relay.config.relay_topic, second.to_bytes())

        self.assertEqual(relay.queued_count, 1)
        self.assertEqual(relay._tasks.get_nowait(), first)
        with self.assertRaises(queue.Empty):
            relay._tasks.get_nowait()
        self.assertEqual(source.publish_calls, [])

    def test_duplicate_source_delivery_is_allowed_and_keeps_same_id(self) -> None:
        relay, source, destination = self.make_relay()
        self.make_ready(relay, source, destination)
        envelope = MessageEnvelope(
            message_id="duplicate-event",
            topic="destination/data",
            payload={"value": 7},
        )

        self.assertTrue(relay._forward_once(envelope))
        self.assertTrue(relay._forward_once(envelope))

        self.assertEqual(len(destination.publish_calls), 2)
        destination_ids = [
            DeliveryEnvelope.from_bytes(call["payload"]).message_id
            for call in destination.publish_calls
        ]
        self.assertEqual(destination_ids, [envelope.message_id, envelope.message_id])
        self.assertEqual(len(source.publish_calls), 2)


class RelayPahoStyleLifecycleTests(unittest.TestCase):
    """connect()/loop_start()/disconnect()/loop_stop() must honestly
    delegate to start()/stop(), bringing up/tearing down BOTH brokers."""

    def make_relay(self):
        source = FakeClient()
        destination = FakeClient()
        relay = Relay(
            relay_config(),
            source_client_factory=client_factory_for(source),
            destination_client_factory=client_factory_for(destination),
        )
        return relay, source, destination

    def test_connect_and_loop_start_bring_up_both_brokers(self) -> None:
        relay, source, destination = self.make_relay()
        self.addCleanup(relay.stop)

        relay.connect()

        self.assertTrue(relay.is_running)
        self.assertEqual(
            destination.connect_calls, [("destination-broker", 1883, 60)]
        )
        self.assertEqual(source.connect_calls, [("source-broker", 1883, 60)])
        # loop_start() after connect() is a harmless, documented no-op.
        same_instance = relay.loop_start()
        self.assertIs(same_instance, relay)

    def test_source_connected_and_destination_connected_are_independent(self) -> None:
        relay, source, destination = self.make_relay()
        self.addCleanup(relay.stop)
        relay.connect()

        self.assertFalse(relay.source_connected)
        self.assertFalse(relay.destination_connected)

        destination.emit_connect()
        self.assertFalse(relay.source_connected)
        self.assertTrue(relay.destination_connected)

        source.emit_connect()
        self.assertTrue(relay.source_connected)
        self.assertTrue(relay.destination_connected)

    def test_loop_stop_and_disconnect_both_tear_down(self) -> None:
        relay, _source, _destination = self.make_relay()
        relay.connect()

        relay.loop_stop()
        self.assertFalse(relay.is_running)
        # disconnect() after loop_stop() is a harmless, documented no-op.
        relay.disconnect()


class RelayDeprecatedCompatTests(unittest.TestCase):
    def test_data_topic_keyword_still_works_and_warns(self) -> None:
        # relay_config() always sets relay_topic, so exercise the deprecated
        # alias through a bare RelayConfig() call instead.
        with self.assertWarns(DeprecationWarning):
            config = RelayConfig(
                source_host="s", destination_host="d", data_topic="legacy/topic"
            )

        self.assertEqual(config.relay_topic, "legacy/topic")

    def test_reliable_mqtt_bridge_is_the_same_class_as_relay(self) -> None:
        self.assertIs(ReliableMqttBridge, Relay)

    def test_bridge_logger_keyword_still_works_and_warns(self) -> None:
        import logging

        custom_logger = logging.getLogger("test-legacy-bridge-logger")
        with self.assertWarns(DeprecationWarning):
            relay = Relay(
                relay_config(),
                source_client_factory=client_factory_for(FakeClient()),
                destination_client_factory=client_factory_for(FakeClient()),
                bridge_logger=custom_logger,
            )

        self.assertIs(relay._logger, custom_logger)

    def test_old_module_path_still_importable(self) -> None:
        from reliomq.bridge import ReliableMqttBridge as ShimBridge
        from reliomq.bridge import Relay as ShimRelay

        self.assertIs(ShimBridge, Relay)
        self.assertIs(ShimRelay, Relay)


if __name__ == "__main__":
    unittest.main()

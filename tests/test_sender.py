from __future__ import annotations

import tempfile
import threading
import time
import unittest
import warnings
from pathlib import Path

from fakes import FakeClient, FakePublishInfo, client_factory_for
from reliomq.config import SenderConfig
from reliomq.protocol import DeliveryAck, MessageEnvelope
from reliomq.sender import DeliveryStatus, ReliablePublisher, Sender


def sender_config(outbox_path: Path, **overrides) -> SenderConfig:
    values = {
        "host": "source-broker",
        "outbox_path": outbox_path,
        "relay_topic": "reliable/input",
        "delivery_ack_topic": "reliable/ack",
        "delivery_ack_timeout": 0.002,
        "mqtt_puback_timeout": 0.01,
        "retry_interval": 0.01,
    }
    values.update(overrides)
    return SenderConfig(**values)


class SenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.outbox_path = Path(self.temporary_directory.name) / "pending.jsonl"

    def make_sender(self, **config_overrides):
        client = FakeClient()
        sender = Sender(
            sender_config(self.outbox_path, **config_overrides),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(sender.stop)
        return sender, client

    @staticmethod
    def make_ready(sender: Sender, client: FakeClient) -> None:
        client.connected = True
        sender._connected.set()
        sender._ack_subscription_ready.set()

    @staticmethod
    def ack_each_publish(sender: Sender, client: FakeClient) -> None:
        def hook(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            client.emit_message(
                sender.config.delivery_ack_topic,
                DeliveryAck(message_id=envelope.message_id).to_bytes(),
            )

        client.publish_hook = hook

    def test_successful_publish_and_matching_ack_remove_durable_head(self) -> None:
        sender, client = self.make_sender()
        self.make_ready(sender, client)
        self.ack_each_publish(sender, client)
        message_id = sender.publish(
            "factory/machine/data", {"temperature": 24.5}, message_id="event-ok"
        )

        status = sender._process_oldest_once()

        self.assertEqual(status, DeliveryStatus.DELIVERED)
        self.assertEqual(message_id, "event-ok")
        self.assertEqual(sender.pending_count(), 0)
        self.assertTrue(sender.wait_for_delivery(message_id, timeout=0))
        call = client.publish_calls[0]
        self.assertEqual(call["topic"], sender.config.relay_topic)
        self.assertEqual(call["qos"], 1)
        self.assertFalse(call["retain"])

    def test_publish_while_broker_unavailable_is_durable_and_not_attempted(self) -> None:
        sender, client = self.make_sender()
        message_id = sender.publish("factory/data", {"value": 1})

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.NOT_READY)
        self.assertTrue(sender.outbox.contains(message_id))
        self.assertEqual(client.publish_calls, [])

    def test_delivery_ack_timeout_retains_message_and_restart_loads_same_id(self) -> None:
        sender, client = self.make_sender()
        self.make_ready(sender, client)
        message_id = sender.publish(
            "factory/data", {"value": 1}, message_id="stable-timeout-id"
        )

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(sender.pending_count(), 1)

        restarted_client = FakeClient()
        restarted = Sender(
            sender.config,
            client_factory=client_factory_for(restarted_client),
        )
        self.addCleanup(restarted.stop)
        oldest = restarted.outbox.peek_oldest()
        self.assertIsNotNone(oldest)
        self.assertEqual(oldest.message_id, message_id)

    def test_publish_return_error_and_confirmation_timeout_retain_message(self) -> None:
        for info in (
            FakePublishInfo(rc=4, published=False),
            FakePublishInfo(rc=0, published=False),
            FakePublishInfo(wait_error=TimeoutError("timeout")),
        ):
            with self.subTest(rc=info.rc, published=info.published):
                path = Path(self.temporary_directory.name) / (
                    f"pending-{len(list(Path(self.temporary_directory.name).glob('*')))}.jsonl"
                )
                client = FakeClient()
                sender = Sender(
                    sender_config(path), client_factory=client_factory_for(client)
                )
                self.addCleanup(sender.stop)
                self.make_ready(sender, client)
                sender.publish("factory/data", 1)
                client.publish_results.append(info)

                self.assertEqual(
                    sender._process_oldest_once(), DeliveryStatus.RETRY
                )
                self.assertEqual(sender.pending_count(), 1)

    def test_fifo_recovery_uses_oldest_before_new_messages(self) -> None:
        sender, client = self.make_sender()
        first = sender.publish("factory/data", 1, message_id="fifo-1")
        second = sender.publish("factory/data", 2, message_id="fifo-2")
        self.make_ready(sender, client)
        third = sender.publish("factory/data", 3, message_id="fifo-3")
        observed: list[str] = []

        def hook(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            observed.append(envelope.message_id)
            client.emit_message(
                sender.config.delivery_ack_topic,
                DeliveryAck(envelope.message_id).to_bytes(),
            )

        client.publish_hook = hook

        statuses = [sender._process_oldest_once() for _ in range(3)]

        self.assertEqual(statuses, [DeliveryStatus.DELIVERED] * 3)
        self.assertEqual(observed, [first, second, third])
        self.assertEqual(sender.pending_count(), 0)

    def test_matching_ack_removes_exactly_one_message(self) -> None:
        sender, client = self.make_sender()
        sender.publish("factory/data", 1, message_id="head")
        sender.publish("factory/data", 2, message_id="tail")
        self.make_ready(sender, client)
        self.ack_each_publish(sender, client)

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)

        self.assertEqual(sender.pending_count(), 1)
        self.assertEqual(sender.outbox.peek_oldest().message_id, "tail")

    def test_wrong_message_id_and_malformed_ack_do_not_remove_message(self) -> None:
        malformed_values = (
            DeliveryAck("other-event").to_bytes(),
            b"not-json",
            b'{"version":1}',
            b"\xff",
        )
        for index, ack_payload in enumerate(malformed_values):
            with self.subTest(ack_payload=ack_payload):
                path = Path(self.temporary_directory.name) / f"wrong-{index}.jsonl"
                client = FakeClient()
                sender = Sender(
                    sender_config(path), client_factory=client_factory_for(client)
                )
                self.addCleanup(sender.stop)
                self.make_ready(sender, client)
                sender.publish("factory/data", 1, message_id=f"expected-{index}")
                client.publish_hook = lambda _call, value=ack_payload: client.emit_message(
                    sender.config.delivery_ack_topic, value
                )

                self.assertEqual(
                    sender._process_oldest_once(), DeliveryStatus.RETRY
                )
                self.assertEqual(sender.pending_count(), 1)

    def test_late_ack_is_ignored_while_next_message_waits(self) -> None:
        sender, client = self.make_sender()
        self.make_ready(sender, client)
        sender.publish("factory/data", 1, message_id="old-event")
        sender.publish("factory/data", 2, message_id="new-event")

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        client.emit_message(
            sender.config.delivery_ack_topic, DeliveryAck("old-event").to_bytes()
        )

        self.ack_each_publish(sender, client)
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        client.publish_hook = lambda _call: client.emit_message(
            sender.config.delivery_ack_topic, DeliveryAck("old-event").to_bytes()
        )

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(sender.outbox.peek_oldest().message_id, "new-event")

    def test_disconnect_then_connect_and_suback_resume_recovery(self) -> None:
        sender, client = self.make_sender()
        sender.publish("factory/data", 1, message_id="recover-after-connect")

        client.emit_connect()
        self.assertFalse(sender._connection_ready())
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.NOT_READY)

        client.emit_latest_suback((1,))
        self.assertTrue(sender._connection_ready())
        self.ack_each_publish(sender, client)
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)

        client.emit_disconnect()
        self.assertFalse(sender._connection_ready())

    def test_retry_keeps_the_same_message_id(self) -> None:
        sender, client = self.make_sender()
        self.make_ready(sender, client)
        message_id = sender.publish("factory/data", 1)
        observed: list[str] = []

        def record_only(call) -> None:
            observed.append(MessageEnvelope.from_bytes(call["payload"]).message_id)

        client.publish_hook = record_only
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        self.ack_each_publish(sender, client)
        client.publish_hook = lambda call: (
            observed.append(MessageEnvelope.from_bytes(call["payload"]).message_id),
            client.emit_message(
                sender.config.delivery_ack_topic,
                DeliveryAck(
                    MessageEnvelope.from_bytes(call["payload"]).message_id
                ).to_bytes(),
            ),
        )

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(observed, [message_id, message_id])

    def test_duplicate_ack_cannot_remove_the_next_message(self) -> None:
        sender, client = self.make_sender()
        self.make_ready(sender, client)
        sender.publish("factory/data", 1, message_id="duplicate-ack")
        sender.publish("factory/data", 2, message_id="untouched-tail")

        def duplicate_ack(_call) -> None:
            payload = DeliveryAck("duplicate-ack").to_bytes()
            client.emit_message(sender.config.delivery_ack_topic, payload)
            client.emit_message(sender.config.delivery_ack_topic, payload)

        client.publish_hook = duplicate_ack
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        client.emit_message(
            sender.config.delivery_ack_topic, DeliveryAck("duplicate-ack").to_bytes()
        )

        self.assertEqual(sender.pending_count(), 1)
        self.assertEqual(sender.outbox.peek_oldest().message_id, "untouched-tail")

    def test_shutdown_interrupts_ack_wait_and_leaves_inflight_durable(self) -> None:
        sender, client = self.make_sender(delivery_ack_timeout=30.0)
        message_id = sender.publish("factory/data", 1, message_id="shutdown-event")
        publish_called = threading.Event()
        client.publish_hook = lambda _call: publish_called.set()
        sender.start()
        client.emit_connect()
        client.emit_latest_suback((1,))

        self.assertTrue(publish_called.wait(timeout=1.0))
        sender.stop()

        self.assertTrue(sender.outbox.contains(message_id))
        self.assertEqual(sender.pending_count(), 1)

    def test_retry_attempt_counter_increments_and_resets_on_delivery(self) -> None:
        # In-memory-only diagnostic counter surfaced in retry logs; never
        # persisted and never affects delivery decisions.
        sender, client = self.make_sender()
        message_id = sender.publish("factory/data", 1, message_id="counted")

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.NOT_READY)
        self.assertNotIn(message_id, sender._retry_attempts)

        self.make_ready(sender, client)
        client.publish_results.append(FakePublishInfo(rc=4, published=False))
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(sender._retry_attempts[message_id], 1)

        client.publish_results.append(FakePublishInfo(rc=4, published=False))
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(sender._retry_attempts[message_id], 2)

        self.ack_each_publish(sender, client)
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertNotIn(message_id, sender._retry_attempts)


class SenderPahoStyleLifecycleTests(unittest.TestCase):
    """connect()/loop_start()/disconnect()/loop_stop()/is_connected() must
    honestly delegate to start()/stop() -- these tests pin that down rather
    than re-testing delivery behavior already covered by SenderTests."""

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.outbox_path = Path(self.temporary_directory.name) / "pending.jsonl"
        self.client = FakeClient()
        self.sender = Sender(
            sender_config(self.outbox_path),
            client_factory=client_factory_for(self.client),
        )
        self.addCleanup(self.sender.stop)

    def test_is_connected_reflects_paho_connection_state_only(self) -> None:
        self.assertFalse(self.sender.is_connected())

        self.sender.connect()
        self.client.emit_connect()
        self.assertTrue(self.sender.is_connected())

        self.client.emit_disconnect()
        self.assertFalse(self.sender.is_connected())

    def test_connect_and_loop_start_are_both_equivalent_to_start(self) -> None:
        self.sender.connect()
        self.assertTrue(self.sender._started)
        # loop_start() after connect() is a harmless, documented no-op.
        same_instance = self.sender.loop_start()
        self.assertIs(same_instance, self.sender)
        self.assertEqual(self.client.connect_calls, [("source-broker", 1883, 60)])

    def test_loop_stop_and_disconnect_are_both_equivalent_to_stop(self) -> None:
        self.sender.connect()
        self.sender.loop_stop()
        self.assertFalse(self.sender._started)
        # disconnect() after loop_stop() is a harmless, documented no-op.
        self.sender.disconnect()

    def test_context_manager_matches_explicit_connect_loop_start(self) -> None:
        explicit = Sender(
            sender_config(Path(self.temporary_directory.name) / "explicit.jsonl"),
            client_factory=client_factory_for(FakeClient()),
        )
        explicit.connect()
        explicit.loop_start()
        self.assertTrue(explicit._started)
        explicit.loop_stop()
        explicit.disconnect()
        self.assertFalse(explicit._started)

        with Sender(
            sender_config(Path(self.temporary_directory.name) / "ctx.jsonl"),
            client_factory=client_factory_for(FakeClient()),
        ) as ctx_sender:
            self.assertTrue(ctx_sender._started)
        self.assertFalse(ctx_sender._started)


class SenderOutboxAttributeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.outbox_path = Path(self.temporary_directory.name) / "pending.jsonl"
        self.sender = Sender(
            sender_config(self.outbox_path),
            client_factory=client_factory_for(FakeClient()),
        )
        self.addCleanup(self.sender.stop)

    def test_outbox_attribute_is_an_outbox(self) -> None:
        from reliomq.outbox import Outbox

        self.assertIsInstance(self.sender.outbox, Outbox)

    def test_store_property_reads_outbox_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            self.assertIs(self.sender.store, self.sender.outbox)


class SenderTimeoutWiringTests(unittest.TestCase):
    """Regression coverage for the mqtt_puback_timeout/delivery_ack_timeout
    rename: prove each config value actually governs the layer its name
    promises, not just that the field exists under a new name."""

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.outbox_path = Path(self.temporary_directory.name) / "pending.jsonl"

    def make_ready(self, sender: Sender, client: FakeClient) -> None:
        client.connected = True
        sender._connected.set()
        sender._ack_subscription_ready.set()

    def test_mqtt_puback_timeout_is_passed_to_the_paho_publish_wait(self) -> None:
        client = FakeClient()
        sender = Sender(
            sender_config(
                self.outbox_path, mqtt_puback_timeout=1.234, delivery_ack_timeout=5.0
            ),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(sender.stop)
        self.make_ready(sender, client)

        info = FakePublishInfo(rc=0, published=True)
        client.publish_results.append(info)

        def ack_immediately(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            client.emit_message(
                sender.config.delivery_ack_topic,
                DeliveryAck(envelope.message_id).to_bytes(),
            )

        client.publish_hook = ack_immediately
        sender.publish("factory/data", 1, message_id="wiring-puback")

        status = sender._process_oldest_once()

        self.assertEqual(status, DeliveryStatus.DELIVERED)
        # The exact configured mqtt_puback_timeout -- not the
        # delivery_ack_timeout, not the library default -- must be what
        # reaches Paho's own wait_for_publish().
        self.assertEqual(info.wait_timeouts, [1.234])

    def test_delivery_ack_timeout_governs_the_ack_wait_not_mqtt_puback_timeout(
        self,
    ) -> None:
        client = FakeClient()
        sender = Sender(
            sender_config(
                self.outbox_path, mqtt_puback_timeout=10.0, delivery_ack_timeout=0.05
            ),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(sender.stop)
        self.make_ready(sender, client)
        # No publish_hook and no queued FakePublishInfo -- client.publish()
        # falls back to a default FakePublishInfo(rc=0, published=True), so
        # the MQTT PUBACK confirms instantly and only the DeliveryAck wait
        # can be the bottleneck below.
        sender.publish("factory/data", 1, message_id="wiring-ack")

        started = time.monotonic()
        status = sender._process_oldest_once()
        elapsed = time.monotonic() - started

        self.assertEqual(status, DeliveryStatus.RETRY)
        # Bounded well under mqtt_puback_timeout=10.0: if that field were
        # governing this wait instead of delivery_ack_timeout=0.05, this
        # assertion would fail (or the test would hang for ~10s).
        self.assertLess(elapsed, 2.0)


class SenderDeprecatedCompatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.outbox_path = Path(self.temporary_directory.name) / "pending.jsonl"
        client = FakeClient()
        self.client = client
        self.sender = ReliablePublisher(
            sender_config(self.outbox_path),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(self.sender.stop)

    def test_reliable_publisher_is_the_same_class_as_sender(self) -> None:
        self.assertIs(ReliablePublisher, Sender)
        self.assertIsInstance(self.sender, Sender)

    def test_publish_event_id_keyword_still_works_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            message_id = self.sender.publish(
                "factory/data", 1, event_id="legacy-publish-id"
            )

        self.assertEqual(message_id, "legacy-publish-id")

    def test_wait_for_delivery_event_id_keyword_still_works_and_warns(self) -> None:
        self.sender.publish("factory/data", 1, message_id="legacy-wait-id")

        with self.assertWarns(DeprecationWarning):
            delivered = self.sender.wait_for_delivery(
                event_id="legacy-wait-id", timeout=0
            )

        self.assertFalse(delivered)  # still pending; broker was never readied

    def test_conflicting_message_id_and_event_id_on_publish_raise(self) -> None:
        with self.assertRaises(ValueError), warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.sender.publish(
                "factory/data", 1, message_id="a", event_id="b"
            )

    def test_old_module_path_still_importable(self) -> None:
        from reliomq.publisher import ReliablePublisher as ShimPublisher
        from reliomq.publisher import Sender as ShimSender

        self.assertIs(ShimPublisher, Sender)
        self.assertIs(ShimSender, Sender)


if __name__ == "__main__":
    unittest.main()

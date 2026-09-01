from __future__ import annotations

import tempfile
import threading
import unittest
import warnings
from pathlib import Path

from fakes import FakeClient, FakePublishInfo, client_factory_for
from reliomq.config import PublisherConfig
from reliomq.protocol import Ack, MessageEnvelope
from reliomq.publisher import DeliveryStatus, ReliablePublisher


def publisher_config(queue_path: Path, **overrides) -> PublisherConfig:
    values = {
        "host": "source-broker",
        "queue_path": queue_path,
        "envelope_topic": "reliable/input",
        "ack_topic": "reliable/ack",
        "ack_timeout": 0.002,
        "publish_timeout": 0.01,
        "retry_interval": 0.01,
    }
    values.update(overrides)
    return PublisherConfig(**values)


class ReliablePublisherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.queue_path = Path(self.temporary_directory.name) / "pending.jsonl"

    def make_publisher(self, **config_overrides):
        client = FakeClient()
        publisher = ReliablePublisher(
            publisher_config(self.queue_path, **config_overrides),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(publisher.stop)
        return publisher, client

    @staticmethod
    def make_ready(publisher: ReliablePublisher, client: FakeClient) -> None:
        client.connected = True
        publisher._connected.set()
        publisher._ack_subscription_ready.set()

    @staticmethod
    def ack_each_publish(publisher: ReliablePublisher, client: FakeClient) -> None:
        def hook(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            client.emit_message(
                publisher.config.ack_topic,
                Ack(message_id=envelope.message_id).to_bytes(),
            )

        client.publish_hook = hook

    def test_successful_publish_and_matching_ack_remove_durable_head(self) -> None:
        publisher, client = self.make_publisher()
        self.make_ready(publisher, client)
        self.ack_each_publish(publisher, client)
        message_id = publisher.publish(
            "factory/machine/data", {"temperature": 24.5}, message_id="event-ok"
        )

        status = publisher._process_oldest_once()

        self.assertEqual(status, DeliveryStatus.DELIVERED)
        self.assertEqual(message_id, "event-ok")
        self.assertEqual(publisher.pending_count(), 0)
        self.assertTrue(publisher.wait_for_delivery(message_id, timeout=0))
        call = client.publish_calls[0]
        self.assertEqual(call["topic"], publisher.config.envelope_topic)
        self.assertEqual(call["qos"], 1)
        self.assertFalse(call["retain"])

    def test_publish_while_broker_unavailable_is_durable_and_not_attempted(self) -> None:
        publisher, client = self.make_publisher()
        message_id = publisher.publish("factory/data", {"value": 1})

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.NOT_READY)
        self.assertTrue(publisher.store.contains(message_id))
        self.assertEqual(client.publish_calls, [])

    def test_ack_timeout_retains_message_and_restart_loads_same_id(self) -> None:
        publisher, client = self.make_publisher()
        self.make_ready(publisher, client)
        message_id = publisher.publish(
            "factory/data", {"value": 1}, message_id="stable-timeout-id"
        )

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(publisher.pending_count(), 1)

        restarted_client = FakeClient()
        restarted = ReliablePublisher(
            publisher.config,
            client_factory=client_factory_for(restarted_client),
        )
        self.addCleanup(restarted.stop)
        oldest = restarted.store.peek_oldest()
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
                publisher = ReliablePublisher(
                    publisher_config(path), client_factory=client_factory_for(client)
                )
                self.addCleanup(publisher.stop)
                self.make_ready(publisher, client)
                publisher.publish("factory/data", 1)
                client.publish_results.append(info)

                self.assertEqual(
                    publisher._process_oldest_once(), DeliveryStatus.RETRY
                )
                self.assertEqual(publisher.pending_count(), 1)

    def test_fifo_recovery_uses_oldest_before_new_messages(self) -> None:
        publisher, client = self.make_publisher()
        first = publisher.publish("factory/data", 1, message_id="fifo-1")
        second = publisher.publish("factory/data", 2, message_id="fifo-2")
        self.make_ready(publisher, client)
        third = publisher.publish("factory/data", 3, message_id="fifo-3")
        observed: list[str] = []

        def hook(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            observed.append(envelope.message_id)
            client.emit_message(
                publisher.config.ack_topic, Ack(envelope.message_id).to_bytes()
            )

        client.publish_hook = hook

        statuses = [publisher._process_oldest_once() for _ in range(3)]

        self.assertEqual(statuses, [DeliveryStatus.DELIVERED] * 3)
        self.assertEqual(observed, [first, second, third])
        self.assertEqual(publisher.pending_count(), 0)

    def test_matching_ack_removes_exactly_one_message(self) -> None:
        publisher, client = self.make_publisher()
        publisher.publish("factory/data", 1, message_id="head")
        publisher.publish("factory/data", 2, message_id="tail")
        self.make_ready(publisher, client)
        self.ack_each_publish(publisher, client)

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.DELIVERED)

        self.assertEqual(publisher.pending_count(), 1)
        self.assertEqual(publisher.store.peek_oldest().message_id, "tail")

    def test_wrong_message_id_and_malformed_ack_do_not_remove_message(self) -> None:
        malformed_values = (
            Ack("other-event").to_bytes(),
            b"not-json",
            b'{"version":1}',
            b"\xff",
        )
        for index, ack_payload in enumerate(malformed_values):
            with self.subTest(ack_payload=ack_payload):
                path = Path(self.temporary_directory.name) / f"wrong-{index}.jsonl"
                client = FakeClient()
                publisher = ReliablePublisher(
                    publisher_config(path), client_factory=client_factory_for(client)
                )
                self.addCleanup(publisher.stop)
                self.make_ready(publisher, client)
                publisher.publish("factory/data", 1, message_id=f"expected-{index}")
                client.publish_hook = lambda _call, value=ack_payload: client.emit_message(
                    publisher.config.ack_topic, value
                )

                self.assertEqual(
                    publisher._process_oldest_once(), DeliveryStatus.RETRY
                )
                self.assertEqual(publisher.pending_count(), 1)

    def test_late_ack_is_ignored_while_next_message_waits(self) -> None:
        publisher, client = self.make_publisher()
        self.make_ready(publisher, client)
        publisher.publish("factory/data", 1, message_id="old-event")
        publisher.publish("factory/data", 2, message_id="new-event")

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.RETRY)
        client.emit_message(publisher.config.ack_topic, Ack("old-event").to_bytes())

        self.ack_each_publish(publisher, client)
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.DELIVERED)
        client.publish_hook = lambda _call: client.emit_message(
            publisher.config.ack_topic, Ack("old-event").to_bytes()
        )

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(publisher.store.peek_oldest().message_id, "new-event")

    def test_disconnect_then_connect_and_suback_resume_recovery(self) -> None:
        publisher, client = self.make_publisher()
        publisher.publish("factory/data", 1, message_id="recover-after-connect")

        client.emit_connect()
        self.assertFalse(publisher._connection_ready())
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.NOT_READY)

        client.emit_latest_suback((1,))
        self.assertTrue(publisher._connection_ready())
        self.ack_each_publish(publisher, client)
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.DELIVERED)

        client.emit_disconnect()
        self.assertFalse(publisher._connection_ready())

    def test_retry_keeps_the_same_message_id(self) -> None:
        publisher, client = self.make_publisher()
        self.make_ready(publisher, client)
        message_id = publisher.publish("factory/data", 1)
        observed: list[str] = []

        def record_only(call) -> None:
            observed.append(MessageEnvelope.from_bytes(call["payload"]).message_id)

        client.publish_hook = record_only
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.RETRY)
        self.ack_each_publish(publisher, client)
        client.publish_hook = lambda call: (
            observed.append(MessageEnvelope.from_bytes(call["payload"]).message_id),
            client.emit_message(
                publisher.config.ack_topic,
                Ack(
                    MessageEnvelope.from_bytes(call["payload"]).message_id
                ).to_bytes(),
            ),
        )

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(observed, [message_id, message_id])

    def test_duplicate_ack_cannot_remove_the_next_message(self) -> None:
        publisher, client = self.make_publisher()
        self.make_ready(publisher, client)
        publisher.publish("factory/data", 1, message_id="duplicate-ack")
        publisher.publish("factory/data", 2, message_id="untouched-tail")

        def duplicate_ack(_call) -> None:
            payload = Ack("duplicate-ack").to_bytes()
            client.emit_message(publisher.config.ack_topic, payload)
            client.emit_message(publisher.config.ack_topic, payload)

        client.publish_hook = duplicate_ack
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.DELIVERED)
        client.emit_message(
            publisher.config.ack_topic, Ack("duplicate-ack").to_bytes()
        )

        self.assertEqual(publisher.pending_count(), 1)
        self.assertEqual(publisher.store.peek_oldest().message_id, "untouched-tail")

    def test_shutdown_interrupts_ack_wait_and_leaves_inflight_durable(self) -> None:
        publisher, client = self.make_publisher(ack_timeout=30.0)
        message_id = publisher.publish("factory/data", 1, message_id="shutdown-event")
        publish_called = threading.Event()
        client.publish_hook = lambda _call: publish_called.set()
        publisher.start()
        client.emit_connect()
        client.emit_latest_suback((1,))

        self.assertTrue(publish_called.wait(timeout=1.0))
        publisher.stop()

        self.assertTrue(publisher.store.contains(message_id))
        self.assertEqual(publisher.pending_count(), 1)

    def test_retry_attempt_counter_increments_and_resets_on_delivery(self) -> None:
        # In-memory-only diagnostic counter surfaced in retry logs; never
        # persisted and never affects delivery decisions.
        publisher, client = self.make_publisher()
        message_id = publisher.publish("factory/data", 1, message_id="counted")

        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.NOT_READY)
        self.assertNotIn(message_id, publisher._retry_attempts)

        self.make_ready(publisher, client)
        client.publish_results.append(FakePublishInfo(rc=4, published=False))
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(publisher._retry_attempts[message_id], 1)

        client.publish_results.append(FakePublishInfo(rc=4, published=False))
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(publisher._retry_attempts[message_id], 2)

        self.ack_each_publish(publisher, client)
        self.assertEqual(publisher._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertNotIn(message_id, publisher._retry_attempts)


class ReliablePublisherDeprecatedCompatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.queue_path = Path(self.temporary_directory.name) / "pending.jsonl"
        client = FakeClient()
        self.client = client
        self.publisher = ReliablePublisher(
            publisher_config(self.queue_path),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(self.publisher.stop)

    def test_publish_event_id_keyword_still_works_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            message_id = self.publisher.publish(
                "factory/data", 1, event_id="legacy-publish-id"
            )

        self.assertEqual(message_id, "legacy-publish-id")

    def test_wait_for_delivery_event_id_keyword_still_works_and_warns(self) -> None:
        self.publisher.publish("factory/data", 1, message_id="legacy-wait-id")

        with self.assertWarns(DeprecationWarning):
            delivered = self.publisher.wait_for_delivery(
                event_id="legacy-wait-id", timeout=0
            )

        self.assertFalse(delivered)  # still pending; broker was never readied

    def test_conflicting_message_id_and_event_id_on_publish_raise(self) -> None:
        with self.assertRaises(ValueError), warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.publisher.publish(
                "factory/data", 1, message_id="a", event_id="b"
            )


if __name__ == "__main__":
    unittest.main()

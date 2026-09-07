from __future__ import annotations

import tempfile
import threading
import time
import unittest
import unittest.mock
import warnings
from pathlib import Path

from fakes import FakeClient, FakePublishInfo, client_factory_for
from reliomq.config import SenderConfig
from reliomq.durability import (
    DurableMode,
    FastMode,
    FastQueueFullError,
    GroupMode,
)
from reliomq.outbox import OutboxError
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
            mode=DurableMode(),
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
            mode=DurableMode(),
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


class SenderDurabilityModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self._sender_number = 0

    def make_sender(self, mode=None, **config_overrides):
        self._sender_number += 1
        outbox_path = Path(self.temporary_directory.name) / (
            f"modes-{self._sender_number}.jsonl"
        )
        client = FakeClient()
        config = sender_config(outbox_path, **config_overrides)
        if mode is None:
            sender = Sender(
                config,
                client_factory=client_factory_for(client),
            )
        else:
            sender = Sender(
                config,
                mode=mode,
                client_factory=client_factory_for(client),
            )
        self.addCleanup(sender.stop)
        return sender, client

    @staticmethod
    def roomy_fast_mode(**overrides) -> FastMode:
        values = {
            "ram_max_messages": 100,
            "ram_max_bytes": 1_000_000,
            "high_watermark": 1.0,
            "max_ram_age": 60.0,
            "disconnect_grace": 60.0,
            "spill_batch_messages": 100,
            "spill_batch_bytes": 1_000_000,
        }
        values.update(overrides)
        return FastMode(**values)

    @staticmethod
    def make_ready(sender: Sender, client: FakeClient) -> None:
        client.connected = True
        sender._connected.set()
        sender._ack_subscription_ready.set()

    @staticmethod
    def ack_each_publish(sender: Sender, client: FakeClient, observed=None) -> None:
        def hook(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            if observed is not None:
                observed.append(envelope.message_id)
            client.emit_message(
                sender.config.delivery_ack_topic,
                DeliveryAck(envelope.message_id).to_bytes(),
            )

        client.publish_hook = hook

    def test_default_and_explicit_modes_are_client_level_choices(self) -> None:
        default_sender, _client = self.make_sender()
        self.assertIsInstance(default_sender.mode, FastMode)
        default_sender.publish("factory/data", 1, message_id="default-fast")
        self.assertFalse(default_sender.outbox.contains("default-fast"))
        self.assertEqual(default_sender.pending_count(), 1)

        for mode in (
            DurableMode(),
            GroupMode(sync_interval=60.0, ack_checkpoint_interval=60.0),
            self.roomy_fast_mode(),
        ):
            with self.subTest(mode=type(mode).__name__):
                sender, _client = self.make_sender(mode)
                self.assertIs(sender.mode, mode)

    def test_mode_is_immutable_and_publish_has_no_mode_override(self) -> None:
        mode = self.roomy_fast_mode()
        sender, _client = self.make_sender(mode)

        with self.assertRaises(AttributeError):
            sender.mode = DurableMode()  # type: ignore[misc]
        with self.assertRaises(TypeError):
            sender.publish(  # type: ignore[call-arg]
                "factory/data", 1, mode=DurableMode()
            )
        with self.assertRaises(TypeError):
            sender.publish(  # type: ignore[call-arg]
                "factory/data", 1, durability="durable"
            )

        self.assertIs(sender.mode, mode)
        self.assertEqual(sender.pending_count(), 0)

    def test_group_appends_immediately_and_syncs_at_the_group_boundary(self) -> None:
        sender, client = self.make_sender(
            GroupMode(
                sync_messages=2,
                sync_interval=60.0,
                sync_bytes=1_000_000,
                ack_checkpoint_interval=60.0,
            )
        )

        with unittest.mock.patch.object(
            sender.outbox, "append", wraps=sender.outbox.append
        ) as append, unittest.mock.patch.object(
            sender.outbox, "sync", wraps=sender.outbox.sync
        ) as sync:
            sender.publish("factory/data", 1, message_id="group-one")

            self.assertTrue(sender.outbox.contains("group-one"))
            self.assertEqual(client.publish_calls, [])
            self.assertEqual(append.call_args.kwargs, {"sync": False})
            self.assertEqual(sync.call_count, 0)

            sender.publish("factory/data", 2, message_id="group-two")

            self.assertEqual(append.call_count, 2)
            self.assertEqual(sync.call_count, 1)
            self.assertEqual(sender._persistence.unsynced_messages, 0)

    def test_group_shutdown_syncs_data_and_checkpoints_ack_progress(self) -> None:
        sender, client = self.make_sender(
            GroupMode(
                sync_messages=100,
                sync_interval=60.0,
                sync_bytes=1_000_000,
                ack_checkpoint_messages=100,
                ack_checkpoint_interval=60.0,
            )
        )
        sender.publish("factory/data", 1, message_id="group-shutdown")
        self.make_ready(sender, client)
        self.ack_each_publish(sender, client)

        with unittest.mock.patch.object(
            sender.outbox, "sync", wraps=sender.outbox.sync
        ) as sync, unittest.mock.patch.object(
            sender.outbox, "checkpoint", wraps=sender.outbox.checkpoint
        ) as checkpoint:
            self.assertEqual(
                sender._process_oldest_once(), DeliveryStatus.DELIVERED
            )
            self.assertEqual(sync.call_count, 0)
            self.assertEqual(checkpoint.call_count, 0)

            sender.stop()

            self.assertEqual(sync.call_count, 1)
            self.assertEqual(checkpoint.call_count, 1)

    def test_fast_healthy_delivery_never_writes_to_disk(self) -> None:
        sender, client = self.make_sender(self.roomy_fast_mode())
        self.make_ready(sender, client)
        self.ack_each_publish(sender, client)

        with unittest.mock.patch.object(
            sender.outbox, "append", wraps=sender.outbox.append
        ) as append, unittest.mock.patch.object(
            sender.outbox, "append_many", wraps=sender.outbox.append_many
        ) as append_many:
            message_id = sender.publish(
                "factory/data", {"value": 1}, message_id="fast-healthy"
            )

            self.assertEqual(sender.pending_count(), 1)
            self.assertEqual(sender.outbox.load(), [])
            self.assertFalse(sender.wait_for_delivery(message_id, timeout=0))
            self.assertEqual(
                sender._process_oldest_once(), DeliveryStatus.DELIVERED
            )

            self.assertEqual(sender.pending_count(), 0)
            self.assertTrue(sender.wait_for_delivery(message_id, timeout=0))
            self.assertEqual(sender.outbox.load(), [])
            append.assert_not_called()
            append_many.assert_not_called()

    def test_fast_hard_count_and_byte_limits_do_not_overcommit_ram(self) -> None:
        count_sender, _client = self.make_sender(
            self.roomy_fast_mode(ram_max_messages=1, high_watermark=1.0)
        )
        spill_error = OutboxError("disk unavailable")
        with unittest.mock.patch.object(
            count_sender.outbox, "append_many", side_effect=spill_error
        ), self.assertLogs("reliomq.sender", level="ERROR"):
            count_sender.publish("factory/data", 1, message_id="count-one")
            with self.assertRaises(FastQueueFullError):
                count_sender.publish("factory/data", 2, message_id="count-two")

        self.assertEqual(count_sender.pending_count(), 1)
        self.assertEqual(len(count_sender._fast_queue), 1)

        oversized = MessageEnvelope(
            message_id="too-large",
            topic="factory/data",
            payload={"value": "x" * 20},
        )
        byte_sender, _client = self.make_sender(
            self.roomy_fast_mode(ram_max_bytes=len(oversized.to_bytes()) - 1)
        )

        with self.assertRaisesRegex(FastQueueFullError, "ram_max_bytes"):
            byte_sender.publish(
                "factory/data",
                {"value": "x" * 20},
                message_id="too-large",
            )

        self.assertEqual(byte_sender.pending_count(), 0)
        self.assertEqual(byte_sender._fast_ram_bytes, 0)

    def test_fast_high_watermark_spills_on_count_or_bytes(self) -> None:
        count_sender, _client = self.make_sender(
            self.roomy_fast_mode(
                ram_max_messages=4,
                high_watermark=0.5,
            )
        )
        with unittest.mock.patch.object(
            count_sender.outbox,
            "append_many",
            wraps=count_sender.outbox.append_many,
        ) as append_many:
            count_sender.publish("factory/data", 1, message_id="count-a")
            self.assertEqual(len(count_sender._fast_queue), 1)
            count_sender.publish("factory/data", 2, message_id="count-b")

            self.assertEqual(len(count_sender._fast_queue), 0)
            self.assertEqual(
                [item.message_id for item in count_sender.outbox.load()],
                ["count-a", "count-b"],
            )
            self.assertEqual(append_many.call_count, 1)

        first = MessageEnvelope(
            message_id="bytes-a", topic="factory/data", payload="same"
        )
        second = MessageEnvelope(
            message_id="bytes-b", topic="factory/data", payload="same"
        )
        byte_sender, _client = self.make_sender(
            self.roomy_fast_mode(
                ram_max_bytes=len(first.to_bytes()) + len(second.to_bytes()),
                high_watermark=0.75,
            )
        )
        byte_sender.publish("factory/data", "same", message_id="bytes-a")
        self.assertEqual(len(byte_sender._fast_queue), 1)
        byte_sender.publish("factory/data", "same", message_id="bytes-b")

        self.assertEqual(len(byte_sender._fast_queue), 0)
        self.assertEqual(
            [item.message_id for item in byte_sender.outbox.load()],
            ["bytes-a", "bytes-b"],
        )

    def test_fast_oldest_age_timer_spills_without_another_publish(self) -> None:
        sender, _client = self.make_sender(
            self.roomy_fast_mode(max_ram_age=0.02)
        )
        spilled = threading.Event()
        append_many = sender.outbox.append_many

        def observe_spill(envelopes, *, sync=True):
            result = append_many(envelopes, sync=sync)
            spilled.set()
            return result

        with unittest.mock.patch.object(
            sender.outbox, "append_many", side_effect=observe_spill
        ):
            sender.publish("factory/data", 1, message_id="aged-fast")
            self.assertTrue(spilled.wait(timeout=1.0))

        self.assertTrue(sender.outbox.contains("aged-fast"))
        self.assertEqual(len(sender._fast_queue), 0)

    def test_fast_disconnect_spills_only_after_continuous_grace(self) -> None:
        sender, client = self.make_sender(
            self.roomy_fast_mode(
                max_ram_age=60.0,
                disconnect_grace=0.05,
            )
        )
        self.make_ready(sender, client)
        sender.publish("factory/data", 1, message_id="disconnect-fast")
        spilled = threading.Event()
        append_many = sender.outbox.append_many

        def observe_spill(envelopes, *, sync=True):
            result = append_many(envelopes, sync=sync)
            spilled.set()
            return result

        with unittest.mock.patch.object(
            sender.outbox, "append_many", side_effect=observe_spill
        ):
            client.emit_disconnect()
            client.emit_connect()
            self.assertFalse(spilled.wait(timeout=0.08))

            client.emit_disconnect()
            self.assertTrue(spilled.wait(timeout=1.0))

        self.assertTrue(sender.outbox.contains("disconnect-fast"))
        self.assertEqual(len(sender._fast_queue), 0)

    def test_fast_delivery_ack_timeout_spills_before_retry(self) -> None:
        sender, client = self.make_sender(
            self.roomy_fast_mode(), delivery_ack_timeout=0.01
        )
        self.make_ready(sender, client)
        sender.publish("factory/data", 1, message_id="ack-timeout-fast")

        self.assertEqual(sender.outbox.load(), [])
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)

        self.assertTrue(sender.outbox.contains("ack-timeout-fast"))
        self.assertEqual(len(sender._fast_queue), 0)
        self.assertEqual(sender.pending_count(), 1)

    def test_fast_disconnect_during_puback_wait_observes_grace(self) -> None:
        sender, client = self.make_sender(self.roomy_fast_mode())
        self.make_ready(sender, client)
        sender.publish("factory/data", 1, message_id="puback-disconnect")
        client.publish_results.append(
            FakePublishInfo(
                published=False,
                wait_hook=lambda _timeout: client.emit_disconnect(),
            )
        )

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        self.assertEqual(sender.outbox.load(), [])
        self.assertEqual(len(sender._fast_queue), 1)
        self.assertEqual(sender.pending_count(), 1)

        # Reconnect inside the grace period and complete without a spill.
        client.emit_connect()
        client.emit_latest_suback()
        client.publish_hook = lambda _call: client.emit_message(
            sender.config.delivery_ack_topic,
            DeliveryAck(message_id="puback-disconnect").to_bytes(),
        )
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(sender.outbox.load(), [])
        self.assertEqual(sender.pending_count(), 0)

    def test_fast_clean_shutdown_spills_every_pending_message(self) -> None:
        sender, _client = self.make_sender(self.roomy_fast_mode())
        for number in range(3):
            sender.publish(
                "factory/data", number, message_id=f"shutdown-{number}"
            )

        self.assertEqual(sender.outbox.load(), [])
        sender.stop()

        self.assertEqual(len(sender._fast_queue), 0)
        self.assertEqual(sender._fast_ram_bytes, 0)
        self.assertEqual(
            [item.message_id for item in sender.outbox.load()],
            ["shutdown-0", "shutdown-1", "shutdown-2"],
        )

    def test_fast_spill_batches_honor_message_and_byte_caps(self) -> None:
        message_sender, _client = self.make_sender(
            self.roomy_fast_mode(spill_batch_messages=2)
        )
        for number in range(5):
            message_sender.publish(
                "factory/data", number, message_id=f"message-batch-{number}"
            )

        with unittest.mock.patch.object(
            message_sender.outbox,
            "append_many",
            wraps=message_sender.outbox.append_many,
        ) as append_many:
            message_sender.stop()

        self.assertEqual(
            [
                [envelope.message_id for envelope in call.args[0]]
                for call in append_many.call_args_list
            ],
            [
                ["message-batch-0", "message-batch-1"],
                ["message-batch-2", "message-batch-3"],
                ["message-batch-4"],
            ],
        )

        sample = MessageEnvelope(
            message_id="bytes-batch-0",
            topic="factory/data",
            payload="same",
        )
        byte_sender, _client = self.make_sender(
            self.roomy_fast_mode(
                spill_batch_messages=10,
                spill_batch_bytes=2 * len(sample.to_bytes()),
            )
        )
        for number in range(5):
            byte_sender.publish(
                "factory/data", "same", message_id=f"bytes-batch-{number}"
            )

        with unittest.mock.patch.object(
            byte_sender.outbox,
            "append_many",
            wraps=byte_sender.outbox.append_many,
        ) as append_many:
            byte_sender.stop()

        self.assertEqual(
            [len(call.args[0]) for call in append_many.call_args_list],
            [2, 2, 1],
        )

    def test_failed_fast_spill_keeps_ram_ownership_until_fsync_succeeds(self) -> None:
        sender, _client = self.make_sender(self.roomy_fast_mode())
        sender.publish(
            "factory/data", {"value": 1}, message_id="ram-owned"
        )
        item = sender._fast_queue[0]
        ram_bytes = sender._fast_ram_bytes

        with unittest.mock.patch.object(
            sender.outbox,
            "append_many",
            side_effect=OutboxError("simulated fsync failure"),
        ), self.assertLogs("reliomq.sender", level="ERROR"):
            with self.assertRaisesRegex(OutboxError, "simulated fsync failure"):
                sender.stop()

        self.assertIs(sender._fast_queue[0], item)
        self.assertEqual(item.state.value, "ram_only")
        self.assertEqual(sender._fast_ram_bytes, ram_bytes)
        self.assertEqual(sender.outbox.load(), [])

        sender.stop()
        self.assertEqual(item.state.value, "disk_backed")
        self.assertEqual(len(sender._fast_queue), 0)
        self.assertTrue(sender.outbox.contains("ram-owned"))

    def test_fast_publish_failure_preserves_stable_id_across_restart(self) -> None:
        sender, client = self.make_sender(self.roomy_fast_mode())
        self.make_ready(sender, client)
        client.publish_results.append(FakePublishInfo(rc=4, published=False))
        message_id = sender.publish("factory/data", {"sequence": 1})

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
        stored = sender.outbox.peek_oldest()
        self.assertIsNotNone(stored)
        self.assertEqual(stored.message_id, message_id)
        self.assertEqual(stored.payload, {"sequence": 1})

        restarted_client = FakeClient()
        restarted = Sender(
            sender.config,
            mode=self.roomy_fast_mode(),
            client_factory=client_factory_for(restarted_client),
        )
        self.addCleanup(restarted.stop)

        recovered = restarted.outbox.peek_oldest()
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.message_id, message_id)
        self.assertEqual(restarted.pending_count(), 1)


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

    def test_start_rejects_while_a_previous_worker_is_still_stopping(self) -> None:
        release = threading.Event()
        lingering = threading.Thread(target=release.wait, daemon=True)
        lingering.start()
        self.sender._worker = lingering
        try:
            with self.assertRaisesRegex(RuntimeError, "still stopping"):
                self.sender.start()
            self.assertEqual(self.client.connect_calls, [])
        finally:
            release.set()
            lingering.join(timeout=1.0)

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

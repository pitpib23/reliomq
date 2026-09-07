from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from fakes import FakeClient, client_factory_for
from reliomq.config import SenderConfig
from reliomq.durability import DurableMode, FastMode, GroupMode
from reliomq.outbox import Outbox, OutboxError
from reliomq.protocol import DeliveryAck, MessageEnvelope
from reliomq.sender import DeliveryStatus, Sender


class ModeStorageIntegrationTests(unittest.TestCase):
    """Check mode guarantees against real segment files and fsync calls."""

    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        self.next_path = 0

    def make_sender(self, mode, *, outbox=None, ack_timeout=0.005):
        self.next_path += 1
        path = self.root / f"pending-{self.next_path}"
        client = FakeClient()
        sender = Sender(
            SenderConfig(
                host="source-broker",
                outbox_path=path,
                delivery_ack_timeout=ack_timeout,
                mqtt_puback_timeout=0.01,
                retry_interval=0.01,
            ),
            mode=mode,
            outbox=outbox,
            client_factory=client_factory_for(client),
        )
        self.addCleanup(sender.stop)
        return sender, client

    @staticmethod
    def ready(sender, client):
        client.connected = True
        sender._connected.set()
        sender._ack_subscription_ready.set()

    @staticmethod
    def acknowledge(sender, client, message_id):
        client.emit_message(
            sender.config.delivery_ack_topic,
            DeliveryAck(message_id=message_id).to_bytes(),
        )

    def acknowledge_every_publish(self, sender, client):
        def on_publish(call):
            envelope = MessageEnvelope.from_bytes(call["payload"])
            self.acknowledge(sender, client, envelope.message_id)

        client.publish_hook = on_publish

    @staticmethod
    def pending_ids(outbox):
        return [message.message_id for message in outbox.load()]

    @staticmethod
    def slow_group(**overrides):
        arguments = {
            "sync_messages": 20,
            "sync_interval": 3600,
            "sync_bytes": 1024 * 1024 * 1024,
            "ack_checkpoint_messages": 1000,
            "ack_checkpoint_interval": 3600,
        }
        arguments.update(overrides)
        return GroupMode(**arguments)

    @staticmethod
    def slow_fast(**overrides):
        arguments = {"max_ram_age": 3600, "disconnect_grace": 3600}
        arguments.update(overrides)
        return FastMode(**arguments)

    def test_one_hundred_durable_messages_each_fsync_real_segment_data(self):
        sender, _client = self.make_sender(DurableMode())
        # Construction metadata is deliberately outside the measurement.
        with patch("reliomq.outbox.os.fsync", wraps=os.fsync) as fsync:
            for number in range(100):
                sender.publish("factory/data", number, message_id=f"durable-{number}")
            self.assertEqual(fsync.call_count, 100)

        recovered = Outbox(sender.outbox.path)
        self.assertEqual(self.pending_ids(recovered), [f"durable-{n}" for n in range(100)])

    def test_one_hundred_group_messages_append_immediately_and_fsync_five_times(self):
        sender, _client = self.make_sender(self.slow_group())
        segment = next(sender.outbox.storage_path.glob("segment-*.dat"))
        previous_size = segment.stat().st_size
        with (
            patch.object(sender.outbox, "append", wraps=sender.outbox.append) as append,
            patch("reliomq.outbox.os.fsync", wraps=os.fsync) as fsync,
        ):
            for number in range(100):
                sender.publish("factory/data", number, message_id=f"group-{number}")
                current_size = segment.stat().st_size
                self.assertGreater(current_size, previous_size)
                previous_size = current_size
            self.assertEqual(append.call_count, 100)
            self.assertTrue(all(call.kwargs == {"sync": False} for call in append.call_args_list))
            self.assertEqual(fsync.call_count, 5)

        self.assertEqual(len(Outbox(sender.outbox.path)), 100)

    def test_one_hundred_healthy_fast_deliveries_write_no_persistent_messages(self):
        sender, client = self.make_sender(self.slow_fast())
        self.ready(sender, client)
        self.acknowledge_every_publish(sender, client)
        before = {
            path.name: path.read_bytes()
            for path in sender.outbox.storage_path.iterdir()
        }
        with (
            patch.object(sender.outbox, "append", wraps=sender.outbox.append) as append,
            patch.object(sender.outbox, "append_many", wraps=sender.outbox.append_many) as append_many,
            patch("reliomq.outbox.os.fsync", wraps=os.fsync) as fsync,
        ):
            for number in range(100):
                sender.publish("factory/data", number, message_id=f"fast-{number}")
                self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
            sender.stop()
            append.assert_not_called()
            append_many.assert_not_called()
            fsync.assert_not_called()

        after = {
            path.name: path.read_bytes()
            for path in sender.outbox.storage_path.iterdir()
        }
        self.assertEqual(after, before)
        self.assertEqual(sender.pending_count(), 0)
        self.assertEqual(sender._fast_ram_bytes, 0)

    def test_group_acked_messages_replay_until_cursor_is_checkpointed(self):
        sender, client = self.make_sender(self.slow_group(sync_messages=1))
        self.ready(sender, client)
        self.acknowledge_every_publish(sender, client)
        for number in range(3):
            sender.publish("factory/data", number, message_id=f"cursor-{number}")
        checkpoint = sender.outbox.storage_path / "checkpoint.json"
        before = checkpoint.read_bytes()

        for _ in range(2):
            self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)

        self.assertEqual(self.pending_ids(sender.outbox), ["cursor-2"])
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertEqual(
            self.pending_ids(Outbox(sender.outbox.path)),
            ["cursor-0", "cursor-1", "cursor-2"],
        )

        sender._persistence.checkpoint()

        self.assertNotEqual(checkpoint.read_bytes(), before)
        self.assertEqual(self.pending_ids(Outbox(sender.outbox.path)), ["cursor-2"])

    def test_group_ack_count_checkpoints_without_waiting_for_data_count(self):
        sender, client = self.make_sender(self.slow_group(ack_checkpoint_messages=2))
        self.ready(sender, client)
        self.acknowledge_every_publish(sender, client)
        for number in range(4):
            sender.publish("factory/data", number, message_id=f"count-{number}")
        checkpoint = sender.outbox.storage_path / "checkpoint.json"
        before = checkpoint.read_bytes()
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(checkpoint.read_bytes(), before)

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)

        self.assertNotEqual(checkpoint.read_bytes(), before)
        self.assertEqual(self.pending_ids(Outbox(sender.outbox.path)), ["count-2", "count-3"])

    def test_group_ack_timer_checkpoints_without_another_publish_or_ack(self):
        sender, client = self.make_sender(
            self.slow_group(ack_checkpoint_interval=0.05)
        )
        self.ready(sender, client)
        self.acknowledge_every_publish(sender, client)
        sender.publish("factory/data", 1, message_id="timer-acked")
        sender.publish("factory/data", 2, message_id="timer-pending")
        checkpointed = threading.Event()
        real_checkpoint = sender.outbox.checkpoint

        def checkpoint_and_signal():
            real_checkpoint()
            checkpointed.set()

        with patch.object(sender.outbox, "checkpoint", side_effect=checkpoint_and_signal):
            self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
            self.assertTrue(checkpointed.wait(2), "ACK checkpoint timer did not run")

        self.assertEqual(self.pending_ids(Outbox(sender.outbox.path)), ["timer-pending"])

    def test_group_finished_closed_segment_checkpoints_and_deletes_only_that_segment(self):
        outbox = Outbox(self.root / "rotating", segment_max_records=2)
        sender, client = self.make_sender(self.slow_group(), outbox=outbox)
        self.ready(sender, client)
        self.acknowledge_every_publish(sender, client)
        for number in range(3):
            sender.publish("factory/data", number, message_id=f"segment-{number}")
        segments = sorted(outbox.storage_path.glob("segment-*.dat"))
        self.assertEqual(len(segments), 2)
        active_bytes = segments[1].read_bytes()

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertTrue(segments[0].exists())
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)

        self.assertFalse(segments[0].exists())
        self.assertEqual(segments[1].read_bytes(), active_bytes)
        self.assertEqual(self.pending_ids(Outbox(outbox.path)), ["segment-2"])

    def test_all_modes_retry_the_original_snapshot_after_caller_mutates_payload(self):
        for mode in (DurableMode(), self.slow_group(), self.slow_fast()):
            with self.subTest(mode=type(mode).__name__):
                sender, client = self.make_sender(mode)
                self.ready(sender, client)
                payload = {"measurements": [{"temperature": 25}], "labels": ["original"]}
                message_id = sender.publish("factory/data", payload, message_id="snapshot")
                expected = MessageEnvelope(
                    message_id="snapshot",
                    topic="factory/data",
                    payload={"measurements": [{"temperature": 25}], "labels": ["original"]},
                )
                payload["measurements"][0]["temperature"] = 99
                payload["labels"].append("mutated")

                self.assertEqual(sender._process_oldest_once(), DeliveryStatus.RETRY)
                self.assertEqual(sender.outbox.peek_oldest().to_bytes(), expected.to_bytes())
                self.acknowledge_every_publish(sender, client)
                self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)

                self.assertEqual(message_id, "snapshot")
                self.assertEqual(
                    [call["payload"] for call in client.publish_calls],
                    [expected.to_bytes(), expected.to_bytes()],
                )

    def test_fast_recovery_delivers_disk_prefix_before_new_ram_suffix(self):
        outbox = Outbox(self.root / "recovered")
        outbox.append_many(
            [MessageEnvelope(message_id=f"disk-{n}", topic="factory/data", payload=n) for n in range(2)]
        )
        sender, client = self.make_sender(self.slow_fast(), outbox=outbox)
        self.ready(sender, client)
        self.acknowledge_every_publish(sender, client)
        with patch.object(outbox, "append_many", wraps=outbox.append_many) as spill:
            for number in range(2):
                sender.publish("factory/data", number, message_id=f"ram-{number}")
            self.assertEqual(self.pending_ids(outbox), ["disk-0", "disk-1"])
            self.assertEqual(sender.pending_count(), 4)
            for _ in range(4):
                self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
            spill.assert_not_called()

        self.assertEqual(
            [MessageEnvelope.from_bytes(call["payload"]).message_id for call in client.publish_calls],
            ["disk-0", "disk-1", "ram-0", "ram-1"],
        )
        self.assertEqual(sender.pending_count(), 0)

    def test_fast_ack_during_spill_cannot_release_ram_before_real_fsync_finishes(self):
        sender, client = self.make_sender(self.slow_fast(), ack_timeout=2)
        self.ready(sender, client)
        message_id = sender.publish("factory/data", {"value": 1}, message_id="racing-ack")
        ram_bytes = sender._fast_ram_bytes
        published = threading.Event()
        fsync_entered = threading.Event()
        release_fsync = threading.Event()
        delivered = threading.Event()
        statuses = []
        failures = []
        real_fsync = os.fsync
        client.publish_hook = lambda _call: published.set()

        def blocked_fsync(descriptor):
            if not fsync_entered.is_set():
                fsync_entered.set()
                if not release_fsync.wait(2):
                    raise OSError("test did not release fsync")
            return real_fsync(descriptor)

        def deliver():
            try:
                statuses.append(sender._process_oldest_once())
            except Exception as error:
                failures.append(error)
            finally:
                delivered.set()

        def spill():
            try:
                with sender._queue_lock:
                    sender._spill_fast_batch_locked()
            except Exception as error:
                failures.append(error)

        delivery_thread = threading.Thread(target=deliver)
        spill_thread = threading.Thread(target=spill)
        with patch("reliomq.outbox.os.fsync", side_effect=blocked_fsync):
            delivery_thread.start()
            try:
                self.assertTrue(published.wait(1))
                spill_thread.start()
                self.assertTrue(fsync_entered.wait(1))
                # The spill thread is paused inside fsync while it owns the
                # queue lock, so these observations are stable without taking it.
                self.assertEqual(len(sender._fast_queue), 1)
                self.assertEqual(sender._fast_queue[0].state.value, "persisting")
                self.assertEqual(sender._fast_ram_bytes, ram_bytes)
                self.acknowledge(sender, client, message_id)
                self.assertFalse(delivered.wait(0.02))
                self.assertEqual(sender._fast_ram_bytes, ram_bytes)
            finally:
                release_fsync.set()
                delivery_thread.join(3)
                if spill_thread.ident is not None:
                    spill_thread.join(3)

        self.assertFalse(delivery_thread.is_alive())
        self.assertFalse(spill_thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(statuses, [DeliveryStatus.DELIVERED])
        self.assertEqual(sender._fast_ram_bytes, 0)
        self.assertEqual(sender.pending_count(), 0)
        self.assertEqual(len(Outbox(sender.outbox.path)), 0)

    def test_failed_real_fast_fsync_retains_ram_for_later_successful_spill(self):
        sender, _client = self.make_sender(self.slow_fast())
        sender.publish("factory/data", {"value": 1}, message_id="failed-fsync")
        ram_bytes = sender._fast_ram_bytes
        real_fsync = os.fsync
        first_call = True

        def fail_first_fsync(descriptor):
            nonlocal first_call
            if first_call:
                first_call = False
                raise OSError("injected fsync failure")
            return real_fsync(descriptor)

        with patch("reliomq.outbox.os.fsync", side_effect=fail_first_fsync):
            with sender._queue_lock:
                with self.assertRaises(OutboxError):
                    sender._spill_fast_batch_locked()
                self.assertEqual(len(sender._fast_queue), 1)
                self.assertEqual(sender._fast_queue[0].state.value, "ram_only")
                self.assertEqual(sender._fast_ram_bytes, ram_bytes)

        with sender._queue_lock:
            sender._spill_fast_batch_locked()

        self.assertEqual(sender._fast_ram_bytes, 0)
        self.assertEqual(self.pending_ids(Outbox(sender.outbox.path)), ["failed-fsync"])
        self.assertEqual(sender.pending_count(), 1)

    def test_fast_backlog_retains_payload_bytes_only_in_its_bounded_ram_suffix(self):
        mode = self.slow_fast(
            ram_max_messages=4,
            ram_max_bytes=100_000,
            high_watermark=1,
            spill_batch_messages=2,
        )
        sender, _client = self.make_sender(mode)
        for number in range(6):
            sender.publish(
                "factory/data",
                {"value": "x" * 20_000, "number": number},
                message_id=f"bounded-{number}",
            )

        # The disk index may grow past the RAM message limit; its entries
        # must not retain the payloads that were moved out of the RAM suffix.
        self.assertGreater(sender.pending_count(), mode.ram_max_messages)
        self.assertEqual(len(sender.outbox), 4)
        self.assertEqual(len(sender._fast_queue), 2)
        self.assertTrue(all(item._encoded is None for item in list(sender._pending)[:4]))
        self.assertTrue(all(item._encoded is not None for item in sender._fast_queue))
        retained_bytes = sum(len(item._encoded or b"") for item in sender._pending)
        self.assertEqual(retained_bytes, sender._fast_ram_bytes)
        self.assertLessEqual(retained_bytes, mode.ram_max_bytes)
        for entry in sender.outbox._pending:
            self.assertFalse(hasattr(entry, "envelope"))
            self.assertFalse(hasattr(entry, "payload"))
            self.assertFalse(hasattr(entry, "encoded"))
            self.assertFalse(hasattr(entry, "_encoded"))

        sender.stop()

        self.assertEqual(sender.pending_count(), 6)
        self.assertEqual(sender._fast_ram_bytes, 0)
        self.assertTrue(all(item._encoded is None for item in sender._pending))
        self.assertEqual(sender.outbox.get("bounded-0").payload["value"], "x" * 20_000)

    def test_sender_reopens_and_delivers_without_materializing_outbox_load(self):
        path = self.root / "lazy-recovery"
        original = Outbox(path)
        original.append_many(
            [
                MessageEnvelope(
                    message_id=f"lazy-{number}",
                    topic="factory/data",
                    payload={"number": number, "value": "z" * 20_000},
                )
                for number in range(3)
            ]
        )
        client = FakeClient()
        read_pending_ids = Outbox.pending_ids
        with (
            patch.object(Outbox, "load", side_effect=AssertionError("eager backlog load")),
            patch.object(Outbox, "pending_ids", autospec=True, side_effect=read_pending_ids) as ids,
        ):
            reopened = Sender(
                SenderConfig(host="source-broker", outbox_path=path),
                mode=self.slow_fast(),
                client_factory=client_factory_for(client),
            )
            self.addCleanup(reopened.stop)
            ids.assert_called_once_with(reopened.outbox)
            self.assertEqual(reopened.pending_count(), 3)
            self.assertTrue(all(item._encoded is None for item in reopened._pending))
            self.ready(reopened, client)
            self.acknowledge_every_publish(reopened, client)
            for _ in range(3):
                self.assertEqual(reopened._process_oldest_once(), DeliveryStatus.DELIVERED)
            self.assertEqual(reopened.pending_count(), 0)

        self.assertEqual(
            [MessageEnvelope.from_bytes(call["payload"]).message_id for call in client.publish_calls],
            ["lazy-0", "lazy-1", "lazy-2"],
        )

    def test_group_rotation_counts_only_new_active_record_as_unsynced(self):
        outbox = Outbox(self.root / "rotation-accounting", segment_max_records=2)
        sender, _client = self.make_sender(self.slow_group(), outbox=outbox)
        initial_generation = outbox.data_sync_generation
        for number in range(2):
            sender.publish("factory/data", number, message_id=f"rotation-{number}")
        self.assertEqual(sender._persistence.unsynced_messages, 2)
        self.assertEqual(outbox.data_sync_generation, initial_generation)

        sender.publish("factory/data", 2, message_id="rotation-2")

        self.assertGreater(outbox.data_sync_generation, initial_generation)
        self.assertEqual(sender._persistence.unsynced_messages, 1)
        self.assertEqual(
            sender._persistence.unsynced_bytes,
            outbox.record_size(outbox.get("rotation-2")),
        )
        generation_after_rotation = outbox.data_sync_generation
        sender.stop()

        self.assertGreater(outbox.data_sync_generation, generation_after_rotation)
        self.assertEqual(sender._persistence.unsynced_messages, 0)
        self.assertEqual(sender._persistence.unsynced_bytes, 0)
        self.assertEqual(
            self.pending_ids(Outbox(outbox.path)),
            ["rotation-0", "rotation-1", "rotation-2"],
        )


if __name__ == "__main__":
    unittest.main()

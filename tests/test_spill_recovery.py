from __future__ import annotations

import errno
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fakes import FakeClient, client_factory_for
from reliomq import FastMode, Sender, SenderConfig
from reliomq.outbox import Outbox, OutboxError
from reliomq.protocol import DeliveryAck, MessageEnvelope
from reliomq.sender import DeliveryStatus


class FastSpillRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "pending"

    def make_sender(self) -> tuple[Sender, FakeClient, Outbox]:
        outbox = Outbox(self.path, segment_max_records=1)
        client = FakeClient()
        sender = Sender(
            SenderConfig(
                host="test-broker",
                outbox_path=self.path,
                mqtt_puback_timeout=0.01,
                delivery_ack_timeout=0.01,
            ),
            mode=FastMode(
                ram_max_messages=100,
                high_watermark=1.0,
                max_ram_age=3600,
            ),
            outbox=outbox,
            client_factory=client_factory_for(client),
        )
        self.addCleanup(sender.stop)
        sender.publish("measurements", {"value": 1}, message_id="A")
        sender.publish("measurements", {"value": 2}, message_id="B")
        return sender, client, outbox

    def fail_spill_after_first_segment(self, sender: Sender, outbox: Outbox) -> None:
        original_write = outbox._write_chunk_locked
        calls = 0

        def fail_second_chunk(chunk, *, sync):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OutboxError("injected failure after first committed segment")
            return original_write(chunk, sync=sync)

        with patch.object(outbox, "_write_chunk_locked", side_effect=fail_second_chunk):
            with self.assertRaises(OutboxError):
                with sender._queue_lock:
                    sender._spill_fast_batch_locked()
        self.assertEqual([item.message_id for item in outbox.load()], ["A"])
        self.assertEqual(sender.pending_count(), 2)

    @staticmethod
    def make_ready_and_ack(sender: Sender, client: FakeClient) -> None:
        client.connected = True
        sender._connected.set()
        sender._ack_subscription_ready.set()

        def acknowledge(call):
            message_id = MessageEnvelope.from_bytes(call["payload"]).message_id
            client.emit_message(
                sender.config.delivery_ack_topic,
                DeliveryAck(message_id=message_id).to_bytes(),
            )

        client.publish_hook = acknowledge

    def test_ack_after_partial_spill_reconciles_disk_prefix(self) -> None:
        sender, client, outbox = self.make_sender()
        self.fail_spill_after_first_segment(sender, outbox)
        self.make_ready_and_ack(sender, client)

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual([item.message_id for item in outbox.load()], ["B"])
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(sender.pending_count(), 0)
        self.assertEqual(outbox.load(), [])
        self.assertEqual(Outbox(self.path).load(), [])

    def test_ack_retains_partial_spill_while_reconciliation_fails(self) -> None:
        sender, client, outbox = self.make_sender()
        self.fail_spill_after_first_segment(sender, outbox)
        self.make_ready_and_ack(sender, client)

        with patch.object(
            outbox, "append_many", side_effect=OutboxError("disk still unavailable")
        ):
            with self.assertLogs("reliomq.sender", level="ERROR"):
                self.assertEqual(
                    sender._process_oldest_once(), DeliveryStatus.STORE_ERROR
                )
        self.assertEqual(sender.pending_count(), 2)
        self.assertFalse(sender.wait_for_delivery("A", timeout=0))
        self.assertEqual([item.message_id for item in outbox.load()], ["A"])

        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(sender._process_oldest_once(), DeliveryStatus.DELIVERED)
        self.assertEqual(outbox.load(), [])


class FailedAppendRollbackTests(unittest.TestCase):
    def test_failed_rollback_cannot_corrupt_accepted_prefix_on_next_append(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pending"
            outbox = Outbox(path)
            first = MessageEnvelope(message_id="A", topic="x", payload=1)
            uncertain = MessageEnvelope(
                message_id="B", topic="x", payload="uncommitted" * 100
            )
            following = MessageEnvelope(message_id="C", topic="x", payload=3)
            outbox.append(first)
            segment = next(path.glob("segment-*.dat"))
            accepted_prefix = segment.read_bytes()
            original_open = Path.open

            class FailedWriteAndRollback:
                def __init__(self, file):
                    self.file = file

                def __enter__(self):
                    self.file.__enter__()
                    return self

                def __exit__(self, *args):
                    return self.file.__exit__(*args)

                def __getattr__(self, name):
                    return getattr(self.file, name)

                def write(self, data):
                    self.file.write(data)
                    self.file.flush()
                    raise OSError("injected write failure after bytes reached the file")

                def truncate(self, *args):
                    raise OSError("injected rollback failure")

            def faulty_open(candidate, mode="r", *args, **kwargs):
                file = original_open(candidate, mode, *args, **kwargs)
                if candidate == segment and mode == "r+b":
                    return FailedWriteAndRollback(file)
                return file

            with patch.object(Path, "open", faulty_open):
                with self.assertLogs("reliomq.outbox", level="WARNING"):
                    with self.assertRaises(OutboxError):
                        outbox.append(uncertain)

            self.assertTrue(segment.read_bytes().startswith(accepted_prefix))
            # Fail closed until an explicit reopen reconciles the unknown
            # tail. Overwriting it with a shorter message corrupts framing.
            with self.assertRaises(OutboxError):
                outbox.append(following)
            recovered = Outbox(path).load()
            self.assertEqual(recovered[0], first)
            self.assertNotIn(following, recovered)


class NamespaceRecoveryTests(unittest.TestCase):
    def test_reopen_syncs_installed_segment_and_ancestors_after_rotation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pending"
            outbox = Outbox(path, segment_max_records=1)
            first = MessageEnvelope(message_id="A", topic="x", payload=1)
            interrupted = MessageEnvelope(message_id="B", topic="x", payload=2)
            following = MessageEnvelope(message_id="C", topic="x", payload=3)
            outbox.append(first)

            with patch.object(
                outbox,
                "_fsync_directory",
                side_effect=OSError(errno.EIO, "directory sync failed after rename"),
            ):
                with self.assertRaises(OutboxError):
                    outbox.append(interrupted)
            self.assertEqual(len(list(path.glob("segment-*.dat"))), 2)

            # Opening must repair the uncertain namespace boundary before a
            # later file-only durable append can report acceptance.
            with patch.object(Outbox, "_fsync_directory", autospec=True) as sync:
                reopened = Outbox(path, segment_max_records=1)
                self.assertEqual(
                    [call.args[1] for call in sync.call_args_list],
                    [path, *path.parents],
                )
                self.assertTrue(reopened.append(following))
            self.assertEqual(reopened.load(), [first, following])

    def test_reopen_fails_if_either_required_namespace_sync_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pending"
            first = MessageEnvelope(message_id="A", topic="x", payload=1)
            outbox = Outbox(path)
            outbox.append(first)

            for failed_path in (path, path.parent):
                with self.subTest(failed_path=failed_path):
                    def fail_selected(_outbox, candidate):
                        if candidate == failed_path:
                            raise OSError(errno.EIO, "required namespace sync failed")

                    with patch.object(
                        Outbox, "_fsync_directory", autospec=True,
                        side_effect=fail_selected,
                    ):
                        with self.assertRaises(OutboxError):
                            Outbox(path)
            self.assertEqual(Outbox(path).load(), [first])

    def test_reopen_fsyncs_recovered_deferred_active_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pending"
            outbox = Outbox(path)
            first = MessageEnvelope(message_id="A", topic="x", payload=1)
            outbox.append(first, sync=False)
            segment = next(path.glob("segment-*.dat"))
            active_identity = (segment.stat().st_dev, segment.stat().st_ino)
            real_fsync = os.fsync
            active_sync_calls = []

            def record_fsync(descriptor):
                file_stat = os.fstat(descriptor)
                if (file_stat.st_dev, file_stat.st_ino) == active_identity:
                    active_sync_calls.append(descriptor)
                return real_fsync(descriptor)

            # The earlier process could have flushed GroupMode bytes into the
            # OS cache without completing fsync. Opening must establish their
            # data boundary before returning a usable recovered queue.
            with patch("reliomq.outbox.os.fsync", side_effect=record_fsync):
                reopened = Outbox(path)
                self.assertEqual(len(active_sync_calls), 1)
            self.assertEqual(reopened.load(), [first])


if __name__ == "__main__":
    unittest.main()

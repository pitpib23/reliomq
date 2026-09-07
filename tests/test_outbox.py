from __future__ import annotations

import errno
import json
import os
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest.mock import patch

from reliomq.outbox import (
    AppendResult,
    DurableMessageStore,
    Outbox,
    OutboxError,
    StoreError,
)
from reliomq.protocol import MessageEnvelope


def message(number: int, *, message_id: str | None = None) -> MessageEnvelope:
    return MessageEnvelope(
        message_id=message_id or f"event-{number}",
        topic="factory/data",
        payload={"sequence": number},
    )


def journal(*records: bytes) -> bytes:
    return b"".join(record + b"\n" for record in records)


def segment_paths(outbox: Outbox) -> list[Path]:
    return sorted(outbox.storage_path.glob("segment-*.dat"))


class OutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "pending.jsonl"

    def open_outbox(self, **kwargs: object) -> Outbox:
        return Outbox(self.path, **kwargs)

    def test_new_storage_is_a_segmented_directory(self) -> None:
        outbox = self.open_outbox()

        self.assertEqual(outbox.path, self.path)
        self.assertEqual(outbox.storage_path, self.path)
        self.assertTrue(self.path.is_dir())
        self.assertEqual(len(segment_paths(outbox)), 1)
        self.assertEqual(
            json.loads((self.path / "format.json").read_text(encoding="utf-8")),
            {"format": "reliomq-segmented-outbox", "version": 1},
        )

    def test_append_load_restart_and_fifo_removal(self) -> None:
        outbox = self.open_outbox()
        messages = [message(1), message(2), message(3)]
        for envelope in messages:
            self.assertTrue(outbox.append(envelope))

        restarted = self.open_outbox()
        self.assertEqual(restarted.load(), messages)
        self.assertEqual(restarted.peek_oldest(), messages[0])
        self.assertEqual(restarted.size(), 3)

        for index, envelope in enumerate(messages, start=1):
            self.assertTrue(restarted.remove_oldest(envelope))
            self.assertEqual(restarted.size(), 3 - index)

        self.assertEqual(self.open_outbox().load(), [])

    def test_remove_requires_exact_oldest_message(self) -> None:
        outbox = self.open_outbox()
        first, second = message(1), message(2)
        outbox.append(first)
        outbox.append(second)
        changed_first = MessageEnvelope(
            message_id=first.message_id,
            topic=first.topic,
            payload={"sequence": 999},
        )

        self.assertFalse(outbox.remove_oldest(second))
        self.assertFalse(outbox.remove_oldest(changed_first))
        self.assertEqual(outbox.load(), [first, second])

    def test_duplicate_pending_message_id_is_not_appended(self) -> None:
        outbox = self.open_outbox()
        first = message(1)
        conflicting = MessageEnvelope(
            message_id=first.message_id,
            topic="other/topic",
            payload="different",
        )

        self.assertTrue(outbox.append(first))
        self.assertFalse(outbox.append(conflicting))
        self.assertEqual(outbox.load(), [first])

    def test_append_snapshots_mutable_envelope_payload(self) -> None:
        outbox = self.open_outbox()
        payload = {"nested": [1]}
        supplied = MessageEnvelope(
            message_id="mutable-input",
            topic="factory/data",
            payload=payload,
        )
        expected = MessageEnvelope(
            message_id="mutable-input",
            topic="factory/data",
            payload={"nested": [1]},
        )

        outbox.append(supplied)
        payload["nested"].append(2)

        self.assertEqual(outbox.load(), [expected])
        self.assertTrue(outbox.remove_oldest(expected))

    def test_load_does_not_expose_mutable_internal_payload(self) -> None:
        outbox = self.open_outbox()
        expected = MessageEnvelope(
            message_id="mutable-output",
            topic="factory/data",
            payload={"nested": [1]},
        )
        outbox.append(expected)

        loaded = outbox.load()[0]
        assert isinstance(loaded.payload, dict)
        nested = loaded.payload["nested"]
        assert isinstance(nested, list)
        nested.append(2)

        self.assertEqual(outbox.peek_oldest(), expected)
        self.assertTrue(outbox.remove_oldest(expected))

    def test_append_many_is_idempotent_and_rejects_conflicting_content(self) -> None:
        outbox = self.open_outbox()
        first, second = message(1), message(2)

        result = outbox.append_many([first, first, second])

        self.assertIsInstance(result, AppendResult)
        self.assertEqual(result.appended_count, 2)
        self.assertEqual(
            result.bytes_written,
            Outbox.record_size(first) + Outbox.record_size(second),
        )
        self.assertEqual(outbox.load(), [first, second])
        self.assertEqual(outbox.append_many([first]).appended_count, 0)

        conflicting = message(99, message_id=first.message_id)
        with self.assertRaises(OutboxError):
            outbox.append_many([conflicting])

    def test_append_many_rotates_only_at_the_record_limit(self) -> None:
        outbox = self.open_outbox(segment_max_records=2)
        messages = [message(1), message(2), message(3)]

        result = outbox.append_many(messages)

        self.assertEqual(result.appended_count, 3)
        self.assertTrue(result.rotated)
        self.assertEqual(len(segment_paths(outbox)), 2)
        self.assertEqual(self.open_outbox(segment_max_records=2).load(), messages)

    def test_rotation_happens_before_an_individual_append_exceeds_limit(self) -> None:
        outbox = self.open_outbox(segment_max_records=2)
        messages = [message(1), message(2), message(3), message(4), message(5)]
        for envelope in messages:
            outbox.append(envelope)

        self.assertEqual(len(segment_paths(outbox)), 3)
        self.assertEqual(self.open_outbox(segment_max_records=2).load(), messages)

    def test_volatile_cursor_replays_until_explicit_checkpoint(self) -> None:
        outbox = self.open_outbox()
        first, second, third = message(1), message(2), message(3)
        for envelope in (first, second, third):
            outbox.append(envelope)

        self.assertTrue(outbox.remove_oldest(first, sync=False))
        self.assertEqual(outbox.load(), [second, third])
        self.assertEqual(self.open_outbox().load(), [first, second, third])

        outbox.checkpoint()

        self.assertEqual(self.open_outbox().load(), [second, third])

    def test_reusing_volatile_completed_id_forces_checkpoint_before_append(self) -> None:
        outbox = self.open_outbox()
        original = message(1, message_id="reused-id")
        replacement = message(2, message_id="reused-id")
        outbox.append(original)
        outbox.remove_oldest(original, sync=False)

        self.assertTrue(outbox.append(replacement))
        self.assertEqual(self.open_outbox().load(), [replacement])

    def test_durable_cursor_is_honored_after_restart(self) -> None:
        outbox = self.open_outbox()
        first, second = message(1), message(2)
        outbox.append(first)
        outbox.append(second)

        self.assertTrue(outbox.remove_oldest(first))

        restarted = self.open_outbox()
        self.assertEqual(restarted.load(), [second])
        self.assertEqual(restarted.peek_oldest(), second)

    def test_cleanup_deletes_only_fully_consumed_closed_segments(self) -> None:
        outbox = self.open_outbox(segment_max_records=2)
        messages = [message(number) for number in range(1, 6)]
        for envelope in messages:
            outbox.append(envelope)
        first_segment, second_segment, active_segment = segment_paths(outbox)

        self.assertTrue(outbox.remove_oldest(messages[0]))
        self.assertTrue(first_segment.exists())

        self.assertTrue(outbox.remove_oldest(messages[1]))
        self.assertFalse(first_segment.exists())
        self.assertTrue(second_segment.exists())
        self.assertTrue(active_segment.exists())

        self.assertTrue(outbox.remove_oldest(messages[2]))
        self.assertTrue(second_segment.exists())
        self.assertTrue(outbox.remove_oldest(messages[3]))
        self.assertFalse(second_segment.exists())

        self.assertTrue(outbox.remove_oldest(messages[4]))
        self.assertTrue(active_segment.exists())
        self.assertEqual(self.open_outbox(segment_max_records=2).load(), [])

    def test_failed_completion_checkpoint_does_not_delete_segment_or_head(self) -> None:
        outbox = self.open_outbox(segment_max_records=2)
        first, second, third = message(1), message(2), message(3)
        for envelope in (first, second, third):
            outbox.append(envelope)
        first_segment = segment_paths(outbox)[0]
        self.assertTrue(outbox.remove_oldest(first))
        checkpoint_path = outbox.storage_path / "checkpoint.json"
        checkpoint_before = checkpoint_path.read_bytes()

        with patch("reliomq.outbox.os.replace", side_effect=OSError("replace failed")):
            with self.assertRaises(OutboxError):
                outbox.remove_oldest(second)

        self.assertTrue(first_segment.exists())
        self.assertEqual(checkpoint_path.read_bytes(), checkpoint_before)
        self.assertEqual(outbox.load(), [second, third])
        self.assertEqual(
            self.open_outbox(segment_max_records=2).load(), [second, third]
        )

    def test_compact_checkpoints_volatile_progress_and_cleans_segments(self) -> None:
        outbox = self.open_outbox(segment_max_records=2)
        first, second, third = message(1), message(2), message(3)
        for envelope in (first, second, third):
            outbox.append(envelope)
        first_segment = segment_paths(outbox)[0]
        outbox.remove_oldest(first, sync=False)
        outbox.remove_oldest(second, sync=False)

        outbox.compact()

        self.assertFalse(first_segment.exists())
        self.assertEqual(self.open_outbox(segment_max_records=2).load(), [third])

    def test_torn_active_tail_is_repaired_to_last_record_boundary(self) -> None:
        outbox = self.open_outbox()
        first, lost, third = message(1), message(2), message(3)
        outbox.append(first)
        active_segment = segment_paths(outbox)[0]
        first_boundary = active_segment.stat().st_size
        outbox.append(lost)
        with active_segment.open("r+b") as file:
            file.truncate(active_segment.stat().st_size - 5)

        with self.assertLogs("reliomq.outbox", level="WARNING"):
            recovered = self.open_outbox()

        self.assertEqual(active_segment.stat().st_size, first_boundary)
        self.assertEqual(recovered.load(), [first])
        recovered.append(third)
        self.assertEqual(self.open_outbox().load(), [first, third])

    def test_bad_checksum_on_final_active_record_fails_conservatively(self) -> None:
        outbox = self.open_outbox()
        first, corrupted = message(1), message(2)
        outbox.append(first)
        active_segment = segment_paths(outbox)[0]
        outbox.append(corrupted)
        contents = bytearray(active_segment.read_bytes())
        contents[-1] ^= 0x01
        active_segment.write_bytes(contents)
        corrupt_size = active_segment.stat().st_size

        with self.assertRaisesRegex(OutboxError, "checksum mismatch"):
            self.open_outbox()
        self.assertEqual(active_segment.stat().st_size, corrupt_size)

    def test_corrupt_closed_segment_fails_conservatively(self) -> None:
        outbox = self.open_outbox(segment_max_records=1)
        outbox.append(message(1))
        outbox.append(message(2))
        closed_segment, _active_segment = segment_paths(outbox)
        contents = bytearray(closed_segment.read_bytes())
        contents[-1] ^= 0x01
        closed_segment.write_bytes(contents)

        with self.assertRaisesRegex(OutboxError, "checksum mismatch"):
            self.open_outbox(segment_max_records=1)

    def test_missing_middle_segment_fails_instead_of_skipping_messages(self) -> None:
        outbox = self.open_outbox(segment_max_records=1)
        for envelope in (message(1), message(2), message(3)):
            outbox.append(envelope)
        _first_segment, missing_segment, _active_segment = segment_paths(outbox)
        missing_segment.unlink()

        with self.assertRaisesRegex(OutboxError, "segment"):
            self.open_outbox(segment_max_records=1)

    def test_checkpoint_repairs_corrupt_checkpoint_metadata(self) -> None:
        outbox = self.open_outbox()
        envelope = message(1)
        outbox.append(envelope)
        checkpoint_path = outbox.storage_path / "checkpoint.json"
        checkpoint_path.write_bytes(b'{"version":')

        with self.assertLogs("reliomq.outbox", level="WARNING"):
            recovered = self.open_outbox()
        self.assertEqual(recovered.load(), [envelope])

        recovered.checkpoint()

        metadata = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        self.assertEqual(metadata["version"], 1)
        with self.assertNoLogs("reliomq.outbox", level="WARNING"):
            restarted = self.open_outbox()
        self.assertEqual(restarted.load(), [envelope])

    def test_legacy_envelope_jsonl_migrates_and_retains_source(self) -> None:
        first, second, third = message(1), message(2), message(3)
        source = journal(first.to_bytes(), second.to_bytes())
        self.path.write_bytes(source)

        recovered = self.open_outbox()

        self.assertEqual(recovered.path, self.path)
        self.assertEqual(recovered.storage_path, Path(f"{self.path}.segments"))
        self.assertEqual(recovered.load(), [first, second])
        self.assertEqual(self.path.read_bytes(), source)
        self.assertTrue(recovered.append(third))
        self.assertEqual(self.open_outbox().load(), [first, second, third])

    def test_legacy_v1_journal_replays_ack_and_migrates(self) -> None:
        first, second, third = message(1), message(2), message(3)
        source = journal(
            Outbox._encode_enqueue(first),
            Outbox._encode_enqueue(second),
            Outbox._encode_ack(first.message_id),
            Outbox._encode_enqueue(third),
        )
        self.path.write_bytes(source)

        recovered = self.open_outbox()

        self.assertEqual(recovered.load(), [second, third])
        self.assertEqual(self.path.read_bytes(), source)
        self.assertTrue(Path(f"{self.path}.segments").is_dir())

    def test_legacy_migration_skips_corrupt_records_and_torn_final_line(self) -> None:
        first, second = message(1), message(2)
        self.path.write_bytes(
            journal(
                first.to_bytes(),
                b'{"journal_version":1,"op":"unsupported"}',
                Outbox._encode_enqueue(second),
            )
            + b'{"journal_version":1'
        )

        with self.assertLogs("reliomq.outbox", level="WARNING") as captured:
            recovered = self.open_outbox()

        self.assertEqual(recovered.load(), [first, second])
        self.assertGreaterEqual(len(captured.output), 2)

    def test_migrated_sidecar_is_authoritative_on_later_opens(self) -> None:
        first, source_only = message(1), message(2)
        self.path.write_bytes(journal(first.to_bytes()))
        recovered = self.open_outbox()
        self.assertEqual(recovered.load(), [first])
        self.path.write_bytes(journal(first.to_bytes(), source_only.to_bytes()))

        self.assertEqual(self.open_outbox().load(), [first])

    def test_deferred_append_is_flushed_by_explicit_sync(self) -> None:
        outbox = self.open_outbox()
        envelope = message(1)
        with patch("reliomq.outbox.os.fsync", wraps=os.fsync) as fsync:
            self.assertTrue(outbox.append(envelope, sync=False))
            calls_after_append = fsync.call_count
            outbox.sync()

        self.assertEqual(calls_after_append, 0)
        self.assertGreaterEqual(fsync.call_count, 1)
        self.assertEqual(self.open_outbox().load(), [envelope])

    def test_invalid_existing_directory_is_not_treated_as_empty_queue(self) -> None:
        self.path.mkdir()

        with self.assertRaises(OutboxError):
            self.open_outbox()

    def test_directory_sync_propagates_real_io_errors(self) -> None:
        outbox = self.open_outbox()
        with patch("reliomq.outbox.os.open", side_effect=OSError(errno.EIO, "I/O failure")):
            with self.assertRaises(OSError) as captured:
                outbox._fsync_directory(outbox.storage_path)
        self.assertEqual(captured.exception.errno, errno.EIO)

    def test_unsupported_directory_sync_is_best_effort(self) -> None:
        outbox = self.open_outbox()
        with patch(
            "reliomq.outbox.os.open",
            side_effect=OSError(errno.EINVAL, "directory sync unsupported"),
        ):
            outbox._fsync_directory(outbox.storage_path)

    @unittest.skipUnless(os.name == "nt", "Windows CRT drive-root behavior")
    def test_windows_drive_root_open_enoent_is_treated_as_unsupported(self) -> None:
        outbox = self.open_outbox()
        root = Path(outbox.storage_path.anchor)
        with patch("reliomq.outbox.os.open", side_effect=FileNotFoundError(errno.ENOENT, "CRT")):
            outbox._fsync_directory(root)
            with self.assertRaises(FileNotFoundError):
                outbox._fsync_directory(outbox.storage_path / "missing")

    def test_cleanup_directory_sync_failure_does_not_undo_committed_ack(self) -> None:
        outbox = self.open_outbox(segment_max_records=1)
        first, second = message(1), message(2)
        outbox.append(first)
        outbox.append(second)
        # The required checkpoint barrier succeeds; only the later cleanup
        # barrier fails. The head must still advance consistently in memory.
        with patch.object(
            outbox, "_fsync_directory",
            side_effect=[None, OSError(errno.EIO, "cleanup sync failure")],
        ), self.assertLogs("reliomq.outbox", level="WARNING"):
            self.assertTrue(outbox.remove_oldest(first))
        self.assertEqual(outbox.load(), [second])
        self.assertEqual(self.open_outbox().load(), [second])

    def test_opening_an_outbox_logs_the_pending_count(self) -> None:
        outbox = self.open_outbox()
        outbox.append(message(1))

        with self.assertLogs("reliomq.outbox", level="INFO") as captured:
            self.open_outbox()

        self.assertTrue(
            any("Outbox opened" in line and "pending=1" in line for line in captured.output)
        )

    def test_contains_accepts_message_id_positionally(self) -> None:
        outbox = self.open_outbox()
        outbox.append(message(1))

        self.assertTrue(outbox.contains("event-1"))
        self.assertFalse(outbox.contains("event-missing"))

    def test_contains_event_id_keyword_still_works_and_warns(self) -> None:
        outbox = self.open_outbox()
        outbox.append(message(1))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertTrue(outbox.contains(event_id="event-1"))

        self.assertTrue(
            any(issubclass(warning.category, DeprecationWarning) for warning in caught)
        )


class DeprecatedOutboxAliasTests(unittest.TestCase):
    def test_durable_message_store_is_the_same_class_as_outbox(self) -> None:
        self.assertIs(DurableMessageStore, Outbox)

    def test_store_error_is_the_same_class_as_outbox_error(self) -> None:
        self.assertIs(StoreError, OutboxError)

    def test_old_module_path_still_importable(self) -> None:
        from reliomq.store import DurableMessageStore as ShimStore
        from reliomq.store import StoreError as ShimStoreError

        self.assertIs(ShimStore, Outbox)
        self.assertIs(ShimStoreError, OutboxError)


if __name__ == "__main__":
    unittest.main()

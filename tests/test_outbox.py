from __future__ import annotations

import tempfile
import unittest
import warnings
from pathlib import Path

from reliomq.outbox import DurableMessageStore, Outbox, OutboxError, StoreError
from reliomq.protocol import MessageEnvelope


def message(number: int) -> MessageEnvelope:
    return MessageEnvelope(
        message_id=f"event-{number}",
        topic="factory/data",
        payload={"sequence": number},
    )


class OutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "pending.jsonl"
        self.outbox = Outbox(self.path)

    def test_append_load_restart_and_fifo_removal(self) -> None:
        messages = [message(1), message(2), message(3)]
        for envelope in messages:
            self.assertTrue(self.outbox.append(envelope))

        restarted = Outbox(self.path)
        self.assertEqual(restarted.load(), messages)
        self.assertEqual(restarted.peek_oldest(), messages[0])
        self.assertEqual(restarted.size(), 3)

        for index, envelope in enumerate(messages, start=1):
            self.assertTrue(restarted.remove_oldest(envelope))
            self.assertEqual(restarted.size(), 3 - index)

    def test_remove_requires_exact_oldest_message(self) -> None:
        first, second = message(1), message(2)
        self.outbox.append(first)
        self.outbox.append(second)

        changed_first = MessageEnvelope(
            message_id=first.message_id,
            topic=first.topic,
            payload={"sequence": 999},
        )
        self.assertFalse(self.outbox.remove_oldest(second))
        self.assertFalse(self.outbox.remove_oldest(changed_first))
        self.assertEqual(self.outbox.load(), [first, second])

    def test_duplicate_pending_message_id_is_not_appended(self) -> None:
        first = message(1)
        conflicting = MessageEnvelope(
            message_id=first.message_id,
            topic="other/topic",
            payload="different",
        )

        self.assertTrue(self.outbox.append(first))
        self.assertFalse(self.outbox.append(conflicting))
        self.assertEqual(self.outbox.load(), [first])

    def test_corrupt_records_are_preserved_and_do_not_hide_valid_records(self) -> None:
        self.path.write_bytes(b"{not-json}\n")
        valid = message(1)
        with self.assertLogs("reliomq.outbox", level="WARNING"):
            self.assertTrue(self.outbox.append(valid))

        self.assertEqual(self.outbox.peek_oldest(), valid)
        self.assertTrue(self.outbox.remove_oldest(valid))
        self.assertEqual(self.path.read_bytes(), b"{not-json}\n")

    def test_append_after_torn_record_starts_a_new_line(self) -> None:
        self.path.write_bytes(b'{"version":1')
        valid = message(1)

        with self.assertLogs("reliomq.outbox", level="WARNING"):
            self.outbox.append(valid)

        contents = self.path.read_bytes()
        self.assertTrue(contents.startswith(b'{"version":1\n'))
        self.assertTrue(contents.endswith(valid.to_bytes() + b"\n"))
        self.assertEqual(self.outbox.load(), [valid])

    def test_read_errors_are_not_treated_as_an_empty_queue(self) -> None:
        self.path.mkdir()

        with self.assertRaises(OutboxError):
            self.outbox.peek_oldest()
        with self.assertRaises(OutboxError):
            self.outbox.size()

    def test_opening_an_outbox_logs_the_pending_count(self) -> None:
        self.outbox.append(message(1))

        with self.assertLogs("reliomq.outbox", level="INFO") as captured:
            Outbox(self.path)

        self.assertTrue(
            any("Outbox opened" in line and "pending=1" in line for line in captured.output)
        )

    def test_contains_accepts_message_id_positionally(self) -> None:
        self.outbox.append(message(1))

        self.assertTrue(self.outbox.contains("event-1"))
        self.assertFalse(self.outbox.contains("event-missing"))

    def test_contains_event_id_keyword_still_works_and_warns(self) -> None:
        self.outbox.append(message(1))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertTrue(self.outbox.contains(event_id="event-1"))

        self.assertTrue(
            any(issubclass(w.category, DeprecationWarning) for w in caught)
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

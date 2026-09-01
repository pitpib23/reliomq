from __future__ import annotations

import threading
import time
import unittest

from reliomq.ack import AckTracker


class AckTrackerTests(unittest.TestCase):
    def test_matching_id_wakes_the_waiter_with_a_true_result(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")

        self.assertTrue(tracker.match("event-1"))

        self.assertTrue(tracker.wait(timeout=1.0))

    def test_wrong_id_does_not_match_and_wait_times_out(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")

        self.assertFalse(tracker.match("some-other-event"))

        self.assertFalse(tracker.wait(timeout=0.05))

    def test_match_before_wait_is_not_lost(self) -> None:
        # begin() must run before the network publish so a same-thread or
        # near-simultaneous ACK callback can never race ahead of wait().
        tracker = AckTracker()
        tracker.begin("event-1")
        tracker.match("event-1")

        self.assertTrue(tracker.wait(timeout=1.0))

    def test_interrupt_wakes_the_waiter_without_reporting_a_match(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")

        def interrupt_soon() -> None:
            time.sleep(0.02)
            tracker.interrupt()

        threading.Thread(target=interrupt_soon).start()

        self.assertFalse(tracker.wait(timeout=5.0))

    def test_end_clears_expectation_so_a_later_ack_is_considered_late(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")
        tracker.match("event-1")
        tracker.wait(timeout=1.0)
        tracker.end()

        self.assertIsNone(tracker.expected_message_id)
        self.assertFalse(tracker.match("event-1"))

    def test_ack_arriving_after_timeout_boundary_is_not_reported_as_matched(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")

        # No match() call: wait() must time out and then close the window so
        # a callback that acquires the lock immediately afterward is late.
        self.assertFalse(tracker.wait(timeout=0.02))
        self.assertFalse(tracker.match("event-1"))

    def test_begin_while_already_active_raises(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")

        with self.assertRaises(RuntimeError):
            tracker.begin("event-2")

    def test_begin_rejects_empty_or_non_string_message_id(self) -> None:
        tracker = AckTracker()
        for value in ("", None, 123):
            with self.subTest(value=value), self.assertRaises(ValueError):
                tracker.begin(value)  # type: ignore[arg-type]

    def test_end_allows_a_fresh_begin_for_the_next_message(self) -> None:
        tracker = AckTracker()
        tracker.begin("event-1")
        tracker.end()

        tracker.begin("event-2")
        self.assertTrue(tracker.match("event-2"))
        self.assertTrue(tracker.wait(timeout=1.0))

    def test_concurrent_matches_from_multiple_threads_only_the_correct_id_wins(
        self,
    ) -> None:
        tracker = AckTracker()
        tracker.begin("winner")
        results: list[bool] = []

        def fire(message_id: str) -> None:
            results.append(tracker.match(message_id))

        threads = [
            threading.Thread(target=fire, args=(candidate_id,))
            for candidate_id in ("loser-a", "winner", "loser-b")
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sorted(results), [False, False, True])
        self.assertTrue(tracker.wait(timeout=1.0))


if __name__ == "__main__":
    unittest.main()

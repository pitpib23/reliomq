from __future__ import annotations

import dataclasses
import logging
import math
import unittest
from dataclasses import dataclass

from reliomq.durability import (
    DurableMode,
    FastMode,
    GroupMode,
    resolve_mode,
)
from reliomq.outbox import OutboxError
from reliomq.persistence import PersistencePolicy
from reliomq.protocol import MessageEnvelope


def message(number: int) -> MessageEnvelope:
    return MessageEnvelope(
        message_id=f"persistence-{number}",
        topic="factory/data",
        payload={"sequence": number},
    )


@dataclass(frozen=True)
class _AppendResult:
    appended_count: int
    bytes_written: int
    rotated: bool


class _FakeOutbox:
    def __init__(self) -> None:
        self.append_calls: list[tuple[MessageEnvelope, bool]] = []
        self.append_many_calls: list[tuple[list[MessageEnvelope], bool]] = []
        self.remove_calls: list[tuple[MessageEnvelope, bool]] = []
        self.events: list[str] = []
        self.sync_calls = 0
        self.message_fsync_calls = 0
        self.checkpoint_calls = 0
        self.record_bytes = 100
        self.closed_segment_pending = False
        self.sync_errors: list[Exception] = []
        self.checkpoint_errors: list[Exception] = []

    def record_size(self, _envelope: MessageEnvelope) -> int:
        return self.record_bytes

    def append(self, envelope: MessageEnvelope, *, sync: bool = True) -> bool:
        self.append_calls.append((envelope, sync))
        self.events.append(f"append:{sync}")
        if sync:
            self.message_fsync_calls += 1
        return True

    def append_many(
        self, envelopes: list[MessageEnvelope], *, sync: bool = True
    ) -> _AppendResult:
        copied = list(envelopes)
        self.append_many_calls.append((copied, sync))
        self.events.append(f"append_many:{sync}")
        return _AppendResult(len(copied), len(copied) * self.record_bytes, False)

    def sync(self) -> None:
        self.sync_calls += 1
        self.events.append("sync")
        if self.sync_errors:
            raise self.sync_errors.pop(0)

    def remove_oldest(
        self, envelope: MessageEnvelope, *, sync: bool = True
    ) -> bool:
        self.remove_calls.append((envelope, sync))
        self.events.append(f"remove:{sync}")
        return True

    def checkpoint(self) -> None:
        self.checkpoint_calls += 1
        self.events.append("checkpoint")
        if self.checkpoint_errors:
            raise self.checkpoint_errors.pop(0)

    def completed_closed_segment_pending(self) -> bool:
        return self.closed_segment_pending


class _FakeTimer:
    def __init__(self, interval: float, callback) -> None:
        self.interval = interval
        self.callback = callback
        self.name = ""
        self.daemon = False
        self.started = False
        self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        self.callback()


class _TimerFactory:
    def __init__(self) -> None:
        self.timers: list[_FakeTimer] = []

    def __call__(self, interval: float, callback) -> _FakeTimer:
        timer = _FakeTimer(interval, callback)
        self.timers.append(timer)
        return timer


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class ModeClassTests(unittest.TestCase):
    def test_defaults_are_exact_and_modes_are_frozen(self) -> None:
        self.assertEqual(DurableMode(), DurableMode())
        self.assertEqual(
            GroupMode(),
            GroupMode(
                sync_messages=20,
                sync_interval=0.25,
                sync_bytes=65_536,
                ack_checkpoint_messages=50,
                ack_checkpoint_interval=1.0,
            ),
        )
        self.assertEqual(
            FastMode(),
            FastMode(
                ram_max_messages=10_000,
                ram_max_bytes=32 * 1024 * 1024,
                high_watermark=0.75,
                max_ram_age=5.0,
                disconnect_grace=3.0,
                spill_batch_messages=1_000,
                spill_batch_bytes=4 * 1024 * 1024,
            ),
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            GroupMode().sync_messages = 2  # type: ignore[misc]

    def test_group_accepts_custom_values_and_normalizes_durations(self) -> None:
        mode = GroupMode(
            sync_messages=7,
            sync_interval=2,
            sync_bytes=1234,
            ack_checkpoint_messages=9,
            ack_checkpoint_interval=3,
        )
        self.assertEqual(mode.sync_messages, 7)
        self.assertEqual(mode.sync_interval, 2.0)
        self.assertEqual(mode.sync_bytes, 1234)
        self.assertEqual(mode.ack_checkpoint_messages, 9)
        self.assertEqual(mode.ack_checkpoint_interval, 3.0)

    def test_group_rejects_invalid_thresholds(self) -> None:
        integer_fields = (
            "sync_messages",
            "sync_bytes",
            "ack_checkpoint_messages",
        )
        for field in integer_fields:
            for value in (0, -1, True, 1.0, "1"):
                with self.subTest(field=field, value=value), self.assertRaises(
                    ValueError
                ):
                    GroupMode(**{field: value})

        time_fields = ("sync_interval", "ack_checkpoint_interval")
        for field in time_fields:
            for value in (0, -1, True, "1", math.nan, math.inf, -math.inf):
                with self.subTest(field=field, value=value), self.assertRaises(
                    ValueError
                ):
                    GroupMode(**{field: value})

    def test_fast_accepts_custom_values_and_zero_disconnect_grace(self) -> None:
        mode = FastMode(
            ram_max_messages=7,
            ram_max_bytes=2048,
            high_watermark=1,
            max_ram_age=2,
            disconnect_grace=0,
            spill_batch_messages=3,
            spill_batch_bytes=1024,
        )
        self.assertEqual(mode.high_watermark, 1.0)
        self.assertEqual(mode.max_ram_age, 2.0)
        self.assertEqual(mode.disconnect_grace, 0.0)

    def test_fast_rejects_invalid_integer_fields(self) -> None:
        fields = (
            "ram_max_messages",
            "ram_max_bytes",
            "spill_batch_messages",
            "spill_batch_bytes",
        )
        for field in fields:
            for value in (0, -1, True, 1.0, "1"):
                with self.subTest(field=field, value=value), self.assertRaises(
                    ValueError
                ):
                    FastMode(**{field: value})

    def test_fast_rejects_invalid_ratio_and_time_fields(self) -> None:
        for value in (0, -0.1, 1.01, True, "0.5", math.nan, math.inf):
            with self.subTest(field="high_watermark", value=value), self.assertRaises(
                ValueError
            ):
                FastMode(high_watermark=value)

        for field in ("max_ram_age",):
            for value in (0, -1, True, "1", math.nan, math.inf):
                with self.subTest(field=field, value=value), self.assertRaises(
                    ValueError
                ):
                    FastMode(**{field: value})

        for value in (-1, True, "1", math.nan, math.inf):
            with self.subTest(field="disconnect_grace", value=value), self.assertRaises(
                ValueError
            ):
                FastMode(disconnect_grace=value)

    def test_resolve_mode_defaults_and_rejects_non_instances(self) -> None:
        self.assertEqual(resolve_mode(None), DurableMode())
        mode = GroupMode()
        self.assertIs(resolve_mode(mode), mode)
        for invalid in (DurableMode, "durable", object(), True):
            with self.subTest(invalid=invalid), self.assertRaises(TypeError):
                resolve_mode(invalid)  # type: ignore[arg-type]


class PersistencePolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.outbox = _FakeOutbox()
        self.clock = _Clock()
        self.timers = _TimerFactory()
        self.logger = logging.getLogger("tests.persistence")

    def policy(self, mode=None) -> PersistencePolicy:
        return PersistencePolicy(
            self.outbox,  # type: ignore[arg-type]
            mode or GroupMode(),
            logger=self.logger,
            clock=self.clock,
            timer_factory=self.timers,
        )

    def test_durable_append_and_completion_use_immediate_sync_contract(self) -> None:
        policy = self.policy(DurableMode())
        envelope = message(1)

        self.assertTrue(policy.append(envelope))
        self.assertTrue(policy.complete(envelope))

        self.assertEqual(self.outbox.append_calls, [(envelope, True)])
        self.assertEqual(self.outbox.remove_calls, [(envelope, True)])
        self.assertEqual(self.timers.timers, [])

    def test_durable_write_amplification_is_one_message_fsync_per_append(self) -> None:
        policy = self.policy(DurableMode())

        for number in range(100):
            self.assertTrue(policy.append(message(number)))

        self.assertEqual(len(self.outbox.append_calls), 100)
        self.assertEqual(self.outbox.message_fsync_calls, 100)
        self.assertEqual(self.outbox.sync_calls, 0)

    def test_fast_persistence_is_only_available_through_spill_helpers(self) -> None:
        policy = self.policy(FastMode())
        envelopes = [message(1), message(2)]

        with self.assertRaisesRegex(RuntimeError, "RAM queue"):
            policy.append(envelopes[0])
        self.assertTrue(policy.append_durable(envelopes[0]))
        result = policy.append_many_durable(envelopes)

        self.assertEqual(self.outbox.append_calls, [(envelopes[0], True)])
        self.assertEqual(self.outbox.append_many_calls, [(envelopes, True)])
        self.assertEqual(result.appended_count, 2)

    def test_group_appends_each_record_without_individual_fsync(self) -> None:
        policy = self.policy(GroupMode(sync_messages=3, sync_bytes=1000))

        policy.append(message(1))
        policy.append(message(2))

        self.assertEqual([sync for _, sync in self.outbox.append_calls], [False, False])
        self.assertEqual(self.outbox.sync_calls, 0)
        self.assertEqual(policy.unsynced_messages, 2)
        self.assertEqual(policy.unsynced_bytes, 200)

    def test_group_message_count_trigger_shares_one_sync(self) -> None:
        policy = self.policy(GroupMode(sync_messages=2, sync_bytes=1000))

        policy.append(message(1))
        first_timer = self.timers.timers[-1]
        policy.append(message(2))

        self.assertEqual(self.outbox.sync_calls, 1)
        self.assertEqual(policy.unsynced_messages, 0)
        self.assertTrue(first_timer.cancelled)

    def test_group_write_amplification_is_five_syncs_for_100_messages(self) -> None:
        policy = self.policy(
            GroupMode(
                sync_messages=20,
                sync_interval=60.0,
                sync_bytes=10_000_000,
            )
        )

        for number in range(100):
            self.assertTrue(policy.append(message(number)))

        self.assertEqual(len(self.outbox.append_calls), 100)
        self.assertEqual(self.outbox.message_fsync_calls, 0)
        self.assertEqual(self.outbox.sync_calls, 5)

    def test_group_byte_trigger_uses_framed_record_size(self) -> None:
        self.outbox.record_bytes = 60
        policy = self.policy(GroupMode(sync_messages=99, sync_bytes=120))

        policy.append(message(1))
        policy.append(message(2))

        self.assertEqual(self.outbox.sync_calls, 1)

    def test_group_elapsed_trigger_is_measured_from_last_completed_sync(self) -> None:
        policy = self.policy(
            GroupMode(sync_messages=99, sync_interval=0.25, sync_bytes=9999)
        )
        self.clock.now = 0.3

        policy.append(message(1))

        self.assertEqual(self.outbox.sync_calls, 1)
        self.assertEqual(policy.unsynced_messages, 0)

    def test_group_timer_syncs_without_another_publish(self) -> None:
        policy = self.policy(
            GroupMode(sync_messages=99, sync_interval=0.25, sync_bytes=9999)
        )
        policy.append(message(1))
        timer = self.timers.timers[-1]

        self.assertEqual(timer.interval, 0.25)
        self.assertEqual(timer.name, "reliomq-group-data-sync")
        self.assertTrue(timer.daemon)
        timer.fire()

        self.assertEqual(self.outbox.sync_calls, 1)

    def test_failed_data_trigger_retains_counters_and_retries(self) -> None:
        self.outbox.sync_errors.append(OutboxError("disk unavailable"))
        policy = self.policy(GroupMode(sync_messages=1, sync_interval=0.5))

        with self.assertLogs("tests.persistence", level="ERROR"):
            self.assertTrue(policy.append(message(1)))

        self.assertEqual(policy.unsynced_messages, 1)
        retry = self.timers.timers[-1]
        self.assertEqual(retry.interval, 0.5)
        retry.fire()
        self.assertEqual(policy.unsynced_messages, 0)
        self.assertEqual(self.outbox.sync_calls, 2)

    def test_group_ack_count_trigger_checkpoints_separately(self) -> None:
        policy = self.policy(
            GroupMode(
                sync_messages=99,
                sync_bytes=9999,
                ack_checkpoint_messages=2,
            )
        )
        first, second = message(1), message(2)

        self.assertTrue(policy.complete(first))
        self.assertEqual(self.outbox.checkpoint_calls, 0)
        self.assertEqual(policy.acked_since_checkpoint, 1)
        self.assertTrue(policy.complete(second))

        self.assertEqual(
            self.outbox.remove_calls, [(first, False), (second, False)]
        )
        self.assertEqual(self.outbox.checkpoint_calls, 1)
        self.assertEqual(policy.acked_since_checkpoint, 0)

    def test_group_ack_timer_checkpoints_without_another_ack(self) -> None:
        policy = self.policy(GroupMode(ack_checkpoint_interval=0.75))
        policy.complete(message(1))
        timer = self.timers.timers[-1]

        self.assertEqual(timer.interval, 0.75)
        self.assertEqual(timer.name, "reliomq-group-ack-checkpoint")
        timer.fire()

        self.assertEqual(self.outbox.checkpoint_calls, 1)

    def test_closed_segment_forces_group_checkpoint(self) -> None:
        self.outbox.closed_segment_pending = True
        policy = self.policy(GroupMode(ack_checkpoint_messages=99))

        policy.complete(message(1))

        self.assertEqual(self.outbox.checkpoint_calls, 1)

    def test_successful_checkpoint_also_covers_pending_data(self) -> None:
        policy = self.policy(
            GroupMode(
                sync_messages=99,
                sync_bytes=9999,
                ack_checkpoint_messages=1,
            )
        )
        policy.append(message(1))
        self.assertEqual(policy.unsynced_messages, 1)

        policy.complete(message(1))

        self.assertEqual(self.outbox.checkpoint_calls, 1)
        self.assertEqual(policy.unsynced_messages, 0)
        self.assertEqual(policy.acked_since_checkpoint, 0)

    def test_failed_ack_checkpoint_retains_progress_and_retries(self) -> None:
        self.outbox.checkpoint_errors.append(OutboxError("metadata unavailable"))
        policy = self.policy(GroupMode(ack_checkpoint_messages=1))

        with self.assertLogs("tests.persistence", level="ERROR"):
            self.assertTrue(policy.complete(message(1)))

        self.assertEqual(policy.acked_since_checkpoint, 1)
        retry = self.timers.timers[-1]
        self.assertEqual(retry.interval, 1.0)
        retry.fire()
        self.assertEqual(self.outbox.checkpoint_calls, 2)
        self.assertEqual(policy.acked_since_checkpoint, 0)

    def test_flush_syncs_data_before_checkpoint_and_is_idempotent(self) -> None:
        policy = self.policy(
            GroupMode(sync_messages=99, sync_bytes=9999, ack_checkpoint_messages=99)
        )
        policy.append(message(1))
        policy.complete(message(1))

        policy.flush()
        policy.flush()

        self.assertEqual(self.outbox.events[-2:], ["sync", "checkpoint"])
        self.assertEqual(self.outbox.sync_calls, 1)
        self.assertEqual(self.outbox.checkpoint_calls, 1)

    def test_flush_surfaces_sync_failure_and_retains_dirty_range(self) -> None:
        policy = self.policy(GroupMode(sync_messages=99, sync_bytes=9999))
        policy.append(message(1))
        self.outbox.sync_errors.append(OutboxError("shutdown sync failed"))

        with self.assertRaisesRegex(OutboxError, "shutdown sync failed"):
            policy.flush()

        self.assertEqual(policy.unsynced_messages, 1)

    def test_cancelled_timer_callback_cannot_repeat_completed_sync(self) -> None:
        policy = self.policy(GroupMode(sync_messages=2, sync_bytes=9999))
        policy.append(message(1))
        stale_timer = self.timers.timers[-1]
        policy.append(message(2))
        self.assertTrue(stale_timer.cancelled)

        stale_timer.fire()

        self.assertEqual(self.outbox.sync_calls, 1)


if __name__ == "__main__":
    unittest.main()

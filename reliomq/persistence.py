"""Client-level persistence policy for :class:`reliomq.sender.Sender`."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from typing import Protocol

from .durability import DeliveryMode, DurableMode, FastMode, GroupMode
from .outbox import AppendResult, Outbox, OutboxError
from .protocol import MessageEnvelope


class _Timer(Protocol):
    """The small subset shared by ``threading.Timer`` and test doubles."""

    name: str
    daemon: bool

    def start(self) -> None: ...

    def cancel(self) -> None: ...


_TimerFactory = Callable[[float, Callable[[], None]], _Timer]


class PersistencePolicy:
    """Coordinate one immutable client mode with a segmented Outbox.

    The Outbox owns record framing, segment rotation, filesystem locking, and
    cursor persistence. This coordinator owns the policy decisions about when
    GroupMode data and ACK progress cross their independent durability
    boundaries. FastMode's RAM queue remains Sender-owned; this class only
    supplies durable single/batch operations for spill and recovered records.
    """

    def __init__(
        self,
        outbox: Outbox,
        mode: DeliveryMode,
        *,
        logger: logging.Logger | None = None,
        clock: Callable[[], float] = time.monotonic,
        timer_factory: _TimerFactory = threading.Timer,
    ) -> None:
        if type(mode) not in (DurableMode, GroupMode, FastMode):
            raise TypeError(
                "mode must be a DurableMode, GroupMode, or FastMode instance"
            )

        self._outbox = outbox
        self._mode = mode
        self._logger = logger or logging.getLogger(__name__)
        self._clock = clock
        self._timer_factory = timer_factory
        self._lock = threading.RLock()

        now = clock()
        self._unsynced_messages = 0
        self._unsynced_bytes = 0
        self._last_data_sync = now
        self._acked_since_checkpoint = 0
        self._last_ack_checkpoint = now

        self._data_timer: _Timer | None = None
        self._ack_timer: _Timer | None = None
        self._data_timer_generation = 0
        self._ack_timer_generation = 0

        if isinstance(mode, GroupMode):
            required = (
                "record_size",
                "sync",
                "checkpoint",
                "completed_closed_segment_pending",
            )
            missing = [
                name
                for name in required
                if not callable(getattr(outbox, name, None))
            ]
            if missing:
                methods = ", ".join(f"outbox.{name}()" for name in missing)
                raise OutboxError(f"GroupMode requires {methods}")

    @property
    def mode(self) -> DeliveryMode:
        """The immutable mode selected when this policy was constructed."""

        return self._mode

    @property
    def unsynced_messages(self) -> int:
        """Number of GroupMode appends not covered by a known policy sync."""

        with self._lock:
            return self._unsynced_messages

    @property
    def unsynced_bytes(self) -> int:
        """Framed GroupMode bytes not covered by a known policy sync."""

        with self._lock:
            return self._unsynced_bytes

    @property
    def acked_since_checkpoint(self) -> int:
        """GroupMode ACK advances not yet covered by a known checkpoint."""

        with self._lock:
            return self._acked_since_checkpoint

    def append(self, envelope: MessageEnvelope) -> bool:
        """Accept one persistent message according to this client's mode.

        FastMode publication is intentionally not handled here: it must first
        enter Sender's bounded RAM queue. Use :meth:`append_durable` or
        :meth:`append_many_durable` only when that queue performs a spill.
        """

        if isinstance(self._mode, DurableMode):
            return self.append_durable(envelope)
        if isinstance(self._mode, GroupMode):
            return self._append_group(envelope)
        raise RuntimeError(
            "FastMode messages must enter the Sender RAM queue before spill"
        )

    def append_durable(self, envelope: MessageEnvelope) -> bool:
        """Append and fsync one message, used by DurableMode and Fast spill."""

        # Keep the historical call shape for simple Outbox-compatible doubles.
        return self._outbox.append(envelope)

    def append_many_durable(
        self, envelopes: Sequence[MessageEnvelope]
    ) -> AppendResult:
        """Append a FastMode spill batch with one durable batch boundary.

        The concrete return value is the Outbox ``AppendResult`` containing
        ``appended_count``, ``bytes_written``, and ``rotated``.
        """

        append_many = getattr(self._outbox, "append_many", None)
        if not callable(append_many):
            raise OutboxError("FastMode spill requires outbox.append_many()")
        return append_many(envelopes, sync=True)

    def complete(self, envelope: MessageEnvelope) -> bool:
        """Advance the head after an authoritative DeliveryAck.

        DurableMode and disk-backed FastMode persist the cursor before return.
        GroupMode advances its runtime cursor immediately and checkpoints it
        only on its independent count/time/closed-segment triggers.
        """

        if not isinstance(self._mode, GroupMode):
            # Preserve the established default call shape for compatible
            # injected Outboxes while retaining sync=True semantics.
            return self._outbox.remove_oldest(envelope)

        with self._lock:
            removed = self._outbox.remove_oldest(envelope, sync=False)
            if not removed:
                return False

            self._acked_since_checkpoint += 1
            now = self._clock()
            closed_segment = bool(
                self._outbox.completed_closed_segment_pending()
            )
            due = (
                self._acked_since_checkpoint
                >= self._mode.ack_checkpoint_messages
                or now - self._last_ack_checkpoint
                >= self._mode.ack_checkpoint_interval
                or closed_segment
            )
            if due:
                self._checkpoint_group_locked(suppress_errors=True)
            else:
                self._arm_ack_timer_locked()
            return True

    def checkpoint(self) -> None:
        """Force pending GroupMode ACK progress to stable storage."""

        if not isinstance(self._mode, GroupMode):
            self._outbox.checkpoint()
            return
        with self._lock:
            self._checkpoint_group_locked(suppress_errors=False)

    def flush(self) -> None:
        """Finish all pending GroupMode durability work for clean shutdown.

        Timer generations are invalidated first. The policy is intentionally
        reusable: a later publish after a stop/restart may arm fresh timers.
        """

        if not isinstance(self._mode, GroupMode):
            return

        with self._lock:
            self._cancel_data_timer_locked()
            self._cancel_ack_timer_locked()
            if self._unsynced_messages:
                self._sync_group_data_locked(suppress_errors=False)
            if self._acked_since_checkpoint:
                self._checkpoint_group_locked(suppress_errors=False)

    def _append_group(self, envelope: MessageEnvelope) -> bool:
        mode = self._mode
        assert isinstance(mode, GroupMode)

        with self._lock:
            # Calculate before append so an accounting error cannot make an
            # already accepted record appear rejected to its caller.
            framed_bytes = self._outbox.record_size(envelope)
            data_generation = getattr(self._outbox, "data_sync_generation", None)
            ack_generation = getattr(self._outbox, "checkpoint_generation", None)
            appended = self._outbox.append(envelope, sync=False)
            if not appended:
                return False

            now = self._clock()
            if data_generation != getattr(self._outbox, "data_sync_generation", None):
                # Rotation (or safe ID reuse) synced the previous append range.
                # The just-appended message belongs to the new unsynced range.
                self._cancel_data_timer_locked()
                self._unsynced_messages = 0
                self._unsynced_bytes = 0
                self._last_data_sync = now
            if ack_generation != getattr(self._outbox, "checkpoint_generation", None):
                self._cancel_ack_timer_locked()
                self._acked_since_checkpoint = 0
                self._last_ack_checkpoint = now
            self._unsynced_messages += 1
            self._unsynced_bytes += framed_bytes
            due = (
                self._unsynced_messages >= mode.sync_messages
                or self._unsynced_bytes >= mode.sync_bytes
                or now - self._last_data_sync >= mode.sync_interval
            )
            if due:
                self._sync_group_data_locked(suppress_errors=True)
            else:
                self._arm_data_timer_locked()
            if self._outbox.completed_closed_segment_pending():
                self._checkpoint_group_locked(suppress_errors=True)
            return True

    def _sync_group_data_locked(self, *, suppress_errors: bool) -> None:
        self._cancel_data_timer_locked()
        if not self._unsynced_messages:
            return

        try:
            self._outbox.sync()
        except OutboxError:
            if not suppress_errors:
                raise
            self._logger.exception(
                "GroupMode data sync failed; the unsynced range is retained"
            )
            self._arm_data_timer_locked(retry=True)
            return

        self._unsynced_messages = 0
        self._unsynced_bytes = 0
        self._last_data_sync = self._clock()

    def _checkpoint_group_locked(self, *, suppress_errors: bool) -> None:
        self._cancel_ack_timer_locked()
        if not self._acked_since_checkpoint:
            return

        try:
            # Outbox.checkpoint() first syncs segment data through the target
            # cursor, then atomically persists the cursor and safely reclaims
            # any fully consumed closed segments.
            self._outbox.checkpoint()
        except OutboxError:
            if not suppress_errors:
                raise
            self._logger.exception(
                "GroupMode ACK checkpoint failed; restart may replay ACKed messages"
            )
            self._arm_ack_timer_locked(retry=True)
            return

        now = self._clock()
        self._acked_since_checkpoint = 0
        self._last_ack_checkpoint = now

        # A successful checkpoint establishes a data sync boundary too.
        self._cancel_data_timer_locked()
        self._unsynced_messages = 0
        self._unsynced_bytes = 0
        self._last_data_sync = now

    def _arm_data_timer_locked(self, *, retry: bool = False) -> None:
        if self._data_timer is not None or not self._unsynced_messages:
            return
        mode = self._mode
        assert isinstance(mode, GroupMode)
        delay = (
            mode.sync_interval
            if retry
            else max(
                0.0,
                self._last_data_sync + mode.sync_interval - self._clock(),
            )
        )
        self._data_timer_generation += 1
        generation = self._data_timer_generation
        timer = self._timer_factory(
            delay, lambda: self._on_data_timer(generation)
        )
        timer.name = "reliomq-group-data-sync"
        timer.daemon = True
        self._data_timer = timer
        timer.start()

    def _arm_ack_timer_locked(self, *, retry: bool = False) -> None:
        if self._ack_timer is not None or not self._acked_since_checkpoint:
            return
        mode = self._mode
        assert isinstance(mode, GroupMode)
        delay = (
            mode.ack_checkpoint_interval
            if retry
            else max(
                0.0,
                self._last_ack_checkpoint
                + mode.ack_checkpoint_interval
                - self._clock(),
            )
        )
        self._ack_timer_generation += 1
        generation = self._ack_timer_generation
        timer = self._timer_factory(
            delay, lambda: self._on_ack_timer(generation)
        )
        timer.name = "reliomq-group-ack-checkpoint"
        timer.daemon = True
        self._ack_timer = timer
        timer.start()

    def _cancel_data_timer_locked(self) -> None:
        self._data_timer_generation += 1
        timer = self._data_timer
        self._data_timer = None
        if timer is not None:
            timer.cancel()

    def _cancel_ack_timer_locked(self) -> None:
        self._ack_timer_generation += 1
        timer = self._ack_timer
        self._ack_timer = None
        if timer is not None:
            timer.cancel()

    def _on_data_timer(self, generation: int) -> None:
        with self._lock:
            if generation != self._data_timer_generation:
                return
            self._data_timer = None
            self._sync_group_data_locked(suppress_errors=True)

    def _on_ack_timer(self, generation: int) -> None:
        with self._lock:
            if generation != self._ack_timer_generation:
                return
            self._ack_timer = None
            self._checkpoint_group_locked(suppress_errors=True)

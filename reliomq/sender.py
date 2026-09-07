"""Durable, application-acknowledged MQTT sender."""

from __future__ import annotations

import logging
import math
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any

import paho.mqtt.client as mqtt

from ._compat import resolve_renamed_argument, warn_deprecated_attribute
from .ack import AckTracker
from .config import SenderConfig
from .durability import (
    DeliveryMode,
    DurableMode,
    FastMode,
    FastQueueFullError,
    GroupMode,
    resolve_mode,
)
from .mqtt import (
    ClientFactory,
    confirmed_publish,
    create_client,
    reason_code_is_success,
    suback_is_success,
)
from .observability import enable_logging
from .outbox import Outbox, OutboxError
from .persistence import PersistencePolicy
from .protocol import DeliveryAck, MessageEnvelope, ProtocolError


logger = logging.getLogger(__name__)


class DeliveryStatus(str, Enum):
    """Result of one deterministic oldest-message delivery attempt."""

    EMPTY = "empty"
    NOT_READY = "not_ready"
    DELIVERED = "delivered"
    RETRY = "retry"
    STORE_ERROR = "store_error"
    STOPPED = "stopped"


@dataclass(slots=True)
class _PendingMessage:
    """FIFO metadata with payload bytes owned only while a message is in RAM."""

    message_id: str
    _encoded: bytes | None
    outbox: Outbox
    encoded_size: int
    enqueued_at: float
    state: _StorageState
    spill_attempted: bool = False

    @property
    def encoded(self) -> bytes:
        if self._encoded is not None:
            return self._encoded
        getter = getattr(self.outbox, "get", None)
        envelope = (
            getter(self.message_id)
            if callable(getter)
            else next(
                (item for item in self.outbox.load() if item.message_id == self.message_id),
                None,
            )
        )
        if envelope is None:
            raise OutboxError(f"pending message missing from Outbox: {self.message_id!r}")
        return envelope.to_bytes()

    @property
    def envelope(self) -> MessageEnvelope:
        return MessageEnvelope.from_bytes(self.encoded)

    @property
    def persisted(self) -> bool:
        return self.state is _StorageState.DISK_BACKED


class _StorageState(str, Enum):
    """FastMode ownership state; persistent modes stay DISK_BACKED."""

    RAM_ONLY = "ram_only"
    PERSISTING = "persisting"
    DISK_BACKED = "disk_backed"


class Sender:
    """Publish JSON messages with durable FIFO retry and correlated ACKs.

    This is reliomq's main entry point. Each instance selects exactly one
    immutable client-level mode for its lifetime. :class:`FastMode` is the
    default and starts new work in bounded RAM, spilling on configured safety
    triggers. :class:`DurableMode` is the explicit crash-safe policy that
    fsyncs the complete envelope before :meth:`publish` returns.
    :class:`GroupMode` appends every envelope immediately but batches fsync
    and ACK checkpoints. Use separate Sender instances when traffic needs
    separate policies. Recovered records always remain disk-backed. A
    persisted envelope stays in the Outbox until
    **both** the QoS 1 MQTT publish to the broker *and* an
    application-level :class:`~reliomq.protocol.DeliveryAck` (published back
    by a :class:`~reliomq.relay.Relay`, or by your own code speaking the
    same wire protocol) have been confirmed -- an MQTT PUBACK alone is
    never treated as "delivered."

    **If you know `paho-mqtt`:** the lifecycle is deliberately familiar --
    ``connect()``/``loop_start()`` to bring it up, ``publish()`` to send,
    ``loop_stop()``/``disconnect()`` to tear down, ``is_connected()`` to
    check status. The important difference: Paho's ``publish()`` is a
    transport operation: it hands one message to the network. reliomq's
    ``publish()`` is a managed-delivery operation: it first accepts the
    complete envelope under the selected persistence policy, then keeps it
    in one FIFO until a :class:`~reliomq.protocol.DeliveryAck` confirms it
    actually got there. RAM-first intake is the default; select
    :class:`DurableMode` explicitly when every accepted message must survive
    sudden process or power loss. See the README's "If you already know Paho
    MQTT" section for the full mapping.

    Typical usage::

        config = SenderConfig(
            host="localhost",
            outbox_path="pending.jsonl",
            debug=True,  # see reliomq's INFO/DEBUG logs with zero setup
        )
        with Sender(config) as sender:
            message_id = sender.publish(
                "factory/machine1/data",
                {"temperature": 25.2},
            )
            sender.wait_for_delivery(message_id, timeout=10.0)

    ``connect()``/``disconnect()`` (and their ``loop_start()``/``loop_stop()``
    and ``start()``/``stop()`` spellings) are idempotent and safe to call
    from any thread. A stopped
    instance may be started again. After a successful clean stop, all
    pending envelopes are in the same Outbox and resume on the next start.
    """

    def __init__(
        self,
        config: SenderConfig,
        *,
        mode: DeliveryMode | None = None,
        client_factory: ClientFactory | None = None,
        outbox: Outbox | None = None,
        store: Outbox | None = None,
    ) -> None:
        if not isinstance(config, SenderConfig):
            raise TypeError("config must be a SenderConfig")
        self._mode = resolve_mode(mode)

        resolved_outbox = resolve_renamed_argument(
            new_value=outbox,
            old_value=store,
            new_name="outbox",
            old_name="store",
            owner="Sender",
            default=None,
        )
        if resolved_outbox is not None and not isinstance(resolved_outbox, Outbox):
            # Tests and applications may provide a compatible Outbox double;
            # structural validation below gives it a useful error message.
            required = (
                "append",
                "peek_oldest",
                "remove_oldest",
                "size",
                "load",
                "contains",
            )
            if any(
                not callable(getattr(resolved_outbox, name, None))
                for name in required
            ):
                raise TypeError("outbox must implement the Outbox API")

        if config.log_level is not None:
            enable_logging(config.log_level)

        self.config = config
        self.outbox = (
            resolved_outbox
            if resolved_outbox is not None
            else Outbox(config.outbox_path, logger=logger)
        )

        client_id = config.client_id or f"mqtt-reliable-{uuid.uuid4().hex}"
        self.client = create_client(
            client_factory,
            client_id=client_id,
            userdata={"role": "reliable sender"},
        )
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(
            min_delay=config.reconnect_min_delay,
            max_delay=config.reconnect_max_delay,
        )

        self._stop_event = threading.Event()
        self._connected = threading.Event()
        self._ack_subscription_ready = threading.Event()
        self._wakeup = threading.Event()
        self._ack_tracker = AckTracker()

        self._lifecycle_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._commit_lock = threading.Lock()
        self._subscription_lock = threading.Lock()
        self._delivery_condition = threading.Condition()
        self._queue_lock = threading.RLock()

        ids_reader = getattr(self.outbox, "pending_ids", None)
        recovered_ids = (
            ids_reader()
            if callable(ids_reader)
            else [envelope.message_id for envelope in self.outbox.load()]
        )
        self._pending: deque[_PendingMessage] = deque(
            _PendingMessage(
                message_id=message_id,
                _encoded=None,
                outbox=self.outbox,
                encoded_size=0,
                enqueued_at=time.monotonic(),
                state=_StorageState.DISK_BACKED,
            )
            for message_id in recovered_ids
        )
        self._pending_by_id: dict[str, _PendingMessage] = {
            item.message_id: item for item in self._pending
        }
        # Contains exactly the RAM-owned suffix in FastMode. The unified
        # _pending deque also contains recovered/spilled disk records.
        self._fast_queue: deque[_PendingMessage] = deque()
        self._fast_ram_bytes = 0
        self._fast_timer: threading.Timer | None = None
        self._fast_timer_generation = 0
        self._fast_timer_deadline: float | None = None
        self._fast_retry_not_before = 0.0
        self._disconnected_since: float | None = None
        self._persistence = PersistencePolicy(
            self.outbox,
            self._mode,
            logger=logger,
        )

        self._subscription_mid: int | None = None
        self._next_subscription_attempt = 0.0
        self._worker: threading.Thread | None = None
        self._started = False
        self._loop_started = False
        self._connection_count = 0

        # In-memory only, purely for diagnostics: how many times the current
        # logical head has been retried, keyed by message_id. Never
        # persisted, never read back, and never affects delivery decisions
        # -- popped on success so it cannot grow past the queue depth.
        self._retry_attempts: dict[str, int] = {}

        logger.info(
            "Sender initialized | broker=%s:%s | mode=%s | outbox_path=%s | pending=%s",
            config.host,
            config.port,
            type(self._mode).__name__,
            self.outbox.path,
            len(recovered_ids),
        )

    # ------------------------------------------------------------------
    # Public API -- lifecycle
    # ------------------------------------------------------------------

    def start(self) -> Sender:
        """Start Paho's network loop and the FIFO recovery worker.

        Repeated calls while running are harmless. A stopped instance may be
        started again; all pending envelopes remain in the same Outbox.

        Paho-familiar aliases :meth:`connect` and :meth:`loop_start` call
        this exact method -- see the class docstring for why reliomq does
        not offer a "connected but not processing" state the way raw Paho
        can.
        """

        with self._lifecycle_lock:
            if self._started:
                return self
            if self._worker is not None and self._worker.is_alive():
                raise RuntimeError("previous Sender worker is still stopping")

            # Construction already replayed and validated the Outbox. This
            # snapshot also includes any messages published before start().
            pending = self.pending_count()

            with self._commit_lock:
                self._stop_event.clear()
            self._connected.clear()
            self._ack_subscription_ready.clear()
            with self._subscription_lock:
                self._subscription_mid = None
                self._next_subscription_attempt = 0.0

            logger.info(
                "Connecting to broker | host=%s | port=%s", self.config.host, self.config.port
            )
            connect_result = self.client.connect_async(
                self.config.host,
                self.config.port,
                self.config.keepalive,
            )
            if connect_result not in (None, mqtt.MQTT_ERR_SUCCESS):
                raise OSError(f"MQTT connect request failed: {connect_result}")

            loop_result = self.client.loop_start()
            if loop_result not in (None, mqtt.MQTT_ERR_SUCCESS):
                raise OSError(f"MQTT network loop failed to start: {loop_result}")
            self._loop_started = True

            self._worker = threading.Thread(
                target=self._delivery_worker,
                name="reliomq-sender",
                daemon=True,
            )
            self._started = True
            with self._queue_lock:
                if isinstance(self._mode, FastMode):
                    self._disconnected_since = (
                        None if self._connected.is_set() else time.monotonic()
                    )
                    self._schedule_fast_maintenance_locked()
            self._worker.start()
            self._wakeup.set()

        logger.info(
            "Sender started | broker=%s:%s | pending=%s",
            self.config.host,
            self.config.port,
            pending,
        )
        if pending:
            logger.info(
                "Restored %s pending message(s) from a previous run | outbox_path=%s",
                pending,
                self.outbox.path,
            )
        return self

    def connect(self) -> Sender:
        """Paho-familiar alias for :meth:`start`.

        In raw Paho, ``connect()`` establishes the MQTT connection and you
        separately choose how to run its network loop. reliomq cannot offer
        that split honestly: managed delivery *is* the background worker
        that watches the live FIFO and manages retries/ACKs, and there is no
        useful state where a connection exists but that worker doesn't run.
        So ``connect()`` does the complete job :meth:`start` does -- call
        :meth:`loop_start` afterward if you like the two-call Paho shape
        (it is a harmless no-op at that point), or skip straight to
        ``publish()``.
        """

        return self.start()

    def loop_start(self) -> Sender:
        """Paho-familiar alias for :meth:`start` -- see :meth:`connect`."""

        return self.start()

    def stop(self) -> None:
        """Stop cleanly and persist bounded-loss work where possible.

        Paho-familiar aliases :meth:`disconnect` and :meth:`loop_stop` call
        this exact method. Outstanding group writes are fsync'd, and RAM-only
        fast entries are durably spilled in FIFO order before this returns.
        """

        persistence_error: OutboxError | None = None
        with self._lifecycle_lock:
            was_active = (
                self._started
                or self._loop_started
                or (self._worker is not None and self._worker.is_alive())
            )
            if was_active:
                logger.info("Sender stopping | pending=%s", self.pending_count())

                # The commit lock establishes a clean boundary: after stop is
                # observed, an ACK cannot race into completion/removal.
                with self._commit_lock:
                    self._stop_event.set()
                self._ack_tracker.interrupt()
                self._wakeup.set()
                with self._delivery_condition:
                    self._delivery_condition.notify_all()

                try:
                    self.client.disconnect()
                except Exception:
                    logger.debug("MQTT disconnect request failed", exc_info=True)

                worker = self._worker
                if worker is not None and worker is not threading.current_thread():
                    worker.join(
                        timeout=(
                            self.config.mqtt_puback_timeout
                            + self.config.delivery_ack_timeout
                            + 2.0
                        )
                    )
                    if worker.is_alive():
                        logger.error("Sender worker did not stop promptly")

                worker_still_alive = worker is not None and worker.is_alive()

                if self._loop_started:
                    try:
                        self.client.loop_stop()
                    except Exception:
                        logger.debug("MQTT network loop stop failed", exc_info=True)

                self._loop_started = False
                self._started = False
                # Retain a pathological still-running worker reference so a
                # restart cannot create a second delivery worker. Once it has
                # exited, start() may safely replace the reference.
                self._worker = worker if worker_still_alive else None
                self._connected.clear()
                self._ack_subscription_ready.clear()

            # Serialize against publish() so every fast item that linearized
            # before this shutdown boundary is included in the spill.
            with self._publish_lock:
                try:
                    with self._queue_lock:
                        self._cancel_fast_timer_locked()
                        self._disconnected_since = None
                        self._spill_unpersisted_fast_locked()
                except OutboxError as error:
                    persistence_error = error
                    logger.exception("Could not persist fast messages during shutdown")
                try:
                    self._persistence.flush()
                except OutboxError as error:
                    if persistence_error is None:
                        persistence_error = error
                    logger.exception("Could not flush group messages during shutdown")

        if was_active:
            logger.info("Sender stopped | pending=%s", self.pending_count())
        if persistence_error is not None:
            raise persistence_error

    def disconnect(self) -> None:
        """Paho-familiar alias for :meth:`stop`."""

        self.stop()

    def loop_stop(self) -> None:
        """Paho-familiar alias for :meth:`stop` -- see :meth:`disconnect`."""

        self.stop()

    def is_connected(self) -> bool:
        """Mirror Paho's ``is_connected()``: True once the MQTT CONNECT completed.

        This is honestly just the transport connection state -- it does
        **not** by itself mean reliomq is ready to deliver. Right after
        connecting there is a brief window where this is True but the
        internal DeliveryAck subscription (which happens automatically)
        hasn't finished yet. Don't poll this to decide whether to ``publish()`` --
        that's always safe, even before :meth:`start`/:meth:`connect`. Use
        :meth:`wait_for_delivery`/:meth:`pending_count` to reason about
        delivery, not connection, state.
        """

        return self._connected.is_set()

    def __enter__(self) -> Sender:
        return self.start()

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Public API -- messages
    # ------------------------------------------------------------------

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        message_id: str | None = None,
        event_id: str | None = None,
    ) -> str:
        """Enqueue a JSON-compatible payload and return its stable ID.

        Storage behavior is fixed by the Sender's constructor-level
        :attr:`mode`; it cannot be overridden per publish. The ID is assigned
        and the payload is snapshotted to canonical wire bytes before the
        message enters RAM or persistent storage.

        Returning never means the destination accepted the message; MQTT
        PUBACK and the application :class:`~reliomq.protocol.DeliveryAck`
        happen asynchronously. Call :meth:`wait_for_delivery` for that.

        The call is safe before :meth:`connect`/:meth:`start`; delivery
        begins once the sender is started. Repeating an identical message
        with the same explicit ``message_id`` is idempotent. Reusing a
        pending ID for different content raises ``ValueError``.
        """

        resolved_message_id = resolve_renamed_argument(
            new_value=message_id,
            old_value=event_id,
            new_name="message_id",
            old_name="event_id",
            owner="Sender.publish",
            default="",
        )
        candidate = (
            MessageEnvelope(topic=topic, payload=payload)
            if not resolved_message_id
            else MessageEnvelope(
                message_id=resolved_message_id,
                topic=topic,
                payload=payload,
            )
        )
        # Round-tripping freezes nested mutable payloads at the acceptance
        # boundary and gives RAM accounting the exact bytes sent on retries.
        encoded = candidate.to_bytes()
        envelope = MessageEnvelope.from_bytes(encoded)
        encoded_size = len(encoded)

        with self._publish_lock, self._queue_lock:
            existing_item = self._pending_by_id.get(envelope.message_id)
            if existing_item is not None:
                if existing_item.encoded != encoded:
                    raise ValueError(
                        f"message_id {envelope.message_id!r} is already pending "
                        "with different content"
                    )
                pending = len(self._pending)
            elif isinstance(self._mode, FastMode):
                self._make_fast_room_locked(encoded_size)
                item = _PendingMessage(
                    message_id=envelope.message_id,
                    _encoded=encoded,
                    outbox=self.outbox,
                    encoded_size=encoded_size,
                    enqueued_at=time.monotonic(),
                    state=_StorageState.RAM_ONLY,
                )
                self._pending.append(item)
                self._pending_by_id[envelope.message_id] = item
                self._fast_queue.append(item)
                self._fast_ram_bytes += encoded_size
                self._spill_fast_high_water_locked()
                self._schedule_fast_maintenance_locked()
                pending = len(self._pending)
            else:
                appended = self._persistence.append(envelope)
                if not appended:
                    disk_envelope = next(
                        (
                            queued
                            for queued in self.outbox.load()
                            if queued.message_id == envelope.message_id
                        ),
                        None,
                    )
                    if (
                        disk_envelope is None
                        or disk_envelope.to_bytes() != envelope.to_bytes()
                    ):
                        raise ValueError(
                            f"message_id {envelope.message_id!r} is already pending "
                            "with different content"
                        )
                item = _PendingMessage(
                    message_id=envelope.message_id,
                    _encoded=None,
                    outbox=self.outbox,
                    encoded_size=encoded_size,
                    enqueued_at=time.monotonic(),
                    state=_StorageState.DISK_BACKED,
                )
                self._pending.append(item)
                self._pending_by_id[envelope.message_id] = item
                pending = len(self._pending)
            storage_state = self._pending_by_id[envelope.message_id].state.value

        self._wakeup.set()
        if isinstance(self._mode, FastMode):
            logger.info(
                "Message accepted by FastMode | message_id=%s | topic=%s | "
                "storage=%s | pending=%s",
                envelope.message_id,
                envelope.topic,
                storage_state,
                pending,
            )
        else:
            # Retain the long-standing phrase as useful log/API compatibility.
            logger.info(
                "Message stored in Outbox | message_id=%s | topic=%s | "
                "mode=%s | pending=%s",
                envelope.message_id,
                envelope.topic,
                type(self._mode).__name__,
                pending,
            )
        return envelope.message_id

    @property
    def mode(self) -> DeliveryMode:
        """The immutable persistence/delivery mode selected at construction."""

        return self._mode

    def pending_count(self) -> int:
        """Return the number of valid messages currently awaiting delivery.

        This is a reliomq-specific extension -- plain MQTT has no notion of
        a managed delivery backlog. Use it to size a "delivery is falling behind"
        warning in a long-running loop (see ``examples/sensor_loop.py``),
        not as a substitute for :meth:`wait_for_delivery`.
        """

        with self._queue_lock:
            return len(self._pending)

    def wait_for_delivery(
        self,
        message_id: str | None = None,
        timeout: float | None = None,
        *,
        event_id: str | None = None,
    ) -> bool:
        """Block until one message (or the whole logical FIFO) is delivered.

        This is a reliomq-specific extension with no Paho equivalent --
        Paho's ``publish()`` result only tells you the broker accepted one
        publish, never that anything downstream processed it. Here,
        "delivered" means a matching :class:`~reliomq.protocol.DeliveryAck`
        arrived and the message left the Sender's pending state.

        Pass the ``message_id`` returned by :meth:`publish` to wait for one
        specific message, or omit it to wait for this client's queue to drain.
        ``False`` means the timeout expired or the sender stopped while the
        requested message(s) remained pending. Durable and persisted messages
        stay safe in the Outbox; an unpromoted fast message remains RAM-only.
        Outbox failures propagate as
        :class:`~reliomq.outbox.OutboxError` rather than being reported as
        successful delivery.

        Don't call this after every ``publish()`` in a tight loop (e.g. a
        sensor reading every few seconds) -- that would serialize every
        reading behind a network round trip. Call :meth:`pending_count`
        instead to monitor backlog without blocking.
        """

        resolved_message_id = resolve_renamed_argument(
            new_value=message_id,
            old_value=event_id,
            new_name="message_id",
            old_name="event_id",
            owner="Sender.wait_for_delivery",
            default="",
        ) or None

        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            # Check the logical predicate while holding the same condition
            # used by the completer. This avoids losing a notification between
            # the check and wait.
            with self._delivery_condition:
                with self._queue_lock:
                    pending = (
                        bool(self._pending)
                        if resolved_message_id is None
                        else resolved_message_id in self._pending_by_id
                    )
                if not pending:
                    return True
                if self._stop_event.is_set():
                    return False

                remaining = (
                    None if deadline is None else deadline - time.monotonic()
                )
                if remaining is not None and remaining <= 0:
                    return False
                self._delivery_condition.wait(timeout=remaining)

    @property
    def store(self) -> Outbox:
        """Deprecated alias for :attr:`outbox`."""

        warn_deprecated_attribute(owner="Sender", old_name="store", new_name="outbox")
        return self.outbox

    # ------------------------------------------------------------------
    # FastMode RAM ownership and persistence transitions
    # ------------------------------------------------------------------

    def _spill_unpersisted_fast_locked(self) -> None:
        """Durably persist every RAM-owned FastMode message in FIFO batches."""

        while self._fast_queue:
            self._spill_fast_batch_locked()

    def _spill_fast_batch_locked(self, *, eligible_count: int | None = None) -> int:
        """Persist one bounded RAM prefix and release ownership after fsync."""

        if not isinstance(self._mode, FastMode) or not self._fast_queue:
            return 0

        selected: list[_PendingMessage] = []
        selected_bytes = 0
        max_messages = self._mode.spill_batch_messages
        if eligible_count is not None:
            max_messages = min(max_messages, eligible_count)

        for item in self._fast_queue:
            if len(selected) >= max_messages:
                break
            if item.state is not _StorageState.RAM_ONLY:
                raise OutboxError("FastMode RAM ownership state is inconsistent")
            if (
                selected
                and selected_bytes + item.encoded_size
                > self._mode.spill_batch_bytes
            ):
                break
            selected.append(item)
            selected_bytes += item.encoded_size

        if not selected:
            return 0

        for item in selected:
            item.state = _StorageState.PERSISTING
            # A batch can commit one segment and fail on a later segment.
            # Until a retry confirms the whole selected prefix, ACK handling
            # must not forget its RAM owner and leave an orphaned disk head.
            item.spill_attempted = True

        try:
            result = self._persistence.append_many_durable(
                [item.envelope for item in selected]
            )
            if result.appended_count != len(selected):
                # This can only be a retry after an uncertain storage failure
                # or external single-process-contract violation. Release RAM
                # ownership only after proving every exact envelope exists and
                # forcing the durable boundary again.
                for item in selected:
                    stored = self.outbox.get(item.message_id)
                    if stored is None or stored.to_bytes() != item.encoded:
                        raise OutboxError(
                            "FastMode spill did not persist every selected message"
                        )
                self.outbox.sync()
        except Exception:
            for item in selected:
                item.state = _StorageState.RAM_ONLY
            raise

        for item in selected:
            if not self._fast_queue or self._fast_queue[0] is not item:
                raise OutboxError("FastMode FIFO changed during spill")
            self._fast_queue.popleft()
            self._fast_ram_bytes -= item.encoded_size
            item.state = _StorageState.DISK_BACKED
            item._encoded = None

        self._fast_retry_not_before = 0.0
        logger.info(
            "FastMode spill committed | messages=%s | bytes=%s | first_message_id=%s",
            len(selected),
            selected_bytes,
            selected[0].message_id,
        )
        return len(selected)

    def _make_fast_room_locked(self, encoded_size: int) -> None:
        """Enforce hard message and byte limits without silently dropping."""

        assert isinstance(self._mode, FastMode)
        if encoded_size > self._mode.ram_max_bytes:
            raise FastQueueFullError(
                "message exceeds FastMode ram_max_bytes "
                f"({encoded_size}>{self._mode.ram_max_bytes})"
            )

        while (
            len(self._fast_queue) + 1 > self._mode.ram_max_messages
            or self._fast_ram_bytes + encoded_size > self._mode.ram_max_bytes
        ):
            if not self._fast_queue:
                raise FastQueueFullError("FastMode RAM limits prevent acceptance")
            try:
                spilled = self._spill_fast_batch_locked()
            except OutboxError as error:
                raise FastQueueFullError(
                    "FastMode RAM is full and its oldest batch could not be spilled"
                ) from error
            if not spilled:
                raise FastQueueFullError("FastMode RAM limits prevent acceptance")

    def _fast_high_water_reached_locked(self) -> bool:
        if not isinstance(self._mode, FastMode) or not self._fast_queue:
            return False
        message_mark = max(
            1,
            math.ceil(
                self._mode.ram_max_messages * self._mode.high_watermark
            ),
        )
        byte_mark = max(
            1,
            math.ceil(self._mode.ram_max_bytes * self._mode.high_watermark),
        )
        return (
            len(self._fast_queue) >= message_mark
            or self._fast_ram_bytes >= byte_mark
        )

    def _spill_fast_high_water_locked(self) -> None:
        """Spill enough batches to move back below both high-water marks."""

        try:
            while self._fast_high_water_reached_locked():
                if not self._spill_fast_batch_locked():
                    break
        except OutboxError:
            # The message is already accepted and still has its sole copy in
            # bounded RAM. Retry asynchronously and surface a hard failure on
            # stop or if capacity later prevents another acceptance.
            self._fast_retry_not_before = time.monotonic() + self.config.retry_interval
            logger.exception("FastMode high-water spill failed; RAM ownership retained")

    def _cancel_fast_timer_locked(self) -> None:
        self._fast_timer_generation += 1
        timer = self._fast_timer
        self._fast_timer = None
        self._fast_timer_deadline = None
        if timer is not None:
            timer.cancel()

    def _schedule_fast_maintenance_locked(self) -> None:
        """Arm the next age/disconnect timer, including before start()."""

        if not isinstance(self._mode, FastMode) or not self._fast_queue:
            self._cancel_fast_timer_locked()
            return

        now = time.monotonic()
        deadline = self._fast_queue[0].enqueued_at + self._mode.max_ram_age
        if self._disconnected_since is not None:
            deadline = min(
                deadline,
                self._disconnected_since + self._mode.disconnect_grace,
            )
        if self._fast_retry_not_before > now and deadline <= now:
            deadline = self._fast_retry_not_before

        if (
            self._fast_timer is not None
            and self._fast_timer_deadline is not None
            and abs(self._fast_timer_deadline - deadline) < 1e-9
        ):
            return

        self._cancel_fast_timer_locked()
        generation = self._fast_timer_generation
        timer = threading.Timer(
            max(0.0, deadline - now),
            self._on_fast_maintenance_timer,
            args=(generation,),
        )
        timer.name = "reliomq-fast-spill"
        timer.daemon = True
        self._fast_timer = timer
        self._fast_timer_deadline = deadline
        timer.start()

    def _on_fast_maintenance_timer(self, generation: int) -> None:
        with self._publish_lock, self._queue_lock:
            if generation != self._fast_timer_generation:
                return
            self._fast_timer = None
            self._fast_timer_deadline = None
            self._run_fast_maintenance_locked()

    def _run_fast_maintenance_locked(self) -> None:
        """Spill all RAM messages selected by age or disconnect triggers."""

        if not isinstance(self._mode, FastMode) or not self._fast_queue:
            self._schedule_fast_maintenance_locked()
            return

        now = time.monotonic()
        if self._fast_retry_not_before > now:
            self._schedule_fast_maintenance_locked()
            return

        disconnected_due = (
            self._disconnected_since is not None
            and now - self._disconnected_since >= self._mode.disconnect_grace
        )
        try:
            if disconnected_due:
                self._spill_unpersisted_fast_locked()
            else:
                while self._fast_queue:
                    eligible = sum(
                        item.enqueued_at + self._mode.max_ram_age <= now
                        for item in self._fast_queue
                    )
                    if not eligible:
                        break
                    self._spill_fast_batch_locked(eligible_count=eligible)
                while self._fast_high_water_reached_locked():
                    self._spill_fast_batch_locked()
        except OutboxError:
            self._fast_retry_not_before = now + self.config.retry_interval
            logger.exception("Timed FastMode spill failed; RAM ownership retained")
        self._schedule_fast_maintenance_locked()

    def _promote_fast_for_retry(
        self, item: _PendingMessage, *, reason: str
    ) -> bool:
        """Persist a fast item before relying on it for a later retry."""

        with self._queue_lock:
            if item.persisted:
                return True
            if self._pending_by_id.get(item.message_id) is not item:
                return True
            try:
                # FIFO delivery makes this item the earliest RAM-only entry.
                # Persist one bounded prefix so retry spill has one sequential
                # write and one fsync rather than one sync per message.
                if not self._fast_queue or self._fast_queue[0] is not item:
                    raise OutboxError(
                        "fast queue ordering is inconsistent with logical FIFO"
                    )
                self._spill_fast_batch_locked()
                self._schedule_fast_maintenance_locked()
            except OutboxError:
                logger.exception(
                    "Could not promote fast message for retry | message_id=%s | reason=%s",
                    item.message_id,
                    reason,
                )
                return False
        return True

    def _complete_item(self, item: _PendingMessage) -> DeliveryStatus:
        """Commit a valid DeliveryAck and remove exactly the logical head."""

        envelope = item.envelope
        message_id = envelope.message_id
        with self._commit_lock:
            if self._stop_event.is_set():
                return DeliveryStatus.STOPPED
            with self._queue_lock:
                if not self._pending or self._pending[0] is not item:
                    logger.error(
                        "DeliveryAck matched but message was not the logical oldest | "
                        "message_id=%s",
                        message_id,
                    )
                    return DeliveryStatus.STORE_ERROR

                if not item.persisted and item.spill_attempted:
                    try:
                        self._spill_fast_batch_locked()
                    except OutboxError:
                        logger.exception(
                            "DeliveryAck cannot complete an uncertain FastMode spill | "
                            "message_id=%s", message_id,
                        )
                        return DeliveryStatus.STORE_ERROR

                if item.persisted:
                    try:
                        removed = self._persistence.complete(envelope)
                    except OutboxError:
                        logger.exception(
                            "DeliveryAck matched but Outbox completion failed | "
                            "message_id=%s",
                            message_id,
                        )
                        return DeliveryStatus.STORE_ERROR
                    if not removed:
                        logger.error(
                            "DeliveryAck matched but message was not the Outbox oldest | "
                            "message_id=%s",
                            message_id,
                        )
                        return DeliveryStatus.STORE_ERROR
                else:
                    if self._fast_queue and self._fast_queue[0] is item:
                        self._fast_queue.popleft()
                    else:
                        # Defensive fallback; under FIFO scheduling the item is
                        # always the first RAM-only fast record.
                        try:
                            self._fast_queue.remove(item)
                        except ValueError:
                            pass
                    self._fast_ram_bytes -= item.encoded_size

                self._pending.popleft()
                self._pending_by_id.pop(message_id, None)
                self._schedule_fast_maintenance_locked()

        self._retry_attempts.pop(message_id, None)
        with self._delivery_condition:
            self._delivery_condition.notify_all()
        logger.info(
            "Message completed | message_id=%s | pending=%s",
            message_id,
            self.pending_count(),
        )
        return DeliveryStatus.DELIVERED

    # ------------------------------------------------------------------
    # MQTT callbacks and connection state
    # ------------------------------------------------------------------

    def _on_connect(
        self, client, _userdata, _flags, reason_code, _properties
    ) -> None:
        if not reason_code_is_success(reason_code):
            self._mark_disconnected()
            logger.warning("MQTT connection rejected | reason=%s", reason_code)
            return

        self._connection_count += 1
        reconnect = self._connection_count > 1
        self._connected.set()
        self._ack_subscription_ready.clear()
        with self._queue_lock:
            self._disconnected_since = None
            self._schedule_fast_maintenance_locked()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_attempt = 0.0
        self._request_ack_subscription(client)
        self._wakeup.set()
        logger.info(
            "MQTT connection established | broker=%s:%s | reconnect=%s",
            self.config.host,
            self.config.port,
            reconnect,
        )

    def _on_connect_fail(self, _client, _userdata, *_args) -> None:
        self._mark_disconnected()
        logger.warning("MQTT connection attempt failed")

    def _on_disconnect(
        self, _client, _userdata, _flags, reason_code, _properties
    ) -> None:
        self._mark_disconnected()
        log = logger.debug if self._stop_event.is_set() else logger.warning
        log("MQTT disconnected | reason=%s", reason_code)

    def _on_subscribe(
        self, _client, _userdata, mid, reason_codes, _properties
    ) -> None:
        # `mid` is Paho's MQTT-protocol packet identifier for this SUBACK --
        # unrelated to reliomq's own per-message `message_id` used elsewhere
        # in this file and deliberately named differently to avoid confusing
        # the two.
        with self._subscription_lock:
            expected_mid = self._subscription_mid
            if expected_mid not in (-1, mid):
                logger.debug("Ignoring stale SUBACK | mid=%s", mid)
                return
            self._subscription_mid = None

            if suback_is_success(reason_codes) and self._connected.is_set():
                self._ack_subscription_ready.set()
                self._next_subscription_attempt = 0.0
                ready = True
            else:
                self._ack_subscription_ready.clear()
                self._next_subscription_attempt = (
                    time.monotonic() + self.config.retry_interval
                )
                ready = False

        self._wakeup.set()
        if ready:
            logger.debug(
                "DeliveryAck subscription ready | topic=%s | mid=%s",
                self.config.delivery_ack_topic,
                mid,
            )
        else:
            logger.warning(
                "DeliveryAck subscription rejected | topic=%s | mid=%s",
                self.config.delivery_ack_topic,
                mid,
            )

    def _on_message(self, _client, _userdata, message) -> None:
        if message.topic != self.config.delivery_ack_topic:
            logger.warning("Ignoring message on unexpected topic %s", message.topic)
            return

        try:
            acknowledgement = DeliveryAck.from_bytes(message.payload)
        except (ProtocolError, TypeError, ValueError) as error:
            logger.warning("Ignoring malformed DeliveryAck: %s", error)
            return

        if self._ack_tracker.match(acknowledgement.message_id):
            logger.debug(
                "Matching DeliveryAck received | message_id=%s",
                acknowledgement.message_id,
            )
        else:
            logger.warning(
                "Ignoring late or unmatched DeliveryAck | message_id=%s",
                acknowledgement.message_id,
            )

    def _mark_disconnected(self) -> None:
        self._connected.clear()
        self._ack_subscription_ready.clear()
        with self._queue_lock:
            if (
                isinstance(self._mode, FastMode)
                and self._disconnected_since is None
                and not self._stop_event.is_set()
            ):
                self._disconnected_since = time.monotonic()
            self._schedule_fast_maintenance_locked()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_attempt = 0.0
        self._ack_tracker.interrupt()
        self._wakeup.set()

    def _request_ack_subscription(self, client=None) -> bool:
        """Request/retry the DeliveryAck subscription, awaiting SUBACK before use."""

        if not self._connected.is_set() or self._stop_event.is_set():
            return False
        if self._ack_subscription_ready.is_set():
            return True

        now = time.monotonic()
        with self._subscription_lock:
            if self._ack_subscription_ready.is_set():
                return True
            if (
                self._subscription_mid is not None
                and now < self._next_subscription_attempt
            ):
                return False
            if now < self._next_subscription_attempt:
                return False
            # -1 also lets a synchronous test double's SUBACK match before
            # subscribe() has returned its real message ID.
            self._subscription_mid = -1
            self._next_subscription_attempt = now + self.config.retry_interval

        mqtt_client = client or self.client
        try:
            # `mid` here is Paho's MQTT packet identifier for this SUBSCRIBE
            # request, not a reliomq message_id.
            result, mid = mqtt_client.subscribe(
                self.config.delivery_ack_topic,
                qos=self.config.qos,
            )
        except Exception:
            with self._subscription_lock:
                self._subscription_mid = None
            logger.exception(
                "DeliveryAck subscription request failed for %s",
                self.config.delivery_ack_topic,
            )
            return False

        with self._subscription_lock:
            if result != mqtt.MQTT_ERR_SUCCESS:
                self._subscription_mid = None
                self._ack_subscription_ready.clear()
                logger.warning(
                    "DeliveryAck subscription request rejected | topic=%s | result=%s",
                    self.config.delivery_ack_topic,
                    result,
                )
                return False
            # A synchronous callback may already have changed the sentinel.
            if self._subscription_mid == -1:
                self._subscription_mid = mid
        return self._ack_subscription_ready.is_set()

    def _connection_ready(self) -> bool:
        if not (
            self._connected.is_set() and self._ack_subscription_ready.is_set()
        ):
            return False
        try:
            return bool(self.client.is_connected())
        except Exception:
            logger.debug("Could not query Paho connection state", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # Deterministic delivery state machine
    # ------------------------------------------------------------------

    def _process_oldest_once(self) -> DeliveryStatus:
        """Attempt this client's logical oldest message exactly once."""

        if self._stop_event.is_set():
            return DeliveryStatus.STOPPED

        with self._queue_lock:
            item = self._pending[0] if self._pending else None
        if item is None:
            return DeliveryStatus.EMPTY
        envelope = item.envelope
        encoded = envelope.to_bytes()
        if not self._connection_ready():
            self._request_ack_subscription()
            if isinstance(self._mode, FastMode):
                with self._queue_lock:
                    self._run_fast_maintenance_locked()
            return DeliveryStatus.NOT_READY

        message_id = envelope.message_id
        attempt = self._retry_attempts.get(message_id, 0) + 1
        logger.debug(
            "Delivery attempt starting | message_id=%s | topic=%s | attempt=%s",
            message_id,
            envelope.topic,
            attempt,
        )

        self._ack_tracker.begin(message_id)
        try:
            if self._stop_event.is_set():
                return DeliveryStatus.STOPPED

            logger.debug(
                "Publish attempt | message_id=%s | topic=%s | mqtt_puback_timeout=%s",
                message_id,
                self.config.relay_topic,
                self.config.mqtt_puback_timeout,
            )
            published = confirmed_publish(
                self.client,
                self.config.relay_topic,
                encoded,
                qos=self.config.qos,
                retain=False,
                timeout=self.config.mqtt_puback_timeout,
            )
            if not published:
                # Covers both an immediate broker-level rejection and the
                # PUBACK simply never arriving within mqtt_puback_timeout --
                # confirmed_publish() does not distinguish the two, and
                # either way the outcome for the caller is identical: no
                # confirmed PUBACK, so this attempt is retried.
                if self._stop_event.is_set():
                    return DeliveryStatus.STOPPED
                if isinstance(self._mode, FastMode):
                    if self._connection_ready():
                        if not self._promote_fast_for_retry(
                            item, reason="MQTT PUBACK not confirmed"
                        ):
                            return DeliveryStatus.STORE_ERROR
                    else:
                        # A transport interruption during the PUBACK wait
                        # observes the same disconnect grace as an interrupted
                        # DeliveryAck wait; age/pressure triggers still apply.
                        with self._queue_lock:
                            self._run_fast_maintenance_locked()
                return self._schedule_retry(
                    message_id,
                    reason="MQTT PUBACK not confirmed within mqtt_puback_timeout",
                )

            logger.debug("MQTT PUBACK received | message_id=%s", message_id)
            logger.debug(
                "Waiting for DeliveryAck | message_id=%s | delivery_ack_timeout=%s",
                message_id,
                self.config.delivery_ack_timeout,
            )

            if not self._ack_tracker.wait(self.config.delivery_ack_timeout):
                if self._stop_event.is_set():
                    logger.debug(
                        "DeliveryAck wait interrupted by shutdown | message_id=%s",
                        message_id,
                    )
                    return DeliveryStatus.STOPPED
                # A live connection means the end-to-end ACK timeout itself
                # elapsed, which is a FastMode spill trigger. A disconnect
                # interruption instead observes disconnect_grace before spill.
                if isinstance(self._mode, FastMode):
                    if self._connection_ready():
                        if not self._promote_fast_for_retry(
                            item, reason="DeliveryAck timeout"
                        ):
                            return DeliveryStatus.STORE_ERROR
                    else:
                        with self._queue_lock:
                            self._run_fast_maintenance_locked()
                return self._schedule_retry(
                    message_id,
                    reason="DeliveryAck not confirmed within delivery_ack_timeout",
                )

            logger.info("DeliveryAck received | message_id=%s", message_id)

            return self._complete_item(item)
        finally:
            self._ack_tracker.end()

    def _schedule_retry(self, message_id: str, *, reason: str) -> DeliveryStatus:
        """Log why a delivery attempt failed and that it will be retried."""

        attempt = self._retry_attempts.get(message_id, 0) + 1
        self._retry_attempts[message_id] = attempt
        logger.warning(
            "Delivery attempt failed, will retry | message_id=%s | attempt=%s | "
            "reason=%s",
            message_id,
            attempt,
            reason,
        )
        logger.info(
            "Delivery retry scheduled | message_id=%s | attempt=%s | delay=%s",
            message_id,
            attempt,
            self.config.retry_interval,
        )
        return DeliveryStatus.RETRY

    def _delivery_worker(self) -> None:
        while not self._stop_event.is_set():
            self._wakeup.clear()
            try:
                status = self._process_oldest_once()
            except Exception:
                # An unexpected worker failure must leave the logical oldest
                # untouched; fast messages are promoted on defined failure
                # paths, while an unforeseen pre-promotion crash remains
                # within fast mode's explicitly weaker guarantee.
                logger.exception("Unexpected reliable delivery worker error")
                status = DeliveryStatus.RETRY

            if status is DeliveryStatus.DELIVERED:
                # Drain confirmed messages without an artificial retry delay.
                continue
            if status is DeliveryStatus.STOPPED or self._stop_event.is_set():
                break
            self._wakeup.wait(timeout=self.config.retry_interval)


# Deprecated (0.1.x/0.2.x) alias -- same class, same behavior.
ReliablePublisher = Sender

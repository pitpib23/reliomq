"""Durable, application-acknowledged MQTT sender."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from enum import Enum
from typing import Any

import paho.mqtt.client as mqtt

from ._compat import resolve_renamed_argument, warn_deprecated_attribute
from .ack import AckTracker
from .config import SenderConfig
from .mqtt import (
    ClientFactory,
    confirmed_publish,
    create_client,
    reason_code_is_success,
    suback_is_success,
)
from .observability import enable_logging
from .outbox import Outbox, OutboxError
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


class Sender:
    """Publish JSON messages with durable FIFO retry and correlated ACKs.

    This is reliomq's main entry point. Every call to :meth:`publish` writes
    the complete message envelope to the durable :class:`~reliomq.outbox.Outbox`
    *before returning* -- a crash the instant after ``publish()`` returns
    cannot lose the message. One background worker always selects the
    oldest stored envelope, so a live message can never overtake recovery
    traffic from a previous crash or outage. The envelope stays in the
    Outbox until **both** the QoS 1 MQTT publish to the broker *and* an
    application-level :class:`~reliomq.protocol.DeliveryAck` (published back
    by a :class:`~reliomq.relay.Relay`, or by your own code speaking the
    same wire protocol) have been confirmed -- an MQTT PUBACK alone is
    never treated as "delivered."

    **If you know `paho-mqtt`:** the lifecycle is deliberately familiar --
    ``connect()``/``loop_start()`` to bring it up, ``publish()`` to send,
    ``loop_stop()``/``disconnect()`` to tear down, ``is_connected()`` to
    check status. The important difference: Paho's ``publish()`` is a
    transport operation: it hands one message to the network. reliomq's
    ``publish()`` is a reliable-delivery operation: it durably persists the
    message first and keeps retrying it -- across reconnects, and across a
    full process restart -- until a :class:`~reliomq.protocol.DeliveryAck`
    confirms it actually got there. See the README's "If you already know
    Paho MQTT" section for the full mapping.

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
    and ``start()``/``stop()`` spellings -- all four names do the exact same
    thing) are idempotent and safe to call from any thread. A stopped
    instance may be started again -- all pending envelopes remain in the
    same Outbox, so no message is lost across restarts.
    """

    def __init__(
        self,
        config: SenderConfig,
        *,
        client_factory: ClientFactory | None = None,
        outbox: Outbox | None = None,
        store: Outbox | None = None,
    ) -> None:
        if not isinstance(config, SenderConfig):
            raise TypeError("config must be a SenderConfig")

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

        self._subscription_mid: int | None = None
        self._next_subscription_attempt = 0.0
        self._worker: threading.Thread | None = None
        self._started = False
        self._loop_started = False
        self._connection_count = 0

        # In-memory only, purely for diagnostics: how many times the current
        # durable head has been retried, keyed by message_id. Never
        # persisted, never read back, and never affects delivery decisions
        # -- popped on success so it cannot grow past the queue depth.
        self._retry_attempts: dict[str, int] = {}

        logger.info(
            "Sender initialized | broker=%s:%s | outbox_path=%s",
            config.host,
            config.port,
            self.outbox.path,
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

            # Fail visibly if the Outbox cannot be inspected. An I/O error
            # must never be confused with an empty queue.
            pending = self.outbox.size()

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
        that split honestly: durable delivery *is* the background worker
        that watches the Outbox and manages retries/ACKs, and there is no
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
        """Stop cleanly without removing or regenerating the in-flight message.

        Paho-familiar aliases :meth:`disconnect` and :meth:`loop_stop` call
        this exact method.
        """

        with self._lifecycle_lock:
            if not self._started and not self._loop_started:
                return

            logger.info("Sender stopping | pending=%s", self.outbox.size())

            # The commit lock establishes a clean boundary: after stop is
            # observed, an ACK cannot race into a durable removal.
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

            if self._loop_started:
                try:
                    self.client.loop_stop()
                except Exception:
                    logger.debug("MQTT network loop stop failed", exc_info=True)

            self._loop_started = False
            self._started = False
            self._worker = None
            self._connected.clear()
            self._ack_subscription_ready.clear()

        logger.info("Sender stopped | pending=%s", self.outbox.size())

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
        """Durably enqueue a JSON-compatible payload and return its stable ID.

        This means reliomq has **accepted the message into its reliable
        delivery workflow** -- the payload is fsync'd to the Outbox before
        this call returns. It does **not** mean the destination broker has
        accepted it yet, that a :class:`~reliomq.protocol.DeliveryAck` has
        come back, or that the message has left the Outbox; those all
        happen afterward, asynchronously, on the background worker started
        by :meth:`connect`/:meth:`start`. Call :meth:`wait_for_delivery` if
        your code needs to know when that finishes.

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
        envelope = (
            MessageEnvelope(topic=topic, payload=payload)
            if not resolved_message_id
            else MessageEnvelope(
                message_id=resolved_message_id,
                topic=topic,
                payload=payload,
            )
        )

        with self._publish_lock:
            appended = self.outbox.append(envelope)
            if not appended:
                existing = next(
                    (
                        queued
                        for queued in self.outbox.load()
                        if queued.message_id == envelope.message_id
                    ),
                    None,
                )
                if (
                    existing is None
                    or existing.to_bytes() != envelope.to_bytes()
                ):
                    raise ValueError(
                        f"message_id {envelope.message_id!r} is already pending "
                        "with different content"
                    )

        self._wakeup.set()
        logger.info(
            "Message stored in Outbox | message_id=%s | topic=%s | pending=%s",
            envelope.message_id,
            envelope.topic,
            self.outbox.size(),
        )
        return envelope.message_id

    def pending_count(self) -> int:
        """Return the number of valid messages currently awaiting delivery.

        This is a reliomq-specific extension -- plain MQTT has no notion of
        a durable backlog. Use it to size a "delivery is falling behind"
        warning in a long-running loop (see ``examples/sensor_loop.py``),
        not as a substitute for :meth:`wait_for_delivery`.
        """

        return self.outbox.size()

    def wait_for_delivery(
        self,
        message_id: str | None = None,
        timeout: float | None = None,
        *,
        event_id: str | None = None,
    ) -> bool:
        """Block until one message (or the whole Outbox) is fully delivered.

        This is a reliomq-specific extension with no Paho equivalent --
        Paho's ``publish()`` result only tells you the broker accepted one
        publish, never that anything downstream processed it. Here,
        "delivered" means a matching :class:`~reliomq.protocol.DeliveryAck`
        arrived and the message was removed from the Outbox.

        Pass the ``message_id`` returned by :meth:`publish` to wait for one
        specific message, or omit it to wait for the Outbox to fully drain.
        ``False`` means the timeout expired or the sender stopped while the
        requested message(s) remained pending -- the message is still safe
        in the Outbox and will be retried. Outbox failures propagate as
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
            # Check the durable predicate while holding the same condition
            # used by the remover. This avoids losing a notification between
            # the check and wait.
            with self._delivery_condition:
                pending = (
                    self.outbox.size() > 0
                    if resolved_message_id is None
                    else self.outbox.contains(resolved_message_id)
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
        """Attempt the durable oldest message exactly once."""

        if self._stop_event.is_set():
            return DeliveryStatus.STOPPED

        try:
            envelope = self.outbox.peek_oldest()
        except OutboxError:
            logger.exception("Cannot inspect Outbox")
            return DeliveryStatus.STORE_ERROR

        if envelope is None:
            return DeliveryStatus.EMPTY
        if not self._connection_ready():
            self._request_ack_subscription()
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
                envelope.to_bytes(),
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
                # Covers both delivery_ack_timeout actually elapsing and the
                # wait being interrupted by an unrelated disconnect (not a
                # shutdown, handled above) -- either way the message stays
                # in the Outbox and this attempt is retried.
                return self._schedule_retry(
                    message_id,
                    reason="DeliveryAck not confirmed within delivery_ack_timeout",
                )

            logger.info("DeliveryAck received | message_id=%s", message_id)

            with self._commit_lock:
                if self._stop_event.is_set():
                    return DeliveryStatus.STOPPED
                try:
                    removed = self.outbox.remove_oldest(envelope)
                except OutboxError:
                    logger.exception(
                        "DeliveryAck matched but Outbox removal failed | "
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

            self._retry_attempts.pop(message_id, None)
            with self._delivery_condition:
                self._delivery_condition.notify_all()
            logger.info(
                "Message completed | message_id=%s | pending=%s",
                message_id,
                self.outbox.size(),
            )
            return DeliveryStatus.DELIVERED
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
                # An unexpected worker failure must leave the durable oldest
                # untouched and retryable.
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

"""Durable, application-acknowledged MQTT publisher."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from enum import Enum
from typing import Any

import paho.mqtt.client as mqtt

from .ack import AckTracker
from .config import ReliabilityConfig
from .mqtt import (
    ClientFactory,
    confirmed_publish,
    create_client,
    reason_code_is_success,
    suback_is_success,
)
from .protocol import Ack, MessageEnvelope, ProtocolError
from .store import DurableMessageStore, StoreError


logger = logging.getLogger(__name__)


class DeliveryStatus(str, Enum):
    """Result of one deterministic oldest-message delivery attempt."""

    EMPTY = "empty"
    NOT_READY = "not_ready"
    DELIVERED = "delivered"
    RETRY = "retry"
    STORE_ERROR = "store_error"
    STOPPED = "stopped"


class ReliablePublisher:
    """Publish JSON messages with durable FIFO retry and correlated ACKs.

    Every call to :meth:`publish` writes the complete message envelope to the
    durable queue before returning.  A single worker always selects the oldest
    stored envelope, so live messages cannot overtake recovery traffic.  The
    envelope remains stored until both the QoS 1 source publish and its
    application-level ACK have been confirmed.
    """

    def __init__(
        self,
        config: ReliabilityConfig,
        *,
        client_factory: ClientFactory | None = None,
        store: DurableMessageStore | None = None,
    ) -> None:
        if not isinstance(config, ReliabilityConfig):
            raise TypeError("config must be a ReliabilityConfig")
        if store is not None and not isinstance(store, DurableMessageStore):
            # Tests and applications may provide a compatible store double;
            # structural validation below gives it a useful error message.
            required = (
                "append",
                "peek_oldest",
                "remove_oldest",
                "size",
                "load",
                "contains",
            )
            if any(not callable(getattr(store, name, None)) for name in required):
                raise TypeError("store must implement the DurableMessageStore API")

        self.config = config
        self.store = (
            store
            if store is not None
            else DurableMessageStore(config.queue_path, logger=logger)
        )

        client_id = config.client_id or f"mqtt-reliable-{uuid.uuid4().hex}"
        self.client = create_client(
            client_factory,
            client_id=client_id,
            userdata={"role": "reliable publisher"},
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> ReliablePublisher:
        """Start Paho's network loop and the FIFO recovery worker.

        Repeated calls while running are harmless.  A stopped instance may be
        started again; all pending envelopes remain in the same durable store.
        """

        with self._lifecycle_lock:
            if self._started:
                return self

            # Fail visibly if the durable queue cannot be inspected.  An I/O
            # error must never be confused with an empty queue.
            pending = self.store.size()

            with self._commit_lock:
                self._stop_event.clear()
            self._connected.clear()
            self._ack_subscription_ready.clear()
            with self._subscription_lock:
                self._subscription_mid = None
                self._next_subscription_attempt = 0.0

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
                name="reliable-mqtt-publisher",
                daemon=True,
            )
            self._started = True
            self._worker.start()
            self._wakeup.set()

        logger.info(
            "Reliable publisher started | broker=%s:%s | pending=%s",
            self.config.host,
            self.config.port,
            pending,
        )
        return self

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        event_id: str | None = None,
    ) -> str:
        """Durably enqueue a JSON-compatible payload and return its stable ID.

        The call is safe before :meth:`start`; delivery begins when the
        publisher starts.  Repeating an identical message with the same
        explicit ID is idempotent.  Reusing a pending ID for different content
        raises ``ValueError``.
        """

        if event_id is None:
            envelope = MessageEnvelope(topic=topic, payload=payload)
        else:
            envelope = MessageEnvelope(
                event_id=event_id,
                topic=topic,
                payload=payload,
            )

        with self._publish_lock:
            appended = self.store.append(envelope)
            if not appended:
                existing = next(
                    (
                        queued
                        for queued in self.store.load()
                        if queued.event_id == envelope.event_id
                    ),
                    None,
                )
                if (
                    existing is None
                    or existing.to_bytes() != envelope.to_bytes()
                ):
                    raise ValueError(
                        f"event_id {envelope.event_id!r} is already pending "
                        "with different content"
                    )

        self._wakeup.set()
        logger.debug(
            "Message durably queued | event_id=%s | topic=%s",
            envelope.event_id,
            envelope.topic,
        )
        return envelope.event_id

    def pending_count(self) -> int:
        """Return the number of valid messages currently awaiting delivery."""

        return self.store.size()

    def wait_for_delivery(
        self,
        event_id: str | None = None,
        timeout: float | None = None,
    ) -> bool:
        """Wait for one event, or for the whole queue when ``event_id`` is None.

        ``False`` means the timeout expired or the publisher stopped while the
        requested message(s) remained pending.  Store failures propagate as
        :class:`StoreError` rather than being reported as successful delivery.
        """

        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            # Check the durable predicate while holding the same condition
            # used by the remover.  This avoids losing a notification between
            # the check and wait.
            with self._delivery_condition:
                pending = (
                    self.store.size() > 0
                    if event_id is None
                    else self.store.contains(event_id)
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

    def stop(self) -> None:
        """Stop cleanly without removing or regenerating the in-flight event."""

        with self._lifecycle_lock:
            if not self._started and not self._loop_started:
                return

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
                        self.config.publish_timeout
                        + self.config.ack_timeout
                        + 2.0
                    )
                )
                if worker.is_alive():
                    logger.error("Reliable publisher worker did not stop promptly")

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

        logger.info("Reliable publisher stopped | pending=%s", self.store.size())

    def __enter__(self) -> ReliablePublisher:
        return self.start()

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.stop()

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

        self._connected.set()
        self._ack_subscription_ready.clear()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_attempt = 0.0
        self._request_ack_subscription(client)
        self._wakeup.set()
        logger.info(
            "MQTT connected | broker=%s:%s", self.config.host, self.config.port
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
        self, _client, _userdata, message_id, reason_codes, _properties
    ) -> None:
        with self._subscription_lock:
            expected_mid = self._subscription_mid
            if expected_mid not in (-1, message_id):
                logger.debug("Ignoring stale SUBACK | mid=%s", message_id)
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
                "ACK subscription ready | topic=%s | mid=%s",
                self.config.ack_topic,
                message_id,
            )
        else:
            logger.warning(
                "ACK subscription rejected | topic=%s | mid=%s",
                self.config.ack_topic,
                message_id,
            )

    def _on_message(self, _client, _userdata, message) -> None:
        if message.topic != self.config.ack_topic:
            logger.warning("Ignoring message on unexpected topic %s", message.topic)
            return

        try:
            acknowledgement = Ack.from_bytes(message.payload)
        except (ProtocolError, TypeError, ValueError) as error:
            logger.warning("Ignoring malformed ACK: %s", error)
            return

        if self._ack_tracker.match(acknowledgement.event_id):
            logger.debug(
                "Matching ACK received | event_id=%s", acknowledgement.event_id
            )
        else:
            logger.warning(
                "Ignoring late or unmatched ACK | event_id=%s",
                acknowledgement.event_id,
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
        """Request/retry the ACK subscription, awaiting SUBACK before use."""

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
            result, message_id = mqtt_client.subscribe(
                self.config.ack_topic,
                qos=self.config.qos,
            )
        except Exception:
            with self._subscription_lock:
                self._subscription_mid = None
            logger.exception(
                "ACK subscription request failed for %s", self.config.ack_topic
            )
            return False

        with self._subscription_lock:
            if result != mqtt.MQTT_ERR_SUCCESS:
                self._subscription_mid = None
                self._ack_subscription_ready.clear()
                logger.warning(
                    "ACK subscription request rejected | topic=%s | result=%s",
                    self.config.ack_topic,
                    result,
                )
                return False
            # A synchronous callback may already have changed the sentinel.
            if self._subscription_mid == -1:
                self._subscription_mid = message_id
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
            envelope = self.store.peek_oldest()
        except StoreError:
            logger.exception("Cannot inspect durable MQTT queue")
            return DeliveryStatus.STORE_ERROR

        if envelope is None:
            return DeliveryStatus.EMPTY
        if not self._connection_ready():
            self._request_ack_subscription()
            return DeliveryStatus.NOT_READY

        self._ack_tracker.begin(envelope.event_id)
        try:
            if self._stop_event.is_set():
                return DeliveryStatus.STOPPED

            published = confirmed_publish(
                self.client,
                self.config.data_topic,
                envelope.to_bytes(),
                qos=self.config.qos,
                retain=False,
                timeout=self.config.publish_timeout,
            )
            if not published:
                logger.warning(
                    "Reliable source publish not confirmed | event_id=%s",
                    envelope.event_id,
                )
                return DeliveryStatus.RETRY

            if not self._ack_tracker.wait(self.config.ack_timeout):
                logger.warning(
                    "Application ACK timeout or interruption | event_id=%s",
                    envelope.event_id,
                )
                return (
                    DeliveryStatus.STOPPED
                    if self._stop_event.is_set()
                    else DeliveryStatus.RETRY
                )

            with self._commit_lock:
                if self._stop_event.is_set():
                    return DeliveryStatus.STOPPED
                try:
                    removed = self.store.remove_oldest(envelope)
                except StoreError:
                    logger.exception(
                        "ACK matched but durable removal failed | event_id=%s",
                        envelope.event_id,
                    )
                    return DeliveryStatus.STORE_ERROR

            if not removed:
                logger.error(
                    "ACK matched but message was not the durable oldest | "
                    "event_id=%s",
                    envelope.event_id,
                )
                return DeliveryStatus.STORE_ERROR

            with self._delivery_condition:
                self._delivery_condition.notify_all()
            logger.debug("Reliable delivery confirmed | event_id=%s", envelope.event_id)
            return DeliveryStatus.DELIVERED
        finally:
            self._ack_tracker.end()

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

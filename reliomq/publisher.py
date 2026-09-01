"""Durable, application-acknowledged MQTT publisher."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from enum import Enum
from typing import Any

import paho.mqtt.client as mqtt

from ._compat import resolve_renamed_argument
from .ack import AckTracker
from .config import PublisherConfig
from .mqtt import (
    ClientFactory,
    confirmed_publish,
    create_client,
    reason_code_is_success,
    suback_is_success,
)
from .observability import enable_logging
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

    This is reliomq's main entry point. Every call to :meth:`publish` writes
    the complete message envelope to the durable queue *before returning* --
    a crash the instant after ``publish()`` returns cannot lose the message.
    One background worker always selects the oldest stored envelope, so a
    live message can never overtake recovery traffic from a previous crash
    or outage. The envelope stays in the durable queue until **both** the
    QoS 1 MQTT publish to the broker *and* an application-level ACK
    (published back by a :class:`~reliomq.bridge.ReliableMqttBridge`, or by
    your own code speaking the same wire protocol) have been confirmed --
    an MQTT PUBACK alone is never treated as "delivered."

    Typical usage::

        config = PublisherConfig(
            host="localhost",
            queue_path="pending.jsonl",
            debug=True,  # see reliomq's INFO/DEBUG logs with zero setup
        )
        with ReliablePublisher(config) as publisher:
            message_id = publisher.publish(
                topic="factory/machine1/data",
                payload={"temperature": 25.2},
            )
            publisher.wait_for_delivery(message_id, timeout=10.0)

    ``start()``/``stop()`` are idempotent and safe to call from any thread.
    A stopped instance may be started again -- all pending envelopes remain
    in the same durable store, so no message is lost across restarts.
    """

    def __init__(
        self,
        config: PublisherConfig,
        *,
        client_factory: ClientFactory | None = None,
        store: DurableMessageStore | None = None,
    ) -> None:
        if not isinstance(config, PublisherConfig):
            raise TypeError("config must be a PublisherConfig")
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

        if config.log_level is not None:
            enable_logging(config.log_level)

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
        self._connection_count = 0

        # In-memory only, purely for diagnostics: how many times the current
        # durable head has been retried, keyed by message_id. Never
        # persisted, never read back, and never affects delivery decisions
        # -- popped on success so it cannot grow past the queue depth.
        self._retry_attempts: dict[str, int] = {}

        logger.info(
            "Publisher initialized | broker=%s:%s | queue_path=%s",
            config.host,
            config.port,
            self.store.path,
        )

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
                name="reliable-mqtt-publisher",
                daemon=True,
            )
            self._started = True
            self._worker.start()
            self._wakeup.set()

        logger.info(
            "Publisher started | broker=%s:%s | pending=%s",
            self.config.host,
            self.config.port,
            pending,
        )
        if pending:
            logger.info(
                "Restored %s pending message(s) from a previous run | queue_path=%s",
                pending,
                self.store.path,
            )
        return self

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        message_id: str | None = None,
        event_id: str | None = None,
    ) -> str:
        """Durably enqueue a JSON-compatible payload and return its stable ID.

        The call is safe before :meth:`start`; delivery begins when the
        publisher starts.  Repeating an identical message with the same
        explicit ``message_id`` is idempotent.  Reusing a pending ID for
        different content raises ``ValueError``.
        """

        resolved_message_id = resolve_renamed_argument(
            new_value=message_id,
            old_value=event_id,
            new_name="message_id",
            old_name="event_id",
            owner="ReliablePublisher.publish",
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
            appended = self.store.append(envelope)
            if not appended:
                existing = next(
                    (
                        queued
                        for queued in self.store.load()
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
            "Message accepted and durably queued | message_id=%s | topic=%s | pending=%s",
            envelope.message_id,
            envelope.topic,
            self.store.size(),
        )
        return envelope.message_id

    def pending_count(self) -> int:
        """Return the number of valid messages currently awaiting delivery."""

        return self.store.size()

    def wait_for_delivery(
        self,
        message_id: str | None = None,
        timeout: float | None = None,
        *,
        event_id: str | None = None,
    ) -> bool:
        """Wait for one message, or the whole queue when ``message_id`` is None.

        ``False`` means the timeout expired or the publisher stopped while the
        requested message(s) remained pending.  Store failures propagate as
        :class:`StoreError` rather than being reported as successful delivery.
        """

        resolved_message_id = resolve_renamed_argument(
            new_value=message_id,
            old_value=event_id,
            new_name="message_id",
            old_name="event_id",
            owner="ReliablePublisher.wait_for_delivery",
            default="",
        ) or None

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
                    if resolved_message_id is None
                    else self.store.contains(resolved_message_id)
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
        """Stop cleanly without removing or regenerating the in-flight message."""

        with self._lifecycle_lock:
            if not self._started and not self._loop_started:
                return

            logger.info("Publisher stopping | pending=%s", self.store.size())

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

        logger.info("Publisher stopped | pending=%s", self.store.size())

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
                "ACK subscription ready | topic=%s | mid=%s",
                self.config.ack_topic,
                mid,
            )
        else:
            logger.warning(
                "ACK subscription rejected | topic=%s | mid=%s",
                self.config.ack_topic,
                mid,
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

        if self._ack_tracker.match(acknowledgement.message_id):
            logger.debug(
                "Matching ACK received | message_id=%s", acknowledgement.message_id
            )
        else:
            logger.warning(
                "Ignoring late or unmatched ACK | message_id=%s",
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
            # `mid` here is Paho's MQTT packet identifier for this SUBSCRIBE
            # request, not a reliomq message_id.
            result, mid = mqtt_client.subscribe(
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
            envelope = self.store.peek_oldest()
        except StoreError:
            logger.exception("Cannot inspect durable MQTT queue")
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
                "Publish attempt | message_id=%s | topic=%s",
                message_id,
                self.config.envelope_topic,
            )
            published = confirmed_publish(
                self.client,
                self.config.envelope_topic,
                envelope.to_bytes(),
                qos=self.config.qos,
                retain=False,
                timeout=self.config.publish_timeout,
            )
            if not published:
                return self._schedule_retry(
                    message_id,
                    reason="broker publish not confirmed",
                )

            logger.debug("Broker publish confirmed (PUBACK) | message_id=%s", message_id)
            logger.debug(
                "Waiting for application acknowledgement | message_id=%s | timeout=%s",
                message_id,
                self.config.ack_timeout,
            )

            if not self._ack_tracker.wait(self.config.ack_timeout):
                if self._stop_event.is_set():
                    logger.debug(
                        "ACK wait interrupted by shutdown | message_id=%s", message_id
                    )
                    return DeliveryStatus.STOPPED
                return self._schedule_retry(
                    message_id,
                    reason="application ACK timeout or interruption",
                )

            logger.info(
                "Application acknowledgement received | message_id=%s", message_id
            )

            with self._commit_lock:
                if self._stop_event.is_set():
                    return DeliveryStatus.STOPPED
                try:
                    removed = self.store.remove_oldest(envelope)
                except StoreError:
                    logger.exception(
                        "ACK matched but durable removal failed | message_id=%s",
                        message_id,
                    )
                    return DeliveryStatus.STORE_ERROR

            if not removed:
                logger.error(
                    "ACK matched but message was not the durable oldest | "
                    "message_id=%s",
                    message_id,
                )
                return DeliveryStatus.STORE_ERROR

            self._retry_attempts.pop(message_id, None)
            with self._delivery_condition:
                self._delivery_condition.notify_all()
            logger.info(
                "Message delivered and removed from durable queue | "
                "message_id=%s | pending=%s",
                message_id,
                self.store.size(),
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
            "Retry scheduled | message_id=%s | attempt=%s | delay=%s",
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

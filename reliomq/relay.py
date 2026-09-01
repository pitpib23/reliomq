"""End-to-end MQTT forwarding with application-level acknowledgements.

The relay deliberately has no durable Outbox of its own.  A source
``Sender`` retains each message until this relay confirms the destination
QoS 1 publication and then publishes the correlated :class:`~reliomq.protocol.DeliveryAck`.
If any step fails, no DeliveryAck is sent and the source sender remains
responsible for retrying the durable message.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
import warnings
from typing import Any

import paho.mqtt.client as mqtt

from .config import RelayConfig
from .mqtt import (
    ClientFactory,
    confirmed_publish,
    create_client,
    reason_code_is_success,
    suback_is_success,
)
from .observability import enable_logging
from .protocol import DeliveryAck, DeliveryEnvelope, MessageEnvelope, ProtocolError


logger = logging.getLogger(__name__)


def _generated_client_id(role: str) -> str:
    """Return a short, process-unique MQTT client ID."""

    return f"mr-relay-{role}-{uuid.uuid4().hex[:8]}"


class Relay:
    """Forward reliable envelopes and ACK only confirmed destination delivery.

    Sits between two brokers: it subscribes to a source sender's relay
    topic, republishes each message's payload to its real application topic
    on the destination broker, and -- only once that destination publish is
    QoS 1 confirmed -- publishes a :class:`~reliomq.protocol.DeliveryAck`
    back to the source so the sender can retire its durable record. If any
    step fails (either broker offline, a malformed envelope, a full
    internal queue, a timeout, or shutdown), no DeliveryAck is sent, and the
    source sender keeps its durable copy and retries.

    **If you know `paho-mqtt`:** ``Relay`` manages *two* Paho clients (one
    per broker), so its lifecycle is Paho-familiar but necessarily coarser
    than :class:`~reliomq.sender.Sender`'s -- ``connect()``/``start()``
    bring up *both* broker connections together; there is no way to
    connect to only one side through this API. See ``source_connected``/
    ``destination_connected`` if you need to tell them apart.

    One worker serializes all destination publishes.  Its in-memory queue is
    intentionally bounded and non-durable: dropping or abandoning a relay
    task is safe because the source sender receives no DeliveryAck and
    therefore retains its durable copy for a later retry.

    Repeated lifecycle calls are safe, and a stopped relay can be started
    again with an empty volatile task queue.
    """

    def __init__(
        self,
        config: RelayConfig,
        *,
        client_factory: ClientFactory | None = None,
        source_client_factory: ClientFactory | None = None,
        destination_client_factory: ClientFactory | None = None,
        bridge_logger: logging.Logger | None = None,
        relay_logger: logging.Logger | None = None,
    ) -> None:
        if not isinstance(config, RelayConfig):
            raise TypeError("config must be a RelayConfig")

        if config.log_level is not None:
            enable_logging(config.log_level)

        self.config = config
        resolved_logger = relay_logger or bridge_logger
        if bridge_logger is not None and relay_logger is None:
            warnings.warn(
                "Relay(bridge_logger=...) is deprecated and will be removed "
                "in a future release; use Relay(relay_logger=...) instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self._logger = resolved_logger or logger
        source_factory = source_client_factory or client_factory
        destination_factory = destination_client_factory or client_factory

        source_client_id = config.source_client_id or _generated_client_id("src")
        destination_client_id = (
            config.destination_client_id or _generated_client_id("dst")
        )
        self.source_client = create_client(
            source_factory,
            client_id=source_client_id,
            userdata={"role": "reliable-relay-source"},
        )
        self.destination_client = create_client(
            destination_factory,
            client_id=destination_client_id,
            userdata={"role": "reliable-relay-destination"},
        )
        if self.source_client is self.destination_client:
            raise ValueError("source and destination clients must be independent")

        for client in (self.source_client, self.destination_client):
            client.reconnect_delay_set(
                min_delay=config.reconnect_min_delay,
                max_delay=config.reconnect_max_delay,
            )

        self.source_client.on_connect = self._on_source_connect
        self.source_client.on_connect_fail = self._on_source_connect_fail
        self.source_client.on_disconnect = self._on_source_disconnect
        self.source_client.on_subscribe = self._on_source_subscribe
        self.source_client.on_message = self._on_source_message

        self.destination_client.on_connect = self._on_destination_connect
        self.destination_client.on_connect_fail = self._on_destination_connect_fail
        self.destination_client.on_disconnect = self._on_destination_disconnect

        self._source_connected = threading.Event()
        self._source_subscription_ready = threading.Event()
        self._destination_connected = threading.Event()
        self._accepting = threading.Event()
        self._stop_event = threading.Event()

        self._tasks: queue.Queue[MessageEnvelope] = queue.Queue(
            maxsize=config.max_queue_size
        )
        self._intake_lock = threading.Lock()
        self._subscription_lock = threading.Lock()
        self._subscription_mid: int | None = None
        self._next_subscription_retry = 0.0

        self._lifecycle_lock = threading.Lock()
        self._running = False
        self._worker: threading.Thread | None = None

        self._logger.info(
            "Relay initialized | source=%s:%s | destination=%s:%s | "
            "relay_topic=%s | delivery_ack_topic=%s",
            config.source_host,
            config.source_port,
            config.destination_host,
            config.destination_port,
            config.relay_topic,
            config.delivery_ack_topic,
        )

    # ------------------------------------------------------------------
    # Connection and subscription callbacks
    # ------------------------------------------------------------------

    def _on_source_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _connect_flags: Any,
        reason_code: Any,
        _properties: Any,
    ) -> None:
        self._source_subscription_ready.clear()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_retry = 0.0

        if not reason_code_is_success(reason_code):
            self._source_connected.clear()
            self._logger.warning(
                "Source broker connection refused | reason=%s", reason_code
            )
            return

        self._source_connected.set()
        self._logger.info(
            "Source broker connection established | broker=%s:%s",
            self.config.source_host,
            self.config.source_port,
        )
        self._request_source_subscription(client)

    def _on_source_connect_fail(
        self, _client: mqtt.Client, _userdata: Any, *_args: Any
    ) -> None:
        self._source_connected.clear()
        self._source_subscription_ready.clear()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_retry = 0.0
        if not self._stop_event.is_set():
            self._logger.warning("Source broker connection attempt failed")

    def _on_source_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: Any,
        reason_code: Any,
        _properties: Any,
    ) -> None:
        self._source_connected.clear()
        self._source_subscription_ready.clear()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_retry = 0.0
        if not self._stop_event.is_set():
            self._logger.warning(
                "Source broker disconnected | reason=%s", reason_code
            )

    def _request_source_subscription(self, client: mqtt.Client | None = None) -> None:
        """Subscribe once and wait for its matching SUBACK before readiness."""

        if (
            not self._accepting.is_set()
            or not self._source_connected.is_set()
            or self._source_subscription_ready.is_set()
        ):
            return

        now = time.monotonic()
        with self._subscription_lock:
            if self._source_subscription_ready.is_set():
                return
            if now < self._next_subscription_retry:
                return
            # The sentinel permits a synchronous test double to invoke
            # on_subscribe before subscribe() returns its real MID.
            self._subscription_mid = -1
            self._next_subscription_retry = now + self.config.retry_interval

        source = client or self.source_client
        try:
            # `mid` is Paho's MQTT packet identifier for this SUBSCRIBE
            # request -- unrelated to a reliomq message_id.
            result, mid = source.subscribe(
                self.config.relay_topic, qos=self.config.qos
            )
        except Exception:
            with self._subscription_lock:
                self._subscription_mid = None
            self._logger.exception(
                "Source subscription request failed | topic=%s",
                self.config.relay_topic,
            )
            return

        with self._subscription_lock:
            if result != mqtt.MQTT_ERR_SUCCESS:
                self._subscription_mid = None
                self._logger.warning(
                    "Source subscription request rejected | topic=%s | rc=%s",
                    self.config.relay_topic,
                    result,
                )
                return

            # A synchronous SUBACK has already cleared the sentinel and set
            # readiness; do not overwrite that completed state.
            if self._subscription_mid == -1:
                self._subscription_mid = mid

    def _on_source_subscribe(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        mid: int,
        reason_codes: Any,
        _properties: Any,
    ) -> None:
        with self._subscription_lock:
            if self._subscription_mid not in (-1, mid):
                self._logger.debug(
                    "Ignoring unexpected source SUBACK | mid=%s", mid
                )
                return
            self._subscription_mid = None

            if suback_is_success(reason_codes) and self._source_connected.is_set():
                ready = True
                self._next_subscription_retry = 0.0
            else:
                ready = False
                self._next_subscription_retry = (
                    time.monotonic() + self.config.retry_interval
                )

        if ready:
            self._source_subscription_ready.set()
            self._logger.info(
                "Source subscription ready | topic=%s", self.config.relay_topic
            )
        else:
            self._source_subscription_ready.clear()
            self._logger.warning(
                "Source subscription rejected | topic=%s | reasons=%s",
                self.config.relay_topic,
                reason_codes,
            )

    def _retry_source_subscription_if_due(self) -> None:
        if (
            not self._source_connected.is_set()
            or self._source_subscription_ready.is_set()
        ):
            return
        with self._subscription_lock:
            retry_at = self._next_subscription_retry
        if retry_at == 0.0 or time.monotonic() >= retry_at:
            self._request_source_subscription()

    def _on_destination_connect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _connect_flags: Any,
        reason_code: Any,
        _properties: Any,
    ) -> None:
        if not reason_code_is_success(reason_code):
            self._destination_connected.clear()
            self._logger.warning(
                "Destination broker connection refused | reason=%s", reason_code
            )
            return
        self._destination_connected.set()
        self._logger.info(
            "Destination broker connection established | broker=%s:%s",
            self.config.destination_host,
            self.config.destination_port,
        )

    def _on_destination_connect_fail(
        self, _client: mqtt.Client, _userdata: Any, *_args: Any
    ) -> None:
        self._destination_connected.clear()
        if not self._stop_event.is_set():
            self._logger.warning("Destination broker connection attempt failed")

    def _on_destination_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: Any,
        reason_code: Any,
        _properties: Any,
    ) -> None:
        self._destination_connected.clear()
        if not self._stop_event.is_set():
            self._logger.warning(
                "Destination broker disconnected | reason=%s", reason_code
            )

    # ------------------------------------------------------------------
    # Intake and confirmed forwarding
    # ------------------------------------------------------------------

    def _on_source_message(
        self, _client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        if not self._accepting.is_set():
            return
        if not self._source_subscription_ready.is_set():
            # Paho should not normally dispatch a subscribed message before
            # SUBACK.  Keeping this guard makes readiness explicit and fails
            # closed if a broker/client behaves unexpectedly during reconnect.
            self._logger.warning("Ignoring source message before subscription ready")
            return
        if message.topic != self.config.relay_topic:
            self._logger.warning("Ignoring unexpected source topic %s", message.topic)
            return

        try:
            envelope = MessageEnvelope.from_bytes(message.payload)
        except ProtocolError as error:
            self._logger.warning(
                "Ignoring invalid source envelope | topic=%s | error=%s",
                message.topic,
                error,
            )
            return
        except Exception:
            # A protocol implementation bug must still fail closed: never ACK
            # input that was not successfully and strictly validated.
            self._logger.exception("Could not decode source envelope; no DeliveryAck sent")
            return

        # Serialize the final acceptance check with stop().  Without this
        # boundary, a callback that began decoding just before shutdown could
        # enqueue after stop had already drained the volatile queue.
        with self._intake_lock:
            if (
                not self._accepting.is_set()
                or not self._source_subscription_ready.is_set()
            ):
                return
            try:
                self._tasks.put_nowait(envelope)
            except queue.Full:
                self._logger.error(
                    "Relay queue full; source message left unacknowledged | "
                    "message_id=%s",
                    envelope.message_id,
                )
                return

        self._logger.debug(
            "Source message queued | message_id=%s | depth=%s",
            envelope.message_id,
            self._tasks.qsize(),
        )

    @staticmethod
    def _client_is_connected(
        client: mqtt.Client, connection_event: threading.Event
    ) -> bool:
        if not connection_event.is_set():
            return False
        try:
            return bool(client.is_connected())
        except Exception:
            return False

    def _forward_once(self, envelope: MessageEnvelope) -> bool:
        """Attempt one complete destination-publish/DeliveryAck sequence.

        The method is intentionally one-shot and side-effect bounded so the
        worker can serialize it and unit tests can exercise every failure
        transition deterministically.  A ``False`` result always means that
        no confirmed DeliveryAck was sent; the durable source remains
        responsible for a later retry.
        """

        if not isinstance(envelope, MessageEnvelope):
            raise TypeError("envelope must be a MessageEnvelope")
        message_id = envelope.message_id
        if not self._client_is_connected(
            self.destination_client, self._destination_connected
        ):
            self._logger.debug(
                "Destination unavailable; no DeliveryAck sent | message_id=%s",
                message_id,
            )
            return False

        try:
            delivery_payload = DeliveryEnvelope(
                message_id=message_id,
                payload=envelope.payload,
            ).to_bytes()
        except Exception:
            self._logger.exception(
                "Could not encode destination envelope; no DeliveryAck sent | "
                "message_id=%s",
                message_id,
            )
            return False

        self._logger.debug(
            "Forwarding to destination | message_id=%s | topic=%s",
            message_id,
            envelope.topic,
        )
        if not confirmed_publish(
            self.destination_client,
            envelope.topic,
            delivery_payload,
            qos=self.config.qos,
            retain=False,
            timeout=self.config.destination_publish_timeout,
        ):
            self._logger.warning(
                "Destination publish failed; no DeliveryAck sent | message_id=%s | "
                "topic=%s",
                message_id,
                envelope.topic,
            )
            return False

        self._logger.info(
            "Relay forwarded | message_id=%s | destination_topic=%s",
            message_id,
            envelope.topic,
        )

        # From this point a source retry can duplicate the remote delivery.
        # That is the deliberate at-least-once tradeoff if DeliveryAck
        # publication fails after the destination PUBACK.
        if not self._client_is_connected(self.source_client, self._source_connected):
            self._logger.warning(
                "Source unavailable after destination success; no DeliveryAck "
                "sent | message_id=%s",
                message_id,
            )
            return False

        try:
            ack_payload = DeliveryAck(message_id=message_id).to_bytes()
        except Exception:
            self._logger.exception(
                "Could not encode DeliveryAck | message_id=%s", message_id
            )
            return False

        if not confirmed_publish(
            self.source_client,
            self.config.delivery_ack_topic,
            ack_payload,
            qos=self.config.qos,
            retain=False,
            timeout=self.config.source_ack_publish_timeout,
        ):
            self._logger.warning(
                "DeliveryAck publish failed | message_id=%s", message_id
            )
            return False

        self._logger.info("DeliveryAck sent | message_id=%s", message_id)
        return True

    def _worker_main(self) -> None:
        poll_seconds = min(0.5, self.config.retry_interval)
        while not self._stop_event.is_set():
            self._retry_source_subscription_if_due()
            try:
                envelope = self._tasks.get(timeout=poll_seconds)
            except queue.Empty:
                continue

            try:
                if self._stop_event.is_set():
                    # Shutdown owns the remaining task; leaving it un-ACKed
                    # is safe because the source still has its durable copy.
                    continue
                self._forward_once(envelope)
            except Exception:
                self._logger.exception(
                    "Unexpected Relay forwarding error; no DeliveryAck sent"
                )
            finally:
                self._tasks.task_done()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def _successful_start_result(result: Any) -> bool:
        return result is None or result == mqtt.MQTT_ERR_SUCCESS

    def _connect_and_start_loop(
        self, client: mqtt.Client, host: str, port: int
    ) -> None:
        self._logger.info("Connecting to broker | host=%s | port=%s", host, port)
        connect_result = client.connect_async(host, port, self.config.keepalive)
        if not self._successful_start_result(connect_result):
            raise RuntimeError(
                f"MQTT connect_async request failed for {host}:{port}: "
                f"{connect_result}"
            )
        loop_result = client.loop_start()
        if not self._successful_start_result(loop_result):
            raise RuntimeError(
                f"MQTT loop_start failed for {host}:{port}: {loop_result}"
            )

    def start(self) -> Relay:
        """Start the worker and both Paho network loops.

        This is the one lifecycle call that exists for ``Relay``: it brings
        up the *source and destination broker connections together*, plus
        the forwarding worker. There is no meaningful "connected but not
        forwarding" state to separate out (an un-forwarded message sitting
        in the relay's internal queue is exactly the situation this library
        exists to avoid leaving unresolved), so unlike
        :class:`~reliomq.sender.Sender`, ``Relay`` does not pretend to
        offer a Paho-style ``loop_start()`` distinct from ``connect()`` --
        :meth:`connect` is simply an alias for this method.
        """

        with self._lifecycle_lock:
            if self._running:
                return self
            self._logger.info(
                "Relay starting | source=%s:%s | destination=%s:%s",
                self.config.source_host,
                self.config.source_port,
                self.config.destination_host,
                self.config.destination_port,
            )
            self._running = True
            self._stop_event.clear()
            with self._intake_lock:
                self._accepting.set()
            self._worker = threading.Thread(
                target=self._worker_main,
                name="reliomq-relay",
                daemon=False,
            )
            self._worker.start()

        try:
            # Establish destination connectivity first to reduce unproductive
            # intake while it is unavailable.  Both connections remain fully
            # asynchronous and use Paho's automatic reconnect backoff.
            self._connect_and_start_loop(
                self.destination_client,
                self.config.destination_host,
                self.config.destination_port,
            )
            self._connect_and_start_loop(
                self.source_client,
                self.config.source_host,
                self.config.source_port,
            )
        except Exception:
            self._logger.exception("Could not start Relay")
            self.stop()
            raise

        self._logger.info("Relay started")
        return self

    def connect(self) -> Relay:
        """Paho-familiar alias for :meth:`start`.

        Connects **both** the source and destination brokers together --
        see the class docstring. There is no per-broker ``connect()`` in
        this API; construct a plain `paho-mqtt` client yourself if you need
        to manage one side independently.
        """

        return self.start()

    def loop_start(self) -> Relay:
        """Paho-familiar alias for :meth:`start` -- see :meth:`connect`."""

        return self.start()

    def stop(self) -> None:
        """Stop intake, finish the current bounded attempt, and disconnect.

        Queued tasks that have not begun forwarding are deliberately
        abandoned without DeliveryAcks. Their source senders will recover
        them from their durable Outboxes.
        """

        with self._lifecycle_lock:
            if not self._running:
                return
            self._logger.info(
                "Relay stopping | queued=%s", self._tasks.qsize()
            )
            self._running = False
            with self._intake_lock:
                self._accepting.clear()
            self._source_subscription_ready.clear()
            self._stop_event.set()
            worker = self._worker

        # confirmed_publish bounds both halves of a current attempt.  Do not
        # disconnect either client until that sequence has had time to finish.
        finish_timeout = (
            self.config.destination_publish_timeout
            + self.config.source_ack_publish_timeout
            + 1.0
        )
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=finish_timeout)

        abandoned = 0
        while True:
            try:
                self._tasks.get_nowait()
            except queue.Empty:
                break
            abandoned += 1
            self._tasks.task_done()
        if abandoned:
            self._logger.warning(
                "Relay stopped with queued messages unacknowledged | count=%s",
                abandoned,
            )

        for client in (self.source_client, self.destination_client):
            try:
                client.disconnect()
            except Exception:
                self._logger.debug("MQTT disconnect failed", exc_info=True)
            try:
                client.loop_stop()
            except Exception:
                self._logger.debug("MQTT loop_stop failed", exc_info=True)

        if (
            worker is not None
            and worker is not threading.current_thread()
            and worker.is_alive()
        ):
            worker.join(timeout=1.0)
            if worker.is_alive():
                self._logger.error("Relay worker did not stop after client shutdown")

        self._source_connected.clear()
        self._source_subscription_ready.clear()
        self._destination_connected.clear()
        with self._subscription_lock:
            self._subscription_mid = None
            self._next_subscription_retry = 0.0
        with self._lifecycle_lock:
            self._worker = None

        self._logger.info("Relay stopped")

    def disconnect(self) -> None:
        """Paho-familiar alias for :meth:`stop` (disconnects both brokers)."""

        self.stop()

    def loop_stop(self) -> None:
        """Paho-familiar alias for :meth:`stop` -- see :meth:`disconnect`."""

        self.stop()

    close = stop

    @property
    def is_running(self) -> bool:
        with self._lifecycle_lock:
            return self._running

    @property
    def source_connected(self) -> bool:
        """Mirror Paho's ``is_connected()`` for the source-broker client only."""

        return self._source_connected.is_set()

    @property
    def destination_connected(self) -> bool:
        """Mirror Paho's ``is_connected()`` for the destination-broker client only."""

        return self._destination_connected.is_set()

    @property
    def source_subscription_ready(self) -> bool:
        """Whether the exact source data subscription has received SUBACK."""

        return self._source_subscription_ready.is_set()

    @property
    def queued_count(self) -> int:
        """Return the current volatile relay queue depth for diagnostics."""

        return self._tasks.qsize()

    def __enter__(self) -> Relay:
        return self.start()

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.stop()


# Deprecated (0.1.x/0.2.x) alias -- same class, same behavior.
ReliableMqttBridge = Relay

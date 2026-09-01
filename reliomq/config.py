"""Validated configuration objects for reliable MQTT delivery.

:class:`PublisherConfig` configures :class:`~reliomq.publisher.ReliablePublisher`
and :class:`BridgeConfig` configures :class:`~reliomq.bridge.ReliableMqttBridge`
-- the naming is deliberately parallel so which config goes with which
component is obvious on sight.

Every field is validated eagerly in ``__post_init__``: an invalid config
raises :class:`ConfigError` immediately at construction, never silently
later during a connection attempt.
"""

from __future__ import annotations

import logging
import math
import os
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._compat import resolve_renamed_argument, warn_deprecated_attribute
from .observability import normalize_log_level


DEFAULT_ENVELOPE_TOPIC = "reliomq/messages"
DEFAULT_ACK_TOPIC = "reliomq/acks"

# Deprecated alias; the default value itself has not changed.
DEFAULT_DATA_TOPIC = DEFAULT_ENVELOPE_TOPIC


class ConfigError(ValueError):
    """Raised when reliability configuration is internally inconsistent."""


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty string")
    if "\x00" in value:
        raise ConfigError(f"{name} must not contain a NUL character")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ConfigError(f"{name} must contain valid Unicode") from error
    return value


def _host(value: Any, name: str) -> str:
    host = _text(value, name).strip()
    if any(character.isspace() for character in host):
        raise ConfigError(f"{name} must not contain whitespace")
    return host


def _port(value: Any, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 65535:
        raise ConfigError(f"{name} must be an integer between 1 and 65535")
    return value


def _keepalive(value: Any, name: str = "keepalive") -> int:
    if type(value) is not int or not 1 <= value <= 65535:
        raise ConfigError(f"{name} must be an integer between 1 and 65535")
    return value


def _positive_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or type(value) not in (int, float) or value <= 0:
        raise ConfigError(f"{name} must be a finite positive number")
    try:
        normalized = float(value)
    except OverflowError as error:
        raise ConfigError(f"{name} must be a finite positive number") from error
    if not math.isfinite(normalized):
        raise ConfigError(f"{name} must be a finite positive number")
    return normalized


def _positive_integer(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ConfigError(f"{name} must be a positive integer")
    return value


def _qos_one(value: Any) -> int:
    if type(value) is not int or value != 1:
        raise ConfigError("qos must be exactly 1 for reliable delivery")
    return value


def _client_id(value: Any, name: str) -> str | None:
    if value is None:
        return None
    client_id = _text(value, name)
    if len(client_id.encode("utf-8")) > 65535:
        raise ConfigError(f"{name} is too long for MQTT")
    return client_id


def _publish_topic(value: Any, name: str) -> str:
    topic = _text(value, name)
    if "+" in topic or "#" in topic:
        raise ConfigError(f"{name} must be a publish topic without '+' or '#'")
    if len(topic.encode("utf-8")) > 65535:
        raise ConfigError(f"{name} is too long for MQTT")
    return topic


def _queue_path(value: Any) -> Path:
    try:
        raw_path = os.fspath(value)
    except TypeError as error:
        raise ConfigError("queue_path must be a filesystem path") from error
    if isinstance(raw_path, bytes):
        raise ConfigError("queue_path must be a text filesystem path")
    if not raw_path or "\x00" in raw_path:
        raise ConfigError("queue_path must be a non-empty filesystem path")
    return Path(raw_path).expanduser()


def _validate_reconnect_range(minimum: float, maximum: float) -> None:
    if minimum > maximum:
        raise ConfigError(
            "reconnect_min_delay must be less than or equal to "
            "reconnect_max_delay"
        )


def _debug_flag(value: Any) -> bool:
    if type(value) is not bool:
        raise ConfigError("debug must be a bool")
    return value


def _log_level(value: Any, debug: bool) -> int | None:
    """Resolve ``log_level``/``debug`` into one effective level (or None).

    ``debug=True`` is shorthand for ``log_level="DEBUG"``; combining it with
    a *different* explicit ``log_level`` is rejected rather than silently
    picking one, matching this module's fail-loud validation style.
    """

    if value is None:
        return logging.DEBUG if debug else None
    try:
        resolved = normalize_log_level(value)
    except ValueError as error:
        raise ConfigError(f"log_level is invalid: {error}") from error
    if debug and resolved != logging.DEBUG:
        raise ConfigError(
            "debug=True conflicts with an explicit log_level other than "
            '"DEBUG"; set only one'
        )
    return resolved


@dataclass(frozen=True, slots=True, init=False)
class PublisherConfig:
    """Configuration for a durable, application-acknowledged publisher.

    ``host``/``queue_path`` are the only required fields. ``envelope_topic``
    and ``ack_topic`` are reliomq's own transport topics (not the
    application topic you pass to :meth:`~reliomq.publisher.ReliablePublisher.publish`)
    and must match the :class:`BridgeConfig` on the other end. Set
    ``log_level=`` or ``debug=True`` for zero-setup runtime visibility --
    see the README's "Logging" section for what each level shows.
    """

    host: str
    queue_path: str | os.PathLike[str]
    port: int
    client_id: str | None
    envelope_topic: str
    ack_topic: str
    qos: int
    ack_timeout: float
    retry_interval: float
    keepalive: int
    publish_timeout: float
    reconnect_min_delay: float
    reconnect_max_delay: float
    log_level: int | None
    debug: bool

    def __init__(
        self,
        host: str,
        queue_path: str | os.PathLike[str],
        port: int = 1883,
        client_id: str | None = None,
        envelope_topic: str | None = None,
        ack_topic: str = DEFAULT_ACK_TOPIC,
        qos: int = 1,
        ack_timeout: float = 3.0,
        retry_interval: float = 10.0,
        keepalive: int = 60,
        publish_timeout: float = 2.0,
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        log_level: int | str | None = None,
        debug: bool = False,
        *,
        data_topic: str | None = None,
    ) -> None:
        resolved_envelope_topic = resolve_renamed_argument(
            new_value=envelope_topic,
            old_value=data_topic,
            new_name="envelope_topic",
            old_name="data_topic",
            owner="PublisherConfig",
            default=DEFAULT_ENVELOPE_TOPIC,
            error_cls=ConfigError,
        )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "queue_path", queue_path)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "client_id", client_id)
        object.__setattr__(self, "envelope_topic", resolved_envelope_topic)
        object.__setattr__(self, "ack_topic", ack_topic)
        object.__setattr__(self, "qos", qos)
        object.__setattr__(self, "ack_timeout", ack_timeout)
        object.__setattr__(self, "retry_interval", retry_interval)
        object.__setattr__(self, "keepalive", keepalive)
        object.__setattr__(self, "publish_timeout", publish_timeout)
        object.__setattr__(self, "reconnect_min_delay", reconnect_min_delay)
        object.__setattr__(self, "reconnect_max_delay", reconnect_max_delay)
        object.__setattr__(self, "log_level", log_level)
        object.__setattr__(self, "debug", debug)
        self.__post_init__()

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", _host(self.host, "host"))
        object.__setattr__(self, "queue_path", _queue_path(self.queue_path))
        object.__setattr__(self, "port", _port(self.port, "port"))
        object.__setattr__(self, "client_id", _client_id(self.client_id, "client_id"))
        object.__setattr__(
            self, "envelope_topic", _publish_topic(self.envelope_topic, "envelope_topic")
        )
        object.__setattr__(
            self, "ack_topic", _publish_topic(self.ack_topic, "ack_topic")
        )
        if self.envelope_topic == self.ack_topic:
            raise ConfigError("envelope_topic and ack_topic must be different")
        object.__setattr__(self, "qos", _qos_one(self.qos))
        object.__setattr__(
            self, "ack_timeout", _positive_number(self.ack_timeout, "ack_timeout")
        )
        object.__setattr__(
            self,
            "retry_interval",
            _positive_number(self.retry_interval, "retry_interval"),
        )
        object.__setattr__(self, "keepalive", _keepalive(self.keepalive))
        object.__setattr__(
            self,
            "publish_timeout",
            _positive_number(self.publish_timeout, "publish_timeout"),
        )
        reconnect_minimum = _positive_number(
            self.reconnect_min_delay, "reconnect_min_delay"
        )
        reconnect_maximum = _positive_number(
            self.reconnect_max_delay, "reconnect_max_delay"
        )
        _validate_reconnect_range(reconnect_minimum, reconnect_maximum)
        object.__setattr__(self, "reconnect_min_delay", reconnect_minimum)
        object.__setattr__(self, "reconnect_max_delay", reconnect_maximum)
        object.__setattr__(self, "debug", _debug_flag(self.debug))
        object.__setattr__(
            self, "log_level", _log_level(self.log_level, self.debug)
        )

    @property
    def data_topic(self) -> str:
        """Deprecated alias for :attr:`envelope_topic`."""

        warn_deprecated_attribute(
            owner="PublisherConfig", old_name="data_topic", new_name="envelope_topic"
        )
        return self.envelope_topic


class ReliabilityConfig(PublisherConfig):
    """Deprecated alias for :class:`PublisherConfig`.

    Kept so ``from reliomq import ReliabilityConfig`` and existing
    ``ReliabilityConfig(...)`` construction calls continue to work
    unchanged; it will be removed in a future release.
    """

    def __post_init__(self) -> None:
        warnings.warn(
            "ReliabilityConfig is deprecated and will be removed in a future "
            "release; use PublisherConfig instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__post_init__()


@dataclass(frozen=True, slots=True, init=False)
class BridgeConfig:
    """Configuration for forwarding reliable envelopes between MQTT brokers.

    ``envelope_topic``/``ack_topic`` must match the :class:`PublisherConfig`
    on the source side. Set ``log_level=`` or ``debug=True`` for zero-setup
    runtime visibility -- see the README's "Logging" section.
    """

    source_host: str
    destination_host: str
    source_port: int
    destination_port: int
    source_client_id: str | None
    destination_client_id: str | None
    envelope_topic: str
    ack_topic: str
    qos: int
    keepalive: int
    destination_publish_timeout: float
    source_ack_publish_timeout: float
    retry_interval: float
    reconnect_min_delay: float
    reconnect_max_delay: float
    max_queue_size: int
    log_level: int | None
    debug: bool

    def __init__(
        self,
        source_host: str,
        destination_host: str,
        source_port: int = 1883,
        destination_port: int = 1883,
        source_client_id: str | None = None,
        destination_client_id: str | None = None,
        envelope_topic: str | None = None,
        ack_topic: str = DEFAULT_ACK_TOPIC,
        qos: int = 1,
        keepalive: int = 60,
        destination_publish_timeout: float = 2.0,
        source_ack_publish_timeout: float = 0.5,
        retry_interval: float = 10.0,
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        max_queue_size: int = 1000,
        log_level: int | str | None = None,
        debug: bool = False,
        *,
        data_topic: str | None = None,
    ) -> None:
        resolved_envelope_topic = resolve_renamed_argument(
            new_value=envelope_topic,
            old_value=data_topic,
            new_name="envelope_topic",
            old_name="data_topic",
            owner="BridgeConfig",
            default=DEFAULT_ENVELOPE_TOPIC,
            error_cls=ConfigError,
        )
        object.__setattr__(self, "source_host", source_host)
        object.__setattr__(self, "destination_host", destination_host)
        object.__setattr__(self, "source_port", source_port)
        object.__setattr__(self, "destination_port", destination_port)
        object.__setattr__(self, "source_client_id", source_client_id)
        object.__setattr__(self, "destination_client_id", destination_client_id)
        object.__setattr__(self, "envelope_topic", resolved_envelope_topic)
        object.__setattr__(self, "ack_topic", ack_topic)
        object.__setattr__(self, "qos", qos)
        object.__setattr__(self, "keepalive", keepalive)
        object.__setattr__(
            self, "destination_publish_timeout", destination_publish_timeout
        )
        object.__setattr__(
            self, "source_ack_publish_timeout", source_ack_publish_timeout
        )
        object.__setattr__(self, "retry_interval", retry_interval)
        object.__setattr__(self, "reconnect_min_delay", reconnect_min_delay)
        object.__setattr__(self, "reconnect_max_delay", reconnect_max_delay)
        object.__setattr__(self, "max_queue_size", max_queue_size)
        object.__setattr__(self, "log_level", log_level)
        object.__setattr__(self, "debug", debug)
        self.__post_init__()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_host", _host(self.source_host, "source_host")
        )
        object.__setattr__(
            self,
            "destination_host",
            _host(self.destination_host, "destination_host"),
        )
        object.__setattr__(
            self, "source_port", _port(self.source_port, "source_port")
        )
        object.__setattr__(
            self,
            "destination_port",
            _port(self.destination_port, "destination_port"),
        )
        object.__setattr__(
            self,
            "source_client_id",
            _client_id(self.source_client_id, "source_client_id"),
        )
        object.__setattr__(
            self,
            "destination_client_id",
            _client_id(self.destination_client_id, "destination_client_id"),
        )
        object.__setattr__(
            self, "envelope_topic", _publish_topic(self.envelope_topic, "envelope_topic")
        )
        object.__setattr__(
            self, "ack_topic", _publish_topic(self.ack_topic, "ack_topic")
        )
        if self.envelope_topic == self.ack_topic:
            raise ConfigError("envelope_topic and ack_topic must be different")
        if (
            self.source_host == self.destination_host
            and self.source_port == self.destination_port
            and self.source_client_id is not None
            and self.source_client_id == self.destination_client_id
        ):
            raise ConfigError(
                "source_client_id and destination_client_id must differ when "
                "both clients use the same broker"
            )
        object.__setattr__(self, "qos", _qos_one(self.qos))
        object.__setattr__(self, "keepalive", _keepalive(self.keepalive))
        object.__setattr__(
            self,
            "destination_publish_timeout",
            _positive_number(
                self.destination_publish_timeout, "destination_publish_timeout"
            ),
        )
        object.__setattr__(
            self,
            "source_ack_publish_timeout",
            _positive_number(
                self.source_ack_publish_timeout, "source_ack_publish_timeout"
            ),
        )
        object.__setattr__(
            self,
            "retry_interval",
            _positive_number(self.retry_interval, "retry_interval"),
        )
        reconnect_minimum = _positive_number(
            self.reconnect_min_delay, "reconnect_min_delay"
        )
        reconnect_maximum = _positive_number(
            self.reconnect_max_delay, "reconnect_max_delay"
        )
        _validate_reconnect_range(reconnect_minimum, reconnect_maximum)
        object.__setattr__(self, "reconnect_min_delay", reconnect_minimum)
        object.__setattr__(self, "reconnect_max_delay", reconnect_maximum)
        object.__setattr__(
            self,
            "max_queue_size",
            _positive_integer(self.max_queue_size, "max_queue_size"),
        )
        object.__setattr__(self, "debug", _debug_flag(self.debug))
        object.__setattr__(
            self, "log_level", _log_level(self.log_level, self.debug)
        )

    @property
    def data_topic(self) -> str:
        """Deprecated alias for :attr:`envelope_topic`."""

        warn_deprecated_attribute(
            owner="BridgeConfig", old_name="data_topic", new_name="envelope_topic"
        )
        return self.envelope_topic

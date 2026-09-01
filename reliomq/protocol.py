"""Wire protocol for reliable MQTT messages and acknowledgements.

Three envelope shapes travel over MQTT, all sharing one field that lets a
single message be traced end-to-end: ``message_id``.

- :class:`MessageEnvelope` -- what :class:`~reliomq.sender.Sender` puts on
  the wire (on the *relay topic*, see ``SenderConfig``): the application's
  ``topic``/``payload`` plus a stable ``message_id``.
- :class:`DeliveryEnvelope` -- what :class:`~reliomq.relay.Relay` publishes
  to the final *application* topic: just ``payload`` plus the same
  ``message_id``, so a consumer can deduplicate.
- :class:`DeliveryAck` -- what the relay publishes back to the sender's
  delivery-ack topic once the destination delivery above is confirmed: only
  a ``message_id``. This is a reliomq-specific, application-level
  acknowledgement -- distinct from (and stronger than) an MQTT PUBACK,
  which only proves the broker accepted one publish.

On the wire, the JSON field is still spelled ``event_id`` for every one of
these -- that has not changed since 0.1.0, so a 0.1.x sender and a 0.3.x
relay (or vice versa) remain fully interoperable across a rolling upgrade.
Only the Python-facing name changed, because "event" reads as "something
happened" when what this really is is a stable ID for *one message*.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from ._compat import (
    deprecated_function_alias,
    resolve_renamed_argument,
    warn_deprecated_attribute,
)


PROTOCOL_VERSION = 1
MESSAGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
# Deprecated alias; the pattern itself never changed.
EVENT_ID_PATTERN = MESSAGE_ID_PATTERN

JsonScalar: TypeAlias = bool | int | float | str | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
WireData: TypeAlias = str | bytes | bytearray | memoryview


class ProtocolError(ValueError):
    """Raised when a value is not valid for the reliability wire protocol."""


def new_message_id() -> str:
    """Return a compact UUID suitable for stable correlation across retries."""

    return uuid.uuid4().hex


def validate_message_id(value: Any) -> str:
    if not isinstance(value, str) or MESSAGE_ID_PATTERN.fullmatch(value) is None:
        raise ProtocolError(
            "message_id must contain 1-128 ASCII letters, digits, '_' or '-'"
        )
    return value


def validate_publish_topic(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ProtocolError("topic must be a non-empty string")
    if "\x00" in value or "+" in value or "#" in value:
        raise ProtocolError("topic must not contain NUL, '+' or '#'")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ProtocolError("topic must contain valid Unicode") from error
    if len(encoded) > 65535:
        raise ProtocolError("topic is too long for MQTT")
    return value


def _validate_version(value: Any) -> int:
    if type(value) is not int or value != PROTOCOL_VERSION:
        raise ProtocolError(f"version must be exactly {PROTOCOL_VERSION}")
    return value


def _validate_string(value: str, location: str) -> None:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ProtocolError(f"{location} must contain valid Unicode") from error


def _validate_json_value(
    value: Any,
    *,
    location: str = "payload",
    active_containers: set[int] | None = None,
) -> None:
    """Reject Python conveniences that do not have an exact JSON representation."""

    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ProtocolError(f"{location} contains a non-finite number")
        return
    if type(value) is str:
        _validate_string(value, location)
        return

    if active_containers is None:
        active_containers = set()

    if type(value) is list:
        identity = id(value)
        if identity in active_containers:
            raise ProtocolError(f"{location} contains a circular reference")
        active_containers.add(identity)
        try:
            for index, item in enumerate(value):
                _validate_json_value(
                    item,
                    location=f"{location}[{index}]",
                    active_containers=active_containers,
                )
        finally:
            active_containers.remove(identity)
        return

    if type(value) is dict:
        identity = id(value)
        if identity in active_containers:
            raise ProtocolError(f"{location} contains a circular reference")
        active_containers.add(identity)
        try:
            for key, item in value.items():
                if type(key) is not str:
                    raise ProtocolError(f"{location} object keys must be strings")
                _validate_string(key, f"{location} object key")
                _validate_json_value(
                    item,
                    location=f"{location}.{key}",
                    active_containers=active_containers,
                )
        finally:
            active_containers.remove(identity)
        return

    raise ProtocolError(
        f"{location} has unsupported type {type(value).__name__}; "
        "use only JSON null, booleans, numbers, strings, arrays and objects"
    )


def validate_json_value(value: Any) -> None:
    """Validate an arbitrary strict JSON value and normalize recursion errors."""

    try:
        _validate_json_value(value)
    except RecursionError as error:
        raise ProtocolError("payload exceeds the supported nesting depth") from error


def _canonical_bytes(value: dict[str, JsonValue]) -> bytes:
    try:
        text = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return text.encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as error:
        raise ProtocolError(f"value cannot be encoded as strict JSON: {error}") from error


def _reject_constant(value: str) -> None:
    raise ProtocolError(f"non-standard JSON number is not allowed: {value}")


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _decode_json(data: WireData) -> Any:
    if isinstance(data, str):
        text = data
    elif isinstance(data, memoryview):
        try:
            text = data.tobytes().decode("utf-8")
        except UnicodeDecodeError as error:
            raise ProtocolError(f"wire payload is not valid UTF-8: {error}") from error
    elif isinstance(data, (bytes, bytearray)):
        try:
            text = bytes(data).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ProtocolError(f"wire payload is not valid UTF-8: {error}") from error
    else:
        raise ProtocolError("wire payload must be str or bytes-like")

    try:
        return json.loads(
            text,
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except ProtocolError:
        raise
    except (json.JSONDecodeError, RecursionError) as error:
        raise ProtocolError(f"wire payload is not valid JSON: {error}") from error


def _require_schema(value: Any, expected_keys: frozenset[str], name: str) -> None:
    if type(value) is not dict:
        raise ProtocolError(f"{name} must be a JSON object")
    actual_keys = frozenset(value)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        details: list[str] = []
        if missing:
            details.append(f"missing={missing}")
        if extra:
            details.append(f"extra={extra}")
        raise ProtocolError(f"invalid {name} schema ({', '.join(details)})")


def _resolve_message_id(
    message_id: str | None, event_id: str | None, *, owner: str
) -> str:
    """Resolve message_id vs. the deprecated event_id keyword.

    A fresh ID is only generated when neither was supplied.
    """

    if message_id is not None and event_id is None:
        return message_id

    resolved = resolve_renamed_argument(
        new_value=message_id,
        old_value=event_id,
        new_name="message_id",
        old_name="event_id",
        owner=owner,
        default="",
        error_cls=ProtocolError,
    )
    return resolved or new_message_id()


@dataclass(frozen=True, slots=True, init=False)
class MessageEnvelope:
    """A durable source message, including its destination MQTT topic.

    ``message_id`` is the stable identifier a caller can follow through logs
    end-to-end: it is generated automatically unless supplied, and it never
    changes across retries of the same message.
    """

    topic: str
    payload: JsonValue
    message_id: str
    version: int

    def __init__(
        self,
        topic: str,
        payload: JsonValue,
        message_id: str | None = None,
        version: int = PROTOCOL_VERSION,
        *,
        event_id: str | None = None,
    ) -> None:
        resolved_id = _resolve_message_id(
            message_id, event_id, owner="MessageEnvelope"
        )
        object.__setattr__(self, "topic", topic)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "message_id", resolved_id)
        object.__setattr__(self, "version", version)
        self.__post_init__()

    def __post_init__(self) -> None:
        validate_publish_topic(self.topic)
        validate_message_id(self.message_id)
        _validate_version(self.version)
        validate_json_value(self.payload)

    @property
    def event_id(self) -> str:
        """Deprecated alias for :attr:`message_id`."""

        warn_deprecated_attribute(
            owner="MessageEnvelope", old_name="event_id", new_name="message_id"
        )
        return self.message_id

    def to_bytes(self) -> bytes:
        return encode_message(self)

    @classmethod
    def from_bytes(cls, data: WireData) -> MessageEnvelope:
        return decode_message(data)


@dataclass(frozen=True, slots=True, init=False)
class DeliveryEnvelope:
    """Destination payload retaining the correlation ID for deduplication."""

    message_id: str
    payload: JsonValue
    version: int

    def __init__(
        self,
        message_id: str | None = None,
        payload: JsonValue = None,
        version: int = PROTOCOL_VERSION,
        *,
        event_id: str | None = None,
    ) -> None:
        resolved_id = _resolve_message_id(
            message_id, event_id, owner="DeliveryEnvelope"
        )
        object.__setattr__(self, "message_id", resolved_id)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "version", version)
        self.__post_init__()

    def __post_init__(self) -> None:
        validate_message_id(self.message_id)
        _validate_version(self.version)
        validate_json_value(self.payload)

    @property
    def event_id(self) -> str:
        """Deprecated alias for :attr:`message_id`."""

        warn_deprecated_attribute(
            owner="DeliveryEnvelope", old_name="event_id", new_name="message_id"
        )
        return self.message_id

    def to_bytes(self) -> bytes:
        return encode_delivery(self)

    @classmethod
    def from_bytes(cls, data: WireData) -> DeliveryEnvelope:
        return decode_delivery(data)


@dataclass(frozen=True, slots=True, init=False)
class DeliveryAck:
    """A reliomq-specific, application-level end-to-end acknowledgement.

    Correlated solely by a stable ``message_id``. Published by a
    :class:`~reliomq.relay.Relay` back to the sender's delivery-ack topic
    once (and only once) the destination publish has itself been QoS 1
    confirmed. Not the same thing as an MQTT PUBACK: a PUBACK proves a
    broker accepted one publish; a ``DeliveryAck`` proves the message
    actually reached its real destination topic.
    """

    message_id: str
    version: int

    def __init__(
        self,
        message_id: str | None = None,
        version: int = PROTOCOL_VERSION,
        *,
        event_id: str | None = None,
    ) -> None:
        resolved_id = _resolve_message_id(message_id, event_id, owner="DeliveryAck")
        object.__setattr__(self, "message_id", resolved_id)
        object.__setattr__(self, "version", version)
        self.__post_init__()

    def __post_init__(self) -> None:
        validate_message_id(self.message_id)
        _validate_version(self.version)

    @property
    def event_id(self) -> str:
        """Deprecated alias for :attr:`message_id`."""

        warn_deprecated_attribute(
            owner="DeliveryAck", old_name="event_id", new_name="message_id"
        )
        return self.message_id

    def to_bytes(self) -> bytes:
        return encode_ack(self)

    @classmethod
    def from_bytes(cls, data: WireData) -> DeliveryAck:
        return decode_ack(data)


# Deprecated (0.1.x/0.2.x) alias -- same class, same wire format.
Ack = DeliveryAck


def encode_message(envelope: MessageEnvelope) -> bytes:
    if not isinstance(envelope, MessageEnvelope):
        raise ProtocolError("encode_message requires a MessageEnvelope")
    return _canonical_bytes(
        {
            "version": envelope.version,
            "event_id": envelope.message_id,
            "topic": envelope.topic,
            "payload": envelope.payload,
        }
    )


def decode_message(data: WireData) -> MessageEnvelope:
    value = _decode_json(data)
    _require_schema(
        value,
        frozenset(("version", "event_id", "topic", "payload")),
        "message envelope",
    )
    return MessageEnvelope(
        version=value["version"],
        message_id=value["event_id"],
        topic=value["topic"],
        payload=value["payload"],
    )


def encode_delivery(envelope: DeliveryEnvelope) -> bytes:
    if not isinstance(envelope, DeliveryEnvelope):
        raise ProtocolError("encode_delivery requires a DeliveryEnvelope")
    return _canonical_bytes(
        {
            "version": envelope.version,
            "event_id": envelope.message_id,
            "payload": envelope.payload,
        }
    )


def decode_delivery(data: WireData) -> DeliveryEnvelope:
    value = _decode_json(data)
    _require_schema(
        value,
        frozenset(("version", "event_id", "payload")),
        "delivery envelope",
    )
    return DeliveryEnvelope(
        version=value["version"],
        message_id=value["event_id"],
        payload=value["payload"],
    )


def encode_ack(ack: DeliveryAck) -> bytes:
    if not isinstance(ack, DeliveryAck):
        raise ProtocolError("encode_ack requires a DeliveryAck")
    return _canonical_bytes({"version": ack.version, "event_id": ack.message_id})


def decode_ack(data: WireData) -> DeliveryAck:
    value = _decode_json(data)
    _require_schema(value, frozenset(("version", "event_id")), "acknowledgement")
    return DeliveryAck(version=value["version"], message_id=value["event_id"])


# ---------------------------------------------------------------------------
# Deprecated pre-0.2 names. These still work, but warn and will be removed in
# a future release; prefer the message_id-based names above.
# ---------------------------------------------------------------------------


new_event_id = deprecated_function_alias(
    new_message_id,
    old_name="new_event_id",
    new_name="new_message_id",
    owner="reliomq.protocol",
)
validate_event_id = deprecated_function_alias(
    validate_message_id,
    old_name="validate_event_id",
    new_name="validate_message_id",
    owner="reliomq.protocol",
)

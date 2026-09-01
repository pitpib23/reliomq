"""Wire protocol for reliable MQTT messages and acknowledgements."""

from __future__ import annotations

import json
import math
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, TypeAlias


PROTOCOL_VERSION = 1
EVENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

JsonScalar: TypeAlias = bool | int | float | str | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
WireData: TypeAlias = str | bytes | bytearray | memoryview


class ProtocolError(ValueError):
    """Raised when a value is not valid for the reliability wire protocol."""


def new_event_id() -> str:
    """Return a compact UUID suitable for stable correlation across retries."""

    return uuid.uuid4().hex


def validate_event_id(value: Any) -> str:
    if not isinstance(value, str) or EVENT_ID_PATTERN.fullmatch(value) is None:
        raise ProtocolError(
            "event_id must contain 1-128 ASCII letters, digits, '_' or '-'"
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


@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    """A durable source message, including its destination MQTT topic."""

    topic: str
    payload: JsonValue
    event_id: str = field(default_factory=new_event_id)
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        validate_publish_topic(self.topic)
        validate_event_id(self.event_id)
        _validate_version(self.version)
        validate_json_value(self.payload)

    def to_bytes(self) -> bytes:
        return encode_message(self)

    @classmethod
    def from_bytes(cls, data: WireData) -> MessageEnvelope:
        return decode_message(data)


@dataclass(frozen=True, slots=True)
class DeliveryEnvelope:
    """Destination payload retaining the correlation ID for deduplication."""

    event_id: str
    payload: JsonValue
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        validate_event_id(self.event_id)
        _validate_version(self.version)
        validate_json_value(self.payload)

    def to_bytes(self) -> bytes:
        return encode_delivery(self)

    @classmethod
    def from_bytes(cls, data: WireData) -> DeliveryEnvelope:
        return decode_delivery(data)


@dataclass(frozen=True, slots=True)
class Ack:
    """A source acknowledgement correlated solely by a stable event ID."""

    event_id: str
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        validate_event_id(self.event_id)
        _validate_version(self.version)

    def to_bytes(self) -> bytes:
        return encode_ack(self)

    @classmethod
    def from_bytes(cls, data: WireData) -> Ack:
        return decode_ack(data)


def encode_message(envelope: MessageEnvelope) -> bytes:
    if not isinstance(envelope, MessageEnvelope):
        raise ProtocolError("encode_message requires a MessageEnvelope")
    return _canonical_bytes(
        {
            "version": envelope.version,
            "event_id": envelope.event_id,
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
        event_id=value["event_id"],
        topic=value["topic"],
        payload=value["payload"],
    )


def encode_delivery(envelope: DeliveryEnvelope) -> bytes:
    if not isinstance(envelope, DeliveryEnvelope):
        raise ProtocolError("encode_delivery requires a DeliveryEnvelope")
    return _canonical_bytes(
        {
            "version": envelope.version,
            "event_id": envelope.event_id,
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
        event_id=value["event_id"],
        payload=value["payload"],
    )


def encode_ack(ack: Ack) -> bytes:
    if not isinstance(ack, Ack):
        raise ProtocolError("encode_ack requires an Ack")
    return _canonical_bytes({"version": ack.version, "event_id": ack.event_id})


def decode_ack(data: WireData) -> Ack:
    value = _decode_json(data)
    _require_schema(value, frozenset(("version", "event_id")), "acknowledgement")
    return Ack(version=value["version"], event_id=value["event_id"])

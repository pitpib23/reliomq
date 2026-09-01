"""Small Paho MQTT helpers shared by reliable publisher and bridge clients."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any, TypeAlias

import paho.mqtt.client as mqtt


logger = logging.getLogger(__name__)

ClientFactory: TypeAlias = Callable[..., mqtt.Client]


def default_client_factory(
    *, client_id: str, userdata: Any = None
) -> mqtt.Client:
    """Create a Paho 2.x client using the VERSION2 callback API."""

    return mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        userdata=userdata,
    )


def create_client(
    client_factory: ClientFactory | None,
    *,
    client_id: str,
    userdata: Any = None,
) -> mqtt.Client:
    """Create a client through an injectable keyword-based factory."""

    factory = client_factory or default_client_factory
    return factory(client_id=client_id, userdata=userdata)


def reason_code_is_success(reason_code: Any) -> bool:
    """Return whether a Paho connect/disconnect reason represents success."""

    is_failure = getattr(reason_code, "is_failure", None)
    if is_failure is not None:
        return not bool(is_failure)
    try:
        return int(reason_code) == 0
    except (TypeError, ValueError):
        return reason_code == 0


def suback_is_success(reason_codes: Sequence[Any] | Any) -> bool:
    """Return whether every subscription in a VERSION2 SUBACK was granted."""

    if reason_codes is None:
        return False
    if isinstance(reason_codes, (str, bytes, bytearray)) or not isinstance(
        reason_codes, Sequence
    ):
        reason_codes = (reason_codes,)
    if not reason_codes:
        return False

    for reason_code in reason_codes:
        is_failure = getattr(reason_code, "is_failure", None)
        if is_failure is not None:
            if bool(is_failure):
                return False
            continue
        try:
            # MQTT 3 SUBACK grants QoS 0, 1, or 2; 0x80 means failure.
            if int(reason_code) not in (0, 1, 2):
                return False
        except (TypeError, ValueError):
            return False
    return True


def confirmed_publish(
    client: mqtt.Client,
    topic: str,
    payload: str | bytes | bytearray | None,
    *,
    qos: int = 1,
    retain: bool = False,
    timeout: float,
) -> bool:
    """Publish and wait until Paho confirms completion.

    For QoS 1 this includes receipt of PUBACK from the connected broker.  It
    does not mean a downstream application consumed the message.
    """

    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return False
        info.wait_for_publish(timeout=timeout)
        return bool(info.is_published())
    except Exception:
        logger.exception("MQTT publish confirmation failed for topic %s", topic)
        return False


# Readable aliases for callers that prefer predicate-first naming.
is_success_reason_code = reason_code_is_success
is_success_suback = suback_is_success

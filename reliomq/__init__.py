"""Durable, application-acknowledged MQTT delivery primitives.

Start with :class:`Sender` and :class:`SenderConfig`::

    from reliomq import Sender, SenderConfig

    with Sender(SenderConfig(host="localhost", outbox_path="pending.jsonl")) as sender:
        message_id = sender.publish("factory/machine1/data", {"temperature": 25.2})
        sender.wait_for_delivery(message_id, timeout=10.0)

If you know `paho-mqtt`, the lifecycle should feel familiar: ``connect()``,
``loop_start()``, ``publish()``, ``loop_stop()``, ``disconnect()``,
``is_connected()`` all exist and mean approximately what you'd expect --
see the README's "If you already know Paho MQTT" section for exactly where
reliomq's stronger delivery guarantees make ``publish()`` behave
differently from plain MQTT.

:class:`Relay` (with :class:`RelayConfig`) is the optional second half: an
end-to-end forwarder between two brokers. :class:`Outbox` and
:class:`~reliomq.protocol.DeliveryAck` are the reliomq-specific durability
and acknowledgement primitives underneath both -- see the README's
"Architecture overview" for what each is responsible for.

Call :func:`enable_logging` (or set ``debug=True``/``log_level=`` on a
config object) for a runtime view of what the library is doing with no
``logging`` setup required.

Earlier names (``ReliablePublisher``, ``PublisherConfig``,
``ReliabilityConfig``, ``ReliableMqttBridge``, ``BridgeConfig``,
``DurableMessageStore``, ``Ack``, ``queue_path=``, ``envelope_topic=``,
``data_topic=``, ``ack_topic=``, ``event_id=``) still work in 0.3.0 -- they
now emit :class:`DeprecationWarning` and point at their replacement. See
``CHANGELOG.md`` for the full migration guide.
"""

from .config import (
    BridgeConfig,
    ConfigError,
    PublisherConfig,
    RelayConfig,
    ReliabilityConfig,
    SenderConfig,
)
from .observability import enable_logging
from .outbox import DurableMessageStore, Outbox, OutboxError, StoreError
from .protocol import (
    Ack,
    DeliveryAck,
    DeliveryEnvelope,
    MessageEnvelope,
    ProtocolError,
)
from .relay import Relay, ReliableMqttBridge
from .sender import DeliveryStatus, ReliablePublisher, Sender

__all__ = [
    # Primary API
    "Sender",
    "SenderConfig",
    "Relay",
    "RelayConfig",
    "Outbox",
    "OutboxError",
    "DeliveryAck",
    "DeliveryEnvelope",
    "MessageEnvelope",
    "ProtocolError",
    "ConfigError",
    "DeliveryStatus",
    "enable_logging",
    # Deprecated aliases, kept for backward compatibility.
    "ReliablePublisher",
    "PublisherConfig",
    "ReliabilityConfig",
    "ReliableMqttBridge",
    "BridgeConfig",
    "DurableMessageStore",
    "StoreError",
    "Ack",
]

__version__ = "0.4.0"

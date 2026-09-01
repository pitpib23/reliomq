"""Durable, application-acknowledged MQTT delivery primitives.

Start with :class:`ReliablePublisher` and :class:`PublisherConfig` -- see the
README's "How reliomq works" and "Getting started" sections for the full
picture. Call :func:`enable_logging` (or set ``debug=True``/``log_level=``
on a config object) for a runtime view of what the library is doing with no
``logging`` setup required.

v0.1.0 names (``ReliabilityConfig``, ``event_id=`` keywords, ``data_topic``)
still work in v0.2.0 -- they now emit :class:`DeprecationWarning` and point
at their v0.2 replacement. See ``CHANGELOG.md`` for the full migration guide.
"""

from .bridge import ReliableMqttBridge
from .config import (
    BridgeConfig,
    ConfigError,
    PublisherConfig,
    ReliabilityConfig,
)
from .observability import enable_logging
from .protocol import (
    Ack,
    DeliveryEnvelope,
    MessageEnvelope,
    ProtocolError,
)
from .publisher import DeliveryStatus, ReliablePublisher
from .store import DurableMessageStore, StoreError

__all__ = [
    "Ack",
    "BridgeConfig",
    "ConfigError",
    "DeliveryEnvelope",
    "DeliveryStatus",
    "DurableMessageStore",
    "MessageEnvelope",
    "ProtocolError",
    "PublisherConfig",
    "ReliableMqttBridge",
    "ReliablePublisher",
    "StoreError",
    "enable_logging",
    # Deprecated (v0.1) aliases, kept for backward compatibility.
    "ReliabilityConfig",
]

__version__ = "0.2.0"

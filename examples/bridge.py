"""Minimal source-to-destination reliable bridge service.

`log_level="INFO"` narrates both broker connections, each forwarded message,
and each confirmed source ACK -- run this alongside a publisher (e.g.
basic.py, pointed at the same envelope_topic/ack_topic) to watch a message
cross from source to destination broker.
"""

import threading

from reliomq import BridgeConfig, ReliableMqttBridge


config = BridgeConfig(
    source_host="localhost",
    source_port=1883,
    destination_host="mqtt.example.net",
    destination_port=1883,
    envelope_topic="reliable/ingress",
    ack_topic="reliable/acks",
    log_level="INFO",
)

bridge = ReliableMqttBridge(config)
try:
    bridge.start()
    threading.Event().wait()
except KeyboardInterrupt:
    pass
finally:
    bridge.stop()

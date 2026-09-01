"""Minimal source-to-destination reliable bridge service."""

import threading

from reliomq import BridgeConfig, ReliableMqttBridge


config = BridgeConfig(
    source_host="localhost",
    source_port=1883,
    destination_host="mqtt.example.net",
    destination_port=1883,
    data_topic="reliable/ingress",
    ack_topic="reliable/acks",
)

bridge = ReliableMqttBridge(config)
try:
    bridge.start()
    threading.Event().wait()
except KeyboardInterrupt:
    pass
finally:
    bridge.stop()


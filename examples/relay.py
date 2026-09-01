"""Minimal source-to-destination Relay service.

`log_level="INFO"` narrates both broker connections, each forwarded
message, and each DeliveryAck sent -- run this alongside a Sender (e.g.
basic.py, pointed at the same relay_topic/delivery_ack_topic) to watch a
message cross from source to destination broker.

Relay's lifecycle is Paho-familiar (`connect()`/`loop_start()`/
`loop_stop()`/`disconnect()` all work, same as `start()`/`stop()`), but
unlike Sender there is only one lifecycle call: `connect()` brings up
*both* the source and destination broker connections together, because a
message sitting connected-but-unforwarded is exactly the situation this
library exists to avoid leaving unresolved. See `relay.source_connected`/
`relay.destination_connected` if you need to tell the two apart.
"""

import threading

from reliomq import Relay, RelayConfig


config = RelayConfig(
    source_host="localhost",
    source_port=1883,
    destination_host="mqtt.example.net",
    destination_port=1883,
    relay_topic="reliable/ingress",
    delivery_ack_topic="reliable/acks",
    log_level="INFO",
)

relay = Relay(config)
try:
    relay.connect()
    threading.Event().wait()
except KeyboardInterrupt:
    pass
finally:
    relay.disconnect()

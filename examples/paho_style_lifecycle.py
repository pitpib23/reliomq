"""The explicit `paho-mqtt`-style lifecycle, shown side by side with the
context-manager form basic.py uses.

Both forms are equivalent -- `with Sender(config) as sender:` simply calls
`connect()` on entry and `disconnect()` on exit for you. `connect()`,
`start()`, and `loop_start()` are three names for the exact same
operation; likewise `disconnect()`, `stop()`, and `loop_stop()`. reliomq
cannot honestly offer Paho's finer-grained split (connect without running
the background worker) because durable delivery *is* that worker -- see the
`Sender` docstring and the README's "If you already know Paho MQTT"
section for why.
"""

from reliomq import Sender, SenderConfig


config = SenderConfig(
    host="localhost",
    outbox_path="mqtt_pending.jsonl",
    log_level="INFO",
)

# --- Paho-familiar explicit lifecycle -------------------------------------
sender = Sender(config)
sender.connect()
sender.loop_start()  # harmless no-op here -- connect() already did this

try:
    message_id = sender.publish(
        "factory/machine1/data",
        {"temperature": 25.2},
    )
    sender.wait_for_delivery(message_id, timeout=10.0)
finally:
    sender.loop_stop()  # harmless no-op if disconnect() runs right after
    sender.disconnect()

# --- Equivalent context-manager form ---------------------------------------
with Sender(config) as sender:
    message_id = sender.publish(
        "factory/machine1/data",
        {"temperature": 25.2},
    )
    sender.wait_for_delivery(message_id, timeout=10.0)

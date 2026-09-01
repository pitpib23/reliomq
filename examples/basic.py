"""Minimal durable Sender example.

This is the shortest useful shape: build a config, use Sender as a context
manager, publish, optionally wait for delivery. `log_level=` turns on
reliomq's INFO-level lifecycle logging with no `logging` setup of your
own -- run this script and watch stderr to see it connect, store the
message in the Outbox, get the broker PUBACK, wait for the DeliveryAck, and
confirm delivery. See debug_logging.py for the deeper DEBUG-level view, and
paho_style_lifecycle.py for the explicit connect()/loop_start() shape.
"""

from reliomq import Sender, SenderConfig


config = SenderConfig(
    host="localhost",
    port=1883,
    outbox_path="mqtt_pending.jsonl",
    relay_topic="reliable/ingress",
    delivery_ack_topic="reliable/acks",
    ack_timeout=3.0,
    retry_interval=10.0,
    log_level="INFO",
)

with Sender(config) as sender:
    # publish() means reliomq accepted the message into its durable
    # delivery workflow -- not that it has arrived anywhere yet.
    message_id = sender.publish(
        "factory/machine1/data",
        {"temperature": 25.2, "pressure": 4.1},
    )
    print(f"durably accepted as {message_id}")

    # wait_for_delivery() is reliomq-specific: it blocks until a
    # DeliveryAck confirms the message actually reached its destination.
    if sender.wait_for_delivery(message_id, timeout=10.0):
        print("destination broker delivery confirmed")
    else:
        print("still pending; the same ID will be retried after restart")

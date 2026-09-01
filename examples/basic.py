"""Minimal durable publisher example.

This is the canonical "getting started" shape: build a config, start the
publisher, publish, optionally wait for delivery, then stop. `log_level=`
turns on reliomq's INFO-level lifecycle logging with no `logging` setup of
your own -- run this script and watch stderr to see it connect, queue the
message, get the broker PUBACK, wait for the application ACK, and confirm
delivery. See debug_logging.py for the deeper DEBUG-level view.
"""

from reliomq import PublisherConfig, ReliablePublisher


config = PublisherConfig(
    host="localhost",
    port=1883,
    queue_path="mqtt_pending.jsonl",
    envelope_topic="reliable/ingress",
    ack_topic="reliable/acks",
    ack_timeout=3.0,
    retry_interval=10.0,
    log_level="INFO",
)

with ReliablePublisher(config) as publisher:
    message_id = publisher.publish(
        topic="factory/machine1/data",
        payload={"temperature": 25.2, "pressure": 4.1},
    )
    print(f"durably accepted as {message_id}")

    if publisher.wait_for_delivery(message_id, timeout=10.0):
        print("destination broker delivery confirmed")
    else:
        print("still pending; the same ID will be retried after restart")

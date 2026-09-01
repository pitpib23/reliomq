"""Minimal durable publisher example."""

from reliomq import ReliabilityConfig, ReliablePublisher


config = ReliabilityConfig(
    host="localhost",
    port=1883,
    queue_path="mqtt_pending.jsonl",
    data_topic="reliable/ingress",
    ack_topic="reliable/acks",
    ack_timeout=3.0,
    retry_interval=10.0,
)

with ReliablePublisher(config) as publisher:
    event_id = publisher.publish(
        topic="factory/machine1/data",
        payload={"temperature": 25.2, "pressure": 4.1},
    )
    print(f"durably accepted as {event_id}")

    if publisher.wait_for_delivery(event_id, timeout=10.0):
        print("destination broker delivery confirmed")
    else:
        print("still pending; the same ID will be retried after restart")


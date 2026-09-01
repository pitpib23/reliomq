"""Manual, one-file demonstration of the full delivery path.

Requires a real local Mosquitto broker (or any MQTT 3.1.1/5 broker) reachable
at localhost:1883 -- this is NOT part of the automated test suite, which uses
fake clients for determinism. Use this script to see actual PUBACKs, actual
reconnect/backoff, and actual queue files on disk.

    mosquitto -p 1883 &
    python examples/local_end_to_end.py

It starts a ReliableMqttBridge (source and destination both on the same
local broker, different topics, to avoid requiring two brokers), starts a
plain subscriber standing in for the final consumer, then publishes a few
messages through a ReliablePublisher and prints what each side observed.

Try killing the broker mid-run (Ctrl+C the mosquitto process, restart it) to
watch pending_count() rise while the broker is down and then drain back to
zero once it is back, without losing or duplicating in this simple demo.
"""

from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from reliomq import (
    BridgeConfig,
    ReliabilityConfig,
    ReliableMqttBridge,
    ReliablePublisher,
)
from reliomq.protocol import DeliveryEnvelope


logging.basicConfig(level=logging.INFO)

BROKER_HOST = "localhost"
BROKER_PORT = 1883
DESTINATION_TOPIC = "demo/machine1/data"
DATA_TOPIC = "reliable/demo/ingress"
ACK_TOPIC = "reliable/demo/acks"


def start_consumer() -> list[DeliveryEnvelope]:
    """A minimal final consumer; see consumer_dedup.py for the real pattern."""

    received: list[DeliveryEnvelope] = []

    def on_message(_client, _userdata, message) -> None:
        received.append(DeliveryEnvelope.from_bytes(message.payload))

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.on_connect = lambda c, _u, _f, _r, _p: c.subscribe(
        DESTINATION_TOPIC, qos=1
    )
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()
    return received


def main() -> None:
    queue_directory = tempfile.mkdtemp(prefix="reliomq-demo-")
    queue_path = Path(queue_directory) / "pending.jsonl"
    print(f"durable queue file: {queue_path}")

    received = start_consumer()

    bridge = ReliableMqttBridge(
        BridgeConfig(
            source_host=BROKER_HOST,
            source_port=BROKER_PORT,
            destination_host=BROKER_HOST,
            destination_port=BROKER_PORT,
            data_topic=DATA_TOPIC,
            ack_topic=ACK_TOPIC,
        )
    )
    bridge.start()

    publisher = ReliablePublisher(
        ReliabilityConfig(
            host=BROKER_HOST,
            port=BROKER_PORT,
            queue_path=queue_path,
            data_topic=DATA_TOPIC,
            ack_topic=ACK_TOPIC,
            ack_timeout=5.0,
            retry_interval=3.0,
        )
    )
    publisher.start()

    try:
        for reading_number in range(1, 4):
            event_id = publisher.publish(
                topic=DESTINATION_TOPIC,
                payload={"reading": reading_number, "value": reading_number * 1.5},
            )
            print(f"published {event_id}, pending={publisher.pending_count()}")

        delivered = publisher.wait_for_delivery(timeout=15.0)
        print(f"all delivered: {delivered}, pending={publisher.pending_count()}")
        time.sleep(0.5)  # let the consumer's callback thread catch up
        print(f"consumer received {len(received)} message(s):")
        for envelope in received:
            print(f"  event_id={envelope.event_id} payload={envelope.payload}")
    finally:
        publisher.stop()
        bridge.stop()


if __name__ == "__main__":
    main()

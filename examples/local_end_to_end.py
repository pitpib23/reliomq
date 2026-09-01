"""Manual, one-file demonstration of the full delivery path.

Requires a real local Mosquitto broker (or any MQTT 3.1.1/5 broker) reachable
at localhost:1883 -- this is NOT part of the automated test suite, which uses
fake clients for determinism. Use this script to see actual PUBACKs, actual
reconnect/backoff, and actual Outbox files on disk. Both components have
`log_level="INFO"` set, so you will also see reliomq's own narration of the
whole lifecycle interleaved with the print()s below; set `debug=True`
instead for the deeper DEBUG-level view (see debug_logging.py).

    mosquitto -p 1883 &
    python examples/local_end_to_end.py

It starts a Relay (source and destination both on the same local broker,
different topics, to avoid requiring two brokers), starts a plain
subscriber standing in for the final consumer, then publishes a few
messages through a Sender and prints what each side observed.

Try killing the broker mid-run (Ctrl+C the mosquitto process, restart it) to
watch pending_count() rise while the broker is down and then drain back to
zero once it is back, without losing or duplicating in this simple demo.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from reliomq import Relay, RelayConfig, Sender, SenderConfig
from reliomq.protocol import DeliveryEnvelope


BROKER_HOST = "localhost"
BROKER_PORT = 1883
DESTINATION_TOPIC = "demo/machine1/data"
RELAY_TOPIC = "reliable/demo/ingress"
DELIVERY_ACK_TOPIC = "reliable/demo/acks"


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
    outbox_directory = tempfile.mkdtemp(prefix="reliomq-demo-")
    outbox_path = Path(outbox_directory) / "pending.jsonl"
    print(f"Outbox file: {outbox_path}")

    received = start_consumer()

    relay = Relay(
        RelayConfig(
            source_host=BROKER_HOST,
            source_port=BROKER_PORT,
            destination_host=BROKER_HOST,
            destination_port=BROKER_PORT,
            relay_topic=RELAY_TOPIC,
            delivery_ack_topic=DELIVERY_ACK_TOPIC,
            log_level="INFO",
        )
    )
    relay.connect()

    sender = Sender(
        SenderConfig(
            host=BROKER_HOST,
            port=BROKER_PORT,
            outbox_path=outbox_path,
            relay_topic=RELAY_TOPIC,
            delivery_ack_topic=DELIVERY_ACK_TOPIC,
            ack_timeout=5.0,
            retry_interval=3.0,
            log_level="INFO",
        )
    )
    sender.connect()

    try:
        for reading_number in range(1, 4):
            message_id = sender.publish(
                DESTINATION_TOPIC,
                {"reading": reading_number, "value": reading_number * 1.5},
            )
            print(f"published {message_id}, pending={sender.pending_count()}")

        delivered = sender.wait_for_delivery(timeout=15.0)
        print(f"all delivered: {delivered}, pending={sender.pending_count()}")
        time.sleep(0.5)  # let the consumer's callback thread catch up
        print(f"consumer received {len(received)} message(s):")
        for envelope in received:
            print(f"  message_id={envelope.message_id} payload={envelope.payload}")
    finally:
        sender.disconnect()
        relay.disconnect()


if __name__ == "__main__":
    main()

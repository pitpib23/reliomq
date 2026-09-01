"""Destination-side consumer showing the recommended dedup pattern.

The library guarantees at-least-once delivery, not exactly-once: a Relay can
publish successfully and then fail to deliver its own DeliveryAck, causing
the source to retry an already-delivered message. This example is a plain
Paho subscriber -- it is NOT part of reliomq -- showing how any final
consumer of a Relay's destination messages should use the retained
`message_id` to make handling idempotent.

Run against the same broker/topic the Relay forwards to:

    python examples/consumer_dedup.py
"""

from __future__ import annotations

import logging
from collections import OrderedDict

import paho.mqtt.client as mqtt

from reliomq.protocol import DeliveryEnvelope, ProtocolError


logging.basicConfig(level=logging.INFO)

DESTINATION_HOST = "mqtt.example.net"
DESTINATION_PORT = 1883
DESTINATION_TOPIC = "factory/machine1/data"

# A small bounded LRU-style set is enough in most deployments; swap this for
# a persistent store (file/db) if the consumer itself must survive restarts
# without ever reprocessing a message it already handled.
MAX_REMEMBERED_IDS = 10_000
_seen_message_ids: "OrderedDict[str, None]" = OrderedDict()


def already_processed(message_id: str) -> bool:
    if message_id in _seen_message_ids:
        _seen_message_ids.move_to_end(message_id)
        return True
    _seen_message_ids[message_id] = None
    if len(_seen_message_ids) > MAX_REMEMBERED_IDS:
        _seen_message_ids.popitem(last=False)
    return False


def handle_reading(payload) -> None:
    """Replace with real business logic. Must be safe to call twice."""

    logging.info("Processing reading: %s", payload)


def on_message(_client, _userdata, message: mqtt.MQTTMessage) -> None:
    try:
        envelope = DeliveryEnvelope.from_bytes(message.payload)
    except ProtocolError as error:
        logging.warning("Ignoring malformed delivery on %s: %s", message.topic, error)
        return

    if already_processed(envelope.message_id):
        logging.info("Duplicate delivery ignored | message_id=%s", envelope.message_id)
        return

    handle_reading(envelope.payload)


def main() -> None:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.on_connect = lambda c, _u, _f, _r, _p: c.subscribe(
        DESTINATION_TOPIC, qos=1
    )

    client.connect(DESTINATION_HOST, DESTINATION_PORT)
    client.loop_forever()


if __name__ == "__main__":
    main()

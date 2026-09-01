from __future__ import annotations

import math
import unittest

from reliomq.protocol import (
    Ack,
    DeliveryEnvelope,
    MessageEnvelope,
    ProtocolError,
)


class ProtocolTests(unittest.TestCase):
    def test_message_round_trip_is_canonical_and_preserves_id(self) -> None:
        envelope = MessageEnvelope(
            event_id="event-001",
            topic="factory/line/data",
            payload={"z": [1, True, None], "a": "temperature"},
        )

        encoded = envelope.to_bytes()

        self.assertEqual(MessageEnvelope.from_bytes(encoded), envelope)
        self.assertEqual(encoded, envelope.to_bytes())
        self.assertIn(b'"event_id":"event-001"', encoded)

    def test_destination_envelope_retains_deduplication_id(self) -> None:
        delivery = DeliveryEnvelope(event_id="same-id", payload=[1, 2, 3])

        self.assertEqual(DeliveryEnvelope.from_bytes(delivery.to_bytes()), delivery)

    def test_ack_requires_exact_schema_and_valid_id(self) -> None:
        self.assertEqual(Ack.from_bytes(Ack("ack-id").to_bytes()), Ack("ack-id"))
        with self.assertRaises(ProtocolError):
            Ack.from_bytes(b'{"version":1,"event_id":"ack-id","ok":true}')
        with self.assertRaises(ProtocolError):
            Ack.from_bytes(b'{"version":1,"event_id":"bad id"}')

    def test_malformed_utf8_json_and_duplicate_keys_are_rejected(self) -> None:
        bad_values = (
            b"\xff",
            b"{not-json}",
            b'{"version":1,"event_id":"a","event_id":"b"}',
        )
        for value in bad_values:
            with self.subTest(value=value), self.assertRaises(ProtocolError):
                Ack.from_bytes(value)

    def test_only_strict_json_payload_types_are_supported(self) -> None:
        invalid_payloads = (
            (1, 2),
            {1: "non-string key"},
            math.nan,
            math.inf,
            b"bytes",
            object(),
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload), self.assertRaises(ProtocolError):
                MessageEnvelope(topic="data/out", payload=payload)  # type: ignore[arg-type]

    def test_publish_topics_cannot_be_empty_or_contain_wildcards(self) -> None:
        for topic in ("", "factory/+", "factory/#", "bad\x00topic"):
            with self.subTest(topic=topic), self.assertRaises(ProtocolError):
                MessageEnvelope(topic=topic, payload=None)


if __name__ == "__main__":
    unittest.main()


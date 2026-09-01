from __future__ import annotations

import math
import unittest
import warnings

from reliomq.protocol import (
    Ack,
    DeliveryEnvelope,
    MessageEnvelope,
    ProtocolError,
    new_event_id,
    new_message_id,
    validate_event_id,
    validate_message_id,
)


class ProtocolTests(unittest.TestCase):
    def test_message_round_trip_is_canonical_and_preserves_id(self) -> None:
        envelope = MessageEnvelope(
            message_id="event-001",
            topic="factory/line/data",
            payload={"z": [1, True, None], "a": "temperature"},
        )

        encoded = envelope.to_bytes()

        self.assertEqual(MessageEnvelope.from_bytes(encoded), envelope)
        self.assertEqual(encoded, envelope.to_bytes())
        # The wire field is still spelled "event_id" -- unchanged since
        # 0.1.0, so old and new deployments stay interoperable.
        self.assertIn(b'"event_id":"event-001"', encoded)

    def test_destination_envelope_retains_deduplication_id(self) -> None:
        delivery = DeliveryEnvelope(message_id="same-id", payload=[1, 2, 3])

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


class DeprecatedEventIdCompatTests(unittest.TestCase):
    """v0.1.0 called this identifier `event_id`; v0.2.0 calls it `message_id`.

    The wire format did not change (still `"event_id"` in the JSON), only
    the Python-facing name. These tests pin down that the old keyword and
    the old `.event_id` property both still work, both warn, and both stay
    correlated with the new `.message_id`.
    """

    def test_event_id_keyword_still_constructs_and_warns(self) -> None:
        for cls, kwargs in (
            (MessageEnvelope, {"topic": "t", "payload": 1}),
            (DeliveryEnvelope, {"payload": 1}),
            (Ack, {}),
        ):
            with self.subTest(cls=cls.__name__):
                with self.assertWarns(DeprecationWarning):
                    instance = cls(event_id="legacy-id", **kwargs)
                self.assertEqual(instance.message_id, "legacy-id")

    def test_event_id_property_reads_message_id_and_warns(self) -> None:
        envelope = MessageEnvelope(topic="t", payload=1, message_id="new-style-id")

        with self.assertWarns(DeprecationWarning):
            self.assertEqual(envelope.event_id, "new-style-id")

    def test_conflicting_message_id_and_event_id_raise(self) -> None:
        with self.assertRaises(ProtocolError):
            Ack(message_id="a", event_id="b")

    def test_matching_message_id_and_event_id_is_accepted_with_one_warning(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ack = Ack(message_id="same", event_id="same")
        self.assertEqual(ack.message_id, "same")
        self.assertTrue(
            any(issubclass(w.category, DeprecationWarning) for w in caught)
        )

    def test_new_event_id_alias_still_generates_a_valid_id_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            generated = new_event_id()

        self.assertEqual(validate_message_id(generated), generated)

    def test_validate_event_id_alias_still_validates_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            self.assertEqual(validate_event_id("ok-id"), "ok-id")

        with self.assertWarns(DeprecationWarning), self.assertRaises(ProtocolError):
            validate_event_id("bad id with spaces")

    def test_new_message_id_and_validate_message_id_do_not_warn(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any warning here fails the test
            generated = new_message_id()
            self.assertEqual(validate_message_id(generated), generated)


if __name__ == "__main__":
    unittest.main()

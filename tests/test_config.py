from __future__ import annotations

import logging
import unittest
from pathlib import Path

from reliomq.config import (
    BridgeConfig,
    ConfigError,
    PublisherConfig,
    RelayConfig,
    ReliabilityConfig,
    SenderConfig,
)


class SenderConfigTests(unittest.TestCase):
    def test_defaults_apply_and_outbox_path_is_normalized_to_a_path(self) -> None:
        config = SenderConfig(host="broker.local", outbox_path="pending.jsonl")

        self.assertEqual(config.port, 1883)
        self.assertEqual(config.qos, 1)
        self.assertEqual(config.relay_topic, "reliomq/messages")
        self.assertEqual(config.delivery_ack_topic, "reliomq/acks")
        self.assertIsInstance(config.outbox_path, Path)
        self.assertIsNone(config.client_id)
        self.assertIsNone(config.log_level)
        self.assertFalse(config.debug)

    def test_outbox_path_is_required(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(host="broker")  # type: ignore[call-arg]

    def test_is_frozen_and_hashable_like_a_value_object(self) -> None:
        config = SenderConfig(host="broker", outbox_path="q.jsonl")

        with self.assertRaises(AttributeError):
            config.host = "other"  # type: ignore[misc]

    def test_rejects_blank_or_whitespace_host(self) -> None:
        for host in ("", "   ", "has space"):
            with self.subTest(host=host), self.assertRaises(ConfigError):
                SenderConfig(host=host, outbox_path="q.jsonl")

    def test_rejects_out_of_range_or_non_integer_port(self) -> None:
        for port in (0, -1, 65536, 1883.0, "1883"):
            with self.subTest(port=port), self.assertRaises(ConfigError):
                SenderConfig(host="broker", outbox_path="q.jsonl", port=port)  # type: ignore[arg-type]

    def test_rejects_any_qos_other_than_one(self) -> None:
        for qos in (0, 2, True):
            with self.subTest(qos=qos), self.assertRaises(ConfigError):
                SenderConfig(host="broker", outbox_path="q.jsonl", qos=qos)  # type: ignore[arg-type]

    def test_rejects_wildcard_or_oversized_topics(self) -> None:
        for topic in ("factory/+", "factory/#", "x" * 65536):
            with self.subTest(topic=topic), self.assertRaises(ConfigError):
                SenderConfig(host="broker", outbox_path="q.jsonl", relay_topic=topic)

    def test_rejects_identical_relay_and_delivery_ack_topics(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(
                host="broker",
                outbox_path="q.jsonl",
                relay_topic="same/topic",
                delivery_ack_topic="same/topic",
            )

    def test_rejects_non_positive_or_non_finite_timeouts(self) -> None:
        for value in (0, -1.0, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                SenderConfig(host="broker", outbox_path="q.jsonl", ack_timeout=value)

    def test_rejects_reconnect_minimum_greater_than_maximum(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(
                host="broker",
                outbox_path="q.jsonl",
                reconnect_min_delay=10.0,
                reconnect_max_delay=1.0,
            )

    def test_rejects_empty_or_null_byte_outbox_path(self) -> None:
        for path in ("", "bad\x00path"):
            with self.subTest(path=path), self.assertRaises(ConfigError):
                SenderConfig(host="broker", outbox_path=path)

    def test_rejects_oversized_client_id(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(
                host="broker", outbox_path="q.jsonl", client_id="x" * 70000
            )

    def test_log_level_accepts_name_or_int_and_normalizes(self) -> None:
        by_name = SenderConfig(host="broker", outbox_path="q.jsonl", log_level="INFO")
        by_int = SenderConfig(host="broker", outbox_path="q.jsonl", log_level=logging.INFO)

        self.assertEqual(by_name.log_level, logging.INFO)
        self.assertEqual(by_int.log_level, logging.INFO)

    def test_debug_true_is_shorthand_for_log_level_debug(self) -> None:
        config = SenderConfig(host="broker", outbox_path="q.jsonl", debug=True)

        self.assertEqual(config.log_level, logging.DEBUG)

    def test_debug_true_conflicting_with_log_level_raises(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(
                host="broker", outbox_path="q.jsonl", debug=True, log_level="INFO"
            )

    def test_debug_true_matching_log_level_debug_is_accepted(self) -> None:
        config = SenderConfig(
            host="broker", outbox_path="q.jsonl", debug=True, log_level="DEBUG"
        )

        self.assertEqual(config.log_level, logging.DEBUG)

    def test_rejects_unrecognized_log_level_name(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(host="broker", outbox_path="q.jsonl", log_level="LOUD")

    def test_rejects_non_bool_debug(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(host="broker", outbox_path="q.jsonl", debug=1)  # type: ignore[arg-type]


class SenderConfigDeprecatedCompatTests(unittest.TestCase):
    def test_queue_path_keyword_still_works_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = SenderConfig(host="broker", queue_path="legacy.jsonl")

        self.assertTrue(str(config.outbox_path).endswith("legacy.jsonl"))

    def test_queue_path_property_reads_outbox_path_and_warns(self) -> None:
        config = SenderConfig(host="broker", outbox_path="q.jsonl")

        with self.assertWarns(DeprecationWarning):
            self.assertEqual(config.queue_path, config.outbox_path)

    def test_relay_topic_alias_chain_from_envelope_topic(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = SenderConfig(
                host="broker", outbox_path="q.jsonl", envelope_topic="v0.2-topic"
            )

        self.assertEqual(config.relay_topic, "v0.2-topic")
        with self.assertWarns(DeprecationWarning):
            self.assertEqual(config.envelope_topic, "v0.2-topic")

    def test_relay_topic_alias_chain_from_data_topic(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = SenderConfig(
                host="broker", outbox_path="q.jsonl", data_topic="v0.1-topic"
            )

        self.assertEqual(config.relay_topic, "v0.1-topic")
        with self.assertWarns(DeprecationWarning):
            self.assertEqual(config.data_topic, "v0.1-topic")

    def test_conflicting_relay_topic_and_data_topic_raise(self) -> None:
        with self.assertRaises(ConfigError):
            SenderConfig(
                host="broker",
                outbox_path="q.jsonl",
                relay_topic="a",
                data_topic="b",
            )

    def test_delivery_ack_topic_alias_chain_from_ack_topic(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = SenderConfig(host="broker", outbox_path="q.jsonl", ack_topic="legacy/ack")

        self.assertEqual(config.delivery_ack_topic, "legacy/ack")
        with self.assertWarns(DeprecationWarning):
            self.assertEqual(config.ack_topic, "legacy/ack")

    def test_publisher_config_alias_still_constructs_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = PublisherConfig(host="broker", outbox_path="q.jsonl")

        self.assertIsInstance(config, SenderConfig)

    def test_reliability_config_alias_still_constructs_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = ReliabilityConfig(host="broker", outbox_path="q.jsonl")

        self.assertIsInstance(config, SenderConfig)
        self.assertEqual(config.relay_topic, "reliomq/messages")


class RelayConfigTests(unittest.TestCase):
    def make(self, **overrides):
        values = {"source_host": "src", "destination_host": "dst"}
        values.update(overrides)
        return RelayConfig(**values)

    def test_defaults_apply(self) -> None:
        config = self.make()

        self.assertEqual(config.source_port, 1883)
        self.assertEqual(config.destination_port, 1883)
        self.assertEqual(config.max_queue_size, 1000)
        self.assertEqual(config.qos, 1)
        self.assertEqual(config.relay_topic, "reliomq/messages")
        self.assertEqual(config.delivery_ack_topic, "reliomq/acks")
        self.assertIsNone(config.log_level)

    def test_rejects_identical_relay_and_delivery_ack_topics(self) -> None:
        with self.assertRaises(ConfigError):
            self.make(relay_topic="same", delivery_ack_topic="same")

    def test_rejects_non_positive_max_queue_size(self) -> None:
        for value in (0, -1, 1.5):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                self.make(max_queue_size=value)  # type: ignore[arg-type]

    def test_rejects_same_broker_with_identical_explicit_client_ids(self) -> None:
        with self.assertRaises(ConfigError):
            self.make(
                source_host="broker",
                destination_host="broker",
                source_port=1883,
                destination_port=1883,
                source_client_id="shared",
                destination_client_id="shared",
            )

    def test_allows_same_broker_with_distinct_client_ids(self) -> None:
        config = self.make(
            source_host="broker",
            destination_host="broker",
            source_client_id="a",
            destination_client_id="b",
        )
        self.assertEqual(config.source_host, config.destination_host)

    def test_allows_same_broker_when_client_ids_are_auto_generated(self) -> None:
        # Neither side has pinned an explicit client_id, so the runtime will
        # generate two distinct random IDs; construction must not guess wrong.
        config = self.make(source_host="broker", destination_host="broker")
        self.assertIsNone(config.source_client_id)
        self.assertIsNone(config.destination_client_id)

    def test_debug_true_is_shorthand_for_log_level_debug(self) -> None:
        config = self.make(debug=True)

        self.assertEqual(config.log_level, logging.DEBUG)


class RelayConfigDeprecatedCompatTests(unittest.TestCase):
    def test_data_topic_keyword_still_works_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = RelayConfig(
                source_host="s", destination_host="d", data_topic="legacy/topic"
            )

        self.assertEqual(config.relay_topic, "legacy/topic")

    def test_bridge_config_alias_still_constructs_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = BridgeConfig(source_host="s", destination_host="d")

        self.assertIsInstance(config, RelayConfig)


if __name__ == "__main__":
    unittest.main()

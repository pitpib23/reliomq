from __future__ import annotations

import logging
import unittest
from pathlib import Path

from reliomq.config import (
    BridgeConfig,
    ConfigError,
    PublisherConfig,
    ReliabilityConfig,
)


class PublisherConfigTests(unittest.TestCase):
    def test_defaults_apply_and_queue_path_is_normalized_to_a_path(self) -> None:
        config = PublisherConfig(host="broker.local", queue_path="pending.jsonl")

        self.assertEqual(config.port, 1883)
        self.assertEqual(config.qos, 1)
        self.assertEqual(config.envelope_topic, "reliomq/messages")
        self.assertEqual(config.ack_topic, "reliomq/acks")
        self.assertIsInstance(config.queue_path, Path)
        self.assertIsNone(config.client_id)
        self.assertIsNone(config.log_level)
        self.assertFalse(config.debug)

    def test_is_frozen_and_hashable_like_a_value_object(self) -> None:
        config = PublisherConfig(host="broker", queue_path="q.jsonl")

        with self.assertRaises(AttributeError):
            config.host = "other"  # type: ignore[misc]

    def test_rejects_blank_or_whitespace_host(self) -> None:
        for host in ("", "   ", "has space"):
            with self.subTest(host=host), self.assertRaises(ConfigError):
                PublisherConfig(host=host, queue_path="q.jsonl")

    def test_rejects_out_of_range_or_non_integer_port(self) -> None:
        for port in (0, -1, 65536, 1883.0, "1883"):
            with self.subTest(port=port), self.assertRaises(ConfigError):
                PublisherConfig(host="broker", queue_path="q.jsonl", port=port)  # type: ignore[arg-type]

    def test_rejects_any_qos_other_than_one(self) -> None:
        for qos in (0, 2, True):
            with self.subTest(qos=qos), self.assertRaises(ConfigError):
                PublisherConfig(host="broker", queue_path="q.jsonl", qos=qos)  # type: ignore[arg-type]

    def test_rejects_wildcard_or_oversized_topics(self) -> None:
        for topic in ("factory/+", "factory/#", "x" * 65536):
            with self.subTest(topic=topic), self.assertRaises(ConfigError):
                PublisherConfig(host="broker", queue_path="q.jsonl", envelope_topic=topic)

    def test_rejects_identical_envelope_and_ack_topics(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(
                host="broker",
                queue_path="q.jsonl",
                envelope_topic="same/topic",
                ack_topic="same/topic",
            )

    def test_rejects_non_positive_or_non_finite_timeouts(self) -> None:
        for value in (0, -1.0, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                PublisherConfig(host="broker", queue_path="q.jsonl", ack_timeout=value)

    def test_rejects_reconnect_minimum_greater_than_maximum(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(
                host="broker",
                queue_path="q.jsonl",
                reconnect_min_delay=10.0,
                reconnect_max_delay=1.0,
            )

    def test_rejects_empty_or_null_byte_queue_path(self) -> None:
        for path in ("", "bad\x00path"):
            with self.subTest(path=path), self.assertRaises(ConfigError):
                PublisherConfig(host="broker", queue_path=path)

    def test_rejects_oversized_client_id(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(
                host="broker", queue_path="q.jsonl", client_id="x" * 70000
            )

    def test_log_level_accepts_name_or_int_and_normalizes(self) -> None:
        by_name = PublisherConfig(host="broker", queue_path="q.jsonl", log_level="INFO")
        by_int = PublisherConfig(host="broker", queue_path="q.jsonl", log_level=logging.INFO)

        self.assertEqual(by_name.log_level, logging.INFO)
        self.assertEqual(by_int.log_level, logging.INFO)

    def test_debug_true_is_shorthand_for_log_level_debug(self) -> None:
        config = PublisherConfig(host="broker", queue_path="q.jsonl", debug=True)

        self.assertEqual(config.log_level, logging.DEBUG)

    def test_debug_true_conflicting_with_log_level_raises(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(
                host="broker", queue_path="q.jsonl", debug=True, log_level="INFO"
            )

    def test_debug_true_matching_log_level_debug_is_accepted(self) -> None:
        config = PublisherConfig(
            host="broker", queue_path="q.jsonl", debug=True, log_level="DEBUG"
        )

        self.assertEqual(config.log_level, logging.DEBUG)

    def test_rejects_unrecognized_log_level_name(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(host="broker", queue_path="q.jsonl", log_level="LOUD")

    def test_rejects_non_bool_debug(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(host="broker", queue_path="q.jsonl", debug=1)  # type: ignore[arg-type]


class PublisherConfigDeprecatedCompatTests(unittest.TestCase):
    def test_data_topic_keyword_still_works_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = PublisherConfig(
                host="broker", queue_path="q.jsonl", data_topic="legacy/topic"
            )

        self.assertEqual(config.envelope_topic, "legacy/topic")

    def test_data_topic_property_reads_envelope_topic_and_warns(self) -> None:
        config = PublisherConfig(
            host="broker", queue_path="q.jsonl", envelope_topic="new/topic"
        )

        with self.assertWarns(DeprecationWarning):
            self.assertEqual(config.data_topic, "new/topic")

    def test_conflicting_envelope_topic_and_data_topic_raise(self) -> None:
        with self.assertRaises(ConfigError):
            PublisherConfig(
                host="broker",
                queue_path="q.jsonl",
                envelope_topic="a",
                data_topic="b",
            )

    def test_reliability_config_alias_still_constructs_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = ReliabilityConfig(host="broker", queue_path="q.jsonl")

        self.assertIsInstance(config, PublisherConfig)
        self.assertEqual(config.envelope_topic, "reliomq/messages")


class BridgeConfigTests(unittest.TestCase):
    def make(self, **overrides):
        values = {"source_host": "src", "destination_host": "dst"}
        values.update(overrides)
        return BridgeConfig(**values)

    def test_defaults_apply(self) -> None:
        config = self.make()

        self.assertEqual(config.source_port, 1883)
        self.assertEqual(config.destination_port, 1883)
        self.assertEqual(config.max_queue_size, 1000)
        self.assertEqual(config.qos, 1)
        self.assertEqual(config.envelope_topic, "reliomq/messages")
        self.assertIsNone(config.log_level)

    def test_rejects_identical_envelope_and_ack_topics(self) -> None:
        with self.assertRaises(ConfigError):
            self.make(envelope_topic="same", ack_topic="same")

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

    def test_data_topic_keyword_still_works_and_warns(self) -> None:
        with self.assertWarns(DeprecationWarning):
            config = self.make(data_topic="legacy/topic")

        self.assertEqual(config.envelope_topic, "legacy/topic")


if __name__ == "__main__":
    unittest.main()

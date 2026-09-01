"""Regression tests for reliomq's zero-setup observability story.

Covers: default-quiet behavior (no handler/import-time side effects),
``reliomq.observability.enable_logging`` itself, how ``PublisherConfig``/
``BridgeConfig``'s ``log_level=``/``debug=`` wire into it, and that INFO vs.
DEBUG actually carry the content the README promises for each level.
"""

from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path

from fakes import FakeClient, client_factory_for

import reliomq.observability as observability
from reliomq.bridge import ReliableMqttBridge
from reliomq.config import BridgeConfig, PublisherConfig
from reliomq.observability import PACKAGE_LOGGER_NAME, enable_logging, normalize_log_level
from reliomq.protocol import Ack, MessageEnvelope
from reliomq.publisher import ReliablePublisher


class _ObservabilityIsolation(unittest.TestCase):
    """Save/restore the "reliomq" logger + observability module globals.

    ``enable_logging`` is deliberately process-global and idempotent (that
    is the point: any number of components can call it safely), which means
    tests that exercise it must restore state afterward or they will bleed
    into unrelated tests running later in the same process.
    """

    def setUp(self) -> None:
        self.package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
        saved_handlers = list(self.package_logger.handlers)
        saved_level = self.package_logger.level
        saved_propagate = self.package_logger.propagate
        saved_module_handler = observability._handler

        for handler in saved_handlers:
            self.package_logger.removeHandler(handler)
        observability._handler = None
        self.package_logger.setLevel(logging.NOTSET)
        self.package_logger.propagate = True

        def _restore() -> None:
            for handler in list(self.package_logger.handlers):
                self.package_logger.removeHandler(handler)
            for handler in saved_handlers:
                self.package_logger.addHandler(handler)
            observability._handler = saved_module_handler
            self.package_logger.setLevel(saved_level)
            self.package_logger.propagate = saved_propagate

        self.addCleanup(_restore)


class NormalizeLogLevelTests(unittest.TestCase):
    def test_accepts_level_names_case_insensitively(self) -> None:
        self.assertEqual(normalize_log_level("info"), logging.INFO)
        self.assertEqual(normalize_log_level("DEBUG"), logging.DEBUG)
        self.assertEqual(normalize_log_level(" Warning "), logging.WARNING)

    def test_accepts_plain_int(self) -> None:
        self.assertEqual(normalize_log_level(logging.ERROR), logging.ERROR)
        self.assertEqual(normalize_log_level(5), 5)

    def test_rejects_unknown_name(self) -> None:
        with self.assertRaises(ValueError):
            normalize_log_level("LOUD")

    def test_rejects_bool(self) -> None:
        # bool is a subclass of int; must not silently pass through as 0/1.
        with self.assertRaises(ValueError):
            normalize_log_level(True)


class EnableLoggingTests(_ObservabilityIsolation):
    def test_attaches_exactly_one_handler_and_is_idempotent(self) -> None:
        enable_logging("INFO")
        enable_logging("DEBUG")

        self.assertEqual(len(self.package_logger.handlers), 1)
        self.assertEqual(self.package_logger.level, logging.DEBUG)

    def test_disables_propagation_to_avoid_duplicate_lines(self) -> None:
        enable_logging("INFO")

        self.assertFalse(self.package_logger.propagate)

    def test_root_logger_is_never_touched(self) -> None:
        root_handlers_before = list(logging.getLogger().handlers)

        enable_logging("DEBUG")

        self.assertEqual(logging.getLogger().handlers, root_handlers_before)


class ConfigDrivenLoggingTests(_ObservabilityIsolation):
    def setUp(self) -> None:
        super().setUp()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def test_default_construction_never_enables_logging(self) -> None:
        publisher = ReliablePublisher(
            PublisherConfig(host="h", queue_path=Path(self.directory.name) / "q.jsonl"),
            client_factory=client_factory_for(FakeClient()),
        )
        self.addCleanup(publisher.stop)

        self.assertEqual(self.package_logger.handlers, [])
        self.assertTrue(self.package_logger.propagate)

    def test_publisher_config_log_level_enables_the_package_logger(self) -> None:
        publisher = ReliablePublisher(
            PublisherConfig(
                host="h",
                queue_path=Path(self.directory.name) / "q.jsonl",
                log_level="INFO",
            ),
            client_factory=client_factory_for(FakeClient()),
        )
        self.addCleanup(publisher.stop)

        self.assertEqual(self.package_logger.level, logging.INFO)
        self.assertEqual(len(self.package_logger.handlers), 1)

    def test_publisher_config_debug_true_enables_debug_level(self) -> None:
        publisher = ReliablePublisher(
            PublisherConfig(
                host="h", queue_path=Path(self.directory.name) / "q.jsonl", debug=True
            ),
            client_factory=client_factory_for(FakeClient()),
        )
        self.addCleanup(publisher.stop)

        self.assertEqual(self.package_logger.level, logging.DEBUG)

    def test_bridge_config_debug_true_enables_debug_level(self) -> None:
        bridge = ReliableMqttBridge(
            BridgeConfig(source_host="s", destination_host="d", debug=True),
            source_client_factory=client_factory_for(FakeClient()),
            destination_client_factory=client_factory_for(FakeClient()),
        )
        self.addCleanup(bridge.stop)

        self.assertEqual(self.package_logger.level, logging.DEBUG)


class LifecycleLogContentTests(unittest.TestCase):
    """assertLogs manages its own handler/level independent of whatever
    enable_logging() has done process-wide, so no isolation fixture needed.
    """

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.queue_path = Path(self.directory.name) / "pending.jsonl"

    def _wire_publisher(self):
        client = FakeClient()
        publisher = ReliablePublisher(
            PublisherConfig(
                host="h",
                queue_path=self.queue_path,
                ack_timeout=0.05,
                publish_timeout=0.05,
                retry_interval=0.05,
            ),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(publisher.stop)

        def ack_hook(call) -> None:
            envelope = MessageEnvelope.from_bytes(call["payload"])
            client.emit_message(
                publisher.config.ack_topic, Ack(envelope.message_id).to_bytes()
            )

        client.publish_hook = ack_hook
        return publisher, client

    def test_info_level_tells_the_lifecycle_story(self) -> None:
        publisher, client = self._wire_publisher()

        with self.assertLogs("reliomq", level="INFO") as captured:
            publisher.start()
            client.emit_connect()
            client.emit_latest_suback((1,))
            message_id = publisher.publish("factory/x", {"v": 1})
            self.assertTrue(publisher.wait_for_delivery(message_id, timeout=2.0))
            publisher.stop()

        joined = "\n".join(captured.output)
        for expected in (
            "Publisher started",
            "Message accepted and durably queued",
            "MQTT connection established",
            "Application acknowledgement received",
            "Message delivered and removed from durable queue",
            "Publisher stopping",
            "Publisher stopped",
        ):
            self.assertIn(expected, joined, f"missing expected INFO line: {expected!r}")
        self.assertIn(message_id, joined)
        # DEBUG-only detail must not leak into an INFO-level capture.
        self.assertNotIn("Publish attempt", joined)

    def test_debug_level_adds_diagnostic_detail_not_shown_at_info(self) -> None:
        publisher, client = self._wire_publisher()

        with self.assertLogs("reliomq", level="DEBUG") as captured:
            publisher.start()
            client.emit_connect()
            client.emit_latest_suback((1,))
            message_id = publisher.publish("factory/x", {"v": 1})
            self.assertTrue(publisher.wait_for_delivery(message_id, timeout=2.0))
            publisher.stop()

        joined = "\n".join(captured.output)
        for expected in (
            "Delivery attempt starting",
            "Publish attempt",
            "Broker publish confirmed (PUBACK)",
            "Waiting for application acknowledgement",
            "Matching ACK received",
        ):
            self.assertIn(expected, joined, f"missing expected DEBUG line: {expected!r}")

    def test_retry_is_visible_at_info_with_reason_at_warning(self) -> None:
        client = FakeClient()
        publisher = ReliablePublisher(
            PublisherConfig(
                host="h",
                queue_path=self.queue_path,
                ack_timeout=0.02,
                publish_timeout=0.02,
                retry_interval=0.02,
            ),
            client_factory=client_factory_for(client),
        )
        self.addCleanup(publisher.stop)
        client.connected = True
        publisher._connected.set()
        publisher._ack_subscription_ready.set()
        publisher.publish("factory/x", {"v": 1}, message_id="retry-me")

        with self.assertLogs("reliomq", level="INFO") as captured:
            status = publisher._process_oldest_once()

        from reliomq.publisher import DeliveryStatus

        self.assertEqual(status, DeliveryStatus.RETRY)
        joined = "\n".join(captured.output)
        self.assertIn("Retry scheduled", joined)
        self.assertIn("retry-me", joined)
        self.assertIn("attempt=1", joined)
        warning_lines = [line for line in captured.output if "WARNING" in line]
        self.assertTrue(
            any("Delivery attempt failed" in line for line in warning_lines)
        )


if __name__ == "__main__":
    unittest.main()

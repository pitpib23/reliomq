"""Periodic publisher loop: the shape most edge/IoT integrations actually use.

Unlike basic.py's one-shot publish, this simulates a sensor that produces a
reading every few seconds for the life of the process. It shows the pattern
recommended for long-running services:

- start() once, publish() many times from the sensor loop;
- never block the sensor loop on delivery -- publish() only waits for the
  durable append, not for the network;
- check pending_count() to size a "delivery is behind" warning rather than
  polling wait_for_delivery() per reading, which would serialize readings
  behind network round trips;
- stop() on SIGINT/SIGTERM leaves any in-flight message durably queued for
  the next run instead of losing it.

Run against a local broker for a real trial:

    python examples/sensor_loop.py
"""

from __future__ import annotations

import logging
import random
import signal
import threading
import time

from reliomq import ReliabilityConfig, ReliablePublisher


logging.basicConfig(level=logging.INFO)

READING_INTERVAL_SECONDS = 5.0
PENDING_WARNING_THRESHOLD = 20


def read_sensor() -> dict:
    """Stand-in for real hardware I/O -- replace with an actual sensor read."""

    return {"temperature_c": round(20 + random.uniform(-2, 5), 2)}


def main() -> None:
    config = ReliabilityConfig(
        host="localhost",
        port=1883,
        queue_path="sensor_pending.jsonl",
        data_topic="reliable/ingress",
        ack_topic="reliable/acks",
        ack_timeout=3.0,
        retry_interval=10.0,
    )

    publisher = ReliablePublisher(config)
    publisher.start()

    shutdown_requested = threading.Event()

    def request_shutdown(_signal_number, _frame) -> None:
        shutdown_requested.set()

    signal.signal(signal.SIGINT, request_shutdown)
    try:
        signal.signal(signal.SIGTERM, request_shutdown)
    except (AttributeError, ValueError):
        pass  # SIGTERM is not available on every platform/thread.

    try:
        while not shutdown_requested.is_set():
            reading = read_sensor()
            event_id = publisher.publish(
                topic="factory/machine1/temperature", payload=reading
            )

            pending = publisher.pending_count()
            if pending >= PENDING_WARNING_THRESHOLD:
                logging.warning(
                    "Delivery is falling behind | pending=%s | latest=%s",
                    pending,
                    event_id,
                )

            shutdown_requested.wait(timeout=READING_INTERVAL_SECONDS)
    finally:
        # Any reading already durably queued survives this call and the
        # process exit; it is retried the next time this program starts.
        publisher.stop()


if __name__ == "__main__":
    main()

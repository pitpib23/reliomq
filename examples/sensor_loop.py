"""Periodic Sender loop: the shape most edge/IoT integrations actually use.

Unlike basic.py's one-shot publish, this simulates a sensor that produces a
reading every few seconds for the life of the process. It shows the pattern
recommended for long-running services:

- connect()/loop_start() once, publish() many times from the sensor loop
  (never recreate the Sender per reading);
- never block the sensor loop on delivery -- publish() only waits for the
  durable Outbox append, not for the network;
- check pending_count() to size a "delivery is behind" warning rather than
  polling wait_for_delivery() per reading, which would serialize readings
  behind network round trips;
- loop_stop()/disconnect() on SIGINT/SIGTERM leaves any in-flight message
  durably queued for the next run instead of losing it;
- `log_level="INFO"` on the config gives a running narration of connects,
  stored readings, and confirmed deliveries with zero `logging` setup.

Run against a local broker for a real trial:

    python examples/sensor_loop.py
"""

from __future__ import annotations

import logging
import random
import signal
import threading
import time

from reliomq import Sender, SenderConfig


READING_INTERVAL_SECONDS = 5.0
PENDING_WARNING_THRESHOLD = 20


def read_sensor() -> dict:
    """Stand-in for real hardware I/O -- replace with an actual sensor read."""

    return {"temperature_c": round(20 + random.uniform(-2, 5), 2)}


def main() -> None:
    config = SenderConfig(
        host="localhost",
        port=1883,
        outbox_path="sensor_pending.jsonl",
        relay_topic="reliable/ingress",
        delivery_ack_topic="reliable/acks",
        delivery_ack_timeout=3.0,
        retry_interval=10.0,
        log_level="INFO",
    )

    sender = Sender(config)
    sender.connect()
    sender.loop_start()  # harmless no-op here -- connect() already did this

    shutdown_requested = threading.Event()

    def request_shutdown(_signal_number, _frame) -> None:
        shutdown_requested.set()

    signal.signal(signal.SIGINT, request_shutdown)
    try:
        signal.signal(signal.SIGTERM, request_shutdown)
    except (AttributeError, ValueError):
        pass  # SIGTERM is not available on every platform/thread.

    # This app-level warning is separate from reliomq's own INFO logging
    # above; use the standard library the same way you would for any other
    # part of your application.
    app_logger = logging.getLogger(__name__)

    try:
        while not shutdown_requested.is_set():
            reading = read_sensor()
            message_id = sender.publish("factory/machine1/temperature", reading)

            pending = sender.pending_count()
            if pending >= PENDING_WARNING_THRESHOLD:
                app_logger.warning(
                    "Delivery is falling behind | pending=%s | latest=%s",
                    pending,
                    message_id,
                )

            shutdown_requested.wait(timeout=READING_INTERVAL_SECONDS)
    finally:
        # Any reading already stored in the Outbox survives this call and
        # the process exit; it is retried the next time this program starts.
        sender.loop_stop()
        sender.disconnect()


if __name__ == "__main__":
    main()

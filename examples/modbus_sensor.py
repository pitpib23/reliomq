"""Read-only Modbus TCP sensor bridged to MQTT through reliomq.

A realistic edge-device shape: poll a PLC/sensor over Modbus TCP on a fixed
interval, publish each reading through a `Sender`, and let reliomq worry
about the network -- broker outages, reconnects, and process restarts all
just mean the Outbox grows and drains; no reading is lost either way.

This example only ever calls `read_holding_registers()` -- it never writes
to the device. `pymodbus` is an OPTIONAL dependency reliomq itself does not
require; install it to run this specific example:

    pip install pymodbus

Targets the pymodbus 3.x API. Adjust register addresses/count, the decode
logic, and MODBUS_HOST/MODBUS_UNIT_ID for your actual device -- the numbers
below are placeholders.

Run against a local broker (and a real or simulated Modbus TCP device) for
a real trial:

    python examples/modbus_sensor.py
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import UTC, datetime

try:
    from pymodbus.client import ModbusTcpClient
except ImportError as error:  # pragma: no cover - documentation example
    raise SystemExit(
        "This example requires pymodbus, which reliomq does not depend on. "
        "Install it with: pip install pymodbus"
    ) from error

from reliomq import Sender, SenderConfig


MODBUS_HOST = "192.168.1.50"
MODBUS_PORT = 502
MODBUS_UNIT_ID = 1  # a.k.a. "slave" in older pymodbus versions
TEMPERATURE_REGISTER = 0
POLL_INTERVAL_SECONDS = 1.0
PENDING_WARNING_THRESHOLD = 50

logger = logging.getLogger(__name__)


def read_temperature(modbus: ModbusTcpClient) -> float:
    """Read one holding register and decode it. Raises on any Modbus error.

    Read-only by design: this example never calls a Modbus *write*
    function. Replace the register address and scaling with your device's
    actual map.
    """

    result = modbus.read_holding_registers(
        address=TEMPERATURE_REGISTER, count=1, slave=MODBUS_UNIT_ID
    )
    if result.isError():
        raise IOError(f"Modbus read failed: {result}")
    raw_value = result.registers[0]
    return raw_value / 10.0  # e.g. a device reporting tenths of a degree


def main() -> None:
    # Initialize both clients once, outside the polling loop.
    modbus = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)

    sender = Sender(
        SenderConfig(
            host="localhost",
            outbox_path="sensor-outbox.jsonl",
            relay_topic="reliomq/relay",
            delivery_ack_topic="reliomq/acks",
            log_level="INFO",
        )
    )

    shutdown_requested = threading.Event()

    def request_shutdown(_signal_number, _frame) -> None:
        shutdown_requested.set()

    signal.signal(signal.SIGINT, request_shutdown)
    try:
        signal.signal(signal.SIGTERM, request_shutdown)
    except (AttributeError, ValueError):
        pass  # SIGTERM is not available on every platform/thread.

    sender.connect()
    sender.loop_start()  # harmless no-op here -- connect() already did this

    try:
        if not modbus.connect():
            raise IOError(f"Could not connect to Modbus device at {MODBUS_HOST}:{MODBUS_PORT}")

        while not shutdown_requested.is_set():
            try:
                temperature = read_temperature(modbus)
            except Exception:
                # A transient Modbus read failure should not crash the
                # process or stop the MQTT side; log it and try again next
                # interval. Persistent failures show up clearly in the logs.
                logger.exception("Modbus read failed; will retry next interval")
                shutdown_requested.wait(timeout=POLL_INTERVAL_SECONDS)
                continue

            payload = {
                "device": "boiler-01",
                "temperature_c": temperature,
                "timestamp": datetime.now(UTC).isoformat(),
            }

            # publish() durably stores the reading and returns immediately --
            # it does NOT wait on the network. If the MQTT broker is offline
            # right now, this call still succeeds: the reading sits in the
            # Outbox and reliomq keeps retrying it in the background. A
            # restart of this whole process is equally safe -- the next run
            # opens the same outbox_path and resumes exactly where it left
            # off, so no reading collected while the broker was unreachable
            # is ever lost.
            message_id = sender.publish("factory/boiler-01/telemetry", payload)

            pending = sender.pending_count()
            if pending >= PENDING_WARNING_THRESHOLD:
                logger.warning(
                    "Delivery is falling behind | pending=%s | latest=%s",
                    pending,
                    message_id,
                )

            shutdown_requested.wait(timeout=POLL_INTERVAL_SECONDS)
    finally:
        modbus.close()
        sender.loop_stop()
        sender.disconnect()


if __name__ == "__main__":
    main()

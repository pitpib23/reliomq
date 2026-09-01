"""Optional real-broker end-to-end test.

Set ``RUN_MQTT_INTEGRATION=1`` and install ``mosquitto`` on PATH to run it.
The test starts two loopback-only broker processes on ephemeral ports.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt

from reliomq import (
    BridgeConfig,
    DeliveryEnvelope,
    PublisherConfig,
    ReliableMqttBridge,
    ReliablePublisher,
)


MOSQUITTO = shutil.which("mosquitto")
RUN_INTEGRATION = os.environ.get("RUN_MQTT_INTEGRATION") == "1"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Mosquitto did not listen on port {port}")


def start_broker(port: int) -> subprocess.Popen[bytes]:
    assert MOSQUITTO is not None
    creation_flags = 0
    if os.name == "nt":
        creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(
        [MOSQUITTO, "-p", str(port)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creation_flags,
    )
    try:
        wait_for_port(port)
    except Exception:
        process.terminate()
        process.wait(timeout=2)
        raise
    return process


@unittest.skipUnless(
    RUN_INTEGRATION and MOSQUITTO,
    "set RUN_MQTT_INTEGRATION=1 and install mosquitto to run",
)
class MosquittoIntegrationTests(unittest.TestCase):
    def test_publisher_bridge_ack_and_destination_delivery(self) -> None:
        source_port = free_port()
        destination_port = free_port()
        source_broker = start_broker(source_port)
        destination_broker = start_broker(destination_port)
        self.addCleanup(self._stop_process, destination_broker)
        self.addCleanup(self._stop_process, source_broker)

        received = threading.Event()
        subscription_ready = threading.Event()
        delivery_payloads: list[bytes] = []
        destination_topic = f"integration/output/{uuid.uuid4().hex}"
        envelope_topic = f"integration/input/{uuid.uuid4().hex}"
        ack_topic = f"integration/ack/{uuid.uuid4().hex}"

        consumer = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"integration-consumer-{uuid.uuid4().hex}",
        )
        consumer.on_connect = lambda client, _userdata, _flags, reason, _props: (
            client.subscribe(destination_topic, qos=1)
            if not getattr(reason, "is_failure", int(reason) != 0)
            else None
        )
        consumer.on_subscribe = (
            lambda _client, _userdata, _mid, _reasons, _props: subscription_ready.set()
        )

        def on_delivery(_client, _userdata, message) -> None:
            delivery_payloads.append(bytes(message.payload))
            received.set()

        consumer.on_message = on_delivery
        consumer.connect("127.0.0.1", destination_port, 30)
        consumer.loop_start()
        self.addCleanup(consumer.loop_stop)
        self.addCleanup(consumer.disconnect)
        self.assertTrue(subscription_ready.wait(timeout=5.0))

        bridge = ReliableMqttBridge(
            BridgeConfig(
                source_host="127.0.0.1",
                source_port=source_port,
                destination_host="127.0.0.1",
                destination_port=destination_port,
                envelope_topic=envelope_topic,
                ack_topic=ack_topic,
            )
        )
        bridge.start()
        self.addCleanup(bridge.stop)

        with tempfile.TemporaryDirectory() as directory:
            publisher = ReliablePublisher(
                PublisherConfig(
                    host="127.0.0.1",
                    port=source_port,
                    queue_path=Path(directory) / "pending.jsonl",
                    envelope_topic=envelope_topic,
                    ack_topic=ack_topic,
                    ack_timeout=5.0,
                    retry_interval=0.1,
                )
            )
            publisher.start()
            self.addCleanup(publisher.stop)

            message_id = publisher.publish(
                destination_topic,
                {"temperature": 24.5},
            )

            self.assertTrue(publisher.wait_for_delivery(message_id, timeout=10.0))
            self.assertTrue(received.wait(timeout=5.0))
            delivery = DeliveryEnvelope.from_bytes(delivery_payloads[0])
            self.assertEqual(delivery.message_id, message_id)
            self.assertEqual(delivery.payload, {"temperature": 24.5})

    @staticmethod
    def _stop_process(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()


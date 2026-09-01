"""Injecting TLS and username/password auth via a custom client factory.

reliomq never reads credentials or certificate paths from its own
config objects -- SenderConfig/RelayConfig only describe reliability
behavior (topics, timeouts, Outbox path). Connection-security concerns are
applied to the underlying Paho client through an injectable factory, using
the same construction hook the test suite uses to inject fakes.
"""

from __future__ import annotations

import ssl

import paho.mqtt.client as mqtt

from reliomq import Sender, SenderConfig


def make_tls_client(*, client_id: str, userdata=None) -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        userdata=userdata,
    )
    client.username_pw_set(username="factory-sensor-1", password="use-a-secret-manager")
    client.tls_set(
        ca_certs="/etc/reliomq/ca.pem",
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
    return client


config = SenderConfig(
    host="mqtt.example.net",
    port=8883,
    outbox_path="mqtt_pending.jsonl",
    relay_topic="reliable/ingress",
    delivery_ack_topic="reliable/acks",
)

sender = Sender(config, client_factory=make_tls_client)
sender.connect()
sender.publish("factory/machine1/data", {"temperature": 25.2})
sender.disconnect()

"""Use separate Sender instances when streams need different guarantees.

Run with a source broker on localhost and a Relay using the topics below.
"""

from reliomq import DurableMode, FastMode, GroupMode, Sender, SenderConfig


def config(*, client_id: str, outbox_path: str) -> SenderConfig:
    return SenderConfig(
        host="localhost",
        client_id=client_id,
        outbox_path=outbox_path,
        relay_topic="reliable/ingress",
        delivery_ack_topic="reliable/acks",
        log_level="INFO",
    )


def main() -> None:
    # One client = one mode. Each client also owns a distinct MQTT client ID
    # and Outbox path; never share an Outbox between live clients/processes.
    # Durable writes and fsyncs now; Group writes now and fsyncs later; Fast
    # starts in RAM and writes to disk only when a spill trigger requires it.
    critical = Sender(
        config(client_id="multi-critical", outbox_path="outbox-multi-critical"),
        mode=DurableMode(),
    )
    telemetry = Sender(
        config(client_id="multi-telemetry", outbox_path="outbox-multi-telemetry"),
        mode=GroupMode(),
    )
    heartbeat = Sender(
        config(client_id="multi-heartbeat", outbox_path="outbox-multi-heartbeat"),
        mode=FastMode(),
    )

    with critical, telemetry, heartbeat:
        critical.publish("factory/alarm", {"active": True})
        telemetry.publish("factory/temperature", {"celsius": 25.2})
        heartbeat_id = heartbeat.publish("factory/heartbeat", {"online": True})
        # Allow healthy FastMode delivery before context exit triggers spill.
        heartbeat.wait_for_delivery(heartbeat_id, timeout=2.0)
        # Context exit syncs/spills any work still pending in each client.


if __name__ == "__main__":
    main()

"""DurableMode: strongest acceptance guarantee for critical events.

Run with a source broker on localhost and a Relay using the topics below.
"""

from reliomq import DurableMode, Sender, SenderConfig


def main() -> None:
    config = SenderConfig(
        host="localhost",
        client_id="factory-critical",
        outbox_path="outbox-critical",
        relay_topic="reliable/ingress",
        delivery_ack_topic="reliable/acks",
        log_level="INFO",
    )

    mode = DurableMode()
    # Omitting mode entirely, Sender(config), gives the same DurableMode.
    # DurableMode appends and fsyncs every message before publish() returns.
    # It also checkpoints every DeliveryAck. This gives the strongest
    # power-loss protection and lowest practical replay window, at the cost
    # of the highest SD-card sync activity.
    # Delivery remains at-least-once: a crash before an ACK checkpoint can
    # replay the same message ID, so consumers should deduplicate IDs.
    with Sender(config, mode=mode) as sender:
        message_id = sender.publish(
            "factory/ng-count",
            {"line": 4, "increment": 1},
        )
        print(f"critical event accepted as {message_id}")
        if sender.wait_for_delivery(message_id, timeout=10.0):
            print("destination broker delivery confirmed")
        else:
            print("still pending; this ID will be retried on the next start")


if __name__ == "__main__":
    main()

"""GroupMode = write now + fsync later, using grouped synchronization.

Run with a source broker on localhost and a Relay using the topics below.
"""

from reliomq import GroupMode, Sender, SenderConfig


def main() -> None:
    config = SenderConfig(
        host="localhost",
        client_id="factory-telemetry",
        outbox_path="outbox-telemetry",
        relay_topic="reliable/ingress",
        delivery_ack_topic="reliable/acks",
        log_level="INFO",
    )
    mode = GroupMode()  # Simplest choice: use the validated defaults.
    # A custom configuration; adjust these thresholds for your stream's
    # acceptable loss and duplicate-replay windows.
    mode = GroupMode(
        sync_messages=20,
        sync_interval=None,  # Disable the time trigger; count/bytes remain.
        sync_bytes=64 * 1024,
        ack_checkpoint_messages=50,
        ack_checkpoint_interval=1.0,
    )

    # Every message is appended to a segment immediately, but is not fsynced
    # individually. Here a group fsync is triggered by message count, bytes,
    # segment rotation, or clean shutdown. Only the range covered by a
    # completed fsync is guaranteed after power loss. ACK cursor progress is
    # batched separately: ACK count, time, a consumed closed segment, or stop
    # triggers a checkpoint. Uncheckpointed ACKs can replay after a crash.
    # Grouping reduces SD sync activity at sustained rates; at low rates the
    # Any individual GroupMode trigger can be disabled with None, but at least
    # one data-sync trigger and one ACK-checkpoint trigger must remain enabled.
    with Sender(config, mode=mode) as sender:
        message_id = sender.publish(
            "factory/line-4/temperature",
            {"celsius": 25.2},
        )
        print(f"telemetry appended as {message_id}")
        if sender.wait_for_delivery(message_id, timeout=10.0):
            print("destination broker delivery confirmed")
        # Exiting completes both deferred data sync and ACK checkpoint work.


if __name__ == "__main__":
    main()

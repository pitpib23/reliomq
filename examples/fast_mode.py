"""FastMode = RAM now + disk only if needed, for replaceable live values.

Run with a source broker on localhost and a Relay using the topics below.
"""

from reliomq import FastMode, FastQueueFullError, Sender, SenderConfig


def main() -> None:
    config = SenderConfig(
        host="localhost",
        client_id="factory-heartbeat",
        outbox_path="outbox-heartbeat",
        relay_topic="reliable/ingress",
        delivery_ack_topic="reliable/acks",
        log_level="INFO",
    )
    mode = FastMode()  # Simplest choice: use the validated defaults.
    # A custom configuration showing how one trigger can be disabled.
    mode = FastMode(
        ram_max_messages=10_000,
        ram_max_bytes=32 * 1024 * 1024,
        high_watermark=0.75,
        max_ram_age=5.0,
        disconnect_grace=None,  # Do not spill merely because MQTT is offline.
        spill_batch_messages=1_000,
        spill_batch_bytes=4 * 1024 * 1024,
    )

    # Healthy messages go RAM -> MQTT -> DeliveryAck without a message write.
    # Count/byte high water, maximum age, a delivery retry timeout, or clean
    # shutdown spills oldest messages in bounded batches. This configuration
    # disables disconnect-only spilling; other spill rules remain active.
    # Byte limits count serialized envelopes, including the topic and ID;
    # runtime metadata and temporary buffers are additional memory.
    with Sender(config, mode=mode) as sender:
        try:
            message_id = sender.publish(
                "factory/line-4/heartbeat",
                {"online": True},
            )
            print(f"heartbeat accepted as {message_id}")
            # Give the healthy RAM-only path time to receive DeliveryAck.
            # Leaving the context immediately would trigger shutdown spill.
            if sender.wait_for_delivery(message_id, timeout=2.0):
                print("destination broker delivery confirmed")
            else:
                print("still pending; context exit will durably spill it")
        except FastQueueFullError:
            print("FastMode RAM is full and spill could not make room; retry later")


if __name__ == "__main__":
    main()

"""Diagnosing delivery problems with DEBUG-level logging.

Runs with no broker required -- it deliberately points at a host nothing is
listening on, so you can watch reliomq's own diagnosis of "this isn't
working" without setting anything up first. `debug=True` is shorthand for
`log_level="DEBUG"`; either goes on the config object, and both attach a
lightweight stderr handler to reliomq's own logger with zero `logging`
module setup on your part (see the README's "Logging" section for what each
level shows and how this interacts with your own logging configuration).

Run it and read the stderr output top to bottom:

    python examples/debug_logging.py

You should see: the durable queue accepting the message immediately
(`publish()` never waits on the network), a connection attempt that never
completes, and -- once you point this at a real broker instead -- you would
additionally see each publish attempt, the broker PUBACK, the wait for the
application ACK, and either delivery confirmation or a retry with its
reason. Change HOST/PORT below to a real broker to see that full picture.
"""

from __future__ import annotations

import time

from reliomq import PublisherConfig, ReliablePublisher


# Intentionally nothing is listening here. Swap in a real broker to see the
# rest of the lifecycle (PUBACK, application ACK, delivery) in DEBUG detail.
HOST = "localhost"
PORT = 18830

config = PublisherConfig(
    host=HOST,
    port=PORT,
    queue_path="debug_pending.jsonl",
    retry_interval=1.0,
    debug=True,
)

with ReliablePublisher(config) as publisher:
    message_id = publisher.publish(
        topic="factory/machine1/data",
        payload={"temperature": 25.2},
    )
    print(f"message {message_id} durably queued; watching for a few seconds...")
    time.sleep(4.0)
    print(f"pending={publisher.pending_count()} -- see the DEBUG lines above for why")

# reliomq

**Durable, end-to-end confirmed MQTT delivery for Python.**

`reliomq` sits on top of [`paho-mqtt`](https://pypi.org/project/paho-mqtt/)
and adds the part QoS 1 doesn't give you: a message survives a crash, an
outage, or a broker restart, and is retried automatically — in order, under
its original ID — until an application confirms it actually arrived.

```python
from reliomq import ReliabilityConfig, ReliablePublisher

publisher = ReliablePublisher(ReliabilityConfig(
    host="localhost",
    queue_path="pending.jsonl",
    data_topic="reliable/ingress",
    ack_topic="reliable/acks",
))
publisher.start()
publisher.publish(topic="factory/machine1/data", payload={"temperature": 25.2})
publisher.stop()
```

That call durably queues the message before it ever touches the network.
Everything else — reconnects, retries, ordering, and knowing when it's
actually safe to forget the message — is handled for you.

Requires **Python 3.11+** and **Paho MQTT 2.x**.

## Why not just `qos=1`?

QoS 1 only proves the broker your process is directly connected to accepted
one publish. It proves nothing about:

- whether your process crashes or loses power before that publish happens;
- whether the broker forwards it any further (a bridge, another hop);
- whether anything ever confirms, at the application level, that the message
  did its job.

`reliomq` closes that gap with a durable outbox plus an **application-level
ACK**, so "the network layer said OK" is never mistaken for "the message is
handled."

## Features

- **Durable-before-network writes** — every `publish()` is `fsync`'d to disk
  before the first network attempt, so a crash immediately after `publish()`
  returns still can't lose the message.
- **QoS 1 MQTT delivery**, always — reliability isn't opt-in per call.
- **Application-level end-to-end ACK**, correlated by a stable `event_id`,
  in addition to the MQTT PUBACK.
- **Automatic, unique event IDs** — generated for you, or you can supply
  your own; never regenerated on retry.
- **Strict FIFO recovery** — the oldest pending message is always retried
  first; a new live message can never overtake it.
- **Automatic restart recovery** — a fresh process just opens the same
  queue file and continues exactly where the last one left off.
- **Automatic retry** on broker outage, network failure, publish errors,
  publish-confirmation timeout, and ACK timeout — nothing is deleted on
  any of these.
- **Optional bridge component** (`ReliableMqttBridge`) that relays between
  two brokers and only ACKs the source *after* the destination publish is
  confirmed — never before.
- **Fail-closed forwarding** — any bridge failure (offline, malformed
  message, full queue, timeout, shutdown) sends no ACK, so the source keeps
  retrying instead of silently dropping the message.
- **Safe ACK handling** — stale, late, duplicate, wrong-ID, and malformed
  ACKs are all detected and ignored rather than treated as success.
- **Automatic reconnect/backoff** via Paho, with explicit connection-state
  tracking so the library never publishes while it knows it's disconnected.
- **Clean shutdown** — stopping interrupts an in-progress ACK wait without
  ever deleting the durable record for the message being waited on.
- **Corruption-safe storage** — a damaged queue line is logged and skipped,
  never silently deleted; a crash mid-write can't destroy the next record.
- **Pluggable client construction** — inject your own `client_factory` for
  TLS, auth, or any other Paho client customization.
- **Strict JSON envelope validation** — rejects `NaN`/`Infinity`, bytes,
  tuples, non-string keys, and other values with no exact JSON form.
- **Thread-safe by design** — internal locks are never held during a
  network wait, so an ACK can never be missed to a race.

## Reliability guarantee

`reliomq` provides **at-least-once delivery to the destination broker**
when the publisher and bridge are used together. It favors *never silently
losing a message* over *never duplicating one*.

```text
application
    |  fsync durable outbox
    v
ReliablePublisher -- QoS 1 --> source broker
                                   |
                                   v
                            ReliableMqttBridge
                                   |
                                   | QoS 1 + PUBACK
                                   v
                           destination broker
                                   |
                                   | correlated application ACK
                                   v
                         publisher removes outbox head
```

A record is deleted from the durable outbox **only** after a valid ACK
carrying its exact `event_id` arrives back on the configured ACK topic.
Broker acceptance alone is not proof that a final subscriber processed the
message — if you need that stronger boundary, use a persistent MQTT
subscription or extend the protocol with a consumer ACK of your own.

Duplicates remain possible: the destination publish can succeed and the
source ACK can then be lost, in which case the publisher correctly retries
the same stable `event_id`. **Consumers should store processed event IDs
and make handling idempotent** — see `examples/consumer_dedup.py`.

## Install

```bash
# From GitHub, pinned to a release (recommended)
pip install "git+https://github.com/pitpib23/reliomq.git@v0.1.0"

# Or track the latest commit on main
pip install "git+https://github.com/pitpib23/reliomq.git"

# Or from a local clone
pip install -e /path/to/reliomq
```

The only runtime dependency is `paho-mqtt>=2,<3`. There is no GPIO or other
hardware dependency of any kind.

## Publisher integration

```python
from reliomq import ReliabilityConfig, ReliablePublisher

config = ReliabilityConfig(
    host="localhost",
    port=1883,
    queue_path="mqtt_pending.jsonl",
    data_topic="reliable/ingress",
    ack_topic="reliable/acks",
    ack_timeout=3.0,
    publish_timeout=2.0,
    retry_interval=10.0,
)

publisher = ReliablePublisher(config)
publisher.start()

event_id = publisher.publish(
    topic="factory/machine1/data",
    payload={"temperature": 25.2, "pressure": 4.1},
)

# Optional: block application code until this process observes delivery.
delivered = publisher.wait_for_delivery(event_id, timeout=10.0)

publisher.stop()
```

`ReliablePublisher` also works as a context manager: `with
ReliablePublisher(config) as publisher: ...` calls `start()`/`stop()` for
you.

`payload` may be any strict JSON value: an object with string keys, an
array, a string, a finite number, a boolean, or `null`. `NaN`, infinity,
bytes, tuples, custom objects, and mappings with non-string keys are
rejected. An ID is generated automatically unless `event_id=` is supplied.
Explicit IDs must be globally unique and must not be reused for different
content.

`publish()` means "durably accepted," not "already delivered." If the
outbox cannot be written safely, it raises a `StoreError` instead of
pretending to have accepted the message.

### Replacing a normal Paho publish

Before:

```python
client.publish("factory/machine1/data", payload=json_text, qos=1)
```

After:

```python
event_id = publisher.publish(
    topic="factory/machine1/data",
    payload={"temperature": 25.2},
)
```

The caller no longer owns reconnect loops, ACK races, durable retry, or FIFO
recovery.

## Bridge integration

Use `ReliableMqttBridge` only if you need to relay messages from one broker
to another. Run it as its own service or process; its source `data_topic`
and `ack_topic` must match the publisher's configuration.

```python
from reliomq import BridgeConfig, ReliableMqttBridge

bridge = ReliableMqttBridge(
    BridgeConfig(
        source_host="localhost",
        source_port=1883,
        destination_host="mqtt.example.net",
        destination_port=1883,
        data_topic="reliable/ingress",
        ack_topic="reliable/acks",
        destination_publish_timeout=2.0,
        source_ack_publish_timeout=0.5,
    )
)

bridge.start()
...
bridge.stop()
```

The bridge forwards to the destination topic stored in each message. The
destination payload retains the deduplication key:

```json
{
  "version": 1,
  "event_id": "47913ac65ac84213a9361b393b845708",
  "payload": {"temperature": 25.2}
}
```

The bridge's handoff queue is intentionally memory-only. A malformed
message, full queue, outage, publish error, timeout, or shutdown produces no
success ACK; the publisher still owns the durable record and retries it.
Deploy only one ordinary bridge subscriber per route unless duplicate
forwarding is intended.

## Wire protocol

Publisher to bridge, on `data_topic` with QoS 1 and `retain=False`:

```json
{
  "version": 1,
  "event_id": "47913ac65ac84213a9361b393b845708",
  "topic": "factory/machine1/data",
  "payload": {"temperature": 25.2}
}
```

Bridge to publisher, on `ack_topic` with QoS 1 and `retain=False`:

```json
{"version": 1, "event_id": "47913ac65ac84213a9361b393b845708"}
```

Protocol objects are strict and versioned. Unknown/missing fields, malformed
UTF-8/JSON, invalid IDs, and ACKs for any ID other than the one currently in
flight are ignored. Correlation uses the ID alone rather than payload
equality, so it works with any payload shape.

## Persistence and FIFO recovery

The outbox is a lightweight JSONL file suitable for edge devices:

- one complete message per line;
- append, flush, and file `fsync` before `publish()` returns;
- one in-process lock around reads and writes;
- same-directory temporary file, `fsync`, and atomic replace when removing
  the confirmed head;
- best-effort parent-directory `fsync` on platforms that support it;
- stable event ID and destination topic across restart and every retry.

Corrupt physical lines are logged, preserved byte-for-byte, and skipped when
finding the FIFO order among valid messages — never silently deleted. I/O
errors are raised instead of being mistaken for an empty queue. If a crash
leaves a torn final line, the next append begins on a new line so it does
not destroy the next valid record.

One process must own a queue path. The store is thread-safe but does not
attempt cross-process file locking. JSONL removal rewrites the file, so a
database-backed store may be more appropriate for extremely large queues or
sustained high write rates.

## Connection, retry, and shutdown behavior

Both components use Paho's asynchronous network loop and reconnect backoff.
The publisher does not send until the broker connection and ACK subscription
are ready. A disconnect interrupts the current ACK wait without removing its
record. Reconnect wakes recovery immediately; other failures retry after
`retry_interval`.

`ReliablePublisher.stop()` interrupts an ACK wait and joins the worker.
Because the in-flight message was already durable, it remains for the next
process. `ReliableMqttBridge.stop()` stops accepting new input, lets the
bounded in-flight publish/ACK sequence finish, and leaves queued items
unacknowledged so their publishers recover them. Start/stop are idempotent;
a stopped instance is not restartable, so create a new instance to restart a
service.

## Configuration reference

QoS is fixed at 1 on both configs. Topics are validated as publish topics
and cannot contain MQTT wildcards. Authentication/TLS is applied by
supplying a configured `client_factory` when constructing a component (see
`examples/tls_auth_client.py`) — the config objects intentionally carry no
credentials.

`ReliabilityConfig`:

| Field | Default | Meaning |
|---|---|---|
| `host` | required | Broker hostname |
| `queue_path` | required | Durable outbox file path |
| `port` | `1883` | Broker port |
| `client_id` | auto-generated | MQTT client ID |
| `data_topic` | `reliomq/messages` | Topic the publisher sends on |
| `ack_topic` | `reliomq/acks` | Topic the publisher listens on for ACKs |
| `qos` | `1` | Fixed at 1 |
| `ack_timeout` | `3.0`s | How long to wait for the application ACK |
| `publish_timeout` | `2.0`s | How long to wait for MQTT publish confirmation |
| `retry_interval` | `10.0`s | Delay between retries after a failure |
| `keepalive` | `60`s | MQTT keepalive |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |

`BridgeConfig`:

| Field | Default | Meaning |
|---|---|---|
| `source_host` / `destination_host` | required | The two brokers being bridged |
| `source_port` / `destination_port` | `1883` | Ports for each broker |
| `source_client_id` / `destination_client_id` | auto-generated | MQTT client IDs for each side |
| `data_topic` / `ack_topic` | `reliomq/messages` / `reliomq/acks` | Must match the publisher |
| `qos` | `1` | Fixed at 1 |
| `keepalive` | `60`s | MQTT keepalive |
| `destination_publish_timeout` | `2.0`s | Confirmation wait on the destination publish |
| `source_ack_publish_timeout` | `0.5`s | Confirmation wait on the source ACK publish |
| `retry_interval` | `10.0`s | Subscription retry delay |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |
| `max_queue_size` | `1000` | Bound on the bridge's in-memory handoff queue |

## Logging

`reliomq` uses the standard library `logging` module and configures
nothing on its own — no handlers, no forced level, no `basicConfig()` call.
Nothing prints until your application configures logging.

| Logger | Used by |
|---|---|
| `reliomq.publisher` | `ReliablePublisher`, and the `DurableMessageStore` it creates internally |
| `reliomq.bridge` | `ReliableMqttBridge` (override with `bridge_logger=`) |
| `reliomq.store` | a `DurableMessageStore` you construct directly without passing `logger=` |
| `reliomq.mqtt` | the `confirmed_publish()` helper |

Turn logs on with one line, since propagation is never disabled:

```python
import logging
logging.basicConfig(level=logging.INFO)               # everything, INFO and up
logging.getLogger("reliomq").setLevel(logging.DEBUG)   # or scope it to just this library
```

**Levels, by meaning:**

- **DEBUG** — routine traffic: message durably queued, ACK subscription
  ready, delivery confirmed.
- **INFO** — lifecycle: publisher/bridge started/stopped, MQTT connected.
- **WARNING** — recoverable trouble that's expected during normal outage
  handling: disconnects, rejected subscriptions, late/malformed/wrong-ID
  ACKs ignored, forward failures, a full bridge queue. Nothing is lost when
  you see these.
- **ERROR** (some via `logger.exception()`, with a traceback) — things that
  should not happen: the worker failing to stop promptly on shutdown, an ACK
  matching a message that turned out not to be the durable oldest, or an
  unexpected exception in the delivery/forward loop.

## Examples

All scripts live in `examples/` and are runnable directly (`python
examples/<name>.py`) against a real broker unless noted otherwise:

- `basic.py` — minimal one-shot publish and `wait_for_delivery`.
- `sensor_loop.py` — a long-running periodic publisher with graceful
  SIGINT/SIGTERM shutdown and a pending-backlog warning; the shape most
  edge/IoT integrations actually use.
- `bridge.py` — minimal standalone forwarder service.
- `consumer_dedup.py` — a plain Paho subscriber (not part of this package)
  showing the recommended `event_id` deduplication pattern for a final
  consumer of bridged messages.
- `local_end_to_end.py` — publisher + bridge + consumer wired together
  against one local Mosquitto instance, so you can watch real PUBACKs,
  reconnects, and the on-disk queue file; kill and restart the broker
  mid-run to see `pending_count()` rise and drain.
- `tls_auth_client.py` — injecting TLS and username/password auth through a
  custom `client_factory` without adding security config to the library.

## Tests

Run the deterministic suite from this directory:

```bash
python -m unittest discover -s tests -v
```

`test_protocol.py`, `test_store.py`, `test_ack.py`, `test_mqtt.py`, and
`test_config.py` each test one module in isolation: envelope/ACK wire
encoding, durable FIFO storage, the single-waiter ACK correlator, the small
Paho helper functions, and configuration validation, respectively.
`test_publisher.py` and `test_bridge.py` drive each component through a fake
Paho client to exercise success, broker outage, return-code failure,
publish-confirmation timeout, ACK timeout, restart, FIFO recovery,
wrong/late/malformed/duplicate ACKs, reconnect, bridge failure/success, and
shutdown state transitions. `test_pipeline.py` goes a level higher: it wires
a real `ReliablePublisher` to a real `ReliableMqttBridge` through two linked
fake clients that relay `publish()` calls the way a broker would, so the two
components run on their own real background threads and exchange genuine
envelope/ACK traffic — catching integration regressions that per-component
unit tests with directly injected ACKs cannot see. An optional Mosquitto
integration test is skipped when the broker executable is unavailable.

## License

MIT — see [LICENSE](LICENSE).

# reliomq

**Durable, end-to-end confirmed MQTT delivery for Python.**

`reliomq` sits on top of [`paho-mqtt`](https://pypi.org/project/paho-mqtt/)
and adds the part QoS 1 doesn't give you: a message survives a crash, an
outage, or a broker restart, and is retried automatically — in order, under
its original ID — until an application confirms it actually arrived.

```python
from reliomq import PublisherConfig, ReliablePublisher

publisher = ReliablePublisher(PublisherConfig(
    host="localhost",
    queue_path="pending.jsonl",
    debug=True,  # see what reliomq is doing, with zero logging setup
))
publisher.start()
publisher.publish(topic="factory/machine1/data", payload={"temperature": 25.2})
publisher.stop()
```

That call durably queues the message before it ever touches the network.
Everything else — reconnects, retries, ordering, and knowing when it's
actually safe to forget the message — is handled for you.

Requires **Python 3.11+** and **Paho MQTT 2.x**.

> **Upgrading from 0.1.x?** Nothing breaks. `ReliabilityConfig`, `event_id=`
> keywords, and `data_topic=` still work — they now emit a
> `DeprecationWarning` pointing at their v0.2 replacement. See
> [Migrating from 0.1.x](#migrating-from-01x) below and
> [CHANGELOG.md](CHANGELOG.md) for the full picture.

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

## How reliomq works

Two components, each with one job:

```text
your application
      |
      |  publish(topic, payload)  -- returns immediately, message_id assigned
      v
ReliablePublisher              <-- durable outbox (JSONL file, fsync'd)
      |
      |  QoS 1 publish, on the *envelope topic*
      v
source MQTT broker
      |
      v
ReliableMqttBridge              <-- subscribes to the envelope topic
      |
      |  QoS 1 publish, on the message's real *application topic*
      v
destination MQTT broker  --->  your consumer(s)
      |
      |  once that publish is broker-confirmed...
      v
ReliableMqttBridge publishes an Ack back on the *ack topic*
      |
      v
ReliablePublisher matches the Ack to message_id, THEN removes it
from the durable outbox
```

The publisher never deletes a message on network confirmation alone. It
deletes a message only after a matching `Ack` comes back — which the bridge
only sends once *its own* publish to the real destination topic was itself
QoS 1 confirmed. That is the whole design in one sentence: **two hops, two
confirmations, one durable record that only goes away after both.**

### Two different kinds of "confirmed" — don't conflate them

| | What it proves | Who provides it |
|---|---|---|
| **MQTT PUBACK** (QoS 1) | The broker *this process is connected to* accepted one publish. | Paho / the broker, automatically. |
| **Application ACK** (`Ack` envelope) | A `ReliableMqttBridge` (or your own code speaking the same protocol) actually forwarded the message to its real destination topic, broker-confirmed. | reliomq's own wire protocol, on the `ack_topic`. |

A message is only removed from the durable outbox after **both**. This is
why the message stays queued (and keeps retrying) through: a broker outage,
a bridge that's down, a bridge that forwarded but whose own ACK got lost, or
your process crashing and restarting mid-delivery — in every one of those
cases, "network said OK" was never good enough on its own, so nothing was
deleted.

### What happens on restart

A fresh process just opens the same `queue_path` file and continues exactly
where the last one left off — same pending `message_id`s, same FIFO order,
no message ever "skipped" in favor of a newer one. See
[Persistence and FIFO recovery](#persistence-and-fifo-recovery) for the
on-disk details.

## Architecture overview

| Component | Responsibility | You touch it when... |
|---|---|---|
| `PublisherConfig` | Validated settings for `ReliablePublisher`: broker, topics, timeouts, queue path, logging. | Constructing a publisher. |
| `BridgeConfig` | Same, for `ReliableMqttBridge`: source + destination brokers. | Constructing a bridge. |
| `ReliablePublisher` | **Public API.** Durably queues messages and retries them until an application ACK confirms delivery. | This is what your application calls: `publish()`, `wait_for_delivery()`, `pending_count()`. |
| `ReliableMqttBridge` | **Public API.** Relays from a source broker to a destination broker and only ACKs the source after the destination publish is confirmed. | Run as its own process/service between two brokers. Not needed if you only care about durable *delivery to a broker* rather than end-to-end confirmed forwarding. |
| `DurableMessageStore` | Crash-safe, on-disk FIFO queue (JSONL). Used internally by `ReliablePublisher`; safe to use directly for inspection/maintenance scripts. | Rarely — mostly internal. |
| `MessageEnvelope` / `DeliveryEnvelope` / `Ack` | The three wire-protocol shapes, all correlated by `message_id`. | Only if you're implementing your own consumer or a compatible bridge from scratch. |
| `enable_logging()` | Attaches reliomq's zero-setup logging handler. | Called for you by `log_level=`/`debug=` on either config; call it directly if you want the same thing without a config object. |

## Features

- **Durable-before-network writes** — every `publish()` is `fsync`'d to disk
  before the first network attempt, so a crash immediately after `publish()`
  returns still can't lose the message.
- **QoS 1 MQTT delivery**, always — reliability isn't opt-in per call.
- **Application-level end-to-end ACK**, correlated by a stable `message_id`,
  in addition to the MQTT PUBACK.
- **Automatic, unique message IDs** — generated for you, or you can supply
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
- **Zero-setup runtime visibility** — `debug=True` or `log_level=` on either
  config gives you a running narration of connects, queued messages, ACKs,
  and retries, without touching Python's `logging` module yourself.

## Reliability guarantee

`reliomq` provides **at-least-once delivery to the destination broker**
when the publisher and bridge are used together. It favors *never silently
losing a message* over *never duplicating one*.

A record is deleted from the durable outbox **only** after a valid ACK
carrying its exact `message_id` arrives back on the configured ACK topic.
Broker acceptance alone is not proof that a final subscriber processed the
message — if you need that stronger boundary, use a persistent MQTT
subscription or extend the protocol with a consumer ACK of your own.

Duplicates remain possible: the destination publish can succeed and the
source ACK can then be lost, in which case the publisher correctly retries
the same stable `message_id`. **Consumers should store processed message
IDs and make handling idempotent** — see `examples/consumer_dedup.py`.

## Install

```bash
# From GitHub, pinned to a release (recommended)
pip install "git+https://github.com/pitpib23/reliomq.git@v0.2.0"

# Or track the latest commit on main
pip install "git+https://github.com/pitpib23/reliomq.git"

# Or from a local clone
pip install -e /path/to/reliomq
```

The only runtime dependency is `paho-mqtt>=2,<3`. There is no GPIO or other
hardware dependency of any kind.

## Getting started

The canonical shape: build a config, start the publisher, publish, wait for
delivery if you care, then stop. `log_level="INFO"` is the one line that
gets you visibility with no `logging` setup:

```python
from reliomq import PublisherConfig, ReliablePublisher

config = PublisherConfig(
    host="localhost",
    port=1883,
    queue_path="mqtt_pending.jsonl",
    envelope_topic="reliable/ingress",
    ack_topic="reliable/acks",
    ack_timeout=3.0,
    retry_interval=10.0,
    log_level="INFO",
)

with ReliablePublisher(config) as publisher:   # __enter__ calls start()
    message_id = publisher.publish(
        topic="factory/machine1/data",
        payload={"temperature": 25.2, "pressure": 4.1},
    )

    # Optional: block application code until this process observes delivery.
    delivered = publisher.wait_for_delivery(message_id, timeout=10.0)
# __exit__ calls stop(); any still-pending message survives for next time.
```

`payload` may be any strict JSON value: an object with string keys, an
array, a string, a finite number, a boolean, or `null`. `NaN`, infinity,
bytes, tuples, custom objects, and mappings with non-string keys are
rejected. A `message_id` is generated automatically unless `message_id=` is
supplied. Explicit IDs must be globally unique and must not be reused for
different content.

`publish()` means "durably accepted," not "already delivered." If the
outbox cannot be written safely, it raises a `StoreError` instead of
pretending to have accepted the message.

### Debugging a delivery problem

Swap `log_level="INFO"` for `debug=True` (equivalent to `log_level="DEBUG"`)
to see every internal step: publish attempts, PUBACK confirmation, waiting
for the application ACK, why a retry happened, and so on:

```python
config = PublisherConfig(host="localhost", queue_path="pending.jsonl", debug=True)
```

Run `examples/debug_logging.py` for a runnable version of this — it points
at a broker on purpose that isn't there, so you can see the DEBUG output
with zero setup.

### Replacing a normal Paho publish

Before:

```python
client.publish("factory/machine1/data", payload=json_text, qos=1)
```

After:

```python
message_id = publisher.publish(
    topic="factory/machine1/data",
    payload={"temperature": 25.2},
)
```

The caller no longer owns reconnect loops, ACK races, durable retry, or FIFO
recovery.

## Bridge integration

Use `ReliableMqttBridge` only if you need to relay messages from one broker
to another with end-to-end confirmation. Run it as its own service or
process; its source `envelope_topic` and `ack_topic` must match the
publisher's configuration.

```python
from reliomq import BridgeConfig, ReliableMqttBridge

bridge = ReliableMqttBridge(
    BridgeConfig(
        source_host="localhost",
        source_port=1883,
        destination_host="mqtt.example.net",
        destination_port=1883,
        envelope_topic="reliable/ingress",
        ack_topic="reliable/acks",
        destination_publish_timeout=2.0,
        source_ack_publish_timeout=0.5,
        log_level="INFO",
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

Publisher to bridge, on `envelope_topic` with QoS 1 and `retain=False`:

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

> **Note on naming:** the Python API calls this identifier `message_id`
> (see [Migrating from 0.1.x](#migrating-from-01x)) — but the JSON field on
> the wire is still spelled `event_id`, unchanged since 0.1.0. That's
> deliberate: it means a 0.1.x publisher and a 0.2.x bridge (or vice versa)
> stay fully interoperable through a rolling upgrade. Only the Python-facing
> name changed.

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
- stable message ID and destination topic across restart and every retry.

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
and cannot contain MQTT wildcards. `envelope_topic`/`ack_topic` are
reliomq's own transport topics — not the application topic you pass to
`publish()`. Authentication/TLS is applied by supplying a configured
`client_factory` when constructing a component (see
`examples/tls_auth_client.py`) — the config objects intentionally carry no
credentials.

`PublisherConfig`:

| Field | Default | Meaning |
|---|---|---|
| `host` | required | Broker hostname |
| `queue_path` | required | Durable outbox file path |
| `port` | `1883` | Broker port |
| `client_id` | auto-generated | MQTT client ID |
| `envelope_topic` | `reliomq/messages` | Topic the publisher sends its envelope on (not the application topic) |
| `ack_topic` | `reliomq/acks` | Topic the publisher listens on for ACKs |
| `qos` | `1` | Fixed at 1 |
| `ack_timeout` | `3.0`s | How long to wait for the application ACK |
| `publish_timeout` | `2.0`s | How long to wait for MQTT publish confirmation |
| `retry_interval` | `10.0`s | Delay between retries after a failure |
| `keepalive` | `60`s | MQTT keepalive |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |
| `log_level` | `None` | `"DEBUG"`/`"INFO"`/... or a `logging` level int; `None` leaves logging exactly as-is (see [Logging](#logging)) |
| `debug` | `False` | Shorthand for `log_level="DEBUG"`; conflicts if combined with a different explicit `log_level` |

`BridgeConfig`:

| Field | Default | Meaning |
|---|---|---|
| `source_host` / `destination_host` | required | The two brokers being bridged |
| `source_port` / `destination_port` | `1883` | Ports for each broker |
| `source_client_id` / `destination_client_id` | auto-generated | MQTT client IDs for each side |
| `envelope_topic` / `ack_topic` | `reliomq/messages` / `reliomq/acks` | Must match the publisher |
| `qos` | `1` | Fixed at 1 |
| `keepalive` | `60`s | MQTT keepalive |
| `destination_publish_timeout` | `2.0`s | Confirmation wait on the destination publish |
| `source_ack_publish_timeout` | `0.5`s | Confirmation wait on the source ACK publish |
| `retry_interval` | `10.0`s | Subscription retry delay |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |
| `max_queue_size` | `1000` | Bound on the bridge's in-memory handoff queue |
| `log_level` | `None` | Same as `PublisherConfig.log_level` |
| `debug` | `False` | Same as `PublisherConfig.debug` |

## Logging

`reliomq` uses the standard library `logging` module and, by default,
installs nothing: no handlers, no forced level, no `basicConfig()` call.
Nothing prints at INFO/DEBUG until you opt in one of two ways.

### The easy way: `log_level=`/`debug=` on your config

```python
config = PublisherConfig(host="localhost", queue_path="pending.jsonl", log_level="INFO")
# or, equivalently for the deepest view:
config = PublisherConfig(host="localhost", queue_path="pending.jsonl", debug=True)
```

This calls `reliomq.enable_logging()` for you, which attaches one
`StreamHandler` (to stderr) directly to the `"reliomq"` logger and turns off
further propagation from it — so it can never duplicate a line through a
root/application handler you've already configured elsewhere. Call
`enable_logging()` yourself if you want the same thing without going
through a config object:

```python
from reliomq import enable_logging
enable_logging("DEBUG")
```

It is safe to call (or trigger via `log_level=`) more than once — from
multiple publishers/bridges, for example — only one handler is ever
attached.

### The manual way: full control over formatting/routing

If you'd rather reliomq's records flow into your own logging setup (your
own formatter, your own handlers, merged with the rest of your app's logs),
skip `log_level=`/`debug=` entirely and configure Python's `logging` module
yourself — propagation stays on by default in that case:

```python
import logging
logging.basicConfig(level=logging.INFO)               # everything, INFO and up
logging.getLogger("reliomq").setLevel(logging.DEBUG)   # or scope it to just this library
```

### What each level shows

| Logger | Used by |
|---|---|
| `reliomq.publisher` | `ReliablePublisher`, and the `DurableMessageStore` it creates internally |
| `reliomq.bridge` | `ReliableMqttBridge` (override with `bridge_logger=`) |
| `reliomq.store` | a `DurableMessageStore` you construct directly without passing `logger=` |
| `reliomq.mqtt` | client creation and the `confirmed_publish()` helper |

**INFO** tells the story of the message lifecycle — enough to follow what's
happening without opening the source:

- publisher/bridge initialized and started, broker connecting/connected;
- a message accepted and durably queued (with the current pending count);
- pending messages restored after a restart;
- the application ACK received, and the message removed from the durable
  queue once delivery is confirmed;
- a retry being scheduled (with which message and how long until the next
  attempt);
- graceful shutdown.

**DEBUG** adds the diagnostic detail for tracing *one* message end-to-end by
its `message_id`, or figuring out why something didn't happen:

- each delivery attempt starting, with its attempt number;
- the publish attempt and the MQTT PUBACK confirmation, separately from the
  application ACK;
- waiting for the application ACK, and ACK matching (including *why* a
  stale/late/wrong-ID/malformed ACK was ignored);
- store-level decisions (append, duplicate-ID skip, removal);
- MQTT client creation and subscription bookkeeping.

Payloads are never logged, at any level — only `message_id`s, topics, and
counts. That's deliberate: DEBUG should never require an opt-in beyond the
level itself to be safe to turn on in production.

**WARNING** — recoverable trouble that's expected during normal outage
handling: disconnects, rejected subscriptions, late/malformed/wrong-ID ACKs
ignored, forward failures, a full bridge queue, and the reason a delivery
attempt is being retried. Nothing is lost when you see these.

**ERROR** (some via `logger.exception()`, with a traceback) — things that
should not happen: the worker failing to stop promptly on shutdown, an ACK
matching a message that turned out not to be the durable oldest, or an
unexpected exception in the delivery/forward loop.

## Examples

All scripts live in `examples/` and are runnable directly (`python
examples/<name>.py`) against a real broker unless noted otherwise:

- `basic.py` — the canonical minimal example: one-shot publish,
  `wait_for_delivery`, and `log_level="INFO"` for zero-setup visibility.
- `debug_logging.py` — `debug=True` walkthrough; runs with no broker at all
  (it points at one on purpose that isn't there) so you can see DEBUG-level
  diagnosis with zero setup.
- `sensor_loop.py` — a long-running periodic publisher with graceful
  SIGINT/SIGTERM shutdown and a pending-backlog warning; the shape most
  edge/IoT integrations actually use.
- `bridge.py` — minimal standalone forwarder service.
- `consumer_dedup.py` — a plain Paho subscriber (not part of this package)
  showing the recommended `message_id` deduplication pattern for a final
  consumer of bridged messages.
- `local_end_to_end.py` — publisher + bridge + consumer wired together
  against one local Mosquitto instance, so you can watch real PUBACKs,
  reconnects, and the on-disk queue file; kill and restart the broker
  mid-run to see `pending_count()` rise and drain.
- `tls_auth_client.py` — injecting TLS and username/password auth through a
  custom `client_factory` without adding security config to the library.

## Migrating from 0.1.x

v0.2.0 is backward compatible: every 0.1.x name below still works today and
will keep working for a deprecation period, just with a `DeprecationWarning`
pointing at its replacement. Nothing you already have deployed breaks.

| Old (0.1.x) | New (0.2.0) | Notes |
|---|---|---|
| `ReliabilityConfig` | `PublisherConfig` | Same fields; name now pairs with `ReliablePublisher` the way `BridgeConfig` pairs with `ReliableMqttBridge`. |
| `data_topic=` (on either config) | `envelope_topic=` | Disambiguates reliomq's own transport topic from the application `topic=` you pass to `publish()`. A `.data_topic` property still reads back `.envelope_topic` if you have existing code that accesses it. |
| `event_id=` (on `publish()`, `wait_for_delivery()`, the protocol dataclasses) | `message_id=` | Same value, clearer name: it identifies *one message*, not an "event." An `.event_id` property still reads back `.message_id` on the protocol objects. **The wire format is unchanged** — the JSON field is still `"event_id"`, so 0.1.x and 0.2.x processes remain interoperable. |
| `reliomq.protocol.new_event_id()` / `validate_event_id()` | `new_message_id()` / `validate_message_id()` | Same behavior. |

```python
# Before (0.1.x) -- still works, now warns
from reliomq import ReliabilityConfig, ReliablePublisher
config = ReliabilityConfig(host="localhost", queue_path="pending.jsonl", data_topic="in")
publisher = ReliablePublisher(config)
message = publisher.publish("t", {"x": 1}, event_id="my-id")

# After (0.2.0)
from reliomq import PublisherConfig, ReliablePublisher
config = PublisherConfig(host="localhost", queue_path="pending.jsonl", envelope_topic="in")
publisher = ReliablePublisher(config)
message = publisher.publish("t", {"x": 1}, message_id="my-id")
```

Everything else — the reliability guarantee, retry/reconnect/shutdown
behavior, and the on-disk queue format — is unchanged. See
[CHANGELOG.md](CHANGELOG.md) for the complete v0.2.0 release notes.

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
unit tests with directly injected ACKs cannot see. `test_logging.py` covers
the observability story: default-quiet behavior, `enable_logging()`
idempotency and non-duplication, `log_level=`/`debug=` wiring, and that
INFO/DEBUG actually carry the content documented above. Deprecated-alias
compatibility (old class/keyword/property names, each with its
`DeprecationWarning`) is tested alongside its own module in a dedicated test
class per file rather than a separate file. An optional Mosquitto
integration test is skipped when the broker executable is unavailable.

## License

MIT — see [LICENSE](LICENSE).

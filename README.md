# reliomq

**Durable, end-to-end confirmed MQTT delivery for Python.**

`reliomq` sits on top of [`paho-mqtt`](https://pypi.org/project/paho-mqtt/)
and adds the part QoS 1 doesn't give you: a message survives a crash, an
outage, or a broker restart, and is retried automatically — in order, under
its original ID — until an application confirms it actually arrived.

```python
from reliomq import Sender, SenderConfig

sender = Sender(SenderConfig(
    host="localhost",
    outbox_path="pending.jsonl",
    debug=True,  # see what reliomq is doing, with zero logging setup
))
sender.connect()
sender.publish("factory/machine1/data", {"temperature": 25.2})
sender.disconnect()
```

That call durably stores the message in the **Outbox** before it ever
touches the network. Everything else — reconnects, retries, ordering, and
knowing when it's actually safe to forget the message — is handled for you.

Requires **Python 3.11+** and **Paho MQTT 2.x**.

> **Upgrading from an earlier release?** Nothing breaks. `ReliablePublisher`/
> `PublisherConfig`/`ReliableMqttBridge`/`BridgeConfig`/`DurableMessageStore`/
> `Ack`, `queue_path=`, `envelope_topic=`/`data_topic=`, `ack_topic=`, and
> `event_id=` all still work — they now emit a `DeprecationWarning` pointing
> at their replacement. See [Migrating to 0.3.0](#migrating-to-030) below
> and [CHANGELOG.md](CHANGELOG.md) for the full picture.

## Why not just `qos=1`?

QoS 1 only proves the broker your process is directly connected to accepted
one publish. It proves nothing about:

- whether your process crashes or loses power before that publish happens;
- whether the broker forwards it any further (a relay, another hop);
- whether anything ever confirms, at the application level, that the message
  did its job.

`reliomq` closes that gap with a durable Outbox plus an application-level
**DeliveryAck**, so "the network layer said OK" is never mistaken for "the
message is handled."

## How reliomq works

```text
Application
    |
    |  sender.publish(topic, payload)  -- returns immediately, message_id assigned
    v
Sender                    <-- Outbox: durable, fsync'd JSONL file
    |
    |  QoS 1 publish, on the *relay topic*
    v
source MQTT broker
    |
    v
Relay                     <-- subscribes to the relay topic
    |
    |  QoS 1 publish, on the message's real *application* topic
    v
destination MQTT broker  --->  your consumer(s)
    |
    |  once that publish is broker-confirmed...
    v
Relay publishes a DeliveryAck back on the *delivery-ack topic*
    |
    v
Sender matches the DeliveryAck to message_id, THEN removes it
from the Outbox
```

`Sender` never deletes a message on network confirmation alone. It deletes a
message only after a matching `DeliveryAck` comes back — which `Relay` only
sends once *its own* publish to the real destination topic was itself QoS 1
confirmed. That is the whole design in one sentence: **two hops, two
confirmations, one durable record that only goes away after both.**

### Two different kinds of "confirmed" — don't conflate them

| | What it proves | Who provides it |
|---|---|---|
| **MQTT PUBACK** (QoS 1) | The broker *this process is connected to* accepted one publish. | Paho / the broker, automatically. |
| **DeliveryAck** | A `Relay` (or your own code speaking the same protocol) actually forwarded the message to its real destination topic, broker-confirmed. | reliomq's own wire protocol, on the delivery-ack topic. |

A message is only removed from the Outbox after **both**. This is why the
message stays queued (and keeps retrying) through: a broker outage, a relay
that's down, a relay that forwarded but whose own DeliveryAck got lost, or
your process crashing and restarting mid-delivery — in every one of those
cases, "network said OK" was never good enough on its own, so nothing was
deleted. See [Persistence and restart recovery](#persistence-and-restart-recovery)
for the on-disk details.

## Architecture overview

| Component | Responsibility | You touch it when... |
|---|---|---|
| `SenderConfig` | Validated settings for `Sender`: broker, topics, timeouts, Outbox path, logging. | Constructing a sender. |
| `RelayConfig` | Same, for `Relay`: source + destination brokers. | Constructing a relay. |
| `Sender` | **Public API.** Durably stores messages and retries them until a `DeliveryAck` confirms delivery. | This is what your application calls: `connect()`, `publish()`, `wait_for_delivery()`, `pending_count()`. |
| `Relay` | **Public API.** Relays from a source broker to a destination broker and only ACKs the source after the destination publish is confirmed. | Run as its own process/service between two brokers. Not needed if you only care about durable *delivery to a broker* rather than end-to-end confirmed forwarding. |
| `Outbox` | Crash-safe, on-disk FIFO queue (JSONL). Used internally by `Sender`; safe to use directly for inspection/maintenance scripts. | Rarely — mostly internal. |
| `DeliveryAck` / `MessageEnvelope` / `DeliveryEnvelope` | The wire-protocol shapes, all correlated by `message_id`. | Only if you're implementing your own consumer or a compatible relay from scratch. |
| `enable_logging()` | Attaches reliomq's zero-setup logging handler. | Called for you by `log_level=`/`debug=` on either config; call it directly if you want the same thing without a config object. |

## If you already know Paho MQTT

`Sender`'s lifecycle is deliberately Paho-shaped:

| Paho MQTT | reliomq | Same semantics? |
|---|---|---|
| `Client` | `Sender` / `Relay` | Not quite — see below. |
| `client.connect()` | `sender.connect()` (alias: `start()`) | Yes, plus starts the delivery worker -- see below. |
| `client.loop_start()` | `sender.loop_start()` (alias: `start()`) | Same call as `connect()` in reliomq -- see below. |
| `client.loop_stop()` | `sender.loop_stop()` (alias: `stop()`) | Same call as `disconnect()`. |
| `client.disconnect()` | `sender.disconnect()` (alias: `stop()`) | Yes. |
| `client.is_connected()` | `sender.is_connected()` | Yes -- transport state only, see its docstring. |
| `client.publish()` | `sender.publish()` | **No — see below.** |
| MQTT message ID (`mid`) | `message_id` | No -- different layer entirely, see below. |
| PUBACK | *(the MQTT-level acknowledgement `publish()` waits for internally)* | -- |
| *(nothing)* | `DeliveryAck` | reliomq-specific: application-level, end-to-end. |
| *(nothing)* | `Outbox` | reliomq-specific: the durable queue underneath everything. |
| *(nothing)* | `wait_for_delivery()` | reliomq-specific: blocks for a `DeliveryAck`, not a PUBACK. |
| *(nothing)* | `pending_count()` | reliomq-specific: size of the durable backlog. |

**The crucial difference:** Paho's `publish()` is primarily an MQTT
*transport* operation — it hands one message to the network and its result
tells you whether the broker accepted that one publish. reliomq's
`publish()` is a *reliable-delivery* operation: it durably persists the
message first (before touching the network at all) and then manages
retry/reconnect/acknowledgement for it — across outages, and across a full
process restart — until a `DeliveryAck` confirms it actually got to its
real destination. Do not assume these behave identically; see
[Publishing](#publishing) below for exactly what `publish()`'s return value
does and doesn't promise.

**On the lifecycle merge:** raw Paho lets you `connect()` without ever
calling `loop_start()` (or vice versa in some patterns) — the connection and
the background network thread are separable. reliomq cannot honestly offer
that split: durable delivery *is* the background worker that watches the
Outbox and manages retries/ACKs, and there is no useful state where a
connection exists but that worker isn't running. So on `Sender`,
`connect()`, `start()`, and `loop_start()` are three names for the exact
same one operation (same for `disconnect()`/`stop()`/`loop_stop()`) — call
whichever reads best in your code; calling more than one is a harmless
no-op. `Relay` manages *two* Paho clients (one per broker), so its
`connect()`/`start()` brings up both connections together — see
[Relay integration](#relay-integration).

## Features

- **Durable-before-network writes** — every `publish()` is `fsync`'d to the
  Outbox before the first network attempt, so a crash immediately after
  `publish()` returns still can't lose the message.
- **QoS 1 MQTT delivery**, always — reliability isn't opt-in per call.
- **Application-level end-to-end `DeliveryAck`**, correlated by a stable
  `message_id`, in addition to the MQTT PUBACK.
- **Automatic, unique message IDs** — generated for you, or you can supply
  your own; never regenerated on retry.
- **Strict FIFO recovery** — the oldest pending message is always retried
  first; a new live message can never overtake it.
- **Automatic restart recovery** — a fresh process just opens the same
  Outbox file and continues exactly where the last one left off.
- **Automatic retry** on broker outage, network failure, publish errors,
  publish-confirmation timeout, and DeliveryAck timeout — nothing is
  deleted on any of these.
- **Optional `Relay` component** that forwards between two brokers and
  only ACKs the source *after* the destination publish is confirmed —
  never before.
- **Fail-closed forwarding** — any relay failure (offline, malformed
  message, full queue, timeout, shutdown) sends no DeliveryAck, so the
  source keeps retrying instead of silently dropping the message.
- **Safe ACK handling** — stale, late, duplicate, wrong-ID, and malformed
  DeliveryAcks are all detected and ignored rather than treated as success.
- **Automatic reconnect/backoff** via Paho, with explicit connection-state
  tracking so the library never publishes while it knows it's disconnected.
- **Clean shutdown** — stopping interrupts an in-progress ACK wait without
  ever deleting the durable record for the message being waited on.
- **Corruption-safe storage** — a damaged Outbox line is logged and
  skipped, never silently deleted; a crash mid-write can't destroy the
  next record.
- **Pluggable client construction** — inject your own `client_factory` for
  TLS, auth, or any other Paho client customization.
- **Strict JSON envelope validation** — rejects `NaN`/`Infinity`, bytes,
  tuples, non-string keys, and other values with no exact JSON form.
- **Thread-safe by design** — internal locks are never held during a
  network wait, so a DeliveryAck can never be missed to a race.
- **Zero-setup runtime visibility** — `debug=True` or `log_level=` on
  either config gives you a running narration of connects, stored
  messages, ACKs, and retries, without touching Python's `logging` module
  yourself.

## Reliability guarantee

`reliomq` provides **at-least-once delivery to the destination broker**
when `Sender` and `Relay` are used together. It favors *never silently
losing a message* over *never duplicating one*.

A record is deleted from the Outbox **only** after a valid `DeliveryAck`
carrying its exact `message_id` arrives back on the configured delivery-ack
topic. Broker acceptance alone is not proof that a final subscriber
processed the message — if you need that stronger boundary, use a
persistent MQTT subscription or extend the protocol with a consumer ACK of
your own.

Duplicates remain possible: the destination publish can succeed and the
DeliveryAck can then be lost, in which case `Sender` correctly retries the
same stable `message_id`. **Consumers should store processed message IDs
and make handling idempotent** — see `examples/consumer_dedup.py`.

## Install

```bash
# From GitHub, pinned to a release (recommended)
pip install "git+https://github.com/pitpib23/reliomq.git@v0.3.0"

# Or track the latest commit on main
pip install "git+https://github.com/pitpib23/reliomq.git"

# Or from a local clone
pip install -e /path/to/reliomq
```

The only runtime dependency is `paho-mqtt>=2,<3`. There is no GPIO, Modbus,
or other hardware dependency of any kind (see
[examples/modbus_sensor.py](examples/modbus_sensor.py) for an example that
*optionally* uses `pymodbus`, installed separately).

## Getting started

The shortest useful shape — context manager, one publish, done:

```python
from reliomq import Sender, SenderConfig

config = SenderConfig(host="localhost", outbox_path="pending.jsonl", log_level="INFO")

with Sender(config) as sender:          # __enter__ calls connect()
    sender.publish("factory/machine1/data", {"temperature": 25.2})
# __exit__ calls disconnect(); any still-pending message survives for next time.
```

The explicit, Paho-familiar form (equivalent — see
[examples/paho_style_lifecycle.py](examples/paho_style_lifecycle.py)):

```python
sender = Sender(config)
sender.connect()
sender.loop_start()  # harmless no-op here -- connect() already did this

try:
    message_id = sender.publish(
        "factory/machine1/data",
        {"temperature": 25.2, "pressure": 4.1},
    )
    delivered = sender.wait_for_delivery(message_id, timeout=10.0)
finally:
    sender.loop_stop()
    sender.disconnect()
```

## Publishing

`payload` may be any strict JSON value: an object with string keys, an
array, a string, a finite number, a boolean, or `null`. `NaN`, infinity,
bytes, tuples, custom objects, and mappings with non-string keys are
rejected. `publish()` returns the message's `message_id` directly (a plain
`str` — not a wrapper object, since there's nothing more to inspect
synchronously: reliomq's whole design is that delivery confirmation happens
later, asynchronously). A `message_id` is generated automatically unless
you supply `message_id=`. Explicit IDs must be globally unique and must not
be reused for different content.

**`publish()`'s return means "reliomq accepted the message into its durable
workflow" — nothing more.** It does *not* mean:

- the destination broker has already accepted it;
- a `DeliveryAck` has already come back;
- the message has already left the Outbox.

If the Outbox cannot be written safely, `publish()` raises a `StoreError`
(see `Outbox`/`OutboxError`) instead of pretending to have accepted the
message.

```python
# 1. Basic publish
message_id = sender.publish("factory/sensor-01", {"temperature": 25.2})

# 2. Capture message_id for later correlation (logs, a database row, etc.)
readings[message_id] = {"sensor": "sensor-01", "sent_at": time.time()}

# 3. Publish and continue -- the common case in a loop; never blocks on the network
for reading in poll_sensor():
    sender.publish("factory/sensor-01", reading)

# 4. Publish and wait for end-to-end delivery (reliomq-specific; no Paho equivalent)
message_id = sender.publish("factory/sensor-01", {"temperature": 25.2})
if sender.wait_for_delivery(message_id, timeout=10.0):
    print("confirmed delivered")
else:
    print("still pending -- reliomq keeps retrying it")

# 5. Publish while the broker/network is offline -- this still succeeds:
#    the message is durably stored and reliomq retries once connectivity
#    returns. publish() never raises just because the network is down.
message_id = sender.publish("factory/sensor-01", {"temperature": 25.2})

# 6. Restart with pending messages -- construct Sender again with the same
#    outbox_path; every undelivered message from the previous run is still
#    there, in the same order, under the same message_id, and delivery
#    resumes automatically once connect()/start() runs.
sender = Sender(SenderConfig(host="localhost", outbox_path="pending.jsonl"))
sender.connect()

# 7. Inspect the durable backlog without blocking (see sensor_loop.py for
#    the recommended pattern: warn past a threshold, never poll-wait per reading)
if sender.pending_count() > 50:
    logging.warning("delivery is falling behind: %s pending", sender.pending_count())
```

Do not call `wait_for_delivery()` after every `publish()` in a tight loop
(e.g. a sensor reading every few seconds) — that serializes every reading
behind a network round trip. Use `pending_count()` to monitor backlog
without blocking instead; see `examples/sensor_loop.py`.

## Debugging

Swap `log_level="INFO"` for `debug=True` (equivalent to `log_level="DEBUG"`)
to see every internal step: publish attempts, PUBACK confirmation, waiting
for the DeliveryAck, why a retry happened, and so on:

```python
config = SenderConfig(host="localhost", outbox_path="pending.jsonl", debug=True)
```

Run [examples/debug_logging.py](examples/debug_logging.py) for a runnable
version of this — it points at a broker on purpose that isn't there, so you
can see the DEBUG output with zero setup. See [Logging](#logging) below for
exactly what each level shows.

## Relay integration

Use `Relay` only if you need to forward messages from one broker to another
with end-to-end confirmation. Run it as its own service or process; its
`relay_topic` and `delivery_ack_topic` must match the `Sender`'s
configuration.

```python
from reliomq import Relay, RelayConfig

relay = Relay(
    RelayConfig(
        source_host="localhost",
        source_port=1883,
        destination_host="mqtt.example.net",
        destination_port=1883,
        relay_topic="reliable/ingress",
        delivery_ack_topic="reliable/acks",
        destination_publish_timeout=2.0,
        source_ack_publish_timeout=0.5,
        log_level="INFO",
    )
)

relay.connect()  # brings up BOTH the source and destination broker connections
...
relay.disconnect()
```

`Relay.connect()`/`start()` (and `disconnect()`/`stop()`) intentionally has
only one lifecycle call, unlike `Sender` — there is no separate
`loop_start()`-without-`connect()` split offered, because a message sitting
connected-but-unforwarded is exactly the situation this library exists to
avoid leaving unresolved. `loop_start()`/`loop_stop()` aliases exist for
naming symmetry with `Sender` and do the same thing as `connect()`/
`disconnect()`. If you need to tell the two broker connections apart, use
the read-only `relay.source_connected`/`relay.destination_connected`
properties.

The relay forwards to the destination topic stored in each message. The
destination payload retains the deduplication key:

```json
{
  "version": 1,
  "event_id": "47913ac65ac84213a9361b393b845708",
  "payload": {"temperature": 25.2}
}
```

`Relay`'s handoff queue is intentionally memory-only, not another Outbox. A
malformed message, full queue, outage, publish error, timeout, or shutdown
produces no DeliveryAck; the sender still owns the durable record and
retries it. Deploy only one ordinary relay subscriber per route unless
duplicate forwarding is intended.

## Cookbook

### Generic sensor loop

The normal long-running pattern — one `Sender`, created once, publishing on
an interval (full runnable version in
[examples/sensor_loop.py](examples/sensor_loop.py)):

```python
sender = Sender(config)
sender.connect()
sender.loop_start()

try:
    while True:
        payload = read_sensor()
        message_id = sender.publish("factory/sensor-01", payload)
        time.sleep(5)
finally:
    sender.loop_stop()
    sender.disconnect()
```

### Modbus TCP sensor

A realistic read-only Modbus TCP poller bridged to MQTT is in
[examples/modbus_sensor.py](examples/modbus_sensor.py). It only ever calls
`read_holding_registers()` (no writes), initializes both the Modbus client
and the `Sender` once, and shows: periodic polling, payload construction,
INFO logging, error handling around individual reads, graceful
`KeyboardInterrupt`/`SIGTERM` shutdown, and why offline-MQTT and
process-restart are both non-events for data already collected. It requires
the optional `pymodbus` package (`pip install pymodbus`) — reliomq itself
has no Modbus or hardware dependency.

## Wire protocol

`Sender` to `Relay`, on `relay_topic` with QoS 1 and `retain=False`:

```json
{
  "version": 1,
  "event_id": "47913ac65ac84213a9361b393b845708",
  "topic": "factory/machine1/data",
  "payload": {"temperature": 25.2}
}
```

`Relay` to `Sender`, on `delivery_ack_topic` with QoS 1 and `retain=False`:

```json
{"version": 1, "event_id": "47913ac65ac84213a9361b393b845708"}
```

> **Note on naming:** the Python API calls this identifier `message_id` —
> but the JSON field on the wire is still spelled `event_id`, unchanged
> since 0.1.0. That's deliberate: it means a 0.1.x sender and a 0.3.x relay
> (or vice versa) stay fully interoperable through a rolling upgrade. Only
> the Python-facing name changed, and the protocol version has not changed
> since 0.1.0 for the same reason.

Protocol objects are strict and versioned. Unknown/missing fields, malformed
UTF-8/JSON, invalid IDs, and DeliveryAcks for any ID other than the one
currently in flight are ignored. Correlation uses the ID alone rather than
payload equality, so it works with any payload shape.

## Persistence and restart recovery

The Outbox is a lightweight JSONL file suitable for edge devices:

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

One process must own an `outbox_path`. `Outbox` is thread-safe but does not
attempt cross-process file locking. JSONL removal rewrites the file, so a
database-backed store may be more appropriate for extremely large queues or
sustained high write rates.

## Connection, retry, and shutdown behavior

Both components use Paho's asynchronous network loop and reconnect backoff.
`Sender` does not send until the broker connection and DeliveryAck
subscription are ready. A disconnect interrupts the current ACK wait
without removing its record. Reconnect wakes recovery immediately; other
failures retry after `retry_interval`.

`Sender.disconnect()`/`stop()` interrupts an ACK wait and joins the worker.
Because the in-flight message was already durable, it remains for the next
process. `Relay.disconnect()`/`stop()` stops accepting new input, lets the
bounded in-flight publish/ACK sequence finish, and leaves queued items
unacknowledged so their senders recover them. Lifecycle calls are
idempotent; a stopped instance is not restartable, so create a new instance
to restart a service.

## Configuration reference

QoS is fixed at 1 on both configs. Topics are validated as publish topics
and cannot contain MQTT wildcards. `relay_topic`/`delivery_ack_topic` are
reliomq's own transport topics — not the application topic you pass to
`publish()`. Authentication/TLS is applied by supplying a configured
`client_factory` when constructing a component (see
`examples/tls_auth_client.py`) — the config objects intentionally carry no
credentials.

`SenderConfig`:

| Field | Default | Meaning |
|---|---|---|
| `host` | required | Broker hostname |
| `outbox_path` | required | Durable Outbox file path |
| `port` | `1883` | Broker port |
| `client_id` | auto-generated | MQTT client ID |
| `relay_topic` | `reliomq/messages` | Topic the sender sends its envelope on (not the application topic) |
| `delivery_ack_topic` | `reliomq/acks` | Topic the sender listens on for DeliveryAcks |
| `qos` | `1` | Fixed at 1 |
| `ack_timeout` | `3.0`s | How long to wait for the DeliveryAck |
| `publish_timeout` | `2.0`s | How long to wait for MQTT publish confirmation |
| `retry_interval` | `10.0`s | Delay between retries after a failure |
| `keepalive` | `60`s | MQTT keepalive |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |
| `log_level` | `None` | `"DEBUG"`/`"INFO"`/... or a `logging` level int; `None` leaves logging exactly as-is (see [Logging](#logging)) |
| `debug` | `False` | Shorthand for `log_level="DEBUG"`; conflicts if combined with a different explicit `log_level` |

`RelayConfig`:

| Field | Default | Meaning |
|---|---|---|
| `source_host` / `destination_host` | required | The two brokers being connected |
| `source_port` / `destination_port` | `1883` | Ports for each broker |
| `source_client_id` / `destination_client_id` | auto-generated | MQTT client IDs for each side |
| `relay_topic` / `delivery_ack_topic` | `reliomq/messages` / `reliomq/acks` | Must match the sender |
| `qos` | `1` | Fixed at 1 |
| `keepalive` | `60`s | MQTT keepalive |
| `destination_publish_timeout` | `2.0`s | Confirmation wait on the destination publish |
| `source_ack_publish_timeout` | `0.5`s | Confirmation wait on the DeliveryAck publish |
| `retry_interval` | `10.0`s | Subscription retry delay |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |
| `max_queue_size` | `1000` | Bound on the relay's in-memory handoff queue |
| `log_level` | `None` | Same as `SenderConfig.log_level` |
| `debug` | `False` | Same as `SenderConfig.debug` |

## Logging

`reliomq` uses the standard library `logging` module and, by default,
installs nothing: no handlers, no forced level, no `basicConfig()` call.
Nothing prints at INFO/DEBUG until you opt in one of two ways.

### The easy way: `log_level=`/`debug=` on your config

```python
config = SenderConfig(host="localhost", outbox_path="pending.jsonl", log_level="INFO")
# or, equivalently for the deepest view:
config = SenderConfig(host="localhost", outbox_path="pending.jsonl", debug=True)
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
multiple senders/relays, for example — only one handler is ever attached.

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
| `reliomq.sender` | `Sender`, and the `Outbox` it creates internally |
| `reliomq.relay` | `Relay` (override with `relay_logger=`) |
| `reliomq.outbox` | an `Outbox` you construct directly without passing `logger=` |
| `reliomq.mqtt` | client creation and the `confirmed_publish()` helper |

**INFO** tells the story of the message lifecycle — enough to follow what's
happening without opening the source. Consistently uses reliomq's own
vocabulary (`Sender`, `Relay`, `Outbox`, `DeliveryAck`):

- Sender/Relay initialized and started, broker connecting/connected;
- Outbox opened, with its pending count;
- pending messages restored after a restart;
- a message accepted and stored in the Outbox (with the current pending
  count);
- the DeliveryAck received, and the message completed (removed from the
  Outbox) once delivery is confirmed;
- a retry being scheduled (with which message and how long until the next
  attempt);
- `Relay forwarded` / `DeliveryAck sent` on the relay side;
- graceful shutdown.

**DEBUG** adds the diagnostic detail for tracing *one* message end-to-end by
its `message_id`, or figuring out why something didn't happen. Lower-level
MQTT-protocol detail correctly stays MQTT-specific here (`PUBACK`, Paho's
own `mid`, `MQTT publish`) rather than being forced into reliomq's
vocabulary — precision matters more than consistency at this layer:

- each delivery attempt starting, with its attempt number;
- the publish attempt and the MQTT PUBACK confirmation, separately from the
  DeliveryAck;
- waiting for the DeliveryAck, and ACK matching (including *why* a
  stale/late/wrong-ID/malformed one was ignored);
- Outbox-level decisions (append, duplicate-ID skip, removal);
- MQTT client creation and subscription bookkeeping, including Paho's own
  `mid` (packet identifier) where relevant — deliberately not renamed to
  `message_id`, since it is a different concept at a different layer.

Payloads and credentials are never logged, at any level — only
`message_id`s, topics, and counts. That's deliberate: DEBUG should never
require an opt-in beyond the level itself to be safe to turn on in
production.

**WARNING** — recoverable trouble that's expected during normal outage
handling: disconnects, rejected subscriptions, late/malformed/wrong-ID
DeliveryAcks ignored, forward failures, a full relay queue, and the reason
a delivery attempt is being retried. Nothing is lost when you see these.

**ERROR** (some via `logger.exception()`, with a traceback) — things that
should not happen: the worker failing to stop promptly on shutdown, a
DeliveryAck matching a message that turned out not to be the Outbox oldest,
or an unexpected exception in the delivery/forward loop.

## Public API

Every supported public class, method, property, and exception, with where
to see it used.

### `Sender` / `SenderConfig`

The main entry point. See [Getting started](#getting-started) and
[Publishing](#publishing) above for full examples of everything below.

- **`Sender(config, *, client_factory=None, outbox=None)`** — construct a
  sender. Raises `TypeError` if `config` isn't a `SenderConfig`. Reads
  `config.log_level` and calls `enable_logging()` if set. Side effect:
  opens (or creates) the Outbox at `config.outbox_path` immediately.
- **`sender.connect()`** / **`sender.start()`** / **`sender.loop_start()`**
  — three names for one operation: start the MQTT connection and the
  background delivery worker. Idempotent; safe to call more than once.
  Returns `self`.
  ```python
  sender.connect()
  ```
- **`sender.disconnect()`** / **`sender.stop()`** / **`sender.loop_stop()`**
  — three names for one operation: stop cleanly without losing the
  in-flight message. Idempotent.
  ```python
  sender.disconnect()
  ```
- **`sender.is_connected()`** — `bool`. Mirrors Paho: true once the MQTT
  connection is up. Does *not* by itself mean reliomq is ready to
  deliver (see its docstring); don't poll it to decide whether `publish()`
  is safe to call — it always is.
  ```python
  if sender.is_connected():
      print("MQTT transport is up")
  ```
- **`sender.publish(topic, payload, *, message_id=None)`** — durably store
  a message; returns its `str` `message_id`. Safe before `connect()`.
  Raises `ValueError` if `message_id` is reused for different content;
  raises `StoreError` if the Outbox can't be written. See
  [Publishing](#publishing) for the full set of examples and exactly what
  the return value does/doesn't promise.
- **`sender.wait_for_delivery(message_id=None, timeout=None)`** — `bool`.
  Blocks for one message (or the whole Outbox if `message_id` is omitted).
  reliomq-specific; no Paho equivalent.
  ```python
  delivered = sender.wait_for_delivery(message_id, timeout=10.0)
  ```
- **`sender.pending_count()`** — `int`. Current Outbox backlog size.
  reliomq-specific.
  ```python
  if sender.pending_count() > 50:
      logging.warning("falling behind")
  ```
- **`sender.outbox`** — the `Outbox` instance backing this sender. Public,
  safe to inspect (e.g. `sender.outbox.peek_oldest()`).
- **`with Sender(config) as sender:`** — context manager; `__enter__` calls
  `connect()`, `__exit__` calls `disconnect()`.
- **`SenderConfig(host, outbox_path, ...)`** — see
  [Configuration reference](#configuration-reference) for every field.
  Raises `ConfigError` on any invalid value, immediately at construction.

### `Relay` / `RelayConfig`

Optional end-to-end forwarder between two brokers. See
[Relay integration](#relay-integration) above for full examples.

- **`Relay(config, *, client_factory=None, source_client_factory=None, destination_client_factory=None, relay_logger=None)`**
  — construct a relay. Raises `TypeError` if `config` isn't a `RelayConfig`.
- **`relay.connect()`** / **`relay.start()`** / **`relay.loop_start()`** —
  bring up *both* the source and destination broker connections plus the
  forwarding worker, together. Idempotent.
- **`relay.disconnect()`** / **`relay.stop()`** / **`relay.loop_stop()`** —
  tear both down cleanly; queued-but-unforwarded messages are abandoned
  (safe: their senders still hold the durable record). Idempotent.
- **`relay.source_connected`** / **`relay.destination_connected`** —
  `bool` properties, independent per-broker connection state.
- **`relay.source_subscription_ready`** — `bool`. Whether the relay-topic
  subscription on the source broker has been confirmed (SUBACK).
- **`relay.queued_count`** — `int`. Current depth of the relay's in-memory
  (non-durable) handoff queue — not the same thing as a sender's
  `pending_count()`.
- **`relay.is_running`** — `bool`.
- **`with Relay(config) as relay:`** — context manager, same shape as
  `Sender`'s.
- **`RelayConfig(source_host, destination_host, ...)`** — see
  [Configuration reference](#configuration-reference).

### `Outbox`

The durable queue underneath `Sender`. Deliberately not dressed up as an
MQTT concept — this is what gives reliomq its durability, and plain MQTT
has nothing like it. Safe to use directly for a maintenance/inspection
script; see [Persistence and restart recovery](#persistence-and-restart-recovery).

```python
from reliomq import Outbox

outbox = Outbox("pending.jsonl")
print(outbox.size(), "messages pending")
for envelope in outbox.load():
    print(envelope.message_id, envelope.topic)
```

- **`Outbox(path, logger=None)`** — opens (or creates on first append) the
  file at `path`. Logs its pending count at INFO immediately.
- **`outbox.append(envelope)`** — `bool`; durably stores a `MessageEnvelope`
  before returning. `False` if that `message_id` is already pending.
- **`outbox.peek_oldest()`** — `MessageEnvelope | None`; the current FIFO
  head, without removing it.
- **`outbox.remove_oldest(expected)`** — `bool`; removes the head only if
  it exactly matches `expected` (this is what keeps FIFO/ACK correlation
  trustworthy across a restart).
- **`outbox.load()`** — `list[MessageEnvelope]`; every pending message, in
  order.
- **`outbox.size()`** / **`len(outbox)`** — `int`.
- **`outbox.contains(message_id)`** — `bool`.
- **`OutboxError`** — raised for I/O failures (never for an empty queue,
  which is a normal `None`/`0`/`[]` result, not an error).

### `DeliveryAck` / `MessageEnvelope` / `DeliveryEnvelope`

The wire-protocol dataclasses; only relevant if you're writing your own
consumer or a compatible relay. See [Wire protocol](#wire-protocol).

- **`MessageEnvelope(topic, payload, message_id=None)`** — what `Sender`
  puts on the relay topic.
- **`DeliveryEnvelope(payload, message_id=None)`** — what `Relay` puts on
  the real destination topic.
- **`DeliveryAck(message_id=None)`** — what `Relay` puts on the
  delivery-ack topic. All three: `.to_bytes()` / `.from_bytes(data)` for
  wire encode/decode, and raise `ProtocolError` on anything invalid.

### `enable_logging()`

See [Logging](#logging) above.

```python
from reliomq import enable_logging
enable_logging("INFO")
```

### Exceptions

| Exception | Raised when |
|---|---|
| `ConfigError` | A `SenderConfig`/`RelayConfig` field is invalid, at construction. |
| `ProtocolError` | A wire-protocol value/payload is invalid (malformed JSON, wrong schema, non-JSON payload type, etc.). |
| `OutboxError` | The Outbox file can't be read or written safely (I/O failure — never used for "queue is empty"). |
| `ValueError` (from `sender.publish()`) | An explicit `message_id` is reused for different content. |

## Examples

All scripts live in `examples/` and are runnable directly (`python
examples/<name>.py`) against a real broker unless noted otherwise:

- `basic.py` — the shortest useful `Sender` example: context manager,
  one publish, `wait_for_delivery`, `log_level="INFO"`.
- `paho_style_lifecycle.py` — the explicit `connect()`/`loop_start()`/
  `loop_stop()`/`disconnect()` form, shown equivalent to the context
  manager.
- `debug_logging.py` — `debug=True` walkthrough; runs with no broker at all
  (it points at one on purpose that isn't there) so you can see DEBUG-level
  diagnosis with zero setup.
- `sensor_loop.py` — a long-running periodic sender with graceful
  SIGINT/SIGTERM shutdown and a pending-backlog warning; the shape most
  edge/IoT integrations actually use.
- `modbus_sensor.py` — a realistic read-only Modbus TCP poller bridged to
  MQTT (optional `pymodbus` dependency).
- `relay.py` — minimal standalone `Relay` service.
- `consumer_dedup.py` — a plain Paho subscriber (not part of this package)
  showing the recommended `message_id` deduplication pattern for a final
  consumer of relayed messages.
- `local_end_to_end.py` — `Sender` + `Relay` + consumer wired together
  against one local Mosquitto instance, so you can watch real PUBACKs,
  reconnects, and the on-disk Outbox file; kill and restart the broker
  mid-run to see `pending_count()` rise and drain.
- `tls_auth_client.py` — injecting TLS and username/password auth through a
  custom `client_factory` without adding security config to the library.

## Migrating to 0.3.0

0.3.0 is backward compatible: every earlier name below still works today
and will keep working for a deprecation period, just with a
`DeprecationWarning` pointing at its replacement. Nothing you already have
deployed breaks.

| Old | New | Notes |
|---|---|---|
| `ReliablePublisher` | `Sender` | Same class, renamed. Gained `connect()`/`disconnect()`/`loop_start()`/`loop_stop()`/`is_connected()` -- all new, none removed. |
| `PublisherConfig` (0.2.x) / `ReliabilityConfig` (0.1.x) | `SenderConfig` | Same fields. |
| `ReliableMqttBridge` | `Relay` | Same class, renamed. Gained `connect()`/`disconnect()`/`loop_start()`/`loop_stop()`/`source_connected`/`destination_connected`. |
| `BridgeConfig` | `RelayConfig` | Same fields. |
| `DurableMessageStore` | `Outbox` | Same class, renamed. `StoreError` renamed to `OutboxError` (both still work). |
| `Ack` | `DeliveryAck` | Same class, renamed. |
| `queue_path=` | `outbox_path=` | |
| `data_topic=` (0.1.x) / `envelope_topic=` (0.2.x) | `relay_topic=` | Two generations of alias, both still accepted. |
| `ack_topic=` | `delivery_ack_topic=` | |
| `sender.store` (was `publisher.store`) | `sender.outbox` | |
| `reliomq.publisher` / `reliomq.bridge` / `reliomq.store` (module paths) | `reliomq.sender` / `reliomq.relay` / `reliomq.outbox` | Old import paths still work via thin re-export modules. |
| `Relay(..., bridge_logger=...)` | `Relay(..., relay_logger=...)` | |

`publish()` and `wait_for_delivery()` are **not** being renamed to
`send()`/`wait_until_delivered()` — they were already the right,
Paho-familiar names and stay that way.

```python
# Before -- still works, now warns
from reliomq import ReliabilityConfig, ReliablePublisher
config = ReliabilityConfig(host="localhost", queue_path="pending.jsonl", data_topic="in")
publisher = ReliablePublisher(config)
publisher.start()
message_id = publisher.publish("t", {"x": 1}, event_id="my-id")
publisher.stop()

# After (0.3.0)
from reliomq import Sender, SenderConfig
config = SenderConfig(host="localhost", outbox_path="pending.jsonl", relay_topic="in")
sender = Sender(config)
sender.connect()
message_id = sender.publish("t", {"x": 1}, message_id="my-id")
sender.disconnect()
```

Everything else — the reliability guarantee, retry/reconnect/shutdown
behavior, and the on-disk Outbox format — is unchanged. See
[CHANGELOG.md](CHANGELOG.md) for the complete 0.3.0 release notes,
including the earlier 0.2.0 migration table (`event_id`→`message_id`).

## Tests

Run the deterministic suite from this directory:

```bash
python -m unittest discover -s tests -v
```

`test_protocol.py`, `test_outbox.py`, `test_ack.py`, `test_mqtt.py`, and
`test_config.py` each test one module in isolation: envelope/DeliveryAck
wire encoding, durable FIFO storage, the single-waiter ACK correlator, the
small Paho helper functions, and configuration validation, respectively.
`test_sender.py` and `test_relay.py` drive each component through a fake
Paho client to exercise success, broker outage, return-code failure,
publish-confirmation timeout, DeliveryAck timeout, restart, FIFO recovery,
wrong/late/malformed/duplicate ACKs, reconnect, relay failure/success,
Paho-style lifecycle aliases, and shutdown state transitions.
`test_pipeline.py` goes a level higher: it wires a real `Sender` to a real
`Relay` through two linked fake clients that relay `publish()` calls the
way a broker would, so the two components run on their own real background
threads and exchange genuine envelope/ACK traffic — catching integration
regressions that per-component unit tests with directly injected ACKs
cannot see. `test_logging.py` covers the observability story:
default-quiet behavior, `enable_logging()` idempotency and
non-duplication, `log_level=`/`debug=` wiring, and that INFO/DEBUG actually
carry the content documented above. Deprecated-alias compatibility (old
class/module/keyword/property names, each with its `DeprecationWarning`) is
tested alongside its own module in a dedicated test class per file rather
than a separate file. An optional Mosquitto integration test is skipped
when the broker executable is unavailable.

## License

MIT — see [LICENSE](LICENSE).

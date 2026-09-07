# reliomq

**RAM-first by default, end-to-end confirmed MQTT delivery for Python.**

`reliomq` sits on top of [`paho-mqtt`](https://pypi.org/project/paho-mqtt/)
and adds stable message IDs, retry, FIFO ordering, and application-level
delivery confirmation. The default `FastMode` keeps healthy-path messages in
bounded RAM and spills them when a safety trigger fires. Select
`mode=DurableMode()` when every accepted message must survive an immediate
crash or power loss, or `mode=GroupMode()` for disk-first batching. A `Sender`
owns exactly one delivery/persistence mode for its lifetime; use separate
senders when an application needs more than one mode.

```python
from reliomq import Sender, SenderConfig

sender = Sender(SenderConfig(
    host="localhost",
    outbox_path="pending",
    debug=True,  # see what reliomq is doing, with zero logging setup
))
sender.connect()
sender.publish("factory/machine1/data", {"temperature": 25.2})
sender.disconnect()
```

That call uses the `FastMode` default: the message enters a bounded RAM FIFO
before network delivery. Pending work spills to the **Outbox** on configured
safety triggers and clean shutdown, but RAM-only work may be lost in a sudden
process or power failure. Everything else — reconnects, retries, ordering,
and knowing when it's actually safe to forget the message — is handled for
you.

Requires **Python 3.11+** and **Paho MQTT 2.x**.

> **Upgrading to 0.6.1?** The default mode changed: `Sender(config)` now means
> `Sender(config, mode=FastMode())`. If you relied on the previous crash-safe
> default, pass `mode=DurableMode()` explicitly. Deprecated compatibility names
> such as `ReliablePublisher`/
> `PublisherConfig`/`ReliableMqttBridge`/`BridgeConfig`/`DurableMessageStore`/
> `Ack`, `queue_path=`, `envelope_topic=`/`data_topic=`, `ack_topic=`,
> `ack_timeout=`/`publish_timeout=`, and `event_id=` all still work — they
> now emit a `DeprecationWarning` pointing at their replacement. See
> [Migrating to 0.4.0](#migrating-to-040) and
> [Migrating to 0.3.0](#migrating-to-030) below, and
> [CHANGELOG.md](CHANGELOG.md), for the full picture.

## I want to...

| I want to... | Use... |
|---|---|
| Publish sensor/telemetry data reliably | `sender.publish()` — see [Publishing](#publishing) |
| Choose strict, group-synced, or RAM-first intake | Construct the `Sender` with `mode=DurableMode()`, `GroupMode()`, or `FastMode()` — see [Delivery and persistence modes](#delivery-and-persistence-modes) |
| Continue collecting data while MQTT/network is unavailable | Keep calling `sender.publish()` within the selected mode's local capacity/guarantee; the live FIFO and background retry handle delivery — see [Temporary network outage](#5-temporary-network-outage) |
| Know whether one specific message completed | `sender.wait_for_delivery(message_id, timeout=...)` |
| See whether messages are backing up | `sender.pending_count()` |
| Move reliably-delivered messages between a source and destination broker | `Relay` — see [Relay integration](#relay-integration) |
| Diagnose why a message is stuck | `debug=True` + trace its `message_id` in the logs — see [Debugging a stuck message](#4-debugging-a-stuck-message) |
| Tune how long reliomq waits for the MQTT broker's QoS 1 PUBACK | `mqtt_puback_timeout` |
| Tune how long reliomq waits for its own `DeliveryAck` from `Relay` | `delivery_ack_timeout` |
| Change how quickly failed/pending deliveries retry | `retry_interval` |

See [Timeouts, ACKs, and Blocking Behavior](#timeouts-acks-and-blocking-behavior)
and [What to Use and When](#what-to-use-and-when) below for the full picture.

## Why not just `qos=1`?

QoS 1 only proves the broker your process is directly connected to accepted
one publish. It proves nothing about:

- whether your process crashes or loses power before that publish happens;
- whether the broker forwards it any further (a relay, another hop);
- whether anything ever confirms, at the application level, that the message
  did its job.

`reliomq` closes the delivery-confirmation gap with an application-level
**DeliveryAck**, so "the network layer said OK" is never mistaken for "the
message is handled." Persistence is a separate choice: the `FastMode` default
is RAM-first, while explicit `DurableMode` closes the local crash/power-loss
gap with a durable Outbox before network eligibility.

## How reliomq works

```text
Application
    |
    |  sender.publish(topic, payload)  -- stable message_id assigned
    v
Sender                    <-- one fixed mode and one FIFO for this client
    |                         segmented Outbox: durable/group/spilled-fast
    |                         bounded RAM: FastMode records not yet spilled
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
from the live FIFO and advances persistent state when disk-backed
```

`Sender` never completes a message on network confirmation alone. It removes
a message from its live FIFO only after a matching `DeliveryAck` comes back
— which `Relay` only sends once *its own* publish to the real destination
topic was itself QoS 1 confirmed. Disk-backed completion advances an Outbox
cursor: immediately in `DurableMode` and for spilled `FastMode` records, or at
the configured checkpoint boundary in `GroupMode`. That is the delivery
design in one sentence: **two hops, two confirmations, one FIFO completion
boundary.**

### Two different kinds of "confirmed" — don't conflate them

| | What it proves | Who provides it |
|---|---|---|
| **MQTT PUBACK** (QoS 1) | The broker *this process is connected to* accepted one publish. | Paho / the broker, automatically. |
| **DeliveryAck** | A `Relay` (or your own code speaking the same protocol) actually forwarded the message to its real destination topic, broker-confirmed. | reliomq's own wire protocol, on the delivery-ack topic. |

A message is only completed after **both**. Until then it stays in the live
FIFO and keeps retrying through a broker outage, a relay that's down, or a
lost DeliveryAck. Restart recovery applies to records that reached the
Outbox: always for an accepted `DurableMode` publish, through the latest
completed data fsync in `GroupMode`, and after a successful spill in
`FastMode`. See [Delivery and persistence modes](#delivery-and-persistence-modes) and
[Persistence and restart recovery](#persistence-and-restart-recovery) for
the exact crash boundaries.

## Timeouts, ACKs, and Blocking Behavior

There are three separate waits in reliomq, and mixing them up is the single
easiest way to misread a log or misconfigure a deployment. Each is named
after exactly what it waits for, on purpose:

| Name | Waits for | Layer | Runs where? | Blocks user code? | What happens on timeout? |
|---|---|---|---|---|---|
| `mqtt_puback_timeout` | The MQTT QoS 1 PUBACK for **one** publish attempt | MQTT / Paho | reliomq's background delivery worker thread | No | That attempt is retained and retried after `retry_interval`; `FastMode` spills on failure while connected. A transport disconnect instead observes `disconnect_grace`. |
| `delivery_ack_timeout` | reliomq's own `DeliveryAck`, published by `Relay` | reliomq protocol | reliomq's background delivery worker thread | No | Same retry behavior; `FastMode` spills after the first connected DeliveryAck timeout. A transport disconnect instead observes `disconnect_grace`. |
| `wait_for_delivery(..., timeout=N)` | The **entire** delivery workflow finishing for one message -- both waits above, however many retries it takes | Application / caller | The thread that called `wait_for_delivery()` | **Yes** | Returns `False` to the caller. The message stays pending and the background worker keeps retrying it; this timeout does not change the client's mode. |

`mqtt_puback_timeout` and `delivery_ack_timeout` are both `SenderConfig`
fields that tune internal, background retry behavior; neither one is ever
awaited by your own code. `wait_for_delivery(timeout=...)`'s `timeout=` is
the only one of the three that belongs to the caller.

### Two threads, not one

```text
YOUR APPLICATION THREAD              RELIOMQ BACKGROUND (delivery worker thread)

message_id = sender.publish(...)       # default Sender uses FastMode
        |
        |  accepted into bounded RAM
        └───────────────────────►   live FIFO: message queued
                                     |
                                     |  worker wakes, picks the oldest message
                                     v
                                     MQTT publish attempt
                                     |
                                     |  wait up to mqtt_puback_timeout
                                     v
                                     MQTT PUBACK
                                     |    (timeout instead? -> retry after retry_interval)
                                     |  wait up to delivery_ack_timeout
                                     v
                                     DeliveryAck
                                     |    (timeout instead? -> retry after retry_interval)
                                     v
                                     message completed; disk cursor advances
                                     only when the record was disk-backed


sender.wait_for_delivery(
    message_id,
    timeout=30,
)
        |
        |  BLOCKS THIS THREAD HERE, up to `timeout` seconds
        |
        +-- delivery completes --------> returns True
        |
        +-- timeout expires ------------> returns False
                                           (the worker above keeps retrying;
                                            this timeout does not stop it)
```

`publish()` never performs a network wait. It enqueues into this sender's FIFO
according to the mode selected when the sender was constructed and wakes the worker. The
worker (one per `Sender`, named `reliomq-sender`) is the only thing that runs
an MQTT publish attempt or waits for either ACK; it runs for the lifetime of
the sender, independent of whether anything ever calls
`wait_for_delivery()`.

## Architecture overview

| Component | Responsibility | You touch it when... |
|---|---|---|
| `SenderConfig` | Validated transport settings for `Sender`: broker, topics, timeouts, Outbox path, and logging. | Constructing a sender. |
| `RelayConfig` | Same, for `Relay`: source + destination brokers. | Constructing a relay. |
| `Sender` | **Public API.** Owns one immutable mode and retries its FIFO until a `DeliveryAck` confirms each message; `FastMode` is the default. | This is what your application calls: `connect()`, `publish()`, `wait_for_delivery()`, `pending_count()`. |
| `Relay` | **Public API.** Relays from a source broker to a destination broker and only ACKs the source after the destination publish is confirmed. | Run as its own process/service between two brokers. Not needed if you only care about durable *delivery to a broker* rather than end-to-end confirmed forwarding. |
| `Outbox` | Segmented on-disk FIFO with a persistent head cursor. It contains persistent records, not RAM-only `FastMode` messages. Direct inspection/maintenance requires exclusive ownership of its path. | Rarely — mostly internal. |
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
| *(nothing)* | `Outbox` | reliomq-specific: the persistent journal underneath durable recovery. |
| *(nothing)* | `wait_for_delivery()` | reliomq-specific: blocks for a `DeliveryAck`, not a PUBACK. |
| *(nothing)* | `pending_count()` | reliomq-specific: size of this sender's live backlog. |

**The crucial difference:** Paho's `publish()` is primarily an MQTT
*transport* operation — it hands one message to the network and its result
tells you whether the broker accepted that one publish. reliomq's
`publish()` enters a managed, end-to-end-confirmed delivery workflow with
retry, FIFO ordering, and a stable ID. Its default `FastMode` uses bounded RAM
before network eligibility and may lose RAM-only work on sudden failure;
explicit `GroupMode` and `DurableMode` provide progressively stronger
persistence guarantees. Do not assume these behave identically;
see [Publishing](#publishing) below for exactly what `publish()`'s return
value does and doesn't promise.

**On the lifecycle merge:** raw Paho lets you `connect()` without ever
calling `loop_start()` (or vice versa in some patterns) — the connection and
the background network thread are separable. reliomq cannot honestly offer
that split: managed delivery *is* the background worker that watches the
live FIFO and persistent journal, and there is no useful state where a
connection exists but that worker isn't running. So on `Sender`,
`connect()`, `start()`, and `loop_start()` are three names for the exact
same one operation (same for `disconnect()`/`stop()`/`loop_stop()`) — call
whichever reads best in your code; calling more than one is a harmless
no-op. `Relay` manages *two* Paho clients (one per broker), so its
`connect()`/`start()` brings up both connections together — see
[Relay integration](#relay-integration).

## Features

- **RAM-first by default** — `Sender(config)` is equivalent to
  `Sender(config, mode=FastMode())`; healthy-path messages avoid persistent
  writes and spill on configured safety triggers.
- **One client, one mode** — choose `DurableMode`, `GroupMode`, or
  `FastMode` when constructing a sender. Create separate senders for streams
  with different policies; `publish()` has no mode override.
- **QoS 1 MQTT delivery**, always — the transport QoS cannot be lowered per
  call, regardless of durability mode.
- **Application-level end-to-end `DeliveryAck`**, correlated by a stable
  `message_id`, in addition to the MQTT PUBACK.
- **Automatic, unique message IDs** — generated for you, or you can supply
  your own; never regenerated on retry.
- **One FIFO per sender** — the oldest pending message is always attempted
  first. A `FastMode` spill selects the oldest RAM records first.
- **Automatic restart recovery** — a fresh process opens the same segmented
  Outbox and resumes every record that reached a durability boundary.
- **Automatic retry** on broker outage, network failure, publish errors,
  `mqtt_puback_timeout` expiry, and `delivery_ack_timeout` expiry — nothing
  is deleted on any of these.
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
- **Clean shutdown** — stopping interrupts an in-progress ACK wait, durably
  spills outstanding `FastMode` messages, and syncs/checkpoints `GroupMode`;
  an I/O failure is raised rather than silently reported as success.
- **Segmented storage** — append-only, length-and-CRC-framed segment records
  plus a persistent `(segment_id, byte_offset)` cursor avoid rewriting the
  remaining queue after every ACK. Completed closed segments are deleted only
  after recovery state is safe.
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
when `Sender` and `Relay` are used together and the message remains in the
live FIFO or recoverable Outbox. It favors *never silently losing a
persisted message* over *never duplicating one*. Across sudden process or
power loss, only `DurableMode` has that guarantee immediately when
`publish()` returns. `GroupMode` may lose records after its latest completed
data fsync, and a RAM-only `FastMode` message may be lost entirely.

A live message is completed **only** after a valid `DeliveryAck` carrying
its exact `message_id` arrives back on the configured delivery-ack topic.
For a persisted message, completion advances a logical Outbox cursor.
`DurableMode` checkpoints that progress after every ACK; `GroupMode` batches
it and can therefore replay recently ACKed messages after a crash. Broker acceptance alone is not proof that a final subscriber
processed the message — if you need that stronger boundary, use a persistent
MQTT subscription or extend the protocol with a consumer ACK of your own.

Duplicates remain possible: the destination publish can succeed and the
DeliveryAck can then be lost, in which case `Sender` correctly retries the
same stable `message_id`. **Consumers should store processed message IDs
and make handling idempotent** — see `examples/consumer_dedup.py`.

## Install

```bash
pip install reliomq
```

The only runtime dependency is `paho-mqtt>=2,<3`. There is no GPIO, Modbus,
or other hardware dependency of any kind (see
[examples/modbus_sensor.py](examples/modbus_sensor.py) for an example that
*optionally* uses `pymodbus`, installed separately).

## Getting started

The shortest useful shape — context manager, one publish, done:

```python
from reliomq import Sender, SenderConfig

config = SenderConfig(host="localhost", outbox_path="pending", log_level="INFO")

with Sender(config) as sender:          # __enter__ calls connect()
    sender.publish("factory/machine1/data", {"temperature": 25.2})
# Default FastMode: RAM-only work can be lost on sudden failure.
# __exit__ calls disconnect() and spills still-pending work on clean shutdown.
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

**`publish()`'s return means "reliomq accepted the message into this
sender's live delivery FIFO under its configured mode" — nothing more.** It
does *not* mean:

- the destination broker has already accepted it;
- a `DeliveryAck` has already come back;
- the message has already completed or left the live FIFO.

Required foreground persistence failures raise `OutboxError` (the deprecated
`StoreError` name still aliases it). Deferred GroupMode sync/checkpoint failures
are logged and retried; `stop()` raises if it cannot finish that work.
`FastMode` raises `FastQueueFullError` rather than silently
dropping an accepted or already-queued message when bounded RAM cannot admit
more work.

### Delivery and persistence modes

A `Sender` receives one mode at construction and keeps it for its entire
lifetime. `publish()` deliberately has no `mode=` or `durability=` argument,
and topics never select a policy automatically. If one application needs
different guarantees, create different senders with different MQTT client IDs
and different Outbox paths.

```python
from reliomq import DurableMode, FastMode, GroupMode, Sender

critical = Sender(critical_config, mode=DurableMode())
telemetry = Sender(telemetry_config, mode=GroupMode())
heartbeat = Sender(heartbeat_config, mode=FastMode())
```

`Sender(config)` is exactly equivalent to
`Sender(config, mode=FastMode())`. Pass `mode=DurableMode()` explicitly for
crash-safe acceptance before `publish()` returns.

| Property | `DurableMode` | `GroupMode` | `FastMode` |
|---|---|---|---|
| Mental model | Write now, fsync now | Write now, fsync later | RAM now, disk only if needed |
| Initial storage | Disk | Filesystem append | RAM |
| Message write | Every message | Every message | Only on spill |
| Message fsync | Every message | Batched | Spill only |
| ACK persistence | Every ACK | Batched | None while RAM-only; aggressive once disk-backed |
| Power-loss guarantee | Strongest after accepted publish | Only records covered by a completed fsync | RAM-only messages may be lost |
| Duplicate replay | Lowest practical sender-side window | Bounded by ACK checkpoint window | Depends on whether the message spilled |
| SD-card activity | Highest | Much lower | Lowest on a healthy route |
| Typical use | Critical events | Important/high-rate telemetry | Live or replaceable values |

In plain language:

- **DurableMode:** “I cannot lose an accepted message.”
- **GroupMode:** “I still want disk as the normal path, but I do not want to
  fsync every message.”
- **FastMode:** “I do not want disk in the healthy path; persist only when
  recovery is needed.”

#### `DurableMode`

`DurableMode()` appends, flushes, and fsyncs every complete envelope before
`publish()` returns or network delivery may begin. A successful acceptance is
therefore recoverable after sudden power loss, subject to normal filesystem
and hardware guarantees. After every valid `DeliveryAck`, it persists and
fsyncs the head cursor before forgetting the message. This minimizes the
sender-side duplicate window but produces the most storage synchronization.

It remains at-least-once, not exactly-once: if the destination processes a
message and power fails before the returned DeliveryAck cursor is safely
checkpointed, the same stable `message_id` can be delivered again.

#### `GroupMode`

Every `GroupMode` publish appends and flushes its complete envelope to the
active segment immediately. It does **not** normally fsync that message
individually. A data fsync covers the accumulated appends when any of these
triggers fires:

- `sync_messages=20` unsynced messages;
- `sync_interval=0.25` seconds since the last completed data fsync;
- `sync_bytes=65536` unsynced framed bytes;
- segment rotation; or
- clean shutdown.

Only records covered by a completed data fsync are guaranteed recoverable
after power loss. A record that has merely been appended may or may not
survive, depending on the OS, filesystem, and storage device.

ACK progress is batched separately. The RAM head advances after a valid
`DeliveryAck`, while the persistent cursor is checkpointed when either
`ack_checkpoint_messages=50` ACKs accumulate,
`ack_checkpoint_interval=1.0` second elapses, a closed segment is fully consumed, or
clean shutdown begins. Consequently GroupMode has two intentional crash
windows:

1. Appended records after the latest data fsync may be lost.
2. ACKed records after the latest cursor checkpoint may be replayed.

A checkpoint first syncs any deferred data needed for safe recovery, so ACK
checkpoint triggers can also end the unsynced-message window. For a count-only
workload with `sync_messages=20`, 100 appends need five data fsyncs, provided
no timer, byte limit, rotation, or ACK checkpoint fires first. Metadata syncs
are additional. At low message rates the default 0.25-second timer will often
fire before 20 messages accumulate; batch size alone does not determine sync
frequency. Timers also run while delivery is blocked or before `start()`.
These are trigger intervals, not hard real-time deadlines: scheduling delays
or storage errors can extend both windows. Deferred sync/checkpoint failures
are logged and retried without undoing an accepted append or DeliveryAck;
clean shutdown raises `OutboxError` if the required work still cannot finish.

```python
mode = GroupMode(
    sync_messages=20,
    sync_interval=0.25,
    sync_bytes=64 * 1024,
    ack_checkpoint_messages=50,
    ack_checkpoint_interval=1.0,
)
```

#### `FastMode`

`FastMode` assigns the stable ID and stores the complete envelope in bounded
RAM first. On a healthy route, MQTT delivery plus its matching `DeliveryAck`
removes the record without any persistent message write.

Default hard limits are `ram_max_messages=10000` and
`ram_max_bytes=33554432` (32 MiB). Bytes mean the canonical serialized envelope,
including topic and stable message ID. These bounds apply to RAM-only queued
payloads, not total process memory: Python object/index overhead, transient
encoding/spill buffers, and the current in-flight snapshot are additional.
Disk-backed payloads are read on demand rather than retained for the entire
backlog. Spill begins at
`high_watermark=0.75` of either limit, when the oldest RAM message reaches
`max_ram_age=5.0` seconds, after a continuous MQTT transport disconnect of
`disconnect_grace=3.0` seconds, on the first PUBACK failure or DeliveryAck
timeout while the connection remains ready, or when clean shutdown
begins. A brief disconnect that recovers within the grace period does not
spill solely because of that disconnect.

Spills select the oldest RAM records in batches bounded by
`spill_batch_messages=1000` and `spill_batch_bytes=4194304` (4 MiB of serialized
envelopes). A record larger than the batch byte target spills alone, provided
it fits `ram_max_bytes`. Each batch uses one sequential write and data fsync
per touched segment; crossing a rotation boundary requires additional file
and metadata synchronization. The RAM copies remain authoritative until the
entire batch's required fsyncs succeed; a failed spill
cannot silently discard them. Once disk-backed, records recover and retry from
the segmented Outbox under the same IDs, and their ACK cursor is persisted
aggressively. RAM-only records can be lost after sudden power failure.

See [durable_mode.py](examples/durable_mode.py),
[group_mode.py](examples/group_mode.py), [fast_mode.py](examples/fast_mode.py),
and [multiple_clients.py](examples/multiple_clients.py) for ready-to-copy
programs.

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

# 5. Publish while the broker/network is offline -- DurableMode still
#    succeeds locally and retries when connectivity returns. publish() does
#    not fail merely because the network is down.
message_id = sender.publish("factory/sensor-01", {"temperature": 25.2})

# 6. Restart with pending persisted messages -- construct Sender again with
#    the same outbox_path; recovery preserves order and message_id. A GroupMode
#    suffix after its latest fsync and RAM-only FastMode work are outside this promise.
sender = Sender(
    SenderConfig(host="localhost", outbox_path="pending"),
    mode=DurableMode(),
)
sender.connect()

# 7. Inspect this sender's whole live backlog without blocking
#    (see sensor_loop.py for
#    the recommended pattern: warn past a threshold, never poll-wait per reading)
if sender.pending_count() > 50:
    logging.warning("delivery is falling behind: %s pending", sender.pending_count())
```

Do not call `wait_for_delivery()` after every `publish()` in a tight loop
(e.g. a sensor reading every few seconds) — that serializes every reading
behind a network round trip. Use `pending_count()` to monitor the entire
live FIFO, including RAM-only `FastMode` messages, without blocking instead; see
`examples/sensor_loop.py`.

## Debugging

Swap `log_level="INFO"` for `debug=True` (equivalent to `log_level="DEBUG"`)
to see every internal step: publish attempts, PUBACK confirmation, waiting
for the DeliveryAck, why a retry happened, and so on:

```python
config = SenderConfig(host="localhost", outbox_path="pending", debug=True)
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
produces no DeliveryAck; the sender still owns the pending message and
retains it for retry (spilling an attempted RAM-only `FastMode` message when
its retry trigger is reached).
Deploy only one ordinary relay subscriber per route unless duplicate
forwarding is intended.

## Common Usage Patterns

### 1. Fire-and-continue telemetry

```python
message_id = sender.publish("factory/sensor-01", payload)
```

Use for: sensors, telemetry, periodic readings, continuous data collection
-- anything where the application should keep producing data rather than
pause for each one. The full sensor-loop example selects `DurableMode`
explicitly, so reliomq stores and retries each message in the background; the
caller never blocks on the network. Use the `FastMode` default only if its
documented RAM-only crash-loss window is acceptable. **Do not** call
`wait_for_delivery()` after every reading in a loop like this -- it would
serialize every reading behind a network round trip. The application should
normally keep collecting data at its own pace.

Full runnable version, including graceful shutdown and a backlog warning:
[examples/sensor_loop.py](examples/sensor_loop.py):

```python
sender = Sender(config, mode=DurableMode())
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

A realistic version of this same pattern reading real hardware over Modbus
TCP (read-only -- it only ever calls `read_holding_registers()`, never a
write) is in [examples/modbus_sensor.py](examples/modbus_sensor.py); it
requires the optional `pymodbus` package (`pip install pymodbus`) --
reliomq itself has no Modbus or hardware dependency.

### 2. Publish and require confirmation

```python
message_id = sender.publish("factory/critical-event", payload)

if not sender.wait_for_delivery(message_id, timeout=30):
    handle_still_pending(message_id)
```

Use for: critical events, workflows where the next action genuinely depends
on successful delivery, or any case where synchronous confirmation is
required before proceeding. **`wait_for_delivery()` blocks the calling
thread** for up to `timeout` seconds; the message stays in the live FIFO and
reliomq's background worker keeps retrying it independent of whether/how this
call returns. Calling `wait_for_delivery()` does not change the sender's mode
or strengthen that mode's crash guarantee.

### 3. Operational monitoring

```python
pending = sender.pending_count()
```

Use for health checks, metrics, dashboards, and alerting on a growing
backlog. For `FastMode`, the count includes RAM-only messages and is therefore
not necessarily `sender.outbox.size()`.
`pending_count()` never blocks. See
[`sender.pending_count()`](#senderpending_count) above for what a rising
count can indicate.

### 4. Debugging a stuck message

Turn on `debug=True` (equivalent to `log_level="DEBUG"`), then trace one
`message_id` through the sequence it should follow:

```
durable/group accepted     (INFO  "Message stored in Outbox ... mode=...")
fast accepted              (INFO  "Message accepted by FastMode ... storage=...")
optional fast spill        (INFO  "FastMode spill ...")
MQTT publish attempt       (DEBUG "Publish attempt ... mqtt_puback_timeout=...")
MQTT PUBACK                (DEBUG "MQTT PUBACK received")
waiting for DeliveryAck    (DEBUG "Waiting for DeliveryAck ... delivery_ack_timeout=...")
DeliveryAck received       (INFO  "DeliveryAck received")
completed from live FIFO   (INFO  "Message completed")
```

Wherever the sequence stops tells you what to look at:

- **Stops after an accepted/stored line, no publish attempt appears:** the sender
  isn't connected/ready yet -- check `sender.is_connected()` and for
  `MQTT connection` WARNING lines.
- **Stops after "MQTT publish attempt", no PUBACK:** the source broker
  isn't reachable, or is rejecting the publish -- look for
  `MQTT PUBACK not confirmed within mqtt_puback_timeout`; consider raising
  `mqtt_puback_timeout` only if this is a genuine latency issue, not an
  outage.
- **Stops after "MQTT PUBACK received", no DeliveryAck:** the message
  reached the source broker, but nothing sent a `DeliveryAck` back -- is a
  `Relay` actually running and subscribed to this `relay_topic`? Is *its*
  destination publish succeeding? Look for
  `DeliveryAck not confirmed within delivery_ack_timeout` and, on the
  relay's own logs, `Relay forwarded` / `DeliveryAck sent`.
- **Repeats "Delivery retry scheduled" indefinitely:** the attempted message
  has been retained for retry (`FastMode` spills when its retry trigger is
  reached), but
  something downstream is never completing -- this is exactly what
  `pending_count()` would show growing.

### 5. Temporary network outage

When the source broker, the destination broker, or `Relay` itself becomes
unavailable — or a `DeliveryAck` simply stops coming back — the
application can keep calling `sender.publish(...)` normally within the
selected mode's limits. `DurableMode` fsyncs locally. `GroupMode` appends
locally and follows its bounded fsync window. `FastMode` accepts into bounded
RAM, waits through `disconnect_grace`, and spills if the outage persists. The
FIFO drains oldest-first once the route returns. Required foreground
persistence failures raise `OutboxError`; deferred GroupMode sync/checkpoint
failures are logged and retried. FastMode capacity exhaustion raises
`FastQueueFullError`.

### 6. Graceful shutdown

```python
sender.loop_stop()
sender.disconnect()
```

What happens: intake stops immediately; if a delivery attempt is
in-flight, `disconnect()`/`stop()` interrupts its ACK wait (without ever
reporting that message as delivered) and waits for the worker thread to
finish. It then durably spills all outstanding `FastMode` messages, or fsyncs
outstanding data and ACK progress for `GroupMode`, before returning. If that
persistence work fails, shutdown raises `OutboxError`; it does not silently
claim a clean durability boundary. After a successful clean stop, a new
`Sender` with the same `outbox_path` resumes every pending message in order
under the same `message_id`.

### 7. When Relay is required

`Relay` is required whenever you need reliomq's actual end-to-end
guarantee -- a `DeliveryAck` only exists because something implementing the
protocol produced one:

```text
Sender
    |
    v
source broker
    |
    v
Relay
    |
    v
destination broker
    |
    v
DeliveryAck
    |
    v
Sender
```

A plain `mosquitto_sub` (or any plain MQTT subscriber) on the destination
topic does **not** generate a `DeliveryAck` -- it just receives the
message. Without a `Relay` (or your own code speaking the same wire
protocol) subscribed to the `relay_topic` and publishing `DeliveryAck`s
back, a `Sender`'s messages remain pending and retry *forever*, since
nothing will ever confirm them. Their crash recovery still follows the
owning sender's client-level mode. There is no "direct mode" that skips
`Relay` while keeping end-to-end confirmation -- if you only need durable,
retried delivery *to one broker* (QoS 1 plus persistence, without a second
hop or an application-level ACK), that is a materially weaker guarantee
than what this library is for; reliomq does not currently offer that as a
separate mode.

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

The Outbox is a segmented append-only FIFO suitable for edge devices. New
storage is a directory containing numbered data files and atomic checkpoint
state, conceptually:

```text
pending/
    segment-00000000000000000001.dat
    segment-00000000000000000002.dat
    format.json
    checkpoint.json
```

Each segment record is a canonical envelope framed with a length and CRC.
The highest-numbered segment is active and appendable; closed segments are
immutable. A segment rotates before its configured maximum of 8 MiB or 10,000
records would be exceeded (a single oversized record is allowed so the queue
can always make progress). Rotation syncs the old segment first.

Acknowledging one message does not delete bytes or copy the surviving queue.
It moves a logical head identified by `(segment_id, byte_offset)`. The
checkpoint is atomic, versioned JSON containing `version`, `generation`,
`segment_id`, and `byte_offset`. Startup validates the remaining segment frames
and resumes the FIFO at the saved record boundary. Validation scans records;
recovery is not a constant-time seek-only operation. The live index keeps
record locations and IDs, with payloads read from disk on demand. Old bytes
remain until their closed segment is fully consumed.

Opening an existing queue syncs any recovered active-segment records before
new mode counters start. It also retries directory metadata barriers through
the storage path's ancestors, covering an interrupted installation or rotation.
Directory fsync is best-effort on platforms that do not support it (notably
Windows); power-loss guarantees depend on the filesystem and storage device
honoring the available file and metadata durability operations.

Cleanup follows a strict order: persist the completion cursor, fsync the
required checkpoint state, then delete the fully consumed closed segment.
The active segment and partially consumed segments are never rewritten.
`outbox.compact()` now means checkpoint plus safe completed-segment cleanup;
it does not compact live payload into a replacement file.

An active segment's incomplete final record is repaired to its last valid
record boundary. A complete record with a bad checksum, or corruption in a
closed immutable segment, fails conservatively instead of skipping uncertain
messages. Outbox I/O errors are raised instead of being mistaken for an empty
queue; missing or malformed checkpoint metadata replays surviving records
conservatively. GroupMode's automatic sync/checkpoint retries are described above.

### Existing Outbox migration

An existing regular-file Outbox at the configured path remains recoverable.
On first open, reliomq replays the legacy envelope-only JSONL format and the
version-1 enqueue/ACK journal format, then atomically builds segmented storage
in a sibling `<outbox_path>.segments` directory. The original file is retained
as a migration source; the sidecar is authoritative on later opens. This also
makes interrupted migration retryable without silently discarding queued
messages. `outbox.path` remains the configured path and `outbox.storage_path`
reports the actual segmented directory.

Once segmented storage exists, older reliomq versions cannot consume new
records. Drain the queue and retain the original file or a backup before a
software downgrade.

Exactly one live `Outbox` owner may operate on an `outbox_path`, including
within a single process. An instance is thread-safe, but separate instances
do not share locks or live state, and there is no cross-process file lock.
RAM-only `FastMode` messages are outside the persistent Outbox until spill;
after a failed spill they may also have a partial disk copy, while the sender
retains RAM ownership until the complete batch is confirmed durable.

Once an `Outbox` belongs to a `Sender`, treat `sender.outbox` as read-only
while that sender is alive. For maintenance, stop the sender and relinquish
its old instance before opening the same path elsewhere; construct a fresh
`Sender` after direct Outbox changes. `Outbox.load()` deliberately materializes
the complete pending backlog for callers that explicitly request it.

## Connection, retry, and shutdown behavior

Both components use Paho's asynchronous network loop and reconnect backoff.
`Sender` does not send until the broker connection and DeliveryAck
subscription are ready. A disconnect interrupts the current ACK wait
without removing its record. Reconnect wakes recovery immediately; other
failures retry after `retry_interval`.

`Sender.disconnect()`/`stop()` interrupts an ACK wait and joins the worker.
It then spills outstanding FastMode messages or syncs GroupMode data and ACK
progress before a successful return, so the cleanly stopped queue remains for
the next process. A stopped `Sender` can be started again and resumes that
same live FIFO. `Relay.disconnect()`/`stop()` stops accepting new input, lets the
bounded in-flight publish/ACK sequence finish, and leaves queued items
unacknowledged so their senders recover them. Lifecycle calls are
idempotent.

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
| `outbox_path` | required | Persistent Outbox path; new queues use a directory and legacy files migrate to a sibling `.segments` directory |
| `port` | `1883` | Broker port |
| `client_id` | auto-generated | MQTT client ID |
| `relay_topic` | `reliomq/messages` | Topic the sender sends its envelope on (not the application topic) |
| `delivery_ack_topic` | `reliomq/acks` | Topic the sender listens on for DeliveryAcks |
| `qos` | `1` | Fixed at 1 |
| `delivery_ack_timeout` | `3.0`s | Background wait for reliomq's own `DeliveryAck` (see below) |
| `mqtt_puback_timeout` | `2.0`s | Background wait for the MQTT QoS 1 PUBACK (see below) |
| `retry_interval` | `10.0`s | Delay between retries after a failure (see below) |
| `keepalive` | `60`s | MQTT keepalive |
| `reconnect_min_delay` / `reconnect_max_delay` | `1.0`s / `60.0`s | Paho reconnect backoff range |
| `log_level` | `None` | `"DEBUG"`/`"INFO"`/... or a `logging` level int; `None` leaves logging exactly as-is (see [Logging](#logging)) |
| `debug` | `False` | Shorthand for `log_level="DEBUG"`; conflicts if combined with a different explicit `log_level` |

Mode configuration belongs to the mode object passed to `Sender`, not to
`SenderConfig`:

| `GroupMode` field | Default | Meaning |
|---|---|---|
| `sync_messages` | `20` | Unsynced message count that triggers one data fsync |
| `sync_interval` | `0.25`s | Data sync trigger interval since the last completed fsync while unsynced data exists; scheduling/I/O delays may extend it |
| `sync_bytes` | `65536` | Unsynced framed bytes that trigger one data fsync |
| `ack_checkpoint_messages` | `50` | ACKs accumulated in RAM before cursor checkpoint |
| `ack_checkpoint_interval` | `1.0`s | Pending ACK cursor checkpoint trigger interval; scheduling/I/O delays may extend it |

| `FastMode` field | Default | Meaning |
|---|---|---|
| `ram_max_messages` | `10000` | Hard bound on RAM-owned message count |
| `ram_max_bytes` | `33554432` | Hard bound on RAM-only queued canonical envelope bytes (32 MiB); not total process memory |
| `high_watermark` | `0.75` | Fraction of either RAM bound that begins spill |
| `max_ram_age` | `5.0`s | Oldest-message age that begins spill |
| `disconnect_grace` | `3.0`s | Continuous MQTT transport-disconnected time allowed before spill; a pending/rejected ACK subscription is covered by `max_ram_age` |
| `spill_batch_messages` | `1000` | Maximum messages selected per normal spill batch |
| `spill_batch_bytes` | `4194304` | Serialized envelope byte target per spill batch (4 MiB); one larger record may spill alone |

Mode objects are frozen after construction. Invalid mode settings raise
`ValueError`: counts and byte limits must be positive integers, durations
must be finite and positive (except `disconnect_grace`, which may be zero),
and `high_watermark` must be finite in `(0, 1]`. Booleans, NaN, and infinity
are rejected. `DurableMode()` has no configuration fields.

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

### Timeout and retry settings in detail

| Setting | Default | Unit | Controls | Layer | Where it runs | Blocks caller? | Normal usage |
|---|---|---|---|---|---|---|---|
| `SenderConfig.mqtt_puback_timeout` | `2.0` | seconds | How long one publish attempt waits for the broker's QoS 1 PUBACK | MQTT / Paho | Background delivery worker | No | Leave at the default. Raise it if logs show recurring `MQTT PUBACK not confirmed` retries against a broker/network with genuinely higher latency than 2s. |
| `SenderConfig.delivery_ack_timeout` | `3.0` | seconds | How long one publish attempt waits for reliomq's own `DeliveryAck` after the PUBACK | reliomq protocol | Background delivery worker | No | Leave at the default. Raise it if `Relay`-to-destination latency is high and logs show `DeliveryAck not confirmed` retries that keep just missing the window. |
| `RelayConfig.destination_publish_timeout` | `2.0` | seconds | How long the relay waits for the destination broker's PUBACK | MQTT / Paho | `Relay`'s forwarding worker | No | Leave at the default; same tuning logic as `mqtt_puback_timeout`, for the destination broker specifically. |
| `RelayConfig.source_ack_publish_timeout` | `0.5` | seconds | How long the relay waits for its own `DeliveryAck` publish to be PUBACK'd on the source broker | MQTT / Paho | `Relay`'s forwarding worker | No | Leave at the default. |
| `retry_interval` (both configs) | `10.0` | seconds | Delay before retrying after any failure (PUBACK timeout, DeliveryAck timeout, disconnect) | reliomq | Background worker | No | Leave at the default. Increase for long expected outages or to reduce retry traffic; decrease if brokers/network can absorb faster retries and you want quicker recovery. |
| `Sender.wait_for_delivery(timeout=N)` | `None` (wait forever) | seconds | How long **your thread** blocks waiting for one message's entire workflow | Application / caller | Your calling thread | **Yes** | Set this per call based on how long that specific workflow can afford to wait -- there is no library-wide default to tune. |

None of the background timeouts need to change for normal operation; they
exist to be tuned only when logs show a specific, recurring problem (see
[Debugging a stuck message](#4-debugging-a-stuck-message)).

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
- a DurableMode/GroupMode message stored in the Outbox, or a FastMode message
  accepted into RAM, with the client mode and current pending count;
- FastMode spill selection and successful disk backing;
- the DeliveryAck received, and the message completed once delivery is
  confirmed;
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
- the publish attempt (tagged with the exact `mqtt_puback_timeout` in
  effect) and `MQTT PUBACK received`, logged as its own distinct line from
  the DeliveryAck below;
- `Waiting for DeliveryAck` (tagged with the exact `delivery_ack_timeout` in
  effect), and DeliveryAck matching -- including *why* a
  stale/late/wrong-ID/malformed one was ignored;
- Outbox segment append/rotation, cursor checkpoint, duplicate-ID, and cleanup
  decisions;
- MQTT client creation and subscription bookkeeping, including Paho's own
  `mid` (packet identifier) where relevant — deliberately not renamed to
  `message_id`, since it is a different concept at a different layer.

Every timeout-governed DEBUG line names its own config field explicitly
(`mqtt_puback_timeout=...` or `delivery_ack_timeout=...`) rather than a bare
"timeout" — see [Timeouts, ACKs, and Blocking Behavior](#timeouts-acks-and-blocking-behavior).
Payloads and credentials are never logged, at any level — only
`message_id`s, topics, and counts. That's deliberate: DEBUG should never
require an opt-in beyond the level itself to be safe to turn on in
production.

**WARNING** — recoverable trouble that's expected during normal outage
handling: disconnects, rejected subscriptions, late/malformed/wrong-ID
DeliveryAcks ignored, forward failures, a full relay queue, and the reason
a delivery attempt is being retried -- always phrased as either
`MQTT PUBACK not confirmed within mqtt_puback_timeout` or
`DeliveryAck not confirmed within delivery_ack_timeout`, never an
unqualified "ack timeout". The message remains pending for retry; FastMode
spills it before persistent retry when the configured trigger is reached.

**ERROR** (some via `logger.exception()`, with a traceback) — things that
should not happen: the worker failing to stop promptly on shutdown, a
DeliveryAck matching a message that turned out not to be the logical oldest,
or an unexpected exception in the delivery/forward loop.

## What to Use and When

A decision table for every supported public tool. "Background?" means it
runs (or configures) work on reliomq's own worker thread, independent of
your code; "Blocks caller?" means calling it can pause the thread that
called it.

| Tool | What it is | Background? | Blocks caller? |
|---|---|---|---|
| `Sender` / `SenderConfig` | The main entry point: one client-level mode, FIFO retry, and end-to-end-acknowledged publishing; `FastMode` by default | Owns one background worker | No (construction/config only) |
| `Relay` / `RelayConfig` | Optional end-to-end forwarder between two brokers | Owns one background worker | No (construction/config only) |
| `Outbox` | Segmented persistent queue and head cursor underneath `Sender`; excludes RAM-only FastMode messages | N/A (a data store, not a process) | Briefly, for its own file I/O locking -- negligible |
| `DeliveryAck` | The wire-protocol shape of reliomq's own end-to-end acknowledgement | N/A (a data class) | N/A |
| `sender.publish()` | Enqueue one message under the sender's fixed mode | Triggers background work | No network wait; local work depends on mode |
| `sender.wait_for_delivery()` | Block until one message (or the whole live FIFO) finishes | No -- it only *observes* background work | **Yes** |
| `sender.pending_count()` | Read this sender's live FIFO depth | No | No |
| `sender.connect()` / `.start()` / `.loop_start()` | Start the MQTT connection + background worker (one operation, three names) | Starts background work | Briefly, for connection setup |
| `sender.disconnect()` / `.stop()` / `.loop_stop()` | Stop cleanly, join the worker, and finish the mode's required spill/sync/checkpoint | Stops background work | Yes, for worker shutdown and local persistence |
| `mqtt_puback_timeout` | Config: MQTT PUBACK wait per attempt | Governs background work | No |
| `delivery_ack_timeout` | Config: DeliveryAck wait per attempt | Governs background work | No |
| `retry_interval` | Config: delay between retries | Governs background work | No |
| `log_level="INFO"` | Config: lifecycle narration | N/A (logging config) | No |
| `log_level="DEBUG"` | Config: full diagnostic detail, per `message_id` | N/A (logging config) | No |

### `Sender`

**What it does:** maintains one FIFO under one immutable client-level mode and
retries messages until a `DeliveryAck` confirms them. The default
`DurableMode` stores before network delivery and survives restart; the other
modes opt into narrower crash guarantees.

**Use when:**
- your application needs reliable publishing through a temporary
  MQTT/network outage;
- you need a strict durable, grouped-sync, or RAM-first policy for a stream;
- you want automatic retry without writing that logic yourself.

**Do not use when:**
- plain `paho-mqtt` is already enough for your use case (no durability or
  end-to-end confirmation needed);
- losing a message during an outage is genuinely acceptable;
- you don't need reliomq's delivery workflow at all -- a raw `Client` is
  simpler.

**Background:** owns one delivery-worker thread for its whole lifetime
(started by `connect()`/`start()`/`loop_start()`).

**Example:** see [Getting started](#getting-started).

### `sender.wait_for_delivery()`

**What it does:** waits for one message to complete reliomq's full delivery
workflow (PUBACK **and** DeliveryAck).

**Use when:**
- the next step in your application genuinely depends on confirmed
  delivery;
- a critical workflow requires synchronous confirmation before continuing;
- the caller intentionally wants to wait for exactly one message.

**Do not use when:**
- continuously reading sensors or publishing telemetry;
- inside a high-frequency loop -- blocking on every message serializes
  your whole data-collection rate behind network round trips;
- you only want to *monitor* backlog, not block on it (use
  `pending_count()` instead).

**Background:** No -- reliomq's own background delivery continues
independently of this call either way.

**Blocks calling thread:** **YES.**

**Example:**

```python
message_id = sender.publish("factory/machine1/data", payload)
if not sender.wait_for_delivery(message_id, timeout=30):
    handle_still_pending(message_id)
```

### `sender.pending_count()`

**What it does:** returns the current number of messages still in this
sender's live FIFO awaiting delivery, including both RAM-only and disk-backed
FastMode state.

**Use when:**
- checking operational health;
- exposing a backlog metric or building a dashboard/health endpoint;
- alerting on a growing backlog;
- troubleshooting why data doesn't seem to be clearing.

An increasing count can indicate: the source broker is unavailable, `Relay`
is unavailable, the destination broker is unavailable, `DeliveryAck`s
aren't returning, or some other connectivity/retry problem — pair it with
DEBUG logging to find out which.

**Background:** No.

**Blocks calling thread:** No (besides negligible internal
synchronization).

**Example:**

```python
pending = sender.pending_count()
if pending > 50:
    logging.warning("delivery backlog: %s messages pending", pending)
```

### `mqtt_puback_timeout`

**Use for:** controlling how long reliomq waits for the MQTT QoS 1 PUBACK
during one delivery attempt.

**Normally:** leave the default (`2.0`s).

**Change it when:** the broker/network is unusually slow, logs show
recurring `MQTT PUBACK not confirmed` retries, or your latency
characteristics justify a larger or smaller value.

**INTERNAL / BACKGROUND. DOES NOT BLOCK USER CODE.**

### `delivery_ack_timeout`

**Use for:** controlling how long reliomq waits for its own `DeliveryAck`
from `Relay`.

**Normally:** leave the default (`3.0`s).

**Change it when:** `Relay`-to-destination latency is high, `DeliveryAck`s
frequently arrive just after the current timeout, or logs show repeated
`DeliveryAck not confirmed` retry behavior.

**INTERNAL / BACKGROUND. DOES NOT BLOCK USER CODE.**

### `retry_interval`

**Use for:** controlling how aggressively reliomq retries failed/pending
deliveries.

**Normally:** leave the default (`10.0`s).

**Increase it when:** outages may last a long time, you want lower retry
traffic, or you want to reduce log/network churn.

**Decrease it when:** fast recovery matters and your brokers/network can
safely absorb more frequent retry attempts.

## Public API

Every supported public class, method, property, and exception, with where
to see it used.

### `Sender` / `SenderConfig`

The main entry point. See [Getting started](#getting-started) and
[Publishing](#publishing) above for full examples of everything below.

- **`Sender(config, *, mode=None, client_factory=None, outbox=None)`** —
  construct a sender. `mode=None` selects `FastMode()`; otherwise pass one
  `DurableMode`, `GroupMode`, or `FastMode` instance. The normalized mode is
  exposed read-only as `sender.mode`. Raises `TypeError` for another value or
  if `config` isn't a `SenderConfig`. Reads `config.log_level` and calls
  `enable_logging()` if set. Side effect: opens or creates the segmented
  Outbox for `config.outbox_path` immediately.
- **`sender.connect()`** / **`sender.start()`** / **`sender.loop_start()`**
  — three names for one operation: start the MQTT connection and the
  background delivery worker. Idempotent; safe to call more than once.
  Returns `self`.
  ```python
  sender.connect()
  ```
- **`sender.disconnect()`** / **`sender.stop()`** / **`sender.loop_stop()`**
  — three names for one operation: stop the worker and complete the selected
  mode's required spill, data sync, and ACK checkpoint. Idempotent; raises
  `OutboxError` if the persistence boundary cannot be completed.
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
- **`sender.publish(topic, payload, *, message_id=None)`** — enqueue a message
  in this sender's FIFO and return its stable `str` `message_id`. It is safe
  before `connect()` and deliberately accepts no per-publish mode. Raises
  `ValueError` if a `message_id` is reused for different content,
  `OutboxError` for required persistence failures, and
  `FastQueueFullError` when FastMode's bounded RAM cannot accept more work. See
  [Publishing](#publishing) for the full set of examples and exactly what
  the return value does/doesn't promise.
- **`sender.wait_for_delivery(message_id=None, timeout=None)`** — `bool`.
  Blocks for one message (or the whole live FIFO if `message_id` is omitted).
  reliomq-specific; no Paho equivalent.
  ```python
  delivered = sender.wait_for_delivery(message_id, timeout=10.0)
  ```
- **`sender.pending_count()`** — `int`. Current live backlog for this sender;
  in FastMode this can exceed `sender.outbox.size()` while messages remain
  RAM-only. reliomq-specific.
  ```python
  if sender.pending_count() > 50:
      logging.warning("falling behind")
  ```
- **`sender.outbox`** — the `Outbox` instance backing this sender. Public and
  safe to inspect (e.g. `sender.outbox.peek_oldest()`), but in FastMode it
  exposes only the disk-backed portion. Do not mutate it while the owning
  `Sender` is alive; direct changes bypass the live FIFO snapshot.
- **`with Sender(config) as sender:`** — context manager; `__enter__` calls
  `connect()`, `__exit__` calls `disconnect()`.
- **`SenderConfig(host, outbox_path, ...)`** — see
  [Configuration reference](#configuration-reference) for every field.
  Raises `ConfigError` on any invalid value, immediately at construction.
- **`DurableMode()`** — explicit strict mode: per-message data fsync and aggressive
  ACK cursor checkpoint.
- **`GroupMode(...)`** — immediate disk append with batched data fsync and
  separately batched ACK checkpoints.
- **`FastMode(...)`** — the default; bounded RAM-first intake with batched
  spill to the Outbox on configured triggers.

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
  (their senders still hold the pending message under the owning client's
  mode). Idempotent.
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

The segmented persistent queue underneath `Sender`. Deliberately not dressed
up as an MQTT concept — this is what gives persisted messages their
restart recovery, and plain MQTT has nothing like it. Direct use in a
maintenance/inspection script requires exclusive ownership of the path; see
[Persistence and restart recovery](#persistence-and-restart-recovery).

```python
from reliomq import Outbox

outbox = Outbox("pending")
print(outbox.size(), "messages pending")
for envelope in outbox.load():
    print(envelope.message_id, envelope.topic)
```

- **`Outbox(path, logger=None, *, segment_max_bytes=8388608, segment_max_records=10000)`**
  — opens or creates segmented storage. An existing legacy regular file is
  migrated to authoritative sibling `.segments` storage. Logs its pending
  count at INFO immediately.
- **`outbox.path`** / **`outbox.storage_path`** — configured path and actual
  segmented-storage directory, respectively.
- **`outbox.append(envelope, *, sync=True)`** — `bool`; appends an enqueue
  record. The default durably stores it before returning; `sync=False`
  deliberately defers fsync. `False` if that `message_id` is already pending.
- **`outbox.append_many(envelopes, *, sync=True)`** — append a batch with one
  sequential write per touched segment; used by FastMode spill. Returns an
  `AppendResult` with `appended_count`, `bytes_written`, and `rotated`.
  With `sync=True`, all touched segments are fsync-confirmed before return.
- **`outbox.sync()`** — force deferred segment writes to stable storage.
- **`outbox.peek_oldest()`** — `MessageEnvelope | None`; the current FIFO
  head, without removing it.
- **`outbox.remove_oldest(expected, *, sync=True)`** — `bool`; advances the
  logical head only if it exactly matches `expected`. The default atomically
  persists/fsyncs the cursor before removing the in-memory head; `sync=False`
  leaves cursor progress volatile until `checkpoint()`.
- **`outbox.checkpoint()`** — sync deferred data, persist/fsync current cursor,
  then delete fully consumed closed segments.
- **`outbox.load()`** — `list[MessageEnvelope]`; every pending message, in
  order.
- **`outbox.size()`** / **`len(outbox)`** — `int`.
- **`outbox.contains(message_id)`** — `bool`.
- **`outbox.compact()`** — compatibility maintenance name for checkpoint plus
  completed-segment cleanup; it never rewrites live payload records.
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
| `FastQueueFullError` | A FastMode publish cannot fit within its bounded RAM limits and spill cannot make room; nothing is silently discarded. |
| `ValueError` (from `sender.publish()`) | An explicit `message_id` is reused for different content. |
| `ValueError` (from a mode constructor) | A mode threshold, duration, or high-water ratio is invalid. |
| `TypeError` (from `Sender(...)`) | `mode` is not a `DurableMode`, `GroupMode`, or `FastMode` instance. |

## Examples

All scripts live in `examples/` and are runnable directly (`python
examples/<name>.py`) against a real broker unless noted otherwise:

- `basic.py` — the shortest useful `Sender` example: context manager,
  one publish, `wait_for_delivery`, `log_level="INFO"`.
- `durable_mode.py` — critical-event sender with per-message fsync and
  aggressive ACK checkpointing.
- `group_mode.py` — immediate append, batched data fsync, and batched ACK
  checkpoint configuration for telemetry.
- `fast_mode.py` — bounded RAM-first delivery, spill triggers/batching, and
  backpressure handling.
- `multiple_clients.py` — three independent senders, one per mode, with
  distinct client IDs and Outbox paths.
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

## Migrating to 0.4.0

0.4.0 is a pure naming/documentation release on top of 0.3.0 -- no classes,
modules, or lifecycle methods changed. It renames the two internal,
background-worker timeouts on `SenderConfig` so the layer is obvious from
the name alone; see [Timeouts, ACKs, and Blocking Behavior](#timeouts-acks-and-blocking-behavior)
for why.

| Old | New | Notes |
|---|---|---|
| `ack_timeout=` | `delivery_ack_timeout=` | The name now says which ACK -- reliomq's own, not MQTT's PUBACK. |
| `publish_timeout=` | `mqtt_puback_timeout=` | The name now says which layer -- MQTT, not reliomq's end-to-end delivery. |

Both old names still work, unchanged, with a `DeprecationWarning`. See
[CHANGELOG.md](CHANGELOG.md) for the complete 0.4.0 release notes.

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

In 0.3.0, everything else — the reliability guarantee,
retry/reconnect/shutdown behavior, and the then-current on-disk Outbox
format — was unchanged. The segmented Outbox migration still replays that
legacy format transparently. See
[CHANGELOG.md](CHANGELOG.md) for the complete 0.3.0 release notes,
including the earlier 0.2.0 migration table (`event_id`→`message_id`).

## Tests

Run the deterministic suite from this directory:

```bash
python -m unittest discover -s tests -v
```

`test_protocol.py`, `test_outbox.py`, `test_persistence.py`, `test_ack.py`,
`test_mqtt.py`, and `test_config.py` test the wire protocol, segmented queue,
cursor/checkpoint recovery, legacy migration, mode policies, ACK correlation,
Paho helpers, and configuration validation in isolation.
`test_sender.py` and `test_relay.py` drive each component through a fake
Paho client to exercise success, broker outage, return-code failure,
`mqtt_puback_timeout` expiry, `delivery_ack_timeout` expiry (including that
each config value is actually wired to the layer its name promises, not
just present under a new name), restart, FIFO recovery, wrong/late/
malformed/duplicate DeliveryAcks, all three fixed client modes and spill/sync
boundaries, reconnect, relay failure/success, Paho-style lifecycle aliases,
and shutdown state transitions.
`test_mode_integration.py` checks the real segmented Outbox through all three
modes, including 100 Durable publishes/100 data fsyncs, 100 Group appends/five
data fsyncs under count-only triggers, and 100 healthy Fast deliveries/zero
persistent message writes. It also checks ACK checkpoints, spill/ACK races,
and bounded RAM payload retention. `test_spill_recovery.py` injects partial
spill and rollback failures to verify retry ownership and FIFO recovery.
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

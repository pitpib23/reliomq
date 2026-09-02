# Changelog

All notable changes to this project are documented in this file.

## 0.4.0 — 2026-09-02

A timeout-naming and execution-model documentation follow-up to 0.3.0. No
class/module/topic renames, no lifecycle changes -- this release is purely
about naming the two internal, background timeouts after exactly what each
one waits for, and making the blocking-vs-background execution model
explicit enough to answer "which ACK is this?" and "does this block my
code?" without reading the source. Reliability semantics, the wire
protocol, and the on-disk Outbox format are all unchanged from 0.1.0.

### Timeout naming

- **`SenderConfig.publish_timeout` → `mqtt_puback_timeout`**,
  **`SenderConfig.ack_timeout` → `delivery_ack_timeout`**. In a two-layer
  acknowledgement protocol, "ack_timeout" doesn't say which ACK, and
  "publish_timeout" doesn't say which publish (the source MQTT publish, or
  the whole delivery?). The new names put the layer directly in the name:
  `mqtt_puback_timeout` governs only the MQTT/Paho QoS 1 PUBACK wait for
  one publish attempt; `delivery_ack_timeout` governs only the wait for
  reliomq's own end-to-end `DeliveryAck`. Both remain internal,
  background-worker timeouts that never block the calling thread --
  `Sender.wait_for_delivery(timeout=...)` is unchanged and remains the only
  one of the three waits that belongs to the caller. `RelayConfig`'s
  already-unambiguous `destination_publish_timeout` /
  `source_ack_publish_timeout` were not touched.

### Documentation

- New README sections addressing the timeout/blocking execution model:
  "I want to..." (a quick decision table near the top), "Timeouts, ACKs,
  and Blocking Behavior" (the three-wait table plus a two-thread diagram
  distinguishing the calling thread from reliomq's background delivery
  worker), "What to Use and When" (a background-vs-blocking decision table
  for every public tool, plus a compact what-it-does/use-when/don't-use-
  when/background/blocks-caller/example writeup for `Sender`,
  `wait_for_delivery()`, `pending_count()`, `mqtt_puback_timeout`,
  `delivery_ack_timeout`, and `retry_interval`), and a "Timeout and retry
  settings in detail" table (layer, where it runs, whether it blocks the
  caller, and recommended usage for every timeout/retry setting on both
  configs).
- README's "Cookbook" section (0.3.0) renamed and expanded to "Common
  Usage Patterns": fire-and-continue telemetry, publish-and-confirm,
  operational monitoring, debugging a stuck message, temporary network
  outage, graceful shutdown, and when `Relay` is required -- each pattern
  now states plainly whether it blocks the caller.

### Logging terminology

- Retry-reason strings now name the specific layer that failed to confirm
  rather than a generic phrase: `"MQTT PUBACK not confirmed within
  mqtt_puback_timeout"` and `"DeliveryAck not confirmed within
  delivery_ack_timeout"` replace the previous `"broker publish not
  confirmed"` / `"DeliveryAck timeout or interruption"`. Pure log-text
  changes; the retry decision and timing behind them are unchanged.
- DEBUG-level `Publish attempt` and `Waiting for DeliveryAck` lines now
  include the exact configured `mqtt_puback_timeout=`/
  `delivery_ack_timeout=` value in effect, so a value can be read directly
  off the log line that's blocked on it.
- `"Broker publish confirmed (PUBACK)"` → `"MQTT PUBACK received"`;
  `"ACK subscription ready/rejected"` and `"ACK subscription request
  failed/rejected"` → `"DeliveryAck subscription ..."` (this is
  specifically the subscription to the delivery-ack topic, not a PUBACK);
  `"Retry scheduled"` → `"Delivery retry scheduled"`;
  Outbox's `"Persisted message removed"` → `"Message removed from
  Outbox"`. None of these renames change what triggers the log line or
  what happens afterward.

### Compatibility

- `ack_timeout=`, `publish_timeout=` -- still accepted as `SenderConfig`
  constructor keywords (each resolving to the new field, each warning) and
  as read-back properties. All 0.3.0 and earlier deprecated aliases
  (`ReliablePublisher`, `PublisherConfig`, `ReliabilityConfig`,
  `ReliableMqttBridge`, `BridgeConfig`, `DurableMessageStore`, `StoreError`,
  `Ack`, `queue_path=`, `data_topic=`/`envelope_topic=`, `ack_topic=`,
  `sender.store`, `Relay(bridge_logger=...)`, and the old
  `reliomq.publisher`/`.bridge`/`.store` module paths) are unchanged and
  still work.
- No intentional breaking changes.

### Migration

`mqtt_puback_timeout=`/`delivery_ack_timeout=` replace
`publish_timeout=`/`ack_timeout=` on `SenderConfig` -- update at your own
pace, since the old names keep working (with a `DeprecationWarning`).
Nothing else changed.

## 0.3.0 — 2026-09-01

A naming, usability, and documentation follow-up to 0.2.0: reliomq's public
class names are now short and Paho-familiar, and the `Sender`/`Relay`
lifecycle deliberately echoes `paho-mqtt`'s `connect()`/`loop_start()`/
`publish()`/`loop_stop()`/`disconnect()` shape, without pretending
reliomq's stronger delivery guarantees behave identically to plain MQTT.
0.2.0's own naming direction (`event_id`→`message_id`, INFO/DEBUG
observability, `log_level=`/`debug=`) is unchanged and fully preserved.
Reliability semantics, the wire protocol, and the on-disk Outbox format are
all unchanged from 0.1.0/0.2.0.

### Naming migration

- **`ReliablePublisher` → `Sender`**, **`ReliableMqttBridge` → `Relay`**,
  **`PublisherConfig`/`ReliabilityConfig` → `SenderConfig`**,
  **`BridgeConfig` → `RelayConfig`**, **`DurableMessageStore` → `Outbox`**
  (`StoreError` → `OutboxError`), **`Ack` → `DeliveryAck`**. Every old name
  is kept as a working alias (see Compatibility below).
- **`queue_path=` → `outbox_path=`**, **`data_topic=`/`envelope_topic=` →
  `relay_topic=`**, **`ack_topic=` → `delivery_ack_topic=`** on both
  configs. The topic field has now been renamed twice across releases
  (0.1.0 `data_topic` → 0.2.0 `envelope_topic` → 0.3.0 `relay_topic`); all
  three spellings are still accepted as constructor keywords and read-back
  properties.
- Canonical module layout: `reliomq/sender.py`, `reliomq/relay.py`,
  `reliomq/outbox.py`. The old `reliomq/publisher.py`,
  `reliomq/bridge.py`, `reliomq/store.py` module paths still work as thin
  re-export shims.
- **`publish()` and `wait_for_delivery()` are unchanged and are not being
  renamed** to `send()`/`wait_until_delivered()` — they were already the
  right, Paho-familiar names.

### Paho-familiar lifecycle (new, additive)

- `Sender` gained `connect()`, `disconnect()`, `loop_start()`, `loop_stop()`,
  and `is_connected()`. `connect()`/`start()`/`loop_start()` are three names
  for the exact same operation (likewise `disconnect()`/`stop()`/
  `loop_stop()`) — reliomq cannot honestly separate "connected" from
  "processing the Outbox" the way raw Paho can, since durable delivery *is*
  that background worker; this is documented explicitly rather than faked.
  `is_connected()` mirrors Paho's own semantics (transport state only) and
  is documented as not implying delivery-readiness.
- `Relay` gained `connect()`, `disconnect()`, `loop_start()`, `loop_stop()`,
  `source_connected`, and `destination_connected`. `Relay.connect()` brings
  up *both* broker connections together, by design — there is no
  per-broker `connect()` in this API.
- `sender.outbox` is the new public attribute name for the durable store
  (was `sender.store`); `sender.store` still works as a read-only alias.
- Fixed a latent naming collision introduced by 0.2.0's `event_id`→
  `message_id` rename: two internal SUBACK-callback parameters were also
  named `message_id`/`event_id` but represent Paho's own MQTT packet
  identifier (`mid`), an unrelated concept. Renamed those internal
  parameters to `mid` so the two concepts can't be confused; no public API
  or log output was affected (log lines already said `mid=`).

### Documentation

- New README sections: "How reliomq works" (mental-model diagram +
  PUBACK-vs-DeliveryAck table), "Architecture overview", "If you already
  know Paho MQTT" (mapping table + the two important differences),
  "Publishing" (all seven canonical publish patterns: basic, capture ID,
  publish-and-continue, publish-and-wait, offline publish, restart
  recovery, pending-count monitoring), "Relay integration", "Cookbook",
  and a full "Public API" reference covering every public class, method,
  property, and exception with a minimal example each.
- New examples: `paho_style_lifecycle.py` (explicit lifecycle shown
  equivalent to the context-manager form) and `modbus_sensor.py` (a
  realistic, read-only Modbus TCP poller bridged to MQTT through `Sender`;
  requires the optional `pymodbus` package, not a reliomq dependency).
  `examples/bridge.py` renamed to `examples/relay.py`.
- All examples and their docstrings updated to the new preferred names and
  lifecycle calls.

### Compatibility

- `ReliablePublisher`, `PublisherConfig`, `ReliabilityConfig`,
  `ReliableMqttBridge`, `BridgeConfig`, `DurableMessageStore`, `StoreError`,
  `Ack` — all kept as working aliases of their new names, each emitting
  `DeprecationWarning`.
- `queue_path=`, `data_topic=`/`envelope_topic=`, `ack_topic=` — all still
  accepted as constructor keywords (each resolving to the new field, each
  warning) and as read-back properties.
- `sender.store` (property, warns) still reads `sender.outbox`.
- `Relay(..., bridge_logger=...)` still works as an alias for
  `relay_logger=` (warns).
- Old module import paths (`reliomq.publisher`, `reliomq.bridge`,
  `reliomq.store`) still work.
- No intentional breaking changes. If you only ever imported names from
  `reliomq.__all__` and didn't hardcode the JSON wire field name, nothing
  in your code needs to change for this release to work.

### Migration

See the README's [Migrating to 0.3.0](README.md#migrating-to-030) section
for a full old-API → new-API table with before/after code. The 0.2.0
migration table below (`event_id`→`message_id`, etc.) is still accurate
and complete for anyone upgrading directly from 0.1.x.

## 0.2.0 — 2026-09-01

A usability, naming, documentation, and observability release. The
reliability guarantee, wire protocol, retry/reconnect/shutdown behavior, and
on-disk queue format are all unchanged from 0.1.0 — this release is about
making the library easier to pick up, easier to watch run, and easier to
diagnose when something goes wrong, not about changing what it does.

### Clearer terminology

- **`ReliabilityConfig` → `PublisherConfig`.** The name now pairs with
  `ReliablePublisher` the way `BridgeConfig` already paired with
  `ReliableMqttBridge` — which config goes with which component is now
  obvious on sight.
- **`data_topic` → `envelope_topic`** (on both configs). The old name was
  easy to confuse with the *application* `topic` you pass to `publish()`;
  `envelope_topic` makes clear it's reliomq's own transport topic for its
  message envelope, ties into the existing `MessageEnvelope`/
  `DeliveryEnvelope` vocabulary, and is not the topic your payload ends up
  on.
- **`event_id` → `message_id`** (on `publish()`, `wait_for_delivery()`, the
  protocol dataclasses, `DurableMessageStore.contains()`, and every log
  line). "Event" read as "something happened"; this value identifies *one
  message*, stable across every retry of it — `message_id` says that
  directly. **The wire format is unchanged**: the JSON field is still
  spelled `"event_id"`, so a 0.1.x publisher and a 0.2.x bridge (or vice
  versa) remain fully interoperable through a rolling upgrade. Only the
  Python-facing name changed.
- `reliomq.protocol.new_event_id()` / `validate_event_id()` renamed to
  `new_message_id()` / `validate_message_id()`.

### Improved public API usability

- `PublisherConfig`/`BridgeConfig` gained `log_level=`/`debug=` fields (see
  Observability below) so runtime visibility is one constructor argument,
  not a separate `logging` setup step.
- Removed a dead, unused internal alias (`ReliableMqttBridge._forward`) that
  duplicated `_forward_once` with no callers.
- `DurableMessageStore` now logs its pending count when opened, so
  constructing one (directly, or via `ReliablePublisher`) is visible in the
  log instead of silent.

### Observability

- New `reliomq.enable_logging(level)`: attaches one lightweight stderr
  handler directly to the `"reliomq"` logger, idempotently, and disables
  further propagation from it so it can never duplicate a line through a
  root/application handler you've already configured. Safe to call (or
  trigger) more than once.
- New `log_level=`/`debug=` fields on `PublisherConfig`/`BridgeConfig` call
  `enable_logging()` for you at construction time. `debug=True` is
  shorthand for `log_level="DEBUG"`; combining it with a *different*
  explicit `log_level` raises `ConfigError` rather than silently picking
  one.
- Default behavior is unchanged and still fully quiet: without
  `log_level=`/`debug=`, nothing is installed and INFO/DEBUG stay invisible,
  exactly like 0.1.0.
- Substantially expanded INFO/DEBUG coverage across the whole lifecycle:
  initialization, connecting, connected/reconnected, durable store opened,
  pending messages restored after a restart, a message accepted and
  queued, each delivery attempt, the MQTT PUBACK vs. the application ACK
  logged as clearly separate events, delivery confirmed and removed from
  the durable queue, retry scheduling (with an in-memory, log-only
  per-message attempt counter — never persisted, never affects delivery
  decisions), and graceful shutdown. Every line that concerns one message
  carries its `message_id` so a single message can be traced end-to-end.
  Payloads are never logged at any level.
- Fixed a latent naming collision in the publisher/bridge SUBACK callbacks:
  Paho's own MQTT packet identifier parameter was previously also named
  `message_id`/`event_id` in a couple of internal call sites, which would
  have collided with reliomq's own `message_id` concept once introduced.
  Renamed those to `mid` (matching the existing `_subscription_mid`
  internal field) to keep the two concepts unambiguous.

### Documentation

- README gained a "How reliomq works" section (plain-language message
  lifecycle, with an explicit table distinguishing the MQTT PUBACK from the
  application ACK) and an "Architecture overview" table (what each
  component is responsible for, and when you'd interact with it).
- README's Logging section rewritten around the new `log_level=`/`debug=`
  path, with a level-by-level table of what INFO vs. DEBUG actually show.
- New `examples/debug_logging.py`: a `debug=True` walkthrough that runs
  with no broker required, so DEBUG-level diagnosis is visible with zero
  setup.
- All examples updated to the new preferred names (`PublisherConfig`,
  `envelope_topic`, `message_id`) and, where they construct a reliomq
  config, to `log_level="INFO"` instead of manual `logging.basicConfig()`.
- Docstrings expanded on the main public classes (`ReliablePublisher`,
  `ReliableMqttBridge`, `DurableMessageStore`) to explain what each is for,
  when to use it, and important lifecycle/failure behavior — not just
  restate the class name.

### Compatibility

- `ReliabilityConfig` kept as a subclass alias of `PublisherConfig` —
  `isinstance()` checks against `PublisherConfig` still pass for it.
  Constructing it emits `DeprecationWarning`.
- `data_topic=` still accepted as a constructor keyword on both configs
  (resolves to `envelope_topic`, warns), and `.data_topic` still readable
  as a property (warns).
- `event_id=` still accepted as a keyword on `publish()`,
  `wait_for_delivery()`, `MessageEnvelope`, `DeliveryEnvelope`, `Ack`, and
  `DurableMessageStore.contains()` (resolves to `message_id`, warns), and
  `.event_id` still readable as a property on the protocol dataclasses
  (warns). Passing both a new-style and old-style value with *different*
  content raises rather than silently picking one.
- `AckTracker` (fully internal, never exported) was renamed with no
  compatibility shim — nothing outside this package constructs one.
- No intentional breaking changes. If you only ever imported the names
  listed in `reliomq.__all__` and didn't hardcode the JSON wire field name,
  nothing in your code needs to change for this release to work.

### Migration

See the README's [Migrating from 0.1.x](README.md#migrating-from-01x)
section for a full old-API → new-API table with before/after code.

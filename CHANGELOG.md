# Changelog

All notable changes to this project are documented in this file.

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

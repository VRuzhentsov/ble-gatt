# 0004 — A `mock-broker` feature for cross-process e2e, not a new mock

## Status

Accepted.

## Context

Fini wants a real CI gate: a Playwright two-process e2e where two real
`fini-app` processes connect over the actual transport code path (dial loop,
peripheral accept, backoff, session claim), with a mock radio underneath
instead of hardware. `backend::mock::MockNetwork` already does exactly the
right in-process simulation — including fault injection
(`simulate_loss_for_session`, `arm_scan_failure`, and friends) — but it is an
`Arc` shared inside one process, so it cannot bridge two OS processes.

The alternative — duplicating this simulation inside fini as a second mock —
was rejected: it would drift from this library's own `Backend`/
`GattConnection` contract the moment either side changed, defeating the
point of testing against the real trait.

## Decision

### Broker process, not a peer-hosted bus

`MockNetwork` gains a `Radio` enum (`Local`/`Remote`) behind it. `Local` is
exactly today's behavior — the only variant that exists with the feature off,
and `MockNetwork::new()`'s signature and behavior are unchanged. `Remote`
connects over TCP to a separate `MockNetwork::serve()` broker process instead
of sharing an in-process `Arc`.

The broker owns the shared state as its own process, rather than one peer's
process hosting a bus the other connects to: an actors-harness that kills
processes independently must not let one actor's death tear down the radio
the *other* actor still needs. A peer-hosted bus can't survive that; a
separate broker can. This also mirrors Bumble's `bumble-link-relay`, and
positions a later swap to real BlueZ (Bumble+VHCI) as a smaller step than it
would be from a peer-hosted design.

### The broker reuses `LocalRadio` verbatim — wire parity is structural

`MockNetwork`'s prior direct-field access from `MockBackend`/
`MockGattConnection` is replaced with a named async method surface
(`connect`, `advertise`, `read`, `subscribe`, …) that both `Local` and
`Remote` implement identically. The broker's request handlers call the exact
same `LocalRadio` methods `Radio::Local` dispatches to in-process — there is
one implementation of radio semantics, not two kept in sync by hand. This is
what makes "the broker behaves like the in-process mock" a structural
guarantee rather than an aspiration, and it's proven directly:
`tests/mock_broker.rs` replays `tests/mock_protocol.rs`'s scenarios verbatim
against a real loopback socket.

### Length-prefixed JSON over hand-rolled binary framing

The wire protocol (`Envelope`/`Frame`/`Request`/`Response`/`Push`) is
serialized as `u32`-length-prefixed `serde_json`, gated behind a new
`mock-broker` Cargo feature (`serde`, `serde_json` as optional deps,
`tokio/net` + `tokio/io-util`, `uuid/serde`). This is test-only,
non-perf-sensitive infrastructure — hand-rolled per-variant binary encoding
would cost hundreds of lines of hand-maintained encode/decode sites with no
compiler-enforced round-trip safety, for no benefit this use case needs.
`serde`/`serde_json` are new to the core `ble-gatt` crate but not to this
workspace — `tauri-plugin-ble-gatt` already depends on both.

Existing domain types (`GattEvent`, `GattServiceSpec`, `PeerAddress`, etc.)
gained `#[cfg_attr(feature = "mock-broker", derive(Serialize, Deserialize))]`
directly rather than a parallel set of `Wire*` mirror types: they are plain
data with no handles or trait objects, so a mirror would only add a drift
risk with no corresponding benefit.

### `BleError::Transport`, unconditional

A `mock-broker` connection failure (the broker died, the socket dropped) is
a distinct error class from `Gatt` (a GATT-level refusal) or `NotConnected`
(a stale-session refusal) — and downstream liveness logic in consumers may
depend on distinguishing them, so a transport hiccup must not be silently
flattened into an existing variant. `BleError` gained a `Transport(String)`
variant unconditionally (not itself feature-gated) so its shape doesn't
differ by build configuration; only its `Serialize`/`Deserialize` derive is
gated behind `mock-broker`. This is a semver-relevant addition — any
exhaustive `match` on `BleError` gains one more required arm — accepted as
worth taking now rather than special-casing the enum's shape per feature.

### Client-allocated subscription ids

`Request::Subscribe` carries a **client-allocated**, not broker-allocated,
`subscription_id`. The client registers its local delivery channel under
that id before sending the request, which closes the only otherwise-possible
race in this protocol: a `Push::NotifyItem` arriving before the client has
anywhere to route it. The same reasoning is why `RegisterAddress` needs no
acknowledgement: it is always the first frame a client sends, the shared
per-connection outbox queue preserves that ordering on the wire, and every
operation that could cause a push to an address is itself gated behind that
address having already taken some prior action (advertise to be
discoverable, subscribe to become a notify target) — which can only happen
after its own `RegisterAddress` was already sent.

### Explicit non-goal: fault injection over the wire

Every existing `simulate_*`/`arm_scan_failure`/`set_advertisement_data`/
`disconnected_peers` use in the test suite runs against `Local`; fini's
Phase 1 e2e is happy-path only. These stay Local-only in `MockNetwork` and
panic with a clear message if called against a `Remote` connection, rather
than silently no-op-ing — a silent no-op here could produce a false test
pass. Wiring fault injection over the broker is a natural follow-up if a
later phase needs it, not solved here.

One accepted, documented divergence from this scope boundary: locally,
dropping a `subscribe()` stream breaks the sender→receiver pipe immediately,
so `notify()`'s "reached nobody" error can observe it right away. Over
`Remote`, a client-side stream drop doesn't tell the broker's forwarder task
to stop — only an explicit disconnect does. Accepted because it only affects
that one error-observability path, not the connect/read/write/notify/
subscribe/disconnect happy path fini's e2e needs.

## Consequences

- The default build (feature off) is byte-for-byte today's behavior: zero
  new dependencies, and every existing test in `tests/mock_protocol.rs` /
  `tests/datagram.rs` passes unchanged — this was the hard acceptance bar
  for the whole change.
- `fini` can bridge two real OS processes over this mock without ble-gatt
  and fini maintaining two divergent simulations of the same trait contract.
- Enabling `ble-gatt/mock-broker` from behind fini's own target-gated
  dependency edge (linux/android) is a real Cargo feature-unification risk
  worth a `cargo tree -e features` spike on fini's actual CI runner target —
  flagged for fini to verify, not resolved here.
- If a later phase needs fault injection reachable from a test harness that
  isn't one of the two connected processes (e.g. an orchestrator injecting a
  fault into the broker's own radio), this ADR's Local-only boundary will
  need revisiting — most likely by adding an explicit control-plane request
  class rather than lifting the panic guard.

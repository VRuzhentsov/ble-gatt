# 0005 — An owned link-state machine, and the `PeerLink` tier it enables

## Status

**Proposed — draft under review.** Not yet accepted. `PeerLink` (the consumer
API in `docs/interface.md`) depends on this; the internal state machine here is
the prerequisite.

## Context

`ble-gatt` tracks "what is the connection to this peer doing" as several
independent facts that nothing reconciles, on both backends.

**Android central** (`android.rs` `ConnectionState` + the Kotlin bridge):

| Fact | Means | Cleared by |
|---|---|---|
| `session: u64` | connection generation | never (overwritten) |
| `live: bool` | a `BluetoothGatt` is open | `onDisconnected` JNI, `clear_pending_connect` |
| `connected_tx: Option` | a dial is in flight | `onConnected`, failure paths |
| `disconnected_tx: Option` | a `disconnect()` is awaiting confirmation | `onDisconnected` |
| Kotlin `connectedGatts` / `gattSessions` / `priorityLowered` / `priorityFallbacks` | platform handle bookkeeping | `onConnectionStateChange` STATE_DISCONNECTED |

**Linux central** (`linux.rs` `LinuxBackend`):

| Fact | Means | Cleared by |
|---|---|---|
| `dialed: HashMap<PeerAddress,u64>` | generation of the current dial/link | CONNECT_TIMEOUT, refusal, guard drop, link-loss watcher |
| `in_flight: HashSet` | a dial is happening right now | `InFlightGuard` |
| `pending_cleanup: HashSet` | an abandoned dial's cleanup disconnect is unconfirmed | the cleanup task, when a disconnect finally returns Ok / NotConnected |

**Peripheral, both backends:** `served_peers` / `server_sessions:
HashMap<PeerAddress,u64>` plus disconnect watchers and the `superseded` guard.

**No component owns a transition, so no transition can have an effect.** That is
the root cause, not a detail of it — the same diagnosis Fini's ADR-0005 reached
for its transport layer.

### The failure it produced

Real hardware, Pixel 6 Pro + Linux desktop, both on merged `3483f81`:

1. BLE session up, both sides green.
2. Phone adapter off (`adb shell svc bluetooth disable`).
3. Phone adapter back on.
4. Phone dials the desktop.

```
11:49:20 datagram  connect: dialling 88:D8:2E:BA:72:27
11:49:20 transport connect ... failed: a connection to this peer is already open
11:49:28 ... identical    11:49:44 ... identical
11:50:14 did not complete auth within 60s; pausing automatic retries
```

Android sends **no GATT disconnect callback** when the adapter is toggled. So
`onDisconnected` never fires, `state.live` stays `true`, and every `connect()`
is refused as "already open" — permanently, with nothing able to clear it.

Linux is not affected today: `connect()` there has no persistent "already
connected" refusal, and BlueZ emits `Connected(false)` on adapter-off, which
the existing link-loss watcher catches. But Linux has the same *topology* —
scattered facts, no owned transition — so it is one platform quirk away from an
equivalent bug (a GATT-133 storm, the app backgrounded mid-connect, an
`onConnectionStateChange` failure status, airplane mode).

Adding an adapter-off handler fixes this instance. The goal is to make **"the
record says connected but nothing is"** a state that cannot be written, caught
by a test that fails if you try.

## Decision

Introduce an explicit state machine that **owns** the connection state. Not one
machine — three small ones sharing one contract. A single machine that owned
the whole peer set would stop being a readable transition table and become a
scheduler.

### Three machines

**`RadioState`** — one per backend. The universal power-state seam.

```
States:  Off | On | Unsupported
Events:  PoweredOn | PoweredOff | NoAdapter
Effect:  InvalidateAllLinks   (on any -> Off / -> Unsupported transition)
```

Barely a machine, but it is the seam: a new backend implements one thing —
"tell `RadioState` when the radio died" — and inherits correct teardown of
every link. It can grow (`Resetting` for Android's TURNING_OFF/ON, airplane
mode, a headset radio sleeping) without touching per-peer code. v1 folds
`STATE_TURNING_*` into `Off`.

`Unsupported` (no usable adapter for the wanted role, ever) is distinct from
`Off` (a toggle). `PeerLink` surfaces this so a consumer stops polling
`capabilities()` to guess — a real request from Fini's transport layer, which
does exactly that poll every 60s today.

**`CentralLink`** — per peer address. Central-role connection lifecycle; where
the bug lives.

```
Idle                              no link, nothing in flight; connect() may proceed
Dialing { since }                 connect() issued, platform callback pending
Connected { session }             link up, GattConnection handed to caller
Disconnecting { since, session }  disconnect() issued, confirmation pending
Draining { since }                an abandoned dial's cleanup teardown is running
                                  (Linux pending_cleanup; on Android never entered)
RadioOff                          radio is down; left only by RadioBack
```

> **Invariant:** a platform GATT connection to this peer exists **iff** the
> state is `Connected` or `Disconnecting`.

`Dialing` = not connected yet. `Draining` = a *previous* attempt's teardown.
Every "already open, refuses forever" bug is a violation of that one sentence.

**`PeripheralLink`** — per peer address. A remote central attached to our GATT
server.

```
Absent | Accepted { session } | Serving { session }
       | Disconnecting { since, session } | RadioOff
```

> **Invariant:** our GATT server holds a connection slot for this peer **iff**
> the state is `Accepted`, `Serving`, or `Disconnecting`.

Thinner (no dial), same discipline. Replaces `served_peers` / `server_sessions`
+ the disconnect watchers' generation checks.

### The shared contract

- `fn apply(&self, event, now: Instant) -> (Self, Vec<Effect>)` — pure. No
  clock read, no lock, no I/O; `now` is an argument. A test-only
  `dispatch_*_at(peer, event, now)` injects it, so deadlines measured in tens
  of seconds are tested in microseconds.
- `match (self, event)` — flat pairs, not `match state { match event }`.
- The radio-loss event is matched near the top as `(_, RadioLost)`, its effect
  decided from a `holds_platform_resource(&self)` helper — valid from every
  state, so a stray adapter bounce is never an unhandled pair.
- Unhandled `(state, event)` → `(self.clone(), vec![])`, documented: events race
  transitions that already moved past them.
- One exhaustive property test per machine walking every `(state, event)` pair,
  asserting the invariant both directions: a teardown effect is emitted only
  when a resource-holding state is left, and a resource-holding state is never
  left silently (one exception: the event that reports an already-gone
  resource).

### One effect variant

```
TearDown { emit_event: bool }
  - always: close/forget the platform link handle for this peer (idempotent)
  - iff emit_event: push GattEvent::Disconnected onto events()
```

`emit_event` is `false` only for a teardown the caller asked for and already
knows about (`DisconnectConfirmed`). Dialing is **not** an effect — the caller
owns that decision and reports it as `DialStarted`, so the state never claims an
attempt nobody made. Waking the pending `connect()` future is **not** an effect
either — the oneshot stays as async plumbing the executor bridges, keeping the
machine pure.

### `CentralLink` transitions

| From | Event | To | Effect |
|---|---|---|---|
| `Idle` | `DialStarted{s}` | `Dialing` | — |
| `Dialing` | `DialSucceeded` | `Connected{s}` | — |
| `Dialing` | `DialFailed` / `Tick` past `DIAL_WINDOW` | `Idle` | `TearDown{false}` |
| `Connected` | `DisconnectRequested` | `Disconnecting` | — |
| `Connected` | `LinkDropped` | `Idle` | `TearDown{true}` |
| `Disconnecting` | `DisconnectConfirmed` | `Idle` | — |
| `Disconnecting` | `Tick` past `DISCONNECT_WINDOW` | `Idle` | `TearDown{true}` |
| `Idle`/`Dialing` | `CleanupStarted` | `Draining` | — |
| `Draining` | `CleanupConfirmed` | `Idle` | — |
| `Draining` | `Tick` past `DRAIN_WINDOW` | `Draining` | `TearDown{false}` (retry) |
| any | `RadioLost` | `RadioOff` | `TearDown{true}` if it held a resource, else — |
| `RadioOff` | `RadioBack` | `Idle` | — |
| any other | — | unchanged | — |

`CentralLink` has **no `GaveUp` state**. "Gave up after N redials" is a policy
of the `PeerLink` tier (`docs/interface.md`), not of one attempt's transport
lifecycle — `PeerLink` owns the redial ladder, counts attempts, and projects
`PeerStatus::GaveUp`, leaving each individual attempt as a clean
`Idle → Dialing → …` pass through this machine. Keeping exhaustion out of the
machine keeps its table small and its invariant about one thing.

### Ownership and wiring

Each backend holds `StdMutex<RadioState>`, `StdMutex<HashMap<PeerAddress,
CentralLink>>`, `StdMutex<HashMap<PeerAddress, PeripheralLink>>`, plus an
executor-owned `central_waiters` map for the oneshots. A `dispatch_*` helper
captures `now` once, applies under the lock, runs effects outside it.

Deleted: Android `live` / `connected_tx`-as-state / `disconnected_tx`-as-state;
Linux `in_flight` / `dialed`-as-lifecycle / `pending_cleanup`; both backends'
`served_peers` / `server_sessions`. All fold into the three machines.
`cleanup_permits` stays (it bounds concurrency, not lifecycle).

New Kotlin: a runtime `BroadcastReceiver` for
`BluetoothAdapter.ACTION_STATE_CHANGED`, registered in `init {}`, unregistered
in `closeAll()`; `STATE_OFF`/`STATE_ON` → a new `onRadioState` JNI callback →
`dispatch_radio`. `RadioState`'s `InvalidateAllLinks` effect fans `RadioLost`
to every per-peer machine.

One `tokio::spawn` per backend, `interval(1s)`, dispatches `Tick` to the radio
and every per-peer machine.

### Decisions taken, with reasoning

| Question | Choice | Why |
|---|---|---|
| One enum or three machines | Three | A set-owning machine is a scheduler, not a table; the flat table is most of the readability. |
| Radio state: per-peer fact or its own machine | Own machine, fanned out | Single source of truth for "is the radio up"; a new backend wires one thing. |
| `Draining` (Linux `pending_cleanup`) in v1 | Yes | Leaving it a side map is exactly the pattern this ADR removes; doing it later is a second migration of the same code. |
| Deadline constants | Unified in `link_state.rs` | The machine is the source of truth for "too long"; per-backend drift is what the scatter looked like. |
| Mock backend | Fault injector only | No real radio or callbacks to model; a `simulate_radio_off()` driving the same fan-out is enough. |
| Rollout | One PR | User's call. Pure table + tests green before any wiring, so only the wiring is unverified on first hardware contact. |

## Consequences

The pure transition tables are exhaustively testable without a radio, a peer,
or a runtime — which the current design is not, and that is why every defect in
this area was found on hardware.

`PeerLink` (`docs/interface.md`) becomes possible: with the library owning an
honest per-peer link state, it can own dialing, the redial ladder, glare, and
adapter-bounce recovery, and a consumer stops reimplementing all of it. Fini's
transport layer confirmed the reduction from its actual code: `dial` /
`dial_with_backoff` / `spawn_dial_loop` / `should_dial_peer` and four
process-global maps (`in_flight_dials`, `dial_backoff_until`, `dial_exhausted`,
`accepting_side_unconnected_since`) move out. The one thing the consumer keeps
touching is *rendering* "gave up" — so `PeerLink` must expose `PeerStatus::GaveUp`
plus a `retry(peer)` call, or the consumer rebuilds a shadow of the exhaustion
tracking just to draw its UI row.

This is a large change touching the connection path on both backends and the
Kotlin bridge. Comments on the deleted maps document past P1 fixes
(`in_flight` timing, quarantine-on-failure, the `superseded` guard); each needs
a corresponding transition or it regresses, and they are enumerated in those
comments to be walked one by one.

## Risks

Landing in one piece means no intermediate version verifiable on hardware —
and hardware is what found every defect here. Mitigation: the three transition
tables and their property tests are written and green before any wiring.

The Linux `Draining` migration touches the abandoned-connect cleanup path,
which has been through roughly six rounds of automated-review P1 fixes. Its
retry-until-`Ok`/`NotConnected` loop, its per-attempt bound, and its
backend-wide `cleanup_permits` semaphore each map to a specific transition or
effect and must be preserved, not re-derived.

## Verification

- Unit: every `(state, event)` pair for all three machines, no-op pairs included.
- Unit: the two invariants, property-tested after every transition.
- Regression: `cargo test` default and `--features mock-broker`, unchanged.
- Hardware (acceptance): establish a BLE session, toggle the phone adapter off
  then on, dial again — the dial is **accepted**, not refused; and
  `GattEvent::Disconnected` was emitted for the pre-toggle link.
- Hardware: `make e2e-devices` stays green on the external-actor pair.

## Open questions

- One `Tick` loop per backend (current lean) vs. per-machine timers.
- `GattConnection` after `RadioLost`: `read`/`write` → `NotConnected` via the
  machine instead of `ensure_current`. Same behaviour, one source — confirm
  that is the contract.
- Whether `RadioState` needs `Resetting` in v1 or can fold TURNING_* into `Off`
  until hardware shows a flapping adapter causing churn.
- `DIAL_WINDOW` / `DISCONNECT_WINDOW` / `DRAIN_WINDOW` values — start from the
  current `CONNECT_TIMEOUT` (20s) / `DISCONNECT_TIMEOUT` (5s) /
  `CLEANUP_DISCONNECT_TIMEOUT` (10s), tune on hardware evidence.

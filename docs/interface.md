# How `ble-gatt` is meant to be used

`ble-gatt` is a **carrier**. It moves bytes between two devices over BLE GATT
and does not interpret them — no framing, no session protocol, no encryption
(`docs/adr/0003`). This document is about *which shape of carrier* a given
consumer should reach for. The internals behind the tier-3 API are in
`docs/adr/0005`.

## The one-sentence decision

| You are… | Use |
|---|---|
| talking to third-party hardware that speaks its own GATT protocol | **Tier 1 — raw GATT** |
| moving whole messages to/from one peer and you want to own the connection yourself | **Tier 2 — datagram** |
| keeping app-to-app links to a set of peers alive over time | **Tier 3 — `PeerLink`** ← the default for app-to-app |

Most app-to-app consumers (Fini, a Bitchat-style mesh) want **Tier 3**. Tier 2
is the primitive it is built on, kept public for the rare consumer that
genuinely wants to drive one channel by hand.

---

## Tier 1 — raw GATT (`Backend`)

Scan, connect, read, write, subscribe; advertise, notify. You speak in
characteristics and MTUs. This tier imposes nothing — it exists for the
app-to-device case (a vendor sensor with its own command framing, CRC, and
challenge/response) where any layer the library added would be dead weight.

```rust
let mut conn = backend.connect(&peer).await?;
let value = conn.read(SENSOR_CHAR).await?;
conn.write(COMMAND_CHAR, cmd_bytes).await?;
let mut notifications = conn.subscribe(TELEMETRY_CHAR).await?;
```

You own everything: when to reconnect, what the bytes mean, how to frame them.

---

## Tier 2 — datagram (`datagram::connect` / `datagram::serve`)

An ordered, opaque-bytes **message pipe** to one peer. Hand it a `Vec<u8>` of
any size, get exactly that `Vec<u8>` out the other end. Fragmentation against
the negotiated MTU and bounded reassembly are handled inside; boundaries are
preserved so you can layer encryption on top without reinventing framing.

```rust
let mut channel = datagram::connect(backend, &peer, &config).await?;
channel.send(encrypted_message).await?;
let incoming = channel.recv().await?;
```

**You still own the connection lifecycle**: deciding to dial, redialing after a
drop, ensuring one link per peer, resolving simultaneous-dial glare, recovering
from an adapter power-cycle. You also supply the `Backend` and run on your own
runtime — the `DatagramChannel` here holds the `GattConnection` directly. For a
one-shot transfer or a test that is fine. For a long-lived app-to-app
relationship it is a lot of machinery to get right — which is why Tier 3
exists, **on top of this**, not replacing it.

---

## Tier 3 — `PeerLink` — the intended default for app-to-app

You declare **which peers you care about** and **one policy** (who dials). The
library owns keeping links alive; you consume channels and status.

### Configure once

```rust
// Normal case: PeerLink builds and owns the platform backend.
let link: Arc<PeerLink> = PeerLink::new(PeerLinkConfig {
    service:         MY_SERVICE,
    characteristic:  MY_CHARACTERISTIC,
    role:            LinkRole::Symmetric { local_id: my_node_id.into() },
    limits:          Default::default(),
    retry_budget:    RetryBudget::default(),
    max_links:       MaxLinks::default(),   // concurrent-link cap; conservative default
});

// Tests / advanced: inject an already-built backend (e.g. a MockNetwork).
let link = PeerLink::with_backend(backend, config);
```

**`PeerLink::new` is synchronous, infallible, and needs no ambient runtime.**
It returns a handle immediately. Internally it spawns **one dedicated OS thread
hosting its own Tokio runtime** — the driver, the platform backend, and all
per-peer machinery live there. So `new()` works in any binary, including one
with no `#[tokio::main]` (a CLI entry point). Adapter acquisition happens on
that thread, asynchronously, and may fail or be slow — that shows up as
`radio()` reporting `Off` or `Unsupported`, not a construction error. "A handle
exists, the radio may not" is a cleaner story than "the handle may not exist
yet," and "one call, works anywhere" beats "one call, but only from async
context" — a requirement a sync signature does not advertise.

Because the driver owns its own runtime, the `DatagramChannel` handed out by
`PeerLinkEvent::Up` is a **message-passing handle** — `send` / `recv` move bytes
across a channel to the driver thread, which does the GATT work. It is usable
from any context (the consumer's runtime, or none). This differs from Tier 2,
whose `DatagramChannel` holds the `GattConnection` directly and runs on the
caller's runtime.

The handle's contract across that hop:

- **`send` returns the real `BleError`, not a flattened string.** In particular
  `BleError::GattBusy` (a transient GATT rejection worth retrying) stays
  distinguishable from a permanent refusal — the crossing must not collapse
  them, or the caller-side retry that replaced the removed internal one
  (`docs/adr/0005` context) silently becomes retry-everything or retry-nothing.
- **`recv()` returns `None` when the link is closed** — and driver-thread death
  maps to `None`, never a hang. A handle whose far end is gone reads as closed.
- **A full internal send queue surfaces as `BleError::GattBusy`**, not as
  backpressure indistinguishable from a stall — so it slots into the same retry.

Fields. `service` + `characteristic` are the wire contract. `role` is the one
genuine protocol decision — who dials for a pair that can both see each other —
and has no sensible default, because it needs a stable identity both peers
compare the same way (glare; `docs/adr/0003` revision). `limits`,
`retry_budget`, and `max_links` have defaults.

`max_links` caps how many peers hold a live link at once, because a BLE central
has a hard concurrent-link limit (commonly ~7 on Android, higher on BlueZ). You
may `track()` more peers than that — the extra ones report
`PeerStatus::Queued` and are promoted when a slot frees (a tracked peer
disconnects or is `untrack()`ed). The default is conservative (4); raise it if
you know your platform allows more.

| `LinkRole` | Behaviour |
|---|---|
| `Symmetric { local_id }` | dials tracked peers *and* accepts inbound; `local_id` breaks the dial tie |
| `DialOnly` | only ever dials (central-only platform, a hub) |
| `AcceptOnly` | only ever advertises and accepts (a peripheral) |

### Declare interest

```rust
link.track(peer_address);     // keep a link to this peer alive
link.untrack(peer_address);   // stop; drop the channel, disconnect, forget
link.retry(peer_address);     // a tracked peer that gave up: try again now

let mut found = link.discover().await?;   // peers advertising the service
// discovery is untrusted metadata — you decide which to track()
```

### Consume the event stream

```rust
// Each call is an independent subscription — events() is a broadcast, so a
// second consumer is an addition, not a breaking change.
let mut events = link.events();
while let Some(ev) = events.next().await {
    match ev {
        PeerLinkEvent::Up { peer, channel }   => { /* a message pipe, valid until Down */ }
        PeerLinkEvent::Down { peer, reason }  => { /* the channel is dead */ }
        PeerLinkEvent::Status { peer, status } => { /* Connecting / Connected / Waiting / GaveUp / … */ }
        PeerLinkEvent::RadioChanged { status } => { /* On | Off | Unsupported */ }
    }
}

let s = link.status(&peer);           // snapshot for a UI row, without waiting
let r = link.radio();                 // On | Off | Unsupported, without waiting
```

`PeerStatus`:

```
Untracked
Unavailable { reason: RadioOff | Unsupported | RoleCannotReach }
Queued                           // tracked, but max_links is full — waiting for a slot
Connecting                       // trying — dialing, or waiting for an inbound link
Connected
Waiting { retry_at: Instant }    // a previous attempt failed; backing off before the next
GaveUp                           // the retry budget is spent; left by retry() or an inbound link
```

A projection of the library's internal state — you never see the state machine.

**`GaveUp` is reachable in *both* roles.** The dialer reaches it by exhausting
its redial budget. The acceptor — the side that is *not* the designated dialer
for a pair — reaches it when no inbound link arrives within the same budget.
Without the second path an acceptor's row would sit on `Connecting` forever
(a real past defect in Fini's `accepting_side_unconnected_since` /
`check_accepting_side_exhaustion`). `GaveUp` is deliberately observable, not
swallowed: a consumer rendering a "gave up — tap to retry" row needs to *see*
it and needs `retry(peer)` to restart. `retry_budget` therefore covers both
"how many redials" and "how long to wait for an inbound link."

**`RadioStatus::Unsupported`** is reported when the platform has no usable BLE
adapter for the configured `role` at all (as opposed to `Off`, a toggle). This
replaces a consumer polling `capabilities()` to guess.

### The library owns

- building and owning the platform backend (Tier 3 consumers never touch `Backend`)
- dialing tracked peers (when `role` and the tiebreak allow)
- the retry budget on *both* sides — redials for the dialer, an inbound-link
  deadline for the acceptor — and giving up (observable as `GaveUp`)
- accepting inbound links
- exactly one link per peer; resolving glare via `role`
- tearing everything down on radio-off and re-establishing on radio-on
- reporting radio state (`On` / `Off` / `Unsupported`), so the consumer stops
  polling `capabilities()` to guess whether an adapter exists
- surfacing an honest per-peer status

### The consumer still owns

- **which** addresses to `track` (discovery is untrusted; you choose)
- the **identity** used for the dial tiebreak (`local_id`)
- **every precondition the library is not** — network availability, a stored
  address from the consumer's own pairing flow, a per-pair enable/opt-out
  setting, OS-bond checks the consumer does for its own auth gate. `PeerLink`
  reports exactly one precondition: the radio. A consumer whose own state
  machine spans several transports (Fini's is keyed `(peer, transport)` and
  also serves a network transport) must keep the rest, because `ble-gatt`
  cannot own preconditions for a transport it is not.
- everything **above the byte boundary**: authentication, encryption, and any
  app-level liveness proof. The library carries bytes; it cannot know your
  peer's app has stopped answering, only that the BLE link exists.

### Multiple peers

`PeerLink` is multi-peer from the start — `track()` per peer, `status()` and
events per peer. Three guarantees make N peers safe rather than N times the
risk:

- **Per-peer isolation.** One unresponsive peer does not stall the others. The
  per-peer state machines are independent, and the driver does not hold a
  backend-wide lock across any one peer's platform call (the current Linux
  backend does — a stuck dial there freezes every peer, and the host app; that
  lock becomes per-address as part of this work). A test proves one wedged peer
  leaves the rest connecting.
- **The link cap is visible.** More tracked peers than `max_links` → the extra
  ones are `Queued`, not an indistinguishable `Connecting`, and promote when a
  slot frees.
- **Retry budgets are independent and do not coordinate.** N peers back off on
  their own ladders against the shared radio; that is deliberate — the only
  cross-peer arbitration is the `max_links` slot queue.

---

## `PeerLink` and a consumer's own state machine

A consumer like Fini runs its own session state machine on top (auth, an
encrypted channel, a ping/ack liveness proof). The seam:

| | `ble-gatt` (`PeerLink`) | consumer (e.g. Fini's `LinkState`) |
|---|---|---|
| Owns | transport: dial, connect, drop, redial ladder + give-up, radio | session: auth, proof, fade; preconditions the library is not |
| Driven by | platform callbacks | `PeerLinkEvent` + its own ping ticks |
| "Link nominally up but peer silent" | not visible — a dead BLE link is just `Down` | its own `Fading` state |
| "Gave up retrying" | owned here; exposed as `GaveUp` + `retry()` | *rendered* from that, not tracked |

`PeerLinkEvent::Up` starts the consumer's auth exchange; `Down` ends its
session. The consumer *renders* `GaveUp` and calls `retry()` on the user's tap,
but does not track exhaustion itself. What moves out of the consumer entirely:
the dial loop, backoff maps, and glare/exhaustion apparatus (in Fini's case,
`dial` / `dial_with_backoff` / `spawn_dial_loop` / `should_dial_peer` wiring and
four process-global maps whose comments document repeated past P1 fixes). What
stays: Fini's own pairing-protocol frames over the datagram tier, add-mode UX,
DB-backed eligibility, and the non-radio preconditions above.

---

## Worked example — Fini

```rust
// Built where DeviceConnectionState is built — sync, infallible, no await.
let link = PeerLink::new(PeerLinkConfig {
    service: FINI_SERVICE, characteristic: FINI_CHAR,
    role: LinkRole::Symmetric { local_id: my_node_id.into() },
    limits: Default::default(),
    retry_budget: RetryBudget::default(),
});

link.track(device.ble_address);   // when a paired device is known

while let Some(ev) = link.events().next().await {
    match ev {
        PeerLinkEvent::Up { peer, channel } => {
            let session = secure_channel::authenticate(channel).await?;
            sessions.insert(peer, session);
        }
        PeerLinkEvent::Down { peer, .. } => { sessions.remove(&peer); }
        PeerLinkEvent::Status { peer, status } => { device_rows.update(peer, project(status)); }
        PeerLinkEvent::RadioOff => device_rows.all_grey(),
        _ => {}
    }
}
```

What Fini **no longer does**: `backend.connect()`, a `HashMap<(peer,transport),
LinkState>` for the transport layer, `should_dial_peer` wiring beyond supplying
`local_id`, redial loops, glare resolution, adapter-bounce recovery.

---

## Pointers

- **`docs/adr/0003`** — why the library is a carrier, not a crypto library; the
  two original tiers.
- **`docs/adr/0005`** — the owned link-state machine that makes `PeerLink`
  possible, and the "connected but isn't" bug class it removes.
- `src/datagram/mod.rs` — the fragmentation primitive and its wire-shape limits.

## Open questions

- `discover` + explicit `track`, vs. an opt-in "auto-track anything advertising
  the service" mode. (lean: explicit only.)
- `RetryBudget` shape — a fixed default (redial ladder 1/2/4/8s cap 30s; give up
  after ~60s total; acceptor inbound deadline ~60s) plus override, vs.
  consumer-supplied only. (lean: default + override.)
- Whether `PeerLink` exposes the negotiated MTU / `max_message_len` per peer.
  (lean: yes, on `Up` — a consumer sizing its own payloads needs it.)
- Does `PeerLink` build the backend internally (`new`) *and* accept an injected
  one (`with_backend`), or only the latter? (lean: both — `new` for the normal
  case so a Tier 3 consumer never touches `Backend`; `with_backend` for tests
  and the mock.)

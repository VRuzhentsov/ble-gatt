# Current APIs and the target peer contract

`ble-gatt` is a **carrier**. It moves bytes over BLE GATT. The datagram profile
adds fragmentation and message boundaries, but not an application-session
protocol or end-to-end encryption (ADR-0003).

The [provisional target](adr/0014-core-peer-ownership.md) gives the core one
symmetric peer contract, independent of iroh: either peer can initiate or
reconnect. The public initiation-mode choice is not part of that target.
The source has not yet been refactored. This page documents current APIs;
examples using `LinkRole` or `InitiationPolicy` are not the proposed new API.

See the [core](../ble-gatt/README.md),
[iroh adapter](../ble-gatt-iroh/README.md) and
[Tauri adapter](../tauri-plugin-ble-gatt/README.md) for the ownership boundaries.

## Choosing a current API

| You are… | Use |
|---|---|
| talking to third-party hardware that speaks its own GATT protocol | **Tier 1 — raw GATT** |
| moving whole messages to/from one peer and you want to own the connection yourself | **Tier 2 — datagram** |
| keeping non-iroh byte channels to a set of peers alive over time | **Tier 3 — current `PeerLink`**, with its existing role policy |

Current iroh consumers use the datagram tier through `ble-gatt-iroh`, not the
existing `PeerLink` controller. The target must reuse core BLE behavior in both
cases without duplicating lifecycle ownership. It does not yet select an
operation-scoped or persistent tracked-peer API.

---

## iroh consumers: use the adapter, not a second `PeerLink`

For QUIC over BLE, `ble-gatt-iroh` already uses the datagram tier directly.
Install its `hooks()` on the iroh endpoint; supply a dialler, discovered peer
addresses and incoming channels. Do **not** also run `PeerLink` over those
channels: that would add another retry/close owner. The `PeerLink` examples
below describe the generic byte-channel API, not Fini's current integration.

```rust,ignore
let key = iroh::SecretKey::generate();
let transport = ble_gatt_iroh::BleGattTransport::builder()
    .dialer(ble_gatt_iroh::dial_with(adapter.clone(), config.clone()))
    .initiation_policy(ble_gatt_iroh::InitiationPolicy::EitherPeer)
    .build();
let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
    .secret_key(key)
    .add_custom_transport(Arc::new(transport.clone()))
    .address_lookup(transport.address_lookup())
    .hooks(transport.hooks())
    .bind().await?;
transport.set_peer_address(remote_endpoint_id, advertised_address);
// Feed datagram::serve's channels into transport.attach(channel).
// Either endpoint may call endpoint.connect(remote_endpoint_id, alpn).
```

This is a current-API example, not the target design. Today callers select
`EitherPeer` explicitly after verifying crossed-dial support. The
unchanged default is `PreviousDialer` (30-second yield after an unexpected
incoming-link loss). If crossed dials are unsafe, configure both peers with
`InitiationPolicy::Ordered { local_id: key.public() }` before moving `key`
into the endpoint: only the smaller endpoint key initiates. Individual
`CapabilityReport.central/peripheral` booleans do not prove concurrent-role
support. See [ADR-0013](adr/0013-symmetric-iroh-initiation.md).

Fini retains permitted peers, sync settings and application-session policy.
A losing application candidate closes its own iroh connection, **not** all
BLE links for that key while another connection uses them. The adapter's
hooks perform ordinary last-user release. Explicit `close_links` is a force
close for shutdown/revocation/abandoned connects, not candidate rejection.

## Tier 1 — raw GATT (`Driver`)

Scan, connect, read, write, subscribe; advertise, notify. You speak in
characteristics and MTUs. This tier imposes nothing — it exists for the
app-to-device case (a vendor sensor with its own command framing, CRC, and
challenge/response) where any layer the library added would be dead weight.

```rust
let mut conn = driver.connect(&peer).await?;
let value = conn.read(SENSOR_CHAR).await?;
conn.write(COMMAND_CHAR, cmd_bytes).await?;
let mut notifications = conn.subscribe(TELEMETRY_CHAR).await?;
```

You own everything: when to reconnect, what the bytes mean, how to frame them.

---

## Tier 2 — datagram (`datagram::connect` / `datagram::serve`)

An opaque-bytes **message pipe** to one peer. Messages are subject to the
configured size and reassembly bounds. Fragmentation against the negotiated
MTU and reassembly are handled inside; boundaries are preserved so you can
layer encryption on top without implementing GATT fragmentation yourself.

```rust
let mut channel = datagram::connect(&adapter, &peer, &config).await?;
channel.send(encrypted_message).await?;
let incoming = channel.recv().await?;
```

**You still own the connection lifecycle**: deciding to dial, redialing after a
drop, choosing usable channels, resolving simultaneous-dial glare, recovering
from an adapter power-cycle. You supply an `Adapter` and run on your own
runtime — the `DatagramChannel` here holds the `GattConnection` directly. For a
one-shot transfer or a test that is fine. For a long-lived app-to-app
relationship it is a lot of machinery to get right — which is why Tier 3
exists, **on top of this**, not replacing it.

---

## Tier 3 — `PeerLink` (current tracked-peer API)

This section describes the implemented controller, not the target public
design. Its `Symmetric` role chooses the lower `LinkId` to dial. That must not
be confused with either-peer initiation in ADR-0014.

You declare **which peers you care about** and **one policy** (who dials). The
library owns keeping links alive; you consume channels and status.

### Configure once

```rust
// Normal case: PeerLink builds and owns the platform driver.
let link: Arc<PeerLink> = PeerLink::new(PeerLinkConfig {
    // service + characteristic + fragmentation bounds all live here
    datagram:     DatagramConfig::new(MY_SERVICE, MY_CHARACTERISTIC),
    role:         LinkRole::Symmetric { local_id: my_node_id.into() },
    retry_budget: RetryBudget::default(),
    max_links:    MaxLinks::default(),   // concurrent-link cap; conservative default
});

// Tests / advanced: inject an already-built driver (e.g. a MockDriver).
let link = PeerLink::with_driver(driver, config);
```

**`PeerLink::new` is synchronous, infallible, and needs no ambient runtime.**
It returns a handle immediately. Internally it spawns **one dedicated OS thread
hosting its own Tokio runtime** — the worker, the platform driver, and all
per-peer machinery live there. So `new()` works in any binary, including one
with no `#[tokio::main]` (a CLI entry point). Adapter acquisition happens on
that thread, asynchronously, and may fail or be slow — that shows up as
`radio()` reporting `Off` or `Unsupported`, not a construction error. "A handle
exists, the radio may not" is a cleaner story than "the handle may not exist
yet," and "one call, works anywhere" beats "one call, but only from async
context" — a requirement a sync signature does not advertise.

Because the worker owns its own runtime, the `PeerChannel` handed out by
`PeerLinkEvent::Up` is a **message-passing handle** — `send` / `recv` move bytes
across a channel to the worker thread, which does the GATT work. It is usable
from any context (the consumer's runtime, or none). This differs from Tier 2's
`DatagramChannel`, which holds the `GattConnection` directly and runs on the
caller's runtime.

The handle's contract across that hop:

- **`send` returns the real `BleError`, not a flattened string.** In particular
  `BleError::GattBusy` (a transient GATT rejection worth retrying) stays
  distinguishable from a permanent refusal — the crossing must not collapse
  them, or the caller-side retry that replaced the removed internal one
  (`docs/adr/0005` context) silently becomes retry-everything or retry-nothing.
- **`recv()` returns `None` when the link is closed** — and worker-thread death
  maps to `None`, never a hang. A handle whose far end is gone reads as closed.
- **A full internal send queue surfaces as `BleError::GattBusy`**, not as
  backpressure indistinguishable from a stall — so it slots into the same retry.
- **`request_connection_priority` follows the same crossing rules.** It asks
  the platform for a latency/power trade-off on the live link
  (`ConnectionPriority::LowPower` while idle, `High` while traffic is expected)
  and can be called any number of times. Android only, and only on a link this
  side dialled — the Linux driver and accepted links return
  `BleError::Unsupported`, which a cross-platform caller treats as a no-op. A
  request, not a guarantee: `Ok` means the platform accepted it. The first
  explicit request replaces the Android driver's automatic bootstrap priority
  (HIGH at connect, BALANCED once bootstrap ends) for that connection; a
  reconnect starts over, so re-request after each `Up`. Also on
  `GattConnection` (Tier 1) and `DatagramChannel` (Tier 2).

Fields. `datagram` carries the wire contract (service + characteristic) and
fragmentation bounds. The current `role` field selects who dials; `Symmetric`
requires a stable identity for its ordering rule. This is existing API behavior,
not a target requirement for the caller to choose an initiator.
`retry_budget` and `max_links` have defaults.

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

**The accepting side currently holds one inbound datagram channel at a time.**
The HAL already has per-peer notification operations, but the datagram serving
implementation still enforces a single inbound channel. A second tracked peer
must wait until that channel is released or its budget expires. `max_links > 1`
does not remove this cap. Multi-inbound serving needs a separate design and
tests; it is not included in the symmetric-initiation agreement.

### Declare interest

```rust
// peer_id is the identity for the dial tiebreak — only consulted for
// LinkRole::Symmetric; pass anything comparable for the other roles.
link.track(peer_address, peer_id);   // keep a link to this peer alive
link.untrack(peer_address);          // stop; drop the channel, disconnect, forget
link.retry(peer_address);            // a tracked peer that gave up: try again now

let found: Vec<PeerAddress> = link.discover().await?;   // a bounded snapshot
// discovery is untrusted metadata — you decide which to track()
```

### Consume the event stream

```rust
// Each call is an independent subscription — events() is a broadcast, so a
// second consumer is an addition, not a breaking change.
let mut events = link.events();
while let Some(ev) = events.next().await {
    match ev {
        PeerLinkEvent::Up { peer, max_message_len, channel } => { /* a PeerChannel, valid until Down */ }
        PeerLinkEvent::Down { peer }              => { /* the channel is dead */ }
        PeerLinkEvent::Status { peer, status }    => { /* Connecting / Connected / Waiting / GaveUp / … */ }
        PeerLinkEvent::RadioChanged { status }    => { /* On | Off | Unsupported */ }
    }
}

let s = link.status(&peer);           // snapshot for a UI row, without waiting
let r = link.radio();                 // On | Off | Unsupported, without waiting
```

`PeerStatus`:

```
Untracked
Unavailable                      // radio Off / Unsupported
Queued                           // tracked, but max_links is full — waiting for a slot
Connecting                       // trying — dialing, or waiting for an inbound link
Connected
Waiting                          // a previous attempt failed; backing off before the next
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

- building and owning the platform driver (Tier 3 consumers never touch `Driver`)
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
  per-peer state machines are independent. The current Linux driver uses
  address-keyed dial/teardown locks rather than one driver-wide dial lock.
  The mock isolation test checks that a wedged peer leaves other peers able
  to connect; that test does not certify native hardware behavior.
- **The link cap is visible.** More tracked peers than `max_links` → the extra
  ones are `Queued`, not an indistinguishable `Connecting`, and promote when a
  slot frees.
- **Retry budgets are independent and do not coordinate.** N peers back off on
  their own ladders against the shared radio; that is deliberate — the only
  cross-peer arbitration is the `max_links` slot queue.

---

## Current `PeerLink` and a non-iroh consumer's state machine

A non-iroh consumer can run its own session state machine on top (auth, an
encrypted channel, a ping/ack liveness proof). This is a generic composition,
not a description of Fini's current iroh integration. The seam:

| | `ble-gatt` (`PeerLink`) | non-iroh consumer |
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

## Illustrative non-iroh application (current API)

This schematic example uses the current `PeerLink` types. Product-specific
authentication and UI functions are illustrative. It is not Fini's current
implementation and does not demonstrate the target API.

```rust
// Built where DeviceConnectionState is built — sync, infallible, no await.
let link = PeerLink::new(PeerLinkConfig {
    datagram: DatagramConfig::new(FINI_SERVICE, FINI_CHAR),
    role: LinkRole::Symmetric { local_id: my_node_id.into() },
    retry_budget: RetryBudget::default(),
    max_links: MaxLinks::default(),
});

link.track(device.ble_address, device.node_id.into());   // when a paired device is known

let mut events = link.events();
while let Some(ev) = events.next().await {
    match ev {
        PeerLinkEvent::Up { peer, channel, .. } => {
            let session = secure_channel::authenticate(channel).await?;
            sessions.insert(peer, session);
        }
        PeerLinkEvent::Down { peer } => { sessions.remove(&peer); }
        PeerLinkEvent::Status { peer, status } => { device_rows.update(peer, project(status)); }
        PeerLinkEvent::RadioChanged { status } => { device_rows.set_radio(status); }
    }
}
```

In this composition, the consumer delegates transport retry and radio recovery
to `PeerLink`. That does not prove any particular application has adopted it.
For the current iroh integration, use the adapter section above instead.

---

## Pointers

- **`docs/adr/0003`** — why the library is a carrier, not a crypto library; the
  two original tiers.
- **`docs/adr/0005`** — the owned link-state machine that makes `PeerLink`
  possible, and the "connected but isn't" bug class it removes.
- [`profile/datagram/mod.rs`](../ble-gatt/src/profile/datagram/mod.rs) — the
  fragmentation primitive and current channel limits.

## Current `PeerLink` API details, not target decisions

- **Track model:** explicit `track()` only. Discovery is untrusted; a consumer
  that wants "connect to everything advertising the service" writes a loop over
  `discover()`.
- **`RetryBudget`:** `default()` (redial ladder 1/2/4/8s cap 30s, give up after
  ~60s total, acceptor inbound deadline ~60s), fully overridable.
- **`Up` carries the peer's `max_message_len`** — a consumer sizing its own
  payloads needs it.
- **Construction:** `new(config)` builds the driver internally (a Tier-3
  consumer never touches `Driver`); `with_driver(driver, config)` injects
  one, for tests and the mock.

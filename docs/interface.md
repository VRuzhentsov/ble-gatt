# Current APIs and the target peer contract

`ble-gatt` is a **carrier**. It moves bytes over BLE GATT. The datagram profile
adds fragmentation and message boundaries, but not an application-session
protocol or end-to-end encryption (ADR-0003).

The [target under review](adr/0012-discovery-like-nearby.md) gives the core one
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

Current iroh consumers use datagrams through `ble-gatt-iroh`, not `PeerLink`.
The target must serve both without duplicate lifecycle owners. Request/event/release
signatures remain open. Losing outgoing attempts fail; incoming winners proceed
separately through acceptance. Examples below use current APIs.

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
support. See [ADR-0012's current implementation gap](adr/0012-discovery-like-nearby.md#current-implementation-gap).

Fini retains permitted peers, sync settings and application-session policy.
A losing application candidate closes its own iroh connection, **not** all
BLE links for that key while another connection uses them. The adapter's
hooks perform ordinary last-user release. Explicit `close_links` is a force
close for shutdown/revocation/abandoned connects, not candidate rejection.

## Tier 1 — raw GATT (`Driver`)

Use characteristics and MTUs directly for vendor protocols. Raw GATT adds no
message framing, CRC or authentication exchange.

```rust
let mut conn = driver.connect(&peer).await?;
let value = conn.read(SENSOR_CHAR).await?;
conn.write(COMMAND_CHAR, cmd_bytes).await?;
let mut notifications = conn.subscribe(TELEMETRY_CHAR).await?;
```

You own everything: when to reconnect, what the bytes mean, how to frame them.

---

## Tier 2 — datagram (`datagram::connect` / `datagram::serve`)

A channel carries complete opaque messages to one peer, within configured size
and reassembly bounds. It fragments messages for the negotiated MTU and restores
boundaries before delivery. Encryption remains outside the channel.

```rust
let mut channel = datagram::connect(&adapter, &peer, &config).await?;
channel.send(encrypted_message).await?;
let incoming = channel.recv().await?;
```

**You own the lifecycle:** dialing, retry, channel selection, crossed attempts
and radio recovery. Supply an `Adapter` and runtime. `DatagramChannel` holds its
`GattConnection` directly. Current `PeerLink` adds lifecycle control above this tier.

---

## Tier 3 — `PeerLink` (current tracked-peer API)

This section describes the implemented controller, not the target public
design. Its `Symmetric` role chooses the lower `LinkId` to dial. That must not
be confused with the new core protocol in ADR-0012.

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

**`PeerLink::new` is synchronous, infallible and needs no ambient runtime.**
It returns immediately and starts one OS thread with its own Tokio runtime.
The worker owns the driver and per-peer operations. Asynchronous adapter
acquisition failures appear through `radio()` as `Off` or `Unsupported`, not constructor errors.

`PeerLinkEvent::Up` supplies a `PeerChannel` message-passing handle. Its `send` /
`recv` cross to the worker for GATT operations, independently of the consumer's
runtime. Tier 2 instead runs its owned `GattConnection` on the caller's runtime.

The handle's contract across that hop:

- **`send` preserves typed `BleError` values.** Keep retryable `BleError::GattBusy`
  distinct from permanent refusal across the worker boundary (ADR-0005).
- **`recv()` returns `None` after link closure or worker death**, rather than hanging.
- **A full send queue returns `BleError::GattBusy`**, so callers can apply the same retry policy.
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

`max_links` caps live peers. Additional tracked peers report `PeerStatus::Queued`
until disconnect or `untrack()` frees a slot. The default is 4; increase it only
when the platform permits more concurrent links.

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

A public view of internal state, not the state machine itself.

**Both roles can reach `GaveUp`.** Dialers exhaust the redial budget; acceptors
exhaust the inbound wait budget. Both report exhaustion instead of remaining
`Connecting` indefinitely. Consumers display `GaveUp` and use `retry(peer)` to restart.

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
- **non-radio preconditions:** network availability, paired addresses, per-pair
  settings and application OS-bond checks. `PeerLink` reports radio state, not
  eligibility for other transports.
- everything **above the byte boundary**: authentication, encryption, and any
  app-level liveness proof. The library carries bytes; it cannot know your
  peer's app has stopped answering, only that the BLE link exists.

### Multiple peers

`track()`, `status()` and events operate per peer:

- **Per-peer isolation.** One unresponsive peer does not stall the others. The
  per-peer state machines are independent. The current Linux driver uses
  address-keyed dial/teardown locks rather than one driver-wide dial lock.
  The mock isolation test checks that a wedged peer leaves other peers able
  to connect; that test does not certify native hardware behavior.
- **The link cap is visible.** More tracked peers than `max_links` → the extra
  ones are `Queued`, not an indistinguishable `Connecting`, and promote when a
  slot frees.
- **Retry budgets are independent.** Each peer backs off separately against the
  shared radio. Only the `max_links` slot queue coordinates peers.

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

`PeerLinkEvent::Up` starts application authentication; `Down` ends the session.
The consumer displays `GaveUp` and calls `retry()` on request. It does not
duplicate exhaustion tracking, dial loops, backoff maps or crossed-attempt control.
Pairing frames, pairing UI, stored eligibility and non-radio preconditions remain application concerns.

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

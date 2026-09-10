# How `ble-gatt` is meant to be used

> **Status: DRAFT for review.** This describes the *intended* shape. Tier 3
> (`PeerLink`) does not exist yet — it is the subject of `docs/adr/0005`.
> Tiers 1 and 2 exist today.

`ble-gatt` is a **carrier**. It moves bytes between two devices over BLE GATT
and does not interpret them — no framing, no session protocol, no encryption
(`docs/adr/0003`). This document is about *which shape of carrier* a given
consumer should reach for.

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
from an adapter power-cycle. For a one-shot transfer or a test that is fine. For
a long-lived app-to-app relationship it is a lot of machinery to get right —
which is why Tier 3 exists.

---

## Tier 3 — `PeerLink` — the intended default for app-to-app

You declare **which peers you care about** and **one policy** (who dials). The
library owns keeping links alive; you consume channels and status.

### Configure once

```rust
let link = PeerLink::start(backend, PeerLinkConfig {
    service:        MY_SERVICE,
    characteristic: MY_CHARACTERISTIC,
    role:           LinkRole::Symmetric { local_id: my_node_id.into() },
    limits:         Default::default(),
    redial_backoff: Some(BackoffLadder::default()),
}).await?;
```

Four fields. `service` + `characteristic` are the wire contract. `role` is the
one genuine protocol decision — who dials for a pair that can both see each
other — and has no sensible default, because it needs a stable identity both
peers compare the same way (glare; `docs/adr/0003` revision). `limits` and
`redial_backoff` have defaults.

| `LinkRole` | Behaviour |
|---|---|
| `Symmetric { local_id }` | dials tracked peers *and* accepts inbound; `local_id` breaks the dial tie |
| `DialOnly` | only ever dials (central-only platform, a hub) |
| `AcceptOnly` | only ever advertises and accepts (a peripheral) |

### Declare interest

```rust
link.track(peer_address);     // keep a link to this peer alive
link.untrack(peer_address);   // stop; drop the channel, disconnect, forget

let mut found = link.discover().await?;   // peers advertising the service
// discovery is untrusted metadata — you decide which to track()
```

### Consume one event stream

```rust
while let Some(ev) = link.events().next().await {
    match ev {
        PeerLinkEvent::Up { peer, channel }   => { /* a message pipe, valid until Down */ }
        PeerLinkEvent::Down { peer, reason }  => { /* the channel is dead */ }
        PeerLinkEvent::Status { peer, status } => { /* Connecting / Connected / Waiting / … */ }
        PeerLinkEvent::RadioOff => { /* every link is down */ }
        PeerLinkEvent::RadioOn  => { /* links to tracked peers will be re-established */ }
    }
}

let s = link.status(&peer);   // snapshot for a UI row, without waiting
```

`PeerStatus`: `Untracked | Unavailable | Connecting | Connected | Waiting {
retry_at } | GaveUp`. A projection of the library's internal state — you never
see the state machine.

### The library owns

- dialing tracked peers (when `role` and the tiebreak allow)
- redialing on drop, with backoff
- accepting inbound links
- exactly one link per peer; resolving glare via `role`
- tearing everything down on radio-off and re-establishing on radio-on
- surfacing an honest per-peer status

### The consumer still owns

- **which** addresses to `track` (discovery is untrusted; you choose)
- the **identity** used for the dial tiebreak (`local_id`)
- everything **above the byte boundary**: authentication, encryption, and any
  app-level liveness proof. The library carries bytes; it cannot know your
  peer's app has stopped answering, only that the BLE link exists.

---

## `PeerLink` and a consumer's own state machine

A consumer like Fini runs its own session state machine on top (auth, an
encrypted channel, a ping/ack liveness proof). The seam:

| | `ble-gatt` (`PeerLink`) | consumer (e.g. Fini's `LinkState`) |
|---|---|---|
| Owns | transport: dial, connect, drop, redial, radio | session: auth, proof, fade |
| Driven by | platform callbacks | `PeerLinkEvent::Up` / `Down` + its own ping ticks |
| "Link nominally up but peer silent" | not visible — a dead BLE link is just `Down` | its own `Fading` state |

`PeerLinkEvent::Up` starts the consumer's auth exchange; `Down` ends its
session. A consumer's machine keeps only what is above the byte boundary; the
dial / backoff / precondition machinery moves into `ble-gatt`.

---

## Worked example — Fini

```rust
let link = PeerLink::start(backend, PeerLinkConfig {
    service: FINI_SERVICE, characteristic: FINI_CHAR,
    role: LinkRole::Symmetric { local_id: my_node_id.into() },
    limits: Default::default(),
    redial_backoff: Some(BackoffLadder::default()),
}).await?;

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

- Does `PeerLink` fully replace Tier 2's public role, or sit strictly above it?
  (lean: sit above; Tier 2 stays public.)
- `discover` + explicit `track`, vs. an opt-in "auto-track anything advertising
  the service" mode. (lean: explicit only.)
- Backoff ladder: a fixed default (1/2/4/8s, 30s cap) plus override, vs.
  consumer-supplied only. (lean: default + override.)
- Whether `PeerLink` exposes the negotiated MTU / `max_message_len` per peer.
  (lean: yes, on `Up` — a consumer sizing its own payloads needs it.)

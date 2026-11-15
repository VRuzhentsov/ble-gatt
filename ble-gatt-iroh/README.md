# ble-gatt-iroh: iroh over BLE messages

`ble-gatt-iroh` adapts [`ble-gatt`](../ble-gatt/README.md) to the external
[iroh](https://docs.iroh.computer/concepts/endpoints) network stack. It is one
of the three crates in this workspace. iroh itself is not a workspace crate.

## Status: current code and agreed target

The [shared design under review](../docs/adr/0012-discovery-like-nearby.md)
assigns fundamental symmetric BLE behavior to the core. The current adapter
still contains initiation policies and BLE coordination that need review.
This document records the boundary; it does not claim that relocation is done.

## Target responsibility

This crate translates between two interfaces:

- The core's BLE connections and complete-message channels.
- iroh's custom addresses and QUIC packet send/receive interfaces.

It associates authenticated iroh peers with BLE destinations and tracks iroh
users of a carrier. When no iroh user needs the carrier, it releases its use
through the core's ownership contract. Native connection establishment,
competing-attempt handling and teardown safety belong in `ble-gatt`.

The adapter requests use/release through the core rather than independently
dialling, retrying or disconnecting the same native resource. iroh hooks and
path/user bookkeeping remain here where they describe iroh facts, not OS state.

## Who owns what

- **Application:** permitted peers, discovery intent, pairing, ALPN protocols,
  sync settings and application-session acceptance.
- **iroh:** authenticated QUIC connections, encryption, streams and paths.
- **This adapter:** custom packet/address translation and iroh-specific carrier
  usage accounting.
- **Core `ble-gatt`:** generic BLE peer behavior, complete messages, connection
  ownership and native drivers.

A peer key is authenticated identity. A BLE address is a current destination.
A short advertisement fingerprint is a discovery hint, not authentication.

## Packet flow

```text
Application protocol
        |
iroh streams / QUIC + TLS
        |
BleGattTransport: custom packets and addresses
        |
ble-gatt: complete messages and BLE ownership
        |
GATT operations / platform driver
```

One QUIC datagram is carried as one complete BLE message. The core can fragment
that message into smaller GATT values and reassemble it before delivery.
The adapter does not implement a second QUIC handshake or application protocol.

## Current API and migration gap

Implemented entrypoints in [`src/lib.rs`](src/lib.rs) include:

- `BleGattTransport::builder` and `dial_with` for an outgoing datagram channel.
- `attach` and `attach_after` for incoming channels, including a saved first
  packet supplied by an application router.
- `set_peer_address` for discovered destinations.
- `address_lookup` and `hooks` for iroh identity/path integration.
- `close_link` and `close_links` for explicit force-close operations.

The current transport does not scan or advertise by itself. Its caller supplies
scan results and channels from `datagram::serve`. The future discovery/request
API is not chosen by this documentation change.

Current `InitiationPolicy` variants are implementation history, not the target
public design:

- `PreviousDialer` is the current default and can yield after incoming loss.
- `EitherPeer` is the current explicit no-yield mode.
- `Ordered` restricts initiation by endpoint-key order.

The target removes that public choice rather than only changing its default.
The same symmetric behavior must be available to a non-iroh core consumer.
See [ADR-0012](../docs/adr/0012-discovery-like-nearby.md) for the target and its
current implementation gap. The current adapter retains crossed candidates;
the target core instead arbitrates pending attempts. Its losing outgoing
request reports failure while an incoming winner proceeds separately.
Do not turn that outcome into a silent outgoing-to-incoming success handoff.

Do not place the existing `PeerLink` controller under this adapter without a
contract change. It has a different acceptance policy and a different channel
type. Reusing existing core mechanisms requires integration, not nested owners.

## Custom-transport contract

The adapter implements iroh's `CustomTransport`, `CustomEndpoint` and
`CustomSender` traits. It binds a packet endpoint, reports local addresses,
receives packets with address metadata and sends packets to custom addresses.
Both endpoints need compatible transport support.

The extension is enabled by `unstable-custom-transports`. This is an upstream
API-stability warning: an iroh update can require adapter changes. It is not a
statement that BLE or encryption is inherently unreliable. Keep this coupling
here, review dependency updates, and test the resolved version.

The manifest's `iroh = "1.3"` is a compatible version requirement, not an exact
pin. Consult the resolved lockfile and version-matched API documentation.
The packet carrier must support QUIC's minimum 1,200-byte datagram; native GATT
writes may be smaller because the core supplies fragmentation.

Upstream references:

- [CustomTransport](https://docs.rs/iroh/1.4.1/iroh/endpoint/transports/trait.CustomTransport.html)
- [CustomEndpoint](https://docs.rs/iroh/1.4.1/iroh/endpoint/transports/trait.CustomEndpoint.html)
- [CustomSender](https://docs.rs/iroh/1.4.1/iroh/endpoint/transports/trait.CustomSender.html)
- [Custom transport packet requirements](https://www.iroh.computer/blog/tor-custom-transport)

## Lifetimes and validation

A QUIC connection, its current path, a historical address mapping and a BLE
candidate task have different lifetimes. An application rejecting one QUIC
connection must not force-close another user's shared carrier. Current hooks
separate current use from history; history alone must not retain idle BLE use.

The shared diagnostic in
[`examples/support/iroh_diagnostic.rs`](examples/support/iroh_diagnostic.rs)
is used by the Linux entrypoint, the Tauri sample and mock integration tests.
It is example/test support, not part of the production transport API.
An empty linked-peer map, joined candidate tasks, reported native-session
closure and exhaustive OS teardown are distinct claims.

See the [architecture map](../docs/architecture.md),
[usage guide](../docs/interface.md) and
[hardware acceptance checklist](../docs/hardware-verification.md).

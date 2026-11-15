# ble-gatt-iroh: iroh over managed BLE messages

`ble-gatt-iroh` carries iroh QUIC packets over core `PeerChannel` messages.
iroh authenticates public keys with TLS and supplies encryption, streams and paths.
The adapter does not authenticate a Bluetooth address or core endpoint identifier.

## Ownership

- Applications choose discovery, advertising, permitted peers and incoming acceptance.
- Core `PeerLink` owns managed metadata, establishment, crossed attempts, liveness and native cleanup.
- This adapter translates packets and addresses. It tracks authenticated QUIC users and their current paths.
- iroh owns QUIC connections. Several QUIC connections can share one core carrier.

Raw GATT and raw datagram services remain separate. Do not pass their channels to this adapter.
Both peers need the managed core protocol. There is no legacy downgrade or initiation-policy switch.

## Setup

Create one shared core worker for the driver and managed service configuration:

```rust,ignore
let peer_link = PeerLink::with_driver(driver, PeerLinkConfig::new(datagram_config));
let mut incoming = peer_link.incoming()?;
peer_link.set_advertising(true).await?;

let transport = BleGattTransport::builder()
    .dialer(dial_with(peer_link.clone()))
    .build();
let attached = transport.clone();
tokio::spawn(async move {
    while let Some(request) = incoming.next().await {
        // Apply the application's acceptance policy here.
        match request.accept().await {
            Ok(channel) => { attached.attach(channel); }
            Err(error) => log::debug!("incoming BLE attempt failed: {error}"),
        }
    }
});

let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
    .clear_ip_transports()
    .add_custom_transport(Arc::new(transport.clone()))
    .address_lookup(transport.address_lookup())
    .hooks(transport.hooks())
    .bind().await?;
```

Use `peer_link.discover()` to find managed peers. Record a permitted iroh key's
current destination with `transport.set_peer_address(key, peer.address)`.
A discovery hint does not prove the key. TLS verifies that key during the QUIC handshake.
Stopping discovery or advertising does not release established channel use.

`dial_with(Arc<PeerLink>)` first acquires an existing channel. Otherwise, it requests
communication and accepts its outgoing attempt. The remote application must also accept.
An outgoing BLE loser returns an error. The application accepts and attaches an
incoming winner separately. The adapter does not report that winner as outgoing success.
Queued packets can use the independently attached carrier. QUIC retransmits lost packets.
A BLE attempt failure does not force a legitimate QUIC connection to fail.

## Packet and user lifetimes

One QUIC packet becomes one complete core message. Core fragmentation handles smaller GATT values.
Queues are bounded. Overflow remains packet loss, which QUIC can recover.
Only one packet task reads an address's channel. A replacement waits for the old receiver to stop.
The adapter does not retain competing native candidates or choose their winner.

Install `hooks()` on the endpoint. Hooks use weak connection handles and reconcile
current path snapshots. Historical addresses remain available for close barriers and
late-packet suppression. History alone does not retain idle channel use.
A current QUIC user protects the shared carrier from the adapter's idle timer.
Ending one user preserves other current users.

For local QUIC close, release follows the queued close-frame barrier.
Queue loss or a failed barrier cannot prove that the close frame was sent.
The last legitimate user releases the adapter's scoped core use with `PeerChannel::release`.
It never directly disconnects a native channel. Other core users remain valid.
`close_link` and `close_links` release this transport's use and cancel its pending requests.
Use the core's explicit disconnect API when the application intends a global disconnect.

### Terminal disconnect and a new application user

The adapter reads `PeerChannel::close_reason()` before it closes the matching packet queue.
`LocalDisconnect`, `PeerDisconnect`, `ProtocolError`, and `Shutdown` suppress automatic acquisition for that authorization generation.
Existing QUIC packets cannot authorize another generation, regardless of their contents.
`LinkLost` remains recoverable. Scoped release is not a global disconnect.

A fresh application core request, or independently accepted incoming request, can establish a channel.
Pass that live channel to `attach` to authorize its replacement packet generation.
With hooks installed, a new explicit `Endpoint::connect` after terminal closure also authorizes acquisition.
Retransmissions do not invoke `before_connect`. Adding a user to a healthy carrier does not reset its generation.
Without hooks, use explicit core acquisition and attachment to resume after terminal closure.

This is the existing authorization contract, not a new recovery mode.
Integrated proof of the idle-close/new-authorization race remains pending.

Authorization is shared by users of the address, not inferred from packet headers.
Once the application authorizes a replacement, existing users may also send through it.
Stale packet-task cleanup cannot suppress or remove that replacement.
No initiation mode or independent native retry controller is involved.

## Examples and checks

`examples/hw_iroh_peer.rs` uses the shared diagnostic in
`examples/support/iroh_diagnostic.rs`. It uses managed discovery and independent
incoming acceptance. `tests/hardware_diagnostic.rs` runs this diagnostic over the mock radio.
These software tests do not establish physical radio or OS teardown behavior.

Run:

```sh
cargo test -p ble-gatt-iroh --all-targets
cargo clippy -p ble-gatt-iroh --all-targets -- -D warnings
```

The crate implements iroh's unstable custom-transport interface. Its compatible
version requirement is not an exact pin. Check the resolved lockfile when updating iroh.
See [ADR-0012](../docs/adr/0012-discovery-like-nearby.md) for shared responsibilities
and the [hardware checklist](../docs/hardware-verification.md) for native acceptance limits.

# BLE APIs and peer ownership

`ble-gatt` carries bytes. It does not authenticate or encrypt them.
[ADR-0012](adr/0012-discovery-like-nearby.md) describes the managed protocol
implemented in this PR. Software checks do not establish hardware acceptance.

## Choose an API

| Requirement | API | Lifecycle owner |
|---|---|---|
| Vendor-specific characteristics | `Adapter::central` / `peripheral`, `Driver` | Caller |
| Complete messages without managed establishment | `datagram::connect` / `serve` | Caller |
| Discovery, mutual acceptance and scoped messages | `PeerLink` | One core worker |
| QUIC over managed messages | `ble-gatt-iroh` | Core worker; adapter accounts for QUIC users |

Raw GATT and datagram APIs remain independent. They do not add the managed
handshake. Do not run another lifecycle controller over the same native service.
The core works in a standalone CLI without iroh or Tauri; see the
[core crate](../ble-gatt/README.md). Android still needs bridge packaging and context.

## Managed peer communication

`PeerLink::new(PeerLinkConfig::new(config))` starts one dedicated worker thread
with its own Tokio runtime. `with_driver(driver, config)` injects a driver.
Construction does not wait for platform acquisition and does not prove radio
availability. The worker coordinates attempts and reuses datagram fragmentation.
It replaces the old tracking/ranking worker contract, not another manager beside it.

`PeerLinkConfig` contains `datagram` and `max_links` (`MaxLinks`, default 4).
There are no public tracking, retry-budget or initiation-policy modes.
`PeerLink` is the retained implementation name, not an application naming requirement.

### Minimal outgoing use

This function uses the current API. The caller supplies a discovered address
and profile configuration. The remote application must accept through `incoming()`.

```rust
use ble_gatt::{datagram::DatagramConfig, PeerAddress, PeerLink, PeerLinkConfig};

async fn send_once(config: DatagramConfig, peer: PeerAddress) -> ble_gatt::Result<()> {
    let link = PeerLink::new(PeerLinkConfig::new(config));
    let result = async {
        let request = link.request(&peer).await?;
        let channel = request.accept().await?;
        channel.send(b"hello".to_vec()).await?;
        channel.release().await
    }
    .await;
    let cleanup = link.close().await;
    result.and(cleanup)
}
```

`PeerLink::new` is available on Linux, Android and Windows. Use `with_driver`
for radio-free tests. The function is a usage example, not an executed device test.

### Discovery and incoming requests

- `set_advertising(true).await` starts managed serving and publishes endpoint metadata.
- `discover().await` returns a stream of `Result<(EndpointId, DiscoveredPeer)>`.
  Drop the stream to stop discovery. Only one managed discovery stream can be active.
- `request(&peer).await` returns outgoing initiation information, not an established channel.
- `incoming()` returns the bounded `ConnectionRequest` stream. Claim it once,
  before serving; another call returns `AdapterBusy`.
- `ConnectionRequest::endpoint_id()` and `peer()` report correlation information,
  not authenticated identity. Apply application eligibility and acceptance here.
- `accept(self).await` waits for both sides to accept and returns `PeerChannel`.
  `reject(self).await` rejects that attempt. Dropping a request cancels only its attempt.

Consume incoming requests with `tokio_stream::StreamExt::next`. Accept a winning
incoming request independently when a crossed outgoing request fails. Never
report the incoming winner as success for the losing outgoing attempt.

Managed discovery completes missing profile metadata through GATT. It reuses
known metadata for an owned attempt rather than opening a replacement session.
Each discovery allows one bounded probe per address, up to 64 addresses.
A fresh discovery resets that probe set. Raw scans do not inherit completion.

### Messages and closure

`PeerChannel` provides `send(Vec<u8>)`, `recv()`, `max_message_len()`, `peer()`
and `endpoint_id()`. `recv().await` returns `Option<Result<Vec<u8>>>`.
Clones share one receive queue; they are not broadcast subscriptions.
`PeerLink::channel(&peer)` acquires an existing live scoped use without dialing.
A full send queue reports `BleError::GattBusy`; oversize messages fail validation.

A full application receive queue pauses that generation's transport task, not
the shared worker. Other peers, native sends and control commands remain responsive.
The task retains at most one pending record outside the existing bounded queues.
Queue pressure alone does not report loss. Admission retains the existing
five-second I/O bound; expiry ends the generation. Native end or generation
cancellation interrupts pending admission without waiting for application capacity.
Clones still share one receiver and one receive-gap signal.

- `release(self).await` releases one scoped use. The last use awaits retirement,
  including matching native cleanup that has already started.
  Dropping the last handle requests asynchronous retirement instead.
- `channel.disconnect().await` invalidates that channel generation, including
  its other handles, and awaits its retirement. It cannot disconnect a later replacement generation.
- `link.disconnect(endpoint).await` ends current attempts/channels for that core endpoint
  and awaits their matching retirements, including those already in progress.
- `set_advertising(false).await` hides the advertisement but retains serving,
  established sessions and subscriptions. It is not destructive server stop.
- `close().await` ends worker communication and stops serving.

A new request can report `AdapterBusy` while an earlier generation owns the
address. Before reusing it, await that old local generation's last release or
disconnect. A remote peer's completed disconnect is not a local cleanup barrier.
These waits cover owned software cleanup, not physical OS/radio teardown.

`close_reason()` is set before receive ends. Reasons are `LocalDisconnect`,
`PeerDisconnect`, `LinkLost`, `ProtocolError`, `Released` and `Shutdown`.
Attempt failures use `BleError::Attempt(AttemptError)`, including rejection,
cancellation, supersession, equal nonces, timeout and peer loss. Native errors
and malformed-profile `BleError::Protocol` remain distinct.

Establishment has a 30-second deadline, including acceptance. Established links
send five-second keepalive probes and detect loss after 30 seconds without valid
traffic. A finished message does not close a healthy channel. A terminal attempt
does not restart automatically. New communication requires a new request.

`request_connection_priority` forwards a platform request on a live channel.
Unsupported platforms or roles return `BleError::Unsupported`; success is not a
latency guarantee.

## iroh integration

Use one shared `PeerLink` with `ble_gatt_iroh::dial_with(link.clone())`.
Accept incoming requests separately and pass established channels to
`BleGattTransport::attach`. Install the transport's `hooks()` on the iroh endpoint.
See the [adapter setup](../ble-gatt-iroh/README.md#setup) for the full composition.

The adapter translates packets and addresses. It retains authenticated key,
current-path and multi-user accounting, not a second native arbitration loop.
Ending one QUIC user releases only its use. Other users keep the carrier.

The adapter reads the core close reason before retiring its packet queue.
Explicit local/peer disconnect, protocol failure and shutdown block automatic
acquisition for that authorization generation. Old QUIC retransmissions cannot
authorize replacement. Unexpected `LinkLost` remains recoverable.
A fresh explicit iroh connect with hooks, or a separately acquired and attached
core channel, authorizes a replacement. Scoped release is not global disconnect.
This is the existing authorization contract, not a new recovery mode.

## Raw APIs and lifetime limits

Raw `Central::connect` returns an owned `Connection` for `read`, `write` and
`subscribe`. Its Drop schedules cleanup when a Tokio runtime is available;
explicit disconnect can be awaited. `Peripheral::serve` returns `ServerHandle`:
`stop` awaits shutdown, while Drop requests background shutdown.

`datagram::connect(&adapter, &peer, &config).await` returns `DatagramChannel`.
Its `send` and `recv` preserve complete message boundaries. The caller owns
dialing, retries, selection and recovery. Dropping the `datagram::serve` output
stream stops channel delivery, not the service or existing channels.
Use `datagram::stop_serving` for destructive shutdown.

### Driver event streams and receive loss

Subscribe to `Driver::lifecycle_events()` before acquiring a session.
Linux, Android, Windows and mock drivers isolate this bounded stream from
characteristic writes at publication. The mock broker preserves the source stream;
`ManagedDriver` and Tauri's lazy driver forward it separately.

`Driver::events()` and `Adapter::events()` retain the legacy all-events stream:
radio/connection events and inbound characteristic writes. Repeated `Connected`
reports remain possible. Client notifications use `GattConnection::subscribe`, not
this stream. The isolated stream suppresses known duplicate session reports.

Third-party drivers inherit a default that filters writes from `events()`.
That fallback does not isolate storage. Its unknown `GattEvent::Lagged` must
remain critical; filtering cannot prove that only payloads were lost.

Raw datagram serving reads lifecycle and payload streams separately. Known payload
loss reports `BleError::ReceiveGap`; later complete messages can still arrive.
Critical `Lagged` or lifecycle-stream end terminates the affected raw channel;
it is not recoverable payload loss. Broker transport loss also reports critical loss.
Bounded lifecycle storage can overflow; payload isolation does not promise lossless lifecycle delivery.

Logical retirement, reported native session closure and physical OS/radio teardown
are separate evidence. WinRT peripheral exclusion cannot prove physical disconnect.
See [hardware verification](hardware-verification.md) and
[ADR-0011](adr/0011-windows-winrt-driver.md) before making platform claims.

## Source references

- [Public exports](../ble-gatt/src/lib.rs)
- [Driver event ports](../ble-gatt/src/hal/mod.rs)
- [Bounded native/mock event publication](../ble-gatt/src/drivers/event_bus.rs)
- [Requests and scoped channels](../ble-gatt/src/connection/peer_link.rs)
- [Worker and attempt ownership](../ble-gatt/src/connection/managed_worker.rs)
- [Managed record codec](../ble-gatt/src/connection/managed_codec.rs)
- [Raw datagram profile](../ble-gatt/src/profile/datagram/mod.rs)

ADR-0005 remains a historical record. Its unused crate-private `link_state`
implementation is retired; it is not the managed worker's live state machine.

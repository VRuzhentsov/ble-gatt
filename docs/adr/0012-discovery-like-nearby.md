# 0012: Nearby-inspired BLE peer communication

## Status

PR #38 implements the selected core protocol and adapter migration. Software checks
and hardware acceptance are separate. Keep the PR draft until its acceptance gates pass.

This ADR owns the shared contract. [ADR-0011](0011-windows-winrt-driver.md) owns
Windows-native choices. Earlier accepted ADRs retain their historical contracts.

## Decision

Provide discovery, establishment and complete messages in reusable `ble-gatt`,
without iroh or Tauri. Follow selected Google Nearby mechanisms through our own
BLE protocol, not Google SDK or wire-protocol interoperability. Keep raw GATT
and caller-owned datagrams separate for vendor and other protocols.

### 1. Responsibilities

- **Core `PeerLink`:** managed discovery, requests, mutual acceptance, arbitration,
  message bounds, liveness and native channel-use ownership.
- **Drivers:** OS objects, callbacks, serialized native operations and session/generation guards.
- **`ble-gatt-iroh`:** packet/address translation, authenticated-key mappings and current QUIC user/path accounting.
- **External iroh:** QUIC authentication, encryption, streams and path selection.
- **Tauri plugin:** host access, permissions, lazy construction and Android bridge/context setup.
- **Applications:** permitted peers, pairing, acceptance and application protocols.

Refactor the existing dedicated core worker; do not add another manager.
Adapters acquire/release core use instead of independently arbitrating native
connections. The unused crate-private `link_state` implementation is retired.
ADR-0005 remains historical, not the current managed state-machine specification.

`Adapter` supplies raw radio/role access. Central and peripheral are connection
roles, not permanent application identities. Writes and notifications already
provide bidirectional messages. Separate role capabilities do not prove native
simultaneous-role or crossed-attempt support.

### 2. Discovery and metadata

**Reference.** Nearby separates discovery from connection requests.[5][9]
Its BLE tracker retrieves full metadata through GATT and caches successful reads.[2]
Windows can use an alternate-UUID advertisement to trigger retrieval.[3]

**Implementation.** Managed serving inserts version/capability and endpoint bytes
into `DatagramConfig::advertised_service_data`. Initial readable GATT metadata
contains the same service data. Managed discovery uses radio data when present;
otherwise, it performs a bounded GATT read. It reuses known metadata for owned
attempts rather than opening a replacement session.

Each discovery permits one five-second probe per address, up to 64 addresses.
A fresh discovery resets the probe set. This is not Nearby's header/hash cache
protocol. Raw scanning remains raw; `advertisement::complete` is an opt-in helper
with separate timer-refresh and bounded retry behavior.

A metadata read is not accepted message communication. Release only resources
owned by the read. Acquired-resource Drop cleanup is asynchronous and needs a
running Tokio runtime. Cancellation during native acquisition still needs validation.

WinRT can include `ServiceData` when space permits and report
`StartedWithoutAllAdvertisementData`.[1] Select completion from received profile
information, not the OS name. Metadata value/radio updates are separate operations,
not an atomic transaction; coherent generations and partial failure remain review gates.

### 3. Requests, acceptance and scoped use

**Reference.** Nearby requires both sides to accept; either can reject.[5]
Stopping discovery/advertising preserves established communication.[10]

```text
advertise / discover -> discovered peer -> request
    -> acceptance on both sides -> bidirectional messages -> disconnected
```

**Implementation.** `PeerLinkConfig::new(datagram)` configures one worker;
`max_links` defaults to four. `PeerLink::new` starts the worker runtime;
`with_driver` injects a driver. Construction does not prove platform availability.

- `discover().await` returns a stream of endpoint IDs and discovered peers.
- `incoming()` claims one bounded request stream.
- `request(&address).await` returns `ConnectionRequest`, not an established channel.
- `accept(self).await` waits for both decisions and returns `PeerChannel`.
  `reject(self).await` rejects that attempt. Drop cancels only that request.
- `channel(&address)` acquires a live scoped use without another dial.
- Channel clones share a receive queue, not independent subscriptions.
- `release(self).await` releases one use and awaits retirement of the last use.
  Dropping the last handle requests asynchronous retirement.
- Channel `disconnect().await` invalidates its generation, not a later replacement.
  Link `disconnect(endpoint).await` ends current communication for that endpoint.
- `set_advertising(false).await` changes visibility, not server ownership.
  `close().await` ends communication and stops serving.

There is no public tracking, ranking, retry-budget or initiation-policy mode.
A terminal request does not restart automatically. Finishing a message does not
close a healthy channel. Establishment, including acceptance, has a 30-second
bound. Keepalive probes run every five seconds; 30 seconds without valid traffic
ends the channel. Native events complement these records.[11]

Each generation's existing transport task waits for bounded application receive
capacity, with at most one pending record outside the queues. The shared worker
does not wait for that capacity. Native sends, other peers and control remain
responsive. Admission keeps the five-second I/O bound; native end or retirement
interrupts it. Queue pressure alone is not receive loss.

Native/mock drivers isolate bounded `Driver::lifecycle_events()` from payloads;
broker and wrapper forwarding preserve the source stream. Legacy `events()`
still includes characteristic writes. The third-party default only filters that
legacy stream, so unknown `Lagged` remains critical. Known payload `ReceiveGap`
allows later messages; critical lifecycle loss ends the affected channel.

The datagram server still admits one inbound peer. `MaxLinks` is not proof of
Cluster/M-to-N support. Native failures before request exchange cannot be repaired
by nonce arbitration.

### 4. Endpoint correlation and arbitration

**Reference.** Nearby requests carry an endpoint ID and fresh signed 32-bit
nonce.[4][13] Endpoint lifetime is separate from native address lifetime.[12]

**Implementation.** A fresh 128-bit `EndpointId` identifies one worker lifetime.
Discovery and handshake correlate the same endpoint. Endpoint IDs, wire attempt
IDs and native session generations are distinct; none authenticates the peer.

For crossed requests to the same endpoint:

- Larger signed nonce wins; close only the losing attempt.
- Equal nonces fail both. Generate fresh values for fresh attempts.
- A duplicate local request does not dial again.
- Duplicate incoming requests do not re-arbitrate an established endpoint.
- The outgoing loser reports failure. The incoming winner proceeds separately
  through acceptance; it must not silently fulfill the outgoing caller.[4]

Generation checks protect replacements from stale effects. Native guards and
serialization remain necessary around the effects themselves. Logical WinRT
exclusion does not prove physical disconnection.

### 5. Selected protocol and typed outcomes

The authoritative encoding is [managed_codec.rs](../../ble-gatt/src/connection/managed_codec.rs).
Records use existing datagram fragmentation/reassembly. Numeric fields are
big-endian. Endpoint fields are 16 raw bytes; attempts are unsigned 64-bit values.
Nonces are signed 32-bit values and use signed comparison.

Managed metadata is exactly 20 bytes: ASCII `BG`, version `1`, capability `1`,
then the endpoint ID. Unsupported managed metadata fails closed; there is no
legacy downgrade. Record lengths include the tag:

| Tag | Record | Fields after tag | Length |
|---|---|---|---|
| 1 | Request | version, capability, sender, target, attempt, nonce | 47 bytes |
| 2 | Response | sender, target, attempt | 41 bytes |
| 3 | Decision | attempt, action: accept=0, reject=1, cancel=2 | 10 bytes |
| 4 | Data | attempt, opaque payload | 9 bytes plus bounded payload |
| 5 | Disconnect | attempt | 9 bytes |
| 6 | Ping | attempt | 9 bytes |
| 7 | Pong | attempt | 9 bytes |

Version/capability fields occur in metadata and Request, not every record.
Reject malformed lengths, unknown tags and invalid decisions. Validate endpoint
and attempt correlation before establishment or delivery. Application payloads
have their own tag; never infer control from QUIC or arbitrary payload prefixes.

`BleError::Attempt(AttemptError)` preserves typed terminal attempt outcomes:
`Rejected`, `Cancelled`, `Superseded`, `EqualNonce`, `TimedOut`, `PeerLost`,
`Disconnected`, `PeerDisconnected`, `Released`, `QueueFull`, `Closed` and
`InvalidRecord`. Native errors and malformed-profile `BleError::Protocol` remain distinct.

`PeerChannelCloseReason` is set before receive closes: `LocalDisconnect`,
`PeerDisconnect`, `LinkLost`, `ProtocolError`, `Released` or `Shutdown`.
Attempt failure and established-channel closure are separate facts.

The adapter blocks automatic acquisition after local/peer disconnect, protocol
failure or shutdown for that authorization generation. Old QUIC packets cannot
authorize a replacement, regardless of payload. Unexpected `LinkLost` remains
recoverable. Scoped release is not global disconnect.

A fresh explicit iroh connect with hooks, or an independently acquired and
attached core channel, authorizes replacement. Several authenticated QUIC users
can share it. Ending one user must preserve the others. Stale packet-task cleanup
must not suppress or remove the replacement.

This remains the existing application-authorization contract, not a new mode.

Correlation and acceptance provide neither authentication nor encryption
([ADR-0003](0003-carrier-not-crypto-and-the-two-api-tiers.md)). iroh supplies security
in its composition; other consumers need their own layer. Unauthenticated peers
can forge correlation fields. Bounds limit resource use, not hostile identity
claims. Borrowed arbitration does not provide Nearby's complete security guarantees.

## Alternatives and consequences

- Keeping initiator modes places generic BLE behavior in an optional integration.
  Permanent identity ranking prevents either-peer initiation. Neither is selected.
- Nested controllers duplicate native retry/teardown ownership.
- The full Nearby SDK/protocol exceeds the selected own-protocol scope.
- Returning an incoming winner as outgoing success changes attempt-level semantics.

The selected API intentionally replaces the old tracked-peer and adapter-policy
surfaces. Raw vendor APIs remain separate. See [current usage](../interface.md).

<a id="current-implementation-gap"></a>

## Migration and verification gates for this PR

The working tree now contains the handshake, managed adapter migration, initial
service-data publication, native visibility port and Android ABI 7 forwarding.
These are source observations, not final acceptance results.

Before acceptance, verify:

1. Non-iroh establishment, both initiators, both nonce orderings, equality,
   rejection, duplicate requests and typed outgoing failure with separate incoming success.
2. Acquisition cancellation, bounded metadata failures, active-user safety,
   stale generations, discovery/visibility stop, idle liveness and terminal disconnect.
3. Scoped multi-user release, current-versus-historical paths and adapter
   terminal authorization. Retransmissions must not undo disconnect.
4. Native ports, mock/broker forwarding, Tauri wrappers, compiled JNI descriptors,
   APK packaging and actual consumer readiness. Source parity alone is insufficient.
5. Metadata generation/partial-failure behavior, WinRT watcher cache/overflow,
   callback bounds and native acquisition/startup cancellation.
6. Software checks, then separately authorized Windows/Linux/Android physical
   acceptance. Role booleans and old diagnostic results do not certify this protocol.

The shared iroh diagnostic now uses managed discovery and incoming acceptance.
Its main timeout does not bound every early return or final shutdown await.
The sample is a separate workspace. Root checks prove neither its packaging nor
native execution. See [hardware verification](../hardware-verification.md).

Mocks, packaged JNI, reported native sessions and physical teardown prove different
things. Missing native scan results block discovery, not arbitration. This revision
changes no device settings or consumer dependencies.

## Reuse and licensing

Nearby's inspected implementation uses Apache-2.0.[7] Adaptation must meet license
conditions, including distributed license/notices, modified-file notices and
applicable `NOTICE` attribution. Its patent grant is scoped; it does not generally
grant trademark rights.[8]

This design revision copies no implementation code. Before a port, inspect file
headers, notices and dependencies. Preserve their attribution/license requirements.
This is not blanket clearance for an interoperable implementation or replacement notices.
Keep the existing btleplug BSD notice with the Windows code.

## Sources

[1] https://learn.microsoft.com/en-us/uwp/api/windows.devices.bluetooth.genericattributeprofile.gattserviceprovideradvertisingparameters.servicedata
[2] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/connections/implementation/mediums/ble/discovered_peripheral_tracker.cc
[3] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/internal/platform/implementation/windows/ble.cc
[4] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/connections/implementation/base_pcp_handler.cc
[5] https://developers.google.com/nearby/connections/android/manage-connections
[7] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/LICENSE
[8] https://www.apache.org/licenses/LICENSE-2.0
[9] https://developers.google.com/nearby/connections/android/discover-devices
[10] https://developers.google.com/android/reference/com/google/android/gms/nearby/connection/ConnectionsClient
[11] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/connections/implementation/endpoint_manager.cc
[12] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/connections/implementation/client_proxy.cc
[13] https://raw.githubusercontent.com/google/nearby/eed92ff1567bda238e1a1a6a231d0ddace486a55/connections/implementation/proto/offline_wire_formats.proto

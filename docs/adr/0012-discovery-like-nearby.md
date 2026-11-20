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
Its connection advertisement can travel whole in the radio ("fast advertisement",
at most 27 bytes of service data).[14] Otherwise its BLE medium keeps the full
advertisement in GATT, advertises a short header with a hash of it, reads each
header once, and backs failed reads off from 1 s to 5 min.[2][15]
Windows can use an alternate-UUID advertisement to trigger retrieval.[3]

**Implementation.** `ManagedMetadata` constructs complete readable metadata with
the unchanged version/capability and endpoint header. It rejects caller use of
the reserved service-data key and any field omitted by the readable encoder.
The radio carries no readable value and no local name; it carries two compact
records of it instead (§5), our own format, not Nearby's:

- the **advertisement header** (9 bytes): the FNV-1a-32 hash of the readable
  value and the endpoint tag (the endpoint's first four bytes), as service data
  under the managed service;
- the **fast advertisement** (21 to 27 bytes), only when
  `DatagramConfig::fast_advertisement_company` names a company: the hash, the
  whole endpoint, and the caller's manufacturer data under that company (at most
  6 bytes, Fini's 5). It travels as that company's manufacturer data and
  replaces the caller's entry; a scanner restores the entry from it.

The hash is a cache key, not a security boundary (ADR-0003). FNV-1a needs no new
dependency; Nearby truncates SHA-256 for the same field.

**Placement.** Core builds both records; each driver places what fits:

1. Android: the scan response carries the fast advertisement, otherwise the
   header (or, for a raw spec, the name). The advertising packet carries the UUID,
   the other manufacturer data and service data under other UUIDs
   (`GattServiceSpec::legacy_placement`, `AdvertisePlacement.kt`). Android's
   `ADVERTISE_FAILED_DATA_TOO_LARGE` retries once without the fast advertisement.
2. Windows: the provider's `ServiceData` (Windows 10 1903 and later) carries the
   header when the legacy packet has room. Windows puts the service UUID in that
   service-data section and keeps its UUID lists too, so next to a 128-bit UUID
   it never has room: on the Windows 11 (build 26200) test PC every start reported
   `StartedWithoutAllAdvertisementData`, and phones read the PC as before. An
   extended provider advertisement (`UseLowEnergyUncoded1MPhyAsSecondaryPhy`,
   build 26100) has room but legacy scanners cannot see it: phase 2. Instead,
   as Nearby's Windows medium does, a `BluetoothLEAdvertisementPublisher` sends
   the fast advertisement next to the provider (legacy, non-connectable). On the
   test PC it advertises from another address than the provider, so scanners join
   the two by endpoint (below; ADR-0011 §3, amended).
3. Linux: BlueZ has no scan response for us, and the header does not fit next to
   the 128-bit UUID list (`legacy_advertising_len`), so it is left out. Scanners
   read a Linux peer, as before.
4. Mock: Android's placement (`RadioCapacity::Legacy`), or none (`RadioCapacity::None`).

The fast advertisement leaves no room for the name in Android's scan response.
The name comes from one read instead (the completion read below).
`GattServiceSpec::advertising_mode` (`DatagramConfig::advertising_mode`, default
`Balanced`) sets Android's advertise mode and BlueZ's interval; Windows has no API.
`PowerProfile::advertising_mode` advises one per power profile; nothing switches it
automatically.

**Discovery.** Managed discovery reads each result's records:

- *Fast advertisement:* the endpoint is known, so the peer is reported at once,
  with the caller's manufacturer data restored. If the readable value rebuilt from
  the radio fields has the advertised hash, the peer is complete. Otherwise (no
  name on Android) one **completion read** per endpoint and hash fills in the rest
  and reports again if fields changed. It is low priority: it waits while a dial,
  another read or a retirement holds the address, a `request` to that address
  cancels it (the request is refused once, like one meeting a probe, and a retry
  gets the address), and its failures are not reported. Its deadline is 6 s, not
  10 s: on hardware a PC read of a phone that was reading the PC at the same time
  lost its link ("Instant Passed") and waited out every timeout; a retry after the
  1 s backoff runs on a fresh link. The Windows driver also ends GATT discovery as
  soon as the link it opened drops.
- *Fast advertisement without the service UUID* (a Windows publisher, at an
  address of its own, not connectable): never reported at that address. Once a
  read of a connectable address found the same endpoint, the record is reported
  at that address, and later sightings of that address without a record are
  answered from the record's key: one read per hash, also across scans.
- *Advertisement header:* a peer whose header is in the cache is reported from it
  without a read, at its current address and signal strength, but only for a
  fresh sighting (BlueZ re-reports cached devices without a signal strength).
  Otherwise one read; later reads only when the hash changes.
- *No record* (a Windows or Linux peer, an old peer, a foreign version): a read
  after a 300 ms grace for the scan response (Windows reports it apart from the
  advertisement), then the ten-second refresh, as before.

Reads use the existing bounded probe owner and its 10 s deadline (6 s for a
completion read). The cache of
readable values lives for the worker, not one scan: up to 64 values, least
recently seen out first, forgotten after 5 min unseen. Keys survive address
rotation. A failed read waits 1 s, doubling to 5 min, and only the first failure
of a peer not yet reported is reported. Refused initial probes report `QueueFull`
for capacity or `AdapterBusy` for an address reservation. Finite scans finish
admitted initial and completion reads and close.

Scanners match on the UUID list or service data under the service UUID.
Unknown record versions fall back to the read; the readable value itself stays
fail-closed. Old builds do not understand the records (accepted: both PRs ship
together).

Owned attempts supply known endpoint data without opening a replacement session.
Readable-only fields are not refreshed over those sessions. An inbound peripheral
session has no remote characteristic-read port. This remains a consumer acceptance
gap, not proof of continuous complete metadata refresh for connected peers.
Raw scanning and its opt-in `advertisement::complete` behavior remain unchanged.

Not done here (phase 2): extended advertisements carrying the fast record and all
caller fields (Android `AdvertisingSet`, BlueZ secondary channel, Windows 26100's
extended provider), and the header bit plus 3 s read delay Nearby uses for them.

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
- `update_advertisement(GattAdvertisement).await` retains validated desired fields
  on admission. Native replacement and readable-value writes remain separate effects.
  Startup, update, visibility, and managed recovery share the bounded Worker lane.
  Failures guard show until a successful hide and full hidden retry complete.
  Lifecycle `Lagged` means unknown native state, not confirmed radio loss.
  A started owner guards updates and show until the same reconciliation completes.
  It needs no new radio On event or server restart.
  Hidden or guarded owners do not advertise on radio On. Recovery reuses the
  server reader and latest desired metadata; ordinary updates retain generations.

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

Radio records (§2) are versioned apart from `BG` v1. Their first byte is
`1 1 1 1 1 K V V` (`K` the kind, `VV` the version less one), never valid UTF-8,
so no scanner reads one as a name:

| First byte | Record | Fields after it | Length | Carrier |
|---|---|---|---|---|
| `0xF8` | Advertisement header v1 | hash (4), endpoint tag (4) | 9 bytes | service data under the managed service |
| `0xFC` | Fast advertisement v1 | hash (4), endpoint (16), tail (0 to 6) | 21 to 27 bytes | manufacturer data under the fast company (extended advertisements: service data, phase 2) |

The hash is FNV-1a-32 (offset basis `0x811C9DC5`, prime `0x01000193`) over the
complete readable characteristic value. The tail is the caller's manufacturer
data under the fast company; a tail over 6 bytes means no fast advertisement.

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
service-data publication, native visibility port, the radio records with their
read cache (§2) and Android ABI 8 forwarding.
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
[14] https://raw.githubusercontent.com/google/nearby/3dd6a16/connections/implementation/mediums/ble.cc (fast advertisement, `ble_advertisement.h` "leaves 27")
[15] https://raw.githubusercontent.com/google/nearby/3dd6a16/connections/implementation/mediums/ble/advertisement_read_result.cc (read backoff 1 s to 5 min)

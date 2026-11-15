# 0012: Nearby-inspired BLE peer communication

## Status

Design under review in PR #38. The agreed contracts include responsibility
boundaries, the Nearby lifecycle and attempt-level tie-break outcomes.
Rust interfaces, control encoding and remaining platform contracts need review.
The core nonce handshake and adapter migration are **not implemented**.
They are completion requirements of **this PR**, not follow-up work.

This ADR consolidates the shared design. [ADR-0011](0011-windows-winrt-driver.md)
covers Windows-native choices. Earlier accepted ADRs retain their historical contracts.

## Decision

Provide discovery, connection establishment and messages in the reusable core,
without requiring iroh or Tauri. Follow selected Google Nearby mechanisms with
our own BLE protocol, not Google SDK or wire-protocol interoperability.
Keep raw GATT available for vendor protocols.

### 1. Responsibilities

- **Core `ble-gatt`:** discovery completion, establishment, acceptance, competing
  attempts, complete messages, liveness and ownership of native connection use.
- **Drivers:** implement `Driver` / `GattConnection`. Own OS objects, callbacks,
  serialized native operations and generation-aware effects. Report native limits.
- **`ble-gatt-iroh`:** translate packets/addresses and account for iroh carrier
  users. Retain authenticated-key mappings and QUIC path facts.
- **External iroh:** QUIC authentication, encryption, streams and path selection.
- **Tauri plugin:** host access, permissions, lazy construction and Android
  context/bridge setup. Packet traffic need not pass through it.
- **Applications:** permitted peers, product pairing, acceptance policy and
  application data/session protocols. Acceptance can be automatic; no dialog is required.

Core `Adapter` provides local radio access. An integration adapter translates
another stack's contract. Central/peripheral are connection roles, not permanent
application identities. One BLE connection already carries traffic both ways
through central writes and peripheral notifications.

Do not give two independent retry/close loops control of one native resource.
Adapters request use/release through the core; drivers perform scoped effects.
Complementary central and peripheral roles remain possible.
The [architecture map](../architecture.md) groups responsibilities, not approved new types or crates.

### 2. Discovery and metadata

**Reference.** Nearby separates advertising/discovery from connection requests.[5][9]
Its BLE tracker fetches full information through GATT, caches successful reads
and selects retries.[2] Its Windows implementation can use an alternate-UUID-only
advertisement to construct an internal header and trigger GATT retrieval.[3]

**Core contract.** Use radio metadata when it satisfies the profile. Otherwise,
fetch missing metadata through GATT and emit the same discovered-peer information.
A metadata read does not establish a message connection. Release only resources
owned by that read.

Select this behavior from received information and profile support, not `if Windows`.
Shared code owns parsing, cache/retry policy and cancellation. Drivers report
advertisements and implement reads/value updates.

WinRT includes `ServiceData` when space permits and can report
`StartedWithoutAllAdvertisementData`. Extended-advertisement capacity depends on
the platform.[1] Windows is not universally unable to advertise service data or exchange metadata.

Today's opt-in helper uses timer-based refresh, not Nearby's exact header/hash
protocol. Select cache freshness, bounded retry and cancellation before migration.
Do not require Google's assigned UUID or invent a Windows-only lifecycle.

### 3. Connection lifecycle

**Reference.** After a request, both sides receive initiation information.
Both must accept before establishment; either can reject.[5]

```text
advertise / discover -> discovered peer -> request
    -> initiation / acceptance on both sides
    -> established bidirectional messages -> disconnected
```

**Core contract.** Discovery does not connect every peer automatically. Either
eligible peer can request communication. The core coordinates attempts through
one symmetric behavior, without public `PreviousDialer`, `EitherPeer` or identity-ordered modes.

Stopping discovery or advertising preserves established communication, as Nearby
documents.[10] Retain required native objects. Stopping a raw server is a different operation.

Completing a message preserves a healthy connection. Core liveness uses driver
events independently of payloads. Nearby's endpoint workers provide keep-alive/loss
detection; exact core wire messages and timing remain open.[11]

Explicit disconnect ends communication and outstanding transfers. Cancellation
can also stop establishment.[10] An independent loop must not undo either action.
A losing candidate or one iroh user ending is not a global disconnect.

Nearby workers can use an available replacement channel. Without one, they
discard the endpoint and report disconnection.[11] Internal recovery differs from
a new request after terminal disconnection. This design adds neither Wi-Fi upgrades
nor indefinite automatic reconnection.

### 4. Endpoint correlation and tie-break

**Reference.** Nearby outgoing requests contain an endpoint ID and a fresh random
signed 32-bit nonce.[4][13] It manages endpoint IDs separately and can retain them
during ongoing connections.[12]

**Core contract.** Correlate incoming/outgoing requests with a core endpoint
identifier, not an iroh key, display name or ordered native address. Discovery
and handshake must identify the same endpoint. Keep endpoint, attempt and native
session generation distinct. None alone authenticates the remote party.

Follow the reference's attempt-level arbitration:[4]

- Outgoing nonce greater than incoming: retain outgoing; close incoming.
- Incoming nonce greater than outgoing: close outgoing; continue incoming.
- Equal nonces: fail both; use a fresh nonce for each fresh attempt.
- An ordinary duplicate incoming request does not re-arbitrate an established endpoint.
- A duplicate local request must not start another native dial.

The losing outgoing request reports failure/rejection. Process the winning incoming
request separately through acceptance.[4] **Do not silently fulfill the outgoing
request with the incoming survivor.** Attempt failure differs from established-peer disconnection.

These are BLE outcomes, not a requirement to fail an outgoing QUIC connection.
Multiple authenticated iroh connections can share the winning carrier.

Close only the losing attempt. Stale completion/cancellation must not close its
replacement or remove its queue. Apply generation guards and serialization around
native effects, not only in a separate policy check. WinRT exclusion does not
prove physical disconnection.

Requests must arrive before arbitration can run. A nonce cannot repair an OS
rejection before that exchange. Separate role capabilities do not prove simultaneous-role or crossed-attempt support.

### 5. Protocol and security

Require explicit version/capability detection and bounded control handling.
Define compatibility before replacing callers; older peers might not support arbitration.

Separate control records from application payloads. Reuse bounded datagram
fragmentation/reassembly without interpreting QUIC or guessing from payload prefixes.
Fix control encoding, endpoint representation and numeric comparison together.
Both peers must select the same attempt.

Keep the handshake out of raw GATT. [ADR-0003](0003-carrier-not-crypto-and-the-two-api-tiers.md)
still applies: correlation and acceptance provide neither authentication nor
encryption. iroh supplies security in its composition; other consumers need their
own layer. Borrowing arbitration does not provide Nearby's complete security guarantees.
Document threat assumptions before implementation.

## Current implementation gap

Current code differs from the target:

- Windows implements common native ports. Shared metadata completion and
  `set_characteristic_value` cross native, mock and wrapper boundaries.
- `advertisement::complete` is opt-in and uses timer refresh. Raw scans do not
  inherit completion. Cache freshness and safe reads during active communication remain open.
- `datagram::connect` and `serve` are separate. No shared core endpoint-ID/nonce contract exists.
- The iroh adapter still exposes `InitiationPolicy`: `PreviousDialer` is the
  default; `EitherPeer` and `Ordered` remain implemented.
- It retains crossed candidates, protects current users from idle release and
  reconciles path snapshots separately from address history. Preserve iroh usage
  accounting, not independent native arbitration. These tests do not verify the new tie-break.
- Existing `PeerLink::Symmetric` selects a lower-`LinkId` dialler. It is not the
  target controller. Check contracts before reusing state tables, handles or guards.
- The datagram server admits one inbound peer. Equal-peer initiation does not establish Cluster/M-to-N support.
- Android needs a compatible native bridge/context. Kotlin currently lives in
  the Tauri plugin; host-neutral packaging remains open.

The following distinguishes software fixes from remaining source findings. Native behavior remains unverified:

- Current `stop_advertising` / `stop_serving` can destroy serving and active
  communication. Separate visibility from serving before claiming Nearby stop semantics.
- [Metadata completion](../../ble-gatt/src/profile/advertisement.rs) now reuses
  `Connection` cleanup and retains exhausted budgets in a bounded table.
  Regression tests cover timeout, output cancellation and repeated sightings.
  Cleanup after an acquired connection is dropped remains asynchronous and needs
  a running Tokio runtime. Native teardown and cancellation during acquisition remain unverified.
- [Metadata publication](../../ble-gatt/src/profile/datagram/mod.rs) updates radio
  data and readable values separately. Define coherent generations, setter
  eligibility and partial-failure recovery; do not call it atomic.
- Windows acquisition/subscription cleanup and close/exclusion reporting need
  contract changes; see ADR-0011. [Watcher](../../ble-gatt/src/drivers/windows/watcher.rs)
  overflow/cache expiry must not permanently hide a discovered peer.
- Android needs a scan-start exception rollback regression. A throwing native
  start can leave assigned callback ownership busy. Roll back only failed
  acquisition; preserve a genuinely active scan.
- The diagnostic's 180-second main timeout excludes some early returns and final
  shutdown awaits. The sample is a separate workspace. Root checks prove neither
  its packaging, compiled JNI descriptors nor native execution.

See [current APIs](../interface.md) and [diagnostic procedures](../hardware-verification.md).

## Alternatives and consequences

- Keeping initiation modes, or defaulting to `EitherPeer`, leaves core behavior
  in an optional integration. Permanent identity ranking prevents either-peer initiation.
- Nesting controllers duplicates native retry/teardown ownership.
- The full Nearby SDK/protocol exceeds the selected mechanisms and own-protocol scope.
- Returning the incoming winner as outgoing success changes the chosen attempt-level semantics.

The selected design needs a versioned protocol, core/adapter refactor and explicit
consumer migration. Windows support and old candidate tests do not satisfy it.

## Remaining interface decisions before implementation

- Request/event/accept/reject/cancel/close APIs, duplicate-request errors and awaited shared-user release.
- Endpoint representation/lifetime, bounded encoding, version negotiation and metadata/datagram compatibility.
- Handshake deadlines, liveness, failure/equal-nonce backoff and terminal disconnection reporting.
- Atomic session-scoped cleanup across drivers, busy operations, native role
  collisions and WinRT exclusion versus physical disconnect.
- Discovery freshness, retry budget, cancellation under active use and non-Tauri Android packaging where needed.

Keep these gates in this consolidated design. They do not approve public types or defaults.

## Migration and verification gates for this PR

1. Resolve remaining contracts and legal-reuse scope before implementation.
2. Implement core establishment/arbitration without iroh/Tauri. Test either
   initiator, crossed attempts, equal nonces, rejection, attempt outcomes,
   duplicate requests and healthy-connection preservation.
3. Test metadata reads, cancellation at each acquisition boundary, bounded
   failures, stale callbacks, stop-discovery/advertising during communication,
   idle liveness and explicit disconnect without unintended reconnect.
4. Adapt iroh; retain key/path/user accounting. Prove that ending one QUIC
   connection preserves another user's carrier. Replace old modes and candidate
   assertions only when replacement tests exist.
5. Verify affected native drivers, mock/broker forwarding, JNI/context and Tauri
   wrappers. Preserve raw GATT behavior.
6. Run software checks. Require separate authorization for native/physical
   Windows/Linux/Android acceptance and actual consumer checks.

Retain busy/private-address safety, idle survival, stale-generation protection,
current-versus-historical paths and multi-user release tests. Replace mode-specific
and no-tie-break carrier expectations with core arbitration tests, not multiple-QUIC-user expectations.
Keep source JNI guards separate from bytecode, packaging and runtime checks.

The current diagnostic tests the old candidate model. Mocks, packaged JNI,
observed native sessions and exhaustive physical teardown prove different things.
Old results do not certify the new handshake. Missing native scan results block
discovery, not arbitration. This revision changes no device settings or consumer source.
Keep PR #38 draft until implementation and acceptance gates pass.

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

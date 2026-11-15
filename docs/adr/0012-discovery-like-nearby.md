# 0012 — Nearby-inspired BLE peer communication

## Status

Design under review in PR #38. The responsibility boundaries, Nearby lifecycle
and attempt-level tie-break outcomes were agreed in chat. The exact Rust API,
control-message encoding and remaining platform contracts still need review.
The core nonce handshake and adapter migration are **not implemented**.

This is the single new shared-design ADR for this PR. Windows-specific native
operations belong in [ADR-0011](0011-windows-winrt-driver.md). The other new,
unmerged design drafts are consolidated here, not retained as superseding ADRs.
Earlier accepted ADRs remain records of their implemented contracts.

The new core tie-break is a completion requirement of **this PR**, not merely
a possible follow-up. Publishing this design does not complete that requirement.

## Decision question

How can the reusable core provide equal-peer discovery, connection establishment
and messages across platform drivers, following Google Nearby's mechanisms,
without making iroh or Tauri the owner of generic BLE behavior?

## Context and constraints

One BLE connection already carries traffic both ways: central writes and
peripheral notifications. Symmetric establishment is a different problem:
either eligible peer must be able to initiate without a permanent device ranking.

We follow Nearby's behavior and selected mechanisms while retaining our own BLE
protocol. This is not Google Nearby SDK or wire-protocol interoperability.
Raw GATT remains available for devices that speak a vendor protocol.

The architecture diagram is in [architecture.md](../architecture.md). Its boxes
are responsibilities, not approval of new class names or another crate.

## Decision

### 1. Responsibility boundaries

- **Core `ble-gatt`:** discovery completion, peer establishment and acceptance,
  competing-attempt arbitration, complete messages, liveness and ownership of
  native connection use. This must work without iroh or Tauri.
- **Drivers:** implement `Driver` / `GattConnection`; own OS objects, callbacks,
  serialized native operations and generation-aware effects. Report platform
  limits instead of pretending a requested native action succeeded.
- **`ble-gatt-iroh`:** translate packets/addresses and account for iroh users of
  a core carrier. Keep authenticated-key mapping and QUIC path facts here.
- **External iroh:** QUIC authentication, encryption, streams and path selection.
- **Tauri plugin:** application access, permissions, lazy construction and Android
  host context/bridge setup. It is not an obligatory packet-processing hop.
- **Applications:** permitted peers, product pairing, acceptance policy and their
  data/session protocol. Acceptance may be automated; no dialog is mandated.

Core `Adapter` means local radio access. An integration adapter means a crate
that translates another stack's contract. Central/peripheral are roles of a
particular BLE connection, not permanent identities assigned to applications.

Two independent retry/close loops must not control the same native resource.
Adapters request use/release through the core; drivers perform scoped native
operations. This does not prohibit complementary central and peripheral roles.

### 2. Discovery and metadata

**Reference.** Nearby separates advertising/discovery from requesting a
connection.[5][9] Its BLE tracker supports full information fetched through
GATT, successful-read caching and retry decisions.[2] The inspected Windows
implementation also recognizes an alternate-UUID-only advertisement and
constructs an internal header to trigger GATT retrieval.[3]

**Core contract.** Radio metadata sufficient for the profile takes the fast
path. Otherwise discovery obtains the missing metadata through GATT and emits
the same discovered-peer information. A metadata read is not an established
message connection. Its cleanup releases only resources it owns.

The decision depends on received information and advertised profile support,
not `if Windows`. Shared code owns parsing, cache/retry policy and cancellation;
drivers report advertisements and implement reads/value updates.

**Platform boundary.** WinRT `ServiceData` is included only when space is
available; `StartedWithoutAllAdvertisementData` can report partial inclusion.
Extended-advertisement capacity is platform-dependent.[1] This is not a blanket
claim that Windows cannot advertise service data or exchange the metadata.

**Difference to resolve.** Today's helper uses timer-based refresh and an
opt-in completion stream. That is not Nearby's exact header/hash protocol.
Select cache freshness, bounded retry and cancellation rules before migration;
do not invent Windows-only lifecycle rules or require Google's assigned UUID.

### 3. Connection lifecycle

**Reference.** After a request, both sides receive initiation information and
must accept before the connection is established. Either can reject.[5]

```text
advertise / discover -> discovered peer -> request
    -> initiation / acceptance on both sides
    -> established bidirectional messages -> disconnected
```

**Core contract.** Discovery does not automatically establish a message
connection to every peer. Both eligible peers may request communication;
the core coordinates those attempts. There is one symmetric peer behavior,
not public `PreviousDialer`, `EitherPeer` or identity-ordered modes.

Stopping discovery or advertising preserves established communication, as in
Nearby's documented API.[10] Keep native objects required by active channels.
A raw server-stop operation is not the same action as stopping discoverability.

Message completion does not release a healthy connection. Nearby's endpoint
workers include keep-alive/loss detection independent of payloads.[11] Liveness
belongs in the core with driver events; exact wire messages/timing remain open.

Explicit disconnect ends communication and outstanding transfers; cancellation
can also stop establishment.[10] Do not undo it with an independent reconnect
loop. A losing candidate or a single iroh user ending is not global disconnect.

Nearby's inspected workers can continue on an available replacement channel;
without one, they discard the endpoint and notify disconnection.[11] Internal
channel recovery and a new request after terminal disconnection are distinct.
This does not add Wi-Fi upgrades or promise indefinite automatic reconnection.

### 4. Endpoint correlation and the new tie-break

**Reference.** Nearby's request includes an endpoint ID and a random signed
32-bit nonce generated for each outgoing attempt.[4][13] Endpoint IDs are
managed separately and can be retained while connections are ongoing.[12]

**Core contract.** Correlate competing incoming/outgoing requests using a
core protocol endpoint identifier, not an iroh key, display name or ordered
native address. Discovery and handshake must describe the same endpoint.
Distinguish that endpoint, a connection attempt and a native session generation.
None of these values alone authenticates the remote party.

Follow the reference's attempt-level arbitration:[4]

- Outgoing nonce greater than incoming: retain outgoing, close incoming attempt.
- Incoming nonce greater than outgoing: close outgoing, continue incoming attempt.
- Equal nonces: fail both attempts; any fresh attempt gets a fresh nonce.
- An already-established endpoint is not re-arbitrated by an ordinary duplicate
  incoming request. A duplicate local request must not spawn another native dial.

**Observable outcome.** Preserve Nearby's attempt-level behavior: the losing
outgoing request reports failure/rejection, while the winning incoming request
is processed separately through acceptance.[4] Do not silently fulfill the
outgoing request with the incoming survivor. Attempt failure and established
peer disconnection are different events.

These are BLE establishment outcomes, not a rule that one outgoing QUIC
connection must fail. Multiple authenticated iroh connections may share the
winning carrier. Preserve that distinction in adapter migration and tests.

**Ownership.** Close only the losing attempt. Stale completion/cancellation must
not close its replacement or remove its queue. Native generation protection
and operation serialization stay around the actual effects, not just in a
separate policy check. WinRT exclusion is not proof of physical disconnection.

Arbitration requires delivery of the initial requests. A nonce cannot repair
an OS attempt rejected before that exchange. Separate role capabilities do not
prove simultaneous-role or crossed-attempt support.

### 5. Protocol and security effects

The peer profile needs explicit version/capability detection and bounded
control-message handling. Older peers must not be silently assumed to support
arbitration. Specify compatibility behavior before replacing current callers.

Keep control records distinct from application payloads. Reuse bounded datagram
fragmentation/reassembly without interpreting QUIC or guessing from arbitrary
payload prefixes. Exact encoding, endpoint representation and numeric comparison
encoding must be fixed together; different peers must select the same attempt.

Do not inject this handshake into generic raw GATT operations. The existing
carrier-not-crypto boundary in [ADR-0003](0003-carrier-not-crypto-and-the-two-api-tiers.md)
remains: correlation/acceptance is not authentication or encryption. iroh supplies
those properties for its composition; a non-iroh consumer needs its own security
layer. Borrowing arbitration does not give our profile Nearby's complete
security guarantees. Document threat assumptions before implementation.

## Current implementation gap

This section describes current code, not a second target design:

- Windows implements the common native ports; shared metadata completion and
  `set_characteristic_value` already cross native/mock/wrapper boundaries.
- `advertisement::complete` is opt-in. Its timer-based refresh, cancellation and
  failure-budget behavior need review; raw scans do not inherit completion.
- `datagram::connect` and `serve` remain separate. There is no shared core
  endpoint-ID/nonce establishment contract.
- The iroh adapter still implements `InitiationPolicy`, defaulting to
  `PreviousDialer`. `EitherPeer` and `Ordered` remain current API variants.
- It retains crossed candidates, protects current iroh users from idle release,
  reconciles path snapshots and separates current use from address history.
  These are not tests of the new core tie-break. Preserve necessary iroh usage
  accounting without retaining independent native arbitration.
- Existing `PeerLink::Symmetric` chooses a lower-`LinkId` dialler. It is not the
  target or an approved class to place under the adapter. Reuse suitable state
  tables, handles and guards only after their contracts are checked.
- The datagram server still admits one inbound peer. This design does not claim
  Nearby Cluster/M-to-N support merely because it adopts equal-peer initiation.
- Android's Rust driver needs a compatible native bridge/context. Kotlin lives
  in the Tauri plugin today; host-neutral packaging is not decided here.

Whole-PR source review also identifies these concrete contract gaps. They are
static findings, not reproduced native failures or fixes made by this design:

- Visibility and serving are bundled: current `stop_advertising` /
  `stop_serving` can destroy serving and active communication. Separate these
  intents in the generic contract before claiming Nearby stop semantics.
- [Metadata completion](../../ble-gatt/src/profile/advertisement.rs) has no
  cancellation-safe disconnect guard across its read; exhausted entries can be
  recreated with a fresh budget. Preserve bounded failure state, interruptible
  scan ownership and explicit completeness under repeated sightings.
- [Metadata publication](../../ble-gatt/src/profile/datagram/mod.rs) updates
  radio data and readable values separately. Define coherent generations,
  setter eligibility and partial-failure recovery rather than calling it atomic.
- Windows acquisition/subscription cleanup and close/exclusion reporting need
  contract work; see ADR-0011. [Watcher](../../ble-gatt/src/drivers/windows/watcher.rs)
  delivery overflow/cache expiry must not permanently hide a discovered peer.
- Android scan-start exception rollback needs a regression: assigning callback
  ownership before a throwing native start can leave later starts busy. Preserve
  a genuinely active scan while rolling back only the failed acquisition.
- The diagnostic's main timeout does not bound every early return or final
  shutdown await. The sample is a separate workspace; root checks do not prove
  its packaging, compiled JNI descriptors or native execution.

Current API examples remain in [interface.md](../interface.md). Diagnostic
instructions are in [hardware-verification.md](../hardware-verification.md).

## Alternatives and consequences

Keeping the three initiation modes, or merely making `EitherPeer` the default,
would leave fundamental behavior in an optional integration. Not selected.
A permanent identity-ranked initiator would violate either-peer establishment.
Nesting independent controllers would duplicate native retry/teardown ownership.

Using the full Nearby protocol/SDK would change scope beyond selected mechanisms
and our own protocol. Not selected. Returning the incoming winner as success to
the outgoing caller would simplify some consumers but change the chosen
attempt-level semantics. Not selected.

The selected design requires a versioned protocol and a real core/adapter
refactor. Windows support and old candidate tests cannot substitute for it.
Existing consumers need an explicit migration, not a dependency update alone.

## Remaining interface decisions before implementation

- Rust request/event/accept/reject/cancel/close interfaces, duplicate-request
  errors and awaited release semantics for shared users.
- Endpoint-ID representation and lifetime; bounded control encoding, version
  negotiation and compatibility with the current metadata/datagram formats.
- Handshake deadlines, liveness timing, retry/backoff after failure/equal nonces
  and the terminal point at which the application receives disconnection.
- Atomic session-scoped cleanup across each driver, including busy operations,
  native role collisions and WinRT's exclusion versus physical disconnect.
- Discovery cache freshness, retry budget and cancellation under an active link;
  non-Tauri Android bridge/context packaging if its current path is insufficient.

These are explicit review gates, not approved public types or hidden defaults.
Do not create more ADRs to decide sections of this same unmerged design.

## Migration and verification gates for this PR

1. Fix the remaining contracts and legal-reuse scope before source changes.
2. Implement establishment/arbitration in the core and test without iroh/Tauri:
   either initiator, crossed attempts, equal nonces, rejection, attempt-specific
   outcomes, duplicate requests and healthy-connection preservation.
3. Exercise metadata reads, cancellation at each acquisition boundary, bounded
   failures, stale callbacks, stop-discovery/advertising during communication,
   idle liveness and explicit disconnect without unintended reconnect.
4. Adapt iroh to the core; retain key/path/user accounting. Prove that one QUIC
   connection ending does not release another user's carrier. Retire old public
   initiation modes and candidate-specific assertions only with replacements.
5. Verify every native driver, mock/broker forwarding, JNI/context and Tauri
   wrapper touched by a contract change. Preserve generic raw GATT behavior.
6. Run core and integration software checks, then separately authorized native
   and physical acceptance for Windows/Linux/Android and actual consumer use.

The existing diagnostic tests the current iroh candidate model. Mock results,
packaged JNI execution, observed native sessions and exhaustive physical teardown
are different evidence levels. Old results do not certify the new handshake.

Retain busy/private-address safety, idle survival, stale-generation protection,
current-versus-historical iroh paths and multi-user release tests. Replace public
mode-specific and no-tie-break carrier expectations with core arbitration tests;
do not replace valid multiple-QUIC-user expectations. Keep source-level JNI
guards, and separately require bytecode/packaging/runtime checks where needed.

A scan delivering no native results remains a discovery blocker, not evidence
against arbitration. No device settings or consumer source change in this
revision. Keep PR #38 draft until its implementation/acceptance gates are met.

## Reuse and licensing

Nearby's inspected implementation is Apache-2.0 licensed.[7] The license permits
adaptation subject to its conditions, including distributed license/notices,
modified-file notices and applicable `NOTICE` attribution.[8] Its patent grant
is scoped and it does not generally grant trademark rights.[8]

This design revision copies no implementation code. Before a port, inspect the
specific file headers, applicable notices and dependencies; preserve their
attribution/license requirements. This is not blanket legal clearance for a
complete interoperable implementation or permission to replace existing notices.
The existing btleplug BSD notice remains attached to Windows code.

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

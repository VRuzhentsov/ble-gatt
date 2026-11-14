# 0013 — Explicit symmetric initiation, with iroh owning connection reuse

## Status

Implemented locally; radio-free tests only. Hardware acceptance and Fini
adoption remain separate gates. No new handshake, wire format or bridge ABI.

## Decision and alternatives

Use the existing ADR-0008 model: either peer may initiate, reuse incoming
paths learned by iroh, tolerate crossed candidates where the platform allows
it, and release links only after their last iroh user ends. This is the
approved direction, not a proposal to add Google's collision-nonce protocol.

Options considered:

1. Keep the preventive lower-identity dialler rule everywhere, as core
   `PeerLink::LinkRole::Symmetric` currently does. It avoids crossed attempts
   but a larger-identity peer cannot initiate while the smaller peer waits.
2. Use iroh's existing paths and hooks, following the duplicate-tolerant
   iroh/iroh-gossip precedent already recorded in ADR-0008. This permits either
   initiator without adding another session manager or interpreting QUIC.
   It cannot make a platform tolerate crossed physical dials.
3. Add a per-attempt arbitration exchange, as the inspected Google Nearby
   implementation does. It needs new protocol state and still cannot resolve
   a connection aborted before its first message arrives. No evidence from
   the existing mock tests requires that extra protocol.

**Selected: option 2, with an explicit fallback from option 1.** No external
source code was copied. This extends ADR-0008 rather than replacing it.

## Public policy and compatibility

`BleGattTransportBuilder::initiation_policy` takes:

| Policy | New outgoing BLE links |
|---|---|
| `PreviousDialer` (unchanged default) | Either peer can cold-start. After unexpected incoming-link loss, yield to its previous dialler for 30 seconds. |
| `EitherPeer` | Either peer can cold-start or reconnect immediately, including the larger endpoint key. No previous-role yield. |
| `Ordered { local_id }` | Only the smaller iroh endpoint key initiates. Both sides configure this fallback with their own actual endpoint key. A unique remote mapping from `set_peer_address` is required; unknown or ambiguous destinations are not dialled. |

Policy applies only when no outgoing queue exists. It never refuses traffic
on an existing incoming link or closes a link to enforce identity ordering.
An accept-only iroh transport still omits the dialler; a dial-only one does
not serve/attach incoming channels. Core `LinkRole::DialOnly`, `AcceptOnly`
and the existing lower-`LinkId` `Symmetric` behaviour are unchanged.
`PeerLink::Symmetric` is **not** an either-peer initiation implementation.

`CapabilityReport { central, peripheral }` reports individual roles only.
Both being true does not report mixed-role or crossed-dial support. There is
no portable runtime concurrency probe in this change. The caller selects
`EitherPeer` explicitly only for a platform combination it has validated;
otherwise both peers use `Ordered` (or retain the compatibility default).
Do not guess concurrency support from an OS name or from these two booleans.
Mismatched policies can prevent a cold-start connection and are not negotiated
by a new on-wire protocol.

Both symmetric policies retain the private-source-address protection: an
address learned only as an incoming source is not assumed dialable. Discovery
must supply the peer's advertised address with `set_peer_address`.

## One lifecycle owner per responsibility

| Owner | Responsibility |
|---|---|
| Core `ble-gatt` drivers and connection state machines | Physical GATT operations, radio state, generation-bound connection handles and cleanup; typed busy versus unavailable errors. |
| Core datagram profile | Fragmentation, reassembly, served-channel generations, per-session notification/disconnect operations. |
| `ble-gatt-iroh` | One pending outbound queue per address, explicit initiation policy, candidate tasks, key/link mapping learned through hooks, link release after the last iroh user. |
| iroh | QUIC handshakes and authentication, incoming/outgoing connections, paths and reuse. |
| Application (Fini) | Permitted peers, discovery schedule, pairing, sync settings, ALPN and application-session acceptance/selection. |

Do not run `PeerLink` and `BleGattTransport` as independent owners of the same
channels. The iroh adapter already uses `DatagramChannel` directly, not
`PeerLink`; replacing that with another retry/close controller would duplicate
ownership. Fini's session-selection decision must close only the losing QUIC
connection, not `close_links(peer_key)` while another connection still uses
that peer. Explicit `close_link(s)` remains a force-close for genuine shutdown,
revocation or abandoning a connect, not ordinary candidate rejection.

## Crossed candidates and release

The newest attached channel remains the outbound queue. Older candidates
continue receiving: the remote endpoint may still be using them. iroh's hooks
count live connections per BLE address, so a single connection ending cannot
release an address still used by another connection. Queue identity checks
prevent stale dial completion/failure cleanup from removing its replacement;
driver and datagram generation guards protect replacement physical sessions.

The idle timer is now only a backstop for addresses with no live iroh users.
Previously it could disconnect a quiet but live connection, including a
central-role candidate whose BlueZ disconnect also affected the other role.
With hooks installed, idle expiry rechecks **current path** ownership and
leaves active links alone. A connection still alive on IP or another BLE
address does not indefinitely protect a BLE path it has closed. The adapter
subscribes to path events before reading the initial `Connection::paths()`
snapshot, then reconciles current address use from a fresh snapshot on every
event, including `Closed` and `Lagged`. Queued older `Opened` events cannot
restore a path absent from that snapshot; multiple paths at one address and
another connection using it still protect the carrier. This follows iroh
1.4.1's documented subscribe-before-snapshot pattern rather than interpreting
events as an infallible delta log.

Historical key/address associations remain separate: they are still needed
by `close_links`, close-frame barriers and ended-packet suppression. Closing
connections retain protection until their existing close barrier completes;
merely closing a path neither ends the QUIC connection nor erases its history.
Last-user release still sends the QUIC close first and observes the
configured release delay and existing platform-specific cleanup (ADR-0008).
Without hooks, the compatibility idle backstop still applies.

Ownership is conservative and address-scoped, not a new per-candidate QUIC
path manager. Late short-header packets can arrive after release and cause
the server to hand out another channel. If only ended connections use the
address and this candidate has not seen a new handshake, the candidate is
closed instead of waiting another idle timeout. A packet with the long-header
bit set (or another active user) protects it from that early rejection, not
from idle expiry. `is_quic_initial` is only the existing top-bit routing
heuristic: it does not validate an Initial or authenticate a handshake.
No new wire exchange is added; `attach_after` routes its saved first
packet through the same lifecycle check and preserves packet order.

If crossed candidates share an address, all remain protected
while any current path uses that address; they can all release after the final
user. This intentionally trades a temporary extra candidate for not closing
an active connection. It does not promise immediate pruning to one physical
link, nor prove crossed-dial safety on BlueZ/Android/WinRT.

## Topology limit

Actual `datagram::serve_driver` still refuses another inbound peer while one
is served. `notify_peer` now exists but that alone did not lift the profile's
single-inbound-peer limit. This change does not claim Nearby Cluster/M-to-N
support. Multiple outbound peers and one inbound peer remain the profile's
current topology.

## Verification and remaining gates

Mock tests exercise larger-key cold-start and reconnect, incoming path reuse
without another BLE dial, concurrent connections, keeping the survivor after
one crossed application candidate ends, an active link surviving idle expiry,
last-user cleanup, stale outbound-queue cleanup, unknown/private destinations,
and retry after `GattBusy`. Existing core tests cover stale generation events,
radio recovery, explicit roles, capability refusal and busy/unavailable typing.

Runtime regressions replay the documented post-path-close snapshot at the
same reconciliation seam as the hooks. A real QUIC connection continues
delivering datagrams on IP while its historical BLE candidate expires idle;
a separate live connection retaining a path at that BLE address prevents
expiry until it too moves away. The BLE candidate is a real mock-radio
datagram task, not a source-text assertion. iroh has no public close-path
operation, so this is snapshot-seam coverage, not a physically induced
multipath-closure test.

These are host/mock claims, not hardware claims. The Nokia run with zero native
scan callbacks remains a separate discovery issue; initiation policy is not a
fix for missing scan results. The prior near-simultaneous hardware run had
transient failures; no test here erases that evidence.

Before adopting `EitherPeer` in Fini or publishing: parent reviews the local
changes and independent review, then obtains separately authorized hardware
evidence for both initiation directions, crossed starts, reconnect, stable
payload transfer and cleanup. No Fini source or phone settings are changed by
this stage. No nonce or cryptographic handshake is added without concrete
follow-up evidence requiring it.

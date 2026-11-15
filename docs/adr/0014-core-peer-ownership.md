# 0014 — Core-owned symmetric BLE peers, with external adapters

## Status

Provisional architecture agreement, November 14, 2026. Documentation only;
implementation, API design and physical acceptance remain pending.

This records the agreed ownership boundary for the next revision of PR #38.
It supersedes the **target public policy** in ADR-0013, not the description of
what the current code does. Existing APIs and their behavior have not changed.

## Context

The workspace contains a reusable core, an iroh transport adapter and a Tauri
host adapter. iroh is an external dependency, not another workspace crate.
The current `ble-gatt-iroh` implements three initiation policies, while the
core's existing `PeerLink::Symmetric` selects a lower-identity dialler. That
places the desired equal-peer behavior in an integration rather than making
it available to every core consumer.

The required behavior is not an opt-in iroh mode. Two eligible application
peers must be able to initiate communication and reconnect independently of
which peer opened the previous connection.

## Provisional decision

### Core `ble-gatt`

Own generic BLE communication: central/peripheral operations, complete messages,
symmetric peer initiation/reconnection, competing-attempt coordination, native
connection lifetimes and generation-aware cleanup. Keep the high-level peer
contract independent of iroh and Tauri types.

Either device can initiate. One established connection carries messages both
ways. The application does not select `PreviousDialer`, `EitherPeer` or an
identity-ordered initiator as alternative peer-to-peer behaviors.

Platform coordination may serialize or back off physical attempts internally.
It must preserve the ability of either peer to request communication, rather
than assign a permanent initiator or silently change the public semantics.
Unsupported platform behavior is reported explicitly. Availability of central
and peripheral separately does not prove crossed-attempt safety.

Raw GATT operations remain available for vendor devices and role-specific
protocols. They are not forced into the library's application-peer profile.

### `ble-gatt-iroh`

Adapt the generic core to iroh's custom packet/address interfaces. Retain iroh
peer-key mapping, QUIC path observations and accounting of iroh users of a BLE
carrier. Express use/release through the core rather than implement competing
native connection control. Do not move iroh identities or hook types into the
core merely to share code.

### `tauri-plugin-ble-gatt`

Own host access and initialization: commands/events, lazy driver construction,
permissions and Android context/bridge integration. Reuse core communication;
do not create another transport controller in Tauri or JavaScript.

### External iroh and applications

Iroh owns authenticated QUIC connections, encryption, streams and paths.
Applications own permitted peers, product pairing, communication intent, sync
settings and application-session selection. They may use the core directly or
select a network stack and its adapter.

## Alternatives and precedents

1. Keep the three public iroh-adapter initiation modes. This preserves the
   present implementation with minimal migration, but does not satisfy the
   required single behavior or make it available without iroh. Not selected.
2. Make `EitherPeer` the default but retain alternatives. This changes a default,
   not the ownership boundary or the single-contract requirement. Not selected.
3. Put symmetric BLE behavior in the reusable core and adapt it to iroh.
   Selected provisionally. It requires a real ownership/API refactor, not
   nesting the current `PeerLink` under the current adapter.

[Google Nearby Connections](https://developers.google.com/nearby/connections/strategies)
provides a behavioral precedent for peers initiating and accepting connections.
[Berty/Wesh](https://berty.tech/docs/protocol) documents proximity transports
under libp2p, a precedent for adapting local connectivity to an external network
stack. Neither establishes an identical algorithm or proves our platform support.

Iroh's [CustomTransport interface](https://docs.rs/iroh/1.4.1/iroh/endpoint/transports/trait.CustomTransport.html)
is an explicit upstream extension. Its `unstable-custom-transports` feature
means upgrades may require adapter changes, not that BLE or encryption is
inherently unreliable. This reinforces isolation of external API coupling.

## Open design questions

These need resolution before the implementation plan is fixed:

- Is core communication demand operation-scoped or a persistent tracked-peer
  request? What starts and ends automatic retry?
- How does a caller express discovery/advertising intent and power limits?
- Which existing `Connection`, `PeerLink`, state-table and datagram ownership
  mechanisms are reused or changed? What is the one owner for each resource?
- How do iroh's multiple users retain/release a shared carrier without native
  ownership leaking back into the adapter?
- How are native connection attempts coordinated on each supported platform?
- Does a non-Tauri Android host need different bridge packaging? The current
  Kotlin bridge lives in the plugin; no packaging refactor is selected yet.

No new public type, extra crate, handshake, wire format, multi-inbound support
or universal background/locked-device guarantee is approved by this decision.

## Migration and acceptance requirements

Keep current APIs clearly labeled until replacements exist. Evaluate existing
core mechanisms before adding a new lifecycle object. Do not make the present
`PeerLink` and adapter independent owners of the same channels.

Prove the symmetric contract without iroh, then through the iroh adapter.
Cover either initiator, connection reuse, competing attempts, cancellation,
radio recovery, replacement-session protection and last-user release. Test
raw GATT behavior separately. Review wrappers, mock/broker forwarding and the
real consumer when their contracts change.

Mock tests, packaged JNI execution, observed native sessions and physical OS
teardown are separate evidence levels. New documentation is not runtime proof.

## Related documents

- [Architecture map](../architecture.md)
- [Core crate](../../ble-gatt/README.md)
- [iroh adapter](../../ble-gatt-iroh/README.md)
- [Tauri adapter](../../tauri-plugin-ble-gatt/README.md)
- [ADR-0013: current initiation implementation](0013-symmetric-iroh-initiation.md)

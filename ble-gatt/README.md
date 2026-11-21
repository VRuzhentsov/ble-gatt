# ble-gatt: reusable BLE communication

`ble-gatt` provides raw GATT, complete-message channels, managed peer communication
and platform drivers. It has no iroh or Tauri dependency. A standalone Rust CLI
can use the [managed example](../docs/interface.md#minimal-outgoing-use) directly.

## Working-tree status

[ADR-0012](../docs/adr/0012-discovery-like-nearby.md) is implemented in this PR:
explicit requests, mutual acceptance, endpoint/nonce arbitration,
scoped channels and keepalive records. Software checks and hardware acceptance
are separate.

## Public surfaces

- `Adapter`: shared driver, radio state, capabilities and role access.
- `Central` / `Peripheral`: raw scan, GATT client and GATT server operations.
- `datagram`: bounded fragmentation and reassembly without managed establishment.
- `advertisement`: readable GATT metadata and opt-in raw scan completion.
- `PeerLink`: one managed core coordinator with a dedicated worker runtime.
- `power`: advice about scan schedules, connection limits and priority, not radio control.

Raw central/peripheral and datagram operations remain independent. Use them for
vendor protocols or caller-owned channels. They do not acquire the managed handshake.

## Managed communication

Construct `PeerLink::new(PeerLinkConfig::new(datagram_config))`, or inject a driver
with `with_driver`. The configuration contains `datagram` and `max_links`.
Construction starts the worker; it does not certify radio availability.

Claim `incoming()` once. Use `set_advertising(true).await` for managed serving
and `discover().await` for a stream of endpoint IDs and discovered peers.
`request(&address).await` returns `ConnectionRequest`. Both sides must call
`accept().await` before either obtains `PeerChannel`; either side can reject.
Dropping the request cancels its attempt.

Either peer can initiate. The larger signed attempt nonce wins crossed requests;
equal nonces fail both. The outgoing loser fails independently of the incoming
winner. No public role-ranking, tracking or retry-policy mode remains.
The existing worker was refactored; there is no second manager beside it.

Channels carry bidirectional complete messages. Clones share a receive queue.
`release().await` releases scoped use and retires the last use.
`disconnect().await` invalidates that channel generation; the link-level
`disconnect(endpoint).await` ends current communication for that endpoint.
`close_reason()` distinguishes terminal disconnect from unexpected link loss.
A terminal request does not restart automatically.

Application receive pressure waits in that generation's existing transport task,
not the shared worker. The wait is bounded and cancellable; it does not invent
payload loss. Known `ReceiveGap` differs from terminal lifecycle loss. Native/mock
`Driver::lifecycle_events()` isolates payload traffic; the third-party default
only filters legacy events. See [event-stream limits](../docs/interface.md#driver-event-streams-and-receive-loss).

Dropping discovery or disabling advertising preserves established communication.
Visibility control retains the native server and subscriptions. Raw server stop
is destructive. Finishing a message does not close a healthy channel.

## Modules and drivers

```text
src/
  entities/       Types, typed outcomes and role/session events
  hal/            Driver and GattConnection ports
  roles/          Adapter, Central and Peripheral
  connection/     Owned Connection, PeerLink worker and managed codec
  profile/        Datagram framing and advertisement completion
  power/          Power-profile advice
  drivers/        Linux/BlueZ, Android/JNI, Windows/WinRT and mock/broker
```

The unused crate-private `connection/link_state.rs` implementation is retired.
Historical ADR-0005 remains unchanged. Apple drivers are not implemented.

Android needs compatible native packaging and Application context. The Kotlin
bridge currently ships in [the Tauri plugin](../tauri-plugin-ble-gatt/android).
Rust/JNI and Kotlin require bridge ABI 8. Host-neutral packaging remains open.

## Ownership and validation limits

Drivers own OS objects, callbacks, serialized native effects and session-generation
guards. Core coordination does not replace these protections.
Raw `Connection` Drop cleanup is asynchronous and needs a Tokio runtime.
Await explicit cleanup when completion matters.

Applications own permitted peers, acceptance, pairing and data protocols.
iroh supplies authentication, encryption, streams and paths in its composition.
The iroh adapter translates packets and accounts for users of the same core
carrier; it does not independently arbitrate native connections.

Mock checks, host builds, packaged JNI execution, native-session observations
and physical teardown are separate evidence. Role capability booleans do not
prove simultaneous-role hardware support. The datagram server still admits one
inbound peer; `MaxLinks` does not remove that limit.

See [API usage](../docs/interface.md), [architecture](../docs/architecture.md),
[glossary](../docs/glossary.md) and [hardware verification](../docs/hardware-verification.md).

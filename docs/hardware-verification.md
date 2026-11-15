# Hardware verification

Verify raw datagram examples and managed communication in
[ADR-0012](adr/0012-discovery-like-nearby.md) separately. This guide lists required
checks, not passed scenarios.

## Status and scope

The shared iroh diagnostic now uses core managed discovery, requests and
independent incoming acceptance. Hardware acceptance remains pending;
software checks are separate. Old candidate-model results
do not certify the new protocol. Windows acceptance is listed in
[ADR-0011](adr/0011-windows-winrt-driver.md#verification-and-known-limits).

Historical discovery, release-JNI and crossed-start failures describe specific runs,
not permanent platform limits. Zero scan callbacks in one emulator run do not
prove universal emulator failure. Missing native scan results block discovery;
arbitration cannot repair them.

Device testing requires separate authorization and exact build/input provenance.
This documentation revision changes no hardware, phone settings or consumer source.
Source checks, mocks, packaged JNI, native events and physical teardown prove different things.

## Software checks for the current implementation

Suggested checks from the repository root; this guide does not record them as run:

```sh
cargo test --workspace --locked --offline
cargo test -p ble-gatt --test peer_link --test peer_protocol --locked --offline
cargo test -p ble-gatt --test advertising_visibility --test native_advertisement_state --locked --offline
cargo test -p ble-gatt --features mock-broker --locked --offline
cargo test -p tauri-plugin-ble-gatt --test jni_parity --locked --offline
cargo test -p ble-gatt-iroh --test hardware_diagnostic --locked --offline
cargo build -p ble-gatt-iroh --example hw_iroh_peer --locked --offline
```

The shared runner is `ble-gatt-iroh/examples/support/iroh_diagnostic.rs`.
The Linux entrypoint, sample Android application and integration tests use it.
The radio-free tests run actual iroh endpoints over mock GATT; this proves the
managed software path, not physical crossed-attempt safety or OS teardown.

## Android diagnostic packaging

Use the sample's normal SDK/NDK/JDK prerequisites:

```sh
cd examples/tauri-app
npm ci
npm run tauri -- android init --ci
sh android-diagnostic/prepare.sh
npm run tauri -- android build --ci --debug --apk --target aarch64
```

The overlay applies only to this sample's ignored generated Android project.
Reapply it after initialization. It does not edit a consumer or the production
plugin. The debug-only, non-exported, non-sticky foreground service keeps the
process alive and stops on completion or within 190 seconds; it holds no wake
lock. The Rust diagnostic requires this process's service marker.

Use package `dev.blegatt.example.debug` and activity
`dev.blegatt.example.MainActivity`. Verify the packaged ABI, manifest and bridge
bytecode, including bridge ABI 7. Source parity alone is insufficient. Diagnostic properties
are read only by debug builds. Do not run the interactive/datagram buttons at
the same time as the iroh runner.

## Device and privacy boundary

Before device discovery, installation, launch or log capture, obtain explicit
authorization and select a connected, authorized serial. Placeholder inputs are
not verified targets. Never install over Fini or another application.

- Preserve the protected lock screen. Do not wake/unlock or automate native input.
- Do not change radio, location, power, security or background settings implicitly.
- Check existing permission prerequisites; request any needed interaction through
  the sample's foreground permission UI and the user.
- The sample's debug `neverForLocation` assertion is app-specific, not a library
  default for all consumers. A foreground service does not itself prove locked
  or background discovery eligibility on every Android version.
- Capture only the diagnostic process; do not clear unrelated logcat.
- Keep generated secrets, full keys, addresses, device identifiers and private logs
  outside tracked source. Retain third-party licensing notices.

The service's 190-second bound does not prove five-minute idle survival.
Do not extend a run by silently disabling security or keeping a display awake.

## Current runner inputs

Generate fresh diagnostic iroh identities in a private host directory outside
this repository. `hw_iroh_peer --keygen` does not touch a radio. Keep the secret
values out of commits and public logs; these are not production pairing keys.

Linux environment inputs:

- `BLE_DIAG_SECRET`: local throwaway secret.
- `BLE_DIAG_PEER`: the remote full public key.
- `BLE_DIAG_SIDE=linux`.
- `BLE_DIAG_MODE`: `phone-initiate`, `linux-initiate` or `both`.
- `BLE_DIAG_START`: optional common scheduled Unix-second start.

Android debug properties:

- Clear `debug.blegatt.role` when selecting the iroh diagnostic.
- `debug.blegatt.iroh.mode`, `.secret`, `.peer`, `.start` carry the matching
  mode, phone secret, Linux public key and optional schedule.
- Only after install/run authorization, launch the diagnostic package. Between
  runs, stop only that package; clear only its own debug properties afterward.

Use the built binary with those Linux inputs. The main scenario's 180-second
timeout includes discovery and scheduled waiting. Some early returns and final
shutdown awaits remain outside it; failure cleanup is not fully bounded.
Start the host promptly after the phone, or use separate terminals and a common schedule.
Check clocks read-only. Discovery must complete before the scheduled time for
truly crossed native attempts; the schedule alone proves nothing about overlap.

The side/mode mapping must be demonstrated by native callbacks:

- `phone-initiate`: phone central, Linux peripheral.
- `linux-initiate`: Linux central, phone peripheral.
- `both`: both applications initiate; this does not imply two physical links.

Do not install an incoming private source address as an outgoing discovery
mapping. A short advertisement fingerprint is only a prefilter; iroh TLS
checks the configured public key.

## Required evidence for the current iroh runner

Each initiator opens two authenticated QUIC connections, transfers a synthetic
4096-byte payload with distinct rounds, verifies exact echoes and confirms
that connection reuse does not increase its dial-attempt counter. It closes
only the first QUIC connection, waits beyond the unused-link backstop, and
proves the survivor still transfers round 3 without another dial.

Correlate both peers' logs:

- `START`, `READY` and authenticated `QUIC` events. Android additionally needs
  `EitherPeerService READY` and the marker from the current process.
- Native central/client and peripheral/server callbacks and session generations,
  not just requested modes or QUIC incoming/outgoing labels.
- Initiator `PAYLOAD matched` for rounds 1/2/3, `REUSE`, and `SURVIVOR`.
  Listener evidence includes distinct received rounds with `echo_ack=true` and
  `rounds_mask=111`, not repeats of one round.
- Stream `send.stopped() == Ok(None)` is receipt acknowledgement, not proof that
  the remote application's business operation completed.
- Before shutdown, `NATIVE_CLEANUP active_sessions=0 ... evidence=driver_events
  before_shutdown=true`, then `CLEANUP address_queues=0 ... before_shutdown=true`
  and joined incoming carrier tasks. Lagged/missing lifecycle evidence fails.
- The current `PASS payload-reuse-survivor-cleanup` remains software evidence
  with `native_teardown=unverified` unless all native/OS sessions are separately
  correlated. An empty queue map or observed ready-session closure is not proof
  of every raw ACL/session ending. Shutdown-induced teardown is not last-user
  cleanup. Emergency force-close cannot turn a failure into a pass.

A unilateral listener need not scan/dial or emit initiator-only markers. Both
peers in `both` mode must satisfy the applicable evidence. A surviving QUIC
handle alone does not prove unchanged native carrier continuity; the remote
peer or OS may have replaced it. A failed required role is not a pass because
another role subsequently connected. Repeat after cleanup for reconnect evidence.

## Managed core: separate acceptance gates

The working tree contains non-iroh protocol tests and the migrated adapter.
Require fresh evidence, independent of historical diagnostic passes, for:

1. Non-iroh establishment in either direction and matching endpoint correlation.
2. Crossed attempts with both nonce orderings; equal nonces fail both.
3. Losing outgoing failure/rejection and separate accepted incoming delivery;
   no silent success handoff and no false global disconnection.
4. Rejection, duplicate requests, cancellation and explicit disconnect without
   unintended reconnect. Old QUIC retransmissions must not authorize replacement;
   fresh explicit connects/attachment can authorize it. Test unexpected loss separately.
5. Stale completion, replacement generations and loser cleanup preserving the
   winner, existing users and healthy established communication.
6. Discovery metadata reads while another user is active, bounded failures,
   stopping discovery/advertising during communication and idle liveness.
7. Version mismatch/control framing and opaque payload preservation; raw vendor
   GATT must remain unaffected.
8. iroh multi-user/path accounting with core-owned establishment, then native
   visibility, cancellation and physical checks on each required platform pair.

Review metadata partial failure and WinRT watcher overflow/cache behavior.
Source ownership guards and ABI checks do not prove native cancellation,
compiled JNI descriptors, sample packaging or callback timing.

Keep unsupported capabilities and the current single-inbound-peer profile limit
explicit. Do not claim Cluster/M-to-N, locked-device reliability or exhaustive
physical teardown from a mock or emulator pass.

## Existing datagram echo example

`ble-gatt/examples/hw_peer.rs` and the sample's `src-tauri/src/hw_verify.rs`
still provide the older fragmented echo path. An authorized Linux run selects
`cargo run -p ble-gatt --example hw_peer -- --role peripheral` or `--role central`.
The sample selects `debug.blegatt.role` instead of the iroh properties.

Look for `HWVERIFY: READY`, `HWVERIFY: PASS round-trip complete`, or failure
markers. One round trip proves that scenario, not churn/recovery, mixed-role
concurrency, background scanning or the new handshake. Do not mix this run with
the iroh diagnostic or infer its current results from old logs.

## Prerequisite references

- https://developer.android.com/develop/connectivity/bluetooth/bt-permissions
- https://developer.android.com/develop/background-work/services/fgs/service-types

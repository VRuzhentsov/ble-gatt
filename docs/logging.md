# Logging

This library is meant to be diagnosable from a log dump alone. That is not a
nicety: no two-device round trip has ever been confirmed on real radios (see
`hardware-verification.md`), and on the platform that matters most — Android,
inside a released app — attaching a debugger is not an option. The logs are
the verification surface.

Everything goes through the [`log`](https://docs.rs/log) facade. A consumer
that installs no logger pays nothing and sees nothing.

## Targets

Targets are module paths, so they filter naturally:

| Target | Covers |
| --- | --- |
| `ble_gatt::datagram` | fragmentation, reassembly, channel and session lifecycle |
| `ble_gatt::backend::linux` | BlueZ: advertising, discovery, MTU, notify sessions |
| `ble_gatt::backend::android` | JNI boundary in both directions |
| `tauri_plugin_ble_gatt::android_lazy` | deferred backend construction |

```bash
RUST_LOG=ble_gatt=debug                    # the whole library
RUST_LOG=ble_gatt::datagram=trace          # per-fragment detail only
RUST_LOG=ble_gatt=info,ble_gatt::backend=debug
```

## Levels

Chosen so that **`info` alone tells the story of a healthy session**, and a
failure is visible at `warn` without turning anything up.

| Level | Contains | Volume |
| --- | --- | --- |
| `error` | JNI attach failure, capability probe failure, scan rejected by the platform | never in a healthy run |
| `warn` | recoverable anomalies: refused central, superseded session, dropped fragment, queue overflow, failed write | never in a healthy run |
| `info` | lifecycle: capabilities, advertise start/stop, scan start, peer discovered, connect, MTU negotiated, subscribe, channel ready, disconnect | ~10 lines per session |
| `debug` | per-message: send/recv byte counts, message ids, fragment counts | one line per message |
| `trace` | per-fragment writes and notifies, ignored scan results, reassembly completion | one line per fragment |

`error` and `warn` are reserved for things that are actually wrong. A busy
`info` run is a working run; any `warn` is a lead.

## Reading a healthy session

Central side:

```
INFO ble_gatt::backend::linux  capabilities: central=true peripheral=true
INFO ble_gatt::backend::linux  scan: starting discovery for service 0000b1e6-…
INFO ble_gatt::backend::linux  scan: discovered AA:BB:… name=Some("…") rssi=Some(-52)
INFO ble_gatt::datagram        connect: dialling AA:BB:… service=0000b1e6-…
INFO ble_gatt::backend::linux  connect: link established to AA:BB:… session=3
INFO ble_gatt::backend::linux  subscribe: AA:BB:… negotiated ATT MTU 517
INFO ble_gatt::datagram        connect: channel ready to AA:BB:… session=Some(3) fragment_budget=506 max_message_len=…
DEBUG ble_gatt::datagram       send: 512 bytes to AA:BB:… as msg_id=0 in 2 fragment(s)
DEBUG ble_gatt::datagram       recv: 512 bytes from AA:BB:…
```

Peripheral side:

```
INFO ble_gatt::datagram        serve: advertising service 0000b1e6-…
INFO ble_gatt::backend::linux  advertise: registering service 0000b1e6-… with 1 characteristic(s)
INFO ble_gatt::backend::linux  advertise: registered, generation=2
INFO ble_gatt::datagram        serve: advertising accepted, awaiting centrals
INFO ble_gatt::backend::linux  notify session: central AA:BB:… subscribed to 0000b1e7-… session=4
INFO ble_gatt::datagram        serve: accepted central AA:BB:… session=Some(4) generation=0 fragment_budget=…
```

Each line names the peer and, where one exists, the session — because the
bugs this library has actually had were session-identity bugs, and a log
that omits the session cannot distinguish a stale event from a current one.

### Localising a failure

The sequence is the diagnostic. Find the last line that appeared and the
first that did not:

| Last line seen | What failed |
| --- | --- |
| `capabilities: …peripheral=false` | the adapter cannot advertise; nothing downstream will work |
| `scan: starting discovery` with no `discovered` | nothing is advertising the service, or reports are not reaching the host |
| `connect: dialling` with no `link established` | the peer refused or vanished mid-dial |
| `link established` with no `negotiated ATT MTU` | service discovery or the characteristic lookup failed |
| `channel ready` with no `send` | the caller never sent; not a transport problem |
| `send` with no matching `recv` on the peer | fragments left but did not arrive — check the peer's `reassembly:` warnings |

## Android

`eprintln!` was previously used for diagnostics. On Android stderr is not
captured by logcat, so those messages went nowhere on the one platform where
they were most needed — that is why they are all `log` records now.

Records reach logcat through `tauri-plugin-log`, which must be registered
**outside** any `debug_assertions` guard for a release build to log at all.
The JNI callbacks are logged on entry (`jni on…`), so a break between Kotlin
and Rust is visible as a Kotlin-side log line with no Rust counterpart.

## Privacy

Peer addresses and advertised names are logged. Both are already broadcast
in the clear over the air, so this reveals nothing an observer with a radio
does not have — but it does mean these logs identify nearby devices, and
they should be treated accordingly before being attached to a bug report.
Payload **contents** are never logged; only byte counts.

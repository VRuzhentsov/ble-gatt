# Isolated EitherPeer hardware diagnostic

This extends the existing examples, not Fini. Linux runs
`ble-gatt-iroh/examples/hw_iroh_peer.rs`; the Android example includes the exact
same application-level runner. Both serve/attach **and** provide `dial_with`,
select `InitiationPolicy::EitherPeer`, and install that transport's hooks on
that same iroh endpoint. There is no `PeerLink`, new handshake, pairing store,
application database, or production identity. IP transports and relays are
explicitly disabled.

## Build and radio-free checks

From the repository root:

```sh
cargo test -p ble-gatt-iroh --test hardware_diagnostic
cargo build -p ble-gatt-iroh --example hw_iroh_peer
```

The tests execute this runner with two actual iroh endpoints over the in-process
mock radio: either unilateral initiator (always the larger key), and a scheduled
crossed application start. A mock pass does not prove mixed-role hardware support.

Android uses the existing Tauri example and its normal SDK/NDK/JDK prerequisites:

```sh
cd examples/tauri-app
npm ci
npm run tauri -- android init --ci
sh android-diagnostic/prepare.sh
npm run tauri -- android build --ci --debug --apk --target aarch64
```

`prepare.sh` only copies the checked-in example lifetime overlay to this example's
ignored generated Android project. Reapply it after `android init`. It does not
edit the library plugin or another application's generated project. The manifest
service and its two foreground-service permissions are **debug-only**. The
activity starts it only in a debug diagnostic launch. The service is non-exported,
not sticky, and stops on completion or within 190 seconds; it holds no wake lock
and changes no radio, location, keyguard, power, or security settings. Rust requires
a marker from this process's running foreground service before using the adapter.
The existing `.debug` application-ID suffix is retained, independent of release.
The generated example's minimum SDK is 24, so its manifest permits Android 10
(API 29); verify the packaged manifest rather than assuming an init command
rewrites an already-generated Gradle project.

## Before any phone action

Device discovery, installation, launch, and log capture require explicit parent/
user authorization and a connected, authorized target. **No command below is an
automatic preparation step.** Select the verified phone serial, never a model
name. Do not touch any other phone or install over `com.fini.app` or its debug
variant. The only install target is `dev.blegatt.example.debug`; the activity class
is still `dev.blegatt.example.MainActivity`.

Check the existing permission and location prerequisites before running:

* The plugin already declares legacy Bluetooth/admin and `ACCESS_FINE_LOCATION`
  through API 30, and Bluetooth scan/connect/advertise on API 31+. This diagnostic
  neither adds location permissions nor silently grants any permission. Its
  debug manifest asserts `neverForLocation` for scanning: it carries bytes and
  does not derive physical location. Without that app-owned assertion, API 31+
  may deliver no scan callbacks despite a Bluetooth grant, since this app does
  not request fine location above API 30. Android may filter some beacon formats
  under `neverForLocation`; arbitrary library consumers must choose their own
  location/scan policy rather than inheriting that assertion from the plugin.
* Approve the existing plugin permissions using the example's foreground UI
  (`request existing Bluetooth permissions` explicitly invokes the plugin's
  permission API), if needed. Ask the
  user for that brief manual interaction; do not unlock or automate screen input.
* A connected-device foreground service is process lifetime, **not proof of
  background discovery eligibility**. Android's Bluetooth-permissions guide
  documents a background-location requirement for service-based discovery on
  Android 10/11. This example intentionally does not add that permission or a
  location service. Missing callbacks while locked remain a separate prerequisite/
  discovery blocker, not an EitherPeer failure or permission to change settings.
  Consult the Android Developers Bluetooth permissions and foreground-service
  type documentation before separately approving any follow-up change.
* Record current location/radio/permission and keyguard state read-only. Preserve
  it. Starting the app does not authorize waking/unlocking the phone or changing
  any setting. A foreground transfer does not establish locked scanning.

## Fresh diagnostic identities and exact launch inputs

Use a private host directory outside the repository. `--keygen` uses iroh's
`SecretKey::generate`; it does not initialize a radio. Keys are throwaway test
identities, not Fini keys. Never put their values into a commit or public log.
The examples below intentionally use placeholders rather than device identifiers.

```bash
set -o pipefail
umask 077
BIN='<absolute-path-to-built-hw_iroh_peer>'
RUN='<private-host-run-directory>'
mkdir -p "$RUN"
"$BIN" --keygen > "$RUN/linux.env"
"$BIN" --keygen > "$RUN/phone.env"
. "$RUN/phone.env"
PHONE_SECRET=$BLE_DIAG_SECRET
PHONE_PUBLIC=$BLE_DIAG_PUBLIC
. "$RUN/linux.env"
LINUX_PUBLIC=$BLE_DIAG_PUBLIC
# BLE_DIAG_SECRET now contains the Linux-only diagnostic secret.

SERIAL='<verified-authorized-phone-serial>'
APK='<absolute-path-to-verified-arm64-debug-apk>'
MODE=phone-initiate  # repeat separately with linux-initiate, then both
# Optional common scheduled start, especially for both; confirm host/phone clocks
# read-only first. Discovery must complete before this time for crossed BLE dials.
START=$(date -d '+45 seconds' +%s)

# Only after the parent confirms install/run authorization:
adb -s "$SERIAL" install -r "$APK"
adb -s "$SERIAL" shell am force-stop dev.blegatt.example.debug
adb -s "$SERIAL" shell setprop debug.blegatt.role ''
adb -s "$SERIAL" shell setprop debug.blegatt.iroh.mode "$MODE"
adb -s "$SERIAL" shell setprop debug.blegatt.iroh.secret "$PHONE_SECRET"
adb -s "$SERIAL" shell setprop debug.blegatt.iroh.peer "$LINUX_PUBLIC"
adb -s "$SERIAL" shell setprop debug.blegatt.iroh.start "$START"
adb -s "$SERIAL" shell am start -n dev.blegatt.example.debug/dev.blegatt.example.MainActivity

BLE_DIAG_SECRET="$BLE_DIAG_SECRET" BLE_DIAG_PEER="$PHONE_PUBLIC" \
BLE_DIAG_SIDE=linux BLE_DIAG_MODE="$MODE" BLE_DIAG_START="$START" \
  "$BIN" 2>&1 | tee "$RUN/linux-$MODE.log"
```

Run the host command promptly after the phone launch, or in a separate terminal.
Both runs are bounded to 180 seconds including discovery and scheduled waiting.
Property values are read once; force-stop **only this diagnostic package** between
runs. Do not run the datagram UI buttons concurrently with the iroh harness.
With the iroh mode property absent the existing datagram/interactive example is
unchanged. Only debug builds honor the iroh properties.

Capture logs without clearing unrelated logcat. Resolve the diagnostic PID and
capture its process only (start the capture early enough to retain startup):

```bash
PID=$(adb -s "$SERIAL" shell pidof dev.blegatt.example.debug | tr -d '\r')
adb -s "$SERIAL" logcat --pid="$PID" -v threadtime > "$RUN/phone-$MODE.log"
# In a separate shell, read-only service/lifetime and keyguard observations:
adb -s "$SERIAL" shell dumpsys activity services dev.blegatt.example.debug
adb -s "$SERIAL" shell dumpsys window policy
```

The logcat command is intentionally an operator-controlled capture; stop it after
the bounded test. This documentation does not authorize any lasting host process.
For repeated runs, generate fresh keys again. To disable future diagnostic runs,
after the authorized session clear only its own debug properties:

```bash
for prop in mode secret peer start; do
  adb -s "$SERIAL" shell setprop "debug.blegatt.iroh.$prop" ''
done
```

## Assertions and interpretation

Each initiator opens two authenticated QUIC connections. It sends a 4096-byte
synthetic payload with its full public key and a round number, requires a
byte-identical echo, and confirms the second connection caused no extra dial.
It closes only the first QUIC connection, waits three seconds (longer than the
two-second unused-link backstop), then proves the second still transfers the
correct payload **and** that the dial-attempt counter remains unchanged through
the wait and successful round-3 echo. A surviving QUIC handle alone is insufficient:
QUIC can recover over a replacement BLE candidate. Finally both connections end.
Before endpoint/router/adapter shutdown, software acceptance requires the current
address-to-queue map to be empty and all retained incoming candidate task handles
to finish and join normally within the cleanup deadline. The runner also
subscribes to driver lifecycle events **before** serving or dialling, tracks
address/role/session tuples, and waits for every observed session to disconnect.
`NATIVE_CLEANUP active_sessions=0 ... evidence=driver_events before_shutdown=true`
is required before shutdown; missing lifecycle evidence or a lagged event stream
fails closed. A concrete stale disconnect cannot remove a newer session; drivers
without disconnect session IDs use the documented address/role fallback. It
rechecks and joins late candidates after that wait. Ordinary cleanup never
calls `close_links`; emergency shutdown cannot turn a failed check into success.
`CLEANUP address_queues=0` does **not** mean every physical/native session is gone:
outgoing task handles are internal to the adapter, and the serving listener is
still running at the evidence snapshot. Hardware acceptance additionally requires
native session/generation teardown evidence **before shutdown**, including any
superseded candidates. There are no production records to create, read, or clean up.
Each listener requires distinct acknowledged rounds 1, 2 and 3, not three copies
of one round. Only `send.stopped() == Ok(None)` establishes stream-data receipt
acknowledgement (not remote application processing); a stop code or error fails.

### Common evidence on both peers

* `START` identifies full local/remote public keys, `EitherPeer`, and IP/relay off.
* `READY`; on Android also `EitherPeerService READY` plus a running foreground
  service. Its private marker includes the current PID, not a stale readiness flag.
* Authenticated `QUIC` lines and applicable `CANDIDATE role=central` /
  `ATTACH role=peripheral` lines identify the actual datagram direction. Match
  these with native Android client/server callbacks and native Linux/Android
  session/generation handles, not merely the requested mode. A QUIC outgoing
  connection can reuse a peripheral-role BLE link.
* `NATIVE_CLEANUP active_sessions=0 observed_sessions=... evidence=driver_events
  before_shutdown=true`, then `CLEANUP address_queues=0 incoming_candidates_joined=...
  before_shutdown=true`, followed by `PASS payload-reuse-survivor-cleanup`. Driver
  events prove observed ready-session closure, not every OS-level connection:
  Android's server can report an unsubscribed ACL connection that never became
  a ready peripheral session. This PASS remains software evidence only
  (`native_teardown=unverified`). For a hardware pass,
  correlate **all** native candidate/session releases before endpoint/router/
  adapter shutdown. Shutdown-induced teardown is not last-user cleanup proof.
* Native carrier/session continuity through the surviving transfer is mandatory
  for hardware acceptance, even when the local dial counter is unchanged: the
  other peer or platform could have replaced the physical carrier.
* A timeout or failed role is not a pass merely because an opposite-role
  connection subsequently appeared.

### Initiator evidence (phone, Linux, or both as selected)

* `DISCOVERY` / `MAP` use an actual advertised address and matching identity hint.
  The five-byte manufacturer hint fits legacy advertising; if absent, the
  library's Nearby-style GATT-advertisement read completes the record. The hint
  is only a prefilter; iroh TLS authenticates the full supplied key. An incoming
  private source address is never installed as an outgoing discovery mapping.
* `PAYLOAD matched` for rounds 1/2/3, `REUSE`, and `SURVIVOR ... unchanged=true`.
  Match stable QUIC handles and dial-attempt counts across connection 2, first
  close, idle wait, and round-3 echo; require the native continuity evidence above.

### Listener evidence (the non-initiator, or both in `both` mode)

* Authenticated incoming QUIC connections and `PAYLOAD received` for distinct
  rounds 1/2/3 with `echo_ack=true`, reaching `rounds_mask=111`.
* A unilateral listener need not discover, install an outgoing map, dial, emit
  `PAYLOAD matched`, `REUSE`, or `SURVIVOR`. The initiator's correlated logs prove
  those checks. In `both` mode each peer must satisfy both role checklists.

`phone-initiate` requires phone-central/Linux-peripheral native evidence;
`linux-initiate` requires the reverse. `both` means both applications connect,
not a claim that two physical links must form: iroh may reuse the incoming path.
Use the native attempt timestamps to distinguish truly crossed physical dials
from successful incoming-path reuse. A common scheduled Unix-second start keeps
the fractional part of each peer's current time when calculating its delay;
truncating it would add a different discovery-time fraction to each start (up
to one second of skew). Device clocks must still agree; the schedule alone is
not simultaneous-native-dial evidence. Repeat after cleanup for reconnect evidence.
A single run does not establish long-term stability, locked discovery, recovery
from a radio toggle, or platform mixed-role support. Never toggle a radio as an
unannounced extension of this test.

Official prerequisite references (not authorization to change permissions):

```text
https://developer.android.com/develop/connectivity/bluetooth/bt-permissions
https://developer.android.com/develop/background-work/services/fgs/service-types
```

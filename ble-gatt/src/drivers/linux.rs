//! Linux backend: BlueZ via `bluer` (async D-Bus binding, 442+ GitHub stars,
//! the standard pure-Rust BlueZ interface). Supports both central and
//! peripheral role, since BlueZ's D-Bus GATT API exposes both — see the
//! plan's library-research table for why no other permissively licensed
//! crate covers peripheral mode on every platform this project targets.
//!
//! Connection-lifecycle events (`GattEvent::Connected`/`Disconnected`) are
//! derived from GATT activity (first characteristic write from a device,
//! and a notify session stopping), not from BlueZ's own device-connection
//! D-Bus signal — that would need a second, independently-driven
//! `Device::events()` watcher per connected peer. Good enough for Stage 1
//! (peer lifecycle in Fini's integration is driven by its own auth
//! handshake over the link, not by this event), revisit if a consumer needs
//! precise link-level connect/disconnect timing.

use std::collections::{HashMap, HashSet};

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use bluer::adv::Advertisement;
use bluer::gatt::local::{
    Application, Characteristic as LocalCharacteristic, CharacteristicNotify,
    CharacteristicNotifyMethod, CharacteristicRead, CharacteristicWrite, CharacteristicWriteMethod,
    Service as LocalService,
};
use bluer::gatt::local::{
    ApplicationHandle, CharacteristicControl, CharacteristicControlEvent, ReqResult,
};
use bluer::gatt::CharacteristicWriter;
use bluer::{Adapter, AdapterEvent, Session};
use futures::stream::StreamExt;
/// How often a served peer's notify sessions are checked for closure. A
/// central can unsubscribe without disconnecting, which ends the session as
/// surely as a link drop but produces no device property change.
const NOTIFY_SESSION_POLL: std::time::Duration = std::time::Duration::from_millis(500);

/// How long `notify_matching` waits for a peer's `AcquireNotify` writer to
/// register before giving up, when the peer is already a known served
/// session (see the module doc comment on why `Connected` is derived from
/// GATT activity, not a single D-Bus signal). A central subscribes before
/// writing, and BLE delivers ATT operations on one link in order — but
/// `AcquireNotify` and a characteristic write are two independently driven
/// D-Bus round trips on the bluer side, with no ordering guarantee between
/// them even though the *air* traffic that triggered them was ordered
/// correctly. On a fresh, fast reconnect the write's handler can fire and
/// even reply before the slower `AcquireNotify` round trip completes,
/// making the very first reply to a legitimately subscribed peer fail with
/// "no live notify session" purely on timing. Bounded so a peer that
/// genuinely never subscribes (the raw-GATT-tier, write-only use case this
/// backend also serves) still fails promptly rather than stalling every
/// `notify`/`notify_peer` caller for this long.
const NOTIFY_WRITER_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const NOTIFY_WRITER_ACQUIRE_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// How long `connect()` waits for this backend's own discovery to stop
/// before dialling anyway.
///
/// Dropping a scan stream used to leave discovery running for an unknown
/// time: `bluer` sends `StopDiscovery` from a spawned task once its session
/// token drops, so a caller that dropped its scan and dialled immediately
/// raced `Connect()` against a still-running discovery. Real-hardware
/// evidence (Pixel 6 Pro <-> Linux desktop, fini PR 185): that race produced
/// `le-connection-abort-by-local` two seconds into the dial. `connect()`
/// now issues (or waits for) the `StopDiscovery` itself; bounded so a
/// BlueZ that never replies costs a dial this long rather than stalling it.
const DISCOVERY_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// D-Bus reply timeout for `SetDiscoveryFilter`/`StartDiscovery`/
/// `StopDiscovery` on the discovery connection.
const DISCOVERY_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use crate::hal::{Backend, BoxStream, GattConnection};
use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// How long `LinuxBackend::connect` waits for BlueZ's `Connect` D-Bus method
/// to reply before giving up.
///
/// Real-hardware evidence: with no bound at all, a `Connect` call to an
/// unresponsive/out-of-range peer can hang effectively forever — not just
/// past a caller's own external timeout, since `dial_lock` is held across
/// the whole call and every subsequent dial (and, evidence suggests, the
/// shared D-Bus connection's own capacity for outstanding calls) piles up
/// behind it rather than that one dial simply failing slowly. A caller-side
/// `tokio::time::timeout` around the whole `connect()` future does not fix
/// this: dropping that future does not cancel BlueZ's own in-progress
/// Connect (see `LinuxConnectGuard`'s doc comment), so the stuck D-Bus call
/// and the `dial_lock` hold both outlive the caller giving up on it.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// How long a cancelled connect attempt's cleanup waits for BlueZ's
/// `Disconnect` to reply. BlueZ documents `Disconnect` as the sanctioned way
/// to cancel a pending `Connect`, but real-hardware evidence shows it can
/// itself hit the same D-Bus timeout the stuck `Connect` was already
/// sitting on ("cleanup disconnect ... failed: ... D-Bus error ...
/// Timeout waiting for reply") — an unbounded cleanup call holding the same
/// `dial_lock` is just as capable of starving every other dial as the
/// original stuck connect was. Shorter than `CONNECT_TIMEOUT`: this is a
/// best-effort cancellation signal, not an operation worth waiting as long
/// for.
const CLEANUP_DISCONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long the delayed cleanup task (armed once `CLEANUP_DISCONNECT_TIMEOUT`
/// itself elapses) waits between retries of its own `Disconnect` call, while
/// an address stays quarantined in `pending_cleanup`. Not itself bounded by
/// a timeout — see that task's own doc comment for why a definitive Err
/// must not lift the quarantine either.
const PENDING_CLEANUP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Hard cap on how many `connect()` attempts this backend will allow to be
/// outstanding at once with no guaranteed way to clean up after them if
/// abandoned or timed out. Codex P1 ("bound cleanup attempts across peer
/// addresses", then "reserve cleanup capacity before issuing Connect"):
/// bounding concurrency to one live disconnect attempt *per address*
/// (`LinuxConnectGuard::drop`'s own retry loop) leaves no bound *across*
/// addresses, and acquiring that bound *lazily* — only once a connect
/// attempt has already failed and its cleanup task has already been spawned
/// — bounds nothing either: a connect attempt that cannot get cleanup
/// capacity has, by then, already been issued to BlueZ and already left
/// with a cleanup task permanently queued behind the semaphore instead of
/// ever sending the `Disconnect` needed to cancel it. So the permit is
/// reserved by `connect()` itself, before `device.connect()` is ever
/// issued (see the reservation site there) — a `connect()` call that
/// cannot obtain one is rejected outright rather than proceeding into an
/// attempt this backend cannot guarantee it can ever clean up.
const MAX_CONCURRENT_CLEANUP_DISCONNECTS: usize = 4;

/// `read`/`write_with_type` do **not** retry a GATT operation BlueZ rejects
/// — that used to live here as an internal exponential backoff (6 attempts,
/// 200ms doubling to 2000ms, ~7s total), modeled on Android's real-hardware-
/// proven `gattBusyRetryDelayMs`/`GATT_BUSY_MAX_RETRIES`. It was removed:
/// fini's peer traced its two real call sites (the add-mode scan pass and
/// `find_peer_address`'s per-candidate probe) and confirmed both cap a
/// single GATT operation at ~4s, so a ~7s internal retry chain could never
/// run to completion inside either caller's actual timeout — any dial that
/// needed the later attempts just looked like a caller-abandoned future
/// instead of a real retry outcome, and there is no fixed internal budget
/// that suits every caller. Retrying is now the caller's decision, sized to
/// whatever budget that specific caller actually has: this returns after a
/// single attempt, classified via `is_transient_gatt_error`/
/// `BleError::GattBusy` below so a caller that wants to retry can tell a
/// worth-retrying rejection apart from a permanent one without guessing.
///
/// Real-hardware evidence motivating retrying *at all* still stands: a
/// central-role write to a peer that just finished connecting (services
/// already resolved — `find_characteristic` waits for that itself) can
/// fail immediately with BlueZ's own "Not connected", 100% reproducibly, on
/// the very first fragment of the very first message on a fresh channel.
/// `bluer` maps this to the generic `ErrorKind::Failed`, not a
/// distinguishable "not ready yet" kind.
///
/// True only for the specific `bluer::ErrorKind` that represents a
/// transient, momentary GATT rejection worth a caller retrying, surfaced as
/// `BleError::GattBusy` rather than `BleError::Gatt`. A structured,
/// permanent refusal — `NotAuthorized`, `NotPermitted`, `NotSupported`, and
/// the like — is never worth retrying, so only the generic `Failed` kind
/// (BlueZ's own catch-all for a transient, momentary rejection) is treated
/// as busy.
fn is_transient_gatt_error(err: &bluer::Error) -> bool {
    err.kind == bluer::ErrorKind::Failed
}

/// True when `err` is BlueZ reporting `org.bluez.Error.NotConnected` — a
/// D-Bus error name with no dedicated `bluer::ErrorKind` variant, so it
/// surfaces as `ErrorKind::Internal(InternalErrorKind::DBus(name))` with
/// the original D-Bus error name preserved verbatim in `name` (see
/// `bluer::Error`'s `From<dbus::Error>` impl). Matched structurally on that
/// name rather than on `err.message()`'s free-form text, which BlueZ does
/// not guarantee stays stable.
fn is_not_connected(err: &bluer::Error) -> bool {
    matches!(
        &err.kind,
        bluer::ErrorKind::Internal(bluer::InternalErrorKind::DBus(name))
            if name == "org.bluez.Error.NotConnected"
    )
}

/// True when `err` is BlueZ's `org.bluez.Error.InProgress` — which
/// `StartDiscovery` returns when the calling D-Bus client is *already* in
/// BlueZ's discovery list.
fn is_in_progress(err: &bluer::Error) -> bool {
    err.kind == bluer::ErrorKind::InProgress
}

/// Maps a failure to start discovery. A busy adapter is not a missing one:
/// only `InProgress` becomes `AdapterBusy`, everything else keeps the
/// existing `AdapterUnavailable` mapping.
fn discovery_error(err: bluer::Error) -> BleError {
    if is_in_progress(&err) {
        BleError::AdapterBusy(err.to_string())
    } else {
        BleError::AdapterUnavailable(err.to_string())
    }
}

/// True when a failed `StartDiscovery` proves this client is not
/// discovering. Not for `InProgress` (BlueZ still lists the client) nor for
/// a reply timeout (`org.freedesktop.DBus.Error.NoReply`: BlueZ may have
/// accepted it) — in both, a stop stays owed.
fn start_proved_not_running(err: &bluer::Error) -> bool {
    let no_reply = matches!(
        &err.kind,
        bluer::ErrorKind::Internal(bluer::InternalErrorKind::DBus(name))
            if name == "org.freedesktop.DBus.Error.NoReply"
    );
    !is_in_progress(err) && !no_reply
}

/// This backend's own view of its discovery. Only touched briefly, never
/// across an `.await`; `DiscoveryBus::op` is what orders the D-Bus calls.
#[derive(Debug, Default)]
struct ScanState {
    /// Scan streams handed out and not yet dropped.
    live: usize,
    /// Our last `StartDiscovery` succeeded and no `StopDiscovery` has
    /// succeeded since. Stays set after a *failed* stop while the adapter
    /// still reports discovering, so the stop stays owed and the next
    /// `connect()` retries it instead of dialling into that discovery.
    running: bool,
}

impl ScanState {
    /// Whether discovery is on with no scan left to use it — i.e. a
    /// `StopDiscovery` is owed.
    fn idle_running(&self) -> bool {
        self.live == 0 && self.running
    }
}

/// Discovery, on a D-Bus connection this backend owns instead of `bluer`'s.
///
/// `bluer` ignores `StopDiscovery`'s result (it is only trace-logged inside
/// `Adapter::discovery_session`) and exposes neither a public
/// `StopDiscovery` nor its connection to send one on. If that call failed,
/// BlueZ kept our client in its discovery list while `bluer` believed the
/// session ended, and every later `StartDiscovery` was refused with
/// `InProgress` ("Operation already in progress") — nothing ever sent
/// `StopDiscovery` again, so only an app restart (dropping the D-Bus
/// connection) cleared it. Real-hardware evidence (fini PR 185): 591
/// consecutive scan failures over 20 minutes after an aborted dial.
///
/// Replacing `bluer`'s session was not enough: a dropped `bluer::Session`
/// stays on the bus for ~30 s (its event loop awaits `RemoveMatch` replies
/// on a connection whose I/O task it already aborted), so BlueZ would keep
/// discovering — and racing dials — for that long. Owning the connection
/// lets this backend send `StopDiscovery` itself, see its result, and have
/// `connect()` wait for it.
///
/// BlueZ keeps discovery per D-Bus client, not per call: a second
/// `StartDiscovery` from the same client is refused with `InProgress`. So
/// concurrent scans share one discovery, started by the first and stopped
/// after the last is dropped — the refcounting `bluer` used to do.
struct DiscoveryBus {
    conn: Arc<dbus::nonblock::SyncConnection>,
    io_task: tokio::task::JoinHandle<()>,
    adapter_path: dbus::Path<'static>,
    /// The backend's own `bluer` adapter, only for reading `Discovering`.
    adapter: Adapter,
    /// Serialises every `StartDiscovery`/`StopDiscovery` together with the
    /// `ScanState` decision that led to it, so a stop can never overtake
    /// the start of a scan that has already been counted, or vice versa.
    op: AsyncMutex<()>,
    state: StdMutex<ScanState>,
}

impl DiscoveryBus {
    async fn open(adapter: &Adapter) -> Result<Self> {
        let unavailable = |err: String| BleError::AdapterUnavailable(err);
        let (resource, conn) = tokio::task::spawn_blocking(dbus_tokio::connection::new_system_sync)
            .await
            .map_err(|err| unavailable(err.to_string()))?
            .map_err(|err| unavailable(err.to_string()))?;
        let io_task = tokio::spawn(async move {
            let err = resource.await;
            log::warn!("scan: discovery D-Bus connection lost: {err}");
        });
        let adapter_path = dbus::Path::new(format!("/org/bluez/{}", adapter.name())).map_err(unavailable)?;
        Ok(Self {
            conn,
            io_task,
            adapter_path,
            adapter: adapter.clone(),
            op: AsyncMutex::new(()),
            state: StdMutex::new(ScanState::default()),
        })
    }

    async fn call(&self, method: &str, args: impl dbus::arg::AppendAll) -> bluer::Result<()> {
        let proxy =
            dbus::nonblock::Proxy::new("org.bluez", &self.adapter_path, DISCOVERY_CALL_TIMEOUT, &*self.conn);
        proxy.method_call::<(), _, _, _>("org.bluez.Adapter1", method, args).await.map_err(bluer::Error::from)
    }

    /// `SetDiscoveryFilter` + `StartDiscovery`, with the same default filter
    /// `bluer` sent (any UUID, interleaved LE/BR-EDR scan).
    async fn start_discovery(&self) -> bluer::Result<()> {
        use dbus::arg::{RefArg, Variant};
        let mut filter: HashMap<&str, Variant<Box<dyn RefArg>>> = HashMap::new();
        filter.insert("UUIDs", Variant(Box::new(Vec::<String>::new())));
        filter.insert("Transport", Variant(Box::new("auto".to_string())));
        self.call("SetDiscoveryFilter", (filter,)).await?;
        self.call("StartDiscovery", ()).await
    }

    /// Counts a new scan, starting discovery if it is not already on.
    ///
    /// `running` alone is not trusted — BlueZ drops every client's
    /// discovery when the adapter powers off, without any call of ours — so
    /// it is confirmed against the adapter's `Discovering` property.
    async fn begin_scan(self: &Arc<Self>) -> Result<ScanGuard> {
        let _op = self.op.lock().await;
        let running = self.state.lock().unwrap().running;
        let still_on = running && self.adapter.is_discovering().await.unwrap_or(false);
        if !still_on {
            // Marked *before* the call: if this future is cancelled, or the
            // reply times out, after BlueZ accepted `StartDiscovery`, the
            // stop must already be owed — otherwise the next `connect()`
            // would skip `settle_discovery` and dial into it (Codex P1 on
            // PR #21). Cleared below only when BlueZ's reply proves
            // discovery did not start.
            self.state.lock().unwrap().running = true;
            let result = match self.start_discovery().await {
                Ok(()) => Ok(()),
                // BlueZ says this client is already discovering although we
                // have not started it: a previous `StopDiscovery` failed or
                // BlueZ never saw it. Clear it and retry once.
                Err(err) if is_in_progress(&err) => {
                    log::warn!(
                        "scan: discovery failed to start: {err} — BlueZ still counts this client as \
                         discovering; sending StopDiscovery and retrying once"
                    );
                    if let Err(err) = self.call("StopDiscovery", ()).await {
                        log::warn!("scan: StopDiscovery (wedge recovery) failed: {err}");
                    }
                    self.start_discovery().await.inspect_err(|err| {
                        log::warn!("scan: discovery failed to start after StopDiscovery: {err}");
                    })
                }
                Err(err) => {
                    log::warn!("scan: discovery failed to start: {err}");
                    Err(err)
                }
            };
            if let Err(err) = result {
                if start_proved_not_running(&err) {
                    self.state.lock().unwrap().running = false;
                }
                return Err(discovery_error(err));
            }
        }
        let mut state = self.state.lock().unwrap();
        state.running = true;
        state.live += 1;
        Ok(ScanGuard(self.clone()))
    }

    /// Sends `StopDiscovery` if discovery is on and no scan is live. Waits
    /// for any start or stop already in flight first, so once this returns
    /// our client's discovery has really been stopped — or the stop failed,
    /// which is logged and leaves it owed (see `ScanState::running`).
    async fn stop_if_idle(&self, why: &str) {
        let _op = self.op.lock().await;
        if !self.state.lock().unwrap().idle_running() {
            return;
        }
        match self.call("StopDiscovery", ()).await {
            Ok(()) => {
                self.state.lock().unwrap().running = false;
                log::info!("scan: discovery stopped ({why})");
            }
            Err(err) => {
                // BlueZ refuses a stop while keeping this client listed
                // (e.g. `InProgress` while an earlier request of ours is
                // pending) — the wedge this type exists for. Only an
                // adapter that is not discovering at all proves there is
                // nothing left to stop; unreadable counts as still on.
                // `Discovering` is adapter-wide, so another process
                // scanning keeps the stop owed: each later dial then
                // retries it once, bounded by `DISCOVERY_STOP_TIMEOUT`.
                let still_on = self.adapter.is_discovering().await.unwrap_or(true);
                if still_on {
                    log::warn!(
                        "scan: StopDiscovery failed ({why}): {err} — the adapter is still discovering; \
                         the stop stays owed and is retried before the next dial"
                    );
                } else {
                    self.state.lock().unwrap().running = false;
                    log::warn!(
                        "scan: StopDiscovery failed ({why}): {err} — the adapter is not discovering, so \
                         nothing is left to stop"
                    );
                }
            }
        }
    }
}

impl Drop for DiscoveryBus {
    fn drop(&mut self) {
        self.io_task.abort();
    }
}

/// Held by a scan stream. Dropping the last one schedules `StopDiscovery`;
/// a `connect()` that runs before the scheduled task gets there sends it
/// itself (`LinuxBackend::settle_discovery`).
struct ScanGuard(Arc<DiscoveryBus>);

impl Drop for ScanGuard {
    fn drop(&mut self) {
        let idle = {
            let mut state = self.0.state.lock().unwrap();
            state.live = state.live.saturating_sub(1);
            state.idle_running()
        };
        if !idle {
            return;
        }
        // No runtime (a stream dropped outside one): the next `connect()`
        // or scan settles it instead.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let bus = self.0.clone();
            runtime.spawn(async move { bus.stop_if_idle("last scan ended").await });
        }
    }
}

pub struct LinuxBackend {
    // Held only to keep the D-Bus session connection alive for the
    // lifetime of the backend; `Adapter` clones its own `Arc` into the
    // session internals, so this field is otherwise unused.
    _session: Session,
    adapter: Adapter,
    values: Arc<StdMutex<HashMap<CharacteristicUuid, Vec<u8>>>>,
    /// Centrals currently being watched for a peripheral-role disconnect.
    /// Doubles as a spawn guard: writes are frequent, and a watcher per
    /// write would spawn an unbounded number of tasks per peer.
    served_peers: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    /// Live notify sessions, keyed by characteristic *and subscriber*.
    ///
    /// `CharacteristicNotifyMethod::Io` is used rather than `Fun` precisely
    /// because BlueZ's `AcquireNotify` carries the device address while
    /// `StartNotify` does not — so each session here has a known peer, with
    /// no inference involved.
    notify_writers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
    events_tx: broadcast::Sender<GattEvent>,
    app_handle: AsyncMutex<Option<ApplicationHandle>>,
    adv_handle: AsyncMutex<Option<bluer::adv::AdvertisementHandle>>,
    /// Aborts the notify-session watchers started by `advertise`, *and* the
    /// per-peer disconnect watchers they spawn — those were previously
    /// detached, so `stop_advertising` could not cancel them.
    /// Synchronous on purpose: it is only ever pushed to and drained, and
    /// an async mutex forced an `.await` into the middle of effects that
    /// must be indivisible with their generation check.
    server_watch: Arc<StdMutex<Vec<tokio::task::AbortHandle>>>,
    /// Session ids for served peers, so a watcher that finishes late cannot
    /// act on the session that replaced it.
    next_session: Arc<AtomicU64>,
    /// Serialises advertisement setup against teardown.
    ///
    /// `advertise` awaits two BlueZ registrations; without this a
    /// `stop_advertising` could complete in that window — invalidating the
    /// generation and clearing the installed handles — and the suspended
    /// setup would then install its own handles regardless, leaving the
    /// service advertising after stop had returned.
    advertise_lock: Arc<AsyncMutex<()>>,
    /// Serialises every GATT operation against dial and teardown.
    ///
    /// `bluer`'s `Device` is address-backed, so checking ownership and then
    /// awaiting `disconnect()` leaves a window in which a reconnect installs
    /// a new dial — and the in-flight call then drops *that* link. A plain
    /// mutex around the check cannot help, because the platform call is
    /// asynchronous; the two operations have to exclude each other for their
    /// whole duration.
    dial_lock: Arc<AsyncMutex<()>>,
    /// Which advertisement is current. The GATT application's own handlers
    /// are not registered in `server_watch` and cannot be aborted, so
    /// dropping the application handle does not stop a handler that is
    /// already running — it has to check for itself.
    ///
    /// A mutex rather than an atomic: a handler must compare the generation
    /// **and** publish without being preempted in between, and
    /// `stop_advertising` must not be able to change it mid-publication.
    /// `broadcast::Sender::send` does not block, so holding this across the
    /// send is safe.
    advertise_generation: Arc<StdMutex<u64>>,
    /// Addresses this backend dialled itself, in the central role. BlueZ
    /// reports one `Connected` property per device regardless of who
    /// initiated, so without this an outbound connection would be
    /// misreported as a central arriving at our server.
    dialed: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    /// Addresses with a cancelled connect attempt's cleanup `Disconnect`
    /// still genuinely unresolved on BlueZ's side, past
    /// `CLEANUP_DISCONNECT_TIMEOUT`. `connect()` refuses to dial a
    /// quarantined address rather than risk that stale `Disconnect`
    /// eventually landing on the replacement it would establish — see
    /// `LinuxConnectGuard::drop`'s doc comment for why timing that call out
    /// cannot simply mean "forget about it."
    pending_cleanup: Arc<StdMutex<HashSet<PeerAddress>>>,
    /// Addresses with a dial genuinely in flight right now — from the
    /// moment `connect()` accepts the attempt until it resolves (success,
    /// error, or `CONNECT_TIMEOUT`), tracked separately from `dialed`
    /// because `dialed` also legitimately holds an already-*established*
    /// generation for the whole life of a connection, which a fresh
    /// `connect()` call is allowed to supersede.
    ///
    /// Real-hardware evidence: a caller that starts a new dial to the same
    /// peer before an earlier one has resolved (rather than awaiting or
    /// cancelling it first) produces a genuinely concurrent second
    /// `Connect()` to the same `Device` — `dial_lock` only serialises them
    /// at the platform call, it does not stop a second one from being
    /// attempted at all, so both queue up, BlueZ rejects the fast loser
    /// immediately, and this repeats every retry cycle without ever
    /// establishing a link, unboundedly incrementing `dialed`'s generation
    /// counter in the process. Refusing a second dial to an address already
    /// in flight outright is cheaper and more honest than letting it queue
    /// behind `dial_lock` just to fail the same way after burning a real
    /// D-Bus round trip.
    in_flight: Arc<StdMutex<HashSet<PeerAddress>>>,
    /// Source of the permits `connect()` reserves before ever issuing
    /// `device.connect()`, and that a `LinuxConnectGuard` then holds for the
    /// life of its cleanup task if the attempt is abandoned. See
    /// `MAX_CONCURRENT_CLEANUP_DISCONNECTS`'s doc comment.
    cleanup_permits: Arc<tokio::sync::Semaphore>,
    /// See `DiscoveryBus`.
    discovery: Arc<DiscoveryBus>,
}

impl LinuxBackend {
    pub async fn new() -> Result<Self> {
        let session = Session::new()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let adapter = session
            .default_adapter()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        // An adapter that exists but is powered off is *not* a construction
        // failure: `PeerLink` needs the backend to exist so its `Powered`
        // watcher can report the adapter coming back, and `radio_status()`
        // reports the current state. Only a missing adapter (handled above)
        // is unrecoverable. Operations attempted while unpowered fail on
        // their own, honestly.
        let discovery = Arc::new(DiscoveryBus::open(&adapter).await?);
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);

        // Watch the adapter's `Powered` property and forward it as
        // `GattEvent::RadioChanged`, so `PeerLink` can project per-peer
        // `Unavailable` and a consumer stops polling `capabilities()`.
        // Unlike Android, BlueZ *does* emit `Connected(false)` on the
        // devices when the adapter powers down, so the existing per-link
        // watchers still clear `dialed` — this only adds the aggregate
        // signal.
        {
            let adapter = adapter.clone();
            let events_tx = events_tx.clone();
            tokio::spawn(async move {
                let Ok(mut events) = adapter.events().await else {
                    return;
                };
                while let Some(event) = events.next().await {
                    if let AdapterEvent::PropertyChanged(bluer::AdapterProperty::Powered(powered)) =
                        event
                    {
                        let status = if powered {
                            crate::entities::models::RadioStatus::On
                        } else {
                            crate::entities::models::RadioStatus::Off
                        };
                        log::info!("radio: adapter powered={powered}");
                        let _ = events_tx.send(GattEvent::RadioChanged { status });
                    }
                }
            });
        }

        // Watch for bluetoothd itself disappearing and coming back — a
        // crash or a package upgrade restarting the service — not just the
        // adapter's own `Powered` property above. Once bluetoothd exits
        // there is no process left to emit `Powered(false)`, so the watcher
        // above's D-Bus subscription just goes silent; nothing today ever
        // reports the radio unusable for that case, and nothing re-registers
        // the GATT application and advertisement BlueZ forgot when it died.
        // `org.freedesktop.DBus.NameOwnerChanged` for `org.bluez` is the one
        // signal the session bus itself still emits, since it is the bus
        // daemon — not bluetoothd — that notices the connection close.
        //
        // Reported as `RadioChanged` rather than a new event: from a
        // consumer's point of view "bluetoothd is gone" and "the adapter
        // powered off" mean the same thing, nothing BLE works until it comes
        // back, and `Peripheral::serve` already reacts to `RadioStatus::On`
        // by re-registering.
        {
            let events_tx = events_tx.clone();
            let conn = discovery.conn.clone();
            tokio::spawn(async move {
                let rule =
                    dbus::message::MatchRule::new_signal("org.freedesktop.DBus", "NameOwnerChanged");
                let Ok(matched) = conn.add_match(rule).await else {
                    log::warn!(
                        "bluez watch: could not subscribe to NameOwnerChanged; restart recovery is disabled"
                    );
                    return;
                };
                let (_token, mut stream) = matched.msg_stream();
                while let Some(msg) = stream.next().await {
                    let (name, _old_owner, new_owner) = msg.get3::<&str, &str, &str>();
                    if name != Some("org.bluez") {
                        continue;
                    }
                    let status = match new_owner {
                        Some(owner) if !owner.is_empty() => crate::entities::models::RadioStatus::On,
                        _ => crate::entities::models::RadioStatus::Off,
                    };
                    log::info!("bluez watch: org.bluez owner changed, reporting radio {status:?}");
                    let _ = events_tx.send(GattEvent::RadioChanged { status });
                }
            });
        }

        Ok(Self {
            _session: session,
            adapter,
            values: Arc::new(StdMutex::new(HashMap::new())),
            served_peers: Arc::new(StdMutex::new(HashMap::new())),
            next_session: Arc::new(AtomicU64::new(1)),
            dial_lock: Arc::new(AsyncMutex::new(())),
            advertise_lock: Arc::new(AsyncMutex::new(())),
            advertise_generation: Arc::new(StdMutex::new(1)),
            notify_writers: Arc::new(AsyncMutex::new(HashMap::new())),
            events_tx,
            app_handle: AsyncMutex::new(None),
            adv_handle: AsyncMutex::new(None),
            server_watch: Arc::new(StdMutex::new(Vec::new())),
            dialed: Arc::new(StdMutex::new(HashMap::new())),
            pending_cleanup: Arc::new(StdMutex::new(HashSet::new())),
            in_flight: Arc::new(StdMutex::new(HashSet::new())),
            cleanup_permits: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CLEANUP_DISCONNECTS)),
            discovery,
        })
    }

    /// Before a dial: if discovery is still on with no scan left using it
    /// (the caller just dropped its scan), stop it and wait for BlueZ to
    /// confirm, so `Connect()` does not race it. See
    /// `DISCOVERY_STOP_TIMEOUT`. A scan the caller keeps live is left
    /// alone — overlapping it with a dial is the caller's choice. Never
    /// fails the dial.
    async fn settle_discovery(&self) {
        if !self.discovery.state.lock().unwrap().idle_running() {
            return;
        }
        if tokio::time::timeout(DISCOVERY_STOP_TIMEOUT, self.discovery.stop_if_idle("before dialling"))
            .await
            .is_err()
        {
            log::warn!("connect: discovery did not stop within {DISCOVERY_STOP_TIMEOUT:?}; dialling anyway");
        }
    }
}

/// Track notify sessions for one characteristic.
///
/// Each `Notify` event is a central acquiring a notification channel, and —
/// unlike `StartNotify` — it carries the subscriber's address. That makes
/// this both the peripheral-role arrival signal and the notify path itself:
/// service-attributed (an unrelated device connected to the same adapter
/// never appears here), notification-ready by construction, and identified
/// without inference.
/// Report the loss of an outbound connection, but only while this watcher
/// still owns the dial it was armed for.
///
/// A peer that drops and reconnects quickly leaves the old watcher holding a
/// queued `Connected(false)`. Emitting that unconditionally would announce a
/// disconnect for the *replacement* link — whose datagram watcher would then
/// tear down a channel that is working — and would clear a `dialed` entry
/// that now belongs to the new connection.
fn report_central_loss(
    events_tx: &broadcast::Sender<GattEvent>, dialed: &Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    peer: &PeerAddress, generation: u64,
) {
    let mut dialed = dialed.lock().unwrap();
    if dialed.get(peer) != Some(&generation) {
        return;
    }
    // Stop suppressing inbound reports for this address: a later connection
    // from it may genuinely be an inbound one.
    dialed.remove(peer);
    drop(dialed);
    log::info!("link lost: {} session={generation} (central role)", peer.0);
    let _ = events_tx.send(GattEvent::Disconnected {
        peer: peer.clone(),
        local_role: Role::Central,
        session: Some(generation),
    });
}

#[allow(clippy::too_many_arguments)]
async fn watch_notify_sessions(
    mut control: CharacteristicControl, uuid: CharacteristicUuid,
    writers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
    events_tx: broadcast::Sender<GattEvent>,
    served_peers: Arc<StdMutex<HashMap<PeerAddress, u64>>>, adapter: Adapter,
    next_session: Arc<AtomicU64>, watchers: Arc<StdMutex<Vec<tokio::task::AbortHandle>>>,
    advertise_generation: Arc<StdMutex<u64>>, this_generation: u64,
) {
    while let Some(event) = control.next().await {
        let CharacteristicControlEvent::Notify(writer) = event else {
            continue;
        };
        let address = writer.device_address();
        let peer = PeerAddress(address.to_string());

        // Aborting this task's handle does not interrupt a poll already in
        // progress, so a watcher can resume after a stop/restart and push a
        // dead writer into the replacement's map, claim a `served_peers`
        // slot and announce a peer the new server never accepted — which
        // then refuses the legitimate central.
        //
        // The async lock on `writers` is taken *first*, so that from the
        // generation check onward there is no await and every effect below
        // is indivisible with it.
        let mut writers_guard = writers.lock().await;
        let session = {
            let current = advertise_generation.lock().unwrap();
            if *current != this_generation {
                log::warn!(
                    "notify session: dropping a subscribe from {} for stale generation {this_generation} \
                     (current {current})",
                    peer.0
                );
                return;
            }

            let sessions = writers_guard.entry(uuid).or_default();
            sessions.retain(|w| !w.is_closed().unwrap_or(true));
            sessions.push(writer);

            let mut served = served_peers.lock().unwrap();
            if served.contains_key(&peer) {
                continue;
            }
            let session = next_session.fetch_add(1, Ordering::Relaxed);
            served.insert(peer.clone(), session);

            log::info!(
                "notify session: central {} subscribed to {} session={session}",
                peer.0,
                uuid.0
            );
            let _ = events_tx.send(GattEvent::Connected {
                peer: peer.clone(),
                local_role: Role::Peripheral,
                session: Some(session),
            });
            session
        };
        drop(writers_guard);

        let handle = spawn_peripheral_disconnect_watch(
            adapter.clone(),
            address,
            peer,
            events_tx.clone(),
            served_peers.clone(),
            writers.clone(),
            session,
        );
        // Registered so `stop_advertising` cancels it; previously these were
        // detached and outlived the server that created them.
        watchers.lock().unwrap().push(handle.abort_handle());
    }
}

/// Emit `Disconnected { local_role: Peripheral }` once `address` drops its
/// connection, then forget it so a later reconnect re-arms a fresh watcher.
///
/// Separate from the central-role watcher in `connect` because the two carry
/// different roles and different cleanup: `datagram::serve` filters strictly
/// on `local_role`, so emitting the central variant here would be silently
/// ignored.
#[allow(clippy::too_many_arguments)]
fn spawn_peripheral_disconnect_watch(
    adapter: Adapter, address: bluer::Address, peer: PeerAddress,
    events_tx: broadcast::Sender<GattEvent>,
    served_peers: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    writers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
    session: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // This session's own writer may still be mid-`AcquireNotify` — see
        // `NOTIFY_WRITER_ACQUIRE_TIMEOUT`'s doc comment on `notify_matching`.
        // Without this grace period, this watcher's first poll (500ms,
        // `NOTIFY_SESSION_POLL`) fires well before that 3s window has any
        // chance to complete: a session this young genuinely having no live
        // writer yet is indistinguishable from one that never will, so
        // `has_live_session` alone would race ahead of `notify_matching`'s
        // own wait, remove `served_peers` first, and turn the first reply's
        // "no live notify session" into "session has been superseded" —
        // still a failure, just reported by a different code path.
        let spawned_at = tokio::time::Instant::now();
        // Any exit path must clear the guard, or a peer that failed to be
        // watched once could never be watched again.
        let _ = async {
            let device = adapter.device(address).ok()?;
            let mut changes = device.events().await.ok()?;
            // Poll once after subscribing, as the central-role watcher does.
            // A central that dropped between acquiring its notify session and
            // this subscription taking effect has already emitted its
            // `Connected(false)`, and waiting for a change that has been and
            // gone means no peripheral-role `Disconnected` is ever produced —
            // `serve()` then holds the stale peer forever and refuses every
            // reconnect.
            if device.is_connected().await.unwrap_or(false) {
                loop {
                    // The session ends on *either* signal. A central that
                    // disables its CCCD while staying connected is just as
                    // gone from this server's point of view: it can no longer
                    // be reached by notify, so leaving it in `serve`'s
                    // single-central slot would fail every send while locking
                    // every other central out.
                    let disconnected = async {
                        while let Some(event) = changes.next().await {
                            if matches!(
                                event,
                                bluer::DeviceEvent::PropertyChanged(
                                    bluer::DeviceProperty::Connected(false)
                                )
                            ) {
                                return;
                            }
                        }
                    };
                    tokio::select! {
                        _ = disconnected => break,
                        _ = tokio::time::sleep(NOTIFY_SESSION_POLL) => {
                            let past_acquire_grace =
                                spawned_at.elapsed() >= NOTIFY_WRITER_ACQUIRE_TIMEOUT;
                            if past_acquire_grace && !has_live_session(&writers, &peer).await {
                                break;
                            }
                        }
                    }
                }
            }
            Some(())
        }
        .await;
        // Only if this watcher still owns the session. The same address can
        // have reconnected and been served afresh while this task was
        // finishing; emitting then would tear down the replacement channel
        // and erase its spawn guard, and the entry it removed would be the
        // new session's.
        //
        // The writers lock is taken first, matching `notify_matching` and
        // `watch_notify_sessions`: one order everywhere, so a notify in
        // flight cannot observe a session this removal is about to retire.
        let _writers = writers.lock().await;
        let mut served = served_peers.lock().unwrap();
        if served.get(&peer) != Some(&session) {
            return;
        }
        served.remove(&peer);
        drop(served);
        log::info!("link lost: {} session={session} (peripheral role)", peer.0);
        let _ = events_tx.send(GattEvent::Disconnected {
            peer: peer.clone(),
            local_role: Role::Peripheral,
            session: Some(session),
        });
    })
}

/// Whether `peer` still holds any open notify session.
///
/// Polled rather than awaited on `CharacteristicWriter::closed()` because the
/// writers live in a shared map that `notify` needs concurrent access to —
/// taking one out to await its closure would block sends to everyone else.
async fn has_live_session(
    writers: &Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
    peer: &PeerAddress,
) -> bool {
    let mut writers = writers.lock().await;
    let mut live = false;
    for sessions in writers.values_mut() {
        sessions.retain(|w| !w.is_closed().unwrap_or(true));
        if sessions.iter().any(|w| w.device_address().to_string() == peer.0) {
            live = true;
        }
    }
    live
}

impl LinuxBackend {
    /// Deliver to every live notify session matching `want`.
    ///
    /// Sessions are pruned before *and* after writing, because one can close
    /// mid-write. Reaching nobody is an error: a reliable `send` must never
    /// be told a dropped payload was delivered.
    async fn notify_matching(
        &self, characteristic: CharacteristicUuid, value: Vec<u8>,
        owner: Option<(&PeerAddress, u64)>, want: impl Fn(&CharacteristicWriter) -> bool,
        nobody: &str,
    ) -> Result<()> {
        // Only an addressed call (`owner: Some`, i.e. `notify_peer`) waits.
        // `notify()`'s broadcast has no specific peer to wait *for* — with
        // `owner: None` this deadline is simply never consulted below.
        let deadline = owner.map(|_| tokio::time::Instant::now() + NOTIFY_WRITER_ACQUIRE_TIMEOUT);
        loop {
            // The writers lock is taken *first*, and the session is validated
            // while holding it. Checking before acquiring it left a gap in which
            // the named session could drop and the same address acquire a
            // replacement writer — after which the address predicate selects the
            // replacement and the stale channel's fragment is reassembled there
            // as current data.
            //
            // Every mutation of `served_peers` takes this lock first as well, so
            // the check and the selection below cannot be separated.
            let mut writers = self.notify_writers.lock().await;
            if let Some((peer, session)) = owner {
                if self.served_peers.lock().unwrap().get(peer) != Some(&session) {
                    log::warn!(
                        "notify: refusing — session {session} for {} has been superseded",
                        peer.0
                    );
                    return Err(BleError::NotConnected(peer.0.clone()));
                }
            }
            // Bridges a real race, not a defensive check: a central
            // subscribes before writing, and BLE delivers one link's ATT
            // operations in order, but `AcquireNotify` and a characteristic
            // write are two independently driven D-Bus round trips on the
            // bluer side with no ordering guarantee between them — see
            // `NOTIFY_WRITER_ACQUIRE_TIMEOUT`'s doc comment. Without this, a
            // peer's very first reply after a fresh, fast reconnect can lose
            // that race and fail immediately with "no live notify session"
            // even though the peer's subscription is genuinely in flight.
            //
            // Must check `is_closed()` here too, not just `want` — a
            // reconnect's *previous* session can leave a closed writer for
            // this same address still sitting in the vector (nothing prunes
            // it until the next `AcquireNotify` event runs its own
            // retain-before-push, which for this exact race hasn't happened
            // yet). Matching on address alone made this see a stale, dead
            // writer as "already live," skip waiting entirely, and fail
            // immediately once the real prune ran a few lines down —
            // silently turning the bound-wait into a no-op for precisely
            // the reconnect case it exists to cover.
            let has_match = writers
                .get(&characteristic)
                .is_some_and(|sessions| sessions.iter().any(|w| !w.is_closed().unwrap_or(true) && want(w)));
            if !has_match {
                if let Some(deadline) = deadline {
                    if tokio::time::Instant::now() < deadline {
                        drop(writers);
                        tokio::time::sleep(NOTIFY_WRITER_ACQUIRE_POLL).await;
                        continue;
                    }
                }
            }
            let Some(sessions) = writers.get_mut(&characteristic) else {
                log::warn!("notify: no active notify session on {}", characteristic.0);
                return Err(BleError::Gatt("no active notify session for characteristic".to_string()));
            };
            sessions.retain(|w| !w.is_closed().unwrap_or(true));

            let mut delivered = false;
            let mut last_error = None;
            for writer in sessions.iter_mut().filter(|w| want(w)) {
                // write_all, not write: a short write would truncate a fragment,
                // and reassembly would then fail on the far side with nothing
                // reported here.
                match writer.write_all(&value).await {
                    Ok(()) => delivered = true,
                    Err(err) => last_error = Some(err),
                }
            }
            sessions.retain(|w| !w.is_closed().unwrap_or(true));

            if !delivered {
                log::warn!("notify: {} bytes on {} reached {nobody}", value.len(), characteristic.0);
                return Err(BleError::Gatt(match last_error {
                    Some(err) => format!("notify reached {nobody}: {err}"),
                    None => format!("notify reached {nobody}"),
                }));
            }
            log::trace!("notify: {} bytes delivered on {}", value.len(), characteristic.0);
            return Ok(());
        }
    }
}

/// Cancellation guard for `LinuxBackend::connect`'s `device.connect().await`
/// -- see the doc comment at its construction site for why this exists.
/// `bluer::Device::disconnect()` is async and `Drop::drop` cannot await, so
/// the actual BlueZ teardown is a detached `tokio::spawn`; only the local
/// `dialed` bookkeeping is cleared synchronously here.
struct LinuxConnectGuard {
    adapter: Adapter,
    address: bluer::Address,
    dialed: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    dial_lock: Arc<AsyncMutex<()>>,
    pending_cleanup: Arc<StdMutex<HashSet<PeerAddress>>>,
    /// Reserved by `connect()` before `device.connect()` was ever issued —
    /// see `MAX_CONCURRENT_CLEANUP_DISCONNECTS`'s doc comment. `Some` for
    /// the guard's entire life; taken and moved into the cleanup task if
    /// `Drop::drop` finds `armed` still true, otherwise dropped (returning
    /// the permit to the pool immediately) along with the rest of the
    /// guard's fields once a successful connect makes it unarmed.
    cleanup_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    peer: PeerAddress,
    generation: u64,
    armed: bool,
}

/// Releases `in_flight`'s entry for `peer` on every exit from `connect()`,
/// from the moment it is inserted onward — including a cancellation while
/// still queued for `dial_lock`, before `LinuxConnectGuard` even exists.
/// Codex P1: without this, a caller giving up while queued (a timeout, a
/// `select!` losing a race, all while another peer's operation holds
/// `dial_lock`) left the entry stuck forever, since nothing was yet
/// constructed to remove it — permanently refusing every future dial to
/// that same peer as "already in flight" for a dial that no longer exists.
struct InFlightGuard {
    in_flight: Arc<StdMutex<HashSet<PeerAddress>>>,
    peer: PeerAddress,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.lock().unwrap().remove(&self.peer);
    }
}

impl Drop for LinuxConnectGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Logged synchronously, unconditionally, before anything else in
        // this function — real-hardware evidence showed a caller dropping
        // the whole `connect()` future (e.g. a wrapping probe timeout at a
        // higher layer) leaves no trace at all otherwise: the CONNECT_TIMEOUT
        // branch below logs its own warning before returning, but a future
        // dropped mid-await never reaches that code, so this is the only
        // line that will ever record such an abandonment. A CONNECT_TIMEOUT
        // firing does still reach here too (after already logging its own
        // line above), which is an acceptable, informative duplicate: it
        // confirms cleanup actually started rather than leaving that as an
        // inference from the disconnect-completed line alone.
        log::warn!(
            "connect: {} abandoned before completing (connect guard dropped); quarantining and cleaning up in the background",
            self.peer.0
        );
        {
            let mut dialed = self.dialed.lock().unwrap();
            if dialed.get(&self.peer) == Some(&self.generation) {
                dialed.remove(&self.peer);
            }
        }
        // Quarantined *synchronously*, before this function — and so
        // `connect()`'s own stack frame — ever returns to its caller.
        // Codex P1: publishing this only later (e.g. only once a first
        // bounded cleanup attempt itself times out) left a real window
        // where an immediate retry raced in before any quarantine existed
        // at all — `connect()`'s own quarantine checks have nothing to see
        // until *something* inserts into this set, and previously nothing
        // did until well after this guard had already returned control to
        // the caller. BlueZ's original Connect() request may still be
        // genuinely in flight the instant this guard drops; nothing has
        // confirmed otherwise yet, so the quarantine cannot wait for that
        // confirmation either.
        self.pending_cleanup.lock().unwrap().insert(self.peer.clone());
        let adapter = self.adapter.clone();
        let address = self.address;
        let dial_lock = self.dial_lock.clone();
        let dialed = self.dialed.clone();
        let pending_cleanup = self.pending_cleanup.clone();
        let peer = self.peer.clone();
        // Reserved by `connect()` before it ever issued `device.connect()`
        // — see `MAX_CONCURRENT_CLEANUP_DISCONNECTS`'s doc comment and the
        // reservation site there. Moved into the cleanup task rather than
        // acquired here: by the time this Drop runs, capacity for this
        // specific attempt is already guaranteed, not merely hoped for.
        // Held for the task's entire lifetime; dropped (returning the
        // permit to the pool) only once the task itself ends, right
        // alongside clearing `pending_cleanup` below.
        let cleanup_permit = self.cleanup_permit.take();
        tokio::spawn(async move {
            let _permit = cleanup_permit;
            // At most one disconnect attempt is ever outstanding at a time.
            // Each is spawned as its own task rather than raced directly
            // against CLEANUP_DISCONNECT_TIMEOUT: dropping a future when a
            // timeout elapses does not cancel the underlying D-Bus call
            // already sent to BlueZ (the same reasoning `CONNECT_TIMEOUT`'s
            // own guard rests on), so a timeout here just means *this
            // loop's wait* gave up, not that the attempt itself is gone —
            // `handle` stays `Some` and the next iteration keeps polling the
            // very same task instead of spawning a new one alongside it.
            // Codex caught this the first time as an unbounded-accumulation
            // risk ("avoid accumulating unresolved disconnect calls"): an
            // earlier version of this loop spawned a fresh attempt on every
            // iteration regardless, so a peer whose BlueZ simply never
            // replies could pile up an unbounded number of outstanding
            // D-Bus calls and task handles — the exact request pile-up this
            // mechanism exists to prevent, just recursed into its own retry
            // loop. Never starting attempt N+1 before attempt N is known to
            // have actually finished (Ok, Err, or panic — not merely timed
            // out waiting) also fully subsumes the earlier, separate fix for
            // "keep quarantine until every timed-out disconnect resolves":
            // with only ever one attempt alive, there is nothing left
            // outstanding to join once the loop breaks.
            let mut handle: Option<tokio::task::JoinHandle<bluer::Result<()>>> = None;
            loop {
                if handle.is_none() {
                    // Every other operation in this file that touches
                    // `Device` holds `dial_lock` across the platform call,
                    // precisely because `Device` resolves by address: an
                    // unguarded disconnect here could land on a connection
                    // a retry has since legitimately established, not the
                    // abandoned attempt this guard owns. See
                    // `LinuxGattConnection::disconnect`'s doc comment for
                    // the same reasoning at the sharpest edge of it.
                    //
                    // Codex P1 ("poll timed-out cleanup without reacquiring
                    // the global lock"): held only around *starting* a
                    // fresh attempt, not around waiting on one already
                    // running — that D-Bus call proceeds independently of
                    // this lock once issued, and the address is already
                    // quarantined regardless, so holding a backend-wide
                    // lock for another `CLEANUP_DISCONNECT_TIMEOUT` just to
                    // poll an existing task needlessly blocked every other
                    // peer's dial/read/write for that whole span, repeated
                    // on every timeout, and multiple stuck peers could
                    // compound the delay past any caller's own timeout.
                    let _dial = dial_lock.lock().await;
                    // A newer dial claiming this address while quarantined
                    // should be structurally impossible now — connect()'s
                    // own quarantine checks (before and after acquiring
                    // this same lock) refuse a dial to a quarantined peer
                    // outright. Kept as a defensive check, not a load-
                    // bearing one: the synchronous removal above only ever
                    // clears *this* generation's own entry, so any presence
                    // here would necessarily be someone else's.
                    if dialed.lock().unwrap().contains_key(&peer) {
                        break;
                    }
                    let Ok(device) = adapter.device(address) else {
                        break;
                    };
                    handle = Some(tokio::spawn(async move { device.disconnect().await }));
                    // Dropped here, before the wait below — see the comment
                    // above for why holding it that long is unnecessary and
                    // was itself a Codex P1.
                }
                // Bounded per wait for the same reason CONNECT_TIMEOUT
                // exists — an unbounded wait is exactly as capable of
                // starving this task's own progress as the stuck connect it
                // exists to clean up after (though, unlike before, it no
                // longer holds `dial_lock` while doing so). Only the wait is
                // bounded, not the attempt: on a timeout the spawned task
                // keeps running in the background and is polled again next
                // iteration, never abandoned for a fresh one.
                let outcome = tokio::time::timeout(CLEANUP_DISCONNECT_TIMEOUT, handle.as_mut().unwrap()).await;
                match outcome {
                    Ok(Ok(Ok(()))) => {
                        log::info!("connect: {} cleanup disconnect completed", peer.0);
                        break;
                    }
                    // Codex P1: an unconditional retry-forever here left the
                    // address quarantined permanently once BlueZ ever
                    // answered NotConnected — which it does precisely when
                    // the abandoned Connect had already failed on its own,
                    // or an earlier Disconnect completed but its D-Bus
                    // reply was lost. Either way, the state this cleanup
                    // exists to reach — not connected — is already true, so
                    // treat it exactly like a successful disconnect rather
                    // than an error to retry past.
                    Ok(Ok(Err(err))) if is_not_connected(&err) => {
                        log::info!(
                            "connect: {} cleanup disconnect reports not connected; treating as already clean",
                            peer.0
                        );
                        break;
                    }
                    Ok(Ok(Err(err))) => {
                        log::warn!(
                            "connect: {} cleanup disconnect attempt failed ({err}), retrying in \
                             {PENDING_CLEANUP_RETRY_DELAY:?} — still quarantined",
                            peer.0
                        );
                        handle = None; // resolved (failed); a fresh attempt may start next iteration
                        tokio::time::sleep(PENDING_CLEANUP_RETRY_DELAY).await;
                    }
                    Ok(Err(join_err)) => {
                        log::warn!(
                            "connect: {} cleanup disconnect task ended unexpectedly ({join_err}), retrying \
                             in {PENDING_CLEANUP_RETRY_DELAY:?} — still quarantined",
                            peer.0
                        );
                        handle = None;
                        tokio::time::sleep(PENDING_CLEANUP_RETRY_DELAY).await;
                    }
                    Err(_elapsed) => {
                        log::warn!(
                            "connect: {} cleanup disconnect attempt timed out after \
                             {CLEANUP_DISCONNECT_TIMEOUT:?}, still running in the background; waiting on it \
                             again rather than starting another — still quarantined",
                            peer.0
                        );
                        // `handle` stays Some: keep polling the same
                        // attempt next iteration instead of accumulating a
                        // second one. No extra sleep: the timeout itself
                        // already spent CLEANUP_DISCONNECT_TIMEOUT waiting.
                    }
                }
            }
            pending_cleanup.lock().unwrap().remove(&peer);
        });
    }
}

/// What a scan learned about one device at one moment.
enum ScannedPeer {
    /// The services BlueZ knows so far do not include the target. More may
    /// still arrive (a scan response can carry the target UUID after the
    /// first advertisement listed others).
    NotTarget,
    /// Matches, with manufacturer data.
    Complete(DiscoveredPeer),
    /// Matches, but no manufacturer data has arrived yet.
    NoManufacturerData(DiscoveredPeer),
    /// BlueZ has not reported any services for it yet.
    Unknown,
}

/// How long a device that is not yet complete is watched for the rest of its
/// properties.
const SCAN_COMPLETION_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);

/// At most this many devices are watched at once, so a room full of beacons
/// cannot turn one scan into a flood of D-Bus match rules.
const SCAN_MAX_WATCHERS: usize = 16;

fn scan_watchers() -> &'static std::sync::atomic::AtomicUsize {
    static WATCHERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    &WATCHERS
}

async fn read_scanned_peer(adapter: &Adapter, address: bluer::Address, target: uuid::Uuid) -> ScannedPeer {
    let Ok(device) = adapter.device(address) else {
        return ScannedPeer::Unknown;
    };
    let uuids = device.uuids().await.ok().flatten().unwrap_or_default();
    if uuids.is_empty() {
        return ScannedPeer::Unknown;
    }
    if !uuids.contains(&target) {
        log::trace!(
            "scan: ignoring {address} — advertises {} service(s), none matching",
            uuids.len()
        );
        return ScannedPeer::NotTarget;
    }
    let name = device.name().await.ok().flatten();
    let manufacturer_data: std::collections::BTreeMap<u16, Vec<u8>> = device
        .manufacturer_data()
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let service_data = device
        .service_data()
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(|(uuid, data)| (ServiceUuid(uuid), data))
        .collect();
    let rssi = device.rssi().await.ok().flatten();
    log::info!("scan: discovered {address} name={name:?} rssi={rssi:?}");
    let complete = !manufacturer_data.is_empty();
    let peer = DiscoveredPeer {
        address: PeerAddress(address.to_string()),
        name,
        services: uuids.into_iter().map(ServiceUuid).collect(),
        manufacturer_data,
        service_data,
        rssi,
    };
    if complete {
        ScannedPeer::Complete(peer)
    } else {
        ScannedPeer::NoManufacturerData(peer)
    }
}

/// Watches one device's property changes for up to `SCAN_COMPLETION_WINDOW`
/// and reports it as soon as it is complete -- the services known and the
/// target among them, with manufacturer data.
///
/// A match without manufacturer data is reported too, once, the moment its
/// services show the target (`match_sent` says whether the caller already
/// did): an advertiser that never sends manufacturer data -- the default for
/// `GattServiceSpec::new` -- would otherwise never be reported at all.
///
/// With every slot taken the device is not watched, and it is left out of
/// `reported` so its next `DeviceAdded` in this scan tries again.
async fn watch_for_completion(
    adapter: Adapter,
    address: bluer::Address,
    target: uuid::Uuid,
    reported: Arc<StdMutex<HashMap<bluer::Address, bool>>>,
    tx: tokio::sync::mpsc::Sender<Result<DiscoveredPeer>>,
    mut match_sent: bool,
) {
    let Some(_slot) = WatcherSlot::take() else {
        reported.lock().unwrap().remove(&address);
        return;
    };
    let Ok(device) = adapter.device(address) else {
        return;
    };
    let Ok(mut changes) = device.events().await else {
        return;
    };
    let deadline = tokio::time::Instant::now() + SCAN_COMPLETION_WINDOW;
    loop {
        // Read first: the data may have arrived between the event that put
        // this device here and the subscription above.
        match read_scanned_peer(&adapter, address, target).await {
            ScannedPeer::Complete(peer) => {
                reported.lock().unwrap().insert(address, true);
                let _ = tx.send(Ok(peer)).await;
                return;
            }
            ScannedPeer::NoManufacturerData(peer) if !match_sent => {
                match_sent = true;
                if tx.send(Ok(peer)).await.is_err() {
                    return;
                }
            }
            _ => {}
        }
        tokio::select! {
            change = changes.next() => {
                if change.is_none() {
                    return;
                }
            }
            () = tokio::time::sleep_until(deadline) => return,
            () = tx.closed() => return,
        }
    }
}

/// One of the `SCAN_MAX_WATCHERS` slots, released when dropped.
struct WatcherSlot;

impl WatcherSlot {
    fn take() -> Option<Self> {
        use std::sync::atomic::Ordering;
        if scan_watchers().fetch_add(1, Ordering::SeqCst) >= SCAN_MAX_WATCHERS {
            scan_watchers().fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Self)
    }
}

impl Drop for WatcherSlot {
    fn drop(&mut self) {
        scan_watchers().fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl Backend for LinuxBackend {
    async fn capabilities(&self) -> CapabilityReport {
        let peripheral = self
            .adapter
            .supported_advertising_capabilities()
            .await
            .ok()
            .flatten()
            .is_some();
        log::info!("capabilities: central=true peripheral={peripheral}");
        CapabilityReport {
            central: true,
            peripheral,
        }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let adapter = self.adapter.clone();
        log::info!("scan: starting discovery for service {}", service.0);
        // The guard is created before the calls below, so a failure in
        // them still stops the discovery just started.
        let scan_guard = self.discovery.begin_scan().await?;
        // The same stream `bluer::Adapter::discover_devices` built: already
        // known devices first, then adapter events until discovery stops.
        let changes = adapter.events().await.map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let known = adapter
            .device_addresses()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let events = futures::stream::iter(known).map(AdapterEvent::DeviceAdded).chain(changes.take_while(
            |event| {
                std::future::ready(!matches!(
                    event,
                    AdapterEvent::PropertyChanged(bluer::AdapterProperty::Discovering(false))
                ))
            },
        ));
        let target = service.0;

        // Events are handled in a task that owns the discovery guard and ends
        // when the caller drops the returned stream (the channel closes),
        // which is what stops discovery.
        //
        // BlueZ reports a device the moment the first advertisement is heard,
        // often before its service UUIDs or manufacturer data are filled in.
        // Evaluating only that moment dropped a nearby peer until the *next*
        // discovery session began and BlueZ re-reported it from its cache --
        // tens of seconds for two devices side by side. So a device that is
        // not yet complete is watched for a few seconds and reported the
        // moment it is.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<DiscoveredPeer>>(32);
        tokio::spawn(async move {
            let _scan_guard = scan_guard;
            let reported: Arc<StdMutex<HashMap<bluer::Address, bool>>> = Arc::new(StdMutex::new(HashMap::new()));
            let mut events = Box::pin(events);
            loop {
                let event = tokio::select! {
                    event = events.next() => event,
                    () = tx.closed() => break,
                };
                let Some(AdapterEvent::DeviceAdded(address)) = event else {
                    if event.is_none() {
                        break;
                    }
                    continue;
                };
                if reported.lock().unwrap().contains_key(&address) {
                    continue;
                }
                match read_scanned_peer(&adapter, address, target).await {
                    ScannedPeer::Complete(peer) => {
                        reported.lock().unwrap().insert(address, true);
                        if tx.send(Ok(peer)).await.is_err() {
                            break;
                        }
                    }
                    // Matches, but no manufacturer data yet: reported now, as
                    // before, and again when the data arrives.
                    ScannedPeer::NoManufacturerData(peer) => {
                        reported.lock().unwrap().insert(address, false);
                        if tx.send(Ok(peer)).await.is_err() {
                            break;
                        }
                        tokio::spawn(watch_for_completion(
                            adapter.clone(),
                            address,
                            target,
                            reported.clone(),
                            tx.clone(),
                            true,
                        ));
                    }
                    // Nothing known about its services yet, or not the target
                    // among those known so far: wait for the rest.
                    ScannedPeer::Unknown | ScannedPeer::NotTarget => {
                        reported.lock().unwrap().insert(address, false);
                        tokio::spawn(watch_for_completion(
                            adapter.clone(),
                            address,
                            target,
                            reported.clone(),
                            tx.clone(),
                            false,
                        ));
                    }
                }
            }
        });
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let address: bluer::Address = peer.0.parse().map_err(|_| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: "invalid Bluetooth address".to_string(),
        })?;
        let device = self.adapter.device(address).map_err(|err| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: err.to_string(),
        })?;
        // A previous cancelled attempt's cleanup Disconnect to this exact
        // address may still be genuinely in flight on BlueZ's side — see
        // `LinuxConnectGuard::drop`'s doc comment. Dialling in now would
        // risk that stale Disconnect landing on the replacement link this
        // call is about to establish. Checked before `dial_lock` so a
        // quarantined address fails fast rather than queuing behind it.
        if self.pending_cleanup.lock().unwrap().contains(peer) {
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "a previous cancelled connect attempt is still being cleaned up".to_string(),
            });
        }
        // Refuse outright rather than queue behind `dial_lock`: a caller
        // that starts a new dial to this peer before an earlier one has
        // resolved produces a genuinely concurrent second `Connect()` to
        // the same `Device` — BlueZ rejects the loser immediately, so
        // letting it through just burns a real D-Bus round trip to fail the
        // same way this check already knows it will. Checked before
        // `dial_lock` for the same reason the quarantine check above is.
        if !self.in_flight.lock().unwrap().insert(peer.clone()) {
            log::warn!("connect: {} refused — a dial to this peer is already in flight", peer.0);
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "a connect attempt to this peer is already in flight".to_string(),
            });
        }
        // Constructed immediately — no `.await` between the insert above
        // and here — so a cancellation from this point on, including one
        // that arrives while merely queued for `dial_lock` below, always
        // has something alive to release the entry it just claimed.
        let _in_flight_guard = InFlightGuard { in_flight: self.in_flight.clone(), peer: peer.clone() };
        // Before `dial_lock`: this can wait up to `DISCOVERY_STOP_TIMEOUT`,
        // which must not hold up GATT operations on other links.
        self.settle_discovery().await;
        // Recorded *before* dialling: BlueZ can publish the Connected
        // property before `connect()` returns, and the inbound watcher would
        // otherwise race us and announce our own outbound link as a central
        // arriving at our server.
        //
        // Held across the platform call, so a concurrent `disconnect` on an
        // older handle cannot land midway and drop this link.
        let _dial = self.dial_lock.lock().await;
        // Re-checked now that `dial_lock` is actually held: the check above
        // only sees state as of *before* this call queued for the lock.
        // Real-hardware evidence (Codex review): a cancelled attempt's
        // cleanup can still be inside its own bounded `Disconnect` — itself
        // holding `dial_lock` — when this call's pre-check ran and found
        // `pending_cleanup` empty. If that cleanup times out and publishes
        // the quarantine *while this call is already queued* for the lock
        // it just released, the earlier check never sees it — this call
        // would dial straight into the address the fresh, now-unbounded
        // cleanup disconnect is still targeting. `in_flight` needs no
        // matching re-check: unlike `pending_cleanup`, it can only become
        // *un*-set while queued (this call's own entry persists for as
        // long as it holds it), never newly set against this same peer out
        // from under it.
        if self.pending_cleanup.lock().unwrap().contains(peer) {
            // `_in_flight_guard` releases `in_flight` on this return; no
            // manual cleanup needed here.
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "a previous cancelled connect attempt is still being cleaned up".to_string(),
            });
        }
        // The generation distinguishes successive dials to the same address,
        // so a watcher from a previous connection cannot report a disconnect
        // for the one that replaced it.
        let dial_generation = self.next_session.fetch_add(1, Ordering::Relaxed);
        self.dialed.lock().unwrap().insert(peer.clone(), dial_generation);
        // Reserved *before* `device.connect()` below is ever issued, not
        // lazily once cleanup begins — see `MAX_CONCURRENT_CLEANUP_DISCONNECTS`'s
        // doc comment (Codex P1, "reserve cleanup capacity before issuing
        // Connect"): if this attempt is later abandoned or times out, its
        // cleanup will need this exact capacity, and reserving it only
        // after the fact bounds nothing — a connect this backend cannot
        // guarantee it can ever clean up must not be issued at all.
        let cleanup_permit = match self.cleanup_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let mut dialed = self.dialed.lock().unwrap();
                if dialed.get(peer) == Some(&dial_generation) {
                    dialed.remove(peer);
                }
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: format!(
                        "no cleanup capacity available: {MAX_CONCURRENT_CLEANUP_DISCONNECTS} peers already \
                         awaiting cleanup"
                    ),
                });
            }
        };
        log::info!("connect: dialling {} generation={dial_generation}", peer.0);
        // Cancellation guard, mirroring the Android backend's `ConnectGuard`
        // -- `connect()` is an async fn, so a caller can drop this future
        // mid-await (a timeout, a `select!` losing a race) while suspended
        // inside `device.connect().await` below. Without this, neither of
        // that call's own two return paths ever runs: the pending `dialed`
        // entry stays behind (every retry then refused as "already
        // dialling"), and more damagingly, BlueZ's own connect request keeps
        // running independently of the cancelled Rust future and can
        // complete anyway -- leaving a real ACL/GATT link with nothing on
        // the Rust side ever owning it, so even an explicit retry can fail
        // with "already connected" until that orphaned link drops
        // externally. Disarmed right after `device.connect()` actually
        // returns, on both its Ok and Err paths (each already does its own
        // correct cleanup/handoff).
        let mut guard = LinuxConnectGuard {
            adapter: self.adapter.clone(),
            address,
            dialed: self.dialed.clone(),
            dial_lock: self.dial_lock.clone(),
            pending_cleanup: self.pending_cleanup.clone(),
            cleanup_permit: Some(cleanup_permit),
            peer: peer.clone(),
            generation: dial_generation,
            armed: true,
        };
        // Bounded, not just cancellation-safe: without this, an unresponsive
        // peer's Connect reply never arriving holds `dial_lock` (and, real-
        // hardware evidence suggests, whatever capacity the shared D-Bus
        // connection has for outstanding calls) forever, so every later dial
        // queues up behind one that will never finish. See
        // `CONNECT_TIMEOUT`'s doc comment.
        let connect_result = match tokio::time::timeout(CONNECT_TIMEOUT, device.connect()).await {
            Ok(result) => {
                // The D-Bus call genuinely completed (Ok or Err) — nothing
                // left for the guard to cancel.
                guard.armed = false;
                result
            }
            Err(_elapsed) => {
                // Guard stays armed: unlike a completed call, BlueZ's own
                // Connect may still resolve later on its own schedule — the
                // exact orphaned-link scenario this guard exists to clean up
                // after, just triggered by our own timeout instead of a
                // caller dropping the future.
                log::warn!(
                    "connect: {} timed out after {CONNECT_TIMEOUT:?} waiting for BlueZ's Connect reply",
                    peer.0
                );
                let mut dialed = self.dialed.lock().unwrap();
                if dialed.get(peer) == Some(&dial_generation) {
                    dialed.remove(peer);
                }
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: format!("timed out after {CONNECT_TIMEOUT:?} waiting for BlueZ to respond to Connect"),
                });
            }
        };
        if let Err(err) = connect_result {
            log::warn!("connect: {} refused the link: {err}", peer.0);
            let mut dialed = self.dialed.lock().unwrap();
            if dialed.get(peer) == Some(&dial_generation) {
                dialed.remove(peer);
            }
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: err.to_string(),
            });
        }
        log::info!("connect: link established to {} session={dial_generation}", peer.0);
        let _ = self.events_tx.send(GattEvent::Connected {
            peer: peer.clone(),
            local_role: Role::Central,
            session: Some(dial_generation),
        });

        // Watch BlueZ's own Connected property so an *unsolicited* drop (peer
        // out of range, powered off) reaches `events()`. Without this a
        // caller mid-transfer has no way to distinguish "slow" from "gone" —
        // see `Backend::events`. The task ends when the device disconnects or
        // the property stream closes, so it does not leak per connection.
        let watch_device = device.clone();
        let watch_peer = peer.clone();
        let events_tx = self.events_tx.clone();
        let dialed = self.dialed.clone();
        let generation = dial_generation;
        tokio::spawn(async move {
            let Ok(mut changes) = watch_device.events().await else {
                return;
            };
            // Poll once after subscribing. A peer that dropped between
            // `connect()` returning and this subscription taking effect has
            // already emitted its `Connected(false)`, and waiting for a
            // change that has been and gone means no central-role
            // `Disconnected` is ever emitted — leaving `dialed` stale and a
            // datagram receiver blocked forever.
            if !watch_device.is_connected().await.unwrap_or(false) {
                report_central_loss(&events_tx, &dialed, &watch_peer, generation);
                return;
            }
            while let Some(event) = changes.next().await {
                let bluer::DeviceEvent::PropertyChanged(bluer::DeviceProperty::Connected(false)) =
                    event
                else {
                    continue;
                };
                report_central_loss(&events_tx, &dialed, &watch_peer, generation);
                return;
            }
        });

        Ok(Box::new(LinuxGattConnection {
            session: dial_generation,
            dialed: self.dialed.clone(),
            dial_lock: self.dial_lock.clone(),
            served_peers: self.served_peers.clone(),
            events_tx: self.events_tx.clone(),
            peer: peer.clone(),
            device,
            att_mtu: AtomicU16::new(crate::hal::DEFAULT_ATT_MTU),
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        log::info!(
            "advertise: registering service {} with {} characteristic(s)",
            service.uuid.0,
            service.characteristics.len()
        );
        // Held for the whole setup, including both BlueZ registrations, so a
        // concurrent stop cannot interleave with installing the handles.
        let _serialise = self.advertise_lock.lock().await;
        // Abort the previous generation's watchers *before* spawning any of
        // this one's, so no task registered below can be mistaken for a
        // leftover and cancelled at the end of this call.
        for previous in self.server_watch.lock().unwrap().drain(..) {
            previous.abort();
        }
        let values = self.values.clone();
        let next_session = self.next_session.clone();
        // Reserved (not yet published) before anything is registered, so
        // every handler built below is stamped with the generation it
        // belongs to. Deliberately *not* written into `advertise_generation`
        // here: doing so immediately would mark the *outgoing* generation's
        // still-live handlers stale before its BlueZ objects are actually
        // replaced below, silently dropping any write or subscribe that
        // lands on them in that window — hardware-confirmed to happen on a
        // re-advertise. Published only once the new application and
        // advertisement are installed, by which point the outgoing
        // generation's BlueZ objects are already being replaced anyway.
        let advertise_generation = self.advertise_generation.clone();
        let this_generation = *advertise_generation.lock().unwrap() + 1;
        let server_watch = self.server_watch.clone();
        let notify_writers = self.notify_writers.clone();
        let mut notify_sessions = Vec::new();
        let served_peers = self.served_peers.clone();
        let adapter = self.adapter.clone();
        let events_tx = self.events_tx.clone();

        let mut local_characteristics = Vec::with_capacity(service.characteristics.len());
        for spec in &service.characteristics {
            let uuid = spec.uuid;
            values.lock().unwrap().insert(uuid, spec.initial_value.clone());

            let read = spec.readable.then(|| {
                let values = values.clone();
                CharacteristicRead {
                    read: true,
                    fun: Box::new(move |_req| {
                        let values = values.clone();
                        Box::pin(async move {
                            let value = values.lock().unwrap().get(&uuid).cloned().unwrap_or_default();
                            ReqResult::Ok(value)
                        })
                    }),
                    ..Default::default()
                }
            });

            let write = spec.writable.then(|| {
                let values = values.clone();
                let events_tx = events_tx.clone();
                let served_peers = served_peers.clone();
                let adapter = adapter.clone();
                let notify_writers = notify_writers.clone();
                let next_session = next_session.clone();
                let server_watch = server_watch.clone();
                let advertise_generation = advertise_generation.clone();
                CharacteristicWrite {
                    write: true,
                    write_without_response: true,
                    method: CharacteristicWriteMethod::Fun(Box::new(move |value, req| {
                        let values = values.clone();
                        let events_tx = events_tx.clone();
                        let served_peers = served_peers.clone();
                        let adapter = adapter.clone();
                        let notify_writers = notify_writers.clone();
                        let next_session = next_session.clone();
                        let server_watch = server_watch.clone();
                        let advertise_generation = advertise_generation.clone();
                        let address = req.device_address;
                        let peer = PeerAddress(address.to_string());
                        Box::pin(async move {
                            // A handler descheduled around the awaits below
                            // can resume after advertising has been stopped
                            // and restarted. Publishing then would inject
                            // this payload into the *replacement* `serve`
                            // session, where a datagram fragment is accepted
                            // as current data. Dropping the application
                            // handle does not prevent it — an already-running
                            // handler is not cancelled — so it checks here.
                            // Every effect below is generation-scoped, not
                            // just the final publish. A handler resuming
                            // after a stop/restart would otherwise overwrite
                            // the replacement server's characteristic value,
                            // claim its single-central slot with a phantom
                            // peer, announce that peer and spawn a watcher
                            // for it — after which the new `serve` refuses
                            // the real central. Guarding only the last send
                            // left all of that reachable.
                            //
                            // `server_watch` is a sync mutex precisely so
                            // there is no await anywhere in here: from the
                            // check onward this is indivisible.
                            let current = advertise_generation.lock().unwrap();
                            if *current != this_generation {
                                log::warn!(
                                    "write: dropping a write from {} for stale generation \
                                     {this_generation} (current {current})",
                                    peer.0
                                );
                                return ReqResult::Ok(());
                            }

                            values.lock().unwrap().insert(uuid, value.clone());

                            // BlueZ gives a GATT *server* no connection
                            // callback at all — the write itself is the only
                            // signal that a central is present. So the first
                            // write from a peer arms the same Connected-
                            // property watcher the central role uses.
                            //
                            // The session is allocated before `Connected` is
                            // published: publishing first meant the very
                            // first write for a peer carried `session: None`,
                            // so `serve` recorded the channel with no
                            // identity and could not reject a queued
                            // `Disconnected` from the previous connection.
                            let (session, newly_served) = {
                                let mut served = served_peers.lock().unwrap();
                                match served.get(&peer) {
                                    Some(existing) => (*existing, false),
                                    None => {
                                        let session =
                                            next_session.fetch_add(1, Ordering::Relaxed);
                                        served.insert(peer.clone(), session);
                                        (session, true)
                                    }
                                }
                            };

                            let _ = events_tx.send(GattEvent::Connected {
                                peer: peer.clone(),
                                local_role: Role::Peripheral,
                                session: Some(session),
                            });

                            if newly_served {
                                let handle = spawn_peripheral_disconnect_watch(
                                    adapter,
                                    address,
                                    peer.clone(),
                                    events_tx.clone(),
                                    served_peers,
                                    notify_writers,
                                    session,
                                );
                                server_watch.lock().unwrap().push(handle.abort_handle());
                            }

                            let _ = events_tx.send(GattEvent::CharacteristicWritten {
                                peer,
                                characteristic: uuid,
                                value,
                            });
                            drop(current);

                            ReqResult::Ok(())
                        })
                    })),
                    ..Default::default()
                }
            });

            // `Io` rather than `Fun`: BlueZ passes the subscribing device's
            // address to `AcquireNotify` but not to `StartNotify`, so this is
            // the only notify mode on Linux that says *who* subscribed.
            let (control, control_handle) = spec.notifiable
                .then(bluer::gatt::local::characteristic_control)
                .map(|(c, h)| (Some(c), h))
                .unwrap_or_else(|| (None, Default::default()));

            let notify = spec.notifiable.then(|| CharacteristicNotify {
                notify: true,
                method: CharacteristicNotifyMethod::Io,
                ..Default::default()
            });

            if let Some(control) = control {
                notify_sessions.push(tokio::spawn(watch_notify_sessions(
                    control,
                    uuid,
                    notify_writers.clone(),
                    events_tx.clone(),
                    served_peers.clone(),
                    adapter.clone(),
                    next_session.clone(),
                    server_watch.clone(),
                    advertise_generation.clone(),
                    this_generation,
                )));
            }

            local_characteristics.push(LocalCharacteristic {
                uuid: uuid.0,
                read,
                write,
                notify,
                control_handle,
                ..Default::default()
            });
        }

        let app = Application {
            services: vec![LocalService {
                uuid: service.uuid.0,
                primary: true,
                characteristics: local_characteristics,
                ..Default::default()
            }],
            ..Default::default()
        };

        let app_handle = self.adapter.serve_gatt_application(app).await.map_err(|err| {
            log::warn!("advertise: BlueZ rejected the GATT application: {err}");
            BleError::Gatt(err.to_string())
        })?;

        let adv = Advertisement {
            service_uuids: [service.uuid.0].into_iter().collect(),
            discoverable: Some(true),
            manufacturer_data: service.manufacturer_data.clone(),
            service_data: service
                .service_data
                .iter()
                .map(|(uuid, value)| (uuid.0, value.clone()))
                .collect(),
            ..Default::default()
        };
        let adv_handle = self.adapter.advertise(adv).await.map_err(|err| {
            log::warn!("advertise: BlueZ rejected the advertisement: {err}");
            BleError::Gatt(err.to_string())
        })?;

        // Install the new BlueZ objects and publish the generation together,
        // immediately after both registrations succeeded — see the doc
        // comment on `this_generation`'s reservation above. Published last:
        // a failed registration above leaves the outgoing generation (and
        // its still-live BlueZ objects) valid rather than burning a
        // generation number for a re-advertise that never took effect.
        *self.app_handle.lock().await = Some(app_handle);
        *self.adv_handle.lock().await = Some(adv_handle);
        *advertise_generation.lock().unwrap() = this_generation;
        log::info!("advertise: registered, generation={this_generation}");

        // The previous generation's watchers were already aborted before any
        // of this generation's tasks were spawned — see the drain near the
        // top of this function. Draining *here* would have been wrong: a
        // central can acquire a notify session between the application being
        // served and this point, and `watch_notify_sessions` pushes that
        // peer's disconnect watcher into the same vector — so a drain here
        // would abort a watcher belonging to the new advertisement, leaving
        // that peer stuck in `served_peers` with nothing to report its loss.
        self.server_watch
            .lock()
            .unwrap()
            .extend(notify_sessions.iter().map(|h| h.abort_handle()));
        // The JoinHandles themselves are dropped here, which detaches rather
        // than cancels — the AbortHandles above are what stop them.
        drop(notify_sessions);
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        log::info!("advertise: stopping");
        let _serialise = self.advertise_lock.lock().await;
        // Invalidate first, so a handler already running publishes nothing.
        // Taking the same lock the handlers publish under means an
        // in-progress publication completes before this, or is rejected —
        // never interleaved.
        *self.advertise_generation.lock().unwrap() += 1;
        for watch in self.server_watch.lock().unwrap().drain(..) {
            watch.abort();
        }
        // Announce the departure of every central we were serving *before*
        // forgetting them. Silently clearing the set left `datagram::serve`
        // holding those peers in its single-central map with their channels
        // still live, so a later `advertise` had the old task disconnecting
        // the new server's central as an interloper — locking the new
        // `serve` out until the stale peer happened to drop physically.
        let served: Vec<PeerAddress> =
            self.served_peers.lock().unwrap().drain().map(|(peer, _)| peer).collect();
        for peer in served {
            log::info!("advertise: releasing served central {} on stop", peer.0);
            let _ = self.events_tx.send(GattEvent::Disconnected {
                peer,
                local_role: Role::Peripheral,
                session: None,
            });
        }
        *self.app_handle.lock().await = None;
        *self.adv_handle.lock().await = None;
        self.notify_writers.lock().await.clear();
        Ok(())
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.notify_matching(characteristic, value, None, |_| true, "no subscriber")
            .await
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        let wanted = peer.0.clone();
        self.notify_matching(
            characteristic,
            value,
            session.map(|session| (peer, session)),
            move |writer: &CharacteristicWriter| writer.device_address().to_string() == wanted,
            &format!("{} has no live notify session", peer.0),
        )
        .await
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        // Serialised against dial (`dial_lock`) and, separately, against
        // peripheral-role session admission (`notify_writers`) — the lock
        // `watch_notify_sessions` takes before installing a replacement
        // `served_peers` entry. `dial_lock` alone does not serialise against
        // that: `watch_notify_sessions` never takes it, so a new session
        // could be installed between this call's check and its
        // `device.disconnect()`, and this call would then tear down the
        // replacement it just installed instead of the stale session it was
        // asked to end. Both are held across the check *and* the disconnect,
        // for the reason `dial_lock` already was: `Device` resolves by
        // address, so validating and then awaiting would let either kind of
        // reconnect land in the gap and have this call drop the replacement.
        let _dial = self.dial_lock.lock().await;
        let _writers = self.notify_writers.lock().await;
        if let Some(session) = session {
            if self.served_peers.lock().unwrap().get(peer) != Some(&session) {
                return Ok(());
            }
        }
        let Ok(address) = peer.0.parse() else {
            return Err(BleError::Gatt(format!("malformed peer address {}", peer.0)));
        };
        // Already gone is success: this is called to guarantee absence, not
        // to assert presence.
        let Ok(device) = self.adapter.device(address) else {
            return Ok(());
        };
        device
            .disconnect()
            .await
            .map_err(|err| BleError::Gatt(format!("disconnecting {} failed: {err}", peer.0)))
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let rx = self.events_tx.subscribe();
        Box::pin(
            tokio_stream::wrappers::BroadcastStream::new(rx)
                .map(|item| match item {
                    Ok(event) => event,
                    Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                        GattEvent::Lagged { dropped: n }
                    }
                }),
        )
    }

    async fn radio_status(&self) -> crate::entities::models::RadioStatus {
        if self.adapter.is_powered().await.unwrap_or(false) {
            crate::entities::models::RadioStatus::On
        } else {
            crate::entities::models::RadioStatus::Off
        }
    }
}

struct LinuxGattConnection {
    /// The dial this connection belongs to; see `GattEvent::Connected`.
    session: u64,
    /// Current dial per address, so this handle can tell whether it still
    /// owns the link. Without it, a handle kept across a reconnect drives
    /// BlueZ through the address-backed `Device` proxy and disconnects the
    /// connection that replaced it.
    dialed: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    /// See `LinuxBackend::dial_lock`.
    dial_lock: Arc<AsyncMutex<()>>,
    /// Peripheral-role sessions currently being served, shared with
    /// `LinuxBackend`. BlueZ keeps one ACL per remote address — disconnecting
    /// it from the central role tears down a peripheral-role session on the
    /// same peer too, since both sides of `GattConnection`/`Backend` address
    /// the same `bluer::Device`. Checked by `disconnect()` before it would
    /// otherwise kill a link the peripheral role still needs.
    served_peers: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    /// So `disconnect()` can report the central-role link as lost without
    /// actually tearing down the ACL — see its doc comment.
    events_tx: broadcast::Sender<GattEvent>,
    peer: PeerAddress,
    device: bluer::Device,
    /// Negotiated ATT MTU, refreshed from BlueZ whenever a characteristic
    /// operation gives us a cheap opportunity to read it. Starts at the
    /// spec-mandated minimum so `max_write_len()` is never optimistic before
    /// the real value is known.
    att_mtu: AtomicU16,
}

impl LinuxGattConnection {
    async fn find_characteristic(
        &self, characteristic: CharacteristicUuid,
    ) -> Result<bluer::gatt::remote::Characteristic> {
        let services = self
            .device
            .services()
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        for service in services {
            let chars = service.characteristics().await.map_err(|err| BleError::Gatt(err.to_string()))?;
            for candidate in chars {
                let uuid = candidate.uuid().await.map_err(|err| BleError::Gatt(err.to_string()))?;
                if uuid == characteristic.0 {
                    return Ok(candidate);
                }
            }
        }
        Err(BleError::Gatt(format!("characteristic {} not found on peer", characteristic.0)))
    }
}

impl LinuxGattConnection {
    /// Refuse to act when a newer dial to this peer has superseded us,
    /// matching the Android and mock backends.
    fn ensure_current(&self) -> Result<()> {
        // A *missing* entry is not ownership. `report_central_loss` removes
        // the address once the link drops, so treating absence as "still
        // ours" let a retained handle keep driving the address-backed
        // `Device` — and if that peer had since connected to us in the
        // peripheral role, its `disconnect()` would tear down that inbound
        // link instead.
        match self.dialed.lock().unwrap().get(&self.peer) {
            Some(now) if *now == self.session => Ok(()),
            _ => Err(BleError::NotConnected(self.peer.0.clone())),
        }
    }
}

#[async_trait]
impl GattConnection for LinuxGattConnection {
    fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }

    fn session(&self) -> Option<u64> {
        Some(self.session)
    }

    fn att_mtu(&self) -> u16 {
        self.att_mtu.load(Ordering::Relaxed)
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let _dial = self.dial_lock.lock().await;
        self.ensure_current()?;
        let target = self.find_characteristic(characteristic).await?;
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        // A single attempt — no internal retry. See `is_transient_gatt_error`'s
        // doc comment for why: no fixed backend-side retry budget fits every
        // caller, so a caller that wants to retry a `BleError::GattBusy`
        // does so itself, sized to its own timeout.
        match target.read().await {
            Ok(value) => Ok(value),
            Err(err) if is_transient_gatt_error(&err) => {
                log::warn!("read: {} on {} rejected, possibly transient: {err}", characteristic.0, self.peer.0);
                Err(BleError::GattBusy(err.to_string()))
            }
            Err(err) => {
                log::warn!("read: {} on {} failed: {err}", characteristic.0, self.peer.0);
                Err(BleError::Gatt(err.to_string()))
            }
        }
    }

    async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
    ) -> Result<()> {
        let request = bluer::gatt::remote::CharacteristicWriteRequest {
            op_type: match write_type {
                WriteType::WithResponse => bluer::gatt::WriteOp::Request,
                WriteType::WithoutResponse => bluer::gatt::WriteOp::Command,
            },
            ..Default::default()
        };
        // A single attempt — no internal retry; see `read`'s matching
        // comment and `is_transient_gatt_error`'s doc comment.
        let _dial = self.dial_lock.lock().await;
        self.ensure_current()?;
        let target = self.find_characteristic(characteristic).await?;
        // Cache the negotiated MTU opportunistically: BlueZ only publishes a
        // characteristic's MTU once the link is up, so this is the first
        // point it can be observed without a speculative extra round trip.
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        log::trace!("write: {} bytes to {} on {} ({write_type:?})", value.len(), characteristic.0, self.peer.0);
        match target.write_ext(&value, &request).await {
            Ok(()) => Ok(()),
            Err(err) if is_transient_gatt_error(&err) => {
                log::warn!("write: {} bytes to {} rejected, possibly transient: {err}", value.len(), self.peer.0);
                Err(BleError::GattBusy(err.to_string()))
            }
            Err(err) => {
                log::warn!("write: {} bytes to {} failed: {err}", value.len(), self.peer.0);
                Err(BleError::Gatt(err.to_string()))
            }
        }
    }

    async fn subscribe(
        &mut self, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        let _dial = self.dial_lock.lock().await;
        self.ensure_current()?;
        let target = self.find_characteristic(characteristic).await?;
        // Refresh here too. `datagram::connect` only subscribes before it
        // fixes its fragment budget, so without this a Linux datagram
        // channel stayed at the 23-byte spec minimum — 14 payload bytes per
        // fragment — for its whole life, however large the negotiated MTU.
        if let Ok(mtu) = target.mtu().await {
            log::info!(
                "subscribe: {} negotiated ATT MTU {mtu} on {}",
                self.peer.0,
                characteristic.0
            );
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        } else {
            log::warn!(
                "subscribe: {} did not publish an MTU; staying at the {}-byte default",
                self.peer.0,
                crate::hal::DEFAULT_ATT_MTU
            );
        }
        let notify_stream = target.notify().await.map_err(|err| {
            log::warn!("subscribe: notify failed on {}: {err}", characteristic.0);
            BleError::Gatt(err.to_string())
        })?;
        log::info!("subscribe: {} subscribed to {}", self.peer.0, characteristic.0);
        // BlueZ delivers notifications over a socket this stream reads
        // directly, so there is no library-side queue to overflow — nothing
        // here can silently drop a payload.
        Ok(Box::pin(notify_stream.map(Ok)))
    }

    async fn disconnect(&mut self) -> Result<()> {
        // The most damaging operation for a stale handle: `Device` is backed
        // by the address, so this would drop whichever link currently owns
        // it — including one established after this handle's own.
        //
        // The lock is taken *before* the check and held across the platform
        // call, so a reconnect cannot slip in between them. Checking and
        // then awaiting was still a check-then-act, just with a smaller
        // window.
        let _dial = self.dial_lock.lock().await;
        self.ensure_current()?;

        // BlueZ keeps exactly one ACL per remote address. If the same peer
        // also holds a live peripheral-role session right now (it dialled
        // us while we were dialling it, or just never disconnected after),
        // `Device::disconnect()` tears down that shared link too — dropping
        // a central-role channel this call's caller is done with would
        // silently kill an inbound session a completely different consumer
        // is still relying on. Hardware-confirmed: closing a central probe
        // link ended the phone's own inbound connection on the same ACL a
        // moment later, losing an in-flight reply.
        //
        // Reporting the central-role link as lost rather than leaving it
        // silently stuck: the caller asked to disconnect, and from its own
        // point of view this *is* gone — it just doesn't get to take the
        // shared radio link down with it.
        if self.served_peers.lock().unwrap().contains_key(&self.peer) {
            report_central_loss(&self.events_tx, &self.dialed, &self.peer, self.session);
            return Ok(());
        }

        self.device.disconnect().await.map_err(|err| BleError::Gatt(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbus::channel::MatchingReceiver;

    #[test]
    fn in_progress_maps_to_adapter_busy_not_unavailable() {
        let busy = bluer::Error { kind: bluer::ErrorKind::InProgress, message: "Operation already in progress".into() };
        assert!(matches!(discovery_error(busy), BleError::AdapterBusy(_)));
        let off = bluer::Error { kind: bluer::ErrorKind::NotReady, message: "Resource Not Ready".into() };
        assert!(matches!(discovery_error(off), BleError::AdapterUnavailable(_)));
    }

    #[test]
    fn stop_is_owed_only_when_running_with_no_live_scan() {
        let mut state = ScanState::default();
        assert!(!state.idle_running());
        state.running = true;
        state.live = 1;
        assert!(!state.idle_running());
        state.live = 0;
        assert!(state.idle_running());
    }

    /// A fake BlueZ adapter with BlueZ's per-client discovery rules
    /// (`src/adapter.c`): `StartDiscovery` from a client already in the
    /// discovery list is refused with `InProgress`, and `Discovering` is
    /// true while any client is in it. `fail_next_stop` makes the next
    /// `StopDiscovery` fail *and leave the client listed* — the wedge seen
    /// on real hardware.
    #[derive(Default)]
    struct FakeAdapter {
        clients: HashSet<String>,
        fail_next_stop: bool,
        starts: usize,
        /// Held inside the `StartDiscovery` handler before it replies, so a
        /// test can cancel the caller after BlueZ has accepted the call.
        start_delay: std::time::Duration,
    }

    type Fake = Arc<StdMutex<FakeAdapter>>;

    /// A private `dbus-daemon`, killed on drop.
    ///
    /// libdbus reads the system bus address once per process, so only one
    /// test in this binary can use a private bus: every scenario that needs
    /// one lives in `discovery_recovers_from_a_failed_stop`.
    struct PrivateBus(std::process::Child);

    impl Drop for PrivateBus {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Starts a private bus, points this process's *system* bus at it, and
    /// serves `FakeAdapter` as `org.bluez` `/org/bluez/hci0` on it. `None`
    /// when no `dbus-daemon` is installed.
    async fn fake_bluez() -> Option<(PrivateBus, Fake)> {
        use std::io::BufRead;
        let mut child = std::process::Command::new("dbus-daemon")
            .args(["--session", "--nofork", "--print-address"])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .ok()?;
        let mut address = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut address).unwrap();
        let bus = PrivateBus(child);
        // Only this module's tests touch the system bus.
        std::env::set_var("DBUS_SYSTEM_BUS_ADDRESS", address.trim());

        let (resource, conn) = dbus_tokio::connection::new_system_sync().unwrap();
        tokio::spawn(resource);
        conn.request_name("org.bluez", false, true, false).await.unwrap();
        let fake = Fake::default();
        let mut cr = dbus_crossroads::Crossroads::new();
        cr.set_async_support(Some((
            conn.clone(),
            Box::new(|task| {
                tokio::spawn(task);
            }),
        )));
        let iface = cr.register("org.bluez.Adapter1", |b: &mut dbus_crossroads::IfaceBuilder<Fake>| {
            b.property::<bool, _>("Discovering").get(|_, fake| Ok(!fake.lock().unwrap().clients.is_empty()));
            b.method("SetDiscoveryFilter", ("filter",), (), |_, _, (_filter,): (dbus::arg::PropMap,)| Ok(()));
            // Async so `start_delay` holds only this reply, not a runtime
            // thread.
            b.method_with_cr_async("StartDiscovery", (), (), |mut ctx, cr, ()| {
                let fake: Fake = cr.data_mut::<Fake>(ctx.path()).unwrap().clone();
                let sender = ctx.message().sender().unwrap().to_string();
                async move {
                    let delay = fake.lock().unwrap().start_delay;
                    tokio::time::sleep(delay).await;
                    let result = {
                        let mut fake = fake.lock().unwrap();
                        if fake.clients.insert(sender) {
                            fake.starts += 1;
                            Ok(())
                        } else {
                            Err(("org.bluez.Error.InProgress", "Operation already in progress").into())
                        }
                    };
                    ctx.reply(result)
                }
            });
            b.method("StopDiscovery", (), (), |ctx, fake, ()| {
                let sender = ctx.message().sender().unwrap().to_string();
                let mut fake = fake.lock().unwrap();
                if std::mem::take(&mut fake.fail_next_stop) {
                    return Err(("org.bluez.Error.Failed", "injected StopDiscovery failure").into());
                }
                if !fake.clients.remove(&sender) {
                    return Err(("org.bluez.Error.Failed", "No discovery started").into());
                }
                Ok(())
            });
        });
        cr.insert("/org/bluez/hci0", &[iface], fake.clone());
        conn.start_receive(
            dbus::message::MatchRule::new_method_call(),
            Box::new(move |msg, conn| {
                let _ = cr.handle_message(msg, conn);
                true
            }),
        );
        Some((bus, fake))
    }

    /// The real-hardware wedge, end to end against the fake. A failed
    /// `StopDiscovery` that leaves our client listed must keep the stop
    /// owed, so the next `connect()` retries it rather than dialling into
    /// the discovery (Codex P1 on PR #21). And if our state has lost track
    /// of a listed client (e.g. a start cancelled after BlueZ accepted it),
    /// the next scan's `InProgress` must be cleared and the scan started
    /// anyway, not fail every scan from then on. And a scan cancelled
    /// mid-start must leave the stop owed. Also covers refcounted sharing.
    #[tokio::test(flavor = "multi_thread")]
    async fn discovery_recovers_from_a_failed_stop() {
        let Some((_bus, fake)) = fake_bluez().await else {
            eprintln!("dbus-daemon not installed; skipping");
            return;
        };
        let session = Session::new().await.unwrap();
        let adapter = session.adapter("hci0").unwrap();
        let discovery = Arc::new(DiscoveryBus::open(&adapter).await.unwrap());

        // Two concurrent scans share one discovery.
        let first = discovery.begin_scan().await.unwrap();
        let second = discovery.begin_scan().await.unwrap();
        assert_eq!(fake.lock().unwrap().starts, 1);
        drop(first);
        assert!(!discovery.state.lock().unwrap().idle_running(), "a live scan still needs discovery");

        // The last scan ends and its StopDiscovery fails: wedged, and the
        // stop must stay owed.
        fake.lock().unwrap().fail_next_stop = true;
        drop(second);
        discovery.stop_if_idle("test").await;
        assert_eq!(fake.lock().unwrap().clients.len(), 1, "fake must now be wedged");
        assert!(discovery.state.lock().unwrap().idle_running(), "a failed stop must stay owed");

        // What `connect()` awaits: the owed stop is retried, and discovery
        // has really stopped once it returns.
        discovery.stop_if_idle("test").await;
        assert!(fake.lock().unwrap().clients.is_empty(), "discovery must be stopped before a dial");
        assert!(!discovery.state.lock().unwrap().running);

        // Our state loses track of a client BlueZ still lists: the next scan
        // gets `InProgress`, and must recover instead of failing.
        fake.lock().unwrap().clients.insert(session_unique_name(&discovery));
        let third = discovery.begin_scan().await.expect("scan must recover from the wedge");
        assert_eq!(fake.lock().unwrap().clients.len(), 1);
        assert_eq!(fake.lock().unwrap().starts, 2);
        drop(third);
        discovery.stop_if_idle("test").await;
        assert!(fake.lock().unwrap().clients.is_empty());

        // A scan cancelled after BlueZ accepted its `StartDiscovery` must
        // leave the stop owed, so the next `connect()` stops discovery
        // instead of dialling into it (Codex P1 on PR #21).
        fake.lock().unwrap().start_delay = std::time::Duration::from_millis(300);
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_millis(50), discovery.begin_scan()).await;
        assert!(cancelled.is_err(), "the scan must be cancelled mid-start");
        // Let the fake finish accepting the start.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        fake.lock().unwrap().start_delay = std::time::Duration::ZERO;
        assert_eq!(fake.lock().unwrap().clients.len(), 1, "BlueZ accepted the start");
        assert!(discovery.state.lock().unwrap().idle_running(), "the stop must be owed");
        discovery.stop_if_idle("test").await;
        assert!(fake.lock().unwrap().clients.is_empty(), "discovery must be stopped before a dial");
    }

    #[test]
    fn only_a_definite_start_failure_clears_the_owed_stop() {
        let err = |kind| bluer::Error { kind, message: String::new() };
        assert!(start_proved_not_running(&err(bluer::ErrorKind::NotReady)));
        assert!(!start_proved_not_running(&err(bluer::ErrorKind::InProgress)));
        assert!(!start_proved_not_running(&err(bluer::ErrorKind::Internal(
            bluer::InternalErrorKind::DBus("org.freedesktop.DBus.Error.NoReply".into())
        ))));
    }

    fn session_unique_name(discovery: &DiscoveryBus) -> String {
        discovery.conn.unique_name().to_string()
    }
}

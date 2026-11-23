//! Linux driver: BlueZ via `bluer` (async D-Bus binding, 442+ GitHub stars,
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

mod advertisement_release;
mod driver;

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
/// driver also serves) still fails promptly rather than stalling every
/// `notify`/`notify_peer` caller for this long.
const NOTIFY_WRITER_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// How often a GATT handler that BlueZ called before `advertise` published
/// its generation checks again, and how many times a handler waits (2s) before
/// it is treated as stale.
const GENERATION_PUBLISH_POLL: std::time::Duration = std::time::Duration::from_millis(10);
const GENERATION_PUBLISH_MAX_POLLS: usize = 200;

/// How long to wait, after bluetoothd reappears on the bus, for its adapter
/// to report powered before giving up on reporting the radio back `On`.
const BLUEZ_RESTART_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const BLUEZ_RESTART_READY_POLL: std::time::Duration = std::time::Duration::from_millis(250);
const NOTIFY_WRITER_ACQUIRE_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// How long `connect()` waits for this driver's own discovery to stop
/// before dialling anyway.
///
/// Dropping a scan stream used to leave discovery running for an unknown
/// time: `bluer` sends `StopDiscovery` from a spawned task once its session
/// token drops, so a caller that dropped its scan and dialled immediately
/// raced `Connect()` against a still-running discovery. Real-hardware
/// evidence (Android phone <-> Linux desktop, fini PR 185): that race produced
/// `le-connection-abort-by-local` two seconds into the dial. `connect()`
/// now issues (or waits for) the `StopDiscovery` itself; bounded so a
/// BlueZ that never replies costs a dial this long rather than stalling it.
const DISCOVERY_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// D-Bus reply timeout for `SetDiscoveryFilter`/`StartDiscovery`/
/// `StopDiscovery` on the discovery connection.
const DISCOVERY_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long `update_advertisement` waits for BlueZ to free the instance of
/// the advertisement it withdrew, on a controller with no spare one; bounded
/// so a BlueZ that never reports it costs an update this long, not a hang.
const ADVERTISING_INSTANCE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// How often that wait reads BlueZ's free instance count.
const ADVERTISING_INSTANCE_CHECK_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(20);

use crate::drivers::event_bus::EventBus;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    advertised_name, carries_fast_advertisement, truncate_name, CapabilityReport,
    CharacteristicUuid, DiscoveredPeer, FastAdvertisement, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType, LEGACY_ADVERTISING_DATA_LEN, LOCAL_NAME_MAX_LEN,
};
use crate::hal::{BoxStream, Driver, GattConnection};

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// How long `LinuxDriver::connect` waits for BlueZ's `Connect` D-Bus method
/// to reply before giving up.
///
/// Real-hardware evidence: with no bound at all, a `Connect` call to an
/// unresponsive/out-of-range peer can hang effectively forever — not just
/// past a caller's own external timeout, since `dial_lock` is held across
/// the whole call and every subsequent dial to that address (and, when the
/// lock was still driver-wide, every other peer's operations; and,
/// evidence suggests, the
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
/// `dial_lock` is just as capable of starving later dials to the address as
/// the original stuck connect was. Shorter than `CONNECT_TIMEOUT`: this is a
/// best-effort cancellation signal, not an operation worth waiting as long
/// for.
const CLEANUP_DISCONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long the delayed cleanup task (armed once `CLEANUP_DISCONNECT_TIMEOUT`
/// itself elapses) waits between retries of its own `Disconnect` call, while
/// an address stays quarantined in `pending_cleanup`. Not itself bounded by
/// a timeout — see that task's own doc comment for why a definitive Err
/// must not lift the quarantine either.
const PENDING_CLEANUP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Hard cap on how many `connect()` attempts this driver will allow to be
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
/// attempt this driver cannot guarantee it can ever clean up.
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
impl BleError {
    fn bluez_transient(err: &bluer::Error) -> bool {
        err.kind == bluer::ErrorKind::Failed
    }

    /// True when `err` is BlueZ reporting `org.bluez.Error.NotConnected` — a
    /// D-Bus error name with no dedicated `bluer::ErrorKind` variant, so it
    /// surfaces as `ErrorKind::Internal(InternalErrorKind::DBus(name))` with
    /// the original D-Bus error name preserved verbatim in `name` (see
    /// `bluer::Error`'s `From<dbus::Error>` impl). Matched structurally on that
    /// name rather than on `err.message()`'s free-form text, which BlueZ does
    /// not guarantee stays stable.
    fn bluez_not_connected(err: &bluer::Error) -> bool {
        matches!(
            &err.kind,
            bluer::ErrorKind::Internal(bluer::InternalErrorKind::DBus(name))
                if name == "org.bluez.Error.NotConnected"
        )
    }

    /// True when `err` is BlueZ's `org.bluez.Error.InProgress` — which
    /// `StartDiscovery` returns when the calling D-Bus client is *already* in
    /// BlueZ's discovery list.
    fn bluez_in_progress(err: &bluer::Error) -> bool {
        err.kind == bluer::ErrorKind::InProgress
    }

    /// Maps a failure to start discovery. A busy adapter is not a missing one:
    /// only `InProgress` becomes `AdapterBusy`, everything else keeps the
    /// existing `AdapterUnavailable` mapping.
    fn bluez_discovery(err: bluer::Error) -> BleError {
        if BleError::bluez_in_progress(&err) {
            BleError::AdapterBusy(err.to_string())
        } else {
            BleError::AdapterUnavailable(err.to_string())
        }
    }

    /// True when a failed `StartDiscovery` proves this client is not
    /// discovering. Not for `InProgress` (BlueZ still lists the client) nor for
    /// a reply timeout (`org.freedesktop.DBus.Error.NoReply`: BlueZ may have
    /// accepted it) — in both, a stop stays owed.
    fn bluez_start_not_running(err: &bluer::Error) -> bool {
        let no_reply = matches!(
            &err.kind,
            bluer::ErrorKind::Internal(bluer::InternalErrorKind::DBus(name))
                if name == "org.freedesktop.DBus.Error.NoReply"
        );
        !BleError::bluez_in_progress(err) && !no_reply
    }
}

/// This driver's own view of its discovery. Only touched briefly, never
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

/// Discovery, on a D-Bus connection this driver owns instead of `bluer`'s.
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
/// lets this driver send `StopDiscovery` itself, see its result, and have
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
    /// The driver's own `bluer` adapter, only for reading `Discovering`.
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
        let adapter_path =
            dbus::Path::new(format!("/org/bluez/{}", adapter.name())).map_err(unavailable)?;
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
        let proxy = dbus::nonblock::Proxy::new(
            "org.bluez",
            &self.adapter_path,
            DISCOVERY_CALL_TIMEOUT,
            &*self.conn,
        );
        proxy
            .method_call::<(), _, _, _>("org.bluez.Adapter1", method, args)
            .await
            .map_err(bluer::Error::from)
    }

    /// `SetDiscoveryFilter` + `StartDiscovery`: any UUID, LE only.
    ///
    /// LE only because a scan here only ever looks for GATT services, which
    /// BR/EDR inquiry never finds. `"auto"` (what `bluer` sent) interleaves
    /// that inquiry with the LE scan, and inquiry takes radio time from
    /// every live connection on the adapter, including the LE link a scan
    /// runs alongside.
    async fn start_discovery(&self) -> bluer::Result<()> {
        use dbus::arg::{RefArg, Variant};
        let mut filter: HashMap<&str, Variant<Box<dyn RefArg>>> = HashMap::new();
        filter.insert("UUIDs", Variant(Box::new(Vec::<String>::new())));
        filter.insert("Transport", Variant(Box::new("le".to_string())));
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
                Err(err) if BleError::bluez_in_progress(&err) => {
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
                if BleError::bluez_start_not_running(&err) {
                    self.state.lock().unwrap().running = false;
                }
                return Err(BleError::bluez_discovery(err));
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
/// itself (`LinuxDriver::settle_discovery`).
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

/// The BlueZ advertisement for `service`, shared by `advertise` and
/// `update_advertisement`.
impl GattServiceSpec {
    /// `with_interval`: ask for the interval of `advertising_mode`. BlueZ
    /// before 5.x without the experimental `MinInterval`/`MaxInterval`
    /// refuses it; `register_advertisement` then retries without.
    ///
    /// BlueZ cannot place data in the scan response (its `ScanResponse*`
    /// properties are experimental, and bluer does not expose them), so the
    /// fast advertisement is not sent, and the service data under `uuid` is
    /// left out when it does not fit the legacy packet: a scanner then reads
    /// the GATT advertisement (ADR-0012 §2).
    fn bluez_advertisement(&self, with_interval: bool) -> Advertisement {
        let mut service_data = self.service_data.clone();
        if self.legacy_advertising_len() > LEGACY_ADVERTISING_DATA_LEN
            && service_data.remove(&self.uuid).is_some()
        {
            log::info!(
                "advertise: the service data under {} does not fit the legacy advertisement; scanners read the GATT advertisement",
                self.uuid.0
            );
        }
        let interval = with_interval.then(|| self.advertising_mode.interval());
        Advertisement {
            service_uuids: [self.uuid.0].into_iter().collect(),
            discoverable: Some(true),
            manufacturer_data: self.manufacturer_data.clone(),
            service_data: service_data
                .into_iter()
                .map(|(uuid, value)| (uuid.0, value))
                .collect(),
            // BlueZ sends LocalName in the scan response. Truncated here so a
            // long name is cut on a character boundary, not by BlueZ.
            local_name: self
                .local_name
                .as_deref()
                .map(|name| truncate_name(name, LOCAL_NAME_MAX_LEN).to_string()),
            min_interval: interval,
            max_interval: interval,
            ..Default::default()
        }
    }
}

/// One dial/teardown lock per peer address (see `LinuxDriver::dial_lock`).
///
/// The race the lock closes is per address: `Device` resolves by address, so
/// only operations on the *same* address can drop each other's link. A
/// single driver-wide lock also made every other peer wait behind a slow
/// dial: on hardware, a 20s Connect to a stale address held a live link's
/// write for those 20s, and the QUIC handshake on that link timed out.
///
/// Entries no one holds or waits on are dropped on the next `lock`, so
/// rotating private addresses do not grow the map without bound.
#[derive(Clone, Default)]
struct DialLocks(Arc<StdMutex<HashMap<PeerAddress, Arc<AsyncMutex<()>>>>>);

impl DialLocks {
    async fn lock(&self, peer: &PeerAddress) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.0.lock().unwrap();
            locks.retain(|_, lock| Arc::strong_count(lock) > 1);
            locks.entry(peer.clone()).or_default().clone()
        };
        lock.lock_owned().await
    }
}

pub struct LinuxDriver {
    // Held only to keep the D-Bus session connection alive for the
    // lifetime of the driver; `Adapter` clones its own `Arc` into the
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
    events_tx: EventBus,
    app_handle: AsyncMutex<Option<ApplicationHandle>>,
    adv_handle: AsyncMutex<Option<bluer::adv::AdvertisementHandle>>,
    advertisement_release: advertisement_release::AdvertisementRelease,
    /// The spec the live application and advertisement were built from, so
    /// `update_advertisement` can tell whether a spec is for the served
    /// service. `None` while nothing is fully registered.
    served_service: StdMutex<Option<GattServiceSpec>>,
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
    /// Serialises every GATT operation against dial and teardown, per peer
    /// address.
    ///
    /// `bluer`'s `Device` is address-backed, so checking ownership and then
    /// awaiting `disconnect()` leaves a window in which a reconnect installs
    /// a new dial — and the in-flight call then drops *that* link. A plain
    /// mutex around the check cannot help, because the platform call is
    /// asynchronous; the two operations have to exclude each other for their
    /// whole duration. See [`DialLocks`] for why it is per address.
    dial_lock: DialLocks,
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
    /// Source of every value `advertise_generation` ever takes. Reserving
    /// `current + 1` let a cancelled `advertise` and the next one share a
    /// number, so the abandoned call's handlers would count as current in the
    /// replacement server. Strictly increasing, so no number is reused.
    generation_counter: Arc<AtomicU64>,
    /// Addresses this driver dialled itself, in the central role. BlueZ
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
    /// Services `sweep_leftover_links` already ran for.
    swept: StdMutex<HashSet<ServiceUuid>>,
    /// The company a scan admits fast advertisements under: that of the last
    /// spec advertised with one, else [`FastAdvertisement::DEFAULT_SCAN_COMPANY`].
    fast_scan_company: AtomicU16,
}

/// Reports bluetoothd itself leaving and rejoining the bus — a crash or a
/// package upgrade restarting it — as `RadioChanged` `Off`/`On`. Once
/// bluetoothd exits no process is left to emit `Powered(false)`, so the
/// adapter-property watcher goes silent, and nothing re-registered the GATT
/// application and advertisement BlueZ forgot. `NameOwnerChanged` for
/// `org.bluez` is emitted by the bus daemon itself, so it still arrives.
///
/// `RadioChanged` rather than a new event: for a consumer, "bluetoothd is
/// gone" and "the adapter powered off" mean the same, and `datagram::serve`
/// and `Peripheral::serve` already re-register on `RadioStatus::On`.
///
/// Returns once the subscription is in place, so no restart is missed
/// between this call and the watch starting.
impl LinuxDriver {
    async fn watch_bluez_restart(
        conn: Arc<dbus::nonblock::SyncConnection>,
        adapter: Adapter,
        events_tx: EventBus,
    ) {
        let rule = dbus::message::MatchRule::new_signal("org.freedesktop.DBus", "NameOwnerChanged");
        let Ok(matched) = conn.add_match(rule).await else {
            log::warn!(
            "bluez watch: could not subscribe to NameOwnerChanged; restart recovery is disabled"
        );
            return;
        };
        tokio::spawn(async move {
            let (_token, mut stream) = matched.msg_stream();
            while let Some(msg) = stream.next().await {
                let (name, _old_owner, new_owner) = msg.get3::<&str, &str, &str>();
                if name != Some("org.bluez") {
                    continue;
                }
                if new_owner.is_none_or(str::is_empty) {
                    log::warn!("bluez watch: bluetoothd left the bus, reporting radio Off");
                    let _ = events_tx.send(GattEvent::RadioChanged {
                        status: crate::entities::models::RadioStatus::Off,
                    });
                    continue;
                }
                // bluetoothd claims its bus name before it has exported and
                // powered the adapter, and `On` tells consumers to re-register
                // right away — so wait until it is usable.
                let ready = tokio::time::timeout(BLUEZ_RESTART_READY_TIMEOUT, async {
                    while !adapter.is_powered().await.unwrap_or(false) {
                        tokio::time::sleep(BLUEZ_RESTART_READY_POLL).await;
                    }
                })
                .await;
                if ready.is_err() {
                    log::warn!(
                        "bluez watch: bluetoothd is back but the adapter is not powered after \
                     {BLUEZ_RESTART_READY_TIMEOUT:?}; leaving radio Off"
                    );
                    continue;
                }
                log::info!(
                "bluez watch: bluetoothd is back and the adapter is powered, reporting radio On"
            );
                let _ = events_tx.send(GattEvent::RadioChanged {
                    status: crate::entities::models::RadioStatus::On,
                });
            }
        });
    }

    pub async fn new() -> Result<Self> {
        let session = Session::new()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        let adapter = session
            .default_adapter()
            .await
            .map_err(|err| BleError::AdapterUnavailable(err.to_string()))?;
        // An adapter that exists but is powered off is *not* a construction
        // failure: `PeerLink` needs the driver to exist so its `Powered`
        // watcher can report the adapter coming back, and `radio_status()`
        // reports the current state. Only a missing adapter (handled above)
        // is unrecoverable. Operations attempted while unpowered fail on
        // their own, honestly.
        let discovery = Arc::new(DiscoveryBus::open(&adapter).await?);
        let events_tx = EventBus::new(EVENT_CHANNEL_CAPACITY);

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

        LinuxDriver::watch_bluez_restart(
            discovery.conn.clone(),
            adapter.clone(),
            events_tx.clone(),
        )
        .await;

        Ok(Self {
            _session: session,
            adapter,
            values: Arc::new(StdMutex::new(HashMap::new())),
            served_peers: Arc::new(StdMutex::new(HashMap::new())),
            next_session: Arc::new(AtomicU64::new(1)),
            dial_lock: DialLocks::default(),
            advertise_lock: Arc::new(AsyncMutex::new(())),
            advertise_generation: Arc::new(StdMutex::new(1)),
            generation_counter: Arc::new(AtomicU64::new(1)),
            notify_writers: Arc::new(AsyncMutex::new(HashMap::new())),
            events_tx,
            app_handle: AsyncMutex::new(None),
            adv_handle: AsyncMutex::new(None),
            advertisement_release: Default::default(),
            served_service: StdMutex::new(None),
            server_watch: Arc::new(StdMutex::new(Vec::new())),
            dialed: Arc::new(StdMutex::new(HashMap::new())),
            pending_cleanup: Arc::new(StdMutex::new(HashSet::new())),
            in_flight: Arc::new(StdMutex::new(HashSet::new())),
            cleanup_permits: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_CLEANUP_DISCONNECTS,
            )),
            discovery,
            swept: StdMutex::new(HashSet::new()),
            fast_scan_company: AtomicU16::new(FastAdvertisement::DEFAULT_SCAN_COMPANY),
        })
    }

    /// Scan for fast advertisements under the company `spec` sends its own
    /// under, when it has one.
    fn note_fast_company(&self, spec: &GattServiceSpec) {
        if let Some(fast) = &spec.fast_advertisement {
            self.fast_scan_company
                .store(fast.company_id, Ordering::Relaxed);
        }
    }

    /// The first time a service is scanned for or advertised, before either
    /// starts: disconnect every device BlueZ reports connected that offers
    /// the service and that this process neither dialled nor serves
    /// (ADR-0009 D2). BlueZ keeps the links of a process that died, and on
    /// hardware one left at a phone's old address made it abort every dial
    /// to the phone's new one until the adapter was power-cycled. Android,
    /// Apple and Windows release a dead app's links themselves.
    async fn sweep_leftover_links(&self, service: ServiceUuid) {
        if !self.swept.lock().unwrap().insert(service) {
            return;
        }
        let Ok(addresses) = self.adapter.device_addresses().await else {
            return;
        };
        for address in addresses {
            let peer = PeerAddress(address.to_string());
            if self.dialed.lock().unwrap().contains_key(&peer)
                || self.served_peers.lock().unwrap().contains_key(&peer)
            {
                continue;
            }
            let Ok(device) = self.adapter.device(address) else {
                continue;
            };
            if !device.is_connected().await.unwrap_or(false) {
                continue;
            }
            let offers = device
                .uuids()
                .await
                .ok()
                .flatten()
                .is_some_and(|uuids| uuids.contains(&service.0));
            if !offers {
                continue;
            }
            log::warn!(
                "startup: disconnecting {}: connected with service {}, but not by this process (left by an earlier one)",
                peer.0,
                service.0
            );
            let _dial = self.dial_lock.lock(&peer).await;
            match tokio::time::timeout(CLEANUP_DISCONNECT_TIMEOUT, device.disconnect()).await {
                Ok(Ok(())) => log::info!("startup: {} disconnected", peer.0),
                Ok(Err(err)) => log::warn!("startup: disconnecting {} failed: {err}", peer.0),
                Err(_) => log::warn!(
                    "startup: disconnecting {} timed out after {CLEANUP_DISCONNECT_TIMEOUT:?}",
                    peer.0
                ),
            }
        }
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
        if tokio::time::timeout(
            DISCOVERY_STOP_TIMEOUT,
            self.discovery.stop_if_idle("before dialling"),
        )
        .await
        .is_err()
        {
            log::warn!("connect: discovery did not stop within {DISCOVERY_STOP_TIMEOUT:?}; dialling anyway");
        }
    }
}

impl LinuxDriver {
    /// Register `service`'s advertisement with the interval of its
    /// advertising mode, or, when BlueZ refuses that, once more without it.
    async fn register_advertisement(
        &self,
        service: &GattServiceSpec,
    ) -> bluer::Result<bluer::adv::AdvertisementHandle> {
        match self
            .adapter
            .advertise(service.bluez_advertisement(true))
            .await
        {
            Ok(handle) => {
                log::info!(
                    "advertise: interval {:?} ({:?})",
                    service.advertising_mode.interval(),
                    service.advertising_mode
                );
                Ok(handle)
            }
            Err(err) => {
                log::info!("advertise: BlueZ refused the advertising interval ({err}); registering without it");
                self.adapter
                    .advertise(service.bluez_advertisement(false))
                    .await
            }
        }
    }

    async fn release_advertisement(
        &self,
        handle: &mut Option<bluer::adv::AdvertisementHandle>,
    ) -> Result<()> {
        let released = self
            .advertisement_release
            .release(
                handle,
                || self.adapter.supported_advertising_instances(),
                ADVERTISING_INSTANCE_WAIT,
                ADVERTISING_INSTANCE_CHECK_INTERVAL,
            )
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        Self::check_advertisement_release(released)
    }

    async fn wait_advertisement_release(&self) -> Result<()> {
        let released = self
            .advertisement_release
            .wait(
                || self.adapter.supported_advertising_instances(),
                ADVERTISING_INSTANCE_WAIT,
                ADVERTISING_INSTANCE_CHECK_INTERVAL,
            )
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        Self::check_advertisement_release(released)
    }

    fn check_advertisement_release(released: bool) -> Result<()> {
        if !released {
            return Err(BleError::Gatt(
                "advertisement unregister was not confirmed before the deadline".into(),
            ));
        }
        Ok(())
    }

    /// A generation number never handed out before; see `generation_counter`.
    fn next_generation(&self) -> u64 {
        self.generation_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Announces every served central as gone, then forgets it and its notify
    /// sessions. For when the registration serving them ends (stop, or a
    /// replacement `advertise`), whose disconnect watchers are aborted.
    ///
    /// Announcing first matters: silently clearing left `datagram::serve`
    /// holding those peers with their channels live, so the next server
    /// refused the real central as an interloper. And forgetting matters: a
    /// stale `served_peers` entry makes `watch_notify_sessions` skip the
    /// peer's next subscription, so it is never given a session or a
    /// disconnect watcher again.
    async fn release_served_peers(&self, why: &str) {
        let mut writers = self.notify_writers.lock().await;
        let served: Vec<PeerAddress> = self
            .served_peers
            .lock()
            .unwrap()
            .drain()
            .map(|(peer, _)| peer)
            .collect();
        self.announce_released(served, why);
        writers.clear();
    }

    fn announce_released(&self, served: Vec<PeerAddress>, why: &str) {
        for peer in served {
            log::info!("advertise: releasing served central {} on {why}", peer.0);
            let _ = self.events_tx.send(GattEvent::Disconnected {
                peer,
                local_role: Role::Peripheral,
                session: None,
            });
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "a detached task: it gets its own copy of each shared part of the driver"
    )]
    async fn watch_notifications(
        mut control: CharacteristicControl,
        uuid: CharacteristicUuid,
        writers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
        events_tx: EventBus,
        served_peers: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
        adapter: Adapter,
        next_session: Arc<AtomicU64>,
        watchers: Arc<StdMutex<Vec<tokio::task::AbortHandle>>>,
        advertise_generation: Arc<StdMutex<u64>>,
        this_generation: u64,
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
            // A subscription can reach this generation's handlers in the instant
            // between BlueZ serving the application and `advertise` publishing
            // the generation. That is "not yet current", not stale: wait for the
            // publish instead of returning, which would end this watcher and
            // ignore every later subscription. `advertise` publishes while
            // holding `writers`, so once the generation is current here it
            // cannot change before the check below.
            // Bounded: an `advertise` cancelled between serving and publishing
            // never publishes, and this watcher must not wait for it forever.
            // Generations are never reused, so once a later one is published
            // this one reads as stale below and the watcher ends.
            let mut polls = 0;
            let mut writers_guard = loop {
                let guard = writers.lock().await;
                if *advertise_generation.lock().unwrap() >= this_generation
                    || polls >= GENERATION_PUBLISH_MAX_POLLS
                {
                    break guard;
                }
                drop(guard);
                polls += 1;
                tokio::time::sleep(GENERATION_PUBLISH_POLL).await;
            };
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

            let handle = LinuxDriver::watch_peripheral_disconnect(
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
    #[allow(
        clippy::too_many_arguments,
        reason = "a detached task: it gets its own copy of each shared part of the driver"
    )]
    fn watch_peripheral_disconnect(
        adapter: Adapter,
        address: bluer::Address,
        peer: PeerAddress,
        events_tx: EventBus,
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
                        () = disconnected => break,
                        () = tokio::time::sleep(NOTIFY_SESSION_POLL) => {
                            let past_acquire_grace =
                                spawned_at.elapsed() >= NOTIFY_WRITER_ACQUIRE_TIMEOUT;
                            if past_acquire_grace && !LinuxDriver::has_notification_session(&writers, &peer).await {
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
    async fn has_notification_session(
        writers: &Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
        peer: &PeerAddress,
    ) -> bool {
        let mut writers = writers.lock().await;
        let mut live = false;
        for sessions in writers.values_mut() {
            sessions.retain(|w| !w.is_closed().unwrap_or(true));
            if sessions
                .iter()
                .any(|w| w.device_address().to_string() == peer.0)
            {
                live = true;
            }
        }
        live
    }

    /// Deliver to every live notify session matching `want`.
    ///
    /// Sessions are pruned before *and* after writing, because one can close
    /// mid-write. Reaching nobody is an error: a reliable `send` must never
    /// be told a dropped payload was delivered.
    async fn notify_matching(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
        owner: Option<(&PeerAddress, u64)>,
        want: impl Fn(&CharacteristicWriter) -> bool,
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
            let has_match = writers.get(&characteristic).is_some_and(|sessions| {
                sessions
                    .iter()
                    .any(|w| !w.is_closed().unwrap_or(true) && want(w))
            });
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
                return Err(BleError::Gatt(
                    "no active notify session for characteristic".to_string(),
                ));
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
                log::warn!(
                    "notify: {} bytes on {} reached {nobody}",
                    value.len(),
                    characteristic.0
                );
                return Err(BleError::Gatt(match last_error {
                    Some(err) => format!("notify reached {nobody}: {err}"),
                    None => format!("notify reached {nobody}"),
                }));
            }
            log::trace!(
                "notify: {} bytes delivered on {}",
                value.len(),
                characteristic.0
            );
            return Ok(());
        }
    }
}

/// Cancellation guard for `LinuxDriver::connect`'s `device.connect().await`
/// -- see the doc comment at its construction site for why this exists.
/// `bluer::Device::disconnect()` is async and `Drop::drop` cannot await, so
/// the actual BlueZ teardown is a detached `tokio::spawn`; only the local
/// `dialed` bookkeeping is cleared synchronously here.
struct LinuxConnectGuard {
    adapter: Adapter,
    address: bluer::Address,
    dialed: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    dial_lock: DialLocks,
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
        self.pending_cleanup
            .lock()
            .unwrap()
            .insert(self.peer.clone());
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
                    // quarantined regardless, so holding the lock (then
                    // driver-wide, now this address's own) for another
                    // `CLEANUP_DISCONNECT_TIMEOUT` just to poll an existing
                    // task needlessly blocked other operations for that
                    // whole span, repeated on every timeout.
                    let _dial = dial_lock.lock(&peer).await;
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
                let outcome =
                    tokio::time::timeout(CLEANUP_DISCONNECT_TIMEOUT, handle.as_mut().unwrap())
                        .await;
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
                    Ok(Ok(Err(err))) if BleError::bluez_not_connected(&err) => {
                        log::info!(
                            "connect: {} cleanup disconnect reports not connected; treating as already clean",
                            peer.0
                        );
                        break;
                    }
                    // BlueZ removed the device object (it does for a stale
                    // private address it no longer sees). A device that no
                    // longer exists has no link left to clean up; retrying
                    // kept the address quarantined for minutes on hardware.
                    Ok(Ok(Err(err))) if err.kind == bluer::ErrorKind::NotFound => {
                        log::info!(
                            "connect: {} cleanup disconnect found the device already removed; treating as clean",
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
    /// The services BlueZ knows so far do not include the target, and it
    /// carries no fast advertisement. More may still arrive (a scan response
    /// can carry the target UUID after the first advertisement listed
    /// others).
    NotTarget,
    /// Matches, with manufacturer data.
    Complete(DiscoveredPeer),
    /// Matches, but no manufacturer data has arrived yet.
    NoManufacturerData(DiscoveredPeer),
    /// BlueZ has not reported any services for it yet.
    Unknown,
}

/// How often a scan re-reads the devices it has not fully reported yet. Reads
/// are property gets on BlueZ's cached objects, not radio work, and a device
/// that does not advertise the target costs a `UUIDs` and a
/// `ManufacturerData` read per tick.
const SCAN_RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// The radio records a scan last reported for one device. RSSI is left out:
/// it changes with nearly every advertisement.
#[derive(PartialEq)]
struct ReportedRecords {
    manufacturer_data: std::collections::BTreeMap<u16, Vec<u8>>,
    service_data: std::collections::BTreeMap<ServiceUuid, Vec<u8>>,
    name: Option<String>,
}

/// What one scan has reported, per device.
#[derive(Default)]
struct ScanReports(HashMap<bluer::Address, ReportedRecords>);

impl ScanReports {
    /// Records `peer` as reported at `address`. Returns `false`, and records
    /// nothing, when its manufacturer data, service data and name are the
    /// ones last reported there.
    fn record(&mut self, address: bluer::Address, peer: &DiscoveredPeer) -> bool {
        let now = ReportedRecords {
            manufacturer_data: peer.manufacturer_data.clone(),
            service_data: peer.service_data.clone(),
            name: peer.name.clone(),
        };
        if self.0.get(&address) == Some(&now) {
            return false;
        }
        self.0.insert(address, now);
        true
    }
}

impl ScannedPeer {
    /// Whether a device is for `target`: BlueZ lists the target among its
    /// services, or it carries a managed fast advertisement under
    /// `fast_company` (a Windows peer publishes that in an advertisement of
    /// its own, without the service UUID; ADR-0011). The Windows watcher
    /// admits the same devices.
    fn is_target(
        uuids: &HashSet<uuid::Uuid>,
        target: uuid::Uuid,
        fast_company: u16,
        manufacturer_data: &std::collections::BTreeMap<u16, Vec<u8>>,
    ) -> bool {
        uuids.contains(&target) || carries_fast_advertisement(fast_company, manufacturer_data)
    }

    async fn read(
        adapter: &Adapter,
        address: bluer::Address,
        target: uuid::Uuid,
        fast_company: u16,
    ) -> ScannedPeer {
        let Ok(device) = adapter.device(address) else {
            return ScannedPeer::Unknown;
        };
        let uuids = device.uuids().await.ok().flatten().unwrap_or_default();
        // Read before the service check: a fast advertisement admits a
        // device that lists no services.
        let manufacturer_data: std::collections::BTreeMap<u16, Vec<u8>> = device
            .manufacturer_data()
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
            .into_iter()
            .collect();
        if !Self::is_target(&uuids, target, fast_company, &manufacturer_data) {
            if uuids.is_empty() {
                return ScannedPeer::Unknown;
            }
            log::trace!(
                "scan: ignoring {address} — advertises {} service(s), none matching",
                uuids.len()
            );
            return ScannedPeer::NotTarget;
        }
        // BlueZ keeps device objects for addresses it heard in earlier scans and
        // hands them to every new scan; it sets RSSI only once this discovery
        // actually hears the device. Reporting a device without RSSI sent callers
        // to addresses a phone had already rotated away from (Android picks a new
        // private address each time it restarts advertising): 21 of 24 reports in
        // one hardware session, each costing a 20s dial timeout. Treated as not yet
        // known, so the completion watcher reports it once it is really heard.
        let Some(rssi) = device.rssi().await.ok().flatten() else {
            log::debug!("scan: {address} is cached but not heard in this scan yet");
            return ScannedPeer::Unknown;
        };
        let platform_name = device.name().await.ok().flatten();
        let service_data = device
            .service_data()
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
            .into_iter()
            .map(|(uuid, data)| (ServiceUuid(uuid), data))
            .collect();
        let name = advertised_name(ServiceUuid(target), &service_data, platform_name);
        let complete = !manufacturer_data.is_empty();
        let rssi = Some(rssi);
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
}

struct LinuxGattConnection {
    /// The dial this connection belongs to; see `GattEvent::Connected`.
    session: u64,
    /// Current dial per address, so this handle can tell whether it still
    /// owns the link. Without it, a handle kept across a reconnect drives
    /// BlueZ through the address-backed `Device` proxy and disconnects the
    /// connection that replaced it.
    dialed: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    /// See `LinuxDriver::dial_lock`.
    dial_lock: DialLocks,
    /// Peripheral-role sessions currently being served, shared with
    /// `LinuxDriver`. BlueZ keeps one ACL per remote address — disconnecting
    /// it from the central role tears down a peripheral-role session on the
    /// same peer too, since both sides of `GattConnection`/`Driver` address
    /// the same `bluer::Device`. Checked by `disconnect()` before it would
    /// otherwise kill a link the peripheral role still needs.
    served_peers: Arc<StdMutex<HashMap<PeerAddress, u64>>>,
    /// Peripheral-role session admission lock (see `watch_notify_sessions`),
    /// held by `disconnect()` across its `served_peers` check and the platform
    /// call, as `disconnect_peer` does.
    notify_writers: Arc<AsyncMutex<HashMap<CharacteristicUuid, Vec<CharacteristicWriter>>>>,
    /// So `disconnect()` can report the central-role link as lost without
    /// actually tearing down the ACL — see its doc comment.
    events_tx: EventBus,
    peer: PeerAddress,
    device: bluer::Device,
    /// Negotiated ATT MTU, refreshed from BlueZ whenever a characteristic
    /// operation gives us a cheap opportunity to read it. Starts at the
    /// spec-mandated minimum so `max_write_len()` is never optimistic before
    /// the real value is known.
    att_mtu: AtomicU16,
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
impl LinuxGattConnection {
    fn report_loss(
        events_tx: &EventBus,
        dialed: &Arc<StdMutex<HashMap<PeerAddress, u64>>>,
        peer: &PeerAddress,
        generation: u64,
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
}

impl LinuxGattConnection {
    async fn find_characteristic(
        &self,
        characteristic: CharacteristicUuid,
    ) -> Result<bluer::gatt::remote::Characteristic> {
        let services = self
            .device
            .services()
            .await
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        for service in services {
            let chars = service
                .characteristics()
                .await
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            for candidate in chars {
                let uuid = candidate
                    .uuid()
                    .await
                    .map_err(|err| BleError::Gatt(err.to_string()))?;
                if uuid == characteristic.0 {
                    return Ok(candidate);
                }
            }
        }
        Err(BleError::Gatt(format!(
            "characteristic {} not found on peer",
            characteristic.0
        )))
    }
    /// Refuse to act when a newer dial to this peer has superseded us,
    /// matching the Android and mock drivers.
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
        let _dial = self.dial_lock.lock(&self.peer).await;
        self.ensure_current()?;
        let target = self.find_characteristic(characteristic).await?;
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        // A single attempt — no internal retry. See `is_transient_gatt_error`'s
        // doc comment for why: no fixed driver-side retry budget fits every
        // caller, so a caller that wants to retry a `BleError::GattBusy`
        // does so itself, sized to its own timeout.
        match target.read().await {
            Ok(value) => Ok(value),
            Err(err) if BleError::bluez_transient(&err) => {
                log::warn!(
                    "read: {} on {} rejected, possibly transient: {err}",
                    characteristic.0,
                    self.peer.0
                );
                Err(BleError::GattBusy(err.to_string()))
            }
            Err(err) => {
                log::warn!(
                    "read: {} on {} failed: {err}",
                    characteristic.0,
                    self.peer.0
                );
                Err(BleError::Gatt(err.to_string()))
            }
        }
    }

    async fn write_with_type(
        &mut self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
        write_type: WriteType,
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
        let _dial = self.dial_lock.lock(&self.peer).await;
        self.ensure_current()?;
        let target = self.find_characteristic(characteristic).await?;
        // Cache the negotiated MTU opportunistically: BlueZ only publishes a
        // characteristic's MTU once the link is up, so this is the first
        // point it can be observed without a speculative extra round trip.
        if let Ok(mtu) = target.mtu().await {
            self.att_mtu.store(mtu as u16, Ordering::Relaxed);
        }
        log::trace!(
            "write: {} bytes to {} on {} ({write_type:?})",
            value.len(),
            characteristic.0,
            self.peer.0
        );
        match target.write_ext(&value, &request).await {
            Ok(()) => Ok(()),
            Err(err) if BleError::bluez_transient(&err) => {
                log::warn!(
                    "write: {} bytes to {} rejected, possibly transient: {err}",
                    value.len(),
                    self.peer.0
                );
                Err(BleError::GattBusy(err.to_string()))
            }
            Err(err) => {
                log::warn!(
                    "write: {} bytes to {} failed: {err}",
                    value.len(),
                    self.peer.0
                );
                Err(BleError::Gatt(err.to_string()))
            }
        }
    }

    async fn subscribe(
        &mut self,
        characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        let _dial = self.dial_lock.lock(&self.peer).await;
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
        log::info!(
            "subscribe: {} subscribed to {}",
            self.peer.0,
            characteristic.0
        );
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
        log::debug!(
            "disconnect: {} session={} requested",
            self.peer.0,
            self.session
        );
        let _dial = self.dial_lock.lock(&self.peer).await;
        if let Err(err) = self.ensure_current() {
            log::debug!(
                "disconnect: {} session={} not current: {err}",
                self.peer.0,
                self.session
            );
            return Err(err);
        }

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
        //
        // The admission lock is held from this check through the platform
        // call, so a peer cannot be admitted as served in between and then
        // lose its session to the disconnect. BlueZ itself still drops the
        // ACL ~2s after the call, which no lock here can cover.
        let _admission = self.notify_writers.lock().await;
        if self.served_peers.lock().unwrap().contains_key(&self.peer) {
            log::debug!(
                "disconnect: {} is also served here; leaving the shared link up",
                self.peer.0
            );
            LinuxGattConnection::report_loss(
                &self.events_tx,
                &self.dialed,
                &self.peer,
                self.session,
            );
            return Ok(());
        }

        log::debug!(
            "disconnect: {} session={} asking BlueZ to disconnect",
            self.peer.0,
            self.session
        );
        let result = self
            .device
            .disconnect()
            .await
            .map_err(|err| BleError::Gatt(err.to_string()));
        log::debug!(
            "disconnect: {} session={} BlueZ answered: {result:?}",
            self.peer.0,
            self.session
        );
        result
    }
}

#[cfg(test)]
mod tests;

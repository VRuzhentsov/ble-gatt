//! Android driver: raw JNI (`jni` crate) + `ndk-context`, calling directly
//! into `dev.blegatt.BleGattBridge` (a plain Kotlin class shipped in
//! `tauri-plugin-ble-gatt/android/`, *not* a `@TauriPlugin`) — see
//! docs/adr/0002 for why this bypasses Tauri's own mobile-plugin IPC path.
//! No permissively licensed Rust crate does BLE peripheral (GATT server)
//! mode on Android (see the workspace README's library-research table), so
//! this is hand-written against Android's `BluetoothGatt`/
//! `BluetoothGattServer` APIs via the Kotlin bridge class.
//!
//! ## The native-handle contract (read before touching this file)
//!
//! `Inner` is heap-allocated behind an `Arc`. `AndroidDriver::new()` passes
//! `Arc::into_raw(inner.clone()) as jlong` to the `BleGattBridge` Kotlin
//! constructor — a strong reference the bridge owns and that is never
//! reclaimed, so the allocation outlives any callback the JVM may still have
//! queued. See the comment at the call site for why releasing it would be
//! unsound,
//! which stores it and echoes it back on every callback
//! (`Native.kt`/`Java_dev_blegatt_NativeKt_on*`). Every callback
//! reconstructs a borrowed `&Inner` via `&*(native_handle as *const Inner)`
//! — it never takes ownership, so the pointer must stay valid for as long
//! as the Kotlin object might still call back. `AndroidGattConnection`
//! holds its own `Arc<Inner>` clone, keeping the allocation alive even if
//! `AndroidDriver` itself is dropped first while connections are still
//! live. `Drop for AndroidDriver` calls the bridge's `closeAll()`, which
//! disconnects and closes every open `BluetoothGatt` and the GATT server
//! before the allocation can go away. That matters: each open GATT holds a
//! Kotlin callback capturing `native_handle`, so leaving them open meant a
//! later link-state callback could rebuild an `Arc` from freed memory.
//!
//! That retained reference is what makes the contract sound. An earlier
//! revision used `Arc::as_ptr` and documented the resulting use-after-free
//! as an accepted residual risk; it was not acceptable — a callback already
//! queued when the last `Arc` dropped would increment a strong count through
//! a freed allocation. Ownership, not timing, is what closes that window.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use crate::drivers::event_bus::EventBus;
use async_trait::async_trait;
use jni::objects::{
    GlobalRef, JByteArray, JClass, JIntArray, JObject, JObjectArray, JString, JValue,
};
use jni::sys::{jboolean, jint, jlong};
use jni::{JNIEnv, JavaVM};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

/// Scan results buffered before the oldest are dropped. Advertisements are
/// periodic, so dropping one costs at most a short delay before that peer is
/// seen again — whereas an unbounded queue grows for as long as a busy radio
/// environment outpaces the consumer.
const DISCOVERY_QUEUE_DEPTH: usize = 64;

/// Notification payloads buffered per characteristic. Dropping one is real
/// data loss (the datagram layer's reassembly timeout reaps the affected
/// message), but a JVM callback thread cannot block waiting for the
/// consumer, and unbounded growth costs the whole process rather than one
/// message.
const NOTIFY_QUEUE_DEPTH: usize = 256;

/// How long to wait for Android to confirm a disconnect before reporting it
/// as failed. Generous: a real disconnect is fast, and a caller that gets an
/// error here is expected to retry rather than assume the link is gone.
const DISCONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long `connect` waits after this side's GATT client to an address
/// disconnected before dialling it again. Android keeps the radio link up
/// for its 1 s idle timer after the client goes, and a new client in that
/// window reuses the old interface: its service discovery queued behind a
/// command the old one left and never completed (emulator, API 34: 0 of 11
/// first sessions after pairing; with this wait 5 of 5).
const REDIAL_GAP: std::time::Duration = std::time::Duration::from_millis(1500);

/// Locally-assigned code for "an advertise was cancelled by
/// `stop_advertising` before Android reported its result". Above the range
/// Android's `AdvertiseCallback` uses, so it cannot be mistaken for a
/// controller error.
const ADVERTISE_ERROR_STOPPED: i32 = 102;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    advertised_name, AdvertisingMode, CapabilityReport, CharacteristicUuid, ConnectionPriority,
    DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress, RadioStatus, Role, ScanResponse,
    ServiceUuid, WriteType, LEGACY_ADVERTISING_DATA_LEN,
};
use crate::hal::{BoxStream, Driver, GattConnection};

const BRIDGE_CLASS_BINARY_NAME: &str = "dev.blegatt.BleGattBridge";
const CALLBACK_CLASS: &str = "dev/blegatt/NativeKt";

/// Bumped whenever `android.rs` gains a JNI call or callback the Kotlin
/// bridge must implement. `BleGattBridge.kt::bridgeAbiVersion()` returns the
/// matching number; a mismatch is logged at construction (see
/// `AndroidDriver::new`). v2 added `onRadioState` / `bridgeAbiVersion` /
/// `isRadioEnabled` (ADR-0005's adapter-state receiver). v3 added
/// `requestConnectionPriority`. v4 added the local-name byte array to
/// `startAdvertising`. v5 added `setCharacteristicValue` (ADR-0012).
/// v6 changed `startScan` from a boolean to a scan-start status code.
/// v7 added visibility-only `setAdvertisingEnabled`.
/// v8 made the scan response's byte array service data chosen by Rust (the
/// advertisement header or the name) and added the fast advertisement
/// (company, data) and the advertise mode to `startAdvertising` and
/// `updateAdvertising` (ADR-0012 §2).
const BRIDGE_ABI_VERSION: i32 = 8;

const EVENT_CHANNEL_CAPACITY: usize = 64;

/// A live notification route: the subscribe request that created it, where
/// payloads go, and the flag raised when the queue drops one.
type NotifyRoute = (u64, mpsc::Sender<Vec<u8>>, Arc<AtomicBool>);

#[derive(Default)]
struct ConnectionState {
    /// Distinguishes successive connections to this address, so a lifecycle
    /// callback queued from a previous one is recognisable as stale.
    session: u64,
    /// A `BluetoothGatt` for this address is open. Distinct from
    /// `connected_tx`, which only covers a connect still in flight — once
    /// `onConnected` takes that sender, only this flag still says the link
    /// exists.
    live: bool,
    connected_tx: Option<oneshot::Sender<()>>,
    /// Pending read, tagged with the characteristic it was issued for.
    ///
    /// Routing on address alone let a delayed callback from a *cancelled*
    /// read resolve whichever read replaced it — returning the earlier
    /// characteristic's bytes as the later one's result, which is silent
    /// data corruption rather than an error.
    /// Pending read, tagged with the request id it was issued under.
    ///
    /// A request id rather than the characteristic: routing on address alone
    /// let a delayed callback from a cancelled read resolve its replacement,
    /// and the characteristic cannot disambiguate either, since successive
    /// operations routinely target the same one.
    read_tx: Option<(u64, oneshot::Sender<Result<Vec<u8>>>)>,
    /// Pending write, tagged like `read_tx` and for the same reason.
    write_tx: Option<(u64, oneshot::Sender<Result<()>>)>,
    /// Pending subscribes, tagged with the request id that issued them
    /// so a cancelled attempt's callback cannot resolve its retry.
    subscribe_tx: HashMap<CharacteristicUuid, (u64, oneshot::Sender<bool>)>,
    /// Notification routes, tagged with the subscribe request that created
    /// them. The tag is what lets cleanup identify its *own* route: keying
    /// on `subscribe_tx` did not work, because `onSubscribed` removes that
    /// entry before the future returns — so on a rejected subscription the
    /// predicate was already false and the dead route stayed.
    notify_tx: HashMap<CharacteristicUuid, NotifyRoute>,
    disconnected_tx: Option<oneshot::Sender<()>>,
}

/// State of the one scan this driver allows at a time.
#[derive(Default)]
struct ScanState {
    /// Where results go. `None` means no scan is active.
    tx: Option<mpsc::Sender<Result<DiscoveredPeer>>>,
    /// Terminal failure, delivered out-of-band from the result queue: if the
    /// queue is saturated when `onScanFailed` arrives, an error pushed onto
    /// it would be dropped, and a slow consumer would then drain the
    /// buffered successes and see an ordinary end-of-stream — reporting a
    /// truncated peer list as success for a controller failure.
    error: Option<BleError>,
    /// Which scan owns the fields above. A dropped stream must only tear
    /// down *its own* generation: without this, a stream dropped after a
    /// replacement scan had already installed itself would stop the new
    /// scan's platform session and clear its sender, leaving a scan that
    /// returned successfully with an immediately-closed result stream.
    generation: u64,
    /// The service this scan looks for, under whose service data an
    /// Android peer sends its name (`advertised_name`).
    service: Option<ServiceUuid>,
    /// What this scan last reported per address (merged over the address's
    /// advertisements), and when. Android calls
    /// back for every advertisement, several times a second per peer; a
    /// result whose data did not change is passed on only every
    /// `SCAN_REPORT_REFRESH`, so a consumer still sees the peer as present.
    reported: HashMap<String, (ReportedContent, std::time::Instant)>,
}

/// The data of one reported result, without the signal strength.
type ReportedContent = (
    Option<String>,
    Vec<ServiceUuid>,
    BTreeMap<u16, Vec<u8>>,
    BTreeMap<ServiceUuid, Vec<u8>>,
);

/// Most addresses one scan remembers for that, as the Windows driver's
/// `MAX_SCAN_PEERS`.
const SCAN_REPORTED_CAPACITY: usize = 1024;

/// How often an unchanged result is passed on again. Below the 15 s after
/// which a consumer like Fini drops a device it has not heard.
const SCAN_REPORT_REFRESH: std::time::Duration = std::time::Duration::from_secs(5);

mod advertisement_outcome;
mod driver;
use advertisement_outcome::PendingAdvertisement;

/// A spec's advertisement data as `startAdvertising` and
/// `updateAdvertising` take it.
struct AdvertisementArgs<'local> {
    manufacturer_ids: JIntArray<'local>,
    manufacturer_values: JObjectArray<'local>,
    service_data_uuids: JObjectArray<'local>,
    service_data_values: JObjectArray<'local>,
    /// Service data under the served UUID for the scan response: the
    /// caller's (the managed advertisement header) or the local name.
    scan_response_service_data: JObject<'local>,
    /// The fast advertisement's company identifier, -1 for none.
    fast_company_id: i32,
    fast_data: JObject<'local>,
    /// `AdvertiseSettings.ADVERTISE_MODE_*`.
    advertise_mode: i32,
}

impl<'local> AdvertisementArgs<'local> {
    fn new(env: &mut JNIEnv<'local>, service: &GattServiceSpec) -> Result<Self> {
        // Same parallel-array flattening `onPeerDiscovered` uses on the read
        // side (see that callback's own comment): a `java.util.HashMap`
        // across JNI costs several reflective calls per entry, so
        // manufacturer/service advertisement data crosses as an id/UUID
        // array plus a matching value array instead.
        let ids: Vec<i32> = service
            .manufacturer_data
            .keys()
            .map(|id| i32::from(*id))
            .collect();
        let manufacturer_ids = env
            .new_int_array(ids.len() as i32)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        env.set_int_array_region(&manufacturer_ids, 0, &ids)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let manufacturer_values = env
            .new_object_array(ids.len() as i32, "[B", JObject::null())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        for (i, value) in service.manufacturer_data.values().enumerate() {
            let jvalue = env
                .byte_array_from_slice(value)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_object_array_element(&manufacturer_values, i as i32, &jvalue)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
        }

        // The placement (ADR-0012 §2): the advertising packet carries the
        // UUID, the manufacturer data and service data under other UUIDs; the
        // scan response carries the fast advertisement, which Kotlin folds
        // over `manufacturer_data[company]` (`AdvertisePlacement.kt`), or
        // else the service data under the served UUID, or else the name.
        let placement = service.legacy_placement(true);
        if placement.advertising_len() > LEGACY_ADVERTISING_DATA_LEN {
            log::warn!(
                "advertise: the advertising packet needs {} bytes, more than {LEGACY_ADVERTISING_DATA_LEN}; Android will refuse it",
                placement.advertising_len()
            );
        }
        let other_service_data: Vec<_> = service
            .service_data
            .iter()
            .filter(|(uuid, _)| **uuid != service.uuid)
            .collect();
        let service_data_uuids = env
            .new_object_array(
                other_service_data.len() as i32,
                "java/lang/String",
                JObject::null(),
            )
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let service_data_values = env
            .new_object_array(other_service_data.len() as i32, "[B", JObject::null())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        for (i, (uuid, value)) in other_service_data.into_iter().enumerate() {
            let uuid_str = env
                .new_string(uuid.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_object_array_element(&service_data_uuids, i as i32, &uuid_str)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let jvalue = env
                .byte_array_from_slice(value)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_object_array_element(&service_data_values, i as i32, &jvalue)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
        }

        // Android cannot set a name per advertisement, so the name travels
        // as service data under the service UUID in the scan response
        // (`GattServiceSpec::local_name`), unless the caller's service data
        // under it does. Null when there is neither.
        let own = match service.legacy_placement(false).scan_response {
            ScanResponse::ServiceData(data) => Some(data),
            _ => None,
        };
        let scan_response_service_data = match own {
            Some(data) => JObject::from(
                env.byte_array_from_slice(&data)
                    .map_err(|err| BleError::Gatt(err.to_string()))?,
            ),
            None => JObject::null(),
        };
        let (fast_company_id, fast_data) = match &service.fast_advertisement {
            Some(fast) => (
                i32::from(fast.company_id),
                JObject::from(
                    env.byte_array_from_slice(&fast.data)
                        .map_err(|err| BleError::Gatt(err.to_string()))?,
                ),
            ),
            None => (-1, JObject::null()),
        };

        Ok(Self {
            manufacturer_ids,
            manufacturer_values,
            service_data_uuids,
            service_data_values,
            scan_response_service_data,
            fast_company_id,
            fast_data,
            advertise_mode: AdvertiseMode::of(service.advertising_mode),
        })
    }
}

/// `android.bluetooth.le.AdvertiseSettings` modes.
struct AdvertiseMode;

impl AdvertiseMode {
    fn of(mode: AdvertisingMode) -> i32 {
        match mode {
            AdvertisingMode::LowPower => 0,
            AdvertisingMode::Balanced => 1,
            AdvertisingMode::LowLatency => 2,
        }
    }
}

pub struct AndroidDriver {
    inner: Arc<Inner>,
}

impl AndroidDriver {
    /// Addresses currently subscribed to `characteristic` on our server.
    fn subscribed_peers(&self, characteristic: CharacteristicUuid) -> Result<Vec<PeerAddress>> {
        let mut env = self.inner.env()?;
        let uuid = env
            .new_string(characteristic.0.to_string())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let bridge = self.inner.bridge()?;
        let result = env.call_method(
            bridge.as_obj(),
            "subscribedAddresses",
            "(Ljava/lang/String;)[Ljava/lang/String;",
            &[JValue::Object(&uuid)],
        );
        let array = match result {
            Ok(value) => value.l().map_err(|err| BleError::Gatt(err.to_string()))?,
            Err(err) => return Err(BleError::jni_error(&mut env, "subscribedAddresses", err)),
        };
        let array = JObjectArray::from(array);
        let len = env.get_array_length(&array).unwrap_or(0);
        let mut peers = Vec::with_capacity(len as usize);
        for i in 0..len {
            let Ok(item) = env.get_object_array_element(&array, i) else {
                continue;
            };
            peers.push(PeerAddress(JniCodec::read_jstring(
                &mut env,
                &JString::from(item),
            )));
        }
        Ok(peers)
    }
    pub async fn new() -> Result<Self> {
        let ctx = ndk_context::android_context();
        // SAFETY: the Android runtime sets the process JVM in `ndk_context`
        // before native code runs, and that JVM lives as long as the process.
        let vm = unsafe { JavaVM::from_raw(ctx.vm().cast()) }.map_err(|err| {
            BleError::AdapterUnavailable(format!("JavaVM::from_raw failed: {err}"))
        })?;

        // Scoped so this first `env` (which borrows `vm`) is dropped before
        // `vm` moves into `Inner` below; a fresh `env` is re-attached from
        // `inner.vm` afterward (attaching an already-attached thread again
        // is a documented no-op in jni-rs, not a second real attach).
        let context = {
            let env = vm
                .attach_current_thread_as_daemon()
                .map_err(|err| BleError::AdapterUnavailable(format!("JNI attach failed: {err}")))?;
            // SAFETY: `ndk_context` holds a global reference to the
            // application context, which stays valid for the process.
            let context_obj = unsafe { JObject::from_raw(ctx.context().cast()) };
            env.new_global_ref(context_obj).map_err(|err| {
                BleError::AdapterUnavailable(format!("global ref of Context failed: {err}"))
            })?
        };

        Self::with_context(vm, context).await
    }

    /// Uses the embedding application's JVM and owned Application context.
    /// Unlike [`Self::new`], this never reads or initializes `ndk-context`.
    /// The context must belong to `vm`; use the process-lifetime Application,
    /// not an Activity whose lifetime ends on configuration changes.
    pub async fn with_context(vm: JavaVM, context: GlobalRef) -> Result<Self> {
        let server_events_tx = EventBus::new(EVENT_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            vm,
            context,
            bridge: OnceLock::new(),
            connections: StdMutex::new(HashMap::new()),
            scan: StdMutex::new(ScanState::default()),
            server_events_tx,
            att_mtus: StdMutex::new(HashMap::new()),
            client_disconnected_at: StdMutex::new(HashMap::new()),
            advertise_tx: StdMutex::new(None),
            notify_waiters: StdMutex::new(HashMap::new()),
            next_notify_id: AtomicU64::new(1),
            next_op_id: AtomicU64::new(1),
            server_sessions: StdMutex::new(HashMap::new()),
            advertise_generation: AtomicU64::new(1),
            advertise_lock: tokio::sync::Mutex::new(()),
            served_service: StdMutex::new(None),
        });

        // See the module doc comment: this pointer must stay valid for as
        // long as any `Arc<Inner>` handle (this `AndroidDriver`, or any
        // `AndroidGattConnection` cloned from it) is alive.
        // `into_raw`, not `as_ptr`: the Kotlin bridge holds a *strong*
        // reference for as long as it might call back. With `as_ptr` the
        // handle was non-owning, so once the last `Arc<Inner>` dropped, a
        // Binder callback already queued would run `increment_strong_count`
        // on a freed allocation — undefined behaviour, not merely a stale
        // read. `closeAll()` narrows the window by stopping the sources of
        // new callbacks; it cannot retract one already in flight, which is
        // why the window has to be closed by ownership instead.
        //
        // This reference is deliberately never reclaimed. Releasing it would
        // reintroduce exactly the race it exists to remove, since nothing
        // can prove the JVM has no queued callback left. The cost is one
        // `Inner` per driver — in practice one per process — which is a
        // bounded leak traded for the elimination of a use-after-free.
        let native_handle = Arc::into_raw(inner.clone()) as jlong;
        let mut env = inner
            .env()
            .map_err(|err| BleError::AdapterUnavailable(format!("JNI re-attach failed: {err}")))?;
        let bridge_class = AndroidDriver::load_app_class(
            &mut env,
            inner.context.as_obj(),
            BRIDGE_CLASS_BINARY_NAME,
        )
        .map_err(|err| {
            BleError::AdapterUnavailable(format!("failed to load BleGattBridge class: {err}"))
        })?;
        let bridge_obj = env
            .new_object(
                &bridge_class,
                "(Landroid/content/Context;J)V",
                &[
                    JValue::Object(inner.context.as_obj()),
                    JValue::Long(native_handle),
                ],
            )
            .map_err(|err| {
                let detail = BleError::describe_pending_exception(&mut env);
                BleError::AdapterUnavailable(format!(
                    "BleGattBridge construction failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                ))
            })?;
        let bridge_ref = env.new_global_ref(bridge_obj).map_err(|err| {
            BleError::AdapterUnavailable(format!("global ref of bridge failed: {err}"))
        })?;
        inner
            .bridge
            .set(bridge_ref)
            .map_err(|_| BleError::AdapterUnavailable("bridge already initialized".to_string()))?;

        // The Kotlin bridge does not travel with this crate — a consumer that
        // vendors its own copy (fini does) can be running an older
        // `BleGattBridge.kt` than this `android.rs` expects. That failure is
        // otherwise silent: everything compiles, the Rust is new, and a
        // feature like the adapter-state receiver simply never fires because
        // nothing on the Java side calls its JNI entry point. A version
        // mismatch is logged loudly rather than made fatal, since a consumer
        // may legitimately be mid-migration.
        let bridge_abi = inner
            .env()
            .ok()
            .and_then(|mut env| inner.call_int(&mut env, "bridgeAbiVersion", "()I").ok());
        match bridge_abi {
            Some(v) if v == BRIDGE_ABI_VERSION => {}
            Some(v) => log::error!(
                "BleGattBridge.kt ABI v{v} but this ble-gatt expects v{BRIDGE_ABI_VERSION} — \
                 the vendored bridge is out of date; features added since v{v} will not work. \
                 Copy tauri-plugin-ble-gatt/android/src/main/kotlin/dev/blegatt/BleGattBridge.kt \
                 (and Native.kt) from this crate's revision."
            ),
            None => log::warn!(
                "BleGattBridge.kt predates ABI versioning (no bridgeAbiVersion method) — \
                 update the vendored bridge to this crate's revision"
            ),
        }

        Ok(Self { inner })
    }
}

/// `FindClass` — used implicitly by `JNIEnv::new_object` when given a class
/// name string — only searches the bootstrap classloader when called from a
/// thread the JVM did not create itself, which is exactly this thread
/// (attached via `attach_current_thread_as_daemon`, not JVM-spawned). The
/// bootstrap classloader can only find core Android framework classes,
/// never app-defined ones — confirmed directly, not assumed: an earlier
/// build of this file failed with `ClassNotFoundException: Didn't find
/// class "dev.blegatt.BleGattBridge"` at exactly this call site. The
/// standard fix is to resolve the class through the app's own classloader,
/// `Context.getClassLoader()`. Not the classloader of the context's class:
/// the context is the Application, and an app without its own subclass has
/// a plain `android.app.Application`, a framework class.
impl AndroidDriver {
    fn load_app_class<'a>(
        env: &mut JNIEnv<'a>,
        context: &JObject,
        binary_name: &str,
    ) -> Result<JClass<'a>> {
        let class_loader = env
            .call_method(context, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])
            .and_then(jni::objects::JValueGen::l)
            .map_err(|err| BleError::Gatt(format!("getClassLoader failed: {err}")))?;
        let name = env
            .new_string(binary_name)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let class_obj = env
            .call_method(
                &class_loader,
                "loadClass",
                "(Ljava/lang/String;)Ljava/lang/Class;",
                &[JValue::Object(&name)],
            )
            .and_then(jni::objects::JValueGen::l)
            .map_err(|err| BleError::Gatt(format!("loadClass({binary_name}) failed: {err}")))?;
        Ok(JClass::from(class_obj))
    }
}

// Referenced only to keep the `CALLBACK_CLASS` constant documented and
// grep-discoverable alongside `BRIDGE_CLASS`; the JVM resolves the actual
// symbol by the exported function names below, not this constant.
#[allow(dead_code, reason = "only keeps `CALLBACK_CLASS` in use; see above")]
impl AndroidDriver {
    fn _callback_class_name() -> &'static str {
        CALLBACK_CLASS
    }
}

impl Drop for AndroidDriver {
    fn drop(&mut self) {
        // Best-effort quiesce — see the module doc comment's native-handle
        // contract for what this does and does not guarantee.
        if let Ok(mut env) = self.inner.env() {
            // Close every open GATT, not just scan/advertising. Each one
            // holds a Kotlin callback capturing `native_handle`; leaving
            // them open meant a later link-state callback could rebuild an
            // `Arc` from freed memory once the last handle went away.
            let _ = self.inner.call_void(&mut env, "closeAll", "()V", &[]);
        }
    }
}

/// Wraps the discovery channel so dropping the stream (the caller losing
/// interest in scan results) stops the underlying Android scan — mirrors
/// `LinuxDriver::scan`'s stream-owns-the-scan-lifetime contract.
struct ScanStream {
    inner: Arc<Inner>,
    rx: ReceiverStream<Result<DiscoveredPeer>>,
    generation: u64,
}

impl tokio_stream::Stream for ScanStream {
    type Item = Result<DiscoveredPeer>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match std::pin::Pin::new(&mut self.rx).poll_next(cx) {
            // Queue drained. Every buffered result has been delivered, so
            // now surface any terminal failure — taking it so the stream
            // ends on the poll after.
            std::task::Poll::Ready(None) => {
                let error = self.inner.scan.lock().unwrap().error.take();
                std::task::Poll::Ready(error.map(Err))
            }
            other => other,
        }
    }
}

impl Drop for ScanStream {
    fn drop(&mut self) {
        // Take the lock first and keep it across the platform stop, the same
        // way `scan()` holds it across `startScan`. Stopping and clearing as
        // two unlocked steps let a replacement scan install itself in
        // between, after which this drop would stop *its* session and clear
        // *its* sender.
        let mut scan = self.inner.scan.lock().unwrap();
        if scan.generation != self.generation {
            // Superseded: a newer scan owns the platform session now, and
            // tearing it down is not this stream's business.
            return;
        }
        if let Ok(mut env) = self.inner.env() {
            let _ = self.inner.call_void(&mut env, "stopScan", "()V", &[]);
        }
        scan.tx = None;
    }
}

/// Notification stream that reports dropped payloads.
///
/// The flag is checked ahead of the queue for the same reason the datagram
/// layer's is: overflow happens exactly when the queue is full, so an error
/// pushed onto it would be the first thing discarded.
struct NotifyStream {
    rx: ReceiverStream<Vec<u8>>,
    overflow: Arc<AtomicBool>,
}

impl tokio_stream::Stream for NotifyStream {
    type Item = Result<Vec<u8>>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.overflow.swap(false, Ordering::SeqCst) {
            return std::task::Poll::Ready(Some(Err(BleError::notification_receive_gap())));
        }
        std::pin::Pin::new(&mut self.rx)
            .poll_next(cx)
            .map(|item| item.map(Ok))
    }
}

struct AndroidGattConnection {
    inner: Arc<Inner>,
    address: String,
    session: Option<u64>,
}

impl AndroidGattConnection {
    /// Refuse to act if this handle no longer owns the address.
    ///
    /// State here is keyed by address, so a handle kept across an
    /// unsolicited disconnect and reconnect would otherwise operate on the
    /// *replacement* session — installing its waiters into that session's
    /// slots, or telling Kotlin to disconnect a GATT that belongs to
    /// someone else. Every operation checks this first.
    /// Check ownership *and* install this operation's waiter in one
    /// critical section.
    ///
    /// Checking first and assigning afterwards let a reconnect land in
    /// between, so a stale handle overwrote the replacement session's
    /// waiter — Kotlin then rejected the stale operation, that rejection
    /// removed the sender, and the replacement's own callback found no
    /// waiter, leaving its future pending forever.
    fn with_session<T>(&self, install: impl FnOnce(&mut ConnectionState) -> T) -> Result<T> {
        let mut connections = self.inner.connections.lock().unwrap();
        let state = connections
            .get_mut(&self.address)
            .ok_or_else(|| BleError::NotConnected(self.address.clone()))?;
        match self.session {
            // A handle with no session predates session tracking; fall back
            // to address behaviour rather than refusing outright.
            Some(mine) if mine != state.session => {
                Err(BleError::NotConnected(self.address.clone()))
            }
            _ => Ok(install(state)),
        }
    }
}

#[async_trait]
impl GattConnection for AndroidGattConnection {
    fn peer(&self) -> PeerAddress {
        PeerAddress(self.address.clone())
    }

    fn session(&self) -> Option<u64> {
        self.session
    }

    fn att_mtu(&self) -> u16 {
        self.inner
            .att_mtus
            .lock()
            .unwrap()
            .get(&self.address)
            .copied()
            .unwrap_or(crate::hal::DEFAULT_ATT_MTU)
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let request_id = self.inner.next_op_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.with_session(|state| {
            // Refuse rather than replace: a second read while one is in
            // flight would strand the first caller, and the platform has one
            // outstanding read per connection anyway.
            if state.read_tx.is_some() {
                return Err(BleError::Gatt(format!(
                    "a read from {} is already in progress",
                    self.address
                )));
            }
            state.read_tx = Some((request_id, tx));
            Ok(())
        })??;
        // Armed *before* the fallible JNI setup below, not after. Installing
        // the sender and then returning early on a transient failure — a JNI
        // attach, a string allocation, a missing runtime permission — left
        // the stale sender in place, so every later read on a perfectly live
        // connection was refused as already in progress, long after the
        // cause was fixed.
        let guard = PendingOpGuard {
            inner: self.inner.clone(),
            address: self.address.clone(),
            op: PendingOp::Read,
            request_id,
        };
        {
            let mut env = self.inner.env()?;
            let address = env
                .new_string(&self.address)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "readCharacteristic",
                "(Ljava/lang/String;Ljava/lang/String;JJ)V",
                &[
                    JValue::Object(&address),
                    JValue::Object(&uuid),
                    JValue::Long(request_id as i64),
                    JValue::Long(self.session.unwrap_or_default() as i64),
                ],
            )?;
        }
        // The guard covers every exit from here too, including a dropped
        // future, so a cancelled read cannot block later ones for the life
        // of the link.
        let result = rx
            .await
            .map_err(|_| BleError::NotConnected(self.address.clone()));
        drop(guard);
        result?
    }

    async fn write_with_type(
        &mut self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
        write_type: WriteType,
    ) -> Result<()> {
        let request_id = self.inner.next_op_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.with_session(|state| state.write_tx = Some((request_id, tx)))?;
        // Same reasoning as `read`: armed before the fallible setup, so a
        // transient JNI failure cannot strand the slot.
        let guard = PendingOpGuard {
            inner: self.inner.clone(),
            address: self.address.clone(),
            op: PendingOp::Write,
            request_id,
        };
        {
            let mut env = self.inner.env()?;
            let address = env
                .new_string(&self.address)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bytes = env
                .byte_array_from_slice(&value)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let without_response = matches!(write_type, WriteType::WithoutResponse);
            self.inner.call_void(
                &mut env,
                "writeCharacteristic",
                "(Ljava/lang/String;Ljava/lang/String;[BZJJ)V",
                &[
                    JValue::Object(&address),
                    JValue::Object(&uuid),
                    JValue::Object(&bytes),
                    JValue::Bool(jboolean::from(without_response)),
                    JValue::Long(request_id as i64),
                    JValue::Long(self.session.unwrap_or_default() as i64),
                ],
            )?;
        }
        let result = rx
            .await
            .map_err(|_| BleError::NotConnected(self.address.clone()));
        drop(guard);
        result?
    }

    async fn subscribe(
        &mut self,
        characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        let request_id = self.inner.next_op_id.fetch_add(1, Ordering::Relaxed);
        let (confirm_tx, confirm_rx) = oneshot::channel();
        let (notify_tx, notify_rx) = mpsc::channel(NOTIFY_QUEUE_DEPTH);
        let overflow = Arc::new(AtomicBool::new(false));
        self.with_session(|state| {
            state
                .subscribe_tx
                .insert(characteristic, (request_id, confirm_tx));
            state
                .notify_tx
                .insert(characteristic, (request_id, notify_tx, overflow.clone()));
        })?;
        // Every unsuccessful exit — a peer rejecting the subscription, an
        // unknown UUID, a JNI failure, or this future being dropped — must
        // remove both entries. Leaving them behind grows the connection's
        // maps with dead senders, and a failed retry for a UUID that already
        // had a live route would replace it with one whose receiver is gone.
        let subscribe_guard = SubscribeGuard {
            inner: self.inner.clone(),
            address: self.address.clone(),
            characteristic,
            request_id,
            armed: true,
        };
        {
            let mut env = self.inner.env()?;
            let address = env
                .new_string(&self.address)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "subscribeCharacteristic",
                "(Ljava/lang/String;Ljava/lang/String;JJ)V",
                &[
                    JValue::Object(&address),
                    JValue::Object(&uuid),
                    JValue::Long(request_id as i64),
                    JValue::Long(self.session.unwrap_or_default() as i64),
                ],
            )?;
        }
        let subscribed = confirm_rx
            .await
            .map_err(|_| BleError::NotConnected(self.address.clone()))?;
        if !subscribed {
            return Err(BleError::Gatt(format!(
                "peer refused notifications on characteristic {}",
                characteristic.0
            )));
        }
        // Subscribed: the entries are live, so the guard must not remove
        // them.
        let mut subscribe_guard = subscribe_guard;
        subscribe_guard.armed = false;
        Ok(Box::pin(NotifyStream {
            rx: ReceiverStream::new(notify_rx),
            overflow,
        }))
    }

    async fn request_connection_priority(&mut self, priority: ConnectionPriority) -> Result<()> {
        // Same ordering as `BleGattBridge.requestConnectionPriority`'s
        // `priority` argument.
        let code = match priority {
            ConnectionPriority::LowPower => 0,
            ConnectionPriority::Balanced => 1,
            ConnectionPriority::High => 2,
        };
        let outcome = {
            let mut env = self.inner.env()?;
            let address = env
                .new_string(&self.address)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_int_with(
                &mut env,
                "requestConnectionPriority",
                "(Ljava/lang/String;JI)I",
                &[
                    JValue::Object(&address),
                    JValue::Long(self.session.unwrap_or_default() as i64),
                    JValue::Int(code),
                ],
            )?
        };
        // Codes documented on the Kotlin side.
        match outcome {
            0 => Ok(()),
            1 => Err(BleError::NotConnected(self.address.clone())),
            _ => Err(BleError::Gatt(format!(
                "the platform refused the {priority:?} connection priority request for {}",
                self.address
            ))),
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Most important here: a stale handle's `disconnect` would otherwise
        // tear down the connection that replaced it.
        let (tx, rx) = oneshot::channel();
        self.with_session(|state| state.disconnected_tx = Some(tx))?;
        {
            let mut env = self.inner.env()?;
            let address = env
                .new_string(&self.address)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "disconnect",
                "(Ljava/lang/String;J)V",
                &[
                    JValue::Object(&address),
                    JValue::Long(self.session.unwrap_or_default() as i64),
                ],
            )?;
        }
        // Report a disconnect that never completed. Swallowing the timeout
        // told the caller cleanup had succeeded when `ConnectionState.live`
        // and the Kotlin GATT entry were both still set — so the connection
        // handle was dropped as done, while the live-connection guard went on
        // refusing reconnects to that address with nothing left able to close
        // it. An error keeps the handle in the caller's hands to retry.
        match tokio::time::timeout(DISCONNECT_TIMEOUT, rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(BleError::Gatt(format!(
                "disconnect from {} was abandoned before the platform confirmed it",
                self.address
            ))),
            Err(_) => Err(BleError::Gatt(format!(
                "disconnect from {} timed out after {:?}; the platform connection may still \
                 be open",
                self.address, DISCONNECT_TIMEOUT
            ))),
        }
    }
}

/// Turn a failed JNI call into a `BleError`, **describing and clearing any
/// pending Java exception first**.
///
/// This is not just for nicer messages. `attach_current_thread_as_daemon`
/// leaves the Tokio worker attached to the JVM, so an uncleared exception
/// stays pending on that worker and makes every later JNI call scheduled
/// there fail too — long after the original cause (a missing runtime
/// Bluetooth permission, say) has been fixed.
impl BleError {
    fn jni_error(env: &mut JNIEnv, method: &str, err: jni::errors::Error) -> BleError {
        let detail = BleError::describe_pending_exception(env);
        BleError::Gatt(format!(
            "{method} failed: {err}{}",
            detail.map(|d| format!(" ({d})")).unwrap_or_default()
        ))
    }
}

/// jni-rs's `Result::Err` for a JNI call that failed because Java threw
/// just says "Java exception was thrown" — the crate does not surface the
/// exception's own message. This pulls it out (and prints the full stack
/// trace to logcat via `ExceptionDescribe`) before clearing it, since an
/// uncleared pending exception would abort the next JNI call made on this
/// thread.
impl BleError {
    fn describe_pending_exception(env: &mut JNIEnv) -> Option<String> {
        if !env.exception_check().ok()? {
            return None;
        }
        let _ = env.exception_describe();
        let throwable = env.exception_occurred().ok()?;
        let _ = env.exception_clear();
        let message = env
            .call_method(&throwable, "toString", "()Ljava/lang/String;", &[])
            .ok()?
            .l()
            .ok()?;
        let message = JString::from(message);
        Some(JniCodec::read_jstring(env, &message))
    }
}

/// Converts the string and byte-array values at the JNI boundary.
struct JniCodec;

impl JniCodec {
    fn read_jstring(env: &mut JNIEnv, s: &JString) -> String {
        if s.is_null() {
            String::new()
        } else {
            env.get_string(s).map(String::from).unwrap_or_default()
        }
    }

    fn read_optional_jstring(env: &mut JNIEnv, s: &JString) -> Option<String> {
        if s.is_null() {
            None
        } else {
            env.get_string(s).map(String::from).ok()
        }
    }

    /// Reads the parallel key/value arrays `Native.kt` flattens advertisement
    /// maps into — see its doc comment for why the boundary looks like this.
    fn read_byte_array_array(env: &mut JNIEnv, array: &JObjectArray, len: i32) -> Vec<Vec<u8>> {
        (0..len)
            .map(|i| {
                env.get_object_array_element(array, i)
                    .ok()
                    .map(JByteArray::from)
                    .and_then(|bytes| env.convert_byte_array(&bytes).ok())
                    .unwrap_or_default()
            })
            .collect()
    }
}

struct Inner {
    vm: JavaVM,
    context: GlobalRef,
    bridge: OnceLock<GlobalRef>,
    connections: StdMutex<HashMap<String, ConnectionState>>,
    /// Sender and terminal error for the active scan, under **one** lock.
    ///
    /// They cannot be separate mutexes: `scan()` and `onScanFailed` would
    /// acquire them in opposite orders. And the lock is deliberately held
    /// across the `startScan` JNI call, which is what makes the exclusivity
    /// check atomic — see `scan()`.
    scan: StdMutex<ScanState>,
    /// Named `server_events_tx` historically, but now carries both roles'
    /// lifecycle events — including central-side link loss. See
    /// `Driver::events`.
    server_events_tx: EventBus,
    /// Negotiated ATT MTU per peer address, populated from
    /// `onMtuChanged`. Shared rather than held per-`AndroidGattConnection`
    /// because the JNI callback arrives on a Binder thread with only the
    /// address to key on.
    att_mtus: StdMutex<HashMap<String, u16>>,
    /// When this side's GATT client to each address last disconnected; see
    /// `REDIAL_GAP`.
    client_disconnected_at: StdMutex<HashMap<String, std::time::Instant>>,
    /// Resolved by `onAdvertiseResult`. Android decides asynchronously
    /// whether an advertisement actually started, so `advertise()` must wait
    /// for it rather than reporting success the moment the JNI call returns.
    advertise_tx: StdMutex<Option<PendingAdvertisement<GattServiceSpec>>>,
    /// Outstanding server notifications, keyed by request id. Android
    /// reports the real send status on `onNotificationSent`, so `notify`
    /// must wait for it rather than trusting the initiating call.
    notify_waiters: StdMutex<HashMap<u64, oneshot::Sender<bool>>>,
    next_notify_id: AtomicU64,
    /// Request ids for reads and writes, so a completion can be matched to
    /// the operation that issued it rather than to whatever now occupies the
    /// slot.
    next_op_id: AtomicU64,
    /// Session id per central attached to our GATT server, assigned when it
    /// subscribes. Gives server-side peers the same identity outbound
    /// connections have, so a stale caller cannot disconnect the session
    /// that replaced the one it meant.
    server_sessions: StdMutex<HashMap<String, u64>>,
    /// Which advertise attempt is current. A guard for an abandoned attempt
    /// must not tear down the advertisement that replaced it.
    advertise_generation: AtomicU64,
    /// Serialises starting an advertisement against cleaning up an abandoned
    /// one. Reading the generation and then acting on it are two steps, so
    /// without this a retry could increment, install its sender and start
    /// its server in between — and the abandoned attempt's cleanup would
    /// then take the retry's sender and stop the retry's advertisement.
    advertise_lock: tokio::sync::Mutex<()>,
    /// The spec the live server and advertisement were started from, so
    /// `update_advertisement` can tell whether a spec is for the served
    /// service. `None` until an advertise succeeds.
    served_service: StdMutex<Option<GattServiceSpec>>,
}

impl Inner {
    fn env(&self) -> Result<JNIEnv<'_>> {
        self.vm
            .attach_current_thread_as_daemon()
            .map_err(|err| BleError::Gatt(format!("JNI attach failed: {err}")))
    }

    /// Drop a failed connect attempt's slot, and the whole entry if nothing
    /// else is using it, so a retry is not rejected as already in progress.
    fn clear_pending_connect(&self, address: &str) {
        let mut connections = self.connections.lock().unwrap();
        if let Some(state) = connections.get_mut(address) {
            state.connected_tx = None;
            // Also clear `live`: a callback that landed while the attempt was
            // being abandoned would otherwise leave the address marked open
            // with no Rust handle owning it, and every later connect refused
            // as "already open".
            state.live = false;
            if state.disconnected_tx.is_none() && state.notify_tx.is_empty() {
                connections.remove(address);
            }
        }
    }

    fn bridge(&self) -> Result<&GlobalRef> {
        self.bridge
            .get()
            .ok_or_else(|| BleError::Gatt("BleGattBridge not initialized".to_string()))
    }

    /// Callers own the `JNIEnv` (via `self.env()`) and pass it in explicitly
    /// — rather than this method re-attaching internally — so call sites
    /// can scope `env` (and its borrowed JNI locals) to end *before* any
    /// subsequent `.await`. `JNIEnv` is `!Send`; holding one across an
    /// await point would make every `Driver`/`GattConnection` future
    /// non-`Send`, which the trait's `Send` supertrait bound forbids.
    fn call_void(&self, env: &mut JNIEnv, method: &str, sig: &str, args: &[JValue]) -> Result<()> {
        let bridge = self.bridge()?;
        let result = env.call_method(bridge.as_obj(), method, sig, args);
        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(BleError::jni_error(env, method, err)),
        }
    }

    fn call_bool(&self, env: &mut JNIEnv, method: &str, sig: &str) -> Result<bool> {
        let bridge = self.bridge()?;
        let result = env
            .call_method(bridge.as_obj(), method, sig, &[])
            .and_then(jni::objects::JValueGen::z);
        match result {
            Ok(value) => Ok(value),
            Err(err) => Err(BleError::jni_error(env, method, err)),
        }
    }

    fn call_int(&self, env: &mut JNIEnv, method: &str, sig: &str) -> Result<i32> {
        self.call_int_with(env, method, sig, &[])
    }

    fn call_int_with(
        &self,
        env: &mut JNIEnv,
        method: &str,
        sig: &str,
        args: &[JValue],
    ) -> Result<i32> {
        let bridge = self.bridge()?;
        let result = env
            .call_method(bridge.as_obj(), method, sig, args)
            .and_then(jni::objects::JValueGen::i);
        match result {
            Ok(value) => Ok(value),
            Err(err) => Err(BleError::jni_error(env, method, err)),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the parameters of the Kotlin `onPeerDiscovered` callback, one for one"
    )]
    fn peer_discovered<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        generation: jlong,
        advertised_service_uuids: JObjectArray<'local>,
        address: JString<'local>,
        name: JString<'local>,
        rssi: jint,
        manufacturer_ids: JIntArray<'local>,
        manufacturer_values: JObjectArray<'local>,
        service_data_uuids: JObjectArray<'local>,
        service_data_values: JObjectArray<'local>,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        let name = JniCodec::read_optional_jstring(&mut env, &name);

        let mut manufacturer_data = BTreeMap::new();
        if let Ok(len) = env.get_array_length(&manufacturer_ids) {
            let mut ids = vec![0i32; len as usize];
            if env
                .get_int_array_region(&manufacturer_ids, 0, &mut ids)
                .is_ok()
            {
                let values = JniCodec::read_byte_array_array(&mut env, &manufacturer_values, len);
                for (id, value) in ids.into_iter().zip(values) {
                    manufacturer_data.insert(id as u16, value);
                }
            }
        }

        let mut service_data = BTreeMap::new();
        if let Ok(len) = env.get_array_length(&service_data_uuids) {
            let values = JniCodec::read_byte_array_array(&mut env, &service_data_values, len);
            for (i, value) in values.into_iter().enumerate() {
                let Ok(uuid_obj) = env.get_object_array_element(&service_data_uuids, i as i32)
                else {
                    continue;
                };
                let uuid_str = JniCodec::read_jstring(&mut env, &JString::from(uuid_obj));
                if let Ok(uuid) = Uuid::parse_str(&uuid_str) {
                    service_data.insert(ServiceUuid(uuid), value);
                }
            }
        }

        // Android already parsed these to match the scan filter, so reporting
        // them costs nothing — and an empty list made a `DiscoveredPeer` from
        // this driver claim no advertised services at all, unlike Linux and the
        // mock, breaking consumers that inspect the field.
        let mut services = Vec::new();
        if let Ok(len) = env.get_array_length(&advertised_service_uuids) {
            for i in 0..len {
                let Ok(item) = env.get_object_array_element(&advertised_service_uuids, i) else {
                    continue;
                };
                if let Ok(uuid) =
                    Uuid::parse_str(&JniCodec::read_jstring(&mut env, &JString::from(item)))
                {
                    services.push(ServiceUuid(uuid));
                }
            }
        }

        let mut scan = inner.scan.lock().unwrap();
        // Discard results belonging to a scan that has already been replaced.
        // A callback can already be executing when its scan is stopped, and
        // without this it would be delivered as a *later* scan's discovery —
        // reporting a peer that never matched the new scan's service filter.
        if scan.generation != generation as u64 {
            log::trace!(
            "jni onPeerDiscovered: ignoring result from superseded scan generation {generation}"
        );
            return;
        }
        if scan.tx.is_some() {
            // Android reports each advertisement of an address on its own. A
            // Windows peer sends two (its GATT provider's, with the service
            // UUID, and its fast advertisement), so merge what one address
            // advertised during this scan, as the Windows driver does.
            let (mut services, manufacturer_data, service_data, name) =
                match scan.reported.get(&address) {
                    Some(((last_name, last_services, last_manufacturer, last_service), _)) => {
                        let mut merged_services = last_services.clone();
                        for uuid in services {
                            if !merged_services.contains(&uuid) {
                                merged_services.push(uuid);
                            }
                        }
                        let mut merged_manufacturer = last_manufacturer.clone();
                        merged_manufacturer.extend(manufacturer_data);
                        let mut merged_service = last_service.clone();
                        merged_service.extend(service_data);
                        (
                            merged_services,
                            merged_manufacturer,
                            merged_service,
                            name.or_else(|| last_name.clone()),
                        )
                    }
                    None => (services, manufacturer_data, service_data, name),
                };
            let name = match scan.service {
                Some(service) => advertised_name(service, &service_data, name),
                None => name,
            };
            services.sort();
            let content: ReportedContent = (
                name.clone(),
                services.clone(),
                manufacturer_data.clone(),
                service_data.clone(),
            );
            let now = std::time::Instant::now();
            match scan.reported.get(&address) {
                Some((last, at))
                    if *last == content && now.duration_since(*at) < SCAN_REPORT_REFRESH =>
                {
                    return;
                }
                None if scan.reported.len() >= SCAN_REPORTED_CAPACITY => {
                    // Full: report without remembering, as before.
                }
                _ => {
                    scan.reported.insert(address.clone(), (content, now));
                }
            }
            let peer = DiscoveredPeer {
                address: PeerAddress(address),
                name,
                services,
                manufacturer_data,
                service_data,
                rssi: Some(rssi as i16),
            };
            log::debug!("jni onPeerDiscovered: {} rssi={rssi}", peer.address.0);
            // try_send on a bounded queue: this runs on a JVM callback thread
            // that must not block, and in a dense advertising environment the
            // radio can outpace any consumer indefinitely.
            let address = peer.address.0.clone();
            let tx = scan.tx.as_ref().expect("checked above");
            if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(Ok(peer)) {
                // Not reported, so not remembered either.
                scan.reported.remove(&address);
                log::warn!(
                    "jni onPeerDiscovered: discovery queue full, dropping scan result; \
                 the peer will reappear on its next advertisement"
                );
            }
        }
    }
    fn advertise_result<'local>(
        self: Arc<Self>,
        _env: JNIEnv<'local>,
        success: jboolean,
        error_code: jint,
    ) {
        let inner = self;
        if success != 0 {
            log::info!("jni onAdvertiseResult: advertising started");
        } else {
            log::error!("jni onAdvertiseResult: advertising failed, error_code={error_code}");
        }
        let pending = inner.advertise_tx.lock().unwrap().take();
        if let Some(pending) = pending {
            pending.complete(
                if success != 0 {
                    Ok(())
                } else {
                    Err(error_code)
                },
                &inner.served_service,
            );
        } else {
            log::warn!("jni onAdvertiseResult: no pending advertise request to complete");
        }
    }
    fn scan_failed<'local>(
        self: Arc<Self>,
        _env: JNIEnv<'local>,
        generation: jlong,
        error_code: jint,
    ) {
        let inner = self;
        // Deliver the failure *as a stream item* before closing, then drop the
        // sender to end the stream. Closing alone would make a scan that never
        // started indistinguishable from a scan that found nothing — the caller
        // would report "no peers" when the real answer is a permission denial or
        // a powered-off adapter.
        // Recorded in a dedicated slot rather than pushed onto the result queue:
        // a saturated queue would swallow the error, and the consumer would then
        // drain the buffered successes and see an ordinary end-of-stream.
        let mut scan = inner.scan.lock().unwrap();
        // A stale failure must not terminate the scan that replaced it.
        if scan.generation != generation as u64 {
            log::trace!(
                "jni onScanFailed: ignoring failure from superseded generation {generation}"
            );
            return;
        }
        log::error!("jni onScanFailed: ScanCallback error code {error_code}");
        scan.error = Some(BleError::Gatt(format!(
            "Android scan failed (ScanCallback error code {error_code})"
        )));
        // Dropping the sender ends the queue; `ScanStream` yields the recorded
        // error as its final item.
        scan.tx.take();
    }
    fn mtu_changed<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        address: JString<'local>,
        mtu: jint,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        log::info!("jni onMtuChanged: {address} negotiated ATT MTU {mtu}");
        inner.att_mtus.lock().unwrap().insert(address, mtu as u16);
    }
    fn connected<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        address: JString<'local>,
        from_server: jboolean,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        log::info!(
            "jni onConnected: {address} from_server={}",
            from_server != 0
        );
        // Only the *client* path may resolve a pending outbound connect. A
        // server-side callback for the same address would otherwise complete it
        // before client-side service discovery had run, handing back a
        // connection with no characteristics.
        if from_server == 0 {
            let mut connections = inner.connections.lock().unwrap();
            let state = connections.entry(address.clone()).or_default();
            state.live = true;
            if let Some(tx) = state.connected_tx.take() {
                let _ = tx.send(());
            }
        }
        // Publish regardless of whether a client connect was pending: Linux
        // already does this, and `Driver::events` documents both roles.
        //
        // Deliberately NOT emitting a peripheral-role `Connected` for
        // `from_server`. A central is connected long before it writes the CCCD,
        // and a server that greeted it at this point would notify with no
        // subscriber. The peripheral-role announcement comes from
        // `onServerSubscribed` instead.
        if from_server == 0 {
            let session = inner
                .connections
                .lock()
                .unwrap()
                .get(&address)
                .map(|state| state.session);
            let _ = inner.server_events_tx.send(GattEvent::Connected {
                peer: PeerAddress(address),
                local_role: Role::Central,
                session,
            });
        }
    }
    fn disconnected<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        address: JString<'local>,
        from_server: jboolean,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        log::info!(
            "jni onDisconnected: {address} from_server={}",
            from_server != 0
        );
        let disconnected_session = if from_server != 0 {
            // Server-side peers carry the session minted when they subscribed.
            let session = inner.server_sessions.lock().unwrap().remove(&address);
            // The MTU belongs to the link, which both roles share. Kept while
            // our own outbound connection to this address is still live;
            // otherwise a later session from the address could inherit a larger
            // MTU it never negotiated and get oversized fragments.
            let client_live = inner
                .connections
                .lock()
                .unwrap()
                .get(&address)
                .is_some_and(|state| state.live);
            if !client_live {
                inner.att_mtus.lock().unwrap().remove(&address);
            }
            session
        } else {
            inner
                .connections
                .lock()
                .unwrap()
                .get(&address)
                .map(|state| state.session)
        };
        // Likewise: a central leaving our GATT server says nothing about an
        // outbound connection we hold to that same address. Tearing down the
        // client state here would kill reads, writes and subscriptions on a
        // still-live link.
        if from_server == 0 {
            // Same rule from the other side: a central still attached to our
            // server is on the same link and keeps its MTU.
            if !inner.server_sessions.lock().unwrap().contains_key(&address) {
                inner.att_mtus.lock().unwrap().remove(&address);
            }
            inner
                .client_disconnected_at
                .lock()
                .unwrap()
                .insert(address.clone(), std::time::Instant::now());
            let mut connections = inner.connections.lock().unwrap();
            if let Some(mut state) = connections.remove(&address) {
                state.live = false;
                if let Some(tx) = state.disconnected_tx {
                    let _ = tx.send(());
                }
                // A connect attempt that never reaches `onConnected` ends in
                // `onDisconnected` instead — resolving `connected_tx` here (with no
                // receiver-visible success value) makes the pending `connect()`
                // call fail via the dropped-sender path in `Driver::connect`.
            }
        }
        // Publish regardless of whether we had per-connection state: this fires
        // for unsolicited drops (peer out of range, powered off) as well as
        // disconnects we asked for, and `Driver::events()` is the only way a
        // caller learns about the former.
        let _ = inner.server_events_tx.send(GattEvent::Disconnected {
            peer: PeerAddress(address),
            local_role: if from_server != 0 {
                Role::Peripheral
            } else {
                Role::Central
            },
            // The session this disconnect belongs to, so a consumer can ignore
            // a loss event queued from a connection that has been replaced.
            session: disconnected_session,
        });
    }
    fn radio_state_changed<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        status: JString<'local>,
    ) {
        let inner = self;
        let status = JniCodec::read_jstring(&mut env, &status);
        log::info!("jni onRadioState: {status}");
        let radio_status = match status.as_str() {
            "on" => RadioStatus::On,
            "unsupported" => RadioStatus::Unsupported,
            _ => RadioStatus::Off,
        };
        if !matches!(radio_status, RadioStatus::On) {
            let stale: Vec<(String, ConnectionState)> =
                inner.connections.lock().unwrap().drain().collect();
            for (address, mut state) in stale {
                state.live = false;
                inner.att_mtus.lock().unwrap().remove(&address);
                let session = state.session;
                if let Some(tx) = state.disconnected_tx.take() {
                    let _ = tx.send(());
                }
                let _ = inner.server_events_tx.send(GattEvent::Disconnected {
                    peer: PeerAddress(address),
                    local_role: Role::Central,
                    session: Some(session),
                });
            }
            inner.server_sessions.lock().unwrap().clear();
            // Kotlin's radio receiver stopped advertising, so a start result
            // still awaited is ignored when it comes, as after
            // `stop_advertising`: end the wait here, or `update_advertisement`
            // keeps `advertise_lock` and blocks every later advertise.
            if let Some(pending) = inner.advertise_tx.lock().unwrap().take() {
                let _ = pending.send(Err(ADVERTISE_ERROR_STOPPED));
            }
        }
        let _ = inner.server_events_tx.send(GattEvent::RadioChanged {
            status: radio_status,
        });
    }
    fn characteristic_read<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        request_id: jlong,
        address: JString<'local>,
        _characteristic_uuid: JString<'local>,
        value: JByteArray<'local>,
        success: jboolean,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        let bytes = env.convert_byte_array(&value).unwrap_or_default();
        let mut connections = inner.connections.lock().unwrap();
        if let Some(state) = connections.get_mut(&address) {
            // Only resolve the read this callback belongs to. A delayed callback
            // from a cancelled read would otherwise hand the waiting caller
            // another operation's bytes — a wrong answer rather than a failure,
            // which is the harder kind to notice.
            if state.read_tx.as_ref().map(|(id, _)| *id) != Some(request_id as u64) {
                return;
            }
            if let Some((_, tx)) = state.read_tx.take() {
                let result = if success != 0 {
                    Ok(bytes)
                } else {
                    Err(BleError::Gatt("characteristic read failed".to_string()))
                };
                let _ = tx.send(result);
            }
        }
    }
    fn characteristic_written<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        request_id: jlong,
        address: JString<'local>,
        _characteristic_uuid: JString<'local>,
        success: jboolean,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        let mut connections = inner.connections.lock().unwrap();
        if let Some(state) = connections.get_mut(&address) {
            // Same tagging as reads. Untagged, a cancelled write's completion
            // resolved the *next* write — reporting the old operation's status
            // and letting the following datagram fragment start before the real
            // write had finished.
            if state.write_tx.as_ref().map(|(id, _)| *id) != Some(request_id as u64) {
                return;
            }
            if let Some((_, tx)) = state.write_tx.take() {
                let result = if success != 0 {
                    Ok(())
                } else {
                    Err(BleError::Gatt("characteristic write failed".to_string()))
                };
                let _ = tx.send(result);
            }
        }
    }
    fn characteristic_changed<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        address: JString<'local>,
        characteristic_uuid: JString<'local>,
        value: JByteArray<'local>,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        let characteristic_uuid = JniCodec::read_jstring(&mut env, &characteristic_uuid);
        let bytes = env.convert_byte_array(&value).unwrap_or_default();
        let Ok(uuid) = Uuid::parse_str(&characteristic_uuid) else {
            return;
        };
        let connections = inner.connections.lock().unwrap();
        if let Some(state) = connections.get(&address) {
            if let Some((_, tx, overflow)) = state.notify_tx.get(&CharacteristicUuid(uuid)) {
                // JVM callback thread: cannot block, so a saturated consumer
                // costs the payload rather than unbounded heap growth. But the
                // peer has already had this notification confirmed as sent, so
                // the drop must be *reported* — otherwise a single-fragment
                // message vanishes and a fragmented one merely expires, with
                // neither endpoint ever learning why.
                log::trace!(
                    "jni onCharacteristicChanged: {} bytes from {address} on {uuid}",
                    bytes.len()
                );
                if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(bytes) {
                    overflow.store(true, Ordering::SeqCst);
                    log::warn!(
                        "jni onCharacteristicChanged: notification queue full for {address}, \
                     dropping payload and reporting the gap to the subscriber"
                    );
                }
            } else {
                log::debug!(
                    "jni onCharacteristicChanged: no subscriber for {uuid} on {address}, \
                 discarding {} bytes",
                    bytes.len()
                );
            }
        } else {
            log::debug!("jni onCharacteristicChanged: no connection state for {address}");
        }
    }
    fn server_characteristic_written<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        address: JString<'local>,
        characteristic_uuid: JString<'local>,
        value: JByteArray<'local>,
        session: jlong,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        let characteristic_uuid = JniCodec::read_jstring(&mut env, &characteristic_uuid);
        let bytes = env.convert_byte_array(&value).unwrap_or_default();
        let Ok(uuid) = Uuid::parse_str(&characteristic_uuid) else {
            return;
        };
        // Kept as a safety net for stacks where `onConnectionStateChange` on the
        // server callback does not fire; `serve` treats `Connected` as
        // idempotent, so a duplicate here is harmless. Always the peripheral
        // role — this is a write arriving at *our* GATT server.
        // Kotlin supplies the session, minting one if this write is the first
        // sign of the peer. A central that writes before enabling its CCCD
        // arrives here first, and `serve` records *this* event — the later
        // `onServerSubscribed` is discarded as a duplicate. Emitting `None` here
        // therefore left the channel sessionless for its whole life, so its
        // sends fell back to targeting whichever session owned the address.
        let session = session as u64;
        log::debug!(
        "jni onServerCharacteristicWritten: {} bytes from {address} on {uuid} session={session}",
        bytes.len()
    );
        inner
            .server_sessions
            .lock()
            .unwrap()
            .insert(address.clone(), session);
        let _ = inner.server_events_tx.send(GattEvent::Connected {
            peer: PeerAddress(address.clone()),
            local_role: Role::Peripheral,
            session: Some(session),
        });
        let _ = inner
            .server_events_tx
            .send(GattEvent::CharacteristicWritten {
                peer: PeerAddress(address),
                characteristic: CharacteristicUuid(uuid),
                value: bytes,
                session: Some(session),
            });
    }
    fn subscribed<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        request_id: jlong,
        address: JString<'local>,
        characteristic_uuid: JString<'local>,
        success: jboolean,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        let characteristic_uuid = JniCodec::read_jstring(&mut env, &characteristic_uuid);
        let Ok(uuid) = Uuid::parse_str(&characteristic_uuid) else {
            return;
        };
        let mut connections = inner.connections.lock().unwrap();
        if let Some(state) = connections.get_mut(&address) {
            // Resolve only the attempt this callback belongs to.
            if state
                .subscribe_tx
                .get(&CharacteristicUuid(uuid))
                .map(|(id, _)| *id)
                != Some(request_id as u64)
            {
                return;
            }
            if let Some((_, tx)) = state.subscribe_tx.remove(&CharacteristicUuid(uuid)) {
                if success != 0 {
                    log::info!("jni onSubscribed: {address} subscribed to {uuid}");
                } else {
                    log::warn!("jni onSubscribed: {address} failed to subscribe to {uuid}");
                }
                let _ = tx.send(success != 0);
            }
        }
    }
    fn server_subscribed<'local>(
        self: Arc<Self>,
        mut env: JNIEnv<'local>,
        address: JString<'local>,
        session: jlong,
    ) {
        let inner = self;
        let address = JniCodec::read_jstring(&mut env, &address);
        // Kotlin mints these, so validating a session and acting on it can be
        // one transition under its monitor — Rust cannot hold its own map across
        // a JNI call without risking a cycle. Recorded here so `serve` and the
        // event stream can see it.
        let session = session as u64;
        inner
            .server_sessions
            .lock()
            .unwrap()
            .insert(address.clone(), session);
        let _ = inner.server_events_tx.send(GattEvent::Connected {
            peer: PeerAddress(address),
            local_role: Role::Peripheral,
            session: Some(session),
        });
    }

    fn notification_sent<'local>(
        self: Arc<Self>,
        _env: JNIEnv<'local>,
        request_id: jlong,
        success: jboolean,
    ) {
        let inner = self;
        let waiter = inner
            .notify_waiters
            .lock()
            .unwrap()
            .remove(&(request_id as u64));
        if let Some(tx) = waiter {
            let _ = tx.send(success != 0);
        }
    }
}

/// # Safety contract
/// See the module doc comment: `native_handle` must be a live `Arc<Inner>`
/// pointer. Never called by Rust directly — only by the JVM, from whatever
/// thread owns the Android Bluetooth callback (usually a Binder thread).
impl Inner {
    unsafe fn from_handle(native_handle: jlong) -> Arc<Self> {
        let ptr = native_handle as *const Inner;
        Arc::increment_strong_count(ptr);
        Arc::from_raw(ptr)
    }
}

#[derive(Clone, Copy)]
enum PendingOp {
    Read,
    Write,
}

/// Releases a pending operation slot on every exit path, including a dropped
/// future and a synchronous JNI failure.
///
/// Must be armed *before* the fallible setup it protects: a slot left
/// occupied by a transient failure blocks every later operation on a live
/// connection, long after the cause is gone.
struct PendingOpGuard {
    inner: Arc<Inner>,
    address: String,
    op: PendingOp,
    /// Which attempt this guard belongs to. Compared against the slot's own
    /// id on drop -- see the Drop impl's own comment for why.
    request_id: u64,
}

impl Drop for PendingOpGuard {
    fn drop(&mut self) {
        // A P1 review finding: a request abandoned here (a `timeout`, a
        // losing `select!` branch) while `attemptWrite`/`attemptRead` had a
        // GATT-busy retry scheduled left that retry with no way to learn
        // this side gave up. It would fire anyway once the stack stopped
        // being busy, transmit the abandoned payload over the air, and its
        // eventual completion callback -- Kotlin matches these by FIFO
        // position, not by id -- would then resolve whichever *different*
        // request happens to be next in the queue. `still_pending` is only
        // true when nothing has consumed this slot yet, i.e. exactly the
        // abandoned case: on a normal completion, `onCharacteristicRead`/
        // `onCharacteristicWriteResult` already took it via `.take()`
        // before this ever runs, so the common path pays no extra JNI call.
        let still_pending = {
            let mut connections = self.inner.connections.lock().unwrap();
            let Some(state) = connections.get_mut(&self.address) else {
                return;
            };
            let still_pending = match self.op {
                PendingOp::Read => {
                    state.read_tx.as_ref().map(|(id, _)| *id) == Some(self.request_id)
                }
                PendingOp::Write => {
                    state.write_tx.as_ref().map(|(id, _)| *id) == Some(self.request_id)
                }
            };
            // A P1 review finding: clearing unconditionally here reset a
            // *replacement* request's slot too whenever this guard outlived
            // a disconnect-and-reconnect to the same address (`connections`
            // is keyed on address, not session, so a new connection's
            // read_tx/write_tx lives in the very same `State`) -- silently
            // failing or losing the callback for a live operation that had
            // nothing to do with this abandoned one. Only clear when the
            // slot is still actually this guard's own, exactly the
            // condition `still_pending` already establishes.
            if still_pending {
                match self.op {
                    PendingOp::Read => state.read_tx = None,
                    PendingOp::Write => state.write_tx = None,
                }
            }
            still_pending
        };
        // The lock above is dropped before this: `cancelPendingOperation`
        // takes Kotlin's own `gattLock`, and a blocking JNI call is not
        // something to make while still holding a Rust mutex.
        if still_pending {
            if let Ok(mut env) = self.inner.env() {
                let _ = self.inner.call_void(
                    &mut env,
                    "cancelPendingOperation",
                    "(J)V",
                    &[JValue::Long(self.request_id as i64)],
                );
            }
        }
    }
}

/// Removes a subscription's waiter and notification route unless the
/// subscribe succeeded.
///
/// `onSubscribed` only removes `subscribe_tx`, so without this a rejected,
/// failed or abandoned attempt left `notify_tx` holding a sender whose
/// receiver had been dropped — accumulating per distinct UUID until the
/// connection closed, and clobbering a live route when a retry for an
/// existing UUID failed.
struct SubscribeGuard {
    inner: Arc<Inner>,
    address: String,
    characteristic: CharacteristicUuid,
    request_id: u64,
    armed: bool,
}

impl Drop for SubscribeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Same reasoning as `PendingOpGuard`'s Drop impl: a subscribe
        // abandoned while `attemptSubscribe` had a GATT-busy retry scheduled
        // needs that retry cancelled, or it fires anyway and its completion
        // callback resolves whichever different request is next in the
        // queue. `still_pending` (the *subscribe* slot specifically --
        // `notify_tx` is the separate route this guard also unwinds, not
        // what `attemptSubscribe`'s retry checks) is only true when nothing
        // has consumed it yet, so a normal completion pays no extra call.
        let still_pending = {
            let mut connections = self.inner.connections.lock().unwrap();
            let Some(state) = connections.get_mut(&self.address) else {
                return;
            };
            // Keyed on the *notification route's* own id, not on
            // `subscribe_tx`: a rejected subscription has already had its
            // `subscribe_tx` removed by `onSubscribed`, so predicating on
            // that entry meant the dead route was never cleaned up — the
            // exact leak this guard exists to prevent. Both entries are
            // still conditional on ownership so a retry that has claimed
            // them is not stranded.
            let still_pending = state
                .subscribe_tx
                .get(&self.characteristic)
                .is_some_and(|(id, _)| *id == self.request_id);
            if still_pending {
                state.subscribe_tx.remove(&self.characteristic);
            }
            if state
                .notify_tx
                .get(&self.characteristic)
                .is_some_and(|(id, _, _)| *id == self.request_id)
            {
                state.notify_tx.remove(&self.characteristic);
            }
            still_pending
        };
        if still_pending {
            if let Ok(mut env) = self.inner.env() {
                let _ = self.inner.call_void(
                    &mut env,
                    "cancelPendingOperation",
                    "(J)V",
                    &[JValue::Long(self.request_id as i64)],
                );
            }
        }
    }
}

/// Tears down an `advertise()` that was abandoned before its result was
/// observed.
///
/// `Drop` cannot await, so the teardown is handed to a short detached task.
/// Leaving the attempt running would strand a GATT server that no caller
/// holds — and unlike a stranded connection, that also blocks every later
/// advertise on the same bridge.
struct AdvertiseGuard {
    inner: Arc<Inner>,
    /// The attempt this guard belongs to. Without it the detached cleanup
    /// task acts on whichever advertisement is current when it happens to be
    /// scheduled — tearing down the retry that replaced the abandoned one.
    generation: u64,
    armed: bool,
}

impl Drop for AdvertiseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = self.inner.clone();
        let generation = self.generation;
        tokio::spawn(async move {
            // Serialised against `advertise`, so the check and the teardown
            // are one transition: a retry cannot slip between reading the
            // generation and acting on it, which would otherwise have this
            // task take the retry's sender and stop the retry's server.
            let _serialise = inner.advertise_lock.lock().await;
            // Only if no retry has superseded this attempt. `stopAdvertising`
            // is addressless, so acting unconditionally would stop whatever
            // is advertising now.
            //
            // Skipping cleanup here does not leak the abandoned attempt:
            // `startAdvertising` tears down any predecessor before opening
            // its own server, so the retry that superseded this one has
            // already closed it.
            if inner.advertise_generation.load(Ordering::SeqCst) != generation {
                return;
            }
            if let Some(pending) = inner.advertise_tx.lock().unwrap().take() {
                let _ = pending.send(Err(ADVERTISE_ERROR_STOPPED));
            }
            if let Ok(mut env) = inner.env() {
                let _ = inner.call_void(&mut env, "stopAdvertising", "()V", &[]);
            }
        });
    }
}

/// Undoes a `connect()` that never completed, including one abandoned by a
/// dropped future. Closing the platform attempt matters as much as clearing
/// the slot: an abandoned `BluetoothGatt` left open would keep the address
/// occupied on the Kotlin side.
struct ConnectGuard {
    inner: Arc<Inner>,
    address: String,
    armed: bool,
}

impl Drop for ConnectGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Close *before* releasing ownership, not after.
        //
        // Both this map and the Kotlin bridge key connections by address
        // alone, so the ordering is what makes the cleanup unambiguous: while
        // the pending slot is still set, `connect` rejects any retry for this
        // address, so no replacement GATT can exist yet and
        // `closeConnection` can only be closing the attempt this guard owns.
        // Clearing first opened a window where a retry installs its own GATT
        // under the same key and this close destroys *that* one instead,
        // leaving the retry failed or hung.
        if let Ok(mut env) = self.inner.env() {
            if let Ok(address) = env.new_string(&self.address) {
                let _ = self.inner.call_void(
                    &mut env,
                    "closeConnection",
                    "(Ljava/lang/String;)V",
                    &[JValue::Object(&address)],
                );
            }
        }
        self.inner.clear_pending_connect(&self.address);
    }
}

// ---------------------------------------------------------------------
// Native callbacks — implemented here, declared as `external fun` in
// Native.kt. Every signature must match Native.kt exactly: Kotlin
// compiles those top-level functions as static methods on `NativeKt`,
// giving the fixed JNI symbol name `Java_dev_blegatt_NativeKt_<name>`.
// ---------------------------------------------------------------------
#[no_mangle]
#[allow(
    clippy::too_many_arguments,
    reason = "the JNI signature of the Kotlin `onPeerDiscovered` external fun"
)]
pub extern "system" fn Java_dev_blegatt_NativeKt_onPeerDiscovered<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    generation: jlong,
    advertised_service_uuids: JObjectArray<'local>,
    address: JString<'local>,
    name: JString<'local>,
    rssi: jint,
    manufacturer_ids: JIntArray<'local>,
    manufacturer_values: JObjectArray<'local>,
    service_data_uuids: JObjectArray<'local>,
    service_data_values: JObjectArray<'local>,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.peer_discovered(
        env,
        generation,
        advertised_service_uuids,
        address,
        name,
        rssi,
        manufacturer_ids,
        manufacturer_values,
        service_data_uuids,
        service_data_values,
    );
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onAdvertiseResult<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    success: jboolean,
    error_code: jint,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.advertise_result(_env, success, error_code);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onScanFailed<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    generation: jlong,
    error_code: jint,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.scan_failed(_env, generation, error_code);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onMtuChanged<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    address: JString<'local>,
    mtu: jint,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.mtu_changed(env, address, mtu);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onConnected<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    address: JString<'local>,
    from_server: jboolean,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.connected(env, address, from_server);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onDisconnected<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    address: JString<'local>,
    from_server: jboolean,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.disconnected(env, address, from_server);
}

/// The local adapter's state changed. `status` is `"on"` / `"off"` /
/// `"unsupported"`. Android does not fire a GATT disconnect callback for an
/// adapter toggle, so the Kotlin receiver has already called
/// `onDisconnected` for each address it held — this sweeps any straggler
/// `connections` entry defensively and emits `GattEvent::RadioChanged` so a
/// consumer (`PeerLink`) learns the radio's usability changed.
#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onRadioState<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    status: JString<'local>,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.radio_state_changed(env, status);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicRead<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    request_id: jlong,
    address: JString<'local>,
    _characteristic_uuid: JString<'local>,
    value: JByteArray<'local>,
    success: jboolean,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.characteristic_read(
        env,
        request_id,
        address,
        _characteristic_uuid,
        value,
        success,
    );
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicWriteResult<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    request_id: jlong,
    address: JString<'local>,
    _characteristic_uuid: JString<'local>,
    success: jboolean,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.characteristic_written(
        env,
        request_id,
        address,
        _characteristic_uuid,
        success,
    );
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicChanged<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    address: JString<'local>,
    characteristic_uuid: JString<'local>,
    value: JByteArray<'local>,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.characteristic_changed(
        env,
        address,
        characteristic_uuid,
        value,
    );
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onServerCharacteristicWritten<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    address: JString<'local>,
    characteristic_uuid: JString<'local>,
    value: JByteArray<'local>,
    session: jlong,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.server_characteristic_written(
        env,
        address,
        characteristic_uuid,
        value,
        session,
    );
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onSubscribed<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    request_id: jlong,
    address: JString<'local>,
    characteristic_uuid: JString<'local>,
    success: jboolean,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.subscribed(
        env,
        request_id,
        address,
        characteristic_uuid,
        success,
    );
}

/// A central enabled notifications on one of our server characteristics.
///
/// This is the peripheral-role equivalent of "connected", and deliberately
/// later than the physical connection: it is the first moment the notify
/// path back to that peer exists, so a server that greets on this event
/// cannot lose the greeting. It also attributes the peer to *this* service,
/// which a bare connection does not.
#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onServerSubscribed<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    address: JString<'local>,
    session: jlong,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.server_subscribed(env, address, session);
}

/// Completion of a queued server notification.
#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onNotifySent<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    native_handle: jlong,
    request_id: jlong,
    success: jboolean,
) {
    // SAFETY: `native_handle` comes from `Arc::into_raw` in `with_context`,
    // and that strong count is never released, so the `Inner` is alive.
    unsafe { Inner::from_handle(native_handle) }.notification_sent(_env, request_id, success);
}

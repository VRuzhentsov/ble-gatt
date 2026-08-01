//! Android backend: raw JNI (`jni` crate) + `ndk-context`, calling directly
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
//! `Inner` is heap-allocated behind an `Arc`. `AndroidBackend::new()` passes
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
//! `AndroidBackend` itself is dropped first while connections are still
//! live. `Drop for AndroidBackend` calls the bridge's `closeAll()`, which
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

use async_trait::async_trait;
use jni::objects::{GlobalRef, JByteArray, JClass, JIntArray, JObject, JObjectArray, JString, JValue};
use jni::sys::{jboolean, jint, jlong};
use jni::{JNIEnv, JavaVM};
use tokio::sync::{broadcast, mpsc, oneshot};
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

/// Locally-assigned code for "an advertise was cancelled by
/// `stop_advertising` before Android reported its result". Above the range
/// Android's `AdvertiseCallback` uses, so it cannot be mistaken for a
/// controller error.
const ADVERTISE_ERROR_STOPPED: i32 = 102;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    Role, ServiceUuid, WriteType,
};

const BRIDGE_CLASS_BINARY_NAME: &str = "dev.blegatt.BleGattBridge";
const CALLBACK_CLASS: &str = "dev/blegatt/NativeKt";

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

/// State of the one scan this backend allows at a time.
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
    /// `Backend::events`.
    server_events_tx: broadcast::Sender<GattEvent>,
    /// Negotiated ATT MTU per peer address, populated from
    /// `onMtuChanged`. Shared rather than held per-`AndroidGattConnection`
    /// because the JNI callback arrives on a Binder thread with only the
    /// address to key on.
    att_mtus: StdMutex<HashMap<String, u16>>,
    /// Resolved by `onAdvertiseResult`. Android decides asynchronously
    /// whether an advertisement actually started, so `advertise()` must wait
    /// for it rather than reporting success the moment the JNI call returns.
    advertise_tx: StdMutex<Option<oneshot::Sender<std::result::Result<(), i32>>>>,
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
    /// await point would make every `Backend`/`GattConnection` future
    /// non-`Send`, which the trait's `Send` supertrait bound forbids.
    fn call_void(&self, env: &mut JNIEnv, method: &str, sig: &str, args: &[JValue]) -> Result<()> {
        let bridge = self.bridge()?;
        let result = env.call_method(bridge.as_obj(), method, sig, args);
        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(jni_error(env, method, err)),
        }
    }

    fn call_bool(&self, env: &mut JNIEnv, method: &str, sig: &str) -> Result<bool> {
        let bridge = self.bridge()?;
        let result = env.call_method(bridge.as_obj(), method, sig, &[]).and_then(|v| v.z());
        match result {
            Ok(value) => Ok(value),
            Err(err) => Err(jni_error(env, method, err)),
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
fn jni_error(env: &mut JNIEnv, method: &str, err: jni::errors::Error) -> BleError {
    let detail = describe_pending_exception(env);
    BleError::Gatt(format!(
        "{method} failed: {err}{}",
        detail.map(|d| format!(" ({d})")).unwrap_or_default()
    ))
}

pub struct AndroidBackend {
    inner: Arc<Inner>,
}

impl AndroidBackend {
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
            Err(err) => return Err(jni_error(&mut env, "subscribedAddresses", err)),
        };
        let array = JObjectArray::from(array);
        let len = env.get_array_length(&array).unwrap_or(0);
        let mut peers = Vec::with_capacity(len as usize);
        for i in 0..len {
            let Ok(item) = env.get_object_array_element(&array, i) else {
                continue;
            };
            peers.push(PeerAddress(read_jstring(&mut env, &JString::from(item))));
        }
        Ok(peers)
    }
}

impl AndroidBackend {
    pub async fn new() -> Result<Self> {
        let ctx = ndk_context::android_context();
        let vm = unsafe { JavaVM::from_raw(ctx.vm().cast()) }
            .map_err(|err| BleError::AdapterUnavailable(format!("JavaVM::from_raw failed: {err}")))?;

        // Scoped so this first `env` (which borrows `vm`) is dropped before
        // `vm` moves into `Inner` below; a fresh `env` is re-attached from
        // `inner.vm` afterward (attaching an already-attached thread again
        // is a documented no-op in jni-rs, not a second real attach).
        let context = {
            let env = vm
                .attach_current_thread_as_daemon()
                .map_err(|err| BleError::AdapterUnavailable(format!("JNI attach failed: {err}")))?;
            let context_obj = unsafe { JObject::from_raw(ctx.context().cast()) };
            env.new_global_ref(context_obj)
                .map_err(|err| BleError::AdapterUnavailable(format!("global ref of Context failed: {err}")))?
        };

        let (server_events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            vm,
            context,
            bridge: OnceLock::new(),
            connections: StdMutex::new(HashMap::new()),
            scan: StdMutex::new(ScanState::default()),
            server_events_tx,
            att_mtus: StdMutex::new(HashMap::new()),
            advertise_tx: StdMutex::new(None),
            notify_waiters: StdMutex::new(HashMap::new()),
            next_notify_id: AtomicU64::new(1),
            next_op_id: AtomicU64::new(1),
            server_sessions: StdMutex::new(HashMap::new()),
            advertise_generation: AtomicU64::new(1),
            advertise_lock: tokio::sync::Mutex::new(()),
        });

        // See the module doc comment: this pointer must stay valid for as
        // long as any `Arc<Inner>` handle (this `AndroidBackend`, or any
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
        // `Inner` per backend — in practice one per process — which is a
        // bounded leak traded for the elimination of a use-after-free.
        let native_handle = Arc::into_raw(inner.clone()) as jlong;
        let mut env = inner
            .env()
            .map_err(|err| BleError::AdapterUnavailable(format!("JNI re-attach failed: {err}")))?;
        let bridge_class = load_app_class(&mut env, inner.context.as_obj(), BRIDGE_CLASS_BINARY_NAME)
            .map_err(|err| BleError::AdapterUnavailable(format!("failed to load BleGattBridge class: {err}")))?;
        let bridge_obj = env
            .new_object(
                &bridge_class,
                "(Landroid/content/Context;J)V",
                &[JValue::Object(inner.context.as_obj()), JValue::Long(native_handle)],
            )
            .map_err(|err| {
                let detail = describe_pending_exception(&mut env);
                BleError::AdapterUnavailable(format!(
                    "BleGattBridge construction failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                ))
            })?;
        let bridge_ref = env
            .new_global_ref(bridge_obj)
            .map_err(|err| BleError::AdapterUnavailable(format!("global ref of bridge failed: {err}")))?;
        inner
            .bridge
            .set(bridge_ref)
            .map_err(|_| BleError::AdapterUnavailable("bridge already initialized".to_string()))?;

        Ok(Self { inner })
    }
}

impl Drop for AndroidBackend {
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

#[async_trait]
impl Backend for AndroidBackend {
    /// Every failure path here logs before falling back. `false`/`false` is
    /// indistinguishable from "this device genuinely has no BLE support", so
    /// a silent fallback turns a real bug into a plausible-looking hardware
    /// limitation — which is exactly what happened once already in this
    /// codebase and cost a whole debugging session. See docs/adr/0002.
    async fn capabilities(&self) -> CapabilityReport {
        let mut env = match self.inner.env() {
            Ok(env) => env,
            Err(err) => {
                log::error!("capabilities: JNI attach failed: {err}");
                return CapabilityReport::default();
            }
        };
        let central = match self.inner.call_bool(&mut env, "hasCentralSupport", "()Z") {
            Ok(value) => value,
            Err(err) => {
                let detail = describe_pending_exception(&mut env);
                log::error!(
                    "capabilities: hasCentralSupport failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                );
                false
            }
        };
        let peripheral = match self.inner.call_bool(&mut env, "hasPeripheralSupport", "()Z") {
            Ok(value) => value,
            Err(err) => {
                let detail = describe_pending_exception(&mut env);
                log::error!(
                    "capabilities: hasPeripheralSupport failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                );
                false
            }
        };
        log::info!("capabilities: central={central} peripheral={peripheral}");
        CapabilityReport { central, peripheral }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        let (tx, rx) = mpsc::channel(DISCOVERY_QUEUE_DEPTH);
        // Install the new sender only *after* the platform confirms the scan
        // started, and hold that lock across the JNI call.
        //
        // Holding it is what makes this atomic. `onScanFailed` for the
        // *existing* scan can fire while we are inside `startScan`; if we
        // had swapped our sender in first it would close ours instead of the
        // active scan's, and the active scan's stream would then hang
        // forever with its error never surfaced. Blocking that callback for
        // the duration of one JNI call is cheap, and Android posts
        // `ScanCallback` through a handler rather than calling it inline on
        // this thread, so it cannot deadlock us.
        let mut scan = self.inner.scan.lock().unwrap();
        // Chosen before `startScan` so the platform callbacks can echo it
        // back: a callback that fires during the call must be attributable
        // to this scan, not to whatever was installed before.
        let generation = scan.generation + 1;
        {
            let mut env = self.inner.env()?;
            let uuid = env
                .new_string(service.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bridge = self.inner.bridge()?;
            let started = env
                .call_method(
                    bridge.as_obj(),
                    "startScan",
                    "(Ljava/lang/String;J)Z",
                    &[JValue::Object(&uuid), JValue::Long(generation as i64)],
                )
                .and_then(|v| v.z());
            match started {
                // Same exception-clearing contract as `call_void`/`call_bool`:
                // a `startScan` that throws (a missing runtime Bluetooth
                // permission is the common case) would otherwise leave the
                // exception pending on the daemon-attached Tokio worker and
                // poison every later JNI call scheduled there.
                Err(err) => {
                    log::error!("scan: startScan threw for service {}", service.0);
                    return Err(jni_error(&mut env, "startScan", err));
                }
                // Rejected. The active scan keeps ownership untouched — we
                // never took it away.
                Ok(false) => {
                    log::warn!("scan: refused — a scan is already active");
                    return Err(BleError::Gatt(
                        "a scan is already active; concurrent scans are not supported".to_string(),
                    ));
                }
                Ok(true) => {
                    log::info!("scan: started for service {} generation={generation}", service.0);
                }
            }
        }
        // Ours now. Installed before the lock is released, so an
        // `onScanFailed` that fired during `startScan` — and is blocked on
        // this lock — finds our sender and records against our scan.
        scan.tx = Some(tx);
        scan.error = None;
        scan.generation = generation;
        drop(scan);

        Ok(Box::pin(ScanStream {
            inner: self.inner.clone(),
            rx: ReceiverStream::new(rx),
            generation,
        }))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let (connected_tx, connected_rx) = oneshot::channel();
        let session;
        {
            let mut connections = self.inner.connections.lock().unwrap();
            let state = connections.entry(peer.0.clone()).or_default();
            // Reject rather than overwrite, for *both* an in-flight connect
            // and one that already succeeded. The Kotlin bridge keeps one
            // `BluetoothGatt` per address: a second connect would replace it
            // without closing the first, leave two Rust handles sharing the
            // replacement's callback state, and let a stale callback from the
            // old GATT delete the new map entry — breaking both handles.
            if state.connected_tx.is_some() {
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: "a connection to this peer is already in progress".to_string(),
                });
            }
            if state.live {
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: "a connection to this peer is already open".to_string(),
                });
            }
            state.session = self.inner.next_op_id.fetch_add(1, Ordering::Relaxed);
            state.connected_tx = Some(connected_tx);
            session = state.session;
        }

        // Every failure below must clear `connected_tx`. Leaving it set once
        // its receiver is gone makes the guard above reject every retry for
        // the lifetime of the backend — so a transient cause (a missing
        // runtime permission, a powered-down adapter) would become permanent
        // even after it was fixed.
        let dial = (|| -> Result<()> {
            let mut env = self.inner.env()?;
            let session = session as i64;
            let address = env.new_string(&peer.0).map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner
                .call_void(
                    &mut env,
                    "connect",
                    "(Ljava/lang/String;J)V",
                    &[JValue::Object(&address), JValue::Long(session)],
                )
        })();
        if let Err(err) = dial {
            log::warn!("connect: JNI dial to {} failed: {err}", peer.0);
            self.inner.clear_pending_connect(&peer.0);
            return Err(err);
        }
        log::info!("connect: dialling {} session={session}", peer.0);

        // Cancellation guard. `connect()` is an async fn, so its caller can
        // drop the future mid-await — a timeout, a `select!` losing a race —
        // and nothing below would then run. Without this, the pending slot
        // stays populated and every retry is refused as "already in
        // progress"; and if the callback later lands anyway, it marks a GATT
        // live that no Rust handle owns, after which retries are refused as
        // "already open" instead. Disarmed on success.
        let guard = ConnectGuard {
            inner: self.inner.clone(),
            address: peer.0.clone(),
            armed: true,
        };

        if connected_rx.await.is_err() {
            // The guard handles cleanup on the way out.
            log::warn!("connect: {} disconnected before the connection completed", peer.0);
            return Err(BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "disconnected before connection completed".to_string(),
            });
        }
        log::info!("connect: {} connected session={session}", peer.0);
        let mut guard = guard;
        guard.armed = false;

        let session = self
            .inner
            .connections
            .lock()
            .unwrap()
            .get(&peer.0)
            .map(|state| state.session);
        Ok(Box::new(AndroidGattConnection {
            inner: self.inner.clone(),
            address: peer.0.clone(),
            session,
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        // Held across claiming the generation and issuing the platform call,
        // so an abandoned attempt's cleanup cannot interleave with a retry.
        // Released before awaiting the result, so a cancelled attempt's
        // cleanup is able to acquire it.
        // Held across claiming the generation *and* issuing the platform
        // call. Releasing after the increment left two windows: a
        // `stop_advertising` could complete while no attempt was installed
        // and this future would then start a server after the stop returned,
        // and two attempts could claim generations in one order and call
        // Kotlin in the other, so the older one replaced the newer. Released
        // before awaiting the result, so a cancelled attempt's cleanup can
        // still acquire it.
        let serialise = self.inner.advertise_lock.lock().await;
        let advertise_generation =
            self.inner.advertise_generation.fetch_add(1, Ordering::SeqCst) + 1;
        // Scoped: `JNIEnv` is `!Send`, so it must be entirely out of scope
        // before the `.await` below, or the whole future stops being `Send`
        // and no longer satisfies the trait.
        let rx = {
        let mut env = self.inner.env()?;
        let n = service.characteristics.len() as i32;

        let service_uuid = env
            .new_string(service.uuid.0.to_string())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let char_uuids = env
            .new_object_array(n, "java/lang/String", JObject::null())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let readable = env.new_boolean_array(n).map_err(|err| BleError::Gatt(err.to_string()))?;
        let writable = env.new_boolean_array(n).map_err(|err| BleError::Gatt(err.to_string()))?;
        let notifiable = env.new_boolean_array(n).map_err(|err| BleError::Gatt(err.to_string()))?;
        let values = env
            .new_object_array(n, "[B", JObject::null())
            .map_err(|err| BleError::Gatt(err.to_string()))?;

        for (i, characteristic) in service.characteristics.iter().enumerate() {
            let i = i as i32;
            let uuid_str = env
                .new_string(characteristic.uuid.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_object_array_element(&char_uuids, i, &uuid_str)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_boolean_array_region(&readable, i, &[characteristic.readable as jboolean])
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_boolean_array_region(&writable, i, &[characteristic.writable as jboolean])
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_boolean_array_region(&notifiable, i, &[characteristic.notifiable as jboolean])
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let value = env
                .byte_array_from_slice(&characteristic.initial_value)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            env.set_object_array_element(&values, i, &value)
                .map_err(|err| BleError::Gatt(err.to_string()))?;
        }

        let (tx, rx) = oneshot::channel();
        *self.inner.advertise_tx.lock().unwrap() = Some(tx);
        self.inner.call_void(
            &mut env,
            "startAdvertising",
            "(Ljava/lang/String;[Ljava/lang/String;[Z[Z[Z[[B)V",
            &[
                JValue::Object(&service_uuid),
                JValue::Object(&char_uuids),
                JValue::Object(&readable),
                JValue::Object(&writable),
                JValue::Object(&notifiable),
                JValue::Object(&values),
            ],
        )?;
        rx
        };

        drop(serialise);

        // Armed once the platform call has been issued. A caller that times
        // out or drops this future runs none of the code below, and dropping
        // `rx` performs no platform cleanup — so Kotlin would go on to
        // finish service registration and start advertising for an attempt
        // nobody is waiting on. That leaves a live server the caller never
        // received, and a retry then either overwrites the bridge's sole
        // server reference or fails as already advertising.
        let guard = AdvertiseGuard {
            inner: self.inner.clone(),
            generation: advertise_generation,
            armed: true,
        };

        let outcome = match rx.await {
            Ok(Ok(())) => {
                log::info!("advertise: started, generation={advertise_generation}");
                Ok(())
            }
            Ok(Err(code)) => {
                log::warn!("advertise: Android refused to start (AdvertiseCallback error {code})");
                Err(BleError::Gatt(format!(
                    "Android refused to start advertising (AdvertiseCallback error {code})"
                )))
            }
            Err(_) => {
                log::warn!("advertise: outcome was never reported");
                Err(BleError::Gatt(
                    "advertise outcome was never reported".to_string(),
                ))
            }
        };
        // Disarmed whenever an outcome was actually observed, success or
        // failure: success hands the caller a server it now owns, and a
        // failure has already been torn down by `failAdvertise` on the
        // Kotlin side. The guard exists solely for the path where this
        // future is dropped before `rx` resolves, where neither is true.
        let mut guard = guard;
        guard.armed = false;
        outcome
    }

    async fn stop_advertising(&self) -> Result<()> {
        // Same lock `advertise` holds across its platform call. Without it a
        // stop could land between an attempt installing its sender and
        // issuing `startAdvertising`: the stop resolves that sender and
        // stops whatever is visible, then the advertise resumes and starts a
        // server *after* stop has already returned — and because the attempt
        // observed an outcome, its guard is disarmed and nothing cleans it
        // up.
        let _serialise = self.inner.advertise_lock.lock().await;
        // Resolve any advertise still waiting on Android's asynchronous
        // start result. Kotlin invalidates the server generation, so that
        // callback is deliberately ignored when it arrives — which means
        // nothing else would ever complete this waiter and `advertise()`
        // would hang for the life of the process.
        if let Some(pending) = self.inner.advertise_tx.lock().unwrap().take() {
            let _ = pending.send(Err(ADVERTISE_ERROR_STOPPED));
        }
        log::info!("advertise: stopping");
        let mut env = self.inner.env()?;
        self.inner.call_void(&mut env, "stopAdvertising", "()V", &[])
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        // Broadcast is expressed as one addressed send per subscriber, so
        // every payload goes through the same queued, completion-confirmed
        // path — Android has no atomic multi-device notify anyway.
        let subscribers = self.subscribed_peers(characteristic)?;
        if subscribers.is_empty() {
            return Err(BleError::Gatt(
                "notify reached no subscriber — not advertising, unknown characteristic, \
                 or nobody subscribed"
                    .to_string(),
            ));
        }
        let mut delivered = false;
        let mut last_error = None;
        for peer in subscribers {
            match self.notify_peer(&peer, None, characteristic, value.clone()).await {
                Ok(()) => delivered = true,
                Err(err) => last_error = Some(err),
            }
        }
        if !delivered {
            return Err(last_error
                .unwrap_or_else(|| BleError::Gatt("notify reached no subscriber".to_string())));
        }
        Ok(())
    }

    async fn notify_peer(
        &self, peer: &PeerAddress, session: Option<u64>, characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        // The session is validated inside Kotlin, atomically with selecting
        // the subscriber and enqueuing. Checking it here would be a separate
        // transition, and Rust cannot hold `server_sessions` across the JNI
        // call without completing a deadlock cycle through the bridge
        // monitor. 0 means "whichever session holds this address".
        let request_id = self.inner.next_notify_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner.notify_waiters.lock().unwrap().insert(request_id, tx);

        let queued = (|| -> Result<bool> {
            let mut env = self.inner.env()?;
            let address = env.new_string(&peer.0).map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bytes =
                env.byte_array_from_slice(&value).map_err(|err| BleError::Gatt(err.to_string()))?;
            let bridge = self.inner.bridge()?;
            let result = env
                .call_method(
                    bridge.as_obj(),
                    "notifyCharacteristicTo",
                    "(Ljava/lang/String;Ljava/lang/String;[BJJ)Z",
                    &[
                        JValue::Object(&address),
                        JValue::Object(&uuid),
                        JValue::Object(&bytes),
                        JValue::Long(request_id as i64),
                        JValue::Long(session.unwrap_or(0) as i64),
                    ],
                )
                .and_then(|v| v.z());
            match result {
                Ok(value) => Ok(value),
                Err(err) => Err(jni_error(&mut env, "notifyCharacteristicTo", err)),
            }
        })();

        match queued {
            // Nothing was queued, so no callback is coming — drop the waiter
            // rather than leaving this future to hang forever.
            Ok(false) | Err(_) => {
                log::warn!(
                    "notify: {} was not queued for {} — no live notify session",
                    value.len(),
                    peer.0
                );
                self.inner.notify_waiters.lock().unwrap().remove(&request_id);
                queued?;
                return Err(BleError::Gatt(format!(
                    "{} has no live notify session for this characteristic",
                    peer.0
                )));
            }
            Ok(true) => {}
        }

        // Android reports the real status here, not from the call above.
        // Returning Ok on initiation alone would tell a reliable caller a
        // fragment was sent when transmission had actually failed.
        match rx.await {
            Ok(true) => {
                log::trace!("notify: {} bytes delivered to {}", value.len(), peer.0);
                Ok(())
            }
            Ok(false) => {
                log::warn!("notify: {} bytes to {} failed in transmission", value.len(), peer.0);
                Err(BleError::Gatt(format!("notify to {} failed", peer.0)))
            }
            Err(_) => {
                log::warn!("notify: send to {} was abandoned before completion", peer.0);
                Err(BleError::Gatt(format!(
                    "notify to {} was abandoned before completion",
                    peer.0
                )))
            }
        }
    }

    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        // The session is validated inside Kotlin, atomically with selecting
        // and cancelling the device. Checking it here and then calling would
        // be two transitions, and a Binder callback can replace the
        // subscription in between. 0 means "whichever session holds this
        // address", matching `None` at the port.
        let mut env = self.inner.env()?;
        let address = env
            .new_string(&peer.0)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        self.inner.call_void(
            &mut env,
            "disconnectServerPeer",
            "(Ljava/lang/String;J)V",
            &[
                JValue::Object(&address),
                JValue::Long(session.unwrap_or(0) as i64),
            ],
        )
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let rx = self.inner.server_events_tx.subscribe();
        Box::pin(tokio_stream::wrappers::BroadcastStream::new(rx).map(|item| match item {
            Ok(event) => event,
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                GattEvent::Lagged { dropped: n }
            }
        }))
    }
}

/// Wraps the discovery channel so dropping the stream (the caller losing
/// interest in scan results) stops the underlying Android scan — mirrors
/// `LinuxBackend::scan`'s stream-owns-the-scan-lifetime contract.
struct ScanStream {
    inner: Arc<Inner>,
    rx: ReceiverStream<Result<DiscoveredPeer>>,
    generation: u64,
}

impl tokio_stream::Stream for ScanStream {
    type Item = Result<DiscoveredPeer>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
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
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.overflow.swap(false, Ordering::SeqCst) {
            return std::task::Poll::Ready(Some(Err(BleError::Gatt(
                "notification queue overflowed: payloads were dropped and at least one \
                 message is lost"
                    .to_string(),
            ))));
        }
        std::pin::Pin::new(&mut self.rx).poll_next(cx).map(|item| item.map(Ok))
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
            .unwrap_or(crate::backend::DEFAULT_ATT_MTU)
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
        };
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
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
        let result = rx.await.map_err(|_| BleError::NotConnected(self.address.clone()));
        drop(guard);
        result?
    }

    async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
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
        };
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            let bytes = env.byte_array_from_slice(&value).map_err(|err| BleError::Gatt(err.to_string()))?;
            let without_response = matches!(write_type, WriteType::WithoutResponse);
            self.inner.call_void(
                &mut env,
                "writeCharacteristic",
                "(Ljava/lang/String;Ljava/lang/String;[BZJJ)V",
                &[
                    JValue::Object(&address),
                    JValue::Object(&uuid),
                    JValue::Object(&bytes),
                    JValue::Bool(without_response as jboolean),
                    JValue::Long(request_id as i64),
                    JValue::Long(self.session.unwrap_or_default() as i64),
                ],
            )?;
        }
        let result = rx.await.map_err(|_| BleError::NotConnected(self.address.clone()));
        drop(guard);
        result?
    }

    async fn subscribe(
        &mut self, characteristic: CharacteristicUuid,
    ) -> Result<BoxStream<Result<Vec<u8>>>> {
        let request_id = self.inner.next_op_id.fetch_add(1, Ordering::Relaxed);
        let (confirm_tx, confirm_rx) = oneshot::channel();
        let (notify_tx, notify_rx) = mpsc::channel(NOTIFY_QUEUE_DEPTH);
        let overflow = Arc::new(AtomicBool::new(false));
        self.with_session(|state| {
            state.subscribe_tx.insert(characteristic, (request_id, confirm_tx));
            state.notify_tx.insert(characteristic, (request_id, notify_tx, overflow.clone()));
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
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
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

    async fn disconnect(&mut self) -> Result<()> {
        // Most important here: a stale handle's `disconnect` would otherwise
        // tear down the connection that replaced it.
        let (tx, rx) = oneshot::channel();
        self.with_session(|state| state.disconnected_tx = Some(tx))?;
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
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

// ---------------------------------------------------------------------
// Native callbacks — implemented here, declared as `external fun` in
// Native.kt. Every signature must match Native.kt exactly: Kotlin
// compiles those top-level functions as static methods on `NativeKt`,
// giving the fixed JNI symbol name `Java_dev_blegatt_NativeKt_<name>`.
// ---------------------------------------------------------------------

/// # Safety contract
/// See the module doc comment: `native_handle` must be a live `Arc<Inner>`
/// pointer. Never called by Rust directly — only by the JVM, from whatever
/// thread owns the Android Bluetooth callback (usually a Binder thread).
unsafe fn inner_from_handle(native_handle: jlong) -> Arc<Inner> {
    let ptr = native_handle as *const Inner;
    Arc::increment_strong_count(ptr);
    Arc::from_raw(ptr)
}

/// jni-rs's `Result::Err` for a JNI call that failed because Java threw
/// just says "Java exception was thrown" — the crate does not surface the
/// exception's own message. This pulls it out (and prints the full stack
/// trace to logcat via `ExceptionDescribe`) before clearing it, since an
/// uncleared pending exception would abort the next JNI call made on this
/// thread.
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
    Some(read_jstring(env, &message))
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
/// obtained from any object the app's classloader already loaded — the
/// `Context` passed in at construction time.
fn load_app_class<'a>(env: &mut JNIEnv<'a>, context: &JObject, binary_name: &str) -> Result<JClass<'a>> {
    let context_class = env.get_object_class(context).map_err(|err| BleError::Gatt(err.to_string()))?;
    let class_loader = env
        .call_method(&context_class, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])
        .and_then(|v| v.l())
        .map_err(|err| BleError::Gatt(format!("getClassLoader failed: {err}")))?;
    let name = env.new_string(binary_name).map_err(|err| BleError::Gatt(err.to_string()))?;
    let class_obj = env
        .call_method(
            &class_loader,
            "loadClass",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            &[JValue::Object(&name)],
        )
        .and_then(|v| v.l())
        .map_err(|err| BleError::Gatt(format!("loadClass({binary_name}) failed: {err}")))?;
    Ok(JClass::from(class_obj))
}

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

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_dev_blegatt_NativeKt_onPeerDiscovered<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, generation: jlong,
    advertised_service_uuids: JObjectArray<'local>, address: JString<'local>,
    name: JString<'local>, rssi: jint, manufacturer_ids: JIntArray<'local>,
    manufacturer_values: JObjectArray<'local>, service_data_uuids: JObjectArray<'local>,
    service_data_values: JObjectArray<'local>,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let name = read_optional_jstring(&mut env, &name);

    let mut manufacturer_data = BTreeMap::new();
    if let Ok(len) = env.get_array_length(&manufacturer_ids) {
        let mut ids = vec![0i32; len as usize];
        if env.get_int_array_region(&manufacturer_ids, 0, &mut ids).is_ok() {
            let values = read_byte_array_array(&mut env, &manufacturer_values, len);
            for (id, value) in ids.into_iter().zip(values) {
                manufacturer_data.insert(id as u16, value);
            }
        }
    }

    let mut service_data = BTreeMap::new();
    if let Ok(len) = env.get_array_length(&service_data_uuids) {
        let values = read_byte_array_array(&mut env, &service_data_values, len);
        for (i, value) in values.into_iter().enumerate() {
            let Ok(uuid_obj) = env.get_object_array_element(&service_data_uuids, i as i32) else {
                continue;
            };
            let uuid_str = read_jstring(&mut env, &JString::from(uuid_obj));
            if let Ok(uuid) = Uuid::parse_str(&uuid_str) {
                service_data.insert(ServiceUuid(uuid), value);
            }
        }
    }

    // Android already parsed these to match the scan filter, so reporting
    // them costs nothing — and an empty list made a `DiscoveredPeer` from
    // this backend claim no advertised services at all, unlike Linux and the
    // mock, breaking consumers that inspect the field.
    let mut services = Vec::new();
    if let Ok(len) = env.get_array_length(&advertised_service_uuids) {
        for i in 0..len {
            let Ok(item) = env.get_object_array_element(&advertised_service_uuids, i) else {
                continue;
            };
            if let Ok(uuid) = Uuid::parse_str(&read_jstring(&mut env, &JString::from(item))) {
                services.push(ServiceUuid(uuid));
            }
        }
    }

    let scan = inner.scan.lock().unwrap();
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
    if let Some(tx) = scan.tx.as_ref() {
        // try_send on a bounded queue: this runs on a JVM callback thread
        // that must not block, and in a dense advertising environment the
        // radio can outpace any consumer indefinitely.
        let peer = DiscoveredPeer {
            address: PeerAddress(address),
            name,
            services,
            manufacturer_data,
            service_data,
            rssi: Some(rssi as i16),
        };
        log::info!("jni onPeerDiscovered: {} rssi={rssi}", peer.address.0);
        if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(Ok(peer)) {
            log::warn!(
                "jni onPeerDiscovered: discovery queue full, dropping scan result; \
                 the peer will reappear on its next advertisement"
            );
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onAdvertiseResult<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, success: jboolean,
    error_code: jint,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    if success != 0 {
        log::info!("jni onAdvertiseResult: advertising started");
    } else {
        log::error!("jni onAdvertiseResult: advertising failed, error_code={error_code}");
    }
    let pending = inner.advertise_tx.lock().unwrap().take();
    if let Some(tx) = pending {
        let _ = tx.send(if success != 0 { Ok(()) } else { Err(error_code) });
    } else {
        log::warn!("jni onAdvertiseResult: no pending advertise request to complete");
    }
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onScanFailed<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, generation: jlong,
    error_code: jint,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
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
        log::trace!("jni onScanFailed: ignoring failure from superseded generation {generation}");
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

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onMtuChanged<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    mtu: jint,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    log::info!("jni onMtuChanged: {address} negotiated ATT MTU {mtu}");
    inner.att_mtus.lock().unwrap().insert(address, mtu as u16);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onConnected<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    from_server: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    log::info!("jni onConnected: {address} from_server={}", from_server != 0);
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
    // already does this, and `Backend::events` documents both roles.
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
            peer: PeerAddress(address.clone()),
            local_role: Role::Central,
            session,
        });
    }
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onDisconnected<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    from_server: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    log::info!("jni onDisconnected: {address} from_server={}", from_server != 0);
    let disconnected_session = if from_server != 0 {
        // Server-side peers carry the session minted when they subscribed.
        inner.server_sessions.lock().unwrap().remove(&address)
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
        inner.att_mtus.lock().unwrap().remove(&address);
        let mut connections = inner.connections.lock().unwrap();
        if let Some(mut state) = connections.remove(&address) {
            state.live = false;
            if let Some(tx) = state.disconnected_tx {
                let _ = tx.send(());
            }
            // A connect attempt that never reaches `onConnected` ends in
            // `onDisconnected` instead — resolving `connected_tx` here (with no
            // receiver-visible success value) makes the pending `connect()`
            // call fail via the dropped-sender path in `Backend::connect`.
        }
    }
    // Publish regardless of whether we had per-connection state: this fires
    // for unsolicited drops (peer out of range, powered off) as well as
    // disconnects we asked for, and `Backend::events()` is the only way a
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

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicRead<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, request_id: jlong,
    address: JString<'local>, _characteristic_uuid: JString<'local>, value: JByteArray<'local>,
    success: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
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

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicWriteResult<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, request_id: jlong,
    address: JString<'local>, _characteristic_uuid: JString<'local>, success: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
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

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicChanged<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    characteristic_uuid: JString<'local>, value: JByteArray<'local>,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let characteristic_uuid = read_jstring(&mut env, &characteristic_uuid);
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

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onServerCharacteristicWritten<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    characteristic_uuid: JString<'local>, value: JByteArray<'local>, session: jlong,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let characteristic_uuid = read_jstring(&mut env, &characteristic_uuid);
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
    let _ = inner.server_events_tx.send(GattEvent::CharacteristicWritten {
        peer: PeerAddress(address),
        characteristic: CharacteristicUuid(uuid),
        value: bytes,
    });
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onSubscribed<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, request_id: jlong,
    address: JString<'local>,
    characteristic_uuid: JString<'local>, success: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let characteristic_uuid = read_jstring(&mut env, &characteristic_uuid);
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

// Referenced only to keep the `CALLBACK_CLASS` constant documented and
// grep-discoverable alongside `BRIDGE_CLASS`; the JVM resolves the actual
// symbol by the exported function names above, not this constant.
#[allow(dead_code)]
fn _callback_class_name() -> &'static str {
    CALLBACK_CLASS
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
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    session: jlong,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
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
}

impl Drop for PendingOpGuard {
    fn drop(&mut self) {
        let mut connections = self.inner.connections.lock().unwrap();
        if let Some(state) = connections.get_mut(&self.address) {
            match self.op {
                PendingOp::Read => state.read_tx = None,
                PendingOp::Write => state.write_tx = None,
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
        let mut connections = self.inner.connections.lock().unwrap();
        let Some(state) = connections.get_mut(&self.address) else {
            return;
        };
        // Keyed on the *notification route's* own id, not on `subscribe_tx`:
        // a rejected subscription has already had its `subscribe_tx` removed
        // by `onSubscribed`, so predicating on that entry meant the dead
        // route was never cleaned up — the exact leak this guard exists to
        // prevent. Both entries are still conditional on ownership so a
        // retry that has claimed them is not stranded.
        if state
            .subscribe_tx
            .get(&self.characteristic)
            .is_some_and(|(id, _)| *id == self.request_id)
        {
            state.subscribe_tx.remove(&self.characteristic);
        }
        if state
            .notify_tx
            .get(&self.characteristic)
            .is_some_and(|(id, _, _)| *id == self.request_id)
        {
            state.notify_tx.remove(&self.characteristic);
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

/// Completion of a queued server notification.
#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onNotifySent<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, request_id: jlong,
    success: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let waiter = inner.notify_waiters.lock().unwrap().remove(&(request_id as u64));
    if let Some(tx) = waiter {
        let _ = tx.send(success != 0);
    }
}

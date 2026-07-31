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
//! `Arc::as_ptr(&inner) as jlong` to the `BleGattBridge` Kotlin constructor,
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
//! The residual risk is now narrower but not zero: a callback already
//! *executing* on a Binder thread at the moment the last `Arc<Inner>` is
//! released still observes a dangling pointer. Closing the GATTs first
//! removes the sources of new callbacks; it cannot retract one already in
//! flight. Documented rather than silently assumed away, matching the
//! don't-hide-hard-cases convention this project inherited.

use std::collections::{BTreeMap, HashMap};
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

#[derive(Default)]
struct ConnectionState {
    connected_tx: Option<oneshot::Sender<()>>,
    read_tx: Option<oneshot::Sender<Result<Vec<u8>>>>,
    write_tx: Option<oneshot::Sender<Result<()>>>,
    subscribe_tx: HashMap<CharacteristicUuid, oneshot::Sender<bool>>,
    notify_tx: HashMap<CharacteristicUuid, mpsc::Sender<Vec<u8>>>,
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
}

impl Inner {
    fn env(&self) -> Result<JNIEnv<'_>> {
        self.vm
            .attach_current_thread_as_daemon()
            .map_err(|err| BleError::Gatt(format!("JNI attach failed: {err}")))
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
        });

        // See the module doc comment: this pointer must stay valid for as
        // long as any `Arc<Inner>` handle (this `AndroidBackend`, or any
        // `AndroidGattConnection` cloned from it) is alive.
        let native_handle = Arc::as_ptr(&inner) as jlong;
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
                eprintln!("[ble-gatt][android] capabilities: JNI attach failed: {err}");
                return CapabilityReport::default();
            }
        };
        let central = match self.inner.call_bool(&mut env, "hasCentralSupport", "()Z") {
            Ok(value) => value,
            Err(err) => {
                let detail = describe_pending_exception(&mut env);
                eprintln!(
                    "[ble-gatt][android] capabilities: hasCentralSupport failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                );
                false
            }
        };
        let peripheral = match self.inner.call_bool(&mut env, "hasPeripheralSupport", "()Z") {
            Ok(value) => value,
            Err(err) => {
                let detail = describe_pending_exception(&mut env);
                eprintln!(
                    "[ble-gatt][android] capabilities: hasPeripheralSupport failed: {err}{}",
                    detail.map(|d| format!(" ({d})")).unwrap_or_default()
                );
                false
            }
        };
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
                    "(Ljava/lang/String;)Z",
                    &[JValue::Object(&uuid)],
                )
                .and_then(|v| v.z());
            match started {
                // Same exception-clearing contract as `call_void`/`call_bool`:
                // a `startScan` that throws (a missing runtime Bluetooth
                // permission is the common case) would otherwise leave the
                // exception pending on the daemon-attached Tokio worker and
                // poison every later JNI call scheduled there.
                Err(err) => return Err(jni_error(&mut env, "startScan", err)),
                // Rejected. The active scan keeps ownership untouched — we
                // never took it away.
                Ok(false) => {
                    return Err(BleError::Gatt(
                        "a scan is already active; concurrent scans are not supported".to_string(),
                    ))
                }
                Ok(true) => {}
            }
        }
        // Ours now. Installed before the lock is released, so an
        // `onScanFailed` that fired during `startScan` — and is blocked on
        // this lock — finds our sender and records against our scan.
        scan.tx = Some(tx);
        scan.error = None;
        drop(scan);

        Ok(Box::pin(ScanStream {
            inner: self.inner.clone(),
            rx: ReceiverStream::new(rx),
        }))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let (connected_tx, connected_rx) = oneshot::channel();
        {
            let mut connections = self.inner.connections.lock().unwrap();
            let state = connections.entry(peer.0.clone()).or_default();
            // Reject rather than overwrite. Both this map and the Kotlin
            // bridge hold exactly one GATT per address, so a second
            // concurrent connect to the same peer would drop the first
            // caller's `connected_tx` — and that caller would then wait
            // forever while the single callback resolved only the second.
            if state.connected_tx.is_some() {
                return Err(BleError::ConnectFailed {
                    peer: peer.0.clone(),
                    reason: "a connection to this peer is already in progress".to_string(),
                });
            }
            state.connected_tx = Some(connected_tx);
        }
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&peer.0).map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner
                .call_void(&mut env, "connect", "(Ljava/lang/String;)V", &[JValue::Object(&address)])?;
        }

        connected_rx.await.map_err(|_| BleError::ConnectFailed {
            peer: peer.0.clone(),
            reason: "disconnected before connection completed".to_string(),
        })?;

        Ok(Box::new(AndroidGattConnection {
            inner: self.inner.clone(),
            address: peer.0.clone(),
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
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

        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(code)) => Err(BleError::Gatt(format!(
                "Android refused to start advertising (AdvertiseCallback error {code})"
            ))),
            Err(_) => Err(BleError::Gatt(
                "advertise outcome was never reported".to_string(),
            )),
        }
    }

    async fn stop_advertising(&self) -> Result<()> {
        let mut env = self.inner.env()?;
        self.inner.call_void(&mut env, "stopAdvertising", "()V", &[])
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let mut env = self.inner.env()?;
        let uuid = env
            .new_string(characteristic.0.to_string())
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        let bytes = env.byte_array_from_slice(&value).map_err(|err| BleError::Gatt(err.to_string()))?;
        let bridge = self.inner.bridge()?;
        let delivered = env
            .call_method(
                bridge.as_obj(),
                "notifyCharacteristic",
                "(Ljava/lang/String;[B)Z",
                &[JValue::Object(&uuid), JValue::Object(&bytes)],
            )
            .and_then(|v| v.z());
        let delivered = match delivered {
            Ok(value) => value,
            Err(err) => return Err(jni_error(&mut env, "notifyCharacteristic", err)),
        };
        if !delivered {
            return Err(BleError::Gatt(
                "notify delivered to nobody — not advertising, unknown characteristic, \
                 or no subscriber"
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn disconnect_peer(&self, peer: &PeerAddress) -> Result<()> {
        let mut env = self.inner.env()?;
        let address = env
            .new_string(&peer.0)
            .map_err(|err| BleError::Gatt(err.to_string()))?;
        self.inner.call_void(
            &mut env,
            "disconnectServerPeer",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&address)],
        )
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let rx = self.inner.server_events_tx.subscribe();
        Box::pin(tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|item| item.ok()))
    }
}

/// Wraps the discovery channel so dropping the stream (the caller losing
/// interest in scan results) stops the underlying Android scan — mirrors
/// `LinuxBackend::scan`'s stream-owns-the-scan-lifetime contract.
struct ScanStream {
    inner: Arc<Inner>,
    rx: ReceiverStream<Result<DiscoveredPeer>>,
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
        if let Ok(mut env) = self.inner.env() {
            let _ = self.inner.call_void(&mut env, "stopScan", "()V", &[]);
        }
        self.inner.scan.lock().unwrap().tx = None;
    }
}

struct AndroidGattConnection {
    inner: Arc<Inner>,
    address: String,
}

#[async_trait]
impl GattConnection for AndroidGattConnection {
    fn peer(&self) -> PeerAddress {
        PeerAddress(self.address.clone())
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
        let (tx, rx) = oneshot::channel();
        {
            let mut connections = self.inner.connections.lock().unwrap();
            let state = connections.entry(self.address.clone()).or_default();
            state.read_tx = Some(tx);
        }
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "readCharacteristic",
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[JValue::Object(&address), JValue::Object(&uuid)],
            )?;
        }
        rx.await.map_err(|_| BleError::NotConnected(self.address.clone()))?
    }

    async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut connections = self.inner.connections.lock().unwrap();
            let state = connections.entry(self.address.clone()).or_default();
            state.write_tx = Some(tx);
        }
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
                "(Ljava/lang/String;Ljava/lang/String;[BZ)V",
                &[
                    JValue::Object(&address),
                    JValue::Object(&uuid),
                    JValue::Object(&bytes),
                    JValue::Bool(without_response as jboolean),
                ],
            )?;
        }
        rx.await.map_err(|_| BleError::NotConnected(self.address.clone()))?
    }

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Vec<u8>>> {
        let (confirm_tx, confirm_rx) = oneshot::channel();
        let (notify_tx, notify_rx) = mpsc::channel(NOTIFY_QUEUE_DEPTH);
        {
            let mut connections = self.inner.connections.lock().unwrap();
            let state = connections.entry(self.address.clone()).or_default();
            state.subscribe_tx.insert(characteristic, confirm_tx);
            state.notify_tx.insert(characteristic, notify_tx);
        }
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
            let uuid = env
                .new_string(characteristic.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "subscribeCharacteristic",
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[JValue::Object(&address), JValue::Object(&uuid)],
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
        Ok(Box::pin(ReceiverStream::new(notify_rx)))
    }

    async fn disconnect(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut connections = self.inner.connections.lock().unwrap();
            connections.entry(self.address.clone()).or_default().disconnected_tx = Some(tx);
        }
        {
            let mut env = self.inner.env()?;
            let address = env.new_string(&self.address).map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner.call_void(
                &mut env,
                "disconnect",
                "(Ljava/lang/String;)V",
                &[JValue::Object(&address)],
            )?;
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
        Ok(())
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
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
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

    let scan = inner.scan.lock().unwrap();
    if let Some(tx) = scan.tx.as_ref() {
        // try_send on a bounded queue: this runs on a JVM callback thread
        // that must not block, and in a dense advertising environment the
        // radio can outpace any consumer indefinitely.
        let peer = DiscoveredPeer {
            address: PeerAddress(address),
            name,
            services: Vec::new(),
            manufacturer_data,
            service_data,
            rssi: Some(rssi as i16),
        };
        if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(Ok(peer)) {
            eprintln!(
                "[ble-gatt][android] discovery queue full, dropping scan result; \
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
    let pending = inner.advertise_tx.lock().unwrap().take();
    if let Some(tx) = pending {
        let _ = tx.send(if success != 0 { Ok(()) } else { Err(error_code) });
    }
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onScanFailed<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, error_code: jint,
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
    inner.att_mtus.lock().unwrap().insert(address, mtu as u16);
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onConnected<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    from_server: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    // Only the *client* path may resolve a pending outbound connect. A
    // server-side callback for the same address would otherwise complete it
    // before client-side service discovery had run, handing back a
    // connection with no characteristics.
    if from_server == 0 {
        let mut connections = inner.connections.lock().unwrap();
        if let Some(state) = connections.get_mut(&address) {
            if let Some(tx) = state.connected_tx.take() {
                let _ = tx.send(());
            }
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
        let _ = inner.server_events_tx.send(GattEvent::Connected {
            peer: PeerAddress(address.clone()),
            local_role: Role::Central,
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
    // Likewise: a central leaving our GATT server says nothing about an
    // outbound connection we hold to that same address. Tearing down the
    // client state here would kill reads, writes and subscriptions on a
    // still-live link.
    if from_server == 0 {
        inner.att_mtus.lock().unwrap().remove(&address);
        let mut connections = inner.connections.lock().unwrap();
        if let Some(state) = connections.remove(&address) {
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
    });
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onCharacteristicRead<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    _characteristic_uuid: JString<'local>, value: JByteArray<'local>, success: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let bytes = env.convert_byte_array(&value).unwrap_or_default();
    let mut connections = inner.connections.lock().unwrap();
    if let Some(state) = connections.get_mut(&address) {
        if let Some(tx) = state.read_tx.take() {
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
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
    _characteristic_uuid: JString<'local>, success: jboolean,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let mut connections = inner.connections.lock().unwrap();
    if let Some(state) = connections.get_mut(&address) {
        if let Some(tx) = state.write_tx.take() {
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
        if let Some(tx) = state.notify_tx.get(&CharacteristicUuid(uuid)) {
            // JVM callback thread: cannot block, so a saturated consumer
            // costs the affected message (reaped by the reassembly timeout)
            // rather than unbounded heap growth.
            if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(bytes) {
                eprintln!(
                    "[ble-gatt][android] notification queue full for {address}, dropping \
                     payload; any message it belonged to will time out"
                );
            }
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onServerCharacteristicWritten<'local>(
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
    // Kept as a safety net for stacks where `onConnectionStateChange` on the
    // server callback does not fire; `serve` treats `Connected` as
    // idempotent, so a duplicate here is harmless. Always the peripheral
    // role — this is a write arriving at *our* GATT server.
    let _ = inner.server_events_tx.send(GattEvent::Connected {
        peer: PeerAddress(address.clone()),
        local_role: Role::Peripheral,
    });
    let _ = inner.server_events_tx.send(GattEvent::CharacteristicWritten {
        peer: PeerAddress(address),
        characteristic: CharacteristicUuid(uuid),
        value: bytes,
    });
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onSubscribed<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
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
        if let Some(tx) = state.subscribe_tx.remove(&CharacteristicUuid(uuid)) {
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
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let _ = inner.server_events_tx.send(GattEvent::Connected {
        peer: PeerAddress(address),
        local_role: Role::Peripheral,
    });
}

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
//! live. The residual risk: if *every* `Arc<Inner>` handle is dropped while
//! a JNI callback is genuinely in flight on another thread, that callback
//! observes a dangling pointer. `Drop for AndroidBackend` mitigates this by
//! best-effort quiescing the bridge (stop scan/advertising) before the
//! count can reach zero, but does not eliminate the race — documented
//! honestly rather than silently assumed away, matching Fini's own
//! don't-hide-hard-cases convention this project inherited.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use async_trait::async_trait;
use jni::objects::{GlobalRef, JByteArray, JClass, JIntArray, JObject, JObjectArray, JString, JValue};
use jni::sys::{jboolean, jint, jlong};
use jni::{JNIEnv, JavaVM};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    ServiceUuid, WriteType,
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
    notify_tx: HashMap<CharacteristicUuid, mpsc::UnboundedSender<Vec<u8>>>,
    disconnected_tx: Option<oneshot::Sender<()>>,
}

struct Inner {
    vm: JavaVM,
    context: GlobalRef,
    bridge: OnceLock<GlobalRef>,
    connections: StdMutex<HashMap<String, ConnectionState>>,
    discovery_tx: StdMutex<Option<mpsc::UnboundedSender<DiscoveredPeer>>>,
    /// Named `server_events_tx` historically, but now carries both roles'
    /// lifecycle events — including central-side link loss. See
    /// `Backend::events`.
    server_events_tx: broadcast::Sender<GattEvent>,
    /// Negotiated ATT MTU per peer address, populated from
    /// `onMtuChanged`. Shared rather than held per-`AndroidGattConnection`
    /// because the JNI callback arrives on a Binder thread with only the
    /// address to key on.
    att_mtus: StdMutex<HashMap<String, u16>>,
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
        env.call_method(bridge.as_obj(), method, sig, args)
            .map_err(|err| BleError::Gatt(format!("{method} failed: {err}")))?;
        Ok(())
    }

    fn call_bool(&self, env: &mut JNIEnv, method: &str, sig: &str) -> Result<bool> {
        let bridge = self.bridge()?;
        env.call_method(bridge.as_obj(), method, sig, &[])
            .and_then(|v| v.z())
            .map_err(|err| BleError::Gatt(format!("{method} failed: {err}")))
    }
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
            discovery_tx: StdMutex::new(None),
            server_events_tx,
            att_mtus: StdMutex::new(HashMap::new()),
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
            let _ = self.inner.call_void(&mut env, "stopScan", "()V", &[]);
            let _ = self.inner.call_void(&mut env, "stopAdvertising", "()V", &[]);
        }
    }
}

#[async_trait]
impl Backend for AndroidBackend {
    async fn capabilities(&self) -> CapabilityReport {
        let Ok(mut env) = self.inner.env() else {
            return CapabilityReport::default();
        };
        let central = self.inner.call_bool(&mut env, "hasCentralSupport", "()Z").unwrap_or(false);
        let peripheral = self.inner.call_bool(&mut env, "hasPeripheralSupport", "()Z").unwrap_or(false);
        CapabilityReport { central, peripheral }
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<DiscoveredPeer>> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.inner.discovery_tx.lock().unwrap() = Some(tx);
        {
            let mut env = self.inner.env()?;
            let uuid = env
                .new_string(service.0.to_string())
                .map_err(|err| BleError::Gatt(err.to_string()))?;
            self.inner
                .call_void(&mut env, "startScan", "(Ljava/lang/String;)V", &[JValue::Object(&uuid)])?;
        }
        Ok(Box::pin(ScanStream {
            inner: self.inner.clone(),
            rx: UnboundedReceiverStream::new(rx),
        }))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let (connected_tx, connected_rx) = oneshot::channel();
        {
            let mut connections = self.inner.connections.lock().unwrap();
            connections.entry(peer.0.clone()).or_default().connected_tx = Some(connected_tx);
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
        )
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
        self.inner.call_void(
            &mut env,
            "notifyCharacteristic",
            "(Ljava/lang/String;[B)V",
            &[JValue::Object(&uuid), JValue::Object(&bytes)],
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
    rx: UnboundedReceiverStream<DiscoveredPeer>,
}

impl tokio_stream::Stream for ScanStream {
    type Item = DiscoveredPeer;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.rx).poll_next(cx)
    }
}

impl Drop for ScanStream {
    fn drop(&mut self) {
        if let Ok(mut env) = self.inner.env() {
            let _ = self.inner.call_void(&mut env, "stopScan", "()V", &[]);
        }
        *self.inner.discovery_tx.lock().unwrap() = None;
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
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
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
        Ok(Box::pin(UnboundedReceiverStream::new(notify_rx)))
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

    let discovery_tx = inner.discovery_tx.lock().unwrap();
    if let Some(tx) = discovery_tx.as_ref() {
        let _ = tx.send(DiscoveredPeer {
            address: PeerAddress(address),
            name,
            services: Vec::new(),
            manufacturer_data,
            service_data,
            rssi: Some(rssi as i16),
        });
    }
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
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    let mut connections = inner.connections.lock().unwrap();
    if let Some(state) = connections.get_mut(&address) {
        if let Some(tx) = state.connected_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_dev_blegatt_NativeKt_onDisconnected<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, native_handle: jlong, address: JString<'local>,
) {
    let inner = unsafe { inner_from_handle(native_handle) };
    let address = read_jstring(&mut env, &address);
    inner.att_mtus.lock().unwrap().remove(&address);
    {
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
            let _ = tx.send(bytes);
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
    let _ = inner.server_events_tx.send(GattEvent::Connected {
        peer: PeerAddress(address.clone()),
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

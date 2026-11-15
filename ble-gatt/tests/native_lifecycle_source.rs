//! Source regressions supplement cross-compilation. They do not exercise a native Bluetooth stack.

fn body<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    source
        .split_once(start)
        .expect(start)
        .1
        .split_once(end)
        .expect(end)
        .0
}

#[test]
fn native_visibility_keeps_server_ownership() {
    for source in [
        include_str!("../src/drivers/linux.rs"),
        include_str!("../src/drivers/windows/mod.rs"),
    ] {
        let toggle = body(
            source,
            "async fn set_advertising_enabled(",
            "async fn stop_advertising(",
        );
        assert!(!toggle.contains("shut_down"));
        assert!(!toggle.contains("forget_server_peers"));
        assert!(!toggle.contains("release_served_peers"));
        assert!(!toggle.contains("app_handle"));
        assert!(toggle.contains("advertise_lock") || toggle.contains("server_lock"));
    }
}

#[test]
fn android_visibility_jni_and_abi_are_coherent() {
    let rust = include_str!("../src/drivers/android.rs");
    let kotlin = include_str!(
        "../../tauri-plugin-ble-gatt/android/src/main/kotlin/dev/blegatt/BleGattBridge.kt"
    );
    assert!(rust.contains("const BRIDGE_ABI_VERSION: i32 = 7;"));
    assert!(kotlin.contains("fun bridgeAbiVersion(): Int = 7"));
    let toggle = body(
        rust,
        "async fn set_advertising_enabled(",
        "async fn stop_advertising(",
    );
    assert!(toggle.contains("\"setAdvertisingEnabled\", \"(Z)V\""));
    assert!(toggle.contains("rx.await"));
    assert!(toggle.contains("bridgeAbiVersion"));
    let stop = body(rust, "async fn stop_advertising(", "async fn notify(");
    assert!(
        stop.find("env.lock_obj").unwrap()
            < stop.find("served_service.lock().unwrap() = None").unwrap()
    );
    assert!(rust.contains("pending.complete("));
    let toggle = body(
        kotlin,
        "fun setAdvertisingEnabled(enabled: Boolean)",
        "fun stopAdvertising()",
    );
    assert!(toggle.contains("synchronized(this)"));
    assert!(toggle.contains("stopAdvertising(it)"));
    assert!(!toggle.contains("++serverGeneration"));
    assert!(!toggle.contains("gattServer?.close"));
    assert!(!toggle.contains("subscribedDevices.clear"));
    assert!(toggle.contains("advertisingStarted"));
}

#[test]
fn windows_acquisition_owns_resources_before_awaits() {
    let device = include_str!("../src/drivers/windows/device.rs");
    let open = body(
        device,
        "pub(super) async fn open(",
        "async fn services_with_cache_mode(",
    );
    assert!(open.contains("AcquiringLink"));
    assert!(
        open.find("acquired.gatt_session = Some").unwrap()
            < open.find("discover_services(").unwrap()
    );
    assert!(device.contains("impl Drop for AcquiringLink"));
    let subscribe = body(device, "async fn subscribe(", "async fn disconnect(");
    assert!(subscribe.find("ValueChangedGuard").unwrap() < subscribe.find("op.await").unwrap());
    assert!(device.contains("impl Drop for ValueChangedGuard"));
    assert!(subscribe.contains("links.get(&self.address) != Some(&self.session)"));
    let server = include_str!("../src/drivers/windows/peripheral.rs");
    assert!(server.contains("impl Drop for Server"));
    let startup = body(
        server,
        "pub(super) async fn start_server(",
        "pub(super) async fn start_provider_advertising(",
    );
    assert!(startup.find("let mut server = Server").unwrap() < startup.find("op.await").unwrap());
}

#[test]
fn windows_server_effects_serialize_with_replacement() {
    let source = include_str!("../src/drivers/windows/mod.rs");
    for (start, end) in [
        (
            "async fn set_characteristic_value(",
            "async fn set_advertising_enabled(",
        ),
        ("async fn notify(", "async fn notify_peer("),
        ("async fn notify_peer(", "async fn disconnect_peer("),
        ("async fn disconnect_peer(", "async fn notify_max_len("),
    ] {
        assert!(
            body(source, start, end).contains("self.inner.server_lock.lock().await"),
            "{start}"
        );
    }
    let source = include_str!("../src/drivers/windows/peripheral.rs");
    let note = body(
        source,
        "pub(super) fn note_server_peer(",
        "fn server_peer_closed(",
    );
    assert!(note.contains("if peer.excluded"));
}

#[test]
fn windows_close_releases_owned_central_resources() {
    let source = include_str!("../src/drivers/windows/mod.rs");
    let close = body(source, "async fn close(", "async fn notify(");
    assert!(close.contains("central_cleanup"));
    assert!(close.contains("drain()"));
    let device = include_str!("../src/drivers/windows/device.rs");
    assert!(device.contains("central_cleanup.lock().unwrap().insert(session"));
    assert!(device.contains("central_cleanup.lock().unwrap().remove(&self.session)"));
    assert!(device.contains(
        "priority_request: Arc<StdMutex<Option<BluetoothLEPreferredConnectionParametersRequest>>>"
    ));
    let subscribe = body(device, "async fn subscribe(", "async fn disconnect(");
    assert!(subscribe.contains("let links = self.inner.central_links.lock().unwrap();"));
    assert!(
        subscribe.find("let links =").unwrap()
            < subscribe
                .find("self.subscriptions.lock().unwrap().push")
                .unwrap()
    );
    assert!(
        subscribe
            .find("self.subscriptions.lock().unwrap().push")
            .unwrap()
            < subscribe.find("drop(links)").unwrap()
    );
}

#[test]
fn windows_server_callbacks_reject_replaced_generations() {
    let source = include_str!("../src/drivers/windows/peripheral.rs");
    let write = body(
        source,
        "async fn handle_write(",
        "pub(super) async fn notify_client(",
    );
    assert!(write.contains("inner.server_generation.lock().unwrap()"));
    assert!(write.contains("*current != generation"));
    let note = body(
        source,
        "pub(super) fn note_server_peer(",
        "fn server_peer_closed(",
    );
    assert!(note.contains("*current != generation"));
    let drop = body(source, "impl Drop for Server", "impl Server {");
    assert!(drop.contains("*current == self.generation"));
}

#[test]
fn metadata_updates_preserve_hidden_visibility() {
    let linux = include_str!("../src/drivers/linux.rs");
    let update = body(
        linux,
        "async fn update_advertisement(",
        "async fn set_advertising_enabled(",
    );
    assert!(update.contains("if adv_handle.is_none()"));
    let windows = include_str!("../src/drivers/windows/mod.rs");
    let update = body(
        windows,
        "async fn update_advertisement(",
        "fn served_service(",
    );
    assert!(update.contains("if desired_visibility"));
    assert!(!update.contains("provider.AdvertisementStatus()"));
    let start = include_str!("../src/drivers/windows/peripheral.rs");
    assert!(start.contains("advertising_start::confirm"));
    let kotlin = include_str!(
        "../../tauri-plugin-ble-gatt/android/src/main/kotlin/dev/blegatt/BleGattBridge.kt"
    );
    let update = body(
        kotlin,
        "fun updateAdvertising(",
        "private fun beginAdvertising(",
    );
    assert!(update.contains("if (advertiseCallback == null)"));
}

#[test]
fn windows_scan_filter_has_the_complete_truth_table() {
    let source = include_str!("../src/drivers/windows/watcher.rs");
    assert!(
        source.contains("if !(matches_service || is_scan_response && seen.contains_key(&address))")
    );
    for matches_service in [false, true] {
        for is_scan_response in [false, true] {
            for known in [false, true] {
                let missing_service = !matches_service;
                let not_known_scan_response = !(is_scan_response && known);
                let old_rejection = missing_service && not_known_scan_response;
                let new_rejection = !(matches_service || is_scan_response && known);
                assert_eq!(old_rejection, new_rejection);
            }
        }
    }
}

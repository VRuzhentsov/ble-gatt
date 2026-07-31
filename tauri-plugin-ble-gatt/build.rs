const COMMANDS: &[&str] = &[
    "ble_capabilities",
    "ble_advertise",
    "ble_stop_advertising",
    "ble_notify",
    "ble_scan_once",
    "ble_connect",
    "ble_read",
    "ble_write",
    "ble_disconnect",
    "ble_connection_mtu",
    "ble_watch_events",
];

fn main() {
    tauri_plugin::Builder::new(COMMANDS).android_path("android").build();
}

//! Real-BlueZ verification for the Linux backend. `#[ignore]`d by default —
//! run explicitly with `cargo test --test linux_loopback -- --ignored`, and
//! only on a Linux machine with `bluetoothd` running and a powered adapter.
//!
//! A single physical radio cannot be its own peer over the air, so this is
//! honestly split into two tiers rather than pretending a single-adapter
//! machine can prove peer-to-peer connectivity:
//!
//! - `peripheral_advertise_and_serve_smoke_test` — real, runs on this
//!   machine's one adapter (`88:D8:2E:BA:72:27` at plan time). Proves the
//!   `Application`/`Advertisement` D-Bus registration this crate builds is
//!   accepted by the real `org.bluez` daemon, not just internally
//!   consistent Rust.
//! - `two_adapter_central_to_peripheral_round_trip` — the true loopback the
//!   plan calls for (one `LinuxBackend` as central, a second as
//!   peripheral, over real BLE). `#[ignore]`d with a reason explaining the
//!   missing second adapter — a second Bluetooth controller (a USB dongle)
//!   or a second Linux machine is required; this is the documented,
//!   non-blocking follow-up the plan already anticipated for real-hardware
//!   verification.

use std::time::Duration;

use ble_gatt::backend::linux::LinuxBackend;
use ble_gatt::{Backend, CharacteristicUuid, GattCharacteristicSpec, GattServiceSpec, ServiceUuid};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a real, powered BlueZ adapter"]
async fn peripheral_advertise_and_serve_smoke_test() {
    let backend = LinuxBackend::new().await.expect("bring up BlueZ session + default adapter");

    let capabilities = backend.capabilities().await;
    assert!(capabilities.central, "BlueZ adapters are always capable of the central role");

    let service = GattServiceSpec {
        uuid: ServiceUuid(Uuid::new_v4()),
        characteristics: vec![GattCharacteristicSpec {
            uuid: CharacteristicUuid(Uuid::new_v4()),
            readable: true,
            writable: true,
            notifiable: true,
            initial_value: b"ble-gatt smoke test".to_vec(),
        }],
    };

    backend
        .advertise(service)
        .await
        .expect("real BlueZ daemon should accept the GATT application + advertisement");

    // Give BlueZ a moment to actually start broadcasting before tearing
    // down, so this is verifiable with `bluetoothctl scan on` run manually
    // alongside `cargo test -- --ignored --nocapture` if desired.
    tokio::time::sleep(Duration::from_secs(2)).await;

    backend.stop_advertising().await.expect("stop_advertising should succeed");
}

#[tokio::test]
#[ignore = "requires two independent Bluetooth adapters (e.g. a USB dongle) or two machines — \
            see the module doc comment; this machine currently has one adapter"]
async fn two_adapter_central_to_peripheral_round_trip() {
    unimplemented!(
        "wire this up once a second adapter is available: bring up two LinuxBackends \
         bound to different adapter names via bluer::Session::adapter, advertise a test \
         service on one, scan+connect+read/write/notify from the other"
    );
}

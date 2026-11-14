//! The GATT advertisement (ADR-0012) over the mock radio: a peer whose
//! radio advertisement carries no data, as a Windows or Apple peer's does,
//! is completed from its GATT advertisement; a peer that advertises its
//! data is not read at all.

use std::sync::Arc;

use ble_gatt::advertisement::{self, GattAdvertisementPolicy, GATT_ADVERTISEMENT_CHARACTERISTIC};
use ble_gatt::datagram::{self, DatagramConfig};
use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{Adapter, CapabilityReport, CharacteristicUuid, Driver, PeerAddress, ServiceUuid};
use tokio_stream::StreamExt;
use uuid::Uuid;

const COMPANY: u16 = 0xFFFF;

fn both_roles() -> CapabilityReport {
    CapabilityReport { central: true, peripheral: true }
}

fn config(flag: u8) -> DatagramConfig {
    let mut config = DatagramConfig::new(
        ServiceUuid(Uuid::from_u128(0xb1e6a000_f101_4000_8000_00805f9b34fb)),
        CharacteristicUuid(Uuid::from_u128(0xb1e6a001_f101_4000_8000_00805f9b34fb)),
    );
    config.advertised_manufacturer_data.insert(COMPANY, vec![flag, 0xAA, 0xBB, 0xCC, 0xDD]);
    config.advertised_local_name = Some("Desktop".into());
    config
}

fn driver(network: &Arc<MockNetwork>, name: &str) -> Arc<MockDriver> {
    Arc::new(MockDriver::new(PeerAddress(name.into()), network.clone(), both_roles()))
}

#[tokio::test]
async fn a_peer_without_advertised_data_is_completed_from_its_gatt_advertisement() {
    let network = MockNetwork::new();
    let windows = driver(&network, "windows");
    let phone = driver(&network, "phone");
    let config = config(0x01);

    // What a Windows peer sends: the service UUID and nothing else on the
    // radio, the whole advertisement data in the GATT advertisement.
    let mut spec = config.service_spec();
    spec.manufacturer_data.clear();
    spec.local_name = None;
    windows.advertise(spec).await.unwrap();

    let scan = phone.scan(config.service).await.unwrap();
    let mut peers = advertisement::complete(phone.clone(), config.service, scan, GattAdvertisementPolicy::default());
    let peer = peers.next().await.expect("a peer").expect("no scan error");
    assert_eq!(peer.address, PeerAddress("windows".into()));
    assert_eq!(peer.manufacturer_data.get(&COMPANY), Some(&vec![0x01, 0xAA, 0xBB, 0xCC, 0xDD]));
    assert_eq!(peer.name.as_deref(), Some("Desktop"));
    assert!(peers.next().await.is_none(), "the scan ended and the peer was read once");
}

#[tokio::test]
async fn a_peer_that_advertises_its_data_is_not_read() {
    let network = MockNetwork::new();
    let linux = driver(&network, "linux");
    let phone = driver(&network, "phone");
    let config = config(0x00);
    // Served without the GATT advertisement characteristic: a read would fail.
    let mut spec = config.service_spec();
    spec.characteristics.retain(|c| c.uuid != GATT_ADVERTISEMENT_CHARACTERISTIC);
    linux.advertise(spec).await.unwrap();

    let scan = phone.scan(config.service).await.unwrap();
    let mut peers = advertisement::complete(phone.clone(), config.service, scan, GattAdvertisementPolicy::default());
    let peer = peers.next().await.expect("a peer").expect("no scan error");
    assert_eq!(peer.manufacturer_data.get(&COMPANY).map(|v| v[0]), Some(0x00));
}

#[tokio::test]
async fn an_advertisement_update_changes_the_gatt_advertisement() {
    let network = MockNetwork::new();
    let windows = driver(&network, "windows");
    let phone = driver(&network, "phone");
    let adapter = Adapter::new(windows.clone()).await;

    let _channels = datagram::serve(&adapter, &config(0x00)).await.unwrap();
    let before = advertisement::read(phone.as_ref(), &PeerAddress("windows".into())).await.unwrap();
    assert_eq!(before.manufacturer_data.get(&COMPANY).map(|v| v[0]), Some(0x00));

    datagram::update_advertisement(&adapter, &config(0x01)).await.unwrap();
    let after = advertisement::read(phone.as_ref(), &PeerAddress("windows".into())).await.unwrap();
    assert_eq!(after.manufacturer_data.get(&COMPANY).map(|v| v[0]), Some(0x01), "the Add Device flag changed");
    assert_eq!(after.local_name.as_deref(), Some("Desktop"));
}

/// Dropping the completed stream must end the scan it wraps at once. The
/// Linux driver stops BlueZ discovery only when its scan stream is dropped,
/// so a forwarding task that waits for the next advertisement first keeps
/// discovery on until some peer happens to advertise.
#[tokio::test]
async fn dropping_the_completed_stream_ends_the_scan_at_once() {
    let network = MockNetwork::new();
    let phone = driver(&network, "phone");
    let config = config(0x01);

    // A scan that never reports a peer, and that says when it is dropped.
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let scan = Box::pin(futures::stream::poll_fn(move |_| {
        let _held_until_drop = &dropped_tx;
        std::task::Poll::Pending
    }));

    let peers = advertisement::complete(phone.clone(), config.service, scan, GattAdvertisementPolicy::default());
    // Let the forwarding task reach its wait for the next advertisement.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(peers);

    let ended = tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx).await;
    assert!(ended.is_ok(), "the scan was still held after the completed stream was dropped");
}

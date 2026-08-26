//! Wire-parity tests for the `mock-broker` feature: the exact same
//! scenarios `tests/mock_protocol.rs` exercises against an in-process
//! `MockNetwork::new()`, replayed against a real socket connection to a
//! `MockNetwork::serve()` broker — the strongest available evidence that
//! `Radio::Remote` behaves identically to `Radio::Local`, since it's the
//! identical assertions against a different transport. See
//! docs/adr/0004-mock-broker-for-cross-process-e2e.md.

use std::sync::Arc;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::{
    Backend, CapabilityReport, CharacteristicUuid, GattCharacteristicSpec, GattEvent,
    GattServiceSpec, PeerAddress, ServiceUuid,
};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use uuid::Uuid;

fn full_capabilities() -> CapabilityReport {
    CapabilityReport { central: true, peripheral: true }
}

/// Binds an OS-assigned loopback port (avoids CI collisions), spawns the
/// broker loop against it, and returns two independent `MockNetwork`
/// connections to it — standing in for fini's two real OS processes, which
/// is out of scope for ble-gatt itself to prove (see the plan).
async fn broker_and_two_clients() -> (Arc<MockNetwork>, Arc<MockNetwork>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let _ = MockNetwork::serve(listener).await;
    });
    let a = MockNetwork::remote(addr).await.expect("connect client a");
    let b = MockNetwork::remote(addr).await.expect("connect client b");
    (a, b)
}

#[tokio::test]
async fn central_discovers_and_reads_the_peripherals_advertised_service() {
    let (peripheral_network, central_network) = broker_and_two_clients().await;
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());

    let peripheral = MockBackend::new(PeerAddress("peripheral-1".to_string()), peripheral_network, full_capabilities());
    peripheral
        .advertise(GattServiceSpec::new(
            service_uuid,
            vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: b"hello".to_vec(),
            }],
        ))
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-1".to_string()), central_network, full_capabilities());

    let mut discovered = central.scan(service_uuid).await.expect("scan should succeed");
    let peer = discovered
        .next()
        .await
        .expect("peripheral should be discovered")
        .expect("discovery should not error");
    assert_eq!(peer.address, PeerAddress("peripheral-1".to_string()));

    let mut connection = central.connect(&peer.address).await.expect("connect should succeed");
    let value = connection.read(characteristic_uuid).await.expect("read should succeed");
    assert_eq!(value, b"hello");
}

#[tokio::test]
async fn write_from_central_is_readable_back_and_fires_a_lifecycle_event() {
    let (peripheral_network, central_network) = broker_and_two_clients().await;
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-2".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), peripheral_network, full_capabilities());
    peripheral
        .advertise(GattServiceSpec::new(
            service_uuid,
            vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: true,
                writable: true,
                notifiable: false,
                initial_value: Vec::new(),
            }],
        ))
        .await
        .expect("advertise should succeed");
    let mut events = peripheral.events();

    let central = MockBackend::new(PeerAddress("central-2".to_string()), central_network, full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");

    connection.write(characteristic_uuid, b"updated".to_vec()).await.expect("write should succeed");

    let written_event = loop {
        match events.next().await.expect("write event should arrive") {
            GattEvent::Connected { .. } => continue,
            other => break other,
        }
    };
    match written_event {
        GattEvent::CharacteristicWritten { characteristic, value, .. } => {
            assert_eq!(characteristic, characteristic_uuid);
            assert_eq!(value, b"updated");
        }
        other => panic!("expected CharacteristicWritten, got {other:?}"),
    }

    let read_back = connection.read(characteristic_uuid).await.expect("read should succeed");
    assert_eq!(read_back, b"updated");
}

#[tokio::test]
async fn server_initiated_notify_is_delivered_to_a_subscribed_central() {
    let (peripheral_network, central_network) = broker_and_two_clients().await;
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-3".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), peripheral_network, full_capabilities());
    peripheral
        .advertise(GattServiceSpec::new(
            service_uuid,
            vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: false,
                writable: false,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        ))
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-3".to_string()), central_network, full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");
    let mut notifications = connection.subscribe(characteristic_uuid).await.expect("subscribe should succeed");

    peripheral.notify(characteristic_uuid, b"push".to_vec()).await.expect("notify should succeed");

    let received = notifications.next().await.expect("notification should arrive").expect("no gap");
    assert_eq!(received, b"push");
}

/// Broker-specific: no `Local` analog. Kills one client's connection mid-
/// session while the survivor holds a live subscription, and asserts the
/// survivor observes a `Disconnected` — the actors-harness killing a
/// process is exactly the scenario `MockNetwork::serve`'s connection-drop
/// sweep exists to survive.
///
/// The central must actually *subscribe* (not just `connect`) for this to
/// observe anything: `stop_advertising`'s teardown — which the sweep reuses
/// verbatim, see `LocalRadio::disconnect_all_for` — only notifies a
/// peripheral's *subscribers*, exactly like today's in-process behavior.
#[tokio::test]
async fn killing_a_clients_connection_tells_the_survivor_it_disconnected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let _ = MockNetwork::serve(listener).await;
    });

    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-killed".to_string());
    let central_addr = PeerAddress("central-survivor".to_string());

    let peripheral_network = MockNetwork::remote(addr).await.expect("connect peripheral");
    let peripheral = MockBackend::new(peripheral_addr.clone(), peripheral_network, full_capabilities());
    peripheral
        .advertise(GattServiceSpec::new(
            service_uuid,
            vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: false,
                writable: false,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        ))
        .await
        .expect("advertise should succeed");

    let central_network = MockNetwork::remote(addr).await.expect("connect central");
    let central = MockBackend::new(central_addr.clone(), central_network, full_capabilities());
    let mut events = central.events();
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");
    let connected = events.next().await.expect("connected event");
    assert!(matches!(connected, GattEvent::Connected { .. }));
    // Subscribing (not just connecting) is what makes this central a
    // "served" peer in `stop_advertising`'s eyes — exactly today's
    // in-process rule, unchanged by the broker. `subscribe`'s own
    // `Connected` event is addressed to the *peripheral*, not this central,
    // so nothing further to drain here.
    let _notifications = connection.subscribe(characteristic_uuid).await.expect("subscribe should succeed");

    // Now kill the peripheral's connection. Dropping every `Arc<MockNetwork>`
    // for this client drops its `RemoteClient`, whose `Drop` impl aborts its
    // reader/writer tasks and closes the socket — standing in for the
    // actors-harness killing this process outright, which the OS would do
    // by closing every fd.
    drop(peripheral);

    let lost = tokio::time::timeout(std::time::Duration::from_secs(5), events.next())
        .await
        .expect("disconnect event should arrive after the peripheral's connection is dropped")
        .expect("disconnect event should reach the central");
    match lost {
        GattEvent::Disconnected { peer, .. } => assert_eq!(peer, peripheral_addr),
        other => panic!("expected Disconnected, got {other:?}"),
    }
}

/// Regression test for a P1 review finding on `RemoteClient`'s
/// reader-loop cleanup: it resolved every pending `call()` when the
/// connection died, but left live `subscribe()` senders untouched, so a
/// subscriber's notify stream just hung forever instead of ending —
/// exactly what would leave `DatagramChannel::recv()` waiting indefinitely
/// after the broker connection is gone.
///
/// This kills the *subscribing* client's own connection to the broker
/// (central), not the peripheral's — the path the fix's `subscriptions.
/// lock().unwrap().clear()` covers, distinct from
/// `killing_a_clients_connection_tells_the_survivor_it_disconnected` above,
/// which exercises the broker's connection-drop sweep instead.
#[tokio::test]
async fn a_clients_own_connection_dying_ends_its_subscription_stream_instead_of_hanging() {
    let (peripheral_network, central_network) = broker_and_two_clients().await;
    let service_uuid = ServiceUuid(Uuid::new_v4());
    let characteristic_uuid = CharacteristicUuid(Uuid::new_v4());
    let peripheral_addr = PeerAddress("peripheral-central-dies".to_string());

    let peripheral = MockBackend::new(peripheral_addr.clone(), peripheral_network, full_capabilities());
    peripheral
        .advertise(GattServiceSpec::new(
            service_uuid,
            vec![GattCharacteristicSpec {
                uuid: characteristic_uuid,
                readable: false,
                writable: false,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        ))
        .await
        .expect("advertise should succeed");

    let central = MockBackend::new(PeerAddress("central-dies".to_string()), central_network, full_capabilities());
    let mut connection = central.connect(&peripheral_addr).await.expect("connect should succeed");
    let mut notifications = connection.subscribe(characteristic_uuid).await.expect("subscribe should succeed");

    // Drop every `Arc<MockNetwork>` reference this central holds -- both
    // `central` and `connection` clone it -- so `RemoteClient::drop` aborts
    // its reader/writer tasks and closes this central's own socket.
    drop(central);
    drop(connection);

    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), notifications.next())
        .await
        .expect("the subscription stream must end once this client's own connection dies, not hang forever");
    assert!(ended.is_none(), "expected the stream to end with None, got {ended:?}");
}

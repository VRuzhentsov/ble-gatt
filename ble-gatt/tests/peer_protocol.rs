//! Managed peer protocol tests. No iroh, Tauri, or hardware is required.
use ble_gatt::{
    datagram::DatagramConfig,
    drivers::mock::{MockDriver, MockNetwork},
    CapabilityReport, CharacteristicUuid, PeerAddress, PeerLink, PeerLinkConfig, ServiceUuid,
};
use std::{sync::Arc, time::Duration};
use tokio_stream::StreamExt;
use uuid::Uuid;

fn config() -> PeerLinkConfig {
    PeerLinkConfig::new(DatagramConfig::new(
        ServiceUuid(Uuid::from_u128(11)),
        CharacteristicUuid(Uuid::from_u128(12)),
    ))
}

fn pair() -> (Arc<PeerLink>, Arc<PeerLink>) {
    let network = MockNetwork::new();
    let make = |address: &str| {
        PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress(address.into()),
                network.clone(),
                CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            )),
            config(),
        )
    };
    (make("a"), make("b"))
}

#[tokio::test]
async fn managed_discovery_reports_endpoint_and_stopping_scan_preserves_traffic() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (a, b) = pair();
        b.set_advertising(true).await.unwrap();
        let mut discovered = a.discover().await.unwrap();
        let (endpoint, peer) = discovered.next().await.unwrap().unwrap();
        assert_eq!(endpoint, b.endpoint_id());
        assert_eq!(peer.address, PeerAddress("b".into()));
        let mut incoming = b.incoming().unwrap();
        let outgoing = a.request(&peer.address).await.unwrap();
        let request = incoming.next().await.unwrap();
        let (left, right) = tokio::join!(outgoing.accept(), request.accept());
        let (left, right) = (left.unwrap(), right.unwrap());
        drop(discovered);
        left.send(b"scan stopped".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap().unwrap(), b"scan stopped");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_local_request_fails_without_replacing_pending_attempt() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (a, b) = pair();
        let mut incoming = b.incoming().unwrap();
        b.set_advertising(true).await.unwrap();
        let peer = PeerAddress("b".into());
        let first = a.request(&peer).await.unwrap();
        let second = a.request(&peer).await;
        assert!(
            matches!(second, Err(ble_gatt::BleError::AdapterBusy(_))),
            "{second:?}"
        );
        let remote = incoming.next().await.unwrap();
        let (left, right) = tokio::join!(first.accept(), remote.accept());
        left.unwrap().send(b"not replaced".to_vec()).await.unwrap();
        assert_eq!(
            right.unwrap().recv().await.unwrap().unwrap(),
            b"not replaced"
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn discovery_completes_missing_radio_metadata_without_touching_active_session() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let scanner = Arc::new(MockDriver::new(
            PeerAddress("a".into()),
            network.clone(),
            caps,
        ));
        let advertiser = Arc::new(MockDriver::new(
            PeerAddress("b".into()),
            network.clone(),
            caps,
        ));
        let a = PeerLink::with_driver(scanner.clone(), config());
        let b = PeerLink::with_driver(advertiser, config());
        let mut incoming = b.incoming().unwrap();
        b.set_advertising(true).await.unwrap();
        network.set_advertisement_data(
            &PeerAddress("b".into()),
            Default::default(),
            Default::default(),
            None,
        );
        let mut scan = a.discover().await.unwrap();
        let (endpoint, peer) = scan.next().await.unwrap().unwrap();
        assert_eq!(endpoint, b.endpoint_id());
        drop(scan);
        let outgoing = a.request(&peer.address).await.unwrap();
        // Reuse the validated pending attempt; a metadata probe must not replace its native session.
        let mut pending_scan = a.discover().await.unwrap();
        assert_eq!(
            pending_scan.next().await.unwrap().unwrap().0,
            b.endpoint_id()
        );
        drop(pending_scan);
        let (left, right) =
            tokio::join!(outgoing.accept(), incoming.next().await.unwrap().accept());
        let left = left.unwrap();
        let right = right.unwrap();
        let mut scan = a.discover().await.unwrap();
        assert_eq!(scan.next().await.unwrap().unwrap().0, b.endpoint_id());
        left.send(b"not probed".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap().unwrap(), b"not probed");
        drop(scan);
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejection_before_accept_retains_its_typed_attempt_outcome() {
    let (a, b) = pair();
    let mut incoming = b.incoming().unwrap();
    b.set_advertising(true).await.unwrap();
    let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
    incoming.next().await.unwrap().reject().await.unwrap();
    let result = outgoing.accept().await;
    assert!(
        matches!(
            result,
            Err(ble_gatt::BleError::Attempt(
                ble_gatt::AttemptError::Rejected
            ))
        ),
        "{result:?}"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn rejection_is_a_typed_attempt_outcome() {
    let (a, b) = pair();
    let mut incoming = b.incoming().unwrap();
    b.set_advertising(true).await.unwrap();
    let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
    let remote = incoming.next().await.unwrap();
    let (result, rejected) = tokio::join!(outgoing.accept(), remote.reject());
    rejected.unwrap();
    assert!(
        matches!(
            result,
            Err(ble_gatt::BleError::Attempt(
                ble_gatt::AttemptError::Rejected
            ))
        ),
        "{result:?}"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn both_accept_before_messages_can_flow() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (a, b) = pair();
        let mut incoming = b.incoming().unwrap();
        b.set_advertising(true).await.unwrap();
        let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
        assert_eq!(outgoing.endpoint_id(), b.endpoint_id());
        assert!(a.channel(&PeerAddress("b".into())).is_none());
        let request = incoming.next().await.unwrap();
        assert_eq!(request.endpoint_id(), a.endpoint_id());
        let (left, right) = tokio::join!(outgoing.accept(), request.accept());
        let (left, right) = (left.unwrap(), right.unwrap());
        left.send(b"hello".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap().unwrap(), b"hello");
        right.send(b"reply".to_vec()).await.unwrap();
        assert_eq!(left.recv().await.unwrap().unwrap(), b"reply");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

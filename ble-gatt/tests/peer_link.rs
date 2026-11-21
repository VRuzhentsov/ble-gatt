//! Scoped managed channel tests replace the removed tracking/retry-mode contract.
//! Raw driver and datagram regression tests remain separate.
use ble_gatt::{
    datagram::DatagramConfig,
    drivers::mock::{MockDriver, MockNetwork},
    CapabilityReport, CharacteristicUuid, PeerAddress, PeerChannel, PeerLink, PeerLinkConfig,
    ServiceUuid,
};
use std::{sync::Arc, time::Duration};
use tokio_stream::StreamExt;
use uuid::Uuid;
fn pair() -> (Arc<PeerLink>, Arc<PeerLink>) {
    let (a, b, _) = pair_network();
    (a, b)
}
fn pair_network() -> (Arc<PeerLink>, Arc<PeerLink>, Arc<MockNetwork>) {
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
            PeerLinkConfig::new(DatagramConfig::new(
                ServiceUuid(Uuid::from_u128(11)),
                CharacteristicUuid(Uuid::from_u128(12)),
            )),
        )
    };
    (make("a"), make("b"), network)
}
async fn establish(a: &PeerLink, b: &PeerLink) -> (PeerChannel, PeerChannel) {
    let mut incoming = b.incoming().unwrap();
    b.set_advertising(true).await.unwrap();
    let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
    let request = incoming.next().await.unwrap();
    let (left, right) = tokio::join!(outgoing.accept(), request.accept());
    (left.unwrap(), right.unwrap())
}
#[tokio::test]
async fn visibility_toggle_preserves_established_messages() {
    let (a, b) = pair();
    let (left, right) = establish(&a, &b).await;
    b.set_advertising(false).await.unwrap();
    left.send(b"hidden".to_vec()).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), b"hidden");
    b.set_advertising(true).await.unwrap();
    right.send(b"visible".to_vec()).await.unwrap();
    assert_eq!(left.recv().await.unwrap().unwrap(), b"visible");
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn explicit_disconnect_records_terminal_reasons_before_receive_closes() {
    let (a, b) = pair();
    let (left, right) = establish(&a, &b).await;
    assert_eq!(left.close_reason(), None);
    left.disconnect().await.unwrap();
    assert!(right.recv().await.is_none());
    assert_eq!(
        left.close_reason(),
        Some(ble_gatt::PeerChannelCloseReason::LocalDisconnect)
    );
    assert_eq!(
        right.close_reason(),
        Some(ble_gatt::PeerChannelCloseReason::PeerDisconnect)
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn stale_channel_disconnect_cannot_disconnect_replacement() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (a, b) = pair();
        let mut incoming = b.incoming().unwrap();
        b.set_advertising(true).await.unwrap();
        let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
        let (left, right) =
            tokio::join!(outgoing.accept(), incoming.next().await.unwrap().accept());
        let stale = left.unwrap();
        let right = right.unwrap();
        a.disconnect(b.endpoint_id()).await.unwrap();
        assert!(right.recv().await.is_none());
        let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
        let (left, right) =
            tokio::join!(outgoing.accept(), incoming.next().await.unwrap().accept());
        let left = left.unwrap();
        let right = right.unwrap();
        stale.disconnect().await.unwrap();
        left.send(b"replacement".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap().unwrap(), b"replacement");
        a.disconnect(b.endpoint_id()).await.unwrap();
        assert!(left.send(vec![1]).await.is_err());
        assert!(a.channel(&PeerAddress("b".into())).is_none());
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_last_use_closes_remote_receive() {
    let (a, b) = pair();
    let (left, right) = establish(&a, &b).await;
    drop(left);
    assert!(tokio::time::timeout(Duration::from_secs(1), right.recv())
        .await
        .unwrap()
        .is_none());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_acquisition_and_releases_linearize_last_use() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..32 {
            let (a, b) = pair();
            let (left, right) = establish(&a, &b).await;
            let clone = left.clone();
            let acquire = {
                let a = a.clone();
                tokio::spawn(async move { a.channel(&PeerAddress("b".into())) })
            };
            let release = tokio::spawn(async move { left.release().await });
            clone.release().await.unwrap();
            release.await.unwrap().unwrap();
            if let Some(acquired) = acquire.await.unwrap() {
                acquired.send(b"concurrent use".to_vec()).await.unwrap();
                assert_eq!(right.recv().await.unwrap().unwrap(), b"concurrent use");
                acquired.release().await.unwrap();
            }
            assert!(a.channel(&PeerAddress("b".into())).is_none());
            assert!(right.recv().await.is_none());
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn healthy_idle_channel_survives_the_whole_loss_interval() {
    let (a, b) = pair();
    let (left, right) = establish(&a, &b).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    left.send(b"still idle healthy".to_vec()).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), b"still idle healthy");
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn stale_request_cancellation_cannot_close_the_next_generation() {
    let _ = env_logger::builder().is_test(true).try_init();
    tokio::time::timeout(Duration::from_secs(5), async {
        let (a, b) = pair();
        let mut incoming = b.incoming().unwrap();
        b.set_advertising(true).await.unwrap();
        let stale = a.request(&PeerAddress("b".into())).await.unwrap();
        let stale_remote = incoming.next().await.unwrap();
        a.disconnect(b.endpoint_id()).await.unwrap();
        let outgoing = a.request(&PeerAddress("b".into())).await.unwrap();
        let remote = incoming.next().await.unwrap();
        let (left, right) = tokio::join!(outgoing.accept(), remote.accept());
        let left = left.unwrap();
        let right = right.unwrap();
        drop(stale);
        drop(stale_remote);
        tokio::time::sleep(Duration::from_millis(100)).await;
        left.send(b"new generation".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap().unwrap(), b"new generation");
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn last_release_awaits_observed_native_disconnect() {
    use ble_gatt::Driver;
    use futures::FutureExt;
    let network = MockNetwork::new();
    let caps = CapabilityReport {
        central: true,
        peripheral: true,
    };
    let central = Arc::new(MockDriver::new(
        PeerAddress("a".into()),
        network.clone(),
        caps,
    ));
    let make_config = || {
        PeerLinkConfig::new(DatagramConfig::new(
            ServiceUuid(Uuid::from_u128(11)),
            CharacteristicUuid(Uuid::from_u128(12)),
        ))
    };
    let a = PeerLink::with_driver(central.clone(), make_config());
    let b = PeerLink::with_driver(
        Arc::new(MockDriver::new(PeerAddress("b".into()), network, caps)),
        make_config(),
    );
    let mut events = central.events();
    let (left, right) = establish(&a, &b).await;
    left.release().await.unwrap();
    let mut disconnected = false;
    while let Some(Some(event)) = events.next().now_or_never() {
        if matches!(event, ble_gatt::GattEvent::Disconnected { .. }) {
            disconnected = true;
        }
    }
    assert!(
        disconnected,
        "release returned before any native disconnect effect"
    );
    assert!(right.recv().await.is_none());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_accept_cancels_only_that_attempt() {
    let (a, b) = pair();
    let mut incoming = b.incoming().unwrap();
    b.set_advertising(true).await.unwrap();
    let request = a.request(&PeerAddress("b".into())).await.unwrap();
    let remote = incoming.next().await.unwrap();
    let accept = tokio::spawn(request.accept());
    tokio::time::sleep(Duration::from_millis(20)).await;
    accept.abort();
    let _ = accept.await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(remote.accept().await.is_err());
    let request = a.request(&PeerAddress("b".into())).await.unwrap();
    let (left, right) = tokio::join!(request.accept(), incoming.next().await.unwrap().accept());
    let left = left.unwrap();
    let right = right.unwrap();
    left.send(b"fresh attempt".to_vec()).await.unwrap();
    assert_eq!(right.recv().await.unwrap().unwrap(), b"fresh attempt");
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn last_scoped_release_retires_generation_but_one_release_preserves_other_uses() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (a, b) = pair();
        let (left, right) = establish(&a, &b).await;
        let clone = left.clone();
        let acquired = a.channel(&PeerAddress("b".into())).unwrap();
        left.release().await.unwrap();
        clone.send(b"shared".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap().unwrap(), b"shared");
        clone.release().await.unwrap();
        assert!(a.channel(&PeerAddress("b".into())).is_some());
        acquired.release().await.unwrap();
        assert!(a.channel(&PeerAddress("b".into())).is_none());
        assert!(tokio::time::timeout(Duration::from_secs(1), right.recv())
            .await
            .unwrap()
            .is_none());
        a.close().await.unwrap();
        b.close().await.unwrap();
    })
    .await
    .unwrap();
}

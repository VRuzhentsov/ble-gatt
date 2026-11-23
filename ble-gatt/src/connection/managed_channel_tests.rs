//! Managed channel data: wire records, admission and receive bounds.
use super::*;

#[test]
fn records_reject_truncation_extensions_unsupported_versions_and_oversize_data() {
    let records = [
        Record::Request {
            sender: EndpointId::from_bytes([1; 16]),
            target: EndpointId::from_bytes([2; 16]),
            attempt: u64::MAX,
            nonce: i32::MIN,
        },
        Record::Response {
            sender: EndpointId::from_bytes([2; 16]),
            target: EndpointId::from_bytes([1; 16]),
            attempt: u64::MAX,
        },
        Record::Decision {
            attempt: 0,
            action: 2,
        },
        Record::Disconnect { attempt: 1 },
        Record::Ping { attempt: 1 },
        Record::Pong { attempt: 1 },
    ];
    for record in records {
        let mut bytes = record.encode();
        assert_eq!(Record::decode(&bytes, 4).unwrap(), record);
        for length in 0..bytes.len() {
            assert!(Record::decode(&bytes[..length], 4).is_err());
        }
        bytes.push(0);
        assert!(Record::decode(&bytes, 4).is_err());
    }
    let mut request = Record::Request {
        sender: EndpointId::from_bytes([1; 16]),
        target: EndpointId::from_bytes([2; 16]),
        attempt: 1,
        nonce: i32::MAX,
    }
    .encode();
    request[1] = 2;
    assert!(Record::decode(&request, 4).is_err());
    assert!(Record::decode(
        &Record::Data {
            attempt: 1,
            payload: vec![0; 5]
        }
        .encode(),
        4
    )
    .is_err());
    assert!(Record::decode(&[255; 9], 4).is_err());
    for bytes in [vec![], vec![b'B', b'G', 0, 1], vec![0; 20]] {
        assert!(managed_codec::ManagedMetadata::decode(&bytes).is_err());
    }
}
#[tokio::test]
async fn data_before_acceptance_fails_closed() {
    let (managed, mut raw, request) = raw_request(config()).await;
    raw.send(
        Record::Data {
            attempt: 71,
            payload: vec![1],
        }
        .encode(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        request.accept().await,
        Err(crate::BleError::Attempt(crate::AttemptError::InvalidRecord))
    ));
    managed.close().await.unwrap();
}
#[tokio::test]
async fn a_stalled_bounded_receiver_resumes_without_losing_scheduled_data() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_millis(300), managed.set_advertising(false))
        .await
        .expect("stalled peer must not block control")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(channel.state.received.lock().await.len(), 1);
    for byte in 0..3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(300), channel.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            vec![byte]
        );
    }
    assert_eq!(channel.close_reason(), None);
    managed.close().await.unwrap();
}
#[tokio::test]
async fn native_end_interrupts_a_stalled_application_admission() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(channel.state.received.lock().await.len(), 1);
    raw.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_millis(300), channel.state.ended.cancelled())
        .await
        .expect("native loss must interrupt stalled admission");
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::LinkLost)
    );
    managed.close().await.unwrap();
}
#[tokio::test]
async fn a_stalled_bounded_receiver_reports_gap_without_ending_generation() {
    let mut config = config();
    config.inbound_queue_depth = 1;
    let (managed, mut raw, channel) = raw_established(config).await;
    for byte in 0..3 {
        raw.send(
            Record::Data {
                attempt: 71,
                payload: vec![byte],
            }
            .encode(),
        )
        .await
        .unwrap();
    }
    // Even with the sole application slot full, lifecycle commands must progress.
    tokio::time::timeout(Duration::from_millis(300), managed.set_advertising(false))
        .await
        .expect("full application queue must not park the state owner")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(channel.state.received.lock().await.len(), 1);
    // Exercise the actual out-of-band loss flag independently of ordinary
    // application backpressure, which must no longer manufacture a gap.
    channel.state.overflow.raise();
    let gap = tokio::time::timeout(Duration::from_millis(300), channel.recv())
        .await
        .unwrap();
    assert!(
        matches!(gap, Some(Err(_))),
        "gap must precede buffered payload: {gap:?}"
    );
    assert_eq!(channel.close_reason(), None);
    for byte in 0..3 {
        assert_eq!(channel.recv().await.unwrap().unwrap(), vec![byte]);
    }
    raw.send(
        Record::Data {
            attempt: 71,
            payload: vec![9],
        }
        .encode(),
    )
    .await
    .unwrap();
    assert_eq!(channel.recv().await.unwrap().unwrap(), vec![9]);
    channel.send(vec![10]).await.unwrap();
    assert!(
        matches!(Record::decode(&raw.recv().await.unwrap().unwrap(), 128).unwrap(), Record::Data { payload, .. } if payload == vec![10])
    );
    managed.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiving_progresses_while_a_large_native_send_is_in_flight() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut config = config();
    config.inbound_queue_depth = 2;
    let (managed, mut raw, channel) = raw_established(config).await;
    let raw_sender = raw.sender();
    tokio::time::timeout(Duration::from_secs(5), async {
        let transmit = channel.send(vec![42; 128 * 1024]);
        let peer_transmit = async {
            for byte in 0..24 {
                raw_sender
                    .send(
                        Record::Data {
                            attempt: 71,
                            payload: vec![byte],
                        }
                        .encode(),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let receiver = channel.clone();
        let receive = tokio::spawn(async move {
            for byte in 0..24 {
                let item = receiver.recv().await;
                assert!(
                    item.is_some(),
                    "receive ended at {byte}: {:?}",
                    receiver.close_reason()
                );
                assert_eq!(item.unwrap().unwrap(), vec![byte]);
            }
        });
        let peer_receive = async {
            assert_eq!(
                Record::decode(&raw.recv().await.unwrap().unwrap(), 128 * 1024).unwrap(),
                Record::Data {
                    attempt: 71,
                    payload: vec![42; 128 * 1024]
                }
            );
        };
        let (sent, (), received, ()) = tokio::join!(transmit, peer_transmit, receive, peer_receive);
        sent.unwrap();
        received.unwrap();
    })
    .await
    .unwrap();
    managed.close().await.unwrap();
}

#[tokio::test]
async fn malformed_record_does_not_keep_a_channel_alive() {
    let (managed, mut raw, channel) = raw_established(config()).await;
    raw.send(vec![255]).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), channel.recv())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::ProtocolError)
    );
    managed.close().await.unwrap();
}
#[tokio::test]
async fn silent_peer_expires_at_the_loss_bound() {
    let (managed, _raw, channel) = raw_established(config()).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(32), channel.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::LinkLost)
    );
    managed.close().await.unwrap();
}
#[tokio::test]
async fn shutdown_records_reason_before_closing_receive() {
    let (managed, _raw, channel) = raw_established(config()).await;
    managed.close().await.unwrap();
    assert!(channel.recv().await.is_none());
    assert_eq!(
        channel.close_reason(),
        Some(PeerChannelCloseReason::Shutdown)
    );
}
#[tokio::test]
async fn raw_legacy_and_wrong_version_peers_never_enter_managed_incoming() {
    for legacy in [true, false] {
        let network = MockNetwork::new();
        let caps = CapabilityReport {
            central: true,
            peripheral: true,
        };
        let raw_driver = Arc::new(MockDriver::new(
            PeerAddress("raw".into()),
            network.clone(),
            caps,
        ));
        let managed = PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress("managed".into()),
                network,
                caps,
            )),
            PeerLinkConfig::new(config()),
        );
        let mut incoming = managed.incoming().unwrap();
        managed.set_advertising(true).await.unwrap();
        let mut raw =
            DatagramChannel::connect_driver(raw_driver, &PeerAddress("managed".into()), &config())
                .await
                .unwrap();
        let mut bytes = if legacy {
            b"legacy payload".to_vec()
        } else {
            Record::Request {
                sender: EndpointId::from_bytes([1; 16]),
                target: managed.endpoint_id(),
                attempt: 1,
                nonce: 0,
            }
            .encode()
        };
        if !legacy {
            bytes[1] = 2;
        }
        raw.send(bytes).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), incoming.next())
                .await
                .is_err()
        );
        managed.close().await.unwrap();
    }
}

use super::*;
use crate::entities::models::{GattServiceSpec, ServiceUuid};

#[tokio::test]
async fn broker_source_payload_overflow_does_not_become_critical_lag() {
    use crate::entities::models::{CharacteristicUuid, GattCharacteristicSpec, WriteType};
    let radio = Arc::new(LocalRadio::default());
    let (outbox, mut received) = mpsc::channel(64);
    let conn = Arc::new(ConnHandle {
        outbox,
        owned_addresses: Mutex::default(),
        forwarders: Mutex::default(),
    });
    let address = PeerAddress("server".into());
    let central = PeerAddress("client".into());
    dispatch(
        Request::RegisterAddress {
            address: address.clone(),
        },
        &radio,
        &conn,
    )
    .await;
    let characteristic = CharacteristicUuid(uuid::Uuid::new_v4());
    radio
        .advertise(
            &address,
            GattServiceSpec::new(
                ServiceUuid(uuid::Uuid::new_v4()),
                vec![GattCharacteristicSpec {
                    uuid: characteristic,
                    readable: true,
                    writable: true,
                    notifiable: false,
                    initial_value: vec![],
                }],
            ),
        )
        .await
        .unwrap();
    let session = radio.connect(&central, &address).await.unwrap();
    // No yield: the actual broker forwarder cannot drain either source.
    for _ in 0..512 {
        radio
            .write_with_type(
                session,
                &central,
                &address,
                characteristic,
                vec![1],
                WriteType::WithResponse,
            )
            .await
            .unwrap();
    }
    radio.disconnect(session, &central, &address).await.unwrap();
    let mut saw_connected = false;
    let mut saw_disconnected = false;
    let mut saw_payload_lag = false;
    while !(saw_connected && saw_disconnected && saw_payload_lag) {
        let envelope = tokio::time::timeout(std::time::Duration::from_millis(300), received.recv())
            .await
            .unwrap()
            .unwrap();
        match envelope.frame {
            Frame::Push(Push::LifecycleEvent { event, .. }) => match event {
                GattEvent::Connected { .. } => saw_connected = true,
                GattEvent::Disconnected { .. } => saw_disconnected = true,
                other => panic!("payload overflow polluted broker critical source: {other:?}"),
            },
            Frame::Push(Push::Event {
                event: GattEvent::Lagged { dropped },
                ..
            }) if dropped > 0 => saw_payload_lag = true,
            _ => {}
        }
    }
    for task in conn.forwarders.lock().unwrap().drain(..) {
        task.abort();
    }
}

#[tokio::test]
async fn stalled_broker_writer_bounds_critical_storage_and_reports_real_lag() {
    let radio = Arc::new(LocalRadio::default());
    let (outbox, mut received) = mpsc::channel(64);
    let conn = Arc::new(ConnHandle {
        outbox,
        owned_addresses: Mutex::default(),
        forwarders: Mutex::default(),
    });
    let address = PeerAddress("server".into());
    let central = PeerAddress("client".into());
    dispatch(
        Request::RegisterAddress {
            address: address.clone(),
        },
        &radio,
        &conn,
    )
    .await;
    radio
        .advertise(
            &address,
            GattServiceSpec::new(ServiceUuid(uuid::Uuid::new_v4()), vec![]),
        )
        .await
        .unwrap();
    // The socket writer never drains. Let the *real broker forwarder*
    // run after each transition: unbounded outbox would hide overflow.
    for _ in 0..256 {
        let session = radio.connect(&central, &address).await.unwrap();
        radio.disconnect(session, &central, &address).await.unwrap();
        tokio::task::yield_now().await;
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
    let mut saw_lag = false;
    while let Ok(Some(envelope)) = tokio::time::timeout_at(deadline, received.recv()).await {
        if matches!(envelope.frame, Frame::Push(Push::LifecycleEvent { event: GattEvent::Lagged { dropped }, .. }) if dropped > 0)
        {
            saw_lag = true;
            break;
        }
    }
    for task in conn.forwarders.lock().unwrap().drain(..) {
        task.abort();
    }
    assert!(saw_lag, "stalled broker writer retained unlimited critical transitions rather than reporting overflow");
}

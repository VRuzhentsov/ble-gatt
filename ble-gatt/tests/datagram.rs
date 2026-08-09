//! Datagram-tier integration tests over the mock backend. CI-safe: no radio,
//! no BlueZ daemon, no Android runtime.
//!
//! These drive the real `connect`/`serve` code paths — fragmentation against
//! a negotiated MTU, reassembly under bounds, and link-loss teardown — with
//! only the platform layer swapped out.

use std::sync::Arc;
use std::time::Duration;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::datagram::{self, DatagramChannel, DatagramConfig};
use ble_gatt::{Backend, CapabilityReport, CharacteristicUuid, PeerAddress, ServiceUuid};
use tokio_stream::StreamExt;
use uuid::Uuid;

fn full_capabilities() -> CapabilityReport {
    CapabilityReport {
        central: true,
        peripheral: true,
    }
}

fn config() -> DatagramConfig {
    DatagramConfig::new(
        ServiceUuid(Uuid::new_v4()),
        CharacteristicUuid(Uuid::new_v4()),
    )
}

/// Stand up a peripheral serving `config` and a central connected to it.
/// Returns both channels plus the backends, which must be kept alive — the
/// mock deregisters a peer's event sink when its backend is dropped.
async fn connected_pair(
    config: &DatagramConfig,
) -> (
    DatagramChannel,
    DatagramChannel,
    Arc<MockBackend>,
    Arc<MockBackend>,
) {
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral".to_string());
    let central_addr = PeerAddress("central".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr,
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), config)
        .await
        .expect("serve should start");

    let central_channel = datagram::connect(central.clone(), &peripheral_addr, config)
        .await
        .expect("connect should succeed");

    let peripheral_channel = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel for the connecting central")
        .expect("channel stream should not be closed");

    (central_channel, peripheral_channel, central, peripheral)
}

#[tokio::test]
async fn a_message_smaller_than_one_fragment_round_trips() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    central.send(b"small".to_vec()).await.expect("send");

    let received = tokio::time::timeout(Duration::from_secs(2), peripheral.recv())
        .await
        .expect("message should arrive")
        .expect("channel open")
        .expect("no error");
    assert_eq!(received, b"small");
}

#[tokio::test]
async fn an_empty_message_round_trips_rather_than_vanishing() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    central.send(Vec::new()).await.expect("send");

    let received = tokio::time::timeout(Duration::from_secs(2), peripheral.recv())
        .await
        .expect("empty message should still arrive")
        .expect("channel open")
        .expect("no error");
    assert!(received.is_empty());
}

#[tokio::test]
async fn a_message_spanning_many_fragments_reassembles_intact() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    // Sized well past the per-fragment budget so this genuinely exercises
    // fragmentation rather than passing by accident.
    let budget = central.fragment_budget();
    let payload: Vec<u8> = (0..=255u8).cycle().take(budget * 7 + 13).collect();
    assert!(payload.len() > budget, "test must actually fragment");

    central.send(payload.clone()).await.expect("send");

    let received = tokio::time::timeout(Duration::from_secs(5), peripheral.recv())
        .await
        .expect("message should arrive")
        .expect("channel open")
        .expect("no error");
    assert_eq!(received, payload);
}

#[tokio::test]
async fn a_message_of_exactly_one_fragment_budget_round_trips() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    // The boundary case: exactly one fragment's worth must not gain a
    // spurious second fragment nor lose its last byte.
    let payload = vec![0xAB; central.fragment_budget()];
    central.send(payload.clone()).await.expect("send");

    let received = tokio::time::timeout(Duration::from_secs(2), peripheral.recv())
        .await
        .expect("message should arrive")
        .expect("channel open")
        .expect("no error");
    assert_eq!(received, payload);
}

#[tokio::test]
async fn several_messages_arrive_in_order_and_intact() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    let budget = central.fragment_budget();
    let messages = vec![
        b"first".to_vec(),
        vec![0x11; budget * 2 + 5], // fragmented
        b"third".to_vec(),
    ];
    for message in &messages {
        central.send(message.clone()).await.expect("send");
    }

    for expected in &messages {
        let received = tokio::time::timeout(Duration::from_secs(5), peripheral.recv())
            .await
            .expect("message should arrive")
            .expect("channel open")
            .expect("no error");
        assert_eq!(&received, expected);
    }
}

#[tokio::test]
async fn the_channel_is_bidirectional() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    central.send(b"ping".to_vec()).await.expect("central send");
    let got_ping = tokio::time::timeout(Duration::from_secs(2), peripheral.recv())
        .await
        .expect("ping should arrive")
        .expect("channel open")
        .expect("no error");
    assert_eq!(got_ping, b"ping");

    peripheral
        .send(b"pong".to_vec())
        .await
        .expect("peripheral send");
    let got_pong = tokio::time::timeout(Duration::from_secs(2), central.recv())
        .await
        .expect("pong should arrive")
        .expect("channel open")
        .expect("no error");
    assert_eq!(got_pong, b"pong");
}

#[tokio::test]
async fn a_message_over_the_configured_limit_is_refused_before_it_is_sent() {
    let mut config = config();
    config.max_message_len = 64;
    let (mut central, _peripheral, _c, _p) = connected_pair(&config).await;

    let error = central
        .send(vec![0u8; 65])
        .await
        .expect_err("oversized message must be refused");
    assert!(
        error.to_string().contains("exceeds"),
        "error should explain the limit, got: {error}"
    );
}

#[tokio::test]
async fn losing_the_peer_closes_the_channel_instead_of_hanging_forever() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-loss".to_string());
    let central_addr = PeerAddress("central-loss".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let _incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve");
    let mut central_channel = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect");

    // The peer vanishes with no API call on our side — out of range, battery
    // dead. This is the case that would otherwise leave `recv()` pending
    // forever, which is precisely why link-loss events exist.
    peripheral.simulate_peer_loss(&central_addr);

    let closed = tokio::time::timeout(Duration::from_secs(2), central_channel.recv())
        .await
        .expect("recv must resolve rather than hang");
    assert!(closed.is_none(), "channel should report closed, not a message");
}

#[tokio::test]
async fn the_reported_message_limit_is_one_the_channel_can_actually_send() {
    let config = config();
    let (mut central, mut peripheral, _c, _p) = connected_pair(&config).await;

    // Regression guard: `max_message_len` used to report the configured
    // ceiling (1 MiB) while the peripheral could only fit ~896 KiB into
    // MAX_FRAGMENTS fragments of its spec-minimum budget. Anything in the
    // gap passed the size check and then failed inside split() one call
    // later. Whatever the channel reports must be genuinely sendable.
    for channel in [&mut central, &mut peripheral] {
        let limit = channel.max_message_len();
        let capacity = channel.fragment_budget() * 65_535;
        assert!(
            limit <= capacity,
            "reported limit {limit} exceeds what {} fragments of {} bytes can carry ({capacity})",
            65_535,
            channel.fragment_budget(),
        );
    }
}

#[tokio::test]
async fn a_zero_queue_depth_is_rejected_rather_than_panicking() {
    // `mpsc::channel(0)` panics. `inbound_queue_depth` is a public field
    // with no non-zero invariant, so a caller's zero used to crash
    // `connect` — and panic `serve`'s detached background task, where
    // nothing observes it at all.
    let network = MockNetwork::new();
    let addr = PeerAddress("peripheral-zero".to_string());
    let backend: Arc<MockBackend> =
        Arc::new(MockBackend::new(addr.clone(), network, full_capabilities()));

    let mut zero_inbound = config();
    zero_inbound.inbound_queue_depth = 0;
    assert!(datagram::serve(backend.clone(), &zero_inbound).await.is_err());

    let mut zero_fragments = config();
    zero_fragments.fragment_queue_depth = 0;
    assert!(datagram::serve(backend, &zero_fragments).await.is_err());
}

#[tokio::test]
async fn serving_and_dialling_on_one_backend_do_not_cross_over() {
    // A backend used in both roles emits Connected for its own outbound
    // dial too. Without role information on the event, serve() treated that
    // as an arriving central: it yielded a phantom channel for the remote
    // peripheral *and* consumed the single-central slot, so the genuine
    // inbound central was then refused.
    let network = MockNetwork::new();
    let config = config();

    let both_roles: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("both-roles".to_string()),
        network.clone(),
        full_capabilities(),
    ));
    let remote: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("remote-peripheral".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    remote
        .advertise(config.service_spec())
        .await
        .expect("remote advertises");
    let mut incoming = datagram::serve(both_roles.clone(), &config)
        .await
        .expect("serve");

    // Dial out from the same backend that is serving.
    let _outbound = datagram::connect(
        both_roles.clone(),
        &PeerAddress("remote-peripheral".to_string()),
        &config,
    )
    .await
    .expect("outbound connect");

    // serve() must not have produced a channel from our own dial-out.
    let phantom = tokio::time::timeout(Duration::from_millis(300), incoming.next()).await;
    assert!(
        phantom.is_err(),
        "serve() yielded a channel for our own outbound connection"
    );
}

#[tokio::test]
async fn a_served_channel_stops_sending_once_its_peer_disconnects() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-stale".to_string());
    let central_addr = PeerAddress("central-stale".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let central_channel = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");

    served.send(b"while-connected".to_vec()).await.expect("send while connected");

    // The peer vanishes, but the caller still holds its channel. Because
    // notify is a broadcast that cannot be addressed to one peer on BlueZ, a
    // send here would reach whichever central is subscribed *next* — leaking
    // this peer's fragments into that peer's stream while `peer()` still
    // reports the departed address.
    drop(central_channel);
    peripheral.simulate_peer_loss(&central_addr);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match served.send(b"after-disconnect".to_vec()).await {
            Err(_) => break,
            Ok(()) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "a channel whose peer has gone must refuse to send, not broadcast \
                     to whoever subscribes next"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

#[tokio::test]
async fn stopping_the_server_releases_the_peer_it_was_serving() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-restart".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let first_central: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-before".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let _first = datagram::connect(first_central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");
    served.send(b"while-serving".to_vec()).await.expect("send while serving");

    // The server goes away while that central is still attached. Neither
    // platform delivers a disconnect callback for a server teardown, so a
    // backend that stays silent leaves the old `serve` generation believing
    // it is still serving that peer.
    peripheral.stop_advertising().await.expect("stop_advertising");
    drop(incoming);

    // Restart and attach a different central.
    let mut restarted = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should restart");
    let second_central: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-after".to_string()),
        network.clone(),
        full_capabilities(),
    ));
    let mut second = datagram::connect(second_central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut second_served = tokio::time::timeout(Duration::from_secs(2), restarted.next())
        .await
        .expect("the restarted server must accept a new central")
        .expect("channel stream should not be closed");

    // The load-bearing assertion. If the stale generation still holds
    // `central-before`, it treats this one as an interloper and disconnects
    // it — so the new session dies even though nothing is wrong with it.
    second_served
        .send(b"to-the-new-central".to_vec())
        .await
        .expect("the restarted server's central must not be disconnected by the old generation");
    let received = tokio::time::timeout(Duration::from_secs(2), second.recv())
        .await
        .expect("the new central should receive it")
        .expect("its channel should still be open")
        .expect("no error");
    assert_eq!(received, b"to-the-new-central");

    // And the channel from the stopped generation must not still be usable.
    assert!(
        served.send(b"after-stop".to_vec()).await.is_err(),
        "a channel whose server has stopped must not keep sending"
    );
}

#[tokio::test]
async fn inbound_overflow_is_reported_to_the_receiver_rather_than_lost_silently() {
    // Tiny queues so the receiver can be overwhelmed deterministically.
    let mut config = config();
    config.fragment_queue_depth = 1;
    config.inbound_queue_depth = 1;

    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-overflow".to_string());
    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-overflow".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let mut sender = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");

    // Flood without draining. The sender's writes are acknowledged by the
    // stack before the datagram layer sees them, so `send` cannot be failed
    // — which is exactly why the loss must reach the receiver instead of
    // disappearing into a reassembly timeout.
    for i in 0..20u8 {
        if sender.send(vec![i; 8]).await.is_err() {
            break;
        }
    }

    // Let `serve` work through the backlog against a receiver that is not
    // draining, so it actually reaches the drop path. Comfortably longer
    // than the backpressure window it waits before giving up.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut saw_error = false;
    for _ in 0..64 {
        match tokio::time::timeout(Duration::from_secs(2), served.recv()).await {
            Ok(Some(Err(_))) => {
                saw_error = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(
        saw_error,
        "a receiver that overflows must be told its stream lost data, not left to \
         infer it from a message that never arrives"
    );
}

#[tokio::test]
async fn a_central_that_unsubscribes_frees_the_single_central_slot() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-unsub".to_string());
    let first_addr = PeerAddress("central-unsub".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let first: Arc<MockBackend> = Arc::new(MockBackend::new(
        first_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let _first_channel = datagram::connect(first.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");
    served.send(b"while-subscribed".to_vec()).await.expect("send while subscribed");

    // The central stops listening but stays connected. It can no longer be
    // reached by notify, so holding it in the single-central slot would fail
    // every send while refusing every other central.
    network.simulate_unsubscribe(&peripheral_addr, &first_addr);

    let second_addr = PeerAddress("central-unsub-2".to_string());
    let second: Arc<MockBackend> = Arc::new(MockBackend::new(
        second_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let mut second_channel = datagram::connect(second.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut second_served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("the slot must be free for the next central")
        .expect("channel stream should not be closed");

    second_served
        .send(b"to-the-second".to_vec())
        .await
        .expect("the newly served central must be reachable");
    let received = tokio::time::timeout(Duration::from_secs(2), second_channel.recv())
        .await
        .expect("second central should receive it")
        .expect("its channel should still be open")
        .expect("no error");
    assert_eq!(received, b"to-the-second");
}

#[tokio::test]
async fn a_server_can_greet_the_moment_it_is_handed_a_channel() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-greet".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-greet".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");

    // The central connects and then waits. It sends nothing — this is the
    // server-speaks-first shape that the whole announce-on-subscribe design
    // exists to support.
    let mut client = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");

    let mut served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");

    // No delay, no retry. A channel handed over before the notify path
    // exists would fail here with "no live notify session" — which is what
    // makes announcing at connection time wrong.
    served
        .send(b"hello-from-the-server".to_vec())
        .await
        .expect("a freshly accepted channel must be immediately usable");

    let received = tokio::time::timeout(Duration::from_secs(2), client.recv())
        .await
        .expect("the greeting should arrive")
        .expect("channel should be open")
        .expect("no error");
    assert_eq!(received, b"hello-from-the-server");
}

#[tokio::test]
async fn a_receiver_already_waiting_is_woken_when_inbound_data_is_lost() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-wake".to_string());
    let central_addr = PeerAddress("central-wake".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let mut client = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let _served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");

    // Park in `recv()` first. This is the case a bare flag cannot reach: the
    // receiver is already blocked, and the dropped payload may be the
    // fragment a pending message needed — so nothing further will ever
    // arrive to wake it and make it re-check.
    let receiver = tokio::spawn(async move {
        matches!(client.recv().await, Some(Err(_)))
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The backend reports a gap: payloads it dropped after the peer had
    // already been told they were sent.
    network.simulate_notification_gap(&peripheral_addr, &central_addr);

    let woken = tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("a parked receiver must be woken by a reported gap, not left hanging")
        .expect("receiver task should not panic");
    assert!(woken, "the wakeup must deliver the loss as an error, not close the channel");
}

#[tokio::test]
async fn dropping_a_served_channel_frees_the_slot_for_the_next_central() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-drop-slot".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let first: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-drop-slot-1".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let _first_client = datagram::connect(first.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");

    // The caller is done with this peer and drops its channel while the
    // central is still connected. `close()` is a no-op in the peripheral
    // role, so the drop is the only signal `serve` can act on — without it
    // the peer keeps the single-central slot forever.
    drop(served);

    let second: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-drop-slot-2".to_string()),
        network.clone(),
        full_capabilities(),
    ));
    let mut second_client = datagram::connect(second.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut second_served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("the freed slot must admit the next central")
        .expect("channel stream should not be closed");

    second_served
        .send(b"to-the-second".to_vec())
        .await
        .expect("the newly served central must be reachable");
    let received = tokio::time::timeout(Duration::from_secs(2), second_client.recv())
        .await
        .expect("second central should receive it")
        .expect("channel should be open")
        .expect("no error");
    assert_eq!(received, b"to-the-second");
}

#[tokio::test]
async fn a_stale_channel_drop_does_not_evict_the_reconnected_peer() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-reuse".to_string());
    let central_addr = PeerAddress("central-reuse".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let first: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let first_client = datagram::connect(first.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let stale = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("channel stream should not be closed");

    // The peer drops, and the caller keeps holding its now-dead channel.
    drop(first_client);
    peripheral.simulate_peer_loss(&central_addr);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The *same address* reconnects and is served afresh.
    let second: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let mut second_client = datagram::connect(second.clone(), &peripheral_addr, &config)
        .await
        .expect("reconnect should succeed");
    let mut reconnected = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel for the reconnected peer")
        .expect("channel stream should not be closed");

    // Only now is the stale channel dropped. Keyed on address alone, its
    // release would evict the entry the reconnected peer now holds and
    // disconnect a central that has done nothing wrong.
    drop(stale);
    tokio::time::sleep(Duration::from_millis(100)).await;

    reconnected
        .send(b"still-serving".to_vec())
        .await
        .expect("a reconnected peer must survive the previous channel being dropped");
    let received = tokio::time::timeout(Duration::from_secs(2), second_client.recv())
        .await
        .expect("the reconnected central should receive it")
        .expect("channel should be open")
        .expect("no error");
    assert_eq!(received, b"still-serving");
}

#[tokio::test]
async fn a_release_survives_a_saturated_accept_queue() {
    // Depth 1 is what made the old bounded release queue discardable.
    let mut config = config();
    config.accept_queue_depth = 1;

    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-relqueue".to_string());
    let central_addr = PeerAddress("central-relqueue".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");

    // First generation, then a reconnect of the same address, then drop the
    // stale channel followed immediately by the live one. The stale release
    // is discarded by design (superseded), but the *live* one must not be —
    // if it is, the slot stays occupied by a channel that no longer exists
    // and nothing later can free it.
    let first: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let first_client = datagram::connect(first.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let stale = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("stream open");

    drop(first_client);
    peripheral.simulate_peer_loss(&central_addr);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let second: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let second_client = datagram::connect(second.clone(), &peripheral_addr, &config)
        .await
        .expect("reconnect should succeed");
    let live = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel for the reconnected peer")
        .expect("stream open");

    drop(stale);
    drop(live);
    drop(second_client);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The slot must be free: a third central has to be accepted.
    let third_addr = PeerAddress("central-relqueue-3".to_string());
    let third: Arc<MockBackend> = Arc::new(MockBackend::new(
        third_addr,
        network.clone(),
        full_capabilities(),
    ));
    let _third_client = datagram::connect(third.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("the slot must be free after the live channel's release")
        .expect("stream open");
}

#[tokio::test]
async fn a_lagged_lifecycle_stream_ends_the_served_session_rather_than_stranding_it() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-lag".to_string());
    let first_addr = PeerAddress("central-lag".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let first: Arc<MockBackend> = Arc::new(MockBackend::new(
        first_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let _first_client = datagram::connect(first.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("stream open");
    served.send(b"while-healthy".to_vec()).await.expect("send while healthy");

    // Events were dropped. One of them may have been this peer's
    // `Disconnected`, and nothing will ever resend it — so treating the lag
    // as recoverable would hold the slot against every future central.
    network.simulate_event_lag(&peripheral_addr, 12);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The holder of the stale channel must be told, not left guessing.
    assert!(
        served.send(b"after-lag".to_vec()).await.is_err(),
        "a session ended by lag must stop accepting sends"
    );

    // And the slot must be free for the next central.
    let second_addr = PeerAddress("central-lag-2".to_string());
    let second: Arc<MockBackend> = Arc::new(MockBackend::new(
        second_addr,
        network.clone(),
        full_capabilities(),
    ));
    let mut second_client = datagram::connect(second.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let mut second_served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("the slot must be free after a lag ends the previous session")
        .expect("stream open");

    second_served.send(b"to-the-second".to_vec()).await.expect("send to new central");
    let received = tokio::time::timeout(Duration::from_secs(2), second_client.recv())
        .await
        .expect("second central should receive it")
        .expect("channel open")
        .expect("no error");
    assert_eq!(received, b"to-the-second");
}

#[tokio::test]
async fn abandoning_connect_mid_setup_still_releases_the_connection() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-cancel".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    peripheral
        .advertise(config.service_spec())
        .await
        .expect("advertise should succeed");

    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-cancel".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    // Stall setup so the caller's timeout lands while `subscribe` is still
    // pending — after the platform connection exists but before the channel
    // does. Nothing on `connect`'s own path runs from here on.
    network.stall_subscribe(Duration::from_secs(30));

    let outcome = tokio::time::timeout(
        Duration::from_millis(200),
        datagram::connect(central.clone(), &peripheral_addr, &config),
    )
    .await;
    assert!(outcome.is_err(), "the connect future should have been cancelled");

    // The established connection must still be released. Without this, on
    // Android the platform GATT stays open and every retry to the address is
    // refused as already open — a timeout permanently killing the peer.
    let mut released = false;
    for _ in 0..40 {
        if network.disconnected_peers().contains(&peripheral_addr) {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        released,
        "an abandoned setup must disconnect the connection it established, not just \
         drop the handle"
    );
}

#[tokio::test]
async fn dropping_a_central_channel_without_close_still_disconnects_it() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-drop-channel".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    peripheral
        .advertise(config.service_spec())
        .await
        .expect("advertise should succeed");

    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        PeerAddress("central-drop-channel".to_string()),
        network.clone(),
        full_capabilities(),
    ));

    let channel = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");

    // Dropped without calling `close()` — cancellation, an early error, or
    // simply forgetting are all the same case from the connection's point of
    // view.
    drop(channel);

    // The established connection must still be released. Without this, on
    // Android the platform GATT stays open and a later `connect` to the same
    // address is refused as already open — see `Drop for DatagramChannel`.
    let mut released = false;
    for _ in 0..40 {
        if network.disconnected_peers().contains(&peripheral_addr) {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        released,
        "dropping a channel without close() must disconnect the connection it holds, \
         not just drop the handle"
    );
}

#[tokio::test]
async fn a_central_parked_in_recv_is_released_when_lifecycle_events_lag() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-clag".to_string());
    let central_addr = PeerAddress("central-clag".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");
    let mut client = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let _served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("stream open");

    // Park in recv() before the lag. The peer's `Disconnected` may be among
    // the discarded events, and the notification stream is explicitly not
    // guaranteed to close with the link — so without terminal handling this
    // caller waits forever on a peer that may already be gone.
    let receiver = tokio::spawn(async move { client.recv().await });
    tokio::time::sleep(Duration::from_millis(150)).await;

    network.simulate_event_lag(&central_addr, 9);

    let outcome = tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("a parked central must be released when lifecycle events lag")
        .expect("receiver task should not panic");
    assert!(
        outcome.is_none() || outcome.map(|item| item.is_err()).unwrap_or(false),
        "the channel must end or report an error, not deliver lag as normal data"
    );
}

#[tokio::test]
async fn a_loss_event_from_a_replaced_connection_does_not_kill_its_successor() {
    let config = config();
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-stale-loss".to_string());
    let central_addr = PeerAddress("central-stale-loss".to_string());

    let peripheral: Arc<MockBackend> = Arc::new(MockBackend::new(
        peripheral_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));
    let central: Arc<MockBackend> = Arc::new(MockBackend::new(
        central_addr.clone(),
        network.clone(),
        full_capabilities(),
    ));

    let mut incoming = datagram::serve(peripheral.clone(), &config)
        .await
        .expect("serve should start");

    // First connection, whose session id we capture, then abandon.
    let first = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("connect should succeed");
    let stale_session = first.session().expect("mock supplies a session");
    let first_served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel")
        .expect("stream open");

    // Both ends of the first session go away, freeing the single-central
    // slot — but its disconnect event has not been delivered yet.
    drop(first);
    drop(first_served);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The same address reconnects and is served afresh.
    let mut second = datagram::connect(central.clone(), &peripheral_addr, &config)
        .await
        .expect("reconnect should succeed");
    let mut second_served = tokio::time::timeout(Duration::from_secs(2), incoming.next())
        .await
        .expect("serve should yield a channel for the reconnect")
        .expect("stream open");

    // The *old* connection's disconnect finally arrives. Matched by address
    // alone it would abort the replacement's reassembly and close a channel
    // that is working.
    network.simulate_loss_for_session(&central_addr, &peripheral_addr, stale_session);
    tokio::time::sleep(Duration::from_millis(200)).await;

    second_served
        .send(b"still-alive".to_vec())
        .await
        .expect("the replacement channel must still be usable");
    let received = tokio::time::timeout(Duration::from_secs(2), second.recv())
        .await
        .expect("a stale loss event must not close the replacement channel")
        .expect("channel should still be open")
        .expect("no error");
    assert_eq!(received, b"still-alive");
}

/// Regression test for `DatagramConfig::advertised_manufacturer_data`: it
/// reaches a scanner through the whole real pipeline (`service_spec()` ->
/// `serve()` -> `Backend::advertise()`), not just when set directly on a
/// `GattServiceSpec` (already covered in `mock_protocol.rs`). This is the
/// mechanism a caller uses to change what's advertised between `serve`
/// calls -- e.g. a discoverability flag toggled on and off by ending the
/// current `serve` stream and starting a new one with an updated config.
#[tokio::test]
async fn advertised_manufacturer_data_reaches_a_scanner_through_serve() {
    let network = MockNetwork::new();
    let peripheral_addr = PeerAddress("peripheral-config-adv".to_string());

    let mut config = config();
    config.advertised_manufacturer_data.insert(0xABCDu16, vec![0x01]);

    let peripheral = MockBackend::new(peripheral_addr.clone(), network.clone(), full_capabilities());
    let _incoming = datagram::serve(Arc::new(peripheral), &config)
        .await
        .expect("serve should start");

    let central = MockBackend::new(PeerAddress("central-config-adv".to_string()), network.clone(), full_capabilities());
    let mut discovered = central.scan(config.service).await.expect("scan should succeed");
    let peer = discovered
        .next()
        .await
        .expect("peripheral should be discovered")
        .expect("discovery should not error");

    assert_eq!(peer.manufacturer_data.get(&0xABCD), Some(&vec![0x01]));
}

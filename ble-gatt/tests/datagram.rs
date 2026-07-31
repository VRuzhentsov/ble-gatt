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

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
use ble_gatt::{CapabilityReport, CharacteristicUuid, PeerAddress, ServiceUuid};
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

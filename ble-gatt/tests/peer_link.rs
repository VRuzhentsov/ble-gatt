//! Tier-3 `PeerLink` against the mock backend — CI-safe, no radio.
//!
//! Proves the consumer-facing contract in `docs/interface.md`: track a peer
//! and get a channel + status, one wedged peer does not stall the others,
//! the radio going off tears links down and coming back re-establishes them,
//! and `max_links` queues the overflow.

use std::sync::Arc;
use std::time::Duration;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::datagram::DatagramConfig;
use ble_gatt::{
    Backend, CapabilityReport, GattCharacteristicSpec, GattServiceSpec, LinkRole, MaxLinks,
    PeerAddress, PeerLink, PeerLinkConfig, PeerLinkEvent, PeerStatus, RadioStatus, RetryBudget,
};
use tokio_stream::StreamExt;
use uuid::Uuid;

const SERVICE: Uuid = Uuid::from_u128(0xb1e6a000_f101_4000_8000_00805f9b34fb);
const CHARACTERISTIC: Uuid = Uuid::from_u128(0xb1e6a001_f101_4000_8000_00805f9b34fb);

fn caps() -> CapabilityReport {
    CapabilityReport { central: true, peripheral: true }
}

fn datagram_config() -> DatagramConfig {
    DatagramConfig::new(
        ble_gatt::ServiceUuid(SERVICE),
        ble_gatt::CharacteristicUuid(CHARACTERISTIC),
    )
}

fn peer_link_config(retry: RetryBudget, max_links: usize) -> PeerLinkConfig {
    PeerLinkConfig {
        datagram: datagram_config(),
        role: LinkRole::DialOnly,
        retry_budget: retry,
        max_links: MaxLinks(max_links),
    }
}

fn tight_budget() -> RetryBudget {
    RetryBudget {
        backoff: vec![Duration::from_millis(50), Duration::from_millis(100)],
        give_up_after: Duration::from_millis(400),
        acceptor_deadline: Duration::from_millis(400),
    }
}

async fn advertise_peripheral(network: &Arc<MockNetwork>, address: &str) -> MockBackend {
    let peripheral = MockBackend::new(PeerAddress(address.to_string()), network.clone(), caps());
    peripheral
        .advertise(GattServiceSpec::new(
            ble_gatt::ServiceUuid(SERVICE),
            vec![GattCharacteristicSpec {
                uuid: ble_gatt::CharacteristicUuid(CHARACTERISTIC),
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: vec![],
            }],
        ))
        .await
        .expect("advertise");
    peripheral
}

/// Poll `f` until it returns `true`, or panic after ~2s. For asserting a
/// value the driver thread updates asynchronously just after an event.
async fn assert_eventually<F: FnMut() -> bool>(mut f: F) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition never became true");
}

/// Wait for the first `PeerLinkEvent` matching `pred`, or panic after 5s.
async fn wait_for<F>(
    events: &mut ble_gatt::BoxStream<PeerLinkEvent>, mut pred: F,
) -> PeerLinkEvent
where
    F: FnMut(&PeerLinkEvent) -> bool,
{
    let fut = async {
        loop {
            let ev = events.next().await.expect("event stream ended");
            if pred(&ev) {
                return ev;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("timed out waiting for a PeerLinkEvent")
}

#[tokio::test(flavor = "multi_thread")]
async fn tracking_a_peer_yields_a_working_channel() {
    let network = MockNetwork::new();
    let _peripheral = advertise_peripheral(&network, "peer-a").await;
    let central = Arc::new(MockBackend::new(PeerAddress("me".into()), network.clone(), caps()));

    let link = PeerLink::with_backend(central, peer_link_config(tight_budget(), 4));
    let mut events = link.events();

    link.track(PeerAddress("peer-a".into()), "peer-a".into());

    let up = wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "peer-a")).await;
    let PeerLinkEvent::Up { channel, .. } = up else { unreachable!() };
    assert_eq!(link.status(&PeerAddress("peer-a".into())), PeerStatus::Connected);

    channel.send(b"hello".to_vec()).await.expect("send");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_wedged_peer_does_not_stall_the_others() {
    let network = MockNetwork::new();
    let _a = advertise_peripheral(&network, "stuck").await;
    let _b = advertise_peripheral(&network, "fine").await;
    let central = Arc::new(MockBackend::new(PeerAddress("me".into()), network.clone(), caps()));
    // "stuck" hangs its dial for far longer than the test.
    central.stall_connect(&PeerAddress("stuck".into()), Duration::from_secs(30));

    let link = PeerLink::with_backend(central, peer_link_config(tight_budget(), 4));
    let mut events = link.events();

    link.track(PeerAddress("stuck".into()), "stuck".into());
    link.track(PeerAddress("fine".into()), "fine".into());

    // "fine" connects while "stuck" is still hanging.
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "fine")).await;
    assert_eq!(link.status(&PeerAddress("fine".into())), PeerStatus::Connected);
    assert_ne!(link.status(&PeerAddress("stuck".into())), PeerStatus::Connected);
}

#[tokio::test(flavor = "multi_thread")]
async fn untracking_drops_the_link() {
    let network = MockNetwork::new();
    let _peripheral = advertise_peripheral(&network, "peer-u").await;
    let central = Arc::new(MockBackend::new(PeerAddress("me".into()), network.clone(), caps()));

    let link = PeerLink::with_backend(central, peer_link_config(tight_budget(), 4));
    let mut events = link.events();

    link.track(PeerAddress("peer-u".into()), "peer-u".into());
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "peer-u")).await;

    link.untrack(PeerAddress("peer-u".into()));
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Down { peer } if peer.0 == "peer-u")).await;
    assert_eventually(|| {
        link.status(&PeerAddress("peer-u".into())) == PeerStatus::Untracked
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_radio_toggle_drops_links_and_re_establishes_them() {
    let network = MockNetwork::new();
    let _peripheral = advertise_peripheral(&network, "peer-r").await;
    let central = Arc::new(MockBackend::new(PeerAddress("me".into()), network.clone(), caps()));

    let link = PeerLink::with_backend(central.clone(), peer_link_config(tight_budget(), 4));
    let mut events = link.events();

    link.track(PeerAddress("peer-r".into()), "peer-r".into());
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "peer-r")).await;

    central.simulate_radio(RadioStatus::Off);
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Down { peer } if peer.0 == "peer-r")).await;
    assert_eventually(|| link.radio() == RadioStatus::Off).await;
    assert_eventually(|| {
        link.status(&PeerAddress("peer-r".into())) == PeerStatus::Unavailable
    })
    .await;

    central.simulate_radio(RadioStatus::On);
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "peer-r")).await;
    assert_eventually(|| link.radio() == RadioStatus::On).await;
    assert_eventually(|| {
        link.status(&PeerAddress("peer-r".into())) == PeerStatus::Connected
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn max_links_queues_the_overflow_and_promotes_on_a_free_slot() {
    let network = MockNetwork::new();
    let _a = advertise_peripheral(&network, "aaa").await;
    let _b = advertise_peripheral(&network, "bbb").await;
    let central = Arc::new(MockBackend::new(PeerAddress("me".into()), network.clone(), caps()));

    let link = PeerLink::with_backend(central, peer_link_config(tight_budget(), 1));
    let mut events = link.events();

    link.track(PeerAddress("aaa".into()), "aaa".into());
    link.track(PeerAddress("bbb".into()), "bbb".into());

    // Deterministic slot order: "aaa" sorts first, so it gets the one slot.
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "aaa")).await;
    wait_for(&mut events, |ev| {
        matches!(ev, PeerLinkEvent::Status { peer, status: PeerStatus::Queued } if peer.0 == "bbb")
    })
    .await;

    link.untrack(PeerAddress("aaa".into()));
    wait_for(&mut events, |ev| matches!(ev, PeerLinkEvent::Up { peer, .. } if peer.0 == "bbb")).await;
}

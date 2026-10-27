//! Two iroh endpoints talking QUIC to each other over `ble-gatt`'s in-process
//! mock radio: one serves datagram channels and attaches them, the other
//! dials through the transport.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ble_gatt::datagram::{self, DatagramConfig};
use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{Adapter, CapabilityReport, CharacteristicUuid, PeerAddress, ServiceUuid};
use ble_gatt_iroh::{custom_addr, dial_with, BleGattTransport, Dialer};
use iroh::endpoint::{presets, Connection};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, RelayMode, SecretKey, TransportAddr};
use tokio_stream::StreamExt;

const ECHO_ALPN: &[u8] = b"ble-gatt-iroh/test/echo";
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct Echo;

impl ProtocolHandler for Echo {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

fn config() -> DatagramConfig {
    DatagramConfig::new(
        ServiceUuid(uuid::Uuid::parse_str("b1e6a100-f101-4000-8000-00805f9b34fb").unwrap()),
        CharacteristicUuid(uuid::Uuid::parse_str("b1e6a101-f101-4000-8000-00805f9b34fb").unwrap()),
    )
}

async fn adapter(network: &Arc<MockNetwork>, address: &str) -> Adapter {
    Adapter::new(Arc::new(MockDriver::new(
        PeerAddress(address.to_string()),
        network.clone(),
        CapabilityReport { central: true, peripheral: true },
    )))
    .await
}

async fn endpoint(secret_key: SecretKey, transport: &BleGattTransport) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(transport.clone()))
        .address_lookup(transport.address_lookup())
        .bind()
        .await
        .expect("bind endpoint")
}

/// A listener that serves datagram channels on `address` and attaches each
/// one, plus a dialer that reaches it.
struct Pair {
    dialer: Endpoint,
    dialer_transport: BleGattTransport,
    listener_id: iroh::EndpointId,
    listener_address: PeerAddress,
    _router: Router,
}

async fn pair() -> Pair {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:02".to_string());

    let listener_transport = BleGattTransport::builder().build();
    let incoming = datagram::serve(&adapter(&network, &listener_address.0).await, &config())
        .await
        .expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(channel) = incoming.next().await {
            attach_to.attach(channel);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = endpoint(listener_key, &listener_transport).await;
    let router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialer_transport = BleGattTransport::builder()
        .dialer(dial_with(adapter(&network, "AA:00:00:00:00:01").await, config()))
        .build();
    dialer_transport.set_peer_address(listener_id, listener_address.clone());
    let dialer = endpoint(SecretKey::generate(), &dialer_transport).await;

    Pair {
        dialer,
        dialer_transport,
        listener_id,
        listener_address,
        _router: router,
    }
}

async fn echo(connection: &Connection, payload: &[u8]) -> Vec<u8> {
    let (mut send, mut recv) = connection.open_bi().await.expect("open stream");
    send.write_all(payload).await.expect("write");
    send.finish().expect("finish");
    recv.read_to_end(payload.len() + 1).await.expect("read echo")
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_echoes_over_the_mock_radio_by_address() {
    let pair = pair().await;
    let address = EndpointAddr::from_parts(
        pair.listener_id,
        [TransportAddr::Custom(custom_addr(&pair.listener_address))],
    );
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(address, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(connection.remote_id(), pair.listener_id, "TLS proved the listener's key");

    let reply = tokio::time::timeout(TIMEOUT, echo(&connection, b"hello over ble-gatt"))
        .await
        .expect("echo in time");
    assert_eq!(reply, b"hello over ble-gatt");
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_dials_by_key_through_the_address_lookup() {
    let pair = pair().await;
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");

    // Larger than one QUIC packet, so it takes many datagrams each way.
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let reply = tokio::time::timeout(TIMEOUT, echo(&connection, &payload))
        .await
        .expect("echo in time");
    assert_eq!(reply, payload);
}

/// An application that shares the datagram service with its own messages
/// reads each new channel's first datagram to route it, then hands QUIC
/// channels over with `attach_after`; the connection still completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_channel_routed_by_its_first_datagram_still_carries_quic() {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:04".to_string());

    let listener_transport = BleGattTransport::builder().build();
    let incoming = datagram::serve(&adapter(&network, &listener_address.0).await, &config())
        .await
        .expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(mut channel) = incoming.next().await {
            let first = channel.recv().await.expect("a first datagram").expect("not a gap");
            assert!(ble_gatt_iroh::is_quic_initial(&first), "a dialler opens with a QUIC Initial");
            attach_to.attach_after(channel, first);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = endpoint(listener_key, &listener_transport).await;
    let _router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialer_transport = BleGattTransport::builder()
        .dialer(dial_with(adapter(&network, "AA:00:00:00:00:03").await, config()))
        .build();
    dialer_transport.set_peer_address(listener_id, listener_address);
    let dialer = endpoint(SecretKey::generate(), &dialer_transport).await;

    let connection = tokio::time::timeout(TIMEOUT, dialer.connect(listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    let reply = tokio::time::timeout(TIMEOUT, echo(&connection, b"routed"))
        .await
        .expect("echo in time");
    assert_eq!(reply, b"routed");
}

#[test]
fn json_never_looks_like_a_quic_initial() {
    assert!(!ble_gatt_iroh::is_quic_initial(br#"{"v":1}"#));
    assert!(!ble_gatt_iroh::is_quic_initial(b""));
    assert!(ble_gatt_iroh::is_quic_initial(&[0xC3, 0, 0, 0, 1]));
}

/// A channel that carries nothing for the idle timeout is closed, and the
/// handle `attach` returned completes, so an application knows the central
/// has gone and the radio is free.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_channel_is_closed() {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:06".to_string());
    let transport = BleGattTransport::builder()
        .link_idle_timeout(Duration::from_millis(200))
        .build();
    let mut incoming = datagram::serve(&adapter(&network, &listener_address.0).await, &config())
        .await
        .expect("serve");
    let _client = datagram::connect(&adapter(&network, "AA:00:00:00:00:05").await, &listener_address, &config())
        .await
        .expect("connect");
    let channel = tokio::time::timeout(TIMEOUT, incoming.next())
        .await
        .expect("accepted in time")
        .expect("a channel");
    let link = transport.attach(channel);
    tokio::time::timeout(Duration::from_secs(5), link)
        .await
        .expect("the idle link closes")
        .expect("the link task ends cleanly");
    assert!(transport.linked_peers().is_empty());
}

/// A channel the peer dialled that then closes idle does not hold back this
/// side's own dial: nobody re-dials after an idle close, so waiting for the
/// peer to dial back would only time out the connect.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_inbound_channel_does_not_hold_back_a_dial() {
    let network = MockNetwork::new();
    let a_address = PeerAddress("AA:00:00:00:00:07".to_string());
    let b_address = PeerAddress("AA:00:00:00:00:08".to_string());

    // Each side serves, attaches what it accepts, and can dial.
    let mut sides = Vec::new();
    for (own, other) in [(&a_address, &b_address), (&b_address, &a_address)] {
        let adapter = adapter(&network, &own.0).await;
        let transport = BleGattTransport::builder()
            .dialer(dial_with(adapter.clone(), config()))
            .link_idle_timeout(Duration::from_millis(500))
            .build();
        let incoming = datagram::serve(&adapter, &config()).await.expect("serve");
        let attach_to = transport.clone();
        tokio::spawn(async move {
            let mut incoming = incoming;
            while let Some(channel) = incoming.next().await {
                attach_to.attach(channel);
            }
        });
        let key = SecretKey::generate();
        let id = key.public();
        let endpoint = endpoint(key, &transport).await;
        let router = Router::builder(endpoint.clone()).accept(ECHO_ALPN, Echo).spawn();
        sides.push((transport, endpoint, id, router, other.clone()));
    }
    let (a_transport, a_endpoint, a_id, _a_router, _) = &sides[0];
    let (b_transport, b_endpoint, b_id, _b_router, _) = &sides[1];
    a_transport.set_peer_address(*b_id, b_address.clone());
    b_transport.set_peer_address(*a_id, a_address.clone());

    // B dials A; A holds that channel as the peripheral.
    let connection = tokio::time::timeout(TIMEOUT, b_endpoint.connect(*a_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&connection, b"b to a")).await.expect("echo in time"), b"b to a");
    connection.close(0u32.into(), b"done");
    tokio::time::timeout(TIMEOUT, async {
        while !a_transport.linked_peers().is_empty() || !b_transport.linked_peers().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both links close idle");

    // Well inside REDIAL_YIELD (30s): A dials B at once.
    let connection = tokio::time::timeout(Duration::from_secs(10), a_endpoint.connect(*b_id, ECHO_ALPN))
        .await
        .expect("A dials without waiting for B to dial back")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&connection, b"a to b")).await.expect("echo in time"), b"a to b");
}

/// An address `set_peer_address` replaced is not dialled, even when iroh
/// still offers it: a phone's old advertising address is gone, and on
/// BlueZ a dial to it cancelled the dial to the current one.
#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_address_is_not_dialled() {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:0A".to_string());
    let old_address = PeerAddress("AA:00:00:00:00:0B".to_string());

    let listener_transport = BleGattTransport::builder().build();
    let incoming = datagram::serve(&adapter(&network, &listener_address.0).await, &config())
        .await
        .expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(channel) = incoming.next().await {
            attach_to.attach(channel);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = endpoint(listener_key, &listener_transport).await;
    let _router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialled = Arc::new(Mutex::new(Vec::new()));
    let inner = dial_with(adapter(&network, "AA:00:00:00:00:09").await, config());
    let record = dialled.clone();
    let dialer: Dialer = Arc::new(move |peer: PeerAddress| {
        record.lock().unwrap().push(peer.clone());
        inner(peer)
    });
    let dialer_transport = BleGattTransport::builder().dialer(dialer).build();
    dialer_transport.set_peer_address(listener_id, old_address.clone());
    dialer_transport.set_peer_address(listener_id, listener_address.clone());
    let dialer = endpoint(SecretKey::generate(), &dialer_transport).await;

    let address = EndpointAddr::from_parts(
        listener_id,
        [
            TransportAddr::Custom(custom_addr(&old_address)),
            TransportAddr::Custom(custom_addr(&listener_address)),
        ],
    );
    let connection = tokio::time::timeout(TIMEOUT, dialer.connect(address, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&connection, b"current")).await.expect("echo in time"), b"current");
    assert!(!dialled.lock().unwrap().contains(&old_address), "dialled the replaced address");
}

/// `close_link` closes an attached channel long before the idle timeout.
#[tokio::test(flavor = "multi_thread")]
async fn close_link_closes_an_attached_channel() {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:0D".to_string());
    let client_address = PeerAddress("AA:00:00:00:00:0C".to_string());
    let transport = BleGattTransport::builder().build();
    let mut incoming = datagram::serve(&adapter(&network, &listener_address.0).await, &config())
        .await
        .expect("serve");
    let _client = datagram::connect(&adapter(&network, &client_address.0).await, &listener_address, &config())
        .await
        .expect("connect");
    let channel = tokio::time::timeout(TIMEOUT, incoming.next())
        .await
        .expect("accepted in time")
        .expect("a channel");
    let link = transport.attach(channel);
    transport.close_link(&client_address);
    tokio::time::timeout(Duration::from_secs(5), link)
        .await
        .expect("the link closes before the idle timeout")
        .expect("the link task ends cleanly");
    assert!(transport.linked_peers().is_empty());
}

/// `close_links` reaches a channel to an address `set_peer_address` has
/// since replaced: a phone's old address, still linked after the
/// connection on it ended.
#[tokio::test(flavor = "multi_thread")]
async fn close_links_closes_a_channel_to_a_replaced_address() {
    let pair = pair().await;
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&connection, b"old")).await.expect("echo in time"), b"old");
    connection.close(0u32.into(), b"done");
    assert_eq!(pair.dialer_transport.linked_peers(), vec![pair.listener_address.clone()]);

    pair.dialer_transport
        .set_peer_address(pair.listener_id, PeerAddress("AA:00:00:00:00:0E".to_string()));
    pair.dialer_transport.close_links(&pair.listener_id);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pair.dialer_transport.linked_peers().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the link to the replaced address closes before the idle timeout");
}

/// Both directions busy at once: the echo writes back while the dialer is
/// still writing, so each side's link sends and receives at the same time.
/// A link that stops reading while a send is in flight can wedge here.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_echo_moves_both_ways_at_once() {
    let pair = pair().await;
    let address = EndpointAddr::from_parts(
        pair.listener_id,
        [TransportAddr::Custom(custom_addr(&pair.listener_address))],
    );
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(address, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");

    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (mut send, mut recv) = connection.open_bi().await.expect("open stream");
    let writing = {
        let payload = payload.clone();
        tokio::spawn(async move {
            send.write_all(&payload).await.expect("write");
            send.finish().expect("finish");
        })
    };
    let reply = tokio::time::timeout(TIMEOUT, recv.read_to_end(payload.len() + 1))
        .await
        .expect("echo in time")
        .expect("read echo");
    writing.await.expect("writer");
    assert_eq!(reply, payload);
}

/// Both sides with the transport's hooks installed; the dialer reaches the
/// listener by key.
struct HookedPair {
    dialer: Endpoint,
    dialer_id: iroh::EndpointId,
    dialer_transport: BleGattTransport,
    listener_id: iroh::EndpointId,
    listener_transport: BleGattTransport,
    _router: Router,
}

async fn hooked_pair(listener: &str, dialer: &str, release_delay: Duration) -> HookedPair {
    let network = MockNetwork::new();
    let listener_address = PeerAddress(listener.to_string());
    let listener_transport = BleGattTransport::builder()
        .link_release_delay(release_delay)
        .build();
    let incoming = datagram::serve(&adapter(&network, listener).await, &config())
        .await
        .expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(channel) = incoming.next().await {
            attach_to.attach(channel);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = Endpoint::builder(presets::Minimal)
        .secret_key(listener_key)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(listener_transport.clone()))
        .hooks(listener_transport.hooks())
        .bind()
        .await
        .expect("bind listener");
    let router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialer_transport = BleGattTransport::builder()
        .dialer(dial_with(adapter(&network, dialer).await, config()))
        .link_release_delay(release_delay)
        .build();
    dialer_transport.set_peer_address(listener_id, listener_address);
    let dialer_key = SecretKey::generate();
    let dialer_id = dialer_key.public();
    let dialer = Endpoint::builder(presets::Minimal)
        .secret_key(dialer_key)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(dialer_transport.clone()))
        .address_lookup(dialer_transport.address_lookup())
        .hooks(dialer_transport.hooks())
        .bind()
        .await
        .expect("bind dialer");
    HookedPair {
        dialer,
        dialer_id,
        dialer_transport,
        listener_id,
        listener_transport,
        _router: router,
    }
}

async fn until_no_links(transport: &BleGattTransport, within: Duration) -> bool {
    tokio::time::timeout(within, async {
        while !transport.linked_peers().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

/// With the hooks, `close_links` by key reaches a link the peer dialled:
/// its address was never set on this side (ADR-0008 D2). The release delay
/// is longer than the test, so only `close_links` can close it.
#[tokio::test(flavor = "multi_thread")]
async fn close_links_reaches_a_link_the_peer_dialled() {
    let pair = hooked_pair("AA:00:00:00:00:12", "AA:00:00:00:00:11", Duration::from_secs(60)).await;
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&connection, b"in")).await.expect("echo in time"), b"in");
    // Ended first, or the dialer's iroh would dial the link straight back.
    connection.close(0u32.into(), b"done");
    drop(connection);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(pair.listener_transport.linked_peers().len(), 1);

    pair.listener_transport.close_links(&pair.dialer_id);
    assert!(
        until_no_links(&pair.listener_transport, Duration::from_secs(5)).await,
        "the link the peer dialled closes by key"
    );
}

/// With the hooks, a link closes soon after the last iroh connection on it
/// ends, long before the idle timeout (ADR-0008 D3).
#[tokio::test(flavor = "multi_thread")]
async fn a_link_closes_when_its_last_connection_ends() {
    let pair = hooked_pair("AA:00:00:00:00:14", "AA:00:00:00:00:13", Duration::from_millis(300)).await;
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&connection, b"once")).await.expect("echo in time"), b"once");
    connection.close(0u32.into(), b"done");
    drop(connection);
    assert!(
        until_no_links(&pair.dialer_transport, Duration::from_secs(5)).await,
        "the dialled link closes before the 20s idle timeout"
    );
    assert!(
        until_no_links(&pair.listener_transport, Duration::from_secs(5)).await,
        "the served link closes before the 20s idle timeout"
    );
}

/// A second connection that starts within the release delay keeps the link
/// (ADR-0008 D3).
#[tokio::test(flavor = "multi_thread")]
async fn a_new_connection_keeps_a_released_link() {
    let pair = hooked_pair("AA:00:00:00:00:16", "AA:00:00:00:00:15", Duration::from_millis(300)).await;
    let first = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&first, b"first")).await.expect("echo in time"), b"first");
    let second = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    first.close(0u32.into(), b"done");
    drop(first);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(pair.dialer_transport.linked_peers().len(), 1, "the link stays for the second connection");
    assert_eq!(tokio::time::timeout(TIMEOUT, echo(&second, b"second")).await.expect("echo in time"), b"second");
}

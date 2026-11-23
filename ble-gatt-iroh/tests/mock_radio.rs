//! Two iroh endpoints talking QUIC to each other over `ble-gatt`'s in-process
//! mock radio: one serves datagram channels and attaches them, the other
//! dials through the transport.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ble_gatt::datagram::DatagramConfig;
use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{
    CapabilityReport, CharacteristicUuid, PeerAddress, PeerLink, PeerLinkConfig, ServiceUuid,
};
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

const CLOSE_ALPN: &[u8] = b"ble-gatt-iroh/test/close";

/// Echoes once, then closes the connection from the accepting side.
#[derive(Debug, Clone)]
struct EchoThenClose;

impl ProtocolHandler for EchoThenClose {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        let _ = send.stopped().await;
        connection.close(2u32.into(), b"off");
        Ok(())
    }
}

fn config() -> DatagramConfig {
    DatagramConfig::new(
        ServiceUuid(uuid::Uuid::parse_str("b1e6a100-f101-4000-8000-00805f9b34fb").unwrap()),
        CharacteristicUuid(uuid::Uuid::parse_str("b1e6a101-f101-4000-8000-00805f9b34fb").unwrap()),
    )
}

async fn peer_link(network: &Arc<MockNetwork>, address: &str) -> Arc<PeerLink> {
    PeerLink::with_driver(
        Arc::new(MockDriver::new(
            PeerAddress(address.to_string()),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        )),
        PeerLinkConfig::new(config()),
    )
}

async fn incoming(
    link: &Arc<PeerLink>,
) -> ble_gatt::Result<n0_future::stream::Boxed<ble_gatt::ConnectionRequest>> {
    let incoming = link.incoming()?;
    link.set_advertising(true).await?;
    let link = link.clone();
    Ok(Box::pin(incoming.map(move |request| {
        let _keep_alive = &link;
        request
    })))
}

async fn endpoint(secret_key: SecretKey, transport: &BleGattTransport) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(transport.clone()))
        .address_lookup(transport.address_lookup())
        .hooks(transport.hooks())
        .bind()
        .await
        .expect("bind endpoint")
}

/// A listener that serves datagram channels on `address` and attaches each
/// one, plus a dialer that reaches it.
struct Pair {
    dialer_link: Arc<PeerLink>,
    listener_link: Arc<PeerLink>,
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
    let listener_link = peer_link(&network, &listener_address.0).await;
    let incoming = incoming(&listener_link).await.expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(request) = incoming.next().await {
            let Ok(channel) = request.accept().await else {
                continue;
            };
            attach_to.attach(channel);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = endpoint(listener_key, &listener_transport).await;
    let router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialer_link = peer_link(&network, "AA:00:00:00:00:01").await;
    let dialer_transport = BleGattTransport::builder()
        .dialer(dial_with(dialer_link.clone()))
        .build();
    dialer_transport.set_peer_address(listener_id, listener_address.clone());
    let dialer = endpoint(SecretKey::generate(), &dialer_transport).await;

    Pair {
        dialer_link,
        listener_link,
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
    recv.read_to_end(payload.len() + 1)
        .await
        .expect("read echo")
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
    assert_eq!(
        connection.remote_id(),
        pair.listener_id,
        "TLS proved the listener's key"
    );

    let reply = tokio::time::timeout(TIMEOUT, echo(&connection, b"hello over ble-gatt"))
        .await
        .expect("echo in time");
    assert_eq!(reply, b"hello over ble-gatt");
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_dials_by_key_through_the_address_lookup() {
    let pair = pair().await;
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
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
    let incoming = incoming(&peer_link(&network, &listener_address.0).await)
        .await
        .expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(request) = incoming.next().await {
            let Ok(channel) = request.accept().await else {
                continue;
            };
            let first = channel
                .recv()
                .await
                .expect("a first datagram")
                .expect("not a gap");
            assert!(
                ble_gatt_iroh::is_quic_initial(&first),
                "a dialler opens with a QUIC Initial"
            );
            attach_to.attach_after(channel, first);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = endpoint(listener_key, &listener_transport).await;
    let _router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialer_transport = BleGattTransport::builder()
        .dialer(dial_with(peer_link(&network, "AA:00:00:00:00:03").await))
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

#[tokio::test(flavor = "multi_thread")]
async fn either_peer_reuses_incoming_and_the_larger_id_can_reconnect() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let network = MockNetwork::new();
    let mut keys = [SecretKey::generate(), SecretKey::generate()];
    keys.sort_by_key(iroh::SecretKey::public);
    let enabled = Arc::new(AtomicBool::new(true));
    let higher_dials = Arc::new(AtomicUsize::new(0));
    let mut sides = Vec::new();
    for (index, key) in keys.into_iter().enumerate() {
        let own = PeerAddress(format!("AA:00:00:00:02:0{index}"));
        let driver = Arc::new(MockDriver::new(
            own.clone(),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        ));
        let adapter = PeerLink::with_driver(driver.clone(), PeerLinkConfig::new(config()));
        let base = dial_with(adapter.clone());
        let enabled = enabled.clone();
        let higher_dials = higher_dials.clone();
        let dialer: Dialer = Arc::new(move |peer| {
            if index == 0 && !enabled.load(Ordering::SeqCst) {
                return Box::pin(async {
                    Err(ble_gatt::BleError::GattBusy(
                        "lower peer is not initiating".into(),
                    ))
                });
            }
            if index == 1 {
                higher_dials.fetch_add(1, Ordering::SeqCst);
            }
            base(peer)
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let mut incoming = incoming(&adapter).await.unwrap();
        let attached = transport.clone();
        tokio::spawn(async move {
            while let Some(request) = incoming.next().await {
                let Ok(channel) = request.accept().await else {
                    continue;
                };
                attached.attach(channel);
            }
        });
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(key)
            .clear_ip_transports()
            .add_custom_transport(Arc::new(transport.clone()))
            .address_lookup(transport.address_lookup())
            .hooks(transport.hooks())
            .bind()
            .await
            .unwrap();
        let router = Router::builder(endpoint.clone())
            .accept(ECHO_ALPN, Echo)
            .spawn();
        sides.push((own, driver, transport, endpoint, router));
    }
    let (low_address, _, low_transport, low, _low_router) = &sides[0];
    let (high_address, high_driver, high_transport, high, _high_router) = &sides[1];
    low_transport.set_peer_address(high.id(), high_address.clone());
    high_transport.set_peer_address(low.id(), low_address.clone());
    let first = tokio::time::timeout(TIMEOUT, low.connect(high.id(), ECHO_ALPN))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echo(&first, b"lower starts").await, b"lower starts");
    let reused = tokio::time::timeout(TIMEOUT, high.connect(low.id(), ECHO_ALPN))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echo(&reused, b"incoming reused").await, b"incoming reused");
    assert_eq!(
        higher_dials.load(Ordering::SeqCst),
        0,
        "incoming path reused without a second BLE dial"
    );

    // Leave the QUIC connections open: this is an unexpected loss, the
    // precise case that used to force the former peripheral to wait 30s.
    enabled.store(false, Ordering::SeqCst);
    high_driver.simulate_peer_loss(low_address);
    tokio::time::timeout(Duration::from_secs(5), async {
        while higher_dials.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("larger id initiates recovery without the old redial yield");
    let reconnected =
        tokio::time::timeout(Duration::from_secs(5), high.connect(low.id(), ECHO_ALPN))
            .await
            .expect("larger id reconnects without the old redial yield")
            .unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&reconnected, b"larger reconnects"))
            .await
            .unwrap(),
        b"larger reconnects"
    );
    assert!(higher_dials.load(Ordering::SeqCst) > 0);
    for connection in [first, reused, reconnected] {
        connection.close(0u32.into(), b"done");
    }
    assert!(until_no_links(low_transport, Duration::from_secs(5)).await);
    assert!(until_no_links(high_transport, Duration::from_secs(5)).await);

    // No existing BLE path now: the larger key can start a fresh session
    // while the smaller peer only accepts, not just reuse its old path.
    let cold = tokio::time::timeout(Duration::from_secs(5), high.connect(low.id(), ECHO_ALPN))
        .await
        .expect("larger id starts from no link")
        .unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&cold, b"larger cold start"))
            .await
            .unwrap(),
        b"larger cold start"
    );
    cold.close(0u32.into(), b"done");
    assert!(until_no_links(low_transport, Duration::from_secs(5)).await);
    assert!(until_no_links(high_transport, Duration::from_secs(5)).await);
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
    let mut incoming = incoming(&peer_link(&network, &listener_address.0).await)
        .await
        .expect("serve");
    let client = peer_link(&network, "AA:00:00:00:00:05").await;
    let (outgoing, accepted) = tokio::join!(
        async {
            client
                .request(&listener_address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { incoming.next().await.unwrap().accept().await.unwrap() },
    );
    let _client = outgoing;
    let channel = accepted;
    let link = transport.attach(channel);
    tokio::time::timeout(Duration::from_secs(5), link)
        .await
        .expect("the idle link closes")
        .expect("the link task ends cleanly");
    assert!(transport.linked_peers().is_empty());
}

/// Releasing the transport cannot disconnect a separate core channel user.
#[tokio::test(flavor = "multi_thread")]
async fn last_quic_user_releases_only_the_transports_core_use() {
    let network = MockNetwork::new();
    let server = peer_link(&network, "shared-server").await;
    let client = peer_link(&network, "shared-client").await;
    let mut requests = incoming(&server).await.unwrap();
    let address = PeerAddress("shared-server".into());
    let (outgoing, channel) = tokio::join!(
        async {
            client
                .request(&address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { requests.next().await.unwrap().accept().await.unwrap() },
    );
    let other_user = channel.clone();
    let transport = BleGattTransport::builder().build();
    let attached = transport.attach(channel);
    transport.close_link(&PeerAddress("shared-client".into()));
    tokio::time::timeout(Duration::from_secs(2), attached)
        .await
        .unwrap()
        .unwrap();
    assert!(transport.linked_peers().is_empty());
    other_user
        .send(b"another core user".to_vec())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), outgoing.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        b"another core user"
    );
    other_user.release().await.unwrap();
    outgoing.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_attachment_does_not_compete_for_the_shared_receive_queue() {
    let network = MockNetwork::new();
    let server = peer_link(&network, "single-receiver").await;
    let client = peer_link(&network, "sender").await;
    let mut requests = incoming(&server).await.unwrap();
    let address = PeerAddress("single-receiver".into());
    let (outgoing, channel) = tokio::join!(
        async {
            client
                .request(&address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { requests.next().await.unwrap().accept().await.unwrap() },
    );
    use iroh::endpoint::transports::{CustomEndpoint, CustomTransport, RecvInfo};
    async fn receive(endpoint: &mut dyn CustomEndpoint) -> Vec<u8> {
        let mut bytes = [0; 256];
        let mut meta = [noq_udp::RecvMeta::default()];
        let mut info = [RecvInfo::new(
            custom_addr(&PeerAddress("sender".into())),
            None,
        )];
        std::future::poll_fn(|cx| {
            endpoint.poll_recv(
                cx,
                &mut [std::io::IoSliceMut::new(&mut bytes)],
                &mut meta,
                &mut info,
            )
        })
        .await
        .unwrap();
        bytes[..meta[0].len].to_vec()
    }
    let transport = BleGattTransport::builder().build();
    let mut endpoint = CustomTransport::bind(&transport).unwrap();

    let first = transport.attach(channel.clone());
    outgoing.send(vec![0, 0]).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), receive(endpoint.as_mut()))
            .await
            .unwrap(),
        vec![0, 0]
    );
    let second = transport.attach(channel.clone());
    let replacement = transport.attach(channel.clone());
    // Numbered packets sent during handoff must arrive exactly once.
    for number in 1..=12u8 {
        outgoing.send(vec![0, number]).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .unwrap()
        .unwrap();
    for number in 1..=12u8 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receive(endpoint.as_mut()))
                .await
                .unwrap(),
            vec![0, number]
        );
    }
    for number in 13..=24u8 {
        outgoing.send(vec![0, number]).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receive(endpoint.as_mut()))
                .await
                .unwrap(),
            vec![0, number]
        );
        let bytes = [1, number];
        channel.send(bytes.to_vec()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), outgoing.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            bytes
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receive(endpoint.as_mut()))
            .await
            .is_err(),
        "no duplicate inbound packet"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), outgoing.recv())
            .await
            .is_err(),
        "no duplicate outbound packet"
    );
    assert!(!replacement.is_finished());
    assert_eq!(transport.linked_peers(), vec![PeerAddress("sender".into())]);
    // Close with two replacements still waiting for their predecessors.
    let waiting = transport.attach(channel.clone());
    let final_waiting = transport.attach(channel);
    transport.close_link(&PeerAddress("sender".into()));
    for task in [replacement, waiting, final_waiting] {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(transport.linked_peers().is_empty());
    outgoing.release().await.unwrap();
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
        let adapter = peer_link(&network, &own.0).await;
        let transport = BleGattTransport::builder()
            .dialer(dial_with(adapter.clone()))
            .link_idle_timeout(Duration::from_millis(500))
            .build();
        let incoming = incoming(&adapter).await.expect("serve");
        let attach_to = transport.clone();
        tokio::spawn(async move {
            let mut incoming = incoming;
            while let Some(request) = incoming.next().await {
                let Ok(channel) = request.accept().await else {
                    continue;
                };
                attach_to.attach(channel);
            }
        });
        let key = SecretKey::generate();
        let id = key.public();
        let endpoint = endpoint(key, &transport).await;
        let router = Router::builder(endpoint.clone())
            .accept(ECHO_ALPN, Echo)
            .spawn();
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
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"b to a"))
            .await
            .expect("echo in time"),
        b"b to a"
    );
    connection.close(0u32.into(), b"done");
    tokio::time::timeout(TIMEOUT, async {
        while !a_transport.linked_peers().is_empty() || !b_transport.linked_peers().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both links close idle");

    // Inside the former adapter delay: A dials B at once.
    let connection = tokio::time::timeout(
        Duration::from_secs(10),
        a_endpoint.connect(*b_id, ECHO_ALPN),
    )
    .await
    .expect("A dials without waiting for B to dial back")
    .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"a to b"))
            .await
            .expect("echo in time"),
        b"a to b"
    );
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
    let incoming = incoming(&peer_link(&network, &listener_address.0).await)
        .await
        .expect("serve");
    let attach_to = listener_transport.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(request) = incoming.next().await {
            let Ok(channel) = request.accept().await else {
                continue;
            };
            attach_to.attach(channel);
        }
    });
    let listener_key = SecretKey::generate();
    let listener_id = listener_key.public();
    let listener = endpoint(listener_key, &listener_transport).await;
    let _router = Router::builder(listener).accept(ECHO_ALPN, Echo).spawn();

    let dialled = Arc::new(Mutex::new(Vec::new()));
    let inner = dial_with(peer_link(&network, "AA:00:00:00:00:09").await);
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
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"current"))
            .await
            .expect("echo in time"),
        b"current"
    );
    assert!(
        !dialled.lock().unwrap().contains(&old_address),
        "dialled the replaced address"
    );
}

/// `close_link` closes an attached channel long before the idle timeout.
#[tokio::test(flavor = "multi_thread")]
async fn close_link_closes_an_attached_channel() {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:0D".to_string());
    let client_address = PeerAddress("AA:00:00:00:00:0C".to_string());
    let transport = BleGattTransport::builder().build();
    let mut incoming = incoming(&peer_link(&network, &listener_address.0).await)
        .await
        .expect("serve");
    let client = peer_link(&network, &client_address.0).await;
    let (outgoing, accepted) = tokio::join!(
        async {
            client
                .request(&listener_address)
                .await
                .unwrap()
                .accept()
                .await
                .unwrap()
        },
        async { incoming.next().await.unwrap().accept().await.unwrap() },
    );
    let _client = outgoing;
    let channel = accepted;
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
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .expect("connect in time")
            .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"old"))
            .await
            .expect("echo in time"),
        b"old"
    );
    connection.close(0u32.into(), b"done");
    assert_eq!(
        pair.dialer_transport.linked_peers(),
        vec![pair.listener_address.clone()]
    );

    pair.dialer_transport.set_peer_address(
        pair.listener_id,
        PeerAddress("AA:00:00:00:00:0E".to_string()),
    );
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
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .is_test(true)
        .try_init();
    let pair = pair().await;
    let address = EndpointAddr::from_parts(
        pair.listener_id,
        [TransportAddr::Custom(custom_addr(&pair.listener_address))],
    );
    let connection = tokio::time::timeout(TIMEOUT, pair.dialer.connect(address, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");

    let local = pair.dialer_link.channel(&pair.listener_address).unwrap();
    let remote = pair
        .listener_link
        .channel(&PeerAddress("AA:00:00:00:00:01".into()))
        .unwrap();
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (mut send, mut recv) = connection.open_bi().await.expect("open stream");
    let writing = {
        let payload = payload.clone();
        tokio::spawn(async move {
            send.write_all(&payload).await.expect("write");
            send.finish().expect("finish");
        })
    };
    let mut reply = Vec::new();
    let reading = tokio::time::timeout(TIMEOUT, async {
        let mut bytes = [0; 8192];
        while let Some(count) = recv.read(&mut bytes).await.expect("read echo") {
            reply.extend_from_slice(&bytes[..count]);
            assert!(reply.len() <= payload.len(), "echo exceeded payload length");
        }
    })
    .await;
    if reading.is_err() {
        let writer_finished = writing.is_finished();
        writing.abort();
        let writer_result = writing.await;
        panic!("echo in time: received {}/{} bytes, writer_finished={writer_finished}, writer_result={writer_result:?}, local_core={:?}, remote_core={:?}, links={:?}, quic={:?}, stats={:?}", reply.len(), payload.len(), local.close_reason(), remote.close_reason(), pair.dialer_transport.linked_peers(), connection.close_reason(), connection.stats());
    }
    writing.await.expect("writer");
    assert_eq!(reply, payload);
}

/// Both sides with the transport's hooks installed; the dialer reaches the
/// listener by key.
struct HookedPair {
    dialer_link: Arc<PeerLink>,
    listener_link: Arc<PeerLink>,
    acquisitions: Arc<std::sync::atomic::AtomicUsize>,
    incoming_requests: Arc<std::sync::atomic::AtomicUsize>,
    dialer: Endpoint,
    dialer_id: iroh::EndpointId,
    dialer_transport: BleGattTransport,
    listener_id: iroh::EndpointId,
    listener_transport: BleGattTransport,
    _router: Router,
}

async fn hooked_pair(listener: &str, dialer: &str, release_delay: Duration) -> HookedPair {
    hooked_pair_with_delays(listener, dialer, release_delay, release_delay).await
}

/// A listener's release delay longer than the test keeps its side of the
/// link, so only the dialer's own release can close it.
async fn hooked_pair_with_delays(
    listener: &str,
    dialer: &str,
    listener_delay: Duration,
    release_delay: Duration,
) -> HookedPair {
    let network = MockNetwork::new();
    let listener_address = PeerAddress(listener.to_string());
    let listener_transport = BleGattTransport::builder()
        .link_release_delay(listener_delay)
        .build();
    let listener_link = peer_link(&network, listener).await;
    let incoming = incoming(&listener_link).await.expect("serve");
    let attach_to = listener_transport.clone();
    let incoming_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted_requests = incoming_requests.clone();
    tokio::spawn(async move {
        let mut incoming = incoming;
        while let Some(request) = incoming.next().await {
            counted_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let Ok(channel) = request.accept().await else {
                continue;
            };
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
    let router = Router::builder(listener)
        .accept(ECHO_ALPN, Echo)
        .accept(CLOSE_ALPN, EchoThenClose)
        .spawn();

    let dialer_link = peer_link(&network, dialer).await;
    let acquisitions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = acquisitions.clone();
    let base = dial_with(dialer_link.clone());
    let dial: Dialer = Arc::new(move |peer| {
        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        base(peer)
    });
    let dialer_transport = BleGattTransport::builder()
        .dialer(dial)
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
        dialer_link,
        listener_link,
        acquisitions,
        incoming_requests,
        dialer,
        dialer_id,
        dialer_transport,
        listener_id,
        listener_transport,
        _router: router,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_core_disconnect_is_not_undone_by_a_live_quic_user() {
    use std::sync::atomic::Ordering;
    for channel_disconnect in [false, true] {
        let pair = hooked_pair(
            "terminal-server",
            "terminal-client",
            Duration::from_secs(60),
        )
        .await;
        let connection =
            tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            echo(&connection, b"before disconnect").await,
            b"before disconnect"
        );
        let other = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            echo(&other, b"second existing user").await,
            b"second existing user"
        );
        let address = PeerAddress("terminal-server".into());
        let old = pair.dialer_link.channel(&address).unwrap();
        let acquisitions = pair.acquisitions.load(Ordering::SeqCst);
        let requests = pair.incoming_requests.load(Ordering::SeqCst);
        if channel_disconnect {
            old.disconnect().await.unwrap();
        } else {
            pair.dialer_link
                .disconnect(pair.listener_link.endpoint_id())
                .await
                .unwrap();
        }
        assert!(tokio::time::timeout(Duration::from_secs(2), old.recv())
            .await
            .unwrap()
            .is_none());
        assert!(until_no_links(&pair.dialer_transport, Duration::from_secs(2)).await);
        assert_eq!(
            old.close_reason(),
            Some(ble_gatt::PeerChannelCloseReason::LocalDisconnect)
        );
        let (mut send, _recv) = connection.open_bi().await.unwrap();
        send.write_all(b"old user after terminal disconnect")
            .await
            .unwrap();
        let (mut other_send, _other_recv) = other.open_bi().await.unwrap();
        other_send
            .write_all(b"other old user after terminal disconnect")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            pair.acquisitions.load(Ordering::SeqCst),
            acquisitions,
            "old QUIC traffic must not reacquire BLE"
        );
        assert_eq!(
            pair.incoming_requests.load(Ordering::SeqCst),
            requests,
            "no new incoming request from retransmission"
        );
        assert!(pair.dialer_transport.linked_peers().is_empty());
        let fresh = pair
            .dialer_link
            .request(&address)
            .await
            .unwrap()
            .accept()
            .await
            .unwrap();
        pair.dialer_transport.attach(fresh);
        let next = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tokio::time::timeout(TIMEOUT, echo(&next, b"fresh user"))
                .await
                .unwrap(),
            b"fresh user"
        );
        connection.close(0u32.into(), b"done");
        next.close(0u32.into(), b"done");
        other.close(0u32.into(), b"done");
        pair._router.shutdown().await.unwrap();
        pair.dialer.close().await;
        pair.listener_link.close().await.unwrap();
        pair.dialer_link.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_explicit_quic_user_can_start_after_terminal_core_disconnect() {
    let pair = hooked_pair(
        "new-user-server",
        "new-user-client",
        Duration::from_secs(60),
    )
    .await;
    let first = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echo(&first, b"first user").await, b"first user");
    pair.dialer_link
        .disconnect(pair.listener_link.endpoint_id())
        .await
        .unwrap();
    assert!(until_no_links(&pair.dialer_transport, Duration::from_secs(2)).await);
    let second = tokio::time::timeout(
        Duration::from_secs(5),
        pair.dialer.connect(pair.listener_id, ECHO_ALPN),
    )
    .await
    .expect("new application connect is not an old user's retransmission")
    .unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&second, b"second user"))
            .await
            .unwrap(),
        b"second user"
    );
    first.close(0u32.into(), b"done");
    second.close(0u32.into(), b"done");
    pair._router.shutdown().await.unwrap();
    pair.dialer.close().await;
    pair.listener_link.close().await.unwrap();
    pair.dialer_link.close().await.unwrap();
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
    let pair = hooked_pair(
        "AA:00:00:00:00:12",
        "AA:00:00:00:00:11",
        Duration::from_secs(60),
    )
    .await;
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .expect("connect in time")
            .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"in"))
            .await
            .expect("echo in time"),
        b"in"
    );
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
    let pair = hooked_pair(
        "AA:00:00:00:00:14",
        "AA:00:00:00:00:13",
        Duration::from_millis(300),
    )
    .await;
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .expect("connect in time")
            .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"once"))
            .await
            .expect("echo in time"),
        b"once"
    );
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

/// The accepting side closes the connection; the dialling side, which only
/// receives the close, drops its handle and releases its link once drained
/// (ADR-0008 D3).
#[tokio::test(flavor = "multi_thread")]
async fn a_link_closes_when_the_peer_closes_its_connection() {
    let pair = hooked_pair_with_delays(
        "AA:00:00:00:00:18",
        "AA:00:00:00:00:17",
        Duration::from_secs(60),
        Duration::ZERO,
    )
    .await;
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, CLOSE_ALPN))
            .await
            .expect("connect in time")
            .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"once"))
            .await
            .expect("echo in time"),
        b"once"
    );
    tokio::time::timeout(TIMEOUT, connection.closed())
        .await
        .expect("closed by the peer in time");
    drop(connection);
    assert!(
        until_no_links(&pair.dialer_transport, Duration::from_secs(5)).await,
        "the dialled link closes before the 20s idle timeout"
    );
}

/// The closing side releases its link once its CONNECTION_CLOSE was sent,
/// not after the drain: a `Connection` it keeps no longer holds the link.
#[tokio::test(flavor = "multi_thread")]
async fn the_closing_side_releases_its_link_once_its_close_is_sent() {
    let pair = hooked_pair_with_delays(
        "AA:00:00:00:00:1F",
        "AA:00:00:00:00:1E",
        Duration::from_secs(60),
        Duration::ZERO,
    )
    .await;
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .expect("connect in time")
            .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"once"))
            .await
            .expect("echo in time"),
        b"once"
    );
    connection.close(0u32.into(), b"done");
    assert!(
        until_no_links(&pair.dialer_transport, Duration::from_secs(5)).await,
        "the dialled link closes while the connection is still held"
    );
    drop(connection);
}

/// The peer closes; the peripheral side, which only receives the close,
/// disconnects the central at once. The dialer's own release delay is longer
/// than the test, so only that disconnect can close its link.
#[tokio::test(flavor = "multi_thread")]
async fn the_receiving_peripheral_disconnects_the_central() {
    let pair = hooked_pair_with_delays(
        "AA:00:00:00:00:21",
        "AA:00:00:00:00:20",
        Duration::ZERO,
        Duration::from_secs(60),
    )
    .await;
    let connection =
        tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
            .await
            .expect("connect in time")
            .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"once"))
            .await
            .expect("echo in time"),
        b"once"
    );
    connection.close(0u32.into(), b"done");
    assert!(
        until_no_links(&pair.listener_transport, Duration::from_secs(5)).await,
        "the served link closes"
    );
    assert!(
        until_no_links(&pair.dialer_transport, Duration::from_secs(5)).await,
        "the central is disconnected"
    );
    drop(connection);
}

/// A caller-supplied destination must authorize a fresh connect even when a
/// terminal core close happened before TLS could teach the adapter its identity.
#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_explicit_address_connect_dispatches_after_a_pre_handshake_disconnect() {
    let network = MockNetwork::new();
    let server = peer_link(&network, "explicit-server").await;
    let client = peer_link(&network, "explicit-client").await;
    let mut incoming = incoming(&server).await.unwrap();
    let (called, mut calls) = tokio::sync::mpsc::unbounded_channel();
    let base = dial_with(client.clone());
    let dialer: Dialer = Arc::new(move |peer| {
        called.send(peer.clone()).unwrap();
        base(peer)
    });
    let transport = BleGattTransport::builder().dialer(dialer).build();
    let endpoint = endpoint(SecretKey::generate(), &transport).await;
    let address = PeerAddress("explicit-server".into());
    let remote = EndpointAddr::from_parts(
        SecretKey::generate().public(),
        [TransportAddr::Custom(custom_addr(&address))],
    );
    // No set_peer_address, no listener QUIC endpoint, no successful handshake.
    let first = tokio::spawn({
        let endpoint = endpoint.clone();
        let remote = remote.clone();
        async move { endpoint.connect(remote, ECHO_ALPN).await }
    });
    let channel = tokio::time::timeout(Duration::from_secs(3), async {
        incoming.next().await.unwrap().accept().await.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(calls.recv().await.unwrap(), address);
    channel.disconnect().await.unwrap();
    assert!(until_no_links(&transport, Duration::from_secs(3)).await);
    first.abort();
    let _ = first.await;

    let second = tokio::spawn({
        let endpoint = endpoint.clone();
        async move { endpoint.connect(remote, ECHO_ALPN).await }
    });
    // Bound request dispatch, not completion of a replacement QUIC handshake.
    let dispatched = tokio::time::timeout(Duration::from_millis(500), calls.recv()).await;
    second.abort();
    let _ = second.await;
    transport.close_link(&address);
    endpoint.close().await;
    client.close().await.unwrap();
    server.close().await.unwrap();
    assert_eq!(
        dispatched.expect("fresh explicit destination must dispatch without set_peer_address"),
        Some(address)
    );
}

/// An idle timer is a backstop for unauthenticated/unused candidates, not
/// permission to close a link still owned by a live iroh connection.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_timeout_does_not_close_a_live_connection() {
    #[derive(Debug, Clone)]
    struct EchoMany;
    impl ProtocolHandler for EchoMany {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                tokio::io::copy(&mut recv, &mut send).await?;
                send.finish()?;
            }
            Ok(())
        }
    }
    let network = MockNetwork::new();
    let address = PeerAddress("AA:00:00:00:01:01".into());
    let server = peer_link(&network, &address.0).await;
    let listener_transport = BleGattTransport::builder()
        .link_idle_timeout(Duration::from_millis(150))
        .build();
    let mut incoming = incoming(&server).await.unwrap();
    let attached = listener_transport.clone();
    tokio::spawn(async move {
        while let Some(request) = incoming.next().await {
            let Ok(channel) = request.accept().await else {
                continue;
            };
            attached.attach(channel);
        }
    });
    let listener = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(listener_transport.clone()))
        .hooks(listener_transport.hooks())
        .bind()
        .await
        .unwrap();
    let listener_id = listener.id();
    let _router = Router::builder(listener)
        .accept(ECHO_ALPN, EchoMany)
        .spawn();
    let client_transport = BleGattTransport::builder()
        .dialer(dial_with(peer_link(&network, "AA:00:00:00:01:02").await))
        .link_idle_timeout(Duration::from_millis(150))
        .build();
    client_transport.set_peer_address(listener_id, address);
    let client = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .add_custom_transport(Arc::new(client_transport.clone()))
        .address_lookup(client_transport.address_lookup())
        .hooks(client_transport.hooks())
        .bind()
        .await
        .unwrap();
    let connection = tokio::time::timeout(TIMEOUT, client.connect(listener_id, ECHO_ALPN))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echo(&connection, b"before idle").await, b"before idle");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        client_transport.linked_peers().len(),
        1,
        "active central link survives idle"
    );
    assert_eq!(
        listener_transport.linked_peers().len(),
        1,
        "active peripheral link survives idle"
    );
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"after idle"))
            .await
            .unwrap(),
        b"after idle"
    );
    connection.close(0u32.into(), b"done");
    assert!(until_no_links(&client_transport, Duration::from_secs(5)).await);
    assert!(until_no_links(&listener_transport, Duration::from_secs(5)).await);
}

/// A second connection that starts within the release delay keeps the link
/// (ADR-0008 D3).
#[tokio::test(flavor = "multi_thread")]
async fn a_new_connection_keeps_a_released_link() {
    let pair = hooked_pair(
        "AA:00:00:00:00:16",
        "AA:00:00:00:00:15",
        Duration::from_millis(300),
    )
    .await;
    let first = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&first, b"first"))
            .await
            .expect("echo in time"),
        b"first"
    );
    let second = tokio::time::timeout(TIMEOUT, pair.dialer.connect(pair.listener_id, ECHO_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    first.close(0u32.into(), b"done");
    drop(first);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        pair.dialer_transport.linked_peers().len(),
        1,
        "the link stays for the second connection"
    );
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&second, b"second"))
            .await
            .expect("echo in time"),
        b"second"
    );
}

/// Both sides connect to each other at the same moment. Both connections
/// work, with no tie-break, and once both end every link closes
/// (ADR-0008 D5).
#[tokio::test(flavor = "multi_thread")]
async fn crossed_connects_both_work_and_their_links_close() {
    let network = MockNetwork::new();
    let a_address = PeerAddress("AA:00:00:00:00:1A".to_string());
    let b_address = PeerAddress("AA:00:00:00:00:1B".to_string());
    let mut sides = Vec::new();
    for own in [&a_address, &b_address] {
        let adapter = peer_link(&network, &own.0).await;
        let transport = BleGattTransport::builder()
            .dialer(dial_with(adapter.clone()))
            .link_idle_timeout(Duration::from_millis(150))
            .link_release_delay(Duration::from_millis(300))
            .build();
        let incoming = incoming(&adapter).await.expect("serve");
        let attach_to = transport.clone();
        tokio::spawn(async move {
            let mut incoming = incoming;
            while let Some(request) = incoming.next().await {
                let Ok(channel) = request.accept().await else {
                    continue;
                };
                attach_to.attach(channel);
            }
        });
        let key = SecretKey::generate();
        let id = key.public();
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(key)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .add_custom_transport(Arc::new(transport.clone()))
            .address_lookup(transport.address_lookup())
            .hooks(transport.hooks())
            .bind()
            .await
            .expect("bind");
        let router = Router::builder(endpoint.clone())
            .accept(ECHO_ALPN, Echo)
            .spawn();
        sides.push((transport, endpoint, id, router));
    }
    let (a_transport, a, a_id, _a_router) = &sides[0];
    let (b_transport, b, b_id, _b_router) = &sides[1];
    a_transport.set_peer_address(*b_id, b_address.clone());
    b_transport.set_peer_address(*a_id, a_address.clone());

    let (a_to_b, b_to_a) = tokio::join!(
        tokio::time::timeout(TIMEOUT, a.connect(*b_id, ECHO_ALPN)),
        tokio::time::timeout(TIMEOUT, b.connect(*a_id, ECHO_ALPN)),
    );
    let a_to_b = a_to_b.expect("a connects in time").expect("a connects");
    let b_to_a = b_to_a.expect("b connects in time").expect("b connects");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&a_to_b, b"a to b"))
            .await
            .expect("echo in time"),
        b"a to b"
    );
    a_to_b.close(0u32.into(), b"losing application candidate");
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert!(
        b_to_a.close_reason().is_none(),
        "ending one crossed connection must not close the other"
    );
    assert!(!a_transport.linked_peers().is_empty());
    assert!(!b_transport.linked_peers().is_empty());
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&b_to_a, b"b to a"))
            .await
            .expect("echo in time"),
        b"b to a"
    );

    b_to_a.close(0u32.into(), b"done");
    drop((a_to_b, b_to_a));
    assert!(
        until_no_links(a_transport, Duration::from_secs(5)).await,
        "a's links close"
    );
    assert!(
        until_no_links(b_transport, Duration::from_secs(5)).await,
        "b's links close"
    );
}

/// A refuses the connection B dialled in, and B's release then drops the
/// link. Nothing is left to make B dial back, so A's own connect goes out at
/// once, without an adapter-owned delay.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_connection_does_not_hold_back_a_dial() {
    let network = MockNetwork::new();
    let a_address = PeerAddress("AA:00:00:00:00:1C".to_string());
    let b_address = PeerAddress("AA:00:00:00:00:1D".to_string());
    let mut sides = Vec::new();
    // A's release delay is longer than the test, so only B's release can
    // drop the link, as on hardware.
    for (own, release_delay) in [
        (&a_address, Duration::from_secs(60)),
        (&b_address, Duration::ZERO),
    ] {
        let adapter = peer_link(&network, &own.0).await;
        let transport = BleGattTransport::builder()
            .dialer(dial_with(adapter.clone()))
            .link_release_delay(release_delay)
            .build();
        let incoming = incoming(&adapter).await.expect("serve");
        let attach_to = transport.clone();
        tokio::spawn(async move {
            let mut incoming = incoming;
            while let Some(request) = incoming.next().await {
                let Ok(channel) = request.accept().await else {
                    continue;
                };
                attach_to.attach(channel);
            }
        });
        let key = SecretKey::generate();
        let id = key.public();
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(key)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .add_custom_transport(Arc::new(transport.clone()))
            .address_lookup(transport.address_lookup())
            .hooks(transport.hooks())
            .bind()
            .await
            .expect("bind");
        let router = Router::builder(endpoint.clone())
            .accept(ECHO_ALPN, Echo)
            .accept(CLOSE_ALPN, EchoThenClose)
            .spawn();
        sides.push((transport, endpoint, id, router));
    }
    let (a_transport, a, a_id, _a_router) = &sides[0];
    let (b_transport, b, b_id, _b_router) = &sides[1];
    a_transport.set_peer_address(*b_id, b_address.clone());
    b_transport.set_peer_address(*a_id, a_address.clone());

    let refused = tokio::time::timeout(TIMEOUT, b.connect(*a_id, CLOSE_ALPN))
        .await
        .expect("connect in time")
        .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&refused, b"b to a"))
            .await
            .expect("echo in time"),
        b"b to a"
    );
    tokio::time::timeout(TIMEOUT, refused.closed())
        .await
        .expect("closed by A in time");
    drop(refused);
    assert!(
        until_no_links(b_transport, Duration::from_secs(5)).await,
        "b's link closes"
    );
    assert!(
        until_no_links(a_transport, Duration::from_secs(5)).await,
        "a sees the link drop"
    );

    // Inside the former adapter delay: A dials B at once.
    let connection = tokio::time::timeout(Duration::from_secs(10), a.connect(*b_id, ECHO_ALPN))
        .await
        .expect("A dials without waiting for B to dial back")
        .expect("connect");
    assert_eq!(
        tokio::time::timeout(TIMEOUT, echo(&connection, b"a to b"))
            .await
            .expect("echo in time"),
        b"a to b"
    );
}

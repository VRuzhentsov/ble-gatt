//! Two iroh endpoints talking QUIC to each other over `ble-gatt`'s in-process
//! mock radio: one serves datagram channels and attaches them, the other
//! dials through the transport.

use std::sync::Arc;
use std::time::Duration;

use ble_gatt::backend::mock::{MockBackend, MockNetwork};
use ble_gatt::backend::Backend;
use ble_gatt::datagram::{self, DatagramConfig};
use ble_gatt::{CapabilityReport, CharacteristicUuid, PeerAddress, ServiceUuid};
use ble_gatt_iroh::{custom_addr, dial_with, BleGattTransport};
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

fn backend(network: &Arc<MockNetwork>, address: &str) -> Arc<dyn Backend> {
    Arc::new(MockBackend::new(
        PeerAddress(address.to_string()),
        network.clone(),
        CapabilityReport { central: true, peripheral: true },
    ))
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
    listener_id: iroh::EndpointId,
    listener_address: PeerAddress,
    _router: Router,
}

async fn pair() -> Pair {
    let network = MockNetwork::new();
    let listener_address = PeerAddress("AA:00:00:00:00:02".to_string());

    let listener_transport = BleGattTransport::builder().build();
    let incoming = datagram::serve(backend(&network, &listener_address.0), &config())
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
        .dialer(dial_with(backend(&network, "AA:00:00:00:00:01"), config()))
        .build();
    dialer_transport.set_peer_address(listener_id, listener_address.clone());
    let dialer = endpoint(SecretKey::generate(), &dialer_transport).await;

    Pair {
        dialer,
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

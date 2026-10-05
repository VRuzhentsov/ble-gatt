//! An [iroh] custom transport that carries QUIC packets over `ble-gatt`'s
//! GATT datagram channel ([`ble_gatt::datagram`]).
//!
//! iroh dials by public key and authenticates and encrypts with TLS 1.3 over
//! QUIC; this crate only moves its packets between two Bluetooth devices.
//! One QUIC packet travels as one datagram message.
//!
//! The transport never scans or advertises on its own. When the radio is on
//! is the application's decision (battery), so the application:
//!
//! - accepts inbound channels however it likes (typically
//!   [`ble_gatt::datagram::serve`] while it wants to be reachable) and hands
//!   each one to [`BleGattTransport::attach`];
//! - tells the transport where a peer can be reached with
//!   [`BleGattTransport::set_peer_address`], for instance after its own scan
//!   heard the peer;
//! - supplies a [`Dialer`] the transport uses to open a channel the first time
//!   iroh sends to an address with no channel yet ([`dial_with`] wraps
//!   [`ble_gatt::datagram::connect`]).
//!
//! ```ignore
//! let transport = BleGattTransport::builder()
//!     .dialer(dial_with(backend.clone(), config.clone()))
//!     .build();
//! let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
//!     .clear_ip_transports()
//!     .add_custom_transport(Arc::new(transport.clone()))
//!     .address_lookup(transport.address_lookup())
//!     .bind()
//!     .await?;
//! ```
//!
//! [iroh]: https://docs.rs/iroh

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use ble_gatt::backend::Backend;
use ble_gatt::datagram::{DatagramChannel, DatagramConfig};
use ble_gatt::PeerAddress;
use iroh::address_lookup::{self, AddressLookup, EndpointData, EndpointInfo, Item};
use iroh::endpoint::transports::{CustomEndpoint, CustomSender, CustomTransport, RecvInfo, Transmit};
use iroh_base::{CustomAddr, EndpointId, TransportAddr};
use tokio::sync::mpsc;

/// The custom address id of this transport ("BLEG"). iroh keeps a registry of
/// transport ids in its `TRANSPORTS.md`; this one is private to `ble-gatt`.
pub const TRANSPORT_ID: u64 = 0x424C_4547;

/// Packets queued for one peer before further ones are dropped. QUIC treats a
/// dropped packet as loss and recovers, so a slow link sheds load instead of
/// growing memory.
const LINK_QUEUE_DEPTH: usize = 64;

/// Packets received from all peers and not yet taken by iroh.
const INBOUND_QUEUE_DEPTH: usize = 256;

/// A packet received from a peer.
type Inbound = (PeerAddress, Vec<u8>);

/// A future opening a datagram channel to a peer.
pub type DialFuture = Pin<Box<dyn Future<Output = ble_gatt::Result<DatagramChannel>> + Send>>;

/// Opens a datagram channel to the given address. Called the first time iroh
/// sends to an address that has no channel.
pub type Dialer = Arc<dyn Fn(PeerAddress) -> DialFuture + Send + Sync>;

/// A [`Dialer`] that opens channels with [`ble_gatt::datagram::connect`].
pub fn dial_with(backend: Arc<dyn Backend>, config: DatagramConfig) -> Dialer {
    Arc::new(move |peer: PeerAddress| {
        let backend = backend.clone();
        let config = config.clone();
        Box::pin(async move { ble_gatt::datagram::connect(backend, &peer, &config).await })
    })
}

/// The iroh address of a Bluetooth peer.
pub fn custom_addr(peer: &PeerAddress) -> CustomAddr {
    CustomAddr::from_parts(TRANSPORT_ID, peer.0.as_bytes())
}

/// The Bluetooth peer an iroh address names, if it is one of this transport's.
pub fn peer_address(addr: &CustomAddr) -> Option<PeerAddress> {
    if addr.id() != TRANSPORT_ID {
        return None;
    }
    String::from_utf8(addr.data().to_vec()).ok().map(PeerAddress)
}

/// Whether `packet` can be the first datagram a dialler sends over a new
/// channel: a QUIC Initial, which has a long header (top bit set). An
/// application sharing one GATT service between QUIC and its own messages
/// can route a new channel by its first datagram, provided its own first
/// byte never has the top bit set (UTF-8 JSON, for one, cannot start so).
pub fn is_quic_initial(packet: &[u8]) -> bool {
    packet.first().is_some_and(|byte| byte & 0x80 != 0)
}

/// Builder for [`BleGattTransport`].
#[derive(Default)]
pub struct BleGattTransportBuilder {
    dialer: Option<Dialer>,
    local_address: Option<PeerAddress>,
}

impl BleGattTransportBuilder {
    /// How to open a channel to a peer. Without one, the transport only sends
    /// over channels handed to it with [`BleGattTransport::attach`].
    pub fn dialer(mut self, dialer: Dialer) -> Self {
        self.dialer = Some(dialer);
        self
    }

    /// This device's own Bluetooth address, if known. Reported to iroh as the
    /// transport's local address.
    pub fn local_address(mut self, address: PeerAddress) -> Self {
        self.local_address = Some(address);
        self
    }

    pub fn build(self) -> BleGattTransport {
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_DEPTH);
        let local = self.local_address.iter().map(custom_addr).collect();
        BleGattTransport {
            inner: Arc::new(Inner {
                links: Mutex::new(HashMap::new()),
                peers: Mutex::new(HashMap::new()),
                inbound_tx,
                inbound_rx: Mutex::new(Some(inbound_rx)),
                dialer: self.dialer,
                local: n0_watcher::Watchable::new(local),
            }),
        }
    }
}

/// iroh custom transport over `ble-gatt` datagram channels. Cheap to clone;
/// clones share one transport.
#[derive(Clone)]
pub struct BleGattTransport {
    inner: Arc<Inner>,
}

struct Inner {
    /// One outbound queue per peer with a channel (open or being dialled).
    links: Mutex<HashMap<PeerAddress, mpsc::Sender<Vec<u8>>>>,
    /// Where each known endpoint can be reached, for address lookup.
    peers: Mutex<HashMap<EndpointId, PeerAddress>>,
    inbound_tx: mpsc::Sender<Inbound>,
    /// Taken by the one endpoint that binds this transport.
    inbound_rx: Mutex<Option<mpsc::Receiver<Inbound>>>,
    dialer: Option<Dialer>,
    local: n0_watcher::Watchable<Vec<CustomAddr>>,
}

impl fmt::Debug for BleGattTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BleGattTransport")
            .field("links", &self.inner.links.lock().expect("poisoned").len())
            .field("dialer", &self.inner.dialer.is_some())
            .finish()
    }
}

impl BleGattTransport {
    pub fn builder() -> BleGattTransportBuilder {
        BleGattTransportBuilder::default()
    }

    /// Carries iroh traffic over `channel`, an open datagram channel in either
    /// role. A channel already attached for the same peer is replaced and
    /// closed.
    pub fn attach(&self, channel: DatagramChannel) {
        let peer = channel.peer();
        let (tx, rx) = mpsc::channel(LINK_QUEUE_DEPTH);
        self.inner
            .links
            .lock()
            .expect("poisoned")
            .insert(peer.clone(), tx.clone());
        tokio::spawn(run_link(self.inner.clone(), peer, channel, rx, tx));
    }

    /// Like [`attach`](Self::attach), for a channel whose first datagram
    /// the caller already read, typically to tell QUIC from its own traffic
    /// with [`is_quic_initial`]. `first` is handed to iroh before anything
    /// else from the channel.
    pub fn attach_after(&self, channel: DatagramChannel, first: Vec<u8>) {
        let _ = self.inner.inbound_tx.try_send((channel.peer(), first));
        self.attach(channel);
    }

    /// Records where `endpoint` can be reached, for iroh's address lookup.
    pub fn set_peer_address(&self, endpoint: EndpointId, address: PeerAddress) {
        self.inner
            .peers
            .lock()
            .expect("poisoned")
            .insert(endpoint, address);
    }

    /// Forgets where `endpoint` can be reached.
    pub fn forget_peer(&self, endpoint: &EndpointId) {
        self.inner.peers.lock().expect("poisoned").remove(endpoint);
    }

    /// Peers with a channel open or being dialled.
    pub fn linked_peers(&self) -> Vec<PeerAddress> {
        self.inner
            .links
            .lock()
            .expect("poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Address lookup that resolves endpoints recorded with
    /// [`set_peer_address`](Self::set_peer_address).
    pub fn address_lookup(&self) -> BleGattAddressLookup {
        BleGattAddressLookup {
            inner: self.inner.clone(),
        }
    }
}

/// Moves packets between one datagram channel and iroh until either side
/// ends.
async fn run_link(
    inner: Arc<Inner>,
    peer: PeerAddress,
    mut channel: DatagramChannel,
    mut outbound: mpsc::Receiver<Vec<u8>>,
    link: mpsc::Sender<Vec<u8>>,
) {
    log::debug!("link to {} up", peer.0);
    loop {
        tokio::select! {
            packet = outbound.recv() => {
                let Some(packet) = packet else { break };
                if let Err(err) = channel.send(packet).await {
                    log::debug!("link to {}: send failed: {err}", peer.0);
                    break;
                }
            }
            // `recv` is cancel-safe: a message is only taken off the queue
            // when this arm completes.
            message = channel.recv() => match message {
                Some(Ok(packet)) => {
                    // Like a full socket buffer: drop, QUIC recovers.
                    if let Err(mpsc::error::TrySendError::Closed(_)) =
                        inner.inbound_tx.try_send((peer.clone(), packet))
                    {
                        break;
                    }
                }
                // A message lost to overflow; QUIC treats it as loss.
                Some(Err(err)) => log::debug!("link to {}: {err}", peer.0),
                None => break,
            },
        }
    }
    let _ = channel.close().await;
    remove_link(&inner, &peer, &link);
    log::debug!("link to {} down", peer.0);
}

/// Removes `peer`'s link only if it is still `link`: a replacement attached
/// meanwhile stays.
fn remove_link(inner: &Inner, peer: &PeerAddress, link: &mpsc::Sender<Vec<u8>>) {
    let mut links = inner.links.lock().expect("poisoned");
    if links.get(peer).is_some_and(|current| current.same_channel(link)) {
        links.remove(peer);
    }
}

impl CustomTransport for BleGattTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        let inbound = self
            .inner
            .inbound_rx
            .lock()
            .expect("poisoned")
            .take()
            .ok_or_else(|| io::Error::other("this transport is already bound to an endpoint"))?;
        Ok(Box::new(Endpoint {
            inner: self.inner.clone(),
            inbound,
        }))
    }
}

struct Endpoint {
    inner: Arc<Inner>,
    inbound: mpsc::Receiver<Inbound>,
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ble_gatt_iroh::Endpoint")
    }
}

impl CustomEndpoint for Endpoint {
    fn watch_local_addrs(&self) -> n0_watcher::Direct<Vec<CustomAddr>> {
        self.inner.local.watch()
    }

    fn create_sender(&self) -> Arc<dyn CustomSender> {
        Arc::new(Sender {
            inner: self.inner.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [noq_udp::RecvMeta],
        recv_infos: &mut [RecvInfo],
    ) -> Poll<io::Result<usize>> {
        assert_eq!(bufs.len(), metas.len(), "bufs and metas differ in length");
        assert_eq!(bufs.len(), recv_infos.len(), "bufs and recv_infos differ in length");
        if bufs.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let local = self.inner.local.get().into_iter().next();
        let mut filled = 0;
        while filled < bufs.len() {
            let (peer, packet) = match self.inbound.poll_recv(cx) {
                Poll::Ready(Some(item)) => item,
                Poll::Ready(None) => break,
                Poll::Pending => break,
            };
            let buf = &mut bufs[filled];
            if buf.len() < packet.len() {
                log::debug!("dropping a {} byte packet from {}: too large", packet.len(), peer.0);
                continue;
            }
            buf[..packet.len()].copy_from_slice(&packet);
            metas[filled].len = packet.len();
            metas[filled].stride = packet.len();
            recv_infos[filled] = RecvInfo::new(custom_addr(&peer), local.clone());
            filled += 1;
        }
        if filled > 0 {
            Poll::Ready(Ok(filled))
        } else {
            Poll::Pending
        }
    }
}

struct Sender {
    inner: Arc<Inner>,
}

impl fmt::Debug for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ble_gatt_iroh::Sender")
    }
}

impl CustomSender for Sender {
    fn is_valid_send_addr(&self, addr: &CustomAddr) -> bool {
        addr.id() == TRANSPORT_ID
    }

    fn poll_send(
        &self,
        _cx: &mut Context,
        dst: &CustomAddr,
        _src: Option<&CustomAddr>,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        let Some(peer) = peer_address(dst) else {
            return Poll::Ready(Err(io::Error::other("not a ble-gatt address")));
        };
        let segment = transmit.segment_size.unwrap_or(transmit.contents.len()).max(1);
        Poll::Ready(
            transmit
                .contents
                .chunks(segment)
                .try_for_each(|packet| self.send(&peer, packet.to_vec())),
        )
    }
}

impl Sender {
    /// Queues one packet for `peer`, dialling it first if there is no channel.
    /// A full queue drops the packet, as a full socket buffer would.
    fn send(&self, peer: &PeerAddress, packet: Vec<u8>) -> io::Result<()> {
        let mut links = self.inner.links.lock().expect("poisoned");
        let packet = match links.get(peer) {
            Some(link) => match link.try_send(packet) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => return Ok(()),
                // The link ended; dial again below.
                Err(mpsc::error::TrySendError::Closed(packet)) => {
                    links.remove(peer);
                    packet
                }
            },
            None => packet,
        };
        let Some(dialer) = self.inner.dialer.clone() else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("no channel to {} and no dialer", peer.0),
            ));
        };
        // Packets sent while the dial runs wait in the queue, so the QUIC
        // handshake does not have to wait for a retransmission.
        let (tx, rx) = mpsc::channel(LINK_QUEUE_DEPTH);
        let _ = tx.try_send(packet);
        links.insert(peer.clone(), tx.clone());
        drop(links);
        let inner = self.inner.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            log::debug!("dialling {}", peer.0);
            match dialer(peer.clone()).await {
                Ok(channel) => run_link(inner, peer, channel, rx, tx).await,
                Err(err) => {
                    log::debug!("dial to {} failed: {err}", peer.0);
                    remove_link(&inner, &peer, &tx);
                }
            }
        });
        Ok(())
    }
}

/// Address lookup over the addresses recorded with
/// [`BleGattTransport::set_peer_address`].
#[derive(Clone)]
pub struct BleGattAddressLookup {
    inner: Arc<Inner>,
}

impl fmt::Debug for BleGattAddressLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BleGattAddressLookup")
    }
}

impl AddressLookup for BleGattAddressLookup {
    fn resolve(
        &self,
        endpoint_id: EndpointId,
    ) -> Option<n0_future::stream::Boxed<Result<Item, address_lookup::Error>>> {
        let peer = self.inner.peers.lock().expect("poisoned").get(&endpoint_id)?.clone();
        let info = EndpointInfo {
            endpoint_id,
            data: EndpointData::from_iter([TransportAddr::Custom(custom_addr(&peer))]),
        };
        Some(Box::pin(n0_future::stream::once(Ok(Item::new(info, "ble-gatt", None)))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_round_trips_through_its_custom_form() {
        let peer = PeerAddress("AA:BB:CC:DD:EE:FF".to_string());
        let addr = custom_addr(&peer);
        assert_eq!(addr.id(), TRANSPORT_ID);
        assert_eq!(peer_address(&addr), Some(peer));
    }

    #[test]
    fn another_transports_address_is_not_a_peer() {
        let addr = CustomAddr::from_parts(0x20, b"AA:BB:CC:DD:EE:FF");
        assert_eq!(peer_address(&addr), None);
    }
}

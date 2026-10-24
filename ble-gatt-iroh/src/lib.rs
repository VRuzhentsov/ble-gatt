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
//!     .dialer(dial_with(adapter.clone(), config.clone()))
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
use std::time::{Duration, Instant};

use ble_gatt::datagram::{DatagramChannel, DatagramConfig};
use ble_gatt::{Adapter, ConnectionPriority, PeerAddress, Role};
use iroh::address_lookup::{self, AddressLookup, EndpointData, EndpointInfo, Item};
use iroh::endpoint::transports::{CustomEndpoint, CustomSender, CustomTransport, RecvInfo, Transmit};
use iroh_base::{CustomAddr, EndpointId, TransportAddr};
use tokio::sync::{mpsc, Notify};

/// The custom address id of this transport ("BLEG"). iroh keeps a registry of
/// transport ids in its `TRANSPORTS.md`; this one is private to `ble-gatt`.
pub const TRANSPORT_ID: u64 = 0x424C_4547;

/// Packets queued for one peer before further ones are dropped. QUIC treats a
/// dropped packet as loss and recovers, so a slow link sheds load instead of
/// growing memory.
const LINK_QUEUE_DEPTH: usize = 64;

/// Packets received from all peers and not yet taken by iroh.
const INBOUND_QUEUE_DEPTH: usize = 256;

/// How long a channel may carry nothing before it is closed. A live QUIC
/// connection sends keep-alives far more often than this, so only a channel
/// whose connection has ended goes idle, and closing it frees the radio.
pub const DEFAULT_LINK_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// After a link the peer dialled (we were the peripheral) ends, how long this
/// side leaves reconnecting to the peer instead of dialling it too — for an
/// address the caller set with `set_peer_address`; an address only ever seen
/// as the source of the peer's connection is not dialled at all. Both sides
/// re-dialling at once crossed on hardware: two links to one peer, each
/// tearing the other down. The dialler re-dials within a second or two (a
/// dial times out at 20s), so this only has to outlast one dial attempt.
const REDIAL_YIELD: Duration = Duration::from_secs(30);

/// A packet received from a peer.
type Inbound = (PeerAddress, Vec<u8>);

/// A future opening a datagram channel to a peer.
pub type DialFuture = Pin<Box<dyn Future<Output = ble_gatt::Result<DatagramChannel>> + Send>>;

/// Opens a datagram channel to the given address. Called the first time iroh
/// sends to an address that has no channel.
pub type Dialer = Arc<dyn Fn(PeerAddress) -> DialFuture + Send + Sync>;

/// A [`Dialer`] that opens channels with [`ble_gatt::datagram::connect`].
///
/// Each new link asks for [`ConnectionPriority::High`]: QUIC needs many
/// connection events, and on the default interval a busy radio (an AX210
/// also serving Wi-Fi or audio) missed enough of them for the link to hit its
/// supervision timeout within seconds. The request is best-effort; Linux has
/// no such control, and the channel is used either way. Links close when idle,
/// so the higher power draw lasts only while there is traffic.
pub fn dial_with(adapter: Adapter, config: DatagramConfig) -> Dialer {
    Arc::new(move |peer: PeerAddress| {
        let adapter = adapter.clone();
        let config = config.clone();
        Box::pin(async move {
            let mut channel = ble_gatt::datagram::connect(&adapter, &peer, &config).await?;
            if let Err(err) = channel.request_connection_priority(ConnectionPriority::High).await {
                log::debug!("link to {}: high connection priority not applied: {err}", peer.0);
            }
            Ok(channel)
        })
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
    link_idle_timeout: Option<Duration>,
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

    /// How long a channel may carry nothing before it is closed; default
    /// [`DEFAULT_LINK_IDLE_TIMEOUT`].
    pub fn link_idle_timeout(mut self, timeout: Duration) -> Self {
        self.link_idle_timeout = Some(timeout);
        self
    }

    pub fn build(self) -> BleGattTransport {
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_DEPTH);
        let local = self.local_address.iter().map(custom_addr).collect();
        BleGattTransport {
            inner: Arc::new(Inner {
                links: Mutex::new(HashMap::new()),
                peers: Mutex::new(HashMap::new()),
                retired: Mutex::new(HashMap::new()),
                closers: Mutex::new(HashMap::new()),
                inbound_tx,
                inbound_rx: Mutex::new(Some(inbound_rx)),
                answered: Mutex::new(HashMap::new()),
                dialer: self.dialer,
                local: n0_watcher::Watchable::new(local),
                link_idle_timeout: self.link_idle_timeout.unwrap_or(DEFAULT_LINK_IDLE_TIMEOUT),
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
    /// Addresses `set_peer_address` replaced or `forget_peer` removed, and
    /// not set again since, with the endpoint they were set for: never
    /// dialled; see `Sender::send`.
    retired: Mutex<HashMap<PeerAddress, EndpointId>>,
    /// A close signal per running link, replaced ones included; see
    /// [`BleGattTransport::close_link`].
    closers: Mutex<HashMap<PeerAddress, Vec<Arc<Notify>>>>,
    inbound_tx: mpsc::Sender<Inbound>,
    /// Taken by the one endpoint that binds this transport.
    inbound_rx: Mutex<Option<mpsc::Receiver<Inbound>>>,
    /// Addresses whose last link was one the peer dialled, with when it
    /// dropped, or `None` if it closed idle (nobody re-dials then); see
    /// [`REDIAL_YIELD`] and `Sender::send`. Cleared when we dial the address
    /// successfully.
    answered: Mutex<HashMap<PeerAddress, Option<Instant>>>,
    dialer: Option<Dialer>,
    local: n0_watcher::Watchable<Vec<CustomAddr>>,
    link_idle_timeout: Duration,
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
    /// role. iroh's packets for the peer go to the newest channel. One
    /// already attached for it keeps delivering what it receives until it
    /// closes idle or the peer drops it: when both sides dial at once, the
    /// peer may still be sending on it.
    ///
    /// The returned handle completes when the channel closes: the peer went
    /// away, it carried nothing for the idle timeout, or
    /// [`close_link`](Self::close_link) closed it.
    pub fn attach(&self, channel: DatagramChannel) -> tokio::task::JoinHandle<()> {
        let peer = channel.peer();
        let (tx, rx) = mpsc::channel(LINK_QUEUE_DEPTH);
        self.inner.answered.lock().expect("poisoned").remove(&peer);
        self.inner
            .links
            .lock()
            .expect("poisoned")
            .insert(peer.clone(), tx.clone());
        let close = add_closer(&self.inner, &peer);
        tokio::spawn(run_link(self.inner.clone(), peer, channel, rx, tx, close))
    }

    /// Closes every channel to `peer`, including one being dialled (it
    /// closes once the dial completes) and one a newer channel replaced, as
    /// if each had gone idle. For an application that knows it is done with
    /// a peer before the idle timeout: on hardware, a lingering channel to a
    /// phone's old address made BlueZ abort every dial to its new one
    /// (`le-connection-abort-by-local`).
    pub fn close_link(&self, peer: &PeerAddress) {
        let closers = self.inner.closers.lock().expect("poisoned").get(peer).cloned();
        for close in closers.into_iter().flatten() {
            // Stores a permit when the link is not waiting yet.
            close.notify_one();
        }
    }

    /// [`close_link`](Self::close_link) for `endpoint`'s current address and
    /// every address [`set_peer_address`](Self::set_peer_address) replaced
    /// for it. A channel the peer dialled from an address never set here
    /// is not reached: close it by its address.
    pub fn close_links(&self, endpoint: &EndpointId) {
        let mut addresses: Vec<PeerAddress> = self
            .inner
            .retired
            .lock()
            .expect("poisoned")
            .iter()
            .filter(|(_, owner)| *owner == endpoint)
            .map(|(address, _)| address.clone())
            .collect();
        addresses.extend(self.inner.peers.lock().expect("poisoned").get(endpoint).cloned());
        for address in &addresses {
            self.close_link(address);
        }
    }

    /// Like [`attach`](Self::attach), for a channel whose first datagram
    /// the caller already read, typically to tell QUIC from its own traffic
    /// with [`is_quic_initial`]. `first` is handed to iroh before anything
    /// else from the channel.
    pub fn attach_after(&self, channel: DatagramChannel, first: Vec<u8>) -> tokio::task::JoinHandle<()> {
        let _ = self.inner.inbound_tx.try_send((channel.peer(), first));
        self.attach(channel)
    }

    /// Records where `endpoint` can be reached, for iroh's address lookup.
    ///
    /// The address it replaces is no longer dialled, though iroh may still
    /// offer it as a path it used before: a phone's old advertising address
    /// is gone, and on BlueZ a dial to it cancelled the dial to the current
    /// one (`le-connection-abort-by-local`). An address never set here is
    /// still dialled, so connecting by an explicit `EndpointAddr` works.
    pub fn set_peer_address(&self, endpoint: EndpointId, address: PeerAddress) {
        let mut peers = self.inner.peers.lock().expect("poisoned");
        let mut retired = self.inner.retired.lock().expect("poisoned");
        retired.remove(&address);
        if let Some(old) = peers.insert(endpoint, address) {
            if !peers.values().any(|current| *current == old) {
                retired.insert(old, endpoint);
            }
        }
    }

    /// Forgets where `endpoint` can be reached. Its address is no longer
    /// dialled, as with an address [`set_peer_address`](Self::set_peer_address)
    /// replaced.
    pub fn forget_peer(&self, endpoint: &EndpointId) {
        let mut peers = self.inner.peers.lock().expect("poisoned");
        if let Some(old) = peers.remove(endpoint) {
            if !peers.values().any(|current| *current == old) {
                self.inner.retired.lock().expect("poisoned").insert(old, *endpoint);
            }
        }
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
///
/// Sending runs in its own task. A peripheral's notifications are small, so
/// one QUIC packet can take a hundred of them; receiving must not wait for
/// that, or each side's acknowledgements stall behind its own sends.
async fn run_link(
    inner: Arc<Inner>,
    peer: PeerAddress,
    mut channel: DatagramChannel,
    mut outbound: mpsc::Receiver<Vec<u8>>,
    link: mpsc::Sender<Vec<u8>>,
    close: Arc<Notify>,
) {
    log::debug!("link to {} up", peer.0);
    let role = channel.local_role();
    let (to_send, mut sending) = mpsc::channel::<Vec<u8>>(LINK_QUEUE_DEPTH);
    let sender = channel.sender();
    let send_peer = peer.clone();
    let mut send_task = tokio::spawn(async move {
        while let Some(packet) = sending.recv().await {
            if let Err(err) = sender.send(packet).await {
                log::debug!("link to {}: send failed: {err}", send_peer.0);
                break;
            }
        }
    });
    let idle = tokio::time::sleep(inner.link_idle_timeout);
    tokio::pin!(idle);
    // Closed by this side (idle or `close_link`), not dropped by the peer.
    let mut closed_here = false;
    loop {
        tokio::select! {
            () = &mut idle => {
                log::debug!("link to {}: idle, closing", peer.0);
                closed_here = true;
                break;
            }
            () = close.notified() => {
                log::debug!("link to {}: closed by the application", peer.0);
                closed_here = true;
                break;
            }
            _ = &mut send_task => break,
            packet = outbound.recv() => {
                idle.as_mut().reset(tokio::time::Instant::now() + inner.link_idle_timeout);
                let Some(packet) = packet else { break };
                // Like a full socket buffer: drop, QUIC recovers.
                if let Err(mpsc::error::TrySendError::Closed(_)) = to_send.try_send(packet) {
                    break;
                }
            }
            // `recv` is cancel-safe: a message is only taken off the queue
            // when this arm completes.
            message = channel.recv() => match message {
                Some(Ok(packet)) => {
                    idle.as_mut().reset(tokio::time::Instant::now() + inner.link_idle_timeout);
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
    send_task.abort();
    remove_closer(&inner, &peer, &close);
    let _ = channel.close().await;
    // Only while this link still owns the peer: an old inbound link ending
    // after a newer one was attached would otherwise mark the peer as having
    // dialled us, and the yield would then hold back dials the newer link's
    // side is entitled to make. A close by this side starts no yield: no
    // QUIC connection is left to resend, so the peer will not dial back, and
    // on hardware this side's next connect timed out waiting for it.
    {
        let mut links = inner.links.lock().expect("poisoned");
        if links.get(&peer).is_some_and(|current| current.same_channel(&link)) {
            if role == Role::Peripheral {
                let dropped = (!closed_here).then(Instant::now);
                inner.answered.lock().expect("poisoned").insert(peer.clone(), dropped);
            }
            links.remove(&peer);
        }
    }
    log::debug!("link to {} down", peer.0);
}

/// Registers a close signal for a new link to `peer`.
fn add_closer(inner: &Inner, peer: &PeerAddress) -> Arc<Notify> {
    let close = Arc::new(Notify::new());
    inner
        .closers
        .lock()
        .expect("poisoned")
        .entry(peer.clone())
        .or_default()
        .push(close.clone());
    close
}

fn remove_closer(inner: &Inner, peer: &PeerAddress, close: &Arc<Notify>) {
    let mut closers = inner.closers.lock().expect("poisoned");
    if let Some(list) = closers.get_mut(peer) {
        list.retain(|other| !Arc::ptr_eq(other, close));
        if list.is_empty() {
            closers.remove(peer);
        }
    }
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
        // An address the caller replaced; see `set_peer_address`. Dropped
        // like any packet with no link: QUIC uses its other paths.
        if self.inner.retired.lock().expect("poisoned").contains_key(peer) {
            log::debug!("not dialling {}: replaced by set_peer_address", peer.0);
            return Ok(());
        }
        // The last link with this address was one the peer dialled. Dropping
        // the packet reads to QUIC as loss, and it resends once a link exists.
        let answered = self.inner.answered.lock().expect("poisoned").get(peer).copied();
        if let Some(dropped) = answered {
            // Seen only as the source of the peer's own connection: iroh
            // remembers it as a path, but a phone dials from a private
            // address it never accepts connections on and soon drops, so
            // dialling it hung 20s on hardware while the address the caller
            // set went unused. Only an address the caller set is dialled.
            let set_by_caller = self.inner.peers.lock().expect("poisoned").values().any(|address| address == peer);
            if !set_by_caller {
                log::debug!("not dialling {}: only known as the source of its own connection", peer.0);
                return Ok(());
            }
            // An address the caller set: the peer re-dials after a drop, and
            // dialling it too would cross. Wait for it, for a while.
            if dropped.is_some_and(|dropped| dropped.elapsed() < REDIAL_YIELD) {
                log::debug!("not dialling {}: waiting for it to dial back", peer.0);
                return Ok(());
            }
        }
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
        // Registered before dialling, so `close_link` during the dial closes
        // the channel as soon as it is up.
        let close = add_closer(&self.inner, peer);
        let inner = self.inner.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            log::debug!("dialling {}", peer.0);
            match dialer(peer.clone()).await {
                Ok(channel) => {
                    // Reachable by dialling after all: no longer inbound-only.
                    inner.answered.lock().expect("poisoned").remove(&peer);
                    run_link(inner, peer, channel, rx, tx, close).await
                }
                Err(err) => {
                    log::debug!("dial to {} failed: {err}", peer.0);
                    remove_closer(&inner, &peer, &close);
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

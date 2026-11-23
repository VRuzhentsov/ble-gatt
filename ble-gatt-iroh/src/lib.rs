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
//!   [`PeerLink::incoming`] while it wants to be reachable) and hands
//!   each one to [`BleGattTransport::attach`];
//! - tells the transport where a peer can be reached with
//!   [`BleGattTransport::set_peer_address`], for instance after its own scan
//!   heard the peer;
//! - supplies a [`Dialer`] the transport uses to open a channel the first time
//!   iroh sends to an address with no channel yet ([`dial_with`] wraps
//!   [`PeerLink::request`]);
//! - installs [`BleGattTransport::hooks`], so the transport learns from iroh
//!   which endpoint each link carries (ADR-0008).
//!
//! The shared [`PeerLink`] owns establishment, crossed BLE attempts and native
//! cleanup. Accept incoming requests separately and pass their channels to
//! [`BleGattTransport::attach`]. A failed outgoing attempt does not fail QUIC;
//! QUIC can retransmit over an independently accepted surviving carrier.
//!
//! ```ignore
//! let transport = BleGattTransport::builder()
//!     .dialer(dial_with(peer_link.clone()))
//!     .build();
//! let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
//!     .clear_ip_transports()
//!     .add_custom_transport(Arc::new(transport.clone()))
//!     .address_lookup(transport.address_lookup())
//!     .hooks(transport.hooks())
//!     .bind()
//!     .await?;
//! ```
//!
//! [iroh]: https://docs.rs/iroh

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ble_gatt::{ConnectionPriority, PeerAddress, PeerChannel, PeerChannelCloseReason, PeerLink};
use iroh::address_lookup::{self, AddressLookup, EndpointData, EndpointInfo, Item};
use iroh::endpoint::transports::{
    CustomEndpoint, CustomSender, CustomTransport, RecvInfo, Transmit,
};
use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, Connection, ConnectionError, EndpointHooks,
    WeakConnectionHandle,
};
use iroh_base::{CustomAddr, EndpointId, TransportAddr};
use n0_future::StreamExt;
use tokio::sync::{mpsc, oneshot, Notify};

/// The custom address id of this transport ("BLEG"). iroh keeps a registry of
/// transport ids in its `TRANSPORTS.md`; this one is private to `ble-gatt`.
pub const TRANSPORT_ID: u64 = 0x424C_4547;

/// Packets queued for one peer before further ones are dropped. QUIC treats a
/// dropped packet as loss and recovers, so a slow link sheds load instead of
/// growing memory.
const LINK_QUEUE_DEPTH: usize = 64;

/// Packets received from all peers and not yet taken by iroh.
const INBOUND_QUEUE_DEPTH: usize = 256;

/// Backstop for an unused channel that carries nothing for this long.
/// With endpoint hooks installed, live iroh users protect it from idle close;
/// their last-user release owns its lifetime instead.
pub const DEFAULT_LINK_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a link stays up after the last iroh connection on it has ended
/// (ADR-0008 D3). None: the closing side waits for its CONNECTION_CLOSE to
/// be sent, so the peer has it before the link goes. An application
/// that wants a link kept for a connection it has yet to open keeps an
/// earlier connection open until then, or sets a delay.
pub const DEFAULT_LINK_RELEASE_DELAY: Duration = Duration::ZERO;

/// How often a closed connection is checked for having drained. noq reports
/// no event for it.
const DRAINED_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Replaced or forgotten addresses kept from being dialled. Past this the one
/// retired longest ago is forgotten, so address rotation does not grow memory.
/// A phone rotates its address about every 15 minutes: this covers days of
/// rotations for one peer.
const RETIRED_ADDRESS_LIMIT: usize = 256;

/// A packet received from a peer.
type Inbound = (PeerAddress, Vec<u8>);

/// What a link's queue carries to its channel.
enum Outgoing {
    Packet(Vec<u8>),
    /// Fired once every packet queued before it has been sent (or the link
    /// ended): the queue is first in, first out.
    Barrier(oneshot::Sender<()>),
}

/// A future opening a datagram channel to a peer.
pub type DialFuture = Pin<Box<dyn Future<Output = ble_gatt::Result<PeerChannel>> + Send>>;

/// Opens a datagram channel to the given address. Called the first time iroh
/// sends to an address that has no channel.
pub type Dialer = Arc<dyn Fn(PeerAddress) -> DialFuture + Send + Sync>;

/// A [`Dialer`] that acquires scoped use from a shared core worker.
///
/// Each new link asks for [`ConnectionPriority::High`]: QUIC needs many
/// connection events, and on the default interval a busy radio (a combo
/// chip also serving Wi-Fi or audio) missed enough of them for the link to hit its
/// supervision timeout within seconds. The request is best-effort; Linux has
/// no such control, and the channel is used either way. Links close when idle,
/// so the higher power draw lasts only while there is traffic.
pub fn dial_with(link: Arc<PeerLink>) -> Dialer {
    BleGattTransport::make_dialer(link)
}

/// The iroh address of a Bluetooth peer.
pub fn custom_addr(peer: &PeerAddress) -> CustomAddr {
    BleGattTransport::encode_address(peer)
}

/// The Bluetooth peer an iroh address names, if it is one of this transport's.
pub fn peer_address(addr: &CustomAddr) -> Option<PeerAddress> {
    BleGattTransport::decode_address(addr)
}

/// Routing heuristic for the first datagram on a new channel: QUIC Initials
/// have a long header (top bit set). This checks that bit only, not the packet
/// type, length, version or authenticity; iroh validates the packet. An
/// application sharing one GATT service between QUIC and its own messages
/// can route a new channel by its first datagram, provided its own first
/// byte never has the top bit set (UTF-8 JSON, for one, cannot start so).
pub fn is_quic_initial(packet: &[u8]) -> bool {
    BleGattTransport::has_initial_header(packet)
}

/// Builder for [`BleGattTransport`].
#[derive(Default)]
pub struct BleGattTransportBuilder {
    dialer: Option<Dialer>,
    local_address: Option<PeerAddress>,
    link_idle_timeout: Option<Duration>,
    link_release_delay: Option<Duration>,
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

    /// How long an unused channel may carry nothing before it is closed;
    /// live users learned through hooks protect it. Default
    /// [`DEFAULT_LINK_IDLE_TIMEOUT`].
    pub fn link_idle_timeout(mut self, timeout: Duration) -> Self {
        self.link_idle_timeout = Some(timeout);
        self
    }

    /// How long a channel stays up after the last iroh connection on it
    /// ended (ADR-0008 D3); default [`DEFAULT_LINK_RELEASE_DELAY`] (none),
    /// and it applies to every link. Applies only with
    /// [`BleGattTransport::hooks`] installed.
    pub fn link_release_delay(mut self, delay: Duration) -> Self {
        self.link_release_delay = Some(delay);
        self
    }

    pub fn build(self) -> BleGattTransport {
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_DEPTH);
        let local = self.local_address.iter().map(custom_addr).collect();
        BleGattTransport {
            inner: Arc::new(Inner {
                links: Mutex::new(HashMap::new()),
                carriers: Mutex::new(HashMap::new()),
                acquisition: Mutex::new(HashMap::new()),
                dropped: Mutex::new(HashMap::new()),
                peers: Mutex::new(HashMap::new()),
                retired: Mutex::new(HashMap::new()),
                closers: Mutex::new(HashMap::new()),
                #[cfg(test)]
                published: Mutex::new(None),
                #[cfg(test)]
                retiring: Mutex::new(None),
                #[cfg(test)]
                terminating: Mutex::new(None),

                learned: Mutex::new(HashMap::new()),
                releasers: Mutex::new(HashMap::new()),
                next_connection: AtomicU64::new(0),
                inbound_tx,
                inbound_rx: Mutex::new(Some(inbound_rx)),
                incoming_addresses: Mutex::new(HashSet::new()),
                dialer: self.dialer,
                local: n0_watcher::Watchable::new(local),
                link_idle_timeout: self.link_idle_timeout.unwrap_or(DEFAULT_LINK_IDLE_TIMEOUT),
                link_release_delay: self
                    .link_release_delay
                    .unwrap_or(DEFAULT_LINK_RELEASE_DELAY),
            }),
        }
    }
}

/// Packet-task ownership, separate from the core's native generation.
struct PacketCarrier {
    queue: mpsc::Sender<Outgoing>,
    close: Arc<Notify>,
    /// Observe core closure before the packet task publishes suppression.
    /// Removed before scoped release so this clone cannot keep idle use alive.
    channel: PeerChannel,
}

#[derive(Default)]
struct Acquisition {
    generation: u64,
    terminal: bool,
}

/// One address's owner, as iroh's connections over it showed.
struct Learned {
    endpoint: EndpointId,
    /// Live connections that have ever used the address: close barriers and
    /// ended-packet suppression need history even after a path closes.
    connections: HashSet<u64>,
    /// Connections whose current path snapshot includes this address.
    current: HashSet<u64>,
    /// Historical users that have not started closing.
    open: HashSet<u64>,
    /// Connections that used the address and have ended (their close sent
    /// or received) but not drained yet; see `Sender::send`.
    ended: HashSet<u64>,
}

/// iroh custom transport over `ble-gatt` datagram channels. Cheap to clone;
/// clones share one transport.
#[derive(Clone)]
pub struct BleGattTransport {
    inner: Arc<Inner>,
}

impl BleGattTransport {
    fn encode_address(peer: &PeerAddress) -> CustomAddr {
        CustomAddr::from_parts(TRANSPORT_ID, peer.0.as_bytes())
    }

    fn decode_address(addr: &CustomAddr) -> Option<PeerAddress> {
        if addr.id() != TRANSPORT_ID {
            return None;
        }
        String::from_utf8(addr.data().to_vec())
            .ok()
            .map(PeerAddress)
    }

    fn has_initial_header(packet: &[u8]) -> bool {
        packet.first().is_some_and(|byte| byte & 0x80 != 0)
    }

    fn make_dialer(link: Arc<PeerLink>) -> Dialer {
        Arc::new(move |peer: PeerAddress| {
            let link = link.clone();
            Box::pin(async move {
                let channel = match link.channel(&peer) {
                    Some(channel) => channel,
                    None => link.request(&peer).await?.accept().await?,
                };
                if let Err(err) = channel
                    .request_connection_priority(ConnectionPriority::High)
                    .await
                {
                    log::debug!(
                        "link to {}: high connection priority not applied: {err}",
                        peer.0
                    );
                }
                Ok(channel)
            })
        })
    }

    pub fn builder() -> BleGattTransportBuilder {
        BleGattTransportBuilder::default()
    }

    /// Carries packets over an accepted core channel. The core selects the
    /// BLE attempt. Attach an incoming winner separately from an outgoing loser.
    /// Replacement waits until the old packet receiver stops. Scoped core
    /// release protects the replacement even when it belongs to another generation.
    ///
    /// The returned handle completes when the channel closes: the peer went
    /// away, it carried nothing for the idle timeout, or
    /// [`close_link`](Self::close_link) closed it.
    pub fn attach(&self, channel: PeerChannel) -> tokio::task::JoinHandle<()> {
        self.attach_channel(channel, None)
    }

    fn attach_channel(
        &self,
        channel: PeerChannel,
        first: Option<Vec<u8>>,
    ) -> tokio::task::JoinHandle<()> {
        let peer = channel.peer();
        let (tx, rx) = mpsc::channel(LINK_QUEUE_DEPTH);
        let close = self.inner.add_closer(&peer);
        self.inner
            .incoming_addresses
            .lock()
            .expect("poisoned")
            .insert(peer.clone());
        let mut links = self.inner.links.lock().expect("poisoned");
        let mut carriers = self.inner.carriers.lock().expect("poisoned");
        let generation = if channel.close_reason().is_none() {
            self.inner.authorize_acquisition(&peer)
        } else {
            self.inner
                .acquisition
                .lock()
                .expect("poisoned")
                .entry(peer.clone())
                .or_default()
                .generation
        };
        links.insert(peer.clone(), tx.clone());
        let previous = carriers.insert(
            peer.clone(),
            PacketCarrier {
                queue: tx.clone(),
                close: close.clone(),
                channel: channel.clone(),
            },
        );
        drop(carriers);
        drop(links);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            if let Some(previous) = previous {
                previous.close.notify_one();
                previous.queue.closed().await;
            }
            inner
                .run_link(peer, channel, rx, (tx, generation), close, first)
                .await;
        })
    }

    /// Releases this transport's scoped channel use and cancels its pending request.
    /// Other core users remain valid. For global disconnect, use `PeerLink`.
    /// Dropping an iroh connect future does not cancel this transport's task.
    pub fn close_link(&self, peer: &PeerAddress) {
        let closers = self
            .inner
            .closers
            .lock()
            .expect("poisoned")
            .get(peer)
            .cloned();
        for close in closers.into_iter().flatten() {
            // Stores a permit when the link is not waiting yet.
            close.notify_one();
        }
    }

    /// [`close_link`](Self::close_link) for `endpoint`'s current address,
    /// every address [`set_peer_address`](Self::set_peer_address) replaced
    /// for it and, with [`hooks`](Self::hooks) installed, every address an
    /// iroh connection to it came over: links the peer dialled included.
    /// Without the hooks, a channel the peer dialled from an address never
    /// set here is not reached: close it by its address.
    pub fn close_links(&self, endpoint: &EndpointId) {
        for address in &self.inner.addresses_of(endpoint) {
            self.close_link(address);
        }
    }

    /// Endpoint hooks that tell the transport which endpoint each link
    /// carries, in either role, from iroh's own handshakes and paths
    /// (ADR-0008). Install them on the endpoint this transport is bound to:
    ///
    /// ```ignore
    /// iroh::Endpoint::builder(presets::Minimal)
    ///     .add_custom_transport(Arc::new(transport.clone()))
    ///     .hooks(transport.hooks())
    /// ```
    ///
    /// With them, [`close_links`](Self::close_links) reaches links the peer
    /// dialled, and a link closes once the last iroh connection on it has
    /// ended, plus any
    /// [`link_release_delay`](BleGattTransportBuilder::link_release_delay),
    /// instead of after the idle timeout. A connection the peer closed ends
    /// at once; one this side closed, once its CONNECTION_CLOSE was sent.
    /// Neither waits out QUIC's closing or draining period. The core decides
    /// whether releasing this use retires the native generation.
    ///
    /// A connection that was not closed holds the link while any
    /// `Connection` clone, `SendStream` or `RecvStream` the application
    /// keeps is alive, since a lost connection ends only when those go.
    pub fn hooks(&self) -> BleGattHooks {
        BleGattHooks {
            inner: self.inner.clone(),
        }
    }

    /// Like [`attach`](Self::attach), for a channel whose first datagram
    /// the caller already read, typically to tell QUIC from its own traffic
    /// with [`is_quic_initial`]. `first` is handed to iroh before anything
    /// else from the channel.
    pub fn attach_after(
        &self,
        channel: PeerChannel,
        first: Vec<u8>,
    ) -> tokio::task::JoinHandle<()> {
        self.attach_channel(channel, Some(first))
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
                Inner::retire_address(&mut retired, old, endpoint);
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
                Inner::retire_address(
                    &mut self.inner.retired.lock().expect("poisoned"),
                    old,
                    *endpoint,
                );
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

impl fmt::Debug for BleGattTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BleGattTransport")
            .field("links", &self.inner.links.lock().expect("poisoned").len())
            .field("dialer", &self.inner.dialer.is_some())
            .finish()
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

struct Inner {
    /// One outbound queue per peer with a channel (open or being dialled).
    links: Mutex<HashMap<PeerAddress, mpsc::Sender<Outgoing>>>,
    /// One packet receiver per attached core channel. Core owns native attempts.
    carriers: Mutex<HashMap<PeerAddress, PacketCarrier>>,
    /// Explicit application users authorize acquisition generations. Old QUIC
    /// traffic cannot reset a terminal generation; it has no hook invocation.
    acquisition: Mutex<HashMap<PeerAddress, Acquisition>>,
    /// When a link to each address last dropped a packet for a full queue;
    /// see `close_sent`.
    dropped: Mutex<HashMap<PeerAddress, Instant>>,
    /// Where each known endpoint can be reached, for address lookup.
    peers: Mutex<HashMap<EndpointId, PeerAddress>>,
    /// Addresses `set_peer_address` replaced or `forget_peer` removed, and
    /// not set again since, with the endpoint they were set for and when:
    /// never dialled; see `Sender::send`. At most `RETIRED_ADDRESS_LIMIT`.
    retired: Mutex<HashMap<PeerAddress, (EndpointId, Instant)>>,
    /// A close signal per running link, replaced ones included; see
    /// [`BleGattTransport::close_link`].
    closers: Mutex<HashMap<PeerAddress, Vec<Arc<Notify>>>>,
    #[cfg(test)]
    published: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    retiring: Mutex<Option<(oneshot::Sender<()>, Arc<Notify>)>>,
    #[cfg(test)]
    terminating: Mutex<Option<(oneshot::Sender<()>, Arc<Notify>)>>,

    /// Historical key/address associations plus separate current-path users,
    /// in either role (ADR-0008 D1). History survives path closure for close
    /// barriers and ended-packet suppression; it alone cannot defeat idle.
    learned: Mutex<HashMap<PeerAddress, Learned>>,
    /// A signal per running link, fired when the last iroh connection on its
    /// address ends (ADR-0008 D3).
    releasers: Mutex<HashMap<PeerAddress, Vec<Arc<Notify>>>>,
    next_connection: AtomicU64,
    inbound_tx: mpsc::Sender<Inbound>,
    /// Taken by the one endpoint that binds this transport.
    inbound_rx: Mutex<Option<mpsc::Receiver<Inbound>>>,
    /// Incoming source addresses are not necessarily connectable addresses.
    incoming_addresses: Mutex<HashSet<PeerAddress>>,
    dialer: Option<Dialer>,
    local: n0_watcher::Watchable<Vec<CustomAddr>>,
    link_idle_timeout: Duration,
    link_release_delay: Duration,
}

impl Inner {
    fn authorize_acquisition(&self, peer: &PeerAddress) -> u64 {
        let mut acquisitions = self.acquisition.lock().expect("poisoned");
        let acquisition = acquisitions.entry(peer.clone()).or_default();
        acquisition.generation += 1;
        acquisition.terminal = false;
        acquisition.generation
    }

    /// Records `address`, set for `endpoint`, as retired. Past
    /// `RETIRED_ADDRESS_LIMIT` the address retired longest ago is forgotten.
    fn retire_address(
        retired: &mut HashMap<PeerAddress, (EndpointId, Instant)>,
        address: PeerAddress,
        endpoint: EndpointId,
    ) {
        retired.insert(address, (endpoint, Instant::now()));
        if retired.len() > RETIRED_ADDRESS_LIMIT {
            let oldest = retired
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(address, _)| address.clone());
            if let Some(oldest) = oldest {
                retired.remove(&oldest);
            }
        }
    }
}

/// Moves packets between one datagram channel and iroh until either side
/// ends.
///
/// Sending runs in its own task. A peripheral's notifications are small, so
/// one QUIC packet can take a hundred of them; receiving must not wait for
/// that, or each side's acknowledgements stall behind its own sends.
impl Inner {
    async fn run_link(
        self: Arc<Self>,
        peer: PeerAddress,
        channel: PeerChannel,
        mut outbound: mpsc::Receiver<Outgoing>,
        ownership: (mpsc::Sender<Outgoing>, u64),
        close: Arc<Notify>,
        mut first: Option<Vec<u8>>,
    ) {
        let (link, generation) = ownership;
        log::debug!("link to {} up", peer.0);

        // A new Initial can establish another QUIC user. Late short-header
        // traffic must not retain a released address without a legitimate user.
        let mut establishing = false;
        let (to_send, mut sending) = mpsc::channel::<Outgoing>(LINK_QUEUE_DEPTH);
        let sender = channel.clone();
        let send_peer = peer.clone();
        let mut send_task = tokio::spawn(async move {
            while let Some(outgoing) = sending.recv().await {
                let packet = match outgoing {
                    Outgoing::Packet(packet) => packet,
                    Outgoing::Barrier(reached) => {
                        let _ = reached.send(());
                        continue;
                    }
                };
                if let Err(err) = sender.send(packet).await {
                    log::debug!("link to {}: send failed: {err}", send_peer.0);
                    break;
                }
            }
        });
        let idle = tokio::time::sleep(self.link_idle_timeout);
        tokio::pin!(idle);
        let release = self.add_releaser(&peer);
        // Armed once the last iroh connection on the link ended; a connection
        // that takes the link over before it fires disarms it.
        let released = tokio::time::sleep(Duration::MAX);
        tokio::pin!(released);
        let mut release_armed = false;

        let mut sender_finished = false;
        loop {
            tokio::select! {
                () = &mut idle => {
                    // Current QUIC users protect this transport's scoped use.
                    if self.in_use(&peer) {
                        idle.as_mut().reset(tokio::time::Instant::now() + self.link_idle_timeout);
                        continue;
                    }
                    log::debug!("link to {}: idle, closing", peer.0);
                    break;
                }
                () = release.notified() => {
                    if !self.in_use(&peer) {
                        released.as_mut().reset(tokio::time::Instant::now() + self.link_release_delay);
                        release_armed = true;
                    }
                }
                () = &mut released, if release_armed => {
                    if self.in_use(&peer) {
                        release_armed = false;
                        continue;
                    }
                    log::debug!("link to {}: no iroh connection left on it, closing", peer.0);
                    break;
                }
                () = close.notified() => {
                    log::debug!("link to {}: closed by the application", peer.0);
                    break;
                }
                _ = &mut send_task => { sender_finished = true; break },
                packet = outbound.recv() => {
                    idle.as_mut().reset(tokio::time::Instant::now() + self.link_idle_timeout);
                    let Some(packet) = packet else { break };
                    match to_send.try_send(packet) {
                        Ok(()) => {}
                        // A barrier is never dropped: it waits for room, behind
                        // the packets queued before it.
                        Err(mpsc::error::TrySendError::Full(barrier @ Outgoing::Barrier(_))) => {
                            if to_send.send(barrier).await.is_err() { break; }
                        }
                        // Like a full socket buffer: drop, QUIC recovers.
                        Err(mpsc::error::TrySendError::Full(Outgoing::Packet(_))) => {
                            self.dropped.lock().expect("poisoned").insert(peer.clone(), Instant::now());
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                // `recv` is cancel-safe: a message is only taken off the queue
                // when this arm completes.
                message = async {
                    match first.take() {
                        Some(packet) => Some(Ok(packet)),
                        None => channel.recv().await,
                    }
                } => match message {
                    Some(Ok(packet)) => {
                        establishing |= is_quic_initial(&packet);
                        if !establishing && self.only_ended(&peer) {
                            log::debug!("link to {}: late traffic for ended connections, closing carrier", peer.0);
                            break;
                        }
                        idle.as_mut().reset(tokio::time::Instant::now() + self.link_idle_timeout);
                        // Like a full socket buffer: drop, QUIC recovers.
                        if let Err(mpsc::error::TrySendError::Closed(_)) =
                            self.inbound_tx.try_send((peer.clone(), packet))
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
        if !sender_finished {
            // An in-flight core send may have been cancelled before completion.
            self.dropped
                .lock()
                .expect("poisoned")
                .insert(peer.clone(), Instant::now());
            send_task.abort();
            let _ = send_task.await;
        }
        #[cfg(test)]
        {
            let terminating = self.terminating.lock().expect("poisoned").take();
            if let Some((reached, resume)) = terminating {
                let _ = reached.send(());
                resume.notified().await;
            }
        }
        // Publish terminal suppression before closing the queue. Sender holds
        // links while testing it, so it cannot observe a gap and start a request.
        // A stale receiver cannot suppress an explicitly attached replacement.
        {
            let links = self.links.lock().expect("poisoned");
            if links
                .get(&peer)
                .is_some_and(|current| current.same_channel(&link))
                && matches!(
                    channel.close_reason(),
                    Some(
                        PeerChannelCloseReason::LocalDisconnect
                            | PeerChannelCloseReason::PeerDisconnect
                            | PeerChannelCloseReason::ProtocolError
                            | PeerChannelCloseReason::Shutdown
                    )
                )
            {
                let mut acquisitions = self.acquisition.lock().expect("poisoned");
                let acquisition = acquisitions.entry(peer.clone()).or_default();
                if acquisition.generation == generation {
                    acquisition.terminal = true;
                }
            }
            outbound.close();
        }
        if !outbound.is_empty() || !to_send.is_closed() {
            self.dropped
                .lock()
                .expect("poisoned")
                .insert(peer.clone(), Instant::now());
        }
        self.remove_closer(&peer, &close);
        self.remove_releaser(&peer, &release);

        #[cfg(test)]
        {
            let retiring = self.retiring.lock().expect("poisoned").take();
            if let Some((reached, resume)) = retiring {
                let _ = reached.send(());
                resume.notified().await;
            }
        }
        // Drop the observation clone before scoped release. A stale task cannot
        // remove the current carrier (or keep its own otherwise-last use alive).
        {
            let mut carriers = self.carriers.lock().expect("poisoned");
            if carriers
                .get(&peer)
                .is_some_and(|current| current.queue.same_channel(&link))
            {
                carriers.remove(&peer);
            }
        }
        // Core release preserves other scoped channel users.
        let _ = channel.release().await;

        // A retired task cannot remove a replacement queue or its user history.
        {
            let mut links = self.links.lock().expect("poisoned");
            if links
                .get(&peer)
                .is_some_and(|current| current.same_channel(&link))
            {
                links.remove(&peer);
                // No link is left at the address. A connection still using it
                // keeps the entry, for `close_links` and a link that replaces it.
                let mut learned = self.learned.lock().expect("poisoned");
                if learned.get(&peer).is_some_and(|learned| {
                    learned.connections.is_empty() && learned.ended.is_empty()
                }) {
                    learned.remove(&peer);
                }
            }
        }
        log::debug!("link to {} down", peer.0);
    }

    /// Registers a close signal for a new link to `peer`.
    fn add_closer(&self, peer: &PeerAddress) -> Arc<Notify> {
        let close = Arc::new(Notify::new());
        self.closers
            .lock()
            .expect("poisoned")
            .entry(peer.clone())
            .or_default()
            .push(close.clone());
        close
    }

    /// Every address known for `endpoint`: the one `set_peer_address` set, the
    /// ones it replaced, and the ones iroh's connections to it came over.
    fn addresses_of(&self, endpoint: &EndpointId) -> Vec<PeerAddress> {
        let mut addresses: Vec<PeerAddress> = self
            .retired
            .lock()
            .expect("poisoned")
            .iter()
            .filter(|(_, (owner, _))| owner == endpoint)
            .map(|(address, _)| address.clone())
            .collect();
        addresses.extend(self.peers.lock().expect("poisoned").get(endpoint).cloned());
        for (address, learned) in self.learned.lock().expect("poisoned").iter() {
            if learned.endpoint == *endpoint && !addresses.contains(address) {
                addresses.push(address.clone());
            }
        }
        addresses
    }

    /// Records that iroh connection `connection` to `endpoint` uses `addr`.
    fn learn(&self, endpoint: EndpointId, connection: u64, addr: &TransportAddr) {
        let TransportAddr::Custom(addr) = addr else {
            return;
        };
        let Some(address) = peer_address(addr) else {
            return;
        };
        let mut learned = self.learned.lock().expect("poisoned");
        let entry = learned.entry(address.clone()).or_insert_with(|| Learned {
            endpoint,
            connections: HashSet::new(),
            current: HashSet::new(),
            open: HashSet::new(),
            ended: HashSet::new(),
        });
        // A private address another device now uses.
        if entry.endpoint != endpoint {
            entry.endpoint = endpoint;
            entry.connections.clear();
            entry.current.clear();
            entry.open.clear();
            entry.ended.clear();
        }
        entry.open.insert(connection);
        entry.current.insert(connection);
        if entry.connections.insert(connection) {
            log::debug!("link to {}: carries {}", address.0, endpoint.fmt_short());
        }
    }

    /// Reconciles one connection's current path snapshot. Events are wakeups,
    /// not a delta log: snapshots also recover missed events.
    fn reconcile_paths(
        &self,
        endpoint: EndpointId,
        connection: u64,
        paths: impl IntoIterator<Item = TransportAddr>,
    ) {
        let mut current = HashSet::new();
        for addr in paths {
            self.learn(endpoint, connection, &addr);
            if let TransportAddr::Custom(addr) = addr {
                if let Some(address) = peer_address(&addr) {
                    current.insert(address);
                }
            }
        }
        // Add before removing: address rotation or multiple paths at one address
        // must never expose a transient unowned carrier to its idle timer.
        for (address, learned) in self.learned.lock().expect("poisoned").iter_mut() {
            if !current.contains(address) {
                learned.current.remove(&connection);
            }
        }
    }

    /// iroh connection `connection` started to close.
    fn connection_closing(&self, connection: u64) {
        for learned in self.learned.lock().expect("poisoned").values_mut() {
            learned.open.remove(&connection);
        }
    }

    /// iroh connection `connection` ended: each link it used and no other live
    /// connection uses is released.
    fn forget_connection(&self, connection: u64) {
        let mut released = Vec::new();
        for (address, learned) in self.learned.lock().expect("poisoned").iter_mut() {
            learned.current.remove(&connection);
            if learned.connections.remove(&connection) {
                learned.ended.insert(connection);
                if learned.connections.is_empty() {
                    released.push(address.clone());
                }
            }
        }
        let releasers = self.releasers.lock().expect("poisoned");
        for address in &released {
            log::debug!(
                "link to {}: its last connection ended, {} link(s) to release",
                address.0,
                releasers.get(address).map_or(0, Vec::len)
            );
            for release in releasers.get(address).into_iter().flatten() {
                // Stores a permit when the link is not waiting yet.
                release.notify_one();
            }
        }
    }

    /// iroh connection `connection`, ended earlier, has drained. An address with
    /// no link left and nothing else using it is forgotten.
    fn connection_drained(&self, connection: u64) {
        // `links` before `learned`, as in `run_link`.
        let links = self.links.lock().expect("poisoned");
        self.learned
            .lock()
            .expect("poisoned")
            .retain(|address, learned| {
                learned.ended.remove(&connection);
                !learned.connections.is_empty()
                    || !learned.ended.is_empty()
                    || links.contains_key(address)
            });
    }

    /// Whether only connections that have ended use `peer`.
    fn only_ended(&self, peer: &PeerAddress) -> bool {
        self.learned
            .lock()
            .expect("poisoned")
            .get(peer)
            .is_some_and(|learned| learned.connections.is_empty() && !learned.ended.is_empty())
    }

    /// Whether a current path uses `peer`, or a historical user is still sending
    /// its close. Historical open connections alone do not defeat idle cleanup.
    fn in_use(&self, peer: &PeerAddress) -> bool {
        self.learned
            .lock()
            .expect("poisoned")
            .get(peer)
            .is_some_and(|learned| {
                !learned.current.is_empty()
                    || learned
                        .connections
                        .iter()
                        .any(|id| !learned.open.contains(id))
            })
    }
}

/// Waits until connection `connection`, closed by this side, has sent its
/// CONNECTION_CLOSE; false if it drained first. noq counts the frame when it
/// builds the packet and hands the packet to the transport under the same
/// lock, so once counted it is in a link's queue, and a barrier queued
/// behind it on each link the connection used comes out once it was sent.
/// Neither noq nor iroh signals either moment, so the count is checked like
/// the drain.
///
/// A full queue drops packets without telling noq, so the barrier alone
/// does not prove the close went out. When a link the connection used
/// dropped one since the close started (`closing`, when the hooks saw it,
/// less one check interval for the wake-up between the two), the close may
/// be the one lost, and this waits for the drain, as before.
impl Inner {
    async fn close_sent(
        &self,
        weak: &WeakConnectionHandle,
        connection: u64,
        closing: Instant,
    ) -> bool {
        loop {
            // Dropped before the sleep: a kept `Connection` would hold the
            // drain off.
            let built = match weak.upgrade() {
                Some(conn) => conn.stats().frame_tx.connection_close > 0,
                None => return false,
            };
            if built {
                break;
            }
            tokio::time::sleep(DRAINED_CHECK_INTERVAL).await;
        }
        // `learned` is released before `links` is taken: `run_link` takes them
        // the other way round.
        let addresses: Vec<PeerAddress> = self
            .learned
            .lock()
            .expect("poisoned")
            .iter()
            .filter(|(_, learned)| learned.connections.contains(&connection))
            .map(|(address, _)| address.clone())
            .collect();
        let links: Vec<mpsc::Sender<Outgoing>> = {
            let links = self.links.lock().expect("poisoned");
            addresses
                .iter()
                .filter_map(|address| links.get(address).cloned())
                .collect()
        };
        let sent = async {
            for link in links {
                let (reached, barrier) = oneshot::channel();
                if link.send(Outgoing::Barrier(reached)).await.is_err() || barrier.await.is_err() {
                    return false;
                }
            }
            true
        };
        let drained = async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(DRAINED_CHECK_INTERVAL).await;
            }
        };
        tokio::pin!(drained);
        tokio::select! {
            sent = sent => {
                if !sent { drained.await; return false; }
            }
            () = &mut drained => return false,
        }
        let since = closing
            .checked_sub(DRAINED_CHECK_INTERVAL)
            .unwrap_or(closing);
        let lost = {
            let dropped = self.dropped.lock().expect("poisoned");
            addresses
                .iter()
                .any(|address| dropped.get(address).is_some_and(|at| *at >= since))
        };
        if lost {
            log::debug!("connection {connection}: a packet was dropped while it closed, waiting for it to drain");
            drained.await;
            return false;
        }
        true
    }

    fn add_releaser(&self, peer: &PeerAddress) -> Arc<Notify> {
        let release = Arc::new(Notify::new());
        self.releasers
            .lock()
            .expect("poisoned")
            .entry(peer.clone())
            .or_default()
            .push(release.clone());
        release
    }

    fn remove_releaser(&self, peer: &PeerAddress, release: &Arc<Notify>) {
        let mut releasers = self.releasers.lock().expect("poisoned");
        if let Some(list) = releasers.get_mut(peer) {
            list.retain(|other| !Arc::ptr_eq(other, release));
            if list.is_empty() {
                releasers.remove(peer);
            }
        }
    }

    fn remove_closer(&self, peer: &PeerAddress, close: &Arc<Notify>) {
        let mut closers = self.closers.lock().expect("poisoned");
        if let Some(list) = closers.get_mut(peer) {
            list.retain(|other| !Arc::ptr_eq(other, close));
            if list.is_empty() {
                closers.remove(peer);
            }
        }
    }

    /// Moves a failed attempt's queued packets to a separately attached carrier.
    /// Queue overflow remains packet loss. QUIC can retransmit those packets.
    fn forward_pending(&self, peer: &PeerAddress, pending: &mut mpsc::Receiver<Outgoing>) {
        pending.close();
        let selected = self
            .carriers
            .lock()
            .expect("poisoned")
            .get(peer)
            .map(|carrier| carrier.queue.clone());
        while let Ok(packet) = pending.try_recv() {
            if selected
                .as_ref()
                .is_none_or(|selected| selected.try_send(packet).is_err())
            {
                self.dropped
                    .lock()
                    .expect("poisoned")
                    .insert(peer.clone(), Instant::now());
            }
        }
    }

    /// Removes `peer`'s link only if it is still `link`: a replacement attached
    /// meanwhile stays.
    fn remove_link(&self, peer: &PeerAddress, link: &mpsc::Sender<Outgoing>) {
        let mut links = self.links.lock().expect("poisoned");
        if links
            .get(peer)
            .is_some_and(|current| current.same_channel(link))
        {
            links.remove(peer);
        }
    }
}

/// Endpoint hooks from [`BleGattTransport::hooks`].
#[derive(Clone)]
pub struct BleGattHooks {
    inner: Arc<Inner>,
}

impl fmt::Debug for BleGattHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ble_gatt_iroh::BleGattHooks")
    }
}

impl EndpointHooks for BleGattHooks {
    async fn before_connect<'a>(
        &'a self,
        remote: &'a iroh::EndpointAddr,
        _alpn: &'a [u8],
    ) -> BeforeConnectOutcome {
        // This callback represents a new application connect, never a QUIC
        // retransmission. Authorize its generation without inspecting packets.
        let _links = self.inner.links.lock().expect("poisoned");
        let mut addresses: HashSet<_> = self.inner.addresses_of(&remote.id).into_iter().collect();
        // Caller destinations authorize this application action, but are not
        // authenticated key/address associations. Only after_handshake learns
        // those. Sender still rejects retired and unknown incoming addresses.
        addresses.extend(remote.addrs.iter().filter_map(|addr| match addr {
            TransportAddr::Custom(addr) => peer_address(addr),
            _ => None,
        }));
        for peer in addresses {
            // Sharing a healthy carrier does not change its authorization:
            // a later terminal close must suppress every existing QUIC user.
            let terminal = self
                .inner
                .acquisition
                .lock()
                .expect("poisoned")
                .get(&peer)
                .is_some_and(|acquisition| acquisition.terminal);
            // Core publishes closure before waking the packet task. A new
            // application connect in that interval must not inherit the old
            // generation just because its queue is not closed yet. Healthy
            // shared carriers still keep all existing users in one generation.
            let core_closed = self
                .inner
                .carriers
                .lock()
                .expect("poisoned")
                .get(&peer)
                .is_some_and(|carrier| carrier.channel.close_reason().is_some());
            if terminal || core_closed {
                self.inner.authorize_acquisition(&peer);
            }
        }
        BeforeConnectOutcome::Accept
    }

    async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
        let endpoint = conn.remote_id();
        let connection = self.inner.next_connection.fetch_add(1, Ordering::Relaxed);
        // Neither keeps the connection alive; see `EndpointHooks`.
        // Subscribe before snapshotting. Every event (including Closed and
        // Lagged) triggers a fresh snapshot, so queued older Opened events
        // cannot resurrect ownership after that path has already closed.
        let mut events = conn.path_events();
        self.inner.reconcile_paths(
            endpoint,
            connection,
            conn.paths().iter().map(|path| path.remote_addr().clone()),
        );
        let weak = conn.weak_handle();
        let closed = weak.closed();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            tokio::pin!(closed);
            loop {
                tokio::select! {
                    _ = &mut closed => break,
                    event = events.next() => match event {
                        Some(_) => {
                            if let Some(conn) = weak.upgrade() {
                                // A closing connection retains its carrier
                                // until the existing close-frame barrier.
                                if conn.close_reason().is_none() {
                                    inner.reconcile_paths(endpoint, connection,
                                        conn.paths().iter().map(|path| path.remote_addr().clone()));
                                }
                            }
                        }
                        None => break,
                    },
                }
            }
            inner.connection_closing(connection);
            // `closed` fires when the close starts: at `close()`, before its
            // CONNECTION_CLOSE is even sent, or when the peer's arrives.
            let reason = weak.upgrade().and_then(|conn| conn.close_reason());
            let closing = Instant::now();
            match reason {
                // Closed by the peer, reset, or silent: this side is draining
                // and sends nothing more (RFC 9000 §10.2.2), so ending the
                // link now loses nothing. Waiting out the drain instead kept
                // a phone's link up 13s on hardware, and the old link made
                // BlueZ abort every dial to the phone meanwhile.
                Some(
                    ConnectionError::ConnectionClosed(_)
                    | ConnectionError::ApplicationClosed(_)
                    | ConnectionError::Reset
                    | ConnectionError::TimedOut,
                ) => {
                    log::debug!(
                        "connection {connection} to {}: closed by the peer ({reason:?}), its links can go",
                        endpoint.fmt_short()
                    );
                }
                // Closed by this side. Cutting the link at once lost the
                // peer's copy of the CONNECTION_CLOSE on hardware, so the link
                // goes once that packet has been sent, without the closing
                // period after it: like `iroh-ble-transport`, which tears its
                // link down as soon as a connection closes.
                _ => {
                    log::debug!(
                        "connection {connection} to {}: closing ({reason:?}), waiting for its CONNECTION_CLOSE to go out",
                        endpoint.fmt_short()
                    );
                    let sent = inner.close_sent(&weak, connection, closing).await;
                    log::debug!(
                        "connection {connection} to {}: {} after {:?}",
                        endpoint.fmt_short(),
                        if sent {
                            "CONNECTION_CLOSE sent"
                        } else {
                            "drained"
                        },
                        closing.elapsed()
                    );
                }
            }
            inner.forget_connection(connection);
            // Still known until drained, so its late packets dial nothing.
            while weak.upgrade().is_some() {
                tokio::time::sleep(DRAINED_CHECK_INTERVAL).await;
            }
            inner.connection_drained(connection);
        });
        AfterHandshakeOutcome::accept()
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
        assert_eq!(
            bufs.len(),
            recv_infos.len(),
            "bufs and recv_infos differ in length"
        );
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
                log::debug!(
                    "dropping a {} byte packet from {}: too large",
                    packet.len(),
                    peer.0
                );
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
        let segment = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .max(1);
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
            Some(link) => match link.try_send(Outgoing::Packet(packet)) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.inner
                        .dropped
                        .lock()
                        .expect("poisoned")
                        .insert(peer.clone(), Instant::now());
                    return Ok(());
                }
                // The link ended; dial again below.
                Err(mpsc::error::TrySendError::Closed(packet)) => {
                    links.remove(peer);
                    packet
                }
            },
            None => Outgoing::Packet(packet),
        };
        // An address the caller replaced; see `set_peer_address`. Dropped
        // like any packet with no link: QUIC uses its other paths.
        if self
            .inner
            .acquisition
            .lock()
            .expect("poisoned")
            .get(peer)
            .is_some_and(|acquisition| acquisition.terminal)
        {
            return Ok(());
        }
        if self
            .inner
            .retired
            .lock()
            .expect("poisoned")
            .contains_key(peer)
        {
            log::debug!("not dialling {}: replaced by set_peer_address", peer.0);
            return Ok(());
        }
        // Only connections that have ended use the address. A closing one
        // answers the peer's late packets with its CONNECTION_CLOSE, and
        // dialling to carry that brought a link back up right after its
        // release. A new connection starts with an Initial, which dials.
        if let Outgoing::Packet(bytes) = &packet {
            if !is_quic_initial(bytes) && self.inner.only_ended(peer) {
                log::debug!(
                    "not dialling {}: only connections that have ended use it",
                    peer.0
                );
                return Ok(());
            }
        }
        // The last link with this address was one the peer dialled. Dropping
        // the packet reads to QUIC as loss, and it resends once a link exists.
        let incoming = self
            .inner
            .incoming_addresses
            .lock()
            .expect("poisoned")
            .contains(peer);
        if incoming {
            // Seen only as the source of the peer's own connection: iroh
            // remembers it as a path, but a phone dials from a private
            // address it never accepts connections on and soon drops, so
            // dialling it hung 20s on hardware while the address the caller
            // set went unused. Only an address the caller set is dialled.
            let set_by_caller = self
                .inner
                .peers
                .lock()
                .expect("poisoned")
                .values()
                .any(|address| address == peer);
            if !set_by_caller {
                log::debug!(
                    "not dialling {}: only known as the source of its own connection",
                    peer.0
                );
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
        let (tx, mut rx) = mpsc::channel(LINK_QUEUE_DEPTH);
        let _ = tx.try_send(packet);
        // Lock order: links, then closers. Observers cannot see a queue
        // until its cancellation signal is registered.
        let close = self.inner.add_closer(peer);
        let generation = self
            .inner
            .acquisition
            .lock()
            .expect("poisoned")
            .entry(peer.clone())
            .or_default()
            .generation;
        links.insert(peer.clone(), tx.clone());
        drop(links);
        #[cfg(test)]
        if let Some(published) = self.inner.published.lock().expect("poisoned").take() {
            published();
        }
        let inner = self.inner.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            // `close_link` cancels the dial: dropping the dialer's future
            // drops the connect under it, which on Linux cancels BlueZ's
            // pending Connect. Waiting for the dial instead kept BlueZ
            // busy, unable to scan or advertise, until its 20s timeout.
            let dialled = tokio::select! {
                biased;
                () = close.notified() => {
                    log::debug!("dial to {}: cancelled by the application", peer.0);
                    inner.remove_closer(&peer, &close);
                    inner.remove_link(&peer, &tx);
                    return;
                }
                dialled = async { dialer(peer.clone()).await } => dialled,
            };
            match dialled {
                Ok(channel) => {
                    let owns_queue = {
                        let links = inner.links.lock().expect("poisoned");
                        let owns = links
                            .get(&peer)
                            .is_some_and(|current| current.same_channel(&tx));
                        if owns {
                            inner.carriers.lock().expect("poisoned").insert(
                                peer.clone(),
                                PacketCarrier {
                                    queue: tx.clone(),
                                    close: close.clone(),
                                    channel: channel.clone(),
                                },
                            );
                        }
                        owns
                    };
                    if !owns_queue {
                        inner.forward_pending(&peer, &mut rx);
                        inner.remove_closer(&peer, &close);
                        let _ = channel.release().await;
                        return;
                    }
                    // Reachable by dialling after all: no longer inbound-only.
                    inner
                        .incoming_addresses
                        .lock()
                        .expect("poisoned")
                        .remove(&peer);
                    inner
                        .run_link(peer, channel, rx, (tx, generation), close, None)
                        .await;
                }
                Err(err) => {
                    log::debug!("dial to {} failed: {err}", peer.0);
                    // Attempt failure does not fail a legitimate QUIC user.
                    inner.forward_pending(&peer, &mut rx);
                    inner.remove_closer(&peer, &close);
                    inner.remove_link(&peer, &tx);
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
        let peer = self
            .inner
            .peers
            .lock()
            .expect("poisoned")
            .get(&endpoint_id)?
            .clone();
        let info = EndpointInfo {
            endpoint_id,
            data: EndpointData::from_iter([TransportAddr::Custom(custom_addr(&peer))]),
        };
        Some(Box::pin(n0_future::stream::once(Ok(Item::new(
            info, "ble-gatt", None,
        )))))
    }
}

#[cfg(test)]
mod tests;

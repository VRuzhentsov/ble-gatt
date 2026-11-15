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

/// The iroh address of a Bluetooth peer.
pub fn custom_addr(peer: &PeerAddress) -> CustomAddr {
    CustomAddr::from_parts(TRANSPORT_ID, peer.0.as_bytes())
}

/// The Bluetooth peer an iroh address names, if it is one of this transport's.
pub fn peer_address(addr: &CustomAddr) -> Option<PeerAddress> {
    if addr.id() != TRANSPORT_ID {
        return None;
    }
    String::from_utf8(addr.data().to_vec())
        .ok()
        .map(PeerAddress)
}

/// Routing heuristic for the first datagram on a new channel: QUIC Initials
/// have a long header (top bit set). This checks that bit only, not the packet
/// type, length, version or authenticity; iroh validates the packet. An
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

/// iroh custom transport over `ble-gatt` datagram channels. Cheap to clone;
/// clones share one transport.
#[derive(Clone)]
pub struct BleGattTransport {
    inner: Arc<Inner>,
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
    /// not set again since, with the endpoint they were set for: never
    /// dialled; see `Sender::send`.
    retired: Mutex<HashMap<PeerAddress, EndpointId>>,
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

fn authorize_acquisition(inner: &Inner, peer: &PeerAddress) -> u64 {
    let mut acquisitions = inner.acquisition.lock().expect("poisoned");
    let acquisition = acquisitions.entry(peer.clone()).or_default();
    acquisition.generation += 1;
    acquisition.terminal = false;
    acquisition.generation
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
        let close = add_closer(&self.inner, &peer);
        self.inner
            .incoming_addresses
            .lock()
            .expect("poisoned")
            .insert(peer.clone());
        let mut links = self.inner.links.lock().expect("poisoned");
        let mut carriers = self.inner.carriers.lock().expect("poisoned");
        let generation = if channel.close_reason().is_none() {
            authorize_acquisition(&self.inner, &peer)
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
            run_link(inner, peer, channel, rx, (tx, generation), close, first).await;
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
        for address in &addresses_of(&self.inner, endpoint) {
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
                self.inner
                    .retired
                    .lock()
                    .expect("poisoned")
                    .insert(old, *endpoint);
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
    let idle = tokio::time::sleep(inner.link_idle_timeout);
    tokio::pin!(idle);
    let release = add_releaser(&inner, &peer);
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
                if in_use(&inner, &peer) {
                    idle.as_mut().reset(tokio::time::Instant::now() + inner.link_idle_timeout);
                    continue;
                }
                log::debug!("link to {}: idle, closing", peer.0);
                break;
            }
            () = release.notified() => {
                if !in_use(&inner, &peer) {
                    released.as_mut().reset(tokio::time::Instant::now() + inner.link_release_delay);
                    release_armed = true;
                }
            }
            () = &mut released, if release_armed => {
                if in_use(&inner, &peer) {
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
                idle.as_mut().reset(tokio::time::Instant::now() + inner.link_idle_timeout);
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
                        inner.dropped.lock().expect("poisoned").insert(peer.clone(), Instant::now());
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
                    if !establishing && only_ended(&inner, &peer) {
                        log::debug!("link to {}: late traffic for ended connections, closing carrier", peer.0);
                        break;
                    }
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
    if !sender_finished {
        // An in-flight core send may have been cancelled before completion.
        inner
            .dropped
            .lock()
            .expect("poisoned")
            .insert(peer.clone(), Instant::now());
        send_task.abort();
        let _ = send_task.await;
    }
    #[cfg(test)]
    {
        let terminating = inner.terminating.lock().expect("poisoned").take();
        if let Some((reached, resume)) = terminating {
            let _ = reached.send(());
            resume.notified().await;
        }
    }
    // Publish terminal suppression before closing the queue. Sender holds
    // links while testing it, so it cannot observe a gap and start a request.
    // A stale receiver cannot suppress an explicitly attached replacement.
    {
        let links = inner.links.lock().expect("poisoned");
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
            let mut acquisitions = inner.acquisition.lock().expect("poisoned");
            let acquisition = acquisitions.entry(peer.clone()).or_default();
            if acquisition.generation == generation {
                acquisition.terminal = true;
            }
        }
        outbound.close();
    }
    if !outbound.is_empty() || !to_send.is_closed() {
        inner
            .dropped
            .lock()
            .expect("poisoned")
            .insert(peer.clone(), Instant::now());
    }
    remove_closer(&inner, &peer, &close);
    remove_releaser(&inner, &peer, &release);

    #[cfg(test)]
    {
        let retiring = inner.retiring.lock().expect("poisoned").take();
        if let Some((reached, resume)) = retiring {
            let _ = reached.send(());
            resume.notified().await;
        }
    }
    // Drop the observation clone before scoped release. A stale task cannot
    // remove the current carrier (or keep its own otherwise-last use alive).
    {
        let mut carriers = inner.carriers.lock().expect("poisoned");
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
        let mut links = inner.links.lock().expect("poisoned");
        if links
            .get(&peer)
            .is_some_and(|current| current.same_channel(&link))
        {
            links.remove(&peer);
            // No link is left at the address. A connection still using it
            // keeps the entry, for `close_links` and a link that replaces it.
            let mut learned = inner.learned.lock().expect("poisoned");
            if learned
                .get(&peer)
                .is_some_and(|learned| learned.connections.is_empty() && learned.ended.is_empty())
            {
                learned.remove(&peer);
            }
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

/// Every address known for `endpoint`: the one `set_peer_address` set, the
/// ones it replaced, and the ones iroh's connections to it came over.
fn addresses_of(inner: &Inner, endpoint: &EndpointId) -> Vec<PeerAddress> {
    let mut addresses: Vec<PeerAddress> = inner
        .retired
        .lock()
        .expect("poisoned")
        .iter()
        .filter(|(_, owner)| *owner == endpoint)
        .map(|(address, _)| address.clone())
        .collect();
    addresses.extend(inner.peers.lock().expect("poisoned").get(endpoint).cloned());
    for (address, learned) in inner.learned.lock().expect("poisoned").iter() {
        if learned.endpoint == *endpoint && !addresses.contains(address) {
            addresses.push(address.clone());
        }
    }
    addresses
}

/// Records that iroh connection `connection` to `endpoint` uses `addr`.
fn learn(inner: &Inner, endpoint: EndpointId, connection: u64, addr: &TransportAddr) {
    let TransportAddr::Custom(addr) = addr else {
        return;
    };
    let Some(address) = peer_address(addr) else {
        return;
    };
    let mut learned = inner.learned.lock().expect("poisoned");
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
    inner: &Inner,
    endpoint: EndpointId,
    connection: u64,
    paths: impl IntoIterator<Item = TransportAddr>,
) {
    let mut current = HashSet::new();
    for addr in paths {
        learn(inner, endpoint, connection, &addr);
        if let TransportAddr::Custom(addr) = addr {
            if let Some(address) = peer_address(&addr) {
                current.insert(address);
            }
        }
    }
    // Add before removing: address rotation or multiple paths at one address
    // must never expose a transient unowned carrier to its idle timer.
    for (address, learned) in inner.learned.lock().expect("poisoned").iter_mut() {
        if !current.contains(address) {
            learned.current.remove(&connection);
        }
    }
}

/// iroh connection `connection` started to close.
fn connection_closing(inner: &Inner, connection: u64) {
    for learned in inner.learned.lock().expect("poisoned").values_mut() {
        learned.open.remove(&connection);
    }
}

/// iroh connection `connection` ended: each link it used and no other live
/// connection uses is released.
fn forget_connection(inner: &Inner, connection: u64) {
    let mut released = Vec::new();
    for (address, learned) in inner.learned.lock().expect("poisoned").iter_mut() {
        learned.current.remove(&connection);
        if learned.connections.remove(&connection) {
            learned.ended.insert(connection);
            if learned.connections.is_empty() {
                released.push(address.clone());
            }
        }
    }
    let releasers = inner.releasers.lock().expect("poisoned");
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
fn connection_drained(inner: &Inner, connection: u64) {
    // `links` before `learned`, as in `run_link`.
    let links = inner.links.lock().expect("poisoned");
    inner
        .learned
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
fn only_ended(inner: &Inner, peer: &PeerAddress) -> bool {
    inner
        .learned
        .lock()
        .expect("poisoned")
        .get(peer)
        .is_some_and(|learned| learned.connections.is_empty() && !learned.ended.is_empty())
}

/// Whether a current path uses `peer`, or a historical user is still sending
/// its close. Historical open connections alone do not defeat idle cleanup.
fn in_use(inner: &Inner, peer: &PeerAddress) -> bool {
    inner
        .learned
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
        let mut addresses: HashSet<_> = addresses_of(&self.inner, &remote.id).into_iter().collect();
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
                authorize_acquisition(&self.inner, &peer);
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
        reconcile_paths(
            &self.inner,
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
                                    reconcile_paths(&inner, endpoint, connection,
                                        conn.paths().iter().map(|path| path.remote_addr().clone()));
                                }
                            }
                        }
                        None => break,
                    },
                }
            }
            connection_closing(&inner, connection);
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
                    let sent = close_sent(&inner, &weak, connection, closing).await;
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
            forget_connection(&inner, connection);
            // Still known until drained, so its late packets dial nothing.
            while weak.upgrade().is_some() {
                tokio::time::sleep(DRAINED_CHECK_INTERVAL).await;
            }
            connection_drained(&inner, connection);
        });
        AfterHandshakeOutcome::accept()
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
async fn close_sent(
    inner: &Inner,
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
    let addresses: Vec<PeerAddress> = inner
        .learned
        .lock()
        .expect("poisoned")
        .iter()
        .filter(|(_, learned)| learned.connections.contains(&connection))
        .map(|(address, _)| address.clone())
        .collect();
    let links: Vec<mpsc::Sender<Outgoing>> = {
        let links = inner.links.lock().expect("poisoned");
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
        let dropped = inner.dropped.lock().expect("poisoned");
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

fn add_releaser(inner: &Inner, peer: &PeerAddress) -> Arc<Notify> {
    let release = Arc::new(Notify::new());
    inner
        .releasers
        .lock()
        .expect("poisoned")
        .entry(peer.clone())
        .or_default()
        .push(release.clone());
    release
}

fn remove_releaser(inner: &Inner, peer: &PeerAddress, release: &Arc<Notify>) {
    let mut releasers = inner.releasers.lock().expect("poisoned");
    if let Some(list) = releasers.get_mut(peer) {
        list.retain(|other| !Arc::ptr_eq(other, release));
        if list.is_empty() {
            releasers.remove(peer);
        }
    }
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

/// Moves a failed attempt's queued packets to a separately attached carrier.
/// Queue overflow remains packet loss. QUIC can retransmit those packets.
fn forward_pending(inner: &Inner, peer: &PeerAddress, pending: &mut mpsc::Receiver<Outgoing>) {
    pending.close();
    let selected = inner
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
            inner
                .dropped
                .lock()
                .expect("poisoned")
                .insert(peer.clone(), Instant::now());
        }
    }
}

/// Removes `peer`'s link only if it is still `link`: a replacement attached
/// meanwhile stays.
fn remove_link(inner: &Inner, peer: &PeerAddress, link: &mpsc::Sender<Outgoing>) {
    let mut links = inner.links.lock().expect("poisoned");
    if links
        .get(peer)
        .is_some_and(|current| current.same_channel(link))
    {
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
            if !is_quic_initial(bytes) && only_ended(&self.inner, peer) {
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
        let close = add_closer(&self.inner, peer);
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
                    remove_closer(&inner, &peer, &close);
                    remove_link(&inner, &peer, &tx);
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
                        forward_pending(&inner, &peer, &mut rx);
                        remove_closer(&inner, &peer, &close);
                        let _ = channel.release().await;
                        return;
                    }
                    // Reachable by dialling after all: no longer inbound-only.
                    inner
                        .incoming_addresses
                        .lock()
                        .expect("poisoned")
                        .remove(&peer);
                    run_link(inner, peer, channel, rx, (tx, generation), close, None).await
                }
                Err(err) => {
                    log::debug!("dial to {} failed: {err}", peer.0);
                    // Attempt failure does not fail a legitimate QUIC user.
                    forward_pending(&inner, &peer, &mut rx);
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
mod tests {
    use super::*;
    use ble_gatt::datagram::DatagramConfig;
    use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
    use ble_gatt::{CapabilityReport, CharacteristicUuid, PeerLinkConfig, ServiceUuid};

    fn managed_peer(
        network: &Arc<MockNetwork>,
        address: &str,
        config: &DatagramConfig,
    ) -> Arc<PeerLink> {
        PeerLink::with_driver(
            Arc::new(MockDriver::new(
                PeerAddress(address.into()),
                network.clone(),
                CapabilityReport {
                    central: true,
                    peripheral: true,
                },
            )),
            PeerLinkConfig::new(config.clone()),
        )
    }

    async fn managed_pair(
        server: &Arc<PeerLink>,
        client: &Arc<PeerLink>,
        address: &PeerAddress,
    ) -> (PeerChannel, PeerChannel) {
        let mut incoming = server.incoming().unwrap();
        server.set_advertising(true).await.unwrap();
        tokio::join!(
            async {
                client
                    .request(address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { incoming.next().await.unwrap().accept().await.unwrap() },
        )
    }

    #[test]
    fn an_address_round_trips_through_its_custom_form() {
        let peer = PeerAddress("AA:BB:CC:DD:EE:FF".to_string());
        let addr = custom_addr(&peer);
        assert_eq!(addr.id(), TRANSPORT_ID);
        assert_eq!(peer_address(&addr), Some(peer));
    }

    #[tokio::test]
    async fn either_authenticated_key_can_request_a_carrier() {
        use std::sync::atomic::AtomicUsize;
        let mut keys = [
            iroh::SecretKey::generate().public(),
            iroh::SecretKey::generate().public(),
        ];
        keys.sort();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        let dialer: Dialer = Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(ble_gatt::BleError::GattBusy(
                    "busy is not unavailable".into(),
                ))
            })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let peer = PeerAddress("remote".into());
        transport.set_peer_address(keys[0], peer.clone());
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        sender.send(&peer, vec![0xc0]).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "an authenticated key does not control BLE initiation"
        );
    }

    #[tokio::test]
    async fn busy_is_retryable_but_unknown_incoming_sources_are_not_dialled() {
        for known in [false, true] {
            let (called, mut calls) = mpsc::unbounded_channel();
            let dialer: Dialer = Arc::new(move |_| {
                called.send(()).unwrap();
                Box::pin(async { Err(ble_gatt::BleError::GattBusy("transient contention".into())) })
            });
            let transport = BleGattTransport::builder().dialer(dialer).build();
            let peer = PeerAddress("remote".into());
            if known {
                transport.set_peer_address(iroh::SecretKey::generate().public(), peer.clone());
            }
            transport
                .inner
                .incoming_addresses
                .lock()
                .unwrap()
                .insert(peer.clone());
            let sender = Sender {
                inner: transport.inner.clone(),
            };
            sender.send(&peer, vec![0xc0]).unwrap();
            let result = tokio::time::timeout(Duration::from_millis(50), calls.recv()).await;
            assert_eq!(result.is_ok(), known);
            if known {
                tokio::task::yield_now().await;
                sender.send(&peer, vec![0xc0]).unwrap();
                tokio::time::timeout(Duration::from_secs(1), calls.recv())
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn stale_carrier_cleanup_cannot_remove_the_replacement() {
        let transport = BleGattTransport::builder().build();
        let peer = PeerAddress("remote".into());
        let (stale, _stale_rx) = mpsc::channel(1);
        let (current, mut current_rx) = mpsc::channel(1);
        transport
            .inner
            .links
            .lock()
            .unwrap()
            .insert(peer.clone(), current.clone());
        remove_link(&transport.inner, &peer, &stale);
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        sender.send(&peer, vec![0xc0]).unwrap();
        assert!(
            matches!(current_rx.recv().await, Some(Outgoing::Packet(packet)) if packet == vec![0xc0])
        );
        remove_link(&transport.inner, &peer, &current);
        assert!(transport.linked_peers().is_empty());
    }

    #[tokio::test]
    async fn late_packets_do_not_resurrect_a_released_incoming_carrier() {
        for (initial, routed, active) in [
            (false, false, false),
            (false, true, false),
            (true, false, false),
            (true, true, false),
            (false, false, true),
        ] {
            let network = MockNetwork::new();
            let server_address = PeerAddress("server".into());
            let client_address = PeerAddress("client".into());
            let config = DatagramConfig::new(
                ServiceUuid(uuid::Uuid::new_v4()),
                CharacteristicUuid(uuid::Uuid::new_v4()),
            );
            let server = managed_peer(&network, &server_address.0, &config);
            let client = managed_peer(&network, &client_address.0, &config);
            let (channel, carrier) = managed_pair(&server, &client, &server_address).await;
            let transport = BleGattTransport::builder().build();
            let id = iroh::SecretKey::generate().public();
            let addr = TransportAddr::Custom(custom_addr(&client_address));
            learn(&transport.inner, id, 0, &addr);
            forget_connection(&transport.inner, 0);
            if active {
                learn(&transport.inner, id, 1, &addr);
            }

            let packet = vec![if initial { 0xc0 } else { 0x40 }, 1, 2, 3];
            let mut task = if routed {
                channel.send(packet).await.unwrap();
                let first = carrier.recv().await.unwrap().unwrap();
                transport.attach_after(carrier, first)
            } else {
                let task = transport.attach(carrier);
                channel.send(packet).await.unwrap();
                task
            };
            if !initial && !active {
                tokio::time::timeout(Duration::from_millis(300), &mut task)
                    .await
                    .expect("late non-Initial traffic cannot keep a released address alive")
                    .unwrap();
            } else {
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), &mut task)
                        .await
                        .is_err(),
                    "a new Initial or another active user protects its carrier"
                );
                transport.close_link(&client_address);
                tokio::time::timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap();
            }
            assert!(transport.linked_peers().is_empty());
        }
    }

    #[tokio::test]
    async fn pending_dial_completion_does_not_remove_an_incoming_replacement() {
        for success in [false, true] {
            let network = MockNetwork::new();
            let local = PeerAddress("local".into());
            let remote = PeerAddress("remote".into());
            let config = DatagramConfig::new(
                ServiceUuid(uuid::Uuid::new_v4()),
                CharacteristicUuid(uuid::Uuid::new_v4()),
            );
            let a = managed_peer(&network, &local.0, &config);
            let b = managed_peer(&network, &remote.0, &config);
            let (complete, completion) = oneshot::channel::<ble_gatt::Result<PeerChannel>>();
            let completion = Arc::new(Mutex::new(Some(completion)));
            let (started, mut starts) = mpsc::unbounded_channel();
            let dialer: Dialer = Arc::new(move |_| {
                let completion = completion.lock().unwrap().take().unwrap();
                let started = started.clone();
                Box::pin(async move {
                    started.send(()).unwrap();
                    completion.await.unwrap()
                })
            });
            let transport = BleGattTransport::builder().dialer(dialer).build();
            let sender = Sender {
                inner: transport.inner.clone(),
            };
            sender.send(&remote, vec![0xc0]).unwrap();
            starts.recv().await.unwrap();
            let (peer_carrier, accepted) = managed_pair(&a, &b, &local).await;
            let duplicate = accepted.clone();
            let replacement = transport.attach(accepted);
            if success {
                // A late acquired use is not another native carrier.
                assert!(complete.send(Ok(duplicate)).is_ok());
            } else {
                drop(duplicate);
                assert!(complete
                    .send(Err(ble_gatt::BleError::GattBusy(
                        "outgoing attempt lost".into()
                    )))
                    .is_ok());
            }
            // Queued packets must use the independently accepted carrier.
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), peer_carrier.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![0xc0]
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                while transport
                    .inner
                    .closers
                    .lock()
                    .unwrap()
                    .get(&remote)
                    .unwrap()
                    .len()
                    != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            sender.send(&remote, b"replacement".to_vec()).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), peer_carrier.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                b"replacement"
            );
            assert!(!replacement.is_finished());
            transport.close_link(&remote);
            tokio::time::timeout(Duration::from_secs(2), replacement)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn cancelling_a_pending_dial_drops_its_future_and_queue() {
        struct OnDrop(Option<oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let (dropped, drop_seen) = oneshot::channel();
        let dropped = Arc::new(Mutex::new(Some(dropped)));
        let (started, mut starts) = mpsc::unbounded_channel();
        let dialer: Dialer = Arc::new(move |_| {
            let probe = OnDrop(dropped.lock().unwrap().take());
            let started = started.clone();
            Box::pin(async move {
                let _probe = probe;
                started.send(()).unwrap();
                std::future::pending().await
            })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let remote = PeerAddress("remote".into());
        Sender {
            inner: transport.inner.clone(),
        }
        .send(&remote, vec![0xc0])
        .unwrap();
        starts.recv().await.unwrap();
        transport.close_link(&remote);
        tokio::time::timeout(Duration::from_secs(1), drop_seen)
            .await
            .unwrap()
            .unwrap();
        tokio::task::yield_now().await;
        assert!(transport.linked_peers().is_empty());
        assert!(transport.inner.closers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn closing_a_published_queue_before_the_dial_task_starts_cancels_it() {
        use std::sync::atomic::AtomicUsize;
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let dialer: Dialer = Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::pending())
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let peer = PeerAddress("publication-race".into());
        let closing = transport.clone();
        let address = peer.clone();
        *transport.inner.published.lock().unwrap() = Some(Box::new(move || {
            assert_eq!(closing.linked_peers(), vec![address.clone()]);
            closing.close_link(&address);
        }));
        Sender {
            inner: transport.inner.clone(),
        }
        .send(&peer, vec![1])
        .unwrap();
        tokio::task::yield_now().await;
        assert!(
            transport.linked_peers().is_empty(),
            "close must see the published queue's cancellation signal"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "cancelled work must not start the dialer"
        );
        assert!(transport.inner.closers.lock().unwrap().is_empty());
    }

    async fn received_packet(endpoint: &mut dyn CustomEndpoint, peer: &PeerAddress) -> Vec<u8> {
        let mut bytes = [0; 256];
        let mut meta = [noq_udp::RecvMeta::default()];
        let mut info = [RecvInfo::new(custom_addr(peer), None)];
        std::future::poll_fn(|cx| {
            endpoint.poll_recv(
                cx,
                &mut [io::IoSliceMut::new(&mut bytes)],
                &mut meta,
                &mut info,
            )
        })
        .await
        .unwrap();
        bytes[..meta[0].len].to_vec()
    }

    #[tokio::test]
    async fn retired_generation_cleanup_cannot_suppress_or_remove_a_live_replacement() {
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let network = MockNetwork::new();
        let server = managed_peer(&network, "generation-server", &config);
        let client = managed_peer(&network, "generation-client", &config);
        let server_address = PeerAddress("generation-server".into());
        let client_address = PeerAddress("generation-client".into());
        let mut incoming = server.incoming().unwrap();
        server.set_advertising(true).await.unwrap();
        let (old_remote, old) = tokio::join!(
            async {
                client
                    .request(&server_address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { incoming.next().await.unwrap().accept().await.unwrap() }
        );
        let transport = BleGattTransport::builder().build();
        let mut endpoint = transport.bind().unwrap();
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        let (reached, retiring) = oneshot::channel();
        let resume = Arc::new(Notify::new());
        *transport.inner.retiring.lock().unwrap() = Some((reached, resume.clone()));
        let old_task = transport.attach(old.clone());
        old_remote.send(vec![0, 0]).await.unwrap();
        assert_eq!(
            received_packet(endpoint.as_mut(), &client_address).await,
            vec![0, 0]
        );
        old.disconnect().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), retiring)
            .await
            .unwrap()
            .unwrap();
        // A has closed its receive queue but cannot finish scoped release yet.
        // Drain the client's generation without resuming the old server adapter task.
        old_remote.release().await.unwrap();
        let (remote, new) = tokio::join!(
            async {
                client
                    .request(&server_address)
                    .await
                    .unwrap()
                    .accept()
                    .await
                    .unwrap()
            },
            async { incoming.next().await.unwrap().accept().await.unwrap() }
        );
        let first_new_task = transport.attach(new.clone());
        let second_new_task = transport.attach(new.clone());
        let new_task = transport.attach(new);
        for number in 1..=3u8 {
            sender.send(&client_address, vec![1, number]).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), remote.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![1, number]
            );
            remote.send(vec![0, number]).await.unwrap();
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    received_packet(endpoint.as_mut(), &client_address)
                )
                .await
                .unwrap(),
                vec![0, number]
            );
        }
        resume.notify_one();
        old_task.await.unwrap();
        first_new_task.await.unwrap();
        second_new_task.await.unwrap();
        for number in 4..=6u8 {
            sender.send(&client_address, vec![1, number]).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), remote.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                vec![1, number]
            );
            remote.send(vec![0, number]).await.unwrap();
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    received_packet(endpoint.as_mut(), &client_address)
                )
                .await
                .unwrap(),
                vec![0, number]
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), remote.recv())
                .await
                .is_err()
        );
        assert!(tokio::time::timeout(
            Duration::from_millis(50),
            received_packet(endpoint.as_mut(), &client_address)
        )
        .await
        .is_err());
        assert!(!new_task.is_finished());
        transport.close_link(&client_address);
        tokio::time::timeout(Duration::from_secs(2), new_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn explicit_destination_hints_preserve_identity_validation_and_dial_guards() {
        let (called, mut calls) = mpsc::unbounded_channel();
        let dialer: Dialer = Arc::new(move |peer| {
            called.send(peer).unwrap();
            Box::pin(async { Err(ble_gatt::BleError::GattBusy("probe dispatched".into())) })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let id = iroh::SecretKey::generate().public();
        let authenticated_id = iroh::SecretKey::generate().public();
        let current = PeerAddress("hint-current".into());
        let retired = PeerAddress("hint-retired".into());
        let private = PeerAddress("hint-private-source".into());
        let supplied = PeerAddress("hint-unlearned".into());
        transport.set_peer_address(id, retired.clone());
        transport.set_peer_address(id, current.clone());
        learn(
            &transport.inner,
            authenticated_id,
            0,
            &TransportAddr::Custom(custom_addr(&supplied)),
        );
        transport
            .inner
            .incoming_addresses
            .lock()
            .unwrap()
            .insert(private.clone());
        for peer in [&current, &retired, &private, &supplied] {
            transport
                .inner
                .acquisition
                .lock()
                .unwrap()
                .entry(peer.clone())
                .or_default()
                .terminal = true;
        }
        let remote = iroh::EndpointAddr::from_parts(
            id,
            [
                TransportAddr::Custom(custom_addr(&current)),
                TransportAddr::Custom(custom_addr(&retired)),
                TransportAddr::Custom(custom_addr(&private)),
                TransportAddr::Custom(custom_addr(&supplied)),
                TransportAddr::Custom(CustomAddr::from_parts(TRANSPORT_ID, &[0xff])),
                TransportAddr::Custom(CustomAddr::from_parts(0x20, b"other-transport")),
                TransportAddr::Ip("127.0.0.1:12345".parse().unwrap()),
            ],
        );
        transport
            .hooks()
            .before_connect(&remote, b"fresh-user")
            .await;
        {
            let acquisition = transport.inner.acquisition.lock().unwrap();
            assert_eq!(
                acquisition.len(),
                4,
                "invalid and non-BLE destinations are ignored"
            );
            for peer in [&current, &retired, &private, &supplied] {
                assert!(!acquisition[peer].terminal);
                assert_eq!(
                    acquisition[peer].generation, 1,
                    "mapped and supplied duplicates authorize once"
                );
            }
        }
        {
            let learned = transport.inner.learned.lock().unwrap();
            assert_eq!(
                learned.len(),
                1,
                "caller hints create no authenticated identities"
            );
            assert_eq!(
                learned[&supplied].endpoint, authenticated_id,
                "hints cannot overwrite authenticated identity"
            );
        }
        assert_eq!(transport.inner.peers.lock().unwrap().len(), 1);
        assert_eq!(transport.inner.peers.lock().unwrap()[&id], current);
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        for peer in [&retired, &private] {
            sender.send(peer, vec![0xc0, 1]).unwrap();
        }
        assert!(
            transport.linked_peers().is_empty(),
            "authorization does not remove dial guards"
        );
        assert!(matches!(
            calls.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        sender.send(&supplied, vec![0x40, 2]).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(300), calls.recv())
                .await
                .unwrap()
                .unwrap(),
            supplied
        );
    }

    #[tokio::test]
    async fn explicit_destination_recovers_after_authenticated_history_drains() {
        use iroh::endpoint::presets;

        const ALPN: &[u8] = b"explicit-history";
        let network = MockNetwork::new();
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let address = PeerAddress("history-server".into());
        let server_link = managed_peer(&network, &address.0, &config);
        let client_link = managed_peer(&network, "history-client", &config);
        let server_transport = BleGattTransport::builder().build();
        let mut incoming = server_link.incoming().unwrap();
        server_link.set_advertising(true).await.unwrap();
        let (attached, mut channels) = mpsc::unbounded_channel();
        let attaching = tokio::spawn({
            let transport = server_transport.clone();
            async move {
                while let Some(request) = incoming.next().await {
                    let channel = request.accept().await.unwrap();
                    transport.attach(channel.clone());
                    attached.send(channel).unwrap();
                }
            }
        });
        let server = iroh::Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .alpns(vec![ALPN.to_vec()])
            .add_custom_transport(Arc::new(server_transport.clone()))
            .hooks(server_transport.hooks())
            .bind()
            .await
            .unwrap();
        let transport = BleGattTransport::builder()
            .dialer(dial_with(client_link.clone()))
            .build();
        let client = iroh::Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .add_custom_transport(Arc::new(transport.clone()))
            .hooks(transport.hooks())
            .bind()
            .await
            .unwrap();
        let remote = iroh::EndpointAddr::from_parts(
            server.id(),
            [TransportAddr::Custom(custom_addr(&address))],
        );
        let connect = || async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let (outgoing, accepted) =
                    tokio::join!(client.connect(remote.clone(), ALPN), async {
                        server.accept().await.unwrap().await.unwrap()
                    });
                (outgoing.unwrap(), accepted)
            })
            .await
            .unwrap()
        };
        let (first, accepted) = connect().await;
        let channel = channels.recv().await.unwrap();
        assert_eq!(first.remote_id(), server.id());
        assert_eq!(
            transport
                .inner
                .learned
                .lock()
                .unwrap()
                .get(&address)
                .unwrap()
                .endpoint,
            server.id(),
            "real TLS handshake established the learned identity"
        );
        let generation = transport.inner.acquisition.lock().unwrap()[&address].generation;
        let (second, other_accepted) = connect().await;
        assert_eq!(
            transport.inner.acquisition.lock().unwrap()[&address].generation,
            generation,
            "a fresh explicit user shares a healthy carrier's authorization"
        );
        assert!(matches!(
            channels.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        second
            .send_datagram(b"shared carrier".to_vec().into())
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), other_accepted.read_datagram())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"shared carrier"
        );
        channel.disconnect().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !transport.linked_peers().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for connection in [&first, &second, &accepted, &other_accepted] {
            connection.close(0u32.into(), b"done");
        }
        drop((first, second, accepted, other_accepted));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !transport.inner.learned.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("authenticated connection history must actually drain");
        assert!(addresses_of(&transport.inner, &server.id()).is_empty());
        assert!(transport.inner.acquisition.lock().unwrap()[&address].terminal);
        let (replacement, replacement_accepted) = connect().await;
        replacement
            .send_datagram(b"after drain".to_vec().into())
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), replacement_accepted.read_datagram())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"after drain"
        );
        replacement.close(0u32.into(), b"done");
        drop((replacement, replacement_accepted));
        client.close().await;
        server.close().await;
        attaching.abort();
        let _ = attaching.await;
        client_link.close().await.unwrap();
        server_link.close().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_authorization_survives_a_terminal_carriers_delayed_publication() {
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let network = MockNetwork::new();
        let server = managed_peer(&network, "authorization-server", &config);
        let client = managed_peer(&network, "authorization-client", &config);
        let (remote, channel) = managed_pair(
            &server,
            &client,
            &PeerAddress("authorization-server".into()),
        )
        .await;
        let peer = channel.peer();
        let (called, mut calls) = mpsc::unbounded_channel();
        let dialer: Dialer = Arc::new(move |peer| {
            called.send(peer).unwrap();
            Box::pin(async { Err(ble_gatt::BleError::GattBusy("probe dispatched".into())) })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let id = iroh::SecretKey::generate().public();
        transport.set_peer_address(id, peer.clone());
        let mut endpoint = transport.bind().unwrap();
        let (reached, terminating) = oneshot::channel();
        let resume = Arc::new(Notify::new());
        *transport.inner.terminating.lock().unwrap() = Some((reached, resume.clone()));
        let task = transport.attach(channel.clone());
        remote.send(vec![0, 1]).await.unwrap();
        assert_eq!(received_packet(endpoint.as_mut(), &peer).await, vec![0, 1]);
        // Core has published the terminal reason, but the old adapter task
        // has not yet published suppression or closed its packet queue.
        remote.disconnect().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), terminating)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            channel.close_reason(),
            Some(PeerChannelCloseReason::PeerDisconnect)
        );
        assert!(matches!(
            transport
                .hooks()
                .before_connect(&iroh::EndpointAddr::from(id), b"new-user")
                .await,
            BeforeConnectOutcome::Accept
        ));
        resume.notify_one();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        Sender {
            inner: transport.inner.clone(),
        }
        .send(&peer, vec![0x40, 2])
        .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(300), calls.recv())
                .await
                .expect(
                    "fresh explicit authorization must survive the old task's terminal publication"
                )
                .unwrap(),
            peer
        );
    }

    // Hookless negative contract coverage: peer scoped release becomes a wire
    // Disconnect, indistinguishable here from explicit peer disconnect. The
    // shared integration endpoint helper installs hooks; this test deliberately
    // withholds the fresh hook until after proving packet-only suppression.
    #[tokio::test]
    async fn peer_idle_release_needs_fresh_authorization_for_reverse_traffic() {
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let network = MockNetwork::new();
        let server = managed_peer(&network, "idle-auth-server", &config);
        let client = managed_peer(&network, "idle-auth-client", &config);
        let (remote, channel) =
            managed_pair(&server, &client, &PeerAddress("idle-auth-server".into())).await;
        let peer = channel.peer();
        let (called, mut calls) = mpsc::unbounded_channel();
        let dialer: Dialer = Arc::new(move |peer| {
            called.send(peer).unwrap();
            Box::pin(async { Err(ble_gatt::BleError::GattBusy("probe dispatched".into())) })
        });
        let transport = BleGattTransport::builder().dialer(dialer).build();
        let id = iroh::SecretKey::generate().public();
        transport.set_peer_address(id, peer.clone());
        let mut endpoint = transport.bind().unwrap();
        let task = transport.attach(channel.clone());
        remote.send(vec![0, 1]).await.unwrap();
        assert_eq!(received_packet(endpoint.as_mut(), &peer).await, vec![0, 1]);
        // Exactly the action performed by the peer's idle packet-task exit.
        remote.release().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            channel.close_reason(),
            Some(PeerChannelCloseReason::PeerDisconnect)
        );
        let sender = Sender {
            inner: transport.inner.clone(),
        };
        for bytes in [vec![0xc0, 1], vec![0x40, 2]] {
            sender.send(&peer, bytes).unwrap();
        }
        assert!(
            transport
                .inner
                .acquisition
                .lock()
                .unwrap()
                .get(&peer)
                .unwrap()
                .terminal
        );
        assert!(transport.linked_peers().is_empty());
        assert!(matches!(
            calls.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        // Only an authoritative new application action clears this state;
        // packets alone above deliberately had no fresh hook invocation.
        transport
            .hooks()
            .before_connect(&iroh::EndpointAddr::from(id), b"reverse")
            .await;
        sender.send(&peer, vec![0x40, 3]).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(300), calls.recv())
                .await
                .unwrap()
                .unwrap(),
            peer
        );
    }

    #[tokio::test]
    async fn queued_old_generation_barrier_cannot_report_delivery_after_replacement() {
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let network = MockNetwork::new();
        let server = managed_peer(&network, "barrier-server", &config);
        let client = managed_peer(&network, "barrier-client", &config);
        let (_remote, channel) =
            managed_pair(&server, &client, &PeerAddress("barrier-server".into())).await;
        let transport = BleGattTransport::builder().build();
        let peer = channel.peer();
        let first = transport.attach(channel.clone());
        let (reached, barrier) = oneshot::channel();
        let old_queue = transport
            .inner
            .links
            .lock()
            .unwrap()
            .get(&peer)
            .unwrap()
            .clone();
        old_queue.try_send(Outgoing::Packet(vec![0, 1])).unwrap();
        old_queue.try_send(Outgoing::Barrier(reached)).unwrap();
        let second = transport.attach(channel.clone());
        let third = transport.attach(channel);
        transport.close_link(&peer);
        for task in [first, second, third] {
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            barrier.await.is_err(),
            "discarded old-generation data is not close-delivery proof"
        );
        assert!(transport.inner.dropped.lock().unwrap().contains_key(&peer));
        assert!(transport.linked_peers().is_empty());
    }

    #[tokio::test]
    async fn failed_queued_close_barrier_cannot_report_close_sent() {
        use iroh::endpoint::presets;
        const ALPN: &[u8] = b"failed-close-barrier";
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let client = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
        let address = iroh::EndpointAddr::from_parts(
            server.id(),
            server
                .bound_sockets()
                .iter()
                .copied()
                .map(TransportAddr::Ip),
        );
        let (connection, accepted) = tokio::join!(client.connect(address, ALPN), async {
            server.accept().await.unwrap().await.unwrap()
        });
        let connection = connection.unwrap();
        let transport = BleGattTransport::builder().build();
        let peer = PeerAddress("failed-barrier".into());
        learn(
            &transport.inner,
            server.id(),
            0,
            &TransportAddr::Custom(custom_addr(&peer)),
        );
        let (queue, mut pending) = mpsc::channel(4);
        queue.try_send(Outgoing::Packet(vec![0, 7])).unwrap();
        transport.inner.links.lock().unwrap().insert(peer, queue);
        let weak = connection.weak_handle();
        connection.close(0u32.into(), b"done");
        let inner = transport.inner.clone();
        let mut proof =
            tokio::spawn(async move { close_sent(&inner, &weak, 0, Instant::now()).await });
        assert!(
            matches!(pending.recv().await, Some(Outgoing::Packet(bytes)) if bytes == vec![0, 7])
        );
        let Outgoing::Barrier(reached) =
            tokio::time::timeout(Duration::from_secs(2), pending.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected close barrier")
        };
        drop(reached);
        drop(pending);
        // Failure can finish false if QUIC already drained, but never true.
        if let Ok(result) = tokio::time::timeout(Duration::from_millis(50), &mut proof).await {
            assert!(!result.unwrap());
        } else {
            drop(connection);
            drop(accepted);
            client.close().await;
            server.close().await;
            assert!(!tokio::time::timeout(Duration::from_secs(5), proof)
                .await
                .unwrap()
                .unwrap());
        }
    }

    // iroh exposes no public close-path operation. Replay its documented
    // post-Closed snapshot at the same reconciliation seam used by hooks,
    // while real QUIC stays alive on IP and a real mock GATT task idles.
    // This is not a claim of physically induced multipath closure.
    async fn historical_path_runtime(survivor: bool) {
        use iroh::endpoint::presets;

        const ALPN: &[u8] = b"path-ownership-regression";
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let client = iroh::Endpoint::bind(presets::Minimal).await.unwrap();
        let address = iroh::EndpointAddr::from_parts(
            server.id(),
            server
                .bound_sockets()
                .iter()
                .copied()
                .map(TransportAddr::Ip),
        );
        let (connection, accepted) = tokio::join!(client.connect(address.clone(), ALPN), async {
            server.accept().await.unwrap().await.unwrap()
        },);
        let connection = connection.unwrap();
        let surviving_connection = if survivor {
            let (other, incoming) = tokio::join!(client.connect(address, ALPN), async {
                server.accept().await.unwrap().await.unwrap()
            },);
            let other = other.unwrap();
            assert_ne!(connection.stable_id(), other.stable_id());
            Some((other, incoming))
        } else {
            None
        };
        assert!(connection
            .paths()
            .iter()
            .any(|path| matches!(path.remote_addr(), TransportAddr::Ip(_))));

        let network = MockNetwork::new();
        let local = PeerAddress("idle-server".into());
        let remote = PeerAddress("historical-client".into());
        let config = DatagramConfig::new(
            ServiceUuid(uuid::Uuid::new_v4()),
            CharacteristicUuid(uuid::Uuid::new_v4()),
        );
        let gatt_server = managed_peer(&network, &local.0, &config);
        let gatt_client = managed_peer(&network, &remote.0, &config);
        let (_carrier, accepted_carrier) = managed_pair(&gatt_server, &gatt_client, &local).await;
        let transport = BleGattTransport::builder()
            .link_idle_timeout(Duration::from_millis(80))
            .build();
        let ble = TransportAddr::Custom(custom_addr(&remote));
        let current: Vec<_> = connection
            .paths()
            .iter()
            .map(|path| path.remote_addr().clone())
            .collect();
        // Prior snapshot had both transports. Closing the BLE path does NOT
        // end this connection, close the carrier, or erase its history.
        reconcile_paths(
            &transport.inner,
            server.id(),
            0,
            current.iter().cloned().chain([ble.clone()]),
        );
        if survivor {
            // A different connection still has a path at the same address.
            reconcile_paths(&transport.inner, server.id(), 1, [ble]);
        }
        let mut task = transport.attach(accepted_carrier);
        reconcile_paths(&transport.inner, server.id(), 0, current);
        if survivor {
            assert!(
                tokio::time::timeout(Duration::from_millis(350), &mut task)
                    .await
                    .is_err(),
                "another connection's current BLE path must protect the carrier"
            );
            let (other, incoming) = surviving_connection.as_ref().unwrap();
            other.send_datagram(b"survivor".to_vec().into()).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), incoming.read_datagram())
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"survivor"
            );
            // The survivor also moves away, without closing the carrier.
            reconcile_paths(&transport.inner, server.id(), 1, []);
        }
        tokio::time::timeout(Duration::from_millis(700), &mut task)
            .await
            .expect("historical BLE ownership must not prevent idle expiry")
            .unwrap();
        assert!(transport.linked_peers().is_empty());
        assert!(
            transport
                .inner
                .learned
                .lock()
                .unwrap()
                .get(&remote)
                .unwrap()
                .connections
                .contains(&0),
            "history must remain for close barriers and ended-packet suppression"
        );
        assert!(connection.close_reason().is_none());
        connection
            .send_datagram(b"still alive on IP".to_vec().into())
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), accepted.read_datagram())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"still alive on IP"
        );
        connection.close(0u32.into(), b"done");
    }

    #[tokio::test]
    async fn historical_ble_path_expires_while_quic_survives_on_ip() {
        historical_path_runtime(false).await;
    }

    #[tokio::test]
    async fn historical_ble_path_keeps_a_different_connections_surviving_path() {
        historical_path_runtime(true).await;
    }

    #[test]
    fn another_transports_address_is_not_a_peer() {
        let addr = CustomAddr::from_parts(0x20, b"AA:BB:CC:DD:EE:FF");
        assert_eq!(peer_address(&addr), None);
    }
}

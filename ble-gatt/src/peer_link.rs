//! Tier 3: `PeerLink` — keep app-to-app links to a set of peers alive.
//!
//! **Read `docs/interface.md` for when to use this instead of the raw
//! `Backend` or the `datagram` tier, and `docs/adr/0005` for the state
//! machines underneath.**
//!
//! The consumer declares which peers it cares about (`track`) and one policy
//! (`role` — who dials). This module owns dialing, the redial ladder and
//! giving up, one link per peer, and tearing everything down on radio-off /
//! re-establishing on radio-on. The consumer consumes a channel and a
//! per-peer status; it never sees the state machines.
//!
//! Everything BLE happens on one dedicated OS thread with its own Tokio
//! runtime (spawned by `new`), so `new` is synchronous, infallible, and
//! works in a binary with no async runtime of its own. The [`PeerChannel`]
//! handed out on [`PeerLinkEvent::Up`] is a message-passing handle to that
//! thread.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, oneshot, Mutex as AsyncMutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::backend::link_state::{
    CentralEvent, CentralLink, PeripheralEvent, PeripheralLink, RadioEvent, RadioState,
};
use crate::backend::{Backend, BoxStream};
use crate::datagram::{self, DatagramChannel, DatagramConfig};
use crate::error::{BleError, Result};
use crate::models::{GattEvent, PeerAddress, RadioStatus, Role};

const EVENT_CHANNEL_CAPACITY: usize = 128;
const TICK_INTERVAL: Duration = Duration::from_secs(1);
/// Backoff before restarting the inbound server after a transient
/// serve/advertise failure.
const SERVE_RETRY_DELAY: Duration = Duration::from_secs(2);
const CHANNEL_SEND_QUEUE: usize = 16;
const CHANNEL_RECV_QUEUE: usize = 64;
const DISCOVER_WINDOW: Duration = Duration::from_secs(3);

/// A stable identity the two peers of a pair compare the same way, to decide
/// which side dials (glare — see `docs/adr/0003`'s revision). This is the
/// *application's* to establish (node ids exchanged during its own
/// handshake); `PeerAddress` is opaque and unordered and cannot be the
/// basis. Only consulted in [`LinkRole::Symmetric`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinkId(pub String);

impl From<String> for LinkId {
    fn from(s: String) -> Self {
        LinkId(s)
    }
}
impl From<&str> for LinkId {
    fn from(s: &str) -> Self {
        LinkId(s.to_owned())
    }
}

/// Who dials, for a pair that can both see each other.
#[derive(Debug, Clone)]
pub enum LinkRole {
    /// Dials tracked peers *and* accepts inbound. For each peer, whichever
    /// side has the lower `LinkId` dials.
    Symmetric { local_id: LinkId },
    /// Only ever dials tracked peers.
    DialOnly,
    /// Only ever advertises and accepts; never dials.
    AcceptOnly,
}

/// Redial ladder for the dialer, and inbound-link deadline for the acceptor.
/// Both sides reach [`PeerStatus::GaveUp`] when the budget is spent.
#[derive(Debug, Clone)]
pub struct RetryBudget {
    /// Backoff before each successive redial. The last entry repeats.
    /// Empty = do not redial (an inbound link is still accepted).
    pub backoff: Vec<Duration>,
    /// Total time from the first failed attempt before the dialer gives up.
    pub give_up_after: Duration,
    /// How long the acceptor waits for an inbound link before giving up.
    pub acceptor_deadline: Duration,
}

impl Default for RetryBudget {
    fn default() -> Self {
        RetryBudget {
            backoff: vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(30),
            ],
            give_up_after: Duration::from_secs(60),
            acceptor_deadline: Duration::from_secs(60),
        }
    }
}

/// How many peers hold a live link at once. A BLE central has a hard
/// concurrent-link limit; tracking more peers than this queues the extra
/// ones as [`PeerStatus::Queued`].
#[derive(Debug, Clone, Copy)]
pub struct MaxLinks(pub usize);

impl Default for MaxLinks {
    fn default() -> Self {
        MaxLinks(4)
    }
}

/// Configure a [`PeerLink`]. See `docs/interface.md`.
pub struct PeerLinkConfig {
    /// The wire contract and datagram bounds. `datagram.service` /
    /// `datagram.characteristic` are the service and characteristic to use.
    pub datagram: DatagramConfig,
    pub role: LinkRole,
    pub retry_budget: RetryBudget,
    pub max_links: MaxLinks,
}

/// One tracked peer's coarse status — a projection of the internal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    Untracked,
    /// Radio off / unsupported.
    Unavailable,
    /// Tracked, but `max_links` is full — waiting for a slot.
    Queued,
    /// Trying — dialing, or waiting for an inbound link.
    Connecting,
    Connected,
    /// A previous attempt failed; backing off before the next.
    Waiting,
    /// The retry budget is spent. Left by `retry()` or an inbound link.
    GaveUp,
}

/// What the consumer reacts to. Delivered on [`PeerLink::events`], a
/// broadcast — each call is an independent subscription.
#[derive(Debug, Clone)]
pub enum PeerLinkEvent {
    /// A link to `peer` is up; the channel is valid until `Down` for this
    /// peer. A redial after a drop delivers a fresh `Up`.
    Up {
        peer: PeerAddress,
        max_message_len: usize,
        channel: Arc<PeerChannel>,
    },
    /// The link is gone; the channel from `Up` is dead.
    Down { peer: PeerAddress },
    /// Coarse status changed. No channel.
    Status { peer: PeerAddress, status: PeerStatus },
    /// The radio's usability changed.
    RadioChanged { status: RadioStatus },
}

/// Message-passing handle to one peer's live link. `send` / `recv` cross to
/// the driver thread, which does the GATT work. Contract (see
/// `docs/interface.md`): `send` returns the real `BleError`
/// (`GattBusy` stays distinguishable); `recv` → `None` on close, including
/// the driver being gone; a full send queue → `BleError::GattBusy`.
pub struct PeerChannel {
    peer: PeerAddress,
    max_message_len: usize,
    send_tx: mpsc::Sender<(Vec<u8>, oneshot::Sender<Result<()>>)>,
    recv_rx: AsyncMutex<mpsc::Receiver<Result<Vec<u8>>>>,
}

impl std::fmt::Debug for PeerChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerChannel")
            .field("peer", &self.peer)
            .field("max_message_len", &self.max_message_len)
            .finish_non_exhaustive()
    }
}

impl PeerChannel {
    pub fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }

    pub fn max_message_len(&self) -> usize {
        self.max_message_len
    }

    pub async fn send(&self, payload: Vec<u8>) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_tx
            .try_send((payload, reply_tx))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => {
                    BleError::GattBusy(format!("send queue full for {}", self.peer.0))
                }
                mpsc::error::TrySendError::Closed(_) => BleError::NotConnected(self.peer.0.clone()),
            })?;
        reply_rx
            .await
            .map_err(|_| BleError::NotConnected(self.peer.0.clone()))?
    }

    pub async fn recv(&self) -> Option<Result<Vec<u8>>> {
        self.recv_rx.lock().await.recv().await
    }
}

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

enum Command {
    Track { peer: PeerAddress, peer_id: LinkId },
    Untrack(PeerAddress),
    Retry(PeerAddress),
    Discover(oneshot::Sender<Result<Vec<PeerAddress>>>),
}

/// See the module docs and `docs/interface.md`.
pub struct PeerLink {
    cmd_tx: mpsc::UnboundedSender<Command>,
    events_tx: broadcast::Sender<PeerLinkEvent>,
    status: Arc<StdMutex<HashMap<PeerAddress, PeerStatus>>>,
    radio: Arc<StdMutex<RadioStatus>>,
    _driver: DriverHandle,
}

struct DriverHandle {
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for DriverHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl PeerLink {
    /// Build a `PeerLink` that constructs and owns the platform backend.
    /// Synchronous and infallible: the backend is acquired on the driver
    /// thread and surfaces as `radio()` reporting `Off` / `Unsupported`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new(config: PeerLinkConfig) -> Arc<Self> {
        Self::spawn(config, None)
    }

    /// Build a `PeerLink` over an already-constructed backend — for tests and
    /// the mock.
    pub fn with_backend(backend: Arc<dyn Backend>, config: PeerLinkConfig) -> Arc<Self> {
        Self::spawn(config, Some(backend))
    }

    fn spawn(config: PeerLinkConfig, backend: Option<Arc<dyn Backend>>) -> Arc<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let status = Arc::new(StdMutex::new(HashMap::new()));
        let radio = Arc::new(StdMutex::new(RadioStatus::Off));

        let driver = Driver {
            config,
            provided_backend: backend,
            cmd_rx,
            events_tx: events_tx.clone(),
            status: status.clone(),
            radio: radio.clone(),
        };
        let join = std::thread::Builder::new()
            .name("ble-gatt-peerlink".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("PeerLink driver runtime");
                rt.block_on(driver.run(shutdown_rx));
            })
            .expect("spawn PeerLink driver thread");

        Arc::new(PeerLink {
            cmd_tx,
            events_tx,
            status,
            radio,
            _driver: DriverHandle { shutdown: Some(shutdown_tx), join: Some(join) },
        })
    }

    /// Keep a link to this peer alive. `peer_id` is only consulted for
    /// [`LinkRole::Symmetric`].
    pub fn track(&self, peer: PeerAddress, peer_id: LinkId) {
        let _ = self.cmd_tx.send(Command::Track { peer, peer_id });
    }

    /// Stop: drop the channel, disconnect, forget the peer.
    pub fn untrack(&self, peer: PeerAddress) {
        let _ = self.cmd_tx.send(Command::Untrack(peer));
    }

    /// A tracked peer that gave up: try again now.
    pub fn retry(&self, peer: PeerAddress) {
        let _ = self.cmd_tx.send(Command::Retry(peer));
    }

    /// Peers advertising the configured service — a bounded snapshot.
    /// Untrusted metadata; the consumer decides which to `track`.
    pub async fn discover(&self) -> Result<Vec<PeerAddress>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Discover(tx))
            .map_err(|_| BleError::AdapterUnavailable("PeerLink driver is gone".into()))?;
        rx.await
            .map_err(|_| BleError::AdapterUnavailable("PeerLink driver is gone".into()))?
    }

    /// Subscribe to the event stream. Each call is an independent
    /// subscription.
    pub fn events(&self) -> BoxStream<PeerLinkEvent> {
        Box::pin(BroadcastStream::new(self.events_tx.subscribe()).filter_map(|item| item.ok()))
    }

    /// One peer's status without waiting for an event.
    pub fn status(&self, peer: &PeerAddress) -> PeerStatus {
        self.status
            .lock()
            .unwrap()
            .get(peer)
            .copied()
            .unwrap_or(PeerStatus::Untracked)
    }

    /// Radio usability without waiting for an event.
    pub fn radio(&self) -> RadioStatus {
        *self.radio.lock().unwrap()
    }
}

// ---------------------------------------------------------------------------
// The driver — runs on the dedicated thread's runtime.
// ---------------------------------------------------------------------------

struct Driver {
    config: PeerLinkConfig,
    provided_backend: Option<Arc<dyn Backend>>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    events_tx: broadcast::Sender<PeerLinkEvent>,
    status: Arc<StdMutex<HashMap<PeerAddress, PeerStatus>>>,
    radio: Arc<StdMutex<RadioStatus>>,
}

/// Per-tracked-peer state the driver owns.
struct Peer {
    central: CentralLink,
    peripheral: PeripheralLink,
    /// `true` when this side is the designated dialer for the pair. The
    /// other side accepts an inbound link instead.
    dials: bool,
    /// When the current run of attempts started, for the give-up clocks.
    trying_since: Option<Instant>,
    attempts: usize,
    /// Set while backing off: no dial before this instant.
    retry_at: Option<Instant>,
    gave_up: bool,
    link: Option<LiveLink>,
}

struct LiveLink {
    pump: tokio::task::AbortHandle,
    /// Held so the pump task's `teardown` receiver stays open; dropping this
    /// (on `tear_down`) tells the pump to drop the `DatagramChannel`.
    _teardown: oneshot::Sender<()>,
}

impl Driver {
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        let backend = match self.acquire_backend().await {
            Some(b) => b,
            None => {
                self.set_radio(RadioStatus::Unsupported);
                let _ = self
                    .events_tx
                    .send(PeerLinkEvent::RadioChanged { status: RadioStatus::Unsupported });
                self.idle_until_shutdown(shutdown).await;
                return;
            }
        };
        // Initial usability = the radio is on AND the backend can do the
        // role this config needs. A `PeerLink` created while Bluetooth is off
        // must not spend the retry budget dialing a dead radio; one asked to
        // `AcceptOnly` on a central-only platform must report `Unsupported`,
        // not churn every peer to `GaveUp`.
        let mut events = backend.events();
        let caps = backend.capabilities().await;
        let role_supported = match self.config.role {
            LinkRole::DialOnly => caps.central,
            LinkRole::AcceptOnly => caps.peripheral,
            // Symmetric needs central to dial at all; accepting is a bonus
            // it degrades to giving up on if peripheral is missing.
            LinkRole::Symmetric { .. } => caps.central,
        };
        let initial = if !role_supported {
            RadioStatus::Unsupported
        } else {
            backend.radio_status().await
        };
        self.set_radio(initial);
        let _ = self
            .events_tx
            .send(PeerLinkEvent::RadioChanged { status: initial });

        let mut radio = match initial {
            RadioStatus::On => RadioState::On,
            RadioStatus::Off => RadioState::Off,
            RadioStatus::Unsupported => RadioState::Unsupported,
        };
        let mut peers: HashMap<PeerAddress, Peer> = HashMap::new();
        let (conn_tx, mut conn_rx) =
            mpsc::unbounded_channel::<(PeerAddress, Result<DatagramChannel>)>();
        let (linkgone_tx, mut linkgone_rx) = mpsc::unbounded_channel::<PeerAddress>();
        let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel::<DatagramChannel>();
        // The inbound server: a task, plus when to (re)start it. `serve`
        // exits on a transient advertise/setup failure, so the driver must
        // notice and restart it rather than sitting without a server until
        // the radio next toggles.
        let mut serve: Option<tokio::task::JoinHandle<()>> = None;
        let mut serve_retry_at: Option<Instant> = None;
        let mut tick = tokio::time::interval(TICK_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        if self.accepts() && radio.usable() {
            serve = Some(self.spawn_serve(&backend, &inbound_tx));
        }

        loop {
            // (Re)start the inbound server when it is wanted, absent, and off
            // any backoff.
            if self.accepts()
                && radio.usable()
                && serve.is_none()
                && serve_retry_at.is_none_or(|at| Instant::now() >= at)
            {
                serve = Some(self.spawn_serve(&backend, &inbound_tx));
                serve_retry_at = None;
            }

            // Wake at the earliest per-peer deadline (a backoff expiring, a
            // give-up clock running out) rather than only on the 1s tick —
            // with a short retry budget the tick alone lets a peer give up
            // before its own retry_at fires.
            let wake = self
                .next_wake(&peers, radio, serve_retry_at)
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));

            tokio::select! {
                _ = &mut shutdown => break,

                _ = tokio::time::sleep_until(wake) => {
                    self.on_tick(&mut peers, radio);
                }

                Some(()) = async {
                    match serve.as_mut() {
                        Some(h) => { let _ = h.await; Some(()) }
                        None => std::future::pending().await,
                    }
                } => {
                    // The server task ended — a transient serve/advertise
                    // failure. Back off, then the top of the loop restarts it.
                    serve = None;
                    serve_retry_at = Some(Instant::now() + SERVE_RETRY_DELAY);
                }

                cmd = self.cmd_rx.recv() => match cmd {
                    None => break,
                    Some(Command::Track { peer, peer_id }) => {
                        self.on_track(&mut peers, peer, peer_id, radio);
                    }
                    Some(Command::Untrack(peer)) => {
                        if let Some(mut p) = peers.remove(&peer) {
                            let had_link = p.link.is_some();
                            self.tear_down(&mut p);
                            if had_link {
                                let _ = self.events_tx.send(PeerLinkEvent::Down { peer: peer.clone() });
                            }
                        }
                        self.publish_status(&peer, PeerStatus::Untracked);
                    }
                    Some(Command::Retry(peer)) => {
                        if let Some(p) = peers.get_mut(&peer) {
                            p.gave_up = false;
                            p.attempts = 0;
                            p.retry_at = None;
                            p.trying_since = Some(Instant::now());
                        }
                    }
                    Some(Command::Discover(reply)) => {
                        let _ = reply.send(self.discover_now(&backend).await);
                    }
                },

                Some(ev) = events.next() => {
                    let was_usable = radio.usable();
                    self.on_backend_event(&mut peers, &mut radio, ev);
                    // Stop the inbound server when the radio goes down; the
                    // top of the loop restarts it when the radio is back.
                    if self.accepts() && was_usable && !radio.usable() {
                        if let Some(h) = serve.take() {
                            h.abort();
                        }
                        serve_retry_at = None;
                    }
                }

                Some((peer, result)) = conn_rx.recv() => {
                    self.on_dial_result(&mut peers, peer, result, &linkgone_tx);
                }

                Some(channel) = inbound_rx.recv() => {
                    self.on_inbound(&mut peers, channel, radio, &linkgone_tx);
                }

                Some(peer) = linkgone_rx.recv() => {
                    self.on_link_gone(&mut peers, peer);
                }

                _ = tick.tick() => {
                    self.on_tick(&mut peers, radio);
                }
            }
            self.pump_dials(&backend, &mut peers, radio, &conn_tx);
            self.refresh_all_status(&peers, radio);
        }

        if let Some(h) = serve.take() {
            h.abort();
        }
        for (_, mut p) in peers.drain() {
            self.tear_down(&mut p);
        }
    }

    /// Whether this role ever accepts an inbound link.
    fn accepts(&self) -> bool {
        !matches!(self.config.role, LinkRole::DialOnly)
    }

    /// The earliest instant the driver needs to wake — a backoff expiring, a
    /// give-up clock running out, or the inbound server's restart backoff.
    fn next_wake(
        &self, peers: &HashMap<PeerAddress, Peer>, radio: RadioState,
        serve_retry_at: Option<Instant>,
    ) -> Option<tokio::time::Instant> {
        let now = Instant::now();
        let base = tokio::time::Instant::now();
        let mut earliest: Option<Duration> = None;
        let mut consider = |d: Duration| {
            earliest = Some(earliest.map_or(d, |e| e.min(d)));
        };
        for p in peers.values() {
            if p.link.is_some() || p.gave_up {
                continue;
            }
            if let Some(at) = p.retry_at {
                consider(at.saturating_duration_since(now));
            }
            // The give-up clock only counts while the radio is usable (see
            // `on_tick`); scheduling a wake for an already-expired deadline
            // during an outage would just spin the loop.
            if radio.usable() {
                if let Some(since) = p.trying_since {
                    let deadline = if p.dials {
                        self.config.retry_budget.give_up_after
                    } else {
                        self.config.retry_budget.acceptor_deadline
                    };
                    consider((since + deadline).saturating_duration_since(now));
                }
            }
        }
        if let Some(at) = serve_retry_at {
            consider(at.saturating_duration_since(now));
        }
        earliest.map(|d| base + d)
    }

    fn spawn_serve(
        &self, backend: &Arc<dyn Backend>, inbound_tx: &mpsc::UnboundedSender<DatagramChannel>,
    ) -> tokio::task::JoinHandle<()> {
        let backend = backend.clone();
        let cfg = self.config.datagram.clone();
        let inbound_tx = inbound_tx.clone();
        tokio::spawn(async move {
            match datagram::serve(backend, &cfg).await {
                Ok(mut stream) => {
                    while let Some(channel) = stream.next().await {
                        if inbound_tx.send(channel).is_err() {
                            break;
                        }
                    }
                }
                Err(err) => log::warn!("peer_link: serve failed: {err}"),
            }
        })
    }

    fn on_inbound(
        &self, peers: &mut HashMap<PeerAddress, Peer>, channel: DatagramChannel, radio: RadioState,
        linkgone_tx: &mpsc::UnboundedSender<PeerAddress>,
    ) {
        let peer = channel.peer();
        // `datagram::serve` already serves one central at a time (its wire
        // shape — notify is a broadcast that cannot be addressed), so the
        // acceptor side of `PeerLink` holds at most one inbound link. A
        // second tracked acceptor peer stays `Connecting` until the slot
        // frees or it gives up. Beyond that, honour `max_links` across both
        // directions so a `Symmetric` instance at its outbound cap does not
        // exceed it on an inbound link.
        let over_cap = Self::committed(peers) >= self.config.max_links.0;
        let Some(p) = peers.get_mut(&peer) else {
            // Not a tracked peer — do not serve a stranger. Dropping the
            // channel disconnects it.
            return;
        };
        // If we are the designated dialer for this pair, already linked, over
        // the link cap, or the radio is not usable, this inbound connection
        // loses: drop it (which disconnects it).
        if p.dials || p.link.is_some() || over_cap || !radio.usable() {
            return;
        }
        let now = Instant::now();
        let session = now.elapsed().as_nanos() as u64;
        let (pl, _) = p.peripheral.apply(PeripheralEvent::CentralConnected { session }, now);
        p.peripheral = pl;
        let (pl, _) = p.peripheral.apply(PeripheralEvent::CentralSubscribed, now);
        p.peripheral = pl;
        p.gave_up = false;
        p.trying_since = None;
        p.retry_at = None;
        let max_len = channel.max_message_len();
        let (link, handle) = self.start_pump(channel, peer.clone(), linkgone_tx.clone());
        p.link = Some(link);
        let _ = self.events_tx.send(PeerLinkEvent::Up {
            peer,
            max_message_len: max_len,
            channel: handle,
        });
    }

    async fn acquire_backend(&mut self) -> Option<Arc<dyn Backend>> {
        if let Some(b) = self.provided_backend.take() {
            return Some(b);
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            crate::backend::platform().await.ok()
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            None
        }
    }

    async fn idle_until_shutdown(&mut self, mut shutdown: oneshot::Receiver<()>) {
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                cmd = self.cmd_rx.recv() => match cmd {
                    None => return,
                    Some(Command::Discover(reply)) => {
                        let _ = reply.send(Err(BleError::AdapterUnavailable("no BLE adapter".into())));
                    }
                    Some(Command::Track { peer, .. }) => {
                        self.publish_status(&peer, PeerStatus::Unavailable);
                    }
                    Some(_) => {}
                },
            }
        }
    }

    async fn discover_now(&self, backend: &Arc<dyn Backend>) -> Result<Vec<PeerAddress>> {
        let mut stream = backend.scan(self.config.datagram.service).await?;
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + DISCOVER_WINDOW;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                item = stream.next() => match item {
                    Some(Ok(peer)) => out.push(peer.address),
                    Some(Err(err)) => return Err(err),
                    None => break,
                },
            }
        }
        Ok(out)
    }

    fn on_track(
        &self, peers: &mut HashMap<PeerAddress, Peer>, peer: PeerAddress, peer_id: LinkId,
        radio: RadioState,
    ) {
        let dials = match &self.config.role {
            LinkRole::DialOnly => true,
            LinkRole::AcceptOnly => false,
            LinkRole::Symmetric { local_id } => *local_id < peer_id,
        };
        peers.entry(peer.clone()).or_insert_with(|| Peer {
            central: CentralLink::new(),
            peripheral: PeripheralLink::new(),
            dials,
            // The give-up clock starts when the peer actually starts trying —
            // for the acceptor that is now (it is waiting for an inbound
            // link); for the dialer it is when it gets a slot and its first
            // dial fires (`pump_dials`), so a peer sitting `Queued` behind a
            // full `max_links` is not ticked to `GaveUp` while it never had a
            // chance.
            trying_since: if dials { None } else { Some(Instant::now()) },
            attempts: 0,
            retry_at: None,
            gave_up: false,
            link: None,
        });
        self.refresh_status(peers, &peer, radio);
    }

    fn pump_dials(
        &self, backend: &Arc<dyn Backend>, peers: &mut HashMap<PeerAddress, Peer>,
        radio: RadioState,
        conn_tx: &mpsc::UnboundedSender<(PeerAddress, Result<DatagramChannel>)>,
    ) {
        if !radio.usable() {
            return;
        }
        let now = Instant::now();
        // A dial in flight reserves a slot too — otherwise with max_links = 1
        // a stalled dial for one peer lets a second dial start, and both can
        // land, exceeding the cap.
        let committed = peers
            .values()
            .filter(|p| p.link.is_some() || matches!(p.central, CentralLink::Dialing { .. }))
            .count();
        let mut budget = self.config.max_links.0.saturating_sub(committed);

        let mut candidates: Vec<PeerAddress> = peers.keys().cloned().collect();
        candidates.sort_by(|a, b| a.0.cmp(&b.0));

        let mut next_session: u64 = now.elapsed().as_nanos() as u64;
        for peer in candidates {
            let p = peers.get_mut(&peer).unwrap();
            if p.link.is_some() || p.gave_up || !p.dials || !p.central.may_dial() {
                continue;
            }
            if p.retry_at.is_some_and(|at| now < at) {
                continue;
            }
            if budget == 0 {
                continue; // status projected as Queued by `refresh_status`
            }
            budget -= 1;
            next_session = next_session.wrapping_add(1);
            let (next, _) = p.central.apply(CentralEvent::DialStarted { session: next_session }, now);
            p.central = next;
            p.attempts += 1;
            // The give-up clock starts here, on the first real attempt — not
            // at `track()` — so a peer that queued behind a full cap is not
            // charged for the wait.
            p.trying_since.get_or_insert(now);

            let backend = backend.clone();
            let cfg = self.config.datagram.clone();
            let conn_tx = conn_tx.clone();
            let peer2 = peer.clone();
            tokio::spawn(async move {
                let result = datagram::connect(backend, &peer2, &cfg).await;
                let _ = conn_tx.send((peer2, result));
            });
        }
    }

    fn on_dial_result(
        &self, peers: &mut HashMap<PeerAddress, Peer>, peer: PeerAddress,
        result: Result<DatagramChannel>, linkgone_tx: &mpsc::UnboundedSender<PeerAddress>,
    ) {
        let Some(p) = peers.get_mut(&peer) else {
            return;
        };
        let now = Instant::now();
        match result {
            Ok(channel) => {
                let (next, _) = p.central.apply(CentralEvent::DialSucceeded, now);
                p.central = next;
                // The state may have moved past `Dialing` while this dial ran
                // — a radio-off, an untrack/retrack, the dial deadline. If so
                // `apply(DialSucceeded)` was a no-op and this result is stale:
                // drop the channel (its `Drop` disconnects) rather than
                // installing a link nothing is expecting.
                if !matches!(p.central, CentralLink::Connected { .. }) {
                    drop(channel);
                    return;
                }
                p.attempts = 0;
                p.trying_since = None;
                p.retry_at = None;
                let max_len = channel.max_message_len();
                let (link, handle) = self.start_pump(channel, peer.clone(), linkgone_tx.clone());
                p.link = Some(link);
                let _ = self.events_tx.send(PeerLinkEvent::Up {
                    peer,
                    max_message_len: max_len,
                    channel: handle,
                });
            }
            Err(_err) => {
                let (next, _) = p.central.apply(CentralEvent::DialFailed, now);
                p.central = next;
                self.apply_backoff(p, now);
            }
        }
    }

    fn on_link_gone(&self, peers: &mut HashMap<PeerAddress, Peer>, peer: PeerAddress) {
        if let Some(p) = peers.get_mut(&peer) {
            if p.link.take().is_some() {
                let now = Instant::now();
                // One of the two applies; the other is a no-op from its
                // current state.
                let (c, _) = p.central.apply(CentralEvent::LinkDropped, now);
                p.central = c;
                let (pl, _) = p.peripheral.apply(PeripheralEvent::CentralDropped, now);
                p.peripheral = pl;
                p.trying_since = Some(now);
                p.attempts = 0;
                p.retry_at = None;
                let _ = self.events_tx.send(PeerLinkEvent::Down { peer });
            }
        }
    }

    fn apply_backoff(&self, p: &mut Peer, now: Instant) {
        let since = *p.trying_since.get_or_insert(now);
        if now.duration_since(since) >= self.config.retry_budget.give_up_after {
            p.gave_up = true;
            p.retry_at = None;
            return;
        }
        let backoff = &self.config.retry_budget.backoff;
        if backoff.is_empty() {
            p.gave_up = true;
            return;
        }
        let idx = p.attempts.saturating_sub(1).min(backoff.len() - 1);
        p.retry_at = Some(now + backoff[idx]);
    }

    fn start_pump(
        &self, mut channel: DatagramChannel, peer: PeerAddress,
        linkgone_tx: mpsc::UnboundedSender<PeerAddress>,
    ) -> (LiveLink, Arc<PeerChannel>) {
        let max_message_len = channel.max_message_len();
        let (send_tx, mut send_rx) =
            mpsc::channel::<(Vec<u8>, oneshot::Sender<Result<()>>)>(CHANNEL_SEND_QUEUE);
        let (recv_tx, recv_rx) = mpsc::channel::<Result<Vec<u8>>>(CHANNEL_RECV_QUEUE);
        let (teardown_tx, mut teardown_rx) = oneshot::channel::<()>();

        let peer2 = peer.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut teardown_rx => break,
                    req = send_rx.recv() => match req {
                        None => break,
                        Some((bytes, reply)) => {
                            let _ = reply.send(channel.send(bytes).await);
                        }
                    },
                    item = channel.recv() => match item {
                        Some(msg) => {
                            if recv_tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
            drop(channel); // Drop disconnects the underlying link.
            let _ = linkgone_tx.send(peer2);
        });

        let handle = Arc::new(PeerChannel {
            peer,
            max_message_len,
            send_tx,
            recv_rx: AsyncMutex::new(recv_rx),
        });
        (LiveLink { pump: task.abort_handle(), _teardown: teardown_tx }, handle)
    }

    fn on_backend_event(
        &self, peers: &mut HashMap<PeerAddress, Peer>, radio: &mut RadioState, ev: GattEvent,
    ) {
        match ev {
            GattEvent::RadioChanged { status } => {
                let event = match status {
                    RadioStatus::On => RadioEvent::PoweredOn,
                    RadioStatus::Off => RadioEvent::PoweredOff,
                    RadioStatus::Unsupported => RadioEvent::NoAdapter,
                };
                let (next, effects) = radio.apply(event, Instant::now());
                *radio = next;
                self.set_radio(status);
                let _ = self.events_tx.send(PeerLinkEvent::RadioChanged { status });

                if !effects.is_empty() {
                    let now = Instant::now();
                    for (peer, p) in peers.iter_mut() {
                        let (c, _) = p.central.apply(CentralEvent::RadioLost, now);
                        p.central = c;
                        let (pl, _) = p.peripheral.apply(PeripheralEvent::RadioLost, now);
                        p.peripheral = pl;
                        if let Some(link) = p.link.take() {
                            link.pump.abort();
                            let _ = self.events_tx.send(PeerLinkEvent::Down { peer: peer.clone() });
                        }
                        p.gave_up = false;
                        p.attempts = 0;
                        p.retry_at = None;
                        // Same rule as `track`: the dialer's clock restarts
                        // when it next actually dials; the acceptor's runs
                        // from now (it will be waiting for an inbound link
                        // once the radio is back).
                        p.trying_since = if p.dials { None } else { Some(now) };
                    }
                }
                if radio.usable() {
                    let now = Instant::now();
                    for p in peers.values_mut() {
                        let (c, _) = p.central.apply(CentralEvent::RadioBack, now);
                        p.central = c;
                        let (pl, _) = p.peripheral.apply(PeripheralEvent::RadioBack, now);
                        p.peripheral = pl;
                    }
                }
            }
            GattEvent::Disconnected { peer, local_role: Role::Central, .. } => {
                if let Some(p) = peers.get_mut(&peer) {
                    if p.link.is_none() {
                        let now = Instant::now();
                        let (c, _) = p.central.apply(CentralEvent::LinkDropped, now);
                        p.central = c;
                    }
                }
            }
            _ => {}
        }
    }

    fn on_tick(&self, peers: &mut HashMap<PeerAddress, Peer>, radio: RadioState) {
        let now = Instant::now();
        for p in peers.values_mut() {
            let (c, _) = p.central.apply(CentralEvent::Tick, now);
            p.central = c;

            // The give-up clock only runs while there is actually a chance of
            // connecting. With the radio off, nothing is being attempted —
            // ticking a peer to `gave_up` here would make `RadioBack` leave
            // it permanently skipped (`RadioLost` already reset `trying_since`,
            // so without this the deadline would still elapse against a dead
            // radio).
            if !radio.usable() || p.link.is_some() || p.gave_up {
                continue;
            }
            let deadline = if p.dials {
                self.config.retry_budget.give_up_after
            } else {
                self.config.retry_budget.acceptor_deadline
            };
            if let Some(since) = p.trying_since {
                if now.duration_since(since) >= deadline {
                    p.gave_up = true;
                    p.retry_at = None;
                }
            }
        }
    }

    fn tear_down(&self, p: &mut Peer) {
        if let Some(link) = p.link.take() {
            link.pump.abort();
            drop(link);
        }
        // Reset both machines to their start state; the dropped channel has
        // already done the platform teardown.
        p.central = CentralLink::new();
        p.peripheral = PeripheralLink::new();
    }

    // ---- status projection ------------------------------------------------

    fn committed(peers: &HashMap<PeerAddress, Peer>) -> usize {
        peers
            .values()
            .filter(|p| p.link.is_some() || matches!(p.central, CentralLink::Dialing { .. }))
            .count()
    }

    fn refresh_all_status(&self, peers: &HashMap<PeerAddress, Peer>, radio: RadioState) {
        let over_cap = Self::committed(peers) >= self.config.max_links.0;
        for (peer, p) in peers {
            self.publish_status(peer, project(p, radio, over_cap));
        }
    }

    fn refresh_status(
        &self, peers: &HashMap<PeerAddress, Peer>, peer: &PeerAddress, radio: RadioState,
    ) {
        if let Some(p) = peers.get(peer) {
            let over_cap = Self::committed(peers) >= self.config.max_links.0;
            self.publish_status(peer, project(p, radio, over_cap));
        }
    }

    fn publish_status(&self, peer: &PeerAddress, status: PeerStatus) {
        let changed = {
            let mut map = self.status.lock().unwrap();
            match status {
                PeerStatus::Untracked => map.remove(peer).is_some(),
                _ => map.insert(peer.clone(), status) != Some(status),
            }
        };
        if changed {
            let _ = self
                .events_tx
                .send(PeerLinkEvent::Status { peer: peer.clone(), status });
        }
    }

    fn set_radio(&self, status: RadioStatus) {
        *self.radio.lock().unwrap() = status;
    }
}

fn project(p: &Peer, radio: RadioState, over_cap: bool) -> PeerStatus {
    if !radio.usable() {
        return PeerStatus::Unavailable;
    }
    if p.link.is_some() {
        return PeerStatus::Connected;
    }
    if p.gave_up {
        return PeerStatus::GaveUp;
    }
    if p.retry_at.is_some() {
        return PeerStatus::Waiting;
    }
    if over_cap && p.dials {
        return PeerStatus::Queued;
    }
    PeerStatus::Connecting
}

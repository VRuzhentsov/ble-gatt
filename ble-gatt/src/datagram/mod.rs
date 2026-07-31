//! The datagram tier: an ordered, opaque-bytes channel to a peer.
//!
//! Raw GATT (`crate::backend`) is characteristic-oriented and caps every
//! write at the negotiated MTU. This module turns that into a message pipe —
//! hand it a `Vec<u8>` of any size, get exactly that `Vec<u8>` out the other
//! end — by fragmenting against the live connection's `max_write_len()` and
//! reassembling under strict bounds.
//!
//! **This tier carries bytes and does not interpret them.** Payloads may be
//! ciphertext, and nothing here inspects content, assumes UTF-8, or imposes
//! framing of its own. Layering encryption on top is the expected use; see
//! `docs/adr/0003`. Consumers that instead need to speak a *third party's*
//! GATT protocol (vendor sensor firmware with its own framing) should use
//! `crate::backend` directly and ignore this module entirely.
//!
//! ## Wire shape
//!
//! One service, one characteristic, both directions — the topology Bitchat
//! uses and the one `GattCharacteristicSpec` already produces:
//!
//! - central → peripheral: characteristic write
//! - peripheral → central: notify on that same characteristic
//!
//! ## Known limitation: the peripheral role serves one central at a time
//!
//! `Backend::notify` pushes to *every* subscribed central — the only
//! per-characteristic primitive both platform backends expose today. With a
//! single connected central this is exactly point-to-point, which is what
//! both current consumers need.
//!
//! With several it would be actively unsafe rather than merely wasteful:
//! each central receives the others' fragments, and since every channel
//! starts its `msg_id` at 0, the first message on two channels shares both
//! `msg_id` and usually `total`. The `InconsistentTotal` guard only fires
//! when `total` differs, so those fragments interleave in a third party's
//! reassembler and complete into a blend of two messages — delivered as
//! valid, with no error.
//!
//! `serve` therefore refuses additional centrals while one is active.
//! Lifting that needs a per-peer notify on the `Backend` port:
//! straightforward on Android (`notifyCharacteristicChanged` already takes a
//! device), unresolved on BlueZ where `bluer`'s notifier does not identify
//! its subscriber.

pub mod fragment;
pub mod reassembly;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_stream::StreamExt;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::datagram::fragment::{split, FragmentHeader, FRAGMENT_HEADER_LEN, MAX_FRAGMENTS};
use crate::datagram::reassembly::{Accept, Reassembler, ReassemblyLimits};
use crate::error::{BleError, Result};
use crate::models::{
    CharacteristicUuid, GattCharacteristicSpec, GattEvent, GattServiceSpec, PeerAddress,
    ServiceUuid, WriteType,
};

pub const DEFAULT_MAX_MESSAGE_LEN: usize = 1024 * 1024;
pub const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_CONCURRENT_REASSEMBLIES: usize = 4;
/// Completed messages buffered before the reassembler applies backpressure.
/// Bounded deliberately: an unbounded queue lets a peer sending valid
/// messages faster than the consumer reads them grow memory without limit.
pub const DEFAULT_INBOUND_QUEUE_DEPTH: usize = 32;

#[derive(Debug, Clone)]
pub struct DatagramConfig {
    pub service: ServiceUuid,
    pub characteristic: CharacteristicUuid,
    pub max_message_len: usize,
    pub reassembly_timeout: Duration,
    pub max_concurrent_reassemblies: usize,
    /// Defaults to `WithResponse`: this is a reliable channel, and ATT write
    /// requests are acknowledged. `WithoutResponse` trades that for
    /// throughput and will silently drop under load — opt in only where the
    /// layer above tolerates loss.
    pub write_type: WriteType,
    /// How many *completed* messages may sit unread before the reassembler
    /// stops accepting more. The reassembly limits bound only partial
    /// messages, so without this a slow or stalled consumer is a memory
    /// exhaustion vector.
    pub inbound_queue_depth: usize,
}

impl DatagramConfig {
    pub fn new(service: ServiceUuid, characteristic: CharacteristicUuid) -> Self {
        Self {
            service,
            characteristic,
            max_message_len: DEFAULT_MAX_MESSAGE_LEN,
            reassembly_timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            max_concurrent_reassemblies: DEFAULT_MAX_CONCURRENT_REASSEMBLIES,
            write_type: WriteType::WithResponse,
            inbound_queue_depth: DEFAULT_INBOUND_QUEUE_DEPTH,
        }
    }

    fn limits(&self) -> ReassemblyLimits {
        ReassemblyLimits {
            max_message_len: self.max_message_len,
            reassembly_timeout: self.reassembly_timeout,
            max_concurrent_reassemblies: self.max_concurrent_reassemblies,
        }
    }

    /// The GATT service this tier expects on the wire. Both roles must agree,
    /// so both derive it from here rather than hand-rolling a spec.
    pub fn service_spec(&self) -> GattServiceSpec {
        GattServiceSpec {
            uuid: self.service,
            characteristics: vec![GattCharacteristicSpec {
                uuid: self.characteristic,
                readable: true,
                writable: true,
                notifiable: true,
                initial_value: Vec::new(),
            }],
        }
    }
}

/// The configured ceiling, lowered to whatever the fragment budget can
/// actually carry. Without this the two limits disagree: a peripheral
/// budgets 14-byte fragments against the spec-minimum MTU, capping a
/// message at ~896 KiB, while `max_message_len` defaults to 1 MiB — so
/// messages in the gap passed the size check and then failed inside
/// `split()` one call later.
fn effective_max_message_len(configured: usize, fragment_budget: usize) -> usize {
    configured.min(fragment_budget.saturating_mul(MAX_FRAGMENTS))
}

/// How a channel pushes bytes at its peer — the only thing that differs
/// between the two roles once fragmentation is factored out.
enum Sink {
    /// Central: write to the remote characteristic.
    Connection {
        connection: Box<dyn GattConnection>,
        write_type: WriteType,
    },
    /// Peripheral: notify subscribed centrals. See the module-level note on
    /// this being a broadcast.
    Notify { backend: Arc<dyn Backend> },
}

/// An ordered, opaque-bytes channel to one peer.
pub struct DatagramChannel {
    peer: PeerAddress,
    characteristic: CharacteristicUuid,
    sink: Mutex<Sink>,
    inbound: ReceiverStream<Result<Vec<u8>>>,
    next_msg_id: u16,
    max_message_len: usize,
    /// Fixed at construction from the negotiated MTU. BLE does not
    /// renegotiate mid-session in practice, so this is not refreshed.
    fragment_budget: usize,
    /// Background tasks owned by this channel, aborted on drop. Without
    /// this, dropping a channel without calling `close()` leaves the
    /// reassembly and link-loss tasks running until the peer or the OS
    /// happens to end the link.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for DatagramChannel {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl DatagramChannel {
    pub fn peer(&self) -> PeerAddress {
        self.peer.clone()
    }

    /// Largest payload a single `send` will accept on *this* channel.
    ///
    /// May be lower than the configured `max_message_len`: a message also
    /// has to fit in `MAX_FRAGMENTS` fragments of `fragment_budget` bytes.
    /// The peripheral role budgets against the spec-minimum MTU, so its
    /// real ceiling is well under the 1 MiB default — reporting the
    /// configured value there would promise a size that `send` then
    /// refuses.
    pub fn max_message_len(&self) -> usize {
        self.max_message_len
    }

    /// Payload bytes carried per fragment on this channel. Exposed mainly so
    /// tests can assert a message genuinely fragmented rather than trusting
    /// that it did.
    pub fn fragment_budget(&self) -> usize {
        self.fragment_budget
    }

    pub async fn send(&mut self, payload: Vec<u8>) -> Result<()> {
        if payload.len() > self.max_message_len {
            return Err(BleError::Gatt(format!(
                "message of {} bytes exceeds the {}-byte limit for this channel",
                payload.len(),
                self.max_message_len
            )));
        }
        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1);

        let fragments = split(msg_id, &payload, self.fragment_budget)?;
        let characteristic = self.characteristic;
        let mut sink = self.sink.lock().await;
        for fragment in fragments {
            match &mut *sink {
                Sink::Connection {
                    connection,
                    write_type,
                } => {
                    connection
                        .write_with_type(characteristic, fragment, *write_type)
                        .await?;
                }
                Sink::Notify { backend } => {
                    backend.notify(characteristic, fragment).await?;
                }
            }
        }
        Ok(())
    }

    /// Next complete message, or `None` once the channel is closed — which
    /// includes the peer vanishing without warning, not just an orderly
    /// `close()`. Without that, a caller mid-conversation would block
    /// forever on a dead link.
    pub async fn recv(&mut self) -> Option<Result<Vec<u8>>> {
        self.inbound.next().await
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut sink = self.sink.lock().await;
        match &mut *sink {
            Sink::Connection { connection, .. } => connection.disconnect().await,
            // The peripheral does not own the link; a central disconnecting
            // is what ends it. Stopping advertising here would tear down
            // every other peer's channel too.
            Sink::Notify { .. } => Ok(()),
        }
    }
}

/// Feed inbound fragments through a reassembler and forward whole messages.
/// Shared by both roles — the only difference is where fragments come from.
///
/// Takes sole ownership of `out`. That is deliberate: the returned handle's
/// `abort()` is then sufficient to close the consumer's `recv()`, because
/// aborting drops the only sender. An earlier version handed a clone to the
/// link-loss watcher as well, so dropping the watcher's copy closed nothing
/// and `recv()` hung forever on a dead link.
fn spawn_reassembly<S>(
    mut fragments: S, limits: ReassemblyLimits, out: mpsc::Sender<Result<Vec<u8>>>,
) -> tokio::task::JoinHandle<()>
where
    S: tokio_stream::Stream<Item = Vec<u8>> + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut reassembler = Reassembler::new(limits);
        // `Reassembler::expire` only runs from inside `accept`, so a peer
        // that sends half a message and then goes quiet would otherwise pin
        // its partial sets for the life of the link — `reassembly_timeout`
        // would silently never fire. Ticking here is what makes the
        // configured timeout real on an idle connection.
        let tick = (limits.reassembly_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                incoming = fragments.next() => {
                    let Some(raw) = incoming else {
                        // Fragment source ended: the link is gone. Dropping
                        // `out` here is what makes `recv()` return `None`.
                        return;
                    };
                    let Some((header, payload)) = FragmentHeader::parse(&raw) else {
                        // Too short to be ours — a foreign write to our
                        // characteristic, not a reason to kill the channel.
                        continue;
                    };
                    match reassembler.accept(header, payload, Instant::now()) {
                        Accept::Complete(message) => {
                            // Bounded, and awaited rather than dropped: an
                            // unbounded queue let a peer sending valid
                            // single-fragment messages faster than the
                            // consumer reads grow memory without limit.
                            // Blocking here pushes back through the BLE
                            // stack, which is where backpressure belongs.
                            if out.send(Ok(message)).await.is_err() {
                                return; // receiver dropped; stop reassembling
                            }
                        }
                        Accept::Pending => {}
                        Accept::Rejected(_) => {
                            // Dropped by policy (bounds breach or malformed).
                            // The channel stays usable: one bad message must
                            // not deny service for the rest of the session.
                        }
                    }
                }
                _ = tokio::time::sleep(tick) => {
                    reassembler.expire(Instant::now());
                }
            }
        }
    })
}

/// Central role: connect to `peer` and bring up a channel over `config`'s
/// service.
pub async fn connect(
    backend: Arc<dyn Backend>, peer: &PeerAddress, config: &DatagramConfig,
) -> Result<DatagramChannel> {
    let mut connection = backend.connect(peer).await?;
    let notifications = connection.subscribe(config.characteristic).await?;

    let budget = connection
        .max_write_len()
        .checked_sub(FRAGMENT_HEADER_LEN)
        .filter(|budget| *budget > 0)
        .ok_or_else(|| {
            BleError::Gatt(format!(
                "negotiated MTU leaves no room for payload after the \
                 {FRAGMENT_HEADER_LEN}-byte fragment header"
            ))
        })?;

    // Subscribe to events *before* spawning the watcher. `events()` is a
    // broadcast subscription, so a disconnect emitted between here and the
    // task's first poll would otherwise be missed entirely — leaving
    // `recv()` pending forever on a link that is already gone.
    let events = backend.events();

    let (tx, rx) = mpsc::channel(config.inbound_queue_depth);
    let reassembly = spawn_reassembly(notifications, config.limits(), tx);
    // The watcher owns the reassembly handle so it can abort it on link
    // loss; the channel keeps the watcher's handle so dropping the channel
    // tears both down.
    let watcher = spawn_link_loss_watch(events, peer.clone(), reassembly);

    Ok(DatagramChannel {
        peer: peer.clone(),
        characteristic: config.characteristic,
        sink: Mutex::new(Sink::Connection {
            connection,
            write_type: config.write_type,
        }),
        inbound: ReceiverStream::new(rx),
        next_msg_id: 0,
        max_message_len: effective_max_message_len(config.max_message_len, budget),
        fragment_budget: budget,
        tasks: vec![watcher],
    })
}

/// Turn link loss into `recv() -> None` by aborting the reassembly task,
/// which drops the only sender on the inbound channel.
///
/// No error is delivered — "the peer walked away" is not a fault of the
/// caller, and a closed channel already says everything it needs to. The
/// notification stream itself cannot be relied on to end here: on a real
/// backend a subscription outlives the link, so waiting for it to close
/// would be waiting forever.
fn spawn_link_loss_watch(
    mut events: BoxStream<GattEvent>, peer: PeerAddress, reassembly: tokio::task::JoinHandle<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            if matches!(&event, GattEvent::Disconnected { peer: lost } if *lost == peer) {
                reassembly.abort();
                return;
            }
        }
        // Event stream ended: nothing more can arrive, so the reassembly
        // task would otherwise linger for the life of the process.
        reassembly.abort();
    })
}

/// Peripheral role: advertise `config`'s service and serve **one** central
/// at a time.
///
/// # Why one, and not many
///
/// `Backend::notify` broadcasts to every subscribed central — it is the only
/// per-characteristic primitive both platforms expose. That makes serving
/// two centrals concurrently actively unsafe, not merely wasteful:
///
/// - every central receives every other central's fragments, and
/// - each `DatagramChannel` starts its `msg_id` counter at 0, so the very
///   first message on two channels shares both `msg_id` and often `total`.
///   The `InconsistentTotal` guard only fires when `total` differs, so those
///   fragments interleave inside a third party's reassembler and complete
///   into a message that is a blend of two — delivered to `recv()` as valid,
///   with no error.
///
/// Silent corruption is worse than a missing feature, so additional centrals
/// are refused while one is active rather than served incorrectly. Lifting
/// this needs a per-peer notify on the `Backend` port: straightforward on
/// Android (`notifyCharacteristicChanged` already takes a device),
/// unresolved on BlueZ.
pub async fn serve(
    backend: Arc<dyn Backend>, config: &DatagramConfig,
) -> Result<BoxStream<DatagramChannel>> {
    backend.advertise(config.service_spec()).await?;

    let (channels_tx, channels_rx) = mpsc::unbounded_channel();
    let mut events = backend.events();
    let config = config.clone();
    // The peripheral has no `GattConnection` to ask for a negotiated MTU, so
    // it budgets against the spec-minimum. Conservative on purpose:
    // undersized fragments always fit, oversized ones would be truncated by
    // the stack with no error.
    let peripheral_budget = crate::backend::DEFAULT_ATT_MTU as usize
        - crate::backend::ATT_HEADER_LEN
        - FRAGMENT_HEADER_LEN;

    tokio::spawn(async move {
        // At most one entry: see the doc comment on why concurrent centrals
        // are refused rather than served.
        let mut inbound: HashMap<PeerAddress, mpsc::UnboundedSender<Vec<u8>>> = HashMap::new();

        while let Some(event) = events.next().await {
            match event {
                GattEvent::Connected { peer } => {
                    // `Connected` is NOT once-per-connection. Both real
                    // backends re-emit it ahead of every characteristic
                    // write, because neither has a true server-side
                    // connection signal to key off (see `Backend::events`).
                    // Rebuilding the channel each time would drop the
                    // previous fragment sender, killing the channel already
                    // handed to the caller and making any multi-fragment
                    // message impossible to reassemble.
                    if inbound.contains_key(&peer) {
                        continue;
                    }
                    if let Some(active) = inbound.keys().next() {
                        eprintln!(
                            "[ble-gatt][datagram] refusing central {} — already serving {} \
                             and notify is a broadcast, so serving both would corrupt \
                             each other's messages",
                            peer.0, active.0
                        );
                        continue;
                    }
                    let (frag_tx, frag_rx) = mpsc::unbounded_channel();
                    let (msg_tx, msg_rx) = mpsc::channel(config.inbound_queue_depth);
                    let reassembly = spawn_reassembly(
                        UnboundedReceiverStream::new(frag_rx),
                        config.limits(),
                        msg_tx,
                    );
                    inbound.insert(peer.clone(), frag_tx);

                    let channel = DatagramChannel {
                        peer: peer.clone(),
                        characteristic: config.characteristic,
                        sink: Mutex::new(Sink::Notify {
                            backend: backend.clone(),
                        }),
                        inbound: ReceiverStream::new(msg_rx),
                        next_msg_id: 0,
                        max_message_len: effective_max_message_len(
                            config.max_message_len,
                            peripheral_budget,
                        ),
                        fragment_budget: peripheral_budget,
                        tasks: vec![reassembly],
                    };
                    if channels_tx.send(channel).is_err() {
                        return; // consumer stopped accepting channels
                    }
                }
                GattEvent::CharacteristicWritten {
                    peer,
                    characteristic,
                    value,
                } => {
                    if characteristic != config.characteristic {
                        continue;
                    }
                    if let Some(tx) = inbound.get(&peer) {
                        let _ = tx.send(value);
                    }
                }
                GattEvent::Disconnected { peer } => {
                    // Dropping the fragment sender ends that peer's
                    // reassembly task, which closes its channel's `recv()`.
                    inbound.remove(&peer);
                }
            }
        }
    });

    Ok(Box::pin(UnboundedReceiverStream::new(channels_rx)))
}

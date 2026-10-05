//! `Connection`: one GATT client connection that publishes its own state
//! (ADR-0007 D5). The model is Kable's and Nordic's `StateFlow<State>`: any
//! number of subscribers follow a `tokio::sync::watch` channel and see the
//! current state as soon as they subscribe. A `CancellationToken` derived
//! from it ties a task to the connection in one line.

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::entities::error::Result;
use crate::entities::models::{
    CharacteristicUuid, ConnectionPriority, ConnectionState, GattEvent, PeerAddress, RadioStatus, Role,
    WriteType,
};
use crate::hal::{BoxStream, GattConnection};

pub struct Connection {
    /// Taken only by `Drop`, to disconnect in the background.
    inner: Option<Box<dyn GattConnection>>,
    /// Cleared by `disconnect()`, so `Drop` does not disconnect twice.
    open: bool,
    state_tx: watch::Sender<ConnectionState>,
    cancel: CancellationToken,
    watcher: JoinHandle<()>,
}

impl Connection {
    /// Wraps a connection this device dialled as `Role::Central`. `events`
    /// must be subscribed **before** the connection was made, so a
    /// disconnect that happens in between is not missed.
    pub(crate) fn new(inner: Box<dyn GattConnection>, events: BoxStream<GattEvent>) -> Self {
        let (state_tx, _) = watch::channel(ConnectionState::Connected);
        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch_for_end(
            events,
            inner.peer(),
            inner.session(),
            state_tx.clone(),
            cancel.clone(),
        ));
        Self {
            inner: Some(inner),
            open: true,
            state_tx,
            cancel,
            watcher,
        }
    }

    /// The connection's state, published (ADR-0007 D5). Each receiver sees
    /// the current value at once and every change after it.
    pub fn state(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }

    /// Fires when the connection ends, for any reason, including this
    /// `Connection` being dropped:
    /// `tokio::spawn(conn.cancelled().run_until_cancelled_owned(task))`.
    pub fn cancelled(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn peer(&self) -> PeerAddress {
        self.conn().peer()
    }

    pub fn session(&self) -> Option<u64> {
        self.conn().session()
    }

    pub fn att_mtu(&self) -> u16 {
        self.conn().att_mtu()
    }

    pub fn max_write_len(&self) -> usize {
        self.conn().max_write_len()
    }

    pub async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        self.conn_mut().read(characteristic).await
    }

    pub async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.conn_mut().write(characteristic, value).await
    }

    pub async fn write_with_type(
        &mut self, characteristic: CharacteristicUuid, value: Vec<u8>, write_type: WriteType,
    ) -> Result<()> {
        self.conn_mut().write_with_type(characteristic, value, write_type).await
    }

    pub async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Result<Vec<u8>>>> {
        self.conn_mut().subscribe(characteristic).await
    }

    pub async fn request_connection_priority(&mut self, priority: ConnectionPriority) -> Result<()> {
        self.conn_mut().request_connection_priority(priority).await
    }

    /// Disconnects and publishes `Disconnected`, whatever the platform
    /// answered.
    pub async fn disconnect(&mut self) -> Result<()> {
        let result = self.conn_mut().disconnect().await;
        self.open = false;
        end(&self.state_tx, &self.cancel);
        result
    }

    /// The raw port, for APIs that take a `GattConnection` (the datagram
    /// profile's `DatagramChannel`). State keeps being published until this
    /// `Connection` is dropped.
    pub fn gatt(&mut self) -> &mut dyn GattConnection {
        self.conn_mut()
    }

    fn conn(&self) -> &dyn GattConnection {
        self.inner.as_deref().expect("only Drop takes the connection")
    }

    fn conn_mut(&mut self) -> &mut dyn GattConnection {
        self.inner.as_deref_mut().expect("only Drop takes the connection")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.watcher.abort();
        end(&self.state_tx, &self.cancel);
        // No driver disconnects on its own drop; on Android the
        // `BluetoothGatt` stays open and the next connect to the peer is
        // refused as already open. `Drop` cannot await, so a task does it,
        // as `DatagramChannel`'s `Drop` does.
        let Some(mut inner) = self.inner.take().filter(|_| self.open) else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            if let Err(err) = inner.disconnect().await {
                log::warn!("drop: could not disconnect {}: {err}", inner.peer().0);
            }
        });
    }
}

fn end(state_tx: &watch::Sender<ConnectionState>, cancel: &CancellationToken) {
    state_tx.send_if_modified(|state| {
        let changed = state.is_connected();
        *state = ConnectionState::Disconnected;
        changed
    });
    cancel.cancel();
}

/// Whether `event` ends the connection to `peer` this device dialled.
/// Events from another connection to the same peer (a different `session`)
/// do not; where either side has no session, the address decides.
fn ends_connection(event: &GattEvent, peer: &PeerAddress, session: Option<u64>) -> bool {
    match event {
        GattEvent::Disconnected {
            peer: event_peer,
            local_role: Role::Central,
            session: event_session,
        } => {
            event_peer == peer
                && match (session, event_session) {
                    (Some(ours), Some(theirs)) => ours == *theirs,
                    _ => true,
                }
        }
        GattEvent::RadioChanged { status } => *status != RadioStatus::On,
        _ => false,
    }
}

async fn watch_for_end(
    mut events: BoxStream<GattEvent>, peer: PeerAddress, session: Option<u64>,
    state_tx: watch::Sender<ConnectionState>, cancel: CancellationToken,
) {
    while let Some(event) = events.next().await {
        if ends_connection(&event, &peer, session) {
            end(&state_tx, &cancel);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disconnected(peer: &str, role: Role, session: Option<u64>) -> GattEvent {
        GattEvent::Disconnected {
            peer: PeerAddress(peer.into()),
            local_role: role,
            session,
        }
    }

    #[test]
    fn only_this_connections_disconnect_ends_it() {
        let peer = PeerAddress("AA".into());
        assert!(ends_connection(&disconnected("AA", Role::Central, Some(1)), &peer, Some(1)));
        assert!(ends_connection(&disconnected("AA", Role::Central, None), &peer, Some(1)));
        assert!(!ends_connection(&disconnected("AA", Role::Central, Some(2)), &peer, Some(1)));
        assert!(!ends_connection(&disconnected("BB", Role::Central, Some(1)), &peer, Some(1)));
        // The same peer connected to our GATT server is a different link.
        assert!(!ends_connection(&disconnected("AA", Role::Peripheral, Some(1)), &peer, Some(1)));
    }

    #[test]
    fn the_radio_going_away_ends_it() {
        let peer = PeerAddress("AA".into());
        let off = GattEvent::RadioChanged {
            status: RadioStatus::Off,
        };
        let on = GattEvent::RadioChanged { status: RadioStatus::On };
        assert!(ends_connection(&off, &peer, None));
        assert!(!ends_connection(&on, &peer, None));
    }
}

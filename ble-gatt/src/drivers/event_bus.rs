//! Bounded fanout only: no connection ownership, work queue or callback task.
use crate::entities::models::{GattEvent, PeerAddress, Role};
use crate::hal::BoxStream;

type ConnectedSessions = Vec<(PeerAddress, Role, Option<u64>)>;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

#[derive(Clone)]
pub(crate) struct EventBus {
    legacy: broadcast::Sender<GattEvent>,
    critical: broadcast::Sender<GattEvent>,
    connected: Arc<Mutex<ConnectedSessions>>,
    capacity: usize,
}

impl EventBus {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            legacy: broadcast::channel(capacity).0,
            critical: broadcast::channel(capacity).0,
            connected: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
            capacity,
        }
    }

    /// Synchronous bounded sends, safe in native callbacks. Legacy vocabulary
    /// (including redundant Connected) is untouched.
    pub(crate) fn send(
        &self,
        event: GattEvent,
    ) -> Result<usize, broadcast::error::SendError<GattEvent>> {
        if !matches!(event, GattEvent::CharacteristicWritten { .. }) {
            self.send_lifecycle(event.clone());
        }
        self.send_legacy(event)
    }

    /// Remote forwarding must keep source-port provenance. A legacy Lagged
    /// item must never be promoted to critical loss at the receiving end.
    pub(crate) fn send_legacy(
        &self,
        event: GattEvent,
    ) -> Result<usize, broadcast::error::SendError<GattEvent>> {
        self.legacy.send(event)
    }

    pub(crate) fn send_lifecycle(&self, event: GattEvent) {
        if matches!(event, GattEvent::CharacteristicWritten { .. }) {
            return;
        }
        // Serialize classification with publishing so racing native callbacks
        // cannot reorder Connected/Disconnected around the deduplication state.
        let mut connected = self.connected.lock().unwrap();
        match &event {
            GattEvent::Connected {
                peer,
                local_role,
                session,
            } => {
                if let Some(entry) = connected
                    .iter_mut()
                    .find(|entry| &entry.0 == peer && entry.1 == *local_role)
                {
                    if entry.2 == *session {
                        return;
                    }
                    entry.2 = *session;
                } else if connected.len() < self.capacity {
                    connected.push((peer.clone(), *local_role, *session));
                }
                // Beyond the bounded cache, forward rather than hide a real
                // transition. Overflow is always observable as critical Lagged.
            }
            GattEvent::Disconnected {
                peer,
                local_role,
                session,
            } => {
                connected.retain(|entry| {
                    !(&entry.0 == peer
                        && entry.1 == *local_role
                        && (session.is_none() || entry.2 == *session))
                });
            }
            GattEvent::RadioChanged { .. } | GattEvent::Lagged { .. } => connected.clear(),
            GattEvent::CharacteristicWritten { .. } => unreachable!(),
        }
        let _ = self.critical.send(event);
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<GattEvent> {
        self.legacy.subscribe()
    }
    pub(crate) fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        Box::pin(
            BroadcastStream::new(self.critical.subscribe()).map(|item| match item {
                Ok(event) => event,
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(dropped)) => {
                    GattEvent::Lagged { dropped }
                }
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dedup_is_critical_only_and_preserves_roles_generations_and_stale_disconnects() {
        let bus = EventBus::new(16);
        let mut legacy = bus.subscribe();
        let mut critical = bus.lifecycle_events();
        let peer = PeerAddress("peer".into());
        let connected = |local_role, session| GattEvent::Connected {
            peer: peer.clone(),
            local_role,
            session: Some(session),
        };
        let disconnected = |session| GattEvent::Disconnected {
            peer: peer.clone(),
            local_role: Role::Peripheral,
            session: Some(session),
        };
        let _ = bus.send(connected(Role::Peripheral, 1));
        let _ = bus.send(connected(Role::Peripheral, 1));
        let _ = bus.send(connected(Role::Peripheral, 2));
        let _ = bus.send(connected(Role::Central, 2));
        let _ = bus.send(disconnected(1));
        let _ = bus.send(connected(Role::Peripheral, 2));
        let _ = bus.send(disconnected(2));
        let _ = bus.send(connected(Role::Peripheral, 2));
        assert!(matches!(
            critical.next().await,
            Some(GattEvent::Connected {
                local_role: Role::Peripheral,
                session: Some(1),
                ..
            })
        ));
        assert!(matches!(
            critical.next().await,
            Some(GattEvent::Connected {
                local_role: Role::Peripheral,
                session: Some(2),
                ..
            })
        ));
        assert!(matches!(
            critical.next().await,
            Some(GattEvent::Connected {
                local_role: Role::Central,
                session: Some(2),
                ..
            })
        ));
        assert!(matches!(
            critical.next().await,
            Some(GattEvent::Disconnected {
                session: Some(1),
                ..
            })
        ));
        assert!(matches!(
            critical.next().await,
            Some(GattEvent::Disconnected {
                session: Some(2),
                ..
            })
        ));
        assert!(matches!(
            critical.next().await,
            Some(GattEvent::Connected {
                session: Some(2),
                ..
            })
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), critical.next())
                .await
                .is_err()
        );
        // Every original item (including both duplicates) stays on legacy.
        for _ in 0..8 {
            legacy.try_recv().unwrap();
        }
        assert!(legacy.try_recv().is_err());
    }
}

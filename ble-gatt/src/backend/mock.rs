//! In-process mock backend: no radio, no OS Bluetooth stack. Two
//! `MockBackend`s that share the same `MockNetwork` can scan/connect/serve
//! against each other, exercising the real `Backend`/`GattConnection` trait
//! contract end-to-end. Mirrors Fini's `transport::sim` adapter — a
//! first-class stand-in for CI-safe protocol tests, not a mock of one.
//!
//! Built entirely on `tokio::sync::{broadcast, Mutex}` rather than
//! hand-rolled pub-sub plumbing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::backend::{Backend, BoxStream, GattConnection};
use crate::error::{BleError, Result};
use crate::models::{
    CapabilityReport, CharacteristicUuid, DiscoveredPeer, GattEvent, GattServiceSpec, PeerAddress,
    ServiceUuid,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;
const NOTIFY_CHANNEL_CAPACITY: usize = 64;

struct PeripheralState {
    service: GattServiceSpec,
    values: HashMap<CharacteristicUuid, Vec<u8>>,
    notify_tx: HashMap<CharacteristicUuid, broadcast::Sender<Vec<u8>>>,
    events_tx: broadcast::Sender<GattEvent>,
}

/// Shared "radio" for a set of `MockBackend`s. Construct one per test and
/// hand an `Arc` clone to each simulated peer — there is no global registry,
/// so unrelated tests never see each other's peers.
#[derive(Default)]
pub struct MockNetwork {
    peripherals: Mutex<HashMap<PeerAddress, PeripheralState>>,
}

impl MockNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

pub struct MockBackend {
    address: PeerAddress,
    network: Arc<MockNetwork>,
    capabilities: CapabilityReport,
}

impl MockBackend {
    /// `capabilities` lets tests simulate a peer that can't do peripheral
    /// mode (e.g. most Android devices) without a second backend impl — see
    /// the plan's role-assignment-with-capability-fallback decision.
    pub fn new(address: PeerAddress, network: Arc<MockNetwork>, capabilities: CapabilityReport) -> Self {
        Self {
            address,
            network,
            capabilities,
        }
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn capabilities(&self) -> CapabilityReport {
        self.capabilities
    }

    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<DiscoveredPeer>> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let matches: Vec<DiscoveredPeer> = peripherals
            .iter()
            .filter(|(addr, state)| **addr != self.address && state.service.uuid == service)
            .map(|(addr, state)| DiscoveredPeer {
                address: addr.clone(),
                name: None,
                services: vec![state.service.uuid],
            })
            .collect();
        drop(peripherals);
        Ok(Box::pin(tokio_stream::iter(matches)))
    }

    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let events_tx = {
            let peripherals = self.network.peripherals.lock().unwrap();
            let state = peripherals.get(peer).ok_or_else(|| BleError::ConnectFailed {
                peer: peer.0.clone(),
                reason: "peer is not advertising".to_string(),
            })?;
            state.events_tx.clone()
        };
        let _ = events_tx.send(GattEvent::Connected {
            peer: self.address.clone(),
        });
        Ok(Box::new(MockGattConnection {
            central: self.address.clone(),
            peripheral: peer.clone(),
            network: self.network.clone(),
        }))
    }

    async fn advertise(&self, service: GattServiceSpec) -> Result<()> {
        if !self.capabilities.peripheral {
            return Err(BleError::PeripheralUnsupported);
        }
        let (events_tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let mut values = HashMap::new();
        let mut notify_tx = HashMap::new();
        for characteristic in &service.characteristics {
            values.insert(characteristic.uuid, characteristic.initial_value.clone());
            notify_tx.insert(characteristic.uuid, broadcast::channel(NOTIFY_CHANNEL_CAPACITY).0);
        }
        let mut peripherals = self.network.peripherals.lock().unwrap();
        peripherals.insert(
            self.address.clone(),
            PeripheralState {
                service,
                values,
                notify_tx,
                events_tx,
            },
        );
        Ok(())
    }

    async fn stop_advertising(&self) -> Result<()> {
        self.network.peripherals.lock().unwrap().remove(&self.address);
        Ok(())
    }

    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.address)
            .ok_or_else(|| BleError::Gatt("not advertising".to_string()))?;
        let tx = state
            .notify_tx
            .get(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let _ = tx.send(value);
        Ok(())
    }

    fn events(&self) -> BoxStream<GattEvent> {
        let peripherals = self.network.peripherals.lock().unwrap();
        match peripherals.get(&self.address) {
            Some(state) => {
                let rx = state.events_tx.subscribe();
                Box::pin(BroadcastStream::new(rx).filter_map(|item| item.ok()))
            }
            None => Box::pin(tokio_stream::empty()),
        }
    }
}

struct MockGattConnection {
    central: PeerAddress,
    peripheral: PeerAddress,
    network: Arc<MockNetwork>,
}

#[async_trait]
impl GattConnection for MockGattConnection {
    fn peer(&self) -> PeerAddress {
        self.peripheral.clone()
    }

    async fn read(&mut self, characteristic: CharacteristicUuid) -> Result<Vec<u8>> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.peripheral)
            .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
        state
            .values
            .get(&characteristic)
            .cloned()
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))
    }

    async fn write(&mut self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        let events_tx = {
            let mut peripherals = self.network.peripherals.lock().unwrap();
            let state = peripherals
                .get_mut(&self.peripheral)
                .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
            state.values.insert(characteristic, value.clone());
            state.events_tx.clone()
        };
        let _ = events_tx.send(GattEvent::CharacteristicWritten {
            peer: self.central.clone(),
            characteristic,
            value,
        });
        Ok(())
    }

    async fn subscribe(&mut self, characteristic: CharacteristicUuid) -> Result<BoxStream<Vec<u8>>> {
        let peripherals = self.network.peripherals.lock().unwrap();
        let state = peripherals
            .get(&self.peripheral)
            .ok_or_else(|| BleError::NotConnected(self.peripheral.0.clone()))?;
        let tx = state
            .notify_tx
            .get(&characteristic)
            .ok_or_else(|| BleError::Gatt("unknown characteristic".to_string()))?;
        let rx = tx.subscribe();
        Ok(Box::pin(BroadcastStream::new(rx).filter_map(|item| item.ok())))
    }

    async fn disconnect(&mut self) -> Result<()> {
        let events_tx = {
            let peripherals = self.network.peripherals.lock().unwrap();
            peripherals.get(&self.peripheral).map(|state| state.events_tx.clone())
        };
        if let Some(tx) = events_tx {
            let _ = tx.send(GattEvent::Disconnected {
                peer: self.central.clone(),
            });
        }
        Ok(())
    }
}

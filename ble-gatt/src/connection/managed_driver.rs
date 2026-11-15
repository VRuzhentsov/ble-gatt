//! Validate metadata on the connection that will carry managed messages.
use super::{acquired_cleanup::Cleanup, managed_codec, peer_link::EndpointId};
use crate::advertisement::{GattAdvertisement, GATT_ADVERTISEMENT_CHARACTERISTIC};
use crate::{
    BleError, BoxStream, CapabilityReport, CharacteristicUuid, DiscoveredPeer, Driver,
    GattConnection, GattEvent, GattServiceSpec, PeerAddress, RadioStatus, Result, ServiceUuid,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

pub(super) struct ManagedDriver {
    pub inner: Arc<dyn Driver>,
    pub service: ServiceUuid,
    pub endpoint: Arc<Mutex<Option<EndpointId>>>,
    pub cleanup: Cleanup,
    pub validate_metadata: bool,
}

#[async_trait]
impl Driver for ManagedDriver {
    async fn capabilities(&self) -> CapabilityReport {
        self.inner.capabilities().await
    }
    async fn scan(&self, service: ServiceUuid) -> Result<BoxStream<Result<DiscoveredPeer>>> {
        self.inner.scan(service).await
    }
    async fn connect(&self, peer: &PeerAddress) -> Result<Box<dyn GattConnection>> {
        let mut acquired = self.cleanup.own(self.inner.connect(peer).await?);
        if !self.validate_metadata {
            return Ok(Box::new(acquired));
        }
        let bytes = acquired.read(GATT_ADVERTISEMENT_CHARACTERISTIC).await?;
        let advertisement = GattAdvertisement::decode(&bytes)?;
        let bytes = advertisement
            .service_data
            .get(&self.service)
            .ok_or_else(|| BleError::Protocol("missing managed profile".into()))?;
        *self.endpoint.lock().unwrap() = Some(managed_codec::endpoint(bytes)?);
        Ok(Box::new(acquired))
    }
    async fn advertise(&self, spec: GattServiceSpec) -> Result<()> {
        self.inner.advertise(spec).await
    }
    async fn update_advertisement(&self, spec: GattServiceSpec) -> Result<()> {
        self.inner.update_advertisement(spec).await
    }
    fn served_service(&self) -> Option<GattServiceSpec> {
        self.inner.served_service()
    }
    async fn set_characteristic_value(
        &self,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        self.inner
            .set_characteristic_value(characteristic, value)
            .await
    }
    async fn stop_advertising(&self) -> Result<()> {
        self.inner.stop_advertising().await
    }
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
    async fn notify(&self, characteristic: CharacteristicUuid, value: Vec<u8>) -> Result<()> {
        self.inner.notify(characteristic, value).await
    }
    async fn notify_peer(
        &self,
        peer: &PeerAddress,
        session: Option<u64>,
        characteristic: CharacteristicUuid,
        value: Vec<u8>,
    ) -> Result<()> {
        self.inner
            .notify_peer(peer, session, characteristic, value)
            .await
    }
    async fn disconnect_peer(&self, peer: &PeerAddress, session: Option<u64>) -> Result<()> {
        self.inner.disconnect_peer(peer, session).await
    }
    async fn notify_max_len(&self, peer: &PeerAddress, session: Option<u64>) -> Option<usize> {
        self.inner.notify_max_len(peer, session).await
    }
    fn lifecycle_events(&self) -> BoxStream<GattEvent> {
        self.inner.lifecycle_events()
    }
    fn events(&self) -> BoxStream<GattEvent> {
        self.inner.events()
    }
    async fn radio_status(&self) -> RadioStatus {
        self.inner.radio_status().await
    }
}

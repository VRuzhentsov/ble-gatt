//! Central role, scanning: `BluetoothLEAdvertisementWatcher`.
//!
//! The watcher scans actively, so each peer can send two `Received` events:
//! its advertisement and its scan response. Only the advertisement carries
//! the service UUID. [`ScanPeers`] therefore starts tracking an address when
//! one of its events is for the service, then also merges that address's
//! scan responses. It holds at most [`MAX_SCAN_PEERS`] addresses and forgets
//! the least recently seen one to take a new one. For each
//! address it keeps the latest packet of each advertisement type and reports
//! their merge, so a field the peer stops sending leaves the result. A peer
//! is reported again whenever its merged data changes, which is the
//! `Driver::scan` contract.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use windows::core::Ref;
use windows::Devices::Bluetooth::Advertisement::{
    BluetoothLEAdvertisementReceivedEventArgs, BluetoothLEAdvertisementScanParameters,
    BluetoothLEAdvertisementType, BluetoothLEAdvertisementWatcher,
    BluetoothLEAdvertisementWatcherStoppedEventArgs, BluetoothLEScanningMode,
};
use windows::Devices::Bluetooth::{BluetoothAddressType, BluetoothError};
use windows::Foundation::TypedEventHandler;

use super::utils::WindowsCodec;
use super::{Inner, DISCOVERY_QUEUE_DEPTH};
use crate::entities::error::{BleError, Result};
use crate::entities::models::{
    advertised_name, carries_fast_advertisement, DiscoveredPeer, PeerAddress, ServiceUuid,
};

/// Most addresses one scan tracks, so a crowded room cannot grow
/// [`ScanPeers`] without limit. A new address beyond it replaces the least
/// recently seen one.
const MAX_SCAN_PEERS: usize = 1024;

/// The fields of one advertising packet, or of several merged.
#[derive(Default, Clone, PartialEq)]
struct AdvertisedData {
    services: Vec<ServiceUuid>,
    manufacturer_data: BTreeMap<u16, Vec<u8>>,
    service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    local_name: Option<String>,
}

impl AdvertisedData {
    /// Add `packet`'s fields; where both hold one, `packet`'s wins.
    fn overlay(&mut self, packet: &AdvertisedData) {
        for uuid in &packet.services {
            if !self.services.contains(uuid) {
                self.services.push(*uuid);
            }
        }
        self.manufacturer_data
            .extend(packet.manufacturer_data.clone());
        self.service_data.extend(packet.service_data.clone());
        if packet.local_name.is_some() {
            self.local_name = packet.local_name.clone();
        }
    }
}

/// What one tracked address advertises now: the latest packet of each
/// advertisement type it sent. One address can send advertisements of two
/// types (a Windows peer's GATT provider is connectable, its fast
/// advertisement publisher non-connectable), and a scan response is a type
/// of its own, so a newer packet replaces only its own type's fields.
#[derive(Default)]
struct SeenPeer {
    /// Keyed by the `BluetoothLEAdvertisementType` value.
    packets: BTreeMap<i32, AdvertisedData>,
    /// The merged data last put in the result queue. A result the full
    /// queue refused is not delivered, so the next event tries it again.
    delivered: Option<AdvertisedData>,
    /// [`ScanPeers::events`] when this address last sent an event taken.
    last_seen: u64,
}

impl SeenPeer {
    /// Replace the latest packet of `advertisement_type`.
    fn record(&mut self, advertisement_type: BluetoothLEAdvertisementType, packet: AdvertisedData) {
        self.packets.insert(advertisement_type.0, packet);
    }

    /// The merged data, when it differs from the data last delivered.
    fn undelivered(&self) -> Option<AdvertisedData> {
        let merged = self.merged();
        (self.delivered.as_ref() != Some(&merged)).then_some(merged)
    }

    /// Note that `data` reached the result queue.
    fn mark_delivered(&mut self, data: AdvertisedData) {
        self.delivered = Some(data);
    }

    /// The latest packets merged, in advertisement type order: the scan
    /// response's fields win over the connectable advertisement's.
    fn merged(&self) -> AdvertisedData {
        let mut merged = AdvertisedData::default();
        for packet in self.packets.values() {
            merged.overlay(packet);
        }
        merged
    }
}

/// The addresses one scan tracks, keyed by the WinRT 64-bit address.
#[derive(Default)]
struct ScanPeers {
    peers: HashMap<u64, SeenPeer>,
    /// Counts the events taken, so [`SeenPeer::last_seen`] orders the
    /// addresses from least to most recently seen.
    events: u64,
}

impl ScanPeers {
    /// The record one `Received` event merges into, or `None` when the event
    /// is dropped. A tracked address takes events for the service and scan
    /// responses. A new address needs an event for the service; when
    /// [`MAX_SCAN_PEERS`] addresses are tracked, it replaces the least
    /// recently seen one.
    fn record_for(
        &mut self,
        address: u64,
        for_service: bool,
        scan_response: bool,
    ) -> Option<&mut SeenPeer> {
        let admitted = if self.peers.contains_key(&address) {
            for_service || scan_response
        } else {
            for_service
        };
        if !admitted {
            return None;
        }
        if !self.peers.contains_key(&address) && self.peers.len() >= MAX_SCAN_PEERS {
            self.evict_least_recent();
        }
        self.events += 1;
        let entry = self.peers.entry(address).or_default();
        entry.last_seen = self.events;
        Some(entry)
    }

    /// Forget the address seen longest ago. The search is linear, and runs
    /// only when a new address arrives at the limit.
    fn evict_least_recent(&mut self) {
        let oldest = self
            .peers
            .iter()
            .min_by_key(|(_, peer)| peer.last_seen)
            .map(|(address, _)| *address);
        if let Some(address) = oldest {
            self.peers.remove(&address);
        }
    }
}

/// The address type of each address scans admitted, for `connect`: Android
/// advertises from a random address, and WinRT needs to be told the type to
/// dial it. Outlives one scan, so a peer found by the last scan can be
/// dialled. Holds at most [`MAX_SCAN_PEERS`] addresses, and forgets the
/// least recently seen one to take a new one, like [`ScanPeers`].
#[derive(Default)]
pub(super) struct AddressTypes {
    /// Each address's type and the [`AddressTypes::events`] count when it
    /// was last recorded.
    types: HashMap<u64, (BluetoothAddressType, u64)>,
    events: u64,
}

impl AddressTypes {
    pub(super) fn record(&mut self, address: u64, kind: BluetoothAddressType) {
        if !self.types.contains_key(&address) && self.types.len() >= MAX_SCAN_PEERS {
            let oldest = self
                .types
                .iter()
                .min_by_key(|(_, (_, seen))| *seen)
                .map(|(address, _)| *address);
            if let Some(oldest) = oldest {
                self.types.remove(&oldest);
            }
        }
        self.events += 1;
        self.types.insert(address, (kind, self.events));
    }

    pub(super) fn get(&self, address: u64) -> Option<BluetoothAddressType> {
        self.types.get(&address).map(|(kind, _)| *kind)
    }
}

pub(super) struct ScanStream {
    inner: Arc<Inner>,
    rx: ReceiverStream<Result<DiscoveredPeer>>,
    error: Arc<StdMutex<Option<BleError>>>,
    done: bool,
    watcher: BluetoothLEAdvertisementWatcher,
    received_token: i64,
    stopped_token: i64,
    generation: u64,
}

/// Logs the first scan's scan-parameter outcome at info level, later ones at
/// debug: it does not change while the process runs.
static SCAN_PARAMETERS_REPORTED: std::sync::Once = std::sync::Once::new();

impl ScanStream {
    /// Ask for the low-latency scan window and interval
    /// (`BluetoothLEAdvertisementScanParameters.LowLatency`, Windows 11
    /// 24H2, build 26100, and later). Managed scans run only while
    /// `discover()` is wanted, so the radio time is short. Where the class
    /// or `IBluetoothLEAdvertisementWatcher3` is missing (`E_NOINTERFACE`,
    /// class not registered) the call fails and the watcher keeps the
    /// system's parameters: that failure is the feature check.
    fn apply_low_latency(watcher: &BluetoothLEAdvertisementWatcher) {
        let outcome = BluetoothLEAdvertisementScanParameters::LowLatency()
            .and_then(|parameters| watcher.SetScanParameters(&parameters));
        let mut first = false;
        SCAN_PARAMETERS_REPORTED.call_once(|| first = true);
        let level = if first {
            log::Level::Info
        } else {
            log::Level::Debug
        };
        match outcome {
            Ok(()) => log::log!(level, "scan parameters: LowLatency applied"),
            Err(err) => log::log!(level, "scan parameters: not available ({err})"),
        }
    }

    /// Whether one advertisement event is for `service`: its UUID list
    /// names it, it carries service data under it (a scan response or an
    /// extended advertisement may hold only that), or it carries a managed
    /// fast advertisement under `fast_company` (a Windows peer publishes that
    /// in an advertisement of its own; the managed scanner checks the record).
    fn matches_service(
        service: ServiceUuid,
        fast_company: u16,
        advertised_uuids: &[ServiceUuid],
        service_data: &BTreeMap<ServiceUuid, Vec<u8>>,
        manufacturer_data: &BTreeMap<u16, Vec<u8>>,
    ) -> bool {
        advertised_uuids.contains(&service)
            || service_data.contains_key(&service)
            || carries_fast_advertisement(fast_company, manufacturer_data)
    }
}

/// Start a watcher for `service`. The caller has already claimed the scan
/// slot under `generation`.
impl ScanStream {
    pub(super) fn start(
        inner: &Arc<Inner>,
        service: ServiceUuid,
        generation: u64,
    ) -> Result<ScanStream> {
        let watcher = BluetoothLEAdvertisementWatcher::new()
            .map_err(|err| BleError::windows_error("AdvertisementWatcher", err))?;
        watcher
            .SetScanningMode(BluetoothLEScanningMode::Active)
            .map_err(|err| BleError::windows_error("SetScanningMode", err))?;
        // Windows 10 2004 and later; harmless to skip where it is missing.
        let _ = watcher.SetAllowExtendedAdvertisements(true);
        Self::apply_low_latency(&watcher);

        let (tx, rx) = mpsc::channel(DISCOVERY_QUEUE_DEPTH);
        let tx = Arc::new(StdMutex::new(Some(tx)));
        let error = Arc::new(StdMutex::new(None));
        let fast_company = inner.fast_scan_company.load(Ordering::Relaxed);

        let received = {
            let tx = tx.clone();
            let weak = Arc::downgrade(inner);
            let peers = StdMutex::new(ScanPeers::default());
            TypedEventHandler::new(
                move |_: Ref<BluetoothLEAdvertisementWatcher>,
                      args: Ref<BluetoothLEAdvertisementReceivedEventArgs>| {
                    let args = args.ok()?;
                    let address = args.BluetoothAddress()?;
                    let advertisement = args.Advertisement()?;
                    let advertised_uuids: Vec<ServiceUuid> = advertisement
                        .ServiceUuids()?
                        .into_iter()
                        .map(|guid| ServiceUuid(WindowsCodec::from_guid(guid)))
                        .collect();
                    let service_data = WindowsCodec::service_data_of(advertisement.DataSections()?);
                    let mut manufacturer_data = BTreeMap::new();
                    for data in advertisement.ManufacturerData()? {
                        manufacturer_data
                            .insert(data.CompanyId()?, WindowsCodec::from_buffer(&data.Data()?)?);
                    }
                    let local_name = advertisement.LocalName()?.to_string();
                    let for_service = Self::matches_service(
                        service,
                        fast_company,
                        &advertised_uuids,
                        &service_data,
                        &manufacturer_data,
                    );
                    let advertisement_type = args.AdvertisementType().unwrap_or_default();
                    let scan_response =
                        advertisement_type == BluetoothLEAdvertisementType::ScanResponse;
                    let packet = AdvertisedData {
                        services: advertised_uuids,
                        manufacturer_data,
                        service_data,
                        local_name: (!local_name.is_empty()).then_some(local_name),
                    };

                    let mut peers = peers.lock().unwrap();
                    let Some(entry) = peers.record_for(address, for_service, scan_response) else {
                        return Ok(());
                    };
                    if let (Some(inner), Ok(kind)) = (weak.upgrade(), args.BluetoothAddressType()) {
                        inner.address_types.lock().unwrap().record(address, kind);
                    }
                    // Report new or changed peer data, including a later scan
                    // response, and data the full queue refused before; a
                    // repeated packet of delivered data changes nothing.
                    entry.record(advertisement_type, packet);
                    let Some(data) = entry.undelivered() else {
                        return Ok(());
                    };
                    let peer = DiscoveredPeer {
                        address: PeerAddress(WindowsCodec::format_address(address)),
                        name: advertised_name(service, &data.service_data, data.local_name.clone()),
                        services: data.services.clone(),
                        manufacturer_data: data.manufacturer_data.clone(),
                        service_data: data.service_data.clone(),
                        rssi: args.RawSignalStrengthInDBm().ok(),
                    };
                    if let Some(tx) = tx.lock().unwrap().as_ref() {
                        if tx.try_send(Ok(peer)).is_ok() {
                            entry.mark_delivered(data);
                        } else {
                            log::debug!("scan: result queue full, retrying on the next event");
                        }
                    }
                    Ok(())
                },
            )
        };
        let stopped = {
            let error = error.clone();
            TypedEventHandler::new(
                move |_: Ref<BluetoothLEAdvertisementWatcher>,
                      args: Ref<BluetoothLEAdvertisementWatcherStoppedEventArgs>| {
                    let reason = args
                        .ok()
                        .and_then(BluetoothLEAdvertisementWatcherStoppedEventArgs::Error)
                        .unwrap_or(BluetoothError::OtherError);
                    log::warn!("scan: watcher stopped, error {}", reason.0);
                    let err = if reason == BluetoothError::RadioNotAvailable
                        || reason == BluetoothError::DisabledByUser
                    {
                        BleError::AdapterUnavailable("the Bluetooth radio is off".into())
                    } else if reason == BluetoothError::ConsentRequired
                        || reason == BluetoothError::DisabledByPolicy
                    {
                        BleError::AdapterUnavailable(format!(
                            "Windows refused the scan (error {})",
                            reason.0
                        ))
                    } else {
                        BleError::Gatt(format!("the scan stopped (error {})", reason.0))
                    };
                    *error.lock().unwrap() = Some(err);
                    // Dropping the sender wakes the stream, which then reports
                    // the error.
                    tx.lock().unwrap().take();
                    Ok(())
                },
            )
        };
        let received_token = watcher
            .Received(&received)
            .map_err(|err| BleError::windows_error("Watcher::Received", err))?;
        let stopped_token = match watcher.Stopped(&stopped) {
            Ok(token) => token,
            Err(err) => {
                let _ = watcher.RemoveReceived(received_token);
                return Err(BleError::windows_error("Watcher::Stopped", err));
            }
        };
        if let Err(err) = watcher.Start() {
            let _ = watcher.RemoveReceived(received_token);
            let _ = watcher.RemoveStopped(stopped_token);
            return Err(BleError::AdapterUnavailable(format!(
                "cannot start the scan: {err}"
            )));
        }
        Ok(ScanStream {
            inner: inner.clone(),
            rx: ReceiverStream::new(rx),
            error,
            done: false,
            watcher,
            received_token,
            stopped_token,
            generation,
        })
    }
}

impl tokio_stream::Stream for ScanStream {
    type Item = Result<DiscoveredPeer>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        if self.done {
            return Poll::Ready(None);
        }
        // Report terminal errors before queued results.
        // Otherwise, a slow consumer could mistake lost results for a complete scan.
        let pending = self.error.lock().unwrap().take();
        if let Some(err) = pending {
            self.done = true;
            return Poll::Ready(Some(Err(err)));
        }
        match std::pin::Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(None) => {
                self.done = true;
                let err = self.error.lock().unwrap().take();
                Poll::Ready(err.map(Err))
            }
            other => other,
        }
    }
}

impl Drop for ScanStream {
    fn drop(&mut self) {
        let _ = self.watcher.RemoveStopped(self.stopped_token);
        let _ = self.watcher.Stop();
        let _ = self.watcher.RemoveReceived(self.received_token);
        let ended = {
            let mut scan = self.inner.scan.lock().unwrap();
            let ended = scan.generation == self.generation;
            if ended {
                scan.active = false;
            }
            ended
        };
        if ended {
            self.inner.pause_publisher(false);
        }
        log::info!("scan: stopped generation={}", self.generation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows::Devices::Bluetooth::Advertisement::BluetoothLEAdvertisementType as Type;

    #[test]
    fn a_result_matches_by_uuid_list_or_by_service_data() {
        let service = ServiceUuid(uuid::Uuid::from_u128(0xB1E6));
        let other = ServiceUuid(uuid::Uuid::from_u128(7));
        let company = crate::entities::models::FastAdvertisement::DEFAULT_SCAN_COMPANY;
        let matches = |uuids: &[ServiceUuid],
                       data: &BTreeMap<ServiceUuid, Vec<u8>>,
                       manufacturer: &BTreeMap<u16, Vec<u8>>| {
            ScanStream::matches_service(service, company, uuids, data, manufacturer)
        };
        let none = BTreeMap::new();
        let no_company = BTreeMap::new();
        assert!(matches(&[service], &none, &no_company));
        assert!(!matches(&[other], &none, &no_company));
        let data: BTreeMap<_, _> = [(service, vec![0xF8; 9])].into();
        assert!(matches(&[], &data, &no_company));
        let foreign: BTreeMap<_, _> = [(other, vec![1])].into();
        assert!(!matches(&[], &foreign, &no_company));
        // A Windows peer's own publisher: only the fast advertisement.
        let fast: BTreeMap<u16, Vec<u8>> = [(0xFFFF, vec![0xFC; 26])].into();
        assert!(matches(&[], &none, &fast));
        let fini: BTreeMap<u16, Vec<u8>> = [(0xFFFF, vec![1, 2, 3, 4, 5])].into();
        assert!(!matches(&[], &none, &fini));
        // The same bytes under another company are not admitted.
        let other_company: BTreeMap<u16, Vec<u8>> = [(0x004C, vec![0xFC; 26])].into();
        assert!(!matches(&[], &none, &other_company));
    }

    /// What the `Received` handler does with a queue that has room: record
    /// the packet, then deliver the merged data if it is undelivered.
    /// Returns whether a result was delivered.
    fn deliver(peer: &mut SeenPeer, advertisement_type: Type, packet: AdvertisedData) -> bool {
        peer.record(advertisement_type, packet);
        match peer.undelivered() {
            Some(data) => {
                peer.mark_delivered(data);
                true
            }
            None => false,
        }
    }

    fn named(name: &str) -> AdvertisedData {
        AdvertisedData {
            local_name: Some(name.into()),
            ..AdvertisedData::default()
        }
    }

    #[test]
    fn a_newer_advertisement_replaces_the_fields_it_no_longer_sends() {
        let service = ServiceUuid(uuid::Uuid::from_u128(0xB1E6));
        let mut peer = SeenPeer::default();
        let first = AdvertisedData {
            services: vec![service],
            manufacturer_data: [(0x0075, vec![1])].into(),
            service_data: [(service, vec![2])].into(),
            local_name: Some("old".into()),
        };
        assert!(deliver(&mut peer, Type::ConnectableUndirected, first));
        let update = AdvertisedData {
            services: vec![service],
            ..AdvertisedData::default()
        };
        assert!(deliver(
            &mut peer,
            Type::ConnectableUndirected,
            update.clone()
        ));
        assert!(peer.merged() == update);
        // A repeated packet changes nothing.
        assert!(!deliver(&mut peer, Type::ConnectableUndirected, update));
    }

    #[test]
    fn a_scan_response_and_a_second_advertisement_add_to_the_advertisement() {
        let service = ServiceUuid(uuid::Uuid::from_u128(0xB1E6));
        let mut peer = SeenPeer::default();
        let provider = AdvertisedData {
            services: vec![service],
            local_name: Some("short".into()),
            ..AdvertisedData::default()
        };
        assert!(deliver(
            &mut peer,
            Type::ConnectableUndirected,
            provider.clone()
        ));
        assert!(deliver(&mut peer, Type::ScanResponse, named("complete")));
        let fast = AdvertisedData {
            manufacturer_data: [(0xFFFF, vec![0xFC; 26])].into(),
            ..AdvertisedData::default()
        };
        assert!(deliver(&mut peer, Type::NonConnectableUndirected, fast));
        // The provider's advertisement again: still merged with the others.
        assert!(!deliver(&mut peer, Type::ConnectableUndirected, provider));
        let merged = peer.merged();
        assert_eq!(merged.services, vec![service]);
        assert_eq!(merged.manufacturer_data[&0xFFFF], vec![0xFC; 26]);
        assert_eq!(merged.local_name.as_deref(), Some("complete"));
        // A scan response without the name removes it; the advertisement's
        // name is reported again.
        assert!(deliver(
            &mut peer,
            Type::ScanResponse,
            AdvertisedData::default()
        ));
        assert_eq!(peer.merged().local_name.as_deref(), Some("short"));
    }

    #[test]
    fn a_result_the_full_queue_refused_is_tried_again_by_the_next_event() {
        let mut peer = SeenPeer::default();
        peer.record(Type::ConnectableUndirected, named("peer"));
        // The queue was full: the result is not marked delivered.
        assert!(peer.undelivered().is_some());
        // The same advertisement again tries the same data again.
        assert!(deliver(
            &mut peer,
            Type::ConnectableUndirected,
            named("peer")
        ));
        assert!(peer.undelivered().is_none());
        assert!(!deliver(
            &mut peer,
            Type::ConnectableUndirected,
            named("peer")
        ));
    }

    #[test]
    fn a_new_address_at_the_limit_replaces_the_least_recently_seen_one() {
        let mut scan = ScanPeers::default();
        for address in 0..MAX_SCAN_PEERS as u64 {
            assert!(scan.record_for(address, true, false).is_some());
        }
        // Address 0 sends a scan response, so address 1 is now the oldest.
        assert!(scan.record_for(0, false, true).is_some());
        let new = MAX_SCAN_PEERS as u64;
        assert!(scan.record_for(new, true, false).is_some());
        assert_eq!(scan.peers.len(), MAX_SCAN_PEERS);
        assert!(scan.peers.contains_key(&0));
        assert!(!scan.peers.contains_key(&1));
        assert!(scan.peers.contains_key(&new));
        // A new address still needs an event for the service.
        assert!(scan.record_for(new + 1, false, true).is_none());
        assert_eq!(scan.peers.len(), MAX_SCAN_PEERS);
    }

    #[test]
    fn address_types_keep_the_most_recently_seen_addresses() {
        let mut types = AddressTypes::default();
        for address in 0..MAX_SCAN_PEERS as u64 {
            types.record(address, BluetoothAddressType::Public);
        }
        // Address 0 is seen again, so address 1 is now the oldest.
        types.record(0, BluetoothAddressType::Random);
        let new = MAX_SCAN_PEERS as u64;
        types.record(new, BluetoothAddressType::Random);
        assert_eq!(types.types.len(), MAX_SCAN_PEERS);
        assert_eq!(types.get(0), Some(BluetoothAddressType::Random));
        assert_eq!(types.get(1), None);
        assert_eq!(types.get(new), Some(BluetoothAddressType::Random));
    }
}

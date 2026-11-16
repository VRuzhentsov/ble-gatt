//! Central role, scanning: `BluetoothLEAdvertisementWatcher`.
//!
//! The scan settings and the matched-address cache are adapted from
//! btleplug (`src/winrtble/ble/watcher.rs`, `BLEWatcher::start` and
//! `MatchCache`), Copyright (c) 2020-2021, Nonpolynomial, BSD-3-Clause: see
//! `LICENSES/btleplug-BSD-3-Clause.txt`. Like btleplug, a scan response is
//! only taken from an address whose advertisement already matched the
//! service, and that set of addresses is bounded.
//!
//! Unlike btleplug, the driver merges the advertisement and the scan
//! response itself and reports a peer again when its data changes, which is
//! the `Driver::scan` contract.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use windows::core::Ref;
use windows::Devices::Bluetooth::Advertisement::{
    BluetoothLEAdvertisementReceivedEventArgs, BluetoothLEAdvertisementType,
    BluetoothLEAdvertisementWatcher, BluetoothLEAdvertisementWatcherStoppedEventArgs,
    BluetoothLEScanningMode,
};
use windows::Devices::Bluetooth::BluetoothError;
use windows::Foundation::TypedEventHandler;

use super::utils::WindowsCodec;
use super::{Inner, DISCOVERY_QUEUE_DEPTH};
use crate::entities::error::{BleError, Result};
use crate::entities::models::{advertised_name, DiscoveredPeer, PeerAddress, ServiceUuid};

/// Most addresses one scan tracks. Same bound as btleplug's
/// `MATCH_CACHE_CAPACITY`; a crowded room must not grow the map without
/// limit.
const MATCH_CACHE_CAPACITY: usize = 1024;

/// What one matched address has advertised so far. Active scanning delivers
/// the advertisement and the scan response as separate `Received` events,
/// and only the advertisement carries the service UUID, so the two are
/// merged here before a peer is reported.
#[derive(Default, Clone, PartialEq)]
struct SeenPeer {
    services: Vec<ServiceUuid>,
    manufacturer_data: BTreeMap<u16, Vec<u8>>,
    service_data: BTreeMap<ServiceUuid, Vec<u8>>,
    local_name: Option<String>,
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

        let (tx, rx) = mpsc::channel(DISCOVERY_QUEUE_DEPTH);
        let tx = Arc::new(StdMutex::new(Some(tx)));
        let error = Arc::new(StdMutex::new(None));

        let received =
            {
                let tx = tx.clone();
                let weak = Arc::downgrade(inner);
                let seen: StdMutex<HashMap<u64, SeenPeer>> = StdMutex::new(HashMap::new());
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
                let matches_service = advertised_uuids.contains(&service);
                let is_scan_response =
                    args.AdvertisementType() == Ok(BluetoothLEAdvertisementType::ScanResponse);

                let mut seen = seen.lock().unwrap();
                if !(matches_service || is_scan_response && seen.contains_key(&address)) {
                    return Ok(());
                }
                if !seen.contains_key(&address) && seen.len() >= MATCH_CACHE_CAPACITY {
                    return Ok(());
                }
                if let (Some(inner), Ok(kind)) = (weak.upgrade(), args.BluetoothAddressType()) {
                    inner.address_types.lock().unwrap().insert(address, kind);
                }
                let entry = seen.entry(address).or_default();
                let previous_data = entry.clone();
                for uuid in advertised_uuids {
                    if !entry.services.contains(&uuid) {
                        entry.services.push(uuid);
                    }
                }
                for data in advertisement.ManufacturerData()? {
                    entry
                        .manufacturer_data
                        .insert(data.CompanyId()?, WindowsCodec::from_buffer(&data.Data()?)?);
                }
                entry
                    .service_data
                    .extend(WindowsCodec::service_data_of(advertisement.DataSections()?));
                let name = advertisement.LocalName()?.to_string();
                if !name.is_empty() {
                    entry.local_name = Some(name);
                }
                // Report new or changed peer data, including a later scan response.
                // Ignore repeated packets with no data changes.
                if *entry == previous_data {
                    return Ok(());
                }
                let peer = DiscoveredPeer {
                    address: PeerAddress(WindowsCodec::format_address(address)),
                    name: advertised_name(service, &entry.service_data, entry.local_name.clone()),
                    services: entry.services.clone(),
                    manufacturer_data: entry.manufacturer_data.clone(),
                    service_data: entry.service_data.clone(),
                    rssi: args.RawSignalStrengthInDBm().ok(),
                };
                if let Some(tx) = tx.lock().unwrap().as_ref() {
                    if tx.try_send(Ok(peer)).is_err() {
                        log::debug!("scan: result queue full, dropping a result");
                    }
                }
                Ok(())
            },
        )
            };
        let stopped = {
            let tx = tx.clone();
            let error = error.clone();
            TypedEventHandler::new(
                move |_: Ref<BluetoothLEAdvertisementWatcher>,
                      args: Ref<BluetoothLEAdvertisementWatcherStoppedEventArgs>| {
                    let reason = args
                        .ok()
                        .and_then(|args| args.Error())
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
        let mut scan = self.inner.scan.lock().unwrap();
        if scan.generation == self.generation {
            scan.active = false;
        }
        log::info!("scan: stopped generation={}", self.generation);
    }
}

//! Peripheral role, radio records: a `BluetoothLEAdvertisementPublisher`
//! next to the GATT provider (ADR-0011 §3, ADR-0012 §2).
//!
//! The provider's advertisement has no room for managed records: Windows
//! repeats the 128-bit service UUID in its service-data section. Google
//! Nearby's Windows medium (`internal/platform/implementation/windows/ble.cc`,
//! Apache-2.0, ideas only) starts a separate publisher for its fast
//! advertisement and keeps the GATT server for reads. This publisher does
//! the same with the fast advertisement as manufacturer data under its
//! company: a legacy, non-connectable advertisement of at most 31 bytes
//! (`GattServiceSpec::validate` caps the record at
//! `FAST_ADVERTISEMENT_MAX_LEN`), which every scanner can see.

use windows::core::Ref;
use windows::Devices::Bluetooth::Advertisement::{
    BluetoothLEAdvertisementPublisher, BluetoothLEAdvertisementPublisherStatus,
    BluetoothLEAdvertisementPublisherStatusChangedEventArgs, BluetoothLEManufacturerData,
};
use windows::Foundation::TypedEventHandler;

use super::utils::WindowsCodec;
use crate::entities::error::{BleError, Result};
use crate::entities::models::{FastAdvertisement, GattServiceSpec};

pub(super) struct FastPublisher {
    publisher: BluetoothLEAdvertisementPublisher,
    status_token: i64,
    /// What it advertises, so an unchanged update keeps it running.
    pub(super) record: FastAdvertisement,
}

impl FastPublisher {
    /// Start a publisher for `spec`'s fast advertisement. `None` when the
    /// spec has none, or Windows refused it (logged): scanners then read the
    /// GATT advertisement, as before.
    /// `paused`: created but not sent yet, because a scan runs.
    pub(super) async fn start(spec: &GattServiceSpec, paused: bool) -> Option<FastPublisher> {
        let record = spec.fast_advertisement.clone()?;
        match Self::try_start(&record, paused).await {
            Ok(publisher) => Some(publisher),
            Err(err) => {
                log::warn!("advertise: fast advertisement publisher not started: {err}");
                None
            }
        }
    }

    async fn try_start(record: &FastAdvertisement, paused: bool) -> Result<FastPublisher> {
        let publisher = BluetoothLEAdvertisementPublisher::new()
            .map_err(|err| BleError::windows_error("AdvertisementPublisher", err))?;
        {
            // `IBuffer` is not `Send`: gone before the next `.await`.
            let data = BluetoothLEManufacturerData::Create(
                record.company_id,
                &WindowsCodec::to_buffer(&record.data)?,
            )
            .map_err(|err| BleError::windows_error("ManufacturerData", err))?;
            publisher
                .Advertisement()
                .and_then(|advertisement| advertisement.ManufacturerData())
                .and_then(|list| list.Append(&data))
                .map_err(|err| BleError::windows_error("ManufacturerData", err))?;
        }
        let status_token = publisher
            .StatusChanged(&TypedEventHandler::new(
                |_: Ref<BluetoothLEAdvertisementPublisher>,
                 args: Ref<BluetoothLEAdvertisementPublisherStatusChangedEventArgs>| {
                    if let Ok(args) = args.ok() {
                        let status = args.Status().map(|status| status.0).unwrap_or(-1);
                        let error = args.Error().map(|error| error.0).unwrap_or(-1);
                        if status == BluetoothLEAdvertisementPublisherStatus::Aborted.0 {
                            log::warn!("advertise: fast advertisement publisher aborted, error {error}");
                        } else {
                            log::debug!("advertise: fast advertisement publisher status {status}, error {error}");
                        }
                    }
                    Ok(())
                },
            ))
            .map_err(|err| BleError::windows_error("Publisher::StatusChanged", err))?;
        let started = FastPublisher {
            publisher,
            status_token,
            record: record.clone(),
        };
        if !paused {
            started
                .publisher
                .Start()
                .map_err(|err| BleError::windows_error("Publisher::Start", err))?;
        }
        log::info!(
            "advertise: fast advertisement published ({} bytes under {:#06x}, legacy{})",
            record.data.len(),
            record.company_id,
            if paused {
                ", paused while scanning"
            } else {
                ""
            }
        );
        Ok(started)
    }

    /// Stop sending while this computer scans, and send again after: the
    /// radio shares its time between the scan and every advertisement, and
    /// with the publisher running the PC heard phones later (ADR-0011 §3).
    pub(super) fn set_paused(&self, paused: bool) {
        let result = if paused {
            self.publisher.Stop()
        } else {
            self.publisher.Start()
        };
        match result {
            Ok(()) => log::debug!("advertise: fast advertisement publisher paused={paused}"),
            Err(err) => {
                log::warn!("advertise: fast advertisement publisher paused={paused} failed: {err}")
            }
        }
    }
}

impl Drop for FastPublisher {
    fn drop(&mut self) {
        let _ = self.publisher.Stop();
        let _ = self.publisher.RemoveStatusChanged(self.status_token);
    }
}

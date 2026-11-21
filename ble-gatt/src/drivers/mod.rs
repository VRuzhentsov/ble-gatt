//! One implementation of the `crate::hal` ports per platform, plus the
//! mock radio (layer: Frameworks & Drivers; see `docs/architecture.md`).

#[cfg(target_os = "android")]
pub mod android;

#[cfg(target_os = "linux")]
pub mod linux;

pub mod mock;

pub(crate) mod event_bus;

#[cfg(target_os = "windows")]
pub mod windows;

use crate::entities::error::{BleError, Result};
use crate::hal::Driver;

/// Only a known dropped notification payload is recoverable. Native operation
/// failures and critical-port lag must not use this constructor.
impl BleError {
    pub(crate) fn notification_receive_gap() -> Self {
        crate::entities::error::BleError::ReceiveGap(
            "notification queue overflowed: payloads were dropped and at least one message is lost"
                .into(),
        )
    }

    /// Decode the `BleGattBridge.startScan` result, not asynchronous Android `ScanCallback` error codes.
    /// This function needs no radio, so host tests can check the JNI contract.
    #[cfg(any(target_os = "android", test))]
    pub(crate) fn android_scan_start_result(status: i32) -> Result<()> {
        use crate::entities::error::BleError;

        // Keep in sync with BleGattBridge's SCAN_* constants (bridge ABI v6).
        match status {
            0 => Ok(()),
            1 => Err(BleError::AdapterBusy(
                "a scan is already active; concurrent scans are not supported".to_string(),
            )),
            2 => Err(BleError::AdapterUnavailable(
                "Android Bluetooth LE scanner is unavailable".to_string(),
            )),
            _ => Err(BleError::Gatt(format!(
                "startScan returned unknown status {status}"
            ))),
        }
    }
}

/// Construct the driver for the platform this binary runs on. Used by
/// `PeerLink::new`; a consumer using the `Driver` trait directly picks its
/// own constructor (`linux::LinuxDriver::new`, `android::AndroidDriver::new`,
/// `windows::WindowsDriver::new`, or `mock::MockDriver::new`).
#[cfg(any(target_os = "linux", target_os = "android", target_os = "windows"))]
pub async fn platform() -> Result<std::sync::Arc<dyn Driver>> {
    #[cfg(target_os = "linux")]
    {
        Ok(std::sync::Arc::new(linux::LinuxDriver::new().await?))
    }
    #[cfg(target_os = "android")]
    {
        Ok(std::sync::Arc::new(android::AndroidDriver::new().await?))
    }
    #[cfg(target_os = "windows")]
    {
        Ok(std::sync::Arc::new(windows::WindowsDriver::new().await?))
    }
}

#[cfg(test)]
mod android_scan_tests {

    use crate::entities::error::BleError;

    #[test]
    fn only_an_explicitly_busy_scan_is_adapter_busy() {
        assert!(BleError::android_scan_start_result(0).is_ok());
        assert!(matches!(
            BleError::android_scan_start_result(1),
            Err(BleError::AdapterBusy(_))
        ));
        assert!(matches!(
            BleError::android_scan_start_result(2),
            Err(BleError::AdapterUnavailable(_))
        ));
        for unknown in [-1, 3, i32::MAX] {
            assert!(matches!(
                BleError::android_scan_start_result(unknown),
                Err(BleError::Gatt(_))
            ));
        }
    }
}

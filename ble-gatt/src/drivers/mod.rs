//! One implementation of the `crate::hal` ports per platform, plus the
//! mock radio (layer: Frameworks & Drivers; see `docs/architecture.md`).

#[cfg(target_os = "android")]
pub mod android;

#[cfg(target_os = "linux")]
pub mod linux;

pub mod mock;

#[cfg(target_os = "windows")]
pub mod windows;

use crate::entities::error::Result;
use crate::hal::Backend;

/// Construct the backend for the platform this binary runs on. Used by
/// `PeerLink::new`; a consumer using the `Backend` trait directly picks its
/// own constructor (`linux::LinuxBackend::new`, `android::AndroidBackend::new`,
/// or `mock::MockBackend::new`).
#[cfg(any(target_os = "linux", target_os = "android"))]
pub async fn platform() -> Result<std::sync::Arc<dyn Backend>> {
    #[cfg(target_os = "linux")]
    {
        Ok(std::sync::Arc::new(linux::LinuxBackend::new().await?))
    }
    #[cfg(target_os = "android")]
    {
        Ok(std::sync::Arc::new(android::AndroidBackend::new().await?))
    }
}

//! BlueZ drops enqueue unregistration. Keep the observed release target across cancellation.
use std::{future::Future, sync::Mutex, time::Duration};

#[derive(Default)]
pub(super) struct AdvertisementRelease {
    pub(super) target: Mutex<Option<u8>>,
}

impl AdvertisementRelease {
    pub(super) async fn release<H, E, F, R>(
        &self,
        handle: &mut Option<H>,
        mut available: F,
        timeout: Duration,
        interval: Duration,
    ) -> Result<bool, E>
    where
        F: FnMut() -> R,
        R: Future<Output = Result<u8, E>>,
    {
        if handle.is_some() {
            let target = available().await?.saturating_add(1);
            *self.target.lock().unwrap() = Some(target);
            *handle = None;
        }
        self.wait(available, timeout, interval).await
    }

    pub(super) async fn wait<E, F, R>(
        &self,
        mut available: F,
        timeout: Duration,
        interval: Duration,
    ) -> Result<bool, E>
    where
        F: FnMut() -> R,
        R: Future<Output = Result<u8, E>>,
    {
        let target = *self.target.lock().unwrap();
        if let Some(target) = target {
            let result = tokio::time::timeout(timeout, async {
                loop {
                    if available().await? >= target {
                        return Ok(());
                    }
                    tokio::time::sleep(interval).await;
                }
            })
            .await;
            match result {
                Ok(Ok(())) => *self.target.lock().unwrap() = None,
                Ok(Err(error)) => return Err(error),
                Err(_) => return Ok(false),
            }
        }
        Ok(true)
    }
}

//! Own an issued native start until a confirmed status transfers ownership to the server.
use std::time::Duration;

pub(super) struct PendingStart<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for PendingStart<F> {
    fn drop(&mut self) {
        if let Some(stop) = self.0.take() {
            stop();
        }
    }
}

impl<F: FnOnce()> PendingStart<F> {
    pub(super) async fn confirm<E>(
        mut started: impl FnMut() -> Result<bool, E>,
        stop: F,
        timeout: Duration,
        interval: Duration,
    ) -> Result<bool, E> {
        let mut pending = Self(Some(stop));
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if started()? {
                pending.0.take();
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(interval).await;
        }
    }
}

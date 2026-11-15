//! Own an issued native start until a confirmed status transfers ownership to the server.
use std::time::Duration;

pub(super) async fn confirm<E>(
    mut started: impl FnMut() -> Result<bool, E>,
    stop: impl FnOnce(),
    timeout: Duration,
    interval: Duration,
) -> Result<bool, E> {
    struct Pending<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for Pending<F> {
        fn drop(&mut self) {
            if let Some(stop) = self.0.take() {
                stop();
            }
        }
    }
    let mut pending = Pending(Some(stop));
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

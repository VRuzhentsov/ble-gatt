//! A native outcome commits metadata even when the Rust caller cancelled its waiter.
use std::sync::Mutex;
use tokio::sync::oneshot;

pub(super) struct PendingAdvertisement<T> {
    pub(super) sender: oneshot::Sender<Result<(), i32>>,
    pub(super) service: Option<T>,
}

impl<T> PendingAdvertisement<T> {
    pub(super) fn complete(self, outcome: Result<(), i32>, committed: &Mutex<Option<T>>) {
        if outcome.is_ok() {
            if let Some(service) = self.service {
                *committed.lock().unwrap() = Some(service);
            }
        }
        let _ = self.sender.send(outcome);
    }

    pub(super) fn send(self, outcome: Result<(), i32>) -> Result<(), Result<(), i32>> {
        self.sender.send(outcome)
    }
}

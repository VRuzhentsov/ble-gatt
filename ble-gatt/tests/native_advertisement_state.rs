//! Executes the same release/start ownership helpers as native drivers, without a radio.
#[path = "../src/drivers/advertisement_outcome.rs"]
mod advertisement_outcome;
#[path = "../src/drivers/advertisement_release.rs"]
mod advertisement_release;

#[tokio::test]
async fn android_failed_update_keeps_committed_snapshot_for_enable() {
    let committed = Mutex::new(Some("A"));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    advertisement_outcome::PendingAdvertisement {
        sender,
        service: Some("B"),
    }
    .complete(Err(5), &committed);
    assert_eq!(receiver.await.unwrap(), Err(5));
    assert_eq!(*committed.lock().unwrap(), Some("A"));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    advertisement_outcome::PendingAdvertisement {
        sender,
        service: Some("B"),
    }
    .send(Err(-1))
    .unwrap();
    assert_eq!(receiver.await.unwrap(), Err(-1));
    assert_eq!(*committed.lock().unwrap(), Some("A"));
}

#[tokio::test]
async fn android_cancelled_update_commits_native_success_without_waiter() {
    let committed = Mutex::new(Some("A"));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    drop(receiver);
    advertisement_outcome::PendingAdvertisement {
        sender,
        service: Some("B"),
    }
    .complete(Ok(()), &committed);
    assert_eq!(*committed.lock().unwrap(), Some("B"));
}
#[path = "../src/drivers/windows/advertising_start.rs"]
mod advertising_start;

#[tokio::test]
async fn windows_cancelled_pending_start_is_stopped_before_metadata_commit() {
    use std::sync::atomic::AtomicBool;
    let pending_a = AtomicBool::new(true);
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        advertising_start::confirm(
            || Ok::<_, ()>(false),
            || pending_a.store(false, Ordering::SeqCst),
            Duration::from_secs(1),
            Duration::from_millis(2),
        ),
    )
    .await;
    assert!(result.is_err());
    assert!(
        !pending_a.load(Ordering::SeqCst),
        "cancelled native A must not survive a later B commit"
    );
}

#[tokio::test]
async fn windows_confirmed_start_transfers_ownership_but_errors_stop() {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    let stopped = AtomicBool::new(false);
    let polls = AtomicUsize::new(0);
    assert!(advertising_start::confirm(
        || Ok::<_, ()>(polls.fetch_add(1, Ordering::SeqCst) >= 2),
        || stopped.store(true, Ordering::SeqCst),
        Duration::from_secs(1),
        Duration::from_millis(2)
    )
    .await
    .unwrap());
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(advertising_start::confirm(
        || Err::<bool, _>(()),
        || stopped.store(true, Ordering::SeqCst),
        Duration::from_secs(1),
        Duration::from_millis(2)
    )
    .await
    .is_err());
    assert!(stopped.load(Ordering::SeqCst));
}

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

struct DelayedRegistration(Arc<AtomicU8>);
impl Drop for DelayedRegistration {
    fn drop(&mut self) {
        let available = self.0.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            available.store(1, Ordering::SeqCst);
        });
    }
}

#[tokio::test]
async fn bluez_hide_waits_for_delayed_one_slot_unregister() {
    let available = Arc::new(AtomicU8::new(0));
    let mut handle = Some(DelayedRegistration(available.clone()));
    let pending = Mutex::new(None);
    let released = advertisement_release::release(
        &mut handle,
        &pending,
        || std::future::ready(Ok::<_, ()>(available.load(Ordering::SeqCst))),
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .await
    .unwrap();
    assert!(released);
    assert_eq!(
        available.load(Ordering::SeqCst),
        1,
        "hide must confirm native capacity release before show"
    );
    assert_eq!(*pending.lock().unwrap(), None);
}

#[tokio::test]
async fn bluez_cancelled_hide_keeps_release_barrier_for_show() {
    let available = Arc::new(AtomicU8::new(0));
    let mut handle = Some(DelayedRegistration(available.clone()));
    let pending = Mutex::new(None);
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        advertisement_release::release(
            &mut handle,
            &pending,
            || std::future::ready(Ok::<_, ()>(available.load(Ordering::SeqCst))),
            Duration::from_secs(1),
            Duration::from_millis(5),
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "hide must remain pending until unregister completes"
    );
    assert!(handle.is_none());
    assert_eq!(*pending.lock().unwrap(), Some(1));
    assert!(advertisement_release::wait(
        &pending,
        || std::future::ready(Ok::<_, ()>(available.load(Ordering::SeqCst))),
        Duration::from_secs(1),
        Duration::from_millis(5)
    )
    .await
    .unwrap());
    assert_eq!(available.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bluez_release_timeout_does_not_confirm_hide() {
    let mut handle = Some(());
    let pending = Mutex::new(None);
    assert!(!advertisement_release::release(
        &mut handle,
        &pending,
        || std::future::ready(Ok::<_, ()>(0)),
        Duration::from_millis(10),
        Duration::from_millis(2)
    )
    .await
    .unwrap());
    assert_eq!(*pending.lock().unwrap(), Some(1));
}

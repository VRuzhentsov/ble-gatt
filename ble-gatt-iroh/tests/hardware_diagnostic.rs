#[path = "../examples/support/iroh_diagnostic.rs"]
mod diagnostic;

use ble_gatt::drivers::mock::{MockDriver, MockNetwork};
use ble_gatt::{Adapter, CapabilityReport, PeerAddress};
use diagnostic::{run, Options};
use iroh::SecretKey;
use std::{sync::Arc, time::Duration};

async fn pair(crossed: bool, reverse: bool) {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();
    let network = MockNetwork::new();
    let mut a = SecretKey::generate();
    let mut b = SecretKey::generate();
    // The selected unilateral initiator is always the larger key.
    if (!reverse && a.public() < b.public()) || (reverse && b.public() < a.public()) {
        std::mem::swap(&mut a, &mut b);
    }
    let aid = a.public();
    let bid = b.public();
    let make = |name: &str| {
        Arc::new(MockDriver::new(
            PeerAddress(name.into()),
            network.clone(),
            CapabilityReport {
                central: true,
                peripheral: true,
            },
        ))
    };
    let aa = Adapter::new(make("diagnostic-a")).await;
    let ba = Adapter::new(make("diagnostic-b")).await;
    let opts = |key, peer, initiate| Options {
        key,
        peer,
        initiate,
        expect_incoming: crossed || !initiate,
        start_at: if crossed {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 2
        } else {
            0
        },
        timeout: Duration::from_secs(30),
    };
    let (ar, br) = tokio::join!(
        run(aa, opts(a, bid, crossed || !reverse)),
        run(ba, opts(b, aid, crossed || reverse))
    );
    assert!(ar.is_ok(), "A: {ar:?}");
    assert!(br.is_ok(), "B: {br:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn linux_initiates_payload_reuse_and_cleanup() {
    pair(false, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn phone_initiates_payload_reuse_and_cleanup() {
    pair(false, true).await
}

#[tokio::test(flavor = "multi_thread")]
async fn crossed_start_payload_reuse_and_cleanup() {
    pair(true, false).await
}

#[test]
fn modes_are_explicit_and_complementary() {
    assert_eq!(
        diagnostic::mode("phone", "phone-initiate").unwrap(),
        (true, false)
    );
    assert_eq!(
        diagnostic::mode("linux", "phone-initiate").unwrap(),
        (false, true)
    );
    assert_eq!(
        diagnostic::mode("linux", "linux-initiate").unwrap(),
        (true, false)
    );
    assert_eq!(
        diagnostic::mode("phone", "linux-initiate").unwrap(),
        (false, true)
    );
    assert_eq!(diagnostic::mode("phone", "both").unwrap(), (true, true));
    assert!(diagnostic::mode("unknown", "both").is_err());
    assert!(diagnostic::mode("linux", "unknown").is_err());
}

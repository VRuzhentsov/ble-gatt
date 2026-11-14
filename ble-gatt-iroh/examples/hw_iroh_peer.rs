//! See docs/either-peer-hardware.md. No phone or radio is touched by --keygen.
#[path = "support/iroh_diagnostic.rs"]
mod diagnostic;
use diagnostic::{mark, Options};
use std::{process::ExitCode, time::Duration};

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info,ble_gatt=debug,ble_gatt_iroh=debug"),
    )
    .format_timestamp_millis()
    .init();
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--keygen"] {
        let key = iroh::SecretKey::generate();
        let hex: String = key.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
        println!("BLE_DIAG_SECRET={hex}\nBLE_DIAG_PUBLIC={}", key.public());
        return ExitCode::SUCCESS;
    }
    if !args.is_empty() {
        eprintln!("usage: hw_iroh_peer [--keygen]; otherwise set BLE_DIAG_SECRET, BLE_DIAG_PEER, BLE_DIAG_SIDE=linux and BLE_DIAG_MODE=phone-initiate|linux-initiate|both (optional BLE_DIAG_START unix seconds)");
        return ExitCode::FAILURE;
    }
    let result = async {
        let read = |name| std::env::var(name).map_err(|_| format!("missing {name}"));
        let key = read("BLE_DIAG_SECRET")?
            .parse()
            .map_err(|_| "invalid diagnostic secret")?;
        let peer = read("BLE_DIAG_PEER")?
            .parse()
            .map_err(|_| "invalid peer public key")?;
        let (initiate, expect_incoming) =
            diagnostic::mode(&read("BLE_DIAG_SIDE")?, &read("BLE_DIAG_MODE")?)?;
        let start_at = std::env::var("BLE_DIAG_START")
            .unwrap_or_else(|_| "0".into())
            .parse()
            .map_err(|_| "invalid BLE_DIAG_START")?;
        let adapter = ble_gatt::Adapter::platform()
            .await
            .map_err(|e| e.to_string())?;
        diagnostic::run(
            adapter,
            Options {
                key,
                peer,
                initiate,
                expect_incoming,
                start_at,
                timeout: Duration::from_secs(180),
            },
        )
        .await
    }
    .await;
    match result {
        Ok(()) => {
            mark("PASS payload-reuse-survivor-cleanup");
            ExitCode::SUCCESS
        }
        Err(e) => {
            mark(format!("FAIL {e}"));
            ExitCode::FAILURE
        }
    }
}

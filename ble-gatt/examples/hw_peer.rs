//! Hardware verification peer — one half of a two-device BLE round trip.
//!
//! Everything else in this repository is verified against a mock. That mock
//! has repeatedly certified behaviour both real backends reject, so nothing
//! here is confirmed until two radios exchange bytes. This binary is the
//! Linux half of that: run it as the peripheral on one machine and as the
//! central on the other (or against the Android example app), and it proves
//! or disproves a datagram round trip with an exit code.
//!
//! ```text
//! # peripheral (advertises, waits for a central, echoes what it receives)
//! cargo run --example hw_peer -- --role peripheral
//!
//! # central (scans, connects, sends, expects the echo back)
//! cargo run --example hw_peer -- --role central
//! ```
//!
//! Both sides print `HWVERIFY:` lines. A driver script asserts on those and
//! on the exit status, so the result is machine-checkable rather than a
//! human squinting at a log.
//!
//! The UUIDs are fixed constants rather than generated, because the two
//! sides are separate processes on separate machines and must agree without
//! a channel to negotiate over.

use std::process::ExitCode;
use std::time::Duration;

use ble_gatt::backend::Backend;
use ble_gatt::datagram::{self, DatagramConfig};
use ble_gatt::{CharacteristicUuid, PeerAddress, ServiceUuid};
use tokio_stream::StreamExt;
use uuid::Uuid;

/// Shared by both halves. Fixed so the two processes agree without
/// negotiating.
const SERVICE: Uuid = Uuid::from_u128(0x0000_b1e6_0000_1000_8000_0080_5f9b_34fb);
const CHARACTERISTIC: Uuid = Uuid::from_u128(0x0000_b1e7_0000_1000_8000_0080_5f9b_34fb);

/// How long the central waits to discover the peripheral. Generous: a real
/// scan depends on the peer's advertising interval.
const SCAN_TIMEOUT: Duration = Duration::from_secs(30);
/// How long either side waits for the exchange once connected.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);

fn config() -> DatagramConfig {
    DatagramConfig::new(ServiceUuid(SERVICE), CharacteristicUuid(CHARACTERISTIC))
}

/// Deliberately larger than one fragment at any plausible MTU, so a passing
/// run proves fragmentation and reassembly over the air rather than just a
/// single-write happy path.
fn probe_payload() -> Vec<u8> {
    (0..512u16).map(|i| (i % 251) as u8).collect()
}

fn pass(what: &str) {
    println!("HWVERIFY: PASS {what}");
}

fn fail(what: &str) {
    println!("HWVERIFY: FAIL {what}");
}

#[tokio::main]
async fn main() -> ExitCode {
    // The library logs through the `log` facade, which discards everything
    // unless a logger is installed. Defaulting to `debug` for `ble_gatt`
    // means a plain run shows the whole pipeline without the caller having
    // to know the target name; `RUST_LOG` still overrides it.
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("ble_gatt=debug"),
    )
    .format_timestamp_millis()
    .init();

    let mut role = None;
    let mut target: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--role" => role = args.next(),
            "--peer" => target = args.next(),
            other => {
                eprintln!("unknown argument {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    let result = match role.as_deref() {
        Some("peripheral") => run_peripheral().await,
        Some("central") => run_central(target).await,
        _ => {
            eprintln!("usage: hw_peer --role <peripheral|central> [--peer <address>]");
            return ExitCode::FAILURE;
        }
    };

    match result {
        Ok(()) => {
            pass("round-trip complete");
            ExitCode::SUCCESS
        }
        Err(err) => {
            fail(&err);
            ExitCode::FAILURE
        }
    }
}

async fn backend() -> Result<std::sync::Arc<dyn Backend>, String> {
    #[cfg(target_os = "linux")]
    {
        let backend = ble_gatt::backend::linux::LinuxBackend::new()
            .await
            .map_err(|err| format!("no usable BlueZ adapter: {err}"))?;
        Ok(std::sync::Arc::new(backend))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("this harness only builds a real backend on Linux".to_string())
    }
}

/// Advertise, accept one central, and echo every message back.
///
/// Echoing rather than just receiving is deliberate: it exercises the
/// peripheral's notify path, which is where most of this library's
/// review findings landed and which no mock test can confirm.
async fn run_peripheral() -> Result<(), String> {
    let backend = backend().await?;
    let caps = backend.capabilities().await;
    println!("HWVERIFY: INFO capabilities central={} peripheral={}", caps.central, caps.peripheral);
    if !caps.peripheral {
        return Err("adapter reports no peripheral support".to_string());
    }

    let config = config();
    let mut incoming = datagram::serve(backend, &config)
        .await
        .map_err(|err| format!("serve failed: {err}"))?;
    println!("HWVERIFY: INFO advertising service {SERVICE}");
    println!("HWVERIFY: READY peripheral");

    let mut channel = tokio::time::timeout(SCAN_TIMEOUT, incoming.next())
        .await
        .map_err(|_| "no central connected within the timeout".to_string())?
        .ok_or_else(|| "serve stream closed before a central arrived".to_string())?;
    println!("HWVERIFY: INFO central connected: {}", channel.peer().0);

    let received = tokio::time::timeout(EXCHANGE_TIMEOUT, channel.recv())
        .await
        .map_err(|_| "no message received within the timeout".to_string())?
        .ok_or_else(|| "channel closed before a message arrived".to_string())?
        .map_err(|err| format!("inbound error: {err}"))?;
    println!("HWVERIFY: INFO received {} bytes", received.len());

    channel
        .send(received)
        .await
        .map_err(|err| format!("echo failed: {err}"))?;
    println!("HWVERIFY: INFO echoed");
    Ok(())
}

/// Scan, connect, send the probe, and require it back byte-for-byte.
async fn run_central(target: Option<String>) -> Result<(), String> {
    let backend = backend().await?;
    let caps = backend.capabilities().await;
    println!("HWVERIFY: INFO capabilities central={} peripheral={}", caps.central, caps.peripheral);

    let peer = match target {
        Some(address) => PeerAddress(address),
        None => {
            println!("HWVERIFY: INFO scanning for {SERVICE}");
            let mut found = backend
                .scan(ServiceUuid(SERVICE))
                .await
                .map_err(|err| format!("scan failed to start: {err}"))?;
            let discovered = tokio::time::timeout(SCAN_TIMEOUT, found.next())
                .await
                .map_err(|_| "no peer advertising the service within the timeout".to_string())?
                .ok_or_else(|| "scan stream ended without a result".to_string())?
                .map_err(|err| format!("scan failed: {err}"))?;
            println!(
                "HWVERIFY: INFO discovered {} rssi={:?} services={}",
                discovered.address.0,
                discovered.rssi,
                discovered.services.len()
            );
            discovered.address
        }
    };

    let config = config();
    let mut channel = datagram::connect(backend, &peer, &config)
        .await
        .map_err(|err| format!("connect failed: {err}"))?;
    println!(
        "HWVERIFY: INFO connected to {} max_message_len={}",
        peer.0,
        channel.max_message_len()
    );
    println!("HWVERIFY: READY central");

    let payload = probe_payload();
    channel
        .send(payload.clone())
        .await
        .map_err(|err| format!("send failed: {err}"))?;
    println!("HWVERIFY: INFO sent {} bytes", payload.len());

    let echoed = tokio::time::timeout(EXCHANGE_TIMEOUT, channel.recv())
        .await
        .map_err(|_| "no echo received within the timeout".to_string())?
        .ok_or_else(|| "channel closed before the echo arrived".to_string())?
        .map_err(|err| format!("inbound error: {err}"))?;

    if echoed != payload {
        return Err(format!(
            "echo mismatch: sent {} bytes, got {} bytes",
            payload.len(),
            echoed.len()
        ));
    }
    println!("HWVERIFY: INFO echo matched byte-for-byte");
    Ok(())
}

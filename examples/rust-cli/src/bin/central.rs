use ble_gatt::Adapter;
use ble_gatt_cli_example::{central, Result};
use std::time::Duration;
use tokio::time::timeout;

const HELP: &str = "Usage: central [TEXT]
Scan for the demo service. Read, write TEXT, and receive its notification.
TEXT defaults to hello. Use 1 to 20 UTF-8 bytes.
This command uses Bluetooth. Run only with your demo peripheral nearby.
Each stage has a 15-second time limit. --help does not open Bluetooth.";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    // Handle help and bad input before native driver setup.
    if args == ["--help"] || args == ["-h"] {
        println!("{HELP}");
        return Ok(());
    }
    if args.len() > 1 || args.first().is_some_and(|s| s.starts_with('-')) {
        return Err(HELP.into());
    }
    let value = args
        .first()
        .map(String::as_bytes)
        .unwrap_or(b"hello")
        .to_vec();
    if value.is_empty() || value.len() > 20 {
        return Err("Use 1 to 20 bytes of text. See --help.".into());
    }
    let wait = Duration::from_secs(15);
    let adapter = timeout(wait, Adapter::platform())
        .await
        .map_err(|_| "Open adapter timed out.")??;
    let result = central(&adapter, value, wait).await;
    timeout(wait, adapter.close())
        .await
        .map_err(|_| "Close adapter timed out.")??;
    let (initial, reply) = result?;
    println!(
        "Read: {:?}\nNotification: {:?}\nDisconnected.",
        initial, reply
    );
    Ok(())
}

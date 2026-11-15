use ble_gatt::Adapter;
use ble_gatt_cli_example::{peripheral, Result};
use std::time::Duration;
use tokio::time::timeout;

const HELP: &str = "Usage: peripheral
Serve the demo GATT service. Echo one write as a notification.
Wait up to 60 seconds for a write and the central to disconnect, then stop.
This command uses Bluetooth. Your adapter must support the peripheral role.
--help does not open Bluetooth.";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    // Help must not construct a native driver or open the radio.
    if args == ["--help"] || args == ["-h"] {
        println!("{HELP}");
        return Ok(());
    }
    if !args.is_empty() {
        return Err(HELP.into());
    }
    let wait = Duration::from_secs(60);
    let adapter = timeout(wait, Adapter::platform())
        .await
        .map_err(|_| "Open adapter timed out.")??;
    let result = peripheral(&adapter, wait).await;
    timeout(wait, adapter.close())
        .await
        .map_err(|_| "Close adapter timed out.")??;
    println!("Echoed: {:?}\nServer stopped.", result?);
    Ok(())
}

# Raw GATT in a Rust CLI

This standalone Cargo project uses `ble-gatt` directly. It does not use Tauri,
iroh, `PeerLink`, or the datagram profile. It follows the crate's existing
role and handle pattern in `ble-gatt/tests/roles.rs`.

- `central` scans for the demo service and connects to the first result. It
  reads `ready`, subscribes, writes text, and checks the notification. It waits
  for disconnect to finish.
- `peripheral` serves one readable, writable, and notifiable characteristic.
  It prints the first write and echoes its bytes to that central. It keeps its
  `ServerHandle` until the central disconnects, then calls `stop()`.
- Both commands call `Adapter::close()` after the role function returns, even
  if that function fails. Time limits also cover setup and cleanup calls.

The two UUIDs in `src/lib.rs` are private demo IDs. They are not product IDs.
There is no device address, pairing, authentication, or encryption in this
example. Keep only your test peripheral nearby: the central selects the first
peer that advertises the service. Use short, non-secret text.

## Build and test without a radio

Run these commands from the repository root. The lock file belongs to this
example; it does not change the root workspace.

```sh
cargo build --manifest-path examples/rust-cli/Cargo.toml --locked --offline --bins
cargo test --manifest-path examples/rust-cli/Cargo.toml --locked --offline
cargo run --manifest-path examples/rust-cli/Cargo.toml --locked --offline --bin central -- --help
cargo run --manifest-path examples/rust-cli/Cargo.toml --locked --offline --bin peripheral -- --help
```

Offline builds need the locked crates in your Cargo cache. These commands do
not scan or open the Bluetooth adapter. The help path returns before native
driver setup. Tests call the same role functions with `MockNetwork` and
`MockDriver`. They check the read, write, notification, timeout, disconnect,
and server stop paths. The help test also starts both binaries with an invalid
Linux system-bus address.

## Run on your own hardware

**The following commands use Bluetooth. Run them only on devices you control.**
Use two devices with supported native drivers, such as Linux with BlueZ or
Windows. The peripheral device must support the peripheral role. Enable
Bluetooth and grant the access your OS requires. An Android host also needs
the crate's native bridge and Application context; this CLI does not set them
up. This example does not add a macOS driver.

On the peripheral device, run:

```sh
cargo run --manifest-path examples/rust-cli/Cargo.toml --locked --offline --bin peripheral
```

Within 60 seconds, run this on the central device:

```sh
cargo run --manifest-path examples/rust-cli/Cargo.toml --locked --offline --bin central -- hello
```

The central accepts one optional text argument. Its default is `hello`. Use
1 to 20 UTF-8 bytes so the write and notification fit the minimum ATT MTU.
The central allows 15 seconds per stage. The peripheral allows 60 seconds for
the write and disconnect together. It also bounds server setup and stop.
These are demo time limits, not a retry system. On error, read the message,
check Bluetooth access and role support, and run both commands again.

No hardware verification was performed for this example. Mock tests prove the
Rust call path and byte exchange. They do not prove native radio behavior,
permissions, notification delivery, or physical OS cleanup.

use std::process::Command;

#[test]
fn help_works_without_a_native_adapter() {
    for (binary, role) in [
        (env!("CARGO_BIN_EXE_central"), "central"),
        (env!("CARGO_BIN_EXE_peripheral"), "peripheral"),
    ] {
        let output = Command::new(binary)
            .arg("--help")
            // On Linux any attempt to open the system bus must fail.
            .env("DBUS_SYSTEM_BUS_ADDRESS", "unix:path=/no-such-ble-demo-bus")
            .output()
            .expect("start binary");
        assert!(output.status.success(), "{role}: {:?}", output);
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains(&format!("Usage: {role}")), "{text}");
        assert!(text.contains("Bluetooth"), "{text}");
        assert!(output.stderr.is_empty());
    }
}

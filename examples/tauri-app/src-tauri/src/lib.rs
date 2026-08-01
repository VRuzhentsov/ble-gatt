mod hw_verify;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
  tauri::Builder::default()
    .plugin(tauri_plugin_ble_gatt::init())
    .setup(|app| {
      // Log before anything else, so the harness markers below reach
      // logcat. `Info` is enough: every `HWVERIFY:` line is logged at it.
      app.handle().plugin(
        tauri_plugin_log::Builder::default()
          .level(log::LevelFilter::Info)
          .build(),
      )?;

      // Runs only when `debug.blegatt.role` is set, so an ordinary launch
      // of the example is unaffected.
      use tauri::Manager;
      let state = app.state::<tauri_plugin_ble_gatt::commands::PluginState>();
      hw_verify::spawn_if_configured(state.backend());
      Ok(())
    })
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}

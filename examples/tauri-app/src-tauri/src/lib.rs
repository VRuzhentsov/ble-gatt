mod hw_verify;
mod iroh_verify;

#[tauri::command]
async fn request_ble_permissions(app: tauri::AppHandle) -> Result<String, String> {
    use tauri_plugin_ble_gatt::BleGattExt;
    tauri::async_runtime::spawn_blocking(move || {
        app.ble_gatt()
            .request_permissions()
            .map(|state| format!("{state:?}"))
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())?
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![request_ble_permissions])
        .plugin(tauri_plugin_ble_gatt::init())
        .setup(|app| {
            // Log before anything else, so the harness markers below reach
            // logcat. Debug also retains native candidate/session evidence.
            app.handle().plugin(
                tauri_plugin_log::Builder::default()
                    .level(log::LevelFilter::Debug)
                    .build(),
            )?;

            // The debug iroh mode takes precedence over the older datagram
            // role property; otherwise retain the existing example path.
            if !iroh_verify::spawn_if_configured(app.handle().clone()) {
                hw_verify::spawn_if_configured(app.handle().clone());
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

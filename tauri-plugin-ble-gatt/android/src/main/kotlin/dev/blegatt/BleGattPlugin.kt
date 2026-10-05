package dev.blegatt

import android.Manifest
import android.app.Activity
import android.os.Build
import app.tauri.PermissionState
import app.tauri.annotation.Command
import app.tauri.annotation.Permission
import app.tauri.annotation.PermissionCallback
import app.tauri.annotation.TauriPlugin
import app.tauri.plugin.Invoke
import app.tauri.plugin.JSObject
import app.tauri.plugin.Plugin

/** Runtime permissions BLE needs on API 31+. */
private const val ALIAS_BLUETOOTH = "bluetooth"

/** Before API 31, scan results counted as location data. */
private const val ALIAS_LEGACY_LOCATION = "bluetoothLegacy"

/**
 * The Tauri side of the plugin: Bluetooth runtime permissions, through
 * Tauri's own permission mechanism (the same one `tauri-plugin-notification`
 * uses). GATT itself goes through [BleGattBridge] over JNI, not through here.
 *
 * Both permission sets are declared, but only the one for the running API
 * level is checked or requested, and the result is reported under one key,
 * `bluetooth`: on API 30 and below `BLUETOOTH_SCAN` does not exist and would
 * always read as denied.
 */
@TauriPlugin(
    permissions = [
        Permission(
            strings = [
                Manifest.permission.BLUETOOTH_SCAN,
                Manifest.permission.BLUETOOTH_CONNECT,
                Manifest.permission.BLUETOOTH_ADVERTISE,
            ],
            alias = ALIAS_BLUETOOTH,
        ),
        Permission(strings = [Manifest.permission.ACCESS_FINE_LOCATION], alias = ALIAS_LEGACY_LOCATION),
    ]
)
class BleGattPlugin(activity: Activity) : Plugin(activity) {
    private fun requiredAlias(): String =
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) ALIAS_BLUETOOTH else ALIAS_LEGACY_LOCATION

    private fun resolveState(invoke: Invoke) {
        val state = getPermissionState(requiredAlias()) ?: PermissionState.DENIED
        val result = JSObject()
        result.put("bluetooth", state.toString())
        invoke.resolve(result)
    }

    @Command
    override fun checkPermissions(invoke: Invoke) {
        resolveState(invoke)
    }

    @Command
    override fun requestPermissions(invoke: Invoke) {
        if (getPermissionState(requiredAlias()) == PermissionState.GRANTED) {
            resolveState(invoke)
        } else {
            requestPermissionForAlias(requiredAlias(), invoke, "permissionsCallback")
        }
    }

    @PermissionCallback
    private fun permissionsCallback(invoke: Invoke) {
        resolveState(invoke)
    }
}

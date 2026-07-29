package dev.blegatt

import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattServer
import android.bluetooth.BluetoothGattServerCallback
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.bluetooth.le.AdvertiseCallback
import android.bluetooth.le.AdvertiseData
import android.bluetooth.le.AdvertiseSettings
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.content.pm.PackageManager
import android.os.ParcelUuid
import android.util.Log
import java.util.UUID

private const val TAG = "BleGattBridge"

/**
 * Real Android BLE GATT central + peripheral implementation. Called from
 * Rust via raw JNI (`env.call_method(...)`) — not through Tauri's mobile
 * plugin `Invoke`/`@Command` machinery, so this class has no `Plugin`
 * base class or `@TauriPlugin` annotation. That is deliberate: it is what
 * keeps a hypothetical non-Tauri Android Rust consumer able to reuse this
 * file (copy it + the manifest permissions into their own app, no Tauri
 * Android runtime required) rather than being structurally locked into
 * Tauri's IPC path. See docs/adr/0002 for the full rationale.
 *
 * Permission checks are deliberately not performed here — declaring
 * `<uses-permission>` in this module's AndroidManifest.xml is this
 * bridge's responsibility; requesting the *runtime* grant from the user is
 * the consuming app's. An operation attempted without a granted permission
 * throws `SecurityException`, which Rust surfaces as a `BleError::Gatt`.
 */
class BleGattBridge(private val context: Context, private val nativeHandle: Long) {

    private val bluetoothManager =
        context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
    private val adapter: BluetoothAdapter? = bluetoothManager.adapter

    init {
        try {
            val hasBleFeature = context.packageManager.hasSystemFeature(PackageManager.FEATURE_BLUETOOTH_LE)
            Log.d(
                TAG,
                "init: FEATURE_BLUETOOTH_LE=$hasBleFeature adapter=${adapter != null} " +
                    "adapter.isEnabled=${adapter?.isEnabled} " +
                    "bluetoothLeScanner=${adapter?.bluetoothLeScanner != null} " +
                    "isMultipleAdvertisementSupported=${adapter?.isMultipleAdvertisementSupported} " +
                    "bluetoothLeAdvertiser=${adapter?.bluetoothLeAdvertiser != null}"
            )
        } catch (e: Exception) {
            Log.e(TAG, "init: capability probe threw", e)
        }
    }

    // --- Central role state ---
    private var scanCallback: ScanCallback? = null
    private val connectedGatts = HashMap<String, BluetoothGatt>()
    private val pendingCharacteristics = HashMap<String, HashMap<String, BluetoothGattCharacteristic>>()

    // --- Peripheral role state ---
    private var gattServer: BluetoothGattServer? = null
    private var advertiseCallback: AdvertiseCallback? = null
    private val serverCharacteristics = HashMap<String, BluetoothGattCharacteristic>()
    private val subscribedDevices = HashMap<String, MutableSet<BluetoothDevice>>()

    fun hasCentralSupport(): Boolean = adapter?.bluetoothLeScanner != null

    fun hasPeripheralSupport(): Boolean =
        adapter?.isMultipleAdvertisementSupported == true && adapter.bluetoothLeAdvertiser != null

    // ---------------------------------------------------------------
    // Central role
    // ---------------------------------------------------------------

    fun startScan(serviceUuid: String) {
        val scanner = adapter?.bluetoothLeScanner ?: return
        val target = UUID.fromString(serviceUuid)
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()
        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, result: ScanResult) {
                val uuids = result.scanRecord?.serviceUuids
                Log.d(TAG, "onScanResult: ${result.device.address} name=${result.device.name} advertisedUuids=$uuids")
                if (uuids != null && uuids.contains(ParcelUuid(target))) {
                    onPeerDiscovered(nativeHandle, result.device.address, result.device.name)
                }
            }

            override fun onScanFailed(errorCode: Int) {
                Log.e(TAG, "onScanFailed: errorCode=$errorCode")
            }
        }
        Log.d(TAG, "startScan: serviceUuid=$serviceUuid scanner=${scanner != null}")
        scanCallback = callback
        // Matching against the raw advertised UUID list in onScanResult
        // above, rather than a native android.bluetooth.le.ScanFilter — a
        // filtered scan against a Root Canal virtual controller (Android
        // emulator's Bluetooth simulator) delivered zero results in testing
        // even when the target peer was confirmed advertising and the
        // filter parameters were accepted (status=0); an unfiltered scan
        // with matching done here works identically on real adapters and
        // sidesteps whatever that native-filter gap is. See docs/adr/0002.
        scanner.startScan(emptyList(), settings, callback)
    }

    fun stopScan() {
        val scanner = adapter?.bluetoothLeScanner ?: return
        scanCallback?.let { scanner.stopScan(it) }
        scanCallback = null
    }

    fun connect(address: String) {
        val device = adapter?.getRemoteDevice(address) ?: return
        val callback = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                when (newState) {
                    BluetoothProfile.STATE_CONNECTED -> gatt.discoverServices()
                    BluetoothProfile.STATE_DISCONNECTED -> {
                        connectedGatts.remove(address)
                        pendingCharacteristics.remove(address)
                        onDisconnected(nativeHandle, address)
                        gatt.close()
                    }
                }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                val byUuid = HashMap<String, BluetoothGattCharacteristic>()
                for (service in gatt.services) {
                    for (characteristic in service.characteristics) {
                        byUuid[characteristic.uuid.toString()] = characteristic
                    }
                }
                pendingCharacteristics[address] = byUuid
                onConnected(nativeHandle, address)
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicRead(
                gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int
            ) {
                onCharacteristicRead(
                    nativeHandle, address, characteristic.uuid.toString(),
                    characteristic.value ?: ByteArray(0), status == BluetoothGatt.GATT_SUCCESS
                )
            }

            override fun onCharacteristicWrite(
                gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int
            ) {
                onCharacteristicWriteResult(
                    nativeHandle, address, characteristic.uuid.toString(), status == BluetoothGatt.GATT_SUCCESS
                )
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicChanged(
                gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic
            ) {
                onCharacteristicChanged(
                    nativeHandle, address, characteristic.uuid.toString(), characteristic.value ?: ByteArray(0)
                )
            }

            override fun onDescriptorWrite(
                gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int
            ) {
                if (status == BluetoothGatt.GATT_SUCCESS) {
                    onSubscribed(nativeHandle, address, descriptor.characteristic.uuid.toString())
                }
            }
        }
        val gatt = device.connectGatt(context, false, callback)
        connectedGatts[address] = gatt
    }

    fun disconnect(address: String) {
        connectedGatts[address]?.disconnect()
    }

    private fun findCharacteristic(address: String, characteristicUuid: String): BluetoothGattCharacteristic? =
        pendingCharacteristics[address]?.get(characteristicUuid)

    fun readCharacteristic(address: String, characteristicUuid: String) {
        val gatt = connectedGatts[address] ?: return
        val characteristic = findCharacteristic(address, characteristicUuid) ?: return
        gatt.readCharacteristic(characteristic)
    }

    @Suppress("DEPRECATION")
    fun writeCharacteristic(address: String, characteristicUuid: String, value: ByteArray) {
        val gatt = connectedGatts[address] ?: return
        val characteristic = findCharacteristic(address, characteristicUuid) ?: return
        characteristic.value = value
        gatt.writeCharacteristic(characteristic)
    }

    fun subscribeCharacteristic(address: String, characteristicUuid: String) {
        val gatt = connectedGatts[address] ?: return
        val characteristic = findCharacteristic(address, characteristicUuid) ?: return
        gatt.setCharacteristicNotification(characteristic, true)
        val cccd = characteristic.getDescriptor(CLIENT_CHARACTERISTIC_CONFIG_UUID) ?: return
        @Suppress("DEPRECATION")
        cccd.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
        @Suppress("DEPRECATION")
        gatt.writeDescriptor(cccd)
    }

    // ---------------------------------------------------------------
    // Peripheral role
    // ---------------------------------------------------------------

    fun startAdvertising(
        serviceUuid: String, characteristicUuids: Array<String>, readable: BooleanArray, writable: BooleanArray,
        notifiable: BooleanArray, initialValues: Array<ByteArray>,
    ) {
        val advertiser = adapter?.bluetoothLeAdvertiser ?: return

        val serverCallback = object : BluetoothGattServerCallback() {
            override fun onCharacteristicReadRequest(
                device: BluetoothDevice, requestId: Int, offset: Int, characteristic: BluetoothGattCharacteristic
            ) {
                val value = characteristic.value ?: ByteArray(0)
                gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, offset, value)
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicWriteRequest(
                device: BluetoothDevice, requestId: Int, characteristic: BluetoothGattCharacteristic,
                preparedWrite: Boolean, responseNeeded: Boolean, offset: Int, value: ByteArray,
            ) {
                characteristic.value = value
                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, offset, value)
                }
                onServerCharacteristicWritten(nativeHandle, device.address, characteristic.uuid.toString(), value)
            }

            override fun onDescriptorWriteRequest(
                device: BluetoothDevice, requestId: Int, descriptor: BluetoothGattDescriptor,
                preparedWrite: Boolean, responseNeeded: Boolean, offset: Int, value: ByteArray,
            ) {
                val characteristicUuid = descriptor.characteristic.uuid.toString()
                if (value.contentEquals(BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE)) {
                    subscribedDevices.getOrPut(characteristicUuid) { mutableSetOf() }.add(device)
                } else {
                    subscribedDevices[characteristicUuid]?.remove(device)
                }
                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, offset, value)
                }
            }
        }
        val server = bluetoothManager.openGattServer(context, serverCallback)
        gattServer = server

        val service = BluetoothGattService(
            UUID.fromString(serviceUuid), BluetoothGattService.SERVICE_TYPE_PRIMARY
        )
        for (i in characteristicUuids.indices) {
            var properties = 0
            var permissions = 0
            if (readable[i]) {
                properties = properties or BluetoothGattCharacteristic.PROPERTY_READ
                permissions = permissions or BluetoothGattCharacteristic.PERMISSION_READ
            }
            if (writable[i]) {
                properties = properties or BluetoothGattCharacteristic.PROPERTY_WRITE
                permissions = permissions or BluetoothGattCharacteristic.PERMISSION_WRITE
            }
            if (notifiable[i]) {
                properties = properties or BluetoothGattCharacteristic.PROPERTY_NOTIFY
            }
            val characteristic = BluetoothGattCharacteristic(
                UUID.fromString(characteristicUuids[i]), properties, permissions
            )
            @Suppress("DEPRECATION")
            characteristic.value = initialValues[i]
            if (notifiable[i]) {
                val cccd = BluetoothGattDescriptor(
                    CLIENT_CHARACTERISTIC_CONFIG_UUID,
                    BluetoothGattDescriptor.PERMISSION_READ or BluetoothGattDescriptor.PERMISSION_WRITE
                )
                characteristic.addDescriptor(cccd)
            }
            service.addCharacteristic(characteristic)
            serverCharacteristics[characteristicUuids[i]] = characteristic
        }
        server.addService(service)

        val settings = AdvertiseSettings.Builder()
            .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_BALANCED)
            .setConnectable(true)
            .build()
        val data = AdvertiseData.Builder()
            .addServiceUuid(ParcelUuid(UUID.fromString(serviceUuid)))
            .setIncludeDeviceName(false)
            .build()
        val callback = object : AdvertiseCallback() {
            override fun onStartSuccess(settingsInEffect: AdvertiseSettings) {
                Log.d(TAG, "advertise onStartSuccess: serviceUuid=$serviceUuid")
            }

            override fun onStartFailure(errorCode: Int) {
                Log.e(TAG, "advertise onStartFailure: errorCode=$errorCode serviceUuid=$serviceUuid")
            }
        }
        advertiseCallback = callback
        Log.d(TAG, "startAdvertising: serviceUuid=$serviceUuid advertiser=${advertiser != null}")
        advertiser.startAdvertising(settings, data, callback)
    }

    fun stopAdvertising() {
        val advertiser = adapter?.bluetoothLeAdvertiser
        advertiseCallback?.let { advertiser?.stopAdvertising(it) }
        advertiseCallback = null
        gattServer?.close()
        gattServer = null
        serverCharacteristics.clear()
        subscribedDevices.clear()
    }

    fun notifyCharacteristic(characteristicUuid: String, value: ByteArray) {
        val characteristic = serverCharacteristics[characteristicUuid] ?: return
        @Suppress("DEPRECATION")
        characteristic.value = value
        val subscribers = subscribedDevices[characteristicUuid] ?: return
        for (device in subscribers) {
            @Suppress("DEPRECATION")
            gattServer?.notifyCharacteristicChanged(device, characteristic, false)
        }
    }

    companion object {
        private val CLIENT_CHARACTERISTIC_CONFIG_UUID: UUID =
            UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
    }
}

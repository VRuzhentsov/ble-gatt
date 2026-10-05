# Alternatives and prior art

An inventory of other BLE libraries that run on Android. It records what
already exists, what to borrow, and what this repo is for. ADR-0001 records
the narrower Rust-only survey behind the decision to write `ble-gatt`. This
file is wider: every language, plus apps that solve the same problem.

**Inclusion rules:**
- **Android is required.** Fini, the app this library is built for, ships on
  Android, so a library without an Android backend is no alternative.
  Notable libraries that fail this rule are named at the end, so nobody has to
  re-check them.
- **100+ GitHub stars.** A few projects below that are listed separately,
  because they compete directly.

**Snapshot:** star counts read from the GitHub API on 2026-10-04. They will
drift.

**Columns:**
- **Roles: C** = central (scan, connect, GATT client).
- **Roles: P** = peripheral (advertise, GATT server). For a multi-platform
  library, **P** means peripheral works on Android, not only on some other
  platform.
- **?** = not confirmed from the project's own docs. Check before relying on it.
- **Other platforms:** platforms supported besides Android. A dash (—) means
  Android only.

## Where `ble-gatt` sits

Of the 28 libraries and plugins below, 21 are central-only. They talk *to* a
device and are never a device themselves. Six can also be a peripheral on
Android (Golden Gate's phone-side role is unconfirmed):

- **Android only:** Nordic's Android-BLE-Library and Kotlin-BLE-Library,
  BLESSED, and kshoji's HID peripheral. Kotlin/Java, with no desktop
  counterpart.
- **Android plus other platforms:** Shiny (.NET) and
  cordova-plugin-bluetoothle. Both live inside a larger app framework.

No Rust library here does the peripheral role on Android, and none pairs it
with a desktop backend. The one Rust crate that does is AGPL-licensed (see
"Below 100 stars").

The closest match to `ble-gatt`'s goal is an app, not a library. bitchat
connects every device as both central and peripheral and runs its own
framing above GATT. That framing layer is what this repo's datagram and
peer-link tiers provide.

## Libraries

| Project | Language | Stars | Roles | Other platforms | Notes |
|---|---|---:|---|---|---|
| [Jasonchenlijian/FastBle](https://github.com/Jasonchenlijian/FastBle) | Java | 5505 | C | — | |
| [dariuszseweryn/RxAndroidBle](https://github.com/dariuszseweryn/RxAndroidBle) | Java | 3540 | C | — | RxJava; well known for documenting Android BLE pitfalls. |
| [dotintent/react-native-ble-plx](https://github.com/dotintent/react-native-ble-plx) | Java / JS | 3441 | C | iOS | React Native. |
| [hbldh/bleak](https://github.com/hbldh/bleak) | Python | 2536 | C | Windows, macOS, Linux | The standard Python BLE client. Android through python-for-android. |
| [pauldemarco/flutter_blue](https://github.com/pauldemarco/flutter_blue) | Dart | 2427 | C | iOS | Abandoned; succeeded by flutter_blue_plus. |
| [nordicsemi/Android-BLE-Library](https://github.com/nordicsemi/Android-BLE-Library) | Java | 2415 | C + P | — | Request queue and server manager. Worth reading for our Kotlin bridge. |
| [innoveit/react-native-ble-manager](https://github.com/innoveit/react-native-ble-manager) | Java / JS | 2333 | C | iOS | React Native. |
| [shinyorg/shiny](https://github.com/shinyorg/shiny) | C# | 1583 | C + P | iOS, macOS, Windows | .NET framework; BLE client plus "hosting" (peripheral). |
| [JuulLabs/kable](https://github.com/JuulLabs/kable) | Kotlin | 1197 | C | iOS, macOS, JavaScript (Web Bluetooth) | Kotlin Multiplatform; a good reference for API shape. |
| [deviceplug/btleplug](https://github.com/deviceplug/btleplug) | Rust | 1177 | C | Windows, macOS, iOS, Linux | Host-side by design; the de facto Rust BLE client. |
| [simpleble/simpleble](https://github.com/simpleble/simpleble) | C++ | 1137 | C | Windows, macOS, iOS, Linux | Python and Rust bindings. Check its licence before use. |
| [chipweinberger/flutter_blue_plus](https://github.com/chipweinberger/flutter_blue_plus) | Dart | 1005 | C | iOS, macOS, Web, Linux, Windows | |
| [inthehand/32feet](https://github.com/inthehand/32feet) | C# | 990 | C | Windows, iOS, macOS | .NET. |
| [dotnet-bluetooth-le/dotnet-bluetooth-le](https://github.com/dotnet-bluetooth-le/dotnet-bluetooth-le) | C# | 957 | C | iOS, macOS, Windows | Plugin.BLE for Xamarin/MAUI. |
| [don/cordova-plugin-ble-central](https://github.com/don/cordova-plugin-ble-central) | Java / JS | 953 | C | iOS | Cordova. |
| [randdusing/cordova-plugin-bluetoothle](https://github.com/randdusing/cordova-plugin-bluetoothle) | Obj-C / Java / JS | 804 | C + P | iOS | Cordova; has peripheral (server) calls. |
| [nordicsemi/Android-Scanner-Compat-Library](https://github.com/nordicsemi/Android-Scanner-Compat-Library) | Java | 796 | C (scan only) | — | |
| [PhilipsHue/flutter_reactive_ble](https://github.com/PhilipsHue/flutter_reactive_ble) | Dart | 737 | C | iOS | |
| [weliem/blessed-android](https://github.com/weliem/blessed-android) | Java | 595 | C + P | — | Has a peripheral manager. |
| [dotintent/FlutterBleLib](https://github.com/dotintent/FlutterBleLib) | Dart | 546 | C | iOS | Can simulate peripherals for tests; compare our `MockBackend`. |
| [nordicsemi/Kotlin-BLE-Library](https://github.com/nordicsemi/Kotlin-BLE-Library) | Kotlin | 529 | C + P | — | Coroutines successor to Android-BLE-Library. |
| [Reedyuk/blue-falcon](https://github.com/Reedyuk/blue-falcon) | Kotlin | 488 | C | iOS, macOS, Windows, JavaScript | Kotlin Multiplatform. |
| [Beepiz/BleGattCoroutines](https://github.com/Beepiz/BleGattCoroutines) | Kotlin | 476 | C | — | |
| [capacitor-community/bluetooth-le](https://github.com/capacitor-community/bluetooth-le) | TypeScript | 359 | C | iOS, Web | Capacitor. |
| [Google-Health-API/golden-gate](https://github.com/Google-Health-API/golden-gate) | C | 313 | C + P ? | iOS, embedded (nRF, ESP32 …) | **Archived.** IP stack (CoAP/DTLS) over BLE for wearables. On phones it acts as central; peripheral is the device side. Closest prior art to our datagram tier. |
| [kshoji/BLE-HID-Peripheral-for-Android](https://github.com/kshoji/BLE-HID-Peripheral-for-Android) | Java | 258 | P | — | HID profile only. |
| [haodynasty/AndroidBleManager](https://github.com/haodynasty/AndroidBleManager) | Java | 244 | C | — | |
| [MnlPhlp/tauri-plugin-blec](https://github.com/MnlPhlp/tauri-plugin-blec) | Rust | 228 | C | iOS, Windows, macOS, Linux | btleplug wrapped as a Tauri plugin. |

## Apps that message over BLE

These are not libraries, but each one solved the problem `ble-gatt`'s upper
tiers address: two ordinary devices talking to each other with no
infrastructure.

| Project | Language | Stars | Roles | Other platforms | Notes |
|---|---|---:|---|---|---|
| [permissionlesstech/bitchat-android](https://github.com/permissionlesstech/bitchat-android) | Kotlin | 7678 | C + P | iOS, macOS (via [the Swift app](https://github.com/permissionlesstech/bitchat), 36341★) | BLE mesh chat: fragmentation, relay and E2E crypto on top of GATT. The two apps are wire-compatible. |
| [berty/berty](https://github.com/berty/berty) | TypeScript / Go | 9311 | C + P ? | iOS, desktop | P2P messenger with a BLE proximity transport under libp2p. |
| [spieglt/FlyingCarpet](https://github.com/spieglt/FlyingCarpet) | Rust | 5330 | C + P ? | iOS, Linux, macOS, Windows | Tauri app; uses BLE to negotiate a Wi-Fi transfer. Prior art for Tauri + BLE on mobile. |
| [briar/briar](https://github.com/briar/briar) | Java | 699 | — | Desktop (separate app) | Offline messenger; uses classic Bluetooth (RFCOMM), not BLE GATT. |

## Below 100 stars but directly comparable

| Project | Language | Stars | Roles | Other platforms | Why it's here |
|---|---|---:|---|---|---|
| [juliansteenbakker/flutter_ble_peripheral](https://github.com/juliansteenbakker/flutter_ble_peripheral) | Dart | 91 | P (advertising only) | iOS | Flutter peripheral, but no GATT server. |
| [himelbrand/react-native-ble-peripheral](https://github.com/himelbrand/react-native-ble-peripheral) | Java / JS | 71 | P | — | React Native peripheral simulator. |
| `blew` / `tauri-plugin-blew` | Rust | — | C + P | ? | The one Rust crate covering both roles on Android, but AGPL-3.0. Ruled out in ADR-0001. |

## Excluded: no Android support

These have 100+ stars but fail the Android rule. They are still useful
reading:

- **Rust:**
  - `bluez/bluer` (Linux). This is `ble-gatt`'s own Linux backend.
  - `alexmoon/bluest`. Android was "planned" when ADR-0001 was written.
  - `embassy-rs/trouble` and `embassy-rs/nrf-softdevice` (embedded).
- **Go:**
  - `tinygo-org/bluetooth`, which does both roles on Linux, macOS, Windows and
    bare metal.
  - `paypal/gatt` and `muka/go-bluetooth`.
- **Python:**
  - `bluepy`, `bless` (peripheral), `python-bluezero`.
  - `google/bumble` (a host stack).
  - `pygatt` and Adafruit's BluefruitLE (both archived).
- **JavaScript:** `noble` and `bleno` (archived), `node-ble`, `gattacker`.
- **Apple only:**
  - BluetoothKit, which does both roles for device-to-device transfer and is
    the Apple counterpart to our tiers 2–3.
  - BabyBluetooth, RxBluetoothKit, bluejay, BlueCap, AsyncBluetooth.
- **C/C++ stacks and embedded:**
  - zephyr, btstack, NimBLE (`mynewt-nimble`, NimBLE-Arduino).
  - gattlib, tinyb (archived), gobbledegook, bluetoe.
- **Other:** `sputnikdev/bluetooth-manager` (Java on Linux), `blue_heron`
  (Elixir).

## What to borrow

- **Windows and Apple backends (M2, M3):** read btleplug's WinRT and
  CoreBluetooth code before writing ours. bluest is a second reference even
  without Android. Both are permissively licensed and mature on the central
  side.
- **Android bridge robustness:** Nordic's Android-BLE-Library and
  RxAndroidBle document many Android GATT quirks: operation queueing, status
  133, bonding, MTU. Check ours against their lists.
- **Android peripheral role:** Nordic's server manager and BLESSED's
  peripheral manager are the two mature open implementations to compare our
  Kotlin GATT server against.
- **Framing and sessions:** bitchat's fragmentation and relay, and Golden
  Gate's IP-over-BLE, are the two serious public designs above GATT. Compare
  them with our datagram tier.
- **Testing without radios:** FlutterBleLib's simulated peripherals do what
  our `MockBackend` does. Google's bumble (a host stack that can be pointed at
  a virtual controller) might let CI exercise the real BlueZ backend.

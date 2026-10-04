# Alternatives and prior art

An inventory of other BLE libraries, so we know what already exists, what to
borrow, and what this repo is for. ADR-0001 records the narrower Rust-only
survey behind the decision to write `ble-gatt`. This file is wider: every
language, plus apps and frameworks that solve the same problem.

**Inclusion rule:** 100+ GitHub stars. A few projects below that are listed
separately, because they compete directly.

**Snapshot:** star counts read from the GitHub API on 2026-10-04. They will
drift.

**Role columns:**
- **C** = central (scan, connect, GATT client).
- **P** = peripheral (advertise, GATT server).
- **?** = not confirmed from the project's own docs. Check before relying on it.

## Where `ble-gatt` sits

Very few libraries do both roles across platforms with a permissive licence,
in any language. Most are central-only: they talk *to* a device and are never
a device themselves. The peripheral role is where the gap is, especially on
Android from Rust. The closest neighbours are:

- **Same idea, other language:**
  - `tinygo-org/bluetooth` (Go).
  - Shiny (.NET).
  - Nordic's Android/Kotlin libraries (Android only).
  - BLESSED (Android only).
  - BluetoothKit and BlueCap (Apple only).
- **Same idea, Rust, one platform:** `bluer`. It is Linux only, and it is
  `ble-gatt`'s own Linux backend.
- **Same goal, as an app rather than a library:** bitchat, Berty, and Google's
  archived Golden Gate. They show that device-to-device messaging over BLE
  needs a framing and session layer above raw GATT. That is what this repo's
  datagram and peer-link tiers are.

## Rust

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [deviceplug/btleplug](https://github.com/deviceplug/btleplug) | 1177 | C | Win, macOS, iOS, Linux, Android | Host-side by design; the de facto Rust BLE client. |
| [bluez/bluer](https://github.com/bluez/bluer) | 450 | C + P | Linux | Official BlueZ bindings; our Linux backend. Also L2CAP, RFCOMM, mesh. |
| [embassy-rs/trouble](https://github.com/embassy-rs/trouble) | 450 | C + P | Bare metal | BLE host stack for microcontrollers. |
| [embassy-rs/nrf-softdevice](https://github.com/embassy-rs/nrf-softdevice) | 341 | C + P | nRF52 | Embedded only. |
| [MnlPhlp/tauri-plugin-blec](https://github.com/MnlPhlp/tauri-plugin-blec) | 228 | C | Tauri desktop + mobile | btleplug wrapped as a Tauri plugin. |
| [alexmoon/bluest](https://github.com/alexmoon/bluest) | 151 | C | Win, macOS, iOS, Linux | README says peripheral is out of scope. |
| [spieglt/FlyingCarpet](https://github.com/spieglt/FlyingCarpet) | 5330 | (app) | Android, iOS, Linux, macOS, Win | Tauri app; uses BLE to negotiate a Wi-Fi transfer. Prior art for Tauri + BLE on mobile. |

## C / C++

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [zephyrproject-rtos/zephyr](https://github.com/zephyrproject-rtos/zephyr) | 16683 | C + P | Embedded RTOS | Full BLE stack inside an RTOS. |
| [bluekitchen/btstack](https://github.com/bluekitchen/btstack) | 2144 | C + P | Embedded, desktop via HCI | Dual-mode stack. |
| [simpleble/simpleble](https://github.com/simpleble/simpleble) | 1137 | C | Win, macOS, iOS, Linux, Android | C++ core with Python and Rust bindings. Check its licence before use. |
| [h2zero/NimBLE-Arduino](https://github.com/h2zero/NimBLE-Arduino) | 1127 | C + P | ESP32, nRF5x | |
| [apache/mynewt-nimble](https://github.com/apache/mynewt-nimble) | 893 | C + P | Embedded | NimBLE host and controller. |
| [labapart/gattlib](https://github.com/labapart/gattlib) | 516 | C | Linux | GATT client over BlueZ. |
| [sj15712795029/bluetooth_stack](https://github.com/sj15712795029/bluetooth_stack) | 485 | C + P | STM32, Linux | Dual-mode stack. |
| [Google-Health-API/golden-gate](https://github.com/Google-Health-API/golden-gate) | 313 | C + P | Android, iOS, embedded | **Archived.** IP stack (CoAP/DTLS) over BLE for wearables. Closest prior art to our datagram tier. |
| [intel-iot-devkit/tinyb](https://github.com/intel-iot-devkit/tinyb) | 270 | C | Linux | **Archived.** BlueZ D-Bus, C++/Java. |
| [nettlep/gobbledegook](https://github.com/nettlep/gobbledegook) | 177 | P | Linux | Standalone BlueZ GATT server. |
| [TorstenRobitzki/bluetoe](https://github.com/TorstenRobitzki/bluetoe) | 144 | P | Embedded | C++ GATT server framework. |

## Go

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [paypal/gatt](https://github.com/paypal/gatt) | 1165 | C + P | Linux, macOS | Unmaintained for years. |
| [tinygo-org/bluetooth](https://github.com/tinygo-org/bluetooth) | 1013 | C + P | Linux, macOS, Win, bare metal | Peripheral availability varies by OS. Closest "both roles, one API" analogue outside Rust. |
| [muka/go-bluetooth](https://github.com/muka/go-bluetooth) | 674 | C + P | Linux | **Archived.** BlueZ D-Bus. |

## Python

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [hbldh/bleak](https://github.com/hbldh/bleak) | 2536 | C | Win, macOS, Linux (Android via p4a) | The standard Python BLE client; asyncio. |
| [IanHarvey/bluepy](https://github.com/IanHarvey/bluepy) | 1633 | C | Linux | |
| [google/bumble](https://github.com/google/bumble) | 563 | C + P | Any OS, needs an HCI controller | Full host stack in Python; also a test tool. |
| [peplin/pygatt](https://github.com/peplin/pygatt) | 531 | C | Linux, BGAPI | **Archived.** |
| [ukBaz/python-bluezero](https://github.com/ukBaz/python-bluezero) | 422 | C + P | Linux | BlueZ wrapper. |
| [adafruit/Adafruit_Python_BluefruitLE](https://github.com/adafruit/Adafruit_Python_BluefruitLE) | 394 | C | Linux, macOS | **Archived.** |
| [kevincar/bless](https://github.com/kevincar/bless) | 195 | P | Win, macOS, Linux | Peripheral companion to bleak. |

## JavaScript / TypeScript

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [noble/noble](https://github.com/noble/noble) | 3454 | C | Node: Linux, macOS, Win | **Archived** (community forks continue). |
| [noble/bleno](https://github.com/noble/bleno) | 2139 | P | Node: Linux, macOS | **Archived.** Peripheral half of noble. |
| [capacitor-community/bluetooth-le](https://github.com/capacitor-community/bluetooth-le) | 359 | C | Web, Android, iOS | |
| [chrvadala/node-ble](https://github.com/chrvadala/node-ble) | 345 | C | Linux | Pure JS over BlueZ D-Bus. |
| [securing/gattacker](https://github.com/securing/gattacker) | 852 | C + P | Node | Security / MITM tool, not a general library. |

## React Native / Cordova

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [dotintent/react-native-ble-plx](https://github.com/dotintent/react-native-ble-plx) | 3441 | C | Android, iOS | |
| [innoveit/react-native-ble-manager](https://github.com/innoveit/react-native-ble-manager) | 2333 | C | Android, iOS | |
| [don/cordova-plugin-ble-central](https://github.com/don/cordova-plugin-ble-central) | 953 | C | Android, iOS | |
| [randdusing/cordova-plugin-bluetoothle](https://github.com/randdusing/cordova-plugin-bluetoothle) | 804 | C + P | Android, iOS | Has peripheral (server) calls. |

## Flutter / Dart

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [pauldemarco/flutter_blue](https://github.com/pauldemarco/flutter_blue) | 2427 | C | Android, iOS | Abandoned; succeeded by flutter_blue_plus. |
| [chipweinberger/flutter_blue_plus](https://github.com/chipweinberger/flutter_blue_plus) | 1005 | C | Android, iOS, macOS, Web, Linux, Win | |
| [PhilipsHue/flutter_reactive_ble](https://github.com/PhilipsHue/flutter_reactive_ble) | 737 | C | Android, iOS | |
| [dotintent/FlutterBleLib](https://github.com/dotintent/FlutterBleLib) | 546 | C | Android, iOS | Can simulate peripherals for tests; compare our `MockBackend`. |

## Android (Java / Kotlin)

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [Jasonchenlijian/FastBle](https://github.com/Jasonchenlijian/FastBle) | 5505 | C | Android | |
| [dariuszseweryn/RxAndroidBle](https://github.com/dariuszseweryn/RxAndroidBle) | 3540 | C | Android | RxJava; well known for documenting Android BLE pitfalls. |
| [nordicsemi/Android-BLE-Library](https://github.com/nordicsemi/Android-BLE-Library) | 2415 | C + P | Android | Request queue, server manager. Worth reading for our Kotlin bridge. |
| [nordicsemi/Android-Scanner-Compat-Library](https://github.com/nordicsemi/Android-Scanner-Compat-Library) | 796 | C (scan) | Android | |
| [weliem/blessed-android](https://github.com/weliem/blessed-android) | 595 | C + P | Android | Has a peripheral manager. |
| [nordicsemi/Kotlin-BLE-Library](https://github.com/nordicsemi/Kotlin-BLE-Library) | 529 | C + P | Android | Coroutines successor to the above. |
| [Beepiz/BleGattCoroutines](https://github.com/Beepiz/BleGattCoroutines) | 476 | C | Android | |
| [kshoji/BLE-HID-Peripheral-for-Android](https://github.com/kshoji/BLE-HID-Peripheral-for-Android) | 258 | P | Android | HID-only peripheral. |
| [haodynasty/AndroidBleManager](https://github.com/haodynasty/AndroidBleManager) | 244 | C | Android | |

## Kotlin Multiplatform

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [JuulLabs/kable](https://github.com/JuulLabs/kable) | 1197 | C | Android, Apple, JS | Coroutine API; good reference for API shape. |
| [Reedyuk/blue-falcon](https://github.com/Reedyuk/blue-falcon) | 488 | C | iOS, Android, macOS, Win, JS | |

## Apple (Swift / Objective-C)

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [coolnameismy/BabyBluetooth](https://github.com/coolnameismy/BabyBluetooth) | 4742 | C (P ?) | iOS, macOS | |
| [rhummelmose/BluetoothKit](https://github.com/rhummelmose/BluetoothKit) | 2300 | C + P | iOS, macOS | Built for device-to-device data transfer, the same goal as our tiers 2–3, but Apple only. |
| [Polidea/RxBluetoothKit](https://github.com/Polidea/RxBluetoothKit) | 1435 | C (P ?) | iOS, macOS | |
| [steamclock/bluejay](https://github.com/steamclock/bluejay) | 1127 | C | iOS | |
| [troystribling/BlueCap](https://github.com/troystribling/BlueCap) | 716 | C + P | iOS | |
| [manolofdez/AsyncBluetooth](https://github.com/manolofdez/AsyncBluetooth) | 202 | C | iOS, macOS | async/await over CoreBluetooth. |

## .NET

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [shinyorg/shiny](https://github.com/shinyorg/shiny) | 1583 | C + P | iOS, macOS, Android, Win, Linux | BLE client plus "hosting" (peripheral) in a larger framework. |
| [inthehand/32feet](https://github.com/inthehand/32feet) | 990 | C | Win, Android, iOS, macOS | |
| [dotnet-bluetooth-le/dotnet-bluetooth-le](https://github.com/dotnet-bluetooth-le/dotnet-bluetooth-le) | 957 | C | Android, iOS, macOS, Win | Plugin.BLE. |

## Java (desktop) / Elixir

| Project | Stars | Roles | Platforms | Notes |
|---|---:|---|---|---|
| [sputnikdev/bluetooth-manager](https://github.com/sputnikdev/bluetooth-manager) | 112 | C | Linux (via TinyB) | |
| [blue-heron/blue_heron](https://github.com/blue-heron/blue_heron) | 118 | C (P ?) | Linux / Nerves via HCI | Elixir. |

## Apps that message over BLE

These are not libraries, but each one solved the problem `ble-gatt`'s upper
tiers address: two ordinary devices talking to each other over BLE, both
acting as central and peripheral.

| Project | Stars | Language | Notes |
|---|---:|---|---|
| [permissionlesstech/bitchat](https://github.com/permissionlesstech/bitchat) | 36341 | Swift | BLE mesh chat on iOS and macOS: fragmentation, relay, E2E crypto on top of GATT. |
| [permissionlesstech/bitchat-android](https://github.com/permissionlesstech/bitchat-android) | 7678 | Kotlin | Android counterpart, wire-compatible with the Swift app. |
| [berty/berty](https://github.com/berty/berty) | 9311 | TypeScript / Go | P2P messenger with a BLE proximity transport under libp2p. |
| [briar/briar](https://github.com/briar/briar) | 699 | Java | Offline messenger; uses classic Bluetooth (RFCOMM), not BLE GATT. |

## Below 100 stars but directly comparable

| Project | Stars | Why it's here |
|---|---:|---|
| [rohitsangwan01/ble-peripheral-rust](https://github.com/rohitsangwan01/ble-peripheral-rust) | 62 | Rust peripheral role, cross-platform; no Android backend (see ADR-0001). |
| [weliem/blessed-bluez](https://github.com/weliem/blessed-bluez) | 98 | BLESSED for Java on BlueZ. |
| [juliansteenbakker/flutter_ble_peripheral](https://github.com/juliansteenbakker/flutter_ble_peripheral) | 91 | Flutter advertising-only peripheral. |
| `blew` / `tauri-plugin-blew` | — | The one Rust crate covering both roles on Android, but AGPL-3.0. Ruled out in ADR-0001. |

## What to borrow

- **Windows and Apple backends (M2, M3):** read btleplug's and bluest's WinRT
  and CoreBluetooth code before writing ours. Both are permissively licensed
  and mature on the central side.
- **Android bridge robustness:** Nordic's Android-BLE-Library and
  RxAndroidBle document many Android GATT quirks: operation queueing, status
  133, bonding, MTU. Check ours against their lists.
- **Framing and sessions:** bitchat's fragmentation and relay, and Golden
  Gate's IP-over-BLE, are the two serious public designs above GATT. Compare
  them with our datagram tier.
- **Testing without radios:** bumble (a host stack that can be pointed at a
  virtual controller) and FlutterBleLib's simulated peripherals do what our
  `MockBackend` does. bumble might let CI exercise the real BlueZ backend.

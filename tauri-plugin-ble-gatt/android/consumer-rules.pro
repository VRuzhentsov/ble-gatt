# dev.blegatt is a thin, entirely-JNI-surfaced bridge module: every method
# on BleGattBridge is invoked by name from the Rust side (env.call_method /
# GetMethodID by exact name+signature), never from a Kotlin/Java call site
# R8's static analysis can see. Left unprotected, a consuming app's release
# R8 pass has no reason to believe these methods are reachable, and can
# rename or strip them -- producing a NoSuchMethodError at runtime that
# only ever reproduces in a release build. Confirmed on real hardware as
# the likely cause of exactly that failure on startAdvertising; see
# ble-gatt/docs/hardware-verification.md, 2026-08-17 entry.
-keep class dev.blegatt.** { *; }
-keepclassmembers class dev.blegatt.** { *; }

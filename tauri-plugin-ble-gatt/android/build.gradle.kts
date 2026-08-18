// Self-contained: buildable standalone (`./gradlew build`, this repo's own
// verification loop, plugin versions resolved via settings.gradle.kts'
// pluginManagement) *and* embeddable as a subproject when a generated
// Tauri app includes this directory (plugin versions resolved by the
// generated app's own root build instead — deliberately unpinned here,
// matching every official Tauri plugin's android/build.gradle.kts; pinning
// a version in a *subproject* conflicts with a version the root project
// already resolved).
plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

repositories {
    google()
    mavenCentral()
}

android {
    namespace = "dev.blegatt"
    compileSdk = 36

    defaultConfig {
        // BluetoothGattServer (peripheral role) needs API 21+; this
        // matches Fini's own minSdk, the actual consuming app.
        minSdk = 24
        // BleGattBridge's methods are invoked by name via raw JNI from the
        // Rust side (env.call_method / GetMethodID), not through any
        // Kotlin/Java call site R8's static analysis can see -- so without
        // this, a consuming app's release R8 pass has nothing telling it
        // they're reachable, and can rename or strip them. Confirmed on
        // real hardware as the likely cause of a NoSuchMethodError on
        // startAdvertising that reproduced only in a release build (see
        // docs/hardware-verification.md, 2026-08-17 entry).
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
    kotlinOptions {
        jvmTarget = "1.8"
    }

    sourceSets {
        getByName("main") {
            kotlin.srcDirs("src/main/kotlin")
        }
    }

    lint {
        // Runtime permission checks are deliberately not this module's
        // job — see BleGattBridge.kt's doc comment. Every flagged call
        // site already has a documented SecurityException-to-BleError
        // path on the Rust side of the JNI boundary.
        disable += "MissingPermission"
    }
}

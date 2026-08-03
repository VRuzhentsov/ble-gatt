pluginManagement {
    repositories {
        google()
        gradlePluginPortal()
        mavenCentral()
    }
    // Only takes effect for a standalone `./gradlew build` in this
    // directory (this repo's own verification loop) — Gradle reads a
    // single settings file per build, so when a generated Tauri app
    // includes this `android/` directory as a subproject, its own root
    // settings.gradle governs plugin versions instead and this block is
    // never consulted. See build.gradle.kts's comment for why the
    // `plugins {}` block there stays deliberately unversioned.
    plugins {
        id("com.android.library") version "8.11.0"
        id("org.jetbrains.kotlin.android") version "1.9.25"
    }
}

rootProject.name = "ble-gatt-android"

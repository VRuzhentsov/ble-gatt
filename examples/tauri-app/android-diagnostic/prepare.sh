#!/bin/sh
# Apply only to this example's ignored, generated Android project after init.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
app="$here/../src-tauri/gen/android/app"
test -f "$app/build.gradle.kts" || { printf '%s\n' 'Run npm run tauri -- android init --ci first' >&2; exit 1; }
mkdir -p "$app/src/main/java/dev/blegatt/example" "$app/src/debug"
cp "$here/MainActivity.kt" "$app/src/main/java/dev/blegatt/example/MainActivity.kt"
cp "$here/DiagnosticService.kt" "$app/src/main/java/dev/blegatt/example/DiagnosticService.kt"
cp "$here/AndroidManifest.xml" "$app/src/debug/AndroidManifest.xml"
printf '%s\n' 'Applied example-only diagnostic lifetime overlay (service declared in debug only).'

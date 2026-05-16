# AntDownload — Android download smoke test

Throwaway Compose app that links the embedded light-node through
[`crates/ant-ffi`](../../crates/ant-ffi)'s JNI surface and downloads
a Swarm reference pasted into a text field. Sibling to
[`examples/ios-download/`](../ios-download/); same scope, same FFI,
different bridge layer (JNI instead of `extern "C"`). Corresponds to
PLAN.md § 9, "Android download smoke test (pre-FFI)".

**This is not the mobile artefact.** It exists to shake out
Android-specific surprises (NDK toolchain quirks, JNI symbol
mangling, `NetworkOnMainThreadException`, `libc++_static` vs
`libc++_shared`, logcat wiring) on a five-method `external fun`
surface that's cheap to delete. The proper UniFFI-generated `.aar`
replaces this at the end of Phase 4.

## Requirements

- macOS or Linux host with:
  - **JDK 17** (Homebrew: `brew install openjdk@17` or any AdoptOpenJDK 17).
  - **Android command-line tools or Android Studio**, with:
    - **Android NDK r26+** (this repo was validated against r27.2). The
      build system reads `ANDROID_NDK_HOME` (preferred) or
      `ANDROID_HOME` to find it.
    - **SDK platform 36** + build-tools 36.x for the Gradle side.
  - **Rust stable** with the Android targets you intend to use:
    ```sh
    rustup target add aarch64-linux-android       # arm64 phones (default, post-2017)
    rustup target add armv7-linux-androideabi     # older armv7 devices
    rustup target add x86_64-linux-android        # emulator
    ```
  - **`cargo-ndk`** front-end:
    ```sh
    cargo install cargo-ndk
    ```
  - **Gradle 9.4.1** is invoked through the wrapper (`./gradlew`); only
    the *first* run needs a system Gradle to bootstrap the wrapper,
    and the wrapper jar is checked in once `gradle wrapper` has been
    run (mirroring the Xcode project files in
    `examples/ios-download/`).

If you installed the SDK via Homebrew on macOS, the env vars are:

```sh
export ANDROID_HOME=/opt/homebrew/share/android-commandlinetools
export ANDROID_NDK_HOME="$ANDROID_HOME/ndk/$(ls -1 "$ANDROID_HOME/ndk" | tail -1)"
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
```

## Building

From the repo root:

```sh
# 1. Cross-compile crates/ant-ffi for the device/emulator ABI(s) you want.
#    Each invocation drops `libant_ffi.so` into
#    examples/android-download/app/src/main/jniLibs/<abi>/.
cargo xtask build-android-arm64                # arm64 phone (most common)
cargo xtask build-android-x86_64               # emulator on Apple Silicon / Intel Mac
cargo xtask build-android-all                  # all three ABIs

# 2. Build the APK.
cd examples/android-download
./gradlew assembleDebug
```

The APK lands in `app/build/outputs/apk/debug/app-debug.apk`. On a
cold cargo cache the Rust cross-compile is the slow step (~5 min);
incremental rebuilds finish in seconds.

## Running on the emulator

```sh
# Boot any AVD (here the system image you've installed in
# Android Studio's Device Manager — emulator -list-avds shows them).
emulator -avd <avd-name> &

adb wait-for-device
adb install -r app/build/outputs/apk/debug/app-debug.apk
adb shell am start -n at.vibing.ant.downloadsmoke/.MainActivity
```

Then in the app:

1. Wait until the status pill flips from "Starting…" (orange) to
   "Ready" (green) — that's the Tokio runtime + libp2p host coming
   up. Reaching full peer-set warmth takes a few more seconds of BZZ
   handshakes; the peer counter on the screen tracks that live.
2. Paste a reference into the text field. Accepted forms:

   | Input | Meaning |
   |---|---|
   | `<64-hex>` | single-chunk or multi-chunk `/bytes` tree |
   | `bytes://<64-hex>` | explicit bytes-tree form |
   | `bzz://<64-hex>` | mantaray manifest; resolves `website-index-document` |
   | `bzz://<64-hex>/some/path` | manifest with explicit path |

3. Tap **Download**. On success the result card shows the byte count,
   wall-clock elapsed time, and a hex prefix (up to 32 bytes).
   Errors surface in red.

The underlying FFI retries the "no peers available" error from a
cold node for up to 10 minutes, so the first run on a fresh emulator
usually succeeds without babysitting.

## Running on a real Android device

Much simpler than the iOS provisioning-profile dance — Android
sideloads via USB debugging:

1. Enable **Developer Options** on the device: `Settings → About
   phone → tap Build number 7 times`. New entry appears under
   `Settings → System → Developer options`.
2. Inside Developer Options, enable **USB debugging**. Plug the phone
   into the Mac. The phone shows a fingerprint dialog; tap **Allow**.
3. Verify connectivity:
   ```sh
   adb devices
   ```
   You should see your device listed as `device` (not `unauthorized`
   or `offline`).
4. Build for the device's ABI (arm64 unless you have a very old
   phone), then install:
   ```sh
   cargo xtask build-android-arm64
   cd examples/android-download
   ./gradlew installDebug
   adb shell am start -n at.vibing.ant.downloadsmoke/.MainActivity
   ```

If the device is on Wi-Fi only and you want over-the-air install,
`adb tcpip 5555` + `adb connect <ip>:5555` works the same as USB
afterwards.

## Driving the app from the shell

`am start` accepts `--es` (extra-string) flags, so a future
`--auto-download <reference>` argument (mirror of the iOS
launch-argument path) would slot in here. v1 of this app doesn't
parse extras yet — paste the hex into the text field manually for
now.

## Logs

The Rust side bridges `tracing` events to logcat under tag
`ant-ffi`. The Kotlin side just uses `android.util.Log` (and the
default Compose / lifecycle tags). To watch only ant output:

```sh
adb logcat -s ant-ffi:V at.vibing.ant.downloadsmoke:V *:S
```

Useful filters when triaging slow downloads:

```sh
adb logcat -s ant-ffi:I | grep -E '(handshake|peer|retrieval)'
```

## Troubleshooting

- **"Failed: nativeInit returned 0 without throwing"** — the FFI
  layer's `catch_unwind` swallowed something it didn't have a string
  for. Check `adb logcat -s ant-ffi:V`; a panic in `init_inner` is
  the most common cause and surfaces with full stack there.
- **`UnsatisfiedLinkError: dlopen failed: cannot locate symbol "Java_…"`** —
  the `libant_ffi.so` in `app/src/main/jniLibs/<abi>/` was built
  without `--features jni`, or against a different package name.
  Re-run `cargo xtask build-android-arm64` from a clean `target/`.
- **"download did not complete within 600 seconds"** — the embedded
  node didn't finish the retrieval in time. Usually means the BZZ
  peer set hasn't warmed up yet; wait ~10 seconds after "Ready"
  before retrying. Cellular-to-mainnet handshakes on cold starts are
  the slowest path we exercise.
- **`A/libc: Fatal signal 6 (SIGABRT)` in logcat referencing Rust** —
  a panic in the Tokio runtime that escaped the `catch_unwind` in
  `crates/ant-ffi/src/jni.rs`. File an issue with the logcat dump;
  this should never happen in steady state.
- **Gradle: "Unable to strip the following libraries, packaging them
  as they are: libant_ffi.so"** — benign warning. The Rust release
  build leaves debug info embedded in the `.so`; `strip` is a
  no-op on it because the relocation tables are different from
  what the NDK strip expects. The proper mobile artefact will
  pre-strip in the build script.

## What lives where

| Concern | Owner |
|---|---|
| `nativeInit` / `nativeDownload` / `nativePeerCount` / `nativeAgentString` / `nativeShutdown` | `crates/ant-ffi/src/jni.rs` |
| `cdylib` emission, `jni` Cargo feature | `crates/ant-ffi/Cargo.toml` |
| `tracing` → `logcat` writer | `crates/ant-ffi/src/lib.rs` (`android_log` mod) |
| Mainnet bootnodes, identity persistence, in-memory chunk cache | `crates/ant-ffi/src/lib.rs` |
| `cargo xtask build-android-*` recipes | `xtask/src/main.rs` |
| AGP 9.x build, Compose UI, JNI Kotlin glue | `app/` |
| Manifest, theme, launcher icon | `app/src/main/{AndroidManifest.xml,res/}` |
| Bundle id `at.vibing.ant.downloadsmoke` (matches iOS) | `app/build.gradle.kts` |

Same rule of thumb as iOS: **removing this example app must not
break the Rust test suite, and removing ant must not break the
example app.** Anything a third-party Android app would have to
reinvent themselves belongs in `crates/ant-ffi/`, not under
`app/`.

## Versions current as of 2026-04

| Component | Version |
|---|---|
| AGP | 9.2.1 |
| Gradle | 9.4.1 |
| Kotlin | 2.3.21 |
| Compose BOM | 2026.04.01 |
| Compile / target SDK | 36 |
| Min SDK | 26 (Android 8.0) |
| NDK API level | 26 |
| `cargo-ndk` | 4.1.2 |
| `jni` Rust crate | 0.22.4 |

Bumping any of these usually requires updating both
`gradle/libs.versions.toml` and the matching xtask / Cargo
declarations in lock-step — see the comments in those files.

## See also

- **iOS counterpart:** [`examples/ios-download/`](../ios-download/) —
  same scope, SwiftUI front-end, hand-written `extern "C"` bridge.
  Both apps consume the same Rust state machine inside `ant-ffi`;
  only the bridge layer differs.
- **Phase 4 mobile artefact:** [PLAN.md § 9](../../PLAN.md) → "Android
  download smoke test (pre-FFI)" + the surrounding § 7 (UniFFI
  surface) and § 12 (size budget) sections.

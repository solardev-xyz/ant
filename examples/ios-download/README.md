# AntDownload — iOS download smoke test

Throwaway SwiftUI app that links the embedded light-node through
[`crates/ant-ffi`](../../crates/ant-ffi) and downloads a Swarm
reference pasted into a text field. Corresponds to PLAN.md § 9, "iOS
download smoke test (pre-FFI)".

**This is not the mobile artefact.** It exists to shake out iOS-side
surprises (ATS, Tokio-in-UIKit, Noise-XX-over-TCP on cellular, stderr
→ OSLog) on a three-function `extern "C"` surface that's cheap to
delete. The proper UniFFI-generated `.xcframework` replaces this at
the end of Phase 4.

## Requirements

- macOS 13+ with **Xcode 15 or newer** (Xcode 26 was the version used
  to validate — any recent release should work).
- Rust stable with the iOS targets you intend to use installed:
  ```sh
  rustup target add aarch64-apple-ios-sim       # simulator (default)
  rustup target add aarch64-apple-ios           # real device (only for §"Running on a real iPhone")
  # Intel-Mac host? Add x86_64-apple-ios for the simulator instead.
  ```
- Apple Silicon Mac to match the default `aarch64-apple-ios-sim`
  simulator slice, or an Intel Mac with the x86_64 slice.

## Running in the simulator

From the repo root:

```sh
# One-time: pre-build the Rust static lib (the Xcode "Build Rust"
# script phase also does this on every ⌘B, so this step is only useful
# if you want to surface Rust errors before opening Xcode).
cargo xtask build-ios-sim

# Open the project and run on a simulator.
open examples/ios-download/AntDownload.xcodeproj
```

In Xcode, pick any iOS Simulator destination (iPhone 17, 16, etc.) and
hit ⌘R. The first build cross-compiles `ant-ffi` (≈2-5 minutes cold,
sub-second incremental); subsequent builds are fast because cargo's
target directory caches artefacts.

Command-line alternative:

```sh
cd examples/ios-download
xcodebuild -project AntDownload.xcodeproj -scheme AntDownload \
  -destination 'platform=iOS Simulator,name=iPhone 17' \
  -configuration Release ARCHS=arm64 ONLY_ACTIVE_ARCH=YES build
```

## Using the app

1. Launch — the status row flips from "Starting…" to "Ready" once the
   embedded node has a Tokio runtime up. Reaching full peer-set warmth
   takes a few more seconds of BZZ handshakes; background activity is
   visible in the Xcode console.
2. Paste a reference into the text field. Accepted forms:

   | Input | Meaning |
   |---|---|
   | `<64-hex>` | single-chunk or multi-chunk `/bytes` tree |
   | `bytes://<64-hex>` | explicit bytes-tree form |
   | `bzz://<64-hex>` | mantaray manifest; resolves `website-index-document` |
   | `bzz://<64-hex>/some/path` | manifest with explicit path |

3. Tap **Download**. On success the Result section shows the total
   byte count, a hex prefix (up to 32 bytes), and wall-clock elapsed
   time. Errors surface in the red Error section.

A known-good single-chunk reference for a cold-start smoke is any
32-byte CAC that currently resolves through `antd get` on your desktop
— the two clients share identical retrieval behaviour.

### Driving the app from the shell

The app accepts a `--auto-download <reference>` launch argument that
pre-fills the text field and fires the download as soon as the node
reports "Ready". This is the cleanest way to smoke the full pipeline
without Accessibility permissions for GUI scripting:

```sh
APP=/Users/you/Library/Developer/Xcode/DerivedData/.../AntDownload.app
UDID=$(xcrun simctl list devices available | grep 'iPhone 17 ' | head -1 | sed -E 's/.*\(([A-F0-9-]+)\).*/\1/')

xcrun simctl boot "$UDID" 2>/dev/null; open -a Simulator
xcrun simctl install "$UDID" "$APP"
xcrun simctl launch "$UDID" at.vibing.ant.downloadsmoke \
    --auto-download 'bzz://<hex>/<optional-path>'
```

The underlying FFI retries the "no peers available; wait for
handshakes to complete" error from a cold node for up to 75 seconds,
so the first run on a fresh simulator usually succeeds without
babysitting.

## Running on a real iPhone

The Xcode project picks the right Rust slice automatically based on
`PLATFORM_NAME`: a simulator destination cross-compiles
`aarch64-apple-ios-sim`, a device destination cross-compiles
`aarch64-apple-ios`. `LIBRARY_SEARCH_PATHS` is `[sdk=...]`-conditional
so the device build can never accidentally link the simulator slice.

One-time GUI setup (cannot be scripted — Apple requires interactive
prompts on the Mac and the iPhone for trust and codesigning):

1. **Enable Developer Mode on the iPhone.** The toggle is hidden until
   a dev tool first contacts the device. Plug the phone in via USB,
   trust the Mac, then run any `xcrun devicectl device info details`
   command from this Mac. The toggle then appears at the bottom of
   `Settings → Privacy & Security → Developer Mode`. Turn it on; the
   phone reboots and prompts again on unlock.
2. **Sign in to Xcode.** `Xcode → Settings → Accounts → +` → Apple ID.
   Click `Manage Certificates… → + → Apple Development` to create the
   iOS development certificate (you most likely only have a macOS
   `Developer ID Application` cert prior to this).
3. **Confirm the team in `project.pbxproj`.** Both `Debug` and
   `Release` configs hard-code `DEVELOPMENT_TEAM = 46XPHRBLH5`
   (Solar Dev Ltd). If you sign in to Xcode with an Apple ID that
   isn't a member of that team, change both occurrences to your own
   team ID — Personal Teams are a 10-char ID visible in
   `Xcode → Settings → Accounts` next to the team name.

Then from the shell:

```sh
# Pre-build the device slice (the Build Rust phase will also do this
# automatically when Xcode targets a device, this is just a sanity
# check that the cross-compile works in isolation).
cargo xtask build-ios-device

cd examples/ios-download

# Build, sign, and install. -allowProvisioningUpdates lets xcodebuild
# create / refresh the provisioning profile non-interactively the
# first time (subsequent builds reuse the cached profile).
xcodebuild -project AntDownload.xcodeproj -scheme AntDownload \
    -configuration Release -destination 'generic/platform=iOS' \
    -allowProvisioningUpdates \
    -derivedDataPath build-device \
    build

DEVICE_UDID=$(xcrun devicectl list devices --json-output - 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["devices"][0]["identifier"])')
APP=build-device/Build/Products/Release-iphoneos/AntDownload.app

xcrun devicectl device install app --device "$DEVICE_UDID" "$APP"
xcrun devicectl device process launch \
    --device "$DEVICE_UDID" at.vibing.ant.downloadsmoke
```

Common failure modes:

- `linker command failed … built for 'iOS-simulator'` — the
  `LIBRARY_SEARCH_PATHS[sdk=...]` overlay is missing or the Rust
  device slice hasn't been built yet. `cargo xtask build-ios-device`
  fixes the latter; the former should already be wired up in
  `project.pbxproj`.
- `The operation failed because Developer Mode is disabled.` — step 1
  above hasn't been completed, or wasn't completed for *this* Mac.
- `No profiles for 'at.vibing.ant.downloadsmoke' were found` — Xcode
  hasn't been signed in (step 2) or the team in
  `project.pbxproj` isn't one your Apple ID has membership in (step 3).
- The phone is `tunnelState: disconnected` in `devicectl list devices`
  — unlock the phone and re-run; iOS suspends the dev tunnel when the
  device is locked for more than a few minutes.

## Troubleshooting

- **"Starting… → Failed: ..."** means `ant_init` returned a non-null
  error string; check the Xcode console for the `tracing` output
  prefixed `ant-ffi`.
- **"download did not complete within 75 seconds"** — the embedded
  node didn't finish the retrieval in time. Usually means the BZZ
  peer set hasn't warmed up yet; wait ~10 seconds after "Ready"
  before retrying. Cellular-to-mainnet handshakes on cold starts are
  the slowest path we exercise.
- **Linker errors referencing `lant_ffi` or `libant_ffi`** — the
  static lib wasn't built. Run `cargo xtask build-ios-sim` from the
  repo root once, then rebuild.
- **ATS blocking TCP dials.** `Info.plist` sets
  `NSAllowsArbitraryLoads=true` for the smoke test; Noise XX over raw
  TCP isn't TLS and ATS would refuse every bootnode. The proper
  mobile artefact (PLAN.md § 8.4) replaces this with host-scoped
  exceptions.

## What lives where

| Concern | Owner |
|---|---|
| `ant_init` / `ant_download` / `ant_free_*` / `ant_shutdown` | `crates/ant-ffi/` |
| `ant.h`, bridging header path | `crates/ant-ffi/include/ant.h` |
| Mainnet bootnodes, identity persistence, in-memory chunk cache | `crates/ant-ffi/src/lib.rs` |
| `cargo xtask build-ios-sim` recipe | `xtask/` |
| SwiftUI views, reference parsing, copy-buffer handling | `AntDownload/*.swift` |
| `Info.plist` (ATS exception, scene manifest) | `AntDownload/Info.plist` |
| Bundle id, codesigning, scheme | `AntDownload.xcodeproj/` |

Rule of thumb: removing this example app must not break the Rust
test suite, and removing ant must not break the example app. If
something you need here isn't provided by the public FFI, it belongs
in `crates/ant-ffi/`.

## See also

- **`bzz://` browser sketch** — the static-link architecture here
  generalises cleanly to a native iOS browser that serves `bzz://`
  content directly through `WKURLSchemeHandler`, with no localhost
  HTTP gateway. Design note (gap list, iOS caveats, where it sits in
  the roadmap) lives in [PLAN.md § 9 → "iOS `bzz://` browser —
  architecture note"](../../PLAN.md).

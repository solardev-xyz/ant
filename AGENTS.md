# Agent Notes

- The implementation plan lives in `PLAN.md` at the repo root.
- **Keep CI green — it is a hard requirement, not a nicety.** Before you
 push or merge anything, the full CI gate must pass locally. CI
 (`.github/workflows/ci.yml`) runs these steps in order and stops at the
 first failure, so run all of them yourself first:
   1. `cargo fmt --all -- --check`
   2. `cargo clippy --workspace --all-targets -- -D warnings`
   3. `cargo clippy -p ant-ffi --features jni --all-targets -- -D warnings`
   4. `cargo clippy -p ant-ffi --features chain --all-targets -- -D warnings`
   5. `cargo test --workspace --lib`
 Steps 3 & 4 are separate because `ant-ffi`'s `jni` (Android) and `chain`
 (iOS light-mode: on-chain `/wallet` `/stamps` `/chequebook`) features are
 off by default, so the whole-workspace step 2 never compiles them — they
 need an explicit per-crate lint or their code is never type-checked by CI.
 Never merge to `main` (or push a branch you expect to be merged) with a
 known-red gate. Toolchain upgrades can introduce new `clippy`/`rustfmt`
 findings on untouched code — fix those too (the gate is whole-repo, not
 just your diff) rather than leaving `main` red. If a finding is a genuine
 false positive, prefer a narrowly-scoped `#[allow(...)]` with a comment
 over disabling the lint workspace-wide.
   - **Per-crate clippy (`-p foo`) is not a substitute for the gate.** It
    skips `cargo fmt` and, more importantly, skips `--all-targets`, so it
    never compiles test/integration targets or the rest of the workspace.
    A change that builds clean under `-p ant-ffi -p ant-p2p` can still be
    red on the real gate. Run steps 1–5 verbatim before opening *or*
    pushing to a PR — not a hand-picked subset. (This was learned the hard
    way: PR #16 went red twice post-open on exactly these two blind spots.)
   - **Adding a variant to a widely-matched enum touches more places than
    your diff shows.** `ControlCommand` (and `ControlAck`) is matched
    exhaustively in several spots across crates — the production node loop
    in `ant-p2p` (`handle_control_command` *and* its run-loop command arm),
    plus **test-only stubs** like the fake node loop in
    `ant-gateway/tests/common/mod.rs`. After adding a variant, grep the
    workspace for every `match` on it (`rg 'ControlCommand::'`) and wire it
    everywhere. The test stubs only fail under `--all-targets`, which is
    precisely why step 2 is whole-workspace-all-targets and per-crate
    checks miss them.
- Bump the patch version (`x.y.Z` → `x.y.Z+1`) of every workspace
 crate whose Cargo.toml declares one — currently `antd`, `antctl`,
 `antop`, `ant-chain`, `ant-control`, `ant-crypto`, `ant-ffi`,
 `ant-gateway`, `ant-node`, `ant-p2p`, `ant-postage`, `ant-retrieval`
 — on **each** deployment to `vibing.at/ant` (or any other production
 host). Do this *before* building the release binary so `antd
 --version` reflects what's actually deployed and the systemd journal
 lets us correlate behaviour to a specific build. Use the same patch
 number across all crates; we ship them as one unit. Bump minor or
 major only when the user asks for it explicitly.
- **Always put version bumps in their own commit**, separate from any
 fix/feature work, so the diff for a change stays clean and easy to
 review/revert. Never fold the `Cargo.toml` patch-version bumps into a
 commit that also contains code changes — commit the bump on its own
 (e.g. `chore: bump workspace crates to x.y.Z`).

## Driving the iOS Drive app on the simulator (idb + log capture)

The example app lives in `examples/ios-drive` (bundle id
`at.vibing.ant.drive`). The booted simulator we use is UDID
`FCDD32E4-137A-45C4-9C7D-BB89340DF6EA` (confirm with `xcrun simctl list
devices booted` — it can change).

**Build / install / launch:**
```bash
cd examples/ios-drive
xcodebuild -project AntDrive.xcodeproj -scheme AntDrive -configuration Debug \
  -destination 'platform=iOS Simulator,id=<UDID>' -derivedDataPath build build
APP=build/Build/Products/Debug-iphonesimulator/AntDrive.app
xcrun simctl install <UDID> "$APP" && xcrun simctl launch <UDID> at.vibing.ant.drive
```
The Xcode build phase recompiles the `ant-ffi` Rust slice, so a plain
`xcodebuild` picks up workspace crate changes.

**Capturing the embedded node's logs.** The FFI routes `tracing` to
**stderr**, which the simulator does **not** forward to the unified log
(`log stream` / `log show` won't see it). To read logs, relaunch with a
console attached:
```bash
xcrun simctl terminate <UDID> at.vibing.ant.drive
nohup xcrun simctl launch --console-pty --terminate-running-process <UDID> \
  at.vibing.ant.drive > /tmp/app_console.txt 2>&1 &
# then: grep -niE "upload completed|post-upload heal" /tmp/app_console.txt
```
Relaunching restarts the embedded node, so wait ~20 s for the bzz
handshakes (`peer_set_size` climbs to ~100) before exercising uploads.

**Driving the UI with idb.** Only `idb_companion` is installed via brew;
the friendly `idb` CLI is the Python `fb-idb` package and is **not** on
PATH. It needs Python ≤3.11 (it breaks on 3.14). Set up a venv once
(use a persistent path like `~/.idb-venv`, not `/tmp`):
```bash
python3.11 -m venv ~/.idb-venv && ~/.idb-venv/bin/pip install fb-idb
IDB=~/.idb-venv/bin/idb
$IDB connect <UDID>
$IDB ui describe-all --udid <UDID>      # a11y tree: labels + frames (in points)
$IDB ui tap --udid <UDID> <x> <y>       # coordinates are POINTS, not pixels
```
Screenshots (`xcrun simctl io <UDID> screenshot f.png`) are at the
device's pixel scale (e.g. 1206×2622 px = 402×874 pt → divide by 3).
Out-of-process system UI (the Photos picker) is invisible to
`describe-all`; tap it by point coordinates derived from a screenshot.
Upload flow: tap **Add** → "Upload photos" → tap a photo thumbnail →
tap the **✓** (Done) button top-right.

# Ant — Implementation Plan

**Project:** Ant — a lightweight Swarm light node.
**Primary deployment:** embeddable Rust library (`libant`) consumed by native iOS and Android apps via UniFFI.
**Secondary deployment:** `antd` daemon for macOS, Linux, Windows, and Raspberry Pi — shipped as signed binaries with platform-appropriate service integration (systemd / launchd / Windows Service).

A from-scratch Rust implementation of a Swarm light node with upload and postage-stamp management. The embedded mobile library is the primary product, with a per-platform artifact size target in the single-digit megabytes. The same core compiles to a standalone `antd` binary for desktop and single-board computers, enabling a personal-node use case (always-on home node, desktop companion, self-hosted gateway) in addition to mobile.

---

## 1. Goals & Non-Goals

### Goals

- Run a Swarm **light node** (downloads + uploads) embedded in mobile apps.
- Full **postage-stamp management** on device from v1 (purchase, top-up, dilute, per-chunk signing).
- Full **SWAP / chequebook** support, required to pay forwarders for pushsync.
- Ship as **one Rust library** with typed Swift + Kotlin bindings generated from a single IDL.
- **Artifact size target: 6–10 MB stripped per platform** on mobile (vs. 30–60 MB for `bee-lite`). Desktop / Pi have no hard size budget.
- Ship `antd` as a **supported secondary deployment** on macOS (Intel + Apple Silicon), Linux (x86_64 + aarch64), Windows (x86_64), and Raspberry Pi 3+ (aarch64) — signed binaries, installers, and service-unit templates.
- Interoperate with the public Swarm mainnet (no fork, no private network).

### Non-Goals (v1)

- Full node features: chunk storage/forwarding, pullsync, storage/bandwidth incentives, staking, redistribution lottery.
- PSS / GSOC messaging.
- Cheque cashing (we send cheques, never receive them as a consumer-class node).
- Exotic libp2p transports (QUIC, WebRTC, WebSocket, relay) on mobile. May be enabled later for desktop / Pi behind cargo feature flags.
- 32-bit ARM (Pi Zero / Pi 1) as a first-class target — best-effort only.
- ACT (access control trie), advanced feeds, custom redundancy schemes. Deferred to v2.

### Explicit constraints

- **Language:** Rust (stable channel; nightly only for optional size-shrinking `build-std`).
- **Binding layer:** UniFFI → Swift `.xcframework` + Kotlin `.aar`.
- **Chain:** Gnosis Chain mainnet only. Testnet (Sepolia-based dev network) for development.
- **No Ethereum full client.** All chain interactions go through a user-configurable JSON-RPC endpoint.

---

## 2. Reference Architecture

```
┌──────────────────────────────────┐   ┌──────────────────────────┐
│   Mobile App (Swift / Kotlin)    │   │        antd CLI          │
│    UI, onboarding, key UX        │   │  dev + interop harness   │
└──────────────┬───────────────────┘   └───────────┬──────────────┘
               │  UniFFI typed bindings            │ direct Rust API
┌──────────────▼───────────────────────────────────▼──────────────┐
│                   libant (cdylib + staticlib)                   │
│                                                                 │
│  ┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │  Public API   │  │  Config / Keys   │  │  Event / Status  │  │
│  └───────┬───────┘  └────────┬─────────┘  └────────┬─────────┘  │
│          │                   │                     │            │
│  ┌───────▼─────────────────────────────────────────▼────────┐   │
│  │                       Node Core                          │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐   │   │
│  │  │ Retrieval│ │ Pushsync │ │  Hive    │ │ Accounting │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └────────────┘   │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐   │   │
│  │  │   BMT    │ │ Mantaray │ │  Stamps  │ │    SWAP    │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐   │
│  │   libp2p       │  │  Gnosis RPC    │  │  Local Storage   │   │
│  │  (TCP/Noise/   │  │  (alloy +      │  │  (SQLite +       │   │
│  │   Yamux/Kad)   │  │   JSON-RPC)    │  │   chunk cache)   │   │
│  └────────────────┘  └────────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

`antd` and the mobile apps share the exact same `libant` core; only the outer shell differs. This keeps protocol behavior identical across dev and production and makes `antd` a faithful interop target.

---

## 3. Technology Stack

### Language & tooling

- **Rust** (MSRV: pin to latest stable minus one at start of project).
- **cargo workspaces** for cleanly separated crates.
- **`cross`** for Linux/ARM cross-compilation (Raspberry Pi, ARM servers, musl static builds).
- **`cargo-ndk`** for Android builds (aarch64, armv7, x86_64 for emulator).
- **`cargo-lipo` / xcframework tooling** for iOS (aarch64-apple-ios, aarch64-apple-ios-sim, x86_64-apple-ios-sim).
- **GitHub Actions native runners** for macOS (Intel + Apple Silicon), Linux, and Windows.
- **UniFFI (latest stable)** for Swift + Kotlin binding generation from one `.udl` file. Mobile only — desktop / server binaries depend on `ant-node` directly.
- **Nix flake** (optional but recommended) for reproducible toolchain across engineers + CI.

### Core crates

| Concern | Crate | Notes |
|---|---|---|
| libp2p | `libp2p` | Features: `tcp`, `dns`, `noise`, `yamux`, `identify`, `kad`, `request-response`, `macros`. Nothing else. |
| Async runtime | `tokio` | Single-threaded flyweight: `rt`, `macros`, `net`, `time`, `sync`, `fs`. |
| Ethereum | `alloy-*` | Modular; pull only `rpc-client`, `primitives`, `signer-local`, `sol-types`, `contract`, `consensus`. Avoid `ethers-rs` (monolithic). |
| Crypto | `k256`, `sha3`, `sha2`, `hmac`, `hkdf`, `chacha20poly1305` | `k256` keeps us inside RustCrypto, no OpenSSL. |
| Protobuf | `prost` + `prost-build` | For pushsync / retrieval / hive messages. |
| Storage | `rusqlite` (bundled) | Plus optional `sqlcipher` for encryption at rest. |
| Serde | `serde`, `serde_json`, `bincode` | JSON for RPC; bincode for internal persistence. |
| RLP | `alloy-rlp` | Smaller than the standalone `rlp` crate. |
| Bindings | `uniffi`, `uniffi-bindgen` | Mobile only. Swift + Kotlin from one IDL. |
| Error handling | `thiserror` (lib), `anyhow` (binary/tests only) | Never leak `anyhow` across FFI. |
| Tracing | `tracing`, `tracing-subscriber` | Wire through to OSLog / logcat / stdout per platform. |
| Config | `figment` or hand-rolled | Keep it tiny. |
| Desktop key storage | `keyring` (feature-gated) | macOS Keychain, Windows DPAPI / Credential Manager, Linux Secret Service. |
| Desktop service | `windows-service` (feature-gated) | Windows Service integration for `antd`. Linux/macOS use systemd/launchd unit files, not crate code. |

### Build-profile for size

```toml
[profile.release]
opt-level = "z"
lto = "fat"
codegen-units = 1
panic = "abort"
strip = "symbols"
debug = false
incremental = false
```

Optional further shrink (nightly): `cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target <triple>`.

### Dependency policy

**Always track the latest stable versions of all third-party dependencies.** No version ever pinned out of inertia.

- **Cargo.toml uses caret requirements** (e.g. `tokio = "1"`) so `cargo update` picks up non-breaking upgrades continuously.
- **`Cargo.lock` is committed** for reproducible builds of the binary / artifact crates (`antd`, `ant-ffi`), but we never rely on it to hold us back on library upgrades.
- **Rust toolchain:** pinned to **latest stable** in `rust-toolchain.toml`. MSRV claim is "current stable"; we do not promise old-toolchain support.
- **Dependency-hygiene PRs:** a dedicated upgrade workflow does:
  - `cargo update` (lockfile refresh) — always applied.
  - `cargo upgrade --incompatible` (from `cargo-edit`) — reviewed, applied when green.
  - Rust toolchain bump when a new stable lands.
  - Test suite + mainnet smoke must pass before the PR merges.
- **Dependabot / Renovate** generate PRs continuously for interim bumps.
- **`cargo-outdated`** in CI fails the build if any dependency is more than **one minor version** behind latest stable.
- **`cargo-deny`** enforces: no yanked versions, no duplicate major versions of the same crate, allowed licenses only, no known-advisory versions (via `cargo-audit` integration).
- **`cargo-vet`** tracks human-reviewed audits of new crate versions entering the tree. Reused for supply-chain review.
- **Exception policy:** a dependency may be held back only with a dated `# PINNED: reason, review by YYYY-MM-DD` comment in `Cargo.toml`. Anything past its review date blocks CI.
- **Single version per crate:** if two transitive deps force us onto two majors of the same crate, the upgrade is itself a blocking task — we don't carry duplicate majors into release artifacts (this is both a size and a security concern).

This policy is explicitly *in tension* with the §13 / §14 guidance to "pin versions." There is no contradiction: **we pin precisely to the latest stable, then we continuously rebase that pin onto new stable releases**. Pinning is for reproducibility and security review, not for freezing.

**Key dependencies we will carry** (versions intentionally not listed — CI enforces "latest stable"):

- `libp2p`, `tokio`, `alloy-*`, `k256`, `sha3`, `sha2`, `hmac`, `hkdf`, `chacha20poly1305`
- `prost`, `rusqlite`, `serde`, `serde_json`, `bincode`, `alloy-rlp`
- `uniffi`, `thiserror`, `tracing`, `tracing-subscriber`
- `keyring`, `windows-service` (platform-gated)

If you're reading this file to pick versions for a `Cargo.toml`, **do not copy version numbers out of this plan** — run `cargo search <crate>` or check `crates.io` at the moment you're adding the dependency.

### Platform matrix

The same `libant` core compiles to every target below. Platform-specific behaviour lives behind cargo features on the outer crates (`ant-ffi` for mobile, `antd` for desktop/server) and on the key-storage backend selection — **never** inside protocol crates.

| Class | Target triple | Artifact | Tier | Notes |
|---|---|---|---|---|
| **iOS device** | `aarch64-apple-ios` | `.xcframework` via `ant-ffi` | 1 | ≤ 10 MB stripped budget. |
| **iOS simulator (Apple Silicon)** | `aarch64-apple-ios-sim` | `.xcframework` | 1 | Packed into same `.xcframework` as device slice. |
| **iOS simulator (Intel)** | `x86_64-apple-ios` | `.xcframework` | 2 | Kept only as long as Intel Macs are on support. |
| **Android arm64** | `aarch64-linux-android` | `.aar` via `ant-ffi` | 1 | ≤ 10 MB stripped budget. |
| **Android armv7** | `armv7-linux-androideabi` | `.aar` | 1 | Older devices; same budget. |
| **Android x86_64** | `x86_64-linux-android` | `.aar` | 2 | Emulator-only in practice. |
| **macOS Apple Silicon** | `aarch64-apple-darwin` | `antd` binary + `.dmg` | 1 | Signed + notarized. |
| **macOS Intel** | `x86_64-apple-darwin` | `antd` binary + `.dmg` | 1 | Universal binary via `lipo`. |
| **Linux x86_64 (glibc)** | `x86_64-unknown-linux-gnu` | `antd` binary + `.deb` / `.rpm` | 1 | Default server target. |
| **Linux x86_64 (musl)** | `x86_64-unknown-linux-musl` | static `antd` | 1 | For containers / scratch images. |
| **Linux aarch64 (glibc)** | `aarch64-unknown-linux-gnu` | `antd` binary + `.deb` | 1 | **Raspberry Pi 4/5, Pi Zero 2 W, ARM servers** (64-bit OS). |
| **Linux aarch64 (musl)** | `aarch64-unknown-linux-musl` | static `antd` | 2 | Portable across distros. |
| **Linux armv7** | `armv7-unknown-linux-gnueabihf` | `antd` binary | 3 | 32-bit Raspberry Pi OS on Pi 2/3/4. Best-effort. |
| **Windows x86_64** | `x86_64-pc-windows-msvc` | `antd.exe` + MSI | 1 | Requires VS build tools on builder. |

**Tiers:**

- **Tier 1 — supported:** built in CI on every PR, tested against mainnet, shipped as signed artifacts.
- **Tier 2 — supported, reduced coverage:** built in CI, shipped but without per-PR blocking tests.
- **Tier 3 — best-effort:** not in CI by default; accept patches; no release guarantee.

### CI build matrix

Single GitHub Actions workflow, one job per (target, channel) pair:

- **Per-PR (fast):** `clippy`, `rustfmt`, `cargo test` on `x86_64-unknown-linux-gnu` + `aarch64-apple-darwin`; cross-compile smoke-build on `aarch64-linux-android` and `aarch64-apple-ios` to catch target-specific regressions.
- **Per-merge-to-main (full):** the full Tier-1 matrix above, producing release artifacts as build artifacts (unsigned). Tier-2 targets built and archived. Mainnet interop suite executed against the `x86_64-unknown-linux-gnu` build of `antd`.
- **Release tag:** produces signed + notarized artifacts for every Tier-1 platform, SBOM, and checksums.

### Per-platform key-storage backend

The `KeyProvider` abstraction (§5.10) is the same on every platform; only the default backend differs:

| Platform | Default backend | Crate |
|---|---|---|
| iOS | Secure Enclave + Keychain | `security-framework` |
| Android | StrongBox / Keystore | JNI bridge via `ant-ffi` |
| macOS | Keychain | `keyring` |
| Windows | DPAPI / Credential Manager | `keyring` |
| Linux desktop | Secret Service (GNOME / KDE) | `keyring` |
| Linux headless / Pi / containers | Encrypted keystore file (Argon2id + AES-GCM), unlocked by passphrase or systemd-creds | in-tree, `ant-crypto` |
| All (optional) | Hardware wallet (Ledger / Trezor) | `hidapi`-based, v2 |

The headless backend is the only one we own end-to-end; the rest are thin adapters over OS APIs. The `KeyProvider` interface guarantees that private key bytes never exist in `libant` memory — all signing is a callback.

### Desktop packaging

Delivered alongside the Tier-1 builds:

- **macOS:** universal `antd` binary, `.dmg` with optional menu-bar wrapper, code-signed + notarized. `launchd` `.plist` template for always-on use.
- **Windows:** MSI installer with an option to register a Windows Service. Authenticode-signed.
- **Linux:** `.deb` + `.rpm` + tarball. `systemd` unit file, `ant` user + `/var/lib/ant` data dir, `ExecStart=/usr/bin/antd --config /etc/ant/antd.toml`.
- **Raspberry Pi:** same `.deb` as Linux aarch64, plus an opinionated install script (`curl | sh`) that handles first-run key setup and optional Tailscale onboarding for "phone delegates to home Pi" scenarios.
- **Docker:** multi-arch image (`linux/amd64`, `linux/arm64`) built from the musl static binary, under ~15 MB.

---

## 4. Workspace Layout

```
ant/
├── Cargo.toml                  # workspace root
├── rust-toolchain.toml         # pinned toolchain
├── crates/
│   ├── ant-crypto/             # secp256k1, keccak, BMT, signatures, overlay derivation
│   ├── ant-p2p/                # libp2p host, BZZ handshake, Hive hints, peer snapshot
│   ├── ant-control/            # local daemon control protocol and socket transport
│   ├── ant-node/               # high-level orchestrator: wires active crates together
│   ├── antd/                   # standalone daemon: CLI, config loader, signal handling
│   └── antctl/                 # local control client and live status TUI
│
│   # Planned after M1.1:
│   ├── ant-store/              # SQLite schema + migrations + repositories
│   ├── ant-chunk/              # Chunk types, serialization, content-addressed chunks, SOCs
│   ├── ant-mantaray/           # Manifest (binary trie) construction + traversal
│   ├── ant-retrieval/          # Retrieval protocol (client only)
│   ├── ant-pushsync/           # Pushsync protocol (client only)
│   ├── ant-accounting/         # Peer debit/credit ledger + threshold logic
│   ├── ant-chain/              # Gnosis RPC, ABIs, tx signing, nonce mgmt
│   ├── ant-stamps/             # Batch state, per-chunk signing, bucket tracking
│   ├── ant-swap/               # Chequebook deploy, cheque issuance, EIP-712 signing
│   ├── ant-gateway/            # Bee-shaped HTTP API (feature-gated, antd only — see Appendix C)
│   └── ant-ffi/                # UniFFI bindings: public API surface, error mapping
├── uniffi/
│   └── ant.udl                 # Interface definition consumed by ant-ffi
├── ios/                        # Xcode project that consumes the xcframework
├── android/                    # Gradle module that consumes the aar
└── xtask/                      # cargo xtask for build orchestration
```

Each crate owns its data model and exposes a small API. `ant-node` is the only crate that knows about everything. `ant-control` is local operator plumbing only and must not leak into protocol crates. `ant-ffi` is the only crate that knows about UniFFI. `antd` is a thin shell around `ant-node` that adds CLI argument parsing, config loading, and signal handling — it must never reach around the core to touch lower crates directly, so that the daemon and the mobile library remain behaviorally identical.

---

## 5. Protocol Scope — What We Actually Build

### 5.1 libp2p host

- Single TCP transport, IPv4 + IPv6.
- Security: Noise XX.
- Muxer: Yamux.
- Identify protocol.
- Kademlia for forwarding-Kademlia neighborhood discovery.
- `request-response` behavior for retrieval / pushsync framing.
- **Peer ID = libp2p identity** derived from secp256k1 node key (compatible with bee's overlay derivation).

### 5.2 Overlay + Hive

- Overlay address = `keccak256(eth_address || network_id || nonce)`, truncated per Swarm spec.
- Hive v2 peer exchange: request peers close to our overlay depth, maintain routing table bins.
- Implement neighborhood depth tracking (simplified: we only need enough peers to route retrieval / pushsync targets).

### 5.3 Retrieval (client)

- Single-stream protobuf request/response.
- Verify chunk integrity: `keccak256(span || payload) == reference` for content-addressed chunks; signature verification for single-owner chunks (SOCs).
- Optional in-memory LRU cache keyed by chunk reference; SQLite-backed persistent cache behind a size cap.

### 5.4 BMT + Chunking

- Chunk size: 4096 bytes payload + 8-byte span.
- BMT hash over 128-segment binary Merkle tree using keccak256.
- Streaming chunker that turns arbitrary byte input into a tree of chunks, yielding the root reference.
- Handle small files (single chunk), normal files, and large files (multi-level intermediate chunks).

### 5.5 Mantaray (manifests)

- Binary trie encoding for path → reference mapping.
- Minimum viable: construct a manifest from a flat list of `(path, reference, metadata)` entries and traverse by path.
- Support "root metadata" node for website-style manifests (index document, error document) — needed for `bzz://` resolution.

### 5.6 Pushsync (client only)

- Push chunk → nearest neighbors via forwarding-Kademlia routing.
- Receive signed receipt from storer peer; verify signature corresponds to overlay.
- Retry with alternative routing on timeout / bad receipt.
- Account each peer hop for SWAP (see §5.8).

### 5.7 Postage stamps

- `PostageStamp` contract on Gnosis Chain: buy batch, top-up, dilute, read batch state.
- Local persistence of each batch: `batch_id`, `owner_key`, `depth`, `bucket_depth`, `amount`, `normalized_balance`, `buckets[]` (bucket counters).
- Per-chunk stamp: sign `keccak256(chunk_addr || batch_id || index || timestamp)` with batch owner key.
- Bucket tracking: maintain per-bucket counters so we never exceed `2^bucket_depth` stamps per bucket.
- Expiry calculation: `stamp_alive = normalized_balance > current_total_outpayment`. Poll chain periodically; warn app before expiry.

### 5.8 SWAP / Chequebook

- `ChequebookFactory` → deploy per-device chequebook at first run. Chequebook holds xBZZ used to pay forwarders.
- `Chequebook` contract: we only call `increaseBalance` and issue off-chain cheques; cashing is the counterparty's job.
- Off-chain accounting per peer: `paymentThreshold`, `disconnectThreshold`, `earlyPayment` logic per bee spec.
- Cheque signing: EIP-712 typed data: `(chequebook, beneficiary, cumulativePayout)` signed by chequebook owner key.
- Persist outstanding cheques; re-issue the highest cumulative payout on peer reconnect.

### 5.9 Gnosis Chain plumbing

- JSON-RPC client over HTTPS using `alloy-rpc-client`.
- ABI encoding for: `ERC20` (xBZZ, xDAI balance checks, approvals), `PostageStamp`, `ChequebookFactory`, `Chequebook`.
- Transaction signing: EIP-155 legacy + EIP-1559; default to EIP-1559 on Gnosis.
- Nonce manager: local monotonic counter, re-sync on reorgs / rejections.
- Gas estimation with a small safety multiplier; fee-bumping on stuck txs.
- Confirmation waiter: configurable block depth (default 2 on Gnosis).

### 5.10 Key management

- Three keys per device:
  1. **Node key** (secp256k1): libp2p identity + overlay.
  2. **Wallet key** (secp256k1): pays gas, owns chequebook, signs cheques.
  3. **Batch owner key(s)** (secp256k1): signs postage stamps. Often same as wallet key, but modelled separately so batches can be imported.
- Keys live **outside the Rust lib**: mobile app provides them to the library via a `KeyProvider` trait exposed through UniFFI, backed by:
  - iOS: Secure Enclave + Keychain
  - Android: StrongBox / Keystore (hardware-backed when available)
- Rust library never persists raw private keys; it only holds public keys + signs through the callback.

---

## 6. Data Model & Persistence

Single SQLite database, versioned migrations. Core tables:

| Table | Purpose |
|---|---|
| `config` | Key-value app config, schema version. |
| `peers` | Known peers, overlay, multiaddrs, last-seen, reputation counters. |
| `chunks` | Content-addressed chunk cache (payload blob + last-access). |
| `batches` | Owned / imported postage batches. |
| `batch_buckets` | Per-batch bucket counters (sharded table). |
| `cheques_issued` | Per-peer latest cumulative payout. |
| `cheques_received` | Stored for audit even though we don't cash on v1. |
| `chain_state` | Latest block, nonce, pending tx tracking. |
| `uploads` | User-level upload jobs with status, reference, manifest metadata. |

Chunks may optionally live on disk (one file per chunk in sharded dirs) if we find SQLite BLOB pressure is significant under load.

---

## 7. Public API (FFI Surface)

Keep the UniFFI surface small and stable. Sketch:

```idl
namespace ant {};

interface Node {
  [Throws=AntError]
  constructor(Config config, KeyProvider keys);

  void start();
  void stop();
  NodeStatus status();

  // retrieval
  [Throws=AntError] bytes download(Reference reference);
  [Throws=AntError] Reference resolve_path(Reference manifest, string path);

  // upload
  [Throws=AntError] UploadHandle upload_bytes(bytes data, UploadOptions opts);
  [Throws=AntError] UploadHandle upload_manifest(sequence<ManifestEntry> entries, UploadOptions opts);

  // stamps
  [Throws=AntError] Batch buy_batch(u8 depth, string amount_plur);
  sequence<Batch> list_batches();
  [Throws=AntError] void topup_batch(BatchId id, string amount_plur);
  [Throws=AntError] void dilute_batch(BatchId id, u8 new_depth);

  // swap
  [Throws=AntError] ChequebookInfo chequebook();
  [Throws=AntError] void deploy_chequebook(string deposit_plur);

  // events
  void set_event_listener(EventListener listener);
};

callback interface KeyProvider {
  bytes public_key(KeyRole role);
  bytes sign(KeyRole role, bytes digest);
};

callback interface EventListener {
  void on_event(AntEvent event);
};
```

All amounts that cross FFI are strings (PLUR / wei) to avoid precision loss; no `BigInt` in UniFFI.

---

## 8. UX & Onboarding Design (mandatory before coding uploads)

The product has to solve three onboarding problems or uploads will be unusable on day one:

### 8.1 Funding ceremony

- User needs xDAI (gas) + xBZZ (stamps + chequebook deposit) on Gnosis Chain.
- **Recommended pattern:** app backend runs a *relayer* that:
  - Funds the user's wallet with a minimal xDAI bootstrap (once, per device, rate-limited).
  - Optionally sells xBZZ in-app (fiat on-ramp via a third-party provider) or sponsors first batch.
- Power users: accept an import flow for existing wallets.

### 8.2 Chequebook pre-deploy

- Chequebook deploy is a blocking tx before the first upload. Options:
  1. **Ship a sponsored deploy** via the relayer: relayer fronts gas, user signs the deploy via EIP-2771 meta-tx, chequebook ends up owned by the user. Best UX.
  2. **Deploy on first app launch** with a clear progress UI ("Preparing your storage…"). Acceptable fallback.
  3. **Background deploy** right after wallet funding, before the user even tries to upload.

### 8.3 Stamp UX

- Never surface `depth`, `amount`, `bucket_depth` in the primary UI.
- Offer presets: "Quick share (1 day, ~1k files)", "Long-term (1 year, ~100k files)", etc. Compute parameters behind the scenes.
- Show expiry as a calendar date with a colored status (green / yellow / red).
- Auto top-up at a user-configurable threshold, with a hard-limit per day to prevent runaway spend.

### 8.4 Background behavior

- iOS: use BGProcessingTask / BGAppRefreshTask for periodic chain polling and chunk-upload resumption.
- Android: WorkManager foreground service for active uploads, regular workers for polling.
- The Rust lib must support **graceful suspend/resume**: save state, close sockets, drop memory; reconnect on resume without re-downloading state from chain.

### 8.5 Key recovery

- Export / import using BIP-39 seed phrase for the wallet key (optionally the node key too).
- Cloud backup is **opt-in** and must be explicit; default is local-only.

---

## 9. Phased Delivery Plan

The work is organized around **three capability milestones**, each of which is independently demoable and, in the case of M1 and M2, independently *shippable* as an internal or external product in its own right:

1. **M1 — Peer discovery & connection.** The node joins the Swarm network and maintains a healthy peer set. No data movement yet. Proves the hardest infrastructure piece (libp2p + overlay + BZZ handshake + Kademlia) in isolation. Split into **M1.0** (basic mainnet connection via `antd`) and **M1.1** (stable neighborhood via Hive + Kademlia).
2. **M2 — Ultra-light node (download only).** The node retrieves any mainnet reference. Shippable as a standalone "Ant Reader" app. No chain, no keys beyond libp2p identity, no xDAI/xBZZ required.
3. **M3 — Light node (uploads + stamps + SWAP).** Full read/write node with on-device stamp management and honest payments. The GA product.

A post-M3 **hardening track** covers UX polish, backgrounding, and beta — without which the product isn't fit to ship, but where the protocol risk is already retired.

Each milestone is subdivided into phases that end with a concrete demo.

---

### Milestone 1 — Peer discovery & connection

Goal: the embedded node connects to Swarm mainnet, completes the BZZ handshake with real bee peers, exchanges peers via Hive, and maintains Kademlia routing table bins consistent with the neighborhood of our overlay address. All development at this stage happens against the `antd` binary; mobile packaging is deferred to the end of M2 where it's meaningful to demo.

#### Phase 0 — Foundations + basic mainnet connection — **M1.0**

Keep the workspace tiny: only the crates needed to dial bee peers, survive the BZZ handshake, expose enough operator control to debug a long-running daemon, and gather the peer data needed for M1.1. No SQLite, no UniFFI, no mobile artifacts yet.

- Workspace skeleton with `ant-crypto`, `ant-p2p`, `ant-node`, `ant-control`, `antd`, and `antctl`.
- CI (GitHub Actions), lint (`clippy -D warnings`), format (`rustfmt`), pre-commit.
- `ant-crypto`: secp256k1 key handling, keccak256, Ethereum address derivation, overlay address derivation (`keccak256(eth_address ‖ network_id_le ‖ nonce)`), BZZ address signing + verification. Unit tests against vectors cross-checked with a local `bee`.
- `ant-p2p`:
  - libp2p host using **secp256k1 identity** (not ed25519), TCP + DNS transports, Noise XX, Yamux, Identify, Ping.
  - `/swarm/handshake/14.0.0/handshake` protobuf implementation (Syn / SynAck / Ack), including bee-compatible half-close behavior.
  - Recursive `/dnsaddr/mainnet.ethswarm.org` resolution and grouped peer dials.
  - Connection lifecycle: reconnect with bounded exponential backoff, log handshake outcome with overlay + agent version, and maintain a target peer pipeline.
  - Drain bee post-handshake pricing streams and parse Hive peer broadcasts into dial hints.
  - Persist a temporary JSON peer snapshot for warm restarts until `ant-store` replaces it.
- `antd`: CLI with `--data-dir`, `--network-id`, `--bootnodes`, `--log-level`, `--key-file`, `--external-address`, control-socket, and peerstore options; generates a persistent node key + nonce on first run; `tracing-subscriber` for human-readable logs.
- `antctl`: `status`, `top`, `version`, and `peers reset` for live daemon inspection.

**Exit (M1.0):** `antd` running on a laptop connects to live mainnet bee peers, completes the BZZ handshake, fills at least a small peer set from bootnode/Hive hints, survives a restart from the JSON peer snapshot, and stays connected across network blips. Log output matches roughly:

```
INFO antd: loaded identity eth=0x1a2b…c4 overlay=0x7f3e…91
INFO ant_p2p: dialing /ip4/…/tcp/1634/p2p/16Uiu2HAm…
INFO ant_p2p: libp2p connected agent="bee/2.7.0"
INFO ant_p2p: bzz handshake ok overlay=0x2c4a…8e full_node=true
INFO ant_p2p: peer set size=1
```

#### Phase 1 — Stable neighborhood via Hive + Kademlia — **M1.1**

- Extend `ant-p2p` with:
  - **Hive v2** peer exchange: keep the current inbound Hive peer broadcast parser, add any missing request path needed to actively ask peers for closer addresses, verify signed BZZ addresses before promotion, and enqueue dials by routing need instead of FIFO only.
  - **Forwarding Kademlia** routing: bin-based routing table keyed on XOR distance from our overlay; neighborhood depth tracking; pluggable peer-selection for later protocols.
  - Peer health and replacement policy: distinguish dial failure, handshake failure, post-handshake disconnect, and long-lived healthy peer.
- `ant-store`: minimal SQLite schema for peers, routing snapshot, and `config` key-value. Migrate the current JSON peer snapshot API behind the store so `antd` behaviour stays stable.
- `ant-node`: orchestrator wiring that owns the node loop, applies backpressure, and surfaces status.
- Foundational cross-compile pipeline: produce stub `.xcframework` + `.aar` from a "hello-world" UniFFI crate so M2 can hit the ground running on mobile.
- Operator acceptance:
  - `antctl top` shows connected peers, selected-peer detail, last BZZ handshake, and routing/neighborhood summary.
  - `antctl peers reset` and daemon startup `--reset-peerstore` remain supported after the SQLite migration.
  - A 24-hour soak script writes a compact acceptance report: peer count over time, routing-bin coverage, reconnects, handshakes, and disconnect reasons.

**Exit (M1.1):** `antd` run for 24 hours against mainnet maintains a stable peer set across suspend/resume, and its Kademlia routing bins match a parallel `bee` node's view of the same neighborhood within a small tolerance. This is the real M1 checkpoint — after this we trust the infrastructure.

---

### Milestone 2 — Ultra-light node: download only

Goal: the node retrieves any reference from mainnet, including multi-chunk files and path-addressed content inside a mantaray manifest. No chain interaction, no wallet. This is a complete, shippable read-only product.

#### Phase 2 — Retrieval

- `ant-retrieval`: client protocol, protobuf messaging, stream multiplexing over libp2p `request-response`.
- Chunk verification: BMT root check for content-addressed chunks; signature check for single-owner chunks.
- Chunk cache (SQLite or sharded file store) with size cap and LRU eviction.
- Retry / alternate-peer selection on failure.

**Exit:** Mobile sample app downloads a known single-chunk and multi-chunk Swarm reference from mainnet.

#### Phase 3 — Mantaray traversal + high-level download API

- `ant-mantaray`: binary-trie decoder, path traversal, root-metadata handling.
- `download(Reference)` → bytes, and `resolve_path(Reference, String)` → Reference, both over FFI.
- Streaming download API so large files don't require holding the whole payload in memory.

**Exit:** Sample app opens a `bzz://` reference with a path and renders the content (e.g., an image or a JSON file inside a directory manifest).

#### Phase 4 — Ultra-light packaging + release-candidate

- Size budget enforcement in CI (fail PRs that push `.xcframework` / `.aar` past the M2 budget).
- Foreground + background suspend/resume for reads.
- Observability bridges (OSLog / logcat).
- Tier-A `ant-gateway` shipped with `antd` (Appendix C): bee-shaped read endpoints, stub wallet/stamps/chequebook, `beeMode = "ultra-light"`. Mobile artifact unchanged (gateway is feature-gated off in `ant-ffi`).
- Internal beta of the download-only build.

**Exit (M2):** Independently shippable ultra-light library. Per-platform artifact ≤ ~5 MB stripped (lighter than the full M3 target because no chain / SWAP / stamps code).

---

### Milestone 3 — Light node: uploads + stamps + SWAP

Goal: everything in M2, plus honest on-device uploading with on-chain postage-stamp management and SWAP-based payment to forwarders.

#### Phase 5 — Chain plumbing, reads first

- `ant-chain`: alloy-based RPC client, ABIs for `ERC20`, `PostageStamp`, `ChequebookFactory`, `Chequebook`.
- Nonce manager, confirmation waiter, fee-bumping scaffolding (not yet exercised).
- `KeyProvider` FFI contract finalized; iOS Keychain + Android Keystore backends wired in.
- Read-only flows: show wallet balances, read an existing batch, read chequebook state.

**Exit:** Mobile app reads and displays an on-chain batch and chequebook for the embedded wallet. No writes yet.

#### Phase 6 — Single-chunk upload with pre-provisioned stamps

- `ant-pushsync`: client protocol, receipt verification.
- `ant-stamps`: per-chunk signing, bucket counters, atomic persistence. Batch injected via configuration — no on-chain purchase yet.
- Accounting stub: record peer debits but do not yet pay.

**Exit:** Upload a single chunk to mainnet with a pre-provisioned batch. End-to-end roundtrip: upload → reference → download via the same embedded node.

#### Phase 7 — SWAP

- `ant-accounting`: per-peer ledger, payment / disconnect thresholds, early-payment logic.
- `ant-swap`: chequebook deploy via factory, EIP-712 cheque signing, cheque issuance on threshold, re-issue on reconnect.
- Integrate with pushsync so every forwarder hop is paid honestly.

**Exit:** Upload ~1 GB of chunks via pushsync while paying peers; peers never disconnect us for non-payment.

#### Phase 8 — On-device stamp lifecycle

- On-chain batch purchase, top-up, dilute.
- Expiry monitoring and app-level events.
- Harden bucket-counter persistence against crash mid-upload (write counter before emitting the chunk to the wire).

**Exit:** End-to-end user flow: buy a batch, upload files, top it up, dilute it — all initiated from the device.

#### Phase 9 — Streaming uploads + mantaray construction

- Streaming chunker with on-the-fly BMT tree construction.
- `ant-mantaray` construction path (previously we only traversed).
- `upload_manifest` API for multi-file directory uploads.
- Tier-B `ant-gateway` endpoints (Appendix C) come online incrementally with the underlying capability: `/wallet`, `/chequebook/*` real after Phase 7; `/stamps` writes after Phase 8; `/bzz` POST, `/bytes` POST, `/chunks*`, `/tags*`, `/feeds/*`, `/soc/*`, `/stewardship/*`, `/envelope/*` after Phase 9. `beeMode` flips to `"light"` once `swap-enable` is honored end-to-end.

**Exit (M3):** Upload a multi-file directory as a single mantaray manifest and retrieve files by path — full read/write parity with a `bee` light node for the supported protocol subset.

---

### Hardening track

Not a capability milestone, but required before GA.

#### Phase 10 — UX, background, resilience

- Graceful suspend/resume on iOS + Android (sockets, timers, chain pollers).
- Upload resumption across app kill / relaunch.
- Onboarding flow wired: relayer-sponsored funding, chequebook pre-deploy, stamp presets.
- Nonce-race resilience, reorg handling, fee-bumping in anger.
- Crash recovery paths for every durable state transition.

**Exit:** App survives being killed mid-upload, relaunched, and finishes the upload. Cold-start to "ready to upload" under 30 s for a returning user.

#### Phase 11 — Interop, beta, polish

- Continuous interop against latest `bee` mainnet release in CI.
- External beta with opt-in telemetry (local-first, no PII).
- Performance / battery / data profiling; iterate.
- Documentation pass: integration guide, API reference, onboarding playbook.

**Exit:** Public beta on both stores.

---

### Milestone summary

| Milestone | Phases | Demo |
|---|---|---|
| **M1.0 Basic mainnet connection** | 0 | `antd` connects to mainnet + completes BZZ handshake |
| **M1.1 Stable neighborhood** | 1 | Stable peer set + correct Kademlia routing bins over 24 h |
| **M2 Ultra-light (download)** | 2, 3, 4 | Download any reference + path resolution; shippable read-only build |
| **M3 Light node (upload + stamps + SWAP)** | 5, 6, 7, 8, 9 | Buy batch → upload directory → retrieve by path, all on device |
| **Hardening** | 10, 11 | Public beta with survived-kill uploads and mainnet interop |

M1 and M2 can be parallelized by a second engineer starting on chain plumbing in Phase 5. Add buffer for unknowns — Swarm's protocols have undocumented edges, and the first honest interop run against `bee` always surfaces something.

### Optional early releases

The milestone structure deliberately supports three pre-GA checkpoints, each useful in its own right:

- **End of M1.0:** **`antd` dogfood release.** Internal-only daemon that peer with mainnet. Zero user value but huge team morale and infrastructure validation signal: a handful of engineers run `antd` on their laptops, file bugs, and the BZZ handshake hardens fast against the wild variety of bee peers out there.
- **End of M1.1:** **internal "Ant Probe" dev tool.** Still no user-facing value, but now provides a stable mainnet-connected node that retrieval work (M2) can be built on top of with confidence. Good point to onboard additional engineers.
- **End of M2:** **shippable public "Ant Reader" app / SDK.** Lets you learn UX and distribution without having to solve the xDAI/xBZZ/chequebook problem yet. Highly recommended — de-risks the hardest *product* problems before you finish the hardest *protocol* problems.

---

## 10. Testing Strategy

### Unit tests

- Cryptography: BMT against bee test vectors; SOC signatures; cheque EIP-712 hashes.
- ABI encoding against known tx hashes.
- Mantaray round-trip on generated trees.
- Bucket counter state machines (property tests with `proptest`).

### Integration tests

- Golden-path interop against a **locally-run bee node** in a Docker container. This is the single most important test surface.
- Each protocol (hive, retrieval, pushsync) has a harness that speaks to a bee container.
- Chain tests against a **local Gnosis dev chain** (`anvil --fork <rpc>` against an archived block, or `nethermind --init`).

### End-to-end tests

- Headless mobile test app driving upload/download scenarios.
- Fuzzing: protobuf decoders with `cargo-fuzz`; ABI decoders; manifest parsers.

### Mainnet smoke tests

- CI job that does one download of a known reference and one small upload against mainnet. Alert on regression.

### Device matrix

- iOS: oldest-supported device (aim for iOS 16+), plus current flagship.
- Android: low-end (4 GB RAM / arm64) + flagship, plus emulator for CI.

---

## 11. Observability

- `tracing` everywhere; spans for upload jobs, downloads, chain calls.
- FFI exposes a `set_log_sink` so the host app bridges:
  - iOS: `OSLog`
  - Android: `android.util.Log` via `log` crate + `android_logger`
- Structured events for UI: `AntEvent::UploadProgress { job, bytes, total }`, `ChequeIssued`, `BatchExpiring`, etc.
- **No telemetry to our backend by default.** Opt-in only, anonymized.

---

## 12. Size Budget

| Component | Target (stripped) |
|---|---|
| libp2p (TCP/Noise/Yamux/Kad) | ≤ 2.5 MB |
| Protocols (retrieval, pushsync, hive, BMT, mantaray) | ≤ 1.0 MB |
| Stamps + SWAP + accounting | ≤ 0.7 MB |
| Alloy + RPC + ABI + signing | ≤ 1.0 MB |
| Tokio (single-threaded) + prost + sqlite | ≤ 2.5 MB |
| Crypto (k256, sha3, sha2, chacha20) | ≤ 0.6 MB |
| Glue + UniFFI scaffolding | ≤ 0.5 MB |
| **Total per platform** | **≤ 8.8 MB** |

Tracked as a CI check that fails the build if `.xcframework` or `.aar` grows past the threshold for a release build.

---

## 13. Security Considerations

- **Private keys never leave the secure hardware.** All signing through the `KeyProvider` callback; Rust holds only public keys.
- **No untrusted deserialization into `Vec<u8>`** without size caps; protobuf messages have explicit max-size limits.
- **Chain call allowlist:** only the specific contracts we know about; refuse generic `eth_call` surface from the host app.
- **Stamp replay / over-issue:** bucket counters persisted atomically before the chunk leaves the device; stamp signatures include monotonic bucket index.
- **Cheque replay:** cumulative payout is monotonically increasing per peer; we never sign a cheque with a lower value than previously sent.
- **Key compromise:** wallet key controls chequebook funds + batches. Offer a panic-withdraw flow to drain the chequebook to a recovery address.
- **Supply chain:** audit crates via `cargo-vet`, gate with `cargo-deny` and `cargo-audit`, reproducible builds via committed `Cargo.lock`. Versions pinned in the lockfile are continuously rebased onto latest stable per the §3 dependency policy — we never freeze dependencies for long.

---

## 14. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Upstream protocol changes in `bee` | High | Medium | Track bee releases; mainnet interop tests in CI; version the wire format. |
| Forwarding Kademlia bugs | Medium | High | Strong integration test harness against live bee; conservative initial peer-selection. |
| iOS background-kill during uploads | High | Medium | Resumable upload state in SQLite; BGProcessingTask. |
| Gnosis Chain reorgs | Low | Medium | Wait for N confirmations (configurable); idempotent tx retry. |
| Stamp over-issuance leading to chunk rejection | Medium | High | Atomic bucket-counter writes; refuse upload if counter can't be persisted first. |
| xBZZ / xDAI acquisition blocks new users | High | High | Relayer-sponsored onboarding; clear "bring your own" path for power users. |
| Artifact size creeps past budget | Medium | Low | CI gate; per-crate size regression dashboard. |
| Libp2p API churn | Medium | Low | Continuous upgrade policy (§3); dedicated upgrade PRs for breaking changes; interop smoke tests gate the merge. |
| Bee HTTP API drifts faster than we track | Medium | Medium | Pinned reference bee version per release (§C.5); golden-response CI corpus (§C.6); 501-stub policy (§C.1) keeps consumers from silently degrading. |

---

## 15. Upstream Tracking

- Subscribe to `ethersphere/bee` releases.
- Maintain a compatibility matrix: our version ↔ bee versions we're known to interop with.
- Participate in Swarm community channels for early warning on protocol changes.
- Keep a "spec conformance" doc mapping each bee spec section to our implementation location.

---

## 16. Open Questions

1. **Do we support encrypted uploads in v1?** Adds ChaCha20 handling + key embedding in reference. Small code cost; larger UX cost (key sharing). Recommend: deferred to v1.1.
2. **Single wallet or separate node/wallet keys?** Simpler UX with one; cleaner isolation with two. Recommend: two keys, shared recovery seed.
3. **Does the app always run its own node, or can it fall back to a gateway?** Fallback is nice but doubles the retrieval code path. Recommend: node-only, ship the smallest node possible.
4. **xBZZ on-ramp inside the app or external?** External first to avoid regulatory overhead; revisit in v2.
5. **Windows / desktop ports?** Not in v1 scope but keep FFI clean so it's trivial later.
6. **Do we expose `ant-gateway` on a Unix domain socket as well as TCP?** Lets local consumers avoid binding the loopback port and dovetails with the existing `ant-control` socket transport. Recommend: yes, behind a `--api-socket <path>` flag, off by default.

---

## 17. Definition of Done for v1

- [ ] Mobile apps on iOS and Android can:
  - [ ] Onboard a new user including wallet funding, chequebook deploy, first batch purchase — all within a single flow under 5 minutes.
  - [ ] Download any Swarm mainnet reference.
  - [ ] Upload a single file (any size up to 4 GB) and share the reference.
  - [ ] Upload a directory as a mantaray manifest and retrieve by path.
  - [ ] Manage batches: buy, top-up, dilute, list, expiry warnings.
  - [ ] Survive app kill mid-upload and resume on next launch.
- [ ] Mainnet interop tests pass against the latest bee release.
- [ ] Per-platform artifact under 10 MB stripped.
- [ ] Key material never reaches the Rust library unencrypted.
- [ ] External security review of crypto + SWAP + stamp code.
- [ ] Documentation: integration guide, API reference, onboarding playbook for app teams.

---

## Appendix A — Bee spec sections we need to implement

- Overlay address derivation
- Binary Merkle Tree (BMT) hash
- Content-addressed chunks
- Single-owner chunks
- Mantaray manifests
- Hive v2 peer exchange
- Forwarding Kademlia routing
- Retrieval protocol
- Pushsync protocol
- Postage stamps (structure, signing, verification we don't do but must produce valid stamps)
- SWAP accounting + cheque format (EIP-712)
- Chequebook contract interface
- PostageStamp contract interface

## Appendix B — Bee spec sections we deliberately do **not** implement

- Pullsync
- Storage incentives / redistribution game
- Staking contract
- PSS / GSOC messaging
- ACT (access control trie)
- Feeds beyond basic SOC usage
- Trojan chunks
- Cheque cashing
- Advanced redundancy (erasure-coded uploads) — parked for v2

---

## Appendix C — Bee compatibility surface

Independently of any specific downstream, we want **`antd` to be a drop-in replacement for `bee` for the protocol subset Ant implements.** Tools written against `bee` — `bee-js` clients, `curl` / `xh` scripts, custom protocol handlers, monitoring dashboards, ENS-resolving gateways — must work unmodified against `antd` for any feature Ant supports, and must fail cleanly (not mysteriously) for any feature Ant does not.

This appendix is the contract for that compatibility. It is intentionally strict on shapes and intentionally narrow on scope: every endpoint listed is either implemented, will be implemented in a stated milestone, or is a permanent stub that returns a structured 501 so consumers can feature-detect.

### C.1 Compatibility tiers

Tied directly to the milestones in §9.

| Tier | Capability | Lands in | What it unlocks |
|---|---|---|---|
| **A — Read-only / ultra-light** | Read endpoints (`/bzz`, `/bytes`, `/chunks`, status, peers, topology, addresses); stub wallet/stamps/chequebook returning empty/zero in a bee-shaped way; `beeMode = "ultra-light"`. | End of **M2** | Any bee-shaped read client (browsers, gateway proxies, scripts that only download) works against `antd` with zero changes. |
| **B — Read + write / light** | All Tier A endpoints + uploads, tags, stamps (buy/topup/dilute), feeds/SOCs, chequebook deposit, wallet balances; `beeMode = "light"`. | End of **M3** | Full `bee-js` API surface for the protocol subset Ant supports; uploaders, publishing tools, and feed-driven apps work unchanged. |
| **Permanently absent** | Anything in Appendix B (PSS, GSOC, pullsync, transactions, staking, ACT, restricted/admin API). | Never | Endpoints return `501 Not Implemented` with `{code:501, message:"not implemented in ant"}` so consumers can branch. |

### C.2 Process & configuration compatibility

- **Binary entry points.** `antd` accepts the same subcommand vocabulary `bee` does for the operations downstreams script against:
  - `antd init [--config <path>]` — generates `keys/swarm.key` (see §C.3) using the password from config; idempotent if a keystore already exists.
  - `antd start [--config <path>]` — runs the daemon.
  - Existing `antd` flags remain authoritative; the subcommands accept the bee-shaped YAML config below in addition to native flags.
- **Config schema.** `--config <file.yaml>` parses the bee YAML. Recognized keys, all optional, with the same semantics bee gives them:

  ```
  api-addr                       # default 127.0.0.1:1633
  data-dir
  password                       # decrypts keys/swarm.key
  mainnet                        # default true
  full-node                      # ant always reports false
  swap-enable                    # M3+; ignored on Tier A
  blockchain-rpc-endpoint        # required when swap-enable=true
  cors-allowed-origins           # default "" (no CORS)
  resolver-options               # ENS RPC URL(s)
  skip-postage-snapshot          # accepted as no-op (we never run the snapshot)
  storage-incentives-enable      # ignored (Appendix B)
  network-id                     # default 1 (mainnet)
  bootnodes                      # additive to dnsaddr defaults
  ```

  Unknown keys are logged at `warn` and ignored. Conflicting native CLI flags win.
- **Daemon lifecycle.** Default `api-addr` is `127.0.0.1:1633` to match bee. `GET /health` answers within 50 ms of socket bind so external supervisors can poll aggressively. `SIGTERM` triggers graceful shutdown; we guarantee exit within 5 s of receiving it.
- **Process identity.** `antd` may be invoked through a `bee` symlink or wrapper script without behavior change; we never branch on `argv[0]`.

### C.3 Identity & on-disk format

- Bee's `keys/swarm.key` is the canonical identity contract: a **Web3 v3 JSON keystore** encrypted with the operator-supplied `password`. Ant adopts this format as its **only** on-disk node-key format from M1.1 onward — the existing `signing.key` / `identity.json` pair is migrated to `keys/swarm.key` on first launch when a password is available, and the legacy files are removed once migration succeeds.
- `keys/` is the only subdirectory of `data-dir` we promise to keep bee-shaped. `statestore/`, `localstore/`, etc. remain Ant-internal; we do not promise byte-level compatibility there, and bee's data-dir is **not** a valid Ant data-dir (and vice versa).
- Externally-injected identities are a first-class flow: an embedder may write `keys/swarm.key` itself (e.g. derived from a vault) and start `antd` without running `antd init`. We detect the existing keystore and skip generation.
- The Web3 v3 implementation lives in `ant-crypto` and is exercised by both `antd` (file-backed) and the desktop `keyring` backend (used as opaque blob storage on Windows / Linux headless).

### C.4 HTTP gateway crate

- New crate **`ant-gateway`** in the workspace, slotted into the "planned after M1.1" group in §4. Depends on `ant-node`; nothing else depends on it.
- Cargo feature `http-api`, **enabled by default for `antd`, disabled for `ant-ffi`** so the mobile artifact never carries the HTTP server (the size budget in §12 is unaffected).
- Built on `axum` + `hyper`, single `tokio` runtime shared with the node core. No second listener, no debug port — bee unified its API in 2.0+ and we follow that convention.
- All response bodies follow bee's `jsonhttp` shape:
  - Success: bee field names, exact casing, no wrapper envelope.
  - Error: `{code: <http-status>, message: <human-readable>, reasons?: [...]}` with the same `code`/`message` field names bee uses.
- Streaming: `/bzz/...` and `/bytes/...` stream the body as chunks are joined; `Content-Length` set when known, `Transfer-Encoding: chunked` otherwise. `Range` is honored against the joiner.
- CORS: off by default; the `cors-allowed-origins` config value, when non-empty, drives an `Access-Control-Allow-Origin` allowlist.

#### C.4.1 Request headers honored

When acting as the bee-shaped server, `ant-gateway` recognizes:

| Header | Direction | Semantics |
|---|---|---|
| `Range` | request | Standard byte ranges on `/bzz`, `/bytes`, `/chunks/<addr>`. |
| `If-None-Match` | request | ETag match against root reference; `304` on hit. |
| `Swarm-Chunk-Retrieval-Timeout` | request | Per-chunk fetch timeout cap for this request. |
| `Swarm-Redundancy-Strategy` | request | Plumbed to `ant-retrieval` redundancy selection. |
| `Swarm-Redundancy-Fallback-Mode` | request | As above. |
| `Swarm-Encrypt` | request (M3) | Encrypted upload toggle (deferred per §16; rejected with 501 until v1.1). |
| `Swarm-Pin` | request (M3) | Pin uploaded references locally. |
| `Swarm-Tag` | request (M3) | Attach upload to existing tag. |
| `Swarm-Postage-Batch-Id` | request (M3) | Batch to stamp the upload with. |
| `Swarm-Collection` | request (M3) | Treat body as tar collection → mantaray. |
| `Swarm-Index-Document` / `Swarm-Error-Document` | request (M3) | Mantaray root metadata. |
| `Swarm-Deferred-Upload` | request (M3) | Async upload; client polls tag for status. |
| `Content-Type`, `Content-Length` | request | Standard. |
| `ETag`, `Swarm-Tag-Uid`, `Swarm-Feed-Index`, `Swarm-Feed-Index-Next` | response | Set where bee sets them. |

#### C.4.2 Endpoint matrix

Methods listed are the ones we serve; bee's other methods on the same path return `405`. "Tier" gives the milestone the endpoint becomes real; rows marked **501** are permanently stubbed.

| Path | Methods | Tier | Notes |
|---|---|---|---|
| `/health` | GET | A | `{status:"ok", version, apiVersion}`. |
| `/readiness` | GET | A | `200` once routing table has ≥1 BZZ-handshaked peer; else `503`. |
| `/node` | GET | A | `{beeMode}`: `"ultra-light"` until SWAP lights up, then `"light"`. |
| `/addresses` | GET | A | `{ethereum, overlay, public_key, pss_public_key:null, underlay:[...]}`. |
| `/peers` | GET | A | `{peers: [...]}` from `ant-p2p` snapshot. |
| `/peers/{addr}` | DELETE | A | Disconnect; matches bee shape. |
| `/topology` | GET | A | `{baseAddr, population, connected, timestamp, nnLowWatermark, depth, reachability, networkAvailability, bins}`. |
| `/blocklist` | GET | A | `{peers: []}` (we don't ban yet); writeable post-M3. |
| `/connect/{multi-address:.+}` | POST | A | Manual dial; useful for tests. |
| `/pingpong/{address}` | POST | A | Round-trip latency. |
| `/bzz/{addr}` | GET, HEAD | A | Mantaray traversal via `ant-retrieval::lookup_path`. |
| `/bzz/{addr}/{path:.*}` | GET, HEAD | A | As above. |
| `/bytes/{addr}` | GET, HEAD | A | Joined byte stream via `ant-retrieval::join`. |
| `/chunks/{addr}` | GET, HEAD | A | Single-chunk fetch. |
| `/wallet` | GET | A→B | Tier A: zero balances, no chain; Tier B: real balances via `ant-chain`. |
| `/stamps` | GET | A→B | Tier A: `{stamps: []}`; Tier B: real batches from `ant-stamps`. |
| `/stamps/{batch-id}` | GET | B | |
| `/stamps/{amount}/{depth}` | POST | B | Buy batch. |
| `/stamps/topup/{batch-id}/{amount}` | PATCH | B | |
| `/stamps/dilute/{batch-id}/{depth}` | PATCH | B | |
| `/chequebook/address` | GET | A→B | Tier A: zero address (consumers treat as not-deployed); Tier B: real address. |
| `/chequebook/balance` | GET | A→B | Tier A: zero; Tier B: real. |
| `/chequebook/deposit/{amount}` | POST | B | |
| `/chequebook/withdraw/{amount}` | POST | B | |
| `/chequebook/cashout/{peer}` | GET, POST | — | **501** — we don't cash cheques (§5.8). |
| `/bytes` | POST | B | Raw byte upload via `ant-pushsync`. |
| `/bzz` | POST | B | File / collection upload (with `Swarm-Collection: true`). |
| `/chunks` | POST | B | Direct chunk upload. |
| `/chunks/stream` | GET (WebSocket) | B | bee-js streams chunks here for high-throughput pushers. |
| `/tags` | GET, POST | B | |
| `/tags/{id}` | GET, DELETE, PATCH | B | Status fields bee-js reads: `split, seen, stored, sent, synced, uid, address, startedAt`. |
| `/feeds/{owner}/{topic}` | GET, POST | B | |
| `/soc/{owner}/{id}` | POST | B | |
| `/pins`, `/pins/check`, `/pins/{ref}` | GET, POST, DELETE | B | Local pin set. |
| `/stewardship/{address}` | GET, PUT | B | Reupload helper; uses `ant-pushsync`. |
| `/envelope/{address}` | POST | B | Stamp envelope for already-existing chunks. |
| `/grantee`, `/grantee/{address}` | * | — | **501** — ACT (Appendix B). |
| `/pss/*`, `/gsoc/*` | * | — | **501** — Appendix B. |
| `/transactions`, `/transactions/{hash}` | * | — | **501** — admin/restricted API. |
| `/reservestate`, `/chainstate`, `/redistributionstate`, `/stake/*` | * | — | **501** — full-node only (Appendix B). |
| `/auth`, `/refresh` | * | — | **501** — restricted-API auth. |

Endpoints not listed here are unknown to bee in the same versions we target, or are intentionally outside our scope; they 404.

### C.5 Pinned bee API version

We pick **one bee minor version per Ant release** as our "reference shape" and document it. Tier A ships against the latest stable bee at the time M2 lands; Tier B is re-snapped to whatever's latest at M3. Compatibility past that is best-effort and tracked in §15. The reference version is recorded in `ant-gateway/COMPAT.md` and exposed at runtime via `/health.apiVersion`.

### C.6 Conformance testing

In addition to the strategy in §10:

- **Golden response corpus.** A vendored set of recorded bee HTTP responses (one file per endpoint per scenario) lives in `crates/ant-gateway/tests/golden/`. CI replays each scenario against `antd` and `bee` side-by-side in containers and asserts JSON-shape equivalence (field names, types, presence — not values).
- **`bee-js` smoke suite.** A subset of the upstream `@ethersphere/bee-js` integration tests is run from a Node test harness against `antd`. We track which suites pass per release.
- **Header-handling tests.** Hyper-level tests that fire each header in §C.4.1 at the gateway and verify the documented behavior.
- **501 regression test.** Every endpoint in §C.4.2 marked permanently absent is asserted to return `501` with the agreed body — this prevents accidental partial implementations.

### C.7 Out of scope by design

- Anything in Appendix B (Pullsync, storage incentives, staking, PSS / GSOC, ACT, cheque cashing, advanced redundancy).
- Restricted / admin API behind bee's `restricted: true` mode; `/auth` and `/refresh` are 501.
- Multi-instance configurations (`bee` running with separate API + debug API ports). We only expose the unified API surface bee added in 2.0+.
- Byte-level compatibility of `data-dir/statestore` and `data-dir/localstore`. Migrating an existing bee data directory into Ant is **not** supported; only the `keys/swarm.key` file carries over.

---

## Appendix D — Tier-A drop-in checklist

Concrete work breakdown for the **first Tier-A drop-in** from Appendix C — the smallest cut of `ant-gateway` that makes a generic bee HTTP consumer behave correctly against `antd`. This is the work that lands inside §9 Phase 4.

### D.1 Goal and scope

Single capability: a bee-shaped read-only HTTP surface on `127.0.0.1:1633`, sufficient for an unmodified `bee-js` client (or any other bee HTTP consumer) to:

- Probe health and version.
- Read peer / topology / address state.
- Resolve and stream `bzz://` content with mantaray path lookup.
- Receive zeroed bee-shaped responses on wallet / stamps / chequebook so consumer UIs degrade gracefully.

**Out of scope for this checklist** (deferred to later M3 phases or the optional follow-ons below): writes, real wallet / stamps / SWAP, ENS resolution, encrypted uploads.

### D.2 Work items

#### D.2.1 Gateway crate scaffold

- New `crates/ant-gateway/` with `axum` + `hyper`. Entry: `Gateway::serve(node, addr)`.
- Cargo feature `http-api`, default-on for `antd`, off elsewhere.
- New flag `--api-addr` on `antd`; default `127.0.0.1:1633`. Gateway started after `Node` is up.
- Bee `jsonhttp` error helper (`{code, message}`).
- `tracing` span per request.

**Done when:** `curl http://127.0.0.1:1633/health` returns `{"status":"ok","version":"...","apiVersion":"..."}` within 50 ms of socket bind.

#### D.2.2 Status endpoints (no retrieval)

| Endpoint | Source |
|---|---|
| `GET /health` | static — version from `env!("CARGO_PKG_VERSION")` |
| `GET /readiness` | `ant-p2p` peer snapshot ≥ 1 BZZ-handshaked peer |
| `GET /node` | hardcoded `{beeMode:"ultra-light", chequebookEnabled:false, swapEnabled:false}` |
| `GET /addresses` | existing identity in `antd::main` (overlay, ethereum, libp2p public key) |
| `GET /peers` | `ant-p2p` peer snapshot |
| `GET /topology` | `ant-p2p` routing table; first cut may flatten all peers into a single bin |

**Done when:** consumers polling these endpoints render version, peer count, and topology total without errors.

#### D.2.3 Retrieval endpoints

- `GET/HEAD /bytes/{addr}` — `ant_retrieval::join`, streamed body.
- `GET/HEAD /chunks/{addr}` — `ant_retrieval::ChunkFetcher`.
- `GET/HEAD /bzz/{addr}` — mantaray root metadata; honor `index-document`, fall back to `lookup_path` with empty path.
- `GET/HEAD /bzz/{addr}/{path:.*}` — `ant_retrieval::lookup_path` + joiner. `Content-Type` from manifest entry metadata; default `application/octet-stream`.
- `Range` (single-range) on `/bytes` and `/bzz`; reject multi-range with `416`.
- Honor `Swarm-Chunk-Retrieval-Timeout`. Accept and ignore `Swarm-Redundancy-Strategy`, `Swarm-Redundancy-Fallback-Mode` for the first cut.
- Stream chunked via `Body::from_stream` so large files don't materialize.

**Done when:** `curl -o out.html http://127.0.0.1:1633/bzz/<known-root>/index.html` produces the same bytes as `antctl get bzz://<known-root>/index.html`.

#### D.2.4 Stub the wallet / stamps / chequebook trio

These exist solely so bee consumers don't see undefined fields. Field names verbatim — `bee-js` indexes by them.

| Endpoint | Body |
|---|---|
| `GET /wallet` | `{bzzBalance:"0", nativeTokenBalance:"0", chainID:100}` |
| `GET /stamps` | `{stamps:[]}` |
| `GET /chequebook/address` | `{chequebookAddress:"0x0000000000000000000000000000000000000000"}` |
| `GET /chequebook/balance` | `{totalBalance:"0", availableBalance:"0"}` |

#### D.2.5 Catch-all 501

Single `fallback` handler returning `501 {code:501, message:"not implemented in ant"}`. Anything outside the Tier-A column of §C.4.2 lands here. Future-proofs feature detection without us having to ship the rest of the matrix this week.

### D.3 Optional follow-ons

Not blockers for the smoke test, but unlock the next layer of integrations cheaply.

- **D.3.1 Bee YAML config + subcommand shim.** `antd init --config <yaml>` and `antd start --config <yaml>` parsing the §C.2 schema. Lets embedders that hand bee a YAML keep doing so unmodified. Roughly half a day; do it right after the smoke test.
- **D.3.2 Web3 v3 keystore.** `keys/swarm.key` read/write per §C.3, with migration from the legacy `signing.key` / `identity.json`. Required as soon as an embedder wants to inject identity (e.g. derived from a vault). Plan to land before any "real" embedder integration goes beyond a smoke test.

### D.4 Acceptance

Two automatable gates plus one manual:

1. **`bee-js` smoke.** Stand up `antd`, point `new Bee("http://127.0.0.1:1633")` at it, exercise `bee.getHealth()`, `bee.getNodeAddresses()`, `bee.getPeers()`, `bee.getTopology()`, `bee.downloadFile(<known-root>, "index.html")`. All succeed without retries beyond the cold-start window.
2. **Curl conformance.** Per §C.6 golden corpus: shape-equivalent to a bee container for every Tier-A endpoint.
3. **Manual browse.** Any bee-shaped browser proxy (custom protocol handler proxying to `127.0.0.1:1633`) navigating to `bzz://<known-root>/` resolves and renders.

This is the exit gate for the Tier-A column of §C.4.2. After it passes, M3 work proceeds against a known-good HTTP surface.

### D.5 Suggested order

Roughly one engineering week if D.3.1 ships with it; three to four days without.

1. D.2.1 + D.2.2 — half a day; verifiable with `curl`.
2. D.2.3 — one to two days; the real work, mostly wiring existing `ant-retrieval` to axum.
3. D.2.4 + D.2.5 — an hour.
4. D.3.1 — half a day, optional but high-leverage.
5. D.4 acceptance.

## Appendix E — Large-file retrieval and gateway streaming hardening

This appendix is a focused hardening track for the gateway's read path. It builds on Appendix D's Tier-A drop-in: D.2.3 wires `ant-retrieval` into axum and ships a streaming `/bytes` GET; this appendix finishes the job for `/bzz`, `HEAD`, true HTTP ranges, browser media, and CLI streaming. It slots into §9 as a hardening sub-track that runs alongside Phases 4 and 10.

### E.1 Purpose

Improve large-file retrieval in Ant so gateway users, browser media elements, and `antctl get` receive stable, progressive downloads without forcing the daemon, gateway, or CLI to hold the entire file body in memory.

The goal is not to build a downloader from scratch. Ant already has important pieces in place. The goal is to finish and harden the streaming path, especially for manifest-backed `/bzz` files, browser video/audio playback, HTTP range requests, and CLI downloads.

Target behaviour:

```text
HTTP client / media element / antctl file writer
        ↑
ordered byte stream or requested byte range
        ↑
metadata-first gateway response
        ↑
bounded streaming buffer
        ↑
parallel Swarm chunk/subtree retrieval with retries
        ↑
routing-aware peer/chunk fetcher
```

### E.2 Current state

Based on the current `solardev-xyz/ant` tree:

- The gateway already has a streaming path for raw `/bytes/{addr}` GET requests without a `Range` header.
- `/bzz/{addr}/path` still resolves the manifest, joins the resolved file body into memory, and then returns the full body.
- HTTP `Range` handling is currently applied after the full body has been materialised.
- `antctl get` receives terminal `Bytes` / `BzzBytes` responses over the public control protocol, with optional progress events, but not a real byte stream.
- The internal retrieval machinery already has useful building blocks: ordered joining, parallel subtree traversal, retries, peer fallback, hedged requests, cache support, and concurrency limits.

Relevant source areas:

```text
crates/ant-gateway/src/retrieval.rs
crates/ant-p2p/src/behaviour.rs
crates/ant-retrieval/src/joiner.rs
crates/ant-retrieval/src/fetcher.rs
crates/ant-control/src/protocol.rs
crates/antctl/src/main.rs
```

### E.3 Browser media requirements

Large audio and video files have stricter requirements than generic file downloads. A browser does not only need bytes. It needs enough HTTP and container metadata to decide whether it can play, how long the media is, and whether seeking is possible.

#### E.3.1 HTTP metadata needed early

For a normal media response, the gateway should provide these headers as soon as the manifest and root span are known:

```http
Content-Type: video/mp4
Content-Length: 104857600
Accept-Ranges: bytes
```

For a range response, the gateway should return:

```http
HTTP/1.1 206 Partial Content
Content-Type: video/mp4
Content-Range: bytes 0-1048575/104857600
Content-Length: 1048576
Accept-Ranges: bytes
```

Important distinction:

- `Content-Length` tells the browser the total object size for full responses.
- `Accept-Ranges: bytes` tells the browser it can ask for specific byte ranges.
- `Content-Range` tells the browser which part of the file it received and the total size.

For Ant, this means the gateway should fetch only the minimum required information first:

```text
/bzz path resolution
    ↓
file reference + content type + filename
    ↓
root chunk / span
    ↓
total size known
    ↓
headers can be returned
```

#### E.3.2 Media-container metadata needed early

After receiving headers, the browser still needs enough file bytes to parse the media container.

Examples:

- MP4: `ftyp`, `moov`, track metadata, codec initialization data.
- WebM: EBML header, segment info, track info, cues if present.
- MP3: ID3 tags, MPEG frames, and sometimes Xing/VBRI metadata for variable-bitrate duration.
- Ogg: Ogg pages and codec headers.

The browser can show duration only after it has parsed the relevant container metadata. Until then, duration may be unknown.

MP4 is especially important:

```text
Good for streaming:

[ ftyp ][ moov ][ mdat ... media data ... ]

Poor for first-byte-only streaming:

[ ftyp ][ mdat ... large media data ... ][ moov ]
```

If the MP4 `moov` atom is at the end, the browser may need to request the tail of the file before it can know duration. This is why true HTTP range support is critical. Sequential streaming from byte `0` is not enough for robust media playback.

#### E.3.3 Typical browser probing behaviour

A browser or media element may first request the beginning:

```http
Range: bytes=0-
```

or:

```http
Range: bytes=0-1048575
```

If required metadata is not near the beginning, it may then request the end:

```http
Range: bytes=103809024-104857599
```

Therefore Ant should treat range support as a media feature, not merely a download-resume feature.

#### E.3.4 Fastest useful response strategy

For video/audio, optimize these steps in order:

1. Resolve `/bzz` manifest path quickly.
2. Fetch the root chunk and read the span to know total file size.
3. Return `Content-Type`, `Content-Length`, and range capability immediately.
4. Support `HEAD` so metadata probes do not fetch the body.
5. Support single-range `GET` so browsers can fetch the beginning and/or tail.
6. Stream only the requested byte range through a range-aware joiner.

The browser-facing goal is not just "download fast". The useful metrics are:

```text
time to headers
time to first body byte
time to media loadedmetadata
time to first frame / first audio
time to seek completion
```

### E.4 Archive and ZIP file requirements

Compressed archives benefit from the same generic streaming and range machinery, but they do not need the same metadata-first optimizations as browser media.

For a normal ZIP download, the browser mainly needs:

```http
Content-Type: application/zip
Content-Length: 104857600
Accept-Ranges: bytes
Content-Disposition: attachment; filename="archive.zip"
```

A browser can download a ZIP sequentially without knowing media duration, codecs, dimensions, or container playback metadata. Therefore ZIP support should not displace the media/range priority.

The benefits of generic streaming and range support for archives are:

1. **No full buffering.** The gateway can stream archive bytes directly to the client instead of joining the entire archive in memory first.
2. **Download resume.** Browsers and download managers can resume interrupted downloads with a request such as:

   ```http
   Range: bytes=50000000-
   ```

3. **Advanced archive inspection.** ZIP files usually store the central directory near the end of the file. Archive-aware clients can request the tail, locate the central directory, list entries, and then selectively request the byte ranges needed for specific files inside the archive.
4. **Selective extraction by smart clients.** A custom app or package manager could fetch only part of a ZIP after reading the central directory. This is not normal browser download behaviour, but generic byte-range support makes it possible.

Important distinction:

```text
video/audio:
    Range support is essential for playback, duration, and seeking

zip download:
    Range support is useful for resume, bounded memory, and advanced clients,
    but not required for a simple sequential browser download
```

For `.tar.gz` or plain `.gz` files, range support is less useful for selective extraction because gzip decompression is mostly sequential unless an external index exists. The gateway can still stream and resume the compressed bytes, but it should not promise random access inside the compressed content.

Implementation guidance:

- Do not special-case ZIP in the first implementation.
- Implement streaming and single-range retrieval generically for all large files.
- Set archive-specific metadata correctly when known, especially `Content-Type` and `Content-Disposition`.
- Treat ZIP resume and advanced inspection as automatic benefits of the generic range layer.

The priority order remains:

```text
1. /bzz streaming
2. HEAD support
3. single Range support
4. media playback validation
5. generic large-file robustness, including ZIP resume
```

### E.5 Problem summary

Large downloads can still fail, stall, or feel poor for several distinct reasons:

1. **Manifest-backed `/bzz` retrieval still buffers the joined file.** This is the main browser-facing problem. Websites, videos, images, archives, and other normal dweb assets usually pass through `/bzz`, not raw `/bytes`.
2. **Browser media needs metadata and ranges, not only streaming.** Video/audio duration and seeking depend on `Content-Length`, `Accept-Ranges`, valid `206 Partial Content` responses, and container metadata such as MP4 `moov` atoms.
3. **The public CLI protocol is terminal-response based.** `antctl get` can show progress, but the actual file data still arrives as one final payload. This increases memory pressure and delays useful output.
4. **Range requests are not true retrieval ranges yet.** Browser media seeking, resumable downloads, and archive-aware clients should fetch only the requested section. The current behaviour joins first and slices afterwards.
5. **Archive downloads benefit from the same generic path.** ZIP files do not need media-style metadata probing, but they still need bounded memory use, clean resume support, and correct download headers.
6. **Streaming output granularity may still be too coarse.** The joiner should emit ordered bytes frequently enough to keep HTTP clients alive and users confident, while still respecting the Swarm tree layout.
7. **Missing data chunks remain fatal unless redundancy recovery is implemented.** Degraded redundancy handling can mask the redundancy level byte, but it does not reconstruct unavailable data chunks via Reed-Solomon recovery.
8. **Failure modes are hard to diagnose.** A large retrieval may fail because of sparse chunks, stale peer state, queue saturation, retry exhaustion, malformed trees, unsupported redundancy, media client aborts, or gateway timeouts. These should be distinguishable in logs and user-facing errors.

### E.6 Design principles

#### E.6.1 Metadata first, body second

The gateway should resolve enough information to answer HTTP metadata quickly before starting expensive body retrieval.

For `/bzz`, that means:

```text
manifest lookup → file reference + metadata → root span → headers → body stream
```

#### E.6.2 Make HTTP ranges first-class

For large media, range retrieval is as important as continuous streaming. The joiner should be able to stream a requested byte interval without first materialising the whole file.

#### E.6.3 Stay Swarm-DAG-native

Do not make arbitrary `100 KiB` byte parts the core abstraction. Fixed output windows are useful as a buffering and UX concept, but the downloader should remain aware of the Swarm chunk tree.

Better model:

```text
Swarm chunks/subtrees fetched out of order
        ↓
joiner verifies and orders data
        ↓
small contiguous byte buffers are emitted
        ↓
gateway / CLI writes them sequentially
```

#### E.6.4 Emit bytes only in file order

Network fetches may complete out of order. The client must receive a strictly ordered byte stream, or a strictly ordered byte stream inside the requested range.

#### E.6.5 Keep buffers bounded

The downloader may fetch ahead, but it must not accumulate unbounded output if the HTTP client or CLI writer is slow.

#### E.6.6 Hide retry logic inside retrieval

The gateway and CLI should consume an async byte stream. They should not know which chunk failed, which peer was retried, or whether a cache hit occurred.

#### E.6.7 Prefer additive protocol changes

Keep existing `GetBytes` / `GetBzz` terminal responses for compatibility. Add streaming variants rather than breaking current clients.

### E.7 Proposed architecture

```text
Gateway / antctl
        ↓
metadata-first response layer
        ↓
streaming retrieval interface
        ↓
manifest resolver, if `/bzz`
        ↓
root/span reader
        ↓
full-stream or range-aware joiner
        ↓
bounded ordered output buffer
        ↓
routing-aware chunk fetcher
        ↓
libp2p retrieval streams
```

The main changes are:

- make `/bzz` consume the same kind of ordered byte stream that raw `/bytes` already uses internally;
- add true single-range retrieval instead of materialise-then-slice;
- expose enough metadata early for browsers and media elements;
- eventually make `antctl get` stream file bytes progressively.

### E.8 Terminology

- **Swarm chunk**: content-addressed chunk, usually around 4 KiB of payload.
- **Intermediate chunk**: chunk containing child references in the Swarm file tree.
- **HTTP body chunk**: arbitrary byte buffer emitted to the HTTP client.
- **Streaming window**: bounded amount of ordered output data being prepared ahead of the client.
- **Prefetch window**: limited lookahead over future chunks/subtrees.
- **Terminal response**: a response that returns the whole joined file body at once.
- **Range request**: HTTP request asking for a specific byte interval.
- **Media metadata**: container-level data required for duration, dimensions, tracks, codecs, and seeking.

### E.9 Phase 1 — Stream `/bzz` responses

#### E.9.1 Goal

Make the most important browser-facing path progressive and memory-bounded.

#### E.9.2 Tasks

- Add an internal `StreamBzz` command or equivalent gateway-facing streaming function.
- Resolve the mantaray path first.
- Once the file reference and metadata are known, return stream metadata:
  - total file size, from the root span when known;
  - content type;
  - filename / content-disposition hint;
  - resolved reference.
- Stream the resolved file body through the same ordered joiner used for raw byte trees.
- Preserve the existing materialising `GetBzz` path for compatibility, tests, and possibly small files.
- For the first PR, keep range requests on the existing path or explicitly route them to the later range implementation.
- Do not advertise `Accept-Ranges: bytes` for paths where true range retrieval is not yet implemented.

#### E.9.3 Gateway requirements

The gateway must:

- start the HTTP response only after manifest/file metadata and root span are known;
- set `Content-Type` from manifest metadata where available;
- set `Content-Length` when the root span is known;
- set `Accept-Ranges: bytes` only when range semantics are actually supported;
- apply backpressure through bounded channels;
- stop retrieval when the HTTP client disconnects;
- avoid buffering the full response body in memory.

#### E.9.4 Acceptance criteria

- `GET /bzz/{addr}/large-file` starts sending body bytes before the full file has been fetched.
- Gateway memory usage does not scale linearly with the full file size for non-range `/bzz` GETs.
- Existing small `/bzz` website and file behaviour remains compatible.
- Errors before the first byte return normal HTTP errors.
- Errors after streaming has started terminate the body stream cleanly and are visible in logs.
- Client disconnect cancels the retrieval task.

#### E.9.5 Recommended first PR

```text
stream bzz file responses through the retrieval joiner
```

Scope:

- add internal streaming BZZ retrieval;
- reuse the existing streaming joiner path;
- keep the existing terminal `GetBzz` path;
- add tests proving `/bzz` large files begin responding before full retrieval completes.

This is the highest-impact step because normal web/media access goes through `/bzz`.

### E.10 Phase 2 — Add metadata-first `HEAD` support

#### E.10.1 Goal

Give browsers, media elements, download managers, and gateways the object metadata they need without forcing body retrieval.

#### E.10.2 Tasks

- Implement `HEAD` for `/bytes`.
- Implement `HEAD` for `/bzz` after manifest path resolution.
- Fetch only what is needed to determine:
  - content type;
  - total size;
  - filename / content-disposition hint;
  - entity tag / stable cache validator, if available;
  - range support status.
- For archives and other downloadable files, return `Content-Disposition: attachment` when manifest metadata or route semantics indicate a download.
- Return headers without joining or streaming the body.
- Keep behaviour consistent with the equivalent `GET` response.

#### E.10.3 Headers to return

For a known object:

```http
Content-Type: video/mp4
Content-Length: 104857600
Accept-Ranges: bytes
ETag: "..."
```

Only return `Accept-Ranges: bytes` once true range retrieval is available for that route.

#### E.10.4 Acceptance criteria

- `HEAD /bytes/{addr}` returns size without body retrieval.
- `HEAD /bzz/{addr}/video.mp4` resolves manifest metadata and returns size/content type without body retrieval.
- `HEAD` does not materialise large files.
- Browser/media metadata probes do not trigger a full download.
- `HEAD /bzz/{addr}/archive.zip` returns size, content type, and download metadata without body retrieval.

### E.11 Phase 3 — True HTTP range retrieval

#### E.11.1 Goal

Support efficient browser media metadata parsing, seeking, and resumable downloads without joining the whole file first.

#### E.11.2 Tasks

- Parse single-range HTTP headers before retrieval:

  ```http
  Range: bytes=start-end
  Range: bytes=start-
  Range: bytes=-suffixLength
  ```

- Resolve root span first to know total file length.
- Map the requested byte range to required Swarm chunks/subtrees.
- Skip output before `start` without sending it.
- Stop retrieval after `end`.
- Return proper `206 Partial Content` responses:
  - `Content-Range`;
  - `Accept-Ranges: bytes`;
  - accurate `Content-Length` for the range body.
- Return `416 Range Not Satisfiable` for invalid ranges.
- Reject or defer multi-range support initially.

#### E.11.3 Media-specific behaviour

The gateway must handle the two most common browser media probes efficiently:

```http
Range: bytes=0-
```

and:

```http
Range: bytes=<tail-start>-<total-end>
```

This allows browsers to read MP4 metadata at the beginning or, when necessary, at the end of the file.

#### E.11.4 Archive-specific behaviour

The same single-range implementation should work for archives without special casing ZIP internals. For ZIP files, tail-range requests allow advanced clients to read the central directory near the end of the archive, while ordinary browsers mainly benefit from download resume.

The gateway should serve the requested compressed bytes exactly as stored. It should not try to decompress, inspect, or rewrite archive contents in the first implementation.

#### E.11.5 Acceptance criteria

- `curl -r 0-999` fetches only the requested section.
- `curl -I` returns metadata without body retrieval.
- Browser video/audio can show duration for files with metadata near the start.
- Browser video/audio can request the tail of an MP4 file when metadata is at the end.
- Seeking works without fetching the whole file.
- Range requests work for both `/bytes` and `/bzz` after manifest resolution.
- Invalid ranges return `416`.
- Multi-range requests are explicitly unsupported or handled safely.

#### E.11.6 Recommended second PR

```text
add single-range streaming retrieval for bytes and bzz
```

Scope:

- implement range parsing and response headers;
- add a range-aware joiner entry point;
- support single-range requests for `/bytes` and `/bzz`;
- add browser/media integration tests where possible.

### E.12 Phase 4 — Smooth ordered output

#### E.12.1 Goal

Make large downloads feel continuous without fighting the Swarm tree layout.

#### E.12.2 Tasks

- Review the current joiner emission threshold and subtree streaming behaviour.
- Introduce a configurable target output buffer size, for example:

  ```rust
  STREAM_TARGET_OUTPUT_BYTES = 128 * 1024;
  STREAM_MIN_EMIT_BYTES = 32 * 1024;
  STREAM_PREFETCH_WINDOWS = 4;
  ```

- Treat those values as output-buffering goals, not as a requirement to split the Swarm tree into artificial byte ranges.
- Emit as soon as a contiguous ordered byte buffer is available and large enough, or when EOF/range-end is reached.
- Maintain bounded lookahead so later chunks/subtrees can be fetched while earlier bytes are being sent.
- Preserve current retry semantics and content verification.

#### E.12.3 Initial defaults

```text
output target:          128 KiB
minimum emit size:       32 KiB
prefetch windows:         4
join fanout:              keep current default initially
per-request concurrency: keep existing cap initially
process-wide cap:        keep existing cap initially
```

These values should be tunable only after observability is in place. Hard-coded constants are acceptable for the first implementation.

#### E.12.4 Acceptance criteria

- Time to first body byte is low after manifest/root resolution.
- Large downloads produce regular body progress under normal mainnet conditions.
- One slow later chunk does not block already-complete earlier output.
- Memory use is bounded approximately by:

  ```text
  active_streams × prefetch_windows × output_target_bytes
  ```

  plus chunk-fetch and protocol overhead.

### E.13 Phase 5 — Add real CLI byte streaming

#### E.13.1 Goal

Allow `antctl get` to write progressively to stdout or disk instead of waiting for a final hex-encoded payload.

#### E.13.2 Current distinction

There are two relevant protocol layers:

1. **Internal daemon/gateway command channel.** This already has chunk-streaming concepts for raw bytes.
2. **Public `antctl` control protocol.** This currently exposes progress events plus terminal `Bytes` / `BzzBytes` payloads.

Phase 5 concerns the public `antctl` control protocol.

#### E.13.3 Tasks

- Add additive public protocol request variants, for example:

  ```rust
  StreamBytes { reference, bypass_cache }
  StreamBzz { reference, path, allow_degraded_redundancy, bypass_cache }
  ```

  or add a clearly versioned streaming mode to existing requests.

- Add streamed response variants, for example:

  ```rust
  StreamStart {
      total_bytes: u64,
      content_type: Option<String>,
      filename: Option<String>,
  }

  BytesChunk {
      data: String, // base64 while NDJSON is used
  }

  StreamDone
  ```

- Continue to support existing terminal responses:

  ```rust
  Bytes { hex }
  BzzBytes { hex, content_type, filename }
  ```

- Prefer base64 over hex for streamed chunks if the transport remains newline-delimited JSON.
- Longer term, consider a framed binary control protocol for byte streams.
- Update `antctl get -o` to stream directly into a file.
- Keep progress rendering on stderr.

#### E.13.4 Acceptance criteria

- `antctl get bytes://... -o file.bin` writes progressively to disk.
- `antctl get bzz://.../large.bin -o file.bin` writes progressively to disk.
- `antctl get ... > file.bin` streams binary bytes to stdout when not in JSON mode.
- Existing clients using terminal `Bytes` / `BzzBytes` still work.
- JSON output mode remains safe for scripts and does not intermix raw binary with JSON.

### E.14 Phase 6 — Observability and failure diagnostics

#### E.14.1 Goal

Make large-download and media-playback failures actionable.

#### E.14.2 Metrics to record per retrieval

- time to first peer;
- time to manifest resolution, for `/bzz`;
- time to root chunk;
- time to headers ready;
- time to first streamed byte;
- time to first range byte;
- total bytes streamed;
- average throughput;
- longest idle gap between emitted output chunks;
- chunks fetched from cache vs network;
- peer attempts per chunk;
- retries per chunk/subtree;
- terminal failure reason;
- whether the downstream client disconnected first.

#### E.14.3 Media-specific diagnostics

When serving media, log:

- whether request was full `GET`, `HEAD`, or range `GET`;
- requested range;
- returned range;
- content type;
- total file size;
- whether the request targeted the beginning or tail of the file;
- whether the response reached first byte.

#### E.14.4 Error details

When possible, include:

- failing chunk address;
- approximate byte offset;
- retry count exhausted;
- peers attempted;
- whether the chunk was ever found in cache;
- whether failure happened before or after first byte;
- whether redundancy was present but unsupported.

#### E.14.5 Acceptance criteria

- Logs distinguish network timeout, missing chunk, malformed tree, unsupported redundancy, max-size rejection, gateway timeout, and client disconnect.
- Logs distinguish full-body streaming failures from range-streaming failures.
- `antctl get` shows a useful failure message without exposing excessive internals by default.
- Retrying a failed command benefits from cached chunks already fetched.
- Tuning fanout, retry count, and window size can be based on measurements rather than guesswork.

### E.15 Phase 7 — Retry and concurrency tuning

#### E.15.1 Goal

Improve completion rate without overwhelming Bee peers, local queues, or Ant's own retrieval tasks.

#### E.15.2 Tasks

- Keep existing process-wide concurrency caps.
- Add per-request budgets for:
  - maximum chunk attempts;
  - maximum distinct peers per chunk;
  - maximum subtree retries;
  - maximum idle time without emitted bytes.
- Consider adaptive behaviour:
  - reduce concurrency when many requests time out;
  - hedge earlier when tail latency is high;
  - retry more aggressively for large files after partial success;
  - avoid blacklisting peers too broadly for isolated chunk failures.
- Ensure concurrent browser media requests do not starve each other.

#### E.15.3 Acceptance criteria

- Four parallel large gateway downloads make progress without unbounded queue growth.
- Four parallel browser media range requests make progress without starving full-file downloads.
- A small number of slow peers does not stall an otherwise healthy download.
- Retry exhaustion produces a precise error.
- Throughput improves or stays stable compared with the current implementation.

### E.16 Phase 8 — Reed-Solomon / redundancy recovery

#### E.16.1 Goal

Stop treating every missing data chunk as fatal when the file was uploaded with recoverable redundancy.

#### E.16.2 Tasks

- Decode Swarm redundancy metadata properly.
- Implement or integrate Bee-compatible Reed-Solomon recovery.
- Recover missing data chunks from parity chunks where possible.
- Keep current degraded mode as a fallback/debug mode.

#### E.16.3 Acceptance criteria

- Files with supported redundancy decode correctly.
- Recoverable missing data chunks no longer fail the whole download.
- Non-recoverable missing chunks still produce precise diagnostics.

### E.17 Milestone plan

#### E.17.1 Milestone A — Browser-facing `/bzz` improvement

1. Stream non-range `/bzz` GET responses.
2. Preserve current `/bytes` streaming behaviour.
3. Add integration tests for large `/bzz` files.
4. Do not advertise range support until true ranges are implemented.

#### E.17.2 Milestone B — Media metadata and ranges

1. Add metadata-first `HEAD` for `/bytes` and `/bzz`.
2. Add true single-range retrieval.
3. Return correct `206`, `Content-Range`, `Content-Length`, and `Accept-Ranges` headers.
4. Test MP4/WebM/audio behaviour in real browsers.

#### E.17.3 Milestone C — Smoothness

1. Tune output granularity.
2. Add bounded prefetch/lookahead.
3. Measure time to first byte and longest output idle gap.

#### E.17.4 Milestone D — CLI streaming

1. Add public streamed control responses.
2. Make `antctl get -o` stream to disk.
3. Keep terminal responses for compatibility.

#### E.17.5 Milestone E — Robustness

1. Improve observability.
2. Tune retry/concurrency behaviour.
3. Add Reed-Solomon recovery.

### E.18 Test plan

#### E.18.1 Unit tests

- Joiner emits bytes in order even when child fetches complete out of order.
- Range-aware joiner emits only the requested byte interval.
- Suffix ranges resolve to the correct byte interval after total size is known.
- Failed chunks are retried before stream failure.
- Receiver drop cancels retrieval cleanly.
- Streaming buffer stays bounded under slow consumer conditions.
- Range mapping selects the correct Swarm chunks.
- Malformed intermediate chunks fail deterministically.

#### E.18.2 Integration tests

- `/bytes/{addr}` large file streams without full materialisation.
- `/bzz/{addr}/large.bin` streams without full materialisation.
- `/bzz/{addr}/index.html` still serves normal website files correctly.
- `HEAD /bytes/{addr}` returns metadata without body.
- `HEAD /bzz/{addr}/video.mp4` returns metadata without body.
- `GET /bytes/{addr}` with `Range: bytes=0-999` fetches only that range.
- `GET /bzz/{addr}/video.mp4` with `Range: bytes=0-999` fetches only that range.
- `GET /bzz/{addr}/video.mp4` with a tail range fetches only the tail.
- `GET /bzz/{addr}/archive.zip` streams without full materialisation.
- `GET /bzz/{addr}/archive.zip` with `Range: bytes=50000000-` resumes from the requested offset.
- `GET /bzz/{addr}/archive.zip` with a tail range fetches only the ZIP tail.
- `antctl get -o large.bin` writes progressively to disk.
- Concurrent large downloads respect process-wide concurrency caps.
- Slow/missing peer simulation triggers fallback without corrupting output order.
- Client disconnect cancels the retrieval task.

#### E.18.3 Browser media tests

Test with at least:

```text
MP4 with moov atom at the beginning
MP4 with moov atom at the end
WebM file
MP3 or Ogg audio file
```

Check:

- video/audio starts without downloading the full file;
- duration appears after metadata is loaded;
- seeking near the middle works;
- seeking near the end works;
- browser performs range requests and receives valid `206` responses;
- tail metadata probing does not trigger full-file retrieval.

#### E.18.4 Archive tests

Test with at least:

```text
ZIP archive around 10 MiB
ZIP archive around 100 MiB
tar.gz archive around 10 MiB
```

Check:

- archive downloads start without full materialisation;
- interrupted ZIP downloads can resume through `Range`;
- tail-range requests against ZIP files return only the requested tail bytes;
- `tar.gz` files stream and resume as compressed bytes, without implying random access inside the compressed content;
- correct `Content-Type`, `Content-Length`, `Accept-Ranges`, and `Content-Disposition` are returned when known.

#### E.18.5 Manual mainnet tests

Test files around:

```text
100 KiB
1 MiB
10 MiB
50 MiB
100 MiB
```

Measure:

- time to headers;
- time to first byte;
- time to browser `loadedmetadata`;
- time to first frame/audio;
- average throughput;
- peak memory;
- longest output idle gap;
- retries per chunk;
- peers used;
- cache hit ratio;
- completion rate under concurrent browser loads.

### E.19 Final acceptance criteria

The improvement is successful when:

- a 10 MiB file streams through `/bytes` without full buffering;
- a 10 MiB file streams through `/bzz` without full buffering;
- a 100 MiB file starts sending bytes soon after root/manifest resolution;
- `HEAD` returns accurate metadata for `/bytes` and `/bzz` without body retrieval;
- single-range requests fetch only the requested byte interval;
- browsers can show duration for normal MP4/WebM/audio files without downloading the full file;
- browsers can seek within large media files using range requests;
- ZIP downloads can resume via range requests without full re-download;
- memory usage remains bounded and does not scale linearly with file size;
- one slow or failing chunk triggers local retry rather than immediate request failure;
- retry exhaustion identifies the failing chunk or approximate byte offset;
- client disconnect cancels retrieval;
- four parallel large gateway downloads do not starve each other;
- warm-cache repeat downloads are materially faster than cold-cache downloads;
- existing small-file, website, and compatibility paths still work.

### E.20 Non-goals for the first pass

- Multi-range HTTP responses.
- Transcoding or rewriting media files.
- Moving MP4 `moov` atoms during gateway serving.
- ZIP-specific central-directory parsing or selective extraction in the gateway.
- Encrypted chunk support.
- Reed-Solomon recovery.
- Replacing the Swarm retrieval protocol.
- Replacing the current control protocol immediately.
- Perfect Bee parity for every edge case.
- Unbounded file sizes.

### E.21 Recommended immediate implementation sequence

1. Add internal `StreamBzz` support.
2. Route non-range `/bzz` GETs through that stream.
3. Add cancellation on downstream HTTP disconnect.
4. Add metadata-first `HEAD` support.
5. Add true single-range retrieval for `/bytes` and `/bzz`.
6. Return correct media-relevant headers: `Content-Type`, `Content-Length`, `Accept-Ranges`, and `Content-Range`.
7. Ensure the same range layer works for generic downloads, including ZIP resume and tail-range requests.
8. Add metrics for time to headers, time to first byte, longest idle gap, retries, and terminal failure reason.
9. Add CLI streaming only after the gateway/media path is stable.

### E.22 Implementation notes

#### E.22.1 Media files

The gateway should not try to fix badly packaged media in the first pass. If an MP4 file has the `moov` atom at the end, the correct gateway behaviour is to support range requests so the browser can fetch the tail quickly. Rewriting the file into "fast start" layout is an upload/tooling concern, not a gateway-serving concern.

#### E.22.2 Archives

The gateway should not try to understand or optimize archive internals in the first pass. ZIP central-directory access and selective extraction are client-level capabilities enabled by generic byte ranges. Ant should focus on correct metadata, bounded streaming, and exact byte-range serving for any file type.

---

## Appendix F — Bee Light-Node Accounting and Client-Side Pseudosettle

This appendix captures the production regression that landed with the `0.3.0`
streaming gateway from Appendix E and the work done to restore stability.
The summary is short: the architectural shift from "materialize the file,
then stream it from disk" to "stream chunks straight to the HTTP client"
exposed a debt-accounting cliff with bee that 0.2.0 had been unintentionally
hiding behind its serial fetch + small concurrency burst pattern.

### F.1 Symptoms

- `0.3.0` deployed to `vibing.at/ant`. Single 55 MiB WAV via `/bzz`
  fetched fine on a freshly-started daemon.
- Repeated downloads of the same file (or sustained traffic for a few
  minutes) caused increasing latency, then truncated downloads
  (`502 Bad Gateway` from the reverse proxy), then full request stalls.
- `antctl peers` showed the connected peer set collapsing from
  ~100 → ~0 over a couple of minutes of sustained `/bzz` load. After
  load stopped, peers slowly re-handshaked back toward 100. Same daemon
  binary, same identity, same chequebook, no other config change.
- `0.2.0` running on the same host, same network, same workload had
  none of these symptoms. Rolling back the binary on the gateway
  restored production immediately. Confirmed regression.

### F.2 Why 0.3.0 hit it and 0.2.0 didn't

Both versions use the same `RoutingFetcher` (8-way fanout, 3-way
hedging, 500 ms hedge delay) and the same chunk-retrieval protocol
against bee. The difference is what the gateway does with the fetcher:

- `0.2.0`: `GET /bzz/...` materializes the full file in memory via
  `join` (the joiner already does the parallel chunk fetch), then
  hands a `Bytes` body to axum which streams it to the client.
  Network traffic is one bursty fetch for the whole file, then
  network-quiet while bytes flow over HTTP.
- `0.3.0`: the joiner is range-aware and emits chunks into an
  `mpsc::Sender<Bytes>` consumed by axum's `Body::from_stream`. As
  soon as the HTTP client TCP buffer drains, joiner pulls more
  chunks. Network traffic is **continuous** for the entire wall-clock
  duration of the download, including the long tail where the client
  is throttled by their own access link.

For a 55 MiB file at a ~5 MiB/s client link:
- `0.2.0`: ~2 seconds of full-tilt fetch, then 9 seconds of HTTP-only.
- `0.3.0`: ~11 seconds of pull-as-the-client-drains fetch, the entire
  duration touching every neighbourhood the file's chunks live in.

The net effect is that `0.3.0` keeps network state across many more
request/response round trips with the same hot peers (those in the
file's neighbourhood), and accumulates per-peer state at bee for the
full download window instead of just the burst.

### F.3 The bee-side budget for a light client

We dug through bee 2.7.x to understand exactly what is being accounted.

`pkg/accounting/accounting.go` — every chunk bee serves us costs us
*accounting units*. The per-chunk price comes from `pricer/pricer.go`
and depends on proximity:
- `basePrice = 10_000` units for a chunk at PO ≥ 7 (in the peer's
  own neighbourhood).
- Each PO step further out **doubles** the price up to a cap.
- At the file's far edge a 4 KiB chunk can cost up to ~`320_000` units.

Our debt at a peer is tracked in their `accountingPeer` map.
On every successful chunk delivery they call `accounting.Apply()` which
adds the price to our balance. When that balance crosses
`paymentThresholdForPeer(peer, fullNode)` they expect us to settle.

For a light node (we send `full_node: false` in the bzz handshake) the
relevant constants in `accounting.go` are:
- `paymentThreshold = 13_500_000` units (full node default), divided
  by `lightFactor = 10` for light clients →
  `lightPaymentThreshold = 1_350_000`.
- `disconnectLimit = paymentThreshold + paymentThreshold/4 = 16_875_000`,
  then divided by `lightFactor` →
  `lightDisconnectLimit ≈ 1_687_500` units.
- `refreshRate = 4_500_000` units / sec full node →
  `lightRefreshRate = 450_000` units / sec.

The disconnect path is `accounting.Apply` → `BlockPeerError` → bee's
libp2p layer calls `Blocklist.Add(overlay, duration, reason)` and
closes the connection. The blocklist duration is computed as
`(debt + paymentThreshold) / refreshRate` seconds — for a light node
hitting the limit cleanly it works out to a few seconds, but multiple
overlapping in-flight requests crossing the threshold simultaneously
can multiply that significantly via the `ghostBalance` accounting.

The arithmetic at our scale: a 55 MiB file is ~14_000 chunks. Even
with 100 peers doing perfect load balancing that's ~140 chunks each;
in practice the file's neighbourhood has ~20 hot peers that take
~70% of the load. At an average chunk price of ~100_000 units, a hot
peer hits `lightDisconnectLimit` after ~17 chunks, i.e. within seconds
of the download starting. **Without a debt-clearing mechanism we run
out of every hot peer's budget before the file is half done.**

`0.2.0` got away with it because its single big burst tended to
finish before bee actually got around to closing the connection — bee
checks the threshold on the *next* incoming request after we cross
it, and the burst was over by then. `0.3.0`'s sustained 100 s of
chunk fetches gives bee plenty of opportunity to evict us mid-stream.

### F.4 Pseudosettle

Bee ships `/swarm/pseudosettle/1.0.0/pseudosettle` exactly for this
case: a free, time-based debt refresh between any two peers.

`pkg/settlement/pseudosettle/pseudosettle.go::handler` does
`min(attempted, lightRefreshRate * elapsed_since_last_settle, current_debt)`
and credits us for that many units. It's symmetric (either side can
dial), single round-trip, and bounded server-side, so we can't abuse
it. Bee implements both sides; Ant only ever needs the dialer side
(we're the one accumulating debt).

The wire protocol on top of bee's standard libp2p substreams:

```text
dialer → listener : varint(0x00)                              // empty pb.Headers
listener → dialer : varint(0x00)                              // empty pb.Headers
dialer → listener : varint(N) + Payment   { amount: bytes }   // big-endian big.Int
listener → dialer : varint(N) + PaymentAck { amount, timestamp }
```

The headers preamble is bee's `pkg/p2p/libp2p/headers.go`; it wraps
every protocol stream regardless of whether the protocol uses
headers. The `Payment.amount` and `PaymentAck.amount` are
`big.Int.Bytes()` (big-endian unsigned, no leading zeroes, empty for
zero). `PaymentAck.timestamp` is a Unix-seconds `int64`.

### F.5 Implementation in `crates/ant-p2p`

New module: `crates/ant-p2p/src/pseudosettle.rs`.

Three pieces:

1. **`refresh_peer(control, peer)`**. Opens the pseudosettle stream,
   writes the headers preamble, sends `Payment{ amount: 27_000_000 }`
   (an over-ask — bee clamps), reads the `PaymentAck`, returns the
   accepted amount + timestamp. Errors are returned and counted.

2. **`run_inbound`**. Defensive: if a peer ever opens pseudosettle
   *toward* us (we don't expect this since we never originate refresh
   inbound, but bee has been seen to probe) we drain the Payment and
   ack zero so they don't account anything to us.

3. **`run_driver(control, notify_rx)`**. Background tokio task. The
   fetcher's hot path (`crates/ant-retrieval/src/fetcher.rs`) sends
   the peer id of every successful chunk fetch into a bounded
   `mpsc::Sender<PeerId>` (1024-deep, `try_send`, drops on
   backpressure). The driver builds a `HashMap<PeerId, PeerState>`
   tracking `last_used` and `last_refresh`, and on a 1 s tick walks
   the set, dispatching `refresh_peer` for any peer whose last
   refresh is at least `MIN_REFRESH_INTERVAL = 2 s` old.

The driver gates concurrent refreshes with
`MAX_INFLIGHT_REFRESHES = 16` to keep the libp2p stream control's
queue from flooding when a download starts touching dozens of new
peers in quick succession.

A second filter, added after the first production deploy exposed a
gateway-scale failure mode, restricts refresh dispatch to peers
currently in the `peers_watch` snapshot (i.e. peers we still have a
direct, handshaked connection to). Without it, the active state
HashMap grew to ~2_200 entries within minutes of sustained gateway
traffic — the fetcher had cycled through that many distinct peers
during streaming — and ~6_000 of the ~6_400 dispatched refreshes per
minute failed with `no addresses for peer`. That starved the
in-flight semaphore so live peers couldn't refresh, and the
`ok` count collapsed from ~2_000/min to ~1/min. After the filter
the metrics stabilise around ~2_300 ok / ~50 failed / 600 MUnit
accepted per minute on a single-stream gateway, with the active set
tracking the routing snapshot at ~100–130 peers. A 5 s grace window
on `last_used` survives short connection flaps (yamux idle resets
that re-establish in <100 ms).

A `DriverMetrics` struct counts refreshes per outcome (`ok`,
`ok_nonzero`, `failed`, `timed_out`) plus total accepted units, and
the driver emits a single `INFO ant_p2p::pseudosettle: refresh
summary (last 60s)` log line per minute summarising the deltas.
Per-peer trace-level events are still available behind
`RUST_LOG=ant_p2p::pseudosettle=trace` for debugging.

Wiring: `crates/ant-p2p/src/sinks.rs` registers the pseudosettle
inbound stream with `libp2p_stream::Control::accept` and spawns
`run_inbound`. `crates/ant-p2p/src/behaviour.rs` constructs the
notify channel, spawns `run_driver` with a clone of `Control`, and
threads the `mpsc::Sender<PeerId>` into every `run_*` (`run_get_bytes`,
`run_stream_bytes`, `run_stream_bzz`, `run_get_bzz`, `run_list_bzz`)
so the `RoutingFetcher` they build can call
`.with_payment_notify(tx)`.

### F.6 Validation

Setup: separate `antd` daemon (release build) on `127.0.0.1:1734`
with its own data dir and identity, no production traffic, alongside
the production `0.2.0` instance still serving `vibing.at/ant`.

Reference fetched: `bzz://c4f8a45301b57d0e36f0f5348ed371aee42ea0b9fe9b3caaf26015d652eedc40/tracks/01%20arrival.wav`,
55 MiB WAV, the same file the user reported failing in 0.3.0.

Three back-to-back fetches on a freshly-started peer set:

| attempt | size       | wall time | http status |
|---------|-----------|-----------|-------------|
| 1       | 54_958_120 | 108 s     | 200         |
| 2       | 54_958_120 | 96 s      | 200         |
| 3       | 54_958_120 | 92 s      | 200         |

The `0.2.0` baseline on the same machine for the same file averaged
~95 s. `0.3.0` with pseudosettle is in the same throughput band and
**faster on repeated fetches** — the pseudosettled peer stays warm
across requests instead of rotating through cold peers.

Pseudosettle traffic during those three downloads:
- ~24_000 successful refreshes
- ~8_000 failures (almost entirely "no addresses for peer" — the peer
  disconnected between us notifying the driver and the driver dialing
  them; harmless, the next chunk fetch refills the active set)
- 0 timeouts
- ~60_000_000_000 accounting units accepted (~60 GUnit, well above
  the 1.7 MUnit-per-peer disconnect floor — pseudosettle is doing
  real debt clearing, not just NAKing).

Peer set behaviour during the same window:
- Stayed in the 99–138 range, never dropped below ~95.
- ~30 disconnects/sec sustained, but matched by reconnects so the
  set size oscillated rather than collapsing.
- Peer churn concentrates on a handful of peers (the same 5–10 peers
  account for >80% of the disconnect events) — almost certainly
  peers we already blocklisted *us* on before pseudosettle landed,
  whose 24 h blocklist entries are still active. They'll age out.
- 0 fetch errors during the entire test window.

The `info`-level summary lines at 60 s granularity make the steady
state easy to spot in production logs:

```
INFO ant_p2p::pseudosettle: refresh summary (last 60s)
    active_peers=124 ok=2450 failed=505 timed_out=1 units_accepted=691280000
```

A rate of 2_000–3_000 `ok` per minute and `units_accepted` in the
hundreds of millions is what a streaming gateway under load looks like.
A rate near zero means either the gateway is idle (fine) or
pseudosettle has stopped working (investigate).

### F.7 What's left

The implementation is correct and stable for the streaming workload
that triggered the regression. Open items for follow-up, in priority
order:

1. **Persist `lightRefreshRate * elapsed` budget across daemon
   restarts.** Currently every restart starts the local refresh
   timer at zero. Bee's view is wall-clock based, so we won't
   double-spend, but a fresh daemon hitting peers we previously
   refreshed will get NAK'd for the first second post-restart. Low
   impact in practice — the next tick just refreshes again — but
   tidy.

2. **Per-peer chunks-per-connection counter in the fetcher.** Would
   give us a direct read on whether pseudosettle is keeping us under
   the disconnect threshold. Currently we infer it from peer set
   stability, which is a coarse proxy.

3. **Inbound payment book-keeping.** `run_inbound` always acks zero,
   which is fine for steady state — but if we ever do start
   accepting work that bee thinks we owe payment for, we'll need
   real bookkeeping. Out of scope for the gateway use case but worth
   noting.

4. **Identify the one specific peer churn pattern remaining**: a
   handful of peers cycle connect/disconnect every ~30 ms. Hypothesis
   is they have us in a stale 24 h blocklist from pre-pseudosettle
   load tests; confirm by waiting 24 h and re-checking. If still
   churning, dig into bee's blocklist persistence to find what's
   keeping them sticky.

## Appendix G — Bee-shaped retrieval and the streaming "moving window"

After Appendix F's pseudosettle work shipped to `vibing.at/ant`, a
head-to-head benchmark against an in-process `ethersphere/bee:2.7.1`
on the same machine and the same network exposed two unrelated
performance gaps:

- Ant was 2–3× slower than bee on cold-cache `/bytes/<ref>` fetches
  of 35–60 MiB tracks; on a 112 MiB track ant timed out at 600 s
  while bee finished in ~205 s.
- Pseudosettle was reporting `active_peers=1` during sustained
  `/bytes` streaming, which superficially looked like a regression
  but actually surfaced a structural problem: the fetcher was
  funnelling almost every chunk through a single peer.

This appendix documents the `0.3.0` → `0.3.0+` retrieval rework that
closes both gaps, plus the follow-up cancel-tolerant hedging fix
(G.7) that converts the remaining ghost-overdraw into clearable real
debt and stabilises the peer set across multi-track sessions.

| track    | size    | bee     | ant before | ant + bee-shaped | ant + cancel-tolerant |
|----------|---------|---------|------------|------------------|-----------------------|
| 02 butterfly | 44 MiB  | 80.6 s  | 186.5 s    | 49.6 s           | 83.0 s                |
| 03 sallarom  | 59 MiB  | 109.6 s | 220.7 s    | 81.5 s           | 139.0 s               |
| 05 highfive  | 35 MiB  | 65.4 s  | 117.3 s    | 30.7 s           | 97.3 s                |
| 06 flutterby | 112 MiB | 204.9 s | TIMEOUT    | 307.8 s          | 286.9 s               |

The "ant + cancel-tolerant" column is slower per-track than the
preceding column on tracks 1-3, but that's the wrong axis to read it
on: the previous column was a *single* fresh fetch with a fully warm
peer set, while the new column is the **fourth track of a continuous
4-track session**. Across the same 4-track sequence the previous
design dropped from 100 → 36 connected peers and timed out on the
fourth track; the new design holds 99–100 peers throughout and
completes all four tracks with byte-identical content (G.7
validation table). The 112 MiB track is now the only one that beats
its previous standalone time, because it's the only one whose
duration was previously ghost-overdraw-bound.

### G.1 What bee actually does (`pkg/retrieval/retrieval.go`)

Reading the bee retrieval client end-to-end clarified that the
previous ant fetcher's hedging design was wrong in a specific way:
it always raced 3 peers per chunk with a 500 ms hedge, treating
every chunk as if it were tail-slow. Bee instead does:

```
errorsLeft := maxOriginErrors  // 32
forwards   := maxMultiplexForwards  // 2

retry()  // first peer, immediately
preemptiveTicker := 1 * time.Second

for errorsLeft > 0 {
    select {
    case <-preemptiveTicker:
        retry()
    case <-retryC:
        peer, _ := s.closestPeer(addr, fullSkip, origin)
        // dispatch fetch for peer; skip.Forever(addr, peer)
    case res := <-resultC:
        if res.err == nil { return res.chunk, nil }
        errorsLeft--
        s.errSkip.Add(addr, res.peer, time.Minute)  // 60 s peer-skip per chunk
        retry()  // backfill on error
    }
}
```

Three load-bearing pieces:

1. **Per-chunk skip set** (`skip.Forever(addr, peer)` and the 60 s
   `errSkip`). Once we've asked peer P for chunk C, we do not ask
   them again for C — even on retry — so each retry attempt naturally
   uses a fresh peer. Sibling chunks are not affected; P is fair game
   for any chunk address other than C.

2. **Preemptive ticker** (1 s). Every wall-clock second, bee
   dispatches a *new* peer on top of whoever's already in flight.
   This is what gives sustained multi-peer fanout for slow chunks
   without firing a hedge eagerly on every fetch.

3. **Generous error budget** (32 origin errors). Bee tolerates up to
   32 peer errors per chunk before giving up — far more than ant's
   previous 3-deep hedge budget.

Bee also has a `maxMultiplexForwards = 2` branch that fans out
immediately when the *current node* is in the chunk's neighbourhood
(forwarding case). As a light origin we are never in any
neighbourhood, so we don't implement that branch.

### G.2 The "moving window" — `langos`

Bee's HTTP layer (`pkg/api/bzz.go::downloadHandler`) wraps the joiner
in `github.com/ethersphere/langos` before passing it to
`http.ServeContent`:

```go
if size <= 10_000_000 { bufSize = 8 * 32 * 1024 }   // 256 KiB
else                  { bufSize = 16 * 32 * 1024 }  // 512 KiB
http.ServeContent(w, r, "", time.Now(), langos.NewBufferedLangos(reader, bufSize))
```

`langos` is a pipelined read-ahead: on every `Read(p)` it kicks a
`go ReadAt(buf[next], offset)` for the next window in the
background. So while the consumer is digesting `peek[N]`, bee's
joiner is already fetching `peek[N+1]`. Combined with the joiner's
`errgroup.Group` fan-out (`pkg/file/joiner.readAtOffset`), every
peek expands into 64 (small) or 128 (large) concurrent leaf-chunk
fetches.

Effective bee parallelism per request: **128 in-flight chunks**
when the lookahead pipeline is fully primed. Ant's previous design
had a hard cap of `RETRIEVAL_REQUEST_INFLIGHT_CAP = 16`; that's the
direct source of the 2-3× throughput gap.

### G.3 Implementation in ant

#### `crates/ant-retrieval/src/fetcher.rs`

`RoutingFetcher::fetch` rewritten to mirror bee's per-chunk loop:

- New constants: `MAX_ORIGIN_ERRORS = 32` (bee match), `HEDGE_DELAY
  = 4 s` (deliberately wider than bee's 1 s preemptive — see §G.4
  for why).
- Per-fetch `asked: Vec<PeerId>` replaces the previous unconditional
  3-wide race. A new helper `pick_next(&asked)` reads
  `peers_rx.borrow()` afresh on every call and returns the
  closest-by-XOR peer not yet asked, so disconnects mid-fetch
  automatically drop out of the candidate pool.
- Loop structure:
  - Initial dispatch: 1 peer.
  - On per-stream error: backfill the next peer immediately
    (mirrors bee's `retry()` in the error arm).
  - On `HEDGE_DELAY` elapsed with no result: dispatch one more
    peer in parallel and reset the timer.
  - Errors decrement `errors_left`; budget exhaustion + empty
    in-flight pool exits the loop.

The cross-call blacklist (CAC mismatch / framing errors) is kept;
it's distinct from bee's `errSkip` because misbehaving peers
*should* be banned for sibling chunks of the same fetch.

#### `crates/ant-retrieval/src/joiner.rs`

The streaming `join_subtree_to_sender` previously had a
`STREAM_RECURSE_THRESHOLD = 1 MiB` switch: any subtree with a child
spanning more than 1 MiB walked its children sequentially via a
`for` loop. For typical mainnet files (44–112 MiB), this collapsed
the entire root level to single-threaded retrieval — only the
leaves at the deepest level actually fanned out. Removed in favour
of a unified parallel-pipelined design at every level:

```rust
// Per-child bounded mpsc (≈ 256 KiB worth of buffered chunks each)
for spec in specs {
    let (tx, rx) = mpsc::channel(STREAM_PIPE_DEPTH);
    producers.push(async move { join_child_to_sender(...&tx).await });
    child_rxs.push(rx);
}

// Driver: run up to FETCH_FANOUT producers concurrently
let producers_drive = stream::iter(producers).buffer_unordered(fanout)
    .try_collect::<()>();

// Merger: drain receivers strictly left-to-right, forward to `out`
let merger = async move {
    for mut rx in child_rxs {
        while let Some(buf) = rx.recv().await { out.send(buf).await?; }
    }
};

tokio::try_join!(producers_drive, merger)?;
```

Each in-flight sibling subtree gets its own bounded channel
(`STREAM_PIPE_DEPTH = 64` ≈ 256 KiB), so it can keep producing
chunks ahead of the in-order merger until its window fills. The
merger drains receivers strictly left-to-right, so output stays in
document order while later siblings can already be fetching
descendants. With `FETCH_FANOUT = 8` parents in flight at any level,
the joiner keeps roughly `8 × 64 = 512` chunks (≈ 2 MiB) of decoded
look-ahead, comparable to bee's `langos` lookahead layered on top
of its joiner errgroup.

The `STREAM_RECURSE_THRESHOLD` constant and its sequential code
path are deleted.

#### `crates/ant-p2p/src/behaviour.rs`

Two semaphore caps tuned for the new design:

- `RETRIEVAL_INFLIGHT_CAP`: 64 → **256**. A soft global cap that
  protects against pathological loops but no longer constrains
  steady-state throughput; the per-request cap and the pseudosettle
  refresh budget do the actual fairness work.
- `RETRIEVAL_REQUEST_INFLIGHT_CAP`: 16 → **64**. Sized to roughly
  match bee's effective `langos lookahead × 2` parallelism for a
  single file while staying inside the per-peer debt allowance the
  pseudosettle driver can clear (see §G.4).

The streaming `mpsc` between the joiner and the control reply pump
is widened from depth 16 to 64 (≈ 256 KiB of decoded body), so a
slow HTTP consumer doesn't immediately back-pressure all the way
into the joiner's per-child pipes.

#### `crates/ant-p2p/src/pseudosettle.rs`

Two knobs tightened to keep up with the higher chunk-fetch rate the
new joiner drives:

- `MIN_REFRESH_INTERVAL`: 2 s → **1.1 s**. Bee's `peerAllowance`
  granularity is wall-clock seconds, so settling more often than
  once a second is wasted effort, but 1.1 s is the floor that lets
  us collect every per-second `lightRefreshRate * 1 s` slice from
  every hot peer.
- `MAX_INFLIGHT_REFRESHES`: 16 → **32**. At ~250 ms RTT/refresh,
  32 in flight gives ~128 successful refreshes per second — enough
  to cycle a 100-peer warm set every ~0.8 s.

### G.4 The ghost-overdraw discovery (why `HEDGE_DELAY = 4 s`)

The first pass of the new fetcher used a 1 s preemptive ticker
(direct bee match). That repeatedly cancelled in-flight retrievals
when a faster peer's response landed first, and the resulting peer
set collapse from 100 → 37 over the four-track benchmark led us to
trace the exact mechanism in
`bee/pkg/accounting/accounting.go::debitAction.Cleanup`:

```go
func (d *debitAction) Cleanup() {
    if d.applied { return }
    // ...
    d.accountingPeer.ghostBalance += d.price
    if ghostBalance > disconnectLimit {
        _ = a.blocklist(d.peer, 1, "ghost overdraw")  // ← us!
    }
}
```

Bee's retrieval handler calls `accounting.PrepareDebit` *before* it
writes the delivery message. If our peer cancels the substream
before bee's `WriteMsgWithContext + debit.Apply()` succeeds, the
deferred `debit.Cleanup()` still increments their `ghostBalance` by
the chunk price — and once ghostBalance crosses
`lightDisconnectLimit` (≈1.69 M units), bee blocklists us locally
for ~1 s and our connection drops.

Implication: every cancelled hedge is "free" only in the sense that
bee never charges us a *real* debit, but it does charge us a ghost
debit, and ghost debits accumulate against the same disconnect
threshold the real ones do. So preemptive multi-peer dispatch is
*not* free.

The mitigation is simply to make the hedge less aggressive. With
`HEDGE_DELAY = 4 s` (chunks resolving in <4 s never trigger a
hedge) the steady-state rate of cancelled streams drops by ~4×,
and the four-track benchmark goes from peer-set collapse to the
~64-peer steady-state captured in the table at the top of this
appendix.

The 1 s ticker may still be the right answer for a node with a
*slow* uplink (where chunks routinely take seconds), but for the
gateway's typical mainnet RTT the wider hedge dominates.

### G.5 Validation

Same setup as Appendix F.6: dev `antd` on port 1733, dockerized
`ethersphere/bee:2.7.1` on port 1833, both pulling from the live
mainnet, ~100 peers warm on each, `/bytes/<ref>` cold-cache fetches
of four `litter-ally.eth/tracks/` WAV files via `curl --max-time 600`.

Final results (table at top of appendix). All four tracks return
byte-identical content to bee's response (`sha256` match). Peer set
behaviour during the four-track sequence: starts at 100, oscillates
between 60-130 during the first three tracks, descends to ~36 by
the end of the 112 MiB fourth track. The descent is gradual (1-5
disconnects/sec) rather than the cliff-edge collapse the previous
design produced, and pseudosettle continues to accept ~1 GUnit/min
of debt clearance throughout.

The remaining gap on the largest track (308 s vs bee's 205 s) is
the steady-state effect of cumulative ghost-overdraw load on hot
peers: even at a 4 s hedge, a multi-minute fetch still cancels
enough streams that the few peers servicing chunks in the file's
neighbourhood eventually blocklist us. The next round of tuning
should look at per-peer concurrency caps (so chunks spread more
evenly across the 100-peer set) or treat streaming as a literal
queue we read from one peer at a time without ever cancelling.

G.7 takes the second of those routes (cancel-tolerant hedging) and
nearly closes the gap.

### G.6 What's still on the table

In rough order of remaining impact (item 2 in this list was
implemented in G.7 below):

1. **Per-peer in-flight cap.** A `HashMap<PeerId, AtomicUsize>` in
   the fetcher would let `pick_next` skip peers already at, say, 4
   concurrent fetches and pick the next-closest instead. Should
   close most of the remaining gap on the 112 MiB track by spreading
   load away from the hottest 5-10 peers.

2. **Connection-level back-pressure.** Right now an HTTP consumer
   that reads slowly back-pressures into our `mpsc` chain, and the
   joiner just blocks on `tx.send`. That's correct for memory
   bounding but means we're not draining peers as fast as we could
   on a fast consumer. A per-connection adaptive depth would help.

3. **Bee 2.7.1 forwarding-side caching.** Bee's
   `forwarderCaching` flag (off in our test setup) makes a peer
   cache chunks it forwards on our behalf. With it on, repeated
   fetches near the same neighbourhood get warmer peers; without
   it, cold chunks always travel the full forwarding chain.

### G.7 Cancel-tolerant hedging

The `HEDGE_DELAY = 4 s` mitigation in G.4 reduced the *rate* of
ghost overdraws but did not eliminate them: the four-track
benchmark still ended with the peer set drained from 100 → 36, and
the 112 MiB fourth track only finished because it had bled enough
peers off the hot set to slow itself down naturally. The structural
fix is to stop creating ghost debits in the first place.

#### G.7.1 The mechanism, again

`bee/pkg/retrieval/retrieval.go::handler` does:

```go
debit, _ := s.accounting.PrepareDebit(ctx, peer, price)  // accountingPeer.shadowReservedBalance += price
defer debit.Cleanup()                                    // ghost-overdraw on the un-applied path

if err := w.WriteMsgWithContext(ctx, &Delivery{...}); err != nil {
    // never applied → defer hits Cleanup → ghostBalance += price
    return err
}
if err := debit.Apply(); err != nil { ... }              // happy path: real debit, pseudosettle clears it
```

The `WriteMsgWithContext` call is what fails when *we* drop our side
of the libp2p substream. So the moment our `RoutingFetcher` sees a
winner come back and does `drop(in_flight)`, every other in-flight
peer's stream closes mid-write, their `Apply()` never runs, and bee
charges them a ghost debit instead.

Pseudosettle (Appendix F) only clears `accountingPeer.balance`, not
`accountingPeer.ghostBalance`. So accumulated ghost debt stays on
the books indefinitely, and once it crosses `lightDisconnectLimit`
(≈1.69 M units) bee blocklists us.

#### G.7.2 The fix

Instead of `drop(in_flight)`, hand the remaining `FuturesUnordered`
to a detached `tokio::spawn`'d drain task that polls each loser to
completion. Each loser's `retrieve_chunk` finishes reading the
delivery message off the wire — which is all bee's server side
needs to complete `WriteMsgWithContext` and call `debit.Apply()`.
The applied debit lands in `accountingPeer.balance`, where the
pseudosettle driver sees it on its next tick and clears it.

`crates/ant-retrieval/src/fetcher.rs::spawn_drain_losers`:

```rust
fn spawn_drain_losers<S>(
    in_flight: S,
    addr: [u8; 32],
    cache: Option<Arc<InMemoryChunkCache>>,
    record_dir: Option<PathBuf>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
) where
    S: futures::stream::Stream<Item = (PeerId, Result<RetrievedChunk, RetrievalError>)>
        + Send + 'static,
{
    tokio::spawn(async move {
        let mut s = Box::pin(in_flight);
        while let Some((peer, result)) = s.next().await {
            if let Ok(chunk) = result {
                // write-through cache + record + pseudosettle notify
                // (the winner already counted progress, so we skip
                // ProgressTracker here to avoid double-counting bytes)
            }
        }
    });
}
```

Three secondary effects of the drain task:

1. **Cache write-through.** A loser that returns a CAC-valid chunk
   is free bytes; we cache them so a sibling chunk request (or any
   later request) skips the network entirely.
2. **Pseudosettle notify.** Each successful loser triggers a
   `payment_notify.try_send(peer)` so the pseudosettle driver
   promptly clears the just-applied debit.
3. **Permit retention.** The `_request_permit` and `_permit`
   semaphore guards stay held for the lifetime of the spawned task,
   which means concurrent fetches in the same request keep waiting
   on the per-request semaphore until losers actually finish. That
   's the right back-pressure: if a peer is responding slowly,
   sibling chunks stop piling on more requests.

#### G.7.3 Validation

Same setup as G.5 (debug `antd` on port 1635, ~100 peers warm,
cold-cache `/bytes/<ref>` fetches of the same four `litter-ally.eth`
WAVs in sequence):

```
=== Baseline peer set: 100 ===
Track 1 (44 MiB):  result: 200 46064680 83.0s   peers after: 99
Track 2 (59 MiB):  result: 200 61455400 139.0s  peers after: 99
Track 3 (35 MiB):  result: 200 36879400 97.3s   peers after: 99
Track 4 (112 MiB): result: 200 117542440 286.9s peers after: 99
=== Final peer set: 99 ===
```

Peer-set behaviour: **99–100 throughout, no collapse on the fourth
track.** Pseudosettle steady-state (`ant_p2p::pseudosettle: refresh
summary`) over the same 10 minutes:

```
ok=2606 failed=52  units_accepted=1.17 GUnit / 60s
ok=2716 failed=82  units_accepted=1.06 GUnit / 60s
ok=2796 failed=40  units_accepted=0.76 GUnit / 60s
ok=2871 failed=33  units_accepted=0.64 GUnit / 60s
ok=2973 failed=36  units_accepted=0.45 GUnit / 60s
ok=2992 failed=45  units_accepted=0.58 GUnit / 60s
ok=2877 failed=60  units_accepted=1.10 GUnit / 60s
ok=2982 failed=35  units_accepted=0.62 GUnit / 60s
ok=2961 failed=39  units_accepted=0.71 GUnit / 60s
ok=2959 failed=35  units_accepted=0.63 GUnit / 60s
```

~3000 successful pseudosettle round-trips/min sustained, 1-3% fail
rate (transient connection churn), and 0.4-1.2 GUnit/min of debt
cleared. That's ~3-7× the pre-fix steady-state because the drain
task now lets bee complete the write that produces a real debit
instead of dropping it as a ghost.

All four downloaded files match the corresponding bee responses
byte-for-byte (`sha256` cross-checked against `/tmp/bee-<ref>.wav`).

#### G.7.4 Production deployment

Built `0.3.0+cancel-tolerant` release (`target/release/antd`,
13.90 MiB), backed up the previous 0.3.0 binary as
`/usr/local/bin/antd-ant.0.3.0-pre-cancel-tolerant.bak`, restarted
`antd.service`. Peer set warmed to 100 within ~30 s. End-to-end
verification through `https://vibing.at/ant/bytes/<ref>`:

| track    | size     | prod-before (0.3.0) | prod-after (0.3.0+) |
|----------|----------|---------------------|---------------------|
| 02 butterfly | 44 MiB | 173.6 s             | 32.5 s              |
| 06 flutterby | 112 MiB | TIMEOUT (>600 s)   | 139.1 s             |

Final peer count after the 112 MiB production fetch: **100**.
Steady-state pseudosettle: `active_peers=100 ok=2919 failed=28
units_accepted=885220000` per minute.

#### G.7.5 Why this isn't the full ghost-overdraw fix

We still cancel hedges on chunks that *error*: if the loser's
`retrieve_chunk` returns `Err(_)` we just discard it. But errors
are bee's signal that the peer doesn't have the chunk, and on the
error path bee's handler returns *before* `PrepareDebit` is called
— there's nothing to apply or cleanup. So the only ghost debits
left are from clean-cancel races where the `WriteMsgWithContext`
attempt collides exactly with our drop, and those should be rare
relative to the steady stream of successful losing fetches.

If we ever do see ghost-overdraw symptoms again (peer-set
disconnection cliff, pseudosettle dropping below the chunk-fetch
rate), the next lever is item 1 in G.6 (per-peer in-flight cap):
spreading the load across more peers reduces each peer's
shadow-reserved-balance high-water mark, which in turn reduces the
window where a clean-cancel race can land us over
`lightDisconnectLimit`.


## Appendix H — Bee-parity admission control: the `Accounting` mirror

After Appendix G's cancel-tolerant hedging stabilised the peer set
during normal large-file streaming, a follow-up benchmark surfaced
a residual gap: the 112 MiB track still finished in 312 s (vs bee
ultra-light at 195 s on the identical file), and the peer set
quietly halved (100 → 48) over the four-track run. The pattern was
the same root cause but a different angle: bee was RST'ing
saturated forwarders before pseudosettle could refresh them.

Reading bee's `pkg/accounting/accounting.go::PrepareCredit`
revealed three structural pieces ant was missing:

1. **Client-side admission control.** Bee's `prepareCredit` checks
   the *predicted* per-peer balance against
   `disconnectLimit + min(int64(elapsed), 1) × refreshRate` *before*
   dispatching, and refuses to dispatch if the prediction would
   cross. The retrieval loop catches `ErrOverdraft` and skips the
   peer for `overDraftRefresh = 600 ms`, then tries the next-closest
   one. Ant had no client-side equivalent; the only signal a peer
   was over its limit was being RST'd at the TCP layer.

2. **Debt-driven pseudosettle trigger.** Bee fires `RefreshFunc`
   synchronously from `creditAction.Apply` when expected debt
   crosses `earlyPayment = 50 % × paymentThreshold`. Ant's
   pseudosettle driver was purely time-driven on a 1 s tick, so a
   peer that crossed the early-payment line between ticks had to
   wait up to a full second before its refresh dispatched.

3. **Faster preemptive fan-out.** Bee's hedge cadence is 1 s; ant
   was at 4 s. Ant widened the cadence in Appendix G specifically
   to reduce hedge-induced ghost debits. With (1) in place the
   widening becomes unnecessary: hedges land on admissible peers
   only, so the worst case of a 1 s hedge is one extra dispatch on
   a peer with headroom, not a pile-on saturating an already-hot
   peer.

### H.1 Implementation

`crates/ant-retrieval/src/accounting.rs` is a new module that
mirrors bee's per-peer `accountingPeer` struct, restricted to the
four fields that affect admission: `balance`, `reserved`,
`last_refresh`, and `last_used`. The module exposes:

- `Accounting::peer_price(overlay, chunk_addr)` — bee's
  `pricer.PeerPrice` formula
  (`(MAX_PO − proximity + 1) × basePrice`).
- `Accounting::try_reserve(peer, price) -> Option<DebitGuard>` — the
  admission gate. Returns `None` (i.e. `ErrOverdraft`) when
  `balance + reserved + price > disconnectLimit`. The fetcher's
  `pick_next` walks ranked peers, calls `try_reserve` on each, and
  the first peer that admits wins; refused peers go on a per-chunk
  skip list with the [`OVERDRAFT_REFRESH = 600 ms`] TTL.
- `DebitGuard::apply()` — moves `price` from `reserved` to
  `balance` on a successful chunk delivery. If the post-apply
  balance crosses `HOT_DEBT_THRESHOLD`, fires a `HotHint` into the
  pseudosettle driver.
- `DebitGuard::Drop` — releases the reservation without applying
  if the chunk fetch errored or was cancelled. Mirrors bee's
  `creditAction.Cleanup`.
- `Accounting::credit(peer, accepted)` — the pseudosettle ack
  callback. Subtracts `accepted` from `balance`, bumps
  `last_refresh` to `Instant::now()`.
- `Accounting::forget(peer)` — invoked from the swarm's
  `ConnectionClosed` handler so a peer's mirror state matches
  bee's `notifyPeerConnect` reset on reconnect.

The thresholds are sized off bee's constants:

| Constant | Value | Origin |
|----------|-------|--------|
| `LIGHT_DISCONNECT_LIMIT` | 1,687,500 units | `(100+25)% × (paymentThreshold/lightFactor) = 1.25 × (13.5 M / 10)` |
| `LIGHT_REFRESH_RATE_PER_SEC` | 450,000 units/s | `refreshRate / lightFactor = 4.5 M / 10` |
| `OVERDRAFT_LIMIT` | 1,687,500 units | `LIGHT_DISCONNECT_LIMIT` (no allowance term) |
| `HOT_DEBT_THRESHOLD` | 843,750 units | `LIGHT_DISCONNECT_LIMIT / 2`, matching bee's `earlyPayment = 50 %` |
| `OVERDRAFT_REFRESH` | 600 ms | `bee/pkg/retrieval/retrieval.go` |
| `HEDGE_DELAY` | 1 s | bee's `preemptiveInterval` |

`OVERDRAFT_LIMIT` is set to the *lower* of bee's two boundaries
(`disconnectLimit` alone, no `min(elapsed, 1) × refreshRate`
allowance term). Bee's allowance is binary on a one-second
boundary — `min(int64(elapsed), 1) × refreshRate` is *either* 0
*or* `refreshRate`, never anything in between — so any
interpolated estimate would either over- or under-shoot. Picking
the lower bound leaves us with a ~450 k unit safety margin between
"ant refuses to dispatch" and "bee disconnects", which is exactly
one second of pseudosettle work.

The pseudosettle driver in `crates/ant-p2p/src/pseudosettle.rs`
gained two changes:

- A second `mpsc::Receiver<HotHint>` channel. Hot hints set the
  `hot` flag on the per-peer state; the dispatch walk on each
  tick processes hot peers first (sorts by `hot ? 0 : 1` before
  iterating).
- `REFRESH_TICK` was tightened from 1 s to 100 ms. The 100 ms
  cadence makes hot hints land on the next tick rather than
  waiting up to a full second for the periodic walk; the existing
  per-peer `MIN_REFRESH_INTERVAL = 1.1 s` still keeps us from
  spamming bee with same-second refreshes (`ErrSettlementTooSoon`).

The driver also accepts an optional `Arc<Accounting>` and calls
`accounting.credit(peer, accepted)` on every successful refresh,
closing the loop between bee's `PaymentAck.accepted` field and our
admission mirror.

### H.2 Wiring

`SwarmState` gained an `accounting: Option<Arc<Accounting>>` field.
The daemon constructs one shared `Accounting` at startup, hands a
clone to every `RoutingFetcher` it builds (via
`with_accounting`), and the same clone to the pseudosettle driver
for crediting back. `ConnectionClosed` calls
`accounting.forget(&peer)` so a flapping peer's mirror state
doesn't carry stale balance across reconnects.

The fetcher's `pick_next` was rewritten to take `&mut
overdraft_skip` and walk ranked peers calling `try_reserve` on
each. The first admissible peer wins; refused peers go in
`overdraft_skip` with a 600 ms expiry. The dispatched future
carries the `DebitGuard` and applies it on success / drops it on
error. The cancel-tolerant drain task (Appendix G) was extended to
apply guards too — losing hedges that delivered still apply their
debits because bee already ran `creditAction.Apply` on its end.

### H.3 Validation

Test bench: `ant 0.3.x` (this commit, with H.1–H.2 wired in) vs
`bee 2.7.1` ultra-light, both fresh datadirs, both 100/136-peer
warm pools, four-track wav benchmark
(`bee-bench/cmp-bench-test.sh`):

| Track | File | Ant | Bee | Δ |
|-------|------|-----|-----|---|
| 1 | butterfly (44 MiB) | **69.7 s** | 75.0 s | ant +7 % faster |
| 2 | sallarom (59 MiB) | **95.4 s** | 102.8 s | ant +7 % faster |
| 3 | high five (35 MiB) | **54.8 s** | 62.6 s | ant +13 % faster |
| 4 | flutterby (112 MiB) | **184.6 s** | 200.1 s | ant +8 % faster |

All four files completed with byte-perfect SHA-256 match between
ant and bee. Ant's peer set stayed at 100 throughout the entire
12-minute bench (28 total disconnects, 24 of them in the warmup
window before the bench started). Pseudosettle accepted 1.3–2.7
billion units/min across the four tracks, vs ~660 M units/min
during the pre-fix collapse.

Two earlier bench iterations during this work showed the regime
sensitivity:

- With `OVERDRAFT_LIMIT = LIGHT_DISCONNECT_LIMIT +
  LIGHT_REFRESH_RATE_PER_SEC` (the upper bound, matching bee's
  max-allowance line): tracks 1–2 ran at 24 s and 27 s (3× faster
  than bee), but tracks 3–4 collapsed the peer set 100 → 1 and
  failed mid-stream. Pulling the cap down to
  `LIGHT_DISCONNECT_LIMIT` alone trades that 3× speedup for
  stability — ant gets bee-parity throughput with bee-parity
  resilience.
- Two earlier attempts at a per-peer in-flight cap
  (`PER_PEER_INFLIGHT_CAP = 8` and `= 16` in
  `RoutingFetcher::fetch`) both *worsened* stability: forcing
  requests onto less-optimal forwarders increased cancellations,
  which increased ghost debits, which collapsed the peer set
  faster. The admission-control mirror replaces this lever
  cleanly: it spreads load to peers with *headroom* rather than
  to peers chosen by an arbitrary hard cap.

### H.4 Deployment

After validation: bump `0.3.1 → 0.3.2` across all eight workspace
crates per `AGENTS.md`'s policy, build the release binary, deploy
to `vibing.at/ant`, and confirm `antd --version` matches what's
serving. The same `0.3.2` binary serves `*.eth.freedom.baby` since
it's the same `antd` instance.

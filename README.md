# Ant

Ant is a lightweight Swarm light node written in Rust, aimed at mobile and
single-binary desktop/server deployments.

**Project status:** the ultra-light **read path (M2)** is live and deployed —
`antd` joins Swarm mainnet, holds a stable ~100-peer warm set, and serves
bee-compatible downloads over a local HTTP gateway. The **upload path (M3 —
stamps + SWAP + pushsync)** and the **mobile artefact** are not started yet.
Wire formats, CLI flags, and on-disk files may still change.

See [`PLAN.md`](PLAN.md) for the full design; §9.0 "Current status" tracks
what's shipped.

## What works today

- **Mainnet peering.** libp2p (TCP, DNS, Noise, Yamux, identify, ping,
  streams), outbound BZZ Swarm handshake on `/swarm/handshake/14.0.0`, Hive
  v2 peer exchange, forwarding-Kademlia routing bins. Bootstraps from
  `/dnsaddr/mainnet.ethswarm.org` by default; warm-restart peer snapshot
  lands you back at the previous set in seconds.
- **Retrieval.** Bee-parity fetcher with cancel-tolerant hedging
  (Appendix G), admission-control mirror of bee's per-peer accounting
  (Appendix H), and client-side pseudosettle (Appendix F) — peer set
  stays full during sustained large-file streaming.
- **Two-tier chunk cache.** In-memory LRU plus a SQLite-backed persistent
  disk cache (10 GB default, configurable, per-request bypass).
- **Mantaray walker.** `bzz://<root>[/path]` resolution, Swarm-feed
  dereferencing, `website-index-document` fallback.
- **Ordered streaming joiner** with single-range (`Range: bytes=a-b`,
  suffix, open-ended) and `HEAD` support — bodies don't materialise in
  memory.
- **Bee-shaped HTTP gateway (Tier A).** `127.0.0.1:1633` by default, so
  unmodified `bee-js` / `curl` / browser proxies work. Endpoint matrix in
  [`crates/ant-gateway/README.md`](crates/ant-gateway/README.md).
- **Operator tooling.** `antctl status`, `antctl top` (live TUI with
  Peers / Routing / Retrieval / Gateway / Requests tabs), `antctl version`,
  `antctl peers reset`, and `antctl get` for downloads with live progress.

## What's not here yet

- No uploads, no postage stamps, no chequebook / SWAP, no chain plumbing.
  (M3 — not started.)
- No mobile artefact. `ant-ffi` and the Swift `.xcframework` / Kotlin
  `.aar` wrappers are planned but not yet built.
- No Reed-Solomon recovery. Files uploaded with redundancy can be read
  today only via `--allow-degraded-redundancy`, and only if every data
  chunk is reachable.
- `antctl get` streams *progress* but the body still arrives as one
  terminal response over the control socket; browsers hitting the gateway
  get true byte streaming. (Appendix E Phase 5 is the last read-path gap.)

## Workspace

| Crate | Purpose |
|---|---|
| [`antd`](crates/antd) | Daemon binary: CLI, config, signal handling, systemd/launchd shell. |
| [`antctl`](crates/antctl) | Local control client: status, top TUI, `get`, `peers reset`. |
| [`ant-node`](crates/ant-node) | High-level orchestrator wiring `ant-p2p`, `ant-retrieval`, caches, gateway. |
| [`ant-p2p`](crates/ant-p2p) | libp2p host, BZZ handshake, Hive peer exchange, pseudosettle driver. |
| [`ant-retrieval`](crates/ant-retrieval) | Fetcher, ordered joiner with ranges, mantaray walker, in-memory + SQLite caches, bee-parity accounting mirror. |
| [`ant-gateway`](crates/ant-gateway) | Bee-shaped HTTP API (`/bytes`, `/bzz`, `/chunks`, HEAD, Range, plus Tier-A status / stub endpoints). |
| [`ant-control`](crates/ant-control) | Wire types for the `antd` ↔ `antctl` Unix-socket control protocol. |
| [`ant-crypto`](crates/ant-crypto) | secp256k1, keccak256, BMT, CAC/SOC validation, overlay derivation. |

Current workspace version: **`0.3.4`** across all crates (the deployment
bump on each release to `vibing.at/ant`; see `AGENTS.md`).

## Requirements

- Rust stable (pinned in `rust-toolchain.toml`).
- Cargo.

## Development

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Release binary:

```sh
cargo build --release -p antd
./target/release/antd --version
```

## Running `antd`

Start a mainnet light-node daemon with defaults:

```sh
cargo run -p antd
```

By default `antd`:

- stores identity, peer snapshot, and the persistent chunk cache under
  `~/.antd`,
- listens for `antctl` on `~/.antd/antd.sock`,
- serves the bee-shaped HTTP gateway on `127.0.0.1:1633`,
- bootstraps peers from `/dnsaddr/mainnet.ethswarm.org`.

Common options:

```sh
cargo run -p antd -- \
  --data-dir ~/.antd \
  --log-level info \
  --external-address /ip4/<public-ip>/tcp/<public-port> \
  --api-addr 127.0.0.1:1633 \
  --disk-cache-max-gb 10
```

- `--external-address` advertises a publicly routable multiaddr via
  libp2p-identify. Strongly recommended behind NAT — without it, bee
  bootnodes stall the handshake by ~10 s.
- `--no-http-api` disables the HTTP gateway for headless deployments.
- `--no-disk-cache` falls back to memory-only caching.
- `--no-peerstore` / `--reset-peerstore` control the warm-restart peer
  snapshot.

## Using `antctl`

```sh
# One-shot status and daemon version
cargo run -p antctl -- status
cargo run -p antctl -- version

# Live dashboard (peers, routing bins, retrieval, gateway, requests)
cargo run -p antctl -- top

# Drop the on-disk peer snapshot without killing current connections
cargo run -p antctl -- peers reset

# Download by chunk / bytes-tree / bzz-manifest reference
cargo run -p antctl -- get <32-byte-hex>                 # single chunk
cargo run -p antctl -- get bytes://<root>   -o blob.bin  # multi-chunk file
cargo run -p antctl -- get bzz://<root>/index.html       # manifest path
cargo run -p antctl -- get bzz://<root>/video.mp4 -o vid.mp4
```

`antctl get` shows a live progress line by default (chunks, bytes, rate,
peers); use `--progress-style visual` for a retro block-map renderer, or
`--no-progress` to silence it.

## Using the HTTP gateway

The same commands work through `curl` against the running `antd`:

```sh
curl -fsS  http://127.0.0.1:1633/health
curl -fsS  http://127.0.0.1:1633/peers   | jq '.peers | length'
curl -fsSI http://127.0.0.1:1633/bzz/<root>/video.mp4
curl -fsSr 0-1048575 http://127.0.0.1:1633/bzz/<root>/video.mp4 -o head.bin
```

Full endpoint matrix and compatibility notes are in
[`crates/ant-gateway/README.md`](crates/ant-gateway/README.md), and the
normative contract is in PLAN.md Appendix C.

## Further reading

- [`PLAN.md`](PLAN.md) — full implementation plan, milestones, and
  engineering appendices (E: streaming gateway, F: pseudosettle,
  G: cancel-tolerant hedging, H: admission-control mirror).
- [`AGENTS.md`](AGENTS.md) — repo-level conventions (versioning policy,
  deployment notes).

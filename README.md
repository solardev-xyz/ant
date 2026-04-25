# Ant

Ant is a lightweight Swarm light node written in Rust.

Project status: Ant is in early development. Wire formats, CLI flags, and
on-disk files may change; do not treat this as a stable, production-grade node
implementation yet.

Implemented so far:

- Mainnet join: default bootnodes (e.g. `/dnsaddr/mainnet.ethswarm.org`) or
  custom ones; libp2p stack: TCP, DNS, Noise, Yamux, identify, ping, streams;
  outbound BZZ Swarm handshake on `/swarm/handshake/14.0.0/handshake`, then
  dialing via bootstrap and hive-sourced peer hints (default target: hundreds of
  peers with a completed BZZ handshake).
- Identity and state: persisted secp256k1 Swarm identity and overlay, optional
  on-disk peer snapshot for warm restarts.
- Control: Unix domain socket for `antctl` — `status` (incl. JSON), `version`,
  `top` (live TUI), `peers reset`.

The workspace is organized as reusable core crates plus two binaries:

- `antd`: daemon entry point for running a local node.
- `antctl`: command-line control client.
- `ant-node`: node orchestration.
- `ant-p2p`: libp2p networking.
- `ant-control`: local control protocol.
- `ant-crypto`: cryptographic primitives.

## Requirements

- Rust stable, managed by `rust-toolchain.toml`
- Cargo

## Development

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets
```

## Running `antd`

Start a mainnet light-node daemon:

```sh
cargo run -p antd
```

By default, `antd` stores identity and peer data under `~/.antd`, listens on a
local control socket at `~/.antd/antd.sock`, and bootstraps from
`/dnsaddr/mainnet.ethswarm.org`.

Common options:

```sh
cargo run -p antd -- \
  --data-dir ~/.antd \
  --log-level info \
  --external-address /ip4/<public-ip>/tcp/<public-port>
```

`--external-address` is useful when the node is reachable from the public
internet. It gives bee peers an advertised address for the BZZ handshake.

Disable or reset the warm-restart peer snapshot:

```sh
cargo run -p antd -- --no-peerstore
cargo run -p antd -- --reset-peerstore
```

## Inspecting With `antctl`

Show daemon status:

```sh
cargo run -p antctl -- status
```

Open the live status TUI:

```sh
cargo run -p antctl -- top
```

Reset the daemon peer snapshot while it is running:

```sh
cargo run -p antctl -- peers reset
```

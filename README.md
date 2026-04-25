# Ant

Ant is a lightweight Swarm light node written in Rust.

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

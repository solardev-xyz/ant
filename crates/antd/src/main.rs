//! `antd` — Swarm light node daemon (M1.0: mainnet dial + BZZ handshake).

use ant_control::{
    ControlCommand, GatewayActivity, IdentityInfo, PeerInfo, StatusSnapshot, PROTOCOL_VERSION,
};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, random_overlay_nonce,
    random_secp256k1_secret, SECP256K1_SECRET_LEN,
};
use ant_gateway::{Gateway, GatewayHandle, GatewayIdentity};
use ant_node::{run_node, NodeConfig};
use ant_p2p::UploadRuntime;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use fs4::{FileExt, TryLockError};
use k256::ecdsa::SigningKey;
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

const AGENT: &str = concat!("antd/", env!("CARGO_PKG_VERSION"));
/// Pinned bee API version this build advertises via `/health.apiVersion`.
/// Bumped per Ant release per PLAN.md §C.5; the constant lives in the
/// binary (not in `ant-gateway`) so the gateway crate stays bee-version
/// agnostic and can be reused in other embedders.
const BEE_API_VERSION: &str = "7.2.0";

#[derive(Parser, Debug)]
#[command(name = "antd", version, about = "Ant Swarm light node (M1.0)")]
struct Opt {
    /// Data directory (identity + nonce persisted here).
    #[arg(long, default_value = "~/.antd")]
    data_dir: PathBuf,

    /// Swarm network id (mainnet = 1).
    #[arg(long, default_value_t = 1)]
    network_id: u64,

    /// Comma-separated bootnode multiaddrs (default: mainnet bootnodes).
    #[arg(long, value_delimiter = ',')]
    bootnodes: Option<Vec<String>>,

    /// Log level (RUST_LOG syntax), e.g. `info`, `debug`.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Path to secp256k1 signing key file (32 raw bytes hex). Generated if missing.
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// Path to the `antctl` control socket. Defaults to `<data-dir>/antd.sock`.
    #[arg(long)]
    control_socket: Option<PathBuf>,

    /// Disable the `antctl` control socket entirely.
    #[arg(long, default_value_t = false)]
    no_control_socket: bool,

    /// Address `ant-gateway` listens on for the bee-shaped HTTP API.
    /// Default mirrors bee's own default (`127.0.0.1:1633`) so unmodified
    /// `bee-js` clients work against `antd` without configuration.
    #[arg(long, default_value = "127.0.0.1:1633")]
    api_addr: SocketAddr,

    /// Disable the bee-shaped HTTP API entirely. Useful for headless
    /// deployments that only need `antctl` access via the control
    /// socket.
    #[arg(long, default_value_t = false)]
    no_http_api: bool,

    /// Externally reachable multiaddrs to advertise via libp2p-identify.
    /// Bee bootnodes stall our handshake by 10 s when their peerstore lacks a
    /// publicly-routable multiaddr for us, so operators behind NAT should
    /// pass their public ip/tcp multiaddr here (comma-separated for multiple).
    #[arg(long, value_delimiter = ',')]
    external_address: Vec<String>,

    /// Path to the JSON peer snapshot. Defaults to `<data-dir>/peers.json`.
    /// Loaded at startup to warm the dial pipeline so the next run skips the
    /// bootnode hop, flushed every 30 s and on shutdown.
    #[arg(long)]
    peers_file: Option<PathBuf>,

    /// Disable the peer snapshot entirely. Every restart will re-bootstrap
    /// through the bootnodes; useful for diagnosing peer-discovery issues.
    #[arg(long, default_value_t = false)]
    no_peerstore: bool,

    /// Delete `<data-dir>/peers.json` (or `--peers-file`) before startup.
    /// One-shot equivalent of `antctl peers reset` for when the daemon isn't
    /// running yet.
    #[arg(long, default_value_t = false)]
    reset_peerstore: bool,

    /// Scope the in-memory chunk cache to a single `antctl get` invocation
    /// instead of sharing it across the daemon's lifetime. Each request
    /// still gets a cache (so retries within the same request skip the
    /// network for chunks already pulled), but a second request for the
    /// same reference starts cold. Useful for reproducing transient
    /// retrieval failures and benchmarking cold-path latency.
    #[arg(long, default_value_t = false)]
    per_request_chunk_cache: bool,

    /// Dump every chunk fetched during a `GetBytes` / `GetBzz` request
    /// to `<DIR>/<hex_addr>.bin` (raw wire bytes: 8-byte LE span ||
    /// payload). Combined with the `MapFetcher::from_dir` helper this
    /// lets the integration tests in `ant-retrieval` replay a real
    /// `antctl get` offline. Debug builds only; release `antd` does not
    /// expose the flag. The directory is created if missing.
    #[cfg(debug_assertions)]
    #[arg(long, value_name = "DIR")]
    record_chunks: Option<PathBuf>,

    /// Path to the persistent (SQLite-backed) chunk cache. Defaults to
    /// `<data-dir>/chunks.sqlite`. Holds the second-tier cache that
    /// sits between the in-memory LRU and the network retrieval path
    /// (see PLAN.md § 6.1).
    #[arg(long)]
    disk_cache_path: Option<PathBuf>,

    /// Hard upper bound on the persistent chunk cache size, in
    /// gigabytes. Defaults to 10 GB (matches the `antd` desktop /
    /// Raspberry Pi default in PLAN.md § 6.1). When the on-disk total
    /// crosses this cap, the cache evicts oldest-by-`last_access`
    /// rows down to ~95% of the cap.
    #[arg(long, default_value_t = 10)]
    disk_cache_max_gb: u64,

    /// Disable the persistent chunk cache entirely. Retrieval falls
    /// back to the legacy `memory -> network` lookup order. Useful
    /// when the disk has no spare capacity for a long-lived cache,
    /// or when you want every restart to start cold.
    #[arg(long, default_value_t = false)]
    no_disk_cache: bool,

    /// Gnosis Chain JSON-RPC endpoint used to read the postage batch
    /// metadata at startup (depth, bucket depth, immutability flag).
    /// Falls back to `GNOSIS_RPC_URL` env var. Required when
    /// `--postage-batch` is set.
    #[arg(long)]
    gnosis_rpc_url: Option<String>,

    /// PostageStamp contract address (mainnet default keeps matching
    /// upstream bee). Override only when running against a fork.
    #[arg(long, default_value = "0x45a1502382541Cd610CC9068e88727426b696293")]
    postage_contract: String,

    /// 32-byte postage batch id (`0x` + 64 hex). Enables uploads
    /// (`POST /chunks`, gateway `/bytes`) by binding stamps to this
    /// batch on the chain. Falls back to `STORAGE_STAMP_BATCH_ID` env.
    #[arg(long)]
    postage_batch: Option<String>,

    /// 32-byte secp256k1 secret of the batch owner (the address
    /// returned by `batchOwner(batch_id)` on the PostageStamp
    /// contract). Required to sign stamps. Falls back to
    /// `STORAGE_STAMP_PRIVATE_KEY` env. The bee node mnemonic at
    /// derivation path `m/44'/60'/0'/0/1` produces this key for the
    /// default Ant test setup.
    #[arg(long)]
    postage_owner_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    /// 32-byte secp256k1 secret (hex).
    signing_key: String,
    /// 32-byte overlay nonce (hex).
    overlay_nonce: String,
    /// libp2p identity as protobuf (hex), or regenerated from signing key if absent.
    #[serde(default)]
    libp2p_keypair: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let process_start = Instant::now();
    let opt = Opt::parse();
    let data_dir = expand_tilde(&opt.data_dir);
    std::fs::create_dir_all(&data_dir).with_context(|| format!("create data dir {data_dir:?}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&opt.log_level)),
        )
        .init();

    // Held for the lifetime of the daemon: dropping the `File` releases the
    // advisory `flock`. Bound at function scope (not in a helper) so it stays
    // alive until `main` returns.
    let _instance_lock = acquire_instance_lock(&data_dir.join("antd.lock"))?;

    raise_nofile_soft_limit();

    let id_path = data_dir.join("identity.json");
    let key_path = opt.key_file.unwrap_or_else(|| data_dir.join("signing.key"));

    let (signing_secret, overlay_nonce, libp2p_keypair) =
        load_or_create_identity(&id_path, &key_path)?;

    let vk = *(SigningKey::from_bytes((&signing_secret).into())
        .context("invalid signing key")?
        .verifying_key());
    let eth = ethereum_address_from_public_key(&vk);
    let overlay = overlay_from_ethereum_address(&eth, opt.network_id, &overlay_nonce);
    let public_key_compressed = vk.to_encoded_point(true).as_bytes().to_vec();
    tracing::info!(
        target: "antd",
        "loaded identity eth=0x{} overlay=0x{}",
        hex::encode(eth),
        hex::encode(overlay),
    );

    let bootnodes: Vec<_> = match &opt.bootnodes {
        Some(v) => v.iter().filter_map(|s| s.parse().ok()).collect(),
        None => ant_p2p::default_mainnet_bootnodes(),
    };

    let external_addrs: Vec<libp2p::multiaddr::Multiaddr> = opt
        .external_address
        .iter()
        .filter_map(|s| match s.parse() {
            Ok(m) => Some(m),
            Err(e) => {
                tracing::warn!(target: "antd", "skipping invalid --external-address {s:?}: {e}");
                None
            }
        })
        .collect();

    let peer_id = libp2p_keypair.public().to_peer_id();
    let control_socket = opt
        .control_socket
        .clone()
        .map(|p| expand_tilde(&p))
        .unwrap_or_else(|| data_dir.join("antd.sock"));

    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let initial_snapshot = StatusSnapshot {
        agent: AGENT.to_string(),
        protocol_version: PROTOCOL_VERSION,
        network_id: opt.network_id,
        pid: std::process::id(),
        started_at_unix,
        identity: IdentityInfo {
            eth_address: format!("0x{}", hex::encode(eth)),
            overlay: format!("0x{}", hex::encode(overlay)),
            peer_id: peer_id.to_string(),
        },
        peers: PeerInfo {
            node_limit: ant_p2p::DEFAULT_TARGET_PEERS as u32,
            ..PeerInfo::default()
        },
        listeners: Vec::new(),
        control_socket: control_socket.display().to_string(),
        retrieval: Default::default(),
    };
    let (status_tx, status_rx) = watch::channel(initial_snapshot);

    let peerstore_path = if opt.no_peerstore {
        None
    } else {
        Some(
            opt.peers_file
                .clone()
                .map(|p| expand_tilde(&p))
                .unwrap_or_else(|| data_dir.join("peers.json")),
        )
    };
    if let Some(p) = &peerstore_path {
        tracing::info!(target: "antd", "peer snapshot at {}", p.display());
        if opt.reset_peerstore {
            match std::fs::remove_file(p) {
                Ok(()) => tracing::info!(
                    target: "antd",
                    "--reset-peerstore: removed {}",
                    p.display(),
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => tracing::info!(
                    target: "antd",
                    "--reset-peerstore: {} did not exist, nothing to do",
                    p.display(),
                ),
                Err(e) => tracing::warn!(
                    target: "antd",
                    "--reset-peerstore: failed to unlink {}: {e}",
                    p.display(),
                ),
            }
        }
    } else if opt.reset_peerstore {
        tracing::warn!(
            target: "antd",
            "--reset-peerstore ignored: peerstore is disabled (--no-peerstore)",
        );
    }

    // Control socket → node loop command channel. Sized at 32: control
    // commands are rare (operator-driven), and even a burst from a scripted
    // client is trivially below this depth. Allocating the channel even when
    // `--no-control-socket` is set keeps `NodeConfig` shape consistent; the
    // sender is dropped below and the receiver's select arm idles on a
    // perpetual `recv().await`.
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(32);

    if opt.per_request_chunk_cache {
        tracing::info!(
            target: "antd",
            "chunk cache scoped to per-request (--per-request-chunk-cache)",
        );
    }

    let chunk_record_dir = resolve_chunk_record_dir(
        #[cfg(debug_assertions)]
        opt.record_chunks.as_deref(),
    )?;

    let disk_cache = if opt.no_disk_cache {
        tracing::info!(target: "antd", "persistent chunk cache disabled (--no-disk-cache)");
        None
    } else {
        let path = opt
            .disk_cache_path
            .clone()
            .map(|p| expand_tilde(&p))
            .unwrap_or_else(|| data_dir.join("chunks.sqlite"));
        // Convert the GB cap to bytes once at startup. Saturating mul
        // means a hypothetical operator typo of `--disk-cache-max-gb
        // 18446744073` (a u64 GB count that overflows on multiplication)
        // produces "as big as we can represent", not a 0-byte cap.
        let max_bytes = opt
            .disk_cache_max_gb
            .saturating_mul(1024 * 1024 * 1024);
        match ant_retrieval::DiskChunkCache::open(&path, max_bytes) {
            Ok(c) => {
                tracing::info!(
                    target: "antd",
                    path = %path.display(),
                    max_gb = opt.disk_cache_max_gb,
                    used_bytes = c.used_bytes(),
                    "opened persistent chunk cache",
                );
                Some(Arc::new(c))
            }
            Err(e) => {
                // A failed disk-cache open is recoverable: log loudly
                // and fall back to the in-memory tier alone, rather
                // than refusing to start the daemon. Operators on a
                // full disk would otherwise lose remote retrieval
                // entirely until the disk gets cleared.
                tracing::warn!(
                    target: "antd",
                    path = %path.display(),
                    "failed to open persistent chunk cache: {e}; falling back to in-memory only",
                );
                None
            }
        }
    };

    // Single shared registry for in-flight gateway HTTP requests.
    // Built unconditionally so the node loop can read from it; only
    // the gateway side ever writes when `--no-http-api` is set, in
    // which case `gateway_activity` stays empty and `antctl top`
    // shows zero gateway rows. Empty registry costs one `Arc` and
    // a `Mutex<HashMap>` — well below a rounding error.
    let gateway_activity: Arc<GatewayActivity> = GatewayActivity::new();

    let upload = build_upload_runtime(
        opt.gnosis_rpc_url.clone(),
        opt.postage_contract.clone(),
        opt.postage_batch.clone(),
        opt.postage_owner_key.clone(),
        data_dir.clone(),
    )
    .await
    .map_err(|e| anyhow!("upload runtime: {e}"))?;

    let node_fut = run_node(
        NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
            .with_status(status_tx)
            .with_process_start(process_start)
            .with_external_addrs(external_addrs)
            .with_peerstore_path(peerstore_path)
            .with_commands(cmd_rx)
            .with_per_request_chunk_cache(opt.per_request_chunk_cache)
            .with_chunk_record_dir(chunk_record_dir)
            .with_gateway_activity(Some(gateway_activity.clone()))
            .with_disk_cache(disk_cache)
            .with_upload(upload),
    );

    let gateway_handle = if opt.no_http_api {
        None
    } else {
        Some(GatewayHandle {
            agent: Arc::new(AGENT.to_string()),
            api_version: Arc::new(BEE_API_VERSION.to_string()),
            identity: Arc::new(GatewayIdentity {
                overlay_hex: hex::encode(overlay),
                ethereum_hex: format!("0x{}", hex::encode(eth)),
                public_key_hex: hex::encode(&public_key_compressed),
                peer_id: peer_id.to_string(),
            }),
            status: status_rx.clone(),
            commands: cmd_tx.clone(),
            activity: gateway_activity.clone(),
        })
    };

    if opt.no_control_socket && gateway_handle.is_none() {
        drop(cmd_tx);
        return node_fut.await.map_err(|e| anyhow::anyhow!("{e}"));
    }

    let api_addr = opt.api_addr;
    let gateway_fut = async move {
        match gateway_handle {
            Some(handle) => Gateway::serve(handle, api_addr)
                .await
                .map_err(|e| anyhow::anyhow!("gateway: {e}")),
            None => std::future::pending::<Result<()>>().await,
        }
    };

    if opt.no_control_socket {
        drop(cmd_tx);
        tokio::select! {
            res = node_fut => res.map_err(|e| anyhow::anyhow!("{e}")),
            res = gateway_fut => res,
        }
    } else {
        let control_path = control_socket.clone();
        let control_fut =
            ant_control::serve(control_path, AGENT.to_string(), status_rx, Some(cmd_tx));
        tracing::info!(
            target: "antd",
            "control socket at {}",
            control_socket.display(),
        );

        tokio::select! {
            res = node_fut => res.map_err(|e| anyhow::anyhow!("{e}")),
            res = control_fut => match res {
                Ok(()) => Ok(()),
                Err(e) => Err(anyhow::anyhow!("control socket: {e}")),
            },
            res = gateway_fut => res,
        }
    }
}

/// Resolve the optional `--record-chunks <DIR>` value to an absolute path
/// (with `~` expanded) and ensure the directory exists, so the
/// `RoutingFetcher` write path can drop a chunk into it without first
/// stat-ing the parent. Returns `Ok(None)` in release builds (the flag
/// is hidden from clap there) and when the operator didn't pass the
/// flag in debug builds; `Err` if `mkdir -p` failed.
fn resolve_chunk_record_dir(
    #[cfg(debug_assertions)] record_chunks: Option<&Path>,
) -> Result<Option<PathBuf>> {
    #[cfg(debug_assertions)]
    {
        if let Some(p) = record_chunks {
            let dir = expand_tilde(p);
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("create --record-chunks dir {dir:?}"))?;
            tracing::info!(
                target: "antd",
                "recording every fetched chunk to {} (--record-chunks)",
                dir.display(),
            );
            return Ok(Some(dir));
        }
    }
    Ok(None)
}

/// Take an exclusive advisory lock on `<data-dir>/antd.lock` so a second
/// `antd` against the same data directory fails fast instead of silently
/// joining the listen queue on `antd.sock` (which leads to split-brain
/// answers — half your `antctl` calls hit the new daemon, half the old).
///
/// Uses `flock(2)` via fs4. The lock lives on the file *descriptor*, so it
/// is released automatically when the process exits — clean shutdown,
/// panic, OOM, or `kill -9`. No stale-pid guesswork required.
fn acquire_instance_lock(lock_path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .with_context(|| format!("open instance lock {lock_path:?}"))?;
    match FileExt::try_lock(&file) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            let owner = std::fs::read_to_string(lock_path).unwrap_or_default();
            let owner = owner.trim();
            let hint = if owner.is_empty() {
                String::new()
            } else {
                format!(" (pid {owner})")
            };
            return Err(anyhow!(
                "another antd is already running against {}{hint}; \
                 refusing to start (delete the file only if you're sure no antd is alive)",
                lock_path.display(),
            ));
        }
        Err(TryLockError::Error(e)) => {
            return Err(anyhow!(e)).with_context(|| format!("flock {lock_path:?}"));
        }
    }
    let mut writable = &file;
    writable.set_len(0).ok();
    writable.seek(SeekFrom::Start(0)).ok();
    writeln!(writable, "{}", std::process::id()).ok();
    tracing::info!(
        target: "antd",
        "acquired instance lock {} (pid {})",
        lock_path.display(),
        std::process::id(),
    );
    Ok(file)
}

/// Bump the soft `RLIMIT_NOFILE` to the hard cap so a busy peer set + dial
/// fan-out doesn't trip the default 1024-fd ulimit on macOS / Linux. We
/// silently keep whatever the OS already gave us if the bump fails — running
/// with too few fds is a degraded but legitimate state, not a startup error.
fn raise_nofile_soft_limit() {
    use rlimit::{getrlimit, setrlimit, Resource};
    let Ok((soft, hard)) = getrlimit(Resource::NOFILE) else {
        return;
    };
    // macOS' real per-process cap is `OPEN_MAX = 10240`, even when `hard`
    // reports `RLIM_INFINITY`. Clamp so `setrlimit` doesn't EINVAL on Darwin.
    let target = hard.min(10240).max(soft);
    if target == soft {
        return;
    }
    if let Err(e) = setrlimit(Resource::NOFILE, target, hard) {
        tracing::warn!(
            target: "antd",
            "could not raise RLIMIT_NOFILE from {soft} to {target}: {e}; \
             expect 'too many open files' under load",
        );
        return;
    }
    tracing::info!(target: "antd", "raised RLIMIT_NOFILE soft limit {soft} → {target}");
}

fn expand_tilde(p: &Path) -> PathBuf {
    if let Some(s) = p.to_str() {
        if let Some(rest) = s.strip_prefix("~/") {
            return dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(rest);
        }
    }
    p.to_path_buf()
}

fn load_or_create_identity(
    id_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; 32], Keypair)> {
    if id_path.exists() {
        let raw = std::fs::read_to_string(id_path).with_context(|| format!("read {id_path:?}"))?;
        let id: IdentityFile = serde_json::from_str(&raw).context("parse identity.json")?;
        let mut signing_secret = [0u8; SECP256K1_SECRET_LEN];
        hex::decode_to_slice(&id.signing_key, &mut signing_secret).context("decode signing_key")?;
        let mut overlay_nonce = [0u8; 32];
        hex::decode_to_slice(&id.overlay_nonce, &mut overlay_nonce)
            .context("decode overlay_nonce")?;
        let kp = if let Some(ref enc) = id.libp2p_keypair {
            let bytes = hex::decode(enc).context("decode libp2p_keypair")?;
            Keypair::from_protobuf_encoding(&bytes).context("libp2p keypair protobuf")?
        } else {
            secp256k1_keypair_from_signing_secret(&signing_secret)?
        };
        return Ok((signing_secret, overlay_nonce, kp));
    }

    let signing_secret = random_secp256k1_secret();
    let overlay_nonce = random_overlay_nonce();
    let kp = secp256k1_keypair_from_signing_secret(&signing_secret)?;

    let id = IdentityFile {
        signing_key: hex::encode(signing_secret),
        overlay_nonce: hex::encode(overlay_nonce),
        libp2p_keypair: Some(hex::encode(kp.to_protobuf_encoding()?)),
    };
    std::fs::write(
        id_path,
        serde_json::to_string_pretty(&id).context("serialize identity")?,
    )
    .with_context(|| format!("write {id_path:?}"))?;

    // Optional raw key file for tooling.
    let _ = std::fs::write(key_path, hex::encode(signing_secret));

    Ok((signing_secret, overlay_nonce, kp))
}

/// Build a [`UploadRuntime`] from CLI flags + env. Returns `Ok(None)`
/// when the operator opts out of uploads (no `--postage-batch` /
/// `STORAGE_STAMP_BATCH_ID`), so `antd` keeps booting in read-only mode.
///
/// On success, fetches `batchOwner / batchDepth / batchBucketDepth /
/// batchImmutableFlag` from the on-chain PostageStamp contract,
/// cross-checks the configured signing key against the chain-recorded
/// owner, and opens the persistent [`StampIssuer`] at
/// `<data_dir>/postage/<batch_id>.bin`. The store is created on first
/// boot and reloaded thereafter so a restart picks up stamps from
/// where the previous process left off (M3 Phase 8 — without this, a
/// restart would reissue indices the network already saw and bee
/// peers would reject our stamps as double-spends).
async fn build_upload_runtime(
    cli_rpc: Option<String>,
    postage_contract: String,
    cli_batch: Option<String>,
    cli_key: Option<String>,
    data_dir: PathBuf,
) -> Result<Option<Arc<UploadRuntime>>> {
    let batch_hex = cli_batch
        .or_else(|| std::env::var("STORAGE_STAMP_BATCH_ID").ok())
        .map(|s| s.trim().to_string());
    let Some(batch_hex) = batch_hex.filter(|s| !s.is_empty()) else {
        tracing::info!(
            target: "antd",
            "uploads disabled: pass --postage-batch (or set STORAGE_STAMP_BATCH_ID) to enable POST /chunks",
        );
        return Ok(None);
    };
    let key_hex = cli_key
        .or_else(|| std::env::var("STORAGE_STAMP_PRIVATE_KEY").ok())
        .ok_or_else(|| anyhow!("--postage-batch set but --postage-owner-key / STORAGE_STAMP_PRIVATE_KEY missing"))?;
    let rpc_url = cli_rpc
        .or_else(|| std::env::var("GNOSIS_RPC_URL").ok())
        .ok_or_else(|| anyhow!("--postage-batch set but --gnosis-rpc-url / GNOSIS_RPC_URL missing"))?;

    let mut batch_id = [0u8; 32];
    hex::decode_to_slice(strip_0x(&batch_hex), &mut batch_id)
        .with_context(|| format!("decode batch id {batch_hex}"))?;
    let mut stamp_key = [0u8; SECP256K1_SECRET_LEN];
    hex::decode_to_slice(strip_0x(&key_hex), &mut stamp_key)
        .context("decode postage owner key")?;

    let signer_eth = {
        let sk =
            SigningKey::from_bytes((&stamp_key).into()).context("postage owner key invalid")?;
        ethereum_address_from_public_key(sk.verifying_key())
    };

    let chain = ant_chain::ChainClient::new(rpc_url);
    let meta = ant_chain::fetch_postage_batch_meta(&chain, &postage_contract, &batch_id)
        .await
        .with_context(|| format!("fetch postage batch meta from {postage_contract}"))?;

    if signer_eth != meta.batch_owner_eth {
        return Err(anyhow!(
            "postage owner key 0x{} does not match on-chain batchOwner 0x{} for batch {}",
            hex::encode(signer_eth),
            hex::encode(meta.batch_owner_eth),
            batch_hex,
        ));
    }

    let postage_dir = data_dir.join("postage");
    let store_path = postage_dir.join(format!("{}.bin", hex::encode(batch_id)));
    let issuer = ant_postage::StampIssuer::open_or_new(
        store_path.clone(),
        batch_id,
        meta.depth,
        meta.bucket_depth,
        meta.immutable,
    )
    .map_err(|e| anyhow!("StampIssuer: {e}"))?;
    let resumed_count = issuer.issued_count();
    tracing::info!(
        target: "antd",
        batch = %format!("0x{}", hex::encode(batch_id)),
        owner = %format!("0x{}", hex::encode(meta.batch_owner_eth)),
        depth = meta.depth,
        bucket_depth = meta.bucket_depth,
        immutable = meta.immutable,
        store = %store_path.display(),
        resumed_indices = resumed_count,
        "uploads enabled (postage stamping ready)",
    );

    Ok(Some(Arc::new(UploadRuntime {
        issuer: std::sync::Mutex::new(issuer),
        stamp_key,
        batch_owner: meta.batch_owner_eth,
    })))
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s)
}

fn secp256k1_keypair_from_signing_secret(secret: &[u8; SECP256K1_SECRET_LEN]) -> Result<Keypair> {
    let mut sk_copy = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut sk_copy)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(kp))
}

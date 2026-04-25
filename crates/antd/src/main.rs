//! `antd` — Swarm light node daemon (M1.0: mainnet dial + BZZ handshake).

use ant_control::{ControlCommand, IdentityInfo, PeerInfo, StatusSnapshot, PROTOCOL_VERSION};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, random_overlay_nonce,
    random_secp256k1_secret, SECP256K1_SECRET_LEN,
};
use ant_node::{run_node, NodeConfig};
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use fs4::{FileExt, TryLockError};
use k256::ecdsa::SigningKey;
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

const AGENT: &str = concat!("antd/", env!("CARGO_PKG_VERSION"));

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

    let node_fut = run_node(
        NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
            .with_status(status_tx)
            .with_process_start(process_start)
            .with_external_addrs(external_addrs)
            .with_peerstore_path(peerstore_path)
            .with_commands(cmd_rx),
    );

    if opt.no_control_socket {
        drop(cmd_tx);
        return node_fut.await.map_err(|e| anyhow::anyhow!("{e}"));
    }

    let control_path = control_socket.clone();
    let control_fut = ant_control::serve(control_path, AGENT.to_string(), status_rx, Some(cmd_tx));
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
    }
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

fn secp256k1_keypair_from_signing_secret(secret: &[u8; SECP256K1_SECRET_LEN]) -> Result<Keypair> {
    let mut sk_copy = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut sk_copy)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(kp))
}

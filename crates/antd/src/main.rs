//! `antd` — Swarm light node daemon (M1.0: mainnet dial + BZZ handshake).

mod chainreader;
mod config;
mod keystore;

use ant_control::{
    ControlCommand, GatewayActivity, IdentityInfo, PeerInfo, RetrievalInfo, StatusSnapshot,
    PROTOCOL_VERSION,
};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, random_overlay_nonce,
    random_secp256k1_secret, SECP256K1_SECRET_LEN,
};
use ant_gateway::{Gateway, GatewayHandle, GatewayIdentity, TagRegistry};
use ant_node::{run_node, NodeConfig};
use ant_p2p::UploadRuntime;
use anyhow::{anyhow, bail, Context, Result};
use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches, Parser};
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
    /// Path to a bee-compatible YAML config file (PLAN.md J.5.E1/E2).
    /// Lets Freedom launch `antd` with the same config it writes for
    /// bee. Explicit CLI flags override values from this file, which
    /// override the built-in defaults. Recognised keys: `api-addr`,
    /// `data-dir`, `password` / `password-file`, `mainnet` /
    /// `network-id`, `blockchain-rpc-endpoint`, `cors-allowed-origins`,
    /// `verbosity`; other bee keys are accepted and ignored.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Password used to decrypt a bee `keys/swarm.key` v3 keystore
    /// (PLAN.md J.5.E3). Usually supplied via the config file's
    /// `password` / `password-file`; this flag is the CLI override.
    #[arg(long)]
    password: Option<String>,

    /// File whose (trimmed) contents are the keystore password. CLI
    /// equivalent of bee's `password-file`.
    #[arg(long)]
    password_file: Option<PathBuf>,

    /// Data directory (identity + nonce persisted here).
    #[arg(long, default_value = "~/.antd")]
    data_dir: PathBuf,

    /// Swarm network id (mainnet = 1).
    #[arg(long, default_value_t = 1)]
    network_id: u64,

    /// Comma-separated bootnode multiaddrs (default: mainnet bootnodes).
    #[arg(long, value_delimiter = ',')]
    bootnodes: Option<Vec<String>>,

    /// Log level (`RUST_LOG` syntax), e.g. `info`, `debug`.
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

    /// CORS allowed origins for the HTTP API (bee's
    /// `cors-allowed-origins`). Comma-separated; `*` allows any origin
    /// and the literal `null` allows opaque-origin pages. Freedom sets
    /// this to `null` so its `bzz://` dweb pages can call `window.swarm`
    /// (PLAN.md J.4.8). Empty (default) disables CORS, matching a bee
    /// node started without the option.
    #[arg(long, value_delimiter = ',')]
    cors_allowed_origins: Vec<String>,

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

    /// Override the peer-set target — how many BZZ-handshake-complete peers
    /// the swarm loop tries to keep online. Default (`None`) leaves
    /// `ant-p2p`'s baked-in `DEFAULT_TARGET_PEERS` (100), which is the
    /// right size for cheap-CPU light-mode operation. Override (e.g. 200)
    /// to widen the network-wide credit budget for long uploads — bee
    /// grants each peer ~5 K PLUR/sec of pseudosettle credit, so doubling
    /// the peer set roughly doubles per-second debit headroom and shaves
    /// the throughput-throttling wall.
    #[arg(long)]
    target_peers: Option<usize>,

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

    /// `PostageStamp` contract address (mainnet default keeps matching
    /// upstream bee). Override only when running against a fork.
    #[arg(long, default_value = "0x45a1502382541Cd610CC9068e88727426b696293")]
    postage_contract: String,

    /// 32-byte postage batch id (`0x` + 64 hex). Enables uploads
    /// (`POST /chunks`, gateway `/bytes`) by binding stamps to this
    /// batch on the chain. Falls back to `STORAGE_STAMP_BATCH_ID` env.
    #[arg(long)]
    postage_batch: Option<String>,

    /// 32-byte secp256k1 secret of the batch owner (the address
    /// returned by `batchOwner(batch_id)` on the `PostageStamp`
    /// contract). Required to sign stamps. Falls back to
    /// `STORAGE_STAMP_PRIVATE_KEY` env. The bee node mnemonic at
    /// derivation path `m/44'/60'/0'/0/1` produces this key for the
    /// default Ant test setup.
    #[arg(long)]
    postage_owner_key: Option<String>,

    /// On startup, do **not** auto-resume upload jobs that were in
    /// `running` state when the daemon last shut down. They're
    /// loaded into memory and listed by `antctl upload list` as
    /// `paused`, so the operator can inspect them and `antctl
    /// upload resume <id>` deliberately. Default behaviour
    /// (auto-resume) matches the "I closed my laptop"
    /// expectation and is what most operators want.
    #[arg(long, default_value_t = false)]
    no_resume_uploads: bool,

    /// 20-byte chequebook contract address (`0x` + 40 hex). When set
    /// alongside `--swap-key`, the daemon enables outbound SWAP
    /// settlement (Phase 7b): every successful pushsync push accrues
    /// debt against the receiver's beneficiary EOA, and an EIP-712
    /// cheque is emitted automatically once the per-peer debt crosses
    /// `LIGHT_PAYMENT_THRESHOLD / 2` (≈ 675 K PLUR). Required for
    /// sustained uploads — without it pushsync stalls after a few
    /// hundred chunks per peer (bee paymentTolerance). Falls back to
    /// `CHEQUEBOOK_ADDRESS` env. The chequebook's on-chain
    /// `issuer()` view must return the EOA derived from `--swap-key`.
    #[arg(long)]
    chequebook: Option<String>,

    /// 32-byte secp256k1 secret of the chequebook's issuer EOA
    /// (`0x` + 64 hex). Falls back to `SWAP_OWNER_KEY` and then
    /// `WALLET_PRIVATE_KEY`. Same key bee derives at
    /// `m/44'/60'/0'/0/0` of the node mnemonic by default; we keep
    /// the convention that the wallet key is also the chequebook
    /// owner. Required when `--chequebook` is set; ignored otherwise.
    #[arg(long)]
    swap_key: Option<String>,

    /// Skip the startup `factory.deployedContracts(chequebook)`
    /// sanity check. Bee silently rejects cheques drawn on
    /// chequebooks that aren't registered with the official
    /// `SimpleSwapFactory`, so by default we refuse to enable
    /// outbound SWAP settlement against an unregistered
    /// chequebook (the daemon still starts, but with pushsync-swap
    /// disabled, so we don't waste bandwidth signing cheques
    /// peers will silently drop). Pass this flag for devnet /
    /// custom-factory scenarios where the check is the wrong
    /// answer.
    #[arg(long, default_value_t = false)]
    chequebook_allow_unverified: bool,
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
    // Parse via `ArgMatches` (not the `Opt::parse()` shortcut) so the
    // config-file merge can tell which settings came from the command
    // line — those win over the config file (PLAN.md J.5.E2).
    let matches = Opt::command().get_matches();
    let mut opt = match Opt::from_arg_matches(&matches) {
        Ok(o) => o,
        Err(e) => e.exit(),
    };
    let resolved_password = apply_config_file(&mut opt, &matches)?;
    let data_dir = expand_tilde(&opt.data_dir);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("create data dir {}", data_dir.display()))?;

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
    let key_path = opt
        .key_file
        .clone()
        .unwrap_or_else(|| data_dir.join("signing.key"));

    // A bee-managed Web3 v3 keystore at `<data-dir>/keys/swarm.key`
    // (Freedom's injected identity) takes precedence over our own
    // `identity.json` (PLAN.md J.5.E3); only fall back to generate /
    // load our native identity when no keystore is present.
    let swarm_key_path = data_dir.join("keys").join("swarm.key");
    let (signing_secret, overlay_nonce, libp2p_keypair) = if swarm_key_path.exists() {
        load_identity_from_keystore(&swarm_key_path, resolved_password.as_deref())?
    } else {
        load_or_create_identity(&id_path, &key_path)?
    };

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
        .map_or_else(|| data_dir.join("antd.sock"), |p| expand_tilde(&p));

    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
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
        external_addresses: Vec::new(),
        control_socket: control_socket.display().to_string(),
        retrieval: RetrievalInfo::default(),
    };
    let (status_tx, status_rx) = watch::channel(initial_snapshot);

    let peerstore_path = if opt.no_peerstore {
        None
    } else {
        Some(
            opt.peers_file
                .clone()
                .map_or_else(|| data_dir.join("peers.json"), |p| expand_tilde(&p)),
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
            .map_or_else(|| data_dir.join("chunks.sqlite"), |p| expand_tilde(&p));
        // Convert the GB cap to bytes once at startup. Saturating mul
        // means a hypothetical operator typo of `--disk-cache-max-gb
        // 18446744073` (a u64 GB count that overflows on multiplication)
        // produces "as big as we can represent", not a 0-byte cap.
        let max_bytes = opt.disk_cache_max_gb.saturating_mul(1024 * 1024 * 1024);
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
    // which case `gateway_activity` stays empty and `antop`
    // shows zero gateway rows. Empty registry costs one `Arc` and
    // a `Mutex<HashMap>` — well below a rounding error.
    let gateway_activity: Arc<GatewayActivity> = GatewayActivity::new();

    let upload = build_upload_runtime(
        opt.gnosis_rpc_url.clone(),
        opt.postage_contract.clone(),
        opt.postage_batch.clone(),
        opt.postage_owner_key.clone(),
        signing_secret,
        eth,
        data_dir.clone(),
    )
    .await
    .map_err(|e| anyhow!("upload runtime: {e}"))?;

    // The node is "light" (publish-capable) once it can stamp + pushsync
    // uploads — i.e. whenever an upload runtime exists. With a chain RPC
    // configured that's true even before any batch is bought, so Freedom
    // (which gates its whole publish UI on `GET /node.beeMode == "light"`,
    // PLAN.md J.4.1) can buy a batch at runtime and immediately upload.
    // Captured before `upload` is moved into the node future below.
    let light_mode = upload.is_some();

    // `antctl upload` job manager. Built unconditionally — listing
    // and inspecting jobs work even when no postage batch is
    // configured. Actually starting a new job will fail (with the
    // standard "uploads not configured" error) on `PushChunk`
    // until the operator wires postage. Persistent state lives at
    // `<data-dir>/uploads/<job_id>.json`, atomically rewritten on
    // each checkpoint (same idiom as `StampIssuer`).
    // Default batch for `antctl upload` jobs that don't name one: the
    // operator's startup `--postage-batch`, if any. Gateway uploads pass
    // the batch via the `Swarm-Postage-Batch-Id` header instead.
    let default_upload_batch = opt
        .postage_batch
        .clone()
        .or_else(|| std::env::var("STORAGE_STAMP_BATCH_ID").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .and_then(|hex_id| {
            let stripped = strip_0x(&hex_id);
            let mut out = [0u8; 32];
            hex::decode_to_slice(stripped, &mut out).ok().map(|()| out)
        });
    let upload_manager = ant_node::UploadManager::new(
        data_dir.join("uploads"),
        cmd_tx.clone(),
        default_upload_batch,
    )
    .with_context(|| {
        format!(
            "open upload state dir at {}",
            data_dir.join("uploads").display()
        )
    })?;
    let restored_jobs = upload_manager
        .rehydrate_from_disk(!opt.no_resume_uploads)
        .with_context(|| {
            format!(
                "scan upload state dir {}",
                upload_manager.state_dir().display()
            )
        })?;
    if restored_jobs > 0 {
        tracing::info!(
            target: "antd",
            count = restored_jobs,
            auto_resume = !opt.no_resume_uploads,
            "rehydrated upload jobs from disk",
        );
    }

    // SWAP listener always-on. Inbound cheques don't require us to own
    // a chequebook (we're the beneficiary, not the issuer), so the
    // only inputs we need are our EOA and the chain id. The ledger
    // persists at `<data-dir>/swap_credits.json`; cheque monotonicity
    // survives restarts.
    let swap_cfg = ant_p2p::SwapConfig {
        our_beneficiary: eth,
        chain_id: ant_chain::tx::GNOSIS_CHAIN_ID,
        ledger_path: data_dir.join("swap_credits.json"),
        events_tx: None,
    };

    // Outbound SWAP settlement (Phase 7b). Built when the operator has
    // configured both `--chequebook` and `--swap-key` (or their env
    // fallbacks). Without it, sustained pushsync uploads stall after
    // a few hundred chunks per peer.
    let pushsync_swap_cfg = build_pushsync_swap_config(
        opt.chequebook.clone(),
        opt.swap_key.clone(),
        data_dir.clone(),
    )?;

    // Pre-flight: every cheque we issue is rejected by bee's
    // `chequeStore.ReceiveCheque` if `factory.deployedContracts(addr)`
    // returns false, and the rejection is silent (just a closed
    // pushsync stream from the peer's side). Catch it loudly here
    // — turning pushsync-swap off when the check fails is strictly
    // better than wasting bandwidth on cheques nobody honours.
    let pushsync_swap_cfg = match (
        pushsync_swap_cfg,
        opt.gnosis_rpc_url
            .clone()
            .or_else(|| std::env::var("GNOSIS_RPC_URL").ok()),
    ) {
        (Some(cfg), Some(rpc)) => {
            match verify_chequebook_with_factory(&rpc, &cfg.chequebook).await {
                Ok(true) => {
                    tracing::info!(
                        target: "antd",
                        chequebook = %format!("0x{}", hex::encode(cfg.chequebook)),
                        "factory check passed — chequebook is registered with the Swarm chequebook factory",
                    );
                    Some(cfg)
                }
                Ok(false) if opt.chequebook_allow_unverified => {
                    tracing::warn!(
                        target: "antd",
                        chequebook = %format!("0x{}", hex::encode(cfg.chequebook)),
                        "chequebook is NOT registered with the Swarm chequebook factory, but --chequebook-allow-unverified was set; bee will reject cheques drawn on this chequebook",
                    );
                    Some(cfg)
                }
                Ok(false) => {
                    tracing::error!(
                        target: "antd",
                        chequebook = %format!("0x{}", hex::encode(cfg.chequebook)),
                        "chequebook is NOT registered with the Swarm chequebook factory; \
                         bee will silently reject every cheque we emit. \
                         Disabling outbound SWAP settlement. \
                         Run `antctl chequebook deploy` to deploy a new factory-registered chequebook, \
                         or pass --chequebook-allow-unverified to override (devnet only).",
                    );
                    None
                }
                Err(e) => {
                    tracing::warn!(
                        target: "antd",
                        chequebook = %format!("0x{}", hex::encode(cfg.chequebook)),
                        error = %e,
                        "factory.deployedContracts call failed; skipping startup factory check",
                    );
                    Some(cfg)
                }
            }
        }
        (cfg, _) => cfg,
    };

    if pushsync_swap_cfg.is_some() {
        tracing::info!(
            target: "antd",
            "outbound SWAP settlement configured — pushsync will emit cheques",
        );
    } else {
        tracing::warn!(
            target: "antd",
            "outbound SWAP settlement NOT configured — pushsync uploads will stall \
             after ~20K chunks across the peer set; pass --chequebook + --swap-key \
             (or set CHEQUEBOOK_ADDRESS + WALLET_PRIVATE_KEY in the environment) to enable",
        );
    }

    let node_fut = run_node(
        NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
            .with_status(status_tx)
            .with_process_start(process_start)
            .with_external_addrs(external_addrs)
            .with_peerstore_path(peerstore_path)
            .with_commands(cmd_rx)
            .with_target_peers(opt.target_peers)
            .with_per_request_chunk_cache(opt.per_request_chunk_cache)
            .with_chunk_record_dir(chunk_record_dir)
            .with_gateway_activity(Some(gateway_activity.clone()))
            .with_disk_cache(disk_cache)
            .with_upload(upload)
            .with_swap(Some(swap_cfg))
            .with_pushsync_swap(pushsync_swap_cfg)
            .with_upload_manager(Some(upload_manager)),
    );

    // Chain context for the gateway's wallet / chequebook / status /
    // chainstate endpoints (PLAN.md J.5 A2/A3/D1/D2). Built only when a
    // Gnosis RPC endpoint is configured; the node's own Ethereum address
    // is the wallet bee-js reports balances for.
    let chain_ctx = {
        let rpc = opt
            .gnosis_rpc_url
            .clone()
            .or_else(|| std::env::var("GNOSIS_RPC_URL").ok())
            .filter(|s| !s.trim().is_empty());
        let chequebook_addr = opt
            .chequebook
            .clone()
            .or_else(|| std::env::var("CHEQUEBOOK_ADDRESS").ok())
            .and_then(|s| {
                let mut a = [0u8; 20];
                hex::decode_to_slice(strip_0x(s.trim()), &mut a)
                    .ok()
                    .map(|()| a)
            });
        // The node's own signing key funds postage buys / chequebook
        // deposits and is the batch owner — same key that derives `eth`.
        chainreader::build(
            rpc,
            opt.postage_contract.clone(),
            eth,
            chequebook_addr,
            ant_chain::tx::GNOSIS_CHAIN_ID,
            Some(signing_secret),
        )
    };

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
            light_mode,
            tags: Arc::new(TagRegistry::new()),
            cors: Arc::new(ant_gateway::CorsConfig::new(
                opt.cors_allowed_origins.iter(),
            )),
            chain: chain_ctx,
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

    // The `antctl`/`antop` control socket is a Unix domain socket; it
    // only exists on Unix targets. On Windows antd runs the node loop +
    // HTTP gateway and operators drive it through the bee-shaped HTTP
    // API instead. `--no-control-socket` forces that same path on Unix.
    #[cfg(unix)]
    let serve_control = !opt.no_control_socket;
    #[cfg(not(unix))]
    let serve_control = false;

    if !serve_control {
        drop(cmd_tx);
        return tokio::select! {
            res = node_fut => res.map_err(|e| anyhow::anyhow!("{e}")),
            res = gateway_fut => res,
            () = shutdown_signal() => Ok(()),
        };
    }

    #[cfg(unix)]
    {
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
            () = shutdown_signal() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    unreachable!("serve_control is always false on non-unix targets")
}

/// Resolve when the process receives `SIGTERM` or `SIGINT` (Ctrl-C).
///
/// systemd / Freedom stop the daemon with `SIGTERM` and expect a prompt
/// (< 5 s) exit (PLAN.md J.5.E4). Returning from `main` drops every
/// runtime handle and exits cleanly; the durable state
/// (`StampIssuer`, upload jobs, peerstore, SWAP ledger) is already
/// checkpointed incrementally, so a clean return needs no extra flush
/// step. We win over the OS default disposition only to log the cause
/// and to give the `select!` an arm that completes — that's what lets
/// the racing futures (node loop, gateway, control socket) be dropped
/// in order rather than the process being torn down mid-syscall.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "antd", "cannot install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
                return;
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "antd", "cannot install SIGINT handler: {e}");
                std::future::pending::<()>().await;
                return;
            }
        };
        let sig = tokio::select! {
            _ = term.recv() => "SIGTERM",
            _ = int.recv() => "SIGINT",
        };
        tracing::info!(target: "antd", "received {sig}; shutting down");
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(target: "antd", "received Ctrl-C; shutting down");
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
                .with_context(|| format!("create --record-chunks dir {}", dir.display()))?;
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
        .with_context(|| format!("open instance lock {}", lock_path.display()))?;
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
            return Err(anyhow!(e)).with_context(|| format!("flock {}", lock_path.display()));
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
///
/// No-op on non-Unix (Windows has no `RLIMIT_NOFILE`); the descriptor
/// ceiling there is governed differently and isn't a startup concern.
#[cfg(not(unix))]
fn raise_nofile_soft_limit() {}

#[cfg(unix)]
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

/// Load `--config` (if given), merging its values into `opt` for every
/// setting the operator did **not** pass on the command line. Returns
/// the resolved keystore password (from `--password` / `--password-file`
/// or the config's `password` / `password-file`), if any.
///
/// CLI > config file > default — the same precedence bee uses, so a
/// Freedom-written config behaves predictably while an operator can
/// still override one knob on the command line.
fn apply_config_file(opt: &mut Opt, matches: &clap::ArgMatches) -> Result<Option<String>> {
    let from_cli = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);

    // CLI password flags take precedence; fall back to the config file
    // below once it's loaded.
    let cli_password = opt.password.clone();
    let cli_password_file = opt.password_file.clone();

    let cfg = match opt.config.as_ref() {
        Some(path) => Some(config::BeeConfig::load(&expand_tilde(path))?),
        None => None,
    };

    if let Some(cfg) = &cfg {
        if !from_cli("data_dir") {
            if let Some(d) = &cfg.data_dir {
                opt.data_dir = PathBuf::from(d);
            }
        }
        if !from_cli("api_addr") {
            if let Some(addr) = cfg.api_socket_addr()? {
                opt.api_addr = addr;
            }
        }
        if !from_cli("network_id") {
            if let Some(n) = cfg.network_id() {
                opt.network_id = n;
            }
        }
        if !from_cli("gnosis_rpc_url") {
            if let Some(rpc) = &cfg.blockchain_rpc_endpoint {
                opt.gnosis_rpc_url = Some(rpc.clone());
            }
        }
        if !from_cli("cors_allowed_origins") {
            let origins = cfg.cors_origins_vec();
            if !origins.is_empty() {
                opt.cors_allowed_origins = origins;
            }
        }
        if !from_cli("log_level") {
            if let Some(level) = cfg.log_level() {
                opt.log_level = level;
            }
        }
        let ignored = cfg.extra.len();
        if ignored > 0 {
            let keys: Vec<&str> = cfg.extra.keys().map(String::as_str).collect();
            tracing::debug!(
                target: "antd",
                "ignoring {ignored} unmodelled bee config key(s): {}",
                keys.join(", "),
            );
        }
    }

    // Resolve the password: CLI flag wins, then CLI password-file, then
    // the config file's `password` / `password-file`.
    if let Some(p) = cli_password {
        return Ok(Some(p));
    }
    if let Some(file) = cli_password_file {
        let raw = std::fs::read_to_string(&file)
            .with_context(|| format!("read --password-file {}", file.display()))?;
        return Ok(Some(raw.trim_end_matches(['\n', '\r']).to_string()));
    }
    if let Some(cfg) = &cfg {
        return cfg.resolve_password();
    }
    Ok(None)
}

/// Load the node identity from a bee Web3 v3 keystore at
/// `<data-dir>/keys/swarm.key` (PLAN.md J.5.E3). The decrypted 32-byte
/// secp256k1 secret becomes our signing key; the libp2p keypair is
/// derived from it and the overlay nonce is zero (bee's default for a
/// node whose nonce isn't separately configured). Using the
/// Freedom-managed keystore as the source of truth means the daemon's
/// Ethereum identity matches the one Freedom provisioned.
fn load_identity_from_keystore(
    swarm_key_path: &std::path::Path,
    password: Option<&str>,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; 32], Keypair)> {
    let password = password.ok_or_else(|| {
        anyhow!(
            "found a bee keystore at {} but no password is configured; \
             set `password` / `password-file` in the --config file or pass --password",
            swarm_key_path.display(),
        )
    })?;
    let json = std::fs::read_to_string(swarm_key_path)
        .with_context(|| format!("read keystore {}", swarm_key_path.display()))?;
    let signing_secret = keystore::decrypt_v3(&json, password)
        .with_context(|| format!("decrypt keystore {}", swarm_key_path.display()))?;
    let overlay_nonce = [0u8; 32];
    let kp = secp256k1_keypair_from_signing_secret(&signing_secret)?;
    tracing::info!(
        target: "antd",
        keystore = %swarm_key_path.display(),
        "loaded node identity from bee v3 keystore",
    );
    Ok((signing_secret, overlay_nonce, kp))
}

fn load_or_create_identity(
    id_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; 32], Keypair)> {
    if id_path.exists() {
        let raw = std::fs::read_to_string(id_path)
            .with_context(|| format!("read {}", id_path.display()))?;
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
    .with_context(|| format!("write {}", id_path.display()))?;

    // Optional raw key file for tooling.
    let _ = std::fs::write(key_path, hex::encode(signing_secret));

    Ok((signing_secret, overlay_nonce, kp))
}

/// Build a [`UploadRuntime`] — the live registry of postage batches the
/// node can stamp uploads with.
///
/// The node wallet (`signing_secret` / `node_eth`) owns every batch it
/// buys on-chain, so a single stamp key signs for all of them; the
/// registry is keyed by batch id and grows at runtime via
/// [`ant_control::ControlCommand::RegisterBatch`] when Freedom buys a
/// batch through `POST /stamps/{amount}/{depth}`.
///
/// At startup we:
///  1. **Reload persisted batches** by scanning `<data_dir>/postage/*.bin`
///     and reopening each [`StampIssuer`] from its header, so a restart
///     resumes every previously-bought batch's counters (no index is
///     ever re-issued — bee peers reject double-spends).
///  2. **Pre-register `--postage-batch`** (back-compat for operators):
///     fetch its on-chain `batchOwner / depth / bucketDepth / immutable`,
///     check the owner against the stamp key, and open its store.
///
/// Returns `Ok(None)` (ultra-light, read-only) only when the node can
/// neither buy (no RPC / chain writer) nor stamp an existing batch.
/// Whenever an RPC endpoint is configured we return `Some` even with an
/// empty registry, so `GET /node` reports `beeMode:"light"` and Freedom
/// lets the publish flow proceed.
///
/// `cli_key` (`--postage-owner-key` / `STORAGE_STAMP_PRIVATE_KEY`) keeps
/// the legacy single-owner behaviour: when set it becomes the stamp key
/// and the configured batch must be owned by it. When absent the node's
/// own wallet key signs stamps (the bee light-node model).
async fn build_upload_runtime(
    cli_rpc: Option<String>,
    postage_contract: String,
    cli_batch: Option<String>,
    cli_key: Option<String>,
    signing_secret: [u8; SECP256K1_SECRET_LEN],
    node_eth: [u8; 20],
    data_dir: PathBuf,
) -> Result<Option<Arc<UploadRuntime>>> {
    let postage_dir = data_dir.join("postage");

    let rpc_url = cli_rpc
        .or_else(|| std::env::var("GNOSIS_RPC_URL").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let can_stamp = rpc_url.is_some();

    let batch_hex = cli_batch
        .or_else(|| std::env::var("STORAGE_STAMP_BATCH_ID").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let key_hex = cli_key
        .or_else(|| std::env::var("STORAGE_STAMP_PRIVATE_KEY").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Stamp key + owner. Default to the node wallet so batches bought at
    // runtime (owned by the node EOA) validate; a legacy owner key keeps
    // the old single-owner pre-configured-batch behaviour.
    let (stamp_key, batch_owner) = match &key_hex {
        Some(k) => {
            let mut sk = [0u8; SECP256K1_SECRET_LEN];
            hex::decode_to_slice(strip_0x(k), &mut sk).context("decode postage owner key")?;
            let eth = {
                let s =
                    SigningKey::from_bytes((&sk).into()).context("postage owner key invalid")?;
                ethereum_address_from_public_key(s.verifying_key())
            };
            (sk, eth)
        }
        None => (signing_secret, node_eth),
    };

    let mut issuers: std::collections::HashMap<[u8; 32], ant_postage::StampIssuer> =
        std::collections::HashMap::new();

    // 1. Reload batches persisted from a previous run.
    if postage_dir.is_dir() {
        match std::fs::read_dir(&postage_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("bin") {
                        continue;
                    }
                    match ant_postage::StampIssuer::open_existing(path.clone()) {
                        Ok(iss) => {
                            let id = *iss.batch_id();
                            tracing::info!(
                                target: "antd",
                                batch = %format!("0x{}", hex::encode(id)),
                                store = %path.display(),
                                resumed_indices = iss.issued_count(),
                                "reloaded persisted postage batch",
                            );
                            issuers.insert(id, iss);
                        }
                        Err(e) => tracing::warn!(
                            target: "antd",
                            store = %path.display(),
                            "skipping unreadable postage store: {e}",
                        ),
                    }
                }
            }
            Err(e) => tracing::warn!(
                target: "antd",
                dir = %postage_dir.display(),
                "scan postage dir: {e}",
            ),
        }
    }

    // 2. Pre-register an operator-configured batch (--postage-batch).
    if let Some(batch_hex) = &batch_hex {
        let rpc = rpc_url.clone().ok_or_else(|| {
            anyhow!("--postage-batch set but --gnosis-rpc-url / GNOSIS_RPC_URL missing")
        })?;
        let mut batch_id = [0u8; 32];
        hex::decode_to_slice(strip_0x(batch_hex), &mut batch_id)
            .with_context(|| format!("decode batch id {batch_hex}"))?;
        let chain = ant_chain::ChainClient::new(rpc);
        let meta = ant_chain::fetch_postage_batch_meta(&chain, &postage_contract, &batch_id)
            .await
            .with_context(|| format!("fetch postage batch meta from {postage_contract}"))?;
        if batch_owner != meta.batch_owner_eth {
            return Err(anyhow!(
                "postage stamp owner 0x{} does not match on-chain batchOwner 0x{} for batch {}",
                hex::encode(batch_owner),
                hex::encode(meta.batch_owner_eth),
                batch_hex,
            ));
        }
        let store_path = postage_dir.join(format!("{}.bin", hex::encode(batch_id)));
        let issuer = ant_postage::StampIssuer::open_or_new(
            store_path.clone(),
            batch_id,
            meta.depth,
            meta.bucket_depth,
            meta.immutable,
        )
        .map_err(|e| anyhow!("StampIssuer: {e}"))?;
        tracing::info!(
            target: "antd",
            batch = %format!("0x{}", hex::encode(batch_id)),
            owner = %format!("0x{}", hex::encode(meta.batch_owner_eth)),
            depth = meta.depth,
            bucket_depth = meta.bucket_depth,
            immutable = meta.immutable,
            store = %store_path.display(),
            "pre-registered operator postage batch",
        );
        issuers.insert(batch_id, issuer);
    }

    // Nothing to stamp with and no way to buy → ultra-light.
    if !can_stamp && issuers.is_empty() {
        tracing::info!(
            target: "antd",
            "uploads disabled: no blockchain-rpc-endpoint and no postage batch — node is ultra-light (read-only)",
        );
        return Ok(None);
    }

    tracing::info!(
        target: "antd",
        batches = issuers.len(),
        can_buy = can_stamp,
        owner = %format!("0x{}", hex::encode(batch_owner)),
        "upload runtime ready (postage stamping)",
    );

    Ok(Some(Arc::new(UploadRuntime {
        issuers: std::sync::Mutex::new(issuers),
        stamp_key,
        batch_owner,
        postage_dir,
    })))
}

/// Build a [`PushsyncSwapConfig`] from the `--chequebook` /
/// `--swap-key` flags, falling back to env. Returns `Ok(None)` when
/// neither side is configured (the operator is consciously running
/// without outbound settlement). Returns `Err` if exactly one side is
/// configured (mismatch is almost certainly an operator bug — silent
/// fallback would hide it).
fn build_pushsync_swap_config(
    cli_chequebook: Option<String>,
    cli_swap_key: Option<String>,
    data_dir: PathBuf,
) -> Result<Option<ant_p2p::PushsyncSwapConfig>> {
    let cb_hex = cli_chequebook.or_else(|| std::env::var("CHEQUEBOOK_ADDRESS").ok());
    let key_hex = cli_swap_key
        .or_else(|| std::env::var("SWAP_OWNER_KEY").ok())
        .or_else(|| std::env::var("WALLET_PRIVATE_KEY").ok());
    match (cb_hex, key_hex) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow!(
            "--chequebook is set but no --swap-key / SWAP_OWNER_KEY / WALLET_PRIVATE_KEY found; \
             outbound SWAP settlement needs both",
        )),
        (None, Some(_)) => {
            // Swap key alone (e.g. WALLET_PRIVATE_KEY in the dev env)
            // is the common case; logging at info level instead of
            // failing is the friendlier UX. Without a chequebook we
            // can't sign cheques, so disable settlement.
            tracing::info!(
                target: "antd",
                "swap key found but no --chequebook / CHEQUEBOOK_ADDRESS; outbound SWAP settlement disabled",
            );
            Ok(None)
        }
        (Some(cb), Some(key)) => {
            let mut chequebook = [0u8; 20];
            hex::decode_to_slice(strip_0x(&cb), &mut chequebook)
                .with_context(|| format!("decode --chequebook {cb}"))?;
            let mut swap_secret = [0u8; SECP256K1_SECRET_LEN];
            hex::decode_to_slice(strip_0x(&key), &mut swap_secret).context("decode --swap-key")?;
            let swap_eoa = {
                let sk =
                    SigningKey::from_bytes((&swap_secret).into()).context("--swap-key invalid")?;
                ethereum_address_from_public_key(sk.verifying_key())
            };
            tracing::info!(
                target: "antd",
                chequebook = %format!("0x{}", hex::encode(chequebook)),
                issuer_eoa = %format!("0x{}", hex::encode(swap_eoa)),
                "outbound SWAP settlement: chequebook + issuer key configured",
            );
            // Use a placeholder PeerEthMap; `ant-node::run_node`
            // overwrites the field with the live one shared with the
            // swarm loop before the config reaches `run`.
            Ok(Some(ant_p2p::PushsyncSwapConfig::new(
                chequebook,
                swap_secret,
                ant_chain::tx::GNOSIS_CHAIN_ID,
                data_dir.join("pushsync_outbound.json"),
                ant_p2p::PeerEthMap::new(),
            )))
        }
    }
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

/// One `eth_call` to the Swarm chequebook factory's
/// `deployedContracts(address)` view. Returns `Ok(true)` iff the
/// factory has a record of having deployed `chequebook` itself —
/// which is exactly the predicate bee's `chequeStore.ReceiveCheque`
/// uses to decide whether to accept a cheque drawn on it. Any RPC
/// failure surfaces as `Err`, distinguishable from the legitimate
/// "no, that contract is unknown to us" answer.
async fn verify_chequebook_with_factory(rpc_url: &str, chequebook: &[u8; 20]) -> Result<bool> {
    use ant_chain::chequebook::{factory_deployed_contracts_calldata, GNOSIS_CHEQUEBOOK_FACTORY};
    use ant_chain::ChainClient;

    let client = ChainClient::new(rpc_url);
    let calldata = factory_deployed_contracts_calldata(chequebook);
    let v = client
        .eth_call(
            &format!("0x{}", hex::encode(GNOSIS_CHEQUEBOOK_FACTORY)),
            &format!("0x{}", hex::encode(&calldata)),
        )
        .await
        .context("factory.deployedContracts eth_call")?;
    if v.len() < 32 {
        bail!(
            "factory.deployedContracts returned <32 bytes (got {} bytes)",
            v.len()
        );
    }
    let last_word = &v[v.len() - 32..];
    Ok(last_word.iter().any(|&b| b != 0))
}

fn secp256k1_keypair_from_signing_secret(secret: &[u8; SECP256K1_SECRET_LEN]) -> Result<Keypair> {
    let mut sk_copy = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut sk_copy)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(kp))
}

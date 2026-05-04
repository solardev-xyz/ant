//! Minimal node orchestration for M1.0 (p2p dial + handshake loop).

use ant_control::{ControlCommand, GatewayActivity, StatusSnapshot};
use ant_crypto::{OVERLAY_NONCE_LEN, SECP256K1_SECRET_LEN};
use ant_p2p::{run, RunConfig, UploadRuntime};
use libp2p::identity::Keypair;
use libp2p::multiaddr::Multiaddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("p2p: {0}")]
    P2p(#[from] ant_p2p::RunError),
}

pub struct NodeConfig {
    pub signing_secret: [u8; SECP256K1_SECRET_LEN],
    pub overlay_nonce: [u8; OVERLAY_NONCE_LEN],
    pub network_id: u64,
    pub bootnodes: Vec<Multiaddr>,
    pub libp2p_keypair: Keypair,
    /// Public multiaddrs advertised via identify. Required for bee bootnodes
    /// to complete the BZZ handshake without a 10 s peerstore timeout.
    pub external_addrs: Vec<Multiaddr>,
    /// Optional live status sink written to by the node loop.
    pub status: Option<watch::Sender<StatusSnapshot>>,
    /// Where to persist the warm-restart peer snapshot. `None` disables it.
    pub peerstore_path: Option<PathBuf>,
    /// Live control-socket commands consumed by the node loop. `None` means
    /// mutating `antctl` subcommands (`peers reset`, …) will fail with a
    /// "channel not wired up" error.
    pub commands: Option<mpsc::Receiver<ControlCommand>>,
    /// Baseline for `PeerInfo` cold-start timings (`antctl top` milestones).
    pub process_start: Instant,
    /// Scope the in-memory chunk cache to a single request. Default
    /// (`false`) keeps one process-wide cache so consecutive
    /// `antctl get` calls share fetched chunks; setting this makes
    /// every command start with an empty cache, which is the right
    /// shape for reproducing transient retrieval failures or for
    /// timing the cold path.
    pub per_request_chunk_cache: bool,
    /// When set, every chunk fetched by a `GetBytes` / `GetBzz`
    /// request is dumped to `<dir>/<hex_addr>.bin` (raw wire bytes:
    /// 8-byte LE span || payload). Used by `antd --record-chunks`
    /// (debug builds only) to capture an offline fixture of every
    /// chunk a successful `antctl get` touched. The directory must
    /// already exist; `None` (the default) disables recording.
    pub chunk_record_dir: Option<PathBuf>,
    /// Live registry of in-flight gateway HTTP requests, shared with
    /// `ant-gateway`. Wired up by `antd::main` when the HTTP API is
    /// enabled; `None` for headless deployments
    /// (`antd --no-http-api`). Surfaces in `antctl top`'s Retrieval
    /// tab.
    pub gateway_activity: Option<Arc<GatewayActivity>>,
    /// Persistent (SQLite-backed) chunk cache, opened by the daemon
    /// at startup and shared across every retrieval request.
    /// `Some(cache)` makes the lookup order `memory -> disk ->
    /// network`; `None` keeps the legacy `memory -> network`
    /// behaviour. `antd` opens it from `--disk-cache-path` (or the
    /// computed default in `<data-dir>/chunks.sqlite`); embedders can
    /// disable it by passing `None`.
    pub disk_cache: Option<Arc<ant_retrieval::DiskChunkCache>>,
    /// Postage stamping + signing runtime for `POST /chunks`. `Some`
    /// enables uploads; `None` returns "uploads not configured".
    pub upload: Option<Arc<UploadRuntime>>,
}

impl NodeConfig {
    pub fn mainnet_default(
        signing_secret: [u8; SECP256K1_SECRET_LEN],
        overlay_nonce: [u8; OVERLAY_NONCE_LEN],
        bootnodes: Vec<Multiaddr>,
        libp2p_keypair: Keypair,
    ) -> Self {
        Self {
            signing_secret,
            overlay_nonce,
            network_id: 1,
            bootnodes,
            libp2p_keypair,
            external_addrs: Vec::new(),
            status: None,
            peerstore_path: None,
            commands: None,
            process_start: Instant::now(),
            per_request_chunk_cache: false,
            chunk_record_dir: None,
            gateway_activity: None,
            disk_cache: None,
            upload: None,
        }
    }

    pub fn with_status(mut self, status: watch::Sender<StatusSnapshot>) -> Self {
        self.status = Some(status);
        self
    }

    pub fn with_external_addrs(mut self, addrs: Vec<Multiaddr>) -> Self {
        self.external_addrs = addrs;
        self
    }

    pub fn with_peerstore_path(mut self, path: Option<PathBuf>) -> Self {
        self.peerstore_path = path;
        self
    }

    pub fn with_commands(mut self, commands: mpsc::Receiver<ControlCommand>) -> Self {
        self.commands = Some(commands);
        self
    }

    pub fn with_process_start(mut self, t: Instant) -> Self {
        self.process_start = t;
        self
    }

    pub fn with_per_request_chunk_cache(mut self, per_request: bool) -> Self {
        self.per_request_chunk_cache = per_request;
        self
    }

    pub fn with_chunk_record_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.chunk_record_dir = dir;
        self
    }

    pub fn with_gateway_activity(mut self, activity: Option<Arc<GatewayActivity>>) -> Self {
        self.gateway_activity = activity;
        self
    }

    pub fn with_disk_cache(mut self, disk_cache: Option<Arc<ant_retrieval::DiskChunkCache>>) -> Self {
        self.disk_cache = disk_cache;
        self
    }

    pub fn with_upload(mut self, upload: Option<Arc<UploadRuntime>>) -> Self {
        self.upload = upload;
        self
    }
}

/// Run the M1.0 node loop until the process is interrupted.
pub async fn run_node(cfg: NodeConfig) -> Result<(), NodeError> {
    run(RunConfig {
        signing_secret: cfg.signing_secret,
        overlay_nonce: cfg.overlay_nonce,
        network_id: cfg.network_id,
        bootnodes: cfg.bootnodes,
        libp2p_keypair: cfg.libp2p_keypair,
        external_addrs: cfg.external_addrs,
        status: cfg.status,
        target_peers: 0,
        peerstore_path: cfg.peerstore_path,
        commands: cfg.commands,
        process_start: cfg.process_start,
        per_request_chunk_cache: cfg.per_request_chunk_cache,
        chunk_record_dir: cfg.chunk_record_dir,
        gateway_activity: cfg.gateway_activity,
        disk_cache: cfg.disk_cache,
        upload: cfg.upload,
    })
    .await?;
    Ok(())
}

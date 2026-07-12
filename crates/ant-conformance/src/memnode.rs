//! In-process, memory-backed instance of ant's production HTTP gateway.
//!
//! [`spawn_mem_gateway`] builds the **production** router via
//! `ant_gateway::testkit::build_router` and wires its control-command
//! channel to a "`MemNode`": a node loop that stores chunks in an
//! in-memory map instead of pushing them to the swarm. Uploads
//! (`POST /chunks`, `POST /soc/...`, `POST /bytes`, `POST /bzz`) land in
//! the map after the same validation the real node performs; downloads
//! (`GET /chunks`, `/soc`, `/bytes`, `/bzz`, `/feeds`) are served back
//! through `ant-retrieval`'s production joiner / mantaray / feed logic
//! running over that map. The result is a fully self-contained
//! system-under-test for differential conformance testing against bee:
//! upload→download round-trips work end-to-end over real HTTP on an
//! ephemeral local port, with no network, disk, or chain dependency.
//!
//! The dispatch shape mirrors the fixture node loop in
//! `ant-gateway/tests/common/mod.rs` so the gateway's `ControlAck`
//! parsing is exercised exactly as production hits it; the differences
//! are that `MemNode` *stores* pushed chunks (the fixture only hashes
//! them) and that `GetFeed` is wired for real through
//! [`ant_retrieval::resolve_sequence_feed_full`] (the fixture stubs it).

use ant_control::{
    ControlAck, ControlCommand, GatewayActivity, IdentityInfo, PeerConnectionInfo, PeerInfo,
    RetrievalInfo, RoutingInfo, StatusSnapshot, StreamRange, PROTOCOL_VERSION,
};
use ant_gateway::testkit::build_router;
use ant_gateway::{CorsConfig, GatewayHandle, GatewayIdentity, TagRegistry};
use ant_retrieval::{
    join_to_sender_range, join_with_options, list_manifest, lookup_path,
    resolve_sequence_feed_after, ByteRange, ChunkFetcher, Feed, FeedError, FeedType, JoinOptions,
    DEFAULT_MAX_FILE_BYTES,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// SOC wire header: `id(32) ‖ signature(65)`; the rest is the wrapped
/// CAC (`span(8 LE) ‖ payload`).
const SOC_HEADER: usize = 32 + 65;

/// Pin membership rows: `(pinned root reference, member addresses)`.
type PinRows = Vec<(Vec<u8>, Vec<[u8; 32]>)>;

/// Shared in-memory chunk store backing the `MemNode`.
///
/// Keys are 32-byte chunk addresses; values are the chunk **wire
/// bytes** — `span(8 LE) ‖ payload` for a CAC, `id ‖ sig ‖ inner_cac`
/// for a SOC — exactly what bee's `swarm.Chunk.Data()` returns. Clones
/// share the same map, so a handle kept by the caller sees (and can
/// seed) everything the gateway stores.
///
/// Implements [`ChunkFetcher`], so `ant-retrieval`'s joiner, mantaray
/// walker, and feed resolver run over it unchanged. The miss error
/// message contains `"not found"` on purpose: `ant_retrieval::feed`'s
/// `is_chunk_not_found` must classify a missing feed-update index as
/// the end of the feed rather than a transient fetch failure.
#[derive(Clone, Default)]
pub struct MemChunkStore {
    chunks: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    /// Pin registry: `(root reference, member addresses)` in
    /// pin-creation order — the in-memory analogue of the production
    /// disk cache's `pins` / `pin_members` tables. `MemNode` never
    /// evicts, so no per-chunk refcount is needed; membership is kept
    /// so `/pins/check` can report bee's integrity rows.
    pins: Arc<Mutex<PinRows>>,
}

impl MemChunkStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 32], Vec<u8>>> {
        self.chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Insert (or replace) the wire bytes stored at `addr`.
    pub fn insert(&self, addr: [u8; 32], wire: Vec<u8>) {
        self.lock().insert(addr, wire);
    }

    /// Wire bytes stored at `addr`, if any.
    #[must_use]
    pub fn get(&self, addr: &[u8; 32]) -> Option<Vec<u8>> {
        self.lock().get(addr).cloned()
    }

    /// Number of chunks currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// True when no chunk has been stored yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock_pins(&self) -> std::sync::MutexGuard<'_, PinRows> {
        self.pins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pinned root references in pin-creation order.
    #[must_use]
    pub fn pinned(&self) -> Vec<Vec<u8>> {
        self.lock_pins().iter().map(|(r, _)| r.clone()).collect()
    }
}

#[async_trait]
impl ChunkFetcher for MemChunkStore {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        self.get(&addr)
            .ok_or_else(|| -> Box<dyn StdError + Send + Sync> {
                format!("chunk {} not found", hex::encode(addr)).into()
            })
    }

    /// Store RS-recovered / replica-recovered chunks like the production
    /// fetcher stores them in its caches, so conformance tests can also
    /// assert the recovered bytes landed intact.
    async fn put_recovered(&self, addr: [u8; 32], wire: &[u8]) {
        self.insert(addr, wire.to_vec());
    }
}

/// In-memory postage runtime backing the `MemNode`'s `Envelope` /
/// `StewardshipPut` handlers: a fixed node signing key plus one real
/// [`ant_postage::StampIssuer`] per batch id, created lazily on first
/// use. Mirrors beemock's `mockpost.WithAcceptAll()`, which hands out a
/// usable issuer for *any* batch id — so the differential can stamp
/// against whatever id the oracle advertises — while the stamps
/// themselves are genuine: real bucket counters, real keccak digest,
/// real secp256k1 signatures that `verify_stamp_owner` accepts.
struct MemPostage {
    secret: [u8; 32],
    owner: [u8; 20],
    issuers: Mutex<HashMap<[u8; 32], ant_postage::StampIssuer>>,
}

/// Batch geometry for lazily-created issuers, matching the fallback
/// issuer bee's `mockpost.WithAcceptAll` mints for **any** batch id:
/// `postage.NewStampIssuer(..., batchDepth: 24, bucketDepth: 6, ...)`
/// (`pkg/postage/mock/service.go`). Beemock stamps and reports
/// `GET /stamps/{id}/buckets` with that geometry, so `MemNode` mirrors
/// it — 64 buckets of `2^18` slots is also ample headroom for a
/// conformance run.
const MEM_BATCH_DEPTH: u8 = 24;
const MEM_BUCKET_DEPTH: u8 = 6;

impl MemPostage {
    fn new() -> Self {
        // Any fixed scalar below the secp256k1 order works; a constant
        // keeps the mem node's issuer address stable across runs.
        let secret = [0xa5u8; 32];
        let sk = k256::ecdsa::SigningKey::from_bytes(&secret.into())
            .expect("fixed scalar is a valid secp256k1 secret");
        let owner = ant_crypto::ethereum_address_from_public_key(sk.verifying_key());
        Self {
            secret,
            owner,
            issuers: Mutex::new(HashMap::new()),
        }
    }

    /// Bucket snapshot for `GET /stamps/{id}/buckets`, mirroring
    /// beemock's `mockpost.WithAcceptAll`: **any** batch id resolves
    /// to an issuer (lazily created with the mock geometry), so the
    /// endpoint never 404s on the mem rig. A batch that has stamped
    /// nothing reports all-zero collisions, exactly like the fresh
    /// issuer bee's mock mints per call.
    fn buckets(&self, batch_id: [u8; 32]) -> ant_control::PostageBucketsView {
        let mut issuers = self
            .issuers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let issuer = match issuers.entry(batch_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(
                ant_postage::StampIssuer::new(batch_id, MEM_BATCH_DEPTH, MEM_BUCKET_DEPTH, false)
                    .expect("mem issuer geometry is valid"),
            ),
        };
        ant_control::PostageBucketsView {
            depth: issuer.batch_depth(),
            bucket_depth: issuer.bucket_depth(),
            bucket_upper_bound: issuer.bucket_upper_bound(),
            collisions: issuer.bucket_counts().to_vec(),
        }
    }

    /// Issue (or reuse — the issuer's per-chunk stamp cache makes this
    /// idempotent) a stamp for `address` under `batch_id`.
    fn stamp(
        &self,
        batch_id: [u8; 32],
        address: &[u8; 32],
    ) -> Result<[u8; ant_postage::STAMP_SIZE], ant_postage::PostageError> {
        let mut issuers = self
            .issuers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let issuer = match issuers.entry(batch_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(
                ant_postage::StampIssuer::new(batch_id, MEM_BATCH_DEPTH, MEM_BUCKET_DEPTH, false)?,
            ),
        };
        ant_postage::sign_stamp_bytes(&self.secret, issuer, address)
    }
}

/// Validate a pre-signed 113-byte stamp the way the production node
/// does: exact size, then signature recovery over the stamp digest for
/// this chunk address.
fn presigned_stamp_valid(pre: &[u8], addr: &[u8; 32]) -> Result<(), String> {
    let stamp: [u8; ant_postage::STAMP_SIZE] = pre.try_into().map_err(|_| {
        format!(
            "presigned stamp must be {} bytes, got {}",
            ant_postage::STAMP_SIZE,
            pre.len()
        )
    })?;
    let batch: &[u8; 32] = stamp[0..32].try_into().expect("slice length");
    let index: &[u8; 8] = stamp[32..40].try_into().expect("slice length");
    let ts: &[u8; 8] = stamp[40..48].try_into().expect("slice length");
    let sig: &[u8; 65] = stamp[48..113].try_into().expect("slice length");
    let digest = ant_postage::postage_sign_digest(addr, batch, index, ts);
    ant_crypto::recover_public_key(sig, digest.as_ref())
        .map(|_| ())
        .map_err(|e| format!("presigned stamp signature invalid: {e}"))
}

/// Configuration for [`spawn_mem_gateway`]. `Default` gives an
/// upload-capable (`light`-mode) gateway with bee's default (deny) CORS
/// policy — the configuration conformance runs want.
#[derive(Debug, Clone)]
pub struct MemGatewayConfig {
    /// Reported via `GET /node.beeMode`: `true` ⇒ `light` (the node
    /// claims upload capability), `false` ⇒ `ultra-light` (read-only).
    /// `MemNode` accepts uploads either way; this only drives the status
    /// surface.
    pub light_mode: bool,
}

impl Default for MemGatewayConfig {
    fn default() -> Self {
        Self { light_mode: true }
    }
}

/// A running in-process gateway: production router + `MemNode` loop,
/// served on an ephemeral local TCP port.
///
/// Obtain one via [`spawn_mem_gateway`]; stop it with
/// [`MemGateway::shutdown`] (dropping it also works — the background
/// tasks are detached and die with the runtime, they just don't get
/// the graceful-shutdown pass).
pub struct MemGateway {
    addr: SocketAddr,
    store: MemChunkStore,
    server: JoinHandle<()>,
    node: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
    /// Keeps the gateway's `watch::Receiver<StatusSnapshot>` from
    /// observing `Closed` mid-test.
    _status_tx: watch::Sender<StatusSnapshot>,
}

impl MemGateway {
    /// The bound listen address (always `127.0.0.1:<ephemeral>`).
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>` — base URL for client requests, no
    /// trailing slash.
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Handle on the backing chunk store, for seeding chunks directly
    /// or asserting on what an upload persisted.
    #[must_use]
    pub fn store(&self) -> &MemChunkStore {
        &self.store
    }

    /// Gracefully stop the HTTP server and wait for both background
    /// tasks (server + `MemNode` loop) to finish.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.server.await;
        // The node loop exits once every command sender is gone; the
        // last one is inside the router the server task just dropped.
        let _ = self.node.await;
    }
}

/// Spawn the production gateway router over a fresh in-memory `MemNode`
/// and serve it on an ephemeral `127.0.0.1` port.
///
/// The returned [`MemGateway`] carries the bound address, a handle on
/// the chunk store, and the shutdown/join handles.
pub async fn spawn_mem_gateway(cfg: MemGatewayConfig) -> MemGateway {
    let store = MemChunkStore::default();

    let (status_tx, status_rx) = watch::channel(mem_snapshot());
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(16);

    // `MemNode` loop: dispatch every command on its own task, like the
    // production node loop (and the gateway test fixture) do, so a
    // slow streaming download can't head-of-line-block an upload.
    let node_store = store.clone();
    let postage = Arc::new(MemPostage::new());
    let node = tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let s = node_store.clone();
            let p = postage.clone();
            tokio::spawn(async move {
                handle_command(&s, &p, cmd).await;
            });
        }
    });

    let handle = GatewayHandle {
        agent: Arc::new("antd/mem".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(mem_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(CorsConfig::default()),
        chain_state: {
            // MemNode has no chain init phase: resolve the state
            // immediately so no endpoint ever answers the 503
            // "chain init in progress" placeholder.
            let cell = Arc::new(std::sync::OnceLock::new());
            let _ = cell.set(ant_gateway::GatewayChainState {
                light_mode: cfg.light_mode,
                chain: None,
            });
            cell
        },
        act_secret: Arc::new(MEM_NODE_SECRET),
    };
    let router = build_router(handle);

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind ephemeral 127.0.0.1 port");
    let addr = listener.local_addr().expect("read bound address");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve in-memory gateway");
    });

    MemGateway {
        addr,
        store,
        server,
        node,
        shutdown: Some(shutdown_tx),
        _status_tx: status_tx,
    }
}

/// Fixed node signing secret: the `MemNode`'s ACT **publisher** identity
/// (bee uses the swarm key for `accesscontrol.NewDefaultSession`).
/// Public so the differential harness and the bee-js runner can compute
/// the matching `swarm-act-publisher` value.
pub const MEM_NODE_SECRET: [u8; 32] = [0xae; 32];

/// Compressed public key (hex) of [`MEM_NODE_SECRET`] — the value
/// `GET /addresses.publicKey` must report so ACT clients (bee-js) can
/// discover the publisher key.
#[must_use]
pub fn mem_node_public_key_hex() -> String {
    let pk = ant_crypto::act::public_key_of(&MEM_NODE_SECRET).expect("fixed scalar is valid");
    hex::encode(ant_crypto::act::compress_public_key(&pk))
}

/// Well-formed identity, mirroring the gateway test fixture's
/// `test_identity` — except the public key, which is the real ACT
/// publisher key so `GET /addresses` and the ACT surface agree.
fn mem_identity() -> GatewayIdentity {
    GatewayIdentity {
        overlay_hex: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899".to_string(),
        ethereum_hex: "0x0102030405060708090a0b0c0d0e0f1011121314".to_string(),
        public_key_hex: mem_node_public_key_hex(),
        peer_id: "12D3KooWMemGateway000000000000000000000000000".to_string(),
    }
}

/// Status snapshot with one connected peer and a populated routing
/// table, so `/readiness` reports ready and the status endpoints have
/// something to render. Mirrors the gateway test fixture's
/// `snapshot_with_one_peer`.
fn mem_snapshot() -> StatusSnapshot {
    StatusSnapshot {
        chain_ready: true,
        agent: "antd/mem".to_string(),
        protocol_version: PROTOCOL_VERSION,
        network_id: 1,
        pid: 0,
        started_at_unix: 1_700_000_000,
        identity: IdentityInfo {
            eth_address: "0x0102030405060708090a0b0c0d0e0f1011121314".to_string(),
            overlay: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                .to_string(),
            peer_id: "12D3KooWMemGateway000000000000000000000000000".to_string(),
        },
        peers: PeerInfo {
            connected: 1,
            node_limit: 16,
            connected_peers: vec![PeerConnectionInfo {
                peer_id: "12D3KooWPeerOne00000000000000000000000000".to_string(),
                direction: "Outbound".to_string(),
                address: "/ip4/127.0.0.1/tcp/1634".to_string(),
                connected_at_unix: 1_700_000_001,
                agent_version: "bee/2.7.0".to_string(),
                bzz_overlay: Some(
                    "0xdeadbeef00000000000000000000000000000000000000000000000000000000"
                        .to_string(),
                ),
                full_node: Some(true),
                last_bzz_at_unix: Some(1_700_000_002),
            }],
            peer_pipeline: Vec::new(),
            last_handshake: None,
            time_to_first_peer_s: Some(0.5),
            time_to_node_limit_s: None,
            routing: RoutingInfo {
                base_overlay: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                    .to_string(),
                size: 1,
                bins: {
                    let mut bins = vec![0u32; 32];
                    bins[5] = 1;
                    bins
                },
                ..Default::default()
            },
        },
        listeners: vec!["/ip4/127.0.0.1/tcp/1634".to_string()],
        external_addresses: Vec::new(),
        control_socket: "/tmp/antd.sock".to_string(),
        retrieval: RetrievalInfo::default(),
    }
}

/// `MemNode`'s single-command dispatcher. The ack shape of every arm
/// matches what the production node loop in `ant-p2p` returns (or,
/// for the commands the gateway never exercises against content, what
/// the `ant-gateway` test fixture returns), so no gateway handler ever
/// hangs on an unanswered channel.
async fn handle_command(store: &MemChunkStore, postage: &MemPostage, cmd: ControlCommand) {
    match cmd {
        // ---- writes -------------------------------------------------
        ControlCommand::PushChunk {
            wire, stamp, ack, ..
        } => {
            // Validate like production: recompute the address by
            // BMT-hashing `span ‖ payload`. Then *store* the wire bytes
            // so the round-trip read works (the fixture only hashes).
            // The batch id is accepted unchecked — `MemNode` models an
            // issuer that stamps anything. A pre-signed stamp (bee's
            // `swarm-postage-stamp` header) is validated exactly like
            // the production node does: size + signature recovery over
            // the stamp digest for this chunk address.
            let reply = if wire.len() < 8 || wire.len() > 8 + 4096 {
                ControlAck::Error {
                    message: format!("chunk wire size out of range: {} bytes", wire.len()),
                }
            } else {
                let mut span = [0u8; 8];
                span.copy_from_slice(&wire[..8]);
                match ant_crypto::bmt_hash_with_span(&span, &wire[8..]) {
                    Some(addr) => {
                        let stamp_err = stamp
                            .as_deref()
                            .and_then(|pre| presigned_stamp_valid(pre, &addr).err());
                        if let Some(message) = stamp_err {
                            ControlAck::Error { message }
                        } else {
                            store.insert(addr, wire);
                            ControlAck::ChunkUploaded {
                                reference: format!("0x{}", hex::encode(addr)),
                            }
                        }
                    }
                    None => ControlAck::Error {
                        message: "failed to BMT-hash chunk".into(),
                    },
                }
            };
            let _ = ack.send(reply);
        }
        ControlCommand::PushSoc {
            address,
            wire,
            stamp,
            ack,
            ..
        } => {
            // Re-validate the wire even though the gateway already ran
            // `soc_valid` (mirroring `ant-p2p`'s defensive re-check),
            // then store the SOC at its owner-bound address. Also store
            // the wrapped CAC under its own BMT address, exactly like
            // the production `PushSoc` handler caches it: a v2 feed
            // update's wrapped CAC *is* the content root the subsequent
            // `GetFeed` → `StreamBytes` read dereferences, and it is
            // never uploaded as a standalone chunk.
            let reply = if wire.len() < SOC_HEADER + 8 || wire.len() > SOC_HEADER + 8 + 4096 {
                ControlAck::Error {
                    message: format!("soc wire size out of range: {} bytes", wire.len()),
                }
            } else if !ant_crypto::soc_valid(&address, &wire) {
                ControlAck::Error {
                    message: "soc validation failed".into(),
                }
            } else if let Some(message) = stamp
                .as_deref()
                .and_then(|pre| presigned_stamp_valid(pre, &address).err())
            {
                ControlAck::Error { message }
            } else {
                let inner = wire[SOC_HEADER..].to_vec();
                let mut inner_span = [0u8; 8];
                inner_span.copy_from_slice(&inner[..8]);
                if let Some(inner_addr) = ant_crypto::bmt_hash_with_span(&inner_span, &inner[8..]) {
                    store.insert(inner_addr, inner);
                }
                store.insert(address, wire);
                ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(address)),
                }
            };
            let _ = ack.send(reply);
        }
        // ---- single-chunk reads --------------------------------------
        ControlCommand::GetChunkRaw { reference, ack } => {
            let reply = match store.fetch(reference).await {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error {
                    message: format!("fetch chunk {}: {e}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        // Pullsync / lurker are live-network P2P surfaces the in-memory
        // conformance node doesn't model; ack empty / drop the stream.
        ControlCommand::PullsyncProbe { ack, .. } => {
            let _ = ack.send(ControlAck::PullsyncProbe(ant_control::PullsyncProbeView::default()));
        }
        ControlCommand::LurkerSubscribe { .. } => {}
        ControlCommand::GetChunk { reference, ack } => {
            let reply = match store.fetch(reference).await {
                Ok(data) => {
                    // Strip the 8-byte span: `GetChunk` acks payload only.
                    let payload = if data.len() >= 8 {
                        data[8..].to_vec()
                    } else {
                        data
                    };
                    ControlAck::Bytes { data: payload }
                }
                Err(e) => ControlAck::Error {
                    message: format!("fetch chunk {}: {e}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        // ---- joined reads (production ant-retrieval logic) -----------
        ControlCommand::GetBytes {
            reference,
            bypass_cache: _,
            progress: _,
            max_bytes,
            ack,
        } => {
            let result = async {
                // Root fetch with dispersed-replica fallback, like the
                // production node loop: a redundant upload's root may be
                // recoverable from its SOC replicas even when the CAC
                // itself is gone.
                let root = ant_retrieval::fetch_root_with_replicas(store, reference)
                    .await
                    .map_err(|e| e.to_string())?;
                let opts = JoinOptions {
                    allow_degraded_redundancy: true,
                };
                let cap = byte_cap(max_bytes);
                join_with_options(store, &root, cap, opts)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            let reply = match result {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error { message: e },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::GetBytesEncrypted {
            reference,
            key,
            bypass_cache: _,
            max_bytes,
            ack,
        } => {
            let mut root_ref = [0u8; ant_retrieval::ENCRYPTED_REF_SIZE];
            root_ref[..32].copy_from_slice(&reference);
            root_ref[32..].copy_from_slice(&key);
            let cap = byte_cap(max_bytes);
            let reply = match ant_retrieval::join_encrypted(store, root_ref, cap).await {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error {
                    message: e.to_string(),
                },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::StreamBytes {
            reference,
            bypass_cache: _,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            stream_via_store(
                store, &reference, /* path */ None, /* allow_degraded */ true, max_bytes,
                range, head_only, ack,
            )
            .await;
        }
        ControlCommand::StreamBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache: _,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            stream_via_store(
                store,
                &reference,
                Some(path),
                allow_degraded_redundancy,
                max_bytes,
                range,
                head_only,
                ack,
            )
            .await;
        }
        ControlCommand::GetBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache: _,
            progress: _,
            max_bytes,
            ack,
        } => {
            let result = async {
                let lookup = lookup_path(store, &reference, &path)
                    .await
                    .map_err(|e| e.to_string())?;
                let cap = byte_cap(max_bytes);
                // Encrypted manifest entry: buffered decrypt-join.
                if let Ok(enc_ref) =
                    <[u8; ant_retrieval::ENCRYPTED_REF_SIZE]>::try_from(lookup.data_ref.as_slice())
                {
                    let body = ant_retrieval::join_encrypted(store, enc_ref, cap)
                        .await
                        .map_err(|e| e.to_string())?;
                    return Ok::<_, String>((body, lookup));
                }
                let data_ref: [u8; 32] = lookup
                    .data_ref
                    .as_slice()
                    .try_into()
                    .map_err(|_| "manifest entry has invalid width".to_string())?;
                let root = ant_retrieval::fetch_root_with_replicas(store, data_ref)
                    .await
                    .map_err(|e| e.to_string())?;
                let opts = JoinOptions {
                    allow_degraded_redundancy,
                };
                let body = join_with_options(store, &root, cap, opts)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<_, String>((body, lookup))
            }
            .await;
            let reply = match result {
                Ok((data, lookup)) => ControlAck::BzzBytes {
                    data,
                    content_type: lookup.content_type,
                    filename: lookup.metadata.get("Filename").cloned(),
                },
                Err(e) => ControlAck::Error { message: e },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::ListBzz {
            reference,
            bypass_cache: _,
            ack,
        } => {
            let reply = match list_manifest(store, &reference).await {
                Ok(entries) => ControlAck::Manifest {
                    entries: entries
                        .into_iter()
                        .map(|entry| ant_control::ManifestEntryInfo {
                            path: entry.path,
                            reference: entry.reference.map(hex::encode),
                            metadata: entry.metadata,
                            size: entry.size,
                        })
                        .collect(),
                },
                Err(e) => ControlAck::Error {
                    message: e.to_string(),
                },
            };
            let _ = ack.send(reply).await;
        }
        // ---- feeds: wired for real, unlike the gateway fixture -------
        ControlCommand::GetFeed {
            owner,
            topic,
            after,
            ack,
        } => {
            // Same resolution path production runs in `ant-p2p`'s
            // `GetFeed` handler, with the in-memory store standing in
            // for the `RoutingFetcher`; the ack shape is identical.
            let feed = Feed {
                owner,
                topic,
                kind: FeedType::Sequence,
            };
            let reply = match resolve_sequence_feed_after(store, &feed, after).await {
                Ok(resolution) => ControlAck::FeedResolved {
                    reference: resolution.reference,
                    index: resolution.index,
                    signature: resolution.signature,
                    v2: resolution.v2,
                    decrypt_key: resolution.decrypt_key,
                },
                Err(FeedError::NoUpdates { .. }) => ControlAck::FeedNotFound,
                Err(e) => ControlAck::Error {
                    message: format!("feed lookup: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        // ---- envelope + stewardship: wired for real ------------------
        ControlCommand::Envelope {
            address,
            batch_id,
            ack,
        } => {
            // Genuine signed stamp from the in-memory issuer — same
            // digest, bucket accounting, and signature the production
            // node produces; only the key differs from beemock's, so
            // the differential masks the value and compares shape.
            let reply = match postage.stamp(batch_id, &address) {
                Ok(stamp) => ControlAck::Envelope {
                    issuer: postage.owner,
                    stamp,
                },
                Err(ant_postage::PostageError::BucketFull) => ControlAck::Error {
                    message: "batch is overissued".into(),
                },
                Err(e) => ControlAck::Error {
                    message: format!("stamp issue failed: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::StewardshipGet { reference, ack } => {
            // Production traversal (span trees + mantaray manifests)
            // over the in-memory store; a missing chunk anywhere in the
            // tree — root included — reports `false`, like bee's
            // steward mapping storage.ErrNotFound to not-retrievable.
            let retrievable = match ant_retrieval::traverse_chunk_addresses(
                store,
                reference,
                DEFAULT_MAX_FILE_BYTES,
            )
            .await
            {
                Ok(addrs) => addrs.iter().all(|addr| store.get(addr).is_some()),
                Err(_) => false,
            };
            let _ = ack.send(ControlAck::Retrievable { retrievable });
        }
        ControlCommand::StewardshipPut { reference, ack, .. } => {
            // Re-upload: traverse, then re-store every chunk (a push to
            // the in-memory "network" is a store). Missing content
            // fails like bee's `Reupload` traversal would.
            let reply = match ant_retrieval::traverse_chunk_addresses(
                store,
                reference,
                DEFAULT_MAX_FILE_BYTES,
            )
            .await
            {
                Ok(addrs) => {
                    let mut missing = None;
                    for addr in &addrs {
                        if let Some(wire) = store.get(addr) {
                            store.insert(*addr, wire);
                        } else {
                            missing = Some(*addr);
                            break;
                        }
                    }
                    match missing {
                        None => ControlAck::Ok {
                            message: format!("re-uploaded {} chunks", addrs.len()),
                        },
                        Some(addr) => ControlAck::Error {
                            message: format!("fetch {}: chunk not found", hex::encode(addr)),
                        },
                    }
                }
                Err(e) => ControlAck::Error {
                    message: format!("re-upload traversal failed: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        // ---- node-management commands: fixture-shaped acks ------------
        ControlCommand::ResetPeerstore { ack } | ControlCommand::Resume { ack } => {
            let _ = ack.send(ControlAck::Ok {
                message: "ok".to_string(),
            });
        }
        ControlCommand::RegisterBatch { ack, .. } => {
            let _ = ack.send(ControlAck::Ok {
                message: "registered (mem node)".into(),
            });
        }
        ControlCommand::EnablePushsyncSwap { ack, .. } => {
            let _ = ack.send(ControlAck::Ok {
                message: "settlement enable ignored (mem node)".into(),
            });
        }
        // Upload* drive `ant-node`'s UploadManager (a filesystem-backed
        // job pipeline); the gateway never emits them, so reject any
        // that leak in rather than model the pipeline.
        ControlCommand::UploadStart { ack, .. }
        | ControlCommand::UploadList { ack }
        | ControlCommand::UploadStatus { ack, .. }
        | ControlCommand::UploadPause { ack, .. }
        | ControlCommand::UploadResume { ack, .. }
        | ControlCommand::UploadRepush { ack, .. }
        | ControlCommand::UploadSuspendAll { ack }
        | ControlCommand::UploadWakeAll { ack }
        | ControlCommand::UploadCancel { ack, .. } => {
            let _ = ack.send(ControlAck::Error {
                message: "Upload* not supported by the in-memory node".into(),
            });
        }
        ControlCommand::UploadFollow { ack, .. } => {
            let _ = ack.try_send(ControlAck::Error {
                message: "Upload* not supported by the in-memory node".into(),
            });
        }
        // `MemNode` has no stamp issuer; report the "uploads disabled"
        // default snapshot / empty batch list like a daemon started
        // without `--postage-batch` (matches the gateway fixture).
        ControlCommand::PostageStatus { ack } => {
            let _ = ack.send(ControlAck::PostageStatus(
                ant_control::PostageStatusView::default(),
            ));
        }
        ControlCommand::PostageList { ack } => {
            let _ = ack.send(ControlAck::PostageList(Vec::new()));
        }
        // `antctl pin` escape hatch: validate + store, same as a disk
        // cache write would.
        ControlCommand::PutChunkLocal { wire, ack } => {
            let reply = if wire.len() < 8 || wire.len() > 8 + 4096 {
                ControlAck::Error {
                    message: format!("chunk wire size out of range: {} bytes", wire.len()),
                }
            } else {
                let mut span = [0u8; 8];
                span.copy_from_slice(&wire[..8]);
                match ant_crypto::bmt_hash_with_span(&span, &wire[8..]) {
                    Some(addr) => {
                        store.insert(addr, wire);
                        ControlAck::Ok {
                            message: format!("stored 0x{}", hex::encode(addr)),
                        }
                    }
                    None => ControlAck::Error {
                        message: "failed to BMT-hash chunk".into(),
                    },
                }
            };
            let _ = ack.send(reply);
        }
        // Read-back propagation checks: the store is the single source,
        // so report reachability in the production JSON shapes (mirrors
        // the gateway fixture's stand-ins).
        ControlCommand::VerifyPropagation {
            reference,
            samples: _,
            probes: _,
            progress: _,
            cancel: _,
            ack,
        } => {
            let retrievable = store.get(&reference).is_some();
            let checked = u64::from(retrievable);
            let body = serde_json::json!({
                "reference": format!("0x{}", hex::encode(reference)),
                "retrievable": retrievable,
                "total_chunks": 1,
                "leaf_chunks": 1,
                "intermediate_chunks": 0,
                "checked_chunks": checked,
                "retrievable_chunks": checked,
                "sampled_leaves": checked,
                "sources": checked,
            });
            let _ = ack.send(ControlAck::Ok {
                message: body.to_string(),
            });
        }
        ControlCommand::AccountingSnapshot { ack } => {
            // MemNode has no peers, so the settlement/balance mirror is
            // empty — exactly what beemock's empty accounting/swap
            // mocks report, letting the differential pin the empty
            // collection shapes of `/balances`, `/settlements`,
            // `/timesettlements`, and `/chequebook/cheque`.
            let _ = ack.send(ControlAck::Accounting(
                ant_control::AccountingSnapshotView::default(),
            ));
        }
        // ---- pins: wired for real against the in-memory store --------
        // `MemNode`'s pin model mirrors the production disk cache: pin
        // = traverse (plain or encrypted) + record membership; the
        // store itself never evicts, so the "exempt from eviction"
        // invariant is trivially true here.
        ControlCommand::PinAdd { reference, ack } => {
            if store.lock_pins().iter().any(|(r, _)| *r == reference) {
                let _ = ack.send(ControlAck::PinAdded {
                    already_pinned: true,
                });
                return;
            }
            let addrs = match reference.len() {
                32 => {
                    let root: [u8; 32] = reference.as_slice().try_into().expect("len checked");
                    ant_retrieval::traverse_chunk_addresses(store, root, DEFAULT_MAX_FILE_BYTES)
                        .await
                }
                64 => {
                    let root: [u8; 64] = reference.as_slice().try_into().expect("len checked");
                    ant_retrieval::traverse_encrypted_chunk_addresses(
                        store,
                        root,
                        DEFAULT_MAX_FILE_BYTES,
                    )
                    .await
                }
                n => {
                    let _ = ack.send(ControlAck::Error {
                        message: format!("invalid pin reference length: {n} bytes"),
                    });
                    return;
                }
            };
            let reply = match addrs {
                Ok(addrs) => {
                    // Every member must be present locally (leaves are
                    // listed, not fetched, by the traversal).
                    match addrs.iter().find(|a| store.get(a).is_none()) {
                        None => {
                            store.lock_pins().push((reference, addrs));
                            ControlAck::PinAdded {
                                already_pinned: false,
                            }
                        }
                        Some(addr) => ControlAck::Error {
                            message: format!("fetch {}: chunk not found", hex::encode(addr)),
                        },
                    }
                }
                Err(e) => ControlAck::Error {
                    message: format!("pin traversal failed: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::PinRemove { reference, ack } => {
            let mut pins = store.lock_pins();
            let before = pins.len();
            pins.retain(|(r, _)| *r != reference);
            let _ = ack.send(ControlAck::PinRemoved {
                was_pinned: pins.len() < before,
            });
        }
        ControlCommand::PinHas { reference, ack } => {
            let pinned = store.lock_pins().iter().any(|(r, _)| *r == reference);
            let _ = ack.send(ControlAck::PinPresent { pinned });
        }
        ControlCommand::PinList { ack } => {
            let _ = ack.send(ControlAck::PinList {
                references: store.pinned(),
            });
        }
        ControlCommand::PinCheck { reference, ack } => {
            let entries: PinRows = store
                .lock_pins()
                .iter()
                .filter(|(r, _)| reference.as_ref().is_none_or(|want| r == want))
                .cloned()
                .collect();
            let mut stats = Vec::new();
            for (r, members) in entries {
                let mut stat = ant_control::PinCheckStat {
                    reference: r,
                    total: members.len() as u64,
                    ..Default::default()
                };
                for addr in members {
                    match store.get(&addr) {
                        Some(wire) => {
                            if !(ant_crypto::cac_valid(&addr, &wire)
                                || ant_crypto::soc_valid(&addr, &wire))
                            {
                                stat.invalid += 1;
                            }
                        }
                        None => stat.missing += 1,
                    }
                }
                stats.push(stat);
            }
            let _ = ack.send(ControlAck::PinCheck { stats });
        }
        // Bucket counters, mirroring beemock's accept-all mock postage
        // service (see [`MemPostage::buckets`]).
        ControlCommand::PostageBuckets { batch_id, ack } => {
            let _ = ack.send(ControlAck::PostageBuckets(postage.buckets(batch_id)));
        }
        ControlCommand::VerifyChunksPresent {
            addresses,
            probes: _,
            include_shallow: _,
            ack,
        } => {
            let missing: Vec<String> = addresses
                .iter()
                .filter(|addr| store.get(addr).is_none())
                .map(|addr| format!("0x{}", hex::encode(addr)))
                .collect();
            let body = serde_json::json!({
                "checked": addresses.len(),
                "missing": missing,
            });
            let _ = ack.send(ControlAck::Ok {
                message: body.to_string(),
            });
        }
    }
}

/// `max_bytes` override → joiner byte cap, defaulting like production.
fn byte_cap(max_bytes: Option<u64>) -> usize {
    max_bytes.map_or(DEFAULT_MAX_FILE_BYTES, |n| {
        usize::try_from(n).unwrap_or(usize::MAX)
    })
}

/// Streaming dispatcher shared by `StreamBytes` and `StreamBzz`,
/// mirroring the production node loop (and the gateway test fixture):
/// `path = None` treats `reference` as a raw `/bytes` data root;
/// `path = Some(_)` walks the manifest first. Emits the
/// `BytesStreamStart` / `BzzStreamStart` prologue, then `BytesChunk`
/// frames, then `StreamDone` / `Error`.
#[allow(clippy::too_many_arguments)]
async fn stream_via_store(
    store: &MemChunkStore,
    reference: &[u8],
    path: Option<String>,
    allow_degraded: bool,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: mpsc::Sender<ControlAck>,
) {
    let is_bzz = path.is_some();
    let result = async {
        let (data_ref, content_type, filename, mutable) = match path {
            None => (reference.to_vec(), None, None, false),
            Some(p) => {
                let lookup = lookup_path(store, reference, &p)
                    .await
                    .map_err(|e| e.to_string())?;
                let filename = lookup.metadata.get("Filename").cloned().or_else(|| {
                    let trimmed = p.trim_matches('/');
                    if trimmed.is_empty() {
                        None
                    } else {
                        trimmed
                            .rsplit('/')
                            .next()
                            .map(std::string::ToString::to_string)
                    }
                });
                (
                    lookup.data_ref,
                    lookup.content_type,
                    filename,
                    lookup.is_feed,
                )
            }
        };
        // Encrypted manifest entry (64-byte `address ‖ key`): buffered
        // decrypt-join, like the production node loop — ciphertext trees
        // can't be streamed chunkwise.
        if let Ok(enc_ref) =
            <[u8; ant_retrieval::ENCRYPTED_REF_SIZE]>::try_from(data_ref.as_slice())
        {
            return stream_encrypted_via_store(
                store,
                enc_ref,
                content_type,
                filename,
                mutable,
                max_bytes,
                range,
                head_only,
                &ack,
            )
            .await;
        }
        let data_ref: [u8; 32] = data_ref
            .as_slice()
            .try_into()
            .map_err(|_| "manifest entry has invalid width".to_string())?;
        // Root fetch with dispersed-replica fallback (bee wraps the root
        // getter of every redundant download in `replicas.NewGetter`).
        let root = ant_retrieval::fetch_root_with_replicas(store, data_ref)
            .await
            .map_err(|e| e.to_string())?;
        // Mask the redundancy level off the high byte of the span so
        // the reported total reflects the real file size, matching the
        // production node loop's `root_span_to_u64_masked`.
        let mut span_bytes: [u8; 8] = root
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| "root shorter than span".to_string())?;
        span_bytes[7] = 0;
        let total = u64::from_le_bytes(span_bytes);
        let prologue = if is_bzz {
            ControlAck::BzzStreamStart {
                total_bytes: total,
                reference: data_ref.to_vec(),
                content_type,
                filename,
                mutable,
            }
        } else {
            ControlAck::BytesStreamStart { total_bytes: total }
        };
        ack.send(prologue)
            .await
            .map_err(|_| "stream closed".to_string())?;
        if head_only {
            return Ok::<(), String>(());
        }
        let (chunk_tx, mut chunk_rx) = mpsc::channel(8);
        let opts = JoinOptions {
            allow_degraded_redundancy: allow_degraded,
        };
        let cap = byte_cap(max_bytes);
        let body_range = range.and_then(|r| ByteRange::clamp(r.start, r.end_inclusive, total));
        let mut join_fut = Box::pin(join_to_sender_range(
            store, &root, cap, opts, body_range, chunk_tx,
        ));
        let mut join_result = None;
        loop {
            tokio::select! {
                result = &mut join_fut, if join_result.is_none() => {
                    join_result = Some(result.map_err(|e| e.to_string()));
                }
                maybe_chunk = chunk_rx.recv() => {
                    match maybe_chunk {
                        Some(data) => {
                            ack.send(ControlAck::BytesChunk { data })
                                .await
                                .map_err(|_| "stream closed".to_string())?;
                        }
                        None => return join_result.unwrap_or(Ok(())),
                    }
                }
            }
        }
    }
    .await;
    let terminal = match result {
        Ok(()) => ControlAck::StreamDone,
        Err(e) => ControlAck::Error { message: e },
    };
    let _ = ack.send(terminal).await;
}

/// Encrypted-body arm of [`stream_via_store`]: emit the `BzzStreamStart`
/// prologue (size from the decrypted root span), then the decrypt-joined
/// body as a single `BytesChunk`, applying any range by slicing.
#[allow(clippy::too_many_arguments)]
async fn stream_encrypted_via_store(
    store: &MemChunkStore,
    enc_ref: [u8; ant_retrieval::ENCRYPTED_REF_SIZE],
    content_type: Option<String>,
    filename: Option<String>,
    mutable: bool,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: &mpsc::Sender<ControlAck>,
) -> Result<(), String> {
    let addr: [u8; 32] = enc_ref[..32].try_into().expect("64-byte ref");
    let key: [u8; 32] = enc_ref[32..].try_into().expect("64-byte ref");
    // Root fetch with dispersed-replica fallback: replicas of an
    // encrypted root are keyed on its 32-byte address half and wrap the
    // encrypted root chunk, so the span prologue survives root loss just
    // like the buffered `join_encrypted` read does.
    let root_wire = ant_retrieval::fetch_root_with_replicas(store, addr)
        .await
        .map_err(|e| e.to_string())?;
    let (span, _) = ant_crypto::decrypt_chunk_parts(&root_wire, &key).map_err(|e| e.to_string())?;
    let (_, plain) = ant_retrieval::rs::decode_span(span);
    let total = u64::from_le_bytes(plain);
    ack.send(ControlAck::BzzStreamStart {
        total_bytes: total,
        reference: enc_ref.to_vec(),
        content_type,
        filename,
        mutable,
    })
    .await
    .map_err(|_| "stream closed".to_string())?;
    if head_only {
        return Ok(());
    }
    let body_range = range.and_then(|r| ByteRange::clamp(r.start, r.end_inclusive, total));
    if range.is_some() && body_range.is_none() {
        return Err("range not satisfiable for resource".to_string());
    }
    let body = ant_retrieval::join_encrypted(store, enc_ref, byte_cap(max_bytes))
        .await
        .map_err(|e| e.to_string())?;
    let data = match body_range {
        Some(r) => {
            let start = usize::try_from(r.start)
                .unwrap_or(usize::MAX)
                .min(body.len());
            let end = usize::try_from(r.end_inclusive)
                .unwrap_or(usize::MAX)
                .saturating_add(1)
                .min(body.len());
            body[start..end].to_vec()
        }
        None => body,
    };
    if !data.is_empty() {
        ack.send(ControlAck::BytesChunk { data })
            .await
            .map_err(|_| "stream closed".to_string())?;
    }
    Ok(())
}

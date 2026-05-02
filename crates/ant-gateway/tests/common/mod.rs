//! Shared test helpers for `ant-gateway` integration tests.
//!
//! These tests don't stand up a real listener / node; they build the
//! same `axum::Router` production uses via [`testkit::build_router`] and
//! drive it through `tower::ServiceExt::oneshot`. Two flavours of
//! handle:
//!
//! - [`status_only_router`] hands the router a fake `StatusSnapshot`
//!   and an `mpsc` receiver that's deliberately left dangling — useful
//!   for endpoint groups (status, stubs, fallback) that never dispatch
//!   a `ControlCommand`.
//! - [`handle_with_fixture_node`] additionally spawns a fake "node loop"
//!   task that handles `GetBzz` / `GetBytes` / `GetChunkRaw` against the
//!   live `bzz_fixture.rs` directory in `ant-retrieval/tests/fixtures`,
//!   exercising the production code paths end-to-end through the HTTP
//!   layer.
//!
//! Cargo compiles each `tests/*.rs` as its own crate, so any helper
//! that isn't called by *one specific* binary will warn. The whole
//! module is marked dead-code-allowed for that reason; the upstream
//! `cargo test -p ant-gateway` invocation still exercises every helper
//! across the test binaries.

#![allow(dead_code)]

use ant_control::{
    ControlAck, ControlCommand, GatewayActivity, IdentityInfo, PeerConnectionInfo, PeerInfo,
    RoutingInfo, StatusSnapshot, StreamRange, PROTOCOL_VERSION,
};
use ant_gateway::testkit::build_router;
use ant_gateway::{GatewayHandle, GatewayIdentity};
use ant_retrieval::{
    join_to_sender_range, join_with_options, list_manifest, lookup_path, ByteRange, ChunkFetcher,
    JoinOptions, DEFAULT_MAX_FILE_BYTES,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response};
use axum::Router;
use http_body_util::BodyExt;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tower::ServiceExt;

/// Path to the chunk fixture that backs the retrieval tests. Reuses
/// the directory already maintained by `ant-retrieval/tests/bzz_fixture.rs`
/// — see that test file for capture instructions. Pointed at via a
/// relative path from `CARGO_MANIFEST_DIR` rather than copied into the
/// gateway crate so a future refresh of the fixture lands once.
pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("ant-retrieval")
        .join("tests")
        .join("fixtures")
        .join("bzz-ab7720-13-4358-2645")
}

/// HashMap-backed `ChunkFetcher` mirroring the one in `bzz_fixture.rs`.
/// Duplicated here so the gateway tests don't depend on a private
/// helper from another crate's `tests/`.
pub struct DirFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
}

impl DirFetcher {
    pub fn from_dir(dir: &Path) -> std::io::Result<Self> {
        let mut chunks = HashMap::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if path.extension().and_then(|s| s.to_str()) != Some("bin") {
                continue;
            }
            let mut addr = [0u8; 32];
            if hex::decode_to_slice(stem, &mut addr).is_err() {
                continue;
            }
            chunks.insert(addr, std::fs::read(&path)?);
        }
        Ok(Self { chunks })
    }
}

#[async_trait]
impl ChunkFetcher for DirFetcher {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        self.chunks
            .get(&addr)
            .cloned()
            .ok_or_else(|| -> Box<dyn StdError + Send + Sync> {
                format!("fixture missing chunk {}", hex::encode(addr)).into()
            })
    }
}

/// A useful default snapshot: one connected peer with a BZZ overlay,
/// a populated routing table, and an obviously-fake but well-formed
/// identity. Caller can mutate the returned `StatusSnapshot` in place
/// before stuffing it in the watch channel.
pub fn snapshot_with_one_peer() -> StatusSnapshot {
    StatusSnapshot {
        agent: "antd/test".to_string(),
        protocol_version: PROTOCOL_VERSION,
        network_id: 1,
        pid: 0,
        started_at_unix: 1_700_000_000,
        identity: IdentityInfo {
            eth_address: "0x0102030405060708090a0b0c0d0e0f1011121314".to_string(),
            overlay: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                .to_string(),
            peer_id: "12D3KooWFakePeerId000000000000000000000000000".to_string(),
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
            },
        },
        listeners: vec!["/ip4/127.0.0.1/tcp/1634".to_string()],
        control_socket: "/tmp/antd.sock".to_string(),
        retrieval: Default::default(),
    }
}

/// `StatusSnapshot` with no connected peers and an empty routing table.
/// Used by `/readiness` to assert the not-ready response.
pub fn empty_snapshot() -> StatusSnapshot {
    let mut s = snapshot_with_one_peer();
    s.peers = PeerInfo::default();
    s
}

/// Default identity used by every test handle. Bee-shaped: bare hex for
/// overlay / publicKey, `0x`-prefixed for ethereum.
pub fn test_identity() -> GatewayIdentity {
    GatewayIdentity {
        overlay_hex: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899".to_string(),
        ethereum_hex: "0x0102030405060708090a0b0c0d0e0f1011121314".to_string(),
        public_key_hex: "020102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
            .to_string(),
        peer_id: "12D3KooWFakePeerId000000000000000000000000000".to_string(),
    }
}

/// Build a router whose `commands` channel is wired to a receiver
/// nobody reads — fine for status / stubs / 501 tests, fatal for any
/// retrieval test (it would deadlock waiting for an ack). For retrieval
/// tests use [`handle_with_fixture_node`] instead.
pub fn status_only_router(snapshot: StatusSnapshot) -> Router {
    let (status_tx, status_rx) = watch::channel(snapshot);
    // Keep the sender alive for the lifetime of the router so the
    // receiver doesn't observe `Closed` mid-test.
    Box::leak(Box::new(status_tx));
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(8);
    Box::leak(Box::new(cmd_rx));
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
    };
    build_router(handle)
}

/// Build a router whose `commands` channel is consumed by a fake node
/// loop that resolves retrieval commands against the on-disk fixture.
/// Returns the router only — the fake task lives for the test process
/// lifetime.
pub fn handle_with_fixture_node() -> Router {
    let snapshot = snapshot_with_one_peer();
    let (status_tx, status_rx) = watch::channel(snapshot);
    Box::leak(Box::new(status_tx));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(8);

    let fetcher = Arc::new(DirFetcher::from_dir(&fixture_dir()).expect("load fixture chunks"));
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let f = fetcher.clone();
            tokio::spawn(async move {
                handle_command(f.as_ref(), cmd).await;
            });
        }
    });

    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
    };
    build_router(handle)
}

/// Single-command dispatcher used by the fake node loop: mirrors the
/// production handlers in `ant-p2p` but pulls chunks from the fixture
/// `DirFetcher`. Keeping the dispatch shape identical means the test
/// exercises the same `ControlAck` parsing in `gateway/src/retrieval.rs`
/// production hits.
async fn handle_command(fetcher: &DirFetcher, cmd: ControlCommand) {
    match cmd {
        ControlCommand::GetChunkRaw { reference, ack } => {
            let reply = match fetcher.fetch(reference).await {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error {
                    message: format!("fetch chunk {}: {e}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::GetChunk { reference, ack } => {
            let reply = match fetcher.fetch(reference).await {
                Ok(data) => {
                    // Strip the 8-byte span to match `GetChunk` (payload only).
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
        ControlCommand::GetBytes {
            reference,
            bypass_cache: _,
            progress: _,
            max_bytes,
            ack,
        } => {
            let result = async {
                let root = fetcher.fetch(reference).await.map_err(|e| e.to_string())?;
                let opts = JoinOptions {
                    allow_degraded_redundancy: true,
                };
                let cap = max_bytes
                    .map(|n| usize::try_from(n).unwrap_or(usize::MAX))
                    .unwrap_or(DEFAULT_MAX_FILE_BYTES);
                join_with_options(fetcher, &root, cap, opts)
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
        ControlCommand::StreamBytes {
            reference,
            bypass_cache: _,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            stream_via_fetcher(
                fetcher,
                reference,
                /* path */ None,
                /* allow_degraded */ true,
                max_bytes,
                range,
                head_only,
                ack,
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
            stream_via_fetcher(
                fetcher,
                reference,
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
                let lookup = lookup_path(fetcher, reference, &path)
                    .await
                    .map_err(|e| e.to_string())?;
                let root = fetcher
                    .fetch(lookup.data_ref)
                    .await
                    .map_err(|e| e.to_string())?;
                let opts = JoinOptions {
                    allow_degraded_redundancy,
                };
                let cap = max_bytes
                    .map(|n| usize::try_from(n).unwrap_or(usize::MAX))
                    .unwrap_or(DEFAULT_MAX_FILE_BYTES);
                let body = join_with_options(fetcher, &root, cap, opts)
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
            let reply = match list_manifest(fetcher, reference).await {
                Ok(entries) => ControlAck::Manifest {
                    entries: entries
                        .into_iter()
                        .map(|entry| ant_control::ManifestEntryInfo {
                            path: entry.path,
                            reference: entry.reference.map(hex::encode),
                            metadata: entry.metadata,
                        })
                        .collect(),
                },
                Err(e) => ControlAck::Error {
                    message: e.to_string(),
                },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::ResetPeerstore { ack } => {
            let _ = ack.send(ControlAck::Ok {
                message: "ok".to_string(),
            });
        }
    }
}

/// Build a router whose `commands` channel is wired to a caller-provided
/// dispatcher. Lets a test inspect the exact `ControlCommand` the
/// gateway emitted (e.g. to assert that `max_bytes` carries the
/// gateway's larger ceiling, not the joiner default).
pub fn router_with_dispatcher<F, Fut>(dispatch: F) -> Router
where
    F: Fn(ControlCommand) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let snapshot = snapshot_with_one_peer();
    let (status_tx, status_rx) = watch::channel(snapshot);
    Box::leak(Box::new(status_tx));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(8);
    let dispatch = Arc::new(dispatch);
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let d = dispatch.clone();
            tokio::spawn(async move {
                d(cmd).await;
            });
        }
    });
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
    };
    build_router(handle)
}

/// Materialise a response body to `Vec<u8>`. Tests don't deal with
/// streaming here — the gateway joiner already collects everything
/// before handing it to axum, so a single buffer is sufficient.
pub async fn body_bytes(resp: Response<Body>) -> Vec<u8> {
    let (_, body) = resp.into_parts();
    body.collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec()
}

/// Helper: send a request through a router via `tower::ServiceExt::oneshot`.
pub async fn send(router: Router, req: Request<Body>) -> Response<Body> {
    router.oneshot(req).await.expect("router oneshot")
}

/// Streaming dispatcher used by both `StreamBytes` and `StreamBzz` in
/// the fake node loop. `path = None` means "treat `reference` as a raw
/// `/bytes` data root"; `path = Some(_)` walks the manifest first.
#[allow(clippy::too_many_arguments)]
async fn stream_via_fetcher(
    fetcher: &DirFetcher,
    reference: [u8; 32],
    path: Option<String>,
    allow_degraded: bool,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: tokio::sync::mpsc::Sender<ControlAck>,
) {
    let is_bzz = path.is_some();
    let result = async {
        let (data_ref, content_type, filename) = match path {
            None => (reference, None, None),
            Some(p) => {
                let lookup = lookup_path(fetcher, reference, &p)
                    .await
                    .map_err(|e| e.to_string())?;
                let filename = lookup.metadata.get("Filename").cloned().or_else(|| {
                    let trimmed = p.trim_matches('/');
                    if trimmed.is_empty() {
                        None
                    } else {
                        trimmed.rsplit('/').next().map(|s| s.to_string())
                    }
                });
                (lookup.data_ref, lookup.content_type, filename)
            }
        };
        let root = fetcher.fetch(data_ref).await.map_err(|e| e.to_string())?;
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
                content_type,
                filename,
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
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel(8);
        let opts = JoinOptions {
            allow_degraded_redundancy: allow_degraded,
        };
        let cap = max_bytes
            .map(|n| usize::try_from(n).unwrap_or(usize::MAX))
            .unwrap_or(DEFAULT_MAX_FILE_BYTES);
        let body_range = range.and_then(|r| ByteRange::clamp(r.start, r.end_inclusive, total));
        let mut join_fut = Box::pin(join_to_sender_range(
            fetcher, &root, cap, opts, body_range, chunk_tx,
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

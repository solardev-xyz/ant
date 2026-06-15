//! D.2.3 retrieval endpoints, streaming edition.
//!
//! Wraps the existing `ant-retrieval` machinery exposed by the node loop
//! over `ant-control`'s command channel. The gateway never talks to
//! libp2p directly — every chunk fetch goes through
//! [`ant_control::ControlCommand`], reusing the routing-aware
//! [`ant_retrieval::RoutingFetcher`], its hedging, and the long-lived
//! chunk cache. Tests swap a fake "node loop" in that replies to the
//! same commands from a fixture-backed map.
//!
//! `/bytes` and `/bzz` go through the streaming dispatch path in all
//! cases:
//!
//! - **GET, no Range**: full streaming join. Headers go out as soon as
//!   the daemon emits `BytesStreamStart` / `BzzStreamStart`; body bytes
//!   stream as the joiner emits ordered subtrees.
//! - **HEAD**: streaming dispatch with `head_only = true`. The daemon
//!   resolves manifest + root span and returns metadata without joining
//!   the chunk tree.
//! - **GET, single Range**: streaming dispatch with a clamped
//!   `StreamRange`. The daemon's range-aware joiner walks only the
//!   chunks that overlap the requested byte interval; the gateway emits
//!   `206 Partial Content` with `Content-Range` and the slice length.
//!
//! Multi-range requests are still rejected with `416` — the joiner
//! produces one ordered byte stream, multipart `byteranges` would
//! demand framing logic the plan explicitly defers (PLAN.md E.20).
//! Body payloads do not materialise in memory: `Body::from_stream`
//! pumps `BytesChunk` acks into hyper as they arrive.
//!
//! `GATEWAY_MAX_FILE_BYTES` keeps the joiner's per-request size cap
//! generous enough for real bzz sites with media payloads (videos,
//! archives, large images); the CLI continues to use
//! `ant_retrieval::DEFAULT_MAX_FILE_BYTES` (32 MiB) where `antctl get`
//! is the only caller.

use std::time::Duration;

use ant_control::{
    ActiveRequestGuard, ControlAck, ControlCommand, GatewayRequestKind, StreamRange,
};
use ant_retrieval::{
    build_collection_manifest, build_single_file_manifest, split_bytes, ManifestFile, SplitChunk,
};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures::stream;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// Bee-shaped per-request override for the per-chunk retrieval timeout.
/// We accept the header and use it to cap the *whole* request in this
/// first cut — finer-grained per-chunk plumbing through the control
/// channel is deferred (PLAN.md D.2.3 explicitly allows this scope).
const SWARM_CHUNK_RETRIEVAL_TIMEOUT: HeaderName =
    HeaderName::from_static("swarm-chunk-retrieval-timeout");
/// Bee request header: when `true`, treat the body as a tar archive and
/// build a multi-file manifest. Default `false` (single-file upload).
const SWARM_COLLECTION: HeaderName = HeaderName::from_static("swarm-collection");
/// Bee request header: name of the manifest entry that should resolve
/// when the user requests `bzz://<root>/` (no path). Mirrored on the
/// root manifest's `/` fork as `website-index-document` metadata.
const SWARM_INDEX_DOCUMENT: HeaderName = HeaderName::from_static("swarm-index-document");
/// Bee request header on `POST /soc/{owner}/{id}`: 65-byte secp256k1
/// signature over `keccak256(id || inner_cac_addr)` (EIP-191 wrapped),
/// hex-encoded with or without an `0x` prefix.
const SWARM_SOC_SIGNATURE: HeaderName = HeaderName::from_static("swarm-soc-signature");
/// Bee request header on `GET /feeds/{owner}/{topic}` (and `/soc`,
/// `/bzz`): when `true`, return only the root chunk's data
/// (`span(8 LE) || payload`) of the resolved content instead of joining
/// and streaming the whole chunk tree. Matches the `Swarm-Only-Root-Chunk`
/// parameter in `bee/pkg/api/feed.go::feedGetHandler`.
const SWARM_ONLY_ROOT_CHUNK: HeaderName = HeaderName::from_static("swarm-only-root-chunk");
/// Bee request header on uploads: a pre-created tag uid to attach this
/// upload to (bee-js sends it when the caller created a tag via
/// `POST /tags` first). Absent on most uploads — bee then auto-creates
/// a tag server-side.
const SWARM_TAG: HeaderName = HeaderName::from_static("swarm-tag");
/// Bee response header on uploads: the tag uid bee-js polls via
/// `GET /tags/{uid}` for progress (PLAN.md J.2.4 / J.4.5).
const SWARM_TAG_UID: HeaderName = HeaderName::from_static("swarm-tag-uid");
/// Bee request header on every upload: the 32-byte postage batch id
/// (64 hex, optional `0x`) that stamps the uploaded chunks. bee-js sends
/// it as `swarm-postage-batch-id` on `POST /bytes|/bzz|/chunks|/soc`.
/// Required: the node selects the matching registered issuer, so a
/// missing / malformed / unknown batch is a `400` (bee's shape).
const SWARM_POSTAGE_BATCH_ID: HeaderName = HeaderName::from_static("swarm-postage-batch-id");

/// Concurrency cap on outbound `PushChunk` commands during a `POST /bzz`
/// upload. Matched to the daemon-side
/// `ant_node::uploads::MAX_PUSH_CONCURRENCY`. See the doc comment
/// there for sizing rationale: bee's per-peer stream acceptance is
/// the bottleneck, not our concurrency cap.
const MAX_PUSH_CONCURRENCY: usize = 32;

/// Hard cap on a single `POST /bzz` request body. Sized to comfortably
/// hold a small website or media payload while keeping the in-memory
/// buffering of the body and split chunks bounded — the gateway holds
/// the entire upload + every assembled chunk in memory until pushsync
/// finishes. 64 MiB covers a typical photo album or short audio file
/// without letting one bad client tip the daemon over.
pub const GATEWAY_MAX_UPLOAD_BYTES: u64 = 64 * 1024 * 1024;

/// `Cache-Control` value emitted on every content-addressed response
/// (`/chunks`, `/bytes`, `/bzz`, `/v0/manifest`). All four endpoints
/// take a 32-byte content hash in the URL — the asset *is* its hash,
/// so revisiting the same URL is provably safe to serve from the
/// browser's HTTP cache forever.
///
/// This matches what bee's gateway has historically returned (cf.
/// `ethersphere/swarm` issue #2213, where bee shipped `max-age=
/// 2147483648, immutable` for bzz responses) and is the reason bee
/// "loads super-fast on reload": after the first fetch, the browser
/// services subsequent requests entirely out of its own HTTP cache
/// without re-walking the manifest or re-joining the BMT. Without
/// this header, a music-player UI loading 13 audio tracks plus
/// artwork on every page navigation re-pays the full retrieval cost,
/// which is a noticeable user-facing regression vs bee.
///
/// Year (`31_536_000` s) instead of bee's 68-year value because some
/// HTTP intermediaries treat unrealistically large `max-age` as
/// invalid; `immutable` already encodes "don't ever revalidate" for
/// browsers that support it (Firefox, Safari, modern Chrome).
const IMMUTABLE_CACHE_CONTROL: HeaderValue =
    HeaderValue::from_static("public, max-age=31536000, immutable");

/// Apply the immutable `Cache-Control` header to a response targeted
/// at a content-addressed endpoint. Idempotent — calling twice just
/// overwrites the existing entry. Public to the module so each
/// response builder can opt in without re-inserting the header
/// inline.
fn set_immutable_cache_headers(resp: &mut Response) {
    let _ = resp
        .headers_mut()
        .insert(header::CACHE_CONTROL, IMMUTABLE_CACHE_CONTROL);
}
/// Hard cap on a `/bytes` or `/bzz` request even with no header.
///
/// Sized for the realistic worst case observed in production: a 44 MiB
/// WAV (~11 K leaf chunks, 88 L1 intermediates) over the live mainnet
/// completes a *single* fetch in ~165 s at our `FETCH_FANOUT=8` joiner
/// width and ~150 ms per-chunk RTT. Concurrent `bzz://` fetches
/// (browsers happily issue 4–6 at once for media galleries) share the
/// process-wide retrieval semaphore (`RETRIEVAL_INFLIGHT_CAP=32` in
/// ant-p2p). 600 s is still a large envelope, but it keeps a wedged
/// handler from pinning a hyper task indefinitely while allowing the
/// concurrent media path to complete under normal mainnet latency.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_mins(10);
/// Single-chunk fetches are bounded by `ant-retrieval`'s internal
/// timeout (20 s) plus the outer retry loop. 60 s leaves room for
/// retries on a single chunk without inheriting the much larger
/// multi-chunk envelope.
const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_mins(1);

/// Per-request body-size ceiling for `/bytes` and `/bzz`. Bee streams,
/// so it imposes no cap at all; the joiner used to materialise the
/// whole body before responding (hence the original 1 GiB sizing),
/// but `dispatch_bzz`/`dispatch_bytes` now go through
/// `join_to_sender_range`, which streams via a bounded
/// (`FETCH_FANOUT * STREAM_PIPE_DEPTH * CHUNK_SIZE` ≈ 4 MiB) pipeline
/// regardless of declared span. The cap is therefore a "sanity bound
/// on the declared span" rather than a memory cap: it stops a
/// pathological manifest claiming a petabyte from making us walk
/// trillions of intermediate chunks before failing.
///
/// 16 GiB sized to cover the largest AI model weights we expect to
/// host (e.g. 70B-parameter LLMs at 4-bit quantisation are ~35-50 GB
/// — those still need a higher ceiling, but at that point the
/// operator should opt in explicitly), and to comfortably swallow
/// modern AV1 long-form video and disk images. The CLI keeps the
/// much tighter `ant_retrieval::DEFAULT_MAX_FILE_BYTES` (32 MiB)
/// since its audience is interactive `antctl get`, not a browser.
///
/// Pre-2026-05-09 this was 1 GiB; raised after the gemma4:e2b
/// 6.7 GiB GGUF upload surfaced "file too large: span 7162394016
/// bytes, cap 1073741824 bytes" on the very first retrieval (see
/// PLAN.md Phase 7g).
pub const GATEWAY_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// `Cache-Control` value for endpoints returning chunks at addresses
/// that may be re-stamped with a new payload. SOCs are addressed by
/// `keccak256(id || owner)`, which is stable across payload updates,
/// so the same URL can return different bytes after an owner update;
/// browsers must not freeze the first response forever the way they do
/// for content-addressed `/chunks/{addr}`.
const MUTABLE_CACHE_CONTROL: HeaderValue = HeaderValue::from_static("no-cache");

/// `GET /chunks/{addr}` and `HEAD /chunks/{addr}`. Returns the chunk's
/// **wire bytes** (`span(8 LE) || payload`) — bee's `chunkstore.Get`
/// returns `Chunk.Data()` which is the wire form, so we must too.
/// Otherwise consumers reuploading via `/chunks` POST would get a
/// malformed chunk.
pub async fn chunk(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, e),
    };

    // Register the request with the live gateway-activity registry so
    // it shows up in the `antop` Retrieval tab. The guard's
    // `Drop` impl removes the entry when this handler returns, so
    // panics and early `return`s clean up automatically.
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, short_reference(&addr));

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::GetChunkRaw {
        reference,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    // Single-chunk fetches go through `GetChunkRaw`, which doesn't
    // emit `Progress` acks — we just record the in-flight count
    // until the bytes come back. The body materialises in one
    // shot so there's no streaming-body lifetime to worry about
    // beyond this handler frame.
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("retrieval timed out after {}s", timeout.as_secs()),
            );
        }
    };

    let bytes = match ack {
        ControlAck::Bytes { data } => data,
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        ControlAck::Error { message } => {
            return json_error(StatusCode::NOT_FOUND, message);
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };
    let len = bytes.len() as u64;
    guard.update(1, 1, 0, len);
    chunk_response(method, bytes)
}

/// `GET /soc/{owner}/{id}` and `HEAD /soc/{owner}/{id}` — fetch a
/// single-owner-chunk by `(owner, id)`. The path carries the 20-byte
/// owner Ethereum address and the 32-byte id, both hex-encoded with
/// optional `0x` / `0X` prefix. The gateway derives
/// `address = keccak256(id || owner)` and dispatches the same
/// retrieval path used by `/chunks/{addr}`; `ant_retrieval::retrieve_chunk`
/// validates the response via `soc_valid`, so no further crypto is
/// done here.
///
/// Returns the SOC's **wire bytes** (`id(32) || sig(65) || inner_cac`)
/// with `Content-Type: application/octet-stream`. Cache-Control is
/// `no-cache` because the same address is mutable: a later
/// `POST /soc/{owner}/{id}` by the same owner replaces the payload at
/// the same URL.
///
/// Bee's HTTP API exposes the same shape on `GET /soc/{owner}/{id}`,
/// see `bee/pkg/api/soc.go`.
pub async fn download_soc(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, id_hex)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let owner = match parse_hex_fixed::<20>(&owner_hex) {
        Ok(o) => o,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad owner: {e}")),
    };
    let id = match parse_hex_fixed::<32>(&id_hex) {
        Ok(i) => i,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad id: {e}")),
    };

    let mut addr_input = [0u8; 32 + 20];
    addr_input[..32].copy_from_slice(&id);
    addr_input[32..].copy_from_slice(&owner);
    let reference = ant_crypto::keccak256(&addr_input);

    let guard = handle.activity.begin(
        GatewayRequestKind::Soc,
        short_reference(&hex::encode(reference)),
    );

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::GetChunkRaw {
        reference,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("retrieval timed out after {}s", timeout.as_secs()),
            );
        }
    };

    let bytes = match ack {
        ControlAck::Bytes { data } => data,
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        ControlAck::Error { message } => {
            return json_error(StatusCode::NOT_FOUND, message);
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw (soc)");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };
    let len = bytes.len() as u64;
    guard.update(1, 1, 0, len);
    soc_response(method, bytes)
}

/// Build the `200 OK` (or empty `200 OK` for `HEAD`) response for a
/// SOC fetch. Mirrors [`chunk_response`] for layout, but stamps a
/// mutable `Cache-Control` because SOCs at the same address can be
/// re-uploaded with new payload.
fn soc_response(method: Method, body: Vec<u8>) -> Response {
    let len = body.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(body)
    };
    let mut resp = Response::new(body);
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    let _ = resp
        .headers_mut()
        .insert(header::CACHE_CONTROL, MUTABLE_CACHE_CONTROL);
    resp
}

/// Decode a fixed-length hex string with optional `0x` prefix into a
/// byte array. The error string starts with `"must be N bytes …"`;
/// callers prepend a field name (`"reference {…}"`, `"owner {…}"`)
/// to produce a 400 body that pinpoints which path segment was
/// malformed.
fn parse_hex_fixed<const N: usize>(s: &str) -> Result<[u8; N], String> {
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if stripped.len() != N * 2 {
        return Err(format!(
            "must be {} bytes ({} hex chars); got {}",
            N,
            N * 2,
            stripped.len()
        ));
    }
    let mut out = [0u8; N];
    hex::decode_to_slice(stripped, &mut out).map_err(|e| format!("invalid hex: {e}"))?;
    Ok(out)
}

/// Extract the postage batch id from the `swarm-postage-batch-id`
/// request header. Returns a bee-shaped `400` response when the header
/// is missing or not 32-byte hex — the node then selects the registered
/// issuer for this id (an unknown batch fails the push with
/// `"batch … not usable"`, which the upload handlers map to `400`).
#[allow(clippy::result_large_err)]
fn parse_postage_batch_header(headers: &HeaderMap) -> Result<[u8; 32], Response> {
    let raw = headers
        .get(&SWARM_POSTAGE_BATCH_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            json_error(
                StatusCode::BAD_REQUEST,
                "missing required swarm-postage-batch-id header",
            )
        })?;
    parse_hex_fixed::<32>(raw).map_err(|e| {
        json_error(
            StatusCode::BAD_REQUEST,
            format!("bad swarm-postage-batch-id: {e}"),
        )
    })
}

/// `POST /chunks` — accept a single content-addressed chunk's wire
/// bytes (`span(8 LE) || payload`), stamp it locally, and pushsync it
/// to the closest neighbourhood peer. Body cap matches bee:
/// `8 + 4096`. Returns bee-shaped `{"reference": "<lowercase 64-hex>"}`
/// with status 201 on success.
pub async fn upload_chunk(
    State(handle): State<GatewayHandle>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() < 8 || body.len() > 8 + 4096 {
        return json_error(
            StatusCode::BAD_REQUEST,
            format!("chunk wire size out of range: {} bytes", body.len()),
        );
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, "upload".to_string());

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::PushChunk {
        wire: body.to_vec(),
        batch_id,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("upload timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::ChunkUploaded { reference } => {
            guard.update(1, 1, 0, body.len() as u64);
            let stripped = reference
                .strip_prefix("0x")
                .or_else(|| reference.strip_prefix("0X"))
                .unwrap_or(reference.as_str())
                .to_string();
            let mut resp = json_response(
                StatusCode::CREATED,
                &serde_json::json!({ "reference": stripped }),
            );
            set_immutable_cache_headers(&mut resp);
            resp
        }
        ControlAck::Error { message } => {
            // Bee mirrors not-stamped / out-of-batch failures with 4xx;
            // map "uploads not configured" to 503 because the operator
            // can fix it without a code change.
            let status = if message.starts_with("uploads not configured") {
                StatusCode::SERVICE_UNAVAILABLE
            } else if message.starts_with("chunk wire size") || message.contains("not usable") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::BAD_GATEWAY
            };
            json_error(status, message)
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PushChunk");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `POST /soc/{owner}/{id}` — accept a single-owner-chunk's inner CAC
/// payload, validate the signature, then stamp + pushsync the SOC. The
/// path carries the 32-byte `id` and the 20-byte `owner` Ethereum
/// address; the signature is supplied via the `swarm-soc-signature`
/// header (bee convention). The body is the **inner CAC wire**:
/// `span(8 LE) || payload(≤4096)`. The full SOC wire that hits the
/// network is `id || sig || body`, addressed at
/// `keccak256(id || owner)`.
///
/// Bee's HTTP API exposes the same shape on `POST /soc/{owner}/{id}`,
/// see `bee/pkg/api/soc.go`. Returns bee-shaped
/// `{"reference": "<lowercase 64-hex>"}` with status 201 on success.
pub async fn upload_soc(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, id_hex)): Path<(String, String)>,
    Query(query): Query<UploadSocQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let owner = match parse_hex_fixed::<20>(&owner_hex) {
        Ok(o) => o,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad owner: {e}")),
    };
    let id = match parse_hex_fixed::<32>(&id_hex) {
        Ok(i) => i,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad id: {e}")),
    };

    // Bee accepts the SOC signature in either the `swarm-soc-signature`
    // header (older bee / bee-js ≤ 9) or the `?sig=…` query parameter
    // (modern bee / bee-js ≥ 10). Mirror that to keep both client
    // generations working unchanged.
    let sig_str: &str = if let Some(h) = headers.get(&SWARM_SOC_SIGNATURE) {
        match h.to_str() {
            Ok(s) => s,
            Err(_) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "swarm-soc-signature must be ascii hex",
                );
            }
        }
    } else if let Some(s) = query.sig.as_deref() {
        s
    } else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "missing soc signature: provide swarm-soc-signature header or ?sig=… query param",
        );
    };
    let sig = match parse_hex_fixed::<65>(sig_str) {
        Ok(s) => s,
        Err(e) => {
            return json_error(StatusCode::BAD_REQUEST, format!("bad soc signature: {e}"));
        }
    };

    if body.len() < 8 || body.len() > 8 + 4096 {
        return json_error(
            StatusCode::BAD_REQUEST,
            format!("soc inner cac size out of range: {} bytes", body.len()),
        );
    }

    // SOC wire: id(32) || sig(65) || inner_cac(span(8) || payload).
    let mut wire = Vec::with_capacity(32 + 65 + body.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&body);

    // Address binds id and owner: address = keccak256(id || owner).
    let mut addr_input = [0u8; 32 + 20];
    addr_input[..32].copy_from_slice(&id);
    addr_input[32..].copy_from_slice(&owner);
    let address = ant_crypto::keccak256(&addr_input);

    if !ant_crypto::soc_valid(&address, &wire) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "soc signature does not recover the supplied owner",
        );
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let guard = handle.activity.begin(
        GatewayRequestKind::Soc,
        short_reference(&hex::encode(address)),
    );

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::PushSoc {
        address,
        wire,
        batch_id,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("upload timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::ChunkUploaded { reference } => {
            guard.update(1, 1, 0, body.len() as u64);
            let stripped = reference
                .strip_prefix("0x")
                .or_else(|| reference.strip_prefix("0X"))
                .unwrap_or(reference.as_str())
                .to_string();
            // SOC writes are mutable at the address level (the owner can
            // sign a new payload at the same id), so don't apply the
            // immutable cache headers `upload_chunk` uses.
            json_response(
                StatusCode::CREATED,
                &serde_json::json!({ "reference": stripped }),
            )
        }
        ControlAck::NotReady { message } => json_error(StatusCode::SERVICE_UNAVAILABLE, message),
        ControlAck::Error { message } => {
            let status = if message.contains("not usable") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::BAD_GATEWAY
            };
            json_error(status, message)
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PushSoc");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `POST /bzz` — single-file or collection upload. Bee-shaped: the
/// gateway splits the body into a chunk tree (4 KiB leaves +
/// intermediate join chunks), wraps it in a Mantaray manifest with
/// the right per-file metadata, then pushes every chunk to the
/// network's pushsync neighbourhood. Returns
/// `{"reference":"<root manifest hash>"}` with status 201.
///
/// Body interpretation depends on the `swarm-collection` request
/// header (Bee compat) — defaults to `false`:
///
/// - `swarm-collection: false`: the body is one file. The filename
///   comes from the `?name=` query parameter (bee uses the same
///   knob); if absent, the manifest entry is `index.html`. The MIME
///   type comes from the `Content-Type` request header (default
///   `application/octet-stream`).
/// - `swarm-collection: true`: the body is an uncompressed tar
///   archive. Each regular file becomes a manifest entry under its
///   archive path. `swarm-index-document` selects the file that
///   `bzz://<root>/` resolves to (default: `index.html` if present).
///
/// Cap: [`GATEWAY_MAX_UPLOAD_BYTES`] on the overall body. The
/// manifest itself today has to fit in one CAC chunk (≤ ~30 entries
/// with average filename length); collections that exceed that
/// return 413 with a message pointing at the future multi-chunk
/// manifest work.
pub async fn upload_bzz(
    State(handle): State<GatewayHandle>,
    Query(query): Query<UploadBzzQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() as u64 > GATEWAY_MAX_UPLOAD_BYTES {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "request body {} bytes exceeds upload cap {} bytes",
                body.len(),
                GATEWAY_MAX_UPLOAD_BYTES
            ),
        );
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let collection = headers
        .get(SWARM_COLLECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"));

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);

    let activity_label = match (collection, query.name()) {
        (true, _) => "upload-bzz-collection".to_string(),
        (false, Some(name)) => format!("upload-bzz {name}"),
        (false, None) => "upload-bzz".to_string(),
    };
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Bzz, activity_label);

    let assembled = match assemble_bzz(&query, &headers, &body, collection) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let total_chunks = assembled.chunks.len();
    debug!(
        target: "ant_gateway",
        collection,
        body_len = body.len(),
        chunks = total_chunks,
        manifest_root = %hex::encode(assembled.root),
        "bzz upload assembled"
    );
    guard.update(0, total_chunks as u64, total_chunks as u32, 0);

    if let Err(err_resp) = push_chunks(&handle, &assembled.chunks, batch_id, timeout, &guard).await
    {
        return err_resp;
    }

    let reference_hex = hex::encode(assembled.root);
    let mut resp = json_response(
        StatusCode::CREATED,
        &serde_json::json!({ "reference": reference_hex }),
    );
    set_immutable_cache_headers(&mut resp);
    finalize_upload_tag(
        &handle,
        &headers,
        total_chunks as u64,
        assembled.root,
        &mut resp,
    );
    resp
}

/// Resolve (or create) the upload tag for a just-finished synchronous
/// upload and stamp the `Swarm-Tag-Uid` response header so bee-js can
/// poll `GET /tags/{uid}`. If the client pre-created a tag and sent it
/// in `Swarm-Tag`, that one is marked complete and echoed; otherwise a
/// fresh already-synced tag is minted (bee's server-side auto-tag
/// behaviour). The whole upload is already pushsynced by the time we
/// get here, so the tag is reported fully synced immediately.
fn finalize_upload_tag(
    handle: &GatewayHandle,
    headers: &HeaderMap,
    total_chunks: u64,
    root: [u8; 32],
    resp: &mut Response,
) {
    let uid = match headers
        .get(&SWARM_TAG)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u32>().ok())
    {
        Some(existing) => {
            handle.tags.complete(existing, total_chunks, root);
            existing
        }
        None => handle.tags.create_completed(total_chunks, root),
    };
    if let Ok(v) = HeaderValue::from_str(&uid.to_string()) {
        resp.headers_mut().insert(SWARM_TAG_UID, v);
    }
}

/// `POST /bytes` — raw byte upload. Splits the body into a content-
/// addressed chunk tree (no manifest) and pushsyncs every chunk,
/// returning `{"reference":"<data-root hash>"}` with status 201. This
/// is what bee-js uses for feed payloads larger than one chunk and for
/// direct `bee.uploadData` calls (PLAN.md J.2.5 / C1). The returned
/// reference is a `/bytes/<ref>` reference — it has no mantaray
/// manifest, unlike `POST /bzz`.
pub async fn upload_bytes(
    State(handle): State<GatewayHandle>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() as u64 > GATEWAY_MAX_UPLOAD_BYTES {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "request body {} bytes exceeds upload cap {} bytes",
                body.len(),
                GATEWAY_MAX_UPLOAD_BYTES
            ),
        );
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Bytes, "upload-bytes".to_string());

    let split = split_bytes(&body);
    let total_chunks = split.chunks.len();
    debug!(
        target: "ant_gateway",
        body_len = body.len(),
        chunks = total_chunks,
        data_root = %hex::encode(split.root),
        "bytes upload assembled"
    );
    guard.update(0, total_chunks as u64, total_chunks as u32, 0);

    if let Err(err_resp) = push_chunks(&handle, &split.chunks, batch_id, timeout, &guard).await {
        return err_resp;
    }

    let reference_hex = hex::encode(split.root);
    let mut resp = json_response(
        StatusCode::CREATED,
        &serde_json::json!({ "reference": reference_hex }),
    );
    set_immutable_cache_headers(&mut resp);
    finalize_upload_tag(
        &handle,
        &headers,
        total_chunks as u64,
        split.root,
        &mut resp,
    );
    resp
}

/// `POST /feeds/{owner}/{topic}` — create a feed manifest (bee-js
/// `createFeedManifest`). Builds the mantaray feed manifest binding
/// `(owner, topic)` for a `Sequence` feed, pushsyncs its single chunk,
/// and returns `{"reference":"<manifest root>"}` (PLAN.md J.2.5 / C2).
/// The reference resolves transparently to the feed's current update
/// via any bee gateway. `?type=` defaults to `sequence`; `epoch` feeds
/// are not supported (501), matching the read path.
pub async fn create_feed(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, topic_hex)): Path<(String, String)>,
    Query(query): Query<FeedQuery>,
    headers: HeaderMap,
) -> Response {
    let owner = match parse_hex_fixed::<20>(&owner_hex) {
        Ok(o) => o,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad owner: {e}")),
    };
    let topic = match parse_hex_fixed::<32>(&topic_hex) {
        Ok(t) => t,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad topic: {e}")),
    };

    if let Some(kind) = query.r#type.as_deref() {
        match kind.to_ascii_lowercase().as_str() {
            "sequence" => {}
            "epoch" => {
                return json_error(StatusCode::NOT_IMPLEMENTED, "epoch feeds are not supported");
            }
            other => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("unknown feed type: {other}"),
                );
            }
        }
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);
    let guard = handle.activity.begin(
        GatewayRequestKind::Feed,
        short_reference(&hex::encode(topic)),
    );

    let manifest = match ant_retrieval::build_feed_manifest(&owner, &topic) {
        Ok(m) => m,
        Err(e) => return map_manifest_error(e),
    };
    let total_chunks = manifest.chunks.len();
    guard.update(0, total_chunks as u64, total_chunks as u32, 0);

    if let Err(err_resp) = push_chunks(&handle, &manifest.chunks, batch_id, timeout, &guard).await {
        return err_resp;
    }

    let reference_hex = hex::encode(manifest.root);
    let mut resp = json_response(
        StatusCode::CREATED,
        &serde_json::json!({ "reference": reference_hex }),
    );
    // Feed manifests are content-addressed and immutable (the manifest
    // binds owner+topic forever; only the feed *updates* it points at
    // change), so the manifest reference itself caches like any CAC.
    set_immutable_cache_headers(&mut resp);
    resp
}

/// Plan: address of the manifest root + every chunk that has to be
/// pushed for the upload to be retrievable (data leaves +
/// intermediate join chunks + manifest chunks).
struct AssembledUpload {
    root: [u8; 32],
    chunks: Vec<SplitChunk>,
}

#[allow(clippy::result_large_err)]
fn assemble_bzz(
    query: &UploadBzzQuery,
    headers: &HeaderMap,
    body: &Bytes,
    collection: bool,
) -> Result<AssembledUpload, Response> {
    if collection {
        let index_doc = headers
            .get(SWARM_INDEX_DOCUMENT)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().trim_start_matches('/').to_string())
            .filter(|s| !s.is_empty());
        assemble_collection(body, index_doc.as_deref())
    } else {
        let filename = query
            .name()
            .map(|s| s.trim().trim_start_matches('/').to_string())
            .or_else(|| {
                headers
                    .get(SWARM_INDEX_DOCUMENT)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.trim().trim_start_matches('/').to_string())
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "index.html".to_string());

        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && s != "application/octet-stream");

        assemble_single_file(body, &filename, content_type.as_deref())
    }
}

#[allow(clippy::result_large_err)]
fn assemble_single_file(
    body: &Bytes,
    filename: &str,
    content_type: Option<&str>,
) -> Result<AssembledUpload, Response> {
    let split = split_bytes(body);
    let manifest = match build_single_file_manifest(filename, content_type, split.root) {
        Ok(m) => m,
        Err(e) => return Err(map_manifest_error(e)),
    };
    let mut chunks = split.chunks;
    chunks.extend(manifest.chunks);
    Ok(AssembledUpload {
        root: manifest.root,
        chunks,
    })
}

#[allow(clippy::result_large_err)]
fn assemble_collection(body: &Bytes, index_doc: Option<&str>) -> Result<AssembledUpload, Response> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(body.as_ref()));
    let mut files: Vec<ManifestFile> = Vec::new();
    let mut data_chunks: Vec<SplitChunk> = Vec::new();

    let entries = match archive.entries() {
        Ok(e) => e,
        Err(e) => {
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                format!("invalid tar archive: {e}"),
            ));
        }
    };
    for entry in entries {
        let mut entry = match entry {
            Ok(e) => e,
            Err(e) => {
                return Err(json_error(
                    StatusCode::BAD_REQUEST,
                    format!("malformed tar entry: {e}"),
                ));
            }
        };
        // Only regular files become manifest entries. Directories,
        // symlinks, hardlinks, longlink/longname pax extensions etc. are
        // silently skipped — bee's collection upload does the same.
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let path_owned = match entry.path() {
            Ok(p) => p.to_path_buf(),
            Err(e) => {
                return Err(json_error(
                    StatusCode::BAD_REQUEST,
                    format!("tar entry has bad path: {e}"),
                ));
            }
        };
        let path_str = match path_owned.to_str() {
            Some(s) => s
                .trim_start_matches("./")
                .trim_start_matches('/')
                .to_string(),
            None => {
                return Err(json_error(
                    StatusCode::BAD_REQUEST,
                    "tar entry path is not valid UTF-8",
                ));
            }
        };
        if path_str.is_empty() {
            continue;
        }

        let mut buf = Vec::with_capacity(entry.header().size().unwrap_or(0) as usize);
        if let Err(e) = std::io::Read::read_to_end(&mut entry, &mut buf) {
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                format!("tar entry {path_str}: read failed: {e}"),
            ));
        }
        let split = split_bytes(&buf);
        data_chunks.extend(split.chunks);
        let ct = content_type_from_extension(&path_str).map(std::string::ToString::to_string);
        files.push(ManifestFile {
            path: path_str,
            content_type: ct,
            data_ref: split.root,
        });
    }
    if files.is_empty() {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "tar archive contains no regular files",
        ));
    }

    // Default index doc: `index.html` if it's present and the caller
    // didn't override. Bee's `Bee.UploadCollection` uses the same heuristic.
    let resolved_index = index_doc.map(str::to_string).or_else(|| {
        files
            .iter()
            .find(|f| f.path == "index.html")
            .map(|f| f.path.clone())
    });
    let manifest = match build_collection_manifest(&files, resolved_index.as_deref()) {
        Ok(m) => m,
        Err(e) => return Err(map_manifest_error(e)),
    };

    let mut chunks = data_chunks;
    chunks.extend(manifest.chunks);
    Ok(AssembledUpload {
        root: manifest.root,
        chunks,
    })
}

#[allow(clippy::result_large_err)]
fn map_manifest_error(e: ant_retrieval::manifest_writer::ManifestWriteError) -> Response {
    use ant_retrieval::manifest_writer::ManifestWriteError;
    match e {
        ManifestWriteError::SegmentTooLong { .. } | ManifestWriteError::EmptyPath => {
            json_error(StatusCode::BAD_REQUEST, e.to_string())
        }
        ManifestWriteError::ManifestTooBig { .. } => {
            json_error(StatusCode::PAYLOAD_TOO_LARGE, e.to_string())
        }
    }
}

/// Push `chunks` to the network in parallel via `PushChunk` control
/// commands. Bounded by `MAX_PUSH_CONCURRENCY` so a 1 K-chunk file
/// doesn't open 1 K simultaneous outbound libp2p streams. Returns an
/// HTTP error response on the first chunk that fails to push.
#[allow(clippy::result_large_err)]
async fn push_chunks(
    handle: &GatewayHandle,
    chunks: &[SplitChunk],
    batch_id: [u8; 32],
    timeout: std::time::Duration,
    guard: &ActiveRequestGuard,
) -> Result<(), Response> {
    let total = chunks.len() as u64;
    let pushed = std::sync::atomic::AtomicU64::new(0);
    let bytes_done = std::sync::atomic::AtomicU64::new(0);
    let push_one = |chunk: SplitChunk| {
        let handle = handle.clone();
        let pushed = &pushed;
        let bytes_done = &bytes_done;
        async move {
            let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
            let cmd = ControlCommand::PushChunk {
                wire: chunk.wire.clone(),
                batch_id,
                ack: ack_tx,
            };
            if handle.commands.send(cmd).await.is_err() {
                return Err(node_unavailable());
            }
            match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(ControlAck::ChunkUploaded { reference })) => {
                    let stripped = reference
                        .strip_prefix("0x")
                        .or_else(|| reference.strip_prefix("0X"))
                        .unwrap_or(reference.as_str())
                        .to_string();
                    let want = hex::encode(chunk.address);
                    if stripped != want {
                        return Err(json_error(
                            StatusCode::BAD_GATEWAY,
                            format!("node reference {stripped} != local {want} (BMT mismatch)"),
                        ));
                    }
                    pushed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    bytes_done.fetch_add(
                        chunk.wire.len() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    Ok(())
                }
                Ok(Ok(ControlAck::Error { message })) => {
                    let status = if message.starts_with("uploads not configured") {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else if message.contains("not usable") {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::BAD_GATEWAY
                    };
                    Err(json_error(status, format!("push chunk failed: {message}")))
                }
                Ok(Ok(other)) => {
                    warn!(target: "ant_gateway", ?other, "unexpected ack from PushChunk");
                    Err(json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unexpected node ack",
                    ))
                }
                Ok(Err(_)) => Err(node_unavailable()),
                Err(_) => Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("upload timed out after {}s", timeout.as_secs()),
                )),
            }
        }
    };

    let mut stream = stream::iter(chunks.iter().cloned())
        .map(push_one)
        .buffer_unordered(MAX_PUSH_CONCURRENCY);

    while let Some(result) = stream.next().await {
        result?;
        let p = pushed.load(std::sync::atomic::Ordering::Relaxed);
        let b = bytes_done.load(std::sync::atomic::Ordering::Relaxed);
        let in_flight = (total - p).min(u64::from(u32::MAX)) as u32;
        guard.update(p, total, in_flight, b);
    }
    Ok(())
}

/// Trim a hex reference to its leading 8 chars (16 if the caller
/// passed `0x` prefix) so the `antop` Retrieval tab column stays
/// readable. Empty / invalid references just round-trip unchanged.
fn short_reference(addr: &str) -> String {
    let stripped = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .unwrap_or(addr);
    let head: String = stripped.chars().take(8).collect();
    if head.is_empty() {
        addr.to_string()
    } else {
        head
    }
}

fn chunk_response(method: Method, body: Vec<u8>) -> Response {
    let len = body.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(body)
    };
    let mut resp = Response::new(body);
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    set_immutable_cache_headers(&mut resp);
    resp
}

/// `GET /bytes/{addr}` and `HEAD /bytes/{addr}`.
///
/// Drives the streaming dispatcher for every shape: full GET, HEAD, and
/// single-range GET. The daemon's range-aware joiner does the heavy
/// lifting — the gateway only translates HTTP semantics into a
/// [`StreamRange`] and emits the right status / headers.
pub async fn bytes(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    Query(query): Query<BytesQuery>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, e),
    };

    let raw_range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let head_only = method == Method::HEAD;

    let activity_guard = Some(
        handle
            .activity
            .begin(GatewayRequestKind::Bytes, short_reference(&addr)),
    );

    // Phase 1: dispatch with head_only=true if the caller is HEAD;
    // for GET we go straight to the streaming dispatcher.
    let mut started =
        match dispatch_stream_bytes(&handle, reference, timeout, None, head_only, activity_guard)
            .await
        {
            Ok(s) => s,
            Err(e) => return e,
        };

    let total = started.total_bytes;
    let parsed_range = match raw_range.as_deref() {
        None => None,
        Some(raw) => match parse_single_range(raw, total) {
            Ok(r) => r,
            Err(RangeError::Multi) => {
                started.cancel();
                return json_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "multi-range requests not supported",
                );
            }
            Err(RangeError::Unsatisfiable) => {
                started.cancel();
                let mut resp = json_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "range not satisfiable for this resource",
                );
                let cr = format!("bytes */{total}");
                if let Ok(v) = HeaderValue::from_str(&cr) {
                    resp.headers_mut().insert(header::CONTENT_RANGE, v);
                }
                return resp;
            }
            Err(RangeError::Malformed) => {
                started.cancel();
                return json_error(StatusCode::BAD_REQUEST, "malformed Range header");
            }
        },
    };

    // No Range, or Range covers the whole object: serve the in-flight
    // full-stream response we already started.
    if parsed_range.is_none() {
        let content_type = sniff_content_type(&started.first_bytes, query.filename());
        let filename = raw_bytes_filename(&addr, query.filename(), content_type);
        return streaming_response(
            head_only,
            total,
            started.into_body(head_only),
            content_type,
            Some(&filename),
        );
    }

    // Range request: cancel the full-body stream we just started and
    // dispatch a fresh range-scoped one. Cancellation is a drop on the
    // ack receiver; the daemon notices the closed channel and stops
    // joining further subtrees. Move the activity guard from the
    // cancelled `Started` into the new dispatch so the registry slot
    // stays alive across the swap (otherwise `antop` would see
    // the row blink out and back in for every range request).
    let carry_guard = started.take_activity_guard();
    started.cancel();
    let (start, end) = parsed_range.expect("parsed_range was just checked");
    let stream_range = StreamRange {
        start,
        end_inclusive: end,
    };
    let ranged = match dispatch_stream_bytes(
        &handle,
        reference,
        timeout,
        Some(stream_range),
        head_only,
        carry_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };
    let sniffable_total = ranged.total_bytes;
    // Sniffing on a tail range would mis-detect an MP4 as octet-stream.
    // Use the filename hint when it's a partial response and the leading
    // bytes don't start at offset 0.
    let content_type = if start == 0 {
        sniff_content_type(&ranged.first_bytes, query.filename())
    } else {
        query
            .filename()
            .and_then(content_type_from_extension)
            .unwrap_or("application/octet-stream")
    };
    let filename = raw_bytes_filename(&addr, query.filename(), content_type);
    let body_len = end - start + 1;
    partial_content_response(
        head_only,
        sniffable_total,
        start,
        end,
        body_len,
        ranged.into_body(head_only),
        content_type,
        Some(&filename),
    )
}

/// `GET /bzz/{addr}` and `HEAD /bzz/{addr}`. Empty path resolves to
/// the manifest's `website-index-document` metadata; our `lookup_path`
/// already implements that fallback.
pub async fn bzz_root(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    bzz_inner(handle, addr, String::new(), method, headers).await
}

/// `GET /bzz/{addr}/{*path}` and the matching HEAD. Walks the
/// manifest, joins the data tree, returns bytes. Content-Type comes
/// from the manifest's per-file metadata when present; otherwise
/// defaults to `application/octet-stream` (bee does the same).
pub async fn bzz_with_path(
    State(handle): State<GatewayHandle>,
    Path((addr, path)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    bzz_inner(handle, addr, path, method, headers).await
}

/// `GET /v0/manifest/{addr}`. Lists the paths and metadata currently
/// discoverable in a mantaray manifest. This is an Ant-specific
/// extension — bee does not expose manifest enumeration on its public
/// HTTP API — and is namespaced under `/v0/` to keep the bee Tier-A
/// surface byte-for-byte compatible.
pub async fn manifest(State(handle): State<GatewayHandle>, Path(addr): Path<String>) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, e),
    };

    let _guard = handle
        .activity
        .begin(GatewayRequestKind::Manifest, short_reference(&addr));

    match dispatch_list_bzz(&handle, reference, DEFAULT_REQUEST_TIMEOUT).await {
        Ok(entries) => {
            // Manifest listing keys off the same content-addressed
            // root the user passed in, so the listing itself is
            // immutable. Cache it like a `/bzz` payload to avoid
            // re-walking the manifest on every page navigation.
            let mut resp = json_response(StatusCode::OK, &ManifestListing { entries });
            set_immutable_cache_headers(&mut resp);
            resp
        }
        Err(e) => e,
    }
}

async fn bzz_inner(
    handle: GatewayHandle,
    addr: String,
    path: String,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, e),
    };

    let raw_range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let head_only = method == Method::HEAD;

    let activity_label = if path.is_empty() {
        short_reference(&addr)
    } else {
        format!("{}/{path}", short_reference(&addr))
    };
    let activity_guard = Some(
        handle
            .activity
            .begin(GatewayRequestKind::Bzz, activity_label),
    );

    let mut started = match dispatch_stream_bzz(
        &handle,
        reference,
        path.clone(),
        timeout,
        None,
        head_only,
        activity_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };

    let total = started.total_bytes;
    let parsed_range = match raw_range.as_deref() {
        None => None,
        Some(raw) => match parse_single_range(raw, total) {
            Ok(r) => r,
            Err(RangeError::Multi) => {
                started.cancel();
                return json_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "multi-range requests not supported",
                );
            }
            Err(RangeError::Unsatisfiable) => {
                started.cancel();
                let mut resp = json_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "range not satisfiable for this resource",
                );
                let cr = format!("bytes */{total}");
                if let Ok(v) = HeaderValue::from_str(&cr) {
                    resp.headers_mut().insert(header::CONTENT_RANGE, v);
                }
                return resp;
            }
            Err(RangeError::Malformed) => {
                started.cancel();
                return json_error(StatusCode::BAD_REQUEST, "malformed Range header");
            }
        },
    };

    let content_type = started
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let filename = started.filename.clone();

    if parsed_range.is_none() {
        return streaming_response(
            head_only,
            total,
            started.into_body(head_only),
            &content_type,
            filename.as_deref(),
        );
    }

    let carry_guard = started.take_activity_guard();
    started.cancel();
    let (start, end) = parsed_range.expect("parsed_range was just checked");
    let stream_range = StreamRange {
        start,
        end_inclusive: end,
    };
    let ranged = match dispatch_stream_bzz(
        &handle,
        reference,
        path,
        timeout,
        Some(stream_range),
        head_only,
        carry_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };
    let body_len = end - start + 1;
    partial_content_response(
        head_only,
        total,
        start,
        end,
        body_len,
        ranged.into_body(head_only),
        &content_type,
        filename.as_deref(),
    )
}

#[derive(Debug, Serialize)]
struct ManifestListing {
    entries: Vec<ant_control::ManifestEntryInfo>,
}

#[derive(Debug, Default, Deserialize)]
pub struct BytesQuery {
    #[serde(default, alias = "name")]
    filename: Option<String>,
}

impl BytesQuery {
    fn filename(&self) -> Option<&str> {
        self.filename
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

/// `?name=<filename>` for `POST /bzz` single-file mode. Bee accepts the
/// same query parameter on its `/bzz` upload endpoint to override the
/// manifest entry name without requiring a multipart body.
#[derive(Debug, Default, Deserialize)]
pub struct UploadBzzQuery {
    #[serde(default)]
    name: Option<String>,
}

impl UploadBzzQuery {
    fn name(&self) -> Option<&str> {
        self.name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

/// `?sig=<65-byte hex>` for `POST /soc/{owner}/{id}`. Bee-js ≥ 10
/// (matching modern bee) sends the SOC signature here instead of in
/// the `swarm-soc-signature` header. `upload_soc` accepts either form.
#[derive(Debug, Default, Deserialize)]
pub struct UploadSocQuery {
    #[serde(default)]
    sig: Option<String>,
}

/// Result of dispatching a streaming retrieval against the node loop:
/// the prologue ack (size + optional manifest metadata) plus the live
/// receiver still draining body chunks. The gateway sends headers as
/// soon as it has this struct in hand and, for non-HEAD GETs, drains
/// the receiver into the HTTP response body via `Body::from_stream`.
///
/// `cancel()` drops the receiver so the daemon notices the closed
/// channel and stops fetching further subtrees. We use that when a
/// `Range` request comes in: we issue an unranged dispatch first to
/// learn `total_bytes`, then re-dispatch with a `StreamRange` once
/// the range parses. Cheap because the daemon's joiner has barely
/// started by the time the gateway has parsed the range.
struct Started {
    total_bytes: u64,
    first_bytes: Vec<u8>,
    /// Manifest metadata: only populated for `StreamBzz`. `None` for
    /// `StreamBytes` since the raw chunk tree carries no content type
    /// or filename.
    content_type: Option<String>,
    filename: Option<String>,
    first_chunk: Option<Vec<u8>>,
    rx: mpsc::Receiver<ControlAck>,
    /// RAII guard for the gateway-activity registry slot. Lives for
    /// the duration of the HTTP exchange: handed off into the
    /// streaming-body unfold state on `into_body` so the slot stays
    /// alive while the response body drains, and dropped explicitly
    /// in `cancel()` when we redispatch a fresh `Started` for a
    /// range request (the new `Started` brings its own guard).
    activity_guard: Option<ActiveRequestGuard>,
}

impl Started {
    fn cancel(&mut self) {
        self.rx.close();
    }

    /// Extract the activity guard so the caller can carry the
    /// registry slot across a cancel-and-redispatch (Range parsing
    /// path). The redispatched `Started` brings its own `cancel`
    /// semantics; reusing the existing guard keeps the entry stable
    /// across the brief window where two streams are active.
    const fn take_activity_guard(&mut self) -> Option<ActiveRequestGuard> {
        self.activity_guard.take()
    }

    /// Hand back the response body. For `HEAD` we want an empty body
    /// regardless of what the daemon sent; for `GET` we wrap the live
    /// receiver in a stream that pulls each `BytesChunk` out as a
    /// `Bytes` slice. Consumes `self` because the caller can either
    /// drain the body or cancel — never both.
    fn into_body(self, head_only: bool) -> Body {
        if head_only {
            self.rx.close_now();
            return Body::empty();
        }
        let Self {
            first_chunk,
            rx,
            activity_guard,
            ..
        } = self;
        let activity = activity_guard
            .as_ref()
            .map(ant_control::ActiveRequestGuard::handle);
        let body_stream = stream::unfold(
            (first_chunk, rx, activity, activity_guard),
            |(mut first, mut rx, activity, guard)| async move {
                if let Some(data) = first.take() {
                    return Some((
                        Ok::<Bytes, std::io::Error>(Bytes::from(data)),
                        (None, rx, activity, guard),
                    ));
                }
                loop {
                    match rx.recv().await {
                        Some(ControlAck::BytesChunk { data }) => {
                            return Some((
                                Ok::<Bytes, std::io::Error>(Bytes::from(data)),
                                (None, rx, activity, guard),
                            ));
                        }
                        Some(ControlAck::StreamDone) | None => return None,
                        Some(ControlAck::Progress(p)) => {
                            if let Some(h) = activity.as_ref() {
                                h.update(
                                    p.chunks_done,
                                    p.total_chunks_estimate,
                                    p.in_flight,
                                    p.bytes_done,
                                );
                            }
                        }
                        Some(ControlAck::Error { message }) => {
                            return Some((
                                Err(std::io::Error::other(message)),
                                (None, rx, activity, guard),
                            ));
                        }
                        Some(other) => {
                            warn!(
                                target: "ant_gateway",
                                ?other,
                                "unexpected ack during streaming body"
                            );
                            return Some((
                                Err(std::io::Error::other("unexpected node ack")),
                                (None, rx, activity, guard),
                            ));
                        }
                    }
                }
            },
        );
        Body::from_stream(body_stream)
    }
}

/// Helper trait so we can `close_now()` a receiver synchronously after
/// we've consumed `self`. `Receiver::close()` is async-cancellable but
/// `close()` itself is sync — we just want it shut so the daemon's
/// `send().await` returns Err and the spawned task tears down.
trait ReceiverClose {
    fn close_now(self);
}

impl ReceiverClose for mpsc::Receiver<ControlAck> {
    fn close_now(mut self) {
        self.close();
    }
}

// `Err(Response)` is the routine "early-return an error response" path —
// boxing it would add a heap alloc to the failure case for no benefit.
#[allow(clippy::result_large_err)]
async fn dispatch_stream_bytes(
    handle: &GatewayHandle,
    reference: [u8; 32],
    timeout: Duration,
    range: Option<StreamRange>,
    head_only: bool,
    activity_guard: Option<ActiveRequestGuard>,
) -> Result<Started, Response> {
    let (ack_tx, ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::StreamBytes {
        reference,
        bypass_cache: false,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        range,
        head_only,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    consume_stream_prologue(ack_rx, timeout, false, head_only, activity_guard).await
}

#[allow(clippy::result_large_err)]
async fn dispatch_stream_bzz(
    handle: &GatewayHandle,
    reference: [u8; 32],
    path: String,
    timeout: Duration,
    range: Option<StreamRange>,
    head_only: bool,
    activity_guard: Option<ActiveRequestGuard>,
) -> Result<Started, Response> {
    let (ack_tx, ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::StreamBzz {
        reference,
        path,
        // The plan calls out that mainnet bzz roots commonly carry a
        // redundancy level byte we don't yet RS-decode; bee's reader
        // strips it and serves anyway. We follow that — the joiner
        // still fails fast if a data chunk is actually missing.
        allow_degraded_redundancy: true,
        bypass_cache: false,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        range,
        head_only,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    consume_stream_prologue(ack_rx, timeout, true, head_only, activity_guard).await
}

/// Walk the prologue of a `StreamBytes` / `StreamBzz` response: forward
/// `Progress` samples to the activity registry, accept the appropriate
/// `*StreamStart` ack, then peek the first body chunk so the caller can
/// sniff content type and hand the rest off to hyper as a stream. For
/// `head_only = true` we also accept a terminal `StreamDone`
/// immediately after the prologue — no body to peek.
#[allow(clippy::result_large_err)]
async fn consume_stream_prologue(
    mut rx: mpsc::Receiver<ControlAck>,
    timeout: Duration,
    expect_bzz: bool,
    head_only: bool,
    activity_guard: Option<ActiveRequestGuard>,
) -> Result<Started, Response> {
    let activity_handle = activity_guard
        .as_ref()
        .map(ant_control::ActiveRequestGuard::handle);
    let total_bytes;
    let mut content_type = None;
    let mut filename = None;
    loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ControlAck::BytesStreamStart { total_bytes: t })) if !expect_bzz => {
                total_bytes = t;
                break;
            }
            Ok(Some(ControlAck::BzzStreamStart {
                total_bytes: t,
                content_type: ct,
                filename: fn_,
            })) if expect_bzz => {
                total_bytes = t;
                content_type = ct;
                filename = fn_;
                break;
            }
            Ok(Some(ControlAck::Progress(p))) => {
                if let Some(h) = activity_handle.as_ref() {
                    h.update(
                        p.chunks_done,
                        p.total_chunks_estimate,
                        p.in_flight,
                        p.bytes_done,
                    );
                }
            }
            Ok(Some(ControlAck::Error { message })) => {
                return Err(map_retrieval_error(message));
            }
            Ok(Some(other)) => {
                warn!(target: "ant_gateway", ?other, expect_bzz, "unexpected ack before stream start");
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ));
            }
            Ok(None) => return Err(node_unavailable()),
            Err(_) => {
                return Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                ));
            }
        }
    }

    if head_only {
        return Ok(Started {
            total_bytes,
            first_bytes: Vec::new(),
            content_type,
            filename,
            first_chunk: None,
            rx,
            activity_guard,
        });
    }

    let first_chunk = loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ControlAck::BytesChunk { data })) => break Some(data),
            Ok(Some(ControlAck::StreamDone)) => break None,
            Ok(Some(ControlAck::Progress(p))) => {
                if let Some(h) = activity_handle.as_ref() {
                    h.update(
                        p.chunks_done,
                        p.total_chunks_estimate,
                        p.in_flight,
                        p.bytes_done,
                    );
                }
            }
            Ok(Some(ControlAck::Error { message })) => {
                return Err(map_retrieval_error(message));
            }
            Ok(Some(other)) => {
                warn!(target: "ant_gateway", ?other, "unexpected ack before first body chunk");
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ));
            }
            Ok(None) => return Err(node_unavailable()),
            Err(_) => {
                return Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                ));
            }
        }
    };
    let first_bytes = first_chunk
        .as_ref()
        .map(|b| b[..b.len().min(512)].to_vec())
        .unwrap_or_default();

    Ok(Started {
        total_bytes,
        first_bytes,
        content_type,
        filename,
        first_chunk,
        rx,
        activity_guard,
    })
}

/// Map a daemon-reported retrieval error string into the right HTTP
/// status. `"not found"` (joiner's `FetchChunk` wrapping a "missing
/// chunk" remote) and manifest-lookup misses (`'path'`) become 404; the
/// rest are bad-gateway conditions.
fn map_retrieval_error(message: String) -> Response {
    let status = if message.contains("not found") || message.contains('\'') {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_GATEWAY
    };
    json_error(status, message)
}

#[allow(clippy::result_large_err)]
async fn dispatch_list_bzz(
    handle: &GatewayHandle,
    reference: [u8; 32],
    timeout: Duration,
) -> Result<Vec<ant_control::ManifestEntryInfo>, Response> {
    let (ack_tx, mut ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::ListBzz {
        reference,
        bypass_cache: false,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    drain_terminal(&mut ack_rx, timeout)
        .await
        .and_then(|ack| match ack {
            ControlAck::Manifest { entries } => Ok(entries),
            ControlAck::Error { message } => Err(json_error(StatusCode::NOT_FOUND, message)),
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from ListBzz");
                Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ))
            }
        })
}

/// Walk the streaming ack channel, dropping `Progress` samples until a
/// terminal variant lands. Mirrors `ant-control::server::dispatch_streaming`
/// but without rewriting each ack to a JSON wire response — we only
/// care about the terminal payload.
#[allow(clippy::result_large_err)]
async fn drain_terminal(
    rx: &mut mpsc::Receiver<ControlAck>,
    timeout: Duration,
) -> Result<ControlAck, Response> {
    loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ControlAck::Progress(_))) => {}
            Ok(Some(ack)) => return Ok(ack),
            Ok(None) => return Err(node_unavailable()),
            Err(_) => {
                return Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                ));
            }
        }
    }
}

fn parse_reference(s: &str) -> Result<[u8; 32], String> {
    parse_hex_fixed::<32>(s).map_err(|e| format!("reference {e}"))
}

fn request_timeout(headers: &HeaderMap, default: Duration) -> Duration {
    headers
        .get(&SWARM_CHUNK_RETRIEVAL_TIMEOUT)
        .and_then(|v| v.to_str().ok())
        .and_then(humantime::parse_duration_lite)
        .unwrap_or(default)
}

/// Build a `200 OK` (or empty `200 OK` for `HEAD`) response for a full
/// streaming GET. `body` should already be `Body::empty()` when
/// `head_only`; we set headers identically either way so HEAD and GET
/// agree on `Content-Length` / `Accept-Ranges` / `Content-Disposition`.
fn streaming_response(
    head_only: bool,
    total_bytes: u64,
    body: Body,
    content_type: &str,
    filename: Option<&str>,
) -> Response {
    let body = if head_only { Body::empty() } else { body };
    let mut resp = Response::new(body);
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(total_bytes));
    let _ = resp
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
        }
    }
    set_immutable_cache_headers(&mut resp);
    resp
}

/// Build a `206 Partial Content` (or empty `206 Partial Content` for
/// `HEAD`) response with the canonical bee-shape headers: `Content-Range`,
/// `Content-Length` (the slice length, not the total), `Accept-Ranges`,
/// and `Content-Disposition` when a filename is known. `body` should
/// already only contain the requested byte interval — the streaming
/// joiner emits exactly those bytes on a range dispatch.
#[allow(clippy::too_many_arguments)]
fn partial_content_response(
    head_only: bool,
    total_bytes: u64,
    start: u64,
    end_inclusive: u64,
    body_len: u64,
    body: Body,
    content_type: &str,
    filename: Option<&str>,
) -> Response {
    let body = if head_only { Body::empty() } else { body };
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(body_len));
    let _ = resp
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    let cr = format!("bytes {start}-{end_inclusive}/{total_bytes}");
    if let Ok(v) = HeaderValue::from_str(&cr) {
        resp.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
        }
    }
    set_immutable_cache_headers(&mut resp);
    resp
}

fn content_disposition(filename: &str) -> Option<HeaderValue> {
    let filename = sanitize_filename(filename);
    HeaderValue::from_str(&format!("inline; filename=\"{filename}\"")).ok()
}

fn sanitize_filename(filename: &str) -> String {
    let mut out = String::with_capacity(filename.len().min(128));
    for ch in filename.chars() {
        let safe = match ch {
            '"' | '\'' | '\\' | '/' | ':' | '<' | '>' | '|' | '?' | '*' => '_',
            c if c.is_control() => '_',
            c => c,
        };
        out.push(safe);
        if out.len() >= 128 {
            break;
        }
    }
    let trimmed = out.trim_matches([' ', '.']).trim();
    if trimmed.is_empty() {
        "download.bin".to_string()
    } else {
        trimmed.to_string()
    }
}

fn raw_bytes_filename(hash: &str, requested: Option<&str>, content_type: &str) -> String {
    if let Some(name) = requested {
        return sanitize_filename(name);
    }
    let hash = hash
        .strip_prefix("0x")
        .or_else(|| hash.strip_prefix("0X"))
        .unwrap_or(hash);
    let ext = extension_for_content_type(content_type).unwrap_or("bin");
    format!("{hash}.{ext}")
}

fn sniff_content_type(bytes: &[u8], filename: Option<&str>) -> &'static str {
    if let Some(name) = filename {
        if let Some(content_type) = content_type_from_extension(name) {
            return content_type;
        }
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return "image/png";
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return "image/jpeg";
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif";
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return "audio/wav";
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return "image/webp";
    }
    if bytes.starts_with(b"%PDF-") {
        return "application/pdf";
    }
    if bytes.starts_with(b"ID3")
        || bytes.starts_with(&[0xff, 0xfb])
        || bytes.starts_with(&[0xff, 0xf3])
    {
        return "audio/mpeg";
    }
    if bytes.starts_with(b"OggS") {
        return "application/ogg";
    }
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        return "video/mp4";
    }
    "application/octet-stream"
}

fn content_type_from_extension(filename: &str) -> Option<&'static str> {
    let ext = filename.rsplit('.').next()?.to_ascii_lowercase();
    match ext.as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "svg" => Some("image/svg+xml"),
        "wav" => Some("audio/wav"),
        "mp3" => Some("audio/mpeg"),
        "ogg" | "oga" => Some("audio/ogg"),
        "mp4" | "m4v" => Some("video/mp4"),
        "webm" => Some("video/webm"),
        "pdf" => Some("application/pdf"),
        "txt" => Some("text/plain; charset=utf-8"),
        "html" | "htm" => Some("text/html; charset=utf-8"),
        "map" | "json" => Some("application/json"),
        "js" | "mjs" => Some("text/javascript; charset=utf-8"),
        "css" => Some("text/css; charset=utf-8"),
        "wasm" => Some("application/wasm"),
        "woff" => Some("font/woff"),
        "woff2" => Some("font/woff2"),
        "ico" => Some("image/x-icon"),
        "xml" => Some("application/xml; charset=utf-8"),
        _ => None,
    }
}

fn extension_for_content_type(content_type: &str) -> Option<&'static str> {
    match content_type.split(';').next().unwrap_or(content_type) {
        "image/png" => Some("png"),
        "image/jpeg" => Some("jpg"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "image/svg+xml" => Some("svg"),
        "audio/wav" => Some("wav"),
        "audio/mpeg" => Some("mp3"),
        "audio/ogg" => Some("ogg"),
        "video/mp4" => Some("mp4"),
        "video/webm" => Some("webm"),
        "application/pdf" => Some("pdf"),
        "text/plain" => Some("txt"),
        "text/html" => Some("html"),
        "application/json" => Some("json"),
        "text/javascript" | "application/javascript" => Some("js"),
        "text/css" => Some("css"),
        "application/wasm" => Some("wasm"),
        "font/woff" => Some("woff"),
        "font/woff2" => Some("woff2"),
        "image/x-icon" => Some("ico"),
        "application/xml" | "text/xml" => Some("xml"),
        _ => None,
    }
}

#[derive(Debug)]
enum RangeError {
    Multi,
    Unsatisfiable,
    Malformed,
}

/// Parse a `Range: bytes=START-END` header.
///
/// Returns `Ok(None)` when the header is structurally a `bytes=...`
/// range but covers the whole resource (no narrowing) or is missing the
/// `bytes=` unit (RFC 9110 §14.2 says we "must ignore" unknown units).
/// `Ok(Some((start, end)))` is inclusive on both ends, ready for slice
/// indexing.
fn parse_single_range(raw: &str, total: u64) -> Result<Option<(u64, u64)>, RangeError> {
    let raw = raw.trim();
    let Some(rest) = raw.strip_prefix("bytes=") else {
        return Ok(None);
    };
    if rest.contains(',') {
        return Err(RangeError::Multi);
    }
    let (start_s, end_s) = rest.split_once('-').ok_or(RangeError::Malformed)?;
    if start_s.is_empty() {
        // `bytes=-N` → suffix range, last N bytes.
        let n: u64 = end_s.parse().map_err(|_| RangeError::Malformed)?;
        if n == 0 || total == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        let n = n.min(total);
        return Ok(Some((total - n, total - 1)));
    }
    let start: u64 = start_s.parse().map_err(|_| RangeError::Malformed)?;
    let end: u64 = if end_s.is_empty() {
        if total == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        total - 1
    } else {
        end_s.parse().map_err(|_| RangeError::Malformed)?
    };
    if start > end || start >= total {
        return Err(RangeError::Unsatisfiable);
    }
    let end = end.min(total - 1);
    Ok(Some((start, end)))
}

fn node_unavailable() -> Response {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "node loop is no longer accepting commands",
    )
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response {
    let body = serde_json::to_vec(value).expect("serialize json response");
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = status;
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    resp
}

/// `swarm-feed-index` HTTP response header carrying the resolved
/// update's sequence index as bee marshals it: the 8-byte big-endian
/// `u64`, hex-encoded to 16 lowercase chars (e.g. `0000000000000002`).
/// Matches `cur.MarshalBinary()` in `bee/pkg/api/feed.go::feedGetHandler`
/// and `bee/pkg/feeds/sequence/sequence.go::index::MarshalBinary`.
const SWARM_FEED_INDEX: HeaderName = HeaderName::from_static("swarm-feed-index");

/// `swarm-feed-index-next` — the next possible update index
/// (`index + 1`), same 8-byte big-endian hex shape. bee-js uses it to
/// address the next write without rescanning the feed.
const SWARM_FEED_INDEX_NEXT: HeaderName = HeaderName::from_static("swarm-feed-index-next");

/// `swarm-feed-resolved-version` — `"v1"` or `"v2"`. We only resolve
/// the legacy v1 payload layout, so this is always `v1`.
const SWARM_FEED_RESOLVED_VERSION: HeaderName =
    HeaderName::from_static("swarm-feed-resolved-version");

/// `?type=` query parameter on `GET /feeds/{owner}/{topic}`. Default is
/// `sequence`. Epoch feeds aren't implemented; the gateway returns
/// `501` for them so client probes can distinguish "feed type not
/// supported" from "feed not found".
#[derive(Debug, Deserialize, Default)]
pub struct FeedQuery {
    #[serde(default)]
    r#type: Option<String>,
}

/// `GET /feeds/{owner}/{topic}` — resolve a sequence feed to its latest
/// update and serve the content it points at, byte-for-byte like bee's
/// `feedGetHandler`:
///
/// - The body is the **dereferenced content** at the update's wrapped
///   reference (the joined chunk tree), served as
///   `application/octet-stream`. With `Swarm-Only-Root-Chunk: true` we
///   instead return that reference's root chunk verbatim
///   (`span(8 LE) || payload`), matching bee's only-root-chunk branch.
/// - `swarm-feed-index` carries the resolved index as 8-byte big-endian
///   hex (`cur.MarshalBinary()`); `swarm-feed-index-next` is `index + 1`;
///   `swarm-soc-signature` is the update chunk's 65-byte signature (hex);
///   `swarm-feed-resolved-version` is `v1`.
/// - `Cache-Control: no-cache` because feed contents are mutable, and an
///   `ETag` of the resolved content reference is set so conditional
///   re-fetches short-circuit.
/// - Range requests are honored (`206 Partial Content`); `HEAD` returns
///   the headers with an empty body.
pub async fn download_feed(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, topic_hex)): Path<(String, String)>,
    Query(query): Query<FeedQuery>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let owner = match parse_hex_fixed::<20>(&owner_hex) {
        Ok(o) => o,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad owner: {e}")),
    };
    let topic = match parse_hex_fixed::<32>(&topic_hex) {
        Ok(t) => t,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("bad topic: {e}")),
    };

    if let Some(kind) = query.r#type.as_deref() {
        match kind.to_ascii_lowercase().as_str() {
            "sequence" => {}
            "epoch" => {
                return json_error(StatusCode::NOT_IMPLEMENTED, "epoch feeds are not supported");
            }
            other => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("unknown feed type: {other}"),
                );
            }
        }
    }

    let guard = handle.activity.begin(
        GatewayRequestKind::Feed,
        short_reference(&hex::encode(topic)),
    );

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::GetFeed {
        owner,
        topic,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("feed lookup timed out after {}s", timeout.as_secs()),
            );
        }
    };

    let (reference, index, signature, v2, decrypt_key) = match ack {
        ControlAck::FeedResolved {
            reference,
            index,
            signature,
            v2,
            decrypt_key,
        } => (reference, index, signature, v2, decrypt_key),
        ControlAck::FeedNotFound => {
            return json_error(StatusCode::NOT_FOUND, "no feed updates found");
        }
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        ControlAck::Error { message } => {
            // Detailed retrieval errors (peer addresses, internal
            // failure modes) belong in the operator log, not in the
            // public response body. Match the bee feed handler, which
            // returns a generic "feed lookup failed" body.
            warn!(target: "ant_gateway", %message, "feed lookup failed");
            return json_error(StatusCode::BAD_GATEWAY, "feed lookup failed");
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetFeed");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };

    let head_only = method == Method::HEAD;

    // Encrypted feed content (bee's 80-byte v1 payload): the node joins
    // and decrypts `reference ‖ key` into a buffer (the payload is small).
    // We serve it in one shot — applying any Range by slicing — rather
    // than through the streaming joiner, which only handles plaintext.
    if let Some(key) = decrypt_key {
        let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(1);
        let cmd = ControlCommand::GetBytesEncrypted {
            reference,
            key,
            bypass_cache: false,
            max_bytes: None,
            ack: ack_tx,
        };
        if handle.commands.send(cmd).await.is_err() {
            return node_unavailable();
        }
        let ack = match tokio::time::timeout(timeout, ack_rx.recv()).await {
            Ok(Some(ack)) => ack,
            Ok(None) => return node_unavailable(),
            Err(_) => {
                return json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                );
            }
        };
        let data = match ack {
            ControlAck::Bytes { data } => data,
            ControlAck::NotReady { message } => {
                return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
            }
            ControlAck::Error { message } => {
                warn!(target: "ant_gateway", %message, "encrypted feed content fetch failed");
                return json_error(StatusCode::BAD_GATEWAY, "feed content cannot be retrieved");
            }
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from GetBytesEncrypted");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
            }
        };
        let total = data.len() as u64;
        guard.update(1, 1, 0, total);

        let raw_range = headers
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let parsed_range = match raw_range.as_deref() {
            None => None,
            Some(raw) => match parse_single_range(raw, total) {
                Ok(r) => r,
                Err(RangeError::Multi) => {
                    return json_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "multi-range requests not supported",
                    );
                }
                Err(RangeError::Unsatisfiable) => {
                    let mut resp = json_error(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "range not satisfiable for this resource",
                    );
                    if let Ok(v) = HeaderValue::from_str(&format!("bytes */{total}")) {
                        resp.headers_mut().insert(header::CONTENT_RANGE, v);
                    }
                    return resp;
                }
                Err(RangeError::Malformed) => {
                    return json_error(StatusCode::BAD_REQUEST, "malformed Range header");
                }
            },
        };

        let mut resp = match parsed_range {
            None => {
                let len = data.len();
                let body = if head_only {
                    Body::empty()
                } else {
                    Body::from(data)
                };
                let mut resp = Response::new(body);
                let _ = resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                let _ = resp
                    .headers_mut()
                    .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
                resp
            }
            Some((start, end)) => {
                let slice = if head_only {
                    Vec::new()
                } else {
                    data[start as usize..=end as usize].to_vec()
                };
                let body_len = end - start + 1;
                let body = if head_only {
                    Body::empty()
                } else {
                    Body::from(slice)
                };
                let mut resp = Response::new(body);
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                let _ = resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                let _ = resp
                    .headers_mut()
                    .insert(header::CONTENT_LENGTH, HeaderValue::from(body_len));
                if let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")) {
                    resp.headers_mut().insert(header::CONTENT_RANGE, v);
                }
                resp
            }
        };
        apply_feed_headers(&mut resp, reference, index, &signature, v2);
        return resp;
    }

    // `Swarm-Only-Root-Chunk: true` — return the resolved reference's
    // root chunk verbatim (`span(8 LE) || payload`) without joining the
    // whole tree. Bee writes `wc.Data()` here; we fetch the raw chunk
    // at `reference` via the same path `/chunks/{addr}` uses.
    if header_is_true(&headers, &SWARM_ONLY_ROOT_CHUNK) {
        let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
        let cmd = ControlCommand::GetChunkRaw {
            reference,
            ack: ack_tx,
        };
        if handle.commands.send(cmd).await.is_err() {
            return node_unavailable();
        }
        let ack = match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(ack)) => ack,
            Ok(Err(_)) => return node_unavailable(),
            Err(_) => {
                return json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                );
            }
        };
        let data = match ack {
            ControlAck::Bytes { data } => data,
            ControlAck::NotReady { message } => {
                return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
            }
            ControlAck::Error { message } => {
                warn!(target: "ant_gateway", %message, "feed root chunk fetch failed");
                return json_error(StatusCode::NOT_FOUND, "wrapped chunk cannot be retrieved");
            }
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw (feed)");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
            }
        };
        guard.update(1, 1, 0, data.len() as u64);
        let len = data.len();
        let body = if head_only {
            Body::empty()
        } else {
            Body::from(data)
        };
        let mut resp = Response::new(body);
        let _ = resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let _ = resp
            .headers_mut()
            .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
        apply_feed_headers(&mut resp, reference, index, &signature, v2);
        return resp;
    }

    // Default path: dereference `reference` and stream its content, the
    // same join `/bytes/{addr}` performs. Reuse the streaming dispatch
    // (with Range support) and then overlay the bee feed headers and a
    // forced `application/octet-stream` content type.
    let raw_range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let stream_timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);

    let mut started = match dispatch_stream_bytes(
        &handle,
        reference,
        stream_timeout,
        None,
        head_only,
        Some(guard),
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };

    let total = started.total_bytes;
    let parsed_range = match raw_range.as_deref() {
        None => None,
        Some(raw) => match parse_single_range(raw, total) {
            Ok(r) => r,
            Err(RangeError::Multi) => {
                started.cancel();
                return json_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "multi-range requests not supported",
                );
            }
            Err(RangeError::Unsatisfiable) => {
                started.cancel();
                let mut resp = json_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "range not satisfiable for this resource",
                );
                let cr = format!("bytes */{total}");
                if let Ok(v) = HeaderValue::from_str(&cr) {
                    resp.headers_mut().insert(header::CONTENT_RANGE, v);
                }
                return resp;
            }
            Err(RangeError::Malformed) => {
                started.cancel();
                return json_error(StatusCode::BAD_REQUEST, "malformed Range header");
            }
        },
    };

    if parsed_range.is_none() {
        let mut resp = streaming_response(
            head_only,
            total,
            started.into_body(head_only),
            "application/octet-stream",
            None,
        );
        apply_feed_headers(&mut resp, reference, index, &signature, v2);
        return resp;
    }

    let carry_guard = started.take_activity_guard();
    started.cancel();
    let (start, end) = parsed_range.expect("parsed_range was just checked");
    let stream_range = StreamRange {
        start,
        end_inclusive: end,
    };
    let ranged = match dispatch_stream_bytes(
        &handle,
        reference,
        stream_timeout,
        Some(stream_range),
        head_only,
        carry_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };
    let sniffable_total = ranged.total_bytes;
    let body_len = end - start + 1;
    let mut resp = partial_content_response(
        head_only,
        sniffable_total,
        start,
        end,
        body_len,
        ranged.into_body(head_only),
        "application/octet-stream",
        None,
    );
    apply_feed_headers(&mut resp, reference, index, &signature, v2);
    resp
}

/// Overlay the bee-shaped feed response headers onto a response built by
/// the shared byte-streaming helpers. Sets `swarm-feed-index` /
/// `swarm-feed-index-next` (8-byte big-endian hex of `index` and
/// `index + 1`), `swarm-soc-signature` (hex of the 65-byte signature),
/// `swarm-feed-resolved-version` (`v1` or `v2` per `v2`), an `ETag` of
/// the resolved content reference, and exposes the feed headers via
/// `Access-Control-Expose-Headers`. Forces `Cache-Control: no-cache`
/// because feed contents are mutable (overriding the immutable cache the
/// byte-stream helpers stamp).
fn apply_feed_headers(
    resp: &mut Response,
    reference: [u8; 32],
    index: u64,
    signature: &[u8; 65],
    v2: bool,
) {
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&hex::encode(index.to_be_bytes())) {
        h.insert(SWARM_FEED_INDEX, v);
    }
    if let Ok(v) = HeaderValue::from_str(&hex::encode(index.saturating_add(1).to_be_bytes())) {
        h.insert(SWARM_FEED_INDEX_NEXT, v);
    }
    if let Ok(v) = HeaderValue::from_str(&hex::encode(signature)) {
        h.insert(SWARM_SOC_SIGNATURE, v);
    }
    h.insert(
        SWARM_FEED_RESOLVED_VERSION,
        HeaderValue::from_static(if v2 { "v2" } else { "v1" }),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", hex::encode(reference))) {
        h.insert(header::ETAG, v);
    }
    h.insert(header::CACHE_CONTROL, MUTABLE_CACHE_CONTROL);
    for name in [SWARM_FEED_INDEX, SWARM_FEED_INDEX_NEXT, SWARM_SOC_SIGNATURE] {
        if let Ok(v) = HeaderValue::from_str(name.as_str()) {
            h.append(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
        }
    }
}

/// Parse a bee-style boolean request header (`Swarm-Only-Root-Chunk`,
/// etc.). Accepts the values Go's `strconv.ParseBool` treats as true
/// that clients actually send: `1`, `t`, `true` (any case).
fn header_is_true(headers: &HeaderMap, name: &HeaderName) -> bool {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .is_some_and(|s| s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("t") || s == "1")
}

/// Tiny `humantime`-shaped duration parser so we don't pull in a whole
/// crate for one optional header. Accepts plain integers (interpreted
/// as seconds, matching bee) and `<n>ms`, `<n>s`, `<n>m` suffixes.
mod humantime {
    use std::time::Duration;

    pub fn parse_duration_lite(s: &str) -> Option<Duration> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if let Ok(secs) = s.parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }
        for (suffix, factor_ms) in [("ms", 1u64), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)] {
            if let Some(num) = s.strip_suffix(suffix) {
                if let Ok(n) = num.trim().parse::<u64>() {
                    return Some(Duration::from_millis(n * factor_ms));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_range() {
        assert_eq!(parse_single_range("bytes=0-9", 1024).unwrap(), Some((0, 9)));
    }

    #[test]
    fn parses_open_ended() {
        assert_eq!(
            parse_single_range("bytes=10-", 1024).unwrap(),
            Some((10, 1023))
        );
    }

    #[test]
    fn parses_suffix() {
        assert_eq!(
            parse_single_range("bytes=-100", 1024).unwrap(),
            Some((924, 1023))
        );
    }

    #[test]
    fn rejects_multi_range() {
        assert!(matches!(
            parse_single_range("bytes=0-10,20-30", 1024),
            Err(RangeError::Multi)
        ));
    }

    #[test]
    fn rejects_unsatisfiable() {
        assert!(matches!(
            parse_single_range("bytes=2000-3000", 1024),
            Err(RangeError::Unsatisfiable)
        ));
    }

    #[test]
    fn parses_decimal_seconds_for_timeout() {
        assert_eq!(
            humantime::parse_duration_lite("5"),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            humantime::parse_duration_lite("250ms"),
            Some(Duration::from_millis(250))
        );
    }
}

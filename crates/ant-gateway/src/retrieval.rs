//! D.2.3 retrieval endpoints.
//!
//! Wraps the existing `ant-retrieval` machinery exposed by the node loop
//! over `ant-control`'s command channel. The gateway never talks to
//! libp2p directly — every chunk fetch goes through
//! [`ant_control::ControlCommand`], reusing the routing-aware
//! [`ant_retrieval::RoutingFetcher`], its hedging, and the long-lived
//! chunk cache. This also means tests can swap a fake "node loop" in
//! that replies to the same commands from a fixture-backed map.
//!
//! Body payloads materialise in memory before streaming back. The plan
//! (PLAN.md D.2.3) calls for `Body::from_stream` so large files don't
//! materialise, but the underlying control command still acks one
//! `Vec<u8>`; switching to chunk-by-chunk delivery would require a
//! streaming retrieval API on `ant-control` and is deliberately out of
//! scope for the Tier-A drop-in. We raise the joiner's `max_bytes`
//! ceiling to [`GATEWAY_MAX_FILE_BYTES`] so real bzz sites with media
//! payloads (videos, archives, large images) actually load — the CLI
//! keeps the conservative `DEFAULT_MAX_FILE_BYTES` (32 MiB) where
//! `antctl get` is the only caller.

use std::time::Duration;

use ant_control::{ControlAck, ControlCommand};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures::stream;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// Bee-shaped per-request override for the per-chunk retrieval timeout.
/// We accept the header and use it to cap the *whole* request in this
/// first cut — finer-grained per-chunk plumbing through the control
/// channel is deferred (PLAN.md D.2.3 explicitly allows this scope).
const SWARM_CHUNK_RETRIEVAL_TIMEOUT: HeaderName =
    HeaderName::from_static("swarm-chunk-retrieval-timeout");
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
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
/// Single-chunk fetches are bounded by `ant-retrieval`'s internal
/// timeout (20 s) plus the outer retry loop. 60 s leaves room for
/// retries on a single chunk without inheriting the much larger
/// multi-chunk envelope.
const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-request body-size ceiling for `/bytes` and `/bzz`. Bee streams,
/// so it imposes no cap at all; we materialise the joined body in
/// memory before responding, so an unbounded ceiling would let one
/// pathological manifest exhaust process RAM. 1 GiB covers every
/// realistic web/media payload (videos, archives, large image sets)
/// while still rejecting a malicious root chunk that claims a
/// multi-terabyte span. Each in-flight request costs at most this much
/// resident memory until the response drains. The CLI keeps the much
/// tighter `ant_retrieval::DEFAULT_MAX_FILE_BYTES` (32 MiB) since its
/// audience is interactive `antctl get`, not a browser.
pub(crate) const GATEWAY_MAX_FILE_BYTES: u64 = 1024 * 1024 * 1024;

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

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

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

    let bytes = match ack {
        ControlAck::Bytes { data } => data,
        ControlAck::Error { message } => {
            return json_error(StatusCode::NOT_FOUND, message);
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };

    write_response(method, bytes, "application/octet-stream", None)
}

/// `GET /bytes/{addr}` and `HEAD /bytes/{addr}`. Joins the chunk tree
/// and streams the resulting bytes back as a single body. Honors a
/// single-range `Range` header against the joined output.
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

    let range_header = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);

    if method == Method::GET && range_header.is_none() {
        return match dispatch_stream_bytes(&handle, reference, timeout).await {
            Ok(streamed) => {
                let content_type = sniff_content_type(&streamed.first_bytes, query.filename());
                let filename = raw_bytes_filename(&addr, query.filename(), content_type);
                streaming_response(
                    streamed.total_bytes,
                    streamed.body,
                    content_type,
                    Some(&filename),
                )
            }
            Err(e) => e,
        };
    }

    let body = match dispatch_get_bytes(&handle, reference, timeout).await {
        Ok(b) => b,
        Err(e) => return e,
    };

    let content_type = sniff_content_type(&body, query.filename());
    let filename = raw_bytes_filename(&addr, query.filename(), content_type);
    finalize_with_range(method, body, content_type, Some(&filename), range_header)
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

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);

    let outcome = match dispatch_get_bzz(&handle, reference, path, timeout).await {
        Ok(o) => o,
        Err(e) => return e,
    };

    let content_type = outcome
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream")
        .to_string();
    let range_header = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    finalize_with_range(
        method,
        outcome.data,
        &content_type,
        outcome.filename.as_deref(),
        range_header,
    )
}

struct BzzOutcome {
    data: Vec<u8>,
    content_type: Option<String>,
    filename: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct BytesQuery {
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

struct StreamedBytes {
    total_bytes: u64,
    first_bytes: Vec<u8>,
    body: Body,
}

// `Err(Response)` is the routine "early-return an error response" path —
// boxing it would add a heap alloc to the failure case for no benefit.
#[allow(clippy::result_large_err)]
async fn dispatch_get_bytes(
    handle: &GatewayHandle,
    reference: [u8; 32],
    timeout: Duration,
) -> Result<Vec<u8>, Response> {
    let (ack_tx, mut ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::GetBytes {
        reference,
        bypass_cache: false,
        // Keep the control ack channel active while large joins are
        // making progress; `drain_terminal` drops these samples but each
        // one proves the node task is alive, so the gateway timeout acts
        // as an idle timeout instead of a hard wall for multi-minute media
        // downloads.
        progress: true,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    drain_terminal(&mut ack_rx, timeout)
        .await
        .and_then(|ack| match ack {
            ControlAck::Bytes { data } => Ok(data),
            ControlAck::Error { message } => Err(json_error(StatusCode::NOT_FOUND, message)),
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from GetBytes");
                Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ))
            }
        })
}

#[allow(clippy::result_large_err)]
async fn dispatch_stream_bytes(
    handle: &GatewayHandle,
    reference: [u8; 32],
    timeout: Duration,
) -> Result<StreamedBytes, Response> {
    let (ack_tx, mut ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::StreamBytes {
        reference,
        bypass_cache: false,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }

    let total_bytes = loop {
        match tokio::time::timeout(timeout, ack_rx.recv()).await {
            Ok(Some(ControlAck::BytesStreamStart { total_bytes })) => break total_bytes,
            Ok(Some(ControlAck::Progress(_))) => continue,
            Ok(Some(ControlAck::Error { message })) => {
                return Err(json_error(StatusCode::NOT_FOUND, message));
            }
            Ok(Some(other)) => {
                warn!(target: "ant_gateway", ?other, "unexpected ack before StreamBytes start");
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

    let first_chunk = loop {
        match tokio::time::timeout(timeout, ack_rx.recv()).await {
            Ok(Some(ControlAck::BytesChunk { data })) => break Some(data),
            Ok(Some(ControlAck::StreamDone)) => break None,
            Ok(Some(ControlAck::Progress(_))) => continue,
            Ok(Some(ControlAck::Error { message })) => {
                return Err(json_error(StatusCode::NOT_FOUND, message));
            }
            Ok(Some(other)) => {
                warn!(target: "ant_gateway", ?other, "unexpected ack before first StreamBytes chunk");
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

    let body_stream = stream::unfold((first_chunk, ack_rx), |(mut first, mut rx)| async move {
        if let Some(data) = first.take() {
            return Some((Ok::<Bytes, std::io::Error>(Bytes::from(data)), (None, rx)));
        }
        loop {
            match rx.recv().await {
                Some(ControlAck::BytesChunk { data }) => {
                    return Some((Ok::<Bytes, std::io::Error>(Bytes::from(data)), (None, rx)));
                }
                Some(ControlAck::StreamDone) | None => return None,
                Some(ControlAck::Progress(_)) => continue,
                Some(ControlAck::Error { message }) => {
                    return Some((
                        Err(std::io::Error::new(std::io::ErrorKind::Other, message)),
                        (None, rx),
                    ));
                }
                Some(other) => {
                    warn!(target: "ant_gateway", ?other, "unexpected ack during StreamBytes body");
                    return Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "unexpected node ack",
                        )),
                        (None, rx),
                    ));
                }
            }
        }
    });

    Ok(StreamedBytes {
        total_bytes,
        first_bytes,
        body: Body::from_stream(body_stream),
    })
}

#[allow(clippy::result_large_err)]
async fn dispatch_get_bzz(
    handle: &GatewayHandle,
    reference: [u8; 32],
    path: String,
    timeout: Duration,
) -> Result<BzzOutcome, Response> {
    let (ack_tx, mut ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::GetBzz {
        reference,
        path,
        // The plan calls out that mainnet bzz roots commonly carry a
        // redundancy level byte we don't yet RS-decode; bee's reader
        // strips it and serves anyway. We follow that — the joiner
        // still fails fast if a data chunk is actually missing.
        allow_degraded_redundancy: true,
        bypass_cache: false,
        progress: true,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    drain_terminal(&mut ack_rx, timeout)
        .await
        .and_then(|ack| match ack {
            ControlAck::BzzBytes {
                data,
                content_type,
                filename,
            } => Ok(BzzOutcome {
                data,
                content_type,
                filename,
            }),
            ControlAck::Error { message } => {
                let status = if message.contains("not found") || message.contains("'") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::BAD_GATEWAY
                };
                Err(json_error(status, message))
            }
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from GetBzz");
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
            Ok(Some(ControlAck::Progress(_))) => continue,
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
    let stripped = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if stripped.len() != 64 {
        return Err(format!(
            "reference must be 32 bytes (64 hex chars); got {}",
            stripped.len()
        ));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(stripped, &mut out).map_err(|e| format!("invalid hex: {e}"))?;
    Ok(out)
}

fn request_timeout(headers: &HeaderMap, default: Duration) -> Duration {
    headers
        .get(&SWARM_CHUNK_RETRIEVAL_TIMEOUT)
        .and_then(|v| v.to_str().ok())
        .and_then(humantime::parse_duration_lite)
        .unwrap_or(default)
}

fn write_response(
    method: Method,
    body: Vec<u8>,
    content_type: &str,
    filename: Option<&str>,
) -> Response {
    let len = body.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(body)
    };
    let mut resp = Response::new(body);
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    let _ = resp
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
        }
    }
    resp
}

fn streaming_response(
    total_bytes: u64,
    body: Body,
    content_type: &str,
    filename: Option<&str>,
) -> Response {
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
        "json" => Some("application/json"),
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
        _ => None,
    }
}

/// Apply a single-range `Range` header against the joined body if the
/// caller sent one. Multi-range (`bytes=0-10,20-30`) is rejected with
/// `416` per PLAN.md D.2.3 — bee does the same — because the joiner
/// has already materialised the file, so streaming multi-part responses
/// is not warranted.
fn finalize_with_range(
    method: Method,
    body: Vec<u8>,
    content_type: &str,
    filename: Option<&str>,
    range: Option<String>,
) -> Response {
    let Some(raw) = range else {
        return write_response(method, body, content_type, filename);
    };
    match parse_single_range(&raw, body.len() as u64) {
        Ok(Some((start, end))) => {
            let total = body.len();
            let slice = body[start as usize..=end as usize].to_vec();
            let len = slice.len();
            let body_obj = if method == Method::HEAD {
                Body::empty()
            } else {
                Body::from(slice)
            };
            let mut resp = Response::new(body_obj);
            *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            let _ = resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
            );
            let _ = resp
                .headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
            let _ = resp
                .headers_mut()
                .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            let cr = format!("bytes {start}-{end}/{total}");
            if let Ok(v) = HeaderValue::from_str(&cr) {
                resp.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            if let Some(name) = filename {
                if let Some(v) = content_disposition(name) {
                    resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
                }
            }
            resp
        }
        Ok(None) => write_response(method, body, content_type, filename),
        Err(RangeError::Multi) => json_error(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "multi-range requests not supported",
        ),
        Err(RangeError::Unsatisfiable) => {
            let mut resp = json_error(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "range not satisfiable for this resource",
            );
            let cr = format!("bytes */{}", body.len());
            if let Ok(v) = HeaderValue::from_str(&cr) {
                resp.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            resp
        }
        Err(RangeError::Malformed) => json_error(StatusCode::BAD_REQUEST, "malformed Range header"),
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

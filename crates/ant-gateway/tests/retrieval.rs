//! D.2.3 retrieval endpoints, replayed end-to-end against the same
//! `bzz_fixture.rs` chunks `ant-retrieval` already locks against. The
//! fake "node loop" in `common::handle_with_fixture_node` answers the
//! same `ControlCommand` shape production hits, so these tests exercise
//! the gateway's HTTP layer + the joiner / mantaray / cache machinery
//! without needing libp2p.
//!
//! Fixture facts (see `ant-retrieval/tests/bzz_fixture.rs`):
//! - Manifest root:  `ab77201f6541a9ceafb98a46c643273cfa397a87798273dd17feb2aa366ce2e6`
//! - Path:           `13/4358/2645.png`
//! - Data ref:       `285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296`
//! - Body sha-256:   `2fedb435506d7f61f6c1014d94a7422f53e4a1a6bdd1ff8231c1215033e5ea3d`
//! - Body length:    18 604 bytes (a 256x256 PNG tile)

mod common;

use ant_control::{ControlAck, ControlCommand};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use common::{body_bytes, fixture_dir, handle_with_fixture_node, router_with_dispatcher, send};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

const MANIFEST_ROOT: &str = "ab77201f6541a9ceafb98a46c643273cfa397a87798273dd17feb2aa366ce2e6";
const DATA_REF: &str = "285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296";
const BODY_LEN: usize = 18_604;
const BODY_SHA256: &str = "2fedb435506d7f61f6c1014d94a7422f53e4a1a6bdd1ff8231c1215033e5ea3d";

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// `GET /bzz/{root}/{path}` walks the manifest, joins the data tree,
/// and returns the file body. Verifies the same byte stream as
/// `ant-retrieval/tests/bzz_fixture.rs` plus the bee-shaped headers
/// (`Content-Type: image/png`, `Accept-Ranges: bytes`).
#[tokio::test]
async fn bzz_with_path_returns_fixture_png() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png",
    );
    assert_eq!(resp.headers().get(header::ACCEPT_RANGES).unwrap(), "bytes",);
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
    assert!(bytes.starts_with(&[0x89, b'P', b'N', b'G']));
}

/// `HEAD /bzz/...` must mirror the `GET` headers but return an empty body.
#[tokio::test]
async fn bzz_head_omits_body() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png",
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        BODY_LEN.to_string().as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert!(bytes.is_empty(), "HEAD must not carry a body");
}

/// `Range: bytes=0-1023` returns 206 + the requested slice + the right
/// `Content-Range` header.
#[tokio::test]
async fn bzz_range_request_returns_partial_content() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=0-1023")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes 0-1023/{BODY_LEN}").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 1024);
}

/// `/bzz/<root>` and `/bytes/<root>` must dispatch the joiner command
/// with a `max_bytes` ceiling well above the joiner's CLI-grade default
/// (32 MiB). Real bzz sites routinely carry single files >32 MiB; the
/// gateway has to opt in to a larger ceiling or every video / archive
/// 502s at the joiner step. Regression for the
/// "join data ...: file too large: span 36879400 bytes, cap 33554432
/// bytes" report from freedom-browser.
#[tokio::test]
async fn gateway_raises_joiner_max_bytes_above_cli_default() {
    use ant_retrieval::DEFAULT_MAX_FILE_BYTES;

    let captured: Arc<Mutex<Vec<Option<u64>>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_for_dispatch = captured.clone();
    let router = router_with_dispatcher(move |cmd| {
        let captured = captured_for_dispatch.clone();
        async move {
            match cmd {
                ControlCommand::StreamBytes { max_bytes, ack, .. } => {
                    captured.lock().await.push(max_bytes);
                    let _ = ack
                        .send(ControlAck::BytesStreamStart { total_bytes: 4 })
                        .await;
                    let _ = ack
                        .send(ControlAck::BytesChunk {
                            data: b"stub".to_vec(),
                        })
                        .await;
                    let _ = ack.send(ControlAck::StreamDone).await;
                }
                ControlCommand::StreamBzz { max_bytes, ack, .. } => {
                    captured.lock().await.push(max_bytes);
                    let _ = ack
                        .send(ControlAck::BzzStreamStart {
                            total_bytes: 4,
                            content_type: Some("text/plain".to_string()),
                            filename: None,
                        })
                        .await;
                    let _ = ack
                        .send(ControlAck::BytesChunk {
                            data: b"stub".to_vec(),
                        })
                        .await;
                    let _ = ack.send(ControlAck::StreamDone).await;
                }
                _ => {}
            }
        }
    });

    let uri_bytes = format!("/bytes/{DATA_REF}");
    let _ = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(&uri_bytes)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let uri_bzz = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let _ = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri_bzz)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let seen = captured.lock().await.clone();
    assert_eq!(seen.len(), 2, "both commands should reach the dispatcher");
    for (i, max_bytes) in seen.iter().enumerate() {
        let n = max_bytes.unwrap_or_else(|| {
            panic!("dispatch {i}: gateway must set max_bytes, got None (would inherit CLI cap)")
        });
        assert!(
            n > DEFAULT_MAX_FILE_BYTES as u64,
            "dispatch {i}: max_bytes={n} must exceed CLI default {DEFAULT_MAX_FILE_BYTES}",
        );
    }
}

/// `/bzz/<root>/` (trailing slash, no path) must route to the manifest
/// root just like `/bzz/<root>`. freedom-browser's `bzz://` protocol
/// handler canonicalises every URL via `new URL(...)` which always
/// produces a trailing `/`, so a regression here would break it.
#[tokio::test]
async fn bzz_trailing_slash_resolves_to_manifest_root() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // The fixture manifest has no `website-index-document`, so a
    // properly-routed request resolves to a 404 from `lookup_path`,
    // not the 501 catch-all.
    assert_ne!(
        resp.status(),
        StatusCode::NOT_IMPLEMENTED,
        "trailing-slash bzz must NOT fall through to the 501 fallback",
    );
}

/// Multi-range requests are rejected with 416 (PLAN.md D.2.3).
#[tokio::test]
async fn bzz_multi_range_rejected_416() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=0-99,200-299")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

/// `GET /bytes/{addr}` joins the multi-chunk tree directly without a
/// manifest hop. Pointing at the fixture's data ref must reproduce the
/// exact same body the manifest path resolves to.
#[tokio::test]
async fn bytes_returns_fixture_payload() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png"
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_DISPOSITION).unwrap(),
        format!("inline; filename=\"{DATA_REF}.png\"").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
}

#[tokio::test]
async fn bytes_filename_query_sets_content_headers() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}?filename=tile.wav");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "audio/wav"
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_DISPOSITION).unwrap(),
        "inline; filename=\"tile.wav\"",
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
}

/// `GET /chunks/{addr}` returns the **wire bytes** (`span (8 LE) || payload`),
/// matching bee's `chunkstore.Get`. Compare against the on-disk fixture
/// chunk to lock that wiring.
#[tokio::test]
async fn chunks_returns_wire_bytes() {
    let router = handle_with_fixture_node();
    let uri = format!("/chunks/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );
    let bytes = body_bytes(resp).await;
    let expected =
        std::fs::read(fixture_dir().join(format!("{DATA_REF}.bin"))).expect("fixture chunk");
    assert_eq!(bytes, expected);
}

/// Bad reference (wrong length / non-hex) maps to 400 with the bee-shaped
/// error body, not 500 / 404.
#[tokio::test]
async fn bytes_rejects_short_reference_with_400() {
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/bytes/abc123")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_bytes(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], 400);
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("reference must be 32 bytes"));
}

/// `HEAD /bytes/{addr}` mirrors `HEAD /bzz/...`: it must return
/// `Content-Length` and `Accept-Ranges` headers identical to the GET
/// response, with an empty body.
#[tokio::test]
async fn bytes_head_omits_body() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        BODY_LEN.to_string().as_str(),
    );
    assert_eq!(resp.headers().get(header::ACCEPT_RANGES).unwrap(), "bytes",);
    let bytes = body_bytes(resp).await;
    assert!(bytes.is_empty(), "HEAD must not carry a body");
}

/// `HEAD /bzz/...` and `HEAD /bytes/...` must signal `head_only=true`
/// to the daemon so it returns size + content-type without joining the
/// chunk tree. Asserts directly on the dispatched `ControlCommand` —
/// catches regressions where the gateway accidentally falls back to the
/// full-body path and just hides the body inside axum.
#[tokio::test]
async fn head_dispatches_head_only_flag() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let saw_head_only = Arc::new(AtomicBool::new(false));
    let saw_body_join = Arc::new(AtomicBool::new(false));
    let head_clone = saw_head_only.clone();
    let body_clone = saw_body_join.clone();
    let router = router_with_dispatcher(move |cmd| {
        let head = head_clone.clone();
        let body = body_clone.clone();
        async move {
            if let ControlCommand::StreamBzz { head_only, ack, .. } = cmd {
                if head_only {
                    head.store(true, Ordering::SeqCst);
                }
                let _ = ack
                    .send(ControlAck::BzzStreamStart {
                        total_bytes: 1024,
                        content_type: Some("video/mp4".to_string()),
                        filename: Some("clip.mp4".to_string()),
                    })
                    .await;
                if !head_only {
                    // If the gateway ever drove a HEAD request through
                    // the body path, we'd see this branch fire and
                    // surface as an extra `BytesChunk` in the response.
                    body.store(true, Ordering::SeqCst);
                    let _ = ack
                        .send(ControlAck::BytesChunk {
                            data: vec![0u8; 1024],
                        })
                        .await;
                }
                let _ = ack.send(ControlAck::StreamDone).await;
            }
        }
    });

    let uri = format!("/bzz/{MANIFEST_ROOT}/clip.mp4");
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get(header::CONTENT_LENGTH).unwrap(), "1024",);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "video/mp4",
    );
    let bytes = body_bytes(resp).await;
    assert!(bytes.is_empty());
    assert!(
        saw_head_only.load(Ordering::SeqCst),
        "HEAD must dispatch with head_only=true",
    );
    assert!(
        !saw_body_join.load(Ordering::SeqCst),
        "HEAD must not trigger any body chunk fetch",
    );
}

/// `Range: bytes=0-` (browser range-probe) returns `206 Partial Content`
/// covering the whole file. Browsers issue this exact request to decide
/// whether the server supports ranges; without `206` they fall back to
/// progressive download and lose seeking.
#[tokio::test]
async fn bzz_range_zero_dash_returns_206() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=0-")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes 0-{}/{BODY_LEN}", BODY_LEN - 1).as_str(),
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        BODY_LEN.to_string().as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
}

/// Suffix range (`bytes=-N`) returns the last N bytes. This is what
/// browsers issue when a media container's metadata sits at the tail of
/// the file (MP4 with `moov` at the end).
#[tokio::test]
async fn bzz_suffix_range_returns_tail() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=-256")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let expected_start = BODY_LEN - 256;
    let expected_end = BODY_LEN - 1;
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes {expected_start}-{expected_end}/{BODY_LEN}").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 256);
}

/// `Range: bytes=START-` (open-ended) returns `[START, END]` of the file.
/// This is the canonical "resume my interrupted download" shape.
#[tokio::test]
async fn bzz_open_ended_range_returns_remainder() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let start: usize = 10_000;
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, format!("bytes={start}-"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes {start}-{}/{BODY_LEN}", BODY_LEN - 1).as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN - start);
}

/// Range request with a single-range header that goes past EOF returns
/// `416 Range Not Satisfiable` with a `Content-Range: bytes */<total>`
/// header so the client can correct its request.
#[tokio::test]
async fn bzz_range_past_eof_returns_416() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=1000000-2000000")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes */{BODY_LEN}").as_str(),
    );
}

/// Range request on `/bytes/{addr}` mirrors the `/bzz` semantics.
#[tokio::test]
async fn bytes_range_request_returns_partial_content() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=100-199")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes 100-199/{BODY_LEN}").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 100);
}

/// Unknown chunk → bee-shaped 404 (the joiner's "fetch failed" lands on
/// `ControlAck::Error`, which the gateway maps to 404 for the retrieval
/// surface).
#[tokio::test]
async fn chunks_404_on_missing_reference() {
    let router = handle_with_fixture_node();
    let unknown = "00".repeat(32);
    let uri = format!("/chunks/{unknown}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 404);
}

/// Every content-addressed endpoint must emit the `Cache-Control:
/// public, max-age=..., immutable` header so the browser can serve
/// reloads from its own HTTP cache without re-walking the manifest.
/// Bee has set this header on `/bzz` since at least its 1.x line; not
/// matching it makes a music-player-style site that reloads the same
/// 13 audio tracks + artwork on every navigation feel dramatically
/// slower than against bee, even when our disk cache hit rate is
/// 100% (cf. discussion 2026-05 / `c4f8a45301...`). The test pins
/// every endpoint at once because the response builders share a
/// helper — a regression that disables one would likely disable all
/// — and a single failing endpoint is enough to defeat the
/// browser's reload-from-cache behaviour for a typical bzz site.
#[tokio::test]
async fn content_addressed_endpoints_emit_immutable_cache_control() {
    let router = handle_with_fixture_node();

    // The chunk endpoint serves the raw manifest root chunk by hash.
    let chunk_addr = "ed5b81dac5d34d22acd6db28ee864bc6f4d0d31db17f9f4ec6e62a89c1f31cab";
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/chunks/{chunk_addr}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // Don't assert status — the fixture pool may or may not contain
    // this exact chunk; we only care that whatever response shape we
    // *do* return for content-addressed paths carries the header.
    if resp.status() == StatusCode::OK {
        let cc = resp
            .headers()
            .get(header::CACHE_CONTROL)
            .expect("/chunks must emit Cache-Control");
        assert!(
            cc.to_str().unwrap().contains("immutable"),
            "/chunks Cache-Control missing `immutable`: {cc:?}",
        );
    }

    // The bytes endpoint streams the joiner output for a content
    // hash. Always immutable.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/bytes/{DATA_REF}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/bytes must emit Cache-Control");
    let cc_str = cc.to_str().unwrap();
    assert!(
        cc_str.contains("immutable") && cc_str.contains("max-age="),
        "/bytes Cache-Control missing `immutable` / `max-age`: {cc_str}",
    );
    let _ = body_bytes(resp).await;

    // The bzz endpoint resolves a manifest path. Always immutable.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/bzz must emit Cache-Control");
    assert!(
        cc.to_str().unwrap().contains("immutable"),
        "/bzz Cache-Control missing `immutable`: {cc:?}",
    );
    let _ = body_bytes(resp).await;

    // Range responses (206) MUST also emit Cache-Control or the
    // browser will re-fetch the partial bytes on every replay.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png"))
            .header(header::RANGE, "bytes=0-1023")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/bzz 206 must emit Cache-Control");
    assert!(
        cc.to_str().unwrap().contains("immutable"),
        "/bzz 206 Cache-Control missing `immutable`: {cc:?}",
    );
    let _ = body_bytes(resp).await;

    // The Ant-specific manifest listing is content-addressed too.
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/v0/manifest/{MANIFEST_ROOT}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/v0/manifest must emit Cache-Control");
    assert!(
        cc.to_str().unwrap().contains("immutable"),
        "/v0/manifest Cache-Control missing `immutable`: {cc:?}",
    );
}

/// `POST /chunks` round-trips a stamped CAC chunk via the
/// `PushChunk` control command and returns bee-shaped
/// `{"reference": "<lowercase 64-hex>"}` with `201 Created`.
/// The fixture node responds with the BMT hash of the wire bytes
/// (mirrors what `ant-p2p::handle_control_command` does locally
/// before pushsync), so we can lock the wire shape without
/// standing up libp2p.
#[tokio::test]
async fn post_chunks_returns_bee_shaped_reference() {
    let payload: &[u8] = b"hello swarm";
    let (addr, wire) = ant_crypto::bmt::cac_new(payload).expect("cac_new");

    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/chunks")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(
        json["reference"].as_str().unwrap(),
        hex::encode(addr),
        "POST /chunks reference must be bare lowercase 64-hex (no 0x), bee-compatible",
    );
}

/// Empty / over-sized bodies must be rejected with a 400 bee-shaped
/// error before the upload path is even contacted, so a malicious
/// caller can't burn a postage slot on a bogus chunk.
#[tokio::test]
async fn post_chunks_rejects_oversized_body() {
    let router = handle_with_fixture_node();
    let too_big = vec![0u8; 8 + 4097];
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/chunks")
            .body(Body::from(too_big))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 400);
    assert!(
        json["message"].as_str().unwrap().contains("out of range"),
        "expected size error message, got {}",
        json["message"],
    );
}

/// `POST /bzz?name=hello.txt` chunks the request body, builds a
/// single-file mantaray, pushes every chunk via `PushChunk`, then
/// returns a Bee-shaped 201 with the manifest root. Reusing the fake
/// node loop's BMT-only `PushChunk` handler means we only assert
/// reference shape + chunk count here; the production smoke test
/// covers the live pushsync handshake.
#[tokio::test]
async fn post_bzz_single_file_returns_manifest_reference() {
    let payload = b"uploaded via ant\n".to_vec();
    let pushed_chunks = Arc::new(Mutex::new(Vec::<[u8; 32]>::new()));
    let pushed = pushed_chunks.clone();
    let router = router_with_dispatcher(move |cmd| {
        let pushed = pushed.clone();
        async move {
            if let ControlCommand::PushChunk { wire, ack } = cmd {
                let mut span = [0u8; 8];
                span.copy_from_slice(&wire[..8]);
                let payload = &wire[8..];
                let addr = ant_crypto::bmt::bmt_hash_with_span(&span, payload).unwrap();
                pushed.lock().await.push(addr);
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(addr)),
                });
            }
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bzz?name=hello.txt")
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Body::from(payload.clone()))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let reference = json["reference"].as_str().unwrap().to_string();
    assert_eq!(reference.len(), 64, "reference must be bare 64-hex");

    // Single-file manifest = data leaf (1) + slash stub + file leaf + root = 4 chunks.
    let pushed = pushed_chunks.lock().await;
    assert_eq!(
        pushed.len(),
        4,
        "expected 4 chunks (data + slash stub + file leaf + manifest root), got {}: {:?}",
        pushed.len(),
        pushed.iter().map(hex::encode).collect::<Vec<_>>(),
    );
    // The last chunk pushed isn't strictly the root (stream::buffer_unordered
    // doesn't guarantee order), but the manifest root *must* be in the set.
    let mut want = [0u8; 32];
    hex::decode_to_slice(&reference, &mut want).unwrap();
    assert!(
        pushed.iter().any(|a| a == &want),
        "manifest root {reference} not among pushed chunks",
    );
}

/// `POST /bzz` with `swarm-collection: true` and a tar archive should
/// produce a multi-file manifest. The returned reference must be
/// retrievable as a manifest with each archive entry's path resolving
/// to its own data ref. We don't have round-trip retrieval for the
/// just-uploaded chunks here (the dispatcher only handles `PushChunk`),
/// but we do verify the chunk count makes sense and the response
/// shape is bee-compatible.
#[tokio::test]
async fn post_bzz_collection_uploads_tar_archive() {
    // Build a tiny tar archive with two files.
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        for (path, body) in [
            ("index.html", &b"<h1>hi</h1>"[..]),
            ("style.css", &b"body{color:red}"[..]),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, body).unwrap();
        }
        builder.finish().unwrap();
    }

    let pushed_chunks = Arc::new(Mutex::new(Vec::<[u8; 32]>::new()));
    let pushed = pushed_chunks.clone();
    let router = router_with_dispatcher(move |cmd| {
        let pushed = pushed.clone();
        async move {
            if let ControlCommand::PushChunk { wire, ack } = cmd {
                let mut span = [0u8; 8];
                span.copy_from_slice(&wire[..8]);
                let payload = &wire[8..];
                let addr = ant_crypto::bmt::bmt_hash_with_span(&span, payload).unwrap();
                pushed.lock().await.push(addr);
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(addr)),
                });
            }
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bzz")
            .header("swarm-collection", "true")
            .body(Body::from(tar_bytes))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let reference = json["reference"].as_str().unwrap().to_string();
    assert_eq!(reference.len(), 64);

    // 2 file data chunks (each ≤ CHUNK_SIZE = 1 leaf each) + 2 manifest
    // file leaves + slash stub for index-doc + root = 6 chunks.
    let pushed = pushed_chunks.lock().await;
    assert!(
        pushed.len() >= 5,
        "expected ≥5 chunks for 2-file collection, got {}",
        pushed.len(),
    );
    let mut want = [0u8; 32];
    hex::decode_to_slice(&reference, &mut want).unwrap();
    assert!(
        pushed.iter().any(|a| a == &want),
        "collection manifest root {reference} not among pushed chunks",
    );
}

// =====================================================================
// SOC reads: GET / HEAD `/soc/{owner}/{id}`.
//
// Freedom Browser navigates `bzz://` URLs; once the gateway resolves a
// feed-mounted manifest it sometimes wants the underlying SOC bytes
// directly (e.g. to surface the publisher key without re-walking the
// feed). These tests use `router_with_dispatcher` so we can stage a
// single `(addr → soc_wire)` mapping rather than dropping fixture
// chunks on disk; the gateway only ever sends `GetChunkRaw` for SOCs,
// so the dispatcher stays small.
// =====================================================================

/// Owner secret + id + payload bundle. Every test builds one with a
/// fresh `secret` byte so the owner address is deterministic but
/// unique per test, avoiding any cross-test cache interference.
struct SocFixture {
    owner: [u8; 20],
    id: [u8; 32],
    soc_address: [u8; 32],
    wire: Vec<u8>,
    inner_cac_wire: Vec<u8>,
}

/// Build a self-consistent SOC: signs `keccak256(id || inner_cac_addr)`
/// with `secret`, packs `id || sig || inner_cac_wire`, and computes the
/// `keccak256(id || owner)` address that `GET /soc/{owner}/{id}` should
/// resolve to.
fn signed_soc(secret: [u8; 32], id: [u8; 32], payload: &[u8]) -> SocFixture {
    use k256::ecdsa::{SigningKey, VerifyingKey};

    let sk = SigningKey::from_bytes(&secret.into()).expect("valid secret");
    let vk = VerifyingKey::from(&sk);
    let owner = ant_crypto::ethereum_address_from_public_key(&vk);

    let (inner_cac_addr, inner_cac_wire) = ant_crypto::cac_new(payload).expect("cac_new");

    let mut digest_input = [0u8; 64];
    digest_input[..32].copy_from_slice(&id);
    digest_input[32..].copy_from_slice(&inner_cac_addr);
    let sig = ant_crypto::sign_handshake_data(&secret, &ant_crypto::keccak256(&digest_input))
        .expect("sign");

    let mut soc_addr_input = [0u8; 52];
    soc_addr_input[..32].copy_from_slice(&id);
    soc_addr_input[32..].copy_from_slice(&owner);
    let soc_address = ant_crypto::keccak256(&soc_addr_input);

    let mut wire = Vec::with_capacity(32 + 65 + inner_cac_wire.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&inner_cac_wire);

    SocFixture {
        owner,
        id,
        soc_address,
        wire,
        inner_cac_wire,
    }
}

/// Build a router that serves the staged SOC at its derived address and
/// errors on any other reference. Mirrors how the production node loop
/// resolves `GetChunkRaw` against the chunk store.
fn router_serving_soc(fixture: &SocFixture) -> axum::Router {
    let addr = fixture.soc_address;
    let wire = fixture.wire.clone();
    router_with_dispatcher(move |cmd| {
        let wire = wire.clone();
        async move {
            match cmd {
                ControlCommand::GetChunkRaw { reference, ack } => {
                    let reply = if reference == addr {
                        ControlAck::Bytes { data: wire }
                    } else {
                        ControlAck::Error {
                            message: format!("not found: {}", hex::encode(reference)),
                        }
                    };
                    let _ = ack.send(reply);
                }
                _ => panic!("unexpected command in SOC test: {cmd:?}"),
            }
        }
    })
}

#[tokio::test]
async fn get_soc_returns_wire_bytes_for_known_address() {
    let fixture = signed_soc([0x11; 32], [0xa1; 32], b"hello soc");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes, fixture.wire);
}

#[tokio::test]
async fn get_soc_accepts_0x_prefixed_hex() {
    let fixture = signed_soc([0x12; 32], [0xa2; 32], b"0x-prefixed");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/0x{}/0X{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await, fixture.wire);
}

#[tokio::test]
async fn get_soc_rejects_bad_owner_hex() {
    let fixture = signed_soc([0x13; 32], [0xa3; 32], b"x");
    let router = router_serving_soc(&fixture);
    // Owner is too short (19 bytes worth of hex).
    let uri = format!("/soc/{}/{}", "ab".repeat(19), hex::encode(fixture.id));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 400);
    assert!(json["message"].as_str().unwrap().contains("bad owner"));
}

#[tokio::test]
async fn get_soc_rejects_bad_id_hex() {
    let fixture = signed_soc([0x14; 32], [0xa4; 32], b"x");
    let router = router_serving_soc(&fixture);
    // Id contains a non-hex character.
    let uri = format!("/soc/{}/{}", hex::encode(fixture.owner), "zz".repeat(32));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(json["message"].as_str().unwrap().contains("bad id"));
}

#[tokio::test]
async fn get_soc_returns_404_when_chunk_missing() {
    let fixture = signed_soc([0x15; 32], [0xa5; 32], b"x");
    // Drop the wire so the dispatcher can't find this address.
    let other_id = [0xff; 32];
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(other_id)
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn head_soc_returns_200_no_body() {
    let fixture = signed_soc([0x16; 32], [0xa6; 32], b"head probe");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let expected_len = fixture.wire.len().to_string();
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        expected_len.as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert!(
        bytes.is_empty(),
        "HEAD body must be empty, got {} bytes",
        bytes.len()
    );
}

#[tokio::test]
async fn get_soc_does_not_set_immutable_cache_control() {
    // SOCs are mutable at the address level — same `(owner, id)` after
    // a re-upload returns a different inner CAC. The cache header must
    // not let the browser pin the first response forever.
    let fixture = signed_soc([0x17; 32], [0xa7; 32], b"mutable");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/soc must emit Cache-Control");
    let cc_str = cc.to_str().unwrap();
    assert!(
        !cc_str.contains("immutable"),
        "/soc Cache-Control must not be immutable: {cc_str}",
    );
    assert!(
        cc_str.contains("no-cache"),
        "/soc Cache-Control should be `no-cache`, got {cc_str}",
    );
}

#[tokio::test]
async fn get_soc_returns_full_soc_wire_not_inner_cac() {
    // The body must be `id(32) || sig(65) || inner_cac_wire`, not just
    // the inner CAC. Pin the length bookkeeping so a refactor that
    // accidentally peels the SOC header before responding fails loudly.
    let fixture = signed_soc([0x18; 32], [0xa8; 32], b"layout pin");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 32 + 65 + fixture.inner_cac_wire.len());
    assert_eq!(&bytes[..32], &fixture.id);
    assert_eq!(&bytes[32 + 65..], fixture.inner_cac_wire.as_slice());
}

/// Oversized body must be rejected with 413 before chunking begins.
#[tokio::test]
async fn post_bzz_rejects_oversized_body() {
    let router = handle_with_fixture_node();
    // GATEWAY_MAX_UPLOAD_BYTES is 64 MiB; build something just over.
    let too_big = vec![0u8; 64 * 1024 * 1024 + 1];
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bzz")
            .body(Body::from(too_big))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

// =====================================================================
// Sequence-feed reads: GET `/feeds/{owner}/{topic}`.
//
// These tests stage their own SOC chunks at every sequence-update
// address the resolver will probe. The dispatcher answers
// `GetChunkRaw` from a HashMap so the gateway → control → resolver
// wiring is exercised end-to-end without standing up a swarm.
// =====================================================================

use std::collections::HashMap;

/// Build a v1 sequence-feed update SOC chunk (`id || sig || inner_cac`)
/// for `(owner, topic)` at `index`, embedding `wrapped_ref` and `ts`.
/// Returns `(soc_addr, soc_wire)`. Mirrors the helper in
/// `ant-retrieval::feed::tests::make_sequence_update_v1`.
fn make_feed_update(
    secret: &[u8; 32],
    owner: &[u8; 20],
    topic: &[u8; 32],
    index: u64,
    ts: u64,
    wrapped_ref: &[u8; 32],
) -> ([u8; 32], Vec<u8>) {
    let mut id_input = Vec::with_capacity(40);
    id_input.extend_from_slice(topic);
    id_input.extend_from_slice(&index.to_be_bytes());
    let id = ant_crypto::keccak256(&id_input);

    let mut payload = Vec::with_capacity(40);
    payload.extend_from_slice(&ts.to_be_bytes());
    payload.extend_from_slice(wrapped_ref);
    let (inner_cac_addr, inner_cac_wire) = ant_crypto::cac_new(&payload).expect("cac");

    let mut sig_input = Vec::with_capacity(64);
    sig_input.extend_from_slice(&id);
    sig_input.extend_from_slice(&inner_cac_addr);
    let prehash = ant_crypto::keccak256(&sig_input);
    let sig = ant_crypto::sign_handshake_data(secret, &prehash).expect("sign");

    let mut soc_addr_input = Vec::with_capacity(52);
    soc_addr_input.extend_from_slice(&id);
    soc_addr_input.extend_from_slice(owner);
    let soc_addr = ant_crypto::keccak256(&soc_addr_input);

    let mut wire = Vec::with_capacity(32 + 65 + inner_cac_wire.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&inner_cac_wire);
    (soc_addr, wire)
}

fn deterministic_owner(secret: &[u8; 32]) -> [u8; 20] {
    use k256::ecdsa::{SigningKey, VerifyingKey};
    let sk = SigningKey::from_bytes(secret.into()).expect("valid secret");
    let vk = VerifyingKey::from(&sk);
    ant_crypto::ethereum_address_from_public_key(&vk)
}

/// Stage `updates` (index → `wrapped_ref`) and return a router that
/// resolves them via the real `resolve_sequence_feed_full`. The
/// dispatcher answers `GetChunkRaw` from a `HashMap`; the gateway's
/// `download_feed` uses `ControlCommand::GetFeed`, but the test
/// fixture's `GetFeed` arm hands the work to a `RoutingFetcher`-style
/// `ChunkFetcher` over the same chunk map. Here we plug in directly.
struct FeedScenario {
    secret: [u8; 32],
    owner: [u8; 20],
    topic: [u8; 32],
}

impl FeedScenario {
    fn new(secret_seed: u8, topic_seed: u8) -> Self {
        let secret = [secret_seed; 32];
        let owner = deterministic_owner(&secret);
        let topic = [topic_seed; 32];
        Self {
            secret,
            owner,
            topic,
        }
    }

    fn router(&self, updates: &[(u64, [u8; 32], u64)]) -> axum::Router {
        let mut chunks: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
        for (index, reference, ts) in updates {
            let (addr, wire) = make_feed_update(
                &self.secret,
                &self.owner,
                &self.topic,
                *index,
                *ts,
                reference,
            );
            chunks.insert(addr, wire);
        }
        let owner = self.owner;
        let topic = self.topic;
        router_with_dispatcher(move |cmd| {
            let chunks = chunks.clone();
            let owner = owner;
            let topic = topic;
            async move {
                handle_feed_test_command(&chunks, owner, topic, cmd).await;
            }
        })
    }
}

async fn handle_feed_test_command(
    chunks: &HashMap<[u8; 32], Vec<u8>>,
    expected_owner: [u8; 20],
    expected_topic: [u8; 32],
    cmd: ControlCommand,
) {
    match cmd {
        ControlCommand::GetChunkRaw { reference, ack } => {
            let reply = match chunks.get(&reference) {
                Some(wire) => ControlAck::Bytes { data: wire.clone() },
                None => ControlAck::Error {
                    message: format!("not found: {}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::GetFeed { owner, topic, ack } => {
            // Resolve via the real `resolve_sequence_feed_full` against
            // the staged HashMap. Mirrors how the production node-side
            // handler runs the resolver, so an end-to-end gateway test
            // exercises the wiring without a swarm.
            assert_eq!(owner, expected_owner);
            assert_eq!(topic, expected_topic);
            let feed = ant_retrieval::Feed {
                owner,
                topic,
                kind: ant_retrieval::FeedType::Sequence,
            };
            let fetcher = MapFetcher {
                chunks: chunks.clone(),
            };
            let reply = match ant_retrieval::resolve_sequence_feed_full(&fetcher, &feed).await {
                Ok(r) => ControlAck::FeedResolved {
                    id: ant_retrieval::sequence_update_id(&topic, r.index),
                    reference: r.reference,
                    index: r.index,
                    ts: r.ts,
                },
                Err(ant_retrieval::FeedError::NoUpdates { .. }) => ControlAck::FeedNotFound,
                Err(e) => ControlAck::Error {
                    message: format!("feed lookup: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        other => panic!("unexpected command in feed test: {other:?}"),
    }
}

/// Tiny `ChunkFetcher` over an owned `HashMap<addr, wire>` so the feed
/// test dispatcher can drive `resolve_sequence_feed_full` without
/// touching the live network.
struct MapFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
}

#[async_trait::async_trait]
impl ant_retrieval::ChunkFetcher for MapFetcher {
    async fn fetch(
        &self,
        addr: [u8; 32],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.chunks.get(&addr).cloned().ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                format!("not found: {}", hex::encode(addr)).into()
            },
        )
    }
}

fn feed_uri(owner: &[u8; 20], topic: &[u8; 32]) -> String {
    format!("/feeds/{}/{}", hex::encode(owner), hex::encode(topic))
}

#[tokio::test]
async fn get_feed_returns_latest_reference() {
    let scenario = FeedScenario::new(0x21, 0xb1);
    let target = [0xddu8; 32];
    let router = scenario.router(&[(0, [0x10; 32], 100), (1, [0x20; 32], 200), (2, target, 300)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );

    let expected_id = ant_retrieval::sequence_update_id(&scenario.topic, 2);
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        hex::encode(expected_id).as_str(),
        "swarm-feed-index must be the SOC id (bee shape), not the zero-padded u64 index",
    );

    let body = body_bytes(resp).await;
    assert_eq!(body.len(), 40);
    assert_eq!(&body[..8], &300u64.to_be_bytes());
    assert_eq!(&body[8..], &target);
}

#[tokio::test]
async fn get_feed_accept_json_returns_json_body() {
    let scenario = FeedScenario::new(0x22, 0xb2);
    let target = [0xeeu8; 32];
    let router = scenario.router(&[(0, target, 1234)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(header::ACCEPT, "application/json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json",
    );
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["reference"].as_str().unwrap(), hex::encode(target));
    assert_eq!(json["ts"].as_u64().unwrap(), 1234);
    assert_eq!(
        json["index"].as_u64().unwrap(),
        0,
        "feed JSON index is a number, not a hex string",
    );
}

#[tokio::test]
async fn get_feed_accept_xml_returns_xml_body() {
    let scenario = FeedScenario::new(0x23, 0xb3);
    let target = [0xffu8; 32];
    let router = scenario.router(&[(0, target, 7)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(header::ACCEPT, "application/xml")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/xml",
    );
    let body = String::from_utf8(body_bytes(resp).await).unwrap();
    assert!(body.contains(&format!("<reference>{}</reference>", hex::encode(target))));
    assert!(body.contains("<ts>7</ts>"));
    assert!(body.contains("<index>0</index>"));
}

#[tokio::test]
async fn get_feed_unknown_accept_falls_back_to_octet_stream() {
    let scenario = FeedScenario::new(0x24, 0xb4);
    let target = [0xaau8; 32];
    let router = scenario.router(&[(0, target, 5)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(header::ACCEPT, "text/csv")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );
}

#[tokio::test]
async fn get_feed_defaults_to_sequence_type() {
    // No `?type=` parameter; should default to Sequence (the only
    // implemented variant) and resolve.
    let scenario = FeedScenario::new(0x25, 0xb5);
    let router = scenario.router(&[(0, [0x55; 32], 1)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_feed_explicit_type_sequence_accepted() {
    let scenario = FeedScenario::new(0x26, 0xb6);
    let router = scenario.router(&[(0, [0x66; 32], 1)]);
    let uri = format!(
        "/feeds/{}/{}?type=sequence",
        hex::encode(scenario.owner),
        hex::encode(scenario.topic),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_feed_type_epoch_returns_501() {
    let scenario = FeedScenario::new(0x27, 0xb7);
    let router = scenario.router(&[]);
    let uri = format!(
        "/feeds/{}/{}?type=epoch",
        hex::encode(scenario.owner),
        hex::encode(scenario.topic),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn get_feed_unknown_type_returns_400() {
    let scenario = FeedScenario::new(0x28, 0xb8);
    let router = scenario.router(&[]);
    let uri = format!(
        "/feeds/{}/{}?type=bogus",
        hex::encode(scenario.owner),
        hex::encode(scenario.topic),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_feed_accepts_0x_prefixed_hex() {
    let scenario = FeedScenario::new(0x29, 0xb9);
    let target = [0x77u8; 32];
    let router = scenario.router(&[(0, target, 2)]);
    let uri = format!(
        "/feeds/0x{}/0X{}",
        hex::encode(scenario.owner),
        hex::encode(scenario.topic),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_feed_rejects_bad_owner_hex() {
    let scenario = FeedScenario::new(0x2a, 0xba);
    let router = scenario.router(&[]);
    let uri = format!("/feeds/{}/{}", "ab".repeat(19), hex::encode(scenario.topic));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(json["message"].as_str().unwrap().contains("bad owner"));
}

#[tokio::test]
async fn get_feed_rejects_bad_topic_hex() {
    let scenario = FeedScenario::new(0x2b, 0xbb);
    let router = scenario.router(&[]);
    let uri = format!("/feeds/{}/{}", hex::encode(scenario.owner), "zz".repeat(32));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(json["message"].as_str().unwrap().contains("bad topic"));
}

#[tokio::test]
async fn get_feed_returns_404_for_empty_feed() {
    let scenario = FeedScenario::new(0x2c, 0xbc);
    let router = scenario.router(&[]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_feed_does_not_set_immutable_cache_control() {
    let scenario = FeedScenario::new(0x2d, 0xbd);
    let router = scenario.router(&[(0, [0x99; 32], 1)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/feeds must emit Cache-Control");
    let cc_str = cc.to_str().unwrap();
    assert!(
        !cc_str.contains("immutable"),
        "/feeds Cache-Control must not be immutable: {cc_str}",
    );
    assert!(
        cc_str.contains("no-cache"),
        "/feeds Cache-Control should be `no-cache`, got {cc_str}",
    );
}

#[tokio::test]
async fn get_feed_qvalue_picks_best_match() {
    let scenario = FeedScenario::new(0x2e, 0xbe);
    let target = [0xab; 32];
    let router = scenario.router(&[(0, target, 9)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            // text/html is preferred but unsupported; json is the only
            // recognised format and wins despite a lower q-value.
            .header(header::ACCEPT, "text/html;q=0.9, application/json;q=0.5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json",
    );
}

#[tokio::test]
async fn get_feed_malformed_qvalue_discards_entry() {
    // RFC 7231 §5.3.1: a malformed q-value disqualifies the entry, so a
    // bogus `application/json;q=abc` must not outrank the well-formed
    // `application/xml`. Pin this so future refactors of
    // `parse_q_thousandths` can't silently regress to "treat malformed
    // as default 1.0", which would invert the client's preference.
    let scenario = FeedScenario::new(0x2f, 0xbe);
    let router = scenario.router(&[(0, [0xcd; 32], 7)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(header::ACCEPT, "application/json;q=abc, application/xml")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/xml",
    );
}

#[tokio::test]
async fn get_feed_uppercase_qvalue_param_is_recognised() {
    // RFC 7231 §3.1.1.1: parameter names are case-insensitive. A client
    // sending `Q=` must be honoured the same as `q=`; otherwise the
    // picker would silently treat the entry as "no q-value seen" and
    // give it the default 1.0, inverting the client's preference.
    let scenario = FeedScenario::new(0x31, 0xbf);
    let router = scenario.router(&[(0, [0xee; 32], 3)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(
                header::ACCEPT,
                "application/json;Q=0.1, application/xml;Q=0.9",
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/xml",
    );
}

#[tokio::test]
async fn get_feed_accept_wildcard_falls_back_to_octet_stream() {
    // `Accept: */*` carries no preference; mirror bee and serve the
    // octet-stream body so existing browser clients keep working.
    let scenario = FeedScenario::new(0x2f, 0xbf);
    let router = scenario.router(&[(0, [0x42; 32], 1)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(header::ACCEPT, "*/*")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );
}

#[tokio::test]
async fn head_feed_returns_headers_no_body() {
    // `HEAD /feeds/...` runs the full lookup so existence-probe
    // clients can read `swarm-feed-index` and `Content-Length` without
    // paying for the 40-byte body transfer.
    let scenario = FeedScenario::new(0x30, 0xc0);
    let target = [0x77u8; 32];
    let router = scenario.router(&[(0, target, 5)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        "40",
        "HEAD must echo the GET body length",
    );
    let expected_id = ant_retrieval::sequence_update_id(&scenario.topic, 0);
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        hex::encode(expected_id).as_str(),
    );
    let body = body_bytes(resp).await;
    assert!(body.is_empty(), "HEAD body must be empty");
}

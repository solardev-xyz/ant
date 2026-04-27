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
                ControlCommand::GetBytes { max_bytes, ack, .. } => {
                    captured.lock().await.push(max_bytes);
                    let _ = ack
                        .send(ControlAck::Bytes {
                            data: b"stub".to_vec(),
                        })
                        .await;
                }
                ControlCommand::GetBzz { max_bytes, ack, .. } => {
                    captured.lock().await.push(max_bytes);
                    let _ = ack
                        .send(ControlAck::BzzBytes {
                            data: b"stub".to_vec(),
                            content_type: Some("text/plain".to_string()),
                            filename: None,
                        })
                        .await;
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
        "application/octet-stream",
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
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

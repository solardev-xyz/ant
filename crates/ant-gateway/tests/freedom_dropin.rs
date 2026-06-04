//! Freedom drop-in surface (PLAN.md Appendix J): the endpoints the
//! next Freedom release needs that landed in this change —
//! `GET /node` light-mode gate, `POST /bytes`, `POST /feeds`, the
//! `/tags` progress surface, and the real `/stamps` listing.
//!
//! These drive the same production `axum::Router` the binary serves,
//! via `tower::ServiceExt::oneshot`, against the in-memory fake node
//! loop (`handle_with_fixture_node`, which BMT-validates every pushed
//! chunk) or a bespoke dispatcher for the postage-status path.

mod common;

use ant_control::{ControlAck, ControlCommand, PostageStatusView};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{body_bytes, handle_with_fixture_node, router_with_dispatcher, send};
use serde_json::Value;

/// `GET /node` reports `beeMode:"light"` when the node is upload-capable
/// (the fixture handle sets `light_mode = true`). This is the gate
/// Freedom's `checkSwarmPreFlight` requires before it lets a page
/// publish anything (PLAN.md J.4.1).
#[tokio::test]
async fn node_reports_light_mode_when_upload_capable() {
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/node")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["beeMode"], "light");
    assert_eq!(json["chequebookEnabled"], true);
    assert_eq!(json["swapEnabled"], true);
}

/// `POST /bytes` splits + pushsyncs a raw payload and returns a bare
/// `/bytes` reference (no manifest), plus a `Swarm-Tag-Uid` header that
/// resolves to a fully-synced tag (PLAN.md J.2.5 / C1 + C3).
#[tokio::test]
async fn post_bytes_returns_reference_and_completed_tag() {
    let router = handle_with_fixture_node();
    let body = b"feed payload over 0 bytes but under one chunk".to_vec();
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::POST)
            .uri("/bytes")
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(body))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let tag_uid = resp
        .headers()
        .get("swarm-tag-uid")
        .expect("swarm-tag-uid header present")
        .to_str()
        .unwrap()
        .to_string();
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let reference = json["reference"].as_str().expect("reference string");
    assert_eq!(reference.len(), 64, "reference is 32-byte hex");

    // The tag bee-js would poll resolves to "fully synced".
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/tags/{tag_uid}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let tag: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(tag["total"].as_u64().unwrap() >= 1);
    assert_eq!(tag["synced"], tag["total"]);
    assert_eq!(tag["split"], tag["total"]);
    assert_eq!(tag["address"], reference);
}

/// `POST /tags` then `GET /tags/{uid}` round-trips a bee-shaped tag.
#[tokio::test]
async fn create_and_get_tag() {
    let router = handle_with_fixture_node();
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::POST)
            .uri("/tags")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let uid = json["uid"].as_u64().expect("uid");
    assert!(json["startedAt"].is_string());

    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/tags/{uid}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// `GET /tags/{uid}` for an unknown uid is a clean 404 (bee-shaped),
/// not a 500.
#[tokio::test]
async fn get_unknown_tag_is_404() {
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/tags/424242")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `POST /feeds/{owner}/{topic}` builds + pushes a feed manifest and
/// returns its root reference (PLAN.md J.2.5 / C2).
#[tokio::test]
async fn post_feeds_creates_manifest() {
    let router = handle_with_fixture_node();
    let owner = "0102030405060708090a0b0c0d0e0f1011121314";
    let topic = "2222222222222222222222222222222222222222222222222222222222222222";
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(format!("/feeds/{owner}/{topic}"))
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(
        json["reference"].as_str().expect("reference").len(),
        64,
        "manifest root is 32-byte hex",
    );
}

/// `POST /feeds` with `?type=epoch` is rejected 501 (only sequence
/// feeds are supported, matching the read path).
#[tokio::test]
async fn post_feeds_epoch_is_501() {
    let router = handle_with_fixture_node();
    let owner = "0102030405060708090a0b0c0d0e0f1011121314";
    let topic = "2222222222222222222222222222222222222222222222222222222222222222";
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(format!("/feeds/{owner}/{topic}?type=epoch"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

/// A configured batch is surfaced by `GET /stamps` as a usable,
/// bee-shaped stamp so Freedom's pre-flight lets publishing proceed
/// (PLAN.md J.5.B). The `batchID` drops the `0x` prefix bee never uses.
const TEST_BATCH_0X: &str = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778800";
const TEST_BATCH_BARE: &str =
    "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778800";

fn enabled_postage_router() -> axum::Router {
    router_with_dispatcher(|cmd| async move {
        if let ControlCommand::PostageList { ack } = cmd {
            let view = PostageStatusView {
                enabled: true,
                batch_id: TEST_BATCH_0X.to_string(),
                batch_depth: 24,
                bucket_depth: 16,
                immutable: false,
                bucket_fill_max: 7,
                ..PostageStatusView::default()
            };
            let _ = ack.send(ControlAck::PostageList(vec![view]));
        }
    })
}

#[tokio::test]
async fn stamps_lists_configured_batch_as_usable() {
    let router = enabled_postage_router();
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/stamps")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let stamps = json["stamps"].as_array().expect("stamps array");
    assert_eq!(stamps.len(), 1);
    assert_eq!(stamps[0]["batchID"], TEST_BATCH_BARE);
    assert_eq!(stamps[0]["usable"], true);
    assert_eq!(stamps[0]["depth"], 24);
    assert_eq!(stamps[0]["bucketDepth"], 16);
    assert_eq!(stamps[0]["immutableFlag"], false);
    assert_eq!(stamps[0]["exists"], true);
    assert_eq!(stamps[0]["utilization"], 7);
}

#[tokio::test]
async fn stamp_by_id_matches_then_404() {
    let router = enabled_postage_router();
    // Matching id → 200.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/stamps/{TEST_BATCH_BARE}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["batchID"], TEST_BATCH_BARE);

    // Unknown id → 404.
    let other = "1111111111111111111111111111111111111111111111111111111111111111";
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/stamps/{other}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// When uploads are disabled (no batch configured), `GET /stamps`
/// returns the empty list bee emits on a fresh node — Freedom then
/// shows its "buy storage" CTA rather than crashing.
#[tokio::test]
async fn stamps_empty_when_uploads_disabled() {
    let router = router_with_dispatcher(|cmd| async move {
        if let ControlCommand::PostageList { ack } = cmd {
            let _ = ack.send(ControlAck::PostageList(Vec::new()));
        }
    });
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/stamps")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["stamps"].as_array().expect("stamps array").len(), 0);
}

//! D.2.5 catch-all 501 regression.
//!
//! Anything outside the Tier-A column lands on `not_implemented`. Bee
//! consumers branch on the JSON `code` and `message` fields, so we lock
//! the exact wire shape here. If a future refactor turns the fallback
//! into a 404 or changes the body, this test goes red.

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{body_bytes, send, snapshot_with_one_peer, status_only_router};
use serde_json::Value;

#[tokio::test]
async fn unknown_path_returns_structured_501() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/transactions")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 501);
    assert_eq!(json["message"], "not implemented in ant");
}

/// Tier-B endpoints that bee defines as POST/DELETE/PATCH (writes,
/// stamps mutation, peer control) are out of scope; verify they all
/// drop into the fallback regardless of method.
#[tokio::test]
async fn tier_b_writes_fall_through_to_501() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bytes")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 501);
    assert_eq!(json["message"], "not implemented in ant");
}

/// `GET /pss/...` is permanently-501 per PLAN.md Appendix B; verify the
/// catch-all covers it.
#[tokio::test]
async fn pss_send_falls_through_to_501() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/pss/send/topic/0xabcd")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

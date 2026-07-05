//! `/pins` surface integration tests: bee `pkg/api/pin.go` shapes
//! (also covered end-to-end by the conformance differential's `pins`
//! scenario) plus `GET /pins/check`, which beemock cannot exercise
//! (bee's `pinIntegrityHandler` needs a real `storer.PinIntegrity`
//! backed by a transactional store the API mocks don't provide), so
//! its NDJSON shape is pinned here against the fixture node.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{body_bytes, handle_with_fixture_node};
use tower::ServiceExt;

/// Data reference of the fixture's PNG (see `tests/retrieval.rs`): a
/// complete 5-chunk file tree — the fixture's *manifest* root can't be
/// pinned because the captured chunk set only covers one site path,
/// and fetch-then-pin correctly refuses partially-present content.
const PIN_ROOT: &str = "285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296";
const MISSING_REF: &str = "00000000000000000000000000000000000000000000000000000000000000ff";

async fn send(router: &axum::Router, method: &str, uri: &str) -> (StatusCode, Vec<u8>) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    let status = resp.status();
    (status, body_bytes(resp).await)
}

#[tokio::test]
async fn pin_lifecycle_matches_bee_shapes() {
    let router = handle_with_fixture_node();

    // Fresh pin → bee's jsonhttp.Created(w, nil).
    let (status, body) = send(&router, "POST", &format!("/pins/{PIN_ROOT}")).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(body, br#"{"code":201,"message":"Created"}"#);

    // Idempotent re-pin → 200 OK.
    let (status, body) = send(&router, "POST", &format!("/pins/{PIN_ROOT}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, br#"{"code":200,"message":"OK"}"#);

    // GET echoes the reference.
    let (status, body) = send(&router, "GET", &format!("/pins/{PIN_ROOT}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, format!(r#"{{"reference":"{PIN_ROOT}"}}"#).as_bytes());

    // Listing shows it.
    let (status, body) = send(&router, "GET", "/pins").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        format!(r#"{{"references":["{PIN_ROOT}"]}}"#).as_bytes()
    );

    // The integrity check reports one healthy pin, NDJSON, bee's field
    // order: reference, total, missing, invalid.
    let (status, body) = send(&router, "GET", "/pins/check").await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf8 ndjson");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1, "one pin, one NDJSON line: {text:?}");
    let v: serde_json::Value = serde_json::from_str(lines[0]).expect("ndjson line parses");
    assert_eq!(v["reference"], PIN_ROOT);
    assert!(v["total"].as_u64().expect("total") > 0);
    assert_eq!(v["missing"], 0);
    assert_eq!(v["invalid"], 0);
    assert!(
        lines[0].starts_with(r#"{"reference":"#),
        "bee's PinIntegrityResponse leads with reference: {}",
        lines[0]
    );

    // ?ref= narrows to one pin; an unpinned ref yields no lines.
    let (status, body) = send(&router, "GET", &format!("/pins/check?ref={PIN_ROOT}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(String::from_utf8(body).expect("utf8").lines().count(), 1);
    let (status, body) = send(&router, "GET", &format!("/pins/check?ref={MISSING_REF}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty(), "unpinned ref reports nothing");

    // Unpin → 200; the reference is gone afterwards (404 Not Found,
    // bee's jsonhttp.NotFound(w, nil) shape).
    let (status, body) = send(&router, "DELETE", &format!("/pins/{PIN_ROOT}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, br#"{"code":200,"message":"OK"}"#);
    let (status, body) = send(&router, "GET", &format!("/pins/{PIN_ROOT}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, br#"{"code":404,"message":"Not Found"}"#);
    let (status, _) = send(&router, "DELETE", &format!("/pins/{PIN_ROOT}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Pinning unreachable content is bee's 404 "pin collection failed".
    let (status, body) = send(&router, "POST", &format!("/pins/{MISSING_REF}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, br#"{"code":404,"message":"pin collection failed"}"#);

    // Malformed reference: bee's mapStructure path-param shape with the
    // `reference` field name.
    let (status, body) = send(&router, "POST", "/pins/zznothex").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(v["message"], "invalid path params");
    assert_eq!(v["reasons"][0]["field"], "reference");
}

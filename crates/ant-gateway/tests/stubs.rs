//! D.2.4 stub endpoints. Bee-shaped zero-balance bodies so `bee-js` UIs
//! degrade cleanly. Field names and value types are pinned to bee.

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{body_bytes, send, snapshot_with_one_peer, status_only_router};
use serde_json::Value;

#[tokio::test]
async fn wallet_returns_zero_balances_on_gnosis() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/wallet")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["bzzBalance"], "0");
    assert_eq!(json["nativeTokenBalance"], "0");
    assert_eq!(json["chainID"], 100);
}

// `GET /stamps` is no longer a stub — it now reports the daemon's
// configured postage batch over the control channel. Its empty-list
// (uploads-disabled) and populated-batch behaviour are covered by the
// `freedom_dropin` integration tests, which attach a node dispatcher.

#[tokio::test]
async fn chequebook_address_zeroed() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/chequebook/address")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(
        json["chequebookAddress"],
        "0x0000000000000000000000000000000000000000",
    );
}

#[tokio::test]
async fn chequebook_balance_zero() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/chequebook/balance")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["totalBalance"], "0");
    assert_eq!(json["availableBalance"], "0");
}

//! D.2.2 status endpoints.
//!
//! Each test pins both the HTTP status and the JSON shape the bee-js
//! consumers index by. Where bee uses `camelCase`, the assertion uses
//! the verbatim wire field name (not the Rust struct field) — that's
//! the contract that makes `bee-js` reach `bzz` over us.

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{
    body_bytes, empty_snapshot, send, snapshot_with_one_peer, status_only_router, test_identity,
};
use serde_json::Value;

#[tokio::test]
async fn health_returns_ok_status_with_versions() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["version"], "antd/test");
    assert_eq!(json["apiVersion"], "7.2.0");
}

/// `/readiness` answers 200 once we have at least one BZZ-handshaked peer.
#[tokio::test]
async fn readiness_200_when_peer_handshaked() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/readiness")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// `/readiness` answers 503 when no peer has handshaked yet.
#[tokio::test]
async fn readiness_503_when_no_peers() {
    let router = status_only_router(empty_snapshot());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/readiness")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn node_returns_ultra_light_mode() {
    let router = status_only_router(snapshot_with_one_peer());
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
    assert_eq!(json["beeMode"], "ultra-light");
    assert_eq!(json["gatewayMode"], false);
    assert_eq!(json["chequebookEnabled"], false);
    assert_eq!(json["swapEnabled"], false);
}

/// `/addresses` echoes the static `GatewayIdentity`. Overlay/publicKey
/// drop the `0x` prefix, ethereum keeps it. `pssPublicKey` is required
/// to be present (bee-js panics on `undefined`); we mirror `publicKey`
/// because we don't run a separate PSS key.
#[tokio::test]
async fn addresses_renders_identity_and_listeners() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/addresses")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let identity = test_identity();
    assert_eq!(json["overlay"], identity.overlay_hex);
    assert_eq!(json["ethereum"], identity.ethereum_hex);
    assert_eq!(json["publicKey"], identity.public_key_hex);
    assert_eq!(json["pssPublicKey"], identity.public_key_hex);
    let underlay = json["underlay"].as_array().expect("underlay array");
    assert!(
        underlay.iter().any(|v| v == "/ip4/127.0.0.1/tcp/1634"),
        "underlay should include the listener: {underlay:?}"
    );
}

/// `/peers` reports BZZ-handshaked peers using their **swarm overlay**
/// (no `0x` prefix), not the libp2p peer id. Pre-handshake peers are
/// filtered out.
#[tokio::test]
async fn peers_lists_handshaked_overlays() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/peers")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let peers = json["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 1);
    assert_eq!(
        peers[0]["address"],
        "deadbeef00000000000000000000000000000000000000000000000000000000",
    );
    assert_eq!(peers[0]["fullNode"], true);
}

/// `/topology` mirrors bee's kademlia snapshot layout: a `bins` map with
/// 32 named entries (`bin_0` … `bin_31`) plus the meta fields.
#[tokio::test]
async fn topology_emits_32_bins_and_summary_fields() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/topology")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let identity = test_identity();
    assert_eq!(json["baseAddr"], identity.overlay_hex);
    assert_eq!(json["population"], 1);
    assert_eq!(json["connected"], 1);
    let bins = json["bins"].as_object().expect("bins map");
    assert_eq!(bins.len(), 32, "bee snapshot is fixed at 32 bins");
    for i in 0..32 {
        let key = format!("bin_{i}");
        assert!(bins.contains_key(&key), "missing bin {key}");
    }
    assert_eq!(bins["bin_5"]["population"], 1);
    assert_eq!(bins["bin_5"]["connected"], 1);
    assert_eq!(bins["bin_0"]["population"], 0);
    assert!(json["timestamp"].is_string());
    assert!(json["nnLowWatermark"].is_number());
    assert!(json["depth"].is_number());
    assert!(json["reachability"].is_string());
    assert!(json["networkAvailability"].is_string());
}

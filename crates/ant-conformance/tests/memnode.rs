//! End-to-end HTTP round-trips against the in-process, memory-backed
//! gateway ([`ant_conformance::spawn_mem_gateway`]).
//!
//! Every test drives the **production** router over a real TCP socket
//! on an ephemeral `127.0.0.1` port — no `tower::oneshot` shortcut —
//! so the serving path (axum listener, body streaming, headers) is the
//! exact one the conformance harness will diff against bee. Where a
//! request needs bee-derived inputs (SOC signatures, feed-update
//! chunks) they come from the checked-in vector files, i.e. from the
//! canonical bee implementation itself.

use ant_conformance::{
    load, spawn_mem_gateway, unhex, FeedUpdateVectors, MemGatewayConfig, SocVectors,
};

/// Any well-formed 32-byte hex batch id: `MemNode` stamps everything.
const BATCH_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// SOC wire header: `id(32) ‖ signature(65)`. `POST /soc` takes only
/// the inner CAC (`span ‖ payload`) as the body; the id and signature
/// travel in the path / query.
const SOC_HEADER: usize = 32 + 65;

fn reference_from(body: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(body).expect("upload response is JSON");
    v["reference"]
        .as_str()
        .expect("upload response carries a reference")
        .to_string()
}

/// (a) `POST /bzz` single file → 201 + reference; `GET /bzz/{ref}/`
/// returns the original body.
#[tokio::test]
async fn bzz_single_file_round_trip() {
    let gw = spawn_mem_gateway(MemGatewayConfig::default()).await;
    let client = reqwest::Client::new();
    let body = b"hello from the in-memory conformance gateway".to_vec();

    let resp = client
        .post(format!("{}/bzz", gw.base_url()))
        .header("swarm-postage-batch-id", BATCH_ID)
        .header("content-type", "text/plain")
        .body(body.clone())
        .send()
        .await
        .expect("POST /bzz");
    assert_eq!(resp.status().as_u16(), 201, "POST /bzz must return 201");
    let reference = reference_from(&resp.bytes().await.expect("read POST /bzz body"));
    assert_eq!(reference.len(), 64, "reference must be bare 64-hex");

    let resp = client
        .get(format!("{}/bzz/{reference}/", gw.base_url()))
        .send()
        .await
        .expect("GET /bzz/{ref}/");
    assert_eq!(resp.status().as_u16(), 200, "GET /bzz/{{ref}}/ must be 200");
    let got = resp.bytes().await.expect("read GET /bzz body");
    assert_eq!(
        got.as_ref(),
        body.as_slice(),
        "bzz round-trip body mismatch"
    );

    drop(client);
    gw.shutdown().await;
}

/// (b) `POST /chunks` + `GET /chunks/{addr}` round-trip, pinned against
/// the bee-derived CAC vectors: the returned reference must equal bee's
/// address for the same wire bytes.
#[tokio::test]
async fn chunks_round_trip_matches_bee_vectors() {
    let vectors: ant_conformance::CacVectors = load("cac.json");
    let gw = spawn_mem_gateway(MemGatewayConfig::default()).await;
    let client = reqwest::Client::new();

    for case in &vectors.cases {
        let wire = unhex("chunkDataHex", &case.chunk_data_hex);
        let resp = client
            .post(format!("{}/chunks", gw.base_url()))
            .header("swarm-postage-batch-id", BATCH_ID)
            .body(wire.clone())
            .send()
            .await
            .expect("POST /chunks");
        assert_eq!(
            resp.status().as_u16(),
            201,
            "POST /chunks 201 for case {}",
            case.name
        );
        let reference = reference_from(&resp.bytes().await.expect("read POST /chunks body"));
        assert_eq!(
            reference, case.address_hex,
            "reference must match bee's address for case {}",
            case.name
        );

        let resp = client
            .get(format!("{}/chunks/{reference}", gw.base_url()))
            .send()
            .await
            .expect("GET /chunks/{addr}");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "GET /chunks 200 for case {}",
            case.name
        );
        let got = resp.bytes().await.expect("read GET /chunks body");
        assert_eq!(
            got.as_ref(),
            wire.as_slice(),
            "chunk wire round-trip mismatch for case {}",
            case.name
        );
    }

    drop(client);
    gw.shutdown().await;
}

/// (c) `POST /soc/{owner}/{id}?sig=…` + `GET /soc/{owner}/{id}`
/// round-trip using bee-signed SOCs from the vector file. The POST body
/// is the inner CAC (`chunkDataHex` minus the 32-byte id and 65-byte
/// signature prefix); the GET must return the **unwrapped payload**
/// (bee's `socGetHandler` serves the wrapped chunk's content) with the
/// SOC signature echoed in `swarm-soc-signature`.
#[tokio::test]
async fn soc_round_trip_with_bee_signed_vectors() {
    let vectors: SocVectors = load("soc.json");
    let gw = spawn_mem_gateway(MemGatewayConfig::default()).await;
    let client = reqwest::Client::new();

    for case in &vectors.cases {
        let wire = unhex("chunkDataHex", &case.chunk_data_hex);
        let inner_cac = wire[SOC_HEADER..].to_vec();

        let resp = client
            .post(format!(
                "{}/soc/{}/{}?sig={}",
                gw.base_url(),
                case.owner_hex,
                case.id_hex,
                case.signature_hex
            ))
            .header("swarm-postage-batch-id", BATCH_ID)
            .body(inner_cac.clone())
            .send()
            .await
            .expect("POST /soc");
        assert_eq!(
            resp.status().as_u16(),
            201,
            "POST /soc 201 for case {}",
            case.name
        );
        let reference = reference_from(&resp.bytes().await.expect("read POST /soc body"));
        assert_eq!(
            reference, case.soc_address_hex,
            "reference must match bee's SOC address for case {}",
            case.name
        );

        let resp = client
            .get(format!(
                "{}/soc/{}/{}",
                gw.base_url(),
                case.owner_hex,
                case.id_hex
            ))
            .send()
            .await
            .expect("GET /soc");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "GET /soc 200 for case {}",
            case.name
        );
        let sig_header = resp
            .headers()
            .get("swarm-soc-signature")
            .expect("swarm-soc-signature header present")
            .to_str()
            .expect("ascii signature")
            .to_string();
        assert_eq!(
            sig_header,
            case.signature_hex.trim_start_matches("0x"),
            "swarm-soc-signature must echo the SOC signature for case {}",
            case.name
        );
        let got = resp.bytes().await.expect("read GET /soc body");
        // Bee serves the wrapped chunk's *content*: the inner CAC minus
        // its 8-byte span (single-chunk payloads in the vector file).
        assert_eq!(
            got.as_ref(),
            &inner_cac[8..],
            "SOC payload round-trip mismatch for case {}",
            case.name
        );
    }

    drop(client);
    gw.shutdown().await;
}

/// (d) Feed round-trip: publish the bee-signed v2 feed-update chunk via
/// `POST /soc`, then resolve it through `GET /feeds/{owner}/{topic}`.
/// The body must equal the update's payload and the bee feed headers
/// must be present.
#[tokio::test]
async fn feed_round_trip_resolves_v2_update() {
    let vectors: FeedUpdateVectors = load("feed_updates.json");
    let case = vectors
        .cases
        .iter()
        .find(|c| c.kind == "v2")
        .expect("feed_updates.json carries a v2 case");
    assert_eq!(case.index, "0", "the v2 vector must be the anchor update");

    let gw = spawn_mem_gateway(MemGatewayConfig::default()).await;
    let client = reqwest::Client::new();

    let wire = unhex("chunkDataHex", &case.chunk_data_hex);
    let inner_cac = wire[SOC_HEADER..].to_vec();

    let resp = client
        .post(format!(
            "{}/soc/{}/{}?sig={}",
            gw.base_url(),
            case.owner_hex,
            case.id_hex,
            case.signature_hex
        ))
        .header("swarm-postage-batch-id", BATCH_ID)
        .body(inner_cac)
        .send()
        .await
        .expect("POST /soc (feed update)");
    assert_eq!(resp.status().as_u16(), 201, "feed-update POST must be 201");
    assert_eq!(
        reference_from(&resp.bytes().await.expect("read POST body")),
        case.soc_address_hex,
        "feed-update reference must match bee's SOC address"
    );

    let resp = client
        .get(format!(
            "{}/feeds/{}/{}",
            gw.base_url(),
            case.owner_hex,
            case.topic_hex
        ))
        .send()
        .await
        .expect("GET /feeds");
    assert_eq!(resp.status().as_u16(), 200, "GET /feeds must be 200");

    let feed_index = resp
        .headers()
        .get("swarm-feed-index")
        .expect("swarm-feed-index header must be present")
        .to_str()
        .expect("swarm-feed-index is ascii")
        .to_string();
    assert_eq!(
        feed_index, "0000000000000000",
        "index 0 must render as 8-byte big-endian hex"
    );
    assert_eq!(
        resp.headers()
            .get("swarm-feed-resolved-version")
            .and_then(|v| v.to_str().ok()),
        Some("v2"),
        "the update must resolve as v2"
    );

    let payload = unhex("payloadHex", &case.payload_hex);
    let got = resp.bytes().await.expect("read GET /feeds body");
    assert_eq!(
        got.as_ref(),
        payload.as_slice(),
        "feed body must be the v2 update's payload"
    );

    drop(client);
    gw.shutdown().await;
}

/// (e) `swarm-encrypt: true` end-to-end over the production gateway:
/// encrypted `/bytes` and `/bzz` uploads return 128-hex references that
/// round-trip (including a Range slice and the encrypted mantaray walk
/// by explicit path), and a feed whose v1 update wraps the 64-byte
/// encrypted reference serves the *decrypted* content on `GET /feeds`
/// — the ant-side correctness assertion behind the registered
/// `encryption/feed-latest` divergence (bee's own handler mis-serves
/// this case; see conformance/divergences.json).
#[tokio::test]
async fn encrypted_uploads_round_trip_and_feed_decrypts() {
    let gw = spawn_mem_gateway(MemGatewayConfig::default()).await;
    let client = reqwest::Client::new();

    // Encrypted /bytes (multi-chunk).
    let bytes_body = b"encrypted memnode bytes payload\n".repeat(400);
    let resp = client
        .post(format!("{}/bytes", gw.base_url()))
        .header("swarm-postage-batch-id", BATCH_ID)
        .header("swarm-encrypt", "true")
        .body(bytes_body.clone())
        .send()
        .await
        .expect("POST /bytes");
    assert_eq!(resp.status().as_u16(), 201);
    let bytes_ref = reference_from(&resp.bytes().await.expect("body"));
    assert_eq!(bytes_ref.len(), 128, "encrypted reference must be 128 hex");

    let resp = client
        .get(format!("{}/bytes/{bytes_ref}", gw.base_url()))
        .send()
        .await
        .expect("GET /bytes");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.bytes().await.expect("body").as_ref(),
        bytes_body.as_slice()
    );

    // Range over encrypted content.
    let resp = client
        .get(format!("{}/bytes/{bytes_ref}", gw.base_url()))
        .header("range", "bytes=10-41")
        .send()
        .await
        .expect("GET /bytes range");
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(
        resp.bytes().await.expect("body").as_ref(),
        &bytes_body[10..=41]
    );

    // Encrypted /bzz with explicit path + HEAD.
    let file_body = b"encrypted memnode file body".to_vec();
    let resp = client
        .post(format!("{}/bzz?name=enc.txt", gw.base_url()))
        .header("swarm-postage-batch-id", BATCH_ID)
        .header("content-type", "text/plain")
        .header("swarm-encrypt", "true")
        .body(file_body.clone())
        .send()
        .await
        .expect("POST /bzz");
    assert_eq!(resp.status().as_u16(), 201);
    let file_ref = reference_from(&resp.bytes().await.expect("body"));
    assert_eq!(file_ref.len(), 128);

    for path in ["", "enc.txt"] {
        let resp = client
            .get(format!("{}/bzz/{file_ref}/{path}", gw.base_url()))
            .send()
            .await
            .expect("GET /bzz");
        assert_eq!(resp.status().as_u16(), 200, "path {path:?}");
        assert_eq!(
            resp.bytes().await.expect("body").as_ref(),
            file_body.as_slice(),
            "path {path:?}"
        );
    }
    let resp = client
        .head(format!("{}/bzz/{file_ref}/", gw.base_url()))
        .send()
        .await
        .expect("HEAD /bzz");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok()),
        Some(file_body.len().to_string().as_str()),
        "HEAD must report the decrypted length"
    );

    // Feed wrapping the encrypted /bytes reference (bee's 80-byte v1
    // payload): GET /feeds must serve the decrypted content.
    let secret = ant_crypto::keccak256(b"memnode/enc-feed/key");
    let sk = k256::ecdsa::SigningKey::from_bytes(&secret.into()).expect("valid secret");
    let owner = ant_crypto::ethereum_address_from_public_key(sk.verifying_key());
    let topic = ant_crypto::keccak256(b"memnode/enc-feed/topic");
    let id = ant_retrieval::sequence_update_id(&topic, 0);
    let mut payload = 1_720_000_000u64.to_be_bytes().to_vec();
    payload.extend_from_slice(&unhex("bytes_ref", &bytes_ref));
    let (cac_addr, cac_wire) = ant_crypto::cac_new(&payload).expect("cac");
    let mut digest_input = Vec::with_capacity(64);
    digest_input.extend_from_slice(&id);
    digest_input.extend_from_slice(&cac_addr);
    let sig = ant_crypto::sign_handshake_data(&secret, &ant_crypto::keccak256(&digest_input))
        .expect("sign");
    let resp = client
        .post(format!(
            "{}/soc/{}/{}?sig={}",
            gw.base_url(),
            hex::encode(owner),
            hex::encode(id),
            hex::encode(sig)
        ))
        .header("content-type", "application/octet-stream")
        .header("swarm-postage-batch-id", BATCH_ID)
        .body(cac_wire)
        .send()
        .await
        .expect("POST /soc");
    assert_eq!(resp.status().as_u16(), 201);

    let resp = client
        .get(format!(
            "{}/feeds/{}/{}",
            gw.base_url(),
            hex::encode(owner),
            hex::encode(topic)
        ))
        .send()
        .await
        .expect("GET /feeds");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers()
            .get("swarm-feed-resolved-version")
            .and_then(|v| v.to_str().ok()),
        Some("v1"),
        "80-byte wrapped payload must resolve as legacy v1"
    );
    assert_eq!(
        resp.bytes().await.expect("body").as_ref(),
        bytes_body.as_slice(),
        "feed must serve the decrypted content"
    );

    drop(client);
    gw.shutdown().await;
}

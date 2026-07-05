//! `/chunks/stream` WebSocket conformance: drive ant's production
//! gateway (`MemNode` rig) and bee's real API layer (beemock) over a live
//! WebSocket with the same frames and assert identical protocol
//! behaviour — per-chunk empty-binary acks, retrievability of every
//! acked chunk over plain HTTP, pre-signed-stamp mode, and bee's close
//! semantics on protocol errors.
//!
//! The differential harness's `Step` model is plain HTTP, so the WS
//! flow lives in this dedicated test. Requires the beemock binary
//! (skips with a notice when absent), exactly like
//! `tests/differential.rs`.

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

struct Beemock {
    child: Child,
    base: String,
    batch_id: String,
}

impl Drop for Beemock {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn beemock_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("BEEMOCK_BIN") {
        let p = PathBuf::from(p);
        return p.exists().then_some(p);
    }
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/beemock/beemock");
    p.exists().then_some(p)
}

#[derive(Deserialize)]
struct BeemockHello {
    listening: String,
    #[serde(rename = "batchId")]
    batch_id: String,
}

async fn spawn_beemock(bin: &PathBuf) -> Beemock {
    // Offset from the differential test's port so parallel test
    // binaries don't collide.
    let port = 30000 + (std::process::id() % 10000) as u16;
    let mut child = Command::new(bin)
        .arg("-addr")
        .arg(format!("127.0.0.1:{port}"))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn beemock");
    let stdout = child.stdout.take().expect("beemock stdout");
    let hello_line = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufReader::new(stdout).read_line(&mut line).ok();
        line
    })
    .await
    .expect("join stdout reader");
    let hello: BeemockHello =
        serde_json::from_str(hello_line.trim()).expect("parse beemock hello line");
    let base = format!("http://{}", hello.listening);
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(r) = client.get(format!("{base}/health")).send().await {
            if r.status().is_success() {
                return Beemock {
                    child,
                    base,
                    batch_id: hello.batch_id,
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("beemock did not become healthy at {base}");
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Open `/chunks/stream` on `base` (an `http://…` URL) with the given
/// extra headers.
async fn connect_stream(base: &str, headers: &[(&str, String)]) -> WsStream {
    let url = format!("{}/chunks/stream", base.replace("http://", "ws://"));
    let mut request = url.into_client_request().expect("build ws request");
    for (name, value) in headers {
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(name.as_bytes())
                .expect("header name"),
            value.parse().expect("header value"),
        );
    }
    let (stream, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("websocket upgrade");
    assert_eq!(response.status().as_u16(), 101, "upgrade must succeed");
    stream
}

/// One chunk wire: `span(8 LE) ‖ payload`.
fn chunk_wire(payload: &[u8]) -> (Vec<u8>, [u8; 32]) {
    let mut wire = (payload.len() as u64).to_le_bytes().to_vec();
    wire.extend_from_slice(payload);
    let mut span = [0u8; 8];
    span.copy_from_slice(&wire[..8]);
    let addr = ant_crypto::bmt::bmt_hash_with_span(&span, payload).expect("bmt");
    (wire, addr)
}

/// Send binary frames and expect bee's empty-binary ack after each.
async fn send_and_ack(ws: &mut WsStream, frames: &[Vec<u8>], backend: &str) {
    for (i, frame) in frames.iter().enumerate() {
        ws.send(Message::Binary(frame.clone().into()))
            .await
            .unwrap_or_else(|e| panic!("{backend}: send frame {i}: {e}"));
        let ack = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await
            .unwrap_or_else(|_| panic!("{backend}: ack {i} timed out"))
            .unwrap_or_else(|| panic!("{backend}: stream ended before ack {i}"))
            .unwrap_or_else(|e| panic!("{backend}: ack {i} errored: {e}"));
        match ack {
            Message::Binary(data) => {
                assert!(
                    data.is_empty(),
                    "{backend}: per-chunk ack must be bee's empty binary message, got {} bytes",
                    data.len()
                );
            }
            other => panic!("{backend}: expected empty binary ack, got {other:?}"),
        }
    }
}

/// Expect the next frame to be a Close with `code` and `reason`.
async fn expect_close(ws: &mut WsStream, code: u16, reason: &str, backend: &str) {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await
            .unwrap_or_else(|_| panic!("{backend}: close frame timed out"))
            .unwrap_or_else(|| panic!("{backend}: stream ended without close frame"))
            .unwrap_or_else(|e| panic!("{backend}: read errored: {e}"));
        match msg {
            Message::Close(Some(frame)) => {
                assert_eq!(
                    u16::from(frame.code),
                    code,
                    "{backend}: close code (reason {:?})",
                    frame.reason
                );
                assert_eq!(frame.reason.as_str(), reason, "{backend}: close reason");
                return;
            }
            Message::Close(None) => panic!("{backend}: close frame carried no code/reason"),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("{backend}: expected close frame, got {other:?}"),
        }
    }
}

async fn get_chunk(client: &reqwest::Client, base: &str, addr: [u8; 32]) -> (u16, Vec<u8>) {
    let resp = client
        .get(format!("{base}/chunks/{}", hex::encode(addr)))
        .send()
        .await
        .expect("GET /chunks");
    let status = resp.status().as_u16();
    let body = resp.bytes().await.expect("chunk body").to_vec();
    (status, body)
}

#[tokio::test]
async fn chunk_stream_websocket_matches_bee() {
    let Some(bin) = beemock_bin() else {
        eprintln!(
            "SKIP chunk_stream_websocket_matches_bee: beemock binary not found; \
             build it with `cd conformance/beemock && go build -o beemock .` \
             or set BEEMOCK_BIN"
        );
        return;
    };
    let bee = spawn_beemock(&bin).await;
    let ant =
        ant_conformance::spawn_mem_gateway(ant_conformance::MemGatewayConfig::default()).await;
    let client = reqwest::Client::new();

    let chunks: Vec<(Vec<u8>, [u8; 32])> = (0u8..4)
        .map(|i| chunk_wire(format!("ws chunk stream payload {i}").as_bytes()))
        .collect();
    let frames: Vec<Vec<u8>> = chunks.iter().map(|(w, _)| w.clone()).collect();

    // --- batch-header mode: same frames in, same acks, chunks
    // retrievable over plain HTTP afterwards -------------------------
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let mut ws =
            connect_stream(&base, &[("swarm-postage-batch-id", bee.batch_id.clone())]).await;
        send_and_ack(&mut ws, &frames, name).await;
        let _ = ws.close(None).await;
        for (wire, addr) in &chunks {
            let (status, body) = get_chunk(&client, &base, *addr).await;
            assert_eq!(status, 200, "{name}: streamed chunk must be retrievable");
            assert_eq!(&body, wire, "{name}: retrieved wire must match upload");
        }
    }

    // --- pre-signed-stamp mode: no batch header; each frame carries a
    // 113-byte stamp minted by that backend's own POST /envelope ------
    let (stamped_wire, stamped_addr) = chunk_wire(b"ws stamped chunk payload");
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let env: serde_json::Value = client
            .post(format!("{base}/envelope/{}", hex::encode(stamped_addr)))
            .header("swarm-postage-batch-id", &bee.batch_id)
            .send()
            .await
            .expect("POST /envelope")
            .bytes()
            .await
            .map(|b| serde_json::from_slice(&b).expect("envelope json"))
            .expect("envelope body");
        let field = |k: &str| env[k].as_str().unwrap_or_default().to_string();
        let stamp_hex = format!(
            "{}{}{}{}",
            bee.batch_id,
            field("index"),
            field("timestamp"),
            field("signature")
        );
        let stamp = hex::decode(&stamp_hex).expect("stamp hex");
        assert_eq!(stamp.len(), 113, "{name}: stamp must be 113 bytes");
        let mut frame = stamp;
        frame.extend_from_slice(&stamped_wire);

        let mut ws = connect_stream(&base, &[]).await;
        send_and_ack(&mut ws, &[frame], name).await;
        let _ = ws.close(None).await;
        let (status, body) = get_chunk(&client, &base, stamped_addr).await;
        assert_eq!(status, 200, "{name}: stamped chunk must be retrievable");
        assert_eq!(body, stamped_wire, "{name}: stamped wire round-trips");
    }

    // --- protocol errors: bee's exact close codes/reasons ------------
    // A text frame closes with 1003 "invalid message".
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let mut ws =
            connect_stream(&base, &[("swarm-postage-batch-id", bee.batch_id.clone())]).await;
        ws.send(Message::Text("not binary".into()))
            .await
            .expect("send text frame");
        expect_close(
            &mut ws,
            u16::from(CloseCode::Unsupported),
            "invalid message",
            name,
        )
        .await;
    }

    // An over-sized chunk wire closes with 1011 "invalid chunk data".
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let mut ws =
            connect_stream(&base, &[("swarm-postage-batch-id", bee.batch_id.clone())]).await;
        let mut oversized = (5000u64).to_le_bytes().to_vec();
        oversized.extend_from_slice(&vec![0u8; 5000]);
        ws.send(Message::Binary(oversized.into()))
            .await
            .expect("send oversized frame");
        expect_close(
            &mut ws,
            u16::from(CloseCode::Error),
            "invalid chunk data",
            name,
        )
        .await;
    }

    // Stamp-mode message too small for stamp + chunk: 1011 with bee's
    // verbatim reason.
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let mut ws = connect_stream(&base, &[]).await;
        ws.send(Message::Binary(vec![0u8; 50].into()))
            .await
            .expect("send undersized stamped frame");
        expect_close(
            &mut ws,
            u16::from(CloseCode::Error),
            "message too small for stamp + chunk",
            name,
        )
        .await;
    }

    // Unknown swarm-tag rejects before the upgrade: 404 "tag not found".
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let url = format!("{}/chunks/stream", base.replace("http://", "ws://"));
        let mut request = url.into_client_request().expect("build ws request");
        request
            .headers_mut()
            .insert("swarm-postage-batch-id", bee.batch_id.parse().unwrap());
        request
            .headers_mut()
            .insert("swarm-tag", "999999".parse().unwrap());
        match tokio_tungstenite::connect_async(request).await {
            Ok(_) => panic!("{name}: upgrade with unknown tag must fail"),
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status().as_u16(), 404, "{name}: unknown tag status");
                let body = resp.body().as_deref().unwrap_or_default();
                let v: serde_json::Value = serde_json::from_slice(body).expect("json error body");
                assert_eq!(v["message"], "tag not found", "{name}: unknown tag body");
            }
            Err(e) => panic!("{name}: unexpected upgrade error: {e}"),
        }
    }

    // swarm-tag interaction: create a tag over HTTP, stream one chunk
    // with it, and watch the tag's counters advance on both backends.
    for (name, base) in [("bee", bee.base.clone()), ("ant", ant.base_url())] {
        let tag: serde_json::Value = client
            .post(format!("{base}/tags"))
            .send()
            .await
            .expect("POST /tags")
            .bytes()
            .await
            .map(|b| serde_json::from_slice(&b).expect("tag json"))
            .expect("tag body");
        let uid = tag["uid"].as_u64().expect("tag uid");
        let (wire, _) = chunk_wire(format!("tagged ws chunk {name}").as_bytes());
        let mut ws = connect_stream(
            &base,
            &[
                ("swarm-postage-batch-id", bee.batch_id.clone()),
                ("swarm-tag", uid.to_string()),
            ],
        )
        .await;
        send_and_ack(&mut ws, &[wire], name).await;
        let _ = ws.close(None).await;
        // Both backends must still know the tag; ant's synchronous
        // pipeline reports the chunk fully synced. (beemock's mock
        // session counters stay zero — a mock artifact — so only
        // presence is asserted there.)
        let polled: serde_json::Value = client
            .get(format!("{base}/tags/{uid}"))
            .send()
            .await
            .expect("GET /tags/{uid}")
            .bytes()
            .await
            .map(|b| serde_json::from_slice(&b).expect("tag poll json"))
            .expect("tag poll body");
        assert_eq!(polled["uid"].as_u64(), Some(uid), "{name}: tag survives");
        if name == "ant" {
            assert_eq!(
                polled["synced"].as_u64(),
                Some(1),
                "ant: tag counted the chunk"
            );
        }
    }

    ant.shutdown().await;
}

//! GSOC/PSS subscription `WebSockets` — bee's `GET /gsoc/subscribe/{address}`
//! and `GET /pss/subscribe/{topic}`.
//!
//! Each endpoint opens a lurker subscription on the node (see
//! `ant_p2p::lurker`): the node resides in the relevant neighborhood,
//! pulls its bin live, decodes matching GSOC/PSS messages, and streams
//! their payloads back. This handler forwards each payload to the client
//! as a **binary** WebSocket frame — bee-compatible, so bee-js
//! `gsocSubscribe` / `pssSubscribe` work unmodified.
//!
//! - `/gsoc/subscribe/{address}`: `{address}` is the 32-byte SOC address
//!   (`keccak256(id ‖ owner)`) the app mined; the node watches exactly it
//!   and resides in its neighborhood.
//! - `/pss/subscribe/{topic}`: `{topic}` is the topic string
//!   (keccak256-hashed, bee's `NewTopic`); the node watches that topic in
//!   its own neighborhood (where PSS messages to this node land) and
//!   receives topic-broadcast messages.

use ant_control::{ControlAck, ControlCommand};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::debug;

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// Depth of the node→gateway message buffer for one subscription.
const SUB_CHANNEL_CAP: usize = 64;

/// Optional query for `/pss/subscribe`.
#[derive(Debug, Deserialize)]
pub struct PssSubscribeQuery {
    /// Rendezvous-neighborhood override (64-hex overlay). When set, the
    /// node lurks *this* neighborhood instead of its own overlay — the
    /// basis for many-to-many rooms that no participant owns: every client
    /// lurks the same topic-derived neighborhood and senders address
    /// messages there. Absent ⇒ the node's own neighborhood (directed PSS
    /// to this node).
    neighborhood: Option<String>,
}

/// `GET /gsoc/subscribe/{address}` upgrade handler.
pub async fn gsoc_subscribe(
    State(handle): State<GatewayHandle>,
    Path(address_hex): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(address) = parse_hex32(&address_hex) else {
        return json_error(StatusCode::BAD_REQUEST, "invalid gsoc address");
    };
    // Watch exactly this SOC address; reside in its neighborhood.
    let cmd_target = address;
    ws.on_upgrade(move |socket| {
        run_subscription(handle, socket, cmd_target, vec![address], Vec::new())
    })
}

/// `GET /pss/subscribe/{topic}` upgrade handler.
pub async fn pss_subscribe(
    State(handle): State<GatewayHandle>,
    Path(topic_str): Path<String>,
    Query(query): Query<PssSubscribeQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let topic = ant_crypto::pss::topic_from_string(&topic_str);
    // With `?neighborhood=`, lurk that rendezvous neighborhood; otherwise
    // all-zeros → the node's own neighborhood (directed PSS to this node).
    let target = match &query.neighborhood {
        Some(hex) => match parse_hex32(hex) {
            Some(t) => t,
            None => return json_error(StatusCode::BAD_REQUEST, "invalid neighborhood overlay"),
        },
        None => [0u8; 32],
    };
    ws.on_upgrade(move |socket| run_subscription(handle, socket, target, Vec::new(), vec![topic]))
}

/// Drive one subscription: open the lurker on the node, forward each
/// decoded payload to the client as a binary frame, and tear down when
/// either side closes.
async fn run_subscription(
    handle: GatewayHandle,
    mut socket: WebSocket,
    target: [u8; 32],
    gsoc_addresses: Vec<[u8; 32]>,
    pss_topics: Vec<[u8; 32]>,
) {
    let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(SUB_CHANNEL_CAP);
    let cmd = ControlCommand::LurkerSubscribe {
        target,
        gsoc_addresses,
        pss_topics,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }

    loop {
        tokio::select! {
            // A decoded message from the node → binary frame to the client.
            frame = ack_rx.recv() => match frame {
                Some(ControlAck::LurkerMessage { payload, .. }) => {
                    if socket.send(Message::Binary(payload.into())).await.is_err() {
                        break;
                    }
                }
                Some(_) => {} // no other ack kinds on this stream
                None => break, // node closed the subscription
            },
            // Client activity: a Close (or a socket error) ends the sub.
            // Dropping `ack_rx` stops the node-side lurker.
            client = socket.recv() => match client {
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {} // ignore inbound data/ping frames
            },
        }
    }
    debug!(target: "ant_gateway", "subscription closed");
}

/// Parse a 32-byte hex string (optional `0x`).
fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out).ok()?;
    Some(out)
}

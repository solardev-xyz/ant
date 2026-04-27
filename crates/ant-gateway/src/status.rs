//! D.2.2 status endpoints.
//!
//! Every handler reads the live `StatusSnapshot` exposed via the
//! [`GatewayHandle`] and reshapes it into bee's wire format. Field names
//! are bee's verbatim — bee-js and the `bee` Go client both index by
//! these strings, so any drift will look like data corruption to a
//! consumer.
//!
//! The plan (PLAN.md C.4.2) writes some shapes in snake_case as informal
//! notation, but the reference bee implementation marshals every JSON
//! field in camelCase. We follow bee on the wire: that's what bee-js
//! decodes against.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::handle::GatewayHandle;

#[derive(Debug, Serialize)]
struct HealthBody {
    status: &'static str,
    version: String,
    #[serde(rename = "apiVersion")]
    api_version: String,
}

/// `GET /health`. Constant after the gateway is bound; PLAN.md D.2.1
/// requires this to answer within 50 ms of socket bind so external
/// supervisors can poll aggressively, hence no I/O at all here.
pub async fn health(State(handle): State<GatewayHandle>) -> Response {
    Json(HealthBody {
        status: "ok",
        version: handle.agent.as_str().to_string(),
        api_version: handle.api_version.as_str().to_string(),
    })
    .into_response()
}

/// `GET /readiness`. `200` once we have at least one BZZ-handshaked
/// peer; `503` until then. Bee returns `200 OK` with no body when ready
/// and `503` (no body either) when not — we follow that exactly.
pub async fn readiness(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let ready = snap.peers.connected > 0
        || snap
            .peers
            .connected_peers
            .iter()
            .any(|p| p.bzz_overlay.is_some());
    if ready {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

#[derive(Debug, Serialize)]
struct NodeBody {
    #[serde(rename = "beeMode")]
    bee_mode: &'static str,
    #[serde(rename = "gatewayMode")]
    gateway_mode: bool,
    #[serde(rename = "chequebookEnabled")]
    chequebook_enabled: bool,
    #[serde(rename = "swapEnabled")]
    swap_enabled: bool,
}

/// `GET /node`. Hardcoded to `ultra-light` for Tier A — once SWAP / a
/// chequebook are wired up (Tier B) this flips to `light`.
pub async fn node(State(_handle): State<GatewayHandle>) -> Response {
    Json(NodeBody {
        bee_mode: "ultra-light",
        gateway_mode: false,
        chequebook_enabled: false,
        swap_enabled: false,
    })
    .into_response()
}

#[derive(Debug, Serialize)]
struct AddressesBody {
    overlay: String,
    underlay: Vec<String>,
    ethereum: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(rename = "pssPublicKey")]
    pss_public_key: String,
}

/// `GET /addresses`. `overlay` and `publicKey` are bare hex (bee's
/// `swarm.Address.MarshalJSON` and `crypto.SerializeSecpkPublicKey` both
/// drop the `0x` prefix); `ethereum` keeps its `0x` prefix. We don't
/// have a dedicated PSS key (see PLAN.md Appendix B — PSS is a 501
/// surface), but bee-js panics on a missing `pssPublicKey` field, so we
/// echo `publicKey` rather than emit `null`.
pub async fn addresses(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let underlay: Vec<String> = snap.listeners.clone();
    let identity = &handle.identity;
    Json(AddressesBody {
        overlay: identity.overlay_hex.clone(),
        underlay,
        ethereum: identity.ethereum_hex.clone(),
        public_key: identity.public_key_hex.clone(),
        pss_public_key: identity.public_key_hex.clone(),
    })
    .into_response()
}

#[derive(Debug, Serialize)]
struct PeerEntry {
    address: String,
    #[serde(rename = "fullNode", skip_serializing_if = "Option::is_none")]
    full_node: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PeersBody {
    peers: Vec<PeerEntry>,
}

/// `GET /peers`. Bee's `Peer.address` is the **swarm overlay** (not the
/// libp2p peer-id), so we surface `bzz_overlay` from each connected
/// peer; pre-handshake peers (no `bzz_overlay` yet) are skipped because
/// bee never reports a peer it hasn't handshaked.
pub async fn peers(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let peers: Vec<PeerEntry> = snap
        .peers
        .connected_peers
        .iter()
        .filter_map(|p| {
            let overlay = p.bzz_overlay.as_deref()?;
            Some(PeerEntry {
                address: strip_0x(overlay).to_string(),
                full_node: p.full_node,
            })
        })
        .collect();
    Json(PeersBody { peers }).into_response()
}

#[derive(Debug, Serialize)]
struct BinInfo {
    population: u32,
    connected: u32,
    #[serde(rename = "disconnectedPeers", skip_serializing_if = "Vec::is_empty")]
    disconnected_peers: Vec<PeerEntry>,
    #[serde(rename = "connectedPeers", skip_serializing_if = "Vec::is_empty")]
    connected_peers: Vec<PeerEntry>,
}

#[derive(Debug, Serialize)]
struct TopologyBody {
    #[serde(rename = "baseAddr")]
    base_addr: String,
    population: u32,
    connected: u32,
    timestamp: String,
    #[serde(rename = "nnLowWatermark")]
    nn_low_watermark: u32,
    depth: u32,
    reachability: &'static str,
    #[serde(rename = "networkAvailability")]
    network_availability: &'static str,
    bins: BTreeMap<String, BinInfo>,
}

/// `GET /topology`. Mirrors bee `pkg/topology/kademlia.kademlia.Snapshot`:
/// 32 named bins (`bin_0` … `bin_31`) carrying population / connected
/// counts. We don't yet enumerate per-peer rows inside each bin (the
/// bee-js dashboard renders fine without them), so the bin entries omit
/// `connectedPeers` / `disconnectedPeers` via `skip_serializing_if`.
pub async fn topology(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let routing = &snap.peers.routing;

    let base_addr = if !routing.base_overlay.is_empty() {
        strip_0x(&routing.base_overlay).to_string()
    } else {
        handle.identity.overlay_hex.clone()
    };

    let mut bins = BTreeMap::new();
    let bin_counts: Vec<u32> = if routing.bins.is_empty() {
        vec![0u32; 32]
    } else {
        let mut padded = routing.bins.clone();
        padded.resize(32, 0);
        padded
    };
    for (i, count) in bin_counts.iter().enumerate() {
        bins.insert(
            format!("bin_{i}"),
            BinInfo {
                population: *count,
                connected: *count,
                disconnected_peers: Vec::new(),
                connected_peers: Vec::new(),
            },
        );
    }

    let connected: u32 = bin_counts.iter().sum();
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    Json(TopologyBody {
        base_addr,
        population: routing.size,
        connected,
        timestamp: format_rfc3339(now_unix),
        nn_low_watermark: 0,
        depth: 0,
        reachability: "Unknown",
        network_availability: "Unknown",
        bins,
    })
    .into_response()
}

/// Best-effort RFC3339 (`1970-01-01T00:00:00Z` form) without pulling in
/// `chrono`. Bee's `timestamp` field uses `time.Time.MarshalJSON` which
/// emits RFC3339; `bee-js` parses it via `new Date(...)` which accepts
/// both `Z` and `+00:00` suffixes, so the canonical form is enough.
fn format_rfc3339(unix: u64) -> String {
    // Days since 1970-01-01 → calendar date via Howard Hinnant's
    // civil_from_days (public domain). Cheaper than pulling in `time`
    // for one field.
    let secs = unix as i64;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day / 60) % 60;
    let second = secs_of_day % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hour, minute, second
    )
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

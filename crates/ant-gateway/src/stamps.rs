//! Postage-stamp read surface: `GET /stamps` and `GET /stamps/{id}`.
//!
//! Freedom's publish pre-flight (`checkSwarmPreFlight`) refuses to
//! publish unless `GET /stamps` lists at least one **usable** batch
//! (PLAN.md J.4 / J.7.2). `antd` already supports a single
//! operator-configured batch (`--postage-batch`, validated on-chain at
//! startup and stamped locally for every upload). This module surfaces
//! that batch in bee's `/stamps` JSON shape so an unmodified bee-js /
//! Freedom client sees a usable stamp and lets the publish flow
//! proceed end-to-end.
//!
//! The live counters come from the daemon over the existing
//! [`ControlCommand::PostageStatus`] channel — the same snapshot
//! `antctl postage status` renders — so no new node-loop wiring is
//! needed. Fields the daemon can't know locally without an extra chain
//! read (`amount` per-chunk balance, `batchTTL`) are filled with
//! safe placeholders; see the field comments.
//!
//! On-chain **writes** (`POST /stamps/{amount}/{depth}` to buy,
//! `PATCH /stamps/topup|dilute/...`) are intentionally *not* routed
//! here: they require live-mainnet validation and dynamic batch
//! registration with the running issuer, which is tracked separately
//! (PLAN.md J.5.B). They fall through to the `501` fallback.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;
use tokio::sync::oneshot;

use ant_control::{ControlAck, ControlCommand, PostageStatusView};

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// Bound on how long `GET /stamps` waits for the daemon's postage
/// snapshot. The reply is a synchronous in-memory read of the issuer
/// state on the node loop, so it returns in microseconds in practice;
/// the timeout only exists so a wedged or shutting-down node loop
/// surfaces a clean `503` instead of pinning the hyper task forever
/// (the same failure mode the retrieval handlers guard against).
const POSTAGE_STATUS_TIMEOUT: Duration = Duration::from_secs(10);

/// Placeholder TTL (seconds) emitted for the configured batch until a
/// real `batchTTL` chain read lands. The operator configured a genuine
/// on-chain batch, so reporting a long life keeps bee-js / Freedom from
/// treating it as expired; bee uses `-1` only for batches it can't find
/// on chain, which would make Freedom hide the batch. ~10 years.
const PLACEHOLDER_BATCH_TTL_SECS: i64 = 315_360_000;

/// One entry of bee's `GET /stamps.stamps[]`. Field names + value types
/// match bee's `postage.StampIssuer` JSON exactly (bee-js indexes by
/// these strings and crashes on a wrong type, PLAN.md J.4.2).
#[derive(Debug, Serialize)]
struct StampEntry {
    #[serde(rename = "batchID")]
    batch_id: String,
    utilization: u32,
    usable: bool,
    label: String,
    depth: u8,
    /// Per-chunk balance as a string integer (PLUR). Not known locally
    /// without a chain read; `"0"` is a safe placeholder — bee-js's
    /// `size` getter only needs `depth`, and Freedom's pre-flight only
    /// gates on `usable`.
    amount: String,
    #[serde(rename = "bucketDepth")]
    bucket_depth: u8,
    #[serde(rename = "blockNumber")]
    block_number: u64,
    #[serde(rename = "immutableFlag")]
    immutable_flag: bool,
    exists: bool,
    #[serde(rename = "batchTTL")]
    batch_ttl: i64,
}

#[derive(Debug, Serialize)]
struct StampsBody {
    stamps: Vec<StampEntry>,
}

impl StampEntry {
    /// Map a daemon postage snapshot to bee's stamp shape. Returns
    /// `None` when uploads are disabled (no batch configured) so the
    /// caller emits an empty `stamps` list, matching bee on a
    /// freshly-started node.
    fn from_view(view: &PostageStatusView) -> Option<Self> {
        if !view.enabled {
            return None;
        }
        // bee reports `batchID` as bare lowercase hex (no `0x`); the
        // daemon snapshot carries it `0x`-prefixed.
        let batch_id = view
            .batch_id
            .strip_prefix("0x")
            .or_else(|| view.batch_id.strip_prefix("0X"))
            .unwrap_or(&view.batch_id)
            .to_ascii_lowercase();
        Some(Self {
            batch_id,
            // bee's `utilization` is the fill of the fullest bucket.
            utilization: view.bucket_fill_max,
            // The batch was validated on-chain at daemon startup and is
            // actively stamping, so it is usable.
            usable: true,
            label: String::new(),
            depth: view.batch_depth,
            amount: "0".to_string(),
            bucket_depth: view.bucket_depth,
            block_number: 0,
            immutable_flag: view.immutable,
            exists: true,
            batch_ttl: PLACEHOLDER_BATCH_TTL_SECS,
        })
    }
}

/// Fetch the daemon's postage snapshot over the control channel.
/// Returns `Err(response)` already shaped as an HTTP error when the
/// node loop is gone or replies unexpectedly.
#[allow(clippy::result_large_err)]
async fn fetch_postage(handle: &GatewayHandle) -> Result<PostageStatusView, Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    if handle
        .commands
        .send(ControlCommand::PostageStatus { ack: ack_tx })
        .await
        .is_err()
    {
        return Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop is no longer accepting commands",
        ));
    }
    match tokio::time::timeout(POSTAGE_STATUS_TIMEOUT, ack_rx).await {
        Ok(Ok(ControlAck::PostageStatus(view))) => Ok(view),
        Ok(Ok(other)) => Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unexpected node ack for postage status: {other:?}"),
        )),
        Ok(Err(_)) => Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop dropped the postage status request",
        )),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "postage status request timed out",
        )),
    }
}

/// `GET /stamps`. Lists the configured batch (or an empty list when
/// uploads are disabled), bee-shaped.
pub async fn stamps(State(handle): State<GatewayHandle>) -> Response {
    let view = match fetch_postage(&handle).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let stamps = StampEntry::from_view(&view).into_iter().collect();
    json_ok(&StampsBody { stamps })
}

/// `GET /stamps/{id}`. Returns the single configured batch if `id`
/// matches it, else `404` (bee's "batch not found"). Used by bee-js's
/// extension-cost math (PLAN.md J.2.2).
pub async fn stamp(State(handle): State<GatewayHandle>, Path(id): Path<String>) -> Response {
    let view = match fetch_postage(&handle).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let Some(entry) = StampEntry::from_view(&view) else {
        return json_error(StatusCode::NOT_FOUND, "no postage batch configured");
    };
    let want = id
        .strip_prefix("0x")
        .or_else(|| id.strip_prefix("0X"))
        .unwrap_or(&id)
        .to_ascii_lowercase();
    if want != entry.batch_id {
        return json_error(StatusCode::NOT_FOUND, "batch not found");
    }
    json_ok(&entry)
}

fn json_ok<T: Serialize>(value: &T) -> Response {
    use axum::response::IntoResponse;
    axum::Json(value).into_response()
}

//! Postage-stamp read surface: `GET /stamps` and `GET /stamps/{id}`.
//!
//! Freedom's publish pre-flight (`checkSwarmPreFlight`) refuses to
//! publish unless `GET /stamps` lists at least one **usable** batch
//! (PLAN.md J.4 / J.7.2). `antd` keeps a live registry of postage
//! batches — any pre-configured `--postage-batch` plus every batch
//! bought at runtime via `POST /stamps/{amount}/{depth}`. This module
//! enumerates them in bee's `/stamps` JSON shape so an unmodified
//! bee-js / Freedom client sees the usable stamps and lets the publish
//! flow proceed end-to-end.
//!
//! The live counters come from the daemon over the
//! [`ControlCommand::PostageList`] channel (one view per registered
//! batch). Fields the daemon can't know locally without an extra chain
//! read (`amount` per-chunk balance, `batchTTL`) are filled with safe
//! placeholders; see the field comments.
//!
//! On-chain **writes** (`POST /stamps/{amount}/{depth}` to buy,
//! `PATCH /stamps/topup|dilute/...`) live in [`crate::chain`]; a buy
//! registers the new issuer with the running node before returning, so
//! the bought batch shows up here as `usable` within seconds.

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

/// Fetch every registered postage batch over the control channel.
/// Returns `Err(response)` already shaped as an HTTP error when the
/// node loop is gone or replies unexpectedly.
#[allow(clippy::result_large_err)]
async fn fetch_postage_list(handle: &GatewayHandle) -> Result<Vec<PostageStatusView>, Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    if handle
        .commands
        .send(ControlCommand::PostageList { ack: ack_tx })
        .await
        .is_err()
    {
        return Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop is no longer accepting commands",
        ));
    }
    match tokio::time::timeout(POSTAGE_STATUS_TIMEOUT, ack_rx).await {
        Ok(Ok(ControlAck::PostageList(views))) => Ok(views),
        Ok(Ok(other)) => Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unexpected node ack for postage list: {other:?}"),
        )),
        Ok(Err(_)) => Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop dropped the postage list request",
        )),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "postage list request timed out",
        )),
    }
}

/// `GET /stamps`. Lists every registered batch (those bought at runtime
/// plus any pre-configured one), bee-shaped — or an empty list when
/// none are registered.
pub async fn stamps(State(handle): State<GatewayHandle>) -> Response {
    let views = match fetch_postage_list(&handle).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let mut stamps: Vec<StampEntry> = views.iter().filter_map(StampEntry::from_view).collect();
    enrich_with_chain(&mut stamps, &handle).await;
    json_ok(&StampsBody { stamps })
}

/// Gnosis block time (seconds). Used to convert a batch's remaining
/// per-chunk balance (denominated in price-per-block units) into a TTL,
/// matching bee's `estimateBatchTTL` (`((value-totalAmount)/price) *
/// blockTime`).
const GNOSIS_BLOCK_TIME_SECS: u128 = 5;

/// bee's `batchTTL` (seconds) from a batch's remaining per-chunk balance
/// and the current storage price. `remaining` is
/// `PostageStamp.remainingBalance(id)` — already `normalisedBalance −
/// currentTotalOutPayment`, clamped to `0` on chain for an expired batch
/// — so the only edge cases left are this layer's:
///
/// * `price == 0` → `-1` ("never expires"), matching bee's semantics for
///   a chain that hasn't priced storage; also dodges divide-by-zero.
/// * `remaining == 0` (drained / expired) → `0`.
/// * an astronomically long life is clamped to `i64::MAX` rather than
///   wrapping negative.
fn batch_ttl_secs(remaining: u128, price: u128) -> i64 {
    match remaining
        .saturating_mul(GNOSIS_BLOCK_TIME_SECS)
        .checked_div(price)
    {
        Some(ttl) => i64::try_from(ttl).unwrap_or(i64::MAX),
        None => -1, // price == 0 → never expires (bee semantics)
    }
}

/// How long the per-batch chain enrichment may take before `/stamps`
/// gives up and returns the placeholder `amount` / `batchTTL`. The
/// listing must stay responsive even if the RPC is slow.
const STAMPS_ENRICH_TIMEOUT: Duration = Duration::from_secs(8);

/// Fill in bee's `amount` (the batch's normalised per-chunk balance)
/// and `batchTTL` from the chain when a chain context is configured.
/// Best-effort: any RPC failure / absent chain leaves the placeholders
/// (`amount = "0"`, a long TTL), which already keep bee-js / Freedom
/// happy. `amount = remainingBalance + totalOutPayment` and
/// `batchTTL = (remainingBalance / currentPrice) * blockTime`, matching
/// bee.
async fn enrich_with_chain(stamps: &mut [StampEntry], handle: &GatewayHandle) {
    let Some(chain) = handle.chain.clone() else {
        return;
    };
    if stamps.is_empty() {
        return;
    }
    let _ = tokio::time::timeout(STAMPS_ENRICH_TIMEOUT, async {
        let price = chain.reader.current_price().await.unwrap_or(0);
        let total_out = chain.reader.total_amount().await.unwrap_or(0);
        for entry in stamps.iter_mut() {
            let mut id = [0u8; 32];
            if hex::decode_to_slice(&entry.batch_id, &mut id).is_err() {
                continue;
            }
            let Ok(remaining) = chain.reader.batch_remaining_balance(id).await else {
                continue;
            };
            // Normalised per-chunk balance bee reports as `amount`.
            entry.amount = remaining.saturating_add(total_out).to_string();
            entry.batch_ttl = batch_ttl_secs(remaining, price);
        }
    })
    .await;
}

/// `GET /stamps/{id}`. Returns the registered batch whose id matches,
/// else `404` (bee's "batch not found"). Used by bee-js's
/// extension-cost math (PLAN.md J.2.2).
pub async fn stamp(State(handle): State<GatewayHandle>, Path(id): Path<String>) -> Response {
    let views = match fetch_postage_list(&handle).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let want = id
        .strip_prefix("0x")
        .or_else(|| id.strip_prefix("0X"))
        .unwrap_or(&id)
        .to_ascii_lowercase();
    match views
        .iter()
        .filter_map(StampEntry::from_view)
        .find(|e| e.batch_id == want)
    {
        Some(entry) => {
            let mut one = [entry];
            enrich_with_chain(&mut one, &handle).await;
            let [entry] = one;
            json_ok(&entry)
        }
        None => json_error(StatusCode::NOT_FOUND, "batch not found"),
    }
}

fn json_ok<T: Serialize>(value: &T) -> Response {
    use axum::response::IntoResponse;
    axum::Json(value).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_is_remaining_over_price_times_block_time() {
        // 1_000_000 PLUR left, 100 PLUR/chunk/block → 10_000 blocks
        // × 5s = 50_000s.
        assert_eq!(batch_ttl_secs(1_000_000, 100), 50_000);
    }

    #[test]
    fn zero_price_never_expires() {
        // bee reports -1 for an unpriced chain; also guards the divide.
        assert_eq!(batch_ttl_secs(1_000_000, 0), -1);
        assert_eq!(batch_ttl_secs(0, 0), -1);
    }

    #[test]
    fn drained_batch_is_zero() {
        // remainingBalance() already clamps an expired batch to 0 on
        // chain, so 0 in means a 0s TTL out (not the placeholder).
        assert_eq!(batch_ttl_secs(0, 100), 0);
    }

    #[test]
    fn astronomical_ttl_clamps_to_i64_max() {
        // Would overflow i64 → clamp instead of wrapping negative.
        assert_eq!(batch_ttl_secs(u128::MAX, 1), i64::MAX);
    }
}

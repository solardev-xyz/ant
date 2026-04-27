//! D.2.4 stubbed wallet / stamps / chequebook surface.
//!
//! These bodies exist so bee-shaped consumers don't see undefined fields
//! on a node that doesn't yet do SWAP / wallet / postage. Field names
//! and value types match bee verbatim (zero balances, empty stamp list,
//! all-zero chequebook address) — bee-js indexes by them and crashes on
//! missing fields. PLAN.md D.2.4 spells out the exact bodies; we mirror
//! them.

use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct WalletBody {
    #[serde(rename = "bzzBalance")]
    bzz_balance: &'static str,
    #[serde(rename = "nativeTokenBalance")]
    native_token_balance: &'static str,
    #[serde(rename = "chainID")]
    chain_id: u64,
}

/// `GET /wallet`. Tier A: zero balances, Gnosis Chain ID hardcoded
/// because bee-js's chain-aware paths branch on it. Real values land
/// in Tier B once `ant-chain` reads the wallet.
pub async fn wallet() -> Response {
    Json(WalletBody {
        bzz_balance: "0",
        native_token_balance: "0",
        chain_id: 100,
    })
    .into_response()
}

#[derive(Debug, Serialize)]
struct StampsBody {
    stamps: Vec<()>,
}

/// `GET /stamps`. Tier A: no batches. Bee always returns the outer
/// `stamps` key even when empty, so consumers can iterate without a
/// null check.
pub async fn stamps() -> Response {
    Json(StampsBody { stamps: Vec::new() }).into_response()
}

#[derive(Debug, Serialize)]
struct ChequebookAddressBody {
    #[serde(rename = "chequebookAddress")]
    chequebook_address: &'static str,
}

/// `GET /chequebook/address`. The all-zero address is bee's universal
/// "no chequebook deployed" sentinel — bee-js treats it as "feature
/// absent" rather than crashing.
pub async fn chequebook_address() -> Response {
    Json(ChequebookAddressBody {
        chequebook_address: "0x0000000000000000000000000000000000000000",
    })
    .into_response()
}

#[derive(Debug, Serialize)]
struct ChequebookBalanceBody {
    #[serde(rename = "totalBalance")]
    total_balance: &'static str,
    #[serde(rename = "availableBalance")]
    available_balance: &'static str,
}

/// `GET /chequebook/balance`. Zero in both fields; bee returns string
/// integers (not numbers) so they can carry full BZZ token resolution
/// without JS double-precision loss. We follow that.
pub async fn chequebook_balance() -> Response {
    Json(ChequebookBalanceBody {
        total_balance: "0",
        available_balance: "0",
    })
    .into_response()
}

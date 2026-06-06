//! Minimal Gnosis JSON-RPC helpers for postage batch reads (`PLAN.md` M3 / Phase 5)
//! and writes (Phase 8: createBatch / topUp / increaseDepth via [`tx`]).
//!
//! [`chequebook`] provides the SWAP / cheque primitives (Phase 7):
//! EIP-712 signing of off-chain cheques plus calldata builders for
//! `deployChequebook` and `cashChequeBeneficiary`.

pub mod chequebook;
/// On-chain recovery of node-owned state (postage batches, chequebook)
/// from the node EOA. RPC-driven, so it needs the `chain-rpc` feature.
#[cfg(feature = "chain-rpc")]
pub mod discover;
pub mod tx;

#[cfg(feature = "chain-rpc")]
use serde::Deserialize;
#[cfg(feature = "chain-rpc")]
use serde_json::json;
#[cfg(feature = "chain-rpc")]
use std::{borrow::Cow, fmt::Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[cfg(feature = "chain-rpc")]
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc: {0}")]
    Rpc(String),
    #[error("decode: {0}")]
    Decode(String),
}

#[cfg(feature = "chain-rpc")]
#[derive(Deserialize)]
struct RpcResp<'a> {
    result: Option<Cow<'a, str>>,
    error: Option<RpcErr>,
}

#[cfg(feature = "chain-rpc")]
#[derive(Deserialize)]
struct RpcErr {
    message: Option<String>,
}

/// Mainnet postage stamp contract on Gnosis (Swarm docs / bee defaults).
pub const GNOSIS_POSTAGE_STAMP: &str = "0x45a1502382541Cd610CC9068e88727426b696293";

/// xBZZ on Gnosis.
pub const GNOSIS_BZZ_TOKEN: &str = "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da";

#[cfg(feature = "chain-rpc")]
pub struct ChainClient {
    url: String,
    http: reqwest::Client,
}

#[cfg(feature = "chain-rpc")]
impl ChainClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            http: reqwest::Client::builder()
                .user_agent("ant-chain/1")
                .build()
                .expect("reqwest client"),
        }
    }

    /// Underlying reqwest client. Crate-private — exposed to the
    /// `tx` submodule so its write-side helpers can reuse the same
    /// pooled connection without each handler instantiating its own
    /// client.
    pub(crate) const fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Configured RPC endpoint URL. Crate-private; see [`Self::http`].
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub async fn eth_call(&self, to: &str, data: &str) -> Result<Vec<u8>, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_call",
            "params": [
                { "to": to, "data": data },
                "latest",
            ]
        });

        let v: RpcResp<'_> = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;

        if let Some(e) = v.error {
            return Err(RpcError::Rpc(
                e.message.unwrap_or_else(|| "unknown RPC error".to_string()),
            ));
        }

        let result = v
            .result
            .ok_or_else(|| RpcError::Rpc("missing result".to_string()))?;
        decode_hex_prefixed_bytes(result.as_ref()).map_err(|e| RpcError::Decode(e.to_string()))
    }

    /// `eth_call` with an explicit `from` address — required for any
    /// view that reads `msg.sender` (e.g. chequebook
    /// `cashChequeBeneficiary`, where `msg.sender` is baked into the
    /// EIP-712 cheque digest the contract recomputes locally).
    ///
    /// On a revert this returns the **full** RPC error JSON in
    /// [`RpcError::Rpc`] (message + `data` field). Callers care
    /// about distinguishing `invalid signature` from
    /// `liquid balance not sufficient` — both arrive here, the
    /// caller decides which one is the test result.
    pub async fn eth_call_from(
        &self,
        from: &[u8; 20],
        to: &[u8; 20],
        data: &[u8],
    ) -> Result<Vec<u8>, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_call",
            "params": [
                {
                    "from": format!("0x{}", hex::encode(from)),
                    "to":   format!("0x{}", hex::encode(to)),
                    "data": format!("0x{}", hex::encode(data)),
                },
                "latest",
            ],
        });

        let v: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = v.get("error") {
            return Err(RpcError::Rpc(err.to_string()));
        }
        let result = v
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| RpcError::Rpc("missing result".into()))?;
        decode_hex_prefixed_bytes(result).map_err(|e| RpcError::Decode(e.to_string()))
    }

    /// Native xDAI balance for `addr` (lower 128 bits — that's
    /// 2^128 ≈ 3.4 × 10^38 wei, well above any reasonable wallet).
    pub async fn eth_get_balance_lower128(&self, addr: &[u8; 20]) -> Result<u128, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_getBalance",
            "params": [format!("0x{}", hex::encode(addr)), "latest"],
        });
        let v: RpcResp<'_> = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(e) = v.error {
            return Err(RpcError::Rpc(
                e.message.unwrap_or_else(|| "unknown RPC error".to_string()),
            ));
        }
        let s = v
            .result
            .ok_or_else(|| RpcError::Rpc("missing result".into()))?;
        let s = s.strip_prefix("0x").unwrap_or(&s);
        let s = if s.is_empty() { "0" } else { s };
        let n = u128::from_str_radix(s, 16)
            .map_err(|e| RpcError::Decode(format!("eth_getBalance hex: {e}")))?;
        Ok(n)
    }

    /// ERC-20 balance (lower 128 bits — enough for sane BZZ amounts).
    pub async fn erc20_balance_of_lower128(
        &self,
        token: &str,
        owner_eth: &[u8; 20],
    ) -> Result<u128, RpcError> {
        // balanceOf(address) 0x70a08231
        let mut data = String::with_capacity(2 + 8 + 64);
        data.push_str("0x70a08231");
        write!(data, "{:0>64}", hex::encode(owner_eth)).unwrap();
        let out = self.eth_call(token, &data).await?;
        abi_word_last_u128_be(&out)
    }

    /// Latest block number (`eth_blockNumber`). Drives bee's
    /// `/status.lastSyncedBlock` and `/chainstate.block`/`chainTip` —
    /// `antd` is a light node with no block pipeline of its own, so the
    /// chain tip *is* our synced block.
    pub async fn eth_block_number(&self) -> Result<u64, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_blockNumber",
            "params": [],
        });
        let v: RpcResp<'_> = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(e) = v.error {
            return Err(RpcError::Rpc(
                e.message.unwrap_or_else(|| "unknown RPC error".to_string()),
            ));
        }
        let s = v
            .result
            .ok_or_else(|| RpcError::Rpc("missing result".into()))?;
        let s = s.strip_prefix("0x").unwrap_or(&s);
        let s = if s.is_empty() { "0" } else { s };
        u64::from_str_radix(s, 16).map_err(|e| RpcError::Decode(format!("eth_blockNumber: {e}")))
    }

    /// `PostageStamp.lastPrice()` — current price per chunk per block
    /// (PLUR), the value bee-js's stamp-cost math reads from
    /// `/chainstate.currentPrice`.
    pub async fn postage_last_price(&self, postage_contract: &str) -> Result<u128, RpcError> {
        let data = format!("0x{}", hex::encode(fn_selector(b"lastPrice()")));
        let out = self.eth_call(postage_contract, &data).await?;
        abi_word_last_u128_be(&out)
    }

    /// `PostageStamp.currentTotalOutPayment()` — cumulative per-chunk
    /// outpayment, bee's `/chainstate.totalAmount`. Combined with a
    /// batch's `normalisedBalance` this is what TTL math is derived from.
    pub async fn postage_total_amount(&self, postage_contract: &str) -> Result<u128, RpcError> {
        let data = format!(
            "0x{}",
            hex::encode(fn_selector(b"currentTotalOutPayment()"))
        );
        let out = self.eth_call(postage_contract, &data).await?;
        abi_word_last_u128_be(&out)
    }
}

/// 4-byte ABI function selector = first 4 bytes of `keccak256(sig)`.
#[cfg(feature = "chain-rpc")]
fn fn_selector(sig: &[u8]) -> [u8; 4] {
    use sha3::{Digest, Keccak256};
    let h = Keccak256::digest(sig);
    let mut s = [0u8; 4];
    s.copy_from_slice(&h[..4]);
    s
}

/// Views required for a postage `StampIssuer` (bee `batchDepth` / `batchBucketDepth` / `immutableFlag`).
#[derive(Clone, Debug)]
pub struct PostageBatchMeta {
    pub depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
    pub batch_owner_eth: [u8; 20],
}

#[cfg(feature = "chain-rpc")]
pub async fn fetch_postage_batch_meta(
    client: &ChainClient,
    postage_contract: &str,
    batch_id: &[u8; 32],
) -> Result<PostageBatchMeta, RpcError> {
    let sel_owner = encode_word32_call("2182ddb1", batch_id);
    let sel_depth = encode_word32_call("44beae8e", batch_id);
    let sel_buck = encode_word32_call("32ac57dd", batch_id);
    let sel_imm = encode_word32_call("d968f44b", batch_id);

    let owner_bytes = client.eth_call(postage_contract, &sel_owner).await?;
    last_word_eth_address(&owner_bytes)?;

    let mut batch_owner_eth = [0u8; 20];
    let w = padded_last_word(&owner_bytes)?;
    batch_owner_eth.copy_from_slice(&w[12..32]);

    let d = abi_word_tail_u256_as_u64(&client.eth_call(postage_contract, &sel_depth).await?)?;
    let b = abi_word_tail_u256_as_u64(&client.eth_call(postage_contract, &sel_buck).await?)?;
    let im = abi_word_tail_u256_as_u64(&client.eth_call(postage_contract, &sel_imm).await?)?;

    Ok(PostageBatchMeta {
        depth: u8::try_from(d).map_err(|_| RpcError::Decode("batchDepth".into()))?,
        bucket_depth: u8::try_from(b).map_err(|_| RpcError::Decode("bucketDepth".into()))?,
        immutable: im != 0,
        batch_owner_eth,
    })
}

#[cfg(feature = "chain-rpc")]
fn encode_word32_call(selector4: &str, word32: &[u8; 32]) -> String {
    format!("0x{selector4}{}", hex::encode(word32))
}

#[cfg(feature = "chain-rpc")]
fn decode_hex_prefixed_bytes(s: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s)
}

#[cfg(feature = "chain-rpc")]
fn padded_last_word(data: &[u8]) -> Result<[u8; 32], RpcError> {
    if data.len() < 32 {
        return Err(RpcError::Decode("ABI return shorter than word".into()));
    }
    let mut w = [0u8; 32];
    w.copy_from_slice(&data[data.len() - 32..]);
    Ok(w)
}

#[cfg(feature = "chain-rpc")]
fn last_word_eth_address(ret: &[u8]) -> Result<(), RpcError> {
    let w = padded_last_word(ret)?;
    if !w[..12].iter().all(|&b| b == 0) {
        return Err(RpcError::Decode("bad address ABI padding".into()));
    }
    Ok(())
}

#[cfg(feature = "chain-rpc")]
fn abi_word_tail_u256_as_u64(ret: &[u8]) -> Result<u64, RpcError> {
    let w = padded_last_word(ret)?;
    if w[..24].iter().any(|&b| b != 0) {
        return Err(RpcError::Decode("u256 does not fit u64".into()));
    }
    Ok(u64::from_be_bytes(w[24..].try_into().unwrap()))
}

#[cfg(feature = "chain-rpc")]
fn abi_word_last_u128_be(ret: &[u8]) -> Result<u128, RpcError> {
    let w = padded_last_word(ret)?;
    Ok(u128::from_be_bytes(w[16..32].try_into().unwrap()))
}

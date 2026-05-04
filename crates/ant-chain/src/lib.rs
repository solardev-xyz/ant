//! Minimal Gnosis JSON-RPC helpers for postage batch reads (`PLAN.md` M3 / Phase 5).

use serde::Deserialize;
use serde_json::json;
use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc: {0}")]
    Rpc(String),
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Deserialize)]
struct RpcResp<'a> {
    result: Option<Cow<'a, str>>,
    error: Option<RpcErr>,
}

#[derive(Deserialize)]
struct RpcErr {
    message: Option<String>,
}

/// Mainnet postage stamp contract on Gnosis (Swarm docs / bee defaults).
pub const GNOSIS_POSTAGE_STAMP: &str = "0x45a1502382541Cd610CC9068e88727426b696293";

/// xBZZ on Gnosis.
pub const GNOSIS_BZZ_TOKEN: &str = "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da";

pub struct ChainClient {
    url: String,
    http: reqwest::Client,
}

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

        let v: RpcResp<'_> = self.http.post(&self.url).json(&body).send().await?.json().await?;

        if let Some(e) = v.error {
            return Err(RpcError::Rpc(
                e.message.unwrap_or_else(|| "unknown RPC error".to_string()),
            ));
        }

        let result = v
            .result
            .ok_or_else(|| RpcError::Rpc("missing result".to_string()))?;
        decode_hex_prefixed_bytes(result.as_ref())
            .map_err(|e| RpcError::Decode(e.to_string()))
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
        data.push_str(&format!("{:0>64}", hex::encode(owner_eth)));
        let out = self.eth_call(token, &data).await?;
        abi_word_last_u128_be(&out)
    }
}

/// Views required for a postage `StampIssuer` (bee `batchDepth` / `batchBucketDepth` / `immutableFlag`).
#[derive(Clone, Debug)]
pub struct PostageBatchMeta {
    pub depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
    pub batch_owner_eth: [u8; 20],
}

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

fn encode_word32_call(selector4: &str, word32: &[u8; 32]) -> String {
    format!("0x{selector4}{}", hex::encode(word32))
}

fn decode_hex_prefixed_bytes(s: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s)
}

fn padded_last_word(data: &[u8]) -> Result<[u8; 32], RpcError> {
    if data.len() < 32 {
        return Err(RpcError::Decode("ABI return shorter than word".into()));
    }
    let mut w = [0u8; 32];
    w.copy_from_slice(&data[data.len() - 32..]);
    Ok(w)
}

fn last_word_eth_address(ret: &[u8]) -> Result<(), RpcError> {
    let w = padded_last_word(ret)?;
    if !w[..12].iter().all(|&b| b == 0) {
        return Err(RpcError::Decode("bad address ABI padding".into()));
    }
    Ok(())
}

fn abi_word_tail_u256_as_u64(ret: &[u8]) -> Result<u64, RpcError> {
    let w = padded_last_word(ret)?;
    if w[..24].iter().any(|&b| b != 0) {
        return Err(RpcError::Decode("u256 does not fit u64".into()));
    }
    Ok(u64::from_be_bytes(w[24..].try_into().unwrap()))
}

fn abi_word_last_u128_be(ret: &[u8]) -> Result<u128, RpcError> {
    let w = padded_last_word(ret)?;
    Ok(u128::from_be_bytes(w[16..32].try_into().unwrap()))
}

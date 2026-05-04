//! EIP-155 raw transaction signing and Gnosis JSON-RPC submission.
//!
//! Built for the postage-stamp workflows in M3 Phase 8: BZZ
//! `approve()` followed by `PostageStamp.createBatch / topUp /
//! increaseDepth`. We avoid pulling in the full alloy / ethers stack
//! by implementing the narrowly-scoped subset we need:
//!
//! - Legacy (pre-EIP-1559) tx envelope; Gnosis still accepts these and
//!   bee's own tx layer (`pkg/transaction`) does the same.
//! - EIP-155 chain-id replay protection (`v = chain_id * 2 + 35 + recid`).
//! - Calldata builders for `IERC20.approve(address,uint256)` and the
//!   three PostageStamp write methods.
//! - Receipt polling (`eth_getTransactionReceipt`) with a configurable
//!   timeout and per-poll backoff so a stuck mempool surfaces as a
//!   structured error instead of hanging forever.
//!
//! The signing module re-uses `ant_crypto::sign_prehash`, which already
//! returns a 65-byte `r ‖ s ‖ v` signature over a raw digest — exactly
//! what RLP-encoded EIP-155 expects.

use std::time::{Duration, Instant};

use ant_crypto::{sign_prehash, SECP256K1_SECRET_LEN};
use primitive_types::U256;
use serde_json::json;
use sha3::{Digest, Keccak256};
use thiserror::Error;

use crate::{ChainClient, RpcError};

/// Gnosis chain id (mainnet xDai). Used as the EIP-155 replay prefix
/// for every tx we sign here.
pub const GNOSIS_CHAIN_ID: u64 = 100;

/// Default gas price used when the operator doesn't pass one explicitly:
/// 2 gwei. Gnosis treats a flat 1 gwei minimum as the network floor;
/// 2 gwei keeps us above any rounding-up most validators do without
/// burning unnecessary xDAI.
pub const DEFAULT_GAS_PRICE_WEI: u64 = 2_000_000_000;

/// Conservative gas-limit defaults per workflow, sized off live
/// `eth_estimateGas` against the deployed Gnosis PostageStamp
/// (`0x45a1502382541Cd610CC9068e88727426b696293`):
///
/// - `topUp`: ~370 k → 600 k cap
/// - `increaseDepth`: ~530 k → 800 k cap
/// - `createBatch`: bee uses 600 k → 800 k cap
/// - `approve` on the BZZ ERC-20: ~50 k → 100 k cap
///
/// Each cap is ~1.5× the real cost so a busier-than-typical block
/// (which can spike storage write costs by ~10 %) still fits under
/// the limit.
pub const ERC20_APPROVE_GAS: u64 = 100_000;
pub const POSTAGE_CREATE_BATCH_GAS: u64 = 800_000;
pub const POSTAGE_TOP_UP_GAS: u64 = 600_000;
pub const POSTAGE_INCREASE_DEPTH_GAS: u64 = 800_000;

#[derive(Debug, Error)]
pub enum TxError {
    #[error("rpc: {0}")]
    Rpc(#[from] RpcError),
    #[error("crypto: {0}")]
    Crypto(#[from] ant_crypto::CryptoError),
    #[error("rlp: {0}")]
    Rlp(String),
    #[error("transaction reverted: {receipt_status}")]
    Reverted { receipt_status: String },
    #[error("transaction never mined: {tx_hash} (timeout {wait_secs}s)")]
    Timeout { tx_hash: String, wait_secs: u64 },
    #[error("decode: {0}")]
    Decode(String),
}

/// Parameters that fully describe an EIP-155 legacy transaction.
#[derive(Debug, Clone)]
pub struct LegacyTx {
    pub nonce: u64,
    pub gas_price_wei: u64,
    pub gas_limit: u64,
    /// Recipient contract / address. `None` for contract creation —
    /// not supported by the workflows here, so always `Some` in this
    /// module's helpers.
    pub to: [u8; 20],
    pub value_wei: U256,
    pub data: Vec<u8>,
    pub chain_id: u64,
}

/// RLP-encode and sign `tx` with `secret`, returning the raw bytes
/// suitable for `eth_sendRawTransaction` and the keccak256 tx hash.
pub fn sign_legacy_tx(
    secret: &[u8; SECP256K1_SECRET_LEN],
    tx: &LegacyTx,
) -> Result<(Vec<u8>, [u8; 32]), TxError> {
    let signing_payload = rlp_encode_legacy(tx, /* signing */ true, /* sig */ None);
    let digest = keccak256(&signing_payload);

    let sig = sign_prehash(secret, &digest)?;
    let r = &sig[0..32];
    let s = &sig[32..64];
    // `sign_prehash` returns recid in {27, 28}. EIP-155 wants
    // v = chain_id * 2 + 35 + recid where recid is in {0, 1}.
    let recid = sig[64].checked_sub(27).ok_or_else(|| {
        TxError::Decode(format!("invalid recovery id {} from signer", sig[64]))
    })?;
    let v = tx.chain_id.saturating_mul(2).saturating_add(35).saturating_add(recid as u64);

    let raw = rlp_encode_legacy(
        tx,
        /* signing */ false,
        Some(SignatureBytes {
            v,
            r: r.try_into().unwrap(),
            s: s.try_into().unwrap(),
        }),
    );
    let tx_hash = keccak256(&raw);
    Ok((raw, tx_hash))
}

struct SignatureBytes {
    v: u64,
    r: [u8; 32],
    s: [u8; 32],
}

/// RLP-encode the transaction. When `signing` is true and `sig` is
/// `None`, emit the EIP-155 signing payload (nine fields, last three
/// being `[chain_id, 0, 0]`). When `sig` is `Some`, emit the final
/// signed envelope (nine fields, last three being `[v, r, s]`).
fn rlp_encode_legacy(
    tx: &LegacyTx,
    signing: bool,
    sig: Option<SignatureBytes>,
) -> Vec<u8> {
    let mut s = rlp::RlpStream::new_list(9);
    rlp_append_u64(&mut s, tx.nonce);
    rlp_append_u64(&mut s, tx.gas_price_wei);
    rlp_append_u64(&mut s, tx.gas_limit);
    s.append(&tx.to.as_ref());
    rlp_append_u256(&mut s, &tx.value_wei);
    s.append(&tx.data.as_slice());
    match (signing, sig) {
        (true, None) => {
            rlp_append_u64(&mut s, tx.chain_id);
            s.append_empty_data();
            s.append_empty_data();
        }
        (false, Some(sig)) => {
            rlp_append_u64(&mut s, sig.v);
            rlp_append_truncated_be(&mut s, &sig.r);
            rlp_append_truncated_be(&mut s, &sig.s);
        }
        _ => unreachable!("rlp_encode_legacy called with conflicting (signing, sig)"),
    }
    s.out().to_vec()
}

fn rlp_append_u64(s: &mut rlp::RlpStream, v: u64) {
    if v == 0 {
        s.append_empty_data();
    } else {
        let bytes = v.to_be_bytes();
        let off = bytes.iter().position(|&b| b != 0).unwrap_or(7);
        s.append(&&bytes[off..]);
    }
}

fn rlp_append_u256(s: &mut rlp::RlpStream, v: &U256) {
    if v.is_zero() {
        s.append_empty_data();
        return;
    }
    let buf = v.to_big_endian();
    let off = buf.iter().position(|&b| b != 0).unwrap_or(31);
    s.append(&&buf[off..]);
}

fn rlp_append_truncated_be(s: &mut rlp::RlpStream, v: &[u8; 32]) {
    let off = v.iter().position(|&b| b != 0).unwrap_or(31);
    s.append(&&v[off..]);
}

fn keccak256(data: &[u8]) -> [u8; 32] {
    let h = Keccak256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&h);
    out
}

// --- ABI calldata builders ---

/// `IERC20.approve(address spender, uint256 value)` calldata.
pub fn erc20_approve_calldata(spender: &[u8; 20], value: &U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"approve(address,uint256)")[0..4]);
    data.extend_from_slice(&pad_word_address(spender));
    data.extend_from_slice(&u256_be_word(value));
    data
}

/// `PostageStamp.createBatch(address owner, uint256 initialBalancePerChunk,
///                           uint8 depth, uint8 bucketDepth, bytes32 nonce, bool immutableFlag)`.
pub fn postage_create_batch_calldata(
    owner: &[u8; 20],
    initial_balance_per_chunk: &U256,
    depth: u8,
    bucket_depth: u8,
    nonce: &[u8; 32],
    immutable: bool,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 * 6);
    data.extend_from_slice(
        &keccak256(b"createBatch(address,uint256,uint8,uint8,bytes32,bool)")[0..4],
    );
    data.extend_from_slice(&pad_word_address(owner));
    data.extend_from_slice(&u256_be_word(initial_balance_per_chunk));
    data.extend_from_slice(&pad_word_u8(depth));
    data.extend_from_slice(&pad_word_u8(bucket_depth));
    data.extend_from_slice(nonce);
    data.extend_from_slice(&pad_word_bool(immutable));
    data
}

/// `PostageStamp.topUp(bytes32 batchId, uint256 topupAmountPerChunk)`.
pub fn postage_top_up_calldata(batch_id: &[u8; 32], topup_per_chunk: &U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"topUp(bytes32,uint256)")[0..4]);
    data.extend_from_slice(batch_id);
    data.extend_from_slice(&u256_be_word(topup_per_chunk));
    data
}

/// `PostageStamp.increaseDepth(bytes32 batchId, uint8 newDepth)`.
pub fn postage_increase_depth_calldata(batch_id: &[u8; 32], new_depth: u8) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"increaseDepth(bytes32,uint8)")[0..4]);
    data.extend_from_slice(batch_id);
    data.extend_from_slice(&pad_word_u8(new_depth));
    data
}

/// 4-byte selector for the `BatchCreated` event (used by
/// `eth_getLogs` filtering when watching for our own create txs).
pub fn batch_created_event_topic() -> [u8; 32] {
    keccak256(
        b"BatchCreated(bytes32,uint256,uint256,address,uint8,uint8,bool)",
    )
}

fn pad_word_address(addr: &[u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..32].copy_from_slice(addr);
    w
}

fn pad_word_u8(v: u8) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[31] = v;
    w
}

fn pad_word_bool(v: bool) -> [u8; 32] {
    pad_word_u8(if v { 1 } else { 0 })
}

fn u256_be_word(v: &U256) -> [u8; 32] {
    v.to_big_endian()
}

// --- RPC helpers (write side) ---

impl ChainClient {
    /// Get the next nonce (`pending` block tag, so two back-to-back tx
    /// submissions don't collide on the same nonce).
    pub async fn eth_get_transaction_count_pending(
        &self,
        addr: &[u8; 20],
    ) -> Result<u64, RpcError> {
        let params = json!([format!("0x{}", hex::encode(addr)), "pending"]);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_getTransactionCount",
            "params": params,
        });
        let v: serde_json::Value =
            self.http().post(self.url()).json(&body).send().await?.json().await?;
        if let Some(err) = v.get("error") {
            return Err(RpcError::Rpc(err.to_string()));
        }
        let s = v
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| RpcError::Rpc("missing result".into()))?;
        let s = s.strip_prefix("0x").unwrap_or(s);
        let s = if s.is_empty() { "0" } else { s };
        u64::from_str_radix(s, 16).map_err(|e| RpcError::Decode(e.to_string()))
    }

    /// Submit a pre-signed raw transaction. Returns the canonical
    /// 32-byte tx hash (the same one `sign_legacy_tx` already
    /// computed locally).
    pub async fn eth_send_raw_transaction(&self, raw: &[u8]) -> Result<[u8; 32], RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(raw))],
        });
        let v: serde_json::Value =
            self.http().post(self.url()).json(&body).send().await?.json().await?;
        if let Some(err) = v.get("error") {
            return Err(RpcError::Rpc(err.to_string()));
        }
        let s = v
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| RpcError::Rpc("missing result".into()))?;
        let s = s.strip_prefix("0x").unwrap_or(s);
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).map_err(|e| RpcError::Decode(e.to_string()))?;
        Ok(out)
    }

    /// Poll `eth_getTransactionReceipt` until the receipt arrives or
    /// `wait` elapses. Returns the receipt as a structured `TxReceipt`;
    /// reverted txs become `TxError::Reverted` so a successful return
    /// always means the EVM accepted the tx.
    pub async fn wait_for_receipt(
        &self,
        tx_hash: &[u8; 32],
        wait: Duration,
    ) -> Result<TxReceipt, TxError> {
        let started = Instant::now();
        let mut delay = Duration::from_millis(500);
        loop {
            let body = json!({
                "jsonrpc": "2.0",
                "id": 1u64,
                "method": "eth_getTransactionReceipt",
                "params": [format!("0x{}", hex::encode(tx_hash))],
            });
            let v: serde_json::Value = self
                .http()
                .post(self.url())
                .json(&body)
                .send()
                .await
                .map_err(|e| RpcError::Http(e))?
                .json()
                .await
                .map_err(|e| RpcError::Http(e))?;
            if let Some(err) = v.get("error") {
                return Err(RpcError::Rpc(err.to_string()).into());
            }
            if let Some(receipt) = v.get("result").filter(|r| !r.is_null()) {
                let status = receipt
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                let block_number = receipt
                    .get("blockNumber")
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                if status != "0x1" {
                    return Err(TxError::Reverted {
                        receipt_status: status.to_string(),
                    });
                }
                let logs = receipt
                    .get("logs")
                    .and_then(|l| l.as_array())
                    .map(|arr| arr.iter().map(parse_log).collect::<Result<Vec<_>, _>>())
                    .unwrap_or_else(|| Ok(Vec::new()))?;
                return Ok(TxReceipt {
                    tx_hash: *tx_hash,
                    block_number: parse_hex_u64(block_number).unwrap_or(0),
                    logs,
                });
            }
            if started.elapsed() >= wait {
                return Err(TxError::Timeout {
                    tx_hash: hex::encode(tx_hash),
                    wait_secs: wait.as_secs(),
                });
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(8));
        }
    }
}

fn parse_log(v: &serde_json::Value) -> Result<EventLog, RpcError> {
    let address_hex = v
        .get("address")
        .and_then(|a| a.as_str())
        .ok_or_else(|| RpcError::Decode("log missing address".into()))?
        .trim_start_matches("0x");
    let mut address = [0u8; 20];
    hex::decode_to_slice(address_hex, &mut address)
        .map_err(|e| RpcError::Decode(format!("address: {e}")))?;
    let topics = v
        .get("topics")
        .and_then(|t| t.as_array())
        .ok_or_else(|| RpcError::Decode("log missing topics".into()))?
        .iter()
        .map(|t| {
            let s = t.as_str().ok_or_else(|| RpcError::Decode("non-string topic".into()))?;
            let s = s.trim_start_matches("0x");
            let mut out = [0u8; 32];
            hex::decode_to_slice(s, &mut out).map_err(|e| RpcError::Decode(format!("topic: {e}")))?;
            Ok(out)
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    let data_hex = v
        .get("data")
        .and_then(|d| d.as_str())
        .unwrap_or("0x")
        .trim_start_matches("0x");
    let data = hex::decode(data_hex).map_err(|e| RpcError::Decode(format!("data: {e}")))?;
    Ok(EventLog {
        address,
        topics,
        data,
    })
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let s = s.trim_start_matches("0x");
    let s = if s.is_empty() { "0" } else { s };
    u64::from_str_radix(s, 16).ok()
}

/// Decoded event log emitted by a tx we sent. Just enough fields to
/// pull `BatchCreated.batchId` out of a `createBatch` receipt — see
/// [`extract_created_batch_id`].
#[derive(Debug, Clone)]
pub struct EventLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

/// Tx receipt reduced to the fields the postage workflow needs:
/// confirmation block number plus all emitted logs.
#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub tx_hash: [u8; 32],
    pub block_number: u64,
    pub logs: Vec<EventLog>,
}

/// Pull the new `batchId` out of a `createBatch` receipt by matching
/// the `BatchCreated` event topic and reading the batch id from
/// topic[1] (it's an indexed parameter). Returns `None` if no such
/// event is in the receipt.
pub fn extract_created_batch_id(receipt: &TxReceipt) -> Option<[u8; 32]> {
    let topic = batch_created_event_topic();
    receipt
        .logs
        .iter()
        .find(|l| l.topics.first() == Some(&topic))
        .and_then(|l| l.topics.get(1).copied())
}

/// One-shot helper that holds a signing key + chain id and submits
/// signed txs against a [`ChainClient`]. The high-level methods
/// (`approve_bzz`, `create_batch`, `top_up`, `increase_depth`)
/// pipeline nonce-fetch → calldata → sign → send → wait so the
/// operator only deals with a `Result<TxReceipt, TxError>`.
pub struct Wallet {
    secret: [u8; SECP256K1_SECRET_LEN],
    address: [u8; 20],
    chain_id: u64,
    /// Default per-tx confirmation cap. 60 s comfortably covers
    /// Gnosis's ~5 s block time even when the mempool is busy; bigger
    /// callers can override with [`Wallet::wait_for`].
    pub default_wait: Duration,
    /// Default gas price (wei). Set to [`DEFAULT_GAS_PRICE_WEI`]
    /// unless the operator has a reason to bid higher (chain
    /// congestion).
    pub default_gas_price_wei: u64,
}

impl Wallet {
    /// Build a wallet from a 32-byte secret. Derives the Ethereum
    /// address locally so the operator can cross-check it before
    /// sending value-bearing txs.
    pub fn new(
        secret: [u8; SECP256K1_SECRET_LEN],
        chain_id: u64,
    ) -> Result<Self, TxError> {
        let sk = k256_signing_key(&secret)?;
        let vk = *sk.verifying_key();
        let address = ant_crypto::ethereum_address_from_public_key(&vk);
        Ok(Self {
            secret,
            address,
            chain_id,
            default_wait: Duration::from_secs(60),
            default_gas_price_wei: DEFAULT_GAS_PRICE_WEI,
        })
    }

    pub fn address(&self) -> &[u8; 20] {
        &self.address
    }

    /// Override the default wait timeout.
    pub fn wait_for(mut self, d: Duration) -> Self {
        self.default_wait = d;
        self
    }

    /// `IERC20.approve(spender, value)` against `token`. Returns the
    /// receipt once the tx has confirmed. Use this before
    /// `create_batch` / `top_up` so the PostageStamp contract has
    /// permission to pull BZZ from the wallet.
    pub async fn approve_bzz(
        &self,
        client: &ChainClient,
        token: &[u8; 20],
        spender: &[u8; 20],
        value: U256,
    ) -> Result<TxReceipt, TxError> {
        self.send_signed(
            client,
            *token,
            erc20_approve_calldata(spender, &value),
            ERC20_APPROVE_GAS,
        )
        .await
    }

    /// `PostageStamp.createBatch(...)`. Returns the receipt; pull the
    /// new batch id with [`extract_created_batch_id`].
    #[allow(clippy::too_many_arguments)]
    pub async fn create_batch(
        &self,
        client: &ChainClient,
        postage: &[u8; 20],
        owner: &[u8; 20],
        initial_balance_per_chunk: U256,
        depth: u8,
        bucket_depth: u8,
        nonce: &[u8; 32],
        immutable: bool,
    ) -> Result<TxReceipt, TxError> {
        let data = postage_create_batch_calldata(
            owner,
            &initial_balance_per_chunk,
            depth,
            bucket_depth,
            nonce,
            immutable,
        );
        self.send_signed(client, *postage, data, POSTAGE_CREATE_BATCH_GAS)
            .await
    }

    /// `PostageStamp.topUp(batchId, topupAmountPerChunk)`. Re-fills an
    /// existing batch's per-chunk balance, extending its TTL.
    pub async fn top_up(
        &self,
        client: &ChainClient,
        postage: &[u8; 20],
        batch_id: &[u8; 32],
        topup_per_chunk: U256,
    ) -> Result<TxReceipt, TxError> {
        let data = postage_top_up_calldata(batch_id, &topup_per_chunk);
        self.send_signed(client, *postage, data, POSTAGE_TOP_UP_GAS)
            .await
    }

    /// `PostageStamp.increaseDepth(batchId, newDepth)` (a.k.a "dilute").
    /// Doubles capacity per +1 of `new_depth`; halves the per-chunk
    /// balance because the same total cost now covers twice the
    /// chunks.
    pub async fn increase_depth(
        &self,
        client: &ChainClient,
        postage: &[u8; 20],
        batch_id: &[u8; 32],
        new_depth: u8,
    ) -> Result<TxReceipt, TxError> {
        let data = postage_increase_depth_calldata(batch_id, new_depth);
        self.send_signed(client, *postage, data, POSTAGE_INCREASE_DEPTH_GAS)
            .await
    }

    async fn send_signed(
        &self,
        client: &ChainClient,
        to: [u8; 20],
        data: Vec<u8>,
        gas_limit: u64,
    ) -> Result<TxReceipt, TxError> {
        let nonce = client.eth_get_transaction_count_pending(&self.address).await?;
        let tx = LegacyTx {
            nonce,
            gas_price_wei: self.default_gas_price_wei,
            gas_limit,
            to,
            value_wei: U256::zero(),
            data,
            chain_id: self.chain_id,
        };
        let (raw, _hash_local) = sign_legacy_tx(&self.secret, &tx)?;
        let tx_hash = client.eth_send_raw_transaction(&raw).await?;
        client.wait_for_receipt(&tx_hash, self.default_wait).await
    }
}

fn k256_signing_key(secret: &[u8; SECP256K1_SECRET_LEN]) -> Result<k256::ecdsa::SigningKey, TxError> {
    k256::ecdsa::SigningKey::from_bytes(secret.into())
        .map_err(|e| TxError::Decode(format!("signing key from secret: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Selector for `transfer(address,uint256)` — well-known ERC-20
    /// constant `0xa9059cbb`. Catches any keccak-vs-keccak256 mistake
    /// in our calldata builders.
    #[test]
    fn keccak_selector_matches_known() {
        let sel = &keccak256(b"transfer(address,uint256)")[0..4];
        assert_eq!(hex::encode(sel), "a9059cbb");
    }

    /// `approve(address,uint256)` known selector `0x095ea7b3`.
    #[test]
    fn erc20_approve_selector() {
        let calldata = erc20_approve_calldata(&[0u8; 20], &U256::zero());
        assert_eq!(hex::encode(&calldata[0..4]), "095ea7b3");
        assert_eq!(calldata.len(), 4 + 32 + 32);
    }

    /// `createBatch(address,uint256,uint8,uint8,bytes32,bool)` —
    /// known selector verified against a live Gnosis BatchCreated tx
    /// (`0xc9e741a0…` at PostageStamp `0x45a1502382541Cd610CC9068e88727426b696293`):
    /// the tx input begins with `0x5239af71`, the keccak256-derived
    /// 4-byte selector for the canonical signature.
    #[test]
    fn create_batch_selector() {
        let calldata = postage_create_batch_calldata(
            &[0u8; 20],
            &U256::zero(),
            17,
            16,
            &[0u8; 32],
            true,
        );
        assert_eq!(hex::encode(&calldata[0..4]), "5239af71");
        // 4 selector + 6 words.
        assert_eq!(calldata.len(), 4 + 32 * 6);
    }

    /// `topUp(bytes32,uint256)` known selector — used by bee's tx layer.
    /// Computed manually: keccak256("topUp(bytes32,uint256)")[0..4].
    #[test]
    fn top_up_selector_round_trip() {
        let want = &keccak256(b"topUp(bytes32,uint256)")[0..4];
        let calldata = postage_top_up_calldata(&[0u8; 32], &U256::from(1u64));
        assert_eq!(&calldata[0..4], want);
    }

    /// `increaseDepth(bytes32,uint8)` selector round-trip.
    #[test]
    fn increase_depth_selector_round_trip() {
        let want = &keccak256(b"increaseDepth(bytes32,uint8)")[0..4];
        let calldata = postage_increase_depth_calldata(&[0u8; 32], 18);
        assert_eq!(&calldata[0..4], want);
    }

    /// EIP-155 v computation: given Gnosis chain id 100 and recid 0,
    /// v should be 100 * 2 + 35 + 0 = 235. Validates the math without
    /// requiring a real RPC.
    #[test]
    fn eip155_v_computation() {
        // Build a tiny tx, sign it with a known key, decode v from the
        // signed envelope's RLP. We only verify v lives in the
        // EIP-155 range (235 or 236) — not the exact recid, since
        // that depends on the random k.
        let secret = [0x42u8; SECP256K1_SECRET_LEN];
        let tx = LegacyTx {
            nonce: 0,
            gas_price_wei: DEFAULT_GAS_PRICE_WEI,
            gas_limit: ERC20_APPROVE_GAS,
            to: [0x11u8; 20],
            value_wei: U256::zero(),
            data: vec![],
            chain_id: GNOSIS_CHAIN_ID,
        };
        let (raw, _hash) = sign_legacy_tx(&secret, &tx).unwrap();
        // Decode the outer RLP list and inspect the seventh field (v).
        let list: Vec<rlp::Rlp> = rlp::Rlp::new(&raw)
            .into_iter()
            .collect();
        assert_eq!(list.len(), 9);
        let v: u64 = list[6].as_val().unwrap();
        assert!(
            v == 235 || v == 236,
            "EIP-155 v for Gnosis must be in {{235, 236}}, got {v}",
        );
    }

    /// Round-trip the BatchCreated topic computation against a
    /// hex pulled from a live Gnosis BatchCreated log so a future
    /// signature change in the contract is caught immediately.
    /// Verified by `eth_getLogs` against PostageStamp
    /// `0x45a1502382541Cd610CC9068e88727426b696293` — the topic[0]
    /// of every observed BatchCreated event matches.
    #[test]
    fn batch_created_topic_matches_known() {
        let topic = batch_created_event_topic();
        assert_eq!(
            hex::encode(topic),
            "9b088e2c89b322a3c1d81515e1c88db3d386d022926f0e2d0b9b5813b7413d58",
        );
    }

    /// Sign a tx with a known key, then recover the signer from the
    /// outer (raw) RLP. Recovery must yield the same Ethereum address
    /// the wallet derives from the same secret. This exercises the
    /// full happy path of the signing pipeline (RLP, EIP-155 v
    /// compution, recid translation back to {0, 1}) end-to-end
    /// without needing a real RPC.
    #[test]
    fn round_trip_sign_then_recover() {
        let secret = [0x42u8; SECP256K1_SECRET_LEN];
        let want_addr = {
            let sk = k256_signing_key(&secret).unwrap();
            let vk = *sk.verifying_key();
            ant_crypto::ethereum_address_from_public_key(&vk)
        };

        let tx = LegacyTx {
            nonce: 7,
            gas_price_wei: DEFAULT_GAS_PRICE_WEI,
            gas_limit: ERC20_APPROVE_GAS,
            to: [0xaau8; 20],
            value_wei: U256::zero(),
            data: erc20_approve_calldata(&[0xbbu8; 20], &U256::from(123u64)),
            chain_id: GNOSIS_CHAIN_ID,
        };
        let signing_payload = rlp_encode_legacy(&tx, true, None);
        let digest = keccak256(&signing_payload);

        let (raw, _) = sign_legacy_tx(&secret, &tx).unwrap();

        // Decode the v / r / s out of the raw RLP envelope.
        let parsed: Vec<rlp::Rlp> = rlp::Rlp::new(&raw).into_iter().collect();
        assert_eq!(parsed.len(), 9);
        let v: u64 = parsed[6].as_val().unwrap();
        let r: Vec<u8> = parsed[7].as_val().unwrap();
        let s: Vec<u8> = parsed[8].as_val().unwrap();
        // Build a 65-byte sig in the {r, s, v=27|28} shape ant-crypto expects.
        let mut sig = [0u8; 65];
        sig[32 - r.len()..32].copy_from_slice(&r);
        sig[64 - s.len()..64].copy_from_slice(&s);
        // Reverse EIP-155: recid = v - chain_id*2 - 35; then add 27.
        let recid = (v - tx.chain_id * 2 - 35) as u8;
        sig[64] = recid + 27;

        let recovered = ant_crypto::recover_public_key_from_prehash(&sig, &digest).unwrap();
        let recovered_addr = ant_crypto::ethereum_address_from_public_key(&recovered);
        assert_eq!(recovered_addr, want_addr);
    }

    /// extract_created_batch_id pulls topic[1] when the topic[0] matches.
    #[test]
    fn extract_batch_id_from_synthetic_receipt() {
        let mut topics = vec![batch_created_event_topic()];
        let mut want_batch = [0u8; 32];
        want_batch[0] = 0xab;
        want_batch[31] = 0xcd;
        topics.push(want_batch);
        let receipt = TxReceipt {
            tx_hash: [0u8; 32],
            block_number: 1,
            logs: vec![EventLog {
                address: [0u8; 20],
                topics,
                data: vec![],
            }],
        };
        assert_eq!(extract_created_batch_id(&receipt), Some(want_batch));
    }
}

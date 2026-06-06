//! On-chain recovery of node-owned state from the node EOA (PLAN.md
//! "node-owned on-chain state recovery").
//!
//! Two discoveries, both driven purely by the node's public Ethereum
//! address (the `swarm.key` EOA) over JSON-RPC, so a node started on a
//! data dir carried over from bee — same key, no sidecar registry —
//! comes back up with its existing on-chain state intact:
//!
//! 1. [`discover_owned_batches`] — the postage batches this EOA owns
//!    and that are still funded, so they can be re-registered as usable
//!    stamp issuers.
//! 2. [`discover_owned_chequebook`] — the SWAP chequebook this EOA
//!    deployed, so the node adopts it instead of deploying a fresh one
//!    (stranding the old balance).
//!
//! Both reuse a single cheap scan: the ERC-20 `Transfer` event on the
//! xBZZ token, filtered by the indexed `from` topic = node EOA. That
//! filter returns only the node's own outgoing transfers (a tiny set),
//! and every funded chequebook / bought batch was paid for by an
//! ERC-20 transfer *from* the node EOA, so the target addresses are all
//! in the `to` field of that set.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::json;

use crate::chequebook::{chequebook_issuer_selector, factory_deployed_contracts_calldata};
use crate::tx::batch_created_event_topic;
use crate::{ChainClient, RpcError};

/// `keccak256("Transfer(address,address,uint256)")` — topic[0] of the
/// ERC-20 `Transfer(address indexed from, address indexed to, uint256
/// value)` event. Pinned as a literal (a keccak round-trip test guards
/// it) so the scan filter can't silently drift.
pub const ERC20_TRANSFER_TOPIC: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];

/// xBZZ token deploy block on Gnosis — the lower bound for the log
/// scan (no node transfer predates the token). Bee's hardcoded mainnet
/// constant.
pub const GNOSIS_XBZZ_DEPLOY_BLOCK: u64 = 16_514_506;

/// Initial scan window. Capable RPCs (e.g. `rpc.gnosischain.com`)
/// answer the full token-to-head range in one call because the indexed
/// `from` filter matches almost nothing; range-capped RPCs cause the
/// scanner to shrink the window adaptively.
const INITIAL_SCAN_CHUNK: u64 = 50_000_000;

/// One decoded `eth_getLogs` entry. Richer than [`crate::tx::EventLog`]
/// (which is receipt-shaped): the scan needs the originating tx hash
/// (to fetch the `BatchCreated` event) and block number (to prefer the
/// most-recent chequebook on a tie).
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
    pub tx_hash: [u8; 32],
    pub block_number: u64,
}

/// A postage batch the node EOA owns on-chain and that is still funded.
#[derive(Debug, Clone)]
pub struct DiscoveredBatch {
    pub batch_id: [u8; 32],
    pub depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
    /// `PostageStamp.remainingBalance(batchId)` — the per-chunk balance
    /// left (normalised minus outpayment). `> 0` means unexpired.
    pub remaining_balance: u128,
}

impl ChainClient {
    /// `eth_getLogs` for a single contract address over an inclusive
    /// block range, with the given topic filter (already JSON-encoded
    /// as an array of topic strings / nulls).
    pub async fn eth_get_logs(
        &self,
        address: &str,
        topics: &serde_json::Value,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LogEntry>, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_getLogs",
            "params": [{
                "address": address,
                "fromBlock": format!("0x{from_block:x}"),
                "toBlock": format!("0x{to_block:x}"),
                "topics": topics,
            }],
        });
        let v: serde_json::Value = self
            .http()
            .post(self.url())
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = v.get("error") {
            return Err(RpcError::Rpc(err.to_string()));
        }
        let arr = v
            .get("result")
            .and_then(|r| r.as_array())
            .ok_or_else(|| RpcError::Rpc("eth_getLogs: missing result array".into()))?;
        arr.iter().map(parse_log_entry).collect()
    }

    /// Scan a contract's logs across `[from_block, to_block]`, shrinking
    /// the window on a range-limit RPC error and growing it again after
    /// a success. Public Gnosis RPCs cap `eth_getLogs` ranges (Alchemy's
    /// free tier at 10 blocks, others at 50 000, etc.); the indexed
    /// `from` filter keeps each window cheap so this stays bounded.
    pub async fn scan_logs(
        &self,
        address: &str,
        topics: &serde_json::Value,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LogEntry>, RpcError> {
        let mut out = Vec::new();
        let mut start = from_block;
        let mut chunk = INITIAL_SCAN_CHUNK.min(to_block.saturating_sub(from_block).max(1));
        while start <= to_block {
            let end = start.saturating_add(chunk.saturating_sub(1)).min(to_block);
            match self.eth_get_logs(address, topics, start, end).await {
                Ok(mut logs) => {
                    out.append(&mut logs);
                    start = end.saturating_add(1);
                    chunk = chunk.saturating_mul(2).min(INITIAL_SCAN_CHUNK);
                }
                Err(RpcError::Rpc(msg)) if chunk > 1 && is_range_limit_error(&msg) => {
                    chunk = (chunk / 2).max(1);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// `PostageStamp.remainingBalance(bytes32)` — per-chunk balance left
    /// on a batch (`> 0` means unexpired). Selector `0xd71ba7c4`.
    pub async fn postage_remaining_balance(
        &self,
        postage_contract: &str,
        batch_id: &[u8; 32],
    ) -> Result<u128, RpcError> {
        let mut data = String::with_capacity(2 + 8 + 64);
        data.push_str("0xd71ba7c4");
        write!(data, "{}", hex::encode(batch_id)).unwrap();
        let out = self.eth_call(postage_contract, &data).await?;
        last_word_u128(&out)
    }
}

/// Discover every postage batch owned by `node_eoa` that is still
/// funded. Reuses the xBZZ `Transfer(from = node_eoa)` scan: batch
/// buys / top-ups pull BZZ via `transferFrom`, emitting a `Transfer`
/// whose `to` is the `PostageStamp` contract; the originating tx's
/// receipt carries the `BatchCreated` event with the batch id.
pub async fn discover_owned_batches(
    client: &ChainClient,
    postage_contract: &str,
    xbzz_token: &str,
    node_eoa: &[u8; 20],
    from_block: u64,
) -> Result<Vec<DiscoveredBatch>, RpcError> {
    let postage_addr = parse_addr(postage_contract).ok_or_else(|| {
        RpcError::Decode(format!("bad postage contract address {postage_contract}"))
    })?;
    let to_block = client.eth_block_number().await?;
    let topics = json!([
        format!("0x{}", hex::encode(ERC20_TRANSFER_TOPIC)),
        topic_for_address(node_eoa),
    ]);
    let logs = client
        .scan_logs(xbzz_token, &topics, from_block, to_block)
        .await?;

    // Transactions whose Transfer landed in the PostageStamp contract.
    let mut tx_hashes: BTreeSet<[u8; 32]> = BTreeSet::new();
    for log in &logs {
        if log.topics.len() >= 3 && address_from_topic(&log.topics[2]) == postage_addr {
            tx_hashes.insert(log.tx_hash);
        }
    }

    // Pull the BatchCreated batch ids out of each receipt.
    let bc_topic = batch_created_event_topic();
    let mut batch_ids: BTreeSet<[u8; 32]> = BTreeSet::new();
    for tx in &tx_hashes {
        let receipt = match client.eth_get_transaction_receipt(tx).await {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => return Err(e),
        };
        for log in &receipt.logs {
            if log.address == postage_addr && log.topics.first() == Some(&bc_topic) {
                if let Some(id) = log.topics.get(1) {
                    batch_ids.insert(*id);
                }
            }
        }
    }

    // Verify each candidate against current chain state.
    let mut out = Vec::new();
    for id in &batch_ids {
        let Ok(meta) = crate::fetch_postage_batch_meta(client, postage_contract, id).await else {
            continue;
        };
        if meta.batch_owner_eth != *node_eoa {
            continue;
        }
        let remaining = client
            .postage_remaining_balance(postage_contract, id)
            .await
            .unwrap_or(0);
        if remaining == 0 {
            continue; // expired / drained
        }
        out.push(DiscoveredBatch {
            batch_id: *id,
            depth: meta.depth,
            bucket_depth: meta.bucket_depth,
            immutable: meta.immutable,
            remaining_balance: remaining,
        });
    }
    Ok(out)
}

/// Discover the SWAP chequebook deployed by `node_eoa`. Reuses the same
/// `Transfer(from = node_eoa)` scan: every funded chequebook was
/// deposited into by the node EOA, so it is among the `to` addresses.
/// Each candidate is verified with `factory.deployedContracts(to)` and
/// `to.issuer() == node_eoa`. On more than one match the most-recently
/// funded chequebook wins.
pub async fn discover_owned_chequebook(
    client: &ChainClient,
    factory: &[u8; 20],
    postage_contract: &str,
    xbzz_token: &str,
    node_eoa: &[u8; 20],
    from_block: u64,
) -> Result<Option<[u8; 20]>, RpcError> {
    let postage_addr = parse_addr(postage_contract);
    let xbzz_addr = parse_addr(xbzz_token);
    let to_block = client.eth_block_number().await?;
    let topics = json!([
        format!("0x{}", hex::encode(ERC20_TRANSFER_TOPIC)),
        topic_for_address(node_eoa),
    ]);
    let logs = client
        .scan_logs(xbzz_token, &topics, from_block, to_block)
        .await?;

    // Distinct `to` addresses, keyed by the highest block we saw them
    // funded at (so we can prefer the most-recent on a tie).
    let mut candidates: BTreeMap<[u8; 20], u64> = BTreeMap::new();
    for log in &logs {
        if log.topics.len() < 3 {
            continue;
        }
        let to = address_from_topic(&log.topics[2]);
        if &to == node_eoa || Some(to) == postage_addr || Some(to) == xbzz_addr || to == [0u8; 20] {
            continue;
        }
        let entry = candidates.entry(to).or_insert(0);
        *entry = (*entry).max(log.block_number);
    }

    // Most-recent first.
    let mut ordered: Vec<([u8; 20], u64)> = candidates.into_iter().collect();
    ordered.sort_by_key(|c| std::cmp::Reverse(c.1));

    let factory_hex = format!("0x{}", hex::encode(factory));
    for (cb, _block) in ordered {
        let cb_hex = format!("0x{}", hex::encode(cb));
        // factory.deployedContracts(cb) -> bool
        let dc_data = format!(
            "0x{}",
            hex::encode(factory_deployed_contracts_calldata(&cb))
        );
        let deployed = match client.eth_call(&factory_hex, &dc_data).await {
            Ok(ret) => last_word_nonzero(&ret),
            Err(_) => continue,
        };
        if !deployed {
            continue;
        }
        // cb.issuer() -> address
        let iss_data = format!("0x{}", hex::encode(chequebook_issuer_selector()));
        let issuer = match client.eth_call(&cb_hex, &iss_data).await {
            Ok(ret) if ret.len() >= 32 => {
                address_from_topic(ret[ret.len() - 32..].try_into().unwrap())
            }
            _ => continue,
        };
        if &issuer == node_eoa {
            return Ok(Some(cb));
        }
    }
    Ok(None)
}

fn parse_log_entry(v: &serde_json::Value) -> Result<LogEntry, RpcError> {
    let address = parse_addr(
        v.get("address")
            .and_then(|a| a.as_str())
            .ok_or_else(|| RpcError::Decode("log missing address".into()))?,
    )
    .ok_or_else(|| RpcError::Decode("log bad address".into()))?;
    let topics = v
        .get("topics")
        .and_then(|t| t.as_array())
        .ok_or_else(|| RpcError::Decode("log missing topics".into()))?
        .iter()
        .map(|t| {
            let s = t
                .as_str()
                .ok_or_else(|| RpcError::Decode("non-string topic".into()))?;
            let mut out = [0u8; 32];
            hex::decode_to_slice(s.trim_start_matches("0x"), &mut out)
                .map_err(|e| RpcError::Decode(format!("topic: {e}")))?;
            Ok(out)
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    let data = hex::decode(
        v.get("data")
            .and_then(|d| d.as_str())
            .unwrap_or("0x")
            .trim_start_matches("0x"),
    )
    .map_err(|e| RpcError::Decode(format!("log data: {e}")))?;
    let mut tx_hash = [0u8; 32];
    hex::decode_to_slice(
        v.get("transactionHash")
            .and_then(|t| t.as_str())
            .unwrap_or("0x")
            .trim_start_matches("0x"),
        &mut tx_hash,
    )
    .map_err(|e| RpcError::Decode(format!("transactionHash: {e}")))?;
    let block_number = v
        .get("blockNumber")
        .and_then(|b| b.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0);
    Ok(LogEntry {
        address,
        topics,
        data,
        tx_hash,
        block_number,
    })
}

/// Heuristic: does this RPC error mean "your block range / result set
/// was too big"? Such errors are recoverable by shrinking the window;
/// anything else (auth, malformed) should propagate immediately.
fn is_range_limit_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    [
        "block range",
        "range",
        "more than",
        "exceed",
        "too large",
        "10000",
        "limit",
        "logs matched",
        "response size",
        "up to a",
        "query timeout",
        "too many results",
    ]
    .iter()
    .any(|needle| m.contains(needle))
}

/// 0x-prefixed 64-hex topic word for a 20-byte address (left-padded).
fn topic_for_address(addr: &[u8; 20]) -> String {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(addr);
    format!("0x{}", hex::encode(w))
}

/// The 20-byte address packed into the low bytes of a 32-byte topic /
/// ABI word.
fn address_from_topic(word: &[u8; 32]) -> [u8; 20] {
    let mut a = [0u8; 20];
    a.copy_from_slice(&word[12..32]);
    a
}

fn parse_addr(s: &str) -> Option<[u8; 20]> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut a = [0u8; 20];
    hex::decode_to_slice(s, &mut a).ok().map(|()| a)
}

/// True when the last 32-byte word of an ABI return has any non-zero
/// byte — the bool-return convention used by `deployedContracts`.
fn last_word_nonzero(ret: &[u8]) -> bool {
    ret.len() >= 32 && ret[ret.len() - 32..].iter().any(|&b| b != 0)
}

fn last_word_u128(ret: &[u8]) -> Result<u128, RpcError> {
    if ret.len() < 32 {
        return Err(RpcError::Decode("ABI return shorter than word".into()));
    }
    let w = &ret[ret.len() - 32..];
    Ok(u128::from_be_bytes(w[16..32].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha3::{Digest, Keccak256};

    #[test]
    fn transfer_topic_matches_keccak() {
        let want = Keccak256::digest(b"Transfer(address,address,uint256)");
        assert_eq!(ERC20_TRANSFER_TOPIC, want.as_slice());
    }

    #[test]
    fn remaining_balance_selector() {
        let sel = &Keccak256::digest(b"remainingBalance(bytes32)")[0..4];
        assert_eq!(hex::encode(sel), "d71ba7c4");
    }

    #[test]
    fn topic_address_round_trip() {
        let addr = [0xabu8; 20];
        let topic = topic_for_address(&addr);
        let mut word = [0u8; 32];
        hex::decode_to_slice(topic.trim_start_matches("0x"), &mut word).unwrap();
        assert_eq!(address_from_topic(&word), addr);
        assert_eq!(&word[0..12], &[0u8; 12]);
    }

    #[test]
    fn range_limit_classification() {
        assert!(is_range_limit_error(
            "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range"
        ));
        assert!(is_range_limit_error("exceed maximum block range: 50000"));
        assert!(is_range_limit_error(
            "query returned more than 10000 results"
        ));
        assert!(!is_range_limit_error("invalid api key"));
    }

    #[test]
    fn last_word_helpers() {
        let mut w = [0u8; 32];
        w[31] = 1;
        assert!(last_word_nonzero(&w));
        assert!(!last_word_nonzero(&[0u8; 32]));
        let mut bal = [0u8; 32];
        bal[28..32].copy_from_slice(&0x3a21_096fu32.to_be_bytes());
        assert_eq!(last_word_u128(&bal).unwrap(), 0x3a21_096f);
    }
}

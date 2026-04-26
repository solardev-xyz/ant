//! Multi-chunk file reconstruction.
//!
//! Bee splits files larger than 4 KiB into a balanced k-ary tree of chunks
//! (`pkg/file/joiner`, `pkg/file/splitter`):
//!
//! - **Leaf chunks** carry up to 4096 bytes of file data behind their
//!   8-byte little-endian span. The span equals the payload length.
//! - **Intermediate chunks** carry a list of 32-byte child references
//!   (max 128 per chunk = `swarm.Branches`). Their span is the *total*
//!   size of the subtree rooted at this chunk, not the chunk's own
//!   payload length.
//! - The root chunk's span is the file's full length. If `span <= 4096`
//!   the root *is* a leaf.
//!
//! The splitter wraps levels greedily: a 5 KiB file produces a root with
//! two children (4 KiB + 1 KiB); a 500 KiB file produces a root with 125
//! direct children (each 4 KiB). For any intermediate chunk with `N`
//! children and total span `T`, the first `N-1` children each cover the
//! same `branchSize` bytes (`= 4096 * 128^k` for the smallest `k` that
//! keeps the last child within `branchSize`); the last child carries
//! the remainder. This is the [`subtrie_section`] formula and matches
//! `bee/pkg/file/joiner.subtrieSection` byte-for-byte.
//!
//! # What's not handled
//!
//! - **Erasure-coded / Reed-Solomon redundancy** on intermediate chunks
//!   (`redundancy.Level != NONE`). Detected via the high bits of the span;
//!   we reject the file with a clear error rather than silently mis-decoding.
//! - **Encrypted chunks** (`refLength == 64`). Same: we reject; the
//!   reference would decode in a recognisable way (entry on root manifest
//!   would be 64 bytes, not 32) so the caller can route to a friendlier
//!   "not yet supported" message.
//!
//! Both are uncommon for plain `swarm-cli upload` of a small text file or
//! site, which is the only thing the read-path currently targets.

use crate::ChunkFetcher;
use ant_crypto::CHUNK_SIZE;
use futures::stream::{self, StreamExt};
use thiserror::Error;
use tracing::trace;

/// Default cap on a joined file's total size, in bytes. Sized to comfortably
/// hold a small website or text file while making it impossible for a
/// malformed root chunk (huge declared span, tiny actual contents) to make
/// us allocate gigabytes before noticing. Override per-call when there's a
/// legitimate reason to fetch more.
pub const DEFAULT_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
/// Hard ceiling on intermediate-chunk fan-out. `swarm.Branches`.
const MAX_BRANCHES: usize = CHUNK_SIZE / 32;

/// Maximum number of sibling chunk fetches in flight at once during a
/// single intermediate node's expansion. Each in-flight fetch costs one
/// libp2p stream multiplexed over the existing yamux session and ~4 KiB
/// of buffer; well under the per-peer flow-control window. Tuned for a
/// small file (single intermediate root, < 128 leaves) to complete in
/// roughly two retrieval-RTTs end-to-end. For very deep trees the
/// effective fan-out is still per-level, so larger files benefit
/// proportionally without unbounded concurrency.
const FETCH_FANOUT: usize = 16;

/// Reason a join failed. Distinct variants so the caller can decide
/// whether to retry, fall back, or surface a clean error to the user.
#[derive(Debug, Error)]
pub enum JoinError {
    /// The file's declared span exceeds the configured cap.
    #[error("file too large: span {span} bytes, cap {cap} bytes")]
    TooLarge { span: u64, cap: usize },
    /// An intermediate chunk's payload size isn't a whole multiple of
    /// the reference length, or the chunk is shorter than declared.
    #[error("malformed intermediate chunk at offset {offset}: {detail}")]
    MalformedChunk { offset: u64, detail: String },
    /// The chunk's span carries non-zero high bits, indicating
    /// Reed-Solomon redundancy. We don't implement RS today.
    #[error("redundancy / erasure coding not supported")]
    UnsupportedRedundancy,
    /// Encrypted chunks (64-byte refs) aren't supported. Only triggered
    /// if the *caller* passes a 64-byte root reference; for the
    /// 32-byte path we never decode a 64-byte ref-length intermediate.
    #[error("encrypted chunks not supported")]
    UnsupportedEncryption,
    /// A child fetch failed. Wraps the underlying retrieval error so
    /// `antctl get` can surface a useful message ("not found", "timed
    /// out", peer id, etc.).
    #[error("fetch chunk {addr}: {source}")]
    FetchChunk {
        addr: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Reconstruct a file from a Swarm reference using the chunk tree.
///
/// `root_chunk_data` is the full wire bytes of the chunk addressed by
/// `root_addr` (`span (8 LE) || payload`). Pass it in (rather than fetching
/// it inside) so callers that already have the root chunk in hand — e.g.
/// the manifest path which fetched it to decode mantaray and *then*
/// realises it's actually a binary blob with a missing manifest, or the
/// retrieval-client path which has just verified it — don't fetch twice.
///
/// `fetcher` is responsible for retrieving and CAC-verifying every
/// non-root chunk in the tree. The joiner doesn't itself talk to libp2p;
/// this keeps the multi-chunk reconstruction logic pure and unit-testable
/// against a `HashMap` mock.
pub async fn join(
    fetcher: &dyn ChunkFetcher,
    root_chunk_data: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, JoinError> {
    let (span_raw, payload) = split_chunk(root_chunk_data, 0)?;
    if !is_redundancy_none(span_raw) {
        return Err(JoinError::UnsupportedRedundancy);
    }
    let span = u64::from_le_bytes(span_raw);
    if span > max_bytes as u64 {
        return Err(JoinError::TooLarge {
            span,
            cap: max_bytes,
        });
    }

    let mut out = Vec::with_capacity(span as usize);
    join_subtree(fetcher, payload, span, &mut out).await?;
    Ok(out)
}

/// Recursive joiner core. Appends `subtree_span` bytes worth of file data
/// to `out`, given an intermediate or leaf chunk's *payload* (post-span)
/// and the declared span of the subtree it roots.
///
/// At each intermediate level we fan out up to [`FETCH_FANOUT`] sibling
/// fetches in parallel via `Stream::buffered`, which preserves input
/// order. Once the fetched chunks come back we walk them sequentially:
/// for leaves that's just an append, for sub-intermediates we recurse
/// (and the next level fans out independently). This turns a chain of
/// `N` sequential RTTs into roughly `ceil(N / FETCH_FANOUT)` per
/// intermediate, which is the dominant cost for typical small files
/// where the root has all leaves as direct children.
fn join_subtree<'a>(
    fetcher: &'a dyn ChunkFetcher,
    chunk_payload: &'a [u8],
    subtree_span: u64,
    out: &'a mut Vec<u8>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), JoinError>> + Send + 'a>> {
    Box::pin(async move {
        // Leaf: the chunk *is* the data. `subtree_span` was carried from
        // the parent ref so we don't even need to re-read this chunk's own
        // span; the payload bytes are already what we want.
        if subtree_span <= chunk_payload.len() as u64 {
            out.extend_from_slice(&chunk_payload[..subtree_span as usize]);
            return Ok(());
        }

        // Intermediate: payload must be `[ref32; N]`. We never accept
        // 64-byte (encrypted) refs in the joiner.
        if !chunk_payload.len().is_multiple_of(32) {
            return Err(JoinError::MalformedChunk {
                offset: out.len() as u64,
                detail: format!(
                    "intermediate payload len {} not a multiple of 32",
                    chunk_payload.len()
                ),
            });
        }
        let refs = chunk_payload.len() / 32;
        if refs == 0 || refs > MAX_BRANCHES {
            return Err(JoinError::MalformedChunk {
                offset: out.len() as u64,
                detail: format!("intermediate ref count {refs} out of range"),
            });
        }
        let branch_size = subtrie_section(refs, subtree_span);
        trace!(
            target: "ant_retrieval::joiner",
            refs,
            subtree_span,
            branch_size,
            fanout = FETCH_FANOUT.min(refs).max(1),
            "intermediate chunk",
        );

        // Pre-compute (child_addr, child_span) for every sibling so the
        // parallel fetch can run without touching the parent payload
        // again, and so the order is fixed before we kick off any I/O.
        let mut specs: Vec<([u8; 32], u64)> = Vec::with_capacity(refs);
        let mut remaining = subtree_span;
        for i in 0..refs {
            let mut child_addr = [0u8; 32];
            child_addr.copy_from_slice(&chunk_payload[i * 32..(i + 1) * 32]);
            // Last child carries whatever's left; siblings each cover a
            // full `branch_size` worth of file bytes.
            let child_span = if i == refs - 1 {
                remaining
            } else {
                branch_size.min(remaining)
            };
            specs.push((child_addr, child_span));
            remaining = remaining.saturating_sub(child_span);
        }

        // Fan out the per-sibling fetches. `buffered` keeps results in
        // input order so the byte stream we splice into `out` matches
        // the file's logical layout. We collect into a Vec rather than
        // streaming into the recursion because `out: &mut Vec<u8>` can
        // only be borrowed by one in-flight future at a time — the
        // fetches run in parallel, the assembly is sequential.
        let fanout = FETCH_FANOUT.min(refs).max(1);
        let results: Vec<Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>> =
            stream::iter(specs.iter().copied().map(|(addr, _span)| async move {
                fetcher.fetch(addr).await
            }))
            .buffered(fanout)
            .collect()
            .await;

        for ((addr, child_span), result) in specs.into_iter().zip(results) {
            let child_chunk = result.map_err(|e| JoinError::FetchChunk {
                addr: hex::encode(addr),
                source: e,
            })?;
            let (_, child_payload) = split_chunk(&child_chunk, out.len() as u64)?;
            join_subtree(fetcher, child_payload, child_span, out).await?;
        }
        Ok(())
    })
}

/// Split `[span (8 LE) || payload]` into its two parts, with a positional
/// hint (`offset` in the file) for diagnostics.
fn split_chunk(data: &[u8], offset: u64) -> Result<([u8; 8], &[u8]), JoinError> {
    if data.len() < 8 {
        return Err(JoinError::MalformedChunk {
            offset,
            detail: format!("chunk shorter than span ({} bytes)", data.len()),
        });
    }
    let mut span = [0u8; 8];
    span.copy_from_slice(&data[..8]);
    Ok((span, &data[8..]))
}

/// Reject spans whose top byte is non-zero — bee uses the top bits of the
/// span to encode the redundancy level for RS-protected intermediate
/// chunks. A real file's span tops out at 2^56 bytes (~72 PB), so any
/// legitimate span has its high byte clear.
fn is_redundancy_none(span: [u8; 8]) -> bool {
    span[7] == 0
}

/// Compute the per-child subtrie capacity of an intermediate chunk with
/// `refs` children and total span `subtree_span`.
///
/// Mirrors `bee/pkg/file/joiner.subtrieSection`: starting from one chunk
/// (`4096`), grow by the branching factor (128) until the last child fits
/// within `branch_size`. The first `refs-1` children are full subtrees of
/// `branch_size` bytes; the last child holds the remainder.
fn subtrie_section(refs: usize, subtree_span: u64) -> u64 {
    let refs = refs as u64;
    let branching = MAX_BRANCHES as u64;
    let mut branch_size = CHUNK_SIZE as u64;
    loop {
        // What's left over for the rightmost child if every other child
        // filled `branch_size`. Once that fits, we're done.
        let whats_left = subtree_span.saturating_sub(branch_size * refs.saturating_sub(1));
        if whats_left <= branch_size {
            return branch_size;
        }
        // Defend against a malformed `subtree_span` that would loop forever
        // (refs is bounded by 128, so `branching^4 = 256M` is already far
        // above any sane file size given our 32 MiB cap).
        if branch_size > u64::MAX / branching {
            return branch_size;
        }
        branch_size *= branching;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::cac_new;
    use std::collections::HashMap;
    use std::error::Error;

    /// HashMap-backed fetcher for tests. Pre-populated with whatever
    /// chunks the test wants the joiner to find.
    struct MapFetcher {
        chunks: HashMap<[u8; 32], Vec<u8>>,
    }

    impl MapFetcher {
        fn new() -> Self {
            Self {
                chunks: HashMap::new(),
            }
        }
        fn insert(&mut self, addr: [u8; 32], wire: Vec<u8>) {
            self.chunks.insert(addr, wire);
        }
    }

    #[async_trait::async_trait]
    impl crate::ChunkFetcher for MapFetcher {
        async fn fetch(
            &self,
            addr: [u8; 32],
        ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            self.chunks
                .get(&addr)
                .cloned()
                .ok_or_else(|| -> Box<dyn Error + Send + Sync> {
                    format!("missing chunk {}", hex::encode(addr)).into()
                })
        }
    }

    /// Single-chunk file: the root *is* the data. No intermediate chunks
    /// are fetched; we don't need a populated map.
    #[tokio::test]
    async fn joins_single_chunk_file() {
        let payload = b"hello swarm".to_vec();
        let (_, wire) = cac_new(&payload).unwrap();
        let fetcher = MapFetcher::new();
        let out = join(&fetcher, &wire, DEFAULT_MAX_FILE_BYTES).await.unwrap();
        assert_eq!(out, payload);
    }

    /// 5 KiB file: root has two child refs (4096 + 1024). Verifies the
    /// last-child remainder math and the leaf-chunk truncation.
    #[tokio::test]
    async fn joins_two_chunk_file() {
        // Two leaf chunks: 4096 + 1024 bytes.
        let leaf_a = vec![0xaa; CHUNK_SIZE];
        let leaf_b = vec![0xbb; 1024];
        let (addr_a, wire_a) = cac_new(&leaf_a).unwrap();
        let (addr_b, wire_b) = cac_new(&leaf_b).unwrap();

        // Build the root chunk: span = 5120, payload = addr_a || addr_b.
        let mut intermediate_payload = Vec::new();
        intermediate_payload.extend_from_slice(&addr_a);
        intermediate_payload.extend_from_slice(&addr_b);
        let span: u64 = (CHUNK_SIZE + 1024) as u64;
        let mut root_wire = Vec::with_capacity(8 + intermediate_payload.len());
        root_wire.extend_from_slice(&span.to_le_bytes());
        root_wire.extend_from_slice(&intermediate_payload);

        let mut fetcher = MapFetcher::new();
        fetcher.insert(addr_a, wire_a);
        fetcher.insert(addr_b, wire_b);

        let out = join(&fetcher, &root_wire, DEFAULT_MAX_FILE_BYTES).await.unwrap();
        assert_eq!(out.len(), span as usize);
        assert!(out[..CHUNK_SIZE].iter().all(|&b| b == 0xaa));
        assert!(out[CHUNK_SIZE..].iter().all(|&b| b == 0xbb));
    }

    /// File large enough to require a third level: 130 leaves of 4 KiB
    /// (a touch over 128 chunks → root must point at two intermediate
    /// chunks, the first holding 128 leaves and the second holding 2).
    /// Exercises `subtrie_section` non-trivially.
    #[tokio::test]
    async fn joins_three_level_tree() {
        let mut fetcher = MapFetcher::new();
        let mut leaf_addrs = Vec::with_capacity(130);
        let mut expected: Vec<u8> = Vec::with_capacity(130 * CHUNK_SIZE);
        for i in 0..130u8 {
            let payload = vec![i; CHUNK_SIZE];
            let (addr, wire) = cac_new(&payload).unwrap();
            fetcher.insert(addr, wire);
            leaf_addrs.push(addr);
            expected.extend_from_slice(&payload);
        }

        // First intermediate: 128 leaves, span = 128 * 4096 = 524288.
        let mut int1_payload = Vec::with_capacity(128 * 32);
        for addr in &leaf_addrs[..128] {
            int1_payload.extend_from_slice(addr);
        }
        let int1_span: u64 = 128 * CHUNK_SIZE as u64;
        let mut int1_wire = Vec::new();
        int1_wire.extend_from_slice(&int1_span.to_le_bytes());
        int1_wire.extend_from_slice(&int1_payload);
        let (int1_addr, int1_full) = (chunk_addr(&int1_payload, int1_span), int1_wire.clone());
        fetcher.insert(int1_addr, int1_full);

        // Second intermediate: 2 leaves, span = 2 * 4096 = 8192.
        let mut int2_payload = Vec::with_capacity(2 * 32);
        for addr in &leaf_addrs[128..] {
            int2_payload.extend_from_slice(addr);
        }
        let int2_span: u64 = 2 * CHUNK_SIZE as u64;
        let mut int2_wire = Vec::new();
        int2_wire.extend_from_slice(&int2_span.to_le_bytes());
        int2_wire.extend_from_slice(&int2_payload);
        let (int2_addr, int2_full) = (chunk_addr(&int2_payload, int2_span), int2_wire.clone());
        fetcher.insert(int2_addr, int2_full);

        // Root: 2 children with total span = 130 * 4096 = 532480.
        let mut root_payload = Vec::with_capacity(64);
        root_payload.extend_from_slice(&int1_addr);
        root_payload.extend_from_slice(&int2_addr);
        let total_span: u64 = 130 * CHUNK_SIZE as u64;
        let mut root_wire = Vec::new();
        root_wire.extend_from_slice(&total_span.to_le_bytes());
        root_wire.extend_from_slice(&root_payload);

        let out = join(&fetcher, &root_wire, DEFAULT_MAX_FILE_BYTES).await.unwrap();
        assert_eq!(out.len(), expected.len());
        assert_eq!(&out[..256], &expected[..256], "leading prefix");
        assert_eq!(&out[out.len() - 256..], &expected[expected.len() - 256..], "trailing");
    }

    /// Cap is honored before any allocation happens. Even a manifestly
    /// dishonest root chunk (span = u64::MAX) is rejected promptly.
    #[tokio::test]
    async fn rejects_oversized_span() {
        let bogus_payload = vec![0u8; 32];
        let mut wire = Vec::new();
        wire.extend_from_slice(&u64::MAX.to_le_bytes());
        wire.extend_from_slice(&bogus_payload);
        // u64::MAX has top bit set so we trip UnsupportedRedundancy first.
        // Use a span just over the cap to exercise TooLarge.
        let span: u64 = (DEFAULT_MAX_FILE_BYTES as u64) + 1;
        wire.clear();
        wire.extend_from_slice(&span.to_le_bytes());
        wire.extend_from_slice(&bogus_payload);

        let fetcher = MapFetcher::new();
        let err = join(&fetcher, &wire, DEFAULT_MAX_FILE_BYTES).await.unwrap_err();
        assert!(matches!(err, JoinError::TooLarge { .. }));
    }

    /// Spans with the redundancy bits set are rejected — silently
    /// mis-decoding a redundant file would produce garbled output.
    #[tokio::test]
    async fn rejects_redundancy_span() {
        let payload = vec![0u8; 32];
        let mut span = [0u8; 8];
        span[7] = 1; // any non-zero high byte
        let mut wire = Vec::new();
        wire.extend_from_slice(&span);
        wire.extend_from_slice(&payload);
        let fetcher = MapFetcher::new();
        let err = join(&fetcher, &wire, DEFAULT_MAX_FILE_BYTES).await.unwrap_err();
        assert!(matches!(err, JoinError::UnsupportedRedundancy));
    }

    /// Compute the CAC address of a constructed intermediate chunk for
    /// the test fixtures above. Mirrors `cac_new` but for the case where
    /// the caller already controls span and payload separately.
    fn chunk_addr(payload: &[u8], span: u64) -> [u8; 32] {
        let mut wire = Vec::new();
        wire.extend_from_slice(&span.to_le_bytes());
        wire.extend_from_slice(payload);
        // BMT hash of payload + span prefix. The simpler way is to round
        // through `cac_new` with the payload (which fixes its own span);
        // but here we have a synthetic span, so go via `bmt_hash_with_span`.
        let span_bytes: [u8; 8] = wire[..8].try_into().unwrap();
        ant_crypto::bmt_hash_with_span(&span_bytes, payload).unwrap()
    }
}

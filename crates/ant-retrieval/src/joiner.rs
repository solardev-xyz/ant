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
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::BTreeMap;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::trace;

/// Default cap on a joined file's total size, in bytes. Sized to comfortably
/// hold a small website or text file while making it impossible for a
/// malformed root chunk (huge declared span, tiny actual contents) to make
/// us allocate gigabytes before noticing. Override per-call when there's a
/// legitimate reason to fetch more.
pub const DEFAULT_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
/// Hard ceiling on intermediate-chunk fan-out. `swarm.Branches`.
const MAX_BRANCHES: usize = CHUNK_SIZE / 32;

/// Maximum number of sibling subtrees expanded at once from a single
/// intermediate node. Each subtree may fetch its own descendants, so the
/// daemon's process-wide retrieval semaphore remains the final cap on
/// live libp2p streams; this fanout controls how much of a deep file can
/// make progress instead of walking left-to-right one subtree at a time.
/// Keep this close to Bee's worker-style prefetching while relying on
/// `RoutingFetcher`'s per-request and process-wide semaphores to prevent
/// concurrent media requests from stampeding the network.
const FETCH_FANOUT: usize = 8;
/// Retry a child subtree in place when it fails because one descendant
/// chunk could not be fetched. This is the streaming equivalent of the
/// old whole-file retry loop: once the gateway has emitted earlier bytes
/// it cannot restart the response, so transient mainnet misses must be
/// retried before the child subtree is handed to the caller. Successful
/// descendant chunks are cached by `RoutingFetcher`, so retries normally
/// jump straight to the missing tail.
const SUBTREE_RETRY_ATTEMPTS: usize = 32;
/// Above this span, the streaming joiner descends into a child subtree
/// instead of materialising that whole child as one output buffer. This
/// is what fixes Bee-shaped roots with only a couple of very large
/// children: `/bytes` can emit the first 512 KiB-ish descendants instead
/// of waiting for a 64 MiB subtree to finish.
const STREAM_RECURSE_THRESHOLD: u64 = 1024 * 1024;

/// Knobs that aren't span/payload-defined. Pass [`JoinOptions::default`]
/// for the strict joiner; flip flags on for explicit, narrowly-scoped
/// fallbacks.
#[derive(Debug, Clone, Copy, Default)]
pub struct JoinOptions {
    /// Accept files whose root span carries a non-zero
    /// redundancy/erasure-coding level byte. The joiner masks the level
    /// byte off and walks the chunk tree as if it were level NONE,
    /// which yields the correct bytes whenever every data chunk is
    /// reachable. Reed-Solomon recovery itself is not implemented;
    /// missing chunks remain fatal. Off by default — set explicitly via
    /// `antctl get --allow-degraded-redundancy` so the user opts in to
    /// "best-effort decode without the redundancy benefit".
    pub allow_degraded_redundancy: bool,
}

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
    /// Reed-Solomon redundancy. We don't implement RS today; pass
    /// [`JoinOptions::allow_degraded_redundancy`] to decode anyway
    /// without the redundancy safety net.
    #[error("redundancy / erasure coding not supported (level {level}); pass --allow-degraded-redundancy to decode without RS recovery")]
    UnsupportedRedundancy { level: u8 },
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
    /// The downstream consumer went away while the joiner was streaming
    /// output chunks.
    #[error("output stream closed")]
    OutputClosed,
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
    join_with_options(fetcher, root_chunk_data, max_bytes, JoinOptions::default()).await
}

/// Like [`join`], but with explicit knobs (currently just
/// `allow_degraded_redundancy`). Kept as a separate entry point so the
/// strict default has the simplest possible signature for the test
/// suite and the unit-tested mantaray internals; production callers
/// that need to flip flags use this variant.
pub async fn join_with_options(
    fetcher: &dyn ChunkFetcher,
    root_chunk_data: &[u8],
    max_bytes: usize,
    options: JoinOptions,
) -> Result<Vec<u8>, JoinError> {
    let (span_raw, payload) = split_chunk(root_chunk_data, 0)?;
    let span_raw = if is_redundancy_none(span_raw) {
        span_raw
    } else if options.allow_degraded_redundancy {
        let level = span_raw[7];
        tracing::warn!(
            target: "ant_retrieval::joiner",
            level,
            "root span carries redundancy level {level}; decoding without RS recovery (--allow-degraded-redundancy)",
        );
        let mut masked = span_raw;
        masked[7] = 0;
        masked
    } else {
        return Err(JoinError::UnsupportedRedundancy { level: span_raw[7] });
    };
    let span = u64::from_le_bytes(span_raw);
    if span > max_bytes as u64 {
        return Err(JoinError::TooLarge {
            span,
            cap: max_bytes,
        });
    }

    join_subtree(fetcher, payload, span, 0).await
}

/// Stream a joined file in byte order without materialising the entire
/// body in one `Vec<u8>`.
///
/// This mirrors Bee's HTTP path more closely than [`join`]: the caller
/// fetches the root chunk once, learns the total span, and then receives
/// ordered body chunks as the joiner resolves subtrees. The current
/// implementation emits one buffer per completed child subtree at each
/// intermediate level; that is intentionally coarser than Bee's
/// `io.ReadSeeker`, but it lets the gateway send body bytes while the
/// rest of the tree is still being retrieved.
pub async fn join_to_sender(
    fetcher: &dyn ChunkFetcher,
    root_chunk_data: &[u8],
    max_bytes: usize,
    options: JoinOptions,
    out: mpsc::Sender<Vec<u8>>,
) -> Result<(), JoinError> {
    join_to_sender_range(fetcher, root_chunk_data, max_bytes, options, None, out).await
}

/// Inclusive byte range expressed against the joined file body.
///
/// Both ends are inclusive (matching HTTP `Range: bytes=start-end`);
/// callers ensure `start <= end < total_span` and convert HTTP suffix
/// or open-ended ranges to absolute bounds before calling.
#[derive(Debug, Clone, Copy)]
pub struct ByteRange {
    pub start: u64,
    pub end_inclusive: u64,
}

impl ByteRange {
    /// Build a range covering `[start, end_inclusive]` after clamping to
    /// `total - 1` on the right edge. Returns `None` for empty / invalid
    /// inputs so the caller can short-circuit to a 416 response.
    pub fn clamp(start: u64, end_inclusive: u64, total: u64) -> Option<Self> {
        if total == 0 || start > end_inclusive || start >= total {
            return None;
        }
        Some(Self {
            start,
            end_inclusive: end_inclusive.min(total - 1),
        })
    }
}

/// Like [`join_to_sender`], but only emits the requested byte range.
///
/// `range = None` is identical to [`join_to_sender`]: the whole body
/// streams. With `range = Some(_)` the joiner walks the chunk tree
/// without fetching subtrees that fall entirely outside `[start, end]`,
/// and slices the leftmost / rightmost subtrees to drop any prefix or
/// suffix bytes outside the range. This is what lets `/bzz` and
/// `/bytes` answer browser media seeks (`Range: bytes=N-M`) without
/// joining the whole file first.
///
/// Callers are responsible for emitting the correct HTTP `206
/// Partial Content` headers — this function only produces the body
/// bytes for the range. It does not emit chunks before `start` or
/// after `end`.
pub async fn join_to_sender_range(
    fetcher: &dyn ChunkFetcher,
    root_chunk_data: &[u8],
    max_bytes: usize,
    options: JoinOptions,
    range: Option<ByteRange>,
    out: mpsc::Sender<Vec<u8>>,
) -> Result<(), JoinError> {
    let (span_raw, payload) = split_chunk(root_chunk_data, 0)?;
    let span_raw = if is_redundancy_none(span_raw) {
        span_raw
    } else if options.allow_degraded_redundancy {
        let mut masked = span_raw;
        masked[7] = 0;
        masked
    } else {
        return Err(JoinError::UnsupportedRedundancy { level: span_raw[7] });
    };
    let span = u64::from_le_bytes(span_raw);
    if span > max_bytes as u64 {
        return Err(JoinError::TooLarge {
            span,
            cap: max_bytes,
        });
    }

    match range {
        None => join_subtree_to_sender(fetcher, payload, span, 0, &out).await,
        Some(range) => {
            let clamped = ByteRange {
                start: range.start.min(span.saturating_sub(1)),
                end_inclusive: range.end_inclusive.min(span.saturating_sub(1)),
            };
            join_subtree_range_to_sender(fetcher, payload, span, 0, clamped, &out).await
        }
    }
}

/// Recursive joiner core. Returns `subtree_span` bytes worth of file data,
/// given an intermediate or leaf chunk's *payload* (post-span) and the
/// declared span of the subtree it roots.
///
/// At each intermediate level we fan out up to [`FETCH_FANOUT`] sibling
/// subtrees in parallel. Each sibling builds into its own `Vec<u8>`,
/// then the parent concatenates completed subtrees in document order.
/// That keeps output ordering deterministic without forcing the
/// right-hand siblings to wait for every recursive fetch in the
/// left-hand subtree.
fn join_subtree<'a>(
    fetcher: &'a dyn ChunkFetcher,
    chunk_payload: &'a [u8],
    subtree_span: u64,
    offset: u64,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, JoinError>> + Send + 'a>> {
    Box::pin(async move {
        // Leaf: the chunk *is* the data. `subtree_span` was carried from
        // the parent ref so we don't even need to re-read this chunk's own
        // span; the payload bytes are already what we want.
        if subtree_span <= chunk_payload.len() as u64 {
            return Ok(chunk_payload[..subtree_span as usize].to_vec());
        }

        // Intermediate: payload must be `[ref32; N]`. We never accept
        // 64-byte (encrypted) refs in the joiner.
        if !chunk_payload.len().is_multiple_of(32) {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: format!(
                    "intermediate payload len {} not a multiple of 32",
                    chunk_payload.len()
                ),
            });
        }
        // Bee's splitter zero-pads trailing ref slots when the last
        // branch doesn't fill the chunk. Strip those before computing
        // `refs`, otherwise we dispatch `fetch([0u8; 32])` requests
        // that every peer rejects.
        let effective = effective_payload_size(chunk_payload);
        if effective == 0 {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: "intermediate chunk has no non-zero refs".into(),
            });
        }
        let chunk_payload = &chunk_payload[..effective];
        let refs = chunk_payload.len() / 32;
        if refs > MAX_BRANCHES {
            return Err(JoinError::MalformedChunk {
                offset,
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
        let mut specs: Vec<(usize, [u8; 32], u64, u64)> = Vec::with_capacity(refs);
        let mut remaining = subtree_span;
        let mut child_offset = offset;
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
            specs.push((i, child_addr, child_span, child_offset));
            remaining = remaining.saturating_sub(child_span);
            child_offset = child_offset.saturating_add(child_span);
        }

        // Fan out whole subtrees, not just the immediate child chunk
        // fetch. `buffer_unordered` avoids head-of-line blocking when an
        // early sibling is tail-slow; the index lets us restore byte
        // order before concatenating.
        let fanout = FETCH_FANOUT.min(refs).max(1);
        let mut children = stream::iter(specs.into_iter().map(
            |(index, addr, child_span, child_offset)| async move {
                let child =
                    join_child_with_retries(fetcher, addr, child_span, child_offset).await?;
                Ok((index, child))
            },
        ))
        .buffer_unordered(fanout);

        let mut pending = BTreeMap::new();
        let mut next_index = 0;
        let mut out = Vec::with_capacity(subtree_span as usize);
        while let Some((index, child)) = children.try_next().await? {
            pending.insert(index, child);
            while let Some(child) = pending.remove(&next_index) {
                out.extend_from_slice(&child);
                next_index += 1;
            }
        }
        Ok(out)
    })
}

fn join_subtree_to_sender<'a>(
    fetcher: &'a dyn ChunkFetcher,
    chunk_payload: &'a [u8],
    subtree_span: u64,
    offset: u64,
    out: &'a mpsc::Sender<Vec<u8>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), JoinError>> + Send + 'a>> {
    Box::pin(async move {
        if subtree_span <= chunk_payload.len() as u64 {
            out.send(chunk_payload[..subtree_span as usize].to_vec())
                .await
                .map_err(|_| JoinError::OutputClosed)?;
            return Ok(());
        }

        if !chunk_payload.len().is_multiple_of(32) {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: format!(
                    "intermediate payload len {} not a multiple of 32",
                    chunk_payload.len()
                ),
            });
        }
        // Strip trailing zero-padding before treating the chunk as a
        // ref array; see `effective_payload_size` for the rationale.
        let effective = effective_payload_size(chunk_payload);
        if effective == 0 {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: "intermediate chunk has no non-zero refs".into(),
            });
        }
        let chunk_payload = &chunk_payload[..effective];
        let refs = chunk_payload.len() / 32;
        if refs > MAX_BRANCHES {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: format!("intermediate ref count {refs} out of range"),
            });
        }

        let branch_size = subtrie_section(refs, subtree_span);
        let mut specs: Vec<(usize, [u8; 32], u64, u64)> = Vec::with_capacity(refs);
        let mut remaining = subtree_span;
        let mut child_offset = offset;
        for i in 0..refs {
            let mut child_addr = [0u8; 32];
            child_addr.copy_from_slice(&chunk_payload[i * 32..(i + 1) * 32]);
            let child_span = if i == refs - 1 {
                remaining
            } else {
                branch_size.min(remaining)
            };
            specs.push((i, child_addr, child_span, child_offset));
            remaining = remaining.saturating_sub(child_span);
            child_offset = child_offset.saturating_add(child_span);
        }

        if specs
            .iter()
            .any(|(_, _, child_span, _)| *child_span > STREAM_RECURSE_THRESHOLD)
        {
            for (_, addr, child_span, child_offset) in specs {
                join_child_to_sender(fetcher, addr, child_span, child_offset, out).await?;
            }
            return Ok(());
        }

        let fanout = FETCH_FANOUT.min(refs).max(1);
        let mut children = stream::iter(specs.into_iter().map(
            |(index, addr, child_span, child_offset)| async move {
                let child =
                    join_child_with_retries(fetcher, addr, child_span, child_offset).await?;
                Ok((index, child))
            },
        ))
        .buffer_unordered(fanout);

        let mut pending = BTreeMap::new();
        let mut next_index = 0;
        while let Some((index, child)) = children.try_next().await? {
            pending.insert(index, child);
            while let Some(child) = pending.remove(&next_index) {
                out.send(child).await.map_err(|_| JoinError::OutputClosed)?;
                next_index += 1;
            }
        }
        Ok(())
    })
}

async fn join_child_to_sender(
    fetcher: &dyn ChunkFetcher,
    addr: [u8; 32],
    child_span: u64,
    child_offset: u64,
    out: &mpsc::Sender<Vec<u8>>,
) -> Result<(), JoinError> {
    let mut child_chunk = None;
    for attempt in 1..=SUBTREE_RETRY_ATTEMPTS {
        match fetcher.fetch(addr).await {
            Ok(chunk) => {
                child_chunk = Some(chunk);
                break;
            }
            Err(e) if attempt < SUBTREE_RETRY_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
                drop(e);
            }
            Err(e) => {
                return Err(JoinError::FetchChunk {
                    addr: hex::encode(addr),
                    source: e,
                });
            }
        }
    }
    let child_chunk = child_chunk.expect("retry loop always runs at least once");
    let (_, child_payload) = split_chunk(&child_chunk, child_offset)?;
    join_subtree_to_sender(fetcher, child_payload, child_span, child_offset, out).await
}

/// Range-aware version of [`join_subtree_to_sender`].
///
/// `range` is expressed against the **whole file**, not the subtree. The
/// caller passes the subtree's absolute `offset` and `subtree_span` so
/// every level can convert "which part of this subtree falls inside the
/// requested range?" into chunk-local terms without rebuilding state.
///
/// At an intermediate level we drop child subtrees that fall entirely
/// outside `[start, end]` (no fetch issued at all) and recurse only into
/// the ones that overlap. The leftmost overlapping leaf is sliced to
/// drop any prefix before `range.start`; the rightmost is truncated at
/// `range.end_inclusive + 1`. Output stays strictly in file order so
/// the gateway can pipe it straight into the HTTP response body.
fn join_subtree_range_to_sender<'a>(
    fetcher: &'a dyn ChunkFetcher,
    chunk_payload: &'a [u8],
    subtree_span: u64,
    offset: u64,
    range: ByteRange,
    out: &'a mpsc::Sender<Vec<u8>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), JoinError>> + Send + 'a>> {
    Box::pin(async move {
        let subtree_end = offset.saturating_add(subtree_span);
        // The caller already filtered overlapping subtrees; defensive
        // bound check keeps a buggy descent from emitting stray bytes.
        if subtree_end <= range.start || offset > range.end_inclusive {
            return Ok(());
        }

        if subtree_span <= chunk_payload.len() as u64 {
            // Leaf. Slice down to the file-relative window.
            let leaf = &chunk_payload[..subtree_span as usize];
            let leaf_start = offset;
            let leaf_end_excl = offset + subtree_span;
            let lo = range.start.max(leaf_start) - leaf_start;
            let hi = (range.end_inclusive + 1).min(leaf_end_excl) - leaf_start;
            if lo >= hi {
                return Ok(());
            }
            out.send(leaf[lo as usize..hi as usize].to_vec())
                .await
                .map_err(|_| JoinError::OutputClosed)?;
            return Ok(());
        }

        if !chunk_payload.len().is_multiple_of(32) {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: format!(
                    "intermediate payload len {} not a multiple of 32",
                    chunk_payload.len()
                ),
            });
        }
        let effective = effective_payload_size(chunk_payload);
        if effective == 0 {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: "intermediate chunk has no non-zero refs".into(),
            });
        }
        let chunk_payload = &chunk_payload[..effective];
        let refs = chunk_payload.len() / 32;
        if refs > MAX_BRANCHES {
            return Err(JoinError::MalformedChunk {
                offset,
                detail: format!("intermediate ref count {refs} out of range"),
            });
        }
        let branch_size = subtrie_section(refs, subtree_span);

        // Build per-child specs first, then keep only the ones whose
        // [child_offset, child_offset+child_span) overlaps the range.
        let mut specs: Vec<(usize, [u8; 32], u64, u64)> = Vec::with_capacity(refs);
        let mut remaining = subtree_span;
        let mut child_offset = offset;
        for i in 0..refs {
            let mut child_addr = [0u8; 32];
            child_addr.copy_from_slice(&chunk_payload[i * 32..(i + 1) * 32]);
            let child_span = if i == refs - 1 {
                remaining
            } else {
                branch_size.min(remaining)
            };
            let child_end = child_offset + child_span;
            let overlaps = child_end > range.start && child_offset <= range.end_inclusive;
            if overlaps {
                specs.push((i, child_addr, child_span, child_offset));
            }
            remaining = remaining.saturating_sub(child_span);
            child_offset = child_offset.saturating_add(child_span);
        }

        if specs.is_empty() {
            return Ok(());
        }

        // Walk overlapping children in document order. We can't use the
        // unordered fan-out trick from the full-file path because the
        // sliced leftmost / rightmost children must reach the consumer
        // in order. Inner full-coverage subtrees are still parallelised
        // by [`join_subtree_to_sender`] when we descend into them via
        // `join_child_to_sender`.
        for (_, addr, child_span, child_offset) in specs {
            let child_end = child_offset + child_span;
            let fully_inside = child_offset >= range.start && child_end <= range.end_inclusive + 1;
            if fully_inside {
                join_child_to_sender(fetcher, addr, child_span, child_offset, out).await?;
            } else {
                join_child_range_to_sender(fetcher, addr, child_span, child_offset, range, out)
                    .await?;
            }
        }
        Ok(())
    })
}

async fn join_child_range_to_sender(
    fetcher: &dyn ChunkFetcher,
    addr: [u8; 32],
    child_span: u64,
    child_offset: u64,
    range: ByteRange,
    out: &mpsc::Sender<Vec<u8>>,
) -> Result<(), JoinError> {
    let mut child_chunk = None;
    for attempt in 1..=SUBTREE_RETRY_ATTEMPTS {
        match fetcher.fetch(addr).await {
            Ok(chunk) => {
                child_chunk = Some(chunk);
                break;
            }
            Err(e) if attempt < SUBTREE_RETRY_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
                drop(e);
            }
            Err(e) => {
                return Err(JoinError::FetchChunk {
                    addr: hex::encode(addr),
                    source: e,
                });
            }
        }
    }
    let child_chunk = child_chunk.expect("retry loop always runs at least once");
    let (_, child_payload) = split_chunk(&child_chunk, child_offset)?;
    join_subtree_range_to_sender(fetcher, child_payload, child_span, child_offset, range, out).await
}

async fn join_child_with_retries(
    fetcher: &dyn ChunkFetcher,
    addr: [u8; 32],
    child_span: u64,
    child_offset: u64,
) -> Result<Vec<u8>, JoinError> {
    let mut last_err = None;
    for attempt in 1..=SUBTREE_RETRY_ATTEMPTS {
        match join_child_once(fetcher, addr, child_span, child_offset).await {
            Ok(child) => return Ok(child),
            Err(e) if is_transient_join_error(&e) && attempt < SUBTREE_RETRY_ATTEMPTS => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.expect("retry loop always runs at least once"))
}

async fn join_child_once(
    fetcher: &dyn ChunkFetcher,
    addr: [u8; 32],
    child_span: u64,
    child_offset: u64,
) -> Result<Vec<u8>, JoinError> {
    let child_chunk = fetcher
        .fetch(addr)
        .await
        .map_err(|e| JoinError::FetchChunk {
            addr: hex::encode(addr),
            source: e,
        })?;
    let (_, child_payload) = split_chunk(&child_chunk, child_offset)?;
    join_subtree(fetcher, child_payload, child_span, child_offset).await
}

fn is_transient_join_error(err: &JoinError) -> bool {
    matches!(err, JoinError::FetchChunk { .. })
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

/// Effective payload size of an intermediate chunk: trailing 32-byte
/// all-zero ref slots are zero-padding, not real children.
///
/// Mirrors `bee/pkg/file/utils.go::ChunkPayloadSize`. The splitter that
/// produces intermediate chunks always writes a `[ref; N]` array, but
/// when the file's last branch isn't full it leaves the trailing slots
/// zeroed. The joiner has to drop those slots before iterating —
/// otherwise it dispatches `fetch([0u8; 32])` requests to peers, which
/// bee's retrieval handler rejects with `invalid address queried by
/// peer …`. (We saw this in the wild as a 30s timeout flood when
/// serving a feed-backed `.eth` site whose index document was a 12-ref
/// intermediate chunk in a 128-slot payload — 116 spurious fetches per
/// request.)
///
/// Returns the byte length up to and including the last non-zero
/// 32-byte slot, rounded up to a 32-byte boundary. Callers slice with
/// `payload[..effective_payload_size(payload)]` before computing
/// `refs`. An all-zero payload yields `0`, which the caller surfaces
/// as a malformed chunk (intermediate payloads must hold at least one
/// child).
fn effective_payload_size(payload: &[u8]) -> usize {
    const HASH: usize = 32;
    let mut l = payload.len();
    // Don't overshoot a non-multiple-of-32 payload; outer code already
    // rejects those, but be defensive.
    l -= l % HASH;
    while l >= HASH {
        if payload[l - HASH..l].iter().any(|&b| b != 0) {
            return l;
        }
        l -= HASH;
    }
    0
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
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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
        async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            self.chunks
                .get(&addr)
                .cloned()
                .ok_or_else(|| -> Box<dyn Error + Send + Sync> {
                    format!("missing chunk {}", hex::encode(addr)).into()
                })
        }
    }

    /// Fetcher that makes selected leaf chunks linger long enough for
    /// the test to observe whether independent subtrees overlap.
    struct ObservedFetcher {
        chunks: HashMap<[u8; 32], Vec<u8>>,
        delayed: HashSet<[u8; 32]>,
        active_delayed: AtomicUsize,
        max_active_delayed: AtomicUsize,
    }

    impl ObservedFetcher {
        fn new() -> Self {
            Self {
                chunks: HashMap::new(),
                delayed: HashSet::new(),
                active_delayed: AtomicUsize::new(0),
                max_active_delayed: AtomicUsize::new(0),
            }
        }

        fn insert(&mut self, addr: [u8; 32], wire: Vec<u8>) {
            self.chunks.insert(addr, wire);
        }

        fn insert_delayed(&mut self, addr: [u8; 32], wire: Vec<u8>) {
            self.delayed.insert(addr);
            self.insert(addr, wire);
        }

        fn max_active_delayed(&self) -> usize {
            self.max_active_delayed.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl crate::ChunkFetcher for ObservedFetcher {
        async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            if self.delayed.contains(&addr) {
                let active = self.active_delayed.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active_delayed.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                self.active_delayed.fetch_sub(1, Ordering::SeqCst);
            }

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

        let out = join(&fetcher, &root_wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .unwrap();
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

        let out = join(&fetcher, &root_wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .unwrap();
        assert_eq!(out.len(), expected.len());
        assert_eq!(&out[..256], &expected[..256], "leading prefix");
        assert_eq!(
            &out[out.len() - 256..],
            &expected[expected.len() - 256..],
            "trailing"
        );
    }

    /// Two large L1 subtrees should overlap while joining. The previous
    /// implementation fetched both L1 chunks in parallel, then recursively
    /// joined the left L1 subtree to completion before starting the right
    /// one, so delayed leaf fetch concurrency never exceeded
    /// `FETCH_FANOUT`. The fixed joiner builds sibling subtrees in
    /// parallel and restores byte order at the parent.
    #[tokio::test]
    async fn joins_sibling_subtrees_in_parallel() {
        let mut fetcher = ObservedFetcher::new();
        let mut intermediate_addrs = Vec::with_capacity(2);
        let mut expected: Vec<u8> = Vec::with_capacity(256 * CHUNK_SIZE);

        for group in 0..2u16 {
            let mut intermediate_payload = Vec::with_capacity(128 * 32);
            for leaf in 0..128u16 {
                let byte = ((group * 128 + leaf) % 251) as u8;
                let payload = vec![byte; CHUNK_SIZE];
                let (addr, wire) = cac_new(&payload).unwrap();
                fetcher.insert_delayed(addr, wire);
                intermediate_payload.extend_from_slice(&addr);
                expected.extend_from_slice(&payload);
            }

            let span = 128 * CHUNK_SIZE as u64;
            let mut wire = Vec::new();
            wire.extend_from_slice(&span.to_le_bytes());
            wire.extend_from_slice(&intermediate_payload);
            let addr = chunk_addr(&intermediate_payload, span);
            fetcher.insert(addr, wire);
            intermediate_addrs.push(addr);
        }

        let mut root_payload = Vec::with_capacity(2 * 32);
        for addr in &intermediate_addrs {
            root_payload.extend_from_slice(addr);
        }
        let total_span = expected.len() as u64;
        let mut root_wire = Vec::new();
        root_wire.extend_from_slice(&total_span.to_le_bytes());
        root_wire.extend_from_slice(&root_payload);

        let out = tokio::time::timeout(
            Duration::from_secs(5),
            join(&fetcher, &root_wire, DEFAULT_MAX_FILE_BYTES),
        )
        .await
        .expect("join timed out")
        .unwrap();

        assert_eq!(out, expected);
        assert!(
            fetcher.max_active_delayed() > FETCH_FANOUT,
            "expected delayed leaves from multiple L1 subtrees to overlap; max active was {}",
            fetcher.max_active_delayed()
        );
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
        let err = join(&fetcher, &wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .unwrap_err();
        assert!(matches!(err, JoinError::TooLarge { .. }));
    }

    /// Spans with the redundancy bits set are rejected by default —
    /// silently mis-decoding a redundant file would produce garbled
    /// output.
    #[tokio::test]
    async fn rejects_redundancy_span() {
        let payload = vec![0u8; 32];
        let mut span = [0u8; 8];
        span[7] = 1; // any non-zero high byte
        let mut wire = Vec::new();
        wire.extend_from_slice(&span);
        wire.extend_from_slice(&payload);
        let fetcher = MapFetcher::new();
        let err = join(&fetcher, &wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .unwrap_err();
        assert!(matches!(err, JoinError::UnsupportedRedundancy { level: 1 }));
    }

    /// `--allow-degraded-redundancy` masks the level byte and decodes
    /// the file as if it were level NONE. Verified end-to-end on a
    /// single-leaf root: the joiner trusts the masked span and returns
    /// the payload bytes truncated to that length.
    #[tokio::test]
    async fn allow_degraded_redundancy_masks_level_byte() {
        let payload = b"hello swarm".to_vec();
        let (_, mut wire) = cac_new(&payload).unwrap();
        // Splice a level into the root span. The CAC address mismatch
        // doesn't matter here — we feed the wire bytes straight into
        // `join_with_options`, bypassing the per-chunk verifier.
        wire[7] = 3; // INSANE
        let fetcher = MapFetcher::new();
        let opts = JoinOptions {
            allow_degraded_redundancy: true,
        };
        let out = join_with_options(&fetcher, &wire, DEFAULT_MAX_FILE_BYTES, opts)
            .await
            .unwrap();
        assert_eq!(out, payload);
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

    /// `effective_payload_size` strips trailing zero refs and rounds
    /// down to the last non-zero 32-byte slot. Pinned because a
    /// regression here re-introduces the all-zero fetch flood that
    /// shipped the joiner straight into a 30s timeout when serving a
    /// feed-backed `.eth` site (3 leaves padded to 128 ref slots →
    /// 125 spurious peer requests).
    #[test]
    fn effective_payload_size_strips_trailing_zeros() {
        // No padding: the whole payload is real refs.
        let mut full = vec![0u8; 128 * 32];
        for slot in full.chunks_mut(32) {
            slot[0] = 1;
        }
        assert_eq!(effective_payload_size(&full), 128 * 32);

        // 12 real refs followed by 116 zero slots = bee's typical
        // shape for a small file in a redundancy-NONE intermediate
        // chunk. Effective size is 12 * 32 = 384.
        let mut padded = vec![0u8; 128 * 32];
        for slot in padded[..12 * 32].chunks_mut(32) {
            slot[0] = 0xab;
        }
        assert_eq!(effective_payload_size(&padded), 12 * 32);

        // All zeros: caller surfaces this as a malformed chunk error.
        let zeros = vec![0u8; 4096];
        assert_eq!(effective_payload_size(&zeros), 0);

        // Single non-zero ref at the start.
        let mut one = vec![0u8; 4096];
        one[0] = 1;
        assert_eq!(effective_payload_size(&one), 32);
    }

    /// End-to-end: the joiner must NOT dispatch zero-address fetches
    /// when an intermediate chunk has trailing zero-padded ref slots.
    /// Build a 12-leaf file whose intermediate root is zero-padded to
    /// 128 slots, then prove `join` returns the joined bytes without
    /// ever asking `MapFetcher` for `[0u8; 32]` (`MapFetcher` panics
    /// on missing chunks, so a stray fetch surfaces as a test panic).
    #[tokio::test]
    async fn join_skips_trailing_zero_padded_refs() {
        let mut fetcher = MapFetcher::new();
        let mut leaf_addrs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..12 {
            let payload: Vec<u8> = (0..CHUNK_SIZE as u8).map(|b| b.wrapping_add(i)).collect();
            let (addr, wire) = cac_new(&payload).unwrap();
            fetcher.insert(addr, wire);
            leaf_addrs.push(addr);
            expected.extend_from_slice(&payload);
        }
        // Build the intermediate root: 12 real refs ‖ 116 zero refs.
        let mut root_payload = vec![0u8; 128 * 32];
        for (i, addr) in leaf_addrs.iter().enumerate() {
            root_payload[i * 32..(i + 1) * 32].copy_from_slice(addr);
        }
        let total_span = expected.len() as u64;
        let mut root_wire = Vec::new();
        root_wire.extend_from_slice(&total_span.to_le_bytes());
        root_wire.extend_from_slice(&root_payload);

        let out = join(&fetcher, &root_wire, DEFAULT_MAX_FILE_BYTES)
            .await
            .expect("join should succeed without zero-ref fetches");
        assert_eq!(out, expected);
    }

    /// Build a 130-leaf 3-level tree (same shape as `joins_three_level_tree`)
    /// and return the root wire bytes plus the expected joined body.
    /// Reused across the range tests below so each one stays focused on
    /// the slicing semantics rather than fixture construction.
    async fn build_three_level_fixture(fetcher: &mut MapFetcher) -> (Vec<u8>, Vec<u8>) {
        let mut leaf_addrs = Vec::with_capacity(130);
        let mut expected: Vec<u8> = Vec::with_capacity(130 * CHUNK_SIZE);
        for i in 0..130u8 {
            let payload = vec![i; CHUNK_SIZE];
            let (addr, wire) = cac_new(&payload).unwrap();
            fetcher.insert(addr, wire);
            leaf_addrs.push(addr);
            expected.extend_from_slice(&payload);
        }
        let mut int1_payload = Vec::with_capacity(128 * 32);
        for addr in &leaf_addrs[..128] {
            int1_payload.extend_from_slice(addr);
        }
        let int1_span: u64 = 128 * CHUNK_SIZE as u64;
        let int1_addr = chunk_addr(&int1_payload, int1_span);
        let mut int1_wire = Vec::new();
        int1_wire.extend_from_slice(&int1_span.to_le_bytes());
        int1_wire.extend_from_slice(&int1_payload);
        fetcher.insert(int1_addr, int1_wire);

        let mut int2_payload = Vec::with_capacity(2 * 32);
        for addr in &leaf_addrs[128..] {
            int2_payload.extend_from_slice(addr);
        }
        let int2_span: u64 = 2 * CHUNK_SIZE as u64;
        let int2_addr = chunk_addr(&int2_payload, int2_span);
        let mut int2_wire = Vec::new();
        int2_wire.extend_from_slice(&int2_span.to_le_bytes());
        int2_wire.extend_from_slice(&int2_payload);
        fetcher.insert(int2_addr, int2_wire);

        let mut root_payload = Vec::with_capacity(64);
        root_payload.extend_from_slice(&int1_addr);
        root_payload.extend_from_slice(&int2_addr);
        let total_span: u64 = 130 * CHUNK_SIZE as u64;
        let mut root_wire = Vec::new();
        root_wire.extend_from_slice(&total_span.to_le_bytes());
        root_wire.extend_from_slice(&root_payload);
        (root_wire, expected)
    }

    /// Drain the streaming joiner into a single buffer for comparison.
    async fn drain(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk);
        }
        out
    }

    /// `join_to_sender_range` with `range = None` matches `join_to_sender`
    /// byte-for-byte. Pins the no-range path against a regression that
    /// would otherwise show up only in integration.
    #[tokio::test]
    async fn range_none_matches_full_join() {
        let mut fetcher = MapFetcher::new();
        let (root_wire, expected) = build_three_level_fixture(&mut fetcher).await;
        let (tx, mut rx) = mpsc::channel(8);
        let join = tokio::spawn(async move {
            join_to_sender_range(
                &fetcher,
                &root_wire,
                DEFAULT_MAX_FILE_BYTES,
                JoinOptions::default(),
                None,
                tx,
            )
            .await
        });
        let body = drain(&mut rx).await;
        join.await.unwrap().unwrap();
        assert_eq!(body, expected);
    }

    /// Single-leaf prefix range. Verifies the leftmost-leaf slicing
    /// path returns exactly the requested bytes.
    #[tokio::test]
    async fn range_prefix_single_leaf() {
        let mut fetcher = MapFetcher::new();
        let (root_wire, expected) = build_three_level_fixture(&mut fetcher).await;
        let (tx, mut rx) = mpsc::channel(8);
        let join = tokio::spawn(async move {
            join_to_sender_range(
                &fetcher,
                &root_wire,
                DEFAULT_MAX_FILE_BYTES,
                JoinOptions::default(),
                Some(ByteRange {
                    start: 0,
                    end_inclusive: 999,
                }),
                tx,
            )
            .await
        });
        let body = drain(&mut rx).await;
        join.await.unwrap().unwrap();
        assert_eq!(body.len(), 1000);
        assert_eq!(body, expected[..1000]);
    }

    /// Suffix range crosses an L1 subtree boundary (last 4 KiB of int1 +
    /// first 4 KiB of int2). Exercises both right-edge slicing of int1
    /// and left-edge slicing of int2 in the same request.
    #[tokio::test]
    async fn range_crosses_subtree_boundary() {
        let mut fetcher = MapFetcher::new();
        let (root_wire, expected) = build_three_level_fixture(&mut fetcher).await;
        // int1 covers [0, 128*4096); int2 starts at 128*4096.
        let pivot = 128 * CHUNK_SIZE as u64;
        let start = pivot - 1024;
        let end = pivot + 1023;
        let (tx, mut rx) = mpsc::channel(8);
        let join = tokio::spawn(async move {
            join_to_sender_range(
                &fetcher,
                &root_wire,
                DEFAULT_MAX_FILE_BYTES,
                JoinOptions::default(),
                Some(ByteRange {
                    start,
                    end_inclusive: end,
                }),
                tx,
            )
            .await
        });
        let body = drain(&mut rx).await;
        join.await.unwrap().unwrap();
        let want = &expected[start as usize..=end as usize];
        assert_eq!(body, want);
    }

    /// Tail range of the very last leaf: this is the MP4 `moov`-at-end
    /// case the plan cares about. Browser issues a single
    /// `Range: bytes=<tail>-<total-1>` and only the trailing slice
    /// should reach the consumer.
    #[tokio::test]
    async fn range_tail_only() {
        let mut fetcher = MapFetcher::new();
        let (root_wire, expected) = build_three_level_fixture(&mut fetcher).await;
        let total = expected.len() as u64;
        let start = total - 256;
        let end = total - 1;
        let (tx, mut rx) = mpsc::channel(8);
        let join = tokio::spawn(async move {
            join_to_sender_range(
                &fetcher,
                &root_wire,
                DEFAULT_MAX_FILE_BYTES,
                JoinOptions::default(),
                Some(ByteRange {
                    start,
                    end_inclusive: end,
                }),
                tx,
            )
            .await
        });
        let body = drain(&mut rx).await;
        join.await.unwrap().unwrap();
        assert_eq!(body, &expected[start as usize..=end as usize]);
    }

    /// Range fetch must not touch chunks fully outside the requested
    /// window. Build a fixture where the second L1 subtree's leaves
    /// (last two 4 KiB chunks) are deliberately *not* inserted into
    /// the fetcher; a request for the first 4 KiB must succeed
    /// because the joiner never asks for them. The previous
    /// implementation joined first and sliced afterwards, so it would
    /// trip the missing-chunk error before returning a single byte.
    #[tokio::test]
    async fn range_avoids_fetching_unrelated_subtrees() {
        // Build the same 130-leaf tree, but only insert leaves 0..128
        // into the fetcher. The L1 #2 intermediate chunk is also
        // omitted so a stray fetch panics.
        let mut fetcher = MapFetcher::new();
        let mut leaf_addrs = Vec::with_capacity(130);
        for i in 0..130u8 {
            let payload = vec![i; CHUNK_SIZE];
            let (addr, wire) = cac_new(&payload).unwrap();
            // Skip leaves 128/129 — they live under int2 which we
            // expect never to touch.
            if i < 128 {
                fetcher.insert(addr, wire);
            }
            leaf_addrs.push(addr);
        }
        let mut int1_payload = Vec::with_capacity(128 * 32);
        for addr in &leaf_addrs[..128] {
            int1_payload.extend_from_slice(addr);
        }
        let int1_span: u64 = 128 * CHUNK_SIZE as u64;
        let int1_addr = chunk_addr(&int1_payload, int1_span);
        let mut int1_wire = Vec::new();
        int1_wire.extend_from_slice(&int1_span.to_le_bytes());
        int1_wire.extend_from_slice(&int1_payload);
        fetcher.insert(int1_addr, int1_wire);

        // Compute (but don't insert) int2. Its address must still be
        // valid in the root chunk so the joiner can pick it as a
        // candidate; we just expect the range filter to discard it.
        let mut int2_payload = Vec::with_capacity(2 * 32);
        for addr in &leaf_addrs[128..] {
            int2_payload.extend_from_slice(addr);
        }
        let int2_span: u64 = 2 * CHUNK_SIZE as u64;
        let int2_addr = chunk_addr(&int2_payload, int2_span);

        let mut root_payload = Vec::with_capacity(64);
        root_payload.extend_from_slice(&int1_addr);
        root_payload.extend_from_slice(&int2_addr);
        let total_span: u64 = 130 * CHUNK_SIZE as u64;
        let mut root_wire = Vec::new();
        root_wire.extend_from_slice(&total_span.to_le_bytes());
        root_wire.extend_from_slice(&root_payload);

        let (tx, mut rx) = mpsc::channel(8);
        let join = tokio::spawn(async move {
            join_to_sender_range(
                &fetcher,
                &root_wire,
                DEFAULT_MAX_FILE_BYTES,
                JoinOptions::default(),
                Some(ByteRange {
                    start: 0,
                    end_inclusive: 1023,
                }),
                tx,
            )
            .await
        });
        let body = drain(&mut rx).await;
        join.await.unwrap().expect("range fetch must succeed");
        assert_eq!(body.len(), 1024);
        assert!(body.iter().all(|&b| b == 0u8), "leaf 0 is all zeros");
    }
}

//! Postage stamp issuance — matches bee `pkg/postage/stamp.go` + `stampissuer.go`
//! serialization and signing (`signer.Sign` ⇒ EIP‑191 wrapping of digest).
//!
//! On-disk persistence
//! -------------------
//!
//! Bucket counters are issued strictly monotonically per (batch, bucket).
//! If the daemon restarts we **must not** re-issue an index already
//! handed to the network — bee peers would reject the second stamp as
//! a double-spend and quietly drop us (worse, they could downgrade their
//! reputation of our overlay).
//!
//! [`StampIssuer::open_or_new`] loads the counters from a small file at
//! a caller-supplied path; every successful [`StampIssuer::increment`]
//! writes the updated counters back atomically (`write tmp + fsync +
//! rename`) before the call returns. The on-disk state is therefore
//! always at-least-as-current as the most-recently-issued stamp, so a
//! crash anywhere after the persist call (including after pushsync has
//! shipped the chunk) is safe.
//!
//! File layout (little-endian throughout):
//!
//! ```text
//! [0..4]    magic  = b"ASBC"           (Ant Stamp Bucket Counters)
//! [4..6]    version = 0x0001
//! [6]       batch_depth
//! [7]       bucket_depth
//! [8]       immutable (0/1)
//! [9..16]   reserved (zeros)
//! [16..48]  batch_id
//! [48..N]   buckets: u32 LE * (1 << bucket_depth)
//! [N..N+32] keccak256 of bytes [0..N]   (corruption canary)
//! ```
//!
//! For depth=16 the file is 256 KiB; below the per-second IO budget of
//! any sane disk, and the typical upload-shaped workload only issues a
//! few hundred increments at a time interleaved with pushsync RTTs.

#[cfg(feature = "bee-recover")]
pub mod beestore;

use std::path::{Path, PathBuf};

use ant_crypto::{
    ethereum_address_from_public_key, recover_public_key, sign_handshake_data, CryptoError,
    SECP256K1_SECRET_LEN,
};
use k256::ecdsa::VerifyingKey;
use sha3::{Digest, Keccak256};
use thiserror::Error;

pub const INDEX_SIZE: usize = 8;
pub const TIMESTAMP_SIZE: usize = 8;
pub const SIGNATURE_LEN: usize = 65;
pub const STAMP_SIZE: usize = 32 + INDEX_SIZE + TIMESTAMP_SIZE + SIGNATURE_LEN;

#[derive(Debug, Error)]
pub enum PostageError {
    #[error("crypto: {0}")]
    Crypto(#[from] CryptoError),
    #[error("{0}")]
    Msg(String),
    #[error("bucket full")]
    BucketFull,
    #[error("persist bucket counters: {0}")]
    Persist(String),
    #[error("load bucket counters: {0}")]
    Load(String),
    /// On-disk counters were written for a different batch / depth, so
    /// the file probably belongs to another batch and we must refuse to
    /// continue rather than silently re-issuing indices.
    #[error("bucket store mismatch: on-disk batch={on_disk}, expected={expected}")]
    StoreMismatch { on_disk: String, expected: String },
}

/// Magic bytes identifying an Ant stamp-bucket-counter file. Catches a
/// caller pointing the issuer at, e.g., a stale Bee `postage.db` blob
/// of the same name before we'd corrupt their state.
const STORE_MAGIC: &[u8; 4] = b"ASBC";
/// On-disk file format version. Bumped when the layout changes.
const STORE_VERSION: u16 = 1;
/// Length of the fixed header (magic + version + depths + batch id +
/// reserved). Followed by `4 * (1 << bucket_depth)` bytes of u32-LE
/// counters and a 32-byte keccak256 trailer.
const STORE_HEADER_LEN: usize = 48;

/// Bee `stampIssuerData`-shaped issuer with only the fields Ant needs locally.
///
/// Persistence is opt-in: instances built via [`StampIssuer::new`] keep
/// counters in memory only and lose them on restart. Production callers
/// should always use [`StampIssuer::open_or_new`] so a daemon restart
/// resumes the same counter sequence the network already saw.
#[derive(Debug)]
pub struct StampIssuer {
    batch_id: [u8; 32],
    batch_depth: u8,
    bucket_depth: u8,
    immutable: bool,
    buckets: Vec<u32>,
    /// Optional path the issuer flushes counters to after every
    /// [`StampIssuer::increment`]. `None` for in-memory-only test
    /// instances.
    persist_path: Option<PathBuf>,
}

/// Snapshot of a [`StampIssuer`]'s capacity and fill, suitable for
/// ops dashboards and pre-flight upload checks. Returned by
/// [`StampIssuer::stats`].
///
/// Field semantics:
///
/// - `total_capacity` = `bucket_count * bucket_capacity` = `2^batch_depth`.
///   The number of stamps the batch can issue *if every bucket fills
///   evenly*. Practical capacity is lower because chunk addresses
///   route to specific buckets.
/// - `worst_case_remaining` = `(bucket_capacity − bucket_fill_max) *
///   bucket_count`. The number of new chunks guaranteed to fit no
///   matter how the next file's chunk addresses route (assumes the
///   worst case where every new chunk hashes into the
///   currently-fullest bucket). This is the right number to use as
///   a pre-flight budget for an immutable batch.
/// - `remaining_total` is the sum of free indices across buckets;
///   the *optimistic* counterpart to `worst_case_remaining` (would
///   only be reachable with perfectly even bucket routing).
#[derive(Debug, Clone, Copy)]
pub struct BucketStats {
    pub batch_id: [u8; 32],
    pub batch_depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
    /// Number of buckets in the batch (`2^bucket_depth`).
    pub bucket_count: u64,
    /// Per-bucket index ceiling (`2^(batch_depth − bucket_depth)`).
    pub bucket_capacity: u32,
    /// `bucket_count × bucket_capacity`.
    pub total_capacity: u64,
    /// Indices issued so far across every bucket.
    pub issued: u64,
    pub bucket_fill_min: u32,
    pub bucket_fill_max: u32,
    /// Sum of free indices across all buckets. Reachable only with
    /// perfectly-even chunk-address routing.
    pub remaining_total: u64,
    /// Conservative chunk budget: room left in the *currently
    /// fullest* bucket, scaled to a global count. Use this as the
    /// pre-flight ceiling for "will my next upload fit?" — see the
    /// type-level doc for the reasoning.
    pub worst_case_remaining: u64,
}

impl StampIssuer {
    /// Build an in-memory-only issuer. Useful for tests and one-shot
    /// CLI dry-runs; production callers should use
    /// [`StampIssuer::open_or_new`] so counters survive a restart.
    pub fn new(
        batch_id: [u8; 32],
        batch_depth: u8,
        bucket_depth: u8,
        immutable: bool,
    ) -> Result<Self, PostageError> {
        Self::build(batch_id, batch_depth, bucket_depth, immutable, None)
    }

    /// Open an existing on-disk store at `path` for this batch, or
    /// create a fresh zero-filled file if `path` doesn't exist.
    ///
    /// Refuses to load a file that was written for a different
    /// `(batch_id, batch_depth, bucket_depth)` triple — this catches
    /// `--data-dir` typos that would otherwise re-issue indices the
    /// network has already seen for a different batch.
    pub fn open_or_new(
        path: PathBuf,
        batch_id: [u8; 32],
        batch_depth: u8,
        bucket_depth: u8,
        immutable: bool,
    ) -> Result<Self, PostageError> {
        if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|e| PostageError::Load(format!("{}: {e}", path.display())))?;
            let header = decode_header(&bytes)?;
            if header.batch_id != batch_id {
                return Err(PostageError::StoreMismatch {
                    on_disk: hex::encode(header.batch_id),
                    expected: hex::encode(batch_id),
                });
            }
            if header.batch_depth != batch_depth || header.bucket_depth != bucket_depth {
                return Err(PostageError::StoreMismatch {
                    on_disk: format!(
                        "depth={} bucket_depth={}",
                        header.batch_depth, header.bucket_depth
                    ),
                    expected: format!("depth={batch_depth} bucket_depth={bucket_depth}"),
                });
            }
            let buckets = decode_buckets(&bytes, bucket_depth)?;
            return Ok(Self {
                batch_id,
                batch_depth,
                bucket_depth,
                immutable,
                buckets,
                persist_path: Some(path),
            });
        }
        let issuer = Self::build(batch_id, batch_depth, bucket_depth, immutable, Some(path))?;
        issuer.persist()?;
        Ok(issuer)
    }

    /// Open (or create) an on-disk store at `path`, seeding the bucket
    /// counters from `buckets` **only when the file does not already
    /// exist**. Used by the on-chain recovery path
    /// (`antd` startup) to adopt a batch rediscovered from the node EOA,
    /// carrying over the per-bucket counters recovered from a bee
    /// `stamperstore` so the node never re-stamps a slot the network has
    /// already seen.
    ///
    /// If a store already exists at `path` its counters win (they are
    /// at-least-as-current as anything we could seed) and `buckets` is
    /// ignored — exactly [`Self::open_or_new`]. `buckets.len()` must
    /// equal `2^bucket_depth`.
    pub fn open_or_new_seeded(
        path: PathBuf,
        batch_id: [u8; 32],
        batch_depth: u8,
        bucket_depth: u8,
        immutable: bool,
        buckets: &[u32],
    ) -> Result<Self, PostageError> {
        if path.exists() {
            return Self::open_or_new(path, batch_id, batch_depth, bucket_depth, immutable);
        }
        let n = 1usize
            .checked_shl(u32::from(bucket_depth))
            .ok_or_else(|| PostageError::Msg("bucket bitmap too large".into()))?;
        if buckets.len() != n {
            return Err(PostageError::Msg(format!(
                "seed buckets len {} != 2^bucket_depth {n}",
                buckets.len(),
            )));
        }
        let mut issuer = Self::build(batch_id, batch_depth, bucket_depth, immutable, Some(path))?;
        issuer.buckets = buckets.to_vec();
        issuer.persist()?;
        Ok(issuer)
    }

    /// Reload a persisted issuer from `path`, taking every parameter
    /// (`batch_id`, depths, immutability) from the file header rather
    /// than the caller. Used at daemon startup to rescan
    /// `<data-dir>/postage/*.bin` and resurrect every batch the node
    /// previously bought without a sidecar registry — the filename is
    /// the batch id and the header carries the rest.
    pub fn open_existing(path: PathBuf) -> Result<Self, PostageError> {
        let bytes = std::fs::read(&path)
            .map_err(|e| PostageError::Load(format!("{}: {e}", path.display())))?;
        let header = decode_header(&bytes)?;
        let buckets = decode_buckets(&bytes, header.bucket_depth)?;
        Ok(Self {
            batch_id: header.batch_id,
            batch_depth: header.batch_depth,
            bucket_depth: header.bucket_depth,
            immutable: header.immutable,
            buckets,
            persist_path: Some(path),
        })
    }

    /// Raise the batch depth in place after an on-chain `increaseDepth`
    /// ("dilute"). The collision-bucket count is `2^bucket_depth`, which
    /// is unchanged by a dilute, so the bucket vector keeps its length;
    /// only each bucket's index ceiling (`2^(batch_depth − bucket_depth)`)
    /// grows. We therefore just update the header field and re-persist —
    /// no counter is touched, so no index is ever re-issued. Refuses to
    /// lower the depth (that would shrink capacity below already-issued
    /// indices and let the network reject our stamps).
    pub fn set_batch_depth(&mut self, new_depth: u8) -> Result<(), PostageError> {
        if new_depth <= self.bucket_depth {
            return Err(PostageError::Msg(format!(
                "new depth {new_depth} must exceed bucket_depth {}",
                self.bucket_depth
            )));
        }
        if new_depth < self.batch_depth {
            return Err(PostageError::Msg(format!(
                "refusing to lower batch depth {} → {new_depth}",
                self.batch_depth
            )));
        }
        self.batch_depth = new_depth;
        self.persist()
    }

    fn build(
        batch_id: [u8; 32],
        batch_depth: u8,
        bucket_depth: u8,
        immutable: bool,
        persist_path: Option<PathBuf>,
    ) -> Result<Self, PostageError> {
        if bucket_depth == 0 || bucket_depth >= batch_depth {
            return Err(PostageError::Msg(
                "bucketDepth must be >0 and < batchDepth".into(),
            ));
        }
        let n = 1usize
            .checked_shl(u32::from(bucket_depth))
            .ok_or_else(|| PostageError::Msg("bucket bitmap too large".into()))?;
        Ok(Self {
            batch_id,
            batch_depth,
            bucket_depth,
            immutable,
            buckets: vec![0u32; n],
            persist_path,
        })
    }

    #[must_use]
    pub const fn bucket_depth(&self) -> u8 {
        self.bucket_depth
    }

    #[must_use]
    pub const fn batch_depth(&self) -> u8 {
        self.batch_depth
    }

    #[must_use]
    pub const fn batch_id(&self) -> &[u8; 32] {
        &self.batch_id
    }

    #[must_use]
    pub const fn immutable(&self) -> bool {
        self.immutable
    }

    /// Total number of indices issued so far across every bucket. Used
    /// for ops dashboards / "batch is N% full" reporting; not on the
    /// critical path.
    #[must_use]
    pub fn issued_count(&self) -> u64 {
        self.buckets.iter().map(|&n| u64::from(n)).sum()
    }

    /// Per-bucket fill summary used by `antctl postage status` and the
    /// upload pre-flight check. Walks the in-memory bucket vector
    /// once: `O(2^bucket_depth)` — that's 65 536 elements at the bee
    /// default, ~50 µs in release. Cheap to call on every status
    /// tick.
    #[must_use]
    pub fn stats(&self) -> BucketStats {
        let bucket_count = self.buckets.len() as u64;
        let bucket_capacity = self.bucket_upper_bound();
        let total_capacity = bucket_count.saturating_mul(u64::from(bucket_capacity));
        let issued: u64 = self.buckets.iter().map(|&n| u64::from(n)).sum();
        let (bucket_fill_min, bucket_fill_max) = if self.buckets.is_empty() {
            (0u32, 0u32)
        } else {
            self.buckets
                .iter()
                .copied()
                .fold((u32::MAX, 0u32), |(mn, mx), v| (mn.min(v), mx.max(v)))
        };
        let remaining_total: u64 = self
            .buckets
            .iter()
            .map(|&n| u64::from(bucket_capacity.saturating_sub(n)))
            .sum();
        let worst_case_remaining =
            u64::from(bucket_capacity.saturating_sub(bucket_fill_max)) * bucket_count;
        BucketStats {
            batch_id: self.batch_id,
            batch_depth: self.batch_depth,
            bucket_depth: self.bucket_depth,
            immutable: self.immutable,
            bucket_count,
            bucket_capacity,
            total_capacity,
            issued,
            bucket_fill_min,
            bucket_fill_max,
            remaining_total,
            worst_case_remaining,
        }
    }

    fn bucket_upper_bound(&self) -> u32 {
        1u32 << u32::from(self.batch_depth - self.bucket_depth)
    }

    /// True if the collision bucket the chunk at `addr` maps to is
    /// already at capacity. The next stamp for such a chunk would, on a
    /// mutable batch, wrap the bucket counter back to 0 and re-issue an
    /// already-used `(bucket, index)` slot with a newer timestamp — which
    /// bee storers resolve by keeping the newer chunk and **evicting**
    /// the older one. On a too-small batch (e.g. depth 17, only
    /// `2^(depth-bucket_depth)` slots per bucket) that silently drops
    /// chunks — including chunks of the *same* upload — leaving the
    /// content unretrievable network-wide. Callers that need durable
    /// storage should refuse to stamp when this returns `true` rather
    /// than evict. (An *immutable* batch already errors with
    /// [`PostageError::BucketFull`] in [`Self::increment`].)
    #[must_use]
    pub fn bucket_is_full(&self, addr: &[u8; 32]) -> bool {
        let b_idx = collision_bucket_from_addr(self.bucket_depth, addr);
        self.buckets.get(b_idx as usize).copied().unwrap_or(0) >= self.bucket_upper_bound()
    }

    /// True when the fullest bucket is already at capacity, so *some*
    /// chunk addresses can no longer be stamped without evicting an
    /// earlier chunk (see [`Self::bucket_is_full`]). Equivalent to
    /// `stats().worst_case_remaining == 0`, but without allocating /
    /// walking for the full [`BucketStats`]. Use as a cheap pre-flight
    /// "this batch is saturated" signal.
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        let upper = self.bucket_upper_bound();
        self.buckets.iter().any(|&n| n >= upper)
    }

    /// Reserve the next index in the bucket the chunk address falls in.
    ///
    /// **Persistence**: when this issuer was built via
    /// [`StampIssuer::open_or_new`], the updated counters are flushed
    /// to disk before this call returns. A crash anywhere after this
    /// point is therefore safe; a crash *before* this point loses an
    /// in-flight stamp the network never saw.
    pub fn increment(
        &mut self,
        addr: &[u8; 32],
    ) -> Result<([u8; INDEX_SIZE], [u8; TIMESTAMP_SIZE]), PostageError> {
        let b_idx = collision_bucket_from_addr(self.bucket_depth, addr);
        let cnt = self.buckets.get(b_idx as usize).copied().unwrap_or(0);
        let upper = self.bucket_upper_bound();

        let mut cnt = cnt;
        if cnt == upper {
            if self.immutable {
                return Err(PostageError::BucketFull);
            }
            cnt = 0;
            self.buckets[b_idx as usize] = 0;
        }

        cnt += 1;
        self.buckets[b_idx as usize] = cnt;
        self.persist()?;

        Ok((index_bytes(b_idx, cnt - 1), unix_now_nanos_be()))
    }

    /// Write the current counter state to disk atomically: write to a
    /// `.tmp` sibling, fsync, rename over the canonical path. No-op for
    /// in-memory issuers (no `persist_path`). Surfaced in the public
    /// API so ops can force a flush on demand (e.g. before snapshotting
    /// the data dir for migration).
    pub fn persist(&self) -> Result<(), PostageError> {
        let Some(path) = &self.persist_path else {
            return Ok(());
        };
        let bytes = encode_store(
            &self.batch_id,
            self.batch_depth,
            self.bucket_depth,
            self.immutable,
            &self.buckets,
        );
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    PostageError::Persist(format!("create_dir_all {}: {e}", parent.display()))
                })?;
            }
        }
        let tmp = path.with_extension("tmp");
        atomic_write(&tmp, path, &bytes)
            .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
        Ok(())
    }
}

/// Decoded header (everything before the buckets blob) used to
/// validate that an on-disk store really belongs to the batch we're
/// about to use it with.
struct StoreHeader {
    batch_id: [u8; 32],
    batch_depth: u8,
    bucket_depth: u8,
    immutable: bool,
}

fn encode_store(
    batch_id: &[u8; 32],
    batch_depth: u8,
    bucket_depth: u8,
    immutable: bool,
    buckets: &[u32],
) -> Vec<u8> {
    let body_len = STORE_HEADER_LEN + 4 * buckets.len();
    let mut out = Vec::with_capacity(body_len + 32);
    out.extend_from_slice(STORE_MAGIC);
    out.extend_from_slice(&STORE_VERSION.to_le_bytes());
    out.push(batch_depth);
    out.push(bucket_depth);
    out.push(u8::from(immutable));
    out.extend_from_slice(&[0u8; 7]);
    out.extend_from_slice(batch_id);
    for &b in buckets {
        out.extend_from_slice(&b.to_le_bytes());
    }
    debug_assert_eq!(out.len(), body_len);
    let digest = Keccak256::digest(&out);
    out.extend_from_slice(&digest);
    out
}

fn decode_header(bytes: &[u8]) -> Result<StoreHeader, PostageError> {
    if bytes.len() < STORE_HEADER_LEN + 32 {
        return Err(PostageError::Load(format!(
            "file too short: {} bytes (need ≥ {})",
            bytes.len(),
            STORE_HEADER_LEN + 32
        )));
    }
    if &bytes[0..4] != STORE_MAGIC {
        return Err(PostageError::Load(format!(
            "bad magic: 0x{}",
            hex::encode(&bytes[0..4])
        )));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != STORE_VERSION {
        return Err(PostageError::Load(format!(
            "unsupported version {version} (expected {STORE_VERSION})"
        )));
    }
    let batch_depth = bytes[6];
    let bucket_depth = bytes[7];
    let immutable = bytes[8] != 0;
    let mut batch_id = [0u8; 32];
    batch_id.copy_from_slice(&bytes[16..48]);
    Ok(StoreHeader {
        batch_id,
        batch_depth,
        bucket_depth,
        immutable,
    })
}

fn decode_buckets(bytes: &[u8], bucket_depth: u8) -> Result<Vec<u32>, PostageError> {
    let n = 1usize
        .checked_shl(u32::from(bucket_depth))
        .ok_or_else(|| PostageError::Load("bucket_depth overflow".into()))?;
    let body_len = STORE_HEADER_LEN + 4 * n;
    if bytes.len() != body_len + 32 {
        return Err(PostageError::Load(format!(
            "size mismatch: file is {} bytes, expected {}",
            bytes.len(),
            body_len + 32,
        )));
    }
    let body = &bytes[..body_len];
    let trailer = &bytes[body_len..body_len + 32];
    let digest = Keccak256::digest(body);
    if trailer != digest.as_slice() {
        return Err(PostageError::Load(format!(
            "checksum mismatch (file: {}, computed: {})",
            hex::encode(trailer),
            hex::encode(digest)
        )));
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let off = STORE_HEADER_LEN + 4 * i;
        out.push(u32::from_le_bytes([
            body[off],
            body[off + 1],
            body[off + 2],
            body[off + 3],
        ]));
    }
    Ok(out)
}

fn atomic_write(tmp: &Path, dest: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(tmp, dest)?;
    if let Some(parent) = dest.parent() {
        // Best-effort fsync of the directory so the rename is durable.
        // Platforms where opening a directory for fsync isn't supported
        // (Windows) just silently skip — we cap that here so a failure
        // doesn't bubble up as a fatal stamp issuance error.
        if let Ok(d) = std::fs::File::open(parent) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

#[must_use]
pub fn unix_now_nanos_be() -> [u8; TIMESTAMP_SIZE] {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let mut b = [0u8; TIMESTAMP_SIZE];
    b.copy_from_slice(&now.to_be_bytes());
    b
}

/// Matches bee `pkg/postage/stampissuer.go::toBucket`: top `depth` bits of address as BE u32,
/// masked to collision bucket slots.
#[must_use]
pub fn collision_bucket_from_addr(bucket_depth: u8, addr: &[u8; 32]) -> u32 {
    let top = u32::from_be_bytes(addr[0..4].try_into().unwrap());
    top >> (32 - u32::from(bucket_depth))
}

fn index_bytes(bucket: u32, within_bucket: u32) -> [u8; INDEX_SIZE] {
    let mut out = [0u8; INDEX_SIZE];
    out[0..4].copy_from_slice(&bucket.to_be_bytes());
    out[4..8].copy_from_slice(&within_bucket.to_be_bytes());
    out
}

#[must_use]
pub fn postage_sign_digest(
    chunk_addr: &[u8; 32],
    batch_id: &[u8; 32],
    index: &[u8; 8],
    ts: &[u8; 8],
) -> [u8; 32] {
    let mut h = Keccak256::default();
    h.update(chunk_addr);
    h.update(batch_id);
    h.update(index);
    h.update(ts);
    let out = h.finalize();
    out.into()
}

/// Build a stamped full binary stamp — same layout as bee `Stamp.MarshalBinary`.
pub fn sign_stamp_bytes(
    secret: &[u8; SECP256K1_SECRET_LEN],
    issuer: &mut StampIssuer,
    chunk_address: &[u8; 32],
) -> Result<[u8; STAMP_SIZE], PostageError> {
    let (index, ts) = issuer.increment(chunk_address)?;
    let digest = postage_sign_digest(chunk_address, &issuer.batch_id, &index, &ts);
    let sig = sign_handshake_data(secret, &digest)?;

    let mut stamp = [0u8; STAMP_SIZE];
    stamp[0..32].copy_from_slice(&issuer.batch_id);
    stamp[32..40].copy_from_slice(&index);
    stamp[40..48].copy_from_slice(&ts);
    stamp[48..113].copy_from_slice(&sig);
    Ok(stamp)
}

pub fn stamp_batch_id(stamp: &[u8]) -> Result<&[u8; 32], PostageError> {
    if stamp.len() != STAMP_SIZE {
        return Err(PostageError::Msg("bad stamp size".into()));
    }
    Ok(stamp[0..32].try_into().unwrap())
}

/// Verify ECDSA signer is `expected_eth` — matches bee `RecoverBatchOwner`.
pub fn verify_stamp_owner(
    chunk_addr: &[u8; 32],
    stamp: &[u8; STAMP_SIZE],
    expected_eth: &[u8; 20],
    depth: u8,
    bucket_depth: u8,
) -> Result<(), PostageError> {
    let batch_id = stamp[0..32].try_into().unwrap();
    let index: &[u8; 8] = stamp[32..40].try_into().unwrap();
    let ts: &[u8; 8] = stamp[40..48].try_into().unwrap();
    let sig: &[u8; 65] = stamp[48..113].try_into().unwrap();

    let digest_arr = postage_sign_digest(chunk_addr, batch_id, index, ts);
    let digest_slice: &[u8] = digest_arr.as_ref();
    let vk = recover_public_key(sig, digest_slice)?;

    bucket_check(chunk_addr, *index, bucket_depth, depth)?;

    let eth = ethereum_address_from_public_key(&vk);
    if eth != *expected_eth {
        return Err(PostageError::Msg("stamp owner mismatch".into()));
    }
    Ok(())
}

fn bucket_check(
    chunk_addr: &[u8; 32],
    index: [u8; 8],
    bucket_depth: u8,
    depth: u8,
) -> Result<(), PostageError> {
    let bucket = collision_bucket_from_addr(bucket_depth, chunk_addr);
    let exp_bucket = read_u32_be(&index[0..4]);
    if bucket != exp_bucket {
        return Err(PostageError::Msg("bucket mismatch".into()));
    }

    let within = read_u32_be(&index[4..8]);
    let max_idx = 1u32 << u32::from(depth - bucket_depth);
    if within >= max_idx {
        return Err(PostageError::Msg("invalid stamp index".into()));
    }
    Ok(())
}

fn read_u32_be(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().unwrap())
}

/// Optional signer identity check helper.
#[must_use]
pub fn eth_address_matches_key(pubkey_eth: &[u8; 20], vk: &VerifyingKey) -> bool {
    ethereum_address_from_public_key(vk) == *pubkey_eth
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::random_secp256k1_secret;
    use std::path::PathBuf;

    fn tmpdir() -> PathBuf {
        // A bare nanosecond timestamp collides when two tests start
        // within the same clock tick (coarse on some platforms), and
        // their per-test `remove_dir_all` then races — one test wipes
        // another's `batch.bin`. A per-call atomic counter guarantees a
        // distinct directory regardless of clock resolution.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "ant-postage-test-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            n,
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn stamp_roundtrip_owner() {
        let secret = random_secp256k1_secret();
        let sk = k256::ecdsa::SigningKey::from_bytes(&secret.into()).unwrap();
        let vk = *sk.verifying_key();
        let owner = ethereum_address_from_public_key(&vk);
        let mut batch = [0u8; 32];
        batch[0] = 0xab;
        let mut issuer = StampIssuer::new(batch, 21, 16, false).unwrap();
        let chunk = [7u8; 32];

        let stamp = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        verify_stamp_owner(
            &chunk,
            &stamp,
            &owner,
            issuer.batch_depth,
            issuer.bucket_depth,
        )
        .unwrap();
    }

    /// The headroom predicates must flag a collision bucket as full once
    /// it reaches capacity — the signal the upload path uses to refuse to
    /// stamp (and thus silently evict) on an under-sized batch.
    #[test]
    fn bucket_full_predicate_flags_saturation() {
        let batch_id = [0x5au8; 32];
        // depth 17, bucket_depth 16 => 2^(17-16) = 2 slots per bucket
        // (the postage minimum — exactly the batch that broke uploads).
        let mut issuer = StampIssuer::new(batch_id, 17, 16, false).unwrap();
        let chunk = [9u8; 32];

        assert!(!issuer.bucket_is_full(&chunk));
        assert!(!issuer.is_saturated());

        // First slot used — still room for one more.
        issuer.increment(&chunk).unwrap();
        assert!(!issuer.bucket_is_full(&chunk));

        // Second slot fills the bucket to capacity: a third stamp would
        // wrap and evict, so the predicate must now refuse it.
        issuer.increment(&chunk).unwrap();
        assert!(issuer.bucket_is_full(&chunk));
        assert!(issuer.is_saturated());

        // A chunk routing to an untouched bucket is still stampable.
        let mut other = [0u8; 32];
        other[0] = 0xff;
        other[1] = 0xff;
        assert!(!issuer.bucket_is_full(&other));
    }

    /// Writing then re-loading the store must preserve every counter
    /// exactly. Catches LE/BE mistakes and length-of-buckets drift.
    #[test]
    fn store_round_trips_counters() {
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch_id = [0xabu8; 32];

        // Use a small bucket_depth so the file stays test-friendly (8 → 256
        // counters → 1 KiB file). batch_depth must be > bucket_depth.
        let mut issuer = StampIssuer::open_or_new(path.clone(), batch_id, 12, 8, true).unwrap();
        // Issue indices on a few different addresses to populate
        // distinct buckets. The exact bucket choice depends on
        // collision_bucket_from_addr but we don't care which buckets
        // get hit — only that they round-trip.
        for i in 0..50u8 {
            let mut addr = [0u8; 32];
            addr[0] = i;
            issuer.increment(&addr).unwrap();
        }
        let total_before = issuer.issued_count();
        assert_eq!(total_before, 50);
        let buckets_before = issuer.buckets.clone();

        // Reload. Counters must match exactly.
        drop(issuer);
        let reloaded = StampIssuer::open_or_new(path, batch_id, 12, 8, true).unwrap();
        assert_eq!(reloaded.issued_count(), 50);
        assert_eq!(reloaded.buckets, buckets_before);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Crash recovery: the persisted state must reflect the most recent
    /// successful `increment`. We simulate the crash by issuing N
    /// stamps, dropping the issuer, and verifying the next instance
    /// keeps issuing from index N — never index 0.
    #[test]
    fn no_index_reissue_after_crash() {
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch_id = [0xcdu8; 32];

        // First run: issue 5 stamps on the same chunk address.
        let mut issuer1 = StampIssuer::open_or_new(path.clone(), batch_id, 14, 8, true).unwrap();
        let chunk = [0u8; 32]; // same bucket every time
        let mut issued: Vec<[u8; 8]> = Vec::new();
        for _ in 0..5 {
            let (idx, _) = issuer1.increment(&chunk).unwrap();
            issued.push(idx);
        }
        // Indices within the bucket should be 0..4.
        for (i, idx) in issued.iter().enumerate() {
            let within = u32::from_be_bytes(idx[4..8].try_into().unwrap());
            assert_eq!(within as usize, i);
        }

        // Simulated crash: drop, then re-open with no graceful shutdown.
        drop(issuer1);

        let mut issuer2 = StampIssuer::open_or_new(path, batch_id, 14, 8, true).unwrap();
        // Next index for the same bucket must be 5, not 0.
        let (idx, _) = issuer2.increment(&chunk).unwrap();
        let within = u32::from_be_bytes(idx[4..8].try_into().unwrap());
        assert_eq!(within, 5, "issuer must resume after crash, not reset");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Pointing the issuer at a store from a *different* batch must
    /// fail with `StoreMismatch` so a `--data-dir` typo doesn't cause
    /// us to re-issue indices for the wrong batch.
    #[test]
    fn rejects_store_from_different_batch() {
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch_a = [0xaau8; 32];
        let batch_b = [0xbbu8; 32];

        // Create a store for batch A.
        let _ = StampIssuer::open_or_new(path.clone(), batch_a, 12, 8, true).unwrap();

        // Try to open the same file for batch B.
        let err = StampIssuer::open_or_new(path, batch_b, 12, 8, true).unwrap_err();
        assert!(
            matches!(err, PostageError::StoreMismatch { .. }),
            "expected StoreMismatch, got {err:?}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Tampering with a single byte after the store was written must
    /// be caught by the checksum trailer.
    #[test]
    fn detects_corruption() {
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch_id = [0xeeu8; 32];
        {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch_id, 12, 8, true).unwrap();
            // Issue at least one stamp so the buckets blob is non-zero
            // somewhere.
            issuer.increment(&[0u8; 32]).unwrap();
        }
        // Flip a single byte midway through the buckets blob.
        let mut bytes = std::fs::read(&path).unwrap();
        let target = STORE_HEADER_LEN + 16;
        bytes[target] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let err = StampIssuer::open_or_new(path, batch_id, 12, 8, true).unwrap_err();
        assert!(
            matches!(err, PostageError::Load(_)),
            "expected Load error on checksum mismatch, got {err:?}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}

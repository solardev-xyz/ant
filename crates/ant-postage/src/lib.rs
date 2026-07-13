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

use std::collections::HashMap;
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
    /// Per-chunk stamp cache: maps a chunk address to the latest stamp
    /// issued for it under this batch. Makes stamping **idempotent** —
    /// re-pushing a chunk (post-upload heal, a manual retry, an
    /// interrupted-upload resume) reuses the same bucket **slot** instead
    /// of burning a fresh index. Without this a chunk re-pushed N times
    /// consumes N slots in its collision bucket, so a few heal rounds can
    /// saturate an otherwise-roomy batch. Backed by the `.stamps` sidecar
    /// (see [`append_stamp_record`]): each stamp (fresh or refreshed) is
    /// **appended** with a checksum, so a torn write only ever damages
    /// the trailing record and the address's previous valid record still
    /// recovers its slot. The file is compacted (see [`rewrite_sidecar`])
    /// once it grows past a small multiple of the live entry count, so
    /// re-stamps don't grow it without bound.
    seen: HashMap<[u8; 32], [u8; STAMP_SIZE]>,
    /// Number of physical records currently in the sidecar (≥ `seen`
    /// when re-stamps have appended newer copies not yet compacted away).
    /// Drives the compaction trigger. `0` for in-memory issuers.
    stamp_record_count: usize,
    /// Sidecar path the issued stamps are written to (sibling of
    /// `persist_path`, `.stamps` extension). `None` for in-memory issuers.
    stamps_path: Option<PathBuf>,
    /// Set when a *persistent* issuer's sidecar could not be made
    /// writable (a v1 file whose migration to v2 failed). It is
    /// distinct from an intentional in-memory issuer (`stamps_path`
    /// `None`, this `false`): here the caller **expected** durability,
    /// so [`sign_stamp_bytes`] refuses to issue — a stamp we can't
    /// persist would push to the network and then lose its
    /// address→slot mapping on restart, burning a fresh index on
    /// re-push (and, at capacity, evicting another chunk or being
    /// rejected on an immutable batch).
    stamp_persistence_failed: bool,
}

/// Magic for the per-chunk stamp sidecar (Ant `STamp` Map).
const STAMP_STORE_MAGIC: &[u8; 4] = b"ASTM";
/// Sidecar format version. v2 appends checksummed records (v1 had no
/// checksum). A v1 file is **migrated** on load — its records are read
/// and rewritten as v2 — rather than discarded, so no reusable slot is
/// lost across the upgrade.
const STAMP_STORE_VERSION: u16 = 2;
/// The pre-checksum format: `addr(32) ‖ stamp(113)` records, no
/// per-record checksum. Read only to migrate forward.
const STAMP_STORE_VERSION_V1: u16 = 1;
/// v1 record length (no checksum).
const STAMP_RECORD_LEN_V1: usize = 32 + STAMP_SIZE;
/// Compact the sidecar once physical records exceed
/// `live × STAMP_COMPACT_FACTOR + STAMP_COMPACT_FLOOR`, bounding the
/// growth that append-on-re-stamp would otherwise cause to ~2× the live
/// entry count while keeping small files append-only (cheap).
const STAMP_COMPACT_FACTOR: usize = 2;
const STAMP_COMPACT_FLOOR: usize = 64;
/// Sidecar header: `magic(4) + version(2) + batch_id(32)`.
const STAMP_STORE_HEADER_LEN: usize = 38;
/// Trailing checksum on each record: first 4 bytes of
/// `keccak256(addr ‖ stamp)`. Detects a torn record from a
/// crash-interrupted write.
const STAMP_CHECKSUM_LEN: usize = 4;
/// One record: chunk address(32) + stamp([`STAMP_SIZE`]) + checksum(4).
const STAMP_RECORD_LEN: usize = 32 + STAMP_SIZE + STAMP_CHECKSUM_LEN;

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
            let mut issuer = Self {
                batch_id,
                batch_depth,
                bucket_depth,
                immutable,
                buckets,
                persist_path: Some(path),
                seen: HashMap::new(),
                stamp_record_count: 0,
                stamps_path: None,
                stamp_persistence_failed: false,
            };
            issuer.attach_stamp_store();
            return Ok(issuer);
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
        let mut issuer = Self {
            batch_id: header.batch_id,
            batch_depth: header.batch_depth,
            bucket_depth: header.bucket_depth,
            immutable: header.immutable,
            buckets,
            persist_path: Some(path),
            seen: HashMap::new(),
            stamp_record_count: 0,
            stamps_path: None,
            stamp_persistence_failed: false,
        };
        issuer.attach_stamp_store();
        Ok(issuer)
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
        let mut issuer = Self {
            batch_id,
            batch_depth,
            bucket_depth,
            immutable,
            buckets: vec![0u32; n],
            persist_path,
            seen: HashMap::new(),
            stamp_record_count: 0,
            stamps_path: None,
            stamp_persistence_failed: false,
        };
        issuer.attach_stamp_store();
        Ok(issuer)
    }

    /// Wire the per-chunk stamp sidecar next to the counter file and
    /// load any previously-issued stamps into [`Self::seen`]. No-op for
    /// in-memory issuers (no `persist_path`). Best-effort: a missing or
    /// corrupt sidecar yields an empty cache — the worst case is that a
    /// future re-push re-stamps (the pre-fix behaviour), never a wrong
    /// stamp.
    fn attach_stamp_store(&mut self) {
        if let Some(p) = &self.persist_path {
            let sidecar = stamps_sidecar_path(p);
            let loaded = load_stamp_store(&sidecar, &self.batch_id);
            self.seen = loaded.map;
            if loaded.appendable {
                self.stamp_record_count = loaded.physical_count;
                self.stamps_path = Some(sidecar);
            } else {
                // v1 migration failed. Appending v2 records under the v1
                // header would corrupt it, so the sidecar is left intact
                // for a future restart to migrate — but this issuer
                // CANNOT persist new stamps. Mark persistence failed so
                // `sign_stamp_bytes` refuses to issue: a stamp we can't
                // record would push to the network and then lose its slot
                // mapping on restart. (The recovered stamps stay in
                // memory so reads / `has_stamp` still reflect reality.)
                self.stamp_record_count = 0;
                self.stamps_path = None;
                self.stamp_persistence_failed = true;
            }
        }
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

    /// Max collisions per bucket: `2^(batch_depth - bucket_depth)`.
    /// Public because bee's `GET /stamps/{id}/buckets` surfaces it as
    /// `bucketUpperBound`.
    #[must_use]
    pub fn bucket_upper_bound(&self) -> u32 {
        1u32 << u32::from(self.batch_depth - self.bucket_depth)
    }

    /// Raw per-bucket collision counters, indexed by bucket id. Backs
    /// bee's `GET /stamps/{id}/buckets` (`issuer.Buckets()`).
    #[must_use]
    pub fn bucket_counts(&self) -> &[u32] {
        &self.buckets
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

    /// The stamp already issued for `addr` under this batch, if any.
    /// A `Some` means a re-push can reuse it for free; callers should
    /// check this *before* the [`Self::bucket_is_full`] pre-flight so a
    /// re-push of an already-stamped chunk is never blocked by a now-full
    /// bucket (it consumes no new slot).
    #[must_use]
    pub fn cached_stamp(&self, addr: &[u8; 32]) -> Option<[u8; STAMP_SIZE]> {
        self.seen.get(addr).copied()
    }

    /// Whether a stamp has already been issued for `addr`. Cheap
    /// companion to [`Self::cached_stamp`] for guard conditions.
    #[must_use]
    pub fn has_stamp(&self, addr: &[u8; 32]) -> bool {
        self.seen.contains_key(addr)
    }

    /// Remember the stamp issued for a **new** `addr` and, for persistent
    /// issuers, append its checksummed record to the `.stamps` sidecar
    /// (append + fsync) so a restart recovers it. `O(1)`.
    fn record_stamp(
        &mut self,
        addr: [u8; 32],
        stamp: [u8; STAMP_SIZE],
    ) -> Result<(), PostageError> {
        self.seen.insert(addr, stamp);
        self.append_and_maybe_compact(addr, stamp)
    }

    /// Refresh the stamp for an already-recorded address (a re-stamp that
    /// only changed the timestamp/signature — the bucket slot is
    /// unchanged). Same on-disk path as a fresh stamp: **append** a new
    /// checksummed record (never overwrite in place). Appending is what
    /// makes a torn write survivable — a crash mid-append can only damage
    /// the trailing record, and the address's previous valid record still
    /// recovers its slot on load — while [`Self::append_and_maybe_compact`]
    /// bounds growth so re-stamps don't grow the file without limit.
    fn refresh_cached_stamp(
        &mut self,
        addr: [u8; 32],
        stamp: [u8; STAMP_SIZE],
    ) -> Result<(), PostageError> {
        self.seen.insert(addr, stamp);
        self.append_and_maybe_compact(addr, stamp)
    }

    /// Append one record to the sidecar, then compact if the physical
    /// record count has grown past a small multiple of the live entry
    /// count (re-stamps append newer copies that pile up until then).
    /// Compaction rewrites the file atomically (temp + rename) with one
    /// record per live address.
    fn append_and_maybe_compact(
        &mut self,
        addr: [u8; 32],
        stamp: [u8; STAMP_SIZE],
    ) -> Result<(), PostageError> {
        let Some(path) = self.stamps_path.clone() else {
            return Ok(()); // in-memory issuer
        };
        append_stamp_record(&path, &self.batch_id, &addr, &stamp)?;
        self.stamp_record_count += 1;
        if self.stamp_record_count > self.seen.len() * STAMP_COMPACT_FACTOR + STAMP_COMPACT_FLOOR {
            rewrite_sidecar(&path, &self.batch_id, &self.seen)?;
            self.stamp_record_count = self.seen.len();
        }
        Ok(())
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
    // Refuse to issue when a persistent issuer's sidecar is broken.
    // Failing closed here — before consuming a bucket index — is the
    // only safe choice: a stamp we can't persist would push to the
    // network and then vanish from our address→slot map on restart, so
    // a re-push burns a fresh index and, at capacity, evicts another
    // chunk (mutable) or is rejected (immutable). An *intentional*
    // in-memory issuer (`stamp_persistence_failed` false) is unaffected.
    if issuer.stamp_persistence_failed {
        return Err(PostageError::Persist(
            "stamp sidecar unavailable (v1 migration failed); refusing to issue a \
             non-durable stamp — fix the data dir and restart to migrate"
                .to_string(),
        ));
    }
    // Idempotent per chunk SLOT, not per stamp: an address we've stamped
    // before reuses its (bucket, index) — a re-push (heal / retry /
    // resume) costs no new bucket slot — but the timestamp is refreshed
    // and the stamp re-signed, mirroring bee's stamper
    // (`pkg/postage/stamper.go`: a known address gets
    // `BatchTimestamp = unixTime()` before signing). Returning the
    // byte-identical old stamp instead breaks single-owner-chunk
    // *updates*: bee's reserve treats a same-(batch, index) put whose
    // timestamp is not strictly newer as `ErrOverwriteNewerChunk` and
    // rejects it, so a GSOC update at its stable address would never
    // land on any storer (found live on mainnet, 2026-07-13: update #2
    // of a GSOC room was acked but silently dropped network-wide).
    if let Some(prev) = issuer.cached_stamp(chunk_address) {
        let index: [u8; INDEX_SIZE] = prev[32..40].try_into().expect("stamp layout");
        // Strictly-newer timestamp, guaranteed. bee's reserve rejects a
        // same-(batch, index) put whose timestamp isn't greater than the
        // stored one, so `now` alone is unsafe: coarse clock resolution
        // (two re-stamps in one tick) or a backward clock step would
        // re-create the very rejection this path fixes. Take
        // `max(now, prev + 1)`.
        let prev_ts = u64::from_be_bytes(prev[40..48].try_into().expect("stamp layout"));
        let now = u64::from_be_bytes(unix_now_nanos_be());
        let ts = now.max(prev_ts.saturating_add(1)).to_be_bytes();
        let digest = postage_sign_digest(chunk_address, &issuer.batch_id, &index, &ts);
        let sig = sign_handshake_data(secret, &digest)?;
        let mut fresh = [0u8; STAMP_SIZE];
        fresh[0..32].copy_from_slice(&issuer.batch_id);
        fresh[32..40].copy_from_slice(&index);
        fresh[40..48].copy_from_slice(&ts);
        fresh[48..113].copy_from_slice(&sig);
        // Persist the refreshed stamp by overwriting the address's
        // existing sidecar record in place: no file growth on re-push,
        // and the on-disk timestamp stays current so a restart can't
        // reload a stale one and re-emit a timestamp the network saw.
        issuer.refresh_cached_stamp(*chunk_address, fresh)?;
        return Ok(fresh);
    }

    let (index, ts) = issuer.increment(chunk_address)?;
    let digest = postage_sign_digest(chunk_address, &issuer.batch_id, &index, &ts);
    let sig = sign_handshake_data(secret, &digest)?;

    let mut stamp = [0u8; STAMP_SIZE];
    stamp[0..32].copy_from_slice(&issuer.batch_id);
    stamp[32..40].copy_from_slice(&index);
    stamp[40..48].copy_from_slice(&ts);
    stamp[48..113].copy_from_slice(&sig);
    issuer.record_stamp(*chunk_address, stamp)?;
    Ok(stamp)
}

/// Sidecar path for the per-chunk stamp cache: the counter file's path
/// with its extension swapped to `.stamps` (e.g. `<batch>.bin` →
/// `<batch>.stamps`).
fn stamps_sidecar_path(persist: &Path) -> PathBuf {
    persist.with_extension("stamps")
}

/// The 4-byte record checksum: first bytes of `keccak256(addr ‖ stamp)`.
fn stamp_record_checksum(addr: &[u8; 32], stamp: &[u8; STAMP_SIZE]) -> [u8; STAMP_CHECKSUM_LEN] {
    let mut h = Keccak256::default();
    h.update(addr);
    h.update(stamp);
    let digest = h.finalize();
    let mut c = [0u8; STAMP_CHECKSUM_LEN];
    c.copy_from_slice(&digest[..STAMP_CHECKSUM_LEN]);
    c
}

/// The 8-byte big-endian timestamp embedded in a stamp (`[40..48]`),
/// used to pick the newest record when an address has several.
fn stamp_timestamp(stamp: &[u8; STAMP_SIZE]) -> u64 {
    u64::from_be_bytes(stamp[40..48].try_into().expect("stamp layout"))
}

/// Result of loading a stamp sidecar.
struct LoadedSidecar {
    /// Latest valid stamp per address.
    map: HashMap<[u8; 32], [u8; STAMP_SIZE]>,
    /// Complete physical frames on disk (valid *and* checksum-invalid),
    /// so the compaction trigger accounts for torn frames too.
    physical_count: usize,
    /// Whether it is safe to append v2 records to the on-disk file. Only
    /// `false` when the file is v1 and migration to v2 failed — appending
    /// then would misalign framing, so the issuer runs in-memory-only
    /// this session and a restart retries migration.
    appendable: bool,
}

impl LoadedSidecar {
    fn empty() -> Self {
        Self {
            map: HashMap::new(),
            physical_count: 0,
            appendable: true,
        }
    }
}

/// Load the per-chunk stamp sidecar into `(stamp map, physical record
/// count)`. Best-effort and self-healing:
///
/// - A missing file, or a header whose magic / batch-id doesn't match,
///   yields an empty map (a foreign file is removed so a fresh append
///   starts clean).
/// - A **v1** file (no checksums) is *migrated*, not discarded: its
///   records are read and the file is rewritten as v2, so no reusable
///   slot is lost across the upgrade.
/// - For v2, each record's checksum is verified; a record that fails (a
///   torn append) is skipped. Records are append-only, so an address
///   may appear several times — the **highest-timestamp valid** record
///   wins, which means a torn *latest* write falls back to the previous
///   valid record (its slot survives). A trailing partial record (crash
///   mid-append) is truncated to the last whole-record boundary.
/// - If the file has grown well past the live entry count (re-stamps
///   piling up), it is compacted to one record per address.
///
/// A stale/dropped entry only ever costs a re-stamp, never a wrong
/// stamp, so issuer construction never hard-fails over the sidecar.
fn load_stamp_store(path: &Path, batch_id: &[u8; 32]) -> LoadedSidecar {
    let mut map: HashMap<[u8; 32], [u8; STAMP_SIZE]> = HashMap::new();
    let Ok(bytes) = std::fs::read(path) else {
        return LoadedSidecar::empty();
    };
    if bytes.len() < STAMP_STORE_HEADER_LEN
        || &bytes[0..4] != STAMP_STORE_MAGIC
        || &bytes[6..38] != batch_id.as_slice()
    {
        let _ = std::fs::remove_file(path);
        return LoadedSidecar::empty();
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    let (record_len, has_checksum) = match version {
        STAMP_STORE_VERSION => (STAMP_RECORD_LEN, true),
        STAMP_STORE_VERSION_V1 => (STAMP_RECORD_LEN_V1, false),
        _ => {
            // Unknown future/foreign version: drop it rather than guess.
            let _ = std::fs::remove_file(path);
            return LoadedSidecar::empty();
        }
    };

    // Newest-timestamp-wins per address, keeping only checksum-valid
    // records. Count *every complete physical frame* (valid or not)
    // separately: a checksum-invalid frame still occupies disk space, so
    // it must count toward the compaction trigger or repeated torn
    // writes could grow the file past the bound without ever compacting.
    let mut best_ts: HashMap<[u8; 32], u64> = HashMap::new();
    let mut off = STAMP_STORE_HEADER_LEN;
    let mut physical_frames = 0usize;
    while off + record_len <= bytes.len() {
        physical_frames += 1;
        let mut addr = [0u8; 32];
        addr.copy_from_slice(&bytes[off..off + 32]);
        let mut stamp = [0u8; STAMP_SIZE];
        stamp.copy_from_slice(&bytes[off + 32..off + 32 + STAMP_SIZE]);
        let ok = !has_checksum
            || bytes[off + 32 + STAMP_SIZE..off + record_len]
                == stamp_record_checksum(&addr, &stamp);
        if ok {
            let ts = stamp_timestamp(&stamp);
            if best_ts.get(&addr).is_none_or(|&prev| ts >= prev) {
                best_ts.insert(addr, ts);
                map.insert(addr, stamp);
            }
        }
        off += record_len;
    }

    // v1 files MUST be migrated before any append: appending a 149-byte
    // v2 record under a v1 (145-byte) header would misalign framing and
    // silently lose records on the next load. So if migration fails for a
    // v1 file, mark the sidecar non-appendable — the caller keeps the
    // in-memory map but does NOT append this session (a restart retries
    // migration), rather than corrupting the file. A v2 file whose
    // compaction fails is still safe to append to (same format), so it
    // stays appendable.
    let over_threshold = physical_frames > map.len() * STAMP_COMPACT_FACTOR + STAMP_COMPACT_FLOOR;
    if !has_checksum || over_threshold {
        if rewrite_sidecar(path, batch_id, &map).is_ok() {
            let live = map.len();
            return LoadedSidecar {
                map,
                physical_count: live,
                appendable: true,
            };
        }
        if !has_checksum {
            // v1 migration failed: don't append to the v1 file.
            return LoadedSidecar {
                map,
                physical_count: physical_frames,
                appendable: false,
            };
        }
        // v2 compaction failed: keep appending (same format is fine).
    }
    // Truncate a trailing partial record so the next append is aligned.
    if off < bytes.len() {
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(path) {
            let _ = f.set_len(off as u64);
        }
    }
    LoadedSidecar {
        map,
        physical_count: physical_frames,
        appendable: true,
    }
}

/// Rewrite the sidecar with exactly one v2 record per live address, via
/// the crash-durable [`atomic_write`] (temp write + fsync + rename +
/// **parent-dir fsync**, so the rename itself survives a crash). Used to
/// migrate a v1 file and to compact a v2 file whose re-stamp appends have
/// accumulated.
fn rewrite_sidecar(
    path: &Path,
    batch_id: &[u8; 32],
    live: &HashMap<[u8; 32], [u8; STAMP_SIZE]>,
) -> Result<(), PostageError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
        }
    }
    let mut buf = Vec::with_capacity(STAMP_STORE_HEADER_LEN + live.len() * STAMP_RECORD_LEN);
    buf.extend_from_slice(STAMP_STORE_MAGIC);
    buf.extend_from_slice(&STAMP_STORE_VERSION.to_le_bytes());
    buf.extend_from_slice(batch_id);
    for (addr, stamp) in live {
        buf.extend_from_slice(addr);
        buf.extend_from_slice(stamp);
        buf.extend_from_slice(&stamp_record_checksum(addr, stamp));
    }
    let tmp = path.with_extension("stamps.tmp");
    atomic_write(&tmp, path, &buf)
        .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))
}

/// Append one checksummed `(addr, stamp)` record to the sidecar,
/// creating it with a header first if absent, and fsync before
/// returning so a restart recovers it. Append-only: a crash can only
/// damage the trailing record, which the loader truncates or (if a full
/// but torn record) rejects by checksum, leaving the address's earlier
/// record intact.
fn append_stamp_record(
    path: &Path,
    batch_id: &[u8; 32],
    addr: &[u8; 32],
    stamp: &[u8; STAMP_SIZE],
) -> Result<(), PostageError> {
    use std::io::Write as _;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
        }
    }
    let need_header = !path.exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
    if need_header {
        let mut header = Vec::with_capacity(STAMP_STORE_HEADER_LEN);
        header.extend_from_slice(STAMP_STORE_MAGIC);
        header.extend_from_slice(&STAMP_STORE_VERSION.to_le_bytes());
        header.extend_from_slice(batch_id);
        f.write_all(&header)
            .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
    }
    let mut rec = Vec::with_capacity(STAMP_RECORD_LEN);
    rec.extend_from_slice(addr);
    rec.extend_from_slice(stamp);
    rec.extend_from_slice(&stamp_record_checksum(addr, stamp));
    f.write_all(&rec)
        .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
    f.sync_all()
        .map_err(|e| PostageError::Persist(format!("{}: {e}", path.display())))?;
    Ok(())
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

    /// Re-stamping a known address must reuse its (bucket, index) slot
    /// but refresh the timestamp and re-sign — bee's stamper semantics.
    /// Returning the byte-identical cached stamp breaks SOC updates:
    /// bee's reserve rejects a same-(batch, index) put whose timestamp
    /// isn't strictly newer (`ErrOverwriteNewerChunk`), so a GSOC
    /// update at its stable address never lands anywhere (found live
    /// on mainnet 2026-07-13).
    #[test]
    fn restamp_reuses_slot_with_fresh_timestamp() {
        let secret = random_secp256k1_secret();
        let sk = k256::ecdsa::SigningKey::from_bytes(&secret.into()).unwrap();
        let owner = ethereum_address_from_public_key(sk.verifying_key());
        let mut issuer = StampIssuer::new([0xcd; 32], 21, 16, false).unwrap();
        let chunk = [3u8; 32];

        let ts = |s: &[u8; STAMP_SIZE]| u64::from_be_bytes(s[40..48].try_into().unwrap());
        let first = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        // Rapid-fire re-stamps: each must be strictly newer than the
        // last even inside one clock tick — the monotonic guarantee, not
        // just "call unix_now again". A tight loop is the case coarse
        // resolution would break.
        let mut prev = first;
        for _ in 0..50 {
            let next = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            assert_eq!(&next[32..40], &first[32..40], "index must be reused");
            assert!(
                ts(&next) > ts(&prev),
                "timestamp must be strictly newer ({} vs {})",
                ts(&next),
                ts(&prev)
            );
            prev = next;
        }
        assert_eq!(issuer.issued_count(), 1, "re-stamp must not burn a slot");
        // The refreshed stamp still verifies against the owner.
        verify_stamp_owner(
            &chunk,
            &prev,
            &owner,
            issuer.batch_depth,
            issuer.bucket_depth,
        )
        .unwrap();
        // The cache now holds the latest refreshed stamp.
        assert_eq!(issuer.cached_stamp(&chunk).unwrap(), prev);
    }

    /// Re-stamps append (for crash-safety) but must not grow the sidecar
    /// without bound: compaction caps it at a small multiple of the live
    /// entry count. Hammering one address far past the compaction
    /// threshold must leave the file bounded, not linear in re-stamps.
    #[test]
    fn restamp_growth_is_bounded_by_compaction() {
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let mut issuer = StampIssuer::open_or_new(path.clone(), [0x44; 32], 21, 16, true).unwrap();
        let chunk = [0x5a; 32];
        let sidecar = path.with_extension("stamps");

        // Far more re-stamps than the compaction threshold for one live
        // address (2*1 + 64 = 66).
        for _ in 0..500 {
            sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        }
        let records = (std::fs::metadata(&sidecar).unwrap().len() as usize
            - STAMP_STORE_HEADER_LEN)
            / STAMP_RECORD_LEN;
        assert!(
            records <= STAMP_COMPACT_FACTOR + STAMP_COMPACT_FLOOR + 1,
            "compaction must bound the file ({records} records after 500 re-stamps)"
        );
        // Only one bucket slot was ever consumed despite all the re-stamps.
        assert_eq!(issuer.issued_count(), 1, "re-stamp must not burn slots");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The refreshed re-stamp timestamp must survive a restart: if it
    /// weren't persisted, a restart (which reloads the sidecar) followed
    /// by a re-stamp under a rolled-back clock would re-emit a timestamp
    /// the network already saw, and peers would reject the chunk as
    /// not-strictly-newer. In place of a real clock rollback we assert
    /// the reloaded stamp equals the last one written (so the next
    /// re-stamp's `max(now, prev+1)` builds on the true high-water mark).
    #[test]
    fn restamp_timestamp_survives_restart() {
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x77; 32];
        let chunk = [0x9c; 32];
        let ts = |s: &[u8; STAMP_SIZE]| u64::from_be_bytes(s[40..48].try_into().unwrap());

        // Issue, then re-stamp several times so the persisted record
        // carries an advanced timestamp (not the original).
        let last = {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
            sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            let mut last = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            for _ in 0..5 {
                last = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            }
            last
        };

        // Restart: a fresh issuer reloads the sidecar. The reloaded stamp
        // must be the LATEST written, not the original — otherwise the
        // high-water mark is lost.
        let mut reopened = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
        let reloaded = reopened.cached_stamp(&chunk).expect("stamp recovered");
        assert_eq!(
            ts(&reloaded),
            ts(&last),
            "restart must reload the latest re-stamp timestamp, not a stale one"
        );

        // And the next re-stamp is still strictly newer than everything
        // the network has seen, even though a new issuer instance began.
        let after_restart = sign_stamp_bytes(&secret, &mut reopened, &chunk).unwrap();
        assert!(
            ts(&after_restart) > ts(&last),
            "post-restart re-stamp must exceed the persisted high-water mark"
        );
        // Append-based: at most a handful of records for one address
        // (compaction bounds it; well under the threshold here).
        let sidecar = path.with_extension("stamps");
        let records = (std::fs::metadata(&sidecar).unwrap().len() as usize
            - STAMP_STORE_HEADER_LEN)
            / STAMP_RECORD_LEN;
        assert!(
            records <= STAMP_COMPACT_FACTOR + STAMP_COMPACT_FLOOR + 1,
            "sidecar stays bounded for one address ({records} records)"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A crash mid-append leaves a trailing partial record. The loader
    /// must truncate it so the *next* append writes at a valid boundary
    /// — otherwise framing shifts permanently and every later record
    /// fails to reload.
    #[test]
    fn sidecar_truncates_trailing_partial_record_on_load() {
        use std::io::Write as _;
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x33; 32];
        let sidecar = path.with_extension("stamps");

        let chunk_a = [0xa1; 32];
        {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
            sign_stamp_bytes(&secret, &mut issuer, &chunk_a).unwrap();
        }
        // Simulate a torn append: 20 stray bytes after the good record.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&sidecar)
                .unwrap();
            f.write_all(&[0x77u8; 20]).unwrap();
        }
        assert_eq!(
            std::fs::metadata(&sidecar).unwrap().len() as usize,
            STAMP_STORE_HEADER_LEN + STAMP_RECORD_LEN + 20
        );

        // Reopen: the partial is truncated and the good record recovered.
        let chunk_b = [0xb2; 32];
        let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
        assert!(issuer.has_stamp(&chunk_a), "intact record must survive");
        assert_eq!(
            std::fs::metadata(&sidecar).unwrap().len() as usize,
            STAMP_STORE_HEADER_LEN + STAMP_RECORD_LEN,
            "trailing partial must be truncated on load"
        );
        // A new append lands at the aligned boundary and reloads cleanly.
        sign_stamp_bytes(&secret, &mut issuer, &chunk_b).unwrap();
        drop(issuer);
        let reopened = StampIssuer::open_or_new(path, batch, 21, 16, true).unwrap();
        assert!(reopened.has_stamp(&chunk_a) && reopened.has_stamp(&chunk_b));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A torn in-place overwrite yields a record whose checksum no longer
    /// matches its stamp. The loader must discard that one record (its
    /// address just re-stamps later) while keeping the others — fixed
    /// framing means one corrupt record never shifts its neighbours.
    #[test]
    fn sidecar_discards_checksum_mismatched_record() {
        use std::io::{Seek as _, SeekFrom, Write as _};
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x44; 32];
        let sidecar = path.with_extension("stamps");

        let chunk_a = [0xaa; 32];
        let chunk_b = [0xbb; 32];
        {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
            sign_stamp_bytes(&secret, &mut issuer, &chunk_a).unwrap();
            sign_stamp_bytes(&secret, &mut issuer, &chunk_b).unwrap();
        }
        // Corrupt one byte inside the FIRST record's stamp region without
        // fixing its checksum → simulates a torn write.
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&sidecar)
                .unwrap();
            // First record's stamp starts at header + 32 (past the addr).
            f.seek(SeekFrom::Start((STAMP_STORE_HEADER_LEN + 32 + 5) as u64))
                .unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let reopened = StampIssuer::open_or_new(path, batch, 21, 16, true).unwrap();
        assert!(
            !reopened.has_stamp(&chunk_a),
            "checksum-mismatched record must be discarded"
        );
        assert!(
            reopened.has_stamp(&chunk_b),
            "the intact neighbour must still load (framing unshifted)"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A torn *latest* append (its trailing record corrupt) must fall
    /// back to the address's previous valid record — its bucket slot is
    /// preserved, not lost. Losing it would let a re-stamp burn a fresh
    /// index and, on a full bucket, evict another chunk or (immutable)
    /// reject the existing one.
    #[test]
    fn sidecar_falls_back_to_previous_record_on_torn_latest_append() {
        use std::io::{Seek as _, SeekFrom, Write as _};
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x55; 32];
        let sidecar = path.with_extension("stamps");
        let chunk = [0xcc; 32];
        let ts = |s: &[u8; STAMP_SIZE]| u64::from_be_bytes(s[40..48].try_into().unwrap());

        // Stamp, then re-stamp once → two appended records (old + new).
        let first = {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
            let first = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            let second = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            assert!(ts(&second) > ts(&first));
            first
        };
        assert_eq!(
            (std::fs::metadata(&sidecar).unwrap().len() as usize - STAMP_STORE_HEADER_LEN)
                / STAMP_RECORD_LEN,
            2,
            "two records: the original and the re-stamp"
        );
        // Corrupt the SECOND (latest) record's stamp → torn latest write.
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&sidecar)
                .unwrap();
            f.seek(SeekFrom::Start(
                (STAMP_STORE_HEADER_LEN + STAMP_RECORD_LEN + 32 + 7) as u64,
            ))
            .unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        // Reopen: the slot is recovered from the previous valid record.
        let reopened = StampIssuer::open_or_new(path, batch, 21, 16, true).unwrap();
        let recovered = reopened.cached_stamp(&chunk).expect("slot must survive");
        assert_eq!(
            ts(&recovered),
            ts(&first),
            "must fall back to the previous valid record, not lose the slot"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A v1 sidecar (no checksums) must be **migrated** on load — its
    /// records preserved and the file rewritten as v2 — not deleted.
    /// Deleting would drop reusable slots and, on a full bucket, cause
    /// eviction/rejection.
    #[test]
    fn sidecar_migrates_v1_records_forward() {
        use std::io::Write as _;
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x66; 32];
        let sidecar = path.with_extension("stamps");

        // Hand-write a v1 file: header (magic, version=1, batch) + two
        // v1 records (addr32 ‖ stamp113, no checksum).
        let v1_stamp = |addr: u8, index: u64| -> ([u8; 32], [u8; STAMP_SIZE]) {
            let a = [addr; 32];
            let mut s = [0u8; STAMP_SIZE];
            s[0..32].copy_from_slice(&batch);
            s[32..40].copy_from_slice(&index.to_be_bytes());
            s[40..48].copy_from_slice(&1000u64.to_be_bytes());
            (a, s)
        };
        let (a1, s1) = v1_stamp(0xa1, 1);
        let (a2, s2) = v1_stamp(0xb2, 2);
        {
            std::fs::create_dir_all(sidecar.parent().unwrap()).unwrap();
            let mut f = std::fs::File::create(&sidecar).unwrap();
            f.write_all(STAMP_STORE_MAGIC).unwrap();
            f.write_all(&STAMP_STORE_VERSION_V1.to_le_bytes()).unwrap();
            f.write_all(&batch).unwrap();
            for (a, s) in [(a1, s1), (a2, s2)] {
                f.write_all(&a).unwrap();
                f.write_all(&s).unwrap();
            }
        }

        // Load migrates: both records preserved, file now v2 (with
        // checksums), one record per address.
        let issuer = StampIssuer::open_or_new(path, batch, 21, 16, true).unwrap();
        assert_eq!(issuer.cached_stamp(&a1), Some(s1), "v1 record 1 preserved");
        assert_eq!(issuer.cached_stamp(&a2), Some(s2), "v1 record 2 preserved");
        let bytes = std::fs::read(&sidecar).unwrap();
        assert_eq!(
            u16::from_le_bytes([bytes[4], bytes[5]]),
            STAMP_STORE_VERSION,
            "file migrated to v2"
        );
        assert_eq!(
            (bytes.len() - STAMP_STORE_HEADER_LEN) / STAMP_RECORD_LEN,
            2,
            "two v2 records after migration"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// If v1 migration FAILS (e.g. the temp rewrite path can't be
    /// created), the loader must NOT append v2 records under the v1
    /// header — that would misalign framing and lose entries. Instead the
    /// v1 file is left untouched (a restart retries migration) and the
    /// issuer, and stamping must FAIL closed (not silently issue a
    /// non-durable stamp). Once migration can succeed again, the v1
    /// records recover intact — no successfully issued stamp ever
    /// disappears. Regression for the mixed-format-append + fail-open
    /// hazards.
    #[test]
    fn failed_v1_migration_fails_closed_then_recovers() {
        use std::io::Write as _;
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x88; 32];
        let sidecar = path.with_extension("stamps");

        // Hand-write a v1 file with one record.
        let a1 = [0xa1; 32];
        let mut s1 = [0u8; STAMP_SIZE];
        s1[0..32].copy_from_slice(&batch);
        s1[40..48].copy_from_slice(&1234u64.to_be_bytes());
        {
            std::fs::create_dir_all(sidecar.parent().unwrap()).unwrap();
            let mut f = std::fs::File::create(&sidecar).unwrap();
            f.write_all(STAMP_STORE_MAGIC).unwrap();
            f.write_all(&STAMP_STORE_VERSION_V1.to_le_bytes()).unwrap();
            f.write_all(&batch).unwrap();
            f.write_all(&a1).unwrap();
            f.write_all(&s1).unwrap();
        }
        let v1_bytes_before = std::fs::read(&sidecar).unwrap();

        // Sabotage migration: occupy the exact temp rewrite path
        // (`rewrite_sidecar` uses `<sidecar>.with_extension("stamps.tmp")`)
        // with a directory so `atomic_write`'s File::create fails, while
        // the v1 file itself stays readable/writable.
        let tmp = sidecar.with_extension("stamps.tmp");
        std::fs::create_dir(&tmp).unwrap();

        {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
            // The v1 record is recovered into memory (reads work)…
            assert!(issuer.has_stamp(&a1), "records recovered in memory");
            // …but stamping FAILS CLOSED — no non-durable stamp is issued
            // (the old fail-open bug returned a pushable stamp here whose
            // mapping would vanish on restart).
            let a2 = [0xb2; 32];
            assert!(
                sign_stamp_bytes(&secret, &mut issuer, &a2).is_err(),
                "must refuse to issue when persistence is broken"
            );
            // A re-stamp of a recovered address must also fail closed.
            assert!(
                sign_stamp_bytes(&secret, &mut issuer, &a1).is_err(),
                "re-stamp must also refuse when persistence is broken"
            );
            // The v1 file is byte-identical — nothing was appended.
            assert_eq!(
                std::fs::read(&sidecar).unwrap(),
                v1_bytes_before,
                "v1 file must be untouched when migration fails"
            );
        }

        // Clear the sabotage → a restart can now migrate. The v1 record
        // recovers intact and stamping works again.
        std::fs::remove_dir(&tmp).unwrap();
        let mut issuer = StampIssuer::open_or_new(path, batch, 21, 16, true).unwrap();
        assert_eq!(
            issuer.cached_stamp(&a1),
            Some(s1),
            "the recovered v1 stamp survives — no successfully issued stamp lost"
        );
        // File is now v2 and stamping succeeds again.
        assert_eq!(
            u16::from_le_bytes({
                let b = std::fs::read(&sidecar).unwrap();
                [b[4], b[5]]
            }),
            STAMP_STORE_VERSION,
            "migrated to v2 after the obstruction cleared"
        );
        sign_stamp_bytes(&secret, &mut issuer, &[0xc3; 32]).expect("stamping works post-migration");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Corrupt (checksum-invalid) full frames still occupy disk, so they
    /// must count toward the compaction trigger — otherwise a stream of
    /// torn full-record writes could grow the file past the bound without
    /// ever compacting. Here we stuff the file with garbage full-length
    /// frames and confirm the next load compacts them away.
    #[test]
    fn corrupt_frames_count_toward_compaction() {
        use std::io::Write as _;
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch = [0x99; 32];
        let sidecar = path.with_extension("stamps");
        let chunk = [0x5a; 32];

        // One real record, then a pile of full-length garbage frames
        // (valid framing, bad checksum) well past the compaction floor.
        {
            let mut issuer = StampIssuer::open_or_new(path.clone(), batch, 21, 16, true).unwrap();
            sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        }
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&sidecar)
                .unwrap();
            for _ in 0..(STAMP_COMPACT_FLOOR + 5) {
                f.write_all(&[0x00u8; STAMP_RECORD_LEN]).unwrap(); // zero addr+stamp, checksum won't match
            }
        }
        let records_before = (std::fs::metadata(&sidecar).unwrap().len() as usize
            - STAMP_STORE_HEADER_LEN)
            / STAMP_RECORD_LEN;
        assert!(records_before > STAMP_COMPACT_FLOOR);

        // Reopen: the corrupt frames counted toward the trigger, so the
        // file is compacted down to just the one live record.
        let reopened = StampIssuer::open_or_new(path, batch, 21, 16, true).unwrap();
        assert!(reopened.has_stamp(&chunk));
        let records_after = (std::fs::metadata(&sidecar).unwrap().len() as usize
            - STAMP_STORE_HEADER_LEN)
            / STAMP_RECORD_LEN;
        assert_eq!(records_after, 1, "corrupt frames must be compacted away");
        std::fs::remove_dir_all(&dir).ok();
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

    /// Re-stamping the same chunk must be idempotent: it returns the
    /// identical stamp and consumes **no** new bucket index. This is the
    /// fix for self-inflicted batch saturation — a heal/retry that
    /// re-pushes already-stamped chunks must not burn postage. We also
    /// prove the exemption: even after the bucket has filled, an
    /// already-stamped chunk still re-stamps (reuses) for free.
    #[test]
    fn restamp_is_idempotent_and_free() {
        let secret = random_secp256k1_secret();
        let batch_id = [0x11u8; 32];
        // depth 17, bucket_depth 16 => 2 slots per bucket.
        let mut issuer = StampIssuer::new(batch_id, 17, 16, true).unwrap();
        let chunk = [0x42u8; 32];

        let first = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        let issued_after_first = issuer.issued_count();
        assert_eq!(issued_after_first, 1);
        assert!(issuer.has_stamp(&chunk));

        // Re-stamp the same chunk several times: the (bucket, index)
        // slot is reused — no new index — but each stamp carries a
        // fresh timestamp and signature (bee stamper semantics; a
        // byte-identical replay would make SOC updates undeliverable).
        for _ in 0..5 {
            let again = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            assert_eq!(&again[32..40], &first[32..40], "index slot is reused");
        }
        assert_eq!(
            issuer.issued_count(),
            issued_after_first,
            "re-stamping must not consume bucket slots",
        );

        // Fill the bucket with a *different* chunk in the same bucket so
        // it saturates, then prove the already-stamped chunk still
        // re-stamps for free despite the full bucket.
        let mut sibling = chunk;
        sibling[31] = 0x01; // same top bits => same collision bucket
        sign_stamp_bytes(&secret, &mut issuer, &sibling).unwrap();
        assert!(issuer.bucket_is_full(&chunk));
        let after_full = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
        assert_eq!(&after_full[32..40], &first[32..40]);
    }

    /// The `.stamps` sidecar must survive a restart so a later heal
    /// (manual "Push again") reuses stamps instead of re-issuing them.
    #[test]
    fn stamp_cache_persists_across_restart() {
        let secret = random_secp256k1_secret();
        let dir = tmpdir();
        let path = dir.join("batch.bin");
        let batch_id = [0x22u8; 32];

        let chunk = [0x99u8; 32];
        let original = {
            let mut issuer =
                StampIssuer::open_or_new(path.clone(), batch_id, 17, 16, true).unwrap();
            let s = sign_stamp_bytes(&secret, &mut issuer, &chunk).unwrap();
            assert_eq!(issuer.issued_count(), 1);
            s
        };
        assert!(path.with_extension("stamps").exists());

        // Reopen — the cache should rehydrate, so a re-stamp reuses the
        // original slot (same index, fresh timestamp) and issues no new
        // index.
        let mut reopened = StampIssuer::open_or_new(path, batch_id, 17, 16, true).unwrap();
        assert!(reopened.has_stamp(&chunk));
        let reused = sign_stamp_bytes(&secret, &mut reopened, &chunk).unwrap();
        assert_eq!(&reused[32..40], &original[32..40], "index slot is reused");
        assert_eq!(reopened.issued_count(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }
}

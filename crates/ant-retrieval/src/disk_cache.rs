//! Persistent (SQLite-backed) chunk cache — tier 2 of the cache stack
//! described in `PLAN.md` § 6.1.
//!
//! The retrieval pipeline already validates every chunk on the wire
//! (see [`crate::retrieve_chunk`]), so the in-memory tier
//! ([`crate::InMemoryChunkCache`]) trusts what it stores. The on-disk
//! tier cannot make the same assumption: the file may have been
//! corrupted at rest, partially overwritten, or hand-edited. Every
//! [`DiskChunkCache::get`] therefore re-validates the row's wire bytes
//! against the requested 32-byte address using
//! [`ant_crypto::cac_valid`] / [`ant_crypto::soc_valid`]. A failed
//! validation deletes the row and reports a miss, so a corrupt page
//! cannot poison the retrieval stream — the network path runs and the
//! eventual write-through replaces the row with verified bytes.
//!
//! Async integration rule (also from `PLAN.md` § 6.1): SQLite calls
//! must never run directly on a Tokio retrieval task. Every public
//! method below dispatches the blocking work onto
//! [`tokio::task::spawn_blocking`], so the retrieval future yields
//! while the connection is held. This is the simple form the plan
//! explicitly endorses for the first cut.
//!
//! Lookup order in the daemon is `memory -> disk -> network`. Network
//! successes write through to memory eagerly (already done) and to
//! disk via this cache; disk hits write back to memory so a hot chunk
//! lifts into the LRU after the first retrieval per process.

use ant_crypto::{cac_valid, soc_valid};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{debug, trace, warn};

/// Default eviction "slack" — how far below the cap we drain on every
/// trigger. 95% means an over-cap insert deletes ~5% of the cap in
/// `last_access` order rather than one row at a time, keeping the
/// per-write eviction cost amortised. Mirrors the guidance in
/// `PLAN.md` § 6.1.
const EVICTION_SLACK_RATIO: f64 = 0.95;

/// How "fresh" `last_access` must already be for [`DiskChunkCache::get`]
/// to skip the touch update entirely. Without this, every cache hit
/// would issue an `UPDATE chunks SET last_access = ?` — and because
/// `synchronous=NORMAL` fsync's on each implicit commit, a daemon
/// streaming N chunks per second from disk would queue N fsyncs per
/// second behind the connection mutex. With 14 parallel gateway
/// streams we measured throughput collapsing to ~70 chunks/sec
/// (manifest in `antctl top` as a fully-cache-served file slowly
/// filling its progress bar). 60 s is a generous LRU-ordering
/// granularity — eviction picks a victim because it hasn't been
/// touched in *minutes*, so a write that's lagged by < 1 minute can
/// never re-order things meaningfully — and it makes hot reads pure
/// `SELECT`s.
const LAST_ACCESS_REFRESH_MS: i64 = 60_000;

/// Default size cap for the persistent chunk cache. 10 GB matches
/// `PLAN.md` § 6.1's desktop / Raspberry Pi default. Operators on
/// constrained boxes (mobile / embedded) should set a smaller cap;
/// power users should raise it.
pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum DiskCacheError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("background worker panicked")]
    Panicked,
}

/// SQLite-backed persistent chunk cache.
///
/// Holds a single connection behind an `Arc<Mutex<>>`. Every method
/// is async and calls [`tokio::task::spawn_blocking`] so the SQLite
/// work runs on the blocking pool — the retrieval task that triggered
/// the call yields immediately and we never block a Tokio worker on
/// `fsync`.
///
/// `total_bytes` is an in-memory mirror of `SUM(size)` over the
/// `chunks` table. It is initialised from disk in [`Self::open`] and
/// updated atomically alongside every successful insert / delete /
/// eviction so we don't have to ask SQLite for the sum on every put.
/// The mirror is informational (used by `len`/`used_bytes` and to
/// decide whether to run an eviction sweep); the source of truth
/// remains the table.
pub struct DiskChunkCache {
    inner: Arc<Inner>,
}

struct Inner {
    /// Path the connection was opened from. Useful for tracing /
    /// `antctl top` exposure later; logging it once on open keeps
    /// "where did the daemon put my cache" answerable from the
    /// journal.
    path: PathBuf,
    /// SQLite connection. Wrapped in a sync `Mutex` (not a
    /// `tokio::sync::Mutex`) because every consumer of the lock runs
    /// inside `spawn_blocking`, so blocking on a contended lock is
    /// fine — it's already a blocking thread.
    conn: std::sync::Mutex<Connection>,
    /// Hard upper bound on stored bytes. When `total_bytes` crosses
    /// this on a put, an eviction sweep deletes oldest `last_access`
    /// rows down to `slack_bytes`.
    max_bytes: u64,
    /// Eviction floor: writes never drain below this on a single
    /// sweep. Set in [`Self::open`] to `EVICTION_SLACK_RATIO` of
    /// `max_bytes`.
    slack_bytes: u64,
    /// Live mirror of `SUM(size)` from the `chunks` table.
    total_bytes: AtomicU64,
}

impl DiskChunkCache {
    /// Open (or create) a chunk cache database at `path` with a
    /// `max_bytes` byte cap.
    ///
    /// On first call this creates the schema, applies the recommended
    /// pragmas (WAL, NORMAL fsync, 5 s busy timeout, 8 KiB page size
    /// before any tables exist), and computes the initial
    /// `total_bytes`. Subsequent opens against the same file are
    /// idempotent — `CREATE TABLE IF NOT EXISTS` makes the migration
    /// step crash-safe.
    ///
    /// `max_bytes` is honoured as-is; clamping to a sane minimum is
    /// the wiring layer's job (see `antd`). Tests pass tiny caps to
    /// drive eviction without storing megabytes of payload.
    pub fn open(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self, DiskCacheError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        // Pragmas as recommended in PLAN.md § 6.1. `page_size` must run
        // before any table exists; `CREATE TABLE IF NOT EXISTS` below
        // makes that ordering safe on first open. On subsequent opens
        // the pragma is a no-op (SQLite ignores `page_size` once the
        // file is non-empty), so we don't track the "first open" state
        // ourselves.
        conn.pragma_update(None, "page_size", 8192)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS chunks (
                address     BLOB PRIMARY KEY,
                data        BLOB NOT NULL,
                size        INTEGER NOT NULL,
                last_access INTEGER NOT NULL,
                inserted_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS chunks_last_access_idx
                ON chunks(last_access);",
        )?;

        let initial_total: u64 = conn
            .query_row("SELECT COALESCE(SUM(size), 0) FROM chunks", [], |row| {
                row.get::<_, i64>(0).map(|n| n as u64)
            })
            .unwrap_or(0);

        let slack_bytes = ((max_bytes as f64) * EVICTION_SLACK_RATIO) as u64;

        debug!(
            target: "ant_retrieval::disk_cache",
            path = %path.display(),
            initial_total,
            max_bytes,
            slack_bytes,
            "opened persistent chunk cache",
        );

        Ok(Self {
            inner: Arc::new(Inner {
                path,
                conn: std::sync::Mutex::new(conn),
                max_bytes,
                slack_bytes,
                total_bytes: AtomicU64::new(initial_total),
            }),
        })
    }

    /// Filesystem path the cache was opened against.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Configured byte cap.
    pub fn capacity_bytes(&self) -> u64 {
        self.inner.max_bytes
    }

    /// Live byte total mirrored from `SUM(size)`. May lag by a single
    /// in-flight put / evict (the atomic update lands after the
    /// commit), but never diverges by more than the size of one
    /// chunk.
    pub fn used_bytes(&self) -> u64 {
        self.inner.total_bytes.load(Ordering::Relaxed)
    }

    /// Look up a chunk by address. CAC/SOC-validates the stored row
    /// before returning. A row that fails validation is deleted in
    /// place and the call reports a miss, so the caller falls through
    /// to the network and the eventual write-through replaces the row
    /// with verified bytes.
    ///
    /// `last_access` is refreshed *only when stale* (older than
    /// [`LAST_ACCESS_REFRESH_MS`] ms relative to wall clock). On a hot
    /// cache this means the call is a single indexed `SELECT` and never
    /// takes the SQLite write lock — the difference between "rare
    /// fsync" and "fsync per chunk" is the difference between the
    /// daemon serving from disk at >100 MiB/s vs ~1 MiB/s under
    /// fan-out load.
    pub async fn get(&self, addr: [u8; 32]) -> Result<Option<Vec<u8>>, DiskCacheError> {
        let inner = self.inner.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>, DiskCacheError> {
            let conn = inner.conn.lock().expect("disk cache mutex poisoned");
            let row: Option<(Vec<u8>, i64)> = conn
                .query_row(
                    "SELECT data, last_access FROM chunks WHERE address = ?1",
                    params![&addr[..]],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?;
            let Some((data, last_access)) = row else { return Ok(None) };

            if !cac_valid(&addr, &data) && !soc_valid(&addr, &data) {
                warn!(
                    target: "ant_retrieval::disk_cache",
                    chunk = %hex::encode(addr),
                    bytes = data.len(),
                    "disk row failed validation; deleting and reporting miss",
                );
                let removed = data.len() as u64;
                let n = conn.execute(
                    "DELETE FROM chunks WHERE address = ?1",
                    params![&addr[..]],
                )?;
                if n > 0 {
                    inner.total_bytes.fetch_sub(removed, Ordering::Relaxed);
                }
                return Ok(None);
            }

            // Bump `last_access` only if it's drifted far enough that
            // an eviction comparison could plausibly care. Skipping the
            // write keeps the hot path lock-light: the entire `get`
            // becomes a single indexed `SELECT` plus the in-memory
            // validation hash. A stale-but-not-stale-enough touch
            // never hurts: eviction operates at minutes-to-hours
            // granularity, not seconds.
            let now = unix_now() as i64;
            if now.saturating_sub(last_access) > LAST_ACCESS_REFRESH_MS {
                if let Err(e) = conn.execute(
                    "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                    params![now, &addr[..]],
                ) {
                    trace!(
                        target: "ant_retrieval::disk_cache",
                        chunk = %hex::encode(addr),
                        "last_access bump failed (non-fatal): {e}",
                    );
                }
            }
            Ok(Some(data))
        })
        .await
        .map_err(|_| DiskCacheError::Panicked)?;
        res
    }

    /// Insert or replace `addr`'s wire bytes. Triggers an eviction
    /// sweep down to `slack_bytes` if the post-insert total crosses
    /// `max_bytes`.
    ///
    /// The caller is expected to have already CAC/SOC-validated
    /// `data`. The retrieval pipeline does this in
    /// [`crate::retrieve_chunk`] before any cache write happens; tests
    /// mirror that invariant by feeding only `cac_new` output. We
    /// don't re-validate on the write path to keep the hot loop free
    /// of redundant hashing — the read path's validation closes the
    /// "rest" hole (corruption between writes), which is the case
    /// validation actually has to defend against.
    pub async fn put(&self, addr: [u8; 32], data: Vec<u8>) -> Result<(), DiskCacheError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<(), DiskCacheError> {
            let size = data.len() as u64;
            let now = unix_now();
            let conn = inner.conn.lock().expect("disk cache mutex poisoned");

            // Fetch any existing size for this address so the byte
            // mirror reflects an upsert rather than a double-count.
            // The `address` column is the primary key, so at most one
            // row matches.
            let existing: Option<u64> = conn
                .query_row(
                    "SELECT size FROM chunks WHERE address = ?1",
                    params![&addr[..]],
                    |row| row.get::<_, i64>(0).map(|n| n as u64),
                )
                .optional()?;

            conn.execute(
                "INSERT INTO chunks (address, data, size, last_access, inserted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(address) DO UPDATE SET
                   data = excluded.data,
                   size = excluded.size,
                   last_access = excluded.last_access",
                params![&addr[..], &data, size as i64, now as i64, now as i64],
            )?;

            let delta = size as i64 - existing.unwrap_or(0) as i64;
            let new_total = update_total(&inner.total_bytes, delta);

            if new_total > inner.max_bytes {
                evict_to_slack(&conn, &inner.total_bytes, inner.slack_bytes)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| DiskCacheError::Panicked)??;
        Ok(())
    }

    /// Number of rows. Used by tests; the byte total is the
    /// production-relevant metric for eviction decisions.
    pub async fn row_count(&self) -> Result<u64, DiskCacheError> {
        let inner = self.inner.clone();
        let n = tokio::task::spawn_blocking(move || -> Result<u64, DiskCacheError> {
            let conn = inner.conn.lock().expect("disk cache mutex poisoned");
            let n: i64 = conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            Ok(n as u64)
        })
        .await
        .map_err(|_| DiskCacheError::Panicked)??;
        Ok(n)
    }
}

/// Apply a signed delta to the `total_bytes` mirror, saturating at
/// zero so a stale UPDATE that makes the delta negative can't underflow
/// the atomic.
fn update_total(total: &AtomicU64, delta: i64) -> u64 {
    if delta >= 0 {
        total.fetch_add(delta as u64, Ordering::Relaxed) + delta as u64
    } else {
        let abs = (-delta) as u64;
        let prev = total.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(abs))
        });
        prev.unwrap_or(0).saturating_sub(abs)
    }
}

/// Drain oldest `last_access` rows in a single transaction until the
/// table's total size sits at or below `slack_bytes`. Mirrors the
/// "evict in batches down to ~95% of the cap" guidance in `PLAN.md`
/// § 6.1: a stable slack target stops every put after the cap is hit
/// from triggering its own single-row eviction.
fn evict_to_slack(
    conn: &Connection,
    total_bytes: &AtomicU64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let current = total_bytes.load(Ordering::Relaxed);
    if current <= slack_bytes {
        return Ok(());
    }
    let need_to_free = current.saturating_sub(slack_bytes);

    // Walk oldest-first and accumulate sizes until we cover
    // `need_to_free`. We stage a row id list so the DELETE runs as a
    // single statement against a known set, rather than an unbounded
    // ranged DELETE that has to be re-evaluated against the index on
    // every step.
    let mut stmt = conn
        .prepare("SELECT address, size FROM chunks ORDER BY last_access ASC, address ASC")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)? as u64))
    })?;

    let mut victim_addrs: Vec<Vec<u8>> = Vec::new();
    let mut freed: u64 = 0;
    for row in rows {
        let (addr, size) = row?;
        victim_addrs.push(addr);
        freed += size;
        if freed >= need_to_free {
            break;
        }
    }
    drop(stmt);

    if victim_addrs.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    {
        let mut del = tx.prepare("DELETE FROM chunks WHERE address = ?1")?;
        for addr in &victim_addrs {
            del.execute(params![addr])?;
        }
    }
    tx.commit()?;

    update_total(total_bytes, -(freed as i64));
    debug!(
        target: "ant_retrieval::disk_cache",
        evicted_rows = victim_addrs.len(),
        freed_bytes = freed,
        new_total = total_bytes.load(Ordering::Relaxed),
        slack = slack_bytes,
        "disk cache eviction sweep complete",
    );
    Ok(())
}

/// Wall-clock time in milliseconds since the UNIX epoch, used as the
/// `last_access` / `inserted_at` timestamp. Milliseconds (not seconds)
/// because two writes inside the same wall-clock second are common
/// during a multi-chunk fetch, and ordering them by `last_access`
/// matters for eviction — a seconds-resolution clock leaves the order
/// ambiguous and the eviction sweep could drop the wrong row when
/// two rows share a timestamp.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::cac_new;
    use tempfile::tempdir;

    fn make_chunk(payload: &[u8]) -> ([u8; 32], Vec<u8>) {
        cac_new(payload).expect("cac_new")
    }

    /// Round-trip a chunk through the cache: a put followed by a get
    /// returns the exact wire bytes that were inserted, and the byte
    /// mirror reflects the single inserted row.
    #[tokio::test]
    async fn put_then_get_round_trips() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("c.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"hello disk");

        cache.put(addr, wire.clone()).await.unwrap();
        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(wire.clone()));
        assert_eq!(cache.used_bytes(), wire.len() as u64);
        assert_eq!(cache.row_count().await.unwrap(), 1);
    }

    /// Restart persistence: rows written by one cache handle must be
    /// readable by a fresh handle opened against the same file. This
    /// is the load-bearing property that distinguishes tier 2 from
    /// the in-memory tier — without it the disk cache contributes
    /// nothing across daemon restarts.
    #[tokio::test]
    async fn rows_persist_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.sqlite");
        let (addr, wire) = make_chunk(b"persistence test");

        {
            let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
            cache.put(addr, wire.clone()).await.unwrap();
            assert_eq!(cache.used_bytes(), wire.len() as u64);
        }

        // Re-open against the same file — initial `total_bytes` must
        // be re-derived from the table, and the row must still be
        // gettable.
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        assert_eq!(cache.used_bytes(), wire.len() as u64);
        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(wire));
    }

    /// Hard size-cap eviction: with several distinct chunks inserted
    /// past the cap, the oldest by `last_access` must drop and the
    /// most recently touched ones must remain. We backdate chunks[0]
    /// and chunks[1] far enough that the `get` on chunk[0] crosses
    /// the freshness threshold (deferred-touch optimisation skips the
    /// `UPDATE` for hits inside [`LAST_ACCESS_REFRESH_MS`], so a
    /// realistic eviction test must exercise rows that have actually
    /// aged out of that window). The hand-written `last_access`
    /// values double as a regression guard against any change that
    /// ranks rows by `inserted_at` — the column the eviction sweep
    /// must NOT consult — instead of `last_access`.
    #[tokio::test]
    async fn evicts_oldest_by_last_access() {
        let dir = tempdir().unwrap();
        // Each chunk is 4104 bytes wire (8-byte span + 4096 payload).
        // Cap of 12 KiB fits 2 full chunks comfortably; the third put
        // crosses the cap and the eviction sweep drains down to
        // ~95% slack.
        let cache = DiskChunkCache::open(dir.path().join("evict.sqlite"), 12 * 1024).unwrap();

        let chunks: Vec<([u8; 32], Vec<u8>)> =
            (0..3u8).map(|i| make_chunk(&vec![i; 4096])).collect();

        cache.put(chunks[0].0, chunks[0].1.clone()).await.unwrap();
        cache.put(chunks[1].0, chunks[1].1.clone()).await.unwrap();

        // Backdate chunks[0] to ~20 minutes ago and chunks[1] to ~10
        // minutes ago. The `get` on chunks[0] then promotes it past
        // chunks[1] in `last_access` order, exercising the
        // stale-refresh branch the deferred-touch optimisation gates
        // on. Without backdating both rows, the freshly-inserted
        // timestamps would already be inside the refresh window and
        // the `get` would skip the touch — making the eviction order
        // ambiguous between two near-identical millisecond timestamps.
        let now = unix_now() as i64;
        {
            let inner = cache.inner.clone();
            let addr0 = chunks[0].0;
            let addr1 = chunks[1].0;
            tokio::task::spawn_blocking(move || {
                let conn = inner.conn.lock().unwrap();
                conn.execute(
                    "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                    params![now - 20 * 60_000, &addr0[..]],
                )
                .unwrap();
                conn.execute(
                    "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                    params![now - 10 * 60_000, &addr1[..]],
                )
                .unwrap();
            })
            .await
            .unwrap();
        }

        // Touch chunks[0]: now that its persisted `last_access` is
        // 20 minutes stale, the deferred-touch branch refreshes it,
        // promoting it past chunks[1]'s 10-minute-old timestamp.
        assert!(cache.get(chunks[0].0).await.unwrap().is_some());

        // Inserting chunks[2] pushes total to ~12.3 KiB > 12 KiB cap.
        cache.put(chunks[2].0, chunks[2].1.clone()).await.unwrap();

        assert!(
            cache.used_bytes() <= 12 * 1024,
            "post-eviction total {} bytes must fit the cap (12 KiB)",
            cache.used_bytes(),
        );
        assert!(
            cache.get(chunks[0].0).await.unwrap().is_some(),
            "touched chunk[0] should survive eviction",
        );
        assert!(
            cache.get(chunks[1].0).await.unwrap().is_none(),
            "untouched chunk[1] should have been evicted first (LRU)",
        );
        assert!(
            cache.get(chunks[2].0).await.unwrap().is_some(),
            "newly-inserted chunk[2] should survive eviction",
        );
    }

    /// Corruption handling: a row whose `data` doesn't validate
    /// against its address must be deleted in place on read and the
    /// caller must see a miss. This is the load-bearing safety
    /// property — the network path runs on the miss and the
    /// write-through eventually replaces the bad row, so a single
    /// corrupted page can't poison the retrieval stream.
    #[tokio::test]
    async fn invalid_disk_row_is_deleted_and_reports_miss() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.sqlite");
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"trust me");

        // Plant a row whose bytes do NOT hash to `addr`. We can't go
        // through `put` to get there because production callers
        // invariably pass valid bytes; injecting via raw SQL is the
        // crisp way to model "data corrupted at rest".
        let mut bad = wire.clone();
        bad[8] ^= 0x01;
        {
            let inner = cache.inner.clone();
            tokio::task::spawn_blocking(move || {
                let conn = inner.conn.lock().unwrap();
                conn.execute(
                    "INSERT INTO chunks (address, data, size, last_access, inserted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![&addr[..], &bad, bad.len() as i64, 0i64, 0i64],
                )
                .unwrap();
                inner
                    .total_bytes
                    .fetch_add(bad.len() as u64, Ordering::Relaxed);
            })
            .await
            .unwrap();
        }
        assert_eq!(cache.row_count().await.unwrap(), 1);

        let got = cache.get(addr).await.unwrap();
        assert!(got.is_none(), "corrupt row must be reported as miss");
        assert_eq!(
            cache.row_count().await.unwrap(),
            0,
            "corrupt row must have been deleted in place",
        );
        assert_eq!(
            cache.used_bytes(),
            0,
            "byte mirror must reflect the corrupt-row deletion",
        );

        // Sanity: a subsequent valid put for the same address goes
        // through cleanly, confirming the deletion left no
        // unique-key constraint behind.
        cache.put(addr, wire.clone()).await.unwrap();
        assert_eq!(cache.get(addr).await.unwrap(), Some(wire));
    }

    /// A second `put` for the same address must not double-count the
    /// row in the byte mirror; the upsert's net delta is the
    /// difference of the new and old sizes (zero if same length).
    /// Without this, a hot chunk being re-cached under the
    /// write-through path would inflate `used_bytes` linearly with
    /// retries and trigger spurious eviction.
    #[tokio::test]
    async fn upsert_does_not_double_count() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("upsert.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"upsert");

        cache.put(addr, wire.clone()).await.unwrap();
        cache.put(addr, wire.clone()).await.unwrap();
        cache.put(addr, wire.clone()).await.unwrap();
        assert_eq!(cache.row_count().await.unwrap(), 1);
        assert_eq!(cache.used_bytes(), wire.len() as u64);
    }

    /// Read the persisted `last_access` for a single address. Test-only
    /// helper that mirrors the SQL the eviction sweep walks, so the
    /// touch tests below assert on the same column eviction reads.
    async fn read_last_access(cache: &DiskChunkCache, addr: [u8; 32]) -> i64 {
        let inner = cache.inner.clone();
        tokio::task::spawn_blocking(move || {
            let conn = inner.conn.lock().unwrap();
            conn.query_row(
                "SELECT last_access FROM chunks WHERE address = ?1",
                params![&addr[..]],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        })
        .await
        .unwrap()
    }

    /// Two consecutive reads inside [`LAST_ACCESS_REFRESH_MS`] must not
    /// rewrite the row's `last_access`. Without this property the
    /// cache turns every hit into a `synchronous=NORMAL` fsync — that
    /// was the user-visible regression where a fully-populated disk
    /// cache served at ~1 MiB/s under fan-out load. Asserting on the
    /// stored timestamp is the crispest way to pin the optimisation
    /// because it skips both clock noise and any test-side timing
    /// flake — either the second `get` wrote and the column moved, or
    /// it didn't.
    #[tokio::test]
    async fn fresh_last_access_is_not_rewritten() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("touch-skip.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"hot read");

        cache.put(addr, wire.clone()).await.unwrap();
        let after_put = read_last_access(&cache, addr).await;

        for _ in 0..10 {
            assert_eq!(cache.get(addr).await.unwrap().as_deref(), Some(&wire[..]));
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let after_reads = read_last_access(&cache, addr).await;
        assert_eq!(
            after_put, after_reads,
            "ten consecutive hot reads inside the refresh window must leave \
             last_access untouched (was {after_put}, now {after_reads})",
        );
    }

    /// A read with a stale `last_access` (older than the refresh
    /// window) *must* rewrite it — otherwise the eviction sweep
    /// would never see that the row is hot, and an actively-read
    /// chunk could be evicted under cap pressure.
    #[tokio::test]
    async fn stale_last_access_is_refreshed() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("touch-stale.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"cool read");
        cache.put(addr, wire.clone()).await.unwrap();

        // Backdate the row to ~10 minutes ago so the next `get` sees
        // an obviously-stale timestamp. We can't sleep through the
        // real refresh window in a unit test (60 s wall-clock), and
        // freezing the cache's clock would invite lying to ourselves
        // about the production wiring — backdating the row is the
        // honest equivalent.
        let backdated = unix_now() as i64 - 10 * 60_000;
        {
            let inner = cache.inner.clone();
            tokio::task::spawn_blocking(move || {
                let conn = inner.conn.lock().unwrap();
                conn.execute(
                    "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                    params![backdated, &addr[..]],
                )
                .unwrap();
            })
            .await
            .unwrap();
        }

        assert!(cache.get(addr).await.unwrap().is_some());
        let after = read_last_access(&cache, addr).await;
        assert!(
            after > backdated,
            "stale last_access must be refreshed on read \
             (was {backdated}, now {after})",
        );
        assert!(
            after >= unix_now() as i64 - LAST_ACCESS_REFRESH_MS,
            "refreshed last_access must be within the freshness window of `now`",
        );
    }
}

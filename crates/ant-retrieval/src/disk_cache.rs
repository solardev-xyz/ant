//! Persistent (SQLite-backed) chunk cache — tier 2 of the cache stack
//! described in `PLAN.md` § 6.1.
//!
//! Network retrieval already validates every chunk on the wire (see
//! [`crate::retrieve_chunk`]) before any cache write. **Bee’s
//! `chunkstore.Get` trusts on-disk bytes the same way** — it reads the
//! retrieval index and sharky blob and returns `swarm.NewChunk` in Go
//! without re-proving the BMT against the address. This implementation matches
//! that contract: disk hits return stored wire bytes without
//! [`ant_crypto::cac_valid`] / [`ant_crypto::soc_valid`]. Bitrot or manual
//! edits can still poison a chunk until the next network refresh replaces
//! the row; that is the same durability model as bee’s localstore cache
//! path.
//!
//! Concurrency (bee-ish throughput goals):
//!
//! - **Dedicated read threads** — `get` is a prepared `SELECT` on a read-only
//!   WAL connection; jobs are routed round-robin over crossbeam channels (no
//!   `spawn_blocking` on the hot path). Pool size scales with
//!   [`std::thread::available_parallelism`] (8–32 workers).
//! - **Dedicated writer thread** — all mutating work (batched `put`s,
//!   `last_access` touches past the freshness window, eviction) runs on
//!   one thread with **batched transactions** so bursts amortise fsync.
//! - **Pragmas** — large `mmap_size` / `cache_size` on every connection.
//!
//! Async rule: `get` / `row_count` ship work to dedicated **read threads**
//! (each with a prepared `SELECT`) over a bounded channel — no
//! `spawn_blocking` hot path — so Tokio workers stay lightweight.

use crossbeam_channel::{
    bounded as cb_bounded, Receiver as CbReceiver, Sender as CbSender, TryRecvError,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};

/// Default eviction "slack" — how far below the cap we drain on every
/// trigger. 95% means an over-cap insert deletes ~5% of the cap in
/// `last_access` order rather than one row at a time, keeping the
/// per-write eviction cost amortised. Mirrors the guidance in
/// `PLAN.md` § 6.1.
const EVICTION_SLACK_RATIO: f64 = 0.95;

/// How "fresh" `last_access` must already be for [`DiskChunkCache::get`]
/// to skip a touch write entirely. 60 s keeps hot reads as plain
/// `SELECT`s on the reader connections; rare `UPDATE`s go through the
/// writer queue.
const LAST_ACCESS_REFRESH_MS: i64 = 60_000;

/// Number of dedicated read threads (each owns one read-only connection +
/// cached prepared statements). Bounded to keep mobile FD use sane.
fn read_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
        .clamp(8, 32)
}

/// Max `put` + `touch` rows coalesced into one writer transaction before commit.
/// Larger batches amortise WAL fsync against bursty write-through.
const WRITE_BATCH_MAX: usize = 256;

/// Target mmap size per SQLite connection (256 MiB).
const MMAP_BYTES: i64 = 512 * 1024 * 1024;

/// Negative `cache_size` pragma value = KiB of page cache per connection
/// (~128 MiB here).
const CACHE_KIB: i64 = -256 * 1024;

/// Default size cap for the persistent chunk cache. 10 GB matches
/// `PLAN.md` § 6.1's desktop / Raspberry Pi default. Operators on
/// constrained boxes (mobile / embedded) should set a smaller cap;
/// power users should raise it.
pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Debug, Error, Clone)]
pub enum DiskCacheError {
    #[error("sqlite: {0}")]
    Sqlite(String),
    #[error("io: {0}")]
    Io(String),
    #[error("background worker panicked")]
    Panicked,
    #[error("disk cache writer stopped")]
    WriterStopped,
}

impl From<rusqlite::Error> for DiskCacheError {
    fn from(e: rusqlite::Error) -> Self {
        DiskCacheError::Sqlite(e.to_string())
    }
}

impl From<std::io::Error> for DiskCacheError {
    fn from(e: std::io::Error) -> Self {
        DiskCacheError::Io(e.to_string())
    }
}

/// SQLite-backed persistent chunk cache.
pub struct DiskChunkCache {
    inner: Arc<Inner>,
}

enum WriteMsg {
    Put {
        addr: [u8; 32],
        data: Vec<u8>,
        ack: oneshot::Sender<Result<(), DiskCacheError>>,
    },
    PutBatch {
        items: Vec<([u8; 32], Vec<u8>)>,
        ack: oneshot::Sender<Result<(), DiskCacheError>>,
    },
    Touch {
        addr: [u8; 32],
        last_access: i64,
    },
    Shutdown,
}

enum ReadJob {
    Get {
        addr: [u8; 32],
        reply: oneshot::Sender<Result<Option<Vec<u8>>, DiskCacheError>>,
    },
    Count {
        reply: oneshot::Sender<Result<u64, DiskCacheError>>,
    },
    Shutdown,
}

struct Inner {
    path: PathBuf,
    read_tx: Vec<CbSender<ReadJob>>,
    read_rr: AtomicUsize,
    read_joins: Mutex<Vec<JoinHandle<()>>>,
    write_tx: CbSender<WriteMsg>,
    writer_thread: Mutex<Option<JoinHandle<()>>>,
    max_bytes: u64,
    total_bytes: Arc<AtomicU64>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        for tx in &self.read_tx {
            let _ = tx.send(ReadJob::Shutdown);
        }
        if let Ok(mut joins) = self.read_joins.lock() {
            for h in joins.drain(..) {
                let _ = h.join();
            }
        }
        let _ = self.write_tx.send(WriteMsg::Shutdown);
        if let Ok(mut g) = self.writer_thread.lock() {
            if let Some(h) = g.take() {
                let _ = h.join();
            }
        }
    }
}

impl DiskChunkCache {
    /// Open (or create) a chunk cache database at `path` with a
    /// `max_bytes` byte cap.
    pub fn open(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self, DiskCacheError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let total_bytes = Arc::new(AtomicU64::new(0));
        let tb = total_bytes.clone();
        let slack_bytes = ((max_bytes as f64) * EVICTION_SLACK_RATIO) as u64;

        let (ready_tx, ready_rx) = sync_channel::<()>(0);
        let (write_tx, write_rx) = cb_bounded::<WriteMsg>(65_536);
        let path_thread = path.clone();
        let write_tx_main = write_tx.clone();

        let join = std::thread::Builder::new()
            .name("ant-disk-cache-writer".into())
            .spawn(move || {
                if let Err(e) = writer_main(
                    path_thread,
                    write_rx,
                    ready_tx,
                    tb,
                    max_bytes,
                    slack_bytes,
                ) {
                    warn!(
                        target: "ant_retrieval::disk_cache",
                        "writer thread exited with error: {e}",
                    );
                }
            })
            .map_err(|e| DiskCacheError::Io(e.to_string()))?;

        ready_rx
            .recv()
            .map_err(|_| DiskCacheError::WriterStopped)?;

        let n_read = read_worker_count();
        let mut read_tx = Vec::with_capacity(n_read);
        let mut read_joins = Vec::with_capacity(n_read);
        for i in 0..n_read {
        let (job_tx, job_rx) = cb_bounded::<ReadJob>(4096);
            let path_r = path.clone();
            let wt = write_tx.clone();
            let j = std::thread::Builder::new()
                .name(format!("ant-disk-cache-read-{i}"))
                .spawn(move || read_worker_loop(path_r, job_rx, wt))
                .map_err(|e| DiskCacheError::Io(e.to_string()))?;
            read_tx.push(job_tx);
            read_joins.push(j);
        }

        debug!(
            target: "ant_retrieval::disk_cache",
            path = %path.display(),
            initial_total = total_bytes.load(Ordering::Relaxed),
            max_bytes,
            slack_bytes,
            read_workers = n_read,
            "opened persistent chunk cache",
        );

        Ok(Self {
            inner: Arc::new(Inner {
                path,
                read_tx,
                read_rr: AtomicUsize::new(0),
                read_joins: Mutex::new(read_joins),
                write_tx: write_tx_main,
                writer_thread: Mutex::new(Some(join)),
                max_bytes,
                total_bytes,
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.inner.max_bytes
    }

    pub fn used_bytes(&self) -> u64 {
        self.inner.total_bytes.load(Ordering::Relaxed)
    }

    /// Look up a chunk by address. Trusts stored bytes (bee `chunkstore`
    /// semantics). A stale `last_access` is refreshed via the writer
    /// queue (best-effort). Serviced by a dedicated read thread + prepared
    /// statement (no `spawn_blocking` on the hot path).
    pub async fn get(&self, addr: [u8; 32]) -> Result<Option<Vec<u8>>, DiskCacheError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let i = self.inner.read_rr.fetch_add(1, Ordering::Relaxed) % self.inner.read_tx.len();
        self.inner.read_tx[i]
            .send(ReadJob::Get {
                addr,
                reply: reply_tx,
            })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        reply_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }

    /// Batched insert / upsert in a **single** writer transaction (amortises
    /// WAL fsync). Prefer this when filling the cache with many freshly
    /// validated chunks at once.
    pub async fn put_batch(&self, items: Vec<([u8; 32], Vec<u8>)>) -> Result<(), DiskCacheError> {
        if items.is_empty() {
            return Ok(());
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .write_tx
            .send(WriteMsg::PutBatch {
                items,
                ack: ack_tx,
            })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        ack_rx
            .await
            .map_err(|_| DiskCacheError::Panicked)?
    }

    pub async fn put(&self, addr: [u8; 32], data: Vec<u8>) -> Result<(), DiskCacheError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .write_tx
            .send(WriteMsg::Put {
                addr,
                data,
                ack: ack_tx,
            })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        ack_rx
            .await
            .map_err(|_| DiskCacheError::Panicked)?
    }

    pub async fn row_count(&self) -> Result<u64, DiskCacheError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner.read_tx[0]
            .send(ReadJob::Count { reply: reply_tx })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        reply_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }
}

fn apply_shared_pragmas(conn: &Connection, is_write: bool) -> Result<(), rusqlite::Error> {
    conn.pragma_update(None, "mmap_size", MMAP_BYTES)?;
    conn.pragma_update(None, "cache_size", CACHE_KIB)?;
    if is_write {
        conn.pragma_update(None, "page_size", 8192)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
    }
    Ok(())
}

fn open_write_connection(path: &Path) -> Result<Connection, rusqlite::Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    apply_shared_pragmas(&conn, true)?;
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
    Ok(conn)
}

fn open_read_connection(path: &Path) -> Result<Connection, rusqlite::Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    apply_shared_pragmas(&conn, false)?;
    Ok(conn)
}

fn read_worker_loop(path: PathBuf, rx: CbReceiver<ReadJob>, write_tx: CbSender<WriteMsg>) {
    let Ok(conn) = open_read_connection(&path) else {
        return;
    };
    let Ok(mut sel_stmt) = conn.prepare_cached(
        "SELECT data, last_access FROM chunks WHERE address = ?1",
    ) else {
        return;
    };
    let Ok(mut count_stmt) = conn.prepare_cached("SELECT COUNT(*) FROM chunks") else {
        return;
    };

    while let Ok(job) = rx.recv() {
        match job {
            ReadJob::Shutdown => break,
            ReadJob::Get { addr, reply } => {
                let res = (|| -> Result<Option<Vec<u8>>, DiskCacheError> {
                    let row: Option<(Vec<u8>, i64)> = sel_stmt
                        .query_row(params![&addr[..]], |row| {
                            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                        })
                        .optional()
                        .map_err(DiskCacheError::from)?;
                    let Some((data, last_access)) = row else {
                        return Ok(None);
                    };
                    let now = unix_now() as i64;
                    if now.saturating_sub(last_access) > LAST_ACCESS_REFRESH_MS {
                        let _ = write_tx.send(WriteMsg::Touch {
                            addr,
                            last_access: now,
                        });
                    }
                    Ok(Some(data))
                })();
                let _ = reply.send(res);
            }
            ReadJob::Count { reply } => {
                let res = (|| -> Result<u64, DiskCacheError> {
                    let n: i64 = count_stmt
                        .query_row([], |row| row.get(0))
                        .map_err(DiskCacheError::from)?;
                    Ok(n as u64)
                })();
                let _ = reply.send(res);
            }
        }
    }
}

fn process_put_batch_transaction(
    conn: &mut Connection,
    items: &[([u8; 32], Vec<u8>)],
    total_bytes: &Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for (addr, data) in items {
        put_upsert_tx(&tx, *addr, data.as_slice(), total_bytes)?;
    }
    tx.commit()?;
    let current = total_bytes.load(Ordering::Relaxed);
    if current > max_bytes {
        evict_to_slack(conn, total_bytes, slack_bytes)?;
    }
    Ok(())
}

fn consume_put_touch_batch(
    conn: &mut Connection,
    batch: Vec<WriteMsg>,
    total_bytes: &Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) -> Result<bool, DiskCacheError> {
    let mut shutdown = false;
    let mut put_ops: Vec<([u8; 32], Vec<u8>, oneshot::Sender<Result<(), DiskCacheError>>)> =
        Vec::new();
    let mut touches: Vec<([u8; 32], i64)> = Vec::new();

    for m in batch {
        match m {
            WriteMsg::Put { addr, data, ack } => put_ops.push((addr, data, ack)),
            WriteMsg::Touch { addr, last_access } => touches.push((addr, last_access)),
            WriteMsg::Shutdown => shutdown = true,
            WriteMsg::PutBatch { .. } => {}
        }
    }

    if put_ops.is_empty() && touches.is_empty() {
        return Ok(shutdown);
    }

    let batch_res: Result<(), DiskCacheError> = (|| {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (addr, data, _) in &put_ops {
            put_upsert_tx(&tx, *addr, data, total_bytes)?;
        }
        for (addr, last) in &touches {
            if let Err(e) = tx.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                params![last, &addr[..]],
            ) {
                trace!(
                    target: "ant_retrieval::disk_cache",
                    "last_access touch failed (non-fatal): {e}",
                );
            }
        }
        tx.commit()?;
        Ok(())
    })();

    match batch_res {
        Ok(()) => {
            let current = total_bytes.load(Ordering::Relaxed);
            if current > max_bytes {
                if let Err(e) = evict_to_slack(conn, total_bytes, slack_bytes) {
                    for (_, _, ack) in put_ops {
                        let _ = ack.send(Err(e.clone()));
                    }
                    return Err(e);
                }
            }
            for (_, _, ack) in put_ops {
                let _ = ack.send(Ok(()));
            }
            Ok(shutdown)
        }
        Err(e) => {
            for (_, _, ack) in put_ops {
                let _ = ack.send(Err(e.clone()));
            }
            Err(e)
        }
    }
}

fn writer_main(
    path: PathBuf,
    rx: CbReceiver<WriteMsg>,
    ready_tx: SyncSender<()>,
    total_bytes: Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let mut conn = open_write_connection(&path)?;

    let initial_total: u64 = conn
        .query_row("SELECT COALESCE(SUM(size), 0) FROM chunks", [], |row| {
            row.get::<_, i64>(0).map(|n| n as u64)
        })
        .unwrap_or(0);
    total_bytes.store(initial_total, Ordering::Relaxed);

    ready_tx.send(()).map_err(|_| DiskCacheError::WriterStopped)?;

    'outer: loop {
        match rx.recv() {
            Err(_) => break,
            Ok(WriteMsg::Shutdown) => break,
            Ok(WriteMsg::PutBatch { items, ack }) => {
                let r = process_put_batch_transaction(
                    &mut conn,
                    &items,
                    &total_bytes,
                    max_bytes,
                    slack_bytes,
                );
                let _ = ack.send(r);
                continue;
            }
            Ok(first) => {
                let mut batch = vec![first];
                while batch.len() < WRITE_BATCH_MAX {
                    match rx.try_recv() {
                        Ok(WriteMsg::Shutdown) => {
                            batch.push(WriteMsg::Shutdown);
                            break;
                        }
                        Ok(WriteMsg::PutBatch { items, ack }) => {
                            let shutdown_after = match consume_put_touch_batch(
                                &mut conn,
                                batch,
                                &total_bytes,
                                max_bytes,
                                slack_bytes,
                            ) {
                                Ok(s) => s,
                                Err(e) => {
                                    let _ = ack.send(Err(e));
                                    continue 'outer;
                                }
                            };
                            let r = process_put_batch_transaction(
                                &mut conn,
                                &items,
                                &total_bytes,
                                max_bytes,
                                slack_bytes,
                            );
                            let _ = ack.send(r);
                            if shutdown_after {
                                break 'outer;
                            }
                            continue 'outer;
                        }
                        Ok(m) => batch.push(m),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break 'outer,
                    }
                }

                let shutdown_after = consume_put_touch_batch(
                    &mut conn,
                    batch,
                    &total_bytes,
                    max_bytes,
                    slack_bytes,
                )?;
                if shutdown_after {
                    break 'outer;
                }
            }
        }
    }

    Ok(())
}

fn put_upsert_tx(
    tx: &rusqlite::Transaction<'_>,
    addr: [u8; 32],
    data: &[u8],
    total_bytes: &AtomicU64,
) -> Result<(), DiskCacheError> {
    let size = data.len() as u64;
    let now = unix_now();
    let existing: Option<u64> = tx
        .query_row(
            "SELECT size FROM chunks WHERE address = ?1",
            params![&addr[..]],
            |row| row.get::<_, i64>(0).map(|n| n as u64),
        )
        .optional()?;

    tx.execute(
        "INSERT INTO chunks (address, data, size, last_access, inserted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(address) DO UPDATE SET
                   data = excluded.data,
                   size = excluded.size,
                   last_access = excluded.last_access",
        params![&addr[..], data, size as i64, now as i64, now as i64],
    )?;

    let delta = size as i64 - existing.unwrap_or(0) as i64;
    update_total(total_bytes, delta);
    Ok(())
}

fn update_total(total: &AtomicU64, delta: i64) -> u64 {
    if delta >= 0 {
        total.fetch_add(delta as u64, Ordering::Relaxed) + delta as u64
    } else {
        let abs = (-delta) as u64;
        match total.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(abs))
        }) {
            Ok(prev) => prev.saturating_sub(abs),
            Err(_) => total.load(Ordering::Relaxed),
        }
    }
}

fn evict_to_slack(
    conn: &mut Connection,
    total_bytes: &AtomicU64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let current = total_bytes.load(Ordering::Relaxed);
    if current <= slack_bytes {
        return Ok(());
    }
    let need_to_free = current.saturating_sub(slack_bytes);

    let mut stmt = conn
        .prepare("SELECT address, size FROM chunks ORDER BY last_access ASC, address ASC")
        .map_err(|e| DiskCacheError::from(e))?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)? as u64))
    })?;

    let mut victim_addrs: Vec<Vec<u8>> = Vec::new();
    let mut freed: u64 = 0;
    for row in rows {
        let (addr, size) = row.map_err(|e| DiskCacheError::from(e))?;
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

    let tx = conn.unchecked_transaction().map_err(|e| DiskCacheError::from(e))?;
    {
        let mut del = tx
            .prepare("DELETE FROM chunks WHERE address = ?1")
            .map_err(|e| DiskCacheError::from(e))?;
        for addr in &victim_addrs {
            del.execute(params![addr]).map_err(|e| DiskCacheError::from(e))?;
        }
    }
    tx.commit().map_err(|e| DiskCacheError::from(e))?;

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
    use rusqlite::params as rparams;
    use tempfile::tempdir;

    fn make_chunk(payload: &[u8]) -> ([u8; 32], Vec<u8>) {
        cac_new(payload).expect("cac_new")
    }

    #[tokio::test]
    async fn put_batch_round_trips() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("batch.sqlite"), 1 << 20).unwrap();
        let a = make_chunk(b"a");
        let b = make_chunk(b"b");
        cache
            .put_batch(vec![(a.0, a.1.clone()), (b.0, b.1.clone())])
            .await
            .unwrap();
        assert_eq!(cache.get(a.0).await.unwrap(), Some(a.1));
        assert_eq!(cache.get(b.0).await.unwrap(), Some(b.1));
        assert_eq!(cache.row_count().await.unwrap(), 2);
    }

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

        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        assert_eq!(cache.used_bytes(), wire.len() as u64);
        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(wire));
    }

    #[tokio::test]
    async fn evicts_oldest_by_last_access() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("evict.sqlite");
        let cache = DiskChunkCache::open(&path, 12 * 1024).unwrap();

        let chunks: Vec<([u8; 32], Vec<u8>)> =
            (0..3u8).map(|i| make_chunk(&vec![i; 4096])).collect();

        cache.put(chunks[0].0, chunks[0].1.clone()).await.unwrap();
        cache.put(chunks[1].0, chunks[1].1.clone()).await.unwrap();

        let now = unix_now() as i64;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                rparams![now - 20 * 60_000, &chunks[0].0[..]],
            )
            .unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                rparams![now - 10 * 60_000, &chunks[1].0[..]],
            )
            .unwrap();
        }

        assert!(cache.get(chunks[0].0).await.unwrap().is_some());

        cache.put(chunks[2].0, chunks[2].1.clone()).await.unwrap();

        assert!(
            cache.used_bytes() <= 12 * 1024,
            "post-eviction total {} bytes must fit the cap (12 KiB)",
            cache.used_bytes(),
        );
        assert!(cache.get(chunks[0].0).await.unwrap().is_some());
        assert!(cache.get(chunks[1].0).await.unwrap().is_none());
        assert!(cache.get(chunks[2].0).await.unwrap().is_some());
    }

    /// Bee-style trust: a hand-corrupted row is still returned on read
    /// (same as bee chunkstore returning sharky bytes for a retrieval key).
    #[tokio::test]
    async fn corrupt_disk_row_is_trusted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.sqlite");
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"trust me");
        drop(cache);

        let mut bad = wire.clone();
        bad[8] ^= 0x01;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO chunks (address, data, size, last_access, inserted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                rparams![&addr[..], &bad, bad.len() as i64, 0i64, 0i64],
            )
            .unwrap();
        }
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        assert_eq!(cache.row_count().await.unwrap(), 1);

        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(bad));
    }

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

    async fn read_last_access(cache: &DiskChunkCache, addr: [u8; 32]) -> i64 {
        let path = cache.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.query_row(
                "SELECT last_access FROM chunks WHERE address = ?1",
                rparams![&addr[..]],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        })
        .await
        .unwrap()
    }

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

    #[tokio::test]
    async fn stale_last_access_is_refreshed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("touch-stale.sqlite");
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"cool read");
        cache.put(addr, wire.clone()).await.unwrap();

        let backdated = unix_now() as i64 - 10 * 60_000;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                rparams![backdated, &addr[..]],
            )
            .unwrap();
        }

        assert!(cache.get(addr).await.unwrap().is_some());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after = read_last_access(&cache, addr).await;
        assert!(
            after > backdated,
            "stale last_access must be refreshed on read \
             (was {backdated}, now {after})",
        );
    }
}

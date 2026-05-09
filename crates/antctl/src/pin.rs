//! `antctl pin` — populate the daemon's local disk chunk cache with
//! every chunk of a previously-uploaded file, so this node's HTTP
//! gateway resolves `/bzz/<reference>` immediately without waiting
//! for bee-side neighbourhood replication.
//!
//! Re-uses [`ant_retrieval::StreamingSplitter`] (constant-memory chunk
//! tree) and [`ant_retrieval::build_single_file_manifest`] (the same
//! manifest writer the upload driver uses), so the derived chunk
//! addresses match `antctl upload` byte-for-byte. The CLI dispatches
//! every chunk over the control socket as a
//! [`Request::PutChunkLocal`]; the daemon validates the wire bytes
//! (BMT-with-span match) and writes them straight to the SQLite-backed
//! disk cache (`<data-dir>/cache.sqlite`). No postage stamp, no
//! pushsync — pinning is purely a local mirror.
//!
//! The CLI verifies the derived manifest root against the
//! user-supplied `<reference>` *after* every chunk has been
//! dispatched. A mismatch surfaces as a hard error at the end (the
//! data chunks are still in the cache; LRU will reclaim them
//! eventually if the operator never pins a matching file).
//!
//! Concurrency: 8 worker threads, each holding their own UnixStream.
//! The splitter is much faster than SQLite at this scale, so
//! workers are the bottleneck — but 8x parallelism is enough to
//! pin a 6.7 GiB file (≈ 1.76M chunks) in 2-3 minutes against the
//! local disk-cache writer.

use crate::PinArgs;
use ant_control::{request_sync, Request, Response};
use ant_crypto::CHUNK_SIZE;
use ant_retrieval::{build_single_file_manifest, SplitChunk, StreamingSplitter};
use anyhow::{anyhow, bail, Context, Result};
use memmap2::Mmap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// 8 sockets in parallel saturates the daemon's SQLite writer at the
/// chunk sizes we care about (≤ 8 + 4096 wire bytes); going wider
/// would burn more daemon-side connection slots without improving
/// throughput.
const WORKER_COUNT: usize = 8;

/// Entry point for `antctl pin`. Splits the source file with the
/// streaming splitter, derives the matching mantaray manifest (or
/// skips it under `--raw`), dispatches every chunk to the daemon's
/// disk cache via `Request::PutChunkLocal`, and verifies that the
/// derived root matches the user-supplied `<reference>`.
pub fn run(socket: &Path, args: PinArgs, json: bool) -> Result<()> {
    let target_addr = parse_reference(&args.reference)?;
    let abs_path = args
        .path
        .canonicalize()
        .with_context(|| format!("source path {}", args.path.display()))?;
    let metadata = std::fs::metadata(&abs_path)
        .with_context(|| format!("stat source {}", abs_path.display()))?;
    if !metadata.is_file() {
        bail!("not a regular file: {}", abs_path.display());
    }
    let file_size = metadata.len();

    let display_name = args.name.clone().unwrap_or_else(|| {
        abs_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string()
    });
    let content_type = args
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let (tx, rx) = mpsc::sync_channel::<SplitChunk>(WORKER_COUNT * 8);
    let rx = Arc::new(Mutex::new(rx));
    let socket_path = socket.to_path_buf();
    let mut workers: Vec<JoinHandle<Result<u64>>> = Vec::with_capacity(WORKER_COUNT);
    for _ in 0..WORKER_COUNT {
        let rx = rx.clone();
        let sock = socket_path.clone();
        workers.push(std::thread::spawn(move || worker(rx, sock)));
    }

    let started = Instant::now();
    let data_root = match split_file(&abs_path, file_size, &tx, started) {
        Ok(r) => r,
        Err(e) => {
            drop(tx);
            // Drain workers to surface their error (likely the same
            // socket failure that killed the splitter); only return
            // the splitter error if workers ran cleanly.
            for w in workers {
                if let Ok(Err(werr)) = w.join() {
                    return Err(werr);
                }
            }
            return Err(e);
        }
    };

    let manifest_root = if args.raw {
        data_root
    } else {
        let result = build_single_file_manifest(&display_name, Some(&content_type), data_root)
            .map_err(|e| anyhow!("build manifest: {e}"))?;
        for chunk in result.chunks {
            tx.send(chunk).map_err(|_| anyhow!("pin worker died"))?;
        }
        result.root
    };

    drop(tx);
    let total_pinned = join_workers(workers)?;
    eprintln!();

    if manifest_root != target_addr {
        bail!(
            "manifest root mismatch: derived 0x{} but reference is 0x{}; check --name / --content-type / --raw match the upload",
            hex::encode(manifest_root),
            hex::encode(target_addr),
        );
    }

    report(json, manifest_root, total_pinned, file_size, started.elapsed().as_secs_f64())
}

fn worker(
    rx: Arc<Mutex<Receiver<SplitChunk>>>,
    sock: PathBuf,
) -> Result<u64> {
    let mut count = 0u64;
    loop {
        let chunk = {
            let g = rx.lock().expect("pin rx mutex");
            match g.recv() {
                Ok(c) => c,
                Err(_) => break,
            }
        };
        let req = Request::PutChunkLocal {
            wire_hex: format!("0x{}", hex::encode(&chunk.wire)),
        };
        let resp = request_sync(&sock, &req)
            .map_err(|e| anyhow!("pin chunk 0x{}: {e}", hex::encode(chunk.address)))?;
        match resp {
            Response::Ok { .. } => count += 1,
            Response::Error { message } => bail!("antd: {message}"),
            other => bail!("unexpected response: {other:?}"),
        }
    }
    Ok(count)
}

fn split_file(
    abs_path: &Path,
    file_size: u64,
    tx: &SyncSender<SplitChunk>,
    started: Instant,
) -> Result<[u8; 32]> {
    let file = std::fs::File::open(abs_path)
        .with_context(|| format!("open {}", abs_path.display()))?;
    let body: Option<Mmap> = if file_size == 0 {
        None
    } else {
        // SAFETY: pinning is read-only and the operator is expected
        // to keep the source stable during the run. If it changes
        // mid-run the derived chunk addresses won't match the
        // expected reference and we abort at the end.
        Some(unsafe { Mmap::map(&file)? })
    };
    let mut splitter = StreamingSplitter::new();
    let mut last_report = Instant::now();
    let mut cursor: usize = 0;
    let total = file_size as usize;
    let body_slice = body.as_deref();
    while cursor < total {
        let end = (cursor + CHUNK_SIZE).min(total);
        let leaf = &body_slice.expect("non-empty file mmap")[cursor..end];
        for chunk in splitter.push_leaf(leaf) {
            tx.send(chunk).map_err(|_| anyhow!("pin worker died"))?;
        }
        cursor = end;
        if last_report.elapsed() >= Duration::from_millis(500) {
            let pct = if total > 0 {
                (cursor as f64 / total as f64) * 100.0
            } else {
                100.0
            };
            let mibps = cursor as f64 / started.elapsed().as_secs_f64() / (1024.0 * 1024.0);
            eprint!("\rpin: {pct:6.2}%  {mibps:6.1} MiB/s  ");
            std::io::stderr().flush().ok();
            last_report = Instant::now();
        }
    }
    let (data_root, _bytes, tail) = splitter.finish();
    for chunk in tail {
        tx.send(chunk).map_err(|_| anyhow!("pin worker died"))?;
    }
    Ok(data_root)
}

fn join_workers(workers: Vec<JoinHandle<Result<u64>>>) -> Result<u64> {
    let mut total = 0u64;
    for w in workers {
        match w.join().map_err(|_| anyhow!("worker panicked"))? {
            Ok(n) => total += n,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

fn report(
    json: bool,
    manifest_root: [u8; 32],
    total_pinned: u64,
    file_size: u64,
    elapsed: f64,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "reference": format!("0x{}", hex::encode(manifest_root)),
                "chunks_pinned": total_pinned,
                "bytes": file_size,
                "elapsed_seconds": elapsed,
            })
        );
    } else {
        let mibps = file_size as f64 / elapsed / (1024.0 * 1024.0);
        println!(
            "pinned {total_pinned} chunks ({file_size} bytes) in {elapsed:.1}s ({mibps:.1} MiB/s)"
        );
        println!("  reference: 0x{}", hex::encode(manifest_root));
    }
    Ok(())
}

fn parse_reference(s: &str) -> Result<[u8; 32]> {
    let s = s
        .strip_prefix("bzz://")
        .or_else(|| s.strip_prefix("bytes://"))
        .unwrap_or(s);
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if s.len() != 64 {
        bail!("reference must be 64 hex chars; got {}", s.len());
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out).map_err(|e| anyhow!("invalid hex: {e}"))?;
    Ok(out)
}

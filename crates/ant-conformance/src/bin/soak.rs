//! SOC feed reliability soak (bug-hunt campaign).
//!
//! Reproduces the field failure mode from the 2026-07 reliability
//! report: an app writing per-writer append-only SOC feeds through a
//! light ant node sees intermittent 502s on `POST /soc`, on reading
//! SOCs back, and on feed `latest` resolution — self-healing on retry.
//!
//! The soak drives exactly that workload headlessly and measures it:
//!
//! - **writer node**: upload-capable antd (lab batch via `--batch`),
//!   optionally `--writer-target-peers` to emulate the thin peer set
//!   of an embedded/browser node;
//! - **reader node**: independent ultra-light antd (own data dir, no
//!   batch) for cross-node durability checks;
//! - **W parallel workers**, each writing feeds under FRESH random
//!   owner keys + topics (fresh keyspace neighbourhoods — the worst
//!   case from the report), `--updates-per-feed` sequential updates;
//! - per write: client-style bounded retry loop, every attempt logged;
//! - after each first-201: immediate same-node SOC read-back and feed
//!   `latest` resolution, retried until visible (time-to-readable);
//! - cross-node probes of every written SOC at T ≈ {1, 10, 60, 300} s.
//!
//! Every event is one JSONL line in
//! `perf/results/soak-<label>-<ts>.jsonl` carrying status, body
//! message, latency, the writer node's live peer count and the best
//! connected proximity order to the target address at event time —
//! the correlates the report's Exp E asks for. A summary JSON is
//! written alongside.
//!
//! ```text
//! soak --label baseline --batch 3 --workers 4 --feeds-per-worker 3 \
//!      --updates-per-feed 10 [--writer-target-peers 15] [--port-base 4633]
//! ```

use ant_crypto::{keccak256, sign_handshake_data};
use ant_retrieval::feed::sequence_update_id;
use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WRITE_RETRY_CAP: u32 = 8;
const WRITE_RETRY_DELAY_MS: u64 = 750;
const READBACK_CAP: Duration = Duration::from_secs(30);
const XNODE_SCHEDULE_SECS: &[u64] = &[1, 10, 60, 300];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn load_env() -> HashMap<String, String> {
    let mut vars = HashMap::new();
    if let Ok(body) = std::fs::read_to_string(repo_root().join(".env")) {
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                vars.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    for (k, v) in std::env::vars() {
        vars.insert(k, v);
    }
    vars
}

/// xorshift64* — good enough for payload noise and key material in a
/// test harness (fresh identities per run, no security requirement).
struct Prng(u64);
impl Prng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for c in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            c.copy_from_slice(&v[..c.len()]);
        }
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Antd {
    _guard: ChildGuard,
    api: String,
    data_dir: PathBuf,
}

#[allow(clippy::too_many_arguments)]
fn spawn_antd(
    tag: &str,
    port: u16,
    upload: bool,
    stamp_key_hex: Option<&str>,
    target_peers: Option<u32>,
) -> Antd {
    let data_dir = std::env::temp_dir().join(format!("ant-soak-{tag}-{port}-{}", unix_now()));
    std::fs::create_dir_all(&data_dir).expect("data dir");
    if upload {
        let shared = repo_root().join("perf/state/postage");
        assert!(shared.is_dir(), "lab postage state missing");
        std::os::unix::fs::symlink(&shared, data_dir.join("postage")).expect("postage symlink");
    }
    let api = format!("127.0.0.1:{port}");
    let mut cmd = Command::new(repo_root().join("target/release/antd"));
    cmd.arg("--data-dir")
        .arg(&data_dir)
        .arg("--api-addr")
        .arg(&api)
        .arg("--log-level")
        .arg("warn")
        .stdout(
            std::fs::File::create(data_dir.join("antd.log"))
                .map_or_else(|_| Stdio::null(), Stdio::from),
        )
        .stderr(
            std::fs::OpenOptions::new()
                .append(true)
                .open(data_dir.join("antd.log"))
                .map_or_else(|_| Stdio::null(), Stdio::from),
        );
    if let Some(n) = target_peers {
        cmd.arg("--target-peers").arg(n.to_string());
    }
    cmd.env_remove("GNOSIS_RPC_URL")
        .env_remove("STORAGE_STAMP_BATCH_ID");
    if let Some(k) = stamp_key_hex {
        cmd.env("STORAGE_STAMP_PRIVATE_KEY", k);
    } else {
        cmd.env_remove("STORAGE_STAMP_PRIVATE_KEY");
    }
    let child = cmd.spawn().expect("spawn antd");
    Antd {
        _guard: ChildGuard(child),
        api: format!("http://{api}"),
        data_dir,
    }
}

async fn wait_http_ok(client: &reqwest::Client, url: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(r) = client.get(url).send().await {
            if r.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

/// Overlays of the node's connected peers (for PO coverage) + count.
async fn peer_overlays(client: &reqwest::Client, api: &str) -> Vec<[u8; 32]> {
    let Ok(r) = client.get(format!("{api}/peers")).send().await else {
        return Vec::new();
    };
    let Ok(body) = r.bytes().await else {
        return Vec::new();
    };
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return Vec::new();
    };
    v["peers"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|p| {
                    let hex_str = p["address"].as_str()?;
                    let mut o = [0u8; 32];
                    hex::decode_to_slice(hex_str.trim_start_matches("0x"), &mut o).ok()?;
                    Some(o)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn proximity(a: &[u8; 32], b: &[u8; 32]) -> u8 {
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        let d = x ^ y;
        if d != 0 {
            return (i as u8) * 8 + d.leading_zeros() as u8;
        }
    }
    255
}

fn best_po(overlays: &[[u8; 32]], addr: &[u8; 32]) -> i32 {
    overlays
        .iter()
        .map(|o| i32::from(proximity(o, addr)))
        .max()
        .unwrap_or(-1)
}

/// Shared event sink: JSONL file + counters for the live summary.
struct Sink {
    file: Mutex<std::io::BufWriter<std::fs::File>>,
    started: Instant,
    events: AtomicU64,
}

impl Sink {
    fn emit(&self, mut ev: serde_json::Value) {
        ev["t"] = serde_json::json!(self.started.elapsed().as_secs_f64());
        let mut f = self.file.lock().expect("sink");
        let _ = writeln!(f, "{ev}");
        let _ = f.flush();
        self.events.fetch_add(1, Ordering::Relaxed);
    }
}

struct WriteOutcome {
    ok: bool,
    attempts: u32,
    first_status: u16,
}

/// One SOC write with the client-style retry loop; every attempt is an
/// event. Returns whether it eventually landed and how.
#[allow(clippy::too_many_arguments)]
async fn soc_write(
    client: &reqwest::Client,
    api: &str,
    sink: &Sink,
    batch_hex: &str,
    owner_hex: &str,
    id: &[u8; 32],
    sig: &[u8; 65],
    cac_wire: &[u8],
    soc_addr: &[u8; 32],
    overlays: &[[u8; 32]],
    worker: usize,
    feed: u64,
    index: u64,
) -> WriteOutcome {
    let url = format!(
        "{api}/soc/{owner_hex}/{}?sig={}",
        hex::encode(id),
        hex::encode(sig)
    );
    let mut first_status = 0u16;
    for attempt in 1..=WRITE_RETRY_CAP {
        let t0 = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", "application/octet-stream")
            .header("swarm-postage-batch-id", batch_hex)
            .body(cac_wire.to_vec())
            .send()
            .await;
        let (status, message) = match resp {
            Ok(r) => {
                let status = r.status().as_u16();
                let body = r.text().await.unwrap_or_default();
                let message = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v["message"].as_str().map(str::to_string))
                    .unwrap_or(body);
                (status, message)
            }
            Err(e) => (0, e.to_string()),
        };
        if attempt == 1 {
            first_status = status;
        }
        sink.emit(serde_json::json!({
            "kind": "write",
            "worker": worker, "feed": feed, "index": index,
            "attempt": attempt,
            "status": status,
            "message": if status == 201 { String::new() } else { message },
            "latency_ms": t0.elapsed().as_millis() as u64,
            "addr": hex::encode(soc_addr),
            "po_best": best_po(overlays, soc_addr),
            "peers": overlays.len(),
        }));
        if status == 201 {
            return WriteOutcome {
                ok: true,
                attempts: attempt,
                first_status,
            };
        }
        tokio::time::sleep(Duration::from_millis(
            WRITE_RETRY_DELAY_MS * u64::from(attempt),
        ))
        .await;
    }
    WriteOutcome {
        ok: false,
        attempts: WRITE_RETRY_CAP,
        first_status,
    }
}

/// Poll `url` until 200 (cap [`READBACK_CAP`]); returns
/// `(ok, first_status, attempts, ms_to_ok)`.
async fn read_until_ok(client: &reqwest::Client, url: &str) -> (bool, u16, u32, u64) {
    let start = Instant::now();
    let mut first_status = 0u16;
    let mut attempts = 0u32;
    while start.elapsed() < READBACK_CAP {
        attempts += 1;
        let status = match client.get(url).send().await {
            Ok(r) => {
                let s = r.status().as_u16();
                let _ = r.bytes().await;
                s
            }
            Err(_) => 0,
        };
        if attempts == 1 {
            first_status = status;
        }
        if status == 200 {
            return (
                true,
                first_status,
                attempts,
                start.elapsed().as_millis() as u64,
            );
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    (
        false,
        first_status,
        attempts,
        start.elapsed().as_millis() as u64,
    )
}

#[derive(Clone)]
struct XnodeProbe {
    soc_url_path: String,
    addr: String,
    written_at: Instant,
}

struct Args(HashMap<String, String>);
impl Args {
    fn parse(argv: &[String]) -> Self {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < argv.len() {
            if let Some(k) = argv[i].strip_prefix("--") {
                if i + 1 < argv.len() && !argv[i + 1].starts_with("--") {
                    m.insert(k.to_string(), argv[i + 1].clone());
                    i += 2;
                } else {
                    m.insert(k.to_string(), "true".into());
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        Self(m)
    }
    fn u64_or(&self, k: &str, d: u64) -> u64 {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    fn get(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(String::as_str)
    }
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = Args::parse(&argv);
    let label = args.get("label").unwrap_or("soak").to_string();
    let workers = args.u64_or("workers", 4) as usize;
    let feeds_per_worker = args.u64_or("feeds-per-worker", 3);
    let updates_per_feed = args.u64_or("updates-per-feed", 10);
    let port_base = args.u64_or("port-base", 4633) as u16;
    let writer_tp = args.get("writer-target-peers").and_then(|v| v.parse().ok());
    let batch_n = args.get("batch").unwrap_or("3");
    let payload_bytes = args.u64_or("payload-bytes", 256) as usize;
    // Harsh-mode knobs (field-repro): start writing the moment the API
    // is up (the app doesn't wait for a warm pool), thin reader pool,
    // and a writer restart at the end with a full cold re-read of
    // everything written (the browser-reload scenario).
    let no_warmup = args.get("no-warmup").is_some();
    let reader_tp = args.get("reader-target-peers").and_then(|v| v.parse().ok());
    let restart_reread = args.get("restart-reread").is_some();

    let env = load_env();
    let (bvar, kvar) = match batch_n {
        "4" => ("SWARM_BATCH_ID_4", "SWARM_OWNER_PRIVATE_KEY_4"),
        "5" => ("SWARM_BATCH_ID_5", "ANT_LAB_WALLET_PRIVATE_KEY"),
        _ => ("SWARM_BATCH_ID_3", "SWARM_OWNER_PRIVATE_KEY_3"),
    };
    let batch_hex = env
        .get(bvar)
        .unwrap_or_else(|| panic!("{bvar} missing"))
        .trim_start_matches("0x")
        .to_lowercase();
    let key_hex = env
        .get(kvar)
        .unwrap_or_else(|| panic!("{kvar} missing"))
        .trim_start_matches("0x")
        .to_string();

    let results = repo_root().join("perf/results");
    std::fs::create_dir_all(&results).unwrap();
    let out = results.join(format!("soak-{label}-{}.jsonl", unix_now()));
    let sink = Arc::new(Sink {
        file: Mutex::new(std::io::BufWriter::new(
            std::fs::File::create(&out).expect("open jsonl"),
        )),
        started: Instant::now(),
        events: AtomicU64::new(0),
    });
    println!("events → {}", out.display());

    // Nodes.
    let mut writer_slot = Some(spawn_antd(
        "writer",
        port_base,
        true,
        Some(&key_hex),
        writer_tp,
    ));
    let writer = writer_slot.as_ref().unwrap();
    let reader = spawn_antd("reader", port_base + 1, false, None, reader_tp);
    let client = reqwest::Client::new();
    assert!(
        wait_http_ok(
            &client,
            &format!("{}/health", writer.api),
            Duration::from_mins(2)
        )
        .await,
        "writer api down (log: {})",
        writer.data_dir.join("antd.log").display()
    );
    assert!(
        wait_http_ok(
            &client,
            &format!("{}/health", reader.api),
            Duration::from_mins(2)
        )
        .await,
        "reader api down"
    );
    // Give both a peer warm-up — unless harsh mode says write cold.
    if !no_warmup {
        for api in [&writer.api, &reader.api] {
            let start = Instant::now();
            while start.elapsed() < Duration::from_mins(3) {
                if peer_overlays(&client, api).await.len() >= 20 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    println!(
        "writer peers: {} | reader peers: {}",
        peer_overlays(&client, &writer.api).await.len(),
        peer_overlays(&client, &reader.api).await.len()
    );

    // Shared live peer-overlay snapshot (sampled every 2 s) + sampler.
    let overlays_shared: Arc<Mutex<Vec<[u8; 32]>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let overlays_shared = overlays_shared.clone();
        let sink = sink.clone();
        let client = client.clone();
        let wapi = writer.api.clone();
        let rapi = reader.api.clone();
        tokio::spawn(async move {
            loop {
                let w = peer_overlays(&client, &wapi).await;
                let r = peer_overlays(&client, &rapi).await.len();
                sink.emit(serde_json::json!({
                    "kind": "peers", "writer_peers": w.len(), "reader_peers": r,
                }));
                *overlays_shared.lock().expect("overlays") = w;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // Everything written, for the restart cold re-read:
    // `(soc_path, owner_hex, topic_hex, index)`.
    type WrittenRefs = Arc<Mutex<Vec<(String, String, String, u64)>>>;
    let written: WrittenRefs = Arc::new(Mutex::new(Vec::new()));

    // Cross-node probe queue + prober.
    let (xtx, mut xrx) = tokio::sync::mpsc::unbounded_channel::<XnodeProbe>();
    let xnode_task = {
        let client = client.clone();
        let rapi = reader.api.clone();
        let sink = sink.clone();
        tokio::spawn(async move {
            let mut inflight = tokio::task::JoinSet::new();
            while let Some(p) = xrx.recv().await {
                let client = client.clone();
                let rapi = rapi.clone();
                let sink = sink.clone();
                inflight.spawn(async move {
                    for &t in XNODE_SCHEDULE_SECS {
                        let target = p.written_at + Duration::from_secs(t);
                        if let Some(d) = target.checked_duration_since(Instant::now()) {
                            tokio::time::sleep(d).await;
                        }
                        let status =
                            match client.get(format!("{rapi}{}", p.soc_url_path)).send().await {
                                Ok(r) => {
                                    let s = r.status().as_u16();
                                    let _ = r.bytes().await;
                                    s
                                }
                                Err(_) => 0,
                            };
                        sink.emit(serde_json::json!({
                            "kind": "xnode", "addr": p.addr, "after_secs": t,
                            "status": status,
                        }));
                        if status == 200 {
                            break;
                        }
                    }
                });
                // Bound concurrent probes.
                while inflight.len() > 64 {
                    let _ = inflight.join_next().await;
                }
            }
            while inflight.join_next().await.is_some() {}
        })
    };

    // Workers.
    let mut handles = Vec::new();
    for w in 0..workers {
        let client = client.clone();
        let wapi = writer.api.clone();
        let sink = sink.clone();
        let batch_hex = batch_hex.clone();
        let overlays_shared = overlays_shared.clone();
        let xtx = xtx.clone();
        let written = written.clone();
        handles.push(tokio::spawn(async move {
            let mut prng = Prng(
                (unix_now() ^ 0x5eed_5eed_0000_0000)
                    .wrapping_mul(w as u64 * 2 + 1)
                    .wrapping_add(u64::from(std::process::id()))
                    | 1,
            );
            for feed_i in 0..feeds_per_worker {
                // Fresh identity + topic = fresh keyspace neighbourhoods.
                let (secret, owner) = loop {
                    let mut s = [0u8; 32];
                    prng.fill(&mut s);
                    if let Ok(sk) = k256::ecdsa::SigningKey::from_bytes((&s).into()) {
                        let owner = ant_crypto::ethereum_address_from_public_key(
                            &sk.verifying_key().clone(),
                        );
                        break (s, owner);
                    }
                };
                let owner_hex = hex::encode(owner);
                let mut topic = [0u8; 32];
                prng.fill(&mut topic);
                let topic_hex = hex::encode(topic);

                for idx in 0..updates_per_feed {
                    let mut payload = vec![0u8; payload_bytes];
                    prng.fill(&mut payload);
                    let id = sequence_update_id(&topic, idx);
                    let (cac_addr, cac_wire) = ant_crypto::cac_new(&payload).expect("cac");
                    let mut digest = Vec::with_capacity(64);
                    digest.extend_from_slice(&id);
                    digest.extend_from_slice(&cac_addr);
                    let sig = sign_handshake_data(&secret, &keccak256(&digest)).expect("sign");
                    let mut soc_addr_in = Vec::with_capacity(52);
                    soc_addr_in.extend_from_slice(&id);
                    soc_addr_in.extend_from_slice(&owner);
                    let soc_addr = keccak256(&soc_addr_in);

                    let overlays = overlays_shared.lock().expect("overlays").clone();
                    let outcome = soc_write(
                        &client, &wapi, &sink, &batch_hex, &owner_hex, &id, &sig, &cac_wire,
                        &soc_addr, &overlays, w, feed_i, idx,
                    )
                    .await;
                    if !outcome.ok {
                        sink.emit(serde_json::json!({
                            "kind": "write_gaveup",
                            "worker": w, "feed": feed_i, "index": idx,
                            "attempts": outcome.attempts,
                            "addr": hex::encode(soc_addr),
                        }));
                        continue;
                    }

                    // Read-after-own-write: raw SOC.
                    let soc_path = format!("/soc/{owner_hex}/{}", hex::encode(id));
                    let (ok, first, attempts, ms) =
                        read_until_ok(&client, &format!("{wapi}{soc_path}")).await;
                    sink.emit(serde_json::json!({
                        "kind": "readback_soc",
                        "worker": w, "feed": feed_i, "index": idx,
                        "ok": ok, "first_status": first,
                        "attempts": attempts, "ms_to_ok": ms,
                        "write_attempts": outcome.attempts,
                        "write_first_status": outcome.first_status,
                    }));
                    // Read-after-own-write: feed latest.
                    let (ok, first, attempts, ms) =
                        read_until_ok(&client, &format!("{wapi}/feeds/{owner_hex}/{topic_hex}"))
                            .await;
                    sink.emit(serde_json::json!({
                        "kind": "readback_feed",
                        "worker": w, "feed": feed_i, "index": idx,
                        "ok": ok, "first_status": first,
                        "attempts": attempts, "ms_to_ok": ms,
                    }));

                    written.lock().expect("written").push((
                        soc_path.clone(),
                        owner_hex.clone(),
                        topic_hex.clone(),
                        idx,
                    ));
                    let _ = xtx.send(XnodeProbe {
                        soc_url_path: soc_path,
                        addr: hex::encode(soc_addr),
                        written_at: Instant::now(),
                    });
                }
            }
        }));
    }
    drop(xtx);
    for h in handles {
        let _ = h.await;
    }

    let writer_log = writer.data_dir.join("antd.log");
    if restart_reread {
        // Browser-reload scenario: kill the writer, restart it on the
        // same data dir (disk cache persists; mem cache + peer pool do
        // not), and immediately re-read every SOC + feed head, starting
        // the reads the moment the API answers.
        println!("restart-reread: bouncing the writer node…");
        let data_dir = writer_log.parent().unwrap().to_path_buf();
        writer_slot.take();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let api = format!("127.0.0.1:{port_base}");
        let mut cmd = Command::new(repo_root().join("target/release/antd"));
        cmd.arg("--data-dir")
            .arg(&data_dir)
            .arg("--api-addr")
            .arg(&api)
            .arg("--log-level")
            .arg("warn")
            .stdout(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(data_dir.join("antd.log"))
                    .map_or_else(|_| Stdio::null(), Stdio::from),
            )
            .stderr(Stdio::null());
        if let Some(n) = writer_tp {
            cmd.arg("--target-peers").arg(n.to_string());
        }
        cmd.env_remove("GNOSIS_RPC_URL")
            .env_remove("STORAGE_STAMP_BATCH_ID")
            .env("STORAGE_STAMP_PRIVATE_KEY", &key_hex);
        let restarted = ChildGuard(cmd.spawn().expect("respawn writer"));
        let wapi = format!("http://{api}");
        assert!(
            wait_http_ok(&client, &format!("{wapi}/health"), Duration::from_mins(2)).await,
            "restarted writer api down"
        );
        let refs = written.lock().expect("written").clone();
        println!("restart-reread: {} refs, reading cold…", refs.len());
        let mut seen_feeds = std::collections::HashSet::new();
        for (soc_path, owner_hex, topic_hex, idx) in refs {
            let (ok, first, attempts, ms) =
                read_until_ok(&client, &format!("{wapi}{soc_path}")).await;
            sink.emit(serde_json::json!({
                "kind": "restart_read_soc",
                "index": idx, "ok": ok, "first_status": first,
                "attempts": attempts, "ms_to_ok": ms,
            }));
            if seen_feeds.insert((owner_hex.clone(), topic_hex.clone())) {
                let (ok, first, attempts, ms) =
                    read_until_ok(&client, &format!("{wapi}/feeds/{owner_hex}/{topic_hex}")).await;
                sink.emit(serde_json::json!({
                    "kind": "restart_read_feed",
                    "ok": ok, "first_status": first,
                    "attempts": attempts, "ms_to_ok": ms,
                }));
            }
        }
        drop(restarted);
    }
    // Let the tail of cross-node probes finish (up to the last schedule
    // point past the final write).
    println!("writers done; draining cross-node probes…");
    let _ = tokio::time::timeout(Duration::from_secs(330), xnode_task).await;

    // Copy the writer's warn log next to the events for correlation.
    let _ = std::fs::copy(
        writer_log.clone(),
        results.join(format!(
            "{}.antd.log",
            out.file_name().unwrap().to_string_lossy()
        )),
    );
    println!(
        "soak done: {} events → {}",
        sink.events.load(Ordering::Relaxed),
        out.display()
    );
    drop(reader);
}

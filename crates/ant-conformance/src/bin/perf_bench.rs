//! Mainnet performance benchmark harness (perf-lab).
//!
//! Measures ant's upload / download / feed-resolution / session-warm-up
//! performance against the REAL Swarm mainnet, producing one JSON file
//! per run under `<repo>/perf/results/`. Companion notebook:
//! `<repo>/PERF-LAB.md`.
//!
//! Credentialed like `mainnet_smoke` (reads `<repo>/.env`); costs stamp
//! slots only, never gas. Unlike the smoke it does NOT need
//! `GNOSIS_RPC_URL` at run time: the postage issuer is loaded from the
//! persistent lab postage dir (`perf/state/postage`), which MUST be
//! seeded once via `seed-issuer` before the first upload run (fresh
//! bucket counters on an immutable batch would re-issue already-used
//! `(bucket, index)` slots from earlier throwaway data dirs).
//!
//! Subcommands:
//!
//! ```text
//! perf_bench seed-issuer --merge <a.bin,b.bin,...> [--offset 2]
//! perf_bench status
//! perf_bench warmup    --runs 5 [--peers-target 30] [--timeout-secs 300]
//! perf_bench upload    --size-mib 32 --runs 5 --label baseline
//!                      [--antd-bin path] [--antd-bin-b path --label-b X]
//!                      [--stall-abort-secs 1200] [--peers-min 30]
//! perf_bench download  --refs <ref[,ref...]> --runs 5 --label baseline
//!                      [--kind bytes|bzz]
//! perf_bench feed-setup --counts 1,11,101
//! perf_bench feed      --runs 5 --label baseline
//! ```
//!
//! Methodology guards baked in: fresh random content per upload run
//! (recorded seed), wall-clock window + peer count recorded per run,
//! A/B arms interleaved within one invocation, batch utilization
//! checked before/after every upload run (refuses to start a run at
//! ratio ≥ 0.8, warns at ≥ 0.6 — the goal's stop conditions).

use ant_control::{Request, Response};
use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const UTILIZATION_WARN: f64 = 0.6;
const UTILIZATION_STOP: f64 = 0.8;

/// Default per-run stall-abort: no forward byte progress for this long
/// aborts the upload run (the job manager itself retries forever, so a
/// wedged 512 MiB baseline run would otherwise never terminate).
const DEFAULT_STALL_ABORT_SECS: u64 = 1200;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn lab_state_dir() -> PathBuf {
    repo_root().join("perf/state")
}

fn results_dir() -> PathBuf {
    repo_root().join("perf/results")
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn iso_now() -> String {
    // Coarse ISO-8601 from unix seconds; good enough for run windows.
    let secs = unix_now();
    let days = secs / 86_400;
    let rem = secs % 86_400;
    // civil-from-days (Howard Hinnant's algorithm), valid for our era.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
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

fn git_head() -> String {
    Command::new("git")
        .args(["-C"])
        .arg(repo_root())
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        )
}

/// Deterministic xorshift64* content generator. Fresh seed per run
/// (recorded in the result JSON) so every upload pushes novel chunks —
/// bee dedupes re-uploads, which would make a repeat measure nothing.
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
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

fn write_random_file(path: &Path, bytes: u64, seed: u64) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut prng = Prng(seed | 1);
    let mut block = vec![0u8; 1 << 20];
    let mut left = bytes;
    while left > 0 {
        let n = left.min(block.len() as u64) as usize;
        prng.fill(&mut block[..n]);
        f.write_all(&block[..n])?;
        left -= n as u64;
    }
    f.flush()
}

// ---------------------------------------------------------------------------
// antd process management
// ---------------------------------------------------------------------------

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
    socket: PathBuf,
}

struct AntdSpec {
    bin: PathBuf,
    port: u16,
    /// Symlink the lab postage dir + set the stamp key (upload runs).
    upload_capable: bool,
    stamp_key_hex: Option<String>,
    log_level: &'static str,
}

impl AntdSpec {
    fn spawn(&self, tag: &str) -> Antd {
        let data_dir = std::env::temp_dir().join(format!(
            "ant-perf-{tag}-{}-{}",
            self.port,
            unix_now()
        ));
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        if self.upload_capable {
            let shared = lab_state_dir().join("postage");
            assert!(
                shared.join("").is_dir(),
                "lab postage state missing at {} — run `perf_bench seed-issuer` first",
                shared.display()
            );
            std::os::unix::fs::symlink(&shared, data_dir.join("postage"))
                .expect("symlink postage dir");
        }
        let api = format!("127.0.0.1:{}", self.port);
        let mut cmd = Command::new(&self.bin);
        cmd.arg("--data-dir")
            .arg(&data_dir)
            .arg("--api-addr")
            .arg(&api)
            .arg("--log-level")
            .arg(self.log_level)
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(data_dir.join("antd.log"))
                    .map_or_else(|_| Stdio::null(), Stdio::from),
            );
        // Deliberately NO GNOSIS_RPC_URL / STORAGE_STAMP_BATCH_ID: the
        // issuer comes from the postage-dir scan (no chain reads per
        // run; politer to the public RPC and faster to boot).
        cmd.env_remove("GNOSIS_RPC_URL")
            .env_remove("STORAGE_STAMP_BATCH_ID");
        if let Some(k) = &self.stamp_key_hex {
            cmd.env("STORAGE_STAMP_PRIVATE_KEY", k);
        } else {
            cmd.env_remove("STORAGE_STAMP_PRIVATE_KEY");
        }
        let child = cmd.spawn().expect("spawn antd");
        Antd {
            _guard: ChildGuard(child),
            api: format!("http://{api}"),
            socket: data_dir.join("antd.sock"),
            data_dir,
        }
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

async fn peer_count(client: &reqwest::Client, api: &str) -> usize {
    if let Ok(r) = client.get(format!("{api}/peers")).send().await {
        if let Ok(body) = r.bytes().await {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
                return v["peers"].as_array().map_or(0, Vec::len);
            }
        }
    }
    0
}

/// Wait until the node has ≥ `target` connected peers. Returns
/// `(reached, secs_elapsed, peers_now, milestones)` where `milestones`
/// maps peer-count thresholds to the seconds they were first reached.
async fn wait_peers(
    client: &reqwest::Client,
    api: &str,
    target: usize,
    timeout: Duration,
) -> (bool, f64, usize, Vec<(usize, f64)>) {
    let thresholds = [1usize, 5, 10, 20, 30, 50, 75, 100];
    let mut milestones: Vec<(usize, f64)> = Vec::new();
    let mut hit = 0usize;
    let start = Instant::now();
    let mut peers = 0usize;
    while start.elapsed() < timeout {
        peers = peer_count(client, api).await;
        while hit < thresholds.len() && peers >= thresholds[hit] {
            milestones.push((thresholds[hit], start.elapsed().as_secs_f64()));
            hit += 1;
        }
        if peers >= target {
            return (true, start.elapsed().as_secs_f64(), peers, milestones);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    (false, start.elapsed().as_secs_f64(), peers, milestones)
}

// ---------------------------------------------------------------------------
// control-socket helpers (blocking client hopped through spawn_blocking)
// ---------------------------------------------------------------------------

async fn control<F>(socket: PathBuf, make: F) -> Result<Response, String>
where
    F: FnOnce() -> Request + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        ant_control::request_sync(&socket, &make()).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn postage_status(socket: &Path) -> Option<ant_control::PostageStatusView> {
    match control(socket.to_path_buf(), || Request::PostageStatus).await {
        Ok(Response::PostageStatus(v)) => Some(v),
        _ => None,
    }
}

fn utilization_ratio(v: &ant_control::PostageStatusView) -> f64 {
    if v.bucket_capacity == 0 {
        return 0.0;
    }
    f64::from(v.bucket_fill_max) / f64::from(v.bucket_capacity)
}

// ---------------------------------------------------------------------------
// log histogram
// ---------------------------------------------------------------------------

fn log_histogram(log_path: &Path) -> serde_json::Value {
    let body = std::fs::read_to_string(log_path).unwrap_or_default();
    let mut shallow = 0u64;
    let mut shallow_accepted = 0u64;
    let mut hard_failures = 0u64;
    let mut same_peer_retries = 0u64;
    let mut push_timeouts = 0u64;
    let mut no_peer = 0u64;
    let mut exhausted = 0u64;
    let mut overdraft = 0u64;
    for line in body.lines() {
        if line.contains("shallow receipt; chunk stored but shallow") {
            shallow += 1;
        }
        if line.contains("accepting shallow receipt") {
            shallow_accepted += 1;
        }
        if line.contains("pushsync attempt failed; hedging") {
            hard_failures += 1;
        }
        if line.contains("retrying same peer once") {
            same_peer_retries += 1;
        }
        if line.contains("push timed out") || line.contains("timed out") {
            push_timeouts += 1;
        }
        if line.contains("no peers available") || line.contains("no peer found") {
            no_peer += 1;
        }
        if line.contains("exhausted") {
            exhausted += 1;
        }
        if line.contains("overdraft") || line.contains("Overdraft") {
            overdraft += 1;
        }
    }
    serde_json::json!({
        "shallow_receipts": shallow,
        "shallow_accepted": shallow_accepted,
        "push_hard_failures": hard_failures,
        "same_peer_retries": same_peer_retries,
        "push_timeouts": push_timeouts,
        "no_peer_events": no_peer,
        "exhausted_events": exhausted,
        "overdraft_mentions": overdraft,
    })
}

fn write_result(name: &str, value: &serde_json::Value) -> PathBuf {
    let dir = results_dir();
    std::fs::create_dir_all(&dir).expect("results dir");
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string_pretty(value).unwrap()).expect("write result");
    println!("  result → {}", path.display());
    path
}

// ---------------------------------------------------------------------------
// arg parsing (tiny --key value / --flag parser)
// ---------------------------------------------------------------------------

struct Args(HashMap<String, String>);

impl Args {
    fn parse(argv: &[String]) -> Self {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < argv.len() {
            let a = &argv[i];
            if let Some(key) = a.strip_prefix("--") {
                if i + 1 < argv.len() && !argv[i + 1].starts_with("--") {
                    m.insert(key.to_string(), argv[i + 1].clone());
                    i += 2;
                } else {
                    m.insert(key.to_string(), "true".to_string());
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        Self(m)
    }

    fn get(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(String::as_str)
    }

    fn u64_or(&self, k: &str, default: u64) -> u64 {
        self.get(k).and_then(|v| v.parse().ok()).unwrap_or(default)
    }

    fn str_or<'a>(&'a self, k: &str, default: &'a str) -> &'a str {
        self.get(k).unwrap_or(default)
    }
}

fn default_antd_bin() -> PathBuf {
    repo_root().join("target/release/antd")
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = argv.first().map(String::as_str) else {
        eprintln!(
            "usage: perf_bench <seed-issuer|status|warmup|upload|download|feed-setup|feed> [--flags]"
        );
        std::process::exit(2);
    };
    let args = Args::parse(&argv[1..]);
    match cmd {
        "seed-issuer" => seed_issuer(&args),
        "status" => issuer_status(),
        cmd => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            match cmd {
                "warmup" => rt.block_on(warmup(&args)),
                "upload" => rt.block_on(upload(&args)),
                "download" => rt.block_on(download(&args)),
                "feed-setup" => rt.block_on(feed_setup(&args)),
                "feed" => rt.block_on(feed_bench(&args)),
                other => {
                    eprintln!("unknown subcommand: {other}");
                    std::process::exit(2);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// seed-issuer / status
// ---------------------------------------------------------------------------

/// Build the persistent lab issuer state by merging bucket counters
/// from earlier issuer stores (per-bucket max across all sources) and
/// adding a flat safety `--offset` to EVERY bucket. The offset guards
/// against runs whose issuer state was lost (throwaway temp dirs):
/// those consumed low indices in some unknown subset of buckets, and
/// re-issuing a used `(bucket, index)` on an immutable batch makes
/// storers reject the stamp.
fn seed_issuer(args: &Args) {
    let offset: u32 = args.u64_or("offset", 2) as u32;
    let out_dir = lab_state_dir().join("postage");
    let merge: Vec<PathBuf> = args
        .get("merge")
        .map(|s| s.split(',').map(PathBuf::from).collect())
        .unwrap_or_default();
    assert!(
        !merge.is_empty(),
        "--merge <a.bin,b.bin,...> required (at least one source store for batch params)"
    );

    let mut sources = Vec::new();
    for path in &merge {
        let iss = ant_postage::StampIssuer::open_existing(path.clone())
            .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
        println!(
            "  merge source {}: issued={} fill_max={}",
            path.display(),
            iss.issued_count(),
            iss.stats().bucket_fill_max
        );
        sources.push(iss);
    }
    let first = &sources[0];
    let batch_id = *first.batch_id();
    let batch_depth = first.batch_depth();
    let bucket_depth = first.bucket_depth();
    let immutable = first.immutable();
    for s in &sources[1..] {
        assert_eq!(s.batch_id(), &batch_id, "merge sources are different batches");
        assert_eq!(s.batch_depth(), batch_depth);
        assert_eq!(s.bucket_depth(), bucket_depth);
    }
    let n = 1usize << bucket_depth;
    let cap = 1u32 << u32::from(batch_depth - bucket_depth);
    let mut merged = vec![0u32; n];
    for s in &sources {
        for (i, &c) in s.bucket_counts().iter().enumerate() {
            if c > merged[i] {
                merged[i] = c;
            }
        }
    }
    for c in &mut merged {
        *c = (*c + offset).min(cap);
    }
    let issued: u64 = merged.iter().map(|&c| u64::from(c)).sum();
    let max = merged.iter().copied().max().unwrap_or(0);

    std::fs::create_dir_all(&out_dir).expect("create lab postage dir");
    let out = out_dir.join(format!("{}.bin", hex::encode(batch_id)));
    assert!(
        !out.exists(),
        "{} already exists — refusing to overwrite lab issuer state",
        out.display()
    );
    let iss = ant_postage::StampIssuer::open_or_new_seeded(
        out.clone(),
        batch_id,
        batch_depth,
        bucket_depth,
        immutable,
        &merged,
    )
    .expect("seed lab issuer");
    drop(iss);
    println!(
        "seeded {} (batch depth {batch_depth}, bucket depth {bucket_depth}, immutable {immutable})",
        out.display()
    );
    println!(
        "  seeded counters: issued={issued} fill_max={max}/{cap} ratio={:.4} (flat offset +{offset})",
        f64::from(max) / f64::from(cap)
    );
}

fn issuer_status() {
    let dir = lab_state_dir().join("postage");
    let mut found = false;
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            let path = e.path();
            if path.extension().is_some_and(|x| x == "bin") {
                let iss = ant_postage::StampIssuer::open_existing(path.clone())
                    .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
                let stats = iss.stats();
                let cap = iss.bucket_upper_bound();
                let mean = stats.issued as f64 / stats.bucket_count.max(1) as f64;
                println!(
                    "batch 0x{}: issued={} fill min/mean/max = {}/{mean:.2}/{} of {} → utilization ratio {:.4}, worst-case remaining {} chunks",
                    hex::encode(iss.batch_id()),
                    iss.issued_count(),
                    stats.bucket_fill_min,
                    stats.bucket_fill_max,
                    cap,
                    f64::from(stats.bucket_fill_max) / f64::from(cap),
                    stats.worst_case_remaining,
                );
                found = true;
            }
        }
    }
    if !found {
        println!(
            "no lab issuer state under {} — run `perf_bench seed-issuer`",
            dir.display()
        );
    }
}

// ---------------------------------------------------------------------------
// warmup suite
// ---------------------------------------------------------------------------

async fn warmup(args: &Args) {
    let runs = args.u64_or("runs", 5);
    let target = args.u64_or("peers-target", 30) as usize;
    let timeout = Duration::from_secs(args.u64_or("timeout-secs", 300));
    let label = args.str_or("label", "baseline").to_string();
    let bin = args
        .get("antd-bin")
        .map_or_else(default_antd_bin, PathBuf::from);
    let port_base = args.u64_or("port-base", 3633) as u16;
    let client = reqwest::Client::new();

    for run in 1..=runs {
        println!("warmup run {run}/{runs} …");
        let spec = AntdSpec {
            bin: bin.clone(),
            port: port_base + (run % 8) as u16,
            upload_capable: false,
            stamp_key_hex: None,
            log_level: "warn",
        };
        let spawn_at = Instant::now();
        let antd = spec.spawn("warmup");
        let api_up = wait_http_ok(&client, &format!("{}/health", antd.api), timeout).await;
        let api_secs = spawn_at.elapsed().as_secs_f64();
        let (reached, secs, peers, milestones) =
            wait_peers(&client, &antd.api, target, timeout).await;
        let value = serde_json::json!({
            "suite": "warmup",
            "label": label,
            "run": run,
            "git_commit": git_head(),
            "antd_bin": bin.display().to_string(),
            "started_utc": iso_now(),
            "api_up": api_up,
            "api_up_secs": api_secs,
            "peers_target": target,
            "target_reached": reached,
            "secs_to_target": secs,
            "peers_at_end": peers,
            "milestones": milestones.iter().map(|(n, s)| serde_json::json!({"peers": n, "secs": s})).collect::<Vec<_>>(),
        });
        write_result(
            &format!("{label}-warmup-run{run}-{}.json", unix_now()),
            &value,
        );
        drop(antd);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ---------------------------------------------------------------------------
// upload suite
// ---------------------------------------------------------------------------

struct UploadArm {
    label: String,
    bin: PathBuf,
}

async fn upload(args: &Args) {
    let env = load_env();
    let batch_hex = env
        .get("SWARM_BATCH_ID")
        .expect("SWARM_BATCH_ID missing from .env")
        .trim_start_matches("0x")
        .to_lowercase();
    let key_hex = env
        .get("SWARM_OWNER_PRIVATE_KEY")
        .expect("SWARM_OWNER_PRIVATE_KEY missing from .env")
        .trim_start_matches("0x")
        .to_string();

    let size_mib = args.u64_or("size-mib", 1);
    let runs = args.u64_or("runs", 5);
    let peers_min = args.u64_or("peers-min", 30) as usize;
    let stall_abort = Duration::from_secs(args.u64_or("stall-abort-secs", DEFAULT_STALL_ABORT_SECS));
    let run_timeout = Duration::from_secs(args.u64_or("run-timeout-secs", 6 * 3600));
    let port_base = args.u64_or("port-base", 3633) as u16;

    let mut arms = vec![UploadArm {
        label: args.str_or("label", "baseline").to_string(),
        bin: args
            .get("antd-bin")
            .map_or_else(default_antd_bin, PathBuf::from),
    }];
    if let Some(b) = args.get("antd-bin-b") {
        arms.push(UploadArm {
            label: args.str_or("label-b", "candidate").to_string(),
            bin: PathBuf::from(b),
        });
    }

    let client = reqwest::Client::new();
    for run in 1..=runs {
        for (arm_idx, arm) in arms.iter().enumerate() {
            println!(
                "upload run {run}/{runs} arm '{}' ({} MiB) …",
                arm.label, size_mib
            );
            let outcome = upload_one_run(
                &client,
                arm,
                UploadRunCfg {
                    size_mib,
                    run,
                    peers_min,
                    stall_abort,
                    run_timeout,
                    port: port_base + ((run * arms.len() as u64 + arm_idx as u64) % 8) as u16,
                    batch_hex: &batch_hex,
                    key_hex: &key_hex,
                },
            )
            .await;
            if let Some(ratio) = outcome {
                if ratio >= UTILIZATION_STOP {
                    eprintln!(
                        "STOP: batch utilization ratio {ratio:.3} ≥ {UTILIZATION_STOP} — halting all upload runs (user decision required)"
                    );
                    std::process::exit(3);
                }
                if ratio >= UTILIZATION_WARN {
                    eprintln!(
                        "WARNING: batch utilization ratio {ratio:.3} ≥ {UTILIZATION_WARN} — flag to the user soon"
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

struct UploadRunCfg<'a> {
    size_mib: u64,
    run: u64,
    peers_min: usize,
    stall_abort: Duration,
    run_timeout: Duration,
    port: u16,
    batch_hex: &'a str,
    key_hex: &'a str,
}

/// Runs one upload; returns the post-run utilization ratio (None if
/// unknown) so the caller can enforce the stop conditions.
async fn upload_one_run(
    client: &reqwest::Client,
    arm: &UploadArm,
    cfg: UploadRunCfg<'_>,
) -> Option<f64> {
    let started_utc = iso_now();
    let spec = AntdSpec {
        bin: arm.bin.clone(),
        port: cfg.port,
        upload_capable: true,
        stamp_key_hex: Some(cfg.key_hex.to_string()),
        log_level: "warn",
    };
    let antd = spec.spawn("upload");
    if !wait_http_ok(client, &format!("{}/health", antd.api), Duration::from_mins(2)).await {
        eprintln!("antd api did not come up (log: {})", antd.data_dir.join("antd.log").display());
        return None;
    }

    // Pre-run issuer view (also proves the shared postage state loaded).
    let pre = postage_status(&antd.socket).await;
    let Some(pre) = pre else {
        eprintln!("PostageStatus failed — is the lab postage state seeded + symlinked?");
        return None;
    };
    assert!(pre.enabled, "daemon has no postage issuer — seeding/symlink broken");
    let pre_ratio = utilization_ratio(&pre);
    if pre_ratio >= UTILIZATION_STOP {
        eprintln!("refusing run: utilization {pre_ratio:.3} ≥ {UTILIZATION_STOP}");
        return Some(pre_ratio);
    }
    let chunks_needed = cfg.size_mib * 256 + 64; // data + generous tree/manifest overhead
    if pre.worst_case_remaining_chunks < chunks_needed {
        eprintln!(
            "refusing run: worst-case remaining {} chunks < needed ~{chunks_needed}",
            pre.worst_case_remaining_chunks
        );
        return Some(pre_ratio);
    }

    // Peer warm-up.
    let (warmed, warm_secs, peers_start, _) = wait_peers(
        client,
        &antd.api,
        cfg.peers_min,
        Duration::from_mins(5),
    )
    .await;
    if !warmed {
        eprintln!("peer warm-up failed ({peers_start} peers) — recording aborted run");
    }

    // Fresh random content.
    let seed = unix_now()
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(u64::from(cfg.port));
    let src = antd.data_dir.join(format!("payload-{}mib.bin", cfg.size_mib));
    write_random_file(&src, cfg.size_mib * 1024 * 1024, seed).expect("write payload");

    // Start the job.
    let src_str = src.display().to_string();
    let batch = cfg.batch_hex.to_string();
    let started = Instant::now();
    let job_id = match control(antd.socket.clone(), move || Request::UploadStart {
        source_path: src_str,
        batch_id: Some(batch),
        name: None,
        content_type: None,
        raw: false,
    })
    .await
    {
        Ok(Response::UploadStarted { job_id }) => job_id,
        other => {
            eprintln!("UploadStart failed: {other:?}");
            return None;
        }
    };

    // Poll status until terminal / stalled-out / timed out.
    let mut curve: Vec<serde_json::Value> = Vec::new();
    let mut last_progress = Instant::now();
    let mut last_bytes = 0u64;
    let outcome: String;
    let mut terminal: Option<ant_control::UploadJobView> = None;
    // 1 s polls keep the completion-time error ≤ ~10 % on the small
    // cells; 5 s is plenty above 32 MiB (multi-minute runs).
    let poll = if cfg.size_mib <= 32 {
        Duration::from_secs(1)
    } else {
        Duration::from_secs(5)
    };
    loop {
        tokio::time::sleep(poll).await;
        let jid = job_id.clone();
        let view = match control(antd.socket.clone(), move || Request::UploadStatus {
            job_id: jid,
        })
        .await
        {
            Ok(Response::UploadJob(v)) => v,
            other => {
                eprintln!("UploadStatus failed: {other:?}");
                outcome = "status-error".to_string();
                break;
            }
        };
        let t = started.elapsed().as_secs_f64();
        curve.push(serde_json::json!({
            "t_secs": t,
            "bytes_pushed": view.bytes_pushed,
            "chunks_pushed": view.chunks_pushed,
            "chunks_requeued": view.chunks_requeued,
            "stalled": view.stalled,
            "status": view.status,
        }));
        if view.bytes_pushed > last_bytes {
            last_bytes = view.bytes_pushed;
            last_progress = Instant::now();
        }
        match view.status.as_str() {
            "completed" => {
                outcome = "completed".to_string();
                terminal = Some(view);
                break;
            }
            "failed" | "cancelled" => {
                outcome = view.status.clone();
                terminal = Some(view);
                break;
            }
            _ => {}
        }
        if last_progress.elapsed() > cfg.stall_abort {
            outcome = "stall-aborted".to_string();
            terminal = Some(view);
            let jid = job_id.clone();
            let _ = control(antd.socket.clone(), move || Request::UploadCancel {
                job_id: jid,
            })
            .await;
            break;
        }
        if started.elapsed() > cfg.run_timeout {
            outcome = "run-timeout".to_string();
            terminal = Some(view);
            let jid = job_id.clone();
            let _ = control(antd.socket.clone(), move || Request::UploadCancel {
                job_id: jid,
            })
            .await;
            break;
        }
    }
    let duration = started.elapsed().as_secs_f64();
    let peers_end = peer_count(client, &antd.api).await;
    let post = postage_status(&antd.socket).await;

    let bytes_total = cfg.size_mib * 1024 * 1024;
    let bytes_done = terminal.as_ref().map_or(0, |v| v.bytes_pushed);
    let throughput_kibs = if outcome == "completed" && duration > 0.0 {
        (bytes_total as f64 / 1024.0) / duration
    } else if duration > 0.0 {
        (bytes_done as f64 / 1024.0) / duration
    } else {
        0.0
    };

    let hist = log_histogram(&antd.data_dir.join("antd.log"));
    let post_ratio = post.as_ref().map(utilization_ratio);
    let value = serde_json::json!({
        "suite": "upload",
        "label": arm.label,
        "size_mib": cfg.size_mib,
        "run": cfg.run,
        "git_commit": git_head(),
        "antd_bin": arm.bin.display().to_string(),
        "started_utc": started_utc,
        "ended_utc": iso_now(),
        "content_seed": seed,
        "peers_min": cfg.peers_min,
        "peer_warmup_secs": warm_secs,
        "peers_at_start": peers_start,
        "peers_at_end": peers_end,
        "outcome": outcome,
        "duration_secs": duration,
        "job_reported_secs": terminal
            .as_ref()
            .map(|v| v.last_update_unix.saturating_sub(v.created_at_unix)),
        "throughput_kib_s": throughput_kibs,
        "bytes_pushed": bytes_done,
        "chunks_pushed": terminal.as_ref().map_or(0, |v| v.chunks_pushed),
        "chunks_total": terminal.as_ref().and_then(|v| v.chunks_total),
        "chunks_requeued": terminal.as_ref().map_or(0, |v| v.chunks_requeued),
        "last_error": terminal.as_ref().and_then(|v| v.last_error.clone()),
        "reference": terminal.as_ref().and_then(|v| v.reference.clone()),
        "issued_chunks_before": pre.issued_chunks,
        "issued_chunks_after": post.as_ref().map(|v| v.issued_chunks),
        "utilization_ratio_before": pre_ratio,
        "utilization_ratio_after": post_ratio,
        "worst_case_remaining_after": post.as_ref().map(|v| v.worst_case_remaining_chunks),
        "log_histogram": hist,
        "progress_curve": curve,
    });
    let name = format!(
        "{}-upload-{}mib-run{}-{}.json",
        arm.label,
        cfg.size_mib,
        cfg.run,
        unix_now()
    );
    write_result(&name, &value);
    // Keep the antd log next to the result for forensics (gitignored).
    let _ = std::fs::copy(
        antd.data_dir.join("antd.log"),
        results_dir().join(format!("{name}.antd.log")),
    );
    println!(
        "  {} — {:.1} KiB/s over {:.0}s ({} → outcome {outcome})",
        arm.label, throughput_kibs, duration, cfg.size_mib
    );
    post_ratio.or(Some(pre_ratio))
}

// ---------------------------------------------------------------------------
// download suite
// ---------------------------------------------------------------------------

async fn download(args: &Args) {
    let refs: Vec<String> = args
        .get("refs")
        .map(|s| s.split(',').map(str::to_string).collect())
        .or_else(|| {
            args.get("refs-file").map(|f| {
                std::fs::read_to_string(f)
                    .expect("read refs file")
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .map(str::to_string)
                    .collect()
            })
        })
        .expect("--refs or --refs-file required");
    let kind = args.str_or("kind", "bytes").to_string();
    let runs = args.u64_or("runs", 5);
    let peers_min = args.u64_or("peers-min", 30) as usize;
    let label = args.str_or("label", "baseline").to_string();
    let bin = args
        .get("antd-bin")
        .map_or_else(default_antd_bin, PathBuf::from);
    let port_base = args.u64_or("port-base", 3633) as u16;
    let client = reqwest::Client::new();

    for run in 1..=runs {
        println!("download run {run}/{runs} ({} refs) …", refs.len());
        let spec = AntdSpec {
            bin: bin.clone(),
            port: port_base + (run % 8) as u16,
            upload_capable: false,
            stamp_key_hex: None,
            log_level: "warn",
        };
        let antd = spec.spawn("download");
        if !wait_http_ok(&client, &format!("{}/health", antd.api), Duration::from_mins(2)).await {
            eprintln!("antd api did not come up");
            continue;
        }
        let (_, warm_secs, peers_start, _) =
            wait_peers(&client, &antd.api, peers_min, Duration::from_mins(5)).await;

        let mut fetches = Vec::new();
        for r in &refs {
            // Cold fetch (fresh daemon, nothing cached).
            let cold = fetch_timed(&client, &antd.api, &kind, r).await;
            // Warm fetch (same daemon; disk/mem cache + warm peer set).
            let warm = fetch_timed(&client, &antd.api, &kind, r).await;
            fetches.push(serde_json::json!({
                "reference": r,
                "cold": cold,
                "warm": warm,
            }));
        }
        let value = serde_json::json!({
            "suite": "download",
            "label": label,
            "kind": kind,
            "run": run,
            "git_commit": git_head(),
            "antd_bin": bin.display().to_string(),
            "started_utc": iso_now(),
            "peer_warmup_secs": warm_secs,
            "peers_at_start": peers_start,
            "fetches": fetches,
        });
        write_result(
            &format!("{label}-download-run{run}-{}.json", unix_now()),
            &value,
        );
        drop(antd);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn fetch_timed(
    client: &reqwest::Client,
    api: &str,
    kind: &str,
    reference: &str,
) -> serde_json::Value {
    // For `bzz` the reference is used verbatim so callers can pass
    // `<ref>/`, `<ref>/path/inside.bin`, or a percent-encoded path.
    let url = match kind {
        "bzz" => format!("{api}/bzz/{reference}"),
        _ => format!("{api}/bytes/{reference}"),
    };
    let start = Instant::now();
    match client.get(&url).send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let mut resp = resp;
            let mut ttfb: Option<f64> = None;
            let mut total: u64 = 0;
            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        if ttfb.is_none() {
                            ttfb = Some(start.elapsed().as_secs_f64());
                        }
                        total += chunk.len() as u64;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return serde_json::json!({
                            "ok": false, "status": status, "error": e.to_string(),
                            "bytes": total, "secs": start.elapsed().as_secs_f64(),
                        });
                    }
                }
            }
            let secs = start.elapsed().as_secs_f64();
            serde_json::json!({
                "ok": status == 200,
                "status": status,
                "ttfb_secs": ttfb,
                "secs": secs,
                "bytes": total,
                "throughput_kib_s": if secs > 0.0 { (total as f64 / 1024.0) / secs } else { 0.0 },
            })
        }
        Err(e) => serde_json::json!({"ok": false, "error": e.to_string()}),
    }
}

// ---------------------------------------------------------------------------
// feed suites
// ---------------------------------------------------------------------------

/// One-time setup: create sequence feeds with N updates each (default
/// counts 1, 11, 101 → head indices 0, 10, 100) so `feed` can measure
/// head-resolution latency at different depths. Topics + owner go to
/// `perf/state/feeds.json` (committed to the notebook, content-address
/// public anyway).
async fn feed_setup(args: &Args) {
    let env = load_env();
    let batch_hex = env
        .get("SWARM_BATCH_ID")
        .expect("SWARM_BATCH_ID missing")
        .trim_start_matches("0x")
        .to_lowercase();
    let key_hex = env
        .get("SWARM_OWNER_PRIVATE_KEY")
        .expect("SWARM_OWNER_PRIVATE_KEY missing")
        .trim_start_matches("0x")
        .to_string();
    let secret: [u8; 32] = hex::decode(&key_hex)
        .expect("key hex")
        .try_into()
        .expect("key len");
    let owner = {
        use k256::ecdsa::SigningKey;
        let sk = SigningKey::from_bytes((&secret).into()).expect("key scalar");
        ant_crypto::ethereum_address_from_public_key(&sk.verifying_key().clone())
    };
    let owner_hex = hex::encode(owner);
    let counts: Vec<u64> = args
        .str_or("counts", "1,11,101")
        .split(',')
        .map(|s| s.parse().expect("count"))
        .collect();
    let port = args.u64_or("port-base", 3633) as u16;
    let bin = args
        .get("antd-bin")
        .map_or_else(default_antd_bin, PathBuf::from);
    let client = reqwest::Client::new();

    let spec = AntdSpec {
        bin,
        port,
        upload_capable: true,
        stamp_key_hex: Some(key_hex.clone()),
        log_level: "warn",
    };
    let antd = spec.spawn("feedsetup");
    assert!(
        wait_http_ok(&client, &format!("{}/health", antd.api), Duration::from_mins(2)).await,
        "antd api did not come up"
    );
    let (warmed, _, peers, _) =
        wait_peers(&client, &antd.api, 30, Duration::from_mins(5)).await;
    assert!(warmed, "peer warm-up failed ({peers} peers)");

    let run_nonce = unix_now();
    let mut topics = Vec::new();
    for count in &counts {
        let topic = ant_crypto::keccak256(
            format!("ant-perf-lab/feed-{count}/{run_nonce}").as_bytes(),
        );
        let topic_hex = hex::encode(topic);
        println!("creating feed {topic_hex} with {count} updates …");
        for idx in 0..*count {
            let id = ant_retrieval::feed::sequence_update_id(&topic, idx);
            let payload = format!("perf-lab feed {topic_hex} update {idx} @{run_nonce}");
            let (cac_addr, cac_wire) = ant_crypto::cac_new(payload.as_bytes()).expect("cac");
            let mut digest_input = Vec::with_capacity(64);
            digest_input.extend_from_slice(&id);
            digest_input.extend_from_slice(&cac_addr);
            let sig = ant_crypto::sign_handshake_data(
                &secret,
                &ant_crypto::keccak256(&digest_input),
            )
            .expect("sign");
            let r = client
                .post(format!(
                    "{}/soc/{owner_hex}/{}?sig={}",
                    antd.api,
                    hex::encode(id),
                    hex::encode(sig)
                ))
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", &batch_hex)
                .body(cac_wire)
                .send()
                .await
                .expect("post soc");
            assert!(
                r.status() == 201,
                "feed update {idx} failed: {}",
                r.status()
            );
        }
        topics.push(serde_json::json!({
            "topic": topic_hex,
            "updates": count,
            "head_index": count - 1,
        }));
    }
    let out = serde_json::json!({
        "owner": owner_hex,
        "created_unix": run_nonce,
        "created_utc": iso_now(),
        "topics": topics,
    });
    let path = lab_state_dir().join("feeds.json");
    std::fs::create_dir_all(lab_state_dir()).unwrap();
    std::fs::write(&path, serde_json::to_string_pretty(&out).unwrap()).unwrap();
    println!("feed topics → {}", path.display());
}

async fn feed_bench(args: &Args) {
    let feeds: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            args.str_or(
                "topics-file",
                lab_state_dir().join("feeds.json").to_str().unwrap(),
            ),
        )
        .expect("read feeds.json — run feed-setup first"),
    )
    .expect("parse feeds.json");
    let owner = feeds["owner"].as_str().expect("owner").to_string();
    let runs = args.u64_or("runs", 5);
    let peers_min = args.u64_or("peers-min", 30) as usize;
    let label = args.str_or("label", "baseline").to_string();
    let bin = args
        .get("antd-bin")
        .map_or_else(default_antd_bin, PathBuf::from);
    let port_base = args.u64_or("port-base", 3633) as u16;
    let client = reqwest::Client::new();

    for run in 1..=runs {
        println!("feed run {run}/{runs} …");
        let spec = AntdSpec {
            bin: bin.clone(),
            port: port_base + (run % 8) as u16,
            upload_capable: false,
            stamp_key_hex: None,
            log_level: "warn",
        };
        let antd = spec.spawn("feed");
        if !wait_http_ok(&client, &format!("{}/health", antd.api), Duration::from_mins(2)).await {
            eprintln!("antd api did not come up");
            continue;
        }
        let (_, warm_secs, peers_start, _) =
            wait_peers(&client, &antd.api, peers_min, Duration::from_mins(5)).await;

        let mut resolutions = Vec::new();
        for t in feeds["topics"].as_array().expect("topics") {
            let topic = t["topic"].as_str().unwrap();
            let head = t["head_index"].as_u64().unwrap();
            let url = format!("{}/feeds/{owner}/{topic}", antd.api);
            let mut timings = Vec::new();
            // First resolution on this daemon = cold pool; two more = warm.
            for attempt in 0..3 {
                let start = Instant::now();
                let r = client.get(&url).send().await;
                let (ok, status, idx) = match r {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let idx = resp
                            .headers()
                            .get("swarm-feed-index")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        let _ = resp.bytes().await;
                        (status == 200, status, idx)
                    }
                    Err(_) => (false, 0, None),
                };
                timings.push(serde_json::json!({
                    "attempt": attempt,
                    "cold": attempt == 0,
                    "ok": ok,
                    "status": status,
                    "resolved_index": idx,
                    "secs": start.elapsed().as_secs_f64(),
                }));
            }
            resolutions.push(serde_json::json!({
                "topic": topic,
                "head_index": head,
                "timings": timings,
            }));
        }
        let value = serde_json::json!({
            "suite": "feed",
            "label": label,
            "run": run,
            "git_commit": git_head(),
            "antd_bin": bin.display().to_string(),
            "started_utc": iso_now(),
            "peer_warmup_secs": warm_secs,
            "peers_at_start": peers_start,
            "resolutions": resolutions,
        });
        write_result(
            &format!("{label}-feed-run{run}-{}.json", unix_now()),
            &value,
        );
        drop(antd);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

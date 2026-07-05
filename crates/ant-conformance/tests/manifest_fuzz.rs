//! Property-based differential fuzz for the mantaray manifest writer.
//!
//! Generates hundreds of deterministic pseudo-random file collections
//! (seeded splitmix64 — failures always reproduce), uploads each as a
//! tar to BOTH backends (ant's production gateway on a `MemNode`, and
//! bee's real API layer via `conformance/beemock`), and requires the
//! returned manifest references to be byte-identical. Because manifest
//! references are content addresses, equality proves ant emitted bee's
//! exact node bytes: fork type bits, split/chain structure, metadata
//! JSON, index-document anchors — everything.
//!
//! Stress dimensions (deliberate, not incidental):
//!  - shared prefixes at every depth → fork splits, edge intermediates
//!  - insertion order (the tar order IS the mantaray insertion order;
//!    bee recomputes the path-separator flag on every descend, so
//!    permutations of one file set may legitimately hash differently —
//!    ant must agree PER ORDER, which the shuffled generator covers)
//!  - segment lengths 1, 29, 30, 31, 60+ (chained 30-byte forks)
//!  - a file that is a strict prefix of another path ("a" + "a/b")
//!  - unicode / multibyte names, JSON-escaping hazards ("&", "<", ">")
//!  - 40–60 files in a single directory
//!  - index-document present/absent, files with no extension
//!
//! On top of the reference equality, a few subpath GETs (and the bare
//! `/bzz/<ref>/` root) are spot-checked per case: status, content-type,
//! content-disposition, etag and body must all match.
//!
//! Requires the beemock binary (skips with a notice when absent, like
//! tests/differential.rs). Case count can be raised for a deeper sweep
//! with `ANT_MANIFEST_FUZZ_CASES=2000` (default 300).

use serde_json::Value;
use std::collections::BTreeSet;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// --- beemock child process (same contract as tests/differential.rs) ---------

struct Beemock {
    child: Child,
    base: String,
    batch_id: String,
}

impl Drop for Beemock {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn beemock_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("BEEMOCK_BIN") {
        let p = PathBuf::from(p);
        return p.exists().then_some(p);
    }
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/beemock/beemock");
    p.exists().then_some(p)
}

#[derive(serde::Deserialize)]
struct BeemockHello {
    listening: String,
    #[serde(rename = "batchId")]
    batch_id: String,
}

async fn spawn_beemock(bin: &PathBuf) -> Beemock {
    // Offset by 3 vs the differential test's port so parallel test
    // binaries in one `cargo test -p ant-conformance` run don't race.
    let port = 20000 + ((std::process::id() + 3) % 10000) as u16;
    let mut child = Command::new(bin)
        .arg("-addr")
        .arg(format!("127.0.0.1:{port}"))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn beemock");
    let stdout = child.stdout.take().expect("beemock stdout");
    let hello_line = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufReader::new(stdout).read_line(&mut line).ok();
        line
    })
    .await
    .expect("join stdout reader");
    let hello: BeemockHello =
        serde_json::from_str(hello_line.trim()).expect("parse beemock hello line");
    let base = format!("http://{}", hello.listening);

    let client = reqwest::Client::new();
    for _ in 0..100 {
        if let Ok(r) = client.get(format!("{base}/health")).send().await {
            if r.status().is_success() {
                return Beemock {
                    child,
                    base,
                    batch_id: hello.batch_id,
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("beemock did not become healthy at {base}");
}

// --- deterministic RNG (splitmix64) ------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
    fn chance(&mut self, num: u32, den: u32) -> bool {
        (self.next_u64() % u64::from(den)) < u64::from(num)
    }
    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }
}

// --- tar builder --------------------------------------------------------------

fn tar_entry(name: &str, content: &[u8]) -> Vec<u8> {
    assert!(
        name.len() <= 100,
        "tar name field is 100 bytes; got {} for {name:?}",
        name.len()
    );
    let mut header = vec![0u8; 512];
    header[..name.len()].copy_from_slice(name.as_bytes());
    header[100..107].copy_from_slice(b"0000644");
    header[108..115].copy_from_slice(b"0000000");
    header[116..123].copy_from_slice(b"0000000");
    let size = format!("{:011o}", content.len());
    header[124..135].copy_from_slice(size.as_bytes());
    header[136..147].copy_from_slice(b"00000000000");
    header[156] = b'0';
    header[148..156].copy_from_slice(b"        ");
    let sum: u32 = header.iter().map(|&b| u32::from(b)).sum();
    let chk = format!("{sum:06o}\0 ");
    header[148..156].copy_from_slice(chk.as_bytes());

    let mut out = header;
    out.extend_from_slice(content);
    let pad = (512 - content.len() % 512) % 512;
    out.extend(std::iter::repeat_n(0u8, pad));
    out
}

fn build_tar(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, content) in entries {
        out.extend(tar_entry(name, content));
    }
    out.extend(std::iter::repeat_n(0u8, 1024));
    out
}

// --- case model ----------------------------------------------------------------

#[derive(Clone, Debug)]
struct FuzzCase {
    /// Stable identifier for failure reports ("case 0017" or a
    /// hand-pinned regression name).
    label: String,
    /// (path, body) in tar order — order is semantically significant.
    files: Vec<(String, Vec<u8>)>,
    /// `swarm-index-document` header value, if sent.
    index_document: Option<String>,
}

/// Minimal percent-encoding for request paths: keep unreserved ASCII and
/// `/`, encode everything else byte-wise. Both backends receive the
/// exact same escaped URL, so encoding choices cannot skew the diff.
fn encode_path(path: &str) -> String {
    let mut out = String::new();
    for &b in path.as_bytes() {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/');
        if keep {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            write!(out, "%{b:02X}").expect("writing to String");
        }
    }
    out
}

// --- backend I/O -----------------------------------------------------------------

struct UploadOutcome {
    status: u16,
    reference: Option<String>,
    body: String,
}

async fn upload_tar(
    client: &reqwest::Client,
    base: &str,
    batch: &str,
    case: &FuzzCase,
) -> UploadOutcome {
    let mut req = client
        .post(format!("{base}/bzz"))
        .header("content-type", "application/x-tar")
        .header("swarm-postage-batch-id", batch)
        .header("swarm-collection", "true");
    if let Some(idx) = &case.index_document {
        req = req.header("swarm-index-document", idx);
    }
    let resp = req
        .body(build_tar(&case.files))
        .send()
        .await
        .expect("tar upload request");
    let status = resp.status().as_u16();
    let body = resp.bytes().await.expect("upload response body");
    let reference = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("reference").and_then(Value::as_str).map(String::from));
    UploadOutcome {
        status,
        reference,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

/// Normalized observation of one GET, comparable across backends.
#[derive(PartialEq, Eq, Debug)]
struct GetObs {
    status: u16,
    content_type: Option<String>,
    content_disposition: Option<String>,
    etag: Option<String>,
    /// `location` with the backend base URL stripped (redirects embed
    /// the host:port, which legitimately differs).
    location: Option<String>,
    body: Vec<u8>,
}

async fn get_path(client: &reqwest::Client, base: &str, reference: &str, path: &str) -> GetObs {
    let url = format!("{base}/bzz/{reference}/{}", encode_path(path));
    let resp = client.get(&url).send().await.expect("GET request");
    let status = resp.status().as_u16();
    let hdr = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    };
    let content_type = hdr("content-type");
    let content_disposition = hdr("content-disposition");
    let etag = hdr("etag");
    let location = hdr("location").map(|l| l.strip_prefix(base).unwrap_or(&l).to_string());
    let mut body = resp.bytes().await.expect("GET body").to_vec();
    // JSON bodies compare *semantically*, like tests/differential.rs:
    // bee's jsonhttp encoder appends two newlines to every JSON
    // response (json.Encoder + Fprintln) — whitespace, not API shape.
    if content_type
        .as_deref()
        .is_some_and(|ct| ct.contains("json"))
    {
        if let Ok(v) = serde_json::from_slice::<Value>(&body) {
            body = v.to_string().into_bytes();
        }
    }
    GetObs {
        status,
        content_type,
        content_disposition,
        etag,
        location,
        body,
    }
}

// --- generator --------------------------------------------------------------------

fn dir_pool() -> Vec<String> {
    vec![
        "a".into(),
        "b".into(),
        "ab".into(),
        "sub".into(),
        "s".into(),
        "assets".into(),
        "日記".into(),
        "d".repeat(29),
        "d".repeat(30),
        "d".repeat(31),
        "d".repeat(60),
        format!("{}x", "d".repeat(29)),
        format!("{}0", "commoncommoncommon"),
        format!("{}1", "commoncommoncommon"),
    ]
}

fn stem_pool() -> Vec<String> {
    vec![
        "a".into(),
        "x".into(),
        "file".into(),
        "file2".into(),
        "index".into(),
        "README".into(),
        "日本語ファイル".into(),
        "a&b<c>d".into(),
        "f".repeat(29),
        "f".repeat(30),
        "f".repeat(31),
        "n".repeat(64),
    ]
}

/// Extensions whose content type ant's static table and Go's
/// `mime.TypeByExtension` (as beemock sees it on a stock Linux host)
/// agree on, plus "" and an unknown extension (bee stores an empty
/// Content-Type for those).
const EXT_POOL: &[&str] = &[
    "",
    ".txt",
    ".html",
    ".css",
    ".js",
    ".json",
    ".png",
    ".pdf",
    ".svg",
    ".unknownext",
    ".bin",
    ".csv",
    ".ico",
    ".xml",
];

fn gen_case(case_idx: usize, seed: u64) -> FuzzCase {
    let mut rng = Rng::new(seed ^ (case_idx as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let dirs = dir_pool();
    let stems = stem_pool();

    // Every ~17th case: one big flat directory (40–60 files).
    let big_flat = case_idx % 17 == 3;
    let n_files = if big_flat {
        40 + rng.below(21)
    } else {
        1 + rng.below(13)
    };

    let mut paths: BTreeSet<String> = BTreeSet::new();
    let flat_dir = dirs[rng.below(dirs.len())].clone();
    while paths.len() < n_files {
        let stem = &stems[rng.below(stems.len())];
        let ext = EXT_POOL[rng.below(EXT_POOL.len())];
        let mut name = format!("{stem}{ext}");
        // Occasionally uniquify so big directories can actually fill.
        if rng.chance(1, 2) {
            name = format!("{stem}-{}{ext}", rng.below(100));
        }
        let path = if big_flat {
            format!("{flat_dir}/{name}")
        } else {
            let depth = rng.below(4);
            let mut segs: Vec<String> = (0..depth)
                .map(|_| dirs[rng.below(dirs.len())].clone())
                .collect();
            segs.push(name);
            segs.join("/")
        };
        if path.len() <= 96 {
            paths.insert(path);
        }
    }

    // Strict-prefix stress: with probability 1/4, add a FILE at the
    // directory prefix of an existing nested path ("sub/x" ⇒ file "sub").
    if !big_flat && rng.chance(1, 4) {
        let nested: Vec<String> = paths.iter().filter(|p| p.contains('/')).cloned().collect();
        if !nested.is_empty() {
            let victim = &nested[rng.below(nested.len())];
            let prefix = victim.rsplit_once('/').expect("contains slash").0;
            if !prefix.is_empty() {
                paths.insert(prefix.to_string());
            }
        }
    }

    let mut files: Vec<(String, Vec<u8>)> = paths
        .into_iter()
        .map(|p| {
            let body_len = 1 + rng.below(8);
            let body: Vec<u8> = (0..body_len).map(|_| rng.next_u64() as u8).collect();
            (p, body)
        })
        .collect();
    // Tar order == mantaray insertion order: shuffle so the whole
    // permutation space gets sampled across cases.
    rng.shuffle(&mut files);

    let index_document = match rng.below(4) {
        0 => None,
        1 => Some("index.html".to_string()),
        2 => {
            // Point at a real top-level file when one exists.
            files
                .iter()
                .map(|(p, _)| p)
                .find(|p| !p.contains('/'))
                .cloned()
                .or(Some("index.html".to_string()))
        }
        _ => Some("no-such-file.html".to_string()),
    };

    FuzzCase {
        label: format!("case-{case_idx:04}"),
        files,
        index_document,
    }
}

/// Hand-pinned regression cases: the minimized repros for every
/// divergence class this fuzzer has caught, plus the deliberate corner
/// shapes from the stress checklist. These run before the random sweep
/// so a regression fails loudly and instantly.
fn pinned_cases() -> Vec<FuzzCase> {
    let f = |p: &str| (p.to_string(), format!("body:{p}").into_bytes());
    let case = |label: &str, files: &[(String, Vec<u8>)], idx: Option<&str>| FuzzCase {
        label: label.to_string(),
        files: files.to_vec(),
        index_document: idx.map(str::to_string),
    };
    vec![
        // Fork split ⇒ edge-type intermediate node ("a.txt"/"ab.txt"
        // share prefix "a"): bee marshals the child's full nodeType —
        // edge bit set, value bit clear — on the fork type byte.
        case("split-edge-intermediate", &[f("a.txt"), f("ab.txt")], None),
        // Path-separator flag on a split intermediate is recomputed
        // from the *incoming* path, making the bytes insertion-order
        // dependent: "ab" then "a/c" flags the "a" intermediate; the
        // reverse order does not. Ant must agree with bee PER ORDER.
        case("split-pathsep-order-1", &[f("ab"), f("a/c")], None),
        case("split-pathsep-order-2", &[f("a/c"), f("ab")], None),
        // A file that is a strict prefix of another path, both orders.
        case("file-prefix-of-dir-1", &[f("a"), f("a/b")], None),
        case("file-prefix-of-dir-2", &[f("a/b"), f("a")], None),
        // Value+edge node: strict-prefix file of a sibling ("ab" is a
        // prefix of "abc" at the same trie level).
        case("value-edge-node", &[f("abc"), f("ab")], None),
        // 30-byte fork chaining: segments of length 29/30/31/60 and a
        // sibling that splits a chained head mid-prefix.
        case(
            "chained-forks",
            &[
                f(&"x".repeat(29)),
                f(&"x".repeat(30)),
                f(&"x".repeat(31)),
                f(&"x".repeat(60)),
                f(&format!("{}y", "x".repeat(35))),
            ],
            None,
        ),
        // No extension → bee stores an EMPTY Content-Type in metadata.
        case("no-extension", &[f("README")], None),
        // Go's json.Marshal HTML-escapes <, >, & in metadata.
        case("json-escape-metadata", &[f("a&b<c>d.txt")], None),
        // Unicode / multibyte segment names.
        case(
            "unicode-names",
            &[f("日記/日本語ファイル.txt"), f("日記/x.png")],
            None,
        ),
        // index.html present but NO swarm-index-document header: bee
        // adds no index anchor at the HTTP API level.
        case(
            "index-file-without-header",
            &[f("index.html"), f("style.css")],
            None,
        ),
        // Index header naming a file that is absent from the tar.
        case(
            "index-header-missing-file",
            &[f("only.txt")],
            Some("index.html"),
        ),
        // Duplicate path in one tar: bee's mantaray Add overwrites
        // (last write wins) and the upload succeeds.
        case(
            "duplicate-path",
            &[
                ("dup.txt".to_string(), b"first".to_vec()),
                ("dup.txt".to_string(), b"second".to_vec()),
            ],
            None,
        ),
    ]
}

// --- differential execution ---------------------------------------------------------

struct Failure {
    label: String,
    detail: String,
}

#[derive(Clone)]
struct Backends {
    client: reqwest::Client,
    ant_base: String,
    bee_base: String,
    batch: String,
}

impl Backends {
    /// Upload to both; `Some((ant_ref, bee_ref))` when both accepted.
    /// Err(detail) when statuses/errors diverge.
    async fn upload_both(&self, case: &FuzzCase) -> Result<Option<(String, String)>, String> {
        let ant = upload_tar(&self.client, &self.ant_base, &self.batch, case).await;
        let bee = upload_tar(&self.client, &self.bee_base, &self.batch, case).await;
        if ant.status != bee.status {
            return Err(format!(
                "upload status: ant={} bee={} (ant body: {}; bee body: {})",
                ant.status, bee.status, ant.body, bee.body
            ));
        }
        match (ant.reference, bee.reference) {
            (Some(a), Some(b)) => Ok(Some((a, b))),
            _ => Ok(None), // matching error statuses: agreed rejection
        }
    }

    /// Full comparison for one case; returns divergence details.
    async fn run_case(&self, case: &FuzzCase) -> Vec<String> {
        let mut diffs = Vec::new();
        let refs = match self.upload_both(case).await {
            Ok(r) => r,
            Err(d) => return vec![d],
        };
        let Some((ant_ref, bee_ref)) = refs else {
            return diffs;
        };
        if ant_ref != bee_ref {
            diffs.push(format!("root reference: ant={ant_ref} bee={bee_ref}"));
            // Header/body spot-checks against differing manifests only
            // produce noise; the root diff is the actionable signal.
            return diffs;
        }

        // Spot-check GETs: bare root + up to 3 file paths + one miss +
        // one directory redirect. Deterministic choice (first N paths
        // sorted) keeps reruns identical.
        let mut checks: Vec<String> = vec![String::new()];
        let mut sorted: Vec<&String> = case.files.iter().map(|(p, _)| p).collect();
        sorted.sort();
        sorted.dedup();
        checks.extend(sorted.iter().take(3).map(|p| (*p).clone()));
        checks.push("definitely-missing-path.txt".to_string());
        if let Some(nested) = sorted.iter().find(|p| p.contains('/')) {
            let dir = nested.rsplit_once('/').expect("nested").0;
            checks.push(dir.to_string()); // directory without trailing slash
        }
        for path in checks {
            let ant_obs = get_path(&self.client, &self.ant_base, &ant_ref, &path).await;
            let bee_obs = get_path(&self.client, &self.bee_base, &bee_ref, &path).await;
            if ant_obs != bee_obs {
                diffs.push(format!(
                    "GET {path:?}: ant={ant_obs:?} bee={bee_obs:?}",
                    ant_obs = summarize(&ant_obs),
                    bee_obs = summarize(&bee_obs)
                ));
            }
        }
        diffs
    }

    /// Greedy minimization: drop files while the root refs still
    /// diverge (or the statuses still diverge). Only called on failing
    /// cases, so the extra uploads are bounded by real bugs found.
    async fn shrink(&self, case: &FuzzCase) -> FuzzCase {
        let mut current = case.clone();
        loop {
            let mut shrunk = false;
            let mut i = 0;
            while i < current.files.len() {
                if current.files.len() == 1 {
                    break;
                }
                let mut candidate = current.clone();
                candidate.files.remove(i);
                let still_failing = match self.upload_both(&candidate).await {
                    Err(_) => true,
                    Ok(Some((a, b))) => a != b,
                    Ok(None) => false,
                };
                if still_failing {
                    current = candidate;
                    shrunk = true;
                } else {
                    i += 1;
                }
            }
            // Try dropping the index-document header too.
            if current.index_document.is_some() {
                let mut candidate = current.clone();
                candidate.index_document = None;
                let still_failing = match self.upload_both(&candidate).await {
                    Err(_) => true,
                    Ok(Some((a, b))) => a != b,
                    Ok(None) => false,
                };
                if still_failing {
                    current = candidate;
                    shrunk = true;
                }
            }
            if !shrunk {
                return current;
            }
        }
    }
}

fn summarize(o: &GetObs) -> String {
    format!(
        "status={} ct={:?} cd={:?} etag={:?} loc={:?} body={}",
        o.status,
        o.content_type,
        o.content_disposition,
        o.etag,
        o.location,
        if o.body.len() > 64 {
            format!("<{} bytes> {}…", o.body.len(), hex::encode(&o.body[..32]))
        } else {
            String::from_utf8_lossy(&o.body).into_owned()
        }
    )
}

// --- main test -------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manifest_fuzz_ant_vs_bee() {
    let Some(bin) = beemock_bin() else {
        eprintln!(
            "SKIP manifest_fuzz_ant_vs_bee: beemock binary not found; \
             build it with `cd conformance/beemock && go build -o beemock .` \
             or set BEEMOCK_BIN"
        );
        return;
    };

    let bee = spawn_beemock(&bin).await;
    let ant_gw =
        ant_conformance::spawn_mem_gateway(ant_conformance::MemGatewayConfig::default()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_mins(1))
        .build()
        .expect("build http client");

    let backends = Backends {
        client,
        ant_base: ant_gw.base_url(),
        bee_base: bee.base.clone(),
        batch: bee.batch_id.clone(),
    };

    const SEED: u64 = 0x616e_745f_6d61_6e69; // "ant_mani"
    let n_cases: usize = std::env::var("ANT_MANIFEST_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    let mut cases = pinned_cases();
    cases.extend((0..n_cases).map(|i| gen_case(i, SEED)));
    let total_cases = cases.len();

    // Cases are independent; run a bounded batch concurrently (both
    // backends are in-memory and happily serve parallel requests).
    // Failures collect deterministically by case index.
    const CONCURRENCY: usize = 12;
    let mut failing: Vec<(usize, Vec<String>)> = Vec::new();
    let mut ok = 0usize;
    let mut join_set: tokio::task::JoinSet<(usize, Vec<String>)> = tokio::task::JoinSet::new();
    let mut next = 0usize;
    loop {
        while next < cases.len() && join_set.len() < CONCURRENCY {
            let b = backends.clone();
            let case = cases[next].clone();
            let idx = next;
            join_set.spawn(async move { (idx, b.run_case(&case).await) });
            next += 1;
        }
        let Some(joined) = join_set.join_next().await else {
            break;
        };
        let (idx, diffs) = joined.expect("fuzz case task panicked");
        if diffs.is_empty() {
            ok += 1;
        } else {
            failing.push((idx, diffs));
        }
    }
    failing.sort_by_key(|(idx, _)| *idx);

    // Minimize failing repros sequentially (only pays when a bug was
    // found), capped so a systemic divergence doesn't shrink hundreds.
    let mut failures: Vec<Failure> = Vec::new();
    for (idx, diffs) in failing.into_iter().take(12) {
        let case = &cases[idx];
        let minimized = backends.shrink(case).await;
        let detail = format!(
            "diffs: {}\n  minimized repro: files={:?} index_document={:?}",
            diffs.join("; "),
            minimized
                .files
                .iter()
                .map(|(p, _)| p.as_str())
                .collect::<Vec<_>>(),
            minimized.index_document,
        );
        failures.push(Failure {
            label: case.label.clone(),
            detail,
        });
    }

    ant_gw.shutdown().await;

    println!("=== manifest fuzz: {ok}/{total_cases} cases identical ===");
    for f in &failures {
        println!("FAIL {}\n  {}", f.label, f.detail);
    }
    assert!(
        failures.is_empty(),
        "{} fuzz case(s) diverged between ant and bee (details above)",
        failures.len()
    );
}

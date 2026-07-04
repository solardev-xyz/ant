//! Differential conformance: fire identical HTTP scenarios at ant's
//! production gateway (MemNode-backed, in-process) and at bee's real
//! API layer (`conformance/beemock`, spawned as a child process), then
//! diff the normalized observations.
//!
//! Requires the beemock binary. Build it once with:
//!
//! ```sh
//! cd conformance/beemock && go build -o beemock .
//! ```
//!
//! If the binary is absent (e.g. CI without a Go toolchain) the test
//! **skips** with a notice rather than failing. Override the location
//! with `BEEMOCK_BIN=/path/to/beemock`.
//!
//! Differences that are intentional or tracked live in
//! `conformance/divergences.json`; a diff matching a registry entry is
//! reported as KNOWN and does not fail the run. Any unregistered diff
//! fails.

use ant_conformance::{load, unhex, FeedUpdateVectors, SocVectors};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// --- beemock child process ---------------------------------------------------

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

#[derive(Deserialize)]
struct BeemockHello {
    listening: String,
    #[serde(rename = "batchId")]
    batch_id: String,
}

async fn spawn_beemock(bin: &PathBuf) -> Beemock {
    // Distinct port per test binary run; beemock prints the bound
    // address on stdout, which we parse rather than trust the flag.
    let port = 20000 + (std::process::id() % 10000) as u16;
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

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_mins(1))
        .build()
        .expect("build http client");
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
    // Reap the child before failing so the panic path leaves no zombie.
    let _ = child.kill();
    let _ = child.wait();
    panic!("beemock did not become healthy at {base}");
}

// --- scenario model ----------------------------------------------------------

#[derive(Clone)]
struct Step {
    name: &'static str,
    method: &'static str,
    /// Path template; `{var}` placeholders are substituted from the
    /// backend's variable environment (vars are per-backend because a
    /// value like a tag uid may legitimately differ between backends).
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    /// Extract JSON fields (by key) from the response into variables.
    extract: Vec<(&'static str, &'static str)>,
    /// Step-scoped volatile JSON keys: their values legitimately differ
    /// between backends (e.g. envelope stamps signed by different node
    /// keys), so they're masked *shape-preserving* — presence and value
    /// length still compare — before the diff. Scoped per step so a
    /// generic key name like `index` can't hide real diffs elsewhere.
    volatile: &'static [&'static str],
    /// Step-scoped volatile response headers, masked shape-preserving
    /// like `volatile` (an encrypted upload's `etag` quotes a random
    /// 128-hex reference: the value differs per backend, the format
    /// must not).
    volatile_headers: &'static [&'static str],
    /// Per-backend dynamic step: computes `(path, body)` from the
    /// backend's extracted vars, for steps whose *payload* embeds a
    /// backend-specific value (e.g. a feed update wrapping that
    /// backend's random encrypted reference). Overrides `path`/`body`.
    build: Option<StepBuilder>,
}

/// See [`Step::build`].
type StepBuilder = fn(&HashMap<String, String>) -> (String, Vec<u8>);

impl Step {
    fn new(name: &'static str, method: &'static str, path: impl Into<String>) -> Self {
        Self {
            name,
            method,
            path: path.into(),
            headers: Vec::new(),
            body: Vec::new(),
            extract: Vec::new(),
            volatile: &[],
            volatile_headers: &[],
            build: None,
        }
    }
    fn header(mut self, k: &str, v: impl Into<String>) -> Self {
        self.headers.push((k.to_string(), v.into()));
        self
    }
    fn body(mut self, b: impl Into<Vec<u8>>) -> Self {
        self.body = b.into();
        self
    }
    fn extract(mut self, json_key: &'static str, var: &'static str) -> Self {
        self.extract.push((json_key, var));
        self
    }
    fn volatile(mut self, keys: &'static [&'static str]) -> Self {
        self.volatile = keys;
        self
    }
    fn volatile_headers(mut self, keys: &'static [&'static str]) -> Self {
        self.volatile_headers = keys;
        self
    }
    fn build(mut self, f: StepBuilder) -> Self {
        self.build = Some(f);
        self
    }
}

struct Scenario {
    name: &'static str,
    steps: Vec<Step>,
}

// --- observations ------------------------------------------------------------

/// Response headers we compare. Everything else (date, content-length,
/// connection details, CORS minutiae) is noise for conformance.
const COMPARED_HEADERS: &[&str] = &[
    "content-type",
    "content-disposition",
    "etag",
    "swarm-feed-index",
    "swarm-feed-index-next",
    "swarm-feed-resolved-version",
    "swarm-soc-signature",
    "swarm-tag",
    "swarm-act-history-address",
];

/// Header values that vary legitimately run-to-run: compare presence,
/// not value.
const PRESENCE_ONLY_HEADERS: &[&str] = &["swarm-tag"];

/// JSON body keys whose values are volatile (counters, clocks, chain
/// state); key presence still matters, values are masked before diff.
const VOLATILE_JSON_KEYS: &[&str] = &["uid", "startedAt", "batchTTL", "blockNumber", "txHash"];

#[derive(Debug, PartialEq, Eq, Clone)]
enum BodyRepr {
    Empty,
    Json(String),
    Bytes(String),
}

#[derive(Debug, Clone)]
struct Observation {
    status: u16,
    headers: BTreeMap<String, String>,
    body: BodyRepr,
}

fn mask_json(v: &mut Value, step_volatile: &[&str]) {
    match v {
        Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                if VOLATILE_JSON_KEYS.contains(&k.as_str()) {
                    *val = Value::String("<masked>".into());
                } else if step_volatile.contains(&k.as_str()) {
                    // Shape-preserving mask: the value differs
                    // legitimately (e.g. a stamp signed by a different
                    // node key) but its *length* must still match, so a
                    // 64-hex signature where bee sends 130 hex still
                    // diffs.
                    let masked = match &*val {
                        Value::String(s) => format!("<masked {} chars>", s.len()),
                        other => format!("<masked {}>", kind_of(other)),
                    };
                    *val = Value::String(masked);
                } else {
                    mask_json(val, step_volatile);
                }
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| mask_json(item, step_volatile)),
        _ => {}
    }
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn observe(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    step_volatile: &[&str],
    step_volatile_headers: &[&str],
) -> Observation {
    let mut kept = BTreeMap::new();
    for name in COMPARED_HEADERS {
        if let Some(v) = headers.get(*name) {
            let val = if PRESENCE_ONLY_HEADERS.contains(name) {
                "<present>".to_string()
            } else if step_volatile_headers.contains(name) {
                // Shape-preserving mask, like step-volatile JSON keys.
                format!("<masked {} chars>", v.as_bytes().len())
            } else {
                v.to_str().unwrap_or("<non-ascii>").to_string()
            };
            kept.insert((*name).to_string(), val);
        }
    }
    let is_json = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("json"));
    let body = if body.is_empty() {
        BodyRepr::Empty
    } else if is_json {
        match serde_json::from_slice::<Value>(body) {
            Ok(mut v) => {
                mask_json(&mut v, step_volatile);
                BodyRepr::Json(v.to_string())
            }
            Err(_) => BodyRepr::Bytes(hex::encode(body)),
        }
    } else {
        BodyRepr::Bytes(hex::encode(body))
    };
    Observation {
        status,
        headers: kept,
        body,
    }
}

// --- divergence registry -------------------------------------------------------

#[derive(Deserialize)]
struct DivergenceEntry {
    scenario: String,
    step: String,
    /// "status" | "header:<name>" | "body"
    aspect: String,
    note: String,
}

fn load_registry() -> Vec<DivergenceEntry> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/divergences.json");
    let blob = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("read divergence registry {}: {e}", path.display()));
    serde_json::from_slice(&blob).expect("parse divergences.json")
}

// --- execution ------------------------------------------------------------------

struct Backend {
    name: &'static str,
    base: String,
    vars: HashMap<String, String>,
}

fn substitute(template: &str, vars: &HashMap<String, String>) -> String {
    let mut out = template.to_string();
    for (k, v) in vars {
        out = out.replace(&format!("{{{k}}}"), v);
    }
    out
}

async fn run_step(client: &reqwest::Client, backend: &mut Backend, step: &Step) -> Observation {
    let (path, body) = match step.build {
        Some(f) => f(&backend.vars),
        None => (substitute(&step.path, &backend.vars), step.body.clone()),
    };
    let url = format!("{}{}", backend.base, path);
    let mut req = match step.method {
        "GET" => client.get(&url),
        "HEAD" => client.head(&url),
        "POST" => client.post(&url),
        "PUT" => client.put(&url),
        "DELETE" => client.delete(&url),
        "PATCH" => client.patch(&url),
        other => panic!("unsupported method {other}"),
    };
    for (k, v) in &step.headers {
        req = req.header(k, substitute(v, &backend.vars));
    }
    if !body.is_empty() {
        req = req.body(body);
    }
    let resp = req
        .send()
        .await
        .unwrap_or_else(|e| panic!("{}: {} {} failed: {e}", backend.name, step.method, url));
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    // A backend can violate HTTP by advertising a Content-Length it
    // never delivers (bee does exactly that on GET /feeds for encrypted
    // v1 references — see the registered `encryption/feed-latest`
    // divergence). Surface that as an observable body marker instead of
    // killing the whole run.
    let body = match resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => format!("<body read error: {e}>").into_bytes(),
    };

    if !step.extract.is_empty() {
        if let Ok(v) = serde_json::from_slice::<Value>(&body) {
            for (json_key, var) in &step.extract {
                if let Some(field) = v.get(json_key) {
                    let val = match field {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    backend.vars.insert((*var).to_string(), val);
                }
            }
        }
    }
    observe(
        status,
        &headers,
        &body,
        step.volatile,
        step.volatile_headers,
    )
}

struct Diff {
    scenario: &'static str,
    step: &'static str,
    aspect: String,
    ant: String,
    bee: String,
}

fn compare(
    scenario: &'static str,
    step: &'static str,
    ant: &Observation,
    bee: &Observation,
) -> Vec<Diff> {
    let mut diffs = Vec::new();
    if ant.status != bee.status {
        diffs.push(Diff {
            scenario,
            step,
            aspect: "status".into(),
            ant: ant.status.to_string(),
            bee: bee.status.to_string(),
        });
    }
    let keys: std::collections::BTreeSet<&String> =
        ant.headers.keys().chain(bee.headers.keys()).collect();
    for key in keys {
        let a = ant.headers.get(key);
        let b = bee.headers.get(key);
        if a != b {
            diffs.push(Diff {
                scenario,
                step,
                aspect: format!("header:{key}"),
                ant: a.cloned().unwrap_or_else(|| "<absent>".into()),
                bee: b.cloned().unwrap_or_else(|| "<absent>".into()),
            });
        }
    }
    if ant.body != bee.body {
        let show = |b: &BodyRepr| match b {
            BodyRepr::Empty => "<empty>".to_string(),
            BodyRepr::Json(s) => s.clone(),
            BodyRepr::Bytes(h) if h.len() > 96 => format!("<{} bytes> {}…", h.len() / 2, &h[..96]),
            BodyRepr::Bytes(h) => h.clone(),
        };
        diffs.push(Diff {
            scenario,
            step,
            aspect: "body".into(),
            ant: show(&ant.body),
            bee: show(&bee.body),
        });
    }
    diffs
}

// --- minimal tar builder (for the collection scenario) ---------------------------

fn tar_entry(name: &str, content: &[u8]) -> Vec<u8> {
    let mut header = vec![0u8; 512];
    header[..name.len()].copy_from_slice(name.as_bytes());
    header[100..107].copy_from_slice(b"0000644");
    header[108..115].copy_from_slice(b"0000000");
    header[116..123].copy_from_slice(b"0000000");
    let size = format!("{:011o}", content.len());
    header[124..135].copy_from_slice(size.as_bytes());
    header[136..147].copy_from_slice(b"00000000000");
    header[156] = b'0';
    // Checksum: spaces while summing, then written in place.
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

fn build_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, content) in entries {
        out.extend(tar_entry(name, content));
    }
    out.extend(std::iter::repeat_n(0u8, 1024));
    out
}

// --- encrypted-feed helpers ---------------------------------------------------

/// Identity for the encrypted-feed differential steps: the checked-in
/// conformance key (`det("ant-conformance/key/1")` = one keccak of the
/// label, mirroring vectorgen) and a scenario-private topic.
fn enc_feed_identity() -> ([u8; 32], String, [u8; 32]) {
    let secret = ant_crypto::keccak256(b"ant-conformance/key/1");
    let sk = k256::ecdsa::SigningKey::from_bytes(&secret.into()).expect("valid secret");
    let owner = ant_crypto::ethereum_address_from_public_key(sk.verifying_key());
    let topic = ant_crypto::keccak256(b"differential/enc-feed/topic");
    (secret, hex::encode(owner), topic)
}

/// Per-backend feed update 0 wrapping that backend's freshly-extracted
/// encrypted reference (`enc_feed_ref`): bee's legacy v1 payload
/// `BE8(timestamp) ‖ ref(64)` — an 80-byte wrapped chunk, the encrypted
/// shape `classify_payload` / bee's `isV1Length` recognises. Built per
/// backend because the encrypted reference is random per upload.
fn enc_feed_update_step(vars: &HashMap<String, String>) -> (String, Vec<u8>) {
    let (secret, owner_hex, topic) = enc_feed_identity();
    let enc_ref = hex::decode(
        vars.get("enc_feed_ref")
            .expect("enc_feed_ref extracted by the preceding upload step"),
    )
    .expect("enc_feed_ref is hex");
    assert_eq!(enc_ref.len(), 64, "encrypted reference must be 64 bytes");

    let id = ant_retrieval::sequence_update_id(&topic, 0);
    let mut payload = 1_720_000_000u64.to_be_bytes().to_vec();
    payload.extend_from_slice(&enc_ref);
    let (cac_addr, cac_wire) = ant_crypto::cac_new(&payload).expect("payload fits a chunk");
    let mut digest_input = Vec::with_capacity(64);
    digest_input.extend_from_slice(&id);
    digest_input.extend_from_slice(&cac_addr);
    let digest = ant_crypto::keccak256(&digest_input);
    let sig = ant_crypto::sign_handshake_data(&secret, &digest).expect("sign feed update");
    (
        format!(
            "/soc/{owner_hex}/{}?sig={}",
            hex::encode(id),
            hex::encode(sig)
        ),
        cac_wire,
    )
}

// --- scenarios --------------------------------------------------------------------

fn scenarios() -> Vec<Scenario> {
    let soc: SocVectors = load("soc.json");
    let soc_case = &soc.cases[1]; // random-id-small-payload
    let soc_body = {
        let wire = unhex("chunkDataHex", &soc_case.chunk_data_hex);
        wire[32 + 65..].to_vec()
    };
    let updates: FeedUpdateVectors = load("feed_updates.json");
    let up0 = updates
        .cases
        .iter()
        .find(|c| c.kind == "v2")
        .expect("v2 vector");
    let up0_body = {
        let wire = unhex("chunkDataHex", &up0.chunk_data_hex);
        wire[32 + 65..].to_vec()
    };
    // A second update (index 1) for the same topic/owner, signed by the
    // same key: the v1 vector in feed_updates.json is exactly that.
    let up1 = updates
        .cases
        .iter()
        .find(|c| c.kind == "v1")
        .expect("v1 vector");
    let up1_body = {
        let wire = unhex("chunkDataHex", &up1.chunk_data_hex);
        wire[32 + 65..].to_vec()
    };

    let batch = "{batch}";
    let missing_ref = "00000000000000000000000000000000000000000000000000000000000000ff";
    // Any well-formed 32-byte overlay no backend has ever seen: pins the
    // unknown-peer shapes of the settlement/balance endpoints.
    let unknown_peer = "abababababababababababababababababababababababababababababababab";

    // Duplicate SOC write (swarm-kit Provider Test Center follow-up):
    // same owner/id as the `soc` scenario's upload but a DIFFERENT
    // wrapped payload, with a correspondingly valid signature minted
    // here from the vector's secret. Digest = keccak256(id ‖
    // cac_addr(new_payload)), signed with bee's Ethereum message
    // prefix (`sign_handshake_data`) — the exact pattern
    // tests/vectors.rs pins against bee's own vectors.
    let (dup_sig_hex, dup_body) = {
        let secret = ant_conformance::unhex_arr::<32>("secretHex", &soc_case.secret_hex);
        let id = ant_conformance::unhex_arr::<32>("idHex", &soc_case.id_hex);
        let (cac_addr, cac_wire) =
            ant_crypto::cac_new(b"duplicate soc write: a different payload").expect("cac_new");
        let mut digest_input = Vec::with_capacity(64);
        digest_input.extend_from_slice(&id);
        digest_input.extend_from_slice(&cac_addr);
        let digest = ant_crypto::keccak256(&digest_input);
        let sig = ant_crypto::sign_handshake_data(&secret, &digest).expect("sign duplicate soc");
        (hex::encode(sig), cac_wire)
    };

    // Chunk for the presigned-stamp round trip: `POST /envelope` needs
    // the address *before* the upload, so compute it client-side from a
    // fixed payload (`cac_new` = span-prefixed BMT, what bee's
    // `cac.NewWithDataSpan` computes server-side).
    let (presigned_addr, presigned_wire) =
        ant_crypto::cac_new(b"presigned stamp round trip payload").expect("payload fits a chunk");

    // Multi-chunk (3 leaves) deterministic body for the redundancy
    // scenario: redundancy only changes the tree once there is an
    // intermediate node to erasure-code.
    let redundant_body: Vec<u8> = (0..10240usize).map(|i| ((i * 7) % 251) as u8).collect();

    vec![
        Scenario {
            name: "bytes",
            steps: vec![
                Step::new("upload", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .body(&b"differential bytes payload"[..])
                    .extract("reference", "bytes_ref"),
                Step::new("download", "GET", "/bytes/{bytes_ref}"),
                Step::new("head", "HEAD", "/bytes/{bytes_ref}"),
                // Multi-chunk upload with NO redundancy header: bee
                // applies DefaultUploadLevel = MEDIUM, so the tree gets
                // parities + level-encoded spans — the references only
                // match if ant defaults to MEDIUM too.
                Step::new("upload-multichunk-default", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .body(b"default-level differential payload\n".repeat(400))
                    .extract("reference", "bytes_mc_ref"),
                Step::new(
                    "download-multichunk-default",
                    "GET",
                    "/bytes/{bytes_mc_ref}",
                ),
                Step::new("missing", "GET", format!("/bytes/{missing_ref}")),
                Step::new("upload-no-batch", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .body(&b"x"[..]),
                Step::new("bad-address", "GET", "/bytes/zznothex"),
            ],
        },
        Scenario {
            name: "chunks",
            steps: vec![
                Step::new("upload", "POST", "/chunks")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .body({
                        let payload = b"differential chunk payload".to_vec();
                        let mut wire = (payload.len() as u64).to_le_bytes().to_vec();
                        wire.extend_from_slice(&payload);
                        wire
                    })
                    .extract("reference", "chunk_ref"),
                Step::new("download", "GET", "/chunks/{chunk_ref}"),
                Step::new("missing", "GET", format!("/chunks/{missing_ref}")),
                Step::new("upload-truncated", "POST", "/chunks")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .body(vec![1u8, 2, 3]),
                // Neither batch id nor pre-signed stamp: bee's
                // `batchIdOrStampSig` 400 (the /chunks handler doesn't
                // mark the batch header `required`, unlike /bytes).
                Step::new("upload-no-postage", "POST", "/chunks")
                    .header("content-type", "application/octet-stream")
                    .body({
                        let payload = b"no postage".to_vec();
                        let mut wire = (payload.len() as u64).to_le_bytes().to_vec();
                        wire.extend_from_slice(&payload);
                        wire
                    }),
                // Well-formed hex that isn't a 113-byte stamp: bee's
                // `Stamp.UnmarshalBinary` rejection, verbatim message.
                Step::new("upload-short-stamp", "POST", "/chunks")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-stamp", "deadbeef")
                    .body({
                        let payload = b"short stamp".to_vec();
                        let mut wire = (payload.len() as u64).to_le_bytes().to_vec();
                        wire.extend_from_slice(&payload);
                        wire
                    }),
            ],
        },
        Scenario {
            name: "bzz-file",
            steps: vec![
                Step::new("upload", "POST", "/bzz?name=hello.txt")
                    .header("content-type", "text/plain")
                    .header("swarm-postage-batch-id", batch)
                    .body(&b"hello differential world\n"[..])
                    .extract("reference", "file_ref"),
                Step::new("download", "GET", "/bzz/{file_ref}/"),
                Step::new("head", "HEAD", "/bzz/{file_ref}/"),
            ],
        },
        Scenario {
            name: "bzz-collection",
            steps: vec![
                Step::new("upload", "POST", "/bzz")
                    .header("content-type", "application/x-tar")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-collection", "true")
                    .header("swarm-index-document", "index.html")
                    .body(build_tar(&[
                        ("index.html", b"<h1>index</h1>".as_slice()),
                        ("sub/data.txt", b"nested data".as_slice()),
                    ]))
                    .extract("reference", "dir_ref"),
                Step::new("index", "GET", "/bzz/{dir_ref}/"),
                Step::new("subpath", "GET", "/bzz/{dir_ref}/sub/data.txt"),
                Step::new("missing-path", "GET", "/bzz/{dir_ref}/nope.txt"),
            ],
        },
        // `swarm-encrypt: true` end-to-end. Encrypted references are
        // random per upload, so cross-backend *values* can never match:
        // the upload responses mask the reference shape-preserving
        // (both must be 128 hex chars) and the downloads mask the etag
        // the same way; the decrypted BODIES must still be identical.
        Scenario {
            name: "encryption",
            steps: vec![
                Step::new("upload-bytes", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-encrypt", "true")
                    .body(&b"differential encrypted bytes payload"[..])
                    .extract("reference", "enc_bytes_ref")
                    .volatile(&["reference"]),
                Step::new("download-bytes", "GET", "/bytes/{enc_bytes_ref}")
                    .volatile_headers(&["etag"]),
                Step::new("head-bytes", "HEAD", "/bytes/{enc_bytes_ref}"),
                Step::new("range-bytes", "GET", "/bytes/{enc_bytes_ref}")
                    .header("range", "bytes=13-21")
                    .volatile_headers(&["etag"]),
                // Multi-chunk + explicit redundancy: encryption composes
                // with RS (encrypted erasure tables, halved branching).
                Step::new("upload-bytes-redundant", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-encrypt", "true")
                    .header("swarm-redundancy-level", "2")
                    .body(b"encrypted redundant differential payload\n".repeat(600))
                    .extract("reference", "enc_rs_ref")
                    .volatile(&["reference"]),
                Step::new("download-bytes-redundant", "GET", "/bytes/{enc_rs_ref}")
                    .volatile_headers(&["etag"]),
                // Encrypted single-file /bzz: the manifest nodes are
                // themselves encrypted; walking them requires decrypting
                // every node.
                Step::new("upload-file", "POST", "/bzz?name=secret.txt")
                    .header("content-type", "text/plain")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-encrypt", "true")
                    .body(&b"hello encrypted differential world\n"[..])
                    .extract("reference", "enc_file_ref")
                    .volatile(&["reference"])
                    .volatile_headers(&["etag"]),
                Step::new("download-file", "GET", "/bzz/{enc_file_ref}/")
                    .volatile_headers(&["etag"]),
                Step::new(
                    "download-file-by-name",
                    "GET",
                    "/bzz/{enc_file_ref}/secret.txt",
                )
                .volatile_headers(&["etag"]),
                Step::new("head-file", "HEAD", "/bzz/{enc_file_ref}/").volatile_headers(&["etag"]),
                // Encrypted collection (tar) with subpath walk.
                Step::new("upload-collection", "POST", "/bzz")
                    .header("content-type", "application/x-tar")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-collection", "true")
                    .header("swarm-encrypt", "true")
                    .header("swarm-index-document", "index.html")
                    .body(build_tar(&[
                        ("index.html", b"<h1>secret index</h1>".as_slice()),
                        ("sub/data.txt", b"nested encrypted data".as_slice()),
                    ]))
                    .extract("reference", "enc_dir_ref")
                    .volatile(&["reference"]),
                Step::new("collection-index", "GET", "/bzz/{enc_dir_ref}/")
                    .volatile_headers(&["etag"]),
                Step::new(
                    "collection-subpath",
                    "GET",
                    "/bzz/{enc_dir_ref}/sub/data.txt",
                )
                .volatile_headers(&["etag"]),
                Step::new("collection-missing", "GET", "/bzz/{enc_dir_ref}/nope.txt"),
                // Malformed header value: bee's strconv.ParseBool 400.
                Step::new("bad-encrypt-header", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-encrypt", "maybe")
                    .body(&b"x"[..]),
                // Feed pointing at encrypted content: upload encrypted
                // bytes, wrap the per-backend 64-byte reference in a
                // legacy v1 feed update (80-byte payload), then read the
                // feed — both backends must serve the decrypted content.
                Step::new("upload-feed-content", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-encrypt", "true")
                    .body(&b"encrypted feed content differential"[..])
                    .extract("reference", "enc_feed_ref")
                    .volatile(&["reference"]),
                Step::new("write-enc-feed-update", "POST", "/soc/dynamic")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .build(enc_feed_update_step)
                    .volatile(&["reference"]),
                Step::new("feed-latest", "GET", {
                    let (_, owner_hex, topic) = enc_feed_identity();
                    format!("/feeds/{owner_hex}/{}", hex::encode(topic))
                })
                .volatile_headers(&["etag", "swarm-soc-signature"]),
            ],
        },
        Scenario {
            name: "soc",
            steps: vec![
                Step::new(
                    "upload",
                    "POST",
                    format!(
                        "/soc/{}/{}?sig={}",
                        soc_case.owner_hex, soc_case.id_hex, soc_case.signature_hex
                    ),
                )
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(soc_body.clone()),
                Step::new(
                    "download",
                    "GET",
                    format!("/soc/{}/{}", soc_case.owner_hex, soc_case.id_hex),
                ),
                Step::new(
                    "download-root-chunk",
                    "GET",
                    format!("/soc/{}/{}", soc_case.owner_hex, soc_case.id_hex),
                )
                .header("swarm-only-root-chunk", "true"),
                Step::new(
                    "upload-bad-sig",
                    "POST",
                    format!(
                        "/soc/{}/{}?sig={}",
                        soc_case.owner_hex,
                        soc_case.id_hex,
                        // valid hex, wrong signature (flipped first byte)
                        {
                            let mut sig = unhex("signatureHex", &soc_case.signature_hex);
                            sig[0] ^= 0xff;
                            hex::encode(sig)
                        }
                    ),
                )
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(soc_body.clone()),
                Step::new(
                    "upload-missing-sig",
                    "POST",
                    format!("/soc/{}/{}", soc_case.owner_hex, soc_case.id_hex),
                )
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(soc_body.clone()),
                // Duplicate-write semantics: same SOC (owner/id), new
                // payload, valid new signature. Bee ACCEPTS the write
                // (201, same reference) — SOCs are mutable — and the
                // reserve's SOC put REPLACES the stored payload when
                // the new stamp timestamp is newer
                // (pkg/storer/internal/reserve reserve.go: `ChunkStore().
                // Replace` for ChunkTypeSingleOwner), so the LAST write
                // wins on read. swarm-kit's expectation that a
                // duplicate feed-index write FAILS does not hold for
                // bee at the HTTP level.
                Step::new(
                    "upload-duplicate",
                    "POST",
                    format!(
                        "/soc/{}/{}?sig={}",
                        soc_case.owner_hex, soc_case.id_hex, dup_sig_hex
                    ),
                )
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(dup_body),
                // Pins which payload wins after the duplicate write
                // (body AND swarm-soc-signature header must both flip
                // to the second write's values).
                Step::new(
                    "download-after-duplicate",
                    "GET",
                    format!("/soc/{}/{}", soc_case.owner_hex, soc_case.id_hex),
                ),
            ],
        },
        // The swarm-kit Provider Test Center repro: write update 0,
        // read latest, write update 1, read latest again. Bee sees each
        // write immediately (read-your-own-writes); this is where ant's
        // real node historically diverged (feed_empty / stuck index 0).
        // MemNode stores everything locally, so a diff here on the mem
        // rig means a *gateway-layer* bug rather than the network race.
        Scenario {
            name: "feeds-timeline",
            steps: vec![
                Step::new(
                    "create-manifest",
                    "POST",
                    format!("/feeds/{}/{}?type=sequence", up0.owner_hex, up0.topic_hex),
                )
                .header("swarm-postage-batch-id", batch)
                .extract("reference", "feed_manifest"),
                Step::new(
                    "latest-before-any-update",
                    "GET",
                    format!("/feeds/{}/{}", up0.owner_hex, up0.topic_hex),
                ),
                Step::new(
                    "write-update-0",
                    "POST",
                    format!(
                        "/soc/{}/{}?sig={}",
                        up0.owner_hex, up0.id_hex, up0.signature_hex
                    ),
                )
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(up0_body),
                Step::new(
                    "latest-after-0",
                    "GET",
                    format!("/feeds/{}/{}", up0.owner_hex, up0.topic_hex),
                ),
                Step::new(
                    "write-update-1",
                    "POST",
                    format!(
                        "/soc/{}/{}?sig={}",
                        up1.owner_hex, up1.id_hex, up1.signature_hex
                    ),
                )
                .header("content-type", "application/octet-stream")
                .header("swarm-postage-batch-id", batch)
                .body(up1_body),
                Step::new(
                    "latest-after-1",
                    "GET",
                    format!("/feeds/{}/{}", up0.owner_hex, up0.topic_hex),
                ),
                Step::new("deref-via-bzz", "GET", "/bzz/{feed_manifest}/"),
                Step::new(
                    "epoch-type",
                    "GET",
                    format!("/feeds/{}/{}?type=epoch", up0.owner_hex, up0.topic_hex),
                ),
            ],
        },
        Scenario {
            name: "tags",
            steps: vec![
                Step::new("create", "POST", "/tags").extract("uid", "tag_uid"),
                Step::new("get", "GET", "/tags/{tag_uid}"),
                Step::new("list", "GET", "/tags"),
                Step::new("delete", "DELETE", "/tags/{tag_uid}"),
                Step::new("patch", "PATCH", "/tags/{tag_uid}")
                    .header("content-type", "application/json")
                    .body(format!("{{\"address\":\"{missing_ref}\"}}").into_bytes()),
            ],
        },
        Scenario {
            name: "stewardship-envelope",
            steps: vec![
                Step::new("seed-upload", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .body(&b"stewardship seed"[..])
                    .extract("reference", "st_ref"),
                Step::new("stewardship-check", "GET", "/stewardship/{st_ref}"),
                Step::new("stewardship-reupload", "PUT", "/stewardship/{st_ref}")
                    .header("swarm-postage-batch-id", batch),
                // Never-uploaded root: retrievability is a clean false
                // (bee maps the traversal's not-found to
                // `{"isRetrievable": false}`, not an error).
                Step::new(
                    "stewardship-missing",
                    "GET",
                    format!("/stewardship/{missing_ref}"),
                ),
                // Re-upload requires the batch header (bee marks it
                // `required` on PUT — mapStructure 400).
                Step::new(
                    "stewardship-reupload-no-batch",
                    "PUT",
                    "/stewardship/{st_ref}",
                ),
                // A manifest-rooted tree: stewardship must follow the
                // mantaray forks down to the file data, not just the
                // root span tree.
                Step::new("seed-bzz", "POST", "/bzz?name=steward.txt")
                    .header("content-type", "text/plain")
                    .header("swarm-postage-batch-id", batch)
                    .body(&b"stewardship manifest seed content, long enough to matter"[..])
                    .extract("reference", "st_bzz_ref"),
                Step::new(
                    "stewardship-check-manifest",
                    "GET",
                    "/stewardship/{st_bzz_ref}",
                ),
                Step::new(
                    "stewardship-reupload-manifest",
                    "PUT",
                    "/stewardship/{st_bzz_ref}",
                )
                .header("swarm-postage-batch-id", batch),
                // Envelope: issuer/index/timestamp/signature values are
                // signed by each backend's own node key, so they differ
                // legitimately — masked shape-preserving (key presence
                // + value length still compare).
                Step::new("envelope", "POST", "/envelope/{st_ref}")
                    .header("swarm-postage-batch-id", batch)
                    .volatile(&["issuer", "index", "timestamp", "signature"]),
                // Bee checks the header struct before the path, so the
                // missing batch header wins even on a valid address.
                Step::new("envelope-no-batch", "POST", "/envelope/{st_ref}"),
                Step::new("envelope-bad-address", "POST", "/envelope/zznothex")
                    .header("swarm-postage-batch-id", batch),
                // Presigned round trip: mint an envelope for a chunk
                // address computed client-side, rebuild the 113-byte
                // stamp from the response (per-backend vars — each
                // backend must accept *its own* issuer's stamp), then
                // upload the chunk with `swarm-postage-stamp` only.
                Step::new(
                    "envelope-for-presigned",
                    "POST",
                    format!("/envelope/{}", hex::encode(presigned_addr)),
                )
                .header("swarm-postage-batch-id", batch)
                .volatile(&["issuer", "index", "timestamp", "signature"])
                .extract("index", "env_index")
                .extract("timestamp", "env_timestamp")
                .extract("signature", "env_signature"),
                Step::new("presigned-upload", "POST", "/chunks")
                    .header("content-type", "application/octet-stream")
                    .header(
                        "swarm-postage-stamp",
                        "{batch}{env_index}{env_timestamp}{env_signature}",
                    )
                    .body(presigned_wire.clone()),
                Step::new(
                    "presigned-download",
                    "GET",
                    format!("/chunks/{}", hex::encode(presigned_addr)),
                ),
            ],
        },
        // Read-only settlement/balance endpoints (bee balances.go /
        // settlements.go / chequebook.go last-cheque handlers). Both
        // backends are peerless here, so this pins the empty-collection
        // shapes plus each unknown-peer response: 404 "No balance for
        // peer" (balances/consumed), 404 "no settlements for peer"
        // (settlements), and — deliberately NOT a 404 — 200 with null
        // lastreceived/lastsent for /chequebook/cheque/{peer} (bee's
        // handler tolerates chequebook.ErrNoCheque on both sides).
        // Redundant uploads (swarm-redundancy-level): byte-identical
        // references here are the drop-in proof for ant's Reed-Solomon
        // encoder — the reference is the root of the erasure-coded
        // tree (parity refs + level-encoded spans included), so it only
        // matches if ant produced bee's exact pipeline output. Bodies
        // are multi-chunk (> 4096 B) so redundancy actually reshapes
        // the tree, and every upload is downloaded back on both
        // backends. The invalid header values pin bee's two-tier error
        // shape: Go parse failures keep the map-tag casing
        // (`Swarm-Redundancy-Level`), the range validator lowercases.
        Scenario {
            name: "redundancy",
            steps: vec![
                Step::new("upload-bytes-level1", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "1")
                    .body(redundant_body.clone())
                    .extract("reference", "rs_bytes1"),
                Step::new("download-bytes-level1", "GET", "/bytes/{rs_bytes1}"),
                Step::new("upload-bytes-level4", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "4")
                    .body(redundant_body.clone())
                    .extract("reference", "rs_bytes4"),
                Step::new("download-bytes-level4", "GET", "/bytes/{rs_bytes4}"),
                Step::new("upload-bzz-level1", "POST", "/bzz?name=redundant.bin")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "1")
                    .body(redundant_body.clone())
                    .extract("reference", "rs_bzz1"),
                Step::new("download-bzz-level1", "GET", "/bzz/{rs_bzz1}/"),
                Step::new("upload-collection-level4", "POST", "/bzz")
                    .header("content-type", "application/x-tar")
                    .header("swarm-collection", "true")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "4")
                    .header("swarm-index-document", "index.html")
                    .body(build_tar(&[
                        ("index.html", b"<h1>redundant site</h1>"),
                        ("big/data.bin", redundant_body.as_slice()),
                    ]))
                    .extract("reference", "rs_dir4"),
                Step::new(
                    "download-collection-level4",
                    "GET",
                    "/bzz/{rs_dir4}/big/data.bin",
                ),
                // POST /feeds accepts the header too; a feed manifest
                // is a single mantaray node, so the reference stays the
                // level-independent manifest root (bee only adds
                // dispersed replicas of the node).
                Step::new(
                    "create-feed-manifest-level2",
                    "POST",
                    format!("/feeds/{}/{}?type=sequence", up0.owner_hex, up0.topic_hex),
                )
                .header("swarm-postage-batch-id", batch)
                .header("swarm-redundancy-level", "2")
                .extract("reference", "rs_feed_manifest"),
                Step::new("level-out-of-range", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "9")
                    .body(&b"x"[..]),
                Step::new("level-not-a-number", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "abc")
                    .body(&b"x"[..]),
                Step::new("level-overflows-uint8", "POST", "/bytes")
                    .header("content-type", "application/octet-stream")
                    .header("swarm-postage-batch-id", batch)
                    .header("swarm-redundancy-level", "256")
                    .body(&b"x"[..]),
            ],
        },
        Scenario {
            name: "settlements",
            steps: vec![
                Step::new("balances", "GET", "/balances"),
                Step::new(
                    "balances-unknown-peer",
                    "GET",
                    format!("/balances/{unknown_peer}"),
                ),
                Step::new("consumed", "GET", "/consumed"),
                Step::new(
                    "consumed-unknown-peer",
                    "GET",
                    format!("/consumed/{unknown_peer}"),
                ),
                Step::new("settlements", "GET", "/settlements"),
                Step::new(
                    "settlements-unknown-peer",
                    "GET",
                    format!("/settlements/{unknown_peer}"),
                ),
                Step::new("timesettlements", "GET", "/timesettlements"),
                Step::new("cheque-list", "GET", "/chequebook/cheque"),
                Step::new(
                    "cheque-unknown-peer",
                    "GET",
                    format!("/chequebook/cheque/{unknown_peer}"),
                ),
            ],
        },
    ]
}

// --- main test ----------------------------------------------------------------------

#[tokio::test]
async fn differential_ant_vs_bee() {
    let Some(bin) = beemock_bin() else {
        eprintln!(
            "SKIP differential_ant_vs_bee: beemock binary not found; \
             build it with `cd conformance/beemock && go build -o beemock .` \
             or set BEEMOCK_BIN"
        );
        return;
    };

    let bee = spawn_beemock(&bin).await;
    let ant_gw =
        ant_conformance::spawn_mem_gateway(ant_conformance::MemGatewayConfig::default()).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_mins(1))
        .build()
        .expect("build http client");
    let registry = load_registry();
    let mut known = Vec::new();
    let mut failures = Vec::new();
    let mut ok_count = 0usize;

    for scenario in scenarios() {
        let mut ant = Backend {
            name: "ant",
            base: ant_gw.base_url(),
            vars: HashMap::from([("batch".to_string(), bee.batch_id.clone())]),
        };
        let mut oracle = Backend {
            name: "bee",
            base: bee.base.clone(),
            vars: HashMap::from([("batch".to_string(), bee.batch_id.clone())]),
        };
        for step in &scenario.steps {
            let ant_obs = run_step(&client, &mut ant, step).await;
            let bee_obs = run_step(&client, &mut oracle, step).await;
            let diffs = compare(scenario.name, step.name, &ant_obs, &bee_obs);
            if diffs.is_empty() {
                ok_count += 1;
            }
            for d in diffs {
                let is_known = registry
                    .iter()
                    .any(|e| e.scenario == d.scenario && e.step == d.step && e.aspect == d.aspect);
                if is_known {
                    known.push(d);
                } else {
                    failures.push(d);
                }
            }
        }
    }

    ant_gw.shutdown().await;

    println!("\n=== differential report: {ok_count} steps identical ===");
    if !known.is_empty() {
        println!("--- known divergences ({}) ---", known.len());
        for d in &known {
            let note = registry
                .iter()
                .find(|e| e.scenario == d.scenario && e.step == d.step && e.aspect == d.aspect)
                .map_or("", |e| e.note.as_str());
            println!(
                "KNOWN {}/{} [{}] ant={} bee={} ({note})",
                d.scenario, d.step, d.aspect, d.ant, d.bee
            );
        }
    }
    if !failures.is_empty() {
        println!("--- UNREGISTERED divergences ({}) ---", failures.len());
        for d in &failures {
            println!(
                "DIFF {}/{} [{}]\n  ant: {}\n  bee: {}",
                d.scenario, d.step, d.aspect, d.ant, d.bee
            );
        }
    }
    assert!(
        failures.is_empty(),
        "{} unregistered divergence(s) between ant and bee — either fix ant or \
         (if intentional) register them in conformance/divergences.json",
        failures.len()
    );
}

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
}

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
) -> Observation {
    let mut kept = BTreeMap::new();
    for name in COMPARED_HEADERS {
        if let Some(v) = headers.get(*name) {
            let val = if PRESENCE_ONLY_HEADERS.contains(name) {
                "<present>".to_string()
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
    let url = format!("{}{}", backend.base, substitute(&step.path, &backend.vars));
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
    if !step.body.is_empty() {
        req = req.body(step.body.clone());
    }
    let resp = req
        .send()
        .await
        .unwrap_or_else(|e| panic!("{}: {} {} failed: {e}", backend.name, step.method, url));
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = resp.bytes().await.expect("read body").to_vec();

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
    observe(status, &headers, &body, step.volatile)
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

    // Chunk for the presigned-stamp round trip: `POST /envelope` needs
    // the address *before* the upload, so compute it client-side from a
    // fixed payload (`cac_new` = span-prefixed BMT, what bee's
    // `cac.NewWithDataSpan` computes server-side).
    let (presigned_addr, presigned_wire) =
        ant_crypto::cac_new(b"presigned stamp round trip payload").expect("payload fits a chunk");

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

    let client = reqwest::Client::new();
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

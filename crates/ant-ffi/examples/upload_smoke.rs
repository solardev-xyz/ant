//! FFI-driven upload smoke: drive a real upload entirely through the
//! `ant-ffi` C-ABI entry points, the same surface a C / Swift / Kotlin
//! host calls.
//!
//! It calls [`ant_init`], waits for a BZZ peer set via [`ant_peer_count`],
//! registers a postage batch with [`ant_storage_connect_batch`] (needs the
//! `chain` feature), starts an upload with [`ant_upload_start`], polls
//! [`ant_upload_status`] until the job reports `completed`, and prints the
//! resulting bzz reference as `REF=<hex>` on stdout so the caller can verify
//! retrievability through a gateway.
//!
//! Run it via the `upload-integration` workflow, or locally:
//!
//! ```text
//! ANT_FFI_DATA_DIR=/path/with/funded/identity.json \
//! GNOSIS_RPC_URL=… ANT_BATCH=0x… ANT_FFI_FILE=/tmp/1mb.bin \
//!   cargo run -p ant-ffi --features chain --example upload_smoke
//! ```
//!
//! The identity in `ANT_FFI_DATA_DIR` must own `ANT_BATCH` on Gnosis — the
//! embedded node signs stamps with its own key, so the batch owner has to
//! match (see `ant_storage_connect_batch`).

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::{Duration, Instant};

use ant_ffi::{
    ant_free_string, ant_init, ant_peer_count, ant_shutdown, ant_storage_connect_batch,
    ant_upload_start, ant_upload_status, AntHandle,
};

fn env_required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| {
        eprintln!("missing required env var {key}");
        std::process::exit(2);
    })
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn die(msg: &str) -> ! {
    eprintln!("upload_smoke: {msg}");
    std::process::exit(1);
}

/// Copy a `*mut c_char` the FFI just returned into an owned `String`, then
/// release it with `ant_free_string`. A null pointer yields `None`.
fn take_string(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `ptr` is a NUL-terminated string the FFI allocated and handed
    // to us; we own it and free it exactly once below.
    let owned = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    unsafe { ant_free_string(ptr) };
    Some(owned)
}

fn cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| die("argument contains an interior NUL byte"))
}

fn main() {
    let data_dir = env_required("ANT_FFI_DATA_DIR");
    let rpc = env_required("GNOSIS_RPC_URL");
    let batch = env_required("ANT_BATCH");
    let file = env_required("ANT_FFI_FILE");
    let peer_threshold: i32 = env_or("ANT_PEER_THRESHOLD", 50);
    let peer_timeout = Duration::from_secs(env_or("ANT_PEER_TIMEOUT", 300));
    let upload_timeout = Duration::from_secs(env_or("ANT_UPLOAD_TIMEOUT", 1800));

    let c_data_dir = cstring(&data_dir);
    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: `c_data_dir` is a valid NUL-terminated string; `err` is a
    // writable slot. The returned handle is freed via `ant_shutdown`.
    let handle: *mut AntHandle = unsafe { ant_init(c_data_dir.as_ptr(), &raw mut err) };
    if handle.is_null() {
        die(&take_string(err).unwrap_or_else(|| "ant_init returned null".into()));
    }
    println!("ant_init ok (data dir {data_dir})");

    let params = UploadParams {
        rpc: &rpc,
        batch: &batch,
        file: &file,
        peer_threshold,
        peer_timeout,
        upload_timeout,
    };
    let code = drive(handle, &params);

    // SAFETY: `handle` came from `ant_init` and has not been freed yet.
    unsafe { ant_shutdown(handle) };
    std::process::exit(code);
}

struct UploadParams<'a> {
    rpc: &'a str,
    batch: &'a str,
    file: &'a str,
    peer_threshold: i32,
    peer_timeout: Duration,
    upload_timeout: Duration,
}

fn drive(handle: *mut AntHandle, p: &UploadParams<'_>) -> i32 {
    // 1. Wait for a usable peer set.
    let start = Instant::now();
    loop {
        // SAFETY: `handle` is a live handle from `ant_init`.
        let peers = unsafe { ant_peer_count(handle) };
        if peers >= p.peer_threshold {
            println!("peer set ready: {peers} peers");
            break;
        }
        if start.elapsed() > p.peer_timeout {
            eprintln!(
                "only {peers} peers after timeout (needed {})",
                p.peer_threshold
            );
            return 1;
        }
        std::thread::sleep(Duration::from_secs(5));
    }

    // 2. Register the postage batch for stamping.
    let c_rpc = cstring(p.rpc);
    let c_batch = cstring(p.batch);
    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: all pointers are valid NUL-terminated strings / a writable slot.
    let connected = unsafe {
        ant_storage_connect_batch(handle, c_rpc.as_ptr(), c_batch.as_ptr(), &raw mut err)
    };
    if connected.is_null() {
        eprintln!(
            "ant_storage_connect_batch failed: {}",
            take_string(err).unwrap_or_else(|| "unknown".into())
        );
        return 1;
    }
    take_string(connected);
    println!("connected batch {}", p.batch);

    // 3. Start the upload.
    let c_file = cstring(p.file);
    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: `c_file` / `c_batch` are valid strings; name + content_type are
    // null (let the daemon infer them); `err` is a writable slot.
    let job_ptr = unsafe {
        ant_upload_start(
            handle,
            c_file.as_ptr(),
            c_batch.as_ptr(),
            ptr::null(),
            ptr::null(),
            &raw mut err,
        )
    };
    let Some(job_id) = take_string(job_ptr) else {
        eprintln!(
            "ant_upload_start failed: {}",
            take_string(err).unwrap_or_else(|| "unknown".into())
        );
        return 1;
    };
    println!("upload job {job_id} started for {}", p.file);

    // 4. Poll the job to completion.
    let c_job = cstring(&job_id);
    let start = Instant::now();
    loop {
        let mut err: *mut c_char = ptr::null_mut();
        // SAFETY: `c_job` is a valid string; `err` is a writable slot.
        let view_ptr = unsafe { ant_upload_status(handle, c_job.as_ptr(), &raw mut err) };
        let Some(view_json) = take_string(view_ptr) else {
            eprintln!(
                "ant_upload_status failed: {}",
                take_string(err).unwrap_or_else(|| "unknown".into())
            );
            return 1;
        };
        let view: serde_json::Value =
            serde_json::from_str(&view_json).unwrap_or(serde_json::Value::Null);
        let status = view
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let pushed = view
            .get("chunks_pushed")
            .and_then(serde_json::Value::as_u64);
        let total = view.get("chunks_total").and_then(serde_json::Value::as_u64);
        println!(
            "status={status} chunks={}/{}",
            pushed.map_or_else(|| "?".into(), |n| n.to_string()),
            total.map_or_else(|| "?".into(), |n| n.to_string()),
        );
        match status {
            "completed" => {
                let reference = view
                    .get("reference")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                if reference.is_empty() {
                    eprintln!("upload completed but no reference in view");
                    return 1;
                }
                println!("REF={reference}");
                return 0;
            }
            "failed" | "cancelled" => {
                eprintln!("upload ended with status={status}: {view_json}");
                return 1;
            }
            _ => {}
        }
        if start.elapsed() > p.upload_timeout {
            eprintln!("upload did not complete within timeout (last status={status})");
            return 1;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

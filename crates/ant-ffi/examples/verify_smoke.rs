//! FFI-driven propagation-verify smoke: run the exact
//! [`ant_storage_verify_propagation`] path the `AntDrive` app uses, from a
//! desktop process, so its behaviour (and debug logs) can be inspected
//! without a simulator in the loop.
//!
//! ```text
//! ANT_FFI_DATA_DIR=/tmp/verify-smoke ANT_REF=0x… \
//!   cargo run -p ant-ffi --example verify_smoke
//! ```

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::{Duration, Instant};

use ant_ffi::{
    ant_free_string, ant_init, ant_peer_count, ant_shutdown, ant_storage_verify_propagation,
    AntHandle,
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

/// Copy a `*mut c_char` the FFI just returned into an owned `String`,
/// then release it with `ant_free_string`. A null pointer yields `None`.
fn take_string(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `ptr` is a NUL-terminated string the FFI allocated and
    // handed to us; we own it and free it exactly once below.
    let owned = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    unsafe { ant_free_string(ptr) };
    Some(owned)
}

fn main() {
    let data_dir = env_required("ANT_FFI_DATA_DIR");
    let reference = env_required("ANT_REF");
    let samples: u8 = env_or("ANT_SAMPLES", 0);
    let probes: u8 = env_or("ANT_PROBES", 2);
    let peer_threshold: i32 = env_or("ANT_PEER_THRESHOLD", 50);
    let peer_timeout = Duration::from_secs(env_or("ANT_PEER_TIMEOUT", 300));

    let c_data_dir = CString::new(data_dir.clone()).expect("data dir contains NUL");
    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: `c_data_dir` is a valid NUL-terminated string; `err` is a
    // writable slot. The returned handle is freed via `ant_shutdown`.
    let handle: *mut AntHandle = unsafe { ant_init(c_data_dir.as_ptr(), &raw mut err) };
    if handle.is_null() {
        eprintln!(
            "ant_init failed: {}",
            take_string(err).unwrap_or_else(|| "unknown".into())
        );
        std::process::exit(1);
    }
    println!("ant_init ok (data dir {data_dir})");

    let start = Instant::now();
    loop {
        // SAFETY: `handle` is a live handle from `ant_init`.
        let peers = unsafe { ant_peer_count(handle) };
        if peers >= peer_threshold {
            println!("peer set ready: {peers} peers");
            break;
        }
        if start.elapsed() > peer_timeout {
            eprintln!("only {peers} peers after timeout (needed {peer_threshold})");
            // SAFETY: `handle` is live and not yet freed.
            unsafe { ant_shutdown(handle) };
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    let c_ref = CString::new(reference.clone()).expect("reference contains NUL");
    let mut err: *mut c_char = ptr::null_mut();
    let started = Instant::now();
    // SAFETY: `handle` is live, `c_ref` is a valid string, `err` writable.
    let out = unsafe {
        ant_storage_verify_propagation(handle, c_ref.as_ptr(), samples, probes, &raw mut err)
    };
    let elapsed = started.elapsed();
    match take_string(out) {
        Some(json) => println!("VERIFY ({elapsed:.0?}): {json}"),
        None => println!(
            "VERIFY FAILED ({elapsed:.0?}): {}",
            take_string(err).unwrap_or_else(|| "unknown".into())
        ),
    }

    // SAFETY: `handle` came from `ant_init` and has not been freed yet.
    unsafe { ant_shutdown(handle) };
}

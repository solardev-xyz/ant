//! In-process bee-shaped HTTP gateway for the iOS app.
//!
//! [`ant_start_gateway`] boots the same [`ant_gateway`] HTTP surface
//! `antd` serves — `/bzz`, `/bytes`, `/chunks`, `/feeds`, `/soc`,
//! `/tags`, status, and the bee-stubbed `/wallet` / `/stamps` /
//! `/chequebook` — on a loopback address **inside the app process**,
//! driven by the node loop [`crate::ant_init`] already started.
//!
//! iOS apps cannot spawn the `antd` daemon (no subprocesses in the app
//! sandbox), so this is the in-process equivalent: the existing
//! bee-HTTP Swift layer (`BeeAPIClient`, `BzzSchemeHandler`) points at
//! `http://127.0.0.1:<port>` unchanged. There is exactly one gateway
//! per handle; a second `ant_start_gateway` while one is running is a
//! no-op success.

use crate::{clear_out_err, write_out_err, AntHandle};
use ant_control::GatewayActivity;
use ant_gateway::{CorsConfig, Gateway, GatewayHandle, GatewayIdentity, TagRegistry};
use k256::ecdsa::SigningKey;
use std::ffi::{c_char, CStr};
use std::net::SocketAddr;
use std::sync::Arc;

/// Bee API version advertised via `/health.apiVersion`. Mirrors `antd`'s
/// `BEE_API_VERSION` so `bee-js`/Freedom see the same wire contract.
const BEE_API_VERSION: &str = "7.2.0";

/// Default loopback bind when `api_addr` is null/empty. Mirrors bee's
/// default API port so the iOS bee-HTTP client needs no base-URL change.
const DEFAULT_API_ADDR: &str = "127.0.0.1:1633";

/// Start the in-process bee-shaped HTTP gateway on `api_addr`
/// (default `127.0.0.1:1633` when null/empty), serving the node the
/// handle owns.
///
/// `light_mode` drives `GET /node.beeMode`: `true` advertises
/// `light` (Freedom's `checkSwarmPreFlight` allows publish/feed/SOC
/// writes), `false` advertises `ultra-light` (read-only browsing).
///
/// Returns `true` on success (or if a gateway is already running),
/// `false` on error with an allocated message written to `out_err`
/// (free with [`crate::ant_free_string`]). Idempotent: a second call
/// while one is live is a no-op success.
///
/// # Safety
///
/// `handle` must come from [`crate::ant_init`] and must not have been
/// passed to [`crate::ant_shutdown`]. `api_addr`, if non-null, must be a
/// NUL-terminated UTF-8 string. `out_err`, if non-null, must point to a
/// writable `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_start_gateway(
    handle: *const AntHandle,
    api_addr: *const c_char,
    light_mode: bool,
    out_err: *mut *mut c_char,
) -> bool {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_start_gateway: null handle");
            return false;
        };

        let addr_str = if api_addr.is_null() {
            DEFAULT_API_ADDR.to_string()
        } else {
            match CStr::from_ptr(api_addr).to_str().map(str::trim) {
                Ok(s) if !s.is_empty() => s.to_string(),
                Ok(_) => DEFAULT_API_ADDR.to_string(),
                Err(_) => {
                    write_out_err(out_err, "ant_start_gateway: api_addr is not valid UTF-8");
                    return false;
                }
            }
        };
        let addr: SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(e) => {
                write_out_err(
                    out_err,
                    &format!("ant_start_gateway: invalid api_addr `{addr_str}`: {e}"),
                );
                return false;
            }
        };

        // Idempotent: if a gateway is already live, do nothing. A task
        // that has finished (bind error / aborted) is cleared so a
        // retry can rebind.
        {
            let mut slot = handle.gateway_task.lock().unwrap();
            if slot.as_ref().is_some_and(|task| !task.is_finished()) {
                return true;
            }
            // A finished task (bind error / aborted) is cleared so a
            // retry can rebind.
            *slot = None;
        }

        // Identity surface for `/addresses`. Overlay + peer-id come from
        // the live status snapshot; the compressed secp256k1 public key
        // isn't carried there, so derive it from the signing secret
        // (same key the node loaded at init).
        let (overlay_hex, ethereum_hex, peer_id) = {
            let snap = handle.status_rx.borrow();
            (
                snap.identity.overlay.trim_start_matches("0x").to_string(),
                snap.identity.eth_address.clone(),
                snap.identity.peer_id.clone(),
            )
        };
        let public_key_hex = match SigningKey::from_bytes((&handle.signing_secret).into()) {
            Ok(sk) => hex::encode(sk.verifying_key().to_encoded_point(true).as_bytes()),
            Err(e) => {
                write_out_err(out_err, &format!("ant_start_gateway: invalid signing key: {e}"));
                return false;
            }
        };

        // Probe-bind synchronously so a port clash surfaces as a clean
        // FFI error instead of a silently-dead background task. There's
        // a tiny TOCTOU window before `Gateway::serve` rebinds, but on
        // iOS this process is the only thing touching the port.
        if let Err(e) = std::net::TcpListener::bind(addr) {
            write_out_err(out_err, &format!("ant_start_gateway: bind {addr}: {e}"));
            return false;
        }

        let gw = GatewayHandle {
            agent: Arc::new(crate::ANT_FFI_AGENT.to_string()),
            api_version: Arc::new(BEE_API_VERSION.to_string()),
            identity: Arc::new(GatewayIdentity {
                overlay_hex,
                ethereum_hex,
                public_key_hex,
                peer_id,
            }),
            status: handle.status_rx.clone(),
            commands: handle.cmd_tx.clone(),
            // Standalone activity registry: the gateway is the only
            // writer on iOS (no `antop` Retrieval tab consuming it).
            activity: GatewayActivity::new(),
            light_mode,
            tags: Arc::new(TagRegistry::new()),
            // Freedom's dweb pages fetch/upload from an opaque (`null`)
            // origin, so the gateway must echo `null` in CORS — matches
            // bee started with `--cors-allowed-origins=null`.
            cors: Arc::new(CorsConfig::new(["null"])),
            // No on-chain reader yet: `/wallet` + `/chequebook` report
            // bee zero-stubs and chain-state endpoints fall to 501.
            // Light-mode postage parity wires a `ChainContext` later.
            chain: None,
        };

        let task = handle.runtime.spawn(async move {
            if let Err(e) = Gateway::serve(gw, addr).await {
                tracing::error!(target: "ant-ffi", "in-process gateway ended: {e}");
            }
        });
        *handle.gateway_task.lock().unwrap() = Some(task);
        true
    }
}

/// Stop the in-process HTTP gateway started by [`ant_start_gateway`].
/// Returns `true` if a gateway was running and was aborted, `false` if
/// none was running (or `handle` is null). Safe to call repeatedly.
///
/// # Safety
///
/// `handle` must come from [`crate::ant_init`] and must not have been
/// passed to [`crate::ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_stop_gateway(handle: *const AntHandle) -> bool {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return false;
        };
        match handle.gateway_task.lock().unwrap().take() {
            Some(task) => {
                task.abort();
                true
            }
            None => false,
        }
    }
}

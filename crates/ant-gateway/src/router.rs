//! Axum router assembly.
//!
//! The router is split out of `lib.rs` so the integration tests in
//! `tests/` can build the same `Router<()>` production uses and drive
//! it via `tower::ServiceExt::oneshot` — no socket, no port binding,
//! no flaky timing.

use std::time::Instant;

use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, on, post, MethodFilter, MethodRouter};
use axum::Router;
use tracing::Instrument;

use crate::fallback::not_implemented;
use crate::handle::GatewayHandle;
use crate::{retrieval, status, stubs};

const SERVER_HEADER: &str = concat!("ant-gateway/", env!("CARGO_PKG_VERSION"));

/// Build the production [`axum::Router`] from a [`GatewayHandle`].
///
/// Public so integration tests can mount it without standing up a
/// listener; the binary code in [`crate::Gateway::serve`] uses the same
/// builder.
pub fn build(handle: GatewayHandle) -> Router {
    Router::new()
        .route("/health", get(status::health))
        .route("/readiness", get(status::readiness))
        .route("/node", get(status::node))
        .route("/addresses", get(status::addresses))
        .route("/peers", get(status::peers))
        .route("/topology", get(status::topology))
        .route("/wallet", get(stubs::wallet))
        .route("/stamps", get(stubs::stamps))
        .route("/chequebook/address", get(stubs::chequebook_address))
        .route("/chequebook/balance", get(stubs::chequebook_balance))
        .route("/chunks", post(retrieval::upload_chunk))
        .route("/chunks/{addr}", get_or_head(retrieval::chunk))
        .route("/bzz", post(retrieval::upload_bzz))
        .route("/bytes/{addr}", get_or_head(retrieval::bytes))
        // Ant-specific extension (not part of bee Tier-A): list paths in
        // a mantaray manifest. Namespaced under `/v0/` so the bee compat
        // surface stays clean and the demo UI at `vibing.at/ant/` can
        // still introspect manifests.
        .route("/v0/manifest/{addr}", get(retrieval::manifest))
        .route("/bzz/{addr}", get_or_head(retrieval::bzz_root))
        // Trailing-slash root: bee treats `/bzz/<addr>/` as equivalent to
        // `/bzz/<addr>` (both resolve to the manifest's
        // `website-index-document`). Axum's `{*path}` wildcard requires at
        // least one character, so without this explicit route the
        // trailing-slash form falls through to the 501 fallback —
        // breaking, for example, freedom-browser's `bzz://` protocol
        // handler which canonicalises every URL with `new URL(...)` and
        // therefore appends a `/` whenever the source URL had no path.
        .route("/bzz/{addr}/", get_or_head(retrieval::bzz_root))
        .route("/bzz/{addr}/{*path}", get_or_head(retrieval::bzz_with_path))
        .fallback(not_implemented)
        .layer(middleware::from_fn(request_span))
        .with_state(handle)
}

/// Wrap every request in a tracing span carrying the method and target
/// path, and tag the response with the elapsed time once the handler
/// returns. The span keeps each request's logs from interleaving
/// across concurrent connections; the elapsed log gives ops a one-line
/// "did this work?" trace without enabling debug-level on the joiner.
async fn request_span(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let span = tracing::info_span!(
        target: "ant_gateway",
        "http",
        %method,
        %path,
    );
    let started = Instant::now();
    let mut resp = next.run(req).instrument(span.clone()).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let status = resp.status().as_u16();
    let _entered = span.enter();
    resp.headers_mut().insert(
        axum::http::header::SERVER,
        HeaderValue::from_static(SERVER_HEADER),
    );
    if resp.status() == StatusCode::INTERNAL_SERVER_ERROR
        || resp.status() == StatusCode::SERVICE_UNAVAILABLE
        || resp.status() == StatusCode::GATEWAY_TIMEOUT
    {
        tracing::warn!(target: "ant_gateway", %status, %method, %path, elapsed_ms, "request");
    } else {
        tracing::debug!(target: "ant_gateway", %status, %method, %path, elapsed_ms, "request");
    }
    resp
}

/// Same handler for both `GET` and `HEAD`. Each retrieval handler
/// inspects the `Method` extractor itself and emits an empty body for
/// `HEAD` (axum doesn't strip it for us).
fn get_or_head<H, T, S>(handler: H) -> MethodRouter<S>
where
    H: axum::handler::Handler<T, S> + Clone,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    on(MethodFilter::GET.or(MethodFilter::HEAD), handler)
}

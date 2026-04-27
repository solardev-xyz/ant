//! Catch-all 501 handler.
//!
//! Anything that isn't routed to a real handler — Tier-B endpoints we
//! haven't implemented yet, the permanently-501 endpoints in PLAN.md
//! Appendix B (`/pss`, `/transactions`, etc.), unknown paths — lands
//! here. The body shape is the bee-shaped error from
//! [`crate::error::json_error`] so consumers can feature-detect by
//! parsing the JSON instead of brittle string matching.

use axum::http::StatusCode;
use axum::response::Response;

use crate::error::json_error;

/// `{"code": 501, "message": "not implemented in ant"}` with a `501`
/// status. Fixed message rather than echoing the request path because
/// PLAN.md §C.1 pins this exact phrasing as the contract bee consumers
/// branch on.
pub async fn not_implemented() -> Response {
    json_error(StatusCode::NOT_IMPLEMENTED, "not implemented in ant")
}

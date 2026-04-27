//! Bee `jsonhttp` error helper.
//!
//! Bee's HTTP API returns errors as `{"code": <status>, "message": "..."}`.
//! `bee-js` and downstream tooling key off both fields, so we keep the
//! field names and casing verbatim.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Bee-shaped error body.
///
/// `code` is duplicated into the JSON body so consumers parsing the body
/// alone (some browser proxies don't pipe the HTTP status through their
/// JSON callback) can still branch on the same value.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    pub code: u16,
    pub message: String,
}

/// Convenience wrapper: pair a `StatusCode` with a bee-shaped body and
/// turn the whole thing into an `axum::Response` via `IntoResponse`.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorBody,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorBody {
                code: status.as_u16(),
                message: message.into(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

/// Build a bee-shaped error response from the `(status, message)` pair
/// the PLAN.md helper signature calls for.
pub fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    ApiError::new(status, message).into_response()
}

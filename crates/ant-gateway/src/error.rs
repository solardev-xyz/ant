//! Bee `jsonhttp` error helper.
//!
//! Bee's HTTP API returns errors as `{"code": <status>, "message": "..."}`
//! (see `bee/pkg/jsonhttp/jsonhttp.go`), and its parameter-validation
//! failures additionally carry a `reasons` array of `{field, error}`
//! entries (see `Service.mapStructure` in `bee/pkg/api/api.go`).
//! `bee-js` and downstream tooling key off these fields, so we keep the
//! field names and casing verbatim — including bee's
//! `application/json; charset=utf-8` content type.
//!
//! Keep operator-relevant detail (peer addresses, internal failure
//! modes) in logs; the public body should carry bee's message for the
//! condition, nothing more.

use axum::body::Body;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// The exact `Content-Type` bee's `jsonhttp.Respond` writes on every
/// JSON response.
pub const JSON_CONTENT_TYPE: HeaderValue =
    HeaderValue::from_static("application/json; charset=utf-8");

/// One entry of a bee validation-error `reasons` array:
/// `{"field": "...", "error": "..."}`.
#[derive(Debug, Clone, Serialize)]
pub struct Reason {
    pub field: String,
    pub error: String,
}

impl Reason {
    /// The reason bee's validator emits for a missing required
    /// parameter: `want required:` (tag `required` with empty param).
    pub fn required(field: &str) -> Self {
        Self {
            field: field.to_string(),
            error: "want required:".to_string(),
        }
    }

    /// A reason with an explicit error string.
    pub fn new(field: &str, error: impl Into<String>) -> Self {
        Self {
            field: field.to_string(),
            error: error.into(),
        }
    }
}

/// Which parameter group failed validation. Selects bee's message:
/// `invalid header params` / `invalid path params` / `invalid query
/// params`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    Header,
    Path,
    Query,
}

impl ParamKind {
    const fn message(self) -> &'static str {
        match self {
            Self::Header => "invalid header params",
            Self::Path => "invalid path params",
            Self::Query => "invalid query params",
        }
    }
}

/// Bee-shaped error body.
///
/// `code` is duplicated into the JSON body so consumers parsing the body
/// alone (some browser proxies don't pipe the HTTP status through their
/// JSON callback) can still branch on the same value.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    pub code: u16,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<Reason>,
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
                reasons: Vec::new(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::to_vec(&self.body).expect("serialize error body");
        let mut resp = Response::new(Body::from(body));
        *resp.status_mut() = self.status;
        let _ = resp
            .headers_mut()
            .insert(header::CONTENT_TYPE, JSON_CONTENT_TYPE);
        resp
    }
}

/// Build a bee-shaped error response from the `(status, message)` pair
/// the PLAN.md helper signature calls for.
pub fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    ApiError::new(status, message).into_response()
}

/// Bee's `jsonhttp.Respond(w, code, nil)`: the message is Go's
/// `http.StatusText(code)` — e.g. `404 → "Not Found"`. Used where bee
/// passes `nil` as the response (missing `/bytes` roots, generic
/// download misses).
pub fn status_text_error(status: StatusCode) -> Response {
    json_error(status, status.canonical_reason().unwrap_or("error"))
}

/// `503` for endpoints that need the chain-derived wiring
/// ([`crate::GatewayChainState`]) while the daemon's startup Gnosis
/// reads are still resolving. Retryable within seconds; clients poll
/// `/health.chainReady` for the flip. Distinct from the permanent
/// no-RPC-configured behaviors (zero-stubs / `501`) so a transient
/// window never produces a wrong-but-plausible answer (issue #38).
pub fn chain_initializing() -> Response {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "chain init in progress; retry shortly",
    )
}

/// Bee's parameter-validation failure: `400` with
/// `{"code":400,"message":"invalid <kind> params","reasons":[...]}`.
pub fn params_error(kind: ParamKind, reasons: Vec<Reason>) -> Response {
    let status = StatusCode::BAD_REQUEST;
    ApiError {
        status,
        body: ErrorBody {
            code: status.as_u16(),
            message: kind.message().to_string(),
            reasons,
        },
    }
    .into_response()
}

/// Decode a fixed-length hex parameter the way bee's `mapStructure`
/// hooks do, collecting a bee-shaped [`Reason`] into `reasons` on
/// failure (Go `encoding/hex` error strings: `invalid hex byte: U+007A
/// 'z'` / `odd length hex string`). Accepts an optional `0x`/`0X`
/// prefix as a superset of bee for compatibility with existing ant
/// clients. Returns `None` when a reason was recorded.
pub fn parse_hex_param<const N: usize>(
    field: &str,
    value: &str,
    reasons: &mut Vec<Reason>,
) -> Option<[u8; N]> {
    let stripped = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    if let Some(bad) = stripped.chars().find(|c| !c.is_ascii_hexdigit()) {
        reasons.push(Reason::new(field, go_invalid_hex_byte(bad)));
        return None;
    }
    if !stripped.len().is_multiple_of(2) {
        reasons.push(Reason::new(field, "odd length hex string"));
        return None;
    }
    if stripped.len() != N * 2 {
        // Bee has no direct analogue (its swarm addresses accept any
        // length and fail later); a fixed-size ant parameter reports the
        // expected size in the same reasons shape.
        reasons.push(Reason::new(
            field,
            format!("invalid length: want {N} bytes"),
        ));
        return None;
    }
    let mut out = [0u8; N];
    if hex::decode_to_slice(stripped, &mut out).is_err() {
        reasons.push(Reason::new(field, "odd length hex string"));
        return None;
    }
    Some(out)
}

/// Format an invalid hex byte like Go's `fmt.Sprintf("invalid hex
/// byte: %#U", b)` — e.g. `invalid hex byte: U+007A 'z'`.
pub(crate) fn go_invalid_hex_byte(c: char) -> String {
    format!("invalid hex byte: U+{:04X} '{}'", c as u32, c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_are_omitted_when_empty() {
        let body = ErrorBody {
            code: 404,
            message: "Not Found".into(),
            reasons: Vec::new(),
        };
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(json, r#"{"code":404,"message":"Not Found"}"#);
    }

    #[test]
    fn params_error_matches_bee_shape() {
        let body = ErrorBody {
            code: 400,
            message: ParamKind::Header.message().into(),
            reasons: vec![Reason::required("swarm-postage-batch-id")],
        };
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(
            json,
            r#"{"code":400,"message":"invalid header params","reasons":[{"field":"swarm-postage-batch-id","error":"want required:"}]}"#
        );
    }

    #[test]
    fn hex_reason_uses_go_format() {
        let mut reasons = Vec::new();
        assert!(parse_hex_param::<32>("address", "zznothex", &mut reasons).is_none());
        assert_eq!(reasons[0].error, "invalid hex byte: U+007A 'z'");
        assert_eq!(reasons[0].field, "address");
    }

    #[test]
    fn hex_param_roundtrips() {
        let mut reasons = Vec::new();
        let v = parse_hex_param::<4>("id", "0xdeadbeef", &mut reasons).unwrap();
        assert_eq!(v, [0xde, 0xad, 0xbe, 0xef]);
        assert!(reasons.is_empty());
    }
}

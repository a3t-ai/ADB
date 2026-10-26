//! Optional shared-token auth (W3, G-L1): with `ADB_AUTH_TOKEN` set, every HTTP
//! endpoint but `/health` and every Arrow Flight call needs
//! `Authorization: Bearer <token>`. Unset, ADB is open as before (local dev, CE).
//! One token per ADB; per-tenant credentials come with the control plane (M4).

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The token from `ADB_AUTH_TOKEN`, if set and not empty.
pub fn token_from_env() -> Option<Arc<str>> {
    std::env::var("ADB_AUTH_TOKEN").ok().filter(|t| !t.is_empty()).map(Arc::from)
}

/// Whether an `Authorization` value carries `token` (constant time in its length).
pub fn authorized(value: Option<&str>, token: &str) -> bool {
    let Some(given) = value.and_then(|v| v.strip_prefix("Bearer ")) else { return false };
    let (a, b) = (given.as_bytes(), token.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// HTTP middleware: `/health` stays open (probes); everything else needs the token.
pub async fn require_token(token: Arc<str>, req: Request, next: Next) -> Response {
    if req.uri().path() == "/health" {
        return next.run(req).await;
    }
    let value = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    if authorized(value, &token) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({ "success": false, "error": "unauthorized: ADB needs Authorization: Bearer <token>" })))
            .into_response()
    }
}

/// Flight (gRPC) interceptor: the same check on the `authorization` metadata.
#[derive(Clone)]
pub struct FlightAuth(pub Option<Arc<str>>);

impl tonic::service::Interceptor for FlightAuth {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let Some(token) = &self.0 else { return Ok(req) };
        let value = req.metadata().get("authorization").and_then(|v| v.to_str().ok());
        if authorized(value, token) {
            Ok(req)
        } else {
            Err(tonic::Status::unauthenticated("ADB needs authorization: Bearer <token>"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::authorized;

    #[test]
    fn bearer_must_match_exactly() {
        assert!(authorized(Some("Bearer s3cret"), "s3cret"));
        assert!(!authorized(Some("Bearer s3cre"), "s3cret"));
        assert!(!authorized(Some("Bearer s3cretx"), "s3cret"));
        assert!(!authorized(Some("s3cret"), "s3cret"));
        assert!(!authorized(None, "s3cret"));
    }
}

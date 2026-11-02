//! Optional shared-token auth (W3, G-L1): with `ADB_AUTH_TOKEN` set, every HTTP
//! endpoint but `/health` and every Arrow Flight call needs
//! `Authorization: Bearer <token>`. Unset, ADB is open as before (local dev, CE).
//! One token per ADB; per-tenant credentials come with the control plane (M4).
//!
//! FlowR's engine store (stage H, H8) has a token of its own, `ADB_ENGINE_TOKEN`:
//! it opens only the engine's Flight actions, and the memory token doesn't open
//! them. With neither token set, everything is open (local dev, CE).

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The token from `ADB_AUTH_TOKEN`, if set and not empty.
pub fn token_from_env() -> Option<Arc<str>> {
    std::env::var("ADB_AUTH_TOKEN").ok().filter(|t| !t.is_empty()).map(Arc::from)
}

/// The engine store's token from `ADB_ENGINE_TOKEN`, if set and not empty.
pub fn engine_token_from_env() -> Option<Arc<str>> {
    std::env::var("ADB_ENGINE_TOKEN").ok().filter(|t| !t.is_empty()).map(Arc::from)
}

/// What a Flight call's token opens (set on the request by [`FlightAuth`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// No token configured: everything.
    Open,
    /// The memory token: memory, platform tables, AQL — not the engine.
    Memory,
    /// The engine token: FlowR's engine store only.
    Engine,
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

/// Flight (gRPC) interceptor: the same check on the `authorization` metadata,
/// for the memory token or the engine token; the request carries which
/// ([`Access`]).
#[derive(Clone)]
pub struct FlightAuth {
    pub memory: Option<Arc<str>>,
    pub engine: Option<Arc<str>>,
}

impl tonic::service::Interceptor for FlightAuth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let access = if self.memory.is_none() && self.engine.is_none() {
            Access::Open
        } else {
            let value = req.metadata().get("authorization").and_then(|v| v.to_str().ok());
            if self.memory.as_deref().is_some_and(|t| authorized(value, t)) {
                Access::Memory
            } else if self.engine.as_deref().is_some_and(|t| authorized(value, t)) {
                Access::Engine
            } else {
                return Err(tonic::Status::unauthenticated("ADB needs authorization: Bearer <token>"));
            }
        };
        req.extensions_mut().insert(access);
        Ok(req)
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

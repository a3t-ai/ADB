//! Optional shared-token auth (W3, G-L1): with `ADB_AUTH_TOKEN` set, every HTTP
//! endpoint but `/health` and every Arrow Flight call needs
//! `Authorization: Bearer <token>`. Unset, ADB is open as before (local dev, CE).
//! One token per ADB; per-tenant credentials come with the control plane (M4).
//!
//! FlowR's engine store (stage H, H8) has a token of its own, `ADB_ENGINE_TOKEN`:
//! it opens only the engine's Flight actions, and the memory token doesn't open
//! them. With neither token set, everything is open (local dev, CE).
//!
//! Pool tokens (A3T cluster, gateway step 1 / AC-8): `ADB_ENGINE_POOL_TOKENS`, a
//! JSON object `{"<pool>": "<token>", …}`. A pool token opens the engine store
//! like the engine token, but its holder claims runs and follows task notices
//! of its own pool only, and cannot run the console's operations (purge,
//! outbox, deciding approvals, restore). `ADB_ENGINE_TOKEN` stays the console's
//! token for every pool.

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

/// The pool tokens from `ADB_ENGINE_POOL_TOKENS` (`{"pool": "token"}`).
pub fn pool_tokens_from_env() -> Vec<(Arc<str>, Arc<str>)> {
    let Some(raw) = std::env::var("ADB_ENGINE_POOL_TOKENS").ok().filter(|t| !t.trim().is_empty()) else { return Vec::new() };
    match serde_json::from_str::<std::collections::BTreeMap<String, String>>(&raw) {
        Ok(m) => m.into_iter().filter(|(p, t)| !p.is_empty() && !t.is_empty()).map(|(p, t)| (Arc::from(p), Arc::from(t))).collect(),
        Err(e) => {
            tracing::error!(error = %e, "ADB_ENGINE_POOL_TOKENS is not a JSON object of pool → token; no pool tokens");
            Vec::new()
        }
    }
}

/// On a request made with a pool token: the pool it is limited to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnginePool(pub Arc<str>);

/// What a Flight call's token opens (set on the request by [`FlightAuth`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// No token configured: everything.
    Open,
    /// The memory token: memory, platform tables, AQL — not the engine.
    Memory,
    /// The engine token: FlowR's engine store only.
    Engine,
    /// A scoped token (DB-3): its namespaces only; the request carries its [`Scope`].
    Scoped,
}

/// Whether an `Authorization` value carries `token` (constant time in its length).
pub fn authorized(value: Option<&str>, token: &str) -> bool {
    let Some(given) = value.and_then(|v| v.strip_prefix("Bearer ")) else { return false };
    let (a, b) = (given.as_bytes(), token.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// HTTP middleware: `/health` stays open (probes); everything else needs the token.
pub async fn require_token(token: Option<Arc<str>>, scopes: Arc<Vec<Arc<crate::scope::Scope>>>, mut req: Request, next: Next) -> Response {
    // `/metrics` too when ADB_METRICS_OPEN is set (a scraper inside the cluster, OB-1).
    // The console page (standalone build only) holds no data; it asks for the token.
    if req.uri().path() == "/health" || (req.uri().path() == "/metrics" && metrics_open()) || (cfg!(feature = "console") && req.uri().path() == "/ui") {
        return next.run(req).await;
    }
    let value = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    if token.as_deref().is_some_and(|t| authorized(value, t)) {
        next.run(req).await
    } else if let Some(scope) = scopes.iter().find(|s| authorized(value, &s.token)) {
        // DB-3: only `/query`, which checks each statement; nothing else stays in a namespace.
        if req.uri().path() != "/query" {
            adb_core::metrics::get().auth_failures.with_label_values(&["http", "scope"]).inc();
            let error = format!("{} is not open to the {} token", req.uri().path(), scope.name);
            return (StatusCode::FORBIDDEN, axum::Json(serde_json::json!({ "success": false, "code": "forbidden", "error": error }))).into_response();
        }
        req.extensions_mut().insert(Arc::clone(scope));
        next.run(req).await
    } else {
        let reason = if value.is_some() { "bad_token" } else { "no_token" };
        adb_core::metrics::get().auth_failures.with_label_values(&["http", reason]).inc();
        (StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({ "success": false, "code": "unauthorized", "error": "unauthorized: ADB needs Authorization: Bearer <token>" })))
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
    /// Pool tokens: (pool, token).
    pub pools: Vec<(Arc<str>, Arc<str>)>,
    /// Scoped tokens (DB-3).
    pub scopes: Vec<Arc<crate::scope::Scope>>,
}

impl tonic::service::Interceptor for FlightAuth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let access = if self.memory.is_none() && self.engine.is_none() && self.pools.is_empty() && self.scopes.is_empty() {
            Access::Open
        } else {
            let value = req.metadata().get("authorization").and_then(|v| v.to_str().ok());
            if self.memory.as_deref().is_some_and(|t| authorized(value, t)) {
                Access::Memory
            } else if self.engine.as_deref().is_some_and(|t| authorized(value, t)) {
                Access::Engine
            } else if let Some((pool, _)) = self.pools.iter().find(|(_, t)| authorized(value, t)) {
                req.extensions_mut().insert(EnginePool(Arc::clone(pool)));
                Access::Engine
            } else if let Some(scope) = self.scopes.iter().find(|s| authorized(value, &s.token)) {
                req.extensions_mut().insert(Arc::clone(scope));
                Access::Scoped
            } else {
                let reason = if value.is_some() { "bad_token" } else { "no_token" };
                adb_core::metrics::get().auth_failures.with_label_values(&["flight", reason]).inc();
                return Err(tonic::Status::unauthenticated("ADB needs authorization: Bearer <token>"));
            }
        };
        req.extensions_mut().insert(access);
        Ok(req)
    }
}

/// What a pool token may do with one engine call: `action` is `engine` or
/// `engine_subscribe`, `body` its JSON (`{op, args}` or `{topic}`).
pub fn pool_allows(pool: &str, action: &str, body: &serde_json::Value) -> Result<(), String> {
    match action {
        "engine_subscribe" => {
            let topic = body.get("topic").and_then(|t| t.as_str()).unwrap_or_default();
            match topic.strip_prefix("tasks:") {
                Some(p) if p != pool => Err(format!("the {pool} pool's token follows tasks:{pool} only, not {topic}")),
                _ => Ok(()),
            }
        }
        "engine" => {
            let op = body.get("op").and_then(|o| o.as_str()).unwrap_or_default();
            match op {
                "claim_runs" => {
                    let wanted = body.pointer("/args/pool").and_then(|p| p.as_str()).unwrap_or_default();
                    if wanted == pool { Ok(()) } else { Err(format!("the {pool} pool's token claims from {pool} only, not {wanted}")) }
                }
                "purge_finished" | "outbox" | "outbox_done" | "decide_approval" => Err(format!("{op} is the console's (the engine token), not the {pool} pool's")),
                _ => Ok(()),
            }
        }
        other => Err(format!("{other} is not open to the {pool} pool's token")),
    }
}

#[cfg(test)]
mod tests {
    use super::authorized;

    #[test]
    fn a_pool_token_claims_and_follows_its_own_pool_only() {
        use serde_json::json;
        let ok = |a, b| super::pool_allows("worker", a, &b).is_ok();
        assert!(ok("engine", json!({"op": "claim_runs", "args": {"pool": "worker"}})));
        assert!(!ok("engine", json!({"op": "claim_runs", "args": {"pool": "gateway"}})));
        assert!(ok("engine", json!({"op": "start", "args": {}})));
        assert!(ok("engine", json!({"op": "append", "args": {}})));
        assert!(!ok("engine", json!({"op": "decide_approval", "args": {}})));
        assert!(!ok("engine", json!({"op": "purge_finished", "args": {}})));
        assert!(ok("engine_subscribe", json!({"topic": "tasks:worker"})));
        assert!(!ok("engine_subscribe", json!({"topic": "tasks:default"})));
        assert!(ok("engine_subscribe", json!({"topic": "run:0193"})));
        assert!(!ok("engine_restore", json!({})));
    }

    #[test]
    fn bearer_must_match_exactly() {
        assert!(authorized(Some("Bearer s3cret"), "s3cret"));
        assert!(!authorized(Some("Bearer s3cre"), "s3cret"));
        assert!(!authorized(Some("Bearer s3cretx"), "s3cret"));
        assert!(!authorized(Some("s3cret"), "s3cret"));
        assert!(!authorized(None, "s3cret"));
    }
}

/// `ADB_METRICS_OPEN=1`: `/metrics` needs no token (no secrets in it; for a scraper).
fn metrics_open() -> bool {
    static OPEN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *OPEN.get_or_init(|| std::env::var("ADB_METRICS_OPEN").is_ok_and(|v| v == "1" || v == "true"))
}

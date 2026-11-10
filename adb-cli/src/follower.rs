//! A replica set follower's serving side (C-16, AWS F-20).
//!
//! - **Reads** (RECALL, LOOKUP, SCAN over HTTP `POST /query` and Flight `aql`; Flight
//!   table and SQL reads) run here, on the follower's copy, as fresh as on the leader:
//!   the leader's ReadIndex, applied here first ([`FollowerEngine::fresh`]).
//! - **Everything else over HTTP** is passed through to the leader, streams included,
//!   so a client can send any request to any node.
//! - **Everything else over Flight** answers `unavailable: this ADB node is not the
//!   leader`; FlowR sends writes to the leader and moves on when a node says that.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adb_executor::{Engine, FollowerReads, NOT_LEADER};
use adb_raft::Node;
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

pub struct FollowerEngine {
    pub node: Arc<Node>,
    pub reads: FollowerReads,
}

impl FollowerEngine {
    /// The WAL offset a read here must wait for: the leader's commit index, applied
    /// by this node's state machine (its WAL then holds it).
    async fn fresh(&self) -> Result<adb_core::log::LogOffset, String> {
        let index = self.node.read_index().await.map_err(|e| format!("{NOT_LEADER} and could not reach the leader: {e}"))?;
        self.node.wait_applied(index, Duration::from_secs(5)).await?;
        Ok(self.reads.log_tip())
    }
}

#[async_trait]
impl Engine for FollowerEngine {
    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        if !adb_executor::follower_reads(aql) {
            return Err(NOT_LEADER.into());
        }
        let upto = self.fresh().await?;
        self.reads.query(aql, variables, upto).await
    }

    async fn stats(&self) -> Result<Value, String> {
        Err(NOT_LEADER.into())
    }

    fn platform(&self) -> Option<Arc<adb_platform::PlatformStore>> {
        Some(Arc::clone(self.reads.platform()))
    }

    fn read_only(&self) -> bool {
        true
    }

    async fn read_fresh(&self) -> Result<(), String> {
        let upto = self.fresh().await?;
        self.reads.caught_up(upto, Duration::from_secs(10)).await
    }

    async fn sql(&self, sql: &str, limit: Option<usize>) -> Result<adb_executor::sql::SqlResult, String> {
        let upto = self.fresh().await?;
        self.reads.sql(sql, limit, upto).await
    }
}

#[derive(Clone)]
pub struct Proxy {
    pub node: Arc<Node>,
    pub port: u16,
    pub http: reqwest::Client,
    /// The ADB router over [`FollowerEngine`]: auth, scopes and answers as the leader's.
    pub local: axum::Router,
    /// Whether reads run here (`ADB_FOLLOWER_READS`, default on).
    pub reads: bool,
    /// `ADB_PEERS_HTTP` (`1=http://a:3000,…`): each node's HTTP address, when not the
    /// host of its Raft endpoint on this node's HTTP port (several nodes on one host).
    pub http_peers: Option<std::collections::BTreeMap<u64, String>>,
}

/// Largest request body a follower looks into (a query); larger ones go to the leader.
const MAX_LOOK: usize = 16 * 1024 * 1024;

/// Every request: a read query here, `/health` and the metrics here, the rest to the leader.
pub async fn route(State(p): State<Proxy>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    if matches!(path.as_str(), "/health" | "/metrics" | "/admin/stats/metrics") {
        return serve_local(&p, req).await;
    }
    if path == "/ready" {
        return (StatusCode::SERVICE_UNAVAILABLE, "follower").into_response();
    }
    if p.reads && req.method() == axum::http::Method::POST && path == "/query" {
        let (parts, body) = req.into_parts();
        let bytes = match axum::body::to_bytes(body, MAX_LOOK).await {
            Ok(b) => b,
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        };
        let read = serde_json::from_slice::<Value>(&bytes).ok().and_then(|v| v.get("query").and_then(|q| q.as_str()).map(adb_executor::follower_reads)).unwrap_or(false);
        let req = Request::from_parts(parts, Body::from(bytes));
        return if read { serve_local(&p, req).await } else { to_leader(&p, req).await };
    }
    to_leader(&p, req).await
}

async fn serve_local(p: &Proxy, req: Request) -> Response {
    use tower::ServiceExt;
    match p.local.clone().oneshot(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    }
}

/// Pass `req` to the leader's HTTP port and its answer back, streamed (SSE included).
async fn to_leader(p: &Proxy, req: Request) -> Response {
    let base = match &p.http_peers {
        Some(m) => p.node.leader().and_then(|l| m.get(&l).cloned()),
        None => p.node.leader_at(p.port),
    };
    let Some(base) = base else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({ "error": NOT_LEADER, "leader": null }))).into_response();
    };
    let (parts, body) = req.into_parts();
    let url = format!("{base}{}", parts.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));
    let mut out = p.http.request(parts.method.clone(), &url);
    for (k, v) in parts.headers.iter() {
        if !matches!(k.as_str(), "host" | "connection" | "content-length" | "transfer-encoding") {
            out = out.header(k, v);
        }
    }
    let out = out.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    match out.send().await {
        Ok(r) => {
            let mut resp = Response::builder().status(r.status().as_u16());
            for (k, v) in r.headers().iter() {
                if !matches!(k.as_str(), "connection" | "content-length" | "transfer-encoding") {
                    resp = resp.header(k.as_str(), v.as_bytes());
                }
            }
            resp.body(Body::from_stream(r.bytes_stream())).unwrap_or_else(|e| (StatusCode::BAD_GATEWAY, e.to_string()).into_response())
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({ "error": format!("{NOT_LEADER}; the leader did not answer: {e}"), "leader": p.node.leader() }))).into_response(),
    }
}

/// No kept-alive connection to a follower: once this node leads, a client's pooled
/// connection would still reach the follower's router.
pub async fn close(mut r: Response) -> Response {
    r.headers_mut().insert(axum::http::header::CONNECTION, HeaderValue::from_static("close"));
    r
}

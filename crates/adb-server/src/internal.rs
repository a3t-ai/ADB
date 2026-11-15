//! Step 4: calls between cluster nodes over HTTP ([`HttpPeers`] and the routes that
//! answer them). They sit behind the same token as every other endpoint (a scoped
//! token never reaches them: `require_token` opens only `/query` to one).
//!
//! ```text
//! POST /internal/query            {aql, variables?, reach} → {ok: <answer>} | {err}
//! POST /internal/partition/{p}    PartCall                 → {ok: PartReply} | {err}
//! POST /internal/replicate/{p}    ReplicateReq (msgpack)   → ReplicateAck (msgpack) | 409 text
//! ```
//!
//! A node runs what it owns and answers `not_owner:…` otherwise; it never sends a
//! call on ([`PartitionedEngine::serve_query`], [`PartitionedEngine::serve_part`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adb_cluster::map::Partition;
use adb_cluster::routing::Reach;
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::{PartCall, PartReply, Peers};
use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize)]
struct QueryCall {
    aql: String,
    #[serde(default)]
    variables: Option<HashMap<String, Value>>,
    reach: Reach,
}

fn answer<T: Serialize>(r: Result<T, String>) -> Json<Value> {
    Json(match r {
        Ok(v) => json!({ "ok": v }),
        Err(e) => json!({ "err": e }),
    })
}

async fn query(State(node): State<Arc<PartitionedEngine>>, Json(c): Json<QueryCall>) -> Json<Value> {
    answer(node.serve_query(&c.aql, c.variables.as_ref(), &c.reach).await)
}

async fn partition(State(node): State<Arc<PartitionedEngine>>, Path(p): Path<Partition>, Json(call): Json<PartCall>) -> Json<Value> {
    answer(node.serve_part(p, call).await)
}

/// An owner's records for a partition this node replicates (step 5): MessagePack both
/// ways (the write path); a refusal (`fenced`, `gap`, `not_replica`, `replica_full`) is
/// 409 with its text.
async fn replicate(State(node): State<Arc<PartitionedEngine>>, Path(p): Path<Partition>, body: axum::body::Bytes) -> axum::response::Response {
    use axum::response::IntoResponse;
    let req: adb_executor::replication::ReplicateReq = match rmp_serde::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return (axum::http::StatusCode::BAD_REQUEST, format!("bad replication call: {e}")).into_response(),
    };
    match node.serve_replicate(p, req) {
        Ok(ack) => match rmp_serde::to_vec_named(&ack) {
            Ok(b) => ([(axum::http::header::CONTENT_TYPE, "application/msgpack")], b).into_response(),
            Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        Err(e) => (axum::http::StatusCode::CONFLICT, e).into_response(),
    }
}

/// The internal routes of a cluster node.
pub fn routes(node: Arc<PartitionedEngine>) -> Router {
    Router::new()
        .route("/internal/query", post(query))
        .route("/internal/partition/:p", post(partition))
        .route("/internal/replicate/:p", post(replicate))
        .with_state(node)
}

/// [`Peers`] over HTTP, with this cluster's token.
pub struct HttpPeers {
    http: reqwest::Client,
    token: Option<Arc<str>>,
}

impl HttpPeers {
    pub fn new(token: Option<Arc<str>>) -> Self {
        let http = reqwest::Client::builder().connect_timeout(Duration::from_secs(2)).timeout(Duration::from_secs(120)).pool_idle_timeout(Duration::from_secs(60)).build().expect("HTTP client");
        Self { http, token }
    }

    async fn post<T: serde::de::DeserializeOwned>(&self, url: String, body: &impl Serialize) -> Result<T, String> {
        let mut req = self.http.post(&url).json(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let r = req.send().await.map_err(|e| format!("{url}: {e}"))?;
        let status = r.status();
        let v: Value = r.json().await.map_err(|e| format!("{url}: {status}: {e}"))?;
        match (v.get("ok"), v.get("err").and_then(|e| e.as_str())) {
            (_, Some(e)) => Err(e.to_string()),
            (Some(ok), None) => serde_json::from_value(ok.clone()).map_err(|e| format!("{url}: {e}")),
            (None, None) => Err(format!("{url}: {status}: {v}")),
        }
    }
}

#[async_trait]
impl Peers for HttpPeers {
    async fn query(&self, addr: &str, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let call = QueryCall { aql: aql.into(), variables: variables.cloned(), reach: reach.clone() };
        self.post(format!("{}/internal/query", addr.trim_end_matches('/')), &call).await
    }

    async fn part(&self, addr: &str, partition: Partition, call: PartCall) -> Result<PartReply, String> {
        self.post(format!("{}/internal/partition/{partition}", addr.trim_end_matches('/')), &call).await
    }

    async fn replicate(&self, addr: &str, partition: Partition, req: adb_executor::replication::ReplicateReq) -> Result<adb_executor::replication::ReplicateAck, String> {
        let url = format!("{}/internal/replicate/{partition}", addr.trim_end_matches('/'));
        let body = rmp_serde::to_vec_named(&req).map_err(|e| e.to_string())?;
        let mut r = self.http.post(&url).header("content-type", "application/msgpack").body(body);
        if let Some(t) = &self.token {
            r = r.bearer_auth(t);
        }
        let r = r.send().await.map_err(|e| format!("{url}: {e}"))?;
        let status = r.status();
        let bytes = r.bytes().await.map_err(|e| format!("{url}: {e}"))?;
        if !status.is_success() {
            return Err(String::from_utf8_lossy(&bytes).to_string());
        }
        rmp_serde::from_slice(&bytes).map_err(|e| format!("{url}: {e}"))
    }
}

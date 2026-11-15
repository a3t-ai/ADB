//! Step 4: calls between cluster nodes over HTTP ([`HttpPeers`] and the routes that
//! answer them). They sit behind the same token as every other endpoint (a scoped
//! token never reaches them: `require_token` opens only `/query` to one).
//!
//! ```text
//! POST /internal/query            {aql, variables?, reach} → {ok: <answer>} | {err}
//! POST /internal/partition/{p}    PartCall                 → {ok: PartReply} | {err}
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

/// The internal routes of a cluster node.
pub fn routes(node: Arc<PartitionedEngine>) -> Router {
    Router::new().route("/internal/query", post(query)).route("/internal/partition/:p", post(partition)).with_state(node)
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
}

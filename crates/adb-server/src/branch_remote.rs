//! Branches in their own pod (`.ai/BRANCHING.md` B6), over Arrow Flight:
//!
//! - [`FlightRemote`]: the org's way to a branch pod — run a statement (`aql`), erase
//!   forgotten lineages (`branch_forget`), read its view (`branch_parent_rows`).
//! - [`FlightParentSource`]: a branch's (or org's) rows `AS OF` a time, read from
//!   another ADB's `branch_parent_rows` — a pod reads its parent this way.
//!
//! Every call carries `authorization: Bearer <token>` when a token is given.

use std::sync::Arc;

use adb_core::log::CommitTs;
use adb_core::{AdbError, AdbResult, MemoryType};
use adb_executor::branch_engine::{BranchRemote, ParentSource};
use adb_store::StoredRow;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use tonic::transport::Channel;

/// One Flight action on `url`, its JSON answer.
pub async fn flight_action(url: &str, token: Option<&str>, kind: &str, body: Value) -> Result<Value, String> {
    let channel = Channel::from_shared(url.to_string())
        .map_err(|e| format!("{url}: {e}"))?
        .connect_timeout(std::time::Duration::from_secs(3))
        .timeout(std::time::Duration::from_secs(60))
        .connect()
        .await
        .map_err(|e| format!("connect {url}: {e}"))?;
    let mut client = FlightServiceClient::new(channel).max_decoding_message_size(256 << 20).max_encoding_message_size(64 << 20);
    let mut req = tonic::Request::new(Action { r#type: kind.into(), body: body.to_string().into() });
    if let Some(t) = token.filter(|t| !t.is_empty()) {
        let v = format!("Bearer {t}").parse().map_err(|_| "the token is not a valid header value".to_string())?;
        req.metadata_mut().insert("authorization", v);
    }
    let mut s = client.do_action(req).await.map_err(|e| format!("{kind} on {url}: {}", e.message()))?.into_inner();
    let r = s.next().await.ok_or(format!("{kind} on {url}: no answer"))?.map_err(|e| format!("{kind} on {url}: {}", e.message()))?;
    serde_json::from_slice(&r.body).map_err(|e| format!("{kind} on {url}: {e}"))
}

/// Rows of a namespace `AS OF` a time, from the ADB at `url` (its own view, or that
/// of its hosted branch `branch`).
pub struct FlightParentSource {
    pub url: String,
    pub token: Option<String>,
    pub branch: Option<String>,
}

#[async_trait]
impl ParentSource for FlightParentSource {
    async fn rows_as_of(&self, memory: MemoryType, namespace: &str, at: CommitTs) -> AdbResult<Vec<StoredRow>> {
        let body = json!({ "memory": memory, "namespace": namespace, "branch": self.branch, "at": at.0 });
        let v = flight_action(&self.url, self.token.as_deref(), "branch_parent_rows", body).await.map_err(|e| AdbError::Internal(format!("the branch's parent: {e}")))?;
        serde_json::from_value(v.get("rows").cloned().unwrap_or_default()).map_err(|e| AdbError::Internal(format!("the branch's parent's rows: {e}")))
    }
}

/// The org's way to branches in their own pods.
pub struct FlightRemote {
    pub token: Option<String>,
}

#[async_trait]
impl BranchRemote for FlightRemote {
    async fn query(&self, url: &str, aql: &str, variables: Option<Value>) -> Result<Value, String> {
        let v = flight_action(url, self.token.as_deref(), "aql", json!({ "query": aql, "variables": variables })).await?;
        match v.get("success").and_then(|s| s.as_bool()) {
            Some(true) => Ok(v.get("data").cloned().unwrap_or(Value::Null)),
            _ => Err(v.get("error").and_then(|e| e.as_str()).unwrap_or("the branch's pod refused the statement").to_string()),
        }
    }

    async fn forget(&self, url: &str, memory: MemoryType, lineages: &[String]) -> Result<u64, String> {
        let v = flight_action(url, self.token.as_deref(), "branch_forget", json!({ "memory": memory, "lineages": lineages })).await?;
        Ok(v.get("erased").and_then(|n| n.as_u64()).unwrap_or(0))
    }

    fn source(&self, url: &str, branch: &str) -> Arc<dyn ParentSource> {
        Arc::new(FlightParentSource { url: url.to_string(), token: self.token.clone(), branch: Some(branch.to_string()) })
    }
}

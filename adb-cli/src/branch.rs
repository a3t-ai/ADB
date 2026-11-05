//! A memory branch in its own pod (`.ai/BRANCHING.md` B6): this ADB process is one
//! branch's engine — its own WAL, sequencer and table — reading its parent (the org's
//! ADB) over Arrow Flight `AS OF` the branch point.
//!
//! - `ADB_BRANCH_OF`: the org ADB's Flight URL (`http://adb.a3t.svc.cluster.local:3001`)
//! - `ADB_BRANCH`: `<namespace>/<branch name>` (created on the org first, `branch_create`)
//! - `ADB_BRANCH_TOKEN`: the org's token (default `ADB_AUTH_TOKEN`)
//!
//! On start it reads its branch record from the org, opens its engine, erases the
//! lineages of FORGETs on main it missed while it was down, then serves. The org sends
//! `BRANCH` statements here once the branch is placed (`branch_place {url}`).

use std::sync::Arc;

use adb_executor::branch_engine::ParentView;
use adb_executor::{Engine, LocalEngine};
use adb_server::branch_remote::{flight_action, FlightParentSource, FlightRemote};
use serde_json::json;

pub struct BranchConfig {
    pub org: String,
    pub token: Option<String>,
    pub namespace: String,
    pub name: String,
}

pub fn config() -> Result<Option<BranchConfig>, String> {
    let Some(org) = std::env::var("ADB_BRANCH_OF").ok().filter(|v| !v.is_empty()) else { return Ok(None) };
    let spec = std::env::var("ADB_BRANCH").map_err(|_| "ADB_BRANCH_OF needs ADB_BRANCH=<namespace>/<branch>".to_string())?;
    let (namespace, name) = spec.rsplit_once('/').ok_or_else(|| format!("ADB_BRANCH {spec:?}: use <namespace>/<branch>"))?;
    let token = std::env::var("ADB_BRANCH_TOKEN").or_else(|_| std::env::var("ADB_AUTH_TOKEN")).ok().filter(|t| !t.is_empty());
    Ok(Some(BranchConfig { org, token, namespace: namespace.into(), name: name.into() }))
}

/// How an org (or this pod) reaches branch pods: Flight with this ADB's own token.
pub fn remote() -> Arc<FlightRemote> {
    Arc::new(FlightRemote { token: std::env::var("ADB_AUTH_TOKEN").ok().filter(|t| !t.is_empty()) })
}

pub async fn open(dir: &std::path::Path, c: BranchConfig) -> Result<Arc<dyn Engine>, Box<dyn std::error::Error>> {
    let list = flight_action(&c.org, c.token.as_deref(), "branch_list", json!({ "namespace": c.namespace })).await?;
    let branches: Vec<adb_executor::branches::Branch> = serde_json::from_value(list.get("branches").cloned().unwrap_or_default())?;
    let b = branches.into_iter().find(|b| b.name == c.name).ok_or_else(|| format!("the org has no branch {:?} in namespace {:?}", c.name, c.namespace))?;
    // The parent is main, or another branch hosted in the org's process.
    let parent_branch = (b.parent != adb_executor::branches::MAIN).then(|| b.parent.clone());
    let source = Arc::new(FlightParentSource { url: c.org.clone(), token: c.token.clone(), branch: parent_branch });
    let view = ParentView { source, namespace: b.namespace.clone(), branch: b.name.clone(), point: b.point() };
    let (engine, replayed) = LocalEngine::durable_branch(dir, view).await?;
    tracing::info!(branch = %b.id, org = %c.org, point_ts = b.point_ts, records = replayed.records, "ADB branch engine open (own pod)");
    // DB-20: a new pod's first record names where the branch came from.
    adb_executor::branch_engine::write_origin(&engine, &b).await?;
    // FORGETs on main while this pod was down: erased before it serves.
    let pending = flight_action(&c.org, c.token.as_deref(), "branch_pending", json!({ "namespace": c.namespace, "name": c.name })).await?;
    let forgets: Vec<adb_executor::branches::PendingForget> = serde_json::from_value(pending.get("forgets").cloned().unwrap_or_default())?;
    if !forgets.is_empty() {
        let mut erased = 0;
        for f in &forgets {
            erased += engine.forget_lineages(f.memory, &f.lineages).await?;
        }
        flight_action(&c.org, c.token.as_deref(), "branch_pending_ack", json!({ "namespace": c.namespace, "name": c.name, "count": forgets.len() })).await?;
        tracing::info!(forgets = forgets.len(), erased, "pending FORGETs on main applied");
    }
    Ok(Arc::new(engine))
}

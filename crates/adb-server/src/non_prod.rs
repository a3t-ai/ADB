//! The non-prod ADB pod (DB-2, decision D4): one process hosting every dev and QA branch
//! engine. Each branch keeps its own WAL, sequencer and table under this pod's data dir
//! and reads its parent from the prod cluster over Flight, so dev and QA compute stays off
//! prod and their writes never reach a prod sequencer.
//!
//! - `ADB_NON_PROD_OF`: the prod ADB's Flight URL
//! - `ADB_NON_PROD_URL`: this pod's own Flight URL, as `branch_place` recorded it
//! - `ADB_NON_PROD_TOKEN`: its token on prod (a read-only scoped token, DB-3)
//!
//! It has no main of its own: a statement must name a branch placed here. Branches are
//! created and placed on prod; a branch opens here on first use, and every placed branch
//! opens at start, applying the FORGETs it missed before it serves.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use adb_core::MemoryType;
use adb_executor::branch_engine::{statement_branch, write_origin, ParentView};
use adb_executor::branches::{Branch, PendingForget, MAIN};
use adb_executor::{Engine, LocalEngine};
use async_trait::async_trait;
use serde_json::{json, Value};

use crate::branch_remote::{flight_action, FlightParentSource};

#[derive(Debug, Clone)]
pub struct NonProdConfig {
    pub org: String,
    pub own_url: String,
    pub token: Option<String>,
}

impl NonProdConfig {
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(org) = std::env::var("ADB_NON_PROD_OF").ok().filter(|v| !v.is_empty()) else { return Ok(None) };
        let own_url = std::env::var("ADB_NON_PROD_URL").ok().filter(|v| !v.is_empty()).ok_or("ADB_NON_PROD_OF needs ADB_NON_PROD_URL, this pod's Flight URL")?;
        let token = std::env::var("ADB_NON_PROD_TOKEN").or_else(|_| std::env::var("ADB_AUTH_TOKEN")).ok().filter(|t| !t.is_empty());
        Ok(Some(Self { org, own_url, token }))
    }
}

pub struct NonProdPod {
    config: NonProdConfig,
    root: PathBuf,
    engines: tokio::sync::Mutex<HashMap<String, Arc<LocalEngine>>>,
}

impl NonProdPod {
    /// Open the pod and every branch placed on it.
    pub async fn open(dir: impl Into<PathBuf>, config: NonProdConfig) -> Result<Self, String> {
        let pod = Self { config, root: dir.into().join("branches"), engines: Default::default() };
        std::fs::create_dir_all(&pod.root).map_err(|e| format!("{}: {e}", pod.root.display()))?;
        let list = flight_action(&pod.config.org, pod.config.token.as_deref(), "branch_list", json!({})).await?;
        let branches: Vec<Branch> = serde_json::from_value(list.get("branches").cloned().unwrap_or_default()).map_err(|e| e.to_string())?;
        for b in branches.iter().filter(|b| b.placement.as_deref() == Some(pod.config.own_url.as_str())) {
            pod.open_branch(b).await?;
        }
        tracing::info!(org = %pod.config.org, url = %pod.config.own_url, branches = pod.engines.lock().await.len(), "non-prod ADB pod open");
        Ok(pod)
    }

    fn dir_of(&self, b: &Branch) -> PathBuf {
        let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).collect::<String>();
        self.root.join(format!("{}__{}", safe(&b.namespace), safe(&b.name)))
    }

    /// The engine of `namespace`'s branch `name`, opened on first use. A branch that isn't
    /// placed here is refused: prod would keep serving it from elsewhere, and its writes
    /// would split in two.
    async fn engine(&self, namespace: &str, name: &str) -> Result<Arc<LocalEngine>, String> {
        if let Some(e) = self.engines.lock().await.get(&Branch::id_of(namespace, name)) {
            return Ok(Arc::clone(e));
        }
        let list = flight_action(&self.config.org, self.config.token.as_deref(), "branch_list", json!({ "namespace": namespace })).await?;
        let branches: Vec<Branch> = serde_json::from_value(list.get("branches").cloned().unwrap_or_default()).map_err(|e| e.to_string())?;
        let b = branches.into_iter().find(|b| b.name == name).ok_or_else(|| format!("no branch {name:?} in namespace {namespace:?}"))?;
        if b.placement.as_deref() != Some(self.config.own_url.as_str()) {
            return Err(format!("branch {name:?} of {namespace:?} isn't placed on this pod: place it with branch_place first"));
        }
        self.open_branch(&b).await
    }

    async fn open_branch(&self, b: &Branch) -> Result<Arc<LocalEngine>, String> {
        let mut engines = self.engines.lock().await;
        if let Some(e) = engines.get(&b.id) {
            return Ok(Arc::clone(e));
        }
        if b.parent != MAIN {
            return Err(format!("branch {:?} is a branch of a branch: not on the non-prod pod yet", b.name));
        }
        let source = Arc::new(FlightParentSource { url: self.config.org.clone(), token: self.config.token.clone(), branch: None });
        let view = ParentView { source, namespace: b.namespace.clone(), branch: b.name.clone(), point: b.point().into() };
        let (engine, _) = LocalEngine::durable_branch(self.dir_of(b), view).await?;
        write_origin(&engine, b).await?;
        self.apply_pending(&engine, b).await?;
        let engine = Arc::new(engine);
        engines.insert(b.id.clone(), Arc::clone(&engine));
        Ok(engine)
    }

    /// FORGETs on main this branch missed while the pod was down, erased before it serves.
    async fn apply_pending(&self, engine: &LocalEngine, b: &Branch) -> Result<(), String> {
        let token = self.config.token.as_deref();
        let pending = flight_action(&self.config.org, token, "branch_pending", json!({ "namespace": b.namespace, "name": b.name })).await?;
        let forgets: Vec<PendingForget> = serde_json::from_value(pending.get("forgets").cloned().unwrap_or_default()).map_err(|e| e.to_string())?;
        if forgets.is_empty() {
            return Ok(());
        }
        for f in &forgets {
            engine.forget_lineages(f.memory, &f.lineages).await?;
        }
        flight_action(&self.config.org, token, "branch_pending_ack", json!({ "namespace": b.namespace, "name": b.name, "count": forgets.len() })).await?;
        tracing::info!(branch = %b.id, forgets = forgets.len(), "pending FORGETs on main applied");
        Ok(())
    }
}

#[async_trait]
impl Engine for NonProdPod {
    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        let (branch, namespace) = statement_branch(aql)?.ok_or("this is the non-prod pod: a statement must name its BRANCH")?;
        let namespace = namespace.ok_or_else(|| format!("BRANCH \"{branch}\" needs NAMESPACE: a branch belongs to one memory space"))?;
        self.engine(&namespace, &branch).await?.query(aql, variables).await
    }

    /// Per branch hosted here; this pod has no records of its own.
    async fn stats(&self) -> Result<Value, String> {
        let engines = self.engines.lock().await;
        Ok(json!({ "non_prod_pod": true, "branches": engines.keys().collect::<Vec<_>>() }))
    }

    /// A branch's view, for prod (promote, conflicts, stats).
    async fn parent_rows(&self, memory: MemoryType, namespace: &str, branch: Option<&str>, at: i64) -> Result<Vec<adb_store::StoredRow>, String> {
        let name = branch.filter(|b| *b != MAIN).ok_or("this is the non-prod pod: name the branch")?;
        self.engine(namespace, name).await?.parent_rows(memory, namespace, None, at).await
    }

    /// FORGET on main: a lineage key names its record wherever it was copied, so every
    /// branch here erases its own rows.
    async fn forget_lineages(&self, memory: MemoryType, lineages: &[String]) -> Result<u64, String> {
        let engines: Vec<Arc<LocalEngine>> = self.engines.lock().await.values().cloned().collect();
        let mut erased = 0;
        for e in engines {
            erased += e.forget_lineages(memory, lineages).await?;
        }
        Ok(erased)
    }
}

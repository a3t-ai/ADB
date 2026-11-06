//! Branch engines (`.ai/BRANCHING.md`, B2): a memory branch is its own ADB engine —
//! its own WAL, sequencer and delta table — isolated from the production agent.
//!
//! - [`ParentSource`]: a parent's rows of a namespace as they were at a time. The org's
//!   engine answers from its table ([`StoreSource`]); a branch engine answers with its
//!   own overlay ([`BranchSource`]), so a branch of a branch works recursively.
//! - [`ParentView`]: what a branch engine reads through: its parent source, its
//!   namespace, its branch point.
//! - [`overlay`]: the branch's own rows first, then the parent's; the nearest level
//!   wins per lineage, and a `_branch_deleted` row hides one.
//! - [`BranchHost`]: the org engine's branch engines, opened under
//!   `<data dir>/branches/<namespace>__<name>` (hosted placement).
//!
//! The org's sequencer, WAL, table and compaction never see a branch's writes.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use adb_core::log::CommitTs;
use adb_core::{time, AdbError, AdbResult, MemoryType};
use adb_store::{PartitionStore, StoredRow};
use async_trait::async_trait;

use crate::apply::ApplyBarrier;
use crate::branches::{Branch, Branches, MAIN};

/// A parent's rows of `namespace` in `memory`, as they were at `at`.
#[async_trait]
pub trait ParentSource: Send + Sync {
    async fn rows_as_of(&self, memory: MemoryType, namespace: &str, at: CommitTs) -> AdbResult<Vec<StoredRow>>;
}

/// What a branch engine reads through.
#[derive(Clone)]
pub struct ParentView {
    pub source: Arc<dyn ParentSource>,
    pub namespace: String,
    pub branch: String,
    /// The branch point: the parent is read as it was at this commit time. A rebase
    /// (DB-11) moves it in the live engine.
    pub point: SharedPoint,
}

/// A branch point every holder of a [`ParentView`] sees move.
#[derive(Clone, Debug)]
pub struct SharedPoint(Arc<std::sync::atomic::AtomicI64>);

impl SharedPoint {
    pub fn get(&self) -> CommitTs {
        CommitTs(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
    pub fn set(&self, ts: CommitTs) {
        self.0.store(ts.0, std::sync::atomic::Ordering::SeqCst);
    }
}

impl From<CommitTs> for SharedPoint {
    fn from(ts: CommitTs) -> Self {
        Self(Arc::new(std::sync::atomic::AtomicI64::new(ts.0)))
    }
}

fn ns_filter(namespace: &str) -> String {
    format!("namespace = '{}'", namespace.replace('\'', "''"))
}

async fn wait(barrier: &Option<Arc<ApplyBarrier>>) {
    if let Some(b) = barrier {
        b.wait_for_committed().await;
    }
}

/// A table's rows current at `at` (every version is kept until compaction, and the
/// org's retention pins keep the ones open branches read).
async fn rows_at(store: &PartitionStore, memory: MemoryType, namespace: &str, at: CommitTs, now: CommitTs) -> AdbResult<Vec<StoredRow>> {
    let rows = store.versions(memory.into(), Some(&ns_filter(namespace))).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
    Ok(rows.into_iter().filter(|r| r.is_current_at(at, now)).collect())
}

/// The org (or any engine that is not a branch): its own table.
pub struct StoreSource {
    pub store: Arc<PartitionStore>,
    pub barrier: Option<Arc<ApplyBarrier>>,
}

#[async_trait]
impl ParentSource for StoreSource {
    async fn rows_as_of(&self, memory: MemoryType, namespace: &str, at: CommitTs) -> AdbResult<Vec<StoredRow>> {
        wait(&self.barrier).await;
        rows_at(&self.store, memory, namespace, at, CommitTs::from_datetime(time::now())).await
    }
}

/// A branch engine as the parent of another branch: its overlay at `at`.
pub struct BranchSource {
    pub store: Arc<PartitionStore>,
    pub barrier: Option<Arc<ApplyBarrier>>,
    pub parent: ParentView,
}

#[async_trait]
impl ParentSource for BranchSource {
    async fn rows_as_of(&self, memory: MemoryType, namespace: &str, at: CommitTs) -> AdbResult<Vec<StoredRow>> {
        wait(&self.barrier).await;
        let now = CommitTs::from_datetime(time::now());
        let own = rows_at(&self.store, memory, namespace, at, now).await?;
        // The grandparent as it was at this branch's own point (never later than `at`).
        let up = self.parent.source.rows_as_of(memory, namespace, self.parent.point.get()).await?;
        Ok(overlay(own, up).into_iter().map(|(r, _)| r).collect())
    }
}

/// A row a branch wrote to delete a parent record: it hides the lineage.
pub fn branch_deleted(r: &StoredRow) -> bool {
    r.data.value.get("_branch_deleted").and_then(|v| v.as_bool()).unwrap_or(false)
}

/// The branch's own rows first, then the parent's: the nearest level wins per lineage
/// and a delete marker hides it. Each row comes with `true` when it is the branch's own.
pub fn overlay(own: Vec<StoredRow>, parent: Vec<StoredRow>) -> Vec<(StoredRow, bool)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (rows, mine) in [(own, true), (parent, false)] {
        for r in rows {
            if seen.insert(r.lineage_id.clone()) && !branch_deleted(&r) {
                out.push((r, mine));
            }
        }
    }
    out
}

/// What a branch engine sees of `memory` now: its own current rows, then the parent
/// as of the branch point.
pub async fn view(store: &PartitionStore, barrier: &Option<Arc<ApplyBarrier>>, parent: &ParentView, memory: MemoryType, now: CommitTs) -> AdbResult<Vec<(StoredRow, bool)>> {
    wait(barrier).await;
    let own = rows_at(store, memory, &parent.namespace, now, now).await?;
    let up = parent.source.rows_as_of(memory, &parent.namespace, parent.point.get()).await?;
    Ok(overlay(own, up))
}

/// DB-20: a new branch engine's first record (when its log is empty) is an EPISODIC
/// row naming where it came from: `{event: "branch_created", from_namespace, parent,
/// point_offset, point_ts, created_by}` (D1). Hosted and pod branches alike.
pub async fn write_origin(engine: &crate::LocalEngine, b: &Branch) -> Result<(), String> {
    use crate::Engine;
    let Some(d) = engine.durable_writes() else { return Ok(()) };
    if d.last_commit().await.map_err(|e| e.to_string())?.0 > 0 {
        return Ok(());
    }
    let s = |v: &str| serde_json::to_string(v).unwrap_or_else(|_| "\"\"".into());
    let aql = format!(
        "STORE INTO EPISODIC (event = \"branch_created\", from_namespace = {}, parent = {}, point_offset = \"{}\", point_ts = \"{}\", created_by = {}) NAMESPACE {}",
        s(&b.namespace),
        s(&b.parent),
        b.point_offset,
        b.point_ts,
        s(b.created_by.as_deref().unwrap_or("")),
        s(&b.namespace)
    );
    engine.query(&aql, None).await.map(|_| ())
}

/// The branch a statement names (`BRANCH "b"`, not main) and its NAMESPACE. A
/// pipeline must name one branch in every stage, or none.
pub fn statement_branch(aql: &str) -> Result<Option<(String, Option<String>)>, String> {
    use aql_parser::Statement as S;
    let Ok(stmt) = aql_parser::parse(aql) else { return Ok(None) };
    fn of(s: &S) -> Option<(Option<String>, Option<String>)> {
        let m = match s {
            S::Recall(x) => &x.modifiers,
            S::Lookup(x) => &x.modifiers,
            S::Load(x) => &x.modifiers,
            S::Store(x) => &x.modifiers,
            S::Update(x) => &x.modifiers,
            S::Forget(x) => &x.modifiers,
            S::Scan(x) => &x.modifiers,
            _ => return None,
        };
        Some((m.branch.clone().filter(|b| b != MAIN), m.namespace.clone()))
    }
    let stages: Vec<&S> = match &stmt {
        S::Pipeline(p) => p.stages.iter().collect(),
        other => vec![other],
    };
    let named: Vec<(String, Option<String>)> = stages.iter().filter_map(|s| of(s)).filter_map(|(b, ns)| b.map(|b| (b, ns))).collect();
    let Some(first) = named.first().cloned() else { return Ok(None) };
    if named.len() != stages.len() || named.iter().any(|(b, _)| *b != first.0) {
        return Err(format!("every stage of a pipeline on branch {:?} must name it (BRANCH \"{}\")", first.0, first.0));
    }
    Ok(Some(first))
}

/// DB-3: what a statement touches: per stage, its `NAMESPACE` and whether it writes. A kind
/// that doesn't say (LINK, SUBSCRIBE, …), or a statement that doesn't parse, is an error.
pub fn statement_namespaces(aql: &str) -> Result<Vec<(Option<String>, bool)>, String> {
    use aql_parser::Statement as S;
    let stmt = aql_parser::parse(aql).map_err(|_| "a statement that doesn't parse".to_string())?;
    fn of(s: &S) -> Option<(Option<String>, bool)> {
        Some(match s {
            S::Recall(x) => (x.modifiers.namespace.clone(), false),
            S::Lookup(x) => (x.modifiers.namespace.clone(), false),
            S::Load(x) => (x.modifiers.namespace.clone(), false),
            S::Scan(x) => (x.modifiers.namespace.clone(), false),
            S::Store(x) => (x.modifiers.namespace.clone(), true),
            S::Update(x) => (x.modifiers.namespace.clone(), true),
            S::Forget(x) => (x.modifiers.namespace.clone(), true),
            _ => return None,
        })
    }
    let stages: Vec<&S> = match &stmt {
        S::Pipeline(p) => p.stages.iter().collect(),
        other => vec![other],
    };
    stages.iter().map(|s| of(s).ok_or_else(|| "this kind of statement".to_string())).collect()
}

/// B6: a branch engine in its own ADB process, reached over the network (the
/// server crate implements it over Arrow Flight).
#[async_trait]
pub trait BranchRemote: Send + Sync {
    /// Run a statement on the branch's engine (its `aql` action).
    async fn query(&self, url: &str, aql: &str, variables: Option<serde_json::Value>) -> Result<serde_json::Value, String>;
    /// Have it erase its own rows of `lineages` (FORGET on main reaching it).
    async fn forget(&self, url: &str, memory: MemoryType, lineages: &[String]) -> Result<u64, String>;
    /// Its view (its rows over its parent's at its point), as a parent source.
    fn source(&self, url: &str, branch: &str) -> Arc<dyn ParentSource>;
}

/// The branch engines of an org engine: hosted ones opened here; ones in their own
/// pod (B6) reached through the [`BranchRemote`].
pub struct BranchHost {
    root: PathBuf,
    org: Arc<dyn ParentSource>,
    registry: Arc<Branches>,
    engines: tokio::sync::Mutex<HashMap<String, Arc<crate::LocalEngine>>>,
    remote: std::sync::OnceLock<Arc<dyn BranchRemote>>,
}

impl BranchHost {
    pub fn new(root: PathBuf, org: Arc<dyn ParentSource>, registry: Arc<Branches>) -> Self {
        Self { root, org, registry, engines: Default::default(), remote: Default::default() }
    }

    /// How this org reaches branches in their own pods (B6); set once at start.
    pub fn set_remote(&self, remote: Arc<dyn BranchRemote>) {
        let _ = self.remote.set(remote);
    }

    fn remote(&self) -> Result<&Arc<dyn BranchRemote>, String> {
        self.remote.get().ok_or_else(|| "this ADB can't reach branches in their own pods (no Flight client)".into())
    }

    /// The branch's pod URL, when it has one.
    pub fn placement(&self, namespace: &str, name: &str) -> Option<String> {
        self.registry.get(namespace, name).and_then(|b| b.placement)
    }

    /// Run `aql` on a branch in its own pod (B6).
    pub async fn query_remote(&self, url: &str, aql: &str, variables: Option<serde_json::Value>) -> Result<serde_json::Value, String> {
        self.remote()?.query(url, aql, variables).await
    }

    /// The branch's view as a parent source: its pod's (B6), or its hosted engine's.
    pub async fn source(&self, namespace: &str, name: &str) -> Result<Arc<dyn ParentSource>, String> {
        match self.placement(namespace, name) {
            Some(url) => Ok(self.remote()?.source(&url, name)),
            None => self.engine(namespace, name).await?.as_parent_source().ok_or_else(|| "a branch engine has no table".into()),
        }
    }

    fn dir_of(&self, b: &Branch) -> PathBuf {
        let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).collect::<String>();
        self.root.join(format!("{}__{}", safe(&b.namespace), safe(&b.name)))
    }

    /// The engine of branch `name` of `namespace`, opened on first use (its parent
    /// first, for a branch of a branch).
    pub async fn engine(&self, namespace: &str, name: &str) -> Result<Arc<crate::LocalEngine>, String> {
        let chain = self.registry.chain(namespace, name)?;
        if let Some(b) = chain.iter().find(|b| b.placement.is_some()) {
            return Err(format!("branch {:?} runs in its own pod: a hosted branch can't be on it (B6)", b.name));
        }
        // Open from the one nearest main outwards, each reading through the previous.
        let mut source: Arc<dyn ParentSource> = Arc::clone(&self.org);
        let mut last = None;
        for b in chain.iter().rev() {
            let view = ParentView { source: Arc::clone(&source), namespace: b.namespace.clone(), branch: b.name.clone(), point: b.point().into() };
            let engine = {
                let mut engines = self.engines.lock().await;
                match engines.get(&b.id) {
                    Some(e) => Arc::clone(e),
                    None => {
                        let (e, _) = crate::LocalEngine::durable_branch(self.dir_of(b), view.clone()).await?;
                        // Compaction keeps what this branch's own branches read (B4).
                        if let Some(d) = e.durable_writes() {
                            let (registry, ns, name) = (Arc::clone(&self.registry), b.namespace.clone(), b.name.clone());
                            d.set_pins(Arc::new(move || registry.pins(Some((&ns, &name)))));
                        }
                        write_origin(&e, b).await?;
                        let e = Arc::new(e);
                        engines.insert(b.id.clone(), Arc::clone(&e));
                        e
                    }
                }
            };
            source = engine.as_parent_source().ok_or("a branch engine has no table")?;
            last = Some(engine);
        }
        last.ok_or_else(|| format!("no branch {name:?} in namespace {namespace:?}"))
    }

    /// FORGET on main reaches every branch (B4): each branch engine erases its own
    /// rows of `lineages` (the parent's are already gone, so its view hides them).
    /// Branches of every namespace are visited: a lineage key names its record
    /// wherever it was copied. A branch in its own pod is asked over the network
    /// (B6); the ones that can't be reached now are returned, for the caller to keep
    /// the FORGET pending on them.
    pub async fn forget(&self, memory: MemoryType, lineages: &[String]) -> Result<Vec<Branch>, String> {
        let mut unreachable = Vec::new();
        for b in self.registry.list(None) {
            if let Some(url) = &b.placement {
                if let Err(e) = self.remote()?.forget(url, memory, lineages).await {
                    tracing::warn!(branch = %b.id, %url, error = %e, "branch pod unreachable: the FORGET stays pending until it starts");
                    unreachable.push(b);
                }
                continue;
            }
            let e = self.engine(&b.namespace, &b.name).await?;
            if let Some(d) = e.durable_writes() {
                d.forget_lineages(memory, lineages).await.map_err(|e| format!("branch {}: {e}", b.id))?;
            }
        }
        Ok(unreachable)
    }

    /// Whether a hosted engine exists for `b` (it was opened and wrote its WAL).
    pub fn hosted_exists(&self, b: &Branch) -> bool {
        self.dir_of(b).exists()
    }

    //// Forget an engine (the branch was dropped): close it and delete its data.
    pub async fn remove(&self, b: &Branch) -> Result<(), String> {
        let e = self.engines.lock().await.remove(&b.id);
        drop(e);
        let dir = self.dir_of(b);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(|e| format!("removing branch {}'s data: {e}", b.id))?;
        }
        Ok(())
    }
}

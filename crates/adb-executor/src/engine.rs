//! The engine boundary used by every API surface (REST, MCP, CLI).
//!
//! Surfaces talk to an [`Engine`] only, never to a storage backend, so the engine can
//! be replaced (today's in-memory backends → the log + MemWAL engine of the delivery
//! plan) without touching them. Results use the **ClawDB wire format** that A3T /
//! FlowR already parse (`ClawDbMemory` adapter: `data.data.records`, `/stats` counts).

use std::collections::HashMap;
use std::sync::Arc;

use adb_core::{MemoryRecord, MemoryType};
use async_trait::async_trait;
use serde_json::{json, Map, Value};

use adb_core::log::{Epoch, PartitionId};
use adb_log::{LocalWal, WalConfig};

/// The log a durable engine runs on: the local WAL, or (cluster) the Raft log.
pub type EngineLog = Arc<dyn adb_log::Log>;
use adb_sequencer::Sequencer;
use adb_store::{PartitionStore, StoreConfig};

use crate::apply::ApplyBarrier;
use adb_gateway::Gateway;
use crate::reads::{LanceReads, Reads};
use crate::writes::{DurableWrites, ReplayStats, Writes, SINGLE_PARTITION};
use crate::{Executor, QueryResult, ResultSet};

/// What every API surface needs from the database.
#[async_trait]
pub trait Engine: Send + Sync {
    /// Run one AQL statement. `Ok` holds a ClawDB-format `QueryResult` object
    /// (`{success, data: {type, …}, metadata}`); `Err` is a parse / plan / execution
    /// error message.
    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String>;

    /// Record counts per memory type: `{"working": n, "episodic": n, …}`.
    async fn stats(&self) -> Result<Value, String>;

    /// Subscriber reads (stage E), for engines with a log.
    fn gateway(&self) -> Option<Arc<Gateway>> {
        None
    }

    /// A watch's next frames (W6) after `from` as JSON — each `{cq_id, type, offset,
    /// …}` — waiting up to `wait` for one. A cluster answers on any node: the watch
    /// runs on its namespace's partition, which its id names.
    async fn watch_frames(&self, cq_id: &str, from: Option<u64>, max: usize, wait: std::time::Duration) -> Result<Value, String> {
        let w = self.watcher().ok_or("watches need a durable engine (start ADB with --data-dir)")?;
        let frames = w.next(cq_id, from, max, wait).await.map_err(|e| e.to_string())?;
        serde_json::to_value(frames).map_err(|e| e.to_string())
    }

    /// A subscription's next deliveries (stage E) as JSON — `{deliveries, reset?,
    /// next, tip}` — waiting up to `wait` for one. A cluster answers on any node: the
    /// gateway is on the system partition's owner.
    /// A subscription operation AQL leaves to the API (AQL v0.13): ACK, a scope's
    /// change feed, a start offset — planned as before, run without a statement.
    async fn subscription(&self, _plan: aql_planner::SubscriptionPlan) -> Result<Value, String> {
        Err("subscriptions need a durable engine (start ADB with --data-dir)".into())
    }

    async fn next_events(&self, subscription: &str, from: Option<u64>, max: usize, wait: std::time::Duration) -> Result<Value, String> {
        let gw = self.gateway().ok_or("subscriptions need a durable engine (start ADB with --data-dir)")?;
        let batch = gw.next(subscription, from, max, wait).await.map_err(|e| e.to_string())?;
        serde_json::to_value(batch).map_err(|e| e.to_string())
    }

    /// Platform tables (stage G) for reads, for engines with a log.
    fn platform(&self) -> Option<Arc<adb_platform::PlatformStore>> {
        None
    }

    /// Memory branches (B1): cut, drop and list. Need a durable engine.
    async fn branch_create(&self, _namespace: &str, _name: &str, _parent: Option<&str>, _created_by: Option<&str>) -> Result<crate::branches::Branch, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    async fn branch_drop(&self, _namespace: &str, _name: &str) -> Result<bool, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    async fn branch_list(&self, _namespace: Option<&str>) -> Result<Vec<crate::branches::Branch>, String> {
        Ok(Vec::new())
    }

    /// B5: promote branch `name` of `namespace` as a new agent in namespace `into`
    /// (its view of SEMANTIC, PROCEDURAL and TOOLS, materialised in production); the
    /// original agent and the branch are unchanged. `{namespace, records, from}`.
    async fn branch_promote(&self, _namespace: &str, _name: &str, _into: &str, _allow_stale: bool) -> Result<Value, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    /// B5: clone `namespace` (main, now) as a new agent in namespace `into`.
    async fn space_clone(&self, _namespace: &str, _into: &str) -> Result<Value, String> {
        Err("cloning a memory space needs a durable engine (start ADB with --data-dir)".into())
    }

    /// DB-17: copy a space's procedures (and tools, with `tools`) into `into`. No episodes,
    /// facts or working memory.
    async fn space_copy_procedures(&self, _namespace: &str, _into: &str, _tools: bool) -> Result<Value, String> {
        Err("copying a memory space needs a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-9: clone `namespace` as it was at a past point.
    async fn space_clone_at(&self, _namespace: &str, _into: &str, _at: &crate::branches::PointRef) -> Result<Value, String> {
        Err("cloning a memory space needs a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-4: set or replace a storage quota on the namespaces matching `pattern`.
    async fn quota_set(&self, _pattern: &str, _max_bytes: Option<u64>, _max_records: Option<u64>, _history_days: Option<u64>) -> Result<crate::branches::Quota, String> {
        Err("quotas need a durable engine (start ADB with --data-dir)".into())
    }
    async fn quota_drop(&self, _pattern: &str) -> Result<bool, String> {
        Err("quotas need a durable engine (start ADB with --data-dir)".into())
    }
    async fn quota_list(&self) -> Result<Vec<crate::branches::Quota>, String> {
        Ok(Vec::new())
    }
    /// DB-4: `{namespaces: {ns: {records, bytes}}}`, one namespace's if named. A scan when the
    /// count isn't known yet.
    async fn space_usage(&self, _namespace: Option<&str>) -> Result<Value, String> {
        Err("usage needs a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-12: the spaces that read `space` as a layer.
    async fn layers_dependents(&self, _space: &str) -> Result<Vec<crate::branches::Dependent>, String> {
        Ok(Vec::new())
    }
    /// DB-14: per memory type `{count, bytes, last_write_ts}` of a space's current rows
    /// (`bytes`: payload size, not disk), kept for `max_age` as each read is a scan.
    async fn space_stats(&self, _namespace: &str, _branch: Option<&str>, _max_age: std::time::Duration) -> Result<Value, String> {
        Err("space stats need a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-14: one page of a memory type's current rows, newest first:
    /// `{records, next}`; `after` is the `next` of the page before.
    async fn space_records(&self, _namespace: &str, _memory: MemoryType, _branch: Option<&str>, _after: Option<&str>, _limit: usize) -> Result<Value, String> {
        Err("space records need a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-14: the gateway whose log holds a space's writes.
    async fn space_gateway(&self, _namespace: &str) -> Result<Arc<Gateway>, String> {
        Err("the space feed needs a durable engine (start ADB with --data-dir)".into())
    }

    /// DB-10: the three-way diff of branch `name` against main (`.ai/MERGE.md`):
    /// `{base, main, clean, conflicts}` for `memories` (default all three).
    async fn branch_conflicts(&self, _namespace: &str, _name: &str, _memories: &[MemoryType]) -> Result<Value, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-19: merge the branch's changes of `memories` (default PROCEDURAL) back into
    /// main, with `resolutions` for every conflict; refused when main moved past
    /// `main_offset`. `{merge_id, offset, changed, added, ended}`.
    async fn branch_merge(&self, _namespace: &str, _name: &str, _memories: &[MemoryType], _main_offset: u64, _resolutions: Vec<crate::merge::Resolution>, _by: Option<&str>) -> Result<Value, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-11: move the branch point to main now, settling each conflict in the branch:
    /// `main` drops the branch's change, `edit` writes the value, `branch` keeps it.
    async fn branch_rebase(&self, _namespace: &str, _name: &str, _main_offset: u64, _resolutions: Vec<crate::merge::Resolution>) -> Result<Value, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    /// DB-19: undo merge `merge_id`; records changed since are conflicts (refused
    /// unless `force_skip`, which reverts the rest).
    async fn merge_revert(&self, _namespace: &str, _merge_id: &str, _by: Option<&str>, _force_skip: bool) -> Result<Value, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }

    /// B7: tags (named points of a space) and layers (inherited spaces).
    async fn tag_create(&self, _namespace: &str, _name: &str, _created_by: Option<&str>) -> Result<crate::branches::Tag, String> {
        Err("tags need a durable engine (start ADB with --data-dir)".into())
    }
    async fn tag_drop(&self, _namespace: &str, _name: &str) -> Result<bool, String> {
        Err("tags need a durable engine (start ADB with --data-dir)".into())
    }
    async fn tag_list(&self, _namespace: Option<&str>) -> Result<Vec<crate::branches::Tag>, String> {
        Ok(Vec::new())
    }
    async fn layers_set(&self, _namespace: &str, _layers: Vec<crate::branches::Layer>) -> Result<crate::branches::Layers, String> {
        Err("layers need a durable engine (start ADB with --data-dir)".into())
    }
    async fn layers_get(&self, _namespace: &str) -> Result<Vec<crate::branches::Layer>, String> {
        Ok(Vec::new())
    }

    /// A platform-table write (`Table*` effect), committed through the log and
    /// applied before it returns. Returns its log offset.
    async fn platform_write(&self, _effect: adb_core::log::Effect) -> Result<u64, String> {
        Err("platform tables need a durable engine (start ADB with --data-dir)".into())
    }

    /// Several platform-table writes as one transaction (DB-6): one log record.
    async fn platform_write_many(&self, _effects: Vec<adb_core::log::Effect>) -> Result<u64, String> {
        Err("platform tables need a durable engine (start ADB with --data-dir)".into())
    }

    /// Continuous queries (MVP W6), for engines with a log.
    fn watcher(&self) -> Option<Arc<adb_cq::Watcher>> {
        None
    }

    /// W5 compaction now (admin): `{total, kept, bytes_before, bytes_after, took_ms}`.
    async fn compact(&self) -> Result<Value, String> {
        Err("compaction needs a durable engine (start ADB with --data-dir)".into())
    }

    /// The embedding settings (W9.10): `{settings, source, active, pending}`.
    async fn embedding(&self) -> Result<Value, String> {
        Err("embedding settings need a data directory (start ADB with --data-dir)".into())
    }

    /// Check new embedding settings (a sample embedding of the right size), save
    /// them, and restart ADB to apply them (W9.10).
    async fn set_embedding(&self, _settings: Value) -> Result<Value, String> {
        Err("embedding settings need a data directory (start ADB with --data-dir)".into())
    }

    /// The hot cache (W10): `{mode, source, settings, stats}`.
    async fn cache(&self) -> Result<Value, String> {
        Err("the hot cache needs a durable engine (start ADB with --data-dir)".into())
    }

    /// Check and save new cache rules and rebuild the cache with them (no restart).
    async fn set_cache(&self, _settings: Value) -> Result<Value, String> {
        Err("the hot cache needs a durable engine (start ADB with --data-dir)".into())
    }

    /// A replica set follower's engine (C-16): reads only; every write goes to the
    /// leader, and the transports answer them with [`crate::NOT_LEADER`].
    fn read_only(&self) -> bool {
        false
    }

    /// Wait until reads here see every write acknowledged before this call (a
    /// follower: the leader's commit index applied). Called before platform-table reads.
    async fn read_fresh(&self) -> Result<(), String> {
        Ok(())
    }

    /// Read-only SQL over the memory and platform tables (the superadmin's debug
    /// view, `crate::sql`); at most `limit` rows.
    async fn sql(&self, _sql: &str, _limit: Option<usize>) -> Result<crate::sql::SqlResult, String> {
        Err("SQL needs a durable engine (start ADB with --data-dir)".into())
    }

    /// This engine's memory table as stored, every row: what a cluster's SQL view
    /// gathers from each partition ([`crate::sql::memory_raw`]).
    async fn memory_raw(&self) -> Result<crate::sql::SqlResult, String> {
        Err("SQL needs a durable engine (start ADB with --data-dir)".into())
    }

    /// FlowR's engine store, space `space` (stage H); `None` without a data dir.
    fn flowr_engine(&self, _space: &str) -> Option<Arc<dyn flowr_storage::engine::EngineStore>> {
        None
    }

    /// Step 4b: the Flight endpoint of the node owning the system partition (platform
    /// tables, the engine store), when that isn't this node; the transports send those
    /// calls there. `None`: this node serves them.
    fn system_flight(&self) -> Option<String> {
        None
    }

    /// Step 4b: run `aql` within `reach` (a scoped token's namespaces: statements naming
    /// none run inside them). `None`: this engine can't — a scoped statement must then
    /// name its namespace (DB-3).
    async fn query_reach(&self, _aql: &str, _variables: Option<&HashMap<String, Value>>, _reach: &adb_cluster::routing::Reach) -> Option<Result<Value, String>> {
        None
    }

    /// Load another store's state into engine space `space` (H-5, `adb
    /// import-flowr-engine`): `changes` are `engine_core::Change`s, logged like
    /// any operation's. `first`: refuse a space that already holds runs.
    async fn engine_restore(&self, _space: &str, _changes: Vec<Value>, _first: bool) -> Result<usize, String> {
        Err("FlowR's engine store needs a durable engine (start ADB with --data-dir)".into())
    }
}

/// `ADB_CACHE`: `on` (default), `off`, or `shadow` (every answer from the hot cache
/// is checked against Lance; a difference fails the read).
fn cache_mode() -> String {
    match std::env::var("ADB_CACHE").unwrap_or_default().to_ascii_lowercase().as_str() {
        "off" | "0" | "false" => "off".into(),
        "shadow" => "shadow".into(),
        _ => "on".into(),
    }
}

/// `ADB_CACHE_RULES`: the cache rules (JSON, as `cache.json`) when none are saved.
fn env_cache_rules() -> Result<Option<adb_store::CacheConfig>, String> {
    let Ok(j) = std::env::var("ADB_CACHE_RULES") else { return Ok(None) };
    let c: adb_store::CacheConfig = serde_json::from_str(&j).map_err(|e| format!("ADB_CACHE_RULES: {e}"))?;
    c.check()?;
    Ok(Some(c))
}

/// The hot cache settings saved in `dir`, else `ADB_CACHE_RULES`, else the defaults.
pub(crate) fn cache_config(dir: &std::path::Path) -> Result<adb_store::CacheConfig, String> {
    Ok(match adb_store::CacheConfig::load(dir)? {
        Some(c) => c,
        None => env_cache_rules()?.unwrap_or_default(),
    })
}

/// The hot cache with `cfg`, unless
/// `ADB_CACHE=off`.
pub(crate) fn hot_cache(cfg: adb_store::CacheConfig) -> Option<adb_store::HotCache> {
    match cache_mode().as_str() {
        "off" => None,
        mode => Some(adb_store::HotCache::new(cfg, mode == "shadow").shaped(crate::query::record)),
    }
}

static RESTART: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Ask the server to stop so its supervisor starts it again with new settings
/// (W9.10). Kubernetes restarts the container; elsewhere ADB must be started again.
pub fn request_restart() {
    RESTART.notify_one();
}

/// Resolves once [`request_restart`] was called.
pub async fn restart_requested() {
    RESTART.notified().await
}

/// The embedder for `settings` (none saved: the environment's local model), and
/// the table's dimension and model name for it.
fn embedder_for(settings: Option<&adb_embed::EmbedConfig>) -> (Option<Arc<adb_embed::Embedder>>, i32, Option<String>) {
    // Without settings: the shared embedder's model names the partition's vectors (its
    // vector files are checked against it, and merged under it).
    let Some(c) = settings else {
        let e = adb_embed::Embedder::shared();
        let model = e.as_ref().map(|e| e.model().to_string());
        let dim = e.as_ref().map_or(adb_store::DEFAULT_EMBEDDING_DIM, |e| e.dimension() as i32);
        return (e, dim, model);
    };
    let embedder = if adb_embed::Embedder::disabled() {
        None
    } else {
        match adb_embed::Embedder::from_config(c) {
            Ok(e) => {
                tracing::info!(provider = ?c.provider, model = %c.model, dimension = c.dimension, "embeddings from ADB's settings");
                Some(Arc::new(e))
            }
            Err(e) => {
                tracing::error!(error = %e, "embedding settings unusable; running without vectors");
                None
            }
        }
    };
    (embedder, c.dimension as i32, Some(c.model.clone()))
}

/// The engine: the partition log, its Lance table (the only copy of every memory
/// type, W7), platform tables, subscriptions and watches.
pub struct LocalEngine {
    executor: Executor,
    /// The Lance table and the barrier its reads wait on.
    store: Option<(Arc<PartitionStore>, Option<Arc<ApplyBarrier>>)>,
    gateway: Option<Arc<Gateway>>,
    /// Platform tables (stage G) and the writes that commit them.
    platform: Option<(Arc<adb_platform::PlatformStore>, Arc<dyn Writes>)>,
    /// The TTL expiry timer (L3), stopped with the engine.
    expiry: Option<tokio::task::JoinHandle<()>>,
    /// The durable writes, for compaction on demand (W5).
    durable: Option<Arc<DurableWrites<EngineLog>>>,
    /// Continuous queries (W6) and the task keeping them current.
    watcher: Option<(Arc<adb_cq::Watcher>, tokio::task::JoinHandle<()>)>,
    /// The data directory (W9.10: settings live there).
    dir: std::path::PathBuf,
    /// The embedder in use (W9.10: reported with the settings).
    embedder: Option<Arc<adb_embed::Embedder>>,
    /// Where other spaces (memory layers) are read: this partition, or (cluster) the
    /// router's source, set by [`crate::partitioned::PartitionedEngine`].
    spaces: Option<crate::spaces::SpaceView>,
    /// The log the sequencer appends to (readers on other nodes follow it, step 6).
    log: Option<EngineLog>,
    /// DB-14: recent space stats, as each is a scan: key → (when, stats).
    stats_cache: std::sync::Mutex<std::collections::HashMap<String, (std::time::Instant, Value)>>,
    /// A temporary engine's directory, removed with it (dropped last).
    temp: Option<tempfile::TempDir>,
}

impl Drop for LocalEngine {
    fn drop(&mut self) {
        if let Some(t) = &self.expiry {
            t.abort();
        }
        if let Some((_, t)) = &self.watcher {
            t.abort();
        }
    }
}

/// Runs watched queries for the watcher (W6) and commits its idle `UNWATCH`es.
struct WatchRunner {
    executor: Executor,
    durable: Arc<DurableWrites<EngineLog>>,
}

#[async_trait]
impl adb_cq::Runner for WatchRunner {
    async fn run(&self, spec: &adb_cq::WatchSpec) -> Result<Value, String> {
        let r = self.executor.execute_statement(&spec.statement, &spec.variables).await.map_err(|e| e.to_string())?;
        Ok(to_wire(&r))
    }

    async fn unwatch(&self, cq_id: &str) -> Result<(), String> {
        self.durable.unregister_watch(cq_id).await.map_err(|e| e.to_string())
    }
}

/// `WATCH` / `UNWATCH` (W6) if `aql` is one: they are handled by the engine, which
/// owns the watcher, not the executor.
/// A live query (`SUBSCRIBE RECALL …`, AQL v0.13) or an UNSUBSCRIBE that may end
/// one: the engine registers live queries itself (W6), the planner doesn't.
pub(crate) fn watch_statement(aql: &str) -> Option<aql_parser::ast::Statement> {
    let first = aql.trim_start().split(|c: char| c.is_whitespace()).next()?;
    if !first.eq_ignore_ascii_case("SUBSCRIBE") && !first.eq_ignore_ascii_case("UNSUBSCRIBE") {
        return None;
    }
    match aql_parser::parse(aql).ok()? {
        s @ aql_parser::ast::Statement::Subscribe(aql_parser::ast::SubscribeStmt { target: aql_parser::ast::SubscribeTarget::Query(_), .. }) => Some(s),
        s @ aql_parser::ast::Statement::Unsubscribe(_) => Some(s),
        _ => None,
    }
}

/// BRANCH / FORK / MERGE (AQL v0.13): run by the engine, not the planner (they work
/// across namespaces).
pub(crate) fn namespace_statement(aql: &str) -> Option<aql_parser::ast::Statement> {
    let first = aql.trim_start().split(|c: char| c.is_whitespace()).next()?;
    if !["BRANCH", "FORK", "MERGE"].iter().any(|v| first.eq_ignore_ascii_case(v)) {
        return None;
    }
    aql_parser::parse(aql).ok().filter(|s| matches!(s, aql_parser::ast::Statement::Branch(_) | aql_parser::ast::Statement::Fork(_) | aql_parser::ast::Statement::Merge(_)))
}

/// `FORGET FROM ALL NAMESPACE "x"` with no WHERE: the namespace is dropped.
pub(crate) fn namespace_drop(aql: &str) -> Option<String> {
    let first = aql.trim_start().split(|c: char| c.is_whitespace()).next()?;
    if !first.eq_ignore_ascii_case("FORGET") {
        return None;
    }
    match aql_parser::parse(aql).ok()? {
        aql_parser::ast::Statement::Forget(f) if f.memory_type == aql_parser::ast::MemoryType::All && f.conditions.is_empty() => f.modifiers.namespace,
        _ => None,
    }
}

/// AQL v0.13 scripts: the statements of `aql` in order, each with the USE in force
/// (`default`: a session's namespace before the script) and run by `run` as AQL text.
/// The answer is the last statement's; a failing one stops the script, named by its
/// position as written. A script of USE alone answers with no data.
pub async fn run_script<F, Fut>(aql: &str, default: Option<&str>, run: F) -> Result<Value, String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Value, String>>,
{
    let steps = aql_parser::script_steps(aql, default).map_err(|e| e.to_string())?;
    let mut last = json!({ "success": true, "data": null });
    for (i, stmt) in steps {
        last = Box::pin(run(stmt.to_string())).await.map_err(|e| format!("statement {i}: {e}"))?;
    }
    Ok(last)
}

/// A RECALL naming several namespaces (`NAMESPACE ("a", "b")`, AQL v0.13).
pub(crate) fn across_namespaces(aql: &str) -> Option<(aql_parser::ast::RecallStmt, Vec<String>)> {
    if !aql.contains('(') || !aql.trim_start().get(..6).is_some_and(|v| v.eq_ignore_ascii_case("RECALL")) {
        return None;
    }
    match aql_parser::parse(aql).ok()? {
        aql_parser::ast::Statement::Recall(r) if !r.modifiers.namespaces.is_empty() => {
            let list = r.modifiers.namespaces.clone();
            Some((r, list))
        }
        _ => None,
    }
}

/// A RECALL across `namespaces`: run in each (as AQL text, by `run`), the records
/// together — a LIKE ranked by its confidence, else in the statement's ORDER BY —
/// then its LIMIT. Aggregates across namespaces aren't combined (refused).
pub async fn read_across<F, Fut>(stmt: &aql_parser::ast::RecallStmt, namespaces: &[String], run: F) -> Result<Value, String>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Value, String>>,
{
    if stmt.modifiers.aggregate.is_some() {
        return Err("AGGREGATE over NAMESPACE (…): aggregate one namespace at a time".into());
    }
    let mut records = Vec::new();
    let mut metadata = Value::Null;
    for ns in namespaces {
        let mut one = stmt.clone();
        one.modifiers.namespaces.clear();
        one.modifiers.namespace = Some(ns.clone());
        let v = Box::pin(run(aql_parser::ast::Statement::Recall(one).to_string())).await.map_err(|e| format!("namespace {ns:?}: {e}"))?;
        records.extend(v["data"]["records"].as_array().cloned().unwrap_or_default());
        metadata = v["metadata"].clone();
    }
    let num = |r: &Value, path: &[&str]| path.iter().fold(r, |v, k| &v[*k]).as_f64();
    match (&stmt.predicate, &stmt.modifiers.order_by) {
        (_, Some(o)) => {
            let field = o.field.split('.').collect::<Vec<_>>();
            let key = |r: &Value| {
                let d = field.iter().fold(&r["data"], |v, k| &v[*k]);
                if d.is_null() { field.iter().fold(r, |v, k| &v[*k]).clone() } else { d.clone() }
            };
            records.sort_by(|a, b| {
                let (x, y) = (key(a), key(b));
                let ord = match (x.as_f64(), y.as_f64()) {
                    (Some(p), Some(q)) => p.partial_cmp(&q).unwrap_or(std::cmp::Ordering::Equal),
                    _ => x.to_string().cmp(&y.to_string()),
                };
                if o.ascending { ord } else { ord.reverse() }
            });
        }
        (aql_parser::ast::Predicate::Like { .. }, None) => {
            records.sort_by(|a, b| num(b, &["metadata", "confidence"]).unwrap_or(0.0).partial_cmp(&num(a, &["metadata", "confidence"]).unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal));
        }
        _ => {}
    }
    if let Some(n) = stmt.modifiers.limit {
        records.truncate(n);
    }
    let count = records.len();
    Ok(json!({ "success": true, "data": { "type": "records", "records": records, "count": count }, "metadata": metadata }))
}

/// A statement's point in time: an RFC 3339 timestamp or a date, or a variable that
/// holds one.
pub(crate) fn time_of(t: &aql_parser::ast::TimePoint, variables: Option<&HashMap<String, Value>>) -> Result<adb_core::log::CommitTs, String> {
    let text = match t {
        aql_parser::ast::TimePoint::At { time } => time.clone(),
        aql_parser::ast::TimePoint::Variable { name } => variables
            .and_then(|v| v.get(name))
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| format!("AS OF ${name}: the variable has no time (pass it in `variables`)"))?,
    };
    let at = adb_core::TimePoint::At { time: text.clone() }.resolve(chrono::Utc::now()).ok_or_else(|| format!("AS OF {text:?}: not a time (RFC 3339, or a date)"))?;
    Ok(adb_core::log::CommitTs::from_datetime(at))
}

/// BRANCH / FORK / MERGE over `ops` (one engine, or a cluster's router).
pub(crate) async fn namespace_statement_on(ops: &dyn crate::namespaces::NsOps, stmt: aql_parser::ast::Statement, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
    use aql_parser::ast::Statement;
    use crate::branches::BaseKind;
    match stmt {
        Statement::Branch(n) => crate::namespaces::branch(ops, &n.from, &n.into, n.as_of.as_ref().map(|t| time_of(t, variables)).transpose()?, BaseKind::Branch).await,
        Statement::Fork(n) => crate::namespaces::branch(ops, &n.from, &n.into, n.as_of.as_ref().map(|t| time_of(t, variables)).transpose()?, BaseKind::Fork).await,
        Statement::Merge(m) => crate::namespaces::merge(ops, &m.from, &m.into, m.prefer.map(|p| matches!(p, aql_parser::ast::Prefer::Source))).await,
        _ => Err("not BRANCH, FORK or MERGE".into()),
    }
}

/// A live query's RECALL, as the statement a watch re-runs.
pub(crate) fn live_query(stmt: &aql_parser::ast::Statement) -> Option<aql_parser::ast::Statement> {
    match stmt {
        aql_parser::ast::Statement::Subscribe(aql_parser::ast::SubscribeStmt { target: aql_parser::ast::SubscribeTarget::Query(r), .. }) => {
            Some(aql_parser::ast::Statement::Recall((**r).clone()))
        }
        _ => None,
    }
}

/// What one engine holds (S3 option B step 3). A single node holds everything in
/// partition 0, as before. In a cluster each memory partition `n` holds its memory
/// table, log, branches and watches; the system partition holds the platform tables
/// and FlowR's engine store (and an empty memory table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRole {
    pub id: PartitionId,
    pub platform: bool,
    pub engine: bool,
}

impl PartitionRole {
    /// One node, one partition, everything in it (today's layout).
    pub const SINGLE: Self = Self { id: SINGLE_PARTITION, platform: true, engine: true };

    /// Memory partition `n`: the memory table only.
    pub fn memory(n: u16) -> Self {
        Self { id: PartitionId(n), platform: false, engine: false }
    }

    /// The system partition (id `n`): platform tables and FlowR's engine store.
    pub fn system(n: u16) -> Self {
        Self { id: PartitionId(n), platform: true, engine: true }
    }

    /// The log's directory in an engine's data directory: `p{n}`.
    pub fn log_dir(&self, dir: &std::path::Path) -> std::path::PathBuf {
        dir.join(format!("p{}", self.id.0))
    }
}

/// Open the partition log in `dir/p{n}`. A log this process just closed can still be
/// held for a moment by a background task finishing its step (the maintenance
/// timer is aborted, but stops only when the runtime next polls it): retry for up
/// to 10 s, yielding to the runtime between tries.
async fn open_wal(dir: &std::path::Path, role: PartitionRole) -> Result<LocalWal, String> {
    open_wal_sync(dir, role, adb_log::WalSync::Ack).await
}

async fn open_wal_sync(dir: &std::path::Path, role: PartitionRole, sync: adb_log::WalSync) -> Result<LocalWal, String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match LocalWal::open(role.id, WalConfig::new(role.log_dir(dir)).with_sync(sync)) {
            Err(adb_log::LogError::Locked(_)) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            other => return other.map_err(|e| e.to_string()),
        }
    }
}

/// The partition's Lance table in `dir/store`, sized by the embedding settings
/// (W9.10), and the embedder they name.
/// Object storage behind the node (S3 option B, `.ai/S3_OPTION_B_PLAN.md`): on by
/// default (`ADB_S3=on`), at `ADB_COLD_URI` (e.g. `s3://adb-1/prod/cluster-a`,
/// credentials from `AWS_*`; any Lance URI, a local path too). `ADB_S3=off`, or no
/// `ADB_COLD_URI`, leaves it out: Lance writes to local disk only, the log is the
/// only other copy, and no archive, checkpoint or WAL segment is made.
pub fn object_storage() -> Option<String> {
    let on = std::env::var("ADB_S3").map_or(true, |v| !matches!(v.trim().to_ascii_lowercase().as_str(), "off" | "0" | "false"));
    let uri = std::env::var("ADB_COLD_URI").ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    static SAID: std::sync::Once = std::sync::Once::new();
    match (on, uri) {
        (true, Some(uri)) => {
            SAID.call_once(|| tracing::info!(%uri, "object storage on"));
            Some(uri)
        }
        (true, None) => {
            SAID.call_once(|| tracing::warn!("object storage is on (ADB_S3) but ADB_COLD_URI is not set: writing to local disk only"));
            None
        }
        (false, _) => {
            SAID.call_once(|| tracing::info!("object storage off (ADB_S3=off): writing to local disk only"));
            None
        }
    }
}

pub(crate) async fn open_store(dir: &std::path::Path, partition: PartitionId) -> Result<(Arc<PartitionStore>, Option<Arc<adb_embed::Embedder>>), String> {
    let settings = adb_embed::EmbedConfig::load(dir)?;
    let (embedder, embedding_dim, embedding_model) = embedder_for(settings.as_ref());
    // The replica set's other nodes may embed on this node's model (C-17b).
    adb_embed::peers::serve_with(embedder.clone());
    let cold = object_storage();
    let store_cfg = StoreConfig { embedding_dim, embedding_model, cold, ..StoreConfig::new(dir.join("store")) };
    let store = Arc::new(PartitionStore::open(&store_cfg, partition).await.map_err(|e| e.to_string())?);
    Ok((store, embedder))
}

/// The nightly compaction time (UTC), `ADB_COMPACT_AT` = "HH:MM" (default 03:00;
/// "off" disables it — size thresholds still apply).
fn compaction_hour() -> Option<(u32, u32)> {
    let v = std::env::var("ADB_COMPACT_AT").unwrap_or_else(|_| "03:00".into());
    if v.eq_ignore_ascii_case("off") {
        return None;
    }
    let (h, m) = v.split_once(':')?;
    Some((h.parse().ok().filter(|h| *h < 24)?, m.parse().ok().filter(|m| *m < 60)?))
}

/// How often the expiry timer commits the `Expire`s that came due (D2 §4).
const EXPIRE_TICK: std::time::Duration = std::time::Duration::from_secs(1);
/// Rows the embedding backfill embeds per tick (MVP W4 T7), and how many ticks it
/// rests once nothing is left (new rows are embedded on write).
const BACKFILL_BATCH: usize = 256;
const BACKFILL_IDLE_TICKS: u32 = 60;

/// Aborts a task when dropped (a timer's companion task stops with it).
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// S3 option B: every `ADB_WAL_SEGMENT_MS` (default 1000), ship the log records object
/// storage lacks as WAL segments (`writes::segments`). Ends at once without object
/// storage. Holds only a weak reference.
fn spawn_shipper<L: adb_log::Log + 'static>(writes: std::sync::Weak<DurableWrites<L>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match writes.upgrade() {
            Some(w) if w.log_floor_on() => {}
            _ => return,
        }
        let every = crate::writes::segments::segment_interval();
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(w) = writes.upgrade() else { return };
            if let Err(e) = w.ship_segments().await {
                tracing::warn!(error = %e, "shipping WAL segments to object storage failed; the log keeps the records; retrying");
            }
        }
    })
}

/// Vectors for records stored without waiting for them (the vector queue):
/// woken as each is queued, embedding what is queued in batches until none is left.
/// A weak reference, like the timer's.
fn spawn_deferred_embedder<L: adb_log::Log + 'static>(writes: std::sync::Weak<DurableWrites<L>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(wake) = writes.upgrade().map(|w| w.embed_wake()) else { return };
        loop {
            // Woken by a queued record; the timeout only bounds a missed wake-up.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), wake.notified()).await;
            let Some(w) = writes.upgrade() else { return };
            if w.embeds_queued() == 0 {
                continue;
            }
            // How far vectors lag behind writes.
            crate::steps::value("embed.deferred queued", w.embeds_queued() as u64);
            match w.embed_deferred().await {
                Ok(n) => {
                    crate::steps::value("embed.deferred records per drain", n);
                    // Texts left queued: no compute node up, or the model failing. Not
                    // again at every write's wake-up: in a second.
                    if w.embeds_queued() > 0 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
                Err(e) => tracing::warn!(error = %e, "deferred embedding stopped; retrying"),
            }
        }
    })
}

/// The maintenance timer: every tick, `expire_due`, and the embedding backfill
/// while it finds work (then once a minute), on `writes` while it exists. Holds
/// only a weak reference, so it never keeps the log (and its directory lock) open.
fn spawn_expiry<L: adb_log::Log + 'static>(writes: std::sync::Weak<DurableWrites<L>>) -> tokio::task::JoinHandle<()> {
    let shipper = spawn_shipper(writes.clone());
    let embedder = spawn_deferred_embedder(writes.clone());
    tokio::spawn(async move {
        // S3 option B: the WAL segment shipper lives as long as this timer.
        let _shipper = AbortOnDrop(shipper);
        // Deferred vectors (the vector queue): as long as this timer too.
        let _embedder = AbortOnDrop(embedder);
        // The first tick after one interval, not at once: a just-opened engine is
        // often closed again right away (tests, `adb import`).
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + EXPIRE_TICK, EXPIRE_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut backfill_rest = 0u32;
        let mut ticks = 0u64;
        let nightly = compaction_hour();
        let mut last_nightly = None;
        // How often (ticks of 1 s) compaction thresholds are checked (`ADB_COMPACT_CHECK_SECS`, default 10).
        let check = std::env::var("ADB_COMPACT_CHECK_SECS").ok().and_then(|v| v.parse::<u64>().ok()).filter(|&v| v > 0).unwrap_or(10);
        let archive_every = std::env::var("ADB_ARCHIVE_EVERY_SECS").ok().and_then(|v| v.parse::<u64>().ok()).filter(|&v| v > 0).unwrap_or(30);
        loop {
            tick.tick().await;
            ticks += 1;
            let Some(w) = writes.upgrade() else { return };
            // W5: compaction on size (checked each minute) and nightly.
            let now = chrono::Utc::now();
            let night = nightly.is_some_and(|(h, m)| {
                use chrono::Timelike;
                now.hour() == h && now.minute() == m && last_nightly != Some(now.date_naive())
            });
            // The owner merges a partition's many small vector files into one.
            if ticks % 30 == 0 {
                w.merge_vector_files();
            }
            if night || (ticks % check == 0 && w.compaction_due().await) {
                if night {
                    last_nightly = Some(now.date_naive());
                }
                // In the background, within the node's budget: the tick goes on.
                w.compact_in_background();
            }
            // The log trimmed below what Lance holds, off the write path.
            if let Err(e) = w.purge_log().await {
                tracing::warn!(error = %e, "log purge failed; retrying");
            }
            // S3 option B: every `ADB_ARCHIVE_EVERY_SECS` (default 30), archive a
            // checkpoint to object storage if `ArchivePolicy` says it is due.
            if ticks % archive_every == 0 {
                if let Err(e) = w.archive_tick().await {
                    tracing::warn!(error = %e, "archive to object storage failed; the log keeps the records; retrying");
                }
            }
            // W5-L4: the platform tables, nightly and on their version count.
            if night || (ticks % check == check / 2 && w.platform_compaction_due().await) {
                if let Err(e) = w.compact_platform().await {
                    tracing::warn!(error = %e, "platform compaction failed; retrying later");
                }
            }
            // W9: subscriptions nobody reads (E-L3), each minute.
            if ticks % 60 == 15 {
                if let Err(e) = w.drop_idle_subscriptions().await {
                    tracing::warn!(error = %e, "dropping idle subscriptions failed; retrying later");
                }
            }
            // Stage H: finished runs' journals leave memory; snapshots when due.
            if ticks % 10 == 5 {
                if let Err(e) = w.engine_maintain().await {
                    tracing::warn!(error = %e, "engine upkeep failed; retrying later");
                }
            }
            w.sweep_cache();
            match w.expire_due().await {
                Ok(0) => {}
                Ok(n) => tracing::debug!(expired = n, "TTL expiry committed"),
                Err(e) => tracing::warn!(error = %e, "TTL expiry failed; retrying next tick"),
            }
            if backfill_rest > 0 {
                backfill_rest -= 1;
                continue;
            }
            // `ADB_EMBED_BACKFILL=off`: no backfill (records stored without a vector
            // stay without one until it is on again).
            if std::env::var("ADB_EMBED_BACKFILL").is_ok_and(|v| v.eq_ignore_ascii_case("off")) {
                backfill_rest = BACKFILL_IDLE_TICKS;
                continue;
            }
            match w.embed_backfill(BACKFILL_BATCH).await {
                Ok(0) => backfill_rest = BACKFILL_IDLE_TICKS,
                Ok(n) => tracing::info!(embedded = n, "embedding backfill"),
                Err(e) => {
                    tracing::warn!(error = %e, "embedding backfill failed; retrying in a minute");
                    backfill_rest = BACKFILL_IDLE_TICKS;
                }
            }
        }
    })
}

impl LocalEngine {
    /// Drop subscriptions idle past `ADB_SUB_IDLE` now (the expiry task does it each
    /// minute). Returns their ids.
    pub async fn drop_idle_subscriptions(&self) -> Result<Vec<String>, String> {
        let Some(w) = &self.durable else { return Ok(Vec::new()) };
        w.drop_idle_subscriptions().await.map_err(|e| e.to_string())
    }

    /// The executor (tests).
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    /// A durable engine on a fresh temporary directory, removed when the engine is
    /// dropped (an ADB without `--data-dir`, tests).
    pub async fn temporary() -> Result<Self, String> {
        let dir = tempfile::Builder::new().prefix("adb-").tempdir().map_err(|e| e.to_string())?;
        let (mut engine, _) = Self::durable(dir.path()).await?;
        engine.temp = Some(dir);
        Ok(engine)
    }
}

impl LocalEngine {
    /// A durable engine: replays the write-ahead log in `dir`, then logs every write
    /// there before applying it (stage C), and applies it to the partition's Lance
    /// table in `dir/store` too (stage D2). SEMANTIC and EPISODIC are read from that
    /// table (stage D3). Returns what the replay found.
    pub async fn durable(dir: impl Into<std::path::PathBuf>) -> Result<(Self, ReplayStats), String> {
        let dir = dir.into();
        let wal: EngineLog = Arc::new(open_wal(&dir, PartitionRole::SINGLE).await?);
        Self::durable_on(dir, wal).await
    }

    /// A durable engine for one partition of a cluster node (step 3): its log in
    /// `dir/p{n}`, its table in `dir/store/p{n}`, object storage under `{cold}/p{n}`;
    /// platform tables and the engine store only if `role` has them.
    pub async fn durable_partition(dir: impl Into<std::path::PathBuf>, role: PartitionRole) -> Result<(Self, ReplayStats), String> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| format!("partition data dir {}: {e}", dir.display()))?;
        let wal: EngineLog = Arc::new(open_wal(&dir, role).await?);
        Box::pin(Self::durable_with(dir, wal, role, false)).await
    }

    /// [`Self::durable_partition`] over `log` (a cluster owner's replicated log, step 5).
    pub async fn durable_partition_on(dir: impl Into<std::path::PathBuf>, role: PartitionRole, log: EngineLog) -> Result<(Self, ReplayStats), String> {
        // A cluster owner: nothing ships to object storage before its epoch begins.
        Box::pin(Self::durable_with(dir.into(), log, role, true)).await
    }

    /// Step 6, for readers on other nodes: committed records from `from` (at most `max`,
    /// none past the commit offset — what a failover could drop is never shown).
    pub async fn log_tail(&self, from: u64, max: usize) -> Result<Vec<adb_core::log::LogRecord>, String> {
        let log = self.log.as_ref().ok_or("no log")?;
        let committed = log.durable_offset().0;
        if from > committed {
            return Ok(Vec::new());
        }
        let mut recs = log.read(adb_core::log::LogOffset(from.max(1)), max.min((committed - from + 1) as usize)).await.map_err(|e| e.to_string())?;
        recs.retain(|r| r.offset.0 <= committed);
        Ok(recs)
    }

    /// Step 6: the offset every acknowledged write is at or below.
    pub fn commit_offset(&self) -> u64 {
        self.log.as_ref().map(|l| l.durable_offset().0).unwrap_or(0)
    }

    /// The partition's local WAL in `dir/p{n}`, opened.
    pub async fn open_partition_wal(dir: &std::path::Path, role: PartitionRole) -> Result<Arc<LocalWal>, String> {
        Self::open_partition_wal_sync(dir, role, adb_log::WalSync::Ack).await
    }

    /// As [`Self::open_partition_wal`], syncing as `sync` says: `Behind` for an owner
    /// whose replica holds every record, `Never` for a reader's copy.
    pub async fn open_partition_wal_sync(dir: &std::path::Path, role: PartitionRole, sync: adb_log::WalSync) -> Result<Arc<LocalWal>, String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("partition data dir {}: {e}", dir.display()))?;
        Ok(Arc::new(open_wal_sync(dir, role, sync).await?))
    }

    /// The local WAL in `dir/p0`, opened (a cluster node's state machine holds it).
    pub async fn open_local_wal(dir: &std::path::Path) -> Result<Arc<LocalWal>, String> {
        Ok(Arc::new(open_wal(dir, PartitionRole::SINGLE).await?))
    }

    /// A durable engine over `wal`: the local WAL, or (cluster, M3) the Raft log,
    /// whose appends commit through the replica set and whose reads are the
    /// node's local WAL. Everything else is as [`Self::durable`].
    pub async fn durable_on(dir: impl Into<std::path::PathBuf>, wal: EngineLog) -> Result<(Self, ReplayStats), String> {
        // Boxed: keeps the callers' future types shallow (the compiler's query depth).
        Box::pin(Self::durable_with(dir.into(), wal, PartitionRole::SINGLE, false)).await
    }

    /// The engine's data directory.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.dir
    }

    /// This partition's own table and registry, for a router reading other partitions'
    /// spaces (`crate::spaces`).
    pub fn local_spaces(&self) -> Option<crate::spaces::LocalSpaces> {
        let (store, barrier) = self.store.as_ref()?;
        let registry = Arc::clone(self.durable.as_ref()?.branches());
        Some(crate::spaces::LocalSpaces { store: Some(Arc::clone(store)), barrier: barrier.clone(), registry })
    }

    /// Read other spaces through `spaces` (a cluster's router, step 3).
    pub fn set_spaces(&self, spaces: std::sync::Weak<dyn crate::spaces::Spaces>) {
        if let Some(v) = &self.spaces {
            v.set_remote(spaces);
        }
    }

    /// The engine's writes and reads, as its executor uses them (the partition router
    /// runs statements spanning partitions over them, step 3).
    pub fn io(&self) -> (Arc<dyn Writes>, Arc<dyn crate::reads::Reads>) {
        self.executor.io()
    }

    /// The last record in this engine's log (offset, commit time).
    pub async fn last_commit(&self) -> Result<(u64, adb_core::log::CommitTs), String> {
        let d = self.durable.as_ref().ok_or("no durable log")?;
        d.last_commit().await.map_err(|e| e.to_string())
    }

    /// DB-17: copy only PROCEDURAL (and TOOLS' schemas, with `tools`) of `namespace` into `into`:
    /// nothing a client said or was learned about them travels.
    async fn materialise_procedures(&self, namespace: &str, into: &str, tools: bool) -> Result<u64, String> {
        let memories: &[MemoryType] = if tools { &[MemoryType::Procedural, MemoryType::Tools] } else { &[MemoryType::Procedural] };
        self.materialise_at(namespace, into, memories, now_ts()).await
    }

    /// `space`'s records of `memory` as they were at `at` (a branch namespace reads
    /// through its base, `crate::spaces::SpaceView::rows_at`).
    async fn space_rows(&self, memory: MemoryType, space: &str, at: adb_core::log::CommitTs) -> Result<Vec<adb_store::StoredRow>, String> {
        let v = self.spaces.as_ref().ok_or("space reads need a durable engine (start ADB with --data-dir)")?;
        v.rows_at(memory.into(), space, at, now_ts()).await.map_err(|e| e.to_string())
    }

    /// `namespace`'s `memories` as they were at `at`, copied into `into`.
    async fn materialise_at(&self, namespace: &str, into: &str, memories: &[MemoryType], at: adb_core::log::CommitTs) -> Result<u64, String> {
        let d = self.durable.as_ref().ok_or("copying needs a durable engine (start ADB with --data-dir)")?;
        if namespace == into {
            return Err(format!("promote into a new namespace, not {into:?} itself"));
        }
        let mut rows = Vec::new();
        for m in memories {
            rows.extend(self.space_rows(*m, namespace, at).await?);
        }
        d.materialise(into, rows).await.map_err(|e| e.to_string())
    }

    /// DB-4: (records, payload bytes) of this partition's namespaces `quota` covers.
    pub async fn usage_covered(&self, quota: &crate::branches::Quota) -> Result<(u64, u64), String> {
        let Some(d) = &self.durable else { return Ok((0, 0)) };
        let usage = d.usage().await.map_err(|e| e.to_string())?;
        Ok(usage.iter().filter(|(n, _)| quota.covers(n)).fold((0, 0), |a, (_, u)| (a.0 + u.0, a.1 + u.1)))
    }

    /// Step 3, a copy between partitions — out: `namespace`'s rows of `memories` as
    /// main (or `branch`) saw them at `at`.
    pub async fn copy_out(&self, namespace: &str, branch: Option<&str>, memories: &[MemoryType], at: adb_core::log::CommitTs) -> Result<Vec<adb_store::StoredRow>, String> {
        let space = space_of(namespace, branch);
        let mut rows = Vec::new();
        for m in memories {
            rows.extend(self.space_rows(*m, &space, at).await?);
        }
        Ok(rows)
    }

    /// Step 3, a copy between partitions — in: `rows` materialised in `into`.
    pub async fn copy_in(&self, into: &str, rows: Vec<adb_store::StoredRow>) -> Result<u64, String> {
        let d = self.durable.as_ref().ok_or("copying needs a durable engine (start ADB with --data-dir)")?;
        d.materialise(into, rows).await.map_err(|e| e.to_string())
    }

    /// The log point a `PointRef` names in `namespace` (offset, commit time).
    pub async fn resolve_point(&self, namespace: &str, at: &crate::branches::PointRef) -> Result<(u64, adb_core::log::CommitTs), String> {
        let d = self.durable.as_ref().ok_or("points need a durable engine (start ADB with --data-dir)")?;
        d.resolve_point(namespace, at).await.map_err(|e| e.to_string())
    }

    /// Ship the log records object storage lacks as WAL segments now (the shipper
    /// does this every `ADB_WAL_SEGMENT_MS`). Returns the offset object storage
    /// then holds every record up to.
    pub async fn ship_now(&self) -> Result<u64, String> {
        match &self.durable {
            Some(w) => w.ship_segments().await.map(|s| s.shipped).map_err(|e| e.to_string()),
            None => Ok(0),
        }
    }

    /// Take the partition over at owner epoch `epoch` (`segments`): record where this
    /// owner starts on object storage before it takes writes.
    pub async fn begin_epoch(&self, epoch: u64) -> Result<(), String> {
        match &self.durable {
            Some(w) => w.begin_epoch(epoch).await.map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    /// Archive to object storage now (S3 option B, `ADB_COLD_URI`): the memory table,
    /// the platform tables and the engine, and the log's floor with them. The
    /// maintenance timer does this when due (`ADB_ARCHIVE_MIN_BYTES`,
    /// `ADB_ARCHIVE_MAX_AGE_SECS`), checking every `ADB_ARCHIVE_EVERY_SECS`.
    pub async fn archive_now(&self) -> Result<(), String> {
        match &self.durable {
            Some(w) => w.archive_now().await.map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    /// A newer owner fenced this partition's engine: it takes no more writes.
    pub fn moved(&self) -> bool {
        self.durable.as_ref().is_some_and(|d| d.moved())
    }

    pub fn durable_writes(&self) -> Option<&Arc<DurableWrites<EngineLog>>> {
        self.durable.as_ref()
    }

    /// Wait until every record written so far has its vector (writes don't wait for
    /// them, the vector queue): for a `LIKE` read that must rank them all.
    pub async fn vectors_caught_up(&self, within: std::time::Duration) -> Result<(), String> {
        let Some(w) = &self.durable else { return Ok(()) };
        let deadline = std::time::Instant::now() + within;
        loop {
            if w.embeds_queued() == 0 && w.pending_embeddings().await.map_err(|e| e.to_string())? == 0 {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!("vectors still pending after {within:?}"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn durable_with(dir: std::path::PathBuf, wal: EngineLog, role: PartitionRole, epoch_owner: bool) -> Result<(Self, ReplayStats), String> {
        // Shared: the sequencer appends, subscribers read (stage E).
        let seq = Sequencer::new(Arc::clone(&wal), Epoch(1));
        let (store, embedder) = open_store(&dir, role.id).await?;
        // The field indexes it had, in the background (PF-40).
        store.warm(false);
        let opened = std::time::Instant::now();

        // W10: the hot cache follows every apply from here on (the catch-up too).
        store.set_cache(hot_cache(cache_config(&dir)?)).await.map_err(|e| e.to_string())?;
        let (mut writes, stats) =
            DurableWrites::open(seq, Arc::clone(&store)).await.map_err(|e| e.to_string())?;
        let platform = match role.platform {
            true => Some(Arc::new(adb_platform::PlatformStore::open(dir.join("platform")).await.map_err(|e| e.to_string())?)),
            false => None,
        };
        if let Some(p) = &platform {
            writes.attach_platform(Arc::clone(p)).await.map_err(|e| e.to_string())?;
        }
        // FlowR's engine store (stage H): its spaces under `<dir>/engine`.
        let engine = match role.engine {
            true => Some((adb_engine::EngineHost::open(dir.join("engine")).await.map_err(|e| e.to_string())?, None)),
            false => None,
        };
        let t_engine = std::time::Instant::now();
        let replayed = match engine {
            Some((engine, engine_next)) => writes.attach_engine(engine, engine_next).await.map_err(|e| e.to_string())?,
            None => 0,
        };
        tracing::info!(write_path = ?(t_engine - opened), engine_spaces = ?t_engine.elapsed(), "engine open steps");
        if replayed > 0 {
            tracing::info!(records = replayed, "engine spaces replayed");
        }
        let barrier = writes.barrier();
        // Readers outside the engine see the log with forgotten records hidden until
        // compaction redacts it (FORGET returns before that).
        match writes.seed_forgotten().await {
            Ok(n) if n > 0 => tracing::info!(lineages = n, "forgotten records in the log hidden until the next compaction"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "could not read the log's FORGETs at open"),
        }
        let redacting: Arc<dyn adb_log::Log> = Arc::new(crate::writes::RedactingLog { inner: Arc::clone(&wal), forgotten: writes.forgotten_set() });
        let watch_log: Arc<dyn adb_log::Log> = Arc::clone(&redacting);
        let gateway = barrier.as_ref().map(|b| {
            let log: Arc<dyn adb_log::Log> = redacting;
            let lineages: Arc<dyn adb_gateway::Lineages> = Arc::clone(&store) as Arc<dyn adb_gateway::Lineages>;
            Arc::new(Gateway::new(log, lineages, Arc::clone(writes.subscriptions()), b.watch()))
        });
        writes.set_embedder(embedder.clone());
        if epoch_owner {
            writes.await_epoch();
        }
        let registry = Arc::clone(writes.branches());
        writes.set_pins(Arc::new(move || registry.pins()));
        let spaces = crate::spaces::SpaceView::new(crate::spaces::LocalSpaces { store: Some(Arc::clone(&store)), barrier: barrier.clone(), registry: Arc::clone(writes.branches()) });
        writes.set_spaces(spaces.clone());
        let reads: Arc<dyn Reads> = Arc::new(
            LanceReads::new(Arc::clone(&store), barrier.clone())
                .with_embedder(embedder.clone())
                .with_layers(Some(Arc::clone(writes.branches())))
                .with_spaces(Some(spaces.clone())),
        );
        let durable = Arc::new(writes);
        let expiry = Some(spawn_expiry(Arc::downgrade(&durable)));
        let durable_handle = Arc::clone(&durable);
        let writes: Arc<dyn Writes> = durable;
        let executor = Executor::with_io(Arc::clone(&writes), Arc::clone(&reads));
        // W6: the watcher re-runs watched queries through an executor of its own and
        // reloads the registered watches.
        let watcher = match &barrier {
            Some(b) => {
                let runner = Arc::new(WatchRunner {
                    executor: Executor::with_io(Arc::clone(&writes), reads),
                    durable: Arc::clone(&durable_handle),
                });
                let applied = b.watch();
                let from = *applied.borrow();
                let watcher = adb_cq::Watcher::new(watch_log, applied, runner, from);
                for (id, value) in durable_handle.watches().await.map_err(|e| e.to_string())? {
                    let added = match serde_json::from_value::<adb_cq::WatchSpec>(value) {
                        // A cluster's watch across partitions: the router runs it.
                        Ok(spec) if spec.across.is_some() => Ok(()),
                        Ok(spec) => watcher.add(&id, spec).await.map(|_| ()).map_err(|e| e.to_string()),
                        Err(e) => Err(e.to_string()),
                    };
                    if let Err(e) = added {
                        tracing::warn!(cq = %id, error = %e, "watch not restored");
                    }
                }
                let task = adb_cq::Watcher::spawn(&watcher);
                Some((watcher, task))
            }
            None => None,
        };
        Ok((
            Self {
                executor,
                store: Some((store, barrier)),
                gateway,
                platform: platform.map(|p| (p, writes)),
                expiry,
                durable: Some(durable_handle),
                watcher,
                stats_cache: Default::default(),
                temp: None,
                dir,
                embedder,
                spaces: Some(spaces),
                log: Some(Arc::clone(&wal)),
            },
            stats,
        ))
    }

    /// The log in `dir` replayed into a fresh table in a temporary directory: an
    /// oracle for tests — the same committed records through a second path (log →
    /// apply), not the live table — valid while the log is complete (it is purged
    /// below Lance's durable offset every 256 records).
    pub async fn replay_only(dir: impl Into<std::path::PathBuf>) -> Result<(Self, ReplayStats), String> {
        let dir = dir.into();
        let temp = tempfile::Builder::new().prefix("adb-replay-").tempdir().map_err(|e| e.to_string())?;
        let log = temp.path().join("p0");
        std::fs::create_dir_all(&log).map_err(|e| e.to_string())?;
        for entry in std::fs::read_dir(dir.join("p0")).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            if entry.file_name() != "LOCK" && entry.path().is_file() {
                std::fs::copy(entry.path(), log.join(entry.file_name())).map_err(|e| e.to_string())?;
            }
        }
        let (mut engine, stats) = Self::durable(temp.path()).await?;
        engine.temp = Some(temp);
        Ok((engine, stats))
    }
}

impl LocalEngine {
    /// FlowR's engine store, space `space` (stage H); `None` without a data dir.
    pub fn engine_store(&self, space: &str) -> Option<crate::engine_store::AdbEngineStore<EngineLog>> {
        self.durable.as_ref().map(|d| crate::engine_store::AdbEngineStore::new(Arc::clone(d), space))
    }

    /// Engine upkeep now (tests and admin): evict finished journals, snapshot
    /// every space that changed (`force`) or is due.
    pub async fn engine_maintain(&self, force: bool) -> Result<(), String> {
        let Some(d) = &self.durable else { return Ok(()) };
        d.engine_maintain().await.map_err(|e| e.to_string())?;
        if force {
            d.engine_snapshot_now().await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Compact every engine space's journal now (tests and admin): `(space,
    /// row versions read, kept)`.
    pub async fn engine_compact_journals(&self) -> Result<Vec<(String, usize, usize)>, String> {
        let Some(d) = &self.durable else { return Ok(Vec::new()) };
        let done = d.engine_compact_journals().await.map_err(|e| e.to_string())?;
        Ok(done.into_iter().map(|(s, st)| (s, st.total, st.kept)).collect())
    }

    /// A cluster's watches across partitions, as this partition sees them: what its
    /// log after `after` can have changed ([`adb_cq::Watcher::touched_since`]).
    pub async fn cq_touched(&self, after: Option<u64>, watches: &[(String, adb_cq::WatchSpec)], wait: std::time::Duration) -> Result<adb_cq::Touched, String> {
        let (watcher, _) = self.watcher.as_ref().ok_or("watches need a durable engine (start ADB with --data-dir)")?;
        Ok(watcher.touched_since(after, watches, 10_000, wait).await)
    }

    /// Register a cluster's watch across partitions (on the system partition).
    pub async fn cq_register(&self, spec: &adb_cq::WatchSpec) -> Result<String, String> {
        let d = self.durable.as_ref().ok_or("watches need a durable engine (start ADB with --data-dir)")?;
        Ok(d.register_watch(serde_json::to_value(spec).map_err(|e| e.to_string())?).await.map_err(|e| e.to_string())?.0)
    }

    pub async fn cq_unregister(&self, id: &str) -> Result<(), String> {
        let d = self.durable.as_ref().ok_or("watches need a durable engine (start ADB with --data-dir)")?;
        d.unregister_watch(id).await.map_err(|e| e.to_string())
    }

    /// The cluster's registered watches across partitions (on the system partition).
    pub async fn cq_across(&self) -> Result<Vec<(String, adb_cq::WatchSpec)>, String> {
        let d = self.durable.as_ref().ok_or("watches need a durable engine (start ADB with --data-dir)")?;
        Ok(d.watches()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter_map(|(id, v)| serde_json::from_value::<adb_cq::WatchSpec>(v).ok().filter(|s| s.across.is_some()).map(|s| (id, s)))
            .collect())
    }

    /// Before `namespace` is dropped (AQL v0.13; [`crate::namespaces::before_drop`]).
    async fn before_namespace_drop(&self, namespace: &str) -> Result<(), String> {
        if self.durable.is_none() {
            return Ok(());
        }
        crate::namespaces::before_drop(&crate::namespaces::LocalNs(self), namespace).await
    }

    /// BRANCH / FORK / MERGE on one engine (every namespace here).
    async fn namespace_statement(&self, stmt: aql_parser::ast::Statement, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        let started = std::time::Instant::now();
        let v = namespace_statement_on(&crate::namespaces::LocalNs(self), stmt, variables).await?;
        Ok(to_wire(&QueryResult::subscription(v, started.elapsed().as_millis() as u64)))
    }

    /// A live query: `SUBSCRIBE RECALL …` (register, snapshot) and its UNSUBSCRIBE
    /// (W6). `None`: an UNSUBSCRIBE of something else (a topic subscription), which
    /// the planner handles.
    async fn watch(&self, stmt: aql_parser::ast::Statement, variables: Option<&HashMap<String, Value>>) -> Option<Result<Value, String>> {
        use aql_parser::ast::Statement;
        let started = std::time::Instant::now();
        let value = match (live_query(&stmt), stmt) {
            (Some(query), _) => self.watch_add(query, variables).await,
            (None, Statement::Unsubscribe(u)) => match &self.watcher {
                Some((watcher, _)) if watcher.contains(&u.subscription) => self.watch_remove(&u.subscription).await,
                _ => return None,
            },
            (None, _) => return None,
        };
        Some(value.map(|v| to_wire(&QueryResult::subscription(v, started.elapsed().as_millis() as u64))))
    }

    async fn watch_add(&self, query: aql_parser::ast::Statement, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        let (Some((watcher, _)), Some(durable)) = (&self.watcher, &self.durable) else {
            return Err("live queries need a durable engine (start ADB with --data-dir)".into());
        };
        let spec = adb_cq::WatchSpec::new(query, variables.cloned().unwrap_or_default(), None)?;
        // Refuse a query that fails before anything is committed.
        self.executor.execute_statement(&spec.statement, &spec.variables).await.map_err(|e| e.to_string())?;
        let (id, _) = durable.register_watch(serde_json::to_value(&spec).map_err(|e| e.to_string())?).await.map_err(|e| e.to_string())?;
        match watcher.add(&id, spec).await {
            Ok(frame) => serde_json::to_value(&frame).map_err(|e| e.to_string()),
            Err(e) => {
                let _ = durable.unregister_watch(&id).await;
                Err(e.to_string())
            }
        }
    }

    async fn watch_remove(&self, id: &str) -> Result<Value, String> {
        let (Some((watcher, _)), Some(durable)) = (&self.watcher, &self.durable) else {
            return Err("live queries need a durable engine (start ADB with --data-dir)".into());
        };
        durable.unregister_watch(id).await.map_err(|e| e.to_string())?;
        watcher.remove(id);
        Ok(json!({ "cq_id": id, "unsubscribed": true }))
    }
}


#[async_trait]
impl Engine for LocalEngine {
    fn flowr_engine(&self, space: &str) -> Option<Arc<dyn flowr_storage::engine::EngineStore>> {
        self.engine_store(space).map(|s| Arc::new(s) as Arc<dyn flowr_storage::engine::EngineStore>)
    }

    async fn engine_restore(&self, space: &str, changes: Vec<Value>, first: bool) -> Result<usize, String> {
        let Some(d) = &self.durable else { return Err("FlowR's engine store needs a durable engine (start ADB with --data-dir)".into()) };
        let changes = changes
            .into_iter()
            .map(serde_json::from_value::<flowr_storage::engine_core::Change>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("not an engine change: {e}"))?;
        d.engine_exec(space, |c| c.restore(changes, first)).await.map_err(|e| e.to_string())?.map_err(|e| e.to_string())
    }

    async fn subscription(&self, plan: aql_planner::SubscriptionPlan) -> Result<Value, String> {
        self.executor.execute_plan(&aql_planner::ExecutionPlan::Subscription(plan)).await.map(|r| to_wire(&r)).map_err(|e| e.to_string())
    }

    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        // AQL v0.13: a script (a USE, or statements separated by `;`) runs statement by
        // statement, each with the USE in force.
        if aql_parser::is_script(aql) {
            return run_script(aql, None, |stmt| async move { self.query(&stmt, variables).await }).await;
        }
        if let Some((stmt, list)) = across_namespaces(aql) {
            return read_across(&stmt, &list, |one| async move { self.query(&one, variables).await }).await;
        }
        // OB-3: every AQL statement (HTTP, Flight, MCP) is timed here, once.
        let t0 = std::time::Instant::now();
        // C-5: reads take a permit; beyond the bound they queue here instead of all taking
        // the CPU at once (CPU-saturating reads lost the leader its lead on AWS, 10 Nov).
        // Writes are not bounded: they wait on the log, not the CPU.
        // C-20: only reads that scan take a permit; keyed reads (LOOKUP, or `KEY id = …`)
        // are answered from the hot cache or an index in about a millisecond, and queued
        // behind 1 s scans they waited 1.3 s for nothing (AWS round 1).
        let _permit = if scans(aql) {
            let p = read_permits().acquire().await.map_err(|e| e.to_string())?;
            crate::steps::add("query.read permit wait (ADB_MAX_READS)", t0.elapsed());
            Some(p)
        } else {
            None
        };
        let t1 = std::time::Instant::now();
        let r = self.query_unobserved(aql, variables).await;
        if crate::steps::on() {
            let kind: &'static str = match adb_core::metrics::statement_of(aql).0 {
                "STORE" => "query.exec STORE",
                "UPDATE" => "query.exec UPDATE",
                "FORGET" => "query.exec FORGET",
                "LOOKUP" => "query.exec LOOKUP",
                "RECALL" if scans(aql) => "query.exec RECALL (scan)",
                "RECALL" => "query.exec RECALL (keyed)",
                "AGGREGATE" => "query.exec AGGREGATE",
                "SCAN" => "query.exec SCAN",
                _ => "query.exec other",
            };
            crate::steps::add(kind, t1.elapsed());
        }
        observe_query(aql, t0.elapsed(), &r);
        r
    }

    fn gateway(&self) -> Option<Arc<Gateway>> {
        self.gateway.clone()
    }

    fn platform(&self) -> Option<Arc<adb_platform::PlatformStore>> {
        self.platform.as_ref().map(|(p, _)| Arc::clone(p))
    }

    fn watcher(&self) -> Option<Arc<adb_cq::Watcher>> {
        self.watcher.as_ref().map(|(w, _)| Arc::clone(w))
    }

    async fn compact(&self) -> Result<Value, String> {
        let Some(w) = &self.durable else { return Err("compaction needs a durable engine (start ADB with --data-dir)".into()) };
        let platform = w.compact_platform().await.map_err(|e| e.to_string())?.map(|p| {
            json!({
                "tables": p.tables, "fragments_removed": p.fragments_removed, "old_versions": p.old_versions,
                "bytes_removed": p.bytes_removed, "took_ms": p.took.as_millis() as u64,
            })
        });
        let compacted = w.compact().await.map_err(|e| e.to_string())?;
        match compacted {
            Some(s) => Ok(json!({
                "total": s.total, "kept": s.kept, "bytes_before": s.bytes_before, "bytes_after": s.bytes_after,
                "took_ms": s.took.as_millis() as u64, "platform": platform,
            })),
            None => Err("no table to compact".into()),
        }
    }

    async fn embedding(&self) -> Result<Value, String> {
        let saved = adb_embed::EmbedConfig::load(&self.dir)?;
        let pending = match &self.durable {
            Some(w) => w.pending_embeddings().await.map_err(|e| e.to_string())?,
            None => 0,
        };
        Ok(json!({
            "source": if saved.is_some() { "settings" } else { "default" },
            "settings": saved.unwrap_or_default(),
            "active": self.embedder.as_ref().map(|e| json!({ "model": e.model(), "dimension": e.dimension() })),
            "pending": pending,
        }))
    }

    async fn set_embedding(&self, settings: Value) -> Result<Value, String> {
        if self.temp.is_some() {
            return Err("embedding settings need a data directory (start ADB with --data-dir)".into());
        }
        let c: adb_embed::EmbedConfig = serde_json::from_value(settings).map_err(|e| format!("embedding settings: {e}"))?;
        let e = Arc::new(adb_embed::Embedder::from_config(&c)?);
        e.verify().await?;
        let clears = self.embedder.as_ref().is_none_or(|a| a.model() != c.model || a.dimension() != c.dimension);
        c.save(&self.dir)?;
        tracing::info!(provider = ?c.provider, model = %c.model, dimension = c.dimension, "embedding settings saved; restarting to apply them");
        // After this response has gone out.
        tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            request_restart();
        });
        Ok(json!({ "saved": c, "restart": true, "reembed": clears }))
    }

    async fn cache(&self) -> Result<Value, String> {
        let Some((store, _)) = &self.store else { return Err("the hot cache needs a durable engine (start ADB with --data-dir)".into()) };
        let saved = adb_store::CacheConfig::load(&self.dir)?;
        let source = match (&saved, std::env::var_os("ADB_CACHE_RULES")) {
            (Some(_), _) => "settings",
            (None, Some(_)) => "environment",
            (None, None) => "default",
        };
        let hot = store.hot();
        Ok(json!({
            "mode": cache_mode(),
            "source": source,
            "settings": hot.as_ref().map(|h| h.config()).or(saved).unwrap_or_default(),
            "stats": hot.map(|h| h.stats()),
        }))
    }

    async fn set_cache(&self, settings: Value) -> Result<Value, String> {
        let Some((store, _)) = &self.store else { return Err("the hot cache needs a durable engine (start ADB with --data-dir)".into()) };
        let c: adb_store::CacheConfig = serde_json::from_value(settings).map_err(|e| format!("cache settings: {e}"))?;
        c.check()?;
        if self.temp.is_none() {
            c.save(&self.dir)?;
        }
        let started = std::time::Instant::now();
        store.set_cache(hot_cache(c.clone())).await.map_err(|e| e.to_string())?;
        tracing::info!(rules = c.rules.len(), max_bytes = c.max_bytes, took = ?started.elapsed(), "hot cache rules saved and applied");
        Ok(json!({ "saved": c, "mode": cache_mode(), "stats": store.hot().map(|h| h.stats()), "took_ms": started.elapsed().as_millis() as u64 }))
    }

    async fn sql(&self, sql: &str, limit: Option<usize>) -> Result<crate::sql::SqlResult, String> {
        let Some((store, barrier)) = &self.store else { return Err("SQL needs a durable engine (start ADB with --data-dir)".into()) };
        // Read-your-writes, as AQL reads: the table holds every committed record.
        if let Some(b) = barrier {
            b.wait_for_committed().await;
        }
        crate::sql::run(Some(Arc::clone(store)), self.platform(), sql, limit).await
    }

    async fn memory_raw(&self) -> Result<crate::sql::SqlResult, String> {
        let Some((store, barrier)) = &self.store else { return Err("SQL needs a durable engine (start ADB with --data-dir)".into()) };
        if let Some(b) = barrier {
            b.wait_for_committed().await;
        }
        crate::sql::memory_raw(store).await
    }

    async fn branch_create(&self, namespace: &str, name: &str, parent: Option<&str>, _created_by: Option<&str>) -> Result<crate::branches::Branch, String> {
        crate::review::create(&crate::namespaces::LocalNs(self), namespace, name, parent).await
    }

    async fn branch_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        let child = crate::review::branch_ns(namespace, name);
        if crate::namespaces::NsOps::base_of(&crate::namespaces::LocalNs(self), &child).await?.is_none() {
            return Ok(false);
        }
        self.query(&format!("FORGET FROM ALL NAMESPACE {}", serde_json::to_string(&child).unwrap_or_default()), None).await?;
        Ok(true)
    }

    async fn branch_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Branch>, String> {
        let ops = crate::namespaces::LocalNs(self);
        match namespace {
            Some(ns) => crate::review::list(&ops, ns).await,
            // Every namespace here with branches (their sources hold the point tags).
            None => {
                let spaces = self.local_spaces().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
                let mut sources: Vec<String> = spaces
                    .registry
                    .tags(None)
                    .into_iter()
                    .filter(|t| t.name.starts_with("branch.") && !t.namespace.contains('@'))
                    .map(|t| t.namespace)
                    .collect();
                sources.sort();
                sources.dedup();
                let mut out = Vec::new();
                for ns in sources {
                    out.extend(crate::review::list(&ops, &ns).await?);
                }
                Ok(out)
            }
        }
    }

    async fn tag_create(&self, namespace: &str, name: &str, created_by: Option<&str>) -> Result<crate::branches::Tag, String> {
        let d = self.durable.as_ref().ok_or("tags need a durable engine (start ADB with --data-dir)")?;
        d.tag_create(namespace, name, created_by).await.map_err(|e| e.to_string())
    }

    async fn tag_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        let d = self.durable.as_ref().ok_or("tags need a durable engine (start ADB with --data-dir)")?;
        d.tag_drop(namespace, name).await.map_err(|e| e.to_string())
    }

    async fn tag_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Tag>, String> {
        Ok(self.durable.as_ref().map(|d| d.branches().tags(namespace)).unwrap_or_default())
    }

    async fn layers_set(&self, namespace: &str, layers: Vec<crate::branches::Layer>) -> Result<crate::branches::Layers, String> {
        let d = self.durable.as_ref().ok_or("layers need a durable engine (start ADB with --data-dir)")?;
        d.layers_set(namespace, layers).await.map_err(|e| e.to_string())
    }

    async fn layers_get(&self, namespace: &str) -> Result<Vec<crate::branches::Layer>, String> {
        Ok(self.durable.as_ref().map(|d| d.branches().layers_of(namespace)).unwrap_or_default())
    }

    async fn branch_promote(&self, namespace: &str, name: &str, into: &str, _allow_stale: bool) -> Result<Value, String> {
        crate::review::promote(&crate::namespaces::LocalNs(self), namespace, name, into).await
    }

    async fn branch_rebase(&self, namespace: &str, name: &str, main_offset: u64, resolutions: Vec<crate::merge::Resolution>) -> Result<Value, String> {
        crate::review::rebase(&crate::namespaces::LocalNs(self), namespace, name, main_offset, resolutions).await
    }

    async fn space_clone(&self, namespace: &str, into: &str) -> Result<Value, String> {
        let n = self.materialise_at(namespace, into, &PROMOTED, now_ts()).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN } }))
    }

    async fn space_copy_procedures(&self, namespace: &str, into: &str, tools: bool) -> Result<Value, String> {
        let n = self.materialise_procedures(namespace, into, tools).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN }, "procedures_only": true }))
    }

    async fn space_clone_at(&self, namespace: &str, into: &str, at: &crate::branches::PointRef) -> Result<Value, String> {
        let d = self.durable.as_ref().ok_or("cloning a memory space needs a durable engine (start ADB with --data-dir)")?;
        let (offset, ts) = d.resolve_point(namespace, at).await.map_err(|e| e.to_string())?;
        let n = self.materialise_at(namespace, into, &PROMOTED, ts).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN, "point": { "offset": offset, "ts": ts.0 } } }))
    }

    async fn quota_set(&self, pattern: &str, max_bytes: Option<u64>, max_records: Option<u64>, history_days: Option<u64>) -> Result<crate::branches::Quota, String> {
        let d = self.durable.as_ref().ok_or("quotas need a durable engine (start ADB with --data-dir)")?;
        d.quota_set(pattern, max_bytes, max_records, history_days).await.map_err(|e| e.to_string())
    }

    async fn quota_drop(&self, pattern: &str) -> Result<bool, String> {
        let d = self.durable.as_ref().ok_or("quotas need a durable engine (start ADB with --data-dir)")?;
        d.quota_drop(pattern).await.map_err(|e| e.to_string())
    }

    async fn quota_list(&self) -> Result<Vec<crate::branches::Quota>, String> {
        Ok(self.durable.as_ref().map(|d| d.branches().quotas()).unwrap_or_default())
    }

    async fn space_usage(&self, namespace: Option<&str>) -> Result<Value, String> {
        let d = self.durable.as_ref().ok_or("usage needs a durable engine (start ADB with --data-dir)")?;
        let usage = d.usage().await.map_err(|e| e.to_string())?;
        let namespaces: serde_json::Map<String, Value> = usage
            .into_iter()
            .filter(|(n, _)| namespace.is_none_or(|w| w == n))
            .map(|(n, (records, bytes))| (n, json!({ "records": records, "bytes": bytes })))
            .collect();
        Ok(json!({ "namespaces": namespaces }))
    }

    async fn layers_dependents(&self, space: &str) -> Result<Vec<crate::branches::Dependent>, String> {
        Ok(self.durable.as_ref().map(|d| d.branches().dependents(space)).unwrap_or_default())
    }

    async fn space_stats(&self, namespace: &str, branch: Option<&str>, max_age: std::time::Duration) -> Result<Value, String> {
        let key = format!("{namespace}\u{0}{}", branch.unwrap_or_default());
        if let Some((at, v)) = self.stats_cache.lock().unwrap().get(&key) {
            if at.elapsed() < max_age {
                let mut v = v.clone();
                v["age_ms"] = json!(at.elapsed().as_millis() as u64);
                return Ok(v);
            }
        }
        let space = space_of(namespace, branch);
        let now = now_ts();
        let mut tiers = serde_json::Map::new();
        for m in [MemoryType::Working, MemoryType::Tools, MemoryType::Procedural, MemoryType::Semantic, MemoryType::Episodic] {
            let rows = self.space_rows(m, &space, now).await?;
            let bytes: usize = rows.iter().map(|r| r.data.value.to_string().len()).sum();
            let last = rows.iter().map(|r| r.commit_ts.0).max();
            let name = serde_json::to_value(m).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
            tiers.insert(name, json!({ "count": rows.len(), "bytes": bytes, "last_write_ts": last }));
        }
        let v = json!({ "namespace": namespace, "branch": branch.unwrap_or(crate::branches::MAIN), "tiers": tiers, "age_ms": 0 });
        let mut cache = self.stats_cache.lock().unwrap();
        if cache.len() >= 4096 {
            cache.clear();
        }
        cache.insert(key, (std::time::Instant::now(), v.clone()));
        Ok(v)
    }

    async fn space_records(&self, namespace: &str, memory: MemoryType, branch: Option<&str>, after: Option<&str>, limit: usize) -> Result<Value, String> {
        let mut rows = self.space_rows(memory, &space_of(namespace, branch), now_ts()).await?;
        rows.sort_by(|a, b| (b.commit_ts.0, &b.lineage_id).cmp(&(a.commit_ts.0, &a.lineage_id)));
        let cursor = match after {
            None => None,
            Some(c) => {
                let (ts, lineage) = c.split_once('/').ok_or("after: use the next of the page before")?;
                Some((ts.parse::<i64>().map_err(|_| "after: use the next of the page before")?, lineage.to_string()))
            }
        };
        let limit = limit.clamp(1, 500);
        let mut page = rows.iter().filter(|r| cursor.as_ref().is_none_or(|(ts, l)| (r.commit_ts.0, &r.lineage_id) < (*ts, l))).peekable();
        let mut records = Vec::new();
        let mut next = None;
        while let Some(r) = page.next() {
            records.push(json!({ "lineage": r.lineage_id, "key": r.data.key, "value": r.data.value, "commit_ts": r.commit_ts.0, "version": r.version }));
            if records.len() == limit {
                if page.peek().is_some() {
                    next = Some(format!("{}/{}", r.commit_ts.0, r.lineage_id));
                }
                break;
            }
        }
        Ok(json!({ "records": records, "next": next }))
    }

    /// Every namespace's writes are in this engine's log (a branch's too: it is the
    /// namespace `<ns>@<name>`): one gateway; the feed filters by namespace.
    async fn space_gateway(&self, _namespace: &str) -> Result<Arc<Gateway>, String> {
        self.gateway().ok_or_else(|| "the space feed needs a durable engine (start ADB with --data-dir)".to_string())
    }

    async fn branch_conflicts(&self, namespace: &str, name: &str, memories: &[MemoryType]) -> Result<Value, String> {
        crate::review::conflicts(&crate::namespaces::LocalNs(self), namespace, name, memories).await
    }

    async fn branch_merge(&self, namespace: &str, name: &str, memories: &[MemoryType], main_offset: u64, resolutions: Vec<crate::merge::Resolution>, by: Option<&str>) -> Result<Value, String> {
        crate::review::merge(&crate::namespaces::LocalNs(self), namespace, name, memories, main_offset, resolutions, by).await
    }

    async fn merge_revert(&self, namespace: &str, merge_id: &str, by: Option<&str>, force_skip: bool) -> Result<Value, String> {
        crate::review::revert(&crate::namespaces::LocalNs(self), namespace, merge_id, by, force_skip).await
    }

    async fn platform_write_many(&self, effects: Vec<adb_core::log::Effect>) -> Result<u64, String> {
        match &self.platform {
            Some((_, writes)) => writes.platform_many(effects).await.map_err(|e| e.to_string()),
            None => Err("platform tables need a durable engine (start ADB with --data-dir)".into()),
        }
    }

    async fn platform_write(&self, effect: adb_core::log::Effect) -> Result<u64, String> {
        match &self.platform {
            Some((_, writes)) => writes.platform(effect).await.map_err(|e| e.to_string()),
            None => Err("platform tables need a durable engine (start ADB with --data-dir)".into()),
        }
    }

    async fn stats(&self) -> Result<Value, String> {
        let mut counts = Map::new();
        for (name, t) in [
            ("working", MemoryType::Working),
            ("episodic", MemoryType::Episodic),
            ("procedural", MemoryType::Procedural),
            ("tools", MemoryType::Tools),
            ("semantic", MemoryType::Semantic),
        ] {
            let n = match &self.store {
                Some((store, barrier)) => {
                    if let Some(b) = barrier {
                        b.wait_for_committed().await;
                    }
                    let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
                    store.count_current(t.into(), now).await.map_err(|e| e.to_string())? as usize
                }
                None => 0,
            };
            counts.insert(name.into(), json!(n));
        }
        Ok(Value::Object(counts))
    }
}

/// Whether a statement writes (its verb), for the memory point.
fn is_write(aql: &str) -> bool {
    let verb = aql.trim_start().split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    matches!(verb.as_str(), "STORE" | "UPDATE" | "FORGET" | "LINK" | "UNLINK" | "MERGE" | "PIPELINE")
}

/// ADB result → ClawDB wire `QueryResult`.
pub fn to_wire(r: &QueryResult) -> Value {
    let mut metadata = Map::new();
    metadata.insert("execution_time_ms".into(), json!(r.metadata.execution_time_ms));
    if let Some(n) = r.metadata.records_scanned {
        metadata.insert("records_scanned".into(), json!(n));
    }
    if let Some(n) = r.metadata.records_returned {
        metadata.insert("records_returned".into(), json!(n));
    }
    let mut out = Map::new();
    out.insert("success".into(), json!(r.success));
    if let Some(data) = wire_data(&r.data) {
        out.insert("data".into(), data);
    }
    out.insert("metadata".into(), Value::Object(metadata));
    Value::Object(out)
}

/// `ResultSet` → ClawDB `QueryData` (`stored` / `records` / `affected`), with ADB's
/// extra result kinds under their own `type` tags.
fn wire_data(data: &ResultSet) -> Option<Value> {
    Some(match data {
        ResultSet::Records { records } => json!({
            "type": "records",
            "records": records.iter().map(wire_record).collect::<Vec<_>>(),
            "count": records.len(),
        }),
        ResultSet::Stored { record } => json!({ "type": "stored", "record": wire_record(record) }),
        ResultSet::Count { count } => json!({ "type": "affected", "count": count }),
        ResultSet::Aggregation { values } => json!({ "type": "aggregation", "values": values }),
        ResultSet::Links { links } => json!({ "type": "links", "links": links, "count": links.len() }),
        ResultSet::Pipeline { steps } => json!({
            "type": "pipeline",
            "steps": steps.iter().map(to_wire).collect::<Vec<_>>(),
        }),
        ResultSet::Reflect { sources, links } => json!({
            "type": "reflect",
            "sources": sources.iter().map(|s| json!({
                "memory_type": s.memory_type,
                "records": s.records.iter().map(wire_record).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "links": links,
        }),
        ResultSet::Subscription { value } => {
            let mut v = json!({ "type": "subscription" });
            if let (Some(m), Some(extra)) = (v.as_object_mut(), value.as_object()) {
                m.extend(extra.clone());
            }
            v
        }
        ResultSet::Empty => return None,
    })
}

/// ADB `MemoryRecord` → ClawDB record: `{id, memory_type, data, metadata{namespace,
/// scope, created_at, accessed_at, version, ttl_ms?}}`.
pub fn wire_record(r: &MemoryRecord) -> Value {
    let m = &r.metadata;
    let mut meta = Map::new();
    meta.insert("namespace".into(), json!(m.namespace.as_deref().unwrap_or("default")));
    meta.insert("scope".into(), json!(m.scope));
    meta.insert("created_at".into(), json!(m.created_at));
    meta.insert("accessed_at".into(), json!(m.accessed_at));
    meta.insert("version".into(), json!(m.version));
    if let Some(ttl) = m.ttl {
        meta.insert("ttl_ms".into(), json!(ttl.as_millis() as u64));
    }
    if let Some(c) = m.confidence {
        meta.insert("confidence".into(), json!(c));
    }
    json!({
        "id": r.id,
        "memory_type": r.memory_type,
        "data": r.data,
        "metadata": Value::Object(meta),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// C-20 under AQL v0.13: a keyed RECALL (the planner's exact lookup) takes no read
    /// permit; anything that scans does.
    #[test]
    fn keyed_recalls_skip_the_read_permit() {
        for keyed in [
            r#"RECALL FROM SEMANTIC WHERE id = "f1" NAMESPACE "ns""#,
            r#"recall from working where key = $k namespace "ns""#,
            r#"RECALL FROM EPISODIC WHERE key = "e1" NAMESPACE "ns" WITH LINKS"#,
        ] {
            assert!(!scans(keyed), "{keyed}");
        }
        for scan in [
            r#"RECALL FROM SEMANTIC WHERE id = "f1""#,
            r#"RECALL FROM SEMANTIC WHERE topic = "t1" NAMESPACE "ns" LIMIT 10"#,
            r#"RECALL FROM SEMANTIC WHERE id = "f1" AND topic = "t" NAMESPACE "ns""#,
            r#"RECALL FROM SEMANTIC WHERE key = "k" NAMESPACE "ns" AGGREGATE COUNT(*) AS n"#,
            r#"RECALL FROM SEMANTIC NAMESPACE "ns""#,
        ] {
            assert!(scans(scan), "{scan}");
        }
        assert!(!scans(r#"STORE INTO SEMANTIC (key = "k") NAMESPACE "ns""#));
        // Newest-first episodes within one namespace: the recent-episodes index.
        assert!(!scans(r#"RECALL FROM EPISODIC LIMIT 10 NAMESPACE "ns""#));
        for scan in [
            r#"RECALL FROM EPISODIC LIMIT 10"#,
            r#"RECALL FROM EPISODIC WHERE kind = "call" LIMIT 10 NAMESPACE "ns""#,
            r#"RECALL FROM EPISODIC ORDER BY n LIMIT 10 NAMESPACE "ns""#,
            r#"RECALL FROM EPISODIC LIMIT 100000 NAMESPACE "ns""#,
            r#"RECALL FROM SEMANTIC LIMIT 10 NAMESPACE "ns""#,
        ] {
            assert!(scans(scan), "{scan}");
        }
    }

    #[tokio::test]
    async fn wire_format_matches_flowr_adapter() {
        let engine = LocalEngine::temporary().await.unwrap();
        let stored = engine
            .query(r#"STORE INTO SEMANTIC (key = "k1", content = "hello") NAMESPACE "ns1""#, None)
            .await
            .unwrap();
        assert_eq!(stored["success"], json!(true));
        assert_eq!(stored["data"]["type"], json!("stored"));

        let recalled = engine.query(r#"RECALL FROM SEMANTIC WHERE key = "k1""#, None).await.unwrap();
        // FlowR's ClawDbMemory reads `data.records[*]` of this object (it unwraps the
        // REST envelope first): id, data.key, data.content, memory_type, metadata.created_at.
        let records = recalled["data"]["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(recalled["data"]["count"], json!(1));
        let rec = &records[0];
        assert!(rec["id"].is_string());
        assert_eq!(rec["memory_type"], json!("SEMANTIC"));
        assert_eq!(rec["data"]["content"], json!("hello"));
        assert!(rec["metadata"]["created_at"].is_string());
        assert_eq!(rec["metadata"]["namespace"], json!("ns1"));
        assert_eq!(rec["metadata"]["scope"], json!("private"));
    }

    #[tokio::test]
    async fn stats_counts_per_type() {
        let engine = LocalEngine::temporary().await.unwrap();
        engine.query(r#"STORE INTO EPISODIC (event = "e")"#, None).await.unwrap();
        let s = engine.stats().await.unwrap();
        assert_eq!(s["episodic"], json!(1));
        assert_eq!(s["working"], json!(0));
        // The adapter sums all numeric values.
        let total: u64 = s.as_object().unwrap().values().filter_map(|v| v.as_u64()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn request_variables_bind() {
        let engine = LocalEngine::temporary().await.unwrap();
        engine.query(r#"STORE INTO EPISODIC (pod = "payments")"#, None).await.unwrap();
        engine.query(r#"STORE INTO EPISODIC (pod = "auth")"#, None).await.unwrap();
        let vars: HashMap<String, Value> = [("pod".to_string(), json!("auth"))].into();
        let r = engine.query(r#"RECALL FROM EPISODIC WHERE pod = {pod}"#, Some(&vars)).await.unwrap();
        assert_eq!(r["data"]["count"], json!(1));
        assert_eq!(r["data"]["records"][0]["data"]["pod"], json!("auth"));
    }

    #[tokio::test]
    async fn errors_are_err() {
        let engine = LocalEngine::temporary().await.unwrap();
        assert!(engine.query("THIS IS NOT AQL", None).await.is_err());
        assert!(engine.query(r#"UNSUBSCRIBE "no-such-watch""#, None).await.is_ok(), "unknown: unsubscribed = false");
        assert!(engine.query("RECALL FROM SEMANTIC LIKE $q", None).await.is_err(), "LIKE without a value");
    }
}

/// The memories a promoted or cloned agent takes: what it knows and can do. EPISODIC
/// stays with the original agent; WORKING is session state.
const PROMOTED: [MemoryType; 3] = [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools];

fn now_ts() -> adb_core::log::CommitTs {
    adb_core::log::CommitTs::from_datetime(adb_core::time::now())
}

/// The namespace of branch `branch` of `namespace` (`<ns>@<branch>`), or `namespace`
/// itself for main.
fn space_of(namespace: &str, branch: Option<&str>) -> String {
    crate::review::branch_ns(namespace, branch.unwrap_or_default())
}

impl LocalEngine {
    async fn query_unobserved(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        if let Some(stmt) = watch_statement(aql) {
            if let Some(answer) = self.watch(stmt, variables).await {
                return answer;
            }
        }
        if let Some(stmt) = namespace_statement(aql) {
            return self.namespace_statement(stmt, variables).await;
        }
        if let Some(ns) = namespace_drop(aql) {
            self.before_namespace_drop(&ns).await?;
        }
        let started = std::time::Instant::now();
        let before = match &self.durable {
            Some(d) => Some(d.point().await),
            None => None,
        };
        crate::steps::add("query.point before", started.elapsed());
        let result = match variables {
            Some(vars) if !vars.is_empty() => self.executor.execute_with_vars(aql, vars).await,
            _ => self.executor.execute(aql).await,
        };
        let executed = started.elapsed();
        let mut wire = result.map(|r| to_wire(&r)).map_err(|e| e.to_string());
        // DB-7: the memory point: a write's own record, else the last commit before
        // the statement ran (the answer includes at least that).
        if let (Ok(w), Some(d), Some(before)) = (&mut wire, &self.durable, before) {
            let after = d.point().await;
            let (off, ts) = if after.0 > before.0 && is_write(aql) { after } else { before };
            if let Some(m) = w.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let at = ts.to_datetime().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
                m.insert("point".into(), json!({ "offset": off, "ts": ts.0, "at": at }));
            }
        }
        tracing::debug!(?executed, total = ?started.elapsed(), "query");
        wire
    }
}

/// Whether `aql` is a read that scans (RECALL or SCAN without `KEY`): the reads the
/// permits below bound (C-5, C-20). Writes and keyed reads are not bounded.
pub(crate) fn scans(aql: &str) -> bool {
    adb_core::metrics::statement_of(aql).0 == "RECALL" && !bounded_recall(aql)
}

/// A RECALL answered by a bounded number of lookups, not a scan — the two shapes, read
/// from the tokens (parsing and planning every read to decide its permit cost the key
/// reads ~10%, Hetzner p15):
/// - an exact lookup: `RECALL FROM <memory> WHERE key|id = <value> NAMESPACE <ns>`
///   (optionally `WITH LINKS`), as the planner plans it (not TOOLS, which loads);
/// - newest-first episodes the recent-episodes index answers: `RECALL FROM EPISODIC
///   LIMIT k NAMESPACE <ns>` (either order, k within what the index keeps).
///
/// Anything else counts as a scan (a permit), the cautious side.
fn bounded_recall(aql: &str) -> bool {
    let t = tokens(aql);
    let t: Vec<&str> = t.iter().map(String::as_str).collect();
    let up = |i: usize| t.get(i).map(|w| w.to_ascii_uppercase());
    let is = |i: usize, w: &str| up(i).as_deref() == Some(w);
    let value = |i: usize| t.get(i).is_some_and(|w| w.starts_with('"') || w.starts_with('$') || w.chars().next().is_some_and(|c| c.is_ascii_digit() || c == '-'));
    if !(is(0, "RECALL") && is(1, "FROM")) {
        return false;
    }
    let memory = up(2).unwrap_or_default();
    // RECALL FROM m WHERE key|id = v NAMESPACE ns [WITH LINKS]
    if ["SEMANTIC", "EPISODIC", "PROCEDURAL", "WORKING"].contains(&memory.as_str())
        && is(3, "WHERE")
        && (is(4, "KEY") || is(4, "ID"))
        && t.get(5) == Some(&"=")
        && value(6)
        && is(7, "NAMESPACE")
        && value(8)
        && (t.len() == 9 || (t.len() == 11 && is(9, "WITH") && is(10, "LINKS")))
    {
        return true;
    }
    // RECALL FROM EPISODIC LIMIT k NAMESPACE ns | NAMESPACE ns LIMIT k
    if memory == "EPISODIC" && t.len() == 7 {
        let k = match (up(3).as_deref(), up(5).as_deref()) {
            (Some("LIMIT"), Some("NAMESPACE")) if value(6) => t[4].parse::<usize>().ok(),
            (Some("NAMESPACE"), Some("LIMIT")) if value(4) => t[6].parse::<usize>().ok(),
            _ => None,
        };
        return k.is_some_and(|k| k <= adb_store::recent_index::keep());
    }
    false
}

/// AQL's tokens, cheaply: words, `$vars`, numbers, quoted strings (kept whole, quotes
/// included) and single punctuation.
fn tokens(aql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = aql.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '"' {
            let mut s = String::from(chars.next().unwrap());
            while let Some(d) = chars.next() {
                s.push(d);
                if d == '\\' {
                    if let Some(e) = chars.next() {
                        s.push(e);
                    }
                } else if d == '"' {
                    break;
                }
            }
            out.push(s);
        } else if c.is_alphanumeric() || c == '_' || c == '$' || c == '-' || c == '.' {
            let mut s = String::new();
            while let Some(&d) = chars.peek() {
                if d.is_alphanumeric() || d == '_' || d == '$' || d == '-' || d == '.' {
                    s.push(d);
                    chars.next();
                } else {
                    break;
                }
            }
            out.push(s);
        } else {
            out.push(chars.next().unwrap().to_string());
        }
    }
    out
}

/// Reads running at once (C-5): `ADB_MAX_READS`, default twice the CPUs (at least 2).
pub(crate) fn read_permits() -> &'static tokio::sync::Semaphore {
    static P: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    P.get_or_init(|| {
        let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2);
        let n = std::env::var("ADB_MAX_READS").ok().and_then(|v| v.parse().ok()).unwrap_or((cpus * 2).max(2));
        tracing::info!(permits = n, "concurrent reads bounded (ADB_MAX_READS)");
        tokio::sync::Semaphore::new(n)
    })
}

/// OB-3: one AQL statement's metrics; a `debug` line (`adb::query`) for each, an `info`
/// line for one slower than `ADB_SLOW_QUERY_MS`. Only the statement's fingerprint is
/// logged, never its literals.
pub(crate) fn observe_query(aql: &str, took: std::time::Duration, r: &Result<Value, String>) {
    use adb_core::metrics;
    let (statement, memory) = metrics::statement_of(aql);
    let m = metrics::get();
    let ok = r.is_ok();
    m.queries.with_label_values(&[statement, memory, if ok { "ok" } else { "error" }]).inc();
    m.query_seconds.with_label_values(&[statement, memory]).observe(took.as_secs_f64());
    let rows = r.as_ref().ok().and_then(|v| v.pointer("/data/count").or_else(|| v.get("count"))).and_then(|c| c.as_u64());
    if let Some(n) = rows {
        m.query_rows.with_label_values(&[statement]).observe(n as f64);
    }
    let ms = took.as_secs_f64() * 1000.0;
    if took >= metrics::slow_query() {
        m.slow_queries.with_label_values(&[statement]).inc();
        tracing::info!(target: "adb::query", statement, memory, ms, rows, ok, query = %metrics::fingerprint(aql), "slow query");
    } else {
        tracing::debug!(target: "adb::query", statement, memory, ms, rows, ok, query = %metrics::fingerprint(aql), "query");
    }
}

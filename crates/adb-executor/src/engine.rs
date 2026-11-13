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

use adb_core::log::Epoch;
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
    /// DB-9: a branch of main at a past point.
    async fn branch_create_past(&self, _namespace: &str, _name: &str, _created_by: Option<&str>, _at: &crate::branches::PointRef) -> Result<Value, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
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
    /// DB-14: the gateway whose log holds a space's writes: the org's, or a hosted branch's.
    async fn space_gateway(&self, _namespace: &str, _branch: Option<&str>) -> Result<Arc<Gateway>, String> {
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

    /// B6: rows of `namespace` in `memory` as this engine (or its hosted branch
    /// `branch`) saw them at `at` — what a branch in its own pod reads its parent
    /// through, and what an org reads a pod's view through.
    async fn parent_rows(&self, _memory: MemoryType, _namespace: &str, _branch: Option<&str>, _at: i64) -> Result<Vec<adb_store::StoredRow>, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    /// B6: the branch runs in its own pod at `url` (Flight) from now on.
    async fn branch_place(&self, _namespace: &str, _name: &str, _url: &str) -> Result<crate::branches::Branch, String> {
        Err("memory branches need a durable engine (start ADB with --data-dir)".into())
    }
    /// B6: FORGETs a branch's pod hasn't applied; `ack` drops the first `n` of them.
    async fn branch_pending(&self, _namespace: &str, _name: &str) -> Result<Vec<crate::branches::PendingForget>, String> {
        Ok(Vec::new())
    }
    async fn branch_pending_ack(&self, _namespace: &str, _name: &str, _n: usize) -> Result<(), String> {
        Ok(())
    }
    /// B6, on a branch engine: erase this engine's own rows of `lineages`.
    async fn forget_lineages(&self, _memory: MemoryType, _lineages: &[String]) -> Result<u64, String> {
        Err("not a branch engine".into())
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

    /// FlowR's engine store, space `space` (stage H); `None` without a data dir.
    fn flowr_engine(&self, _space: &str) -> Option<Arc<dyn flowr_storage::engine::EngineStore>> {
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
    let Some(c) = settings else { return (adb_embed::Embedder::shared(), adb_store::DEFAULT_EMBEDDING_DIM, None) };
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
    /// The org engine's branch engines (B2, hosted placement); `None` on a branch engine.
    branch_host: Option<Arc<crate::branch_engine::BranchHost>>,
    /// A branch engine's parent (B2); `None` on the org engine.
    parent: Option<crate::branch_engine::ParentView>,
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
fn watch_statement(aql: &str) -> Option<aql_parser::ast::Statement> {
    let first = aql.trim_start().split(|c: char| c.is_whitespace()).next()?;
    if !first.eq_ignore_ascii_case("WATCH") && !first.eq_ignore_ascii_case("UNWATCH") {
        return None;
    }
    aql_parser::parse(aql).ok()
}

/// Open the partition log in `dir/p0`. A log this process just closed can still be
/// held for a moment by a background task finishing its step (the maintenance
/// timer is aborted, but stops only when the runtime next polls it): retry for up
/// to 10 s, yielding to the runtime between tries.
async fn open_wal(dir: &std::path::Path) -> Result<LocalWal, String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match LocalWal::open(SINGLE_PARTITION, WalConfig::new(dir.join("p0"))) {
            Err(adb_log::LogError::Locked(_)) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            other => return other.map_err(|e| e.to_string()),
        }
    }
}

/// The partition's Lance table in `dir/store`, sized by the embedding settings
/// (W9.10), and the embedder they name.
/// What a follower keeps open and applied (a [`crate::Replica`]), handed to the
/// engine when the node becomes the leader.
pub struct Warm {
    pub(crate) store: Arc<PartitionStore>,
    pub(crate) embedder: Option<Arc<adb_embed::Embedder>>,
    pub(crate) platform: Arc<adb_platform::PlatformStore>,
    pub(crate) engine: adb_engine::EngineHost,
    /// The first log record the follower's engine host hasn't seen.
    pub(crate) engine_next: adb_core::log::LogOffset,
}

pub(crate) async fn open_store(dir: &std::path::Path) -> Result<(Arc<PartitionStore>, Option<Arc<adb_embed::Embedder>>), String> {
    let settings = adb_embed::EmbedConfig::load(dir)?;
    let (embedder, embedding_dim, embedding_model) = embedder_for(settings.as_ref());
    // The replica set's other nodes may embed on this node's model (C-17b).
    adb_embed::peers::serve_with(embedder.clone());
    // S3 option B (`.ai/S3_OPTION_B_PLAN.md`): object storage behind the table, from
    // `ADB_COLD_URI` (e.g. s3://adb-1/prod/cluster-a; credentials from AWS_*).
    let cold = std::env::var("ADB_COLD_URI").ok().filter(|v| !v.trim().is_empty());
    let store_cfg = StoreConfig { embedding_dim, embedding_model, cold, ..StoreConfig::new(dir.join("store")) };
    let store = Arc::new(PartitionStore::open(&store_cfg, SINGLE_PARTITION).await.map_err(|e| e.to_string())?);
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

/// The maintenance timer: every tick, `expire_due`, and the embedding backfill
/// while it finds work (then once a minute), on `writes` while it exists. Holds
/// only a weak reference, so it never keeps the log (and its directory lock) open.
fn spawn_expiry<L: adb_log::Log + 'static>(writes: std::sync::Weak<DurableWrites<L>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
            if night || (ticks % check == 0 && w.compaction_due().await) {
                if night {
                    last_nightly = Some(now.date_naive());
                }
                if let Err(e) = w.compact().await {
                    tracing::warn!(error = %e, "compaction failed; retrying later");
                }
            }
            // S3 option B: archive to object storage every `ADB_ARCHIVE_EVERY_SECS` (default 30).
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
        let wal: EngineLog = Arc::new(open_wal(&dir).await?);
        Self::durable_on(dir, wal).await
    }

    /// The local WAL in `dir/p0`, opened (a cluster node's state machine holds it).
    pub async fn open_local_wal(dir: &std::path::Path) -> Result<Arc<LocalWal>, String> {
        Ok(Arc::new(open_wal(dir).await?))
    }

    /// A durable engine over `wal`: the local WAL, or (cluster, M3) the Raft log,
    /// whose appends commit through the replica set and whose reads are the
    /// node's local WAL. Everything else is as [`Self::durable`].
    pub async fn durable_on(dir: impl Into<std::path::PathBuf>, wal: EngineLog) -> Result<(Self, ReplayStats), String> {
        // Boxed: keeps the callers' future types shallow (the compiler's query depth).
        Box::pin(Self::durable_with(dir.into(), wal, None, None)).await
    }

    /// As [`Self::durable_on`], taking over a follower's already-open table,
    /// platform tables and engine spaces (CA-34: a new leader does not reopen
    /// what its replica kept applied).
    pub async fn durable_warm(dir: impl Into<std::path::PathBuf>, wal: EngineLog, warm: Warm) -> Result<(Self, ReplayStats), String> {
        Box::pin(Self::durable_with(dir.into(), wal, Some(warm), None)).await
    }

    /// A branch engine (B2): its own WAL, sequencer and delta table in `dir`, reading
    /// its parent through `parent` (the org, or another branch).
    pub async fn durable_branch(dir: impl Into<std::path::PathBuf>, parent: crate::branch_engine::ParentView) -> Result<(Self, ReplayStats), String> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| format!("branch data dir {}: {e}", dir.display()))?;
        let wal: EngineLog = Arc::new(open_wal(&dir).await?);
        Box::pin(Self::durable_with(dir, wal, None, Some(parent))).await
    }

    /// The last record in this engine's log (offset, commit time).
    pub async fn last_commit(&self) -> Result<(u64, adb_core::log::CommitTs), String> {
        let d = self.durable.as_ref().ok_or("no durable log")?;
        d.last_commit().await.map_err(|e| e.to_string())
    }

    /// This engine as a parent of a branch: the org's table, or a branch's overlay.
    pub fn as_parent_source(&self) -> Option<Arc<dyn crate::branch_engine::ParentSource>> {
        let (store, barrier) = self.store.as_ref()?;
        Some(match &self.parent {
            None => Arc::new(crate::branch_engine::StoreSource { store: Arc::clone(store), barrier: barrier.clone() }),
            Some(p) => Arc::new(crate::branch_engine::BranchSource { store: Arc::clone(store), barrier: barrier.clone(), parent: p.clone() }),
        })
    }

    /// B5: what `source` sees of `namespace` now, in the memories a new agent takes
    /// (SEMANTIC, PROCEDURAL, TOOLS — EPISODIC stays with the original agent, WORKING
    /// is session state), written into `into`.
    /// DB-10: base (main at the branch point), main now and the branch's view, per
    /// memory type, and their diff. Branches of main only.
    async fn three_way(&self, namespace: &str, name: &str, memories: &[MemoryType]) -> Result<ThreeWay, String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        let host = self.branch_host.as_ref().ok_or("branches are merged on the org's engine")?;
        let branch = d.branches().get(namespace, name).ok_or_else(|| format!("no branch {name:?} in namespace {namespace:?}"))?;
        if branch.parent != crate::branches::MAIN {
            return Err(format!("branch {name:?} is a branch of {:?}: merging into a parent branch isn't supported yet", branch.parent));
        }
        let parent = self.as_parent_source().ok_or("a branch engine has no table")?;
        let view_src = host.source(namespace, name).await?;
        let main_point = d.point().await;
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let mut w = ThreeWay { behind: 0, branch: branch.clone(), main_point, clean: Vec::new(), conflicts: Vec::new(), main: Default::default(), view: Default::default() };
        for m in memories {
            let base = parent.rows_as_of(*m, namespace, branch.point()).await.map_err(|e| e.to_string())?;
            let main = parent.rows_as_of(*m, namespace, now).await.map_err(|e| e.to_string())?;
            let view = view_src.rows_as_of(*m, namespace, now).await.map_err(|e| e.to_string())?;
            let base_ids: std::collections::HashMap<&str, &str> = base.iter().map(|r| (r.lineage_id.as_str(), r.id.as_str())).collect();
            w.behind += main.iter().filter(|r| base_ids.get(r.lineage_id.as_str()) != Some(&r.id.as_str())).count();
            let (c, k) = crate::merge::diff(*m, &base, &main, &view);
            w.clean.extend(c);
            w.conflicts.extend(k);
            w.main.extend(main.into_iter().map(|r| (r.lineage_id.clone(), r)));
            w.view.extend(view.into_iter().map(|r| (r.lineage_id.clone(), r)));
        }
        Ok(w)
    }

    async fn materialise_from(&self, source: Arc<dyn crate::branch_engine::ParentSource>, namespace: &str, into: &str) -> Result<u64, String> {
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        self.materialise_at(source, namespace, into, now).await
    }

    /// DB-17: copy only PROCEDURAL (and TOOLS' schemas, with `tools`) of `namespace` into `into`:
    /// nothing a client said or was learned about them travels.
    async fn materialise_procedures(&self, namespace: &str, into: &str, tools: bool) -> Result<u64, String> {
        let d = self.durable.as_ref().ok_or("copying needs a durable engine (start ADB with --data-dir)")?;
        if namespace == into {
            return Err(format!("promote into a new namespace, not {into:?} itself"));
        }
        let source = self.as_parent_source().ok_or("copying needs a durable engine (start ADB with --data-dir)")?;
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let mut rows = source.rows_as_of(MemoryType::Procedural, namespace, now).await.map_err(|e| e.to_string())?;
        if tools {
            rows.extend(source.rows_as_of(MemoryType::Tools, namespace, now).await.map_err(|e| e.to_string())?);
        }
        d.materialise(into, rows).await.map_err(|e| e.to_string())
    }

    /// DB-14: a space's rows as main or as a hosted or placed branch sees them.
    async fn space_source(&self, namespace: &str, branch: Option<&str>) -> Result<Arc<dyn crate::branch_engine::ParentSource>, String> {
        match branch.filter(|b| !b.is_empty() && *b != crate::branches::MAIN) {
            Some(b) => {
                let host = self.branch_host.as_ref().ok_or("branches are read on the org's engine")?;
                host.source(namespace, b).await
            }
            None => self.as_parent_source().ok_or_else(|| "space reads need a durable engine (start ADB with --data-dir)".to_string()),
        }
    }

    async fn materialise_at(&self, source: Arc<dyn crate::branch_engine::ParentSource>, namespace: &str, into: &str, at: adb_core::log::CommitTs) -> Result<u64, String> {
        let d = self.durable.as_ref().ok_or("promoting needs a durable engine (start ADB with --data-dir)")?;
        if namespace == into {
            return Err(format!("promote into a new namespace, not {into:?} itself"));
        }
        let mut rows = Vec::new();
        for m in [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools] {
            rows.extend(source.rows_as_of(m, namespace, at).await.map_err(|e| e.to_string())?);
        }
        d.materialise(into, rows).await.map_err(|e| e.to_string())
    }

    /// B6: how this org reaches branches in their own pods (the server's Flight client).
    pub fn set_branch_remote(&self, remote: Arc<dyn crate::branch_engine::BranchRemote>) {
        if let Some(host) = &self.branch_host {
            host.set_remote(remote);
        }
    }

    /// The durable writer (B4: the branch host sets pins and forwards FORGETs).
    pub(crate) fn durable_writes(&self) -> Option<&Arc<DurableWrites<EngineLog>>> {
        self.durable.as_ref()
    }

    async fn durable_with(dir: std::path::PathBuf, wal: EngineLog, warm: Option<Warm>, parent: Option<crate::branch_engine::ParentView>) -> Result<(Self, ReplayStats), String> {
        // Shared: the sequencer appends, subscribers read (stage E).
        let seq = Sequencer::new(Arc::clone(&wal), Epoch(1));
        let warm_cache = warm.is_some();
        let (store, embedder, platform_open, engine_open) = match warm {
            Some(w) => {
                // A follower compaction under way stops before it writes (PF-38).
                w.store.yield_compaction(true);
                (w.store, w.embedder, Some(w.platform), Some((w.engine, Some(w.engine_next))))
            }
            None => {
                let (store, embedder) = open_store(&dir).await?;
                // The field indexes it had, in the background (PF-40).
                store.warm(false);
                (store, embedder, None, None)
            }
        };
        let opened = std::time::Instant::now();

        // W10: the hot cache follows every apply from here on (the catch-up too).
        // A follower kept one from the same settings (C-19): the takeover keeps it.
        if !(warm_cache && store.hot().is_some()) {
            store.set_cache(hot_cache(cache_config(&dir)?)).await.map_err(|e| e.to_string())?;
        }
        let (mut writes, stats) =
            DurableWrites::open(seq, Arc::clone(&store)).await.map_err(|e| e.to_string())?;
        let platform = match platform_open {
            Some(p) => p,
            None => Arc::new(adb_platform::PlatformStore::open(dir.join("platform")).await.map_err(|e| e.to_string())?),
        };
        writes.attach_platform(Arc::clone(&platform)).await.map_err(|e| e.to_string())?;
        // FlowR's engine store (stage H): its spaces under `<dir>/engine`.
        let (engine, engine_next) = match engine_open {
            Some(e) => e,
            None => (adb_engine::EngineHost::open(dir.join("engine")).await.map_err(|e| e.to_string())?, None),
        };
        let t_engine = std::time::Instant::now();
        let replayed = writes.attach_engine(engine, engine_next).await.map_err(|e| e.to_string())?;
        tracing::info!(write_path = ?(t_engine - opened), engine_spaces = ?t_engine.elapsed(), "engine open steps");
        store.yield_compaction(false);
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
        writes.set_parent(parent.clone());
        let branch_host = match &parent {
            None => Some(Arc::new(crate::branch_engine::BranchHost::new(
                dir.join("branches"),
                Arc::new(crate::branch_engine::StoreSource { store: Arc::clone(&store), barrier: barrier.clone() }),
                Arc::clone(writes.branches()),
            ))),
            Some(_) => None,
        };
        if let Some(host) = &branch_host {
            writes.set_branch_host(Arc::clone(host));
            let registry = Arc::clone(writes.branches());
            writes.set_pins(Arc::new(move || registry.pins(None)));
        }
        let reads: Arc<dyn Reads> = Arc::new(LanceReads::new(Arc::clone(&store), barrier.clone()).with_embedder(embedder.clone()).with_parent(parent.clone()).with_layers(parent.is_none().then(|| Arc::clone(writes.branches()))));
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
                platform: Some((platform, writes)),
                expiry,
                durable: Some(durable_handle),
                watcher,
                stats_cache: Default::default(),
                temp: None,
                dir,
                embedder,
                branch_host,
                parent,
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

    /// `WATCH` (register, snapshot) and `UNWATCH` (W6).
    async fn watch(&self, stmt: aql_parser::ast::Statement, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        use aql_parser::ast::{StartPosition, Statement};
        let started = std::time::Instant::now();
        let (Some((watcher, _)), Some(durable)) = (&self.watcher, &self.durable) else {
            return Err("WATCH / UNWATCH need a durable engine (start ADB with --data-dir)".into());
        };
        let value = match stmt {
            Statement::Watch(w) => {
                if let StartPosition::Offset(n) = w.start {
                    return Err(format!(
                        "WATCH … FROM OFFSET {n}: a new watch starts now; resume an existing one by its id (GET /watches/{{id}}/events?from=, MCP watch_changes)"
                    ));
                }
                let spec = adb_cq::WatchSpec::new(*w.query, variables.cloned().unwrap_or_default(), w.debounce)?;
                // Refuse a query that fails before anything is committed.
                self.executor.execute_statement(&spec.statement, &spec.variables).await.map_err(|e| e.to_string())?;
                let (id, _) = durable.register_watch(serde_json::to_value(&spec).map_err(|e| e.to_string())?).await.map_err(|e| e.to_string())?;
                match watcher.add(&id, spec).await {
                    Ok(frame) => serde_json::to_value(&frame).map_err(|e| e.to_string())?,
                    Err(e) => {
                        let _ = durable.unregister_watch(&id).await;
                        return Err(e.to_string());
                    }
                }
            }
            Statement::Unwatch(u) => {
                let existed = watcher.contains(&u.query_id);
                if existed {
                    durable.unregister_watch(&u.query_id).await.map_err(|e| e.to_string())?;
                    watcher.remove(&u.query_id);
                }
                json!({ "cq_id": u.query_id, "unwatched": existed })
            }
            _ => unreachable!("watch_statement returns WATCH / UNWATCH only"),
        };
        Ok(to_wire(&QueryResult::subscription(value, started.elapsed().as_millis() as u64)))
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

    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        // OB-3: every AQL statement (HTTP, Flight, MCP) is timed here, once: a branch
        // engine's statements came through its parent, which times them.
        if self.parent.is_some() {
            return self.query_unobserved(aql, variables).await;
        }
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
        let branches = match &self.branch_host {
            Some(host) => host.compact_hosted().await?,
            None => 0,
        };
        match compacted {
            Some(s) => Ok(json!({
                "branches": branches,
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

    async fn branch_create(&self, namespace: &str, name: &str, parent: Option<&str>, created_by: Option<&str>) -> Result<crate::branches::Branch, String> {
        if self.parent.is_some() {
            return Err("branches are created on the org's engine".into());
        }
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        // A branch of a branch is cut at the parent branch's own log position.
        if let (Some(p), Some(host)) = (parent.filter(|p| !p.is_empty() && *p != crate::branches::MAIN), &self.branch_host) {
            if host.placement(namespace, p).is_some() {
                return Err(format!("branch {p:?} runs in its own pod: branching from it isn't supported yet (B6)"));
            }
        }
        let point = match (parent.filter(|p| !p.is_empty() && *p != crate::branches::MAIN), &self.branch_host) {
            (Some(p), Some(host)) if d.branches().get(namespace, p).is_some() => Some(host.engine(namespace, p).await?.last_commit().await?),
            _ => None,
        };
        d.branch_create_at(namespace, name, parent, created_by, point).await.map_err(|e| e.to_string())
    }

    async fn branch_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        let b = d.branches().get(namespace, name);
        let dropped = d.branch_drop(namespace, name).await.map_err(|e| e.to_string())?;
        // B4: its engine goes with it — WAL, table, history.
        if let (true, Some(b), Some(host)) = (dropped, b, &self.branch_host) {
            host.remove(&b).await?;
        }
        Ok(dropped)
    }

    async fn branch_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Branch>, String> {
        Ok(self.durable.as_ref().map(|d| d.branches().list(namespace)).unwrap_or_default())
    }

    async fn tag_create(&self, namespace: &str, name: &str, created_by: Option<&str>) -> Result<crate::branches::Tag, String> {
        let d = self.durable.as_ref().filter(|_| self.parent.is_none()).ok_or("tags are created on the org's engine")?;
        d.tag_create(namespace, name, created_by).await.map_err(|e| e.to_string())
    }

    async fn tag_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        let d = self.durable.as_ref().filter(|_| self.parent.is_none()).ok_or("tags are dropped on the org's engine")?;
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

    async fn parent_rows(&self, memory: MemoryType, namespace: &str, branch: Option<&str>, at: i64) -> Result<Vec<adb_store::StoredRow>, String> {
        let source = match (branch.filter(|b| *b != crate::branches::MAIN), &self.branch_host) {
            (Some(b), Some(host)) => host.source(namespace, b).await?,
            // A pod that is one branch, asked for that branch by name.
            (Some(b), None) if self.parent.as_ref().is_some_and(|p| p.branch == b && p.namespace == namespace) => {
                self.as_parent_source().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?
            }
            (Some(b), None) => return Err(format!("no branch {b:?} here: this engine is a branch itself")),
            (None, _) => {
                if let Some(p) = self.parent.as_ref().filter(|p| p.namespace != namespace) {
                    return Err(format!("this is a branch of {:?}, not {namespace:?}", p.namespace));
                }
                self.as_parent_source().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?
            }
        };
        source.rows_as_of(memory, namespace, adb_core::log::CommitTs(at)).await.map_err(|e| e.to_string())
    }

    async fn branch_place(&self, namespace: &str, name: &str, url: &str) -> Result<crate::branches::Branch, String> {
        let (d, host) = match (&self.durable, &self.branch_host) {
            (Some(d), Some(h)) => (d, h),
            _ => return Err("branches are placed on the org's engine".into()),
        };
        if url.is_empty() {
            return Err("branch_place needs the pod's Flight URL".into());
        }
        let b = d.branches().get(namespace, name).ok_or_else(|| format!("no branch {name:?} in namespace {namespace:?}"))?;
        if b.placement.is_none() && host.hosted_exists(&b) {
            return Err(format!("branch {name:?} already has a hosted engine with its own writes: moving one to a pod isn't supported yet (B6)"));
        }
        if d.branches().has_children(namespace, name) {
            return Err(format!("branch {name:?} has branches of its own: a pod branch can't be a parent yet (B6)"));
        }
        let url = url.to_string();
        let b = d.branch_change(namespace, name, |b| b.placement = Some(url)).await.map_err(|e| e.to_string())?;
        b.ok_or_else(|| format!("no branch {name:?} in namespace {namespace:?}"))
    }

    async fn branch_pending(&self, namespace: &str, name: &str) -> Result<Vec<crate::branches::PendingForget>, String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        Ok(d.branches().get(namespace, name).map(|b| b.pending_forgets).unwrap_or_default())
    }

    async fn branch_pending_ack(&self, namespace: &str, name: &str, n: usize) -> Result<(), String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        if n == 0 {
            return Ok(());
        }
        d.branch_change(namespace, name, |b| {
            let k = n.min(b.pending_forgets.len());
            b.pending_forgets.drain(..k);
        })
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn forget_lineages(&self, memory: MemoryType, lineages: &[String]) -> Result<u64, String> {
        if self.parent.is_none() {
            return Err("forget_lineages runs on a branch engine (FORGET on main reaching it)".into());
        }
        let d = self.durable.as_ref().ok_or("a branch engine is durable")?;
        d.forget_lineages(memory, lineages).await.map_err(|e| e.to_string())
    }

    async fn branch_promote(&self, namespace: &str, name: &str, into: &str, allow_stale: bool) -> Result<Value, String> {
        let host = self.branch_host.as_ref().ok_or("branches are promoted on the org's engine")?;
        let d = self.durable.as_ref().ok_or("promoting needs a durable engine (start ADB with --data-dir)")?;
        let branch = d.branches().get(namespace, name).ok_or_else(|| format!("no branch {name:?} in namespace {namespace:?}"))?;
        // DB-11: a copy of a branch that is behind main would drop main's newer memory.
        if !allow_stale && branch.parent == crate::branches::MAIN {
            let w = self.three_way(namespace, name, &[MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools]).await?;
            if !w.conflicts.is_empty() {
                return Err(format!("merge_conflicts: {} records changed on both main and the branch: {}: rebase first", w.conflicts.len(), w.conflicts.iter().map(|c| c.lineage.as_str()).collect::<Vec<_>>().join(", ")));
            }
            if w.behind > 0 {
                return Err(format!("merge_stale: main changed {} records since the branch point: rebase first (allow_stale promotes without them)", w.behind));
            }
        }
        let source = host.source(namespace, name).await?;
        let n = self.materialise_from(source, namespace, into).await?;
        // D1: the new agent carries the old one's layers, reads its episodes as of now,
        // read-only, and its first record says where it came from.
        let old_layers = d.branches().layers_of(namespace);
        let safe: String = into.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '-' }).take(55).collect();
        let tag = format!("promoted-{safe}");
        d.tag_create(namespace, &tag, None).await.map_err(|e| e.to_string())?;
        let mut layers = vec![crate::branches::Layer { space: namespace.to_string(), mode: crate::branches::LayerMode::History, tag: Some(tag), memories: vec![MemoryType::Episodic], sealed: false }];
        layers.extend(old_layers);
        d.layers_set(into, layers).await.map_err(|e| e.to_string())?;
        let s = |v: &str| serde_json::to_string(v).unwrap_or_else(|_| "\"\"".into());
        let aql = format!(
            "STORE INTO EPISODIC (event = \"promoted_from\", from_namespace = {}, from_branch = {}, point_offset = \"{}\", point_ts = \"{}\") NAMESPACE {}",
            s(namespace), s(name), branch.point_offset, branch.point_ts, s(into)
        );
        self.query(&aql, None).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": name } }))
    }

    async fn branch_rebase(&self, namespace: &str, name: &str, main_offset: u64, resolutions: Vec<crate::merge::Resolution>) -> Result<Value, String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        let host = self.branch_host.as_ref().ok_or("branches are rebased on the org's engine")?;
        if host.placement(namespace, name).is_some() {
            return Err(format!("branch {name:?} runs in its own pod: rebasing one isn't supported yet (B6)"));
        }
        let w = self.three_way(namespace, name, &[MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools]).await?;
        let res: std::collections::HashMap<&str, &crate::merge::Resolution> = resolutions.iter().map(|r| (r.lineage.as_str(), r)).collect();
        let open: Vec<&str> = w.conflicts.iter().filter(|c| !res.contains_key(c.lineage.as_str())).map(|c| c.lineage.as_str()).collect();
        if !open.is_empty() {
            return Err(format!("merge_conflicts: {} conflicts have no resolution: {}", open.len(), open.join(", ")));
        }
        let engine = host.engine(namespace, name).await?;
        let bd = engine.durable_writes().ok_or("a branch engine is durable")?;
        let (mut kept, mut dropped, mut edited) = (0, 0, 0);
        for c in &w.conflicts {
            if let Some(m) = w.main.get(&c.lineage) {
                if m.log_offset > main_offset {
                    return Err(format!("merge_stale: main changed {} at offset {} after {main_offset}: read the conflicts again", c.lineage, m.log_offset));
                }
            }
        }
        for c in &w.conflicts {
            let r = res[c.lineage.as_str()];
            match r.take.as_str() {
                "branch" => kept += 1,
                "main" => {
                    bd.forget_lineages(c.memory, &[adb_store::key_of(&c.lineage).to_string()]).await.map_err(|e| e.to_string())?;
                    dropped += 1;
                }
                "edit" => {
                    let value = r.value.clone().ok_or_else(|| format!("resolution for {}: an edit needs a value", c.lineage))?;
                    let template = w.main.get(&c.lineage).or_else(|| w.view.get(&c.lineage)).ok_or_else(|| format!("resolution for {}: no row to edit", c.lineage))?;
                    bd.branch_set_row(c.memory, &c.lineage, value, template).await.map_err(|e| e.to_string())?;
                    edited += 1;
                }
                other => return Err(format!("resolution for {}: take is branch, main or edit, not {other:?}", c.lineage)),
            }
        }
        // The new point: what the diff read as main. The live engine moves with it.
        let (off, ts) = w.main_point;
        d.branch_change(namespace, name, |b| {
            b.point_offset = off;
            b.point_ts = ts.0;
        })
        .await
        .map_err(|e| e.to_string())?;
        if let Some(p) = engine.parent.as_ref() {
            p.point.set(ts);
        }
        Ok(json!({ "point": { "offset": off, "ts": ts.0 }, "kept": kept, "dropped": dropped, "edited": edited }))
    }

    async fn space_clone(&self, namespace: &str, into: &str) -> Result<Value, String> {
        if self.parent.is_some() {
            return Err("spaces are cloned on the org's engine".into());
        }
        let source = self.as_parent_source().ok_or("cloning a memory space needs a durable engine (start ADB with --data-dir)")?;
        let n = self.materialise_from(source, namespace, into).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN } }))
    }

    async fn branch_create_past(&self, namespace: &str, name: &str, created_by: Option<&str>, at: &crate::branches::PointRef) -> Result<Value, String> {
        if self.parent.is_some() {
            return Err("branches are created on the org's engine".into());
        }
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        let b = d.branch_create_past(namespace, name, created_by, at).await.map_err(|e| e.to_string())?;
        Ok(serde_json::to_value(&b).unwrap_or_default())
    }

    async fn space_copy_procedures(&self, namespace: &str, into: &str, tools: bool) -> Result<Value, String> {
        if self.parent.is_some() {
            return Err("spaces are copied on the org's engine".into());
        }
        let n = self.materialise_procedures(namespace, into, tools).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN }, "procedures_only": true }))
    }

    async fn space_clone_at(&self, namespace: &str, into: &str, at: &crate::branches::PointRef) -> Result<Value, String> {
        if self.parent.is_some() {
            return Err("spaces are cloned on the org's engine".into());
        }
        let d = self.durable.as_ref().ok_or("cloning a memory space needs a durable engine (start ADB with --data-dir)")?;
        let (offset, ts) = d.resolve_point(namespace, at).await.map_err(|e| e.to_string())?;
        let source = self.as_parent_source().ok_or("cloning a memory space needs a durable engine (start ADB with --data-dir)")?;
        let n = self.materialise_at(source, namespace, into, ts).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN, "point": { "offset": offset, "ts": ts.0 } } }))
    }

    async fn quota_set(&self, pattern: &str, max_bytes: Option<u64>, max_records: Option<u64>, history_days: Option<u64>) -> Result<crate::branches::Quota, String> {
        let d = self.durable.as_ref().filter(|_| self.parent.is_none()).ok_or("quotas are set on the org's engine")?;
        d.quota_set(pattern, max_bytes, max_records, history_days).await.map_err(|e| e.to_string())
    }

    async fn quota_drop(&self, pattern: &str) -> Result<bool, String> {
        let d = self.durable.as_ref().filter(|_| self.parent.is_none()).ok_or("quotas are set on the org's engine")?;
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
        let source = self.space_source(namespace, branch).await?;
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let mut tiers = serde_json::Map::new();
        for m in [MemoryType::Working, MemoryType::Tools, MemoryType::Procedural, MemoryType::Semantic, MemoryType::Episodic] {
            let rows = source.rows_as_of(m, namespace, now).await.map_err(|e| e.to_string())?;
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
        let source = self.space_source(namespace, branch).await?;
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let mut rows = source.rows_as_of(memory, namespace, now).await.map_err(|e| e.to_string())?;
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

    async fn space_gateway(&self, namespace: &str, branch: Option<&str>) -> Result<Arc<Gateway>, String> {
        match branch.filter(|b| !b.is_empty() && *b != crate::branches::MAIN) {
            Some(b) => {
                let host = self.branch_host.as_ref().ok_or("branches are read on the org's engine")?;
                if host.placement(namespace, b).is_some() {
                    return Err(format!("branch {b:?} runs in its own pod: follow its feed there (B6)"));
                }
                host.engine(namespace, b).await?.gateway().ok_or_else(|| "a branch engine has no feed".to_string())
            }
            None => self.gateway().ok_or_else(|| "the space feed needs a durable engine (start ADB with --data-dir)".to_string()),
        }
    }

    async fn branch_conflicts(&self, namespace: &str, name: &str, memories: &[MemoryType]) -> Result<Value, String> {
        let all = [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools];
        let w = self.three_way(namespace, name, if memories.is_empty() { &all } else { memories }).await?;
        Ok(json!({
            "base": { "offset": w.branch.point_offset, "ts": w.branch.point_ts },
            "main": { "offset": w.main_point.0, "ts": w.main_point.1 .0 },
            "clean": w.clean,
            "conflicts": w.conflicts,
        }))
    }

    async fn branch_merge(&self, namespace: &str, name: &str, memories: &[MemoryType], main_offset: u64, resolutions: Vec<crate::merge::Resolution>, by: Option<&str>) -> Result<Value, String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        let w = self.three_way(namespace, name, if memories.is_empty() { &[MemoryType::Procedural] } else { memories }).await?;
        let res: std::collections::HashMap<&str, &crate::merge::Resolution> = resolutions.iter().map(|r| (r.lineage.as_str(), r)).collect();
        let open: Vec<&str> = w.conflicts.iter().filter(|c| !res.contains_key(c.lineage.as_str())).map(|c| c.lineage.as_str()).collect();
        if !open.is_empty() {
            return Err(format!("merge_conflicts: {} conflicts have no resolution: {}", open.len(), open.join(", ")));
        }
        // Each settled change: (memory, lineage, the branch's value or None for a forget).
        let mut settled: Vec<(MemoryType, String, Option<Value>)> = Vec::new();
        for c in &w.clean {
            let v = if c.change == "forget" { None } else { Some(c.branch.clone()) };
            settled.push((c.memory, c.lineage.clone(), v));
        }
        for c in &w.conflicts {
            let v = if c.branch.is_null() { None } else { Some(c.branch.clone()) };
            settled.push((c.memory, c.lineage.clone(), v));
        }
        let mut ops = Vec::new();
        for (memory, lineage, branch_value) in settled {
            let value = match res.get(lineage.as_str()) {
                Some(r) if r.take == "main" => continue,
                Some(r) if r.take == "edit" => Some(r.value.clone().ok_or_else(|| format!("resolution for {lineage}: an edit needs a value"))?),
                Some(r) if r.take != "branch" => return Err(format!("resolution for {lineage}: take is branch, main or edit, not {:?}", r.take)),
                _ => branch_value,
            };
            let main = w.main.get(&lineage).cloned();
            if let Some(m) = &main {
                if m.log_offset > main_offset {
                    return Err(format!("merge_stale: main changed {lineage} at offset {} after {main_offset}: read the conflicts again", m.log_offset));
                }
            }
            if main.is_none() && value.is_none() {
                continue;
            }
            let template = w.view.get(&lineage).cloned();
            ops.push(crate::writes::MergeOp { memory, lineage, main, value, template, prev_version: None });
        }
        let (changed, added, ended) = ops.iter().fold((0, 0, 0), |(c, a, e), op| match (&op.main, &op.value) {
            (Some(_), Some(_)) => (c + 1, a, e),
            (None, Some(_)) => (c, a + 1, e),
            _ => (c, a, e + 1),
        });
        let merge_id = format!("{name}-{}", adb_core::time::now().timestamp_micros());
        let mut row = serde_json::Map::new();
        for (k, v) in [("id", merge_id.as_str()), ("kind", "merge"), ("namespace", namespace), ("branch", name), ("by", by.unwrap_or(""))] {
            row.insert(k.into(), json!(v));
        }
        let (offset, _) = d.merge_rows(ops, row).await.map_err(|e| e.to_string())?;
        Ok(json!({ "merge_id": merge_id, "offset": offset, "changed": changed, "added": added, "ended": ended }))
    }

    async fn merge_revert(&self, namespace: &str, merge_id: &str, by: Option<&str>, force_skip: bool) -> Result<Value, String> {
        let d = self.durable.as_ref().ok_or("memory branches need a durable engine (start ADB with --data-dir)")?;
        let platform = self.platform.as_ref().map(|(p, _)| Arc::clone(p)).ok_or("platform tables need a durable engine (start ADB with --data-dir)")?;
        let q = |s: &str| s.replace('\'', "''");
        let rows = match platform.query(crate::writes::MERGES, Some(&format!("namespace = '{}' AND (id = '{}' OR of = '{}')", q(namespace), q(merge_id), q(merge_id))), 100).await {
            Ok(b) => adb_platform::batches_to_json(&b),
            Err(_) => Vec::new(),
        };
        let merge = rows.iter().find(|r| r["kind"] == "merge" && r["id"] == merge_id).ok_or_else(|| format!("merge_not_found: no merge {merge_id:?} in namespace {namespace:?}"))?;
        if rows.iter().any(|r| r["kind"] == "revert" && r["of"] == merge_id) {
            return Err(format!("merge_not_found: merge {merge_id:?} was already reverted"));
        }
        let entries: Vec<Value> = serde_json::from_str(merge["entries"].as_str().unwrap_or("[]")).map_err(|e| format!("merge {merge_id}: entries: {e}"))?;
        let source = self.as_parent_source().ok_or("a branch engine has no table")?;
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let mut current: std::collections::HashMap<(MemoryType, String), adb_store::StoredRow> = Default::default();
        let mut ops = Vec::new();
        let mut moved = Vec::new();
        for e in &entries {
            let memory: MemoryType = serde_json::from_value(e["memory"].clone()).map_err(|x| x.to_string())?;
            let lineage = e["lineage"].as_str().unwrap_or_default().to_string();
            if !current.keys().any(|(m, _)| *m == memory) {
                for r in source.rows_as_of(memory, namespace, now).await.map_err(|x| x.to_string())? {
                    current.insert((memory, r.lineage_id.clone()), r);
                }
            }
            let cur = current.get(&(memory, lineage.clone())).cloned();
            let after = e["after"].as_str();
            // Stored ids carry the memory prefix; the merge recorded the bare id.
            if cur.as_ref().map(|r| adb_store::key_of(&r.id)) != after.map(adb_store::key_of) {
                tracing::debug!(lineage, now = ?cur.as_ref().map(|r| &r.id), merged = ?after, "revert: changed since the merge");
                moved.push(lineage);
                continue;
            }
            let before: Option<adb_store::StoredRow> = serde_json::from_value(e["before"].clone()).ok();
            let template: Option<adb_store::StoredRow> = serde_json::from_value(e["template"].clone()).ok();
            ops.push(match before {
                // Was an add: end it.
                None => crate::writes::MergeOp { memory, lineage, main: cur, value: None, template: None, prev_version: None },
                // Was a change: write the old value back as a new version.
                Some(b) if cur.is_some() => crate::writes::MergeOp { memory, lineage, main: cur, value: Some(b.data.value.clone()), template: None, prev_version: None },
                // Was an end: bring the lineage back.
                Some(b) => {
                    let v = b.version;
                    crate::writes::MergeOp { memory, lineage, main: None, value: Some(b.data.value.clone()), template: Some(b).or(template), prev_version: Some(v) }
                }
            });
        }
        if !moved.is_empty() && !force_skip {
            return Err(format!("revert_conflicts: {} records changed since the merge: {} (force_skip reverts the rest)", moved.len(), moved.join(", ")));
        }
        let mut row = serde_json::Map::new();
        let id = format!("{merge_id}-revert");
        for (k, v) in [("id", id.as_str()), ("kind", "revert"), ("namespace", namespace), ("branch", merge["branch"].as_str().unwrap_or("")), ("by", by.unwrap_or("")), ("of", merge_id)] {
            row.insert(k.into(), json!(v));
        }
        let n = ops.len();
        let (offset, _) = d.merge_rows(ops, row).await.map_err(|e| e.to_string())?;
        Ok(json!({ "reverted": n, "skipped": moved, "offset": offset }))
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

/// DB-10's inputs and diff (see `LocalEngine::three_way`).
struct ThreeWay {
    /// Main's records added or changed since the branch point.
    behind: usize,
    branch: crate::branches::Branch,
    main_point: (u64, adb_core::log::CommitTs),
    clean: Vec<crate::merge::Change>,
    conflicts: Vec<crate::merge::Conflict>,
    main: std::collections::HashMap<String, adb_store::StoredRow>,
    view: std::collections::HashMap<String, adb_store::StoredRow>,
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

    #[tokio::test]
    async fn wire_format_matches_flowr_adapter() {
        let engine = LocalEngine::temporary().await.unwrap();
        let stored = engine
            .query(r#"STORE INTO SEMANTIC (key = "k1", content = "hello") SCOPE private NAMESPACE "ns1""#, None)
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
        assert!(engine.query(r#"UNWATCH "no-such-watch""#, None).await.is_ok(), "unknown watch: unwatched = false");
        assert!(engine.query("RECALL FROM SEMANTIC LIKE $q", None).await.is_err(), "LIKE without a value");
    }
}

impl LocalEngine {
    async fn query_unobserved(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        // B2: a statement on a branch runs on the branch's own engine.
        match crate::branch_engine::statement_branch(aql)? {
            Some((b, ns)) => match (&self.branch_host, &self.parent) {
                (Some(host), _) => {
                    let ns = ns.ok_or_else(|| format!("BRANCH \"{b}\" needs NAMESPACE: a branch belongs to one memory space"))?;
                    // B6: a branch in its own pod runs the statement there.
                    if let Some(url) = host.placement(&ns, &b) {
                        let vars = variables.map(|v| serde_json::to_value(v).unwrap_or_default());
                        return host.query_remote(&url, aql, vars).await;
                    }
                    return host.engine(&ns, &b).await?.query(aql, variables).await;
                }
                (None, Some(p)) => {
                    if b != p.branch || ns.as_deref().is_some_and(|n| n != p.namespace) {
                        return Err(format!("this is branch {:?} of {:?}, not {b:?}", p.branch, p.namespace));
                    }
                }
                (None, None) => return Err("memory branches need a durable engine".into()),
            },
            None => {}
        }
        if let Some(stmt) = watch_statement(aql) {
            return self.watch(stmt, variables).await;
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
    match adb_core::metrics::statement_of(aql).0 {
        "RECALL" | "SCAN" => !aql.split_whitespace().any(|w| w.eq_ignore_ascii_case("KEY")),
        _ => false,
    }
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

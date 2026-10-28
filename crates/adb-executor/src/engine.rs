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

    /// A platform-table write (`Table*` effect), committed through the log and
    /// applied before it returns. Returns its log offset.
    async fn platform_write(&self, _effect: adb_core::log::Effect) -> Result<u64, String> {
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

/// The hot cache with `cfg`, unless
/// `ADB_CACHE=off`.
fn hot_cache(cfg: adb_store::CacheConfig) -> Option<adb_store::HotCache> {
    match cache_mode().as_str() {
        "off" => None,
        mode => Some(adb_store::HotCache::new(cfg, mode == "shadow")),
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
    durable: Option<Arc<DurableWrites<Arc<LocalWal>>>>,
    /// Continuous queries (W6) and the task keeping them current.
    watcher: Option<(Arc<adb_cq::Watcher>, tokio::task::JoinHandle<()>)>,
    /// The data directory (W9.10: settings live there).
    dir: std::path::PathBuf,
    /// The embedder in use (W9.10: reported with the settings).
    embedder: Option<Arc<adb_embed::Embedder>>,
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
    durable: Arc<DurableWrites<Arc<LocalWal>>>,
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
const BACKFILL_BATCH: usize = 64;
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
            if night || (ticks % 60 == 0 && w.compaction_due().await) {
                if night {
                    last_nightly = Some(now.date_naive());
                }
                if let Err(e) = w.compact().await {
                    tracing::warn!(error = %e, "compaction failed; retrying later");
                }
            }
            // W5-L4: the platform tables, nightly and on their version count.
            if night || (ticks % 60 == 30 && w.platform_compaction_due().await) {
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
        // Shared: the sequencer appends, subscribers read (stage E).
        let wal = Arc::new(open_wal(&dir).await?);
        let seq = Sequencer::new(Arc::clone(&wal), Epoch(1));
        // W9.10: the embedding settings size the table's vectors.
        let settings = adb_embed::EmbedConfig::load(&dir)?;
        let (embedder, embedding_dim, embedding_model) = embedder_for(settings.as_ref());
        let store_cfg = StoreConfig { embedding_dim, embedding_model, ..StoreConfig::new(dir.join("store")) };
        let store = Arc::new(PartitionStore::open(&store_cfg, SINGLE_PARTITION).await.map_err(|e| e.to_string())?);
        // W10: the hot cache follows every apply from here on (the catch-up too).
        let cache_cfg = match adb_store::CacheConfig::load(&dir)? {
            Some(c) => c,
            None => env_cache_rules()?.unwrap_or_default(),
        };
        store.set_cache(hot_cache(cache_cfg)).await.map_err(|e| e.to_string())?;
        let (mut writes, stats) =
            DurableWrites::open(seq, Arc::clone(&store)).await.map_err(|e| e.to_string())?;
        let platform = Arc::new(adb_platform::PlatformStore::open(dir.join("platform")).await.map_err(|e| e.to_string())?);
        writes.attach_platform(Arc::clone(&platform)).await.map_err(|e| e.to_string())?;
        let barrier = writes.barrier();
        let watch_log: Arc<dyn adb_log::Log> = Arc::clone(&wal) as Arc<dyn adb_log::Log>;
        let gateway = barrier.as_ref().map(|b| {
            let log: Arc<dyn adb_log::Log> = wal;
            let lineages: Arc<dyn adb_gateway::Lineages> = Arc::clone(&store) as Arc<dyn adb_gateway::Lineages>;
            Arc::new(Gateway::new(log, lineages, Arc::clone(writes.subscriptions()), b.watch()))
        });
        writes.set_embedder(embedder.clone());
        let reads: Arc<dyn Reads> = Arc::new(LanceReads::new(Arc::clone(&store), barrier.clone()).with_embedder(embedder.clone()));
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
                temp: None,
                dir,
                embedder,
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
    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        if let Some(stmt) = watch_statement(aql) {
            return self.watch(stmt, variables).await;
        }
        let started = std::time::Instant::now();
        let result = match variables {
            Some(vars) if !vars.is_empty() => self.executor.execute_with_vars(aql, vars).await,
            _ => self.executor.execute(aql).await,
        };
        let executed = started.elapsed();
        let wire = result.map(|r| to_wire(&r)).map_err(|e| e.to_string());
        tracing::debug!(?executed, total = ?started.elapsed(), "query");
        wire
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
        match w.compact().await.map_err(|e| e.to_string())? {
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

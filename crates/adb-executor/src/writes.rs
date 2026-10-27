//! Where the executor's writes go (DELIVERY_PLAN.md stage C).
//!
//! - [`DirectWrites`] — straight to the in-memory backends (no log): tests and the
//!   in-memory server.
//! - [`DurableWrites`] — log first: each write is resolved into effects
//!   (`adb_core::log::Effect`) at its commit timestamp, appended to the partition log
//!   through the sequencer, and applied only after the log made it durable. On open,
//!   [`replay`] rebuilds the backends from the log.
//!
//! Applying always runs under `adb_core::time::at(commit_ts)`, so a write applied
//! live and the same write replayed after a restart produce the same records (same
//! ids, `created_at`, TTL start, expiry checks; D2_TEMPORAL.md T2).
//!
//! `UPDATE` on SEMANTIC / PROCEDURAL supersedes (D2 §2, stage D4.3): each matched
//! record gets a new version row (new id, same lineage, `version + 1`, fields
//! merged, TTL kept), and the old one is closed. Other memory types, and engines
//! without a Lance table, patch in place.
//!
//! Limits: one partition (`PartitionId(0)`); `FORGET FROM ALL` / `UPDATE ALL` commit
//! one transaction per memory type.

use std::sync::Arc;
use std::time::Duration;

use adb_backends::Adb;
use adb_core::log::{
    CommitTs, Effect, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData,
    Visibility,
};
use adb_core::{time, AdbError, AdbResult, Link, MemoryRecord, MemoryType, Predicate, Scope};
use adb_log::{Log, LogError};
use adb_sequencer::Sequencer;
use adb_store::PartitionStore;

use crate::apply::{Applier, ApplyBarrier};
use crate::reads::check_scope;
use adb_gateway::{sub_value, Registry, SubSpec};
use async_trait::async_trait;
use tokio::sync::Mutex;

/// The partition every stage C write goes to (placement arrives with M3).
pub const SINGLE_PARTITION: PartitionId = PartitionId(0);
/// Tenant and named scope for stage C rows (D3 S6 default).
const TENANT: &str = "default";
const DEFAULT_SCOPE: &str = "tenant:default";

/// The executor's write operations.
#[async_trait]
pub trait Writes: Send + Sync {
    async fn store(
        &self,
        memory_type: MemoryType,
        key: &str,
        payload: serde_json::Value,
        scope: Scope,
        namespace: Option<&str>,
        ttl: Option<Duration>,
        in_scope: Option<&str>,
    ) -> AdbResult<MemoryRecord>;

    /// `in_scope`: only records in that named scope (`IN SCOPE`); `None`: every scope.
    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value, in_scope: Option<&str>) -> AdbResult<u64>;

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>) -> AdbResult<u64>;

    async fn link(
        &self,
        from_type: MemoryType,
        from_id: &str,
        to_type: MemoryType,
        to_id: &str,
        link_type: &str,
        weight: Option<f32>,
    ) -> AdbResult<Link>;

    /// AQL v0.7 UPDATE LINK: set the weight and/or type of the links from any of
    /// `from_ids` to any of `to_ids` (of `link_type`, if given). Returns how many.
    #[allow(clippy::too_many_arguments)]
    async fn update_links(
        &self,
        from_type: MemoryType,
        from_ids: &[String],
        to_type: MemoryType,
        to_ids: &[String],
        link_type: Option<&str>,
        weight: Option<f32>,
        new_type: Option<&str>,
    ) -> AdbResult<u64>;

    /// AQL v0.7 UNLINK: remove those links; the records stay. Returns how many.
    async fn unlink(
        &self,
        from_type: MemoryType,
        from_ids: &[String],
        to_type: MemoryType,
        to_ids: &[String],
        link_type: Option<&str>,
    ) -> AdbResult<u64>;

    /// `PUBLISH TOPIC`: a committed `event` row (stage E). Returns its id and offset.
    async fn publish(&self, _topic: &str, _payload: serde_json::Value, _ttl: Option<Duration>) -> AdbResult<(String, u64)> {
        Err(needs_log())
    }

    /// `SUBSCRIBE`: register a cursor from offset `from` (`None` = from now). Returns
    /// the subscription id and the first offset it will be delivered.
    async fn subscribe(&self, _spec: SubSpec, _from: Option<u64>) -> AdbResult<(String, u64)> {
        Err(needs_log())
    }

    /// `ACK`: move the subscription's cursor to `offset`. Returns the cursor.
    async fn ack(&self, _subscription: &str, _offset: u64) -> AdbResult<u64> {
        Err(needs_log())
    }

    /// `UNSUBSCRIBE`. Returns whether the subscription existed.
    async fn unsubscribe(&self, _subscription: &str) -> AdbResult<bool> {
        Err(needs_log())
    }

    /// A platform-table write (stage G): one `Table*` effect, committed and applied
    /// before it returns. Returns its log offset.
    async fn platform(&self, _effect: Effect) -> AdbResult<u64> {
        Err(AdbError::UnsupportedOperation("platform tables need a durable engine (start ADB with --data-dir)".into()))
    }
}

/// Subscriptions follow the committed log (E9).
/// Most lineages one expiry tick commits (one log record).
const EXPIRE_BATCH: usize = 1000;

/// Memory types whose keys are unique among current records (D3 S4): the
/// long-term, versioned ones. WORKING (scratchpad) and TOOLS (registrations)
/// keep replace-by-key.
pub fn unique_keys(memory: MemoryType) -> bool {
    matches!(memory, MemoryType::Semantic | MemoryType::Episodic | MemoryType::Procedural)
}

/// Whether a STORE must check its key: a key the caller chose (`key` / `id`, or
/// PROCEDURAL's `pattern_id`). Without one the planner made a fresh ULID, which
/// can't collide — and checking means waiting for the applier and scanning Lance.
fn caller_keyed(memory: MemoryType, payload: &serde_json::Value) -> bool {
    unique_keys(memory)
        && (payload.get("key").is_some()
            || payload.get("id").is_some()
            || (memory == MemoryType::Procedural && payload.get("pattern_id").is_some()))
}

fn key_exists(memory: MemoryType, key: &str) -> AdbError {
    AdbError::KeyExists { memory_type: format!("{memory:?}").to_uppercase(), key: key.to_string() }
}

/// Whether the in-memory backends hold a current `memory` record with id `id`.
async fn backend_has(adb: &Adb, memory: MemoryType, id: &str) -> AdbResult<bool> {
    Ok(!adb.lookup(memory, &Predicate::key("id", id)).await?.is_empty())
}

fn needs_table(verb: &str) -> AdbError {
    AdbError::UnsupportedOperation(format!("{verb} (needs the Lance table: a durable engine)"))
}

/// The next version of edge `cur` with a new type and/or weight. Its ends go back
/// to the writer's ids (`qualified` adds the memory types again).
fn edge_version(cur: &adb_store::StoredRow, link_type: Option<&str>, weight: Option<f32>, id: RecordId) -> NewRow {
    let mut data = cur.data.clone();
    data.subject_id = data.subject_id.as_deref().map(|s| adb_store::key_of(s).to_string());
    data.object_id = data.object_id.as_deref().map(|o| adb_store::key_of(o).to_string());
    if let Some(t) = link_type {
        data.link_type = Some(t.to_string());
    }
    if let Some(w) = weight {
        data.weight = Some(w);
    }
    NewRow { id, lineage_id: RecordId(cur.key().to_string()), version: cur.version + 1, data }
}

fn needs_log() -> AdbError {
    AdbError::UnsupportedOperation("subscriptions need a durable engine (start ADB with --data-dir)".into())
}

/// Default topic message TTL (E8).
pub const DEFAULT_EVENT_TTL: Duration = Duration::from_secs(7 * 24 * 3600);
/// The most records the log keeps behind its tip for subscribers (E3).
pub const RETAIN_MAX: u64 = 100_000;

/// Writes straight to the backends.
pub struct DirectWrites(pub Arc<Adb>);

#[async_trait]
impl Writes for DirectWrites {
    async fn store(
        &self,
        memory_type: MemoryType,
        key: &str,
        payload: serde_json::Value,
        scope: Scope,
        namespace: Option<&str>,
        ttl: Option<Duration>,
        in_scope: Option<&str>,
    ) -> AdbResult<MemoryRecord> {
        check_scope(memory_type, in_scope, false)?;
        if caller_keyed(memory_type, &payload) {
            let id = Adb::store_id(memory_type, key, &payload);
            if backend_has(&self.0, memory_type, &id).await? {
                return Err(key_exists(memory_type, &id));
            }
        }
        self.0.store_with_options(memory_type, key, payload, scope, namespace, ttl).await
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value, in_scope: Option<&str>) -> AdbResult<u64> {
        check_scope(memory_type, in_scope, false)?;
        self.0.update(memory_type, predicate, data).await
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>) -> AdbResult<u64> {
        check_scope(memory_type, in_scope, false)?;
        self.0.forget(memory_type, predicate).await
    }

    async fn link(
        &self,
        from_type: MemoryType,
        from_id: &str,
        to_type: MemoryType,
        to_id: &str,
        link_type: &str,
        weight: Option<f32>,
    ) -> AdbResult<Link> {
        // AQL v0.7: the same link again (same ends, same type) sets its weight.
        let (from, to) = ([from_id.to_string()], [to_id.to_string()]);
        if let Some(existing) = self.0.links_between(from_type, &from, to_type, &to, Some(link_type)).await?.into_iter().next() {
            if weight.is_some() {
                self.0.update_links(from_type, &from, to_type, &to, Some(link_type), weight, None).await?;
            }
            return Ok(Link { weight: weight.unwrap_or(existing.weight), ..existing });
        }
        self.0.link(from_type, from_id, to_type, to_id, link_type, weight.unwrap_or(1.0)).await
    }

    async fn update_links(
        &self,
        from_type: MemoryType,
        from_ids: &[String],
        to_type: MemoryType,
        to_ids: &[String],
        link_type: Option<&str>,
        weight: Option<f32>,
        new_type: Option<&str>,
    ) -> AdbResult<u64> {
        self.0.update_links(from_type, from_ids, to_type, to_ids, link_type, weight, new_type).await
    }

    async fn unlink(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>) -> AdbResult<u64> {
        self.0.unlink(from_type, from_ids, to_type, to_ids, link_type).await
    }
}

fn log_err(e: LogError) -> AdbError {
    AdbError::Internal(format!("log: {e}"))
}

fn kind_of(memory: MemoryType) -> Kind {
    match memory {
        MemoryType::Episodic => Kind::Episode,
        MemoryType::Tools => Kind::Tool,
        _ => Kind::Entity,
    }
}

fn row(memory: MemoryType, kind: Kind, value: serde_json::Value) -> RowData {
    RowData {
        tenant_id: TENANT.into(),
        scope: DEFAULT_SCOPE.into(),
        visibility: Visibility::Private,
        namespace: None,
        memory: memory.into(),
        kind,
        key: value.get("key").and_then(|v| v.as_str()).map(String::from),
        subject_id: None,
        predicate: None,
        object_id: None,
        link_type: None,
        weight: None,
        text: value.get("content").and_then(|v| v.as_str()).map(String::from),
        value,
        sealed: None,
        embedding: None,
        ttl_at: None,
        provenance: Provenance::default(),
    }
}

/// Writes through the log.
pub struct DurableWrites<L: Log> {
    adb: Arc<Adb>,
    seq: Mutex<Sequencer<L>>,
    /// The partition's Lance table (stage D2: written alongside the in-memory
    /// backends; reads move to it in D3). `None` = log + memory only.
    store: Option<Arc<PartitionStore>>,
    /// Background task applying committed records to `store` (stage D3.7).
    applier: Option<Applier>,
    /// Live subscriptions (stage E), following registrations as they commit.
    subs: Arc<Registry>,
    /// Platform tables (stage G), applied synchronously.
    platform: Option<Arc<adb_platform::PlatformStore>>,
    /// The live keys of SEMANTIC / EPISODIC / PROCEDURAL (D3 S4 key check), with
    /// their TTL: built from Lance at open, kept by the writes below under the
    /// sequencer lock. Checking a key in Lance instead costs a read per STORE.
    live: std::sync::Mutex<LiveKeys>,
    /// Embeds `content` on STORE / UPDATE and in the backfill (MVP W4); `None`: no
    /// vectors (records are then found by filters and text only).
    embedder: Option<Arc<adb_embed::Embedder>>,
}

pub type LiveKeys = std::collections::HashMap<(Memory, String), Option<CommitTs>>;

impl<L: Log + 'static> DurableWrites<L> {
    /// The current edges from any of `from_ids` to any of `to_ids` (of `link_type`,
    /// if given), after every earlier record is in Lance.
    #[allow(clippy::too_many_arguments)]
    async fn edges_between(
        &self,
        store: &PartitionStore,
        from_type: MemoryType,
        from_ids: &[String],
        to_type: MemoryType,
        to_ids: &[String],
        link_type: Option<&str>,
        ts: CommitTs,
    ) -> AdbResult<Vec<adb_store::StoredRow>> {
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let targets: std::collections::HashSet<String> = to_ids.iter().map(|t| adb_store::row_id(to_type.into(), t)).collect();
        let mut out = Vec::new();
        for from in from_ids {
            let edges = store
                .edges_from(from_type.into(), from, link_type, ts)
                .await
                .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            out.extend(edges.into_iter().filter(|e| e.data.object_id.as_ref().is_some_and(|o| targets.contains(o))));
        }
        Ok(out)
    }

    pub fn set_embedder(&mut self, embedder: Option<Arc<adb_embed::Embedder>>) {
        self.embedder = embedder;
    }

    /// The f16 vector of `text` for the log, or `None` (no model, no text, or the
    /// model failed — logged; the backfill tries again).
    async fn vector_of(&self, text: Option<String>) -> Option<Vec<half::f16>> {
        let (e, t) = (self.embedder.as_ref()?, text?);
        match e.embed(vec![t]).await {
            Ok(mut v) => Some(adb_embed::to_f16(&v.remove(0))),
            Err(e) => {
                tracing::warn!(error = %e, "embedding failed; the backfill will retry");
                None
            }
        }
    }

    /// Embed current SEMANTIC / EPISODIC / PROCEDURAL rows that have text but no
    /// vector — written before there was a model, or whose embedding failed — and
    /// commit the vectors as `Embed` effects, at most `batch` rows per call (MVP W4
    /// T7). Embedding runs outside the sequencer; a row superseded or forgotten in
    /// between just gets a vector it no longer needs. Returns how many.
    pub async fn embed_backfill(&self, batch: usize) -> AdbResult<u64> {
        let (Some(store), Some(embedder)) = (&self.store, &self.embedder) else { return Ok(0) };
        let now = CommitTs::from_datetime(time::now());
        let filter = format!(
            "{} AND memory IN ('semantic', 'episodic', 'procedural') AND tombstone = false AND superseded_by IS NULL AND embedding IS NULL",
            adb_store::RECORD_KINDS
        );
        let rows = store.scan(Some(&filter)).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        let todo: Vec<(adb_store::StoredRow, String)> = rows
            .into_iter()
            .filter(|r| r.is_current(now))
            .filter_map(|r| crate::reads::embed_text(r.data.memory.into(), &r.data.value).map(|t| (r, t)))
            .take(batch)
            .collect();
        if todo.is_empty() {
            return Ok(0);
        }
        let vectors = embedder.embed(todo.iter().map(|(_, t)| t.clone()).collect()).await.map_err(AdbError::EmbeddingError)?;
        let effects: Vec<Effect> = todo
            .iter()
            .zip(vectors)
            .map(|((r, _), v)| Effect::Embed { memory: r.data.memory, id: RecordId(adb_store::key_of(&r.id).to_string()), embedding: adb_embed::to_f16(&v) })
            .collect();
        let n = effects.len() as u64;
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok(n)
    }

    fn key_taken(&self, memory: Memory, key: &str, now: CommitTs) -> bool {
        let live = self.live.lock().unwrap();
        live.get(&(memory, key.to_string())).is_some_and(|ttl| ttl.is_none_or(|t| t > now))
    }

    fn keys_gone(&self, effects: &[Effect]) {
        let mut live = self.live.lock().unwrap();
        for e in effects {
            if let Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } = e {
                live.remove(&(*memory, lineage_id.0.clone()));
            }
        }
    }
    /// Replay `log` into `adb`, bring `store` up to the log's end, then accept new
    /// writes on top of both.
    pub async fn open(adb: Arc<Adb>, mut seq: Sequencer<L>, store: Option<Arc<PartitionStore>>) -> AdbResult<(Self, ReplayStats)> {
        let mut live = LiveKeys::new();
        let stats = match &store {
            // Stage D3: no full log replay. Lance catches up from the log's tail, then
            // the in-memory tier is rebuilt from Lance's current rows.
            Some(s) => {
                let t = std::time::Instant::now();
                let store_caught_up = catch_up(s, seq.log()).await?;
                let caught_up_in = t.elapsed();
                let (hydrated_rows, keys) = hydrate(&adb, s).await?;
                live = keys;
                tracing::info!(store_caught_up, ?caught_up_in, hydrated_rows, live_keys = live.len(), hydrated_in = ?(t.elapsed() - caught_up_in), "store opened");
                let last_offset = seq.log().last_offset();
                let last_commit_ts = match last_offset.0 {
                    0 => None,
                    _ => seq.log().read(last_offset, 1).await.map_err(log_err)?.first().map(|r| r.commit_ts),
                };
                ReplayStats { records: last_offset.0, effects: 0, last_offset, last_commit_ts, store_caught_up, hydrated_rows }
            }
            None => replay(&adb, seq.log()).await?,
        };
        if let Some(last) = stats.last_commit_ts {
            seq.resume_after(last);
        }
        // Caught up with the log: the applier starts from its end.
        let applier = store
            .as_ref()
            .map(|s| Applier::spawn(Arc::clone(s), ApplyBarrier::new(seq.log().last_offset().0)));
        let subs = match &store {
            Some(s) => Registry::load(s, CommitTs::from_datetime(time::now())).await.map_err(AdbError::Internal)?,
            None => Registry::default(),
        };
        Ok((Self { adb, seq: Mutex::new(seq), store, applier, subs: Arc::new(subs), platform: None, live: std::sync::Mutex::new(live), embedder: None }, stats))
    }

    /// The last durable offset (tests and metrics).
    pub async fn durable_offset(&self) -> LogOffset {
        self.seq.lock().await.log().durable_offset()
    }

    /// The Lance table, if any (tests and D3 reads).
    pub fn store(&self) -> Option<&Arc<PartitionStore>> {
        self.store.as_ref()
    }

    /// The table, when `memory`'s records live only there (stage D4.4a).
    fn in_lance(&self, memory: MemoryType) -> Option<&Arc<PartitionStore>> {
        self.store.as_ref().filter(|_| crate::reads::lance_owned(memory))
    }

    /// The records an UPDATE (`forget = false`) or FORGET matches, as lineage ids:
    /// from Lance for the types it owns, with the backends' own matching code over
    /// its rows; FORGET on SEMANTIC / PROCEDURAL also matches superseded versions, so
    /// a value surviving only in an old version is forgotten too (D2 T5).
    async fn targets(&self, memory: MemoryType, predicate: &Predicate, ts: CommitTs, forget: bool, in_scope: Option<&str>) -> AdbResult<Vec<String>> {
        let at = ts.to_datetime();
        check_scope(memory, in_scope, self.in_lance(memory).is_some())?;
        let Some(store) = self.in_lance(memory) else {
            return match forget {
                true => time::at(at, self.adb.forget_targets(memory, predicate)).await,
                false => time::at(at, self.adb.update_targets(memory, predicate)).await,
            };
        };
        // Every earlier record is in Lance first.
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let store_err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
        let view = crate::reads::current_view(store, memory, predicate, ts, in_scope).await?;
        let mut ids = match forget {
            true => time::at(at, view.forget_targets(predicate)).await?,
            false => time::at(at, view.update_targets(predicate)).await?,
        };
        if forget && supersedes(memory) {
            let mut old = store.superseded(memory.into()).await.map_err(store_err)?;
            if let Some(s) = in_scope {
                old.retain(|r| r.data.scope == s);
            }
            if memory == MemoryType::Procedural {
                // Procedural ids come from the payload: one view per version.
                for r in &old {
                    let v = crate::reads::View::of(memory, vec![r.clone()], false).await?;
                    if !time::at(at, v.forget_targets(predicate)).await?.is_empty() {
                        ids.push(r.key().to_string());
                    }
                }
            } else {
                let lineage: std::collections::HashMap<String, String> =
                    old.iter().map(|r| (r.id.clone(), r.key().to_string())).collect();
                let v = crate::reads::View::of(memory, old, true).await?;
                for row_id in time::at(at, v.forget_targets(predicate)).await? {
                    if let Some(key) = lineage.get(&row_id) {
                        ids.push(key.clone());
                    }
                }
            }
            let mut seen = std::collections::HashSet::new();
            ids.retain(|id| seen.insert(id.clone()));
        }
        Ok(ids)
    }

    /// Commit `Expire` for the records whose TTL has passed (D2 §4, L3): one log
    /// record per tick, so every subscriber sees those expiries at one offset.
    /// Reads hide expired records already; this makes expiry an event and marks the
    /// rows for compaction. Returns how many lineages expired.
    pub async fn expire_due(&self) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Ok(0) };
        let mut seq = self.seq.lock().await;
        // Every earlier record is in Lance first.
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let ts = seq.begin();
        let due = store
            .due_expiries(ts, EXPIRE_BATCH)
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        if due.is_empty() {
            return Ok(0);
        }
        let effects: Vec<Effect> =
            due.into_iter().map(|(memory, key)| Effect::Expire { memory, lineage_id: RecordId(key) }).collect();
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        // WORKING / TOOLS live in memory too: drop them there as replay would.
        for effect in &effects {
            if let Effect::Expire { memory, .. } = effect {
                if self.in_lance((*memory).into()).is_none() {
                    apply_count(&self.adb, ts, effect).await?;
                }
            }
        }
        self.to_store(&record, seq.log()).await;
        Ok(effects.len() as u64)
    }

    /// Live subscriptions (stage E).
    pub fn subscriptions(&self) -> &Arc<Registry> {
        &self.subs
    }

    /// Serve platform tables from `platform` (stage G): first apply every log record
    /// above its applied offset (a crash may have cut one short). Returns how many.
    pub async fn attach_platform(&mut self, platform: Arc<adb_platform::PlatformStore>) -> AdbResult<u64> {
        let seq = self.seq.lock().await;
        let mut from = LogOffset(platform.applied_offset().await + 1);
        let mut applied = 0;
        loop {
            let batch = match seq.log().read(from, 1024).await {
                Ok(b) => b,
                // Purged below what the platform store has: nothing to catch up.
                Err(LogError::Purged { oldest, .. }) if oldest.0 <= from.0 => break,
                Err(e) => return Err(log_err(e)),
            };
            let Some(last) = batch.last().map(|r| r.offset) else { break };
            for r in &batch {
                if platform.apply(r).await.map_err(|e| AdbError::Internal(format!("platform: {e}")))? {
                    applied += 1;
                }
            }
            from = last.next();
        }
        drop(seq);
        self.platform = Some(platform);
        Ok(applied)
    }

    /// The read barrier for Lance reads (stage D3.7), if there is a table.
    pub fn barrier(&self) -> Option<Arc<ApplyBarrier>> {
        self.applier.as_ref().map(|a| Arc::clone(a.barrier()))
    }

    /// Hand a committed record to the Lance applier. Called with the sequencer held,
    /// so records are queued in log order; the background task applies them in that
    /// order (stage D3.7). A failed apply is logged there: the write is durable in
    /// the log, and `catch_up` re-applies it on the next open.
    ///
    /// Every `PURGE_EVERY` records it also trims the log (stage D3.5): startup only
    /// needs the records above what Lance has made durable, so segments below
    /// `durable_offset + 1` go. A failed purge is logged; the log just keeps them.
    async fn to_store(&self, record: &LogRecord, log: &L) {
        let (Some(s), Some(applier)) = (&self.store, &self.applier) else { return };
        applier.enqueue(record.clone());
        if record.offset.0 % PURGE_EVERY == 0 {
            match s.durable_offset().await {
                // Lance doesn't fsync on local disk (risk K8): sync first, so the
                // records purged from the log are on disk in Lance, not only cached.
                Ok(d) if d.0 > 0 => {
                    if let Err(e) = s.sync_to_disk().await {
                        tracing::warn!(error = %e, "store sync failed; log not purged");
                        return;
                    }
                    // Keep what subscribers haven't acked, up to RETAIN_MAX behind the
                    // tip (E3); never above Lance's durable offset.
                    let mut floor = d.next();
                    // Platform tables apply synchronously; after a crash they catch up
                    // from the log, so it keeps what they haven't applied.
                    if let Some(p) = &self.platform {
                        floor = LogOffset(floor.0.min(p.applied_offset().await + 1));
                    }
                    if let Some(cursor) = self.subs.min_cursor() {
                        let cap = record.offset.0.saturating_sub(RETAIN_MAX);
                        floor = LogOffset(floor.0.min(cursor.max(cap) + 1));
                    }
                    if floor.0 <= 1 {
                        return;
                    }
                    if let Err(e) = log.purge_below(floor).await {
                        tracing::warn!(below = floor.0, error = %e, "log purge failed; segments kept");
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "Lance durable offset unavailable; log not purged"),
            }
        }
    }
}

/// How often (in log records) the log is trimmed below Lance's durable offset.
pub const PURGE_EVERY: u64 = 256;

/// Apply every log record above the table's `_meta` offset (records lost from the
/// MemWAL by a crash, or written while Lance applies failed). Returns how many.
pub async fn catch_up<L: Log>(store: &PartitionStore, log: &L) -> AdbResult<u64> {
    let store_err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
    let mut from = store.applied_offset().await.map_err(store_err)?.next();
    let mut applied = 0;
    loop {
        let batch = log.read(from, 1024).await.map_err(log_err)?;
        let Some(last) = batch.last().map(|r| r.offset) else { break };
        for record in &batch {
            if store.apply(record).await.map_err(store_err)? {
                applied += 1;
            }
        }
        from = last.next();
    }
    Ok(applied)
}

#[async_trait]
impl<L: Log + 'static> Writes for DurableWrites<L> {
    async fn store(
        &self,
        memory_type: MemoryType,
        key: &str,
        payload: serde_json::Value,
        scope: Scope,
        namespace: Option<&str>,
        ttl: Option<Duration>,
        in_scope: Option<&str>,
    ) -> AdbResult<MemoryRecord> {
        check_scope(memory_type, in_scope, self.in_lance(memory_type).is_some())?;
        // Embedded before the sequencer, so the model doesn't serialise writes (T4).
        let vector = match crate::reads::lance_owned(memory_type) && self.store.is_some() {
            true => self.vector_of(crate::reads::embed_text(memory_type, &payload)).await,
            false => None,
        };
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let id = RecordId(Adb::store_id(memory_type, key, &payload));
        // Checked under the sequencer lock, so two STOREs of one key can't both pass.
        if caller_keyed(memory_type, &payload) {
            let taken = match self.in_lance(memory_type) {
                Some(_) => self.key_taken(memory_type.into(), &id.0, ts),
                None => time::at(ts.to_datetime(), backend_has(&self.adb, memory_type, &id.0)).await?,
            };
            if taken {
                return Err(key_exists(memory_type, &id.0));
            }
        }
        let mut data = row(memory_type, kind_of(memory_type), payload);
        data.visibility = scope.into();
        data.namespace = namespace.map(String::from);
        if let Some(s) = in_scope {
            data.scope = s.to_string();
        }
        data.embedding = vector;
        data.ttl_at = ttl.map(|d| CommitTs(ts.0 + d.as_micros() as i64));
        let new = NewRow { id: id.clone(), lineage_id: id, version: 1, data };
        let effect = match memory_type {
            MemoryType::Episodic => Effect::PutEpisode(new.clone()),
            MemoryType::Tools => Effect::PutTool(new.clone()),
            _ => Effect::PutEntity(new.clone()),
        };
        let record = seq.append_record(ts, None, vec![effect.clone()]).await.map_err(log_err)?;
        if unique_keys(memory_type) && self.store.is_some() {
            self.live.lock().unwrap().insert((memory_type.into(), new.lineage_id.0.clone()), new.data.ttl_at);
        }
        let stored = match self.in_lance(memory_type) {
            // Lance is the only copy (D4.4a): answer with the record as the
            // memory type's backend shapes it, without keeping it in memory.
            Some(_) => shaped_record(memory_type, &new, ts).await?,
            None => apply(&self.adb, ts, &effect).await?.ok_or_else(|| AdbError::Internal("store produced no record".into()))?,
        };
        self.to_store(&record, seq.log()).await;
        Ok(stored)
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value, in_scope: Option<&str>) -> AdbResult<u64> {
        // New `content` gets a new vector (T5), embedded before the sequencer.
        let vector = match crate::reads::lance_owned(memory_type) && self.store.is_some() {
            true => self.vector_of(data.get("content").and_then(|c| c.as_str()).map(String::from)).await,
            false => None,
        };
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let ids = self.targets(memory_type, predicate, ts, false, in_scope).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let fields = match &data {
            serde_json::Value::Object(map) => map.clone(),
            other => {
                let mut m = serde_json::Map::new();
                m.insert("value".into(), other.clone());
                m
            }
        };
        if let (Some(store), true) = (&self.store, supersedes(memory_type)) {
            // `targets` waited for the applier, so the current versions read here
            // are the ones this update follows.
            let mut effects = Vec::with_capacity(ids.len());
            for key in &ids {
                let cur = store
                    .current_version(memory_type.into(), key, ts)
                    .await
                    .map_err(|e| AdbError::Internal(format!("store: {e}")))?
                    .ok_or_else(|| AdbError::Internal(format!("{memory_type:?} {key}: no current version in Lance")))?;
                let mut new = next_version(&cur, &fields, seq.new_id(ts));
                if vector.is_some() {
                    new.data.embedding = vector.clone();
                }
                effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new });
            }
            let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
            self.to_store(&record, seq.log()).await;
            return Ok(effects.len() as u64);
        }
        let mut effects: Vec<Effect> = ids
            .iter()
            .map(|id| Effect::Patch { memory: memory_type.into(), id: RecordId(id.clone()), fields: fields.clone(), version: 0 })
            .collect();
        // EPISODIC patches in place: its new vector rides along in the same record.
        if let Some(v) = &vector {
            effects.extend(ids.iter().map(|id| Effect::Embed { memory: memory_type.into(), id: RecordId(id.clone()), embedding: v.clone() }));
        }
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        // Apply exactly what was logged, so live and replayed updates agree.
        let n = match self.in_lance(memory_type) {
            Some(_) => ids.len() as u64,
            None => time::at(ts.to_datetime(), self.adb.update_ids(memory_type, &ids, serde_json::Value::Object(fields))).await?,
        };
        self.to_store(&record, seq.log()).await;
        Ok(n)
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>) -> AdbResult<u64> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let ids = self.targets(memory_type, predicate, ts, true, in_scope).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let effects: Vec<Effect> =
            ids.iter().map(|id| Effect::Forget { memory: memory_type.into(), lineage_id: RecordId(id.clone()) }).collect();
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        let mut removed = 0;
        if self.in_lance(memory_type).is_some() {
            removed = ids.len() as u64;
        } else {
            for effect in &effects {
                removed += apply_count(&self.adb, ts, effect).await?;
            }
        }
        self.to_store(&record, seq.log()).await;
        Ok(removed)
    }

    async fn link(
        &self,
        from_type: MemoryType,
        from_id: &str,
        to_type: MemoryType,
        to_id: &str,
        link_type: &str,
        weight: Option<f32>,
    ) -> AdbResult<Link> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        // AQL v0.7: the same link again (same ends, same type) sets its weight —
        // a new version of that edge, not a parallel one.
        if let Some(store) = &self.store {
            let existing = self
                .edges_between(store, from_type, &[from_id.to_string()], to_type, &[to_id.to_string()], Some(link_type), ts)
                .await?;
            if let Some(cur) = existing.into_iter().next() {
                if weight.is_none() {
                    // Nothing to change.
                    return crate::reads::to_link(&cur).ok_or_else(|| AdbError::Internal("edge without to_memory".into()));
                }
                let new = edge_version(&cur, None, weight, seq.new_id(ts));
                let effect = Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new: new.clone() };
                let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
                self.to_store(&record, seq.log()).await;
                return crate::reads::to_link(&adb_store::StoredRow::from_new(&adb_store::qualified(&new), ts, 0, 0))
                    .ok_or_else(|| AdbError::Internal("edge without to_memory".into()));
            }
        }
        let id = seq.new_id(ts);
        let to: Memory = to_type.into();
        let mut data = row(from_type, Kind::Edge, serde_json::json!({ "to_memory": to }));
        data.key = None;
        data.text = None;
        data.subject_id = Some(from_id.into());
        data.predicate = Some("link".into());
        data.object_id = Some(to_id.into());
        data.link_type = Some(link_type.into());
        data.weight = Some(weight.unwrap_or(1.0));
        let effect = Effect::PutEdge(NewRow { id: id.clone(), lineage_id: id, version: 1, data });
        let record = seq.append_record(ts, None, vec![effect.clone()]).await.map_err(log_err)?;
        // With a table, links are read from its edge rows only (D4.1, D4.4a).
        let link = match (&self.store, &effect) {
            (Some(_), Effect::PutEdge(n)) => crate::reads::to_link(&adb_store::StoredRow::from_new(&adb_store::qualified(n), ts, 0, 0))
                .ok_or_else(|| AdbError::Internal("edge without to_memory".into()))?,
            _ => apply_link(&self.adb, ts, &effect).await?,
        };
        self.to_store(&record, seq.log()).await;
        Ok(link)
    }

    async fn update_links(
        &self,
        from_type: MemoryType,
        from_ids: &[String],
        to_type: MemoryType,
        to_ids: &[String],
        link_type: Option<&str>,
        weight: Option<f32>,
        new_type: Option<&str>,
    ) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Err(needs_table("UPDATE LINK")) };
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let edges = self.edges_between(store, from_type, from_ids, to_type, to_ids, link_type, ts).await?;
        if edges.is_empty() {
            return Ok(0);
        }
        let effects: Vec<Effect> = edges
            .iter()
            .map(|cur| Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new: edge_version(cur, new_type, weight, seq.new_id(ts)) })
            .collect();
        let n = effects.len() as u64;
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok(n)
    }

    async fn unlink(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Err(needs_table("UNLINK")) };
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let edges = self.edges_between(store, from_type, from_ids, to_type, to_ids, link_type, ts).await?;
        if edges.is_empty() {
            return Ok(0);
        }
        // An edge's own lineage: FORGET of it tombstones every version of the edge.
        let effects: Vec<Effect> =
            edges.iter().map(|e| Effect::Forget { memory: e.data.memory, lineage_id: RecordId(e.key().to_string()) }).collect();
        let n = effects.len() as u64;
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok(n)
    }

    async fn publish(&self, topic: &str, payload: serde_json::Value, ttl: Option<Duration>) -> AdbResult<(String, u64)> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let id = seq.new_id(ts);
        // Topic messages aren't records of any memory type (`RECORD_KINDS`); the
        // `memory` column just needs a value.
        let mut data = row(MemoryType::Episodic, Kind::Event, payload);
        data.key = None;
        data.text = None;
        data.subject_id = Some(topic.to_string());
        data.ttl_at = Some(CommitTs(ts.0 + ttl.unwrap_or(DEFAULT_EVENT_TTL).as_micros() as i64));
        let effect = Effect::PublishEvent(NewRow { id: id.clone(), lineage_id: id.clone(), version: 1, data });
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok((id.0, record.offset.0))
    }

    async fn subscribe(&self, spec: SubSpec, from: Option<u64>) -> AdbResult<(String, u64)> {
        let mut seq = self.seq.lock().await;
        let tip = seq.log().last_offset().0;
        let cursor = match from {
            None => tip,
            Some(n) => {
                let n = n.max(1);
                if n <= tip {
                    if let Err(LogError::Purged { oldest, .. }) = seq.log().read(LogOffset(n), 1).await {
                        return Err(AdbError::QueryError(format!(
                            "cursor_expired: offset {n} is no longer retained; oldest available is {}",
                            oldest.0
                        )));
                    }
                }
                n.min(tip + 1) - 1
            }
        };
        let ts = seq.begin();
        let id = seq.new_id(ts);
        let mut data = row(MemoryType::Working, Kind::Sub, sub_value(&spec, cursor));
        data.key = None;
        data.text = None;
        data.subject_id = Some(id.0.clone());
        let effect = Effect::Register(NewRow { id: id.clone(), lineage_id: id.clone(), version: 1, data });
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.subs.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok((id.0, cursor + 1))
    }

    async fn ack(&self, subscription: &str, offset: u64) -> AdbResult<u64> {
        let mut seq = self.seq.lock().await;
        let sub = self.subs.get(subscription).ok_or_else(|| unknown_sub(subscription))?;
        let offset = offset.min(seq.log().last_offset().0);
        if offset <= sub.cursor {
            return Ok(sub.cursor);
        }
        let ts = seq.begin();
        let effect = Effect::UpdateSubCursor { sub_id: RecordId(subscription.to_string()), cursor: LogOffset(offset) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.subs.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok(offset)
    }

    async fn unsubscribe(&self, subscription: &str) -> AdbResult<bool> {
        let mut seq = self.seq.lock().await;
        if self.subs.get(subscription).is_none() {
            return Ok(false);
        }
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(subscription.to_string()) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.subs.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok(true)
    }

    async fn platform(&self, effect: Effect) -> AdbResult<u64> {
        let platform = self.platform.as_ref().ok_or_else(|| AdbError::UnsupportedOperation("this engine has no platform tables".into()))?;
        let perr = |e: adb_platform::PlatformError| AdbError::QueryError(e.to_string());
        let mut seq = self.seq.lock().await;
        // A logged effect must apply: check the table and its name first.
        platform.validate(&effect).await.map_err(perr)?;
        let ts = seq.begin();
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        platform.apply(&record).await.map_err(perr)?;
        self.to_store(&record, seq.log()).await;
        Ok(record.offset.0)
    }
}

fn unknown_sub(id: &str) -> AdbError {
    AdbError::NotFound { memory_type: "subscription".into(), id: id.to_string() }
}

/// What a replay did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReplayStats {
    pub records: u64,
    pub effects: u64,
    pub last_offset: LogOffset,
    pub last_commit_ts: Option<CommitTs>,
    /// Log records the Lance table was missing and got on open.
    pub store_caught_up: u64,
    /// Rows the in-memory tier was rebuilt from (Lance's current view), on open.
    pub hydrated_rows: u64,
}

/// Rebuild the in-memory hot tier (WORKING, TOOLS) from the Lance table's current
/// rows (stage D3), so startup needs only the log's tail, not all of it. WORKING is
/// put in place as rebuilt; TOOLS re-runs its `store` at the commit time (the backend
/// shapes the payload into its own struct). Everything else stays in Lance (D4.4a).
/// Also returns the live keys of SEMANTIC / EPISODIC / PROCEDURAL (from the same
/// scan), for the D3 S4 key check.
pub async fn hydrate(adb: &Adb, store: &PartitionStore) -> AdbResult<(u64, LiveKeys)> {
    let store_err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
    let now = CommitTs::from_datetime(time::now());
    let rows = store.current(None, now).await.map_err(store_err)?;
    let live: LiveKeys = rows
        .iter()
        .filter(|r| matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool) && unique_keys(r.data.memory.into()))
        .map(|r| ((r.data.memory, r.key().to_string()), r.data.ttl_at))
        .collect();
    // Only the hot tier: SEMANTIC / EPISODIC / PROCEDURAL records and every link
    // are read from Lance (D4.4a).
    let records: Vec<_> = rows
        .into_iter()
        .filter(|r| matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool) && !crate::reads::lance_owned(r.data.memory.into()))
        .collect();
    for r in &records {
        let memory: MemoryType = r.data.memory.into();
        let rec = crate::reads::to_record(r);
        match memory {
            MemoryType::Working => adb.working().insert_record(rec),
            MemoryType::Semantic => adb.semantic().insert_record(rec),
            MemoryType::Episodic => adb.episodic().insert_record(rec),
            MemoryType::Tools | MemoryType::Procedural => {
                let stored = adb.store_with_options(
                    memory,
                    r.key(),
                    r.data.value.clone(),
                    rec.metadata.scope,
                    r.data.namespace.as_deref(),
                    rec.metadata.ttl,
                );
                time::at(r.commit_ts.to_datetime(), stored).await?;
                if memory == MemoryType::Procedural {
                    adb.procedural().set_version(r.key(), u64::from(r.version));
                }
            }
        }
    }
    Ok((records.len() as u64, live))
}

/// Rebuild `adb` from every record in `log`, in order.
pub async fn replay<L: Log>(adb: &Adb, log: &L) -> AdbResult<ReplayStats> {
    let mut stats = ReplayStats::default();
    let mut from = LogOffset(1);
    loop {
        let batch = log.read(from, 1024).await.map_err(log_err)?;
        let Some(last) = batch.last().map(|r| r.offset) else { break };
        for record in &batch {
            for effect in &record.effects {
                match effect {
                    Effect::PutEdge(_) => {
                        apply_link(adb, record.commit_ts, effect).await?;
                    }
                    _ => {
                        apply_count(adb, record.commit_ts, effect).await?;
                    }
                }
                stats.effects += 1;
            }
            stats.records += 1;
            stats.last_commit_ts = Some(record.commit_ts);
        }
        stats.last_offset = last;
        from = last.next();
    }
    Ok(stats)
}

/// The record a STORE of `new` answers with, as `memory`'s backend shapes it
/// (PROCEDURAL fills in its node's defaults), without keeping it anywhere.
async fn shaped_record(memory: MemoryType, new: &NewRow, ts: CommitTs) -> AdbResult<MemoryRecord> {
    let row = adb_store::StoredRow::from_new(&adb_store::qualified(new), ts, 0, 0);
    if memory != MemoryType::Procedural {
        // SEMANTIC / EPISODIC hold records as given: no backend needed.
        return Ok(crate::reads::to_record(&row));
    }
    let view = crate::reads::View::of(memory, vec![row], false).await?;
    time::at(ts.to_datetime(), view.lookup(&Predicate::All, &adb_core::Modifiers::default()))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| AdbError::Internal("store produced no record".into()))
}

/// Memory types whose `UPDATE` writes a new version (D2 §2).
fn supersedes(memory: MemoryType) -> bool {
    matches!(memory, MemoryType::Semantic | MemoryType::Procedural)
}

/// The version after `cur` with `fields` merged in, under the new row id `id`.
fn next_version(cur: &adb_store::StoredRow, fields: &serde_json::Map<String, serde_json::Value>, id: RecordId) -> NewRow {
    let mut data = cur.data.clone();
    match &mut data.value {
        serde_json::Value::Object(v) => v.extend(fields.clone()),
        other => *other = serde_json::Value::Object(fields.clone()),
    }
    if let Some(c) = fields.get("content").and_then(|v| v.as_str()) {
        data.text = Some(c.to_string());
    }
    // The embedded text changed: the old vector no longer describes it (the caller
    // or the backfill embeds the new one).
    if ["content", "pattern", "steps"].iter().any(|f| fields.contains_key(*f)) {
        data.embedding = None;
    }
    // `ttl_at` is carried over as is: UPDATE doesn't extend a TTL (D2 T6).
    NewRow { id, lineage_id: RecordId(cur.key().to_string()), version: cur.version + 1, data }
}

/// The in-memory tier after a supersede: the record (its lineage id) holds the new
/// version, as a restart rebuilds it from Lance (`hydrate`).
async fn apply_version(adb: &Adb, ts: CommitTs, new: &NewRow) -> AdbResult<MemoryRecord> {
    let row = adb_store::StoredRow::from_new(&adb_store::qualified(new), ts, 0, 0);
    let rec = crate::reads::to_record(&row);
    match MemoryType::from(new.data.memory) {
        MemoryType::Procedural => {
            // The backend's `store` adds a node per call: drop the old version first.
            adb.forget_ids(MemoryType::Procedural, &[row.key().to_string()]).await?;
            let stored = adb
                .store_with_options(MemoryType::Procedural, row.key(), row.data.value.clone(), rec.metadata.scope, row.data.namespace.as_deref(), rec.metadata.ttl)
                .await?;
            adb.procedural().set_version(row.key(), u64::from(row.version));
            Ok(stored)
        }
        _ => {
            adb.semantic().insert_record(rec.clone());
            Ok(rec)
        }
    }
}

/// Apply one effect at its commit time; returns the stored record for `Put*`.
async fn apply(adb: &Adb, ts: CommitTs, effect: &Effect) -> AdbResult<Option<MemoryRecord>> {
    time::at(ts.to_datetime(), async {
        match effect {
            Effect::PutEntity(r) | Effect::PutEpisode(r) | Effect::PutTool(r) | Effect::PutFact(r) => {
                let ttl = r.data.ttl_at.map(|at| Duration::from_micros((at.0 - ts.0).max(0) as u64));
                let record = adb
                    .store_with_options(
                        r.data.memory.into(),
                        &r.id.0,
                        r.data.value.clone(),
                        r.data.visibility.into(),
                        r.data.namespace.as_deref(),
                        ttl,
                    )
                    .await?;
                Ok(Some(record))
            }
            Effect::Supersede { new, .. } => apply_version(adb, ts, new).await.map(Some),
            _ => {
                apply_count_now(adb, effect).await?;
                Ok(None)
            }
        }
    })
    .await
}

async fn apply_count(adb: &Adb, ts: CommitTs, effect: &Effect) -> AdbResult<u64> {
    match effect {
        Effect::PutEntity(_) | Effect::PutEpisode(_) | Effect::PutTool(_) | Effect::PutFact(_) | Effect::Supersede { .. } => {
            apply(adb, ts, effect).await.map(|r| r.is_some() as u64)
        }
        _ => time::at(ts.to_datetime(), apply_count_now(adb, effect)).await,
    }
}

/// Non-`Put` effects; the clock is already fixed by the caller.
async fn apply_count_now(adb: &Adb, effect: &Effect) -> AdbResult<u64> {
    match effect {
        Effect::Patch { memory, id, fields, .. } => {
            adb.update_ids((*memory).into(), &[id.0.clone()], serde_json::Value::Object(fields.clone())).await
        }
        Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } => {
            adb.forget_ids((*memory).into(), &[lineage_id.0.clone()]).await
        }
        // Subscriptions and events arrive with stage E.
        _ => Ok(0),
    }
}

async fn apply_link(adb: &Adb, ts: CommitTs, effect: &Effect) -> AdbResult<Link> {
    let Effect::PutEdge(r) = effect else {
        return Err(AdbError::Internal("apply_link needs PutEdge".into()));
    };
    let to: Memory = r
        .data
        .value
        .get("to_memory")
        .cloned()
        .and_then(|v| serde_json::from_value(v).ok())
        .ok_or_else(|| AdbError::Internal("edge without to_memory".into()))?;
    let d = &r.data;
    time::at(
        ts.to_datetime(),
        adb.link_with_id(
            &r.id.0,
            d.memory.into(),
            d.subject_id.as_deref().unwrap_or_default(),
            to.into(),
            d.object_id.as_deref().unwrap_or_default(),
            d.link_type.as_deref().unwrap_or_default(),
            d.weight.unwrap_or(1.0),
        ),
    )
    .await
}

//! Where the executor's writes go (DELIVERY_PLAN.md stage C).
//!
//! [`DurableWrites`] — log first: each write is resolved into effects
//! (`adb_core::log::Effect`) at its commit timestamp, appended to the partition log
//! through the sequencer, and applied to the partition's Lance table by the
//! applier; on open the table catches up from the log's tail. The table is the only
//! copy of every memory type (W7: no in-memory tier).
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

    /// `STORE INTO SEMANTIC FACT [MULTI]` (P1_GRAPH D2): `payload` has `subject`,
    /// `predicate` and `object` (an entity key) or `value`. One current fact per
    /// (subject, predicate) — or per (subject, predicate, object) with `multi` — so
    /// a new one supersedes the old. Missing entities at its ends are created.
    #[allow(clippy::too_many_arguments)]
    async fn store_fact(
        &self,
        _payload: serde_json::Value,
        _multi: bool,
        _scope: Scope,
        _namespace: Option<&str>,
        _ttl: Option<Duration>,
        _in_scope: Option<&str>,
    ) -> AdbResult<MemoryRecord> {
        Err(AdbError::UnsupportedOperation("facts need a durable engine".into()))
    }

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

/// Drop from `r` the effects that carry a forgotten row's content (W5): its puts,
/// versions, patches and vectors, and the edges touching it. `Forget` effects and
/// everything else stay. Returns whether anything was dropped.
fn redact_record(r: &mut LogRecord, rows: &std::collections::HashSet<(Memory, String)>) -> bool {
    let hit = |m: Memory, k: &str| rows.contains(&(m, k.to_string()));
    let before = r.effects.len();
    r.effects.retain(|e| match e {
        Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) => {
            !(hit(n.data.memory, &n.id.0) || hit(n.data.memory, &n.lineage_id.0))
        }
        Effect::Supersede { old, new } => !(hit(new.data.memory, &old.0) || hit(new.data.memory, &new.id.0) || hit(new.data.memory, &new.lineage_id.0)),
        Effect::Patch { memory, id, .. } | Effect::Embed { memory, id, .. } => !hit(*memory, &id.0),
        Effect::PutEdge(n) => {
            let to = n.data.value.get("to_memory").and_then(|v| serde_json::from_value::<Memory>(v.clone()).ok());
            let from_hit = n.data.subject_id.as_deref().is_some_and(|s| hit(n.data.memory, s));
            let to_hit = matches!((to, n.data.object_id.as_deref()), (Some(t), Some(o)) if hit(t, o));
            !(from_hit || to_hit)
        }
        _ => true,
    });
    r.effects.len() != before
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
/// The most records the log keeps behind its tip for subscribers (E3), unless
/// `ADB_LOG_RETAIN` says otherwise. Past it a subscriber gets a `reset` (W9).
pub const RETAIN_MAX: u64 = 100_000;

/// A subscription nobody reads or acks for this long is dropped (W9, E-L3), unless
/// `ADB_SUB_IDLE` (seconds) says otherwise. As long as topic messages are kept.
pub const SUB_IDLE: Duration = DEFAULT_EVENT_TTL;

fn sub_idle() -> Duration {
    std::env::var("ADB_SUB_IDLE").ok().and_then(|v| v.parse().ok()).map(Duration::from_secs).unwrap_or(SUB_IDLE)
}

fn retain_max() -> u64 {
    std::env::var("ADB_LOG_RETAIN").ok().and_then(|v| v.parse().ok()).unwrap_or(RETAIN_MAX)
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
    /// W5: FORGETs waiting for physical erasure, and those a compaction has covered;
    /// concurrent FORGETs share one compaction.
    erase_requested: std::sync::atomic::AtomicU64,
    erase_done: std::sync::atomic::AtomicU64,
    compactor: Mutex<()>,
    /// Offset of the last record with platform-table effects (stage G). The log
    /// must keep it until the platform tables have applied it.
    platform_last: std::sync::atomic::AtomicU64,
    /// The most records the log keeps behind its tip for subscribers (E3):
    /// `ADB_LOG_RETAIN`, default [`RETAIN_MAX`].
    retain_max: u64,
    /// `ADB_SUB_IDLE`, default [`SUB_IDLE`].
    sub_idle: Duration,
    /// TTL timers (W9.12, E-L11): `(ttl_at, row lineage id)` of every record written
    /// with a TTL, seeded from the table at open. The expiry tick takes what is due
    /// instead of scanning the table; each is checked against the table before it
    /// expires (an UPDATE may have moved its TTL, a FORGET removed it).
    timers: std::sync::Mutex<std::collections::BTreeSet<(i64, String)>>,
    /// Validity timers (AQL v0.10, GR-14): `(valid_from or valid_to, row lineage id)`
    /// of facts whose bound is still ahead, seeded at open; the tick commits a
    /// `Validity` event for each once it passes (checked against the table first).
    bounds: std::sync::Mutex<std::collections::BTreeSet<(i64, String)>>,
    /// Lineages forgotten since the last compaction: their payloads are redacted
    /// from the log by the next one (W5).
    forgotten: std::sync::Mutex<std::collections::HashSet<(Memory, String)>>,
}

/// Records written since the last compaction that make one due (W7, user 28 Oct:
/// thousands of small puts slow every scan; at least a quarter of the table).
pub const COMPACT_AFTER_WRITES: u64 = 2_000;

/// How long superseded versions (UPDATE history) are kept (W5, user 27 Oct).
pub const HISTORY: Duration = Duration::from_secs(90 * 86_400);

pub type LiveKeys = std::collections::HashMap<(Memory, String), Option<CommitTs>>;

impl<L: Log + 'static> DurableWrites<L> {
    /// Unsubscribe every subscription idle for longer than `ADB_SUB_IDLE` (W9,
    /// E-L3): an abandoned subscriber stops pinning the log. Returns the ids.
    pub async fn drop_idle_subscriptions(&self) -> AdbResult<Vec<String>> {
        let ids = self.subs.idle(self.sub_idle);
        for id in &ids {
            Writes::unsubscribe(self, id).await?;
            tracing::info!(subscription = %id, idle_secs = self.sub_idle.as_secs(), "idle subscription dropped");
        }
        Ok(ids)
    }

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

    /// The log offset below which the platform tables need nothing: they apply under
    /// the sequencer before anything is purged, so normally no limit; only a failed
    /// apply holds the log at what they have.
    async fn platform_floor(&self) -> Option<LogOffset> {
        let p = self.platform.as_ref()?;
        let applied = p.applied_offset().await;
        (applied < self.platform_last.load(std::sync::atomic::Ordering::SeqCst)).then(|| LogOffset(applied + 1))
    }

    /// W5 compaction: with writes paused and every committed record applied,
    /// rewrite the table without what may go (forgotten, expired, unlinked, history
    /// past [`HISTORY`]), then put an empty marker record on a new log segment and
    /// purge the log below it — so forgotten payloads leave the log too. A
    /// subscriber whose cursor is below the marker gets `cursor_expired` and
    /// resubscribes (W5-L1). `None` without a table.
    pub async fn compact_now(&self) -> AdbResult<Option<adb_store::CompactStats>> {
        let Some(store) = &self.store else { return Ok(None) };
        let store_err = |e: adb_store::StoreError| AdbError::Internal(format!("compaction: {e}"));
        let mut seq = self.seq.lock().await;
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        // Every row id (all versions) of the lineages forgotten since the last
        // compaction: their effects are redacted from the log below.
        let forgotten: std::collections::HashSet<(Memory, String)> = std::mem::take(&mut *self.forgotten.lock().unwrap());
        let mut rows: std::collections::HashSet<(Memory, String)> = forgotten.clone();
        if !forgotten.is_empty() {
            let lineages: Vec<String> = forgotten.iter().map(|(m, k)| adb_store::sql_str(&adb_store::row_id(*m, k))).collect();
            for chunk in lineages.chunks(500) {
                let filter = format!("lineage_id IN ({})", chunk.join(", "));
                for r in store.scan(Some(&filter)).await.map_err(store_err)? {
                    rows.insert((r.data.memory, adb_store::key_of(&r.id).to_string()));
                }
            }
        }
        let stats = store.compact(HISTORY, CommitTs::from_datetime(time::now())).await.map_err(store_err)?;
        let mut redacted = 0;
        if !rows.is_empty() {
            // Close the segment holding the newest records, then rewrite the closed
            // ones without the forgotten payloads; offsets (and every subscriber's
            // position) stay.
            let ts = seq.begin();
            let marker = seq.append_record_on_new_segment(ts, Vec::new()).await.map_err(log_err)?;
            self.to_store(&marker, seq.log()).await;
            let redact = |r: &mut LogRecord| redact_record(r, &rows);
            redacted = seq.log().rewrite_closed(&redact).await.map_err(log_err)?;
        }
        tracing::info!(
            kept = stats.kept, of = stats.total, took = ?stats.took,
            disk_before = stats.bytes_before, disk_after = stats.bytes_after, log_records_redacted = redacted,
            "compacted"
        );
        Ok(Some(stats))
    }

    /// After a FORGET: make sure a compaction that started after it has finished
    /// (immediate physical erasure, W5, user 27 Oct). FORGETs arriving while one
    /// runs wait for the next, which covers them all.
    async fn erase(&self) -> AdbResult<()> {
        use std::sync::atomic::Ordering::SeqCst;
        let mine = self.erase_requested.fetch_add(1, SeqCst) + 1;
        let _one_at_a_time = self.compactor.lock().await;
        if self.erase_done.load(SeqCst) >= mine {
            return Ok(());
        }
        let covering = self.erase_requested.load(SeqCst);
        self.compact_now().await?;
        self.erase_done.store(covering, SeqCst);
        Ok(())
    }

    /// Compaction from the maintenance timer (thresholds, nightly) or an admin.
    pub async fn compact(&self) -> AdbResult<Option<adb_store::CompactStats>> {
        let _one_at_a_time = self.compactor.lock().await;
        self.compact_now().await
    }

    /// Compact the platform tables (W5-L4): deleted rows and old versions leave the
    /// disk. `None` without platform tables.
    pub async fn compact_platform(&self) -> AdbResult<Option<adb_platform::PlatformCompactStats>> {
        let Some(p) = &self.platform else { return Ok(None) };
        let stats = p.compact().await.map_err(|e| AdbError::Internal(format!("platform compaction: {e}")))?;
        tracing::info!(
            tables = stats.tables, fragments_removed = stats.fragments_removed, old_versions = stats.old_versions,
            bytes_removed = stats.bytes_removed, took = ?stats.took, "platform tables compacted"
        );
        Ok(Some(stats))
    }

    /// Whether a platform table gained more than 256 versions since its last
    /// compaction (W5-L4).
    pub async fn platform_compaction_due(&self) -> bool {
        match &self.platform {
            Some(p) => p.compaction_due().await,
            None => false,
        }
    }

    /// Commit a `WATCH` registration (MVP W6): a `cq` row holding the spec. Returns
    /// the watch id and the registration's offset.
    pub async fn register_watch(&self, spec: serde_json::Value) -> AdbResult<(String, u64)> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let id = seq.new_id(ts);
        let mut data = row(MemoryType::Working, Kind::Cq, spec);
        data.key = None;
        data.text = None;
        data.subject_id = Some(id.0.clone());
        let effect = Effect::Register(NewRow { id: id.clone(), lineage_id: id.clone(), version: 1, data });
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok((id.0, record.offset.0))
    }

    /// Commit an `UNWATCH`.
    pub async fn unregister_watch(&self, id: &str) -> AdbResult<()> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(id.to_string()) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok(())
    }

    /// The registered watches (`cq` rows): id and spec.
    pub async fn watches(&self) -> AdbResult<Vec<(String, serde_json::Value)>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let rows = store
            .current(Some("kind = 'cq'"), CommitTs::from_datetime(time::now()))
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        Ok(rows.into_iter().map(|r| (r.id, r.data.value)).collect())
    }

    /// Whether compaction is due on size: more than 16 MemWAL generations (W5), or
    /// many writes since the last one (W7).
    pub async fn compaction_due(&self) -> bool {
        match &self.store {
            Some(s) => s.writes_due(COMPACT_AFTER_WRITES) || s.generation_count().await.is_ok_and(|n| n > 16),
            None => false,
        }
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

    /// Current SEMANTIC / EPISODIC / PROCEDURAL rows with text to embed and no vector.
    async fn unembedded(&self) -> AdbResult<Vec<(adb_store::StoredRow, String)>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        let now = CommitTs::from_datetime(time::now());
        let filter = format!(
            "{} AND memory IN ('semantic', 'episodic', 'procedural') AND tombstone = false AND superseded_by IS NULL AND embedding IS NULL",
            adb_store::RECORD_KINDS
        );
        let rows = store.scan(Some(&filter)).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        Ok(rows
            .into_iter()
            .filter(|r| r.is_current(now))
            .filter_map(|r| crate::reads::embed_text(r.data.memory.into(), &r.data.value).map(|t| (r, t)))
            .collect())
    }

    /// How many records still wait for a vector (W9.10: re-embedding progress).
    pub async fn pending_embeddings(&self) -> AdbResult<u64> {
        Ok(self.unembedded().await?.len() as u64)
    }

    /// Embed current SEMANTIC / EPISODIC / PROCEDURAL rows that have text but no
    /// vector — written before there was a model, or whose embedding failed — and
    /// commit the vectors as `Embed` effects, at most `batch` rows per call (MVP W4
    /// T7). Embedding runs outside the sequencer; a row superseded or forgotten in
    /// between just gets a vector it no longer needs. Returns how many.
    pub async fn embed_backfill(&self, batch: usize) -> AdbResult<u64> {
        let (Some(_), Some(embedder)) = (&self.store, &self.embedder) else { return Ok(0) };
        let todo: Vec<(adb_store::StoredRow, String)> = self.unembedded().await?.into_iter().take(batch).collect();
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
    /// Bring `store` up to the log's end, then accept new writes on top of it.
    /// No full log replay (stage D3): the table catches up from the log's tail.
    pub async fn open(mut seq: Sequencer<L>, store: Arc<PartitionStore>) -> AdbResult<(Self, ReplayStats)> {
        let t = std::time::Instant::now();
        let store_caught_up = catch_up(&store, seq.log()).await?;
        // W9.10: another embedding model's vectors go once the log is applied (the
        // tail may hold that model's `Embed` records); the backfill embeds again.
        if store.model_changed() {
            let c = store.clear_vectors().await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            tracing::info!(rows = c.kept, took = ?c.took, "embedding model changed: vectors cleared");
        }
        let caught_up_in = t.elapsed();
        let (hydrated_rows, live) = hydrate(&store).await?;
        tracing::info!(store_caught_up, ?caught_up_in, hydrated_rows, live_keys = live.len(), hydrated_in = ?(t.elapsed() - caught_up_in), "store opened");
        let last_offset = seq.log().last_offset();
        let last_commit_ts = match last_offset.0 {
            0 => None,
            _ => seq.log().read(last_offset, 1).await.map_err(log_err)?.first().map(|r| r.commit_ts),
        };
        let stats = ReplayStats { records: last_offset.0, effects: 0, last_offset, last_commit_ts, store_caught_up, hydrated_rows };
        if let Some(last) = stats.last_commit_ts {
            seq.resume_after(last);
        }
        // Caught up with the log: the applier starts from its end.
        let applier = Some(Applier::spawn(Arc::clone(&store), ApplyBarrier::new(seq.log().last_offset().0)));
        let subs = Registry::load(&store, CommitTs::from_datetime(time::now())).await.map_err(AdbError::Internal)?;
        let timers = store
            .with_ttl()
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?
            .into_iter()
            .filter_map(|r| r.data.ttl_at.map(|t| (t.0, r.lineage_id)))
            .collect();
        // Bounds after the last commit: one that passed while ADB was down has no
        // event yet, and gets it on the first tick.
        let after = stats.last_commit_ts.map_or(i64::MIN, |t| t.0);
        let bounds = store
            .live(Some("kind = 'fact'"), CommitTs::from_datetime(time::now()))
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?
            .into_iter()
            .flat_map(|r| validity_bounds(&r.data.value).into_iter().filter(|t| *t > after).map(move |t| (t, r.lineage_id.clone())))
            .collect();
        let store = Some(store);
        Ok((Self { seq: Mutex::new(seq), store, applier, subs: Arc::new(subs), platform: None, live: std::sync::Mutex::new(live), embedder: None, erase_requested: Default::default(), erase_done: Default::default(), compactor: Mutex::new(()), platform_last: Default::default(), forgotten: Default::default(), retain_max: retain_max(), sub_idle: sub_idle(), timers: std::sync::Mutex::new(timers), bounds: std::sync::Mutex::new(bounds) }, stats))
    }

    /// Drop what the hot cache holds past its window, TTL or validity (W10).
    pub fn sweep_cache(&self) {
        if let Some(h) = self.store.as_ref().and_then(|s| s.hot()) {
            h.sweep();
        }
    }

    /// The last durable offset (tests and metrics).
    pub async fn durable_offset(&self) -> LogOffset {
        self.seq.lock().await.log().durable_offset()
    }

    /// The Lance table, if any (tests and D3 reads).
    pub fn store(&self) -> Option<&Arc<PartitionStore>> {
        self.store.as_ref()
    }

    /// The records an UPDATE (`forget = false`) or FORGET matches, as lineage ids:
    /// from Lance for the types it owns, with the backends' own matching code over
    /// its rows; FORGET on SEMANTIC / PROCEDURAL also matches every other version, so
    /// a value surviving only in an old version is forgotten too (D2 T5), and so is a
    /// fact outside its validity (AQL v0.10).
    async fn targets(&self, memory: MemoryType, predicate: &Predicate, ts: CommitTs, forget: bool, in_scope: Option<&str>) -> AdbResult<Vec<String>> {
        check_scope(memory, in_scope)?;
        let Some(store) = &self.store else { return Err(needs_table("UPDATE / FORGET")) };
        // Every earlier record is in Lance first.
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let mut ids = crate::query::targets(store, memory, predicate, in_scope, ts).await?;
        if forget && supersedes(memory) {
            let mut old = store.versions(memory.into(), None).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            if let Some(s) = in_scope {
                old.retain(|r| r.data.scope == s);
            }
            for r in &old {
                if crate::query::matches(memory, &crate::query::record(r), predicate) {
                    ids.push(r.key().to_string());
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
        let now = CommitTs::from_datetime(time::now());
        let due = |set: &std::sync::Mutex<std::collections::BTreeSet<(i64, String)>>| set.lock().unwrap().first().is_some_and(|(t, _)| *t <= now.0);
        if !due(&self.timers) && !due(&self.bounds) {
            return Ok(0);
        }
        let mut seq = self.seq.lock().await;
        // Every earlier record is in Lance first.
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let ts = seq.begin();
        let candidates: Vec<String> = {
            let mut timers = self.timers.lock().unwrap();
            let mut out = Vec::new();
            while out.len() < EXPIRE_BATCH && timers.first().is_some_and(|(t, _)| *t <= ts.0) {
                out.push(timers.pop_first().expect("checked").1);
            }
            out
        };
        let mut effects = Vec::new();
        for lineage in &candidates {
            if let Some(r) = store.ttl_version(lineage).await.map_err(|e| AdbError::Internal(format!("store: {e}")))? {
                if r.data.ttl_at.is_some_and(|t| t <= ts) {
                    effects.push(Effect::Expire { memory: r.data.memory, lineage_id: RecordId(r.key().to_string()) });
                }
            }
        }
        let bounds: Vec<(i64, String)> = {
            let mut bounds = self.bounds.lock().unwrap();
            let mut out = Vec::new();
            while out.len() < EXPIRE_BATCH && bounds.first().is_some_and(|(t, _)| *t <= ts.0) {
                out.push(bounds.pop_first().expect("checked"));
            }
            out
        };
        for (t, lineage) in &bounds {
            // Still the live version's bound (a newer version or FORGET may have moved it).
            let Some(r) = store.ttl_version(lineage).await.map_err(|e| AdbError::Internal(format!("store: {e}")))? else { continue };
            if r.data.kind != Kind::Fact || effects.iter().any(|e| matches!(e, Effect::Expire { lineage_id, .. } if lineage_id.0 == r.key())) {
                continue;
            }
            let (from, to) = adb_core::time::validity(&r.data.value);
            for (bound, holds) in [(from, true), (to, false)] {
                if bound == Some(*t) {
                    effects.push(Effect::Validity { memory: r.data.memory, lineage_id: RecordId(r.key().to_string()), holds });
                }
            }
        }
        if effects.is_empty() {
            return Ok(0);
        }
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
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
                // Purged above it: the log drops a platform record only once the
                // platform tables applied it (`platform_floor`), so the gap held
                // nothing for them — go on from the oldest record kept.
                Err(LogError::Purged { oldest, .. }) => {
                    from = oldest;
                    continue;
                }
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
        // TTL timers (W9.12) for records written with one; validity timers for facts
        // with a bound ahead (AQL v0.10).
        for e in &record.effects {
            if let Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) | Effect::Supersede { new: n, .. } = e {
                if let Some(t) = n.data.ttl_at {
                    self.timers.lock().unwrap().insert((t.0, adb_store::row_id(n.data.memory, &n.lineage_id.0)));
                }
                if n.data.kind == Kind::Fact {
                    let lineage = adb_store::row_id(n.data.memory, &n.lineage_id.0);
                    let mut bounds = self.bounds.lock().unwrap();
                    for t in validity_bounds(&n.data.value).into_iter().filter(|t| *t > record.commit_ts.0) {
                        bounds.insert((t, lineage.clone()));
                    }
                }
            }
        }
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
                    // Keep what subscribers haven't acked, up to `retain_max` behind the
                    // tip (E3); never above Lance's durable offset.
                    let mut floor = d.next();
                    // Platform tables apply synchronously; the log keeps a platform
                    // record only if its apply failed (they catch up on the next open).
                    if let Some(p) = self.platform_floor().await {
                        floor = LogOffset(floor.0.min(p.0));
                    }
                    if let Some(cursor) = self.subs.min_cursor() {
                        let cap = record.offset.0.saturating_sub(self.retain_max);
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
        for chunk in batch.chunks(256) {
            applied += store.apply_many(chunk).await.map_err(store_err)? as u64;
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
        check_scope(memory_type, in_scope)?;
        // Embedded before the sequencer, so the model doesn't serialise writes (T4).
        let vector = match crate::reads::lance_owned(memory_type) && self.store.is_some() {
            true => self.vector_of(crate::reads::embed_text(memory_type, &payload)).await,
            false => None,
        };
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let id = RecordId(store_id(memory_type, key, &payload));
        // Checked under the sequencer lock, so two STOREs of one key can't both pass.
        if caller_keyed(memory_type, &payload) {
            if self.key_taken(memory_type.into(), &id.0, ts) {
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
        // The table is the only copy: answer with the record as reads shape it.
        let stored = crate::query::record(&adb_store::StoredRow::from_new(&adb_store::qualified(&new), ts, 0, 0));
        self.to_store(&record, seq.log()).await;
        Ok(stored)
    }

    async fn store_fact(
        &self,
        mut payload: serde_json::Value,
        multi: bool,
        scope: Scope,
        namespace: Option<&str>,
        ttl: Option<Duration>,
        in_scope: Option<&str>,
    ) -> AdbResult<MemoryRecord> {
        let memory = MemoryType::Semantic;
        check_scope(memory, in_scope)?;
        let field = |f: &str| payload.get(f).and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(String::from);
        let (Some(subject), Some(predicate)) = (field("subject"), field("predicate")) else {
            return Err(AdbError::InvalidSyntax("FACT needs subject and predicate".into()));
        };
        let object = field("object");
        let value = payload.get("value").cloned();
        let shown = match (&object, &value) {
            (Some(o), _) => o.clone(),
            (None, Some(v)) => v.as_str().map(String::from).unwrap_or_else(|| v.to_string()),
            (None, None) => return Err(AdbError::InvalidSyntax("FACT needs object or value".into())),
        };
        let key = fact_key(&subject, &predicate, multi.then_some(shown.as_str()));
        if let Some(obj) = payload.as_object_mut() {
            normalize_validity(obj, None)?;
            // On STORE a null bound is no bound.
            obj.retain(|k, v| !(matches!(k.as_str(), "valid_from" | "valid_to") && v.is_null()));
        }
        let confidence = payload.get("confidence").and_then(|v| v.as_f64()).unwrap_or(1.0) as f32;
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("key".into(), serde_json::json!(key));
            obj.entry("content").or_insert_with(|| serde_json::json!(format!("{subject} {predicate} {shown}")));
        }
        let vector = match self.store.is_some() {
            true => self.vector_of(crate::reads::embed_text(memory, &payload)).await,
            false => None,
        };
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let shaped = |kind: Kind, value: serde_json::Value| {
            let mut d = row(memory, kind, value);
            d.visibility = scope.into();
            d.namespace = namespace.map(String::from);
            if let Some(s) = in_scope {
                d.scope = s.to_string();
            }
            d
        };
        let mut data = shaped(Kind::Fact, payload);
        data.subject_id = Some(subject.clone());
        data.predicate = Some(predicate.clone());
        data.object_id = object.clone();
        data.link_type = Some(predicate);
        data.weight = Some(confidence);
        data.embedding = vector;
        data.ttl_at = ttl.map(|d| CommitTs(ts.0 + d.as_micros() as i64));
        let mut effects = Vec::new();
        // Entities at the fact's ends, created when missing (stubs: key and name).
        let mut entities: Vec<String> = Vec::new();
        for e in std::iter::once(&subject).chain(object.iter()) {
            if !self.key_taken(memory.into(), e, ts) && !entities.contains(e) {
                entities.push(e.clone());
                let id = RecordId(e.clone());
                let stub = shaped(Kind::Entity, serde_json::json!({ "key": e, "name": e }));
                effects.push(Effect::PutEntity(NewRow { id: id.clone(), lineage_id: id, version: 1, data: stub }));
            }
        }
        let new = match (self.key_taken(memory.into(), &key, ts), &self.store) {
            (true, Some(store)) => {
                if let Some(b) = self.barrier() {
                    b.wait_for_committed().await;
                }
                let cur = store
                    .current_version(memory.into(), &key, ts)
                    .await
                    .map_err(|e| AdbError::Internal(format!("store: {e}")))?
                    .ok_or_else(|| AdbError::Internal(format!("fact {key}: no current version in Lance")))?;
                let new = NewRow { id: seq.new_id(ts), lineage_id: RecordId(key.clone()), version: cur.version + 1, data };
                effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new: new.clone() });
                new
            }
            _ => {
                let id = RecordId(key.clone());
                let new = NewRow { id: id.clone(), lineage_id: id, version: 1, data };
                effects.push(Effect::PutFact(new.clone()));
                new
            }
        };
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        if self.store.is_some() {
            let mut live = self.live.lock().unwrap();
            live.insert((memory.into(), key.clone()), new.data.ttl_at);
            for e in entities {
                live.insert((memory.into(), e), None);
            }
        }
        let stored = crate::query::record(&adb_store::StoredRow::from_new(&adb_store::qualified(&new), ts, 0, 0));
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
        let mut fields = match &data {
            serde_json::Value::Object(map) => map.clone(),
            other => {
                let mut m = serde_json::Map::new();
                m.insert("value".into(), other.clone());
                m
            }
        };
        if memory_type == MemoryType::Semantic {
            normalize_validity(&mut fields, None)?;
        }
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
                if let (Kind::Fact, (Some(from), Some(to))) = (new.data.kind, time::validity(&new.data.value)) {
                    if from >= to {
                        return Err(AdbError::InvalidSyntax(format!("{key}: valid_from must be before valid_to")));
                    }
                }
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
        self.to_store(&record, seq.log()).await;
        Ok(ids.len() as u64)
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
        self.forgotten.lock().unwrap().extend(ids.iter().map(|id| (Memory::from(memory_type), id.clone())));
        let removed = ids.len() as u64;
        self.to_store(&record, seq.log()).await;
        drop(seq);
        // Physically gone before FORGET returns (W5): table, versions, log.
        if self.store.is_some() {
            self.erase().await.map_err(|e| {
                AdbError::Internal(format!("forgotten (hidden from every read), but not yet erased from disk: {e}; the next compaction erases it"))
            })?;
        }
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
        // Both ends must exist when the link commits (L4, W9): checked under the
        // sequencer, after every earlier write is applied, so a FORGET racing this
        // LINK can't leave it pointing at nothing.
        if let Some(store) = &self.store {
            if let Some(b) = self.barrier() {
                b.wait_for_committed().await;
            }
            for (end, memory, id) in [("source", from_type, from_id), ("target", to_type, to_id)] {
                let key = adb_core::Predicate::Key { field: "id".into(), value: adb_core::Value::String(id.to_string()) };
                if crate::query::read(store, memory, &key, &Default::default(), ts).await?.is_empty() {
                    return Err(AdbError::QueryError(format!("LINK: the {end} record {memory:?} {id:?} doesn't exist (any more)")));
                }
            }
        }
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
        // Links are read from the table's edge rows (D4.1).
        let Effect::PutEdge(n) = &effect else { unreachable!() };
        let link = crate::reads::to_link(&adb_store::StoredRow::from_new(&adb_store::qualified(n), ts, 0, 0))
            .ok_or_else(|| AdbError::Internal("edge without to_memory".into()))?;
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
            // Past log retention the first read is a `reset` from the table (W9).
            Some(n) => n.max(1).min(tip + 1) - 1,
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
        self.platform_last.store(record.offset.0, std::sync::atomic::Ordering::SeqCst);
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

/// The live keys of SEMANTIC / EPISODIC / PROCEDURAL from the table's current rows,
/// for the D3 S4 key check. Returns how many current records it read.
pub async fn hydrate(store: &PartitionStore) -> AdbResult<(u64, LiveKeys)> {
    let now = CommitTs::from_datetime(time::now());
    // Live, not current: a fact outside its validity still holds its key.
    let rows = store.live(None, now).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
    let records: Vec<_> = rows.iter().filter(|r| matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool)).collect();
    let live: LiveKeys = records
        .iter()
        .filter(|r| unique_keys(r.data.memory.into()))
        .map(|r| ((r.data.memory, r.key().to_string()), r.data.ttl_at))
        .collect();
    Ok((records.len() as u64, live))
}

/// The id a STORE gives a record: TOOLS' `tool_id` / PROCEDURAL's `pattern_id` when
/// the payload has one, else the key.
pub fn store_id(memory: MemoryType, key: &str, payload: &serde_json::Value) -> String {
    let field = match memory {
        MemoryType::Tools => Some("tool_id"),
        MemoryType::Procedural => Some("pattern_id"),
        _ => None,
    };
    field.and_then(|f| payload.get(f)).and_then(|v| v.as_str()).map(String::from).unwrap_or_else(|| key.to_string())
}

/// Memory types whose `UPDATE` writes a new version (D2 §2).
fn supersedes(memory: MemoryType) -> bool {
    matches!(memory, MemoryType::Semantic | MemoryType::Procedural)
}

/// A fact's `valid_from` / `valid_to`, the bounds it has (AQL v0.10).
fn validity_bounds(value: &serde_json::Value) -> Vec<i64> {
    let (from, to) = adb_core::time::validity(value);
    from.into_iter().chain(to).collect()
}

/// `valid_from` / `valid_to` in `fields` checked and stored in one form (RFC 3339,
/// UTC: `time::format_instant`); `null` stays (UPDATE removes the bound).
/// With `whole`, the record they'll be part of: `valid_from` must be before `valid_to`.
pub fn normalize_validity(fields: &mut serde_json::Map<String, serde_json::Value>, whole: Option<&serde_json::Value>) -> AdbResult<()> {
    for f in ["valid_from", "valid_to"] {
        match fields.get(f) {
            None | Some(serde_json::Value::Null) => {}
            Some(serde_json::Value::String(s)) => {
                let t = time::parse_instant(s).ok_or_else(|| AdbError::InvalidSyntax(format!("{f}: \"{s}\" is not a date (\"2026-11-01\") or an RFC 3339 time")))?;
                fields.insert(f.into(), serde_json::json!(time::format_instant(t)));
            }
            Some(other) => return Err(AdbError::InvalidSyntax(format!("{f}: a date or time string, not {other}"))),
        }
    }
    let mut merged = whole.cloned().unwrap_or(serde_json::json!({}));
    adb_core::merge_fields(&mut merged, fields);
    if let (Some(from), Some(to)) = adb_core::time::validity(&merged) {
        if from >= to {
            return Err(AdbError::InvalidSyntax("valid_from must be before valid_to".into()));
        }
    }
    Ok(())
}

/// A fact's key: one current fact per (subject, predicate), or per (subject,
/// predicate, object) for a `MULTI` predicate.
pub fn fact_key(subject: &str, predicate: &str, object: Option<&str>) -> String {
    match object {
        Some(o) => format!("fact:{subject}|{predicate}|{o}"),
        None => format!("fact:{subject}|{predicate}"),
    }
}

/// The version after `cur` with `fields` merged in, under the new row id `id`.
fn next_version(cur: &adb_store::StoredRow, fields: &serde_json::Map<String, serde_json::Value>, id: RecordId) -> NewRow {
    let mut data = cur.data.clone();
    adb_core::merge_fields(&mut data.value, fields);
    if let Some(c) = fields.get("content") {
        data.text = c.as_str().map(String::from);
    }
    // The embedded text changed: the old vector no longer describes it (the caller
    // or the backfill embeds the new one).
    if ["content", "pattern", "steps"].iter().any(|f| fields.contains_key(*f)) {
        data.embedding = None;
    }
    // `ttl_at` is carried over as is: UPDATE doesn't extend a TTL (D2 T6).
    NewRow { id, lineage_id: RecordId(cur.key().to_string()), version: cur.version + 1, data }
}


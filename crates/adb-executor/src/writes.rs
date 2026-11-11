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
    /// `namespace`: only records of that namespace (`NAMESPACE`; NS: keys repeat across
    /// namespaces); `None`: every namespace.
    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64>;

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64>;

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

    /// AQL v0.10 MERGE ENTITY (GR-12): SEMANTIC entity `from` merges into `into`.
    /// Returns how many facts and links moved.
    async fn merge(&self, _from: &str, _into: &str) -> AdbResult<u64> {
        Err(needs_table("MERGE"))
    }

    /// The entity a merged entity's key is now an alias of (GR-12).
    fn alias_of(&self, _key: &str) -> Option<String> {
        None
    }

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

    /// Several platform-table effects as one transaction (DB-6): one log record,
    /// checked together, applied in order before it returns.
    async fn platform_many(&self, _effects: Vec<Effect>) -> AdbResult<u64> {
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
/// Whether `predicate` is only `key = literal` or `id = literal` (in any scope): its
/// matches are fixed by the key, which every version of a lineage shares.
fn key_only(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Key { field, .. } => matches!(field.as_str(), "id" | "key"),
        Predicate::Where { conditions } => {
            conditions.len() == 1
                && matches!(&conditions[0], adb_core::Condition::Simple { field, operator: adb_core::Operator::Eq, .. } if matches!(field.as_str(), "id" | "key"))
        }
        _ => false,
    }
}

fn caller_keyed(memory: MemoryType, payload: &serde_json::Value) -> bool {
    unique_keys(memory)
        && (payload.get("key").is_some()
            || payload.get("id").is_some()
            || (memory == MemoryType::Procedural && payload.get("pattern_id").is_some()))
}

/// The record id of caller key `key` in the statement's scope (E-L12): keys are
/// unique per (scope, memory type), so a named scope's keys are `<scope>~<key>`. A
/// key already in that form is taken as it is; any other `~` is refused.
/// The record id of `key` in `in_scope` and `namespace` (E-L12; NS, schema v5).
fn scoped_key(namespace: Option<&str>, in_scope: Option<&str>, key: &str) -> AdbResult<String> {
    if let Some(ns) = namespace.filter(|ns| !adb_store::valid_namespace(ns)) {
        return Err(AdbError::QueryError(format!("NAMESPACE {ns:?}: a namespace can't be empty or contain '|' or '~'")));
    }
    let scope = in_scope.unwrap_or(adb_store::DEFAULT_SCOPE);
    let id = adb_store::scoped_id(scope, key);
    let bare = if scope == adb_store::DEFAULT_SCOPE { id.as_str() } else { &id[scope.len() + 1..] };
    if bare.contains(adb_store::SCOPE_SEP) {
        return Err(AdbError::QueryError(format!(
            "key {key:?}: a key can't contain '~' (a record in a named scope has the id <scope>~<key>)"
        )));
    }
    Ok(adb_store::namespaced(namespace, &id))
}

fn key_exists(memory: MemoryType, key: &str) -> AdbError {
    AdbError::KeyExists { memory_type: format!("{memory:?}").to_uppercase(), key: key.to_string() }
}

/// Drop from `r` the effects that carry a forgotten row's content (W5): its puts,
/// versions, patches and vectors, and the edges touching it. `Forget` effects and
/// everything else stay. Returns whether anything was dropped.
/// Every row id (all versions) of the `forgotten` lineages, themselves included:
/// what [`redact_record`] takes out of the log. Read before the table is compacted.
pub(crate) async fn forgotten_rows(
    store: &adb_store::PartitionStore,
    forgotten: std::collections::HashSet<(Memory, String)>,
) -> Result<std::collections::HashSet<(Memory, String)>, adb_store::StoreError> {
    let lineages: Vec<String> = forgotten.iter().map(|(m, k)| adb_store::sql_str(&adb_store::row_id(*m, k))).collect();
    let mut rows = forgotten;
    for chunk in lineages.chunks(500) {
        let filter = format!("lineage_id IN ({})", chunk.join(", "));
        for r in store.scan(Some(&filter)).await? {
            rows.insert((r.data.memory, adb_store::key_of(&r.id).to_string()));
        }
    }
    Ok(rows)
}

/// A log as readers outside the engine see it (subscriptions, the fact feed, WATCH):
/// records hide what a later FORGET took until compaction redacts the log itself, so no
/// read misses a FORGET (user, 7 Nov).
pub(crate) struct RedactingLog {
    pub(crate) inner: crate::engine::EngineLog,
    pub(crate) forgotten: Arc<std::sync::Mutex<std::collections::HashSet<(Memory, String)>>>,
}

#[async_trait]
impl Log for RedactingLog {
    fn partition(&self) -> adb_core::log::PartitionId {
        self.inner.partition()
    }
    async fn append(&self, record: LogRecord) -> adb_log::LogResult<LogOffset> {
        self.inner.append(record).await
    }
    async fn read(&self, from: LogOffset, max: usize) -> adb_log::LogResult<Vec<LogRecord>> {
        let mut records = self.inner.read(from, max).await?;
        let gone = self.forgotten.lock().unwrap();
        if !gone.is_empty() {
            for r in &mut records {
                redact_record(r, &gone);
            }
        }
        Ok(records)
    }
    fn last_offset(&self) -> LogOffset {
        self.inner.last_offset()
    }
    fn durable_offset(&self) -> LogOffset {
        self.inner.durable_offset()
    }
    async fn purge_below(&self, offset: LogOffset) -> adb_log::LogResult<()> {
        self.inner.purge_below(offset).await
    }
    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> adb_log::LogResult<usize> {
        self.inner.rewrite_closed(redact).await
    }
}

/// The longest a FORGET's bytes wait on disk for a compaction when no other trigger runs one.
const ERASE_WITHIN: Duration = Duration::from_secs(3600);

/// The most platform writes one group commits (one record, one Raft entry).
const PLATFORM_GROUP_MAX: usize = 256;

/// Last write offset per record key (PF-1), bounded: entries at or below the applied
/// offset are dropped once there are many, and a prepare older than what was dropped
/// counts as a conflict (it retries under the sequencer).
#[derive(Default)]
struct KeyOffsets {
    map: std::collections::HashMap<(Memory, String), u64>,
    floor: u64,
}

impl KeyOffsets {
    const MAX: usize = 100_000;

    fn conflict<'a>(&self, keys: impl IntoIterator<Item = &'a (Memory, String)>, snapshot: u64) -> bool {
        snapshot < self.floor || keys.into_iter().any(|k| self.map.get(k).is_some_and(|&o| o > snapshot))
    }

    fn note(&mut self, record: &LogRecord, applied: u64) {
        let off = record.offset.0;
        for e in &record.effects {
            match e {
                Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) | Effect::PutEdge(n) | Effect::Supersede { new: n, .. } => {
                    self.map.insert((n.data.memory, n.lineage_id.0.clone()), off);
                    // A fact about an entity: FORGET's cascade over that entity (GR-13)
                    // must see it.
                    for end in [&n.data.subject_id, &n.data.object_id].into_iter().flatten() {
                        self.map.insert((n.data.memory, refs_key(end)), off);
                    }
                }
                Effect::Patch { memory, id, .. } | Effect::Embed { memory, id, .. } => {
                    self.map.insert((*memory, id.0.clone()), off);
                }
                Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } => {
                    self.map.insert((*memory, lineage_id.0.clone()), off);
                }
                _ => {}
            }
        }
        if self.map.len() > Self::MAX {
            self.map.retain(|_, o| *o > applied);
            self.floor = self.floor.max(applied);
        }
    }
}

/// The pseudo key standing for "the facts about `entity`" in [`KeyOffsets`].
fn refs_key(entity: &str) -> String {
    format!("\u{0}refs:{}", adb_store::key_of(entity))
}

struct PlatformPending {
    effects: Vec<Effect>,
    tx: tokio::sync::oneshot::Sender<AdbResult<u64>>,
}

/// A submitted platform record for the applier (PF-4): applied once `go` says it
/// committed, then each of `done` gets the answer.
struct PlatformTicket {
    record: LogRecord,
    go: tokio::sync::oneshot::Receiver<bool>,
    done: Vec<tokio::sync::oneshot::Sender<AdbResult<u64>>>,
}

/// The platform applier (PF-4): applies committed platform records in log order, off
/// the sequencer, everything waiting at once (`apply_batch`: one Lance commit for a run
/// of upserts into one table), and answers their writers.
fn spawn_platform_applier(platform: Arc<adb_platform::PlatformStore>, applied: Arc<tokio::sync::watch::Sender<u64>>) -> tokio::sync::mpsc::UnboundedSender<PlatformTicket> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PlatformTicket>();
    tokio::spawn(async move {
        // A record that did not commit: the engine restarts; nothing after it applies.
        let mut stopped = false;
        loop {
            // PF-5: what waits in memory goes to Lance when due — between batches, or
            // after FLUSH_AFTER when no write comes.
            let first = tokio::select! {
                t = rx.recv() => match t {
                    Some(t) => t,
                    None => break,
                },
                _ = tokio::time::sleep(adb_platform::FLUSH_AFTER), if platform.pending_rows() > 0 => {
                    if let Err(e) = platform.flush_due().await {
                        tracing::warn!(error = %e, "platform memtable flush failed; retrying");
                    }
                    continue;
                }
            };
            let mut batch = vec![first];
            while batch.len() < 1024 {
                match rx.try_recv() {
                    Ok(t) => batch.push(t),
                    Err(_) => break,
                }
            }
            let mut ready = Vec::with_capacity(batch.len());
            for mut t in batch {
                stopped |= !matches!((&mut t.go).await, Ok(true));
                if stopped {
                    for d in t.done {
                        let _ = d.send(Err(AdbError::Internal("this ADB engine is restarting after a write failed to commit; retry".into())));
                    }
                } else {
                    ready.push(t);
                }
            }
            let Some(last) = ready.last().map(|t| t.record.offset.0) else { continue };
            let records: Vec<LogRecord> = ready.iter().map(|t| t.record.clone()).collect();
            let t0 = std::time::Instant::now();
            let results = platform.apply_batch(&records).await;
            tracing::debug!(target: "adb::write", records = records.len(), apply_us = t0.elapsed().as_micros() as u64, "platform apply");
            for (t, r) in ready.into_iter().zip(results) {
                let r = r.map(|_| t.record.offset.0).map_err(|e| e.to_string());
                for d in t.done {
                    let _ = d.send(r.clone().map_err(AdbError::QueryError));
                }
            }
            applied.send_modify(|a| *a = (*a).max(last));
            if let Err(e) = platform.flush_due().await {
                tracing::warn!(error = %e, "platform memtable flush failed; retrying");
            }
        }
    });
    tx
}

/// Adjacent upserts to the same table on the same key, as one: `merge_insert` keeps the
/// last row per key, which is what applying them one after another leaves. Everything
/// else stays as it is and where it is.
fn merge_upserts(effects: Vec<Effect>) -> Vec<Effect> {
    let mut out: Vec<Effect> = Vec::with_capacity(effects.len());
    for e in effects {
        if let (Some(Effect::TableUpsert { table: t0, key: k0, rows: r0 }), Effect::TableUpsert { table, key, rows }) = (out.last_mut(), &e) {
            if t0 == table && k0 == key {
                r0.extend(rows.iter().cloned());
                continue;
            }
        }
        out.push(e);
    }
    out
}

pub(crate) fn redact_record(r: &mut LogRecord, rows: &std::collections::HashSet<(Memory, String)>) -> bool {
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

pub(crate) fn retain_max() -> u64 {
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

/// DB-19: the platform table that records every merge and revert (append-only).
pub const MERGES: &str = "adb_merges";
pub const MERGE_COLUMNS: [&str; 9] = ["id", "kind", "namespace", "branch", "by", "at", "offset", "entries", "of"];

/// One record change of a merge (or a revert): replace `main` (the main row it expects
/// to be current) with `value`, add `value` as a new lineage (`main` none, `template`
/// gives the row's shape), or end the lineage (`value` none).
#[derive(Debug, Clone)]
pub struct MergeOp {
    pub memory: MemoryType,
    pub lineage: String,
    pub main: Option<adb_store::StoredRow>,
    pub value: Option<serde_json::Value>,
    pub template: Option<adb_store::StoredRow>,
    /// For an add that re-creates an ended lineage: its last version.
    pub prev_version: Option<u32>,
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
    branches: Arc<crate::branches::Branches>,
    /// On a branch engine (B2 / B3): the parent its writes are read against.
    parent: Option<crate::branch_engine::ParentView>,
    /// Platform tables (stage G), applied by their own applier (PF-4).
    platform: Option<Arc<adb_platform::PlatformStore>>,
    /// PF-4: tickets for the platform applier, in log order (set by `attach_platform`).
    platform_tickets: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<PlatformTicket>>>,
    /// The highest offset the platform applier has applied.
    platform_applied: Arc<tokio::sync::watch::Sender<u64>>,
    /// The last submitted record that creates a table or changes one's shape: writes
    /// are validated against the tables, so they wait until the applier has it.
    platform_reshape: std::sync::atomic::AtomicU64,
    /// The live keys of SEMANTIC / EPISODIC / PROCEDURAL (D3 S4 key check), with
    /// their TTL: built from Lance at open, kept by the writes below under the
    /// sequencer lock. Checking a key in Lance instead costs a read per STORE.
    live: std::sync::Mutex<LiveKeys>,
    /// Embeds `content` on STORE / UPDATE and in the backfill (MVP W4); `None`: no
    /// vectors (records are then found by filters and text only).
    embedder: Option<Arc<adb_embed::Embedder>>,
    /// W5: FORGETs waiting for physical erasure, and those a compaction has covered;
    /// concurrent FORGETs share one compaction.
    /// When the oldest FORGET not yet erased from disk committed: compaction is due
    /// once it waited [`ERASE_WITHIN`] (FORGET itself only commits a tombstone).
    forgotten_since: std::sync::Mutex<Option<std::time::Instant>>,
    compactor: Mutex<()>,
    /// Offset of the last record with platform-table effects (stage G). The log
    /// must keep it until the platform tables have applied it.
    platform_last: std::sync::atomic::AtomicU64,
    /// Platform writes waiting for the sequencer: the next holder commits them as one
    /// group (one record, one Lance commit per table), see [`Self::platform_many`].
    platform_queue: std::sync::Mutex<Vec<PlatformPending>>,
    /// The offset of the last write to each record key (PF-1): a write prepared
    /// outside the sequencer against offset N conflicts if one of its keys was
    /// written after N.
    key_offsets: std::sync::Mutex<KeyOffsets>,
    /// A submitted record failed to commit: this engine takes no more writes and
    /// restarts (as when it loses the lead).
    poisoned: std::sync::atomic::AtomicBool,
    /// DB-7: the last committed record (offset, commit time µs), for answers' memory point.
    last_off: std::sync::atomic::AtomicU64,
    last_ts: std::sync::atomic::AtomicI64,
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
    forgotten: Arc<std::sync::Mutex<std::collections::HashSet<(Memory, String)>>>,
    /// DB-4: per namespace, (records, payload bytes) of current rows. Built by one scan when
    /// first needed, added to on each STORE, dropped by FORGET and compaction.
    usage: Mutex<Option<std::collections::HashMap<String, (u64, u64)>>>,
    /// B4: the commit times compaction must keep readable (branch points), per namespace.
    pins: std::sync::OnceLock<Pins>,
    /// B4, on the org's engine: its branch engines, which a FORGET on main reaches too.
    branch_host: std::sync::OnceLock<Arc<crate::branch_engine::BranchHost>>,
    /// Entity aliases (GR-12): a merged entity's key → the entity it merged into,
    /// from the survivors' `aliases` fields at open, kept by MERGE and FORGET.
    aliases: std::sync::Mutex<std::collections::HashMap<String, String>>,
    /// FlowR's engine store (stage H): its spaces, written under the sequencer.
    engine: Option<Arc<Mutex<adb_engine::EngineHost>>>,
    /// Its subscribers, reachable without the host lock.
    engine_subs: Option<Arc<adb_engine::Subs>>,
    /// Pipelined engine writes: the committer (in log order) and the last engine
    /// record submitted and committed. Reads wait for what was submitted before them.
    engine_tickets: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<EngineTicket>>>,
    engine_submitted: std::sync::atomic::AtomicU64,
    engine_committed: Arc<tokio::sync::watch::Sender<u64>>,
}

/// An engine operation for the committer: once `go` says its record committed (none:
/// no durable change), the journal gets its episodes, the space's subscribers its
/// notices, and the caller its answer — all in log order.
struct EngineTicket {
    space: String,
    record: Option<LogRecord>,
    go: Option<tokio::sync::oneshot::Receiver<bool>>,
    notices: Vec<(String, serde_json::Value)>,
    done: tokio::sync::oneshot::Sender<AdbResult<()>>,
}

fn spawn_engine_committer(host: Arc<Mutex<adb_engine::EngineHost>>, committed: Arc<tokio::sync::watch::Sender<u64>>) -> tokio::sync::mpsc::UnboundedSender<EngineTicket> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EngineTicket>();
    tokio::spawn(async move {
        // A record that did not commit: the engine restarts; nothing after it goes on.
        let mut stopped = false;
        while let Some(t) = rx.recv().await {
            if let Some(go) = t.go {
                stopped |= !matches!(go.await, Ok(true));
            }
            if stopped {
                let _ = t.done.send(Err(AdbError::Internal("this ADB engine is restarting after a write failed to commit; retry".into())));
                continue;
            }
            let mut h = host.lock().await;
            let r = match &t.record {
                Some(rec) => h.committed(rec).await.map_err(|e| AdbError::QueryError(e.to_string())),
                None => Ok(()),
            };
            h.notify(&t.space, t.notices);
            drop(h);
            if let Some(rec) = &t.record {
                committed.send_modify(|c| *c = (*c).max(rec.offset.0));
            }
            let _ = t.done.send(r);
        }
    });
    tx
}

/// Records written since the last compaction that make one due (W7, user 28 Oct:
/// thousands of small puts slow every scan; at least a quarter of the table).
pub const COMPACT_AFTER_WRITES: u64 = 500;

/// Compaction policy (7 Nov: reads slow down with every LSM layer and superseded
/// version they merge; one 0.3 s compaction brought merged reads back to Lance-only
/// speed). Due after `ADB_COMPACT_AFTER_WRITES` writes (default 500, or a 20th of the
/// table's rows if more), or past `ADB_COMPACT_GENERATIONS` MemWAL generations
/// (default 4). Until PF-7 a compaction pauses writes while it runs.
pub async fn compaction_due_for(store: &adb_store::PartitionStore) -> bool {
    let env = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    store.writes_due(env("ADB_COMPACT_AFTER_WRITES", COMPACT_AFTER_WRITES)) || store.generation_count().await.is_ok_and(|n| n as u64 > env("ADB_COMPACT_GENERATIONS", 4))
}

/// Compaction pins (B4): per namespace, the oldest branch point read from this engine.
pub type Pins = Arc<dyn Fn() -> std::collections::HashMap<String, CommitTs> + Send + Sync>;

/// How long superseded versions (UPDATE history) are kept (W5, user 27 Oct).
pub const HISTORY: Duration = Duration::from_secs(90 * 86_400);

pub type LiveKeys = std::collections::HashMap<(Memory, String), Option<CommitTs>>;

impl<L: Log + 'static> DurableWrites<L> {
    /// UPDATE of named keys with the sequencer held only to order it (PF-1): targets and
    /// current versions are read first, against what was committed at `snapshot`;
    /// under the lock, a write to any of the keys after `snapshot` is a conflict and
    /// `None` sends the UPDATE down the serial path. The commit is awaited unlocked.
    async fn update_prepared(
        &self,
        memory_type: MemoryType,
        predicate: &Predicate,
        data: &serde_json::Value,
        in_scope: Option<&str>,
        namespace: Option<&str>,
        vector: Option<Vec<half::f16>>,
    ) -> AdbResult<Option<u64>> {
        let Some(store) = &self.store else { return Ok(None) };
        let snapshot = self.last_off.load(std::sync::atomic::Ordering::SeqCst);
        let now = CommitTs::from_datetime(time::now());
        let ids = self.targets(memory_type, predicate, now, false, in_scope, namespace).await?;
        if ids.is_empty() {
            return Ok(None);
        }
        let mut fields = match data {
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
        let mut current = Vec::new();
        if supersedes(memory_type) {
            for key in &ids {
                match store.current_version(memory_type.into(), key, now).await.map_err(|e| AdbError::Internal(format!("store: {e}")))? {
                    Some(cur) => current.push(cur),
                    None => return Ok(None),
                }
            }
        }
        let keys: Vec<(Memory, String)> = ids.iter().map(|k| (memory_type.into(), k.clone())).collect();
        let mut seq = self.seq.lock().await;
        if self.key_offsets.lock().unwrap().conflict(&keys, snapshot) {
            return Ok(None);
        }
        let ts = seq.begin();
        let mut effects = Vec::with_capacity(ids.len());
        if supersedes(memory_type) {
            for (key, cur) in ids.iter().zip(&current) {
                let mut new = next_version(cur, &fields, seq.new_id(ts));
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
        } else {
            effects.extend(ids.iter().map(|id| Effect::Patch { memory: memory_type.into(), id: RecordId(id.clone()), fields: fields.clone(), version: 0 }));
            if let Some(v) = &vector {
                effects.extend(ids.iter().map(|id| Effect::Embed { memory: memory_type.into(), id: RecordId(id.clone()), embedding: v.clone() }));
            }
        }
        let (record, wait) = seq.submit_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store_submitted(&record, seq.log()).await;
        drop(seq);
        self.committed(record.offset.0, wait).await?;
        Ok(Some(ids.len() as u64))
    }

    /// FORGET of named keys as [`Self::update_prepared`]: targets, every version and
    /// (SEMANTIC) the entity's facts are read first; a write to any of them, or a new
    /// fact about a forgotten entity, after `snapshot` is a conflict.
    async fn forget_prepared(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<Option<u64>> {
        let snapshot = self.last_off.load(std::sync::atomic::Ordering::SeqCst);
        let now = CommitTs::from_datetime(time::now());
        let mut ids = self.targets(memory_type, predicate, now, true, in_scope, namespace).await?;
        if ids.is_empty() {
            return Ok(None);
        }
        let entities = ids.clone();
        if memory_type == MemoryType::Semantic {
            ids.extend(self.entity_facts(&ids, in_scope).await?);
        }
        let m: Memory = memory_type.into();
        let mut keys: Vec<(Memory, String)> = ids.iter().map(|k| (m, k.clone())).collect();
        if memory_type == MemoryType::Semantic {
            keys.extend(entities.iter().map(|k| (m, refs_key(k))));
        }
        let mut seq = self.seq.lock().await;
        if self.key_offsets.lock().unwrap().conflict(&keys, snapshot) {
            return Ok(None);
        }
        let ts = seq.begin();
        let effects: Vec<Effect> = ids.iter().map(|id| Effect::Forget { memory: m, lineage_id: RecordId(id.clone()) }).collect();
        self.usage_stale().await;
        let (record, wait) = seq.submit_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        self.note_forgotten(ids.iter().map(|id| (m, id.clone())));
        self.to_store_submitted(&record, seq.log()).await;
        drop(seq);
        self.committed(record.offset.0, wait).await?;
        self.forget_on_branches(memory_type, &ids).await?;
        Ok(Some(ids.len() as u64))
    }

    /// A FORGET on main reaches every branch's own engine (B4): forgotten means gone
    /// everywhere. A branch in a pod that can't be reached now erases them when it
    /// starts (B6).
    async fn forget_on_branches(&self, memory_type: MemoryType, ids: &[String]) -> AdbResult<()> {
        if let Some(host) = self.branch_host.get() {
            let unreachable = host.forget(memory_type, ids).await.map_err(|e| {
                AdbError::Internal(format!("forgotten on main, but a branch still holds a copy: {e}"))
            })?;
            for b in unreachable {
                let pending = crate::branches::PendingForget { memory: memory_type, lineages: ids.to_vec() };
                self.branch_change(&b.namespace, &b.name, |b| b.pending_forgets.push(pending)).await?;
            }
        }
        Ok(())
    }

    /// Commit `batch` under the sequencer (see [`Self::platform_many`]).
    async fn platform_group(&self, seq: &mut Sequencer<L>, batch: Vec<PlatformPending>, submitted: &mut Vec<(u64, adb_log::CommitWait, tokio::sync::oneshot::Sender<bool>)>) {
        let Some(platform) = self.platform.as_ref() else { return };
        // Validation reads the tables' shape: a create or reshape still on its way to
        // the applier must be in first.
        self.platform_settled(self.platform_reshape.load(std::sync::atomic::Ordering::SeqCst)).await;
        let all: Vec<Effect> = batch.iter().flat_map(|p| p.effects.iter().cloned()).collect();
        let together = batch.len() == 1 || platform.validate_all(&all).await.is_ok();
        if !together {
            for p in batch {
                self.platform_submit(seq, p.effects, vec![p.tx], submitted).await;
            }
            return;
        }
        let txs = batch.into_iter().map(|p| p.tx).collect();
        self.platform_submit(seq, merge_upserts(all), txs, submitted).await;
    }

    /// Validate `effects`, submit them as one record and hand it to the platform applier
    /// (PF-4): no Lance work under the sequencer. `done` get the answer once it is
    /// committed and applied; the caller awaits each `submitted` commit after the lock.
    async fn platform_submit(
        &self,
        seq: &mut Sequencer<L>,
        effects: Vec<Effect>,
        done: Vec<tokio::sync::oneshot::Sender<AdbResult<u64>>>,
        submitted: &mut Vec<(u64, adb_log::CommitWait, tokio::sync::oneshot::Sender<bool>)>,
    ) {
        let fail = |done: Vec<tokio::sync::oneshot::Sender<AdbResult<u64>>>, e: String| {
            for d in done {
                let _ = d.send(Err(AdbError::QueryError(e.clone())));
            }
        };
        let Some(platform) = self.platform.as_ref() else { return fail(done, "this engine has no platform tables".into()) };
        // A logged effect must apply: check the tables, names and columns first.
        if let Err(e) = platform.validate_all(&effects).await {
            return fail(done, e.to_string());
        }
        let mut reshape = false;
        for e in &effects {
            reshape |= match e {
                Effect::TableInsert { table, .. } | Effect::TableUpsert { table, .. } => platform.version(table).await.is_none(),
                Effect::TableCreate { .. } | Effect::TableReplace { .. } | Effect::TableAddColumns { .. } | Effect::TableAppendOnly { .. } => true,
                _ => false,
            };
        }
        let ts = seq.begin();
        let (record, wait) = match seq.submit_record(ts, None, effects).await {
            Ok(r) => r,
            Err(e) => return fail(done, log_err(e).to_string()),
        };
        let off = record.offset.0;
        self.platform_last.store(off, std::sync::atomic::Ordering::SeqCst);
        if reshape {
            self.platform_reshape.store(off, std::sync::atomic::Ordering::SeqCst);
        }
        self.to_store_submitted(&record, seq.log()).await;
        let (go, go_rx) = tokio::sync::oneshot::channel();
        let ticket = PlatformTicket { record, go: go_rx, done };
        match self.platform_tickets.lock().unwrap().as_ref() {
            Some(tx) => {
                if let Err(t) = tx.send(ticket) {
                    fail(t.0.done, "the platform applier stopped".into());
                }
            }
            None => fail(ticket.done, "the platform applier is not running".into()),
        }
        submitted.push((off, wait, go));
    }

    /// Wait until the platform applier has applied everything up to `offset`.
    async fn platform_settled(&self, offset: u64) {
        if offset == 0 {
            return;
        }
        let mut rx = self.platform_applied.subscribe();
        let _ = rx.wait_for(|a| *a >= offset).await;
    }

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
        // PF-7/PF-16: no write or read waits for the compaction; the sequencer is held
        // only for the erasure marker below. What committed before the compaction
        // started is in the table first, so what was forgotten before it leaves with it.
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        // Every row id (all versions) of the lineages forgotten since the last
        // compaction: their effects are redacted from the log below.
        // Kept in the set until the log is redacted below: readers of the log
        // (subscribers, through `RedactingLog`) hide them until then.
        let forgotten: std::collections::HashSet<(Memory, String)> = self.forgotten.lock().unwrap().clone();
        let rows = forgotten_rows(store, forgotten.clone()).await.map_err(store_err)?;
        let pins = self.pins.get().map(|f| f()).unwrap_or_default();
        // DB-8: a namespace's own history window, where a quota row sets one.
        let windows: Vec<(String, Duration)> = self.branches.quotas().into_iter()
            .filter_map(|q| q.history_days.map(|d| (q.pattern, Duration::from_secs(d * 86_400))))
            .collect();
        let now = CommitTs::from_datetime(time::now());
        // PF-16: in place, without the sequencer; only the swap holds readers, briefly.
        let stats = store.compact_with_windows(HISTORY, now, &pins, windows).await.map_err(store_err)?;
        let mut redacted = 0;
        if !rows.is_empty() {
            let mut seq = self.seq.lock().await;
            // Close the segment holding the newest records, then rewrite the closed
            // ones without the forgotten payloads; offsets (and every subscriber's
            // position) stay.
            let ts = seq.begin();
            let marker = seq.append_record_on_new_segment(ts, Vec::new()).await.map_err(log_err)?;
            self.to_store(&marker, seq.log()).await;
            let redact = |r: &mut LogRecord| redact_record(r, &rows);
            redacted = seq.log().rewrite_closed(&redact).await.map_err(log_err)?;
        }
        // Gone from the table and the log: no longer hidden at read time.
        {
            let mut f = self.forgotten.lock().unwrap();
            f.retain(|k| !forgotten.contains(k));
            if f.is_empty() {
                *self.forgotten_since.lock().unwrap() = None;
            }
        }
        tracing::info!(
            kept = stats.kept, of = stats.total, took = ?stats.took,
            disk_before = stats.bytes_before, disk_after = stats.bytes_after, log_records_redacted = redacted,
            "compacted"
        );
        self.usage_stale().await;
        Ok(Some(stats))
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
    /// Cut a branch of `namespace`'s memory at the current log position (B1): one
    /// control record, nothing copied.
    /// DB-7: the last commit this engine made (offset, commit time), without the
    /// sequencer lock; from the log when nothing was written since opening.
    pub async fn point(&self) -> (u64, CommitTs) {
        match self.last_off.load(std::sync::atomic::Ordering::SeqCst) {
            0 => self.last_commit().await.unwrap_or((0, CommitTs(0))),
            off => (off, CommitTs(self.last_ts.load(std::sync::atomic::Ordering::SeqCst))),
        }
    }

    /// The last record in this engine's log: its offset and commit time.
    pub async fn last_commit(&self) -> AdbResult<(u64, CommitTs)> {
        let seq = self.seq.lock().await;
        let off = seq.log().last_offset().0;
        let ts = match off {
            0 => CommitTs::from_datetime(time::now()),
            n => seq.log().read(LogOffset(n), 1).await.map_err(log_err)?.first().map(|r| r.commit_ts).unwrap_or_else(|| CommitTs::from_datetime(time::now())),
        };
        Ok((off, ts))
    }

    pub async fn branch_create(&self, namespace: &str, name: &str, parent: Option<&str>, created_by: Option<&str>) -> AdbResult<crate::branches::Branch> {
        self.branch_create_at(namespace, name, parent, created_by, None).await
    }

    /// As [`Self::branch_create`], with the branch point given (a branch of a branch:
    /// its point is on the parent branch's own log, not the org's).
    pub async fn branch_create_at(&self, namespace: &str, name: &str, parent: Option<&str>, created_by: Option<&str>, point: Option<(u64, CommitTs)>) -> AdbResult<crate::branches::Branch> {
        self.branch_create_in(namespace, name, parent, created_by, point, None).await
    }

    /// DB-9: cut a branch of main at a past point (a tag or an offset). The point is
    /// checked and the branch's pin registered under the sequencer's lock.
    pub async fn branch_create_past(&self, namespace: &str, name: &str, created_by: Option<&str>, at: &crate::branches::PointRef) -> AdbResult<crate::branches::Branch> {
        self.branch_create_in(namespace, name, None, created_by, None, Some(at)).await
    }

    /// DB-9: the (offset, time) a past point names.
    pub async fn resolve_point(&self, namespace: &str, at: &crate::branches::PointRef) -> AdbResult<(u64, CommitTs)> {
        let seq = self.seq.lock().await;
        self.point_of(&seq, namespace, at).await
    }

    async fn point_of(&self, seq: &Sequencer<L>, namespace: &str, at: &crate::branches::PointRef) -> AdbResult<(u64, CommitTs)> {
        if let Some(tag) = &at.tag {
            let t = self.branches.tag(namespace, tag).ok_or_else(|| AdbError::QueryError(format!("{namespace:?} has no tag {tag:?}")))?;
            return Ok((t.point_offset, CommitTs(t.point_ts)));
        }
        let off = at.offset.ok_or_else(|| AdbError::QueryError("point_invalid: at needs a tag or an offset".into()))?;
        let tip = seq.log().last_offset().0;
        if off > tip {
            return Err(AdbError::QueryError(format!("point_invalid: offset {off} is past the log's end ({tip})")));
        }
        let held = match seq.log().read(LogOffset(off), 1).await {
            Ok(r) => r.first().filter(|r| r.offset.0 == off).map(|r| r.commit_ts),
            Err(LogError::Purged { .. }) => None,
            Err(e) => return Err(log_err(e)),
        };
        let ts = held.or(at.ts.map(CommitTs)).ok_or_else(|| AdbError::QueryError(format!("point_too_old: offset {off} is no longer in the log: give its ts (from the memory point) or use a tag")))?;
        let kept_from = CommitTs::from_datetime(time::now()).0 - HISTORY.as_micros() as i64;
        if ts.0 < kept_from {
            return Err(AdbError::QueryError(format!("point_too_old: offset {off} is older than the {} days of versions kept: use a tag", HISTORY.as_secs() / 86_400)));
        }
        Ok((off, ts))
    }

    async fn branch_create_in(&self, namespace: &str, name: &str, parent: Option<&str>, created_by: Option<&str>, point: Option<(u64, CommitTs)>, at: Option<&crate::branches::PointRef>) -> AdbResult<crate::branches::Branch> {
        use crate::branches::{check_name, Branch, MAIN};
        check_name(name).map_err(AdbError::QueryError)?;
        let parent = parent.filter(|p| !p.is_empty()).unwrap_or(MAIN);
        if parent != MAIN && self.branches.get(namespace, parent).is_none() {
            return Err(AdbError::QueryError(format!("no branch {parent:?} in namespace {namespace:?} to branch from")));
        }
        if self.branches.get(namespace, name).is_some() {
            return Err(AdbError::QueryError(format!("namespace {namespace:?} already has a branch {name:?}")));
        }
        let mut seq = self.seq.lock().await;
        let (point_offset, point_ts) = match (point, at) {
            (Some(p), _) => p,
            (None, Some(a)) => self.point_of(&seq, namespace, a).await?,
            (None, None) => self.last_point(&seq).await?,
        };
        let id = RecordId(Branch::id_of(namespace, name));
        let b = Branch {
            id: id.0.clone(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            parent: parent.to_string(),
            point_offset,
            point_ts: point_ts.0,
            created_by: created_by.map(String::from),
            placement: None,
            pending_forgets: Vec::new(),
        };
        self.register_branch(&mut seq, &b).await?;
        Ok(b)
    }

    /// Log `b` as the branch's control row (a new one, or its new state: the row of
    /// that id is replaced).
    async fn register_branch(&self, seq: &mut Sequencer<L>, b: &crate::branches::Branch) -> AdbResult<()> {
        self.register_control(seq, &b.id, &b.namespace, serde_json::to_value(b).unwrap_or_default()).await
    }

    /// A `branch`-kind control row (a branch, tag or layer list, by its id's prefix).
    async fn register_control(&self, seq: &mut Sequencer<L>, id: &str, namespace: &str, value: serde_json::Value) -> AdbResult<()> {
        let id = RecordId(id.to_string());
        let ts = seq.begin();
        let mut data = row(MemoryType::Working, Kind::Branch, value);
        data.key = None;
        data.text = None;
        data.namespace = Some(namespace.to_string());
        data.subject_id = Some(id.0.clone());
        let effect = Effect::Register(NewRow { id: id.clone(), lineage_id: id, version: 1, data });
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.branches.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok(())
    }

    /// The log's last offset and its commit time (now, for an empty log): a new
    /// branch's or tag's point.
    async fn last_point(&self, seq: &Sequencer<L>) -> AdbResult<(u64, CommitTs)> {
        let off = seq.log().last_offset().0;
        let ts = match off {
            0 => CommitTs::from_datetime(time::now()),
            n => seq.log().read(LogOffset(n), 1).await.map_err(log_err)?.first().map(|r| r.commit_ts).unwrap_or_else(|| CommitTs::from_datetime(time::now())),
        };
        Ok((off, ts))
    }

    /// DB-4: set or replace the quota on `pattern`.
    pub async fn quota_set(&self, pattern: &str, max_bytes: Option<u64>, max_records: Option<u64>, history_days: Option<u64>) -> AdbResult<crate::branches::Quota> {
        if pattern.is_empty() || (max_bytes.is_none() && max_records.is_none() && history_days.is_none()) {
            return Err(AdbError::QueryError("quota_set needs a pattern and max_bytes, max_records or history_days".into()));
        }
        let q = crate::branches::Quota { id: crate::branches::Quota::id_of(pattern), pattern: pattern.to_string(), max_bytes, max_records, history_days };
        let mut seq = self.seq.lock().await;
        self.register_control(&mut seq, &q.id, pattern, serde_json::to_value(&q).unwrap_or_default()).await?;
        Ok(q)
    }

    /// DB-4: remove the quota on `pattern`; whether it had one.
    pub async fn quota_drop(&self, pattern: &str) -> AdbResult<bool> {
        let id = crate::branches::Quota::id_of(pattern);
        if !self.branches.quotas().iter().any(|q| q.id == id) {
            return Ok(false);
        }
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let record = seq.append_record(ts, None, vec![Effect::Unregister { id: RecordId(id) }]).await.map_err(log_err)?;
        self.branches.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok(true)
    }

    /// DB-4: (records, payload bytes) per namespace, from one scan if not known yet.
    pub async fn usage(&self) -> AdbResult<std::collections::HashMap<String, (u64, u64)>> {
        let mut known = self.usage.lock().await;
        if let Some(u) = known.as_ref() {
            return Ok(u.clone());
        }
        let mut u = std::collections::HashMap::<String, (u64, u64)>::new();
        if let Some(store) = &self.store {
            if let Some(b) = self.barrier() {
                b.wait_for_committed().await;
            }
            let rows = store.current(Some(adb_store::RECORD_KINDS), CommitTs::from_datetime(time::now())).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            for r in rows {
                if let Some(ns) = &r.data.namespace {
                    let e = u.entry(ns.clone()).or_default();
                    e.0 += 1;
                    e.1 += r.data.value.to_string().len() as u64;
                }
            }
        }
        *known = Some(u.clone());
        Ok(u)
    }

    /// DB-4: refuse a new record that would pass a quota covering `namespace`. Checked under
    /// the sequencer lock, so a flood can't slip past between check and write. Branch engines
    /// have none: their writes aren't the org's storage.
    async fn check_quota(&self, namespace: Option<&str>, bytes: u64) -> AdbResult<()> {
        let Some(ns) = namespace.filter(|_| self.parent.is_none()) else { return Ok(()) };
        let quotas: Vec<_> = self.branches.quotas().into_iter().filter(|q| q.covers(ns)).collect();
        if quotas.is_empty() {
            return Ok(());
        }
        let usage = self.usage().await?;
        for q in quotas {
            let (records, used) = usage.iter().filter(|(n, _)| q.covers(n)).fold((0u64, 0u64), |a, (_, u)| (a.0 + u.0, a.1 + u.1));
            if q.max_records.is_some_and(|m| records + 1 > m) {
                return Err(AdbError::QueryError(format!("quota_exceeded: {:?} holds {records} records, its limit is {}", q.pattern, q.max_records.unwrap_or(0))));
            }
            if q.max_bytes.is_some_and(|m| used + bytes > m) {
                return Err(AdbError::QueryError(format!("quota_exceeded: {:?} holds {used} bytes and this adds {bytes}; its limit is {}", q.pattern, q.max_bytes.unwrap_or(0))));
            }
        }
        Ok(())
    }

    /// DB-4: count a stored record, if usage is being kept.
    async fn note_stored(&self, namespace: Option<&str>, bytes: u64) {
        if let (Some(ns), Some(u)) = (namespace, self.usage.lock().await.as_mut()) {
            let e = u.entry(ns.to_string()).or_default();
            e.0 += 1;
            e.1 += bytes;
        }
    }

    /// DB-4: usage is recounted after something left the table.
    async fn usage_stale(&self) {
        *self.usage.lock().await = None;
    }

    /// B7: tag `namespace` at the current log position (`tag:<namespace>/<name>`).
    pub async fn tag_create(&self, namespace: &str, name: &str, created_by: Option<&str>) -> AdbResult<crate::branches::Tag> {
        crate::branches::check_name(name).map_err(AdbError::QueryError)?;
        if self.branches.tag(namespace, name).is_some() {
            return Err(AdbError::QueryError(format!("namespace {namespace:?} already has a tag {name:?}")));
        }
        let mut seq = self.seq.lock().await;
        let (point_offset, ts) = self.last_point(&seq).await?;
        let t = crate::branches::Tag {
            id: crate::branches::Tag::id_of(namespace, name),
            namespace: namespace.into(),
            name: name.into(),
            point_offset,
            point_ts: ts.0,
            created_by: created_by.map(String::from),
        };
        self.register_control(&mut seq, &t.id, namespace, serde_json::to_value(&t).unwrap_or_default()).await?;
        Ok(t)
    }

    /// B7: drop a tag (refused while a pinned layer reads it).
    pub async fn tag_drop(&self, namespace: &str, name: &str) -> AdbResult<bool> {
        if self.branches.tag(namespace, name).is_none() {
            return Ok(false);
        }
        for space in self.branches.layered_by(namespace) {
            if self.branches.layers_of(&space).iter().any(|l| l.space == namespace && l.tag.as_deref() == Some(name)) {
                return Err(AdbError::QueryError(format!("tag {name:?} of {namespace:?} is pinned by {space:?}: change its layers first")));
            }
        }
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(crate::branches::Tag::id_of(namespace, name)) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.branches.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok(true)
    }

    /// B7: set `namespace`'s layers (empty: none). One level: a layer's space has no
    /// layers itself; a pin names an existing tag; layers carry SEMANTIC, PROCEDURAL or
    /// TOOLS only.
    pub async fn layers_set(&self, namespace: &str, layers: Vec<crate::branches::Layer>) -> AdbResult<crate::branches::Layers> {
        use crate::branches::LayerMode;
        if self.parent.is_some() {
            return Err(AdbError::QueryError("layers are set on the org's engine".into()));
        }
        for l in &layers {
            if l.space == namespace {
                return Err(AdbError::QueryError(format!("{namespace:?} can't be a layer of itself")));
            }
            if !adb_store::valid_namespace(&l.space) {
                return Err(AdbError::QueryError(format!("layer space {:?}: not a valid namespace", l.space)));
            }
            // D1: a promoted agent's history reaches back through its predecessors, so a
            // history layer may name a space that has layers.
            let history = l.mode == LayerMode::History;
            if !history && !self.branches.layers_of(&l.space).is_empty() {
                return Err(AdbError::QueryError(format!("{:?} has layers of its own: layers are one level deep (B7)", l.space)));
            }
            if history && (l.sealed || l.memories != [MemoryType::Episodic]) {
                return Err(AdbError::QueryError(format!("a history layer of {:?} carries EPISODIC only and isn't sealed", l.space)));
            }
            if let Some(m) = l.memories.iter().find(|m| !history && !matches!(m, MemoryType::Semantic | MemoryType::Procedural | MemoryType::Tools)) {
                return Err(AdbError::QueryError(format!("a layer carries SEMANTIC, PROCEDURAL or TOOLS, not {m:?}")));
            }
            match (l.mode, &l.tag) {
                (LayerMode::Pin, None) | (LayerMode::History, None) => return Err(AdbError::QueryError(format!("pinning {:?} needs a tag", l.space))),
                (LayerMode::Pin, Some(t)) | (LayerMode::History, Some(t)) if self.branches.tag(&l.space, t).is_none() => {
                    return Err(AdbError::QueryError(format!("{:?} has no tag {t:?}", l.space)));
                }
                _ => {}
            }
        }
        // A layer space stays flat, history layers aside (they name predecessors).
        let layered = self.branches.layered_by(namespace).iter().any(|by| self.branches.layers_of(by).iter().any(|l| l.space == namespace && l.mode != LayerMode::History));
        if layered && layers.iter().any(|l| l.mode != LayerMode::History) {
            return Err(AdbError::QueryError(format!("{namespace:?} is a layer of another space: layers are one level deep (B7)")));
        }
        let mut seq = self.seq.lock().await;
        let ls = crate::branches::Layers { id: crate::branches::Layers::id_of(namespace), namespace: namespace.into(), layers };
        self.register_control(&mut seq, &ls.id, namespace, serde_json::to_value(&ls).unwrap_or_default()).await?;
        Ok(ls)
    }

    /// B7, sealed layers: refuse a write in `namespace` of `key` (a record id without
    /// its namespace) when a sealed layer carrying `memory` has it — or, for a fact, has
    /// a fact with the same subject and predicate.
    async fn check_sealed(&self, memory: MemoryType, namespace: Option<&str>, key: &str, fact: Option<(&str, &str)>, now: CommitTs) -> AdbResult<()> {
        let (Some(ns), Some(store)) = (namespace, &self.store) else { return Ok(()) };
        for l in self.branches.layers_of(ns).iter().filter(|l| l.sealed && l.memories.contains(&memory)) {
            let at = self.layer_point(l).unwrap_or(now);
            let rows = store.versions(memory.into(), Some(&format!("namespace = {}", adb_store::sql_str(&l.space)))).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            let shadows = rows.iter().filter(|r| r.is_current_at(at, now)).any(|r| {
                let bare = adb_store::without_namespace(adb_store::key_of(&r.lineage_id));
                bare == key
                    || fact.is_some_and(|(s, p)| {
                        r.data.kind == Kind::Fact
                            && r.data.predicate.as_deref() == Some(p)
                            && r.data.subject_id.as_deref().map(|x| adb_store::unscoped(adb_store::key_of(x))) == Some(s)
                    })
            });
            if shadows {
                return Err(AdbError::QueryError(format!("{key} belongs to the sealed layer {:?} of {ns:?}: it can't be written or shadowed here", l.space)));
            }
        }
        Ok(())
    }

    /// The commit time a layer is read at: its tag's point for a pin, `None` (now) for a mount.
    pub fn layer_point(&self, l: &crate::branches::Layer) -> Option<CommitTs> {
        match l.mode {
            crate::branches::LayerMode::Mount => None,
            crate::branches::LayerMode::Pin | crate::branches::LayerMode::History => l.tag.as_ref().and_then(|t| self.branches.tag(&l.space, t)).map(|t| CommitTs(t.point_ts)),
        }
    }

    /// Change a branch's control row (B6: placement, pending FORGETs) under the
    /// sequencer lock. `None` when there is no such branch.
    pub async fn branch_change(&self, namespace: &str, name: &str, change: impl FnOnce(&mut crate::branches::Branch)) -> AdbResult<Option<crate::branches::Branch>> {
        let mut seq = self.seq.lock().await;
        let Some(mut b) = self.branches.get(namespace, name) else { return Ok(None) };
        change(&mut b);
        self.register_branch(&mut seq, &b).await?;
        Ok(Some(b))
    }

    /// Drop a branch (its own writes stay in the log until compaction; B4 erases them).
    pub async fn branch_drop(&self, namespace: &str, name: &str) -> AdbResult<bool> {
        if self.branches.get(namespace, name).is_none() {
            return Ok(false);
        }
        if self.branches.has_children(namespace, name) {
            return Err(AdbError::QueryError(format!("branch {name:?} has branches of its own: drop them first")));
        }
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(crate::branches::Branch::id_of(namespace, name)) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.branches.apply(&record);
        self.to_store(&record, seq.log()).await;
        Ok(true)
    }

    pub fn branches(&self) -> &Arc<crate::branches::Branches> {
        &self.branches
    }

    /// Make this a branch engine's writes (B3): reads for targets and key checks go
    /// through the overlay; a parent record changed or deleted gets a row in this table.
    /// What compaction keeps for open branches (B4); set once when the engine opens.
    pub fn set_pins(&self, pins: Pins) {
        let _ = self.pins.set(pins);
    }

    /// B5 (promote / clone): write `rows` — a space's records as a view sees them now —
    /// into `namespace` as new records (version 1, no history), through this engine's
    /// sequencer in batched records. Ids and a FACT's ends move to the new namespace
    /// (NS ids); links and control rows are not copied. `namespace` must hold no
    /// records yet. Returns how many records were written.
    pub async fn materialise(&self, namespace: &str, rows: Vec<adb_store::StoredRow>) -> AdbResult<u64> {
        if self.parent.is_some() {
            return Err(AdbError::QueryError("a new agent is written on the org's engine, not a branch's".into()));
        }
        if !adb_store::valid_namespace(namespace) {
            return Err(AdbError::QueryError(format!("namespace {namespace:?}: can't be empty or contain '|' or '~'")));
        }
        let Some(store) = &self.store else { return Err(needs_table("promote")) };
        let into = |id: &str| adb_store::namespaced(Some(namespace), adb_store::without_namespace(adb_store::key_of(id)));
        let mut rows: Vec<adb_store::StoredRow> = rows.into_iter().filter(|r| matches!(r.data.kind, Kind::Entity | Kind::Tool | Kind::Episode | Kind::Fact)).collect();
        // Entities before the facts about them.
        rows.sort_by_key(|r| (r.data.kind == Kind::Fact, r.commit_ts));
        let mut seq = self.seq.lock().await;
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let now = CommitTs::from_datetime(time::now());
        let taken = store
            .current(Some(&format!("namespace = {} AND {}", adb_store::sql_str(namespace), adb_store::RECORD_KINDS)), now)
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        if !taken.is_empty() {
            return Err(AdbError::QueryError(format!("namespace {namespace:?} already holds {} records: promote into a new one", taken.len())));
        }
        let mut n = 0u64;
        for chunk in rows.chunks(500) {
            let ts = seq.begin();
            let mut effects = Vec::with_capacity(chunk.len());
            let mut keys = Vec::new();
            for r in chunk {
                let id = RecordId(into(&r.lineage_id));
                let mut data = r.data.clone();
                data.namespace = Some(namespace.to_string());
                if data.kind == Kind::Fact {
                    data.subject_id = data.subject_id.as_deref().map(into);
                    data.object_id = data.object_id.as_deref().map(into);
                }
                let memory: MemoryType = data.memory.into();
                if unique_keys(memory) {
                    keys.push(((data.memory, id.0.clone()), data.ttl_at));
                }
                let new = NewRow { id: id.clone(), lineage_id: id, version: 1, data };
                effects.push(match new.data.kind {
                    Kind::Fact => Effect::PutFact(new),
                    Kind::Tool => Effect::PutTool(new),
                    Kind::Episode => Effect::PutEpisode(new),
                    _ => Effect::PutEntity(new),
                });
            }
            n += effects.len() as u64;
            let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
            self.live.lock().unwrap().extend(keys);
            self.to_store(&record, seq.log()).await;
        }
        Ok(n)
    }

    /// DB-19: apply merge `ops` to `namespace`'s main records in one log record, with
    /// `merge_row` appended to the append-only platform table [`MERGES`] in the same
    /// record. Each op names the main row it expects to replace (or none): if main moved
    /// since, nothing is written and the lineages that moved are returned as
    /// `merge_stale`. Returns the record's offset and, per op, the row it wrote (`after`).
    pub async fn merge_rows(&self, ops: Vec<MergeOp>, mut merge_row: serde_json::Map<String, serde_json::Value>) -> AdbResult<(u64, Vec<Option<String>>)> {
        if self.parent.is_some() {
            return Err(AdbError::QueryError("merges are written on the org's engine, not a branch's".into()));
        }
        let Some(store) = &self.store else { return Err(needs_table("merge")) };
        let platform = self.platform.as_ref().ok_or_else(|| needs_table("merge"))?;
        let mut seq = self.seq.lock().await;
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let ts = seq.begin();
        // Main as it is now, under the sequencer lock: nothing can move it until we log.
        let mut stale = Vec::new();
        for op in &ops {
            let now = store
                .current_version(op.memory.into(), adb_store::key_of(&op.lineage), ts)
                .await
                .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            if now.as_ref().map(|r| &r.id) != op.main.as_ref().map(|r| &r.id) {
                stale.push(op.lineage.clone());
            }
        }
        if !stale.is_empty() {
            return Err(AdbError::QueryError(format!("merge_stale: main changed {} since the conflicts were read: {}", stale.len(), stale.join(", "))));
        }
        let mut effects = Vec::new();
        let mut after = Vec::new();
        let mut keys = Vec::new();
        for op in &ops {
            match (&op.main, &op.value) {
                (Some(m), Some(v)) => {
                    let mut data = m.data.clone();
                    data.value = v.clone();
                    data.text = v.get("content").and_then(|c| c.as_str()).map(String::from);
                    data.embedding = None;
                    // The bare key, as UPDATE writes it (the store adds the memory prefix).
                    let new = NewRow { id: seq.new_id(ts), lineage_id: RecordId(adb_store::key_of(&m.lineage_id).to_string()), version: m.version + 1, data };
                    after.push(Some(new.id.0.clone()));
                    effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&m.id).to_string()), new });
                }
                (None, Some(v)) => {
                    let template = op.template.as_ref().ok_or_else(|| AdbError::Internal(format!("merge: no row to add for {}", op.lineage)))?;
                    let mut data = template.data.clone();
                    data.value = v.clone();
                    data.text = v.get("content").and_then(|c| c.as_str()).map(String::from);
                    data.embedding = None;
                    // A lineage that had versions (re-added by a revert) continues them.
                    let (id, version) = match op.prev_version {
                        Some(p) => (seq.new_id(ts), p + 1),
                        None => (RecordId(adb_store::key_of(&op.lineage).to_string()), 1),
                    };
                    let new = NewRow { id, lineage_id: RecordId(adb_store::key_of(&op.lineage).to_string()), version, data };
                    if unique_keys(op.memory) {
                        keys.push(((new.data.memory, new.lineage_id.0.clone()), new.data.ttl_at));
                    }
                    after.push(Some(new.id.0.clone()));
                    effects.push(match new.data.kind {
                        Kind::Fact => Effect::PutFact(new),
                        Kind::Tool => Effect::PutTool(new),
                        Kind::Episode => Effect::PutEpisode(new),
                        _ => Effect::PutEntity(new),
                    });
                }
                (Some(m), None) => {
                    after.push(None);
                    effects.push(Effect::Expire { memory: m.data.memory, lineage_id: RecordId(adb_store::key_of(&m.lineage_id).to_string()) });
                }
                (None, None) => after.push(None),
            }
        }
        let entries: Vec<serde_json::Value> = ops
            .iter()
            .zip(&after)
            .map(|(op, a)| serde_json::json!({ "memory": op.memory, "lineage": op.lineage, "before": op.main, "after": a, "template": op.main.is_none().then_some(op.template.clone()).flatten() }))
            .collect();
        merge_row.insert("entries".into(), serde_json::Value::String(serde_json::Value::Array(entries).to_string()));
        merge_row.insert("at".into(), serde_json::json!(time::now().to_rfc3339()));
        let table = vec![
            Effect::TableCreate { table: MERGES.into(), columns: MERGE_COLUMNS.iter().map(|c| c.to_string()).collect() },
            Effect::TableAppendOnly { table: MERGES.into() },
            Effect::TableInsert { table: MERGES.into(), rows: vec![merge_row] },
        ];
        // Applied here, in order: everything the platform applier holds goes first (PF-4).
        self.platform_settled(self.platform_last.load(std::sync::atomic::Ordering::SeqCst)).await;
        platform.validate_all(&table).await.map_err(|e| AdbError::QueryError(e.to_string()))?;
        effects.extend(table);
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.platform_last.store(record.offset.0, std::sync::atomic::Ordering::SeqCst);
        self.platform_applied.send_modify(|a| *a = (*a).max(record.offset.0));
        platform.apply(&record).await.map_err(|e| AdbError::QueryError(e.to_string()))?;
        self.live.lock().unwrap().extend(keys);
        self.to_store(&record, seq.log()).await;
        Ok((record.offset.0, after))
    }

    /// DB-11: on a branch engine, make `value` the branch's own row of `lineage` (a
    /// rebase's edited resolution): a new version of its own row, or its first own row
    /// over the parent's (`template`).
    pub async fn branch_set_row(&self, memory: MemoryType, lineage: &str, value: serde_json::Value, template: &adb_store::StoredRow) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Err(needs_table("rebase")) };
        let key = adb_store::key_of(lineage).to_string();
        let mut seq = self.seq.lock().await;
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let ts = seq.begin();
        let own = store.current_version(memory.into(), &key, ts).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        let mut data = own.as_ref().unwrap_or(template).data.clone();
        data.text = value.get("content").and_then(|c| c.as_str()).map(String::from);
        data.value = value;
        data.embedding = None;
        let version = own.as_ref().unwrap_or(template).version + 1;
        let new = NewRow { id: seq.new_id(ts), lineage_id: RecordId(key), version, data };
        let effect = match &own {
            Some(o) => Effect::Supersede { old: RecordId(adb_store::key_of(&o.id).to_string()), new },
            None => match new.data.kind {
                Kind::Fact => Effect::PutFact(new),
                Kind::Tool => Effect::PutTool(new),
                Kind::Episode => Effect::PutEpisode(new),
                _ => Effect::PutEntity(new),
            },
        };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok(record.offset.0)
    }

    /// The org's branch engines (B4): a FORGET on main erases their copies too.
    /// Lineages forgotten but not yet redacted from the log (shared with
    /// [`RedactingLog`]).
    pub(crate) fn forgotten_set(&self) -> Arc<std::sync::Mutex<std::collections::HashSet<(Memory, String)>>> {
        Arc::clone(&self.forgotten)
    }

    /// At open: the FORGETs still in the log, so readers keep hiding what they took and
    /// the next compaction redacts it (the set lives in memory). Returns how many.
    pub async fn seed_forgotten(&self) -> AdbResult<usize> {
        let mut gone = Vec::new();
        {
            let seq = self.seq.lock().await;
            let log = seq.log();
            let mut from = LogOffset(1);
            loop {
                let batch = match log.read(from, 1024).await {
                    Ok(b) => b,
                    Err(LogError::Purged { oldest, .. }) if oldest.0 > from.0 => {
                        from = oldest;
                        continue;
                    }
                    Err(LogError::Purged { .. }) => break,
                    Err(e) => return Err(log_err(e)),
                };
                let Some(last) = batch.last().map(|r| r.offset) else { break };
                for r in &batch {
                    for e in &r.effects {
                        if let Effect::Forget { memory, lineage_id } = e {
                            gone.push((*memory, lineage_id.0.clone()));
                        }
                    }
                }
                from = last.next();
            }
        }
        let n = gone.len();
        if n > 0 {
            self.note_forgotten(gone);
        }
        Ok(n)
    }

    pub fn set_branch_host(&self, host: Arc<crate::branch_engine::BranchHost>) {
        let _ = self.branch_host.set(host);
    }

    /// B4: erase `lineages` (keys of `memory`) from this engine's own table, log and
    /// history — a branch engine's copies of records FORGET removed on main. Only the
    /// lineages it holds rows of (or edges to) are written. Returns how many.
    pub async fn forget_lineages(&self, memory: MemoryType, lineages: &[String]) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Ok(0) };
        if lineages.is_empty() {
            return Ok(0);
        }
        let m = Memory::from(memory);
        let by_row: std::collections::HashMap<String, &String> = lineages.iter().map(|k| (adb_store::row_id(m, k), k)).collect();
        let mut seq = self.seq.lock().await;
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let mut held = std::collections::BTreeSet::new();
        let quoted: Vec<String> = by_row.keys().map(|r| adb_store::sql_str(r)).collect();
        for chunk in quoted.chunks(500) {
            let set = chunk.join(", ");
            let filter = format!("lineage_id IN ({set}) OR subject_id IN ({set}) OR object_id IN ({set})");
            for r in store.scan(Some(&filter)).await.map_err(|e| AdbError::Internal(format!("store: {e}")))? {
                for id in [Some(&r.lineage_id), r.data.subject_id.as_ref(), r.data.object_id.as_ref()].into_iter().flatten() {
                    if let Some(k) = by_row.get(id) {
                        held.insert((*k).clone());
                    }
                }
            }
        }
        if held.is_empty() {
            return Ok(0);
        }
        let ts = seq.begin();
        let effects: Vec<Effect> = held.iter().map(|k| Effect::Forget { memory: m, lineage_id: RecordId(k.clone()) }).collect();
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        self.note_forgotten(held.iter().map(|k| (m, k.clone())));
        self.to_store(&record, seq.log()).await;
        drop(seq);
        Ok(held.len() as u64)
    }

    pub fn set_parent(&mut self, parent: Option<crate::branch_engine::ParentView>) {
        self.parent = parent;
    }

    /// On a branch engine: the branch's view of `memory` matching `predicate`, each row
    /// with `true` when it is in this (the branch's) table.
    async fn branch_targets(&self, memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>, now: CommitTs) -> AdbResult<Vec<(adb_store::StoredRow, bool)>> {
        let (Some(store), Some(parent)) = (&self.store, &self.parent) else { return Ok(Vec::new()) };
        let rows = crate::branch_engine::view(store, &self.barrier(), parent, memory, now).await?;
        Ok(rows
            .into_iter()
            .filter(|(r, _)| in_scope.is_none_or(|s| r.data.scope == s))
            .filter(|(r, _)| crate::query::matches(memory, &crate::query::record(r), predicate))
            .collect())
    }

    /// UPDATE / FORGET on a branch engine (B3), under the sequencer lock: the branch's
    /// own row gets a new version; a parent's record gets an override row here in its
    /// lineage (the parent untouched). FORGET writes `_branch_deleted` the same way.
    #[allow(clippy::too_many_arguments)]
    async fn branch_write(
        &self,
        seq: &mut Sequencer<L>,
        ts: CommitTs,
        memory: MemoryType,
        predicate: &Predicate,
        in_scope: Option<&str>,
        fields: &serde_json::Map<String, serde_json::Value>,
        vector: Option<Vec<half::f16>>,
    ) -> AdbResult<u64> {
        let rows = self.branch_targets(memory, predicate, in_scope, ts).await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let mut effects = Vec::with_capacity(rows.len());
        for (cur, mine) in &rows {
            let mut new = next_version(cur, fields, seq.new_id(ts));
            if vector.is_some() {
                new.data.embedding = vector.clone();
            }
            effects.push(match (mine, memory) {
                (true, _) => Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new },
                (false, MemoryType::Episodic) => Effect::PutEpisode(new),
                (false, MemoryType::Tools) => Effect::PutTool(new),
                (false, _) => Effect::PutEntity(new),
            });
        }
        let n = effects.len() as u64;
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store(&record, seq.log()).await;
        Ok(n)
    }

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
        let erase = self.forgotten_since.lock().unwrap().is_some_and(|t| t.elapsed() >= ERASE_WITHIN);
        match &self.store {
            Some(s) => erase || compaction_due_for(s).await,
            None => false,
        }
    }

    /// Lineages a FORGET (or MERGE) just hid: their bytes go at a later compaction.
    fn note_forgotten(&self, gone: impl IntoIterator<Item = (Memory, String)>) {
        self.forgotten.lock().unwrap().extend(gone);
        self.forgotten_since.lock().unwrap().get_or_insert_with(std::time::Instant::now);
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
        let mut aliases = self.aliases.lock().unwrap();
        for e in effects {
            if let Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } = e {
                live.remove(&(*memory, lineage_id.0.clone()));
                // A forgotten entity's aliases go with it.
                if *memory == Memory::Semantic && !aliases.is_empty() {
                    aliases.retain(|_, to| *to != lineage_id.0);
                }
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
        // Schema v4 (E-L12): rows an older ADB wrote in a named scope get ids that
        // carry it, now the tail it may have written is applied.
        if store.schema_version() < 4 {
            let n = store.scope_row_ids().await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            tracing::info!(rows = n, "schema v4: record ids in named scopes carry the scope");
        }
        // Schema v5 (NS): record ids carry their namespace.
        if store.schema_version() < 5 {
            let n = store.namespace_row_ids().await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            tracing::info!(rows = n, "schema v5: record ids in a namespace carry the namespace");
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
        // CA-27: a retried txn_id commits once across a restart or a failover too.
        let window = std::env::var("ADB_TXN_DEDUPE_WINDOW").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000u64);
        let t_recall = std::time::Instant::now();
        let mut steps: Vec<(&str, std::time::Duration)> = Vec::new();
        let recalled = Box::pin(seq.recall_txns(window)).await.map_err(log_err)?;
        steps.push(("txn recall", t_recall.elapsed()));
        if recalled > 0 {
            tracing::info!(recalled, window, took = ?t_recall.elapsed(), "transaction ids recalled from the log tail");
        }
        // Caught up with the log: the applier starts from its end.
        let applier = Some(Applier::spawn(Arc::clone(&store), ApplyBarrier::new(seq.log().last_offset().0)));
        let t_step = std::time::Instant::now();
        let subs = Registry::load(&store, CommitTs::from_datetime(time::now())).await.map_err(AdbError::Internal)?;
        steps.push(("subscriptions", t_step.elapsed()));
        let t_step = std::time::Instant::now();
        let branches = crate::branches::Branches::load(&store, CommitTs::from_datetime(time::now())).await.map_err(AdbError::Internal)?;
        steps.push(("branches", t_step.elapsed()));
        let t_step = std::time::Instant::now();
        let timers = store
            .with_ttl()
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?
            .into_iter()
            .filter_map(|r| r.data.ttl_at.map(|t| (t.0, r.lineage_id)))
            .collect();
        steps.push(("ttl timers", t_step.elapsed()));
        let t_step = std::time::Instant::now();
        // Bounds after the last commit: one that passed while ADB was down has no
        // event yet, and gets it on the first tick. Only facts with a bound are read
        // (PF-38: every live fact was decoded here, ~1.6M rows at a takeover).
        let after = stats.last_commit_ts.map_or(i64::MIN, |t| t.0);
        let bounded = datafusion::prelude::col("kind")
            .eq(datafusion::prelude::lit("fact"))
            .and(adb_store::filter::field_text("valid_from").is_not_null().or(adb_store::filter::field_text("valid_to").is_not_null()));
        let bounds = store
            .live_expr(Some(bounded), CommitTs::from_datetime(time::now()))
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?
            .into_iter()
            .flat_map(|r| validity_bounds(&r.data.value).into_iter().filter(|t| *t > after).map(move |t| (t, r.lineage_id.clone())))
            .collect();
        steps.push(("validity bounds", t_step.elapsed()));
        let t_step = std::time::Instant::now();
        let aliases = load_aliases(&store).await?;
        steps.push(("aliases", t_step.elapsed()));
        tracing::info!(steps = ?steps, "write path opened");
        let store = Some(store);
        Ok((Self { seq: Mutex::new(seq), store, applier, subs: Arc::new(subs), branches: Arc::new(branches), parent: None, platform: None, live: std::sync::Mutex::new(live), embedder: None, forgotten_since: Default::default(), compactor: Mutex::new(()), usage: Mutex::new(None), platform_last: Default::default(), platform_queue: Default::default(), platform_tickets: Default::default(), platform_applied: Arc::new(tokio::sync::watch::Sender::new(0)), platform_reshape: Default::default(), key_offsets: Default::default(), poisoned: Default::default(), last_off: Default::default(), last_ts: Default::default(), forgotten: Default::default(), pins: Default::default(), branch_host: Default::default(), retain_max: retain_max(), sub_idle: sub_idle(), timers: std::sync::Mutex::new(timers), bounds: std::sync::Mutex::new(bounds), aliases: std::sync::Mutex::new(aliases), engine: None, engine_subs: None, engine_tickets: Default::default(), engine_submitted: Default::default(), engine_committed: Arc::new(tokio::sync::watch::Sender::new(0)) }, stats))
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
    async fn targets(&self, memory: MemoryType, predicate: &Predicate, ts: CommitTs, forget: bool, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<Vec<String>> {
        check_scope(memory, in_scope)?;
        let Some(store) = &self.store else { return Err(needs_table("UPDATE / FORGET")) };
        // Every earlier record is in Lance first.
        let t0 = std::time::Instant::now();
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let waited = t0.elapsed();
        let mut ids = crate::query::targets(store, memory, predicate, in_scope, namespace, ts).await?;
        let found = t0.elapsed();
        if forget && supersedes(memory) {
            // Every version FORGET takes. A key never changes between versions, so when
            // the predicate is a key (KEY, or `WHERE key =` / `WHERE id =`), only the
            // lineages just found can hold it: their versions, by primary key (PF-11,
            // C-25). Else every version, filtered here (a value may live only in an
            // older version).
            let by_key = crate::query::keyed(memory, predicate, in_scope, namespace).is_some() || key_only(predicate);
            let old = if by_key {
                let lineages: Vec<String> = ids.iter().map(|id| adb_store::row_id(memory.into(), id)).collect();
                store.versions_of(memory.into(), &lineages).await
            } else {
                store.versions(memory.into(), None).await
            };
            let mut old = old.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
            if let Some(s) = in_scope {
                old.retain(|r| r.data.scope == s);
            }
            if let Some(ns) = namespace {
                old.retain(|r| r.data.namespace.as_deref() == Some(ns));
            }
            for r in &old {
                if crate::query::matches(memory, &crate::query::record(r), predicate) {
                    ids.push(r.key().to_string());
                }
            }
            let mut seen = std::collections::HashSet::new();
            ids.retain(|id| seen.insert(id.clone()));
        }
        // Where an UPDATE's / FORGET's time under the lock goes (`RUST_LOG=adb::write=debug`).
        tracing::debug!(
            target: "adb::write", ?memory, forget, targets = ids.len(),
            apply_wait_us = waited.as_micros() as u64,
            find_us = (found - waited).as_micros() as u64,
            versions_us = (t0.elapsed() - found).as_micros() as u64,
            "targets"
        );
        Ok(ids)
    }

    /// GR-13: the facts about the entities among `ids` (as subject or object), every
    /// version, not already in `ids`: forgetting an entity forgets them too, in the
    /// same record. With `in_scope`, only facts of that scope.
    async fn entity_facts(&self, ids: &[String], in_scope: Option<&str>) -> AdbResult<Vec<String>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        let err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
        let in_list = |keys: &[String]| keys.iter().map(|k| adb_store::sql_str(&adb_store::row_id(Memory::Semantic, k))).collect::<Vec<_>>().join(", ");
        let entities = store.versions(Memory::Semantic, Some(&format!("kind = 'entity' AND lineage_id IN ({})", in_list(ids)))).await.map_err(err)?;
        if entities.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = entities.iter().map(|r| r.key().to_string()).collect();
        let ends = in_list(&keys);
        let mut filter = format!("kind = 'fact' AND (subject_id IN ({ends}) OR object_id IN ({ends}))");
        if let Some(s) = in_scope {
            filter.push_str(&format!(" AND scope = {}", adb_store::sql_str(s)));
        }
        let mut out: Vec<String> = Vec::new();
        for r in store.versions(Memory::Semantic, Some(&filter)).await.map_err(err)? {
            let key = r.key().to_string();
            if !ids.contains(&key) && !out.contains(&key) {
                out.push(key);
            }
        }
        Ok(out)
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
            for r in platform.apply_batch(&batch).await {
                if r.map_err(|e| AdbError::Internal(format!("platform: {e}")))? {
                    applied += 1;
                }
            }
            from = last.next();
        }
        platform.flush().await.map_err(|e| AdbError::Internal(format!("platform: {e}")))?;
        self.platform_applied.send_replace(seq.log().last_offset().0);
        drop(seq);
        *self.platform_tickets.lock().unwrap() = Some(spawn_platform_applier(Arc::clone(&platform), Arc::clone(&self.platform_applied)));
        self.platform = Some(platform);
        Ok(applied)
    }

    /// The read barrier for Lance reads (stage D3.7), if there is a table.
    /// Attach FlowR's engine store (stage H): replay the log above its
    /// snapshots and journal partitions, then give every lease one fresh period.
    /// `seen`: the first record a warm host (a follower's, C-19) hasn't applied; the
    /// replay starts there instead of at the snapshots.
    pub async fn attach_engine(&mut self, mut host: adb_engine::EngineHost, seen: Option<LogOffset>) -> AdbResult<u64> {
        let eng = |e: adb_engine::EngineError| AdbError::Internal(e.to_string());
        let seq = self.seq.lock().await;
        let mut from = host.replay_from().await.map_err(eng)?;
        if let Some(seen) = seen {
            from = LogOffset(from.0.max(seen.0));
        }
        let mut replayed = 0;
        loop {
            let batch = match seq.log().read(from, 1024).await {
                Ok(b) => b,
                // The log keeps every record the engine needs (`floor`): a purged
                // stretch held none.
                Err(LogError::Purged { oldest, .. }) if oldest.0 > from.0 => {
                    from = oldest;
                    continue;
                }
                Err(LogError::Purged { .. }) => break,
                Err(e) => return Err(log_err(e)),
            };
            let Some(last) = batch.last().map(|r| r.offset) else { break };
            for r in &batch {
                if r.effects.iter().any(|e| matches!(e, Effect::Engine { .. })) {
                    host.replay(r).await.map_err(eng)?;
                    replayed += 1;
                }
            }
            from = last.next();
        }
        host.replayed(chrono::Utc::now());
        drop(seq);
        self.engine_subs = Some(host.subscribers());
        let host = Arc::new(Mutex::new(host));
        *self.engine_tickets.lock().unwrap() = Some(spawn_engine_committer(Arc::clone(&host), Arc::clone(&self.engine_committed)));
        self.engine = Some(host);
        Ok(replayed)
    }

    fn engine_host(&self) -> AdbResult<&Arc<Mutex<adb_engine::EngineHost>>> {
        self.engine.as_ref().ok_or_else(|| AdbError::UnsupportedOperation("FlowR's engine store needs a durable engine (start ADB with --data-dir)".into()))
    }

    /// Run one engine operation on `space` (stage H): decided on the space's
    /// core under the sequencer, its durable changes logged as one record, then
    /// its journal episodes written and its notices delivered.
    pub async fn engine_exec<T>(&self, space: &str, f: impl FnOnce(&mut flowr_storage::engine_core::EngineCore) -> T) -> AdbResult<T> {
        let eng = |e: adb_engine::EngineError| AdbError::QueryError(e.to_string());
        let host = self.engine_host()?;
        self.writable()?;
        let t0 = std::time::Instant::now();
        let mut seq = self.seq.lock().await;
        let mut h = host.lock().await;
        let locked = t0.elapsed();
        let (out, effect, notices) = h.exec(space, f).await.map_err(eng)?;
        let (done, done_rx) = tokio::sync::oneshot::channel();
        // Pipelined (as memory and platform writes, PF-1 / PF-4): under the sequencer
        // only the core's change and the submission; the commit is awaited after it,
        // and the committer writes the journal and delivers notices in log order.
        let mut pending = None;
        let ticket = match effect {
            Some(effect) => {
                let ts = seq.begin();
                match seq.submit_record(ts, None, vec![effect]).await {
                    Ok((record, wait)) => {
                        drop(h);
                        self.engine_submitted.fetch_max(record.offset.0, std::sync::atomic::Ordering::SeqCst);
                        self.to_store_submitted(&record, seq.log()).await;
                        let (go, go_rx) = tokio::sync::oneshot::channel();
                        pending = Some((record.offset.0, wait, go));
                        EngineTicket { space: space.to_string(), record: Some(record), go: Some(go_rx), notices, done }
                    }
                    Err(e) => {
                        // The core holds changes the log never got: rebuild the space
                        // from its snapshot and the log.
                        tracing::error!(space, error = %e, "engine record not logged; rebuilding the space");
                        if let Err(re) = Self::rebuild_space(&mut h, space, seq.log()).await {
                            tracing::error!(space, error = %re, "engine space not rebuilt");
                        }
                        return Err(log_err(e));
                    }
                }
            }
            None => {
                drop(h);
                EngineTicket { space: space.to_string(), record: None, go: None, notices, done }
            }
        };
        let sent = self.engine_tickets.lock().unwrap().as_ref().map(|tx| tx.send(ticket).is_ok()).unwrap_or(false);
        drop(seq);
        let submitted = t0.elapsed();
        if let Some((off, wait, go)) = pending {
            let ok = self.committed(off, wait).await.is_ok();
            let _ = go.send(ok);
        }
        if !sent {
            return Err(AdbError::Internal("the engine committer is not running".into()));
        }
        done_rx.await.unwrap_or_else(|_| Err(AdbError::Internal("engine write dropped".into())))?;
        // Where an engine write's time goes (`RUST_LOG=adb::engine=debug`).
        tracing::debug!(
            target: "adb::engine",
            lock_us = locked.as_micros() as u64,
            submit_us = (submitted - locked).as_micros() as u64,
            commit_us = (t0.elapsed() - submitted).as_micros() as u64,
            "engine write"
        );
        Ok(out)
    }

    /// Wait until every engine record submitted before now has committed and its
    /// journal and notices are out: a read never sees a change that may not commit.
    async fn engine_settled(&self) {
        let want = self.engine_submitted.load(std::sync::atomic::Ordering::SeqCst);
        if want == 0 {
            return;
        }
        let mut rx = self.engine_committed.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(30), rx.wait_for(|c| *c >= want)).await;
    }

    async fn rebuild_space(h: &mut adb_engine::EngineHost, space: &str, log: &L) -> AdbResult<()> {
        let eng = |e: adb_engine::EngineError| AdbError::Internal(e.to_string());
        h.reopen(space).await.map_err(eng)?;
        let mut from = h.replay_from().await.map_err(eng)?;
        loop {
            let batch = log.read(from, 1024).await.map_err(log_err)?;
            let Some(last) = batch.last().map(|r| r.offset) else { break };
            for r in batch.iter().filter(|r| r.effects.iter().any(|e| matches!(e, Effect::Engine { space: s, .. } if s == space))) {
                h.replay(r).await.map_err(eng)?;
            }
            from = last.next();
        }
        Ok(())
    }

    /// Read `space`'s engine state (no log).
    pub async fn engine_read<T>(&self, space: &str, f: impl FnOnce(&flowr_storage::engine_core::EngineCore) -> T) -> AdbResult<T> {
        let eng = |e: adb_engine::EngineError| AdbError::QueryError(e.to_string());
        self.engine_settled().await;
        self.engine_host()?.lock().await.read(space, f).await.map_err(eng)
    }

    /// A run's journal after `after_seq`: from memory while the core holds it,
    /// else its EPISODIC history.
    pub async fn engine_journal(&self, space: &str, run: flowr_storage::engine::RunId, after_seq: u64) -> AdbResult<Vec<flowr_storage::engine::JournalEntry>> {
        let eng = |e: adb_engine::EngineError| AdbError::QueryError(e.to_string());
        self.engine_settled().await;
        let mut h = self.engine_host()?.lock().await;
        match h.read(space, |c| c.read_from(run, after_seq)).await.map_err(eng)? {
            Some(entries) => Ok(entries),
            None => h.archived(space, run, after_seq).await.map_err(eng),
        }
    }

    /// The engine's subscribers (for `subscribe`, which can't wait on a lock).
    pub fn engine_subscribers(&self) -> Option<Arc<adb_engine::Subs>> {
        self.engine_subs.clone()
    }

    /// Publish a notice in `space` (FlowR's `Notifier::publish`).
    pub async fn engine_notify(&self, space: &str, topic: &str, msg: serde_json::Value) -> AdbResult<()> {
        self.engine_host()?.lock().await.notify(space, vec![(topic.to_string(), msg)]);
        Ok(())
    }

    /// Engine upkeep (each tick): finished runs' journals leave memory, and
    /// snapshots that are due are written.
    pub async fn engine_maintain(&self) -> AdbResult<()> {
        let Some(host) = &self.engine else { return Ok(()) };
        let eng = |e: adb_engine::EngineError| AdbError::Internal(e.to_string());
        let spaces = host.lock().await.spaces();
        let now = chrono::Utc::now();
        for space in spaces {
            let runs = host.lock().await.evictable(&space, now).await.map_err(eng)?;
            if !runs.is_empty() {
                self.engine_exec(&space, |c| runs.iter().for_each(|r| c.evict_journal(*r))).await?;
            }
        }
        host.lock().await.snapshot_due(false).await.map_err(eng)?;
        let due = host.lock().await.journals_to_compact().await;
        for j in due {
            match j.run().await {
                Ok(st) => tracing::info!(space = %j.space, kept = st.kept, total = st.total, bytes_before = st.bytes_before, bytes_after = st.bytes_after, took = ?st.took, "engine journal compacted"),
                Err(e) => tracing::warn!(space = %j.space, error = %e, "engine journal compaction failed"),
            }
        }
        Ok(())
    }

    /// Compact every engine space's journal now (tests, admin).
    pub async fn engine_compact_journals(&self) -> AdbResult<Vec<(String, adb_store::CompactStats)>> {
        let Some(host) = &self.engine else { return Ok(Vec::new()) };
        let all = host.lock().await.journals();
        let mut out = Vec::new();
        for j in all {
            out.push((j.space.clone(), j.run().await.map_err(|e| AdbError::Internal(e.to_string()))?));
        }
        Ok(out)
    }

    /// Snapshot every engine space that changed since its last snapshot.
    pub async fn engine_snapshot_now(&self) -> AdbResult<usize> {
        let Some(host) = &self.engine else { return Ok(0) };
        host.lock().await.snapshot_due(true).await.map_err(|e| AdbError::Internal(e.to_string()))
    }

    /// The spaces this engine holds.
    pub async fn engine_spaces(&self) -> Vec<String> {
        match &self.engine {
            Some(h) => h.lock().await.spaces(),
            None => Vec::new(),
        }
    }

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
        self.to_store_with(record, log, true).await
    }

    /// As [`Self::to_store`] for a record submitted but maybe not yet durable (PF-1):
    /// the applier holds it until [`Self::committed`] marks it.
    async fn to_store_submitted(&self, record: &LogRecord, log: &L) {
        self.to_store_with(record, log, false).await
    }

    /// Wait for a submitted record's commit, outside the sequencer. A failure stops
    /// this engine's writes and asks for a restart (the leader lost the replica set).
    async fn committed(&self, offset: u64, wait: adb_log::CommitWait) -> AdbResult<()> {
        match wait.await {
            Ok(_) => {
                if let Some(b) = self.barrier() {
                    b.durable(offset);
                }
                Ok(())
            }
            Err(e) => {
                self.poisoned.store(true, std::sync::atomic::Ordering::SeqCst);
                if let Some(b) = self.barrier() {
                    b.abandon();
                }
                tracing::error!(offset, error = %e, "a submitted record did not commit: this engine stops taking writes and restarts");
                crate::engine::request_restart();
                Err(log_err(e))
            }
        }
    }

    fn writable(&self) -> AdbResult<()> {
        if self.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AdbError::Internal("this ADB engine is restarting after a write failed to commit; retry".into()));
        }
        Ok(())
    }

    async fn to_store_with(&self, record: &LogRecord, log: &L, durable: bool) {
        let applied = self.barrier().map_or(0, |b| b.applied_offset());
        self.key_offsets.lock().unwrap().note(record, applied);
        self.last_off.fetch_max(record.offset.0, std::sync::atomic::Ordering::SeqCst);
        self.last_ts.fetch_max(record.commit_ts.0, std::sync::atomic::Ordering::SeqCst);
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
        if durable {
            applier.enqueue(record.clone());
        } else {
            applier.enqueue_submitted(record.clone());
        }
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
                    // FlowR's engine needs the records above its snapshots (stage H).
                    if let Some(e) = &self.engine {
                        if let Some(f) = e.lock().await.floor().await {
                            floor = LogOffset(floor.0.min(f.0));
                        }
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
        // A branch engine's records belong to its namespace (B3).
        let namespace = match &self.parent {
            Some(p) => match namespace {
                Some(ns) if ns != p.namespace => return Err(AdbError::QueryError(format!("branch {:?} belongs to namespace {:?}, not {ns:?}", p.branch, p.namespace))),
                _ => Some(p.namespace.as_str()),
            },
            None => namespace,
        };
        if self.parent.is_none() {
            let bare = scoped_key(None, in_scope, &store_id(memory_type, key, &payload))?;
            self.check_sealed(memory_type, namespace, &bare, None, CommitTs::from_datetime(time::now())).await?;
        }
        let bytes = payload.to_string().len() as u64;
        self.writable()?;
        let mut seq = self.seq.lock().await;
        self.check_quota(namespace, bytes).await?;
        let ts = seq.begin();
        let id = RecordId(scoped_key(namespace, in_scope, &store_id(memory_type, key, &payload))?);
        if self.parent.is_some() && caller_keyed(memory_type, &payload) {
            let seen = self.branch_targets(memory_type, &Predicate::Where { conditions: Vec::new() }, None, ts).await?;
            if seen.iter().any(|(r, _)| adb_store::key_of(&r.lineage_id) == id.0) {
                return Err(AdbError::QueryError(format!("{} exists on this branch (from it or its parent): UPDATE it to change it on the branch", id.0)));
            }
        }
        if memory_type == MemoryType::Semantic {
            if let Some(to) = self.alias_of(&id.0) {
                return Err(AdbError::QueryError(format!("{} was merged into {to}: store into {to}", id.0)));
            }
        }
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
        // Submitted in order under the sequencer, committed after it is released (PF-1).
        let (record, wait) = seq.submit_record(ts, None, vec![effect.clone()]).await.map_err(log_err)?;
        if unique_keys(memory_type) && self.store.is_some() {
            self.live.lock().unwrap().insert((memory_type.into(), new.lineage_id.0.clone()), new.data.ttl_at);
        }
        // The table is the only copy: answer with the record as reads shape it.
        let stored = crate::query::record(&adb_store::StoredRow::from_new(&adb_store::qualified(&new), ts, 0, 0));
        self.to_store_submitted(&record, seq.log()).await;
        // Counted under the sequencer, so concurrent STOREs can't both pass the quota.
        self.note_stored(namespace, bytes).await;
        drop(seq);
        self.committed(record.offset.0, wait).await?;
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
        // The ends' record ids in the fact's scope (E-L12); a merged entity's key
        // names the entity it merged into (GR-12).
        let subject_id = scoped_key(namespace, in_scope, &subject)?;
        let subject_id = self.alias_of(&subject_id).unwrap_or(subject_id);
        let object_id = field("object").map(|o| scoped_key(namespace, in_scope, &o)).transpose()?;
        let object_id = object_id.map(|o| self.alias_of(&o).unwrap_or(o));
        let subject = adb_store::unscoped(&subject_id).to_string();
        let object = object_id.as_deref().map(|o| adb_store::unscoped(o).to_string());
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("subject".into(), serde_json::json!(subject));
            if let Some(o) = &object {
                obj.insert("object".into(), serde_json::json!(o));
            }
        }
        let value = payload.get("value").cloned();
        let shown = match (&object, &value) {
            (Some(o), _) => o.clone(),
            (None, Some(v)) => v.as_str().map(String::from).unwrap_or_else(|| v.to_string()),
            (None, None) => return Err(AdbError::InvalidSyntax("FACT needs object or value".into())),
        };
        let bare_key = fact_key(&subject, &predicate, multi.then_some(shown.as_str()));
        if self.parent.is_none() {
            let fk = adb_store::scoped_id(in_scope.unwrap_or(adb_store::DEFAULT_SCOPE), &bare_key);
            self.check_sealed(memory, namespace, &fk, Some((subject.as_str(), predicate.as_str())), CommitTs::from_datetime(time::now())).await?;
        }
        let key = adb_store::space_id(namespace, in_scope.unwrap_or(adb_store::DEFAULT_SCOPE), &bare_key);
        if let Some(obj) = payload.as_object_mut() {
            normalize_validity(obj, None)?;
            // On STORE a null bound is no bound.
            obj.retain(|k, v| !(matches!(k.as_str(), "valid_from" | "valid_to") && v.is_null()));
        }
        let confidence = payload.get("confidence").and_then(|v| v.as_f64()).unwrap_or(1.0) as f32;
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("key".into(), serde_json::json!(bare_key));
            obj.entry("content").or_insert_with(|| serde_json::json!(format!("{subject} {predicate} {shown}")));
        }
        let vector = match self.store.is_some() {
            true => self.vector_of(crate::reads::embed_text(memory, &payload)).await,
            false => None,
        };
        let fact_bytes = payload.to_string().len() as u64;
        let mut seq = self.seq.lock().await;
        self.check_quota(namespace, fact_bytes).await?;
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
        data.subject_id = Some(subject_id.clone());
        data.predicate = Some(predicate.clone());
        data.object_id = object_id.clone();
        data.link_type = Some(predicate);
        data.weight = Some(confidence);
        data.embedding = vector;
        data.ttl_at = ttl.map(|d| CommitTs(ts.0 + d.as_micros() as i64));
        let mut effects = Vec::new();
        // Entities at the fact's ends, created when missing (stubs: key and name).
        let mut entities: Vec<String> = Vec::new();
        for e in std::iter::once(&subject_id).chain(object_id.iter()) {
            if !self.key_taken(memory.into(), e, ts) && !entities.contains(e) {
                entities.push(e.clone());
                let id = RecordId(e.clone());
                let bare = adb_store::unscoped(e);
                let stub = shaped(Kind::Entity, serde_json::json!({ "key": bare, "name": bare }));
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
        self.note_stored(namespace, fact_bytes).await;
        Ok(stored)
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        // New `content` gets a new vector (T5), embedded before the sequencer.
        let vector = match crate::reads::lance_owned(memory_type) && self.store.is_some() {
            true => self.vector_of(data.get("content").and_then(|c| c.as_str()).map(String::from)).await,
            false => None,
        };
        self.writable()?;
        if self.parent.is_none() && self.store.is_some() && crate::query::keyed(memory_type, predicate, in_scope, namespace).is_some() {
            if let Some(n) = self.update_prepared(memory_type, predicate, &data, in_scope, namespace, vector.clone()).await? {
                return Ok(n);
            }
        }
        let t0 = std::time::Instant::now();
        let mut seq = self.seq.lock().await;
        tracing::debug!(target: "adb::write", ?memory_type, lock_us = t0.elapsed().as_micros() as u64, "update locked");
        let ts = seq.begin();
        if self.parent.is_some() {
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
            return self.branch_write(&mut seq, ts, memory_type, predicate, in_scope, &fields, vector).await;
        }
        let ids = self.targets(memory_type, predicate, ts, false, in_scope, namespace).await?;
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
            let resolved = t0.elapsed();
            let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
            self.to_store(&record, seq.log()).await;
            tracing::debug!(target: "adb::write", ?memory_type, held_us = resolved.as_micros() as u64, log_us = (t0.elapsed() - resolved).as_micros() as u64, "update committed");
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

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        self.writable()?;
        if self.parent.is_none() && self.store.is_some() && crate::query::keyed(memory_type, predicate, in_scope, namespace).is_some() {
            if let Some(n) = self.forget_prepared(memory_type, predicate, in_scope, namespace).await? {
                return Ok(n);
            }
        }
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        // On a branch engine (B3): a delete marker hides the record on the branch; the
        // parent keeps it (erasing data for good is FORGET on main).
        if self.parent.is_some() {
            let mut fields = serde_json::Map::new();
            fields.insert("_branch_deleted".into(), serde_json::Value::Bool(true));
            return self.branch_write(&mut seq, ts, memory_type, predicate, in_scope, &fields, None).await;
        }
        let mut ids = self.targets(memory_type, predicate, ts, true, in_scope, namespace).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        if memory_type == MemoryType::Semantic {
            ids.extend(self.entity_facts(&ids, in_scope).await?);
        }
        let effects: Vec<Effect> =
            ids.iter().map(|id| Effect::Forget { memory: memory_type.into(), lineage_id: RecordId(id.clone()) }).collect();
        self.usage_stale().await;
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        self.note_forgotten(ids.iter().map(|id| (Memory::from(memory_type), id.clone())));
        let removed = ids.len() as u64;
        self.to_store(&record, seq.log()).await;
        drop(seq);
        // Hidden from every read from the commit on; the bytes leave disk at the next
        // compaction (user, 7 Nov: no read may miss a FORGET; disk removal can wait).
        self.forget_on_branches(memory_type, &ids).await?;
        Ok(removed)
    }

    fn alias_of(&self, key: &str) -> Option<String> {
        let aliases = self.aliases.lock().unwrap();
        if aliases.is_empty() {
            return None;
        }
        aliases.get(key).cloned()
    }

    async fn merge(&self, from: &str, into: &str) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Err(needs_table("MERGE")) };
        if [from, into].iter().any(|k| k.contains(adb_store::SCOPE_SEP)) {
            return Err(AdbError::UnsupportedOperation("MERGE of entities in a named scope (E-L12-a): MERGE works in the default scope".into()));
        }
        let memory = Memory::Semantic;
        let err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        // Every earlier record is in Lance first.
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        if let Some(to) = self.alias_of(from) {
            return Err(AdbError::QueryError(format!("MERGE: {from} was already merged into {to}")));
        }
        let into = self.alias_of(into).unwrap_or_else(|| into.to_string());
        if from == into {
            return Err(AdbError::QueryError(format!("MERGE: {from} is {into}")));
        }
        let entity = |key: String| async move {
            match store.current_version(memory, &key, ts).await {
                Ok(Some(r)) if r.data.kind == Kind::Entity => Ok(r),
                Ok(_) => Err(AdbError::QueryError(format!("MERGE: no entity {key}"))),
                Err(e) => Err(err(e)),
            }
        };
        let (dup, survivor) = (entity(from.to_string()).await?, entity(into.clone()).await?);
        let end = adb_store::sql_str(&adb_store::row_id(memory, from));
        let about = |kind: &str| format!("kind = '{kind}' AND (subject_id = {end} OR object_id = {end})");

        // The duplicate's facts, written again about the survivor: of two facts for
        // one key, the newer is current (the older stays as history, user 30 Oct).
        let mut facts = store.live(Some(&about("fact")), ts).await.map_err(err)?;
        facts.sort_by_key(|r| r.commit_ts);
        let mut effects = Vec::new();
        // New key → the newest of the duplicate's facts that land on it. A fact whose
        // key doesn't change (the duplicate is its single-valued object) gets a new
        // version; the others move to a new key, and their old one is forgotten.
        let mut moved: Vec<(String, RowData, CommitTs)> = Vec::new();
        for f in &facts {
            let (key, d) = moved_fact(&f.data, f.key(), from, &into);
            if key != f.key() {
                effects.push(Effect::Forget { memory, lineage_id: RecordId(f.key().to_string()) });
            }
            moved.retain(|(k, ..)| *k != key);
            moved.push((key, d, f.commit_ts));
        }
        // Its links, moved to the survivor (the old ones go with the entity).
        let edges = store.live(Some(&about("edge")), ts).await.map_err(err)?;
        let theirs: std::collections::HashSet<(String, String, Option<String>)> = {
            let mine = adb_store::sql_str(&adb_store::row_id(memory, &into));
            let f = format!("kind = 'edge' AND (subject_id = {mine} OR object_id = {mine})");
            store.live(Some(&f), ts).await.map_err(err)?.into_iter().map(|e| (e.data.subject_id.unwrap_or_default(), e.data.object_id.unwrap_or_default(), e.data.link_type)).collect()
        };
        let mut new_edges = Vec::new();
        for e in &edges {
            let swap = |id: &Option<String>| match id.as_deref() {
                Some(i) if adb_store::sql_str(i) == end => into.clone(),
                Some(i) => adb_store::key_of(i).to_string(),
                None => String::new(),
            };
            let (s, o) = (swap(&e.data.subject_id), swap(&e.data.object_id));
            let mut d = e.data.clone();
            d.subject_id = Some(s);
            d.object_id = Some(o);
            let n = NewRow { id: seq.new_id(ts), lineage_id: RecordId(String::new()), version: 1, data: d };
            let q = adb_store::qualified(&n);
            if q.data.subject_id == q.data.object_id || theirs.contains(&(q.data.subject_id.clone().unwrap_or_default(), q.data.object_id.clone().unwrap_or_default(), q.data.link_type.clone())) {
                continue;
            }
            new_edges.push(NewRow { lineage_id: n.id.clone(), ..n });
        }
        // The survivor takes the duplicate's key (and aliases) as aliases, and the
        // fields it doesn't have.
        let mut aliases = aliases_of(&survivor.data.value);
        for a in std::iter::once(from.to_string()).chain(aliases_of(&dup.data.value)) {
            if !aliases.contains(&a) {
                aliases.push(a);
            }
        }
        let mut fields = serde_json::Map::new();
        if let (Some(d), Some(s)) = (dup.data.value.as_object(), survivor.data.value.as_object()) {
            for (k, v) in d {
                if !s.contains_key(k) && k != "aliases" {
                    fields.insert(k.clone(), v.clone());
                }
            }
        }
        fields.insert("aliases".into(), serde_json::json!(aliases));
        let survivor_next = next_version(&survivor, &fields, seq.new_id(ts));
        effects.push(Effect::Forget { memory, lineage_id: RecordId(from.to_string()) });
        effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&survivor.id).to_string()), new: survivor_next });

        let mut n = 0u64;
        let mut put_keys = Vec::new();
        for (key, d, at) in moved {
            let in_place = facts.iter().any(|f| f.key() == key);
            match store.current_version(memory, &key, ts).await.map_err(err)? {
                // The survivor's own fact is newer: it stays.
                Some(cur) if !in_place && cur.commit_ts >= at => {}
                Some(cur) => {
                    effects.push(Effect::Supersede {
                        old: RecordId(adb_store::key_of(&cur.id).to_string()),
                        new: NewRow { id: seq.new_id(ts), lineage_id: RecordId(key.clone()), version: cur.version + 1, data: d },
                    });
                    n += 1;
                }
                None => {
                    put_keys.push((key.clone(), d.ttl_at));
                    let id = RecordId(key);
                    effects.push(Effect::PutFact(NewRow { id: id.clone(), lineage_id: id, version: 1, data: d }));
                    n += 1;
                }
            }
        }
        n += new_edges.len() as u64;
        effects.extend(new_edges.into_iter().map(Effect::PutEdge));
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        {
            let mut live = self.live.lock().unwrap();
            for (k, ttl) in put_keys {
                live.insert((memory, k), ttl);
            }
            let mut a = self.aliases.lock().unwrap();
            for to in a.values_mut().filter(|to| to.as_str() == from) {
                *to = into.clone();
            }
            for alias in &aliases {
                a.insert(alias.clone(), into.clone());
            }
        }
        let gone = effects.iter().filter_map(|e| match e {
            Effect::Forget { memory, lineage_id } => Some((*memory, lineage_id.0.clone())),
            _ => None,
        });
        self.note_forgotten(gone);
        self.to_store(&record, seq.log()).await;
        drop(seq);
        Ok(n)
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
        self.platform_many(vec![effect]).await
    }

    /// Platform writes commit in groups: each joins a queue, and whoever holds the
    /// sequencer takes everything queued and commits it as one record, upserts to the
    /// same table merged into one effect, so one Lance commit (most of a write's time)
    /// serves the whole group. Each is still answered only once it is committed and
    /// applied; a group whose combined check fails is committed one write at a time,
    /// so only the bad write gets the error.
    async fn platform_many(&self, effects: Vec<Effect>) -> AdbResult<u64> {
        if self.platform.is_none() {
            return Err(AdbError::UnsupportedOperation("this engine has no platform tables".into()));
        }
        if effects.is_empty() {
            return Err(AdbError::QueryError("a transaction needs at least one operation".into()));
        }
        self.writable()?;
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        self.platform_queue.lock().unwrap().push(PlatformPending { effects, tx });
        loop {
            tokio::select! {
                // Committed by another write's holder.
                r = &mut rx => return r.unwrap_or_else(|_| Err(AdbError::Internal("platform write dropped".into()))),
                seq = self.seq.lock() => {
                    let mut seq = seq;
                    let batch: Vec<PlatformPending> = {
                        let mut q = self.platform_queue.lock().unwrap();
                        let n = q.len().min(PLATFORM_GROUP_MAX);
                        q.drain(..n).collect()
                    };
                    if batch.is_empty() {
                        // Ours went with an earlier holder's group: its answer comes.
                        drop(seq);
                        return rx.await.unwrap_or_else(|_| Err(AdbError::Internal("platform write dropped".into())));
                    }
                    let mut submitted = Vec::new();
                    self.platform_group(&mut seq, batch, &mut submitted).await;
                    drop(seq);
                    // PF-4: the commits are awaited outside the sequencer; the applier
                    // applies each once its commit is in.
                    for (off, wait, go) in submitted {
                        let ok = self.committed(off, wait).await.is_ok();
                        let _ = go.send(ok);
                    }
                }
            }
        }
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
    // A follower taking the lead kept them by every apply (PF-38): no scan.
    let records = match store.take_live_keys(now) {
        Some(keys) => {
            tracing::info!(keys = keys.len(), "live keys taken from the follower's index");
            keys
        }
        // Only the key columns (C-19): the full rows cost a takeover ~1.2 s at 160k rows.
        None => store.live_keys(now).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?,
    };
    let live: LiveKeys = records
        .iter()
        .filter(|r| unique_keys(r.memory.into()))
        .map(|r| ((r.memory, adb_store::key_of(&r.lineage_id).to_string()), r.ttl_at))
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

/// Memory types whose `UPDATE` writes a new version (D2 §2; DB-8 adds EPISODIC and TOOLS, so
/// "what the agent knew at 14:32" covers them). WORKING is session state and changes in place.
fn supersedes(memory: MemoryType) -> bool {
    !matches!(memory, MemoryType::Working)
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
/// An entity's `aliases` (GR-12): the keys of the entities merged into it.
/// Entity aliases (GR-12) from the live entities' `aliases` fields: alias → entity.
pub(crate) async fn load_aliases(store: &PartitionStore) -> AdbResult<std::collections::HashMap<String, String>> {
    Ok(store
        .live(Some("kind = 'entity'"), CommitTs::from_datetime(time::now()))
        .await
        .map_err(|e| AdbError::Internal(format!("store: {e}")))?
        .iter()
        .flat_map(|r| aliases_of(&r.data.value).into_iter().map(move |a| (a, r.key().to_string())))
        .collect())
}

fn aliases_of(value: &serde_json::Value) -> Vec<String> {
    value.get("aliases").and_then(|a| a.as_array()).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect()).unwrap_or_default()
}

/// `d`, a fact about entity `from` as the table holds it, as a fact about `into`:
/// bare ends with `from` replaced, its payload and default `content` to match.
/// Returns its key (one per (subject, predicate) unless the old key had the object).
fn moved_fact(d: &RowData, key: &str, from: &str, into: &str) -> (String, RowData) {
    let mut d = d.clone();
    let bare = |id: &Option<String>| id.as_deref().map(|i| adb_store::key_of(i).to_string());
    let swap = |id: Option<String>| id.map(|i| if i == from { into.to_string() } else { i });
    let (old_s, old_o) = (bare(&d.subject_id).unwrap_or_default(), bare(&d.object_id));
    let (subject, object) = (swap(Some(old_s.clone())).unwrap_or_default(), swap(old_o.clone()));
    let predicate = d.predicate.clone().unwrap_or_default();
    let shown = |o: &Option<String>| o.clone().unwrap_or_else(|| d.value.get("value").map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string())).unwrap_or_default());
    let multi = key != fact_key(&old_s, &predicate, None);
    let new_key = fact_key(&subject, &predicate, multi.then(|| shown(&object)).as_deref());
    let old_content = format!("{old_s} {predicate} {}", shown(&old_o));
    let new_content = format!("{subject} {predicate} {}", shown(&object));
    if let Some(v) = d.value.as_object_mut() {
        v.insert("subject".into(), serde_json::json!(subject));
        if let Some(o) = &object {
            v.insert("object".into(), serde_json::json!(o));
        }
        v.insert("key".into(), serde_json::json!(new_key));
        if v.get("content").and_then(|c| c.as_str()) == Some(old_content.as_str()) {
            v.insert("content".into(), serde_json::json!(new_content));
            d.text = Some(new_content);
            // Embedded again by the backfill.
            d.embedding = None;
        }
    }
    d.subject_id = Some(subject);
    d.object_id = object;
    (new_key, d)
}

pub fn fact_key(subject: &str, predicate: &str, object: Option<&str>) -> String {
    match object {
        Some(o) => format!("fact:{subject}|{predicate}|{o}"),
        None => format!("fact:{subject}|{predicate}"),
    }
}

/// The version after `cur` with `fields` merged in, under the new row id `id`.
fn next_version(cur: &adb_store::StoredRow, fields: &serde_json::Map<String, serde_json::Value>, id: RecordId) -> NewRow {
    let mut data = cur.data.clone();
    // A fact's ends as the log carries them: the stored row has them qualified, and
    // the store qualifies the new version again (GR-15 found them doubled).
    if data.kind == Kind::Fact {
        for e in [&mut data.subject_id, &mut data.object_id].into_iter().flatten() {
            *e = adb_store::key_of(e).to_string();
        }
    }
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


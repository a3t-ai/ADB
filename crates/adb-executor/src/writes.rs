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

#[path = "segments.rs"]
pub mod segments;
use crate::reads::check_scope;
use adb_gateway::{sub_value, Registry, SubSpec};
use async_trait::async_trait;
use tokio::sync::Mutex;

/// The partition every stage C write goes to (placement arrives with M3).
/// The sequencer, timed (`ADB_STEP_STATS`): the wait for it and how long it is held,
/// per call site (`seq.wait @<line>` / `seq.held @<line>`).
pub(crate) struct SeqGuard<'a, L: Log> {
    g: tokio::sync::MutexGuard<'a, Sequencer<L>>,
    held: &'static str,
    t: std::time::Instant,
    holder: &'a SeqHolder,
}

/// Who holds the sequencer now (its site) and since when: a write path that stalls
/// says what it is waiting behind ([`DurableWrites::sequencer_holder`]).
#[derive(Default)]
pub(crate) struct SeqHolder(std::sync::Mutex<Option<(&'static str, std::time::Instant)>>);

impl<L: Log> std::ops::Deref for SeqGuard<'_, L> {
    type Target = Sequencer<L>;
    fn deref(&self) -> &Sequencer<L> {
        &self.g
    }
}

impl<L: Log> std::ops::DerefMut for SeqGuard<'_, L> {
    fn deref_mut(&mut self) -> &mut Sequencer<L> {
        &mut self.g
    }
}

impl<L: Log> Drop for SeqGuard<'_, L> {
    fn drop(&mut self) {
        *self.holder.0.lock().unwrap() = None;
        crate::steps::add(self.held, self.t.elapsed());
        crate::steps::add("seq.held (all sites)", self.t.elapsed());
    }
}

async fn seq_timed<'a, L: Log>(m: &'a Mutex<Sequencer<L>>, holder: &'a SeqHolder, wait: &'static str, held: &'static str) -> SeqGuard<'a, L> {
    let t0 = std::time::Instant::now();
    let g = m.lock().await;
    crate::steps::add(wait, t0.elapsed());
    crate::steps::add("seq.wait (all sites)", t0.elapsed());
    let t = std::time::Instant::now();
    *holder.0.lock().unwrap() = Some((held, t));
    SeqGuard { g, held, t, holder }
}

macro_rules! seq_lock {
    ($self:ident) => {
        seq_timed(&$self.seq, &$self.seq_holder, concat!("seq.wait @", line!()), concat!("seq.held @", line!())).await
    };
}

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

    /// `LINK` whose target is in another partition (S3 option B step 3): as
    /// [`Self::link`], but only the source is checked here; the router checked the
    /// target in its own partition (a later FORGET of it leaves the link pointing at
    /// nothing, which following it skips).
    async fn link_remote(&self, _from_type: MemoryType, _from_id: &str, _to_type: MemoryType, _to_id: &str, _link_type: &str, _weight: Option<f32>) -> AdbResult<Link> {
        Err(AdbError::UnsupportedOperation("links across partitions need a durable engine".into()))
    }

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

    /// AQL v0.13: a record's `aliases` field was written — the alias map follows.
    async fn aliases_changed(&self) -> AdbResult<()> {
        Ok(())
    }

    /// The entity a merged entity's record id (its key, with its namespace) is now an
    /// alias of (GR-12).
    fn alias_of(&self, _key: &str) -> Option<String> {
        None
    }

    /// [`Self::alias_of`], where the answer may be on another node (a cluster's
    /// router asks each partition in the call's scope).
    async fn alias_resolve(&self, key: &str) -> AdbResult<Option<String>> {
        Ok(self.alias_of(key))
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

    /// The topic a subscription follows (`None`: unknown, or a scope subscription).
    async fn subscription_topic(&self, _subscription: &str) -> AdbResult<Option<String>> {
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
/// One key of an AQL v0.13 MERGE ([`DurableWrites::merge_apply`]).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergeWrite {
    pub memory: Memory,
    /// The record's key without its namespace (or a table id of it).
    pub key: String,
    /// The target namespace's own record of the key now, if it has one.
    pub current: Option<adb_store::StoredRow>,
    /// What the key becomes (the source's record), or `None`: removed.
    pub source: Option<adb_store::StoredRow>,
}

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

/// An UPDATE / FORGET that names no single record (no `IN SCOPE` key, no `WHERE id =`):
/// it runs under the sequencer, reading Lance and waiting for its commit there (the
/// slow path, ~15 ms held each on AWS). `ADB_UNSCOPED_WRITES=reject` refuses it;
/// the default runs it and counts it (`ADB_STEP_STATS`).
fn slow_write_allowed(memory: MemoryType, statement: &str) -> AdbResult<()> {
    static REJECT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *REJECT.get_or_init(|| std::env::var("ADB_UNSCOPED_WRITES").is_ok_and(|v| v.eq_ignore_ascii_case("reject"))) {
        return Err(AdbError::QueryError(format!(
            "{statement} INTO/FROM {memory:?} must name one record: IN SCOPE \"<scope>\" with WHERE key = …, or WHERE id = … (this ADB has ADB_UNSCOPED_WRITES=reject)"
        )));
    }
    crate::steps::add(if statement == "UPDATE" { "update.slow path (no single key)" } else { "forget.slow path (no single key)" }, std::time::Duration::ZERO);
    Ok(())
}

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
    fn floor_commit(&self) -> Option<(LogOffset, CommitTs)> {
        self.inner.floor_commit()
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
                Effect::Patch { memory, id, .. } => {
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
        Effect::Patch { memory, id, .. } => !hit(*memory, &id.0),
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

/// The deferred-vector queue: (memory, record id, text) in the order written. A
/// record written again before its vector came keeps only its newest text: an older
/// one is dropped when taken (its vector would describe text the record no longer
/// has, and could land after the newer one's).
#[derive(Default)]
pub(crate) struct EmbedQueue {
    items: std::collections::VecDeque<(Memory, String, String, u64)>,
    /// The newest generation queued per record.
    newest: std::collections::HashMap<(Memory, String), u64>,
    next: u64,
}

impl EmbedQueue {
    /// Queue a text; `false` when the queue is full.
    pub(crate) fn push(&mut self, memory: Memory, id: String, text: String) -> bool {
        if self.items.len() >= EMBED_SOON_MAX {
            return false;
        }
        self.next += 1;
        self.newest.insert((memory, id.clone()), self.next);
        self.items.push_back((memory, id, text, self.next));
        true
    }

    /// Up to `n` texts to embed, oldest first, without superseded ones.
    pub(crate) fn take(&mut self, n: usize) -> Vec<(Memory, String, String)> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let Some((memory, id, text, g)) = self.items.pop_front() else { break };
            let key = (memory, id);
            match self.newest.get(&key) {
                Some(&newest) if newest == g => {
                    self.newest.remove(&key);
                    out.push((key.0, key.1, text));
                }
                _ => crate::steps::value("embed.deferred superseded texts dropped", 1),
            }
        }
        out
    }

    /// Put a batch that couldn't be embedded back at the front, except records
    /// written again since (their newer text is queued).
    pub(crate) fn requeue(&mut self, batch: Vec<(Memory, String, String)>) {
        for (memory, id, text) in batch.into_iter().rev() {
            let key = (memory, id);
            if self.newest.contains_key(&key) {
                continue;
            }
            self.next += 1;
            self.newest.insert(key.clone(), self.next);
            self.items.push_front((key.0, key.1, text, self.next));
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.newest.clear();
    }
}

#[cfg(test)]
mod embed_queue_tests {
    use super::*;

    #[test]
    fn a_record_written_again_keeps_only_its_newest_text() {
        let mut q = EmbedQueue::default();
        q.push(Memory::Episodic, "a".into(), "old".into());
        q.push(Memory::Episodic, "b".into(), "b1".into());
        q.push(Memory::Episodic, "a".into(), "new".into());
        q.push(Memory::Semantic, "a".into(), "other type".into());
        let got = q.take(10);
        assert_eq!(got, vec![
            (Memory::Episodic, "b".to_string(), "b1".to_string()),
            (Memory::Episodic, "a".to_string(), "new".to_string()),
            (Memory::Semantic, "a".to_string(), "other type".to_string()),
        ]);
        assert!(q.is_empty());
        // Taken: a later write queues again.
        q.push(Memory::Episodic, "a".into(), "newer".into());
        assert_eq!(q.take(1).len(), 1);
    }
}

/// Records waiting for a deferred vector at most; more wait for the backfill scan.
const EMBED_SOON_MAX: usize = 200_000;
/// Texts per vector file of [`DurableWrites::embed_deferred`]: each file is one object
/// write and one log record, so not too few (at 32 a busy cluster made ~100 files a
/// second). The model still runs them [`adb_embed::MODEL_CALL`] at a time, so a
/// foreground request waits little behind them on a worker.
const EMBED_SOON_BATCH: usize = 256;

/// Deferred-embedding calls in flight at once per node (`ADB_EMBED_DEFER_PARALLEL`,
/// default 2): background work, in the model's background lane.
fn embed_parallel() -> usize {
    std::env::var("ADB_EMBED_DEFER_PARALLEL").ok().and_then(|v| v.parse().ok()).filter(|&n: &usize| n > 0).unwrap_or(2)
}

/// How long after the last deferred record the backfill scan waits.
const BACKFILL_AFTER_DEFERRED: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) fn retain_max() -> u64 {
    std::env::var("ADB_LOG_RETAIN").ok().and_then(|v| v.parse().ok()).unwrap_or(RETAIN_MAX)
}

/// The commit time of the record at `offset` (none for 0): from the log, or — when
/// the log no longer has it locally because it starts above an archive floor (a
/// partition restored from object storage) — from where the log was started.
/// Reading only the log failed every open after such a restore ("offset 2 was
/// purged"). A floor without a recorded time (a point archived before it was kept)
/// gives none: the clock then resumes from the wall clock.
pub(crate) async fn commit_ts_at<L: Log + ?Sized>(log: &L, offset: u64) -> AdbResult<Option<CommitTs>> {
    if offset == 0 {
        return Ok(None);
    }
    match log.read(LogOffset(offset), 1).await {
        Ok(recs) => Ok(recs.first().filter(|r| r.offset.0 == offset).map(|r| r.commit_ts)),
        Err(LogError::Purged { .. }) => {
            let floor = log.floor_commit().filter(|(o, _)| o.0 == offset).map(|(_, ts)| ts);
            if floor.is_none() {
                tracing::warn!(offset, "the log's record {offset} is only in the archive and its commit time was not kept; the clock resumes from the wall clock");
            }
            Ok(floor)
        }
        Err(e) => Err(log_err(e)),
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
    seq: Mutex<Sequencer<L>>,
    /// Who holds `seq` now.
    seq_holder: SeqHolder,
    /// A cluster owner before its epoch began: nothing is shipped yet.
    epoch_pending: std::sync::atomic::AtomicBool,
    /// The sequencer's log, shared: trimming and FORGET redaction use it without the
    /// sequencer (they need no ordering; holding it stalled every write).
    log: Arc<L>,
    /// The partition's Lance table (stage D2: written alongside the in-memory
    /// backends; reads move to it in D3). `None` = log + memory only.
    store: Option<Arc<PartitionStore>>,
    /// Background task applying committed records to `store` (stage D3.7).
    applier: Option<Applier>,
    /// Live subscriptions (stage E), following registrations as they commit.
    subs: Arc<Registry>,
    branches: Arc<crate::branches::Branches>,
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
    /// With object storage: the lowest log offset the log must keep — one past the
    /// lowest of the memory table's, the platform tables' and the engine's archived
    /// offsets ([`Self::archive_tick`]). 1 until the first archive.
    archive_floor: std::sync::atomic::AtomicU64,
    /// A compaction started by the maintenance tick is under way.
    compacting: Arc<std::sync::atomic::AtomicBool>,
    /// The log's tip at the last trim ([`Self::purge_log`]).
    purged_at: std::sync::atomic::AtomicU64,
    /// The floor the last `Effect::Archived` record announced.
    archive_marked: std::sync::atomic::AtomicU64,
    /// The offset of the last `Archived` record: records up to it are nothing new
    /// to archive (an idle node archives nothing).
    archive_record: std::sync::atomic::AtomicU64,
    /// Object storage holds every record up to this offset (WAL segments above the
    /// checkpoint); `None` until the first ship looks. Serialises shipping.
    shipped: tokio::sync::Mutex<Option<u64>>,
    /// The owner epoch object keys are written under (`segments`; 0 on a single node).
    epoch: std::sync::atomic::AtomicU64,
    /// Where other spaces (layers) are read: set at open (`crate::spaces`).
    spaces: std::sync::OnceLock<crate::spaces::SpaceView>,
    /// Platform writes waiting for the sequencer: the next holder commits them as one
    /// group (one record, one Lance commit per table), see [`Self::platform_many`].
    platform_queue: std::sync::Mutex<Vec<PlatformPending>>,
    /// The offset of the last write to each record key (PF-1): a write prepared
    /// outside the sequencer against offset N conflicts if one of its keys was
    /// written after N.
    key_offsets: std::sync::Mutex<KeyOffsets>,
    /// Records waiting for a vector (PF-51): memory, row id, text.
    backfill_queue: std::sync::Mutex<std::collections::VecDeque<(Memory, String, String)>>,
    /// The vector map's model switches the backfill has seen ([`Self::follow_embedding`]).
    vector_switches: std::sync::atomic::AtomicUsize,
    /// Records stored without waiting for their vector ([`DurableWrites::embed_deferred`]): (memory,
    /// record id, text), embedded by [`DurableWrites::embed_deferred`] as soon as it
    /// wakes. Bounded: past [`EMBED_SOON_MAX`] a record waits for the backfill scan.
    embed_soon: std::sync::Mutex<EmbedQueue>,
    embed_wake: Arc<tokio::sync::Notify>,
    /// When a record was last queued for its vector: the backfill scan waits while
    /// the deferred embedder has work (both scanning and embedding the same rows
    /// burned the nodes' CPU, Hetzner p15).
    deferred_at: std::sync::Mutex<Option<std::time::Instant>>,
    /// A submitted record failed to commit: this engine takes no more writes and
    /// restarts (as when it loses the lead).
    poisoned: Arc<std::sync::atomic::AtomicBool>,
    /// A newer owner holds this partition (a commit was fenced): this engine takes no
    /// more writes; the node goes on, and its router drops it (a cluster partition).
    moved: Arc<std::sync::atomic::AtomicBool>,
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
    /// Lineages forgotten since the last compaction: their payloads are redacted
    /// from the log by the next one (W5).
    forgotten: Arc<std::sync::Mutex<std::collections::HashSet<(Memory, String)>>>,
    /// DB-4: per namespace, (records, payload bytes) of current rows. Built by one scan when
    /// first needed, added to on each STORE, dropped by FORGET and compaction.
    usage: Mutex<Option<std::collections::HashMap<String, (u64, u64)>>>,
    /// B4: the commit times compaction must keep readable (branch points), per namespace.
    pins: std::sync::OnceLock<Pins>,
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
            let mut ct = std::time::Instant::now();
            if let Some(go) = t.go {
                stopped |= !matches!(go.await, Ok(true));
            }
            crate::steps::lap("engine.committer.1 wait for commit", &mut ct);
            if stopped {
                let _ = t.done.send(Err(AdbError::Internal("this ADB engine is restarting after a write failed to commit; retry".into())));
                continue;
            }
            let mut h = host.lock().await;
            crate::steps::lap("engine.committer.2 host lock wait", &mut ct);
            let r = match &t.record {
                Some(rec) => h.committed(rec).await.map_err(|e| AdbError::QueryError(e.to_string())),
                None => Ok(()),
            };
            crate::steps::lap("engine.committer.3 journal (host locked)", &mut ct);
            h.notify(&t.space, t.notices);
            drop(h);
            crate::steps::lap("engine.committer.4 notify (host locked)", &mut ct);
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
    ) -> AdbResult<Option<Vec<String>>> {
        let Some(store) = &self.store else { return Ok(None) };
        let mut t = std::time::Instant::now();
        let snapshot = self.last_off.load(std::sync::atomic::Ordering::SeqCst);
        let now = CommitTs::from_datetime(time::now());
        let ids = self.targets(memory_type, predicate, now, false, in_scope, namespace).await?;
        crate::steps::lap("update.1 targets", &mut t);
        if ids.is_empty() {
            return Ok(None);
        }
        let fields = match data {
            serde_json::Value::Object(map) => map.clone(),
            other => {
                let mut m = serde_json::Map::new();
                m.insert("value".into(), other.clone());
                m
            }
        };
        let mut current = Vec::new();
        if supersedes(memory_type) {
            for key in &ids {
                match store.current_version(memory_type.into(), key, now).await.map_err(|e| AdbError::Internal(format!("store: {e}")))? {
                    Some(cur) => current.push(cur),
                    None => return Ok(None),
                }
            }
        }
        // PF-48: the rows this record changes, for the applier (in-place types: the row).
        let hint: Vec<adb_store::StoredRow> = if supersedes(memory_type) {
            current.clone()
        } else {
            // Every type's patched row is read by the applier (WORKING too): hand it over.
            let mut rows = Vec::with_capacity(ids.len());
            for key in &ids {
                if let Ok(Some(r)) = store.get_cached(&adb_store::row_id(memory_type.into(), key)).await {
                    rows.push(r);
                }
            }
            rows
        };
        crate::steps::lap("update.2 current versions", &mut t);
        let keys: Vec<(Memory, String)> = ids.iter().map(|k| (memory_type.into(), k.clone())).collect();
        let mut seq = seq_lock!(self);
        crate::steps::lap("update.3 sequencer lock wait", &mut t);
        if self.key_offsets.lock().unwrap().conflict(&keys, snapshot) {
            crate::steps::add("update.x conflict -> slow path", std::time::Duration::ZERO);
            return Ok(None);
        }
        let ts = seq.begin();
        let mut effects = Vec::with_capacity(ids.len());
        // The rows a deferred vector goes to: a superseding UPDATE's new versions (the
        // key's row is then the old version), else the patched records.
        let mut embed_ids = ids.clone();
        if supersedes(memory_type) {
            embed_ids.clear();
            for (key, cur) in ids.iter().zip(&current) {
                let mut new = next_version(cur, &fields, seq.new_id(ts));
                embed_ids.push(new.id.0.clone());
                if let (Kind::Fact, (Some(from), Some(to))) = (new.data.kind, time::validity(&new.data.value)) {
                    if from >= to {
                        return Err(AdbError::InvalidSyntax(format!("{key}: valid_from must be before valid_to")));
                    }
                }
                effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new });
            }
        } else {
            effects.extend(ids.iter().map(|id| Effect::Patch { memory: memory_type.into(), id: RecordId(id.clone()), fields: fields.clone(), version: 0 }));
        }
        let (record, wait) = seq.submit_record(ts, None, effects).await.map_err(log_err)?;
        store.hint_rows(record.offset.0, hint);
        self.to_store_submitted(&record).await;
        drop(seq);
        crate::steps::lap("update.4 submit under lock", &mut t);
        self.committed(record.offset.0, wait).await?;
        crate::steps::lap("update.5 commit wait", &mut t);
        Ok(Some(embed_ids))
    }

    /// FORGET of named keys as [`Self::update_prepared`]: targets, every version and
    /// (SEMANTIC) the entity's facts are read first; a write to any of them, or a new
    /// fact about a forgotten entity, after `snapshot` is a conflict.
    async fn forget_prepared(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<Option<u64>> {
        let mut t = std::time::Instant::now();
        let snapshot = self.last_off.load(std::sync::atomic::Ordering::SeqCst);
        let now = CommitTs::from_datetime(time::now());
        let mut ids = self.targets(memory_type, predicate, now, true, in_scope, namespace).await?;
        crate::steps::lap("forget.1 targets", &mut t);
        if ids.is_empty() {
            return Ok(None);
        }
        let entities = ids.clone();
        if memory_type == MemoryType::Semantic {
            ids.extend(self.entity_facts(&ids, in_scope).await?);
        }
        let m: Memory = memory_type.into();
        // A record that is now another's alias leaves its links to that record: the
        // slow path does that (rare).
        if memory_type == MemoryType::Semantic && ids.iter().any(|id| self.alias_of(id).is_some()) {
            return Ok(None);
        }
        let mut keys: Vec<(Memory, String)> = ids.iter().map(|k| (m, k.clone())).collect();
        if memory_type == MemoryType::Semantic {
            keys.extend(entities.iter().map(|k| (m, refs_key(k))));
        }
        // PF-48: every version of what it forgets, for the applier (edges are read there).
        let hint = match &self.store {
            Some(store) => {
                let lineages: Vec<String> = ids.iter().map(|id| adb_store::row_id(m, id)).collect();
                store.versions_of(m, &lineages).await.unwrap_or_default()
            }
            None => Vec::new(),
        };
        crate::steps::lap("forget.2 entity facts + rows", &mut t);
        let mut seq = seq_lock!(self);
        crate::steps::lap("forget.3 sequencer lock wait", &mut t);
        if self.key_offsets.lock().unwrap().conflict(&keys, snapshot) {
            crate::steps::add("forget.x conflict -> slow path", std::time::Duration::ZERO);
            return Ok(None);
        }
        let ts = seq.begin();
        let effects: Vec<Effect> = ids.iter().map(|id| Effect::Forget { memory: m, lineage_id: RecordId(id.clone()) }).collect();
        self.usage_stale().await;
        let (record, wait) = seq.submit_record(ts, None, effects.clone()).await.map_err(log_err)?;
        if let Some(store) = &self.store {
            store.hint_rows(record.offset.0, hint);
        }
        self.keys_gone(&effects);
        self.note_forgotten(ids.iter().map(|id| (m, id.clone())));
        self.to_store_submitted(&record).await;
        drop(seq);
        crate::steps::lap("forget.4 submit under lock", &mut t);
        self.committed(record.offset.0, wait).await?;
        crate::steps::lap("forget.5 commit wait", &mut t);
        Ok(Some(ids.len() as u64))
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
        self.to_store_submitted(&record).await;
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
        let windows: Vec<(String, Duration)> = self.space_view().quotas().await?.into_iter()
            .filter_map(|q| q.history_days.map(|d| (q.pattern, Duration::from_secs(d * 86_400))))
            .collect();
        let now = CommitTs::from_datetime(time::now());
        // PF-16: in place, without the sequencer; only the swap holds readers, briefly.
        // On the compaction runtime, within the node's budget (`adb_store::budget`).
        let st = Arc::clone(store);
        let stats = adb_store::budget::run(async move { st.compact_with_windows(HISTORY, now, &pins, windows).await }).await.map_err(store_err)?;
        // With object storage the archive holds every payload before the log loses any.
        if store.log_floor().is_some() {
            store.archive().await.map_err(store_err)?;
        }
        let mut redacted = 0;
        if !rows.is_empty() {
            // Close the segment holding the newest records (the only step that takes
            // the sequencer), then rewrite the closed ones without the forgotten
            // payloads, while writes go on into the new segment; offsets (and every
            // subscriber's position) stay. Done under the sequencer, the rewrite held
            // every write for up to 1.4 s (local bench, 15 Nov).
            {
                let mut seq = seq_lock!(self);
                let ts = seq.begin();
                let marker = seq.append_record_on_new_segment(ts, Vec::new()).await.map_err(log_err)?;
                self.to_store(&marker).await;
            }
            let redact = |r: &mut LogRecord| redact_record(r, &rows);
            redacted = self.log.rewrite_closed(&redact).await.map_err(log_err)?;
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

    /// Start a compaction in the background unless one is under way: the maintenance
    /// tick goes on (log trim, archive) while it waits for the node's budget and runs.
    pub fn compact_in_background(self: &Arc<Self>) {
        if self.compacting.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let me = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = me.compact().await {
                tracing::warn!(error = %e, "compaction failed; retrying later");
            }
            me.compacting.store(false, std::sync::atomic::Ordering::SeqCst);
        });
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
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        let id = seq.new_id(ts);
        let mut data = row(MemoryType::Working, Kind::Cq, spec);
        data.key = None;
        data.text = None;
        data.subject_id = Some(id.0.clone());
        let effect = Effect::Register(NewRow { id: id.clone(), lineage_id: id.clone(), version: 1, data });
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.to_store(&record).await;
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
        let seq = seq_lock!(self);
        let off = seq.log().last_offset().0;
        let ts = match off {
            0 => CommitTs::from_datetime(time::now()),
            n => commit_ts_at(seq.log(), n).await?.unwrap_or_else(|| CommitTs::from_datetime(time::now())),
        };
        Ok((off, ts))
    }

    /// DB-9: the (offset, time) a past point names.
    pub async fn resolve_point(&self, namespace: &str, at: &crate::branches::PointRef) -> AdbResult<(u64, CommitTs)> {
        let seq = seq_lock!(self);
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
        self.to_store(&record).await;
        Ok(())
    }

    /// The log's last offset and its commit time (now, for an empty log): a new
    /// branch's or tag's point.
    async fn last_point(&self, seq: &Sequencer<L>) -> AdbResult<(u64, CommitTs)> {
        let off = seq.log().last_offset().0;
        let ts = match off {
            0 => CommitTs::from_datetime(time::now()),
            n => commit_ts_at(seq.log(), n).await?.unwrap_or_else(|| CommitTs::from_datetime(time::now())),
        };
        Ok((off, ts))
    }

    /// DB-4: set or replace the quota on `pattern`.
    pub async fn quota_set(&self, pattern: &str, max_bytes: Option<u64>, max_records: Option<u64>, history_days: Option<u64>) -> AdbResult<crate::branches::Quota> {
        if pattern.is_empty() || (max_bytes.is_none() && max_records.is_none() && history_days.is_none()) {
            return Err(AdbError::QueryError("quota_set needs a pattern and max_bytes, max_records or history_days".into()));
        }
        let q = crate::branches::Quota { id: crate::branches::Quota::id_of(pattern), pattern: pattern.to_string(), max_bytes, max_records, history_days };
        let mut seq = seq_lock!(self);
        self.register_control(&mut seq, &q.id, pattern, serde_json::to_value(&q).unwrap_or_default()).await?;
        Ok(q)
    }

    /// DB-4: remove the quota on `pattern`; whether it had one.
    pub async fn quota_drop(&self, pattern: &str) -> AdbResult<bool> {
        let id = crate::branches::Quota::id_of(pattern);
        if !self.branches.quotas().iter().any(|q| q.id == id) {
            return Ok(false);
        }
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        let record = seq.append_record(ts, None, vec![Effect::Unregister { id: RecordId(id) }]).await.map_err(log_err)?;
        self.branches.apply(&record);
        self.to_store(&record).await;
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
        let Some(ns) = namespace else { return Ok(()) };
        let view = self.space_view();
        let quotas: Vec<_> = view.quotas().await?.into_iter().filter(|q| q.covers(ns)).collect();
        if quotas.is_empty() {
            return Ok(());
        }
        let usage = self.usage().await?;
        for q in quotas {
            let (mut records, mut used) = usage.iter().filter(|(n, _)| q.covers(n)).fold((0u64, 0u64), |a, (_, u)| (a.0 + u.0, a.1 + u.1));
            // A pattern can cover namespaces in other partitions (option A, 14 Nov):
            // their current usage counts too. One namespace lives here only.
            if q.pattern.ends_with('*') {
                if let Some(store) = &self.store {
                    let (r, b) = view.usage_elsewhere(&q, store.partition()).await?;
                    records += r;
                    used += b;
                }
            }
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
        let mut seq = seq_lock!(self);
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

    /// AQL v0.13 BRANCH / FORK: a tag on `namespace` at `at` (now when `None`): it holds
    /// the point a branch reads through to. A past point must be within the history
    /// kept (`HISTORY`).
    pub async fn tag_create_at(&self, namespace: &str, name: &str, at: Option<CommitTs>) -> AdbResult<crate::branches::Tag> {
        let mut seq = seq_lock!(self);
        let (last_offset, last_ts) = self.last_point(&seq).await?;
        let (point_offset, point_ts) = match at {
            Some(t) if t.0 < last_ts.0 => {
                let kept_from = CommitTs::from_datetime(time::now()).0 - HISTORY.as_micros() as i64;
                if t.0 < kept_from {
                    return Err(AdbError::QueryError(format!("AS OF: older than the {} days of versions kept", HISTORY.as_secs() / 86_400)));
                }
                (0, t.0)
            }
            _ => (last_offset, last_ts.0),
        };
        let t = crate::branches::Tag {
            id: crate::branches::Tag::id_of(namespace, name),
            namespace: namespace.into(),
            name: name.into(),
            point_offset,
            point_ts,
            created_by: None,
        };
        self.register_control(&mut seq, &t.id, namespace, serde_json::to_value(&t).unwrap_or_default()).await?;
        Ok(t)
    }

    /// AQL v0.13 MERGE: write into `namespace` (its own records) in one log record:
    /// per op, the source's record for a key (a new version of the namespace's own
    /// record of that key, or a new record), or `None` to remove the namespace's own
    /// record of that key. A link's two ends are re-pointed into `namespace`.
    pub async fn merge_apply(&self, namespace: &str, ops: Vec<MergeWrite>) -> AdbResult<u64> {
        if ops.is_empty() {
            return Ok(0);
        }
        let mut seq = seq_lock!(self);
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let ts = seq.begin();
        // An id of the table (`<memory tag>:<record id>`) or a record id, in `namespace`.
        let here = |id: &str| adb_store::namespaced(Some(namespace), adb_store::without_namespace(adb_store::key_of(id)));
        let mut effects = Vec::new();
        for op in &ops {
            let lineage = here(&op.key);
            match (&op.current, &op.source) {
                (cur, Some(src)) => {
                    let mut data = src.data.clone();
                    data.namespace = Some(namespace.to_string());
                    for e in [&mut data.subject_id, &mut data.object_id].into_iter().flatten() {
                        *e = here(e);
                    }
                    let new = |id: RecordId, version: u32| NewRow { id, lineage_id: RecordId(lineage.clone()), version, data: data.clone() };
                    effects.push(match cur {
                        Some(c) => Effect::Supersede { old: RecordId(adb_store::key_of(&c.id).to_string()), new: new(seq.new_id(ts), c.version + 1) },
                        None => match data.kind {
                            Kind::Edge => Effect::PutEdge(new(RecordId(lineage.clone()), 1)),
                            Kind::Fact => Effect::PutFact(new(RecordId(lineage.clone()), 1)),
                            Kind::Tool => Effect::PutTool(new(RecordId(lineage.clone()), 1)),
                            Kind::Episode => Effect::PutEpisode(new(RecordId(lineage.clone()), 1)),
                            _ => Effect::PutEntity(new(RecordId(lineage.clone()), 1)),
                        },
                    });
                }
                (Some(_), None) => effects.push(Effect::Forget { memory: op.memory, lineage_id: RecordId(lineage) }),
                (None, None) => {}
            }
        }
        let n = effects.len() as u64;
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        self.note_forgotten(effects.iter().filter_map(|e| match e {
            Effect::Forget { memory, lineage_id } => Some((*memory, lineage_id.0.clone())),
            _ => None,
        }));
        self.to_store(&record).await;
        Ok(n)
    }

    /// A control record of `namespace` (`id`, e.g. `merge:<ns>/<id>`): a merge's
    /// entries, for its revert. Kept in the table like branch rows.
    pub async fn record_put(&self, namespace: &str, id: &str, value: serde_json::Value) -> AdbResult<()> {
        let mut seq = seq_lock!(self);
        let mut v = value;
        if let Some(m) = v.as_object_mut() {
            m.insert("id".into(), serde_json::Value::String(id.to_string()));
        }
        self.register_control(&mut seq, id, namespace, v).await
    }

    /// The control records whose id starts with `prefix`.
    pub async fn records(&self, prefix: &str) -> AdbResult<Vec<serde_json::Value>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let rows = store.current(Some("kind = 'branch'"), CommitTs::from_datetime(time::now())).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        Ok(rows.into_iter().map(|r| r.data.value).filter(|v| v.get("id").and_then(|i| i.as_str()).is_some_and(|i| i.starts_with(prefix))).collect())
    }

    /// AQL v0.13: record `b` (`base:<namespace>`), or drop it (`None` for `namespace`).
    pub async fn base_set(&self, namespace: &str, b: Option<&crate::branches::Base>) -> AdbResult<()> {
        let mut seq = seq_lock!(self);
        match b {
            Some(b) => self.register_control(&mut seq, &b.id, &b.namespace, serde_json::to_value(b).unwrap_or_default()).await,
            None => {
                let ts = seq.begin();
                let effect = Effect::Unregister { id: RecordId(crate::branches::Base::id_of(namespace)) };
                let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
                self.branches.apply(&record);
                self.to_store(&record).await;
                Ok(())
            }
        }
    }

    /// B7: drop a tag (refused while a pinned layer reads it).
    pub async fn tag_drop(&self, namespace: &str, name: &str) -> AdbResult<bool> {
        if self.branches.tag(namespace, name).is_none() {
            return Ok(false);
        }
        let view = self.space_view();
        for space in view.layered_by(namespace).await? {
            if view.layers_of(&space).await?.iter().any(|l| l.space == namespace && l.tag.as_deref() == Some(name)) {
                return Err(AdbError::QueryError(format!("tag {name:?} of {namespace:?} is pinned by {space:?}: change its layers first")));
            }
        }
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(crate::branches::Tag::id_of(namespace, name)) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.branches.apply(&record);
        self.to_store(&record).await;
        Ok(true)
    }

    /// B7: set `namespace`'s layers (empty: none). One level: a layer's space has no
    /// layers itself; a pin names an existing tag; layers carry SEMANTIC, PROCEDURAL or
    /// TOOLS only.
    pub async fn layers_set(&self, namespace: &str, layers: Vec<crate::branches::Layer>) -> AdbResult<crate::branches::Layers> {
        use crate::branches::LayerMode;
        let view = self.space_view();
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
            if !history && !view.layers_of(&l.space).await?.is_empty() {
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
                (LayerMode::Pin, Some(t)) | (LayerMode::History, Some(t)) => {
                    if view.tag(&l.space, t).await?.is_none() {
                        return Err(AdbError::QueryError(format!("{:?} has no tag {t:?}", l.space)));
                    }
                }
                _ => {}
            }
        }
        // A layer space stays flat, history layers aside (they name predecessors).
        let mut layered = false;
        for by in view.layered_by(namespace).await? {
            layered |= view.layers_of(&by).await?.iter().any(|l| l.space == namespace && l.mode != LayerMode::History);
        }
        if layered && layers.iter().any(|l| l.mode != LayerMode::History) {
            return Err(AdbError::QueryError(format!("{namespace:?} is a layer of another space: layers are one level deep (B7)")));
        }
        let mut seq = seq_lock!(self);
        let ls = crate::branches::Layers { id: crate::branches::Layers::id_of(namespace), namespace: namespace.into(), layers };
        self.register_control(&mut seq, &ls.id, namespace, serde_json::to_value(&ls).unwrap_or_default()).await?;
        Ok(ls)
    }

    /// B7, sealed layers: refuse a write in `namespace` of `key` (a record id without
    /// its namespace) when a sealed layer carrying `memory` has it — or, for a fact, has
    /// a fact with the same subject and predicate.
    async fn check_sealed(&self, memory: MemoryType, namespace: Option<&str>, key: &str, fact: Option<(&str, &str)>, now: CommitTs) -> AdbResult<()> {
        let (Some(ns), Some(store)) = (namespace, &self.store) else { return Ok(()) };
        let _ = store;
        let view = self.space_view();
        for l in self.branches.layers_of(ns).iter().filter(|l| l.sealed && l.memories.contains(&memory)) {
            let at = view.layer_point(l).await?.unwrap_or(now);
            let rows = view.versions(memory.into(), &l.space).await?;
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

    /// Read other spaces (layers) through `view` (set once, at open).
    pub fn set_spaces(&self, view: crate::spaces::SpaceView) {
        let _ = self.spaces.set(view);
    }

    /// Where other spaces are read: the view set at open, else this table and registry.
    pub fn space_view(&self) -> crate::spaces::SpaceView {
        match self.spaces.get() {
            Some(v) => v.clone(),
            None => crate::spaces::SpaceView::new(crate::spaces::LocalSpaces { store: self.store.clone(), barrier: None, registry: Arc::clone(&self.branches) }),
        }
    }

    pub fn branches(&self) -> &Arc<crate::branches::Branches> {
        &self.branches
    }

    /// What compaction keeps for tags, and so for the branches and forks whose points
    /// they hold (B4); set once when the engine opens.
    pub fn set_pins(&self, pins: Pins) {
        let _ = self.pins.set(pins);
    }

    /// B5 (promote / clone): write `rows` — a space's records as a view sees them now —
    /// into `namespace` as new records (version 1, no history), through this engine's
    /// sequencer in batched records. Ids and a FACT's ends move to the new namespace
    /// (NS ids); links and control rows are not copied. `namespace` must hold no
    /// records yet. Returns how many records were written.
    pub async fn materialise(&self, namespace: &str, rows: Vec<adb_store::StoredRow>) -> AdbResult<u64> {
        if !adb_store::valid_namespace(namespace) {
            return Err(AdbError::QueryError(format!("namespace {namespace:?}: can't be empty or contain '|' or '~'")));
        }
        let Some(store) = &self.store else { return Err(needs_table("promote")) };
        let into = |id: &str| adb_store::namespaced(Some(namespace), adb_store::without_namespace(adb_store::key_of(id)));
        let mut rows: Vec<adb_store::StoredRow> = rows.into_iter().filter(|r| matches!(r.data.kind, Kind::Entity | Kind::Tool | Kind::Episode | Kind::Fact)).collect();
        // Entities before the facts about them.
        rows.sort_by_key(|r| (r.data.kind == Kind::Fact, r.commit_ts));
        let mut seq = seq_lock!(self);
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
            self.to_store(&record).await;
        }
        Ok(n)
    }

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
            let seq = seq_lock!(self);
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

    /// AQL v0.13: whether `namespace` is a branch (BRANCH FROM … INTO it): its writes
    /// follow the branch rules against its base's records.
    fn branch_ns<'a>(&self, namespace: Option<&'a str>) -> Option<&'a str> {
        namespace.filter(|ns| self.branches.base_of(ns).is_some_and(|b| b.kind == crate::branches::BaseKind::Branch))
    }

    /// The view of the branch namespace `branch_ns` (AQL v0.13) of `memory` matching
    /// `predicate`, each row with `true` when the branch itself holds it (else
    /// inherited from its base). None without a branch.
    async fn branch_targets_in(&self, memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>, now: CommitTs, branch_ns: Option<&str>) -> AdbResult<Vec<(adb_store::StoredRow, bool)>> {
        let Some(ns) = branch_ns else { return Ok(Vec::new()) };
        let rows = self.space_view().rows_at(memory.into(), ns, now, now).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let mine = r.data.namespace.as_deref() == Some(ns);
                (r, mine)
            })
            .filter(|(r, _)| in_scope.is_none_or(|s| r.data.scope == s))
            .filter(|(r, _)| crate::query::matches(memory, &crate::query::record(r), predicate))
            .collect())
    }

    /// UPDATE / FORGET in a branch namespace, under the sequencer lock: the branch's
    /// own row gets a new version; a record inherited from its base gets an override
    /// row in the branch (the base untouched). FORGET writes `_branch_deleted` the same way.
    #[allow(clippy::too_many_arguments)]
    async fn branch_write(
        &self,
        seq: &mut Sequencer<L>,
        ts: CommitTs,
        memory: MemoryType,
        predicate: &Predicate,
        in_scope: Option<&str>,
        fields: &serde_json::Map<String, serde_json::Value>,
        branch_ns: Option<&str>,
    ) -> AdbResult<Vec<String>> {
        let rows = self.branch_targets_in(memory, predicate, in_scope, ts, branch_ns).await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut effects = Vec::with_capacity(rows.len());
        let mut ids = Vec::with_capacity(rows.len());
        for (cur, mine) in &rows {
            let mut new = next_version(cur, fields, seq.new_id(ts));
            ids.push(new.id.0.clone());
            // An inherited record's override is the branch namespace's own.
            if let (false, Some(ns)) = (*mine, branch_ns) {
                new.data.namespace = Some(ns.to_string());
            }
            effects.push(match (mine, memory) {
                (true, _) => Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new },
                (false, MemoryType::Episodic) => Effect::PutEpisode(new),
                (false, MemoryType::Tools) => Effect::PutTool(new),
                (false, _) => Effect::PutEntity(new),
            });
        }
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store(&record).await;
        Ok(ids)
    }

    pub async fn unregister_watch(&self, id: &str) -> AdbResult<()> {
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(id.to_string()) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.to_store(&record).await;
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
    /// Whether object storage is on (`ADB_COLD_URI`): the log is kept for it.
    pub fn log_floor_on(&self) -> bool {
        self.store.as_ref().is_some_and(|s| s.log_floor().is_some())
    }

    /// Object storage (S3 option B): archive a checkpoint of the node when
    /// [`adb_store::ArchivePolicy`] says so — the new folded data reached
    /// `ADB_ARCHIVE_MIN_BYTES`, or `ADB_ARCHIVE_MAX_AGE_SECS` passed with anything
    /// new in the log — folding first so the checkpoint covers the log's tip. Until
    /// then the log holds what object storage lacks. A no-op without object storage.
    pub async fn archive_tick(&self) -> AdbResult<()> {
        Box::pin(self.archive_checkpoint(false)).await
    }

    /// Archive a checkpoint now, whatever the policy (on request, tests).
    pub async fn archive_now(&self) -> AdbResult<()> {
        Box::pin(self.archive_checkpoint(true)).await
    }

    async fn archive_checkpoint(&self, force: bool) -> AdbResult<()> {
        let Some(store) = &self.store else { return Ok(()) };
        if store.log_floor().is_none() {
            return Ok(());
        }
        let policy = adb_store::ArchivePolicy::from_env();
        let tip = seq_lock!(self).log().last_offset().0;
        let seen = store.archive_watermark().1 .0.max(self.archive_record.load(std::sync::atomic::Ordering::SeqCst));
        let pending = store.archive_pending().await;
        if !force && !policy.due(pending.bytes, tip > seen, pending.since_last) {
            return Ok(());
        }
        if tip > store.archive_watermark().1 .0 {
            self.compact().await?;
        }
        let stats = store.archive().await.map_err(|e| AdbError::Internal(format!("archive: {e}")))?;
        let cold = store.cold_uri().unwrap_or_default().to_string();
        let (trees_floor, mut node) = Box::pin(self.archive_trees(&cold, policy)).await?;
        let floor = stats.watermark.min(trees_floor);
        self.archive_floor.store(floor + 1, std::sync::atomic::Ordering::SeqCst);
        tracing::info!(files = stats.files, version = stats.version, memory = stats.watermark, floor, held = tip.saturating_sub(floor), "archived to object storage");
        // Tell every replica where the node now restores from (only when that moved: an
        // idle node writes nothing). Raft snapshots are taken at this point.
        if floor > self.archive_marked.load(std::sync::atomic::Ordering::SeqCst) {
            if let Some(current) = store.archived_current() {
                node.insert("memory".into(), current);
            }
            // The floor record's commit time: a node restored to this point starts its
            // log above the floor, and resumes its clock after it (`commit_ts_at`).
            if let Some(ts) = commit_ts_at(self.log.as_ref(), floor).await? {
                node.insert("floor_ts".into(), serde_json::json!(ts.0));
            }
            let point = crate::restore::ArchivePoint { cold: cold.clone(), floor, node: serde_json::Value::Object(node) };
            let record = {
                let mut seq = seq_lock!(self);
                let effects = vec![Effect::Archived { cold, floor: LogOffset(floor), node: point.node.clone() }];
                let ts = seq.begin();
                let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
                self.to_store(&record).await;
                record
            };
            self.archive_marked.store(floor, std::sync::atomic::Ordering::SeqCst);
            self.archive_record.store(record.offset.0, std::sync::atomic::Ordering::SeqCst);
            // Where a node restores from, then the WAL segments the checkpoint covers go.
            let deleted = match self.checkpoint_segments(&point, store.partition()).await? {
                segments::Checkpointed::Done { deleted } => deleted,
                segments::Checkpointed::Fenced { newer } => {
                    // A newer owner has the partition: take no more writes (the engine restarts).
                    tracing::error!(epoch = self.epoch(), newer, "this owner has been fenced off; no more writes");
                    self.poisoned.store(true, std::sync::atomic::Ordering::SeqCst);
                    return Err(AdbError::Internal(format!("fenced: epoch {newer} has taken this partition over from epoch {}", self.epoch())));
                }
            };
            tracing::info!(floor, deleted, "checkpoint point written; covered WAL segments deleted");
        }
        Ok(())
    }

    /// Archive the platform tables and the engine's spaces beside the memory table;
    /// returns the lowest offset they can be rebuilt to (`u64::MAX`: neither exists),
    /// and where each is (`NODE`).
    async fn archive_trees(&self, cold: &str, policy: adb_store::ArchivePolicy) -> AdbResult<(u64, serde_json::Map<String, serde_json::Value>)> {
        let err = |e: adb_store::StoreError| AdbError::Internal(format!("archive: {e}"));
        let mut floor = u64::MAX;
        let mut lineages = serde_json::Map::new();
        if let Some(p) = &self.platform {
            // `_applied.json` as it is before the copy: the archive holds at least that.
            // Past the last platform record, the platform needs nothing from the log.
            let platform_last = self.platform_last.load(std::sync::atomic::Ordering::SeqCst);
            let root = p.root().to_path_buf();
            // Each table's new small fragments into large files first.
            p.merge_unarchived(&adb_store::tree_archived(&root), policy.min_bytes).await.map_err(|e| AdbError::Internal(format!("archive: platform merge: {e}")))?;
            // Under the platform's file lock (no compaction deletes a version meanwhile):
            // `_applied.json`, and each table at the version it names.
            let (lineage, offset) = p
                .with_tables(|_| async {
                    let applied = std::fs::read(root.join("_applied.json")).ok();
                    let parsed = applied.as_ref().and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok());
                    let offset = parsed.as_ref().and_then(|v| v.get("offset").and_then(|o| o.as_u64())).unwrap_or(0);
                    let pins: std::collections::HashMap<String, u64> = parsed
                        .as_ref()
                        .and_then(|v| v.get("versions").and_then(|m| m.as_object()).cloned())
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|(t, v)| v.as_u64().map(|v| (format!("{t}.lance"), v)))
                        .collect();
                    let last: Vec<(String, Vec<u8>)> = applied.map(|b| vec![("_applied.json".to_string(), b)]).unwrap_or_default();
                    Box::pin(adb_store::archive_tree(cold, "platform", &root, &|_| false, &last, &pins)).await.map(|(l, _)| (l, offset))
                })
                .await
                .map_err(err)?;
            lineages.insert("platform".into(), serde_json::json!({ "lineage": lineage, "watermark": offset }));
            if offset < platform_last {
                floor = floor.min(offset);
            }
        }
        if let Some(e) = &self.engine {
            let (dir, points) = e.lock().await.archive_points();
            let mut engine_floor = u64::MAX;
            let mut last = Vec::new();
            for pt in points {
                let journal = adb_store::table_meta_offset(&pt.journal).await.map_err(err)?;
                // A space whose snapshot and journal hold all its changes needs nothing.
                if pt.snapshot_offset < pt.last_offset || journal < pt.last_offset {
                    engine_floor = engine_floor.min(pt.snapshot_offset.min(journal));
                }
                last.extend(pt.snapshot);
            }
            if engine_floor != u64::MAX || dir.exists() {
                let (lineage, _) = Box::pin(adb_store::archive_tree(cold, "engine", &dir, &|rel: &str| rel.contains("_mem_wal"), &last, &Default::default())).await.map_err(err)?;
                let w = if engine_floor == u64::MAX { 0 } else { engine_floor };
                lineages.insert("engine".into(), serde_json::json!({ "lineage": lineage, "watermark": w }));
                if engine_floor != u64::MAX {
                    floor = floor.min(engine_floor);
                }
            }
        }
        Ok((floor, lineages))
    }

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

    /// Current SEMANTIC / EPISODIC / PROCEDURAL rows with text to embed and no vector.
    async fn unembedded(&self) -> AdbResult<Vec<(adb_store::StoredRow, String)>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        // Unembedded for the model in force (the setting may have just changed).
        if let Some(e) = &self.embedder {
            store.vectors().follow(&e.model(), e.dimension());
        }
        let now = CommitTs::from_datetime(time::now());
        let filter = format!(
            "{} AND memory IN ('semantic', 'episodic', 'procedural') AND tombstone = false AND superseded_by IS NULL",
            adb_store::RECORD_KINDS
        );
        let rows = store.scan(Some(&filter)).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        let vectors = store.vectors();
        Ok(rows
            .into_iter()
            .filter(|r| r.is_current(now) && !vectors.contains(&r.id))
            .filter_map(|r| crate::reads::embed_text(r.data.memory.into(), &r.data.value).map(|t| (r, t)))
            .collect())
    }

    /// Point the partition's vectors at the embedding setting's model; `true` when that
    /// changed them (the backfill then has every record to embed again, at once: the
    /// texts queued for the old model are dropped).
    pub fn follow_embedding(&self) -> bool {
        let (Some(store), Some(e)) = (&self.store, &self.embedder) else { return false };
        store.vectors().follow(&e.model(), e.dimension());
        // A switch made anywhere (a LIKE read, a status call) counts.
        let n = store.vectors().switches();
        if self.vector_switches.swap(n, std::sync::atomic::Ordering::Relaxed) == n {
            return false;
        }
        self.backfill_queue.lock().unwrap().clear();
        *self.deferred_at.lock().unwrap() = None;
        true
    }

    /// Whether vector files are being loaded (rows look unembedded until they are).
    pub fn vectors_loading(&self) -> bool {
        self.store.as_ref().is_some_and(|s| s.vectors().loading() > 0)
    }

    /// How many records still wait for a vector (W9.10: re-embedding progress).
    pub async fn pending_embeddings(&self) -> AdbResult<u64> {
        Ok(self.unembedded().await?.len() as u64)
    }

    /// Embed current SEMANTIC / EPISODIC / PROCEDURAL rows that have text but no
    /// vector — written before there was a model, or whose embedding failed — into a
    /// vector file ([`Self::store_vectors`]), at most `batch` rows per call (MVP W4
    /// T7). Embedding runs outside the sequencer; a row superseded or forgotten in
    /// between just gets a vector it no longer needs. Returns how many.
    pub async fn embed_backfill(&self, batch: usize) -> AdbResult<u64> {
        let (Some(store), Some(_)) = (&self.store, &self.embedder) else { return Ok(0) };
        // Rows waiting for a deferred vector are the deferred embedder's: no scan for
        // them while it has work, or had some lately (they'd be found and embedded twice).
        // Nor while vector files are still loading (their rows look unembedded).
        if self.embeds_queued() > 0 || store.vectors().loading() > 0 || self.deferred_at.lock().unwrap().is_some_and(|t| t.elapsed() < BACKFILL_AFTER_DEFERRED) {
            return Ok(0);
        }
        // PF-51: one scan fills a queue (memory, row id, text) and the batches come from
        // it; the table is scanned again only when the queue is empty. Scanning every
        // unembedded row for each batch of 64 pinned the leader's CPU for hours with
        // 700k records to embed (AWS r10). A queued row that changed meanwhile gets a
        // vector on its old version (harmless); its new version is found by the next scan.
        const QUEUE_MAX: usize = 50_000;
        if self.backfill_queue.lock().unwrap().is_empty() {
            let found: Vec<(Memory, String, String)> = self.unembedded().await?.into_iter().take(QUEUE_MAX).map(|(r, t)| (r.data.memory, adb_store::key_of(&r.id).to_string(), t)).collect();
            self.backfill_queue.lock().unwrap().extend(found);
        }
        let todo: Vec<(Memory, String, String)> = {
            let mut q = self.backfill_queue.lock().unwrap();
            let n = batch.min(q.len());
            q.drain(..n).collect()
        };
        if todo.is_empty() {
            return Ok(0);
        }
        self.store_vectors(&todo).await
    }

    /// On the owner: merge the partition's vector files once there are many
    /// (`adb_store::vectors::MERGE_AT`), in the background.
    pub fn merge_vector_files(&self) {
        let Some(store) = &self.store else { return };
        if self.writable().is_err() || store.vectors().files() < adb_store::vectors::MERGE_AT {
            return;
        }
        let map = Arc::clone(store.vectors());
        tokio::spawn(async move {
            if let Err(e) = map.merge().await {
                tracing::warn!(error = %e, "vector files not merged");
            }
        });
    }

    /// Vectors for `todo`'s records (memory, record id, text), as one vector file
    /// (`adb_store::vectors`): a compute node embeds them and writes the file, or,
    /// without compute nodes, this node's model does and this node writes it. Then a
    /// `Vectors` record names the file, and every node applying it loads the file.
    /// No row of the table changes. An embedding or file error is an
    /// [`AdbError::EmbeddingError`] (the texts can be tried again).
    async fn store_vectors(&self, todo: &[(Memory, String, String)]) -> AdbResult<u64> {
        let (Some(store), Some(embedder)) = (&self.store, &self.embedder) else { return Ok(0) };
        if todo.is_empty() {
            return Ok(0);
        }
        let map = store.vectors();
        // The setting may change at any time: the vectors carry the model they were
        // made with, and the map follows the model in force.
        let (model, dim) = (embedder.model(), embedder.dimension());
        map.follow(&model, dim);
        let items: Vec<(String, String)> = todo.iter().map(|(m, id, t)| (adb_store::row_id(*m, id), t.clone())).collect();
        let uri = match embedder.embed_to_file(map.location(), &items).await {
            Some(r) => r.map_err(AdbError::EmbeddingError)?,
            None => {
                let vectors = adb_embed::embed_in_calls(embedder, items.iter().map(|(_, t)| t.clone()).collect()).await.map_err(AdbError::EmbeddingError)?;
                if vectors.first().is_some_and(|v| v.len() != dim) || embedder.model() != model {
                    return Err(AdbError::EmbeddingError("the embedding setting changed during the batch; embedding it again".into()));
                }
                let entries: Vec<adb_store::vectors::Entry> = items.into_iter().zip(vectors).map(|((id, _), v)| (id, adb_embed::to_f16(&v))).collect();
                let uri = adb_store::vectors::write(map.location(), &model, dim, &entries).await.map_err(AdbError::EmbeddingError)?;
                map.insert(&uri, &model, entries);
                uri
            }
        };
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        let record = seq.append_record(ts, None, vec![Effect::Vectors { uri }]).await.map_err(log_err)?;
        self.to_store(&record).await;
        Ok(todo.len() as u64)
    }

    /// Queue a stored record's text for its vector ([`DurableWrites::embed_deferred`]).
    fn embed_later(&self, memory: Memory, id: String, text: String) {
        if !self.embed_soon.lock().unwrap().push(memory, id, text) {
            tracing::debug!("deferred embeddings queue full: the backfill scan embeds the rest");
            return;
        }
        *self.deferred_at.lock().unwrap() = Some(std::time::Instant::now());
        self.embed_wake.notify_one();
    }

    /// An UPDATE's new text, for each record it changed ([`DurableWrites::embed_deferred`]).
    fn embed_ids_later(&self, memory_type: MemoryType, ids: &[String], text: Option<&str>) {
        if let Some(text) = text {
            for id in ids {
                self.embed_later(memory_type.into(), id.clone(), text.to_string());
            }
        }
    }

    /// Woken when a record is queued for its vector.
    pub fn embed_wake(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.embed_wake)
    }

    /// Records waiting for a deferred vector.
    pub fn embeds_queued(&self) -> usize {
        self.embed_soon.lock().unwrap().len()
    }

    /// Embed the queued records into vector files ([`Self::store_vectors`]), in
    /// batches of [`EMBED_SOON_BATCH`], up to `ADB_EMBED_DEFER_PARALLEL` at a time.
    /// Returns how many got a vector. A batch that fails stays queued, in order, and
    /// is tried again on the next wake-up.
    pub async fn embed_deferred(&self) -> AdbResult<u64> {
        if self.embedder.is_none() {
            self.embed_soon.lock().unwrap().clear();
            return Ok(0);
        }
        let mut done = 0;
        loop {
            let batches: Vec<Vec<(Memory, String, String)>> = {
                let mut q = self.embed_soon.lock().unwrap();
                let mut out = Vec::new();
                while out.len() < embed_parallel() && !q.is_empty() {
                    let b = q.take(EMBED_SOON_BATCH);
                    if !b.is_empty() {
                        out.push(b);
                    }
                }
                out
            };
            if batches.is_empty() {
                return Ok(done);
            }
            type Stored<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = AdbResult<u64>> + Send + 'a>>;
            let calls: Vec<Stored<'_>> = batches.iter().map(|b| Box::pin(self.store_vectors(b)) as Stored<'_>).collect();
            let results = futures::future::join_all(calls).await;
            let mut failed: Vec<(Memory, String, String)> = Vec::new();
            let mut error = None;
            for (batch, r) in batches.into_iter().zip(results) {
                match r {
                    Ok(n) => done += n,
                    Err(AdbError::EmbeddingError(e)) => {
                        failed.extend(batch);
                        error = Some(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            // No compute node up, or the model failing: the texts stay queued, in order,
            // and the embedder tries again on its next wake-up (at most a second).
            if let Some(e) = error {
                tracing::warn!(error = %e, records = failed.len(), queued = self.embeds_queued(), "deferred embedding failed; the texts stay queued");
                self.embed_soon.lock().unwrap().requeue(failed);
                return Ok(done);
            }
        }
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
        let last_commit_ts = commit_ts_at(seq.log(), last_offset.0).await?;
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
        let aliases = load_aliases(&store).await?;
        steps.push(("aliases", t_step.elapsed()));
        tracing::info!(steps = ?steps, "write path opened");
        let store = Some(store);
        Ok((Self { log: seq.shared_log(), seq: Mutex::new(seq), seq_holder: Default::default(), epoch_pending: Default::default(), store, applier, subs: Arc::new(subs), branches: Arc::new(branches), platform: None, live: std::sync::Mutex::new(live), embedder: None, forgotten_since: Default::default(), compactor: Mutex::new(()), usage: Mutex::new(None), platform_last: Default::default(), archive_floor: std::sync::atomic::AtomicU64::new(1), purged_at: Default::default(), compacting: Default::default(), archive_marked: Default::default(), archive_record: Default::default(), shipped: Default::default(), epoch: Default::default(), spaces: Default::default(), platform_queue: Default::default(), platform_tickets: Default::default(), platform_applied: Arc::new(tokio::sync::watch::Sender::new(0)), platform_reshape: Default::default(), key_offsets: Default::default(), backfill_queue: Default::default(), vector_switches: Default::default(), embed_soon: Default::default(), embed_wake: Default::default(), deferred_at: Default::default(), poisoned: Default::default(), moved: Default::default(), last_off: Default::default(), last_ts: Default::default(), forgotten: Default::default(), pins: Default::default(), retain_max: retain_max(), sub_idle: sub_idle(), timers: std::sync::Mutex::new(timers), aliases: std::sync::Mutex::new(aliases), engine: None, engine_subs: None, engine_tickets: Default::default(), engine_submitted: Default::default(), engine_committed: Arc::new(tokio::sync::watch::Sender::new(0)) }, stats))
    }

    /// Drop what the hot cache holds past its window, TTL or validity (W10).
    pub fn sweep_cache(&self) {
        if let Some(h) = self.store.as_ref().and_then(|s| s.hot()) {
            h.sweep();
        }
    }

    /// The last durable offset (tests and metrics).
    pub async fn durable_offset(&self) -> LogOffset {
        seq_lock!(self).log().durable_offset()
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
        // Every earlier write to what it reads is in Lance first. By key, only that
        // key's last write (and, for an entity, its facts'): waiting for every
        // committed record held each UPDATE / FORGET behind the applier catching up
        // with all concurrent writes, ~70-190/s flat on AWS r7 (PF-43). Anything
        // written after the snapshot is the sequencer's conflict check's.
        let t0 = std::time::Instant::now();
        let m: Memory = memory.into();
        let keyed = crate::query::keyed(memory, predicate, in_scope, namespace);
        let last_write = |ids: &mut dyn Iterator<Item = &String>| -> u64 {
            let ko = self.key_offsets.lock().unwrap();
            let at = |k: (Memory, String)| ko.map.get(&k).copied().unwrap_or(0);
            ids.map(|id| {
                let own = at((m, id.clone()));
                if forget && memory == MemoryType::Semantic { own.max(at((m, refs_key(id)))) } else { own }
            })
            .max()
            .unwrap_or(0)
        };
        let barrier = self.barrier();
        let read_from = match (&barrier, &keyed) {
            (Some(b), Some(id)) => {
                b.wait_for_applied(last_write(&mut std::iter::once(id))).await;
                b.applied_offset()
            }
            (Some(b), None) => {
                b.wait_for_committed().await;
                u64::MAX
            }
            (None, _) => u64::MAX,
        };
        let waited = t0.elapsed();
        crate::steps::add(if keyed.is_some() { "targets.a wait for key applied" } else { "targets.a wait for all applied" }, waited);
        let mut ids = crate::query::targets(store, memory, predicate, in_scope, namespace, ts).await?;
        crate::steps::add("targets.b read", t0.elapsed() - waited);
        // What it found must have been applied when it read: else wait and read again.
        if let (Some(b), true) = (&barrier, read_from != u64::MAX) {
            let need = last_write(&mut ids.iter());
            if need > read_from {
                b.wait_for_applied(need).await;
                ids = crate::query::targets(store, memory, predicate, in_scope, namespace, ts).await?;
            }
        }
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
    /// The links of the SEMANTIC records `ids` that are now aliases of other records,
    /// as new links of those records (ends re-pointed; ones they have already, or that
    /// would join a record to itself, skipped).
    async fn links_to_survivors(&self, seq: &mut Sequencer<L>, ts: CommitTs, ids: &[String]) -> AdbResult<Vec<Effect>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        let err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
        let mut out = Vec::new();
        for id in ids {
            let Some(to) = self.alias_of(id) else { continue };
            let end = adb_store::sql_str(&adb_store::row_id(Memory::Semantic, id));
            let edges = store.live(Some(&format!("kind = 'edge' AND (subject_id = {end} OR object_id = {end})")), ts).await.map_err(err)?;
            let to_row = adb_store::row_id(Memory::Semantic, &to);
            let theirs: std::collections::HashSet<(String, String, Option<String>)> = {
                let mine = adb_store::sql_str(&to_row);
                store
                    .live(Some(&format!("kind = 'edge' AND (subject_id = {mine} OR object_id = {mine})")), ts)
                    .await
                    .map_err(err)?
                    .into_iter()
                    .map(|e| (e.data.subject_id.unwrap_or_default(), e.data.object_id.unwrap_or_default(), e.data.link_type))
                    .collect()
            };
            for e in edges {
                let swap = |x: &Option<String>| x.as_deref().map(|v| if adb_store::sql_str(v) == end { to.clone() } else { adb_store::key_of(v).to_string() });
                let mut d = e.data.clone();
                d.subject_id = swap(&e.data.subject_id);
                d.object_id = swap(&e.data.object_id);
                let n = NewRow { id: seq.new_id(ts), lineage_id: RecordId(String::new()), version: 1, data: d };
                let q = adb_store::qualified(&n);
                if q.data.subject_id == q.data.object_id || theirs.contains(&(q.data.subject_id.clone().unwrap_or_default(), q.data.object_id.clone().unwrap_or_default(), q.data.link_type.clone())) {
                    continue;
                }
                out.push(Effect::PutEdge(NewRow { lineage_id: n.id.clone(), ..n }));
            }
        }
        Ok(out)
    }

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
        if !due(&self.timers) {
            return Ok(0);
        }
        let mut seq = seq_lock!(self);
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
        if effects.is_empty() {
            return Ok(0);
        }
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        self.to_store(&record).await;
        Ok(effects.len() as u64)
    }

    /// Live subscriptions (stage E).
    pub fn subscriptions(&self) -> &Arc<Registry> {
        &self.subs
    }

    /// Serve platform tables from `platform` (stage G): first apply every log record
    /// above its applied offset (a crash may have cut one short). Returns how many.
    pub async fn attach_platform(&mut self, platform: Arc<adb_platform::PlatformStore>) -> AdbResult<u64> {
        let seq = seq_lock!(self);
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
        let seq = seq_lock!(self);
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
        let mut et = t0;
        let mut seq = seq_lock!(self);
        crate::steps::lap("engine.exec.1 sequencer wait", &mut et);
        let mut h = host.lock().await;
        crate::steps::lap("engine.exec.2 host lock wait (under seq)", &mut et);
        let locked = t0.elapsed();
        let (out, effect, notices) = h.exec(space, f).await.map_err(eng)?;
        crate::steps::lap("engine.exec.3 core exec (under seq)", &mut et);
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
                        self.to_store_submitted(&record).await;
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
        crate::steps::lap("engine.exec.4 submit (under seq)", &mut et);
        let submitted = t0.elapsed();
        if let Some((off, wait, go)) = pending {
            self.committed_then(off, wait, go).await;
        }
        crate::steps::lap("engine.exec.5 commit wait", &mut et);
        if !sent {
            return Err(AdbError::Internal("the engine committer is not running".into()));
        }
        done_rx.await.unwrap_or_else(|_| Err(AdbError::Internal("engine write dropped".into())))?;
        crate::steps::lap("engine.exec.6 committer done wait", &mut et);
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
        let _t = crate::steps::timer("engine.read settle wait");
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
        let mut t = std::time::Instant::now();
        let mut h = self.engine_host()?.lock().await;
        crate::steps::lap("engine.read host lock wait", &mut t);
        let r = h.read(space, f).await.map_err(eng);
        drop(h);
        crate::steps::lap("engine.read (host locked)", &mut t);
        r
    }

    /// A run's journal after `after_seq`: from memory while the core holds it,
    /// else its EPISODIC history.
    pub async fn engine_journal(&self, space: &str, run: flowr_storage::engine::RunId, after_seq: u64) -> AdbResult<Vec<flowr_storage::engine::JournalEntry>> {
        let eng = |e: adb_engine::EngineError| AdbError::QueryError(e.to_string());
        self.engine_settled().await;
        let t = std::time::Instant::now();
        let mut h = self.engine_host()?.lock().await;
        crate::steps::add("engine.journal host lock wait", t.elapsed());
        let _held = crate::steps::timer("engine.journal (host locked)");
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
        let t = std::time::Instant::now();
        let h = self.engine_host()?.lock().await;
        crate::steps::add("engine.notify host lock wait", t.elapsed());
        h.notify(space, vec![(topic.to_string(), msg)]);
        Ok(())
    }

    /// Engine upkeep (each tick): finished runs' journals leave memory, and
    /// snapshots that are due are written.
    pub async fn engine_maintain(&self) -> AdbResult<()> {
        let Some(host) = &self.engine else { return Ok(()) };
        let _all = crate::steps::timer("engine.maintain (all)");
        let eng = |e: adb_engine::EngineError| AdbError::Internal(e.to_string());
        let spaces = host.lock().await.spaces();
        let now = chrono::Utc::now();
        for space in spaces {
            let runs = host.lock().await.evictable(&space, now).await.map_err(eng)?;
            if !runs.is_empty() {
                self.engine_exec(&space, |c| runs.iter().for_each(|r| c.evict_journal(*r))).await?;
            }
        }
        {
            let t = std::time::Instant::now();
            let mut h = host.lock().await;
            crate::steps::add("engine.maintain snapshot lock wait", t.elapsed());
            let _t = crate::steps::timer("engine.maintain snapshot (host locked)");
            h.snapshot_due(false).await.map_err(eng)?;
        }
        let due = host.lock().await.journals_to_compact().await;
        for j in due {
            let _t = crate::steps::timer("engine.maintain journal compaction");
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
    async fn to_store(&self, record: &LogRecord) {
        self.to_store_with(record, true).await
    }

    /// As [`Self::to_store`] for a record submitted but maybe not yet durable (PF-1):
    /// the applier holds it until [`Self::committed`] marks it.
    async fn to_store_submitted(&self, record: &LogRecord) {
        self.to_store_with(record, false).await
    }

    /// Wait for a submitted record's commit, outside the sequencer. A failure stops
    /// this engine's writes and asks for a restart (the leader lost the replica set).
    async fn committed(&self, offset: u64, wait: adb_log::CommitWait) -> AdbResult<()> {
        match self.commit_task(offset, wait, None).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(AdbError::Internal(format!("log: {e}"))),
            Err(e) => Err(AdbError::Internal(format!("commit wait: {e}"))),
        }
    }

    /// [`Self::committed`], then the in-order committer (engine or platform) told on
    /// `go` whether the record committed.
    async fn committed_then(&self, offset: u64, wait: adb_log::CommitWait, go: tokio::sync::oneshot::Sender<bool>) -> bool {
        matches!(self.commit_task(offset, wait, Some(go)).await, Ok(Ok(())))
    }

    /// The commit wait in a task of its own (PF-42): it marks the record durable (or
    /// stops this engine) and answers `go` whatever happens to the request that
    /// submitted it. Awaited in the request, a cancelled request (a client timeout, a
    /// reset HTTP/2 stream) dropped `go` unanswered: the committer read that as "did
    /// not commit" and refused every later write, with no log line and no restart.
    fn commit_task(&self, offset: u64, wait: adb_log::CommitWait, go: Option<tokio::sync::oneshot::Sender<bool>>) -> tokio::task::JoinHandle<Result<(), String>> {
        let (barrier, poisoned, moved) = (self.barrier(), Arc::clone(&self.poisoned), Arc::clone(&self.moved));
        tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            let w = wait.await;
            crate::steps::add("raft commit (submit -> durable)", t0.elapsed());
            let r = match w {
                Ok(_) => {
                    if let Some(b) = &barrier {
                        b.durable(offset);
                    }
                    Ok(())
                }
                Err(e) => {
                    poisoned.store(true, std::sync::atomic::Ordering::SeqCst);
                    if let Some(b) = &barrier {
                        b.abandon();
                    }
                    // A newer owner fenced this one (the partition moved): only this
                    // partition stops here. Restarting the node for it took every other
                    // partition down too (Hetzner round 6).
                    if e.to_string().contains("fenced:") {
                        moved.store(true, std::sync::atomic::Ordering::SeqCst);
                        tracing::warn!(offset, error = %e, "the partition moved to a newer owner: this engine takes no more writes; the node goes on");
                    } else {
                        tracing::error!(offset, error = %e, "a submitted record did not commit: this engine stops taking writes and restarts");
                        crate::engine::request_restart();
                    }
                    Err(e.to_string())
                }
            };
            if let Some(go) = go {
                let _ = go.send(r.is_ok());
            }
            r
        })
    }

    /// Whether a newer owner holds this partition (see `moved`).
    pub fn moved(&self) -> bool {
        self.moved.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn writable(&self) -> AdbResult<()> {
        if self.moved() {
            return Err(AdbError::QueryError("moved: this partition moved to a newer owner; retry".into()));
        }
        if self.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AdbError::Internal("this ADB engine is restarting after a write failed to commit; retry".into()));
        }
        Ok(())
    }

    async fn to_store_with(&self, record: &LogRecord, durable: bool) {
        let _whole = crate::steps::timer("to_store (all, under lock)");
        let applied = self.barrier().map_or(0, |b| b.applied_offset());
        self.key_offsets.lock().unwrap().note(record, applied);
        crate::steps::value("record effects", record.effects.len() as u64);
        self.last_off.fetch_max(record.offset.0, std::sync::atomic::Ordering::SeqCst);
        self.last_ts.fetch_max(record.commit_ts.0, std::sync::atomic::Ordering::SeqCst);
        let (Some(_), Some(applier)) = (&self.store, &self.applier) else { return };
        // TTL timers (W9.12) for records written with one.
        for e in &record.effects {
            if let Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) | Effect::Supersede { new: n, .. } = e {
                if let Some(t) = n.data.ttl_at {
                    self.timers.lock().unwrap().insert((t.0, adb_store::row_id(n.data.memory, &n.lineage_id.0)));
                }
            }
        }
        if durable {
            applier.enqueue(record.clone());
        } else {
            applier.enqueue_submitted(record.clone());
        }
    }

    /// Trim the log below what Lance holds on disk (and everything else that needs it),
    /// once `ADB_PURGE_EVERY` records have come since the last trim. Called by the
    /// maintenance tick, not on the write path: inline, it ran under the sequencer every
    /// 256 records and held every write up to 100 ms (local bench, 15 Nov). The floor is
    /// worked out without the sequencer; only the trim itself takes it.
    pub async fn purge_log(&self) -> AdbResult<()> {
        let tip = self.last_off.load(std::sync::atomic::Ordering::SeqCst);
        let since = self.purged_at.load(std::sync::atomic::Ordering::SeqCst);
        if tip < since + purge_every() {
            return Ok(());
        }
        let Some(s) = &self.store else { return Ok(()) };
        let _timed = crate::steps::timer("purge (all)");
        let mut pt = std::time::Instant::now();
        let durable_off = s.durable_offset().await;
        crate::steps::lap("purge.1 durable offset", &mut pt);
        let d = match durable_off {
            Ok(d) if d.0 > 0 => d,
            Ok(_) => return Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "Lance durable offset unavailable; log not purged");
                return Ok(());
            }
        };
        // Lance doesn't fsync on local disk (risk K8): sync first, so the records purged
        // from the log are on disk in Lance, not only cached.
        if let Err(e) = s.sync_to_disk().await {
            tracing::warn!(error = %e, "store sync failed; log not purged");
            return Ok(());
        }
        crate::steps::lap("purge.2 lance sync to disk", &mut pt);
        // Keep what subscribers haven't acked, up to `retain_max` behind the tip (E3);
        // never above Lance's durable offset.
        let mut floor = d.next();
        // With object storage: never above what it can rebuild (the lowest of the memory
        // table's, platform tables' and engine's archived offsets).
        if s.log_floor().is_some() {
            floor = LogOffset(floor.0.min(self.archive_floor.load(std::sync::atomic::Ordering::SeqCst)));
        }
        // Platform tables apply synchronously; the log keeps a platform record only if
        // its apply failed (they catch up on the next open).
        if let Some(p) = self.platform_floor().await {
            floor = LogOffset(floor.0.min(p.0));
        }
        crate::steps::lap("purge.3 platform floor", &mut pt);
        // FlowR's engine needs the records above its snapshots (stage H).
        if let Some(e) = &self.engine {
            let host = e.lock().await;
            if let Some(f) = host.floor().await {
                floor = LogOffset(floor.0.min(f.0));
            }
        }
        crate::steps::lap("purge.4 engine floor", &mut pt);
        if let Some(cursor) = self.subs.min_cursor() {
            let cap = tip.saturating_sub(self.retain_max);
            floor = LogOffset(floor.0.min(cursor.max(cap) + 1));
        }
        self.purged_at.store(tip, std::sync::atomic::Ordering::SeqCst);
        if floor.0 <= 1 {
            return Ok(());
        }
        let purged = self.log.purge_below(floor).await;
        crate::steps::lap("purge.5 log purge below", &mut pt);
        if let Err(e) = purged {
            tracing::warn!(below = floor.0, error = %e, "log purge failed; segments kept");
        }
        Ok(())
    }
}

/// How often (in log records) the log is trimmed below Lance's durable offset.
pub const PURGE_EVERY: u64 = 256;

fn purge_every() -> u64 {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::env::var("ADB_PURGE_EVERY").ok().and_then(|v| v.parse().ok()).unwrap_or(PURGE_EVERY).max(1))
}

/// Apply every log record above the table's `_meta` offset (records lost from the
/// MemWAL by a crash, or written while Lance applies failed). Returns how many.
pub async fn catch_up<L: Log>(store: &PartitionStore, log: &L) -> AdbResult<u64> {
    let store_err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
    let mut from = store.applied_offset().await.map_err(store_err)?.next();
    let mut applied = 0;
    loop {
        let batch = match log.read(from, 1024).await {
            Ok(b) => b,
            // Purged above the table's offset: the log drops records only below what
            // the table (and the platform tables and the engine) made durable, so the
            // gap held nothing the table lacks. The table's offset counts memory
            // records only: after a platform-only record it sits below a restore
            // floor, and the WAL of a partition restored from object storage starts
            // above that floor — opening it failed ("offset 2 was purged").
            Err(LogError::Purged { oldest, .. }) if oldest.0 > from.0 => {
                from = oldest;
                continue;
            }
            Err(e) => return Err(log_err(e)),
        };
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
        let mut t = std::time::Instant::now();
        // The writer doesn't wait for the vector: the text goes to the vector queue
        // after the commit ([`DurableWrites::embed_deferred`]).
        let text = match crate::reads::lance_owned(memory_type) && self.store.is_some() && self.embedder.is_some() {
            true => crate::reads::embed_text(memory_type, &payload),
            false => None,
        };
        let bare = scoped_key(None, in_scope, &store_id(memory_type, key, &payload))?;
        self.check_sealed(memory_type, namespace, &bare, None, CommitTs::from_datetime(time::now())).await?;
        let bytes = payload.to_string().len() as u64;
        self.writable()?;
        crate::steps::lap("store.2 sealed check", &mut t);
        let mut seq = seq_lock!(self);
        crate::steps::lap("store.3 sequencer lock wait", &mut t);
        let held = std::time::Instant::now();
        self.check_quota(namespace, bytes).await?;
        let ts = seq.begin();
        let id = RecordId(scoped_key(namespace, in_scope, &store_id(memory_type, key, &payload))?);
        let branch_ns = self.branch_ns(namespace);
        // A key the branch inherits can't be stored again: one keyed lookup through the
        // branch and its bases (it read every row of the branch's view per STORE).
        if let (Some(ns), true) = (branch_ns, caller_keyed(memory_type, &payload)) {
            let bare = adb_store::without_namespace(&id.0);
            if self.space_view().key_at(memory_type.into(), ns, bare, ts, ts).await?.is_some() {
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
        data.ttl_at = ttl.map(|d| CommitTs(ts.0 + d.as_micros() as i64));
        let new = NewRow { id: id.clone(), lineage_id: id, version: 1, data };
        let effect = match memory_type {
            MemoryType::Episodic => Effect::PutEpisode(new.clone()),
            MemoryType::Tools => Effect::PutTool(new.clone()),
            _ => Effect::PutEntity(new.clone()),
        };
        crate::steps::lap("store.4 checks under lock", &mut t);
        // Submitted in order under the sequencer, committed after it is released (PF-1).
        let (record, wait) = seq.submit_record(ts, None, vec![effect.clone()]).await.map_err(log_err)?;
        crate::steps::lap("store.5 raft submit (under lock)", &mut t);
        if unique_keys(memory_type) && self.store.is_some() {
            self.live.lock().unwrap().insert((memory_type.into(), new.lineage_id.0.clone()), new.data.ttl_at);
        }
        // The table is the only copy: answer with the record as reads shape it.
        let stored = crate::query::record(&adb_store::StoredRow::from_new(&adb_store::qualified(&new), ts, 0, 0));
        self.to_store_submitted(&record).await;
        crate::steps::lap("store.6 to applier (under lock)", &mut t);
        // Counted under the sequencer, so concurrent STOREs can't both pass the quota.
        self.note_stored(namespace, bytes).await;
        drop(seq);
        crate::steps::add("store.L lock held", held.elapsed());
        crate::steps::lap("store.7 quota note + release", &mut t);
        self.committed(record.offset.0, wait).await?;
        crate::steps::lap("store.8 commit wait", &mut t);
        if let Some(text) = text {
            self.embed_later(memory_type.into(), new.lineage_id.0.clone(), text);
        }
        Ok(stored)
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        // New `content` gets a new vector (T5), after the commit: the writer doesn't wait.
        let branch_ns = self.branch_ns(namespace);
        let deferred = match crate::reads::lance_owned(memory_type) && self.store.is_some() && self.embedder.is_some() {
            true => data.get("content").and_then(|c| c.as_str()).filter(|t| !t.trim().is_empty()).map(String::from),
            false => None,
        };
        self.writable()?;
        if branch_ns.is_none() && self.store.is_some() {
            if crate::query::keyed(memory_type, predicate, in_scope, namespace).is_some() {
                if let Some(embed_ids) = self.update_prepared(memory_type, predicate, &data, in_scope, namespace).await? {
                    self.embed_ids_later(memory_type, &embed_ids, deferred.as_deref());
                    return Ok(embed_ids.len() as u64);
                }
            } else {
                slow_write_allowed(memory_type, "UPDATE")?;
            }
        }
        let t0 = std::time::Instant::now();
        let mut seq = seq_lock!(self);
        tracing::debug!(target: "adb::write", ?memory_type, lock_us = t0.elapsed().as_micros() as u64, "update locked");
        let ts = seq.begin();
        if branch_ns.is_some() {
            let fields = match &data {
                serde_json::Value::Object(map) => map.clone(),
                other => {
                    let mut m = serde_json::Map::new();
                    m.insert("value".into(), other.clone());
                    m
                }
            };
            let ids = self.branch_write(&mut seq, ts, memory_type, predicate, in_scope, &fields, branch_ns).await?;
            drop(seq);
            self.embed_ids_later(memory_type, &ids, deferred.as_deref());
            return Ok(ids.len() as u64);
        }
        let ids = self.targets(memory_type, predicate, ts, false, in_scope, namespace).await?;
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
            let mut embed_ids = Vec::with_capacity(ids.len());
            for key in &ids {
                let cur = store
                    .current_version(memory_type.into(), key, ts)
                    .await
                    .map_err(|e| AdbError::Internal(format!("store: {e}")))?
                    .ok_or_else(|| AdbError::Internal(format!("{memory_type:?} {key}: no current version in Lance")))?;
                let mut new = next_version(&cur, &fields, seq.new_id(ts));
                embed_ids.push(new.id.0.clone());
                if let (Kind::Fact, (Some(from), Some(to))) = (new.data.kind, time::validity(&new.data.value)) {
                    if from >= to {
                        return Err(AdbError::InvalidSyntax(format!("{key}: valid_from must be before valid_to")));
                    }
                }
                effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new });
            }
            let resolved = t0.elapsed();
            let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
            self.to_store(&record).await;
            tracing::debug!(target: "adb::write", ?memory_type, held_us = resolved.as_micros() as u64, log_us = (t0.elapsed() - resolved).as_micros() as u64, "update committed");
            self.embed_ids_later(memory_type, &embed_ids, deferred.as_deref());
            return Ok(effects.len() as u64);
        }
        let effects: Vec<Effect> = ids
            .iter()
            .map(|id| Effect::Patch { memory: memory_type.into(), id: RecordId(id.clone()), fields: fields.clone(), version: 0 })
            .collect();
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        self.to_store(&record).await;
        self.embed_ids_later(memory_type, &ids, deferred.as_deref());
        Ok(ids.len() as u64)
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        self.writable()?;
        let branch_ns = self.branch_ns(namespace);
        if self.store.is_some() && crate::query::keyed(memory_type, predicate, in_scope, namespace).is_none() {
            slow_write_allowed(memory_type, "FORGET")?;
        }
        if branch_ns.is_none() && self.store.is_some() && crate::query::keyed(memory_type, predicate, in_scope, namespace).is_some() {
            if let Some(n) = self.forget_prepared(memory_type, predicate, in_scope, namespace).await? {
                return Ok(n);
            }
        }
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        // In a branch namespace: a delete marker hides the record on the branch; the
        // base keeps it (erasing data for good is FORGET on the base).
        if branch_ns.is_some() {
            let mut fields = serde_json::Map::new();
            fields.insert("_branch_deleted".into(), serde_json::Value::Bool(true));
            return Ok(self.branch_write(&mut seq, ts, memory_type, predicate, in_scope, &fields, branch_ns).await?.len() as u64);
        }
        let mut ids = self.targets(memory_type, predicate, ts, true, in_scope, namespace).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        if memory_type == MemoryType::Semantic {
            ids.extend(self.entity_facts(&ids, in_scope).await?);
        }
        // AQL v0.13: a record whose key is now another record's alias leaves its links
        // to that record (they named it; the alias reaches it) — in the same write.
        let mut effects: Vec<Effect> = Vec::new();
        if memory_type == MemoryType::Semantic {
            effects.extend(self.links_to_survivors(&mut seq, ts, &ids).await?);
        }
        effects.extend(ids.iter().map(|id| Effect::Forget { memory: memory_type.into(), lineage_id: RecordId(id.clone()) }));
        self.usage_stale().await;
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        self.keys_gone(&effects);
        self.note_forgotten(ids.iter().map(|id| (Memory::from(memory_type), id.clone())));
        let removed = ids.len() as u64;
        self.to_store(&record).await;
        drop(seq);
        // Hidden from every read from the commit on; the bytes leave disk at the next
        // compaction (user, 7 Nov: no read may miss a FORGET; disk removal can wait).
        Ok(removed)
    }

    async fn aliases_changed(&self) -> AdbResult<()> {
        let Some(store) = &self.store else { return Ok(()) };
        if let Some(b) = self.barrier() {
            b.wait_for_committed().await;
        }
        let fresh = load_aliases(store).await?;
        *self.aliases.lock().unwrap() = fresh;
        Ok(())
    }

    fn alias_of(&self, key: &str) -> Option<String> {
        let aliases = self.aliases.lock().unwrap();
        if aliases.is_empty() {
            return None;
        }
        aliases.get(key).cloned()
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
        self.link_with(from_type, from_id, to_type, to_id, link_type, weight, true).await
    }

    async fn link_remote(&self, from_type: MemoryType, from_id: &str, to_type: MemoryType, to_id: &str, link_type: &str, weight: Option<f32>) -> AdbResult<Link> {
        self.link_with(from_type, from_id, to_type, to_id, link_type, weight, false).await
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
        let mut seq = seq_lock!(self);
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
        self.to_store(&record).await;
        Ok(n)
    }

    async fn unlink(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>) -> AdbResult<u64> {
        let Some(store) = &self.store else { return Err(needs_table("UNLINK")) };
        let mut seq = seq_lock!(self);
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
        self.to_store(&record).await;
        Ok(n)
    }

    async fn publish(&self, topic: &str, payload: serde_json::Value, ttl: Option<Duration>) -> AdbResult<(String, u64)> {
        let mut seq = seq_lock!(self);
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
        self.to_store(&record).await;
        Ok((id.0, record.offset.0))
    }

    async fn subscribe(&self, spec: SubSpec, from: Option<u64>) -> AdbResult<(String, u64)> {
        let mut seq = seq_lock!(self);
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
        self.to_store(&record).await;
        Ok((id.0, cursor + 1))
    }

    async fn ack(&self, subscription: &str, offset: u64) -> AdbResult<u64> {
        let mut seq = seq_lock!(self);
        let sub = self.subs.get(subscription).ok_or_else(|| unknown_sub(subscription))?;
        let offset = offset.min(seq.log().last_offset().0);
        if offset <= sub.cursor {
            return Ok(sub.cursor);
        }
        let ts = seq.begin();
        let effect = Effect::UpdateSubCursor { sub_id: RecordId(subscription.to_string()), cursor: LogOffset(offset) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.subs.apply(&record);
        self.to_store(&record).await;
        Ok(offset)
    }

    async fn subscription_topic(&self, subscription: &str) -> AdbResult<Option<String>> {
        Ok(self.subs.get(subscription).and_then(|s| match s.spec.target {
            adb_gateway::Target::Topic(t) => Some(t),
            adb_gateway::Target::Scope(_) => None,
        }))
    }

    async fn unsubscribe(&self, subscription: &str) -> AdbResult<bool> {
        let mut seq = seq_lock!(self);
        if self.subs.get(subscription).is_none() {
            return Ok(false);
        }
        let ts = seq.begin();
        let effect = Effect::Unregister { id: RecordId(subscription.to_string()) };
        let record = seq.append_record(ts, None, vec![effect]).await.map_err(log_err)?;
        self.subs.apply(&record);
        self.to_store(&record).await;
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
                seq = seq_timed(&self.seq, &self.seq_holder, concat!("seq.wait @", line!()), concat!("seq.held @", line!())) => {
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
                        self.committed_then(off, wait, go).await;
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

/// An entity's `aliases` (GR-12): the keys of the entities merged into it.
/// Entity aliases (GR-12) from the live entities' `aliases` fields: alias → entity.
pub(crate) async fn load_aliases(store: &PartitionStore) -> AdbResult<std::collections::HashMap<String, String>> {
    Ok(store
        .live(Some("kind = 'entity'"), CommitTs::from_datetime(time::now()))
        .await
        .map_err(|e| AdbError::Internal(format!("store: {e}")))?
        .iter()
        // By record id: the alias in its entity's namespace.
        .flat_map(|r| aliases_of(&r.data.value).into_iter().map(move |a| (adb_store::namespaced(adb_store::namespace_of(r.key()), &a), r.key().to_string())))
        .collect())
}

fn aliases_of(value: &serde_json::Value) -> Vec<String> {
    value.get("aliases").and_then(|a| a.as_array()).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect()).unwrap_or_default()
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


impl<L: Log + 'static> DurableWrites<L> {
    /// `LINK`: both ends checked under the sequencer (`check_target`), or only the
    /// source (a target in another partition, [`Writes::link_remote`]).
    #[allow(clippy::too_many_arguments)]
    async fn link_with(
        &self,
        from_type: MemoryType,
        from_id: &str,
        to_type: MemoryType,
        to_id: &str,
        link_type: &str,
        weight: Option<f32>,
        check_target: bool,
    ) -> AdbResult<Link> {
        let mut seq = seq_lock!(self);
        let ts = seq.begin();
        // Both ends must exist when the link commits (L4, W9): checked under the
        // sequencer, after every earlier write is applied, so a FORGET racing this
        // LINK can't leave it pointing at nothing.
        if let Some(store) = &self.store {
            if let Some(b) = self.barrier() {
                b.wait_for_committed().await;
            }
            for (end, memory, id) in [("source", from_type, from_id), ("target", to_type, to_id)].into_iter().filter(|(end, _, _)| check_target || *end == "source") {
                let key = adb_core::Predicate::Key { field: "id".into(), value: adb_core::Value::String(id.to_string()) };
                // A branch namespace's end may be inherited from its base (AQL v0.13):
                // the branch's view, as its reads see the ends.
                let branch_ns = self.branch_ns(adb_store::namespace_of(id));
                let missing = if branch_ns.is_some() {
                    self.branch_targets_in(memory, &key, None, ts, branch_ns).await?.is_empty()
                } else {
                    crate::query::read(store, memory, &key, &Default::default(), ts).await?.is_empty()
                };
                if missing {
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
                self.to_store(&record).await;
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
        self.to_store(&record).await;
        Ok(link)
    }
}

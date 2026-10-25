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
    ) -> AdbResult<MemoryRecord>;

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value) -> AdbResult<u64>;

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate) -> AdbResult<u64>;

    async fn link(
        &self,
        from_type: MemoryType,
        from_id: &str,
        to_type: MemoryType,
        to_id: &str,
        link_type: &str,
        weight: f32,
    ) -> AdbResult<Link>;
}

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
    ) -> AdbResult<MemoryRecord> {
        self.0.store_with_options(memory_type, key, payload, scope, namespace, ttl).await
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value) -> AdbResult<u64> {
        self.0.update(memory_type, predicate, data).await
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate) -> AdbResult<u64> {
        self.0.forget(memory_type, predicate).await
    }

    async fn link(
        &self,
        from_type: MemoryType,
        from_id: &str,
        to_type: MemoryType,
        to_id: &str,
        link_type: &str,
        weight: f32,
    ) -> AdbResult<Link> {
        self.0.link(from_type, from_id, to_type, to_id, link_type, weight).await
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
}

impl<L: Log + 'static> DurableWrites<L> {
    /// Replay `log` into `adb`, bring `store` up to the log's end, then accept new
    /// writes on top of both.
    pub async fn open(adb: Arc<Adb>, mut seq: Sequencer<L>, store: Option<Arc<PartitionStore>>) -> AdbResult<(Self, ReplayStats)> {
        let stats = match &store {
            // Stage D3: no full log replay. Lance catches up from the log's tail, then
            // the in-memory tier is rebuilt from Lance's current rows.
            Some(s) => {
                let store_caught_up = catch_up(s, seq.log()).await?;
                let hydrated_rows = hydrate(&adb, s).await?;
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
        Ok((Self { adb, seq: Mutex::new(seq), store, applier }, stats))
    }

    /// The last durable offset (tests and metrics).
    pub async fn durable_offset(&self) -> LogOffset {
        self.seq.lock().await.log().durable_offset()
    }

    /// The Lance table, if any (tests and D3 reads).
    pub fn store(&self) -> Option<&Arc<PartitionStore>> {
        self.store.as_ref()
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
                Ok(d) if d.0 > 0 => {
                    if let Err(e) = log.purge_below(d.next()).await {
                        tracing::warn!(below = d.0 + 1, error = %e, "log purge failed; segments kept");
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
    ) -> AdbResult<MemoryRecord> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let id = RecordId(Adb::store_id(memory_type, key, &payload));
        let mut data = row(memory_type, kind_of(memory_type), payload);
        data.visibility = scope.into();
        data.namespace = namespace.map(String::from);
        data.ttl_at = ttl.map(|d| CommitTs(ts.0 + d.as_micros() as i64));
        let new = NewRow { id: id.clone(), lineage_id: id, version: 1, data };
        let effect = match memory_type {
            MemoryType::Episodic => Effect::PutEpisode(new),
            MemoryType::Tools => Effect::PutTool(new),
            _ => Effect::PutEntity(new),
        };
        let record = seq.append_record(ts, None, vec![effect.clone()]).await.map_err(log_err)?;
        let stored = apply(&self.adb, ts, &effect).await?.ok_or_else(|| AdbError::Internal("store produced no record".into()))?;
        self.to_store(&record, seq.log()).await;
        Ok(stored)
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: serde_json::Value) -> AdbResult<u64> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let ids = time::at(ts.to_datetime(), self.adb.update_targets(memory_type, predicate)).await?;
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
            // Every earlier record is in Lance first, so the current versions read
            // here are the ones this update follows.
            if let Some(b) = self.barrier() {
                b.wait_for_committed().await;
            }
            let mut effects = Vec::with_capacity(ids.len());
            for key in &ids {
                let cur = store
                    .current_version(memory_type.into(), key, ts)
                    .await
                    .map_err(|e| AdbError::Internal(format!("store: {e}")))?
                    .ok_or_else(|| AdbError::Internal(format!("{memory_type:?} {key}: no current version in Lance")))?;
                effects.push(Effect::Supersede { old: RecordId(adb_store::key_of(&cur.id).to_string()), new: next_version(&cur, &fields, seq.new_id(ts)) });
            }
            let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
            for effect in &effects {
                apply(&self.adb, ts, effect).await?;
            }
            self.to_store(&record, seq.log()).await;
            return Ok(effects.len() as u64);
        }
        let effects: Vec<Effect> = ids
            .iter()
            .map(|id| Effect::Patch { memory: memory_type.into(), id: RecordId(id.clone()), fields: fields.clone(), version: 0 })
            .collect();
        let record = seq.append_record(ts, None, effects).await.map_err(log_err)?;
        // Apply exactly what was logged, so live and replayed updates agree.
        let n = time::at(ts.to_datetime(), self.adb.update_ids(memory_type, &ids, serde_json::Value::Object(fields))).await?;
        self.to_store(&record, seq.log()).await;
        Ok(n)
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate) -> AdbResult<u64> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let ids = time::at(ts.to_datetime(), self.adb.forget_targets(memory_type, predicate)).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let effects: Vec<Effect> =
            ids.iter().map(|id| Effect::Forget { memory: memory_type.into(), lineage_id: RecordId(id.clone()) }).collect();
        let record = seq.append_record(ts, None, effects.clone()).await.map_err(log_err)?;
        let mut removed = 0;
        for effect in &effects {
            removed += apply_count(&self.adb, ts, effect).await?;
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
        weight: f32,
    ) -> AdbResult<Link> {
        let mut seq = self.seq.lock().await;
        let ts = seq.begin();
        let id = seq.new_id(ts);
        let to: Memory = to_type.into();
        let mut data = row(from_type, Kind::Edge, serde_json::json!({ "to_memory": to }));
        data.key = None;
        data.text = None;
        data.subject_id = Some(from_id.into());
        data.predicate = Some("link".into());
        data.object_id = Some(to_id.into());
        data.link_type = Some(link_type.into());
        data.weight = Some(weight);
        let effect = Effect::PutEdge(NewRow { id: id.clone(), lineage_id: id, version: 1, data });
        let record = seq.append_record(ts, None, vec![effect.clone()]).await.map_err(log_err)?;
        let link = apply_link(&self.adb, ts, &effect).await?;
        self.to_store(&record, seq.log()).await;
        Ok(link)
    }
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

/// Rebuild the in-memory backends from the Lance table's current rows (stage D3),
/// so startup needs only the log's tail, not all of it. Records go first, then the
/// links that point at them. WORKING / SEMANTIC / EPISODIC records are put in place
/// as rebuilt; TOOLS and PROCEDURAL re-run their `store` at the commit time (their
/// backends shape the payload into their own structs), then get their version back.
pub async fn hydrate(adb: &Adb, store: &PartitionStore) -> AdbResult<u64> {
    let store_err = |e: adb_store::StoreError| AdbError::Internal(format!("store: {e}"));
    let now = CommitTs::from_datetime(time::now());
    let rows = store.current(None, now).await.map_err(store_err)?;
    let (edges, records): (Vec<_>, Vec<_>) = rows.into_iter().partition(|r| r.data.kind == Kind::Edge);
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
    for e in &edges {
        // Back to the log's ids: the table qualifies them by memory type.
        let mut data = e.data.clone();
        data.subject_id = data.subject_id.as_deref().map(|s| adb_store::key_of(s).to_string());
        data.object_id = data.object_id.as_deref().map(|s| adb_store::key_of(s).to_string());
        let effect = Effect::PutEdge(NewRow {
            id: RecordId(adb_store::key_of(&e.id).to_string()),
            lineage_id: RecordId(adb_store::key_of(&e.lineage_id).to_string()),
            version: e.version,
            data,
        });
        apply_link(adb, e.commit_ts, &effect).await?;
    }
    Ok((records.len() + edges.len()) as u64)
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

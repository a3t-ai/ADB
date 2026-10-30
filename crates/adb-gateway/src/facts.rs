//! The fact feed (GR-15): SEMANTIC facts as (subject, predicate, object / value,
//! confidence, source, version) rows, in log order, for a consumer that keeps a copy
//! (the storage side of a KBLaM-style knowledge base, `adb-goals`).
//!
//! A consumer reads a **snapshot** (the facts in the current view, and the offset it
//! is as of), then the **changes** after it: `add` (a fact entered the current view:
//! stored, replaced by a new version, or its `valid_from` passed) and `remove` (it
//! left: replaced, forgotten, expired, or its `valid_to` passed). Keyed by `id` (the
//! fact's key): an `add` replaces what the consumer holds for that id; a `remove`
//! drops it, and removing what the consumer doesn't hold is a no-op.
//!
//! Whether a logged fact is in the current view is judged at its record's commit
//! time, so reading an offset twice gives the same rows; a fact that starts holding
//! later enters with its `begin` record (AQL v0.10). When `from` is no longer in the
//! log the answer is a snapshot again: the consumer drops its copy and takes it.

use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Kind, LogOffset, LogRecord, Memory, RowData};
use adb_log::LogError;
use adb_store::StoredRow;

use crate::{Gateway, GatewayError, Lineages};

/// Which facts a consumer follows, and where it is.
#[derive(Debug, Clone, Default)]
pub struct FactQuery {
    /// The first offset to read; `None`: a snapshot.
    pub from: Option<u64>,
    pub scope: Option<String>,
    pub namespace: Option<String>,
    /// Changes per read (whole log records: a read may go a little over).
    pub max: usize,
    /// How long to wait for the first change.
    pub wait: Duration,
}

/// One row of the feed.
#[derive(Debug, Clone, PartialEq)]
pub struct FactChange {
    /// `add` or `remove`.
    pub op: &'static str,
    /// The log offset that made the change (in a snapshot: that wrote the version).
    pub offset: u64,
    pub commit_ts: CommitTs,
    /// The fact's key (its lineage).
    pub id: String,
    /// The version added or removed (`None`: whichever the consumer holds).
    pub version: Option<u32>,
    pub scope: String,
    /// The rest is set on `add` only.
    pub fact: Option<Fact>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fact {
    pub namespace: Option<String>,
    pub subject: String,
    pub predicate: String,
    pub object: Option<String>,
    /// The payload's `value` (for a fact with a value, not an object).
    pub value: Option<serde_json::Value>,
    pub confidence: f32,
    pub source: Option<String>,
    pub agent: Option<String>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    /// The whole payload.
    pub data: serde_json::Value,
}

/// One read of the feed, and where the next starts.
#[derive(Debug, Clone, Default)]
pub struct FactBatch {
    pub changes: Vec<FactChange>,
    /// True when `changes` is a snapshot (every `add`): replace the copy with it.
    pub snapshot: bool,
    /// Pass as `from` next time.
    pub next: u64,
    /// The applied tip when the read returned.
    pub tip: u64,
}

/// The fact in `d`, with bare entity keys: the table qualifies ids by memory type
/// (and so does a version an `UPDATE` made from a stored row), and a named scope's
/// ids carry the scope (E-L12; the change has its own `scope`).
fn fact_of(d: &RowData) -> Fact {
    let s = |k: &str| d.value.get(k).and_then(|v| v.as_str()).map(String::from);
    let bare = |id: &Option<String>| id.as_deref().map(|i| adb_store::unscoped(adb_store::key_of(i)).to_string());
    Fact {
        namespace: d.namespace.clone(),
        subject: bare(&d.subject_id).unwrap_or_default(),
        predicate: d.predicate.clone().unwrap_or_default(),
        object: bare(&d.object_id),
        value: d.value.get("value").cloned(),
        confidence: d.weight.unwrap_or(1.0),
        source: d.provenance.source.clone(),
        agent: d.provenance.agent_id.clone(),
        valid_from: s("valid_from"),
        valid_to: s("valid_to"),
        data: d.value.clone(),
    }
}

fn holds(d: &RowData, t: CommitTs) -> bool {
    let (from, to) = adb_core::time::validity(&d.value);
    from.is_none_or(|f| f <= t.0) && to.is_none_or(|u| u > t.0)
}

fn wanted(q: &FactQuery, scope: &str, namespace: Option<&str>) -> bool {
    q.scope.as_deref().is_none_or(|s| s == scope) && q.namespace.as_deref().is_none_or(|n| namespace == Some(n))
}

fn add(offset: u64, ts: CommitTs, id: &str, version: u32, d: &RowData) -> FactChange {
    FactChange { op: "add", offset, commit_ts: ts, id: id.to_string(), version: Some(version), scope: d.scope.clone(), fact: Some(fact_of(d)) }
}

fn remove(offset: u64, ts: CommitTs, id: &str, version: Option<u32>, scope: &str) -> FactChange {
    FactChange { op: "remove", offset, commit_ts: ts, id: id.to_string(), version, scope: scope.to_string(), fact: None }
}

/// A current row of the table as a snapshot `add`.
pub fn snapshot_row(r: &StoredRow) -> FactChange {
    add(r.log_offset, r.commit_ts, r.key(), r.version, &r.data)
}

/// The fact changes of `record` that `q` follows.
pub async fn fact_changes(q: &FactQuery, record: &LogRecord, lineages: &dyn Lineages) -> Vec<FactChange> {
    let (offset, ts) = (record.offset.0, record.commit_ts);
    let mut out = Vec::new();
    for effect in &record.effects {
        match effect {
            Effect::PutFact(n) if wanted(q, &n.data.scope, n.data.namespace.as_deref()) && holds(&n.data, ts) => {
                out.push(add(offset, ts, &n.lineage_id.0, n.version, &n.data));
            }
            Effect::Supersede { new: n, .. } if n.data.kind == Kind::Fact && wanted(q, &n.data.scope, n.data.namespace.as_deref()) => {
                let key = &n.lineage_id.0;
                out.push(remove(offset, ts, key, n.version.checked_sub(1), &n.data.scope));
                if holds(&n.data, ts) {
                    out.push(add(offset, ts, key, n.version, &n.data));
                }
            }
            Effect::Forget { memory: Memory::Semantic, lineage_id } | Effect::Expire { memory: Memory::Semantic, lineage_id } => {
                if let Some((scope, Kind::Fact)) = lineages.lineage(Memory::Semantic, &lineage_id.0).await {
                    if q.scope.as_deref().is_none_or(|s| s == scope) {
                        out.push(remove(offset, ts, &lineage_id.0, None, &scope));
                    }
                }
            }
            Effect::Validity { memory: Memory::Semantic, lineage_id, holds: true } => {
                // The version that held when the bound passed (a later one arrives
                // with its own record).
                if let Some(r) = lineages.fact_at(&lineage_id.0, ts).await {
                    if wanted(q, &r.data.scope, r.data.namespace.as_deref()) {
                        out.push(add(offset, ts, &lineage_id.0, r.version, &r.data));
                    }
                }
            }
            Effect::Validity { memory: Memory::Semantic, lineage_id, holds: false } => {
                if let Some((scope, Kind::Fact)) = lineages.lineage(Memory::Semantic, &lineage_id.0).await {
                    if q.scope.as_deref().is_none_or(|s| s == scope) {
                        out.push(remove(offset, ts, &lineage_id.0, None, &scope));
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Records read from the log per call.
const READ_BATCH: usize = 256;

impl Gateway {
    /// One read of the fact feed (GR-15).
    pub async fn facts(&self, q: &FactQuery) -> Result<FactBatch, GatewayError> {
        let mut applied = self.applied.clone();
        let Some(from) = q.from.filter(|f| *f > 0) else {
            let tip = *applied.borrow_and_update();
            return self.fact_snapshot(q, tip).await;
        };
        let mut next = from;
        let deadline = tokio::time::Instant::now() + q.wait;
        loop {
            let tip = *applied.borrow_and_update();
            let mut out = Vec::new();
            while next <= tip && out.len() < q.max {
                let n = ((tip - next + 1) as usize).min(READ_BATCH);
                let records = match self.log.read(LogOffset(next), n).await {
                    Ok(r) => r,
                    // Past log retention: start again from the table.
                    Err(LogError::Purged { .. }) => return self.fact_snapshot(q, tip).await,
                    Err(e) => return Err(GatewayError::Log(e.to_string())),
                };
                if records.is_empty() {
                    break;
                }
                for r in &records {
                    next = r.offset.0 + 1;
                    out.extend(fact_changes(q, r, self.lineages.as_ref()).await);
                    if out.len() >= q.max {
                        break;
                    }
                }
            }
            if !out.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok(FactBatch { changes: out, snapshot: false, next, tip });
            }
            if tokio::time::timeout_at(deadline, applied.changed()).await.is_err() {
                let tip = *applied.borrow();
                return Ok(FactBatch { changes: Vec::new(), snapshot: false, next, tip });
            }
        }
    }

    /// The facts in the current view, as of applied offset `tip` (or a little
    /// later: the changes after `tip` may repeat some, which is harmless).
    async fn fact_snapshot(&self, q: &FactQuery, tip: u64) -> Result<FactBatch, GatewayError> {
        let rows = self.lineages.current_facts(q.scope.as_deref(), q.namespace.as_deref()).await.map_err(GatewayError::Log)?;
        let mut changes: Vec<FactChange> = rows.iter().map(snapshot_row).collect();
        changes.sort_by_key(|c| c.offset);
        Ok(FactBatch { changes, snapshot: true, next: tip + 1, tip })
    }
}

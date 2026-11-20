//! Log records and effects: what the sequencer commits and the applier replays.
//!
//! Every mutation is a [`LogRecord`] on one partition's log (ARCHITECTURE.md §9); the
//! table is a materialized view of it. Effects follow D2_TEMPORAL.md §6, row fields
//! follow D3_SCHEMA.md §1. How records are encoded and encrypted on disk is the log
//! implementation's job (`adb-log`).

use std::fmt;

use chrono::{DateTime, TimeZone, Utc};
use half::f16;
use serde::{Deserialize, Serialize};

/// Number of partitions (ARCHITECTURE.md §6; decision D1 still open).
pub const PARTITIONS: u16 = 4096;

/// A partition: `hash(tenant_id, scope) mod PARTITIONS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionId(pub u16);

/// Position of a record in its partition's log. Offsets start at 1; 0 means "nothing".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct LogOffset(pub u64);

impl LogOffset {
    pub const ZERO: LogOffset = LogOffset(0);

    pub fn next(self) -> LogOffset {
        LogOffset(self.0 + 1)
    }
}

/// Owner epoch of a partition; a stale owner's writes are fenced by it (§11.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Epoch(pub u64);

/// Transaction time: the sequencer's commit timestamp in microseconds since the Unix
/// epoch (D2 T2). Strictly increasing on one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitTs(pub i64);

impl CommitTs {
    pub fn from_datetime(t: DateTime<Utc>) -> Self {
        CommitTs(t.timestamp_micros())
    }

    pub fn to_datetime(self) -> DateTime<Utc> {
        Utc.timestamp_micros(self.0).single().unwrap_or_default()
    }

    /// Milliseconds, the resolution of a ULID's time part.
    pub fn millis(self) -> u64 {
        self.0.div_euclid(1000).max(0) as u64
    }
}

/// A row id or lineage id: a ULID as its 26-character string (D3 S1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordId(pub String);

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Generates ULIDs whose time part is the commit timestamp, monotonic within one
/// millisecond, so ids sort in log order on one partition.
#[derive(Default)]
pub struct IdGen {
    inner: ulid::Generator,
}

impl fmt::Debug for IdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IdGen")
    }
}

impl IdGen {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next(&mut self, at: CommitTs) -> RecordId {
        let time = std::time::UNIX_EPOCH + std::time::Duration::from_millis(at.millis());
        let id = self
            .inner
            .generate_from_datetime(time)
            // Overflow means 2^80 ids in one millisecond; fall back to a fresh one.
            .unwrap_or_else(|_| ulid::Ulid::from_datetime(time));
        RecordId(id.to_string())
    }
}

/// AQL memory types (`FROM` / `INTO` target).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Memory {
    Working,
    Tools,
    Procedural,
    Semantic,
    Episodic,
}

impl Memory {
    /// The snake_case name, as serialized and as stored in the `memory` column.
    pub fn tag(self) -> &'static str {
        match self {
            Memory::Working => "working",
            Memory::Tools => "tools",
            Memory::Procedural => "procedural",
            Memory::Semantic => "semantic",
            Memory::Episodic => "episodic",
        }
    }

    pub fn from_tag(tag: &str) -> Option<Self> {
        [Memory::Working, Memory::Tools, Memory::Procedural, Memory::Semantic, Memory::Episodic]
            .into_iter()
            .find(|m| m.tag() == tag)
    }
}

impl From<crate::MemoryType> for Memory {
    fn from(t: crate::MemoryType) -> Self {
        match t {
            crate::MemoryType::Working => Memory::Working,
            crate::MemoryType::Tools => Memory::Tools,
            crate::MemoryType::Procedural => Memory::Procedural,
            crate::MemoryType::Semantic => Memory::Semantic,
            crate::MemoryType::Episodic => Memory::Episodic,
        }
    }
}

impl From<Memory> for crate::MemoryType {
    fn from(m: Memory) -> Self {
        match m {
            Memory::Working => crate::MemoryType::Working,
            Memory::Tools => crate::MemoryType::Tools,
            Memory::Procedural => crate::MemoryType::Procedural,
            Memory::Semantic => crate::MemoryType::Semantic,
            Memory::Episodic => crate::MemoryType::Episodic,
        }
    }
}

/// Row shape (D3 §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Entity,
    Fact,
    Edge,
    Episode,
    Tool,
    Event,
    Sub,
    Cq,
    Meta,
    /// A memory branch (agents as repos, B1): a control row, never a record.
    Branch,
    /// The tombstone log: one row per FORGET or expiry, written with the tombstones it
    /// records and kept forever (physical deletion is only by a manual purge). Never a
    /// record: reads of memory skip it (`RECORD_KINDS`).
    TombLog,
}

/// AQL v0.5 `SCOPE private | shared | cluster`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    #[default]
    Private,
    Shared,
    Cluster,
}

impl From<crate::Scope> for Visibility {
    fn from(s: crate::Scope) -> Self {
        match s {
            crate::Scope::Private => Visibility::Private,
            crate::Scope::Shared => Visibility::Shared,
            crate::Scope::Cluster => Visibility::Cluster,
        }
    }
}

impl From<Visibility> for crate::Scope {
    fn from(v: Visibility) -> Self {
        match v {
            Visibility::Private => crate::Scope::Private,
            Visibility::Shared => crate::Scope::Shared,
            Visibility::Cluster => crate::Scope::Cluster,
        }
    }
}

/// Who wrote a row, and in which transaction.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Provenance {
    pub agent_id: Option<String>,
    pub source: Option<String>,
    pub txn_id: Option<String>,
}

/// The fields a writer supplies for a row (D3 §1). The engine adds `id`,
/// `lineage_id`, `version`, `commit_ts`, `log_offset` and `epoch`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowData {
    pub tenant_id: String,
    pub scope: String,
    pub visibility: Visibility,
    pub namespace: Option<String>,
    pub memory: Memory,
    pub kind: Kind,
    pub key: Option<String>,
    pub subject_id: Option<String>,
    pub predicate: Option<String>,
    pub object_id: Option<String>,
    pub link_type: Option<String>,
    pub weight: Option<f32>,
    /// The payload: a JSON object, plaintext (D3 S2).
    pub value: serde_json::Value,
    /// Sealed fields, already encrypted with the tenant key (D3 S2).
    pub sealed: Option<Vec<u8>>,
    /// Copy of the payload's `content` (D3 S3).
    pub text: Option<String>,
    /// Embedding of `text`, f16 in the log (S9); f32 in the table.
    pub embedding: Option<Vec<f16>>,
    pub ttl_at: Option<CommitTs>,
    pub provenance: Provenance,
}

/// A new row version: engine-assigned identity plus the writer's fields. Ids are
/// assigned before the record is logged, so replay recreates the same rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewRow {
    pub id: RecordId,
    pub lineage_id: RecordId,
    pub version: u32,
    pub data: RowData,
}

/// One change inside a log record (D2 §6, SUBSCRIPTIONS_CQ_SPEC.md).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case")]
pub enum Effect {
    /// `STORE INTO WORKING / SEMANTIC / PROCEDURAL` (flat AQL payloads, D3 S7).
    PutEntity(NewRow),
    /// A (subject, predicate, object) fact from the SDK / MCP triple calls (D3 S7).
    PutFact(NewRow),
    /// `STORE INTO EPISODIC`.
    PutEpisode(NewRow),
    /// `STORE INTO TOOLS`.
    PutTool(NewRow),
    /// `LINK`: an edge between lineages.
    PutEdge(NewRow),
    /// `PUBLISH TOPIC`.
    PublishEvent(NewRow),
    /// `UPDATE` on SEMANTIC / PROCEDURAL: close `old` and add `new` in one step.
    Supersede { old: RecordId, new: NewRow },
    /// `UPDATE` on EPISODIC / WORKING / TOOLS: merge `fields` into the row in place.
    /// (Stage C also patches SEMANTIC / PROCEDURAL; `Supersede` arrives with stage D.)
    Patch {
        memory: Memory,
        id: RecordId,
        fields: serde_json::Map<String, serde_json::Value>,
        version: u32,
    },
    /// `FORGET`: tombstone every version of the lineage and its edges (D2 §5).
    Forget { memory: Memory, lineage_id: RecordId },
    /// TTL expiry, committed by the owner's timer (D2 §4).
    Expire { memory: Memory, lineage_id: RecordId },
    /// A fact's `valid_from` (`holds`) or `valid_to` (not `holds`) passed (AQL
    /// v0.10, GR-14), committed by the owner's timer. Changes no row — reads test
    /// validity themselves — so subscribers and continuous queries see the boundary.
    Validity { memory: Memory, lineage_id: RecordId, holds: bool },
    /// `SUBSCRIBE` / `WATCH` registration (`kind = sub | cq` row).
    Register(NewRow),
    /// Batched cursor advance for a subscription.
    UpdateSubCursor { sub_id: RecordId, cursor: LogOffset },
    /// `UNSUBSCRIBE` / `UNWATCH`.
    Unregister { id: RecordId },
    /// Platform tables (stage G, ARCHITECTURE §15.1): FlowR / A3T registry tables,
    /// each its own Lance dataset with UTF-8 columns, `call:lancedb` semantics.
    /// Create `table` with these columns, if it doesn't exist.
    TableCreate { table: String, columns: Vec<String> },
    /// Append rows (field → string value); creates the table from the first row's
    /// fields if it doesn't exist.
    TableInsert { table: String, rows: Vec<serde_json::Map<String, serde_json::Value>> },
    /// Set `values` (column → SQL expression) on the rows matching `filter` (SQL).
    TableUpdate { table: String, filter: String, values: Vec<(String, String)> },
    /// Delete the rows matching `filter` (SQL).
    TableDelete { table: String, filter: String },
    /// Insert `rows`, replacing the rows whose `key` columns match (one Lance
    /// `merge_insert`, one version): FlowR's state and registry writes, which were
    /// a delete then an insert (CA-8). A key repeated in `rows` keeps its last row.
    TableUpsert { table: String, key: Vec<String>, rows: Vec<serde_json::Map<String, serde_json::Value>> },
    /// Replace the whole table — its columns and rows — in one step (copying a
    /// platform database across backends, FlowR's switch).
    TableReplace { table: String, columns: Vec<String>, rows: Vec<serde_json::Map<String, serde_json::Value>> },
    /// Add UTF-8 columns to an existing table (DB-6): old rows read them as null. A
    /// column the table already has is skipped.
    TableAddColumns { table: String, columns: Vec<String> },
    /// Make `table` append-only (DB-13): from then on only inserts and
    /// [`TablePurge`](Effect::TablePurge) touch it; update, delete, upsert and replace are
    /// refused. For error logs, audit and decision records. Can't be undone.
    TableAppendOnly { table: String },
    /// Retention for any table, the only removal an append-only table allows (DB-13):
    /// delete the rows whose `column` (an ISO time, compared as text) is before
    /// `before`.
    TablePurge { table: String, column: String, before: String },
    /// New vectors are in the vector file at `uri` (20 Nov 2026, `adb_store::vectors`):
    /// every node that applies this loads the file. The vectors themselves don't travel
    /// in the log, and no row of the table changes.
    Vectors { uri: String },
    /// Before 20 Nov 2026 a vector travelled in the log (`embed`). Decoded so logs
    /// written then still read (vc-dev's data was kept, user 20 Nov); never written,
    /// and applied as nothing: the backfill makes those records' vectors again, as
    /// vector files.
    #[serde(rename = "embed")]
    LegacyEmbed { memory: Memory, id: RecordId, embedding: Vec<f16> },
    /// FlowR's engine (stage H, `.ai/H_ENGINE_STORE.md`): the durable changes one
    /// operation made in engine space `space` (`flowr_storage::engine_core::Change`
    /// as JSON, applied in order by `adb-engine`; opaque here).
    Engine { space: String, changes: serde_json::Value },
    /// S3 option B (`.ai/S3_OPTION_B_PLAN.md` §3.3): the node's state up to `floor` is
    /// in object storage at `cold` (`node`: the memory table's `CURRENT`, the platform
    /// and engine lineages). Changes no data; every replica learns from it where a
    /// node that is behind the log restores from, and Raft snapshots are taken there.
    Archived { cold: String, floor: LogOffset, node: serde_json::Value },
}

/// One committed transaction on one partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    pub partition: PartitionId,
    pub offset: LogOffset,
    pub epoch: Epoch,
    pub commit_ts: CommitTs,
    /// Client transaction id, for dedupe of retried writes.
    pub txn_id: Option<String>,
    pub effects: Vec<Effect>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> RowData {
        RowData {
            tenant_id: "t1".into(),
            scope: "agent:42".into(),
            visibility: Visibility::Private,
            namespace: None,
            memory: Memory::Semantic,
            kind: Kind::Entity,
            key: Some("acme-renewal".into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            value: serde_json::json!({"content": "Acme renews on 1 Nov", "account": "acme"}),
            sealed: None,
            text: Some("Acme renews on 1 Nov".into()),
            embedding: Some(vec![f16::from_f32(0.5), f16::from_f32(-1.25)]),
            ttl_at: None,
            provenance: Provenance::default(),
        }
    }

    #[test]
    fn ids_sort_in_commit_order() {
        let mut ids = IdGen::new();
        let t = CommitTs(1_761_300_000_000_000);
        let a = ids.next(t);
        let b = ids.next(t); // same millisecond: still increasing
        let c = ids.next(CommitTs(t.0 + 1_000));
        assert_eq!(a.0.len(), 26);
        assert!(a < b && b < c);
    }

    #[test]
    fn commit_ts_round_trips_through_datetime() {
        let t = CommitTs(1_761_300_000_123_456);
        assert_eq!(CommitTs::from_datetime(t.to_datetime()), t);
        assert_eq!(t.millis(), 1_761_300_000_123);
    }

    #[test]
    fn log_record_round_trips() {
        let mut ids = IdGen::new();
        let ts = CommitTs(1_761_300_000_000_000);
        let old = ids.next(ts);
        let new = NewRow { id: ids.next(ts), lineage_id: old.clone(), version: 2, data: row() };
        let record = LogRecord {
            partition: PartitionId(7),
            offset: LogOffset(42),
            epoch: Epoch(3),
            commit_ts: ts,
            txn_id: Some("txn-1".into()),
            effects: vec![
                Effect::Supersede { old, new },
                Effect::UpdateSubCursor { sub_id: ids.next(ts), cursor: LogOffset(41) },
                Effect::Expire { memory: Memory::Working, lineage_id: ids.next(ts) },
                Effect::Validity { memory: Memory::Semantic, lineage_id: ids.next(ts), holds: false },
            ],
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains(r#""effect":"supersede""#));
        let back: LogRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back, record);
        // f16 embeddings survive the round trip exactly.
        if let Effect::Supersede { new, .. } = &back.effects[0] {
            assert_eq!(new.data.embedding.as_ref().unwrap()[1].to_f32(), -1.25);
        }
    }

    /// A log written before vector files (`"effect":"embed"`) still decodes.
    #[test]
    fn an_old_inline_vector_record_still_decodes() {
        let old = r#"{"effect":"embed","memory":"semantic","id":"k1","embedding":[14336,48448]}"#;
        match serde_json::from_str::<Effect>(old).unwrap() {
            Effect::LegacyEmbed { memory, id, embedding } => {
                assert_eq!((memory, id.0.as_str(), embedding.len()), (Memory::Semantic, "k1", 2));
            }
            other => panic!("{other:?}"),
        }
        let packed = rmp_serde::to_vec_named(&serde_json::from_str::<Effect>(old).unwrap()).unwrap();
        assert!(matches!(rmp_serde::from_slice::<Effect>(&packed).unwrap(), Effect::LegacyEmbed { .. }));
    }
}

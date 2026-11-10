//! Rows as the table holds them, and their Arrow encoding.

use std::sync::Arc;

use adb_core::log::{CommitTs, Kind, Memory, NewRow, Provenance, RowData, Visibility};
use arrow_array::builder::{
    BooleanBuilder, FixedSizeListBuilder, Float32Builder, LargeBinaryBuilder, StringBuilder,
    TimestampMicrosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, LargeBinaryArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, SchemaRef};
use half::f16;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::schema::META_ID;
use crate::{StoreError, StoreResult};

/// The table's row id for record or edge `id` of memory type `memory`:
/// `<memory>:<id>`. Record ids are the AQL keys, unique per memory type only, and one
/// table holds every type under one primary key — unqualified, `x` stored in WORKING
/// and in SEMANTIC was one row, and the second write replaced the first (schema v3).
pub fn row_id(memory: Memory, id: &str) -> String {
    format!("{}:{id}", memory.tag())
}

/// The id a row id was made from (`row_id`'s inverse); other ids unchanged.
pub fn key_of(row_id: &str) -> &str {
    match row_id.split_once(':') {
        Some((tag, id)) if Memory::from_tag(tag).is_some() => id,
        _ => row_id,
    }
}

/// The named scope a row is in when the statement names none (D3 S6, without an
/// agent identity).
pub const DEFAULT_SCOPE: &str = "tenant:default";

/// Separates a named scope from a key in a record id (E-L12): scope names can't
/// contain it (`IN SCOPE` checks), and keys don't.
pub const SCOPE_SEP: char = '~';

/// The record id for `key` in named scope `scope`: keys are unique per (scope,
/// memory type) (D3 S4, schema v4), so a key in a named scope is `<scope>~<key>`;
/// in the default scope it is the key itself. A key already in that form stays.
pub fn scoped_id(scope: &str, key: &str) -> String {
    match scope == DEFAULT_SCOPE || key.strip_prefix(scope).is_some_and(|r| r.starts_with(SCOPE_SEP)) {
        true => key.to_string(),
        false => format!("{scope}{SCOPE_SEP}{key}"),
    }
}

/// Separates a namespace from the rest of a record id (NS, schema v5): a record in
/// namespace `n` has the id `n|<scope part>~<key>`. Namespaces can't contain it or `~`.
pub const NAMESPACE_SEP: char = '|';

/// Whether `namespace` can be part of a record id (no `|`, no `~`, not empty).
pub fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty() && !namespace.contains(NAMESPACE_SEP) && !namespace.contains(SCOPE_SEP)
}

/// The record id for `key` in `scope` and `namespace` (NS): keys are unique per
/// (namespace, scope, memory type). Without a namespace it is [`scoped_id`].
pub fn space_id(namespace: Option<&str>, scope: &str, key: &str) -> String {
    namespaced(namespace, &scoped_id(scope, key))
}

/// `id` (a scoped id, `[<scope>~]<key>`) in `namespace`: `<ns>|[<scope>]~<key>`. An id
/// already in a namespace stays.
pub fn namespaced(namespace: Option<&str>, id: &str) -> String {
    match namespace {
        None => id.to_string(),
        Some(_) if namespace_of(id).is_some() => id.to_string(),
        Some(ns) => match id.contains(SCOPE_SEP) {
            true => format!("{ns}{NAMESPACE_SEP}{id}"),
            false => format!("{ns}{NAMESPACE_SEP}{SCOPE_SEP}{id}"),
        },
    }
}

/// The namespace a record id carries (the part before the first `~`, up to `|`).
pub fn namespace_of(id: &str) -> Option<&str> {
    let (head, _) = id.split_once(SCOPE_SEP)?;
    head.split_once(NAMESPACE_SEP).map(|(ns, _)| ns)
}

/// `id` without its namespace: the scoped id (`[<scope>~]<key>`).
pub fn without_namespace(id: &str) -> &str {
    match namespace_of(id) {
        None => id,
        Some(ns) => {
            let rest = &id[ns.len() + 1..];
            rest.strip_prefix(SCOPE_SEP).unwrap_or(rest)
        }
    }
}

/// The key a record id was made from (`scoped_id`'s inverse).
pub fn unscoped(id: &str) -> &str {
    id.split_once(SCOPE_SEP).map_or(id, |(_, k)| k)
}

/// `n` as the table stores it: id and lineage qualified by its memory type, and an
/// edge's ends by the memory types they belong to.
pub fn qualified(n: &NewRow) -> NewRow {
    let mut n = n.clone();
    let m = n.data.memory;
    n.id.0 = row_id(m, &n.id.0);
    n.lineage_id.0 = row_id(m, &n.lineage_id.0);
    // A fact's ends are records of its own memory type (P1.4).
    if n.data.kind == Kind::Fact {
        n.data.subject_id = n.data.subject_id.map(|s| row_id(m, &s));
        n.data.object_id = n.data.object_id.map(|o| row_id(m, &o));
    }
    if n.data.kind == Kind::Edge {
        n.data.subject_id = n.data.subject_id.map(|s| row_id(m, &s));
        let to = n.data.value.get("to_memory").and_then(|v| serde_json::from_value::<Memory>(v.clone()).ok());
        if let (Some(to), Some(o)) = (to, n.data.object_id.as_deref()) {
            n.data.object_id = Some(row_id(to, o));
        }
    }
    n
}

/// One row version: the writer's fields plus the engine's (D3 §1, D2 §1). Serialised
/// when a branch in its own pod reads its parent over Flight (B6).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredRow {
    pub id: String,
    pub lineage_id: String,
    pub data: RowData,
    pub commit_ts: CommitTs,
    pub valid_from: CommitTs,
    pub valid_to: Option<CommitTs>,
    pub superseded_by: Option<String>,
    pub version: u32,
    pub tombstone: bool,
    pub log_offset: u64,
    pub epoch: u64,
}

impl StoredRow {
    /// A new version committed at `commit_ts` by log record `log_offset`.
    pub fn from_new(n: &NewRow, commit_ts: CommitTs, log_offset: u64, epoch: u64) -> Self {
        Self {
            id: n.id.0.clone(),
            lineage_id: n.lineage_id.0.clone(),
            data: n.data.clone(),
            commit_ts,
            valid_from: commit_ts,
            valid_to: None,
            superseded_by: None,
            version: n.version,
            tombstone: false,
            log_offset,
            epoch,
        }
    }

    /// The record's id as AQL sees it: its lineage's (D2 T1), unqualified.
    pub fn key(&self) -> &str {
        key_of(&self.lineage_id)
    }

    /// In the current view at `now` (D2 §3): live, and a fact holds at `now`.
    pub fn is_current(&self, now: CommitTs) -> bool {
        self.is_live(now) && self.holds_at(now)
    }

    /// The lineage's version at `now` in transaction time: not superseded, not
    /// closed, not expired, not forgotten — whatever a fact's own validity says.
    /// Writes (supersede, key checks) and the hot cache work on live rows.
    pub fn is_live(&self, now: CommitTs) -> bool {
        !self.tombstone
            && self.superseded_by.is_none()
            && self.valid_to.is_none_or(|t| t > now)
            && self.data.ttl_at.is_none_or(|t| t > now)
    }

    /// A fact holds at `t` when `t` is within its `valid_from` / `valid_to` (AQL
    /// v0.10); every other record, and a fact without them, always holds.
    pub fn holds_at(&self, t: CommitTs) -> bool {
        if self.data.kind != Kind::Fact {
            return true;
        }
        let (from, to) = adb_core::time::validity(&self.data.value);
        from.is_none_or(|f| f <= t.0) && to.is_none_or(|u| u > t.0)
    }

    /// The version ADB held at `tx` (`AS OF`), if it holds at `valid` (D2 §8): a
    /// history read. Forgotten and expired records have no history.
    pub fn is_current_at(&self, tx: CommitTs, valid: CommitTs) -> bool {
        !self.tombstone
            && self.commit_ts <= tx
            && self.valid_to.is_none_or(|t| t > tx)
            && self.data.ttl_at.is_none_or(|t| t > tx)
            && self.holds_at(valid)
    }

    /// The `_meta` control row carrying the applied log offset (§9.3).
    pub fn meta(applied_offset: u64, commit_ts: CommitTs, epoch: u64, extra: serde_json::Value) -> Self {
        let mut value = serde_json::json!({
            "applied_offset": applied_offset,
            "schema_version": crate::schema::SCHEMA_VERSION,
        });
        if let (Some(v), Some(e)) = (value.as_object_mut(), extra.as_object()) {
            v.extend(e.clone());
        }
        Self {
            id: META_ID.into(),
            lineage_id: META_ID.into(),
            data: RowData {
                tenant_id: String::new(),
                scope: String::new(),
                visibility: Visibility::Private,
                namespace: None,
                memory: Memory::Working,
                kind: Kind::Meta,
                key: None,
                subject_id: None,
                predicate: None,
                object_id: None,
                link_type: None,
                weight: None,
                value,
                sealed: None,
                text: None,
                embedding: None,
                ttl_at: None,
                provenance: Provenance::default(),
            },
            commit_ts,
            valid_from: commit_ts,
            valid_to: None,
            superseded_by: None,
            version: 0,
            tombstone: false,
            log_offset: applied_offset,
            epoch,
        }
    }
}

fn enum_str<T: Serialize>(t: &T) -> String {
    serde_json::to_value(t).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default()
}

fn parse_enum<T: DeserializeOwned>(s: &str, column: &str) -> StoreResult<T> {
    serde_json::from_value(serde_json::Value::String(s.into()))
        .map_err(|_| StoreError::Decode(format!("{column}: unknown value '{s}'")))
}

/// Rows → one record batch in `schema`.
pub fn encode(rows: &[StoredRow], schema: &SchemaRef, dim: i32) -> StoreResult<RecordBatch> {
    let n = rows.len();
    let s = |f: &dyn Fn(&StoredRow) -> Option<String>| -> ArrayRef {
        let mut b = StringBuilder::with_capacity(n, n * 16);
        for r in rows {
            b.append_option(f(r));
        }
        Arc::new(b.finish())
    };
    let tsa = |f: &dyn Fn(&StoredRow) -> Option<i64>| -> ArrayRef {
        let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
        for r in rows {
            b.append_option(f(r));
        }
        Arc::new(b.finish())
    };
    let mut weight = Float32Builder::with_capacity(n);
    // Sized for these rows: a default builder holds room for 1024 values, and a batch
    // keeps its buffers' whole capacity for as long as it stays in a memtable. Most
    // batches here are a row or two.
    let sealed_bytes: usize = rows.iter().filter_map(|r| r.data.sealed.as_deref()).map(<[u8]>::len).sum();
    let mut sealed = LargeBinaryBuilder::with_capacity(n, sealed_bytes);
    let mut version = UInt32Builder::with_capacity(n);
    let mut tombstone = BooleanBuilder::with_capacity(n);
    let mut log_offset = UInt64Builder::with_capacity(n);
    let mut epoch = UInt64Builder::with_capacity(n);
    let mut embedding = FixedSizeListBuilder::with_capacity(Float32Builder::with_capacity(n * dim as usize), dim, n)
        .with_field(Arc::new(Field::new("item", DataType::Float32, true)));
    for r in rows {
        weight.append_option(r.data.weight);
        sealed.append_option(r.data.sealed.as_deref());
        version.append_value(r.version);
        tombstone.append_value(r.tombstone);
        log_offset.append_value(r.log_offset);
        epoch.append_value(r.epoch);
        match &r.data.embedding {
            Some(v) if v.len() == dim as usize => {
                for x in v {
                    embedding.values().append_value(x.to_f32());
                }
                embedding.append(true);
            }
            _ => {
                // No embedding (or one of another model's dimension): null.
                embedding.values().append_nulls(dim as usize);
                embedding.append(false);
            }
        }
    }
    let columns: Vec<ArrayRef> = vec![
        s(&|r| Some(r.id.clone())),
        s(&|r| Some(r.lineage_id.clone())),
        s(&|r| Some(r.data.tenant_id.clone())),
        s(&|r| Some(r.data.scope.clone())),
        s(&|r| Some(enum_str(&r.data.visibility))),
        s(&|r| r.data.namespace.clone()),
        s(&|r| Some(enum_str(&r.data.memory))),
        s(&|r| Some(enum_str(&r.data.kind))),
        s(&|r| r.data.key.clone()),
        s(&|r| r.data.subject_id.clone()),
        s(&|r| r.data.predicate.clone()),
        s(&|r| r.data.object_id.clone()),
        s(&|r| r.data.link_type.clone()),
        Arc::new(weight.finish()),
        {
            // JSONB, as Lance's json_* functions read it.
            let mut b = LargeBinaryBuilder::with_capacity(n, 0);
            for r in rows {
                if r.data.value.is_null() {
                    b.append_null();
                } else {
                    let bytes = lance_arrow::json::encode_json(&r.data.value.to_string())
                        .map_err(|e| StoreError::Encode(format!("value: {e}")))?;
                    b.append_value(bytes);
                }
            }
            Arc::new(b.finish()) as ArrayRef
        },
        Arc::new(sealed.finish()),
        s(&|r| r.data.text.clone()),
        Arc::new(embedding.finish()),
        tsa(&|r| Some(r.commit_ts.0)),
        tsa(&|r| Some(r.valid_from.0)),
        tsa(&|r| r.valid_to.map(|t| t.0)),
        s(&|r| r.superseded_by.clone()),
        Arc::new(version.finish()),
        tsa(&|r| r.data.ttl_at.map(|t| t.0)),
        Arc::new(tombstone.finish()),
        s(&|r| r.data.provenance.agent_id.clone()),
        s(&|r| r.data.provenance.source.clone()),
        s(&|r| r.data.provenance.txn_id.clone()),
        Arc::new(log_offset.finish()),
        Arc::new(epoch.finish()),
    ];
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| StoreError::Encode(e.to_string()))
}

fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> StoreResult<&'a T> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| StoreError::Decode(format!("column {name} missing or of the wrong type")))
}

fn opt_str(a: &StringArray, i: usize) -> Option<String> {
    (!a.is_null(i)).then(|| a.value(i).to_string())
}

fn opt_ts(a: &TimestampMicrosecondArray, i: usize) -> Option<CommitTs> {
    (!a.is_null(i)).then(|| CommitTs(a.value(i)))
}

fn opt_col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Option<&'a T> {
    b.column_by_name(name).and_then(|c| c.as_any().downcast_ref::<T>())
}

/// A record's key as the write path tracks it (`hydrate`): its memory type, lineage
/// and expiry, read without the payload and embedding columns.
#[derive(Debug, Clone)]
pub struct LiveKey {
    pub memory: adb_core::log::Memory,
    pub lineage_id: String,
    pub ttl_at: Option<CommitTs>,
}

/// The rows of a batch projected to [`LIVE_KEY_COLUMNS`] that are live at `now`
/// (as [`StoredRow::is_live`]).
pub fn decode_live_keys(b: &RecordBatch, now: CommitTs) -> StoreResult<Vec<LiveKey>> {
    let lineage = col::<StringArray>(b, "lineage_id")?;
    let memory = col::<StringArray>(b, "memory")?;
    let valid_to = col::<TimestampMicrosecondArray>(b, "valid_to")?;
    let superseded_by = col::<StringArray>(b, "superseded_by")?;
    let ttl_at = col::<TimestampMicrosecondArray>(b, "ttl_at")?;
    let tombstone = col::<BooleanArray>(b, "tombstone")?;
    let mut out = Vec::with_capacity(b.num_rows());
    for i in 0..b.num_rows() {
        let ttl = opt_ts(ttl_at, i);
        let live = !tombstone.value(i)
            && superseded_by.is_null(i)
            && opt_ts(valid_to, i).is_none_or(|t| t > now)
            && ttl.is_none_or(|t| t > now);
        if live {
            out.push(LiveKey { memory: parse_enum(memory.value(i), "memory")?, lineage_id: lineage.value(i).to_string(), ttl_at: ttl });
        }
    }
    Ok(out)
}

/// The columns [`decode_live_keys`] reads.
pub const LIVE_KEY_COLUMNS: [&str; 7] = ["id", "lineage_id", "memory", "valid_to", "superseded_by", "ttl_at", "tombstone"];

/// A record batch → rows. Every column is needed but `embedding` and `sealed`, which
/// reads that don't use them leave out (W7: a vector is 384 floats per row).
pub fn decode(b: &RecordBatch) -> StoreResult<Vec<StoredRow>> {
    let id = col::<StringArray>(b, "id")?;
    let lineage = col::<StringArray>(b, "lineage_id")?;
    let tenant = col::<StringArray>(b, "tenant_id")?;
    let scope = col::<StringArray>(b, "scope")?;
    let visibility = col::<StringArray>(b, "visibility")?;
    let namespace = col::<StringArray>(b, "namespace")?;
    let memory = col::<StringArray>(b, "memory")?;
    let kind = col::<StringArray>(b, "kind")?;
    let key = col::<StringArray>(b, "key")?;
    let subject = col::<StringArray>(b, "subject_id")?;
    let predicate = col::<StringArray>(b, "predicate")?;
    let object = col::<StringArray>(b, "object_id")?;
    let link_type = col::<StringArray>(b, "link_type")?;
    let weight = col::<Float32Array>(b, "weight")?;
    let value = col::<LargeBinaryArray>(b, "value")?;
    let sealed = opt_col::<LargeBinaryArray>(b, "sealed");
    let text = col::<StringArray>(b, "text")?;
    let embedding = opt_col::<FixedSizeListArray>(b, "embedding");
    let commit_ts = col::<TimestampMicrosecondArray>(b, "commit_ts")?;
    let valid_from = col::<TimestampMicrosecondArray>(b, "valid_from")?;
    let valid_to = col::<TimestampMicrosecondArray>(b, "valid_to")?;
    let superseded_by = col::<StringArray>(b, "superseded_by")?;
    let version = col::<UInt32Array>(b, "version")?;
    let ttl_at = col::<TimestampMicrosecondArray>(b, "ttl_at")?;
    let tombstone = col::<BooleanArray>(b, "tombstone")?;
    let p_agent = col::<StringArray>(b, "provenance_agent")?;
    let p_source = col::<StringArray>(b, "provenance_source")?;
    let p_txn = col::<StringArray>(b, "provenance_txn")?;
    let log_offset = col::<UInt64Array>(b, "log_offset")?;
    let epoch = col::<UInt64Array>(b, "epoch")?;

    let mut rows = Vec::with_capacity(b.num_rows());
    for i in 0..b.num_rows() {
        let emb = embedding.filter(|e| !e.is_null(i)).map(|embedding| {
            let v = embedding.value(i);
            let f = v.as_any().downcast_ref::<Float32Array>().map(|a| a.values().to_vec()).unwrap_or_default();
            f.into_iter().map(f16::from_f32).collect::<Vec<_>>()
        });
        let value = if value.is_null(i) {
            serde_json::Value::Null
        } else {
            let text = lance_arrow::json::decode_json(value.value(i));
            serde_json::from_str(&text).map_err(|e| StoreError::Decode(format!("value: {e}")))?
        };
        rows.push(StoredRow {
            id: id.value(i).to_string(),
            lineage_id: lineage.value(i).to_string(),
            data: RowData {
                tenant_id: tenant.value(i).to_string(),
                scope: scope.value(i).to_string(),
                visibility: parse_enum(visibility.value(i), "visibility")?,
                namespace: opt_str(namespace, i),
                memory: parse_enum(memory.value(i), "memory")?,
                kind: parse_enum(kind.value(i), "kind")?,
                key: opt_str(key, i),
                subject_id: opt_str(subject, i),
                predicate: opt_str(predicate, i),
                object_id: opt_str(object, i),
                link_type: opt_str(link_type, i),
                weight: (!weight.is_null(i)).then(|| weight.value(i)),
                value,
                sealed: sealed.filter(|s| !s.is_null(i)).map(|s| s.value(i).to_vec()),
                text: opt_str(text, i),
                embedding: emb,
                ttl_at: opt_ts(ttl_at, i),
                provenance: Provenance {
                    agent_id: opt_str(p_agent, i),
                    source: opt_str(p_source, i),
                    txn_id: opt_str(p_txn, i),
                },
            },
            commit_ts: CommitTs(commit_ts.value(i)),
            valid_from: CommitTs(valid_from.value(i)),
            valid_to: opt_ts(valid_to, i),
            superseded_by: opt_str(superseded_by, i),
            version: version.value(i),
            tombstone: tombstone.value(i),
            log_offset: log_offset.value(i),
            epoch: epoch.value(i),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod ns_tests {
    use super::*;

    /// A batch stays in a memtable with its buffers' whole capacity, so a one-row batch
    /// must not hold room for a thousand.
    #[test]
    fn a_small_batch_keeps_small_buffers() {
        let dim = 384;
        let schema = crate::memory_schema(dim);
        let meta = StoredRow::meta(1, adb_core::log::CommitTs(0), 1, serde_json::json!({}));
        let batch = encode(std::slice::from_ref(&meta), &schema, dim).unwrap();
        let held = batch.get_array_memory_size();
        assert!(held < 8 * 1024, "one row holds {held} bytes");
    }

    #[test]
    fn namespaced_ids() {
        assert_eq!(space_id(None, DEFAULT_SCOPE, "user:alice"), "user:alice");
        assert_eq!(space_id(Some("acme--bot"), DEFAULT_SCOPE, "user:alice"), "acme--bot|~user:alice");
        assert_eq!(space_id(Some("acme--bot"), "team:x", "user:alice"), "acme--bot|team:x~user:alice");
        assert_eq!(space_id(None, "team:x", "user:alice"), "team:x~user:alice");
        for id in ["acme--bot|~user:alice", "acme--bot|team:x~user:alice"] {
            assert_eq!(namespaced(Some("acme--bot"), id), id, "idempotent");
            assert_eq!(namespace_of(id), Some("acme--bot"));
            assert_eq!(unscoped(id), "user:alice");
        }
        assert_eq!(without_namespace("acme--bot|~user:alice"), "user:alice");
        assert_eq!(without_namespace("acme--bot|team:x~user:alice"), "team:x~user:alice");
        // A fact key has `|` but no `~`: not namespaced.
        assert_eq!(namespace_of("fact:a|p|b"), None);
        assert_eq!(namespace_of("team:x~fact:a|p|b"), None);
        assert_eq!(namespace_of("crm|~fact:a|p|b"), Some("crm"));
        assert_eq!(unscoped("crm|~fact:a|p|b"), "fact:a|p|b");
        assert!(!valid_namespace("a|b") && !valid_namespace("a~b") && valid_namespace("acme--bot"));
    }
}

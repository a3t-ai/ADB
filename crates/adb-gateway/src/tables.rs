//! The platform-table change feed (DB-5): every write to the platform tables, in log
//! order, from an offset. FlowR follows it to keep triggers, the registry and policies
//! the same on every pod without polling the tables.
//!
//! A read returns the changes after `from` (waiting up to `wait` for the first), and
//! `next` to pass as `from` next time. A consumer with no offset, or one the log no
//! longer holds (`ADB_LOG_RETAIN`), gets `reset: true`: reload the tables, then follow
//! from `next`.

use std::time::Duration;

use adb_core::log::{Effect, LogOffset, LogRecord};
use adb_log::LogError;
use serde_json::{json, Value};

use crate::{Gateway, GatewayError};

#[derive(Debug, Clone, Default)]
pub struct TableQuery {
    pub from: Option<u64>,
    /// Only these tables (empty: all).
    pub tables: Vec<String>,
    /// Changes per read (whole log records: a read may go a little over).
    pub max: usize,
    pub wait: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableBatch {
    /// `{offset, ts, table, op, …}` per change; `op` is `create_table`, `insert`,
    /// `upsert`, `update`, `delete`, `replace`, `add_columns`, `append_only` or `purge`,
    /// with that operation's fields.
    pub changes: Vec<Value>,
    /// True: the consumer must reload the tables before following from `next`.
    pub reset: bool,
    pub next: u64,
    pub tip: u64,
}

/// A read of one memory space's feed (DB-14).
#[derive(Debug, Clone, Default)]
pub struct SpaceQuery {
    pub namespace: String,
    pub from: Option<u64>,
    /// Only these memory types, any case (empty: all).
    pub memories: Vec<String>,
    pub max: usize,
    pub wait: Duration,
}

const READ_BATCH: usize = 256;

/// The change an effect makes to `namespace`'s memory: `{offset, ts, op, memory, key?,
/// lineage, value?}`, with `op` one of `put`, `update`, `patch`, `forget`, `expire`.
/// Links, topics and control rows aren't memory records. FORGET rewrites the log, so an
/// erased payload is never read here; a `forget` names the key only (W5-L2).
pub fn space_change(r: &LogRecord, e: &Effect, namespace: &str) -> Option<Value> {
    let row = |op: &str, n: &adb_core::log::NewRow| {
        (n.data.namespace.as_deref() == Some(namespace)).then(|| {
            json!({ "op": op, "memory": n.data.memory.tag(), "key": n.data.key, "lineage": n.lineage_id.0, "value": n.data.value })
        })
    };
    let by_id = |op: &str, memory: &adb_core::log::Memory, id: &str, value: Option<Value>| {
        (adb_store::rows::namespace_of(id) == Some(namespace)).then(|| {
            let mut c = json!({ "op": op, "memory": memory.tag(), "key": adb_store::rows::without_namespace(id), "lineage": id });
            if let Some(v) = value {
                c["value"] = v;
            }
            c
        })
    };
    let mut c = match e {
        Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) => row("put", n),
        Effect::Supersede { new, .. } => row("update", new),
        Effect::Patch { memory, id, fields, .. } => by_id("patch", memory, &id.0, Some(Value::Object(fields.clone()))),
        Effect::Forget { memory, lineage_id } => by_id("forget", memory, &lineage_id.0, None),
        Effect::Expire { memory, lineage_id } => by_id("expire", memory, &lineage_id.0, None),
        _ => None,
    }?;
    c["offset"] = json!(r.offset.0);
    c["ts"] = json!(r.commit_ts.0);
    Some(c)
}

/// The change an effect makes, if it is a platform-table effect.
pub fn table_change(r: &LogRecord, e: &Effect) -> Option<(String, Value)> {
    let (table, body) = match e {
        Effect::TableCreate { table, columns } => (table, json!({ "op": "create_table", "columns": columns })),
        Effect::TableInsert { table, rows } => (table, json!({ "op": "insert", "rows": rows })),
        Effect::TableUpsert { table, key, rows } => (table, json!({ "op": "upsert", "key": key, "rows": rows })),
        Effect::TableUpdate { table, filter, values } => (
            table,
            json!({ "op": "update", "filter": filter, "values": values.iter().map(|(c, v)| (c.clone(), json!(v))).collect::<serde_json::Map<_, _>>() }),
        ),
        Effect::TableDelete { table, filter } => (table, json!({ "op": "delete", "filter": filter })),
        Effect::TableReplace { table, columns, rows } => (table, json!({ "op": "replace", "columns": columns, "rows": rows })),
        Effect::TableAddColumns { table, columns } => (table, json!({ "op": "add_columns", "columns": columns })),
        Effect::TableAppendOnly { table } => (table, json!({ "op": "append_only" })),
        Effect::TablePurge { table, column, before } => (table, json!({ "op": "purge", "column": column, "before": before })),
        _ => return None,
    };
    let mut body = body;
    if let Some(m) = body.as_object_mut() {
        m.insert("offset".into(), json!(r.offset.0));
        m.insert("ts".into(), json!(r.commit_ts.0));
        m.insert("table".into(), json!(table));
    }
    Some((table.clone(), body))
}

impl Gateway {
    /// One read of the platform-table feed (DB-5).
    pub async fn table_changes(&self, q: &TableQuery) -> Result<TableBatch, GatewayError> {
        self.follow(q.from, q.max, q.wait, |r, out| {
            for e in &r.effects {
                if let Some((table, change)) = table_change(r, e) {
                    if q.tables.is_empty() || q.tables.contains(&table) {
                        out.push(change);
                    }
                }
            }
        })
        .await
    }

    /// One read of a memory space's feed (DB-14).
    pub async fn space_changes(&self, q: &SpaceQuery) -> Result<TableBatch, GatewayError> {
        self.follow(q.from, q.max, q.wait, |r, out| {
            for e in &r.effects {
                if let Some(change) = space_change(r, e, &q.namespace) {
                    if q.memories.is_empty() || change["memory"].as_str().is_some_and(|m| q.memories.iter().any(|w| w.eq_ignore_ascii_case(m))) {
                        out.push(change);
                    }
                }
            }
        })
        .await
    }

    /// Log records after `from` through `take`, long-polling up to `wait` for a first
    /// change. Without an offset, or past the log's retention: `reset`.
    async fn follow(&self, from: Option<u64>, max: usize, wait: Duration, take: impl Fn(&LogRecord, &mut Vec<Value>)) -> Result<TableBatch, GatewayError> {
        let mut applied = self.applied.clone();
        let Some(from) = from.filter(|f| *f > 0) else {
            let tip = *applied.borrow_and_update();
            return Ok(TableBatch { changes: Vec::new(), reset: true, next: tip + 1, tip });
        };
        let max = max.max(1);
        let mut next = from;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let tip = *applied.borrow_and_update();
            let mut out = Vec::new();
            while next <= tip && out.len() < max {
                let n = ((tip - next + 1) as usize).min(READ_BATCH);
                let records = match self.log.read(LogOffset(next), n).await {
                    Ok(r) => r,
                    Err(LogError::Purged { .. }) => return Ok(TableBatch { changes: Vec::new(), reset: true, next: tip + 1, tip }),
                    Err(e) => return Err(GatewayError::Log(e.to_string())),
                };
                if records.is_empty() {
                    break;
                }
                for r in &records {
                    next = r.offset.0 + 1;
                    take(r, &mut out);
                    if out.len() >= max {
                        break;
                    }
                }
            }
            if !out.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok(TableBatch { changes: out, reset: false, next, tip });
            }
            if tokio::time::timeout_at(deadline, applied.changed()).await.is_err() {
                let tip = *applied.borrow();
                return Ok(TableBatch { changes: Vec::new(), reset: false, next, tip });
            }
        }
    }
}

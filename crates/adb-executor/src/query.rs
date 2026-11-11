//! The query engine (MVP W7, `W7_QUERY_ENGINE.md`): AQL reads as one DataFusion
//! scan of an LSM snapshot (current view, memory type, scope, namespace and a
//! superset of the payload filter pushed down; vectors only when asked for), then
//! AQL semantics once, for every memory type: exact WHERE / KEY / PATTERN, WINDOW,
//! ORDER BY, LIMIT, RETURN, aggregates, links in batches.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use adb_core::log::{CommitTs, Kind, Memory};
use adb_core::{
    evaluate_conditions, evaluate_conditions_on_record, AdbError, AdbResult, AggregateFunc, AggregateFuncType, Condition, Link,
    MemoryRecord, MemoryType, Metadata, Modifiers, OrderBy, Predicate, Scope, Value, Window,
};
use adb_store::{decode, predicate_filter, row_id, Cached, PartitionStore, StoredRow};
use datafusion::prelude::{col, lit, Expr};

use crate::reads::{to_link, to_record};

fn store_err(e: adb_store::StoreError) -> AdbError {
    AdbError::Internal(format!("store: {e}"))
}

/// Record rows of `memory` (not edges, events, registrations), in `in_scope`.
fn records_of(memory: Memory, in_scope: Option<&str>, namespace: Option<&str>) -> Expr {
    let kinds = ["entity", "fact", "episode", "tool"].map(lit).to_vec();
    let mut f = col("memory").eq(lit(memory.tag())).and(col("kind").in_list(kinds, false));
    if let Some(s) = in_scope {
        f = f.and(col("scope").eq(lit(s.to_string())));
    }
    if let Some(ns) = namespace {
        f = f.and(col("namespace").eq(lit(ns.to_string())));
    }
    f
}

/// The current rows matching `filter`, from one snapshot; `vectors`: with the
/// `embedding` column (LIKE), otherwise without it.
pub async fn current_rows(store: &PartitionStore, filter: Expr, now: CommitTs, vectors: bool) -> AdbResult<Vec<StoredRow>> {
    // Only cheap column filters go into the scan; the current view (D2 §3: not
    // superseded, closed or expired) is tested on the decoded rows — its timestamp
    // tests cost more in the scan (measured, W7).
    let filter = col("tombstone").eq(lit(false)).and(filter);
    let started = std::time::Instant::now();
    let batches = store
        .query(move |ctx| async move {
            let df = ctx.table("memory").await?.filter(filter)?;
            let df = if vectors {
                df
            } else {
                let cols: Vec<String> =
                    df.schema().fields().iter().map(|f| f.name().clone()).filter(|n| n != "embedding" && n != "sealed").collect();
                df.select_columns(&cols.iter().map(String::as_str).collect::<Vec<_>>())?
            };
            df.collect().await
        })
        .await
        .map_err(store_err)?;
    let scanned = started.elapsed();
    let mut rows = Vec::new();
    for b in &batches {
        rows.extend(decode(b).map_err(store_err)?);
    }
    tracing::debug!(?scanned, decoded = ?(started.elapsed() - scanned), rows = rows.len(), "query scan");
    // The pushed-down filter ran in the LSM; the view is re-checked on the decoded rows.
    rows.retain(|r| r.is_current(now) && r.data.kind != Kind::Meta);
    Ok(rows)
}

// ------------------------------------------------------------------ records

/// A PROCEDURAL payload as its procedure node (the shape the procedural memory type
/// has always returned): defaults filled, `steps` a list, numeric `variables`; other
/// payload fields kept as they are (L7).
pub fn procedure_value(key: &str, data: &serde_json::Value, version: u32) -> serde_json::Value {
    let s = |k: &str, d: &str| data.get(k).and_then(|v| v.as_str()).unwrap_or(d).to_string();
    let count = |k: &str| data.get(k).and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|i| i.max(0) as u64))).unwrap_or(0);
    let steps: Vec<String> = match data.get("steps") {
        Some(serde_json::Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
        Some(serde_json::Value::String(s)) => s.lines().map(String::from).collect(),
        _ => Vec::new(),
    };
    let mut variables = serde_json::Map::new();
    match data.get("variables") {
        Some(serde_json::Value::Object(o)) => {
            for (k, v) in o {
                if let Some(f) = v.as_f64() {
                    variables.insert(k.clone(), serde_json::json!(f));
                }
            }
        }
        Some(serde_json::Value::String(s)) => {
            for (k, v) in s.split(',').filter_map(|p| p.split_once('=')) {
                if let Ok(f) = v.trim().parse::<f64>() {
                    variables.insert(k.trim().to_string(), serde_json::json!(f));
                }
            }
        }
        _ => {}
    }
    let mut node = serde_json::json!({
        "pattern_id": data.get("pattern_id").and_then(|v| v.as_str()).unwrap_or(key),
        "pattern": s("pattern", ""),
        "steps": steps,
        "severity": s("severity", "info"),
        "confidence": data.get("confidence").and_then(|v| v.as_f64()).unwrap_or(0.5),
        "source": s("source", "manual"),
        "success_count": count("success_count"),
        "failure_count": count("failure_count"),
        "variables": variables,
        "metadata": data.get("metadata").cloned().unwrap_or(serde_json::Value::Null),
        "version": version,
    });
    if let (Some(n), Some(d)) = (node.as_object_mut(), data.as_object()) {
        for (k, v) in d {
            if !n.contains_key(k) {
                n.insert(k.clone(), v.clone());
            }
        }
    }
    node
}

/// A TOOLS payload as its tool record: defaults filled, unknown fields kept in
/// `metadata`.
pub fn tool_value(key: &str, data: &serde_json::Value) -> serde_json::Value {
    let tool_id = data.get("tool_id").and_then(|v| v.as_str()).unwrap_or(key).to_string();
    let s = |k: &str, d: &str| data.get(k).and_then(|v| v.as_str()).unwrap_or(d).to_string();
    let mut relevance = serde_json::Map::new();
    if let Some(o) = data.get("relevance_scores").and_then(|v| v.as_object()) {
        for (task, score) in o {
            if let Some(f) = score.as_f64() {
                relevance.insert(task.clone(), serde_json::json!((f as f32).clamp(0.0, 1.0)));
            }
        }
    }
    let mut meta = data.get("metadata").cloned().unwrap_or(serde_json::json!({}));
    const KNOWN: [&str; 9] = ["tool_id", "name", "description", "schema", "category", "ranking", "relevance_scores", "metadata", "key"];
    if let (Some(m), Some(d)) = (meta.as_object_mut(), data.as_object()) {
        for (k, v) in d {
            if !KNOWN.contains(&k.as_str()) {
                m.insert(k.clone(), v.clone());
            }
        }
    }
    serde_json::json!({
        "tool_id": tool_id,
        "name": data.get("name").and_then(|v| v.as_str()).unwrap_or(&tool_id),
        "description": s("description", ""),
        "schema": data.get("schema").cloned().unwrap_or(serde_json::Value::Null),
        "category": s("category", "general"),
        "ranking": data.get("ranking").and_then(|v| v.as_f64()).unwrap_or(0.5),
        "call_count": data.get("call_count").and_then(|v| v.as_u64()).unwrap_or(0),
        "last_called": data.get("last_called").cloned().unwrap_or(serde_json::Value::Null),
        "relevance_scores": relevance,
        "metadata": meta,
    })
}

/// A row as the record AQL returns. PROCEDURAL and TOOLS are shaped (above) and
/// reported `shared`, as their memory types always were; their namespace is kept.
pub fn record(r: &StoredRow) -> MemoryRecord {
    match r.data.memory {
        Memory::Procedural | Memory::Tools => {
            let mut m = Metadata::new(Scope::Shared);
            m.created_at = r.commit_ts.to_datetime();
            m.accessed_at = r.commit_ts.to_datetime();
            m.namespace = r.data.namespace.clone();
            m.version = u64::from(r.version);
            let (id, data) = if r.data.memory == Memory::Procedural {
                let v = procedure_value(r.key(), &r.data.value, r.version);
                (v["pattern_id"].as_str().unwrap_or(r.key()).to_string(), v)
            } else {
                let v = tool_value(r.key(), &r.data.value);
                (v["tool_id"].as_str().unwrap_or(r.key()).to_string(), v)
            };
            if r.data.memory == Memory::Tools {
                m.version = 1;
            }
            MemoryRecord { id, memory_type: r.data.memory.into(), data, metadata: m }
        }
        _ => to_record(r),
    }
}

// ------------------------------------------------------------------ predicates

fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(a) => format!("{a:?}"),
        Value::Variable(v) => format!("${{{v}}}"),
    }
}

fn json_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Token-overlap similarity of two texts (PROCEDURAL `PATTERN`).
pub fn jaccard(a: &str, b: &str) -> f32 {
    let ta: HashSet<&str> = a.split_whitespace().collect();
    let tb: HashSet<&str> = b.split_whitespace().collect();
    if ta.is_empty() && tb.is_empty() {
        return 1.0;
    }
    let union = ta.union(&tb).count();
    if union == 0 { 0.0 } else { ta.intersection(&tb).count() as f32 / union as f32 }
}

/// Whether `r` (of `memory`) matches `predicate` exactly. `LIKE` and `PATTERN` are
/// ranked elsewhere and match nothing here.
pub fn matches(memory: MemoryType, r: &MemoryRecord, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::All => true,
        Predicate::Where { conditions } => match memory {
            // Conditions see the shaped tool record; procedures (whose payload is
            // kept, W9.1) as other records, so `metadata.*` works too.
            MemoryType::Tools => evaluate_conditions(&r.data, conditions),
            _ => evaluate_conditions_on_record(r, conditions),
        },
        Predicate::Key { field, value } => {
            let want = value_text(value);
            match (memory, field.as_str()) {
                (MemoryType::Procedural, "pattern_id" | "id") | (MemoryType::Tools, "tool_id" | "id") => r.id == want,
                (MemoryType::Procedural, _) => r.data.get(field).map(json_text) == Some(want),
                (MemoryType::Tools, "name" | "category") => {
                    matches!(value, Value::String(_)) && r.data.get(field).and_then(|v| v.as_str()) == Some(want.as_str())
                }
                (MemoryType::Tools, _) => false,
                (_, "id" | "key") => r.id == want,
                _ => r.data.get(field).map(json_text) == Some(want),
            }
        }
        Predicate::Like { .. } | Predicate::Pattern { .. } => false,
    }
}

// ------------------------------------------------------------------ ordering

fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            a.as_f64().unwrap_or(0.0).partial_cmp(&b.as_f64().unwrap_or(0.0)).unwrap_or(Ordering::Equal)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// `ORDER BY`: numbers by value, strings by text; a missing value sorts last
/// ascending and first descending (as SQL). Ties keep the input order.
pub fn order_by<R: Borrow<MemoryRecord>>(records: &mut Vec<R>, order: &OrderBy) {
    // Each record's sort value once, not per comparison.
    let mut keyed: Vec<(Option<serde_json::Value>, R)> = records.drain(..).map(|r| (r.borrow().order_value(&order.field), r)).collect();
    keyed.sort_by(|(a, _), (b, _)| {
        let c = match (a, b) {
            (Some(x), Some(y)) => cmp_json(x, y),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if order.ascending { c } else { c.reverse() }
    });
    records.extend(keyed.into_iter().map(|(_, r)| r));
}

/// `records` cut to the first `k` of `ORDER BY o` (stable over the default order:
/// equal sort values keep `default_order`'s order), sorted.
fn top_k<R: Borrow<MemoryRecord>>(memory: MemoryType, records: &mut Vec<R>, o: &OrderBy, k: usize) {
    let mut keyed: Vec<(Option<serde_json::Value>, R)> = records.drain(..).map(|r| (r.borrow().order_value(&o.field), r)).collect();
    let by_value = |a: &Option<serde_json::Value>, b: &Option<serde_json::Value>| {
        let c = match (a, b) {
            (Some(x), Some(y)) => cmp_json(x, y),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if o.ascending { c } else { c.reverse() }
    };
    let default = |a: &MemoryRecord, b: &MemoryRecord| {
        let c = (a.metadata.created_at, &a.id).cmp(&(b.metadata.created_at, &b.id));
        if memory == MemoryType::Episodic { c.reverse() } else { c }
    };
    let cmp = |(av, ar): &(Option<serde_json::Value>, R), (bv, br): &(Option<serde_json::Value>, R)| by_value(av, bv).then_with(|| default(ar.borrow(), br.borrow()));
    if k == 0 {
        return;
    }
    keyed.select_nth_unstable_by(k - 1, cmp);
    keyed.truncate(k);
    keyed.sort_by(cmp);
    records.extend(keyed.into_iter().map(|(_, r)| r));
}

fn oldest_first<R: Borrow<MemoryRecord>>(records: &mut [R]) {
    records.sort_by(|a, b| {
        let (a, b) = (a.borrow(), b.borrow());
        (a.metadata.created_at, &a.id).cmp(&(b.metadata.created_at, &b.id))
    });
}

/// The order without `ORDER BY`: EPISODIC newest first, the others oldest first;
/// ties by id.
pub fn default_order<R: Borrow<MemoryRecord>>(memory: MemoryType, records: &mut [R]) {
    oldest_first(records);
    if memory == MemoryType::Episodic {
        records.reverse();
    }
}

/// `WINDOW` over records in default (oldest first) order.
pub fn window<R: Borrow<MemoryRecord>>(mut records: Vec<R>, w: &Window) -> Vec<R> {
    match w {
        Window::LastN { count } => {
            let skip = records.len().saturating_sub(*count);
            records.split_off(skip)
        }
        Window::LastDuration { duration } => {
            let cutoff = adb_core::time::now() - chrono::Duration::from_std(*duration).unwrap_or_default();
            records.into_iter().filter(|r| r.borrow().metadata.created_at > cutoff).collect()
        }
        Window::TopBy { count, field } => {
            records.sort_by(|a, b| match (b.borrow().data.get(field), a.borrow().data.get(field)) {
                (Some(x), Some(y)) => cmp_json(x, y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            });
            records.truncate(*count);
            records
        }
        Window::Since { condition } => {
            let start = records.iter().position(|r| condition.matches(&r.borrow().data)).unwrap_or(records.len());
            records.split_off(start)
        }
    }
}

/// `WINDOW` on `RECALL FROM EPISODIC` (AQL v0.9): over the episodes in commit order,
/// before `ORDER BY`, `LIMIT` and ranking.
fn in_window<R: Borrow<MemoryRecord>>(mut records: Vec<R>, w: &Window) -> Vec<R> {
    oldest_first(&mut records);
    window(records, w)
}

/// `RETURN` (dotted paths as `metadata.scope` too).
pub fn project(records: &mut [MemoryRecord], fields: &[String]) {
    for r in records.iter_mut() {
        r.data = r.project_fields(fields);
    }
}

// ------------------------------------------------------------------ reads

/// Record `key` of `memory` by primary key (Lance's point lookup, ~1 ms): its first
/// version's row, if that is current and passes the scope / namespace filters. `None`
/// when it isn't — updated, forgotten, missing, or its first version compacted away —
/// and the caller scans by lineage instead (exact either way).
async fn point(store: &PartitionStore, memory: Memory, key: &str, in_scope: Option<&str>, namespace: Option<&str>, now: CommitTs) -> AdbResult<Option<StoredRow>> {
    let Some(r) = store.get(&row_id(memory, key)).await.map_err(store_err)? else { return Ok(None) };
    let ok = r.is_current(now)
        && r.data.memory == memory
        && matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool)
        && in_scope.is_none_or(|s| r.data.scope == s)
        && namespace.is_none_or(|ns| r.data.namespace.as_deref() == Some(ns));
    Ok(ok.then_some(r))
}

/// Where a read's candidates come from (W10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The hot cache where it can answer, else Lance.
    Cache,
    Lance,
}

/// A read's candidates. `floor`: from the cache, rows committed at or before it
/// may be missing (`None`: complete).
struct Candidates {
    /// Shared with the cache (copied only when returned, W10-L8).
    records: Vec<Arc<MemoryRecord>>,
    cached: bool,
    floor: Option<CommitTs>,
}

/// A read's answer: from the cache, from Lance, or the cache's rows weren't
/// enough (read again from Lance).
enum Answer<T> {
    Cache(T),
    Lance(T),
    Insufficient,
}

impl<T> Answer<T> {
    fn of(c: &Candidates, v: T) -> Self {
        if c.cached { Answer::Cache(v) } else { Answer::Lance(v) }
    }
}

/// Run `f` from the cache, falling back to Lance; in shadow mode a cached answer
/// is checked against Lance's and a difference is an error.
async fn served<T, F, Fut>(store: &PartitionStore, what: &str, f: F, same: impl Fn(&T, &T) -> bool, show: impl Fn(&T) -> String) -> AdbResult<T>
where
    F: Fn(Source) -> Fut,
    Fut: std::future::Future<Output = AdbResult<Answer<T>>>,
{
    let lance = |a: Answer<T>| match a {
        Answer::Cache(v) | Answer::Lance(v) => v,
        Answer::Insufficient => unreachable!("a Lance read is complete"),
    };
    let Some(hot) = store.hot() else { return Ok(lance(f(Source::Lance).await?)) };
    // Shadow: both answers from the same state — no put began or ended between
    // them (a write applied in between makes them differ rightly); else again.
    let mut tries = 0;
    let (answer, seq) = loop {
        let seq = store.apply_seq();
        let a = f(Source::Cache).await?;
        if !hot.shadow() || seq % 2 == 0 || tries == 5 {
            break (a, seq);
        }
        tries += 1;
        tokio::task::yield_now().await;
    };
    match answer {
        Answer::Lance(v) => Ok(v),
        Answer::Cache(v) => {
            hot.hit();
            if hot.shadow() && seq % 2 == 0 {
                let l = lance(f(Source::Lance).await?);
                if store.apply_seq() == seq && !same(&v, &l) {
                    hot.mismatch();
                    tracing::error!(what, cache = %show(&v), lance = %show(&l), "hot cache answer differs from Lance");
                    return Err(AdbError::Internal(format!("hot cache shadow check: {what} differs from Lance (cache {}, Lance {})", show(&v), show(&l))));
                }
            }
            Ok(v)
        }
        Answer::Insufficient => {
            hot.miss();
            Ok(lance(f(Source::Lance).await?))
        }
    }
}

fn same_records(a: &Vec<MemoryRecord>, b: &Vec<MemoryRecord>) -> bool {
    serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
}

/// The records a read returns, copied out of the candidates.
fn owned(records: Vec<Arc<MemoryRecord>>) -> Vec<MemoryRecord> {
    records.into_iter().map(|r| Arc::try_unwrap(r).unwrap_or_else(|r| (*r).clone())).collect()
}

/// A cached row's record (decoded at apply when the cache has the shape).
fn cached_record(c: &Cached) -> Arc<MemoryRecord> {
    c.record.clone().unwrap_or_else(|| Arc::new(record(&c.row)))
}

fn show_records(v: &Vec<MemoryRecord>) -> String {
    let ids: Vec<&str> = v.iter().take(20).map(|r| r.id.as_str()).collect();
    format!("{} records {ids:?}", v.len())
}

/// The candidates from the hot cache, if it keeps `memory`: as from Lance, every
/// current record row of `memory` in the scope and namespace (the exact predicate
/// runs after, as on Lance's superset).
fn cached(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs) -> Option<Candidates> {
    let hot = store.hot()?;
    let m: Memory = memory.into();
    let (in_scope, namespace) = (modifiers.in_scope.as_deref(), modifiers.namespace.as_deref());
    let ok = |r: &StoredRow| {
        r.is_current(now) && in_scope.is_none_or(|s| r.data.scope == s) && namespace.is_none_or(|ns| r.data.namespace.as_deref() == Some(ns))
    };
    // KEY: the lineage's current version, as the point lookup finds it.
    if let Some(key) = keyed(memory, predicate, in_scope, namespace) {
        if let Some(Some(c)) = hot.lineage(m, &row_id(m, &key)) {
            if ok(&c.row) {
                return Some(Candidates { records: vec![cached_record(&c)], cached: true, floor: None });
            }
        }
    }
    let hit = hot.rows(m, in_scope)?;
    Some(Candidates { records: hit.rows.iter().filter(|c| ok(&c.row)).map(cached_record).collect(), cached: true, floor: hit.floor })
}

/// The key a KEY predicate names, when it is the record's key. `WHERE key = "x"`
/// is a point lookup only within one scope (`in_scope`): without IN SCOPE it means
/// the payload key in every scope (E-L12), where several records can hold it.
pub(crate) fn keyed(memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> Option<String> {
    match predicate {
        Predicate::Key { field, value } => {
            let keyed = matches!(field.as_str(), "id" | "key")
                || (memory == MemoryType::Procedural && field == "pattern_id")
                || (memory == MemoryType::Tools && field == "tool_id");
            keyed.then(|| value_text(value))
        }
        // `WHERE key = "x"` alone (PF-11): the record whose key is x, so the same
        // primary-key lookup. The callers still run `matches` on what it finds.
        Predicate::Where { conditions } if conditions.len() == 1 && !matches!(memory, MemoryType::Procedural | MemoryType::Tools) => match &conditions[0] {
            adb_core::Condition::Simple { field, operator: adb_core::Operator::Eq, value, .. }
                if field == "id" || (field == "key" && in_scope.is_some()) =>
            {
                // `id` arrives as the scoped id already (`scope_ids`); `key` is the bare
                // payload key, so its row id is built here.
                Some(match (field.as_str(), in_scope) {
                    ("key", Some(scope)) => adb_store::space_id(namespace, scope, &value_text(value)),
                    _ => value_text(value),
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// The candidates of a read: current rows of `memory` passing the pushed-down part
/// of `predicate`, as records.
async fn candidates(
    store: &PartitionStore,
    memory: MemoryType,
    predicate: &Predicate,
    modifiers: &Modifiers,
    now: CommitTs,
    source: Source,
) -> AdbResult<Candidates> {
    // The field index first, from either source: it answers from the current rows,
    // as the cache and Lance do, without visiting every row (PF-27).
    if keyed(memory, predicate, modifiers.in_scope.as_deref(), modifiers.namespace.as_deref()).is_none() {
        if let Some(c) = indexed(store, memory, predicate, modifiers, now).await? {
            return Ok(c);
        }
    }
    if source == Source::Cache {
        if let Some(c) = cached(store, memory, predicate, modifiers, now) {
            return Ok(c);
        }
    }
    let lance = |records: Vec<MemoryRecord>| Ok(Candidates { records: records.into_iter().map(Arc::new).collect(), cached: false, floor: None });
    let m: Memory = memory.into();
    // KEY lookups: the primary-key fast path first (W7).
    if let Some(key) = keyed(memory, predicate, modifiers.in_scope.as_deref(), modifiers.namespace.as_deref()) {
        if let Some(r) = point(store, m, &key, modifiers.in_scope.as_deref(), modifiers.namespace.as_deref(), now).await? {
            return lance(vec![record(&r)]);
        }
    }
    let mut filter = records_of(m, modifiers.in_scope.as_deref(), modifiers.namespace.as_deref());
    // Payload pushdown where the payload is the record (PROCEDURAL / TOOLS conditions
    // see shaped records with defaults the payload may lack).
    if !matches!(memory, MemoryType::Procedural | MemoryType::Tools) {
        if let Some(p) = predicate_filter(m, predicate) {
            filter = filter.and(p);
        }
    }
    lance(current_rows(store, filter, now, false).await?.iter().map(record).collect())
}

/// The `field = literal` conditions a predicate requires (a top-level AND chain), as
/// (field, index key) pairs; empty when it has none, or any OR.
fn required_equalities(predicate: &Predicate) -> Vec<(String, String)> {
    let Predicate::Where { conditions } = predicate else { return Vec::new() };
    if conditions.iter().skip(1).any(|c| matches!(c.logical_op(), Some(adb_core::LogicalOp::Or))) {
        return Vec::new();
    }
    conditions
        .iter()
        .filter_map(|c| match c {
            Condition::Simple { field, operator: adb_core::Operator::Eq, value, .. } if field != "id" && !field.is_empty() && !field.contains('.') => {
                adb_store::field_index::literal_key(value).map(|k| (field.clone(), k))
            }
            _ => None,
        })
        .collect()
}

/// How many records a read can stop at, when the index's order is the read's order:
/// default order, a LIMIT, nothing that needs every match (ORDER BY, WINDOW,
/// AGGREGATE, GROUP BY, HAVING, links).
fn early_stop(modifiers: &Modifiers) -> Option<usize> {
    let all = modifiers.order_by.is_some()
        || modifiers.window.is_some()
        || modifiers.aggregate.is_some()
        || modifiers.group_by.is_some()
        || modifiers.having.is_some()
        || modifiers.follow_links.is_some()
        || modifiers.with_links.is_some();
    (!all).then_some(modifiers.limit).flatten()
}

/// Most rows an index read fetches from Lance by key (the rest come from the hot
/// cache); past it the read scans the type instead (PF-27).
const INDEX_FETCH_MAX: usize = 256;

/// Build the (`memory`, `field`) index from the type's current rows (PF-27).
async fn build_index(store: &PartitionStore, memory: Memory, field: &str) -> AdbResult<()> {
    let t0 = std::time::Instant::now();
    let filter = format!("memory = '{}' AND {} AND tombstone = false AND superseded_by IS NULL", memory.tag(), adb_store::RECORD_KINDS);
    match store.scan(Some(&filter)).await {
        Ok(rows) => {
            store.fields().finish(memory, field, &rows);
            tracing::debug!(target: "adb::index", ?memory, field, rows = rows.len(), ms = t0.elapsed().as_millis() as u64, "field index scan");
            Ok(())
        }
        Err(e) => {
            store.fields().abandon(memory, field);
            Err(store_err(e))
        }
    }
}

/// Candidates from the field index (PF-27): the records holding the value of one
/// required `field = literal`, fetched by id (the hot cache first, then Lance), in
/// the read's default order — stopping at LIMIT where the read allows. `None`: no
/// usable index (no such condition, a field not indexable, a build under way).
async fn indexed(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs) -> AdbResult<Option<Candidates>> {
    let m: Memory = memory.into();
    let fields = store.fields();
    if !fields.enabled() || !adb_store::field_index::indexable(m) {
        return Ok(None);
    }
    let eqs = required_equalities(predicate);
    if eqs.is_empty() {
        return Ok(None);
    }
    // The built index with the fewest records for its value; else build the first.
    let best = eqs.iter().filter_map(|(f, k)| fields.count(m, f, k).map(|n| (f, k, n))).min_by_key(|(_, _, n)| *n);
    let (field, key) = match best {
        Some((f, k, _)) => (f.clone(), k.clone()),
        None => {
            let (f, k) = &eqs[0];
            if !fields.begin(m, f) {
                return Ok(None);
            }
            build_index(store, m, f).await?;
            (f.clone(), k.clone())
        }
    };
    let adb_store::Lookup::Ready(mut entries) = fields.lookup(m, &field, &key) else { return Ok(None) };
    if memory == MemoryType::Episodic {
        entries.reverse(); // newest first, as EPISODIC reads come
    }
    let (in_scope, namespace) = (modifiers.in_scope.as_deref(), modifiers.namespace.as_deref());
    let ok = |r: &StoredRow| {
        r.is_current(now) && in_scope.is_none_or(|s| r.data.scope == s) && namespace.is_none_or(|ns| r.data.namespace.as_deref() == Some(ns))
    };
    let stop = early_stop(modifiers);
    let hot = store.hot();
    let mut out: Vec<Arc<MemoryRecord>> = Vec::new();
    let mut matched = 0usize;
    let mut fetched = 0usize;
    let page = stop.map_or(512, |k| (k * 2).clamp(16, 512));
    let mut i = 0;
    while i < entries.len() {
        let chunk = &entries[i..(i + page).min(entries.len())];
        i += chunk.len();
        let mut rows: HashMap<&str, Arc<MemoryRecord>> = HashMap::new();
        let mut missing: Vec<&str> = Vec::new();
        for (_, id, lineage) in chunk {
            match hot.as_ref().and_then(|h| h.lineage(m, lineage)) {
                Some(Some(c)) if c.row.id.as_str() == &**id => {
                    if ok(&c.row) {
                        rows.insert(id, cached_record(&c));
                    }
                }
                _ => missing.push(id),
            }
        }
        // Rows the cache doesn't hold: by primary key, one at a time (an `id IN (…)`
        // filter scans the whole table). Past INDEX_FETCH_MAX of them, one scan of the
        // type is cheaper: the regular path.
        fetched += missing.len();
        if fetched > INDEX_FETCH_MAX {
            return Ok(None);
        }
        for group in missing.chunks(16) {
            let got = futures::future::join_all(group.iter().map(|id| store.get(id))).await;
            for (id, r) in group.iter().zip(got) {
                if let Some(r) = r.map_err(store_err)? {
                    if ok(&r) {
                        rows.insert(id, Arc::new(record(&r)));
                    }
                }
            }
        }
        // In index order; count the ones the whole predicate takes, to stop early.
        let mut last_ts = None;
        for (ts, id, _) in chunk {
            if let Some(r) = rows.remove(&**id) {
                if matches(memory, &r, predicate) && modifiers.scope.as_ref().is_none_or(|v| &r.metadata.scope == v) {
                    matched += 1;
                    last_ts = Some(*ts);
                }
                out.push(r);
            }
        }
        // Enough, and the next entry isn't a tie with the last one taken (ties are
        // ordered by id afterwards, so all of a tie must be in).
        if let (Some(k), Some(t)) = (stop, last_ts) {
            if matched >= k && entries.get(i).is_none_or(|e| e.0 != t) {
                break;
            }
        }
    }
    Ok(Some(Candidates { records: out, cached: false, floor: None }))
}

/// `RECALL` / `LOOKUP` (not `LIKE`): candidates → exact predicate → visibility →
/// order → `LIMIT` → `RETURN`. `PATTERN` (PROCEDURAL) ranks by token overlap, its
/// score in `metadata.confidence`.
pub async fn read(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    served(store, "read", |source| read_from(store, memory, predicate, modifiers, now, source), same_records, show_records).await
}

async fn read_from(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs, source: Source) -> AdbResult<Answer<Vec<MemoryRecord>>> {
    let c = candidates(store, memory, predicate, modifiers, now, source).await?;
    if let Predicate::Pattern { pattern_var, threshold, text } = predicate {
        if memory != MemoryType::Procedural {
            return Ok(Answer::of(&c, Vec::new()));
        }
        if c.floor.is_some() {
            return Ok(Answer::Insufficient);
        }
        // The literal or bound value; an unbound `$name` matches its name (as before).
        let query = text.as_deref().unwrap_or(pattern_var.trim_start_matches('$'));
        let threshold = threshold.unwrap_or(0.7);
        let mut scored: Vec<(f32, Arc<MemoryRecord>)> = c
            .records
            .iter()
            .filter_map(|r| {
                let s = jaccard(query, r.data["pattern"].as_str().unwrap_or(""));
                (s >= threshold).then(|| (s, r.clone()))
            })
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal).then_with(|| a.1.id.cmp(&b.1.id)));
        if let Some(n) = modifiers.limit {
            scored.truncate(n);
        }
        // The overlap score is the match's confidence, as LIKE's similarity is (F-L2).
        let out = scored
            .into_iter()
            .map(|(s, r)| {
                let mut r = (*r).clone();
                r.metadata.confidence = Some(s);
                r
            })
            .collect();
        return Ok(Answer::of(&c, out));
    }
    let mut records: Vec<Arc<MemoryRecord>> = c.records.iter().filter(|r| matches(memory, r, predicate)).cloned().collect();
    if let Some(v) = &modifiers.scope {
        records.retain(|r| &r.metadata.scope == v);
    }
    if let Some(w) = &modifiers.window {
        // A window needs every episode it might start from.
        if c.floor.is_some() {
            return Ok(Answer::Insufficient);
        }
        records = in_window(records, w);
    }
    match (&modifiers.order_by, modifiers.limit) {
        // ORDER BY … LIMIT k (C-11): the k first in the full order, without sorting
        // every record — same answer, as the order is total (ties by default order).
        (Some(o), Some(k)) if c.floor.is_none() && k < records.len() => top_k(memory, &mut records, o, k),
        (o, _) => {
            default_order(memory, &mut records);
            if let Some(o) = o {
                order_by(&mut records, o);
            }
        }
    }
    // From an incomplete bucket: only a newest-first read whose rows are all newer
    // than the floor (rows that may be missing are older, so they'd rank below).
    if let Some(floor) = c.floor {
        let newest_first = memory == MemoryType::Episodic && modifiers.order_by.is_none();
        let enough = modifiers.limit.is_some_and(|k| k == 0 || (records.len() >= k && records[k - 1].metadata.created_at > floor.to_datetime()));
        if !(newest_first && enough) {
            return Ok(Answer::Insufficient);
        }
    }
    if let Some(n) = modifiers.limit {
        records.truncate(n);
    }
    let mut records = owned(records);
    if let Some(f) = &modifiers.return_fields {
        project(&mut records, f);
    }
    Ok(Answer::of(&c, records))
}

/// Whether a read asks for history (AQL v0.10): `AS OF`, `VALID AT` or `WITH HISTORY`.
pub fn is_history(modifiers: &Modifiers) -> bool {
    modifiers.as_of.is_some() || modifiers.valid_at.is_some() || modifiers.with_history
}

/// A history read's time, at `now` for `AGO`.
fn instant(t: &adb_core::TimePoint, what: &str, now: CommitTs) -> AdbResult<CommitTs> {
    t.resolve(now.to_datetime()).map(CommitTs::from_datetime).ok_or_else(|| match t {
        adb_core::TimePoint::Variable { name } => AdbError::QueryError(format!("{what} ${name}: the variable has no value (pass a date or time in `variables`)")),
        _ => AdbError::QueryError(format!("{what}: not a date (\"2026-11-01\") or an RFC 3339 time")),
    })
}

/// `RECALL` / `LOOKUP` with `AS OF`, `VALID AT` or `WITH HISTORY` (AQL v0.10, L9 /
/// GR-14) over SEMANTIC or PROCEDURAL, from Lance (the hot cache holds current rows
/// only). `AS OF t`: each record's version at t, if it held at t; `VALID AT v`: facts
/// holding at v, from the versions current at `AS OF` (default now); `WITH HISTORY`:
/// every version recorded by `AS OF`, whatever its validity. Each record carries
/// `_recorded_at` (its version's commit) and `_replaced_at` (when a newer version
/// replaced it, by `AS OF`; else null). Versions superseded more than
/// `writes::HISTORY` ago are compacted away; forgotten and expired records have none.
pub async fn history(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    if matches!(memory, MemoryType::Working) {
        return Err(AdbError::UnsupportedOperation("AS OF and WITH HISTORY aren't valid on WORKING: it's session state, without versions".into()));
    }
    if modifiers.follow_links.is_some() || modifiers.with_links.is_some() || matches!(predicate, Predicate::Pattern { .. }) {
        return Err(AdbError::UnsupportedOperation("FOLLOW LINKS, WITH LINKS and PATTERN don't read history (AS OF, VALID AT, WITH HISTORY)".into()));
    }
    let tx = modifiers.as_of.as_ref().map(|t| instant(t, "AS OF", now)).transpose()?.unwrap_or(now);
    let valid = modifiers.valid_at.as_ref().map(|t| instant(t, "VALID AT", now)).transpose()?.unwrap_or(tx);
    let (in_scope, namespace) = (modifiers.in_scope.as_deref(), modifiers.namespace.as_deref());
    let rows = store.versions(memory.into(), None).await.map_err(store_err)?;
    let micros = |t: CommitTs| serde_json::json!(t.to_datetime().to_rfc3339_opts(chrono::SecondsFormat::Micros, true));
    let mut records: Vec<MemoryRecord> = rows
        .iter()
        .filter(|r| in_scope.is_none_or(|s| r.data.scope == s) && namespace.is_none_or(|ns| r.data.namespace.as_deref() == Some(ns)))
        .filter(|r| match modifiers.with_history {
            true => r.commit_ts <= tx && (modifiers.valid_at.is_none() || r.holds_at(valid)),
            false => r.is_current_at(tx, valid),
        })
        .map(|r| {
            let mut rec = record(r);
            if let Some(obj) = rec.data.as_object_mut() {
                obj.insert("_recorded_at".into(), micros(r.commit_ts));
                obj.insert("_replaced_at".into(), r.valid_to.filter(|t| *t <= tx).map_or(serde_json::Value::Null, micros));
            }
            rec
        })
        .filter(|r| matches(memory, r, predicate))
        .collect();
    if let Some(v) = &modifiers.scope {
        records.retain(|r| &r.metadata.scope == v);
    }
    default_order(memory, &mut records);
    if let Some(o) = &modifiers.order_by {
        order_by(&mut records, o);
    }
    if let Some(n) = modifiers.limit {
        records.truncate(n);
    }
    if let Some(f) = &modifiers.return_fields {
        project(&mut records, f);
    }
    Ok(records)
}

/// `RECALL` / `LOOKUP` on a branch engine (B2): the branch's view (its rows, then its
/// parent's as of the point), then the predicate and modifiers as on main.
pub fn branch_read(rows: Vec<StoredRow>, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
    if is_history(modifiers) {
        return Err(AdbError::UnsupportedOperation("AS OF, VALID AT and WITH HISTORY are not read on a branch yet".into()));
    }
    if modifiers.follow_links.is_some() || modifiers.with_links.is_some() || matches!(predicate, Predicate::Pattern { .. }) {
        return Err(AdbError::UnsupportedOperation("FOLLOW LINKS, WITH LINKS and PATTERN are not read on a branch yet".into()));
    }
    let in_scope = modifiers.in_scope.as_deref();
    let mut records: Vec<MemoryRecord> = rows
        .iter()
        .filter(|r| in_scope.is_none_or(|s| r.data.scope == s))
        .map(record)
        .filter(|r| matches(memory, r, predicate))
        .collect();
    if let Some(v) = &modifiers.scope {
        records.retain(|r| &r.metadata.scope == v);
    }
    default_order(memory, &mut records);
    if let Some(o) = &modifiers.order_by {
        order_by(&mut records, o);
    }
    if let Some(n) = modifiers.limit {
        records.truncate(n);
    }
    if let Some(f) = &modifiers.return_fields {
        project(&mut records, f);
    }
    Ok(records)
}

/// `SCAN FROM WORKING [WINDOW …]`: oldest first, then the window, `LIMIT`, `RETURN`.
pub async fn scan(store: &PartitionStore, modifiers: &Modifiers, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    served(store, "scan", |source| scan_from(store, modifiers, now, source), same_records, show_records).await
}

async fn scan_from(store: &PartitionStore, modifiers: &Modifiers, now: CommitTs, source: Source) -> AdbResult<Answer<Vec<MemoryRecord>>> {
    let c = candidates(store, MemoryType::Working, &Predicate::All, modifiers, now, source).await?;
    let mut records = c.records.clone();
    default_order(MemoryType::Working, &mut records);
    // From an incomplete bucket: a window over the newest rows, all newer than the floor.
    if let Some(floor) = c.floor {
        let floor = floor.to_datetime();
        let covered = match &modifiers.window {
            Some(Window::LastN { count }) => *count == 0 || (records.len() >= *count && records[records.len() - count].metadata.created_at > floor),
            Some(Window::LastDuration { duration }) => adb_core::time::now() - chrono::Duration::from_std(*duration).unwrap_or_default() >= floor,
            _ => false,
        };
        if !covered {
            return Ok(Answer::Insufficient);
        }
    }
    if let Some(w) = &modifiers.window {
        records = window(records, w);
    }
    if let Some(n) = modifiers.limit {
        records.truncate(n);
    }
    let mut records = owned(records);
    if let Some(f) = &modifiers.return_fields {
        project(&mut records, f);
    }
    Ok(Answer::of(&c, records))
}

/// `LOAD FROM TOOLS`: matching tools by ranking (blended 70 / 30 with the relevance
/// for `task` / `task_type` when the WHERE names one), at most `limit`.
pub async fn load(store: &PartitionStore, predicate: &Predicate, limit: usize, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    served(store, "load", |source| load_from(store, predicate, limit, now, source), same_records, show_records).await
}

async fn load_from(store: &PartitionStore, predicate: &Predicate, limit: usize, now: CommitTs, source: Source) -> AdbResult<Answer<Vec<MemoryRecord>>> {
    let c = candidates(store, MemoryType::Tools, predicate, &Modifiers::default(), now, source).await?;
    if c.floor.is_some() {
        return Ok(Answer::Insufficient);
    }
    let mut records: Vec<Arc<MemoryRecord>> = c.records.iter().filter(|r| matches(MemoryType::Tools, r, predicate)).cloned().collect();
    let task = match predicate {
        Predicate::Where { conditions } => conditions.iter().find_map(|c| match c {
            Condition::Simple { field, value: Value::String(s), .. } if field == "task" || field == "task_type" => Some(s.clone()),
            _ => None,
        }),
        _ => None,
    };
    let score = |r: &Arc<MemoryRecord>| {
        let ranking = r.data["ranking"].as_f64().unwrap_or(0.5);
        match &task {
            Some(t) => ranking * 0.7 + r.data["relevance_scores"].get(t).and_then(|v| v.as_f64()).unwrap_or(0.5) * 0.3,
            None => ranking,
        }
    };
    default_order(MemoryType::Tools, &mut records);
    records.sort_by(|a, b| score(b).partial_cmp(&score(a)).unwrap_or(Ordering::Equal));
    records.truncate(limit);
    Ok(Answer::of(&c, owned(records)))
}

/// The keys (or, with `history`, the row ids of every version) that UPDATE /
/// FORGET with `predicate` touch.
pub async fn targets(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>, now: CommitTs) -> AdbResult<Vec<String>> {
    let sorted = |v: &Vec<String>| {
        let mut v = v.clone();
        v.sort();
        v
    };
    served(store, "targets", |source| targets_from(store, memory, predicate, in_scope, namespace, now, source), |a, b| sorted(a) == sorted(b), |v| format!("{:?}", sorted(v)))
        .await
}

async fn targets_from(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>, now: CommitTs, source: Source) -> AdbResult<Answer<Vec<String>>> {
    let modifiers = Modifiers { in_scope: in_scope.map(String::from), namespace: namespace.map(String::from), ..Default::default() };
    let c = candidates(store, memory, predicate, &modifiers, now, source).await?;
    if c.floor.is_some() {
        return Ok(Answer::Insufficient);
    }
    // A procedure's or tool's record id is its payload id (bare): its lineage carries
    // the namespace (NS).
    let lineage = |id: &str| match (memory, namespace) {
        (MemoryType::Procedural | MemoryType::Tools, Some(_)) => adb_store::space_id(namespace, in_scope.unwrap_or(adb_store::DEFAULT_SCOPE), id),
        _ => id.to_string(),
    };
    let ids = c.records.iter().filter(|r| matches(memory, r, predicate)).map(|r| lineage(&r.id)).collect();
    Ok(Answer::of(&c, ids))
}

// ------------------------------------------------------------------ LIKE

/// `RECALL … LIKE` (W4): every current row of `memory` passing `IN SCOPE` /
/// `NAMESPACE`, ranked by [`crate::rank`] against the query text and / or vector;
/// `MIN_CONFIDENCE`, `LIMIT` (default 10), then `ORDER BY` and `RETURN` over the
/// selected. From the hot cache when it holds the whole memory type (scope) with
/// vectors (W10-L1).
pub async fn similar(store: &PartitionStore, memory: MemoryType, modifiers: &Modifiers, text: Option<&str>, vector: Option<&[f32]>, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    served(store, "like", |source| similar_from(store, memory, modifiers, text, vector, now, source), same_records, show_records).await
}

async fn similar_from(
    store: &PartitionStore,
    memory: MemoryType,
    modifiers: &Modifiers,
    text: Option<&str>,
    vector: Option<&[f32]>,
    now: CommitTs,
    source: Source,
) -> AdbResult<Answer<Vec<MemoryRecord>>> {
    let m: Memory = memory.into();
    let in_scope = modifiers.in_scope.as_deref();
    let hot = store.hot().filter(|h| source == Source::Cache && h.tracks(m));
    let (mut rows, cached): (Vec<Arc<StoredRow>>, bool) = match hot {
        Some(h) => {
            let Some(hit) = h.rows(m, in_scope).filter(|hit| hit.floor.is_none() && h.vectors()) else { return Ok(Answer::Insufficient) };
            (hit.rows.into_iter().map(|c| c.row).filter(|r| r.is_current(now)).collect(), true)
        }
        None => {
            let kinds = ["entity", "fact", "episode", "tool"].map(lit).to_vec();
            let mut filter = col("memory").eq(lit(m.tag())).and(col("kind").in_list(kinds, false));
            if let Some(s) = in_scope {
                filter = filter.and(col("scope").eq(lit(s.to_string())));
            }
            (current_rows(store, filter, now, true).await?.into_iter().map(Arc::new).collect(), false)
        }
    };
    if let Some(ns) = &modifiers.namespace {
        rows.retain(|r| r.data.namespace.as_ref() == Some(ns));
    }
    let out = rank_rows(rows, modifiers, text, vector, now);
    Ok(if cached { Answer::Cache(out) } else { Answer::Lance(out) })
}

/// `LIKE`'s ranking of candidate `rows` (already filtered by scope and namespace):
/// `WINDOW`, rank against the text / vector, `MIN_CONFIDENCE`, `LIMIT`, then
/// `ORDER BY` and `RETURN`. B7: a space with layers ranks its merged rows here.
pub fn rank_rows(mut rows: Vec<Arc<StoredRow>>, modifiers: &Modifiers, text: Option<&str>, vector: Option<&[f32]>, now: CommitTs) -> Vec<MemoryRecord> {
    // One order whatever the source (ranking breaks ties by position).
    rows.sort_by(|a, b| (a.commit_ts, &a.id).cmp(&(b.commit_ts, &b.id)));
    if let Some(w) = &modifiers.window {
        struct At(usize, MemoryRecord);
        impl Borrow<MemoryRecord> for At {
            fn borrow(&self) -> &MemoryRecord {
                &self.1
            }
        }
        let keep: HashSet<usize> = in_window(rows.iter().enumerate().map(|(i, r)| At(i, record(r))).collect(), w).into_iter().map(|a| a.0).collect();
        let mut i = 0;
        rows.retain(|_| {
            i += 1;
            keep.contains(&(i - 1))
        });
    }
    let vectors: Vec<Option<Vec<f32>>> = rows.iter().map(|r| r.data.embedding.as_ref().map(|e| e.iter().map(|x| x.to_f32()).collect())).collect();
    let texts: Vec<Option<String>> = rows.iter().map(|r| crate::reads::embed_text(r.data.memory.into(), &r.data.value)).collect();
    let candidates: Vec<crate::rank::Candidate> =
        rows.iter().enumerate().map(|(i, r)| crate::rank::Candidate { embedding: vectors[i].as_deref(), text: texts[i].as_deref(), at: r.commit_ts }).collect();
    let ranked = crate::rank::rank(&candidates, vector, text, modifiers.min_confidence, now);
    let limit = modifiers.limit.unwrap_or(10);
    let mut out: Vec<MemoryRecord> = ranked
        .into_iter()
        .take(limit)
        .map(|r| {
            let mut rec = record(&rows[r.index]);
            rec.metadata.confidence = r.similarity;
            rec
        })
        .collect();
    if let Some(order) = &modifiers.order_by {
        out.sort_by(|a, b| {
            let (x, y) = (a.order_value(&order.field), b.order_value(&order.field));
            let c = match (x.as_ref(), y.as_ref()) {
                (Some(x), Some(y)) => x.to_string().cmp(&y.to_string()),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            if order.ascending { c } else { c.reverse() }
        });
    }
    if let Some(fields) = &modifiers.return_fields {
        for r in out.iter_mut() {
            if let serde_json::Value::Object(m) = &mut r.data {
                m.retain(|k, _| fields.iter().any(|f| f == k || f == "*"));
            }
        }
    }
    out
}

// ------------------------------------------------------------------ aggregates

fn number(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// `AGGREGATE COUNT / SUM / AVG / MIN / MAX` over `records` (SUM of nothing is 0,
/// the others null).
pub fn aggregate(records: &[MemoryRecord], funcs: &[AggregateFunc]) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for f in funcs {
        let alias = f.alias.clone().unwrap_or_else(|| format!("{:?}", f.func).to_lowercase());
        let values: Vec<f64> = match &f.field {
            Some(field) => records.iter().filter_map(|r| r.data.get(field)).filter_map(number).collect(),
            None => Vec::new(),
        };
        let num = |x: f64| serde_json::Number::from_f64(if x == 0.0 { 0.0 } else { x }).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null);
        let v = match (f.func, &f.field) {
            (AggregateFuncType::Count, _) => serde_json::json!(records.len()),
            (_, None) => serde_json::Value::Null,
            (AggregateFuncType::Sum, _) if values.is_empty() => serde_json::json!(0),
            (AggregateFuncType::Sum, _) => num(values.iter().sum()),
            (_, _) if values.is_empty() => serde_json::Value::Null,
            (AggregateFuncType::Avg, _) => num(values.iter().sum::<f64>() / values.len() as f64),
            (AggregateFuncType::Min, _) => num(values.iter().cloned().fold(f64::INFINITY, f64::min)),
            (AggregateFuncType::Max, _) => num(values.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
        };
        out.insert(alias, v);
    }
    serde_json::Value::Object(out)
}

/// Sets `path` (dotted) in `row`, making the objects on the way.
fn set_path(row: &mut serde_json::Value, path: &str, v: serde_json::Value) {
    let mut cur = row;
    let mut parts = path.split('.').peekable();
    while let Some(p) = parts.next() {
        let obj = cur.as_object_mut().expect("group rows are objects");
        if parts.peek().is_none() {
            obj.insert(p.to_string(), v);
            return;
        }
        cur = obj.entry(p.to_string()).or_insert_with(|| serde_json::json!({}));
        if !cur.is_object() {
            *cur = serde_json::json!({});
        }
    }
}

/// `GROUP BY … AGGREGATE … [HAVING] [ORDER BY] [LIMIT] [RETURN]` (AQL v0.9): one row
/// per distinct value of `fields` (a missing field groups as null), holding the group
/// fields (dotted ones nested) and the aggregates. Groups come in the order their
/// first record does; `HAVING`, `ORDER BY`, `LIMIT` and `RETURN` apply to the rows.
pub fn group_aggregate(records: &[MemoryRecord], fields: &[String], modifiers: &Modifiers) -> Vec<serde_json::Value> {
    let funcs = modifiers.aggregate.as_deref().unwrap_or_default();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<(Vec<serde_json::Value>, Vec<MemoryRecord>)> = Vec::new();
    for r in records {
        let key: Vec<serde_json::Value> = fields.iter().map(|f| r.resolve_field(f).unwrap_or(serde_json::Value::Null)).collect();
        let i = *index.entry(serde_json::Value::Array(key.clone()).to_string()).or_insert_with(|| {
            groups.push((key, Vec::new()));
            groups.len() - 1
        });
        groups[i].1.push(r.clone());
    }
    let mut rows: Vec<serde_json::Value> = groups
        .into_iter()
        .map(|(key, members)| {
            let mut row = serde_json::json!({});
            for (f, v) in fields.iter().zip(key) {
                set_path(&mut row, f, v);
            }
            if let serde_json::Value::Object(aggs) = aggregate(&members, funcs) {
                for (k, v) in aggs {
                    row[k] = v;
                }
            }
            row
        })
        .collect();
    if let Some(having) = &modifiers.having {
        rows.retain(|row| evaluate_conditions(row, having));
    }
    if let Some(o) = &modifiers.order_by {
        let mut keyed: Vec<(Option<serde_json::Value>, serde_json::Value)> =
            rows.into_iter().map(|r| (adb_core::get_nested_field(&r, &o.field).filter(|v| !v.is_null()).cloned(), r)).collect();
        keyed.sort_by(|(a, _), (b, _)| {
            let c = match (a, b) {
                (Some(x), Some(y)) => cmp_json(x, y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            if o.ascending { c } else { c.reverse() }
        });
        rows = keyed.into_iter().map(|(_, r)| r).collect();
    }
    if let Some(n) = modifiers.limit {
        rows.truncate(n);
    }
    if let Some(keep) = &modifiers.return_fields {
        rows = rows
            .into_iter()
            .map(|r| {
                let mut out = serde_json::json!({});
                for f in keep {
                    if let Some(v) = adb_core::get_nested_field(&r, f) {
                        set_path(&mut out, f, v.clone());
                    }
                }
                out
            })
            .collect();
    }
    rows
}

// ------------------------------------------------------------------ links

/// The current links leaving each of `keys` (records of `memory`), of `link_type`
/// if given, oldest first: one scan for all of them (P4).
pub async fn links_from(store: &PartitionStore, memory: MemoryType, keys: &[String], link_type: Option<&str>, now: CommitTs) -> AdbResult<HashMap<String, Vec<Link>>> {
    let mut out: HashMap<String, Vec<Link>> = HashMap::new();
    if keys.is_empty() {
        return Ok(out);
    }
    let m: Memory = memory.into();
    for chunk in keys.chunks(500) {
        let ids: Vec<Expr> = chunk.iter().map(|k| lit(row_id(m, k))).collect();
        let mut filter = col("kind").eq(lit("edge")).and(col("subject_id").in_list(ids, false));
        if let Some(t) = link_type {
            filter = filter.and(col("link_type").eq(lit(t.to_string())));
        }
        let mut rows = current_rows(store, filter, now, false).await?;
        rows.sort_by(|a, b| (a.commit_ts, &a.id).cmp(&(b.commit_ts, &b.id)));
        for l in rows.iter().filter_map(to_link) {
            out.entry(l.from_id.clone()).or_default().push(l);
        }
    }
    Ok(out)
}

/// The links touching each of `keys` of `memory` that `select` picks (AQL v0.8):
/// leaving it (outbound), pointing at it (inbound) or both, by that key, oldest
/// first. A link between two of `keys` is listed under each end it is selected for.
/// From the hot cache when it holds every link (P1.3).
pub async fn links_touching(store: &PartitionStore, memory: MemoryType, keys: &[String], select: &adb_core::LinkSelect, now: CommitTs) -> AdbResult<HashMap<String, Vec<Link>>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let ids = |m: &HashMap<String, Vec<Link>>| {
        let mut v: Vec<(&String, Vec<&String>)> = m.iter().map(|(k, ls)| (k, ls.iter().map(|l| &l.id).collect())).collect();
        v.sort();
        format!("{v:?}")
    };
    served(store, "links", |source| links_touching_from(store, memory, keys, select, now, source), |a, b| ids(a) == ids(b), ids).await
}

async fn links_touching_from(
    store: &PartitionStore,
    memory: MemoryType,
    keys: &[String],
    select: &adb_core::LinkSelect,
    now: CommitTs,
    source: Source,
) -> AdbResult<Answer<HashMap<String, Vec<Link>>>> {
    let m: Memory = memory.into();
    let ends: Vec<String> = keys.iter().map(|k| row_id(m, k)).collect();
    let picked = |r: &StoredRow| select.types.is_empty() || r.data.link_type.as_ref().is_some_and(|t| select.types.contains(t));
    if let Some(hot) = store.hot().filter(|_| source == Source::Cache) {
        if let Some(edges) = hot.edges_touching(&ends) {
            let rows: Vec<StoredRow> = edges.iter().filter(|r| r.is_current(now) && picked(r)).map(|r| (**r).clone()).collect();
            return Ok(Answer::Cache(links_by_end(rows, memory, keys, select)));
        }
    }
    let mut rows = Vec::new();
    for chunk in ends.chunks(500) {
        let ids: Vec<Expr> = chunk.iter().map(|e| lit(e.clone())).collect();
        let by_end = match select.direction {
            adb_core::LinkDirection::Outbound => col("subject_id").in_list(ids, false),
            adb_core::LinkDirection::Inbound => col("object_id").in_list(ids, false),
            adb_core::LinkDirection::Both => col("subject_id").in_list(ids.clone(), false).or(col("object_id").in_list(ids, false)),
        };
        // Links: edges, and facts whose object is an entity (P1.4).
        let mut filter = col("kind").in_list(vec![lit("edge"), lit("fact")], false).and(col("object_id").is_not_null()).and(by_end);
        if !select.types.is_empty() {
            filter = filter.and(col("link_type").in_list(select.types.iter().map(|t| lit(t.clone())).collect(), false));
        }
        rows.extend(current_rows(store, filter, now, false).await?);
    }
    // A link between two chunks' keys comes back twice.
    let mut seen = HashSet::new();
    rows.retain(|r| seen.insert(r.id.clone()));
    Ok(Answer::Lance(links_by_end(rows, memory, keys, select)))
}

/// Edge rows as links under the key of each end `select` picks, oldest first.
fn links_by_end(mut rows: Vec<StoredRow>, memory: MemoryType, keys: &[String], select: &adb_core::LinkSelect) -> HashMap<String, Vec<Link>> {
    rows.sort_by(|a, b| (a.commit_ts, &a.id).cmp(&(b.commit_ts, &b.id)));
    let wanted: HashSet<&String> = keys.iter().collect();
    let mut out: HashMap<String, Vec<Link>> = HashMap::new();
    for l in rows.iter().filter_map(to_link).filter(|l| select.matches(l)) {
        if select.direction.outbound() && l.from_type == memory && wanted.contains(&l.from_id) {
            out.entry(l.from_id.clone()).or_default().push(l.clone());
        }
        if select.direction.inbound() && l.to_type == memory && wanted.contains(&l.to_id) {
            out.entry(l.to_id.clone()).or_default().push(l);
        }
    }
    out
}

/// Link targets fetched by point lookup rather than a scan (each ~1 ms; a scan
/// costs ~10–40 ms).
const POINT_LOOKUPS: usize = 16;

/// The current records `keys` of `memory`, by key: from the hot cache when it can
/// say for every key, else point lookups for a few, one scan for the rest.
pub async fn by_keys(store: &PartitionStore, memory: MemoryType, keys: &[String], now: CommitTs) -> AdbResult<HashMap<String, MemoryRecord>> {
    let same = |a: &HashMap<String, MemoryRecord>, b: &HashMap<String, MemoryRecord>| {
        let sorted = |m: &HashMap<String, MemoryRecord>| serde_json::to_value(m.iter().collect::<std::collections::BTreeMap<_, _>>()).ok();
        sorted(a) == sorted(b)
    };
    let show = |m: &HashMap<String, MemoryRecord>| {
        let mut k: Vec<&String> = m.keys().collect();
        k.sort();
        format!("{k:?}")
    };
    served(store, "by_keys", |source| by_keys_from(store, memory, keys, now, source), same, show).await
}

async fn by_keys_from(store: &PartitionStore, memory: MemoryType, keys: &[String], now: CommitTs, source: Source) -> AdbResult<Answer<HashMap<String, MemoryRecord>>> {
    let mut out = HashMap::new();
    let m: Memory = memory.into();
    if let Some(hot) = store.hot().filter(|h| source == Source::Cache && h.tracks(m)) {
        for k in keys {
            match hot.lineage(m, &row_id(m, k)) {
                Some(Some(c)) => {
                    if c.row.is_current(now) {
                        out.insert(c.row.key().to_string(), (*cached_record(&c)).clone());
                    }
                }
                Some(None) => {}
                None => return Ok(Answer::Insufficient),
            }
        }
        return Ok(Answer::Cache(out));
    }
    // A few targets: point lookups (W7); the rest in one scan.
    let mut rest: Vec<String> = Vec::new();
    if keys.len() <= POINT_LOOKUPS {
        for k in keys {
            match point(store, m, k, None, None, now).await? {
                Some(r) => {
                    out.insert(r.key().to_string(), record(&r));
                }
                None => rest.push(k.clone()),
            }
        }
    } else {
        rest = keys.to_vec();
    }
    for chunk in rest.chunks(500) {
        let ids: Vec<Expr> = chunk.iter().map(|k| lit(row_id(m, k))).collect();
        let filter = records_of(m, None, None).and(col("lineage_id").in_list(ids, false));
        for r in current_rows(store, filter, now, false).await? {
            out.insert(r.key().to_string(), record(&r));
        }
    }
    Ok(Answer::Lance(out))
}

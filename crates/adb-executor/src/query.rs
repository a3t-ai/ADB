//! The query engine (MVP W7, `W7_QUERY_ENGINE.md`): AQL reads as one DataFusion
//! scan of an LSM snapshot (current view, memory type, scope, namespace and a
//! superset of the payload filter pushed down; vectors only when asked for), then
//! AQL semantics once, for every memory type: exact WHERE / KEY / PATTERN, WINDOW,
//! ORDER BY, LIMIT, RETURN, aggregates, links in batches.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use adb_core::log::{CommitTs, Kind, Memory};
use adb_core::{
    evaluate_conditions, evaluate_conditions_on_record, AdbError, AdbResult, AggregateFunc, AggregateFuncType, Condition, Link,
    MemoryRecord, MemoryType, Metadata, Modifiers, OrderBy, Predicate, Scope, Value, Window,
};
use adb_store::{decode, predicate_filter, row_id, PartitionStore, StoredRow};
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
/// has always returned): defaults filled, `steps` a list, numeric `variables`.
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
    serde_json::json!({
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
    })
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
            // Conditions see the shaped node / tool record.
            MemoryType::Procedural | MemoryType::Tools => evaluate_conditions(&r.data, conditions),
            _ => evaluate_conditions_on_record(r, conditions),
        },
        Predicate::Key { field, value } => {
            let want = value_text(value);
            match (memory, field.as_str()) {
                (MemoryType::Procedural, "pattern_id" | "id") | (MemoryType::Tools, "tool_id" | "id") => r.id == want,
                (MemoryType::Procedural, "severity" | "source") | (MemoryType::Tools, "name" | "category") => {
                    matches!(value, Value::String(_)) && r.data.get(field).and_then(|v| v.as_str()) == Some(want.as_str())
                }
                (MemoryType::Procedural | MemoryType::Tools, _) => false,
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
pub fn order_by(records: &mut [MemoryRecord], order: &OrderBy) {
    records.sort_by(|a, b| {
        let c = match (a.order_value(&order.field), b.order_value(&order.field)) {
            (Some(x), Some(y)) => cmp_json(&x, &y),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if order.ascending { c } else { c.reverse() }
    });
}

/// The order without `ORDER BY`: EPISODIC newest first, the others oldest first;
/// ties by id.
pub fn default_order(memory: MemoryType, records: &mut [MemoryRecord]) {
    records.sort_by(|a, b| (a.metadata.created_at, &a.id).cmp(&(b.metadata.created_at, &b.id)));
    if memory == MemoryType::Episodic {
        records.reverse();
    }
}

/// `WINDOW` over records in default (oldest first) order.
pub fn window(mut records: Vec<MemoryRecord>, w: &Window) -> Vec<MemoryRecord> {
    match w {
        Window::LastN { count } => {
            let skip = records.len().saturating_sub(*count);
            records.split_off(skip)
        }
        Window::LastDuration { duration } => {
            let cutoff = adb_core::time::now() - chrono::Duration::from_std(*duration).unwrap_or_default();
            records.into_iter().filter(|r| r.metadata.created_at > cutoff).collect()
        }
        Window::TopBy { count, field } => {
            records.sort_by(|a, b| match (b.data.get(field), a.data.get(field)) {
                (Some(x), Some(y)) => cmp_json(x, y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            });
            records.truncate(*count);
            records
        }
        Window::Since { condition } => {
            let start = records.iter().position(|r| condition.matches(&r.data)).unwrap_or(records.len());
            records.split_off(start)
        }
    }
}

/// `RETURN` (dotted paths as `metadata.scope` too).
pub fn project(records: &mut [MemoryRecord], fields: &[String]) {
    for r in records.iter_mut() {
        r.data = r.project_fields(fields);
    }
}

// ------------------------------------------------------------------ reads

/// The candidates of a read: current rows of `memory` passing the pushed-down part
/// of `predicate`, as records.
async fn candidates(
    store: &PartitionStore,
    memory: MemoryType,
    predicate: &Predicate,
    modifiers: &Modifiers,
    now: CommitTs,
) -> AdbResult<Vec<MemoryRecord>> {
    let m: Memory = memory.into();
    let mut filter = records_of(m, modifiers.in_scope.as_deref(), modifiers.namespace.as_deref());
    // Payload pushdown where the payload is the record (PROCEDURAL / TOOLS conditions
    // see shaped records with defaults the payload may lack).
    if !matches!(memory, MemoryType::Procedural | MemoryType::Tools) {
        if let Some(p) = predicate_filter(m, predicate) {
            filter = filter.and(p);
        }
    }
    Ok(current_rows(store, filter, now, false).await?.iter().map(record).collect())
}

/// `RECALL` / `LOOKUP` (not `LIKE`): candidates → exact predicate → visibility →
/// order → `LIMIT` → `RETURN`. `PATTERN` (PROCEDURAL) ranks by token overlap.
pub async fn read(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    let mut records = candidates(store, memory, predicate, modifiers, now).await?;
    if let Predicate::Pattern { pattern_var, threshold } = predicate {
        if memory != MemoryType::Procedural {
            return Ok(Vec::new());
        }
        let query = pattern_var.trim_start_matches('$');
        let threshold = threshold.unwrap_or(0.7);
        let mut scored: Vec<(f32, MemoryRecord)> = records
            .into_iter()
            .filter_map(|r| {
                let s = jaccard(query, r.data["pattern"].as_str().unwrap_or(""));
                (s >= threshold).then_some((s, r))
            })
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal).then_with(|| a.1.id.cmp(&b.1.id)));
        let mut out: Vec<MemoryRecord> = scored.into_iter().map(|(_, r)| r).collect();
        if let Some(n) = modifiers.limit {
            out.truncate(n);
        }
        return Ok(out);
    }
    records.retain(|r| matches(memory, r, predicate));
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
    let mut records = candidates(store, MemoryType::Working, &Predicate::All, modifiers, now).await?;
    default_order(MemoryType::Working, &mut records);
    if let Some(w) = &modifiers.window {
        records = window(records, w);
    }
    if let Some(n) = modifiers.limit {
        records.truncate(n);
    }
    if let Some(f) = &modifiers.return_fields {
        project(&mut records, f);
    }
    Ok(records)
}

/// `LOAD FROM TOOLS`: matching tools by ranking (blended 70 / 30 with the relevance
/// for `task` / `task_type` when the WHERE names one), at most `limit`.
pub async fn load(store: &PartitionStore, predicate: &Predicate, limit: usize, now: CommitTs) -> AdbResult<Vec<MemoryRecord>> {
    let mut records = candidates(store, MemoryType::Tools, predicate, &Modifiers::default(), now).await?;
    records.retain(|r| matches(MemoryType::Tools, r, predicate));
    let task = match predicate {
        Predicate::Where { conditions } => conditions.iter().find_map(|c| match c {
            Condition::Simple { field, value: Value::String(s), .. } if field == "task" || field == "task_type" => Some(s.clone()),
            _ => None,
        }),
        _ => None,
    };
    let score = |r: &MemoryRecord| {
        let ranking = r.data["ranking"].as_f64().unwrap_or(0.5);
        match &task {
            Some(t) => ranking * 0.7 + r.data["relevance_scores"].get(t).and_then(|v| v.as_f64()).unwrap_or(0.5) * 0.3,
            None => ranking,
        }
    };
    default_order(MemoryType::Tools, &mut records);
    records.sort_by(|a, b| score(b).partial_cmp(&score(a)).unwrap_or(Ordering::Equal));
    records.truncate(limit);
    Ok(records)
}

/// The keys (or, with `history`, the row ids of every version) that UPDATE /
/// FORGET with `predicate` touch.
pub async fn targets(store: &PartitionStore, memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>, now: CommitTs) -> AdbResult<Vec<String>> {
    let modifiers = Modifiers { in_scope: in_scope.map(String::from), ..Default::default() };
    let records = candidates(store, memory, predicate, &modifiers, now).await?;
    Ok(records.into_iter().filter(|r| matches(memory, r, predicate)).map(|r| r.id).collect())
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

/// The current records `keys` of `memory`, by key: one scan.
pub async fn by_keys(store: &PartitionStore, memory: MemoryType, keys: &[String], now: CommitTs) -> AdbResult<HashMap<String, MemoryRecord>> {
    let mut out = HashMap::new();
    let m: Memory = memory.into();
    for chunk in keys.chunks(500) {
        let ids: Vec<Expr> = chunk.iter().map(|k| lit(row_id(m, k))).collect();
        let filter = records_of(m, None, None).and(col("lineage_id").in_list(ids, false));
        for r in current_rows(store, filter, now, false).await? {
            out.insert(r.key().to_string(), record(&r));
        }
    }
    Ok(out)
}

//! Where the executor's reads come from (DELIVERY_PLAN.md stage D3).
//!
//! - [`DirectReads`] — the in-memory backends (`adb-backends`).
//! - [`LanceReads`] — SEMANTIC and EPISODIC records, and every link, from the
//!   partition's Lance table (its current view, D2_TEMPORAL.md §3); other memory
//!   types from the backends (WORKING and TOOLS are the in-memory hot tier).
//!   PROCEDURAL reads from Lance too (stage D4.2). Links are the table's edge rows (stage D4.1), so a
//!   forgotten record's links are gone at once, as D2 §5 says, not only after a
//!   restart.
//!
//! `LanceReads` rebuilds the records from the table and answers with the backend's
//! own query code (a throwaway backend filled with `insert_record`), so predicates,
//! ordering and modifiers behave exactly as before. The predicate is also pushed into
//! the Lance scan where it can be (`adb_store::predicate_filter`, stage D3.6), so a
//! read loads only a superset of the matching rows instead of the whole memory type.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adb_backends::{Adb, Backend, EpisodicBackend, ProceduralBackend, SemanticBackend};
use adb_core::log::{CommitTs, Memory};
use adb_core::{time, AdbError, AdbResult, Link, MemoryRecord, MemoryType, Metadata, Modifiers, Predicate};
use adb_store::{predicate_filter, PartitionStore, StoredRow};
use datafusion::prelude::{col, lit};
use async_trait::async_trait;

use crate::apply::ApplyBarrier;

/// The executor's read operations.
#[async_trait]
pub trait Reads: Send + Sync {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>>;

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>>;

    async fn lookup(&self, memory: MemoryType, predicate: &Predicate) -> AdbResult<Vec<MemoryRecord>> {
        self.lookup_with_modifiers(memory, predicate, &Modifiers::default()).await
    }

    /// Links leaving record `from_id` (`WITH LINKS`, `FOLLOW LINKS`), oldest first.
    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>>;

    /// Links leaving each of `from_ids`, by record id.
    async fn links_from_many(&self, from_type: MemoryType, from_ids: &[String], link_type: Option<&str>) -> AdbResult<HashMap<String, Vec<Link>>> {
        let mut out = HashMap::new();
        for id in from_ids {
            out.insert(id.clone(), self.links_from(from_type, id, link_type).await?);
        }
        Ok(out)
    }

    /// The current records `ids` of `memory`, by id (link targets).
    async fn records_by_id(&self, memory: MemoryType, ids: &[String]) -> AdbResult<HashMap<String, MemoryRecord>> {
        let mut out = HashMap::new();
        for id in ids {
            let key = Predicate::Key { field: "id".into(), value: adb_core::Value::String(id.clone()) };
            if let Some(r) = self.lookup(memory, &key).await?.into_iter().next() {
                out.insert(id.clone(), r);
            }
        }
        Ok(out)
    }

    /// `SCAN FROM WORKING [WINDOW …] [LIMIT n] [RETURN …]`.
    async fn scan(&self, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>>;

    /// `LOAD FROM TOOLS`: matching tools by ranking, at most `limit`.
    async fn load(&self, predicate: &Predicate, limit: usize) -> AdbResult<Vec<MemoryRecord>>;
}

/// Where reads are answered (W7): `plan` (the query engine, default), `view` (the
/// old throwaway backends) or `shadow` (both; differences are logged), from
/// `ADB_QUERY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    Plan,
    View,
    Shadow,
}

pub fn query_mode() -> QueryMode {
    static MODE: std::sync::OnceLock<QueryMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("ADB_QUERY").as_deref() {
        Ok("view") => QueryMode::View,
        Ok("shadow") => QueryMode::Shadow,
        _ => QueryMode::Plan,
    })
}

/// Differences between the old and new read paths found in shadow mode.
pub static SHADOW_DIFFERENCES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Compare a shadow read (records as sets, without `accessed_at`, which the old
/// WORKING path set to the read time); log a difference.
fn shadow_compare(what: &str, plan: &[MemoryRecord], view: &[MemoryRecord]) {
    let norm = |rs: &[MemoryRecord]| {
        let mut v: Vec<serde_json::Value> = rs
            .iter()
            .map(|r| {
                let mut j = serde_json::to_value(r).unwrap_or_default();
                if let Some(m) = j.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                    m.remove("accessed_at");
                }
                j
            })
            .collect();
        v.sort_by_key(|j| j.to_string());
        v
    };
    let (p, v) = (norm(plan), norm(view));
    if p != v {
        SHADOW_DIFFERENCES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::warn!(read = what, plan = %serde_json::Value::Array(p), view = %serde_json::Value::Array(v), "query shadow difference");
    }
}

/// Reads from the in-memory backends.
pub struct DirectReads(pub Arc<Adb>);

#[async_trait]
impl Reads for DirectReads {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        check_scope(memory, modifiers.in_scope.as_deref(), false)?;
        self.0.lookup_with_modifiers(memory, predicate, modifiers).await
    }

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        check_scope(memory, modifiers.in_scope.as_deref(), false)?;
        if matches!(predicate, Predicate::Like { .. }) {
            // The in-memory backends hold no vectors: refuse rather than match all.
            return Err(AdbError::UnsupportedOperation("LIKE (relevance recall needs a durable engine: start ADB with --data-dir)".into()));
        }
        self.0.recall_with_modifiers(memory, predicate, modifiers).await
    }

    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>> {
        self.0.get_links_from(from_type, from_id, link_type).await
    }

    async fn scan(&self, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let mut records = match &modifiers.window {
            Some(w) => self.0.scan_window(w).await?,
            None => self.0.scan().await?,
        };
        if let Some(n) = modifiers.limit {
            records.truncate(n);
        }
        Ok(records)
    }

    async fn load(&self, predicate: &Predicate, limit: usize) -> AdbResult<Vec<MemoryRecord>> {
        self.0.load(predicate, limit).await
    }
}

/// A stored row as the backends hold it.
pub fn to_record(r: &StoredRow) -> MemoryRecord {
    let mut m = Metadata::new(r.data.visibility.into());
    m.created_at = r.commit_ts.to_datetime();
    m.accessed_at = r.valid_from.to_datetime();
    m.namespace = r.data.namespace.clone();
    m.ttl = r.data.ttl_at.map(|t| Duration::from_micros((t.0 - r.commit_ts.0).max(0) as u64));
    m.version = u64::from(r.version);
    MemoryRecord { id: r.key().to_string(), memory_type: r.data.memory.into(), data: r.data.value.clone(), metadata: m }
}

/// An edge row as a link: ends back to their record ids, `created_at` its commit.
pub fn to_link(r: &StoredRow) -> Option<Link> {
    let d = &r.data;
    let to: Memory = d.value.get("to_memory").cloned().and_then(|v| serde_json::from_value(v).ok())?;
    Some(Link {
        id: r.key().to_string(),
        from_type: d.memory.into(),
        from_id: adb_store::key_of(d.subject_id.as_deref()?).to_string(),
        to_type: to.into(),
        to_id: adb_store::key_of(d.object_id.as_deref()?).to_string(),
        link_type: d.link_type.clone().unwrap_or_default(),
        weight: d.weight.unwrap_or(1.0),
        created_at: r.commit_ts.to_datetime(),
        updated_at: r.valid_from.to_datetime(),
    })
}

/// SEMANTIC and EPISODIC records and every link from Lance, the rest from the
/// backends.
pub struct LanceReads {
    adb: Arc<Adb>,
    store: Arc<PartitionStore>,
    /// Waits for the background applier, so reads see every write that returned
    /// before them (stage D3.7). `None` = the table is written synchronously.
    barrier: Option<Arc<ApplyBarrier>>,
    /// Embeds the text of `LIKE $q` (MVP W4); `None`: only vectors can be passed.
    embedder: Option<Arc<adb_embed::Embedder>>,
}

/// A throwaway backend over some of the table's rows, answering with the backend's
/// own query code (reads, and the targets of UPDATE / FORGET, stage D4.4a).
pub(crate) enum View {
    Semantic(SemanticBackend),
    Episodic(EpisodicBackend),
    Procedural(ProceduralBackend),
}

impl View {
    /// `rows` (current, or history with `by_row_id`) as `memory`'s backend holds
    /// them. With `by_row_id` each row keeps its own row id, so several versions of
    /// one record can sit in one view (FORGET over history, D2 T5).
    pub(crate) async fn of(memory: MemoryType, mut rows: Vec<StoredRow>, by_row_id: bool) -> AdbResult<View> {
        let record = |r: &StoredRow| {
            let mut rec = to_record(r);
            if by_row_id {
                rec.id = r.id.clone();
            }
            rec
        };
        Ok(match memory {
            MemoryType::Semantic => {
                let b = SemanticBackend::new()?;
                rows.iter().for_each(|r| b.insert_record(record(r)));
                View::Semantic(b)
            }
            MemoryType::Procedural => {
                // The backend shapes payloads into its own nodes: re-run each `store`
                // at its commit time (as `hydrate` does), oldest first, then restore
                // the version. Its ids come from the payload's `pattern_id`, so
                // history needs one view per row (`by_row_id` is not supported).
                let b = ProceduralBackend::new();
                rows.sort_by(|a, b| (a.commit_ts, &a.id).cmp(&(b.commit_ts, &b.id)));
                for r in &rows {
                    let rec = to_record(r);
                    let stored = b.store(r.key(), r.data.value.clone(), rec.metadata.scope, r.data.namespace.as_deref(), rec.metadata.ttl);
                    time::at(r.commit_ts.to_datetime(), stored).await?;
                    b.set_version(r.key(), u64::from(r.version));
                }
                View::Procedural(b)
            }
            _ => {
                let b = EpisodicBackend::new();
                rows.iter().for_each(|r| b.insert_record(record(r)));
                View::Episodic(b)
            }
        })
    }

    fn backend(&self) -> &dyn Backend {
        match self {
            View::Semantic(b) => b,
            View::Episodic(b) => b,
            View::Procedural(b) => b,
        }
    }

    pub(crate) async fn lookup(&self, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        self.backend().lookup(predicate, modifiers).await
    }

    pub(crate) async fn recall(&self, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        self.backend().recall(predicate, modifiers).await
    }

    pub(crate) async fn update_targets(&self, predicate: &Predicate) -> AdbResult<Vec<String>> {
        self.backend().update_targets(predicate).await
    }

    pub(crate) async fn forget_targets(&self, predicate: &Predicate) -> AdbResult<Vec<String>> {
        self.backend().forget_targets(predicate).await
    }
}

/// The memory types whose records live only in Lance once there is a table
/// (stage D4.4a); WORKING and TOOLS are the in-memory hot tier.
/// `IN SCOPE` (named scopes, AQL v0.6) is served where rows carry their scope: the
/// Lance-owned memory types on a durable engine (`durable`). Elsewhere it is refused
/// rather than ignored — a FORGET that ignored its scope would forget everywhere.
pub fn check_scope(memory: MemoryType, in_scope: Option<&str>, durable: bool) -> AdbResult<()> {
    let Some(s) = in_scope else { return Ok(()) };
    let ok_chars = s.chars().all(|c| c.is_ascii_alphanumeric() || ":_-./@".contains(c));
    if s.is_empty() || s.len() > 128 || !ok_chars {
        return Err(AdbError::QueryError(format!(
            "IN SCOPE {s:?}: a scope is 1-128 characters of letters, digits and : _ - . / @ (e.g. \"team:billing\")"
        )));
    }
    if !durable {
        return Err(AdbError::UnsupportedOperation("IN SCOPE (named scopes need a durable engine: start ADB with --data-dir)".into()));
    }
    if !lance_owned(memory) {
        return Err(AdbError::UnsupportedOperation(format!("IN SCOPE on {memory:?} (supported on SEMANTIC, EPISODIC and PROCEDURAL)")));
    }
    Ok(())
}

/// The text ADB embeds for a record (D3 S3 / W4 T3): `content`; for PROCEDURAL
/// without it, `pattern` and `steps`.
pub fn embed_text(memory: MemoryType, value: &serde_json::Value) -> Option<String> {
    let s = |k: &str| value.get(k).and_then(|v| v.as_str()).filter(|t| !t.trim().is_empty());
    if let Some(c) = s("content") {
        return Some(c.to_string());
    }
    if memory == MemoryType::Procedural {
        let parts: Vec<&str> = [s("pattern"), s("steps")].into_iter().flatten().collect();
        if !parts.is_empty() {
            return Some(parts.join(". "));
        }
    }
    None
}

pub fn lance_owned(memory: MemoryType) -> bool {
    matches!(memory, MemoryType::Semantic | MemoryType::Episodic | MemoryType::Procedural)
}

/// A view of `memory`'s current rows at `now` that may match `predicate`.
pub(crate) async fn current_view(
    store: &PartitionStore,
    memory: MemoryType,
    predicate: &Predicate,
    now: CommitTs,
    in_scope: Option<&str>,
) -> AdbResult<View> {
    let m: Memory = memory.into();
    // Record rows only: not edges, topic events or subscriptions (`RECORD_KINDS`).
    let kinds = ["entity", "fact", "episode", "tool"].map(lit).to_vec();
    let mut filter = col("memory").eq(lit(m.tag())).and(col("kind").in_list(kinds, false));
    if let Some(s) = in_scope {
        filter = filter.and(col("scope").eq(lit(s)));
    }
    // Not for PROCEDURAL: its conditions see the backend's `ProcedureNode`, whose
    // defaults (`severity`, `confidence`, `version`, counts) the payload may lack,
    // so a payload filter could drop a match. Procedure sets are small.
    if memory != MemoryType::Procedural {
        if let Some(p) = predicate_filter(m, predicate) {
            filter = filter.and(p);
        }
    }
    let rows = store.current_expr(Some(filter), now).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
    View::of(memory, rows, false).await
}

impl LanceReads {
    pub fn new(adb: Arc<Adb>, store: Arc<PartitionStore>, barrier: Option<Arc<ApplyBarrier>>) -> Self {
        Self { adb, store, barrier, embedder: None }
    }

    pub fn with_embedder(mut self, embedder: Option<Arc<adb_embed::Embedder>>) -> Self {
        self.embedder = embedder;
        self
    }

    /// `RECALL FROM <memory> LIKE $q …` (MVP W4): the current rows passing the
    /// statement's filters (`IN SCOPE`, `NAMESPACE`, TTL), ranked by
    /// [`crate::rank`] against `$q` — text (embedded here, and matched as words) or
    /// a vector — cut by `MIN_CONFIDENCE` (cosine) and `LIMIT` (default 10). An
    /// explicit `ORDER BY` reorders the selected records.
    async fn similar(&self, memory: MemoryType, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let query = modifiers.like_query.as_ref().ok_or_else(|| AdbError::QueryError("LIKE $q: the variable has no value (pass it in `variables`)".into()))?;
        let (query_text, query_vec) = match query {
            serde_json::Value::String(t) => {
                let embedded = match &self.embedder {
                    Some(e) => Some(e.embed(vec![t.clone()]).await.map_err(AdbError::EmbeddingError)?.remove(0)),
                    None => None,
                };
                (Some(t.clone()), embedded)
            }
            serde_json::Value::Array(xs) => {
                let v: Option<Vec<f32>> = xs.iter().map(|x| x.as_f64().map(|f| f as f32)).collect();
                (None, Some(v.ok_or_else(|| AdbError::QueryError("LIKE $q: a vector is a list of numbers".into()))?))
            }
            other => return Err(AdbError::QueryError(format!("LIKE $q: text or a vector, not {other}"))),
        };
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        let now = CommitTs::from_datetime(time::now());
        let m: Memory = memory.into();
        let kinds = ["entity", "fact", "episode", "tool"].map(lit).to_vec();
        let mut filter = col("memory").eq(lit(m.tag())).and(col("kind").in_list(kinds, false));
        if let Some(s) = &modifiers.in_scope {
            filter = filter.and(col("scope").eq(lit(s.clone())));
        }
        let mut rows = crate::query::current_rows(&self.store, filter, now, true).await?;
        if let Some(ns) = &modifiers.namespace {
            rows.retain(|r| r.data.namespace.as_ref() == Some(ns));
        }
        let vectors: Vec<Option<Vec<f32>>> =
            rows.iter().map(|r| r.data.embedding.as_ref().map(|e| e.iter().map(|x| x.to_f32()).collect())).collect();
        let texts: Vec<Option<String>> = rows.iter().map(|r| embed_text(r.data.memory.into(), &r.data.value)).collect();
        let candidates: Vec<crate::rank::Candidate> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| crate::rank::Candidate { embedding: vectors[i].as_deref(), text: texts[i].as_deref(), at: r.commit_ts })
            .collect();
        let ranked = crate::rank::rank(&candidates, query_vec.as_deref(), query_text.as_deref(), modifiers.min_confidence, now);
        let limit = modifiers.limit.unwrap_or(10);
        let mut out: Vec<MemoryRecord> = ranked
            .into_iter()
            .take(limit)
            .map(|r| {
                let mut rec = crate::query::record(&rows[r.index]);
                rec.metadata.confidence = r.similarity;
                rec
            })
            .collect();
        if let Some(order) = &modifiers.order_by {
            out.sort_by(|a, b| {
                let (x, y) = (a.order_value(&order.field), b.order_value(&order.field));
                let c = match (x.as_ref(), y.as_ref()) {
                    (Some(x), Some(y)) => x.to_string().cmp(&y.to_string()),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
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
        Ok(out)
    }

    /// The memory types served from Lance.
    pub fn serves(memory: MemoryType) -> bool {
        lance_owned(memory)
    }

    async fn committed(&self) -> CommitTs {
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        CommitTs::from_datetime(time::now())
    }

    /// A non-`LIKE` RECALL / LOOKUP in the current mode (`lookup`: the backends'
    /// `lookup`, else `recall`).
    async fn read(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, lookup: bool) -> AdbResult<Vec<MemoryRecord>> {
        let old = || async {
            match (Self::serves(memory), lookup) {
                (false, true) => self.adb.lookup_with_modifiers(memory, predicate, modifiers).await,
                (false, false) => self.adb.recall_with_modifiers(memory, predicate, modifiers).await,
                (true, true) => self.view(memory, predicate, modifiers.in_scope.as_deref()).await?.lookup(predicate, modifiers).await,
                (true, false) => self.view(memory, predicate, modifiers.in_scope.as_deref()).await?.recall(predicate, modifiers).await,
            }
        };
        match query_mode() {
            QueryMode::View => old().await,
            QueryMode::Plan => crate::query::read(&self.store, memory, predicate, modifiers, self.committed().await).await,
            QueryMode::Shadow => {
                let plan = crate::query::read(&self.store, memory, predicate, modifiers, self.committed().await).await?;
                let view = old().await?;
                shadow_compare(&format!("{memory:?} {predicate:?} {modifiers:?}"), &plan, &view);
                Ok(plan)
            }
        }
    }

    /// A throwaway backend holding the memory type's current rows that may match
    /// `predicate`.
    async fn view(&self, memory: MemoryType, predicate: &Predicate, in_scope: Option<&str>) -> AdbResult<View> {
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        current_view(&self.store, memory, predicate, CommitTs::from_datetime(time::now()), in_scope).await
    }
}

#[async_trait]
impl Reads for LanceReads {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        check_scope(memory, modifiers.in_scope.as_deref(), true)?;
        self.read(memory, predicate, modifiers, true).await
    }

    async fn links_from_many(&self, from_type: MemoryType, from_ids: &[String], link_type: Option<&str>) -> AdbResult<HashMap<String, Vec<Link>>> {
        let now = self.committed().await;
        crate::query::links_from(&self.store, from_type, from_ids, link_type, now).await
    }

    async fn records_by_id(&self, memory: MemoryType, ids: &[String]) -> AdbResult<HashMap<String, MemoryRecord>> {
        if query_mode() == QueryMode::View {
            let mut out = HashMap::new();
            for id in ids {
                let key = Predicate::Key { field: "id".into(), value: adb_core::Value::String(id.clone()) };
                if let Some(r) = self.lookup(memory, &key).await?.into_iter().next() {
                    out.insert(id.clone(), r);
                }
            }
            return Ok(out);
        }
        let now = self.committed().await;
        crate::query::by_keys(&self.store, memory, ids, now).await
    }

    async fn scan(&self, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let old = || DirectReads(Arc::clone(&self.adb));
        match query_mode() {
            QueryMode::View => old().scan(modifiers).await,
            QueryMode::Plan => crate::query::scan(&self.store, modifiers, self.committed().await).await,
            QueryMode::Shadow => {
                let plan = crate::query::scan(&self.store, modifiers, self.committed().await).await?;
                let mut view = old().scan(modifiers).await?;
                if let Some(f) = &modifiers.return_fields {
                    crate::query::project(&mut view, f);
                }
                shadow_compare(&format!("SCAN {modifiers:?}"), &plan, &view);
                Ok(plan)
            }
        }
    }

    async fn load(&self, predicate: &Predicate, limit: usize) -> AdbResult<Vec<MemoryRecord>> {
        match query_mode() {
            QueryMode::View => self.adb.load(predicate, limit).await,
            QueryMode::Plan => crate::query::load(&self.store, predicate, limit, self.committed().await).await,
            QueryMode::Shadow => {
                let plan = crate::query::load(&self.store, predicate, limit, self.committed().await).await?;
                let view = self.adb.load(predicate, limit).await?;
                shadow_compare(&format!("LOAD {predicate:?} {limit}"), &plan, &view);
                Ok(plan)
            }
        }
    }

    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>> {
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        let now = CommitTs::from_datetime(time::now());
        let rows = self
            .store
            .edges_from(from_type.into(), from_id, link_type, now)
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        Ok(rows.iter().filter_map(to_link).collect())
    }

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        check_scope(memory, modifiers.in_scope.as_deref(), true)?;
        if matches!(predicate, Predicate::Like { .. }) {
            if !Self::serves(memory) {
                return Err(AdbError::UnsupportedOperation(format!("LIKE on {memory:?} (supported on SEMANTIC, EPISODIC and PROCEDURAL)")));
            }
            return self.similar(memory, modifiers).await;
        }
        self.read(memory, predicate, modifiers, false).await
    }
}

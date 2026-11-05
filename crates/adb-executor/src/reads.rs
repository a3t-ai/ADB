//! Where the executor's reads come from: [`LanceReads`], every memory type and every
//! link from the partition's Lance table (its current view, D2_TEMPORAL.md §3),
//! answered by the query engine (`crate::query`, MVP W7). Links are the table's
//! edge rows (stage D4.1), so a forgotten record's links are gone at once.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adb_core::log::{CommitTs, Memory};
use adb_core::{time, AdbError, AdbResult, Link, LinkSelect, MemoryRecord, MemoryType, Metadata, Modifiers, Predicate};
use adb_store::{PartitionStore, StoredRow};
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

    /// The links touching each of `ids` that `select` picks (AQL v0.8: types,
    /// direction, minimum weight), by record id.
    async fn links_touching(&self, memory: MemoryType, ids: &[String], select: &LinkSelect) -> AdbResult<HashMap<String, Vec<Link>>> {
        if select.direction.inbound() {
            return Err(AdbError::UnsupportedOperation("inbound links need LanceReads".into()));
        }
        let mut out = HashMap::new();
        for (id, links) in self.links_from_many(memory, ids, None).await? {
            out.insert(id, links.into_iter().filter(|l| select.matches(l)).collect());
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

/// A stored row as a record (before PROCEDURAL / TOOLS shaping, `query::record`).
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
    // A fact with an entity object is a link of its predicate (P1.4).
    let to: Memory = match d.kind {
        adb_core::log::Kind::Fact => d.memory,
        _ => d.value.get("to_memory").cloned().and_then(|v| serde_json::from_value(v).ok())?,
    };
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
    store: Arc<PartitionStore>,
    /// Waits for the background applier, so reads see every write that returned
    /// before them (stage D3.7). `None` = the table is written synchronously.
    barrier: Option<Arc<ApplyBarrier>>,
    /// Embeds the text of `LIKE $q` (MVP W4); `None`: only vectors can be passed.
    embedder: Option<Arc<adb_embed::Embedder>>,
    /// On a branch engine (B2): reads overlay the branch's rows on its parent's.
    parent: Option<crate::branch_engine::ParentView>,
    /// B7: the org's tags and layers (a read in a namespace with layers sees them).
    registry: Option<Arc<crate::branches::Branches>>,
}

/// `IN SCOPE` (named scopes, AQL v0.6) is served on the long-term memory types.
/// Elsewhere it is refused rather than ignored — a FORGET that ignored its scope
/// would forget everywhere.
pub fn check_scope(memory: MemoryType, in_scope: Option<&str>) -> AdbResult<()> {
    let Some(s) = in_scope else { return Ok(()) };
    let ok_chars = s.chars().all(|c| c.is_ascii_alphanumeric() || ":_-./@".contains(c));
    if s.is_empty() || s.len() > 128 || !ok_chars {
        return Err(AdbError::QueryError(format!(
            "IN SCOPE {s:?}: a scope is 1-128 characters of letters, digits and : _ - . / @ (e.g. \"team:billing\")"
        )));
    }
    if !lance_owned(memory) {
        return Err(AdbError::UnsupportedOperation(format!("IN SCOPE on {memory:?} (supported on SEMANTIC, EPISODIC and PROCEDURAL)")));
    }
    Ok(())
}

/// `predicate` with the record ids it names (`KEY id` / `KEY key`, `WHERE id`) as
/// ids in named scope `scope` (E-L12): a key there is `<scope>~<key>`. Payload
/// fields (`key`, `subject`, …) stay as stored: bare keys.
pub fn scope_ids(predicate: &Predicate, namespace: Option<&str>, scope: &str) -> Predicate {
    fn value(v: &adb_core::Value, ns: Option<&str>, scope: &str) -> adb_core::Value {
        match v {
            adb_core::Value::String(k) => adb_core::Value::String(adb_store::space_id(ns, scope, k)),
            adb_core::Value::Array(items) => adb_core::Value::Array(items.iter().map(|i| value(i, ns, scope)).collect()),
            other => other.clone(),
        }
    }
    fn conditions(cs: &[adb_core::Condition], ns: Option<&str>, scope: &str) -> Vec<adb_core::Condition> {
        cs.iter()
            .map(|c| match c {
                adb_core::Condition::Simple { field, operator, value: v, logical_op } if field == "id" => {
                    adb_core::Condition::Simple { field: field.clone(), operator: *operator, value: value(v, ns, scope), logical_op: *logical_op }
                }
                adb_core::Condition::Group { conditions: g, logical_op } => {
                    adb_core::Condition::Group { conditions: conditions(g, ns, scope), logical_op: *logical_op }
                }
                other => other.clone(),
            })
            .collect()
    }
    match predicate {
        Predicate::Key { field, value: v } if matches!(field.as_str(), "id" | "key") => Predicate::Key { field: field.clone(), value: value(v, namespace, scope) },
        Predicate::Where { conditions: cs } => Predicate::Where { conditions: conditions(cs, namespace, scope) },
        other => other.clone(),
    }
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

/// The long-term memory types: named scopes (`IN SCOPE`), embeddings and `LIKE`.
/// WORKING and TOOLS are scratchpad and registry (all in the table too, W7).
pub fn lance_owned(memory: MemoryType) -> bool {
    matches!(memory, MemoryType::Semantic | MemoryType::Episodic | MemoryType::Procedural)
}

impl LanceReads {
    pub fn new(store: Arc<PartitionStore>, barrier: Option<Arc<ApplyBarrier>>) -> Self {
        Self { store, barrier, embedder: None, parent: None, registry: None }
    }

    pub fn with_parent(mut self, parent: Option<crate::branch_engine::ParentView>) -> Self {
        self.parent = parent;
        self
    }

    /// B7: reads in a namespace with layers see them.
    pub fn with_layers(mut self, registry: Option<Arc<crate::branches::Branches>>) -> Self {
        self.registry = registry;
        self
    }

    /// B7 (spec §3.9): the layers of the statement's namespace carrying `memory`
    /// (`None`: no NAMESPACE, or none apply).
    fn layers_for(&self, memory: MemoryType, modifiers: &Modifiers) -> Option<(String, Vec<crate::branches::Layer>)> {
        let (reg, ns) = (self.registry.as_ref()?, modifiers.namespace.as_deref()?);
        let layers: Vec<crate::branches::Layer> = reg.layers_of(ns).into_iter().filter(|l| l.memories.contains(&memory)).collect();
        (!layers.is_empty()).then(|| (ns.to_string(), layers))
    }

    /// The rows a space with layers reads: the sealed layers' records first, then the
    /// space's own, then unsealed layers' — one per key, the first source wins. A
    /// mount reads its space now; a pin at its tag. A layer's record is given this
    /// namespace's id (its `namespace` column says where it is from).
    async fn layer_rows(&self, memory: MemoryType, ns: &str, layers: &[crate::branches::Layer], in_scope: Option<&str>, now: CommitTs) -> AdbResult<Vec<StoredRow>> {
        let reg = self.registry.as_ref().expect("layers come from the registry");
        let m: adb_core::log::Memory = memory.into();
        let rows_at = |space: String, at: CommitTs| {
            let store = Arc::clone(&self.store);
            async move {
                let rows = store.versions(m, Some(&format!("namespace = {}", adb_store::sql_str(&space)))).await.map_err(|e| AdbError::Internal(format!("store: {e}")))?;
                AdbResult::Ok(rows.into_iter().filter(|r| r.is_current_at(at, now) && in_scope.is_none_or(|s| r.data.scope == s)).collect::<Vec<_>>())
            }
        };
        let point = |l: &crate::branches::Layer| match l.mode {
            crate::branches::LayerMode::Mount => Some(now),
            crate::branches::LayerMode::Pin | crate::branches::LayerMode::History => l.tag.as_ref().and_then(|t| reg.tag(&l.space, t)).map(|t| CommitTs(t.point_ts)),
        };
        let mut sources = Vec::new();
        for l in layers.iter().filter(|l| l.sealed) {
            if let Some(at) = point(l) {
                sources.push((rows_at(l.space.clone(), at).await?, false));
            }
        }
        sources.push((rows_at(ns.to_string(), now).await?, true));
        for l in layers.iter().filter(|l| !l.sealed) {
            if let Some(at) = point(l) {
                sources.push((rows_at(l.space.clone(), at).await?, false));
            }
        }
        let mut seen = std::collections::HashSet::new();
        let mut rows = Vec::new();
        for (source, own) in sources {
            for mut r in source {
                let bare = adb_store::without_namespace(adb_store::key_of(&r.lineage_id)).to_string();
                if !seen.insert(bare.clone()) {
                    continue;
                }
                if !own {
                    let here = adb_store::row_id(m, &adb_store::namespaced(Some(ns), &bare));
                    if r.id == r.lineage_id {
                        r.id = here.clone();
                    }
                    r.lineage_id = here;
                }
                rows.push(r);
            }
        }
        Ok(rows)
    }

    /// B7: a read in a namespace with layers. `None`: no layer applies, or the
    /// statement reads what layers don't serve yet (history, links, PATTERN): the
    /// space's own records.
    async fn layered(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, now: CommitTs) -> AdbResult<Option<Vec<MemoryRecord>>> {
        let Some((ns, layers)) = self.layers_for(memory, modifiers) else { return Ok(None) };
        if crate::query::is_history(modifiers)
            || modifiers.follow_links.is_some()
            || modifiers.with_links.is_some()
            || matches!(predicate, Predicate::Pattern { .. } | Predicate::Like { .. })
        {
            return Ok(None);
        }
        let rows = self.layer_rows(memory, &ns, &layers, modifiers.in_scope.as_deref(), now).await?;
        Ok(Some(crate::query::branch_read(rows, memory, predicate, modifiers)?))
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
        // B7: a space with layers ranks its merged rows.
        if let Some((ns, layers)) = self.layers_for(memory, modifiers).filter(|_| self.parent.is_none()) {
            let rows = self.layer_rows(memory, &ns, &layers, modifiers.in_scope.as_deref(), now).await?;
            return Ok(crate::query::rank_rows(rows.into_iter().map(Arc::new).collect(), modifiers, query_text.as_deref(), query_vec.as_deref(), now));
        }
        crate::query::similar(&self.store, memory, modifiers, query_text.as_deref(), query_vec.as_deref(), now).await
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

    /// A non-`LIKE` RECALL / LOOKUP.
    async fn read(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let now = self.committed().await;
        if let Some(p) = &self.parent {
            let rows = crate::branch_engine::view(&self.store, &self.barrier, p, memory, now).await?;
            return crate::query::branch_read(rows.into_iter().map(|(r, _)| r).collect(), memory, predicate, modifiers);
        }
        if let Some(records) = self.layered(memory, predicate, modifiers, now).await? {
            return Ok(records);
        }
        if crate::query::is_history(modifiers) {
            return crate::query::history(&self.store, memory, predicate, modifiers, now).await;
        }
        crate::query::read(&self.store, memory, predicate, modifiers, now).await
    }
}

#[async_trait]
impl Reads for LanceReads {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        check_scope(memory, modifiers.in_scope.as_deref())?;
        self.read(memory, predicate, modifiers).await
    }

    async fn links_from_many(&self, from_type: MemoryType, from_ids: &[String], link_type: Option<&str>) -> AdbResult<HashMap<String, Vec<Link>>> {
        let now = self.committed().await;
        crate::query::links_from(&self.store, from_type, from_ids, link_type, now).await
    }

    async fn links_touching(&self, memory: MemoryType, ids: &[String], select: &LinkSelect) -> AdbResult<HashMap<String, Vec<Link>>> {
        let now = self.committed().await;
        crate::query::links_touching(&self.store, memory, ids, select, now).await
    }

    async fn records_by_id(&self, memory: MemoryType, ids: &[String]) -> AdbResult<HashMap<String, MemoryRecord>> {
        let now = self.committed().await;
        crate::query::by_keys(&self.store, memory, ids, now).await
    }

    async fn scan(&self, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        crate::query::scan(&self.store, modifiers, self.committed().await).await
    }

    async fn load(&self, predicate: &Predicate, limit: usize) -> AdbResult<Vec<MemoryRecord>> {
        crate::query::load(&self.store, predicate, limit, self.committed().await).await
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
        check_scope(memory, modifiers.in_scope.as_deref())?;
        if matches!(predicate, Predicate::Like { .. }) {
            if self.parent.is_some() {
                return Err(AdbError::UnsupportedOperation("LIKE is not read on a branch yet".into()));
            }
            if crate::query::is_history(modifiers) {
                return Err(AdbError::UnsupportedOperation("LIKE doesn't read history (AS OF, VALID AT, WITH HISTORY)".into()));
            }
            if !Self::serves(memory) {
                return Err(AdbError::UnsupportedOperation(format!("LIKE on {memory:?} (supported on SEMANTIC, EPISODIC and PROCEDURAL)")));
            }
            return self.similar(memory, modifiers).await;
        }
        self.read(memory, predicate, modifiers).await
    }
}

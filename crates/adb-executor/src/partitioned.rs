//! A cluster node's partitions behind one engine (S3 option B, step 3).
//!
//! Every memory partition is a [`LocalEngine`] of its own ([`PartitionRole::memory`],
//! `dir/parts/p{n}`), opened on first use; the system partition
//! ([`PartitionRole::system`]) holds the platform tables, FlowR's engine store and the
//! **namespace catalog** (platform table `adb_namespaces`: every namespace that has
//! been written, and its partition).
//!
//! A statement runs within the scope its call carries ([`Reach`], built by A3T/FlowR;
//! ADB only enforces it):
//!
//! - **One namespace** (every stage names the same one): run as is on that
//!   namespace's partition — the common case (an agent's own memory).
//! - **Otherwise** (no `NAMESPACE`, or several): the router's own executor runs it over
//!   [`RoutedReads`] / [`RoutedWrites`]. Reads go to the namespaces of the catalog the
//!   scope allows — each with the exact-namespace filter every read path already
//!   enforces — or, for a full-access call, to each partition holding data; the
//!   results are unioned, then ordered and cut as one engine would (`ORDER BY`, the
//!   default order, `LIMIT`; similarity lists merged by reciprocal rank). Aggregates
//!   are computed by the executor over the union, so they are exact. Writes run in
//!   each target and add up; across partitions they are not atomic.
//! - **Links** are kept with their source record. A link whose two ends are in
//!   different partitions is written on the source's partition after the router has
//!   checked the target exists (the target may still be forgotten later: following
//!   it then finds nothing, as for any forgotten record). Links whose far end is
//!   outside the scope are left out of the answer.
//! - Topics, subscriptions and platform writes go to the system partition; `WATCH`
//!   needs one namespace (watches across partitions come later, SUBSCRIPTIONS_CQ_SPEC
//!   §6).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;


use adb_cluster::map::{Partition, PartitionMap};
use adb_cluster::routing::{self, Reach};
use adb_core::{AdbError, AdbResult, Link, LinkSelect, MemoryRecord, MemoryType, Modifiers, Predicate, Scope};
use async_trait::async_trait;
use serde_json::Value;

use crate::engine::{to_wire, Engine, LocalEngine, PartitionRole};
use crate::reads::Reads;
use crate::writes::Writes;
use crate::Executor;

/// The catalog's platform table.
pub const CATALOG: &str = "adb_namespaces";

/// One place a routed read or write runs: a partition, and the namespace it is
/// limited to there (`None`: every namespace in it — a full-access call only).
#[derive(Clone)]
struct Target {
    partition: Partition,
    namespace: Option<String>,
    writes: Arc<dyn Writes>,
    reads: Arc<dyn Reads>,
}

/// A cluster node's partitions as one [`Engine`].
pub struct PartitionedEngine {
    dir: PathBuf,
    map: std::sync::RwLock<PartitionMap>,
    parts: tokio::sync::Mutex<HashMap<Partition, Arc<LocalEngine>>>,
    system: Arc<LocalEngine>,
    /// Namespace → partition, as recorded in [`CATALOG`].
    catalog: std::sync::RwLock<BTreeMap<String, Partition>>,
}

impl PartitionedEngine {
    /// Open a node's partitions under `dir` with `map` (the system partition now, the
    /// memory partitions on first use) and load the catalog.
    pub async fn open(dir: impl Into<PathBuf>, map: PartitionMap) -> Result<Arc<Self>, String> {
        let dir = dir.into();
        if map.partitions == 0 {
            return Err("the partition map has no partitions yet".into());
        }
        let sys = routing::system_partition(&map);
        let (system, _) = LocalEngine::durable_partition(dir.join("parts").join(format!("p{sys}")), PartitionRole::system(sys)).await?;
        let mut catalog = BTreeMap::new();
        if let Some(p) = system.platform() {
            if p.list_tables().map_err(|e| e.to_string())?.iter().any(|t| t == CATALOG) {
                let rows = p.query(CATALOG, None, usize::MAX >> 1).await.map_err(|e| e.to_string())?;
                for r in adb_platform::batches_to_json(&rows) {
                    let ns = r.get("namespace").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                    let part = r.get("partition").and_then(|v| v.as_str()).and_then(|v| v.parse().ok());
                    if let Some(part) = part {
                        catalog.insert(ns, part);
                    }
                }
            }
        }
        Ok(Arc::new(Self { dir, map: std::sync::RwLock::new(map), parts: Default::default(), system: Arc::new(system), catalog: std::sync::RwLock::new(catalog) }))
    }

    /// The system partition's engine.
    pub fn system(&self) -> &Arc<LocalEngine> {
        &self.system
    }

    /// The partition `namespace` lives in (`""`: records without a namespace).
    pub fn partition_of(&self, namespace: &str) -> Partition {
        routing::partition_of(&self.map.read().unwrap(), namespace)
    }

    /// The engine of memory partition `p`, opened on first use.
    pub async fn part(&self, p: Partition) -> Result<Arc<LocalEngine>, String> {
        let mut parts = self.parts.lock().await;
        if let Some(e) = parts.get(&p) {
            return Ok(Arc::clone(e));
        }
        let (e, _) = LocalEngine::durable_partition(self.dir.join("parts").join(format!("p{p}")), PartitionRole::memory(p)).await?;
        let e = Arc::new(e);
        parts.insert(p, Arc::clone(&e));
        Ok(e)
    }

    /// Every namespace written so far, with its partition.
    pub fn catalog(&self) -> BTreeMap<String, Partition> {
        self.catalog.read().unwrap().clone()
    }

    /// Record `namespace` in the catalog (before its first write commits).
    async fn note(&self, namespace: &str) -> Result<(), String> {
        if self.catalog.read().unwrap().contains_key(namespace) {
            return Ok(());
        }
        let p = self.partition_of(namespace);
        let row = serde_json::json!({ "namespace": namespace, "partition": p.to_string() });
        self.system.platform_write(adb_core::log::Effect::TableInsert { table: CATALOG.into(), rows: vec![row.as_object().cloned().unwrap_or_default()] }).await?;
        self.catalog.write().unwrap().insert(namespace.to_string(), p);
        Ok(())
    }

    /// Where a statement without one namespace reads and writes: for a full-access
    /// call each partition holding data, else each catalog namespace in scope.
    async fn targets(&self, reach: &Reach) -> Result<Vec<Target>, String> {
        let catalog = self.catalog();
        let mut out = Vec::new();
        match reach {
            Reach::All => {
                let partitions: BTreeSet<Partition> = catalog.values().copied().collect();
                for p in partitions {
                    let e = self.part(p).await?;
                    let (writes, reads) = e.io();
                    out.push(Target { partition: p, namespace: None, writes, reads });
                }
            }
            Reach::Only(_) => {
                for (ns, p) in catalog.iter().filter(|(ns, _)| reach.allows(ns)) {
                    let e = self.part(*p).await?;
                    let (writes, reads) = e.io();
                    out.push(Target { partition: *p, namespace: Some(ns.clone()), writes, reads });
                }
            }
        }
        Ok(out)
    }

    /// The one target of `namespace` (checked against the scope).
    async fn target_of(&self, reach: &Reach, namespace: &str) -> AdbResult<Target> {
        if !reach.allows(namespace) {
            return Err(out_of_reach(namespace));
        }
        let p = self.partition_of(namespace);
        let e = self.part(p).await.map_err(AdbError::Internal)?;
        let (writes, reads) = e.io();
        Ok(Target { partition: p, namespace: Some(namespace.to_string()), writes, reads })
    }

    /// The engine of `namespace`'s partition.
    pub async fn engine_of(&self, namespace: &str) -> Result<Arc<LocalEngine>, String> {
        self.part(self.partition_of(namespace)).await
    }

    /// The engine both namespaces live in; an operation spanning two partitions isn't
    /// built yet and is refused rather than run on one of them.
    async fn engine_of_both(&self, what: &str, a: &str, b: &str) -> Result<Arc<LocalEngine>, String> {
        let (pa, pb) = (self.partition_of(a), self.partition_of(b));
        if pa != pb {
            return Err(format!("{what} from {a:?} (partition {pa}) into {b:?} (partition {pb}): across partitions is not built yet"));
        }
        self.part(pa).await
    }

    /// The memory partitions holding data (the catalog's), opened.
    async fn data_parts(&self) -> Result<Vec<Arc<LocalEngine>>, String> {
        let ps: BTreeSet<Partition> = self.catalog.read().unwrap().values().copied().collect();
        let mut out = Vec::new();
        for p in ps {
            out.push(self.part(p).await?);
        }
        Ok(out)
    }

    /// Run `aql` within `reach` (see the module docs).
    pub async fn query_in(self: &Arc<Self>, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let touches = touches(aql);
        for (ns, _) in &touches {
            if let Some(ns) = ns {
                if !reach.allows(ns) {
                    return Err(out_of_reach(ns).to_string());
                }
            }
        }
        let names: BTreeSet<Option<&str>> = touches.iter().map(|(ns, _)| ns.as_deref()).collect();
        let writes_any = touches.iter().any(|(_, w)| *w);
        // One namespace: as is, on its partition.
        if let (1, Some(Some(ns))) = (names.len(), names.iter().next()) {
            if writes_any {
                self.note(ns).await?;
            }
            let e = self.part(self.partition_of(ns)).await?;
            return e.query(aql, variables).await;
        }
        let first = aql.trim_start().split(|c: char| c.is_whitespace()).next().unwrap_or_default();
        if first.eq_ignore_ascii_case("WATCH") {
            return Err("WATCH needs NAMESPACE in a cluster: watches across partitions come later".into());
        }
        let io = Arc::new(Routed { node: Arc::clone(self), reach: reach.clone() });
        let executor = Executor::with_io(Arc::clone(&io) as Arc<dyn Writes>, io as Arc<dyn Reads>);
        let r = match variables {
            Some(v) if !v.is_empty() => executor.execute_with_vars(aql, v).await,
            _ => executor.execute(aql).await,
        };
        r.map(|r| to_wire(&r)).map_err(|e| e.to_string())
    }
}

/// Per stage: its `NAMESPACE` and whether it writes. Kinds that name none (LINK,
/// UNLINK, MERGE, SUBSCRIBE, …) count as none named: they run within the call's scope
/// through the router. A statement that doesn't parse goes to the router's executor,
/// which reports the parse error.
fn touches(aql: &str) -> Vec<(Option<String>, bool)> {
    use aql_parser::Statement as S;
    let Ok(stmt) = aql_parser::parse(aql) else { return vec![(None, false)] };
    fn of(s: &S) -> (Option<String>, bool) {
        match s {
            S::Recall(x) => (x.modifiers.namespace.clone(), false),
            S::Lookup(x) => (x.modifiers.namespace.clone(), false),
            S::Load(x) => (x.modifiers.namespace.clone(), false),
            S::Scan(x) => (x.modifiers.namespace.clone(), false),
            S::Store(x) => (x.modifiers.namespace.clone(), true),
            S::Update(x) => (x.modifiers.namespace.clone(), true),
            S::Forget(x) => (x.modifiers.namespace.clone(), true),
            _ => (None, true),
        }
    }
    match &stmt {
        S::Pipeline(p) => p.stages.iter().map(of).collect(),
        other => vec![of(other)],
    }
}

fn out_of_reach(namespace: &str) -> AdbError {
    AdbError::QueryError(format!("forbidden: namespace {namespace:?} is outside this call's scope"))
}

/// The router's reads and writes for one statement within one scope.
struct Routed {
    node: Arc<PartitionedEngine>,
    reach: Reach,
}

impl Routed {
    /// The targets for a call naming `namespace` (or none).
    async fn targets(&self, namespace: Option<&str>) -> AdbResult<Vec<Target>> {
        match namespace {
            Some(ns) => Ok(vec![self.node.target_of(&self.reach, ns).await?]),
            None => self.node.targets(&self.reach).await.map_err(AdbError::Internal),
        }
    }

    /// One target per partition the scope reaches, unlimited by namespace: for reads
    /// and writes by record id (which a namespace filter doesn't narrow). Every record
    /// they return is checked against the scope by the caller.
    async fn per_partition(&self) -> AdbResult<Vec<Target>> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for t in self.targets(None).await? {
            if seen.insert(t.partition) {
                out.push(Target { namespace: None, ..t });
            }
        }
        Ok(out)
    }

    /// Run `f` once on every partition the scope reaches.
    async fn each_partition<T, F, Fut>(&self, f: F) -> AdbResult<Vec<T>>
    where
        F: Fn(Target) -> Fut,
        Fut: std::future::Future<Output = AdbResult<T>>,
    {
        let mut out = Vec::new();
        for t in self.per_partition().await? {
            out.push(f(t).await?);
        }
        Ok(out)
    }

    /// The target to write a new record of `namespace` into (none: `""`).
    async fn write_target(&self, namespace: Option<&str>) -> AdbResult<Target> {
        let ns = namespace.unwrap_or("");
        let t = self.node.target_of(&self.reach, ns).await?;
        self.node.note(ns).await.map_err(AdbError::Internal)?;
        Ok(t)
    }

    /// The target holding record `id` of `memory` within the scope, if any.
    async fn holder(&self, memory: MemoryType, id: &str) -> AdbResult<Option<Target>> {
        for t in self.per_partition().await? {
            if let Some(r) = t.reads.records_by_id(memory, &[id.to_string()]).await?.remove(id) {
                // By id, a partition answers for every namespace in it: the scope decides.
                if self.reach.allows(r.metadata.namespace.as_deref().unwrap_or("")) {
                    return Ok(Some(t));
                }
            }
        }
        Ok(None)
    }

    /// Of `links`, those whose far end (`to_id`) can be seen in this scope.
    async fn visible(&self, links: Vec<Link>) -> AdbResult<Vec<Link>> {
        if matches!(self.reach, Reach::All) || links.is_empty() {
            return Ok(links);
        }
        let mut by_type: HashMap<MemoryType, Vec<String>> = HashMap::new();
        for l in &links {
            by_type.entry(l.to_type).or_default().push(l.to_id.clone());
        }
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for (memory, ids) in by_type {
            seen.extend(self.records_by_id(memory, &ids).await?.into_keys());
        }
        Ok(links.into_iter().filter(|l| seen.contains(&l.to_id)).collect())
    }

    /// Run `f` on every target of `namespace`, gathering the answers.
    async fn each<T, F, Fut>(&self, namespace: Option<&str>, f: F) -> AdbResult<Vec<T>>
    where
        F: Fn(Target) -> Fut,
        Fut: std::future::Future<Output = AdbResult<T>>,
    {
        let mut out = Vec::new();
        for t in self.targets(namespace).await? {
            out.push(f(t).await?);
        }
        Ok(out)
    }
}

/// Merge per-target answers as one engine would order and cut them.
fn merge(memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers, lists: Vec<Vec<MemoryRecord>>) -> Vec<MemoryRecord> {
    let mut all: Vec<MemoryRecord> = if matches!(predicate, Predicate::Like { .. } | Predicate::Pattern { .. }) && modifiers.order_by.is_none() {
        // Similarity: each list is ranked by its partition; fuse the ranks (RRF, k = 60),
        // ties to the higher similarity.
        let mut scored: Vec<(f64, f32, MemoryRecord)> = Vec::new();
        for list in lists {
            for (i, r) in list.into_iter().enumerate() {
                let sim = r.metadata.confidence.unwrap_or(-1.0);
                scored.push((1.0 / (60.0 + i as f64 + 1.0), sim, r));
            }
        }
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(b.1.total_cmp(&a.1)));
        scored.into_iter().map(|(_, _, r)| r).collect()
    } else {
        let mut all: Vec<MemoryRecord> = lists.into_iter().flatten().collect();
        crate::query::default_order(memory, &mut all);
        if let Some(o) = &modifiers.order_by {
            crate::query::order_by(&mut all, o);
        }
        all
    };
    if let Some(n) = modifiers.limit {
        all.truncate(n);
    }
    all
}

fn with_namespace(modifiers: &Modifiers, namespace: &Option<String>) -> Modifiers {
    let mut m = modifiers.clone();
    if namespace.is_some() {
        m.namespace = namespace.clone();
    }
    m
}

#[async_trait]
impl Reads for Routed {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let lists = self
            .each(modifiers.namespace.as_deref(), |t| async move { t.reads.lookup_with_modifiers(memory, predicate, &with_namespace(modifiers, &t.namespace)).await })
            .await?;
        Ok(merge(memory, predicate, modifiers, lists))
    }

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let lists = self
            .each(modifiers.namespace.as_deref(), |t| async move { t.reads.recall_with_modifiers(memory, predicate, &with_namespace(modifiers, &t.namespace)).await })
            .await?;
        Ok(merge(memory, predicate, modifiers, lists))
    }

    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>> {
        let lists = self.each_partition(|t| async move { t.reads.links_from(from_type, from_id, link_type).await }).await?;
        self.visible(lists.into_iter().flatten().collect()).await
    }

    async fn links_touching(&self, memory: MemoryType, ids: &[String], select: &LinkSelect) -> AdbResult<HashMap<String, Vec<Link>>> {
        let maps = self.each_partition(|t| async move { t.reads.links_touching(memory, ids, select).await }).await?;
        let mut out: HashMap<String, Vec<Link>> = HashMap::new();
        for m in maps {
            for (id, links) in m {
                out.entry(id).or_default().extend(links);
            }
        }
        let mut visible = HashMap::new();
        for (id, links) in out {
            visible.insert(id, self.visible(links).await?);
        }
        Ok(visible)
    }

    async fn records_by_id(&self, memory: MemoryType, ids: &[String]) -> AdbResult<HashMap<String, MemoryRecord>> {
        let maps = self.each_partition(|t| async move { t.reads.records_by_id(memory, ids).await }).await?;
        let mut out = HashMap::new();
        for m in maps {
            for (id, r) in m {
                if self.reach.allows(r.metadata.namespace.as_deref().unwrap_or("")) {
                    out.insert(id, r);
                }
            }
        }
        Ok(out)
    }

    async fn scan(&self, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        let lists = self.each(modifiers.namespace.as_deref(), |t| async move { t.reads.scan(&with_namespace(modifiers, &t.namespace)).await }).await?;
        Ok(merge(MemoryType::Working, &Predicate::All, modifiers, lists))
    }

    async fn load(&self, predicate: &Predicate, limit: usize) -> AdbResult<Vec<MemoryRecord>> {
        let lists = self.each_partition(|t| async move { t.reads.load(predicate, limit).await }).await?;
        let mut all: Vec<MemoryRecord> = lists.into_iter().flatten().filter(|r| self.reach.allows(r.metadata.namespace.as_deref().unwrap_or(""))).collect();
        all.sort_by(|a, b| b.data.get("ranking").and_then(|v| v.as_f64()).unwrap_or(0.0).total_cmp(&a.data.get("ranking").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        all.truncate(limit);
        Ok(all)
    }
}

#[async_trait]
impl Writes for Routed {
    async fn store(&self, memory_type: MemoryType, key: &str, payload: Value, scope: Scope, namespace: Option<&str>, ttl: Option<Duration>, in_scope: Option<&str>) -> AdbResult<MemoryRecord> {
        let t = self.write_target(namespace).await?;
        t.writes.store(memory_type, key, payload, scope, namespace, ttl, in_scope).await
    }

    async fn store_fact(&self, payload: Value, multi: bool, scope: Scope, namespace: Option<&str>, ttl: Option<Duration>, in_scope: Option<&str>) -> AdbResult<MemoryRecord> {
        let t = self.write_target(namespace).await?;
        t.writes.store_fact(payload, multi, scope, namespace, ttl, in_scope).await
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: Value, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        let n = self.each(namespace, |t| { let data = data.clone(); async move { t.writes.update(memory_type, predicate, data, in_scope, t.namespace.as_deref()).await } }).await?;
        Ok(n.into_iter().sum())
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        let n = self.each(namespace, |t| async move { t.writes.forget(memory_type, predicate, in_scope, t.namespace.as_deref()).await }).await?;
        Ok(n.into_iter().sum())
    }

    async fn link(&self, from_type: MemoryType, from_id: &str, to_type: MemoryType, to_id: &str, link_type: &str, weight: Option<f32>) -> AdbResult<Link> {
        let from = self.holder(from_type, from_id).await?.ok_or_else(|| AdbError::QueryError(format!("LINK: the source record {from_type:?} {from_id:?} doesn't exist (any more)")))?;
        let to = self.holder(to_type, to_id).await?.ok_or_else(|| AdbError::QueryError(format!("LINK: the target record {to_type:?} {to_id:?} doesn't exist (any more)")))?;
        if from.partition == to.partition {
            return from.writes.link(from_type, from_id, to_type, to_id, link_type, weight).await;
        }
        from.writes.link_remote(from_type, from_id, to_type, to_id, link_type, weight).await
    }

    async fn update_links(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>, weight: Option<f32>, new_type: Option<&str>) -> AdbResult<u64> {
        let n = self.each_partition(|t| async move { t.writes.update_links(from_type, from_ids, to_type, to_ids, link_type, weight, new_type).await }).await?;
        Ok(n.into_iter().sum())
    }

    async fn unlink(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>) -> AdbResult<u64> {
        let n = self.each_partition(|t| async move { t.writes.unlink(from_type, from_ids, to_type, to_ids, link_type).await }).await?;
        Ok(n.into_iter().sum())
    }

    async fn publish(&self, topic: &str, payload: Value, ttl: Option<Duration>) -> AdbResult<(String, u64)> {
        self.node.system.io().0.publish(topic, payload, ttl).await
    }

    async fn subscribe(&self, spec: adb_gateway::SubSpec, from: Option<u64>) -> AdbResult<(String, u64)> {
        self.node.system.io().0.subscribe(spec, from).await
    }

    async fn ack(&self, subscription: &str, offset: u64) -> AdbResult<u64> {
        self.node.system.io().0.ack(subscription, offset).await
    }

    async fn unsubscribe(&self, subscription: &str) -> AdbResult<bool> {
        self.node.system.io().0.unsubscribe(subscription).await
    }

    async fn platform(&self, effect: adb_core::log::Effect) -> AdbResult<u64> {
        self.node.system.io().0.platform(effect).await
    }

    async fn platform_many(&self, effects: Vec<adb_core::log::Effect>) -> AdbResult<u64> {
        self.node.system.io().0.platform_many(effects).await
    }
}

#[async_trait]
impl Engine for Arc<PartitionedEngine> {
    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        self.query_in(aql, variables, &Reach::All).await
    }

    async fn stats(&self) -> Result<Value, String> {
        let parts: Vec<Partition> = self.parts.lock().await.keys().copied().collect();
        Ok(serde_json::json!({ "partitions_open": parts, "namespaces": self.catalog.read().unwrap().len() }))
    }

    fn platform(&self) -> Option<Arc<adb_platform::PlatformStore>> {
        self.system.platform()
    }

    async fn platform_write(&self, effect: adb_core::log::Effect) -> Result<u64, String> {
        self.system.platform_write(effect).await
    }

    async fn platform_write_many(&self, effects: Vec<adb_core::log::Effect>) -> Result<u64, String> {
        self.system.platform_write_many(effects).await
    }

    fn flowr_engine(&self, space: &str) -> Option<Arc<dyn flowr_storage::engine::EngineStore>> {
        self.system.flowr_engine(space)
    }

    async fn engine_restore(&self, space: &str, changes: Vec<Value>, first: bool) -> Result<usize, String> {
        self.system.engine_restore(space, changes, first).await
    }

    // ---- one namespace: its partition

    async fn branch_create(&self, namespace: &str, name: &str, parent: Option<&str>, created_by: Option<&str>) -> Result<crate::branches::Branch, String> {
        self.note(namespace).await?;
        self.engine_of(namespace).await?.branch_create(namespace, name, parent, created_by).await
    }

    async fn branch_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        self.engine_of(namespace).await?.branch_drop(namespace, name).await
    }

    async fn branch_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Branch>, String> {
        match namespace {
            Some(ns) => self.engine_of(ns).await?.branch_list(Some(ns)).await,
            None => {
                let mut out = Vec::new();
                for e in self.data_parts().await? {
                    out.extend(e.branch_list(None).await?);
                }
                Ok(out)
            }
        }
    }

    async fn branch_create_past(&self, namespace: &str, name: &str, created_by: Option<&str>, at: &crate::branches::PointRef) -> Result<Value, String> {
        self.engine_of(namespace).await?.branch_create_past(namespace, name, created_by, at).await
    }

    async fn space_stats(&self, namespace: &str, branch: Option<&str>, max_age: Duration) -> Result<Value, String> {
        self.engine_of(namespace).await?.space_stats(namespace, branch, max_age).await
    }

    async fn space_records(&self, namespace: &str, memory: MemoryType, branch: Option<&str>, after: Option<&str>, limit: usize) -> Result<Value, String> {
        self.engine_of(namespace).await?.space_records(namespace, memory, branch, after, limit).await
    }

    async fn space_gateway(&self, namespace: &str, branch: Option<&str>) -> Result<Arc<adb_gateway::Gateway>, String> {
        self.engine_of(namespace).await?.space_gateway(namespace, branch).await
    }

    async fn branch_conflicts(&self, namespace: &str, name: &str, memories: &[MemoryType]) -> Result<Value, String> {
        self.engine_of(namespace).await?.branch_conflicts(namespace, name, memories).await
    }

    async fn branch_merge(&self, namespace: &str, name: &str, memories: &[MemoryType], main_offset: u64, resolutions: Vec<crate::merge::Resolution>, by: Option<&str>) -> Result<Value, String> {
        self.engine_of(namespace).await?.branch_merge(namespace, name, memories, main_offset, resolutions, by).await
    }

    async fn branch_rebase(&self, namespace: &str, name: &str, main_offset: u64, resolutions: Vec<crate::merge::Resolution>) -> Result<Value, String> {
        self.engine_of(namespace).await?.branch_rebase(namespace, name, main_offset, resolutions).await
    }

    async fn merge_revert(&self, namespace: &str, merge_id: &str, by: Option<&str>, force_skip: bool) -> Result<Value, String> {
        self.engine_of(namespace).await?.merge_revert(namespace, merge_id, by, force_skip).await
    }

    async fn parent_rows(&self, memory: MemoryType, namespace: &str, branch: Option<&str>, at: i64) -> Result<Vec<adb_store::StoredRow>, String> {
        self.engine_of(namespace).await?.parent_rows(memory, namespace, branch, at).await
    }

    async fn branch_place(&self, namespace: &str, name: &str, url: &str) -> Result<crate::branches::Branch, String> {
        self.engine_of(namespace).await?.branch_place(namespace, name, url).await
    }

    async fn branch_pending(&self, namespace: &str, name: &str) -> Result<Vec<crate::branches::PendingForget>, String> {
        self.engine_of(namespace).await?.branch_pending(namespace, name).await
    }

    async fn branch_pending_ack(&self, namespace: &str, name: &str, n: usize) -> Result<(), String> {
        self.engine_of(namespace).await?.branch_pending_ack(namespace, name, n).await
    }

    async fn tag_create(&self, namespace: &str, name: &str, created_by: Option<&str>) -> Result<crate::branches::Tag, String> {
        self.engine_of(namespace).await?.tag_create(namespace, name, created_by).await
    }

    async fn tag_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        self.engine_of(namespace).await?.tag_drop(namespace, name).await
    }

    async fn tag_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Tag>, String> {
        match namespace {
            Some(ns) => self.engine_of(ns).await?.tag_list(Some(ns)).await,
            None => {
                let mut out = Vec::new();
                for e in self.data_parts().await? {
                    out.extend(e.tag_list(None).await?);
                }
                Ok(out)
            }
        }
    }

    async fn layers_get(&self, namespace: &str) -> Result<Vec<crate::branches::Layer>, String> {
        self.engine_of(namespace).await?.layers_get(namespace).await
    }

    async fn layers_dependents(&self, space: &str) -> Result<Vec<crate::branches::Dependent>, String> {
        let mut out = Vec::new();
        for e in self.data_parts().await? {
            out.extend(e.layers_dependents(space).await?);
        }
        Ok(out)
    }

    async fn space_usage(&self, namespace: Option<&str>) -> Result<Value, String> {
        match namespace {
            Some(ns) => self.engine_of(ns).await?.space_usage(Some(ns)).await,
            None => Err("space usage of every namespace in a cluster: ask per namespace".into()),
        }
    }

    // ---- two namespaces: one partition only, for now

    async fn branch_promote(&self, namespace: &str, name: &str, into: &str, allow_stale: bool) -> Result<Value, String> {
        let e = self.engine_of_both("branch_promote", namespace, into).await?;
        self.note(into).await?;
        e.branch_promote(namespace, name, into, allow_stale).await
    }

    async fn space_clone(&self, namespace: &str, into: &str) -> Result<Value, String> {
        let e = self.engine_of_both("space_clone", namespace, into).await?;
        self.note(into).await?;
        e.space_clone(namespace, into).await
    }

    async fn space_clone_at(&self, namespace: &str, into: &str, at: &crate::branches::PointRef) -> Result<Value, String> {
        let e = self.engine_of_both("space_clone_at", namespace, into).await?;
        self.note(into).await?;
        e.space_clone_at(namespace, into, at).await
    }

    async fn space_copy_procedures(&self, namespace: &str, into: &str, tools: bool) -> Result<Value, String> {
        let e = self.engine_of_both("space_copy_procedures", namespace, into).await?;
        self.note(into).await?;
        e.space_copy_procedures(namespace, into, tools).await
    }

    async fn layers_set(&self, namespace: &str, layers: Vec<crate::branches::Layer>) -> Result<crate::branches::Layers, String> {
        for l in &layers {
            self.engine_of_both("a memory layer", &l.space, namespace).await?;
        }
        self.engine_of(namespace).await?.layers_set(namespace, layers).await
    }

    // ---- every partition

    async fn forget_lineages(&self, memory: MemoryType, lineages: &[String]) -> Result<u64, String> {
        let mut n = 0;
        for e in self.data_parts().await? {
            n += e.forget_lineages(memory, lineages).await?;
        }
        Ok(n)
    }

    async fn compact(&self) -> Result<Value, String> {
        let mut out = vec![self.system.compact().await?];
        for e in self.data_parts().await? {
            out.push(e.compact().await?);
        }
        Ok(Value::Array(out))
    }
}

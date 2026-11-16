//! Partitions owned by other nodes (S3 option B, step 4: the gateway).
//!
//! Every node runs the partition router ([`crate::partitioned`]). A partition this
//! node owns is a local engine; one it doesn't is a [`RemotePartition`]: the same
//! reads and writes ([`Reads`], [`Writes`]) and the same partition operations, sent to
//! the owner as a [`PartCall`] over [`Peers`] (HTTP between nodes; in one process for
//! tests). The owner checks it owns the partition and answers; a node that doesn't
//! answers `not_owner:<node>:<addr>`, and the caller re-reads the map and retries once.
//!
//! Partition operations that aren't reads or writes (branches, spaces, tags, layers,
//! copies, quotas, platform writes) go as one [`PartCall::Op`]: a name and JSON
//! arguments, run by [`dispatch_op`] — the same code whether the partition is here or
//! on another node.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adb_cluster::map::{NodeId, Partition, PartitionMap};
use adb_cluster::routing::Reach;
use adb_core::{AdbError, AdbResult, Link, LinkSelect, MemoryRecord, MemoryType, Modifiers, Predicate, Scope};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::engine::{Engine, LocalEngine};
use crate::spaces::Spaces as _;
use crate::reads::Reads;
use crate::writes::Writes;

/// Where this node stands in the cluster: who it is, and the current map.
pub trait Placement: Send + Sync {
    fn me(&self) -> NodeId;
    fn map(&self) -> Arc<PartitionMap>;
    /// Whether this node holds its lease (it heard the control plane within one): a
    /// node that doesn't may have lost its partitions, and stops serving them (step 7).
    fn leased(&self) -> bool {
        true
    }
}

/// The calls between nodes.
#[async_trait]
pub trait Peers: Send + Sync {
    /// Run a statement on the node at `addr` (which owns its one partition).
    async fn query(&self, addr: &str, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String>;
    /// Run one call on partition `partition` of the node at `addr`.
    async fn part(&self, addr: &str, partition: Partition, call: PartCall) -> Result<PartReply, String>;
    /// Send an owner's records to partition `partition`'s replica at `addr` (step 5).
    async fn replicate(&self, addr: &str, partition: Partition, req: crate::replication::ReplicateReq) -> Result<crate::replication::ReplicateAck, String>;
}

/// The answer of a node that doesn't own the partition asked of it.
/// The prefix of a peer call's error when the call was never sent (connection refused
/// or timed out): safe to send again, elsewhere.
pub const UNREACHABLE: &str = "unreachable:";

pub fn not_owner(partition: Partition, map: &PartitionMap) -> String {
    match map.assignments.get(&partition) {
        Some(a) => format!("not_owner:{}:{}: partition {partition} is owned by node {} (epoch {})", a.owner, map.members.get(&a.owner).map(|m| m.addr.as_str()).unwrap_or(""), a.owner, a.epoch),
        None => format!("not_owner:0:: partition {partition} has no owner yet"),
    }
}

/// The owner address in a [`not_owner`] answer, if it names one.
pub fn owner_in(error: &str) -> Option<String> {
    let rest = error.strip_prefix("not_owner:")?;
    let (_, rest) = rest.split_once(':')?;
    let addr = rest.split(": ").next()?;
    (!addr.is_empty()).then(|| addr.to_string())
}

/// One call on a partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartCall {
    Lookup { memory: MemoryType, predicate: Predicate, modifiers: Modifiers },
    Recall { memory: MemoryType, predicate: Predicate, modifiers: Modifiers },
    LinksFrom { from_type: MemoryType, from_id: String, link_type: Option<String> },
    LinksTouching { memory: MemoryType, ids: Vec<String>, select: LinkSelect },
    RecordsById { memory: MemoryType, ids: Vec<String> },
    Scan { modifiers: Modifiers },
    Load { predicate: Predicate, limit: usize },
    Store { memory: MemoryType, key: String, payload: Value, scope: Scope, namespace: Option<String>, ttl_ms: Option<u64>, in_scope: Option<String> },
    StoreFact { payload: Value, multi: bool, scope: Scope, namespace: Option<String>, ttl_ms: Option<u64>, in_scope: Option<String> },
    Update { memory: MemoryType, predicate: Predicate, data: Value, in_scope: Option<String>, namespace: Option<String> },
    Forget { memory: MemoryType, predicate: Predicate, in_scope: Option<String>, namespace: Option<String> },
    Link { from_type: MemoryType, from_id: String, to_type: MemoryType, to_id: String, link_type: String, weight: Option<f32>, remote_target: bool },
    UpdateLinks { from_type: MemoryType, from_ids: Vec<String>, to_type: MemoryType, to_ids: Vec<String>, link_type: Option<String>, weight: Option<f32>, new_type: Option<String> },
    Unlink { from_type: MemoryType, from_ids: Vec<String>, to_type: MemoryType, to_ids: Vec<String>, link_type: Option<String> },
    /// A partition operation by name ([`dispatch_op`]).
    Op { op: String, args: Value },
}

/// What a [`PartCall`] answers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartReply {
    Records(Vec<MemoryRecord>),
    ById(HashMap<String, MemoryRecord>),
    Links(Vec<Link>),
    LinkMap(HashMap<String, Vec<Link>>),
    Count(u64),
    Record(MemoryRecord),
    Link(Link),
    Value(Value),
}

fn unexpected(r: PartReply) -> AdbError {
    AdbError::Internal(format!("partition call: unexpected answer {r:?}"))
}

fn ms(d: Option<Duration>) -> Option<u64> {
    d.map(|d| d.as_millis() as u64)
}

/// Run `call` on a partition engine this node owns.
pub async fn serve_part(e: &LocalEngine, call: PartCall) -> Result<PartReply, String> {
    let (w, r) = e.io();
    let err = |e: AdbError| e.to_string();
    let dur = |m: Option<u64>| m.map(Duration::from_millis);
    Ok(match call {
        PartCall::Lookup { memory, predicate, modifiers } => PartReply::Records(r.lookup_with_modifiers(memory, &predicate, &modifiers).await.map_err(err)?),
        PartCall::Recall { memory, predicate, modifiers } => PartReply::Records(r.recall_with_modifiers(memory, &predicate, &modifiers).await.map_err(err)?),
        PartCall::LinksFrom { from_type, from_id, link_type } => PartReply::Links(r.links_from(from_type, &from_id, link_type.as_deref()).await.map_err(err)?),
        PartCall::LinksTouching { memory, ids, select } => PartReply::LinkMap(r.links_touching(memory, &ids, &select).await.map_err(err)?),
        PartCall::RecordsById { memory, ids } => PartReply::ById(r.records_by_id(memory, &ids).await.map_err(err)?),
        PartCall::Scan { modifiers } => PartReply::Records(r.scan(&modifiers).await.map_err(err)?),
        PartCall::Load { predicate, limit } => PartReply::Records(r.load(&predicate, limit).await.map_err(err)?),
        PartCall::Store { memory, key, payload, scope, namespace, ttl_ms, in_scope } => {
            PartReply::Record(w.store(memory, &key, payload, scope, namespace.as_deref(), dur(ttl_ms), in_scope.as_deref()).await.map_err(err)?)
        }
        PartCall::StoreFact { payload, multi, scope, namespace, ttl_ms, in_scope } => {
            PartReply::Record(w.store_fact(payload, multi, scope, namespace.as_deref(), dur(ttl_ms), in_scope.as_deref()).await.map_err(err)?)
        }
        PartCall::Update { memory, predicate, data, in_scope, namespace } => PartReply::Count(w.update(memory, &predicate, data, in_scope.as_deref(), namespace.as_deref()).await.map_err(err)?),
        PartCall::Forget { memory, predicate, in_scope, namespace } => PartReply::Count(w.forget(memory, &predicate, in_scope.as_deref(), namespace.as_deref()).await.map_err(err)?),
        PartCall::Link { from_type, from_id, to_type, to_id, link_type, weight, remote_target } => PartReply::Link(if remote_target {
            w.link_remote(from_type, &from_id, to_type, &to_id, &link_type, weight).await.map_err(err)?
        } else {
            w.link(from_type, &from_id, to_type, &to_id, &link_type, weight).await.map_err(err)?
        }),
        PartCall::UpdateLinks { from_type, from_ids, to_type, to_ids, link_type, weight, new_type } => {
            PartReply::Count(w.update_links(from_type, &from_ids, to_type, &to_ids, link_type.as_deref(), weight, new_type.as_deref()).await.map_err(err)?)
        }
        PartCall::Unlink { from_type, from_ids, to_type, to_ids, link_type } => PartReply::Count(w.unlink(from_type, &from_ids, to_type, &to_ids, link_type.as_deref()).await.map_err(err)?),
        PartCall::Op { op, args } => PartReply::Value(dispatch_op(e, &op, args).await?),
    })
}

fn arg<T: serde::de::DeserializeOwned>(args: &Value, name: &str) -> Result<T, String> {
    serde_json::from_value(args.get(name).cloned().unwrap_or(Value::Null)).map_err(|e| format!("argument {name}: {e}"))
}

fn val<T: Serialize>(v: T) -> Result<Value, String> {
    serde_json::to_value(v).map_err(|e| e.to_string())
}

/// A partition operation by name, on a partition engine this node owns.
pub async fn dispatch_op(e: &LocalEngine, op: &str, a: Value) -> Result<Value, String> {
    let s = |n: &str| arg::<String>(&a, n);
    let o = |n: &str| arg::<Option<String>>(&a, n);
    match op {
        "query" => {
            let vars: Option<HashMap<String, Value>> = arg(&a, "variables")?;
            e.query(&s("aql")?, vars.as_ref()).await
        }
        "branch_create" => val(e.branch_create(&s("namespace")?, &s("name")?, o("parent")?.as_deref(), o("created_by")?.as_deref()).await?),
        "branch_drop" => val(e.branch_drop(&s("namespace")?, &s("name")?).await?),
        "branch_list" => val(e.branch_list(o("namespace")?.as_deref()).await?),
        "branch_create_past" => e.branch_create_past(&s("namespace")?, &s("name")?, o("created_by")?.as_deref(), &arg(&a, "at")?).await,
        "space_stats" => e.space_stats(&s("namespace")?, o("branch")?.as_deref(), Duration::from_millis(arg(&a, "max_age_ms")?)).await,
        "space_records" => e.space_records(&s("namespace")?, arg(&a, "memory")?, o("branch")?.as_deref(), o("after")?.as_deref(), arg(&a, "limit")?).await,
        "branch_conflicts" => e.branch_conflicts(&s("namespace")?, &s("name")?, &arg::<Vec<MemoryType>>(&a, "memories")?).await,
        "branch_merge" => e.branch_merge(&s("namespace")?, &s("name")?, &arg::<Vec<MemoryType>>(&a, "memories")?, arg(&a, "main_offset")?, arg(&a, "resolutions")?, o("by")?.as_deref()).await,
        "branch_rebase" => e.branch_rebase(&s("namespace")?, &s("name")?, arg(&a, "main_offset")?, arg(&a, "resolutions")?).await,
        "merge_revert" => e.merge_revert(&s("namespace")?, &s("merge_id")?, o("by")?.as_deref(), arg(&a, "force_skip")?).await,
        "parent_rows" => val(e.parent_rows(arg(&a, "memory")?, &s("namespace")?, o("branch")?.as_deref(), arg(&a, "at")?).await?),
        "branch_place" => val(e.branch_place(&s("namespace")?, &s("name")?, &s("url")?).await?),
        "branch_pending" => val(e.branch_pending(&s("namespace")?, &s("name")?).await?),
        "branch_pending_ack" => val(e.branch_pending_ack(&s("namespace")?, &s("name")?, arg(&a, "n")?).await?),
        "tag_create" => val(e.tag_create(&s("namespace")?, &s("name")?, o("created_by")?.as_deref()).await?),
        "tag_drop" => val(e.tag_drop(&s("namespace")?, &s("name")?).await?),
        "tag_list" => val(e.tag_list(o("namespace")?.as_deref()).await?),
        "layers_get" => val(e.layers_get(&s("namespace")?).await?),
        "layers_set" => val(e.layers_set(&s("namespace")?, arg(&a, "layers")?).await?),
        "layers_dependents" => val(e.layers_dependents(&s("space")?).await?),
        "space_usage" => e.space_usage(o("namespace")?.as_deref()).await,
        "forget_lineages" => val(e.forget_lineages(arg(&a, "memory")?, &arg::<Vec<String>>(&a, "lineages")?).await?),
        "compact" => e.compact().await,
        // The cluster's SQL view: this partition's memory rows, Arrow IPC.
        "memory_raw" => Ok(json!({ "ipc": crate::sql::to_ipc(&e.memory_raw().await?)? })),
        "embedding" => e.embedding().await,
        "cache" => e.cache().await,
        // Cluster-wide settings, copied into a partition before it opens.
        "settings" => Ok(json!({
            "embedding": adb_embed::EmbedConfig::load(e.data_dir())?,
            "cache": adb_store::CacheConfig::load(e.data_dir())?,
        })),
        "set_cache" => e.set_cache(arg(&a, "settings")?).await,
        "branch_promote" => e.branch_promote(&s("namespace")?, &s("name")?, &s("into")?, arg(&a, "allow_stale")?).await,
        "space_clone" => e.space_clone(&s("namespace")?, &s("into")?).await,
        "space_clone_at" => e.space_clone_at(&s("namespace")?, &s("into")?, &arg(&a, "at")?).await,
        "space_copy_procedures" => e.space_copy_procedures(&s("namespace")?, &s("into")?, arg(&a, "tools")?).await,
        // Copies between partitions, and what promoting needs.
        "promote_check" => val(e.promote_check(&s("namespace")?, &s("name")?, arg(&a, "allow_stale")?).await?),
        "copy_out" => val(e.copy_out(&s("namespace")?, o("branch")?.as_deref(), &arg::<Vec<MemoryType>>(&a, "memories")?, arg(&a, "at")?).await?),
        "copy_in" => val(e.copy_in(&s("into")?, arg(&a, "rows")?).await?),
        "resolve_point" => val(e.resolve_point(&s("namespace")?, &arg(&a, "at")?).await?),
        // Other spaces (memory layers) and quotas.
        "space_versions" => {
            let l = e.local_spaces().ok_or("no table")?;
            val(l.versions(arg(&a, "memory")?, &s("space")?).await.map_err(|e| e.to_string())?)
        }
        "space_tag" => val(e.local_spaces().ok_or("no table")?.registry.tag(&s("space")?, &s("name")?)),
        "space_layers" => val(e.local_spaces().ok_or("no table")?.registry.layers_of(&s("space")?)),
        "space_layered_by" => val(e.local_spaces().ok_or("no table")?.registry.layered_by(&s("space")?)),
        "quotas" => val(e.local_spaces().ok_or("no table")?.registry.quotas()),
        "quota_set" => val(e.quota_set(&s("pattern")?, arg(&a, "max_bytes")?, arg(&a, "max_records")?, arg(&a, "history_days")?).await?),
        "quota_drop" => val(e.quota_drop(&s("pattern")?).await?),
        "usage_covered" => val(e.usage_covered(&arg(&a, "quota")?).await?),
        // Readers on other nodes (step 6).
        "log_tail" => val(e.log_tail(arg(&a, "from")?, arg(&a, "max")?).await?),
        "commit_offset" => val(e.commit_offset()),
        // The system partition.
        "platform_write" => val(e.platform_write(arg(&a, "effect")?).await?),
        "platform_write_many" => val(e.platform_write_many(arg(&a, "effects")?).await?),
        // Writes a remote partition handle sends on (any node, the owner runs them).
        "w_publish" => {
            let ttl = arg::<Option<u64>>(&a, "ttl_ms")?.map(Duration::from_millis);
            val(e.io().0.publish(&s("topic")?, arg(&a, "payload")?, ttl).await.map_err(|x| x.to_string())?)
        }
        "w_subscribe" => val(e.io().0.subscribe(arg(&a, "spec")?, arg(&a, "from")?).await.map_err(|x| x.to_string())?),
        "w_ack" => val(e.io().0.ack(&s("subscription")?, arg(&a, "offset")?).await.map_err(|x| x.to_string())?),
        "w_unsubscribe" => val(e.io().0.unsubscribe(&s("subscription")?).await.map_err(|x| x.to_string())?),
        "w_platform" => val(e.io().0.platform(arg(&a, "effect")?).await.map_err(|x| x.to_string())?),
        "w_platform_many" => val(e.io().0.platform_many(arg(&a, "effects")?).await.map_err(|x| x.to_string())?),
        "w_merge" => val(e.io().0.merge(&s("from")?, &s("into")?).await.map_err(|x| x.to_string())?),
        // A watch's next frames, from this partition's watcher.
        "watch_frames" => {
            let wait = Duration::from_millis(arg::<u64>(&a, "wait_ms")?.min(30_000));
            e.watch_frames(&s("cq_id")?, arg(&a, "from")?, arg(&a, "max")?, wait).await
        }
        // A subscription's next deliveries, from the gateway on the system partition.
        "next_events" => {
            let wait = Duration::from_millis(arg::<u64>(&a, "wait_ms")?.min(30_000));
            e.next_events(&s("subscription")?, arg(&a, "from")?, arg(&a, "max")?, wait).await
        }
        "catalog_rows" => {
            let Some(p) = e.platform() else { return val(Vec::<Value>::new()) };
            if !p.list_tables().map_err(|e| e.to_string())?.iter().any(|t| t == crate::partitioned::CATALOG) {
                return val(Vec::<Value>::new());
            }
            let rows = p.query(crate::partitioned::CATALOG, None, usize::MAX >> 1).await.map_err(|e| e.to_string())?;
            val(adb_platform::batches_to_json(&rows))
        }
        other => Err(format!("unknown partition operation {other:?}")),
    }
}

/// A partition owned by another node.
pub struct RemotePartition {
    pub partition: Partition,
    pub addr: String,
    pub peers: Arc<dyn Peers>,
}

impl RemotePartition {
    /// `call` on the owner; a `not_owner` answer names the owner now: retried there once.
    async fn send(&self, call: PartCall) -> Result<PartReply, String> {
        match self.peers.part(&self.addr, self.partition, call.clone()).await {
            Err(e) if e.starts_with("not_owner:") => match owner_in(&e).filter(|a| *a != self.addr) {
                Some(addr) => self.peers.part(&addr, self.partition, call).await,
                None => Err(e),
            },
            other => other,
        }
    }

    async fn call(&self, call: PartCall) -> AdbResult<PartReply> {
        self.send(call).await.map_err(AdbError::QueryError)
    }

    /// A partition operation by name on the owner.
    /// [`Self::op`] with a typed answer, as a write's error.
    async fn op_typed<T: serde::de::DeserializeOwned>(&self, op: &str, args: Value) -> AdbResult<T> {
        let v = self.op(op, args).await.map_err(AdbError::QueryError)?;
        serde_json::from_value(v).map_err(|e| AdbError::Internal(format!("{op}: {e}")))
    }

    pub async fn op(&self, op: &str, args: Value) -> Result<Value, String> {
        match self.send(PartCall::Op { op: op.into(), args }).await? {
            PartReply::Value(v) => Ok(v),
            other => Err(unexpected(other).to_string()),
        }
    }
}

macro_rules! expect {
    ($r:expr, $v:ident) => {
        match $r {
            PartReply::$v(x) => Ok(x),
            other => Err(unexpected(other)),
        }
    };
}

#[async_trait]
impl Reads for RemotePartition {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        expect!(self.call(PartCall::Lookup { memory, predicate: predicate.clone(), modifiers: modifiers.clone() }).await?, Records)
    }

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        expect!(self.call(PartCall::Recall { memory, predicate: predicate.clone(), modifiers: modifiers.clone() }).await?, Records)
    }

    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>> {
        expect!(self.call(PartCall::LinksFrom { from_type, from_id: from_id.into(), link_type: link_type.map(String::from) }).await?, Links)
    }

    async fn links_touching(&self, memory: MemoryType, ids: &[String], select: &LinkSelect) -> AdbResult<HashMap<String, Vec<Link>>> {
        expect!(self.call(PartCall::LinksTouching { memory, ids: ids.to_vec(), select: select.clone() }).await?, LinkMap)
    }

    async fn records_by_id(&self, memory: MemoryType, ids: &[String]) -> AdbResult<HashMap<String, MemoryRecord>> {
        expect!(self.call(PartCall::RecordsById { memory, ids: ids.to_vec() }).await?, ById)
    }

    async fn scan(&self, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        expect!(self.call(PartCall::Scan { modifiers: modifiers.clone() }).await?, Records)
    }

    async fn load(&self, predicate: &Predicate, limit: usize) -> AdbResult<Vec<MemoryRecord>> {
        expect!(self.call(PartCall::Load { predicate: predicate.clone(), limit }).await?, Records)
    }
}

#[async_trait]
impl Writes for RemotePartition {
    async fn store(&self, memory_type: MemoryType, key: &str, payload: Value, scope: Scope, namespace: Option<&str>, ttl: Option<Duration>, in_scope: Option<&str>) -> AdbResult<MemoryRecord> {
        expect!(self.call(PartCall::Store { memory: memory_type, key: key.into(), payload, scope, namespace: namespace.map(String::from), ttl_ms: ms(ttl), in_scope: in_scope.map(String::from) }).await?, Record)
    }

    async fn store_fact(&self, payload: Value, multi: bool, scope: Scope, namespace: Option<&str>, ttl: Option<Duration>, in_scope: Option<&str>) -> AdbResult<MemoryRecord> {
        expect!(self.call(PartCall::StoreFact { payload, multi, scope, namespace: namespace.map(String::from), ttl_ms: ms(ttl), in_scope: in_scope.map(String::from) }).await?, Record)
    }

    async fn update(&self, memory_type: MemoryType, predicate: &Predicate, data: Value, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        expect!(self.call(PartCall::Update { memory: memory_type, predicate: predicate.clone(), data, in_scope: in_scope.map(String::from), namespace: namespace.map(String::from) }).await?, Count)
    }

    async fn forget(&self, memory_type: MemoryType, predicate: &Predicate, in_scope: Option<&str>, namespace: Option<&str>) -> AdbResult<u64> {
        expect!(self.call(PartCall::Forget { memory: memory_type, predicate: predicate.clone(), in_scope: in_scope.map(String::from), namespace: namespace.map(String::from) }).await?, Count)
    }

    async fn link(&self, from_type: MemoryType, from_id: &str, to_type: MemoryType, to_id: &str, link_type: &str, weight: Option<f32>) -> AdbResult<Link> {
        expect!(self.call(PartCall::Link { from_type, from_id: from_id.into(), to_type, to_id: to_id.into(), link_type: link_type.into(), weight, remote_target: false }).await?, Link)
    }

    async fn link_remote(&self, from_type: MemoryType, from_id: &str, to_type: MemoryType, to_id: &str, link_type: &str, weight: Option<f32>) -> AdbResult<Link> {
        expect!(self.call(PartCall::Link { from_type, from_id: from_id.into(), to_type, to_id: to_id.into(), link_type: link_type.into(), weight, remote_target: true }).await?, Link)
    }

    async fn update_links(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>, weight: Option<f32>, new_type: Option<&str>) -> AdbResult<u64> {
        expect!(
            self.call(PartCall::UpdateLinks { from_type, from_ids: from_ids.to_vec(), to_type, to_ids: to_ids.to_vec(), link_type: link_type.map(String::from), weight, new_type: new_type.map(String::from) }).await?,
            Count
        )
    }

    async fn unlink(&self, from_type: MemoryType, from_ids: &[String], to_type: MemoryType, to_ids: &[String], link_type: Option<&str>) -> AdbResult<u64> {
        expect!(self.call(PartCall::Unlink { from_type, from_ids: from_ids.to_vec(), to_type, to_ids: to_ids.to_vec(), link_type: link_type.map(String::from) }).await?, Count)
    }

    // Topics, subscriptions, platform tables and entity merges: run by the owner.

    async fn merge(&self, from: &str, into: &str) -> AdbResult<u64> {
        self.op_typed("w_merge", serde_json::json!({ "from": from, "into": into })).await
    }

    async fn publish(&self, topic: &str, payload: Value, ttl: Option<Duration>) -> AdbResult<(String, u64)> {
        self.op_typed("w_publish", serde_json::json!({ "topic": topic, "payload": payload, "ttl_ms": ms(ttl) })).await
    }

    async fn subscribe(&self, spec: adb_gateway::SubSpec, from: Option<u64>) -> AdbResult<(String, u64)> {
        self.op_typed("w_subscribe", serde_json::json!({ "spec": spec, "from": from })).await
    }

    async fn ack(&self, subscription: &str, offset: u64) -> AdbResult<u64> {
        self.op_typed("w_ack", serde_json::json!({ "subscription": subscription, "offset": offset })).await
    }

    async fn unsubscribe(&self, subscription: &str) -> AdbResult<bool> {
        self.op_typed("w_unsubscribe", serde_json::json!({ "subscription": subscription })).await
    }

    async fn platform(&self, effect: adb_core::log::Effect) -> AdbResult<u64> {
        self.op_typed("w_platform", serde_json::json!({ "effect": effect })).await
    }

    async fn platform_many(&self, effects: Vec<adb_core::log::Effect>) -> AdbResult<u64> {
        self.op_typed("w_platform_many", serde_json::json!({ "effects": effects })).await
    }
}

/// Peers in one process, for tests: every call goes through JSON, as on the wire.
pub struct LocalPeers {
    nodes: std::sync::RwLock<HashMap<String, std::sync::Weak<crate::partitioned::PartitionedEngine>>>,
}

impl LocalPeers {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { nodes: Default::default() })
    }

    /// Reach `node` at `addr`.
    pub fn add(&self, addr: &str, node: &Arc<crate::partitioned::PartitionedEngine>) {
        self.nodes.write().unwrap().insert(addr.into(), Arc::downgrade(node));
    }

    /// `addr` stops answering (a node that died).
    pub fn remove(&self, addr: &str) {
        self.nodes.write().unwrap().remove(addr);
    }

    fn node(&self, addr: &str) -> Result<Arc<crate::partitioned::PartitionedEngine>, String> {
        self.nodes.read().unwrap().get(addr).and_then(|w| w.upgrade()).ok_or_else(|| format!("{UNREACHABLE}node {addr} is unreachable"))
    }
}

fn wire<T: Serialize + serde::de::DeserializeOwned>(v: &T) -> Result<T, String> {
    serde_json::from_slice(&serde_json::to_vec(v).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}

#[async_trait]
impl Peers for LocalPeers {
    async fn query(&self, addr: &str, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let node = self.node(addr)?;
        let (vars, reach) = (variables.map(wire).transpose()?, wire(reach)?);
        let r = node.serve_query(aql, vars.as_ref(), &reach).await?;
        wire(&r)
    }

    async fn part(&self, addr: &str, partition: Partition, call: PartCall) -> Result<PartReply, String> {
        let node = self.node(addr)?;
        let r = node.serve_part(partition, wire(&call)?).await?;
        wire(&r)
    }

    async fn replicate(&self, addr: &str, partition: Partition, req: crate::replication::ReplicateReq) -> Result<crate::replication::ReplicateAck, String> {
        let node = self.node(addr)?;
        let req: crate::replication::ReplicateReq = rmp_serde::from_slice(&rmp_serde::to_vec_named(&req).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        node.serve_replicate(partition, req)
    }
}

/// The JSON a [`PartCall::Op`] carries, built from named values.
pub fn args(pairs: &[(&str, Value)]) -> Value {
    Value::Object(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
}

pub fn j<T: Serialize>(v: T) -> Value {
    json!(v)
}

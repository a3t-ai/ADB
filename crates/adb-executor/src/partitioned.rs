//! A node's partitions behind one engine (S3 option B, steps 3 and 4).
//!
//! Every memory partition is a [`LocalEngine`] of its own ([`PartitionRole::memory`],
//! `dir/parts/p{n}`), opened on first use; the system partition
//! ([`PartitionRole::system`]) holds the platform tables, FlowR's engine store and the
//! **namespace catalog** (platform table `adb_namespaces`: every namespace that has
//! been written, and its partition).
//!
//! **In a cluster** ([`PartitionedEngine::open_cluster`], step 4) a node holds only the
//! partitions it owns; any other is a [`RemotePartition`] — the same calls, sent to its
//! owner ([`crate::remote`]). Any node accepts any request: a statement on one
//! namespace goes whole to that namespace's owner (one hop); a stale map is corrected
//! by the owner's `not_owner` answer, and the call is retried once.
//!
//! A statement runs within the scope its call carries ([`Reach`], built by A3T/FlowR;
//! ADB only enforces it):
//!
//! - **One namespace** (every stage names the same one): run as is on that
//!   namespace's partition — the common case (an agent's own memory).
//! - **Otherwise** (no `NAMESPACE`, or several): the router's own executor runs it over
//!   the partitions' reads and writes. Reads go to the namespaces of the catalog the
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
//! - Topics, subscriptions and platform writes go to the system partition. A `WATCH`
//!   on one namespace runs on its partition's owner; one across partitions on the
//!   system partition's owner ([`crate::cluster_watch`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use adb_cluster::map::{Partition, PartitionMap};
use adb_cluster::routing::{self, Reach};
use adb_core::{AdbError, AdbResult, Link, LinkSelect, MemoryRecord, MemoryType, Modifiers, Predicate, Scope};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::engine::{to_wire, Engine, LocalEngine, PartitionRole};
use crate::reads::Reads;
use crate::remote::{dispatch_op, not_owner, owner_in, Peers, Placement, RemotePartition};
use crate::writes::Writes;
use crate::Executor;

/// The catalog's platform table.
pub const CATALOG: &str = "adb_namespaces";

/// A partition, here or on its owner.
#[derive(Clone)]
pub enum Handle {
    Local(Arc<LocalEngine>),
    Remote(Arc<RemotePartition>),
}

impl Handle {
    pub fn io(&self) -> (Arc<dyn Writes>, Arc<dyn Reads>) {
        match self {
            Handle::Local(e) => e.io(),
            Handle::Remote(r) => (Arc::clone(r) as Arc<dyn Writes>, Arc::clone(r) as Arc<dyn Reads>),
        }
    }

    /// A partition operation by name ([`crate::remote::dispatch_op`]), here or on the owner.
    pub async fn op(&self, op: &str, args: Value) -> Result<Value, String> {
        match self {
            Handle::Local(e) => Box::pin(dispatch_op(e, op, args)).await,
            Handle::Remote(r) => r.op(op, args).await,
        }
    }

    pub async fn op_as<T: DeserializeOwned>(&self, op: &str, args: Value) -> Result<T, String> {
        serde_json::from_value(self.op(op, args).await?).map_err(|e| format!("{op}: {e}"))
    }
}

/// One place a routed read or write runs: a partition, and the namespace it is
/// limited to there (`None`: every namespace in it — a full-access call only).
#[derive(Clone)]
struct Target {
    partition: Partition,
    namespace: Option<String>,
    writes: Arc<dyn Writes>,
    reads: Arc<dyn Reads>,
}

impl Target {
    fn of(partition: Partition, namespace: Option<String>, h: &Handle) -> Self {
        let (writes, reads) = h.io();
        Target { partition, namespace, writes, reads }
    }
}

struct Cluster {
    placement: Arc<dyn Placement>,
    peers: Arc<dyn Peers>,
}

/// A node's partitions as one [`Engine`].
pub struct PartitionedEngine {
    dir: PathBuf,
    /// The map on one node (in a cluster: the placement's, current).
    map: std::sync::RwLock<Arc<PartitionMap>>,
    cluster: Option<Cluster>,
    /// Each partition's engine, opened once: concurrent callers share the one open, and
    /// partitions open (or are taken over) side by side — no node-wide lock (a takeover
    /// of one partition held up every request, and a second opener raced the first).
    parts: std::sync::Mutex<HashMap<Partition, (u64, PartSlot)>>,
    /// Partitions being taken over by the background task, and how many at once.
    opening: std::sync::Mutex<BTreeSet<Partition>>,
    /// Takeovers done here (tests, metrics).
    taken_over: std::sync::atomic::AtomicU64,
    takeovers: Arc<tokio::sync::Semaphore>,
    /// Namespace → partition, as recorded in [`CATALOG`] (refreshed before every
    /// statement spanning partitions: other nodes add namespaces too).
    catalog: std::sync::RwLock<BTreeMap<String, Partition>>,
    /// When the catalog was last read from the system partition.
    catalog_at: std::sync::Mutex<Option<std::time::Instant>>,
    /// The quota rules (the system partition's), as last read and when: refreshed in the
    /// background every `ADB_CATALOG_TTL_MS`, so a write's quota check never calls
    /// another node under its partition's sequencer (Hetzner round 7: a round trip
    /// under the sequencer on every write serialised a whole node).
    quota_rules: std::sync::RwLock<Option<(std::time::Instant, Vec<crate::branches::Quota>)>>,
    /// Other spaces read from their own partitions (memory layers); each partition
    /// engine holds it weakly.
    spaces: Arc<dyn crate::spaces::Spaces>,
    /// What this node holds as the replica of other nodes' partitions (step 5).
    replicas: Arc<crate::replication::ReplicaBuffers>,
    /// Readers of partitions other nodes own (step 6), opened on first read.
    readers: std::sync::Mutex<HashMap<Partition, ReaderSlot>>,
    /// At most this many readers built at once (each replays its partition's tail).
    reader_builds: Arc<tokio::sync::Semaphore>,
    /// Reads per partition at this node in the current second (when a reader pays).
    read_rate: std::sync::Mutex<(std::time::Instant, HashMap<Partition, u64>)>,
    /// Forwarded reads alternate between the owner and the replica.
    spread: std::sync::atomic::AtomicU64,
    /// Watches across partitions, on the system partition's owner ([`crate::cluster_watch`]).
    cwatches: tokio::sync::Mutex<Option<Arc<crate::cluster_watch::ClusterWatches>>>,
    me: std::sync::Weak<PartitionedEngine>,
}

/// A reader's records: this node's replica buffer while it is the partition's replica
/// (every acknowledged record is there), else the owner in the current map.
struct PartSource {
    partition: Partition,
    node: std::sync::Weak<PartitionedEngine>,
}

#[async_trait]
impl crate::readers::ReaderSource for PartSource {
    async fn tail(&self, from: u64, max: usize) -> Result<Vec<adb_core::log::LogRecord>, String> {
        let node = self.node.upgrade().ok_or("the partition router has stopped")?;
        if node.is_replica(self.partition) {
            if let Some(recs) = node.replicas.tail_from(self.partition, from, max) {
                return Ok(recs);
            }
        }
        node.handle(self.partition).await?.op_as("log_tail", json!({ "from": from, "max": max })).await
    }

    async fn commit_offset(&self) -> Result<u64, String> {
        let node = self.node.upgrade().ok_or("the partition router has stopped")?;
        if node.is_replica(self.partition) {
            if let Some(held) = node.replicas.held(self.partition) {
                return Ok(held);
            }
        }
        node.handle(self.partition).await?.op_as("commit_offset", json!({})).await
    }
}

/// A partition's engine on this node, once opened.
type PartSlot = Arc<tokio::sync::OnceCell<Arc<LocalEngine>>>;

/// `ADB_FORWARD_WAIT_MS`: how long a statement waits for an unreachable owner's
/// partition to move to another node (default 10000: a lease plus a takeover).
fn forward_wait() -> Duration {
    Duration::from_millis(std::env::var("ADB_FORWARD_WAIT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000))
}

/// `ADB_TAKEOVERS`: partitions taken over at once by a node (default 8).
fn takeovers() -> usize {
    std::env::var("ADB_TAKEOVERS").ok().and_then(|v| v.parse().ok()).filter(|&n| n > 0).unwrap_or(8)
}

/// A reader's state on this node.
enum ReaderSlot {
    /// Being built in the background: reads go to the owner meanwhile.
    Building,
    Ready(Arc<crate::readers::Reader>),
    /// The last build failed: tried again after a while.
    Failed(std::time::Instant),
}

/// `ADB_READER_BUILDS`: readers built at once on a node (default 4).
fn reader_builds() -> usize {
    std::env::var("ADB_READER_BUILDS").ok().and_then(|v| v.parse().ok()).filter(|&n| n > 0).unwrap_or(4)
}

/// `ADB_READERS`: readers on (default) or off (`0`: reads go to the owner).
fn readers_on() -> bool {
    std::env::var("ADB_READERS").map_or(true, |v| v != "0" && v != "off")
}

/// `ADB_READER_HOT`: reads per second of a partition at a node that is neither its
/// owner nor its replica past which that node keeps a reader of it (default 1000).
fn reader_hot() -> u64 {
    std::env::var("ADB_READER_HOT").ok().and_then(|v| v.parse().ok()).unwrap_or(1000)
}

/// `ADB_READER_IDLE_SECS`: such a reader is dropped after this long without reads (60).
fn reader_idle() -> std::time::Duration {
    std::time::Duration::from_secs(std::env::var("ADB_READER_IDLE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60))
}

/// Where an owned partition's records go: its replica in the current map.
struct PartTarget {
    partition: Partition,
    node: std::sync::Weak<PartitionedEngine>,
    /// The epoch this owner opened the partition at, and this node: the replica is
    /// the one the map names for THAT ownership, never a later epoch's.
    epoch: u64,
    me: adb_cluster::map::NodeId,
}

#[async_trait]
impl crate::replication::ReplicaTarget for PartTarget {
    fn target(&self) -> Option<(String, u64)> {
        let node = self.node.upgrade()?;
        let map = node.map();
        let a = map.assignments.get(&self.partition)?;
        if a.owner != self.me || a.epoch != self.epoch {
            return None;
        }
        let addr = map.members.get(&a.replica?)?.addr.clone();
        Some((addr, self.epoch))
    }

    fn superseded(&self) -> Option<String> {
        let node = self.node.upgrade()?;
        let map = node.map();
        let a = map.assignments.get(&self.partition)?;
        (a.epoch > self.epoch || (a.epoch == self.epoch && a.owner != self.me))
            .then(|| format!("partition {} is at epoch {} (owner {}); this owner held epoch {}", self.partition, a.epoch, a.owner, self.epoch))
    }

    async fn replicate(&self, addr: &str, req: crate::replication::ReplicateReq) -> Result<crate::replication::ReplicateAck, String> {
        let node = self.node.upgrade().ok_or("the partition router has stopped")?;
        let c = node.cluster.as_ref().ok_or("replication needs a cluster")?;
        c.peers.replicate(addr, self.partition, req).await
    }
}

/// Each space read from its own partition, here or on its owner (`crate::spaces`).
struct RouterSpaces {
    node: std::sync::Weak<PartitionedEngine>,
}

impl RouterSpaces {
    fn node(&self) -> AdbResult<Arc<PartitionedEngine>> {
        self.node.upgrade().ok_or_else(|| AdbError::Internal("the partition router has stopped".into()))
    }

    async fn handle_of(&self, space: &str) -> AdbResult<Handle> {
        let node = self.node()?;
        node.handle(node.partition_of(space)).await.map_err(AdbError::Internal)
    }
}

fn local_spaces(e: &LocalEngine) -> AdbResult<crate::spaces::LocalSpaces> {
    e.local_spaces().ok_or_else(|| AdbError::Internal("this partition has no table".into()))
}

#[async_trait]
impl crate::spaces::Spaces for RouterSpaces {
    async fn versions(&self, memory: adb_core::log::Memory, space: &str) -> AdbResult<Vec<adb_store::StoredRow>> {
        match self.handle_of(space).await? {
            Handle::Local(e) => local_spaces(&e)?.versions(memory, space).await,
            h => h.op_as("space_versions", json!({ "memory": memory, "space": space })).await.map_err(AdbError::Internal),
        }
    }

    async fn tag(&self, space: &str, name: &str) -> AdbResult<Option<crate::branches::Tag>> {
        match self.handle_of(space).await? {
            Handle::Local(e) => local_spaces(&e)?.tag(space, name).await,
            h => h.op_as("space_tag", json!({ "space": space, "name": name })).await.map_err(AdbError::Internal),
        }
    }

    async fn layers_of(&self, space: &str) -> AdbResult<Vec<crate::branches::Layer>> {
        match self.handle_of(space).await? {
            Handle::Local(e) => local_spaces(&e)?.layers_of(space).await,
            h => h.op_as("space_layers", json!({ "space": space })).await.map_err(AdbError::Internal),
        }
    }

    async fn base_of(&self, space: &str) -> AdbResult<Option<crate::branches::Base>> {
        match self.handle_of(space).await? {
            Handle::Local(e) => local_spaces(&e)?.base_of(space).await,
            h => h.op_as("space_base", json!({ "space": space })).await.map_err(AdbError::Internal),
        }
    }

    async fn quotas(&self) -> AdbResult<Vec<crate::branches::Quota>> {
        let node = self.node()?;
        // The system partition's owner reads its registry; any other node its copy
        // (fetched once here if it has none yet, then kept fresh in the background).
        if let Some(e) = node.system_local() {
            return local_spaces(&e)?.quotas().await;
        }
        // Younger than `ADB_CATALOG_TTL_MS` (the background keeps it so): no call.
        let ttl = Duration::from_millis(std::env::var("ADB_CATALOG_TTL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000));
        if let Some((t, q)) = node.quota_rules.read().unwrap().as_ref() {
            if t.elapsed() < ttl {
                return Ok(q.clone());
            }
        }
        node.refresh_quotas().await.map_err(AdbError::Internal)
    }

    async fn usage_elsewhere(&self, quota: &crate::branches::Quota, except: adb_core::log::PartitionId) -> AdbResult<(u64, u64)> {
        let node = self.node()?;
        node.catalog_recent().await.map_err(AdbError::Internal)?;
        let mut total = (0, 0);
        let parts: BTreeSet<Partition> = node.catalog.read().unwrap().iter().filter(|(ns, _)| quota.covers(ns)).map(|(_, p)| *p).collect();
        for p in parts.into_iter().filter(|p| *p != except.0) {
            let (r, b): (u64, u64) = node.handle(p).await.map_err(AdbError::Internal)?.op_as("usage_covered", json!({ "quota": quota })).await.map_err(AdbError::Internal)?;
            total = (total.0 + r, total.1 + b);
        }
        Ok(total)
    }

    async fn layered_by(&self, space: &str) -> AdbResult<Vec<String>> {
        let node = self.node()?;
        // A pin anywhere holds a tag: every namespace, as of now.
        node.refresh_catalog().await.map_err(AdbError::Internal)?;
        let mut out = Vec::new();
        for h in node.data_handles().await.map_err(AdbError::Internal)? {
            match h {
                Handle::Local(e) => out.extend(local_spaces(&e)?.layered_by(space).await?),
                h => out.extend(h.op_as::<Vec<String>>("space_layered_by", json!({ "space": space })).await.map_err(AdbError::Internal)?),
            }
        }
        Ok(out)
    }
}

impl PartitionedEngine {
    /// One node holding every partition of `map` (the system partition now, the
    /// memory partitions on first use), and its catalog.
    pub async fn open(dir: impl Into<PathBuf>, map: PartitionMap) -> Result<Arc<Self>, String> {
        if map.partitions == 0 {
            return Err("the partition map has no partitions yet".into());
        }
        let node = Self::new(dir.into(), map, None);
        Box::pin(node.handle(node.sys())).await?;
        Box::pin(node.refresh_catalog()).await?;
        Ok(node)
    }

    /// A cluster node (step 4): it holds the partitions `placement`'s map gives it and
    /// reaches the others through `peers`.
    pub async fn open_cluster(dir: impl Into<PathBuf>, placement: Arc<dyn Placement>, peers: Arc<dyn Peers>) -> Result<Arc<Self>, String> {
        let map = placement.map();
        if map.partitions == 0 {
            return Err("the partition map has no partitions yet".into());
        }
        let node = Self::new(dir.into(), (*map).clone(), Some(Cluster { placement, peers }));
        if node.owns(node.sys()) {
            Box::pin(node.handle(node.sys())).await?;
        }
        // Step 7: partitions given to this node are taken over at once, not at their
        // first request — until then the replica's memory is the only copy of their
        // writes not yet on object storage.
        let weak = Arc::downgrade(&node);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                let Some(node) = weak.upgrade() else { return };
                let map = node.map();
                let me = node.cluster.as_ref().map(|c| c.placement.me());
                let opened: BTreeSet<Partition> = node.opened().into_iter().map(|(p, _)| p).collect();
                for (p, a) in map.assignments.iter() {
                    // Taken over side by side (`ADB_TAKEOVERS` at once), each once.
                    if Some(a.owner) == me && !opened.contains(p) && node.leased() && node.opening.lock().unwrap().insert(*p) {
                        let (n, p) = (Arc::clone(&node), *p);
                        let permits = Arc::clone(&node.takeovers);
                        tokio::spawn(async move {
                            let _permit = permits.acquire_owned().await;
                            if let Err(e) = Box::pin(n.handle(p)).await {
                                tracing::warn!(partition = p, error = %e, "taking the partition over failed; retrying");
                            }
                            n.opening.lock().unwrap().remove(&p);
                        });
                    }
                    // The replica's reader, built ahead of the first read.
                    if Some(a.owner) != me && a.replica == me && *p != node.sys() {
                        let _ = node.reader(*p).await;
                    }
                }
                node.drop_idle_readers();
                node.drop_unowned();
                node.quotas_due().await;
                // The system partition's owner runs the watches across partitions, from
                // its takeover on (their frames go on without a request).
                if node.system_epoch_here().is_some() && opened.contains(&node.sys()) {
                    let n = Arc::clone(&node);
                    tokio::spawn(async move {
                        if let Err(e) = n.cluster_watches().await {
                            tracing::debug!(error = %e, "watches across partitions not started yet");
                        }
                    });
                }
            }
        });
        Ok(node)
    }

    fn new(dir: PathBuf, map: PartitionMap, cluster: Option<Cluster>) -> Arc<Self> {
        let _ = std::fs::create_dir_all(dir.join("parts"));
        Arc::new_cyclic(|me| Self {
            dir: dir.clone(),
            map: std::sync::RwLock::new(Arc::new(map)),
            cluster,
            parts: Default::default(),
            opening: Default::default(),
            taken_over: Default::default(),
            takeovers: Arc::new(tokio::sync::Semaphore::new(takeovers())),
            catalog: Default::default(),
            catalog_at: Default::default(),
            quota_rules: Default::default(),
            spaces: Arc::new(RouterSpaces { node: me.clone() }),
            replicas: Arc::new(crate::replication::ReplicaBuffers::new(dir.join("parts"))),
            readers: Default::default(),
            reader_builds: Arc::new(tokio::sync::Semaphore::new(reader_builds())),
            read_rate: std::sync::Mutex::new((std::time::Instant::now(), HashMap::new())),
            spread: Default::default(),
            cwatches: Default::default(),
            me: me.clone(),
        })
    }

    /// The current map.
    pub fn map(&self) -> Arc<PartitionMap> {
        match &self.cluster {
            Some(c) => c.placement.map(),
            None => Arc::clone(&self.map.read().unwrap()),
        }
    }

    fn sys(&self) -> Partition {
        routing::system_partition(&self.map())
    }

    /// Whether this node owns partition `p` (one node: every one).
    pub fn owns(&self, p: Partition) -> bool {
        match &self.cluster {
            None => true,
            Some(c) => self.map().assignments.get(&p).is_some_and(|a| a.owner == c.placement.me()),
        }
    }

    /// The partition `namespace` lives in (`""`: records without a namespace).
    pub fn partition_of(&self, namespace: &str) -> Partition {
        routing::partition_of(&self.map(), namespace)
    }

    /// Whether this node is partition `p`'s replica in the current map.
    pub fn is_replica(&self, p: Partition) -> bool {
        match &self.cluster {
            None => false,
            Some(c) => self.map().assignments.get(&p).is_some_and(|a| a.replica == Some(c.placement.me())),
        }
    }

    /// Count a read of `p` here; whether `p` is hot here (`ADB_READER_HOT`).
    fn hot(&self, p: Partition) -> bool {
        let mut rate = self.read_rate.lock().unwrap();
        if rate.0.elapsed() >= std::time::Duration::from_secs(1) {
            *rate = (std::time::Instant::now(), HashMap::new());
        }
        let n = rate.1.entry(p).or_default();
        *n += 1;
        *n >= reader_hot()
    }

    /// Drop the readers of partitions this node is not the replica of that had no reads
    /// for `ADB_READER_IDLE_SECS` (their directories go when they are next built).
    fn drop_idle_readers(&self) {
        let idle = reader_idle();
        let gone: Vec<Partition> = {
            let readers = self.readers.lock().unwrap();
            readers
                .iter()
                .filter_map(|(p, slot)| match slot {
                    ReaderSlot::Ready(r) if !self.is_replica(*p) && r.idle() >= idle => Some(*p),
                    _ => None,
                })
                .collect()
        };
        if gone.is_empty() {
            return;
        }
        let mut readers = self.readers.lock().unwrap();
        for p in gone {
            if readers.remove(&p).is_some() {
                tracing::info!(partition = p, "reader idle: dropped");
            }
        }
    }

    /// How many partition takeovers this node has started (tests, metrics).
    pub fn takeovers_started(&self) -> u64 {
        self.taken_over.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether this node has a reader of `p` ready (tests, metrics).
    pub fn has_reader(&self, p: Partition) -> bool {
        self.ready_reader(p).is_some()
    }

    /// Close the engines of partitions this node no longer owns (moved by a failover or
    /// a rebalance): they take no writes, and a later takeover opens a fresh one.
    fn drop_unowned(&self) {
        if self.cluster.is_none() {
            return;
        }
        let map = self.map();
        let me = self.cluster.as_ref().map(|c| c.placement.me());
        let mut parts = self.parts.lock().unwrap();
        let gone: Vec<Partition> = parts
            .iter()
            .filter(|(p, (e, c))| c.initialized() && map.assignments.get(p).is_none_or(|a| Some(a.owner) != me || a.epoch != *e))
            .map(|(p, _)| *p)
            .collect();
        for p in gone {
            parts.remove(&p);
            tracing::info!(partition = p, "partition moved away: its engine closed here");
        }
    }

    /// The reader of `p` if one is ready here (none is built).
    fn ready_reader(&self, p: Partition) -> Option<Arc<crate::readers::Reader>> {
        match self.readers.lock().unwrap().get(&p) {
            Some(ReaderSlot::Ready(r)) => Some(Arc::clone(r)),
            _ => None,
        }
    }

    /// The reader to answer a read of `p` here: the replica's always, another node's
    /// once `p` is hot here; `None` sends the read on.
    async fn reader_for_read(&self, p: Partition) -> Option<Arc<crate::readers::Reader>> {
        if self.is_replica(p) || self.ready_reader(p).is_some() {
            self.hot(p);
            return self.reader(p).await;
        }
        if self.hot(p) {
            return self.reader(p).await;
        }
        None
    }

    /// Whether this node holds its lease (one node: always).
    fn leased(&self) -> bool {
        self.cluster.as_ref().is_none_or(|c| c.placement.leased())
    }

    /// Partition `p`: its engine if this node owns it, else its owner.
    pub async fn handle(&self, p: Partition) -> Result<Handle, String> {
        let Some(c) = &self.cluster else { return Ok(Handle::Local(Box::pin(self.part(p)).await?)) };
        let map = self.map();
        let a = map.assignments.get(&p).ok_or_else(|| not_owner(p, &map))?;
        if a.owner == c.placement.me() {
            // Without a lease this node may have lost the partition already (step 7).
            if !c.placement.leased() {
                return Err(format!("no_lease: node {} hasn't heard the control plane within a lease; it doesn't serve partition {p}", a.owner));
            }
            return Ok(Handle::Local(Box::pin(self.part(p)).await?));
        }
        let addr = map.members.get(&a.owner).map(|m| m.addr.clone()).ok_or_else(|| not_owner(p, &map))?;
        Ok(Handle::Remote(Arc::new(RemotePartition { partition: p, addr, peers: Arc::clone(&c.peers) })))
    }

    /// Take partition `p` over at `epoch` (step 7): its directory rebuilt from object
    /// storage — only what epochs below `epoch` wrote — then the records this node holds
    /// as its replica appended (the acknowledged writes not yet on object storage). A
    /// gap between the two is refused: serving would lose acknowledged writes.
    async fn take_over(&self, p: Partition, epoch: u64, pdir: &std::path::Path) -> Result<(), String> {
        let t0 = std::time::Instant::now();
        self.taken_over.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if pdir.exists() {
            std::fs::remove_dir_all(pdir).map_err(|e| format!("taking over partition {p}: {e}"))?;
        }
        let cold = crate::engine::object_storage();
        // Sealed first: from here no record of the old owner is acknowledged by this
        // replica, so the tail taken now holds every one it ever acknowledged.
        let was_replica = self.replicas.held(p).is_some();
        let (tail_epoch, tail) = self.replicas.seal(p, epoch)?;
        // The control plane moves a partition only to its replica (failover promotes
        // it; a rebalance makes a node the replica first). A node taking over without
        // ever having been the replica has only object storage: whatever the old owner
        // acknowledged and had not shipped is not here.
        if !was_replica {
            tracing::warn!(partition = p, epoch, "taking over a partition this node never replicated: only what object storage holds; acknowledged writes the old owner had not shipped are not here");
        }
        let mut ends_at = 0;
        if let Some(cold) = &cold {
            let st = crate::restore::restore_partition_from_object_storage(pdir, cold, adb_core::log::PartitionId(p), Some(epoch)).await?;
            ends_at = st.log_ends_at;
        }
        let role = if p == self.sys() { PartitionRole::system(p) } else { PartitionRole::memory(p) };
        let mut appended = 0;
        if !tail.is_empty() && tail_epoch < epoch {
            let wal = LocalEngine::open_partition_wal(pdir, role).await?;
            for r in tail {
                let last = adb_log::Log::last_offset(&*wal).0;
                if r.offset.0 <= last {
                    continue;
                }
                if r.offset.0 != last + 1 {
                    return Err(format!("taking over partition {p}: object storage ends at {last}, this replica's tail starts at {}: acknowledged writes would be lost", r.offset.0));
                }
                adb_log::Log::append(&*wal, r).await.map_err(|e| format!("taking over partition {p}: {e}"))?;
                appended += 1;
            }
            ends_at = adb_log::Log::last_offset(&*wal).0;
        }
        tracing::info!(partition = p, epoch, from_object_storage = cold.is_some(), tail_records = appended, log_ends_at = ends_at, took_ms = t0.elapsed().as_millis() as u64, "partition taken over");
        Ok(())
    }

    /// The reader of memory partition `p`, if this node reads it rather than owns it
    /// (a cluster, object storage, `ADB_READERS` on): opened on first use; `None` falls
    /// back to asking the owner.
    ///
    /// A reader is built in the background (a few at a time, `ADB_READER_BUILDS`): until
    /// it is ready this answers `None`, and the read goes to the owner — a read never
    /// waits for a reader to be built.
    pub async fn reader(&self, p: Partition) -> Option<Arc<crate::readers::Reader>> {
        if self.cluster.is_none() || !readers_on() || p == self.sys() {
            return None;
        }
        let cold = crate::engine::object_storage()?;
        {
            let mut readers = self.readers.lock().unwrap();
            if self.owns(p) {
                // Owned now (a takeover): its reader is of no more use.
                readers.remove(&p);
                return None;
            }
            match readers.get(&p) {
                Some(ReaderSlot::Ready(r)) => return Some(Arc::clone(r)),
                Some(ReaderSlot::Building) => return None,
                Some(ReaderSlot::Failed(at)) if at.elapsed() < std::time::Duration::from_secs(30) => return None,
                _ => {}
            }
            readers.insert(p, ReaderSlot::Building);
        }
        let me = self.me.upgrade()?;
        let permits = Arc::clone(&self.reader_builds);
        tokio::spawn(async move {
            let _permit = permits.acquire_owned().await;
            let source = Arc::new(PartSource { partition: p, node: Arc::downgrade(&me) });
            let t0 = std::time::Instant::now();
            let built = Box::pin(crate::readers::Reader::open(me.dir.join("parts").join(format!("reader-p{p}")), p, Some(cold), source)).await;
            let mut readers = me.readers.lock().unwrap();
            match built {
                Ok(r) if !me.owns(p) => {
                    tracing::info!(partition = p, took_ms = t0.elapsed().as_millis() as u64, "reader ready");
                    readers.insert(p, ReaderSlot::Ready(r));
                }
                Ok(_) => {
                    readers.remove(&p);
                }
                Err(e) => {
                    tracing::warn!(partition = p, error = %e, "reader not built; reads go to the owner, retried later");
                    readers.insert(p, ReaderSlot::Failed(std::time::Instant::now()));
                }
            }
        });
        None
    }

    async fn system_handle(&self) -> Result<Handle, String> {
        self.handle(self.sys()).await
    }

    /// The handle of `namespace`'s partition.
    async fn handle_of(&self, namespace: &str) -> Result<Handle, String> {
        self.handle(self.partition_of(namespace)).await
    }

    /// The engine of partition `p` on this node, opened on first use.
    /// Boxed with an explicit `Send`: opening a partition reaches the system
    /// partition's, through here again.
    pub fn part(&self, p: Partition) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Arc<LocalEngine>, String>> + Send + '_>> {
        Box::pin(async move {
            // One slot per partition and epoch: a partition this node owns again at a
            // newer epoch gets a new slot (and a takeover); an engine a newer owner fenced
            // at this epoch answers `moved` until the map here catches up.
            let epoch = self.map().assignments.get(&p).map_or(0, |a| a.epoch);
            let slot = {
                let mut parts = self.parts.lock().unwrap();
                if parts.get(&p).is_some_and(|(e, _)| *e != epoch) {
                    parts.remove(&p);
                }
                Arc::clone(&parts.entry(p).or_insert_with(|| (epoch, Default::default())).1)
            };
            if let Some(e) = slot.get() {
                if e.moved() {
                    return Err(format!("moved: partition {p} moved to a newer owner; retry"));
                }
                return Ok(Arc::clone(e));
            }
            let t0 = std::time::Instant::now();
            let me = self.me.upgrade().ok_or("the partition router has stopped")?;
            // The slot's initialisation runs inside a task of its own, never in the caller:
            // a caller given up (a client timeout) would abandon an initialisation held in
            // its own future, and the next caller start a second open on the same
            // directory while the first still ran (Hetzner round 8: a takeover failed and
            // writes waited 44 s). The task can't be cancelled; every caller waits on the
            // one open.
            let task_slot = Arc::clone(&slot);
            let e = tokio::spawn(async move { task_slot.get_or_try_init(|| async { Box::pin(me.open_part(p)).await }).await.map(Arc::clone) })
                .await
                .map_err(|e| format!("opening partition {p}: {e}"))??;
            crate::steps::add("partition open wait", t0.elapsed());
            Ok(e)
        })
    }

    /// Open partition `p` (taking it over first when this node holds an older epoch):
    /// called once per partition, through its slot.
    async fn open_part(&self, p: Partition) -> Result<Arc<LocalEngine>, String> {
        let sys = self.sys();
        let pdir = self.dir.join("parts").join(format!("p{p}"));
        // Step 7: owned at an epoch this directory doesn't hold — taken over from another
        // node, or held at an older epoch — rebuilt from object storage and this node's
        // replica tail before anything opens it.
        let epoch = self.map().assignments.get(&p).map(|a| a.epoch);
        if let (Some(_), Some(epoch)) = (&self.cluster, epoch) {
            let held = std::fs::read_to_string(pdir.join("owner.epoch")).ok().and_then(|s| s.trim().parse::<u64>().ok());
            if held != Some(epoch) {
                Box::pin(self.take_over(p, epoch, &pdir)).await?;
            }
        }
        std::fs::create_dir_all(&pdir).map_err(|e| format!("partition data dir {}: {e}", pdir.display()))?;
        if p != sys {
            // Cluster-wide settings (one embedding model, one set of cache rules): the
            // system partition's, copied before a partition opens.
            let settings: Value = match self.system_local() {
                Some(s) => dispatch_op(&s, "settings", json!({})).await?,
                // Boxed: opening a partition may reach the system partition's handle.
                None => Box::pin(self.system_handle()).await?.op("settings", json!({})).await.unwrap_or(Value::Null),
            };
            if let Some(c) = settings.get("embedding").cloned().filter(|v| !v.is_null()) {
                serde_json::from_value::<adb_embed::EmbedConfig>(c).map_err(|e| e.to_string())?.save(&pdir)?;
            }
            if let Some(c) = settings.get("cache").cloned().filter(|v| !v.is_null()) {
                serde_json::from_value::<adb_store::CacheConfig>(c).map_err(|e| e.to_string())?.save(&pdir)?;
            }
        }
        let role = if p == sys { PartitionRole::system(p) } else { PartitionRole::memory(p) };
        let (e, _) = match (&self.cluster, crate::replication::replicas()) {
            // A cluster owner: every record also on the partition's replica (step 5).
            (Some(_), n) if n > 0 => {
                let wal = LocalEngine::open_partition_wal_sync(&pdir, role, crate::replication::owner_sync()).await?;
                let me_id = self.cluster.as_ref().map(|c| c.placement.me()).unwrap_or_default();
                let target = Arc::new(PartTarget { partition: p, node: self.me.clone(), epoch: epoch.unwrap_or(1), me: me_id });
                let log = crate::replication::ReplicatedLog::new(wal as Arc<dyn adb_log::Log>, target);
                Box::pin(LocalEngine::durable_partition_on(&pdir, role, log as crate::engine::EngineLog)).await?
            }
            _ => Box::pin(LocalEngine::durable_partition(&pdir, role)).await?,
        };
        // A cluster owner records its epoch on object storage before taking writes, then
        // here (a restart at the same epoch opens what it has).
        if let (Some(_), Some(epoch)) = (&self.cluster, epoch) {
            e.begin_epoch(epoch).await?;
            std::fs::write(pdir.join("owner.epoch"), epoch.to_string()).map_err(|e| format!("owner epoch: {e}"))?;
            self.replicas.clear(p);
        }
        // Layer spaces in other partitions are read from there.
        e.set_spaces(Arc::downgrade(&self.spaces));
        let e = Arc::new(e);
        Ok(e)
    }

    /// The system partition's engine, while this node owns it and has it open: looked
    /// up, not kept, so an engine closed when the partition moved away is not used.
    fn system_local(&self) -> Option<Arc<LocalEngine>> {
        if !self.owns(self.sys()) {
            return None;
        }
        let parts = self.parts.lock().unwrap();
        parts.get(&self.sys()).and_then(|(_, c)| c.get().cloned()).filter(|e| !e.moved())
    }

    /// The partitions open here, with their engines.
    fn opened(&self) -> Vec<(Partition, Arc<LocalEngine>)> {
        self.parts.lock().unwrap().iter().filter_map(|(p, (_, c))| c.get().map(|e| (*p, Arc::clone(e)))).collect()
    }

    /// Every namespace written so far, with its partition (as last read).
    pub fn catalog(&self) -> BTreeMap<String, Partition> {
        self.catalog.read().unwrap().clone()
    }

    /// The catalog, read again if older than `ADB_CATALOG_TTL_MS` (default 1000): for
    /// checks that run on many writes (pattern quotas), which may lag a namespace
    /// another node added that long ago.
    pub async fn catalog_recent(&self) -> Result<(), String> {
        let ttl = Duration::from_millis(std::env::var("ADB_CATALOG_TTL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000));
        let fresh = self.catalog_at.lock().unwrap().is_some_and(|t| t.elapsed() < ttl);
        if fresh || self.cluster.is_none() {
            return Ok(());
        }
        self.refresh_catalog().await
    }

    /// Read the quota rules again from the system partition (they are configuration:
    /// a change reaches the other nodes within `ADB_CATALOG_TTL_MS`).
    pub async fn refresh_quotas(&self) -> Result<Vec<crate::branches::Quota>, String> {
        let q: Vec<crate::branches::Quota> = match self.system_handle().await? {
            Handle::Local(e) => {
                let local = local_spaces(&e).map_err(|e| e.to_string())?;
                crate::spaces::Spaces::quotas(&local).await.map_err(|e| e.to_string())?
            }
            h => h.op_as("quotas", json!({})).await?,
        };
        *self.quota_rules.write().unwrap() = Some((std::time::Instant::now(), q.clone()));
        Ok(q)
    }

    /// Keep the copy of the quota rules fresh (the background loop, not the write path).
    async fn quotas_due(&self) {
        let ttl = Duration::from_millis(std::env::var("ADB_CATALOG_TTL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000));
        let due = self.quota_rules.read().unwrap().as_ref().is_none_or(|(t, _)| t.elapsed() >= ttl);
        if due && self.cluster.is_some() && self.system_local().is_none() {
            if let Err(e) = self.refresh_quotas().await {
                tracing::debug!(error = %e, "quota rules not refreshed; the last copy stays");
            }
        }
    }

    /// Read the catalog again from the system partition.
    pub async fn refresh_catalog(&self) -> Result<(), String> {
        let rows: Vec<Value> = self.system_handle().await?.op_as("catalog_rows", json!({})).await?;
        *self.catalog_at.lock().unwrap() = Some(std::time::Instant::now());
        let mut catalog = self.catalog.write().unwrap();
        for r in rows {
            let ns = r.get("namespace").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            if let Some(p) = r.get("partition").and_then(|v| v.as_str()).and_then(|v| v.parse().ok()) {
                catalog.insert(ns, p);
            }
        }
        Ok(())
    }

    /// Record `namespace` in the catalog (before its first write commits).
    async fn note(&self, namespace: &str) -> Result<(), String> {
        // A branch (`ns@b`) is reached by naming it, never by a read across every
        // namespace: not in the catalog.
        if namespace.contains(routing::BRANCH_SEP) || self.catalog.read().unwrap().contains_key(namespace) {
            return Ok(());
        }
        let p = self.partition_of(namespace);
        let row = json!({ "namespace": namespace, "partition": p.to_string() });
        let effect = adb_core::log::Effect::TableInsert { table: CATALOG.into(), rows: vec![row.as_object().cloned().unwrap_or_default()] };
        self.system_handle().await?.op("platform_write", json!({ "effect": effect })).await?;
        self.catalog.write().unwrap().insert(namespace.to_string(), p);
        Ok(())
    }

    /// A target on partition `p`: its own reads and writes here; for a partition another
    /// node owns, its writes go to the owner and its reads to this node's reader, once
    /// that holds everything committed now (else to the owner too).
    async fn target(&self, p: Partition, namespace: Option<String>) -> Result<Target, String> {
        let h = self.handle(p).await?;
        let mut t = Target::of(p, namespace, &h);
        if matches!(h, Handle::Remote(_)) {
            if let Some(r) = self.reader_for_read(p).await {
                match r.fresh_reads().await {
                    Ok(reads) => t.reads = reads,
                    Err(e) => tracing::debug!(partition = p, error = %e, "reader behind; this read goes to the owner"),
                }
            }
        }
        Ok(t)
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
                    out.push(self.target(p, None).await?);
                }
            }
            Reach::Only(_) => {
                for (ns, p) in catalog.iter().filter(|(ns, _)| reach.allows(ns)) {
                    out.push(self.target(*p, Some(ns.clone())).await?);
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
        self.target(p, Some(namespace.to_string())).await.map_err(AdbError::QueryError)
    }

    /// The partitions holding data (the catalog's), here or remote.
    async fn data_handles(&self) -> Result<Vec<Handle>, String> {
        let ps: BTreeSet<Partition> = self.catalog.read().unwrap().values().copied().collect();
        let mut out = Vec::new();
        for p in ps {
            out.push(self.handle(p).await?);
        }
        Ok(out)
    }

    /// Run `aql` within `reach` (see the module docs).
    pub async fn query_in(self: &Arc<Self>, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        // AQL v0.13: a script runs statement by statement, each with its USE applied.
        if aql_parser::is_script(aql) {
            return crate::engine::run_script(aql, None, |stmt| async move { Box::pin(self.query_in(&stmt, variables, reach)).await }).await;
        }
        if let Some(r) = Box::pin(self.watch_routed(aql, variables, reach)).await {
            return r;
        }
        let (single, writes_any) = single_namespace(aql, reach)?;
        if let Some(ns) = single {
            if writes_any {
                self.note(&ns).await?;
            }
            return Box::pin(self.on_owner(&ns, aql, variables, reach)).await;
        }
        Box::pin(self.across(aql, variables, reach)).await
    }

    /// A statement on one namespace, on its partition: here, or forwarded whole to the
    /// owner (one hop; a `not_owner` answer means the map here was stale: retried once,
    /// at the owner it names or the map now names).
    async fn on_owner(self: &Arc<Self>, ns: &str, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let p = self.partition_of(ns);
        // A read of a partition another node owns: answered here, by its reader (step 6).
        let read = !self.owns(p) && crate::follower::follower_reads(aql);
        if read {
            if let Some(r) = self.reader_for_read(p).await {
                // A reader behind or failing: the owner answers instead.
                match Box::pin(r.query(aql, variables)).await {
                    Ok(v) => return Ok(v),
                    Err(e) => tracing::debug!(partition = p, error = %e, "reader didn't answer; asking the owner"),
                }
            }
            // No reader here: every other read to the replica (its reader), the rest
            // to the owner; the replica unable to answer passes it to the owner.
            if let Some(addr) = self.replica_addr(p) {
                if self.spread.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 2 == 0 {
                    let peers = &self.cluster.as_ref().expect("a replica means a cluster").peers;
                    let t = std::time::Instant::now();
                    let r = peers.query(&addr, aql, variables, reach).await;
                    crate::steps::add("router.forward to replica (round trip)", t.elapsed());
                    match r {
                        Ok(v) => return Ok(v),
                        Err(e) => tracing::debug!(partition = p, error = %e, "the replica didn't answer; asking the owner"),
                    }
                }
            }
        }
        let mut last: Option<String> = None;
        let mut attempt = 0;
        // An owner that can't be reached (the call was never sent: it died, the map
        // hasn't moved yet): asked again, through the map as it is then, until another
        // node owns the partition or `ADB_FORWARD_WAIT_MS` passes — the write waits for
        // the failover instead of failing.
        let deadline = std::time::Instant::now() + forward_wait();
        loop {
            let addr = match self.handle(p).await? {
                Handle::Local(e) => return Box::pin(e.query(aql, variables)).await,
                Handle::Remote(r) => match (&last, attempt) {
                    (Some(e), 1) => owner_in(e).filter(|a| *a != r.addr).unwrap_or_else(|| r.addr.clone()),
                    _ => r.addr.clone(),
                },
            };
            let peers = &self.cluster.as_ref().expect("a remote partition means a cluster").peers;
            let t = std::time::Instant::now();
            let r = peers.query(&addr, aql, variables, reach).await;
            crate::steps::add("router.forward to owner (round trip)", t.elapsed());
            match r {
                Err(e) if e.starts_with("not_owner:") && attempt == 0 => {
                    last = Some(e);
                    attempt = 1;
                }
                Err(e) if e.starts_with(crate::remote::UNREACHABLE) && std::time::Instant::now() < deadline => {
                    tracing::debug!(partition = p, error = %e, "owner unreachable; waiting for the map to move");
                    last = None;
                    attempt = 0;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) if e.starts_with("not_owner:") => return Err(e),
                other => return other,
            }
        }
    }

    /// A live query (`SUBSCRIBE RECALL …`) and its UNSUBSCRIBE (W6) on a cluster;
    /// `None` for any other statement (a topic's UNSUBSCRIBE too). A live query runs
    /// on the owner of its namespace's partition, and its id names that partition
    /// (`<partition>.<id>`), so UNSUBSCRIBE and its frames (MCP watch_changes, SSE)
    /// reach it from any node; one without a namespace, or across several, runs on
    /// the system partition (`c.<id>`).
    async fn watch_routed(self: &Arc<Self>, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Option<Result<Value, String>> {
        use aql_parser::ast::Statement;
        // A statement that doesn't parse: the router's executor reports it.
        let stmt = crate::engine::watch_statement(aql)?;
        Some(match stmt {
            Statement::Subscribe(_) => {
                let query = crate::engine::live_query(&stmt)?;
                let single = match &query {
                    Statement::Recall(r) if r.modifiers.namespaces.is_empty() => r.modifiers.namespace.clone(),
                    _ => None,
                };
                let Some(ns) = single else {
                    return Some(match self.system_handle().await {
                        Ok(Handle::Local(_)) => Box::pin(self.cluster_watch(aql, variables, reach)).await,
                        Ok(Handle::Remote(r)) => r.op("cluster_watch", json!({ "aql": aql, "variables": variables, "reach": reach })).await,
                        Err(e) => Err(e),
                    });
                };
                if !reach.allows(&ns) {
                    return Some(Err(out_of_reach(&ns).to_string()));
                }
                let p = self.partition_of(&ns);
                Box::pin(self.on_owner(&ns, aql, variables, reach)).await.map(|mut v| {
                    prefix_cq_ids(&mut v, p);
                    v
                })
            }
            Statement::Unsubscribe(u) => {
                if let Some(id) = u.subscription.strip_prefix(crate::cluster_watch::PREFIX) {
                    return Some(match self.system_handle().await {
                        Ok(Handle::Local(_)) => self.cluster_unwatch(id).await,
                        Ok(Handle::Remote(r)) => r.op("cluster_unwatch", json!({ "id": id })).await,
                        Err(e) => Err(e),
                    });
                }
                // Not a live query's id: a topic subscription's, for the planner.
                let (p, id) = split_cq_id(&u.subscription)?;
                let local = format!("UNSUBSCRIBE {}", serde_json::to_string(id).unwrap_or_default());
                Box::pin(self.query_on(p, &local)).await.map(|mut v| {
                    prefix_cq_ids(&mut v, p);
                    v
                })
            }
            _ => return None,
        })
    }

    /// `aql` on partition `p`'s owner (here, or one call to it).
    async fn query_on(self: &Arc<Self>, p: Partition, aql: &str) -> Result<Value, String> {
        match self.handle(p).await? {
            Handle::Local(e) => Box::pin(e.query(aql, None)).await,
            Handle::Remote(r) => r.op("query", json!({ "aql": aql, "variables": null })).await,
        }
    }

    /// A watch's frames (`<partition>.<id>`), from its partition's watcher.
    pub async fn watch_frames_of(self: &Arc<Self>, cq_id: &str, from: Option<u64>, max: usize, wait: Duration) -> Result<Value, String> {
        if let Some(id) = cq_id.strip_prefix(crate::cluster_watch::PREFIX) {
            return match self.system_handle().await? {
                Handle::Local(_) => self.cluster_watches().await?.frames(id, from, max, wait).await,
                Handle::Remote(r) => r.op("cluster_watch_frames", json!({ "id": id, "from": from, "max": max, "wait_ms": wait.as_millis() as u64 })).await,
            };
        }
        let (p, id) = split_cq_id(cq_id).ok_or_else(|| format!("unknown watch {cq_id}"))?;
        let mut v = match self.handle(p).await? {
            Handle::Local(e) => e.watch_frames(id, from, max, wait).await?,
            Handle::Remote(r) => r.op("watch_frames", json!({ "cq_id": id, "from": from, "max": max, "wait_ms": wait.as_millis() as u64 })).await?,
        };
        prefix_cq_ids(&mut v, p);
        Ok(v)
    }

    /// A statement spanning partitions: the router's executor over their reads and writes.
    async fn across(self: &Arc<Self>, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let first = aql.trim_start().split(|c: char| c.is_whitespace()).next().unwrap_or_default();
        if first.eq_ignore_ascii_case("WATCH") || first.eq_ignore_ascii_case("UNWATCH") {
            // `query_in` routes them (`watch_routed`); a forwarded one is not run here.
            return Err("WATCH / UNWATCH are routed by the node that took the call".into());
        }
        // At most ADB_CATALOG_TTL_MS old: a namespace another node created within that
        // may be missed by a statement naming none (re-reading it on every such
        // statement cost a platform-table read each).
        self.catalog_recent().await?;
        let io = Arc::new(Routed { node: Arc::clone(self), reach: reach.clone() });
        let executor = Executor::with_io(Arc::clone(&io) as Arc<dyn Writes>, io as Arc<dyn Reads>);
        let r = match variables {
            Some(v) if !v.is_empty() => executor.execute_with_vars(aql, v).await,
            _ => executor.execute(aql).await,
        };
        r.map(|r| to_wire(&r)).map_err(|e| e.to_string())
    }

    /// A statement another node forwarded here (it was on one namespace, which this
    /// node owned in that node's map): run here, or `not_owner` — never forwarded on.
    pub async fn serve_query(self: &Arc<Self>, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let _t = crate::steps::timer("router.serve forwarded query");
        let (single, _) = single_namespace(aql, reach)?;
        match single {
            Some(ns) => {
                let p = self.partition_of(&ns);
                if !self.owns(p) {
                    // The replica answers reads sent to it from its reader, when ready.
                    if self.is_replica(p) && crate::follower::follower_reads(aql) {
                        if let Some(r) = self.ready_reader(p) {
                            self.hot(p);
                            if let Ok(v) = Box::pin(r.query(aql, variables)).await {
                                return Ok(v);
                            }
                        }
                    }
                    return Err(not_owner(p, &self.map()));
                }
                Box::pin(self.part(p)).await?.query(aql, variables).await
            }
            None => Box::pin(self.across(aql, variables, reach)).await,
        }
    }

    /// An owner's records for partition `p`, of which this node is the replica (step 5):
    /// `not_replica` if it isn't in this node's map, `fenced` for an older epoch.
    pub fn serve_replicate(&self, p: Partition, req: crate::replication::ReplicateReq) -> Result<crate::replication::ReplicateAck, String> {
        let map = self.map();
        let me = self.cluster.as_ref().map(|c| c.placement.me()).ok_or("replication needs a cluster")?;
        let a = map.assignments.get(&p).ok_or_else(|| format!("not_replica: partition {p} has no assignment here"))?;
        // A newer epoch here: the sender no longer owns the partition.
        if a.epoch > req.epoch {
            return Err(format!("fenced: partition {p} is at epoch {} (owner {}); epoch {} is stale", a.epoch, a.owner, req.epoch));
        }
        if a.replica != Some(me) {
            return Err(format!("not_replica: node {me} is not partition {p}'s replica (epoch {})", a.epoch));
        }
        let ack = self.replicas.append(p, a.epoch, req)?;
        // Its reader applies them now.
        if let Some(r) = self.ready_reader(p) {
            r.nudge();
        }
        Ok(ack)
    }

    /// Partition `p`'s replica's address, when it has one other than this node.
    fn replica_addr(&self, p: Partition) -> Option<String> {
        let map = self.map();
        let me = self.cluster.as_ref()?.placement.me();
        let r = map.assignments.get(&p)?.replica.filter(|r| *r != me)?;
        map.members.get(&r).map(|m| m.addr.clone())
    }

    /// What this node holds as partition `p`'s replica: (epoch, records above object storage).
    pub fn replica_tail(&self, p: Partition) -> Result<(u64, Vec<adb_core::log::LogRecord>), String> {
        self.replicas.tail(p)
    }

    /// (records in memory, bytes in memory, bytes spilled) of partition `p`'s replica here.
    pub fn replica_usage(&self, p: Partition) -> (usize, u64, u64) {
        self.replicas.usage(p)
    }

    /// A partition call another node sent here: run on the partition if this node
    /// owns it, else `not_owner`.
    pub async fn serve_part(&self, p: Partition, call: crate::remote::PartCall) -> Result<crate::remote::PartReply, String> {
        if !self.owns(p) {
            return Err(not_owner(p, &self.map()));
        }
        // The cluster's SQL view runs on the system partition's owner, over the node.
        // So do the watches across partitions.
        if let crate::remote::PartCall::Op { op, args } = &call {
            if p == self.sys() {
                let a = |k: &str| args[k].as_str().unwrap_or_default();
                let reply = |v: Value| Ok(crate::remote::PartReply::Value(v));
                match op.as_str() {
                    "sql" => {
                        let r = self.sql_view(a("sql"), args["limit"].as_u64().map(|l| l as usize)).await?;
                        return reply(json!({ "ipc": crate::sql::to_ipc(&r)? }));
                    }
                    "cluster_watch" | "cluster_unwatch" | "cluster_watch_frames" => {
                        let node = self.me.upgrade().ok_or("the node is stopping")?;
                        return reply(match op.as_str() {
                            "cluster_watch" => {
                                let vars: Option<HashMap<String, Value>> = serde_json::from_value(args["variables"].clone()).map_err(|e| e.to_string())?;
                                let reach: Reach = serde_json::from_value(args["reach"].clone()).map_err(|e| e.to_string())?;
                                Box::pin(node.cluster_watch(a("aql"), vars.as_ref(), &reach)).await?
                            }
                            "cluster_unwatch" => node.cluster_unwatch(a("id")).await?,
                            _ => {
                                let wait = Duration::from_millis(args["wait_ms"].as_u64().unwrap_or(0));
                                let max = args["max"].as_u64().unwrap_or(100) as usize;
                                node.cluster_watches().await?.frames(a("id"), args["from"].as_u64(), max, wait).await?
                            }
                        });
                    }
                    _ => {}
                }
            }
        }
        let e = self.part(p).await?;
        crate::remote::serve_part(&e, call).await
    }

    /// The system partition's epoch, when this node owns it (one node: 0).
    pub(crate) fn system_epoch_here(&self) -> Option<u64> {
        let sys = self.sys();
        self.owns(sys).then(|| self.map().assignments.get(&sys).map_or(0, |a| a.epoch))
    }

    /// An operation on the system partition, here or on its owner.
    pub(crate) async fn system_op(&self, op: &str, args: Value) -> Result<Value, String> {
        self.system_handle().await?.op(op, args).await
    }

    /// The partitions holding data, as the catalog now says.
    pub(crate) async fn data_partitions(&self) -> Result<BTreeSet<Partition>, String> {
        self.refresh_catalog().await?;
        Ok(self.catalog.read().unwrap().values().copied().collect())
    }

    /// A parsed statement across the partitions, within `reach` (a watch's re-run).
    pub(crate) async fn run_across(self: &Arc<Self>, stmt: &aql_parser::ast::Statement, variables: &HashMap<String, Value>, reach: &Reach) -> Result<Value, String> {
        self.catalog_recent().await?;
        let io = Arc::new(Routed { node: Arc::clone(self), reach: reach.clone() });
        let executor = Executor::with_io(Arc::clone(&io) as Arc<dyn Writes>, io as Arc<dyn Reads>);
        executor.execute_statement(stmt, variables).await.map(|r| to_wire(&r)).map_err(|e| e.to_string())
    }

    /// The watches across partitions, on the system partition's owner (started here
    /// on first use, again after this node took the system partition over).
    pub(crate) async fn cluster_watches(self: &Arc<Self>) -> Result<Arc<crate::cluster_watch::ClusterWatches>, String> {
        let epoch = self.system_epoch_here().ok_or_else(|| not_owner(self.sys(), &self.map()))?;
        let mut g = self.cwatches.lock().await;
        if let Some(cw) = g.as_ref() {
            if cw.epoch == epoch && cw.alive() {
                return Ok(Arc::clone(cw));
            }
            cw.stop();
        }
        let cw = crate::cluster_watch::ClusterWatches::start(self, epoch).await?;
        *g = Some(Arc::clone(&cw));
        Ok(cw)
    }

    /// `WATCH` across partitions (no NAMESPACE, or several), on the system partition's
    /// owner: registered there with the call's scope, its snapshot taken across.
    pub(crate) async fn cluster_watch(self: &Arc<Self>, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Result<Value, String> {
        let started = std::time::Instant::now();
        let stmt = aql_parser::parse(aql).map_err(|e| e.to_string())?;
        let query = crate::engine::live_query(&stmt).ok_or("not a live query (SUBSCRIBE RECALL …)")?;
        let mut spec = adb_cq::WatchSpec::new(query, variables.cloned().unwrap_or_default(), None)?;
        spec.across = Some(serde_json::to_value(reach).map_err(|e| e.to_string())?);
        // Refuse a query that fails before anything is committed.
        self.run_across(&spec.statement, &spec.variables, reach).await?;
        let cw = self.cluster_watches().await?;
        let id = self.system_op("cq_register", json!({ "spec": spec })).await?["id"].as_str().ok_or("cq_register: no id")?.to_string();
        let frame = match cw.add(&id, spec).await {
            Ok(f) => f,
            Err(e) => {
                let _ = self.system_op("cq_unregister", json!({ "id": id })).await;
                return Err(e);
            }
        };
        Ok(to_wire(&crate::QueryResult::subscription(frame, started.elapsed().as_millis() as u64)))
    }

    /// `UNWATCH c.<id>`, on the system partition's owner.
    pub(crate) async fn cluster_unwatch(self: &Arc<Self>, id: &str) -> Result<Value, String> {
        let started = std::time::Instant::now();
        let cw = self.cluster_watches().await?;
        let existed = cw.contains(id);
        if existed {
            self.system_op("cq_unregister", json!({ "id": id })).await?;
            cw.remove(id);
        }
        let v = json!({ "cq_id": format!("{}{id}", crate::cluster_watch::PREFIX), "unwatched": existed });
        Ok(to_wire(&crate::QueryResult::subscription(v, started.elapsed().as_millis() as u64)))
    }

    /// Read-only SQL (the superadmin's debug view, [`crate::sql`]) on a cluster: run
    /// on the system partition's owner, over the platform tables there and the memory
    /// rows of every partition holding data (and the system partition's), gathered.
    pub async fn sql_view(&self, sql: &str, limit: Option<usize>) -> Result<crate::sql::SqlResult, String> {
        let ipc = |v: Value, what: &str| -> Result<crate::sql::SqlResult, String> { crate::sql::from_ipc(v["ipc"].as_str().ok_or_else(|| format!("{what}: no rows in the answer"))?) };
        let sys = match self.system_handle().await? {
            Handle::Remote(r) => return ipc(r.op("sql", json!({ "sql": sql, "limit": limit })).await?, "sql"),
            Handle::Local(e) => e,
        };
        self.refresh_catalog().await?;
        let mut parts: BTreeSet<Partition> = self.catalog.read().unwrap().values().copied().collect();
        parts.insert(self.sys());
        let gathered = futures::future::try_join_all(parts.into_iter().map(|p| async move {
            match self.handle(p).await? {
                Handle::Local(e) => e.memory_raw().await,
                Handle::Remote(r) => ipc(r.op("memory_raw", json!({})).await?, "memory_raw"),
            }
            .map_err(|e| format!("partition {p}: {e}"))
        }))
        .await?;
        let schema = Arc::clone(&gathered.first().ok_or("no partitions")?.schema);
        let batches = gathered.into_iter().flat_map(|r| r.batches).collect();
        crate::sql::run_gathered(crate::sql::SqlResult { schema, batches }, sys.platform(), sql, limit).await
    }

    /// A partition operation on `namespace`'s partition.
    async fn op_ns<T: DeserializeOwned>(&self, namespace: &str, op: &str, args: Value) -> Result<T, String> {
        self.handle_of(namespace).await?.op_as(op, args).await
    }

    /// A partition operation on every partition holding data, gathered.
    async fn op_all<T: DeserializeOwned>(&self, op: &str, args: Value) -> Result<Vec<T>, String> {
        // Every partition holding data, as of now (other nodes add namespaces too).
        self.refresh_catalog().await?;
        let mut out = Vec::new();
        for h in self.data_handles().await? {
            out.push(h.op_as(op, args.clone()).await?);
        }
        Ok(out)
    }
}

/// The one namespace every stage of `aql` names (`None`: none, or several), and
/// whether it writes; a named namespace outside `reach` is refused.
fn single_namespace(aql: &str, reach: &Reach) -> Result<(Option<String>, bool), String> {
    let touches = touches(aql);
    for (ns, _) in &touches {
        if let Some(ns) = ns {
            if !reach.allows(ns) {
                return Err(out_of_reach(ns).to_string());
            }
        }
    }
    // A namespace's topics (AQL v0.12) are on the system partition, within the scope.
    if let Some(ns) = topic_namespace(aql) {
        if !reach.allows(&ns) {
            return Err(out_of_reach(&ns).to_string());
        }
    }
    let names: BTreeSet<Option<&str>> = touches.iter().map(|(ns, _)| ns.as_deref()).collect();
    let writes_any = touches.iter().any(|(_, w)| *w);
    match (names.len(), names.iter().next()) {
        (1, Some(Some(ns))) => Ok((Some(ns.to_string()), writes_any)),
        _ => Ok((None, writes_any)),
    }
}

/// Per stage: its `NAMESPACE` and whether it writes. Kinds that name none (LINK,
/// UNLINK, MERGE, SUBSCRIBE, …) count as none named: they run within the call's scope
/// through the router. A statement that doesn't parse goes to the router's executor,
/// which reports the parse error.
fn touches(aql: &str) -> Vec<(Option<String>, bool)> {
    use aql_parser::Statement as S;
    let Ok(stmt) = aql_parser::parse(aql) else { return vec![(None, false)] };
    let of = touches_of;
    match &stmt {
        S::Pipeline(p) => p.stages.iter().map(of).collect(),
        other => vec![of(other)],
    }
}

/// The NAMESPACE a topic statement names (SUBSCRIBE, PUBLISH, ACK, UNSUBSCRIBE).
fn topic_namespace(aql: &str) -> Option<String> {
    use aql_parser::Statement as S;
    match aql_parser::parse(aql).ok()? {
        S::Subscribe(x) => x.namespace,
        S::Publish(x) => x.modifiers.namespace,
        S::Unsubscribe(x) => x.namespace,
        _ => None,
    }
}

/// One statement's `NAMESPACE` and whether it writes ([`touches`]).
fn touches_of(s: &aql_parser::Statement) -> (Option<String>, bool) {
    use aql_parser::Statement as S;
    {
        match s {
            S::Recall(x) => (x.modifiers.namespace.clone(), false),
            S::Store(x) => (x.modifiers.namespace.clone(), true),
            S::Update(x) => (x.modifiers.namespace.clone(), true),
            S::Forget(x) => (x.modifiers.namespace.clone(), true),
            // A live query is on its RECALL's namespace (its partition registers it);
            // the owner runs it — it isn't a read a reader answers.
            S::Subscribe(aql_parser::ast::SubscribeStmt { target: aql_parser::ast::SubscribeTarget::Query(r), .. }) => (r.modifiers.namespace.clone(), false),
            // A link's two ends are in its NAMESPACE (AQL v0.12): one partition.
            S::Link(x) => (x.namespace.clone(), true),
            S::Unlink(x) => (x.namespace.clone(), true),
            _ => (None, true),
        }
    }
}

/// A cluster watch id: `<partition>.<id on that partition>`.
fn split_cq_id(cq_id: &str) -> Option<(Partition, &str)> {
    let (p, id) = cq_id.split_once('.')?;
    let p: Partition = p.parse().ok()?;
    (!id.is_empty()).then_some((p, id))
}

/// Every `cq_id` in an answer (a WATCH's snapshot frame, UNWATCH's answer, frames)
/// named with its partition.
fn prefix_cq_ids(v: &mut Value, p: Partition) {
    match v {
        Value::Object(m) => {
            if let Some(Value::String(id)) = m.get_mut("cq_id") {
                if split_cq_id(id).is_none_or(|(q, _)| q != p) {
                    *id = format!("{p}.{id}");
                }
            }
            m.values_mut().for_each(|x| prefix_cq_ids(x, p));
        }
        Value::Array(a) => a.iter_mut().for_each(|x| prefix_cq_ids(x, p)),
        _ => {}
    }
}

fn now() -> adb_core::log::CommitTs {
    adb_core::log::CommitTs::from_datetime(adb_core::time::now())
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
            None => self.node.targets(&self.reach).await.map_err(AdbError::QueryError),
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

    /// The system partition's writes (topics, subscriptions, platform tables).
    async fn system(&self) -> AdbResult<Arc<dyn Writes>> {
        Ok(self.node.system_handle().await.map_err(AdbError::QueryError)?.io().0)
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
        let n = self
            .each(namespace, |t| {
                let data = data.clone();
                async move { t.writes.update(memory_type, predicate, data, in_scope, t.namespace.as_deref()).await }
            })
            .await?;
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
        self.system().await?.publish(topic, payload, ttl).await
    }

    async fn subscribe(&self, spec: adb_gateway::SubSpec, from: Option<u64>) -> AdbResult<(String, u64)> {
        self.system().await?.subscribe(spec, from).await
    }

    async fn ack(&self, subscription: &str, offset: u64) -> AdbResult<u64> {
        self.system().await?.ack(subscription, offset).await
    }

    async fn unsubscribe(&self, subscription: &str) -> AdbResult<bool> {
        self.system().await?.unsubscribe(subscription).await
    }

    async fn subscription_topic(&self, subscription: &str) -> AdbResult<Option<String>> {
        self.system().await?.subscription_topic(subscription).await
    }

    async fn platform(&self, effect: adb_core::log::Effect) -> AdbResult<u64> {
        self.system().await?.platform(effect).await
    }

    async fn platform_many(&self, effects: Vec<adb_core::log::Effect>) -> AdbResult<u64> {
        self.system().await?.platform_many(effects).await
    }

    /// A merged entity's key across partitions: the first partition in scope that
    /// knows it (GR-12).
    async fn alias_resolve(&self, key: &str) -> AdbResult<Option<String>> {
        let found = self.each(None, |t| async move { t.writes.alias_resolve(key).await }).await?;
        Ok(found.into_iter().flatten().next())
    }

    /// MERGE ENTITY: in the namespace's partition (without one: the entities without
    /// one, in their partition).
    async fn merge(&self, from: &str, into: &str, namespace: Option<&str>) -> AdbResult<u64> {
        self.write_target(namespace).await?.writes.merge(from, into, namespace).await
    }
}

#[async_trait]
impl Engine for Arc<PartitionedEngine> {
    async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>) -> Result<Value, String> {
        Box::pin(self.query_in(aql, variables, &Reach::All)).await
    }

    async fn query_reach(&self, aql: &str, variables: Option<&HashMap<String, Value>>, reach: &Reach) -> Option<Result<Value, String>> {
        Some(Box::pin(self.query_in(aql, variables, reach)).await)
    }

    fn system_flight(&self) -> Option<String> {
        let sys = self.sys();
        if self.owns(sys) {
            return None;
        }
        let map = self.map();
        let owner = map.assignments.get(&sys)?.owner;
        map.members.get(&owner)?.flight.clone()
    }

    async fn stats(&self) -> Result<Value, String> {
        let parts: Vec<Partition> = self.opened().into_iter().map(|(p, _)| p).collect();
        Ok(json!({ "partitions_open": parts, "namespaces": self.catalog.read().unwrap().len() }))
    }

    /// The platform tables, on the node owning the system partition (`None` elsewhere:
    /// the transports send those calls to that node, step 4b).
    fn platform(&self) -> Option<Arc<adb_platform::PlatformStore>> {
        self.system_local().and_then(|s| s.platform())
    }

    async fn sql(&self, sql: &str, limit: Option<usize>) -> Result<crate::sql::SqlResult, String> {
        self.sql_view(sql, limit).await
    }

    async fn platform_write(&self, effect: adb_core::log::Effect) -> Result<u64, String> {
        self.system_handle().await?.op_as("platform_write", json!({ "effect": effect })).await
    }

    async fn platform_write_many(&self, effects: Vec<adb_core::log::Effect>) -> Result<u64, String> {
        self.system_handle().await?.op_as("platform_write_many", json!({ "effects": effects })).await
    }

    /// FlowR's engine store, on the node owning the system partition (step 4b).
    fn flowr_engine(&self, space: &str) -> Option<Arc<dyn flowr_storage::engine::EngineStore>> {
        self.system_local().and_then(|s| s.flowr_engine(space))
    }

    async fn watch_frames(&self, cq_id: &str, from: Option<u64>, max: usize, wait: Duration) -> Result<Value, String> {
        self.watch_frames_of(cq_id, from, max, wait).await
    }

    /// The gateway lives with the system partition: here when this node owns it.
    fn gateway(&self) -> Option<Arc<adb_gateway::Gateway>> {
        self.system_local().and_then(|s| s.gateway())
    }

    async fn next_events(&self, subscription: &str, from: Option<u64>, max: usize, wait: Duration) -> Result<Value, String> {
        match self.system_local() {
            Some(s) => s.next_events(subscription, from, max, wait).await,
            None => {
                let args = json!({ "subscription": subscription, "from": from, "max": max, "wait_ms": wait.as_millis() as u64 });
                self.system_handle().await?.op("next_events", args).await
            }
        }
    }

    async fn engine_restore(&self, space: &str, changes: Vec<Value>, first: bool) -> Result<usize, String> {
        let s = self.system_local().ok_or("FlowR's engine store is on the node owning the system partition")?;
        s.engine_restore(space, changes, first).await
    }

    // ---- one namespace: its partition

    async fn branch_create(&self, namespace: &str, name: &str, parent: Option<&str>, created_by: Option<&str>) -> Result<crate::branches::Branch, String> {
        self.note(namespace).await?;
        self.op_ns(namespace, "branch_create", json!({ "namespace": namespace, "name": name, "parent": parent, "created_by": created_by })).await
    }

    async fn branch_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        self.op_ns(namespace, "branch_drop", json!({ "namespace": namespace, "name": name })).await
    }

    async fn branch_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Branch>, String> {
        match namespace {
            Some(ns) => self.op_ns(ns, "branch_list", json!({ "namespace": ns })).await,
            None => Ok(self.op_all::<Vec<crate::branches::Branch>>("branch_list", json!({ "namespace": null })).await?.into_iter().flatten().collect()),
        }
    }

    async fn branch_create_past(&self, namespace: &str, name: &str, created_by: Option<&str>, at: &crate::branches::PointRef) -> Result<Value, String> {
        self.op_ns(namespace, "branch_create_past", json!({ "namespace": namespace, "name": name, "created_by": created_by, "at": at })).await
    }

    async fn space_stats(&self, namespace: &str, branch: Option<&str>, max_age: Duration) -> Result<Value, String> {
        self.op_ns(namespace, "space_stats", json!({ "namespace": namespace, "branch": branch, "max_age_ms": max_age.as_millis() as u64 })).await
    }

    async fn space_records(&self, namespace: &str, memory: MemoryType, branch: Option<&str>, after: Option<&str>, limit: usize) -> Result<Value, String> {
        self.op_ns(namespace, "space_records", json!({ "namespace": namespace, "memory": memory, "branch": branch, "after": after, "limit": limit })).await
    }

    /// A space's change stream: served by the node owning its partition.
    async fn space_gateway(&self, namespace: &str, branch: Option<&str>) -> Result<Arc<adb_gateway::Gateway>, String> {
        match self.handle_of(namespace).await? {
            Handle::Local(e) => e.space_gateway(namespace, branch).await,
            Handle::Remote(r) => Err(format!("not_owner:0:{}: namespace {namespace:?}'s changes are served by its owner", r.addr)),
        }
    }

    async fn branch_conflicts(&self, namespace: &str, name: &str, memories: &[MemoryType]) -> Result<Value, String> {
        self.op_ns(namespace, "branch_conflicts", json!({ "namespace": namespace, "name": name, "memories": memories })).await
    }

    async fn branch_merge(&self, namespace: &str, name: &str, memories: &[MemoryType], main_offset: u64, resolutions: Vec<crate::merge::Resolution>, by: Option<&str>) -> Result<Value, String> {
        self.op_ns(namespace, "branch_merge", json!({ "namespace": namespace, "name": name, "memories": memories, "main_offset": main_offset, "resolutions": resolutions, "by": by })).await
    }

    async fn branch_rebase(&self, namespace: &str, name: &str, main_offset: u64, resolutions: Vec<crate::merge::Resolution>) -> Result<Value, String> {
        self.op_ns(namespace, "branch_rebase", json!({ "namespace": namespace, "name": name, "main_offset": main_offset, "resolutions": resolutions })).await
    }

    async fn merge_revert(&self, namespace: &str, merge_id: &str, by: Option<&str>, force_skip: bool) -> Result<Value, String> {
        self.op_ns(namespace, "merge_revert", json!({ "namespace": namespace, "merge_id": merge_id, "by": by, "force_skip": force_skip })).await
    }

    async fn parent_rows(&self, memory: MemoryType, namespace: &str, branch: Option<&str>, at: i64) -> Result<Vec<adb_store::StoredRow>, String> {
        self.op_ns(namespace, "parent_rows", json!({ "memory": memory, "namespace": namespace, "branch": branch, "at": at })).await
    }

    async fn branch_place(&self, namespace: &str, name: &str, url: &str) -> Result<crate::branches::Branch, String> {
        self.op_ns(namespace, "branch_place", json!({ "namespace": namespace, "name": name, "url": url })).await
    }

    async fn branch_pending(&self, namespace: &str, name: &str) -> Result<Vec<crate::branches::PendingForget>, String> {
        self.op_ns(namespace, "branch_pending", json!({ "namespace": namespace, "name": name })).await
    }

    async fn branch_pending_ack(&self, namespace: &str, name: &str, n: usize) -> Result<(), String> {
        self.op_ns(namespace, "branch_pending_ack", json!({ "namespace": namespace, "name": name, "n": n })).await
    }

    async fn tag_create(&self, namespace: &str, name: &str, created_by: Option<&str>) -> Result<crate::branches::Tag, String> {
        self.op_ns(namespace, "tag_create", json!({ "namespace": namespace, "name": name, "created_by": created_by })).await
    }

    async fn tag_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        self.op_ns(namespace, "tag_drop", json!({ "namespace": namespace, "name": name })).await
    }

    async fn tag_list(&self, namespace: Option<&str>) -> Result<Vec<crate::branches::Tag>, String> {
        match namespace {
            Some(ns) => self.op_ns(ns, "tag_list", json!({ "namespace": ns })).await,
            None => Ok(self.op_all::<Vec<crate::branches::Tag>>("tag_list", json!({ "namespace": null })).await?.into_iter().flatten().collect()),
        }
    }

    async fn layers_get(&self, namespace: &str) -> Result<Vec<crate::branches::Layer>, String> {
        self.op_ns(namespace, "layers_get", json!({ "namespace": namespace })).await
    }

    async fn layers_dependents(&self, space: &str) -> Result<Vec<crate::branches::Dependent>, String> {
        Ok(self.op_all::<Vec<crate::branches::Dependent>>("layers_dependents", json!({ "space": space })).await?.into_iter().flatten().collect())
    }

    async fn space_usage(&self, namespace: Option<&str>) -> Result<Value, String> {
        match namespace {
            Some(ns) => self.op_ns(ns, "space_usage", json!({ "namespace": ns })).await,
            None => Err("space usage of every namespace in a cluster: ask per namespace".into()),
        }
    }

    // ---- two namespaces: in one partition as before; across partitions read out of
    // the source's, materialised in the target's (not one transaction: a copy that
    // fails half way is repeated into a fresh namespace)

    async fn branch_promote(&self, namespace: &str, name: &str, into: &str, allow_stale: bool) -> Result<Value, String> {
        self.note(into).await?;
        if self.partition_of(namespace) == self.partition_of(into) {
            return self.op_ns(namespace, "branch_promote", json!({ "namespace": namespace, "name": name, "into": into, "allow_stale": allow_stale })).await;
        }
        let (src, dst) = (self.handle_of(namespace).await?, self.handle_of(into).await?);
        let branch: crate::branches::Branch = src.op_as("promote_check", json!({ "namespace": namespace, "name": name, "allow_stale": allow_stale })).await?;
        let all = [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools];
        let rows: Value = src.op("copy_out", json!({ "namespace": namespace, "branch": name, "memories": all, "at": now() })).await?;
        let n: u64 = dst.op_as("copy_in", json!({ "into": into, "rows": rows })).await?;
        // D1, as on one engine: the new agent carries the old one's layers, reads its
        // episodes as of now (a history layer, read from the old one's partition), and
        // its first record says where it came from.
        let old_layers: Vec<crate::branches::Layer> = src.op_as("layers_get", json!({ "namespace": namespace })).await?;
        let safe: String = into.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '-' }).take(55).collect();
        let tag = format!("promoted-{safe}");
        src.op("tag_create", json!({ "namespace": namespace, "name": tag, "created_by": null })).await?;
        let mut layers = vec![crate::branches::Layer { space: namespace.to_string(), mode: crate::branches::LayerMode::History, tag: Some(tag), memories: vec![MemoryType::Episodic], sealed: false }];
        layers.extend(old_layers);
        dst.op("layers_set", json!({ "namespace": into, "layers": layers })).await?;
        let s = |v: &str| serde_json::to_string(v).unwrap_or_else(|_| "\"\"".into());
        let aql = format!(
            "STORE INTO EPISODIC (event = \"promoted_from\", from_namespace = {}, from_branch = {}, point_offset = \"{}\", point_ts = \"{}\") NAMESPACE {}",
            s(namespace), s(name), branch.point_offset, branch.point_ts, s(into)
        );
        dst.op("query", json!({ "aql": aql, "variables": null })).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": name } }))
    }

    async fn space_clone(&self, namespace: &str, into: &str) -> Result<Value, String> {
        self.note(into).await?;
        if self.partition_of(namespace) == self.partition_of(into) {
            return self.op_ns(namespace, "space_clone", json!({ "namespace": namespace, "into": into })).await;
        }
        let all = [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools];
        let rows: Value = self.handle_of(namespace).await?.op("copy_out", json!({ "namespace": namespace, "branch": null, "memories": all, "at": now() })).await?;
        let n: u64 = self.op_ns(into, "copy_in", json!({ "into": into, "rows": rows })).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN } }))
    }

    async fn space_clone_at(&self, namespace: &str, into: &str, at: &crate::branches::PointRef) -> Result<Value, String> {
        self.note(into).await?;
        if self.partition_of(namespace) == self.partition_of(into) {
            return self.op_ns(namespace, "space_clone_at", json!({ "namespace": namespace, "into": into, "at": at })).await;
        }
        let src = self.handle_of(namespace).await?;
        let (offset, ts): (u64, adb_core::log::CommitTs) = src.op_as("resolve_point", json!({ "namespace": namespace, "at": at })).await?;
        let all = [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools];
        let rows: Value = src.op("copy_out", json!({ "namespace": namespace, "branch": null, "memories": all, "at": ts })).await?;
        let n: u64 = self.op_ns(into, "copy_in", json!({ "into": into, "rows": rows })).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN, "point": { "offset": offset, "ts": ts.0 } } }))
    }

    async fn space_copy_procedures(&self, namespace: &str, into: &str, tools: bool) -> Result<Value, String> {
        self.note(into).await?;
        if self.partition_of(namespace) == self.partition_of(into) {
            return self.op_ns(namespace, "space_copy_procedures", json!({ "namespace": namespace, "into": into, "tools": tools })).await;
        }
        let memories: Vec<MemoryType> = if tools { vec![MemoryType::Procedural, MemoryType::Tools] } else { vec![MemoryType::Procedural] };
        let rows: Value = self.handle_of(namespace).await?.op("copy_out", json!({ "namespace": namespace, "branch": null, "memories": memories, "at": now() })).await?;
        let n: u64 = self.op_ns(into, "copy_in", json!({ "into": into, "rows": rows })).await?;
        Ok(json!({ "namespace": into, "records": n, "from": { "namespace": namespace, "branch": crate::branches::MAIN }, "procedures_only": true }))
    }

    async fn layers_set(&self, namespace: &str, layers: Vec<crate::branches::Layer>) -> Result<crate::branches::Layers, String> {
        // Layer spaces are read from their own partitions (RouterSpaces).
        self.note(namespace).await?;
        self.op_ns(namespace, "layers_set", json!({ "namespace": namespace, "layers": layers })).await
    }

    // ---- every partition

    async fn forget_lineages(&self, memory: MemoryType, lineages: &[String]) -> Result<u64, String> {
        Ok(self.op_all::<u64>("forget_lineages", json!({ "memory": memory, "lineages": lineages })).await?.into_iter().sum())
    }

    // ---- quotas: kept once, in the system partition (every partition reads them)

    async fn quota_set(&self, pattern: &str, max_bytes: Option<u64>, max_records: Option<u64>, history_days: Option<u64>) -> Result<crate::branches::Quota, String> {
        self.system_handle().await?.op_as("quota_set", json!({ "pattern": pattern, "max_bytes": max_bytes, "max_records": max_records, "history_days": history_days })).await
    }

    async fn quota_drop(&self, pattern: &str) -> Result<bool, String> {
        self.system_handle().await?.op_as("quota_drop", json!({ "pattern": pattern })).await
    }

    async fn quota_list(&self) -> Result<Vec<crate::branches::Quota>, String> {
        self.system_handle().await?.op_as("quotas", json!({})).await
    }

    // ---- settings: cluster-wide

    async fn embedding(&self) -> Result<Value, String> {
        self.system_handle().await?.op("embedding", json!({})).await
    }

    /// Checked and saved by the system partition (which restarts ADB to apply them),
    /// and saved for every other partition on this node, so all embed with one model.
    /// Changed on the node owning the system partition.
    async fn set_embedding(&self, settings: Value) -> Result<Value, String> {
        let s = self.system_local().ok_or("embedding settings are changed on the node owning the system partition")?;
        let c: adb_embed::EmbedConfig = serde_json::from_value(settings.clone()).map_err(|e| format!("embedding settings: {e}"))?;
        let r = s.set_embedding(settings).await?;
        if let Ok(dirs) = std::fs::read_dir(self.dir.join("parts")) {
            for d in dirs.flatten().filter(|d| d.path().is_dir()) {
                c.save(&d.path())?;
            }
        }
        Ok(r)
    }

    async fn cache(&self) -> Result<Value, String> {
        self.system_handle().await?.op("cache", json!({})).await
    }

    /// Applied to the system partition and every partition holding data (each saves
    /// them); a partition opened later copies the system partition's.
    async fn set_cache(&self, settings: Value) -> Result<Value, String> {
        let r = self.system_handle().await?.op("set_cache", json!({ "settings": settings })).await?;
        let sys = self.sys();
        for (_, e) in self.opened().into_iter().filter(|(p, _)| *p != sys) {
            e.set_cache(settings.clone()).await?;
        }
        for h in self.data_handles().await? {
            if let Handle::Remote(r) = h {
                r.op("set_cache", json!({ "settings": settings })).await?;
            }
        }
        Ok(r)
    }

    async fn compact(&self) -> Result<Value, String> {
        let mut out = vec![self.system_handle().await?.op("compact", json!({})).await?];
        out.extend(self.op_all::<Value>("compact", json!({})).await?);
        Ok(Value::Array(out))
    }
}

#[cfg(test)]
mod watch_routing_tests {
    use super::*;

    #[test]
    fn a_watch_names_its_query_namespace_and_ids_carry_the_partition() {
        let stmt = aql_parser::parse(r#"SUBSCRIBE RECALL FROM SEMANTIC NAMESPACE "demo--w""#).expect("parses");
        assert_eq!(touches_of(&stmt).0.as_deref(), Some("demo--w"), "{stmt:?}");
        assert_eq!(split_cq_id("12.abc"), Some((12, "abc")));
        assert_eq!(split_cq_id("abc"), None);
        assert_eq!(split_cq_id("12."), None);
        let mut v = json!({ "data": { "cq_id": "abc", "frames": [{ "cq_id": "abc" }, { "cq_id": "7.abc" }] } });
        prefix_cq_ids(&mut v, 7);
        assert_eq!(v, json!({ "data": { "cq_id": "7.abc", "frames": [{ "cq_id": "7.abc" }, { "cq_id": "7.abc" }] } }));
    }
}

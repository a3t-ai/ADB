//! R1/R2 spike: a 3-node openraft 0.10.0-alpha.36 replica set in one process.
//!
//! Each node's log store appends to its own WAL file through a dedicated OS thread
//! that group-writes + `fdatasync`s and only then fires openraft's `IOFlushed`
//! callbacks (the async-flush contract). An extra delay can be injected into one
//! node's flush to test R2: if the leader's fsync runs in parallel with replication,
//! slowing only the leader's fsync must not slow commits (two followers are a quorum).
//!
//!   raftproto <data-dir> <seconds> <clients> <payload-bytes> <scenario>
//!   scenarios: normal | slow-leader | slow-one-follower | slow-both-followers
//!   env: EXTRA_FLUSH_MS (default 20), NET_DELAY_US (default 0, one way)

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::{self, Cursor, Write};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{Stream, TryStreamExt};
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse};
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{Config, EntryPayload, OptionalSend, Raft};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Req {
    pub partition: u32,
    pub payload: Vec<u8>,
    pub op: Op,
}

/// ADB-shaped log records (R7): data batches, owners' durable-offset checkpoints,
/// and ownership claims. The replicated state machine is only the partition map.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Data { offset: u64 },
    Applied { offset: u64 },
    Claim { epoch: u64, node: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Part {
    pub owner: u64,
    pub epoch: u64,
    pub last_offset: u64,
    /// Durable applied offset reported by the owner (in ADB: `_meta` in Lance).
    pub applied: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Resp(pub u64);

impl std::fmt::Display for Req {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Req(p{}, {}B)", self.partition, self.payload.len())
    }
}
impl std::fmt::Display for Resp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Resp({})", self.0)
    }
}

openraft::declare_raft_types!(
    pub TC:
        D = Req,
        R = Resp,
        Node = (),
);

type NodeId = u64;
type R = Raft<TC, Arc<Sm>>;

// ---------------- WAL thread ----------------

struct FlushReq {
    bytes: Vec<u8>,
    cb: IOFlushed<TC>,
}

/// One OS thread per node: drain everything queued, write, fdatasync once, then ack all.
fn spawn_wal(path: std::path::PathBuf, extra_delay_us: Arc<AtomicU64>, fsyncs: Arc<AtomicU64>) -> mpsc::Sender<FlushReq> {
    let (tx, rx) = mpsc::channel::<FlushReq>();
    std::thread::spawn(move || {
        let mut f = OpenOptions::new().create(true).append(true).open(&path).unwrap();
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            while let Ok(more) = rx.try_recv() {
                batch.push(more);
            }
            for r in &batch {
                f.write_all(&r.bytes).unwrap();
            }
            let res = f.sync_data();
            fsyncs.fetch_add(1, Ordering::Relaxed);
            let d = extra_delay_us.load(Ordering::Relaxed);
            if d > 0 {
                std::thread::sleep(Duration::from_micros(d));
            }
            for r in batch {
                r.cb.io_completed(res.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string())));
            }
        }
    });
    tx
}

// ---------------- log store ----------------

#[derive(Default)]
struct LogInner {
    log: BTreeMap<u64, EntryOf<TC>>,
    vote: Option<VoteOf<TC>>,
    committed: Option<LogIdOf<TC>>,
    purged: Option<LogIdOf<TC>>,
}

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
    wal: mpsc::Sender<FlushReq>,
}

impl RaftLogReader<TC> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().log.range(range).map(|(_, e)| e.clone()).collect())
    }
    async fn read_vote(&mut self) -> Result<Option<VoteOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().vote)
    }
}

impl RaftLogStorage<TC> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TC>, io::Error> {
        let i = self.inner.lock().unwrap();
        let last = i.log.values().next_back().map(|e| e.log_id()).or(i.purged);
        Ok(LogState { last_purged_log_id: i.purged, last_log_id: last })
    }
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &VoteOf<TC>) -> Result<(), io::Error> {
        // Votes are rare; a production store fsyncs them. Kept in memory here.
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }
    async fn save_committed(&mut self, c: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        self.inner.lock().unwrap().committed = c;
        Ok(())
    }
    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().committed)
    }
    /// Entries become readable immediately (so replication can start); the callback
    /// fires only after the WAL thread has fsynced them.
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TC>) -> Result<(), io::Error>
    where I: IntoIterator<Item = EntryOf<TC>> + OptionalSend {
        let mut bytes = Vec::new();
        {
            let mut i = self.inner.lock().unwrap();
            for e in entries {
                serde_json::to_writer(&mut bytes, &e).map_err(io::Error::other)?;
                bytes.push(b'\n');
                i.log.insert(e.index(), e);
            }
        }
        self.wal.send(FlushReq { bytes, cb: callback }).map_err(io::Error::other)?;
        Ok(())
    }
    async fn truncate_after(&mut self, last: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        let start = last.map(|l| l.index() + 1).unwrap_or(0);
        self.inner.lock().unwrap().log.retain(|k, _| *k < start);
        Ok(())
    }
    async fn purge(&mut self, log_id: LogIdOf<TC>) -> Result<(), io::Error> {
        let mut i = self.inner.lock().unwrap();
        i.log.retain(|k, _| *k > log_id.index());
        i.purged = Some(log_id);
        Ok(())
    }
}

// ---------------- state machine ----------------

#[derive(Default)]
pub struct Sm {
    parts: Mutex<BTreeMap<u32, Part>>,
    last_applied: Mutex<Option<LogIdOf<TC>>>,
    membership: Mutex<StoredMembershipOf<TC>>,
    snapshot: Mutex<Option<(SnapshotMetaOf<TC>, Vec<u8>)>>,
}

impl RaftSnapshotBuilder<TC> for Arc<Sm> {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TC, Cursor<Vec<u8>>>, io::Error> {
        let meta = SnapshotMetaOf::<TC> {
            last_log_id: *self.last_applied.lock().unwrap(),
            last_membership: self.membership.lock().unwrap().clone(),
        };
        // The snapshot is only the partition map — a pointer; table data never travels.
        let data = serde_json::to_vec(&*self.parts.lock().unwrap()).map_err(io::Error::other)?;
        *self.snapshot.lock().unwrap() = Some((meta.clone(), data.clone()));
        Ok(SnapshotOf::<TC, Cursor<Vec<u8>>> { meta, snapshot: Cursor::new(data) })
    }
}

impl RaftStateMachine<TC> for Arc<Sm> {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogIdOf<TC>>, StoredMembershipOf<TC>), io::Error> {
        Ok((*self.last_applied.lock().unwrap(), self.membership.lock().unwrap().clone()))
    }
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<TC>, io::Error>> + Unpin + OptionalSend {
        while let Some((entry, responder)) = entries.try_next().await? {
            *self.last_applied.lock().unwrap() = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Membership(ref m) => {
                    *self.membership.lock().unwrap() = StoredMembershipOf::<TC>::new(Some(entry.log_id), m.clone());
                }
                EntryPayload::Normal(ref req) => {
                    let mut parts = self.parts.lock().unwrap();
                    let p = parts.entry(req.partition).or_default();
                    match req.op {
                        Op::Data { offset } => p.last_offset = p.last_offset.max(offset),
                        Op::Applied { offset } => p.applied = p.applied.max(offset),
                        Op::Claim { epoch, node } => {
                            if epoch > p.epoch {
                                p.epoch = epoch;
                                p.owner = node;
                            }
                        }
                    }
                }
                _ => {}
            }
            if let Some(r) = responder {
                r.send(Resp(entry.log_id.index()));
            }
        }
        Ok(())
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
    async fn install_snapshot(&mut self, meta: &SnapshotMetaOf<TC>, snapshot: Self::SnapshotData) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        *self.parts.lock().unwrap() = serde_json::from_slice(&data).map_err(io::Error::other)?;
        *self.last_applied.lock().unwrap() = meta.last_log_id;
        *self.membership.lock().unwrap() = meta.last_membership.clone();
        *self.snapshot.lock().unwrap() = Some((meta.clone(), data));
        Ok(())
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TC, Self::SnapshotData>>, io::Error> {
        Ok(self
            .snapshot
            .lock()
            .unwrap()
            .clone()
            .map(|(meta, d)| SnapshotOf::<TC, Self::SnapshotData> { meta, snapshot: Cursor::new(d) }))
    }
}

// ---------------- in-process network ----------------

#[derive(Clone, Default)]
pub struct Router {
    nodes: Arc<Mutex<HashMap<NodeId, R>>>,
    delay_us: Arc<AtomicU64>,
}

impl Router {
    fn get(&self, id: NodeId) -> Option<R> {
        self.nodes.lock().unwrap().get(&id).cloned()
    }
    async fn hop(&self) {
        let d = self.delay_us.load(Ordering::Relaxed);
        if d > 0 {
            tokio::time::sleep(Duration::from_micros(d)).await;
        }
    }
}

pub struct Conn {
    router: Router,
    target: NodeId,
}

fn unreachable<E: std::error::Error + 'static>(e: E) -> RPCError<TC> {
    RPCError::Unreachable(Unreachable::new(&e))
}

impl RaftNetworkFactory<TC> for Router {
    type Network = Conn;
    async fn new_client(&mut self, target: NodeId, _node: &()) -> Self::Network {
        Conn { router: self.clone(), target }
    }
}

impl RaftNetworkV2<TC> for Conn {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(&mut self, rpc: AppendEntriesRequest<TC>, _o: RPCOption) -> Result<AppendEntriesResponse<TC>, RPCError<TC>> {
        let t = self.router.get(self.target).ok_or_else(|| unreachable(io::Error::other("node down")))?;
        self.router.hop().await;
        let r = t.append_entries(rpc).await.map_err(|e| RPCError::Network(NetworkError::new(&e)));
        self.router.hop().await;
        r
    }
    async fn transfer_leader(&mut self, req: TransferLeaderRequest<TC>, _o: RPCOption) -> Result<TransferLeaderResponse<TC>, RPCError<TC>> {
        let t = self.router.get(self.target).ok_or_else(|| unreachable(io::Error::other("node down")))?;
        t.handle_transfer_leader(req).await.map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
    async fn vote(&mut self, rpc: VoteRequest<TC>, _o: RPCOption) -> Result<VoteResponse<TC>, RPCError<TC>> {
        let t = self.router.get(self.target).ok_or_else(|| unreachable(io::Error::other("node down")))?;
        self.router.hop().await;
        t.vote(rpc).await.map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TC>,
        snapshot: SnapshotOf<TC, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _o: RPCOption,
    ) -> Result<SnapshotResponse<TC>, StreamingError<TC>> {
        let t = self
            .router
            .get(self.target)
            .ok_or_else(|| StreamingError::Unreachable(Unreachable::new(&io::Error::other("node down"))))?;
        t.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| StreamingError::Network(NetworkError::new(&e)))
    }
}

// ---------------- harness ----------------

struct Node {
    raft: R,
    log: LogStore,
    sm: Arc<Sm>,
    extra_flush_us: Arc<AtomicU64>,
    fsyncs: Arc<AtomicU64>,
}

fn default_cfg() -> Config {
    Config { heartbeat_interval: 50, election_timeout_min: 300, election_timeout_max: 600, ..Default::default() }
}

/// Start one node with an empty log and state machine and register it with the router.
async fn make_node(dir: &str, router: &Router, id: NodeId, cfg: Arc<Config>) -> Node {
    let extra = Arc::new(AtomicU64::new(0));
    let fsyncs = Arc::new(AtomicU64::new(0));
    let path = std::path::Path::new(dir).join(format!("node{id}.wal"));
    let _ = std::fs::remove_file(&path);
    let wal = spawn_wal(path, extra.clone(), fsyncs.clone());
    let log = LogStore { inner: Arc::new(Mutex::new(LogInner::default())), wal };
    let sm = Arc::new(Sm::default());
    let raft = Raft::new(id, cfg, router.clone(), log.clone(), sm.clone()).await.unwrap();
    router.nodes.lock().unwrap().insert(id, raft.clone());
    SMS.lock().unwrap().insert(id, sm.clone());
    Node { raft, log, sm, extra_flush_us: extra, fsyncs }
}

async fn start(dir: &str, router: &Router, ids: &[NodeId]) -> HashMap<NodeId, Node> {
    start_with(dir, router, ids, default_cfg()).await
}

async fn start_with(dir: &str, router: &Router, ids: &[NodeId], cfg: Config) -> HashMap<NodeId, Node> {
    let cfg = Arc::new(cfg.validate().unwrap());
    let mut nodes = HashMap::new();
    for &id in ids {
        nodes.insert(id, make_node(dir, router, id, cfg.clone()).await);
    }
    nodes
}

async fn leader(nodes: &HashMap<NodeId, Node>) -> NodeId {
    loop {
        for n in nodes.values() {
            if let Some(l) = n.raft.metrics().borrow_watched().current_leader {
                return l;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn pct(v: &mut [Duration], q: f64) -> Duration {
    v[((v.len() as f64 * q) as usize).min(v.len() - 1)]
}

// ---------------- R7: snapshot as a partition map; wiped follower rejoins ----------------

fn first_index(log: &LogStore) -> Option<u64> {
    log.inner.lock().unwrap().log.keys().next().copied()
}
fn has(log: &LogStore, idx: u64) -> bool {
    log.inner.lock().unwrap().log.contains_key(&idx)
}

async fn wait_until<F: Fn() -> bool>(what: &str, f: F) {
    let t = Instant::now();
    while !f() {
        if t.elapsed() > Duration::from_secs(20) {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// mode = "default": openraft purges right after a snapshot (max_in_snapshot_log_to_keep = 0).
/// mode = "guarded": openraft never auto-purges; ADB purges only below the lowest durable
/// applied offset across partitions (§11.3).
async fn r7(dir: &str, mode: &str) {
    std::fs::create_dir_all(dir).unwrap();
    let mut cfg = default_cfg();
    cfg.snapshot_policy = openraft::SnapshotPolicy::Never;
    cfg.purge_batch_size = 1;
    cfg.max_in_snapshot_log_to_keep = if mode == "guarded" { u64::MAX / 4 } else { 0 };
    let router = Router::default();
    let mut nodes = start_with(dir, &router, &[1, 2, 3], cfg.clone()).await;
    nodes[&1].raft.initialize([1u64, 2, 3].into_iter().map(|i| (i, ())).collect::<BTreeMap<_, _>>()).await.unwrap();
    let l = leader(&nodes).await;
    let w = |partition: u32, op: Op| {
        let raft = nodes[&l].raft.clone();
        async move { raft.client_write(Req { partition, payload: vec![], op }).await.unwrap().log_id.index() }
    };

    // Three partitions owned by nodes 1..3. Partition 2's applier is stuck at offset 50.
    for p in 0..3u32 {
        w(p, Op::Claim { epoch: 1, node: p as u64 + 1 }).await;
    }
    let mut idx = HashMap::new();
    for o in 1..=600u64 {
        for p in 0..3u32 {
            idx.insert((p, o), w(p, Op::Data { offset: o }).await);
        }
        if o % 100 == 0 {
            w(0, Op::Applied { offset: o }).await;
            w(1, Op::Applied { offset: o }).await;
            if o == 100 {
                w(2, Op::Applied { offset: 50 }).await;
            }
        }
    }
    let needed = idx[&(2, 51)]; // first log entry partition 2 still has to apply
    let last = nodes[&l].raft.metrics().borrow_watched().last_log_index.unwrap();

    // Snapshot on the leader.
    nodes[&l].raft.trigger().snapshot().await.unwrap();
    {
        let r = nodes[&l].raft.clone();
        wait_until("leader snapshot", || r.metrics().borrow_watched().snapshot.map(|s| s.index()) >= Some(last)).await;
    }
    if mode == "guarded" {
        // ADB's rule: purge only below the lowest durable applied offset across partitions.
        let parts = nodes[&l].sm.parts.lock().unwrap().clone();
        let upto = parts.iter().map(|(p, s)| idx.get(&(*p, s.applied)).copied().unwrap_or(0)).min().unwrap();
        nodes[&l].raft.trigger().purge_log(upto).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let snap_bytes = nodes[&l].sm.snapshot.lock().unwrap().as_ref().map(|s| s.1.len()).unwrap_or(0);
    println!(
        "r7 [{mode}]: {last} log entries; partition 2 applied=50 → still needs log index {needed}..; leader snapshot at {:?} ({snap_bytes} bytes); leader log now starts at {:?}",
        nodes[&l].raft.metrics().borrow_watched().snapshot.map(|s| s.index()),
        first_index(&nodes[&l].log)
    );

    // The follower owning partition 2 loses its disk. A wiped node must NOT rejoin under
    // its old id (it would vote with no log); it comes back as a NEW node through a
    // membership change: add as learner (catches up), then replace the old voter.
    let old = if l == 3 { 2 } else { 3 };
    nodes[&old].raft.shutdown().await.unwrap();
    router.nodes.lock().unwrap().remove(&old);
    let victim: NodeId = 4;
    let fresh = make_node(dir, &router, victim, Arc::new(cfg.clone().validate().unwrap())).await;
    nodes.insert(victim, fresh);
    let t = Instant::now();
    nodes[&l].raft.add_learner(victim, (), true).await.unwrap();
    let caught_up = t.elapsed();
    let voters: std::collections::BTreeSet<NodeId> = [1u64, 2, 3].into_iter().filter(|x| *x != old).chain([victim]).collect();
    nodes[&l].raft.change_membership(voters.clone(), false).await.unwrap();
    println!(
        "  node {old} wiped; node {victim} added as learner and caught up in {caught_up:.2?}; membership now {voters:?} after {:.2?}",
        t.elapsed()
    );
    let got_snapshot = nodes[&victim].sm.snapshot.lock().unwrap().is_some();
    let same_map = *nodes[&victim].sm.parts.lock().unwrap() == *nodes[&l].sm.parts.lock().unwrap();
    println!(
        "  wiped node {victim} rejoined: installed snapshot={got_snapshot}, partition map identical to leader={same_map}, its log starts at {:?}",
        first_index(&nodes[&victim].log)
    );
    for (name, id) in [("leader", l), ("rejoined node", victim)] {
        println!("  {name:<14} (node {id}) has log entry {needed} needed by partition 2: {}", has(&nodes[&id].log, needed));
    }
    let other = [1, 2, 3].into_iter().find(|x| *x != l && *x != old).unwrap();
    println!("  other follower (node {other}) has it: {}", has(&nodes[&other].log, needed));
    for n in nodes.values() {
        let _ = n.raft.shutdown().await;
    }
}

// ---------------- R8: membership change under load ----------------

fn current_leader_of(router: &Router, id: NodeId) -> Option<NodeId> {
    router.get(id).and_then(|r| r.metrics().borrow_watched().current_leader)
}

fn current_leader(router: &Router) -> Option<NodeId> {
    let nodes: Vec<R> = router.nodes.lock().unwrap().values().cloned().collect();
    nodes.iter().filter_map(|r| r.metrics().borrow_watched().current_leader).max()
}

/// Replace `old` with a fresh node `new`: learner → catch up → membership change.
async fn replace(dir: &str, router: &Router, cfg: &Arc<Config>, old: NodeId, new: NodeId, voters: &mut std::collections::BTreeSet<NodeId>) -> Duration {
    let t = Instant::now();
    let fresh = make_node(dir, router, new, cfg.clone()).await;
    loop {
        let Some(l) = current_leader(router) else { continue };
        let lr = router.get(l).unwrap();
        if lr.add_learner(new, (), true).await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let next: std::collections::BTreeSet<NodeId> = voters.iter().copied().filter(|x| *x != old).chain([new]).collect();
        match lr.change_membership(next.clone(), false).await {
            Ok(_) => {
                *voters = next;
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    // The removed node shuts down once it is out of the membership.
    if let Some(r) = router.get(old) {
        let _ = r.shutdown().await;
    }
    router.nodes.lock().unwrap().remove(&old);
    std::mem::forget(fresh); // keep the new node's handles alive for the rest of the run
    t.elapsed()
}

async fn r8(dir: &str, mode: &str) {
    std::fs::create_dir_all(dir).unwrap();
    let cfg = Arc::new(default_cfg().validate().unwrap());
    let router = Router::default();
    let nodes = start_with(dir, &router, &[1, 2, 3], default_cfg()).await;
    nodes[&1].raft.initialize([1u64, 2, 3].into_iter().map(|i| (i, ())).collect::<BTreeMap<_, _>>()).await.unwrap();
    let _ = leader(&nodes).await;
    let mut voters: std::collections::BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();

    let clients = 32u32;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acked: Arc<Vec<AtomicU64>> = Arc::new((0..clients).map(|_| AtomicU64::new(0)).collect());
    let lat = Arc::new(Mutex::new(Vec::<(Duration, Duration)>::new())); // (time since start, latency)
    let errors = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for p in 0..clients {
        let (router, stop, acked, lat, errors) = (router.clone(), stop.clone(), acked.clone(), lat.clone(), errors.clone());
        tasks.push(tokio::spawn(async move {
            let mut offset = 1u64;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                loop {
                    let r = current_leader(&router).and_then(|l| router.get(l));
                    let Some(r) = r else {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    };
                    match r.client_write(Req { partition: p, payload: vec![0; 256], op: Op::Data { offset } }).await {
                        Ok(_) => break,
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                }
                lat.lock().unwrap().push((t0.elapsed(), t.elapsed()));
                acked[p as usize].store(offset, Ordering::Relaxed);
                offset += 1;
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    let l = current_leader(&router).unwrap();
    let follower = *voters.iter().find(|x| **x != l).unwrap();
    let d1 = replace(dir, &router, &cfg, follower, 4, &mut voters).await;
    println!("r8: t=3s replaced follower {follower} with node 4 in {d1:.2?}; voters {voters:?}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let l = current_leader(&router).unwrap();
    if mode == "transfer" {
        // Hand leadership to another voter first, so removing the old leader needs no election.
        let to = *voters.iter().find(|x| **x != l).unwrap();
        let t = Instant::now();
        router.get(l).unwrap().trigger().transfer_leader(to).await.unwrap();
        while current_leader_of(&router, to) != Some(to) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        println!("r8: leadership transferred {l} → {to} in {:.2?}", t.elapsed());
    }
    let d2 = replace(dir, &router, &cfg, l, 5, &mut voters).await;
    println!("r8: t=6s replaced LEADER {l} with node 5 in {d2:.2?}; voters {voters:?}; new leader {:?}", current_leader(&router));
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }

    let mut l = lat.lock().unwrap().clone();
    let total = l.len();
    // Throughput per 100 ms bucket, and the worst stall.
    let mut buckets = vec![0u64; 100];
    for (at, _) in &l {
        let b = (at.as_millis() / 100) as usize;
        if b < buckets.len() {
            buckets[b] += 1;
        }
    }
    let active: Vec<u64> = buckets.iter().copied().take(89).collect();
    l.sort_by_key(|x| x.1);
    println!(
        "  {total} commits, {} client retries; latency p50={:.2?} p99={:.2?} max(stall)={:.2?}; commits per 100 ms: median {} min {}",
        errors.load(Ordering::Relaxed),
        l[total / 2].1,
        l[total * 99 / 100].1,
        l[total - 1].1,
        { let mut a = active.clone(); a.sort(); a[a.len() / 2] },
        active.iter().min().unwrap()
    );
    // Every acknowledged write must be on every final voter.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut lost = 0;
    let sms = SMS.lock().unwrap().clone();
    for id in &voters {
        let parts = sms[id].parts.lock().unwrap().clone();
        for p in 0..clients {
            if parts.get(&p).map(|x| x.last_offset).unwrap_or(0) < acked[p as usize].load(Ordering::Relaxed) {
                lost += 1;
            }
        }
    }
    println!("  final voters {voters:?}: partitions with an acknowledged write missing = {lost} (of {} checked)", clients as usize * voters.len());
    std::process::exit(0);
}

/// R5: owner → leader forwarding. The leader listens on TCP loopback; an owner that is
/// not the leader sends its (optionally batched) transactions there and waits for the
/// commit. Framing: u32 length + JSON `Vec<Req>`; reply: u64 last log index.
async fn r5(dir: &str, secs: u64) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    std::fs::create_dir_all(dir).unwrap();
    let router = Router::default();
    let nodes = start(dir, &router, &[1, 2, 3]).await;
    nodes[&1].raft.initialize([1u64, 2, 3].into_iter().map(|i| (i, ())).collect::<BTreeMap<_, _>>()).await.unwrap();
    let l = leader(&nodes).await;
    let leader_raft = nodes[&l].raft.clone();

    // Leader-side forwarding endpoint.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let raft = leader_raft.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).unwrap();
                let raft = raft.clone();
                tokio::spawn(async move {
                    let mut len = [0u8; 4];
                    while sock.read_exact(&mut len).await.is_ok() {
                        let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
                        sock.read_exact(&mut buf).await.unwrap();
                        let reqs: Vec<Req> = serde_json::from_slice(&buf).unwrap();
                        // A forwarded batch is proposed as concurrent client writes; Raft
                        // group-commits them. ADB would send one log record per batch.
                        let futs = reqs.into_iter().map(|r| raft.client_write(r));
                        let mut last = 0;
                        for r in futures::future::join_all(futs).await {
                            last = last.max(r.unwrap().log_id.index());
                        }
                        sock.write_all(&last.to_le_bytes()).await.unwrap();
                    }
                });
            }
        });
    }

    let req = |c: u32| Req { partition: c, payload: vec![c as u8; 1024], op: Op::Data { offset: 0 } };
    for (mode, clients, batch) in [
        ("direct", 1u32, 1usize),
        ("forwarded", 1, 1),
        ("direct", 16, 1),
        ("forwarded", 16, 1),
        ("forwarded", 16, 8),
    ] {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut tasks = Vec::new();
        for c in 0..clients {
            let raft = leader_raft.clone();
            tasks.push(tokio::spawn(async move {
                let mut lat = Vec::new();
                let mut txns = 0usize;
                let mut sock = if mode == "forwarded" {
                    let s = tokio::net::TcpStream::connect(addr).await.unwrap();
                    s.set_nodelay(true).unwrap();
                    Some(s)
                } else {
                    None
                };
                while Instant::now() < deadline {
                    let t = Instant::now();
                    match sock.as_mut() {
                        None => {
                            raft.client_write(req(c)).await.unwrap();
                        }
                        Some(s) => {
                            let body = serde_json::to_vec(&(0..batch).map(|_| req(c)).collect::<Vec<_>>()).unwrap();
                            s.write_all(&(body.len() as u32).to_le_bytes()).await.unwrap();
                            s.write_all(&body).await.unwrap();
                            let mut idx = [0u8; 8];
                            s.read_exact(&mut idx).await.unwrap();
                        }
                    }
                    lat.push(t.elapsed());
                    txns += batch;
                }
                (lat, txns)
            }));
        }
        let mut lat = Vec::new();
        let mut txns = 0;
        for t in tasks {
            let (l, n) = t.await.unwrap();
            lat.extend(l);
            txns += n;
        }
        lat.sort();
        println!(
            "r5 {mode:<10} clients={clients:<3} txns/request={batch}: {:.0} txns/s  request p50={:.2?} p90={:.2?} p99={:.2?}",
            txns as f64 / secs as f64,
            pct(&mut lat, 0.5),
            pct(&mut lat, 0.9),
            pct(&mut lat, 0.99)
        );
    }
    std::process::exit(0);
}

/// R6: crash the leader under load (no membership change) and measure the stall.
async fn r6(dir: &str, emin: u64, emax: u64, hb: u64) {
    std::fs::create_dir_all(dir).unwrap();
    let mut cfg = default_cfg();
    cfg.election_timeout_min = emin;
    cfg.election_timeout_max = emax;
    cfg.heartbeat_interval = hb;
    let router = Router::default();
    let nodes = start_with(dir, &router, &[1, 2, 3], cfg).await;
    nodes[&1].raft.initialize([1u64, 2, 3].into_iter().map(|i| (i, ())).collect::<BTreeMap<_, _>>()).await.unwrap();
    let _ = leader(&nodes).await;
    let clients = 32u32;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acked: Arc<Vec<AtomicU64>> = Arc::new((0..clients).map(|_| AtomicU64::new(0)).collect());
    let lat = Arc::new(Mutex::new(Vec::<(Duration, Duration)>::new()));
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for p in 0..clients {
        let (router, stop, acked, lat) = (router.clone(), stop.clone(), acked.clone(), lat.clone());
        tasks.push(tokio::spawn(async move {
            let mut offset = 1u64;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                loop {
                    let Some(r) = current_leader(&router).and_then(|l| router.get(l)) else {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        continue;
                    };
                    match tokio::time::timeout(Duration::from_millis(500), r.client_write(Req { partition: p, payload: vec![0; 256], op: Op::Data { offset } })).await {
                        Ok(Ok(_)) => break,
                        _ => tokio::time::sleep(Duration::from_millis(2)).await,
                    }
                }
                lat.lock().unwrap().push((t0.elapsed(), t.elapsed()));
                acked[p as usize].store(offset, Ordering::Relaxed);
                offset += 1;
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let old = current_leader(&router).unwrap();
    let crash_at = t0.elapsed();
    let _ = router.get(old).unwrap().shutdown().await; // crash: no step-down, no transfer
    router.nodes.lock().unwrap().remove(&old);
    let mut new_leader = None;
    let te = Instant::now();
    while new_leader.is_none() || new_leader == Some(old) {
        new_leader = [1u64, 2, 3].into_iter().filter(|x| *x != old).find_map(|id| {
            let l = current_leader_of(&router, id)?;
            (l != old).then_some(l)
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elected = te.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }
    // Stall = longest gap with no commit completing, around the crash.
    let mut done: Vec<Duration> = lat.lock().unwrap().iter().map(|(at, _)| *at).filter(|at| *at > crash_at - Duration::from_millis(50)).collect();
    done.sort();
    let gap = done.windows(2).map(|w| w[1] - w[0]).max().unwrap_or_default();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let sms = SMS.lock().unwrap().clone();
    let mut lost = 0;
    for id in [1u64, 2, 3].into_iter().filter(|x| *x != old) {
        let parts = sms[&id].parts.lock().unwrap().clone();
        for p in 0..clients {
            if parts.get(&p).map(|x| x.last_offset).unwrap_or(0) < acked[p as usize].load(Ordering::Relaxed) {
                lost += 1;
            }
        }
    }
    println!(
        "r6 election {emin}-{emax} ms, heartbeat {hb} ms: leader {old} crashed; new leader {new_leader:?} elected after {} ms; \
         longest commit gap {} ms; acknowledged writes missing on survivors = {lost}", elected.as_millis(), gap.as_millis()
    );
    std::process::exit(0);
}

/// Every node's state machine by id (R8 reads the final partition maps).
static SMS: std::sync::LazyLock<Mutex<HashMap<NodeId, Arc<Sm>>>> = std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.get(1).map(String::as_str) == Some("r7") {
        r7(&a[2], &a[3]).await;
        std::process::exit(0);
    }
    if a.get(1).map(String::as_str) == Some("r5") {
        r5(&a[2], a.get(3).map(|x| x.parse().unwrap()).unwrap_or(8)).await;
    }
    if a.get(1).map(String::as_str) == Some("r6") {
        let n = |i: usize| a[i].parse::<u64>().unwrap();
        r6(&a[2], n(3), n(4), n(5)).await;
    }
    if a.get(1).map(String::as_str) == Some("r8") {
        r8(&a[2], a.get(3).map(String::as_str).unwrap_or("plain")).await;
    }
    let (dir, secs, clients, payload, scenario) =
        (&a[1], a[2].parse::<u64>().unwrap(), a[3].parse::<usize>().unwrap(), a[4].parse::<usize>().unwrap(), a[5].as_str());
    std::fs::create_dir_all(dir).unwrap();
    let extra_ms: u64 = std::env::var("EXTRA_FLUSH_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let router = Router::default();
    router.delay_us.store(std::env::var("NET_DELAY_US").ok().and_then(|v| v.parse().ok()).unwrap_or(0), Ordering::Relaxed);

    let nodes = start(dir, &router, &[1, 2, 3]).await;
    nodes[&1].raft.initialize([1u64, 2, 3].into_iter().map(|i| (i, ())).collect::<BTreeMap<_, _>>()).await.unwrap();
    let l = leader(&nodes).await;
    let followers: Vec<NodeId> = [1, 2, 3].into_iter().filter(|x| *x != l).collect();
    let slow: Vec<NodeId> = match scenario {
        "normal" => vec![],
        "slow-leader" => vec![l],
        "slow-one-follower" => vec![followers[0]],
        "slow-both-followers" => followers.clone(),
        s => panic!("unknown scenario {s}"),
    };
    for id in &slow {
        nodes[id].extra_flush_us.store(extra_ms * 1000, Ordering::Relaxed);
    }

    // Warm up.
    let raft = nodes[&l].raft.clone();
    for _ in 0..50 {
        raft.client_write(Req { partition: 0, payload: vec![7; payload], op: Op::Data { offset: 0 } }).await.unwrap();
    }
    let f0: Vec<u64> = [1, 2, 3].iter().map(|i| nodes[i].fsyncs.load(Ordering::Relaxed)).collect();

    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for c in 0..clients {
        let raft = raft.clone();
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            while Instant::now() < deadline {
                let t = Instant::now();
                raft.client_write(Req { partition: c as u32, payload: vec![c as u8; payload], op: Op::Data { offset: 0 } }).await.unwrap();
                lat.push(t.elapsed());
            }
            lat
        }));
    }
    let mut lat = Vec::new();
    for t in tasks {
        lat.extend(t.await.unwrap());
    }
    lat.sort();
    let fs: Vec<u64> = [1, 2, 3].iter().zip(&f0).map(|(i, b)| nodes[i].fsyncs.load(Ordering::Relaxed) - b).collect();
    println!(
        "{scenario:<20} clients={clients:<3} payload={payload}B extra={}ms net={}us: {} commits ({:.0}/s)  p50={:.2?} p90={:.2?} p99={:.2?} max={:.2?}  fsyncs/node={fs:?} (leader={l})",
        if slow.is_empty() { 0 } else { extra_ms },
        router.delay_us.load(Ordering::Relaxed),
        lat.len(),
        lat.len() as f64 / secs as f64,
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.9),
        pct(&mut lat, 0.99),
        lat[lat.len() - 1]
    );
    for n in nodes.values() {
        let _ = n.raft.shutdown().await;
    }
    std::process::exit(0);
}

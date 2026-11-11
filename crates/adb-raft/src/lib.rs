//! ADB replica sets (M3, `.ai/M3_RAFT.md`): one Raft group of 3 nodes over the
//! single partition's log.
//!
//! - [`store::LogStore`] — openraft's own log and vote, durable in `<dir>/raft`.
//! - [`sm::StateMachine`] — appends each committed ADB [`LogRecord`] to the node's
//!   WAL (`<dir>/p0`), so every reader of the WAL works as on one node.
//! - [`net`] — Raft RPCs between nodes over HTTP (MessagePack bodies).
//! - [`RaftLog`] — the `adb_log::Log` the leader's sequencer appends through:
//!   an append returns once the record is committed and in the local WAL.
//! - [`Node`] — starts all of it and tells who leads.

pub mod net;
pub mod sm;
pub mod store;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use adb_core::log::{LogOffset, LogRecord, PartitionId};
use adb_log::{LocalWal, Log, LogError, LogResult};
use async_trait::async_trait;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{BasicNode, Config, Raft, ReadPolicy, SnapshotPolicy};
use serde::{Deserialize, Serialize};

/// One Raft entry: an ADB log record, and whether it starts a new WAL segment
/// (W5 erasure).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cmd {
    pub record: LogRecord,
    #[serde(default)]
    pub new_segment: bool,
    /// Filler that keeps a redacted entry the length it had on disk (see
    /// [`store::LogStore::redact`]). Empty, and then not written, on every new entry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pad: Vec<u8>,
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cmd(offset {})", self.record.offset.0)
    }
}

/// The answer to a committed [`Cmd`]: its offset in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Applied(pub u64);

impl fmt::Display for Applied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Applied({})", self.0)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Cmd,
        R = Applied,
        Node = BasicNode,
);

pub type NodeId = u64;
pub type AdbRaft = Raft<TypeConfig, Arc<sm::StateMachine>>;

/// This node and its peers.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: NodeId,
    /// Every voter (this node included): id → the base URL of its Raft endpoint.
    pub peers: BTreeMap<NodeId, String>,
}

impl ClusterConfig {
    /// `ADB_NODE_ID` and `ADB_PEERS` (`1=http://adb-0.adb:3002,2=…`), or `None`
    /// when `ADB_NODE_ID` is unset (a single node).
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(id) = std::env::var("ADB_NODE_ID").ok().filter(|v| !v.is_empty()) else { return Ok(None) };
        // `from-hostname`: a StatefulSet pod `adb-<n>` is node n + 1.
        let id = if id == "from-hostname" {
            let host = std::env::var("HOSTNAME").unwrap_or_default();
            let ordinal: NodeId = host.rsplit('-').next().and_then(|n| n.parse().ok()).ok_or_else(|| format!("ADB_NODE_ID=from-hostname, but HOSTNAME {host:?} does not end in -<ordinal>"))?;
            (ordinal + 1).to_string()
        } else {
            id
        };
        let node_id: NodeId = id.parse().map_err(|_| format!("ADB_NODE_ID {id:?} is not a number"))?;
        let peers = std::env::var("ADB_PEERS").map_err(|_| "ADB_NODE_ID is set but ADB_PEERS is not".to_string())?;
        Ok(Some(Self { node_id, peers: parse_peers(&peers)? }))
    }
}

/// `1=http://a:3002,2=http://b:3002`.
pub fn parse_peers(s: &str) -> Result<BTreeMap<NodeId, String>, String> {
    let mut out = BTreeMap::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (id, url) = part.split_once('=').ok_or_else(|| format!("ADB_PEERS entry {part:?} is not id=url"))?;
        let id: NodeId = id.trim().parse().map_err(|_| format!("ADB_PEERS id {id:?} is not a number"))?;
        out.insert(id, url.trim().trim_end_matches('/').to_string());
    }
    if out.is_empty() {
        return Err("ADB_PEERS names no node".into());
    }
    Ok(out)
}

/// Heartbeat 50 ms, election 500–1000 ms, Pre-Vote on. R6 measured 150–300 ms /
/// 30 ms on one host; on Kubernetes that let one CPU-heavy request on the leader
/// (or a slow node) look like a dead leader. Pre-Vote: a node that cannot win (one
/// restarted with a new pod IP that the leader does not reach yet) does not raise
/// its term, so it cannot depose a healthy leader when it is reached again. A
/// snapshot (a pointer: the data is the WAL and the tables) every 50,000 entries,
/// so openraft can trim its own log. Every value can be overridden by env.
pub fn raft_config() -> Config {
    let ms = |var: &str, d: u64| std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    Config {
        cluster_name: "adb".into(),
        heartbeat_interval: ms("ADB_RAFT_HEARTBEAT_MS", 50),
        election_timeout_min: ms("ADB_RAFT_ELECTION_MIN_MS", 500),
        election_timeout_max: ms("ADB_RAFT_ELECTION_MAX_MS", 1000),
        snapshot_policy: SnapshotPolicy::LogsSinceLast(ms("ADB_RAFT_SNAPSHOT_EVERY", 50_000)),
        max_in_snapshot_log_to_keep: ms("ADB_RAFT_LOG_KEEP", 20_000),
        enable_pre_vote: Some(std::env::var("ADB_RAFT_PRE_VOTE").map_or(true, |v| v != "0" && v != "false")),
        ..Default::default()
    }
}

/// A started Raft node.
pub struct Node {
    pub id: NodeId,
    pub raft: AdbRaft,
    pub wal: Arc<LocalWal>,
    pub config: ClusterConfig,
    /// openraft's log, shared with Raft: erasure rewrites it (see [`RaftLog`]).
    store: store::LogStore,
    /// Strict follower reads (C-16, PF-46): reads queue here; one task asks the leader
    /// for its read index, once for everything queued, and answers them all.
    read_index_q: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<Result<u64, String>>>,
}

/// The read-index batcher (PF-46): takes every read queued, makes one ReadIndex call to
/// the leader, answers them all, and goes on. A read that arrives during a call waits
/// for the next one, which starts after it arrived, so each still sees every write
/// acknowledged before it arrived. Nothing is locked across the round trip (etcd and
/// TiKV batch the same way). Before: one mutex, held per read; ~430 reads/s per
/// follower, 50-190 ms waits, CPU idle (AWS).
fn spawn_read_index_batcher(
    id: NodeId,
    raft: AdbRaft,
    peers: std::collections::BTreeMap<NodeId, String>,
    http: reqwest::Client,
) -> tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<Result<u64, String>>> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<Result<u64, String>>>();
    tokio::spawn(async move {
        let mut waiting = Vec::new();
        while rx.recv_many(&mut waiting, 65_536).await > 0 {
            adb_core::steps::value("follower.read index requests per call", waiting.len() as u64);
            let t = std::time::Instant::now();
            let leader = raft.metrics().borrow_watched().current_leader;
            let r = match leader {
                Some(l) if l == id => net::leader_read_index(&raft).await,
                Some(l) => match peers.get(&l) {
                    Some(url) => ask_read_index(&http, l, url).await,
                    None => Err(format!("leader {l} is not in ADB_PEERS")),
                },
                None => Err("no leader".to_string()),
            };
            adb_core::steps::add("follower.read.1c read index call", t.elapsed());
            for w in waiting.drain(..) {
                let _ = w.send(r.clone());
            }
        }
    });
    tx
}

async fn ask_read_index(http: &reqwest::Client, leader: NodeId, url: &str) -> Result<u64, String> {
    let r = http.get(format!("{url}/raft/read_index")).send().await.map_err(|e| format!("leader {leader}: {e}"))?;
    if !r.status().is_success() {
        return Err(format!("leader {leader}: {}", r.text().await.unwrap_or_default()));
    }
    r.json::<u64>().await.map_err(|e| format!("leader {leader}: {e}"))
}

impl Node {
    /// Open the Raft log in `dir/raft` and the state machine over `wal`, start
    /// Raft, and on the lowest id with an empty log, initialize the group with
    /// every peer as a voter. The Raft RPC endpoint is served separately
    /// ([`net::router`]).
    pub async fn start(dir: &std::path::Path, wal: Arc<LocalWal>, config: ClusterConfig) -> Result<Self, String> {
        let log = store::LogStore::open(dir.join("raft")).map_err(|e| format!("raft log: {e}"))?;
        let empty = log.is_empty();
        let sm = Arc::new(sm::StateMachine::open(dir.join("raft"), Arc::clone(&wal)).map_err(|e| format!("raft state: {e}"))?);
        let cfg = Arc::new(raft_config().validate().map_err(|e| e.to_string())?);
        let store = log.clone();
        let raft = Raft::new(config.node_id, cfg, net::Network::new(), log, sm).await.map_err(|e| e.to_string())?;
        // openraft's own stage latencies and batch sizes every 10 s (ADB_STEP_STATS=1).
        if adb_core::steps::on() {
            let r = raft.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    match r.runtime_stats().await {
                        Ok(mut st) => {
                            st.build_log_stage_histograms();
                            tracing::info!(target: "adb::steps", "openraft runtime stats\n{}", st.display().multiline().human_readable());
                        }
                        Err(_) => return,
                    }
                }
            });
        }
        let lowest = config.peers.keys().next().copied();
        if empty && lowest == Some(config.node_id) {
            let members: BTreeMap<NodeId, BasicNode> = config.peers.iter().map(|(id, url)| (*id, BasicNode { addr: url.clone() })).collect();
            match raft.initialize(members).await {
                Ok(()) => tracing::info!(node = config.node_id, peers = ?config.peers, "raft group initialized"),
                Err(e) => tracing::info!(node = config.node_id, error = %e, "raft group not initialized here"),
            }
        }
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(500))
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .map_err(|e| e.to_string())?;
        let read_index_q = spawn_read_index_batcher(config.node_id, raft.clone(), config.peers.clone(), http);
        Ok(Self { id: config.node_id, raft, wal, config, store, read_index_q })
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow_watched().current_leader
    }

    pub fn is_leader(&self) -> bool {
        self.leader() == Some(self.id)
    }

    /// Resolves once this node leads (`lead`), or once it no longer does.
    pub async fn until_lead(&self, lead: bool) {
        let mut m = self.raft.metrics();
        loop {
            if self.is_leader() == lead {
                return;
            }
            if m.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Wait until this node leads and its state machine has applied everything
    /// committed before it took over (so its WAL is complete). `false` if it
    /// lost the lead meanwhile.
    pub async fn ready_to_lead(&self) -> bool {
        if !(self.is_leader() && self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await.is_ok()) {
            return false;
        }
        // PF-44: the state machine answers before the WAL's sync, so records it was
        // handed may still be queued in the WAL writer: readable only once written. The
        // engine opens from the WAL, so it waits for them (else a takeover would open
        // at the queued end and never apply what was still in flight).
        let t = std::time::Instant::now();
        while self.wal.durable_offset() < self.wal.last_offset() {
            if t.elapsed() > std::time::Duration::from_secs(30) {
                tracing::error!(durable = self.wal.durable_offset().0, last = self.wal.last_offset().0, "the WAL did not sync its queued records within 30 s; not taking the lead");
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        adb_core::steps::add("raft.takeover WAL settle wait", t.elapsed());
        self.is_leader()
    }

    /// A leader that is about to stop (SIGTERM: a roll, a drain) hands the lead to
    /// the most caught-up follower, so the replica set does not wait out an election
    /// timeout. True once another node leads; false if none took over within `within`.
    pub async fn hand_over(&self, within: std::time::Duration) -> bool {
        if !self.is_leader() {
            return true;
        }
        let m = self.raft.metrics().borrow_watched().clone();
        let target = m
            .replication
            .as_ref()
            .and_then(|r| r.iter().filter(|(id, _)| **id != self.id).max_by_key(|(_, l)| l.as_ref().map(|l| l.index)).map(|(id, _)| *id));
        let Some(to) = target else { return false };
        if let Err(e) = self.raft.trigger().transfer_leader(to).await {
            tracing::warn!(error = %e, to, "leader transfer not started");
            return false;
        }
        tracing::info!(node = self.id, to, "handing the lead over before stopping");
        tokio::time::timeout(within, self.until_lead(false)).await.is_ok()
    }

    /// The log index a linearizable read must see (C-16): the leader's ReadIndex, asked
    /// of the leader over its Raft endpoint through the batcher (PF-46).
    pub async fn read_index(&self) -> Result<u64, String> {
        let t = std::time::Instant::now();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.read_index_q.send(tx).map_err(|_| "the read-index batcher stopped".to_string())?;
        let r = rx.await.map_err(|_| "the read-index batcher stopped".to_string())?;
        adb_core::steps::add("follower.read.1 read index (queue + call)", t.elapsed());
        r
    }

    /// Wait until this node's state machine applied log index `index` (its WAL then
    /// holds every record up to it).
    pub async fn wait_applied(&self, index: u64, within: std::time::Duration) -> Result<(), String> {
        if let Some(a) = self.raft.metrics().borrow_watched().last_applied.as_ref() {
            adb_core::steps::value("follower.read raft entries behind", index.saturating_sub(a.index));
        }
        self.raft.wait(Some(within)).applied_index_at_least(Some(index), "follower read").await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// The leader's address on `port` (its HTTP or Flight port; same host as its
    /// Raft endpoint in ADB_PEERS).
    pub fn leader_at(&self, port: u16) -> Option<String> {
        let url = self.config.peers.get(&self.leader()?)?;
        let (base, _) = url.rsplit_once(':')?;
        Some(format!("{base}:{port}"))
    }

    /// The log the leader's engine runs on, and a follower's replica reads.
    pub fn log(&self) -> Arc<RaftLog> {
        Arc::new(RaftLog { raft: self.raft.clone(), wal: Arc::clone(&self.wal), store: self.store.clone() })
    }

    /// Status for `/cluster/status`.
    pub fn status(&self) -> serde_json::Value {
        let m = self.raft.metrics().borrow_watched().clone();
        serde_json::json!({
            "node": self.id,
            "state": format!("{:?}", m.state),
            "leader": m.current_leader,
            "term": m.current_term,
            "last_log_index": m.last_log_index,
            "last_applied": m.last_applied.map(|l| l.index),
            "wal_offset": self.wal.last_offset().0,
            "peers": self.config.peers,
        })
    }
}

/// The leader's log: appends commit through the replica set; everything else is
/// the node's WAL.
pub struct RaftLog {
    raft: AdbRaft,
    wal: Arc<LocalWal>,
    store: store::LogStore,
}

impl RaftLog {
    async fn write(&self, record: LogRecord, new_segment: bool) -> LogResult<LogOffset> {
        let offset = record.offset;
        match self.raft.client_write(Cmd { record, new_segment, pad: Vec::new() }).await {
            Ok(r) => Ok(LogOffset(r.data.0)),
            Err(e) => Err(LogError::Io(format!("record {} not committed by the replica set: {e}", offset.0))),
        }
    }
}

#[async_trait]
impl Log for RaftLog {
    fn partition(&self) -> PartitionId {
        self.wal.partition()
    }

    async fn append(&self, record: LogRecord) -> LogResult<LogOffset> {
        self.write(record, false).await
    }

    async fn append_on_new_segment(&self, record: LogRecord) -> LogResult<LogOffset> {
        self.write(record, true).await
    }

    /// Hand the record to Raft's core (in submission order) and return: the commit is
    /// awaited by the caller after it releases the sequencer (PF-1/PF-2), so records
    /// pipeline into shared AppendEntries and fsyncs.
    async fn submit(&self, record: LogRecord) -> LogResult<adb_log::CommitWait> {
        let offset = record.offset;
        let (responder, log_id, rx) = openraft::impls::ProgressResponder::<TypeConfig, _>::new();
        let t = std::time::Instant::now();
        self.raft
            .client_write_ff(Cmd { record, new_segment: false, pad: Vec::new() }, Some(responder))
            .await
            .map_err(|e| LogError::Io(format!("record {} not submitted to the replica set: {e}", offset.0)))?;
        adb_core::steps::add("raft.client_write_ff", t.elapsed());
        Ok(Box::pin(async move {
            // Held until the commit resolves: dropped early, openraft warns on every write.
            let _log_id = log_id;
            match rx.await {
                Ok(Ok(r)) => Ok(LogOffset(r.data.0)),
                Ok(Err(e)) => Err(LogError::Io(format!("record {} not committed by the replica set: {e}", offset.0))),
                Err(_) => Err(LogError::Io(format!("record {}: the replica set dropped it before committing", offset.0))),
            }
        }))
    }

    async fn read(&self, from: LogOffset, max: usize) -> LogResult<Vec<LogRecord>> {
        self.wal.read(from, max).await
    }

    fn last_offset(&self) -> LogOffset {
        self.wal.last_offset()
    }

    fn durable_offset(&self) -> LogOffset {
        self.wal.durable_offset()
    }

    async fn purge_below(&self, offset: LogOffset) -> LogResult<()> {
        self.wal.purge_below(offset).await
    }

    /// The WAL's closed segments, then openraft's own log: both hold every record's
    /// effects, so a forgotten payload leaves this node's disk only once both are done.
    /// Returns the WAL records changed.
    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize> {
        let n = self.wal.rewrite_closed(redact).await?;
        let entries = self.store.redact(redact).map_err(|e| LogError::Io(format!("raft log redaction: {e}")))?;
        tracing::info!(wal_records = n, raft_entries = entries, "forgotten payloads redacted from the logs");
        Ok(n)
    }
}

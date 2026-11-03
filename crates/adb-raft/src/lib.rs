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
        let raft = Raft::new(config.node_id, cfg, net::Network::new(), log, sm).await.map_err(|e| e.to_string())?;
        let lowest = config.peers.keys().next().copied();
        if empty && lowest == Some(config.node_id) {
            let members: BTreeMap<NodeId, BasicNode> = config.peers.iter().map(|(id, url)| (*id, BasicNode { addr: url.clone() })).collect();
            match raft.initialize(members).await {
                Ok(()) => tracing::info!(node = config.node_id, peers = ?config.peers, "raft group initialized"),
                Err(e) => tracing::info!(node = config.node_id, error = %e, "raft group not initialized here"),
            }
        }
        Ok(Self { id: config.node_id, raft, wal, config })
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
        self.is_leader() && self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await.is_ok() && self.is_leader()
    }

    /// The log the leader's engine runs on.
    pub fn log(&self) -> Arc<RaftLog> {
        Arc::new(RaftLog { raft: self.raft.clone(), wal: Arc::clone(&self.wal) })
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
}

impl RaftLog {
    async fn write(&self, record: LogRecord, new_segment: bool) -> LogResult<LogOffset> {
        let offset = record.offset;
        match self.raft.client_write(Cmd { record, new_segment }).await {
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

    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize> {
        self.wal.rewrite_closed(redact).await
    }
}

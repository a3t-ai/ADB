//! The control service (voters) and the control client (every node).
//!
//! - **Voters** (`ADB_CONTROL_PEERS`, default `ADB_PEERS`: three nodes) run the
//!   control group ([`ControlNode`]). Its leader takes heartbeats, keeps the leases
//!   ([`crate::leases`]), adds joining nodes, and every heartbeat interval applies what
//!   [`crate::map::plan`] proposes.
//! - **Every node** runs a [`ControlClient`]: it heartbeats the control leader with its
//!   load and keeps the map it answers with, for the gateway, the owner and the replica.
//!
//! HTTP, on each voter's control endpoint:
//!
//! ```text
//! POST /control/heartbeat   {node, addr, flight, load} → {map}   (leader; else 421 {leader})
//! GET  /control/map         the map as applied on this voter
//! POST /control/raft/*      the group's Raft RPCs
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::leases::{self, Leases};
use crate::map::{plan, ControlCmd, NodeId, Partition, PartitionMap};
use crate::raft::{self as craft, ControlRaft, LogStore, StateMachine};

/// `ADB_PARTITIONS`: how many partitions the cluster has (fixed once the cluster
/// starts).
pub fn partitions() -> Partition {
    std::env::var("ADB_PARTITIONS").ok().and_then(|v| v.parse().ok()).filter(|&v: &Partition| v > 0).unwrap_or(64)
}

/// `1=http://a:3003,2=http://b:3003`.
pub fn parse_peers(s: &str) -> Result<BTreeMap<NodeId, String>, String> {
    let mut out = BTreeMap::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (id, url) = part.split_once('=').ok_or_else(|| format!("peer entry {part:?} is not id=url"))?;
        let id: NodeId = id.trim().parse().map_err(|_| format!("peer id {id:?} is not a number"))?;
        out.insert(id, url.trim().trim_end_matches('/').to_string());
    }
    if out.is_empty() {
        return Err("no control peer named".into());
    }
    Ok(out)
}

/// What a heartbeat carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node: NodeId,
    /// The node's data-plane base URL.
    pub addr: String,
    /// Its Arrow Flight endpoint.
    #[serde(default)]
    pub flight: Option<String>,
    /// Its own load report (higher = busier).
    pub load: f64,
}

/// A voter that doesn't lead answers 421 with this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotLeader {
    pub leader: Option<String>,
}

/// Reads the highest epoch object storage shows per partition (`run_with`).
pub type EpochFloors = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<BTreeMap<Partition, u64>, String>> + Send>> + Send + Sync>;

/// A started control-group voter.
pub struct ControlNode {
    pub id: NodeId,
    pub raft: ControlRaft,
    /// Voter id → its control endpoint base URL.
    pub voters: BTreeMap<NodeId, String>,
    map: watch::Receiver<PartitionMap>,
    /// While this voter leads: the leases (`None` otherwise).
    leases: Arc<Mutex<Option<Leases>>>,
    /// Serialises writes proposed by heartbeats and the planner.
    proposing: tokio::sync::Mutex<()>,
}

/// The control plane's own runtime: the Raft group, its endpoint, the planner and the
/// heartbeats run here, on `ADB_CONTROL_THREADS` threads (default 2) no request ever
/// uses, with their own connections. A stuck data path may stop the system; it must
/// never make a live node look dead (Hetzner round 7: a node busy with requests missed
/// its heartbeats).
pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        let n = std::env::var("ADB_CONTROL_THREADS").ok().and_then(|v| v.parse().ok()).filter(|&n: &usize| n > 0).unwrap_or(2);
        tokio::runtime::Builder::new_multi_thread().worker_threads(n).thread_name("adb-control").enable_all().build().expect("the control runtime")
    })
}

/// `ADB_START_GRACE_SECS`: how long a new leader waits for every voter to be live
/// before the first placement (default 30).
fn start_grace() -> std::time::Duration {
    std::time::Duration::from_secs(std::env::var("ADB_START_GRACE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(30))
}

/// `ADB_REBALANCE_SETTLE_SECS`: how long the cluster is whole, with no repair, before
/// rebalancing (default 60).
fn rebalance_settle() -> std::time::Duration {
    std::time::Duration::from_secs(std::env::var("ADB_REBALANCE_SETTLE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60))
}

/// `ADB_REBALANCE_SECS`: at most one rebalancing step this often (default 10). A node
/// taking a partition over needs that long or less (S3 option B, 15 Nov).
fn rebalance_every() -> std::time::Duration {
    std::time::Duration::from_secs(std::env::var("ADB_REBALANCE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(10))
}

impl ControlNode {
    /// Open the group's log and map in `dir/control`, start Raft, and, on the lowest
    /// voter id with an empty log, initialise the group. Serve [`Self::router`] on the
    /// voter's control endpoint, then [`Self::run`].
    pub async fn start(dir: &std::path::Path, id: NodeId, voters: BTreeMap<NodeId, String>) -> Result<Arc<Self>, String> {
        let log = LogStore::open(dir.join("control/log")).map_err(|e| format!("control log: {e}"))?;
        let empty = log.is_empty();
        let sm = Arc::new(StateMachine::open(dir.join("control/sm")).map_err(|e| format!("control state: {e}"))?);
        let map = sm.watch();
        let cfg = Arc::new(craft::raft_config().validate().map_err(|e| e.to_string())?);
        let raft = ControlRaft::new(id, cfg, craft::Network::new(), log, sm).await.map_err(|e| e.to_string())?;
        if empty && voters.keys().next() == Some(&id) {
            let members: BTreeMap<NodeId, BasicNode> = voters.iter().map(|(id, url)| (*id, BasicNode { addr: url.clone() })).collect();
            match raft.initialize(members).await {
                Ok(()) => tracing::info!(node = id, voters = ?voters, "control group initialized"),
                Err(e) => tracing::info!(node = id, error = %e, "control group not initialized here"),
            }
        }
        Ok(Arc::new(Self { id, raft, voters, map, leases: Default::default(), proposing: Default::default() }))
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow_watched().current_leader
    }

    pub fn is_leader(&self) -> bool {
        self.leader() == Some(self.id)
    }

    /// The map as applied on this voter.
    pub fn map(&self) -> PartitionMap {
        self.map.borrow().clone()
    }

    /// Propose one command; its outcome once committed.
    pub async fn propose(&self, cmd: ControlCmd) -> Result<u64, String> {
        let _one = self.proposing.lock().await;
        let r = self.raft.client_write(cmd).await.map_err(|e| e.to_string())?;
        r.data.0.map_err(|e| e.to_string())
    }

    /// The leader's work, while this voter leads: every heartbeat interval, the
    /// partition count first, then what the planner proposes. Runs until dropped.
    pub async fn run(self: Arc<Self>, partitions: Partition) {
        self.run_with(partitions, None).await
    }

    /// [`Self::run`]; before the partition count is set, `floors` gives the highest
    /// epoch object storage shows per partition ([`ControlCmd::EpochFloors`]).
    pub async fn run_with(self: Arc<Self>, partitions: Partition, floors: Option<EpochFloors>) {
        let mut tick = tokio::time::interval(leases::heartbeat());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut leading = false;
        // A new leader waits a full interval before its first rebalance step.
        let mut rebalanced = std::time::Instant::now();
        let mut led_since = std::time::Instant::now();
        // The last repair (a lost owner or replica replaced): no rebalancing until the
        // cluster has been whole for `ADB_REBALANCE_SETTLE_SECS`.
        let mut repaired = std::time::Instant::now();
        loop {
            tick.tick().await;
            let now = self.is_leader();
            if now && !leading {
                // A new leader: every member gets a full lease before anything moves.
                *self.leases.lock().unwrap() = Some(Leases::new(leases::lease()));
                led_since = std::time::Instant::now();
                tracing::info!(node = self.id, "leading the control group");
            } else if !now && leading {
                *self.leases.lock().unwrap() = None;
            }
            leading = now;
            if !leading {
                continue;
            }
            // Committed entries of earlier terms are applied before planning on them.
            if let Err(e) = self.raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex).await {
                tracing::debug!(error = %e, "control: not planning (no linearizable read)");
                continue;
            }
            let map = self.map();
            if map.partitions == 0 {
                if let Some(f) = &floors {
                    match f().await {
                        Ok(floors) if !floors.is_empty() => {
                            tracing::info!(partitions = floors.len(), "object storage already holds partitions: their epochs start above what it shows");
                            if let Err(e) = self.propose(ControlCmd::EpochFloors { floors }).await {
                                tracing::warn!(error = %e, "epoch floors not set");
                                continue;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, "object storage's epochs not read; retrying");
                            continue;
                        }
                    }
                }
                if let Err(e) = self.propose(ControlCmd::Init { partitions }).await {
                    tracing::warn!(error = %e, "partition count not set");
                }
                continue;
            }
            let (live, reported) = {
                let l = self.leases.lock().unwrap();
                let Some(l) = l.as_ref() else { continue };
                (l.live(map.members.keys().copied()), l.reported())
            };
            // The first placement waits for every voter of the control group (or
            // `ADB_START_GRACE_SECS`): placing on whoever was up first left a node empty
            // and the rebalancer moving partitions under load for minutes (Hetzner,
            // round 4).
            if map.assignments.is_empty() {
                let voters: std::collections::BTreeSet<NodeId> = self.raft.metrics().borrow_watched().membership_config.membership().voter_ids().collect();
                let missing: Vec<NodeId> = voters.iter().copied().filter(|v| !live.contains(v)).collect();
                if !missing.is_empty() && led_since.elapsed() < start_grace() {
                    tracing::debug!(?missing, "control: first placement waits for every node");
                    continue;
                }
            }
            let mut changes = plan(&map, &live, &reported);
            if !changes.is_empty() {
                repaired = std::time::Instant::now();
            }
            // One step toward an even spread, every `ADB_REBALANCE_SECS`, only while every
            // member is live and nothing was repaired for `ADB_REBALANCE_SETTLE_SECS`:
            // moving partitions among the survivors of a failure, under its load, took a
            // second node down (Hetzner round 6).
            let whole = map.members.keys().all(|m| live.contains(m));
            if changes.is_empty() && whole && repaired.elapsed() >= rebalance_settle() && rebalanced.elapsed() >= rebalance_every() {
                if let Some(c) = crate::map::rebalance(&map, &live) {
                    rebalanced = std::time::Instant::now();
                    changes.push(c);
                }
            }
            tracing::debug!(?live, changes = changes.len(), "control: planned");
            for change in changes {
                tracing::info!(cmd = %change.cmd, reason = change.reason, "control: reassigning");
                if let Err(e) = self.propose(change.cmd.clone()).await {
                    tracing::warn!(cmd = %change.cmd, error = %e, "control: reassignment not applied");
                    break;
                }
            }
        }
    }

    /// `/control/heartbeat`, `/control/map` and the Raft RPCs.
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/control/heartbeat", post(heartbeat))
            .route("/control/map", get(map))
            .layer(axum::middleware::from_fn(crate::identity::guard))
            .with_state(Arc::clone(self))
            .merge(craft::router(self.raft.clone()))
    }
}

async fn heartbeat(State(node): State<Arc<ControlNode>>, Json(hb): Json<Heartbeat>) -> axum::response::Response {
    // Only a leader a quorum still follows takes heartbeats (its Raft lease, else a
    // ReadIndex round): an old leader cut off from the others, or stopped, must not
    // keep nodes heartbeating it while a new one plans without them.
    let confirmed = match node.raft.ensure_linearizable(openraft::ReadPolicy::LeaseRead).await {
        Ok(_) => true,
        Err(_) => node.raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex).await.is_ok(),
    };
    let heard = confirmed && {
        let mut l = node.leases.lock().unwrap();
        match l.as_mut() {
            Some(l) if node.is_leader() => {
                l.heard(hb.node, hb.load);
                true
            }
            _ => false,
        }
    };
    if !heard {
        // Never itself: a voter that can't confirm it leads doesn't know who does.
        let leader = node.leader().filter(|l| *l != node.id).and_then(|l| node.voters.get(&l).cloned());
        return (StatusCode::MISDIRECTED_REQUEST, Json(NotLeader { leader })).into_response();
    }
    let map = node.map();
    if map.members.get(&hb.node).map(|m| (&m.addr, &m.flight)) != Some((&hb.addr, &hb.flight)) {
        if let Err(e) = node.propose(ControlCmd::Join { node: hb.node, addr: hb.addr.clone(), flight: hb.flight.clone() }).await {
            return (StatusCode::SERVICE_UNAVAILABLE, e).into_response();
        }
        tracing::info!(node = hb.node, addr = %hb.addr, "node joined");
        return Json(node.map()).into_response();
    }
    Json(map).into_response()
}

async fn map(State(node): State<Arc<ControlNode>>) -> Json<PartitionMap> {
    Json(node.map())
}

/// Every node's view of the map, kept by heartbeating the control leader.
#[derive(Clone)]
pub struct ControlClient {
    rx: watch::Receiver<PartitionMap>,
    /// When the control leader last answered (a node that can't reach it for a lease
    /// may have lost its partitions).
    last_ok: Arc<Mutex<Option<std::time::Instant>>>,
    /// The heartbeat task: stopped once the last clone is dropped.
    _task: Arc<AbortOnDrop>,
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl ControlClient {
    /// Start heartbeating as `node` (data plane at `addr`, Flight at `flight`) to the
    /// voters' control endpoints; `load` gives the load to report each time.
    pub fn start(node: NodeId, addr: String, flight: Option<String>, voters: BTreeMap<NodeId, String>, load: impl Fn() -> f64 + Send + 'static) -> Self {
        let (tx, rx) = watch::channel(PartitionMap::default());
        let last_ok: Arc<Mutex<Option<std::time::Instant>>> = Default::default();
        let ok = Arc::clone(&last_ok);
        let task = tokio::spawn(async move {
            let http = reqwest::Client::builder().connect_timeout(Duration::from_millis(500)).timeout(Duration::from_secs(2)).build().expect("HTTP client");
            let urls: Vec<String> = voters.values().cloned().collect();
            let mut target = urls.first().cloned().unwrap_or_default();
            let mut next_try = 0usize;
            let mut tick = tokio::time::interval(leases::heartbeat());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let hb = Heartbeat { node, addr: addr.clone(), flight: flight.clone(), load: load() };
                match crate::identity::tag(http.post(format!("{target}/control/heartbeat"))).json(&hb).send().await {
                    Ok(r) if r.status().is_success() => match r.json::<PartitionMap>().await {
                        Ok(map) => {
                            *ok.lock().unwrap() = Some(std::time::Instant::now());
                            tx.send_if_modified(|m| {
                                let changed = *m != map;
                                *m = map;
                                changed
                            });
                        }
                        Err(e) => tracing::debug!(error = %e, "control: unreadable map"),
                    },
                    Ok(r) if r.status() == StatusCode::MISDIRECTED_REQUEST => {
                        match r.json::<NotLeader>().await.ok().and_then(|n| n.leader).filter(|l| *l != target) {
                            Some(l) => target = l,
                            None => {
                                next_try = (next_try + 1) % urls.len().max(1);
                                target = urls.get(next_try).cloned().unwrap_or_default();
                            }
                        }
                    }
                    Ok(r) => tracing::debug!(status = %r.status(), "control: heartbeat refused"),
                    Err(e) => {
                        tracing::debug!(error = %e, %target, "control: heartbeat failed");
                        next_try = (next_try + 1) % urls.len().max(1);
                        target = urls.get(next_try).cloned().unwrap_or_default();
                    }
                }
            }
        });
        Self { rx, last_ok, _task: Arc::new(AbortOnDrop(task)) }
    }

    /// The latest map heard.
    pub fn map(&self) -> PartitionMap {
        self.rx.borrow().clone()
    }

    /// Changes of the map.
    pub fn subscribe(&self) -> watch::Receiver<PartitionMap> {
        self.rx.clone()
    }

    /// Whether the control leader answered within the last lease.
    pub fn in_touch(&self) -> bool {
        self.last_ok.lock().unwrap().is_some_and(|t| t.elapsed() < leases::lease())
    }
}

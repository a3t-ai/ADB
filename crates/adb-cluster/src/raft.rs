//! The control plane's Raft group (openraft): its log, its state machine (the
//! [`PartitionMap`]) and its RPCs.
//!
//! The log carries only [`ControlCmd`]s (a node joining, a partition reassigned) and
//! openraft's own entries, so it is small: the whole of it lives in memory and on
//! disk as one file (`<dir>/control/log`), replaced atomically on every change, and a
//! snapshot (the map itself) every `ADB_CONTROL_SNAPSHOT_EVERY` (default 1000)
//! entries keeps it short. The data path never goes through it.

use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use futures::{Stream, TryStreamExt};
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::error::{NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse};
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{BasicNode, Config, EntryPayload, OptionalSend, Raft, SnapshotPolicy};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::map::{ControlCmd, NodeId, PartitionMap, Refused};

impl fmt::Display for ControlCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The answer to a committed [`ControlCmd`]: applied, or refused by the map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome(pub Result<u64, Refused>);

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ControlCmd,
        R = Outcome,
        Node = BasicNode,
);

type TC = TypeConfig;
pub type ControlRaft = Raft<TC, Arc<StateMachine>>;

/// Heartbeat 100 ms, election 1–2 s (the control plane is not latency critical; a
/// leader change only pauses reassignments), Pre-Vote on.
pub fn raft_config() -> Config {
    let ms = |var: &str, d: u64| std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    Config {
        cluster_name: "adb-control".into(),
        heartbeat_interval: ms("ADB_CONTROL_HEARTBEAT_MS", 100),
        election_timeout_min: ms("ADB_CONTROL_ELECTION_MIN_MS", 1000),
        election_timeout_max: ms("ADB_CONTROL_ELECTION_MAX_MS", 2000),
        snapshot_policy: SnapshotPolicy::LogsSinceLast(ms("ADB_CONTROL_SNAPSHOT_EVERY", 1000)),
        max_in_snapshot_log_to_keep: 100,
        enable_pre_vote: Some(true),
        ..Default::default()
    }
}

// ---------------------------------------------------------------- files

fn invalid(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Write `v` to `dir/name` atomically (temporary file, fsync, rename, directory fsync).
fn put<T: Serialize>(dir: &Path, name: &str, v: &T) -> io::Result<()> {
    use std::io::Write;
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&rmp_serde::to_vec_named(v).map_err(invalid)?)?;
    f.sync_all()?;
    std::fs::rename(&tmp, dir.join(name))?;
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

fn get<T: DeserializeOwned>(dir: &Path, name: &str) -> io::Result<Option<T>> {
    match std::fs::read(dir.join(name)) {
        Ok(b) => rmp_serde::from_slice(&b).map(Some).map_err(invalid),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------- log

#[derive(Default, Clone, Serialize, Deserialize)]
struct LogDisk {
    vote: Option<VoteOf<TC>>,
    committed: Option<LogIdOf<TC>>,
    purged: Option<LogIdOf<TC>>,
    entries: BTreeMap<u64, EntryOf<TC>>,
}

/// openraft's log and vote: all in memory, saved whole on every change.
#[derive(Clone)]
pub struct LogStore {
    dir: PathBuf,
    disk: Arc<Mutex<LogDisk>>,
}

impl LogStore {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let disk = get(&dir, "log")?.unwrap_or_default();
        Ok(Self { dir, disk: Arc::new(Mutex::new(disk)) })
    }

    pub fn is_empty(&self) -> bool {
        let d = self.disk.lock().unwrap();
        d.entries.is_empty() && d.purged.is_none() && d.vote.is_none()
    }

    fn save(&self, d: &LogDisk) -> io::Result<()> {
        put(&self.dir, "log", d)
    }

    fn change(&self, f: impl FnOnce(&mut LogDisk)) -> io::Result<()> {
        let mut d = self.disk.lock().unwrap();
        let mut next = d.clone();
        f(&mut next);
        self.save(&next)?;
        *d = next;
        Ok(())
    }
}

impl RaftLogReader<TC> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> Result<Vec<EntryOf<TC>>, io::Error> {
        Ok(self.disk.lock().unwrap().entries.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TC>>, io::Error> {
        Ok(self.disk.lock().unwrap().vote.clone())
    }
}

impl RaftLogStorage<TC> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TC>, io::Error> {
        let d = self.disk.lock().unwrap();
        let last = d.entries.values().next_back().map(|e| e.log_id()).or_else(|| d.purged.clone());
        Ok(LogState { last_purged_log_id: d.purged.clone(), last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TC>) -> Result<(), io::Error> {
        self.change(|d| d.vote = Some(vote.clone()))
    }

    async fn save_committed(&mut self, c: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        self.change(|d| d.committed = c)
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TC>>, io::Error> {
        Ok(self.disk.lock().unwrap().committed.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TC>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TC>> + OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let r = self.change(|d| {
            for e in entries {
                d.entries.insert(e.index(), e);
            }
        });
        let answer = r.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string()));
        callback.io_completed(answer);
        r
    }

    async fn truncate_after(&mut self, last: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        let from = last.as_ref().map(|l| l.index() + 1).unwrap_or(0);
        self.change(|d| {
            d.entries.split_off(&from);
        })
    }

    async fn purge(&mut self, log_id: LogIdOf<TC>) -> Result<(), io::Error> {
        let from = log_id.index() + 1;
        self.change(|d| {
            d.entries = d.entries.split_off(&from);
            d.purged = Some(log_id);
        })
    }
}

// ---------------------------------------------------------------- state machine

#[derive(Default, Clone, Serialize, Deserialize)]
struct SmDisk {
    last_applied: Option<LogIdOf<TC>>,
    membership: StoredMembershipOf<TC>,
    map: PartitionMap,
}

/// The ownership map, built by applying the log; every applied change is published
/// on [`StateMachine::watch`].
pub struct StateMachine {
    dir: PathBuf,
    state: Mutex<SmDisk>,
    snapshot: Mutex<Option<(SnapshotMetaOf<TC>, Vec<u8>)>>,
    map_tx: watch::Sender<PartitionMap>,
}

impl StateMachine {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let state: SmDisk = get(&dir, "state")?.unwrap_or_default();
        let snapshot = get(&dir, "snapshot")?;
        let (map_tx, _) = watch::channel(state.map.clone());
        Ok(Self { dir, state: Mutex::new(state), snapshot: Mutex::new(snapshot), map_tx })
    }

    /// The map as applied here, and its changes.
    pub fn watch(&self) -> watch::Receiver<PartitionMap> {
        self.map_tx.subscribe()
    }
}

impl RaftSnapshotBuilder<TC> for Arc<StateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TC, Cursor<Vec<u8>>>, io::Error> {
        let s = self.state.lock().unwrap().clone();
        let data = rmp_serde::to_vec_named(&s.map).map_err(invalid)?;
        let meta = SnapshotMetaOf::<TC> { last_log_id: s.last_applied, last_membership: s.membership };
        put(&self.dir, "snapshot", &(meta.clone(), data.clone()))?;
        *self.snapshot.lock().unwrap() = Some((meta.clone(), data.clone()));
        Ok(SnapshotOf::<TC, Cursor<Vec<u8>>> { meta, snapshot: Cursor::new(data) })
    }
}

impl RaftStateMachine<TC> for Arc<StateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogIdOf<TC>>, StoredMembershipOf<TC>), io::Error> {
        let s = self.state.lock().unwrap();
        Ok((s.last_applied.clone(), s.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TC>, io::Error>> + Unpin + OptionalSend,
    {
        let mut answers = Vec::new();
        let mut next = self.state.lock().unwrap().clone();
        while let Some((entry, responder)) = entries.try_next().await? {
            let answer = match entry.payload {
                EntryPayload::Normal(cmd) => {
                    let r = next.map.apply(&cmd).map(|_| next.map.version);
                    if let Err(e) = &r {
                        tracing::info!(cmd = %cmd, refused = %e, "control command refused");
                    }
                    Outcome(r)
                }
                EntryPayload::Membership(m) => {
                    next.membership = StoredMembershipOf::<TC>::new(Some(entry.log_id.clone()), m);
                    Outcome(Ok(next.map.version))
                }
                EntryPayload::Blank => Outcome(Ok(next.map.version)),
            };
            next.last_applied = Some(entry.log_id.clone());
            answers.push((responder, answer));
        }
        put(&self.dir, "state", &next)?;
        let map = next.map.clone();
        *self.state.lock().unwrap() = next;
        self.map_tx.send_if_modified(|m| {
            let changed = *m != map;
            *m = map;
            changed
        });
        for (responder, answer) in answers {
            if let Some(r) = responder {
                r.send(answer);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMetaOf<TC>, snapshot: Self::SnapshotData) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let map: PartitionMap = rmp_serde::from_slice(&data).map_err(invalid)?;
        let next = SmDisk { last_applied: meta.last_log_id.clone(), membership: meta.last_membership.clone(), map: map.clone() };
        put(&self.dir, "state", &next)?;
        put(&self.dir, "snapshot", &(meta.clone(), data.clone()))?;
        *self.state.lock().unwrap() = next;
        *self.snapshot.lock().unwrap() = Some((meta.clone(), data));
        self.map_tx.send_replace(map);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TC, Self::SnapshotData>>, io::Error> {
        Ok(self.snapshot.lock().unwrap().clone().map(|(meta, data)| SnapshotOf::<TC, Cursor<Vec<u8>>> { meta, snapshot: Cursor::new(data) }))
    }
}

// ---------------------------------------------------------------- network

#[derive(Clone)]
pub struct Network {
    http: reqwest::Client,
}

impl Network {
    pub fn new() -> Self {
        let http = reqwest::Client::builder().connect_timeout(Duration::from_millis(500)).pool_idle_timeout(Duration::from_secs(30)).build().expect("HTTP client");
        Self { http }
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Conn {
    http: reqwest::Client,
    base: String,
}

impl RaftNetworkFactory<TC> for Network {
    type Network = Conn;
    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        Conn { http: self.http.clone(), base: node.addr.trim_end_matches('/').to_string() }
    }
}

fn unreachable(e: impl fmt::Display) -> Unreachable<TC> {
    Unreachable::new(&io::Error::other(e.to_string()))
}

#[derive(Serialize, Deserialize)]
struct SnapshotReq {
    vote: VoteOf<TC>,
    meta: SnapshotMetaOf<TC>,
    data: Vec<u8>,
}

impl Conn {
    async fn call<Req: Serialize, Resp: DeserializeOwned, E: DeserializeOwned>(&self, path: &str, body: &Req, timeout: Duration) -> Result<Result<Resp, E>, Unreachable<TC>> {
        let bytes = rmp_serde::to_vec_named(body).map_err(unreachable)?;
        let r = self
            .http
            .post(format!("{}/control/raft/{path}", self.base))
            .timeout(timeout)
            .header("content-type", "application/msgpack")
            .body(bytes)
            .send()
            .await
            .map_err(unreachable)?;
        if !r.status().is_success() {
            return Err(unreachable(format!("{} answered {}", self.base, r.status())));
        }
        let b = r.bytes().await.map_err(unreachable)?;
        rmp_serde::from_slice(&b).map_err(unreachable)
    }

    async fn rpc<Req: Serialize, Resp: DeserializeOwned>(&self, path: &str, body: &Req, o: &RPCOption) -> Result<Resp, RPCError<TC>> {
        match self.call::<_, Resp, RaftError<TC>>(path, body, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::Network(NetworkError::new(&e))),
            Err(u) => Err(RPCError::Unreachable(u)),
        }
    }
}

impl RaftNetworkV2<TC> for Conn {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(&mut self, rpc: AppendEntriesRequest<TC>, o: RPCOption) -> Result<AppendEntriesResponse<TC>, RPCError<TC>> {
        self.rpc("append", &rpc, &o).await
    }

    async fn vote(&mut self, rpc: VoteRequest<TC>, o: RPCOption) -> Result<VoteResponse<TC>, RPCError<TC>> {
        self.rpc("vote", &rpc, &o).await
    }

    async fn pre_vote(&mut self, rpc: VoteRequest<TC>, o: RPCOption) -> Result<VoteResponse<TC>, RPCError<TC>> {
        self.rpc("pre_vote", &rpc, &o).await
    }

    async fn transfer_leader(&mut self, req: TransferLeaderRequest<TC>, o: RPCOption) -> Result<TransferLeaderResponse<TC>, RPCError<TC>> {
        self.rpc("transfer", &req, &o).await
    }

    /// The snapshot is the map: small, sent whole.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TC>,
        snapshot: SnapshotOf<TC, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        o: RPCOption,
    ) -> Result<SnapshotResponse<TC>, StreamingError<TC>> {
        let req = SnapshotReq { vote, meta: snapshot.meta, data: snapshot.snapshot.into_inner() };
        match self.call::<_, SnapshotResponse<TC>, String>("snapshot", &req, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(StreamingError::Network(NetworkError::new(&io::Error::other(e)))),
            Err(u) => Err(StreamingError::Unreachable(u)),
        }
    }
}

fn msgpack<T: Serialize>(v: &T) -> axum::response::Response {
    match rmp_serde::to_vec_named(v) {
        Ok(b) => ([(axum::http::header::CONTENT_TYPE, "application/msgpack")], b).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn decode<T: DeserializeOwned>(b: &Bytes) -> Result<T, axum::response::Response> {
    rmp_serde::from_slice(b).map_err(|e| (StatusCode::BAD_REQUEST, format!("bad control raft request: {e}")).into_response())
}

async fn append(State(raft): State<ControlRaft>, body: Bytes) -> axum::response::Response {
    match decode::<AppendEntriesRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.append_entries(req).await),
        Err(r) => r,
    }
}

async fn vote(State(raft): State<ControlRaft>, body: Bytes) -> axum::response::Response {
    match decode::<VoteRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.vote(req).await),
        Err(r) => r,
    }
}

async fn pre_vote(State(raft): State<ControlRaft>, body: Bytes) -> axum::response::Response {
    match decode::<VoteRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.pre_vote(req).await),
        Err(r) => r,
    }
}

async fn transfer(State(raft): State<ControlRaft>, body: Bytes) -> axum::response::Response {
    match decode::<TransferLeaderRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.handle_transfer_leader(req).await),
        Err(r) => r,
    }
}

async fn snapshot(State(raft): State<ControlRaft>, body: Bytes) -> axum::response::Response {
    match decode::<SnapshotReq>(&body) {
        Ok(req) => {
            let snap = SnapshotOf::<TC, Cursor<Vec<u8>>> { meta: req.meta, snapshot: Cursor::new(req.data) };
            let r: Result<SnapshotResponse<TC>, String> = raft.install_full_snapshot(req.vote, snap).await.map_err(|e| e.to_string());
            msgpack(&r)
        }
        Err(r) => r,
    }
}

/// The control group's Raft RPCs (`/control/raft/*`).
pub fn router(raft: ControlRaft) -> Router {
    Router::new()
        .route("/control/raft/append", post(append))
        .route("/control/raft/vote", post(vote))
        .route("/control/raft/pre_vote", post(pre_vote))
        .route("/control/raft/transfer", post(transfer))
        .route("/control/raft/snapshot", post(snapshot))
        .with_state(raft)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::log::{StoreBuilder, Suite};
    use openraft::StorageError;

    struct Builder;

    impl StoreBuilder<TC, LogStore, Arc<StateMachine>, tempfile::TempDir> for Builder {
        async fn build(&self) -> Result<(tempfile::TempDir, LogStore, Arc<StateMachine>), StorageError<TC>> {
            let d = tempfile::tempdir().unwrap();
            let log = LogStore::open(d.path().join("log")).unwrap();
            let sm = StateMachine::open(d.path().join("sm")).unwrap();
            Ok((d, log, Arc::new(sm)))
        }
    }

    /// openraft's own conformance suite, log and state machine.
    #[tokio::test]
    async fn openraft_storage_suite() {
        Suite::test_all(Builder).await.unwrap();
    }
}

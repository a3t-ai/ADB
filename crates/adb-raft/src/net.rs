//! Raft RPCs between nodes: `POST <peer>/raft/{append,vote,pre_vote,snapshot,transfer}`,
//! MessagePack bodies. A node's answer is the `Result` openraft returned, so a
//! remote error comes back as itself; a failed HTTP call is "unreachable".

use std::io::{self, Cursor};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use openraft::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::error::{NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{BasicNode, OptionalSend};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{AdbRaft, NodeId, TypeConfig as TC};

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

/// A peer without this route (an older build during a rolling update).
const NOT_FOUND: &str = "raft route not found on the peer";

fn unreachable(e: impl std::fmt::Display) -> Unreachable<TC> {
    Unreachable::new(&io::Error::other(e.to_string()))
}

#[derive(Serialize, Deserialize)]
struct SnapshotReq {
    vote: VoteOf<TC>,
    meta: SnapshotMetaOf<TC>,
}

impl Conn {
    /// POST `body` to `path`; the answer is openraft's `Result` on the peer.
    async fn call<Req: Serialize, Resp: DeserializeOwned, E: DeserializeOwned>(&self, path: &str, body: &Req, timeout: Duration) -> Result<Result<Resp, E>, Unreachable<TC>> {
        let bytes = rmp_serde::to_vec_named(body).map_err(unreachable)?;
        let r = self
            .http
            .post(format!("{}/raft/{path}", self.base))
            .timeout(timeout)
            .header("content-type", "application/msgpack")
            .body(bytes)
            .send()
            .await
            .map_err(unreachable)?;
        if r.status() == StatusCode::NOT_FOUND {
            return Err(unreachable(NOT_FOUND));
        }
        if !r.status().is_success() {
            return Err(unreachable(format!("{} answered {}", self.base, r.status())));
        }
        let b = r.bytes().await.map_err(unreachable)?;
        rmp_serde::from_slice(&b).map_err(unreachable)
    }
}

impl RaftNetworkV2<TC> for Conn {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(&mut self, rpc: AppendEntriesRequest<TC>, o: RPCOption) -> Result<AppendEntriesResponse<TC>, RPCError<TC>> {
        match self.call::<_, AppendEntriesResponse<TC>, RaftError<TC>>("append", &rpc, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::Network(NetworkError::new(&e))),
            Err(u) => Err(RPCError::Unreachable(u)),
        }
    }

    async fn vote(&mut self, rpc: VoteRequest<TC>, o: RPCOption) -> Result<VoteResponse<TC>, RPCError<TC>> {
        match self.call::<_, VoteResponse<TC>, RaftError<TC>>("vote", &rpc, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::Network(NetworkError::new(&e))),
            Err(u) => Err(RPCError::Unreachable(u)),
        }
    }

    /// Pre-Vote (`enable_pre_vote`): would the peer vote for us at the next term?
    /// A peer too old to know the RPC counts as granting, as openraft's default
    /// does, so a rolling update stays live.
    async fn pre_vote(&mut self, rpc: VoteRequest<TC>, o: RPCOption) -> Result<VoteResponse<TC>, RPCError<TC>> {
        match self.call::<_, VoteResponse<TC>, RaftError<TC>>("pre_vote", &rpc, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::Network(NetworkError::new(&e))),
            Err(u) if u.to_string().contains(NOT_FOUND) => Ok(VoteResponse::new(rpc.vote, None, true)),
            Err(u) => Err(RPCError::Unreachable(u)),
        }
    }

    async fn transfer_leader(&mut self, req: TransferLeaderRequest<TC>, o: RPCOption) -> Result<TransferLeaderResponse<TC>, RPCError<TC>> {
        match self.call::<_, TransferLeaderResponse<TC>, RaftError<TC>>("transfer", &req, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(RPCError::Network(NetworkError::new(&e))),
            Err(u) => Err(RPCError::Unreachable(u)),
        }
    }

    /// The snapshot is a pointer: only its meta travels.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TC>,
        snapshot: SnapshotOf<TC, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        o: RPCOption,
    ) -> Result<SnapshotResponse<TC>, StreamingError<TC>> {
        let req = SnapshotReq { vote, meta: snapshot.meta };
        match self.call::<_, SnapshotResponse<TC>, String>("snapshot", &req, o.hard_ttl()).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(StreamingError::Network(NetworkError::new(&io::Error::other(e)))),
            Err(u) => Err(StreamingError::Unreachable(u)),
        }
    }
}

// ---------------------------------------------------------------- server side

fn msgpack<T: Serialize>(v: &T) -> axum::response::Response {
    match rmp_serde::to_vec_named(v) {
        Ok(b) => ([(axum::http::header::CONTENT_TYPE, "application/msgpack")], b).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn decode<T: DeserializeOwned>(b: &Bytes) -> Result<T, axum::response::Response> {
    rmp_serde::from_slice(b).map_err(|e| (StatusCode::BAD_REQUEST, format!("bad raft request: {e}")).into_response())
}

async fn append(State(raft): State<AdbRaft>, body: Bytes) -> axum::response::Response {
    match decode::<AppendEntriesRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.append_entries(req).await),
        Err(r) => r,
    }
}

async fn vote(State(raft): State<AdbRaft>, body: Bytes) -> axum::response::Response {
    match decode::<VoteRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.vote(req).await),
        Err(r) => r,
    }
}

async fn pre_vote(State(raft): State<AdbRaft>, body: Bytes) -> axum::response::Response {
    match decode::<VoteRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.pre_vote(req).await),
        Err(r) => r,
    }
}

async fn transfer(State(raft): State<AdbRaft>, body: Bytes) -> axum::response::Response {
    match decode::<TransferLeaderRequest<TC>>(&body) {
        Ok(req) => msgpack(&raft.handle_transfer_leader(req).await),
        Err(r) => r,
    }
}

async fn snapshot(State(raft): State<AdbRaft>, body: Bytes) -> axum::response::Response {
    match decode::<SnapshotReq>(&body) {
        Ok(req) => {
            let snap = SnapshotOf::<TC, Cursor<Vec<u8>>> { meta: req.meta, snapshot: Cursor::new(Vec::new()) };
            let r: Result<SnapshotResponse<TC>, String> = raft.install_full_snapshot(req.vote, snap).await.map_err(|e| e.to_string());
            msgpack(&r)
        }
        Err(r) => r,
    }
}

/// Follower reads (C-16): the leader's ReadIndex — the log index a read must see.
/// 503 when this node doesn't lead.
async fn read_index(State(raft): State<AdbRaft>) -> axum::response::Response {
    match leader_read_index(&raft).await {
        Ok(index) => axum::Json(index).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

/// The index a linearizable read must see, on the leader (PF-30): within its lease
/// (a quorum acknowledged it less than a lease ago) without contacting the
/// followers; once the lease has lapsed, by a ReadIndex heartbeat round.
pub async fn leader_read_index(raft: &AdbRaft) -> Result<u64, String> {
    match raft.get_read_log_id(openraft::ReadPolicy::LeaseRead).await {
        Ok((read, _)) => Ok(read.index()),
        Err(_) => raft.get_read_log_id(openraft::ReadPolicy::ReadIndex).await.map(|(read, _)| read.index()).map_err(|e| e.to_string()),
    }
}

async fn metrics(State(raft): State<AdbRaft>) -> axum::response::Response {
    let m = raft.metrics().borrow_watched().clone();
    axum::Json(serde_json::json!({
        "node": m.id,
        "state": format!("{:?}", m.state),
        "leader": m.current_leader,
        "term": m.current_term,
        "last_log_index": m.last_log_index,
        "last_applied": m.last_applied.map(|l| l.index),
    }))
    .into_response()
}

/// The Raft endpoint (serve it on the node's Raft port, reachable by peers).
pub fn router(raft: AdbRaft) -> Router {
    Router::new()
        .route("/raft/append", post(append))
        .route("/raft/vote", post(vote))
        .route("/raft/pre_vote", post(pre_vote))
        .route("/raft/transfer", post(transfer))
        .route("/raft/snapshot", post(snapshot))
        .route("/raft/metrics", get(metrics))
        .route("/raft/read_index", get(read_index))
        .with_state(raft)
}

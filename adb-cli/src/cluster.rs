//! `adb serve-http` as a cluster node (S3 option B, step 8): `ADB_NODE_ID` set.
//!
//! Every node runs the same pieces: the control client (heartbeats, the map), the
//! partition router (owner of some partitions, replica of others, reader of the rest),
//! HTTP (`/query`, `/internal/*`, `/ready`) and Arrow Flight. The voters
//! (`ADB_CONTROL_PEERS`, default `ADB_PEERS`) also run the control group.
//!
//! | Variable | What |
//! |---|---|
//! | `ADB_NODE_ID` | this node's id (`from-hostname`: a StatefulSet pod `adb-<n>` is n + 1) |
//! | `ADB_CONTROL_PEERS` | voters' control endpoints: `1=http://adb-0.adb:3002,…` |
//! | `ADB_CONTROL_PORT` | where this node serves the control group (a voter; default 3002) |
//! | `ADB_ADVERTISE` | this node's HTTP base as the others reach it (`http://10.0.0.1:3000`) |
//! | `ADB_ADVERTISE_FLIGHT` | its Flight endpoint as the others reach it |
//! | `ADB_PARTITIONS` | memory partitions (default 64; fixed for the cluster's life) |
//! | `ADB_COLD_URI` | object storage — required: the replica keeps the WAL in memory only; its `cluster.json` names the cluster ([`crate::cluster_mark`]) |
//!
//! A node starts serving once the map gives every partition an owner. Stopping is
//! plain: its partitions move after its lease lapses (a planned hand-over is open).

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use adb_cluster::map::{NodeId, PartitionMap};
use adb_cluster::{ControlClient, ControlNode};
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::Placement;
use adb_executor::Engine;
use axum::routing::get;
use axum::Router;

/// This node's settings, from the environment; `None` without `ADB_NODE_ID`.
pub struct ClusterConfig {
    pub id: NodeId,
    pub voters: BTreeMap<NodeId, String>,
    pub control_port: u16,
    pub advertise: String,
    pub advertise_flight: Option<String>,
}

impl ClusterConfig {
    pub fn from_env(port: u16, flight_port: u16) -> Result<Option<Self>, String> {
        let Some(id) = std::env::var("ADB_NODE_ID").ok().filter(|v| !v.is_empty()) else { return Ok(None) };
        let id: NodeId = if id == "from-hostname" {
            let host = std::env::var("HOSTNAME").unwrap_or_default();
            host.rsplit('-').next().and_then(|n| n.parse::<u64>().ok()).map(|n| n + 1).ok_or_else(|| format!("ADB_NODE_ID=from-hostname, but HOSTNAME {host:?} does not end in -<ordinal>"))?
        } else {
            id.parse().map_err(|_| format!("ADB_NODE_ID {id:?} is not a number"))?
        };
        let peers = std::env::var("ADB_CONTROL_PEERS").or_else(|_| std::env::var("ADB_PEERS")).map_err(|_| "ADB_NODE_ID is set but ADB_CONTROL_PEERS is not".to_string())?;
        let voters = adb_cluster::control::parse_peers(&peers)?;
        let control_port = std::env::var("ADB_CONTROL_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3002);
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "127.0.0.1".into());
        let advertise = std::env::var("ADB_ADVERTISE").unwrap_or_else(|_| format!("http://{host}:{port}"));
        let advertise_flight = std::env::var("ADB_ADVERTISE_FLIGHT").ok().or_else(|| (flight_port != 0).then(|| format!("http://{host}:{flight_port}")));
        Ok(Some(Self { id, voters, control_port, advertise, advertise_flight }))
    }
}

/// The router's view of the cluster: the control client's map (kept as one shared
/// copy, replaced when it changes) and its lease.
struct ClientPlacement {
    me: NodeId,
    client: ControlClient,
    map: Arc<RwLock<Arc<PartitionMap>>>,
}

impl Placement for ClientPlacement {
    fn me(&self) -> NodeId {
        self.me
    }
    fn map(&self) -> Arc<PartitionMap> {
        Arc::clone(&self.map.read().unwrap())
    }
    fn leased(&self) -> bool {
        self.client.in_touch()
    }
}

async fn bind(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let mut tries = 0;
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => return Ok(l),
            Err(e) if tries < 50 => {
                tries += 1;
                tracing::debug!(%addr, error = %e, "port busy; retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whether every partition (and the system partition) has an owner.
fn whole(m: &PartitionMap) -> bool {
    m.partitions > 0 && m.all_partitions().all(|p| m.assignments.contains_key(&p))
}

pub async fn run(dir: &Path, config: ClusterConfig, host: IpAddr, port: u16, flight_port: u16, shutdown: impl std::future::Future<Output = &'static str>) -> Result<(), Box<dyn std::error::Error>> {
    if adb_executor::engine::object_storage().is_none() {
        return Err("a cluster node needs object storage (ADB_COLD_URI): a replica keeps the WAL in memory only".into());
    }
    // Which cluster this is: object storage's mark against this disk's, before Raft or
    // a partition sees anything of this disk.
    let cold = adb_executor::engine::object_storage().expect("checked above");
    let first = config.voters.keys().next() == Some(&config.id);
    if let Some(mark) = crate::cluster_mark::settle(dir, &cold, first, adb_cluster::control::partitions()).await? {
        tracing::info!(node = config.id, cluster = %mark.id, "ADB cluster");
    }
    let token = adb_server::auth::token_from_env();
    // The control group, on a voter: on the control plane's own runtime (its threads,
    // its connections), so request load can't hold up Raft or the planner.
    let rt = adb_cluster::control::runtime();
    if config.voters.contains_key(&config.id) {
        let (dir, id, voters, control_port) = (dir.to_path_buf(), config.id, config.voters.clone(), config.control_port);
        rt.spawn(async move {
            let node = ControlNode::start(&dir, id, voters).await?;
            let app = node.router();
            let listener = bind(SocketAddr::new(host, control_port)).await.map_err(|e| e.to_string())?;
            tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!(error = %e, "control endpoint stopped");
                }
            });
            // A cluster started again from object storage: partitions start above the
            // epochs it already shows.
            let cold = adb_executor::engine::object_storage().expect("checked above");
            let floors: adb_cluster::control::EpochFloors = Arc::new(move || {
                let cold = cold.clone();
                Box::pin(async move {
                    let parts = adb_cluster::control::partitions();
                    let mut out = BTreeMap::new();
                    for p in 0..=parts {
                        let epochs = adb_executor::writes::segments::list_epochs(&cold, adb_core::log::PartitionId(p)).await.map_err(|e| e.to_string())?;
                        if let Some((e, _)) = epochs.last() {
                            out.insert(p, *e);
                        }
                    }
                    Ok(out)
                })
            });
            tokio::spawn(Arc::clone(&node).run_with(adb_cluster::control::partitions(), Some(floors)));
            tracing::info!(node = id, port = control_port, "control group voter started");
            Ok::<(), String>(())
        })
        .await
        .map_err(|e| e.to_string())??;
    }
    // The map, from the control plane.
    // The heartbeats too: their task on the control runtime.
    let client = {
        let _on = rt.enter();
        ControlClient::start(config.id, config.advertise.clone(), config.advertise_flight.clone(), config.voters.clone(), || 0.0)
    };
    let mut rx = client.subscribe();
    let t0 = std::time::Instant::now();
    while !whole(&rx.borrow()) {
        if t0.elapsed() > Duration::from_secs(120) {
            return Err("the control plane gave no complete map within 120 s".into());
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), rx.changed()).await;
    }
    let shared = Arc::new(RwLock::new(Arc::new(rx.borrow().clone())));
    {
        let shared = Arc::clone(&shared);
        let mut rx = client.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                *shared.write().unwrap() = Arc::new(rx.borrow().clone());
            }
        });
    }
    tracing::info!(node = config.id, partitions = shared.read().unwrap().partitions, took_ms = t0.elapsed().as_millis() as u64, "cluster map complete");
    let placement = Arc::new(ClientPlacement { me: config.id, client: client.clone(), map: shared });
    let peers = Arc::new(adb_server::internal::HttpPeers::new(token.clone()));
    let node = PartitionedEngine::open_cluster(dir, placement as Arc<dyn Placement>, peers as _).await?;
    let in_touch = client.clone();
    let app = Router::new()
        .route("/ready", get(move || {
            let ok = in_touch.in_touch();
            async move { if ok { (axum::http::StatusCode::OK, "ready") } else { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "no lease") } }
        }))
        .merge(adb_server::cluster_router(Arc::clone(&node), token.clone()));
    let http = axum::serve(bind(SocketAddr::new(host, port)).await?, app);
    let engine: Arc<dyn Engine> = Arc::new(Arc::clone(&node));
    let flight = async {
        match flight_port {
            0 => std::future::pending().await,
            p => adb_server::serve_flight(SocketAddr::new(host, p), engine).await,
        }
    };
    tracing::info!(node = config.id, advertise = %config.advertise, "ADB cluster node serving");
    tokio::select! {
        r = http => r?,
        r = flight => r?,
        s = shutdown => tracing::info!(signal = s, "ADB cluster node shutting down"),
        _ = adb_executor::restart_requested() => tracing::info!("ADB stopping to apply its new settings"),
    }
    Ok(())
}

//! `adb serve-http` in a replica set (M3, `.ai/M3_RAFT.md`): `ADB_NODE_ID` and
//! `ADB_PEERS` set.
//!
//! - Raft RPCs on `ADB_RAFT_PORT` (default 3002), always.
//! - **Leader:** the engine opens on the Raft log and serves HTTP and Flight as a
//!   single node does; `/ready` answers 200. A Kubernetes Service with a
//!   readiness probe on `/ready` therefore sends clients to the leader only.
//! - **Follower:** a [`Replica`] applies the replicated WAL as it grows; HTTP
//!   answers `/health` 200, `/ready` 503 and everything else 503 with the
//!   leader's id; Flight is not served.
//! - A leader that loses the lead stops the process (exit code 75): its open
//!   connections hold the engine, and a fresh start as a follower is the clean
//!   way out. The supervisor (Kubernetes) starts it again.

use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use adb_executor::{Engine, EngineLog, LocalEngine, Replica};
use adb_raft::{ClusterConfig, Node};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};

/// Exit code after losing the lead (the supervisor restarts ADB as a follower).
pub const LOST_LEAD: i32 = 75;

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

pub async fn run(dir: &Path, config: ClusterConfig, host: IpAddr, port: u16, flight_port: u16, shutdown: impl std::future::Future<Output = &'static str>) -> Result<(), Box<dyn std::error::Error>> {
    let raft_port: u16 = std::env::var("ADB_RAFT_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3002);
    let wal = LocalEngine::open_local_wal(dir).await?;
    let node = Arc::new(Node::start(dir, Arc::clone(&wal), config).await?);
    tracing::info!(node = node.id, peers = ?node.config.peers, raft_port, "ADB replica set member started");

    let status_node = Arc::clone(&node);
    let raft_app = adb_raft::net::router(node.raft.clone()).route("/cluster/status", get(move || {
        let n = Arc::clone(&status_node);
        async move { Json(n.status()) }
    }));
    let raft_listener = bind(SocketAddr::new(host, raft_port)).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(raft_listener, raft_app).await {
            tracing::error!(error = %e, "raft endpoint stopped");
        }
    });

    tokio::pin!(shutdown);
    loop {
        if node.is_leader() && node.ready_to_lead().await {
            tracing::info!(node = node.id, "leading the replica set: opening the engine");
            let log: EngineLog = node.log();
            let (engine, stats) = LocalEngine::durable_on(dir, log).await?;
            tracing::info!(records = stats.records, last_offset = stats.last_offset.0, "ADB engine open (leader)");
            let engine: Arc<dyn Engine> = Arc::new(engine);
            let app = Router::new().route("/ready", get(|| async { "leader" })).merge(adb_server::router(Arc::clone(&engine)));
            let http = axum::serve(bind(SocketAddr::new(host, port)).await?, app);
            let flight = async {
                match flight_port {
                    0 => std::future::pending().await,
                    p => adb_server::serve_flight(SocketAddr::new(host, p), Arc::clone(&engine)).await,
                }
            };
            tokio::select! {
                r = http => r?,
                r = flight => r?,
                _ = node.until_lead(false) => {
                    tracing::warn!(node = node.id, leader = ?node.leader(), "lost the lead; stopping so the supervisor restarts this node as a follower");
                    std::process::exit(LOST_LEAD);
                }
                s = &mut shutdown => {
                    tracing::info!(signal = s, "ADB shutting down");
                    let _ = node.raft.shutdown().await;
                    return Ok(());
                }
                _ = adb_executor::restart_requested() => {
                    tracing::info!("ADB stopping to apply its new settings");
                    let _ = node.raft.shutdown().await;
                    return Ok(());
                }
            }
        }

        // A follower (or a candidate, or a leader not yet ready).
        let mut replica = Replica::open(dir, Arc::clone(&wal) as EngineLog).await?;
        let leader_node = Arc::clone(&node);
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/ready", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "follower") }))
            .fallback(move || {
                let leader = leader_node.leader();
                async move { (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({ "error": "this ADB node is not the leader", "leader": leader }))) }
            })
            // No kept-alive connection to a follower: once this node leads, a
            // client's pooled connection would still reach this router.
            .layer(axum::middleware::map_response(|mut r: axum::response::Response| async move {
                r.headers_mut().insert(axum::http::header::CONNECTION, axum::http::HeaderValue::from_static("close"));
                r
            }));
        let http = std::future::IntoFuture::into_future(axum::serve(bind(SocketAddr::new(host, port)).await?, app));
        tokio::pin!(http);
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if let Err(e) = replica.step().await {
                        tracing::warn!(error = %e, "replica step failed; retrying");
                    }
                }
                _ = node.until_lead(true) => break,
                r = &mut http => { r?; }
                s = &mut shutdown => {
                    tracing::info!(signal = s, "ADB shutting down");
                    let _ = node.raft.shutdown().await;
                    return Ok(());
                }
            }
        }
        // Becoming leader: catch up, then (next turn) open the engine.
        if let Err(e) = replica.step().await {
            tracing::warn!(error = %e, "last replica step before leading failed");
        }
        drop(replica);
    }
}

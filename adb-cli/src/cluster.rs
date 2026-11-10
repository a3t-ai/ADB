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
    // C-4: Raft (its core, heartbeats, replication and RPC server) runs on its own
    // threads. On the shared runtime, CPU-heavy queries kept the leader from sending
    // heartbeats in time and lost it the lead (AWS, 10 Nov). Everything else talks to it
    // through openraft's channels, as before.
    let raft_rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::env::var("ADB_RAFT_THREADS").ok().and_then(|n| n.parse().ok()).unwrap_or(2))
            .thread_name("adb-raft")
            .enable_all()
            .build()?,
    ));
    let start = {
        let (dir, wal) = (dir.to_path_buf(), Arc::clone(&wal));
        raft_rt.spawn(async move { Node::start(&dir, wal, config).await })
    };
    let node = Arc::new(start.await.map_err(|e| format!("raft runtime: {e}"))??);
    tracing::info!(node = node.id, peers = ?node.config.peers, raft_port, "ADB replica set member started");

    let status_node = Arc::clone(&node);
    let raft_app = adb_raft::net::router(node.raft.clone()).route("/cluster/status", get(move || {
        let n = Arc::clone(&status_node);
        async move { Json(n.status()) }
    }));
    let raft_listener = bind(SocketAddr::new(host, raft_port)).await?;
    // A listener belongs to the runtime it was registered on: re-register it on Raft's.
    let raft_listener = raft_listener.into_std()?;
    raft_rt.spawn(async move {
        let listener = match tokio::net::TcpListener::from_std(raft_listener) {
            Ok(l) => l,
            Err(e) => return tracing::error!(error = %e, "raft endpoint not started"),
        };
        if let Err(e) = axum::serve(listener, raft_app).await {
            tracing::error!(error = %e, "raft endpoint stopped");
        }
    });

    tokio::pin!(shutdown);
    // What the follower kept open, handed over when this node takes the lead (CA-34).
    let mut warm: Option<adb_executor::Warm> = None;
    loop {
        if node.is_leader() && node.ready_to_lead().await {
            let t0 = std::time::Instant::now();
            let log: EngineLog = node.log();
            let (engine, stats) = match warm.take() {
                Some(w) => {
                    tracing::info!(node = node.id, "leading the replica set: taking over the replica's open engine");
                    LocalEngine::durable_warm(dir, log, w).await?
                }
                None => {
                    tracing::info!(node = node.id, "leading the replica set: opening the engine");
                    LocalEngine::durable_on(dir, log).await?
                }
            };
            tracing::info!(records = stats.records, last_offset = stats.last_offset.0, took_ms = t0.elapsed().as_millis() as u64, "ADB engine open (leader)");
            engine.set_branch_remote(crate::branch::remote());
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
                    let t0 = std::time::Instant::now();
                    let moved = node.hand_over(Duration::from_secs(3)).await;
                    tracing::info!(moved, took_ms = t0.elapsed().as_millis() as u64, leader = ?node.leader(), "lead handed over");
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
        // The Raft log, not only the WAL: erasure must reach openraft's own log too.
        let mut replica = Replica::open(dir, node.log() as EngineLog).await?;
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
        // Becoming leader: catch up, then (next turn) hand the open parts over.
        let keep_warm = std::env::var("ADB_WARM_HANDOVER").map_or(true, |v| v != "0");
        match replica.step().await {
            Ok(_) if keep_warm => warm = Some(replica.into_warm()),
            Ok(_) => drop(replica),
            Err(e) => {
                tracing::warn!(error = %e, "last replica step before leading failed; the engine will be reopened");
                drop(replica);
            }
        }
    }
}

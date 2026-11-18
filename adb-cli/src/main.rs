#![recursion_limit = "256"]
//! `adb` — the ADB command-line interface.
//!
//! - `adb serve` — MCP over stdio (Claude Code, Claude Desktop)
//! - `adb serve --http [--host 0.0.0.0] [--port 3000] [--flight-port 3001]` — REST +
//!   MCP over HTTP/SSE, a drop-in for ClawDB's server (A3T), plus Arrow Flight for
//!   FlowR's `call:adb` (platform tables, AQL); `adb serve-http` is the same
//! - `adb query "<AQL>"` — run one statement and print the JSON result
//!
//! With `--data-dir <dir>` (or `ADB_DATA_DIR`) every write goes to a write-ahead log
//! in that directory first and is replayed on start, so memory survives restarts.
//! Without it ADB keeps memory in RAM only, as before.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use adb_mcp::McpServer;
use clap::{Parser, Subcommand};

mod alloc;
mod cluster;
mod cluster_mark;
mod import;
mod import_engine;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "adb", version, about = "Agent Database — memory for AI agents")]
struct Cli {
    /// Keep memory durable in this directory (write-ahead log, replayed on start)
    #[arg(long, global = true, env = "ADB_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the MCP server on stdio (default), or the HTTP server with --http
    Serve {
        /// Serve REST + MCP over HTTP instead of MCP on stdio
        #[arg(long)]
        http: bool,
        #[arg(long, default_value = "0.0.0.0")]
        host: IpAddr,
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
        /// Arrow Flight port (FlowR's `call:adb`, platform tables); 0 = off
        #[arg(long, default_value_t = 3001)]
        flight_port: u16,
    },
    /// Run the HTTP server (same as `serve --http`)
    ServeHttp {
        #[arg(long, default_value = "0.0.0.0")]
        host: IpAddr,
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
        /// Arrow Flight port (FlowR's `call:adb`, platform tables); 0 = off
        #[arg(long, default_value_t = 3001)]
        flight_port: u16,
    },
    /// Run an embedding worker (PF-49): `POST /embeddings` (the OpenAI wire format)
    /// from the local model. ADB nodes with `ADB_EMBED_URL=http://<host>:<port>[,…]`
    /// send their texts here and run no model themselves.
    ServeEmbed {
        #[arg(long, default_value = "0.0.0.0")]
        host: IpAddr,
        #[arg(short, long, default_value_t = 3003)]
        port: u16,
    },
    /// Copy an embedded LanceDB directory (A3T's a3t-platform.lance) into a running
    /// ADB's platform tables over Arrow Flight
    ImportLancedb {
        /// The LanceDB directory (holding <table>.lance datasets)
        path: std::path::PathBuf,
        /// ADB's Flight endpoint
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        flight: String,
        /// Replace the rows of tables ADB already has (default: skip them)
        #[arg(long)]
        replace: bool,
    },
    /// Copy a FlowR SQLite engine store (flowr.db: runs, journals, tasks, waits)
    /// into an engine space of a running ADB over Arrow Flight (uses
    /// ADB_ENGINE_TOKEN); the space must hold no runs yet
    ImportFlowrEngine {
        /// FlowR's flowr.db, or with --snapshot an ADB engine space's snapshot.json
        path: std::path::PathBuf,
        /// PATH is another ADB's engine snapshot (`<data>/engine/<space>/snapshot.json`,
        /// taken with its log replayed), not a SQLite store
        #[arg(long)]
        snapshot: bool,
        /// With --snapshot: that ADB's Flight endpoint, serving its data, to read
        /// finished runs' journals back from (token ADB_ENGINE_TOKEN_FROM)
        #[arg(long)]
        journals_from: Option<String>,
        /// ADB's Flight endpoint
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        flight: String,
        /// The engine space (FlowR's FLOWR_ENGINE_SPACE)
        #[arg(long, default_value = "default")]
        space: String,
    },
    /// Read-only SQL over a running ADB's memory and platform tables, over Arrow
    /// Flight (superadmin debug view; uses ADB_AUTH_TOKEN). Tables: memory,
    /// memory_current, memory_raw and each platform table.
    Sql {
        /// The SQL query (SELECT / WITH / SHOW / EXPLAIN)
        query: String,
        /// ADB's Flight endpoint
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        flight: String,
        /// Most rows to return (default 1,000, at most 10,000)
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Execute one AQL statement and print the result as JSON
    Query {
        /// The AQL statement
        query: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    // A client command: no engine of its own.
    if let Some(Commands::ImportLancedb { path, flight, replace }) = &cli.command {
        return import::import(path, flight, *replace).await.map_err(|e| e.into());
    }
    if let Some(Commands::ImportFlowrEngine { path, flight, space, snapshot, journals_from }) = &cli.command {
        if *snapshot {
            return import_engine::import_snapshot(path, flight, space, journals_from.as_deref()).await.map_err(|e| e.into());
        }
        return import_engine::import(path, flight, space).await.map_err(|e| e.into());
    }
    if let Some(Commands::Sql { query, flight, limit }) = &cli.command {
        return import::sql(query, flight, *limit).await.map_err(|e| e.into());
    }
    let command = cli.command.unwrap_or(Commands::Serve { http: false, host: [0, 0, 0, 0].into(), port: 3000, flight_port: 3001 });

    // Logs always go to stderr: stdout carries MCP messages in stdio mode.
    tracing_subscriber::registry()
        .with(log_filter(std::env::var("RUST_LOG").ok()))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_ansi(false))
        .init();
    match adb_core::disk::raise_open_files() {
        Ok(n) => tracing::info!(limit = n, "open files"),
        Err(e) => tracing::warn!(error = %e, "could not raise the open-file limit"),
    }
    alloc::report_every(alloc::report_interval());
    adb_core::steps::spawn_lag_probe();
    if let Commands::ServeEmbed { host, port } = &command {
        return serve_embed(*host, *port).await;
    }

    // A cluster node (S3 option B, step 8): ADB_NODE_ID set.
    if let (Some(dir), Commands::Serve { http: true, host, port, flight_port } | Commands::ServeHttp { host, port, flight_port }) = (&cli.data_dir, &command) {
        if let Some(config) = cluster::ClusterConfig::from_env(*port, *flight_port)? {
            return cluster::run(dir, config, *host, *port, *flight_port, shutdown_signal()).await;
        }
    }

    let engine: Arc<dyn Engine> = match &cli.data_dir {
        Some(dir) => {
            let (engine, replayed) = LocalEngine::durable(dir).await?;
            tracing::info!(
                dir = %dir.display(),
                records = replayed.records,
                last_offset = replayed.last_offset.0,
                "ADB durable: replayed the write-ahead log"
            );
            Arc::new(engine)
        }
        None => {
            tracing::info!("ADB without --data-dir: a temporary directory, removed when ADB stops — memory is lost on restart");
            Arc::new(LocalEngine::temporary().await?)
        }
    };

    match command {
        Commands::Serve { http: false, .. } => {
            tracing::info!("ADB MCP server on stdio");
            let mcp = McpServer::new(engine);
            tokio::select! {
                r = mcp.run_stdio() => r?,
                s = shutdown_signal() => tracing::info!(signal = s, "ADB shutting down"),
            }
        }
        Commands::Serve { http: true, host, port, flight_port } | Commands::ServeHttp { host, port, flight_port } => {
            // Arrow Flight next to HTTP (stage G): FlowR's native wire.
            let flight = async {
                match flight_port {
                    0 => std::future::pending().await,
                    p => adb_server::serve_flight(SocketAddr::new(host, p), Arc::clone(&engine)).await,
                }
            };
            tokio::select! {
                r = adb_server::serve(SocketAddr::new(host, port), Arc::clone(&engine)) => r?,
                r = flight => r?,
                // Acknowledged writes are already durable (acked after fsync); returning
                // drops the engine, which drains and syncs the log.
                s = shutdown_signal() => tracing::info!(signal = s, "ADB shutting down"),
                // New settings (W9.10): stop; the supervisor starts ADB again.
                _ = adb_executor::restart_requested() => tracing::info!("ADB stopping to apply its new settings"),
            }
        }
        Commands::ImportLancedb { .. } | Commands::ImportFlowrEngine { .. } | Commands::Sql { .. } | Commands::ServeEmbed { .. } => unreachable!("handled before the engine opens"),
        Commands::Query { query } => match engine.query(&query, None).await {
            Ok(v) => println!("{}", serde_json::to_string_pretty(&v)?),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
    }
    Ok(())
}

/// SIGTERM (Kubernetes stopping the pod) or Ctrl-C.
async fn shutdown_signal() -> &'static str {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    }
}

/// `RUST_LOG`, with Lance and DataFusion held at `warn` unless it names them:
/// Lance 14 logs every query plan at INFO, which buries ADB's own logs under
/// `RUST_LOG=info`.
fn log_filter(env: Option<String>) -> tracing_subscriber::EnvFilter {
    let spec = log_spec(env);
    tracing_subscriber::EnvFilter::try_new(&spec).unwrap_or_else(|e| {
        eprintln!("RUST_LOG {spec:?}: {e}; using the default");
        tracing_subscriber::EnvFilter::new(log_spec(None))
    })
}

fn log_spec(env: Option<String>) -> String {
    let mut spec = env.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| "adb=info,adb_server=info".into());
    for quiet in ["lance", "datafusion"] {
        if !spec.split(',').any(|d| d.trim().starts_with(quiet)) {
            spec.push_str(&format!(",{quiet}=warn"));
        }
    }
    spec
}

#[cfg(test)]
mod log_tests {
    use super::log_spec;

    #[test]
    fn lance_and_datafusion_stay_quiet_unless_named() {
        assert_eq!(log_spec(None), "adb=info,adb_server=info,lance=warn,datafusion=warn");
        assert_eq!(log_spec(Some("info".into())), "info,lance=warn,datafusion=warn");
        assert_eq!(log_spec(Some("info,lance=debug".into())), "info,lance=debug,datafusion=warn");
        assert_eq!(log_spec(Some(" ".into())), "adb=info,adb_server=info,lance=warn,datafusion=warn");
    }
}

/// `adb serve-embed`: the local model behind `POST /embeddings` and `/v1/embeddings`.
async fn serve_embed(host: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use axum::routing::{get, post};
    // The worker embeds with its own model, never through a pool.
    std::env::remove_var("ADB_EMBED_URL");
    let embedder = adb_embed::Embedder::from_env().ok_or("serve-embed needs the model (ADB_EMBED_MODEL_DIR) and ADB_EMBED not off")?;
    async fn embed(axum::extract::State(e): axum::extract::State<std::sync::Arc<adb_embed::Embedder>>, axum::Json(req): axum::Json<serde_json::Value>) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
        let texts: Vec<String> = match req.get("input") {
            Some(serde_json::Value::String(s)) => vec![s.clone()],
            Some(serde_json::Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
            _ => Vec::new(),
        };
        let _t = adb_core::steps::timer("embed.served by this worker");
        match e.embed_here(texts).await {
            Ok(v) => {
                let data: Vec<serde_json::Value> = v.into_iter().enumerate().map(|(i, e)| serde_json::json!({ "object": "embedding", "index": i, "embedding": e })).collect();
                (axum::http::StatusCode::OK, axum::Json(serde_json::json!({ "object": "list", "model": e.model(), "data": data })))
            }
            Err(err) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!({ "error": err }))),
        }
    }
    let app = axum::Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/embeddings", post(embed))
        .route("/v1/embeddings", post(embed))
        .with_state(embedder);
    let addr = std::net::SocketAddr::new(host, port);
    tracing::info!(%addr, workers = adb_embed::workers(), "ADB embedding worker");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).with_graceful_shutdown(async { shutdown_signal().await; }).await?;
    Ok(())
}

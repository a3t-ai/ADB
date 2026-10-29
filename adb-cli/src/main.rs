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

mod import;
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
    if let Some(Commands::Sql { query, flight, limit }) = &cli.command {
        return import::sql(query, flight, *limit).await.map_err(|e| e.into());
    }
    let command = cli.command.unwrap_or(Commands::Serve { http: false, host: [0, 0, 0, 0].into(), port: 3000, flight_port: 3001 });

    // Logs always go to stderr: stdout carries MCP messages in stdio mode.
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "adb=info,adb_server=info".into()))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_ansi(false))
        .init();

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
        Commands::ImportLancedb { .. } | Commands::Sql { .. } => unreachable!("handled before the engine opens"),
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

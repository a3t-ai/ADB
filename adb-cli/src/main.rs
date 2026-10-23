//! `adb` — the ADB command-line interface.
//!
//! - `adb serve` — MCP over stdio (Claude Code, Claude Desktop)
//! - `adb serve --http [--host 0.0.0.0] [--port 3000]` — REST + MCP over HTTP/SSE,
//!   a drop-in for ClawDB's server (A3T); `adb serve-http` is the same
//! - `adb query "<AQL>"` — run one statement and print the JSON result

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use adb_mcp::McpServer;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "adb", version, about = "Agent Database — memory for AI agents")]
struct Cli {
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
    },
    /// Run the HTTP server (same as `serve --http`)
    ServeHttp {
        #[arg(long, default_value = "0.0.0.0")]
        host: IpAddr,
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
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
    let command = cli.command.unwrap_or(Commands::Serve { http: false, host: [0, 0, 0, 0].into(), port: 3000 });

    // Logs always go to stderr: stdout carries MCP messages in stdio mode.
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "adb=info,adb_server=info".into()))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_ansi(false))
        .init();

    let engine: Arc<dyn Engine> = Arc::new(LocalEngine::default());

    match command {
        Commands::Serve { http: false, .. } => {
            tracing::info!("ADB MCP server on stdio");
            McpServer::new(engine).run_stdio().await?;
        }
        Commands::Serve { http: true, host, port } | Commands::ServeHttp { host, port } => {
            adb_server::serve(SocketAddr::new(host, port), engine).await?;
        }
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

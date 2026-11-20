//! ADB Console: a small web UI built into the binary, so a node needs nothing else
//! to be looked at and used.
//!
//! | Path | Auth | What |
//! |---|---|---|
//! | `GET /ui` | open (the page holds no data; it asks for the token) | the console page |
//! | `GET /admin/console` | the memory token | this node: version, uptime, mode, auth, storage, every connection point, the cluster map |
//!
//! Everything else the page shows comes from the endpoints clients already use:
//! `/health`, `/stats`, `/query`, `/admin/embedding`, `/admin/cache`,
//! `/admin/compact`, `/admin/stats/metrics`, `/internal/compute`.

use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::http::header;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use serde_json::{json, Value};

const PAGE: &str = include_str!("console/index.html");

/// How this node was started (set once by `adb` before it serves).
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    /// `single` or `cluster`.
    pub mode: &'static str,
    pub node_id: Option<u64>,
    pub http_port: u16,
    /// 0: Flight is off.
    pub flight_port: u16,
    /// The control plane's port (cluster voters).
    pub control_port: Option<u16>,
    /// This node's data-plane address as the cluster knows it.
    pub advertise: Option<String>,
    pub data_dir: Option<String>,
}

/// The cluster map and whether this node is in touch with the control plane.
pub type MapSource = Arc<dyn Fn() -> (adb_cluster::map::PartitionMap, bool) + Send + Sync>;

static INFO: OnceLock<NodeInfo> = OnceLock::new();
static MAP: OnceLock<MapSource> = OnceLock::new();
static STARTED: OnceLock<(Instant, u64)> = OnceLock::new();

/// Record how this node runs (call once, before serving).
pub fn describe(info: NodeInfo) {
    started();
    let _ = INFO.set(info);
}

/// A cluster node: where the console reads the map.
pub fn cluster_map(source: MapSource) {
    let _ = MAP.set(source);
}

fn started() -> &'static (Instant, u64) {
    STARTED.get_or_init(|| (Instant::now(), SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)))
}

/// `GET /ui`
pub(crate) async fn page() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
            // Only this origin's scripts and calls, unless the user points the page
            // at another node (connect-src allows any http(s) origin for that).
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' http: https:; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
            ),
        ],
        PAGE,
    )
}

/// `GET /admin/console`
pub(crate) async fn info() -> Json<Value> {
    Json(report())
}

fn report() -> Value {
    let (t0, at) = started();
    let info = INFO.get().cloned().unwrap_or(NodeInfo {
        mode: "single",
        node_id: None,
        http_port: 0,
        flight_port: 0,
        control_port: None,
        advertise: None,
        data_dir: None,
    });
    let cold = std::env::var("ADB_COLD_URI").ok().filter(|v| !v.is_empty());
    let auth = json!({
        "memory_token": crate::auth::token_from_env().is_some(),
        "engine_token": crate::auth::engine_token_from_env().is_some(),
        "pool_tokens": crate::auth::pool_tokens_from_env().len(),
        "scoped_tokens": crate::scope::scopes_from_env().len(),
        "metrics_open": std::env::var("ADB_METRICS_OPEN").map(|v| !v.is_empty() && v != "0").unwrap_or(false),
    });
    let cluster = MAP.get().map(|source| {
        let (map, in_touch) = source();
        let mut owns = std::collections::BTreeMap::<u64, usize>::new();
        let mut replicas = std::collections::BTreeMap::<u64, usize>::new();
        let mut unreplicated = Vec::new();
        for (p, a) in &map.assignments {
            *owns.entry(a.owner).or_default() += 1;
            match a.replica {
                Some(r) => *replicas.entry(r).or_default() += 1,
                None => unreplicated.push(*p),
            }
        }
        let unassigned: Vec<u16> = map.all_partitions().filter(|p| !map.assignments.contains_key(p)).collect();
        let members: Vec<Value> = map
            .members
            .iter()
            .map(|(id, m)| {
                json!({
                    "id": id, "addr": m.addr, "flight": m.flight,
                    "owns": owns.get(id).copied().unwrap_or(0),
                    "replicas": replicas.get(id).copied().unwrap_or(0),
                })
            })
            .collect();
        let assignments: Vec<Value> =
            map.assignments.iter().map(|(p, a)| json!({ "partition": p, "owner": a.owner, "replica": a.replica, "epoch": a.epoch })).collect();
        json!({
            "in_touch": in_touch,
            "map_version": map.version,
            "partitions": map.partitions,
            "members": members,
            "assignments": assignments,
            "unassigned": unassigned,
            "unreplicated": unreplicated,
            "pins": map.pins,
        })
    });
    json!({
        "product": "ADB",
        "version": env!("CARGO_PKG_VERSION"),
        "started_at": at,
        "uptime_s": t0.elapsed().as_secs(),
        "node": info,
        "storage": { "data_dir": info.data_dir, "object_storage": cold },
        "auth": auth,
        "endpoints": endpoints(&info),
        "cluster": cluster,
    })
}

/// Every way in, with what it is for. URLs are paths or ports: the page puts the
/// host it reached this node at in front.
fn endpoints(info: &NodeInfo) -> Value {
    let mut out = vec![
        json!({ "name": "AQL over HTTP", "kind": "http", "method": "POST", "path": "/query", "body": "{\"query\", \"variables\"?}", "use": "Run any AQL statement; JSON answer {success, data, error?, code?}" }),
        json!({ "name": "Health", "kind": "http", "method": "GET", "path": "/health", "use": "Liveness probe; always open" }),
        json!({ "name": "Stats", "kind": "http", "method": "GET", "path": "/stats", "use": "Counts per memory type / partitions open" }),
        json!({ "name": "Metrics (Prometheus)", "kind": "http", "method": "GET", "path": "/metrics", "use": "Prometheus text; open with ADB_METRICS_OPEN" }),
        json!({ "name": "Metrics (JSON)", "kind": "http", "method": "GET", "path": "/admin/stats/metrics", "use": "Every metric, histograms with p50/p90/p99" }),
        json!({ "name": "Embedding setting", "kind": "http", "method": "GET/PUT", "path": "/admin/embedding", "use": "Provider (local, openai, custom), model, where it runs; applied live" }),
        json!({ "name": "Hot cache", "kind": "http", "method": "GET/PUT", "path": "/admin/cache", "use": "Cache rules and figures; applied live" }),
        json!({ "name": "Compact", "kind": "http", "method": "POST", "path": "/admin/compact", "use": "Compact the tables now" }),
        json!({ "name": "Subscription events (SSE)", "kind": "sse", "method": "GET", "path": "/subscriptions/:id/events", "use": "Topic messages for a subscription" }),
        json!({ "name": "WATCH events (SSE)", "kind": "sse", "method": "GET", "path": "/watches/:id/events", "use": "Changes for a WATCH" }),
        json!({ "name": "MCP (streamable HTTP)", "kind": "mcp", "method": "POST", "path": "/mcp", "use": "Agents and tools: remember, recall, forget, link, aql_query, watch, publish/subscribe" }),
        json!({ "name": "MCP (SSE)", "kind": "mcp", "method": "GET+POST", "path": "/mcp/sse", "use": "Older MCP clients: events on /mcp/sse, messages to /mcp/message" }),
        json!({ "name": "MCP (stdio)", "kind": "cli", "command": "adb serve", "use": "MCP on stdin/stdout for a local agent" }),
        json!({ "name": "Compute nodes", "kind": "http", "method": "GET", "path": "/internal/compute", "use": "Compute nodes this node sends embedding work to" }),
        json!({ "name": "Console", "kind": "http", "method": "GET", "path": "/ui", "use": "This page" }),
    ];
    if info.flight_port != 0 {
        out.push(json!({ "name": "Arrow Flight", "kind": "flight", "port": info.flight_port, "use": "Arrow batches: do_get {table|sql}, do_put, do_action(aql, list_tables, create_table, update, delete, compact, engine…)" }));
    }
    if let Some(p) = info.control_port {
        out.push(json!({ "name": "Control plane (Raft)", "kind": "internal", "port": p, "path": "/control/map", "use": "Between ADB nodes only" }));
    }
    if info.mode == "cluster" {
        out.push(json!({ "name": "Readiness", "kind": "http", "method": "GET", "path": "/ready", "use": "200 when this node holds its control-plane lease" }));
        out.push(json!({ "name": "Node to node", "kind": "internal", "path": "/internal/*", "use": "Replication and forwarding between ADB nodes only" }));
    }
    if info.flight_port != 0 {
        out.push(json!({ "name": "CLI: SQL", "kind": "cli", "command": "ADB_AUTH_TOKEN=… adb sql \"<SQL>\" --flight grpc://host:port", "use": "Read-only SQL over a running node (memory, memory_current, platform tables)" }));
    }
    out.push(json!({ "name": "CLI: AQL", "kind": "cli", "command": "adb --data-dir <dir> query \"<AQL>\"", "use": "One statement on a data directory no node is running on; for a running node use POST /query" }));
    Value::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_report_lists_flight_only_when_it_runs() {
        let mut info = NodeInfo { mode: "single", node_id: None, http_port: 3000, flight_port: 0, control_port: None, advertise: None, data_dir: None };
        let names = |v: Value| v.as_array().unwrap().iter().map(|e| e["name"].as_str().unwrap().to_string()).collect::<Vec<_>>();
        assert!(!names(endpoints(&info)).contains(&"Arrow Flight".to_string()));
        info.flight_port = 3001;
        assert!(names(endpoints(&info)).contains(&"Arrow Flight".to_string()));
    }

    #[test]
    fn the_page_is_built_in() {
        assert!(PAGE.contains("<title>ADB Console</title>"));
    }
}

//! MCP request handling, independent of transport.
//!
//! [`McpServer::handle`] serves stdio (`adb serve`), streamable HTTP (`POST /mcp`) and
//! SSE (`/mcp/sse` + `/mcp/message`, what A3T uses) alike.

use std::collections::HashMap;
use std::sync::Arc;

use adb_executor::Engine;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::protocol::{JsonRpcRequest, JsonRpcResponse, ToolCallResult};
use crate::tools::{self, literal, memory_type, payload, quote, where_eq};

/// MCP protocol versions this server speaks; the client's choice wins if listed.
const PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";

const GUIDE_URI: &str = "adb://guide/memory";
const GRAMMAR_URI: &str = "adb://aql/grammar";
const GUIDE: &str = include_str!("guide.md");

/// The MCP server: protocol handling over an [`Engine`].
#[derive(Clone)]
pub struct McpServer {
    engine: Arc<dyn Engine>,
}

impl McpServer {
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    /// Handle one JSON-RPC message. Notifications (no `id`) get no response.
    pub async fn handle(&self, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
        if req.id.is_none() {
            // Notifications (`notifications/initialized`, `notifications/cancelled`, …)
            return None;
        }
        let id = req.id.clone();
        Some(match req.method.as_str() {
            "initialize" => JsonRpcResponse::success(id, initialize(req.params.as_ref())),
            "ping" => JsonRpcResponse::success(id, json!({})),
            "tools/list" => JsonRpcResponse::success(id, tools::tool_list()),
            "tools/call" => match self.call_tool(req.params).await {
                Ok(result) => JsonRpcResponse::success(id, serde_json::to_value(result).unwrap_or_default()),
                Err((code, msg)) => JsonRpcResponse::error(id, code, msg),
            },
            "resources/list" => JsonRpcResponse::success(id, resource_list()),
            "resources/read" => match read_resource(req.params.as_ref()) {
                Ok(v) => JsonRpcResponse::success(id, v),
                Err(msg) => JsonRpcResponse::error(id, -32602, msg),
            },
            "prompts/list" => JsonRpcResponse::success(id, prompt_list()),
            "prompts/get" => match get_prompt(req.params.as_ref()) {
                Ok(v) => JsonRpcResponse::success(id, v),
                Err(msg) => JsonRpcResponse::error(id, -32602, msg),
            },
            "shutdown" => JsonRpcResponse::success(id, Value::Null),
            m => JsonRpcResponse::error(id, -32601, format!("Method not found: {m}")),
        })
    }

    /// Back-compat: handle a request and always return a response object.
    pub async fn handle_request(&self, req: JsonRpcRequest) -> JsonRpcResponse {
        let id = req.id.clone();
        self.handle(req).await.unwrap_or_else(|| JsonRpcResponse::success(id, json!({})))
    }

    /// Serve MCP over stdio: one JSON-RPC message per line.
    pub async fn run_stdio(&self) -> std::io::Result<()> {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        let mut out = tokio::io::stdout();
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
                Ok(req) => self.handle(req).await,
                Err(e) => Some(JsonRpcResponse::error(None, -32700, format!("Parse error: {e}"))),
            };
            if let Some(resp) = response {
                let mut text = serde_json::to_string(&resp).unwrap_or_default();
                text.push('\n');
                out.write_all(text.as_bytes()).await?;
                out.flush().await?;
            }
        }
        Ok(())
    }

    async fn call_tool(&self, params: Option<Value>) -> Result<ToolCallResult, (i32, String)> {
        let params = params.ok_or((-32602, "Missing params".to_string()))?;
        let name = params.get("name").and_then(Value::as_str).ok_or((-32602, "Missing tool name".to_string()))?;
        let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
        let result = match name {
            // `query`, `store` and `stats` are the names A3T's agent-memory-mcp resource lists.
            "aql_query" | "query" => self.aql_query(&args).await,
            "store_memory" | "store" => self.run(store_memory(&args)).await,
            "recall_memory" => self.run(recall_memory(&args)).await,
            "get_stats" | "stats" => match self.engine.stats().await {
                Ok(s) => Ok(s),
                Err(e) => Err(format!("Stats failed: {e}")),
            },
            "remember" => self.remember(&args).await,
            "recall" => self.run(recall(&args)).await,
            "forget" => self.run(forget(&args)).await,
            "link" => self.run(link(&args)).await,
            "subscribe" => self.run(subscribe(&args)).await,
            "publish" => self.run(publish(&args)).await,
            "ack" => self.run(ack(&args)).await,
            "unsubscribe" => self.run(unsubscribe(&args)).await,
            "next_events" => self.next_events(&args).await,
            other => Err(format!("Unknown tool: {other}")),
        };
        Ok(match result {
            Ok(v) => ToolCallResult::json(&v),
            Err(e) => ToolCallResult::error(e),
        })
    }

    async fn aql_query(&self, args: &Value) -> Result<Value, String> {
        let query = args.get("query").and_then(Value::as_str).ok_or("Missing required parameter: query")?;
        let vars: Option<HashMap<String, Value>> = args
            .get("variables")
            .and_then(Value::as_object)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        self.engine.query(query, vars.as_ref()).await.map_err(|e| format!("Query failed: {e}"))
    }

    /// A subscription's next deliveries through the gateway (stage E).
    async fn next_events(&self, args: &Value) -> Result<Value, String> {
        let gw = self.engine.gateway().ok_or("subscriptions need a durable engine (start ADB with --data-dir)")?;
        let sub = args.get("subscription").and_then(Value::as_str).ok_or("Missing required: subscription")?;
        let from = args.get("from").and_then(Value::as_u64);
        let max = args.get("max").and_then(Value::as_u64).unwrap_or(20).clamp(1, 500) as usize;
        let wait = args.get("wait_seconds").and_then(Value::as_u64).unwrap_or(10).min(30);
        let batch = gw.next(sub, from, max, std::time::Duration::from_secs(wait)).await.map_err(|e| e.to_string())?;
        serde_json::to_value(batch).map_err(|e| e.to_string())
    }

    /// Run AQL built by a shortcut tool.
    /// `remember` sets a fact: a new key is stored, a key in use is updated (a new
    /// version, history kept) — unlike `STORE` / `store_memory`, which refuse a key
    /// in use (D3 S4).
    async fn remember(&self, args: &Value) -> Result<Value, String> {
        let aql = remember(args)?;
        let Some(key) = args.get("key").and_then(Value::as_str) else {
            return self.run(Ok(aql)).await;
        };
        match self.engine.query(&aql, None).await {
            Err(e) if e.to_string().contains("key_exists") => {
                let mut v = self.run(remember_update(args, key)).await?;
                if let Some(m) = v.as_object_mut() {
                    m.insert("updated_existing_key".into(), json!(key));
                }
                Ok(v)
            }
            other => other.map_err(|e| format!("Query failed: {e}\nAQL: {aql}")),
        }
    }

    async fn run(&self, aql: Result<String, String>) -> Result<Value, String> {
        let aql = aql?;
        self.engine.query(&aql, None).await.map_err(|e| format!("Query failed: {e}\nAQL: {aql}"))
    }
}

fn initialize(params: Option<&Value>) -> Value {
    let requested = params.and_then(|p| p.get("protocolVersion")).and_then(Value::as_str);
    let version = requested
        .filter(|v| PROTOCOL_VERSIONS.contains(v))
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": {
            "tools": { "listChanged": false },
            "resources": { "listChanged": false },
            "prompts": { "listChanged": false }
        },
        "serverInfo": { "name": "adb", "version": env!("CARGO_PKG_VERSION") },
        "instructions": "ADB is agent memory. Use remember / recall / forget / link for everyday memory, subscribe / publish / next_events / ack for topics shared between agents, or aql_query for anything in AQL. Read adb://guide/memory first."
    })
}

fn obj(args: &Value, name: &str) -> serde_json::Map<String, Value> {
    args.get(name).and_then(Value::as_object).cloned().unwrap_or_default()
}

/// ClawDB `store_memory`: `STORE INTO <type> (fields) [SCOPE s] [NAMESPACE "ns"]`.
fn store_memory(args: &Value) -> Result<String, String> {
    let mt = memory_type(args, "memory_type", None)?;
    let data = args.get("data").and_then(Value::as_object).ok_or("Missing or invalid: data (must be object)")?;
    if data.is_empty() {
        return Err("data must not be empty".into());
    }
    let mut q = format!("STORE INTO {mt} {}", payload(data)?);
    if let Some(scope) = args.get("scope").and_then(Value::as_str) {
        match scope {
            "private" | "shared" | "cluster" => q.push_str(&format!(" SCOPE {scope}")),
            other => return Err(format!("Invalid scope: {other}")),
        }
    }
    if let Some(ns) = args.get("namespace").and_then(Value::as_str) {
        q.push_str(&format!(" NAMESPACE {}", quote(ns)));
    }
    Ok(q)
}

/// ClawDB `recall_memory`: `RECALL FROM <type> [WHERE …] [ORDER BY …] [LIMIT n]`.
fn recall_memory(args: &Value) -> Result<String, String> {
    let mt = memory_type(args, "memory_type", None)?;
    let mut q = format!("RECALL FROM {mt}{}", where_eq(&obj(args, "filter"))?);
    if let Some(order) = args.get("order_by").and_then(Value::as_str) {
        let desc = args.get("order_desc").and_then(Value::as_bool).unwrap_or(false);
        q.push_str(&format!(" ORDER BY {} {}", tools::field(order)?, if desc { "DESC" } else { "ASC" }));
    }
    if let Some(limit) = args.get("limit").and_then(Value::as_u64) {
        q.push_str(&format!(" LIMIT {limit}"));
    }
    Ok(q)
}

fn str_arg<'a>(args: &'a Value, name: &str) -> Result<&'a str, String> {
    args.get(name).and_then(Value::as_str).ok_or(format!("Missing required: {name}"))
}

/// `SUBSCRIBE TOPIC "t" | SCOPE "s" [KINDS …] [WHERE …] [FROM OFFSET n]`.
fn subscribe(args: &Value) -> Result<String, String> {
    let mut q = match (args.get("topic").and_then(Value::as_str), args.get("scope").and_then(Value::as_str)) {
        (Some(t), None) => format!("SUBSCRIBE TOPIC {}", quote(t)),
        (None, Some(s)) => format!("SUBSCRIBE SCOPE {}", quote(s)),
        _ => return Err("subscribe needs exactly one of topic or scope".into()),
    };
    if let Some(kinds) = args.get("kinds").and_then(Value::as_array).filter(|k| !k.is_empty()) {
        let kinds: Vec<&str> = kinds.iter().filter_map(Value::as_str).collect();
        if let Some(bad) = kinds.iter().find(|k| !["entity", "fact", "edge", "episode", "tool", "event"].contains(k)) {
            return Err(format!("unknown kind {bad}"));
        }
        q.push_str(&format!(" KINDS {}", kinds.join(", ")));
    }
    let conds: Vec<String> = obj(args, "filter")
        .iter()
        .map(|(k, v)| Ok(format!("{} = {}", tools::field(k)?, literal(v))))
        .collect::<Result<_, String>>()?;
    if !conds.is_empty() {
        q.push_str(&format!(" WHERE {}", conds.join(" AND ")));
    }
    if let Some(n) = args.get("from_offset").and_then(Value::as_u64) {
        q.push_str(&format!(" FROM OFFSET {n}"));
    }
    Ok(q)
}

/// `PUBLISH TOPIC "t" (fields) [TTL d]`.
fn publish(args: &Value) -> Result<String, String> {
    let topic = str_arg(args, "topic")?;
    let fields = obj(args, "fields");
    if fields.is_empty() {
        return Err("publish needs non-empty fields".into());
    }
    let mut q = format!("PUBLISH TOPIC {} {}", quote(topic), payload(&fields)?);
    if let Some(ttl) = args.get("ttl").and_then(Value::as_str) {
        let ok = ttl.len() > 1 && ttl[..ttl.len() - 1].chars().all(|c| c.is_ascii_digit()) && ttl.ends_with(['s', 'm', 'h', 'd']);
        if !ok {
            return Err(format!("ttl must look like 30s, 15m, 1h or 7d, not {ttl:?}"));
        }
        q.push_str(&format!(" TTL {ttl}"));
    }
    Ok(q)
}

fn ack(args: &Value) -> Result<String, String> {
    let offset = args.get("offset").and_then(Value::as_u64).ok_or("Missing required: offset")?;
    Ok(format!("ACK {} OFFSET {offset}", quote(str_arg(args, "subscription")?)))
}

fn unsubscribe(args: &Value) -> Result<String, String> {
    Ok(format!("UNSUBSCRIBE {}", quote(str_arg(args, "subscription")?)))
}

fn remember(args: &Value) -> Result<String, String> {
    let mt = memory_type(args, "memory_type", Some("SEMANTIC"))?;
    let content = args.get("content").and_then(Value::as_str).ok_or("Missing required: content")?;
    let mut fields = obj(args, "fields");
    fields.insert("content".into(), json!(content));
    if let Some(key) = args.get("key").and_then(Value::as_str) {
        fields.insert("key".into(), json!(key));
    }
    let mut q = format!("STORE INTO {mt} {}", payload(&fields)?);
    if let Some(ns) = args.get("namespace").and_then(Value::as_str) {
        q.push_str(&format!(" NAMESPACE {}", quote(ns)));
    }
    Ok(q)
}

/// `remember` on a key in use: `UPDATE INTO <type> WHERE key = … (content, fields)`.
fn remember_update(args: &Value, key: &str) -> Result<String, String> {
    let mt = memory_type(args, "memory_type", Some("SEMANTIC"))?;
    let content = args.get("content").and_then(Value::as_str).ok_or("Missing required: content")?;
    let mut fields = obj(args, "fields");
    fields.insert("content".into(), json!(content));
    Ok(format!("UPDATE INTO {mt} WHERE key = {} {}", quote(key), payload(&fields)?))
}

fn recall(args: &Value) -> Result<String, String> {
    let mt = memory_type(args, "memory_type", Some("SEMANTIC"))?;
    let mut conds: Vec<String> = Vec::new();
    if let Some(text) = args.get("contains").and_then(Value::as_str) {
        conds.push(format!("content CONTAINS {}", quote(text)));
    }
    for (k, v) in obj(args, "filter") {
        conds.push(format!("{} = {}", tools::field(&k)?, literal(&v)));
    }
    let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(10);
    let filter = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
    Ok(format!("RECALL FROM {mt}{filter} LIMIT {limit}"))
}

fn forget(args: &Value) -> Result<String, String> {
    let mt = memory_type(args, "memory_type", None)?;
    let mut filter = obj(args, "filter");
    if let Some(key) = args.get("key").and_then(Value::as_str) {
        filter.insert("key".into(), json!(key));
    }
    if filter.is_empty() {
        return Err("forget needs a key or a filter (refusing to forget everything)".into());
    }
    Ok(format!("FORGET FROM {mt}{}", where_eq(&filter)?))
}

fn link(args: &Value) -> Result<String, String> {
    let from_t = memory_type(args, "from_type", None)?;
    let to_t = memory_type(args, "to_type", None)?;
    let s = |n: &str| args.get(n).and_then(Value::as_str).ok_or(format!("Missing required: {n}"));
    let mut q = format!(
        "LINK FROM {from_t} WHERE key = {} TO {to_t} WHERE key = {} TYPE {}",
        quote(s("from_key")?),
        quote(s("to_key")?),
        quote(s("link_type")?)
    );
    if let Some(w) = args.get("weight").and_then(Value::as_f64) {
        q.push_str(&format!(" WEIGHT {w:?}"));
    }
    Ok(q)
}

fn resource_list() -> Value {
    json!({ "resources": [
        { "uri": GUIDE_URI, "name": "How to use ADB memory", "mimeType": "text/markdown",
          "description": "Memory types, tools and AQL in brief — read this first" },
        { "uri": GRAMMAR_URI, "name": "AQL grammar", "mimeType": "text/plain",
          "description": "The full AQL grammar (pest), v0.5 plus v0.6 draft" }
    ]})
}

fn read_resource(params: Option<&Value>) -> Result<Value, String> {
    let uri = params.and_then(|p| p.get("uri")).and_then(Value::as_str).ok_or("Missing uri")?;
    let (mime, text) = match uri {
        GUIDE_URI => ("text/markdown", GUIDE),
        GRAMMAR_URI => ("text/plain", aql_parser::GRAMMAR),
        other => return Err(format!("Unknown resource: {other}")),
    };
    Ok(json!({ "contents": [{ "uri": uri, "mimeType": mime, "text": text }] }))
}

fn prompt_list() -> Value {
    json!({ "prompts": [{
        "name": "memory_smoke_test",
        "description": "Walk through remember → recall → link → forget to check ADB end to end"
    }]})
}

fn get_prompt(params: Option<&Value>) -> Result<Value, String> {
    match params.and_then(|p| p.get("name")).and_then(Value::as_str) {
        Some("memory_smoke_test") => Ok(json!({
            "description": "ADB memory smoke test",
            "messages": [{ "role": "user", "content": { "type": "text", "text":
                "Test the ADB memory server step by step and report each result:\n\
                 1. remember the fact \"Acme renews on 1 Nov\" with key \"acme-renewal\" and field account = \"acme\".\n\
                 2. remember the event \"Called Acme about renewal\" in EPISODIC with key \"acme-call-1\".\n\
                 3. recall SEMANTIC memories containing \"renews\".\n\
                 4. link the fact to the event with link_type \"discussed_in\".\n\
                 5. run aql_query: RECALL FROM SEMANTIC WHERE key = \"acme-renewal\" WITH LINKS ALL\n\
                 6. forget the fact by key, then recall again and confirm it is gone.\n\
                 7. call get_stats." } }]
        })),
        Some(other) => Err(format!("Unknown prompt: {other}")),
        None => Err("Missing prompt name".into()),
    }
}

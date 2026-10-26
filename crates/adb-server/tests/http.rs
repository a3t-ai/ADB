//! HTTP tests over a real listener: REST, MCP streamable HTTP and MCP over SSE.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use adb_executor::LocalEngine;
use futures::StreamExt;
use serde_json::{json, Value};

async fn start() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let app = adb_server::router(Arc::new(LocalEngine::default()));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn post(url: &str, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new().post(url).json(&body).send().await.unwrap();
    let status = r.status().as_u16();
    let text = r.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

#[tokio::test]
async fn health_and_stats() {
    let base = start().await;
    let h: Value = reqwest::get(format!("{base}/health")).await.unwrap().json().await.unwrap();
    assert_eq!(h["status"], json!("ok"));

    post(&format!("{base}/query"), json!({"query": r#"STORE INTO WORKING (task = "t1")"#})).await;
    let s: Value = reqwest::get(format!("{base}/stats")).await.unwrap().json().await.unwrap();
    assert_eq!(s["success"], json!(true));
    assert_eq!(s["data"]["working"], json!(1));
}

/// Mirrors FlowR's `ClawDbMemory` adapter: `execute_aql` returns `body.data`, then
/// `parse_recall_results` reads `data.records` (falling back to `records`).
#[tokio::test]
async fn query_matches_flowr_clawdb_adapter() {
    let base = start().await;
    let (st, body) = post(
        &format!("{base}/query"),
        json!({"query": r#"STORE INTO SEMANTIC (key = "k1", content = "hello") SCOPE private NAMESPACE "agent-7""#}),
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(body["success"], json!(true));
    assert!(body["execution_time_ms"].is_u64());

    let (_, body) = post(&format!("{base}/query"), json!({"query": r#"RECALL FROM SEMANTIC WHERE key = "k1""#})).await;
    assert_eq!(body["success"], json!(true));
    let data = &body["data"]; // what execute_aql returns
    let records = data["data"]["records"].as_array().expect("data.data.records");
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert!(r["id"].is_string());
    assert_eq!(r["data"]["key"], json!("k1"));
    assert_eq!(r["data"]["content"], json!("hello"));
    assert_eq!(r["memory_type"], json!("SEMANTIC"));
    assert!(r["metadata"]["created_at"].is_string());
}

#[tokio::test]
async fn query_errors_and_variables() {
    let base = start().await;
    let (st, body) = post(&format!("{base}/query"), json!({"query": "NOT AQL"})).await;
    assert_eq!(st, 400);
    assert_eq!(body["success"], json!(false));
    assert!(body["error"].is_string());

    post(&format!("{base}/query"), json!({"query": r#"STORE INTO EPISODIC (pod = "auth")"#})).await;
    post(&format!("{base}/query"), json!({"query": r#"STORE INTO EPISODIC (pod = "payments")"#})).await;
    let (_, body) = post(
        &format!("{base}/query"),
        json!({"query": "RECALL FROM EPISODIC WHERE pod = {p}", "variables": {"p": "auth"}}),
    )
    .await;
    assert_eq!(body["data"]["data"]["count"], json!(1));
}

#[tokio::test]
async fn mcp_streamable_http() {
    let base = start().await;
    let (st, r) = post(&format!("{base}/mcp"), json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}})).await;
    assert_eq!(st, 200);
    assert_eq!(r["result"]["protocolVersion"], json!("2025-06-18"));

    let (st, _) = post(&format!("{base}/mcp"), json!({"jsonrpc": "2.0", "method": "notifications/initialized"})).await;
    assert_eq!(st, 202, "notifications get 202 and no body");

    let (_, r) = post(&format!("{base}/mcp"), json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {"name": "remember", "arguments": {"content": "hello", "key": "h"}}})).await;
    assert!(r["result"]["isError"].is_null(), "{r}");

    let (_, r) = post(&format!("{base}/mcp"), json!([
        {"jsonrpc": "2.0", "id": 3, "method": "ping"},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 4, "method": "tools/list"}
    ])).await;
    assert_eq!(r.as_array().unwrap().len(), 2, "batch: two responses, notification skipped");
}

/// FlowR's `call:mcp`: the resource URL `…/mcp/sse` becomes `…/mcp/message`, posted with no
/// session; it reads `result.content[0].text`. Tool names as A3T's resource lists them.
#[tokio::test]
async fn mcp_message_without_session_flowr_call_mcp() {
    let base = start().await;
    let (st, r) = post(&format!("{base}/mcp/message"), json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "query", "arguments": {"query": "STORE INTO SEMANTIC (key = \"k\", content = \"v\")"}}})).await;
    assert_eq!(st, 200);
    assert!(r["result"]["isError"].is_null(), "{r}");
    let (_, r) = post(&format!("{base}/mcp/message"), json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {"name": "stats", "arguments": {}}})).await;
    let stats: Value = serde_json::from_str(r["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(stats["semantic"], json!(1), "{stats}");

    let (st, _) = post(&format!("{base}/mcp/message?sessionId=nope"), json!({"jsonrpc": "2.0", "id": 3, "method": "ping"})).await;
    assert_eq!(st, 404, "an unknown session is still an error");
}

/// A3T's path: GET /mcp/sse → `endpoint` event → POST there → response on the stream.
#[tokio::test]
async fn mcp_over_sse() {
    let base = start().await;
    let resp = reqwest::get(format!("{base}/mcp/sse")).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();

    async fn next_event(stream: &mut (impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin), buf: &mut String) -> (String, String) {
        loop {
            if let Some(pos) = buf.find("\n\n") {
                let block: String = buf.drain(..pos + 2).collect();
                let mut event = String::new();
                let mut data = String::new();
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        event = v.trim().to_string();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim());
                    }
                }
                if !event.is_empty() || !data.is_empty() {
                    return (event, data);
                }
                continue;
            }
            let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next()).await.unwrap().unwrap().unwrap();
            buf.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    let (event, endpoint) = next_event(&mut stream, &mut buf).await;
    assert_eq!(event, "endpoint");
    assert!(endpoint.starts_with("/mcp/message?sessionId="), "{endpoint}");

    let client = reqwest::Client::new();
    let st = client
        .post(format!("{base}{endpoint}"))
        .json(&json!({"jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "aql_query", "arguments": {"query": r#"STORE INTO WORKING (k = "v")"#}}}))
        .send().await.unwrap().status().as_u16();
    assert_eq!(st, 202);

    let (event, data) = next_event(&mut stream, &mut buf).await;
    assert_eq!(event, "message");
    let msg: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(msg["id"], json!(7));
    assert!(msg["result"]["content"][0]["text"].as_str().unwrap().contains("stored"));

    let st = client.post(format!("{base}/mcp/message?sessionId=nope")).json(&json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
        .send().await.unwrap().status().as_u16();
    assert_eq!(st, 404);
}

/// W3 / G-L1: with a token, every endpoint but /health needs `Authorization:
/// Bearer <token>`.
#[tokio::test]
async fn token_guards_everything_but_health() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let app = adb_server::router_with_token(Arc::new(LocalEngine::default()), Some(Arc::from("s3cret")));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let c = reqwest::Client::new();

    assert_eq!(c.get(format!("{base}/health")).send().await.unwrap().status(), 200, "health stays open");
    let q = json!({"query": r#"STORE INTO WORKING (task = "t1")"#});
    assert_eq!(c.post(format!("{base}/query")).json(&q).send().await.unwrap().status(), 401);
    assert_eq!(c.post(format!("{base}/query")).bearer_auth("wrong").json(&q).send().await.unwrap().status(), 401);
    let mcp = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    assert_eq!(c.post(format!("{base}/mcp")).json(&mcp).send().await.unwrap().status(), 401);
    assert_eq!(c.get(format!("{base}/mcp/sse")).send().await.unwrap().status(), 401);

    let r = c.post(format!("{base}/query")).bearer_auth("s3cret").json(&q).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(c.post(format!("{base}/mcp")).bearer_auth("s3cret").json(&mcp).send().await.unwrap().status(), 200);
}

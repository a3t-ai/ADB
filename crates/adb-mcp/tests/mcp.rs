//! MCP protocol tests against the local engine.

use std::sync::Arc;

use adb_executor::LocalEngine;
use adb_mcp::protocol::JsonRpcRequest;
use adb_mcp::McpServer;
use serde_json::{json, Value};

fn server() -> McpServer {
    McpServer::new(Arc::new(LocalEngine::default()))
}

fn req(id: i64, method: &str, params: Value) -> JsonRpcRequest {
    JsonRpcRequest { jsonrpc: "2.0".into(), id: Some(json!(id)), method: method.into(), params: Some(params) }
}

/// Call a tool; return (is_error, parsed JSON text or raw text).
async fn call(s: &McpServer, name: &str, args: Value) -> (bool, Value) {
    let r = s.handle(req(1, "tools/call", json!({"name": name, "arguments": args}))).await.unwrap();
    let result = r.result.expect("tools/call returns a result");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    let is_error = result.get("isError").and_then(Value::as_bool).unwrap_or(false);
    (is_error, serde_json::from_str(&text).unwrap_or(Value::String(text)))
}

#[tokio::test]
async fn initialize_negotiates_version() {
    let s = server();
    let r = s.handle(req(1, "initialize", json!({"protocolVersion": "2025-03-26", "capabilities": {}}))).await.unwrap();
    let res = r.result.unwrap();
    assert_eq!(res["protocolVersion"], json!("2025-03-26"));
    assert_eq!(res["serverInfo"]["name"], json!("adb"));
    assert!(res["capabilities"]["tools"].is_object());
    assert!(res["capabilities"]["resources"].is_object());

    let r = s.handle(req(2, "initialize", json!({"protocolVersion": "1999-01-01"}))).await.unwrap();
    assert_eq!(r.result.unwrap()["protocolVersion"], json!("2024-11-05"));
}

#[tokio::test]
async fn notifications_get_no_response() {
    let s = server();
    let n = JsonRpcRequest { jsonrpc: "2.0".into(), id: None, method: "notifications/initialized".into(), params: None };
    assert!(s.handle(n).await.is_none());
}

#[tokio::test]
async fn tool_list_has_clawdb_and_llm_tools() {
    let s = server();
    let r = s.handle(req(1, "tools/list", json!({}))).await.unwrap().result.unwrap();
    let names: Vec<&str> = r["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    for t in ["aql_query", "store_memory", "recall_memory", "get_stats", "remember", "recall", "forget", "link"] {
        assert!(names.contains(&t), "missing tool {t}");
    }
}

#[tokio::test]
async fn clawdb_tools_round_trip() {
    let s = server();
    let (err, v) = call(&s, "store_memory", json!({
        "memory_type": "EPISODIC", "data": {"key": "inc-1", "pod": "payments"}, "scope": "private", "namespace": "a3t"
    })).await;
    assert!(!err, "{v}");
    assert_eq!(v["data"]["type"], json!("stored"));

    let (err, v) = call(&s, "recall_memory", json!({"memory_type": "EPISODIC", "filter": {"pod": "payments"}, "limit": 5})).await;
    assert!(!err, "{v}");
    assert_eq!(v["data"]["count"], json!(1));
    assert_eq!(v["data"]["records"][0]["metadata"]["namespace"], json!("a3t"));

    let (err, v) = call(&s, "aql_query", json!({"query": "RECALL FROM EPISODIC WHERE pod = {p}", "variables": {"p": "payments"}})).await;
    assert!(!err, "{v}");
    assert_eq!(v["data"]["count"], json!(1));

    let (err, v) = call(&s, "get_stats", json!({})).await;
    assert!(!err);
    assert_eq!(v["episodic"], json!(1));
}

#[tokio::test]
async fn llm_tools_remember_recall_link_forget() {
    let s = server();
    let (err, v) = call(&s, "remember", json!({"content": "Acme renews on 1 Nov", "key": "acme-renewal", "fields": {"account": "acme"}})).await;
    assert!(!err, "{v}");
    let (err, v) = call(&s, "remember", json!({"content": "Called Acme", "memory_type": "episodic", "key": "acme-call-1"})).await;
    assert!(!err, "{v}");

    let (err, v) = call(&s, "recall", json!({"contains": "renews"})).await;
    assert!(!err, "{v}");
    assert_eq!(v["data"]["count"], json!(1));
    assert_eq!(v["data"]["records"][0]["data"]["account"], json!("acme"));

    let (err, v) = call(&s, "link", json!({
        "from_type": "SEMANTIC", "from_key": "acme-renewal", "to_type": "EPISODIC", "to_key": "acme-call-1",
        "link_type": "discussed_in", "weight": 0.9
    })).await;
    assert!(!err, "link failed: {v}");

    let (err, v) = call(&s, "forget", json!({"memory_type": "SEMANTIC", "key": "acme-renewal"})).await;
    assert!(!err, "{v}");
    let (_, v) = call(&s, "recall", json!({"contains": "renews"})).await;
    assert_eq!(v["data"]["count"], json!(0));
}

#[tokio::test]
async fn shortcut_tools_reject_bad_input() {
    let s = server();
    let (err, _) = call(&s, "recall_memory", json!({"memory_type": "EPISODIC", "filter": {"x = 1 OR y": 2}})).await;
    assert!(err, "field names must not inject AQL");
    let (err, _) = call(&s, "store_memory", json!({"memory_type": "WORKING; FORGET", "data": {"a": 1}})).await;
    assert!(err);
    let (err, _) = call(&s, "forget", json!({"memory_type": "WORKING"})).await;
    assert!(err, "forget without key or filter must refuse");
    let (err, _) = call(&s, "no_such_tool", json!({})).await;
    assert!(err);
    let (err, v) = call(&s, "aql_query", json!({"query": "NOT AQL"})).await;
    assert!(err, "{v}");
}

#[tokio::test]
async fn resources_and_prompts() {
    let s = server();
    let r = s.handle(req(1, "resources/list", json!({}))).await.unwrap().result.unwrap();
    assert_eq!(r["resources"].as_array().unwrap().len(), 2);
    let r = s.handle(req(2, "resources/read", json!({"uri": "adb://aql/grammar"}))).await.unwrap().result.unwrap();
    assert!(r["contents"][0]["text"].as_str().unwrap().contains("recall_stmt"));
    let r = s.handle(req(3, "resources/read", json!({"uri": "adb://guide/memory"}))).await.unwrap().result.unwrap();
    assert!(r["contents"][0]["text"].as_str().unwrap().contains("Memory types"));
    let r = s.handle(req(4, "prompts/get", json!({"name": "memory_smoke_test"}))).await.unwrap().result.unwrap();
    assert!(r["messages"][0]["content"]["text"].as_str().unwrap().contains("remember"));
    let r = s.handle(req(5, "resources/read", json!({"uri": "adb://nope"}))).await.unwrap();
    assert!(r.error.is_some());
}

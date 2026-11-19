//! MCP protocol tests against the local engine.
#![recursion_limit = "256"]

use std::sync::Arc;

use adb_executor::LocalEngine;
use adb_mcp::protocol::JsonRpcRequest;
use adb_mcp::McpServer;
use serde_json::{json, Value};

async fn server() -> McpServer {
    McpServer::new(Arc::new(LocalEngine::temporary().await.unwrap()))
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
    let s = server().await;
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
    let s = server().await;
    let n = JsonRpcRequest { jsonrpc: "2.0".into(), id: None, method: "notifications/initialized".into(), params: None };
    assert!(s.handle(n).await.is_none());
}

#[tokio::test]
async fn tool_list_has_clawdb_and_llm_tools() {
    let s = server().await;
    let r = s.handle(req(1, "tools/list", json!({}))).await.unwrap().result.unwrap();
    let names: Vec<&str> = r["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    for t in ["aql_query", "store_memory", "recall_memory", "get_stats", "remember", "recall", "forget", "link", "unlink", "watch", "watch_changes", "unwatch"] {
        assert!(names.contains(&t), "missing tool {t}");
    }
}

#[tokio::test]
async fn clawdb_tools_round_trip() {
    let s = server().await;
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
    let s = server().await;
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
    let s = server().await;
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
    let s = server().await;
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

/// `remember` sets a fact: the same key again updates it (a new version on the
/// durable engine), while `store_memory` (a STORE shortcut) refuses the key (D3 S4).
#[tokio::test]
async fn remember_updates_a_key_in_use_and_store_refuses_it() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let s = McpServer::new(Arc::new(engine));
    let (err, v) = call(&s, "remember", json!({"content": "Acme renews on 1 Nov", "key": "acme", "fields": {"account": "acme"}})).await;
    assert!(!err, "{v}");
    let (err, v) = call(&s, "remember", json!({"content": "Acme renews on 1 Dec", "key": "acme"})).await;
    assert!(!err, "second remember of a key must update it: {v}");
    assert_eq!(v["updated_existing_key"], json!("acme"), "{v}");
    let (_, v) = call(&s, "aql_query", json!({"query": r#"RECALL FROM SEMANTIC WHERE id = "acme""#})).await;
    let rec = &v["data"]["records"][0];
    assert_eq!(rec["data"]["content"], json!("Acme renews on 1 Dec"), "{v}");
    assert_eq!(rec["data"]["account"], json!("acme"), "fields not given are kept: {v}");
    assert_eq!(rec["metadata"]["version"], json!(2), "{v}");

    let (err, v) = call(&s, "store_memory", json!({"memory_type": "SEMANTIC", "data": {"key": "acme", "content": "x"}})).await;
    assert!(err && v.to_string().contains("key_exists"), "{v}");
}

/// `scope` on remember / recall / forget is a named scope (`IN SCOPE`).
#[tokio::test]
async fn llm_tools_take_a_named_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let s = McpServer::new(Arc::new(engine));
    for (key, scope) in [("f1", "agent:1"), ("f2", "agent:2")] {
        let (err, v) = call(&s, "remember", json!({"content": format!("fact {key}"), "key": key, "scope": scope})).await;
        assert!(!err, "{v}");
    }
    let (_, v) = call(&s, "recall", json!({"scope": "agent:1"})).await;
    assert_eq!(v["data"]["count"], json!(1), "{v}");
    let (_, v) = call(&s, "recall", json!({})).await;
    assert_eq!(v["data"]["count"], json!(2), "{v}");
    let (_, v) = call(&s, "forget", json!({"key": "f2", "memory_type": "SEMANTIC", "scope": "agent:1"})).await;
    assert_eq!(v["data"]["count"], json!(0), "forget in another scope: {v}");
}

/// `link` again with the same type sets the weight; `unlink` removes the link and
/// keeps both memories (AQL v0.7).
#[tokio::test]
async fn link_twice_reweights_and_unlink_removes() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let s = McpServer::new(Arc::new(engine));
    call(&s, "remember", json!({"content": "restarting fixes OOM", "key": "fix"})).await;
    call(&s, "remember", json!({"content": "inc", "key": "inc", "memory_type": "episodic"})).await;
    let ends = json!({"from_type": "SEMANTIC", "from_key": "fix", "to_type": "EPISODIC", "to_key": "inc", "link_type": "applied_to"});
    for w in [0.5, 0.8] {
        let mut a = ends.clone();
        a["weight"] = json!(w);
        let (err, v) = call(&s, "link", a).await;
        assert!(!err, "{v}");
    }
    let (_, v) = call(&s, "aql_query", json!({"query": r#"RECALL FROM SEMANTIC WHERE key = "fix" WITH LINKS"#})).await;
    let links = v["data"]["records"][0]["data"]["links"].as_array().cloned().unwrap_or_default();
    assert_eq!(links.len(), 1, "{v}");
    assert!((links[0]["weight"].as_f64().unwrap() - 0.8).abs() < 1e-6, "{v}");
    let (err, v) = call(&s, "unlink", ends).await;
    assert!(!err && v["data"]["count"] == json!(1), "{v}");
    let (_, v) = call(&s, "recall", json!({"memory_type": "EPISODIC"})).await;
    assert_eq!(v["data"]["count"], json!(1), "the memories stay: {v}");
}

/// MVP W4: `recall` with `like` ranks by relevance and reports a confidence.
/// Needs the model (`ADB_EMBED_MODEL_DIR`); skipped without it.
#[tokio::test]
async fn recall_like_ranks_by_relevance() {
    if !std::env::var("ADB_EMBED_MODEL_DIR").is_ok_and(|d| std::path::Path::new(&d).join("model.onnx").exists()) {
        return eprintln!("no ADB_EMBED_MODEL_DIR; skipped");
    }
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let s = McpServer::new(Arc::new(engine));
    for (k, c) in [("renewal", "Acme's contract renews on 1 November"), ("oom", "The payments pod runs out of memory"), ("vpn", "Restart the VPN client to reconnect")] {
        call(&s, "remember", json!({"content": c, "key": k})).await;
    }
    let (err, v) = call(&s, "recall", json!({"like": "when is the acme renewal", "limit": 2})).await;
    assert!(!err, "{v}");
    assert_eq!(v["data"]["records"][0]["id"], json!("renewal"), "{v}");
    assert!(v["data"]["records"][0]["metadata"]["confidence"].as_f64().unwrap() > 0.5, "{v}");
    let (err, _) = call(&s, "recall", json!({"like": "x", "contains": "y"})).await;
    assert!(err, "like with contains is refused");
}

/// `watch` → snapshot; `watch_changes` from its offset → the diff; `unwatch` (W6).
#[tokio::test]
async fn watch_tools_follow_a_query() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let s = McpServer::new(Arc::new(engine));
    let (err, v) = call(&s, "watch", json!({"query": r#"RECALL FROM SEMANTIC WHERE account = "acme""#, "debounce_ms": 50})).await;
    assert!(!err, "{v}");
    assert_eq!(v["type"], "snapshot", "{v}");
    let (id, offset) = (v["cq_id"].as_str().unwrap().to_string(), v["offset"].as_u64().unwrap());
    let (err, v) = call(&s, "remember", json!({"content": "Acme renews on 1 Nov", "key": "acme", "fields": {"account": "acme"}})).await;
    assert!(!err, "{v}");
    let (err, v) = call(&s, "watch_changes", json!({"cq_id": id, "from": offset, "wait_seconds": 5})).await;
    assert!(!err, "{v}");
    assert_eq!(v["frames"][0]["type"], "diff", "{v}");
    assert_eq!(v["frames"][0]["added"][0]["id"], "acme", "{v}");
    assert!(v["from"].as_u64().unwrap() > offset);
    let (err, v) = call(&s, "unwatch", json!({"cq_id": id})).await;
    assert!(!err && v["data"]["unsubscribed"] == json!(true), "{v}");
}

/// AQL v0.13 (no facts): `remember` with subject + predicate + object is a LINK of
/// TYPE predicate between the two records (both must exist); with a value, a field.
#[tokio::test]
async fn remember_stores_facts() {
    let s = server().await;
    for k in ["person:ada", "org:acme"] {
        let (err, v) = call(&s, "aql_query", json!({"query": format!(r#"STORE INTO SEMANTIC (key = "{k}")"#)})).await;
        assert!(!err, "{v}");
    }
    let (err, v) = call(&s, "remember", json!({"content": "Ada works at Acme", "subject": "person:ada", "predicate": "works_at", "object": "org:acme"})).await;
    assert!(!err, "{v}");
    let (err, v) = call(&s, "remember", json!({"content": "Ada is 36", "subject": "person:ada", "predicate": "age", "value": 36})).await;
    assert!(!err, "{v}");
    let (err, v) = call(&s, "aql_query", json!({"query": r#"RECALL FROM SEMANTIC WHERE key = "person:ada" FOLLOW LINKS TYPE "works_at""#})).await;
    assert!(!err, "{v}");
    assert_eq!(v["data"]["count"], json!(1), "{v}");
    assert_eq!(v["data"]["records"][0]["data"]["key"], json!("org:acme"), "{v}");
    let (err, v) = call(&s, "aql_query", json!({"query": r#"RECALL FROM SEMANTIC WHERE key = "person:ada""#})).await;
    assert!(!err && v["data"]["records"][0]["data"]["age"] == json!(36), "{v}");
    // An end that doesn't exist: refused (no silent stubs).
    let (err, _) = call(&s, "remember", json!({"content": "x", "subject": "person:nobody", "predicate": "p", "object": "org:acme"})).await;
    assert!(err);
    // A fact is SEMANTIC.
    let (err, _) = call(&s, "remember", json!({"content": "x", "memory_type": "episodic", "subject": "a", "predicate": "p", "object": "b"})).await;
    assert!(err);
}

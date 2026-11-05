//! DB-5, DB-6, DB-13 over Flight: a transaction is one record across tables; columns
//! can be added; an append-only table refuses changes (code `append_only`) but takes
//! purges; the table feed follows every change in order, from an offset, and waits
//! for the next one.

use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use serde_json::{json, Value};

async fn action(url: &str, kind: &str, body: Value) -> Result<Value, (String, String)> {
    let ch = tonic::transport::Channel::from_shared(url.to_string()).unwrap().connect().await.unwrap();
    let mut c = FlightServiceClient::new(ch);
    match c.do_action(Action { r#type: kind.into(), body: body.to_string().into() }).await {
        Ok(r) => {
            use futures::StreamExt;
            Ok(serde_json::from_slice(&r.into_inner().next().await.unwrap().unwrap().body).unwrap())
        }
        Err(s) => Err((s.metadata().get("adb-code").and_then(|v| v.to_str().ok()).unwrap_or("").to_string(), s.message().to_string())),
    }
}

/// Follow the feed from `from` until `n` changes arrived (the feed trails the commit by
/// the applier, so a reader long-polls, as FlowR does).
async fn follow(url: &str, from: u64, tables: Value, n: usize) -> (Value, u64) {
    let (mut all, mut next) = (Vec::new(), from);
    for _ in 0..20 {
        let r = action(url, "table_changes", json!({ "from": next, "tables": tables, "wait_ms": 2000 })).await.unwrap();
        all.extend(r["changes"].as_array().unwrap().iter().cloned());
        next = r["next"].as_u64().unwrap();
        if all.len() >= n {
            break;
        }
    }
    (Value::Array(all), next)
}

fn ops(changes: &Value) -> Vec<String> {
    changes.as_array().unwrap().iter().map(|c| format!("{}:{}", c["table"].as_str().unwrap(), c["op"].as_str().unwrap())).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn transactions_append_only_and_the_table_feed() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let e = Arc::clone(&engine);
    tokio::spawn(async move {
        let _ = adb_server::serve_flight(std::net::SocketAddr::from(([127, 0, 0, 1], port)), e).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let f = format!("http://127.0.0.1:{port}");

    // A new consumer starts with a reset.
    let first = action(&f, "table_changes", json!({})).await.unwrap();
    assert_eq!(first["reset"], json!(true));
    let from = first["next"].as_u64().unwrap();

    // One transaction across two tables, one of them created in it.
    let t = action(&f, "transaction", json!({ "ops": [
        { "op": "create_table", "table": "agents", "columns": ["id", "name"] },
        { "op": "insert", "table": "agents", "rows": [{ "id": "a1", "name": "Bot" }] },
        { "op": "insert", "table": "agent_versions", "rows": [{ "id": "v1", "agent": "a1" }] },
    ]})).await.unwrap();
    assert_eq!(t["ops"], json!(3));
    let (changes, from) = follow(&f, from, json!([]), 3).await;
    assert_eq!(ops(&changes), ["agents:create_table", "agents:insert", "agent_versions:insert"]);
    let offsets: Vec<u64> = changes.as_array().unwrap().iter().map(|c| c["offset"].as_u64().unwrap()).collect();
    assert!(offsets.windows(2).all(|w| w[0] == w[1]), "one record: {offsets:?}");

    // A refused transaction writes nothing.
    let bad = action(&f, "transaction", json!({ "ops": [
        { "op": "add_columns", "table": "agents", "columns": ["status"] },
        { "op": "update", "table": "agents", "filter": "id = 'a1'", "values": { "status": "live" } },
    ]})).await.unwrap_err();
    assert_eq!(bad.0, "transaction_invalid", "{}", bad.1);

    action(&f, "add_columns", json!({ "table": "agents", "columns": ["status"] })).await.unwrap();
    action(&f, "update", json!({ "table": "agents", "filter": "id = 'a1'", "values": { "status": "live" } })).await.unwrap();

    // Append-only: created so in one call; inserts and purges only.
    action(&f, "create_table", json!({ "table": "a3t_errors", "columns": ["id", "time"], "append_only": true })).await.unwrap();
    action(&f, "transaction", json!({ "ops": [{ "op": "insert", "table": "a3t_errors", "rows": [
        { "id": "e1", "time": "2026-10-01T00:00:00Z" }, { "id": "e2", "time": "2026-11-05T00:00:00Z" }] }] })).await.unwrap();
    for (kind, body) in [
        ("update", json!({ "table": "a3t_errors", "filter": "id = 'e1'", "values": { "time": "x" } })),
        ("delete", json!({ "table": "a3t_errors", "filter": "id = 'e1'" })),
        ("upsert", json!({ "table": "a3t_errors", "key": ["id"], "rows": [{ "id": "e1", "time": "x" }] })),
    ] {
        let (code, msg) = action(&f, kind, body).await.unwrap_err();
        assert_eq!(code, "append_only", "{kind}: {msg}");
    }
    action(&f, "purge", json!({ "table": "a3t_errors", "column": "time", "before": "2026-11-01T00:00:00Z" })).await.unwrap();

    let (changes, next) = follow(&f, from, json!(["agents", "a3t_errors"]), 6).await;
    assert_eq!(
        ops(&changes),
        ["agents:add_columns", "agents:update", "a3t_errors:create_table", "a3t_errors:append_only", "a3t_errors:insert", "a3t_errors:purge"]
    );

    // Long poll: a read with nothing new waits for the next write.
    let f2 = f.clone();
    let waiter = tokio::spawn(async move { action(&f2, "table_changes", json!({ "from": next, "wait_ms": 5000 })).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    action(&f, "transaction", json!({ "ops": [{ "op": "insert", "table": "agents", "rows": [{ "id": "a2", "name": "Two" }] }] })).await.unwrap();
    let r = waiter.await.unwrap();
    assert_eq!(ops(&r["changes"]), ["agents:insert"]);
    assert_eq!(r["changes"][0]["rows"][0]["id"], json!("a2"));
}

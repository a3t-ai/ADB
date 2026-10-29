//! Stage G: ADB over Arrow Flight — platform tables and AQL, as FlowR's `call:adb`
//! uses them, against a durable engine, across a restart.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightDescriptor, Ticket};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use serde_json::{json, Value};
use tonic::transport::Channel;

async fn start(dir: &std::path::Path) -> (FlightServiceClient<Channel>, tokio::task::JoinHandle<()>) {
    let (engine, _) = LocalEngine::durable(dir).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let server = tokio::spawn(async move {
        adb_server::serve_flight(addr, engine).await.unwrap();
    });
    for _ in 0..100 {
        if let Ok(c) = Channel::from_shared(format!("http://{addr}")).unwrap().connect().await {
            return (FlightServiceClient::new(c), server);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Flight server did not start");
}

async fn action(c: &mut FlightServiceClient<Channel>, kind: &str, body: Value) -> Result<Value, tonic::Status> {
    let mut s = c.do_action(Action { r#type: kind.into(), body: body.to_string().into() }).await?.into_inner();
    let r = s.next().await.unwrap()?;
    Ok(serde_json::from_slice(&r.body).unwrap())
}

async fn get(c: &mut FlightServiceClient<Channel>, ticket: Value) -> Result<Vec<Value>, tonic::Status> {
    let s = c.do_get(Ticket { ticket: ticket.to_string().into() }).await?.into_inner();
    let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(s.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))))
        .try_collect()
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
    let mut rows = adb_platform::batches_to_json(&batches);
    rows.sort_by_key(|r| r["id"].as_str().unwrap_or_default().to_string());
    Ok(rows)
}

async fn put(c: &mut FlightServiceClient<Channel>, table: &str, rows: &[(&str, &str, &str)]) -> Value {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("org_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let col = |i: usize| Arc::new(StringArray::from(rows.iter().map(|r| [r.0, r.1, r.2][i]).collect::<Vec<_>>())) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema, vec![col(0), col(1), col(2)]).unwrap();
    let data = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![table.into()])))
        .build(futures::stream::iter(vec![Ok(batch)]))
        .map(|r| r.unwrap());
    let r = c.do_put(data).await.unwrap().into_inner().next().await.unwrap().unwrap();
    serde_json::from_slice(&r.app_metadata).unwrap()
}

fn ids(rows: &[Value]) -> Vec<&str> {
    rows.iter().map(|r| r["id"].as_str().unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn platform_tables_and_aql_over_flight() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut c, server) = start(dir.path()).await;
        let r = action(&mut c, "create_table", json!({ "table": "agent_configs", "schema": { "id": "string", "org_id": "string", "name": "string", "status": "string" } })).await.unwrap();
        assert_eq!((r["created"].as_bool(), r["exists"].as_bool()), (Some(true), Some(false)), "{r}");
        let r = action(&mut c, "create_table", json!({ "table": "agent_configs" })).await.unwrap();
        assert_eq!(r["exists"], true, "create_if_not_exists");
        let r = put(&mut c, "agent_configs", &[("a1", "org-default", "Support"), ("a2", "org-default", "Ops"), ("a3", "org-x", "Sales")]).await;
        assert_eq!(r["inserted"], 3, "{r}");
        assert_eq!(ids(&get(&mut c, json!({ "table": "agent_configs", "filter": r#"org_id = "org-default""# })).await.unwrap()), ["a1", "a2"]);
        assert_eq!(get(&mut c, json!({ "table": "agent_configs", "limit": 1 })).await.unwrap().len(), 1);
        let r = action(&mut c, "update", json!({ "table": "agent_configs", "filter": "id = 'a2'", "values": { "status": "active", "name": "Ops's" } })).await.unwrap();
        assert_eq!(r["updated"], true);
        let a2 = &get(&mut c, json!({ "table": "agent_configs", "filter": "id = 'a2'" })).await.unwrap()[0];
        assert_eq!((a2["status"].as_str(), a2["name"].as_str()), (Some("active"), Some("Ops's")));
        action(&mut c, "delete", json!({ "table": "agent_configs", "filter": "id = 'a1'" })).await.unwrap();
        assert_eq!(ids(&get(&mut c, json!({ "table": "agent_configs" })).await.unwrap()), ["a2", "a3"]);
        // A new table from do_put; list_tables.
        put(&mut c, "audit_log", &[("e1", "org-default", "login")]).await;
        assert_eq!(action(&mut c, "list_tables", json!({})).await.unwrap(), json!(["agent_configs", "audit_log"]));
        // Errors are statuses: a missing table, a bad filter, an unknown column.
        assert_eq!(get(&mut c, json!({ "table": "nope" })).await.unwrap_err().code(), tonic::Code::NotFound);
        assert_eq!(action(&mut c, "delete", json!({ "table": "agent_configs", "filter": "id = = 'x'" })).await.unwrap_err().code(), tonic::Code::InvalidArgument);
        assert_eq!(action(&mut c, "update", json!({ "table": "agent_configs", "filter": "id = 'a2'", "values": { "colour": "teal" } })).await.unwrap_err().code(), tonic::Code::InvalidArgument);
        // AQL rides the same wire, with POST /query's answer.
        let r = action(&mut c, "aql", json!({ "query": r#"STORE INTO WORKING (key = "w1", task = "flight")"# })).await.unwrap();
        assert_eq!(r["success"], true, "{r}");
        let r = action(&mut c, "aql", json!({ "query": "SCAN FROM WORKING" })).await.unwrap();
        assert_eq!(r["data"]["data"]["records"][0]["data"]["task"], "flight", "{r}");
        let r = action(&mut c, "aql", json!({ "query": "FORGET FROM WORKING" })).await.unwrap();
        assert_eq!(r["success"], false, "AQL errors come back in the body: {r}");
        server.abort();
    }
    // Restart: platform tables and memory came back (log + Lance).
    let (mut c, server) = start(dir.path()).await;
    assert_eq!(ids(&get(&mut c, json!({ "table": "agent_configs" })).await.unwrap()), ["a2", "a3"]);
    let r = action(&mut c, "aql", json!({ "query": "SCAN FROM WORKING" })).await.unwrap();
    assert_eq!(r["data"]["data"]["records"][0]["data"]["task"], "flight");
    server.abort();
}

/// W3 / G-L1: with a token, a Flight call without `authorization: Bearer <token>`
/// is refused (unauthenticated); with it, it runs.
#[tokio::test]
async fn flight_token_is_required_when_set() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let server = tokio::spawn(async move {
        adb_server::serve_flight_with_token(addr, engine, Some(Arc::from("s3cret"))).await.unwrap();
    });
    let channel = loop {
        if let Ok(c) = Channel::from_shared(format!("http://{addr}")).unwrap().connect().await {
            break c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let mut open = FlightServiceClient::new(channel.clone());
    let e = action(&mut open, "list_tables", json!({})).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "{e}");

    let mut authed = FlightServiceClient::with_interceptor(channel, |mut req: tonic::Request<()>| {
        req.metadata_mut().insert("authorization", "Bearer s3cret".parse().unwrap());
        Ok(req)
    });
    let r = authed
        .do_action(tonic::Request::new(arrow_flight::Action { r#type: "list_tables".into(), body: b"{}".to_vec().into() }))
        .await;
    assert!(r.is_ok(), "{r:?}");
    server.abort();
}

/// The superadmin's SQL console: read-only SQL over memory and platform tables.
#[tokio::test(flavor = "multi_thread")]
async fn sql_over_memory_and_platform_tables() {
    let dir = tempfile::tempdir().unwrap();
    let (mut c, server) = start(dir.path()).await;
    put(&mut c, "agent_configs", &[("a1", "o1", "Alpha"), ("a2", "o1", "Beta"), ("a3", "o2", "Gamma")]).await;
    for q in [
        r#"STORE INTO SEMANTIC (key = "f1", content = "fact one", n = 1)"#,
        r#"STORE INTO SEMANTIC (key = "f2", content = "fact two", n = 2)"#,
        r#"UPDATE INTO SEMANTIC WHERE key = "f1" (n = 10)"#,
        r#"FORGET FROM SEMANTIC WHERE key = "f2""#,
    ] {
        let r = action(&mut c, "aql", json!({ "query": q })).await.unwrap();
        assert_eq!(r["success"], true, "{q}: {r}");
    }
    let sql = |s: &str| json!({ "sql": s });
    // SQL results have any type (`Utf8View`, numbers): as text, for the row helper.
    async fn get(c: &mut FlightServiceClient<Channel>, ticket: Value) -> Result<Vec<Value>, tonic::Status> {
        let s = c.do_get(Ticket { ticket: ticket.to_string().into() }).await?.into_inner();
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(s.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))))
            .try_collect()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let text: Vec<RecordBatch> = batches
            .iter()
            .map(|b| {
                let cols: Vec<Arc<dyn Array>> = b.columns().iter().map(|c| arrow_cast::cast(c, &DataType::Utf8).unwrap()).collect();
                let fields: Vec<Field> = b.schema().fields().iter().map(|f| Field::new(f.name(), DataType::Utf8, true)).collect();
                RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
            })
            .collect();
        Ok(adb_platform::batches_to_json(&text))
    }

    // A platform table, with a filter, an order and a group.
    let rows = get(&mut c, sql("SELECT id, name FROM agent_configs WHERE org_id = 'o1' ORDER BY id")).await.unwrap();
    assert_eq!(ids(&rows), ["a1", "a2"]);
    let rows = get(&mut c, sql("SELECT org_id AS id, CAST(count(*) AS VARCHAR) AS n FROM agent_configs GROUP BY org_id ORDER BY org_id")).await.unwrap();
    assert_eq!(rows, [json!({ "id": "o1", "n": "2" }), json!({ "id": "o2", "n": "1" })]);

    // Memory, read straight after the writes: current rows only in memory_current,
    // the payload as JSON text, every version in memory.
    let rows = get(&mut c, sql("SELECT key AS id, value FROM memory_current WHERE memory = 'semantic'")).await.unwrap();
    assert_eq!(ids(&rows), ["f1"], "{rows:?}");
    let v: Value = serde_json::from_str(rows[0]["value"].as_str().unwrap()).unwrap();
    assert_eq!(v["n"], 10, "{v}");
    let rows = get(&mut c, sql("SELECT CAST(count(*) AS VARCHAR) AS id FROM memory WHERE key = 'f1'")).await.unwrap();
    assert_eq!(ids(&rows), ["2"], "f1 has two versions");
    let rows = get(&mut c, sql("SELECT id FROM memory_raw WHERE id = '_meta'")).await.unwrap();
    assert_eq!(ids(&rows), ["_meta"]);

    // A join across memory and a platform table, and the table list.
    let rows = get(&mut c, sql("SELECT a.id FROM agent_configs a JOIN memory_current m ON m.key = 'f1' WHERE a.org_id = 'o2'")).await.unwrap();
    assert_eq!(ids(&rows), ["a3"]);
    let rows = get(&mut c, sql("SELECT table_name AS id FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")).await.unwrap();
    assert_eq!(ids(&rows), ["agent_configs", "memory", "memory_current", "memory_raw"]);

    // The limit, and writes refused.
    let rows = get(&mut c, json!({ "sql": "SELECT id FROM agent_configs", "limit": 2 })).await.unwrap();
    assert_eq!(rows.len(), 2);
    for bad in ["INSERT INTO agent_configs VALUES ('x', 'o', 'n')", "DROP TABLE agent_configs", "CREATE TABLE t AS SELECT 1", "DELETE FROM agent_configs"] {
        let e = get(&mut c, sql(bad)).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "{bad}: {e}");
    }
    assert_eq!(get(&mut c, sql("SELECT id FROM agent_configs")).await.unwrap().len(), 3);
    server.abort();
}

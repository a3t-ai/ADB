//! DB-3 end to end, over Flight and HTTP: an org's token reaches its own namespaces only,
//! and a read-only token can't write. Own test binary: it sets
//! `ADB_SCOPED_TOKENS`.

use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use serde_json::{json, Value};
use tonic::metadata::MetadataValue;

async fn action(url: &str, token: &str, kind: &str, body: Value) -> Result<Value, String> {
    let ch = tonic::transport::Channel::from_shared(url.to_string()).unwrap().connect().await.unwrap();
    let mut c = FlightServiceClient::new(ch);
    let mut req = tonic::Request::new(Action { r#type: kind.into(), body: body.to_string().into() });
    req.metadata_mut().insert("authorization", MetadataValue::try_from(format!("Bearer {token}")).unwrap());
    match c.do_action(req).await {
        Ok(r) => {
            use futures::StreamExt;
            Ok(serde_json::from_slice(&r.into_inner().next().await.unwrap().unwrap().body).unwrap())
        }
        Err(s) => Err(s.metadata().get("adb-code").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_stay_in_their_namespaces() {
    std::env::set_var(
        "ADB_SCOPED_TOKENS",
        json!([
            { "name": "acme", "token": "acme-token", "write": ["acme--*"] },
            { "name": "globex", "token": "globex-token", "write": ["globex--*"] },
            { "name": "read-all", "token": "np-token", "read": ["*"] },
        ])
        .to_string(),
    );
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let e = Arc::clone(&engine);
    tokio::spawn(async move {
        let _ = adb_server::serve_flight_with_tokens(std::net::SocketAddr::from(([127, 0, 0, 1], port)), e, Some(Arc::from("root")), None).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let f = format!("http://127.0.0.1:{port}");
    let aql = |token: &'static str, q: &'static str| {
        let f = f.clone();
        async move { action(&f, token, "aql", json!({ "query": q })).await }
    };

    // Each org writes and reads its own.
    assert!(aql("acme-token", r#"STORE INTO SEMANTIC (key = "a", content = "acme secret") NAMESPACE "acme--bot""#).await.is_ok());
    assert!(aql("globex-token", r#"STORE INTO SEMANTIC (key = "g", content = "globex secret") NAMESPACE "globex--bot""#).await.is_ok());
    let own = aql("acme-token", r#"RECALL FROM SEMANTIC NAMESPACE "acme--bot""#).await.unwrap();
    assert!(own.to_string().contains("acme secret"), "{own}");

    // And never the other's: reads, writes, forgets, branches, copies, stats, tables.
    for q in [
        r#"RECALL FROM SEMANTIC NAMESPACE "globex--bot""#,
        r#"STORE INTO SEMANTIC (key = "x", content = "y") NAMESPACE "globex--bot""#,
        r#"FORGET FROM SEMANTIC WHERE key = "g" NAMESPACE "globex--bot""#,
        r#"RECALL FROM SEMANTIC"#,
    ] {
        assert_eq!(aql("acme-token", q).await.unwrap_err(), "forbidden", "{q}");
    }
    for (kind, body) in [
        ("branch_create", json!({ "namespace": "globex--bot", "name": "b" })),
        ("space_stats", json!({ "namespace": "globex--bot" })),
        ("space_records", json!({ "namespace": "globex--bot", "memory": "SEMANTIC" })),
        ("space_changes", json!({ "namespace": "globex--bot" })),
        ("space_clone", json!({ "namespace": "globex--bot", "into": "acme--stolen" })),
        ("layers_set", json!({ "namespace": "acme--bot", "layers": [{ "space": "globex--bot", "mode": "mount", "memories": ["SEMANTIC"] }] })),
        ("branch_list", json!({})),
        ("list_tables", json!({})),
        ("compact", json!({})),
        ("insert", json!({ "table": "t", "rows": [] })),
        ("engine", json!({ "space": "s", "op": "x", "args": {} })),
    ] {
        assert!(action(&f, "acme-token", kind, body).await.is_err(), "{kind}");
    }
    assert!(action(&f, "acme-token", "space_stats", json!({ "namespace": "acme--bot", "max_age_ms": 0 })).await.is_ok());

    // A table read through a ticket is closed too.
    let ch = tonic::transport::Channel::from_shared(f.clone()).unwrap().connect().await.unwrap();
    let mut c = FlightServiceClient::new(ch);
    let mut req = tonic::Request::new(Ticket { ticket: json!({ "table": "anything" }).to_string().into() });
    req.metadata_mut().insert("authorization", MetadataValue::try_from("Bearer acme-token").unwrap());
    assert_eq!(c.do_get(req).await.err().map(|s| s.code()), Some(tonic::Code::PermissionDenied));

    // A read-everything token reads every namespace and writes nothing.
    let read = aql("np-token", r#"RECALL FROM SEMANTIC NAMESPACE "globex--bot""#).await.unwrap();
    assert!(read.to_string().contains("globex secret"), "{read}");
    assert!(action(&f, "np-token", "space_stats", json!({ "namespace": "acme--bot", "max_age_ms": 0 })).await.is_ok());
    assert_eq!(aql("np-token", r#"STORE INTO SEMANTIC (key = "x", content = "y") NAMESPACE "acme--bot""#).await.unwrap_err(), "forbidden");
    assert!(action(&f, "np-token", "branch_create", json!({ "namespace": "acme--bot", "name": "b" })).await.is_err());

    // An unknown token gets nothing; the main token is unchanged.
    assert!(aql("guess", r#"RECALL FROM SEMANTIC NAMESPACE "acme--bot""#).await.is_err());
    assert!(action(&f, "root", "list_tables", json!({})).await.is_ok());

    // HTTP: the same check on /query, and nothing else.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = adb_server::router_with_token(Arc::clone(&engine), Some(Arc::from("root")));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let http = reqwest::Client::new();
    let post = |token: &'static str, q: &'static str| {
        let (http, base) = (http.clone(), base.clone());
        async move { http.post(format!("{base}/query")).bearer_auth(token).json(&json!({ "query": q })).send().await.unwrap().status().as_u16() }
    };
    assert_eq!(post("acme-token", r#"RECALL FROM SEMANTIC NAMESPACE "acme--bot""#).await, 200);
    assert_eq!(post("acme-token", r#"RECALL FROM SEMANTIC NAMESPACE "globex--bot""#).await, 403);
    assert_eq!(post("np-token", r#"STORE INTO SEMANTIC (key = "x", content = "y") NAMESPACE "acme--bot""#).await, 403);
    assert_eq!(post("nobody", r#"RECALL FROM SEMANTIC NAMESPACE "acme--bot""#).await, 401);
    let mcp = http.post(format!("{base}/mcp")).bearer_auth("acme-token").json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })).send().await.unwrap();
    assert_eq!(mcp.status().as_u16(), 403);
    assert_eq!(http.get(format!("{base}/stats")).bearer_auth("acme-token").send().await.unwrap().status().as_u16(), 403);
}

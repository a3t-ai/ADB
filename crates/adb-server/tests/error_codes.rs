//! DB-18: every refusal carries a stable code beside its sentence: in `POST /query`
//! answers (`code`), in the Flight `aql` answer (`code`), and on refused Flight actions
//! (the `adb-code` metadata of the gRPC status).

use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use serde_json::{json, Value};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn query(base: &str, q: &str) -> Value {
    reqwest::Client::new().post(format!("{base}/query")).json(&json!({ "query": q })).send().await.unwrap().json().await.unwrap()
}

/// A Flight action's answer, or its refusal's `adb-code` and message.
async fn action(url: &str, kind: &str, body: Value) -> Result<Value, (String, String)> {
    let ch = tonic::transport::Channel::from_shared(url.to_string()).unwrap().connect().await.unwrap();
    let mut c = FlightServiceClient::new(ch);
    match c.do_action(Action { r#type: kind.into(), body: body.to_string().into() }).await {
        Ok(r) => {
            use futures::StreamExt;
            let first = r.into_inner().next().await.unwrap().unwrap();
            Ok(serde_json::from_slice(&first.body).unwrap())
        }
        Err(s) => Err((
            s.metadata().get("adb-code").and_then(|v| v.to_str().ok()).unwrap_or("").to_string(),
            s.message().to_string(),
        )),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refusals_carry_stable_codes() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = adb_server::router(Arc::clone(&engine));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let port = free_port();
    let e = Arc::clone(&engine);
    tokio::spawn(async move {
        let _ = adb_server::serve_flight(std::net::SocketAddr::from(([127, 0, 0, 1], port)), e).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let flight = format!("http://127.0.0.1:{port}");

    // HTTP /query
    let ok = query(&base, r#"STORE INTO SEMANTIC (key = "k", fact = "a") NAMESPACE "crm""#).await;
    assert_eq!(ok["success"], json!(true));
    assert!(ok.get("code").is_none(), "no code on success: {ok}");
    for (q, want) in [
        (r#"STORE INTO SEMANTIC (key = "k", fact = "b") NAMESPACE "crm""#, "key_exists"),
        (r#"STORE INTO SEMANTIC (fact = "x") NAMESPACE "a|b""#, "namespace_invalid"),
        // AQL v0.13: the parser refuses a FORGET with neither WHERE nor NAMESPACE.
        ("FORGET FROM WORKING", "parse_error"),
        ("RECALL FRM SEMANTIC", "parse_error"),
    ] {
        let a = query(&base, q).await;
        assert_eq!(a["success"], json!(false), "{q}: {a}");
        assert_eq!(a["code"], json!(want), "{q}: {a}");
    }

    // Flight `aql`
    let a = action(&flight, "aql", json!({ "query": r#"STORE INTO SEMANTIC (key = "k", fact = "c") NAMESPACE "crm""# })).await.unwrap();
    assert_eq!(a["code"], json!("key_exists"), "{a}");

    // Flight actions: the status carries `adb-code`
    action(&flight, "branch_create", json!({ "namespace": "crm", "name": "try" })).await.unwrap();
    for (kind, body, want) in [
        ("branch_create", json!({ "namespace": "crm", "name": "try" }), "branch_exists"),
        ("branch_create", json!({ "namespace": "crm", "name": "bad name" }), "branch_name_invalid"),
        ("branch_promote", json!({ "namespace": "crm", "name": "try", "into": "crm" }), "promote_into_self"),
        ("tag_drop", json!({ "namespace": "crm", "name": "v1" }), ""),
        ("layers_set", json!({ "namespace": "crm", "layers": [{ "space": "crm", "mode": "mount", "memories": ["SEMANTIC"] }] }), "layer_invalid"),
        ("no_such_action", json!({}), "unsupported"),
    ] {
        match action(&flight, kind, body).await {
            Ok(v) => assert_eq!(want, "", "{kind} answered {v}, expected {want}"),
            Err((code, msg)) => assert_eq!(code, want, "{kind}: {msg}"),
        }
    }
}

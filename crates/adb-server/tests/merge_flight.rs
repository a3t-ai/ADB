//! DB-10 / DB-19 over Flight, as FlowR calls them: branch_conflicts → branch_merge
//! with resolutions → merge_revert; refusals carry their codes.

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

#[tokio::test(flavor = "multi_thread")]
async fn conflicts_merge_and_revert_over_flight() {
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
    let aql = |q: &'static str| {
        let f = f.clone();
        async move { action(&f, "aql", json!({ "query": q })).await.unwrap() }
    };

    aql(r#"STORE INTO PROCEDURAL (key = "refund", steps = "v1") NAMESPACE "ops""#).await;
    action(&f, "branch_create", json!({ "namespace": "ops", "name": "try" })).await.unwrap();
    aql(r#"UPDATE INTO PROCEDURAL WHERE key = "refund" (steps = "branch") NAMESPACE "ops" BRANCH "try""#).await;
    aql(r#"STORE INTO PROCEDURAL (key = "escalate", steps = "new") NAMESPACE "ops" BRANCH "try""#).await;
    aql(r#"UPDATE INTO PROCEDURAL WHERE key = "refund" (steps = "main") NAMESPACE "ops""#).await;

    let d = action(&f, "branch_conflicts", json!({ "namespace": "ops", "name": "try", "memories": ["PROCEDURAL"] })).await.unwrap();
    assert_eq!(d["conflicts"].as_array().unwrap().len(), 1, "{d}");
    assert_eq!(d["clean"][0]["change"], json!("add"), "{d}");
    let c = &d["conflicts"][0];
    let main_offset = d["main"]["offset"].clone();

    let (code, msg) = action(&f, "branch_merge", json!({ "namespace": "ops", "name": "try", "main_offset": main_offset })).await.unwrap_err();
    assert_eq!(code, "merge_conflicts", "{msg}");

    let m = action(&f, "branch_merge", json!({
        "namespace": "ops", "name": "try", "main_offset": main_offset, "merged_by": "ops@a3t",
        "resolutions": [{ "lineage": c["lineage"], "memory": c["memory"], "take": "edit", "value": { "key": "refund", "steps": "edited" } }],
    })).await.unwrap();
    assert_eq!((m["changed"].clone(), m["added"].clone()), (json!(1), json!(1)), "{m}");
    let now = aql(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "ops" LIMIT 10"#).await;
    let s = now.to_string();
    assert!(s.contains("edited") && s.contains("new") && !s.contains("\"main\""), "{now}");

    let r = action(&f, "merge_revert", json!({ "namespace": "ops", "merge_id": m["merge_id"], "reverted_by": "ops@a3t" })).await.unwrap();
    assert_eq!(r["reverted"], json!(2), "{r}");
    let (code, _) = action(&f, "merge_revert", json!({ "namespace": "ops", "merge_id": m["merge_id"] })).await.unwrap_err();
    assert_eq!(code, "merge_not_found");
}

//! W9.10: embedding settings. A provider is checked with a sample embedding,
//! saved in the data directory and applied at the next start: another model or
//! dimension clears the vectors and the backfill embeds every record again.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

/// An OpenAI-compatible endpoint whose vectors (dimension 4) depend on the text and
/// on the model name, so two models give different vectors.
/// Texts embedded per model, as the endpoint saw them.
type Seen = Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>;

async fn mock_endpoint() -> (String, Seen) {
    use axum::{routing::post, Json, Router};
    let seen: Seen = Default::default();
    let counter = Arc::clone(&seen);
    let app = Router::new().route(
        "/v1/embeddings",
        post(move |Json(body): Json<Value>| async move {
            *counter.lock().unwrap().entry(body["model"].as_str().unwrap_or("").to_string()).or_default() += body["input"].as_array().map_or(0, |a| a.len());
            let salt = body["model"].as_str().unwrap_or("").len() as f32;
            let data: Vec<Value> = body["input"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let t = t.as_str().unwrap();
                    let has = |w: &str| if t.contains(w) { 1.0 } else { 0.0 };
                    json!({ "index": i, "embedding": [has("renew") + 0.01, has("memory"), has("deploy"), 0.1 * salt] })
                })
                .collect();
            Json(json!({ "data": data }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/v1"), seen)
}

async fn serve(engine: Arc<LocalEngine>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let app = adb_server::router(engine);
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), task)
}

async fn wait_embedded(engine: &LocalEngine) -> Value {
    for _ in 0..100 {
        let e = engine.embedding().await.unwrap();
        if e["pending"] == 0 {
            return e;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("backfill didn't finish: {}", engine.embedding().await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_provider_is_checked_saved_and_applied_at_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (url, seen) = mock_endpoint().await;
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        let engine = Arc::new(engine);
        for q in [
            r#"STORE INTO SEMANTIC (key = "acme", content = "Acme renews in November")"#,
            r#"STORE INTO SEMANTIC (key = "oom", content = "the pod ran out of memory")"#,
            r#"STORE INTO EPISODIC (key = "d1", content = "deploy of the billing service")"#,
        ] {
            engine.query(q, None).await.unwrap();
        }
        let (base, server) = serve(Arc::clone(&engine)).await;
        let http = reqwest::Client::new();
        let get = || async { http.get(format!("{base}/admin/embedding")).send().await.unwrap().json::<Value>().await.unwrap() };
        assert_eq!(get().await["data"]["source"], "default");

        // Wrong dimension: refused, nothing saved.
        let bad = json!({ "provider": "openai", "model": "mock-a", "dimension": 8, "url": url });
        let r: Value = http.put(format!("{base}/admin/embedding")).json(&bad).send().await.unwrap().json().await.unwrap();
        assert_eq!(r["success"], false, "{r}");
        assert!(r["error"].as_str().unwrap().contains("4 dimensions"), "{r}");
        assert_eq!(get().await["data"]["source"], "default");

        let good = json!({ "provider": "openai", "model": "mock-a", "dimension": 4, "url": url });
        let r: Value = http.put(format!("{base}/admin/embedding")).json(&good).send().await.unwrap().json().await.unwrap();
        assert_eq!(r["success"], true, "{r}");
        assert_eq!(r["data"]["restart"], true);
        assert_eq!(r["data"]["reembed"], true);
        tokio::time::timeout(Duration::from_secs(3), adb_executor::restart_requested()).await.expect("restart requested");
        // What `adb serve` does then: stop serving and close the engine.
        server.abort();
        let _ = server.await;
    }

    // The restart: 4-dimension vectors from the endpoint for every record.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let e = wait_embedded(&engine).await;
    assert_eq!(e["source"], "settings");
    assert_eq!(e["active"], json!({ "model": "mock-a", "dimension": 4 }));
    let vars = std::collections::HashMap::from([("q".to_string(), json!("when does it renew?"))]);
    let r = engine.query("RECALL FROM SEMANTIC LIKE $q LIMIT 1", Some(&vars)).await.unwrap();
    assert_eq!(r["data"]["records"][0]["id"], "acme", "{r}");

    // Another model of the same size: vectors cleared at open, embedded again.
    adb_embed::EmbedConfig { model: "mock-bb".into(), ..serde_json::from_value(e["settings"].clone()).unwrap() }.save(dir.path()).unwrap();
    drop(engine);
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let e = wait_embedded(&engine).await;
    assert_eq!(e["active"]["model"], "mock-bb");
    // mock-a's vectors were cleared: all three records went to mock-bb (plus the
    // sample a PUT would embed; none here, the file was edited directly).
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.get("mock-bb").copied(), Some(3), "{seen:?}");
}

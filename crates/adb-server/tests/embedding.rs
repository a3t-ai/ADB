//! Embedding settings (W9.10; applied in place 20 Nov 2026). A provider is checked
//! with a sample embedding, applied at once without a restart and saved in the data
//! directory: another model or dimension makes every record's vector again.

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
async fn a_provider_is_checked_applied_in_place_and_kept_across_a_restart() {
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
        let put = |v: Value| {
            let (http, base) = (http.clone(), base.clone());
            async move { http.put(format!("{base}/admin/embedding")).json(&v).send().await.unwrap().json::<Value>().await.unwrap() }
        };
        assert_eq!(get().await["data"]["source"], "default");

        // `openai` needs the key's variable; a wrong dimension is refused; nothing saved.
        let r = put(json!({ "provider": "openai", "model": "text-embedding-3-small", "dimension": 384 })).await;
        assert!(r["error"].as_str().unwrap().contains("api_key_env"), "{r}");
        let r = put(json!({ "provider": "custom", "model": "mock-a", "dimension": 8, "url": url })).await;
        assert_eq!(r["success"], false, "{r}");
        assert!(r["error"].as_str().unwrap().contains("4 dimensions"), "{r}");
        assert_eq!(get().await["data"]["source"], "default");

        // Applied in place: no restart, every record gets mock-a's vector.
        let r = put(json!({ "provider": "custom", "model": "mock-a", "dimension": 4, "url": url })).await;
        assert_eq!(r["success"], true, "{r}");
        assert_eq!((r["data"]["restart"].clone(), r["data"]["reembed"].clone()), (json!(false), json!(true)));
        let e = wait_embedded(&engine).await;
        assert_eq!(e["source"], "settings");
        assert_eq!((e["active"]["provider"].clone(), e["active"]["model"].clone()), (json!("custom"), json!("mock-a")));
        let vars = std::collections::HashMap::from([("q".to_string(), json!("when does it renew?"))]);
        let r = engine.query("RECALL FROM SEMANTIC LIKE $q LIMIT 1", Some(&vars)).await.unwrap();
        assert_eq!(r["data"]["records"][0]["id"], "acme", "{r}");

        // Another model of the same size, in place: every record embedded again.
        let r = put(json!({ "provider": "custom", "model": "mock-bb", "dimension": 4, "url": url })).await;
        assert_eq!(r["data"]["reembed"], true, "{r}");
        let e = wait_embedded(&engine).await;
        assert_eq!(e["active"]["model"], "mock-bb");
        let r = engine.query("RECALL FROM SEMANTIC LIKE $q LIMIT 1", Some(&vars)).await.unwrap();
        assert_eq!(r["data"]["records"][0]["id"], "acme", "{r}");
        server.abort();
        let _ = server.await;
    }
    // Three records, the setting's sample check and the LIKE's text.
    let before = seen.lock().unwrap().get("mock-bb").copied();
    assert_eq!(before, Some(5), "{:?}", seen.lock().unwrap());

    // After a restart: the saved setting, the vectors read from their files (nothing
    // embedded again).
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let e = wait_embedded(&engine).await;
    assert_eq!((e["source"].clone(), e["active"]["model"].clone()), (json!("settings"), json!("mock-bb")));
    let vars = std::collections::HashMap::from([("q".to_string(), json!("the pod is out of memory"))]);
    let r = engine.query("RECALL FROM SEMANTIC LIKE $q LIMIT 1", Some(&vars)).await.unwrap();
    assert_eq!(r["data"]["records"][0]["id"], "oom", "{r}");
    assert_eq!(seen.lock().unwrap().get("mock-bb").copied(), Some(6), "only the LIKE text is embedded");
}

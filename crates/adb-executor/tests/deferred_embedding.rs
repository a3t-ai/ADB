//! `ADB_EMBED_DEFER=episodic`: STORE EPISODIC doesn't wait for its vector; the vectors
//! follow from the deferred embedder, and LIKE then ranks the episodes by meaning.
//! Its own test binary: the setting is read once per process.
//! Needs the model (`ADB_EMBED_MODEL_DIR`); skipped without it.

use std::collections::HashMap;

use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

fn have_model() -> bool {
    let ok = std::env::var("ADB_EMBED_MODEL_DIR").is_ok_and(|d| std::path::Path::new(&d).join("model.onnx").exists());
    if !ok {
        eprintln!("no ADB_EMBED_MODEL_DIR; skipped");
    }
    ok
}

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

async fn like(engine: &LocalEngine, aql: &str, text: &str) -> Value {
    let vars: HashMap<String, Value> = [("q".to_string(), json!(text))].into();
    engine.query(aql, Some(&vars)).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn keys(v: &Value) -> Vec<String> {
    v["data"]["records"].as_array().unwrap().iter().map(|r| {
        let id = r["id"].as_str().unwrap();
        id.split_once("|~").map(|(_, k)| k).unwrap_or(id).to_string()
    }).collect()
}

const FACTS: &[(&str, &str)] = &[
    ("acme-renewal", "Acme's contract renews on the first of November"),
    ("acme-owner", "Priya owns the Acme account"),
    ("payments-oom", "The payments pod keeps running out of memory"),
    ("invoice-day", "Invoices are sent on the third of every month"),
    ("vpn-reset", "To reset the VPN, restart the client and sign in again"),
];

/// With `ADB_EMBED_DEFER=episodic` STORE EPISODIC doesn't wait for its vector:
/// the vectors follow from the deferred embedder, and LIKE then ranks the episodes by
/// meaning as it ranks facts.
#[tokio::test]
async fn episodes_get_their_vectors_after_the_store() {
    if !have_model() {
        return;
    }
    std::env::set_var("ADB_EMBED_DEFER", "episodic");
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for (k, c) in FACTS {
        q(&engine, &format!(r#"STORE INTO EPISODIC (key = "{k}", content = "{c}") NAMESPACE "crm""#)).await;
    }
    engine.vectors_caught_up(std::time::Duration::from_secs(60)).await.expect("the deferred vectors arrive");
    for (question, want) in [("When is the Acme deal up for renewal?", "acme-renewal"), ("service crashes with OOM", "payments-oom"), ("my VPN connection is broken", "vpn-reset")] {
        let r = like(&engine, r#"RECALL FROM EPISODIC LIKE $q NAMESPACE "crm" LIMIT 3"#, question).await;
        assert_eq!(keys(&r).first().map(String::as_str), Some(want), "{question}: {:?}", keys(&r));
    }
}

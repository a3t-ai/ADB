//! MVP W4: relevance recall — `RECALL … LIKE $q` over local embeddings and words.
//! Needs the model (`ADB_EMBED_MODEL_DIR`, tests/fetch-model.sh); skipped without it.

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

#[tokio::test]
async fn like_ranks_by_meaning_and_honours_the_modifiers() {
    if !have_model() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for (k, c) in FACTS {
            q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "{k}", content = "{c}") NAMESPACE "crm""#)).await;
        }
        q(&engine, r#"STORE INTO SEMANTIC (key = "other-ns", content = "Acme renews in November") NAMESPACE "elsewhere""#).await;
        // Writes don't wait for their vectors: LIKE ranks what has one.
        engine.vectors_caught_up(std::time::Duration::from_secs(60)).await.unwrap();

        // Paraphrases with few shared words find the right fact first.
        for (question, want) in [
            ("When is the Acme deal up for renewal?", "acme-renewal"),
            ("Who is responsible for the Acme customer?", "acme-owner"),
            ("service crashes with OOM", "payments-oom"),
            ("billing date", "invoice-day"),
            ("my VPN connection is broken", "vpn-reset"),
        ] {
            let r = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "crm" LIMIT 3"#, question).await;
            if std::env::var("RECALL_DEBUG").is_ok() {
                for rec in r["data"]["records"].as_array().unwrap() {
                    eprintln!("{question:45} {:15} {:.3}", rec["id"].as_str().unwrap(), rec["metadata"]["confidence"].as_f64().unwrap_or(0.0));
                }
            }
            assert_eq!(keys(&r).first().map(String::as_str), Some(want), "{question}: {:?}", keys(&r));
            let conf = r["data"]["records"][0]["metadata"]["confidence"].as_f64().unwrap();
            assert!(conf > 0.5 && conf <= 1.0, "confidence {conf}");
        }
        // LIMIT and NAMESPACE.
        let r = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "crm" LIMIT 2"#, "Acme").await;
        assert_eq!(keys(&r).len(), 2);
        assert!(!keys(&r).contains(&"other-ns".to_string()));
        // CONFIDENCE cuts the unrelated ones.
        let all = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "crm" LIMIT 10"#, "Acme contract renewal").await;
        let strict = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "crm" CONFIDENCE 0.75 LIMIT 10"#, "Acme contract renewal").await;
        assert!(keys(&strict).len() < keys(&all).len(), "{:?} vs {:?}", keys(&strict), keys(&all));
        assert_eq!(keys(&strict).first().map(String::as_str), Some("acme-renewal"));
        // UPDATE re-embeds.
        q(&engine, r#"UPDATE INTO SEMANTIC WHERE key = "vpn-reset" (content = "The office printer needs new toner")"#).await;
        engine.vectors_caught_up(std::time::Duration::from_secs(60)).await.unwrap();
        let r = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "crm" LIMIT 1"#, "printer out of ink").await;
        assert_eq!(keys(&r), ["vpn-reset"]);
    }
    // The vectors are in the log and the table: a restart ranks the same, without
    // re-embedding stored rows.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "crm" LIMIT 1"#, "When is the Acme deal up for renewal?").await;
    assert_eq!(keys(&r), ["acme-renewal"]);
    // IN SCOPE and a vector query.
    q(&engine, r#"STORE INTO SEMANTIC (key = "scoped", content = "Acme renews in November") IN SCOPE "team:crm""#).await;
    engine.vectors_caught_up(std::time::Duration::from_secs(60)).await.unwrap();
    let r = like(&engine, r#"RECALL FROM SEMANTIC LIKE $q IN SCOPE "team:crm" LIMIT 5"#, "Acme").await;
    assert_eq!(keys(&r), ["team:crm~scoped"], "a named scope's ids carry it (E-L12)");
}

#[tokio::test]
async fn like_without_a_value_or_on_working_is_an_error() {
    if !have_model() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert!(engine.query("RECALL FROM SEMANTIC LIKE $q", None).await.is_err(), "unbound $q");
    let vars: HashMap<String, Value> = [("q".to_string(), json!("x"))].into();
    assert!(engine.query("RECALL FROM WORKING LIKE $q", Some(&vars)).await.is_err());
}

//! History reads (L9): `AS OF` and `WITH HISTORY`. (AQL v0.13 has no FACT and no
//! `VALID AT`: validity, where wanted, is ordinary fields.)

use std::collections::HashMap;
use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn records(v: &Value) -> Vec<Value> {
    v["data"]["records"].as_array().cloned().unwrap_or_default()
}

/// `AS OF`: each record as it was then (an UPDATE, a later STORE); `$t` and a
/// timestamp; PROCEDURAL too. History reads refuse LIKE and links.
#[tokio::test]
async fn as_of_reads_past_versions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let employers = |aql: String| {
        let engine = &engine;
        async move {
            let mut v: Vec<String> = records(&q(engine, &aql).await).iter().filter_map(|r| r["data"]["employer"].as_str().map(String::from)).collect();
            v.sort();
            v
        }
    };
    q(&engine, r#"STORE INTO SEMANTIC (key = "person:ada", employer = "org:acme")"#).await;
    q(&engine, r#"STORE INTO PROCEDURAL (pattern_id = "deploy", pattern = "deploy app", steps = ["build"])"#).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t1 = chrono::Utc::now().to_rfc3339();
    tokio::time::sleep(Duration::from_millis(20)).await;
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE key = "person:ada" (employer = "org:globex")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "person:bob", employer = "org:acme")"#).await;
    q(&engine, r#"UPDATE INTO PROCEDURAL WHERE pattern_id = "deploy" (steps = ["build", "ship"])"#).await;

    let at = |aql: &str| aql.replace("T1", &t1);
    let hour_ago = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    assert_eq!(employers(r#"RECALL FROM SEMANTIC WHERE employer != """#.into()).await, ["org:acme", "org:globex"]);
    assert_eq!(employers(at(r#"RECALL FROM SEMANTIC WHERE employer != "" AS OF "T1""#)).await, ["org:acme"]);
    let vars: HashMap<String, Value> = [("t".to_string(), Value::String(t1.clone()))].into();
    let r = engine.query(r#"RECALL FROM SEMANTIC WHERE key = "person:ada" AS OF $t"#, Some(&vars)).await.unwrap();
    assert_eq!(records(&r).len(), 1, "{r}");
    assert_eq!(records(&r)[0]["data"]["employer"], "org:acme");
    assert!(employers(format!(r#"RECALL FROM SEMANTIC WHERE employer != "" AS OF "{hour_ago}""#)).await.is_empty());
    // bob didn't exist at T1.
    let r = q(&engine, &at(r#"RECALL FROM SEMANTIC WHERE key = "person:bob" AS OF "T1""#)).await;
    assert!(records(&r).is_empty(), "{r}");

    let r = q(&engine, &at(r#"RECALL FROM PROCEDURAL WHERE pattern_id = "deploy" AS OF "T1""#)).await;
    assert_eq!(records(&r)[0]["metadata"]["version"], 1, "{r}");
    let r = q(&engine, r#"RECALL FROM PROCEDURAL WHERE pattern_id = "deploy" WITH HISTORY"#).await;
    assert_eq!(records(&r).len(), 2, "{r}");
    // AS OF bounds WITH HISTORY to the versions recorded by then.
    let r = q(&engine, &at(r#"RECALL FROM SEMANTIC WHERE key = "person:ada" WITH HISTORY AS OF "T1""#)).await;
    assert_eq!(records(&r).len(), 1, "{r}");
    assert!(records(&r)[0]["data"]["_replaced_at"].is_null(), "not replaced by T1: {r}");
    // Aggregates count versions.
    let r = q(&engine, r#"RECALL FROM SEMANTIC WHERE employer != "" WITH HISTORY AGGREGATE COUNT(*) AS n"#).await;
    assert_eq!(r["data"]["values"]["n"], 3, "{r}");

    for bad in [
        format!(r#"RECALL FROM SEMANTIC LIKE $q AS OF "{hour_ago}""#),
        format!(r#"RECALL FROM SEMANTIC WHERE key = "person:ada" AS OF "{hour_ago}" FOLLOW LINKS"#),
        r#"RECALL FROM SEMANTIC WHERE key = "person:ada" AS OF "yesterday""#.to_string(),
        r#"RECALL FROM SEMANTIC WHERE key = "person:ada" AS OF $unbound"#.to_string(),
        format!(r#"RECALL FROM WORKING WHERE a = 1 AS OF "{hour_ago}""#),
    ] {
        assert!(engine.query(&bad, None).await.is_err(), "{bad} should be refused");
    }
}


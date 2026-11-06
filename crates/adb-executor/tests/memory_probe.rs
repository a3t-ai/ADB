//! Which work grows the heap, per kind of operation (leader memory, CA-53). Not a pass/fail
//! test: run by hand, one kind at a time, as each needs a fresh process:
//!   MEM_KIND=table_write cargo test -p adb-executor --test memory_probe -- --ignored --nocapture

use adb_core::log::Effect;
use adb_executor::{Engine, LocalEngine};
use serde_json::json;

fn anon_mib() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    s.lines().find(|l| l.starts_with("RssAnon")).and_then(|l| l.split_whitespace().nth(1)).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0) / 1024
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn probe() {
    let kind = std::env::var("MEM_KIND").unwrap_or_else(|_| "table_write".into());
    let n: usize = std::env::var("MEM_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let table = "probe";
    e.platform_write(Effect::TableCreate { table: table.into(), columns: vec!["id".into(), "body".into()] }).await.unwrap();
    let body = "x".repeat(2000);
    let row = |i: usize| json!({ "id": i.to_string(), "body": body }).as_object().unwrap().clone();
    // Something to read, for the read kinds.
    for i in 0..200 {
        e.platform_write(Effect::TableInsert { table: table.into(), rows: vec![row(i)] }).await.unwrap();
        e.query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", content = "some words {i}") NAMESPACE "p""#), None).await.unwrap();
    }
    let platform = e.platform().unwrap();
    println!("{kind} start: {} MiB", anon_mib());
    for i in 0..n {
        match kind.as_str() {
            "table_write" => {
                e.platform_write(Effect::TableInsert { table: table.into(), rows: vec![row(1000 + i)] }).await.unwrap();
            }
            "table_upsert" => {
                e.platform_write(Effect::TableUpsert { table: table.into(), key: vec!["id".into()], rows: vec![row(i % 50)] }).await.unwrap();
            }
            "table_read" => {
                platform.query(table, Some("id = '5'"), 10).await.unwrap();
            }
            "memory_write" => {
                e.query(&format!(r#"STORE INTO SEMANTIC (key = "w{i}", content = "more words {i}") NAMESPACE "p""#), None).await.unwrap();
            }
            "memory_write_long" => {
                let long = "many different words about things ".repeat(1 + (i % 40));
                e.query(&format!(r#"STORE INTO SEMANTIC (key = "l{i}", content = "{long}") NAMESPACE "p""#), None).await.unwrap();
            }
            "like_read" => {
                let vars: std::collections::HashMap<String, serde_json::Value> = [("q".to_string(), json!(format!("question number {i} about some words")))].into();
                e.query(r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "p" LIMIT 3"#, Some(&vars)).await.unwrap();
            }
            "memory_read" => {
                e.query(r#"RECALL FROM SEMANTIC WHERE key = "k5" NAMESPACE "p""#, None).await.unwrap();
            }
            "write_then_read" => {
                e.platform_write(Effect::TableInsert { table: table.into(), rows: vec![row(1000 + i)] }).await.unwrap();
                platform.query(table, Some("id = '5'"), 10).await.unwrap();
            }
            other => panic!("unknown kind {other}"),
        }
        if (i + 1) % (n / 5).max(1) == 0 {
            println!("{kind} {:>6} ops: {} MiB", i + 1, anon_mib());
        }
    }
}

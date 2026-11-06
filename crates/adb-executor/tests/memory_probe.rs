//! Which work grows the heap, per kind of operation (leader memory, CA-53). Not a pass/fail
//! test: run by hand, one kind at a time, as each needs a fresh process:
//!   MEM_KIND=table_write cargo test -p adb-executor --test memory_probe -- --ignored --nocapture

use adb_core::log::Effect;
use adb_executor::{Engine, LocalEngine};
use serde_json::json;

extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

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
    let store = e.flowr_engine("default").unwrap();
    let new_run = |i: usize| flowr_storage::engine::NewRun {
        run_id: uuid::Uuid::now_v7(),
        namespace: "p".into(),
        flow: format!("f{}", i % 5),
        version: String::new(),
        def_hash: String::new(),
        parent_run_id: None,
        durability: flowr_storage::engine::Durability::Durable,
        debug: false,
        breakpoints: None,
        principal: serde_json::Value::Null,
        trigger: "probe".into(),
        input: json!({ "payload": "y".repeat(5000) }),
        deadline_at: None,
        dedupe_key: None,
    };
    // Runs to list, for the kinds that read them.
    if kind == "engine_list" || kind == "engine_snapshot" || kind == "big_answers" {
        for i in 0..3000 {
            let run = new_run(i);
            let task = flowr_storage::engine::NewTask::start(&run);
            store.start(run, task).await.unwrap();
        }
    }
    // A log like a real server's, for the kinds that read it.
    if let Some(mb) = std::env::var("MEM_LOG_MB").ok().and_then(|v| v.parse::<usize>().ok()) {
        for i in 0..mb * 1024 / 8 {
            let rows: Vec<_> = (0..4).map(|k| row(100_000 + i * 4 + k)).collect();
            e.platform_write(Effect::TableInsert { table: table.into(), rows }).await.unwrap();
        }
        println!("{kind} log written: {} MiB", anon_mib());
        // Closes the segment holding them, as a FORGET's marker does.
        e.query(r#"STORE INTO SEMANTIC (key = "roll", content = "x") NAMESPACE "p""#, None).await.unwrap();
        e.query(r#"FORGET FROM SEMANTIC WHERE key = "roll" NAMESPACE "p""#, None).await.unwrap();
    }
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
            "sql" => {
                e.sql("SELECT count(*) FROM probe", None).await.unwrap();
            }
            "sql_memory" => {
                e.sql("SELECT count(*) FROM memory", None).await.unwrap();
            }
            "sql_schema" => {
                e.sql("SELECT table_name, count(*) FROM information_schema.columns GROUP BY table_name", None).await.unwrap();
            }
            "engine_start" => {
                let run = new_run(i);
                let task = flowr_storage::engine::NewTask::start(&run);
                store.start(run, task).await.unwrap();
            }
            "engine_list" => {
                store.list(flowr_storage::engine::RunQuery { limit: 1000, ..Default::default() }).await.unwrap();
            }
            "engine_subscribe" => {
                drop(store.subscribe(&format!("run:{i}")));
            }
            // As the server answers: many callers at once, each answer one big JSON text.
            "big_answers" => {
                let mut tasks = Vec::new();
                for _ in 0..12 {
                    let store = store.clone();
                    tasks.push(tokio::spawn(async move {
                        let page = store.list(flowr_storage::engine::RunQuery { limit: 3000, ..Default::default() }).await.unwrap();
                        serde_json::to_string(&page.items).unwrap().len()
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            }
            // FORGET compacts before it returns (W5).
            "forget" => {
                e.query(&format!(r#"STORE INTO SEMANTIC (key = "gone{i}", content = "to forget") NAMESPACE "p""#), None).await.unwrap();
                e.query(&format!(r#"FORGET FROM SEMANTIC WHERE key = "gone{i}" NAMESPACE "p""#), None).await.unwrap();
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
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    println!("{kind} idle 5 s: {} MiB", anon_mib());
    // Retained or live? This hands free arena memory back to the system; what stays is in use.
    unsafe { malloc_trim(0) };
    println!("{kind} after malloc_trim: {} MiB", anon_mib());
}

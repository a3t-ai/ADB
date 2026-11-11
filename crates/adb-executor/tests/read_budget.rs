//! PF-37: reads hold a bounded amount of memory. A read that would keep more decoded
//! rows than the node's read budget fails with a clear error instead of growing; an
//! `ORDER BY … LIMIT k` or a `LIMIT` read keeps only k rows while it scans, so it
//! answers within the same budget, and with the same rows as before.
//! Own test binary: it sets `ADB_READ_MEMORY_MB` for the whole process.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn records(v: &Value) -> Vec<Value> {
    v["data"]["records"].as_array().cloned().or_else(|| v["data"]["data"]["records"].as_array().cloned()).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_stay_within_the_read_memory_budget() {
    std::env::set_var("ADB_READ_MEMORY_MB", "4");
    // Reads from Lance, not the hot cache or the field index; no embedding model.
    std::env::set_var("ADB_CACHE", "off");
    std::env::set_var("ADB_FIELD_INDEX", "off");
    std::env::set_var("ADB_EMBED", "off");
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    const N: usize = 12_000;
    for i in 0..N {
        q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "k{i:05}", topic = "t{}", n = {}, content = "fact number {i} about a topic, padded so a row costs what a real one does")"#, i % 20, i % 997)).await;
    }

    // Every row: more than 4 MB once decoded — refused, not grown.
    let all = engine.query("RECALL FROM SEMANTIC ALL", None).await;
    let err = all.expect_err("a read past the budget fails").to_string();
    assert!(err.contains("read memory budget exceeded"), "{err}");

    // The first 5 by n (descending), ties in default order: keys of i = 996, 1993, …
    let top = q(&engine, "RECALL FROM SEMANTIC ALL ORDER BY n DESC LIMIT 5").await;
    let keys: Vec<String> = records(&top).iter().map(|r| r["key"].as_str().or_else(|| r["data"]["key"].as_str()).unwrap_or_default().to_string()).collect();
    let want: Vec<String> = (0..N).filter(|i| i % 997 == 996).take(5).map(|i| format!("k{i:05}")).collect();
    assert_eq!(keys, want, "{top}");

    // A filtered read with LIMIT, in default order (oldest first).
    let some = q(&engine, r#"RECALL FROM SEMANTIC WHERE topic = "t3" LIMIT 4"#).await;
    let keys: Vec<String> = records(&some).iter().map(|r| r["key"].as_str().or_else(|| r["data"]["key"].as_str()).unwrap_or_default().to_string()).collect();
    assert_eq!(keys, vec!["k00003", "k00023", "k00043", "k00063"], "{some}");

    // Aggregates fold while they scan (PF-39): over every row, within the same budget.
    let agg = q(&engine, "RECALL FROM SEMANTIC ALL AGGREGATE COUNT(*) AS c, SUM(n) AS s, MAX(n) AS m").await;
    let values = |v: &Value| if v["data"]["values"].is_null() { v["data"]["data"]["values"].clone() } else { v["data"]["values"].clone() };
    let a = values(&agg);
    let sum: u64 = (0..N as u64).map(|i| i % 997).sum();
    assert_eq!(a["c"], serde_json::json!(N), "{agg}");
    assert_eq!(a["s"].as_f64(), Some(sum as f64), "{agg}");
    assert_eq!(a["m"].as_f64(), Some(996.0), "{agg}");
    // Grouped: one row per topic, in the order of each group's first record.
    let grouped = q(&engine, "RECALL FROM SEMANTIC ALL GROUP BY topic AGGREGATE COUNT(*) AS c").await;
    let rows = values(&grouped);
    let rows = rows.as_array().unwrap_or_else(|| panic!("{grouped}"));
    assert_eq!(rows.len(), 20, "{grouped}");
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r["topic"], serde_json::json!(format!("t{i}")), "{grouped}");
        assert_eq!(r["c"], serde_json::json!(N / 20), "{grouped}");
    }
    // With HAVING and ORDER BY / LIMIT on the groups.
    let some = q(&engine, "RECALL FROM SEMANTIC WHERE n < 10 GROUP BY topic AGGREGATE COUNT(*) AS c HAVING c > 0 ORDER BY topic DESC LIMIT 2").await;
    let rows = values(&some);
    assert_eq!(rows.as_array().map(Vec::len), Some(2), "{some}");
    assert_eq!(rows[0]["topic"], serde_json::json!("t9"), "{some}");

    // The budget came back: the same reads again.
    assert_eq!(records(&q(&engine, "RECALL FROM SEMANTIC ALL ORDER BY n DESC LIMIT 5").await).len(), 5);
}

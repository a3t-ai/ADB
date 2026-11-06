//! DB-9: a past point whose offset has left the log. Its memory point's time still
//! names it, within the 90 days of versions kept; without a time, or older, it is
//! refused with `point_too_old`. Own test binary: it sets `ADB_LOG_RETAIN`.

use std::time::Duration;

use adb_executor::branches::PointRef;
use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

/// Enough writes for the log to be trimmed (see `subscription_retention.rs`). The log
/// drops whole segments, and a FORGET starts a new one.
async fn flood(engine: &LocalEngine) {
    for round in 0..3 {
        for i in 0..300 {
            q(engine, &format!(r#"STORE INTO WORKING (key = "fill-{round}-{i}", i = {i})"#)).await;
        }
        tokio::time::sleep(Duration::from_millis(1300)).await;
    }
}

#[tokio::test]
async fn an_offset_that_left_the_log_needs_its_time_and_a_recent_one() {
    std::env::set_var("ADB_LOG_RETAIN", "10");
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();

    let stored = q(&engine, r#"STORE INTO SEMANTIC (key = "fact", content = "original") NAMESPACE "crm""#).await;
    let point = stored["metadata"]["point"].clone();
    let offset = point["offset"].as_u64().unwrap_or_else(|| panic!("no point in {stored}"));
    let ts = point["ts"].as_i64().unwrap_or_else(|| panic!("no ts in {stored}"));
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE key = "fact" (content = "changed later") NAMESPACE "crm""#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "roll", content = "x") NAMESPACE "crm""#).await;
    q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "roll" NAMESPACE "crm""#).await;
    flood(&engine).await;

    // No time: the log can't say what the offset was.
    let e = engine.branch_create_past("crm", "no-time", None, &PointRef { offset: Some(offset), ..Default::default() }).await.unwrap_err();
    assert_eq!(adb_core::codes::code_of(&e), "point_too_old", "{e}");

    // Older than the versions kept.
    let old = ts - 91 * 86_400 * 1_000_000;
    let e = engine.branch_create_past("crm", "too-old", None, &PointRef { offset: Some(offset), ts: Some(old), ..Default::default() }).await.unwrap_err();
    assert_eq!(adb_core::codes::code_of(&e), "point_too_old", "{e}");
    assert!(engine.space_clone_at("crm", "crm-old", &PointRef { offset: Some(offset), ts: Some(old), ..Default::default() }).await.is_err());

    // With the memory point's time: it reads what that point held.
    engine.branch_create_past("crm", "with-time", None, &PointRef { offset: Some(offset), ts: Some(ts), ..Default::default() }).await.unwrap();
    let read = q(&engine, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "with-time" LIMIT 10"#).await.to_string();
    assert!(read.contains("original") && !read.contains("changed later"), "{read}");
}

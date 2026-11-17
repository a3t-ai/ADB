//! DB-8: EPISODIC and TOOLS read as of a point, as SEMANTIC and PROCEDURAL do; a branch at a
//! past point sees them as they were; each namespace can set its own history window.

use adb_executor::{Engine, LocalEngine};
use std::time::Duration;

async fn q(e: &LocalEngine, aql: &str) -> String {
    e.query(aql, None).await.unwrap_or_else(|err| panic!("{aql}: {err}")).to_string()
}

fn point(answer: &str) -> (u64, i64) {
    let v: serde_json::Value = serde_json::from_str(answer).unwrap();
    (v["metadata"]["point"]["offset"].as_u64().unwrap(), v["metadata"]["point"]["ts"].as_i64().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn episodes_and_tools_read_as_they_were() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&e, r#"STORE INTO EPISODIC (key = "ep", content = "first telling") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO TOOLS (key = "t", name = "lookup", description = "old description", schema = "{}") NAMESPACE "crm""#).await;
    let answer = q(&e, r#"STORE INTO SEMANTIC (key = "marker", content = "m") NAMESPACE "crm""#).await;
    let (offset, ts) = point(&answer);
    tokio::time::sleep(Duration::from_millis(5)).await;
    q(&e, r#"UPDATE INTO EPISODIC WHERE key = "ep" (content = "second telling") NAMESPACE "crm""#).await;
    q(&e, r#"UPDATE INTO TOOLS WHERE key = "t" (description = "new description") NAMESPACE "crm""#).await;

    // Now: one record each, the latest.
    let now = q(&e, r#"RECALL FROM EPISODIC ALL NAMESPACE "crm""#).await;
    assert!(now.contains("second telling") && !now.contains("first telling"), "{now}");
    let stats = e.space_stats("crm", None, Duration::ZERO).await.unwrap();
    assert_eq!(stats["tiers"]["EPISODIC"]["count"], serde_json::json!(1), "{stats}");

    // As of the point: as they were.
    let at = chrono::DateTime::from_timestamp_micros(ts).unwrap().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let then = q(&e, &format!(r#"RECALL FROM EPISODIC ALL AS OF "{at}" NAMESPACE "crm""#)).await;
    assert!(then.contains("first telling") && !then.contains("second telling"), "{then}");
    let then = q(&e, &format!(r#"RECALL FROM TOOLS ALL AS OF "{at}" NAMESPACE "crm""#)).await;
    assert!(then.contains("old description") && !then.contains("new description"), "{then}");
    let all = q(&e, r#"RECALL FROM EPISODIC ALL WITH HISTORY NAMESPACE "crm""#).await;
    assert!(all.contains("first telling") && all.contains("second telling"), "{all}");

    // WORKING has none, and says so.
    assert!(e.query(r#"RECALL FROM WORKING ALL WITH HISTORY NAMESPACE "crm""#, None).await.is_err());

    // A branch at that point reads them as they were.
    let at_point = adb_executor::branches::PointRef { offset: Some(offset), ..Default::default() };
    e.branch_create_past("crm", "past", None, &at_point).await.unwrap();
    let on_branch = q(&e, r#"RECALL FROM EPISODIC ALL NAMESPACE "crm" BRANCH "past""#).await;
    assert!(on_branch.contains("first telling") && !on_branch.contains("second telling"), "{on_branch}");
    let tools = q(&e, r#"RECALL FROM TOOLS ALL NAMESPACE "crm" BRANCH "past""#).await;
    assert!(tools.contains("old description") && !tools.contains("new description"), "{tools}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_namespace_sets_its_own_history_window() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for ns in ["short", "normal"] {
        q(&e, &format!(r#"STORE INTO SEMANTIC (key = "k", content = "old {ns}") NAMESPACE "{ns}""#)).await;
        q(&e, &format!(r#"UPDATE INTO SEMANTIC WHERE key = "k" (content = "new {ns}") NAMESPACE "{ns}""#)).await;
    }
    e.quota_set("short", None, None, Some(0)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // Compaction applies the windows (a FORGET no longer compacts: PF-6, 7 Nov).
    e.compact().await.unwrap();
    let short = q(&e, r#"RECALL FROM SEMANTIC ALL WITH HISTORY NAMESPACE "short""#).await;
    assert!(short.contains("new short") && !short.contains("old short"), "{short}");
    let normal = q(&e, r#"RECALL FROM SEMANTIC ALL WITH HISTORY NAMESPACE "normal""#).await;
    assert!(normal.contains("new normal") && normal.contains("old normal"), "{normal}");
}

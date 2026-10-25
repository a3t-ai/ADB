//! Stage C: a durable engine rebuilds exactly the same state from its log.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

/// Writes covering every memory type, UPDATE, FORGET and LINK.
const WRITES: &[&str] = &[
    r#"STORE INTO WORKING (task = "t1", step = 1) TTL 1h"#,
    r#"STORE INTO WORKING (task = "t2", step = 2)"#,
    r#"STORE INTO SEMANTIC (key = "acme", concept = "acme", content = "Acme renews on 1 Nov", active = true)"#,
    r#"STORE INTO EPISODIC (bid_id = "e1", campaign = "summer", won = false, cpm_paid = 2.5)"#,
    r#"STORE INTO EPISODIC (bid_id = "e2", campaign = "summer", won = false, cpm_paid = 1.0)"#,
    r#"STORE INTO PROCEDURAL (pattern_id = "oom-fix", pattern = "oom", steps = "restart the pod", confidence = 0.6)"#,
    r#"STORE INTO TOOLS (tool_id = "search", name = "search", description = "web search", ranking = 0.7)"#,
    r#"UPDATE INTO EPISODIC WHERE bid_id = "e1" (won = true, brand_lift = 0.04)"#,
    r#"UPDATE INTO WORKING WHERE task = "t1" (step = 5)"#,
    r#"UPDATE INTO PROCEDURAL WHERE pattern = "oom" (confidence = 0.9)"#,
    r#"FORGET FROM WORKING WHERE task = "t2""#,
    r#"FORGET FROM EPISODIC WHERE bid_id = "e2""#,
    r#"LINK FROM SEMANTIC WHERE concept = "acme" TO EPISODIC WHERE bid_id = "e1" TYPE "evidence" WEIGHT 0.8"#,
    r#"LINK FROM PROCEDURAL WHERE pattern = "oom" TO EPISODIC WHERE bid_id = "e1" TYPE "applied_to" WEIGHT 0.95"#,
];

/// Reads whose results must be identical before and after a restart.
const READS: &[&str] = &[
    r#"SCAN FROM WORKING WINDOW LAST 10"#,
    r#"RECALL FROM SEMANTIC WHERE active = true RETURN concept, content"#,
    r#"RECALL FROM EPISODIC WHERE campaign = "summer""#,
    r#"LOOKUP FROM PROCEDURAL WHERE pattern = "oom" WITH LINKS ALL"#,
    r#"LOAD FROM TOOLS WHERE ranking > 0.5 ORDER BY ranking DESC"#,
    r#"RECALL FROM SEMANTIC WHERE concept = "acme" FOLLOW LINKS TYPE "evidence""#,
];

/// A query result without timing fields.
fn stable(mut v: Value) -> Value {
    if let Some(m) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        m.remove("execution_time_ms");
    }
    v
}

async fn snapshot(engine: &LocalEngine) -> Vec<Value> {
    let mut out = Vec::new();
    for q in READS {
        out.push(stable(engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"))));
    }
    out.push(engine.stats().await.unwrap());
    out
}

#[tokio::test]
async fn restart_rebuilds_identical_state() {
    let dir = tempfile::tempdir().unwrap();

    let before = {
        let (engine, stats) = LocalEngine::durable(dir.path()).await.unwrap();
        assert_eq!(stats.records, 0);
        for q in WRITES {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        snapshot(&engine).await
    };

    // Sanity: the writes did what they say.
    let stats = &before[READS.len()];
    assert_eq!(stats["working"], 1, "{stats}");
    assert_eq!(stats["episodic"], 1, "{stats}");
    let working = before[0].to_string();
    assert!(working.contains(r#""step":5"#), "{working}");

    let (engine, replayed) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(replayed.records, WRITES.len() as u64, "one log record per write");
    let after = snapshot(&engine).await;
    for (i, (b, a)) in before.iter().zip(&after).enumerate() {
        assert_eq!(b, a, "read {i} ({}) differs after restart", READS.get(i).unwrap_or(&"stats"));
    }

    // New writes continue after the replayed ones, and survive another restart.
    engine.query(r#"STORE INTO WORKING (task = "t3", step = 1)"#, None).await.unwrap();
    drop(engine);
    let (engine, replayed) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(replayed.records, WRITES.len() as u64 + 1);
    assert_eq!(engine.stats().await.unwrap()["working"], 2);
}

#[tokio::test]
async fn ttl_measured_from_commit_time_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        engine.query(r#"STORE INTO WORKING (ttl_key = "short") TTL 300ms"#, None).await.unwrap();
        engine.query(r#"STORE INTO WORKING (ttl_key = "long") TTL 1h"#, None).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    // Replay must not restart the short TTL: it expired while ADB was down.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = engine.query(r#"SCAN FROM WORKING WINDOW LAST 10"#, None).await.unwrap().to_string();
    assert!(r.contains("long") && !r.contains("short"), "{r}");
}

#[tokio::test]
async fn reads_and_failed_writes_are_not_logged() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        engine.query(r#"RECALL FROM EPISODIC WHERE campaign = "none""#, None).await.unwrap();
        // An UPDATE / FORGET that matches nothing writes no log record.
        engine.query(r#"UPDATE INTO EPISODIC WHERE bid_id = "ghost" (won = false)"#, None).await.unwrap();
        engine.query(r#"FORGET FROM WORKING WHERE task = "ghost""#, None).await.unwrap();
        assert!(engine.query("STORE INTO", None).await.is_err());
    }
    let (_, replayed) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(replayed.records, 0);
}

/// Stage D2: the partition's Lance table, brought up to the log's end, as engine
/// startup does it (open the store, catch up from the log).
async fn lance_view(dir: &std::path::Path) -> (u64, Vec<adb_store::StoredRow>) {
    use adb_core::log::{CommitTs, PartitionId};
    let store = adb_store::PartitionStore::open(&adb_store::StoreConfig::new(dir.join("store")), PartitionId(0)).await.unwrap();
    let wal = adb_log::LocalWal::open(PartitionId(0), adb_log::WalConfig::new(dir.join("p0"))).unwrap();
    adb_executor::writes::catch_up(&store, &wal).await.unwrap();
    let applied = store.applied_offset().await.unwrap().0;
    let now = CommitTs::from_datetime(chrono::Utc::now());
    let rows = store.current(None, now).await.unwrap();
    store.close().await.unwrap();
    (applied, rows)
}

fn summary(rows: &[adb_store::StoredRow]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = rows
        .iter()
        .map(|r| (format!("{:?}/{:?}", r.data.memory, r.data.kind), r.data.value.get("bid_id").or(r.data.value.get("task")).or(r.data.value.get("concept")).or(r.data.value.get("pattern")).or(r.data.value.get("tool_id")).or(r.data.link_type.as_ref().map(|_| &r.data.value)).map(|v| v.to_string()).unwrap_or_default()))
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn lance_table_mirrors_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in WRITES {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
    }
    let (applied, rows) = lance_view(dir.path()).await;
    assert_eq!(applied, WRITES.len() as u64, "every log record applied");
    let s = summary(&rows);
    // working t1 (t2 forgotten), semantic acme, episodic e1 (e2 forgotten),
    // procedural oom-fix, tools search, two edges.
    assert_eq!(rows.len(), 7, "{s:?}");
    let e1 = rows.iter().find(|r| r.data.value.get("bid_id").and_then(|v| v.as_str()) == Some("e1")).unwrap();
    assert_eq!(e1.data.value["won"], true, "UPDATE applied in Lance");
    assert_eq!(e1.data.value["brand_lift"], 0.04);
    let t1 = rows.iter().find(|r| r.data.value.get("task").and_then(|v| v.as_str()) == Some("t1")).unwrap();
    assert_eq!(t1.data.value["step"], 5);
    assert!(t1.data.ttl_at.is_some(), "TTL carried into Lance");
    assert_eq!(rows.iter().filter(|r| r.data.link_type.is_some()).count(), 2, "{s:?}");
}

#[tokio::test]
async fn lance_catches_up_from_the_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in WRITES {
            engine.query(q, None).await.unwrap();
        }
    }
    let (_, before) = lance_view(dir.path()).await;
    // Lose the Lance table entirely: the log rebuilds it on the next open.
    std::fs::remove_dir_all(dir.path().join("store")).unwrap();
    {
        let (_engine, stats) = LocalEngine::durable(dir.path()).await.unwrap();
        assert_eq!(stats.store_caught_up, WRITES.len() as u64);
    }
    let (applied, after) = lance_view(dir.path()).await;
    assert_eq!(applied, WRITES.len() as u64);
    assert_eq!(summary(&before), summary(&after));
}

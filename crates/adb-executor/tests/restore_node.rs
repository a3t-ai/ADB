//! S3 option B, phase 1 step 2b: a node rebuilt from object storage. Node A writes
//! memory and platform rows, archives, and writes more. Node B is restored from the
//! archive point A announced (`Effect::Archived`) and gets the records after it,
//! as Raft would send them; then B answers every read as A does.
//!
//! Its own test binary: it sets `ADB_COLD_URI` for the process (a local directory
//! stands in for the bucket).

use adb_core::log::{Effect, LogOffset, PartitionId};
use adb_executor::restore::{restore_node, ArchivePoint};
use adb_executor::{Engine, LocalEngine};
use adb_log::{LocalWal, Log, WalConfig};
use serde_json::{json, Value};

const READS: &[&str] = &[
    r#"RECALL FROM EPISODIC WHERE campaign = "summer""#,
    r#"RECALL FROM EPISODIC WHERE campaign = "autumn""#,
    r#"RECALL FROM SEMANTIC WHERE active = true RETURN concept, content"#,
    r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS ALL DEPTH 2"#,
];

fn stable(mut v: Value) -> Value {
    if let Some(m) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        m.remove("execution_time_ms");
    }
    v
}

async fn reads(engine: &LocalEngine) -> Vec<Value> {
    let mut out = Vec::new();
    for q in READS {
        out.push(stable(engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"))));
    }
    let users = engine.platform().unwrap().query("users", None, 100).await.unwrap();
    out.push(json!(adb_platform::batches_to_json(&users)));
    out
}

fn user(id: &str, name: &str) -> serde_json::Map<String, Value> {
    json!({ "id": id, "name": name }).as_object().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_is_rebuilt_from_object_storage_and_the_log_after_it() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");

    let wal_cfg = |dir: &std::path::Path| WalConfig { dir: dir.join("p0"), segment_bytes: 4096 };
    let wal: adb_executor::EngineLog = std::sync::Arc::new(LocalWal::open(PartitionId(0), wal_cfg(a_dir.path())).unwrap());
    let (a, _) = LocalEngine::durable_on(a_dir.path(), wal).await.unwrap();
    for i in 0..120 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "s{i}", campaign = "summer", n = {i})"#), None).await.unwrap();
    }
    a.query(r#"STORE INTO SEMANTIC (key = "acme", concept = "acme", content = "Acme renews on 1 Nov", active = true)"#, None).await.unwrap();
    a.query(r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme", confidence = 0.9)"#, None).await.unwrap();
    a.query(r#"STORE INTO SEMANTIC (key = "old", concept = "old", content = "to forget", active = true)"#, None).await.unwrap();
    a.query(r#"FORGET FROM SEMANTIC WHERE key = "old""#, None).await.unwrap();
    a.platform_write(Effect::TableInsert { table: "users".into(), rows: vec![user("u1", "Ada"), user("u2", "Bob")] }).await.unwrap();
    a.platform_write(Effect::TableUpdate { table: "users".into(), filter: "id = 'u2'".into(), values: vec![("name".into(), "'Bobby'".into())] }).await.unwrap();

    // Archive: A announces the point in its log.
    a.archive_now().await.unwrap();

    // After the point: more of everything, which B gets from the log.
    for i in 0..40 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "a{i}", campaign = "autumn", n = {i})"#), None).await.unwrap();
    }
    a.platform_write(Effect::TableInsert { table: "users".into(), rows: vec![user("u3", "Cy")] }).await.unwrap();
    a.query(r#"FORGET FROM SEMANTIC WHERE key = "acme""#, None).await.unwrap();
    let want = reads(&a).await;
    drop(a);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // The point A announced, and the records after it, read from A's log.
    let log = LocalWal::open(PartitionId(0), wal_cfg(a_dir.path())).unwrap();
    let tip = log.last_offset().0;
    let first = (1..=tip).find(|o| futures::executor::block_on(log.read(LogOffset(*o), 1)).is_ok()).unwrap();
    let records = log.read(LogOffset(first), usize::MAX).await.unwrap();
    let point = records
        .iter()
        .flat_map(|r| r.effects.iter())
        .filter_map(|e| match e {
            Effect::Archived { cold, floor, node } => Some(ArchivePoint { cold: cold.clone(), floor: floor.0, node: node.clone() }),
            _ => None,
        })
        .last()
        .expect("A announced its archive point");
    eprintln!("point: floor {} of {tip}; node {}", point.floor, point.node);
    assert!(point.floor > 100, "the point holds the first writes: {point:?}");
    assert!(point.node["memory"]["version"].as_u64().is_some() && point.node["platform"]["lineage"].is_string(), "{point:?}");
    drop(log);

    // B: an old, unrelated directory restored over (as a node that fell behind).
    std::fs::create_dir_all(b_dir.path().join("store")).unwrap();
    std::fs::write(b_dir.path().join("store/stale"), "old").unwrap();
    let stats = restore_node(b_dir.path(), &point).await.unwrap();
    eprintln!("restored: {stats:?}");
    assert!(!b_dir.path().join("store/stale").exists());
    assert!(!b_dir.path().join(".restoring").exists() && !b_dir.path().join(".before-restore").exists());
    {
        // What Raft sends after the point.
        let b_log = LocalWal::open(PartitionId(0), wal_cfg(b_dir.path())).unwrap();
        assert_eq!(b_log.last_offset().0, point.floor, "the log starts right after the point");
        for r in records.into_iter().filter(|r| r.offset.0 > point.floor) {
            b_log.append(r).await.unwrap();
        }
    }
    let wal: adb_executor::EngineLog = std::sync::Arc::new(LocalWal::open(PartitionId(0), wal_cfg(b_dir.path())).unwrap());
    let (b, replay) = LocalEngine::durable_on(b_dir.path(), wal).await.unwrap();
    eprintln!("B replayed {} records", replay.records);
    let got = reads(&b).await;
    for (i, (w, g)) in want.iter().zip(&got).enumerate() {
        assert_eq!(w, g, "read {i} ({}) differs", READS.get(i).unwrap_or(&"platform users"));
    }
    // B goes on writing from where A stopped.
    b.query(r#"STORE INTO EPISODIC (bid_id = "w1", campaign = "winter", n = 1)"#, None).await.unwrap();
    let w = b.query(r#"RECALL FROM EPISODIC WHERE campaign = "winter""#, None).await.unwrap();
    assert_eq!(w["data"]["count"], 1, "{w}");
}

//! S3 option B §3.0: a node rebuilt from object storage alone. Node A writes memory
//! and platform rows, ships its log as WAL segments, takes a checkpoint (which
//! deletes the segments it covers), writes more and ships again. A is then gone; node
//! B is rebuilt from `POINT`, the checkpoint and the remaining segments, and answers
//! every read as A did.
//!
//! Its own test binary: it sets `ADB_COLD_URI` for the process (a local directory
//! stands in for the bucket).

use adb_core::log::{Effect, PartitionId};
use adb_executor::restore::restore_from_object_storage;
use adb_executor::writes::segments::{list_segments, read_point};
use adb_executor::{Engine, LocalEngine};
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
async fn a_node_is_rebuilt_from_object_storage_alone() {
    let cold_dir = tempfile::tempdir().unwrap();
    rebuilt(format!("file://{}", cold_dir.path().display())).await;
}

/// The same against real object storage: `ADB_S3_TEST_URI` (e.g.
/// `s3://adb-1/adb-dev/restore-s3`) and the `AWS_*` variables.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs ADB_S3_TEST_URI and AWS_* (real object storage)"]
async fn a_node_is_rebuilt_from_s3_alone() {
    let Ok(prefix) = std::env::var("ADB_S3_TEST_URI") else { return };
    rebuilt(format!("{}/run-{}", prefix.trim_end_matches('/'), chrono::Utc::now().timestamp_millis())).await;
}

/// The tests set process-wide variables (`ADB_COLD_URI`): one at a time.
static ONE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn rebuilt(cold: String) {
    let _one = ONE.lock().await;
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    std::env::set_var("ADB_COLD_URI", &cold);
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    // Small segments, so one ship makes several.
    std::env::set_var("ADB_WAL_SEGMENT_BYTES", "16384");

    let (a, _) = LocalEngine::durable(a_dir.path()).await.unwrap();
    for i in 0..120 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "s{i}", campaign = "summer", n = {i})"#), None).await.unwrap();
    }
    a.query(r#"STORE INTO SEMANTIC (key = "acme", concept = "acme", content = "Acme renews on 1 Nov", active = true)"#, None).await.unwrap();
    a.query(r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme", confidence = 0.9)"#, None).await.unwrap();
    a.query(r#"STORE INTO SEMANTIC (key = "old", concept = "old", content = "to forget", active = true)"#, None).await.unwrap();
    a.query(r#"FORGET FROM SEMANTIC WHERE key = "old""#, None).await.unwrap();
    a.platform_write(Effect::TableInsert { table: "users".into(), rows: vec![user("u1", "Ada"), user("u2", "Bob")] }).await.unwrap();

    // 1. Shipped: segments hold every record so far.
    let shipped = a.ship_now().await.unwrap();
    let segs = list_segments(&cold, PartitionId(0)).await.unwrap();
    eprintln!("shipped to {shipped}: {} segments {:?}..{:?}", segs.len(), segs.first(), segs.last());
    assert!(segs.len() > 1, "small segments: several");
    assert_eq!(segs.first().unwrap().first, 1);
    assert_eq!(segs.last().unwrap().last, shipped);

    // 2. A checkpoint: POINT written, the segments it covers deleted.
    a.archive_now().await.unwrap();
    let point = read_point(&cold, PartitionId(0)).await.unwrap().expect("POINT written");
    let left = list_segments(&cold, PartitionId(0)).await.unwrap();
    eprintln!("checkpoint floor {}; segments left {left:?}", point.floor);
    assert!(point.floor >= shipped, "the checkpoint holds what was shipped");
    assert!(left.iter().all(|s| s.last > point.floor), "covered segments deleted: {left:?}");

    // 3. More of everything after the checkpoint, shipped only as segments.
    for i in 0..40 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "a{i}", campaign = "autumn", n = {i})"#), None).await.unwrap();
    }
    a.platform_write(Effect::TableUpdate { table: "users".into(), filter: "id = 'u2'".into(), values: vec![("name".into(), "'Bobby'".into())] }).await.unwrap();
    a.platform_write(Effect::TableInsert { table: "users".into(), rows: vec![user("u3", "Cy")] }).await.unwrap();
    a.query(r#"FORGET FROM SEMANTIC WHERE key = "acme""#, None).await.unwrap();
    let shipped = a.ship_now().await.unwrap();
    let left = list_segments(&cold, PartitionId(0)).await.unwrap();
    eprintln!("shipped to {shipped}; segments above the checkpoint {left:?}");
    assert!(!left.is_empty() && left.last().unwrap().last == shipped);
    let want = reads(&a).await;
    drop(a);
    drop(a_dir);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 4. B from object storage alone (A's directory is gone).
    let t = std::time::Instant::now();
    let stats = restore_from_object_storage(b_dir.path(), &cold).await.unwrap();
    eprintln!("restored in {:?}: {stats:?}", t.elapsed());
    assert_eq!(stats.log_starts_at, point.floor + 1);
    assert_eq!(stats.log_ends_at, shipped, "every shipped record is back in the log");
    assert!(stats.segment_records > 0);
    let (b, replay) = LocalEngine::durable(b_dir.path()).await.unwrap();
    eprintln!("B replayed {} records", replay.records);
    let got = reads(&b).await;
    for (i, (w, g)) in want.iter().zip(&got).enumerate() {
        assert_eq!(w, g, "read {i} ({}) differs", READS.get(i).unwrap_or(&"platform users"));
    }
    // B goes on writing from where A stopped, and ships on from there.
    b.query(r#"STORE INTO EPISODIC (bid_id = "w1", campaign = "winter", n = 1)"#, None).await.unwrap();
    let w = b.query(r#"RECALL FROM EPISODIC WHERE campaign = "winter""#, None).await.unwrap();
    assert_eq!(w["data"]["count"], 1, "{w}");
    assert!(b.ship_now().await.unwrap() > shipped);
    eprintln!("whole run {:?}", started.elapsed());
}

//! S3 option B §3.0: a node rebuilt from object storage alone. Node A writes memory
//! and platform rows, ships its log as WAL segments, takes a checkpoint (which
//! deletes the segments it covers), writes more and ships again. Node B is then
//! rebuilt from the checkpoint point and the remaining segments, answers every read
//! as A did, and takes the partition over at epoch 1. A, now stale, goes on writing
//! and shipping: its checkpoint is refused (fenced) and a third node C, rebuilt
//! afterwards, holds B's writes and none of A's stale ones.
//!
//! Its own test binary: it sets `ADB_COLD_URI` for the process (a local directory
//! stands in for the bucket).

use adb_core::log::{Effect, PartitionId};
use adb_executor::restore::restore_from_object_storage;
use adb_executor::writes::segments::{list_epochs, list_segments, read_point, Fence};
use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

const READS: &[&str] = &[
    r#"RECALL FROM EPISODIC WHERE campaign = "summer""#,
    r#"RECALL FROM EPISODIC WHERE campaign = "autumn""#,
    r#"RECALL FROM SEMANTIC WHERE active = true RETURN concept, content"#,
    r#"RECALL FROM SEMANTIC WHERE key = "person:ada" FOLLOW LINKS DEPTH 2"#,
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
    a.query(r#"STORE INTO SEMANTIC (key = "person:ada")"#, None).await.unwrap();
    a.query(r#"STORE INTO SEMANTIC (key = "org:acme")"#, None).await.unwrap();
    a.query(r#"LINK FROM SEMANTIC WHERE key = "person:ada" INTO SEMANTIC WHERE key = "org:acme" TYPE "works_at" WEIGHT 0.9"#, None).await.unwrap();
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

    // 2. A checkpoint: its point written, the segments it covers deleted.
    a.archive_now().await.unwrap();
    let (epoch, point) = read_point(&cold, PartitionId(0), &Fence::default()).await.unwrap().expect("point written");
    assert_eq!(epoch, 0, "a single node writes at epoch 0");
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

    // 4. B from object storage alone, as the owner taking over at epoch 1.
    let t = std::time::Instant::now();
    let stats = restore_from_object_storage(b_dir.path(), &cold, Some(1)).await.unwrap();
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
    b.begin_epoch(1).await.unwrap();
    assert_eq!(list_epochs(&cold, PartitionId(0)).await.unwrap(), vec![(1, shipped + 1)]);

    // 5. A, stale, goes on: its writes reach segments under epoch 0, its checkpoint
    // is refused and it takes no more writes.
    for i in 0..5 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "x{i}", campaign = "stale", n = {i})"#), None).await.unwrap();
    }
    assert!(a.ship_now().await.unwrap() > shipped, "the stale owner ships");
    let refused = a.archive_now().await.expect_err("a stale owner's checkpoint is fenced");
    assert!(refused.contains("fenced"), "{refused}");
    assert!(a.query(r#"STORE INTO EPISODIC (bid_id = "y", campaign = "stale", n = 9)"#, None).await.is_err(), "fenced: no more writes");
    drop(a);
    drop(a_dir);

    // 6. B goes on writing from where A stopped, and ships on from there.
    b.query(r#"STORE INTO EPISODIC (bid_id = "w1", campaign = "winter", n = 1)"#, None).await.unwrap();
    let w = b.query(r#"RECALL FROM EPISODIC WHERE campaign = "winter""#, None).await.unwrap();
    assert_eq!(w["data"]["count"], 1, "{w}");
    let b_shipped = b.ship_now().await.unwrap();
    assert!(b_shipped > shipped);
    let want = reads(&b).await;
    drop(b);
    drop(b_dir);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 7. C from object storage: B's writes, none of A's stale ones.
    let c_dir = tempfile::tempdir().unwrap();
    let stats = restore_from_object_storage(c_dir.path(), &cold, None).await.unwrap();
    eprintln!("C restored: {stats:?}");
    assert_eq!(stats.log_ends_at, b_shipped);
    let (c, _) = LocalEngine::durable(c_dir.path()).await.unwrap();
    assert_eq!(reads(&c).await, want);
    let w = c.query(r#"RECALL FROM EPISODIC WHERE campaign = "winter""#, None).await.unwrap();
    assert_eq!(w["data"]["count"], 1, "{w}");
    let x = c.query(r#"RECALL FROM EPISODIC WHERE campaign = "stale""#, None).await.unwrap();
    assert_eq!(x["data"]["count"], 0, "the stale owner's writes were never acked: {x}");
    eprintln!("whole run {:?}", started.elapsed());
}

/// Object storage is on by default and optional: `ADB_S3=off`, or no
/// `ADB_COLD_URI`, leaves Lance on local disk only.
#[tokio::test(flavor = "multi_thread")]
async fn object_storage_is_on_by_default_and_optional() {
    let _one = ONE.lock().await;
    let cold_dir = tempfile::tempdir().unwrap();
    let cold = format!("file://{}", cold_dir.path().display());
    std::env::set_var("ADB_COLD_URI", &cold);
    std::env::remove_var("ADB_S3");
    assert_eq!(adb_executor::engine::object_storage(), Some(cold.clone()), "on by default");
    std::env::set_var("ADB_S3", "off");
    assert_eq!(adb_executor::engine::object_storage(), None, "switched off");
    // Off: a node writes, ships and archives nothing to object storage.
    let dir = tempfile::tempdir().unwrap();
    let (a, _) = LocalEngine::durable(dir.path()).await.unwrap();
    a.query(r#"STORE INTO EPISODIC (bid_id = "d1", campaign = "disk", n = 1)"#, None).await.unwrap();
    assert_eq!(a.ship_now().await.unwrap(), 0);
    a.archive_now().await.unwrap();
    assert_eq!(std::fs::read_dir(cold_dir.path()).unwrap().count(), 0, "nothing on object storage");
    let r = a.query(r#"RECALL FROM EPISODIC WHERE campaign = "disk""#, None).await.unwrap();
    assert_eq!(r["data"]["count"], 1, "{r}");
    std::env::remove_var("ADB_S3");
    std::env::set_var("ADB_COLD_URI", "");
    assert_eq!(adb_executor::engine::object_storage(), None, "on, but no URI: local disk");
}

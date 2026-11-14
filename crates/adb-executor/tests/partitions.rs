//! S3 option B step 3: engines for single partitions of a cluster node. Memory
//! partition 5 keeps its log, table, segments and checkpoints under its own
//! prefix (`{cold}/p5`), has no platform tables or engine store, and is rebuilt
//! from object storage alone; the system partition holds the platform tables.
//!
//! Its own test binary: it sets `ADB_COLD_URI` for the process.

use adb_core::log::{Effect, PartitionId};
use adb_executor::engine::PartitionRole;
use adb_executor::restore::restore_partition_from_object_storage;
use adb_executor::writes::segments::{list_segments, read_point, Fence};
use adb_executor::{Engine, LocalEngine};

#[tokio::test(flavor = "multi_thread")]
async fn a_memory_partition_lives_under_its_own_prefix_and_is_rebuilt_alone() {
    let cold_dir = tempfile::tempdir().unwrap();
    let cold = format!("file://{}", cold_dir.path().display());
    std::env::set_var("ADB_COLD_URI", &cold);
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    let p5 = PartitionId(5);

    let a_dir = tempfile::tempdir().unwrap();
    let (a, _) = LocalEngine::durable_partition(a_dir.path(), PartitionRole::memory(5)).await.unwrap();
    assert!(a.platform().is_none(), "a memory partition has no platform tables");
    for i in 0..30 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "b{i}", n = {i}) NAMESPACE "acme--bot""#), None).await.unwrap();
    }
    assert!(a_dir.path().join("p5").is_dir(), "the log is in p5");
    let shipped = a.ship_now().await.unwrap();
    a.archive_now().await.unwrap();
    for i in 30..40 {
        a.query(&format!(r#"STORE INTO EPISODIC (bid_id = "b{i}", n = {i}) NAMESPACE "acme--bot""#), None).await.unwrap();
    }
    let shipped_after = a.ship_now().await.unwrap();
    assert!(shipped_after > shipped);
    // Everything under p5; nothing under p0, no platform tree.
    let (_, point) = read_point(&cold, p5, &Fence::default()).await.unwrap().expect("p5 checkpoint");
    assert!(point.floor >= shipped);
    assert!(point.node.get("platform").is_none_or(|v| v.is_null()), "no platform in a memory partition: {}", point.node);
    assert!(!list_segments(&cold, p5).await.unwrap().is_empty());
    assert!(list_segments(&cold, PartitionId(0)).await.unwrap().is_empty());
    assert!(!cold_dir.path().join("platform").exists());
    let want = a.query(r#"RECALL FROM EPISODIC WHERE n >= 0 NAMESPACE "acme--bot" LIMIT 100"#, None).await.unwrap();
    assert_eq!(want["data"]["count"], 40, "{want}");
    drop(a);
    drop(a_dir);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Rebuilt from object storage alone, as partition 5.
    let b_dir = tempfile::tempdir().unwrap();
    let stats = restore_partition_from_object_storage(b_dir.path(), &cold, p5, None).await.unwrap();
    assert_eq!(stats.log_ends_at, shipped_after);
    let (b, _) = LocalEngine::durable_partition(b_dir.path(), PartitionRole::memory(5)).await.unwrap();
    let got = b.query(r#"RECALL FROM EPISODIC WHERE n >= 0 NAMESPACE "acme--bot" LIMIT 100"#, None).await.unwrap();
    assert_eq!(got["data"]["records"], want["data"]["records"]);

    // The system partition (64): platform tables.
    let s_dir = tempfile::tempdir().unwrap();
    let (s, _) = LocalEngine::durable_partition(s_dir.path(), PartitionRole::system(64)).await.unwrap();
    let rows = vec![serde_json::json!({ "id": "u1", "name": "Ada" }).as_object().unwrap().clone()];
    s.platform_write(Effect::TableInsert { table: "users".into(), rows }).await.unwrap();
    assert_eq!(s.platform().unwrap().query("users", None, 10).await.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(s_dir.path().join("p64").is_dir());
}

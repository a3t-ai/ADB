//! Memory branches on object storage (S3 option B). A branch engine is hosted by a
//! partition's engine and has its own place under the partition's
//! (`{cold}/p{n}/branches/…`): it never writes into another partition's place, and a
//! node that takes the partition over (its directory rebuilt from object storage)
//! finds the branch's writes there.
//!
//! Its own test binary: it sets `ADB_COLD_URI` for the process (a local directory
//! stands in for the bucket).

use std::time::Duration;

use adb_core::log::PartitionId;
use adb_executor::engine::PartitionRole;
use adb_executor::restore::restore_partition_from_object_storage;
use adb_executor::writes::segments::list_segments;
use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn keys(engine: &LocalEngine, aql: &str) -> Vec<String> {
    let r: Value = engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"));
    let mut k: Vec<String> = r["data"]["records"].as_array().unwrap().iter().filter_map(|r| r["data"]["key"].as_str().or(r["key"].as_str()).map(String::from)).collect();
    k.sort();
    k
}

#[tokio::test(flavor = "multi_thread")]
async fn a_branch_is_kept_on_object_storage_and_survives_a_takeover() {
    let cold_dir = tempfile::tempdir().unwrap();
    let cold = format!("file://{}", cold_dir.path().display());
    std::env::set_var("ADB_COLD_URI", &cold);
    std::env::set_var("ADB_WAL_SEGMENT_MS", "100");
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");

    // Node A owns partition 5 at epoch 1 and hosts branch b1 of namespace crm.
    let a_dir = tempfile::tempdir().unwrap();
    let a_part = a_dir.path().join("p5");
    let (a, _) = LocalEngine::durable_partition(&a_part, PartitionRole::memory(5)).await.unwrap();
    a.begin_epoch(1).await.unwrap();
    a.query(r#"STORE INTO SEMANTIC (key = "main-1", n = 1) NAMESPACE "crm""#, None).await.unwrap();
    a.branch_create("crm", "b1", None, None).await.unwrap();
    for i in 0..20 {
        a.query(&format!(r#"STORE INTO SEMANTIC (key = "br-{i:02}", n = {i}) NAMESPACE "crm" BRANCH "b1""#), None).await.unwrap();
    }
    let on_a = keys(&a, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "b1""#).await;
    assert_eq!(on_a.len(), 21, "{on_a:?}");
    // Shipped (every 100 ms): the partition's log and the branch's.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    a.ship_now().await.unwrap();

    // Nothing in partition 0's place: the branch has its own.
    let p0 = list_segments(&cold, PartitionId(0)).await.unwrap();
    assert!(p0.is_empty(), "the branch shipped its log into partition 0's place: {p0:?}");

    // Node B takes partition 5 over at epoch 2, from object storage alone.
    let b_dir = tempfile::tempdir().unwrap();
    let b_part = b_dir.path().join("p5");
    restore_partition_from_object_storage(&b_part, &cold, PartitionId(5), Some(2)).await.unwrap();
    let (b, _) = LocalEngine::durable_partition(&b_part, PartitionRole::memory(5)).await.unwrap();
    b.begin_epoch(2).await.unwrap();
    assert!(!b_part.join("branches").exists(), "nothing of the branch on B's disk before it is used");
    let on_b = keys(&b, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "b1""#).await;
    assert_eq!(on_b, on_a, "B reads the branch as A did");
    // And B writes on it.
    b.query(r#"STORE INTO SEMANTIC (key = "br-b", n = 99) NAMESPACE "crm" BRANCH "b1""#, None).await.unwrap();
    assert_eq!(keys(&b, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "b1""#).await.len(), 22);
    // Main never saw the branch's writes.
    assert_eq!(keys(&b, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm""#).await, vec!["main-1".to_string()]);

    // A branch dropped and made again under the same name starts empty: the old one's
    // place on object storage is kept, but never read for the new one.
    b.branch_drop("crm", "b1").await.unwrap();
    b.branch_create("crm", "b1", None, None).await.unwrap();
    assert_eq!(keys(&b, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "b1""#).await, vec!["main-1".to_string()]);
}

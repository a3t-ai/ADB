//! S3 option B, phase 1: a whole node archived to object storage — the memory
//! table, the platform tables and the engine — with the log kept only down to what
//! the archive can rebuild. Its own test binary: it sets `ADB_COLD_URI` for the
//! process (a local directory stands in for the bucket).

use adb_core::log::{LogOffset, PartitionId};
use adb_executor::{Engine, LocalEngine};
use adb_log::{LocalWal, Log, WalConfig};

#[tokio::test(flavor = "multi_thread")]
async fn a_node_archives_everything_and_trims_its_log_to_it() {
    let dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");

    // Small log segments, so the purge can drop whole segments in a short test.
    let wal_cfg = WalConfig { dir: dir.path().join("p0"), segment_bytes: 4096 };
    let wal: adb_executor::EngineLog = std::sync::Arc::new(LocalWal::open(PartitionId(0), wal_cfg.clone()).unwrap());
    let (engine, _) = LocalEngine::durable_on(dir.path(), wal).await.unwrap();
    for i in 0..50 {
        engine.query(&format!(r#"STORE INTO EPISODIC (bid_id = "b{i}", n = {i})"#), None).await.unwrap();
    }
    // Archive: the table is folded (more than 10 records held back) and archived,
    // the platform and engine trees beside it.
    engine.archive_now().await.unwrap();
    let current: serde_json::Value = serde_json::from_slice(&std::fs::read(cold.path().join("p0/CURRENT")).unwrap()).unwrap();
    eprintln!("CURRENT {current}");
    assert_eq!(current["watermark"], 50, "{current}");
    let node: serde_json::Value = serde_json::from_slice(&std::fs::read(cold.path().join("NODE")).unwrap()).unwrap();
    eprintln!("NODE {node}");
    assert!(node.get("platform").is_some() && node.get("engine").is_some(), "{node}");
    let lineage = current["lineage"].as_str().unwrap();
    assert!(cold.path().join(format!("p0/{lineage}/mirror/_versions")).exists());

    // More writes: the log is trimmed again, never past what the archive holds.
    for i in 50..400 {
        engine.query(&format!(r#"STORE INTO EPISODIC (bid_id = "b{i}", n = {i})"#), None).await.unwrap();
    }
    engine.archive_now().await.unwrap();
    for i in 400..700 {
        engine.query(&format!(r#"STORE INTO EPISODIC (bid_id = "b{i}", n = {i})"#), None).await.unwrap();
    }
    let watermark = serde_json::from_slice::<serde_json::Value>(&std::fs::read(cold.path().join("p0/CURRENT")).unwrap()).unwrap()["watermark"].as_u64().unwrap();
    drop(engine);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let wal = LocalWal::open(PartitionId(0), wal_cfg).unwrap();
    let first = (1..=700u64).find(|o| futures::executor::block_on(wal.read(LogOffset(*o), 1)).is_ok()).unwrap_or(0);
    eprintln!("watermark {watermark}; the log now starts at {first}");
    assert!(first > 1, "the log was trimmed (an idle platform and engine don't hold it)");
    assert!(first <= watermark + 1, "nothing the archive can't rebuild was trimmed");
}

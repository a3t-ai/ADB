//! A cluster owner's epoch begins while its shipper runs: `begin_epoch` takes the
//! locks in the shipper's order (`shipped`, then the sequencer). Taken the other way
//! round the two deadlocked and the partition took no request again (Hetzner p14:
//! takeovers from a promoted reader, whose shipper had work at once). And nothing is
//! shipped before the epoch begins.
//!
//! Its own test binary: it sets `ADB_COLD_URI`.

use std::sync::Arc;
use std::time::Duration;

use adb_executor::engine::{EngineLog, PartitionRole};
use adb_executor::{Engine, LocalEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_epoch_begins_while_the_shipper_runs() {
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_WAL_SEGMENT_MS", "3600000"); // segments only when asked
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    let dir = tempfile::tempdir().unwrap();
    let role = PartitionRole::memory(3);
    let wal = LocalEngine::open_partition_wal(dir.path(), role).await.unwrap();
    let (e, _) = Box::pin(LocalEngine::durable_partition_on(dir.path(), role, Arc::clone(&wal) as EngineLog)).await.unwrap();
    let e = Arc::new(e);
    for i in 0..3000 {
        e.query(&format!(r#"STORE INTO WORKING (key = "w{i}", n = {i}) NAMESPACE "ns""#), None).await.unwrap();
    }
    assert_eq!(e.ship_now().await.unwrap(), 0, "nothing ships before the owner's epoch begins");

    // The shipper in a tight loop, with records to ship, while epochs begin.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shipper = {
        let (e, stop) = (Arc::clone(&e), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut n = 3000;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                e.query(&format!(r#"STORE INTO WORKING (key = "w{n}", n = {n}) NAMESPACE "ns""#), None).await.unwrap();
                n += 1;
                let _ = e.ship_now().await;
            }
        })
    };
    for epoch in 1..=20 {
        tokio::time::timeout(Duration::from_secs(10), e.begin_epoch(epoch))
            .await
            .unwrap_or_else(|_| panic!("epoch {epoch} didn't begin: the shipper and begin_epoch deadlocked"))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    shipper.await.unwrap();
    assert!(e.ship_now().await.unwrap() > 3000, "once begun, the owner ships");
}

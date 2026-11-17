//! A follower compacts its own table on the leader's thresholds: its MemWAL
//! generations don't pile up, and a forgotten record's payload leaves its disk too.

#![recursion_limit = "256"]

use std::sync::Arc;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_executor::{EngineLog, Replica};
use adb_log::{LocalWal, Log, WalConfig};

fn row(key: &str) -> NewRow {
    NewRow {
        id: RecordId(key.into()),
        lineage_id: RecordId(key.into()),
        version: 1,
        data: RowData {
            tenant_id: "default".into(),
            scope: "tenant:default".into(),
            visibility: Visibility::Private,
            namespace: None,
            memory: Memory::Semantic,
            kind: Kind::Entity,
            key: Some(key.into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            text: None,
            value: serde_json::json!({ "key": key }),
            sealed: None,
            embedding: None,
            ttl_at: None,
            provenance: Provenance::default(),
        },
    }
}

fn record(offset: u64, effects: Vec<Effect>) -> LogRecord {
    LogRecord {
        partition: PartitionId(0),
        offset: LogOffset(offset),
        epoch: Epoch(1),
        commit_ts: CommitTs(1_761_300_000_000_000 + offset as i64),
        txn_id: None,
        effects,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_compacts_and_erases() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(LocalWal::open(PartitionId(0), WalConfig::new(dir.path().join("p0"))).unwrap());
    let log: EngineLog = wal.clone();
    let mut replica = Replica::open(dir.path(), log).await.unwrap();

    // More writes than the leader's threshold, then a FORGET of the first record.
    let n = adb_executor::COMPACT_AFTER_WRITES + 10;
    for i in 1..=n {
        wal.append(record(i, vec![Effect::PutEntity(row(&format!("k{i}")))])).await.unwrap();
    }
    let gone = adb_store::row_id(Memory::Semantic, "k1");
    wal.append(record(n + 1, vec![Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("k1".into()) }])).await.unwrap();
    replica.step().await.unwrap();

    let store = Arc::clone(replica.store());
    let filter = format!("lineage_id = '{gone}'");
    let before = store.scan(Some(&filter)).await.unwrap();
    assert!(before.iter().any(|r| r.data.value != serde_json::json!({})), "before: the forgotten payload is still on disk");
    assert!(store.writes_due(adb_executor::COMPACT_AFTER_WRITES), "the threshold is reached");

    replica.maintain().await;

    // Only the marker stays (W5): tombstoned, its payload gone.
    let after = store.scan(Some(&filter)).await.unwrap();
    assert!(!after.is_empty() && after.iter().all(|r| r.tombstone && r.data.value == serde_json::json!({})), "after: no payload left");
    // The rewrite writes through the MemWAL too, so its own generation is all that stays.
    assert!(store.generation_count().await.unwrap() <= 1, "earlier generations are compacted away");
    assert!(!store.writes_due(adb_executor::COMPACT_AFTER_WRITES));
    let now = CommitTs(1_761_400_000_000_000);
    assert!(store.current_version(Memory::Semantic, "k2", now).await.unwrap().is_some(), "the rest stays");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_erases_its_log_at_the_leaders_marker() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(LocalWal::open(PartitionId(0), WalConfig::new(dir.path().join("p0"))).unwrap());
    let log: EngineLog = wal.clone();
    let mut replica = Replica::open(dir.path(), log).await.unwrap();

    for i in 1..=20 {
        wal.append(record(i, vec![Effect::PutEntity(row(&format!("k{i}")))])).await.unwrap();
    }
    wal.append(record(21, vec![Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("k1".into()) }])).await.unwrap();
    replica.step().await.unwrap();
    let held = |wal: Arc<LocalWal>| async move {
        wal.read(LogOffset(1), 1000).await.unwrap().iter().flat_map(|r| r.effects.clone()).filter(|e| matches!(e, Effect::PutEntity(n) if n.id.0 == "k1")).count()
    };
    assert_eq!(held(wal.clone()).await, 1, "no marker yet: the payload is still in the log");

    // The leader's marker: no effects, on a new segment.
    wal.append_on_new_segment(record(22, Vec::new())).await.unwrap();
    replica.step().await.unwrap();

    assert_eq!(held(wal.clone()).await, 0, "the payload left the follower's log");
    let filter = format!("lineage_id = '{}'", adb_store::row_id(Memory::Semantic, "k1"));
    let rows = replica.store().scan(Some(&filter)).await.unwrap();
    assert!(rows.iter().all(|r| r.tombstone && r.data.value == serde_json::json!({})), "and the table");
    let now = CommitTs(1_761_400_000_000_000);
    assert!(replica.store().current_version(Memory::Semantic, "k2", now).await.unwrap().is_some(), "the rest stays");
    assert_eq!(wal.read(LogOffset(1), 1000).await.unwrap().len(), 22, "every offset stays");
}

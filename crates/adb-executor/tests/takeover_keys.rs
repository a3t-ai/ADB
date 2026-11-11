//! PF-38: a follower keeps the live keys by every apply, and hands them to the write
//! path when it takes the lead — no rescan of the table inside the takeover. The key
//! check must then be what a rescan would give: keys in use are refused, a forgotten
//! key is free again.

#![recursion_limit = "256"]

use std::sync::Arc;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_executor::{Engine, EngineLog, LocalEngine, Replica};
use adb_log::{LocalWal, Log, WalConfig};

fn row(key: &str) -> NewRow {
    NewRow {
        id: RecordId(key.into()),
        lineage_id: RecordId(key.into()),
        version: 1,
        data: RowData {
            tenant_id: "default".into(),
            scope: adb_store::DEFAULT_SCOPE.into(),
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
    LogRecord { partition: PartitionId(0), offset: LogOffset(offset), epoch: Epoch(1), commit_ts: CommitTs(1_761_300_000_000_000 + offset as i64), txn_id: None, effects }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_takeover_uses_the_followers_live_keys() {
    std::env::set_var("ADB_EMBED", "off");
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(LocalWal::open(PartitionId(0), WalConfig::new(dir.path().join("p0"))).unwrap());
    let log: EngineLog = wal.clone();
    let mut replica = Replica::open(dir.path(), log.clone()).await.unwrap();

    // Some records before the index is built, some while and after.
    for i in 1..=20u64 {
        wal.append(record(i, vec![Effect::PutEntity(row(&format!("k{i}")))])).await.unwrap();
    }
    replica.step().await.unwrap();
    let store = Arc::clone(replica.store());
    for _ in 0..200 {
        if store.live_keys_ready() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(store.live_keys_ready(), "the follower indexed its live keys");
    for i in 21..=30u64 {
        wal.append(record(i, vec![Effect::PutEntity(row(&format!("k{i}")))])).await.unwrap();
    }
    wal.append(record(31, vec![Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("k3".into()) }])).await.unwrap();
    replica.step().await.unwrap();
    drop(store);

    // It takes the lead.
    let (engine, _) = LocalEngine::durable_warm(dir.path(), log, replica.into_warm()).await.unwrap();
    for key in ["k2", "k25"] {
        let e = engine.query(&format!(r#"STORE INTO SEMANTIC (key = "{key}", v = 2)"#), None).await.expect_err("a key in use is refused");
        assert!(e.to_string().contains("key_exists"), "{key}: {e}");
    }
    engine.query(r#"STORE INTO SEMANTIC (key = "k3", v = 2)"#, None).await.expect("a forgotten key is free");
    engine.query(r#"STORE INTO SEMANTIC (key = "k99", v = 1)"#, None).await.expect("a new key");
}

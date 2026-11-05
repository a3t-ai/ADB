//! A follower opening a table an older ADB wrote runs the schema migrations too, as
//! the leader's engine does: here v4 → v5 (namespace record ids, `.ai/NS_IDS.md`).

use std::sync::Arc;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_executor::{EngineLog, Replica};
use adb_log::{LocalWal, WalConfig};
use adb_store::{PartitionStore, StoreConfig};

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_migrates_its_table_on_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = PartitionStore::open(&StoreConfig::new(dir.path().join("store")), PartitionId(0)).await.unwrap();
        store.set_schema_version(4);
        let data = RowData {
            tenant_id: "default".into(),
            scope: "tenant:default".into(),
            visibility: Visibility::Private,
            namespace: Some("acme--a".into()),
            memory: Memory::Semantic,
            kind: Kind::Entity,
            key: Some("pref".into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            text: None,
            value: serde_json::json!({ "key": "pref", "fact": "tea" }),
            sealed: None,
            embedding: None,
            ttl_at: None,
            provenance: Provenance::default(),
        };
        let rec = LogRecord {
            partition: PartitionId(0),
            offset: LogOffset(1),
            epoch: Epoch(1),
            commit_ts: CommitTs(1_761_300_000_000_000),
            txn_id: None,
            effects: vec![Effect::PutEntity(NewRow { id: RecordId("pref".into()), lineage_id: RecordId("pref".into()), version: 1, data })],
        };
        store.apply(&rec).await.unwrap();
        store.close().await.unwrap();
    }
    let wal: EngineLog = Arc::new(LocalWal::open(PartitionId(0), WalConfig::new(dir.path().join("p0"))).unwrap());
    let replica = Replica::open(dir.path(), wal).await.unwrap();
    let store = Arc::clone(replica.store());
    assert_eq!(store.schema_version(), 5);
    let now = CommitTs(1_761_400_000_000_000);
    assert!(store.current_version(Memory::Semantic, "acme--a|~pref", now).await.unwrap().is_some(), "the namespaced id");
    assert!(store.current_version(Memory::Semantic, "pref", now).await.unwrap().is_none());
}

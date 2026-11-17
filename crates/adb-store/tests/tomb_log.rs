//! The tombstone log (`.ai/S3_OPTION_B_PLAN.md` §3.5): every FORGET and expiry leaves
//! an entry, written with the tombstones, that normal reads never see and that no
//! compaction, tiering or rewrite removes; with object storage it is archived too.

use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_store::{PartitionStore, StoreConfig};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;
const DAY: i64 = 86_400_000_000;

fn row(key: &str) -> RowData {
    RowData {
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
        text: Some(format!("fact {key}")),
        value: json!({ "key": key, "content": format!("fact {key}") }),
        sealed: None,
        embedding: None,
        ttl_at: None,
        provenance: Provenance::default(),
    }
}

fn rec(offset: u64, ts: i64, txn: Option<&str>, effects: Vec<Effect>) -> LogRecord {
    LogRecord { partition: PartitionId(0), offset: LogOffset(offset), epoch: Epoch(1), commit_ts: CommitTs(ts), txn_id: txn.map(String::from), effects }
}

fn put(k: &str) -> Effect {
    Effect::PutEntity(NewRow { id: RecordId(k.into()), lineage_id: RecordId(k.into()), version: 1, data: row(k) })
}

#[tokio::test(flavor = "multi_thread")]
async fn every_forget_and_expiry_is_logged_and_kept() {
    let root = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    let cfg = StoreConfig {
        root: root.path().to_path_buf(),
        embedding_dim: 4,
        memtable_rows: 500,
        wal_flush: Duration::from_millis(20),
        embedding_model: None,
        cold: Some(format!("file://{}", cold.path().display())),
    };
    let now = CommitTs(T0 + 10 * DAY);
    let history = Duration::from_secs(90 * 86_400);
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    store.apply(&rec(1, T0 + 1, None, (0..10).map(|i| put(&format!("k{i}"))).collect())).await.unwrap();
    // A v2 of k1, so its FORGET hides two versions.
    let v2 = NewRow { id: RecordId("k1-v2".into()), lineage_id: RecordId("k1".into()), version: 2, data: row("k1") };
    store.apply(&rec(2, T0 + 2, None, vec![Effect::Supersede { old: RecordId("k1".into()), new: v2 }])).await.unwrap();
    store
        .apply(&rec(3, T0 + 3, Some("txn-forget-1"), vec![
            Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("k1".into()) },
            Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("k2".into()) },
        ]))
        .await
        .unwrap();
    store.apply(&rec(4, T0 + 4, None, vec![Effect::Expire { memory: Memory::Semantic, lineage_id: RecordId("k3".into()) }])).await.unwrap();
    // A FORGET of something that never existed is logged too (rows: 0).
    store.apply(&rec(5, T0 + 5, None, vec![Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("nope".into()) }])).await.unwrap();

    let check = |log: &[adb_store::TombEntry]| {
        let got: Vec<(&str, &str, u64, u64)> = log.iter().map(|e| (e.event.as_str(), e.lineage.as_str(), e.rows, e.offset)).collect();
        assert_eq!(got, vec![("forget", "k1", 2, 3), ("forget", "k2", 1, 3), ("expire", "k3", 1, 4), ("forget", "nope", 0, 5)], "{log:#?}");
        assert_eq!(log[0].key.as_deref(), Some("k1"));
        assert_eq!(log[0].at, CommitTs(T0 + 3));
        assert_eq!(log[0].txn_id.as_deref(), Some("txn-forget-1"));
        assert_eq!(log[0].scope, "tenant:default");
    };
    check(&store.tombstone_log(0, 100).await.unwrap());
    assert_eq!(store.tombstone_log(3, 100).await.unwrap().len(), 2, "after offset 3");
    // Reads of memory never see the log: 10 facts, 3 hidden.
    assert_eq!(store.count_current(Memory::Semantic, now).await.unwrap(), 7);

    // Compaction, tiering and a full rewrite keep it.
    store.compact(history, now).await.unwrap();
    check(&store.tombstone_log(0, 100).await.unwrap());
    // With object storage nothing is physically removed by compaction: the forgotten
    // fact keeps its payload (no read returns it), so the archive holds it.
    let k2 = store.scan(Some("tombstone = true AND lineage_id = 'semantic:k2'")).await.unwrap();
    assert!(k2.iter().any(|r| r.data.text.as_deref() == Some("fact k2")), "forgotten payload kept: {k2:?}");
    store.archive().await.unwrap();
    store.tier(0).await.unwrap();
    store.compact(history, now).await.unwrap();
    check(&store.tombstone_log(0, 100).await.unwrap());
    store.clear_vectors().await.unwrap();
    check(&store.tombstone_log(0, 100).await.unwrap());
    assert_eq!(store.count_current(Memory::Semantic, now).await.unwrap(), 7);

    // And it is on the mirror: the archived table holds the four entries.
    store.archive().await.unwrap();
    let lineage = std::fs::read_to_string(root.path().join("p0/memory.lance/_adb_lineage")).unwrap();
    let mirror = format!("file://{}/p0/{}/mirror", cold.path().display(), lineage.trim());
    let ds = adb_store::lance::Dataset::open(&mirror).await.unwrap();
    assert_eq!(ds.count_rows(Some("kind = 'tomb_log'".to_string())).await.unwrap(), 4);
    assert_eq!(ds.count_rows(Some("tombstone = true AND text = 'fact k2'".to_string())).await.unwrap(), 1, "the archive holds the forgotten payload");
    store.close().await.unwrap();
}

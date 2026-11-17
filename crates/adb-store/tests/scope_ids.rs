//! Schema v3 → v4 (E-L12): rows an older ADB wrote in a named scope with bare ids
//! get ids that carry the scope; facts and links follow their ends; the default
//! scope's rows and later versions' ULID ids stay; `_meta` says v4 afterwards.

use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_store::{PartitionStore, StoreConfig};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;

fn data(scope: &str, kind: Kind, value: serde_json::Value) -> RowData {
    RowData {
        tenant_id: "default".into(),
        scope: scope.into(),
        visibility: Visibility::Private,
        namespace: None,
        memory: Memory::Semantic,
        kind,
        key: value.get("key").and_then(|v| v.as_str()).map(String::from),
        subject_id: None,
        predicate: None,
        object_id: None,
        link_type: None,
        weight: None,
        text: None,
        value,
        sealed: None,
        embedding: None,
        ttl_at: None,
        provenance: Provenance::default(),
    }
}

fn put(id: &str, lineage: &str, version: u32, d: RowData) -> NewRow {
    NewRow { id: RecordId(id.into()), lineage_id: RecordId(lineage.into()), version, data: d }
}

fn rec(offset: u64, effects: Vec<Effect>) -> LogRecord {
    LogRecord { partition: PartitionId(0), offset: LogOffset(offset), epoch: Epoch(1), commit_ts: CommitTs(T0 + offset as i64 * 1_000), txn_id: None, effects }
}

#[tokio::test(flavor = "multi_thread")]
async fn named_scope_rows_get_scoped_ids() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 1000, wal_flush: Duration::from_millis(50), embedding_model: None, cold: None };
    let now = CommitTs(T0 + 1_000_000);
    {
        let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
        let mut fact = data("team:a", Kind::Fact, json!({ "key": "fact:ada|works_at", "subject": "ada", "object": "acme" }));
        fact.subject_id = Some("ada".into());
        fact.object_id = Some("acme".into());
        let mut edge = data("team:a", Kind::Edge, json!({ "to_memory": "semantic" }));
        edge.subject_id = Some("acme".into());
        edge.object_id = Some("other".into());
        edge.link_type = Some("near".into());
        let records = [
            rec(1, vec![Effect::PutEntity(put("ada", "ada", 1, data("team:a", Kind::Entity, json!({ "key": "ada" }))))]),
            rec(2, vec![Effect::PutEntity(put("acme", "acme", 1, data("team:a", Kind::Entity, json!({ "key": "acme" }))))]),
            rec(3, vec![Effect::PutEntity(put("other", "other", 1, data("tenant:default", Kind::Entity, json!({ "key": "other" }))))]),
            rec(4, vec![Effect::PutFact(put("fact:ada|works_at", "fact:ada|works_at", 1, fact))]),
            rec(5, vec![Effect::Supersede { old: RecordId("ada".into()), new: put("01JV2", "ada", 2, data("team:a", Kind::Entity, json!({ "key": "ada", "v": 2 }))) }]),
            rec(6, vec![Effect::PutEdge(put("e1", "e1", 1, edge))]),
        ];
        for r in &records {
            store.apply(r).await.unwrap();
        }
        store.set_schema_version(3);
        assert_eq!(store.scope_row_ids().await.unwrap(), 5, "ada v1 and v2, acme, the fact, the edge (its source)");
        assert_eq!(store.scope_row_ids().await.unwrap(), 0, "once");
        store.close().await.unwrap();
    }
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    assert_eq!(store.schema_version(), 4);
    let cur = store.current_version(Memory::Semantic, "team:a~ada", now).await.unwrap().expect("ada under its scoped id");
    assert_eq!((cur.id.as_str(), cur.version), ("semantic:01JV2", 2));
    assert!(store.current_version(Memory::Semantic, "ada", now).await.unwrap().is_none());
    assert!(store.current_version(Memory::Semantic, "other", now).await.unwrap().is_some(), "the default scope's ids stay");
    let f = store.current_version(Memory::Semantic, "team:a~fact:ada|works_at", now).await.unwrap().expect("the fact");
    assert_eq!((f.data.subject_id.as_deref(), f.data.object_id.as_deref()), (Some("semantic:team:a~ada"), Some("semantic:team:a~acme")));
    let e = store.scan(Some("kind = 'edge'")).await.unwrap();
    assert_eq!((e[0].data.subject_id.as_deref(), e[0].data.object_id.as_deref()), (Some("semantic:team:a~acme"), Some("semantic:other")));
    store.close().await.unwrap();
}

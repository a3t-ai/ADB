//! Schema v4 → v5 (NS, `.ai/NS_IDS.md`): records an older ADB wrote in a namespace
//! get ids that carry it; a FACT's ends become entities of its own namespace (a
//! shared stub is split: the other namespace gets its own); LINKs follow their ends;
//! records without a namespace keep their ids.

use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_store::{PartitionStore, StoreConfig};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;

fn data(ns: Option<&str>, kind: Kind, value: serde_json::Value) -> RowData {
    RowData {
        tenant_id: "default".into(),
        scope: "tenant:default".into(),
        visibility: Visibility::Private,
        namespace: ns.map(String::from),
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

fn fact(ns: &str, subject: &str, predicate: &str, value: &str) -> NewRow {
    let key = format!("fact:{subject}|{predicate}");
    let mut d = data(Some(ns), Kind::Fact, json!({ "key": key, "subject": subject, "predicate": predicate, "value": value }));
    d.subject_id = Some(subject.into());
    d.predicate = Some(predicate.into());
    put(&key, &key, 1, d)
}

#[tokio::test(flavor = "multi_thread")]
async fn namespaced_rows_get_namespaced_ids_and_shared_stubs_split() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 1000, wal_flush: Duration::from_millis(50), embedding_model: None };
    let now = CommitTs(T0 + 1_000_000);
    {
        let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
        store.set_schema_version(4);
        let mut edge = data(None, Kind::Edge, json!({ "to_memory": "semantic" }));
        edge.subject_id = Some("plain".into());
        edge.object_id = Some("ada".into());
        edge.link_type = Some("knows".into());
        let records = [
            // Agent A created the stub `user:alice` (v4: one record for every namespace).
            rec(1, vec![Effect::PutEntity(put("user:alice", "user:alice", 1, data(Some("acme--a"), Kind::Entity, json!({ "key": "user:alice", "name": "user:alice" }))))]),
            rec(2, vec![Effect::PutFact(fact("acme--a", "user:alice", "likes", "tea"))]),
            // Agent B's fact about the same user pointed at A's stub.
            rec(3, vec![Effect::PutFact(fact("acme--b", "user:alice", "prefers", "email"))]),
            rec(4, vec![Effect::PutEntity(put("ada", "ada", 1, data(Some("acme--a"), Kind::Entity, json!({ "key": "ada" }))))]),
            rec(5, vec![Effect::Supersede { old: RecordId("ada".into()), new: put("01JV2", "ada", 2, data(Some("acme--a"), Kind::Entity, json!({ "key": "ada", "v": 2 }))) }]),
            rec(6, vec![Effect::PutEntity(put("plain", "plain", 1, data(None, Kind::Entity, json!({ "key": "plain" }))))]),
            rec(7, vec![Effect::PutEdge(put("e1", "e1", 1, edge))]),
        ];
        for r in &records {
            store.apply(r).await.unwrap();
        }
        store.close().await.unwrap();
    }
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    assert_eq!(store.schema_version(), 4);
    let changed = store.namespace_row_ids().await.unwrap();
    assert!(changed >= 6, "{changed}: alice, two facts (and their ends), ada v1 and v2, the edge, a new stub");
    assert_eq!(store.namespace_row_ids().await.unwrap(), 0, "once");
    store.close().await.unwrap();

    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    assert_eq!(store.schema_version(), 5);
    let cur = |k: &'static str| {
        let store = &store;
        async move { store.current_version(Memory::Semantic, k, now).await.unwrap() }
    };
    assert!(cur("user:alice").await.is_none(), "no record without a namespace any more");
    assert!(cur("acme--a|~user:alice").await.is_some(), "A's entity");
    let b_stub = cur("acme--b|~user:alice").await.expect("B got its own stub");
    assert_eq!(b_stub.data.namespace.as_deref(), Some("acme--b"));
    assert_eq!(b_stub.data.value, json!({ "key": "user:alice", "name": "user:alice" }));
    let fa = cur("acme--a|~fact:user:alice|likes").await.expect("A's fact");
    assert_eq!(fa.data.subject_id.as_deref(), Some("semantic:acme--a|~user:alice"));
    let fb = cur("acme--b|~fact:user:alice|prefers").await.expect("B's fact");
    assert_eq!(fb.data.subject_id.as_deref(), Some("semantic:acme--b|~user:alice"), "B's fact is about B's entity");
    let ada = cur("acme--a|~ada").await.expect("ada");
    assert_eq!((ada.id.as_str(), ada.version), ("semantic:01JV2", 2), "later versions keep their ULID ids");
    assert!(cur("plain").await.is_some(), "no namespace: the id stays");
    let e = store.scan(Some("kind = 'edge'")).await.unwrap();
    assert_eq!((e[0].data.subject_id.as_deref(), e[0].data.object_id.as_deref()), (Some("semantic:plain"), Some("semantic:acme--a|~ada")));
    store.close().await.unwrap();
}

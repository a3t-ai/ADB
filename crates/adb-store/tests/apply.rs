//! D1: applying log records to a partition's Lance table (MemWAL option A) with the
//! D2 temporal rules, read back through LsmScanner.

use std::time::Duration;

use adb_core::log::{
    CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData,
    Visibility,
};
use adb_store::{key_of, PartitionStore, StoreConfig, StoredRow};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;

fn data(memory: Memory, kind: Kind, value: serde_json::Value) -> RowData {
    RowData {
        tenant_id: "default".into(),
        scope: "tenant:default".into(),
        visibility: Visibility::Private,
        namespace: None,
        memory,
        kind,
        key: value.get("key").and_then(|v| v.as_str()).map(String::from),
        subject_id: None,
        predicate: None,
        object_id: None,
        link_type: None,
        weight: None,
        text: value.get("content").and_then(|v| v.as_str()).map(String::from),
        value,
        sealed: None,
        embedding: None,
        ttl_at: None,
        provenance: Provenance::default(),
    }
}

fn new(id: &str, lineage: &str, version: u32, d: RowData) -> NewRow {
    NewRow { id: RecordId(id.into()), lineage_id: RecordId(lineage.into()), version, data: d }
}

fn rec(offset: u64, effects: Vec<Effect>) -> LogRecord {
    LogRecord {
        partition: PartitionId(0),
        offset: LogOffset(offset),
        epoch: Epoch(1),
        commit_ts: CommitTs(T0 + offset as i64 * 1_000),
        txn_id: None,
        effects,
    }
}

fn cfg(dir: &std::path::Path, memtable_rows: usize) -> StoreConfig {
    StoreConfig { root: dir.to_path_buf(), embedding_dim: 4, memtable_rows, wal_flush: Duration::from_millis(50) }
}

/// The D2 story: put, patch, supersede, link, forget.
fn story() -> Vec<LogRecord> {
    let fact = |v: serde_json::Value| data(Memory::Semantic, Kind::Entity, v);
    let mut edge = data(Memory::Semantic, Kind::Edge, json!({ "to_memory": "episodic" }));
    edge.subject_id = Some("acme".into()); // lineage ids (D2 §3)
    edge.predicate = Some("link".into());
    edge.object_id = Some("e1".into());
    edge.link_type = Some("evidence".into());
    edge.weight = Some(0.8);
    vec![
        rec(1, vec![
            Effect::PutEntity(new("acme", "acme", 1, fact(json!({"key": "acme", "content": "Acme renews on 1 Nov", "account": "acme"})))),
            Effect::PutEpisode(new("e1", "e1", 1, data(Memory::Episodic, Kind::Episode, json!({"bid_id": "e1", "won": false})))),
        ]),
        rec(2, vec![Effect::Patch {
            memory: Memory::Episodic,
            id: RecordId("e1".into()),
            fields: json!({"won": true, "brand_lift": 0.04}).as_object().unwrap().clone(),
            version: 0,
        }]),
        rec(3, vec![Effect::Supersede {
            old: RecordId("acme".into()),
            new: new("acme-v2", "acme", 2, fact(json!({"key": "acme", "content": "Acme renews on 15 Nov", "account": "acme"}))),
        }]),
        rec(4, vec![Effect::PutEdge(new("edge-1", "edge-1", 1, edge))]),
        rec(5, vec![Effect::Forget { memory: Memory::Episodic, lineage_id: RecordId("e1".into()) }]),
    ]
}

fn ids(rows: &[StoredRow]) -> Vec<String> {
    // Row ids are memory-qualified (`semantic:acme`); compare by key.
    let mut v: Vec<String> = rows.iter().map(|r| key_of(&r.id).to_string()).collect();
    v.sort();
    v
}

#[tokio::test]
async fn d2_rules_on_a_lance_partition() {
    let dir = tempfile::tempdir().unwrap();
    let now = CommitTs(T0 + 1_000_000_000);
    {
        let store = PartitionStore::open(&cfg(dir.path(), 10_000), PartitionId(0)).await.unwrap();
        assert_eq!(store.applied_offset().await.unwrap(), LogOffset(0));
        for r in story() {
            assert!(store.apply(&r).await.unwrap());
        }
        assert_eq!(store.applied_offset().await.unwrap(), LogOffset(5));

        // Before the forget (offset 5), the patch had applied in place.
        let all = store.scan(None).await.unwrap();
        let e1 = all.iter().find(|r| r.id == "episodic:e1").unwrap();
        assert_eq!(e1.data.value["won"], true);
        assert_eq!(e1.data.value["brand_lift"], 0.04);
        assert_eq!(e1.version, 2);
        assert!(e1.tombstone, "forget tombstones the episode");

        // Current view: the superseded fact, the forgotten episode and its edge are gone.
        let cur = store.current(None, now).await.unwrap();
        assert_eq!(ids(&cur), vec!["acme-v2"]);
        assert_eq!(cur[0].data.text.as_deref(), Some("Acme renews on 15 Nov"));
        assert_eq!(cur[0].lineage_id, "semantic:acme");
        assert_eq!(cur[0].key(), "acme");
        assert_eq!(cur[0].version, 2);

        // History is kept: v1 is closed and points at v2.
        let v1 = all.iter().find(|r| r.id == "semantic:acme").unwrap();
        assert_eq!(v1.superseded_by.as_deref(), Some("semantic:acme-v2"));
        assert_eq!(v1.valid_to, Some(CommitTs(T0 + 3_000)));
        let edge = all.iter().find(|r| r.id == "semantic:edge-1").unwrap();
        assert!(edge.tombstone, "forget removes edges touching e1");
        // Edge ends are qualified by the memory type each end belongs to.
        assert_eq!(edge.data.subject_id.as_deref(), Some("semantic:acme"));
        assert_eq!(edge.data.object_id.as_deref(), Some("episodic:e1"));

        // Replaying an applied record is a no-op.
        assert!(!store.apply(&story()[2]).await.unwrap());
        store.close().await.unwrap();
    }

    // Reopen: a new writer replays the MemWAL; state and offset are back.
    let store = PartitionStore::open(&cfg(dir.path(), 10_000), PartitionId(0)).await.unwrap();
    assert_eq!(store.applied_offset().await.unwrap(), LogOffset(5));
    assert_eq!(ids(&store.current(None, now).await.unwrap()), vec!["acme-v2"]);
    // Filters run in Lance.
    let by_key = store.current(Some("key = 'acme'"), now).await.unwrap();
    assert_eq!(ids(&by_key), vec!["acme-v2"]);
    store.close().await.unwrap();
}

#[tokio::test]
async fn ttl_hides_expired_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = PartitionStore::open(&cfg(dir.path(), 10_000), PartitionId(0)).await.unwrap();
    let mut short = data(Memory::Working, Kind::Entity, json!({"task": "t1"}));
    short.ttl_at = Some(CommitTs(T0 + 1_500)); // expires 0.5 ms after its commit at T0+1000
    store.apply(&rec(1, vec![Effect::PutEntity(new("w1", "w1", 1, short))])).await.unwrap();
    assert_eq!(store.current(None, CommitTs(T0 + 1_200)).await.unwrap().len(), 1);
    assert_eq!(store.current(None, CommitTs(T0 + 2_000)).await.unwrap().len(), 0);
    store.close().await.unwrap();
}

#[tokio::test]
async fn reads_span_sealed_generations() {
    let dir = tempfile::tempdir().unwrap();
    let now = CommitTs(T0 + 1_000_000_000);
    let n = 40u64;
    {
        // Seal every 5 rows: the 40 records end up across several generations.
        let store = PartitionStore::open(&cfg(dir.path(), 5), PartitionId(3)).await.unwrap();
        for i in 1..=n {
            let d = data(Memory::Episodic, Kind::Episode, json!({"bid_id": format!("b{i}"), "i": i}));
            store.apply(&rec(i, vec![Effect::PutEpisode(new(&format!("b{i}"), &format!("b{i}"), 1, d))])).await.unwrap();
        }
        // Rewrite one early row after it was sealed: the newest version must win.
        store
            .apply(&rec(n + 1, vec![Effect::Patch {
                memory: Memory::Episodic,
                id: RecordId("b2".into()),
                fields: json!({"i": 200}).as_object().unwrap().clone(),
                version: 0,
            }]))
            .await
            .unwrap();
        let cur = store.current(None, now).await.unwrap();
        assert_eq!(cur.len(), n as usize);
        assert_eq!(cur.iter().find(|r| r.key() == "b2").unwrap().data.value["i"], 200);
        store.close().await.unwrap();
    }
    let store = PartitionStore::open(&cfg(dir.path(), 5), PartitionId(3)).await.unwrap();
    assert_eq!(store.applied_offset().await.unwrap(), LogOffset(n + 1));
    let cur = store.current(None, now).await.unwrap();
    assert_eq!(cur.len(), n as usize);
    assert_eq!(cur.iter().find(|r| r.key() == "b2").unwrap().data.value["i"], 200);
    store.close().await.unwrap();
}

#[tokio::test]
async fn durable_offset_follows_the_memwal_and_survives_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 8, wal_flush: Duration::from_millis(30) };
    let put = |i: u64| {
        let d = data(Memory::Episodic, Kind::Episode, json!({"bid_id": format!("b{i}")}));
        rec(i, vec![Effect::PutEpisode(new(&format!("b{i}"), &format!("b{i}"), 1, d))])
    };
    let durable = {
        let store = PartitionStore::open(&cfg, PartitionId(1)).await.unwrap();
        assert_eq!(store.durable_offset().await.unwrap(), LogOffset(0));
        for i in 1..=30 {
            store.apply(&put(i)).await.unwrap(); // seals every 8 rows along the way
        }
        assert!(store.durable_offset().await.unwrap() <= LogOffset(30));
        tokio::time::sleep(Duration::from_millis(400)).await; // past several WAL flushes
        let d = store.durable_offset().await.unwrap();
        assert_eq!(d, LogOffset(30), "all puts durable after the WAL flushed");
        // A few more, then "crash": drop the writer without close.
        for i in 31..=35 {
            store.apply(&put(i)).await.unwrap();
        }
        let d2 = store.durable_offset().await.unwrap();
        assert!(d2 >= d && d2 <= LogOffset(35));
        d2
    };
    let store = PartitionStore::open(&cfg, PartitionId(1)).await.unwrap();
    let recovered = store.applied_offset().await.unwrap();
    assert!(recovered >= durable, "recovered {recovered:?} below the reported durable {durable:?}");
    let rows = store.current(None, CommitTs(T0 + 1_000_000_000)).await.unwrap();
    assert_eq!(rows.len() as u64, recovered.0, "every recovered offset's row is there");
    // The reopened store reports at least what it recovered.
    assert!(store.durable_offset().await.unwrap() >= durable);
    store.close().await.unwrap();
}

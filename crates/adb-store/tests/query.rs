//! W7: DataFusion queries over one LSM snapshot (`PartitionStore::query`).

use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_store::{decode, PartitionStore, StoreConfig};
use datafusion::prelude::{col, lit};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;

fn row(id: &str, version: u32, topic: &str) -> NewRow {
    let value = json!({ "key": id, "topic": topic });
    NewRow {
        id: RecordId(format!("semantic:{id}")),
        lineage_id: RecordId(format!("semantic:{id}")),
        version,
        data: RowData {
            tenant_id: "default".into(),
            scope: "tenant:default".into(),
            visibility: Visibility::Private,
            namespace: None,
            memory: Memory::Semantic,
            kind: Kind::Fact,
            key: Some(id.into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            text: None,
            value,
            sealed: None,
            embedding: Some(vec![half::f16::from_f32(0.5); 4]),
            ttl_at: None,
            provenance: Provenance::default(),
        },
    }
}

fn rec(offset: u64, effects: Vec<Effect>) -> LogRecord {
    LogRecord { partition: PartitionId(0), offset: LogOffset(offset), epoch: Epoch(1), commit_ts: CommitTs(T0 + offset as i64 * 1_000), txn_id: None, effects }
}

/// Newest version per id, the filter applied to it (an old version that matched
/// doesn't leak), projection without the vector; rows in memtables and in flushed
/// generations alike.
#[tokio::test]
async fn query_sees_one_snapshot_newest_per_id() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 3, wal_flush: Duration::from_millis(20), embedding_model: None, cold: None };
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    let mut off = 0;
    for i in 0..8 {
        off += 1;
        store.apply(&rec(off, vec![Effect::PutFact(row(&format!("k{i}"), 1, "a"))])).await.unwrap();
    }
    // k1 moves from topic a to b (a newer row under the same id).
    off += 1;
    store.apply(&rec(off, vec![Effect::PutFact(row("k1", 2, "b"))])).await.unwrap();
    let topic = |t: &str| {
        datafusion::prelude::Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            std::sync::Arc::new(adb_store::filter::json_scalar_udf()),
            vec![col("value"), lit("topic")],
        ))
        .eq(lit(t.to_string()))
    };
    let (a, b) = store
        .query(|ctx| async move {
            let df = ctx.table("memory").await?;
            let cols: Vec<&str> = df.schema().fields().iter().map(|f| f.name().as_str()).filter(|n| *n != "embedding" && *n != "sealed").collect::<Vec<_>>();
            let cols: Vec<String> = cols.into_iter().map(String::from).collect();
            let pick = |df: datafusion::dataframe::DataFrame| df.select_columns(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            let a = pick(df.clone().filter(col("kind").eq(lit("fact")).and(topic("a")))?)?.collect().await?;
            let b = pick(df.filter(topic("b"))?)?.collect().await?;
            Ok((a, b))
        })
        .await
        .unwrap();
    let ids = |bs: &[arrow_array::RecordBatch]| {
        let mut v: Vec<String> = bs.iter().flat_map(|b| decode(b).unwrap()).map(|r| r.data.key.unwrap()).collect();
        v.sort();
        v
    };
    assert_eq!(ids(&a), ["k0", "k2", "k3", "k4", "k5", "k6", "k7"], "k1's old version doesn't match");
    assert_eq!(ids(&b), ["k1"]);
    assert!(a.iter().all(|b| b.column_by_name("embedding").is_none()));
}

/// W7: records applied as one put see each other — a supersede, patch or forget of
/// a record put earlier in the same batch — and match applying them one by one;
/// records at or below the applied offset are skipped.
#[tokio::test]
async fn apply_many_matches_one_by_one() {
    // Bare ids, as the executor logs them: the store qualifies them (`row_id`).
    let row = |id: &str, version: u32, topic: &str| {
        let mut n = row(id, version, topic);
        n.id = RecordId(id.into());
        n.lineage_id = RecordId(id.into());
        n
    };
    let mut fields = serde_json::Map::new();
    fields.insert("topic".into(), json!("patched"));
    let mut v2 = row("k1", 2, "b");
    v2.id = RecordId("k1-v2".into());
    let log = vec![
        rec(1, vec![Effect::PutFact(row("k1", 1, "a")), Effect::PutFact(row("k2", 1, "a")), Effect::PutFact(row("k3", 1, "a"))]),
        rec(2, vec![Effect::Supersede { old: RecordId("k1".into()), new: v2 }]),
        rec(3, vec![Effect::Patch { memory: Memory::Semantic, id: RecordId("k2".into()), fields, version: 0 }]),
        rec(4, vec![Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId("k3".into()) }]),
    ];
    let current = |store: PartitionStore| async move {
        let mut rows: Vec<(String, String, u32)> = store
            .current(Some("kind = 'fact'"), CommitTs(T0 + 1_000_000))
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.lineage_id.clone(), r.data.value["topic"].as_str().unwrap_or("").to_string(), r.version))
            .collect();
        rows.sort();
        rows
    };
    let cfg = |d: &std::path::Path| StoreConfig { root: d.to_path_buf(), embedding_dim: 4, memtable_rows: 1000, wal_flush: Duration::from_millis(20), embedding_model: None, cold: None };
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let one = PartitionStore::open(&cfg(a.path()), PartitionId(0)).await.unwrap();
    for r in &log {
        one.apply(r).await.unwrap();
    }
    let many = PartitionStore::open(&cfg(b.path()), PartitionId(0)).await.unwrap();
    assert_eq!(many.apply_many(&log[..1]).await.unwrap(), 1);
    assert_eq!(many.apply_many(&log).await.unwrap(), 3, "the first is already applied");
    assert_eq!(many.applied_offset().await.unwrap(), LogOffset(4));
    let expected = vec![("semantic:k1".to_string(), "b".to_string(), 2), ("semantic:k2".to_string(), "patched".to_string(), 2)];
    assert_eq!(current(one).await, expected);
    assert_eq!(current(many).await, expected);
}

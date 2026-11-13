//! W7 index spike (`cargo test -p adb-store --release --test index_spike -- --ignored
//! --nocapture`): how long a read by memory type / lineage takes with rows in the
//! memtable, in flushed generations and compacted into the base table. Run 28 Oct
//! also with BTrees on `memory` / `lineage_id` maintained by the MemWAL: at most
//! ~10 % faster, so they were not kept (W7_QUERY_ENGINE.md).

use std::time::{Duration, Instant};

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
use adb_store::{PartitionStore, StoreConfig};
use datafusion::prelude::{col, lit};

const T0: i64 = 1_761_300_000_000_000;

fn row(memory: Memory, key: &str, i: usize) -> NewRow {
    // The store qualifies ids with the memory type (`row_id`).
    let _ = memory.tag();
    NewRow {
        id: RecordId(key.into()),
        lineage_id: RecordId(key.into()),
        version: 1,
        data: RowData {
            tenant_id: "default".into(),
            scope: "tenant:default".into(),
            visibility: Visibility::Private,
            namespace: None,
            memory,
            kind: Kind::Fact,
            key: Some(key.into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            text: Some(format!("fact number {i}")),
            value: serde_json::json!({ "key": key, "topic": format!("t{}", i % 20), "n": i % 997, "content": format!("fact number {i}") }),
            sealed: None,
            embedding: None,
            ttl_at: None,
            provenance: Provenance::default(),
        },
    }
}

async fn time(store: &PartitionStore, label: &str, filter: datafusion::prelude::Expr) {
    let mut times = Vec::new();
    let mut n = 0;
    for _ in 0..15 {
        let f = filter.clone();
        let t = Instant::now();
        n = store
            .query(|ctx| async move {
                let df = ctx.table("memory").await?.filter(f)?;
                let cols: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).filter(|n| n != "embedding" && n != "sealed").collect();
                let b = df.select_columns(&cols.iter().map(String::as_str).collect::<Vec<_>>())?.collect().await?;
                Ok(b.iter().map(|b| b.num_rows()).sum::<usize>())
            })
            .await
            .unwrap();
        times.push(t.elapsed());
    }
    times.sort();
    println!("  {label:<34} {:>8.1} ms  ({n} rows)", times[times.len() / 2].as_secs_f64() * 1000.0);
}

async fn reads(store: &PartitionStore, stage: &str) {
    println!("{stage}:");
    time(store, "memory = working", col("memory").eq(lit("working"))).await;
    time(store, "lineage_id = semantic:k4242", col("lineage_id").eq(lit("semantic:k4242"))).await;
    time(store, "id = semantic:k4242 (pk)", col("id").eq(lit("semantic:k4242"))).await;
    time(store, "memory = semantic (all)", col("memory").eq(lit("semantic"))).await;
}

#[tokio::test]
#[ignore]
async fn index_spike() {
    for (label, memtable_rows) in [("all in the memtable", 50_000), ("flushed generations (memtable 2,000)", 2_000)] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows, wal_flush: Duration::from_millis(200), embedding_model: None, cold: None };
        let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
        let mut off = 0u64;
        for chunk in 0..100 {
            off += 1;
            let mut effects: Vec<Effect> = (0..100).map(|j| Effect::PutFact(row(Memory::Semantic, &format!("k{}", chunk * 100 + j), chunk * 100 + j))).collect();
            if chunk < 2 {
                effects.extend((0..100).map(|j| Effect::PutFact(row(Memory::Working, &format!("w{}", chunk * 100 + j), j))));
            }
            let rec = LogRecord { partition: PartitionId(0), offset: LogOffset(off), epoch: Epoch(1), commit_ts: CommitTs(T0 + off as i64), txn_id: None, effects };
            store.apply(&rec).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("generations: {}", store.generation_count().await.unwrap());
        reads(&store, label).await;
        if memtable_rows == 2_000 {
            store.compact(Duration::from_secs(90 * 86_400), CommitTs(T0 + 1_000_000)).await.unwrap();
            println!("generations after compaction: {}", store.generation_count().await.unwrap());
            reads(&store, "compacted into the base table").await;
        }
    }
}

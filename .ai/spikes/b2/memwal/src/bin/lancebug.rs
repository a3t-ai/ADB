//! Standalone reproduction for the Lance bug report (`.ai/lancebug.md`).
//! Usage: lancebug <empty-local-dir>
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::mem_wal::scanner::{LsmScanner, ShardSnapshot};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance_index::scalar::ScalarIndexParams;
use lance_index::IndexType;
use uuid::Uuid;

fn schema() -> Arc<Schema> {
    let pk = Field::new("id", DataType::Utf8, false).with_metadata(
        [("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into(),
    );
    Arc::new(Schema::new(vec![pk, Field::new("v", DataType::UInt64, true)]))
}

fn batch(k: u64) -> RecordBatch {
    let ids: Vec<String> = (0..20).map(|j| format!("k{k}-{j}")).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(StringArray::from(ids)), Arc::new(UInt64Array::from(vec![k; 20]))],
    )
    .unwrap()
}

#[tokio::main]
async fn main() -> lance::Result<()> {
    let uri = std::env::args().nth(1).expect("usage: lancebug <empty-local-dir>");

    // Base table with a BTree on the primary key; MemWAL maintains it.
    let reader = RecordBatchIterator::new([Ok(batch(0))], schema());
    let mut ds = Dataset::write(reader, &uri, Some(WriteParams::default())).await?;
    ds.create_index(&["id"], IndexType::BTree, Some("id_idx".into()), &ScalarIndexParams::default(), true)
        .await?;
    ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx"]).execute().await?;
    let ds = Arc::new(ds);
    let shard = Uuid::new_v4();

    // Writer 1: large memtable, durable puts (1,000 rows in the WAL), then dies
    // without flushing a generation.
    let w1 = ds
        .mem_wal_writer(shard, ShardWriterConfig::new(shard).with_durable_write(true).with_max_memtable_rows(100_000))
        .await?;
    for k in 1..=50 {
        w1.put(vec![batch(k)]).await?;
    }
    w1.abort().await?;

    // Writer 2: 400-row memtable. Opening it replays the WAL, which seals and
    // flushes memtables *during replay*.
    let w2 = ds
        .mem_wal_writer(shard, ShardWriterConfig::new(shard).with_durable_write(true).with_max_memtable_rows(400))
        .await?;
    let m = w2.manifest().await?.expect("shard manifest");
    for t in &m.sstables {
        let pk = format!("{uri}/_mem_wal/{shard}/{}/_pk_index/page_lookup.lance", t.path);
        println!("generation {} ({}): _pk_index present = {}", t.generation, t.path, std::path::Path::new(&pk).exists());
    }

    // Any LSM read of the shard now fails.
    let mut snap = ShardSnapshot::new(shard).with_current_generation(m.current_generation);
    for t in &m.sstables {
        snap = snap.with_sstable(t.generation, t.path.clone());
    }
    let r = LsmScanner::new(ds.clone(), vec![snap], vec!["id".into()])
        .with_in_memory_memtables(shard, w2.in_memory_memtable_refs().await?)
        .try_into_batch()
        .await;
    match r {
        Ok(b) => println!("LSM scan OK: {} rows", b.num_rows()),
        Err(e) => println!("LSM scan FAILED: {}", e.to_string().lines().next().unwrap_or("")),
    }
    w2.close().await?;
    Ok(())
}

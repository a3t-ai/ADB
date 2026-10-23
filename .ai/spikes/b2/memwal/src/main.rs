//! Spike B2 (+ S10 on paper): does Lance 12.0.0's MemWAL writer path work end to end?
//!
//! 1. Create a base table with an unenforced primary key, initialize MemWAL.
//! 2. Writer 1: durable puts; rows visible in its memtable.
//! 3. Writer 2 opens the same shard while writer 1 is alive (S10): epochs, and
//!    whether writer 1 is fenced on its next put.
//! 4. Writer 2 sees writer 1's durable rows (WAL replay on open).
//! 5. Seal + flush to an SSTable, close, reopen: rows still there.
//! 6. Rough per-put latency for 1-row durable puts on the given store.
//!
//! Usage: b2-memwal <dataset-uri>   (a local dir, or s3://bucket/prefix with AWS_* env)

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use uuid::Uuid;

fn schema() -> Arc<Schema> {
    let pk = Field::new("id", DataType::Utf8, false).with_metadata(
        [("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into(),
    );
    Arc::new(Schema::new(vec![
        pk,
        Field::new("text", DataType::Utf8, true),
        Field::new("log_offset", DataType::UInt64, true),
    ]))
}

fn batch(schema: &Arc<Schema>, ids: std::ops::Range<u64>) -> RecordBatch {
    let id: Vec<String> = ids.clone().map(|i| format!("r{i:08}")).collect();
    let text: Vec<String> = ids.clone().map(|i| format!("row {i}")).collect();
    let off: Vec<u64> = ids.collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(id)),
            Arc::new(StringArray::from(text)),
            Arc::new(UInt64Array::from(off)),
        ],
    )
    .unwrap()
}

async fn memtable_rows(w: &ShardWriter) -> u64 {
    w.scan().await.unwrap().count_rows().await.unwrap()
}

#[tokio::main]
async fn main() -> lance::Result<()> {
    let uri = std::env::args().nth(1).expect("usage: b2-memwal <dataset-uri>");
    let schema = schema();

    // 1. Base table + MemWAL.
    let seed = RecordBatchIterator::new([Ok(batch(&schema, 0..1))], schema.clone());
    let mut ds = Dataset::write(seed, &uri, Some(WriteParams::default())).await?;
    ds.initialize_mem_wal().unsharded().execute().await?;
    println!("[1] base table + MemWAL initialized at {uri}");

    let shard = Uuid::new_v4();
    let cfg = || {
        ShardWriterConfig::new(shard)
            .with_durable_write(true)
            .with_max_wal_flush_interval(Duration::from_millis(5))
            .with_stats_log_interval(None)
    };

    // 2. Writer 1.
    let w1 = ds.mem_wal_writer(shard, cfg()).await?;
    w1.put(vec![batch(&schema, 1..11)]).await?;
    w1.put(vec![batch(&schema, 11..21)]).await?;
    println!("[2] writer1 epoch={} memtable rows={}", w1.epoch(), memtable_rows(&w1).await);

    // 3 + 4. Writer 2 on the same shard while writer 1 is alive.
    let w2 = ds.mem_wal_writer(shard, cfg()).await?;
    println!(
        "[3] writer2 epoch={} (writer1 epoch={}); writer2 memtable rows after open={}",
        w2.epoch(),
        w1.epoch(),
        memtable_rows(&w2).await
    );
    match w1.put(vec![batch(&schema, 21..22)]).await {
        Ok(_) => println!("[3] writer1 put after takeover: ACCEPTED (not fenced)"),
        Err(e) => println!("[3] writer1 put after takeover: rejected: {e}"),
    }
    match w1.check_fenced().await {
        Ok(()) => println!("[3] writer1 check_fenced: not fenced"),
        Err(e) => println!("[3] writer1 check_fenced: fenced: {e}"),
    }
    let _ = w1.abort().await;

    // 5. Seal, flush, close, reopen.
    w2.put(vec![batch(&schema, 100..110)]).await?;
    let before = memtable_rows(&w2).await;
    w2.force_seal_active().await?;
    w2.wait_for_flush_drain().await?;
    let manifest = w2.manifest().await?;
    println!("[5] writer2 rows before seal={before}; manifest after flush: {manifest:?}");
    w2.close().await?;

    let w3 = ds.mem_wal_writer(shard, cfg()).await?;
    println!(
        "[5] writer3 epoch={} memtable rows after reopen={} (flushed rows live in SSTables)",
        w3.epoch(),
        memtable_rows(&w3).await
    );

    // 6. 1-row durable put latency.
    let mut lat = Vec::new();
    for i in 0..200u64 {
        let t = Instant::now();
        w3.put(vec![batch(&schema, 1_000 + i..1_001 + i)]).await?;
        lat.push(t.elapsed());
    }
    lat.sort();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    println!(
        "[6] 200 x 1-row durable put: p50={:?} p90={:?} p99={:?} max={:?}",
        p(0.5),
        p(0.9),
        p(0.99),
        lat[lat.len() - 1]
    );
    println!("[6] wal stats: {:?}", w3.wal_stats());
    w3.close().await?;
    Ok(())
}

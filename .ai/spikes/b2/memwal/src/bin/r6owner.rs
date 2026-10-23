//! R6 (owner failover): where does a MemWAL takeover spend its time?
//!
//!   r6owner <uri-prefix>
//!
//! For each (G flushed generations, N unflushed WAL entries): writer 1 seals G
//! generations, then writes N more batches as N separate WAL entries and dies. A new
//! owner then times: open table → open writer (epoch claim + fence sentinel + WAL
//! replay of N entries) → point lookup of `_meta` through LsmScanner (cold cache).

use std::sync::Arc;
use std::time::{Duration, Instant};

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
    Arc::new(Schema::new(vec![pk, Field::new("log_offset", DataType::UInt64, true)]))
}

/// 20 rows + `_meta`, all at offset k.
fn batch(k: u64) -> RecordBatch {
    let mut ids: Vec<String> = (0..20).map(|j| format!("r{k}-{j}")).collect();
    ids.push("_meta".into());
    RecordBatch::try_new(schema(), vec![Arc::new(StringArray::from(ids)), Arc::new(UInt64Array::from(vec![k; 21]))]).unwrap()
}

fn cfg(shard: Uuid) -> ShardWriterConfig {
    ShardWriterConfig::new(shard)
        .with_durable_write(true) // one WAL entry per put, so N is exact
        .with_max_wal_flush_interval(Duration::from_millis(5))
        .with_max_memtable_rows(10_000_000)
        .with_max_memtable_size(4 << 30)
        .with_max_memtable_batches(1_000_000)
        .with_max_unflushed_memtable_bytes(8 << 30)
        .with_stats_log_interval(None)
}

async fn case(prefix: &str, gens: u64, n: u64) {
    let uri = format!("{prefix}/g{gens}-n{n}-{}", Uuid::new_v4().simple());
    let mut ds = Dataset::write(RecordBatchIterator::new([Ok(batch(0))], schema()), &uri, Some(WriteParams::default()))
        .await
        .unwrap();
    ds.create_index(&["id"], IndexType::BTree, Some("id_idx".into()), &ScalarIndexParams::default(), true)
        .await
        .unwrap();
    ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx"]).execute().await.unwrap();
    let ds = Arc::new(ds);
    let shard = Uuid::new_v4();
    let w1 = ds.mem_wal_writer(shard, cfg(shard)).await.unwrap();
    let mut k = 1;
    for _ in 0..gens {
        for _ in 0..3 {
            w1.put(vec![batch(k)]).await.unwrap();
            k += 1;
        }
        w1.force_seal_active().await.unwrap().wait().await.unwrap();
    }
    for _ in 0..n {
        w1.put(vec![batch(k)]).await.unwrap();
        k += 1;
    }
    let want = k - 1;
    w1.abort().await.unwrap();

    // The new owner.
    let t = Instant::now();
    let base = Arc::new(Dataset::open(&uri).await.unwrap());
    let t_open = t.elapsed();
    let t = Instant::now();
    let w2 = base.mem_wal_writer(shard, cfg(shard)).await.unwrap();
    let t_writer = t.elapsed();
    let t = Instant::now();
    let m = w2.manifest().await.unwrap().unwrap();
    let mut snap = ShardSnapshot::new(shard).with_current_generation(m.current_generation);
    for s in &m.sstables {
        snap = snap.with_sstable(s.generation, s.path.clone());
    }
    let b = LsmScanner::new(base.clone(), vec![snap], vec!["id".into()])
        .with_in_memory_memtables(shard, w2.in_memory_memtable_refs().await.unwrap())
        .filter("id = '_meta'")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let t_meta = t.elapsed();
    let got = b.column_by_name("log_offset").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap().value(0);
    println!(
        "  G={gens:<3} N={n:<4} open table {:>8.0?}  open writer+replay {:>8.0?}  _meta lookup {:>8.0?}  TOTAL {:>8.0?}   (_meta={got}, expected {want})",
        t_open,
        t_writer,
        t_meta,
        t_open + t_writer + t_meta
    );
    w2.close().await.unwrap();
}

/// "Raft is the WAL": no periodic MemWAL WAL flush and non-durable puts, so MemWAL writes
/// WAL entries only when ADB seals. A crash loses the unsealed tail, which ADB replays
/// from its local Raft log; the takeover replays (almost) nothing from object storage.
async fn noflush(prefix: &str) {
    let uri = format!("{prefix}/noflush-{}", Uuid::new_v4().simple());
    let mut ds = Dataset::write(RecordBatchIterator::new([Ok(batch(0))], schema()), &uri, Some(WriteParams::default()))
        .await
        .unwrap();
    ds.create_index(&["id"], IndexType::BTree, Some("id_idx".into()), &ScalarIndexParams::default(), true)
        .await
        .unwrap();
    ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx"]).execute().await.unwrap();
    let ds = Arc::new(ds);
    let shard = Uuid::new_v4();
    let mut c = cfg(shard).with_durable_write(false);
    c.max_wal_flush_interval = None;
    let w1 = ds.mem_wal_writer(shard, c.clone()).await.unwrap();
    let t = Instant::now();
    for k in 1..=500u64 {
        w1.put(vec![batch(k)]).await.unwrap();
        if k % 100 == 0 && k <= 400 {
            w1.force_seal_active().await.unwrap().wait().await.unwrap();
        }
    }
    let put_time = t.elapsed();
    let wal_entries = w1.wal_stats().next_wal_entry_position - 1;
    tokio::time::sleep(Duration::from_secs(2)).await; // nothing should flush on its own
    let wal_entries_later = w1.wal_stats().next_wal_entry_position - 1;
    w1.abort().await.unwrap();

    let t = Instant::now();
    let base = Arc::new(Dataset::open(&uri).await.unwrap());
    let w2 = base.mem_wal_writer(shard, c).await.unwrap();
    let t_writer = t.elapsed();
    let m = w2.manifest().await.unwrap().unwrap();
    let rows = w2.scan().await.unwrap().count_rows().await.unwrap();
    println!(
        "  noflush: 500 puts in {put_time:.0?} (4 seals); WAL entries written = {wal_entries} (2 s later: {wal_entries_later}); \
         takeover open+writer = {t_writer:.0?}; generations = {}; replayed memtable rows = {rows} \
         → durable through offset 400, offsets 401–500 come from the Raft log",
        m.sstables.len()
    );
    w2.close().await.unwrap();
}

#[tokio::main]
async fn main() {
    let prefix = std::env::args().nth(1).expect("usage: r6owner <uri-prefix>");
    if std::env::args().nth(2).as_deref() == Some("noflush") {
        noflush(&prefix).await;
        return;
    }
    println!("r6owner on {prefix}:");
    for (g, n) in [(0, 0), (0, 10), (0, 40), (0, 160), (20, 0), (20, 40)] {
        case(&prefix, g, n).await;
    }
}

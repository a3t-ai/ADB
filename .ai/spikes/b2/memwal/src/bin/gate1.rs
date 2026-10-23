//! Gate 1 benchmarks: memtable option A (Lance MemWAL) vs option B (ADB memtable +
//! periodic `merge_insert`), on the ADB-shaped schema.
//!
//!   gate1 seed   <uri> <rows>                               base table + id/subject/FTS/vector indexes
//!   gate1 writeb <uri> <secs> <batches/s> <rows/batch> <flush_ms> [merge|append]   option B write path (S3/S6)
//!   gate1 writea <uri> <secs> <batches/s> <rows/batch>              option A write path + LSM reads (S3/S4/S5/S6)
//!   gate1 readb  <uri> <queries>                            reads on the base table (S4, option B's Lance side)
//!   gate1 s7     <prefix> <tables> <concurrency> [create]  open → query → close per table (S7)
//!
//! Every command prints object-store request counts from Lance's IOTracker
//! (get_ranges counts once per call; lists once per call).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{
    FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::mem_wal::scanner::{DatasetCache, LsmScanner, ShardSnapshot, SsTableCache};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter, ShardWriterConfig};
use lance::dataset::{Dataset, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance::index::DatasetIndexExt;
use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams};
use lance_index::IndexType;
use lance_io::object_store::ObjectStoreParams;
use lance_io::utils::tracking_store::IOTracker;
use lance_linalg::distance::MetricType;
use uuid::Uuid;

const DIM: i32 = 768;
const VOCAB: u64 = 5_000;
const SUBJECTS: u64 = 5_000;

// ---------- data ----------

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn f32(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32 * 2.0 - 1.0
    }
}

fn schema() -> Arc<Schema> {
    let pk = Field::new("id", DataType::Utf8, false).with_metadata(
        [("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into(),
    );
    Arc::new(Schema::new(vec![
        pk,
        Field::new("kind", DataType::Utf8, true),
        Field::new("subject_id", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            true,
        ),
        Field::new("log_offset", DataType::UInt64, true),
    ]))
}

/// `n` rows with ids `{prefix}{start..start+n}`, all at `offset`.
fn rows(prefix: &str, start: u64, n: u64, offset: u64, rng: &mut Rng) -> RecordBatch {
    let ids: Vec<String> = (start..start + n).map(|i| format!("{prefix}{i:010}")).collect();
    let kinds: Vec<&str> = (0..n).map(|i| if i % 4 == 0 { "episode" } else { "fact" }).collect();
    let subj: Vec<String> = (0..n).map(|_| format!("s{}", rng.below(SUBJECTS))).collect();
    let text: Vec<String> = (0..n)
        .map(|_| (0..20).map(|_| format!("w{}", rng.below(VOCAB))).collect::<Vec<_>>().join(" "))
        .collect();
    let vals: Vec<f32> = (0..n * DIM as u64).map(|_| rng.f32()).collect();
    let emb = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        DIM,
        Arc::new(Float32Array::from(vals)),
        None,
    )
    .unwrap();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(kinds)),
            Arc::new(StringArray::from(subj)),
            Arc::new(StringArray::from(text)),
            Arc::new(emb),
            Arc::new(UInt64Array::from(vec![offset; n as usize])),
        ],
    )
    .unwrap()
}

fn query_vec(rng: &mut Rng) -> Float32Array {
    Float32Array::from((0..DIM).map(|_| rng.f32()).collect::<Vec<_>>())
}

// ---------- io + stats ----------

fn store(tracker: &Arc<IOTracker>) -> ObjectStoreParams {
    ObjectStoreParams { object_store_wrapper: Some(tracker.clone()), ..Default::default() }
}

async fn open(uri: &str, tracker: &Arc<IOTracker>) -> Dataset {
    DatasetBuilder::from_uri(uri).with_store_params(store(tracker)).load().await.unwrap()
}

fn io(label: &str, t: &IOTracker, secs: f64) {
    let s = t.incremental_stats();
    println!(
        "  io[{label}]: reads={} ({:.1}/s, {:.1} MiB) writes={} ({:.1}/s, {:.1} MiB)",
        s.read_iops,
        s.read_iops as f64 / secs,
        s.read_bytes as f64 / 1048576.0,
        s.write_iops,
        s.write_iops as f64 / secs,
        s.written_bytes as f64 / 1048576.0
    );
}

fn pct(label: &str, mut v: Vec<Duration>) {
    if v.is_empty() {
        println!("  {label}: no samples");
        return;
    }
    v.sort();
    let p = |q: f64| v[((v.len() as f64 * q) as usize).min(v.len() - 1)];
    println!(
        "  {label:<38} n={:<5} p50={:>9.2?} p90={:>9.2?} p99={:>9.2?} max={:>9.2?}",
        v.len(),
        p(0.5),
        p(0.9),
        p(0.99),
        v[v.len() - 1]
    );
}

// ---------- seed ----------

async fn seed(uri: &str, n: u64) {
    let tracker = Arc::new(IOTracker::default());
    let mut rng = Rng::new(1);
    let t = Instant::now();
    let chunk = 5_000;
    let batches: Vec<_> = (0..n.div_ceil(chunk))
        .map(|c| Ok(rows("b", c * chunk, chunk.min(n - c * chunk), 0, &mut rng)))
        .collect();
    let params = WriteParams { store_params: Some(store(&tracker)), ..Default::default() };
    let mut ds = Dataset::write(RecordBatchIterator::new(batches, schema()), uri, Some(params))
        .await
        .unwrap();
    println!("seed: wrote {n} rows in {:.1?}", t.elapsed());
    for (name, col, ty) in [("id_idx", "id", IndexType::BTree), ("subj_idx", "subject_id", IndexType::BTree)] {
        let t = Instant::now();
        ds.create_index(&[col], ty, Some(name.into()), &ScalarIndexParams::default(), true)
            .await
            .unwrap();
        println!("seed: {name} (BTree) in {:.1?}", t.elapsed());
    }
    let t = Instant::now();
    ds.create_index(&["text"], IndexType::Inverted, Some("text_idx".into()), &InvertedIndexParams::default(), true)
        .await
        .unwrap();
    println!("seed: text_idx (FTS) in {:.1?}", t.elapsed());
    let t = Instant::now();
    let parts = ((n as f64).sqrt() as usize).clamp(16, 256);
    let vp = VectorIndexParams::ivf_pq(parts, 8, 48, MetricType::Cosine, 50);
    ds.create_index(&["embedding"], IndexType::Vector, Some("vec_idx".into()), &vp, true)
        .await
        .unwrap();
    println!("seed: vec_idx (IVF_PQ, {parts} partitions) in {:.1?}", t.elapsed());
    io("seed", &tracker, 1.0);
}

// ---------- option B: buffer + periodic merge_insert ----------

async fn writeb(uri: &str, secs: u64, rate: u64, rows_per: u64, flush_ms: u64, mode: &str) {
    let tracker = Arc::new(IOTracker::default());
    let ds = Arc::new(open(uri, &tracker).await);
    let _ = tracker.incremental_stats();
    // (arrival, batch) pending in "ADB's memtable"
    let pending: Arc<Mutex<Vec<(Instant, RecordBatch)>>> = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();
    let gen = {
        let pending = pending.clone();
        tokio::spawn(async move {
            let mut rng = Rng::new(7);
            let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
            for k in 0..secs * rate {
                tick.tick().await;
                let b = rows("n", k * rows_per, rows_per, 1 + k, &mut rng);
                pending.lock().unwrap().push((Instant::now(), b));
            }
        })
    };
    let (mut commits, mut lags, mut ds, mut done) = (Vec::new(), Vec::new(), ds, 0u64);
    loop {
        tokio::time::sleep(Duration::from_millis(flush_ms)).await;
        let take: Vec<_> = std::mem::take(&mut *pending.lock().unwrap());
        if take.is_empty() {
            if gen.is_finished() {
                break;
            }
            continue;
        }
        let batches: Vec<RecordBatch> = take.iter().map(|(_, b)| b.clone()).collect();
        let t = Instant::now();
        ds = if mode == "append" {
            let params = WriteParams { mode: WriteMode::Append, store_params: Some(store(&tracker)), ..Default::default() };
            Arc::new(InsertBuilder::new(ds.clone()).with_params(&params).execute(batches).await.unwrap())
        } else {
            let mut b = MergeInsertBuilder::try_new(ds.clone(), vec!["id".into()]).unwrap();
            b.when_matched(WhenMatched::UpdateIf("source.log_offset > target.log_offset".into()))
                .when_not_matched(WhenNotMatched::InsertAll)
                .conflict_retries(0);
            b.try_build().unwrap().execute_batches(batches).await.unwrap().0
        };
        let end = Instant::now();
        commits.push(end - t);
        lags.extend(take.iter().map(|(a, _)| end - *a));
        done += take.len() as u64;
    }
    let wall = start.elapsed().as_secs_f64();
    println!(
        "writeb [{mode}]: {} batches/s x {} rows, flush every {} ms, {secs} s: {done} batches in {} commits",
        rate,
        rows_per,
        flush_ms,
        commits.len()
    );
    pct("commit", commits);
    pct("apply lag (arrival → in Lance)", lags);
    io("writeb", &tracker, wall);
    println!("  versions after: {}", ds.version().version);
}

// ---------- option A: MemWAL ----------

async fn lsm(base: &Arc<Dataset>, w: &ShardWriter, cache: &Arc<dyn DatasetCache>) -> LsmScanner {
    let m = w.manifest().await.unwrap().unwrap();
    let mut snap = ShardSnapshot::new(w.shard_id()).with_current_generation(m.current_generation);
    for t in &m.sstables {
        snap = snap.with_sstable(t.generation, t.path.clone());
    }
    LsmScanner::new(base.clone(), vec![snap], vec!["id".into()])
        .with_in_memory_memtables(w.shard_id(), w.in_memory_memtable_refs().await.unwrap())
        .with_sstable_cache(cache.clone())
}

async fn count(s: LsmScanner) -> usize {
    let mut st = s.try_into_stream().await.unwrap();
    let mut n = 0;
    while let Some(b) = st.next().await {
        n += b.unwrap().num_rows();
    }
    n
}

async fn writea(uri: &str, secs: u64, rate: u64, rows_per: u64) {
    let tracker = Arc::new(IOTracker::default());
    let mut ds = open(uri, &tracker).await;
    // MAINTAINED=id_idx,vec_idx … (default: all four). Fixed at MemWAL init.
    let maintained: Vec<String> = std::env::var("MAINTAINED")
        .unwrap_or("id_idx,subj_idx,text_idx,vec_idx".into())
        .split(',')
        .map(String::from)
        .collect();
    if ds.mem_wal_index_details().await.unwrap().is_none() {
        let t = Instant::now();
        ds.initialize_mem_wal()
            .unsharded()
            .maintained_indexes(maintained.iter().map(|s| s.as_str()))
            .execute()
            .await
            .unwrap();
        println!("writea: MemWAL initialized (maintained: {maintained:?}) in {:.1?}", t.elapsed());
    }
    let ds = Arc::new(ds);
    let shard = Uuid::new_v4();
    // Raft is the WAL: MemWAL runs non-durable. WAL_FLUSH_MS sets the flush interval
    // (default 1 s); the buffer cap is raised so size doesn't trigger flushes first.
    let wal_ms: u64 = std::env::var("WAL_FLUSH_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let cfg = ShardWriterConfig::new(shard)
        .with_durable_write(false)
        .with_max_wal_flush_interval(Duration::from_millis(wal_ms))
        .with_max_wal_buffer_size(512 << 20)
        .with_max_memtable_rows(20_000)
        .with_stats_log_interval(None);
    let t = Instant::now();
    let w = ds.mem_wal_writer(shard, cfg).await.unwrap();
    println!("writea: writer open in {:.1?}", t.elapsed());
    let _ = tracker.incremental_stats();

    let mut rng = Rng::new(9);
    let mut lat = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
    let start = Instant::now();
    let total = secs * rate;
    for k in 0..total {
        tick.tick().await;
        let b = rows("a", k * rows_per, rows_per, 1 + k, &mut rng);
        let t = Instant::now();
        w.put(vec![b]).await.unwrap();
        lat.push(t.elapsed());
    }
    let wall = start.elapsed().as_secs_f64();
    let m = w.manifest().await.unwrap().unwrap();
    println!(
        "writea: {rate} batches/s x {rows_per} rows, {secs} s, non-durable, WAL flush {wal_ms} ms: \
         {total} puts in {wall:.1} s wall ({:.0} puts/s achieved); flushed generations={} (current_generation={})",
        total as f64 / wall,
        m.sstables.len(),
        m.current_generation
    );
    pct("put (memtable insert)", lat);
    io("writea", &tracker, wall);

    // Reads over base + flushed SSTables + live memtable.
    let q = 30;
    let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(16));
    let (mut pk, mut pk_fresh, mut subj, mut vec, mut fts) = (vec![], vec![], vec![], vec![], vec![]);
    let mut fresh_hits = 0;
    let t0 = Instant::now();
    for i in 0..q {
        let id = format!("b{:010}", rng.below(10_000));
        let t = Instant::now();
        let mut s = lsm(&ds, &w, &cache).await;
        s = s.filter(&format!("id = '{id}'")).unwrap();
        count(s).await;
        pk.push(t.elapsed());

        let id = format!("a{:010}", (total - 1 - (i % 10)) * rows_per); // just written
        let t = Instant::now();
        let s = lsm(&ds, &w, &cache).await.filter(&format!("id = '{id}'")).unwrap();
        fresh_hits += count(s).await;
        pk_fresh.push(t.elapsed());

        let t = Instant::now();
        let s = lsm(&ds, &w, &cache).await.filter(&format!("subject_id = 's{}'", rng.below(SUBJECTS))).unwrap();
        count(s).await;
        subj.push(t.elapsed());

        let qv = query_vec(&mut rng);
        let t = Instant::now();
        let s = lsm(&ds, &w, &cache).await.nearest("embedding", &qv, 10).unwrap();
        count(s).await;
        vec.push(t.elapsed());

        let t = Instant::now();
        let s = lsm(&ds, &w, &cache)
            .await
            .full_text_search(FullTextSearchQuery::new(format!("w{}", rng.below(VOCAB))).with_column("text".into()).unwrap().limit(Some(10)))
            .unwrap();
        count(s).await;
        fts.push(t.elapsed());
    }
    println!("writea: LSM reads (base + {} SSTables + memtable), {q} each:", m.sstables.len());
    pct("point lookup (base row)", pk);
    pct("point lookup (just-written row)", pk_fresh);
    println!("  just-written rows found: {fresh_hits}/{q}");
    pct("subject_id scan", subj);
    pct("vector top-10", vec);
    pct("FTS top-10", fts);
    io("lsm reads", &tracker, t0.elapsed().as_secs_f64());
    w.close().await.unwrap();
}

// ---------- reads on the base table (option B's Lance side) ----------

async fn readb(uri: &str, q: usize) {
    let tracker = Arc::new(IOTracker::default());
    let t = Instant::now();
    let ds = open(uri, &tracker).await;
    println!("readb: open in {:.1?}, version {}, {} rows", t.elapsed(), ds.version().version, ds.count_rows(None).await.unwrap());
    let mut rng = Rng::new(3);
    for pass in ["cold", "warm"] {
        let _ = tracker.incremental_stats();
        let t0 = Instant::now();
        let (mut pk, mut subj, mut vec, mut fts) = (vec![], vec![], vec![], vec![]);
        for _ in 0..q {
            let t = Instant::now();
            ds.scan().filter(&format!("id = 'b{:010}'", rng.below(10_000))).unwrap().try_into_batch().await.unwrap();
            pk.push(t.elapsed());
            let t = Instant::now();
            ds.scan().filter(&format!("subject_id = 's{}'", rng.below(SUBJECTS))).unwrap().try_into_batch().await.unwrap();
            subj.push(t.elapsed());
            let qv = query_vec(&mut rng);
            let t = Instant::now();
            ds.scan().nearest("embedding", &qv, 10).unwrap().nprobes(8).try_into_batch().await.unwrap();
            vec.push(t.elapsed());
            let t = Instant::now();
            ds.scan()
                .full_text_search(FullTextSearchQuery::new(format!("w{}", rng.below(VOCAB))).with_column("text".into()).unwrap().limit(Some(10)))
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            fts.push(t.elapsed());
        }
        println!("readb [{pass}], {q} each:");
        pct("point lookup", pk);
        pct("subject_id scan", subj);
        pct("vector top-10 (nprobes 8)", vec);
        pct("FTS top-10", fts);
        io(pass, &tracker, t0.elapsed().as_secs_f64());
    }
}

// ---------- option B maintenance: compaction + incremental index update (D5) ----------

async fn optimize(uri: &str) {
    let tracker = Arc::new(IOTracker::default());
    let mut ds = open(uri, &tracker).await;
    let before = ds.get_fragments().len();
    let t = Instant::now();
    let m = lance::dataset::optimize::compact_files(&mut ds, Default::default(), None).await.unwrap();
    println!(
        "optimize: compact_files {before} → {} fragments (-{} +{}) in {:.1?}",
        ds.get_fragments().len(),
        m.fragments_removed,
        m.fragments_added,
        t.elapsed()
    );
    let t = Instant::now();
    ds.optimize_indices(&lance_index::optimize::OptimizeOptions::default()).await.unwrap();
    println!("optimize: optimize_indices (all 4, incremental) in {:.1?}", t.elapsed());
    io("optimize", &tracker, 1.0);
}

// ---------- S7: open / first query / close ----------

async fn s7(prefix: &str, tables: usize, conc: usize, create: bool) {
    let tracker = Arc::new(IOTracker::default());
    if create {
        let t = Instant::now();
        let mut set = tokio::task::JoinSet::new();
        let sem = Arc::new(tokio::sync::Semaphore::new(conc));
        for i in 0..tables {
            let (uri, tr, sem) = (format!("{prefix}/p{i:04}"), tracker.clone(), sem.clone());
            set.spawn(async move {
                let _p = sem.acquire().await.unwrap();
                let mut rng = Rng::new(i as u64 + 100);
                let b = rows("b", 0, 200, 0, &mut rng);
                let params = WriteParams { store_params: Some(store(&tr)), ..Default::default() };
                let mut ds = Dataset::write(RecordBatchIterator::new([Ok(b)], schema()), &uri, Some(params))
                    .await
                    .unwrap();
                ds.create_index(&["id"], IndexType::BTree, Some("id_idx".into()), &ScalarIndexParams::default(), true)
                    .await
                    .unwrap();
            });
        }
        while set.join_next().await.is_some() {}
        println!("s7: created {tables} tables (200 rows + id BTree) in {:.1?}", t.elapsed());
        io("create", &tracker, t.elapsed().as_secs_f64());
    }
    let lat = Arc::new(Mutex::new(Vec::new()));
    let errs = Arc::new(Mutex::new(Vec::<String>::new()));
    let t = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(conc));
    for i in 0..tables {
        let (uri, tr, sem, lat, errs) =
            (format!("{prefix}/p{i:04}"), tracker.clone(), sem.clone(), lat.clone(), errs.clone());
        set.spawn(async move {
            let _p = sem.acquire().await.unwrap();
            let t = Instant::now();
            let r = async {
                let ds = DatasetBuilder::from_uri(&uri).with_store_params(store(&tr)).load().await?;
                ds.scan().filter("id = 'b0000000042'")?.try_into_batch().await
            }
            .await;
            match r {
                Ok(_) => lat.lock().unwrap().push(t.elapsed()),
                Err(e) => errs.lock().unwrap().push(e.to_string().lines().next().unwrap_or("").chars().take(120).collect()),
            }
        });
    }
    while set.join_next().await.is_some() {}
    let wall = t.elapsed();
    let e = errs.lock().unwrap().clone();
    println!(
        "s7: open → point query → close, {tables} tables, concurrency {conc}: wall {wall:.1?} ({:.0} tables/s), errors {}",
        tables as f64 / wall.as_secs_f64(),
        e.len()
    );
    for x in e.iter().take(3) {
        println!("  error: {x}");
    }
    pct("open-to-first-query", lat.lock().unwrap().clone());
    io("open+query", &tracker, wall.as_secs_f64());
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n = |i: usize, d: u64| a.get(i).map(|s| s.parse().unwrap()).unwrap_or(d);
    match a.get(1).map(String::as_str) {
        Some("seed") => seed(&a[2], n(3, 50_000)).await,
        Some("writeb") => writeb(&a[2], n(3, 30), n(4, 200), n(5, 5), n(6, 1000), a.get(7).map(String::as_str).unwrap_or("merge")).await,
        Some("writea") => writea(&a[2], n(3, 30), n(4, 200), n(5, 5)).await,
        Some("readb") => readb(&a[2], n(3, 30) as usize).await,
        Some("optimize") => optimize(&a[2]).await,
        Some("s7") => s7(&a[2], n(3, 256) as usize, n(4, 32) as usize, a.get(5).map(|s| s == "create").unwrap_or(false)).await,
        _ => eprintln!("usage: see the header of src/bin/gate1.rs"),
    }
}

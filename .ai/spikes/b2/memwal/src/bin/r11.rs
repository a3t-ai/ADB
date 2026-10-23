//! R11: recovery offset and stale-writer safety with option A (Lance MemWAL,
//! non-durable puts, the Raft log as the real WAL).
//!
//! Design under test: each applied log batch k is ONE `put` holding its rows plus the
//! `_meta` row (log_offset = k). A new owner opens the shard writer (new epoch, fence
//! sentinel, WAL replay), reads `_meta` through `LsmScanner`, and replays the log from
//! `_meta + 1`. Batch k upserts 20 of ids r00..r49 with value = k; the expected state
//! after 0..=m is `model(m)` (same scheme as s1s2).
//!
//!   r11 crash  <uri> <runs>      SIGKILL a child applier, reopen as next owner, check
//!   r11 zombie <uri> <rounds>    repeated takeovers while the old writer keeps writing
//!
//! Env: WAL_MS (WAL flush interval, default 20), MEMTABLE_ROWS (default 400 — small, so
//! generations get flushed and compaction-free LSM reads span all levels),
//! CRASH_MAX_MS (kill window, default 2000).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::mem_wal::scanner::{DatasetCache, LsmScanner, ShardSnapshot, SsTableCache};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance_index::scalar::ScalarIndexParams;
use lance_index::IndexType;
use uuid::Uuid;

const IDS: u64 = 50;

fn schema() -> Arc<Schema> {
    let pk = Field::new("id", DataType::Utf8, false).with_metadata(
        [("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into(),
    );
    Arc::new(Schema::new(vec![
        pk,
        Field::new("value", DataType::UInt64, true),
        Field::new("log_offset", DataType::UInt64, true),
    ]))
}

fn ids_of(k: u64) -> impl Iterator<Item = u64> {
    (0..20).map(move |j| (k * 7 + j) % IDS)
}

fn batch(k: u64) -> RecordBatch {
    let mut id: Vec<String> = ids_of(k).map(|i| format!("r{i:02}")).collect();
    id.push("_meta".into());
    let n = id.len();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(id)),
            Arc::new(UInt64Array::from(vec![k; n])),
            Arc::new(UInt64Array::from(vec![k; n])),
        ],
    )
    .unwrap()
}

fn model(m: u64) -> BTreeMap<String, u64> {
    let mut s = BTreeMap::new();
    for k in 0..=m {
        for i in ids_of(k) {
            s.insert(format!("r{i:02}"), k);
        }
        s.insert("_meta".into(), k);
    }
    s
}

fn env(name: &str, d: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn adb_seals() -> bool {
    std::env::var("ADB_SEALS").is_ok()
}

/// ADB_SEALS=1 (workaround for the replay-flush bug): Lance's automatic seal limits are
/// set so high that WAL replay never seals mid-replay; ADB seals itself (`put_and_seal`).
fn cfg(shard: Uuid) -> ShardWriterConfig {
    let c = ShardWriterConfig::new(shard)
        .with_durable_write(false)
        .with_max_wal_flush_interval(Duration::from_millis(env("WAL_MS", 20)))
        .with_stats_log_interval(None);
    if adb_seals() {
        c.with_max_memtable_rows(10_000_000)
            .with_max_memtable_size(4 << 30)
            .with_max_memtable_batches(1_000_000)
            .with_max_unflushed_memtable_bytes(8 << 30)
    } else {
        c.with_max_memtable_rows(env("MEMTABLE_ROWS", 400) as usize)
    }
}

/// Put, and in ADB_SEALS mode seal the memtable once it reaches MEMTABLE_ROWS.
async fn put_and_seal(w: &ShardWriter, b: RecordBatch) -> lance::Result<Option<lance::dataset::mem_wal::SealFence>> {
    w.put(vec![b]).await?;
    if adb_seals() && w.memtable_stats().await?.row_count >= env("MEMTABLE_ROWS", 400) as usize {
        return Ok(Some(w.force_seal_active().await?));
    }
    Ok(None)
}

/// R12 part 2 — ADB's MemWAL GC: delete WAL entries the shard manifest says replay no
/// longer needs (positions ≤ `replay_after_wal_entry_position`). Returns the new low mark.
async fn gc_wal(uri: &str, shard: Uuid, w: &ShardWriter, from: u64) -> u64 {
    let m = w.manifest().await.unwrap().unwrap();
    let upto = m.replay_after_wal_entry_position;
    if upto < from {
        return from;
    }
    let (store, base) = lance_io::object_store::ObjectStore::from_uri(uri).await.unwrap();
    let dir = base.child("_mem_wal").child(shard.to_string()).child("wal");
    for pos in from..=upto {
        let _ = store.delete(&dir.child(lance::dataset::mem_wal::util::wal_entry_filename(pos))).await;
    }
    upto + 1
}

/// Full state as seen by `w` (memtable + flushed generations + base), newest per id.
async fn state(base: &Arc<Dataset>, w: &ShardWriter, cache: &Arc<dyn DatasetCache>) -> BTreeMap<String, u64> {
    let m = w.manifest().await.unwrap().unwrap();
    let mut snap = ShardSnapshot::new(w.shard_id()).with_current_generation(m.current_generation);
    for t in &m.sstables {
        snap = snap.with_sstable(t.generation, t.path.clone());
    }
    let b = LsmScanner::new(base.clone(), vec![snap], vec!["id".into()])
        .with_in_memory_memtables(w.shard_id(), w.in_memory_memtable_refs().await.unwrap())
        .with_sstable_cache(cache.clone())
        .try_into_batch()
        .await
        .unwrap();
    let id = b.column_by_name("id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
    let v = b.column_by_name("value").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut s = BTreeMap::new();
    for r in 0..b.num_rows() {
        if s.insert(id.value(r).to_string(), v.value(r)).is_some() {
            panic!("duplicate primary key {} in one LSM read", id.value(r));
        }
    }
    s
}

fn check(s: &BTreeMap<String, u64>) -> Result<u64, String> {
    let meta = *s.get("_meta").ok_or("no _meta")?;
    let want = model(meta);
    if *s == want {
        return Ok(meta);
    }
    let bad: Vec<String> = want
        .iter()
        .filter(|(k, v)| s.get(*k) != Some(v))
        .take(4)
        .map(|(k, v)| format!("{k}: have {:?} want {v}", s.get(k)))
        .collect();
    Err(format!("meta={meta}: {}", bad.join(", ")))
}

/// Base table with batch 0, BTree on id, MemWAL (maintained: id) — returns the shard id.
async fn create(uri: &str) -> Uuid {
    let _ = std::fs::remove_dir_all(uri);
    let mut ds = Dataset::write(RecordBatchIterator::new([Ok(batch(0))], schema()), uri, Some(WriteParams::default()))
        .await
        .unwrap();
    ds.create_index(&["id"], IndexType::BTree, Some("id_idx".into()), &ScalarIndexParams::default(), true)
        .await
        .unwrap();
    ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx"]).execute().await.unwrap();
    Uuid::new_v4()
}

/// Become the owner: open the writer (new epoch + replay), recover `_meta`.
async fn take_over(uri: &str, shard: Uuid, cache: &Arc<dyn DatasetCache>) -> (Arc<Dataset>, ShardWriter, Result<u64, String>) {
    let base = Arc::new(Dataset::open(uri).await.unwrap());
    let w = base.mem_wal_writer(shard, cfg(shard)).await.unwrap();
    let r = check(&state(&base, &w, cache).await);
    (base, w, r)
}

fn rng() -> impl FnMut(u64) -> u64 {
    let mut x = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1;
    move |n| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n.max(1)
    }
}

// ---------- crash ----------

async fn crash_child(uri: &str, shard: &str) {
    let shard = Uuid::parse_str(shard).unwrap();
    let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(64));
    let (_base, w, r) = take_over(uri, shard, &cache).await;
    let recovered = r.expect("child found an inconsistent table");
    let mut k = recovered + 1;
    // R12: map writer-global batch numbers to log offsets. Everything present after
    // replay is durable; each later put is one batch carrying offset k.
    let st = w.memtable_stats().await.unwrap();
    let base_batches = st.global_offset + st.batch_count;
    let mut offsets: Vec<u64> = Vec::new(); // offsets[i] = log offset of batch base_batches + i
    let claims = std::env::var("CLAIMS_FILE").ok();
    let mut claims_f = claims.as_ref().map(|p| std::fs::OpenOptions::new().create(true).append(true).open(p).unwrap());
    let mut last_claim = 0;
    let gc = std::env::var("WAL_GC").is_ok();
    let mut gc_from = if gc { gc_wal(uri, shard, &w, 1).await } else { 1 };
    loop {
        if let Some(fence) = put_and_seal(&w, batch(k)).await.unwrap() {
            if gc {
                fence.wait().await.unwrap();
                gc_from = gc_wal(uri, shard, &w, gc_from).await;
            }
        }
        offsets.push(k);
        k += 1;
        if let Some(f) = claims_f.as_mut() {
            if offsets.len() % 10 == 0 {
                // Claimed durable offset = offset of the newest WAL-durable batch; this is
                // what ADB would let Raft purge below.
                let durable = w.memtable_stats().await.unwrap().durable_batch_count;
                let claim = if durable > base_batches { offsets[durable - base_batches - 1] } else { recovered };
                if claim > last_claim {
                    use std::io::Write;
                    writeln!(f, "{claim}").unwrap();
                    f.sync_data().unwrap();
                    last_claim = claim;
                }
            }
        }
    }
}

async fn crash(uri: &str, runs: u64) {
    let shard = create(uri).await;
    let exe = std::env::current_exe().unwrap();
    let max_ms = env("CRASH_MAX_MS", 2000);
    let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(64));
    let mut rand = rng();
    let (mut bad, mut last, mut lost_runs, mut gens) = (0, 0u64, 0, 0usize);
    let mut takeover_times = Vec::new();
    let (mut claim_violations, mut claims_checked) = (0, 0u64);
    for run in 0..runs {
        let mut child = std::process::Command::new(&exe)
            .args(["crash-child", uri, &shard.to_string()])
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50 + rand(max_ms))).await;
        child.kill().unwrap();
        child.wait().unwrap();
        let t = std::time::Instant::now();
        let (_base, w, r) = take_over(uri, shard, &cache).await;
        takeover_times.push(t.elapsed());
        if let Ok(p) = std::env::var("CLAIMS_FILE") {
            let max_claim = std::fs::read_to_string(&p).unwrap_or_default().lines().filter_map(|l| l.parse::<u64>().ok()).max().unwrap_or(0);
            if let Ok(m) = &r {
                if *m < max_claim {
                    claim_violations += 1;
                    println!("  run {run}: CLAIM VIOLATION — recovered _meta {m} < claimed durable {max_claim}");
                }
            }
            claims_checked = claims_checked.max(max_claim);
        }
        match r {
            Ok(m) => {
                if m < last {
                    lost_runs += 1; // allowed: non-durable, the Raft log replays it
                }
                last = m;
            }
            Err(e) => {
                bad += 1;
                println!("  run {run}: INCONSISTENT {e}");
            }
        }
        gens = w.manifest().await.unwrap().unwrap().sstables.len();
        w.abort().await.unwrap();
    }
    takeover_times.sort();
    let tt = |q: f64| takeover_times[((takeover_times.len() as f64 * q) as usize).min(takeover_times.len() - 1)];
    println!(
        "  takeover (open writer + WAL replay + read _meta): p50={:.2?} p90={:.2?} max={:.2?}",
        tt(0.5),
        tt(0.9),
        takeover_times[takeover_times.len() - 1]
    );
    if std::env::var("CLAIMS_FILE").is_ok() {
        println!("  R12 durable-offset claims: violations={claim_violations}; highest claim checked={claims_checked}");
    }
    println!(
        "crash: runs={runs}: inconsistent={bad}; final _meta={last}; runs where _meta went back \
         (non-durable tail lost, replayed from Raft in ADB)={lost_runs}; generations={gens}"
    );
}

// ---------- zombie ----------

async fn zombie(uri: &str, rounds: u64) {
    let shard = create(uri).await;
    let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(64));
    let mut rand = rng();
    let (mut base, mut owner, r) = take_over(uri, shard, &cache).await;
    let mut next = r.unwrap() + 1;
    let (mut bad, mut zombie_ok, mut zombie_fenced, mut regress, mut lost) = (0, 0u64, 0u64, 0, 0u64);
    for round in 0..rounds {
        // The owner applies a stretch of the log.
        for _ in 0..(5 + rand(30)) {
            put_and_seal(&owner, batch(next)).await.unwrap();
            next += 1;
        }
        let durable_hint = next - 1;
        // The old owner keeps writing while a successor takes over: half the rounds it
        // continues in log order (a realistic slow owner), half it re-puts random old
        // offsets (adversarial).
        let adversarial = round % 2 == 1;
        let stop = Arc::new(AtomicBool::new(false));
        // The old owner writes in log order while it is the owner; in adversarial rounds
        // it switches to random OLD offsets only once the successor has opened (fenced).
        let taken_over = Arc::new(AtomicBool::new(false));
        let ok = Arc::new(AtomicU64::new(0));
        let fenced = Arc::new(AtomicU64::new(0));
        let z = {
            let (stop, ok, fenced, taken_over) = (stop.clone(), ok.clone(), fenced.clone(), taken_over.clone());
            let mut zk = next;
            let mut zr = rng();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let k = if adversarial && taken_over.load(Ordering::Relaxed) {
                        1 + zr(zk - 1)
                    } else {
                        let k = zk;
                        zk += 1;
                        k
                    };
                    match put_and_seal(&owner, batch(k)).await {
                        Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                        Err(_) => {
                            fenced.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            0
                        }
                    };
                }
                let _ = owner.abort().await;
            })
        };
        tokio::time::sleep(Duration::from_millis(rand(40))).await;
        eprintln!("== round {round}: takeover begins (zombie {})", if adversarial { "adversarial" } else { "in-order" });
        let (b2, w2, r) = take_over(uri, shard, &cache).await;
        taken_over.store(true, Ordering::Relaxed);
        eprintln!("== round {round}: takeover done, new epoch {}", w2.epoch());
        // Let the zombie keep trying against the new owner for a while.
        tokio::time::sleep(Duration::from_millis(30 + rand(60))).await;
        stop.store(true, Ordering::Relaxed);
        z.await.unwrap();
        eprintln!("== round {round}: zombie stopped");
        // Local runs: every generation in the manifest must have its _pk_index sidecar.
        if !uri.contains("://") {
            let m = w2.manifest().await.unwrap().unwrap();
            let missing: Vec<String> = m
                .sstables
                .iter()
                .filter(|t| !std::path::Path::new(&format!("{uri}/_mem_wal/{shard}/{}/_pk_index/page_lookup.lance", t.path)).exists())
                .map(|t| t.path.clone())
                .collect();
            if !missing.is_empty() {
                println!("  round {round}: manifest v{} (epoch {}) lists generations without _pk_index: {missing:?}", m.version, m.writer_epoch);
                eprintln!("== STOP: missing _pk_index {missing:?}");
                return;
            }
        }
        let after = check(&state(&b2, &w2, &cache).await);
        zombie_ok += ok.load(Ordering::Relaxed);
        zombie_fenced += fenced.load(Ordering::Relaxed);
        match (r, after) {
            (Ok(m), Ok(m2)) => {
                if m2 < m {
                    regress += 1;
                    println!("  round {round}: _meta went back after takeover {m} → {m2}");
                }
                if m < durable_hint {
                    lost += durable_hint - m;
                }
                next = m2 + 1;
            }
            (a, b) => {
                bad += 1;
                println!("  round {round} ({}): at takeover {a:?}, after zombie {b:?}", if adversarial { "adversarial" } else { "in-order" });
                next = a.or(b).unwrap_or(0) + 1;
            }
        }
        base = b2;
        owner = w2;
    }
    let _ = base;
    owner.close().await.unwrap();
    println!(
        "zombie: rounds={rounds}: inconsistent={bad}; _meta regressions after takeover={regress}; \
         zombie puts accepted into its own memtable={zombie_ok}, rejected as fenced={zombie_fenced}; \
         batches not yet flushed at takeover (replayed from Raft in ADB)={lost}"
    );
}

/// Minimal repro: does a generation flushed from a WAL-replayed memtable get its
/// `_pk_index` sidecar, and can LSM reads still open it?
async fn repro(uri: &str, durable: bool) {
    let shard = create(uri).await;
    let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(8));
    let base = Arc::new(Dataset::open(uri).await.unwrap());
    let c = cfg(shard).with_durable_write(durable);
    // The old owner gets a big memtable (never flushes); the new owner a small one, so
    // replay overflows it and flushes a generation while the writer is still opening.
    let w1 = base.mem_wal_writer(shard, c.clone().with_max_memtable_rows(100_000)).await.unwrap();
    let nb = env("REPRO_BATCHES", 5);
    for k in 1..=nb {
        w1.put(vec![batch(k)]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await; // let the WAL flush
    w1.abort().await.unwrap(); // owner dies; nothing flushed to a generation
    let w2 = base.mem_wal_writer(shard, c).await.unwrap(); // replays the WAL
    println!("repro[durable={durable}, {nb} batches]: after replay, memtable rows = {}, generations = {}", w2.scan().await.unwrap().count_rows().await.unwrap(), w2.manifest().await.unwrap().unwrap().sstables.len());
    w2.force_seal_active().await.unwrap();
    w2.wait_for_flush_drain().await.unwrap();
    let m = w2.manifest().await.unwrap().unwrap();
    for t in &m.sstables {
        let dir = format!("{uri}/_mem_wal/{shard}/{}", t.path);
        let has = std::path::Path::new(&format!("{dir}/_pk_index/page_lookup.lance")).exists();
        println!("  generation {} ({}): _pk_index present = {has}", t.generation, t.path);
    }
    let r = tokio::time::timeout(Duration::from_secs(30), async {
        std::panic::AssertUnwindSafe(state(&base, &w2, &cache)).await
    })
    .await;
    match r {
        Ok(s) => println!("  LSM read after flush: {:?}", check(&s)),
        Err(_) => println!("  LSM read timed out"),
    }
    // Control: a generation flushed by the original (non-replayed) writer.
    w2.put(vec![batch(nb + 1)]).await.unwrap();
    w2.force_seal_active().await.unwrap();
    w2.wait_for_flush_drain().await.unwrap();
    let m = w2.manifest().await.unwrap().unwrap();
    let t = m.sstables.iter().max_by_key(|t| t.generation).unwrap();
    let dir = format!("{uri}/_mem_wal/{shard}/{}", t.path);
    println!(
        "  control generation {} (written live, not replayed): _pk_index present = {}",
        t.generation,
        std::path::Path::new(&format!("{dir}/_pk_index/page_lookup.lance")).exists()
    );
}

/// MEMWAL_LOG=1: print Lance's MemWAL log lines (flushes, fencing) with timestamps.
struct MemWalLog(std::time::Instant);
impl log::Log for MemWalLog {
    fn enabled(&self, m: &log::Metadata) -> bool {
        m.target().contains("mem_wal")
    }
    fn log(&self, r: &log::Record) {
        if self.enabled(r.metadata()) {
            eprintln!("[{:>8.3}s {:?}] {}", self.0.elapsed().as_secs_f64(), std::thread::current().id(), r.args());
        }
    }
    fn flush(&self) {}
}

#[tokio::main]
async fn main() {
    if std::env::var("MEMWAL_LOG").is_ok() {
        log::set_boxed_logger(Box::new(MemWalLog(std::time::Instant::now()))).unwrap();
        log::set_max_level(log::LevelFilter::Info);
    }
    let a: Vec<String> = std::env::args().collect();
    let n = |i: usize| a.get(i).map(|s| s.parse().unwrap()).unwrap_or(100);
    match a.get(1).map(String::as_str) {
        Some("crash") => crash(&a[2], n(3)).await,
        Some("crash-child") => crash_child(&a[2], &a[3]).await,
        Some("zombie") => zombie(&a[2], n(3)).await,
        Some("repro") => repro(&a[2], a.get(3).map(|s| s == "durable").unwrap_or(false)).await,
        _ => eprintln!("usage: r11 crash|zombie <uri> [n]"),
    }
}

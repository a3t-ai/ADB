//! Spikes S1 (atomic rows + `_meta` across a crash) and S2 (offset guard: a stale
//! applier can't revert rows), on the plain Lance commit path (memtable option B).
//!
//! Batch k upserts 20 rows (ids `r00`..`r49`, chosen by k) with value = log_offset = k,
//! plus the `_meta` row with log_offset = k. The expected table after applying
//! 0..=m in order is `model(m)`; every check compares a table version with
//! `model(_meta of that version)`.
//!
//! Apply modes:
//!   plain    — UpdateAll, Lance's default conflict retries (what §9.3 implies)
//!   noretry  — UpdateAll, conflict_retries(0)
//!   guarded  — UpdateIf("source.log_offset > target.log_offset"), default retries
//!
//! Usage:
//!   s1s2 stale <dir> <mode>             deterministic stale-applier case
//!   s1s2 race  <dir> <mode> <rounds>    owner vs zombie, every version checked
//!   s1s2 crash <dir> <mode> <runs>      SIGKILL a child applier at random times

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{Dataset, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteParams};

const IDS: u64 = 50;
const ROWS_PER_BATCH: u64 = 20;

fn schema() -> Arc<Schema> {
    let pk = Field::new("id", DataType::Utf8, false).with_metadata(
        [("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into(),
    );
    Arc::new(Schema::new(vec![
        pk,
        Field::new("value", DataType::UInt64, false),
        Field::new("log_offset", DataType::UInt64, false),
    ]))
}

fn ids_of(k: u64) -> impl Iterator<Item = u64> {
    (0..ROWS_PER_BATCH).map(move |j| (k * 7 + j) % IDS)
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

async fn state(ds: &Dataset) -> BTreeMap<String, u64> {
    let b = ds.scan().try_into_batch().await.unwrap();
    let id = b.column_by_name("id").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
    let v = b.column_by_name("value").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut s = BTreeMap::new();
    for r in 0..b.num_rows() {
        if s.insert(id.value(r).to_string(), v.value(r)).is_some() {
            panic!("duplicate primary key {} in one version", id.value(r));
        }
    }
    s
}

/// Ok(meta) if `ds` equals model(meta); Err(description) otherwise.
async fn check(ds: &Dataset) -> Result<u64, String> {
    let s = state(ds).await;
    let meta = *s.get("_meta").ok_or("no _meta row")?;
    let want = model(meta);
    if s == want {
        return Ok(meta);
    }
    let bad: Vec<String> = want
        .iter()
        .filter(|(k, v)| s.get(*k) != Some(v))
        .take(5)
        .map(|(k, v)| format!("{k}: have {:?} want {v}", s.get(k)))
        .collect();
    Err(format!("v{} meta={meta}: {}", ds.version().version, bad.join(", ")))
}

async fn apply(ds: Arc<Dataset>, k: u64, mode: &str) -> lance::Result<(Arc<Dataset>, u32)> {
    let mut b = MergeInsertBuilder::try_new(ds, vec!["id".into()])?;
    b.when_not_matched(WhenNotMatched::InsertAll);
    match mode {
        "plain" => b.when_matched(WhenMatched::UpdateAll),
        "noretry" => b.when_matched(WhenMatched::UpdateAll).conflict_retries(0),
        "guarded" => b.when_matched(WhenMatched::UpdateIf(
            "source.log_offset > target.log_offset".into(),
        )),
        m => panic!("unknown mode {m}"),
    };
    let (ds, stats) = b.try_build()?.execute_batches(vec![batch(k)]).await?;
    Ok((ds, stats.num_attempts))
}

async fn create(dir: &str) -> Arc<Dataset> {
    let _ = std::fs::remove_dir_all(dir);
    let r = RecordBatchIterator::new([Ok(batch(0))], schema());
    Arc::new(Dataset::write(r, dir, Some(WriteParams::default())).await.unwrap())
}

fn rng() -> impl FnMut(u64) -> u64 {
    let mut x = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1;
    move |n| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    }
}

async fn stale(dir: &str, mode: &str) {
    let mut ds = create(dir).await;
    let zombie_view = ds.clone(); // the zombie read _meta = 0 here and expects to apply 1
    for k in 1..=5 {
        ds = apply(ds, k, "plain").await.unwrap().0;
    }
    println!("owner applied 1..=5: {:?}", check(&Dataset::open(dir).await.unwrap()).await);
    let r = apply(zombie_view, 1, mode).await;
    let latest = Dataset::open(dir).await.unwrap();
    match r {
        Ok((_, attempts)) => println!("zombie re-apply of offset 1 [{mode}]: COMMITTED after {attempts} attempt(s)"),
        Err(e) => println!("zombie re-apply of offset 1 [{mode}]: rejected: {}", first_line(&e)),
    }
    println!("table now: {:?}", check(&latest).await);
}

async fn race(dir: &str, mode: &str, rounds: u64) {
    let ds = create(dir).await;
    let applied = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    // Zombie: holds a view a few versions old and re-applies an old offset from it.
    let zombie = {
        let (dir, mode, applied, done) = (dir.to_string(), mode.to_string(), applied.clone(), done.clone());
        tokio::spawn(async move {
            let mut rand = rng();
            let (mut ok, mut err) = (0u64, 0u64);
            while !done.load(Ordering::Relaxed) {
                let a = applied.load(Ordering::Relaxed);
                if a < 2 {
                    tokio::task::yield_now().await;
                    continue;
                }
                let latest = Dataset::open(&dir).await.unwrap();
                // Versions start at 1; stay at or above it.
                let back = 1 + rand((latest.version().version - 1).min(4));
                let view = Arc::new(latest.checkout_version(latest.version().version - back).await.unwrap());
                let k = 1 + rand(a);
                match apply(view, k, &mode).await {
                    Ok(_) => ok += 1,
                    Err(_) => err += 1,
                }
            }
            (ok, err)
        })
    };

    let mut ds = ds;
    let mut owner_conflicts = 0;
    for k in 1..=rounds {
        loop {
            let latest = Arc::new(Dataset::open(dir).await.unwrap());
            // The owner's own guard: only apply k on a table whose _meta is k-1.
            let meta = state(&latest).await["_meta"];
            if meta >= k {
                break; // shouldn't happen: only the owner advances _meta legitimately
            }
            match apply(latest, k, mode).await {
                Ok((d, _)) => {
                    ds = d;
                    break;
                }
                Err(_) => owner_conflicts += 1,
            }
        }
        applied.store(k, Ordering::Relaxed);
    }
    done.store(true, Ordering::Relaxed);
    let (zok, zerr) = zombie.await.unwrap();

    // Check every version.
    let last = Dataset::open(dir).await.unwrap().version().version;
    let (mut bad, mut regress, mut prev_meta) = (0, 0, 0);
    for v in 1..=last {
        let d = ds.checkout_version(v).await.unwrap();
        match check(&d).await {
            Ok(m) => {
                if m < prev_meta {
                    regress += 1;
                }
                prev_meta = prev_meta.max(m);
            }
            Err(e) => {
                if bad < 3 {
                    println!("  inconsistent: {e}");
                }
                bad += 1
            }
        }
    }
    let fin = check(&Dataset::open(dir).await.unwrap()).await;
    println!(
        "race [{mode}] rounds={rounds} versions={last}: zombie commits ok={zok} rejected={zerr}; \
         owner retries={owner_conflicts}; versions != model(_meta): {bad}; _meta regressions: {regress}; final: {fin:?}"
    );
}

async fn crash_child(dir: &str, mode: &str) {
    let mut ds = Arc::new(Dataset::open(dir).await.unwrap());
    let mut k = state(&ds).await["_meta"] + 1;
    loop {
        ds = apply(ds, k, mode).await.unwrap().0;
        k += 1;
    }
}

async fn crash(dir: &str, mode: &str, runs: u64) {
    create(dir).await;
    let exe = std::env::current_exe().unwrap();
    let mut rand = rng();
    // Kill window: 30 ms + up to CRASH_MAX_MS. Must exceed the child's start-up plus
    // a few applies, or the kills never land near a commit (object storage: seconds).
    let max_ms: u64 = std::env::var("CRASH_MAX_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let (mut bad, mut open_fail, mut progress) = (0, 0, 0u64);
    let mut commits_per_run = Vec::new();
    for run in 0..runs {
        let mut child = std::process::Command::new(&exe)
            .args(["crash-child", dir, mode])
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30 + rand(max_ms))).await;
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();
        match Dataset::open(dir).await {
            Ok(d) => match check(&d).await {
                Ok(m) => {
                    commits_per_run.push(m - progress);
                    progress = m
                }
                Err(e) => {
                    bad += 1;
                    println!("  run {run}: {e}");
                }
            },
            Err(e) => {
                open_fail += 1;
                println!("  run {run}: open failed: {}", first_line(&e));
            }
        }
    }
    let zero = commits_per_run.iter().filter(|&&c| c == 0).count();
    println!(
        "crash [{mode}] runs={runs} window=30..{}ms: inconsistent={bad} open_failures={open_fail} \
         final _meta={progress}; runs with 0 commits={zero}, with ≥1={}",
        30 + max_ms,
        commits_per_run.len() - zero
    );
}

/// Sequential apply latency (one 21-row batch per commit) — first S3 number for option B.
async fn applybench(dir: &str, mode: &str, n: u64) {
    let mut ds = create(dir).await;
    let mut lat = Vec::new();
    for k in 1..=n {
        let t = std::time::Instant::now();
        ds = apply(ds, k, mode).await.unwrap().0;
        lat.push(t.elapsed());
    }
    lat.sort();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    println!(
        "applybench [{mode}] n={n}: p50={:?} p90={:?} p99={:?} max={:?}",
        p(0.5),
        p(0.9),
        p(0.99),
        lat[lat.len() - 1]
    );
}

fn first_line(e: &impl std::fmt::Display) -> String {
    e.to_string().lines().next().unwrap_or("").chars().take(200).collect()
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n = |i: usize| a.get(i).map(|s| s.parse().unwrap()).unwrap_or(200);
    match a.get(1).map(String::as_str) {
        Some("stale") => stale(&a[2], &a[3]).await,
        Some("race") => race(&a[2], &a[3], n(4)).await,
        Some("crash") => crash(&a[2], &a[3], n(4)).await,
        Some("crash-child") => crash_child(&a[2], &a[3]).await,
        Some("applybench") => applybench(&a[2], &a[3], n(4)).await,
        _ => eprintln!("usage: s1s2 stale|race|crash <dir> <plain|noretry|guarded> [n]"),
    }
}

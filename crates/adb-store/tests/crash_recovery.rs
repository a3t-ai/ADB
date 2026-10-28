//! Spikes R11 / R12 as regression tests (DELIVERY_PLAN stage D gate).
//!
//! - **R11 crash:** a child process applies a deterministic stream of log records to a
//!   partition store and is SIGKILLed at a random moment; on reopen the table must be
//!   exactly the records `1..=_meta` applied in order — rows and `_meta` land together
//!   (one put per record) or not at all.
//! - **R12 durable offset:** the child appends every offset `durable_offset()` reports
//!   to an fsynced claims file; after the kill, the recovered `_meta` must be at least
//!   every claim. That is the rule the log purge (stage D3.5) relies on.
//! - **R11 zombie:** a second writer on the same partition fences the first; the first
//!   one's later writes never become visible.
//!
//! The child is this test binary run again with `ADB_CRASH_CHILD` set. Rounds:
//! `ADB_CRASH_ROUNDS` (default 12).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use adb_core::log::{
    CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData,
    Visibility,
};
use adb_store::{PartitionStore, StoreConfig};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;
const KEYS: u64 = 40;

fn cfg(dir: &Path) -> StoreConfig {
    // Seal every 25 rows, so kills land during seals and flushed generations too.
    StoreConfig { root: dir.to_path_buf(), embedding_dim: 4, memtable_rows: 25, wal_flush: Duration::from_millis(20), embedding_model: None }
}

fn data(value: serde_json::Value) -> RowData {
    RowData {
        tenant_id: "default".into(),
        scope: "tenant:default".into(),
        visibility: Visibility::Private,
        namespace: None,
        memory: Memory::Episodic,
        kind: Kind::Episode,
        key: None,
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

/// What record `k` does: mostly puts over a small key set (so rows are rewritten
/// across generations), some patches, some forgets.
enum Op {
    Put(String),
    Patch(String),
    Forget(String),
}

fn op(k: u64) -> Op {
    let key = format!("r{}", (k * 7) % KEYS);
    match k % 10 {
        3 => Op::Patch(key),
        7 => Op::Forget(key),
        _ => Op::Put(key),
    }
}

fn record(k: u64) -> LogRecord {
    let effect = match op(k) {
        Op::Put(key) => Effect::PutEpisode(NewRow {
            id: RecordId(key.clone()),
            lineage_id: RecordId(key),
            version: 1,
            data: data(json!({ "k": k })),
        }),
        Op::Patch(key) => Effect::Patch {
            memory: Memory::Episodic,
            id: RecordId(key),
            fields: json!({ "patched": k }).as_object().unwrap().clone(),
            version: 0,
        },
        Op::Forget(key) => Effect::Forget { memory: Memory::Episodic, lineage_id: RecordId(key) },
    };
    LogRecord {
        partition: PartitionId(0),
        offset: LogOffset(k),
        epoch: Epoch(1),
        commit_ts: CommitTs(T0 + k as i64 * 1_000),
        txn_id: None,
        effects: vec![effect],
    }
}

/// The table's current view after records `1..=upto`: key → payload.
fn model(upto: u64) -> BTreeMap<String, serde_json::Value> {
    let mut m: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for k in 1..=upto {
        match op(k) {
            Op::Put(key) => {
                m.insert(key, json!({ "k": k }));
            }
            Op::Patch(key) => {
                if let Some(v) = m.get_mut(&key) {
                    v["patched"] = json!(k);
                }
            }
            Op::Forget(key) => {
                m.remove(&key);
            }
        }
    }
    m
}

async fn current(store: &PartitionStore) -> BTreeMap<String, serde_json::Value> {
    let now = CommitTs(T0 + 1_000_000_000_000);
    store.current(None, now).await.unwrap().into_iter().map(|r| (r.key().to_string(), r.data.value)).collect()
}

/// Child: apply records from where the table is, forever, claiming durable offsets.
#[tokio::test(flavor = "multi_thread")]
async fn crash_child() {
    let Ok(dir) = std::env::var("ADB_CRASH_CHILD") else { return };
    let dir = Path::new(&dir);
    let store = PartitionStore::open(&cfg(&dir.join("store")), PartitionId(0)).await.unwrap();
    let mut claims = std::fs::OpenOptions::new().create(true).append(true).open(dir.join("claims")).unwrap();
    let mut k = store.applied_offset().await.unwrap().0;
    loop {
        k += 1;
        store.apply(&record(k)).await.unwrap();
        if k % 5 == 0 {
            let d = store.durable_offset().await.unwrap().0;
            writeln!(claims, "{d}").unwrap();
            claims.sync_all().unwrap();
        }
    }
}

fn pseudo_random(seed: u64) -> u64 {
    // xorshift; seeded from the clock per round.
    let mut x = seed | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[tokio::test(flavor = "multi_thread")]
async fn sigkill_mid_apply_recovers_a_consistent_table() {
    if std::env::var("ADB_CRASH_CHILD").is_ok() {
        return;
    }
    let rounds: u64 = std::env::var("ADB_CRASH_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(12);
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut last_meta = 0;
    for round in 0..rounds {
        let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        // 0.5–2.5 s: past the child's open and replay (most of the first ~400 ms in
        // a debug build), into its apply loop and its seals.
        let wait = 500 + pseudo_random(seed ^ round) % 2_000;
        let mut child = std::process::Command::new(&exe)
            .args(["--exact", "crash_child", "--nocapture"])
            .env("ADB_CRASH_CHILD", dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(wait)).await;
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();

        let store = PartitionStore::open(&cfg(&dir.path().join("store")), PartitionId(0)).await.unwrap();
        let meta = store.applied_offset().await.unwrap().0;
        // R12: every offset the child claimed durable survived the kill.
        let claimed = std::fs::read_to_string(dir.path().join("claims"))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        assert!(meta >= claimed, "round {round}: _meta {meta} below a claimed durable offset {claimed}");
        assert!(meta >= last_meta, "round {round}: _meta went back from {last_meta} to {meta}");
        // R11: the table is exactly records 1..=_meta.
        assert_eq!(current(&store).await, model(meta), "round {round}: table differs from the model at _meta = {meta}");
        store.close().await.unwrap();
        last_meta = meta;
    }
    assert!(last_meta > 0, "the child never applied anything");
    println!("{rounds} kills, recovered _meta {last_meta}");
}

/// R11 zombie: a second writer on the partition claims it; the first one's later
/// writes fail or stay invisible, and the table is the model of the new writer's.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_writer_cannot_change_the_table() {
    if std::env::var("ADB_CRASH_CHILD").is_ok() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let old = PartitionStore::open(&cfg(dir.path()), PartitionId(0)).await.unwrap();
    for k in 1..=30 {
        old.apply(&record(k)).await.unwrap();
    }
    // Give the old writer's WAL a flush, as a live owner would have had.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let new = PartitionStore::open(&cfg(dir.path()), PartitionId(0)).await.unwrap();
    let taken_over_at = new.applied_offset().await.unwrap().0;
    assert!(taken_over_at <= 30);
    // The zombie keeps writing after the takeover; none of it may land.
    for k in 31..=60 {
        let _ = old.apply(&record(k)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The new owner continues from its own offset.
    for k in taken_over_at + 1..=taken_over_at + 10 {
        new.apply(&record(k)).await.unwrap();
    }
    let upto = taken_over_at + 10;
    assert_eq!(new.applied_offset().await.unwrap().0, upto);
    assert_eq!(current(&new).await, model(upto), "the fenced writer changed the table");
    new.close().await.unwrap();
    // And after reopening (the zombie's WAL entries, if any landed, are replayed).
    let again = PartitionStore::open(&cfg(dir.path()), PartitionId(0)).await.unwrap();
    assert_eq!(again.applied_offset().await.unwrap().0, upto);
    assert_eq!(current(&again).await, model(upto));
}

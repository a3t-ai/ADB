//! W5 spike: rewrite compaction — cost, and that forgotten data is physically gone.

use std::time::Duration;

use adb_core::log::{
    CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility,
};
use adb_store::{PartitionStore, StoreConfig};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;
const DAY: i64 = 86_400_000_000;

fn row(key: &str, content: &str) -> RowData {
    RowData {
        tenant_id: "default".into(),
        scope: "tenant:default".into(),
        visibility: Visibility::Private,
        namespace: None,
        memory: Memory::Semantic,
        kind: Kind::Entity,
        key: Some(key.into()),
        subject_id: None,
        predicate: None,
        object_id: None,
        link_type: None,
        weight: None,
        text: Some(content.into()),
        value: json!({ "key": key, "content": content }),
        sealed: None,
        embedding: None,
        ttl_at: None,
        provenance: Provenance::default(),
    }
}

fn rec(offset: u64, ts: i64, effects: Vec<Effect>) -> LogRecord {
    LogRecord { partition: PartitionId(0), offset: LogOffset(offset), epoch: Epoch(1), commit_ts: CommitTs(ts), txn_id: None, effects }
}

/// Every file under `dir` that contains `needle`.
fn files_with(dir: &std::path::Path, needle: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    for e in walk(dir) {
        if std::fs::read(&e).map(|b| b.windows(needle.len()).any(|w| w == needle)).unwrap_or(false) {
            out.push(e.display().to_string());
        }
    }
    out
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() { out.extend(walk(&p)) } else { out.push(p) }
    }
    out
}

async fn spike(n: usize) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 2_000, wal_flush: Duration::from_millis(50), embedding_model: None };
    let now = CommitTs(T0 + 200 * DAY);
    let mut off = 0u64;
    {
        let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
        // n facts, 100 per record; every even one carries a secret, then is forgotten.
        for chunk in (0..n).collect::<Vec<_>>().chunks(100) {
            off += 1;
            let effects = chunk
                .iter()
                .map(|i| {
                    let content = if i % 2 == 0 { format!("SECRET-{i:08}-forget-me") } else { format!("public fact {i}") };
                    Effect::PutEntity(NewRow { id: RecordId(format!("k{i}")), lineage_id: RecordId(format!("k{i}")), version: 1, data: row(&format!("k{i}"), &content) })
                })
                .collect();
            store.apply(&rec(off, T0 + off as i64, effects)).await.unwrap();
        }
        // Old history: 10 facts superseded 120 days ago (past the 90-day horizon)…
        for i in [1usize, 3, 5, 7, 9, 11, 13, 15, 17, 19] {
            off += 1;
            let new = NewRow { id: RecordId(format!("k{i}-v2")), lineage_id: RecordId(format!("k{i}")), version: 2, data: row(&format!("k{i}"), &format!("public fact {i} v2")) };
            store.apply(&rec(off, T0 + 80 * DAY, vec![Effect::Supersede { old: RecordId(format!("k{i}")), new }])).await.unwrap();
        }
        for chunk in (0..n).step_by(2).collect::<Vec<_>>().chunks(100) {
            off += 1;
            let effects = chunk.iter().map(|i| Effect::Forget { memory: Memory::Semantic, lineage_id: RecordId(format!("k{i}")) }).collect();
            store.apply(&rec(off, T0 + 150 * DAY, effects)).await.unwrap();
        }
        store.close().await.unwrap();
    }
    let secret = b"SECRET-00000002-forget-me";
    let before = files_with(dir.path(), secret);
    let stats = PartitionStore::compact_offline(&cfg, PartitionId(0), Duration::from_secs(90 * 86_400), now).await.unwrap();
    eprintln!(
        "SPIKE n={n}: {} row versions → {} kept in {:?}; disk {} KB → {} KB; secret in {} file(s) before",
        stats.total, stats.kept, stats.took, stats.bytes_before / 1024, stats.bytes_after / 1024, before.len()
    );
    assert!(!before.is_empty(), "the secret should be on disk before compaction (else this test proves nothing)");
    assert!(files_with(dir.path(), secret).is_empty(), "forgotten data must be gone from every file");
    // n/2 public facts (10 of them at version 2), a payload-free stub per forgotten
    // lineage, and _meta; the 10 old versions dropped.
    assert_eq!(stats.kept, n / 2 + n / 2 + 1, "{stats:?}");
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    assert_eq!(store.applied_offset().await.unwrap(), LogOffset(off));
    assert_eq!(store.count_current(Memory::Semantic, now).await.unwrap(), (n / 2) as u64);
    let v = store.current_version(Memory::Semantic, "k3", now).await.unwrap().unwrap();
    assert_eq!(v.data.value["content"], "public fact 3 v2");
}

#[tokio::test]
async fn compaction_1k() {
    spike(1_000).await;
}

#[tokio::test]
#[ignore = "W5 spike at 10k rows (its setup applies 5k forgets one by one: ~12 min in debug)"]
async fn compaction_10k() {
    spike(10_000).await;
}

#[tokio::test]
#[ignore = "W5 spike at 100k rows (slow in debug)"]
async fn compaction_100k() {
    spike(100_000).await;
}

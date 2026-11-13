//! S3 option B, phase 0 (`.ai/S3_OPTION_B_PLAN.md`): a partition table with object
//! storage behind it — the cold base registered at creation, the archive to the
//! mirror, the watermark, local cleanup limited to archived versions, and a rewrite
//! starting a new lineage while the old one's archive stays.
//!
//! Runs against a local directory as the object store. With `ADB_S3_TEST_URI`
//! (e.g. `s3://adb-1/adb-dev/store-tier`) and the `AWS_*` variables, the
//! `--ignored` variant runs the same checks against real object storage.

use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility};
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

/// Apply `records` records of 100 facts each, from offset `from + 1`.
async fn apply(store: &PartitionStore, from: u64, records: u64) -> u64 {
    let mut off = from;
    for _ in 0..records {
        off += 1;
        let effects = (0..100)
            .map(|i| {
                let k = format!("k{off}-{i}");
                Effect::PutEntity(NewRow { id: RecordId(k.clone()), lineage_id: RecordId(k.clone()), version: 1, data: row(&k, &format!("fact {k}")) })
            })
            .collect();
        store.apply(&rec(off, T0 + off as i64, effects)).await.unwrap();
    }
    off
}

fn lineage(root: &std::path::Path) -> String {
    std::fs::read_to_string(root.join("p0/memory.lance/_adb_lineage")).unwrap().trim().to_string()
}

async fn rows(uri: &str) -> usize {
    lance::Dataset::open(uri).await.unwrap().count_rows(None).await.unwrap()
}

/// The manifests under a mirror (a local directory, or a prefix on object storage).
async fn mirror_manifests(mirror: &str) -> usize {
    use futures::TryStreamExt;
    let (store, base) = lance::io::ObjectStore::from_uri(mirror).await.unwrap();
    let list: Vec<_> = store.read_dir_all(&base.child("_versions"), None).try_collect().await.unwrap();
    list.iter().filter(|o| o.location.as_ref().ends_with(".manifest")).count()
}

fn local_manifests(root: &std::path::Path) -> usize {
    std::fs::read_dir(root.join("p0/memory.lance/_versions")).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().ends_with(".manifest")).count()
}

async fn tiered(cold: String) {
    let root = tempfile::tempdir().unwrap();
    let cfg = StoreConfig {
        root: root.path().to_path_buf(),
        embedding_dim: 4,
        memtable_rows: 500,
        wal_flush: Duration::from_millis(20),
        embedding_model: None,
        cold: Some(cold.clone()),
    };
    let history = Duration::from_secs(90 * 86_400);
    let now = CommitTs(T0 + 10 * DAY);
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    let l1 = lineage(root.path());
    let base_uri = format!("{}/p0/{l1}", cold.trim_end_matches('/'));
    let mirror = format!("{base_uri}/mirror");

    // 1. Apply, fold into the base table, archive: the mirror holds it, at the watermark.
    let mut off = apply(&store, 0, 20).await;
    store.compact(history, now).await.unwrap();
    let a = store.archive().await.unwrap();
    eprintln!("archive 1: {a:?}");
    assert!(a.files > 0, "{a:?}");
    assert_eq!(a.watermark, off, "the archived version holds everything applied: {a:?}");
    assert_eq!(store.archive_watermark().1, LogOffset(off));

    // 2. The mirror opens on its own, with the local table's rows.
    let local = root.path().join("p0/memory.lance").to_string_lossy().to_string();
    assert_eq!(rows(&mirror).await, rows(&local).await);
    assert_eq!(rows(&mirror).await, 20 * 100 + 1, "facts and _meta");

    // 3. More writes and folds: local cleanup keeps every unarchived version, and the
    //    mirror keeps every manifest ever written.
    off = apply(&store, off, 20).await;
    store.compact(history, now).await.unwrap();
    off = apply(&store, off, 20).await;
    store.compact(history, now).await.unwrap();
    let b = store.archive().await.unwrap();
    eprintln!("archive 2: {b:?}");
    assert_eq!(b.watermark, off);
    let (on_mirror, on_disk) = (mirror_manifests(&mirror).await, local_manifests(root.path()));
    eprintln!("manifests: {on_mirror} on the mirror, {on_disk} local");
    assert!(on_mirror > on_disk, "local cleanup removed old versions; the mirror kept them ({on_mirror} vs {on_disk})");
    assert_eq!(rows(&mirror).await, 60 * 100 + 1);

    // 4. A full rewrite (here: clearing vectors) is a new lineage; the old one stays.
    store.clear_vectors().await.unwrap();
    let l2 = lineage(root.path());
    assert_ne!(l1, l2, "a rewrite starts a new lineage");
    let c = store.archive().await.unwrap();
    eprintln!("archive 3 (new lineage): {c:?}");
    assert_eq!(rows(&format!("{}/p0/{l2}/mirror", cold.trim_end_matches('/'))).await, 60 * 100 + 1);
    assert_eq!(rows(&mirror).await, 60 * 100 + 1, "the old lineage's archive is kept");
    store.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn tiered_store_on_a_local_object_store() {
    let cold = tempfile::tempdir().unwrap();
    tiered(format!("file://{}", cold.path().display())).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs ADB_S3_TEST_URI and AWS_* (real object storage)"]
async fn tiered_store_on_s3() {
    let Ok(prefix) = std::env::var("ADB_S3_TEST_URI") else { return };
    tiered(format!("{}/run-{}", prefix.trim_end_matches('/'), chrono::Utc::now().timestamp_millis())).await;
}

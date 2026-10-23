//! S11: is FORGET physical? Where does a forgotten scope's data survive?
//!
//!   forget <local-dir>
//!
//! Rows of scope `forget-me` carry the marker text `SECRETMARKER` and embeddings filled
//! with one distinctive f32 (3.1415927). They go into the base table and through
//! MemWAL (flushed generation + WAL). After each forget step every file under the
//! table is byte-scanned for the marker and for the vector bytes.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;


use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::dataset::optimize::compact_files;
use lance::dataset::{Dataset, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance::index::DatasetIndexExt;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::IndexType;
use lance_linalg::distance::MetricType;
use uuid::Uuid;

const DIM: i32 = 64;
const MAGIC: f32 = 3.1415927;

fn schema() -> Arc<Schema> {
    let pk = Field::new("id", DataType::Utf8, false).with_metadata(
        [("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into(),
    );
    Arc::new(Schema::new(vec![
        pk,
        Field::new("scope", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
            true,
        ),
        Field::new("log_offset", DataType::UInt64, true),
    ]))
}

fn rows(prefix: &str, n: usize, forget: bool, seed: u64) -> RecordBatch {
    let mut x = seed | 1;
    let ids: Vec<String> = (0..n).map(|i| format!("{prefix}{i:06}")).collect();
    let scope = vec![if forget { "forget-me" } else { "keep" }; n];
    let text: Vec<String> = (0..n)
        .map(|i| if forget { format!("SECRETMARKER payload {i}") } else { format!("ordinary note {i}") })
        .collect();
    let vals: Vec<f32> = (0..n * DIM as usize)
        .map(|_| {
            if forget {
                MAGIC
            } else {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 40) as f32 / (1u64 << 24) as f32
            }
        })
        .collect();
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
            Arc::new(StringArray::from(scope)),
            Arc::new(StringArray::from(text)),
            Arc::new(emb),
            Arc::new(UInt64Array::from(vec![1u64; n])),
        ],
    )
    .unwrap()
}

/// Group a file path into a category by its first meaningful component.
fn category(rel: &Path) -> String {
    let parts: Vec<String> = rel.iter().map(|p| p.to_string_lossy().into_owned()).collect();
    if parts[0] == "_mem_wal" {
        let tail = parts.iter().skip(1).find(|p| p.contains("gen_") || *p == "wal" || *p == "manifest");
        return format!("_mem_wal/{}", match tail {
            Some(t) if t.contains("gen_") => "generation (SSTable)",
            Some(t) => t.as_str(),
            None => "other",
        });
    }
    parts[0].clone()
}

fn find(hay: &[u8], needle: &[u8]) -> usize {
    hay.windows(needle.len()).filter(|w| *w == needle).count()
}

fn scan(dir: &Path, label: &str) {
    let marker = b"SECRETMARKER";
    let vec: Vec<u8> = [MAGIC; 4].iter().flat_map(|f| f.to_le_bytes()).collect();
    let mut acc: BTreeMap<String, (usize, usize, usize, u64)> = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let bytes = std::fs::read(&p).unwrap();
            let c = category(p.strip_prefix(dir).unwrap());
            let a = acc.entry(c).or_default();
            a.0 += 1;
            a.1 += find(&bytes, marker);
            a.2 += find(&bytes, &vec);
            a.3 += bytes.len() as u64;
        }
    }
    println!("-- {label}");
    println!("   {:<30} {:>6} {:>10} {:>14}", "files under", "files", "marker", "vector runs");
    for (c, (f, m, v, _)) in &acc {
        let flag = if *m > 0 || *v > 0 { "  <- residue" } else { "" };
        println!("   {c:<30} {f:>6} {m:>10} {v:>14}{flag}");
    }
}

#[tokio::main]
async fn main() {
    let dir = std::env::args().nth(1).expect("usage: forget <local-dir>");
    let _ = std::fs::remove_dir_all(&dir);
    let root = Path::new(&dir).to_path_buf();

    // Base table: 2,000 keep rows + 500 forget-me rows, with scalar, FTS and vector indexes.
    let base = vec![Ok(rows("k", 2000, false, 5)), Ok(rows("f", 500, true, 0))];
    let mut ds = Dataset::write(RecordBatchIterator::new(base, schema()), &dir, Some(WriteParams::default()))
        .await
        .unwrap();
    for (name, col) in [("id_idx", "id"), ("scope_idx", "scope")] {
        ds.create_index(&[col], IndexType::BTree, Some(name.into()), &ScalarIndexParams::default(), true)
            .await
            .unwrap();
    }
    ds.create_index(&["text"], IndexType::Inverted, Some("text_idx".into()), &InvertedIndexParams::default(), true)
        .await
        .unwrap();
    ds.create_index(
        &["embedding"],
        IndexType::Vector,
        Some("vec_idx".into()),
        &VectorIndexParams::ivf_flat(4, MetricType::L2),
        true,
    )
    .await
    .unwrap();

    // MemWAL: 300 forget-me rows flushed to a generation, 100 more left in the WAL only.
    ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx"]).execute().await.unwrap();
    let shard = Uuid::new_v4();
    let cfg = || ShardWriterConfig::new(shard).with_durable_write(true).with_stats_log_interval(None);
    let w = ds.mem_wal_writer(shard, cfg()).await.unwrap();
    w.put(vec![rows("mf", 300, true, 0), rows("mk", 300, false, 9)]).await.unwrap();
    w.force_seal_active().await.unwrap();
    w.wait_for_flush_drain().await.unwrap();
    w.put(vec![rows("mw", 100, true, 0)]).await.unwrap();
    w.abort().await.unwrap(); // crash-like: last 100 rows only in the WAL
    scan(&root, "0. after writes");

    // 1. Logical forget: delete in the base table, tombstones through MemWAL.
    let mut ds = Dataset::open(&dir).await.unwrap();
    ds.delete("scope = 'forget-me'").await.unwrap();
    let w = ds.mem_wal_writer(shard, cfg()).await.unwrap();
    let keys: Vec<String> = (0..300).map(|i| format!("mf{i:06}")).chain((0..100).map(|i| format!("mw{i:06}"))).collect();
    let key_schema = Arc::new(Schema::new(vec![schema().field(0).clone()]));
    let kb = RecordBatch::try_new(key_schema, vec![Arc::new(StringArray::from(keys))]).unwrap();
    w.delete(vec![kb]).await.unwrap();
    w.close().await.unwrap();
    scan(&root, "1. after DELETE (base) + tombstones (MemWAL) + writer close");

    // 2. Compaction + index rebuild on the base table.
    let mut ds = Dataset::open(&dir).await.unwrap();
    compact_files(&mut ds, Default::default(), None).await.unwrap();
    ds.optimize_indices(&lance_index::optimize::OptimizeOptions::default()).await.unwrap();
    scan(&root, "2. after compact_files + optimize_indices");

    // 3. Old-version cleanup with zero retention.
    let ds = Dataset::open(&dir).await.unwrap();
    let st = ds.cleanup_old_versions(chrono::TimeDelta::zero(), Some(true), None).await.unwrap();
    println!("   cleanup_old_versions: {st:?}");
    scan(&root, "3. after cleanup_old_versions(0)");

    // 4. Full index rebuild from scratch (replace = true).
    let mut ds = Dataset::open(&dir).await.unwrap();
    ds.create_index(&["text"], IndexType::Inverted, Some("text_idx".into()), &InvertedIndexParams::default(), true)
        .await
        .unwrap();
    ds.create_index(
        &["embedding"],
        IndexType::Vector,
        Some("vec_idx".into()),
        &VectorIndexParams::ivf_flat(4, MetricType::L2),
        true,
    )
    .await
    .unwrap();
    let ds = Dataset::open(&dir).await.unwrap();
    ds.cleanup_old_versions(chrono::TimeDelta::zero(), Some(true), None).await.unwrap();
    scan(&root, "4. after rebuilding FTS + vector indexes + cleanup again");
}

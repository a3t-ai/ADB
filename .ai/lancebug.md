# MemWAL: generations sealed during WAL replay are committed without `_pk_index`, breaking all LSM reads of the shard

**Lance version:** 12.0.0 (crates.io) · Rust 1.97.1 · Linux x86_64
**Reproduced on:** local filesystem, and S3-compatible object storage (Hetzner, Ceph RGW)

## Summary

When a `ShardWriter` opens a shard and its WAL replay reaches a memtable seal
threshold, the memtables sealed **during replay** are flushed to generations that have
data, secondary indexes and a bloom filter, but **no `_pk_index` sidecar**. The
generations are still committed to the shard manifest. After that, every
`LsmScanner` read of the shard fails with:

```
Not found: <base>/_mem_wal/<shard>/<hash>_gen_1/_pk_index/page_lookup.lance
```

The shard stays unreadable through the LSM path until the bad generations are
removed by hand.

## When it happens

Whenever the WAL tail a writer replays on open is larger than one memtable's seal
limits (`max_memtable_rows`, `max_memtable_size`, `max_memtable_batches`, or the
`max_unflushed_memtable_bytes` resident limit). In practice:

- the previous writer crashed or was fenced with more unflushed data than one
  memtable holds. This happens even with identical configs when its generation
  flushes were lagging, because frozen memtables awaiting flush are replayed too;
- writers with different memtable limits take over the same shard.

We hit it in failover tests (random SIGKILL of the writer process, and takeovers while
the old writer was still writing) with identical configs on both sides.

## Minimal reproduction

`Cargo.toml` dependencies: `lance = "=12.0.0"`, `lance-index = "=12.0.0"`,
`arrow-array = "58"`, `arrow-schema = "58"`, `tokio = { version = "1", features = ["full"] }`,
`uuid = { version = "1", features = ["v4"] }`.

```rust
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
    let uri = std::env::args().nth(1).expect("usage: repro <empty-local-dir>");

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
```

**Actual output:**

```
generation 1 (3bf88e0b_gen_1): _pk_index present = false
generation 2 (8b81b5ac_gen_2): _pk_index present = false
LSM scan FAILED: Not found: …/_mem_wal/dcd84a94-…/3bf88e0b_gen_1/_pk_index/page_lookup.lance
```

**Expected:** every committed generation has its `_pk_index` sidecar, and the LSM scan
returns all 1,020 rows.

**Controls:**
- the same scenario where replay fits in one memtable (5 batches instead of 50)
  works;
- generations sealed by a live writer after open work;
- a replayed memtable sealed *after* open (`force_seal_active()`) also works, because
  the post-replay index rebuild covers it.

## Cause (as far as we can tell)

In `src/dataset/mem_wal/write.rs`, `replay_memtable_from_wal`:

1. Replayed batches go into the active memtable with
   `active.insert_batches_only(batches)` (line 1479), which skips the in-memory
   indexes, the PK index included.
2. When the next entry would cross a seal threshold, the memtable is sealed and
   flushed with `flush_replayed_memtable(...)` (line 1466 → `flusher.flush` /
   `flush_with_indexes`).
3. Both flush paths write the PK sidecar through `MemTableFlusher::create_pk_index`
   (`memtable/flush.rs`, line 704). That function builds it from the memtable's
   **in-memory** PK index (`registry.pk_training_batches(..)`), which is empty for a
   replay-sealed memtable, so it returns early (lines 716–722) and writes nothing.
   The manifest commit then proceeds without a sidecar.
4. Only the final active memtable gets its indexes rebuilt after replay (comment at
   line 1491: "Sealed memtables needed no in-memory index build: they were flushed
   straight to disk"). The sealed ones did need the PK index, for the sidecar.

## Possible fixes

- Build the PK sidecar for replay-sealed memtables from their batches, e.g. update
  the in-memory PK index during replay, or rebuild it just before
  `flush_replayed_memtable`.
- Or have `create_pk_index` fall back to the memtable's stored batches when the
  in-memory PK index is empty but the memtable has rows and a primary key.
- Defensive: refuse to commit a generation whose memtable has rows and a primary key
  but produced no sidecar, so the error surfaces at flush time instead of on every
  later read.

## Workaround we use

Set `max_memtable_rows`, `max_memtable_size`, `max_memtable_batches` and
`max_unflushed_memtable_bytes` far above anything a replay can reach, and seal from
the application with `ShardWriter::force_seal_active()` at our own limits. Replay then
never seals mid-replay, the post-replay rebuild covers the whole replayed memtable,
and the next seal writes a complete sidecar.

## A related concern (not reproduced)

On the live path the sidecar is also built from the in-memory PK index, which the
asynchronous index applier maintains. If a seal ever ran before the applier caught
up, a *partial* sidecar would be worse than a missing one: rows absent from it could
lose newest-per-PK dedup and let stale versions surface. We did not observe this in
our tests; mentioning it in case the fix touches the same code.

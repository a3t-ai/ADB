//! One partition's memory table: a Lance dataset written through its MemWAL shard
//! (option A, ARCHITECTURE.md §9.2–9.3; spikes B2, R11).
//!
//! - **Apply:** each log record becomes ONE MemWAL `put` holding its rows plus the
//!   `_meta` row (`applied_offset` = the record's offset), so a crash leaves either
//!   all of a record or none of it, and `_meta` says where replay resumes.
//! - **Idempotent:** a record at or below `applied_offset` is skipped, so replaying the
//!   log from anywhere below is safe.
//! - **Reads:** `LsmScanner` over memtable + flushed generations + base table, newest
//!   row per `id`.
//! - **Seals:** ADB-driven (Lance's automatic limits set out of reach, §9.2 workaround
//!   for the replay bug): the active memtable is sealed when it reaches `memtable_rows`.
//! - **Durability:** puts are not durable on their own (`durable_write(false)`): the ADB
//!   log is the WAL. The MemWAL WAL flushes every `wal_flush`; until then a crash is
//!   recovered by replaying the log from `applied_offset + 1`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Kind, LogOffset, LogRecord, NewRow, PartitionId, RecordId};
use arrow_array::RecordBatchIterator;
use arrow_schema::SchemaRef;
use datafusion::logical_expr::Expr;
use lance::dataset::mem_wal::scanner::{DatasetCache, LsmScanner, ShardSnapshot, SsTableCache};
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter, ShardWriterConfig};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance_index::scalar::ScalarIndexParams;
use lance_index::IndexType;
use uuid::Uuid;

use crate::hot::HotCache;
use crate::rows::{decode, encode, qualified, row_id, StoredRow};
use crate::schema::{memory_schema, META_ID, RECORD_KINDS};
use crate::{StoreError, StoreResult};

/// The embedding dimension of ADB's model (bge-small-en-v1.5, MVP W4). One per
/// deployment (D3 S5); a table of another dimension is rewritten at open.
pub const DEFAULT_EMBEDDING_DIM: i32 = 384;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Directory (or object-store URI) holding `p{partition}/memory.lance`.
    pub root: PathBuf,
    /// Platform embedding dimension (D3 S5).
    pub embedding_dim: i32,
    /// ADB seals the active memtable at this many rows.
    pub memtable_rows: usize,
    /// MemWAL WAL flush interval.
    pub wal_flush: Duration,
    /// The embedding model's name, kept in `_meta` (W9.10): a table whose vectors
    /// came from another model has them cleared at open (the backfill re-embeds).
    /// `None`: not checked.
    pub embedding_model: Option<String>,
}

/// The model of tables whose `_meta` names none (written before W9.10).
pub const DEFAULT_EMBEDDING_MODEL: &str = "bge-small-en-v1.5";

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into(), embedding_dim: DEFAULT_EMBEDDING_DIM, memtable_rows: 10_000, wal_flush: Duration::from_secs(1), embedding_model: None }
    }
}

fn lance(e: impl std::fmt::Display) -> StoreError {
    StoreError::Lance(e.to_string())
}

/// Quote a string for a Lance SQL filter.
pub fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The MemWAL shard of a partition: stable across reopen, so a new owner replays it.
fn shard_id(p: PartitionId) -> Uuid {
    Uuid::from_u128(0xadb0_0000_0000_4000_8000_0000_0000_0000 | u128::from(p.0))
}

pub struct PartitionStore {
    /// The store's root directory (`sync_to_disk`).
    root: std::path::PathBuf,
    partition: PartitionId,
    cfg: StoreConfig,
    /// The table and its writer. Reads and applies share it; W5 compaction takes it
    /// exclusively to swap in the rewritten table (`None` only during that swap).
    state: tokio::sync::RwLock<Option<Inner>>,
    schema: SchemaRef,
    dim: i32,
    memtable_rows: usize,
    /// `_meta`'s applied offset, read once on open and advanced by `apply`, so
    /// applying a record doesn't scan the table just to check it.
    applied: std::sync::atomic::AtomicU64,
    /// Records applied since the last compaction, and the rows that compaction kept
    /// (W7 compaction trigger: many small puts slow every scan).
    puts_since_compaction: std::sync::atomic::AtomicU64,
    rows_at_compaction: std::sync::atomic::AtomicU64,
    /// The table's vectors came from another embedding model (W9.10).
    model_changed: std::sync::atomic::AtomicBool,
    /// W10 hot cache, fed by every put (`set_cache`).
    hot: std::sync::RwLock<Option<Arc<HotCache>>>,
    /// Odd while a put and its cache update are under way (`apply_seq`).
    apply_seq: std::sync::atomic::AtomicU64,
}

/// The open table: base dataset, MemWAL writer, SSTable cache, and the put ledger.
struct Inner {
    base: Arc<Dataset>,
    writer: ShardWriter,
    cache: Arc<dyn DatasetCache>,
    /// Log offset of each put since open, to map the MemWAL's durable batch count
    /// back to a log offset (`durable_offset`).
    puts: std::sync::Mutex<PutLedger>,
}

fn open_state(g: &Option<Inner>) -> &Inner {
    g.as_ref().expect("the partition store is open")
}

/// Puts since open, numbered like the MemWAL writer numbers batches (R11 / R12).
#[derive(Debug, Default)]
struct PutLedger {
    /// Writer-global batch number of the first put after open.
    base: usize,
    /// Puts dropped from the front of `offsets` (already known durable).
    first: usize,
    /// Log offset of put `base + first + i`.
    offsets: std::collections::VecDeque<u64>,
    /// Applied offset recovered on open (durable by definition).
    durable_at_open: u64,
}

/// Lance runtime settings for ADB's workload, applied once before Lance is first used.
///
/// Lance's default execution session spills sorts to disk, reserving
/// `min(pool / 3, 40 MB)` up front per sort from a process-wide pool
/// (`LANCE_MEM_POOL_SIZE`, 150 MB): only ~3 sorts can run at once, however small, and
/// concurrent LSM reads fail with "Resources exhausted" (risk K10). ADB's sorts are
/// per-partition and small, so spilling buys nothing: bypass it. `ADB_LANCE_SPILL=1`
/// (or an explicit `LANCE_BYPASS_SPILLING`) keeps Lance's behaviour.
pub fn configure_lance_runtime() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("LANCE_BYPASS_SPILLING").is_none() && std::env::var_os("ADB_LANCE_SPILL").is_none() {
            std::env::set_var("LANCE_BYPASS_SPILLING", "1");
        }
    });
}

impl PartitionStore {
    /// Open (creating if needed) the partition's table and become its writer.
    pub async fn open(cfg: &StoreConfig, partition: PartitionId) -> StoreResult<Self> {
        configure_lance_runtime();
        let dir = cfg.root.join(format!("p{}", partition.0)).join("memory.lance");
        let staged = dir.with_file_name("memory.rewrite.lance");
        // A rewrite (below) that stopped between its two renames: finish it.
        if !dir.exists() && staged.exists() {
            std::fs::rename(&staged, &dir).map_err(|e| StoreError::Decode(format!("finishing the table rewrite: {e}")))?;
        }
        // A compaction that stopped after moving the old table aside: drop it.
        let trash = dir.with_file_name("memory.compacted.trash");
        if dir.exists() && trash.exists() {
            std::fs::remove_dir_all(&trash).map_err(|e| StoreError::Decode(format!("dropping the compacted table: {e}")))?;
        }
        if dir.exists() {
            let ds = Dataset::open(&dir.to_string_lossy()).await.map_err(lance)?;
            if let Some(found) = embedding_dim_of(&ds) {
                if found != cfg.embedding_dim {
                    drop(ds);
                    Box::pin(Self::rewrite_dim(cfg, partition, found)).await?;
                }
            }
        }
        let store = Self::open_at(cfg, partition, &dir).await?;
        // W9.10: vectors from another model can't be compared with this one's. They
        // are cleared after the log's tail is applied (`clear_vectors`), or the
        // replay would bring back the old model's vectors.
        if let Some(want) = &cfg.embedding_model {
            let meta = store.get(META_ID).await?;
            let had = meta.as_ref().and_then(|m| m.data.value.get("embedding_model")).and_then(|v| v.as_str()).unwrap_or(DEFAULT_EMBEDDING_MODEL);
            if had != want {
                tracing::info!(from = %had, to = %want, "embedding model changed: vectors are cleared once the log is applied");
                store.model_changed.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        Ok(store)
    }

    /// The table's embedding column has `from` dimensions and the model `cfg`'s:
    /// rewrite every row (history, tombstones and `_meta` too) into a table of
    /// `cfg.embedding_dim`; the old table stays beside it as `memory.dim<from>.bak`.
    /// Vectors of the old size are dropped (the backfill embeds the text again).
    async fn rewrite_dim(cfg: &StoreConfig, partition: PartitionId, from: i32) -> StoreResult<()> {
        let stats = Self::rewrite_table(cfg, partition, from, &Some, Some(format!("memory.dim{from}.bak")), true).await?;
        tracing::info!(rows = stats.kept, from, to = cfg.embedding_dim, "memory table rewritten for the embedding model's dimension");
        Ok(())
    }

    /// W5 compaction, offline (the table must not be open): rewrite the table
    /// keeping only what may stay — no tombstoned rows (forgotten, expired by the
    /// timer, unlinked, unsubscribed), no rows past their TTL, no versions
    /// superseded more than `history` ago — and delete the old table, so forgotten
    /// data is physically gone (L10). The MemWAL generations go with it.
    pub async fn compact_offline(cfg: &StoreConfig, partition: PartitionId, history: Duration, now: CommitTs) -> StoreResult<CompactStats> {
        let keep = compaction_row(history, now);
        Self::rewrite_table(cfg, partition, cfg.embedding_dim, &keep, None, false).await
    }

    /// W5 compaction on the open table: the same rewrite as [`Self::compact_offline`],
    /// swapped in under the exclusive lock (reads wait for the whole rewrite: the
    /// lock is held throughout).
    /// The caller pauses writes (the sequencer) and makes sure every committed record
    /// is applied first. The old table, its versions and MemWAL files, and any
    /// rewrite backups (`memory.dim*.bak`) are deleted: forgotten data is gone.
    pub async fn compact(&self, history: Duration, now: CommitTs) -> StoreResult<CompactStats> {
        let keep = compaction_row(history, now);
        let stats = self.rewrite_online(&keep, false).await?;
        self.puts_since_compaction.store(0, std::sync::atomic::Ordering::SeqCst);
        self.rows_at_compaction.store(stats.kept as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(stats)
    }

    /// Whether the vectors came from another embedding model than the configured one
    /// (W9.10): the caller applies the log's tail, then [`Self::clear_vectors`].
    pub fn model_changed(&self) -> bool {
        self.model_changed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Drop every vector and record the configured model in `_meta` (W9.10), as an
    /// online rewrite (the caller makes sure nothing is being applied). The
    /// backfill embeds the records again.
    pub async fn clear_vectors(&self) -> StoreResult<CompactStats> {
        let model = self.cfg.embedding_model.clone();
        let keep = move |mut r: StoredRow| {
            r.data.embedding = None;
            if r.id == META_ID {
                if let (Some(m), serde_json::Value::Object(v)) = (&model, &mut r.data.value) {
                    v.insert("embedding_model".into(), m.clone().into());
                }
            }
            Some(r)
        };
        let stats = self.rewrite_online(&keep, true).await?;
        self.model_changed.store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(stats)
    }

    /// Rewrite the open table keeping what `keep` returns, swapped in under the
    /// exclusive lock (compaction, W5; clearing vectors, W9.10 — `vectors_cleared`:
    /// the hot cache drops its vectors in the same swap).
    async fn rewrite_online(&self, keep: &(dyn Fn(StoredRow) -> Option<StoredRow> + Sync), vectors_cleared: bool) -> StoreResult<CompactStats> {
        let started = std::time::Instant::now();
        let dir = self.root.join(format!("p{}", self.partition.0)).join("memory.lance");
        let staged = dir.with_file_name("memory.rewrite.lance");
        let trash = dir.with_file_name("memory.compacted.trash");
        let err = |e: std::io::Error| StoreError::Decode(format!("compaction: {e}"));
        let mut g = self.state.write().await;
        let bytes_before = dir_size(&dir);
        let all = Self::scan_in(open_state(&g), None).await?;
        let total = all.len();
        let rows: Vec<StoredRow> = all.into_iter().filter_map(keep).collect();
        // The new table, complete and closed, beside the old one.
        if staged.exists() {
            std::fs::remove_dir_all(&staged).map_err(err)?;
        }
        Self::create(&staged.to_string_lossy(), &self.schema, self.dim).await?;
        let new = Self::open_inner(&self.cfg, self.partition, &staged).await?;
        for chunk in rows.chunks(10_000) {
            new.writer.put(vec![encode(chunk, &self.schema, self.dim)?]).await.map_err(lance)?;
        }
        new.writer.close().await.map_err(lance)?;
        // On disk before anything old goes (L11, W9): Lance doesn't fsync locally.
        adb_core::disk::sync_fs_async(&staged).await.map_err(err)?;
        // Close the current writer before the directories move: its WAL path is the
        // table's. From here the store is closed; any failure reopens what is there.
        let old = g.take().expect("the partition store is open");
        let swapped = async {
            old.writer.close().await.map_err(lance)?;
            if trash.exists() {
                std::fs::remove_dir_all(&trash).map_err(err)?;
            }
            std::fs::rename(&dir, &trash).map_err(err)?;
            std::fs::rename(&staged, &dir).map_err(err)?;
            StoreResult::Ok(())
        }
        .await;
        if !dir.exists() && staged.exists() {
            let _ = std::fs::rename(&staged, &dir);
        }
        let inner = Self::open_inner(&self.cfg, self.partition, &dir).await?;
        Self::reset_ledger(&inner, self.applied.load(std::sync::atomic::Ordering::SeqCst)).await?;
        *g = Some(inner);
        if let (true, Some(h)) = (vectors_cleared, self.hot()) {
            h.drop_vectors();
        }
        drop(g);
        swapped?;
        // The renames are durable before the old table is deleted.
        adb_core::disk::sync_fs_async(&dir).await.map_err(err)?;
        std::fs::remove_dir_all(&trash).map_err(err)?;
        for e in std::fs::read_dir(dir.parent().unwrap_or(&dir)).map_err(err)?.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("memory.dim") && name.ends_with(".bak") {
                std::fs::remove_dir_all(e.path()).map_err(err)?;
            }
        }
        Ok(CompactStats { total, kept: rows.len(), bytes_before, bytes_after: dir_size(&dir), took: started.elapsed() })
    }

    /// What `_meta` records about the table besides offsets: its embedding size and
    /// (W9.10) the model its vectors came from.
    fn meta_extra(&self) -> serde_json::Value {
        let mut v = serde_json::json!({ "embedding_dim": self.dim });
        if let Some(m) = &self.cfg.embedding_model {
            v["embedding_model"] = m.clone().into();
        }
        v
    }

    /// Whether enough small writes have piled up to compact (W7): more records
    /// applied since the last compaction than max(`min`, a quarter of the rows it
    /// kept), so a compaction's pause stays proportional to what it saves. Counts
    /// from open when this process hasn't compacted yet.
    pub fn writes_due(&self, min: u64) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        self.puts_since_compaction.load(SeqCst) > min.max(self.rows_at_compaction.load(SeqCst) / 4)
    }

    /// MemWAL generations (sealed memtables) not yet compacted away: a W5 threshold.
    pub async fn generation_count(&self) -> StoreResult<usize> {
        let g = self.state.read().await;
        Ok(open_state(&g).writer.manifest().await.map_err(lance)?.map(|m| m.sstables.len()).unwrap_or(0))
    }

    /// Put `rows` into the table (chunks of 10k), outside the log: rewrites only.
    async fn put_rows(&self, rows: &[StoredRow]) -> StoreResult<()> {
        let g = self.state.read().await;
        for chunk in rows.chunks(10_000) {
            open_state(&g).writer.put(vec![encode(chunk, &self.schema, self.dim)?]).await.map_err(lance)?;
        }
        Ok(())
    }

    /// Rewrite the partition's table (read at `from` dimensions) into a fresh one of
    /// `cfg.embedding_dim` with the rows `keep` accepts, then swap it in. With
    /// `backup`, the old table is renamed to it; without, it is deleted (and the
    /// disk synced). Crash-safe: the new table is complete and closed before the
    /// renames, and `open` finishes an interrupted swap.
    async fn rewrite_table(
        cfg: &StoreConfig,
        partition: PartitionId,
        from: i32,
        keep: &(dyn Fn(StoredRow) -> Option<StoredRow> + Sync),
        backup: Option<String>,
        clear_vectors: bool,
    ) -> StoreResult<CompactStats> {
        let started = std::time::Instant::now();
        let dir = cfg.root.join(format!("p{}", partition.0)).join("memory.lance");
        let staged = dir.with_file_name("memory.rewrite.lance");
        let err = |e: std::io::Error| StoreError::Decode(format!("table rewrite: {e}"));
        let bytes_before = dir_size(&dir);
        let old = Self::open_at(&StoreConfig { embedding_dim: from, ..cfg.clone() }, partition, &dir).await?;
        let all = old.scan(None).await?;
        old.close().await?;
        let total = all.len();
        let mut rows: Vec<StoredRow> = all.into_iter().filter_map(keep).collect();
        for r in rows.iter_mut() {
            if from != cfg.embedding_dim || clear_vectors {
                r.data.embedding = None;
            }
            if r.id == META_ID {
                if let serde_json::Value::Object(v) = &mut r.data.value {
                    v.insert("embedding_dim".into(), cfg.embedding_dim.into());
                    if let Some(m) = &cfg.embedding_model {
                        v.insert("embedding_model".into(), m.clone().into());
                    }
                }
            }
        }
        if staged.exists() {
            std::fs::remove_dir_all(&staged).map_err(err)?;
        }
        Self::create(&staged.to_string_lossy(), &memory_schema(cfg.embedding_dim), cfg.embedding_dim).await?;
        let new = Self::open_at(cfg, partition, &staged).await?;
        new.put_rows(&rows).await?;
        new.close().await?;
        // On disk before anything old moves (L11, W9).
        adb_core::disk::sync_fs_async(&staged).await.map_err(err)?;
        match &backup {
            Some(name) => {
                let b = dir.with_file_name(name);
                if b.exists() {
                    std::fs::remove_dir_all(&b).map_err(err)?;
                }
                std::fs::rename(&dir, &b).map_err(err)?;
                std::fs::rename(&staged, &dir).map_err(err)?;
            }
            None => {
                let trash = dir.with_file_name("memory.compacted.trash");
                if trash.exists() {
                    std::fs::remove_dir_all(&trash).map_err(err)?;
                }
                std::fs::rename(&dir, &trash).map_err(err)?;
                std::fs::rename(&staged, &dir).map_err(err)?;
                adb_core::disk::sync_fs_async(&dir).await.map_err(err)?;
                std::fs::remove_dir_all(&trash).map_err(err)?;
            }
        }
        Ok(CompactStats { total, kept: rows.len(), bytes_before, bytes_after: dir_size(&dir), took: started.elapsed() })
    }

    async fn open_at(cfg: &StoreConfig, partition: PartitionId, dir: &std::path::Path) -> StoreResult<Self> {
        let inner = Self::open_inner(cfg, partition, dir).await?;
        let store = Self {
            root: cfg.root.clone(),
            partition,
            cfg: cfg.clone(),
            state: tokio::sync::RwLock::new(Some(inner)),
            schema: memory_schema(cfg.embedding_dim),
            dim: cfg.embedding_dim,
            memtable_rows: cfg.memtable_rows,
            applied: std::sync::atomic::AtomicU64::new(0),
            puts_since_compaction: std::sync::atomic::AtomicU64::new(0),
            rows_at_compaction: std::sync::atomic::AtomicU64::new(0),
            model_changed: std::sync::atomic::AtomicBool::new(false),
            hot: std::sync::RwLock::new(None),
            apply_seq: std::sync::atomic::AtomicU64::new(0),
        };
        let meta = store.get(META_ID).await?.ok_or_else(|| StoreError::Decode("no _meta row".into()))?;
        let applied = meta.data.value.get("applied_offset").and_then(|v| v.as_u64()).unwrap_or(0);
        store.applied.store(applied, std::sync::atomic::Ordering::SeqCst);
        if meta.data.value.get("schema_version").and_then(|v| v.as_u64()).unwrap_or(0) < 3 {
            store.qualify_row_ids(&meta).await?;
        }
        {
            let g = store.state.read().await;
            Self::reset_ledger(open_state(&g), applied).await?;
        }
        Ok(store)
    }

    /// Everything present after the writer's replay is WAL-durable: it is the base
    /// of the batch numbering, and `applied` is durable already.
    async fn reset_ledger(inner: &Inner, applied: u64) -> StoreResult<()> {
        let st = inner.writer.memtable_stats().await.map_err(lance)?;
        *inner.puts.lock().unwrap() = PutLedger { base: st.global_offset + st.batch_count, first: 0, offsets: Default::default(), durable_at_open: applied };
        Ok(())
    }

    /// Open the table in `dir` (creating it if needed) and become its writer.
    async fn open_inner(cfg: &StoreConfig, partition: PartitionId, dir: &std::path::Path) -> StoreResult<Inner> {
        let schema = memory_schema(cfg.embedding_dim);
        let uri = dir.to_string_lossy().to_string();
        if !dir.exists() {
            Self::create(&uri, &schema, cfg.embedding_dim).await?;
        }
        let base = Arc::new(Dataset::open(&uri).await.map_err(lance)?);
        // Schema v1 kept `value` as JSON text; v2 needs JSONB (D3.6). No in-place
        // migration yet: rebuild the table from the log.
        if base.schema().field("value").map(|f| !matches!(f.data_type(), arrow_schema::DataType::LargeBinary)).unwrap_or(false) {
            return Err(StoreError::Decode(format!(
                "{uri} is a schema-v1 table (value as JSON text); this ADB needs v2 (JSONB). \
                 Move the store directory aside and restart: it is rebuilt from the log, if the log is complete"
            )));
        }
        let shard = shard_id(partition);
        let config = ShardWriterConfig::new(shard)
            .with_durable_write(false)
            .with_max_wal_flush_interval(cfg.wal_flush)
            .with_stats_log_interval(None)
            // ADB seals (see the module docs): Lance's own limits out of reach, but finite —
            // Lance sizes buffers from them (usize::MAX overflows), values from spike R11.
            .with_max_memtable_rows(10_000_000)
            .with_max_memtable_size(4 << 30)
            .with_max_memtable_batches(1_000_000)
            .with_max_unflushed_memtable_bytes(8 << 30);
        let writer = base.mem_wal_writer(shard, config).await.map_err(lance)?;
        let _ = schema;
        Ok(Inner { base, writer, cache: Arc::new(SsTableCache::new(64)), puts: std::sync::Mutex::new(PutLedger::default()) })
    }

    /// Schema v2 → v3: rewrite every record and edge row under its memory-qualified id
    /// (`rows::row_id`) and tombstone the unqualified one, in one put with the v3
    /// `_meta`. Records that shared a key across memory types were already one row in
    /// v2; the surviving version keeps the memory type it was last written as. A crash
    /// before the put is durable leaves `_meta` at v2, so the next open redoes it.
    async fn qualify_row_ids(&self, meta: &StoredRow) -> StoreResult<()> {
        let mut out = Vec::new();
        for r in self.scan(None).await? {
            if !matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool | Kind::Edge) {
                continue;
            }
            let mut q = r.clone();
            let m = r.data.memory;
            q.id = row_id(m, &r.id);
            q.lineage_id = row_id(m, &r.lineage_id);
            q.superseded_by = r.superseded_by.as_deref().map(|s| row_id(m, s));
            if r.data.kind == Kind::Edge {
                let n = qualified(&NewRow {
                    id: RecordId(r.id.clone()),
                    lineage_id: RecordId(r.lineage_id.clone()),
                    version: r.version,
                    data: r.data.clone(),
                });
                q.data.subject_id = n.data.subject_id;
                q.data.object_id = n.data.object_id;
            }
            let mut old = r;
            old.tombstone = true;
            out.push(old);
            out.push(q);
        }
        let applied = self.applied.load(std::sync::atomic::Ordering::SeqCst);
        out.push(StoredRow::meta(applied, meta.commit_ts, meta.epoch, self.meta_extra()));
        let batch = encode(&out, &self.schema, self.dim)?;
        let g = self.state.read().await;
        open_state(&g).writer.put(vec![batch]).await.map_err(lance)?;
        Ok(())
    }

    /// Base table with the `_meta` row, a BTree on `id`, and a MemWAL maintaining it.
    async fn create(uri: &str, schema: &SchemaRef, dim: i32) -> StoreResult<()> {
        let meta = StoredRow::meta(0, CommitTs(0), 0, serde_json::json!({ "embedding_dim": dim }));
        let batch = encode(&[meta], schema, dim)?;
        let mut ds = Dataset::write(RecordBatchIterator::new([Ok(batch)], schema.clone()), uri, Some(WriteParams::default()))
            .await
            .map_err(lance)?;
        ds.create_index(&["id"], IndexType::BTree, Some("id_idx".into()), &ScalarIndexParams::default(), true)
            .await
            .map_err(lance)?;
        ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx"]).execute().await.map_err(lance)?;
        Ok(())
    }

    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Every row version matching `filter` (Lance SQL), newest per id, `_meta` included.
    pub async fn scan(&self, filter: Option<&str>) -> StoreResult<Vec<StoredRow>> {
        let g = self.state.read().await;
        Self::scan_in(open_state(&g), filter).await
    }

    async fn scan_in(inner: &Inner, filter: Option<&str>) -> StoreResult<Vec<StoredRow>> {
        let mut scanner = Self::scanner(inner).await?;
        if let Some(f) = filter {
            scanner = scanner.filter(f).map_err(lance)?;
        }
        let batch = scanner.try_into_batch().await.map_err(lance)?;
        decode(&batch)
    }

    /// As [`PartitionStore::scan`] with a DataFusion expression, for filters the SQL
    /// parser can't express: Lance's json_* functions on `value` (stage D3.6).
    /// Filtering happens after the LSM picks the newest version of each row.
    pub async fn scan_expr(&self, filter: Option<Expr>) -> StoreResult<Vec<StoredRow>> {
        let g = self.state.read().await;
        let mut scanner = Self::scanner(open_state(&g)).await?;
        if let Some(f) = filter {
            scanner = scanner.filter_expr(f);
        }
        let batch = scanner.try_into_batch().await.map_err(lance)?;
        decode(&batch)
    }

    /// The current view at `now` of the rows matching `filter` (an expression).
    pub async fn current_expr(&self, filter: Option<Expr>, now: CommitTs) -> StoreResult<Vec<StoredRow>> {
        Ok(self
            .scan_expr(filter)
            .await?
            .into_iter()
            .filter(|r| r.id != META_ID && r.data.kind != Kind::Meta && r.is_current(now))
            .collect())
    }

    /// Run a DataFusion query over one snapshot of the table (MVP W7): `f` gets a
    /// session with the table registered as `memory` and ADB's UDFs, and runs its
    /// plan to the end while the store's state is held — a compaction waits for it.
    pub async fn query<T, F, Fut>(&self, f: F) -> StoreResult<T>
    where
        F: FnOnce(datafusion::prelude::SessionContext) -> Fut,
        Fut: std::future::Future<Output = datafusion::error::Result<T>>,
    {
        self.query_in(datafusion::prelude::SessionConfig::new(), f).await
    }

    /// [`Self::query`] in a session built from `config` (the SQL console turns on
    /// `information_schema`); also registers `adb_json(value)`.
    pub async fn query_in<T, F, Fut>(&self, config: datafusion::prelude::SessionConfig, f: F) -> StoreResult<T>
    where
        F: FnOnce(datafusion::prelude::SessionContext) -> Fut,
        Fut: std::future::Future<Output = datafusion::error::Result<T>>,
    {
        let g = self.state.read().await;
        let inner = open_state(&g);
        let table = Self::snapshot(inner).await?;
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);
        ctx.register_udf(crate::filter::json_scalar_udf());
        ctx.register_udf(crate::filter::json_text_udf());
        ctx.register_table("memory", Arc::new(table)).map_err(lance)?;
        f(ctx).await.map_err(lance)
    }

    async fn snapshot(inner: &Inner) -> StoreResult<crate::table::LsmTable> {
        let manifest = inner.writer.manifest().await.map_err(lance)?;
        let mut snapshot = ShardSnapshot::new(inner.writer.shard_id());
        if let Some(m) = &manifest {
            snapshot = snapshot.with_current_generation(m.current_generation);
            for t in &m.sstables {
                snapshot = snapshot.with_sstable(t.generation, t.path.clone());
            }
        }
        let mut table = crate::table::LsmTable {
            base: inner.base.clone(),
            snapshot,
            shard: inner.writer.shard_id(),
            memtables: inner.writer.in_memory_memtable_refs().await.map_err(lance)?,
            cache: inner.cache.clone(),
            schema: Arc::new(arrow_schema::Schema::empty()),
        };
        table.schema = table.scanner().schema();
        Ok(table)
    }

    /// An LSM scanner over memtable + flushed generations + base table.
    async fn scanner(inner: &Inner) -> StoreResult<LsmScanner> {
        let manifest = inner.writer.manifest().await.map_err(lance)?;
        let mut snap = ShardSnapshot::new(inner.writer.shard_id());
        if let Some(m) = &manifest {
            snap = snap.with_current_generation(m.current_generation);
            for t in &m.sstables {
                snap = snap.with_sstable(t.generation, t.path.clone());
            }
        }
        Ok(LsmScanner::new(inner.base.clone(), vec![snap], vec!["id".into()])
            .with_in_memory_memtables(inner.writer.shard_id(), inner.writer.in_memory_memtable_refs().await.map_err(lance)?)
            .with_sstable_cache(inner.cache.clone()))
    }

    /// The current view (D2 §3) at `now`, without `_meta`.
    pub async fn current(&self, filter: Option<&str>, now: CommitTs) -> StoreResult<Vec<StoredRow>> {
        Ok(self
            .scan(filter)
            .await?
            .into_iter()
            .filter(|r| r.id != META_ID && r.data.kind != Kind::Meta && r.is_current(now))
            .collect())
    }

    /// The current edges leaving record `from` of memory type `memory`, of
    /// `link_type` if given, oldest first (D4.1: links are read from the table, so
    /// a forgotten record's edges are gone, live and after a restart alike).
    pub async fn edges_from(
        &self,
        memory: adb_core::log::Memory,
        from: &str,
        link_type: Option<&str>,
        now: CommitTs,
    ) -> StoreResult<Vec<StoredRow>> {
        let mut filter = format!("kind = 'edge' AND subject_id = {}", sql_str(&row_id(memory, from)));
        if let Some(t) = link_type {
            filter.push_str(&format!(" AND link_type = {}", sql_str(t)));
        }
        let mut rows = self.current(Some(&filter), now).await?;
        rows.sort_by(|a, b| (a.commit_ts, &a.id).cmp(&(b.commit_ts, &b.id)));
        Ok(rows)
    }

    /// The current version of record `key` of memory type `memory` (its lineage's
    /// current row, D2 T1), if any.
    pub async fn current_version(&self, memory: adb_core::log::Memory, key: &str, now: CommitTs) -> StoreResult<Option<StoredRow>> {
        let filter = format!("lineage_id = {} AND {RECORD_KINDS}", sql_str(&row_id(memory, key)));
        Ok(self.current(Some(&filter), now).await?.into_iter().max_by_key(|r| r.version))
    }

    /// `memory`'s superseded record versions that are not forgotten: what FORGET
    /// must also match (D2 T5).
    pub async fn superseded(&self, memory: adb_core::log::Memory) -> StoreResult<Vec<StoredRow>> {
        let filter = format!("memory = {} AND {RECORD_KINDS} AND superseded_by IS NOT NULL AND tombstone = false", sql_str(memory.tag()));
        self.scan(Some(&filter)).await
    }

    /// Current versions of records with a TTL whose expiry isn't logged yet (the
    /// TTL timers' seed at open, W9.12).
    pub async fn with_ttl(&self) -> StoreResult<Vec<StoredRow>> {
        let filter = format!("{RECORD_KINDS} AND ttl_at IS NOT NULL AND tombstone = false AND superseded_by IS NULL");
        Ok(self.scan(Some(&filter)).await?.into_iter().filter(|r| r.valid_to.is_none()).collect())
    }

    /// The current version of lineage `lineage` (a row lineage id), expired or not,
    /// unless its expiry or forget is logged already (W9.12).
    pub async fn ttl_version(&self, lineage: &str) -> StoreResult<Option<StoredRow>> {
        let filter = format!("lineage_id = {} AND {RECORD_KINDS} AND tombstone = false AND superseded_by IS NULL", sql_str(lineage));
        Ok(self.scan(Some(&filter)).await?.into_iter().filter(|r| r.valid_to.is_none()).max_by_key(|r| r.version))
    }

    /// How many records of `memory` are current at `now`.
    pub async fn count_current(&self, memory: adb_core::log::Memory, now: CommitTs) -> StoreResult<u64> {
        let filter = format!("memory = {} AND {RECORD_KINDS}", sql_str(memory.tag()));
        Ok(self.current(Some(&filter), now).await?.len() as u64)
    }

    /// The newest version of row `id`, if any.
    pub async fn get(&self, id: &str) -> StoreResult<Option<StoredRow>> {
        Ok(self.scan(Some(&format!("id = {}", sql_str(id)))).await?.into_iter().next())
    }

    /// Counts puts in two steps (before the put, after the cache took its rows):
    /// odd while one is under way. Two reads between equal, even values saw the
    /// same state in the table and the cache (the W10 shadow check).
    pub fn apply_seq(&self) -> u64 {
        self.apply_seq.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The hot cache, if one is set (W10).
    pub fn hot(&self) -> Option<Arc<HotCache>> {
        self.hot.read().unwrap().clone()
    }

    /// Replace the hot cache (`None`: no cache). The new one is filled from the
    /// table's current record rows of the memory types it keeps, with applies
    /// held off (the store's exclusive lock), so it starts at the applied offset.
    pub async fn set_cache(&self, cache: Option<HotCache>) -> StoreResult<()> {
        let g = self.state.write().await;
        let Some(cache) = cache else {
            *self.hot.write().unwrap() = None;
            return Ok(());
        };
        let tracked = cache.tracked();
        let started = std::time::Instant::now();
        if !tracked.is_empty() {
            let memories: Vec<String> = tracked.iter().map(|m| sql_str(m.tag())).collect();
            let filter = format!("memory IN ({}) AND {RECORD_KINDS} AND tombstone = false AND superseded_by IS NULL", memories.join(", "));
            let mut rows = Self::scan_in(open_state(&g), Some(&filter)).await?;
            rows.retain(|r| r.id != META_ID);
            if cache.config().edges {
                let edges = "(kind = 'edge' OR (kind = 'fact' AND object_id IS NOT NULL)) AND tombstone = false AND superseded_by IS NULL";
                rows.extend(Self::scan_in(open_state(&g), Some(edges)).await?);
            }
            cache.load(rows);
        }
        let st = cache.stats();
        tracing::info!(rows = st.rows, bytes = st.bytes, took = ?started.elapsed(), "hot cache loaded");
        *self.hot.write().unwrap() = Some(Arc::new(cache));
        drop(g);
        Ok(())
    }

    /// The last log offset applied to this table (`_meta`, cached).
    pub async fn applied_offset(&self) -> StoreResult<LogOffset> {
        Ok(LogOffset(self.applied.load(std::sync::atomic::Ordering::SeqCst)))
    }

    /// Apply one committed log record as a single MemWAL put. Records at or below
    /// the applied offset are skipped. Returns whether it was applied.
    pub async fn apply(&self, record: &LogRecord) -> StoreResult<bool> {
        Ok(self.apply_many(std::slice::from_ref(record)).await? > 0)
    }

    /// Apply consecutive records as **one** put (W7: fewer, larger MemWAL batches make
    /// every later scan cheaper). Records at or below the applied offset are skipped;
    /// later effects see the rows earlier ones wrote. Returns how many were applied.
    pub async fn apply_many(&self, records: &[LogRecord]) -> StoreResult<usize> {
        let applied_before = self.applied_offset().await?;
        let records: Vec<&LogRecord> = records.iter().filter(|r| r.offset > applied_before).collect();
        let Some(last) = records.last().copied() else { return Ok(0) };
        let g = self.state.read().await;
        let inner = open_state(&g);
        // Rows these records write, one per id (later effects see earlier ones).
        let mut out: BTreeMap<String, StoredRow> = BTreeMap::new();
        for record in &records {
            Self::rows_of(inner, &mut out, record).await?;
        }
        let (off, ts, epoch) = (last.offset.0, last.commit_ts, last.epoch.0);
        out.insert(META_ID.into(), StoredRow::meta(off, ts, epoch, self.meta_extra()));

        let rows: Vec<StoredRow> = out.into_values().collect();
        let batch = encode(&rows, &self.schema, self.dim)?;
        self.apply_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let put = inner.writer.put(vec![batch]).await.map_err(lance);
        // Still under the state lock, so the cache moves with the table (W10).
        if let (Ok(_), Some(h)) = (&put, self.hot()) {
            h.apply(&rows);
        }
        self.apply_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        put?;
        self.applied.store(off, std::sync::atomic::Ordering::SeqCst);
        inner.puts.lock().unwrap().offsets.push_back(off);
        self.puts_since_compaction.fetch_add(records.len() as u64, std::sync::atomic::Ordering::SeqCst);
        if inner.writer.memtable_stats().await.map_err(lance)?.row_count >= self.memtable_rows {
            inner.writer.force_seal_active().await.map_err(lance)?;
        }
        Ok(records.len())
    }

    /// The rows `record` writes, into `out`.
    async fn rows_of(inner: &Inner, out: &mut BTreeMap<String, StoredRow>, record: &LogRecord) -> StoreResult<()> {
        let ts = record.commit_ts;
        let (off, epoch) = (record.offset.0, record.epoch.0);
        let out = &mut *out;
        for effect in &record.effects {
            match effect {
                // Records and edges get memory-qualified row ids (`rows::row_id`).
                Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) | Effect::PutEdge(n) => {
                    let n = qualified(n);
                    out.insert(n.id.0.clone(), StoredRow::from_new(&n, ts, off, epoch));
                }
                Effect::PublishEvent(n) | Effect::Register(n) => {
                    out.insert(n.id.0.clone(), StoredRow::from_new(n, ts, off, epoch));
                }
                Effect::Supersede { old, new } => {
                    let new = qualified(new);
                    if let Some(mut prev) = Self::row(inner, &out, &row_id(new.data.memory, &old.0)).await? {
                        prev.valid_to = Some(ts);
                        prev.superseded_by = Some(new.id.0.clone());
                        prev.log_offset = off;
                        out.insert(prev.id.clone(), prev);
                    }
                    out.insert(new.id.0.clone(), StoredRow::from_new(&new, ts, off, epoch));
                }
                Effect::Patch { memory, id, fields, .. } => {
                    if let Some(mut cur) = Self::row(inner, &out, &row_id(*memory, &id.0)).await? {
                        adb_core::merge_fields(&mut cur.data.value, fields);
                        if let Some(c) = fields.get("content") {
                            cur.data.text = c.as_str().map(String::from);
                            // The vector described the old text; an `Embed` may follow.
                            cur.data.embedding = None;
                        }
                        cur.version += 1;
                        // Last write (D2 T7); commit_ts stays the STORE's, so annotating an
                        // episode doesn't reorder it (D2 §3).
                        cur.valid_from = ts;
                        cur.log_offset = off;
                        out.insert(cur.id.clone(), cur);
                    }
                }
                Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } => {
                    for mut r in Self::lineage_and_edges(inner, &out, &row_id(*memory, &lineage_id.0)).await? {
                        r.tombstone = true;
                        r.log_offset = off;
                        out.insert(r.id.clone(), r);
                    }
                }
                Effect::Embed { memory, id, embedding } => {
                    if let Some(mut cur) = Self::row(inner, &out, &row_id(*memory, &id.0)).await? {
                        cur.data.embedding = Some(embedding.clone());
                        cur.log_offset = off;
                        out.insert(cur.id.clone(), cur);
                    }
                }
                Effect::UpdateSubCursor { sub_id, cursor } => {
                    if let Some(mut sub) = Self::row(inner, &out, &sub_id.0).await? {
                        if let serde_json::Value::Object(v) = &mut sub.data.value {
                            v.insert("cursor".into(), cursor.0.into());
                        }
                        sub.log_offset = off;
                        out.insert(sub.id.clone(), sub);
                    }
                }
                Effect::Unregister { id } => {
                    if let Some(mut r) = Self::row(inner, &out, &id.0).await? {
                        r.tombstone = true;
                        r.log_offset = off;
                        out.insert(r.id.clone(), r);
                    }
                }
                // Platform tables live in their own datasets (adb-platform); the
                // memory table only moves its applied offset past them.
                Effect::TableCreate { .. } | Effect::TableInsert { .. } | Effect::TableUpdate { .. } | Effect::TableDelete { .. } | Effect::TableReplace { .. } => {}
            }
        }
        Ok(())
    }

    /// A row as this record has it so far, else as stored.
    async fn row(inner: &Inner, out: &BTreeMap<String, StoredRow>, id: &str) -> StoreResult<Option<StoredRow>> {
        match out.get(id) {
            Some(r) => Ok(Some(r.clone())),
            None => Ok(Self::scan_in(inner, Some(&format!("id = {}", sql_str(id)))).await?.into_iter().next()),
        }
    }

    /// Every version of a lineage, and every edge touching it (D2 §5).
    async fn lineage_and_edges(inner: &Inner, out: &BTreeMap<String, StoredRow>, lineage: &str) -> StoreResult<Vec<StoredRow>> {
        let l = sql_str(lineage);
        let filter = format!("lineage_id = {l} OR subject_id = {l} OR object_id = {l}");
        let mut rows: BTreeMap<String, StoredRow> =
            Self::scan_in(inner, Some(&filter)).await?.into_iter().map(|r| (r.id.clone(), r)).collect();
        for r in out.values() {
            let hit = r.lineage_id == lineage
                || r.data.subject_id.as_deref() == Some(lineage)
                || r.data.object_id.as_deref() == Some(lineage);
            if hit {
                rows.insert(r.id.clone(), r.clone());
            }
        }
        Ok(rows.into_values().collect())
    }

    /// Batches the MemWAL has made durable, and the writer's batch numbering base
    /// (for mapping durable batches back to log offsets, D2 of the plan).
    pub async fn durable_batches(&self) -> StoreResult<(usize, usize)> {
        let g = self.state.read().await;
        let st = open_state(&g).writer.memtable_stats().await.map_err(lance)?;
        Ok((st.durable_batch_count, st.global_offset))
    }

    /// The highest log offset this table has made durable (MemWAL WAL flushed): a
    /// crash now recovers at least up to it, so the partition log may be purged
    /// below `durable_offset + 1` (stage D3.5). Maps the writer's durable batch count
    /// to the log offset of that put, as the R11 / R12 spikes validated.
    pub async fn durable_offset(&self) -> StoreResult<LogOffset> {
        let g = self.state.read().await;
        let inner = open_state(&g);
        let durable = inner.writer.memtable_stats().await.map_err(lance)?.durable_batch_count;
        let mut p = inner.puts.lock().unwrap();
        if durable <= p.base {
            return Ok(LogOffset(p.durable_at_open));
        }
        // Put number `durable - 1` is the newest durable one.
        let idx = durable - p.base - 1;
        if idx < p.first {
            // Already trimmed: the front entry was durable when we dropped it.
            return Ok(LogOffset(p.durable_at_open));
        }
        let Some(&off) = p.offsets.get(idx - p.first) else {
            return Ok(LogOffset(p.offsets.back().copied().unwrap_or(p.durable_at_open)));
        };
        // Forget puts before it; keep it as the new floor.
        while p.first < idx {
            p.offsets.pop_front();
            p.first += 1;
        }
        p.durable_at_open = p.durable_at_open.max(off);
        Ok(LogOffset(off))
    }

    /// Flush everything written under the store's root to stable storage (`syncfs`
    /// on its filesystem). Lance on local disk doesn't fsync (risk K8, spike R12 §1):
    /// its data files, manifests and MemWAL entries can sit in the page cache, so a
    /// power failure could lose writes Lance reported durable. ADB calls this before
    /// purging its log below Lance's durable offset (stage D3.5), so the log is only
    /// dropped once what replaces it is on disk.
    pub async fn sync_to_disk(&self) -> StoreResult<()> {
        adb_core::disk::sync_fs_async(&self.root).await.map_err(|e| StoreError::Lance(format!("syncfs {}: {e}", self.root.display())))
    }

    /// Flush and release the writer.
    pub async fn close(self) -> StoreResult<()> {
        match self.state.into_inner() {
            Some(inner) => inner.writer.close().await.map_err(lance),
            None => Ok(()),
        }
    }
}

/// What W5 compaction keeps of a row: `_meta`, and rows that are not tombstoned
/// (forgotten, expired by the timer, unlinked, unsubscribed), not past their TTL,
/// and not a version superseded more than `history` ago. A tombstoned lineage keeps
/// one **stub** — its first row with the payload stripped (no value, text, vector or
/// sealed fields) — so its id still resolves to its memory, kind and scope:
/// subscribers get the `forget` / `unlink` event, and replay finds the tombstone.
fn compaction_row(history: Duration, now: CommitTs) -> impl Fn(StoredRow) -> Option<StoredRow> + Sync {
    let horizon = CommitTs(now.0 - history.as_micros() as i64);
    move |mut r: StoredRow| {
        if r.id == META_ID {
            return Some(r);
        }
        if r.tombstone {
            if r.id != r.lineage_id {
                return None;
            }
            r.data.value = serde_json::json!({});
            r.data.text = None;
            r.data.embedding = None;
            r.data.sealed = None;
            return Some(r);
        }
        let live = r.data.ttl_at.is_none_or(|t| t > now)
            && !(r.superseded_by.is_some() && r.valid_to.is_some_and(|t| t < horizon));
        live.then_some(r)
    }
}

/// What a table rewrite did (W5).
#[derive(Debug, Clone)]
pub struct CompactStats {
    /// Row versions read
    pub total: usize,
    /// Row versions kept
    pub kept: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub took: std::time::Duration,
}

/// Bytes under `dir`.
fn dir_size(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    entries
        .filter_map(|e| e.ok())
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_size(&e.path()),
            _ => e.metadata().map(|m| m.len()).unwrap_or(0),
        })
        .sum()
}

/// The dimension of a table's `embedding` column (`None`: no such column).
fn embedding_dim_of(ds: &Dataset) -> Option<i32> {
    match ds.schema().field("embedding")?.data_type() {
        arrow_schema::DataType::FixedSizeList(_, n) => Some(n),
        _ => None,
    }
}

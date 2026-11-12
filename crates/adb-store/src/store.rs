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
use crate::rows::{decode, encode, key_of, qualified, row_id, scoped_id, unscoped, StoredRow};
use crate::schema::{memory_schema, META_ID, RECORD_KINDS, SCHEMA_VERSION};
use crate::{StoreError, StoreResult};

#[path = "store_fold.rs"]
mod fold;

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

/// Rows per chunk when a compaction rewrites a table.
const REWRITE_CHUNK: usize = 10_000;

/// What a batch costs in memory beyond the contents Lance counts: one array per
/// column (34) with its own small allocations. Measured: 24 KiB per batch of one or two
/// rows (heap profile against Lance's count, 7 Nov).
const BATCH_OVERHEAD: usize = 24 << 10;

/// Bytes at which ADB seals the active memtable, whatever its row count, and of unsealed
/// Batches in the active memtable at which it is sealed (`ADB_MEMTABLE_BATCHES`,
/// default 256, PF-52).
fn memtable_batches() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::env::var("ADB_MEMTABLE_BATCHES").ok().and_then(|v| v.parse().ok()).unwrap_or(256).max(8))
}

/// plus unflushed memtables at which writes wait: `ADB_MEMTABLE_MB` (default 64) and
/// `ADB_MEMTABLE_BACKLOG_MB` (default 1024).
fn memtable_limits() -> (usize, usize) {
    static LIMITS: std::sync::OnceLock<(usize, usize)> = std::sync::OnceLock::new();
    *LIMITS.get_or_init(|| {
        let mb = |var: &str, d: usize| std::env::var(var).ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(d) << 20;
        (mb("ADB_MEMTABLE_MB", 64), mb("ADB_MEMTABLE_BACKLOG_MB", 1024))
    })
}

fn lance(e: impl std::fmt::Display) -> StoreError {
    StoreError::Lance(e.to_string())
}

/// Quote a string for a Lance SQL filter.
/// The table's indexes, kept up to date by the MemWAL as it flushes: `id_idx` (a row
/// version by id) and `lineage_idx` (every version of a record: the current version an
/// UPDATE follows, the versions a FORGET takes, the applier's FORGET; without it each
/// of these scanned the table, PF-11, 7 Nov).
async fn table_indexes(ds: &mut Dataset) -> StoreResult<()> {
    for (col, name) in [("id", "id_idx"), ("lineage_id", "lineage_idx")] {
        ds.create_index(&[col], IndexType::BTree, Some(name.into()), &ScalarIndexParams::default(), true).await.map_err(lance)?;
    }
    ds.initialize_mem_wal().unsharded().maintained_indexes(["id_idx", "lineage_idx"]).execute().await.map_err(lance)?;
    Ok(())
}

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
    /// One fold (compaction, PF-16) or table rewrite at a time.
    folding: tokio::sync::Mutex<()>,
    /// Rows the write path read for a record it then submitted (PF-48), by the
    /// record's offset: the applier uses them instead of reading the table again.
    hints: std::sync::Mutex<std::collections::HashMap<u64, Vec<StoredRow>>>,
    /// The table's vectors came from another embedding model (W9.10).
    model_changed: std::sync::atomic::AtomicBool,
    /// W10 hot cache, fed by every put (`set_cache`).
    hot: std::sync::RwLock<Option<Arc<HotCache>>>,
    /// Field indexes (PF-27), kept in step with every apply like the hot cache.
    fields: crate::field_index::FieldIndex,
    /// Rows the field indexes read by id (`get_cached`), dropped by every put.
    rows: crate::row_cache::RowCache,
    /// The live records' keys, kept by every apply once built (PF-38).
    live: crate::live_index::LiveIndex,
    /// Set while this node takes the lead: a compaction under way gives up between
    /// its steps rather than compete with the takeover (PF-38).
    yield_compaction: std::sync::atomic::AtomicBool,
    /// Odd while a put and its cache update are under way (`apply_seq`).
    apply_seq: std::sync::atomic::AtomicU64,
    /// The schema version `_meta` gets (`schema_version`).
    layout: std::sync::atomic::AtomicU32,
}

/// The open table: base dataset, MemWAL writer, SSTable cache, and the put ledger.
struct Inner {
    base: Arc<Dataset>,
    writer: ShardWriter,
    cache: Arc<dyn DatasetCache>,
    /// Log offset of each put since open, to map the MemWAL's durable batch count
    /// back to a log offset (`durable_offset`).
    puts: std::sync::Mutex<PutLedger>,
    /// Row ids by lineage and by the records an edge or fact touches (built on first
    /// FORGET, kept by every put): a FORGET finds what it erases by id, not by a
    /// scan of the whole table (PF-36). Per open table: a rewrite starts it again.
    links: std::sync::Mutex<Option<Links>>,
}

/// Row ids by lineage, and by subject / object id (see `Inner::links`). Ids of rows a
/// compaction removed may linger until the next rewrite: lookups are a superset.
#[derive(Default)]
struct Links {
    by_lineage: std::collections::HashMap<String, Vec<String>>,
    by_node: std::collections::HashMap<String, Vec<String>>,
}

impl Links {
    fn add(&mut self, id: &str, lineage: &str, subject: Option<&str>, object: Option<&str>) {
        fn put(m: &mut std::collections::HashMap<String, Vec<String>>, k: &str, id: &str) {
            let v = m.entry(k.to_string()).or_default();
            if !v.iter().any(|x| x == id) {
                v.push(id.to_string());
            }
        }
        if id == META_ID {
            return;
        }
        put(&mut self.by_lineage, lineage, id);
        for n in [subject, object].into_iter().flatten() {
            put(&mut self.by_node, n, id);
        }
    }

    /// Ids of every row of `lineage` and every row touching it.
    fn of(&self, lineage: &str) -> Vec<String> {
        let mut ids: Vec<String> = self.by_lineage.get(lineage).cloned().unwrap_or_default();
        for id in self.by_node.get(lineage).into_iter().flatten() {
            if !ids.contains(id) {
                ids.push(id.clone());
            }
        }
        ids
    }
}

/// [`decode`] on several cores for a large batch (C-11): decoding parses every row's
/// payload, the bulk of a scan's CPU. Slices of one batch share its buffers; the
/// rows come back in batch order.
fn decode_parallel(b: &arrow_array::RecordBatch) -> StoreResult<Vec<StoredRow>> {
    const PER_THREAD: usize = 8192;
    let n = b.num_rows();
    let threads = std::thread::available_parallelism().map_or(1, |p| p.get()).min(8).min(n / PER_THREAD).max(1);
    if threads == 1 {
        return decode(b);
    }
    let per = n.div_ceil(threads);
    let parts: Vec<StoreResult<Vec<StoredRow>>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let (off, len) = (t * per, per.min(n.saturating_sub(t * per)));
                let slice = b.slice(off, len);
                s.spawn(move || decode(&slice))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(StoreError::Decode("decode thread panicked".into())))).collect()
    });
    let mut rows = Vec::with_capacity(n);
    for p in parts {
        rows.extend(p?);
    }
    Ok(rows)
}

/// A scan's batches decoded as they arrive, a few at a time on several cores (C-11),
/// so the table is never held whole as Arrow next to its decoded rows (PF-37: one
/// `ORDER BY` over 925k rows took a node from 1.0 to 3.7 GB). Rows in scan order.
async fn decode_stream(scanner: &LsmScanner) -> StoreResult<Vec<StoredRow>> {
    use futures::TryStreamExt;
    const CHUNK_ROWS: usize = 65_536;
    let mut stream = scanner.try_into_stream().await.map_err(lance)?;
    let mut rows = Vec::new();
    let mut pending: Vec<arrow_array::RecordBatch> = Vec::new();
    let mut held = 0usize;
    while let Some(b) = stream.try_next().await.map_err(|e| StoreError::Decode(format!("scan: {e}")))? {
        held += b.num_rows();
        pending.push(b);
        if held >= CHUNK_ROWS {
            rows.extend(decode_batches(std::mem::take(&mut pending))?);
            held = 0;
        }
    }
    rows.extend(decode_batches(pending)?);
    Ok(rows)
}

/// [`decode`] over several batches, contiguous runs of them per thread, in order.
fn decode_batches(batches: Vec<arrow_array::RecordBatch>) -> StoreResult<Vec<StoredRow>> {
    match batches.len() {
        0 => return Ok(Vec::new()),
        1 => return decode_parallel(&batches[0]),
        _ => {}
    }
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    let threads = std::thread::available_parallelism().map_or(1, |p| p.get()).min(8).min(batches.len()).max(1);
    if threads == 1 || n < 8192 {
        let mut rows = Vec::with_capacity(n);
        for b in &batches {
            rows.extend(decode(b)?);
        }
        return Ok(rows);
    }
    let per = batches.len().div_ceil(threads);
    let parts: Vec<StoreResult<Vec<StoredRow>>> = std::thread::scope(|s| {
        let handles: Vec<_> = batches
            .chunks(per)
            .map(|run| {
                s.spawn(move || {
                    let mut out = Vec::new();
                    for b in run {
                        out.extend(decode(b)?);
                    }
                    Ok(out)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(StoreError::Decode("decode thread panicked".into())))).collect()
    });
    let mut rows = Vec::with_capacity(n);
    for p in parts {
        rows.extend(p?);
    }
    Ok(rows)
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

/// Table rows read ahead for a batch of records (PF-47): `None` = not in the table.
type Prefetched = std::collections::HashMap<String, Option<StoredRow>>;

/// Primary-key reads running at once in an apply's prefetch.
const PREFETCH_AT_ONCE: usize = 32;

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

    /// W5 compaction on the open table, in place (PF-16, `store_fold.rs`): what the
    /// rule of [`Self::compact_offline`] drops leaves the base table, and the bytes of
    /// tombstoned and expired rows leave the disk (their fragments are rewritten, old
    /// versions, folded SSTables and covered MemWAL entries deleted). Writes and reads
    /// go on meanwhile; the caller makes sure every committed record is applied first.
    pub async fn compact(&self, history: Duration, now: CommitTs) -> StoreResult<CompactStats> {
        self.compact_pinned(history, now, &Default::default()).await
    }

    /// [`Self::compact`] keeping, per namespace in `pins`, every version that was
    /// current at or after the pinned commit time: the branch points of open memory
    /// branches (`.ai/BRANCHING.md` B4), whose reads go `AS OF` them.
    /// As [`Self::compact_pinned`], with `(pattern, window)` overrides of `history` per
    /// namespace (a namespace, or a prefix ending in `*`; DB-8). A pin older than the window
    /// still wins: a tag or a branch keeps what it reads.
    pub async fn compact_with_windows(&self, history: Duration, now: CommitTs, pins: &std::collections::HashMap<String, CommitTs>, windows: Vec<(String, Duration)>) -> StoreResult<CompactStats> {
        // The latest horizon any namespace uses (pins only move one earlier): base rows
        // superseded before it are the candidates `keep` decides.
        let shortest = windows.iter().map(|(_, w)| *w).fold(history, Duration::min);
        let horizon = CommitTs(now.0 - shortest.as_micros() as i64);
        let keep = compaction_row_windows(history, now, pins.clone(), windows);
        self.fold(&keep, now, horizon).await
    }

    pub async fn compact_pinned(&self, history: Duration, now: CommitTs, pins: &std::collections::HashMap<String, CommitTs>) -> StoreResult<CompactStats> {
        self.compact_with_windows(history, now, pins, Vec::new()).await
    }

    /// Compaction for a table whose rows are written once and never come back
    /// (the engine journal, stage H): as [`Self::compact`], but deleted rows
    /// leave no marker behind. Only safe where no record below the applied
    /// offset is replayed again (writes at or below it are skipped).
    pub async fn compact_append_only(&self, now: CommitTs) -> StoreResult<CompactStats> {
        let base = compaction_row(Duration::ZERO, now);
        let keep = move |r: StoredRow| if r.id != META_ID && r.tombstone { None } else { base(r) };
        self.fold(&keep, now, now).await
    }

    /// The schema version `_meta` records (D3 §4): 3 until `scope_row_ids` ran.
    pub fn schema_version(&self) -> u32 {
        self.layout.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Tests: treat the table as schema `v` (`scope_row_ids` runs again below 4).
    #[doc(hidden)]
    pub fn set_schema_version(&self, v: u32) {
        self.layout.store(v, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether the vectors came from another embedding model than the configured one
    /// (W9.10): the caller applies the log's tail, then [`Self::clear_vectors`].
    pub fn model_changed(&self) -> bool {
        self.model_changed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Schema v3 → v4 (E-L12): give record rows in a named scope ids that carry it
    /// (`rows::scoped_id`), and point facts and links at their ends' new ids, as an
    /// online rewrite (the caller makes sure nothing is being applied; it runs after
    /// the log's tail, which an older ADB wrote with bare ids). Version ids after
    /// the first are ULIDs and stay. Returns how many rows changed.
    pub async fn scope_row_ids(&self) -> StoreResult<usize> {
        use std::collections::HashMap;
        if self.schema_version() >= 4 {
            return Ok(0);
        }
        let mut moved: HashMap<String, String> = HashMap::new();
        for r in self.scan(Some(RECORD_KINDS)).await? {
            let key = key_of(&r.lineage_id);
            let scoped = scoped_id(&r.data.scope, key);
            if scoped != key {
                moved.insert(r.lineage_id.clone(), row_id(r.data.memory, &scoped));
            }
        }
        let n = std::sync::atomic::AtomicUsize::new(0);
        let keep = |mut r: StoredRow| {
            if r.id == META_ID {
                if let serde_json::Value::Object(v) = &mut r.data.value {
                    v.insert("schema_version".into(), 4.into());
                }
                return Some(r);
            }
            let before = (r.id.clone(), r.lineage_id.clone(), r.data.subject_id.clone(), r.data.object_id.clone());
            let to = |id: &str| moved.get(id).cloned();
            if let Some(l) = to(&r.lineage_id) {
                if r.id == r.lineage_id {
                    r.id = l.clone();
                }
                r.lineage_id = l;
            }
            if matches!(r.data.kind, Kind::Fact | Kind::Edge) {
                r.data.subject_id = r.data.subject_id.map(|s| to(&s).unwrap_or(s));
                r.data.object_id = r.data.object_id.map(|o| to(&o).unwrap_or(o));
            }
            if (r.id.clone(), r.lineage_id.clone(), r.data.subject_id.clone(), r.data.object_id.clone()) != before {
                n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Some(r)
        };
        self.rewrite_online(&keep, false).await?;
        self.layout.store(4, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.hot().filter(|_| !moved.is_empty()) {
            let g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
            Self::fill_cache(&g, &h).await?;
        }
        Ok(n.into_inner())
    }

    /// Schema v4 → v5 (NS, `.ai/NS_IDS.md`): record ids carry their namespace
    /// (`rows::space_id`) — keys are unique per (namespace, scope, memory type). As an
    /// online rewrite after the log's tail (the caller makes sure nothing is being
    /// applied): every record in a namespace gets the id `<ns>|<scope part>~<key>`; a
    /// FACT's ends become the entities of the fact's own namespace, with a stub entity
    /// written there when it has none (today's shared stubs split); a LINK's ends
    /// follow their records. Returns how many rows changed or were added.
    pub async fn namespace_row_ids(&self) -> StoreResult<usize> {
        use std::collections::{HashMap, HashSet};
        if self.schema_version() >= 5 {
            return Ok(0);
        }
        let all = self.scan(None).await?;
        let mut moved: HashMap<String, String> = HashMap::new();
        let mut entities: HashSet<String> = HashSet::new();
        let new_lineage = |r: &StoredRow| -> String {
            let key = key_of(&r.lineage_id);
            match r.data.namespace.as_deref() {
                Some(ns) if crate::rows::valid_namespace(ns) => row_id(r.data.memory, &crate::rows::namespaced(Some(ns), key)),
                Some(ns) => {
                    tracing::warn!(namespace = ns, id = %r.id, "schema v5: a namespace with '|' or '~' can't be part of an id: the record keeps its id");
                    r.lineage_id.clone()
                }
                None => r.lineage_id.clone(),
            }
        };
        for r in all.iter().filter(|r| matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool)) {
            let l = new_lineage(r);
            if r.data.kind == Kind::Entity && !r.tombstone {
                entities.insert(l.clone());
            }
            if l != r.lineage_id {
                moved.insert(r.lineage_id.clone(), l);
            }
        }
        // A fact's end in the fact's own namespace.
        let fact_end = |fact: &StoredRow, end: &str| -> String {
            let ns = fact.data.namespace.as_deref().filter(|ns| crate::rows::valid_namespace(ns));
            row_id(fact.data.memory, &crate::rows::namespaced(ns, crate::rows::without_namespace(key_of(end))))
        };
        let mut stubs: HashMap<String, StoredRow> = HashMap::new();
        for f in all.iter().filter(|r| r.data.kind == Kind::Fact && !r.tombstone) {
            for end in [f.data.subject_id.as_deref(), f.data.object_id.as_deref()].into_iter().flatten() {
                let to = fact_end(f, end);
                if entities.contains(&to) || stubs.contains_key(&to) {
                    continue;
                }
                let bare = unscoped(key_of(&to)).to_string();
                let mut data = f.data.clone();
                data.kind = Kind::Entity;
                data.key = Some(bare.clone());
                data.subject_id = None;
                data.predicate = None;
                data.object_id = None;
                data.link_type = None;
                data.weight = None;
                data.value = serde_json::json!({ "key": bare, "name": bare });
                data.sealed = None;
                data.text = None;
                data.embedding = None;
                data.ttl_at = None;
                let stub = StoredRow {
                    id: to.clone(),
                    lineage_id: to.clone(),
                    data,
                    commit_ts: f.commit_ts,
                    valid_from: f.commit_ts,
                    valid_to: None,
                    superseded_by: None,
                    version: 1,
                    tombstone: false,
                    log_offset: f.log_offset,
                    epoch: f.epoch,
                };
                stubs.insert(to, stub);
            }
        }
        drop(all);
        let n = std::sync::atomic::AtomicUsize::new(stubs.len());
        let keep = |mut r: StoredRow| {
            if r.id == META_ID {
                if let serde_json::Value::Object(v) = &mut r.data.value {
                    v.insert("schema_version".into(), 5.into());
                }
                return Some(r);
            }
            let before = (r.id.clone(), r.lineage_id.clone(), r.data.subject_id.clone(), r.data.object_id.clone());
            if let Some(l) = moved.get(&r.lineage_id).cloned() {
                if r.id == r.lineage_id {
                    r.id = l.clone();
                }
                r.lineage_id = l;
            }
            match r.data.kind {
                Kind::Fact => {
                    let ends = (r.data.subject_id.clone(), r.data.object_id.clone());
                    r.data.subject_id = ends.0.map(|s| fact_end(&r, &s));
                    r.data.object_id = ends.1.map(|o| fact_end(&r, &o));
                }
                Kind::Edge => {
                    r.data.subject_id = r.data.subject_id.map(|s| moved.get(&s).cloned().unwrap_or(s));
                    r.data.object_id = r.data.object_id.map(|o| moved.get(&o).cloned().unwrap_or(o));
                }
                _ => {}
            }
            if (r.id.clone(), r.lineage_id.clone(), r.data.subject_id.clone(), r.data.object_id.clone()) != before {
                n.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Some(r)
        };
        let extra: Vec<StoredRow> = stubs.into_values().collect();
        self.rewrite_online_with(&keep, &extra, false).await?;
        self.layout.store(5, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.hot() {
            let g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
            Self::fill_cache(&g, &h).await?;
        }
        Ok(n.into_inner())
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
        self.rewrite_online_with(keep, &[], vectors_cleared).await
    }

    /// Stream the rows `keep` accepts from `inner` into a new table at `staged`, as its
    /// base data, `REWRITE_CHUNK` rows at a time: memory follows the chunk, not the
    /// table, and the new table has no MemWAL generations. The LSM scan yields the
    /// newest version of each id once (`_meta` included), so the base holds no
    /// duplicates. Returns (rows read, rows kept).
    async fn write_kept(&self, inner: &Inner, keep: &(dyn Fn(StoredRow) -> Option<StoredRow> + Sync), staged: &std::path::Path) -> StoreResult<(usize, usize)> {
        use futures::StreamExt;
        let uri = staged.to_string_lossy().to_string();
        let mut stream = Self::scanner(inner).await?.try_into_stream().await.map_err(lance)?;
        let (mut total, mut kept) = (0, 0);
        let mut buf: Vec<StoredRow> = Vec::new();
        let mut ds: Option<Dataset> = None;
        loop {
            let next = stream.next().await;
            if let Some(b) = &next {
                let rows = decode(b.as_ref().map_err(lance)?)?;
                total += rows.len();
                buf.extend(rows.into_iter().filter_map(keep));
            }
            let done = next.is_none();
            if buf.len() >= REWRITE_CHUNK || (done && !buf.is_empty()) {
                kept += buf.len();
                let batch = encode(&buf, &self.schema, self.dim)?;
                buf.clear();
                let mode = if ds.is_some() { lance::dataset::WriteMode::Append } else { lance::dataset::WriteMode::Create };
                let params = WriteParams { mode, ..Default::default() };
                ds = Some(Dataset::write(RecordBatchIterator::new([Ok(batch)], self.schema.clone()), &uri, Some(params)).await.map_err(lance)?);
            }
            if done {
                break;
            }
        }
        let Some(mut ds) = ds else {
            // Nothing at all, not even `_meta` (not expected): an empty table.
            Self::create(&uri, &self.schema, self.dim).await?;
            return Ok((total, kept));
        };
        table_indexes(&mut ds).await?;
        Ok((total, kept))
    }

    /// [`Self::rewrite_online`] adding `extra` rows to the new table (a migration's
    /// new rows: in the same swap, so all or nothing).
    async fn rewrite_online_with(&self, keep: &(dyn Fn(StoredRow) -> Option<StoredRow> + Sync), extra: &[StoredRow], vectors_cleared: bool) -> StoreResult<CompactStats> {
        let _t = adb_core::steps::timer("store.rewrite_online (compaction)");
        let started = std::time::Instant::now();
        let dir = self.root.join(format!("p{}", self.partition.0)).join("memory.lance");
        let staged = dir.with_file_name("memory.rewrite.lance");
        let trash = dir.with_file_name("memory.compacted.trash");
        let err = |e: std::io::Error| StoreError::Decode(format!("compaction: {e}"));
        let _one = self.folding.lock().await;
        let mut g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
        let bytes_before = dir_size(&dir);
        // The new table, complete and closed, beside the old one.
        if staged.exists() {
            std::fs::remove_dir_all(&staged).map_err(err)?;
        }
        let (total, kept) = if extra.is_empty() {
            self.write_kept(open_state(&g), keep, &staged).await?
        } else {
            // A migration's rows replace rows of the same id, which only a put through
            // the MemWAL (newest wins) gets right: in memory, as before. Rare and one-off.
            let all = Self::scan_in(open_state(&g), None).await?;
            let total = all.len();
            let mut rows: Vec<StoredRow> = all.into_iter().filter_map(keep).collect();
            rows.extend(extra.iter().cloned());
            Self::create(&staged.to_string_lossy(), &self.schema, self.dim).await?;
            let new = Self::open_inner(&self.cfg, self.partition, &staged).await?;
            for chunk in rows.chunks(REWRITE_CHUNK) {
                new.writer.put(vec![encode(chunk, &self.schema, self.dim)?]).await.map_err(lance)?;
            }
            new.writer.close().await.map_err(lance)?;
            (total, rows.len())
        };
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
        // Compaction may drop or rewrite rows: cached ones read again.
        self.rows.clear();
        // A migration's rows bypassed the applies: the field indexes rebuild on use.
        // (Compaction keeps every current row as it was, so they stay.)
        if !extra.is_empty() {
            self.fields.clear();
            self.live.abandon();
        }
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
        Ok(CompactStats { total, kept, bytes_before, bytes_after: dir_size(&dir), took: started.elapsed() })
    }

    /// What `_meta` records about the table besides offsets: its embedding size and
    /// (W9.10) the model its vectors came from.
    fn meta_extra(&self) -> serde_json::Value {
        let mut v = serde_json::json!({ "embedding_dim": self.dim, "schema_version": self.schema_version() });
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
        self.puts_since_compaction.load(SeqCst) > min.max(self.rows_at_compaction.load(SeqCst) / 20)
    }

    /// MemWAL generations (sealed memtables) not yet compacted away: a W5 threshold.
    pub async fn generation_count(&self) -> StoreResult<usize> {
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        Ok(open_state(&g).writer.manifest().await.map_err(lance)?.map(|m| m.sstables.len()).unwrap_or(0))
    }

    /// Put `rows` into the table (chunks of 10k), outside the log: rewrites only.
    async fn put_rows(&self, rows: &[StoredRow]) -> StoreResult<()> {
        let _t = adb_core::steps::timer("store.put_rows");
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        for chunk in rows.chunks(10_000) {
            let put = open_state(&g).writer.put(vec![encode(chunk, &self.schema, self.dim)?]).await.map_err(lance);
            self.rows.drop_rows(chunk);
            put?;
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
            folding: tokio::sync::Mutex::new(()),
            hints: Default::default(),
            model_changed: std::sync::atomic::AtomicBool::new(false),
            hot: std::sync::RwLock::new(None),
            fields: crate::field_index::FieldIndex::new(),
            rows: crate::row_cache::RowCache::new(),
            live: crate::live_index::LiveIndex::new(),
            yield_compaction: std::sync::atomic::AtomicBool::new(false),
            apply_seq: std::sync::atomic::AtomicU64::new(0),
            layout: std::sync::atomic::AtomicU32::new(SCHEMA_VERSION),
        };
        let meta = store.get(META_ID).await?.ok_or_else(|| StoreError::Decode("no _meta row".into()))?;
        let applied = meta.data.value.get("applied_offset").and_then(|v| v.as_u64()).unwrap_or(0);
        store.applied.store(applied, std::sync::atomic::Ordering::SeqCst);
        let version = meta.data.value.get("schema_version").and_then(|v| v.as_u64()).unwrap_or(0);
        if version < 4 {
            // v4's ids come after the log's tail (`scope_row_ids`): until then `_meta` says v3.
            store.layout.store(3, std::sync::atomic::Ordering::SeqCst);
        } else if version < 5 {
            // v5's ids (`namespace_row_ids`), likewise after the tail.
            store.layout.store(4, std::sync::atomic::Ordering::SeqCst);
        }
        if version < 3 {
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
            // Half the backlog (Lance requires it to fit): ADB's own seal comes first, at
            // `memtable_limits().0`.
            .with_max_memtable_size(memtable_limits().1 / 2)
            // Each memtable allocates a slot per batch up front, about 72 bytes each.
            .with_max_memtable_batches(100_000)
            // Writes wait here. Below the pod's limit: a flush that falls behind must slow
            // writers down, not end in an OOM.
            .with_max_unflushed_memtable_bytes(memtable_limits().1);
        let writer = base.mem_wal_writer(shard, config).await.map_err(lance)?;
        let _ = schema;
        Ok(Inner { base, writer, cache: Arc::new(SsTableCache::new(64)), puts: std::sync::Mutex::new(PutLedger::default()), links: std::sync::Mutex::new(None) })
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
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        let put = open_state(&g).writer.put(vec![batch]).await.map_err(lance);
        self.rows.drop_rows(&out);
        put?;
        Ok(())
    }

    /// Base table with the `_meta` row, a BTree on `id`, and a MemWAL maintaining it.
    async fn create(uri: &str, schema: &SchemaRef, dim: i32) -> StoreResult<()> {
        let meta = StoredRow::meta(0, CommitTs(0), 0, serde_json::json!({ "embedding_dim": dim }));
        let batch = encode(&[meta], schema, dim)?;
        let mut ds = Dataset::write(RecordBatchIterator::new([Ok(batch)], schema.clone()), uri, Some(WriteParams::default()))
            .await
            .map_err(lance)?;
        table_indexes(&mut ds).await?;
        Ok(())
    }

    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Every row version matching `filter` (Lance SQL), newest per id, `_meta` included.
    pub async fn scan(&self, filter: Option<&str>) -> StoreResult<Vec<StoredRow>> {
        let _t = adb_core::steps::timer("store.scan");
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        Self::scan_in(open_state(&g), filter).await
    }

    async fn scan_in(inner: &Inner, filter: Option<&str>) -> StoreResult<Vec<StoredRow>> {
        let t0 = std::time::Instant::now();
        let mut scanner = Self::scanner(inner).await?;
        if let Some(f) = filter {
            scanner = scanner.filter(f).map_err(lance)?;
        }
        let rows = decode_stream(&scanner).await?;
        // One line per filtered LSM scan (`RUST_LOG=adb_store::scan=debug`).
        tracing::debug!(target: "adb_store::scan", filter = filter.unwrap_or(""), rows = rows.len(), us = t0.elapsed().as_micros() as u64, "scan");
        Ok(rows)
    }

    /// As [`PartitionStore::scan`] with a DataFusion expression, for filters the SQL
    /// parser can't express: Lance's json_* functions on `value` (stage D3.6).
    /// Filtering happens after the LSM picks the newest version of each row.
    pub async fn scan_expr(&self, filter: Option<Expr>) -> StoreResult<Vec<StoredRow>> {
        let _t = adb_core::steps::timer("store.scan_expr");
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        let mut scanner = Self::scanner(open_state(&g)).await?;
        if let Some(f) = filter {
            scanner = scanner.filter_expr(f);
        }
        decode_stream(&scanner).await
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
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
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
            // A filtered scan answers from the memtables' maintained indexes (id_idx,
            // lineage_idx) instead of reading every in-memory row, which is Lance's
            // default (PF-8/PF-11, 7 Nov). Same result: a match counts only as its
            // key's newest visible version.
            .with_memtable_filter_indexes(true)
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

    /// The live rows (transaction time, [`StoredRow::is_live`]) at `now`, without
    /// `_meta`: what writes and key checks see, facts outside their validity included.
    /// [`Self::live`] with a DataFusion filter (payload fields: `filter::field_text`).
    pub async fn live_expr(&self, filter: Option<Expr>, now: CommitTs) -> StoreResult<Vec<StoredRow>> {
        Ok(self
            .scan_expr(filter)
            .await?
            .into_iter()
            .filter(|r| r.id != META_ID && r.data.kind != Kind::Meta && r.is_live(now))
            .collect())
    }

    pub async fn live(&self, filter: Option<&str>, now: CommitTs) -> StoreResult<Vec<StoredRow>> {
        Ok(self
            .scan(filter)
            .await?
            .into_iter()
            .filter(|r| r.id != META_ID && r.data.kind != Kind::Meta && r.is_live(now))
            .collect())
    }

    /// The live records' keys (entities, facts, episodes, tools) at `now`: what the
    /// write path's key check starts from. Reads only the key columns, so a takeover
    /// doesn't decode every payload and embedding (C-19).
    pub async fn live_keys(&self, now: CommitTs) -> StoreResult<Vec<crate::rows::LiveKey>> {
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        let scanner = Self::scanner(open_state(&g))
            .await?
            // Kind only: a row's kind never changes, so no older version can match
            // where its newest doesn't; liveness is checked on the newest version.
            .filter(RECORD_KINDS)
            .map_err(lance)?
            .project(&crate::rows::LIVE_KEY_COLUMNS)
            .map_err(lance)?;
        let batch = scanner.try_into_batch().await.map_err(lance)?;
        crate::rows::decode_live_keys(&batch, now)
    }

    /// Every version of `memory`'s records that isn't forgotten or expired, current
    /// and superseded (AQL v0.10 history reads; `filter` narrows it).
    pub async fn versions(&self, memory: adb_core::log::Memory, filter: Option<&str>) -> StoreResult<Vec<StoredRow>> {
        let _t = adb_core::steps::timer("store.versions");
        let mut f = format!("memory = {} AND {RECORD_KINDS} AND tombstone = false", sql_str(memory.tag()));
        if let Some(x) = filter {
            f.push_str(&format!(" AND ({x})"));
        }
        self.scan(Some(&f)).await
    }

    /// Every version of the given lineages (row lineage ids) that isn't forgotten, as
    /// [`PartitionStore::versions`] with a lineage filter — read by primary key through
    /// the links map, not by a scan of the table (C-25).
    pub async fn versions_of(&self, memory: adb_core::log::Memory, lineages: &[String]) -> StoreResult<Vec<StoredRow>> {
        let _t = adb_core::steps::timer("store.versions_of");
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        let inner = open_state(&g);
        if inner.links.lock().unwrap().is_none() {
            let built = Self::build_links(inner).await?;
            inner.links.lock().unwrap().get_or_insert(built);
        }
        let ids: Vec<String> = {
            let l = inner.links.lock().unwrap();
            let l = l.as_ref().expect("links built above");
            lineages.iter().flat_map(|x| l.by_lineage.get(x).cloned().unwrap_or_default()).collect()
        };
        let mut out = Vec::new();
        for id in ids {
            if let Some(r) = Self::scan_in(inner, Some(&format!("id = {}", sql_str(&id)))).await?.into_iter().next() {
                if r.data.memory == memory && !r.tombstone && matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool) {
                    out.push(r);
                }
            }
        }
        Ok(out)
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
    /// live row, D2 T1; a fact outside its validity too), if any.
    pub async fn current_version(&self, memory: adb_core::log::Memory, key: &str, now: CommitTs) -> StoreResult<Option<StoredRow>> {
        let _t = adb_core::steps::timer("store.current_version");
        let filter = format!("lineage_id = {} AND {RECORD_KINDS}", sql_str(&row_id(memory, key)));
        Ok(self.live(Some(&filter), now).await?.into_iter().max_by_key(|r| r.version))
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

    /// The table rows the record at `offset` changes, as its writer read them (PF-48).
    /// Only for rows no other record can change in between: the writer read them after
    /// every earlier write to them was applied, and submitted the record under the
    /// sequencer's conflict check, so the applier would read these same rows. Reading
    /// them again there cost ~4 ms each, in series: ~200 UPDATEs / FORGETs a second,
    /// and every read waiting behind the applier.
    pub fn hint_rows(&self, offset: u64, rows: Vec<StoredRow>) {
        if rows.is_empty() {
            return;
        }
        let mut h = self.hints.lock().unwrap();
        // Never applied (a failed commit, a restart of the applier): don't keep them.
        if h.len() > 100_000 {
            h.clear();
        }
        h.insert(offset, rows);
    }

    /// [`get`](Self::get) through the row cache (exact: see `row_cache`).
    pub async fn get_cached(&self, id: &str) -> StoreResult<Option<StoredRow>> {
        let _t = adb_core::steps::timer("store.get_cached");
        match self.rows.begin(id) {
            Ok(r) => Ok(Some(r)),
            Err(reading) => {
                let r = self.get(id).await;
                self.rows.finish(reading, r.as_ref().ok().and_then(|r| r.as_ref()));
                r
            }
        }
    }

    /// Counts puts in two steps (before the put, after the cache took its rows):
    /// odd while one is under way. Two reads between equal, even values saw the
    /// same state in the table and the cache (the W10 shadow check).
    pub fn apply_seq(&self) -> u64 {
        self.apply_seq.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The field indexes (PF-27).
    pub fn fields(&self) -> &crate::field_index::FieldIndex {
        &self.fields
    }

    /// Build the field index of (`memory`, `field`) from one scan of the type's current
    /// rows, a batch at a time (no embeddings, no table-sized vector of rows). `false`
    /// when it is built, being built, or not indexable. The pair is remembered on disk,
    /// so the next open builds it in the background (PF-40).
    pub async fn build_field_index(&self, memory: adb_core::log::Memory, field: &str) -> StoreResult<bool> {
        if !self.fields.begin(memory, field) {
            return Ok(false);
        }
        let t0 = std::time::Instant::now();
        let filter = format!("memory = '{}' AND {RECORD_KINDS} AND tombstone = false AND superseded_by IS NULL", memory.tag());
        let mut b = self.fields.builder(memory, field);
        match self.scan_each(&filter, |rows| rows.iter().for_each(|r| b.add(r))).await {
            Ok(n) => {
                self.fields.finish_build(b);
                self.save_index_names();
                tracing::debug!(target: "adb::index", ?memory, field, rows = n, ms = t0.elapsed().as_millis() as u64, "field index scan");
                Ok(true)
            }
            Err(e) => {
                self.fields.abandon(memory, field);
                Err(e)
            }
        }
    }

    /// The rows matching `filter`, decoded and handed to `f` a batch at a time, without
    /// the vector columns. Returns how many there were.
    async fn scan_each(&self, filter: &str, mut f: impl FnMut(Vec<StoredRow>)) -> StoreResult<usize> {
        let _t = adb_core::steps::timer("store.scan_each");
        use futures::TryStreamExt;
        let cols: Vec<String> = self.schema.fields().iter().map(|c| c.name().clone()).filter(|n| n != "embedding" && n != "sealed").collect();
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        let scanner = Self::scanner(open_state(&g)).await?.filter(filter).map_err(lance)?.project(&cols).map_err(lance)?;
        let mut stream = scanner.try_into_stream().await.map_err(lance)?;
        let mut n = 0;
        while let Some(b) = stream.try_next().await.map_err(|e| StoreError::Decode(format!("scan: {e}")))? {
            let rows = decode(&b)?;
            n += rows.len();
            f(rows);
        }
        Ok(n)
    }

    fn index_names_file(&self) -> std::path::PathBuf {
        self.root.join(format!("p{}", self.partition.0)).join("field_indexes.json")
    }

    /// Remember the built field indexes (PF-40): `[["semantic","topic"], …]`.
    fn save_index_names(&self) {
        let names: Vec<(&str, String)> = self.fields.names().into_iter().map(|(m, f)| (m.tag(), f)).collect();
        let path = self.index_names_file();
        let tmp = path.with_extension("json.tmp");
        let r = serde_json::to_vec(&names).map_err(std::io::Error::other).and_then(|b| std::fs::write(&tmp, b)).and_then(|_| std::fs::rename(&tmp, &path));
        if let Err(e) = r {
            tracing::warn!(error = %e, path = %path.display(), "could not save the field index names");
        }
    }

    fn saved_index_names(&self) -> Vec<(adb_core::log::Memory, String)> {
        let Ok(b) = std::fs::read(self.index_names_file()) else { return Vec::new() };
        let names: Vec<(String, String)> = serde_json::from_slice(&b).unwrap_or_default();
        names.into_iter().filter_map(|(m, f)| Some((adb_core::log::Memory::from_tag(&m)?, f))).collect()
    }

    /// Warm up after open, in the background: with `live_keys`, the live-key index a
    /// follower hands over when it takes the lead (PF-38); then the field indexes this
    /// partition had (PF-40), so the first filtered reads don't wait for a build.
    pub fn warm(self: &Arc<Self>, live_keys: bool) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            if live_keys {
                if let Err(e) = me.build_live_keys().await {
                    tracing::warn!(error = %e, "live keys not indexed");
                }
            }
            let names = me.saved_index_names();
            for (m, f) in &names {
                if let Err(e) = me.build_field_index(*m, f).await {
                    tracing::warn!(memory = ?m, field = %f, error = %e, "field index not built at open");
                }
            }
            tracing::info!(live_keys = me.live.is_ready(), field_indexes = names.len(), took = ?t0.elapsed(), "store warmed");
        });
    }

    /// Build the live-key index from one projected scan (PF-38).
    pub async fn build_live_keys(&self) -> StoreResult<()> {
        if !self.live.begin() {
            return Ok(());
        }
        // Every version that isn't forgotten or superseded: time bounds are checked
        // when the keys are taken.
        match self.live_keys(CommitTs(i64::MIN)).await {
            Ok(keys) => {
                self.live.finish(keys);
                Ok(())
            }
            Err(e) => {
                self.live.abandon();
                Err(e)
            }
        }
    }

    /// Whether the live-key index is built and not yet taken (PF-38).
    pub fn live_keys_ready(&self) -> bool {
        self.live.is_ready()
    }

    /// The live keys at `now` from the live-key index, once (PF-38); `None` when it
    /// isn't built — the caller scans ([`Self::live_keys`]).
    pub fn take_live_keys(&self, now: CommitTs) -> Option<Vec<crate::rows::LiveKey>> {
        self.live.take(now)
    }

    /// While `on`, a compaction under way stops between its steps (PF-38: a follower's
    /// fold ran 18 s inside a takeover on a 2-vCPU node).
    pub fn yield_compaction(&self, on: bool) {
        self.yield_compaction.store(on, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn compaction_yields(&self) -> bool {
        self.yield_compaction.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The hot cache, if one is set (W10).
    pub fn hot(&self) -> Option<Arc<HotCache>> {
        self.hot.read().unwrap().clone()
    }

    /// Replace the hot cache (`None`: no cache). The new one is filled from the
    /// table's current record rows of the memory types it keeps, with applies
    /// held off (the store's exclusive lock), so it starts at the applied offset.
    pub async fn set_cache(&self, cache: Option<HotCache>) -> StoreResult<()> {
        let g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
        let Some(cache) = cache else {
            *self.hot.write().unwrap() = None;
            return Ok(());
        };
        Self::fill_cache(&g, &cache).await?;
        *self.hot.write().unwrap() = Some(Arc::new(cache));
        drop(g);
        Ok(())
    }

    /// Fill `cache` from the table's current rows (the caller holds the store lock).
    async fn fill_cache(g: &tokio::sync::RwLockWriteGuard<'_, Option<Inner>>, cache: &HotCache) -> StoreResult<()> {
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
        adb_core::steps::value("store.apply records per batch", records.len() as u64);
        let mut at = std::time::Instant::now();
        let g = self.state.read().await;
        adb_core::steps::lap("store.apply.1 state read-lock wait", &mut at);
        let inner = open_state(&g);
        // Rows these records write, one per id (later effects see earlier ones).
        let mut out: BTreeMap<String, StoredRow> = BTreeMap::new();
        // PF-47: the table rows these records change, read together first. Read one by
        // one inside the loop (a ~5 ms Lance point read each), the applier managed ~180
        // UPDATEs / FORGETs a second and every read waited behind it.
        let mut hinted = Prefetched::new();
        {
            let mut h = self.hints.lock().unwrap();
            for r in &records {
                for row in h.remove(&r.offset.0).into_iter().flatten() {
                    hinted.insert(row.id.clone(), Some(row));
                }
            }
            if !h.is_empty() {
                h.retain(|off, _| *off > last.offset.0);
            }
        }
        adb_core::steps::value("store.apply rows hinted", hinted.len() as u64);
        let pre = Self::prefetch(inner, &records, hinted).await?;
        adb_core::steps::value("store.apply rows prefetched", pre.len() as u64);
        adb_core::steps::lap("store.apply.1b prefetch rows", &mut at);
        for record in &records {
            Self::rows_of(inner, &pre, &mut out, record).await?;
        }
        let (off, ts, epoch) = (last.offset.0, last.commit_ts, last.epoch.0);
        out.insert(META_ID.into(), StoredRow::meta(off, ts, epoch, self.meta_extra()));

        let rows: Vec<StoredRow> = out.into_values().collect();
        adb_core::steps::lap("store.apply.2 rows of records", &mut at);
        let batch = encode(&rows, &self.schema, self.dim)?;
        adb_core::steps::lap("store.apply.3 encode arrow", &mut at);
        self.apply_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let put = inner.writer.put(vec![batch]).await.map_err(lance);
        adb_core::steps::lap("store.apply.4 lance memwal put", &mut at);
        // Still under the state lock, so the cache moves with the table (W10).
        if let (Ok(_), Some(h)) = (&put, self.hot()) {
            h.apply(&rows);
        }
        self.rows.drop_rows(&rows);
        if put.is_ok() {
            self.fields.apply(&rows);
            self.live.apply(&rows);
            if let Some(l) = inner.links.lock().unwrap().as_mut() {
                for r in &rows {
                    l.add(&r.id, &r.lineage_id, r.data.subject_id.as_deref(), r.data.object_id.as_deref());
                }
            }
        }
        adb_core::steps::lap("store.apply.5 caches + indexes", &mut at);
        self.apply_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        put?;
        self.applied.store(off, std::sync::atomic::Ordering::SeqCst);
        inner.puts.lock().unwrap().offsets.push_back(off);
        self.puts_since_compaction.fetch_add(records.len() as u64, std::sync::atomic::Ordering::SeqCst);
        let st = inner.writer.memtable_stats().await.map_err(lance)?;
        adb_core::steps::lap("store.apply.6 memtable stats", &mut at);
        adb_core::steps::value("store.memtable rows", st.row_count as u64);
        adb_core::steps::value("store.memtable batches", st.batch_count as u64);
        let mem = inner.writer.memory();
        // Lance counts the buffers' contents; each batch also costs its arrays and
        // small allocations, which dominate when a put is a record or two.
        let held = mem.active_bytes() + st.batch_count * BATCH_OVERHEAD;
        // PF-52: and on batch count. A point read walks every batch of the active
        // memtable; small applies (backfill vectors, single writes) grew it to ~850
        // batches under the row and byte limits, and keyed reads took ~1 s (AWS r10).
        let seal = st.row_count >= self.memtable_rows || held >= memtable_limits().0 || st.batch_count >= memtable_batches();
        if seal || (self.apply_seq.load(std::sync::atomic::Ordering::Relaxed) / 2) % 1000 == 0 {
            tracing::debug!(
                target: "adb_store::memtable",
                partition = self.partition.0, rows = st.row_count, batches = st.batch_count, seal, held,
                active = mem.active_bytes(), frozen = mem.frozen_bytes(), "memtable"
            );
        }
        if seal {
            // Queued for flush; nothing here waits on it (the fence is dropped).
            inner.writer.force_freeze_active().await.map_err(lance)?;
            adb_core::steps::lap("store.apply.7 seal memtable", &mut at);
        }
        Ok(records.len())
    }

    /// The rows `record` writes, into `out`.
    async fn rows_of(inner: &Inner, pre: &Prefetched, out: &mut BTreeMap<String, StoredRow>, record: &LogRecord) -> StoreResult<()> {
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
                    if let Some(mut prev) = Self::row(inner, pre, &out, &row_id(new.data.memory, &old.0)).await? {
                        prev.valid_to = Some(ts);
                        prev.superseded_by = Some(new.id.0.clone());
                        prev.log_offset = off;
                        out.insert(prev.id.clone(), prev);
                    }
                    out.insert(new.id.0.clone(), StoredRow::from_new(&new, ts, off, epoch));
                }
                Effect::Patch { memory, id, fields, .. } => {
                    if let Some(mut cur) = Self::row(inner, pre, &out, &row_id(*memory, &id.0)).await? {
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
                    for mut r in Self::lineage_and_edges(inner, pre, &out, &row_id(*memory, &lineage_id.0)).await? {
                        r.tombstone = true;
                        r.log_offset = off;
                        out.insert(r.id.clone(), r);
                    }
                }
                Effect::Embed { memory, id, embedding } => {
                    if let Some(mut cur) = Self::row(inner, pre, &out, &row_id(*memory, &id.0)).await? {
                        // Not a change to the record: `log_offset` stays its version's, or a
                        // merge would see main as moved when only its vector arrived (DB-19).
                        cur.data.embedding = Some(embedding.clone());
                        out.insert(cur.id.clone(), cur);
                    }
                }
                Effect::UpdateSubCursor { sub_id, cursor } => {
                    if let Some(mut sub) = Self::row(inner, pre, &out, &sub_id.0).await? {
                        if let serde_json::Value::Object(v) = &mut sub.data.value {
                            v.insert("cursor".into(), cursor.0.into());
                        }
                        sub.log_offset = off;
                        out.insert(sub.id.clone(), sub);
                    }
                }
                Effect::Unregister { id } => {
                    if let Some(mut r) = Self::row(inner, pre, &out, &id.0).await? {
                        r.tombstone = true;
                        r.log_offset = off;
                        out.insert(r.id.clone(), r);
                    }
                }
                // A validity boundary is an event; reads test validity (AQL v0.10).
                Effect::Validity { .. } => {}
                // Platform tables live in their own datasets (adb-platform); the
                // memory table only moves its applied offset past them.
                Effect::TableCreate { .. } | Effect::TableInsert { .. } | Effect::TableUpdate { .. } | Effect::TableDelete { .. } | Effect::TableUpsert { .. } | Effect::TableReplace { .. } | Effect::TableAddColumns { .. } | Effect::TableAppendOnly { .. } | Effect::TablePurge { .. } => {}
                // FlowR's engine (stage H) keeps its own state and journal partition.
                Effect::Engine { .. } => {}
            }
        }
        Ok(())
    }

    /// A row as this record has it so far, else as stored.
    async fn row(inner: &Inner, pre: &Prefetched, out: &BTreeMap<String, StoredRow>, id: &str) -> StoreResult<Option<StoredRow>> {
        match (out.get(id), pre.get(id)) {
            (Some(r), _) => Ok(Some(r.clone())),
            (None, Some(r)) => Ok(r.clone()),
            (None, None) => Ok(Self::scan_in(inner, Some(&format!("id = {}", sql_str(id)))).await?.into_iter().next()),
        }
    }

    /// The table's rows that `records` change (superseded, patched, embedded, forgotten
    /// with their edges, …), read by primary key [`PREFETCH_AT_ONCE`] at a time (PF-47).
    /// The table doesn't change while a batch is built (the applier is its only
    /// writer), so these are the rows the per-record reads would find.
    async fn prefetch(inner: &Inner, records: &[&LogRecord], hinted: Prefetched) -> StoreResult<Prefetched> {
        use futures::StreamExt;
        let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut lineages: Vec<String> = Vec::new();
        for record in records {
            for effect in &record.effects {
                match effect {
                    Effect::Supersede { old, new } => {
                        ids.insert(row_id(new.data.memory, &old.0));
                    }
                    Effect::Patch { memory, id, .. } | Effect::Embed { memory, id, .. } => {
                        ids.insert(row_id(*memory, &id.0));
                    }
                    Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } => lineages.push(row_id(*memory, &lineage_id.0)),
                    Effect::UpdateSubCursor { sub_id, .. } => {
                        ids.insert(sub_id.0.clone());
                    }
                    Effect::Unregister { id } => {
                        ids.insert(id.0.clone());
                    }
                    _ => {}
                }
            }
        }
        if !lineages.is_empty() {
            if inner.links.lock().unwrap().is_none() {
                let built = Self::build_links(inner).await?;
                inner.links.lock().unwrap().get_or_insert(built);
            }
            let links = inner.links.lock().unwrap();
            if let Some(l) = links.as_ref() {
                for lineage in &lineages {
                    ids.extend(l.of(lineage));
                }
            }
        }
        ids.retain(|id| !hinted.contains_key(id));
        let mut found = hinted;
        if ids.len() < 2 {
            return Ok(found);
        }
        adb_core::steps::value("store.apply rows read (no hint)", ids.len() as u64);
        let mut reads = futures::stream::iter(ids.into_iter().map(|id| async move {
            let r = Self::scan_in(inner, Some(&format!("id = {}", sql_str(&id)))).await.map(|v| v.into_iter().next());
            (id, r)
        }))
        .buffer_unordered(PREFETCH_AT_ONCE);
        while let Some((id, r)) = reads.next().await {
            found.insert(id, r?);
        }
        Ok(found)
    }

    /// The links map of the table (`Inner::links`), from its id, lineage, subject and
    /// object columns only.
    async fn build_links(inner: &Inner) -> StoreResult<Links> {
        use arrow_array::{Array, StringArray};
        let t0 = std::time::Instant::now();
        let scanner = Self::scanner(inner).await?.project(&["id", "lineage_id", "subject_id", "object_id"]).map_err(lance)?;
        let b = scanner.try_into_batch().await.map_err(lance)?;
        let col = |n: &str| -> StoreResult<&StringArray> {
            b.column_by_name(n).and_then(|c| c.as_any().downcast_ref::<StringArray>()).ok_or_else(|| StoreError::Decode(format!("links: column {n}")))
        };
        let (id, lineage, subject, object) = (col("id")?, col("lineage_id")?, col("subject_id")?, col("object_id")?);
        let opt = |a: &StringArray, i: usize| (!a.is_null(i)).then(|| a.value(i).to_string());
        let mut links = Links::default();
        for i in 0..b.num_rows() {
            let (s, o) = (opt(subject, i), opt(object, i));
            links.add(id.value(i), lineage.value(i), s.as_deref(), o.as_deref());
        }
        tracing::info!(rows = b.num_rows(), ms = t0.elapsed().as_millis() as u64, "links map built");
        Ok(links)
    }

    /// Every version of a lineage, and every edge touching it (D2 §5).
    async fn lineage_and_edges(inner: &Inner, pre: &Prefetched, out: &BTreeMap<String, StoredRow>, lineage: &str) -> StoreResult<Vec<StoredRow>> {
        // PF-36: the row ids from the links map (built once per open table), then the
        // rows by primary key; the same rows the old OR-filter scan found.
        if inner.links.lock().unwrap().is_none() {
            let built = Self::build_links(inner).await?;
            inner.links.lock().unwrap().get_or_insert(built);
        }
        let ids = inner.links.lock().unwrap().as_ref().map(|l| l.of(lineage)).unwrap_or_default();
        // One primary-key read per row (an `id IN (…)` filter scans the whole table).
        let mut rows: BTreeMap<String, StoredRow> = BTreeMap::new();
        for id in &ids {
            let r = match pre.get(id.as_str()) {
                Some(r) => r.clone(),
                None => Self::scan_in(inner, Some(&format!("id = {}", sql_str(id)))).await?.into_iter().next(),
            };
            if let Some(r) = r {
                rows.insert(r.id.clone(), r);
            }
        }
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
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
        let st = open_state(&g).writer.memtable_stats().await.map_err(lance)?;
        Ok((st.durable_batch_count, st.global_offset))
    }

    /// The highest log offset this table has made durable (MemWAL WAL flushed): a
    /// crash now recovers at least up to it, so the partition log may be purged
    /// below `durable_offset + 1` (stage D3.5). Maps the writer's durable batch count
    /// to the log offset of that put, as the R11 / R12 spikes validated.
    pub async fn durable_offset(&self) -> StoreResult<LogOffset> {
        let _t = adb_core::steps::timer("store.durable_offset");
        self.durable_offset_inner().await
    }

    async fn durable_offset_inner(&self) -> StoreResult<LogOffset> {
        let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
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
        let _t = adb_core::steps::timer("store.sync_to_disk (syncfs)");
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
    compaction_row_pinned(history, now, Default::default())
}

/// [`compaction_row`] with per-namespace pins: a namespace's horizon is never later
/// than its pin, so a superseded version that a branch reads at its point stays.
fn compaction_row_pinned(history: Duration, now: CommitTs, pins: std::collections::HashMap<String, CommitTs>) -> impl Fn(StoredRow) -> Option<StoredRow> + Sync {
    compaction_row_windows(history, now, pins, Vec::new())
}

fn compaction_row_windows(history: Duration, now: CommitTs, pins: std::collections::HashMap<String, CommitTs>, windows: Vec<(String, Duration)>) -> impl Fn(StoredRow) -> Option<StoredRow> + Sync {
    let base = CommitTs(now.0 - history.as_micros() as i64);
    move |mut r: StoredRow| {
        let own = r.data.namespace.as_ref().and_then(|ns| {
            windows.iter().find(|(pattern, _)| match pattern.strip_suffix('*') {
                Some(prefix) => ns.starts_with(prefix),
                None => pattern == ns,
            })
        });
        let base = own.map(|(_, w)| CommitTs(now.0 - w.as_micros() as i64)).unwrap_or(base);
        let horizon = match r.data.namespace.as_ref().and_then(|ns| pins.get(ns)) {
            Some(p) if p.0 < base.0 => *p,
            _ => base,
        };
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

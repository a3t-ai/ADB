//! PF-16: compaction in place, at a cost that follows what changed (`.ai/WRITE_PATH.md`).
//!
//! Until 8 Nov every compaction copied the whole LSM view into a new table and swapped
//! it in, so its cost grew with the table. A fold instead:
//!
//! 1. seals the active memtable and waits for it to land as an SSTable, so every row
//!    applied so far (a forgotten payload included) is in an SSTable;
//! 2. reads the SSTables (the delta) and, from the base table, only the rows whose
//!    history window or TTL ran out (a filtered scan);
//! 3. applies the compaction rule (`keep`) to them: rows it drops are deleted from the
//!    base, the rest are upserted into it by `id` (Lance `merge_insert`);
//! 4. rewrites only the base fragments that need it: those that held a tombstoned or
//!    expired row (its bytes must leave the disk), those with over 10% deleted rows,
//!    and runs of small fragments of one size tier (size-tiered merging: a row is
//!    rewritten about once per tier, not once per compaction);
//! 5. under the store's write lock, for a few milliseconds: swaps the new base in,
//!    drops the folded SSTables from the shard manifest, deletes their directories,
//!    the WAL entries they cover and the base's old versions.
//!
//! Readers and the applier carry on throughout; only step 5 holds them. Every step is
//! safe to repeat after a crash: the SSTables stay listed until step 5, and folding a
//! row twice leaves the same base.

use std::collections::{BTreeMap, HashSet};

use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::dataset::optimize::{compact_files_with_planner, CompactionOptions, CompactionPlan, CompactionPlanner, TaskData};
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance::table::format::Fragment;
use lance_index::mem_wal::ShardManifest;

use super::*;

/// Size tiers (live rows) for merging small fragments: a run of `TIER_RUN` fragments of
/// one tier becomes one fragment of the next.
const TIERS: [usize; 3] = [8_192, 65_536, 524_288];
const TIER_RUN: usize = 8;
/// Ids per `IN (...)` filter.
const ID_CHUNK: usize = 1_000;

fn ts_lit(t: CommitTs) -> Expr {
    lit(ScalarValue::TimestampMicrosecond(Some(t.0), Some("UTC".into())))
}

fn in_list(ids: &[String]) -> Expr {
    col("id").in_list(ids.iter().map(|i| lit(i.as_str())).collect(), false)
}

/// A plain Lance scan returns JSONB columns (`value`) as Arrow JSON text; the rows
/// codec reads Lance's binary form (as the LSM scanner returns it).
pub(super) fn jsonb(b: &arrow_array::RecordBatch) -> StoreResult<arrow_array::RecordBatch> {
    lance_arrow_14::json::convert_json_columns(b).map_err(lance)
}

fn tier(live: usize) -> usize {
    TIERS.iter().position(|t| live < *t).unwrap_or(TIERS.len())
}

/// Fragments to rewrite, in tasks: `erase` ones alone (or within a merge), runs of
/// small fragments of one tier together, and fragments with many deletions alone.
fn plan_rewrites(frags: &[Fragment], erase: &HashSet<u64>) -> Vec<Vec<Fragment>> {
    let live = |f: &Fragment| {
        let rows = f.physical_rows.unwrap_or(0);
        let deleted = f.deletion_file.as_ref().and_then(|d| d.num_deleted_rows).unwrap_or(0);
        (rows, deleted, rows.saturating_sub(deleted))
    };
    let mut tasks: Vec<Vec<Fragment>> = Vec::new();
    let mut taken: HashSet<u64> = HashSet::new();
    // Runs of adjacent fragments in one small tier.
    let mut i = 0;
    while i < frags.len() {
        let t = tier(live(&frags[i]).2);
        let mut j = i;
        while j < frags.len() && tier(live(&frags[j]).2) == t {
            j += 1;
        }
        if t < TIERS.len() && j - i >= TIER_RUN {
            tasks.push(frags[i..j].to_vec());
            taken.extend(frags[i..j].iter().map(|f| f.id));
        }
        i = j;
    }
    for f in frags {
        if taken.contains(&f.id) {
            continue;
        }
        let (rows, deleted, _) = live(f);
        if erase.contains(&f.id) || (rows > 0 && deleted * 10 > rows) {
            tasks.push(vec![f.clone()]);
        }
    }
    tasks
}

struct Planner {
    tasks: Vec<Vec<Fragment>>,
}

#[async_trait::async_trait]
impl CompactionPlanner for Planner {
    async fn plan(&self, ds: &Dataset) -> lance::Result<CompactionPlan> {
        let options = CompactionOptions { materialize_deletions_threshold: 0.0, num_threads: Some(1), ..Default::default() };
        Ok(CompactionPlan { tasks: self.tasks.iter().map(|f| TaskData { fragments: f.clone() }).collect(), read_version: ds.version().version, options })
    }
}

/// What one fold did, for its log line.
#[derive(Debug, Default)]
struct FoldCounts {
    sstables: usize,
    delta: usize,
    expired: usize,
    upserted: usize,
    deleted: usize,
    rewritten: usize,
    wal_entries: usize,
}

impl PartitionStore {
    pub(super) fn table_dir(&self) -> std::path::PathBuf {
        self.root.join(format!("p{}", self.partition.0)).join("memory.lance")
    }

    /// Fold the MemWAL's SSTables (after sealing the active memtable) into the base
    /// table with `keep` applied, and drop the base rows whose window ran out: base rows
    /// that are not current anymore are found by `superseded_by IS NOT NULL AND
    /// valid_to < horizon` (the latest horizon any namespace uses) or `ttl_at <= now`,
    /// and `keep` decides each. See the module docs.
    pub(super) async fn fold(&self, keep: &(dyn Fn(StoredRow) -> Option<StoredRow> + Sync), now: CommitTs, horizon: CommitTs) -> StoreResult<CompactStats> {
        let _t = adb_core::steps::timer("store.fold compaction (all)");
        let _one = self.folding.lock().await;
        // Only before step 3: from there on the base table is being rewritten.
        if self.compaction_yields() {
            return Err(StoreError::Decode("compaction gave way to a takeover; it runs again later".into()));
        }
        let started = std::time::Instant::now();
        // No archive here (S3 option B): object storage gets large files at the
        // archive timer's checkpoints; the cleanup in step 5 keeps every version since
        // the last one (`cleanup_local_versions`).
        let dir = self.table_dir();
        let bytes_before = dir_size(&dir);
        let mut n = FoldCounts::default();

        // 1. Everything applied so far into SSTables.
        let fence = {
            let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
            open_state(&g).writer.force_freeze_active().await.map_err(lance)?
        };
        fence.wait().await.map_err(lance)?;

        // 2. The delta (newest version per id, generation order), and the base snapshot.
        let (base, shard, sstables) = {
            let g = { let _w = adb_core::steps::timer("store.state read-lock wait"); self.state.read().await };
            let inner = open_state(&g);
            let m = inner.writer.manifest().await.map_err(lance)?;
            (inner.base.clone(), inner.writer.shard_id(), m.map(|m| m.sstables).unwrap_or_default())
        };
        let mut sstables = sstables;
        sstables.sort_by_key(|t| t.generation);
        n.sstables = sstables.len();
        let base_uri = dir.to_string_lossy().to_string();
        let mut delta: BTreeMap<String, StoredRow> = BTreeMap::new();
        for t in &sstables {
            let uri = format!("{}/_mem_wal/{}/{}", base_uri.trim_end_matches('/'), shard.as_hyphenated(), t.path);
            let ds = Dataset::open(&uri).await.map_err(lance)?;
            let mut stream = ds.scan().try_into_stream().await.map_err(lance)?;
            while let Some(b) = stream.try_next().await.map_err(lance)? {
                for r in decode(&jsonb(&b)?)? {
                    delta.insert(r.id.clone(), r);
                }
            }
        }
        n.delta = delta.len();
        if self.compaction_yields() {
            return Err(StoreError::Decode("compaction gave way to a takeover; it runs again later".into()));
        }

        let mut upserts: Vec<StoredRow> = Vec::new();
        let mut deletes: Vec<String> = Vec::new();
        // Ids whose bytes must leave the disk: their fragments are rewritten.
        let mut erase: Vec<String> = Vec::new();
        // The ids the SSTables decide: the base scan below leaves them alone.
        let decided: HashSet<String> = delta.keys().cloned().collect();
        for (id, r) in delta {
            if r.tombstone || r.data.ttl_at.is_some_and(|t| t <= now) {
                erase.push(id.clone());
            }
            match keep(r) {
                Some(k) => upserts.push(k),
                None => deletes.push(id),
            }
        }

        // Base rows whose window or TTL ran out.
        {
            let expired = col("ttl_at").lt_eq(ts_lit(now)).or(col("superseded_by").is_not_null().and(col("valid_to").lt(ts_lit(horizon))));
            let mut scan = base.scan();
            scan.filter_expr(expired);
            let mut stream = scan.try_into_stream().await.map_err(lance)?;
            while let Some(b) = stream.try_next().await.map_err(lance)? {
                for r in decode(&jsonb(&b)?)? {
                    if r.id == META_ID || decided.contains(&r.id) {
                        continue;
                    }
                    let ttl = r.data.ttl_at.is_some_and(|t| t <= now);
                    let id = r.id.clone();
                    if keep(r).is_none() {
                        n.expired += 1;
                        if ttl {
                            erase.push(id.clone());
                        }
                        deletes.push(id);
                    }
                }
            }
        }

        if self.compaction_yields() {
            return Err(StoreError::Decode("compaction gave way to a takeover; it runs again later".into()));
        }
        // 3. Into the base, without any lock: readers keep the old snapshot meanwhile.
        let mut ds = (*base).clone();
        ds.checkout_latest().await.map_err(lance)?;
        let mut erase_frags: HashSet<u64> = HashSet::new();
        for chunk in erase.chunks(ID_CHUNK) {
            let mut scan = ds.scan();
            scan.filter_expr(in_list(chunk)).with_row_address().project(&["id"]).map_err(lance)?;
            let batch = scan.try_into_batch().await.map_err(lance)?;
            if let Some(a) = batch.column_by_name("_rowaddr").and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>()) {
                erase_frags.extend(a.values().iter().map(|v| v >> 32));
            }
        }
        for chunk in deletes.chunks(ID_CHUNK) {
            let list = chunk.iter().map(|i| sql_str(i)).collect::<Vec<_>>().join(",");
            ds.delete(&format!("id IN ({list})")).await.map_err(lance)?;
        }
        n.deleted = deletes.len();
        if !upserts.is_empty() {
            n.upserted = upserts.len();
            let batches = upserts.chunks(REWRITE_CHUNK).map(|c| encode(c, &self.schema, self.dim)).collect::<StoreResult<Vec<_>>>()?;
            let mut b = MergeInsertBuilder::try_new(Arc::new(ds), vec!["id".into()]).map_err(lance)?;
            b.when_matched(WhenMatched::UpdateAll).when_not_matched(WhenNotMatched::InsertAll);
            let (merged, _) = b
                .try_build()
                .map_err(lance)?
                .execute_reader(RecordBatchIterator::new(batches.into_iter().map(Ok), self.schema.clone()))
                .await
                .map_err(lance)?;
            ds = (*merged).clone();
        }
        // New fragments into the indexes first: Lance rewrites no group that mixes
        // indexed and unindexed fragments (its remap would lose rows).
        if n.upserted > 0 {
            // The table's own indexes only: Lance's MemWAL system index has no fields.
            let opts = lance_index::optimize::OptimizeOptions::new().index_names(vec!["id_idx".into(), "lineage_idx".into()]);
            ds.optimize_indices(&opts).await.map_err(lance)?;
        }
        let tasks = plan_rewrites(ds.fragments(), &erase_frags);
        n.rewritten = tasks.iter().map(|t| t.len()).sum();
        if !tasks.is_empty() {
            compact_files_with_planner(&mut ds, None, &Planner { tasks }).await.map_err(lance)?;
        }
        let err = |e: std::io::Error| StoreError::Decode(format!("compaction: {e}"));
        adb_core::disk::sync_fs_async(&dir).await.map_err(err)?;

        // 4. The swap and the clean-up, with readers and the applier held.
        let folded: HashSet<u64> = sstables.iter().map(|t| t.generation).collect();
        let top = folded.iter().copied().max();
        let mut g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
        let inner = g.as_mut().expect("the partition store is open");
        inner.base = Arc::new(ds);
        inner.cache = Arc::new(SsTableCache::new(64));
        let epoch = inner.writer.epoch();
        let m = inner
            .writer
            .manifest_store()
            .commit_update(epoch, |c| ShardManifest {
                version: c.next_version(),
                sstables: c.sstables.iter().filter(|t| !folded.contains(&t.generation)).cloned().collect(),
                ..c.clone()
            })
            .await
            .map_err(lance)?;
        // Without object storage every old version; with it, only those the mirror holds.
        let removed = self.cleanup_local_versions(&inner.base).await?;
        let shard_dir = dir.join("_mem_wal").join(shard.as_hyphenated().to_string());
        let listed: HashSet<u64> = m.sstables.iter().map(|t| t.generation).collect();
        // SSTable directories: the folded ones, and any a crash left unlisted below them.
        if let (Some(top), Ok(entries)) = (top, std::fs::read_dir(&shard_dir)) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                let Some(gen) = name.rsplit_once("_gen_").and_then(|(_, g)| g.parse::<u64>().ok()) else { continue };
                if gen <= top && !listed.contains(&gen) {
                    std::fs::remove_dir_all(e.path()).map_err(err)?;
                }
            }
        }
        // WAL entries the flushed SSTables cover (replay starts after them).
        if let Ok(entries) = std::fs::read_dir(shard_dir.join("wal")) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if lance::dataset::mem_wal::util::parse_bit_reversed_filename(&name).is_some_and(|p| p <= m.replay_after_wal_entry_position) {
                    std::fs::remove_file(e.path()).map_err(err)?;
                    n.wal_entries += 1;
                }
            }
        }
        let total = inner.base.count_rows(None).await.map_err(lance)?;
        drop(g);
        adb_core::disk::sync_fs_async(&dir).await.map_err(err)?;
        tracing::info!(
            sstables = n.sstables, delta = n.delta, expired = n.expired, upserted = n.upserted, deleted = n.deleted,
            fragments_rewritten = n.rewritten, wal_entries_removed = n.wal_entries, old_versions = removed.old_versions,
            rows = total, took_ms = started.elapsed().as_millis() as u64, "fold compaction"
        );
        self.puts_since_compaction.store(0, std::sync::atomic::Ordering::SeqCst);
        self.rows_at_compaction.store(total as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(CompactStats { total: n.delta + n.expired, kept: total, bytes_before, bytes_after: dir_size(&dir), took: started.elapsed() })
    }
}

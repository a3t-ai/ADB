//! Object storage behind the local table (S3 option B, `.ai/S3_OPTION_B_PLAN.md`).
//!
//! With [`StoreConfig::cold`] set, a partition's table keeps its root (manifests
//! and hot fragments) on local disk and has a second Lance base on object storage.
//! Each table lineage gets its own prefix:
//!
//! ```text
//! {cold}/p{partition}/{lineage}/cold/data/…   fragments tiered out of the root
//! {cold}/p{partition}/{lineage}/mirror/…      copy of the root, opens as the table
//! ```
//!
//! - **Lineage.** A full rewrite (migration, model change) builds a new table whose
//!   versions restart at 1. Its own lineage id keeps its archive apart from the old
//!   one's, which is kept.
//! - **Archive = checkpoint, large files only.** S3 objects can't be appended to,
//!   so new data starts as small local files (folds every few seconds) and only
//!   reaches object storage in large ones. [`PartitionStore::archive`] first merges
//!   the fragments object storage doesn't hold yet into one file, then copies the
//!   files the table's current version uses and the mirror lacks: data, deletions,
//!   indexes and the transaction, then that version's manifest, then the hint.
//!   Versions made between two archives never reach object storage. The mirror is a
//!   complete table at each archived version.
//! - **When.** [`ArchivePolicy`]: once the new data reaches `ADB_ARCHIVE_MIN_BYTES`
//!   (default 512 MiB), or `ADB_ARCHIVE_MAX_AGE_SECS` (default 1800) after the last
//!   archive if anything changed. Until then the log (three copies) holds it.
//! - **Watermark.** The `_meta` offset of the newest archived version: everything
//!   at or below it can be rebuilt from object storage alone, so the log may be
//!   trimmed to it.
//! - **Nothing is deleted** from object storage. Local cleanup removes only versions
//!   older than the archived one, whose files the mirror already holds.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use lance::dataset::optimize::{compact_files_with_planner, CompactionOptions, CompactionPlan, CompactionPlanner, TaskData};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::table::format::{BasePath, Fragment};

use super::*;

/// Inside the table directory: the id of this table's lineage (Lance ignores
/// files it does not manage).
const LINEAGE_FILE: &str = "_adb_lineage";
/// The cold base's id in `Manifest.base_paths`.
pub const COLD_BASE_ID: u32 = 1;

/// Where a lineage lives on object storage.
fn lineage_uri(cold: &str, partition: PartitionId, lineage: &str) -> String {
    format!("{}/p{}/{}", cold.trim_end_matches('/'), partition.0, lineage)
}

/// Write parameters for a new table (`Create` mode), with the cold base registered
/// when object storage is configured, and the lineage id to record once the table
/// exists ([`record_lineage`]).
pub(crate) fn new_table_params(cfg: &StoreConfig, partition: PartitionId) -> (WriteParams, Option<String>) {
    let Some(cold) = cfg.cold.as_deref() else {
        return (WriteParams { mode: WriteMode::Create, ..Default::default() }, None);
    };
    let lineage = uuid::Uuid::new_v4().simple().to_string();
    let params = WriteParams {
        mode: WriteMode::Create,
        initial_bases: Some(vec![BasePath {
            id: COLD_BASE_ID,
            name: Some("cold".to_string()),
            path: format!("{}/cold", lineage_uri(cold, partition, &lineage)),
            is_dataset_root: true,
        }]),
        ..Default::default()
    };
    (params, Some(lineage))
}

/// Record a new table's lineage in its directory.
pub(crate) fn record_lineage(dir: &Path, lineage: Option<&str>) -> StoreResult<()> {
    if let Some(l) = lineage {
        std::fs::write(dir.join(LINEAGE_FILE), l).map_err(|e| StoreError::Decode(format!("recording the table lineage: {e}")))?;
    }
    Ok(())
}

pub(crate) fn lineage_of(dir: &Path) -> Option<String> {
    std::fs::read_to_string(dir.join(LINEAGE_FILE)).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// When an archive is due (S3 option B: large files only; tuning variables).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchivePolicy {
    /// New data (bytes not on object storage yet) that makes an archive due at once,
    /// and the size the merge aims at: `ADB_ARCHIVE_MIN_BYTES`, default 512 MiB.
    pub min_bytes: u64,
    /// Time after the last archive at which anything new is archived, however
    /// little: `ADB_ARCHIVE_MAX_AGE_SECS`, default 1800 (30 min).
    pub max_age: std::time::Duration,
}

impl Default for ArchivePolicy {
    fn default() -> Self {
        Self { min_bytes: 512 << 20, max_age: std::time::Duration::from_secs(1800) }
    }
}

impl ArchivePolicy {
    /// From `ADB_ARCHIVE_MIN_BYTES` and `ADB_ARCHIVE_MAX_AGE_SECS`, else the defaults.
    pub fn from_env() -> Self {
        let d = Self::default();
        let num = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
        Self {
            min_bytes: num("ADB_ARCHIVE_MIN_BYTES").filter(|&b| b > 0).unwrap_or(d.min_bytes),
            max_age: num("ADB_ARCHIVE_MAX_AGE_SECS").map(std::time::Duration::from_secs).unwrap_or(d.max_age),
        }
    }

    /// Due: `new_bytes` reached the size, or the age passed with anything new.
    pub fn due(&self, new_bytes: u64, anything_new: bool, since_last: std::time::Duration) -> bool {
        new_bytes >= self.min_bytes || (anything_new && since_last >= self.max_age)
    }
}

/// What the table holds that object storage doesn't yet ([`PartitionStore::archive_pending`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArchivePending {
    /// Bytes of local fragments not archived.
    pub bytes: u64,
    /// The table has versions newer than the archived one.
    pub changed: bool,
    /// Time since the last archive (since the store opened, before any).
    pub since_last: std::time::Duration,
}

/// What the archive holds, kept beside the table (`memory.archived.{lineage}`).
#[derive(Debug, Default, Clone)]
pub struct ArchiveState {
    /// Root-relative paths already on the mirror.
    pub(crate) done: HashSet<String>,
    /// The newest table version whose files are all on the mirror.
    pub version: u64,
    /// That version's `_meta` offset: the log may be trimmed to it.
    pub watermark: u64,
    /// When the last archive ran (unix seconds; 0: none recorded).
    pub at: i64,
}

/// What one [`PartitionStore::tier`] moved to the cold base.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TierStats {
    pub fragments: usize,
    pub rows: usize,
    /// Local bytes those fragments held (they leave the disk at the next guarded cleanup).
    pub bytes: u64,
    /// Local bytes of the fragments that stay.
    pub local_after: u64,
}

/// Compacts exactly the given fragments into one file per `max_bytes` (into the
/// cold base with `target_bases`, else the root): the tier job, and the merge of
/// new fragments before an archive.
struct Merge {
    fragments: Vec<Fragment>,
    target_bases: Option<Vec<u32>>,
    max_bytes: Option<u64>,
}

#[async_trait::async_trait]
impl CompactionPlanner for Merge {
    async fn plan(&self, ds: &Dataset) -> lance::Result<CompactionPlan> {
        let mut options = CompactionOptions {
            target_bases: self.target_bases.clone(),
            materialize_deletions_threshold: 0.0,
            num_threads: Some(1),
            ..Default::default()
        };
        if let Some(b) = self.max_bytes {
            // One file per `max_bytes`, whatever the row count.
            options.target_rows_per_fragment = usize::MAX / 2;
            options.max_bytes_per_file = Some(b as usize);
        }
        Ok(CompactionPlan { tasks: vec![TaskData { fragments: self.fragments.clone() }], read_version: ds.version().version, options })
    }
}

/// Merge `fragments` of `ds` into large files in its root (one per `max_bytes`).
/// Rows are kept as they are (deleted rows' dead copies dropped); the old fragments
/// stay referenced by older versions until local cleanup.
pub(crate) async fn merge_fragments(ds: &mut Dataset, fragments: Vec<Fragment>, max_bytes: u64) -> StoreResult<()> {
    compact_files_with_planner(ds, None, &Merge { fragments, target_bases: None, max_bytes: Some(max_bytes) }).await.map_err(lance)?;
    Ok(())
}

/// Root-relative paths of the data files of `f` in the table root (not another base).
fn root_data_files(f: &Fragment) -> impl Iterator<Item = String> + '_ {
    f.files.iter().filter(|d| d.base_id.is_none()).map(|d| format!("data/{}", d.path))
}

/// The table's fragments wholly in the root of which object storage holds no file
/// yet, with their bytes, oldest first.
fn new_fragments(dir: &Path, ds: &Dataset, done: &dyn Fn(&str) -> bool) -> Vec<(Fragment, u64)> {
    let mut out: Vec<(Fragment, u64)> = ds
        .manifest
        .fragments
        .iter()
        .filter(|f| f.files.iter().all(|d| d.base_id.is_none()) && root_data_files(f).all(|r| !done(&r)))
        .map(|f| (f.clone(), root_data_files(f).map(|r| std::fs::metadata(dir.join(&r)).map(|m| m.len()).unwrap_or(0)).sum()))
        .collect();
    out.sort_by_key(|(f, _)| f.id);
    out
}

/// Root-relative paths of every local file version `ds` uses: its data and deletion
/// files, its indexes, its transaction and its manifest. What an archive copies.
async fn version_files(dir: &Path, ds: &Dataset) -> StoreResult<Vec<String>> {
    let mut out = Vec::new();
    for f in ds.manifest.fragments.iter() {
        out.extend(root_data_files(f));
        if let Some(d) = f.deletion_file.as_ref().filter(|d| d.base_id.is_none()) {
            out.push(format!("_deletions/{}-{}-{}.{}", f.id, d.read_version, d.id, d.file_type.suffix()));
        }
    }
    for idx in ds.load_indices().await.map_err(lance)?.iter().filter(|i| i.base_id.is_none()) {
        let rel = format!("_indices/{}", idx.uuid);
        let mut stack = vec![dir.join(&rel)];
        while let Some(d) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&d) else { continue };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(r) = p.strip_prefix(dir) {
                    out.push(r.to_string_lossy().to_string());
                }
            }
        }
    }
    if let Some(t) = &ds.manifest.transaction_file {
        out.push(format!("_transactions/{t}"));
    }
    if let Some(name) = ds.manifest_location().path.filename() {
        out.push(format!("_versions/{name}"));
    }
    out.push("_versions/latest_version_hint.json".to_string());
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// What one [`PartitionStore::archive`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveStats {
    pub files: usize,
    pub bytes: u64,
    pub version: u64,
    pub watermark: u64,
}

fn state_file(dir: &Path, lineage: &str) -> PathBuf {
    dir.with_file_name(format!("memory.archived.{lineage}"))
}

pub(crate) fn load_archive_state(dir: &Path) -> ArchiveState {
    let Some(lineage) = lineage_of(dir) else { return ArchiveState::default() };
    let Ok(text) = std::fs::read_to_string(state_file(dir, &lineage)) else { return ArchiveState::default() };
    let mut st = ArchiveState::default();
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("#version ") {
            st.version = v.parse().unwrap_or(0);
        } else if let Some(w) = line.strip_prefix("#watermark ") {
            st.watermark = w.parse().unwrap_or(0);
        } else if let Some(a) = line.strip_prefix("#at ") {
            st.at = a.parse().unwrap_or(0);
        } else if !line.is_empty() {
            st.done.insert(line.to_string());
        }
    }
    st
}

fn save_archive_state(dir: &Path, lineage: &str, st: &ArchiveState) -> StoreResult<()> {
    let mut lines: Vec<&str> = st.done.iter().map(String::as_str).collect();
    lines.sort_unstable();
    let text = format!("#version {}\n#watermark {}\n#at {}\n{}\n", st.version, st.watermark, st.at, lines.join("\n"));
    let path = state_file(dir, lineage);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).and_then(|_| std::fs::rename(&tmp, &path)).map_err(|e| StoreError::Decode(format!("saving the archive state: {e}")))
}

/// Archive order: data and side files, then manifests, then the hint.
fn archive_rank(rel: &str) -> u8 {
    if rel.ends_with("latest_version_hint.json") {
        2
    } else if rel.starts_with("_versions/") {
        1
    } else {
        0
    }
}

/// The `_meta` offset a table version holds.
pub(crate) async fn meta_offset(ds: &Dataset) -> StoreResult<u64> {
    let mut scan = ds.scan();
    scan.filter(&format!("id = '{}'", crate::schema::META_ID)).map_err(lance)?;
    let batch = scan.try_into_batch().await.map_err(lance)?;
    Ok(decode(&fold::jsonb(&batch)?)?.into_iter().filter_map(|r| r.data.value.get("applied_offset").and_then(|v| v.as_u64())).max().unwrap_or(0))
}

/// Copy the files `base`'s version uses that the mirror lacks to object storage
/// (data and side files, then its manifest, then the hint): a checkpoint;
/// returns what was done and the new state (saved beside the table).
pub(crate) async fn archive_table(cfg: &StoreConfig, partition: PartitionId, dir: &Path, base: &Dataset, mut st: ArchiveState) -> StoreResult<(ArchiveStats, ArchiveState)> {
    let Some(cold) = cfg.cold.as_deref() else { return Ok((ArchiveStats::default(), st)) };
    let Some(lineage) = lineage_of(dir) else {
        return Err(StoreError::Decode(format!("{} has no lineage: it was made before object storage was configured", dir.display())));
    };
    // The version to archive, and its offset, before listing: files of newer
    // versions may be copied too, which is harmless.
    let version = base.version().version;
    let watermark = meta_offset(base).await?;
    let mut todo: Vec<String> = version_files(dir, base).await?.into_iter().filter(|f| !st.done.contains(f) || f.ends_with("latest_version_hint.json")).collect();
    todo.sort_by_key(|f| (archive_rank(f), f.clone()));
    let mirror = format!("{}/mirror", lineage_uri(cold, partition, &lineage));
    let (store, base_path) = lance::io::ObjectStore::from_uri(&mirror).await.map_err(lance)?;
    let mut stats = ArchiveStats::default();
    for rel in todo {
        let bytes = match std::fs::read(dir.join(&rel)) {
            Ok(b) => b,
            // Removed since listing (cleanup of an older version): nothing to copy.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(StoreError::Decode(format!("reading {rel} to archive: {e}"))),
        };
        // One segment at a time: `join` would encode a '/' inside one.
        let dest = rel.split('/').fold(base_path.clone(), |p, part| p.join(part));
        store.put(&dest, &bytes).await.map_err(lance)?;
        stats.files += 1;
        stats.bytes += bytes.len() as u64;
        st.done.insert(rel);
    }
    st.version = st.version.max(version);
    st.watermark = st.watermark.max(watermark);
    st.at = chrono::Utc::now().timestamp();
    save_archive_state(dir, &lineage, &st)?;
    stats.version = st.version;
    stats.watermark = st.watermark;
    Ok((stats, st))
}

/// Archive the table in `dir` as it is on disk (no store open): before a rewrite
/// at open replaces it (`rewrite_dim`).
pub(crate) async fn archive_dir(cfg: &StoreConfig, partition: PartitionId, dir: &Path) -> StoreResult<()> {
    if cfg.cold.is_none() || lineage_of(dir).is_none() {
        return Ok(());
    }
    let base = Dataset::open(&dir.to_string_lossy()).await.map_err(lance)?;
    archive_table(cfg, partition, dir, &base, load_archive_state(dir)).await.map(|_| ())
}

/// What [`archive_tree`] has copied, kept in the tree (`_adb_archived`).
const TREE_STATE: &str = "_adb_archived";

/// Archive a directory tree that is not a partition table — the platform tables,
/// the engine's spaces — under `{cold}/{kind}/{lineage}/`: every file not copied yet
/// (data and side files, then manifests), then `last`, the contents captured by the
/// caller of files rewritten in place (`_applied.json`, `snapshot.json`), so the
/// archive matches the offsets they name. `skip` leaves out working state the log
/// rebuilds (a MemWAL). The lineage is the tree's own (`_adb_lineage`, made on first
/// use): another node's tree never shares its prefix. Nothing is deleted.
///
/// A Lance table in the tree (a directory with `_versions`) is archived as a
/// checkpoint: only the files of one version — `pins[<table dir>]`, else its latest —
/// like [`PartitionStore::archive`]; its other files never reach object storage.
pub async fn archive_tree(
    cold: &str,
    kind: &str,
    dir: &Path,
    skip: &(dyn Fn(&str) -> bool + Sync),
    last: &[(String, Vec<u8>)],
    pins: &std::collections::HashMap<String, u64>,
) -> StoreResult<(String, ArchiveStats)> {
    let lineage = match lineage_of(dir) {
        Some(l) => l,
        None => {
            let l = uuid::Uuid::new_v4().simple().to_string();
            std::fs::create_dir_all(dir).map_err(|e| StoreError::Decode(format!("archive tree: {e}")))?;
            record_lineage(dir, Some(&l))?;
            l
        }
    };
    let state_path = dir.join(TREE_STATE);
    let mut done: HashSet<String> = std::fs::read_to_string(&state_path).map(|t| t.lines().map(String::from).collect()).unwrap_or_default();
    let last_names: HashSet<&str> = last.iter().map(|(n, _)| n.as_str()).collect();
    let mut todo: Vec<String> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let rel_of = |p: &Path| p.strip_prefix(dir).map(|r| r.to_string_lossy().to_string()).unwrap_or_default();
        if d != dir && d.join("_versions").is_dir() {
            // A Lance table: the files of the pinned (else latest) version only.
            let table = rel_of(&d);
            if skip(&table) {
                continue;
            }
            let mut ds = Dataset::open(&d.to_string_lossy()).await.map_err(lance)?;
            if let Some(&v) = pins.get(&table) {
                if v != ds.version().version {
                    ds = ds.checkout_version(v).await.map_err(lance)?;
                }
            }
            for f in version_files(&d, &ds).await? {
                let rel = format!("{table}/{f}");
                if !skip(&rel) && (!done.contains(&rel) || f.ends_with("latest_version_hint.json")) {
                    todo.push(rel);
                }
            }
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&d) else { continue };
        for e in entries.flatten() {
            let p = e.path();
            let rel = rel_of(&p);
            let name = p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
            if rel.is_empty() || skip(&rel) || rel == LINEAGE_FILE || rel == TREE_STATE || name == "LOCK" || name.ends_with(".tmp") || last_names.contains(rel.as_str()) {
                continue;
            }
            if p.is_dir() {
                stack.push(p);
            } else if !done.contains(&rel) {
                todo.push(rel);
            }
        }
    }
    todo.sort_by_key(|f| (archive_rank(f.rsplit_once(".lance/").map(|(_, r)| r).unwrap_or(f)), f.clone()));
    let uri = format!("{}/{}/{}", cold.trim_end_matches('/'), kind, lineage);
    let (store, base) = lance::io::ObjectStore::from_uri(&uri).await.map_err(lance)?;
    let dest = |rel: &str| rel.split('/').fold(base.clone(), |p, part| p.join(part));
    let mut stats = ArchiveStats::default();
    for rel in todo {
        let bytes = match std::fs::read(dir.join(&rel)) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(StoreError::Decode(format!("reading {rel} to archive: {e}"))),
        };
        store.put(&dest(&rel), &bytes).await.map_err(lance)?;
        stats.files += 1;
        stats.bytes += bytes.len() as u64;
        done.insert(rel);
    }
    for (rel, bytes) in last {
        store.put(&dest(rel), bytes).await.map_err(lance)?;
        stats.files += 1;
        stats.bytes += bytes.len() as u64;
    }
    let mut lines: Vec<&str> = done.iter().map(String::as_str).collect();
    lines.sort_unstable();
    std::fs::write(&state_path, lines.join("\n")).map_err(|e| StoreError::Decode(format!("saving the archive state: {e}")))?;
    Ok((lineage, stats))
}

/// Whether the tree in `dir` has archived `rel` (a path under `dir`), from its
/// archive state: for merging only what object storage lacks before an archive.
pub fn tree_archived(dir: &Path) -> HashSet<String> {
    std::fs::read_to_string(dir.join(TREE_STATE)).map(|t| t.lines().map(String::from).collect()).unwrap_or_default()
}

/// The `_meta` offset of the partition table in `table_dir` (0 when there is none).
pub async fn table_meta_offset(table_dir: &Path) -> StoreResult<u64> {
    match Dataset::open(&table_dir.to_string_lossy()).await {
        Ok(ds) => meta_offset(&ds).await,
        Err(_) => Ok(0),
    }
}

impl PartitionStore {
    /// Archive a checkpoint now: merge the fragments object storage lacks into large
    /// files (`policy.min_bytes` each), then copy the files the newest version uses
    /// that the mirror lacks; that version is then archived and its `_meta` offset is
    /// the watermark. A no-op without [`StoreConfig::cold`]. Safe to repeat. The
    /// timer calls it when [`ArchivePolicy::due`]; a rewrite before replacing the table.
    pub async fn archive(&self) -> StoreResult<ArchiveStats> {
        if self.cfg.cold.is_none() {
            return Ok(ArchiveStats::default());
        }
        // The base table changes only under this lock (fold, rewrite, tier, merge).
        let _one = self.folding.lock().await;
        self.archive_locked(ArchivePolicy::from_env()).await
    }

    /// [`Self::archive`] with the folding lock held.
    pub(super) async fn archive_locked(&self, policy: ArchivePolicy) -> StoreResult<ArchiveStats> {
        if self.cfg.cold.is_none() {
            return Ok(ArchiveStats::default());
        }
        let _t = adb_core::steps::timer("store.archive");
        let _one = self.archiving.lock().await;
        let dir = self.table_dir();
        let base = { let g = self.state.read().await; open_state(&g).base.clone() };
        let st = self.archive_state.lock().unwrap().clone();
        // Small new fragments into large files first: object storage gets those only.
        let fresh = new_fragments(&dir, &base, &|r| st.done.contains(r));
        let base = if fresh.len() > 1 {
            let mut ds = (*base).clone();
            merge_fragments(&mut ds, fresh.into_iter().map(|(f, _)| f).collect(), policy.min_bytes).await?;
            let ds = Arc::new(ds);
            let mut g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
            g.as_mut().expect("the partition store is open").base = Arc::clone(&ds);
            ds
        } else {
            base
        };
        let (stats, st) = archive_table(&self.cfg, self.partition, &dir, &base, st).await?;
        *self.archive_state.lock().unwrap() = st;
        // The versions before the checkpoint leave the disk, and with them the small
        // fragments the merge replaced (they never reach object storage).
        self.cleanup_local_versions(&base).await?;
        Ok(stats)
    }

    /// What the table holds that object storage doesn't yet: for [`ArchivePolicy::due`].
    pub async fn archive_pending(&self) -> ArchivePending {
        let base = { let g = self.state.read().await; open_state(&g).base.clone() };
        let st = self.archive_state.lock().unwrap().clone();
        let bytes = new_fragments(&self.table_dir(), &base, &|r| st.done.contains(r)).iter().map(|(_, b)| b).sum();
        let since = if st.at > 0 { chrono::Utc::now().timestamp() - st.at } else { self.opened_at.elapsed().as_secs() as i64 };
        ArchivePending { bytes, changed: base.version().version > st.version, since_last: std::time::Duration::from_secs(since.max(0) as u64) }
    }

    /// The archived point, as the checkpoint point records it: `{lineage, version, watermark}`
    /// (`None` before the first archive).
    pub fn archived_current(&self) -> Option<serde_json::Value> {
        let st = self.archive_state.lock().unwrap().clone();
        let lineage = lineage_of(&self.table_dir())?;
        (st.version > 0).then(|| serde_json::json!({ "lineage": lineage, "version": st.version, "watermark": st.watermark }))
    }

    /// Move the oldest local fragments into the cold base until the local ones hold
    /// at most `keep_local_bytes` (the newest fragment always stays local). Their
    /// local files leave the disk at the next guarded cleanup, once archived. A no-op
    /// without [`StoreConfig::cold`].
    pub async fn tier(&self, keep_local_bytes: u64) -> StoreResult<TierStats> {
        if self.cfg.cold.is_none() {
            return Ok(TierStats::default());
        }
        let _t = adb_core::steps::timer("store.tier");
        // The base table changes only under this lock (fold, rewrite, tier).
        let _one = self.folding.lock().await;
        let dir = self.table_dir();
        let base = { let g = self.state.read().await; open_state(&g).base.clone() };
        let size = |f: &Fragment| -> u64 {
            f.files.iter().map(|d| std::fs::metadata(dir.join("data").join(&d.path)).map(|m| m.len()).unwrap_or(0)).sum()
        };
        // Local fragments, oldest first (fragment ids grow with time).
        let mut local: Vec<(Fragment, u64)> =
            base.manifest.fragments.iter().filter(|f| f.files.iter().all(|d| d.base_id.is_none())).map(|f| (f.clone(), size(f))).collect();
        local.sort_by_key(|(f, _)| f.id);
        let total: u64 = local.iter().map(|(_, b)| b).sum();
        let mut stats = TierStats { local_after: total, ..Default::default() };
        let mut chosen = Vec::new();
        for (f, b) in local.iter().take(local.len().saturating_sub(1)) {
            if stats.local_after <= keep_local_bytes {
                break;
            }
            chosen.push(f.clone());
            stats.fragments += 1;
            stats.rows += f.physical_rows.unwrap_or(0);
            stats.bytes += b;
            stats.local_after -= b;
        }
        if chosen.is_empty() {
            return Ok(stats);
        }
        let mut ds = (*base).clone();
        compact_files_with_planner(&mut ds, None, &Merge { fragments: chosen, target_bases: Some(vec![COLD_BASE_ID]), max_bytes: None }).await.map_err(lance)?;
        let mut g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
        g.as_mut().expect("the partition store is open").base = Arc::new(ds);
        Ok(stats)
    }

    /// The lowest log offset the log must keep while object storage is on: one past
    /// the archive watermark (everything above it can't yet be rebuilt from object
    /// storage alone). `None` without [`StoreConfig::cold`]: the log is trimmed as before.
    pub fn log_floor(&self) -> Option<LogOffset> {
        self.cfg.cold.as_ref().map(|_| LogOffset(self.archive_state.lock().unwrap().watermark + 1))
    }

    /// The object storage behind the table, if any ([`StoreConfig::cold`]).
    pub fn cold_uri(&self) -> Option<&str> {
        self.cfg.cold.as_deref()
    }

    /// The newest archived version and its offset (0 without object storage).
    pub fn archive_watermark(&self) -> (u64, LogOffset) {
        let st = self.archive_state.lock().unwrap();
        (st.version, LogOffset(st.watermark))
    }

    /// Remove local versions the table no longer needs. With object storage, only
    /// versions older than the archived one (their files are on the mirror);
    /// without, every old version, as before.
    pub(crate) async fn cleanup_local_versions(&self, base: &Dataset) -> StoreResult<lance::dataset::cleanup::RemovalStats> {
        if self.cfg.cold.is_none() {
            return base.cleanup_old_versions(chrono::TimeDelta::zero(), Some(true), Some(false)).await.map_err(lance);
        }
        let archived = self.archive_state.lock().unwrap().version;
        let policy = lance::dataset::cleanup::CleanupPolicy {
            before_version: Some(archived),
            delete_unverified: true,
            error_if_tagged_old_versions: false,
            ..Default::default()
        };
        lance::dataset::cleanup::cleanup_old_versions(base, policy).await.map_err(lance)
    }
}

/// Rebuild partition `partition`'s table under `root` from object storage: the
/// archived `version` of `lineage`, shallow-cloned (no data copied: the clone's
/// manifest points at the mirror and the cold base). The restored table starts a
/// new lineage, so its archive never writes into the one it came from, which is
/// kept as it is. `root/p{n}/memory.lance` must not exist.
pub async fn restore_table(cold: &str, root: &Path, partition: PartitionId, lineage: &str, version: u64) -> StoreResult<String> {
    let dir = root.join(format!("p{}", partition.0)).join("memory.lance");
    if dir.exists() {
        return Err(StoreError::Decode(format!("restoring into {}: it exists", dir.display())));
    }
    std::fs::create_dir_all(dir.parent().unwrap_or(root)).map_err(|e| StoreError::Decode(format!("restore: {e}")))?;
    let mirror = format!("{}/mirror", lineage_uri(cold, partition, lineage));
    let mut ds = Dataset::open(&mirror).await.map_err(lance)?;
    ds.shallow_clone(&dir.to_string_lossy(), version, None).await.map_err(lance)?;
    let fresh = uuid::Uuid::new_v4().simple().to_string();
    record_lineage(&dir, Some(&fresh))?;
    Ok(fresh)
}

/// Copy a tree archived by [`archive_tree`] (`{cold}/{kind}/{lineage}`) into `dir`,
/// which must not exist. Returns the number of files.
pub async fn restore_tree(cold: &str, kind: &str, lineage: &str, dir: &Path) -> StoreResult<usize> {
    use futures::TryStreamExt;
    if dir.exists() {
        return Err(StoreError::Decode(format!("restoring into {}: it exists", dir.display())));
    }
    let uri = format!("{}/{}/{}", cold.trim_end_matches('/'), kind, lineage);
    let (store, base) = lance::io::ObjectStore::from_uri(&uri).await.map_err(lance)?;
    let objects: Vec<_> = store.read_dir_all(&base, None).try_collect().await.map_err(lance)?;
    let prefix = format!("{}/", base.as_ref().trim_end_matches('/'));
    let io = |e: std::io::Error| StoreError::Decode(format!("restore {kind}: {e}"));
    std::fs::create_dir_all(dir).map_err(io)?;
    for o in &objects {
        let path = o.location.as_ref();
        let rel = path.strip_prefix(&prefix).unwrap_or(path);
        let to = dir.join(rel);
        std::fs::create_dir_all(to.parent().unwrap_or(dir)).map_err(io)?;
        std::fs::write(&to, store.read_one_all(&o.location).await.map_err(lance)?).map_err(io)?;
    }
    Ok(objects.len())
}

#[cfg(test)]
mod policy_tests {
    use super::ArchivePolicy;
    use std::time::Duration;

    #[test]
    fn due_on_size_or_on_age_with_anything_new() {
        let p = ArchivePolicy { min_bytes: 100, max_age: Duration::from_secs(60) };
        assert!(p.due(100, true, Duration::ZERO), "size reached");
        assert!(p.due(100, false, Duration::ZERO), "size reached, however new");
        assert!(!p.due(99, true, Duration::from_secs(59)), "neither");
        assert!(p.due(1, true, Duration::from_secs(60)), "age passed with something new");
        assert!(!p.due(0, false, Duration::from_secs(3600)), "an idle node archives nothing");
    }
}

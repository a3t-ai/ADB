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
//! - **Archive.** [`PartitionStore::archive`] copies every root file the mirror lacks:
//!   data, deletions, indexes and transactions first, then the version manifests,
//!   then the latest-version hint. The mirror is then a complete table at the version
//!   archived.
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

/// What the archive holds, kept beside the table (`memory.archived.{lineage}`).
#[derive(Debug, Default, Clone)]
pub struct ArchiveState {
    /// Root-relative paths already on the mirror.
    pub(crate) done: HashSet<String>,
    /// The newest table version whose files are all on the mirror.
    pub version: u64,
    /// That version's `_meta` offset: the log may be trimmed to it.
    pub watermark: u64,
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

/// Compacts the given fragments into the cold base.
struct ToCold {
    fragments: Vec<Fragment>,
}

#[async_trait::async_trait]
impl CompactionPlanner for ToCold {
    async fn plan(&self, ds: &Dataset) -> lance::Result<CompactionPlan> {
        let options = CompactionOptions {
            target_bases: Some(vec![COLD_BASE_ID]),
            materialize_deletions_threshold: 0.0,
            num_threads: Some(1),
            ..Default::default()
        };
        Ok(CompactionPlan { tasks: vec![TaskData { fragments: self.fragments.clone() }], read_version: ds.version().version, options })
    }
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
        } else if !line.is_empty() {
            st.done.insert(line.to_string());
        }
    }
    st
}

fn save_archive_state(dir: &Path, lineage: &str, st: &ArchiveState) -> StoreResult<()> {
    let mut lines: Vec<&str> = st.done.iter().map(String::as_str).collect();
    lines.sort_unstable();
    let text = format!("#version {}\n#watermark {}\n{}\n", st.version, st.watermark, lines.join("\n"));
    let path = state_file(dir, lineage);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).and_then(|_| std::fs::rename(&tmp, &path)).map_err(|e| StoreError::Decode(format!("saving the archive state: {e}")))
}

/// Root-relative paths of every file in the table directory that belongs on the
/// mirror (not the MemWAL: the log replays what the base table does not hold).
fn table_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else { continue };
        for e in entries.flatten() {
            let p = e.path();
            let rel = p.strip_prefix(dir).map(|r| r.to_string_lossy().to_string()).unwrap_or_default();
            if rel.is_empty() || rel == LINEAGE_FILE || rel.starts_with("_mem_wal") {
                continue;
            }
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(rel);
            }
        }
    }
    out
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

/// Copy every file of the table in `dir` that the mirror lacks to object storage
/// (data and side files, then manifests, then the hint), as of `base`'s version;
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
    let mut todo: Vec<String> = table_files(dir).into_iter().filter(|f| !st.done.contains(f) || f.ends_with("latest_version_hint.json")).collect();
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

impl PartitionStore {
    /// Copy every table file the mirror lacks to object storage; then the newest
    /// version is archived and its `_meta` offset is the watermark. A no-op without
    /// [`StoreConfig::cold`]. Safe to repeat: a file copied twice is the same file.
    pub async fn archive(&self) -> StoreResult<ArchiveStats> {
        if self.cfg.cold.is_none() {
            return Ok(ArchiveStats::default());
        }
        let _t = adb_core::steps::timer("store.archive");
        let _one = self.archiving.lock().await;
        let base = { let g = self.state.read().await; open_state(&g).base.clone() };
        let st = self.archive_state.lock().unwrap().clone();
        let (stats, st) = archive_table(&self.cfg, self.partition, &self.table_dir(), &base, st).await?;
        *self.archive_state.lock().unwrap() = st;
        Ok(stats)
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
        compact_files_with_planner(&mut ds, None, &ToCold { fragments: chosen }).await.map_err(lance)?;
        let mut g = { let _w = adb_core::steps::timer("store.state WRITE-lock wait"); self.state.write().await };
        g.as_mut().expect("the partition store is open").base = Arc::new(ds);
        Ok(stats)
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

//! Rebuild a node from object storage (S3 option B, `.ai/S3_OPTION_B_PLAN.md` §3.3).
//!
//! A replica that fell behind the replica set's trimmed log (or a new, empty one) is
//! handed the last [`Effect::Archived`](adb_core::log::Effect::Archived) point by Raft.
//! Before the node opens anything, [`restore_node`] builds its data directory from
//! that point: the memory table shallow-cloned from its mirror (no data copied), the
//! platform tables and the engine copied down, and an empty log starting right after
//! the point. Raft then sends every entry after it, and each store skips what it
//! already holds.
//!
//! The restore is built in `dir/.restoring` and swapped in at the end; the old
//! directories are removed only then (their contents are behind the archive anyway).
//! A restore cut short is started again from scratch.

use std::path::Path;

use adb_core::log::{LogOffset, PartitionId};
use serde::{Deserialize, Serialize};

/// The point a node is restored to: what [`adb_core::log::Effect::Archived`] carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchivePoint {
    /// The object-storage prefix (`ADB_COLD_URI`).
    pub cold: String,
    /// Everything at or below this offset is in the archive.
    pub floor: u64,
    /// `{memory: {lineage, version, watermark}, platform: {lineage, ..}, engine: {lineage, ..}}`.
    pub node: serde_json::Value,
}

/// What [`restore_node`] did.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RestoreStats {
    pub memory_version: u64,
    pub platform_files: usize,
    pub engine_files: usize,
    /// The log's first offset now (`floor + 1`).
    pub log_starts_at: u64,
}

/// The directories a restore replaces.
const PARTS: [&str; 4] = ["p0", "store", "platform", "engine"];

/// Rebuild the data directory `dir` from `point` (see the module docs). `dir/raft`
/// and anything else in `dir` are left as they are.
pub async fn restore_node(dir: &Path, point: &ArchivePoint) -> Result<RestoreStats, String> {
    let at = |what: &str| format!("restore ({what})");
    let work = dir.join(".restoring");
    if work.exists() {
        std::fs::remove_dir_all(&work).map_err(|e| format!("{}: {e}", at("clearing an unfinished one")))?;
    }
    std::fs::create_dir_all(&work).map_err(|e| format!("{}: {e}", at("start")))?;
    let mut stats = RestoreStats { log_starts_at: point.floor + 1, ..Default::default() };
    let part = |name: &str| point.node.get(name).filter(|v| !v.is_null());

    if let Some(m) = part("memory") {
        let lineage = m["lineage"].as_str().ok_or_else(|| at("memory: no lineage"))?;
        let version = m["version"].as_u64().ok_or_else(|| at("memory: no version"))?;
        adb_store::restore_table(&point.cold, &work.join("store"), PartitionId(0), lineage, version).await.map_err(|e| format!("{}: {e}", at("memory")))?;
        stats.memory_version = version;
    }
    if let Some(p) = part("platform") {
        let lineage = p["lineage"].as_str().ok_or_else(|| at("platform: no lineage"))?;
        let root = work.join("platform");
        stats.platform_files = adb_store::restore_tree(&point.cold, "platform", lineage, &root).await.map_err(|e| format!("{}: {e}", at("platform")))?;
        adb_platform::PlatformStore::settle_restored(&root).await.map_err(|e| format!("{}: {e}", at("platform")))?;
    }
    if let Some(e) = part("engine") {
        let lineage = e["lineage"].as_str().ok_or_else(|| at("engine: no lineage"))?;
        stats.engine_files = adb_store::restore_tree(&point.cold, "engine", lineage, &work.join("engine")).await.map_err(|e| format!("{}: {e}", at("engine")))?;
    }
    adb_log::LocalWal::create_at(&work.join("p0"), LogOffset(point.floor + 1)).map_err(|e| format!("{}: {e}", at("log")))?;

    // Swap in: the old parts aside, the new ones in place, then the old ones removed.
    let old = dir.join(".before-restore");
    if old.exists() {
        std::fs::remove_dir_all(&old).map_err(|e| format!("{}: {e}", at("swap")))?;
    }
    std::fs::create_dir_all(&old).map_err(|e| format!("{}: {e}", at("swap")))?;
    for name in PARTS {
        if dir.join(name).exists() {
            std::fs::rename(dir.join(name), old.join(name)).map_err(|e| format!("{}: {e}", at("swap")))?;
        }
        if work.join(name).exists() {
            std::fs::rename(work.join(name), dir.join(name)).map_err(|e| format!("{}: {e}", at("swap")))?;
        }
    }
    std::fs::remove_dir_all(&old).map_err(|e| format!("{}: {e}", at("removing the old parts")))?;
    std::fs::remove_dir_all(&work).map_err(|e| format!("{}: {e}", at("done")))?;
    tracing::info!(floor = point.floor, ?stats, "node restored from object storage");
    Ok(stats)
}

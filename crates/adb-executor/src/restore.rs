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
//!
//! [`restore_from_object_storage`] needs nothing but object storage (§3.0): the last
//! checkpoint point the epoch fence allows, the checkpoint, and the WAL segments
//! above its floor.

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
    /// WAL segments read from object storage, and the records they added to the log.
    pub segments: usize,
    pub segment_records: usize,
    /// The log's last offset after the restore.
    pub log_ends_at: u64,
}

/// The directories a restore replaces: the partition's log, then the stores.
fn parts(partition: PartitionId) -> [String; 4] {
    [format!("p{}", partition.0), "store".into(), "platform".into(), "engine".into()]
}

/// Rebuild the data directory `dir` from `point` (see the module docs). `dir/raft`
/// and anything else in `dir` are left as they are.
pub async fn restore_node(dir: &Path, point: &ArchivePoint) -> Result<RestoreStats, String> {
    restore_partition(dir, point, PartitionId(0)).await
}

/// [`restore_node`] for partition `partition` of a cluster node (step 3): its log in
/// `dir/p{n}`, its table in `dir/store/p{n}`.
pub async fn restore_partition(dir: &Path, point: &ArchivePoint, partition: PartitionId) -> Result<RestoreStats, String> {
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
        adb_store::restore_table(&point.cold, &work.join("store"), partition, lineage, version).await.map_err(|e| format!("{}: {e}", at("memory")))?;
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
    adb_log::LocalWal::create_at(&work.join(format!("p{}", partition.0)), LogOffset(point.floor + 1)).map_err(|e| format!("{}: {e}", at("log")))?;

    // Swap in: the old parts aside, the new ones in place, then the old ones removed.
    let old = dir.join(".before-restore");
    if old.exists() {
        std::fs::remove_dir_all(&old).map_err(|e| format!("{}: {e}", at("swap")))?;
    }
    std::fs::create_dir_all(&old).map_err(|e| format!("{}: {e}", at("swap")))?;
    for name in parts(partition) {
        let name = name.as_str();
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

/// Rebuild the data directory `dir` (empty, or a node to replace) from object storage
/// alone: the best checkpoint point the epoch [`Fence`] allows ([`restore_node`]),
/// then every allowed WAL record above its floor appended to the fresh log. Opening
/// the engine on `dir` afterwards replays those records. With no checkpoint yet, the
/// log is rebuilt from the segments alone.
///
/// `before_epoch`: a new owner taking the partition over at epoch E passes `Some(E)`
/// and reads only what epochs below E wrote; `None` reads everything.
///
/// [`Fence`]: crate::writes::segments::Fence
pub async fn restore_from_object_storage(dir: &Path, cold: &str, before_epoch: Option<u64>) -> Result<RestoreStats, String> {
    restore_partition_from_object_storage(dir, cold, PartitionId(0), before_epoch).await
}

/// [`restore_from_object_storage`] for partition `partition` of a cluster node.
pub async fn restore_partition_from_object_storage(dir: &Path, cold: &str, partition: PartitionId, before_epoch: Option<u64>) -> Result<RestoreStats, String> {
    use crate::writes::segments as seg;
    use adb_log::Log;
    let at = |what: &str| format!("restore from object storage ({what})");
    let log_dir = format!("p{}", partition.0);
    let fence = seg::Fence::load(cold, partition, before_epoch).await.map_err(|e| format!("{}: {e}", at("epochs")))?;
    let point = seg::read_point(cold, partition, &fence).await.map_err(|e| format!("{}: {e}", at("point")))?;
    let mut stats = match &point {
        Some((_, p)) => restore_partition(dir, p, partition).await?,
        None => {
            if dir.join(&log_dir).exists() {
                return Err(at("no checkpoint yet, and the directory holds a log: restore into an empty one"));
            }
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", at("start")))?;
            RestoreStats { log_starts_at: 1, ..Default::default() }
        }
    };
    let floor = point.as_ref().map(|(_, p)| p.floor).unwrap_or(0);
    let wal = adb_log::LocalWal::open(partition, adb_log::WalConfig::new(dir.join(&log_dir))).map_err(|e| format!("{}: {e}", at("log")))?;
    // By first offset: each record is taken once, from the first allowed segment
    // holding it (every allowed copy of an offset is the same acked record).
    let segments = seg::list_segments(cold, partition).await.map_err(|e| format!("{}: {e}", at("segments")))?;
    // Fetched `ADB_RESTORE_FETCHES` at a time (default 16), applied in order: one GET
    // after another made a takeover take seconds (Hetzner, 15 Nov).
    use futures::StreamExt;
    let fetches = std::env::var("ADB_RESTORE_FETCHES").ok().and_then(|v| v.parse().ok()).filter(|&n: &usize| n > 0).unwrap_or(16);
    let wanted: Vec<_> = segments.into_iter().filter(|s| fence.admits(s.epoch) && s.last > floor && s.first <= fence.limit(s.epoch)).collect();
    let mut fetched = futures::stream::iter(wanted.into_iter().map(|s| async move { seg::read_segment(cold, partition, s).await.map(|r| (s, r)) })).buffered(fetches);
    // Queued, not waited on one by one: a group sync covers each batch; the last is
    // waited on below.
    let mut last_wait = None;
    while let Some(next) = fetched.next().await {
        let (s, records) = next.map_err(|e| format!("{}: {e}", at("segment")))?;
        stats.segments += 1;
        for r in records {
            // Held by the checkpoint or an earlier segment; or written after this
            // epoch lost the partition (never acked).
            if r.offset.0 <= wal.last_offset().0 || r.offset.0 <= floor || !fence.allows(s.epoch, r.offset.0) {
                continue;
            }
            if r.offset.0 != wal.last_offset().0 + 1 {
                return Err(at(&format!("gap in the WAL segments: offset {} follows {}", r.offset.0, wal.last_offset().0)));
            }
            last_wait = Some(wal.enqueue(r, false).map_err(|e| format!("{}: {e}", at("appending a record")))?);
            stats.segment_records += 1;
        }
    }
    if let Some(w) = last_wait {
        w.await.map_err(|e| format!("{}: {e}", at("appending a record")))?;
    }
    stats.log_ends_at = wal.last_offset().0;
    drop(wal);
    tracing::info!(floor, ?stats, "node restored from object storage alone");
    Ok(stats)
}

//! WAL segments on object storage (S3 option B, `.ai/S3_OPTION_B_PLAN.md` §3.0).
//!
//! Object storage can't be appended to, so the log reaches it as small immutable
//! objects, one per stretch of records:
//!
//! ```text
//! {cold}/p{n}/wal/{first:020}-{last:020}.seg     rmp-encoded Vec<LogRecord>
//! {cold}/p{n}/POINT                              the last checkpoint (ArchivePoint)
//! ```
//!
//! - **Shipping.** Every `ADB_WAL_SEGMENT_MS` (default 1000) the owner puts the
//!   records its log holds durably and object storage lacks, one object per
//!   `ADB_WAL_SEGMENT_BYTES` (default 8 MiB). Object storage then holds every record
//!   up to the shipped offset: the checkpoint below the floor, segments above it.
//! - **Checkpoint.** After each checkpoint (`archive_tick`) the owner writes `POINT`
//!   and deletes the segments wholly at or below its floor: every row, tombstone
//!   and tombstone-log entry they hold is in the checkpoint's large files.
//! - **Restore** ([`crate::restore::restore_from_object_storage`]): `POINT`, the
//!   checkpoint, then every segment above the floor into a fresh local log.

use std::sync::atomic::Ordering;

use adb_core::log::{LogOffset, LogRecord, PartitionId};
use adb_core::{AdbError, AdbResult};
use adb_log::{Log, LogError};
use futures::TryStreamExt;

use super::{log_err, seq_timed, DurableWrites};

/// `ADB_WAL_SEGMENT_MS`: how often segments are shipped.
pub fn segment_interval() -> std::time::Duration {
    std::time::Duration::from_millis(std::env::var("ADB_WAL_SEGMENT_MS").ok().and_then(|v| v.parse().ok()).filter(|&v: &u64| v > 0).unwrap_or(1000))
}

/// `ADB_WAL_SEGMENT_BYTES`: the most one segment holds (one record may exceed it).
fn segment_bytes() -> usize {
    std::env::var("ADB_WAL_SEGMENT_BYTES").ok().and_then(|v| v.parse().ok()).filter(|&v: &usize| v > 0).unwrap_or(8 << 20)
}

fn partition_uri(cold: &str, partition: PartitionId) -> String {
    format!("{}/p{}", cold.trim_end_matches('/'), partition.0)
}

fn segment_name(first: u64, last: u64) -> String {
    format!("{first:020}-{last:020}.seg")
}

fn parse_segment(name: &str) -> Option<(u64, u64)> {
    let (a, b) = name.strip_suffix(".seg")?.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// A segment on object storage: its first and last offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Segment {
    pub first: u64,
    pub last: u64,
}

/// The segments of `partition` under `cold`, in order.
pub async fn list_segments(cold: &str, partition: PartitionId) -> AdbResult<Vec<Segment>> {
    let (store, base) = adb_store::lance::io::ObjectStore::from_uri(&partition_uri(cold, partition)).await.map_err(|e| AdbError::Internal(format!("wal segments: {e}")))?;
    let objects: Vec<_> = store.read_dir_all(&base.clone().join("wal"), None).try_collect().await.map_err(|e| AdbError::Internal(format!("wal segments: {e}")))?;
    let mut out: Vec<Segment> = objects
        .iter()
        .filter_map(|o| o.location.filename().and_then(parse_segment))
        .map(|(first, last)| Segment { first, last })
        .collect();
    out.sort();
    Ok(out)
}

/// The records of one segment.
pub async fn read_segment(cold: &str, partition: PartitionId, seg: Segment) -> AdbResult<Vec<LogRecord>> {
    let (store, base) = adb_store::lance::io::ObjectStore::from_uri(&partition_uri(cold, partition)).await.map_err(|e| AdbError::Internal(format!("wal segment: {e}")))?;
    let bytes = store.read_one_all(&base.clone().join("wal").join(segment_name(seg.first, seg.last))).await.map_err(|e| AdbError::Internal(format!("wal segment: {e}")))?;
    rmp_serde::from_slice(&bytes).map_err(|e| AdbError::Internal(format!("wal segment {}-{}: {e}", seg.first, seg.last)))
}

async fn put(cold: &str, partition: PartitionId, rel: &[&str], bytes: &[u8]) -> AdbResult<()> {
    let (store, base) = adb_store::lance::io::ObjectStore::from_uri(&partition_uri(cold, partition)).await.map_err(|e| AdbError::Internal(format!("object storage: {e}")))?;
    let path = rel.iter().fold(base, |p, part| p.join(*part));
    store.put(&path, bytes).await.map_err(|e| AdbError::Internal(format!("object storage: {e}")))?;
    Ok(())
}

/// Write the last checkpoint's point (`{cold}/p{n}/POINT`).
pub async fn write_point(point: &crate::restore::ArchivePoint, partition: PartitionId) -> AdbResult<()> {
    let bytes = serde_json::to_vec(point).map_err(|e| AdbError::Internal(format!("point: {e}")))?;
    put(&point.cold, partition, &["POINT"], &bytes).await
}

/// The last checkpoint's point, if one was written.
pub async fn read_point(cold: &str, partition: PartitionId) -> AdbResult<Option<crate::restore::ArchivePoint>> {
    let (store, base) = adb_store::lance::io::ObjectStore::from_uri(&partition_uri(cold, partition)).await.map_err(|e| AdbError::Internal(format!("point: {e}")))?;
    match store.read_one_all(&base.clone().join("POINT")).await {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|e| AdbError::Internal(format!("point: {e}"))),
        Err(adb_store::lance::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(AdbError::Internal(format!("point: {e}"))),
    }
}

/// What one [`DurableWrites::ship_segments`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShipStats {
    pub segments: usize,
    pub records: usize,
    pub bytes: usize,
    /// Object storage holds every record up to here.
    pub shipped: u64,
}

impl<L: Log> DurableWrites<L> {
    /// Put the records the log holds durably and object storage lacks as segments
    /// (see the module docs). A no-op without object storage. One at a time.
    pub async fn ship_segments(&self) -> AdbResult<ShipStats> {
        let Some(store) = &self.store else { return Ok(ShipStats::default()) };
        let Some(cold) = store.cold_uri().map(str::to_string) else { return Ok(ShipStats::default()) };
        let partition = store.partition();
        let mut shipped = self.shipped.lock().await;
        let mut from = match *shipped {
            Some(s) => s,
            // First call: what object storage holds already (segments, or a checkpoint).
            None => {
                let last_seg = list_segments(&cold, partition).await?.last().map(|s| s.last).unwrap_or(0);
                let point = read_point(&cold, partition).await?.map(|p| p.floor).unwrap_or(0);
                last_seg.max(point)
            }
        };
        // A checkpoint above it covers the stretch in between.
        from = from.max(self.archive_marked.load(Ordering::SeqCst));
        let mut stats = ShipStats { shipped: from, ..Default::default() };
        let limit = segment_bytes();
        let mut pending: Vec<LogRecord> = Vec::new();
        let mut pending_bytes = 0usize;
        let flush = |recs: Vec<LogRecord>| {
            let cold = cold.clone();
            async move {
                let (first, last) = (recs[0].offset.0, recs[recs.len() - 1].offset.0);
                let bytes = rmp_serde::to_vec_named(&recs).map_err(|e| AdbError::Internal(format!("wal segment: {e}")))?;
                put(&cold, partition, &["wal", &segment_name(first, last)], &bytes).await?;
                AdbResult::Ok((last, recs.len(), bytes.len()))
            }
        };
        loop {
            // Read a batch under the sequencer, releasing it between batches.
            let batch = {
                let seq = seq_timed(&self.seq, "seq.wait @ship", "seq.held @ship").await;
                let durable = seq.log().durable_offset().0;
                if durable <= from {
                    Vec::new()
                } else {
                    match seq.log().read(LogOffset(from + 1), 1024).await {
                        Ok(b) => b.into_iter().filter(|r| r.offset.0 <= durable).collect(),
                        // Trimmed locally below what was shipped can't happen (the purge floor
                        // stays below the checkpoint); a gap means the checkpoint holds it.
                        Err(LogError::Purged { oldest, .. }) if oldest.0 > from + 1 => {
                            from = oldest.0 - 1;
                            continue;
                        }
                        Err(e) => return Err(log_err(e)),
                    }
                }
            };
            let Some(last) = batch.last().map(|r| r.offset.0) else { break };
            for r in batch {
                pending_bytes += rmp_serde::to_vec_named(&r).map(|b| b.len()).unwrap_or(0);
                pending.push(r);
                if pending_bytes >= limit {
                    let (l, n, b) = flush(std::mem::take(&mut pending)).await?;
                    pending_bytes = 0;
                    *shipped = Some(l);
                    stats = ShipStats { segments: stats.segments + 1, records: stats.records + n, bytes: stats.bytes + b, shipped: l };
                }
            }
            from = last;
        }
        if !pending.is_empty() {
            let (l, n, b) = flush(pending).await?;
            stats = ShipStats { segments: stats.segments + 1, records: stats.records + n, bytes: stats.bytes + b, shipped: l };
        }
        *shipped = Some(stats.shipped);
        Ok(stats)
    }

    /// Object storage holds every record up to this offset (0 before the first ship).
    pub async fn shipped_offset(&self) -> u64 {
        self.shipped.lock().await.unwrap_or(0)
    }

    /// After a checkpoint at `point`: write `POINT`, then delete the segments wholly at
    /// or below its floor (their records are in the checkpoint). Returns how many.
    pub(super) async fn checkpoint_segments(&self, point: &crate::restore::ArchivePoint, partition: PartitionId) -> AdbResult<usize> {
        write_point(point, partition).await?;
        let (store, base) = adb_store::lance::io::ObjectStore::from_uri(&partition_uri(&point.cold, partition)).await.map_err(|e| AdbError::Internal(format!("wal segments: {e}")))?;
        let mut deleted = 0;
        for seg in list_segments(&point.cold, partition).await?.into_iter().filter(|s| s.last <= point.floor) {
            store.delete(&base.clone().join("wal").join(segment_name(seg.first, seg.last))).await.map_err(|e| AdbError::Internal(format!("wal segments: {e}")))?;
            deleted += 1;
        }
        Ok(deleted)
    }
}

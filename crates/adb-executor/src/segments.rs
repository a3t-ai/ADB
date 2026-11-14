//! WAL segments, checkpoint points and owner epochs on object storage (S3 option B,
//! `.ai/S3_OPTION_B_PLAN.md` §3.0).
//!
//! Object storage can't be appended to, so the log reaches it as small immutable
//! objects, one per stretch of records. Every key carries the epoch of the owner
//! that wrote it, and nothing an owner writes is ever overwritten by another:
//!
//! ```text
//! {cold}/p{n}/wal/{epoch:010}-{first:020}-{last:020}.seg   rmp-encoded Vec<LogRecord>
//! {cold}/p{n}/points/{epoch:010}-{floor:020}.json          a checkpoint (ArchivePoint)
//! {cold}/p{n}/epochs/{epoch:010}.json                       {"start": offset}
//! ```
//!
//! - **Shipping.** Every `ADB_WAL_SEGMENT_MS` (default 1000) the owner puts the
//!   records its log holds durably and object storage lacks, one object per
//!   `ADB_WAL_SEGMENT_BYTES` (default 8 MiB). Object storage then holds every record
//!   up to the shipped offset: the checkpoint below the floor, segments above it.
//! - **Checkpoint.** After each checkpoint (`archive_tick`) the owner writes its
//!   point, then deletes the segments wholly at or below its floor (every row,
//!   tombstone and tombstone-log entry they hold is in the checkpoint's large files)
//!   and its own older points. It first checks that no newer owner has started
//!   ([`Fenced`]); a stale owner writes and deletes nothing at a checkpoint.
//! - **Epochs.** An owner taking a partition over at epoch E writes `epochs/E` with
//!   the first offset it will write ([`DurableWrites::begin_epoch`]) before it takes
//!   writes. Every record below that start was acked through the replica, which the
//!   new owner was, so it holds them all. Whatever epoch E-1 wrote above `start - 1`
//!   was never acked: the [`Fence`] drops it.
//! - **Restore** ([`crate::restore::restore_from_object_storage`]): the best point the
//!   fence allows, the checkpoint, then every allowed record above its floor.

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

fn ostore_err(what: &'static str) -> impl Fn(adb_store::lance::Error) -> AdbError {
    move |e| AdbError::Internal(format!("{what}: {e}"))
}

/// `(store, path of {cold}/p{n}/{dir}/{name})` (`name` may be empty: the directory).
/// A macro, as the path's type (object_store's) isn't re-exported by Lance.
macro_rules! at {
    ($cold:expr, $partition:expr, $dir:expr, $name:expr) => {{
        let uri = format!("{}/p{}", $cold.trim_end_matches('/'), $partition.0);
        let (store, base) = adb_store::lance::io::ObjectStore::from_uri(&uri).await.map_err(ostore_err("object storage"))?;
        let path = base.clone().join($dir);
        let path = if $name.is_empty() { path } else { path.join($name) };
        (store, path)
    }};
}

/// The object names under `{cold}/p{n}/{dir}`.
async fn names(cold: &str, partition: PartitionId, dir: &str) -> AdbResult<Vec<String>> {
    let (store, path) = at!(cold, partition, dir, "");
    let objects: Vec<_> = store.read_dir_all(&path, None).try_collect().await.map_err(ostore_err("object storage listing"))?;
    Ok(objects.iter().filter_map(|o| o.location.filename().map(str::to_string)).collect())
}

async fn put(cold: &str, partition: PartitionId, dir: &str, name: &str, bytes: &[u8]) -> AdbResult<()> {
    let (store, path) = at!(cold, partition, dir, name);
    store.put(&path, bytes).await.map_err(ostore_err("object storage"))?;
    Ok(())
}

async fn get(cold: &str, partition: PartitionId, dir: &str, name: &str) -> AdbResult<Vec<u8>> {
    let (store, path) = at!(cold, partition, dir, name);
    Ok(store.read_one_all(&path).await.map_err(ostore_err("object storage"))?.to_vec())
}

async fn delete(cold: &str, partition: PartitionId, dir: &str, name: &str) -> AdbResult<()> {
    let (store, path) = at!(cold, partition, dir, name);
    store.delete(&path).await.map_err(ostore_err("object storage"))
}

// ---- segments ----

/// A segment on object storage: the epoch that wrote it, its first and last offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Segment {
    pub first: u64,
    pub last: u64,
    pub epoch: u64,
}

impl Segment {
    fn name(&self) -> String {
        format!("{:010}-{:020}-{:020}.seg", self.epoch, self.first, self.last)
    }

    fn parse(name: &str) -> Option<Self> {
        let mut it = name.strip_suffix(".seg")?.splitn(3, '-');
        Some(Segment { epoch: it.next()?.parse().ok()?, first: it.next()?.parse().ok()?, last: it.next()?.parse().ok()? })
    }
}

/// The segments of `partition` under `cold`, by first offset (then epoch).
pub async fn list_segments(cold: &str, partition: PartitionId) -> AdbResult<Vec<Segment>> {
    let mut out: Vec<Segment> = names(cold, partition, "wal").await?.iter().filter_map(|n| Segment::parse(n)).collect();
    out.sort();
    Ok(out)
}

/// The records of one segment.
pub async fn read_segment(cold: &str, partition: PartitionId, seg: Segment) -> AdbResult<Vec<LogRecord>> {
    let bytes = get(cold, partition, "wal", &seg.name()).await?;
    rmp_serde::from_slice(&bytes).map_err(|e| AdbError::Internal(format!("wal segment {}: {e}", seg.name())))
}

// ---- epochs ----

#[derive(serde::Serialize, serde::Deserialize)]
struct EpochMarker {
    start: u64,
}

/// Every owner epoch that started on `partition`: `(epoch, first offset it wrote)`,
/// in order.
pub async fn list_epochs(cold: &str, partition: PartitionId) -> AdbResult<Vec<(u64, u64)>> {
    let mut out = Vec::new();
    for name in names(cold, partition, "epochs").await? {
        let Some(epoch) = name.strip_suffix(".json").and_then(|e| e.parse::<u64>().ok()) else { continue };
        let m: EpochMarker = serde_json::from_slice(&get(cold, partition, "epochs", &name).await?).map_err(|e| AdbError::Internal(format!("epoch marker {name}: {e}")))?;
        out.push((epoch, m.start));
    }
    out.sort();
    Ok(out)
}

/// What object storage holds that a restore may trust, from the epoch markers.
///
/// Epoch `e` wrote validly up to the start of the next epoch after it, minus one;
/// beyond that it had lost the partition and nothing it wrote was acked.
#[derive(Debug, Clone, Default)]
pub struct Fence {
    /// `(epoch, start)` for the epochs this fence admits, in order.
    epochs: Vec<(u64, u64)>,
    /// Admit only objects of epochs below this (`None`: all).
    before: Option<u64>,
}

impl Fence {
    /// The fence over `partition`'s markers; `before`: only objects written by epochs
    /// below it (a new owner at epoch E restores with `Some(E)`), `None`: all.
    pub async fn load(cold: &str, partition: PartitionId, before: Option<u64>) -> AdbResult<Self> {
        let epochs = list_epochs(cold, partition).await?.into_iter().filter(|(e, _)| before.map_or(true, |b| *e < b)).collect();
        Ok(Fence { epochs, before })
    }

    /// Whether objects of `epoch` are admitted at all.
    pub fn admits(&self, epoch: u64) -> bool {
        self.before.map_or(true, |b| epoch < b)
    }

    /// The last offset `epoch` wrote validly (`u64::MAX`: no later epoch started).
    pub fn limit(&self, epoch: u64) -> u64 {
        self.epochs.iter().filter(|(e, _)| *e > epoch).map(|(_, start)| start.saturating_sub(1)).min().unwrap_or(u64::MAX)
    }

    /// Whether `record` written by `epoch` is admitted.
    pub fn allows(&self, epoch: u64, offset: u64) -> bool {
        self.admits(epoch) && offset <= self.limit(epoch)
    }
}

// ---- points ----

fn point_name(epoch: u64, floor: u64) -> String {
    format!("{epoch:010}-{floor:020}.json")
}

fn parse_point(name: &str) -> Option<(u64, u64)> {
    let (e, f) = name.strip_suffix(".json")?.split_once('-')?;
    Some((e.parse().ok()?, f.parse().ok()?))
}

/// Write a checkpoint's point (`points/{epoch}-{floor}.json`).
pub async fn write_point(point: &crate::restore::ArchivePoint, epoch: u64, partition: PartitionId) -> AdbResult<()> {
    let bytes = serde_json::to_vec(point).map_err(|e| AdbError::Internal(format!("point: {e}")))?;
    put(&point.cold, partition, "points", &point_name(epoch, point.floor), &bytes).await
}

/// The point to restore from: the highest floor among those the fence allows
/// (`None`: no checkpoint yet), with the epoch that wrote it.
pub async fn read_point(cold: &str, partition: PartitionId, fence: &Fence) -> AdbResult<Option<(u64, crate::restore::ArchivePoint)>> {
    let best = names(cold, partition, "points").await?.iter().filter_map(|n| parse_point(n)).filter(|&(e, floor)| fence.allows(e, floor)).max_by_key(|&(e, floor)| (floor, e));
    let Some((epoch, floor)) = best else { return Ok(None) };
    let bytes = get(cold, partition, "points", &point_name(epoch, floor)).await?;
    let point = serde_json::from_slice(&bytes).map_err(|e| AdbError::Internal(format!("point: {e}")))?;
    Ok(Some((epoch, point)))
}

/// What [`DurableWrites::checkpoint_segments`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpointed {
    /// The point written; this many segments deleted.
    Done { deleted: usize },
    /// A newer owner (epoch `newer`) has started on this partition: this one no
    /// longer owns it, and wrote and deleted nothing.
    Fenced { newer: u64 },
}

// ---- the owner's side ----

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
    /// The owner epoch this engine writes object keys under (0 on a single node).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Take the partition over at `epoch`: write `epochs/{epoch}` with the first
    /// offset this owner will write, before any write is taken. Records below it are
    /// the ones the log holds now. A no-op without object storage.
    pub async fn begin_epoch(&self, epoch: u64) -> AdbResult<()> {
        let seq = seq_timed(&self.seq, "seq.wait @epoch", "seq.held @epoch").await;
        if let Some(cold) = self.store.as_ref().and_then(|s| s.cold_uri().map(|c| (c.to_string(), s.partition()))) {
            let start = seq.log().last_offset().0 + 1;
            let bytes = serde_json::to_vec(&EpochMarker { start }).map_err(|e| AdbError::Internal(format!("epoch marker: {e}")))?;
            put(&cold.0, cold.1, "epochs", &format!("{epoch:010}.json"), &bytes).await?;
            tracing::info!(epoch, start, "owner epoch started");
        }
        self.epoch.store(epoch, Ordering::SeqCst);
        // What object storage holds is looked up again under the new epoch.
        *self.shipped.lock().await = None;
        drop(seq);
        Ok(())
    }

    /// Put the records the log holds durably and object storage lacks as segments
    /// (see the module docs). A no-op without object storage. One at a time.
    pub async fn ship_segments(&self) -> AdbResult<ShipStats> {
        let Some(store) = &self.store else { return Ok(ShipStats::default()) };
        let Some(cold) = store.cold_uri().map(str::to_string) else { return Ok(ShipStats::default()) };
        let partition = store.partition();
        let epoch = self.epoch();
        let mut shipped = self.shipped.lock().await;
        let mut from = match *shipped {
            Some(s) => s,
            // First call: what object storage holds already (segments, or a checkpoint).
            None => {
                let fence = Fence::load(&cold, partition, Some(epoch + 1)).await?;
                let last_seg = list_segments(&cold, partition).await?.into_iter().filter(|s| fence.admits(s.epoch)).map(|s| s.last.min(fence.limit(s.epoch))).max().unwrap_or(0);
                let point = read_point(&cold, partition, &fence).await?.map(|(_, p)| p.floor).unwrap_or(0);
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
                let seg = Segment { epoch, first: recs[0].offset.0, last: recs[recs.len() - 1].offset.0 };
                let bytes = rmp_serde::to_vec_named(&recs).map_err(|e| AdbError::Internal(format!("wal segment: {e}")))?;
                put(&cold, partition, "wal", &seg.name(), &bytes).await?;
                AdbResult::Ok((seg.last, recs.len(), bytes.len()))
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

    /// After a checkpoint at `point`: unless a newer owner has started
    /// ([`Checkpointed::Fenced`]), write the point, then delete the segments wholly at
    /// or below its floor (their records are in the checkpoint) and this epoch's older
    /// points.
    pub(super) async fn checkpoint_segments(&self, point: &crate::restore::ArchivePoint, partition: PartitionId) -> AdbResult<Checkpointed> {
        let epoch = self.epoch();
        let cold = point.cold.as_str();
        if let Some(&(newer, _)) = list_epochs(cold, partition).await?.iter().find(|(e, _)| *e > epoch) {
            return Ok(Checkpointed::Fenced { newer });
        }
        write_point(point, epoch, partition).await?;
        let mut deleted = 0;
        for seg in list_segments(cold, partition).await?.into_iter().filter(|s| s.last <= point.floor && s.epoch <= epoch) {
            delete(cold, partition, "wal", &seg.name()).await?;
            deleted += 1;
        }
        for (e, floor) in names(cold, partition, "points").await?.iter().filter_map(|n| parse_point(n)) {
            if e == epoch && floor < point.floor {
                delete(cold, partition, "points", &point_name(e, floor)).await?;
            }
        }
        Ok(Checkpointed::Done { deleted })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        let s = Segment { epoch: 3, first: 10, last: 99 };
        assert_eq!(Segment::parse(&s.name()), Some(s));
        assert_eq!(parse_point(&point_name(2, 500)), Some((2, 500)));
    }

    #[test]
    fn fence_limits_each_epoch_to_the_next_start() {
        let f = Fence { epochs: vec![(1, 101), (2, 251)], before: None };
        assert_eq!(f.limit(0), 100);
        assert_eq!(f.limit(1), 250);
        assert_eq!(f.limit(2), u64::MAX);
        assert!(f.allows(0, 100) && !f.allows(0, 101));
        assert!(f.allows(1, 250) && !f.allows(1, 251));
        // A new owner at epoch 2 restores from epochs 0 and 1 only.
        let g = Fence { epochs: vec![(1, 101)], before: Some(2) };
        assert!(g.allows(1, 10_000) && !g.allows(2, 300));
    }
}

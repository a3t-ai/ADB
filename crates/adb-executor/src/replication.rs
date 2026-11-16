//! Owner → replica (S3 option B, step 5).
//!
//! A partition's owner writes its local WAL and sends every record to the partition's
//! replica; a write is acknowledged once both hold it — the local fsync and the
//! replica's answer run at the same time, so the cost is one round trip. The replica
//! keeps the records in memory only and drops them once a WAL segment holding them is
//! on object storage: at any moment the acknowledged writes not yet on object storage
//! are on two nodes.
//!
//! - **Owner** ([`ReplicatedLog`], an `adb_log::Log` over the local WAL): a task
//!   batches the records to the replica with the owner's epoch and how far object
//!   storage holds the log (`note_shipped`). Its durable offset is what both hold,
//!   so WAL segments only ever ship acknowledged records (what step 1's fencing
//!   relies on). No replica in the map: writes wait. A replica that answers `gap`
//!   (a new, empty one) is sent the WAL from just above the shipped offset.
//! - **Replica** ([`ReplicaBuffers`]): per partition, contiguous records of the current
//!   epoch; an older epoch is refused (`fenced`), so a stale owner can't commit.
//!   Past `ADB_REPLICA_MEM_BYTES` (default 100 MB) the oldest records spill to disk,
//!   fsynced (object storage slow or down); past `ADB_REPLICA_SPILL_MAX_BYTES`
//!   (default 4 GB) appends are refused (`replica_full`) and the owner's writes wait.
//! - **Ack from memory:** the owner's WAL acknowledges a record once written (page
//!   cache) and fsyncs right after, off the ack path (`WalSync::Behind`): with the
//!   replica's copy in memory, a write waits for no disk. A power cut on the owner
//!   loses at most its last unsynced batch locally, and the replica holds it; the
//!   partition moves there once the owner's lease lapses. `ADB_WAL_ACK=fsync` waits
//!   for the owner's fsync as well.
//! - `ADB_WAL_REPLICAS=0`: no replica; a write is acknowledged after the local fsync.

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adb_cluster::map::Partition;
use adb_core::log::{LogOffset, LogRecord, PartitionId};
use adb_log::{CommitWait, Log, LogError, LogResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// `ADB_WAL_REPLICAS`: 1 (default) or 0.
pub fn replicas() -> u64 {
    env_u64("ADB_WAL_REPLICAS", 1).min(1)
}

/// How a cluster owner with a replica syncs its WAL: `ADB_WAL_ACK=memory` (default,
/// `Behind`) or `fsync` (`Ack`).
pub fn owner_sync() -> adb_log::WalSync {
    match std::env::var("ADB_WAL_ACK").map(|v| v.trim().to_ascii_lowercase()) {
        Ok(v) if v == "fsync" => adb_log::WalSync::Ack,
        _ => adb_log::WalSync::Behind,
    }
}

/// One replication call: records of `epoch` (empty: only the shipped offset).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateReq {
    pub epoch: u64,
    /// Object storage holds the log up to here: the replica may drop it.
    pub shipped: u64,
    pub records: Vec<LogRecord>,
}

/// The replica's answer: it holds every record up to `held` (above the shipped offset).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReplicateAck {
    pub held: u64,
}

/// Where the owner's records go: the partition's current replica and epoch.
#[async_trait]
pub trait ReplicaTarget: Send + Sync {
    /// `(replica address, epoch)`; `None`: no replica assigned now.
    fn target(&self) -> Option<(String, u64)>;
    /// Why this owner may no longer write (the partition moved to a newer epoch or
    /// another owner), or `None`. Checked before every replication call: an owner
    /// that kept replicating after the map moved on sent its writes to the NEW
    /// epoch's replica — itself, in a rolling restart — and acknowledged them while
    /// the new owner never got them. Default: never.
    fn superseded(&self) -> Option<String> {
        None
    }
    async fn replicate(&self, addr: &str, req: ReplicateReq) -> Result<ReplicateAck, String>;
}

/// An owner's log: the local WAL, plus the replica (see the module docs).
pub struct ReplicatedLog {
    wal: Arc<dyn Log>,
    acked: watch::Receiver<u64>,
    queue: mpsc::UnboundedSender<u64>,
    shipped: Arc<std::sync::atomic::AtomicU64>,
    /// Set once a replica refused this owner's epoch: it no longer owns the partition.
    fenced: Arc<Mutex<Option<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ReplicatedLog {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ReplicatedLog {
    /// Replicate `wal`'s new records to `target` (the replica holds nothing yet).
    pub fn new(wal: Arc<dyn Log>, target: Arc<dyn ReplicaTarget>) -> Arc<Self> {
        let start = wal.last_offset().0;
        let (acked_tx, acked) = watch::channel(start);
        let (queue, rx) = mpsc::unbounded_channel();
        let shipped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let fenced: Arc<Mutex<Option<String>>> = Default::default();
        let task = tokio::spawn(replicate_loop(Arc::clone(&wal), target, rx, acked_tx, Arc::clone(&shipped), Arc::clone(&fenced), start));
        Arc::new(Self { wal, acked, queue, shipped, fenced, task })
    }

    /// The offset the replica holds every record up to.
    pub fn acked(&self) -> u64 {
        *self.acked.borrow()
    }

    fn wait_acked(&self, offset: u64) -> impl std::future::Future<Output = LogResult<()>> + Send + 'static {
        let mut acked = self.acked.clone();
        let fenced = Arc::clone(&self.fenced);
        async move {
            loop {
                if let Some(e) = fenced.lock().unwrap().clone() {
                    return Err(LogError::Io(e));
                }
                if *acked.borrow() >= offset {
                    return Ok(());
                }
                if acked.changed().await.is_err() {
                    return Err(LogError::Io("the replication task stopped".into()));
                }
            }
        }
    }
}

/// Records per replication call at most.
const REPLICATE_BATCH: usize = 1024;
/// How long a replica may refuse before it is a warning, and how often it is said.
const REPLICATE_WARN_AFTER: Duration = Duration::from_secs(5);
const REPLICATE_WARN_EVERY: Duration = Duration::from_secs(30);

/// The replication task: wake on new records (or every second, to pass the shipped
/// offset on), send what the replica lacks, move `acked`.
async fn replicate_loop(
    wal: Arc<dyn Log>,
    target: Arc<dyn ReplicaTarget>,
    mut rx: mpsc::UnboundedReceiver<u64>,
    acked: watch::Sender<u64>,
    shipped: Arc<std::sync::atomic::AtomicU64>,
    fenced: Arc<Mutex<Option<String>>>,
    start: u64,
) {
    let mut held = start;
    let mut last_addr: Option<String> = None;
    let mut buf = Vec::new();
    // Records per call: halved while the replica refuses a call as too large, whole
    // again once the replica has caught up.
    let mut batch = REPLICATE_BATCH;
    // A replica that keeps refusing stalls this partition's writes: said at WARN
    // (once per REPLICATE_WARN_EVERY), not only at debug.
    let mut failing_since: Option<std::time::Instant> = None;
    let mut warned_at: Option<std::time::Instant> = None;
    loop {
        tokio::select! {
            n = rx.recv_many(&mut buf, 4096) => {
                if n == 0 {
                    return;
                }
                buf.clear();
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        // Send until the replica holds everything appended so far.
        let mut backoff = Duration::from_millis(20);
        loop {
            if let Some(why) = target.superseded() {
                tracing::warn!(partition = wal.partition().0, why = %why, "replication: this owner's epoch is over; writes stop");
                *fenced.lock().unwrap() = Some(format!("fenced: {why}"));
                acked.send_modify(|_| {});
                return;
            }
            let Some((addr, epoch)) = target.target() else {
                // No replica assigned: writes wait (the control plane picks one).
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            if last_addr.as_deref() != Some(addr.as_str()) {
                // Another replica: it starts empty; what it lacks is above the shipped offset.
                held = held.min(shipped.load(std::sync::atomic::Ordering::SeqCst));
                last_addr = Some(addr.clone());
            }
            let last = wal.last_offset().0;
            let ship = shipped.load(std::sync::atomic::Ordering::SeqCst);
            let records = if last > held {
                match wal.read(LogOffset(held + 1), batch).await {
                    Ok(r) => r,
                    // Below `oldest` the WAL was purged — only ever once object storage
                    // held those records (a partition restored from it starts its WAL
                    // above the restore floor). The replica takes them as shipped and
                    // gets the rest from here. Asking for them again forever stalled
                    // every write after a restart.
                    Err(adb_log::LogError::Purged { oldest, .. }) => {
                        let floor = oldest.0.saturating_sub(1);
                        shipped.fetch_max(floor, std::sync::atomic::Ordering::SeqCst);
                        held = held.max(floor);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, from = held + 1, "replication: reading the WAL failed");
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                }
            } else {
                Vec::new()
            };
            let upto = records.last().map(|r| r.offset.0).unwrap_or(held);
            match target.replicate(&addr, ReplicateReq { epoch, shipped: ship, records }).await {
                Ok(ack) => {
                    if let Some(since) = failing_since.take() {
                        if warned_at.is_some() {
                            tracing::info!(partition = wal.partition().0, %addr, failing_for = ?since.elapsed(), "replication: the replica takes records again");
                        }
                        warned_at = None;
                    }
                    // The replica's answer is what it holds.
                    held = ack.held;
                    let _ = upto;
                    acked.send_if_modified(|a| {
                        let moved = held > *a;
                        *a = (*a).max(held);
                        moved
                    });
                    if held >= wal.last_offset().0 {
                        batch = REPLICATE_BATCH;
                        break;
                    }
                }
                Err(e) if e.starts_with("gap:") => {
                    // The replica holds less than sent for: start again from what it has.
                    held = e.trim_start_matches("gap:").split(':').next().and_then(|n| n.trim().parse().ok()).unwrap_or(ship).max(ship);
                }
                Err(e) if e.starts_with("fenced:") => {
                    tracing::error!(error = %e, "replication: a newer owner holds this partition; writes stop");
                    *fenced.lock().unwrap() = Some(e);
                    acked.send_modify(|_| {});
                    return;
                }
                Err(e) => {
                    let too_large = e.starts_with("too_large:");
                    if too_large && batch > 1 {
                        // Fewer records per call; no wait: the replica is fine.
                        batch = (batch / 2).max(1);
                        tracing::debug!(partition = wal.partition().0, batch, "replication: a call was too large; sending fewer records");
                        continue;
                    }
                    let since = *failing_since.get_or_insert_with(std::time::Instant::now);
                    if warned_at.is_none_or(|w| w.elapsed() >= REPLICATE_WARN_EVERY) && since.elapsed() >= REPLICATE_WARN_AFTER {
                        tracing::warn!(error = %e, partition = wal.partition().0, %addr, failing_for = ?since.elapsed(), "replication: the replica refuses records; this partition's writes wait");
                        warned_at = Some(std::time::Instant::now());
                    } else {
                        tracing::debug!(error = %e, %addr, "replication: retrying");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
            }
        }
    }
}

#[async_trait]
impl Log for ReplicatedLog {
    fn partition(&self) -> PartitionId {
        self.wal.partition()
    }

    fn floor_commit(&self) -> Option<(LogOffset, adb_core::log::CommitTs)> {
        self.wal.floor_commit()
    }

    async fn append(&self, record: LogRecord) -> LogResult<LogOffset> {
        self.submit(record).await?.await
    }

    async fn append_on_new_segment(&self, record: LogRecord) -> LogResult<LogOffset> {
        let at = record.offset.0;
        let local = self.wal.append_on_new_segment(record).await?;
        let _ = self.queue.send(at);
        self.wait_acked(at).await?;
        Ok(local)
    }

    /// The local fsync and the replica's answer at the same time.
    async fn submit(&self, record: LogRecord) -> LogResult<CommitWait> {
        if let Some(e) = self.fenced.lock().unwrap().clone() {
            return Err(LogError::Io(e));
        }
        let at = record.offset.0;
        let local = self.wal.submit(record).await?;
        let _ = self.queue.send(at);
        let replica = self.wait_acked(at);
        Ok(Box::pin(async move {
            let (l, r) = tokio::join!(local, replica);
            r?;
            l
        }))
    }

    async fn read(&self, from: LogOffset, max: usize) -> LogResult<Vec<LogRecord>> {
        self.wal.read(from, max).await
    }

    fn last_offset(&self) -> LogOffset {
        self.wal.last_offset()
    }

    /// Durable on both: here and on the replica.
    fn durable_offset(&self) -> LogOffset {
        LogOffset(self.wal.durable_offset().0.min(self.acked()))
    }

    async fn purge_below(&self, offset: LogOffset) -> LogResult<()> {
        self.wal.purge_below(offset).await
    }

    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize> {
        self.wal.rewrite_closed(redact).await
    }

    fn note_shipped(&self, offset: LogOffset) {
        self.shipped.fetch_max(offset.0, std::sync::atomic::Ordering::SeqCst);
        // Wake the task: the replica may drop what object storage now holds.
        let _ = self.queue.send(0);
    }
}

// ---------------------------------------------------------------- the replica

/// One partition's buffer on its replica.
#[derive(Default)]
struct Buffer {
    epoch: u64,
    /// Object storage holds the log up to here (the owner says).
    shipped: u64,
    /// The last record held.
    held: u64,
    /// Records in memory, oldest first, encoded.
    mem: VecDeque<(u64, Vec<u8>)>,
    mem_bytes: u64,
    /// Older records on disk: (first, last) offsets, bytes.
    spilled: Option<(u64, u64)>,
    spill_bytes: u64,
    /// Records have arrived from this epoch's owner with no gap: everything it
    /// acknowledged is here (before that, it may hold some it hasn't sent here yet).
    synced: bool,
    /// This node took the partition over at this epoch ([`ReplicaBuffers::seal`]):
    /// records of older epochs are refused from then on, whatever the map says.
    sealed: u64,
}

/// What a node keeps as the replica of other nodes' partitions.
pub struct ReplicaBuffers {
    dir: PathBuf,
    mem_limit: u64,
    spill_limit: u64,
    parts: Mutex<HashMap<Partition, Buffer>>,
}

impl ReplicaBuffers {
    /// Spill files under `dir` (`replica-p{n}.spill`), limits from the environment.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self::with_limits(dir, env_u64("ADB_REPLICA_MEM_BYTES", 100 << 20), env_u64("ADB_REPLICA_SPILL_MAX_BYTES", 4 << 30))
    }

    /// With explicit limits: `mem` bytes in memory, `spill` bytes on disk.
    pub fn with_limits(dir: impl Into<PathBuf>, mem: u64, spill: u64) -> Self {
        Self { dir: dir.into(), mem_limit: mem, spill_limit: spill, parts: Default::default() }
    }

    fn spill_path(&self, p: Partition) -> PathBuf {
        self.dir.join(format!("replica-p{p}.spill"))
    }

    /// Records of `epoch` for partition `p` (`current_epoch`: the partition's epoch in
    /// this node's map). See the module docs for the answers.
    pub fn append(&self, p: Partition, current_epoch: u64, req: ReplicateReq) -> Result<ReplicateAck, String> {
        if req.epoch < current_epoch {
            return Err(format!("fenced: partition {p} is at epoch {current_epoch}; epoch {} is stale", req.epoch));
        }
        let mut parts = self.parts.lock().unwrap();
        let b = parts.entry(p).or_default();
        // Taken over here: the old owner's records can no longer be acknowledged (the
        // map this call was checked against may be older than the takeover).
        if req.epoch < b.sealed {
            return Err(format!("fenced: partition {p} was taken over here at epoch {}; epoch {} is stale", b.sealed, req.epoch));
        }
        if req.epoch > b.epoch {
            // A new owner: what an older one sent is replaced by what this one holds.
            *b = Buffer { epoch: req.epoch, sealed: b.sealed, ..Default::default() };
            let _ = std::fs::remove_file(self.spill_path(p));
        }
        if req.epoch < b.epoch {
            return Err(format!("fenced: partition {p}'s replica follows epoch {}; epoch {} is stale", b.epoch, req.epoch));
        }
        b.shipped = b.shipped.max(req.shipped);
        // An empty buffer starts right after what object storage holds.
        if b.held < b.shipped {
            b.held = b.held.max(b.shipped);
        }
        if let Some(first) = req.records.first() {
            let first = first.offset.0;
            if first > b.held + 1 {
                return Err(format!("gap:{}: partition {p}'s replica holds up to {}; sent from {first}", b.held, b.held));
            }
        }
        if !req.records.is_empty() {
            b.synced = true;
        }
        for r in req.records {
            if r.offset.0 <= b.held {
                continue;
            }
            let bytes = rmp_serde::to_vec_named(&r).map_err(|e| e.to_string())?;
            b.mem_bytes += bytes.len() as u64;
            b.mem.push_back((r.offset.0, bytes));
            b.held = r.offset.0;
        }
        // Drop what object storage holds.
        while b.mem.front().is_some_and(|(o, _)| *o <= b.shipped) {
            let (_, bytes) = b.mem.pop_front().unwrap();
            b.mem_bytes -= bytes.len() as u64;
        }
        if b.spilled.is_some_and(|(_, last)| last <= b.shipped) {
            b.spilled = None;
            b.spill_bytes = 0;
            let _ = std::fs::remove_file(self.spill_path(p));
        }
        // Past the memory limit: the oldest records to disk, fsynced.
        if b.mem_bytes > self.mem_limit {
            if b.spill_bytes > self.spill_limit {
                return Err(format!("replica_full: partition {p}'s replica spilled {} bytes (ADB_REPLICA_SPILL_MAX_BYTES); object storage is behind", b.spill_bytes));
            }
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(self.spill_path(p)).map_err(|e| format!("replica spill: {e}"))?;
            while b.mem_bytes > self.mem_limit / 2 {
                let Some((o, bytes)) = b.mem.pop_front() else { break };
                f.write_all(&(bytes.len() as u32).to_le_bytes()).and_then(|_| f.write_all(&bytes)).map_err(|e| format!("replica spill: {e}"))?;
                b.mem_bytes -= bytes.len() as u64;
                b.spill_bytes += 4 + bytes.len() as u64;
                b.spilled = Some((b.spilled.map(|(f, _)| f).unwrap_or(o), o));
            }
            f.sync_data().map_err(|e| format!("replica spill: {e}"))?;
        }
        Ok(ReplicateAck { held: b.held })
    }

    /// What partition `p`'s replica holds above object storage (a takeover, step 7):
    /// the spilled records, then those in memory.
    pub fn tail(&self, p: Partition) -> Result<(u64, Vec<LogRecord>), String> {
        let parts = self.parts.lock().unwrap();
        let Some(b) = parts.get(&p) else { return Ok((0, Vec::new())) };
        let mut out = Vec::new();
        if b.spilled.is_some() {
            let bytes = std::fs::read(self.spill_path(p)).map_err(|e| format!("replica spill: {e}"))?;
            let mut at = 0;
            while at + 4 <= bytes.len() {
                let n = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
                let r: LogRecord = rmp_serde::from_slice(&bytes[at + 4..at + 4 + n]).map_err(|e| e.to_string())?;
                if r.offset.0 > b.shipped {
                    out.push(r);
                }
                at += 4 + n;
            }
        }
        for (_, bytes) in &b.mem {
            let r: LogRecord = rmp_serde::from_slice(bytes).map_err(|e| e.to_string())?;
            if r.offset.0 > b.shipped {
                out.push(r);
            }
        }
        Ok((b.epoch, out))
    }

    /// The last record held for partition `p`, once the buffer is synced with its owner
    /// (see `synced`): every acknowledged write is at or below it.
    pub fn held(&self, p: Partition) -> Option<u64> {
        self.parts.lock().unwrap().get(&p).filter(|b| b.synced).map(|b| b.held)
    }

    /// Up to `max` records of partition `p` from offset `from`, when memory holds them
    /// (empty past the last one held); `None` when it doesn't (spilled, on object
    /// storage, or not synced): the caller asks the owner.
    pub fn tail_from(&self, p: Partition, from: u64, max: usize) -> Option<Vec<LogRecord>> {
        let parts = self.parts.lock().unwrap();
        let b = parts.get(&p).filter(|b| b.synced)?;
        if from > b.held {
            return Some(Vec::new());
        }
        let front = b.mem.front()?.0;
        if from < front {
            return None;
        }
        let skip = (from - front) as usize;
        let mut out = Vec::with_capacity(max.min(b.mem.len().saturating_sub(skip)));
        for (o, bytes) in b.mem.iter().skip(skip).take(max) {
            debug_assert!(*o >= from);
            out.push(rmp_serde::from_slice(bytes).ok()?);
        }
        Some(out)
    }

    /// Take partition `p` over at `epoch`: from now on no record of an older epoch is
    /// accepted (an append that checked an older map is refused here, under the same
    /// lock), and what is held now is returned — every record this replica ever
    /// acknowledged for the old owner. Read the tail any other way and an append landing
    /// after the read is acknowledged yet missing from the new owner's log (Hetzner
    /// round 6: 22 acknowledged writes lost).
    pub fn seal(&self, p: Partition, epoch: u64) -> Result<(u64, Vec<LogRecord>), String> {
        {
            let mut parts = self.parts.lock().unwrap();
            let b = parts.entry(p).or_default();
            b.sealed = b.sealed.max(epoch);
        }
        self.tail(p)
    }

    /// Forget partition `p`'s buffer (this node took the partition over: its tail is in
    /// the new owner's log now). The seal stays.
    pub fn clear(&self, p: Partition) {
        let mut parts = self.parts.lock().unwrap();
        if let Some(b) = parts.get_mut(&p) {
            *b = Buffer { sealed: b.sealed, ..Default::default() };
        }
        drop(parts);
        let _ = std::fs::remove_file(self.spill_path(p));
    }

    /// (records in memory, bytes in memory, bytes spilled) for partition `p`.
    pub fn usage(&self, p: Partition) -> (usize, u64, u64) {
        let parts = self.parts.lock().unwrap();
        parts.get(&p).map(|b| (b.mem.len(), b.mem_bytes, b.spill_bytes)).unwrap_or_default()
    }
}

#[cfg(test)]
mod purged_wal_tests {
    use super::*;
    use adb_core::log::{CommitTs, Epoch, LogOffset, LogRecord, PartitionId};

    fn rec(o: u64) -> LogRecord {
        LogRecord { partition: PartitionId(3), offset: LogOffset(o), epoch: Epoch(1), commit_ts: CommitTs(o as i64), txn_id: None, effects: vec![] }
    }

    struct Buffers(ReplicaBuffers);

    #[async_trait]
    impl ReplicaTarget for Buffers {
        fn target(&self) -> Option<(String, u64)> {
            Some(("replica".into(), 1))
        }
        async fn replicate(&self, _addr: &str, req: ReplicateReq) -> Result<ReplicateAck, String> {
            self.0.append(3, 1, req)
        }
    }

    /// An owner whose WAL starts above a purged range (a partition restored from
    /// object storage, or purged after archiving) still gets its writes acknowledged
    /// by a new replica: the purged range counts as shipped.
    #[tokio::test]
    async fn a_new_replica_of_a_purged_wal_acknowledges_writes() {
        let wal = Arc::new(adb_log::MemLog::new(PartitionId(3)));
        for o in 1..=10 {
            wal.append(rec(o)).await.unwrap();
        }
        wal.purge_below(LogOffset(6)).await.unwrap();
        assert!(matches!(wal.read(LogOffset(1), 1).await, Err(adb_log::LogError::Purged { .. })));
        let dir = tempfile::tempdir().unwrap();
        let replica = Arc::new(Buffers(ReplicaBuffers::with_limits(dir.path(), 1 << 20, 1 << 24)));
        let log = ReplicatedLog::new(wal as Arc<dyn Log>, Arc::clone(&replica) as Arc<dyn ReplicaTarget>);
        tokio::time::timeout(Duration::from_secs(5), log.append(rec(11))).await.expect("the write is acknowledged, not stalled").unwrap();
        assert!(log.acked() >= 11);
        let (_, tail) = replica.0.tail(3).unwrap();
        assert_eq!(tail.iter().map(|r| r.offset.0).collect::<Vec<_>>(), (6..=11).collect::<Vec<_>>(), "the replica holds what isn't shipped");
    }
}

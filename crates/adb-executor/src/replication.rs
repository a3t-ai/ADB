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
                match wal.read(LogOffset(held + 1), 1024).await {
                    Ok(r) => r,
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
                    // The replica's answer is what it holds.
                    held = ack.held;
                    let _ = upto;
                    acked.send_if_modified(|a| {
                        let moved = held > *a;
                        *a = (*a).max(held);
                        moved
                    });
                    if held >= wal.last_offset().0 {
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
                    tracing::debug!(error = %e, %addr, "replication: retrying");
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
        if req.epoch > b.epoch {
            // A new owner: what an older one sent is replaced by what this one holds.
            *b = Buffer { epoch: req.epoch, ..Default::default() };
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

    /// (records in memory, bytes in memory, bytes spilled) for partition `p`.
    pub fn usage(&self, p: Partition) -> (usize, u64, u64) {
        let parts = self.parts.lock().unwrap();
        parts.get(&p).map(|b| (b.mem.len(), b.mem_bytes, b.spill_bytes)).unwrap_or_default()
    }
}

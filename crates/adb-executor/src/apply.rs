//! The Lance applier as a background task, and the read barrier that keeps
//! read-your-writes (stage D3.7).
//!
//! Writes return once a record is in the log and the in-memory tier; the record is
//! then queued, in log order, to one task that applies it to the partition's Lance
//! table. A read served from Lance first waits until the table has applied every
//! record committed before the read started ([`ApplyBarrier::wait_for_committed`]),
//! so it sees every write that returned before it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use adb_core::log::LogRecord;
use adb_store::PartitionStore;
use tokio::sync::{mpsc, watch};

/// Committed vs applied-to-Lance log offsets.
#[derive(Debug)]
pub struct ApplyBarrier {
    committed: AtomicU64,
    applied: watch::Sender<u64>,
    /// Highest offset the log has made durable (committed by the replica set). With
    /// pipelined commits (PF-1) a record is queued before it is durable; the applier
    /// waits for this, so no read sees a write that might not commit.
    durable: watch::Sender<u64>,
}

impl ApplyBarrier {
    /// Both offsets start at `start` (the table is caught up with the log on open).
    pub fn new(start: u64) -> Arc<Self> {
        Arc::new(Self { committed: AtomicU64::new(start), applied: watch::Sender::new(start), durable: watch::Sender::new(start) })
    }

    /// A record at `offset` was committed (called in log order, before queueing it).
    pub fn committed(&self, offset: u64) {
        self.committed.fetch_max(offset, Ordering::SeqCst);
    }

    /// The log made every record up to `offset` durable (log order: a later one
    /// committed means the earlier ones are too).
    pub fn durable(&self, offset: u64) {
        self.durable.send_modify(|d| *d = (*d).max(offset));
    }

    /// A submitted record will not commit (PF-1): readers waiting for it stop
    /// waiting; the engine restarts and rebuilds from the committed log.
    pub fn abandon(&self) {
        let committed = self.committed.load(Ordering::SeqCst);
        self.applied(committed);
    }

    /// The applier finished the record at `offset` (applied, or failed and left to
    /// the next open's catch-up — either way readers must not wait for it forever).
    fn applied(&self, offset: u64) {
        self.applied.send_modify(|a| *a = (*a).max(offset));
    }

    /// Follows the applied offset (stage E subscribers wait on it).
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.applied.subscribe()
    }

    pub fn applied_offset(&self) -> u64 {
        *self.applied.borrow()
    }

    /// Wait until the table has applied every record up to `offset`.
    pub async fn wait_for_applied(&self, offset: u64) {
        let mut rx = self.applied.subscribe();
        let _ = rx.wait_for(|a| *a >= offset).await;
    }

    /// Wait until the table has applied every record committed before this call.
    pub async fn wait_for_committed(&self) {
        let target = self.committed.load(Ordering::SeqCst);
        let mut rx = self.applied.subscribe();
        // The sender lives in `self`, so the channel can't close while we wait.
        let _ = rx.wait_for(|a| *a >= target).await;
    }
}

/// Most queued records the applier puts into the table at once.
const APPLY_BATCH: usize = 256;

/// How long the applier waits to gather records into one memtable batch
/// (`ADB_APPLY_GATHER_MS`, default 0 = put whatever is queued at once). Measured 7 Nov:
/// while UPDATE / FORGET wait for the applier under the sequencer, 10 ms made them
/// slower and did not speed up merged reads; revisit after PF-1.
fn gather_window() -> std::time::Duration {
    std::time::Duration::from_millis(std::env::var("ADB_APPLY_GATHER_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
}

/// Queue of committed records for the background applier.
#[derive(Debug, Clone)]
pub struct Applier {
    tx: mpsc::UnboundedSender<LogRecord>,
    barrier: Arc<ApplyBarrier>,
}

impl Applier {
    /// Start the task applying queued records to `store`, in queue order.
    pub fn spawn(store: Arc<PartitionStore>, barrier: Arc<ApplyBarrier>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<LogRecord>();
        let task_barrier = Arc::clone(&barrier);
        let mut durable = barrier.durable.subscribe();
        tokio::spawn(async move {
            // Whatever is queued goes in as one put (W7: fewer, larger batches).
            let mut batch = Vec::with_capacity(APPLY_BATCH);
            let gather = gather_window();
            while rx.recv_many(&mut batch, APPLY_BATCH).await > 0 {
                // Records arriving one at a time would each be a memtable batch, and a
                // read walks every batch: wait briefly for more (PF-9). Nothing is at
                // risk meanwhile: the log keeps every record until Lance has flushed it.
                if !gather.is_zero() && batch.len() < APPLY_BATCH {
                    let until = tokio::time::Instant::now() + gather;
                    while batch.len() < APPLY_BATCH {
                        let room = APPLY_BATCH - batch.len();
                        match tokio::time::timeout_at(until, rx.recv_many(&mut batch, room)).await {
                            Ok(n) if n > 0 => {}
                            _ => break,
                        }
                    }
                }
                let last = batch.last().map_or(0, |r| r.offset.0);
                // Only what the log has committed reaches the table (PF-1).
                let t0 = std::time::Instant::now();
                let _ = durable.wait_for(|d| *d >= last).await;
                crate::steps::add("applier.1 wait durable", t0.elapsed());
                let t1 = std::time::Instant::now();
                let applied = store.apply_many(&batch).await;
                crate::steps::add("applier.2 apply batch", t1.elapsed());
                crate::steps::add(match batch.len() { 0..=1 => "applier.n batch 1", 2..=16 => "applier.n batch 2-16", 17..=128 => "applier.n batch 17-128", _ => "applier.n batch 129+" }, std::time::Duration::ZERO);
                if let Err(e) = applied {
                    tracing::warn!(first = batch[0].offset.0, last, error = %e, "Lance apply failed; the log has it, catch-up on next open");
                }
                task_barrier.applied(last);
                batch.clear();
            }
        });
        Self { tx, barrier }
    }

    /// Queue a committed record (call with the sequencer held, so in log order).
    pub fn enqueue(&self, record: LogRecord) {
        self.barrier.durable(record.offset.0);
        self.enqueue_submitted(record);
    }

    /// Queue a record that is submitted but maybe not yet durable (PF-1): it waits in
    /// the queue for [`ApplyBarrier::durable`]. Call with the sequencer held.
    pub fn enqueue_submitted(&self, record: LogRecord) {
        self.barrier.committed(record.offset.0);
        let offset = record.offset.0;
        if self.tx.send(record).is_err() {
            // The task is gone (runtime shutting down): don't make readers wait.
            tracing::warn!(offset, "Lance applier stopped; record left to the next open's catch-up");
            self.barrier.applied(offset);
        }
    }

    pub fn barrier(&self) -> &Arc<ApplyBarrier> {
        &self.barrier
    }
}

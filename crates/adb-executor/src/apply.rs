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
}

impl ApplyBarrier {
    /// Both offsets start at `start` (the table is caught up with the log on open).
    pub fn new(start: u64) -> Arc<Self> {
        Arc::new(Self { committed: AtomicU64::new(start), applied: watch::Sender::new(start) })
    }

    /// A record at `offset` was committed (called in log order, before queueing it).
    pub fn committed(&self, offset: u64) {
        self.committed.fetch_max(offset, Ordering::SeqCst);
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

    /// Wait until the table has applied every record committed before this call.
    pub async fn wait_for_committed(&self) {
        let target = self.committed.load(Ordering::SeqCst);
        let mut rx = self.applied.subscribe();
        // The sender lives in `self`, so the channel can't close while we wait.
        let _ = rx.wait_for(|a| *a >= target).await;
    }
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
        tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                if let Err(e) = store.apply(&record).await {
                    tracing::warn!(offset = record.offset.0, error = %e, "Lance apply failed; the log has it, catch-up on next open");
                }
                task_barrier.applied(record.offset.0);
            }
        });
        Self { tx, barrier }
    }

    /// Queue a committed record (call with the sequencer held, so in log order).
    pub fn enqueue(&self, record: LogRecord) {
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

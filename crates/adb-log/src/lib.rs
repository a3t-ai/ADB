//! The partition log (ARCHITECTURE.md §9, §11; DELIVERY_PLAN.md stage C).
//!
//! Every mutation is appended here before it is applied; the Lance table is a
//! materialized view of the log. One trait, several implementations:
//!
//! - [`MemLog`] — in memory, for tests and the in-process engine (here).
//! - [`wal::LocalWal`] — segment files on local disk, `fdatasync`, group commit.
//! - openraft replicated log (M3) and object-store commit (§11.5), behind the same
//!   trait.

use adb_core::log::{Epoch, LogOffset, LogRecord, PartitionId};
use async_trait::async_trait;
use parking_lot::Mutex;

pub mod wal;
pub use wal::{LocalWal, WalConfig};

#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub enum LogError {
    #[error("record offset {got:?} out of order: expected {expected:?}")]
    OutOfOrder { expected: LogOffset, got: LogOffset },
    #[error("record for partition {got:?} appended to the log of {log:?}")]
    WrongPartition { log: PartitionId, got: PartitionId },
    #[error("stale epoch {got:?}: the log is at {current:?}")]
    StaleEpoch { current: Epoch, got: Epoch },
    #[error("offset {requested:?} was purged; the oldest retained is {oldest:?}")]
    Purged { requested: LogOffset, oldest: LogOffset },
    #[error("log I/O: {0}")]
    Io(String),
    /// Another `LocalWal` holds the directory (another ADB process, or a log being
    /// closed in this one).
    #[error("{0} is in use by another ADB process")]
    Locked(String),
}

pub type LogResult<T> = Result<T, LogError>;

/// One partition's log.
#[async_trait]
pub trait Log: Send + Sync {
    fn partition(&self) -> PartitionId;

    /// Append one record. Its offset must be `last_offset() + 1` and its epoch at
    /// least the highest seen (a stale owner is fenced, §11.4). Returns once the
    /// record is durable: only then may the write be acknowledged.
    async fn append(&self, record: LogRecord) -> LogResult<LogOffset>;

    /// As [`Log::append`], starting a new segment with this record, so everything
    /// before it can be purged whole (W5: erasing a forgotten record's payload).
    /// Logs without segments just append.
    async fn append_on_new_segment(&self, record: LogRecord) -> LogResult<LogOffset> {
        self.append(record).await
    }

    /// Up to `max` records from `from` (inclusive), in offset order.
    async fn read(&self, from: LogOffset, max: usize) -> LogResult<Vec<LogRecord>>;

    /// Highest appended offset, or `LogOffset::ZERO` for an empty log.
    fn last_offset(&self) -> LogOffset;

    /// Highest durable offset. Equal to `last_offset` for logs whose `append` waits
    /// for durability.
    fn durable_offset(&self) -> LogOffset;

    /// Drop records below `offset` (retention below the applied offset, §11.3).
    async fn purge_below(&self, offset: LogOffset) -> LogResult<()>;

    /// Rewrite the records of the closed segments (all but the one being appended
    /// to) with `redact`, which returns whether it changed a record; offsets stay.
    /// W5: a forgotten record's payload leaves the log without disturbing anyone's
    /// position in it. Returns how many records changed.
    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize>;
}

/// A shared log: the sequencer appends while readers (stage E subscribers) read the
/// same log. `LocalWal` and `MemLog` synchronise internally.
#[async_trait]
impl<L: Log + ?Sized> Log for std::sync::Arc<L> {
    fn partition(&self) -> PartitionId {
        (**self).partition()
    }

    async fn append(&self, record: LogRecord) -> LogResult<LogOffset> {
        (**self).append(record).await
    }

    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize> {
        (**self).rewrite_closed(redact).await
    }

    async fn append_on_new_segment(&self, record: LogRecord) -> LogResult<LogOffset> {
        (**self).append_on_new_segment(record).await
    }

    async fn read(&self, from: LogOffset, max: usize) -> LogResult<Vec<LogRecord>> {
        (**self).read(from, max).await
    }

    fn last_offset(&self) -> LogOffset {
        (**self).last_offset()
    }

    fn durable_offset(&self) -> LogOffset {
        (**self).durable_offset()
    }

    async fn purge_below(&self, offset: LogOffset) -> LogResult<()> {
        (**self).purge_below(offset).await
    }
}

/// An in-memory log: durable for the life of the process only.
#[derive(Debug)]
pub struct MemLog {
    partition: PartitionId,
    state: Mutex<MemState>,
}

#[derive(Debug, Default)]
struct MemState {
    /// Offset of `records[0]`.
    first: LogOffset,
    records: Vec<LogRecord>,
    last: LogOffset,
    epoch: Epoch,
}

impl MemLog {
    pub fn new(partition: PartitionId) -> Self {
        Self {
            partition,
            state: Mutex::new(MemState { first: LogOffset(1), ..Default::default() }),
        }
    }
}

#[async_trait]
impl Log for MemLog {
    fn partition(&self) -> PartitionId {
        self.partition
    }

    async fn append(&self, record: LogRecord) -> LogResult<LogOffset> {
        if record.partition != self.partition {
            return Err(LogError::WrongPartition { log: self.partition, got: record.partition });
        }
        let mut s = self.state.lock();
        if record.epoch < s.epoch {
            return Err(LogError::StaleEpoch { current: s.epoch, got: record.epoch });
        }
        let expected = s.last.next();
        if record.offset != expected {
            return Err(LogError::OutOfOrder { expected, got: record.offset });
        }
        s.epoch = record.epoch;
        s.last = record.offset;
        s.records.push(record);
        Ok(expected)
    }

    async fn read(&self, from: LogOffset, max: usize) -> LogResult<Vec<LogRecord>> {
        let s = self.state.lock();
        if from < s.first {
            return Err(LogError::Purged { requested: from, oldest: s.first });
        }
        let start = (from.0 - s.first.0) as usize;
        Ok(s.records.iter().skip(start).take(max).cloned().collect())
    }

    fn last_offset(&self) -> LogOffset {
        self.state.lock().last
    }

    fn durable_offset(&self) -> LogOffset {
        self.last_offset()
    }

    async fn purge_below(&self, offset: LogOffset) -> LogResult<()> {
        let mut s = self.state.lock();
        let offset = offset.min(s.last.next());
        if offset > s.first {
            let n = (offset.0 - s.first.0) as usize;
            s.records.drain(..n);
            s.first = offset;
        }
        Ok(())
    }

    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize> {
        let mut s = self.state.lock();
        let n = s.records.len().saturating_sub(1);
        Ok(s.records[..n].iter_mut().map(|r| redact(r)).filter(|changed| *changed).count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::log::{CommitTs, Effect, IdGen};

    fn record(offset: u64, epoch: u64) -> LogRecord {
        let mut ids = IdGen::new();
        LogRecord {
            partition: PartitionId(1),
            offset: LogOffset(offset),
            epoch: Epoch(epoch),
            commit_ts: CommitTs(offset as i64),
            txn_id: None,
            effects: vec![Effect::Forget { memory: adb_core::log::Memory::Working, lineage_id: ids.next(CommitTs(offset as i64)) }],
        }
    }

    #[tokio::test]
    async fn appends_in_order_and_reads_back() {
        let log = MemLog::new(PartitionId(1));
        for i in 1..=5 {
            assert_eq!(log.append(record(i, 1)).await.unwrap(), LogOffset(i));
        }
        let got = log.read(LogOffset(2), 2).await.unwrap();
        assert_eq!(got.iter().map(|r| r.offset.0).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(log.last_offset(), LogOffset(5));
        assert_eq!(log.durable_offset(), LogOffset(5));
    }

    #[tokio::test]
    async fn rejects_gaps_and_stale_epochs() {
        let log = MemLog::new(PartitionId(1));
        log.append(record(1, 2)).await.unwrap();
        assert_eq!(
            log.append(record(3, 2)).await,
            Err(LogError::OutOfOrder { expected: LogOffset(2), got: LogOffset(3) })
        );
        assert_eq!(
            log.append(record(2, 1)).await,
            Err(LogError::StaleEpoch { current: Epoch(2), got: Epoch(1) })
        );
        let mut other = record(2, 2);
        other.partition = PartitionId(9);
        assert!(matches!(log.append(other).await, Err(LogError::WrongPartition { .. })));
    }

    #[tokio::test]
    async fn purge_drops_old_records() {
        let log = MemLog::new(PartitionId(1));
        for i in 1..=4 {
            log.append(record(i, 1)).await.unwrap();
        }
        log.purge_below(LogOffset(3)).await.unwrap();
        assert_eq!(
            log.read(LogOffset(2), 10).await,
            Err(LogError::Purged { requested: LogOffset(2), oldest: LogOffset(3) })
        );
        assert_eq!(log.read(LogOffset(3), 10).await.unwrap().len(), 2);
        // Appending continues after a purge.
        log.append(record(5, 1)).await.unwrap();
    }
}

//! The per-partition sequencer (ARCHITECTURE.md §8, §9; DELIVERY_PLAN.md stage C).
//!
//! A sequencer is state on one core's executor, never a thread: it orders a
//! partition's writes, stamps them with the commit clock and appends them to the log.
//! This skeleton has the parts later stages build on:
//!
//! - the commit clock (D2_TEMPORAL.md T2): strictly increasing, the only time source;
//! - contiguous offsets and the owner's epoch on every record;
//! - ids generated from the commit timestamp (D3_SCHEMA.md S1);
//! - `txn_id` dedupe, so a retried write commits once.
//!
//! Still to come in stage C: validation (existence, uniqueness, references, OCC read
//! sets), the pending overlay, group commit, and the WORKING / TOOLS hot tier.

use std::collections::HashMap;

use adb_core::log::{CommitTs, Effect, Epoch, IdGen, LogOffset, LogRecord, RecordId};
use adb_log::{Log, LogResult};

/// The commit clock: `max(wall clock, previous + 1 µs)` (D2 §1).
#[derive(Debug, Default)]
pub struct CommitClock {
    last: Option<CommitTs>,
}

impl CommitClock {
    /// Resume after the last committed timestamp (on open or failover).
    pub fn after(last: CommitTs) -> Self {
        Self { last: Some(last) }
    }

    pub fn tick(&mut self, wall: CommitTs) -> CommitTs {
        let ts = match self.last {
            Some(last) if wall <= last => CommitTs(last.0 + 1),
            _ => wall,
        };
        self.last = Some(ts);
        ts
    }
}

/// A transaction being prepared: its commit timestamp is fixed, so ids made for it
/// carry that time and replay recreates them exactly.
#[derive(Debug)]
pub struct Txn<'a> {
    commit_ts: CommitTs,
    ids: &'a mut IdGen,
}

impl Txn<'_> {
    pub fn commit_ts(&self) -> CommitTs {
        self.commit_ts
    }

    pub fn new_id(&mut self) -> RecordId {
        self.ids.next(self.commit_ts)
    }
}

/// Orders one partition's writes onto its log.
pub struct Sequencer<L: Log> {
    log: L,
    epoch: Epoch,
    clock: CommitClock,
    ids: IdGen,
    /// Committed `txn_id`s and their offsets. Unbounded here; stage C keeps a window.
    seen: HashMap<String, LogOffset>,
    wall: fn() -> CommitTs,
    /// The last offset submitted: ahead of `log.last_offset()` while commits are in
    /// flight (PF-1), so the next record still takes the next offset.
    submitted: LogOffset,
}

fn wall_clock() -> CommitTs {
    CommitTs::from_datetime(chrono::Utc::now())
}

impl<L: Log> Sequencer<L> {
    /// A sequencer for the owner of `log` at `epoch`.
    pub fn new(log: L, epoch: Epoch) -> Self {
        Self::with_wall_clock(log, epoch, wall_clock)
    }

    /// As [`Sequencer::new`] with an injected wall clock (tests).
    pub fn with_wall_clock(log: L, epoch: Epoch, wall: fn() -> CommitTs) -> Self {
        Self { log, epoch, clock: CommitClock::default(), ids: IdGen::new(), seen: HashMap::new(), wall, submitted: LogOffset::ZERO }
    }

    pub fn log(&self) -> &L {
        &self.log
    }

    /// Prepare and commit one transaction. `build` receives the transaction (commit
    /// time and id generator) and returns its effects. A `txn_id` already committed
    /// returns its original offset without writing again.
    pub async fn commit<F>(&mut self, txn_id: Option<String>, build: F) -> LogResult<LogOffset>
    where
        F: FnOnce(&mut Txn<'_>) -> Vec<Effect>,
    {
        if let Some(offset) = txn_id.as_ref().and_then(|id| self.seen.get(id)) {
            return Ok(*offset);
        }
        let commit_ts = self.begin();
        let effects = build(&mut Txn { commit_ts, ids: &mut self.ids });
        self.append(commit_ts, txn_id, effects).await
    }

    /// Two-phase commit for writes that must read state at their commit time first
    /// (an `UPDATE` resolving its targets): `begin` fixes the commit timestamp, the
    /// caller prepares effects as of that time, then [`Sequencer::append`] logs them.
    /// The caller holds the sequencer throughout, so no other write interleaves.
    pub fn begin(&mut self) -> CommitTs {
        self.clock.tick((self.wall)())
    }

    /// A new id carrying `at` (a timestamp from [`Sequencer::begin`]).
    pub fn new_id(&mut self, at: CommitTs) -> RecordId {
        self.ids.next(at)
    }

    /// Whether `txn_id` already committed, and at which offset.
    pub fn committed(&self, txn_id: &str) -> Option<LogOffset> {
        self.seen.get(txn_id).copied()
    }

    /// Log a prepared transaction at `commit_ts`. Resolves once it is durable.
    pub async fn append(&mut self, commit_ts: CommitTs, txn_id: Option<String>, effects: Vec<Effect>) -> LogResult<LogOffset> {
        self.append_record(commit_ts, txn_id, effects).await.map(|r| r.offset)
    }

    /// As [`Sequencer::append`], returning the logged record (for appliers).
    pub async fn append_record(&mut self, commit_ts: CommitTs, txn_id: Option<String>, effects: Vec<Effect>) -> LogResult<LogRecord> {
        let (record, wait) = self.submit_record(commit_ts, txn_id, effects).await?;
        wait.await?;
        Ok(record)
    }

    /// The offset the next record takes: after everything submitted, committed or not.
    fn next_offset(&self) -> LogOffset {
        self.log.last_offset().max(self.submitted).next()
    }

    /// As [`Sequencer::append_record`], returning once the record is submitted, in
    /// order, with its commit to await (PF-1): the caller releases the sequencer first.
    pub async fn submit_record(&mut self, commit_ts: CommitTs, txn_id: Option<String>, effects: Vec<Effect>) -> LogResult<(LogRecord, adb_log::CommitWait)> {
        let record = LogRecord {
            partition: self.log.partition(),
            offset: self.next_offset(),
            epoch: self.epoch,
            commit_ts,
            txn_id: txn_id.clone(),
            effects,
        };
        let wait = self.log.submit(record.clone()).await?;
        self.submitted = record.offset;
        if let Some(id) = txn_id {
            self.seen.insert(id, record.offset);
        }
        Ok((record, wait))
    }

    /// As [`Sequencer::append_record`], on a new log segment (W5: after a
    /// compaction, so everything before it can be purged whole).
    pub async fn append_record_on_new_segment(&mut self, commit_ts: CommitTs, effects: Vec<Effect>) -> LogResult<LogRecord> {
        let record = LogRecord {
            partition: self.log.partition(),
            offset: self.next_offset(),
            epoch: self.epoch,
            commit_ts,
            txn_id: None,
            effects,
        };
        self.log.append_on_new_segment(record.clone()).await?;
        self.submitted = record.offset;
        Ok(record)
    }

    /// Resume after replay: the clock continues after the last committed timestamp.
    pub fn resume_after(&mut self, last: CommitTs) {
        self.clock = CommitClock::after(last);
    }

    /// Recall the `txn_id`s of the last `window` records of the log, so a client
    /// retrying a write after a restart or a failover (a new leader) still commits
    /// it once (CA-27). Returns how many ids were recalled.
    pub async fn recall_txns(&mut self, window: u64) -> LogResult<usize> {
        let last = self.log.last_offset().0;
        if last == 0 || window == 0 {
            return Ok(0);
        }
        let mut from = last.saturating_sub(window - 1).max(1);
        let mut n = 0;
        while from <= last {
            let batch = match self.log.read(LogOffset(from), 4096).await {
                Ok(b) => b,
                // Purged below the window's start: the window starts at the oldest kept.
                Err(adb_log::LogError::Purged { oldest, .. }) if oldest.0 > from => {
                    from = oldest.0;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let Some(end) = batch.last().map(|r| r.offset.0) else { break };
            for r in batch {
                if let Some(id) = r.txn_id {
                    self.seen.insert(id, r.offset);
                    n += 1;
                }
            }
            from = end + 1;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::log::PartitionId;
    use adb_log::MemLog;

    #[test]
    fn clock_never_goes_backwards() {
        let mut clock = CommitClock::default();
        assert_eq!(clock.tick(CommitTs(100)), CommitTs(100));
        assert_eq!(clock.tick(CommitTs(100)), CommitTs(101)); // same instant
        assert_eq!(clock.tick(CommitTs(50)), CommitTs(102)); // wall clock stepped back
        assert_eq!(clock.tick(CommitTs(500)), CommitTs(500));
        let mut resumed = CommitClock::after(CommitTs(500));
        assert_eq!(resumed.tick(CommitTs(400)), CommitTs(501));
    }

    fn forget(txn: &mut Txn<'_>) -> Vec<Effect> {
        vec![Effect::Forget { memory: adb_core::log::Memory::Working, lineage_id: txn.new_id() }]
    }

    #[tokio::test]
    async fn commits_get_contiguous_offsets_and_increasing_times() {
        // A frozen wall clock: every commit still gets a later timestamp.
        let mut seq = Sequencer::with_wall_clock(MemLog::new(PartitionId(3)), Epoch(1), || CommitTs(1_000_000));
        for i in 1..=3 {
            assert_eq!(seq.commit(None, forget).await.unwrap(), LogOffset(i));
        }
        let records = seq.log().read(LogOffset(1), 10).await.unwrap();
        let times: Vec<i64> = records.iter().map(|r| r.commit_ts.0).collect();
        assert_eq!(times, vec![1_000_000, 1_000_001, 1_000_002]);
        assert!(records.iter().all(|r| r.epoch == Epoch(1) && r.partition == PartitionId(3)));
    }

    #[tokio::test]
    async fn ids_carry_the_commit_time() {
        let mut seq = Sequencer::with_wall_clock(MemLog::new(PartitionId(0)), Epoch(1), || CommitTs(1_761_300_000_000_000));
        let mut made = Vec::new();
        seq.commit(None, |txn| {
            made.push(txn.new_id());
            made.push(txn.new_id());
            forget(txn)
        })
        .await
        .unwrap();
        let expected = adb_core::log::IdGen::new().next(CommitTs(1_761_300_000_000_000));
        // Same millisecond prefix (ULID time part is the first 10 characters).
        assert_eq!(&made[0].0[..10], &expected.0[..10]);
        assert!(made[0] < made[1]);
    }

    #[tokio::test]
    async fn retried_txn_commits_once() {
        let mut seq = Sequencer::new(MemLog::new(PartitionId(0)), Epoch(1));
        let a = seq.commit(Some("txn-7".into()), forget).await.unwrap();
        let b = seq.commit(Some("txn-7".into()), forget).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(seq.log().last_offset(), LogOffset(1));
    }

    #[tokio::test]
    async fn a_new_sequencer_on_the_same_log_still_commits_a_retried_txn_once() {
        let log = std::sync::Arc::new(MemLog::new(PartitionId(0)));
        let mut first = Sequencer::new(std::sync::Arc::clone(&log), Epoch(1));
        let a = first.commit(Some("txn-9".into()), forget).await.unwrap();
        first.commit(None, forget).await.unwrap();
        drop(first);
        // A failover: a new sequencer over the same log, as a new leader opens it.
        let mut second = Sequencer::new(std::sync::Arc::clone(&log), Epoch(1));
        assert_eq!(second.recall_txns(1000).await.unwrap(), 1);
        let b = second.commit(Some("txn-9".into()), forget).await.unwrap();
        assert_eq!(a, b, "the retry answers with the first commit's offset");
        assert_eq!(second.log().last_offset(), LogOffset(2), "nothing new was logged");
        // A window that no longer covers it: committed again (bounded memory).
        let mut third = Sequencer::new(std::sync::Arc::clone(&log), Epoch(1));
        assert_eq!(third.recall_txns(1).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn stale_owner_is_fenced() {
        let log = MemLog::new(PartitionId(0));
        let mut new_owner = Sequencer::new(log, Epoch(2));
        new_owner.commit(None, forget).await.unwrap();
        // An old owner (epoch 1) writing to the same log is rejected by the log.
        let record = LogRecord {
            partition: PartitionId(0),
            offset: LogOffset(2),
            epoch: Epoch(1),
            commit_ts: CommitTs(0),
            txn_id: None,
            effects: vec![],
        };
        assert!(new_owner.log().append(record).await.is_err());
    }
}

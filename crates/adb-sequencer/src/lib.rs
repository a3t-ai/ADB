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
        Self { log, epoch, clock: CommitClock::default(), ids: IdGen::new(), seen: HashMap::new(), wall }
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
        if let Some(id) = &txn_id {
            if let Some(offset) = self.seen.get(id) {
                return Ok(*offset);
            }
        }
        let commit_ts = self.clock.tick((self.wall)());
        let effects = build(&mut Txn { commit_ts, ids: &mut self.ids });
        let record = LogRecord {
            partition: self.log.partition(),
            offset: self.log.last_offset().next(),
            epoch: self.epoch,
            commit_ts,
            txn_id: txn_id.clone(),
            effects,
        };
        let offset = self.log.append(record).await?;
        if let Some(id) = txn_id {
            self.seen.insert(id, offset);
        }
        Ok(offset)
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
        vec![Effect::Forget { lineage_id: txn.new_id() }]
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

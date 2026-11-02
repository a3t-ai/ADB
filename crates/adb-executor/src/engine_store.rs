//! FlowR's engine store traits over one ADB engine space (stage H): what the
//! Flight surface serves, what FlowR CE embeds, and what the conformance suite
//! checks. Every write goes through [`DurableWrites::engine_exec`] (decided on
//! the space's core under the sequencer, logged as one record); reads come from
//! the core, and a finished run's evicted journal from its EPISODIC history.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flowr_storage::engine::*;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::Value;

use adb_core::AdbError;
use crate::writes::DurableWrites;

/// One engine space of a durable ADB.
pub struct AdbEngineStore<L: adb_log::Log> {
    writes: Arc<DurableWrites<L>>,
    space: String,
}

impl<L: adb_log::Log> Clone for AdbEngineStore<L> {
    fn clone(&self) -> Self {
        Self { writes: Arc::clone(&self.writes), space: self.space.clone() }
    }
}

impl<L: adb_log::Log + 'static> AdbEngineStore<L> {
    pub fn new(writes: Arc<DurableWrites<L>>, space: impl Into<String>) -> Self {
        Self { writes, space: space.into() }
    }

    pub fn space(&self) -> &str {
        &self.space
    }

    /// Let a run's journal leave memory now (upkeep does it 5 minutes after a
    /// run ends); it is read back from its EPISODIC history.
    pub async fn evict_journal(&self, run: RunId) -> StoreResult<()> {
        self.exec(|c| {
            c.evict_journal(run);
            Ok(())
        })
        .await
    }

    async fn exec<T>(&self, f: impl FnOnce(&mut flowr_storage::engine_core::EngineCore) -> StoreResult<T>) -> StoreResult<T> {
        self.writes.engine_exec(&self.space, f).await.map_err(backend)?
    }

    async fn read<T>(&self, f: impl FnOnce(&flowr_storage::engine_core::EngineCore) -> T) -> StoreResult<T> {
        self.writes.engine_read(&self.space, f).await.map_err(backend)
    }
}

fn backend(e: AdbError) -> StoreError {
    StoreError::Backend(e.to_string())
}

#[async_trait]
impl<L: adb_log::Log + 'static> TaskQueue for AdbEngineStore<L> {
    async fn enqueue(&self, t: NewTask) -> StoreResult<EnqueueOutcome> {
        self.exec(|c| c.enqueue(t)).await
    }
    async fn heartbeat(&self, leases: &[Lease], extend: Duration) -> StoreResult<Vec<LeaseStatus>> {
        let leases = leases.to_vec();
        self.exec(move |c| c.heartbeat(Utc::now(), &leases, extend)).await
    }
    async fn ack(&self, lease: &Lease) -> StoreResult<()> {
        let lease = lease.clone();
        self.exec(move |c| c.ack(&lease)).await
    }
    async fn nack(&self, lease: &Lease, retry_at: Option<DateTime<Utc>>) -> StoreResult<()> {
        let lease = lease.clone();
        self.exec(move |c| c.nack(Utc::now(), &lease, retry_at)).await
    }
    async fn pending(&self, pool: &str) -> StoreResult<usize> {
        self.read(|c| c.pending(pool)).await
    }
    async fn live_pools(&self, within: Duration) -> StoreResult<Vec<String>> {
        self.read(|c| c.live_pools(Utc::now(), within)).await
    }
}

#[async_trait]
impl<L: adb_log::Log + 'static> RunStore for AdbEngineStore<L> {
    async fn start(&self, run: NewRun, task: NewTask) -> StoreResult<(RunId, EnqueueOutcome)> {
        self.exec(|c| c.start(Utc::now(), run, task)).await
    }
    async fn claim_runs(&self, pool: &str, worker: &str, n: usize, lease: Duration) -> StoreResult<Vec<(Lease, Task, Epoch)>> {
        self.exec(|c| c.claim_runs(Utc::now(), pool, worker, n, lease)).await
    }
    async fn renew(&self, run: RunId, epoch: Epoch, lease: Duration) -> StoreResult<bool> {
        self.exec(|c| c.renew(Utc::now(), run, epoch, lease)).await
    }
    async fn release(&self, run: RunId, epoch: Epoch, phase: RunPhase, outcome: RunOutcome, lease: &Lease) -> StoreResult<()> {
        let lease = lease.clone();
        self.exec(move |c| c.release(Utc::now(), run, epoch, phase, outcome, &lease)).await
    }
    async fn request(&self, run: RunId, req: RunRequest) -> StoreResult<()> {
        self.exec(|c| c.request(Utc::now(), run, req)).await
    }
    async fn get(&self, run: RunId) -> StoreResult<Option<RunRow>> {
        self.read(|c| c.get(run)).await
    }
    async fn list(&self, q: RunQuery) -> StoreResult<Page<RunRow>> {
        self.read(|c| c.list(&q)).await
    }
    async fn orphaned(&self) -> StoreResult<Vec<RunRow>> {
        self.read(|c| c.orphaned(Utc::now())).await
    }
    async fn purge_finished(&self, before: DateTime<Utc>) -> StoreResult<usize> {
        self.exec(|c| c.purge_finished(before)).await
    }
    async fn open_wait(&self, run: RunId, epoch: Epoch, wait_id: &str, correlation: Value) -> StoreResult<()> {
        self.exec(|c| c.open_wait(Utc::now(), run, epoch, wait_id, correlation)).await
    }
    async fn close_wait(&self, run: RunId, epoch: Epoch, wait_id: &str) -> StoreResult<bool> {
        self.exec(|c| c.close_wait(run, epoch, wait_id)).await
    }
    async fn deliver_event(&self, namespace: &str, event: Value) -> StoreResult<Vec<RunId>> {
        self.exec(|c| c.deliver_event(Utc::now(), namespace, event)).await
    }
    async fn open_approval(&self, run: RunId, epoch: Epoch, wait_id: &str, request: Value) -> StoreResult<()> {
        self.exec(|c| c.open_approval(Utc::now(), run, epoch, wait_id, request)).await
    }
    async fn approvals(&self) -> StoreResult<Vec<(RunId, String, Value)>> {
        self.read(|c| c.approvals()).await
    }
    async fn decide_approval(&self, wait_id: &str, decision: Value, by: &str) -> StoreResult<bool> {
        self.exec(|c| c.decide_approval(Utc::now(), wait_id, decision, by)).await
    }
}

#[async_trait]
impl<L: adb_log::Log + 'static> Journal for AdbEngineStore<L> {
    async fn append(&self, run: RunId, epoch: Epoch, records: &[Record]) -> StoreResult<AppendOutcome> {
        let records = records.to_vec();
        self.exec(move |c| c.append(Utc::now(), run, epoch, &records)).await
    }
    async fn read_from(&self, run: RunId, after_seq: u64) -> StoreResult<Vec<JournalEntry>> {
        self.writes.engine_journal(&self.space, run, after_seq).await.map_err(backend)
    }
    async fn commit_step(&self, run: RunId, epoch: Epoch, commit: StepCommit) -> StoreResult<u64> {
        self.exec(|c| c.commit_step(Utc::now(), run, epoch, commit)).await
    }
    async fn latest_checkpoint(&self, run: RunId) -> StoreResult<Option<(u64, Vec<u8>)>> {
        self.read(|c| c.latest_checkpoint(run)).await
    }
    async fn checkpoints(&self, run: RunId) -> StoreResult<Vec<CheckpointMeta>> {
        self.read(|c| c.checkpoints(run)).await
    }
    async fn outbox(&self, limit: usize) -> StoreResult<Vec<OutboxRow>> {
        self.read(|c| c.outbox(limit)).await
    }
    async fn outbox_done(&self, up_to_id: u64) -> StoreResult<()> {
        self.exec(|c| c.outbox_done(up_to_id)).await
    }
}

#[async_trait]
impl<L: adb_log::Log + 'static> Notifier for AdbEngineStore<L> {
    async fn publish(&self, topic: &str, msg: Value) -> StoreResult<()> {
        self.writes.engine_notify(&self.space, topic, msg).await.map_err(backend)
    }

    fn subscribe(&self, topic: &str) -> BoxStream<'static, Value> {
        self.writes.engine_subscribers().map(|subs| adb_engine::subscribe_on(&subs, &self.space, topic).boxed()).unwrap_or_else(|| futures::stream::empty().boxed())
    }
}

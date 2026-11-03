//! A follower's copy of the data (M3, `.ai/M3_RAFT.md`).
//!
//! In a replica set only the Raft leader runs an engine. A follower's state
//! machine appends each committed record to its own WAL; the [`Replica`] applies
//! that WAL to the follower's Lance table, platform tables and FlowR engine
//! spaces as it grows — the same catch-up a restarting node does at open — and
//! trims the WAL below what all three hold, as the leader does. When the node
//! becomes leader, the replica is dropped and an engine opens on the same
//! directory with almost nothing left to catch up.

use std::sync::Arc;

use adb_core::log::{Effect, LogOffset};
use adb_log::{Log, LogError};
use adb_store::PartitionStore;

use crate::engine::EngineLog;
use crate::writes::{catch_up, retain_max};

pub struct Replica {
    log: EngineLog,
    store: Arc<PartitionStore>,
    embedder: Option<Arc<adb_embed::Embedder>>,
    platform: Arc<adb_platform::PlatformStore>,
    engine: adb_engine::EngineHost,
    platform_next: LogOffset,
    engine_next: LogOffset,
    retain_max: u64,
}

fn s<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> String {
    move |e| format!("{what}: {e}")
}

impl Replica {
    /// Open `dir`'s table, platform tables and engine spaces, and catch up with `log`.
    pub async fn open(dir: impl AsRef<std::path::Path>, log: EngineLog) -> Result<Self, String> {
        let dir = dir.as_ref();
        let (store, embedder) = crate::engine::open_store(dir).await?;
        let platform = Arc::new(adb_platform::PlatformStore::open(dir.join("platform")).await.map_err(s("platform"))?);
        let platform_next = LogOffset(platform.applied_offset().await + 1);
        let engine = adb_engine::EngineHost::open(dir.join("engine")).await.map_err(s("engine"))?;
        let engine_next = engine.replay_from().await.map_err(s("engine"))?;
        let mut r = Self { log, store, embedder, platform, engine, platform_next, engine_next, retain_max: retain_max() };
        let n = r.step().await?;
        tracing::info!(records = n, offset = r.log.last_offset().0, "replica caught up");
        Ok(r)
    }

    /// Hand what this replica keeps open to the leader's engine
    /// ([`crate::LocalEngine::durable_warm`]); call after a last [`Self::step`].
    pub fn into_warm(self) -> crate::engine::Warm {
        crate::engine::Warm { store: self.store, embedder: self.embedder, platform: self.platform, engine: self.engine }
    }

    /// Records from `from` on, skipping a purged stretch (its records were
    /// applied: the log is purged only below what everything holds).
    async fn read(&self, from: &mut LogOffset) -> Result<Vec<adb_core::log::LogRecord>, String> {
        loop {
            match self.log.read(*from, 1024).await {
                Ok(b) => return Ok(b),
                Err(LogError::Purged { oldest, .. }) if oldest.0 > from.0 => *from = oldest,
                Err(LogError::Purged { .. }) => return Ok(Vec::new()),
                Err(e) => return Err(format!("log: {e}")),
            }
        }
    }

    /// Apply what the WAL gained since the last step; trim it. Returns the
    /// records applied to the table.
    pub async fn step(&mut self) -> Result<u64, String> {
        let applied = catch_up(&self.store, &self.log).await.map_err(s("store"))?;
        loop {
            let mut from = self.platform_next;
            let batch = self.read(&mut from).await?;
            let Some(last) = batch.last().map(|r| r.offset) else { break };
            for r in &batch {
                self.platform.apply(r).await.map_err(s("platform"))?;
            }
            self.platform_next = last.next();
        }
        loop {
            let mut from = self.engine_next;
            let batch = self.read(&mut from).await?;
            let Some(last) = batch.last().map(|r| r.offset) else { break };
            for r in &batch {
                if r.effects.iter().any(|e| matches!(e, Effect::Engine { .. })) {
                    self.engine.replay(r).await.map_err(s("engine"))?;
                }
            }
            self.engine_next = last.next();
        }
        self.engine.snapshot_due(false).await.map_err(s("engine"))?;
        self.purge().await?;
        Ok(applied)
    }

    /// Trim the WAL below what the table (on disk), the platform tables and the
    /// engine spaces hold, keeping `ADB_LOG_RETAIN` records for subscribers that
    /// resume on the next leader.
    async fn purge(&self) -> Result<(), String> {
        let tip = self.log.last_offset().0;
        let durable = self.store.durable_offset().await.map_err(s("store"))?;
        if durable.0 == 0 {
            return Ok(());
        }
        self.store.sync_to_disk().await.map_err(s("store"))?;
        let mut floor = durable.0 + 1;
        floor = floor.min(self.platform_next.0);
        if let Some(f) = self.engine.floor().await {
            floor = floor.min(f.0);
        }
        floor = floor.min(tip.saturating_sub(self.retain_max) + 1);
        if floor > 1 {
            self.log.purge_below(LogOffset(floor)).await.map_err(s("log purge"))?;
        }
        Ok(())
    }
}

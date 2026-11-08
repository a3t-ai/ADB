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
use crate::writes::{catch_up, forgotten_rows, redact_record, retain_max};

pub struct Replica {
    log: EngineLog,
    store: Arc<PartitionStore>,
    embedder: Option<Arc<adb_embed::Embedder>>,
    platform: Arc<adb_platform::PlatformStore>,
    engine: adb_engine::EngineHost,
    platform_next: LogOffset,
    engine_next: LogOffset,
    retain_max: u64,
    /// When [`Self::maintain`] last ran.
    maintained: std::time::Instant,
    /// Lineages forgotten (FORGET records seen) and not yet erased from this node.
    forgotten: std::collections::HashSet<(adb_core::log::Memory, String)>,
    /// The leader's erasure marker has arrived: erase [`Self::forgotten`] here too.
    erase_due: bool,
}

/// How often a follower checks whether its tables need compacting.
const MAINTAIN_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// A follower keeps every version it holds: history windows and branch pins are the
/// leader's to apply, and a follower that applied its own could drop a version the
/// leader still serves. The next leader's compaction trims history as usual.
const FOLLOWER_HISTORY: std::time::Duration = std::time::Duration::from_secs(100 * 365 * 86_400);

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
        let mut r = Self { log, store, embedder, platform, engine, platform_next, engine_next, retain_max: retain_max(), maintained: std::time::Instant::now(), forgotten: Default::default(), erase_due: false };
        let n = r.step().await?;
        tracing::info!(records = n, offset = r.log.last_offset().0, "replica caught up");
        // The table's schema migrations, as the leader's engine runs them on open
        // (`DurableWrites::open`): a follower applies the leader's new-format records,
        // so its own rows must be in that format too (v4 scoped ids, v5 namespace ids).
        if r.store.schema_version() < 4 {
            let n = r.store.scope_row_ids().await.map_err(s("store"))?;
            tracing::info!(rows = n, "schema v4: record ids in named scopes carry the scope");
        }
        if r.store.schema_version() < 5 {
            let n = r.store.namespace_row_ids().await.map_err(s("store"))?;
            tracing::info!(rows = n, "schema v5: record ids in a namespace carry the namespace");
        }
        Ok(r)
    }

    /// The follower's own table (status, tests).
    pub fn store(&self) -> &Arc<PartitionStore> {
        &self.store
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
                self.note_forgets(r);
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
        if self.erase_due {
            self.erase_due = false;
            if let Err(e) = self.erase().await {
                tracing::warn!(error = %e, "follower erasure failed; retrying at the next check");
            }
        }
        self.purge().await?;
        if self.maintained.elapsed() >= MAINTAIN_EVERY {
            self.maintained = std::time::Instant::now();
            self.maintain().await;
        }
        Ok(applied)
    }

    /// Compaction on the leader's thresholds. Without it a follower's sealed memtables,
    /// journal generations and table versions pile up until it leads: each flush then
    /// rewrites a longer manifest, and forgotten rows stay on its disk. Failures are
    /// logged and retried at the next check.
    pub async fn maintain(&mut self) {
        if !self.forgotten.is_empty() && !self.erase_due {
            // An erasure that failed, or FORGETs whose marker was before this replica
            // opened: try (again) now.
            if let Err(e) = self.erase().await {
                tracing::warn!(error = %e, "follower erasure failed; retrying at the next check");
            }
        }
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let due = crate::writes::compaction_due_for(&self.store).await;
        if due {
            match self.store.compact(FOLLOWER_HISTORY, now).await {
                Ok(st) => tracing::info!(kept = st.kept, of = st.total, took = ?st.took, "follower table compacted"),
                Err(e) => tracing::warn!(error = %e, "follower compaction failed; retrying later"),
            }
        }
        for j in self.engine.journals_to_compact().await {
            if let Err(e) = j.run().await {
                tracing::warn!(space = %j.space, error = %e, "follower journal compaction failed");
            }
        }
        if self.platform.compaction_due().await {
            if let Err(e) = self.platform.compact().await {
                tracing::warn!(error = %e, "follower platform compaction failed");
            }
        }
    }

    /// Note a record's FORGETs; the leader's erasure marker (the one record with no
    /// effects, on a new segment) means everything up to it is in closed segments.
    fn note_forgets(&mut self, r: &adb_core::log::LogRecord) {
        if r.effects.is_empty() {
            self.erase_due |= !self.forgotten.is_empty();
            return;
        }
        for e in &r.effects {
            if let Effect::Forget { memory, lineage_id } = e {
                self.forgotten.insert((*memory, lineage_id.0.clone()));
            }
        }
    }

    /// W5 on a follower, as the leader does it after a FORGET: the table without the
    /// forgotten rows, then the log's closed segments and openraft's own log (for a
    /// replica set's [`EngineLog`]) without their payloads. On failure the lineages
    /// stay noted for the next try.
    async fn erase(&mut self) -> Result<(), String> {
        let forgotten = std::mem::take(&mut self.forgotten);
        let r = self.erase_these(&forgotten).await;
        if r.is_err() {
            self.forgotten.extend(forgotten);
        }
        r
    }

    async fn erase_these(&self, forgotten: &std::collections::HashSet<(adb_core::log::Memory, String)>) -> Result<(), String> {
        // The table must hold the FORGETs before it is compacted.
        catch_up(&self.store, &self.log).await.map_err(s("store"))?;
        let rows = forgotten_rows(&self.store, forgotten.clone()).await.map_err(s("store"))?;
        let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
        let st = self.store.compact(FOLLOWER_HISTORY, now).await.map_err(s("compaction"))?;
        let redact = |r: &mut adb_core::log::LogRecord| redact_record(r, &rows);
        let n = self.log.rewrite_closed(&redact).await.map_err(s("log redaction"))?;
        tracing::info!(lineages = forgotten.len(), kept = st.kept, of = st.total, log_records_redacted = n, "follower erased forgotten records");
        Ok(())
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

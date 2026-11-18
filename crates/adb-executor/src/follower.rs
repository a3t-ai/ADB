//! Reads on a follower (C-16, AWS F-20): a replica set's followers answer RECALL,
//! LOOKUP and SCAN from their own copy of the data, so the leader's CPU isn't the
//! only one serving reads.
//!
//! A follower read is as fresh as a leader read: the caller (the cluster layer) gets
//! the leader's commit index (Raft ReadIndex), waits until this node's state machine
//! has applied it, and passes the WAL offset that holds it; [`FollowerReads::query`]
//! waits until the replica's tables have applied that offset before it reads. So a
//! read sees every write acknowledged before it started, on any node.
//!
//! Writes are the leader's: a statement that would write fails with [`NOT_LEADER`]
//! before anything is written (the cluster layer forwards it, or the client moves on).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use adb_core::log::LogOffset;
use adb_core::{AdbError, AdbResult, Link, MemoryRecord, MemoryType, Predicate, Scope};
use adb_log::Log;
use adb_store::PartitionStore;
use async_trait::async_trait;
use serde_json::{json, Value};

use crate::engine::EngineLog;
use crate::reads::{LanceReads, Reads};
use crate::writes::Writes;
use crate::Executor;

/// The error a follower gives a write (and the text clients match to move on).
pub const NOT_LEADER: &str = "this ADB node is not the leader";

/// Statements a follower answers itself; anything else goes to the leader.
pub fn follower_reads(aql: &str) -> bool {
    matches!(adb_core::metrics::statement_of(aql).0, "RECALL" | "LOOKUP" | "SCAN")
}

/// Entity aliases (GR-12) as the follower's table holds them.
pub type Aliases = Arc<RwLock<HashMap<String, String>>>;

/// What the replica has applied: every record up to this WAL offset is in its
/// table and platform tables.
pub type Applied = tokio::sync::watch::Receiver<u64>;

pub struct FollowerReads {
    executor: Executor,
    store: Arc<PartitionStore>,
    platform: Option<Arc<adb_platform::PlatformStore>>,
    log: EngineLog,
    applied: Applied,
    /// Asks the replica to apply now instead of at its next tick.
    wake: Arc<tokio::sync::Notify>,
}

impl FollowerReads {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Arc<PartitionStore>,
        platform: Option<Arc<adb_platform::PlatformStore>>,
        embedder: Option<Arc<adb_embed::Embedder>>,
        registry: Arc<crate::branches::Branches>,
        aliases: Aliases,
        log: EngineLog,
        applied: Applied,
        wake: Arc<tokio::sync::Notify>,
    ) -> Self {
        let reads: Arc<dyn Reads> = Arc::new(LanceReads::new(Arc::clone(&store), None).with_embedder(embedder).with_layers(Some(registry)));
        let writes: Arc<dyn Writes> = Arc::new(ReadOnlyWrites { aliases });
        Self { executor: Executor::with_io(writes, reads), store, platform, log, applied, wake }
    }

    pub fn store(&self) -> &Arc<PartitionStore> {
        &self.store
    }

    /// The reads its executor runs on (a cluster router's reads of this partition, step 6).
    pub fn reads(&self) -> Arc<dyn Reads> {
        self.executor.io().1
    }

    /// `None` on a memory partition's reader (step 6).
    pub fn platform(&self) -> Option<&Arc<adb_platform::PlatformStore>> {
        self.platform.as_ref()
    }

    /// The WAL's last offset: after this node's state machine applied the leader's
    /// read index, everything a read must see is at or below it.
    pub fn log_tip(&self) -> LogOffset {
        self.log.last_offset()
    }

    /// Wait until the tables have applied `upto` (at most `within`).
    pub async fn caught_up(&self, upto: LogOffset, within: Duration) -> Result<(), String> {
        let mut applied = self.applied.clone();
        if *applied.borrow() >= upto.0 {
            return Ok(());
        }
        self.wake.notify_one();
        tokio::time::timeout(within, applied.wait_for(|a| *a >= upto.0))
            .await
            .map_err(|_| format!("this follower has not applied offset {} within {within:?}", upto.0))?
            .map_err(|_| "this follower's replica stopped".to_string())?;
        Ok(())
    }

    /// Run a read (`follower_reads`) once the tables hold `upto`; the answer has the
    /// same form as the leader's, `metadata.point` included.
    pub async fn query(&self, aql: &str, variables: Option<&HashMap<String, Value>>, upto: LogOffset) -> Result<Value, String> {
        if !follower_reads(aql) {
            return Err(NOT_LEADER.into());
        }
        let tc = std::time::Instant::now();
        crate::steps::value("follower.read tables behind (records)", upto.0.saturating_sub(*self.applied.borrow()));
        self.caught_up(upto, Duration::from_secs(10)).await?;
        crate::steps::add("follower.read.3a tables catch-up wait", tc.elapsed());
        let t0 = std::time::Instant::now();
        let _permit = if crate::engine::scans(aql) { Some(crate::engine::read_permits().acquire().await.map_err(|e| e.to_string())?) } else { None };
        crate::steps::add("follower.read.3b read permit wait", t0.elapsed());
        let result = match variables {
            Some(vars) if !vars.is_empty() => self.executor.execute_with_vars(aql, vars).await,
            _ => self.executor.execute(aql).await,
        };
        let mut wire = result.map(|r| crate::engine::to_wire(&r)).map_err(|e| e.to_string());
        if let Ok(w) = &mut wire {
            if let Some(m) = w.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let ts = match self.log.read(upto, 1).await {
                    Ok(r) if r.first().is_some_and(|r| r.offset == upto) => r[0].commit_ts,
                    _ => adb_core::log::CommitTs::from_datetime(adb_core::time::now()),
                };
                let at = ts.to_datetime().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
                m.insert("point".into(), json!({ "offset": upto.0, "ts": ts.0, "at": at }));
                m.insert("served_by".into(), json!("follower"));
            }
        }
        crate::engine::observe_query(aql, t0.elapsed(), &wire);
        wire
    }

    /// Read-only SQL over the follower's tables (Flight `{"sql"}` tickets).
    pub async fn sql(&self, sql: &str, limit: Option<usize>, upto: LogOffset) -> Result<crate::sql::SqlResult, String> {
        self.caught_up(upto, Duration::from_secs(10)).await?;
        crate::sql::run(Some(Arc::clone(&self.store)), self.platform.clone(), sql, limit).await
    }
}

/// A follower's write path: none. Reads still resolve entity aliases.
struct ReadOnlyWrites {
    aliases: Aliases,
}

fn not_leader<T>() -> AdbResult<T> {
    Err(AdbError::UnsupportedOperation(NOT_LEADER.into()))
}

#[async_trait]
impl Writes for ReadOnlyWrites {
    async fn store(&self, _: MemoryType, _: &str, _: Value, _: Scope, _: Option<&str>, _: Option<Duration>, _: Option<&str>) -> AdbResult<MemoryRecord> {
        not_leader()
    }
    async fn update(&self, _: MemoryType, _: &Predicate, _: Value, _: Option<&str>, _: Option<&str>) -> AdbResult<u64> {
        not_leader()
    }
    async fn forget(&self, _: MemoryType, _: &Predicate, _: Option<&str>, _: Option<&str>) -> AdbResult<u64> {
        not_leader()
    }
    async fn link(&self, _: MemoryType, _: &str, _: MemoryType, _: &str, _: &str, _: Option<f32>) -> AdbResult<Link> {
        not_leader()
    }
    async fn update_links(&self, _: MemoryType, _: &[String], _: MemoryType, _: &[String], _: Option<&str>, _: Option<f32>, _: Option<&str>) -> AdbResult<u64> {
        not_leader()
    }
    async fn unlink(&self, _: MemoryType, _: &[String], _: MemoryType, _: &[String], _: Option<&str>) -> AdbResult<u64> {
        not_leader()
    }
    fn alias_of(&self, key: &str) -> Option<String> {
        let a = self.aliases.read().unwrap();
        if a.is_empty() {
            return None;
        }
        a.get(key).cloned()
    }
}

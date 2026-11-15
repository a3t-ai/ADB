//! Readers on every node (S3 option B, step 6).
//!
//! A node answers reads of a partition another node owns from its own **reader** of
//! that partition, without asking the owner for the data:
//!
//! - **Built from object storage**: the partition's last checkpoint (a shallow clone:
//!   the large files stay on object storage, read by range) and the WAL segments
//!   above it ([`crate::restore::restore_partition_from_object_storage`]).
//! - **Kept up by the owner's tail**: the committed records after it, pulled from the
//!   owner (`log_tail`: never past its commit offset — records a failover could drop
//!   are never shown) into the reader's own log and applied ([`crate::Replica`]).
//! - **Fresh reads**: a read first asks the owner for its commit offset — one call for
//!   all reads waiting at that moment — and waits until the reader has applied it, so
//!   it sees every write acknowledged before it began. `ADB_READ_STALE_MS` (default 0):
//!   a reader that heard from the owner that recently answers at once instead, up to
//!   that much behind.
//! - **Rebuilt** at the next checkpoint: every `ADB_READER_REBASE_SECS` (default 600) a
//!   reader whose partition has a newer checkpoint on object storage is built again
//!   from it (local disk stays small); also when the owner's log no longer holds what
//!   the reader needs next.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use adb_cluster::map::Partition;
use adb_core::log::{LogOffset, LogRecord, PartitionId};
use adb_log::Log;
use async_trait::async_trait;
use serde_json::Value;

use crate::engine::{EngineLog, LocalEngine, PartitionRole};
use crate::reads::Reads;

/// The owner, as a reader sees it.
#[async_trait]
pub trait ReaderSource: Send + Sync {
    /// Committed records from `from` (at most `max`).
    async fn tail(&self, from: u64, max: usize) -> Result<Vec<LogRecord>, String>;
    /// The owner's commit offset.
    async fn commit_offset(&self) -> Result<u64, String>;
}

fn stale_ok() -> Duration {
    Duration::from_millis(std::env::var("ADB_READ_STALE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
}

fn rebase_every() -> Duration {
    Duration::from_secs(std::env::var("ADB_READER_REBASE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(600))
}

/// The open state of a reader (replaced whole when it is rebuilt).
struct Open {
    reads: Arc<crate::follower::FollowerReads>,
    log: Arc<adb_log::LocalWal>,
    /// The checkpoint floor it was built from.
    floor: u64,
    /// Its directory: `base/g{gen}` (a rebuild makes the next one).
    gen: u64,
}

/// One partition's reader on a node that doesn't own it.
pub struct Reader {
    partition: Partition,
    open: tokio::sync::RwLock<Open>,
    source: Arc<dyn ReaderSource>,
    /// When the owner last answered a tail pull.
    heard: std::sync::Mutex<Option<Instant>>,
    /// One commit-offset call for all reads waiting (the latest answer, and when).
    asking: tokio::sync::Mutex<()>,
    asked: std::sync::Mutex<Option<(Instant, u64)>>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        if let Some(t) = self.task.lock().unwrap().take() {
            t.abort();
        }
    }
}

/// Build a reader's directory and open it: from object storage when there is any, then
/// the owner's tail.
async fn build(base: &std::path::Path, gen: u64, partition: Partition, cold: Option<&str>) -> Result<(Open, crate::Replica), String> {
    let dir = &base.join(format!("g{gen}"));
    let role = PartitionRole::memory(partition);
    let mut floor = 0;
    if !dir.join(format!("p{partition}")).exists() {
        std::fs::create_dir_all(dir).map_err(|e| format!("reader dir {}: {e}", dir.display()))?;
        if let Some(cold) = cold {
            let st = crate::restore::restore_partition_from_object_storage(dir, cold, PartitionId(partition), None).await?;
            floor = st.log_starts_at.saturating_sub(1);
            tracing::info!(partition, floor, records = st.segment_records, "reader built from object storage");
        }
    }
    let log = LocalEngine::open_partition_wal(dir, role).await?;
    let elog: EngineLog = Arc::clone(&log) as EngineLog;
    let replica = Box::pin(crate::Replica::open_role(dir, role, Arc::clone(&elog))).await?;
    let reads = Arc::new(replica.reader(elog));
    Ok((Open { reads, log, floor, gen }, replica))
}

impl Reader {
    /// Build the reader of `partition` under `base` (anything there from an earlier run
    /// is dropped: a reader is a cache of object storage and the owner), following
    /// `source`.
    pub async fn open(base: PathBuf, partition: Partition, cold: Option<String>, source: Arc<dyn ReaderSource>) -> Result<Arc<Self>, String> {
        let _ = std::fs::remove_dir_all(&base);
        let (open, replica) = build(&base, 0, partition, cold.as_deref()).await?;
        let dir = base;
        let r = Arc::new(Self {
            partition,
            open: tokio::sync::RwLock::new(open),
            source,
            heard: Default::default(),
            asking: Default::default(),
            asked: Default::default(),
            task: Default::default(),
        });
        let task = tokio::spawn(follow(Arc::downgrade(&r), dir, cold, replica));
        *r.task.lock().unwrap() = Some(task);
        Ok(r)
    }

    /// The owner's commit offset now; concurrent askers share one call.
    async fn owner_offset(&self) -> Result<u64, String> {
        let asked_at = Instant::now();
        let _one = self.asking.lock().await;
        // Someone asked after we began waiting: their answer is fresh enough for us.
        if let Some((t, off)) = *self.asked.lock().unwrap() {
            if t >= asked_at {
                return Ok(off);
            }
        }
        let t = Instant::now();
        let off = self.source.commit_offset().await?;
        *self.asked.lock().unwrap() = Some((t, off));
        Ok(off)
    }

    /// What a read must see: the owner's commit offset, or (within
    /// `ADB_READ_STALE_MS` of the last tail) what this reader holds.
    async fn upto(&self) -> Result<u64, String> {
        let stale = stale_ok();
        if !stale.is_zero() && self.heard.lock().unwrap().is_some_and(|t| t.elapsed() <= stale) {
            return Ok(self.open.read().await.log.last_offset().0);
        }
        self.owner_offset().await
    }

    /// A read-only statement, answered here as the owner would.
    pub async fn query(&self, aql: &str, variables: Option<&std::collections::HashMap<String, Value>>) -> Result<Value, String> {
        let upto = self.upto().await?;
        let open = self.open.read().await;
        open.reads.query(aql, variables, LogOffset(upto)).await
    }

    /// This reader's reads, once it holds everything committed now.
    pub async fn fresh_reads(&self) -> Result<Arc<dyn Reads>, String> {
        let upto = self.upto().await?;
        let open = self.open.read().await;
        open.reads.caught_up(LogOffset(upto), Duration::from_secs(10)).await?;
        Ok(open.reads.reads())
    }

    pub fn partition(&self) -> Partition {
        self.partition
    }
}

/// The reader's task: pull the owner's tail and apply it; rebuild from a newer
/// checkpoint when due or when the owner's log no longer reaches back far enough.
async fn follow(me: std::sync::Weak<Reader>, dir: PathBuf, cold: Option<String>, mut replica: crate::Replica) {
    let mut rebased = Instant::now();
    let wake = replica.wake();
    loop {
        let Some(r) = me.upgrade() else { return };
        let (log, floor) = {
            let o = r.open.read().await;
            (Arc::clone(&o.log), o.floor)
        };
        let from = log.last_offset().0 + 1;
        let mut rebuild = false;
        match r.source.tail(from, 1024).await {
            Ok(recs) => {
                *r.heard.lock().unwrap() = Some(Instant::now());
                let n = recs.len();
                for rec in recs {
                    if let Err(e) = log.append(rec).await {
                        tracing::warn!(partition = r.partition, error = %e, "reader: appending the owner's record failed");
                        break;
                    }
                }
                if n > 0 {
                    if let Err(e) = replica.step().await {
                        tracing::warn!(partition = r.partition, error = %e, "reader: apply failed; retrying");
                    }
                }
                if n == 1024 {
                    drop(r);
                    continue;
                }
            }
            // The owner's log was trimmed past us: the checkpoint holds it now.
            Err(e) if e.contains("purged") || e.contains("Purged") => rebuild = true,
            Err(e) => tracing::debug!(partition = r.partition, error = %e, "reader: tail not reached; retrying"),
        }
        // Due to move to a newer checkpoint?
        if !rebuild && rebased.elapsed() >= rebase_every() {
            rebased = Instant::now();
            if let Some(c) = &cold {
                let fence = crate::writes::segments::Fence::default();
                if let Ok(Some((_, p))) = crate::writes::segments::read_point(c, PartitionId(r.partition), &fence).await {
                    rebuild = p.floor > floor;
                }
            }
        }
        if rebuild {
            if let Some(c) = &cold {
                match rebuild_reader(&r, &dir, c).await {
                    Ok(next) => replica = next,
                    Err(e) => tracing::warn!(partition = r.partition, error = %e, "reader: rebuilding from object storage failed; retrying"),
                }
            }
        }
        drop(r);
        tokio::select! {
            _ = wake.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

/// Build the reader again in a new directory from the latest checkpoint, then swap it
/// in (reads in flight finish on the old one) and remove the old directory.
async fn rebuild_reader(r: &Arc<Reader>, base: &std::path::Path, cold: &str) -> Result<crate::Replica, String> {
    let gen = r.open.read().await.gen + 1;
    let _ = std::fs::remove_dir_all(base.join(format!("g{gen}")));
    let (open, replica) = build(base, gen, r.partition, Some(cold)).await?;
    let old = std::mem::replace(&mut *r.open.write().await, open);
    // Reads still on the old one finish first: its directory goes a minute later.
    let old_dir = base.join(format!("g{}", old.gen));
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        drop(old);
        let _ = std::fs::remove_dir_all(old_dir);
    });
    tracing::info!(partition = r.partition, gen, "reader rebuilt from a newer checkpoint");
    Ok(replica)
}

//! FlowR's engine store on ADB (stage H, `.ai/H_ENGINE_STORE.md`).
//!
//! Each **space** (one FlowR installation, H9) holds an [`EngineCore`] — FlowR's
//! own rules, from `flowr-storage` (H2) — and its journal history as EPISODIC
//! rows in the engine's own partition (`engine/<space>/journal`, H5), so agents'
//! memory never sees run history (H15).
//!
//! The host takes one operation at a time (the caller holds ADB's sequencer):
//! [`EngineHost::exec`] runs it on the space's core and returns the
//! `Effect::Engine` to log (durable changes only, H3/H4); after the record is
//! appended, [`EngineHost::committed`] writes the journal's new episodes and
//! purges retention's. After a restart, [`EngineHost::replay`] applies the log
//! above each space's snapshot (snapshots: every 10,000 records or 60 s) and
//! then every lease gets one fresh period.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use adb_core::log::{
    CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData, Visibility,
};
use adb_store::{PartitionStore, StoreConfig};
use chrono::{DateTime, Utc};
use flowr_storage::engine::{JournalEntry, Record, RunId};
use flowr_storage::engine_core::{Change, EngineCore};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Snapshot after this many engine records…
pub const SNAPSHOT_EVERY: u64 = 10_000;
/// …or this long, whichever comes first.
pub const SNAPSHOT_AFTER: Duration = Duration::from_secs(60);
/// A finished run's journal leaves memory this long after it ended (H5).
pub const EVICT_AFTER: Duration = Duration::from_secs(300);
/// The lease every run and task gets after a restart (H4), as FlowR's default.
pub const RESTART_LEASE: Duration = Duration::from_secs(30);

/// Compact a space's journal after this many writes since the last compaction
/// (or a quarter of the rows it kept, whichever is more), or past 16 MemWAL
/// generations: retention's deletes are tombstones until then.
pub const JOURNAL_COMPACT_AFTER: u64 = 2_000;

/// The journal partition's id (row ids don't depend on it).
const JOURNAL_PARTITION: PartitionId = PartitionId(1);

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("engine space {0:?}: a space is 1-64 letters, digits, '-' or '_'")]
    BadSpace(String),
    #[error("engine store: {0}")]
    Store(String),
    #[error("engine snapshot: {0}")]
    Snapshot(String),
}

pub type EngineResult<T> = Result<T, EngineError>;

fn store_err(e: impl std::fmt::Display) -> EngineError {
    EngineError::Store(e.to_string())
}

/// The tenant id on a space's journal rows.
pub fn tenant(space: &str) -> String {
    format!("flowr:{space}")
}

/// A snapshot on disk: the core and the last log offset it includes.
#[derive(Serialize, Deserialize)]
struct Snapshot {
    offset: u64,
    core: EngineCore,
}

/// Journal rows for the space's writer: one log record's episodes and the
/// runs whose history retention removes.
struct JournalJob {
    offset: LogOffset,
    epoch: Epoch,
    commit_ts: CommitTs,
    puts: Vec<Effect>,
    purge: Vec<RunId>,
}

struct Space {
    core: EngineCore,
    journal: Arc<PartitionStore>,
    /// The background writer of the journal partition (log order).
    jobs: UnboundedSender<JournalJob>,
    /// The last log offset handed to the writer, and how far it has written.
    queued: u64,
    written: tokio::sync::watch::Receiver<u64>,
    /// Held by the writer around each write and by a compaction throughout.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// The last log offset the snapshot on disk includes.
    snapshot_offset: u64,
    /// The last log offset with this space's changes.
    last_offset: u64,
    since_snapshot: u64,
    snapshot_at: Instant,
}

/// The core as a snapshot writes it (the fields of [`Snapshot`], borrowed: no copy).
#[derive(Serialize)]
struct SnapshotRef<'a> {
    offset: u64,
    core: &'a EngineCore,
}

/// How many subscriptions between sweeps of topics nobody listens to.
const SUBS_SWEEP_EVERY: u64 = 1024;

/// Subscribers per (space, topic).
pub type Subs = std::sync::Mutex<HashMap<(String, String), Vec<UnboundedSender<Value>>>>;

/// Notices on `topic` in `space`, from now on (no host lock needed).
pub fn subscribe_on(subs: &Subs, space: &str, topic: &str) -> UnboundedReceiver<Value> {
    static SUBSCRIBED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (tx, rx) = unbounded();
    let mut subs = subs.lock().unwrap();
    subs.entry((space.to_string(), topic.to_string())).or_default().push(tx);
    // A run's topic never fires after the run ends, so `notify` alone would keep it for
    // good. Swept here, it costs at most this many dead topics.
    if SUBSCRIBED.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % SUBS_SWEEP_EVERY == SUBS_SWEEP_EVERY - 1 {
        subs.retain(|_, list| {
            list.retain(|tx| !tx.is_closed());
            !list.is_empty()
        });
    }
    rx
}

pub struct EngineHost {
    dir: PathBuf,
    spaces: HashMap<String, Space>,
    subs: Arc<Subs>,
}

fn check_space(name: &str) -> EngineResult<()> {
    let ok = !name.is_empty() && name.len() <= 64 && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    ok.then_some(()).ok_or_else(|| EngineError::BadSpace(name.to_string()))
}

/// Write a space's journal episodes in log order, off the sequencer (the log
/// already holds them: after a crash, replay writes what is missing). A write
/// that keeps failing stops the writer rather than skip past it.
fn spawn_writer(
    space: String,
    journal: Arc<PartitionStore>,
    start: u64,
    write_lock: Arc<tokio::sync::Mutex<()>>,
) -> (UnboundedSender<JournalJob>, tokio::sync::watch::Receiver<u64>) {
    let (tx, mut rx) = unbounded::<JournalJob>();
    let (done, written) = tokio::sync::watch::channel(start);
    tokio::spawn(async move {
        use futures::StreamExt;
        while let Some(job) = rx.next().await {
            let mut tries = 0;
            loop {
                let wrote = {
                    let _held = write_lock.lock().await;
                    write_job(&journal, &job).await
                };
                match wrote {
                    Ok(()) => break,
                    Err(e) if tries < 5 => {
                        tries += 1;
                        tracing::warn!(space, offset = job.offset.0, error = %e, "journal write failed; retrying");
                        tokio::time::sleep(Duration::from_millis(200 << tries)).await;
                    }
                    Err(e) => {
                        tracing::error!(space, offset = job.offset.0, error = %e, "journal writer stopped; the log keeps the records for the next open");
                        return;
                    }
                }
            }
            let _ = done.send(job.offset.0);
        }
    });
    (tx, written)
}

async fn write_job(journal: &PartitionStore, job: &JournalJob) -> EngineResult<()> {
    if job.offset.0 <= journal.applied_offset().await.map_err(store_err)?.0 {
        return Ok(());
    }
    let mut effects = job.puts.clone();
    for id in &job.purge {
        let now = CommitTs::from_datetime(Utc::now());
        let rows = journal.current(Some(&format!("scope = '{id}'")), now).await.map_err(store_err)?;
        effects.extend(rows.into_iter().map(|r| Effect::Forget { memory: Memory::Episodic, lineage_id: RecordId(adb_store::key_of(&r.lineage_id).to_string()) }));
    }
    let rec = LogRecord { partition: JOURNAL_PARTITION, offset: job.offset, epoch: job.epoch, commit_ts: job.commit_ts, txn_id: None, effects };
    journal.apply(&rec).await.map_err(store_err)?;
    Ok(())
}

async fn open_space(dir: &Path) -> EngineResult<Space> {
    std::fs::create_dir_all(dir).map_err(store_err)?;
    let journal = Arc::new(PartitionStore::open(&StoreConfig::new(dir.join("journal")), JOURNAL_PARTITION).await.map_err(store_err)?);
    let applied = journal.applied_offset().await.map_err(store_err)?.0;
    let write_lock = Arc::new(tokio::sync::Mutex::new(()));
    let name = dir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
    let (jobs, written) = spawn_writer(name, Arc::clone(&journal), applied, Arc::clone(&write_lock));
    let (core, snapshot_offset) = match std::fs::read(dir.join("snapshot.json")) {
        Ok(bytes) => {
            let s: Snapshot = serde_json::from_slice(&bytes).map_err(|e| EngineError::Snapshot(e.to_string()))?;
            (s.core, s.offset)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (EngineCore::new(), 0),
        Err(e) => return Err(EngineError::Snapshot(e.to_string())),
    };
    Ok(Space { core, journal, jobs, queued: applied, written, write_lock, snapshot_offset, last_offset: snapshot_offset, since_snapshot: 0, snapshot_at: Instant::now() })
}

/// A one-line summary of a journal record, the episode's text.
fn summary(r: &Record) -> String {
    match r {
        Record::StepCompleted { step_key, duration_us, .. } => format!("step {step_key} completed in {} ms", duration_us / 1000),
        Record::StepFailed { step_key, .. } => format!("step {step_key} failed"),
        Record::StepStarted { step_key, attempt, .. } => format!("step {step_key} started (attempt {})", attempt + 1),
        other => {
            let v = serde_json::to_value(other).unwrap_or(Value::Null);
            let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("record").replace('_', " ");
            match other.step_key() {
                Some(k) => format!("{kind} at {k}"),
                None => kind,
            }
        }
    }
}

/// A journal entry as an EPISODIC row in the engine's partition: scope = the
/// run, key = its sequence number (zero-padded, so keys sort), no embedding,
/// no TTL.
pub fn episode(space: &str, namespace: &str, run: RunId, e: &JournalEntry) -> NewRow {
    let key = format!("{:012}", e.seq);
    let id = RecordId(adb_store::scoped_id(&run.to_string(), &key));
    NewRow {
        id: id.clone(),
        lineage_id: id,
        version: 1,
        data: RowData {
            tenant_id: tenant(space),
            scope: run.to_string(),
            visibility: Visibility::Private,
            namespace: Some(namespace.to_string()),
            memory: Memory::Episodic,
            kind: Kind::Episode,
            key: Some(key),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            value: json!({ "seq": e.seq, "epoch": e.epoch, "at": e.at, "record": e.record }),
            sealed: None,
            text: Some(summary(&e.record)),
            embedding: None,
            ttl_at: None,
            provenance: Provenance::default(),
        },
    }
}

impl EngineHost {
    /// Open the spaces under `dir` (`<dir>/<space>/`), from their snapshots.
    /// Call [`Self::replay`] with the log above [`Self::replay_from`] next.
    pub async fn open(dir: impl Into<PathBuf>) -> EngineResult<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(store_err)?;
        let mut spaces = HashMap::new();
        for entry in std::fs::read_dir(&dir).map_err(store_err)? {
            let entry = entry.map_err(store_err)?;
            let name = entry.file_name().to_string_lossy().to_string();
            if entry.path().is_dir() && check_space(&name).is_ok() {
                spaces.insert(name, open_space(&entry.path()).await?);
            }
        }
        Ok(Self { dir, spaces, subs: Arc::default() })
    }

    pub fn spaces(&self) -> Vec<String> {
        let mut v: Vec<String> = self.spaces.keys().cloned().collect();
        v.sort();
        v
    }

    /// Where replay must start: above the oldest snapshot or journal store.
    pub async fn replay_from(&self) -> EngineResult<LogOffset> {
        let mut from = u64::MAX;
        for s in self.spaces.values() {
            let j = s.journal.applied_offset().await.map_err(store_err)?.0;
            from = from.min(s.snapshot_offset.min(j));
        }
        Ok(LogOffset(if from == u64::MAX { 0 } else { from } + 1))
    }

    async fn space(&mut self, name: &str) -> EngineResult<&mut Space> {
        check_space(name)?;
        if !self.spaces.contains_key(name) {
            let s = open_space(&self.dir.join(name)).await?;
            self.spaces.insert(name.to_string(), s);
        }
        Ok(self.spaces.get_mut(name).unwrap())
    }

    /// Apply a log record during replay: each space's changes above its
    /// snapshot to its core, journal rows above its journal store's offset.
    pub async fn replay(&mut self, record: &LogRecord) -> EngineResult<()> {
        for effect in &record.effects {
            let Effect::Engine { space, changes } = effect else { continue };
            let changes: Vec<Change> = serde_json::from_value(changes.clone()).map_err(|e| EngineError::Snapshot(e.to_string()))?;
            let s = self.space(space).await?;
            if record.offset.0 > s.snapshot_offset {
                for c in &changes {
                    s.core.apply(c);
                }
                s.since_snapshot += 1;
            }
            s.last_offset = s.last_offset.max(record.offset.0);
            let name = space.clone();
            self.write_journal(&name, record, &changes).await?;
        }
        Ok(())
    }

    /// After replay: every lease and ownership gets one fresh period (H4).
    pub fn replayed(&mut self, now: DateTime<Utc>) {
        for s in self.spaces.values_mut() {
            s.core.reset_leases(now, RESTART_LEASE);
        }
    }

    /// Run one operation on `space`'s core. Returns its answer, the effect to log
    /// (`None` when nothing durable changed), and the notices to deliver once
    /// it is logged. The caller holds the sequencer and appends the effect.
    pub async fn exec<T>(&mut self, space: &str, f: impl FnOnce(&mut EngineCore) -> T) -> EngineResult<(T, Option<Effect>, Vec<(String, Value)>)> {
        let s = self.space(space).await?;
        let out = f(&mut s.core);
        let fx = s.core.take_effects();
        let durable: Vec<&Change> = fx.changes.iter().filter(|c| c.durable()).collect();
        let effect = (!durable.is_empty()).then(|| Effect::Engine {
            space: space.to_string(),
            changes: serde_json::to_value(&durable).unwrap_or(Value::Null),
        });
        Ok((out, effect, fx.notices))
    }

    /// Read `space`'s core (no log).
    pub async fn read<T>(&mut self, space: &str, f: impl FnOnce(&EngineCore) -> T) -> EngineResult<T> {
        Ok(f(&self.space(space).await?.core))
    }

    /// The record holding an operation's effect is in the log: write its journal
    /// episodes (and retention's deletions) and count it toward a snapshot.
    pub async fn committed(&mut self, record: &LogRecord) -> EngineResult<()> {
        for effect in &record.effects {
            let Effect::Engine { space, changes } = effect else { continue };
            let changes: Vec<Change> = serde_json::from_value(changes.clone()).map_err(|e| EngineError::Snapshot(e.to_string()))?;
            let s = self.space(space).await?;
            s.last_offset = record.offset.0;
            s.since_snapshot += 1;
            let name = space.clone();
            self.write_journal(&name, record, &changes).await?;
        }
        Ok(())
    }

    /// The journal partition's share of a record, queued for the space's
    /// writer: an episode per new entry, and the runs whose history retention
    /// removes. A record with neither costs the journal nothing.
    async fn write_journal(&mut self, space: &str, record: &LogRecord, changes: &[Change]) -> EngineResult<()> {
        let s = self.spaces.get_mut(space).expect("opened");
        let mut puts = Vec::new();
        let mut purge = Vec::new();
        for c in changes {
            match c {
                Change::Journal { run, entries } => {
                    let ns = s.core.get(*run).map(|r| r.namespace).unwrap_or_default();
                    puts.extend(entries.iter().map(|e| Effect::PutEpisode(episode(space, &ns, *run, e))));
                }
                Change::Run { id, state: None } => purge.push(*id),
                _ => {}
            }
        }
        if puts.is_empty() && purge.is_empty() {
            return Ok(());
        }
        s.queued = record.offset.0;
        s.jobs
            .unbounded_send(JournalJob { offset: record.offset, epoch: record.epoch, commit_ts: record.commit_ts, puts, purge })
            .map_err(|_| EngineError::Store(format!("space {space}: the journal writer stopped")))
    }

    /// Wait until `space`'s writer has written everything queued.
    async fn journal_caught_up(&mut self, space: &str) -> EngineResult<()> {
        let s = self.space(space).await?;
        let want = s.queued;
        let mut w = s.written.clone();
        tokio::time::timeout(Duration::from_secs(30), w.wait_for(|v| *v >= want))
            .await
            .map_err(|_| EngineError::Store(format!("space {space}: journal not written after 30 s")))?
            .map_err(|_| EngineError::Store(format!("space {space}: the journal writer stopped")))?;
        Ok(())
    }

    /// A run's journal history from the partition (its journal left memory).
    pub async fn archived(&mut self, space: &str, run: RunId, after_seq: u64) -> EngineResult<Vec<JournalEntry>> {
        self.journal_caught_up(space).await?;
        let s = self.space(space).await?;
        let now = CommitTs::from_datetime(Utc::now());
        let rows = s.journal.current(Some(&format!("scope = '{run}'")), now).await.map_err(store_err)?;
        let mut out: Vec<JournalEntry> = rows
            .into_iter()
            .filter_map(|r| {
                let v = &r.data.value;
                Some(JournalEntry {
                    seq: v.get("seq")?.as_u64()?,
                    epoch: v.get("epoch")?.as_u64()?,
                    at: serde_json::from_value(v.get("at")?.clone()).ok()?,
                    record: serde_json::from_value(v.get("record")?.clone()).ok()?,
                })
            })
            .filter(|e| e.seq > after_seq)
            .collect();
        out.sort_by_key(|e| e.seq);
        Ok(out)
    }

    /// Runs of `space` whose journal may leave memory now (finished
    /// [`EVICT_AFTER`] ago, journal still held).
    pub async fn evictable(&mut self, space: &str, now: DateTime<Utc>) -> EngineResult<Vec<RunId>> {
        let cut = now - chrono::Duration::from_std(EVICT_AFTER).unwrap_or_default();
        let s = self.space(space).await?;
        Ok(s.core.evictable(cut, 10_000))
    }

    /// The journals whose compaction is due, to compact without the host lock.
    pub async fn journals_to_compact(&self) -> Vec<JournalCompaction> {
        let mut out = Vec::new();
        for (name, s) in &self.spaces {
            let due = s.journal.writes_due(JOURNAL_COMPACT_AFTER) || s.journal.generation_count().await.is_ok_and(|n| n > 16);
            if due {
                out.push(JournalCompaction { space: name.clone(), journal: Arc::clone(&s.journal), write_lock: Arc::clone(&s.write_lock) });
            }
        }
        out
    }

    /// Every space's journal, due or not (tests, admin).
    pub fn journals(&self) -> Vec<JournalCompaction> {
        self.spaces
            .iter()
            .map(|(name, s)| JournalCompaction { space: name.clone(), journal: Arc::clone(&s.journal), write_lock: Arc::clone(&s.write_lock) })
            .collect()
    }

    /// Write the snapshots that are due (every [`SNAPSHOT_EVERY`] records or
    /// [`SNAPSHOT_AFTER`]), after the journal partition is on disk.
    pub async fn snapshot_due(&mut self, force: bool) -> EngineResult<usize> {
        let mut written = 0;
        let names: Vec<String> = self.spaces.keys().cloned().collect();
        for name in names {
            let s = self.spaces.get_mut(&name).unwrap();
            let due = s.since_snapshot > 0 && (force || s.since_snapshot >= SNAPSHOT_EVERY || s.snapshot_at.elapsed() >= SNAPSHOT_AFTER);
            if !due {
                continue;
            }
            let dir = self.dir.join(&name);
            let tmp = dir.join("snapshot.json.tmp");
            {
                use std::io::Write;
                // Straight to the file: no copy of the core and no buffer of its size.
                let f = std::fs::File::create(&tmp).map_err(|e| EngineError::Snapshot(e.to_string()))?;
                let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
                serde_json::to_writer(&mut w, &SnapshotRef { offset: s.last_offset, core: &s.core }).map_err(|e| EngineError::Snapshot(e.to_string()))?;
                w.flush().map_err(|e| EngineError::Snapshot(e.to_string()))?;
                w.get_ref().sync_all().map_err(|e| EngineError::Snapshot(e.to_string()))?;
            }
            std::fs::rename(&tmp, dir.join("snapshot.json")).map_err(|e| EngineError::Snapshot(e.to_string()))?;
            if let Ok(d) = std::fs::File::open(&dir) {
                let _ = d.sync_all();
            }
            s.snapshot_offset = s.last_offset;
            s.since_snapshot = 0;
            s.snapshot_at = Instant::now();
            written += 1;
        }
        Ok(written)
    }

    /// The lowest log offset the engine still needs (`None`: nothing): the
    /// record after each space's snapshot, or after what its journal store has
    /// on disk, whichever is older.
    pub async fn floor(&self) -> Option<LogOffset> {
        let mut floor: Option<u64> = None;
        for s in self.spaces.values() {
            // Journal rows still queued, or written but not yet durable, need
            // their records; with none pending, the journal holds nothing back.
            let durable = s.journal.durable_offset().await.map(|o| o.0).unwrap_or(0);
            let pending = *s.written.borrow() < s.queued || durable < s.queued;
            let needs = if pending { s.snapshot_offset.min(durable) } else { s.snapshot_offset };
            if needs < s.last_offset {
                floor = Some(floor.map_or(needs + 1, |f| f.min(needs + 1)));
            }
        }
        floor.map(LogOffset)
    }

    /// Throw away a space's state and rebuild it from its snapshot (after an
    /// append failed: the core already holds changes that never reached the log).
    pub async fn reopen(&mut self, space: &str) -> EngineResult<()> {
        check_space(space)?;
        let s = open_space(&self.dir.join(space)).await?;
        self.spaces.insert(space.to_string(), s);
        Ok(())
    }

    /// Deliver notices to the space's subscribers (best effort).
    pub fn notify(&self, space: &str, notices: Vec<(String, Value)>) {
        let mut subs = self.subs.lock().unwrap();
        for (topic, msg) in notices {
            let key = (space.to_string(), topic);
            if let Some(list) = subs.get_mut(&key) {
                list.retain(|tx| tx.unbounded_send(msg.clone()).is_ok());
                if list.is_empty() {
                    subs.remove(&key);
                }
            }
        }
    }

    /// Notices on `topic` in `space`, from now on.
    pub fn subscribe(&self, space: &str, topic: &str) -> UnboundedReceiver<Value> {
        subscribe_on(&self.subs, space, topic)
    }

    /// The subscriber table, for handles that subscribe without the host lock.
    pub fn subscribers(&self) -> Arc<Subs> {
        Arc::clone(&self.subs)
    }

    /// For tests: the core's durable state.
    pub async fn durable_view(&mut self, space: &str) -> EngineResult<Value> {
        Ok(self.space(space).await?.core.durable_view())
    }

    /// The epoch the ADB log writes with (records need one).
    pub fn epoch() -> Epoch {
        Epoch(1)
    }
}

/// One space's journal, ready to compact: the rewrite drops the rows retention
/// deleted and folds the MemWAL
/// generations. The writer waits meanwhile; engine operations don't (their
/// episodes queue), and journal reads wait on the table lock.
pub struct JournalCompaction {
    pub space: String,
    journal: Arc<PartitionStore>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl JournalCompaction {
    pub async fn run(&self) -> EngineResult<adb_store::CompactStats> {
        let _held = self.write_lock.lock().await;
        // Journal rows are written once and never superseded: no history, and
        // no marker for deleted rows (the writer skips anything at or below the
        // applied offset, so nothing comes back).
        self.journal.compact_append_only(CommitTs::from_datetime(Utc::now())).await.map_err(store_err)
    }
}

#[cfg(test)]
mod subs_tests {
    use super::*;

    #[test]
    fn topics_nobody_listens_to_are_swept() {
        let subs: Subs = Default::default();
        let kept = subscribe_on(&subs, "s", "live");
        for i in 0..(SUBS_SWEEP_EVERY * 3) {
            drop(subscribe_on(&subs, "s", &format!("run:{i}")));
        }
        let guard = subs.lock().unwrap();
        assert!(guard.len() as u64 <= SUBS_SWEEP_EVERY + 1, "{} topics", guard.len());
        assert!(guard.contains_key(&("s".to_string(), "live".to_string())), "a live topic stays");
        drop(guard);
        drop(kept);
    }
}

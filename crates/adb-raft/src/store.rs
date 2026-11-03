//! openraft's log and vote on disk (`<dir>/raft`).
//!
//! - `entries`: length-prefixed MessagePack frames, appended and fsynced by one
//!   writer thread (several appends share an fsync); a torn last frame from a
//!   crash is dropped on open. Truncation and purge rewrite the file (rare:
//!   truncation only after a leader change, purge after a snapshot).
//! - `vote`, `committed`, `purged`: one value each, replaced atomically.
//!
//! All entries are kept in memory too (the log is trimmed after each snapshot).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};

use openraft::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::OptionalSend;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::TypeConfig as TC;

enum Job {
    Append { bytes: Vec<u8>, cb: IOFlushed<TC> },
    Rewrite { bytes: Vec<u8>, done: mpsc::Sender<io::Result<()>> },
}

#[derive(Default)]
struct Inner {
    log: BTreeMap<u64, EntryOf<TC>>,
    vote: Option<VoteOf<TC>>,
    committed: Option<LogIdOf<TC>>,
    purged: Option<LogIdOf<TC>>,
}

#[derive(Clone)]
pub struct LogStore {
    dir: PathBuf,
    inner: Arc<Mutex<Inner>>,
    writer: mpsc::Sender<Job>,
}

fn frame<T: Serialize>(v: &T, out: &mut Vec<u8>) -> io::Result<()> {
    let b = rmp_serde::to_vec_named(v).map_err(io::Error::other)?;
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(&b);
    Ok(())
}

fn read_frames(path: &Path) -> io::Result<Vec<EntryOf<TC>>> {
    let mut buf = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut buf)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    }
    let mut out = Vec::new();
    let mut at = 0usize;
    while at + 4 <= buf.len() {
        let len = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
        if at + 4 + len > buf.len() {
            tracing::warn!(path = %path.display(), at, "raft log: torn last entry dropped");
            break;
        }
        match rmp_serde::from_slice::<EntryOf<TC>>(&buf[at + 4..at + 4 + len]) {
            Ok(e) => out.push(e),
            Err(e) => {
                tracing::warn!(path = %path.display(), at, error = %e, "raft log: unreadable entry; the rest dropped");
                break;
            }
        }
        at += 4 + len;
    }
    Ok(out)
}

/// Write `v` to `dir/name` atomically (tmp, fsync, rename, fsync the directory).
pub(crate) fn put<T: Serialize>(dir: &Path, name: &str, v: &T) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&rmp_serde::to_vec_named(v).map_err(io::Error::other)?)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

pub(crate) fn get<T: DeserializeOwned>(dir: &Path, name: &str) -> io::Result<Option<T>> {
    match std::fs::read(dir.join(name)) {
        Ok(b) => rmp_serde::from_slice(&b).map(Some).map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn spawn_writer(path: PathBuf) -> io::Result<mpsc::Sender<Job>> {
    let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
    let (tx, rx) = mpsc::channel::<Job>();
    std::thread::Builder::new().name("adb-raft-log".into()).spawn(move || {
        while let Ok(first) = rx.recv() {
            let mut jobs = vec![first];
            while let Ok(more) = rx.try_recv() {
                jobs.push(more);
            }
            let mut pending: Vec<IOFlushed<TC>> = Vec::new();
            let mut result: io::Result<()> = Ok(());
            for job in jobs {
                match job {
                    Job::Append { bytes, cb } => {
                        if result.is_ok() {
                            result = f.write_all(&bytes);
                        }
                        pending.push(cb);
                    }
                    Job::Rewrite { bytes, done } => {
                        // Earlier appends first, then the new file in its place.
                        let flushed = result.as_ref().map_err(|e| io::Error::new(e.kind(), e.to_string())).and_then(|_| f.sync_data());
                        for cb in pending.drain(..) {
                            cb.io_completed(flushed.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string())));
                        }
                        let r = (|| {
                            let tmp = path.with_extension("rewrite");
                            {
                                let mut t = File::create(&tmp)?;
                                t.write_all(&bytes)?;
                                t.sync_all()?;
                            }
                            std::fs::rename(&tmp, &path)?;
                            f = OpenOptions::new().append(true).open(&path)?;
                            Ok(())
                        })();
                        let _ = done.send(r);
                    }
                }
            }
            if !pending.is_empty() {
                let flushed = result.and_then(|_| f.sync_data());
                for cb in pending {
                    cb.io_completed(flushed.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string())));
                }
            }
        }
    })?;
    Ok(tx)
}

impl LogStore {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("entries");
        let mut inner = Inner {
            vote: get(&dir, "vote")?,
            committed: get(&dir, "committed")?,
            purged: get(&dir, "purged")?,
            ..Default::default()
        };
        let purged_at = inner.purged.as_ref().map(|l| l.index());
        for e in read_frames(&path)? {
            if purged_at.map_or(true, |p| e.index() > p) {
                inner.log.insert(e.index(), e);
            }
        }
        let writer = spawn_writer(path)?;
        Ok(Self { dir, inner: Arc::new(Mutex::new(inner)), writer })
    }

    /// No entry and no vote: a node that never took part.
    pub fn is_empty(&self) -> bool {
        let i = self.inner.lock().unwrap();
        i.log.is_empty() && i.vote.is_none() && i.purged.is_none()
    }

    /// Replace the entries file with what is in memory.
    fn rewrite(&self, log: &BTreeMap<u64, EntryOf<TC>>) -> io::Result<()> {
        let mut bytes = Vec::new();
        for e in log.values() {
            frame(e, &mut bytes)?;
        }
        let (done, wait) = mpsc::channel();
        self.writer.send(Job::Rewrite { bytes, done }).map_err(io::Error::other)?;
        wait.recv().map_err(io::Error::other)?
    }
}

impl RaftLogReader<TC> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> Result<Vec<EntryOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().log.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().vote.clone())
    }
}

impl RaftLogStorage<TC> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TC>, io::Error> {
        let i = self.inner.lock().unwrap();
        let last = i.log.values().next_back().map(|e| e.log_id()).or_else(|| i.purged.clone());
        Ok(LogState { last_purged_log_id: i.purged.clone(), last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TC>) -> Result<(), io::Error> {
        put(&self.dir, "vote", vote)?;
        self.inner.lock().unwrap().vote = Some(vote.clone());
        Ok(())
    }

    async fn save_committed(&mut self, c: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        put(&self.dir, "committed", &c)?;
        self.inner.lock().unwrap().committed = c;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().committed.clone())
    }

    /// Readable at once (replication can start); `callback` fires after fsync.
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TC>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TC>> + OptionalSend,
    {
        let mut bytes = Vec::new();
        {
            let mut i = self.inner.lock().unwrap();
            for e in entries {
                frame(&e, &mut bytes)?;
                i.log.insert(e.index(), e);
            }
        }
        self.writer.send(Job::Append { bytes, cb: callback }).map_err(io::Error::other)
    }

    async fn truncate_after(&mut self, last: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        let start = last.map(|l| l.index() + 1).unwrap_or(0);
        let log = {
            let mut i = self.inner.lock().unwrap();
            i.log.retain(|k, _| *k < start);
            i.log.clone()
        };
        self.rewrite(&log)
    }

    async fn purge(&mut self, log_id: LogIdOf<TC>) -> Result<(), io::Error> {
        put(&self.dir, "purged", &Some(log_id.clone()))?;
        let log = {
            let mut i = self.inner.lock().unwrap();
            i.log.retain(|k, _| *k > log_id.index());
            i.purged = Some(log_id);
            i.log.clone()
        };
        self.rewrite(&log)
    }
}

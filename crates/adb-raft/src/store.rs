//! openraft's log and vote on disk (`<dir>/raft`).
//!
//! - `entries`: length-prefixed MessagePack frames, appended and fsynced by one
//!   writer thread (several appends share an fsync); a torn last frame from a
//!   crash is dropped on open. Truncation and purge rewrite the file (rare:
//!   truncation only after a leader change, purge after a snapshot).
//! - `vote`, `committed`, `purged`: one value each, replaced atomically.
//!
//! RAM holds an index (entry number to place in the file) and a tail of recent
//! entries, up to `ADB_RAFT_LOG_TAIL_MB` of encoded bytes (default 8), plus any entry
//! not yet fsynced. Everything else is read through a read-only mmap of `entries`, so
//! the amount of log kept is a disk cost: it can stay large, which keeps a follower
//! that was down from needing a copy of a peer's data (see `sm`), without holding it
//! on the heap. Writes never go through the mapping: fsync stays what makes an entry
//! durable.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::{Range, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};

use memmap2::Mmap;
use openraft::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::OptionalSend;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::TypeConfig as TC;

enum Job {
    /// `upto`: one past the last entry number in `bytes`.
    Append { bytes: Vec<u8>, upto: u64, cb: IOFlushed<TC> },
    /// Keep only this range of the file (positions as in [`Loc`]).
    Rewrite { keep: Range<u64>, done: mpsc::Sender<io::Result<()>> },
    /// Write out earlier appends, report on `held`, then wait: the caller rewrites the
    /// file and sends back the new one to append to (`None`: keep the current one).
    Hold { held: mpsc::Sender<io::Result<()>>, resume: Mutex<mpsc::Receiver<Option<File>>> },
}

/// Where an entry's frame is. `at` counts bytes since the store opened and is not
/// changed by a rewrite; its place in the current file is `at - Inner::base`.
#[derive(Clone, Copy)]
struct Loc {
    at: u64,
    len: u32,
}

#[derive(Default)]
struct Inner {
    index: BTreeMap<u64, Loc>,
    /// Recent entries and any not yet on disk, encoded: what they cost here is their
    /// size on disk, which is what `ADB_RAFT_LOG_TAIL_MB` limits. Parsed, an entry's
    /// JSON takes several times that.
    tail: BTreeMap<u64, Arc<[u8]>>,
    tail_bytes: usize,
    /// Entries below this number are on disk, so may leave `tail`.
    written: u64,
    last: Option<LogIdOf<TC>>,
    /// Position of the next frame.
    next: u64,
    /// Position of the first byte of the current file.
    base: u64,
    /// The current file, mapped when a read first needs it. Replaced with the file
    /// (under this lock), so a mapping and `base` always belong together.
    map: Option<Arc<Mmap>>,
    vote: Option<VoteOf<TC>>,
    committed: Option<LogIdOf<TC>>,
    purged: Option<LogIdOf<TC>>,
}

impl Inner {
    /// Note entries below `upto` as on disk, and drop the oldest beyond the limit.
    fn mark_written(&mut self, upto: u64, limit: usize) {
        self.written = self.written.max(upto);
        while self.tail_bytes > limit {
            let Some((&k, _)) = self.tail.first_key_value() else { break };
            if k >= self.written {
                break;
            }
            if let Some(b) = self.tail.remove(&k) {
                self.tail_bytes -= b.len();
            }
        }
    }

    /// Replace the tail, for a purge or a truncation.
    fn set_tail(&mut self, tail: BTreeMap<u64, Arc<[u8]>>) {
        self.tail_bytes = tail.values().map(|b| b.len()).sum();
        self.tail = tail;
    }
}

#[derive(Clone)]
pub struct LogStore {
    dir: PathBuf,
    inner: Arc<Mutex<Inner>>,
    writer: mpsc::Sender<Job>,
    /// When `committed` was last written (C-3: at most once a second).
    committed_saved_at: Arc<Mutex<Option<std::time::Instant>>>,
}

fn invalid(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn clone_err(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

fn sync_dir(path: &Path) {
    if let Some(Ok(d)) = path.parent().map(File::open) {
        let _ = d.sync_all();
    }
}

fn encode(e: &EntryOf<TC>) -> io::Result<Vec<u8>> {
    rmp_serde::to_vec_named(e).map_err(io::Error::other)
}

/// Read the frames of `path` one at a time, keeping none of them. Returns how many
/// bytes are good; anything after that is a torn or unreadable frame.
fn scan(path: &Path, mut each: impl FnMut(EntryOf<TC>, Loc)) -> io::Result<u64> {
    let f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    if f.metadata()?.len() == 0 {
        return Ok(0);
    }
    // SAFETY: nothing else has the store's directory open yet, see `LogStore::open`.
    let buf = unsafe { Mmap::map(&f)? };
    let mut at = 0usize;
    while at + 4 <= buf.len() {
        let len = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
        if at + 4 + len > buf.len() {
            break;
        }
        match rmp_serde::from_slice::<EntryOf<TC>>(&buf[at + 4..at + 4 + len]) {
            Ok(e) => each(e, Loc { at: at as u64, len: len as u32 }),
            Err(e) => {
                tracing::warn!(path = %path.display(), at, error = %e, "raft log: unreadable entry");
                break;
            }
        }
        at += 4 + len;
    }
    Ok(at as u64)
}

/// Cut what [`scan`] could not read off the file, keeping a copy beside it. Left in
/// place, the next appends would land after it and be lost at the next open.
fn drop_after(path: &Path, good: u64) -> io::Result<()> {
    let Ok(meta) = std::fs::metadata(path) else { return Ok(()) };
    if meta.len() <= good {
        return Ok(());
    }
    tracing::warn!(path = %path.display(), good, bytes = meta.len() - good, "raft log: dropping a torn or unreadable end (a copy is in entries.dropped)");
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    f.seek(SeekFrom::Start(good))?;
    let mut copy = File::create(path.with_extension("dropped"))?;
    io::copy(&mut f, &mut copy)?;
    copy.sync_all()?;
    f.set_len(good)?;
    f.sync_all()
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

/// Replace the file with the `keep` range of it, copying from disk: a purge needs no
/// memory for the entries it keeps.
fn rewrite_file(path: &Path, inner: &Mutex<Inner>, keep: Range<u64>) -> io::Result<File> {
    let tmp = path.with_extension("rewrite");
    let base = inner.lock().unwrap().base;
    {
        let mut src = File::open(path)?;
        src.seek(SeekFrom::Start(keep.start - base))?;
        let mut t = File::create(&tmp)?;
        let want = keep.end - keep.start;
        let copied = io::copy(&mut src.take(want), &mut t)?;
        if copied != want {
            return Err(invalid(format!("raft log rewrite: {copied} of {want} bytes")));
        }
        t.sync_all()?;
    }
    let f;
    {
        // A reader holds this lock while it maps the file and reads `base`.
        let mut i = inner.lock().unwrap();
        std::fs::rename(&tmp, path)?;
        f = OpenOptions::new().append(true).open(path)?;
        i.base = keep.start;
        i.map = None;
    }
    sync_dir(path);
    Ok(f)
}

/// `bytes` (one encoded entry) with `redact` applied to its record, padded to the same
/// length; `None` if `redact` leaves it as it is. The padding is [`crate::Cmd::pad`]:
/// a MessagePack array of zero bytes (one byte each) after a 4-byte key, with a
/// header of 1, 3 or 5 bytes by length.
fn redact_entry(bytes: &[u8], redact: &(dyn Fn(&mut adb_core::log::LogRecord) -> bool + Sync)) -> io::Result<Option<Vec<u8>>> {
    let mut e: EntryOf<TC> = rmp_serde::from_slice(bytes).map_err(invalid)?;
    fn pad(e: &mut EntryOf<TC>) -> Option<&mut Vec<u8>> {
        match &mut e.payload {
            openraft::EntryPayload::Normal(cmd) => Some(&mut cmd.pad),
            _ => None,
        }
    }
    {
        let openraft::EntryPayload::Normal(cmd) = &mut e.payload else { return Ok(None) };
        if !redact(&mut cmd.record) {
            return Ok(None);
        }
        cmd.pad.clear();
    }
    let bare = encode(&e)?.len();
    let extra = bytes.len().checked_sub(bare).ok_or_else(|| invalid("raft log: a redacted entry grew"))?;
    for (header, fits) in [(1usize, 1..16usize), (3, 16..65_536), (5, 65_536..usize::MAX)] {
        let Some(n) = extra.checked_sub(4 + header) else { continue };
        if !fits.contains(&n) {
            continue;
        }
        if let Some(p) = pad(&mut e) {
            *p = vec![0; n];
        }
        let out = encode(&e)?;
        if out.len() == bytes.len() {
            return Ok(Some(out));
        }
    }
    Err(invalid(format!("raft log: cannot pad a redacted entry from {bare} to {} bytes", bytes.len())))
}

/// Copy the file with `redact` applied to every entry, each frame keeping its length,
/// then swap it in. `None` for the file if nothing changed.
fn redact_file(path: &Path, inner: &Mutex<Inner>, redact: &(dyn Fn(&mut adb_core::log::LogRecord) -> bool + Sync)) -> io::Result<(Option<File>, usize)> {
    let src = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((None, 0)),
        Err(e) => return Err(e),
    };
    let len = src.metadata()?.len() as usize;
    if len == 0 {
        return Ok((None, 0));
    }
    // SAFETY: the writer is held (no appends), and only this store writes the file.
    let buf = unsafe { Mmap::map(&src)? };
    let mut changed = Vec::new();
    let mut at = 0usize;
    while at + 4 <= len {
        let n = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
        if at + 4 + n > len {
            break;
        }
        if let Some(out) = redact_entry(&buf[at + 4..at + 4 + n], redact)? {
            changed.push((at + 4, out));
        }
        at += 4 + n;
    }
    if changed.is_empty() {
        return Ok((None, 0));
    }
    let tmp = path.with_extension("redact");
    {
        let mut t = File::create(&tmp)?;
        let mut from = 0usize;
        for (pos, out) in &changed {
            t.write_all(&buf[from..*pos])?;
            t.write_all(out)?;
            from = pos + out.len();
        }
        t.write_all(&buf[from..len])?;
        t.sync_all()?;
    }
    drop(buf);
    let f;
    {
        let mut i = inner.lock().unwrap();
        std::fs::rename(&tmp, path)?;
        f = OpenOptions::new().append(true).open(path)?;
        i.map = None;
        // The recent entries held in RAM too.
        let keys: Vec<u64> = i.tail.keys().copied().collect();
        for k in keys {
            if let Some(out) = redact_entry(&i.tail[&k], redact)? {
                i.tail.insert(k, out.into());
            }
        }
    }
    sync_dir(path);
    Ok((Some(f), changed.len()))
}

fn spawn_writer(path: PathBuf, inner: Arc<Mutex<Inner>>, tail_limit: usize) -> io::Result<mpsc::Sender<Job>> {
    let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
    let (tx, rx) = mpsc::channel::<Job>();
    std::thread::Builder::new().name("adb-raft-log".into()).spawn(move || {
        while let Ok(first) = rx.recv() {
            let mut jobs = vec![first];
            while let Ok(more) = rx.try_recv() {
                jobs.push(more);
            }
            adb_core::steps::value("raft.log writer jobs per batch", jobs.len() as u64);
            let wt = std::time::Instant::now();
            let mut pending: Vec<IOFlushed<TC>> = Vec::new();
            let mut pending_upto = 0u64;
            let mut result: io::Result<()> = Ok(());
            for job in jobs {
                match job {
                    Job::Append { bytes, upto, cb } => {
                        if result.is_ok() {
                            result = f.write_all(&bytes);
                        }
                        pending_upto = pending_upto.max(upto);
                        pending.push(cb);
                    }
                    Job::Rewrite { keep, done } => {
                        // Earlier appends first, then the new file in its place.
                        let flushed = result.as_ref().map_err(clone_err).and_then(|_| f.sync_data());
                        if flushed.is_ok() {
                            inner.lock().unwrap().mark_written(pending_upto, tail_limit);
                        }
                        for cb in pending.drain(..) {
                            cb.io_completed(flushed.as_ref().map(|_| ()).map_err(clone_err));
                        }
                        let r = flushed.and_then(|_| rewrite_file(&path, &inner, keep)).map(|new| f = new);
                        let _ = done.send(r);
                    }
                    Job::Hold { held, resume } => {
                        let flushed = result.as_ref().map_err(clone_err).and_then(|_| f.sync_data());
                        if flushed.is_ok() {
                            inner.lock().unwrap().mark_written(pending_upto, tail_limit);
                        }
                        for cb in pending.drain(..) {
                            cb.io_completed(flushed.as_ref().map(|_| ()).map_err(clone_err));
                        }
                        if held.send(flushed).is_ok() {
                            if let Ok(Some(new)) = resume.into_inner().unwrap().recv() {
                                f = new;
                            }
                        }
                    }
                }
            }
            adb_core::steps::add("raft.log writer write", wt.elapsed());
            if !pending.is_empty() {
                let st = std::time::Instant::now();
                let flushed = result.and_then(|_| f.sync_data());
                adb_core::steps::add("raft.log writer fsync", st.elapsed());
                if flushed.is_ok() {
                    inner.lock().unwrap().mark_written(pending_upto, tail_limit);
                }
                for cb in pending {
                    cb.io_completed(flushed.as_ref().map(|_| ()).map_err(clone_err));
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
        let good = {
            let (index, last) = (&mut inner.index, &mut inner.last);
            scan(&path, |e, loc| {
                if purged_at.map_or(true, |p| e.index() > p) {
                    index.insert(e.index(), loc);
                    *last = Some(e.log_id());
                }
            })?
        };
        drop_after(&path, good)?;
        inner.next = good;
        inner.written = inner.last.as_ref().map_or(0, |l| l.index() + 1);
        let tail_limit = std::env::var("ADB_RAFT_LOG_TAIL_MB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(8) << 20;
        let inner = Arc::new(Mutex::new(inner));
        let writer = spawn_writer(path, Arc::clone(&inner), tail_limit)?;
        Ok(Self { dir, inner, writer, committed_saved_at: Default::default() })
    }

    /// Rewrite every entry whose record `redact` changes (W5 erasure: a forgotten
    /// payload leaves the Raft log too, not only the WAL). Each entry keeps its length
    /// on disk, so every position, and any append queued meanwhile, stays right; the
    /// new file is complete and synced before it replaces the old one, so a crash
    /// leaves one or the other. Appends wait while it runs. Returns how many changed.
    pub fn redact(&self, redact: &(dyn Fn(&mut adb_core::log::LogRecord) -> bool + Sync)) -> io::Result<usize> {
        let (held_tx, held_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let stopped = || io::Error::other("raft log writer stopped");
        self.writer.send(Job::Hold { held: held_tx, resume: Mutex::new(resume_rx) }).map_err(|_| stopped())?;
        held_rx.recv().map_err(|_| stopped())??;
        let r = redact_file(&self.dir.join("entries"), &self.inner, redact);
        let (file, n) = match r {
            Ok(v) => v,
            Err(e) => {
                let _ = resume_tx.send(None);
                return Err(e);
            }
        };
        let _ = resume_tx.send(file);
        Ok(n)
    }

    /// No entry and no vote: a node that never took part.
    pub fn is_empty(&self) -> bool {
        let i = self.inner.lock().unwrap();
        i.index.is_empty() && i.vote.is_none() && i.purged.is_none()
    }

    /// Reduce the entries file to the `keep` range, after earlier appends.
    fn rewrite(&self, keep: Range<u64>) -> io::Result<()> {
        let (done, wait) = mpsc::channel();
        self.writer.send(Job::Rewrite { keep, done }).map_err(io::Error::other)?;
        wait.recv().map_err(io::Error::other)?
    }

    /// The mapping of the current file, covering file position `upto`.
    fn mapped(&self, i: &mut Inner, upto: u64) -> io::Result<Arc<Mmap>> {
        if let Some(m) = &i.map {
            if m.len() as u64 >= upto {
                return Ok(Arc::clone(m));
            }
        }
        let f = File::open(self.dir.join("entries"))?;
        // SAFETY: the file only grows at its end. It is never shortened in place while
        // open: truncation and purge write a new file and rename it over (`rewrite_file`),
        // so an existing mapping keeps the old file and its bytes.
        let m = Arc::new(unsafe { Mmap::map(&f)? });
        if (m.len() as u64) < upto {
            return Err(invalid(format!("raft log: file ends at {}, an entry ends at {upto}", m.len())));
        }
        i.map = Some(Arc::clone(&m));
        Ok(m)
    }
}

impl RaftLogReader<TC> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> Result<Vec<EntryOf<TC>>, io::Error> {
        // Recent entries come from the tail, the rest from the mapping. Either way they
        // are decoded after the lock is released: appends must not wait for a read.
        let (items, map, base) = {
            let mut i = self.inner.lock().unwrap();
            let items: Vec<Result<Arc<[u8]>, Loc>> = i
                .index
                .range(range)
                .map(|(k, loc)| i.tail.get(k).cloned().ok_or(*loc))
                .collect();
            let end = items.iter().filter_map(|r| r.as_ref().err()).map(|l| l.at + 4 + u64::from(l.len)).max();
            let base = i.base;
            let map = match end {
                Some(end) => Some(self.mapped(&mut i, end - base)?),
                None => None,
            };
            (items, map, base)
        };
        items
            .into_iter()
            .map(|item| match item {
                Ok(b) => rmp_serde::from_slice(&b).map_err(invalid),
                Err(loc) => {
                    let map = map.as_ref().ok_or_else(|| invalid("raft log: no mapping"))?;
                    let start = (loc.at - base + 4) as usize;
                    rmp_serde::from_slice(&map[start..start + loc.len as usize]).map_err(invalid)
                }
            })
            .collect()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TC>>, io::Error> {
        Ok(self.inner.lock().unwrap().vote.clone())
    }
}

impl RaftLogStorage<TC> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TC>, io::Error> {
        let i = self.inner.lock().unwrap();
        let last = i.last.clone().or_else(|| i.purged.clone());
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

    /// C-3: openraft calls this on every commit, in its core loop, and the atomic rewrite
    /// took two syncs each time. The committed index is optional to persist (openraft:
    /// a restarted node re-applies up to the quorum's commit), so it is written at most
    /// once a second; the Raft log entries themselves are what is durable.
    async fn save_committed(&mut self, c: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        let due = {
            let mut at = self.committed_saved_at.lock().unwrap();
            let due = at.is_none_or(|t| t.elapsed() >= std::time::Duration::from_secs(1));
            if due {
                *at = Some(std::time::Instant::now());
            }
            due
        };
        if due {
            put(&self.dir, "committed", &c)?;
        }
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
        let mut upto = 0;
        {
            let mut i = self.inner.lock().unwrap();
            for e in entries {
                let b = encode(&e)?;
                let len = b.len() as u32;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(&b);
                let (at, n) = (i.next, e.index());
                i.index.insert(n, Loc { at, len });
                i.next = at + 4 + u64::from(len);
                i.last = Some(e.log_id());
                i.tail.insert(n, Arc::from(b));
                i.tail_bytes += len as usize;
                upto = n + 1;
            }
        }
        self.writer.send(Job::Append { bytes, upto, cb: callback }).map_err(io::Error::other)
    }

    async fn truncate_after(&mut self, last: Option<LogIdOf<TC>>) -> Result<(), io::Error> {
        let start = last.as_ref().map(|l| l.index() + 1).unwrap_or(0);
        let keep = {
            let mut i = self.inner.lock().unwrap();
            let cut = i.index.range(start..).next().map_or(i.next, |(_, l)| l.at);
            i.index.split_off(&start);
            let mut tail = std::mem::take(&mut i.tail);
            tail.split_off(&start);
            i.set_tail(tail);
            i.written = i.written.min(start);
            i.next = cut;
            // A cut past the end changes nothing: `last` may name an entry not held.
            if i.index.is_empty() {
                i.last = None;
            } else if i.last.as_ref().is_some_and(|l| l.index() >= start) {
                i.last = last;
            }
            i.base..cut
        };
        self.rewrite(keep)
    }

    async fn purge(&mut self, log_id: LogIdOf<TC>) -> Result<(), io::Error> {
        put(&self.dir, "purged", &Some(log_id.clone()))?;
        let keep = {
            let mut i = self.inner.lock().unwrap();
            let from = log_id.index() + 1;
            let index = i.index.split_off(&from);
            i.index = index;
            let tail = std::mem::take(&mut i.tail).split_off(&from);
            i.set_tail(tail);
            if i.last.as_ref().is_some_and(|l| l.index() < from) {
                i.last = None;
            }
            i.purged = Some(log_id);
            let start = i.index.first_key_value().map_or(i.next, |(_, l)| l.at);
            start..i.next
        };
        self.rewrite(keep)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::log::{StoreBuilder, Suite};
    use openraft::StorageError;
    use std::sync::Arc;

    fn entry(n: u64) -> EntryOf<TC> {
        openraft::testing::blank_ent::<TC>(1, 1, n)
    }

    async fn put_all(s: &mut LogStore, r: Range<u64>) {
        s.append(r.map(entry), IOFlushed::noop()).await.unwrap();
        // A rewrite of everything waits for all the appends before it.
        let keep = { let i = s.inner.lock().unwrap(); i.base..i.next };
        s.rewrite(keep).unwrap();
    }

    async fn numbers(s: &mut LogStore, r: Range<u64>) -> Vec<u64> {
        s.try_get_log_entries(r).await.unwrap().iter().map(|e| e.index()).collect()
    }

    fn put(key: &str, text_len: usize) -> adb_core::log::Effect {
        use adb_core::log::*;
        Effect::PutEntity(NewRow {
            id: RecordId(key.into()),
            lineage_id: RecordId(key.into()),
            version: 1,
            data: RowData {
                tenant_id: "default".into(),
                scope: "tenant:default".into(),
                visibility: Visibility::Private,
                namespace: None,
                memory: Memory::Semantic,
                kind: Kind::Entity,
                key: Some(key.into()),
                subject_id: None,
                predicate: None,
                object_id: None,
                link_type: None,
                weight: None,
                text: Some("x".repeat(text_len)),
                value: serde_json::json!({ "key": key }),
                sealed: None,
                embedding: None,
                ttl_at: None,
                provenance: Provenance::default(),
            },
        })
    }

    /// Entry `n` committing a record with these effects.
    fn cmd(n: u64, effects: Vec<adb_core::log::Effect>) -> EntryOf<TC> {
        use adb_core::log::*;
        let mut e = entry(n);
        let record = LogRecord { partition: PartitionId(0), offset: LogOffset(n), epoch: Epoch(1), commit_ts: CommitTs(1), txn_id: None, effects };
        e.payload = openraft::EntryPayload::Normal(crate::Cmd { record, new_segment: false, pad: Vec::new() });
        e
    }

    fn keys(e: &EntryOf<TC>) -> Vec<String> {
        match &e.payload {
            openraft::EntryPayload::Normal(c) => c.record.effects.iter().filter_map(|x| match x {
                adb_core::log::Effect::PutEntity(r) => Some(r.id.0.clone()),
                _ => None,
            }).collect(),
            _ => Vec::new(),
        }
    }

    #[tokio::test]
    async fn redaction_takes_payloads_out_and_keeps_every_position() {
        let d = tempfile::tempdir().unwrap();
        let mut s = LogStore::open(d.path()).unwrap();
        // Small and large dropped effects: each padding header size (1, 3 and 5 bytes).
        let entries = vec![
            cmd(0, vec![put("keep0", 10)]),
            cmd(1, vec![put("gone", 3), put("keep1", 10)]),
            cmd(2, vec![put("gone", 300), put("keep2", 10)]),
            cmd(3, vec![put("gone", 200_000)]),
            cmd(4, vec![put("keep4", 10)]),
        ];
        s.append(entries.into_iter(), IOFlushed::noop()).await.unwrap();
        // A redaction that changes nothing still waits for the appends to reach the file.
        assert_eq!(s.redact(&|_| false).unwrap(), 0);
        let size = std::fs::metadata(d.path().join("entries")).unwrap().len();
        let redact = |r: &mut adb_core::log::LogRecord| {
            let n = r.effects.len();
            r.effects.retain(|e| !matches!(e, adb_core::log::Effect::PutEntity(x) if x.id.0 == "gone"));
            r.effects.len() != n
        };
        assert_eq!(s.redact(&redact).unwrap(), 3);
        assert_eq!(std::fs::metadata(d.path().join("entries")).unwrap().len(), size, "same length on disk");
        let raw = std::fs::read(d.path().join("entries")).unwrap();
        assert!(!raw.windows(4).any(|w| w == b"gone"), "no trace left on disk");
        // An append after it lands where the index says.
        s.append([cmd(5, vec![put("keep5", 10)])], IOFlushed::noop()).await.unwrap();
        let want = vec![vec!["keep0"], vec!["keep1"], vec!["keep2"], vec![], vec!["keep4"], vec!["keep5"]];
        let read = |es: Vec<EntryOf<TC>>| es.iter().map(keys).collect::<Vec<_>>();
        // From RAM (the tail), from disk, and after a reopen.
        assert_eq!(read(s.try_get_log_entries(0..6).await.unwrap()), want);
        assert_eq!(s.redact(&|_| false).unwrap(), 0);
        s.inner.lock().unwrap().mark_written(6, 0);
        assert!(s.inner.lock().unwrap().tail.is_empty(), "the next reads come from the file");
        assert_eq!(read(s.try_get_log_entries(0..6).await.unwrap()), want);
        drop(s);
        let mut s = LogStore::open(d.path()).unwrap();
        assert_eq!(read(s.try_get_log_entries(0..6).await.unwrap()), want);
        assert_eq!(s.redact(&redact).unwrap(), 0, "nothing left to take out");
    }

    #[tokio::test]
    async fn reads_come_from_disk_and_the_tail_stays_bounded() {
        let d = tempfile::tempdir().unwrap();
        let mut s = LogStore::open(d.path()).unwrap();
        put_all(&mut s, 0..50_000).await;
        {
            let mut i = s.inner.lock().unwrap();
            i.mark_written(50_000, 0);
            assert!(i.tail.is_empty(), "written entries left the tail");
            assert_eq!(i.index.len(), 50_000);
        }
        assert_eq!(numbers(&mut s, 49_990..50_000).await, (49_990..50_000).collect::<Vec<_>>());
        assert_eq!(numbers(&mut s, 0..3).await, vec![0, 1, 2]);
        // New entries are readable before and after they reach the disk.
        s.append([entry(50_000)], IOFlushed::noop()).await.unwrap();
        assert_eq!(numbers(&mut s, 50_000..50_001).await, vec![50_000]);
    }

    #[tokio::test]
    async fn purge_and_truncate_keep_the_right_entries_across_a_reopen() {
        let d = tempfile::tempdir().unwrap();
        let mut s = LogStore::open(d.path()).unwrap();
        put_all(&mut s, 0..100).await;
        // A mapping taken before the rewrite must not be read against the new file.
        assert_eq!(numbers(&mut s, 10..12).await, vec![10, 11]);
        let purge = entry(39).log_id();
        s.purge(purge.clone()).await.unwrap();
        s.inner.lock().unwrap().mark_written(100, 0);
        assert_eq!(numbers(&mut s, 0..42).await, vec![40, 41]);
        let last = entry(59).log_id();
        s.truncate_after(Some(last)).await.unwrap();
        // The same numbers again, with new contents, after the cut.
        put_all(&mut s, 60..70).await;
        s.inner.lock().unwrap().mark_written(70, 0);
        assert_eq!(numbers(&mut s, 58..62).await, vec![58, 59, 60, 61]);
        assert_eq!(s.get_log_state().await.unwrap().last_log_id.unwrap().index(), 69);
        drop(s);
        let mut s = LogStore::open(d.path()).unwrap();
        assert_eq!(s.get_log_state().await.unwrap().last_purged_log_id, Some(purge));
        assert_eq!(numbers(&mut s, 0..100).await, (40..70).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_torn_end_is_cut_so_later_entries_survive() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("entries");
        let mut s = LogStore::open(d.path()).unwrap();
        put_all(&mut s, 0..5).await;
        drop(s);
        std::thread::sleep(std::time::Duration::from_millis(50));
        let whole = std::fs::read(&path).unwrap();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[200, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(f);

        let mut s = LogStore::open(d.path()).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), whole, "the torn bytes are gone");
        assert_eq!(std::fs::read(path.with_extension("dropped")).unwrap(), [200, 0, 0, 0, 1, 2, 3]);
        put_all(&mut s, 5..8).await;
        drop(s);
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut s = LogStore::open(d.path()).unwrap();
        assert_eq!(numbers(&mut s, 0..10).await, (0..8).collect::<Vec<_>>());
    }

    struct Builder;

    impl StoreBuilder<TC, LogStore, Arc<crate::sm::StateMachine>, tempfile::TempDir> for Builder {
        async fn build(&self) -> Result<(tempfile::TempDir, LogStore, Arc<crate::sm::StateMachine>), StorageError<TC>> {
            let d = tempfile::tempdir().unwrap();
            let wal = adb_log::LocalWal::open(adb_core::log::PartitionId(0), adb_log::WalConfig::new(d.path().join("wal"))).unwrap();
            let log = LogStore::open(d.path().join("raft")).unwrap();
            let sm = crate::sm::StateMachine::open(d.path().join("sm"), Arc::new(wal)).unwrap();
            Ok((d, log, Arc::new(sm)))
        }
    }

    /// openraft's own conformance suite. Its last step installs a snapshot on an empty
    /// state machine, which this one refuses by design: a snapshot here is a pointer
    /// (see `sm`). Every log store step runs before that one.
    #[tokio::test]
    async fn openraft_storage_suite() {
        let err = Suite::test_all(Builder).await.expect_err("the pointer snapshot is refused");
        assert!(err.to_string().contains("trimmed Raft log") || format!("{err:?}").contains("trimmed Raft log"), "{err:?}");
    }
}

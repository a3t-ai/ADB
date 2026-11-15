//! The local WAL: one partition's log as segment files on local disk (stage C).
//!
//! - **Files:** `{first_offset:020}.wal` in the log's directory; a new segment starts
//!   when the current one would pass `segment_bytes`.
//! - **Frames:** `[len: u32 LE][crc32: u32 LE][payload]`, payload = the record in
//!   MessagePack with field names (self-describing, so tagged effects and JSON
//!   payloads decode, and fields can be added later).
//! - **Durability:** one writer thread drains every queued append, writes them and
//!   calls `fdatasync` once (group commit). With [`WalSync::Ack`] (the default)
//!   `append` resolves only after that sync, so an acknowledged write survives a
//!   crash. [`WalSync::Behind`] resolves once the batch is written (page cache) and
//!   syncs right after, off the ack path: for an owner whose replica holds every
//!   record in memory, so the ack costs no disk wait (S3 option B). [`WalSync::Never`]
//!   never syncs: a reader's copy of a log it can rebuild from object storage.
//! - **Recovery:** on open, segments are scanned in order. A bad frame in the last
//!   segment is a torn tail from a crash: the file is cut back to the last good
//!   frame. A bad frame in an older segment is corruption and fails the open.
//! - **Retention:** `purge_below` deletes whole segments below an offset; the active
//!   segment is never deleted.
//! - **One writer:** `open` takes an exclusive lock on `LOCK` in the directory for the
//!   log's lifetime; a second process (e.g. a new pod during a rolling update) fails
//!   to open instead of corrupting the segments.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

use adb_core::log::{Epoch, LogOffset, LogRecord, PartitionId};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::{Log, LogError, LogResult};

const HEADER: usize = 8;
/// Largest frame accepted on recovery; anything bigger is a torn or corrupt length.
const MAX_FRAME: u32 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for this partition's segments; created if missing.
    pub dir: PathBuf,
    /// Roll to a new segment past this size.
    pub segment_bytes: u64,
    /// When an append is acknowledged relative to its `fdatasync`.
    pub sync: WalSync,
}

/// When the WAL syncs, relative to acknowledging an append.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalSync {
    /// Acknowledged after the group's `fdatasync`.
    #[default]
    Ack,
    /// Acknowledged once written (page cache); the `fdatasync` follows at once, off the
    /// ack path. Only where another node holds every acknowledged record.
    Behind,
    /// Never synced: a cache of a log held elsewhere.
    Never,
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        // `ADB_WAL_SEGMENT_BYTES` (at least 4 KiB): the log is purged a whole closed
        // segment at a time, so retention (`ADB_LOG_RETAIN`) is as fine as a segment.
        let segment_bytes = std::env::var("ADB_WAL_SEGMENT_BYTES").ok().and_then(|v| v.parse::<u64>().ok()).map_or(64 * 1024 * 1024, |b| b.max(4096));
        Self { dir: dir.into(), segment_bytes, sync: WalSync::Ack }
    }

    /// The same, syncing as `sync` says.
    pub fn with_sync(mut self, sync: WalSync) -> Self {
        self.sync = sync;
        self
    }
}

/// Where a durable record lives.
#[derive(Debug, Clone, Copy)]
struct Entry {
    segment: u64,
    pos: u64,
    len: u32,
}

#[derive(Debug)]
struct State {
    /// Offset of `index[0]`.
    first: LogOffset,
    index: Vec<Entry>,
    /// First offsets of the segments on disk, oldest first.
    segments: Vec<u64>,
    /// Highest offset accepted for appending (may not be durable yet).
    last: LogOffset,
    /// Highest durable offset.
    durable: LogOffset,
    epoch: Epoch,
    /// Set when a write or sync failed; the log refuses further appends.
    failed: Option<String>,
    /// Number of `fdatasync` calls (group-commit tests and metrics).
    syncs: u64,
}

struct Job {
    offset: LogOffset,
    frame: Vec<u8>,
    /// Start a new segment with this frame (W5).
    roll: bool,
    done: oneshot::Sender<LogResult<LogOffset>>,
}

/// A partition log on local disk.
pub struct LocalWal {
    partition: PartitionId,
    dir: PathBuf,
    state: Arc<Mutex<State>>,
    jobs: Option<mpsc::Sender<Job>>,
    writer: Option<JoinHandle<()>>,
    /// Readers hold it shared while they read segment files; `rewrite_closed`
    /// holds it exclusively while it swaps a rewritten segment and its index in.
    files: parking_lot::RwLock<()>,
    /// Held open (and exclusively locked) while the log is open.
    _lock: File,
}

impl std::fmt::Debug for LocalWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalWal").field("partition", &self.partition).field("dir", &self.dir).finish()
    }
}

fn io(e: impl std::fmt::Display) -> LogError {
    LogError::Io(e.to_string())
}

fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("{first:020}.wal"))
}

fn encode(record: &LogRecord) -> LogResult<Vec<u8>> {
    let payload = rmp_serde::to_vec_named(record).map_err(io)?;
    let mut frame = Vec::with_capacity(HEADER + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn decode(payload: &[u8]) -> LogResult<LogRecord> {
    rmp_serde::from_slice(payload).map_err(io)
}

/// Read one frame at the reader's position. `Ok(None)` = clean end of file;
/// `Err(())` = a torn or corrupt frame.
fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, ()> {
    let mut header = [0u8; HEADER];
    let mut got = 0;
    while got < HEADER {
        match reader.read(&mut header[got..]) {
            Ok(0) if got == 0 => return Ok(None),
            Ok(0) => return Err(()),
            Ok(n) => got += n,
            Err(_) => return Err(()),
        }
    }
    let len = u32::from_le_bytes(header[..4].try_into().unwrap());
    let crc = u32::from_le_bytes(header[4..].try_into().unwrap());
    if len > MAX_FRAME {
        return Err(());
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).map_err(|_| ())?;
    if crc32fast::hash(&payload) != crc {
        return Err(());
    }
    Ok(Some(payload))
}

impl LocalWal {
    /// Create an empty log in `dir` (which must hold none) whose first record will be
    /// `first`: a node restored from object storage up to `first - 1` (S3 option B).
    pub fn create_at(dir: &Path, first: LogOffset) -> LogResult<()> {
        fs::create_dir_all(dir).map_err(io)?;
        let existing = fs::read_dir(dir).map_err(io)?.flatten().any(|e| e.file_name().to_string_lossy().ends_with(".wal"));
        if existing {
            return Err(io(format!("{} already holds a log", dir.display())));
        }
        File::create(segment_path(dir, first.0.max(1))).map_err(io)?;
        sync_dir(dir)
    }

    /// Open (or create) the log in `config.dir`, recovering from a crash if needed.
    pub fn open(partition: PartitionId, config: WalConfig) -> LogResult<Self> {
        fs::create_dir_all(&config.dir).map_err(io)?;
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(config.dir.join("LOCK")).map_err(io)?;
        // Fails fast: a caller that may be reopening a log it just closed retries
        // asynchronously (`Locked`), so the closing tasks can finish meanwhile —
        // sleeping here would block them on a single-threaded runtime.
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(LogError::Locked(config.dir.display().to_string())),
            Err(std::fs::TryLockError::Error(e)) => return Err(io(e)),
        }
        let mut segments: Vec<u64> = fs::read_dir(&config.dir)
            .map_err(io)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str()?.strip_suffix(".wal")?.parse().ok())
            .collect();
        segments.sort_unstable();

        let mut index = Vec::new();
        let mut first = LogOffset(segments.first().copied().unwrap_or(1));
        let mut last = LogOffset(first.0 - 1);
        let mut epoch = Epoch::default();
        let mut tail_len = 0u64;

        for (i, &segment) in segments.iter().enumerate() {
            let is_last = i + 1 == segments.len();
            let path = segment_path(&config.dir, segment);
            if i == 0 {
                first = LogOffset(segment);
                last = LogOffset(segment - 1);
            } else if segment != last.0 + 1 {
                return Err(io(format!("gap before segment {}: last offset {}", path.display(), last.0)));
            }
            let mut reader = BufReader::new(File::open(&path).map_err(io)?);
            let mut pos = 0u64;
            loop {
                match read_frame(&mut reader) {
                    Ok(None) => break,
                    Ok(Some(payload)) => {
                        let record = match decode(&payload) {
                            Ok(r) if r.offset == last.next() && r.partition == partition => r,
                            _ if is_last => {
                                truncate(&path, pos)?;
                                break;
                            }
                            _ => return Err(io(format!("corrupt record in {} at {pos}", path.display()))),
                        };
                        index.push(Entry { segment, pos, len: payload.len() as u32 });
                        last = record.offset;
                        epoch = epoch.max(record.epoch);
                        pos += (HEADER + payload.len()) as u64;
                    }
                    Err(()) if is_last => {
                        truncate(&path, pos)?;
                        break;
                    }
                    Err(()) => return Err(io(format!("corrupt frame in {} at {pos}", path.display()))),
                }
            }
            if is_last {
                tail_len = pos;
            }
        }

        if segments.is_empty() {
            segments.push(1);
            File::create(segment_path(&config.dir, 1)).map_err(io)?;
            sync_dir(&config.dir)?;
        }

        let state = Arc::new(Mutex::new(State {
            first,
            index,
            segments: segments.clone(),
            last,
            durable: last,
            epoch,
            failed: None,
            syncs: 0,
        }));

        let active = *segments.last().unwrap();
        let file = OpenOptions::new().append(true).open(segment_path(&config.dir, active)).map_err(io)?;
        let (jobs, rx) = mpsc::channel::<Job>();
        let writer = {
            let state = Arc::clone(&state);
            let config = config.clone();
            std::thread::Builder::new()
                .name(format!("adb-wal-{}", partition.0))
                .spawn(move || writer_loop(rx, state, config, file, active, tail_len))
                .map_err(io)?
        };

        Ok(Self { partition, dir: config.dir, state, jobs: Some(jobs), writer: Some(writer), files: parking_lot::RwLock::new(()), _lock: lock })
    }

    /// Number of `fdatasync` calls so far.
    pub fn syncs(&self) -> u64 {
        self.state.lock().syncs
    }
}

/// An append queued with [`LocalWal::enqueue`]: resolves once its group's sync is done.
pub type WalWait = std::pin::Pin<Box<dyn std::future::Future<Output = LogResult<LogOffset>> + Send>>;

impl LocalWal {
    async fn append_job(&self, record: LogRecord, roll: bool) -> LogResult<LogOffset> {
        self.enqueue(record, roll)?.await
    }

    /// Queue `record` (on a new segment with `roll`) and return the wait for its sync,
    /// without waiting: a caller with several records queues them all, then waits, and
    /// one group sync covers them (the Raft state machine's apply, C-1). Records queue
    /// in the order of the calls.
    pub fn enqueue(&self, record: LogRecord, roll: bool) -> LogResult<WalWait> {
        if record.partition != self.partition {
            return Err(LogError::WrongPartition { log: self.partition, got: record.partition });
        }
        let frame = encode(&record)?;
        let (done, wait) = oneshot::channel();
        {
            let mut s = self.state.lock();
            if let Some(e) = &s.failed {
                return Err(LogError::Io(format!("log failed earlier: {e}")));
            }
            if record.epoch < s.epoch {
                return Err(LogError::StaleEpoch { current: s.epoch, got: record.epoch });
            }
            let expected = s.last.next();
            if record.offset != expected {
                return Err(LogError::OutOfOrder { expected, got: record.offset });
            }
            s.epoch = record.epoch;
            s.last = record.offset;
            // Sent under the lock, so the writer sees appends in offset order.
            self.jobs
                .as_ref()
                .expect("open log")
                .send(Job { offset: record.offset, frame, done, roll })
                .map_err(|_| LogError::Io("WAL writer stopped".into()))?;
        }
        Ok(Box::pin(async move { wait.await.map_err(|_| LogError::Io("WAL writer stopped".into()))? }))
    }
}

fn truncate(path: &Path, len: u64) -> LogResult<()> {
    tracing::warn!(path = %path.display(), len, "WAL: cutting a torn tail");
    let file = OpenOptions::new().write(true).open(path).map_err(io)?;
    file.set_len(len).map_err(io)?;
    file.sync_all().map_err(io)
}

fn sync_dir(dir: &Path) -> LogResult<()> {
    File::open(dir).and_then(|d| d.sync_all()).map_err(io)
}

fn writer_loop(
    rx: mpsc::Receiver<Job>,
    state: Arc<Mutex<State>>,
    config: WalConfig,
    mut file: File,
    mut segment: u64,
    mut size: u64,
) {
    while let Ok(job) = rx.recv() {
        let mut batch = vec![job];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }
        adb_core::steps::value("wal records per sync", batch.len() as u64);
        let _t = adb_core::steps::timer("wal writer write + fsync");

        let mut written = Vec::with_capacity(batch.len());
        let sync = |file: &File| match config.sync {
            WalSync::Never => Ok(()),
            _ => file.sync_data().map_err(io),
        };
        let result = (|| -> LogResult<()> {
            for job in &batch {
                if size > 0 && (job.roll || size + job.frame.len() as u64 > config.segment_bytes) {
                    sync(&file)?;
                    segment = job.offset.0;
                    file = OpenOptions::new()
                        .create_new(true)
                        .append(true)
                        .open(segment_path(&config.dir, segment))
                        .map_err(io)?;
                    if config.sync != WalSync::Never {
                        sync_dir(&config.dir)?;
                    }
                    state.lock().segments.push(segment);
                    size = 0;
                }
                file.write_all(&job.frame).map_err(io)?;
                written.push(Entry { segment, pos: size, len: (job.frame.len() - HEADER) as u32 });
                size += job.frame.len() as u64;
            }
            match config.sync {
                WalSync::Ack => sync(&file),
                WalSync::Behind | WalSync::Never => Ok(()),
            }
        })();

        let mut s = state.lock();
        match result {
            Ok(()) => {
                if config.sync == WalSync::Ack {
                    s.syncs += 1;
                }
                s.index.extend(written);
                s.durable = batch.last().unwrap().offset;
                drop(s);
                for job in batch {
                    let _ = job.done.send(Ok(job.offset));
                }
                // Behind: acknowledged from the page cache; synced now, before the next
                // batch. A failed sync stops the log (as a failed write does).
                if config.sync == WalSync::Behind {
                    let r = file.sync_data();
                    let mut s = state.lock();
                    match r {
                        Ok(()) => s.syncs += 1,
                        Err(e) => s.failed = Some(e.to_string()),
                    }
                }
            }
            Err(e) => {
                // Nothing in this batch is acknowledged; the log stops accepting writes.
                // Reopening recovers to the last durable frame.
                s.failed = Some(e.to_string());
                drop(s);
                for job in batch {
                    let _ = job.done.send(Err(e.clone()));
                }
            }
        }
    }
    let _ = file.sync_data();
}

impl Drop for LocalWal {
    fn drop(&mut self) {
        // Closing the channel lets the writer drain queued appends and exit.
        self.jobs.take();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

#[async_trait]
impl Log for LocalWal {
    fn partition(&self) -> PartitionId {
        self.partition
    }

    async fn append(&self, record: LogRecord) -> LogResult<LogOffset> {
        self.append_job(record, false).await
    }

    async fn append_on_new_segment(&self, record: LogRecord) -> LogResult<LogOffset> {
        self.append_job(record, true).await
    }

    async fn read(&self, from: LogOffset, max: usize) -> LogResult<Vec<LogRecord>> {
        let _files = self.files.read();
        let entries = {
            let s = self.state.lock();
            if from < s.first {
                return Err(LogError::Purged { requested: from, oldest: s.first });
            }
            let start = (from.0 - s.first.0) as usize;
            s.index.iter().skip(start).take(max).copied().collect::<Vec<_>>()
        };
        let mut out = Vec::with_capacity(entries.len());
        let mut open: Option<(u64, File)> = None;
        for e in entries {
            if open.as_ref().map(|(seg, _)| *seg) != Some(e.segment) {
                open = Some((e.segment, File::open(segment_path(&self.dir, e.segment)).map_err(io)?));
            }
            let file = &open.as_ref().unwrap().1;
            let mut payload = vec![0u8; e.len as usize];
            file.read_exact_at(&mut payload, e.pos + HEADER as u64).map_err(io)?;
            out.push(decode(&payload)?);
        }
        Ok(out)
    }

    fn last_offset(&self) -> LogOffset {
        self.state.lock().last
    }

    fn durable_offset(&self) -> LogOffset {
        self.state.lock().durable
    }

    async fn rewrite_closed(&self, redact: &(dyn for<'r> Fn(&'r mut LogRecord) -> bool + Sync)) -> LogResult<usize> {
        // The closed segments and their index entries (the last segment is the
        // writer's: never rewritten).
        let closed: Vec<(u64, LogOffset, Vec<Entry>)> = {
            let s = self.state.lock();
            let n = s.segments.len().saturating_sub(1);
            let mut out = Vec::new();
            for (k, &seg) in s.segments[..n].iter().enumerate() {
                let next = s.segments[k + 1];
                let from = LogOffset(seg.max(s.first.0));
                if from.0 >= next {
                    continue;
                }
                let start = (from.0 - s.first.0) as usize;
                let len = (next - from.0) as usize;
                out.push((seg, from, s.index[start..(start + len).min(s.index.len())].to_vec()));
            }
            out
        };
        let mut changed = 0;
        for (seg, from, entries) in closed {
            let path = segment_path(&self.dir, seg);
            // One record at a time: a whole segment decoded at once is many times its size
            // on disk, and on a real log that is hundreds of MiB for each FORGET, which
            // glibc then keeps in the calling thread's arena (leader memory, CA-53).
            let source = File::open(&path).map_err(io)?;
            let mut payload = Vec::new();
            let mut read = |e: &Entry, payload: &mut Vec<u8>| -> LogResult<LogRecord> {
                payload.resize(e.len as usize, 0);
                source.read_exact_at(payload, e.pos + HEADER as u64).map_err(io)?;
                decode(payload)
            };
            // Most segments hold nothing forgotten: stop at the first record that does.
            let mut touched = false;
            for e in &entries {
                let mut r = read(e, &mut payload)?;
                if redact(&mut r) {
                    touched = true;
                    break;
                }
            }
            if !touched {
                continue;
            }
            // The rewritten segment, complete and synced, then swapped in with its index.
            let tmp = path.with_extension("wal.rewrite");
            let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp).map_err(io)?;
            let mut new_entries = Vec::with_capacity(entries.len());
            let mut pos = 0u64;
            for e in &entries {
                let mut r = read(e, &mut payload)?;
                if redact(&mut r) {
                    changed += 1;
                }
                let frame = encode(&r)?;
                file.write_all(&frame).map_err(io)?;
                new_entries.push(Entry { segment: seg, pos, len: (frame.len() - HEADER) as u32 });
                pos += frame.len() as u64;
            }
            file.sync_all().map_err(io)?;
            drop(file);
            let _files = self.files.write();
            fs::rename(&tmp, &path).map_err(io)?;
            sync_dir(&self.dir)?;
            let mut s = self.state.lock();
            if from >= s.first {
                let start = (from.0 - s.first.0) as usize;
                for (k, e) in new_entries.into_iter().enumerate() {
                    if let Some(slot) = s.index.get_mut(start + k) {
                        *slot = e;
                    }
                }
            }
        }
        Ok(changed)
    }

    async fn purge_below(&self, offset: LogOffset) -> LogResult<()> {
        let doomed: Vec<u64> = {
            let mut s = self.state.lock();
            // A segment can go when the next one starts at or below `offset`.
            let keep_from = s
                .segments
                .windows(2)
                .take_while(|w| w[1] <= offset.0)
                .count();
            if keep_from == 0 {
                return Ok(());
            }
            let doomed: Vec<u64> = s.segments.drain(..keep_from).collect();
            let new_first = LogOffset(s.segments[0]);
            let dropped = ((new_first.0 - s.first.0) as usize).min(s.index.len());
            s.index.drain(..dropped);
            s.first = new_first;
            doomed
        };
        for segment in doomed {
            fs::remove_file(segment_path(&self.dir, segment)).map_err(io)?;
        }
        sync_dir(&self.dir)
    }
}

/// Size of a log directory on disk (tests and metrics).
pub fn dir_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|it| it.filter_map(|e| e.ok()?.metadata().ok()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::log::{CommitTs, Effect, RecordId};

    /// Deterministic records, so tests can compare what they wrote with what they read.
    fn record(offset: u64) -> LogRecord {
        let ts = CommitTs(1_761_300_000_000_000 + offset as i64);
        LogRecord {
            partition: PartitionId(5),
            offset: LogOffset(offset),
            epoch: Epoch(1),
            commit_ts: ts,
            txn_id: Some(format!("txn-{offset}")),
            effects: vec![Effect::Forget { memory: adb_core::log::Memory::Working, lineage_id: RecordId(format!("01K8LINEAGE{offset:015}")) }],
        }
    }

    fn open(dir: &Path, segment_bytes: u64) -> LocalWal {
        LocalWal::open(PartitionId(5), WalConfig { dir: dir.to_path_buf(), segment_bytes, sync: WalSync::Ack }).unwrap()
    }

    #[tokio::test]
    async fn behind_acks_then_syncs_and_never_does_not_sync() {
        for (mode, synced) in [(WalSync::Behind, true), (WalSync::Never, false)] {
            let dir = tempfile::tempdir().unwrap();
            {
                let wal = LocalWal::open(PartitionId(5), WalConfig { dir: dir.path().to_path_buf(), segment_bytes: 300, sync: mode }).unwrap();
                for o in 1..=20 {
                    wal.append(record(o)).await.unwrap();
                }
                assert_eq!(wal.durable_offset(), LogOffset(20));
                // Behind: each batch is synced right after its ack.
                for _ in 0..100 {
                    if !synced || wal.syncs() > 0 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                assert_eq!(wal.syncs() > 0, synced, "{mode:?}");
            }
            // Written either way: a reopen (no power cut) has every record.
            let wal = LocalWal::open(PartitionId(5), WalConfig { dir: dir.path().to_path_buf(), segment_bytes: 300, sync: mode }).unwrap();
            assert_eq!(wal.read(LogOffset(1), 100).await.unwrap().len(), 20, "{mode:?}");
        }
    }

    #[tokio::test]
    async fn records_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = open(dir.path(), 1 << 20);
            for i in 1..=10 {
                assert_eq!(wal.append(record(i)).await.unwrap(), LogOffset(i));
            }
        }
        let wal = open(dir.path(), 1 << 20);
        assert_eq!(wal.last_offset(), LogOffset(10));
        assert_eq!(wal.durable_offset(), LogOffset(10));
        let got = wal.read(LogOffset(1), 100).await.unwrap();
        assert_eq!(got, (1..=10).map(record).collect::<Vec<_>>());
        wal.append(record(11)).await.unwrap();
    }

    #[tokio::test]
    async fn append_on_new_segment_lets_everything_before_it_be_purged() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open(dir.path(), 64 * 1024 * 1024);
        for i in 1..=5 {
            wal.append(record(i)).await.unwrap();
        }
        wal.append_on_new_segment(record(6)).await.unwrap();
        wal.append(record(7)).await.unwrap();
        wal.purge_below(LogOffset(6)).await.unwrap();
        assert!(matches!(wal.read(LogOffset(5), 1).await, Err(LogError::Purged { .. })), "records before the roll are gone");
        assert_eq!(wal.read(LogOffset(6), 10).await.unwrap().len(), 2);
        let segments = std::fs::read_dir(dir.path()).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".wal")).count();
        assert_eq!(segments, 1);
        drop(wal);
        let wal = open(dir.path(), 64 * 1024 * 1024);
        assert_eq!(wal.last_offset(), LogOffset(7), "reopens after the roll");
    }

    #[tokio::test]
    async fn rolls_segments_reads_across_them_and_purges() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open(dir.path(), 300); // a few records per segment
        for i in 1..=30 {
            wal.append(record(i)).await.unwrap();
        }
        let segments = fs::read_dir(dir.path()).unwrap().count();
        assert!(segments > 3, "expected several segments, got {segments}");
        assert_eq!(wal.read(LogOffset(5), 20).await.unwrap(), (5..=24).map(record).collect::<Vec<_>>());

        wal.purge_below(LogOffset(20)).await.unwrap();
        assert!(fs::read_dir(dir.path()).unwrap().count() < segments);
        assert!(matches!(wal.read(LogOffset(1), 1).await, Err(LogError::Purged { .. })));
        assert_eq!(wal.read(LogOffset(20), 100).await.unwrap().len(), 11);
        drop(wal);

        // The purged log reopens at its oldest retained segment.
        let wal = open(dir.path(), 300);
        assert_eq!(wal.last_offset(), LogOffset(30));
        assert_eq!(wal.read(LogOffset(25), 100).await.unwrap(), (25..=30).map(record).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn torn_tail_is_cut_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = open(dir.path(), 1 << 20);
            for i in 1..=3 {
                wal.append(record(i)).await.unwrap();
            }
        }
        // A crash halfway through writing record 4: half a frame at the end.
        let path = segment_path(dir.path(), 1);
        let frame = encode(&record(4)).unwrap();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&frame[..frame.len() / 2]).unwrap();
        drop(f);
        let before = fs::metadata(&path).unwrap().len();

        let wal = open(dir.path(), 1 << 20);
        assert_eq!(wal.last_offset(), LogOffset(3));
        assert!(fs::metadata(&path).unwrap().len() < before);
        // The log carries on from offset 4.
        wal.append(record(4)).await.unwrap();
        drop(wal);
        assert_eq!(open(dir.path(), 1 << 20).read(LogOffset(1), 10).await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn flipped_bit_in_the_tail_drops_that_record() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = open(dir.path(), 1 << 20);
            for i in 1..=3 {
                wal.append(record(i)).await.unwrap();
            }
        }
        let path = segment_path(dir.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 3] ^= 0x40; // inside record 3's payload
        fs::write(&path, bytes).unwrap();
        let wal = open(dir.path(), 1 << 20);
        assert_eq!(wal.last_offset(), LogOffset(2));
    }

    #[tokio::test]
    async fn corruption_in_an_old_segment_fails_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = open(dir.path(), 300);
            for i in 1..=20 {
                wal.append(record(i)).await.unwrap();
            }
        }
        let path = segment_path(dir.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER + 2] ^= 0x01;
        fs::write(&path, bytes).unwrap();
        assert!(LocalWal::open(PartitionId(5), WalConfig { dir: dir.path().to_path_buf(), segment_bytes: 300, sync: Default::default() }).is_err());
    }

    #[tokio::test]
    async fn concurrent_appends_share_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(open(dir.path(), 1 << 20));
        // Queue 200 appends before awaiting any: the writer batches them.
        let pending: Vec<_> = (1..=200).map(|i| {
            let wal = Arc::clone(&wal);
            let rec = record(i);
            async move { wal.append(rec).await }
        }).collect();
        // Appends must be submitted in offset order; join_all polls them in order.
        let results = futures_join_all(pending).await;
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(wal.durable_offset(), LogOffset(200));
        assert!(wal.syncs() < 200, "group commit: {} syncs for 200 appends", wal.syncs());
    }

    /// Minimal join_all: poll every future in order until all finish.
    async fn futures_join_all<F: std::future::Future>(futs: Vec<F>) -> Vec<F::Output> {
        let mut handles = Vec::new();
        for f in futs {
            handles.push(Box::pin(f));
        }
        // First poll each in order so offsets are submitted in order.
        let mut out: Vec<Option<F::Output>> = (0..handles.len()).map(|_| None).collect();
        std::future::poll_fn(|cx| {
            let mut done = true;
            for (i, h) in handles.iter_mut().enumerate() {
                if out[i].is_none() {
                    match h.as_mut().poll(cx) {
                        std::task::Poll::Ready(v) => out[i] = Some(v),
                        std::task::Poll::Pending => done = false,
                    }
                }
            }
            if done { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
        })
        .await;
        out.into_iter().map(|o| o.unwrap()).collect()
    }

    #[tokio::test]
    async fn a_second_opener_is_refused_until_the_first_closes() {
        let dir = tempfile::tempdir().unwrap();
        let first = open(dir.path(), 1 << 20);
        let second = LocalWal::open(PartitionId(5), WalConfig::new(dir.path()));
        assert!(matches!(second, Err(LogError::Locked(_))), "{second:?}");
        drop(first);
        assert!(LocalWal::open(PartitionId(5), WalConfig::new(dir.path())).is_ok());
    }

    #[tokio::test]
    async fn rejects_out_of_order_and_stale_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open(dir.path(), 1 << 20);
        let mut r = record(1);
        r.epoch = Epoch(3);
        wal.append(r).await.unwrap();
        let mut gap = record(3);
        gap.epoch = Epoch(3);
        assert!(matches!(wal.append(gap).await, Err(LogError::OutOfOrder { .. })));
        assert!(matches!(wal.append(record(2)).await, Err(LogError::StaleEpoch { .. })));
        drop(wal);
        // The epoch is recovered on reopen, so a stale owner stays fenced.
        let wal = open(dir.path(), 1 << 20);
        assert!(matches!(wal.append(record(2)).await, Err(LogError::StaleEpoch { .. })));
    }

    #[test]
    fn frames_are_small() {
        let frame = encode(&record(1)).unwrap();
        assert!(frame.len() < 200, "frame is {} bytes", frame.len());
    }
}

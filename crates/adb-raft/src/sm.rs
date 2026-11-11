//! The Raft state machine: the node's WAL.
//!
//! Applying a committed entry appends its ADB record to the WAL at the record's
//! own offset (one already there is skipped: openraft re-applies entries above
//! the last persisted `applied` after a restart). Raft's blank and membership
//! entries never reach the WAL, so ADB offsets stay contiguous.
//!
//! A snapshot is a pointer (the last applied entry and the membership): the
//! data is the WAL and the tables built from it. Installing one is only possible
//! when this node already holds everything up to it; a node that fell behind
//! openraft's trimmed log needs a copy of a peer's data directory (limitation).

use std::io::{self, Cursor};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use adb_log::{LocalWal, Log};
use futures::{Stream, TryStreamExt};
use openraft::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use serde::{Deserialize, Serialize};

use crate::store::{get, put};
use crate::{Applied, TypeConfig as TC};

#[derive(Default, Clone, Serialize, Deserialize)]
struct Persisted {
    last_applied: Option<LogIdOf<TC>>,
    membership: StoredMembershipOf<TC>,
}

pub struct StateMachine {
    dir: PathBuf,
    wal: Arc<LocalWal>,
    /// What openraft has handed over (its view of `applied`).
    state: Mutex<Persisted>,
    /// What the WAL has made durable: the only `applied` written to disk or put in a
    /// snapshot, so a restart re-applies everything the WAL may have lost (PF-44).
    durable: Mutex<Persisted>,
    snapshot: Mutex<Option<SnapshotMetaOf<TC>>>,
    /// When `applied` was last written (with its sync): at most once a second (C-2).
    saved_at: Mutex<Option<std::time::Instant>>,
    saving: Mutex<()>,
}

/// How stale the `applied` file may be: after a crash, openraft re-applies from it, and
/// entries the WAL already holds are skipped, so a stale value costs a few skipped
/// entries, never a wrong state.
const SAVE_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

impl StateMachine {
    pub fn open(dir: impl Into<PathBuf>, wal: Arc<LocalWal>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let state: Persisted = get(&dir, "applied")?.unwrap_or_default();
        let snapshot: Option<SnapshotMetaOf<TC>> = get(&dir, "snapshot")?;
        Ok(Self { dir, wal, durable: Mutex::new(state.clone()), state: Mutex::new(state), snapshot: Mutex::new(snapshot), saved_at: Mutex::new(None), saving: Mutex::new(()) })
    }

    fn save(&self) -> io::Result<()> {
        // One writer at a time: the WAL waits now finish in their own tasks (PF-44),
        // and two saves at once raced on the temporary file.
        let _one = self.saving.lock().unwrap();
        let s = self.durable.lock().unwrap().clone();
        put(&self.dir, "applied", &s)?;
        *self.saved_at.lock().unwrap() = Some(std::time::Instant::now());
        Ok(())
    }

    /// [`Self::save`] when `must` (membership changed, a snapshot) or the last save is
    /// older than [`SAVE_EVERY`].
    fn save_due(&self, must: bool) -> io::Result<()> {
        let due = must || self.saved_at.lock().unwrap().is_none_or(|t| t.elapsed() >= SAVE_EVERY);
        if due {
            self.save()?;
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<TC> for Arc<StateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TC, Cursor<Vec<u8>>>, io::Error> {
        // The snapshot names the last applied entry: that entry's `applied` is on disk
        // first (C-2 writes it only once a second otherwise), and so is its WAL record:
        // wait for the WAL to make durable what was handed over (PF-44).
        let want = self.state.lock().unwrap().last_applied.as_ref().map(|l| l.index);
        for _ in 0..10_000 {
            if self.durable.lock().unwrap().last_applied.as_ref().map(|l| l.index) >= want {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        self.save()?;
        let s = self.durable.lock().unwrap().clone();
        let meta = SnapshotMetaOf::<TC> {
            last_log_id: s.last_applied,
            last_membership: s.membership,
        };
        put(&self.dir, "snapshot", &meta)?;
        *self.snapshot.lock().unwrap() = Some(meta.clone());
        Ok(SnapshotOf::<TC, Cursor<Vec<u8>>> { meta, snapshot: Cursor::new(Vec::new()) })
    }
}

impl RaftStateMachine<TC> for Arc<StateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogIdOf<TC>>, StoredMembershipOf<TC>), io::Error> {
        let s = self.state.lock().unwrap();
        Ok((s.last_applied.clone(), s.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TC>, io::Error>> + Unpin + OptionalSend,
    {
        // C-1: every record of the batch is queued to the WAL first, then all are waited
        // for together (one group sync), and only then is anyone answered. Awaiting each
        // append before the next made every entry pay its own sync, in series.
        let at0 = std::time::Instant::now();
        let mut waits = Vec::new();
        let mut answers = Vec::new();
        let mut membership = false;
        while let Some((entry, responder)) = entries.try_next().await? {
            let mut answer = Applied(self.wal.last_offset().0);
            match entry.payload {
                EntryPayload::Normal(cmd) => {
                    let at = cmd.record.offset;
                    let last = self.wal.last_offset();
                    if at.0 > last.0 {
                        if at.0 != last.0 + 1 {
                            return Err(io::Error::other(format!("raft entry {} holds WAL offset {} but the WAL ends at {}", entry.log_id.index, at.0, last.0)));
                        }
                        let w = self.wal.enqueue(cmd.record, cmd.new_segment).map_err(|e| io::Error::other(format!("WAL append at {}: {e}", at.0)))?;
                        waits.push((at.0, w));
                    }
                    answer = Applied(at.0);
                }
                EntryPayload::Membership(m) => {
                    self.state.lock().unwrap().membership = StoredMembershipOf::<TC>::new(Some(entry.log_id.clone()), m);
                    membership = true;
                }
                EntryPayload::Blank => {}
            }
            self.state.lock().unwrap().last_applied = Some(entry.log_id.clone());
            answers.push((responder, answer));
        }
        adb_core::steps::value("raft.sm entries per apply", answers.len() as u64);
        adb_core::steps::add("raft.sm.1 enqueue to WAL", at0.elapsed());
        let handed = self.state.lock().unwrap().clone();
        // PF-44: the WAL's sync is waited for off the apply path. Waiting here held every
        // apply for one fsync, and openraft hands over one commit (~1 entry) per apply:
        // ~700 writes/s on any hardware, 230 ms queued between commit and apply (AWS
        // r8). Now the next apply queues behind this one in the WAL writer, whose group
        // sync covers everything queued. Clients are still answered only once their
        // record is durable in the WAL, and `applied` on disk only covers durable records.
        let me = self.clone();
        let finish = async move {
            let wt = std::time::Instant::now();
            for (at, w) in waits {
                if let Err(e) = w.await {
                    // Answers dropped: the leader's writer sees "not committed", stops
                    // taking writes and restarts (PF-42); openraft re-applies on restart.
                    tracing::error!(offset = at, error = %e, "WAL append failed after commit; this node's writes stop until it restarts");
                    return;
                }
            }
            adb_core::steps::add("raft.sm.2 WAL durable wait (off the apply path)", wt.elapsed());
            {
                let mut d = me.durable.lock().unwrap();
                if handed.last_applied.as_ref().map(|l| l.index) > d.last_applied.as_ref().map(|l| l.index) {
                    *d = handed;
                }
            }
            for (responder, answer) in answers {
                if let Some(r) = responder {
                    r.send(answer);
                }
            }
            // C-2: `applied` is written at most once a second (always on a membership
            // change); a stale value only makes openraft re-apply entries the WAL skips.
            if let Err(e) = me.save_due(membership) {
                tracing::warn!(error = %e, "raft applied state not saved; a restart re-applies more");
            }
        };
        if membership {
            // A membership change is rare: done in line, as before.
            finish.await;
        } else {
            tokio::spawn(finish);
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// A pointer snapshot: accepted only if this node's WAL already reaches it.
    async fn install_snapshot(&mut self, meta: &SnapshotMetaOf<TC>, _snapshot: Self::SnapshotData) -> Result<(), io::Error> {
        let mine = self.state.lock().unwrap().last_applied.clone();
        let theirs = meta.last_log_id.clone();
        let covered = match (&mine, &theirs) {
            (_, None) => true,
            (Some(m), Some(t)) => m.index >= t.index,
            (None, Some(_)) => false,
        };
        if !covered {
            return Err(io::Error::other(format!(
                "this node is behind the replica set's trimmed Raft log (applied {:?}, snapshot at {:?}): copy a peer's data directory (WAL {} here)",
                mine.map(|l| l.index),
                theirs.map(|l| l.index),
                self.wal.last_offset().0
            )));
        }
        put(&self.dir, "snapshot", meta)?;
        *self.snapshot.lock().unwrap() = Some(meta.clone());
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TC, Self::SnapshotData>>, io::Error> {
        Ok(self.snapshot.lock().unwrap().clone().map(|meta| SnapshotOf::<TC, Cursor<Vec<u8>>> { meta, snapshot: Cursor::new(Vec::new()) }))
    }
}

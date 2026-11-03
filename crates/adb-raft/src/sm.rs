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
    state: Mutex<Persisted>,
    snapshot: Mutex<Option<SnapshotMetaOf<TC>>>,
}

impl StateMachine {
    pub fn open(dir: impl Into<PathBuf>, wal: Arc<LocalWal>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let state: Persisted = get(&dir, "applied")?.unwrap_or_default();
        let snapshot: Option<SnapshotMetaOf<TC>> = get(&dir, "snapshot")?;
        Ok(Self { dir, wal, state: Mutex::new(state), snapshot: Mutex::new(snapshot) })
    }

    fn save(&self) -> io::Result<()> {
        let s = self.state.lock().unwrap().clone();
        put(&self.dir, "applied", &s)
    }
}

impl RaftSnapshotBuilder<TC> for Arc<StateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TC, Cursor<Vec<u8>>>, io::Error> {
        let s = self.state.lock().unwrap().clone();
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
        let mut changed = false;
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
                        let r = if cmd.new_segment { self.wal.append_on_new_segment(cmd.record).await } else { self.wal.append(cmd.record).await };
                        r.map_err(|e| io::Error::other(format!("WAL append at {}: {e}", at.0)))?;
                    }
                    answer = Applied(at.0);
                }
                EntryPayload::Membership(m) => {
                    self.state.lock().unwrap().membership = StoredMembershipOf::<TC>::new(Some(entry.log_id.clone()), m);
                }
                EntryPayload::Blank => {}
            }
            self.state.lock().unwrap().last_applied = Some(entry.log_id.clone());
            changed = true;
            if let Some(r) = responder {
                r.send(answer);
            }
        }
        if changed {
            self.save()?;
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

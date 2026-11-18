//! The newest episodes of each namespace, kept in step with every apply, so a
//! newest-first read (`RECALL FROM EPISODIC … LIMIT k`, no WHERE / ORDER BY) takes k
//! rows by id instead of reading and sorting every episode of the namespace
//! (Hetzner p14: ~1k/s cluster-wide, each read queued behind the scan permits).
//!
//! - **What it holds.** Per namespace, the ids of its newest live episodes (up to
//!   `ADB_RECENT_EPISODES`, default 256) by commit time. An episode forgotten or
//!   superseded leaves; past the bound the oldest leaves and the namespace is marked
//!   as having older ones.
//! - **What it answers.** The newest k when it holds at least k of a namespace's
//!   episodes, or all of them when none was ever pushed out — everything newer than
//!   its oldest entry is in it, so its first k are the namespace's first k. Otherwise
//!   `None`, and the caller reads as before. Time bounds (TTL) are checked on the rows.
//! - **When it is built.** In the background after open, from one scan of the current
//!   episodes; applies made meanwhile are held and replayed on top (as the live keys).

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use adb_core::log::{CommitTs, Kind, Memory};

use crate::rows::StoredRow;
use crate::schema::META_ID;

/// Episodes kept per namespace (`ADB_RECENT_EPISODES`, default 256): the largest
/// newest-first LIMIT it answers.
pub fn keep() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::env::var("ADB_RECENT_EPISODES").ok().and_then(|v| v.parse().ok()).unwrap_or(256))
}

#[derive(Debug, Clone)]
enum Change {
    Put { namespace: String, at: CommitTs, id: String },
    Gone { id: String },
}

#[derive(Default)]
struct Space {
    newest: BTreeSet<(CommitTs, String)>,
    /// An older episode was pushed out (the set isn't the namespace's every episode).
    evicted: bool,
}

#[derive(Default)]
struct Index {
    spaces: HashMap<String, Space>,
    /// Row id → (namespace, commit time), for the episodes held.
    held: HashMap<String, (String, CommitTs)>,
}

enum State {
    Off,
    Building(Vec<Change>),
    Ready(Index),
}

pub struct RecentIndex {
    state: Mutex<State>,
}

impl Default for RecentIndex {
    fn default() -> Self {
        Self { state: Mutex::new(State::Off) }
    }
}

fn changes(rows: &[StoredRow]) -> Vec<Change> {
    rows.iter()
        .filter(|r| r.id != META_ID && r.data.memory == Memory::Episodic && r.data.kind == Kind::Episode)
        .map(|r| match r.tombstone || r.superseded_by.is_some() {
            true => Change::Gone { id: r.id.clone() },
            false => Change::Put { namespace: r.data.namespace.clone().unwrap_or_default(), at: r.commit_ts, id: r.id.clone() },
        })
        .collect()
}

fn apply(ix: &mut Index, c: Change, keep: usize) {
    match c {
        Change::Put { namespace, at, id } => {
            if let Some((ns, t)) = ix.held.remove(&id) {
                if let Some(s) = ix.spaces.get_mut(&ns) {
                    s.newest.remove(&(t, id.clone()));
                }
            }
            let s = ix.spaces.entry(namespace.clone()).or_default();
            s.newest.insert((at, id.clone()));
            ix.held.insert(id, (namespace, at));
            while s.newest.len() > keep {
                if let Some((_, old)) = s.newest.pop_first() {
                    ix.held.remove(&old);
                    s.evicted = true;
                }
            }
        }
        Change::Gone { id } => {
            if let Some((ns, t)) = ix.held.remove(&id) {
                if let Some(s) = ix.spaces.get_mut(&ns) {
                    s.newest.remove(&(t, id));
                }
            }
        }
    }
}

impl RecentIndex {
    /// Keep the index (or the build in progress) in step with rows just applied.
    pub fn apply(&self, rows: &[StoredRow]) {
        let mut s = self.state.lock().unwrap();
        match &mut *s {
            State::Off => {}
            State::Building(held) => held.extend(changes(rows)),
            State::Ready(ix) => changes(rows).into_iter().for_each(|c| apply(ix, c, keep())),
        }
    }

    /// Start a build: `true` if the caller should now scan and call [`Self::finish`].
    pub fn begin(&self) -> bool {
        let mut s = self.state.lock().unwrap();
        if !matches!(*s, State::Off) {
            return false;
        }
        *s = State::Building(Vec::new());
        true
    }

    /// The scan's current episodes, then what was applied meanwhile.
    pub fn finish(&self, scanned: &[StoredRow]) {
        let mut ix = Index::default();
        for c in changes(scanned) {
            apply(&mut ix, c, keep());
        }
        let mut s = self.state.lock().unwrap();
        if let State::Building(held) = std::mem::replace(&mut *s, State::Off) {
            held.into_iter().for_each(|c| apply(&mut ix, c, keep()));
            tracing::info!(namespaces = ix.spaces.len(), episodes = ix.held.len(), "recent episodes indexed");
            *s = State::Ready(ix);
        }
    }

    pub fn abandon(&self) {
        *self.state.lock().unwrap() = State::Off;
    }

    pub fn is_ready(&self) -> bool {
        matches!(*self.state.lock().unwrap(), State::Ready(_))
    }

    /// The ids of `namespace`'s newest `k` episodes, newest first (ties by id, as the
    /// default order), when the index holds them; `None` otherwise.
    pub fn newest(&self, namespace: &str, k: usize) -> Option<Vec<String>> {
        let s = self.state.lock().unwrap();
        let State::Ready(ix) = &*s else { return None };
        let Some(space) = ix.spaces.get(namespace) else { return Some(Vec::new()) };
        if k > space.newest.len() && space.evicted {
            return None;
        }
        Some(space.newest.iter().rev().take(k).map(|(_, id)| id.clone()).collect())
    }
}

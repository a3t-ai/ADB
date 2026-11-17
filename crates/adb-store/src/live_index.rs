//! The live records' keys, kept in step with every apply (PF-38), so a follower that
//! takes the lead hands the write path its key set instead of rescanning the table
//! (8.4 s at 1.6M rows on a 2-vCPU node, inside the takeover).
//!
//! - **What it holds.** Per (memory type, lineage): the row id of its live version and
//!   that version's `ttl_at`. A row that is current (not forgotten, not superseded)
//!   puts its lineage in; a forgotten or superseded row takes it out if it is the
//!   version held. Liveness in time (`valid_to`, `ttl_at`) is checked when the keys
//!   are taken, as `StoredRow::is_live` does.
//! - **When it is built.** On request ([`LiveIndex::begin`]), from one projected scan
//!   of the key columns; applies made meanwhile are held and replayed on top.
//! - **Who takes it.** The write path, once, when this node becomes the leader
//!   ([`LiveIndex::take`]): from then on the leader keeps its own key set (it must see
//!   writes committed but not applied yet), and this one is off.

use std::collections::HashMap;
use std::sync::Mutex;

use adb_core::log::{CommitTs, Kind, Memory};

use crate::rows::{LiveKey, StoredRow};
use crate::schema::META_ID;

/// One row's effect on the key set.
#[derive(Debug, Clone)]
enum Change {
    /// A current version: its lineage is live at this row.
    Live { memory: Memory, lineage: String, id: String, ttl_at: Option<CommitTs> },
    /// A forgotten or superseded version: its lineage leaves if this row is the one held.
    Gone { memory: Memory, lineage: String, id: String },
}

type Keys = HashMap<(Memory, String), (String, Option<CommitTs>)>;

enum State {
    Off,
    Building(Vec<Change>),
    Ready(Keys),
}

pub struct LiveIndex {
    state: Mutex<State>,
}

impl Default for LiveIndex {
    fn default() -> Self {
        Self::new()
    }
}

fn changes(rows: &[StoredRow]) -> Vec<Change> {
    let mut gone = Vec::new();
    let mut live = Vec::new();
    for r in rows.iter().filter(|r| r.id != META_ID && matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool)) {
        let (memory, lineage, id) = (r.data.memory, r.lineage_id.clone(), r.id.clone());
        if r.tombstone || r.superseded_by.is_some() {
            gone.push(Change::Gone { memory, lineage, id });
        } else {
            live.push(Change::Live { memory, lineage, id, ttl_at: r.data.ttl_at });
        }
    }
    // A batch's rows come in id order: what leaves, then what is live.
    gone.extend(live);
    gone
}

fn apply(keys: &mut Keys, c: Change) {
    match c {
        Change::Live { memory, lineage, id, ttl_at } => {
            keys.insert((memory, lineage), (id, ttl_at));
        }
        Change::Gone { memory, lineage, id } => {
            let k = (memory, lineage);
            if keys.get(&k).is_some_and(|(held, _)| *held == id) {
                keys.remove(&k);
            }
        }
    }
}

impl LiveIndex {
    pub fn new() -> Self {
        Self { state: Mutex::new(State::Off) }
    }

    /// Keep the key set (or the build in progress) in step with rows just applied.
    pub fn apply(&self, rows: &[StoredRow]) {
        let mut s = self.state.lock().unwrap();
        match &mut *s {
            State::Off => {}
            State::Building(held) => held.extend(changes(rows)),
            State::Ready(keys) => changes(rows).into_iter().for_each(|c| apply(keys, c)),
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

    /// The scan's live keys, then what was applied meanwhile.
    pub fn finish(&self, scanned: Vec<LiveKey>) {
        let mut keys: Keys = scanned.into_iter().map(|k| ((k.memory, k.lineage_id), (k.id, k.ttl_at))).collect();
        let mut s = self.state.lock().unwrap();
        if let State::Building(held) = std::mem::replace(&mut *s, State::Off) {
            held.into_iter().for_each(|c| apply(&mut keys, c));
            tracing::info!(keys = keys.len(), "live keys indexed");
            *s = State::Ready(keys);
        }
    }

    /// Give up a build (its scan failed, or the table was rewritten).
    pub fn abandon(&self) {
        *self.state.lock().unwrap() = State::Off;
    }

    pub fn is_ready(&self) -> bool {
        matches!(*self.state.lock().unwrap(), State::Ready(_))
    }

    /// The keys live at `now`, handed over once (the index is off afterwards); `None`
    /// when it isn't built.
    pub fn take(&self, now: CommitTs) -> Option<Vec<LiveKey>> {
        let mut s = self.state.lock().unwrap();
        match std::mem::replace(&mut *s, State::Off) {
            State::Ready(keys) => Some(
                keys.into_iter()
                    .filter(|(_, (_, ttl))| ttl.is_none_or(|t| t > now))
                    .map(|((memory, lineage_id), (id, ttl_at))| LiveKey { memory, lineage_id, ttl_at, id })
                    .collect(),
            ),
            other => {
                *s = other;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, lineage: &str, superseded: bool, tombstone: bool) -> StoredRow {
        let mut r = StoredRow::meta(0, CommitTs(1), 0, serde_json::json!({}));
        r.id = id.into();
        r.lineage_id = lineage.into();
        r.data.memory = Memory::Semantic;
        r.data.kind = Kind::Fact;
        r.superseded_by = superseded.then(|| "next".to_string());
        r.tombstone = tombstone;
        r
    }

    fn keys(i: &LiveIndex) -> Vec<String> {
        let mut k: Vec<String> = i.take(CommitTs(2)).unwrap().into_iter().map(|k| format!("{}@{}", k.lineage_id, k.id)).collect();
        k.sort();
        k
    }

    #[test]
    fn applies_during_the_build_win() {
        let i = LiveIndex::new();
        assert!(i.begin());
        // While the scan runs: `a` gets a new version, `b` is forgotten.
        i.apply(&[row("a1", "a", true, false), row("a2", "a", false, false)]);
        i.apply(&[row("b1", "b", false, true)]);
        let scanned = vec![
            LiveKey { memory: Memory::Semantic, lineage_id: "a".into(), ttl_at: None, id: "a1".into() },
            LiveKey { memory: Memory::Semantic, lineage_id: "b".into(), ttl_at: None, id: "b1".into() },
        ];
        i.finish(scanned);
        assert!(i.is_ready());
        assert_eq!(keys(&i), vec!["a@a2"]);
        assert!(i.take(CommitTs(2)).is_none(), "handed over once");
    }

    #[test]
    fn a_superseded_row_alone_leaves_the_newer_version() {
        let i = LiveIndex::new();
        assert!(i.begin());
        i.finish(Vec::new());
        i.apply(&[row("a1", "a", false, false)]);
        i.apply(&[row("a2", "a", false, false)]);
        // The old version rewritten later (superseded): `a` stays live at a2.
        i.apply(&[row("a1", "a", true, false)]);
        assert_eq!(keys(&i), vec!["a@a2"]);
    }
}

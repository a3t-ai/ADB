//! Field index (PF-27): for a memory type and a payload field, the current records by
//! the field's value, in commit order. `WHERE field = value` then visits the records
//! holding that value instead of scanning every row of the type.
//!
//! - **What it holds.** One entry per lineage: its newest live version's row id and
//!   commit time, under the value its payload has for the field. A version that is
//!   superseded or forgotten leaves; time-bound rows (`valid_to`, `ttl_at`) stay
//!   until a newer version replaces them, so a lookup is a superset: readers run the
//!   exact predicate and the visibility checks on what it returns, as on a scan.
//! - **When it is built.** On first use of a (memory, field) pair, from one scan of
//!   the type's current rows (the scan the read would have made anyway). Applies made
//!   while that scan runs are held and replayed on top, newest version winning, so
//!   no write is missed. Reads meanwhile scan as before. Nothing is persisted: a new
//!   process (or a follower) builds on its first such read.
//! - **Values.** Strings as written, numbers by their f64 value (WHERE compares an
//!   integer literal with a JSON float by value too), booleans, null. Values longer
//!   than [`MAX_VALUE`] aren't indexed: a lookup for such a literal is refused (the
//!   caller scans), so an unindexed long value is never missed.
//! - **Which fields.** Top-level payload fields of SEMANTIC, EPISODIC and WORKING
//!   (PROCEDURAL and TOOLS conditions see shaped records), at most [`MAX_FIELDS`] in
//!   all. `ADB_FIELD_INDEX=off` turns it off.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

use adb_core::log::{Kind, Memory};

use crate::rows::StoredRow;
use crate::schema::META_ID;

/// Longest value (in bytes) the index keeps.
pub const MAX_VALUE: usize = 256;
/// Most (memory, field) pairs indexed at once.
pub const MAX_FIELDS: usize = 32;

/// One record in a posting list: commit time (µs), row id, lineage.
pub type Entry = (i64, Arc<str>, Arc<str>);

/// The index key of a payload value.
pub fn value_key(v: &serde_json::Value) -> Option<String> {
    let k = match v {
        serde_json::Value::String(s) => format!("s:{s}"),
        serde_json::Value::Number(n) => format!("n:{}", n.as_f64()?),
        serde_json::Value::Bool(b) => format!("b:{b}"),
        serde_json::Value::Null => "z".to_string(),
        _ => return None,
    };
    (k.len() <= MAX_VALUE + 2).then_some(k)
}

/// The index key of a `field = literal` condition's literal (`None`: not indexable).
pub fn literal_key(v: &adb_core::Value) -> Option<String> {
    let k = match v {
        adb_core::Value::String(s) => format!("s:{s}"),
        adb_core::Value::Int(i) => format!("n:{}", *i as f64),
        adb_core::Value::Float(f) => format!("n:{f}"),
        adb_core::Value::Bool(b) => format!("b:{b}"),
        adb_core::Value::Null => "z".to_string(),
        _ => return None,
    };
    (k.len() <= MAX_VALUE + 2).then_some(k)
}

/// Whether `memory`'s payload fields can be indexed.
pub fn indexable(memory: Memory) -> bool {
    matches!(memory, Memory::Semantic | Memory::Episodic | Memory::Working)
}

#[derive(Default)]
struct Postings {
    by_value: HashMap<String, BTreeSet<Entry>>,
    /// lineage → (value key, its entry)
    of_lineage: HashMap<Arc<str>, (String, Entry)>,
}

impl Postings {
    fn remove(&mut self, lineage: &str) {
        if let Some((key, e)) = self.of_lineage.remove(lineage) {
            if let Some(set) = self.by_value.get_mut(&key) {
                set.remove(&e);
                if set.is_empty() {
                    self.by_value.remove(&key);
                }
            }
        }
    }

    fn apply(&mut self, field: &str, r: &StoredRow) {
        let ts = r.commit_ts.0;
        let current = self.of_lineage.get(r.lineage_id.as_str()).map(|(_, e)| (e.0, e.1.clone()));
        if r.tombstone {
            // FORGET: the lineage is gone, whatever row carries the tombstone.
            if current.as_ref().is_none_or(|(t, _)| *t <= ts) {
                self.remove(&r.lineage_id);
            }
            return;
        }
        if r.superseded_by.is_some() {
            // The old version of an UPDATE: leaves only if it is still the entry (the
            // new version may already be in, from the same batch).
            if current.as_ref().is_some_and(|(_, id)| &**id == r.id.as_str()) {
                self.remove(&r.lineage_id);
            }
            return;
        }
        if current.as_ref().is_some_and(|(t, _)| *t > ts) {
            return; // an older version than the one held (a replayed apply)
        }
        self.remove(&r.lineage_id);
        if let Some(key) = r.data.value.get(field).and_then(value_key) {
            let lineage: Arc<str> = Arc::from(r.lineage_id.as_str());
            let e: Entry = (ts, Arc::from(r.id.as_str()), lineage.clone());
            self.by_value.entry(key.clone()).or_default().insert(e.clone());
            self.of_lineage.insert(lineage, (key, e));
        }
    }
}

enum Slot {
    /// A build is scanning; applies meanwhile, replayed when it finishes.
    Building(Vec<StoredRow>),
    Ready(Postings),
}

/// A lookup's answer.
pub enum Lookup {
    /// The records holding the value, in commit order (oldest first).
    Ready(Vec<Entry>),
    /// Not built (yet): scan.
    Absent,
}

/// The field indexes of one partition (see the module docs).
pub struct FieldIndex {
    slots: RwLock<HashMap<(Memory, String), Slot>>,
    enabled: bool,
}

impl Default for FieldIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldIndex {
    pub fn new() -> Self {
        let enabled = !std::env::var("ADB_FIELD_INDEX").is_ok_and(|v| v == "off" || v == "0" || v == "false");
        Self { slots: RwLock::new(HashMap::new()), enabled }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Keep every index (and every build in progress) in step with rows just applied.
    pub fn apply(&self, rows: &[StoredRow]) {
        let mut slots = self.slots.write().unwrap();
        if slots.is_empty() {
            return;
        }
        for r in rows.iter().filter(|r| r.id != META_ID && matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode)) {
            for ((memory, field), slot) in slots.iter_mut() {
                if *memory != r.data.memory {
                    continue;
                }
                match slot {
                    Slot::Building(held) => held.push(r.clone()),
                    Slot::Ready(p) => p.apply(field, r),
                }
            }
        }
    }

    /// The records of `memory` whose `field` has the value with `key`.
    pub fn lookup(&self, memory: Memory, field: &str, key: &str) -> Lookup {
        let slots = self.slots.read().unwrap();
        match slots.get(&(memory, field.to_string())) {
            Some(Slot::Ready(p)) => Lookup::Ready(p.by_value.get(key).map(|s| s.iter().cloned().collect()).unwrap_or_default()),
            _ => Lookup::Absent,
        }
    }

    /// How many records hold the value (`None`: not built).
    pub fn count(&self, memory: Memory, field: &str, key: &str) -> Option<usize> {
        match self.slots.read().unwrap().get(&(memory, field.to_string())) {
            Some(Slot::Ready(p)) => Some(p.by_value.get(key).map_or(0, |s| s.len())),
            _ => None,
        }
    }

    /// Start building (`memory`, `field`): `true` if the caller should now scan and
    /// call [`FieldIndex::finish`]; `false` if it is built, being built, not
    /// indexable, or the index is full or off.
    pub fn begin(&self, memory: Memory, field: &str) -> bool {
        if !self.enabled || !indexable(memory) || field.is_empty() || field == "id" || field.contains('.') {
            return false;
        }
        let mut slots = self.slots.write().unwrap();
        let k = (memory, field.to_string());
        if slots.contains_key(&k) || slots.len() >= MAX_FIELDS {
            return false;
        }
        slots.insert(k, Slot::Building(Vec::new()));
        true
    }

    /// Finish a build with the current rows a scan returned (started after
    /// [`FieldIndex::begin`]); applies held meanwhile go on top.
    pub fn finish(&self, memory: Memory, field: &str, rows: &[StoredRow]) {
        let mut p = Postings::default();
        for r in rows.iter().filter(|r| r.data.memory == memory) {
            p.apply(field, r);
        }
        let mut slots = self.slots.write().unwrap();
        let k = (memory, field.to_string());
        if let Some(Slot::Building(held)) = slots.remove(&k) {
            for r in &held {
                p.apply(field, r);
            }
        }
        let (values, records) = (p.by_value.len(), p.of_lineage.len());
        slots.insert(k, Slot::Ready(p));
        tracing::info!(?memory, field, values, records, "field index built");
    }

    /// Drop every index (rows changed outside the applies): reads build again.
    pub fn clear(&self) {
        self.slots.write().unwrap().clear();
    }

    /// Give up a build (its scan failed): the next read starts again.
    pub fn abandon(&self, memory: Memory, field: &str) {
        self.slots.write().unwrap().remove(&(memory, field.to_string()));
    }

    /// (memory, field, distinct values, records) per built index.
    pub fn stats(&self) -> Vec<(Memory, String, usize, usize)> {
        self.slots
            .read()
            .unwrap()
            .iter()
            .filter_map(|((m, f), s)| match s {
                Slot::Ready(p) => Some((*m, f.clone(), p.by_value.len(), p.of_lineage.len())),
                Slot::Building(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::log::{CommitTs, NewRow, RowData};

    fn row(id: &str, lineage: &str, ts: i64, topic: serde_json::Value) -> StoredRow {
        let data: RowData = serde_json::from_value(serde_json::json!({
            "tenant_id": "t", "scope": "default", "visibility": "private", "namespace": null,
            "memory": "semantic", "kind": "fact", "key": lineage, "subject_id": null, "predicate": null,
            "object_id": null, "link_type": null, "weight": null, "value": {"topic": topic},
            "sealed": null, "text": null, "embedding": null, "ttl_at": null,
            "provenance": {"source": "test"}
        }))
        .expect("row data");
        let n = NewRow { id: adb_core::log::RecordId(id.into()), lineage_id: adb_core::log::RecordId(lineage.into()), version: 1, data };
        StoredRow::from_new(&n, CommitTs(ts), 1, 1)
    }

    fn ids(l: Lookup) -> Vec<String> {
        match l {
            Lookup::Ready(v) => v.iter().map(|e| e.1.to_string()).collect(),
            Lookup::Absent => vec!["<absent>".into()],
        }
    }

    #[test]
    fn follows_updates_forgets_and_writes_during_the_build() {
        let ix = FieldIndex::new();
        assert!(ix.begin(Memory::Semantic, "topic"));
        assert!(!ix.begin(Memory::Semantic, "topic"), "one build at a time");
        // Applied while the scan runs: a new record and an update of a scanned one.
        let mut old = row("a1", "a", 10, serde_json::json!("x"));
        ix.apply(&[row("c1", "c", 30, serde_json::json!("x"))]);
        old.superseded_by = Some("a2".into());
        ix.apply(&[old.clone(), row("a2", "a", 40, serde_json::json!("y"))]);
        // The scan saw a (old version) and b.
        ix.finish(Memory::Semantic, "topic", &[row("a1", "a", 10, serde_json::json!("x")), row("b1", "b", 20, serde_json::json!(7))]);
        assert_eq!(ids(ix.lookup(Memory::Semantic, "topic", "s:x")), ["c1"]);
        assert_eq!(ids(ix.lookup(Memory::Semantic, "topic", "s:y")), ["a2"]);
        // Numbers by value: 7 and 7.0 are one key.
        assert_eq!(literal_key(&adb_core::Value::Float(7.0)), literal_key(&adb_core::Value::Int(7)));
        assert_eq!(ids(ix.lookup(Memory::Semantic, "topic", &literal_key(&adb_core::Value::Int(7)).unwrap())), ["b1"]);
        // FORGET (a tombstone) removes the lineage.
        let mut gone = row("c1", "c", 50, serde_json::json!("x"));
        gone.tombstone = true;
        ix.apply(&[gone]);
        assert_eq!(ids(ix.lookup(Memory::Semantic, "topic", "s:x")), Vec::<String>::new());
        // A field removed by an update leaves the index.
        let mut a2 = row("a2", "a", 40, serde_json::json!("y"));
        a2.superseded_by = Some("a3".into());
        let mut a3 = row("a3", "a", 60, serde_json::json!(null));
        a3.data.value = serde_json::json!({"other": 1});
        ix.apply(&[a3, a2]);
        assert_eq!(ids(ix.lookup(Memory::Semantic, "topic", "s:y")), Vec::<String>::new());
        assert!(matches!(ix.lookup(Memory::Semantic, "missing", "s:y"), Lookup::Absent));
    }

    #[test]
    fn long_values_are_not_indexed() {
        let long = "z".repeat(MAX_VALUE + 1);
        assert!(value_key(&serde_json::json!(long)).is_none());
        assert!(literal_key(&adb_core::Value::String(long)).is_none());
    }
}

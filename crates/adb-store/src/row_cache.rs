//! Rows read by primary key for the field indexes (PF-27), kept so a repeated
//! indexed read doesn't go back to Lance for the same rows: a Lance point read costs
//! ~5 ms of CPU on a large table, the cache a hash lookup.
//!
//! Exact, not best-effort: every put drops the ids it writes, a table swap
//! (compaction, migration) drops everything, and a read only fills the cache if no
//! put of its id (and no swap) ran while it read. A reader registers its id first;
//! a drop that meets a registered id marks it, and the reader then leaves the cache
//! alone.
//!
//! Bounded (`ADB_ROW_CACHE_ROWS`, default 16384; 0 turns it off), oldest out first.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::StoredRow;

pub const DEFAULT_ROWS: usize = 16_384;

#[derive(Default)]
struct State {
    rows: HashMap<Arc<str>, StoredRow>,
    order: VecDeque<Arc<str>>,
    /// Ids being read: readers of each, and whether a drop met them meanwhile.
    reading: HashMap<Arc<str>, (u32, bool)>,
}

pub struct RowCache {
    cap: usize,
    state: Mutex<State>,
}

/// A read in progress (`RowCache::begin`); hand it back to `finish`.
pub struct Reading(Option<Arc<str>>);

impl Default for RowCache {
    fn default() -> Self {
        Self::new()
    }
}

impl RowCache {
    pub fn new() -> Self {
        let cap = std::env::var("ADB_ROW_CACHE_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_ROWS);
        Self::with_capacity(cap)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { cap, state: Mutex::new(State::default()) }
    }

    /// The cached row, or a registration for reading it from the table.
    pub fn begin(&self, id: &str) -> Result<StoredRow, Reading> {
        if self.cap == 0 {
            return Err(Reading(None));
        }
        let mut s = self.state.lock().unwrap();
        if let Some(r) = s.rows.get(id) {
            return Ok(r.clone());
        }
        let id: Arc<str> = Arc::from(id);
        s.reading.entry(id.clone()).or_insert((0, false)).0 += 1;
        Err(Reading(Some(id)))
    }

    /// The read is done: keep `row` unless a drop met its id while it was read.
    pub fn finish(&self, reading: Reading, row: Option<&StoredRow>) {
        let Some(id) = reading.0 else { return };
        let mut s = self.state.lock().unwrap();
        let dirty = match s.reading.get_mut(&id) {
            Some((n, dirty)) => {
                *n -= 1;
                let d = *dirty;
                if *n == 0 {
                    s.reading.remove(&id);
                }
                d
            }
            None => true,
        };
        let Some(row) = row else { return };
        if dirty || s.rows.contains_key(&id) {
            return;
        }
        while s.rows.len() >= self.cap {
            let Some(old) = s.order.pop_front() else { break };
            s.rows.remove(&old);
        }
        s.order.push_back(id.clone());
        s.rows.insert(id, row.clone());
    }

    /// A put wrote these rows: drop them, and spoil reads of them under way.
    pub fn drop_rows(&self, rows: &[StoredRow]) {
        if self.cap == 0 {
            return;
        }
        let mut s = self.state.lock().unwrap();
        for r in rows {
            let id: &str = &r.id;
            s.rows.remove(id);
            if let Some(e) = s.reading.get_mut(id) {
                e.1 = true;
            }
        }
        if s.order.len() > 2 * s.rows.len() + 1024 {
            let State { rows, order, .. } = &mut *s;
            order.retain(|k| rows.contains_key(k));
        }
    }

    /// The table was swapped: drop everything, spoil every read under way.
    pub fn clear(&self) {
        let mut s = self.state.lock().unwrap();
        s.rows.clear();
        s.order.clear();
        for e in s.reading.values_mut() {
            e.1 = true;
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, v: i64) -> StoredRow {
        let mut r = StoredRow::meta(v as u64, adb_core::log::CommitTs(v), 0, serde_json::json!({}));
        r.id = id.into();
        r
    }

    #[test]
    fn keeps_and_bounds() {
        let c = RowCache::with_capacity(2);
        for (i, id) in ["a", "b", "c"].iter().enumerate() {
            let Err(t) = c.begin(id) else { panic!() };
            c.finish(t, Some(&row(id, i as i64)));
        }
        assert_eq!(c.len(), 2);
        assert!(c.begin("a").is_err(), "oldest out first");
        assert!(c.begin("c").is_ok());
    }

    #[test]
    fn a_put_during_the_read_keeps_the_old_row_out() {
        let c = RowCache::with_capacity(8);
        let Err(t) = c.begin("a") else { panic!() };
        c.drop_rows(&[row("a", 2)]);
        c.finish(t, Some(&row("a", 1)));
        assert!(c.begin("a").is_err());
        // The next read fills it.
        let Err(t) = c.begin("a") else { panic!() };
        c.finish(t, Some(&row("a", 2)));
        assert!(c.begin("a").is_ok());
        c.drop_rows(&[row("a", 3)]);
        assert!(c.begin("a").is_err(), "a put drops it");
    }

    #[test]
    fn a_swap_during_the_read_keeps_it_out() {
        let c = RowCache::with_capacity(8);
        let Err(t) = c.begin("a") else { panic!() };
        c.clear();
        c.finish(t, Some(&row("a", 1)));
        assert!(c.is_empty());
    }

    #[test]
    fn off() {
        let c = RowCache::with_capacity(0);
        let Err(t) = c.begin("a") else { panic!() };
        c.finish(t, Some(&row("a", 1)));
        assert!(c.is_empty());
    }
}

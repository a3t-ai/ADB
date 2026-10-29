//! The hot cache (W10, `W10_INDEXES_CACHE.md`): current record rows kept in memory
//! per memory type and scope, by rules (the newest n, the newest bytes, a time
//! window, all, none).
//!
//! - **Fed by the applier, not by reads:** every row a put writes goes through
//!   [`HotCache::apply`] in log order, under the same store lock as the put, so the
//!   cache is the table's current view at the applied offset. No invalidation; a
//!   replica applying the same log builds the same cache.
//! - **Complete or not, per bucket:** a bucket remembers a *floor* — the newest
//!   commit time it ever evicted. Every current row of the bucket committed after
//!   the floor is in the cache; at or before it, rows may be missing. A bucket
//!   without a floor holds all of its current rows.
//! - **Readers decide** ([`Hit::floor`]): an answer that needs every row uses the
//!   cache only when it is complete; a newest-first answer also when the rows it
//!   returns are all newer than the floor. Otherwise the read goes to Lance.
//! - Rows are kept as reads decode them (no `embedding`, no `sealed`), so a read
//!   from the cache sees exactly what a read from the table sees.
//! - **Read in place:** a read gets shared pointers to the rows (and to each row's
//!   record, decoded once at apply by the [`Shape`] the cache was given), so it
//!   copies only what it returns (W10-L8).
//! - **Vectors** are kept too unless `vectors: false`, so `LIKE` ranks a complete
//!   bucket in memory (W10-L1).

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use adb_core::log::{CommitTs, Kind, Memory};
use adb_core::memory::MemoryRecord;
use serde::{Deserialize, Serialize};

use crate::rows::StoredRow;
use crate::schema::META_ID;

/// The settings file in the data directory.
pub const SETTINGS_FILE: &str = "cache.json";

/// How much of a bucket (one memory type in one scope) stays in memory.
#[derive(Debug, Clone, PartialEq)]
pub enum Keep {
    None,
    All,
    /// The newest n current records.
    Last(usize),
    /// The newest records that fit in this many bytes.
    Bytes(u64),
    /// Records committed within this long of now.
    Within(Duration),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum KeepRaw {
    Word(String),
    Last { last: usize },
    Bytes { bytes: SizeRaw },
    Within { within: String },
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum SizeRaw {
    N(u64),
    Text(String),
}

impl SizeRaw {
    fn bytes(&self) -> Result<u64, String> {
        match self {
            SizeRaw::N(n) => Ok(*n),
            SizeRaw::Text(t) => parse_size(t),
        }
    }
}

/// `"10MB"`, `"512 KiB"`, `"1g"`, `"4096"` → bytes (K / M / G are powers of 1024).
pub fn parse_size(t: &str) -> Result<u64, String> {
    let s = t.trim().to_ascii_lowercase();
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().map_err(|_| format!("size {t:?}: expected e.g. \"10MB\""))?;
    let mult = match unit.trim().trim_end_matches("ib").trim_end_matches('b') {
        "" => 1u64,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        _ => return Err(format!("size {t:?}: unit is B, KB, MB or GB")),
    };
    Ok((n * mult as f64) as u64)
}

/// `"30s"`, `"15m"`, `"24h"`, `"7d"`.
pub fn parse_duration(t: &str) -> Result<Duration, String> {
    let s = t.trim();
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = num.parse().map_err(|_| format!("duration {t:?}: expected e.g. \"24h\""))?;
    let secs = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => return Err(format!("duration {t:?}: unit is s, m, h or d")),
    };
    Ok(Duration::from_secs(n * secs))
}

fn show_duration(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        _ if s > 0 && s % 86_400 == 0 => format!("{}d", s / 86_400),
        _ if s > 0 && s % 3600 == 0 => format!("{}h", s / 3600),
        _ if s > 0 && s % 60 == 0 => format!("{}m", s / 60),
        _ => format!("{s}s"),
    }
}

impl TryFrom<KeepRaw> for Keep {
    type Error = String;
    fn try_from(r: KeepRaw) -> Result<Self, String> {
        Ok(match r {
            KeepRaw::Word(w) if w == "all" => Keep::All,
            KeepRaw::Word(w) if w == "none" => Keep::None,
            KeepRaw::Word(w) => return Err(format!("keep {w:?}: \"all\", \"none\", {{\"last\": n}}, {{\"bytes\": \"10MB\"}} or {{\"within\": \"24h\"}}")),
            KeepRaw::Last { last } => Keep::Last(last),
            KeepRaw::Bytes { bytes } => Keep::Bytes(bytes.bytes()?),
            KeepRaw::Within { within } => Keep::Within(parse_duration(&within)?),
        })
    }
}

impl From<&Keep> for KeepRaw {
    fn from(k: &Keep) -> Self {
        match k {
            Keep::None => KeepRaw::Word("none".into()),
            Keep::All => KeepRaw::Word("all".into()),
            Keep::Last(n) => KeepRaw::Last { last: *n },
            Keep::Bytes(b) => KeepRaw::Bytes { bytes: SizeRaw::N(*b) },
            Keep::Within(d) => KeepRaw::Within { within: show_duration(*d) },
        }
    }
}

impl Serialize for Keep {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        KeepRaw::from(self).serialize(s)
    }
}

impl<'de> Deserialize<'de> for Keep {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Keep::try_from(KeepRaw::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// One rule: the first rule whose memory type and scope match a bucket decides it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    pub memory: Memory,
    /// A scope name or a glob (`*`, `team:*`).
    #[serde(default = "any_scope")]
    pub scope: String,
    pub keep: Keep,
}

fn any_scope() -> String {
    "*".into()
}

/// The cache's settings (`<data dir>/cache.json`, `GET/PUT /admin/cache`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Node-wide cap; past it the largest buckets give up their oldest rows.
    #[serde(serialize_with = "ser_size", deserialize_with = "de_size")]
    pub max_bytes: u64,
    pub rules: Vec<Rule>,
    /// Keep cached rows' vectors (2 bytes a dimension) for `LIKE` (W10-L1).
    #[serde(default = "yes")]
    pub vectors: bool,
}

fn yes() -> bool {
    true
}

fn ser_size<S: serde::Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(*v)
}

fn de_size<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    SizeRaw::deserialize(d)?.bytes().map_err(serde::de::Error::custom)
}

impl Default for CacheConfig {
    /// The scratchpad and registries whole (as the old hot tier), recent episodes;
    /// SEMANTIC is left to Lance.
    fn default() -> Self {
        let rule = |memory, keep| Rule { memory, scope: any_scope(), keep };
        Self {
            max_bytes: 256 << 20,
            rules: vec![
                rule(Memory::Working, Keep::Bytes(10 << 20)),
                rule(Memory::Tools, Keep::All),
                rule(Memory::Procedural, Keep::All),
                rule(Memory::Episodic, Keep::Last(200)),
                rule(Memory::Semantic, Keep::None),
            ],
            vectors: true,
        }
    }
}

impl CacheConfig {
    pub fn load(data_dir: &Path) -> Result<Option<Self>, String> {
        let p = data_dir.join(SETTINGS_FILE);
        match std::fs::read(&p) {
            Ok(b) => {
                let cfg: Self = serde_json::from_slice(&b).map_err(|e| format!("{}: {e}", p.display()))?;
                cfg.check()?;
                Ok(Some(cfg))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{}: {e}", p.display())),
        }
    }

    /// Save to `data_dir` atomically (temp file, fsync, rename).
    pub fn save(&self, data_dir: &Path) -> Result<(), String> {
        use std::io::Write as _;
        let p = data_dir.join(SETTINGS_FILE);
        let tmp = p.with_extension("json.tmp");
        let io = |e: std::io::Error| format!("{}: {e}", p.display());
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(&serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?).map_err(io)?;
        f.sync_all().map_err(io)?;
        std::fs::rename(&tmp, &p).map_err(io)?;
        if let Ok(d) = std::fs::File::open(data_dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    pub fn check(&self) -> Result<(), String> {
        for r in &self.rules {
            if r.scope.is_empty() || r.scope.len() > 128 {
                return Err(format!("rule for {}: scope is 1-128 characters (or \"*\")", r.memory.tag()));
            }
        }
        Ok(())
    }

    /// What the first matching rule keeps of `memory` in `scope` (none without one).
    pub fn keep(&self, memory: Memory, scope: &str) -> Keep {
        self.rules.iter().find(|r| r.memory == memory && glob(&r.scope, scope)).map(|r| r.keep.clone()).unwrap_or(Keep::None)
    }

    /// Whether any rule can keep rows of `memory` (others are not tracked at all).
    pub fn tracks(&self, memory: Memory) -> bool {
        self.rules.iter().any(|r| r.memory == memory && r.keep != Keep::None)
    }
}

/// `*` matches any run of characters; everything else literally.
pub fn glob(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !s.starts_with(first) || s.len() < first.len() + last.len() || !s.ends_with(last) {
        return false;
    }
    let mut rest = &s[first.len()..s.len() - last.len()];
    for p in &parts[1..parts.len() - 1] {
        match rest.find(p) {
            Some(i) => rest = &rest[i + p.len()..],
            None => return false,
        }
    }
    true
}

/// How the cache decodes a row into the record reads work on (the executor's
/// `record`), once per row at apply.
pub type Shape = fn(&StoredRow) -> MemoryRecord;

/// A cached row and its record (`None` without a [`Shape`]).
#[derive(Debug, Clone)]
pub struct Cached {
    pub row: Arc<StoredRow>,
    pub record: Option<Arc<MemoryRecord>>,
}

/// Rows of a read from the cache.
#[derive(Debug)]
pub struct Hit {
    pub rows: Vec<Cached>,
    /// `None`: every current row the read could select is here. `Some(t)`: rows
    /// committed at or before `t` may be missing.
    pub floor: Option<CommitTs>,
}

/// One bucket's figures (`GET /admin/cache`).
#[derive(Debug, Clone, Serialize)]
pub struct BucketStats {
    pub memory: Memory,
    pub scope: String,
    pub rows: usize,
    pub bytes: u64,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    pub rows: usize,
    pub bytes: u64,
    pub max_bytes: u64,
    /// Reads answered from the cache.
    pub hits: u64,
    /// Reads that went to Lance (bucket incomplete, memory type not kept, or a
    /// newest-first read that needed rows below the floor).
    pub misses: u64,
    /// Shadow mode: answers that differed from Lance's.
    pub mismatches: u64,
    pub buckets: Vec<BucketStats>,
}

struct Entry {
    row: Arc<StoredRow>,
    record: Option<Arc<MemoryRecord>>,
    bytes: u64,
}

impl Entry {
    fn cached(&self) -> Cached {
        Cached { row: self.row.clone(), record: self.record.clone() }
    }
}

struct Bucket {
    keep: Keep,
    rows: HashMap<String, Entry>,
    by_time: BTreeSet<(CommitTs, String)>,
    bytes: u64,
    floor: Option<CommitTs>,
}

impl Bucket {
    fn new(keep: Keep) -> Self {
        Self { keep, rows: HashMap::new(), by_time: BTreeSet::new(), bytes: 0, floor: None }
    }

    fn remove(&mut self, id: &str) -> Option<Entry> {
        let e = self.rows.remove(id)?;
        self.by_time.remove(&(e.row.commit_ts, id.to_string()));
        self.bytes -= e.bytes;
        Some(e)
    }

    /// Drop the oldest row; the floor moves up to it.
    fn evict_oldest(&mut self) -> Option<Entry> {
        let (ts, id) = self.by_time.iter().next().cloned()?;
        self.floor = Some(self.floor.map_or(ts, |f| f.max(ts)));
        self.remove(&id)
    }

    fn over(&self, now: CommitTs) -> bool {
        match &self.keep {
            Keep::All => false,
            Keep::None => !self.rows.is_empty(),
            Keep::Last(n) => self.rows.len() > *n,
            Keep::Bytes(b) => self.bytes > *b,
            Keep::Within(d) => self.by_time.iter().next().is_some_and(|(ts, _)| ts.0 < now.0 - d.as_micros() as i64),
        }
    }
}

#[derive(Default)]
struct State {
    cfg: Option<CacheConfig>,
    buckets: HashMap<(Memory, String), Bucket>,
    /// Row id → its bucket.
    at: HashMap<String, (Memory, String)>,
    /// Lineage id → its current row id (one current version per lineage).
    lineages: HashMap<String, String>,
    bytes: u64,
}

/// See the module docs.
pub struct HotCache {
    state: RwLock<State>,
    shadow: bool,
    shape: Option<Shape>,
    hits: AtomicU64,
    misses: AtomicU64,
    mismatches: AtomicU64,
}

/// Rows the cache holds: current record versions (not edges, events, `_meta`).
fn cacheable(r: &StoredRow) -> bool {
    r.id != META_ID && matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool) && !r.tombstone && r.superseded_by.is_none()
}

fn record_kind(r: &StoredRow) -> bool {
    r.id != META_ID && matches!(r.data.kind, Kind::Entity | Kind::Fact | Kind::Episode | Kind::Tool)
}

/// What a row costs in memory, roughly: its payload as JSON plus ids and columns.
fn row_bytes(r: &StoredRow) -> u64 {
    let s = |o: &Option<String>| o.as_ref().map_or(0, |s| s.len());
    let d = &r.data;
    (serde_json::to_vec(&d.value).map_or(0, |v| v.len())
        + r.id.len()
        + r.lineage_id.len()
        + d.scope.len()
        + d.tenant_id.len()
        + s(&d.namespace)
        + s(&d.key)
        + s(&d.text)
        + s(&d.subject_id)
        + s(&d.object_id)
        + d.embedding.as_ref().map_or(0, |e| e.len() * 2)
        + 256) as u64
}

fn now() -> CommitTs {
    CommitTs::from_datetime(adb_core::time::now())
}

impl HotCache {
    /// An empty cache with `cfg`; [`HotCache::load`] fills it. `shadow`: readers
    /// check every answer against Lance.
    pub fn new(cfg: CacheConfig, shadow: bool) -> Self {
        Self {
            state: RwLock::new(State { cfg: Some(cfg), ..Default::default() }),
            shadow,
            shape: None,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            mismatches: AtomicU64::new(0),
        }
    }

    /// Decode each row with `shape` as it is cached (reads then share the record).
    pub fn shaped(mut self, shape: Shape) -> Self {
        self.shape = Some(shape);
        self
    }

    pub fn shadow(&self) -> bool {
        self.shadow
    }

    pub fn config(&self) -> CacheConfig {
        self.state.read().unwrap().cfg.clone().unwrap_or_default()
    }

    /// The memory types the cache follows (warm-up scans only these).
    pub fn tracked(&self) -> Vec<Memory> {
        let cfg = self.config();
        [Memory::Working, Memory::Tools, Memory::Procedural, Memory::Semantic, Memory::Episodic].into_iter().filter(|m| cfg.tracks(*m)).collect()
    }

    pub fn tracks(&self, memory: Memory) -> bool {
        self.state.read().unwrap().cfg.as_ref().is_some_and(|c| c.tracks(memory))
    }

    /// Whether cached rows carry their vectors (`LIKE` can be answered here).
    pub fn vectors(&self) -> bool {
        self.state.read().unwrap().cfg.as_ref().is_some_and(|c| c.vectors)
    }

    /// Drop every cached vector (the table's were cleared, W9.10); the backfill's
    /// `Embed` records bring them back.
    pub fn drop_vectors(&self) {
        let mut st = self.state.write().unwrap();
        let mut freed = 0;
        for b in st.buckets.values_mut() {
            for e in b.rows.values_mut().filter(|e| e.row.data.embedding.is_some()) {
                let mut r = (*e.row).clone();
                r.data.embedding = None;
                let bytes = row_bytes(&r);
                freed += e.bytes - bytes;
                b.bytes -= e.bytes - bytes;
                e.bytes = bytes;
                e.row = Arc::new(r);
            }
        }
        st.bytes -= freed;
    }

    /// Start from `rows`: every current record row of the tracked memory types.
    pub fn load(&self, rows: Vec<StoredRow>) {
        let mut st = self.state.write().unwrap();
        let cfg = st.cfg.take();
        *st = State { cfg, ..Default::default() };
        let now = now();
        for r in rows {
            Self::upsert(&mut st, r, self.shape);
        }
        Self::trim(&mut st, now);
    }

    /// The rows one put wrote, in the put's order (called under the store lock).
    pub fn apply(&self, rows: &[StoredRow]) {
        let mut st = self.state.write().unwrap();
        let now = now();
        for r in rows.iter().filter(|r| record_kind(r)) {
            Self::upsert(&mut st, r.clone(), self.shape);
        }
        Self::trim(&mut st, now);
    }

    /// Drop rows past their time window, TTL or validity (the expiry tick).
    pub fn sweep(&self) {
        let mut st = self.state.write().unwrap();
        let now = now();
        let gone: Vec<String> = st
            .buckets
            .values()
            .flat_map(|b| b.rows.values().filter(|e| !e.row.is_current(now)).map(|e| e.row.id.clone()))
            .collect();
        for id in gone {
            // Not current any more, so never selected again: no floor change.
            Self::remove(&mut st, &id);
        }
        Self::trim(&mut st, now);
    }

    fn remove(st: &mut State, id: &str) {
        let Some(at) = st.at.remove(id) else { return };
        if let Some(e) = st.buckets.get_mut(&at).and_then(|b| b.remove(id)) {
            st.bytes -= e.bytes;
            if st.lineages.get(&e.row.lineage_id).is_some_and(|cur| cur == id) {
                st.lineages.remove(&e.row.lineage_id);
            }
        }
    }

    fn upsert(st: &mut State, mut r: StoredRow, shape: Option<Shape>) {
        Self::remove(st, &r.id);
        let Some(cfg) = &st.cfg else { return };
        if !cfg.tracks(r.data.memory) || !cacheable(&r) {
            return;
        }
        let key = (r.data.memory, r.data.scope.clone());
        let keep = cfg.keep(key.0, &key.1);
        // As reads decode rows (vectors only when kept).
        if !cfg.vectors {
            r.data.embedding = None;
        }
        r.data.sealed = None;
        let bytes = row_bytes(&r);
        let b = st.buckets.entry(key.clone()).or_insert_with(|| Bucket::new(keep));
        // Below the floor (an old row patched after it was evicted): stays out.
        if b.floor.is_some_and(|f| r.commit_ts <= f) {
            return;
        }
        b.by_time.insert((r.commit_ts, r.id.clone()));
        b.bytes += bytes;
        st.bytes += bytes;
        st.lineages.insert(r.lineage_id.clone(), r.id.clone());
        st.at.insert(r.id.clone(), key);
        let record = shape.map(|f| Arc::new(f(&r)));
        b.rows.insert(r.id.clone(), Entry { row: Arc::new(r), record, bytes });
    }

    /// Each bucket within its rule, then the whole cache within `max_bytes`.
    fn trim(st: &mut State, now: CommitTs) {
        let mut evicted: Vec<Entry> = Vec::new();
        for b in st.buckets.values_mut() {
            while b.over(now) {
                match b.evict_oldest() {
                    Some(e) => evicted.push(e),
                    None => break,
                }
            }
        }
        let max = st.cfg.as_ref().map_or(0, |c| c.max_bytes);
        let mut total = st.bytes - evicted.iter().map(|e| e.bytes).sum::<u64>();
        while total > max {
            let Some(b) = st.buckets.values_mut().filter(|b| !b.rows.is_empty()).max_by_key(|b| b.bytes) else { break };
            match b.evict_oldest() {
                Some(e) => {
                    total -= e.bytes;
                    evicted.push(e);
                }
                None => break,
            }
        }
        for e in evicted {
            st.bytes -= e.bytes;
            st.at.remove(&e.row.id);
            if st.lineages.get(&e.row.lineage_id).is_some_and(|cur| *cur == e.row.id) {
                st.lineages.remove(&e.row.lineage_id);
            }
        }
    }

    /// The cached rows of `memory` (in `scope`, or all scopes), with the floor
    /// below which rows may be missing. `None`: the memory type isn't kept.
    pub fn rows(&self, memory: Memory, scope: Option<&str>) -> Option<Hit> {
        let st = self.state.read().unwrap();
        if !st.cfg.as_ref().is_some_and(|c| c.tracks(memory)) {
            return None;
        }
        let mut rows = Vec::new();
        let mut floor: Option<CommitTs> = None;
        for ((m, s), b) in &st.buckets {
            if *m != memory || scope.is_some_and(|want| want != s) {
                continue;
            }
            rows.extend(b.rows.values().map(Entry::cached));
            if let Some(f) = b.floor {
                floor = Some(floor.map_or(f, |g| g.max(f)));
            }
        }
        // A scope with no bucket has no current rows (every row passes through).
        Some(Hit { rows, floor })
    }

    /// The current row of lineage `lineage` (a row lineage id) of `memory`:
    /// `Some(Some(row))` cached, `Some(None)` known to have none (every bucket of
    /// the memory type complete), `None` unknown.
    pub fn lineage(&self, memory: Memory, lineage: &str) -> Option<Option<Cached>> {
        let st = self.state.read().unwrap();
        if !st.cfg.as_ref().is_some_and(|c| c.tracks(memory)) {
            return None;
        }
        if let Some(id) = st.lineages.get(lineage) {
            let at = st.at.get(id)?;
            if at.0 == memory {
                return st.buckets.get(at).and_then(|b| b.rows.get(id)).map(|e| Some(e.cached()));
            }
        }
        let complete = st.buckets.iter().filter(|((m, _), _)| *m == memory).all(|(_, b)| b.floor.is_none());
        complete.then_some(None)
    }

    pub fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mismatch(&self) {
        self.mismatches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> CacheStats {
        let st = self.state.read().unwrap();
        let mut buckets: Vec<BucketStats> = st
            .buckets
            .iter()
            .map(|((m, s), b)| BucketStats { memory: *m, scope: s.clone(), rows: b.rows.len(), bytes: b.bytes, complete: b.floor.is_none() })
            .collect();
        buckets.sort_by(|a, b| (a.memory.tag(), &a.scope).cmp(&(b.memory.tag(), &b.scope)));
        CacheStats {
            rows: st.at.len(),
            bytes: st.bytes,
            max_bytes: st.cfg.as_ref().map_or(0, |c| c.max_bytes),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            mismatches: self.mismatches.load(Ordering::Relaxed),
            buckets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_durations_and_globs() {
        assert_eq!(parse_size("10MB").unwrap(), 10 << 20);
        assert_eq!(parse_size("512 KiB").unwrap(), 512 << 10);
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_size("ten").is_err());
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert!(glob("*", "anything"));
        assert!(glob("team:*", "team:billing"));
        assert!(!glob("team:*", "user:1"));
        assert!(glob("a*c*e", "abcde"));
        assert!(glob("exact", "exact") && !glob("exact", "exactly"));
    }

    #[test]
    fn config_round_trips() {
        let j = serde_json::json!({
            "max_bytes": "512MB",
            "rules": [
                { "memory": "working", "keep": { "bytes": "10MB" } },
                { "memory": "episodic", "scope": "team:billing", "keep": { "last": 200 } },
                { "memory": "episodic", "keep": { "within": "24h" } },
                { "memory": "tools", "keep": "all" },
                { "memory": "semantic", "keep": "none" }
            ]
        });
        let cfg: CacheConfig = serde_json::from_value(j).unwrap();
        assert_eq!(cfg.max_bytes, 512 << 20);
        assert_eq!(cfg.keep(Memory::Episodic, "team:billing"), Keep::Last(200));
        assert_eq!(cfg.keep(Memory::Episodic, "tenant:default"), Keep::Within(Duration::from_secs(86_400)));
        assert_eq!(cfg.keep(Memory::Procedural, "x"), Keep::None);
        assert!(!cfg.tracks(Memory::Semantic));
        let again: CacheConfig = serde_json::from_value(serde_json::to_value(&cfg).unwrap()).unwrap();
        assert_eq!(again, cfg);
        assert!(serde_json::from_value::<CacheConfig>(serde_json::json!({"max_bytes": 1, "rules": [{"memory": "working", "keep": "some"}]})).is_err());
        assert!(cfg.vectors, "vectors are kept unless turned off");
    }

    fn fact(key: &str) -> StoredRow {
        use adb_core::log::{NewRow, Provenance, RecordId, RowData, Visibility};
        let data = RowData {
            tenant_id: "default".into(),
            scope: "s".into(),
            visibility: Visibility::Private,
            namespace: None,
            memory: Memory::Semantic,
            kind: Kind::Fact,
            key: Some(key.into()),
            subject_id: None,
            predicate: None,
            object_id: None,
            link_type: None,
            weight: None,
            text: None,
            value: serde_json::json!({ "key": key }),
            sealed: None,
            embedding: Some(vec![half::f16::from_f32(0.5); 384]),
            ttl_at: None,
            provenance: Provenance::default(),
        };
        StoredRow::from_new(&NewRow { id: RecordId(key.into()), lineage_id: RecordId(key.into()), version: 1, data }, now(), 1, 1)
    }

    #[test]
    fn vectors_kept_counted_and_dropped() {
        let all = |vectors| CacheConfig { max_bytes: 1 << 20, rules: vec![Rule { memory: Memory::Semantic, scope: "*".into(), keep: Keep::All }], vectors };
        let without = HotCache::new(all(false), false);
        without.apply(&[fact("a")]);
        assert!(without.rows(Memory::Semantic, None).unwrap().rows[0].row.data.embedding.is_none());
        let with = HotCache::new(all(true), false);
        with.apply(&[fact("a")]);
        assert_eq!(with.rows(Memory::Semantic, None).unwrap().rows[0].row.data.embedding.as_ref().map(Vec::len), Some(384));
        assert_eq!(with.stats().bytes, without.stats().bytes + 768);
        with.drop_vectors();
        assert!(with.rows(Memory::Semantic, None).unwrap().rows[0].row.data.embedding.is_none());
        assert_eq!(with.stats().bytes, without.stats().bytes);
        assert_eq!(with.stats().buckets[0].bytes, with.stats().bytes);
    }
}

//! Continuous queries: `WATCH` v1 (MVP W6, `W6_WATCH.md`; spec
//! `SUBSCRIPTIONS_CQ_SPEC.md` §5).
//!
//! - [`WatchSpec`] — a registered watch: the statement, its variables, the debounce.
//! - [`WatchSet`] — the static watch set: which committed effects can change the
//!   result (memory types, `IN SCOPE`).
//! - [`diff`] — the frame between two results of the same query.
//! - [`Watcher`] — one task per partition: tails the applied log like a subscriber,
//!   marks the watches a record touches dirty, re-runs them after their debounce
//!   through a [`Runner`], and keeps each watch's result and its recent frames for
//!   readers. Re-running (decision 1) makes a result after any frame equal to the
//!   query re-run at that frame's offset.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use adb_core::log::{Effect, LogOffset, LogRecord, Memory};
use adb_log::{Log, LogError};
use aql_parser::ast::{MemoryType, Modifiers, Statement};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::watch;

/// Default and allowed `DEBOUNCE` (AQL v0.6 §3).
pub const DEFAULT_DEBOUNCE_MS: u64 = 100;
pub const DEBOUNCE_RANGE_MS: std::ops::RangeInclusive<u64> = 50..=500;
/// Frames kept per watch for resuming readers (decision 2).
pub const FRAME_BUFFER: usize = 1000;
/// A watch nobody has read for this long is dropped (decision 3; `ADB_WATCH_IDLE`).
pub const DEFAULT_IDLE: Duration = Duration::from_secs(24 * 3600);

/// A registered watch: the `value` of its `cq` row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchSpec {
    /// The watched statement (RECALL, LOOKUP, SCAN or LOAD).
    pub statement: Statement,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub variables: HashMap<String, Value>,
    pub debounce_ms: u64,
    /// A cluster's watch across partitions (a statement naming no namespace): the
    /// call's scope it runs in. It is the router's ([`Watcher::poke`]), registered on
    /// the system partition; a partition's own watcher leaves it alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub across: Option<Value>,
}

impl WatchSpec {
    /// A spec for `statement` (the query a `WATCH` wraps), checked.
    pub fn new(statement: Statement, variables: HashMap<String, Value>, debounce: Option<Duration>) -> Result<Self, String> {
        let debounce_ms = debounce.map(|d| d.as_millis() as u64).unwrap_or(DEFAULT_DEBOUNCE_MS);
        if !DEBOUNCE_RANGE_MS.contains(&debounce_ms) {
            return Err(format!("DEBOUNCE {debounce_ms}ms: allowed 50ms-500ms"));
        }
        WatchSet::of(&statement)?;
        Ok(Self { statement, variables, debounce_ms, across: None })
    }

    pub fn watch_set(&self) -> WatchSet {
        WatchSet::of(&self.statement).unwrap_or_default()
    }
}

/// The effects that can change a watch's result (spec §5.3, static form).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WatchSet {
    /// Memory types whose changes matter; `None` = all (queries following links).
    pub memories: Option<HashSet<Memory>>,
    /// `IN SCOPE`: only rows of this scope (puts); `None` = any.
    pub scope: Option<String>,
    /// A `LIKE` query: a vector attached to a row (`Embed`) can change its ranking.
    pub like: bool,
    /// The result's order means something (`ORDER BY`, `LIKE` ranking): diffs say
    /// when it changes. Otherwise rows come in storage order, which is no change.
    pub ordered: bool,
}

fn memory_of(t: MemoryType) -> Memory {
    match t {
        MemoryType::Working => Memory::Working,
        MemoryType::Tools => Memory::Tools,
        MemoryType::Procedural => Memory::Procedural,
        MemoryType::Semantic => Memory::Semantic,
        MemoryType::Episodic => Memory::Episodic,
        // ALL: every memory type (`WatchSet::of` sets `memories: None`).
        MemoryType::All => Memory::Semantic,
    }
}

impl WatchSet {
    /// The watch set of a live query (`SUBSCRIBE RECALL …`, AQL v0.13); anything but
    /// a RECALL is refused.
    pub fn of(statement: &Statement) -> Result<Self, String> {
        let (memory, modifiers, like): (Option<MemoryType>, &Modifiers, bool) = match statement {
            Statement::Recall(r) => (Some(r.memory_type), &r.modifiers, matches!(r.predicate, aql_parser::ast::Predicate::Like { .. })),
            _ => return Err("a live query is a RECALL (SUBSCRIBE RECALL …)".into()),
        };
        let all = matches!(memory, Some(MemoryType::All)) || modifiers.with_links.is_some() || modifiers.follow_links.is_some();
        let memories = (!all).then(|| memory.map(memory_of).into_iter().collect());
        Ok(Self { memories, scope: modifiers.in_scope.clone(), like, ordered: like || modifiers.order_by.is_some() })
    }

    fn memory(&self, m: Memory) -> bool {
        self.memories.as_ref().is_none_or(|s| s.contains(&m))
    }

    /// Whether `record` can change the result.
    pub fn touches(&self, record: &LogRecord) -> bool {
        record.effects.iter().any(|e| match e {
            Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) | Effect::Supersede { new: n, .. } => {
                self.memory(n.data.memory) && self.scope.as_ref().is_none_or(|s| *s == n.data.scope)
            }
            Effect::PutEdge(n) => self.memory(n.data.memory),
            // Patches, forgets and expiries name no scope: the memory type decides.
            Effect::Patch { memory, .. } | Effect::Forget { memory, .. } | Effect::Expire { memory, .. } | Effect::Validity { memory, .. } => {
                self.memory(*memory)
            }
            // New vectors can reorder a LIKE watch's result.
            Effect::Vectors { .. } => self.like,
            _ => false,
        })
    }
}

/// A frame for readers: a full result (`snapshot` / `reset`) or a `diff`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Frame {
    pub cq_id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub offset: u64,
    #[serde(flatten)]
    pub body: serde_json::Map<String, Value>,
}

fn records(result: &Value) -> Option<&Vec<Value>> {
    result.get("data")?.get("records")?.as_array()
}

fn record_key(r: &Value) -> (String, String) {
    let s = |v: &Value| v.as_str().map(String::from).unwrap_or_else(|| v.to_string());
    (s(&r["memory_type"]), s(&r["id"]))
}

/// A record as compared between results: `accessed_at` changes on reads.
fn comparable(r: &Value) -> Value {
    let mut r = r.clone();
    if let Some(m) = r.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        m.remove("accessed_at");
    }
    r
}

/// What changed from `old` to `new` (wire results of one query), or `None`; with
/// `ordered`, a change of order too.
pub fn diff(old: &Value, new: &Value, ordered: bool) -> Option<serde_json::Map<String, Value>> {
    let mut body = serde_json::Map::new();
    let (Some(old_r), Some(new_r)) = (records(old), records(new)) else {
        // Not a record list (a count, an aggregate): the new result when it differs.
        if old.get("data") != new.get("data") {
            body.insert("result".into(), new.clone());
            return Some(body);
        }
        return None;
    };
    let old_by: HashMap<_, _> = old_r.iter().map(|r| (record_key(r), r)).collect();
    let new_keys: Vec<_> = new_r.iter().map(record_key).collect();
    let new_set: HashSet<_> = new_keys.iter().cloned().collect();
    let mut added = Vec::new();
    let mut changed = Vec::new();
    for (r, k) in new_r.iter().zip(&new_keys) {
        match old_by.get(k) {
            None => added.push(r.clone()),
            Some(o) if comparable(o) != comparable(r) => changed.push(r.clone()),
            _ => {}
        }
    }
    let removed: Vec<Value> = old_r
        .iter()
        .map(record_key)
        .filter(|k| !new_set.contains(k))
        .map(|(m, id)| json!({ "memory_type": m, "id": id }))
        .collect();
    // The order a client gets by dropping the removed and appending the added.
    let mut expected: Vec<_> = old_r.iter().map(record_key).filter(|k| new_set.contains(k)).collect();
    expected.extend(new_keys.iter().filter(|k| !old_by.contains_key(*k)).cloned());
    let reordered = ordered && expected != new_keys;
    if added.is_empty() && changed.is_empty() && removed.is_empty() && !reordered {
        return None;
    }
    body.insert("added".into(), Value::Array(added));
    body.insert("removed".into(), Value::Array(removed));
    body.insert("changed".into(), Value::Array(changed));
    if reordered {
        body.insert("order".into(), Value::Array(new_keys.iter().map(|(_, id)| json!(id)).collect()));
    }
    Some(body)
}

/// Runs a watch's query, and commits what the watcher decides on its own.
#[async_trait::async_trait]
pub trait Runner: Send + Sync {
    /// The query's wire result now.
    async fn run(&self, spec: &WatchSpec) -> Result<Value, String>;
    /// Commit an `UNWATCH` (idle expiry).
    async fn unwatch(&self, cq_id: &str) -> Result<(), String>;
}

/// One live watch.
struct Cq {
    spec: WatchSpec,
    set: WatchSet,
    /// Every record up to here is in `result`.
    since: u64,
    result: Value,
    /// Frames after the state at `base`, oldest first.
    base: u64,
    frames: VecDeque<Frame>,
    /// When a matching record first arrived since the last run.
    dirty: Option<Instant>,
    last_read: Instant,
    /// Records FORGET took out of `result` (scrubbed): the next diff removes them.
    forgotten: Vec<(String, String)>,
}

impl Cq {
    fn current(&self) -> u64 {
        self.frames.back().map_or(self.base, |f| f.offset)
    }

    fn full(&self, id: &str, kind: &'static str) -> Frame {
        let mut body = serde_json::Map::new();
        body.insert("result".into(), self.result.clone());
        Frame { cq_id: id.to_string(), kind, offset: self.current(), body }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error("unknown watch {0}")]
    Unknown(String),
    #[error("{0}")]
    Query(String),
}

/// What [`Watcher::touched_since`] found.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Touched {
    /// Read up to here: the next call's `after`.
    pub tip: u64,
    /// The watches a record can have changed.
    pub touched: Vec<String>,
    /// Records forgotten (memory, lineage).
    pub forgotten: Vec<(Memory, String)>,
}

/// The partition's watches and the task that keeps them current.
pub struct Watcher {
    log: Arc<dyn Log>,
    applied: watch::Receiver<u64>,
    runner: Arc<dyn Runner>,
    cqs: Mutex<HashMap<String, Cq>>,
    /// The last log offset the task has matched.
    pos: Mutex<u64>,
    /// Bumped on every new frame; readers wait on it.
    frames: watch::Sender<u64>,
    idle: Duration,
    /// Re-runs in progress (see [`Watcher::pending`]).
    running: std::sync::atomic::AtomicUsize,
}

/// Records matched per log read.
const READ_BATCH: usize = 256;

impl Watcher {
    /// A watcher matching from after `from` (the applied offset at open); idle time
    /// from `ADB_WATCH_IDLE` (seconds), default 24 h.
    pub fn new(log: Arc<dyn Log>, applied: watch::Receiver<u64>, runner: Arc<dyn Runner>, from: u64) -> Arc<Self> {
        let idle = std::env::var("ADB_WATCH_IDLE").ok().and_then(|s| s.parse().ok()).map(Duration::from_secs).unwrap_or(DEFAULT_IDLE);
        Self::with_idle(log, applied, runner, from, idle)
    }

    pub fn with_idle(log: Arc<dyn Log>, applied: watch::Receiver<u64>, runner: Arc<dyn Runner>, from: u64, idle: Duration) -> Arc<Self> {
        Arc::new(Self {
            log,
            applied,
            runner,
            cqs: Mutex::new(HashMap::new()),
            pos: Mutex::new(from),
            frames: watch::channel(0).0,
            idle,
            running: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// The applied tip: every record up to it is readable.
    fn tip(&self) -> u64 {
        *self.applied.borrow()
    }

    /// Add a watch (just registered, or loaded at open) and take its snapshot.
    pub async fn add(&self, id: &str, spec: WatchSpec) -> Result<Frame, WatchError> {
        let since = self.tip();
        let result = self.runner.run(&spec).await.map_err(WatchError::Query)?;
        let set = spec.watch_set();
        let mut cq = Cq { spec, set, since, result, base: since, frames: VecDeque::new(), dirty: None, last_read: Instant::now(), forgotten: Vec::new() };
        // Records the task matched before this watch existed: re-run to cover them.
        let pos = self.pos.lock();
        let mut cqs = self.cqs.lock();
        if *pos > since {
            cq.dirty = Some(Instant::now());
        }
        let frame = cq.full(id, "snapshot");
        cqs.insert(id.to_string(), cq);
        Ok(frame)
    }

    /// Mark watches changed from outside this watcher's log (a cluster's watch across
    /// partitions, told by the partitions): they re-run after their debounce. The
    /// caller first raises the applied tip, so the frames get a new offset.
    pub fn poke(&self, ids: &[String]) {
        let mut cqs = self.cqs.lock();
        for id in ids {
            if let Some(c) = cqs.get_mut(id) {
                c.dirty.get_or_insert_with(Instant::now);
            }
        }
    }

    /// A record forgotten elsewhere (another partition): out of every watch's buffered
    /// frames and result, as a FORGET in this log would be.
    pub fn forget(&self, memory: Memory, key: &str) {
        for c in self.cqs.lock().values_mut() {
            scrub(c, memory, key);
        }
        self.frames.send_modify(|g| *g += 1);
    }

    /// For a cluster's watches across partitions: the records of this log after
    /// `after` (`None`: none, just the tip) that can change `watches`, waiting up to
    /// `wait` for one to be applied. Reads at most `max` records per call.
    pub async fn touched_since(&self, after: Option<u64>, watches: &[(String, WatchSpec)], max: usize, wait: Duration) -> Touched {
        let mut applied = self.applied.clone();
        let Some(after) = after else { return Touched { tip: self.tip(), ..Default::default() } };
        if self.tip() <= after {
            let _ = tokio::time::timeout(wait, applied.wait_for(|t| *t > after)).await;
        }
        let tip = self.tip().min(after + max as u64);
        let sets: Vec<(&String, WatchSet)> = watches.iter().map(|(id, s)| (id, s.watch_set())).collect();
        let mut out = Touched { tip: after, ..Default::default() };
        let mut touched = HashSet::new();
        while out.tip < tip {
            let n = ((tip - out.tip) as usize).min(READ_BATCH);
            let records = match self.log.read(LogOffset(out.tip + 1), n).await {
                Ok(r) if !r.is_empty() => r,
                Ok(_) => break,
                Err(LogError::Purged { .. }) => {
                    // Behind the log: every watch may have changed.
                    return Touched { tip, touched: watches.iter().map(|(id, _)| id.clone()).collect(), forgotten: out.forgotten };
                }
                Err(e) => {
                    tracing::warn!(error = %e, "watch: log read failed");
                    break;
                }
            };
            for r in &records {
                for (id, set) in &sets {
                    if set.touches(r) {
                        touched.insert((*id).clone());
                    }
                }
                for e in &r.effects {
                    if let Effect::Forget { memory, lineage_id } = e {
                        out.forgotten.push((*memory, lineage_id.0.clone()));
                    }
                }
                out.tip = r.offset.0;
            }
        }
        out.touched = touched.into_iter().collect();
        out
    }

    /// Whether the watcher has work left: committed records not yet matched, dirty
    /// watches, or a re-run in progress.
    pub fn pending(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        *self.pos.lock() < self.log.last_offset().0 || self.running.load(SeqCst) > 0 || self.cqs.lock().values().any(|c| c.dirty.is_some())
    }

    /// Drop a watch. Returns whether it existed.
    pub fn remove(&self, id: &str) -> bool {
        self.cqs.lock().remove(id).is_some()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.cqs.lock().contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.cqs.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Frames for a reader that has applied the frame at offset `from`: the ones
    /// after it, or a `reset` when `from` isn't a buffered state (or is `None`).
    /// Waits up to `wait` for a frame when there is none yet.
    pub async fn next(&self, id: &str, from: Option<u64>, max: usize, wait: Duration) -> Result<Vec<Frame>, WatchError> {
        let deadline = tokio::time::Instant::now() + wait;
        let mut seen = self.frames.subscribe();
        loop {
            seen.borrow_and_update();
            {
                let mut cqs = self.cqs.lock();
                let cq = cqs.get_mut(id).ok_or_else(|| WatchError::Unknown(id.to_string()))?;
                cq.last_read = Instant::now();
                let Some(from) = from else { return Ok(vec![cq.full(id, "reset")]) };
                let known = from == cq.base || cq.frames.iter().any(|f| f.offset == from);
                if !known {
                    return Ok(vec![cq.full(id, "reset")]);
                }
                let out: Vec<Frame> = cq.frames.iter().filter(|f| f.offset > from).take(max).cloned().collect();
                if !out.is_empty() {
                    return Ok(out);
                }
            }
            if tokio::time::timeout_at(deadline, seen.changed()).await.is_err() {
                return Ok(Vec::new());
            }
        }
    }

    /// The task: match applied records, re-run due watches, drop idle ones. Holds a
    /// weak reference, so it ends with the watcher.
    pub fn spawn(this: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak: Weak<Self> = Arc::downgrade(this);
        let mut applied = this.applied.clone();
        tokio::spawn(async move {
            let mut last_idle_check = Instant::now();
            loop {
                let wake = match weak.upgrade() {
                    Some(w) => w.next_due(),
                    None => return,
                };
                let wait = wake.map_or(Duration::from_secs(1), |d| d.min(Duration::from_secs(1)));
                let _ = tokio::time::timeout(wait, applied.changed()).await;
                let Some(w) = weak.upgrade() else { return };
                w.match_applied().await;
                w.rerun_due().await;
                if last_idle_check.elapsed() >= Duration::from_secs(60) {
                    last_idle_check = Instant::now();
                    w.drop_idle().await;
                }
            }
        })
    }

    /// How long until the next dirty watch is due.
    fn next_due(&self) -> Option<Duration> {
        let now = Instant::now();
        self.cqs
            .lock()
            .values()
            .filter_map(|c| c.dirty.map(|d| (d + Duration::from_millis(c.spec.debounce_ms)).saturating_duration_since(now)))
            .min()
    }

    /// Match the records applied since the last call.
    pub async fn match_applied(&self) {
        let tip = self.tip();
        loop {
            let from = *self.pos.lock() + 1;
            if from > tip {
                return;
            }
            let n = ((tip - from + 1) as usize).min(READ_BATCH);
            let records = match self.log.read(LogOffset(from), n).await {
                Ok(r) => r,
                Err(LogError::Purged { .. }) => {
                    // Fell behind the log: every watch re-runs.
                    let mut pos = self.pos.lock();
                    for c in self.cqs.lock().values_mut() {
                        c.dirty.get_or_insert_with(Instant::now);
                    }
                    *pos = tip;
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "watch: log read failed; retrying");
                    return;
                }
            };
            if records.is_empty() {
                return;
            }
            let mut pos = self.pos.lock();
            let mut cqs = self.cqs.lock();
            let mut scrubbed = false;
            for r in &records {
                for c in cqs.values_mut() {
                    if r.offset.0 > c.since && c.set.touches(r) {
                        c.dirty.get_or_insert_with(Instant::now);
                    }
                }
                for e in &r.effects {
                    if let Effect::Forget { memory, lineage_id } = e {
                        for c in cqs.values_mut() {
                            scrub(c, *memory, &lineage_id.0);
                        }
                        scrubbed = true;
                    }
                }
                *pos = r.offset.0;
            }
            drop(cqs);
            drop(pos);
            if scrubbed {
                self.frames.send_modify(|g| *g += 1);
            }
        }
    }

    /// Re-run the watches whose debounce has passed; buffer a frame where the result
    /// changed.
    pub async fn rerun_due(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        let now = Instant::now();
        // Counted before the dirty marks are cleared, so `pending` never misses it.
        self.running.fetch_add(1, SeqCst);
        let due: Vec<(String, WatchSpec)> = self
            .cqs
            .lock()
            .iter_mut()
            .filter(|(_, c)| c.dirty.is_some_and(|d| now >= d + Duration::from_millis(c.spec.debounce_ms)))
            .map(|(id, c)| {
                c.dirty = None;
                (id.clone(), c.spec.clone())
            })
            .collect();
        for (id, spec) in due {
            let at = self.tip();
            let result = match self.runner.run(&spec).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(cq = %id, error = %e, "watch: re-run failed; retrying on the next change");
                    continue;
                }
            };
            let mut cqs = self.cqs.lock();
            let Some(c) = cqs.get_mut(&id) else { continue };
            if at <= c.since {
                continue;
            }
            let mut body = diff(&c.result, &result, c.set.ordered);
            let forgotten = std::mem::take(&mut c.forgotten);
            let gone: Vec<Value> = forgotten
                .into_iter()
                .filter(|k| !records(&result).is_some_and(|rs| rs.iter().any(|r| record_key(r) == *k)))
                .map(|(m, id)| json!({ "memory_type": m, "id": id }))
                .collect();
            if !gone.is_empty() {
                let b = body.get_or_insert_with(|| {
                    let mut b = serde_json::Map::new();
                    for k in ["added", "removed", "changed"] {
                        b.insert(k.into(), json!([]));
                    }
                    b
                });
                if let Some(Value::Array(r)) = b.get_mut("removed") {
                    r.extend(gone);
                }
            }
            if let Some(body) = body {
                c.frames.push_back(Frame { cq_id: id.clone(), kind: "diff", offset: at, body });
                while c.frames.len() > FRAME_BUFFER {
                    if let Some(f) = c.frames.pop_front() {
                        c.base = f.offset;
                    }
                }
            } else if c.frames.is_empty() {
                // Nothing changed: the state at `at` is the same one.
                c.base = at;
            }
            c.result = result;
            c.since = at;
            drop(cqs);
            self.frames.send_modify(|g| *g += 1);
        }
        self.running.fetch_sub(1, SeqCst);
    }

    /// Unwatch the watches nobody has read for the idle time (decision 3).
    pub async fn drop_idle(&self) {
        let idle: Vec<String> = self.cqs.lock().iter().filter(|(_, c)| c.last_read.elapsed() >= self.idle).map(|(id, _)| id.clone()).collect();
        for id in idle {
            match self.runner.unwatch(&id).await {
                Ok(()) => {
                    self.remove(&id);
                    tracing::info!(cq = %id, idle_s = self.idle.as_secs(), "watch dropped: unread for the idle time");
                }
                Err(e) => tracing::warn!(cq = %id, error = %e, "watch: idle unwatch failed"),
            }
        }
    }
}

/// Remove a forgotten record from a watch's buffered frames and result (FORGET
/// erases content; the next diff reports the removal).
fn scrub(c: &mut Cq, memory: Memory, key: &str) {
    let tag = memory.tag().to_ascii_uppercase();
    let hit = |r: &Value| {
        let (m, id) = record_key(r);
        m.eq_ignore_ascii_case(&tag) && id == key
    };
    let clean_list = |v: &mut Value| {
        if let Some(a) = v.as_array_mut() {
            a.retain(|r| !hit(r));
        }
    };
    for f in c.frames.iter_mut() {
        for k in ["added", "changed"] {
            if let Some(v) = f.body.get_mut(k) {
                clean_list(v);
            }
        }
        if let Some(rs) = f.body.get_mut("result").and_then(|r| r.get_mut("data")).and_then(|d| d.get_mut("records")) {
            clean_list(rs);
        }
    }
    if let Some(rs) = c.result.get_mut("data").and_then(|d| d.get_mut("records")) {
        let before = rs.as_array().map_or(0, |a| a.len());
        clean_list(rs);
        // The client's copy still has it: the next diff reports the removal.
        if rs.as_array().map_or(0, |a| a.len()) != before {
            c.forgotten.push((tag.clone(), key.to_string()));
            c.dirty.get_or_insert_with(Instant::now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, v: u32, content: &str) -> Value {
        json!({"id": id, "memory_type": "SEMANTIC", "data": {"content": content}, "metadata": {"version": v, "accessed_at": "x"}})
    }

    fn result(rs: Vec<Value>) -> Value {
        json!({"success": true, "data": {"type": "records", "count": rs.len(), "records": rs}})
    }

    #[test]
    fn diffs_records_by_id() {
        let a = result(vec![rec("k1", 1, "a"), rec("k2", 1, "b")]);
        assert_eq!(diff(&a, &a, true), None);
        let mut same = rec("k1", 1, "a");
        same["metadata"]["accessed_at"] = json!("later");
        assert_eq!(diff(&a, &result(vec![same, rec("k2", 1, "b")]), true), None, "accessed_at ignored");
        let b = result(vec![rec("k2", 2, "b2"), rec("k3", 1, "c")]);
        let d = diff(&a, &b, true).unwrap();
        assert_eq!(d["added"], json!([rec("k3", 1, "c")]));
        assert_eq!(d["removed"], json!([{"memory_type": "SEMANTIC", "id": "k1"}]));
        assert_eq!(d["changed"], json!([rec("k2", 2, "b2")]));
        assert!(d.get("order").is_none());
        let swapped = result(vec![rec("k2", 1, "b"), rec("k1", 1, "a")]);
        assert_eq!(diff(&a, &swapped, true).unwrap()["order"], json!(["k2", "k1"]));
        assert_eq!(diff(&a, &swapped, false), None, "unordered: storage order is no change");
        // Not records: the whole result.
        let c1 = json!({"data": {"type": "aggregate", "count": 1}});
        let c2 = json!({"data": {"type": "aggregate", "count": 2}});
        assert_eq!(diff(&c1, &c2, true).unwrap()["result"], c2);
        assert_eq!(diff(&c1, &c1, true), None);
    }

    struct Fake {
        unwatched: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Runner for Fake {
        async fn run(&self, _: &WatchSpec) -> Result<Value, String> {
            Ok(result(vec![rec("k1", 1, "a")]))
        }
        async fn unwatch(&self, id: &str) -> Result<(), String> {
            self.unwatched.lock().push(id.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn idle_watches_are_unwatched() {
        let log: Arc<dyn Log> = Arc::new(adb_log::MemLog::new(adb_core::log::PartitionId(0)));
        let runner = Arc::new(Fake { unwatched: Mutex::new(Vec::new()) });
        let (_tx, rx) = watch::channel(0);
        let w = Watcher::with_idle(log, rx, runner.clone(), 0, Duration::from_millis(50));
        let spec = WatchSpec::new(aql_parser::parse("RECALL FROM SEMANTIC").unwrap(), HashMap::new(), None).unwrap();
        w.add("a", spec.clone()).await.unwrap();
        w.add("b", spec).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        w.next("b", None, 1, Duration::ZERO).await.unwrap(); // b is read
        w.drop_idle().await;
        assert_eq!(*runner.unwatched.lock(), ["a"]);
        assert!(!w.contains("a") && w.contains("b"));
    }

    #[test]
    fn watch_sets() {
        let p = |q: &str| WatchSet::of(&aql_parser::parse(q).unwrap()).unwrap();
        let s = p(r#"RECALL FROM SEMANTIC IN SCOPE "team:a" WHERE topic = "x""#);
        assert_eq!(s.memories, Some([Memory::Semantic].into()));
        assert_eq!(s.scope.as_deref(), Some("team:a"));
        assert!(p("RECALL FROM SEMANTIC WITH LINKS").memories.is_none(), "links: every memory type");
        assert!(p("RECALL FROM SEMANTIC LIKE $q LIMIT 3").like);
        assert!(p("RECALL FROM SEMANTIC ORDER BY n DESC").ordered);
        assert!(!s.ordered);
        assert!(WatchSet::of(&aql_parser::parse(r#"STORE INTO SEMANTIC (key = "a")"#).unwrap()).is_err());
    }
}

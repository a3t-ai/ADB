//! PF-5: platform tables' memtable (`.ai/WRITE_PATH.md`).
//!
//! A platform write used to be acknowledged only after its Lance commit (18–34 ms,
//! one at a time). Now upserts and inserts into existing tables are applied to an
//! in-memory table and acknowledged at once; the applier flushes it to Lance in one
//! commit per table every [`FLUSH_AFTER`] or [`FLUSH_ROWS`] rows. Reads merge it over
//! Lance (newest version of a key wins). Anything else (update or delete by filter,
//! create, replace, purge, new columns) flushes first and applies to Lance directly,
//! so it always sees every earlier write.
//!
//! Durability is the log's: the platform's applied offset (what the log may purge
//! below) only moves with a flush. A crash mid-flush is safe: the flush marker names
//! each table's version before it, and on open a table that moved past it has its
//! rows (`flushed`), so the replay skips them there and applies them elsewhere.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow_array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;

use super::*;

/// Rows in memory at which the applier flushes.
pub const FLUSH_ROWS: usize = 4096;
/// The longest a row waits in memory for its flush.
pub const FLUSH_AFTER: Duration = Duration::from_millis(200);

type Row = serde_json::Map<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FlushMarker {
    /// The last record whose rows the flush carries.
    pub to: u64,
    /// Each table it writes, with its Lance version before.
    pub tables: Vec<(String, u64)>,
}

#[derive(Clone)]
pub(crate) enum Rows {
    /// Upserts on `key`: the newest row per key.
    Upserts { key: Vec<String>, rows: BTreeMap<String, Row> },
    /// Inserts, in log order.
    Inserts(Vec<Row>),
}

#[derive(Clone)]
pub(crate) struct MemTable {
    /// The table's Lance version when its first row came in: nothing else commits to
    /// the table while rows wait, so a later version means they are in Lance.
    pub base: u64,
    pub rows: Rows,
}

#[derive(Default)]
pub(crate) struct Mem {
    tables: std::sync::RwLock<HashMap<String, Arc<MemTable>>>,
    /// The last record applied (to memory or Lance).
    through: AtomicU64,
    rows: AtomicUsize,
    since: std::sync::Mutex<Option<Instant>>,
    /// Flushes per upsert table since its key index was last brought up to date (C-14).
    unindexed: std::sync::Mutex<HashMap<String, u32>>,
}

/// Flushes of an upsert table between key-index updates (C-14): ~2 s at one flush per
/// 200 ms, so a keyed read never scans more than a few seconds of unindexed fragments.
const INDEX_EVERY: u32 = 10;

fn cell(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A row's key as the table stores it (every column UTF-8, as `rows_to_table_batch`).
fn key_of(key: &[String], r: &Row) -> String {
    key.iter().map(|c| r.get(c).map(cell).unwrap_or_default()).collect::<Vec<_>>().join("\u{1f}")
}

impl Mem {
    pub fn through(&self) -> u64 {
        self.through.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.tables.read().unwrap().is_empty()
    }

    /// A record with nothing for memory: the applied offset moves with it only while
    /// nothing waits in memory (else the next flush moves it).
    pub fn advance(&self, offset: u64, applied: &mut u64) {
        if self.is_empty() {
            *applied = (*applied).max(offset);
        }
        self.through.fetch_max(offset, Ordering::SeqCst);
    }

    /// Whether `e` can wait in memory: an upsert on a key, or an insert, into a table
    /// whose memory holds the same kind (and key).
    fn takes(&self, e: &Effect) -> bool {
        let t = self.tables.read().unwrap();
        match e {
            Effect::TableUpsert { table, key, .. } if !key.is_empty() => match t.get(table).map(|m| &m.rows) {
                None => true,
                Some(Rows::Upserts { key: k, .. }) => k == key,
                Some(Rows::Inserts(_)) => false,
            },
            Effect::TableInsert { table, .. } => !matches!(t.get(table).map(|m| &m.rows), Some(Rows::Upserts { .. })),
            _ => false,
        }
    }

    pub fn put(&self, record: &LogRecord, bases: &HashMap<String, u64>) {
        let mut t = self.tables.write().unwrap();
        let mut n = 0;
        for e in &record.effects {
            match e {
                Effect::TableUpsert { table, key, rows } => {
                    let base = bases.get(table).copied().unwrap_or(0);
                    let m = t.entry(table.clone()).or_insert_with(|| Arc::new(MemTable { base, rows: Rows::Upserts { key: key.clone(), rows: BTreeMap::new() } }));
                    if let Rows::Upserts { key, rows: held } = &mut Arc::make_mut(m).rows {
                        for r in rows {
                            held.insert(key_of(key, r), r.clone());
                            n += 1;
                        }
                    }
                }
                Effect::TableInsert { table, rows } => {
                    let base = bases.get(table).copied().unwrap_or(0);
                    let m = t.entry(table.clone()).or_insert_with(|| Arc::new(MemTable { base, rows: Rows::Inserts(Vec::new()) }));
                    if let Rows::Inserts(held) = &mut Arc::make_mut(m).rows {
                        held.extend(rows.iter().cloned());
                        n += rows.len();
                    }
                }
                _ => {}
            }
        }
        drop(t);
        self.rows.fetch_add(n, Ordering::SeqCst);
        self.through.fetch_max(record.offset.0, Ordering::SeqCst);
        self.since.lock().unwrap().get_or_insert_with(Instant::now);
    }

    /// `table`'s rows in memory over Lance version `version` (none once they are in it).
    pub fn rows_over(&self, table: &str, version: u64) -> Option<Arc<MemTable>> {
        self.tables.read().unwrap().get(table).filter(|m| version <= m.base).cloned()
    }

    fn snapshot(&self) -> Vec<(String, Arc<MemTable>)> {
        self.tables.read().unwrap().iter().map(|(t, m)| (t.clone(), Arc::clone(m))).collect()
    }

    fn remove(&self, table: &str) {
        self.tables.write().unwrap().remove(table);
    }

    fn flushed_all(&self) {
        self.rows.store(0, Ordering::SeqCst);
        *self.since.lock().unwrap() = None;
    }

    pub fn due(&self) -> bool {
        self.rows.load(Ordering::SeqCst) >= FLUSH_ROWS || self.since.lock().unwrap().is_some_and(|s| s.elapsed() >= FLUSH_AFTER)
    }
}

/// `batch`'s rows matching `filter`, as a Lance scan would select them (Lance's own
/// parser: A3T's filters quote values with double quotes).
fn filter_batch(batch: &RecordBatch, filter: &str) -> PlatformResult<RecordBatch> {
    let planner = lance_datafusion_14::planner::Planner::new(batch.schema());
    let expr = planner.parse_filter(filter).map_err(lance)?;
    let expr = planner.optimize_expr(expr).map_err(lance)?;
    let phys = planner.create_physical_expr(&expr).map_err(lance)?;
    let mask = phys.evaluate(batch).map_err(lance)?.into_array(batch.num_rows()).map_err(lance)?;
    let mask = mask.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| PlatformError::Lance(format!("filter {filter:?} is not a condition")))?;
    filter_record_batch(batch, mask).map_err(lance)
}

/// At most `limit` rows of `batches`, in order.
fn first(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    let mut left = limit;
    let mut out = Vec::new();
    for b in batches {
        if left == 0 {
            break;
        }
        let n = b.num_rows().min(left);
        out.push(b.slice(0, n));
        left -= n;
    }
    out
}

/// A query over Lance and the rows in memory: Lance's rows first (minus those whose
/// key memory holds a newer version of), then memory's, at most `limit`.
pub(crate) async fn merged_query(ds: &Dataset, mem: &MemTable, filter: Option<&str>, limit: usize) -> PlatformResult<Vec<RecordBatch>> {
    let filter = filter.filter(|f| !f.trim().is_empty());
    let schema: SchemaRef = Arc::new(Schema::from(ds.schema()));
    let (rows, shadow): (Vec<Row>, Option<(&[String], HashSet<&String>)>) = match &mem.rows {
        Rows::Upserts { key, rows } => (rows.values().cloned().collect(), Some((key.as_slice(), rows.keys().collect()))),
        Rows::Inserts(v) => (v.clone(), None),
    };
    let mut held = rows_to_table_batch(Arc::clone(&schema), &rows);
    if let Some(f) = filter {
        held = filter_batch(&held, f)?;
    }
    let mut scan = ds.scan();
    if let Some(f) = filter {
        scan.filter(f).map_err(lance)?;
    }
    let extra = shadow.as_ref().map_or(0, |(_, k)| k.len());
    scan.limit(Some((limit + extra) as i64), None).map_err(lance)?;
    let lance_rows: Vec<RecordBatch> = scan.try_into_stream().await.map_err(lance)?.try_collect().await.map_err(lance)?;
    let mut out = Vec::with_capacity(lance_rows.len() + 1);
    for b in lance_rows {
        let Some((key, keys)) = &shadow else {
            out.push(b);
            continue;
        };
        let cols: Vec<Option<&StringArray>> = key.iter().map(|c| b.column_by_name(c).and_then(|a| a.as_any().downcast_ref::<StringArray>())).collect();
        let keep: BooleanArray = (0..b.num_rows())
            .map(|i| {
                let k = cols.iter().map(|c| c.filter(|a| a.is_valid(i)).map(|a| a.value(i).to_string()).unwrap_or_default()).collect::<Vec<_>>().join("\u{1f}");
                Some(!keys.contains(&k))
            })
            .collect();
        out.push(filter_record_batch(&b, &keep).map_err(lance)?);
    }
    out.push(held);
    Ok(first(out, limit))
}

use std::collections::HashSet;

impl PlatformStore {
    /// Whether every platform effect of `record` can wait in memory.
    /// A table whose Lance version is not the one the checkpoint recorded holds commits
    /// it does not know of (a crash between a commit and `_applied.json`): its records
    /// go the direct way, whose version count skips what landed.
    pub(crate) async fn mem_takes(&self, record: &LogRecord, applied: &Applied) -> bool {
        let mut any = false;
        for e in &record.effects {
            match e {
                Effect::TableUpsert { table, .. } | Effect::TableInsert { table, .. } => {
                    let Some(v) = self.version(table).await else { return false };
                    if applied.versions.get(table) != Some(&v) || !self.mem.takes(e) {
                        return false;
                    }
                    any = true;
                }
                e if table_of(e).is_some() => return false,
                _ => {}
            }
        }
        any
    }

    /// Rows waiting in memory.
    pub fn pending_rows(&self) -> usize {
        self.mem.rows.load(Ordering::SeqCst)
    }

    /// Write what waits in memory to Lance.
    pub async fn flush(&self) -> PlatformResult<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let mut applied = self.applied.lock().await;
        Box::pin(self.flush_locked(&mut applied)).await
    }

    /// [`Self::flush`] when it is due (rows or age). Returns whether it flushed.
    pub async fn flush_due(&self) -> PlatformResult<bool> {
        if !self.mem.due() {
            return Ok(false);
        }
        self.flush().await.map(|_| true)
    }

    pub(crate) async fn flush_locked(&self, applied: &mut Applied) -> PlatformResult<()> {
        let tables = self.mem.snapshot();
        if tables.is_empty() {
            return Ok(());
        }
        let t0 = Instant::now();
        let to = self.mem.through();
        applied.flush = Some(FlushMarker { to, tables: tables.iter().map(|(t, m)| (t.clone(), m.base)).collect() });
        self.save(applied)?;
        let mut rows = 0;
        for (table, m) in &tables {
            let effect = match &m.rows {
                Rows::Upserts { key, rows } => Effect::TableUpsert { table: table.clone(), key: key.clone(), rows: rows.values().cloned().collect() },
                Rows::Inserts(r) => Effect::TableInsert { table: table.clone(), rows: r.clone() },
            };
            rows += match &m.rows {
                Rows::Upserts { rows, .. } => rows.len(),
                Rows::Inserts(r) => r.len(),
            };
            // The table is at `base` (nothing else commits to it while rows wait): this
            // commit is new, whatever the version count says.
            applied.versions.insert(table.clone(), m.base);
            self.apply_effect(applied, &effect).await?;
            if let Some(ds) = self.dataset(table).await {
                applied.versions.insert(table.clone(), ds.version().version);
            }
            applied.flushed.insert(table.clone(), to);
            // Readers already pass it by: the table's version is past its base.
            self.mem.remove(table);
            // C-14: the upsert key's index follows the flushed fragments every few flushes
            // (not only at compaction): get_state and other keyed reads stop scanning a
            // growing pile of small fragments.
            if let Rows::Upserts { key, .. } = &m.rows {
                let due = {
                    let mut u = self.mem.unindexed.lock().unwrap();
                    let n = u.entry(table.clone()).or_insert(0);
                    *n += 1;
                    let due = *n >= INDEX_EVERY;
                    if due {
                        *n = 0;
                    }
                    due
                };
                if due {
                    self.index_upsert_key(applied, table, key).await;
                }
            }
        }
        applied.flush = None;
        applied.offset = applied.offset.max(to);
        self.mem.flushed_all();
        self.save(applied)?;
        tracing::debug!(target: "adb::write", tables = tables.len(), rows, to, us = t0.elapsed().as_micros() as u64, "platform memtable flushed");
        Ok(())
    }
}

impl PlatformStore {
    /// Bring `table`'s key index up to date with its fragments, as a compaction does and
    /// under the same marker (`compacting`): the index commit is no effect, and a crash
    /// during it must not count as one on replay. Failures are logged; the next flush
    /// tries again.
    async fn index_upsert_key(&self, applied: &mut Applied, table: &str, key: &[String]) {
        let Some(ds) = self.dataset(table).await else { return };
        applied.compacting = Some(table.to_string());
        if let Err(e) = self.save(applied) {
            tracing::warn!(%table, error = %e, "key index not updated");
            applied.compacting = None;
            return;
        }
        let mut ds = (*ds).clone();
        let r = index_keys(&mut ds, key).await;
        let _ = ds.checkout_latest().await;
        applied.versions.insert(table.to_string(), ds.version().version);
        applied.compacting = None;
        self.tables.write().await.insert(table.to_string(), Arc::new(ds));
        if let Err(e) = r.and_then(|_| self.save(applied)) {
            tracing::warn!(%table, error = %e, "key index not updated; next flush tries again");
        }
    }
}

//! Platform tables (DELIVERY_PLAN stage G, `G_PLATFORM_TABLES.md`, ARCHITECTURE §15.1).
//!
//! FlowR / A3T's registry tables (`template_registry`, `users`, `agent_configs`, …),
//! with the semantics of FlowR's `call:lancedb`: every column is UTF-8; `query` is a
//! Lance SQL filter + limit; `insert` creates the table from the first row if needed;
//! `update` sets column → SQL expression on matching rows; `delete` by filter. Each
//! table is its own Lance dataset under `<root>/<table>.lance`, so filters behave
//! exactly as in the embedded LanceDB A3T uses today.
//!
//! Writes are log effects (`TableCreate` / `TableInsert` / `TableUpdate` /
//! `TableDelete`), applied in log order and synchronously, before the write returns.
//! `_applied.json` records the last applied offset and each table's Lance version,
//! rewritten after every record. Every effect is at most one Lance commit, so after a
//! crash between a commit and that rewrite, a table whose version is ahead of the
//! file already holds the effect: it is counted, not applied twice (an update or
//! delete re-run later in the log could touch rows inserted after it).
//!
//! Compaction (W5-L4) rewrites each table without its deleted rows and removes its
//! old versions, so platform data deleted in A3T leaves the disk. It commits
//! versions that are no effect: it runs under the write lock, records the table it
//! is compacting in `_applied.json` first, and afterwards records the table's new
//! version; `open` settles a table left marked by a crash at its latest version.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use adb_core::log::{Effect, LogRecord};
use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("no platform table {0}")]
    NoTable(String),
    #[error("invalid table name {0:?}")]
    BadName(String),
    #[error("lance: {0}")]
    Lance(String),
    #[error("io: {0}")]
    Io(String),
    /// DB-13: an update, delete, upsert or replace of an append-only table.
    #[error("table {0} is append-only: rows can't be changed or deleted (purge by age only)")]
    AppendOnly(String),
}

/// The platform table an effect writes, if it is a platform-table effect.
pub fn table_of(e: &Effect) -> Option<&str> {
    match e {
        Effect::TableCreate { table, .. }
        | Effect::TableInsert { table, .. }
        | Effect::TableUpdate { table, .. }
        | Effect::TableDelete { table, .. }
        | Effect::TableUpsert { table, .. }
        | Effect::TableReplace { table, .. }
        | Effect::TableAddColumns { table, .. }
        | Effect::TableAppendOnly { table }
        | Effect::TablePurge { table, .. } => Some(table),
        _ => None,
    }
}

fn valid_column(c: &str) -> bool {
    let mut it = c.chars();
    matches!(it.next(), Some(f) if f.is_ascii_alphabetic() || f == '_') && it.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') && c.len() <= 64
}

pub type PlatformResult<T> = Result<T, PlatformError>;

fn lance(e: impl std::fmt::Display) -> PlatformError {
    PlatformError::Lance(e.to_string())
}

/// What has been applied: the last log offset and each table's Lance version then.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Applied {
    offset: u64,
    versions: BTreeMap<String, u64>,
    /// Each table's version after its last compaction (W5-L4).
    #[serde(default)]
    compacted: BTreeMap<String, u64>,
    /// The table being compacted: its new versions hold no effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compacting: Option<String>,
    /// Tables made append-only (DB-13, `TableAppendOnly`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    append_only: std::collections::BTreeSet<String>,
    /// Each table's upsert key columns: compaction keeps a BTree index on them, so an
    /// upsert's merge looks keys up instead of scanning the table (PF-5).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    upsert_keys: BTreeMap<String, Vec<String>>,
    /// A combined commit of several records' upserts in flight (PF-4,
    /// [`PlatformStore::apply_batch`]): on open, a version of `table` above `base` means
    /// it landed, so every record up to `to` is in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    folding: Option<Folding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Folding {
    table: String,
    base: u64,
    to: u64,
}

/// The one (table, key) a record upserts into, if upserts into it are all it holds.
fn upsert_only(r: &LogRecord) -> Option<(&str, &[String])> {
    let mut one: Option<(&str, &[String])> = None;
    for e in &r.effects {
        let Effect::TableUpsert { table, key, .. } = e else { return None };
        match one {
            None => one = Some((table.as_str(), key.as_slice())),
            Some((t, k)) if t == table && k == key.as_slice() => {}
            Some(_) => return None,
        }
    }
    one
}

/// A BTree index on each of `keys` (named `<column>_idx`), created if missing, then
/// brought up to date with the fragments written since (no rebuild).
async fn index_keys(ds: &mut Dataset, keys: &[String]) -> PlatformResult<()> {
    use lance::index::DatasetIndexExt;
    let have: Vec<String> = ds.load_indices().await.map_err(lance)?.iter().map(|i| i.name.clone()).collect();
    let mut created = false;
    for col in keys {
        let name = format!("{col}_idx");
        if !have.contains(&name) {
            ds.create_index(&[col.as_str()], lance_index::IndexType::BTree, Some(name), &lance_index::scalar::ScalarIndexParams::default(), true)
                .await
                .map_err(lance)?;
            created = true;
        }
    }
    if !created && !keys.is_empty() {
        ds.optimize_indices(&Default::default()).await.map_err(lance)?;
    }
    Ok(())
}

/// What a platform compaction did (W5-L4).
#[derive(Debug, Clone, Default)]
pub struct PlatformCompactStats {
    pub tables: usize,
    pub fragments_removed: usize,
    pub old_versions: u64,
    pub bytes_removed: u64,
    pub took: std::time::Duration,
}

/// Versions a table may gain before it is compacted outside the nightly run.
pub const COMPACT_AFTER_VERSIONS: u64 = 256;

/// One partition's platform tables.
pub struct PlatformStore {
    root: PathBuf,
    /// Serialises writes (the sequencer does already; this guards the file).
    applied: Mutex<Applied>,
    /// Open datasets, refreshed after each write; reads clone the `Arc`.
    tables: RwLock<HashMap<String, Arc<Dataset>>>,
    /// Held by scans while they read files; compaction takes it to delete old ones.
    files: RwLock<()>,
}

/// A table name usable as a directory name.
fn check_name(table: &str) -> PlatformResult<()> {
    let ok = !table.is_empty()
        && table.len() <= 128
        && table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !table.starts_with('_');
    if ok { Ok(()) } else { Err(PlatformError::BadName(table.to_string())) }
}

/// FlowR's `records_to_batch`: an all-UTF-8 batch with the first row's fields as the
/// schema; non-string values become their JSON text.
pub fn rows_to_batch(rows: &[serde_json::Map<String, Value>]) -> RecordBatch {
    let keys: Vec<String> = rows.first().map(|r| r.keys().cloned().collect()).unwrap_or_default();
    let schema = Arc::new(Schema::new(keys.iter().map(|k| Field::new(k, DataType::Utf8, true)).collect::<Vec<_>>()));
    let cols = keys
        .iter()
        .map(|k| {
            let v: Vec<Option<String>> = rows
                .iter()
                .map(|r| r.get(k).map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                }))
                .collect();
            Arc::new(StringArray::from(v)) as Arc<dyn Array>
        })
        .collect();
    RecordBatch::try_new(schema, cols).expect("columns match the schema")
}

/// `rows` as a batch of an existing table's (all-UTF-8) schema: columns a row lacks
/// are null. `validate` has refused fields the table doesn't have.
fn rows_to_table_batch(schema: SchemaRef, rows: &[serde_json::Map<String, Value>]) -> RecordBatch {
    let cols = schema
        .fields()
        .iter()
        .map(|f| {
            let v: Vec<Option<String>> = rows
                .iter()
                .map(|r| r.get(f.name()).map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                }))
                .collect();
            Arc::new(StringArray::from(v)) as Arc<dyn Array>
        })
        .collect();
    RecordBatch::try_new(schema, cols).expect("columns match the schema")
}

/// FlowR's `batches_to_json`: UTF-8 columns as JSON fields, nulls left out.
pub fn batches_to_json(batches: &[RecordBatch]) -> Vec<Value> {
    let mut rows = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let mut o = serde_json::Map::new();
            for (i, f) in b.schema().fields().iter().enumerate() {
                if let Some(a) = b.column(i).as_any().downcast_ref::<StringArray>() {
                    if a.is_valid(r) {
                        o.insert(f.name().clone(), Value::String(a.value(r).to_string()));
                    }
                }
            }
            if !o.is_empty() {
                rows.push(Value::Object(o));
            }
        }
    }
    rows
}

/// A filter that parses and names real columns: Lance only parses it when the scan
/// is planned, so plan one (no data is read).
async fn check_filter(ds: &Dataset, filter: &str) -> PlatformResult<()> {
    let mut scan = ds.scan();
    scan.filter(filter).map_err(lance)?;
    scan.explain_plan(false).await.map(|_| ()).map_err(lance)
}

impl PlatformStore {
    pub async fn open(root: impl Into<PathBuf>) -> PlatformResult<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| PlatformError::Io(e.to_string()))?;
        let mut applied: Applied = match std::fs::read(root.join("_applied.json")) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| PlatformError::Io(format!("_applied.json: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Applied::default(),
            Err(e) => return Err(PlatformError::Io(e.to_string())),
        };
        let interrupted = applied.compacting.take();
        let folding = applied.folding.take();
        let store = Self { root, applied: Mutex::new(Applied::default()), tables: RwLock::new(HashMap::new()), files: RwLock::new(()) };
        for name in store.list_tables()? {
            if let Ok(ds) = Dataset::open(&store.uri(&name)).await {
                // A compaction a crash interrupted: its commits (if any) are no effects.
                if interrupted.as_deref() == Some(name.as_str()) {
                    applied.versions.insert(name.clone(), ds.version().version);
                }
                store.tables.write().await.insert(name, Arc::new(ds));
            }
        }
        // A combined upsert a crash interrupted (PF-4): landed if the table moved past it.
        if let Some(f) = &folding {
            if let Some(ds) = store.tables.read().await.get(&f.table) {
                if ds.version().version > f.base {
                    applied.versions.insert(f.table.clone(), ds.version().version);
                    applied.offset = applied.offset.max(f.to);
                }
            }
        }
        if interrupted.is_some() || folding.is_some() {
            store.save(&applied)?;
        }
        *store.applied.lock().await = applied;
        Ok(store)
    }

    fn uri(&self, table: &str) -> String {
        self.root.join(format!("{table}.lance")).to_string_lossy().to_string()
    }

    /// The last log offset applied.
    pub async fn applied_offset(&self) -> u64 {
        self.applied.lock().await.offset
    }

    /// The tables, by name.
    pub fn list_tables(&self) -> PlatformResult<Vec<String>> {
        let mut names: Vec<String> = std::fs::read_dir(&self.root)
            .map_err(|e| PlatformError::Io(e.to_string()))?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_suffix(".lance")).map(String::from))
            .collect();
        names.sort();
        Ok(names)
    }

    async fn dataset(&self, table: &str) -> Option<Arc<Dataset>> {
        self.tables.read().await.get(table).cloned()
    }

    /// The table's current Lance version (`None` if there is no such table).
    pub async fn version(&self, table: &str) -> Option<u64> {
        self.dataset(table).await.map(|d| d.version().version)
    }

    /// Rows of `table` matching `filter` (Lance SQL), at most `limit`.
    pub async fn query(&self, table: &str, filter: Option<&str>, limit: usize) -> PlatformResult<Vec<RecordBatch>> {
        let _files = self.files.read().await;
        let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.to_string()))?;
        let mut scan = ds.scan();
        if let Some(f) = filter.filter(|f| !f.trim().is_empty()) {
            scan.filter(f).map_err(lance)?;
        }
        scan.limit(Some(limit as i64), None).map_err(lance)?;
        scan.try_into_stream().await.map_err(lance)?.try_collect().await.map_err(lance)
    }

    /// Run `f` over every table's current dataset, by name (the SQL console). A
    /// compaction waits until `f` is done, so the versions it reads stay on disk.
    pub async fn with_tables<T, F, Fut>(&self, f: F) -> T
    where
        F: FnOnce(Vec<(String, Arc<Dataset>)>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _files = self.files.read().await;
        let tables: Vec<(String, Arc<Dataset>)> =
            self.tables.read().await.iter().map(|(n, d)| (n.clone(), Arc::clone(d))).collect();
        f(tables).await
    }

    /// A table's schema.
    pub async fn schema(&self, table: &str) -> PlatformResult<SchemaRef> {
        let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.to_string()))?;
        Ok(Arc::new(Schema::from(ds.schema())))
    }

    /// Check a write before it is logged: a logged effect must apply, or catch-up
    /// would fail on it forever. Unknown tables (for update / delete), bad names,
    /// unknown columns and filters that don't parse are refused here.
    pub async fn validate(&self, effect: &Effect) -> PlatformResult<()> {
        let bad = |m: String| Err(PlatformError::Lance(m));
        // DB-13: an append-only table takes inserts and retention purges only.
        if let Effect::TableUpdate { table, .. } | Effect::TableDelete { table, .. } | Effect::TableUpsert { table, .. } | Effect::TableReplace { table, .. } = effect {
            if self.applied.lock().await.append_only.contains(table) {
                return Err(PlatformError::AppendOnly(table.clone()));
            }
        }
        match effect {
            Effect::TableAddColumns { table, columns } => {
                check_name(table)?;
                self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                match columns.iter().find(|c| !valid_column(c)) {
                    Some(c) => bad(format!("column name {c:?}: letters, digits and _, starting with a letter or _")),
                    None if columns.is_empty() => bad(format!("add columns to {table}: name at least one")),
                    None => Ok(()),
                }
            }
            Effect::TableAppendOnly { table } => {
                check_name(table)?;
                self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.clone())).map(|_| ())
            }
            Effect::TablePurge { table, column, before } => {
                check_name(table)?;
                let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                if Schema::from(ds.schema()).field_with_name(column).is_err() {
                    return bad(format!("table {table} has no column {column}"));
                }
                if before.is_empty() {
                    return bad(format!("purge {table}: name the time to purge before"));
                }
                Ok(())
            }
            Effect::TableCreate { table, .. } => check_name(table),
            Effect::TableInsert { table, rows } => {
                check_name(table)?;
                if let Some(ds) = self.dataset(table).await {
                    let schema = Schema::from(ds.schema());
                    for r in rows {
                        if let Some(k) = r.keys().find(|k| schema.field_with_name(k).is_err()) {
                            return bad(format!("table {table} has no column {k}"));
                        }
                    }
                }
                Ok(())
            }
            Effect::TableUpsert { table, key, rows } => {
                check_name(table)?;
                if key.is_empty() {
                    return bad(format!("an upsert into {table} needs key columns"));
                }
                if let Some(k) = rows.iter().find_map(|r| key.iter().find(|k| !r.contains_key(k.as_str()))) {
                    return bad(format!("a row upserted into {table} has no key column {k}"));
                }
                if let Some(ds) = self.dataset(table).await {
                    let schema = Schema::from(ds.schema());
                    if let Some(k) = rows.iter().flat_map(|r| r.keys()).chain(key.iter()).find(|k| schema.field_with_name(k).is_err()) {
                        return bad(format!("table {table} has no column {k}"));
                    }
                }
                Ok(())
            }
            Effect::TableReplace { table, columns, rows } => {
                check_name(table)?;
                if let Some(k) = rows.iter().flat_map(|r| r.keys()).find(|k| !columns.contains(k)) {
                    return bad(format!("row field {k} is not among the columns of {table}"));
                }
                Ok(())
            }
            Effect::TableUpdate { table, filter, values } => {
                check_name(table)?;
                let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                let schema = Schema::from(ds.schema());
                if let Some((c, _)) = values.iter().find(|(c, _)| schema.field_with_name(c).is_err()) {
                    return bad(format!("table {table} has no column {c}"));
                }
                check_filter(&ds, filter).await
            }
            Effect::TableDelete { table, filter } => {
                check_name(table)?;
                let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                check_filter(&ds, filter).await
            }
            _ => Ok(()),
        }
    }

    /// Check a transaction (DB-6): several effects logged as one record, applied in
    /// order. A table created earlier in the transaction counts as existing. After a
    /// schema change of a table (create, add columns, replace, append-only), a later
    /// update, delete or purge of it in the same transaction is refused: its filter or
    /// columns couldn't be checked, and a logged effect must apply.
    pub async fn validate_all(&self, effects: &[Effect]) -> PlatformResult<()> {
        let mut created: std::collections::HashSet<String> = Default::default();
        let mut reshaped: std::collections::HashSet<String> = Default::default();
        let mut append: std::collections::HashSet<String> = Default::default();
        for e in effects {
            let table = table_of(e).ok_or_else(|| PlatformError::Lance("a transaction holds platform-table effects only".into()))?;
            if append.contains(table) && matches!(e, Effect::TableUpdate { .. } | Effect::TableDelete { .. } | Effect::TableUpsert { .. } | Effect::TableReplace { .. }) {
                return Err(PlatformError::AppendOnly(table.to_string()));
            }
            if reshaped.contains(table) && matches!(e, Effect::TableUpdate { .. } | Effect::TableDelete { .. } | Effect::TablePurge { .. }) {
                return Err(PlatformError::Lance(format!("table {table} changed shape earlier in this transaction: update or delete its rows in a transaction of their own")));
            }
            match self.validate(e).await {
                Err(PlatformError::NoTable(t)) if created.contains(&t) => {}
                other => other?,
            }
            match e {
                Effect::TableCreate { table, .. } | Effect::TableReplace { table, .. } => {
                    created.insert(table.clone());
                    reshaped.insert(table.clone());
                }
                Effect::TableInsert { table, .. } | Effect::TableUpsert { table, .. } => {
                    created.insert(table.clone());
                }
                Effect::TableAddColumns { table, .. } => {
                    reshaped.insert(table.clone());
                }
                Effect::TableAppendOnly { table } => {
                    reshaped.insert(table.clone());
                    append.insert(table.clone());
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Apply a committed record's platform effects (in log order; a record at or
    /// below the applied offset is skipped). Returns whether it was new.
    pub async fn apply(&self, record: &LogRecord) -> PlatformResult<bool> {
        let mut applied = self.applied.lock().await;
        self.apply_locked(&mut applied, record).await
    }

    /// [`Self::apply`] for `records` in log order (PF-4): a run of records that only
    /// upsert into one table on one key is one Lance commit instead of one each — the
    /// commit is most of a platform write's time. One result per record; a run whose
    /// commit fails is applied record by record, so only a bad one gets its error.
    pub async fn apply_batch(&self, records: &[LogRecord]) -> Vec<PlatformResult<bool>> {
        let mut applied = self.applied.lock().await;
        let mut out: Vec<PlatformResult<bool>> = Vec::with_capacity(records.len());
        let mut i = 0;
        while i < records.len() {
            let r = &records[i];
            let Some((table, key)) = (r.offset.0 > applied.offset).then(|| upsert_only(r)).flatten() else {
                out.push(self.apply_locked(&mut applied, r).await);
                i += 1;
                continue;
            };
            // The run: upserts into (table, key), with records for other targets between.
            let mut j = i + 1;
            let mut upserts = 1;
            while j < records.len() {
                let n = &records[j];
                if !n.effects.iter().any(|e| table_of(e).is_some()) {
                    j += 1;
                } else if upsert_only(n) == Some((table, key)) {
                    upserts += 1;
                    j += 1;
                } else {
                    break;
                }
            }
            let base = self.dataset(table).await.map(|d| d.version().version);
            let (Some(base), true) = (base, upserts > 1) else {
                out.push(self.apply_locked(&mut applied, r).await);
                i += 1;
                continue;
            };
            let run = &records[i..j];
            let rows: Vec<_> = run.iter().flat_map(|r| r.effects.iter()).flat_map(|e| match e {
                Effect::TableUpsert { rows, .. } => rows.clone(),
                _ => Vec::new(),
            }).collect();
            let combined = Effect::TableUpsert { table: table.to_string(), key: key.to_vec(), rows };
            let to = run.last().map_or(0, |r| r.offset.0);
            applied.folding = Some(Folding { table: table.to_string(), base, to });
            let landed = match self.save(&applied) {
                Ok(()) => self.apply_effect(&mut applied, &combined).await,
                Err(e) => Err(e),
            };
            applied.folding = None;
            match landed {
                Ok(()) => {
                    if let Some(ds) = self.dataset(table).await {
                        applied.versions.insert(table.to_string(), ds.version().version);
                    }
                    applied.offset = to;
                    let saved = self.save(&applied);
                    for r in run {
                        out.push(match &saved {
                            Ok(()) => Ok(r.effects.iter().any(|e| table_of(e).is_some())),
                            Err(e) => Err(PlatformError::Io(e.to_string())),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(%table, records = run.len(), error = %e, "combined upsert failed; applying its records one at a time");
                    for r in run {
                        out.push(self.apply_locked(&mut applied, r).await);
                    }
                }
            }
            i = j;
        }
        out
    }

    async fn apply_locked(&self, applied: &mut Applied, record: &LogRecord) -> PlatformResult<bool> {
        if record.offset.0 <= applied.offset {
            return Ok(false);
        }
        // Nothing for the platform tables (most records: memory writes). The offset
        // moves in memory only: `_applied.json` is written and fsynced twice per save,
        // and a follower applies every record — saving here held a follower to a few
        // hundred records/s and made a takeover wait for it (8 Nov). After a restart the
        // records since the last save are read again and skipped again.
        if !record.effects.iter().any(|e| table_of(e).is_some()) {
            applied.offset = record.offset.0;
            return Ok(false);
        }
        for effect in &record.effects {
            self.apply_effect(applied, effect).await?;
        }
        // Every effect of the record is in now: record each touched table's real
        // version, so an effect that made no commit (a no-op add of columns, an
        // append-only mark) can't shift the crash count of later records.
        for table in record.effects.iter().filter_map(table_of) {
            if let Some(ds) = self.dataset(table).await {
                applied.versions.insert(table.to_string(), ds.version().version);
            }
        }
        applied.offset = record.offset.0;
        self.save(applied)?;
        Ok(true)
    }

    async fn apply_effect(&self, applied: &mut Applied, effect: &Effect) -> PlatformResult<()> {
        let table = match effect {
            Effect::TableCreate { table, .. }
            | Effect::TableInsert { table, .. }
            | Effect::TableUpdate { table, .. }
            | Effect::TableDelete { table, .. }
            | Effect::TableUpsert { table, .. }
            | Effect::TableReplace { table, .. }
            | Effect::TablePurge { table, .. } => table.clone(),
            // No Lance commit: a mark in `_applied.json`.
            Effect::TableAppendOnly { table } => {
                applied.append_only.insert(table.clone());
                return Ok(());
            }
            // Metadata-only; a column the table has is skipped, so a re-run after a
            // crash finds nothing to add and commits nothing.
            Effect::TableAddColumns { table, columns } => {
                let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                let have = Schema::from(ds.schema());
                let mut new: Vec<&String> = columns.iter().filter(|c| have.field_with_name(c).is_err()).collect();
                new.dedup();
                if new.is_empty() {
                    return Ok(());
                }
                let add = Arc::new(Schema::new(new.iter().map(|c| Field::new(c.as_str(), DataType::Utf8, true)).collect::<Vec<_>>()));
                let mut ds = (*ds).clone();
                ds.add_columns(lance::dataset::NewColumnTransform::AllNulls(add), None, None).await.map_err(lance)?;
                applied.versions.insert(table.clone(), ds.version().version);
                self.tables.write().await.insert(table.clone(), Arc::new(ds));
                return Ok(());
            }
            _ => return Ok(()),
        };
        let current = self.dataset(&table).await.map(|d| d.version().version);
        let recorded = applied.versions.get(&table).copied();
        // Committed before a crash, but not yet in `_applied.json`: count it, one
        // version per effect. A table that exists but isn't recorded was created by
        // this effect (a create, or an insert into a missing table): a new dataset is
        // version 1, so effects after it in the same record are still checked.
        match (current, recorded) {
            (Some(c), Some(r)) if c > r => {
                applied.versions.insert(table, r + 1);
                return Ok(());
            }
            (Some(_), None) => {
                applied.versions.insert(table, 1);
                return Ok(());
            }
            _ => {}
        }
        let ds = match effect {
            Effect::TableCreate { columns, .. } => {
                if current.is_some() {
                    return Ok(()); // create_if_not_exists
                }
                let cols: Vec<String> = if columns.is_empty() { vec!["id".into()] } else { columns.clone() };
                let schema = Arc::new(Schema::new(cols.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect::<Vec<_>>()));
                let empty = RecordBatch::new_empty(schema.clone());
                let params = WriteParams { mode: WriteMode::Create, ..Default::default() };
                Dataset::write(RecordBatchIterator::new([Ok(empty)], schema), &self.uri(&table), Some(params))
                    .await
                    .map_err(lance)?
            }
            Effect::TableInsert { rows, .. } => {
                if rows.is_empty() {
                    return Ok(());
                }
                // A new table takes the first row's fields; an existing one its schema.
                let batch = match self.dataset(&table).await {
                    Some(ds) => rows_to_table_batch(Arc::new(Schema::from(ds.schema())), rows),
                    None => rows_to_batch(rows),
                };
                let schema = batch.schema();
                let mode = if current.is_some() { WriteMode::Append } else { WriteMode::Create };
                let params = WriteParams { mode, ..Default::default() };
                Dataset::write(RecordBatchIterator::new([Ok(batch)], schema), &self.uri(&table), Some(params))
                    .await
                    .map_err(lance)?
            }
            Effect::TableUpsert { key, rows, .. } => {
                if !key.is_empty() {
                    applied.upsert_keys.insert(table.clone(), key.clone());
                }
                if rows.is_empty() {
                    return Ok(());
                }
                // merge_insert needs each key once in its source: the last row wins.
                let mut seen = std::collections::HashSet::new();
                let mut unique: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
                for r in rows.iter().rev() {
                    let k: Vec<String> = key.iter().map(|c| r.get(c).map(|v| v.to_string()).unwrap_or_default()).collect();
                    if seen.insert(k) {
                        unique.push(r.clone());
                    }
                }
                unique.reverse();
                match self.dataset(&table).await {
                    // A missing table is created with these rows, as an insert would.
                    None => {
                        let batch = rows_to_batch(&unique);
                        let schema = batch.schema();
                        let params = WriteParams { mode: WriteMode::Create, ..Default::default() };
                        Dataset::write(RecordBatchIterator::new([Ok(batch)], schema), &self.uri(&table), Some(params))
                            .await
                            .map_err(lance)?
                    }
                    Some(ds) => {
                        let batch = rows_to_table_batch(Arc::new(Schema::from(ds.schema())), &unique);
                        let schema = batch.schema();
                        let mut b = lance::dataset::MergeInsertBuilder::try_new(ds, key.clone()).map_err(lance)?;
                        b.when_matched(lance::dataset::WhenMatched::UpdateAll).when_not_matched(lance::dataset::WhenNotMatched::InsertAll);
                        let (ds, _stats) = b
                            .try_build()
                            .map_err(lance)?
                            .execute_reader(RecordBatchIterator::new([Ok(batch)], schema))
                            .await
                            .map_err(lance)?;
                        Arc::try_unwrap(ds).unwrap_or_else(|a| (*a).clone())
                    }
                }
            }
            Effect::TableUpdate { filter, values, .. } => {
                let ds = self.dataset(&table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                let mut b = lance::dataset::UpdateBuilder::new(ds).update_where(filter).map_err(lance)?;
                for (col, expr) in values {
                    b = b.set(col, expr).map_err(lance)?;
                }
                let r = b.build().map_err(lance)?.execute().await.map_err(lance)?;
                Arc::try_unwrap(r.new_dataset).unwrap_or_else(|a| (*a).clone())
            }
            Effect::TableDelete { filter, .. } => {
                let ds = self.dataset(&table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                let mut ds = (*ds).clone();
                ds.delete(filter).await.map_err(lance)?;
                ds
            }
            Effect::TablePurge { column, before, .. } => {
                let ds = self.dataset(&table).await.ok_or_else(|| PlatformError::NoTable(table.clone()))?;
                let mut ds = (*ds).clone();
                ds.delete(&format!("{column} < '{}'", before.replace('\'', "''"))).await.map_err(lance)?;
                ds
            }
            Effect::TableReplace { columns, rows, .. } => {
                // One overwrite commit: the new columns and rows, or an empty table.
                let cols: Vec<String> = if columns.is_empty() { vec!["id".into()] } else { columns.clone() };
                let schema = Arc::new(Schema::new(cols.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect::<Vec<_>>()));
                let batch = match rows.is_empty() {
                    true => RecordBatch::new_empty(schema.clone()),
                    false => rows_to_table_batch(schema.clone(), rows),
                };
                let mode = if current.is_some() { WriteMode::Overwrite } else { WriteMode::Create };
                let params = WriteParams { mode, ..Default::default() };
                Dataset::write(RecordBatchIterator::new([Ok(batch)], schema), &self.uri(&table), Some(params))
                    .await
                    .map_err(lance)?
            }
            _ => unreachable!(),
        };
        applied.versions.insert(table.clone(), ds.version().version);
        self.tables.write().await.insert(table, Arc::new(ds));
        Ok(())
    }

    /// Whether a table has gained more than [`COMPACT_AFTER_VERSIONS`] versions since
    /// its last compaction.
    pub async fn compaction_due(&self) -> bool {
        let applied = self.applied.lock().await;
        applied.versions.iter().any(|(t, v)| v.saturating_sub(applied.compacted.get(t).copied().unwrap_or(0)) > COMPACT_AFTER_VERSIONS)
    }

    /// Compact every table (W5-L4): rewrite fragments with deleted rows (A3T's
    /// DELETEs, and the rows an UPDATE replaced) and small fragments, then remove
    /// every old version and the files only they used. Writes wait for it.
    pub async fn compact(&self) -> PlatformResult<PlatformCompactStats> {
        let started = std::time::Instant::now();
        let mut applied = self.applied.lock().await;
        let mut stats = PlatformCompactStats::default();
        for table in self.list_tables()? {
            let Some(ds) = self.dataset(&table).await else { continue };
            let mut ds = (*ds).clone();
            applied.compacting = Some(table.clone());
            self.save(&applied)?;
            let options = lance::dataset::optimize::CompactionOptions {
                materialize_deletions_threshold: 0.0,
                num_threads: Some(1),
                ..Default::default()
            };
            let compacted = lance::dataset::optimize::compact_files(&mut ds, options, None).await;
            // Upsert keys indexed (built once, then extended to new fragments), under the
            // same mark: these commits hold no effect either.
            if let Some(keys) = applied.upsert_keys.get(&table).cloned() {
                if let Err(e) = index_keys(&mut ds, &keys).await {
                    tracing::warn!(%table, error = %e, "upsert key index not updated; upserts scan until the next compaction");
                }
            }
            // Whatever it committed, the latest version is now the applied one.
            ds.checkout_latest().await.map_err(lance)?;
            let version = ds.version().version;
            applied.versions.insert(table.clone(), version);
            applied.compacting = None;
            self.save(&applied)?;
            let ds = Arc::new(ds);
            self.tables.write().await.insert(table.clone(), Arc::clone(&ds));
            stats.fragments_removed += compacted.map_err(lance)?.fragments_removed;
            // The compacted files on disk before the old ones are deleted (L11, W9).
            adb_core::disk::sync_fs_async(&self.root).await.map_err(|e| PlatformError::Io(e.to_string()))?;
            let removed = {
                let _files = self.files.write().await;
                ds.cleanup_old_versions(chrono::TimeDelta::zero(), Some(true), Some(false)).await.map_err(lance)?
            };
            applied.compacted.insert(table.clone(), version);
            self.save(&applied)?;
            stats.tables += 1;
            stats.old_versions += removed.old_versions;
            stats.bytes_removed += removed.bytes_removed;
        }
        stats.took = started.elapsed();
        Ok(stats)
    }

    /// Rewrite `_applied.json` atomically (temp file, fsync, rename).
    fn save(&self, applied: &Applied) -> PlatformResult<()> {
        let io = |e: std::io::Error| PlatformError::Io(e.to_string());
        let tmp = self.root.join("_applied.json.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).map_err(io)?;
            f.write_all(&serde_json::to_vec(applied).map_err(|e| PlatformError::Io(e.to_string()))?).map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        std::fs::rename(&tmp, self.root.join("_applied.json")).map_err(io)?;
        if let Ok(dir) = std::fs::File::open(&self.root) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

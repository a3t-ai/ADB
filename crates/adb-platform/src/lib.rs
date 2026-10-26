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
}

/// One partition's platform tables.
pub struct PlatformStore {
    root: PathBuf,
    /// Serialises writes (the sequencer does already; this guards the file).
    applied: Mutex<Applied>,
    /// Open datasets, refreshed after each write; reads clone the `Arc`.
    tables: RwLock<HashMap<String, Arc<Dataset>>>,
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
        let applied = match std::fs::read(root.join("_applied.json")) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| PlatformError::Io(format!("_applied.json: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Applied::default(),
            Err(e) => return Err(PlatformError::Io(e.to_string())),
        };
        let store = Self { root, applied: Mutex::new(applied), tables: RwLock::new(HashMap::new()) };
        for name in store.list_tables()? {
            if let Ok(ds) = Dataset::open(&store.uri(&name)).await {
                store.tables.write().await.insert(name, Arc::new(ds));
            }
        }
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

    /// Rows of `table` matching `filter` (Lance SQL), at most `limit`.
    pub async fn query(&self, table: &str, filter: Option<&str>, limit: usize) -> PlatformResult<Vec<RecordBatch>> {
        let ds = self.dataset(table).await.ok_or_else(|| PlatformError::NoTable(table.to_string()))?;
        let mut scan = ds.scan();
        if let Some(f) = filter.filter(|f| !f.trim().is_empty()) {
            scan.filter(f).map_err(lance)?;
        }
        scan.limit(Some(limit as i64), None).map_err(lance)?;
        scan.try_into_stream().await.map_err(lance)?.try_collect().await.map_err(lance)
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
        match effect {
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

    /// Apply a committed record's platform effects (in log order; a record at or
    /// below the applied offset is skipped). Returns whether it was new.
    pub async fn apply(&self, record: &LogRecord) -> PlatformResult<bool> {
        let mut applied = self.applied.lock().await;
        if record.offset.0 <= applied.offset {
            return Ok(false);
        }
        for effect in &record.effects {
            self.apply_effect(&mut applied, effect).await?;
        }
        applied.offset = record.offset.0;
        self.save(&applied)?;
        Ok(true)
    }

    async fn apply_effect(&self, applied: &mut Applied, effect: &Effect) -> PlatformResult<()> {
        let table = match effect {
            Effect::TableCreate { table, .. }
            | Effect::TableInsert { table, .. }
            | Effect::TableUpdate { table, .. }
            | Effect::TableDelete { table, .. }
            | Effect::TableReplace { table, .. } => table.clone(),
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

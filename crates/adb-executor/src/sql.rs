//! Read-only SQL over everything ADB keeps in Lance — the superadmin's debug view
//! (A3T Query Console, SQL pill), over Arrow Flight only. Flows and agents use AQL.
//!
//! | Table | What |
//! |---|---|
//! | `memory_raw` | the memory table as stored: every version, edges, tombstones, `_meta`; `value` is JSONB |
//! | `memory` | the same rows, readable: `value` as JSON text, `sealed` / `embedding` as booleans |
//! | `memory_current` | `memory`'s current rows (not superseded, forgotten or expired; no `_meta`, no tombstone-log entries) |
//! | `tombstone_log` | every FORGET and expiry, as the tombstone log records it (`value`: event, lineage, memory, key, namespace, rows, time, offset) |
//! | `<table>` | each platform table (`flowr_workflows`, `users`, …), as stored |
//!
//! DDL, DML and statements (`SET`, transactions) are refused; results stop at a row
//! limit and a timeout. `information_schema` lists the tables (`SHOW TABLES`).

use std::sync::Arc;
use std::time::Duration;

use adb_platform::PlatformStore;
use adb_store::lance::datafusion::LanceTableProvider;
use adb_store::lance::Dataset;
use adb_store::PartitionStore;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;

/// Rows a query returns when the caller asks for none.
pub const DEFAULT_LIMIT: usize = 1000;
/// Most rows a query may return.
pub const MAX_LIMIT: usize = 10_000;
/// Longest a query may run.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// A query's columns and rows.
pub struct SqlResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Run `sql` over the memory table (if any) and the platform tables (if any).
pub async fn run(
    store: Option<Arc<PartitionStore>>,
    platform: Option<Arc<PlatformStore>>,
    sql: &str,
    limit: Option<usize>,
) -> Result<SqlResult, String> {
    let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let config = SessionConfig::new().with_information_schema(true);
    let has_memory = store.is_some();
    let session = |ctx: SessionContext| async move {
        if has_memory {
            memory_views(&ctx).await?;
        }
        match &platform {
            Some(p) => {
                p.with_tables(|tables| async move {
                    for (name, ds) in tables {
                        register(&ctx, &name, ds)?;
                    }
                    execute(&ctx, sql, limit).await
                })
                .await
            }
            None => execute(&ctx, sql, limit).await,
        }
    };
    let run = async {
        match &store {
            Some(s) => s.query_in(config, session).await.map_err(|e| e.to_string()),
            None => session(SessionContext::new_with_config(config)).await.map_err(|e| e.to_string()),
        }
    };
    tokio::time::timeout(TIMEOUT, run)
        .await
        .map_err(|_| format!("query stopped after {} s", TIMEOUT.as_secs()))?
}

fn register(ctx: &SessionContext, name: &str, ds: Arc<Dataset>) -> DfResult<()> {
    // A bare reference: table names aren't parsed as SQL (case and `-` kept).
    let name = TableReference::bare(name.to_string());
    if ctx.table_exist(name.clone())? {
        return Ok(());
    }
    ctx.register_table(name, Arc::new(LanceTableProvider::new(ds, false, false)))?;
    Ok(())
}

/// `memory` (registered by the store) becomes `memory_raw`; `memory` and
/// `memory_current` are views over it with readable columns.
async fn memory_views(ctx: &SessionContext) -> DfResult<()> {
    let Some(raw) = ctx.deregister_table("memory")? else { return Ok(()) };
    let columns: Vec<String> = raw
        .schema()
        .fields()
        .iter()
        .map(|f| match f.name().as_str() {
            "value" => "adb_json(value) AS value".to_string(),
            "sealed" => "sealed IS NOT NULL AS sealed".to_string(),
            "embedding" => "embedding IS NOT NULL AS has_embedding".to_string(),
            n => format!("\"{n}\""),
        })
        .collect();
    ctx.register_table("memory_raw", raw)?;
    ctx.sql(&format!("CREATE VIEW memory AS SELECT {} FROM memory_raw", columns.join(", "))).await?;
    ctx.sql(&format!(
        "CREATE VIEW memory_current AS SELECT * FROM memory WHERE NOT tombstone AND superseded_by IS NULL \
         AND (valid_to IS NULL OR valid_to > now()) AND (ttl_at IS NULL OR ttl_at > now()) \
         AND id <> '{}' AND kind <> 'meta' AND kind <> 'tomb_log'",
        adb_store::META_ID
    ))
    .await?;
    ctx.sql("CREATE VIEW tombstone_log AS SELECT * FROM memory WHERE kind = 'tomb_log'").await?;
    Ok(())
}

async fn execute(ctx: &SessionContext, sql: &str, limit: usize) -> DfResult<SqlResult> {
    let read_only = SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false);
    let df = ctx.sql_with_options(sql, read_only).await?.limit(0, Some(limit))?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    Ok(SqlResult { schema, batches })
}

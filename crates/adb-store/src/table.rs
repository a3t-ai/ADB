//! The memory table as a DataFusion table (MVP W7, `W7_QUERY_ENGINE.md`): one
//! snapshot of the LSM — base table, flushed generations and memtables, newest
//! version per id — that a query plan scans with its filters and projection pushed
//! into the LSM scanner.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use lance::dataset::mem_wal::scanner::{DatasetCache, InMemoryMemTables, LsmScanner, ShardSnapshot};
use lance::dataset::Dataset;
use uuid::Uuid;

/// One read's view of the partition's LSM.
#[derive(Clone)]
pub struct LsmTable {
    pub(crate) base: Arc<Dataset>,
    pub(crate) snapshot: ShardSnapshot,
    pub(crate) shard: Uuid,
    pub(crate) memtables: InMemoryMemTables,
    pub(crate) cache: Arc<dyn DatasetCache>,
    pub(crate) schema: SchemaRef,
}

impl std::fmt::Debug for LsmTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmTable").field("shard", &self.shard).finish()
    }
}

fn df(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(e.to_string().into())
}

impl LsmTable {
    pub(crate) fn scanner(&self) -> LsmScanner {
        LsmScanner::new(self.base.clone(), vec![self.snapshot.clone()], vec!["id".into()])
            .with_in_memory_memtables(self.shard, self.memtables.clone())
            .with_sstable_cache(self.cache.clone())
    }
}

#[async_trait]
impl TableProvider for LsmTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Filters run in the LSM scanner after it picks the newest version of each row;
    /// DataFusion applies them again (cheap) so the pushdown can stay a superset.
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(&self, _state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], _limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut scanner = self.scanner();
        if let Some(p) = projection {
            let names: Vec<&str> = p.iter().map(|i| self.schema.field(*i).name().as_str()).collect();
            scanner = scanner.project(&names).map_err(df)?;
        }
        if let Some(f) = conjunction(filters.iter().cloned()) {
            scanner = scanner.filter_expr(f);
        }
        let plan = scanner.create_plan().await.map_err(df)?;
        Ok(Arc::new(LsmExec::new(plan)))
    }
}

/// The LSM scanner's plan as one leaf: DataFusion's physical optimizer must not
/// rewrite it (its sources' statistics don't merge — `EnforceDistribution` fails on
/// them); it runs as Lance runs it.
#[derive(Debug)]
struct LsmExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl LsmExec {
    fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(inner.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { inner, properties: Arc::new(properties) }
    }
}

impl DisplayAs for LsmExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LsmExec")
    }
}

impl ExecutionPlan for LsmExec {
    fn name(&self) -> &str {
        "LsmExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        execute_stream(self.inner.clone(), context)
    }
}

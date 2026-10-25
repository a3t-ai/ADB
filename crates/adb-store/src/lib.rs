//! ADB storage layer: one Lance table and one MemWAL shard per partition, written by
//! the applier from the partition log (ARCHITECTURE.md §9.2–9.3, D2_TEMPORAL.md,
//! D3_SCHEMA.md; DELIVERY_PLAN.md stage D).

pub mod filter;
pub mod rows;
pub mod schema;
pub mod store;

pub use datafusion::logical_expr::Expr;
pub use filter::predicate_filter;
pub use lance;
pub use rows::{key_of, qualified, row_id, StoredRow};
pub use schema::{memory_schema, META_ID, RECORD_KINDS};
pub use store::{configure_lance_runtime, sql_str, PartitionStore, StoreConfig};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("lance: {0}")]
    Lance(String),
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

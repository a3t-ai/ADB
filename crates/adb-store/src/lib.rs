//! ADB storage layer: one Lance table and one MemWAL shard per partition, written by
//! the applier from the partition log (ARCHITECTURE.md §9.2–9.3, D2_TEMPORAL.md,
//! D3_SCHEMA.md; DELIVERY_PLAN.md stage D).

pub mod field_index;
pub mod filter;
pub mod hot;
pub mod live_index;
pub mod recent_index;
pub mod row_cache;
pub mod rows;
pub mod schema;
pub mod store;
pub mod table;
pub mod vectors;

pub use datafusion::logical_expr::Expr;
pub use field_index::{FieldIndex, Lookup};
pub use filter::predicate_filter;
pub use hot::{CacheConfig, CacheStats, Cached, Hit, HotCache, Keep, Rule, Shape};
pub use lance;
pub mod ostore;
pub mod budget;
pub use rows::{decode, key_of, LiveKey, namespace_of, namespaced, qualified, row_id, scoped_id, space_id, unscoped, valid_namespace, without_namespace, StoredRow, DEFAULT_SCOPE, NAMESPACE_SEP, SCOPE_SEP};
pub use schema::{memory_schema, META_ID, RECORD_KINDS};
pub use store::{configure_lance_runtime, sql_str, archive_tree, restore_table, restore_tree, table_meta_offset, tree_archived, ArchivePending, ArchivePolicy, ArchiveStats, CompactStats, PartitionStore, StoreConfig, TierStats, TombEntry, COLD_BASE_ID, DEFAULT_EMBEDDING_DIM};

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

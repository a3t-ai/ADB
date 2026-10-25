//! The memory table's Arrow schema (D3_SCHEMA.md §1).
//!
//! Two simplifications against D3, both storage-level only:
//! - enum columns (`visibility`, `memory`, `kind`) are plain `Utf8`; Lance's file
//!   format dictionary-encodes low-cardinality strings itself;
//! - `provenance` is three flat columns (`provenance_agent`, `provenance_source`,
//!   `provenance_txn`) instead of a struct, which keeps MemWAL / LSM scans simple.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// Id of the control row (D3 `kind = meta`).
pub const META_ID: &str = "_meta";
/// Schema version written into `_meta` (D3 §4).
pub const SCHEMA_VERSION: u32 = 1;

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// The memory table schema for embeddings of `dim` dimensions (D3 S5).
pub fn memory_schema(dim: i32) -> SchemaRef {
    // `id` is the unenforced primary key: a MemWAL put with an existing id replaces it,
    // and LSM reads return the newest row per id.
    let id = Field::new("id", DataType::Utf8, false)
        .with_metadata([("lance-schema:unenforced-primary-key".to_string(), "true".to_string())].into());
    Arc::new(Schema::new(vec![
        id,
        Field::new("lineage_id", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("visibility", DataType::Utf8, false),
        Field::new("namespace", DataType::Utf8, true),
        Field::new("memory", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, true),
        Field::new("subject_id", DataType::Utf8, true),
        Field::new("predicate", DataType::Utf8, true),
        Field::new("object_id", DataType::Utf8, true),
        Field::new("link_type", DataType::Utf8, true),
        Field::new("weight", DataType::Float32, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("sealed", DataType::LargeBinary, true),
        Field::new("text", DataType::Utf8, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        ),
        Field::new("commit_ts", ts(), false),
        Field::new("valid_from", ts(), false),
        Field::new("valid_to", ts(), true),
        Field::new("superseded_by", DataType::Utf8, true),
        Field::new("version", DataType::UInt32, false),
        Field::new("ttl_at", ts(), true),
        Field::new("tombstone", DataType::Boolean, false),
        Field::new("provenance_agent", DataType::Utf8, true),
        Field::new("provenance_source", DataType::Utf8, true),
        Field::new("provenance_txn", DataType::Utf8, true),
        Field::new("log_offset", DataType::UInt64, false),
        Field::new("epoch", DataType::UInt64, false),
    ]))
}

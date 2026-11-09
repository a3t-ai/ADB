//! ADB Core - Core types and traits for Agent Database
//!
//! This crate provides the foundational types used throughout ADB:
//! - Memory types and records
//! - Backend trait definitions
//! - Link/ontology types
//! - Error types
//! - Scope and namespace handling
//! - Log records and effects (`log`)

pub mod metrics;
pub mod codes;
pub mod disk;
pub mod error;
pub mod link;
pub mod log;
pub mod memory;
pub mod predicate;
pub mod scope;
pub mod time;

// Re-exports for convenience
pub use error::{AdbError, AdbResult};
pub use link::{Link, LinkPredicate};
pub use memory::{merge_fields, MemoryRecord, MemoryType, Metadata};
pub use predicate::{evaluate_conditions, get_nested_field, evaluate_conditions_on_record, AggregateFunc, AggregateFuncType, Condition, FollowLinks, LinkDirection, LinkSelect, LogicalOp, Modifiers, Operator, OrderBy, Predicate, TimePoint, Value, Window, WithLinks};
pub use scope::{Namespace, Scope};
pub use time::Ttl;

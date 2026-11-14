//! ADB Executor - Query execution engine
//!
//! Executes planned AQL against the partition log and its Lance table.

// Lance 14's async types nest deeper than the default limit allows (H-0).
#![recursion_limit = "256"]

pub mod engine;
pub mod engine_store;
mod error;
mod executor;
mod result;
pub mod apply;
pub mod query;
pub mod reads;
pub mod rank;
pub mod sql;
pub mod writes;
pub use adb_core::steps;
pub mod replica;
pub mod follower;
pub mod branches;
pub mod branch_engine;
pub mod merge;
pub mod restore;
pub mod partitioned;
pub mod spaces;

pub use error::{ExecutorError, ExecutorResult};
pub use executor::{Executor, MAX_DEPTH, MAX_REACHED};
pub use engine::{request_restart, restart_requested, Engine, LocalEngine};
pub use result::{QueryResult, ResultSet};
pub use apply::ApplyBarrier;
pub use reads::{LanceReads, Reads};
pub use writes::{DurableWrites, ReplayStats, Writes, COMPACT_AFTER_WRITES};
pub use replica::Replica;
pub use follower::{follower_reads, FollowerReads, NOT_LEADER};
pub use engine::{EngineLog, Warm};

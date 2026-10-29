//! ADB Executor - Query execution engine
//!
//! Executes planned AQL against the partition log and its Lance table.

pub mod engine;
mod error;
mod executor;
mod result;
pub mod apply;
pub mod query;
pub mod reads;
pub mod rank;
pub mod sql;
pub mod writes;

pub use error::{ExecutorError, ExecutorResult};
pub use executor::Executor;
pub use engine::{request_restart, restart_requested, Engine, LocalEngine};
pub use result::{QueryResult, ResultSet};
pub use apply::ApplyBarrier;
pub use reads::{LanceReads, Reads};
pub use writes::{DurableWrites, ReplayStats, Writes};

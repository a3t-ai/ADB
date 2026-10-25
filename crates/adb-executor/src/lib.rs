//! ADB Executor - Query execution engine
//!
//! Executes planned queries against ADB backends.

pub mod engine;
mod error;
mod executor;
mod result;
pub mod apply;
pub mod reads;
pub mod writes;

pub use error::{ExecutorError, ExecutorResult};
pub use executor::Executor;
pub use engine::{Engine, LocalEngine};
pub use result::{QueryResult, ResultSet};
pub use apply::ApplyBarrier;
pub use reads::{DirectReads, LanceReads, Reads};
pub use writes::{DirectWrites, DurableWrites, ReplayStats, Writes};

//! ADB Executor - Query execution engine
//!
//! Executes planned queries against ADB backends.

pub mod engine;
mod error;
mod executor;
mod result;

pub use error::{ExecutorError, ExecutorResult};
pub use executor::Executor;
pub use engine::{Engine, LocalEngine};
pub use result::{QueryResult, ResultSet};

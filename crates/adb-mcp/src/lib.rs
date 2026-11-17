//! ADB MCP server — Model Context Protocol interface to agent memory.
//!
//! Transport-independent: [`McpServer::handle`] serves stdio (`adb serve`), streamable
//! HTTP and SSE (mounted by `adb-server`).
//!
//! Tools: `aql_query`, `store_memory`, `recall_memory`, `get_stats` (ClawDB-compatible,
//! used by A3T agents) and `remember`, `recall`, `forget`, `link` (for LLMs that should
//! not need AQL). Resources: a memory guide and the AQL grammar. Prompt:
//! `memory_smoke_test`.

pub mod protocol;
pub mod server;
pub mod tools;

pub use server::McpServer;

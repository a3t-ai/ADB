//! ADB storage layer (skeleton).
//!
//! One Lance table and one MemWAL shard per partition, written by the applier
//! (ARCHITECTURE.md §9.2–9.3, DELIVERY_PLAN.md M1 `adb-store`). For now the crate
//! only pins Lance into the workspace so every crate builds against one Arrow
//! version (K5).

pub use lance;

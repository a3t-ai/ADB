//! ADB's control plane (S3 option B, `.ai/S3_OPTION_B_PLAN.md` §3.0, step 2): a
//! small Raft group holding which node owns each partition, which node holds its
//! WAL replica, and the partition's epoch. Nothing on the data path goes through it.
//!
//! - [`map`]: the [`map::PartitionMap`], the commands that change it, and the planner
//!   that repairs it when nodes come and go.
//! - [`leases`]: who is live, from heartbeats (the control leader's memory only).
//! - [`raft`]: the group's log, state machine and RPCs (openraft).
//! - [`control`]: the voters' service and the client every node runs.

pub mod control;
pub mod leases;
pub mod map;
pub mod raft;

pub use control::{ControlClient, ControlNode};
pub use map::{Assignment, ControlCmd, NodeId, Partition, PartitionMap};

//! Node leases, held by the control leader in memory only (never in the Raft log).
//!
//! Every node heartbeats the control leader every `ADB_HEARTBEAT_MS` (default 500)
//! with its load. A node unheard for `ADB_LEASE_MS` (default 3000) is no longer
//! live, and the planner moves its partitions. A leader that has just started gives
//! every member a full lease first: it does not know when they were last heard, and
//! a control-leader change must not look like every node failing.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::map::NodeId;

/// `ADB_LEASE_MS`: how long a node stays live after its last heartbeat.
pub fn lease() -> Duration {
    Duration::from_millis(std::env::var("ADB_LEASE_MS").ok().and_then(|v| v.parse().ok()).filter(|&v: &u64| v > 0).unwrap_or(3000))
}

/// `ADB_HEARTBEAT_MS`: how often a node heartbeats.
pub fn heartbeat() -> Duration {
    Duration::from_millis(std::env::var("ADB_HEARTBEAT_MS").ok().and_then(|v| v.parse().ok()).filter(|&v: &u64| v > 0).unwrap_or(500))
}

#[derive(Debug, Clone, Copy)]
struct Seen {
    at: Instant,
    load: f64,
}

/// The leader's view of who is live.
#[derive(Debug)]
pub struct Leases {
    lease: Duration,
    /// When this leader started: until a lease has passed since, every member counts
    /// as live (it may have heartbeated the old leader just before).
    since: Instant,
    seen: BTreeMap<NodeId, Seen>,
}

impl Leases {
    pub fn new(lease: Duration) -> Self {
        Self::starting_at(lease, Instant::now())
    }

    fn starting_at(lease: Duration, since: Instant) -> Self {
        Self { lease, since, seen: BTreeMap::new() }
    }

    /// A heartbeat from `node` reporting `load`.
    pub fn heard(&mut self, node: NodeId, load: f64) {
        self.heard_at(node, load, Instant::now());
    }

    fn heard_at(&mut self, node: NodeId, load: f64, at: Instant) {
        self.seen.insert(node, Seen { at, load });
    }

    /// The members live now.
    pub fn live(&self, members: impl IntoIterator<Item = NodeId>) -> BTreeSet<NodeId> {
        self.live_at(members, Instant::now())
    }

    fn live_at(&self, members: impl IntoIterator<Item = NodeId>, now: Instant) -> BTreeSet<NodeId> {
        let grace = now.duration_since(self.since) < self.lease;
        members
            .into_iter()
            .filter(|n| match self.seen.get(n) {
                Some(s) => now.duration_since(s.at) < self.lease,
                None => grace,
            })
            .collect()
    }

    /// Each node's last reported load.
    pub fn reported(&self) -> BTreeMap<NodeId, f64> {
        self.seen.iter().map(|(n, s)| (*n, s.load)).collect()
    }

    /// Whether the start-up grace is over (the planner may move partitions off a node
    /// it never heard from).
    pub fn settled(&self) -> bool {
        self.since.elapsed() >= self.lease
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_leader_counts_every_member_live_for_one_lease() {
        let t0 = Instant::now();
        let lease = Duration::from_secs(3);
        let mut l = Leases::starting_at(lease, t0);
        assert_eq!(l.live_at([1, 2, 3], t0 + Duration::from_secs(1)), [1, 2, 3].into(), "grace");
        l.heard_at(1, 0.5, t0 + Duration::from_secs(2));
        l.heard_at(2, 0.1, t0 + Duration::from_secs(2));
        // After the grace: 3 was never heard; 1 and 2 are within their lease.
        assert_eq!(l.live_at([1, 2, 3], t0 + Duration::from_secs(4)), [1, 2].into());
        // 2 keeps heartbeating, 1 stops.
        l.heard_at(2, 0.1, t0 + Duration::from_secs(5));
        assert_eq!(l.live_at([1, 2, 3], t0 + Duration::from_millis(5500)), [2].into());
        assert_eq!(l.reported()[&1], 0.5);
    }
}

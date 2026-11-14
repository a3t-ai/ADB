//! The ownership map and the planner that keeps it whole (S3 option B §3.0, step 2).
//!
//! The control plane's Raft group replicates only [`ControlCmd`]s — a node joining,
//! a partition (re)assigned. Applying them in order gives every member the same
//! [`PartitionMap`]: for each partition its owner, its WAL replica and its epoch.
//! Heartbeats and load never enter the log: the control leader keeps them in memory
//! ([`crate::leases::Leases`]) and, from them, the [`plan`] proposes the commands that
//! repair the map.
//!
//! - **Epoch.** Raised whenever the owner changes. The new owner records it on object
//!   storage before taking writes (`epochs/{epoch}.json`), the replica refuses
//!   appends of an older epoch, and object keys carry it.
//! - **Owner lost.** Its replica, which holds every acked record not yet on object
//!   storage, becomes the owner (epoch + 1) and a new replica is chosen.
//! - **Replica lost.** A new replica is chosen; the owner sends it what object
//!   storage lacks. The epoch stays.
//! - **Both lost.** Any live node takes the partition over from object storage alone
//!   (epoch + 1); acked records not yet shipped (at most `ADB_WAL_SEGMENT_MS`) are
//!   lost, and the plan says so ([`Change::reason`]).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Partition = u16;

/// Where a partition lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub owner: NodeId,
    /// `None` while no second node is live.
    pub replica: Option<NodeId>,
    pub epoch: u64,
}

/// A node of the cluster, as it joined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// Base URL of its data plane (owner → replica appends, gateway forwarding).
    pub addr: String,
}

/// What the control plane's Raft log holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCmd {
    /// Fix the partition count (once; later ones must match).
    Init { partitions: Partition },
    /// A node joins (or rejoins, perhaps at a new address).
    Join { node: NodeId, addr: String },
    /// A node is removed for good.
    Leave { node: NodeId },
    /// Set where a partition lives. Refused unless `epoch` is the partition's current
    /// epoch (replica change) or the next one (owner change).
    Assign { partition: Partition, assignment: Assignment },
    /// Place the namespaces matching `pattern` (exact, or a prefix `…*`) in
    /// `partition` instead of by hash ([`crate::routing`]). Moves no rows: set it
    /// before a matching namespace holds data (the caller checks the catalog).
    Pin { pattern: String, partition: Partition },
    /// Remove a pin (same caveat).
    Unpin { pattern: String },
}

/// The map every control-plane member builds by applying [`ControlCmd`]s.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMap {
    pub partitions: Partition,
    pub members: BTreeMap<NodeId, Member>,
    pub assignments: BTreeMap<Partition, Assignment>,
    /// Namespace placement overrides: pattern → memory partition.
    #[serde(default)]
    pub pins: BTreeMap<String, Partition>,
    /// Raised by every applied command: who holds the map can tell it is behind.
    pub version: u64,
}

/// Why a command was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum Refused {
    #[error("the cluster has {have} partitions; {asked} asked")]
    PartitionCount { have: Partition, asked: Partition },
    #[error("partition {0} does not exist")]
    NoPartition(Partition),
    #[error("node {0} is not a member")]
    NotMember(NodeId),
    #[error("partition {partition}: epoch {asked} is neither the current ({current}) nor the next")]
    Epoch { partition: Partition, current: u64, asked: u64 },
    #[error("partition {partition}: a new owner needs a new epoch ({current} + 1)")]
    OwnerWithoutEpoch { partition: Partition, current: u64 },
    #[error("partition {0}: owner and replica are the same node")]
    SameNode(Partition),
    #[error("pin {0:?}: a pattern is a namespace or a prefix ending in `*`")]
    BadPattern(String),
    #[error("no pin {0:?}")]
    NoPin(String),
}

impl PartitionMap {
    /// Apply one command (every member does, in log order). A refused command
    /// changes nothing.
    pub fn apply(&mut self, cmd: &ControlCmd) -> Result<(), Refused> {
        match cmd {
            ControlCmd::Init { partitions } => {
                if self.partitions != 0 && self.partitions != *partitions {
                    return Err(Refused::PartitionCount { have: self.partitions, asked: *partitions });
                }
                self.partitions = *partitions;
            }
            ControlCmd::Join { node, addr } => {
                self.members.insert(*node, Member { addr: addr.clone() });
            }
            ControlCmd::Leave { node } => {
                if self.members.remove(node).is_none() {
                    return Err(Refused::NotMember(*node));
                }
            }
            ControlCmd::Pin { pattern, partition } => {
                if pattern.is_empty() || pattern[..pattern.len() - 1].contains('*') {
                    return Err(Refused::BadPattern(pattern.clone()));
                }
                if *partition >= self.partitions {
                    return Err(Refused::NoPartition(*partition));
                }
                self.pins.insert(pattern.clone(), *partition);
            }
            ControlCmd::Unpin { pattern } => {
                if self.pins.remove(pattern).is_none() {
                    return Err(Refused::NoPin(pattern.clone()));
                }
            }
            ControlCmd::Assign { partition, assignment } => {
                if *partition > self.partitions {
                    return Err(Refused::NoPartition(*partition));
                }
                for n in std::iter::once(assignment.owner).chain(assignment.replica) {
                    if !self.members.contains_key(&n) {
                        return Err(Refused::NotMember(n));
                    }
                }
                if assignment.replica == Some(assignment.owner) {
                    return Err(Refused::SameNode(*partition));
                }
                if let Some(cur) = self.assignments.get(partition) {
                    if assignment.epoch != cur.epoch && assignment.epoch != cur.epoch + 1 {
                        return Err(Refused::Epoch { partition: *partition, current: cur.epoch, asked: assignment.epoch });
                    }
                    if assignment.owner != cur.owner && assignment.epoch == cur.epoch {
                        return Err(Refused::OwnerWithoutEpoch { partition: *partition, current: cur.epoch });
                    }
                } else if assignment.epoch > 1 {
                    return Err(Refused::Epoch { partition: *partition, current: 0, asked: assignment.epoch });
                }
                self.assignments.insert(*partition, assignment.clone());
            }
        }
        self.version += 1;
        Ok(())
    }

    /// The partitions `node` owns.
    pub fn owned_by(&self, node: NodeId) -> impl Iterator<Item = (Partition, &Assignment)> {
        self.assignments.iter().filter(move |(_, a)| a.owner == node).map(|(p, a)| (*p, a))
    }

    /// The partitions `node` is the replica of.
    pub fn replicated_by(&self, node: NodeId) -> impl Iterator<Item = (Partition, &Assignment)> {
        self.assignments.iter().filter(move |(_, a)| a.replica == Some(node)).map(|(p, a)| (*p, a))
    }

    /// Every partition that needs an owner: the memory partitions `0..partitions`, then
    /// the system partition (`partitions`).
    pub fn all_partitions(&self) -> std::ops::RangeInclusive<Partition> {
        0..=self.partitions
    }
}

/// One change the planner proposes, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub cmd: ControlCmd,
    pub reason: &'static str,
}

/// The commands that repair `map` given the live nodes and their reported load:
/// every partition with a live owner and, when two nodes are live, a live replica on
/// another node. Owners go to the node owning fewest partitions, replicas to the one
/// replicating fewest (owners carry the writes; a replica holds memory only); ties by
/// reported load, then node id. Proposes nothing when the map is whole.
pub fn plan(map: &PartitionMap, live: &BTreeSet<NodeId>, reported: &BTreeMap<NodeId, f64>) -> Vec<Change> {
    let live: BTreeSet<NodeId> = live.iter().copied().filter(|n| map.members.contains_key(n)).collect();
    if live.is_empty() {
        return Vec::new();
    }
    // (owned, replicated) per live node, as the map will be once the changes so far apply.
    let mut count: BTreeMap<NodeId, (u64, u64)> = live.iter().map(|n| (*n, (0, 0))).collect();
    for a in map.assignments.values() {
        if let Some(c) = count.get_mut(&a.owner) {
            c.0 += 1;
        }
        if let Some(c) = a.replica.and_then(|r| count.get_mut(&r)) {
            c.1 += 1;
        }
    }
    let load = |n: &NodeId| reported.get(n).copied().unwrap_or(0.0);
    let pick = |count: &BTreeMap<NodeId, (u64, u64)>, as_owner: bool, not: Option<NodeId>| -> Option<NodeId> {
        count
            .iter()
            .filter(|(n, _)| Some(**n) != not)
            .min_by(|(a, ca), (b, cb)| {
                let (ka, kb) = if as_owner { ((ca.0, ca.1), (cb.0, cb.1)) } else { ((ca.1, ca.0), (cb.1, cb.0)) };
                ka.cmp(&kb).then(load(a).partial_cmp(&load(b)).unwrap_or(std::cmp::Ordering::Equal)).then(a.cmp(b))
            })
            .map(|(n, _)| *n)
    };
    let mut out = Vec::new();
    if map.partitions == 0 {
        return out;
    }
    for p in map.all_partitions() {
        let cur = map.assignments.get(&p);
        let place_replica = |count: &mut BTreeMap<NodeId, (u64, u64)>, owner: NodeId| {
            let r = pick(count, false, Some(owner));
            if let Some(r) = r {
                count.get_mut(&r).unwrap().1 += 1;
            }
            r
        };
        let (assignment, reason) = match cur {
            None => {
                let Some(owner) = pick(&count, true, None) else { continue };
                count.get_mut(&owner).unwrap().0 += 1;
                let replica = place_replica(&mut count, owner);
                (Assignment { owner, replica, epoch: 1 }, "new partition")
            }
            Some(a) => {
                let owner_live = live.contains(&a.owner);
                let replica_live = a.replica.is_some_and(|r| live.contains(&r));
                match (owner_live, replica_live) {
                    (true, true) => continue,
                    (true, false) => {
                        let Some(r) = place_replica(&mut count, a.owner) else { continue };
                        (Assignment { owner: a.owner, replica: Some(r), epoch: a.epoch }, if a.replica.is_some() { "replica lost: a new replica" } else { "a replica, now that a second node is live" })
                    }
                    (false, true) => {
                        let owner = a.replica.unwrap();
                        let c = count.get_mut(&owner).unwrap();
                        c.0 += 1;
                        c.1 -= 1;
                        let replica = place_replica(&mut count, owner);
                        (Assignment { owner, replica, epoch: a.epoch + 1 }, "owner lost: its replica takes over")
                    }
                    (false, false) => {
                        let Some(owner) = pick(&count, true, None) else { continue };
                        count.get_mut(&owner).unwrap().0 += 1;
                        let replica = place_replica(&mut count, owner);
                        (
                            Assignment { owner, replica, epoch: a.epoch + 1 },
                            "owner and replica lost: taken over from object storage alone (acked records not yet shipped are lost)",
                        )
                    }
                }
            }
        };
        out.push(Change { cmd: ControlCmd::Assign { partition: p, assignment }, reason });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(nodes: &[NodeId], partitions: Partition) -> PartitionMap {
        let mut m = PartitionMap::default();
        m.apply(&ControlCmd::Init { partitions }).unwrap();
        for n in nodes {
            m.apply(&ControlCmd::Join { node: *n, addr: format!("http://n{n}") }).unwrap();
        }
        m
    }

    fn live(nodes: &[NodeId]) -> BTreeSet<NodeId> {
        nodes.iter().copied().collect()
    }

    fn apply_all(m: &mut PartitionMap, changes: &[Change]) {
        for c in changes {
            m.apply(&c.cmd).unwrap_or_else(|e| panic!("{:?}: {e}", c.cmd));
        }
    }

    #[test]
    fn new_partitions_spread_with_a_replica_elsewhere() {
        // 5 memory partitions + the system partition: 6 to place.
        let mut m = map_with(&[1, 2, 3], 5);
        let changes = plan(&m, &live(&[1, 2, 3]), &BTreeMap::new());
        assert_eq!(changes.len(), 6);
        apply_all(&mut m, &changes);
        for n in [1, 2, 3] {
            assert_eq!(m.owned_by(n).count(), 2, "even spread: {m:?}");
        }
        for a in m.assignments.values() {
            assert!(a.replica.is_some() && a.replica != Some(a.owner));
            assert_eq!(a.epoch, 1);
        }
        assert!(plan(&m, &live(&[1, 2, 3]), &BTreeMap::new()).is_empty(), "whole: nothing to do");
    }

    #[test]
    fn owner_lost_its_replica_takes_over_at_the_next_epoch() {
        let mut m = map_with(&[1, 2, 3], 3);
        let c = plan(&m, &live(&[1, 2, 3]), &BTreeMap::new());
        apply_all(&mut m, &c);
        let before = m.clone();
        let changes = plan(&m, &live(&[2, 3]), &BTreeMap::new());
        apply_all(&mut m, &changes);
        for (p, a) in &before.assignments {
            let now = &m.assignments[p];
            if a.owner == 1 {
                assert_eq!(now.owner, a.replica.unwrap(), "the replica takes over");
                assert_eq!(now.epoch, a.epoch + 1);
                assert!(now.replica.is_some() && now.replica != Some(1));
            } else if a.replica == Some(1) {
                assert_eq!((now.owner, now.epoch), (a.owner, a.epoch), "replica change keeps the epoch");
                assert!(now.replica.is_some() && now.replica != Some(1));
            } else {
                assert_eq!(now, a);
            }
        }
    }

    #[test]
    fn both_lost_any_live_node_takes_over_and_says_what_is_lost() {
        let mut m = map_with(&[1, 2, 3], 1);
        for p in m.all_partitions() {
            m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner: 1, replica: Some(2), epoch: 1 } }).unwrap();
        }
        let changes = plan(&m, &live(&[3]), &BTreeMap::new());
        assert_eq!(changes.len(), 2, "the memory partition and the system partition");
        assert!(changes.iter().all(|c| c.reason.contains("lost")));
        apply_all(&mut m, &changes);
        assert_eq!(m.assignments[&0], Assignment { owner: 3, replica: None, epoch: 2 });
        assert_eq!(m.assignments[&1], Assignment { owner: 3, replica: None, epoch: 2 });
    }

    #[test]
    fn the_map_refuses_unsafe_assignments() {
        let mut m = map_with(&[1, 2], 1);
        m.apply(&ControlCmd::Assign { partition: 0, assignment: Assignment { owner: 1, replica: Some(2), epoch: 1 } }).unwrap();
        // A new owner at the same epoch: two owners could write one epoch.
        assert!(matches!(m.apply(&ControlCmd::Assign { partition: 0, assignment: Assignment { owner: 2, replica: None, epoch: 1 } }), Err(Refused::OwnerWithoutEpoch { .. })));
        assert!(matches!(m.apply(&ControlCmd::Assign { partition: 0, assignment: Assignment { owner: 2, replica: None, epoch: 5 } }), Err(Refused::Epoch { .. })));
        assert!(matches!(m.apply(&ControlCmd::Assign { partition: 0, assignment: Assignment { owner: 2, replica: Some(2), epoch: 2 } }), Err(Refused::SameNode(0))));
        assert!(matches!(m.apply(&ControlCmd::Assign { partition: 0, assignment: Assignment { owner: 9, replica: None, epoch: 2 } }), Err(Refused::NotMember(9))));
        assert!(matches!(m.apply(&ControlCmd::Init { partitions: 4 }), Err(Refused::PartitionCount { .. })));
        assert!(matches!(m.apply(&ControlCmd::Assign { partition: 2, assignment: Assignment { owner: 1, replica: None, epoch: 1 } }), Err(Refused::NoPartition(2))), "1 memory partition + system (1)");
        assert!(matches!(m.apply(&ControlCmd::Pin { pattern: "a*b*".into(), partition: 0 }), Err(Refused::BadPattern(_))));
        assert!(matches!(m.apply(&ControlCmd::Pin { pattern: "acme--*".into(), partition: 1 }), Err(Refused::NoPartition(1))), "pins go to memory partitions only");
        assert!(matches!(m.apply(&ControlCmd::Unpin { pattern: "x".into() }), Err(Refused::NoPin(_))));
        let v = m.version;
        m.apply(&ControlCmd::Assign { partition: 0, assignment: Assignment { owner: 2, replica: Some(1), epoch: 2 } }).unwrap();
        assert_eq!(m.version, v + 1);
    }

    #[test]
    fn a_second_node_gets_the_replicas() {
        let mut m = map_with(&[1, 2], 2);
        let c = plan(&m, &live(&[1]), &BTreeMap::new());
        apply_all(&mut m, &c);
        assert!(m.assignments.values().all(|a| a.owner == 1 && a.replica.is_none()));
        let c = plan(&m, &live(&[1, 2]), &BTreeMap::new());
        apply_all(&mut m, &c);
        assert!(m.assignments.values().all(|a| a.replica == Some(2) && a.epoch == 1));
    }
}

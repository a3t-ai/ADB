//! Which partitions a statement runs on (S3 option B, step 3).
//!
//! Responsibilities (user, 14 Nov): A3T/FlowR builds the scopes — namespace names,
//! unique across orgs, and the tokens that carry them. ADB stores each namespace
//! where any scope can be queried, and never lets a call scoped to X reach outside X.
//! ADB treats a namespace as an opaque string.
//!
//! - **Placement.** A namespace lives in exactly one partition: a pin for it (exact,
//!   or the longest matching prefix pattern such as `acme--*`) if there is one, else
//!   `hash(namespace) mod partitions`, with a fixed hash (FNV-1a 64), so every node
//!   and every version agrees. Records without a namespace live under `""`.
//! - **The system partition** (`partitions`, one past the memory partitions) holds
//!   the platform tables, the FlowR engine store and the namespace catalog.
//! - **Scope.** A call carries the namespaces it may reach ([`Reach`]): everything
//!   (a full-access token) or a list of patterns. A statement naming a namespace runs
//!   only if the scope allows it, on that namespace's partition. A statement naming
//!   none runs on the partitions that hold namespaces the scope allows — never all of
//!   them because some might — and each partition filters to the scope again.
//! - **Pins** must be set before a matching namespace holds data: a pin moves no
//!   rows (the caller checks the catalog; see [`crate::map::ControlCmd::Pin`]).

use std::collections::BTreeSet;

use crate::map::{Partition, PartitionMap};

/// FNV-1a 64: stable across platforms, Rust versions and builds.
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Whether `pattern` (exact, or a prefix ending in `*`; `*` alone is everything)
/// matches `namespace`. The same patterns as ADB's scoped tokens (DB-3).
pub fn matches(pattern: &str, namespace: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => namespace.starts_with(prefix),
        None => pattern == namespace,
    }
}

/// Separates a memory branch's name from its namespace: branch `b` of namespace `ns`
/// is the namespace `ns@b`, placed and stored like any namespace
/// (`.ai/BRANCH_NAMESPACES.md`). Reserved for branches.
pub const BRANCH_SEP: char = '@';

/// The partition `namespace` lives in.
pub fn partition_of(map: &PartitionMap, namespace: &str) -> Partition {
    // An exact pin beats any prefix; among prefixes the longest wins.
    if let Some(p) = map.pins.get(namespace) {
        return *p;
    }
    let pinned = map
        .pins
        .iter()
        .filter(|(pat, _)| pat.ends_with('*') && matches(pat, namespace))
        .max_by_key(|(pat, _)| pat.len())
        .map(|(_, p)| *p);
    pinned.unwrap_or_else(|| (fnv1a(namespace) % u64::from(map.partitions.max(1))) as Partition)
}

/// The system partition: platform tables, the FlowR engine store, the catalog.
pub fn system_partition(map: &PartitionMap) -> Partition {
    map.partitions
}

/// The namespaces a call may reach.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Reach {
    /// A full-access token: every namespace.
    All,
    /// Patterns (exact, or prefix `…*`); nothing else.
    Only(Vec<String>),
}

impl Reach {
    pub fn allows(&self, namespace: &str) -> bool {
        match self {
            Reach::All => true,
            Reach::Only(pats) => pats.iter().any(|p| matches(p, namespace)),
        }
    }
}

/// The namespaces that exist (the catalog, in the system partition).
pub trait Catalog {
    /// Every known namespace matching `pattern`.
    fn matching(&self, pattern: &str) -> Vec<String>;
}

impl Catalog for BTreeSet<String> {
    fn matching(&self, pattern: &str) -> Vec<String> {
        match pattern.strip_suffix('*') {
            Some(prefix) => self.range(prefix.to_string()..).take_while(|n| n.starts_with(prefix)).cloned().collect(),
            None => self.get(pattern).cloned().into_iter().collect(),
        }
    }
}

/// A statement refused before it runs anywhere.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutOfReach {
    #[error("namespace {0:?} is outside this call's scope")]
    Namespace(String),
}

/// Where a statement runs: the partitions, and the namespace filter every one of
/// them applies (`None`: no filter, a full-access statement over everything).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub partitions: BTreeSet<Partition>,
    pub filter: Option<Reach>,
}

/// Plan a statement naming `namespace` (or none) for a call that may reach `reach`.
pub fn plan_statement(map: &PartitionMap, catalog: &dyn Catalog, reach: &Reach, namespace: Option<&str>) -> Result<Plan, OutOfReach> {
    if let Some(ns) = namespace {
        if !reach.allows(ns) {
            return Err(OutOfReach::Namespace(ns.to_string()));
        }
        return Ok(Plan { partitions: [partition_of(map, ns)].into(), filter: Some(Reach::Only(vec![ns.to_string()])) });
    }
    match reach {
        Reach::All => Ok(Plan { partitions: (0..map.partitions).collect(), filter: None }),
        Reach::Only(pats) => {
            let mut partitions = BTreeSet::new();
            for pat in pats {
                if pat.ends_with('*') {
                    for ns in catalog.matching(pat) {
                        partitions.insert(partition_of(map, &ns));
                    }
                } else {
                    // An exact namespace: its partition, whether or not it holds data yet.
                    partitions.insert(partition_of(map, pat));
                }
            }
            Ok(Plan { partitions, filter: Some(reach.clone()) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::map::ControlCmd;

    fn map64() -> PartitionMap {
        let mut m = PartitionMap::default();
        m.apply(&ControlCmd::Init { partitions: 64 }).unwrap();
        m
    }

    #[test]
    fn placement_is_stable_and_spread() {
        let m = map64();
        // Fixed: these never change between builds (a change would move data).
        assert_eq!(fnv1a(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(partition_of(&m, "acme--bot"), partition_of(&m, "acme--bot"));
        let used: BTreeSet<Partition> = (0..1000).map(|i| partition_of(&m, &format!("proj{}--agent{i}", i % 7))).collect();
        assert!(used.len() > 60, "1,000 agents spread over most of 64 partitions: {}", used.len());
        assert!(used.iter().all(|p| *p < 64));
        assert_eq!(system_partition(&m), 64);
    }

    #[test]
    fn pins_exact_beats_prefix_longest_prefix_wins() {
        let mut m = map64();
        m.apply(&ControlCmd::Pin { pattern: "acme--*".into(), partition: 5 }).unwrap();
        m.apply(&ControlCmd::Pin { pattern: "acme--big*".into(), partition: 9 }).unwrap();
        m.apply(&ControlCmd::Pin { pattern: "acme--bigbot".into(), partition: 11 }).unwrap();
        assert_eq!(partition_of(&m, "acme--bot"), 5);
        assert_eq!(partition_of(&m, "acme--big-one"), 9);
        assert_eq!(partition_of(&m, "acme--bigbot"), 11);
        assert_ne!(partition_of(&m, "other--bot"), 5, "unpinned: hashed (this one isn't 5)");
    }

    #[test]
    fn a_scope_runs_only_where_its_namespaces_are() {
        let m = map64();
        let catalog: BTreeSet<String> = ["a3t-shared--bot", "a3t-shared--ops", "system-test--x", "demo--y"].into_iter().map(String::from).collect();
        let reach = Reach::Only(vec!["a3t-shared--*".into()]);

        // Naming a namespace: inside the scope → its partition only; outside → refused.
        let p = plan_statement(&m, &catalog, &reach, Some("a3t-shared--bot")).unwrap();
        assert_eq!(p.partitions, [partition_of(&m, "a3t-shared--bot")].into());
        assert_eq!(plan_statement(&m, &catalog, &reach, Some("demo--y")), Err(OutOfReach::Namespace("demo--y".into())));

        // Naming none: the partitions of the scope's namespaces, filtered to the scope.
        let p = plan_statement(&m, &catalog, &reach, None).unwrap();
        let want: BTreeSet<Partition> = ["a3t-shared--bot", "a3t-shared--ops"].iter().map(|n| partition_of(&m, n)).collect();
        assert_eq!(p.partitions, want);
        assert_eq!(p.filter, Some(reach.clone()));

        // Full access: every memory partition, no filter.
        let p = plan_statement(&m, &catalog, &Reach::All, None).unwrap();
        assert_eq!(p.partitions.len(), 64);
        assert_eq!(p.filter, None);

        // A scope with nothing in the catalog yet runs nowhere.
        let p = plan_statement(&m, &catalog, &Reach::Only(vec!["new--*".into()]), None).unwrap();
        assert!(p.partitions.is_empty());
    }
}

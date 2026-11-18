//! AQL v0.13 BRANCH / FORK / MERGE on a cluster: the branch and its source in
//! partitions owned by different nodes. Three nodes in one process (calls between them
//! go through JSON, as on the wire).
//!
//! Its own test binary: object storage off for the process.

use std::sync::{Arc, RwLock};

use adb_cluster::map::{Assignment, ControlCmd, NodeId, Partition, PartitionMap};
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::{LocalPeers, Placement};
use adb_executor::Engine;
use serde_json::Value;

const PARTITIONS: Partition = 8;

struct Static {
    me: NodeId,
    map: RwLock<Arc<PartitionMap>>,
}

impl Placement for Static {
    fn me(&self) -> NodeId {
        self.me
    }
    fn map(&self) -> Arc<PartitionMap> {
        Arc::clone(&self.map.read().unwrap())
    }
}

/// Partition p owned by node p % 3 + 1 (the system partition too).
fn map3() -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for n in 1..=3u64 {
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}"), flight: None }).unwrap();
    }
    for p in m.all_partitions() {
        let owner = u64::from(p) % 3 + 1;
        m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner, replica: Some(owner % 3 + 1), epoch: 1 } }).unwrap();
    }
    m
}


async fn roles(n: &Arc<PartitionedEngine>, ns: &str) -> Vec<String> {
    let r = n.query(&format!(r#"RECALL FROM SEMANTIC NAMESPACE "{ns}""#), None).await.unwrap_or_else(|e| panic!("{ns}: {e}"));
    let mut v: Vec<String> = r["data"]["records"].as_array().unwrap().iter().filter_map(|x| x["data"]["role"].as_str().map(String::from)).collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_fork_merge_across_partitions() {
    std::env::set_var("ADB_S3", "off");
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for n in 1..=3u64 {
        let place = Arc::new(Static { me: n, map: RwLock::new(Arc::new(map3())) });
        let node = PartitionedEngine::open_cluster(dirs[n as usize - 1].path(), place as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        peers.add(&format!("node{n}"), &node);
        nodes.push(node);
    }
    let base = "crm";
    let owner = |ns: &str| u64::from(nodes[0].partition_of(ns)) % 3;
    // A branch and a fork whose partitions another node owns.
    let pick = |prefix: &str| (0..100).map(|i| format!("{prefix}{i}")).find(|n| owner(n) != owner(base)).unwrap();
    let (br, fk) = (pick("crm-try-"), pick("crm-copy-"));
    let q = |i: usize, s: String| {
        let n = Arc::clone(&nodes[i]);
        async move { n.query(&s, None).await.unwrap_or_else(|e| panic!("{s}: {e}")) }
    };
    q(0, format!(r#"STORE INTO SEMANTIC (key = "ada", role = "engineer") NAMESPACE "{base}""#)).await;
    q(1, format!(r#"STORE INTO SEMANTIC (key = "bob", role = "sales") NAMESPACE "{base}""#)).await;
    q(2, format!(r#"BRANCH FROM "{base}" INTO "{br}""#)).await;
    assert_eq!(roles(&nodes[0], &br).await, ["engineer", "sales"], "read through, across partitions");
    q(1, format!(r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CTO") NAMESPACE "{br}""#)).await;
    assert_eq!(roles(&nodes[2], &br).await, ["CTO", "sales"]);
    assert_eq!(roles(&nodes[2], base).await, ["engineer", "sales"]);

    let f = q(0, format!(r#"FORK FROM "{base}" INTO "{fk}""#)).await;
    assert_eq!(f["data"]["records"], 2, "{f}");
    assert_eq!(roles(&nodes[1], &fk).await, ["engineer", "sales"]);

    let m = q(2, format!(r#"MERGE FROM "{br}" INTO "{base}""#)).await;
    assert_eq!(m["data"]["merged"], 1, "{m}");
    assert_eq!(roles(&nodes[0], base).await, ["CTO", "sales"]);

    // Dropping the base is refused while the branch reads through to it.
    let e = nodes[1].query(&format!(r#"FORGET FROM ALL NAMESPACE "{base}""#), None).await.unwrap_err();
    assert!(e.contains("has branches"), "{e}");
    q(1, format!(r#"FORGET FROM ALL NAMESPACE "{br}""#)).await;
    q(1, format!(r#"FORGET FROM ALL NAMESPACE "{base}""#)).await;
    assert_eq!(roles(&nodes[2], &fk).await, ["engineer", "sales"], "the fork keeps its copy");
}

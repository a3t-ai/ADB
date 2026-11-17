//! Readers beyond the owner: node 1 owns every partition, node 2 is the replica, node 3
//! neither. Node 2 keeps a reader fed from its replica buffer and answers fresh reads;
//! node 3 keeps none while the partition is cold there (its reads go to the owner or
//! the replica), builds one once it is hot there (`ADB_READER_HOT`), and drops it when
//! idle (`ADB_READER_IDLE_SECS`).
//!
//! Its own test binary: it sets `ADB_COLD_URI` and the reader settings.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use adb_cluster::map::{Assignment, ControlCmd, NodeId, Partition, PartitionMap};
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::{LocalPeers, Placement};
use adb_executor::Engine;
use serde_json::Value;

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

const PARTITIONS: Partition = 4;

fn map3() -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for n in 1..=3u64 {
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}"), flight: None }).unwrap();
    }
    for p in m.all_partitions() {
        m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner: 1, replica: Some(2), epoch: 1 } }).unwrap();
    }
    m
}

fn ns_of(v: &Value) -> Vec<i64> {
    let mut n: Vec<i64> = v["data"]["records"].as_array().cloned().unwrap_or_default().iter().filter_map(|r| r["data"]["n"].as_i64()).collect();
    n.sort();
    n
}

async fn until(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..150 {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn the_replica_reads_and_other_nodes_only_when_hot() {
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    std::env::set_var("ADB_READER_HOT", "20");
    std::env::set_var("ADB_READER_IDLE_SECS", "1");
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let place = Arc::new(Static { me: i as u64 + 1, map: RwLock::new(Arc::new(map3())) }) as Arc<dyn Placement>;
        let n = Box::pin(PartitionedEngine::open_cluster(d.path(), place, Arc::clone(&peers) as _)).await.unwrap();
        peers.add(&format!("node{}", i + 1), &n);
        nodes.push(n);
    }
    let ns = "a3t-shared--bot";
    let p = nodes[0].partition_of(ns);
    let q = format!(r#"RECALL FROM SEMANTIC WHERE n >= 0 NAMESPACE "{ns}" LIMIT 100"#);
    for i in 0..10 {
        nodes[0].query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }

    // The replica builds its reader by itself and answers from it.
    let reader2 = dirs[1].path().join(format!("parts/reader-p{p}"));
    assert!(until(|| reader2.exists()).await, "the replica keeps a reader");
    let mut v = nodes[1].query(&q, None).await.unwrap();
    for _ in 0..100 {
        if v["metadata"]["served_by"] == "follower" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        v = nodes[1].query(&q, None).await.unwrap();
    }
    assert_eq!(v["metadata"]["served_by"], "follower", "{}", v["metadata"]);
    assert_eq!(ns_of(&v), (0..10).collect::<Vec<_>>());
    // Fresh from its buffer: a write through the owner, seen by the next read there.
    nodes[0].query(&format!(r#"STORE INTO SEMANTIC (key = "w", n = 100) NAMESPACE "{ns}""#), None).await.unwrap();
    assert!(ns_of(&nodes[1].query(&q, None).await.unwrap()).contains(&100));
    nodes[2].query(&format!(r#"FORGET FROM SEMANTIC WHERE key = "w" NAMESPACE "{ns}""#), None).await.unwrap();
    assert!(!ns_of(&nodes[1].query(&q, None).await.unwrap()).contains(&100), "a FORGET is seen at once");

    // Node 3, cold: answered (by the owner or the replica), no reader of its own.
    let reader3 = dirs[2].path().join(format!("parts/reader-p{p}"));
    for _ in 0..5 {
        assert_eq!(ns_of(&nodes[2].query(&q, None).await.unwrap()).len(), 10);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!reader3.exists() && !nodes[2].has_reader(p), "no reader on a node where the partition is cold");

    // Hot at node 3: it builds a reader and answers from it.
    let mut served = Value::Null;
    for _ in 0..300 {
        let v = nodes[2].query(&q, None).await.unwrap();
        assert_eq!(ns_of(&v).len(), 10);
        if v["metadata"]["served_by"] == "follower" && nodes[2].has_reader(p) {
            served = v;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(served["metadata"]["served_by"], "follower", "node 3 reads from its own reader once hot");

    // Idle: node 3's reader is dropped (node 2's stays).
    assert!(until(|| !nodes[2].has_reader(p)).await, "node 3's idle reader was dropped");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(nodes[1].has_reader(p), "the replica's reader stays, idle or not");
    assert_eq!(ns_of(&nodes[2].query(&q, None).await.unwrap()).len(), 10, "still answered");
}

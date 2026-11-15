//! S3 option B step 6: readers on every node. Node 1 owns every partition; node 2
//! answers reads from its own readers (built from object storage, kept up by the
//! owner's tail) — fresh: a write acknowledged through either node is seen by the next
//! read through node 2 — and rebuilds a reader from a newer checkpoint.
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

fn map2() -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for n in 1..=2u64 {
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

#[tokio::test(flavor = "multi_thread")]
async fn every_node_reads_from_its_own_readers() {
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    std::env::set_var("ADB_READER_REBASE_SECS", "1");
    let peers = LocalPeers::new();
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let place = |me| Arc::new(Static { me, map: RwLock::new(Arc::new(map2())) }) as Arc<dyn Placement>;
    let n1 = Box::pin(PartitionedEngine::open_cluster(d1.path(), place(1), Arc::clone(&peers) as _)).await.unwrap();
    let n2 = Box::pin(PartitionedEngine::open_cluster(d2.path(), place(2), Arc::clone(&peers) as _)).await.unwrap();
    peers.add("node1", &n1);
    peers.add("node2", &n2);
    let ns = "a3t-shared--bot";
    let p = n1.partition_of(ns);

    // Some data, a checkpoint on object storage, more data after it.
    for i in 0..20 {
        n1.query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }
    let owner = Box::pin(n1.part(p)).await.unwrap();
    Box::pin(owner.ship_now()).await.unwrap();
    Box::pin(owner.archive_now()).await.unwrap();
    for i in 20..25 {
        n1.query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }

    // Read through node 2: its reader answers, and has everything.
    let q = format!(r#"RECALL FROM SEMANTIC WHERE n >= 0 NAMESPACE "{ns}" LIMIT 100"#);
    let v = n2.query(&q, None).await.unwrap();
    assert_eq!(v["metadata"]["served_by"], "follower", "served by node 2's reader: {}", v["metadata"]);
    assert_eq!(ns_of(&v), (0..25).collect::<Vec<_>>());
    assert!(d2.path().join(format!("parts/reader-p{p}")).exists(), "the reader is on node 2's disk");

    // Fresh: a write through node 1, then through node 2 (sent to the owner): each is
    // seen by the very next read through node 2.
    n1.query(&format!(r#"STORE INTO SEMANTIC (key = "w1", n = 100) NAMESPACE "{ns}""#), None).await.unwrap();
    assert!(ns_of(&n2.query(&q, None).await.unwrap()).contains(&100));
    n2.query(&format!(r#"STORE INTO SEMANTIC (key = "w2", n = 101) NAMESPACE "{ns}""#), None).await.unwrap();
    assert!(ns_of(&n2.query(&q, None).await.unwrap()).contains(&101));
    n2.query(&format!(r#"FORGET FROM SEMANTIC WHERE key = "w1" NAMESPACE "{ns}""#), None).await.unwrap();
    assert!(!ns_of(&n2.query(&q, None).await.unwrap()).contains(&100), "a FORGET is seen at once too");

    // A statement across partitions through node 2: over its readers.
    n1.query(r#"STORE INTO SEMANTIC (key = "o", n = 500) NAMESPACE "demo--y""#, None).await.unwrap();
    let all = n2.query("RECALL FROM SEMANTIC WHERE n >= 100 ORDER BY n ASC", None).await.unwrap();
    assert_eq!(ns_of(&all), vec![101, 500], "{all}");

    // A newer checkpoint: the reader rebuilds from it (its next generation directory).
    Box::pin(owner.ship_now()).await.unwrap();
    Box::pin(owner.archive_now()).await.unwrap();
    let g1 = d2.path().join(format!("parts/reader-p{p}/g1"));
    for _ in 0..100 {
        if g1.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(g1.exists(), "rebuilt from the newer checkpoint");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let v = n2.query(&q, None).await.unwrap();
    let mut want: Vec<i64> = (0..25).collect();
    want.push(101);
    assert_eq!(ns_of(&v), want, "the rebuilt reader answers the same");
}

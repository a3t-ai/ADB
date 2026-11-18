//! A takeover starts from the new owner's reader (Hetzner p13: a full restore of a
//! node's worth of partitions kept every request to them waiting a minute). Node 1 owns
//! a partition, node 2 is its replica and has its reader up; writes go on — some shipped
//! to object storage, the last ones only in node 1's log and node 2's memory. Node 1 is
//! lost; node 2 takes the partition over from its reader (plus the segments above it and
//! its replica tail): no acknowledged write is lost, it takes writes, and it opens again
//! after a restart.
//!
//! Its own test binary: it sets `ADB_COLD_URI`.

use std::sync::atomic::{AtomicBool, Ordering};
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
    leased: AtomicBool,
}

impl Placement for Static {
    fn me(&self) -> NodeId {
        self.me
    }
    fn map(&self) -> Arc<PartitionMap> {
        Arc::clone(&self.map.read().unwrap())
    }
    fn leased(&self) -> bool {
        self.leased.load(Ordering::SeqCst)
    }
}

const PARTITIONS: Partition = 4;

fn map(owner: NodeId, replica: NodeId, epoch: u64) -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for n in 1..=3u64 {
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}"), flight: None }).unwrap();
    }
    for p in m.all_partitions() {
        m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner: 1, replica: Some(2), epoch: 1 } }).unwrap();
        if epoch > 1 {
            m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner, replica: Some(replica), epoch } }).unwrap();
        }
    }
    m
}

fn ns_of(v: &Value) -> Vec<i64> {
    let mut n: Vec<i64> = v["data"]["records"].as_array().cloned().unwrap_or_default().iter().filter_map(|r| r["data"]["n"].as_i64()).collect();
    n.sort();
    n
}

async fn store(node: &Arc<PartitionedEngine>, ns: &str, range: std::ops::Range<i64>) {
    for i in range {
        node.query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_takeover_starts_from_the_replicas_reader() {
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    std::env::set_var("ADB_WAL_SEGMENT_MS", "3600000"); // segments only when asked
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let places: Vec<Arc<Static>> = (1..=3).map(|me| Arc::new(Static { me, map: RwLock::new(Arc::new(map(1, 2, 1))), leased: AtomicBool::new(true) })).collect();
    let mut nodes = Vec::new();
    for (i, place) in places.iter().enumerate() {
        let n = Box::pin(PartitionedEngine::open_cluster(dirs[i].path(), Arc::clone(place) as Arc<dyn Placement>, Arc::clone(&peers) as _)).await.unwrap();
        peers.add(&format!("node{}", i + 1), &n);
        nodes.push(n);
    }
    let ns = "a3t-shared--bot";
    let p = nodes[0].partition_of(ns);
    let q = format!(r#"RECALL FROM SEMANTIC WHERE n >= 0 NAMESPACE "{ns}" LIMIT 1000"#);

    // 20 writes on object storage; node 2 (the replica) builds its reader from there.
    store(&nodes[2], ns, 0..20).await;
    let owner = Box::pin(nodes[0].part(p)).await.unwrap();
    Box::pin(owner.ship_now()).await.unwrap();
    for _ in 0..200 {
        if nodes[1].reader(p).await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(nodes[1].has_reader(p), "node 2's reader is up");
    assert_eq!(ns_of(&nodes[1].query(&q, None).await.unwrap()).len(), 20, "read through node 2's reader");

    // 10 more shipped (above what the reader was built from), then 10 only on node 1
    // and in node 2's memory.
    store(&nodes[2], ns, 20..30).await;
    let shipped = Box::pin(owner.ship_now()).await.unwrap();
    store(&nodes[2], ns, 30..40).await;
    let (_, tail) = nodes[1].replica_tail(p).unwrap();
    assert!(tail.iter().all(|r| r.offset.0 > shipped) && tail.len() >= 10, "the unshipped writes are in node 2's memory");
    drop(owner);

    // Node 1 is lost; node 2 owns the partition at epoch 2.
    peers.remove("node1");
    for place in &places[1..] {
        *place.map.write().unwrap() = Arc::new(map(2, 3, 2));
    }
    let mut got = Vec::new();
    for _ in 0..100 {
        if let Ok(v) = nodes[2].query(&q, None).await {
            got = ns_of(&v);
            if got.len() == 40 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(got, (0..40).collect::<Vec<_>>(), "no acknowledged write lost");
    assert!(nodes[1].takeovers_from_reader() >= 1, "the takeover started from node 2's reader");
    assert!(!nodes[1].has_reader(p), "the reader became the partition");
    assert!(!dirs[1].path().join(format!("parts/reader-p{p}")).exists(), "its directory moved");

    // The new owner takes writes; its replica (node 3) holds them.
    store(&nodes[2], ns, 40..41).await;
    assert_eq!(ns_of(&nodes[1].query(&q, None).await.unwrap()).len(), 41);
    assert!(!nodes[2].replica_tail(p).unwrap().1.is_empty(), "node 3 is the replica now");

    // Node 2 restarts: the promoted directory opens as the partition's own.
    peers.remove("node2");
    let n2 = nodes.remove(1);
    drop(n2);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let n2 = Box::pin(PartitionedEngine::open_cluster(dirs[1].path(), Arc::clone(&places[1]) as Arc<dyn Placement>, Arc::clone(&peers) as _)).await.unwrap();
    peers.add("node2", &n2);
    let mut after = Vec::new();
    for _ in 0..100 {
        if let Ok(v) = n2.query(&q, None).await {
            after = ns_of(&v);
            if after.len() == 41 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(after.len(), 41, "everything there after the restart");
}

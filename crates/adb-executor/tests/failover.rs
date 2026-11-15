//! S3 option B step 7: failover. Node 1 owns a partition, node 2 is its replica; some
//! writes are on object storage, the last ones only in node 1's log and node 2's
//! memory. Node 1 is lost; the map moves the partition to node 2 at epoch 2 (node 3
//! the new replica). Node 2 takes it over — object storage, then its replica tail —
//! and no acknowledged write is lost. Node 1, cut off with its old map, is fenced on its
//! next write; an owner without a lease stops serving.
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

#[tokio::test(flavor = "multi_thread")]
async fn killing_an_owner_loses_no_acknowledged_write() {
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
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

    // 20 writes shipped to object storage, 10 more only on node 1 and node 2.
    for i in 0..20 {
        nodes[2].query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }
    let owner = Box::pin(nodes[0].part(p)).await.unwrap();
    let shipped = Box::pin(owner.ship_now()).await.unwrap();
    for i in 20..30 {
        nodes[2].query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }
    let (_, tail) = nodes[1].replica_tail(p).unwrap();
    assert!(tail.iter().all(|r| r.offset.0 > shipped) && tail.len() >= 10, "the unshipped writes are in node 2's memory");
    drop(owner);

    // Node 1 is lost (cut off; it keeps running with its old map).
    peers.remove("node1");
    for place in &places[1..] {
        *place.map.write().unwrap() = Arc::new(map(2, 3, 2));
    }
    // Node 2 takes the partition over by itself; reads through node 3 see every write.
    let mut got = Vec::new();
    for _ in 0..100 {
        if let Ok(v) = nodes[2].query(&q, None).await {
            got = ns_of(&v);
            if got.len() == 30 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(got, (0..30).collect::<Vec<_>>(), "no acknowledged write lost");
    assert_eq!(std::fs::read_to_string(dirs[1].path().join(format!("parts/p{p}/owner.epoch"))).unwrap(), "2");

    // The new owner takes writes; its replica (node 3) holds them.
    nodes[2].query(&format!(r#"STORE INTO SEMANTIC (key = "after", n = 99) NAMESPACE "{ns}""#), None).await.unwrap();
    assert!(ns_of(&nodes[1].query(&q, None).await.unwrap()).contains(&99));
    assert!(!nodes[2].replica_tail(p).unwrap().1.is_empty(), "node 3 is the replica now");

    // Node 1, still thinking it owns the partition, is fenced on its next write.
    let e = nodes[0].query(&format!(r#"STORE INTO SEMANTIC (key = "stale", n = 1000) NAMESPACE "{ns}""#), None).await.unwrap_err();
    assert!(e.contains("fenced") || e.contains("not committed"), "{e}");
    assert!(!ns_of(&nodes[1].query(&q, None).await.unwrap()).contains(&1000), "the stale write is nowhere");

    // An owner without a lease stops serving its partitions.
    places[1].leased.store(false, Ordering::SeqCst);
    let e = nodes[1].query(&q, None).await.unwrap_err();
    assert!(e.contains("no_lease"), "{e}");
    places[1].leased.store(true, Ordering::SeqCst);
    assert_eq!(ns_of(&nodes[1].query(&q, None).await.unwrap()).len(), 31);
}

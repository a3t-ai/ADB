//! S3 option B step 5: owner → replica. The replica's buffer (fencing, gaps, dropping
//! what object storage holds, spilling to disk, its cap), then an owner and its
//! replica in one process: a write is acknowledged only once the replica holds it,
//! waits while the replica is unreachable, lets the replica drop what is shipped, and
//! stops once a newer epoch fences the owner.
//!
//! Its own test binary: it sets the replica's limits and `ADB_COLD_URI`.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use adb_cluster::map::{Assignment, ControlCmd, NodeId, Partition, PartitionMap};
use adb_core::log::{CommitTs, Epoch, LogOffset, LogRecord, PartitionId};
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::{LocalPeers, Placement};
use adb_executor::replication::{ReplicaBuffers, ReplicateReq};
use adb_executor::Engine;

fn rec(o: u64) -> LogRecord {
    LogRecord { partition: PartitionId(3), offset: LogOffset(o), epoch: Epoch(1), commit_ts: CommitTs(o as i64), txn_id: None, effects: vec![] }
}

fn req(epoch: u64, shipped: u64, from: u64, to: u64) -> ReplicateReq {
    ReplicateReq { epoch, shipped, records: (from..=to).map(rec).collect() }
}

#[test]
fn the_replica_buffer() {
    let dir = tempfile::tempdir().unwrap();
    // A small memory limit, so a few records spill; a cap for the spill.
    let b = ReplicaBuffers::with_limits(dir.path(), 2000, 20000);
    assert_eq!(b.append(3, 1, req(1, 0, 1, 10)).unwrap().held, 10);
    // Again (a retry): nothing new, still 10. A gap: refused with what it holds.
    assert_eq!(b.append(3, 1, req(1, 0, 5, 10)).unwrap().held, 10);
    let e = b.append(3, 1, req(1, 0, 12, 13)).unwrap_err();
    assert!(e.starts_with("gap:10:"), "{e}");
    // Many more: past the memory limit they spill; the tail still has every one.
    assert_eq!(b.append(3, 1, req(1, 0, 11, 60)).unwrap().held, 60);
    let (mem, _, spilled) = b.usage(3);
    assert!(spilled > 0 && mem < 60, "spilled: {mem} in memory, {spilled} bytes on disk");
    let (epoch, tail) = b.tail(3).unwrap();
    assert_eq!((epoch, tail.iter().map(|r| r.offset.0).collect::<Vec<_>>()), (1, (1..=60).collect::<Vec<_>>()));
    // Object storage holds up to 55: the replica drops it (the spill file too).
    b.append(3, 1, req(1, 55, 61, 61)).unwrap();
    let (_, tail) = b.tail(3).unwrap();
    assert_eq!(tail.iter().map(|r| r.offset.0).collect::<Vec<_>>(), (56..=61).collect::<Vec<_>>());
    assert_eq!(b.usage(3).2, 0, "nothing spilled any more");
    // An older epoch is refused: fenced.
    assert!(b.append(3, 2, req(1, 55, 62, 62)).unwrap_err().starts_with("fenced:"));
    // A new owner (epoch 2) starts the buffer again, from what object storage holds.
    assert!(b.append(3, 2, req(2, 70, 75, 75)).unwrap_err().starts_with("gap:70:"));
    assert_eq!(b.append(3, 2, req(2, 70, 71, 72)).unwrap().held, 72);
    // The cap: object storage far behind, the spill full → refused.
    let e = (73..2000u64).step_by(50).find_map(|f| b.append(3, 2, req(2, 70, f, f + 49)).err()).expect("the cap is reached");
    assert!(e.starts_with("replica_full:"), "{e}");
}

/// A takeover seals the buffer: the tail it reads holds every record this replica
/// acknowledged, and an append of the old epoch that checked an older map (the map
/// here passes it) is refused from then on — before, one landing after the tail was
/// read was acknowledged and lost (Hetzner round 6). Clearing keeps the seal.
#[test]
fn a_takeover_seals_the_replica_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let b = ReplicaBuffers::with_limits(dir.path(), 1 << 20, 1 << 24);
    assert_eq!(b.append(5, 1, req(1, 0, 1, 10)).unwrap().held, 10);
    let (epoch, tail) = b.seal(5, 2).unwrap();
    assert_eq!((epoch, tail.len()), (1, 10), "every acknowledged record");
    // The old owner's next records, checked against a map still at epoch 1: refused.
    let e = b.append(5, 1, req(1, 0, 11, 12)).unwrap_err();
    assert!(e.starts_with("fenced:"), "{e}");
    b.clear(5);
    assert!(b.append(5, 1, req(1, 0, 11, 12)).unwrap_err().starts_with("fenced:"), "the seal outlives the buffer");
    // The new owner (epoch 2) replicates as usual.
    assert_eq!(b.append(5, 2, req(2, 10, 11, 12)).unwrap().held, 12);
}

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

/// Node 1 owns every partition; node 2 is the replica of each.
fn map2(epoch: u64) -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for n in 1..=2u64 {
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}"), flight: None }).unwrap();
    }
    for p in m.all_partitions() {
        m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner: 1, replica: Some(2), epoch: 1 } }).unwrap();
        if epoch > 1 {
            m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner: 2, replica: None, epoch } }).unwrap();
        }
    }
    m
}

#[tokio::test(flavor = "multi_thread")]
async fn an_owner_acknowledges_what_its_replica_holds() {
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    // Segments only when asked: a ship in between lets the replica drop records.
    std::env::set_var("ADB_WAL_SEGMENT_MS", "3600000");
    let peers = LocalPeers::new();
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let p1 = Arc::new(Static { me: 1, map: RwLock::new(Arc::new(map2(1))) });
    let p2 = Arc::new(Static { me: 2, map: RwLock::new(Arc::new(map2(1))) });
    let n1 = Box::pin(PartitionedEngine::open_cluster(d1.path(), Arc::clone(&p1) as Arc<dyn Placement>, Arc::clone(&peers) as _)).await.unwrap();
    let n2 = Box::pin(PartitionedEngine::open_cluster(d2.path(), Arc::clone(&p2) as Arc<dyn Placement>, Arc::clone(&peers) as _)).await.unwrap();
    peers.add("node1", &n1);
    peers.add("node2", &n2);
    let ns = "a3t-shared--bot";
    let p = n1.partition_of(ns);

    // A write is acknowledged once the replica holds it.
    for i in 0..5 {
        n1.query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{ns}""#), None).await.unwrap();
    }
    let (epoch, tail) = n2.replica_tail(p).unwrap();
    assert_eq!(epoch, 1);
    assert!(tail.len() >= 5, "the replica holds every acknowledged write: {}", tail.len());
    let last = Box::pin(n1.part(p)).await.unwrap().durable_writes().unwrap().point().await;
    assert_eq!(tail.last().unwrap().offset.0, last.0, "up to the owner's last record");

    // The replica unreachable: a write waits; it completes once the replica is back.
    peers.remove("node2");
    let n1c = Arc::clone(&n1);
    let w = tokio::spawn(async move { n1c.query(&format!(r#"STORE INTO SEMANTIC (key = "late", n = 9) NAMESPACE "{ns}""#), None).await });
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!w.is_finished(), "not acknowledged while the replica can't hold it");
    peers.add("node2", &n2);
    tokio::time::timeout(Duration::from_secs(10), w).await.expect("acknowledged once the replica is back").unwrap().unwrap();

    // Shipped to object storage: the replica drops it.
    let e = Box::pin(n1.part(p)).await.unwrap();
    let shipped = Box::pin(e.ship_now()).await.unwrap();
    assert!(shipped > 0);
    n1.query(&format!(r#"STORE INTO SEMANTIC (key = "after", n = 10) NAMESPACE "{ns}""#), None).await.unwrap();
    let (_, tail) = n2.replica_tail(p).unwrap();
    assert!(tail.iter().all(|r| r.offset.0 > shipped), "only what object storage lacks: {:?}", tail.iter().map(|r| r.offset.0).collect::<Vec<_>>());
    assert!(!tail.is_empty());

    // A newer epoch in the replica's map (node 2 took the partition over): the old
    // owner is fenced and takes no more writes.
    *p2.map.write().unwrap() = Arc::new(map2(2));
    let e = n1.query(&format!(r#"STORE INTO SEMANTIC (key = "stale", n = 11) NAMESPACE "{ns}""#), None).await.unwrap_err();
    assert!(e.contains("not_replica") || e.contains("fenced") || e.contains("not committed"), "{e}");
}

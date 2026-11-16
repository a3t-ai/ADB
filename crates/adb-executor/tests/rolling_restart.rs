//! S3 option B: a rolling restart under write load, after the WAL was archived and
//! purged — the path that stalled every write on vc-dev on 16 Nov (a partition
//! restored from object storage starts its WAL above the restore floor, and its
//! replication asked a new replica for the purged range forever).
//!
//! Three nodes; node 3 takes all the writes, without pause, the whole time:
//!   1. node 1 owns every partition (node 2 the replica); it ships, archives and
//!      purges its log;
//!   2. node 1 goes away: node 2 takes over (epoch 2, node 3 the replica) from
//!      object storage and the replica tail, ships, archives and purges in turn;
//!   3. node 1 comes back as a new process on an empty disk (a replaced pod) and
//!      the partitions move back to it the way the control plane moves them: first
//!      as node 2's replica (same epoch; node 2 sends it the tail), then promoted
//!      (epoch 3, node 2 the replica). It restores from object storage and its tail.
//!
//! Every acknowledged write is then read back through every node, and no write
//! waited longer than the forward window (a stall would hold it forever).
//!
//! Its own test binary: it sets `ADB_COLD_URI`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

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
const NS: &str = "a3t-shared--restart";

/// Every partition (and the system partition) at `owner`/`replica`/`epoch`, after
/// the earlier steps: the map's history, as the control plane builds it (an owner
/// changes only to the replica, at the next epoch; a replica moves at the same one).
fn map(steps: &[(NodeId, NodeId, u64)]) -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for n in 1..=3u64 {
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}"), flight: None }).unwrap();
    }
    for (owner, replica, epoch) in steps {
        for p in m.all_partitions() {
            m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner: *owner, replica: Some(*replica), epoch: *epoch } }).unwrap();
        }
    }
    m
}

fn keys_of(v: &Value) -> Vec<i64> {
    let mut n: Vec<i64> = v["data"]["records"].as_array().cloned().unwrap_or_default().iter().filter_map(|r| r["data"]["n"].as_i64()).collect();
    n.sort();
    n
}

/// Ship, archive and purge the log of every partition `node` owns.
async fn archive_and_purge(node: &Arc<PartitionedEngine>) {
    for p in 0..=PARTITIONS {
        let Ok(e) = Box::pin(node.part(p)).await else { continue };
        let Some(w) = e.durable_writes() else { continue };
        e.ship_now().await.unwrap();
        e.archive_now().await.unwrap();
        w.purge_log().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolling_restart_after_archive_and_purge_loses_nothing_and_never_stalls() {
    // Logs on with RUST_LOG (the takeovers and replication say what they do).
    let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).with_test_writer().try_init();
    let cold = tempfile::tempdir().unwrap();
    std::env::set_var("ADB_COLD_URI", format!("file://{}", cold.path().display()));
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    // Shipping and archiving only when the test asks (the timers would race it).
    std::env::set_var("ADB_ARCHIVE_EVERY_SECS", "3600");
    std::env::set_var("ADB_WAL_SEGMENT_MS", "3600000");
    std::env::set_var("ADB_FORWARD_WAIT_MS", "10000");
    let peers = LocalPeers::new();
    let mut dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let step1 = [(1, 2, 1)];
    let places: Vec<Arc<Static>> = (1..=3).map(|me| Arc::new(Static { me, map: RwLock::new(Arc::new(map(&step1))) })).collect();
    let mut nodes = Vec::new();
    for (i, place) in places.iter().enumerate() {
        let n = Box::pin(PartitionedEngine::open_cluster(dirs[i].path(), Arc::clone(place) as Arc<dyn Placement>, Arc::clone(&peers) as _)).await.unwrap();
        peers.add(&format!("node{}", i + 1), &n);
        nodes.push(n);
    }

    // The writer: through node 3, never pausing; a write that fails (mid-move) is
    // tried again, and only an acknowledged one counts.
    let acked: Arc<Mutex<Vec<i64>>> = Arc::default();
    // Writes counted on `key_exists` (an earlier, unanswered attempt had landed).
    let by_exists: Arc<Mutex<Vec<i64>>> = Arc::default();
    let longest_ms = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (n3, acked, longest_ms, stop, by_exists) = (Arc::clone(&nodes[2]), Arc::clone(&acked), Arc::clone(&longest_ms), Arc::clone(&stop), Arc::clone(&by_exists));
        tokio::spawn(async move {
            let mut i = 0i64;
            while !stop.load(Ordering::SeqCst) {
                let t = Instant::now();
                let r = tokio::time::timeout(Duration::from_secs(30), n3.query(&format!(r#"STORE INTO SEMANTIC (key = "k{i}", n = {i}) NAMESPACE "{NS}""#), None)).await;
                longest_ms.fetch_max(t.elapsed().as_millis() as u64, Ordering::SeqCst);
                match r {
                    Ok(Ok(_)) => {
                        acked.lock().unwrap().push(i);
                        i += 1;
                    }
                    // `key_exists`: the write landed before its answer was lost.
                    Ok(Err(e)) if e.contains("key_exists") => {
                        by_exists.lock().unwrap().push(i);
                        acked.lock().unwrap().push(i);
                        i += 1;
                    }
                    Ok(Err(e)) => {
                        eprintln!("WRITE-ERR k{i}: {}", e.chars().take(160).collect::<String>());
                        tokio::time::sleep(Duration::from_millis(50)).await
                    }
                    Err(_) => panic!("write k{i} stalled for 30 s"),
                }
            }
        })
    };
    let wait_for = |n: usize| {
        let acked = Arc::clone(&acked);
        async move {
            for _ in 0..600 {
                if acked.lock().unwrap().len() >= n {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("the writer stopped at {} acknowledged writes", acked.lock().unwrap().len());
        }
    };

    // 1. Node 1 owns everything; its log goes to object storage and is purged.
    wait_for(30).await;
    archive_and_purge(&nodes[0]).await;
    wait_for(45).await;

    // 2. Node 1 goes away; node 2 takes over (its WAL starts above the floor), node 3
    //    its replica. Writes go on; then node 2 archives and purges too.
    peers.remove("node1");
    let step2 = [(1, 2, 1), (2, 3, 2)];
    for place in &places[1..] {
        *place.map.write().unwrap() = Arc::new(map(&step2));
    }
    let n = acked.lock().unwrap().len();
    wait_for(n + 30).await;
    archive_and_purge(&nodes[1]).await;
    wait_for(n + 45).await;

    // 3. Node 1 comes back as a new process on an empty disk (a replaced pod). First
    //    it becomes node 2's replica (same epoch: node 2 sends it what object storage
    //    lacks), then it is promoted (epoch 3, node 2 the replica) and restores from
    //    object storage and its tail.
    drop(nodes.remove(0));
    dirs[0] = tempfile::tempdir().unwrap();
    let step3a = [(1, 2, 1), (2, 3, 2), (2, 1, 2)];
    let fresh = Arc::new(Static { me: 1, map: RwLock::new(Arc::new(map(&step3a))) });
    let n1 = Box::pin(PartitionedEngine::open_cluster(dirs[0].path(), Arc::clone(&fresh) as Arc<dyn Placement>, Arc::clone(&peers) as _)).await.unwrap();
    peers.add("node1", &n1);
    nodes.insert(0, n1);
    for place in &places[1..] {
        *place.map.write().unwrap() = Arc::new(map(&step3a));
    }
    let n = acked.lock().unwrap().len();
    wait_for(n + 30).await;
    let step3b = [(1, 2, 1), (2, 3, 2), (2, 1, 2), (1, 2, 3)];
    *fresh.map.write().unwrap() = Arc::new(map(&step3b));
    for place in &places[1..] {
        *place.map.write().unwrap() = Arc::new(map(&step3b));
    }
    let n = acked.lock().unwrap().len();
    wait_for(n + 30).await;

    stop.store(true, Ordering::SeqCst);
    writer.await.unwrap();
    let acked = acked.lock().unwrap().clone();
    let longest = longest_ms.load(Ordering::SeqCst);
    assert!(longest < 15_000, "a write waited {longest} ms: replication or a takeover stalled");

    // Every acknowledged write, through every node.
    let q = format!(r#"RECALL FROM SEMANTIC WHERE n >= 0 NAMESPACE "{NS}" LIMIT 10000"#);
    let mut report = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let got = node.query(&q, None).await.map(|v| keys_of(&v)).unwrap_or_default();
        let missing = acked.iter().filter(|k| got.binary_search(k).is_err()).count();
        report.push(format!("node {}: {} of {} present, {missing} missing, first missing {:?}", i + 1, got.len(), acked.len(), acked.iter().find(|k| got.binary_search(k).is_err())));
    }
    eprintln!("READBACK {} | counted on key_exists: {:?}", report.join(" | "), by_exists.lock().unwrap());
    for (i, node) in nodes.iter().enumerate() {
        let mut got = Vec::new();
        for _ in 0..100 {
            if let Ok(v) = node.query(&q, None).await {
                got = keys_of(&v);
                if acked.iter().all(|k| got.binary_search(k).is_ok()) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let missing: Vec<i64> = acked.iter().filter(|k| got.binary_search(k).is_err()).copied().collect();
        assert!(missing.is_empty(), "node {}: {} of {} acknowledged writes missing: {:?}", i + 1, missing.len(), acked.len(), &missing[..missing.len().min(20)]);
    }
    eprintln!("{} writes acknowledged across two takeovers; longest wait {longest} ms", acked.len());
}

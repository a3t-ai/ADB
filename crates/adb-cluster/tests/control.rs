//! The control plane in one process: three voters, three nodes heartbeating them.
//! The map gets every partition an owner and a replica on another node; the control
//! leader (node and voter) is then stopped: a new leader keeps the map and, once the
//! lost node's lease lapses, its partitions go to their replicas at the next epoch.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use adb_cluster::{ControlClient, ControlNode, NodeId, PartitionMap};

struct Voter {
    node: Arc<ControlNode>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

async fn wait_for(what: &str, within: Duration, mut ok: impl FnMut() -> bool) {
    let t = std::time::Instant::now();
    while !ok() {
        assert!(t.elapsed() < within, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn whole(m: &PartitionMap, live: &[NodeId]) -> bool {
    m.partitions > 0
        && m.assignments.len() == m.partitions as usize + 1
        && m.assignments.values().all(|a| live.contains(&a.owner) && a.replica.is_some_and(|r| r != a.owner && live.contains(&r)))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_map_is_kept_and_repaired_across_a_leader_loss() {
    let _ = tracing_subscriber::fmt().with_env_filter("adb_cluster=debug,openraft=warn").with_test_writer().try_init();
    std::env::set_var("ADB_LEASE_MS", "1500");
    std::env::set_var("ADB_HEARTBEAT_MS", "200");
    // 5 memory partitions + the system partition: 2 owners per node.
    const PARTITIONS: u16 = 5;
    let mut listeners = Vec::new();
    let mut voters_at = BTreeMap::new();
    for id in 1..=3u64 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        voters_at.insert(id, format!("http://{}", l.local_addr().unwrap()));
        listeners.push((id, l));
    }
    let mut voters: BTreeMap<NodeId, Voter> = BTreeMap::new();
    for (id, l) in listeners {
        let dir = tempfile::tempdir().unwrap();
        let node = ControlNode::start(dir.path(), id, voters_at.clone()).await.unwrap();
        let app = node.router();
        let serve = tokio::spawn(async move {
            axum::serve(l, app).await.unwrap();
        });
        let run = tokio::spawn(Arc::clone(&node).run(PARTITIONS));
        voters.insert(id, Voter { node, tasks: vec![serve, run], _dir: dir });
    }
    let mut clients: BTreeMap<NodeId, ControlClient> = (1..=3u64).map(|id| (id, ControlClient::start(id, format!("http://data-{id}"), None, voters_at.clone(), move || id as f64 * 0.1))).collect();

    // Every partition placed, every node a member, owners spread evenly.
    let c1 = clients[&1].clone();
    wait_for("a whole map", Duration::from_secs(20), || whole(&c1.map(), &[1, 2, 3])).await;
    let before = c1.map();
    drop(c1);
    eprintln!("map: {:?}", before.assignments);
    assert_eq!(before.members.len(), 3);
    for n in 1..=3 {
        assert_eq!(before.owned_by(n).count(), 2, "owners spread: {:?}", before.assignments);
    }
    assert!(before.assignments.values().all(|a| a.epoch == 1));
    // Every voter applied the same map.
    for v in voters.values() {
        wait_for("voters agree", Duration::from_secs(5), || v.node.map().assignments == before.assignments).await;
    }

    // Stop the control leader: its voter and its node.
    let leader = voters[&1].node.leader().expect("a leader");
    eprintln!("stopping node {leader} (the control leader)");
    let gone = voters.remove(&leader).unwrap();
    for t in &gone.tasks {
        t.abort();
    }
    gone.node.raft.shutdown().await.unwrap();
    drop(gone);
    clients.remove(&leader);
    let rest: Vec<NodeId> = clients.keys().copied().collect();
    let c = clients[&rest[0]].clone();

    // A new leader; the lost node's partitions move once its lease lapses.
    let probe = (c.clone(), voters.values().map(|v| Arc::clone(&v.node)).collect::<Vec<_>>());
    let mut last = std::time::Instant::now();
    wait_for("the map repaired without the lost node", Duration::from_secs(30), || {
        if last.elapsed() > Duration::from_secs(3) {
            last = std::time::Instant::now();
            eprintln!("client map v{} {:?}; voters: {:?}", probe.0.map().version, probe.0.map().assignments, probe.1.iter().map(|n| (n.id, n.leader(), n.map().version)).collect::<Vec<_>>());
        }
        whole(&c.map(), &rest)
    })
    .await;
    let after = c.map();
    eprintln!("after: {:?}", after.assignments);
    let new_leader = voters.values().next().unwrap().node.leader().unwrap();
    assert_ne!(new_leader, leader);
    for (p, a) in &before.assignments {
        let now = &after.assignments[p];
        if a.owner == leader {
            assert_eq!(now.owner, a.replica.unwrap(), "partition {p}: its replica took over");
            assert_eq!(now.epoch, a.epoch + 1, "partition {p}: the next epoch");
        } else {
            assert_eq!((now.owner, now.epoch), (a.owner, a.epoch), "partition {p}: owner kept");
        }
    }
    for v in voters.values() {
        wait_for("voters agree after", Duration::from_secs(5), || v.node.map().assignments == after.assignments).await;
    }
}

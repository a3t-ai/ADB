//! S3 option B step 4: any node accepts any request. Three nodes in one process, the
//! partitions spread over them (calls between them go through JSON, as on the wire).
//! The same workload sent through different nodes answers as one engine does; each
//! node's disk holds only its own partitions; a node with a stale map is corrected by
//! the owner's `not_owner` answer and retries once.
//!
//! Its own test binary: object storage off for the process.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use adb_cluster::map::{Assignment, ControlCmd, NodeId, Partition, PartitionMap};
use adb_cluster::routing::Reach;
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::{LocalPeers, Placement};
use adb_executor::{Engine, LocalEngine};
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
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}") }).unwrap();
    }
    for p in m.all_partitions() {
        let owner = u64::from(p) % 3 + 1;
        m.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner, replica: Some(owner % 3 + 1), epoch: 1 } }).unwrap();
    }
    m
}

fn stable(v: &Value) -> Value {
    let data = &v["data"];
    match data["type"].as_str() {
        Some("records") => Value::Array(data["records"].as_array().unwrap().iter().map(|r| r["data"].clone()).collect()),
        Some("aggregation") => data["values"].clone(),
        Some("affected") => data["count"].clone(),
        _ => data.clone(),
    }
}

const NS: [&str; 4] = ["a3t-shared--bot", "a3t-shared--ops", "demo--y", "system-test--z"];

/// The workload, each statement through node `via(i)`.
async fn workload(via: &dyn Fn(usize) -> Arc<dyn Engine>) {
    let mut i = 0;
    let mut q = |s: String| {
        i += 1;
        let e = via(i);
        async move { e.query(&s, None).await.unwrap_or_else(|err| panic!("{s}: {err}")) }
    };
    for (k, ns) in NS.iter().enumerate() {
        for j in 0..4 {
            q(format!(r#"STORE INTO SEMANTIC (key = "{ns}-k{j}", topic = "t{}", n = {}, owner = "{ns}") NAMESPACE "{ns}""#, j % 2, k * 10 + j)).await;
        }
    }
    q(r#"STORE INTO SEMANTIC (key = "plain", topic = "t0", n = 99, owner = "none")"#.to_string()).await;
    q(r#"LINK FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" TO SEMANTIC WHERE key = "demo--y-k0" TYPE "about""#.to_string()).await;
    q(r#"UPDATE INTO SEMANTIC WHERE topic = "t1" (flag = "odd")"#.to_string()).await;
    q(r#"FORGET FROM SEMANTIC WHERE n = 2"#.to_string()).await;
}

const READS: &[&str] = &[
    r#"RECALL FROM SEMANTIC ALL ORDER BY n DESC"#,
    r#"RECALL FROM SEMANTIC ALL ORDER BY n ASC LIMIT 5"#,
    r#"RECALL FROM SEMANTIC ALL AGGREGATE COUNT(*) AS c, SUM(n) AS s, MAX(n) AS m"#,
    r#"RECALL FROM SEMANTIC ALL GROUP BY topic AGGREGATE COUNT(*) AS c ORDER BY topic ASC"#,
    r#"RECALL FROM SEMANTIC WHERE owner = "demo--y" ORDER BY n ASC NAMESPACE "demo--y""#,
    r#"RECALL FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" FOLLOW LINKS ALL DEPTH 1"#,
];

#[tokio::test(flavor = "multi_thread")]
async fn any_node_answers_and_a_stale_map_corrects_itself() {
    std::env::set_var("ADB_S3", "off");
    let ref_dir = tempfile::tempdir().unwrap();
    let (reference, _) = LocalEngine::durable(ref_dir.path()).await.unwrap();
    let reference: Arc<dyn Engine> = Arc::new(reference);
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut places = Vec::new();
    let mut nodes = Vec::new();
    for n in 1..=3u64 {
        let place = Arc::new(Static { me: n, map: RwLock::new(Arc::new(map3())) });
        let node = PartitionedEngine::open_cluster(dirs[n as usize - 1].path(), Arc::clone(&place) as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        peers.add(&format!("node{n}"), &node);
        places.push(place);
        nodes.push(node);
    }
    let owners: BTreeSet<u64> = NS.iter().map(|ns| u64::from(nodes[0].partition_of(ns)) % 3 + 1).collect();
    assert!(owners.len() >= 2, "the namespaces are owned by different nodes: {owners:?}");

    workload(&|_| Arc::clone(&reference)).await;
    let via = |i: usize| -> Arc<dyn Engine> { Arc::new(Arc::clone(&nodes[i % 3])) };
    workload(&via).await;

    for (i, q) in READS.iter().enumerate() {
        let want = stable(&reference.query(q, None).await.unwrap());
        for node in &nodes {
            let got = stable(&node.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}")));
            assert_eq!(want, got, "read {i} through a node: {q}");
        }
    }
    // Each node holds only its own partitions.
    for (k, dir) in dirs.iter().enumerate() {
        let me = k as u64 + 1;
        let held: Vec<Partition> = std::fs::read_dir(dir.path().join("parts")).map(|d| d.flatten().filter_map(|e| e.file_name().to_str()?.strip_prefix('p')?.parse().ok()).collect()).unwrap_or_default();
        assert!(held.iter().all(|p| u64::from(*p) % 3 + 1 == me), "node {me} holds {held:?}");
    }
    // A scope through a node that owns none of it.
    let scope = Reach::Only(vec!["a3t-shared--*".into()]);
    let mine = nodes[2].query_in(r#"RECALL FROM SEMANTIC ALL ORDER BY n ASC"#, None, &scope).await.unwrap();
    let seen: BTreeSet<String> = stable(&mine).as_array().unwrap().iter().map(|r| r["owner"].as_str().unwrap().to_string()).collect();
    assert_eq!(seen, ["a3t-shared--bot", "a3t-shared--ops"].into_iter().map(String::from).collect());

    // A gateway's map goes stale: it names the wrong owner for demo--y's partition.
    let p = nodes[0].partition_of("demo--y");
    let real = u64::from(p) % 3 + 1;
    let gateway = real % 3 + 1;
    let wrong = gateway % 3 + 1;
    assert!(real != gateway && gateway != wrong && wrong != real);
    {
        let place = &places[(gateway - 1) as usize];
        let mut m = (*place.map()).clone();
        m.assignments.get_mut(&p).unwrap().owner = wrong;
        *place.map.write().unwrap() = Arc::new(m);
    }
    let q = r#"RECALL FROM SEMANTIC WHERE owner = "demo--y" ORDER BY n ASC NAMESPACE "demo--y""#;
    let want = stable(&reference.query(q, None).await.unwrap());
    let r = nodes[(gateway - 1) as usize].query(q, None).await;
    assert_eq!(stable(&r.expect("not_owner from the wrong node, retried at the owner it named")), want);
    // A node asked for a partition it doesn't own refuses, naming the owner.
    let e = nodes[(wrong - 1) as usize].serve_query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "demo--y""#, None, &Reach::All).await.unwrap_err();
    assert!(e.starts_with(&format!("not_owner:{real}:node{real}")), "{e}");
}

fn values(answer: &Value, field: &str) -> Vec<String> {
    let recs = answer.pointer("/data/records").and_then(|r| r.as_array()).cloned().unwrap_or_default();
    let text = |x: &Value| x.as_str().map(String::from).or_else(|| x.get(0).and_then(|y| y.as_str()).map(String::from));
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer(&format!("/data/{field}")).and_then(text)).collect();
    v.sort();
    v
}

/// Partition operations across nodes: a memory layer whose space another node owns, a
/// pattern quota over namespaces on different nodes, a branch promoted into another
/// node's partition — all asked through a node owning none of them.
#[tokio::test(flavor = "multi_thread")]
async fn layers_quotas_and_copies_cross_nodes() {
    use adb_core::MemoryType;
    use adb_executor::branches::{Layer, LayerMode};
    std::env::set_var("ADB_S3", "off");
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for n in 1..=3u64 {
        let place = Arc::new(Static { me: n, map: RwLock::new(Arc::new(map3())) });
        let node = PartitionedEngine::open_cluster(dirs[n as usize - 1].path(), place as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        peers.add(&format!("node{n}"), &node);
        nodes.push(node);
    }
    let owner = |ns: &str| u64::from(nodes[0].partition_of(ns)) % 3 + 1;
    let pick = |prefix: &str, f: &dyn Fn(u64) -> bool| -> &'static str { Box::leak((0..1000).map(|i| format!("{prefix}{i}")).find(|n| f(owner(n))).unwrap().into_boxed_str()) };
    let src = pick("compliance", &|o| o == 1);
    let dst = pick("trading", &|o| o == 2);
    let via = &nodes[2]; // node 3 owns neither
    let q = |aql: String| async move { via.query(&aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}")) };

    // A layer read from another node's partition.
    q(format!(r#"STORE INTO SEMANTIC (key = "rule", fact = "limit 1M") NAMESPACE "{src}""#)).await;
    q(format!(r#"STORE INTO SEMANTIC (key = "desk", fact = "desk 4") NAMESPACE "{dst}""#)).await;
    via.tag_create(src, "v1", None).await.unwrap();
    via.layers_set(dst, vec![Layer { space: src.into(), mode: LayerMode::Pin, tag: Some("v1".into()), memories: vec![MemoryType::Semantic], sealed: true }]).await.unwrap();
    q(format!(r#"UPDATE INTO SEMANTIC WHERE key = "rule" (fact = "limit 2M") NAMESPACE "{src}""#)).await;
    let facts = values(&q(format!(r#"RECALL FROM SEMANTIC WHERE fact != "" NAMESPACE "{dst}""#)).await, "fact");
    assert_eq!(facts, ["desk 4", "limit 1M"], "pinned at v1, read from node 1");
    let refused = via.tag_drop(src, "v1").await.unwrap_err();
    assert!(refused.contains("pinned"), "{refused}");

    // A pattern quota over namespaces on two nodes (catalog read on every check here;
    // by default a node may lag another's new namespace by ADB_CATALOG_TTL_MS).
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let a = pick("q--a", &|o| o == 1);
    let b = pick("q--b", &|o| o == 2);
    via.quota_set("q--*", None, Some(3), None).await.unwrap();
    q(format!(r#"STORE INTO SEMANTIC (key = "1") NAMESPACE "{a}""#)).await;
    q(format!(r#"STORE INTO SEMANTIC (key = "2") NAMESPACE "{a}""#)).await;
    q(format!(r#"STORE INTO SEMANTIC (key = "3") NAMESPACE "{b}""#)).await;
    let e = via.query(&format!(r#"STORE INTO SEMANTIC (key = "4") NAMESPACE "{b}""#), None).await.unwrap_err();
    assert!(e.contains("quota_exceeded"), "the other node's usage counts: {e}");

    // A branch promoted into another node's partition.
    let agent = pick("bot", &|o| o == 1);
    let v2 = pick("bot-v2-", &|o| o == 2);
    q(format!(r#"STORE INTO PROCEDURAL (key = "p1", steps = "p1-v1") NAMESPACE "{agent}""#)).await;
    q(format!(r#"STORE INTO EPISODIC (event = "said hello") NAMESPACE "{agent}""#)).await;
    via.branch_create(agent, "try", None, None).await.unwrap();
    q(format!(r#"UPDATE INTO PROCEDURAL WHERE key = "p1" (steps = "p1-branch") NAMESPACE "{agent}" BRANCH "try""#)).await;
    let p = via.branch_promote(agent, "try", v2, false).await.unwrap();
    assert_eq!(p["records"], 1, "{p}");
    assert_eq!(values(&q(format!(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "{v2}""#)).await, "steps"), ["p1-branch"]);
    let eps = values(&q(format!(r#"RECALL FROM EPISODIC ALL NAMESPACE "{v2}" LIMIT 100"#)).await, "event");
    assert_eq!(eps, ["promoted_from", "said hello"], "its origin, and the old agent's episodes from node 1");
    assert_eq!(via.branch_list(Some(agent)).await.unwrap().len(), 1);
}

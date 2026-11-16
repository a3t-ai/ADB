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
        m.apply(&ControlCmd::Join { node: n, addr: format!("node{n}"), flight: None }).unwrap();
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
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
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

/// The superadmin's SQL view on a cluster: asked through any node, it runs on the
/// system partition's owner over every partition's memory rows (gathered from their
/// owners) and the platform tables, and answers as one engine does.
#[tokio::test(flavor = "multi_thread")]
async fn the_sql_view_spans_every_node() {
    std::env::set_var("ADB_S3", "off");
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let ref_dir = tempfile::tempdir().unwrap();
    let (reference, _) = LocalEngine::durable(ref_dir.path()).await.unwrap();
    let reference: Arc<dyn Engine> = Arc::new(reference);
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for n in 1..=3u64 {
        let place = Arc::new(Static { me: n, map: RwLock::new(Arc::new(map3())) });
        let node = PartitionedEngine::open_cluster(dirs[n as usize - 1].path(), place as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        peers.add(&format!("node{n}"), &node);
        nodes.push(node);
    }
    workload(&|_| Arc::clone(&reference)).await;
    let via = |i: usize| -> Arc<dyn Engine> { Arc::new(Arc::clone(&nodes[i % 3])) };
    workload(&via).await;

    let rows = |r: adb_executor::sql::SqlResult| -> Vec<String> {
        let mut out = Vec::new();
        for b in &r.batches {
            let cols: Vec<_> = (0..b.num_columns()).map(|c| datafusion::arrow::util::display::ArrayFormatter::try_new(b.column(c).as_ref(), &Default::default()).unwrap()).collect();
            for i in 0..b.num_rows() {
                out.push(cols.iter().map(|f| f.value(i).to_string()).collect::<Vec<_>>().join("|"));
            }
        }
        out
    };
    let q = "SELECT namespace, count(*) AS n, sum(CASE WHEN tombstone THEN 1 ELSE 0 END) AS gone FROM memory GROUP BY namespace ORDER BY namespace";
    let want = rows(reference.sql("SELECT namespace, count(*) AS n FROM memory_current GROUP BY namespace ORDER BY namespace", None).await.unwrap());
    assert!(want.len() >= 4, "{want:?}");
    let mut catalogs = BTreeSet::new();
    for (i, node) in nodes.iter().enumerate() {
        let got = rows(node.sql("SELECT namespace, count(*) AS n FROM memory_current GROUP BY namespace ORDER BY namespace", None).await.unwrap_or_else(|e| panic!("node {}: {e}", i + 1)));
        assert_eq!(got, want, "memory_current through node {}", i + 1);
        // The raw view and a platform table answer too.
        assert!(!rows(node.sql(q, None).await.unwrap()).is_empty());
        let catalog = rows(node.sql("SELECT count(*) FROM adb_namespaces", None).await.unwrap_or_else(|e| panic!("node {}: {e}", i + 1)));
        assert_ne!(catalog, vec!["0".to_string()], "the catalog through node {}", i + 1);
        catalogs.insert(catalog);
    }
    assert_eq!(catalogs.len(), 1, "every node reads the same catalog: {catalogs:?}");
}

/// `WATCH` with no NAMESPACE on a cluster: asked through one node, it runs on the
/// system partition's owner across the partitions within the call's scope; writes to
/// namespaces on other nodes reach its frames, read through a third node; UNWATCH
/// from any node ends it.
#[tokio::test(flavor = "multi_thread")]
async fn a_watch_across_partitions_sees_every_node() {
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
    // Namespaces on at least two nodes (and a write before the watch: a catalog).
    let owners: BTreeSet<u64> = NS.iter().map(|ns| u64::from(nodes[0].partition_of(ns)) % 3 + 1).collect();
    assert!(owners.len() >= 2);
    for ns in NS {
        nodes[0].query(&format!(r#"STORE INTO SEMANTIC (key = "{ns}-seed", topic = "seed") NAMESPACE "{ns}""#), None).await.unwrap();
    }
    let sys = u64::from(PARTITIONS) % 3 + 1;
    // 0: takes the watch, 2: reads its frames (neither runs it); 1: the system
    // partition's owner, where the writes go in.
    let via = |k: u64| match k {
        0 => &nodes[(sys % 3) as usize],
        1 => &nodes[(sys - 1) as usize],
        _ => &nodes[((sys + 1) % 3) as usize],
    };
    let w = via(0).query(r#"WATCH RECALL FROM SEMANTIC WHERE topic = "w" DEBOUNCE 50ms"#, None).await.unwrap();
    let snap = &w["data"];
    let snap = if snap["cq_id"].is_string() { snap.clone() } else { w["data"]["value"].clone() };
    let id = snap["cq_id"].as_str().unwrap_or_else(|| panic!("a watch id: {w}")).to_string();
    assert!(id.starts_with("c."), "{id}");
    let scoped = nodes[0]
        .query_in(r#"WATCH RECALL FROM SEMANTIC WHERE topic = "w" DEBOUNCE 50ms"#, None, &Reach::Only(vec!["a3t-shared--*".into()]))
        .await
        .unwrap();
    let scoped_snap = if scoped["data"]["cq_id"].is_string() { scoped["data"].clone() } else { scoped["data"]["value"].clone() };
    let scoped_id = scoped_snap["cq_id"].as_str().unwrap().to_string();

    for ns in NS {
        via(1).query(&format!(r#"STORE INTO SEMANTIC (key = "{ns}-w", topic = "w") NAMESPACE "{ns}""#), None).await.unwrap();
    }
    // Frames through the third node until every write is in.
    let keys_in = |frames: &Value, kind: &str| -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for f in frames.as_array().unwrap() {
            let list = if f["type"] == "diff" { f[kind].clone() } else { f["result"]["data"]["records"].clone() };
            for r in list.as_array().cloned().unwrap_or_default() {
                if let Some(k) = r.pointer("/data/key").and_then(|k| k.as_str()) {
                    out.insert(k.to_string());
                }
            }
        }
        out
    };
    let collect = |cq: String, want: usize| {
        let reader = Arc::clone(via(2));
        async move {
            let mut from = None;
            let mut seen = BTreeSet::new();
            let t0 = std::time::Instant::now();
            while seen.len() < want && t0.elapsed() < std::time::Duration::from_secs(30) {
                let frames = reader.watch_frames(&cq, from, 100, std::time::Duration::from_secs(2)).await.unwrap();
                if let Some(last) = frames.as_array().unwrap().last() {
                    from = last["offset"].as_u64();
                }
                seen.extend(keys_in(&frames, "added"));
            }
            (seen, from)
        }
    };
    let (seen, from) = collect(id.clone(), NS.len()).await;
    let want: BTreeSet<String> = NS.iter().map(|ns| format!("{ns}-w")).collect();
    assert_eq!(seen, want, "the watch across partitions saw every node's write");
    let (scoped_seen, _) = collect(scoped_id.clone(), 2).await;
    let scoped_want: BTreeSet<String> = ["a3t-shared--bot-w", "a3t-shared--ops-w"].into_iter().map(String::from).collect();
    assert_eq!(scoped_seen, scoped_want, "the scoped watch saw only its scope");

    // A FORGET on another node: the next frame removes it.
    via(1).query(r#"FORGET FROM SEMANTIC WHERE key = "demo--y-w" NAMESPACE "demo--y""#, None).await.unwrap();
    let mut removed = false;
    let mut from = from;
    for _ in 0..15 {
        let frames = via(2).watch_frames(&id, from, 100, std::time::Duration::from_secs(2)).await.unwrap();
        if let Some(last) = frames.as_array().unwrap().last() {
            from = last["offset"].as_u64();
        }
        if frames.as_array().unwrap().iter().any(|f| f["removed"].as_array().is_some_and(|r| !r.is_empty())) {
            removed = true;
            break;
        }
    }
    assert!(removed, "the forgotten record left the watch");

    // The system partition's owner restarts: the watch is registered there and runs
    // again; a reader resuming gets a reset, then the frames go on.
    {
        let owner = (sys - 1) as usize;
        let old = nodes.remove(owner);
        peers.remove(&format!("node{sys}"));
        drop(old);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let place = Arc::new(Static { me: sys, map: RwLock::new(Arc::new(map3())) });
        let node = PartitionedEngine::open_cluster(dirs[owner].path(), place as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        peers.add(&format!("node{sys}"), &node);
        nodes.insert(owner, node);
    }
    let via = |k: u64| match k {
        0 => &nodes[(sys % 3) as usize],
        1 => &nodes[(sys - 1) as usize],
        _ => &nodes[((sys + 1) % 3) as usize],
    };
    let reset = via(2).watch_frames(&id, from, 10, std::time::Duration::from_secs(5)).await.unwrap();
    assert_eq!(reset[0]["type"], "reset", "{reset}");
    let mut from = reset[0]["offset"].as_u64();
    via(0).query(r#"STORE INTO SEMANTIC (key = "after-restart", topic = "w") NAMESPACE "a3t-shared--ops""#, None).await.unwrap();
    let mut seen = false;
    for _ in 0..15 {
        let frames = via(2).watch_frames(&id, from, 100, std::time::Duration::from_secs(2)).await.unwrap();
        if let Some(last) = frames.as_array().unwrap().last() {
            from = last["offset"].as_u64();
        }
        if keys_in(&frames, "added").contains("after-restart") {
            seen = true;
            break;
        }
    }
    assert!(seen, "the watch runs again after its owner restarted");

    // UNWATCH through a node that doesn't run it.
    let u = via(1).query(&format!("UNWATCH {}", serde_json::to_string(&id).unwrap()), None).await.unwrap();
    assert!(u.to_string().contains("\"unwatched\":true"), "{u}");
    let e = via(2).watch_frames(&id, None, 10, std::time::Duration::from_millis(10)).await.unwrap_err();
    assert!(e.contains("unknown watch"), "{e}");
}

/// A merged entity's key (GR-12) on a cluster: a LOOKUP naming no namespace runs
/// across partitions and still finds the entity it merged into, through every node.
#[tokio::test(flavor = "multi_thread")]
async fn a_merged_key_resolves_across_partitions() {
    std::env::set_var("ADB_S3", "off");
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let ref_dir = tempfile::tempdir().unwrap();
    let (reference, _) = LocalEngine::durable(ref_dir.path()).await.unwrap();
    let reference: Arc<dyn Engine> = Arc::new(reference);
    let peers = LocalPeers::new();
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for n in 1..=3u64 {
        let place = Arc::new(Static { me: n, map: RwLock::new(Arc::new(map3())) });
        let node = PartitionedEngine::open_cluster(dirs[n as usize - 1].path(), place as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        peers.add(&format!("node{n}"), &node);
        nodes.push(node);
    }
    workload(&|_| Arc::clone(&reference)).await;
    let via = |i: usize| -> Arc<dyn Engine> { Arc::new(Arc::clone(&nodes[i % 3])) };
    workload(&via).await;
    let steps = [
        r#"STORE INTO SEMANTIC (key = "person:ada-lovelace", born = 1815)"#,
        r#"STORE INTO SEMANTIC (key = "person:ada", field = "maths")"#,
        r#"MERGE ENTITY "person:ada-lovelace" INTO "person:ada""#,
    ];
    for (i, s) in steps.iter().enumerate() {
        reference.query(s, None).await.unwrap();
        via(i).query(s, None).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let q = r#"LOOKUP FROM SEMANTIC KEY id = "person:ada-lovelace""#;
    let want = stable(&reference.query(q, None).await.unwrap());
    assert_eq!(want.as_array().map(|a| a.len()), Some(1), "one engine finds the survivor: {want}");
    for (i, node) in nodes.iter().enumerate() {
        let got = stable(&node.query(q, None).await.unwrap());
        assert_eq!(got, want, "through node {}", i + 1);
    }
}

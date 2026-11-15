//! S3 option B step 3: a node's 64 partitions behind one engine.
//!
//! The same workload — several namespaces, records without one, links across
//! namespaces, a FORGET without NAMESPACE — runs on one single-partition engine (the
//! reference) and on the partitioned engine; every read answers the same. Then a call
//! scoped to `a3t-shared--*` sees only its own namespaces: a statement naming another
//! is refused, one naming none stays inside the scope, and links into another
//! namespace are not followed.
//!
//! Its own test binary: object storage off for the process.

use adb_cluster::map::{ControlCmd, PartitionMap};
use adb_cluster::routing::Reach;
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

const NS: [&str; 3] = ["a3t-shared--bot", "a3t-shared--ops", "demo--y"];

fn map64() -> PartitionMap {
    let mut m = PartitionMap::default();
    m.apply(&ControlCmd::Init { partitions: 64 }).unwrap();
    m
}

/// A statement's answer without what differs between engines (ids, times).
fn stable(v: &Value) -> Value {
    let data = &v["data"];
    match data["type"].as_str() {
        Some("records") => Value::Array(data["records"].as_array().unwrap().iter().map(|r| r["data"].clone()).collect()),
        Some("aggregation") => data["values"].clone(),
        Some("affected") => data["count"].clone(),
        _ => data.clone(),
    }
}

async fn workload(e: &dyn Engine) {
    let q = |s: String| async move { e.query(&s, None).await.unwrap_or_else(|err| panic!("{s}: {err}")) };
    for (i, ns) in NS.iter().enumerate() {
        for j in 0..6 {
            q(format!(r#"STORE INTO SEMANTIC (key = "{ns}-k{j}", topic = "t{}", n = {}, owner = "{ns}") NAMESPACE "{ns}""#, j % 2, i * 10 + j)).await;
        }
        q(format!(r#"STORE INTO EPISODIC (key = "{ns}-e", campaign = "summer", n = {i}) NAMESPACE "{ns}""#)).await;
    }
    // No namespace.
    q(r#"STORE INTO SEMANTIC (key = "plain", topic = "t0", n = 99, owner = "none")"#.to_string()).await;
    // A link inside one namespace, and one across namespaces (bot → demo).
    q(r#"LINK FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" TO SEMANTIC WHERE key = "a3t-shared--bot-k1" TYPE "next""#.to_string()).await;
    q(r#"LINK FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" TO SEMANTIC WHERE key = "demo--y-k0" TYPE "about""#.to_string()).await;
    // Updates and a FORGET spanning namespaces.
    q(r#"UPDATE INTO SEMANTIC WHERE topic = "t1" (flag = "odd")"#.to_string()).await;
    q(r#"FORGET FROM SEMANTIC WHERE n = 5"#.to_string()).await;
}

const READS: &[&str] = &[
    r#"RECALL FROM SEMANTIC ALL ORDER BY n DESC"#,
    r#"RECALL FROM SEMANTIC ALL ORDER BY n ASC LIMIT 4"#,
    r#"RECALL FROM SEMANTIC WHERE topic = "t1" ORDER BY n ASC"#,
    r#"RECALL FROM SEMANTIC ALL AGGREGATE COUNT(*) AS c, SUM(n) AS s, MAX(n) AS m"#,
    r#"RECALL FROM SEMANTIC ALL GROUP BY topic AGGREGATE COUNT(*) AS c ORDER BY topic ASC"#,
    r#"RECALL FROM EPISODIC WHERE campaign = "summer" ORDER BY n ASC"#,
    r#"RECALL FROM SEMANTIC WHERE owner = "a3t-shared--bot" ORDER BY n ASC NAMESPACE "a3t-shared--bot""#,
    r#"RECALL FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" FOLLOW LINKS ALL DEPTH 1"#,
    r#"RECALL FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" WITH LINKS ALL"#,
];

#[tokio::test(flavor = "multi_thread")]
async fn partitions_answer_as_one_engine_and_scopes_hold() {
    std::env::set_var("ADB_S3", "off");
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let ref_dir = tempfile::tempdir().unwrap();
    let (reference, _) = LocalEngine::durable(ref_dir.path()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let node = PartitionedEngine::open(dir.path(), map64()).await.unwrap();
    let parts: std::collections::BTreeSet<u16> = NS.iter().map(|n| node.partition_of(n)).collect();
    assert!(parts.len() >= 2, "the namespaces land in different partitions: {parts:?}");

    workload(&reference).await;
    workload(&node).await;

    for q in READS {
        let want = reference.query(q, None).await.unwrap_or_else(|e| panic!("reference {q}: {e}"));
        let got = node.query(q, None).await.unwrap_or_else(|e| panic!("partitioned {q}: {e}"));
        let (w, g) = (stable(&want), stable(&got));
        eprintln!("{q}\n  → {g}");
        assert_eq!(w, g, "{q}");
    }
    let catalog = node.catalog();
    for ns in NS.iter().chain(&[""]) {
        assert_eq!(catalog.get(*ns), Some(&node.partition_of(ns)), "catalog has {ns:?}");
    }

    // Scoped to a3t-shared--*.
    let scope = Reach::Only(vec!["a3t-shared--*".into()]);
    let mine = node.query_in(r#"RECALL FROM SEMANTIC ALL ORDER BY n ASC"#, None, &scope).await.unwrap();
    let owners: std::collections::BTreeSet<String> = stable(&mine).as_array().unwrap().iter().map(|r| r["owner"].as_str().unwrap().to_string()).collect();
    assert_eq!(owners, ["a3t-shared--bot", "a3t-shared--ops"].into_iter().map(String::from).collect(), "only its namespaces");
    let refused = node.query_in(r#"RECALL FROM SEMANTIC ALL NAMESPACE "demo--y""#, None, &scope).await.unwrap_err();
    assert!(refused.contains("forbidden"), "{refused}");
    let refused = node.query_in(r#"STORE INTO SEMANTIC (key = "x") NAMESPACE "demo--y""#, None, &scope).await.unwrap_err();
    assert!(refused.contains("forbidden"), "{refused}");
    let refused = node.query_in(r#"STORE INTO SEMANTIC (key = "x")"#, None, &scope).await.unwrap_err();
    assert!(refused.contains("forbidden"), "no namespace is outside a3t-shared--*: {refused}");
    // The link into demo--y is not followed for this scope; the one inside is.
    let followed = node.query_in(r#"RECALL FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" FOLLOW LINKS ALL DEPTH 1"#, None, &scope).await.unwrap();
    let text = followed.to_string();
    assert!(text.contains("a3t-shared--bot-k1"), "{followed}");
    assert!(!text.contains("demo--y"), "nothing of demo--y: {followed}");
    let listed = node.query_in(r#"RECALL FROM SEMANTIC WHERE key = "a3t-shared--bot-k0" WITH LINKS ALL"#, None, &scope).await.unwrap();
    assert!(listed.to_string().contains("a3t-shared--bot-k1") && !listed.to_string().contains("demo--y"), "no link to demo--y listed: {listed}");
    // A FORGET without NAMESPACE stays in scope.
    let n = node.query_in(r#"FORGET FROM SEMANTIC WHERE topic = "t0""#, None, &scope).await.unwrap();
    assert!(stable(&n).as_u64().unwrap() > 0);
    let demo = node.query(r#"RECALL FROM SEMANTIC WHERE topic = "t0" NAMESPACE "demo--y""#, None).await.unwrap();
    assert!(!stable(&demo).as_array().unwrap().is_empty(), "demo--y untouched");
    let plain = node.query(r#"RECALL FROM SEMANTIC WHERE key = "plain""#, None).await.unwrap();
    assert_eq!(stable(&plain).as_array().unwrap().len(), 1, "no-namespace record untouched");
}

/// Two namespaces in one partition: reads and writes by record id answer for the
/// whole partition, so the scope (not the partition) must decide, and each partition
/// is asked once.
#[tokio::test(flavor = "multi_thread")]
async fn a_shared_partition_never_leaks_across_the_scope() {
    std::env::set_var("ADB_S3", "off");
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let dir = tempfile::tempdir().unwrap();
    let node = PartitionedEngine::open(dir.path(), map64()).await.unwrap();
    let home = node.partition_of("a3t-shared--bot");
    let find = |prefix: &str| (0..10_000).map(|i| format!("{prefix}{i}")).find(|n| node.partition_of(n) == home).unwrap();
    let mate = find("a3t-shared--c"); // in scope, same partition
    let other = find("demo--z"); // out of scope, same partition
    let q = |s: String| {
        let node = std::sync::Arc::clone(&node);
        async move { node.query(&s, None).await.unwrap_or_else(|e| panic!("{s}: {e}")) }
    };
    q(r#"STORE INTO SEMANTIC (key = "a", n = 1) NAMESPACE "a3t-shared--bot""#.into()).await;
    q(format!(r#"STORE INTO SEMANTIC (key = "b", n = 2) NAMESPACE "{mate}""#)).await;
    q(format!(r#"STORE INTO SEMANTIC (key = "secret", n = 3) NAMESPACE "{other}""#)).await;

    let scope = Reach::Only(vec!["a3t-shared--*".into()]);
    // Linking to the out-of-scope record finds no target.
    let e = node.query_in(r#"LINK FROM SEMANTIC WHERE key = "a" TO SEMANTIC WHERE key = "secret" TYPE "x""#, None, &scope).await;
    assert!(e.is_err() || e.as_ref().unwrap().to_string().contains("\"count\":0"), "no link into {other}: {e:?}");
    let all = node.query(r#"RECALL FROM SEMANTIC WHERE key = "a" WITH LINKS ALL"#, None).await.unwrap();
    assert!(!all.to_string().contains("secret"), "nothing linked: {all}");
    // A link inside the scope, across two namespaces of one partition: once.
    node.query_in(r#"LINK FROM SEMANTIC WHERE key = "a" TO SEMANTIC WHERE key = "b" TYPE "x""#, None, &scope).await.unwrap();
    let listed = node.query_in(r#"RECALL FROM SEMANTIC WHERE key = "a" WITH LINKS ALL"#, None, &scope).await.unwrap();
    assert_eq!(listed["data"]["records"][0]["data"]["links"].as_array().map(|l| l.len()), Some(1), "one link, not one per namespace: {listed}");
    let gone = node.query_in(r#"UNLINK FROM SEMANTIC WHERE key = "a" TO SEMANTIC WHERE key = "b" TYPE "x""#, None, &scope).await.unwrap();
    assert_eq!(gone["data"]["count"], 1, "counted once: {gone}");
    // Reads by id don't return the other namespace's record either.
    let mine = node.query_in(r#"RECALL FROM SEMANTIC ALL"#, None, &scope).await.unwrap();
    assert!(!mine.to_string().contains("secret"), "{mine}");
}

fn values(answer: &Value, field: &str) -> Vec<String> {
    let recs = answer.pointer("/data/records").and_then(|r| r.as_array()).cloned().unwrap_or_default();
    // PROCEDURAL keeps `steps` as a list: its first step stands for the record here.
    let text = |x: &Value| x.as_str().map(String::from).or_else(|| x.get(0).and_then(|y| y.as_str()).map(String::from));
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer(&format!("/data/{field}")).and_then(text)).collect();
    v.sort();
    v
}

/// B7 memory layers across partitions: a space reads another space that lives in a
/// different partition (as agents read a3t-docs), pinned or mounted, sealed, ranked
/// by LIKE, reached by FORGET; a pinned tag can't be dropped while another
/// partition's space reads it; layers survive a restart.
#[tokio::test(flavor = "multi_thread")]
async fn memory_layers_reach_across_partitions() {
    use adb_core::MemoryType;
    use adb_executor::branches::{Layer, LayerMode};
    std::env::set_var("ADB_S3", "off");
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let dir = tempfile::tempdir().unwrap();
    let node = PartitionedEngine::open(dir.path(), map64()).await.unwrap();
    let (src, dst) = ("compliance", (0..1000).map(|i| format!("trading{i}")).find(|n| node.partition_of(n) != node.partition_of("compliance")).unwrap());
    assert_ne!(node.partition_of(src), node.partition_of(&dst));
    let dst: &str = Box::leak(dst.into_boxed_str());
    let q = |aql: String| {
        let node = std::sync::Arc::clone(&node);
        async move { node.query(&aql, None).await }
    };
    let fact = |ns: &'static str| {
        let node = std::sync::Arc::clone(&node);
        async move { values(&node.query(&format!(r#"RECALL FROM SEMANTIC WHERE fact != "" NAMESPACE "{ns}" LIMIT 100"#), None).await.unwrap(), "fact") }
    };
    q(format!(r#"STORE INTO SEMANTIC (key = "rule:max-trade", fact = "limit 1M") NAMESPACE "{src}""#)).await.unwrap();
    q(format!(r#"STORE INTO PROCEDURAL (key = "check_trade", pattern = "check a trade", steps = "limits") NAMESPACE "{src}""#)).await.unwrap();
    q(format!(r#"STORE INTO EPISODIC (key = "audit-1", event = "reviewed desk 4") NAMESPACE "{src}""#)).await.unwrap();
    node.tag_create(src, "v1", None).await.unwrap();
    q(format!(r#"UPDATE INTO SEMANTIC WHERE key = "rule:max-trade" (fact = "limit 2M", content = "trade limit is 2M") NAMESPACE "{src}""#)).await.unwrap();
    q(format!(r#"STORE INTO SEMANTIC (key = "desk", fact = "desk 4", content = "desk 4 trades bonds") NAMESPACE "{dst}""#)).await.unwrap();

    let layer = |mode: LayerMode| Layer {
        space: src.into(),
        mode,
        tag: (mode == LayerMode::Pin).then(|| "v1".into()),
        memories: vec![MemoryType::Semantic, MemoryType::Procedural],
        sealed: true,
    };
    assert!(node.layers_set(dst, vec![Layer { tag: Some("nope".into()), ..layer(LayerMode::Pin) }]).await.is_err(), "the tag is checked in the layer's partition");
    node.layers_set(dst, vec![layer(LayerMode::Pin)]).await.unwrap();

    assert_eq!(fact(dst).await, ["desk 4", "limit 1M"], "pinned at v1, read from the other partition");
    let procs = q(format!(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "{dst}""#)).await.unwrap();
    assert_eq!(procs.pointer("/data/count").and_then(|c| c.as_u64()), Some(1), "the inherited procedure: {procs}");
    assert!(q(format!(r#"STORE INTO SEMANTIC (key = "rule:max-trade", fact = "limit 9M") NAMESPACE "{dst}""#)).await.is_err(), "sealed: can't shadow");
    let refused = node.tag_drop(src, "v1").await.unwrap_err();
    assert!(refused.contains("pinned"), "a pin in another partition holds the tag: {refused}");

    node.layers_set(dst, vec![layer(LayerMode::Mount)]).await.unwrap();
    assert_eq!(fact(dst).await, ["desk 4", "limit 2M"], "mounted: current");
    let vars: std::collections::HashMap<String, Value> = [("q".to_string(), serde_json::json!("trade limit"))].into();
    let like = node.query(&format!(r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "{dst}" LIMIT 5"#), Some(&vars)).await.unwrap();
    let found = values(&like, "fact");
    assert!(found.contains(&"limit 2M".to_string()) && found.contains(&"desk 4".to_string()), "the layer's and its own: {like}");
    q(format!(r#"FORGET FROM SEMANTIC WHERE key = "rule:max-trade" NAMESPACE "{src}""#)).await.unwrap();
    assert_eq!(fact(dst).await, ["desk 4"], "FORGET on the layer reaches the mount");

    drop(node);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let node = PartitionedEngine::open(dir.path(), map64()).await.unwrap();
    assert_eq!(node.layers_get(dst).await.unwrap().len(), 1, "layers survive a restart");
    let procs = node.query(&format!(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "{dst}""#), None).await.unwrap();
    assert_eq!(procs.pointer("/data/count").and_then(|c| c.as_u64()), Some(1), "and are read again: {procs}");
}

/// Copies between namespaces in different partitions: clone (now and at a tag),
/// procedures only, and promoting a branch — read out of the source's partition,
/// materialised in the target's, with the promoted agent's history layer reading
/// the old agent's episodes from the old partition.
#[tokio::test(flavor = "multi_thread")]
async fn copies_cross_partitions() {
    use adb_executor::branches::PointRef;
    std::env::set_var("ADB_S3", "off");
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let dir = tempfile::tempdir().unwrap();
    let node = PartitionedEngine::open(dir.path(), map64()).await.unwrap();
    let elsewhere = |prefix: &str| -> &'static str {
        let n = (0..1000).map(|i| format!("{prefix}{i}")).find(|n| node.partition_of(n) != node.partition_of("bot")).unwrap();
        Box::leak(n.into_boxed_str())
    };
    let (v2, clone, at, procs) = (elsewhere("bot-v2-"), elsewhere("bot-clone-"), elsewhere("bot-at-"), elsewhere("bot-procs-"));
    let q = |aql: String| {
        let node = std::sync::Arc::clone(&node);
        async move { node.query(&aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}")) }
    };
    let steps = |ns: &'static str, branch: Option<&'static str>| {
        let node = std::sync::Arc::clone(&node);
        async move {
            let b = branch.map(|b| format!(r#" BRANCH "{b}""#)).unwrap_or_default();
            values(&node.query(&format!(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "{ns}"{b} LIMIT 100"#), None).await.unwrap(), "steps")
        }
    };
    for k in ["p1", "p2"] {
        q(format!(r#"STORE INTO PROCEDURAL (key = "{k}", steps = "{k}-v1") NAMESPACE "bot""#)).await;
    }
    q(r#"STORE INTO SEMANTIC (key = "fact1", fact = "likes tea") NAMESPACE "bot""#.into()).await;
    q(r#"STORE INTO EPISODIC (event = "said hello") NAMESPACE "bot""#.into()).await;
    node.tag_create("bot", "v1", None).await.unwrap();
    q(r#"UPDATE INTO PROCEDURAL WHERE key = "p1" (steps = "p1-v2") NAMESPACE "bot""#.into()).await;

    // Clone now: SEMANTIC, PROCEDURAL, TOOLS — no episodes.
    let c = node.space_clone("bot", clone).await.unwrap();
    assert_eq!(c["records"], 3, "{c}");
    assert_eq!(steps(clone, None).await, ["p1-v2", "p2-v1"]);
    let eps = q(format!(r#"RECALL FROM EPISODIC ALL NAMESPACE "{clone}""#)).await;
    assert_eq!(eps["data"]["count"], 0);
    // Clone at the tag: the older procedure.
    let c = node.space_clone_at("bot", at, &PointRef { tag: Some("v1".into()), ..Default::default() }).await.unwrap();
    assert_eq!(steps(at, None).await, ["p1-v1", "p2-v1"], "{c}");
    // Procedures only.
    let c = node.space_copy_procedures("bot", procs, false).await.unwrap();
    assert_eq!(c["records"], 2, "{c}");
    let sem = q(format!(r#"RECALL FROM SEMANTIC ALL NAMESPACE "{procs}""#)).await;
    assert_eq!(sem["data"]["count"], 0, "no facts travel");

    // Promote a branch into another partition.
    node.branch_create("bot", "try", None, None).await.unwrap();
    q(r#"UPDATE INTO PROCEDURAL WHERE key = "p2" (steps = "p2-branch") NAMESPACE "bot" BRANCH "try""#.into()).await;
    assert_eq!(steps("bot", Some("try")).await, ["p1-v2", "p2-branch"]);
    let p = node.branch_promote("bot", "try", v2, false).await.unwrap();
    assert_eq!(p["records"], 3, "{p}");
    assert_eq!(steps(v2, None).await, ["p1-v2", "p2-branch"]);
    assert_eq!(steps("bot", None).await, ["p1-v2", "p2-v1"], "the old agent is untouched");
    q(r#"STORE INTO EPISODIC (event = "old agent later") NAMESPACE "bot""#.into()).await;
    q(format!(r#"STORE INTO EPISODIC (event = "new agent said hi") NAMESPACE "{v2}""#)).await;
    let eps = values(&q(format!(r#"RECALL FROM EPISODIC ALL NAMESPACE "{v2}" LIMIT 100"#)).await, "event");
    assert_eq!(eps, ["new agent said hi", "promoted_from", "said hello"], "own, origin, and the old agent's (from its partition) as of the promote");
    // Behind main: refused unless allow_stale, as on one engine.
    node.branch_create("bot", "behind", None, None).await.unwrap();
    q(r#"UPDATE INTO PROCEDURAL WHERE key = "p2" (steps = "p2-main") NAMESPACE "bot""#.into()).await;
    let err = node.branch_promote("bot", "behind", elsewhere("bot-v3-"), false).await.unwrap_err();
    assert!(err.starts_with("merge_stale:"), "{err}");
}

/// Quotas in a cluster (option A, user 14 Nov): kept once in the system partition and
/// read by every partition. A pattern quota counts its namespaces in every partition;
/// one namespace's quota is exact. Cache rules are cluster-wide: a partition opened
/// later gets them too.
#[tokio::test(flavor = "multi_thread")]
async fn quotas_and_settings_span_partitions() {
    std::env::set_var("ADB_S3", "off");
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    let dir = tempfile::tempdir().unwrap();
    let node = PartitionedEngine::open(dir.path(), map64()).await.unwrap();
    let a = "acme--a";
    let b: &'static str = Box::leak((0..1000).map(|i| format!("acme--b{i}")).find(|n| node.partition_of(n) != node.partition_of(a)).unwrap().into_boxed_str());
    let q = |aql: String| {
        let node = std::sync::Arc::clone(&node);
        async move { node.query(&aql, None).await }
    };
    node.quota_set("acme--*", None, Some(5), None).await.unwrap();
    node.quota_set(a, None, Some(3), None).await.unwrap();
    assert_eq!(node.quota_list().await.unwrap().len(), 2, "kept once, in the system partition");

    for i in 0..3 {
        q(format!(r#"STORE INTO SEMANTIC (key = "k{i}") NAMESPACE "{a}""#)).await.unwrap();
    }
    let e = q(format!(r#"STORE INTO SEMANTIC (key = "k3") NAMESPACE "{a}""#)).await.unwrap_err();
    assert!(e.contains("quota_exceeded") && e.contains(a), "the namespace's own quota, exact: {e}");
    q(format!(r#"STORE INTO SEMANTIC (key = "k0") NAMESPACE "{b}""#)).await.unwrap();
    q(format!(r#"STORE INTO SEMANTIC (key = "k1") NAMESPACE "{b}""#)).await.unwrap();
    // 3 in a's partition + 2 in b's = the pattern's 5: the next one in b is refused.
    let e = q(format!(r#"STORE INTO SEMANTIC (key = "k2") NAMESPACE "{b}""#)).await.unwrap_err();
    assert!(e.contains("quota_exceeded") && e.contains("acme--*"), "the pattern counts the other partition: {e}");
    q(r#"STORE INTO SEMANTIC (key = "free") NAMESPACE "other--x""#.into()).await.unwrap();
    assert!(node.quota_drop("acme--*").await.unwrap());
    q(format!(r#"STORE INTO SEMANTIC (key = "k2") NAMESPACE "{b}""#)).await.unwrap();

    // Cache rules: applied to the open partitions, copied into one opened later.
    node.set_cache(serde_json::json!({ "max_bytes": "16MB", "rules": [{ "memory": "semantic", "keep": "all" }] })).await.unwrap();
    let late: &'static str = Box::leak((0..1000).map(|i| format!("late{i}")).find(|n| {
        let p = node.partition_of(n);
        ![node.partition_of(a), node.partition_of(b), node.partition_of("other--x")].contains(&p)
    }).unwrap().into_boxed_str());
    q(format!(r#"STORE INTO SEMANTIC (key = "x") NAMESPACE "{late}""#)).await.unwrap();
    let late_dir = dir.path().join("parts").join(format!("p{}", node.partition_of(late)));
    let rules = adb_store::CacheConfig::load(&late_dir).unwrap().expect("the late partition has the cache rules");
    assert_eq!(rules.rules.len(), 1);
}

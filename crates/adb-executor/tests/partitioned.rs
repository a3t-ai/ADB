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

//! W10: reads through the hot cache, in shadow mode (each cached answer is also
//! read from Lance; a difference fails the query). Tight rules, so buckets are
//! evicted from and every path runs: complete buckets, newest-first answers from
//! an incomplete one, fallbacks to Lance, KEY by lineage, UPDATE / FORGET targets,
//! links by key, `LIKE` (ranked from complete buckets), a rules change. Own test
//! binary: it sets `ADB_CACHE`.

use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

async fn n(engine: &LocalEngine, aql: &str) -> usize {
    let v = q(engine, aql).await;
    v["data"]["records"].as_array().map(|a| a.len()).unwrap_or_else(|| panic!("{aql}: {v}"))
}

const READS: &[&str] = &[
    r#"RECALL FROM EPISODIC ALL LIMIT 3"#,
    r#"RECALL FROM EPISODIC ALL LIMIT 3 IN SCOPE "s1""#,
    r#"RECALL FROM EPISODIC ALL LIMIT 40"#,
    r#"RECALL FROM EPISODIC WHERE n > 5 LIMIT 2"#,
    r#"RECALL FROM EPISODIC WHERE n < 3 LIMIT 2"#,
    r#"RECALL FROM EPISODIC ALL ORDER BY n DESC LIMIT 3"#,
    r#"RECALL FROM EPISODIC KEY id = "e29""#,
    r#"RECALL FROM EPISODIC KEY id = "e0""#,
    r#"RECALL FROM EPISODIC ALL AGGREGATE COUNT(*) AS c"#,
    r#"SCAN FROM WORKING WINDOW LAST 3"#,
    r#"SCAN FROM WORKING WINDOW LAST 15"#,
    r#"SCAN FROM WORKING"#,
    r#"RECALL FROM WORKING KEY id = "w19""#,
    r#"RECALL FROM WORKING WHERE i >= 10"#,
    r#"LOOKUP FROM SEMANTIC KEY id = "f24""#,
    r#"LOOKUP FROM SEMANTIC KEY id = "f2""#,
    r#"RECALL FROM SEMANTIC WHERE topic = "t1""#,
    r#"RECALL FROM SEMANTIC WHERE topic = "t1" LIMIT 2"#,
    r#"RECALL FROM SEMANTIC KEY id = "f20" FOLLOW LINKS TYPE "about""#,
    r#"RECALL FROM SEMANTIC WHERE topic = "t0" WITH LINKS ALL"#,
    r#"RECALL FROM PROCEDURAL PATTERN "restart the service" THRESHOLD 0.2"#,
    r#"LOOKUP FROM PROCEDURAL KEY pattern_id = "p3""#,
    r#"LOAD FROM TOOLS WHERE category = "c1" LIMIT 3"#,
];

/// `LIKE $q` reads (text: word ranking; no embedder in this test).
const LIKES: &[(&str, &str)] = &[
    (r#"RECALL FROM SEMANTIC LIKE $q LIMIT 3"#, "topic t1"),
    (r#"RECALL FROM SEMANTIC LIKE $q LIMIT 50"#, "t2"),
    (r#"RECALL FROM EPISODIC LIKE $q LIMIT 5 IN SCOPE "s2""#, "event 2"),
    (r#"RECALL FROM EPISODIC LIKE $q LIMIT 5"#, "event"),
    (r#"RECALL FROM PROCEDURAL LIKE $q LIMIT 2"#, "restart service"),
];

async fn reads(engine: &LocalEngine) {
    for r in READS {
        q(engine, r).await;
    }
    for (r, text) in LIKES {
        let vars = std::collections::HashMap::from([("q".to_string(), json!(text))]);
        engine.query(r, Some(&vars)).await.unwrap_or_else(|e| panic!("{r}: {e}"));
    }
}

#[tokio::test]
async fn reads_through_the_cache_match_lance() {
    std::env::set_var("ADB_CACHE", "shadow");
    std::env::remove_var("ADB_CACHE_RULES");
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let rules = json!({ "max_bytes": "16MB", "rules": [
        { "memory": "episodic", "scope": "s2", "keep": "all" },
        { "memory": "episodic", "keep": { "last": 5 } },
        { "memory": "working", "keep": { "last": 4 } },
        { "memory": "semantic", "keep": { "last": 10 } },
        { "memory": "procedural", "keep": "all" },
        { "memory": "tools", "keep": "all" },
    ]});
    engine.set_cache(rules.clone()).await.unwrap();

    for i in 0..30 {
        let scope = ["s1", "s2", "s3"][i % 3];
        q(&engine, &format!(r#"STORE INTO EPISODIC (key = "e{i}", n = {i}, content = "event {i}") IN SCOPE "{scope}""#)).await;
        if i % 7 == 6 {
            reads(&engine).await;
        }
    }
    for i in 0..20 {
        q(&engine, &format!(r#"STORE INTO WORKING (key = "w{i}", i = {i})"#)).await;
    }
    for i in 0..25 {
        q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "f{i}", topic = "t{}", n = {i}, content = "fact {i} about topic t{}")"#, i % 3, i % 3)).await;
    }
    for i in 0..6 {
        q(&engine, &format!(r#"STORE INTO PROCEDURAL (pattern_id = "p{i}", pattern = "restart the service {i}", steps = "check; restart")"#)).await;
        q(&engine, &format!(r#"STORE INTO TOOLS (tool_id = "tool{i}", name = "tool {i}", category = "c{}", ranking = 0.{i})"#, i % 2)).await;
    }
    for i in 20..25 {
        q(&engine, &format!(r#"LINK FROM SEMANTIC WHERE key = "f{i}" TO EPISODIC WHERE key = "e{}" TYPE "about""#, i + 5)).await;
    }
    reads(&engine).await;

    // Updates and forgets (UPDATE / FORGET targets come from the cache too).
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE topic = "t2" (n = 100)"#).await;
    q(&engine, r#"UPDATE INTO EPISODIC WHERE key = "e28" (note = "annotated")"#).await;
    q(&engine, r#"UPDATE INTO WORKING WHERE i >= 18 (i = 0)"#).await;
    q(&engine, r#"FORGET FROM EPISODIC WHERE key = "e29""#).await;
    q(&engine, r#"FORGET FROM SEMANTIC WHERE n = 100"#).await;
    reads(&engine).await;
    assert_eq!(n(&engine, r#"RECALL FROM EPISODIC KEY id = "e29""#).await, 0);
    assert_eq!(n(&engine, r#"RECALL FROM EPISODIC ALL LIMIT 3"#).await, 3);

    let c = engine.cache().await.unwrap();
    let st = &c["stats"];
    assert_eq!(st["mismatches"], 0, "{c}");
    assert!(st["hits"].as_u64().unwrap() > 50, "{c}");
    assert!(st["misses"].as_u64().unwrap() > 5, "{c}");

    // A rules change rebuilds the cache from the table (its counters start again).
    engine.set_cache(json!({ "max_bytes": "16MB", "rules": [
        { "memory": "episodic", "keep": { "last": 2 } },
        { "memory": "semantic", "keep": "all" },
        { "memory": "working", "keep": { "bytes": "600B" } },
    ]})).await.unwrap();
    reads(&engine).await;

    let c = engine.cache().await.unwrap();
    let st = &c["stats"];
    assert_eq!(c["mode"], "shadow");
    assert_eq!(st["mismatches"], 0, "{c}");
    assert!(st["hits"].as_u64().unwrap() > 5, "{c}");
    assert!(st["misses"].as_u64().unwrap() > 5, "{c}");
    assert!(st["buckets"].as_array().unwrap().iter().any(|b| b["complete"] == false), "{c}");
    std::env::remove_var("ADB_CACHE");
    let _ = rules;
}

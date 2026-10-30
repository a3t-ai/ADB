//! Integration tests for the full AQL pipeline
//!
//! Tests the complete flow: AQL query string -> parser -> planner -> executor -> results

use std::sync::Arc;

use adb_core::{MemoryType, Scope};
use adb_executor::{ExecutorResult, LocalEngine, QueryResult};
use serde_json::json;

/// A temporary engine, seeded through its write path (W7: no in-memory backends).
#[derive(Clone)]
struct Mem(Arc<LocalEngine>);

impl Mem {
    async fn new() -> Self {
        Mem(Arc::new(LocalEngine::temporary().await.unwrap()))
    }

    async fn store(&self, memory: MemoryType, key: &str, data: serde_json::Value) -> adb_core::AdbResult<adb_core::MemoryRecord> {
        self.0.executor().writes().store(memory, key, data, Scope::Private, None, None, None).await
    }

    async fn link(&self, from: MemoryType, from_id: &str, to: MemoryType, to_id: &str, link_type: &str, weight: f32) -> adb_core::AdbResult<adb_core::Link> {
        self.0.executor().writes().link(from, from_id, to, to_id, link_type, Some(weight)).await
    }

    async fn get_links_from(&self, from: MemoryType, id: &str, link_type: Option<&str>) -> adb_core::AdbResult<Vec<adb_core::Link>> {
        self.0.executor().reads().links_from(from, id, link_type).await
    }

    async fn execute(&self, aql: &str) -> ExecutorResult<QueryResult> {
        self.0.executor().execute(aql).await
    }
}

/// Test helper to create executor with pre-populated data
async fn create_test_executor() -> Mem {
    let adb = Mem::new().await;

    // Populate working memory
    adb.store(MemoryType::Working, "task-1", json!({"status": "active", "priority": 5}))
        .await
        .unwrap();
    adb.store(MemoryType::Working, "task-2", json!({"status": "pending", "priority": 3}))
        .await
        .unwrap();
    adb.store(MemoryType::Working, "task-3", json!({"status": "completed", "priority": 8}))
        .await
        .unwrap();

    // Populate tools memory
    adb.store(
        MemoryType::Tools,
        "read-file",
        json!({
            "name": "Read File",
            "description": "Reads file contents from disk",
            "category": "file",
            "ranking": 0.9
        }),
    )
    .await
    .unwrap();
    adb.store(
        MemoryType::Tools,
        "write-file",
        json!({
            "name": "Write File",
            "description": "Writes content to a file",
            "category": "file",
            "ranking": 0.85
        }),
    )
    .await
    .unwrap();

    // Populate procedural memory
    adb.store(
        MemoryType::Procedural,
        "oom-fix",
        json!({
            "pattern": "OOMKilled container memory exceeded",
            "steps": ["Check memory limits", "Increase memory allocation", "Restart pod"],
            "severity": "critical"
        }),
    )
    .await
    .unwrap();

    adb
}

// ============================================================================
// SCAN tests
// ============================================================================

#[tokio::test]
async fn test_scan_all() {
    let executor = create_test_executor().await;

    let result = executor.execute("SCAN FROM WORKING").await.unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 3);
}

#[tokio::test]
async fn test_scan_with_limit() {
    let executor = create_test_executor().await;

    let result = executor.execute("SCAN FROM WORKING LIMIT 2").await.unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 2);
}

// ============================================================================
// RECALL tests
// ============================================================================

#[tokio::test]
async fn test_recall_with_filter() {
    let executor = create_test_executor().await;

    let result = executor
        .execute(r#"RECALL FROM WORKING WHERE status = "active""#)
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, "task-1");
}

#[tokio::test]
async fn test_recall_with_comparison() {
    let executor = create_test_executor().await;

    let result = executor
        .execute("RECALL FROM WORKING WHERE priority > 4")
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 2); // task-1 (5) and task-3 (8)
}

#[tokio::test]
async fn test_recall_all() {
    let executor = create_test_executor().await;

    // Note: RECALL requires WHERE clause in AQL grammar, so use a condition that matches all
    let result = executor
        .execute("RECALL FROM WORKING WHERE priority >= 0")
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 3);
}

// ============================================================================
// LOOKUP tests
// ============================================================================

#[tokio::test]
async fn test_lookup_by_key() {
    let executor = create_test_executor().await;

    // AQL v0.5 §19: LOOKUP is not valid on WORKING memory
    assert!(executor.execute(r#"LOOKUP FROM WORKING KEY id = "task-2""#).await.is_err());

    let result = executor
        .execute(r#"LOOKUP FROM PROCEDURAL KEY id = "oom-fix""#)
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, "oom-fix");
    assert_eq!(records[0].get_str("severity"), Some("critical"));
}

// ============================================================================
// STORE tests
// ============================================================================

#[tokio::test]
async fn test_store_new_record() {
    let executor = create_test_executor().await;

    let result = executor
        .execute(r#"STORE INTO WORKING (key = "new-task", status = "created", priority = 1)"#)
        .await
        .unwrap();

    assert!(result.success);

    // Verify it was stored
    let scan_result = executor.execute("SCAN FROM WORKING").await.unwrap();
    let records = scan_result.get_records().unwrap();
    assert_eq!(records.len(), 4); // 3 original + 1 new
}

// ============================================================================
// FORGET tests
// ============================================================================

#[tokio::test]
async fn test_forget_with_filter() {
    let executor = create_test_executor().await;

    let result = executor
        .execute(r#"FORGET FROM WORKING WHERE status = "completed""#)
        .await
        .unwrap();

    assert!(result.success);
    let count = result.get_count().unwrap();
    assert_eq!(count, 1);

    // Verify it was deleted
    let scan_result = executor.execute("SCAN FROM WORKING").await.unwrap();
    let records = scan_result.get_records().unwrap();
    assert_eq!(records.len(), 2);
}

// ============================================================================
// LOAD (tools) tests
// ============================================================================

#[tokio::test]
async fn test_load_tools() {
    let executor = create_test_executor().await;

    // LOAD requires WHERE clause - use condition that matches all
    let result = executor
        .execute(r#"LOAD FROM TOOLS WHERE category = "file" LIMIT 2"#)
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 2);
    // Should be sorted by ranking (highest first)
    assert_eq!(records[0].id, "read-file");
}

// ============================================================================
// PIPELINE tests
// ============================================================================

#[tokio::test]
async fn test_pipeline_execution() {
    let executor = create_test_executor().await;

    // Pipeline with proper syntax - LOAD requires WHERE clause
    let result = executor
        .execute(r#"PIPELINE test_pipe TIMEOUT 50ms SCAN FROM WORKING LIMIT 1 | LOAD FROM TOOLS WHERE category = "file" LIMIT 1"#)
        .await
        .unwrap();

    assert!(result.success);

    // Check that we got pipeline results
    if let adb_executor::ResultSet::Pipeline { steps } = result.data {
        assert_eq!(steps.len(), 2);
        assert!(steps[0].success);
        assert!(steps[1].success);
    } else {
        panic!("Expected pipeline result set");
    }
}

// ============================================================================
// Cross-memory type tests
// ============================================================================

#[tokio::test]
async fn test_procedural_recall() {
    let executor = create_test_executor().await;

    let result = executor
        .execute(r#"RECALL FROM PROCEDURAL WHERE severity = "critical""#)
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, "oom-fix");
}

// ============================================================================
// Error handling tests
// ============================================================================

#[tokio::test]
async fn test_parse_error() {
    let executor = create_test_executor().await;

    let result = executor.execute("INVALID QUERY SYNTAX").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_empty_result() {
    let executor = create_test_executor().await;

    let result = executor
        .execute(r#"RECALL FROM WORKING WHERE status = "nonexistent""#)
        .await
        .unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    assert_eq!(records.len(), 0);
}

// ============================================================================
// FOLLOW LINKS tests
// ============================================================================

#[tokio::test]
async fn test_follow_links() {
    let adb = Mem::new().await;

    // Store source record in PROCEDURAL
    adb.store(
        MemoryType::Procedural,
        "oom-fix",
        json!({
            "pattern": "OOMKilled",
            "steps": ["Check memory", "Increase limit"]
        }),
    ).await.unwrap();

    // Store target record in EPISODIC
    adb.store(
        MemoryType::Episodic,
        "inc-001",
        json!({
            "incident_type": "OOM",
            "pod": "api-server"
        }),
    ).await.unwrap();

    // Create link from procedural to episodic
    adb.link(
        MemoryType::Procedural,
        "oom-fix",
        MemoryType::Episodic,
        "inc-001",
        "applied_to",
        0.95,
    ).await.unwrap();

    // Execute FOLLOW LINKS query
    let executor = adb.clone();
    let result = executor.execute(
        r#"RECALL FROM PROCEDURAL WHERE pattern = "OOMKilled" FOLLOW LINKS TYPE "applied_to""#
    ).await.unwrap();

    assert!(result.success);
    let records = result.get_records().unwrap();
    // FOLLOW LINKS should return the target records (inc-001), not the source (oom-fix)
    assert_eq!(records.len(), 1, "Expected 1 target record from FOLLOW LINKS");
    assert_eq!(records[0].id, "inc-001");
    assert_eq!(records[0].data["pod"], "api-server");
}

/// SEMANTIC records `ids` and `next` links between them, `(from, to, weight)`.
async fn graph(ids: &[&str], links: &[(&str, &str, f32)]) -> Mem {
    let adb = Mem::new().await;
    for id in ids {
        adb.store(MemoryType::Semantic, id, json!({ "name": id })).await.unwrap();
    }
    for (from, to, w) in links {
        adb.link(MemoryType::Semantic, from, MemoryType::Semantic, to, "next", *w).await.unwrap();
    }
    adb
}

/// `(id, _hop, _path_weight)` of each record `aql` returns.
async fn reached(adb: &Mem, aql: &str) -> Vec<(String, u64, f64)> {
    let result = adb.execute(aql).await.unwrap();
    result
        .get_records()
        .unwrap()
        .iter()
        .map(|r| (r.id.clone(), r.data["_hop"].as_u64().unwrap(), r.data["_path_weight"].as_f64().unwrap()))
        .collect()
}

fn ids(r: &[(String, u64, f64)]) -> Vec<&str> {
    r.iter().map(|(id, _, _)| id.as_str()).collect()
}

/// P1_GRAPH D1 (GR-1): DEPTH n returns everything within n hops, nearest first.
#[tokio::test]
async fn follow_links_depth_walks_a_chain() {
    let adb = graph(&["a", "b", "c", "d"], &[("a", "b", 1.0), ("b", "c", 0.5), ("c", "d", 0.5)]).await;
    let q = |d: u32| format!(r#"RECALL FROM SEMANTIC WHERE name = "a" FOLLOW LINKS TYPE "next" DEPTH {d}"#);
    assert_eq!(ids(&reached(&adb, &q(1)).await), ["b"]);
    assert_eq!(ids(&reached(&adb, &q(2)).await), ["b", "c"]);
    let all = reached(&adb, &q(3)).await;
    assert_eq!(all, [("b".into(), 1, 1.0), ("c".into(), 2, 0.5), ("d".into(), 3, 0.25)]);
    // Without DEPTH: one hop, as before.
    let one = adb.execute(r#"RECALL FROM SEMANTIC WHERE name = "a" FOLLOW LINKS TYPE "next""#).await.unwrap();
    assert_eq!(one.get_records().unwrap().len(), 1);
}

/// Cycles end; the start is never returned; each record once.
#[tokio::test]
async fn follow_links_is_cycle_safe() {
    let adb = graph(&["a", "b", "c"], &[("a", "b", 1.0), ("b", "c", 1.0), ("c", "a", 1.0), ("c", "b", 1.0)]).await;
    let r = reached(&adb, r#"RECALL FROM SEMANTIC WHERE name = "a" FOLLOW LINKS TYPE "next" DEPTH 5"#).await;
    assert_eq!(ids(&r), ["b", "c"]);
}

/// A record reached two ways at one hop counts once, at its best path weight; each
/// hop is ordered by path weight.
#[tokio::test]
async fn follow_links_orders_by_path_weight() {
    let adb = graph(
        &["a", "b", "c", "d", "e"],
        &[("a", "b", 0.2), ("a", "c", 0.9), ("b", "d", 1.0), ("c", "d", 0.5), ("b", "e", 1.0)],
    )
    .await;
    let r = reached(&adb, r#"RECALL FROM SEMANTIC WHERE name = "a" FOLLOW LINKS TYPE "next" DEPTH 2"#).await;
    assert_eq!(ids(&r), ["c", "b", "d", "e"]);
    assert!((r[2].2 - 0.45).abs() < 1e-6, "{r:?}");
    assert!((r[3].2 - 0.2).abs() < 1e-6, "{r:?}");
}

/// DEPTH beyond the cap stops at MAX_DEPTH.
#[tokio::test]
async fn follow_links_depth_is_capped() {
    let names: Vec<String> = (0..8).map(|i| format!("n{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let links: Vec<(&str, &str, f32)> = refs.windows(2).map(|w| (w[0], w[1], 1.0)).collect();
    let adb = graph(&refs, &links).await;
    let r = reached(&adb, r#"RECALL FROM SEMANTIC WHERE name = "n0" FOLLOW LINKS TYPE "next" DEPTH 50"#).await;
    assert_eq!(r.len(), adb_executor::MAX_DEPTH as usize);
}

/// L6 / GR-7: LOOKUP … FOLLOW LINKS returns the linked records, not the source.
#[tokio::test]
async fn lookup_follow_links_returns_targets() {
    let adb = graph(&["a", "b", "c"], &[("a", "b", 1.0), ("b", "c", 1.0)]).await;
    let r = reached(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "a" FOLLOW LINKS TYPE "next" DEPTH 2"#).await;
    assert_eq!(ids(&r), ["b", "c"]);
}

/// REFLECT … FOLLOW LINKS honours DEPTH too.
#[tokio::test]
async fn reflect_follow_links_depth() {
    let adb = graph(&["a", "b", "c"], &[("a", "b", 1.0), ("b", "c", 1.0)]).await;
    let result = adb
        .execute(r#"REFLECT FROM SEMANTIC WHERE name = "a" FOLLOW LINKS TYPE "next" DEPTH 2"#)
        .await
        .unwrap();
    let text = serde_json::to_string(&result.data).unwrap();
    assert!(text.contains("\"c\""), "{text}");
}

/// SEMANTIC records `ids` and typed links `(from, to, type, weight)`.
async fn typed_graph(ids: &[&str], links: &[(&str, &str, &str, f32)]) -> Mem {
    let adb = graph(ids, &[]).await;
    for (from, to, t, w) in links {
        adb.link(MemoryType::Semantic, from, MemoryType::Semantic, to, t, *w).await.unwrap();
    }
    adb
}

/// AQL v0.8 (GR-2): INBOUND walks links backwards, BOTH either way.
#[tokio::test]
async fn follow_links_inbound_and_both() {
    let adb = graph(&["a", "b", "c", "x"], &[("a", "b", 1.0), ("b", "c", 1.0), ("x", "b", 0.5)]).await;
    let q = |s: &str| format!(r#"RECALL FROM SEMANTIC WHERE name = "b" FOLLOW LINKS TYPE "next" {s}"#);
    assert_eq!(ids(&reached(&adb, &q("")).await), ["c"]);
    assert_eq!(ids(&reached(&adb, &q("INBOUND")).await), ["a", "x"]);
    assert_eq!(ids(&reached(&adb, &q("BOTH")).await), ["a", "c", "x"]);
    // c ← b ← a, two hops back.
    let r = reached(&adb, r#"RECALL FROM SEMANTIC WHERE name = "c" FOLLOW LINKS INBOUND DEPTH 2"#).await;
    assert_eq!(ids(&r), ["b", "a", "x"]);
}

/// AQL v0.8 (GR-3): MIN_WEIGHT drops light links; TYPE IN and ALL pick types.
#[tokio::test]
async fn follow_links_min_weight_and_types() {
    let adb = typed_graph(
        &["a", "b", "c", "d"],
        &[("a", "b", "knows", 0.9), ("a", "c", "knows", 0.3), ("a", "d", "works_with", 0.8)],
    )
    .await;
    let q = |s: &str| format!(r#"RECALL FROM SEMANTIC WHERE name = "a" FOLLOW LINKS {s}"#);
    assert_eq!(ids(&reached(&adb, &q(r#"TYPE "knows""#)).await), ["b", "c"]);
    assert_eq!(ids(&reached(&adb, &q(r#"TYPE "knows" MIN_WEIGHT 0.5"#)).await), ["b"]);
    assert_eq!(ids(&reached(&adb, &q(r#"TYPE IN ("knows", "works_with") MIN_WEIGHT 0.5"#)).await), ["b", "d"]);
    assert_eq!(ids(&reached(&adb, &q("ALL")).await), ["b", "d", "c"]);
    assert_eq!(ids(&reached(&adb, &q("")).await), ["b", "d", "c"]);
}

/// WITH LINKS INBOUND lists the links pointing at a record, with their source.
#[tokio::test]
async fn with_links_inbound_lists_sources() {
    let adb = typed_graph(&["a", "b", "c"], &[("a", "c", "next", 1.0), ("b", "c", "next", 0.2), ("c", "a", "next", 1.0)]).await;
    let result = adb
        .execute(r#"LOOKUP FROM SEMANTIC KEY id = "c" WITH LINKS ALL INBOUND MIN_WEIGHT 0.5"#)
        .await
        .unwrap();
    let records = result.get_records().unwrap();
    let links = records[0].data["links"].as_array().unwrap();
    assert_eq!(links.len(), 1, "{links:?}");
    assert_eq!(links[0]["from_id"], "a");
    assert_eq!(links[0]["to_id"], "c");
}

/// REFLECT … FOLLOW LINKS TYPE follows only that type.
#[tokio::test]
async fn reflect_follow_links_respects_type() {
    let adb = typed_graph(&["a", "b", "c"], &[("a", "b", "next", 1.0), ("a", "c", "other", 1.0)]).await;
    let result = adb.execute(r#"REFLECT FROM SEMANTIC WHERE name = "a" FOLLOW LINKS TYPE "next""#).await.unwrap();
    let text = serde_json::to_string(&result.data).unwrap();
    assert!(text.contains("\"b\"") && !text.contains("\"c\""), "{text}");
}

async fn keys(adb: &Mem, aql: &str) -> Vec<String> {
    let result = adb.execute(aql).await.unwrap_or_else(|e| panic!("{aql}: {e}"));
    result.get_records().unwrap().iter().map(|r| r.id.clone()).collect()
}

/// P1.4 (P1_GRAPH D2): a fact with an entity object is a link of its predicate;
/// its entities are created; a new fact for (subject, predicate) supersedes.
#[tokio::test]
async fn facts_are_links_between_entities() {
    let adb = Mem::new().await;
    let fact = |s: &str, p: &str, o: &str| format!(r#"STORE INTO SEMANTIC FACT (subject = "{s}", predicate = "{p}", object = "{o}", confidence = 0.8)"#);
    adb.execute(&fact("person:ada", "works_at", "org:acme")).await.unwrap();
    // Entities made for both ends.
    assert_eq!(keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "org:acme""#).await, ["org:acme"]);
    let follow = r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS TYPE "works_at""#;
    assert_eq!(keys(&adb, follow).await, ["org:acme"]);
    let r = adb.execute(follow).await.unwrap();
    assert!((r.get_records().unwrap()[0].data["_path_weight"].as_f64().unwrap() - 0.8).abs() < 1e-6);

    // A new fact for (person:ada, works_at) supersedes the old one, link and all.
    adb.execute(&fact("person:ada", "works_at", "org:globex")).await.unwrap();
    assert_eq!(keys(&adb, follow).await, ["org:globex"]);
    assert_eq!(keys(&adb, r#"RECALL FROM SEMANTIC WHERE predicate = "works_at""#).await.len(), 1);
    // Inbound: who works at globex; nobody at acme any more.
    assert_eq!(keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "org:globex" FOLLOW LINKS INBOUND"#).await, ["person:ada"]);
    assert!(keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "org:acme" FOLLOW LINKS INBOUND"#).await.is_empty());

    // MULTI: one fact per object.
    let multi = |o: &str| format!(r#"STORE INTO SEMANTIC FACT MULTI (subject = "person:ada", predicate = "knows", object = "{o}")"#);
    adb.execute(&multi("person:bob")).await.unwrap();
    adb.execute(&multi("person:cy")).await.unwrap();
    adb.execute(&multi("person:bob")).await.unwrap();
    let mut known = keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS TYPE "knows""#).await;
    known.sort();
    assert_eq!(known, ["person:bob", "person:cy"]);

    // Two hops over facts: bob ← ada → globex.
    adb.execute(&fact("org:globex", "based_in", "city:london")).await.unwrap();
    let r = keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS TYPE IN ("works_at", "based_in") DEPTH 2"#).await;
    assert_eq!(r, ["org:globex", "city:london"]);

    // A literal value is a fact, not a link.
    adb.execute(r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "born", value = 1815)"#).await.unwrap();
    let born = adb.execute(r#"RECALL FROM SEMANTIC WHERE predicate = "born""#).await.unwrap();
    assert_eq!(born.get_records().unwrap()[0].data["value"], 1815);
    assert_eq!(keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS TYPE "born""#).await.len(), 0);

    // FORGET a fact: its link goes.
    adb.execute(r#"FORGET FROM SEMANTIC WHERE predicate = "based_in""#).await.unwrap();
    let r = keys(&adb, r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS TYPE IN ("works_at", "based_in") DEPTH 2"#).await;
    assert_eq!(r, ["org:globex"]);
}

/// FACT is SEMANTIC only, and needs subject, predicate and object or value.
#[tokio::test]
async fn fact_statements_are_checked() {
    let adb = Mem::new().await;
    for bad in [
        r#"STORE INTO EPISODIC FACT (subject = "a", predicate = "p", object = "b")"#,
        r#"STORE INTO SEMANTIC FACT (subject = "a", predicate = "p")"#,
    ] {
        assert!(adb.execute(bad).await.is_err(), "{bad}");
    }
}

// ============================================================================
// HAVING with alias tests
// ============================================================================

#[tokio::test]
async fn test_having_with_alias() {
    let adb = Mem::new().await;

    // Store some records
    adb.store(
        MemoryType::Episodic,
        "e1",
        json!({"strategy": "tech_news", "ctr": 0.05}),
    ).await.unwrap();
    adb.store(
        MemoryType::Episodic,
        "e2",
        json!({"strategy": "tech_news", "ctr": 0.03}),
    ).await.unwrap();
    adb.store(
        MemoryType::Episodic,
        "e3",
        json!({"strategy": "sports", "ctr": 0.08}),
    ).await.unwrap();

    let executor = adb.clone();

    // Test HAVING with alias - should return the count
    let result = executor.execute(
        r#"RECALL FROM EPISODIC WHERE strategy = "tech_news" AGGREGATE COUNT(*) AS total HAVING total > 0"#
    ).await.unwrap();

    assert!(result.success, "Query should succeed");
    // Should have the aggregate result with the aliased key
    if let adb_executor::ResultSet::Aggregation { values } = &result.data {
        assert!(values.is_object(), "Result should be an object");
        let obj = values.as_object().unwrap();
        assert!(obj.contains_key("total"), "Result should have 'total' key");
        assert_eq!(obj["total"], 2, "Count should be 2");
    } else {
        panic!("Expected Aggregation result");
    }
}

/// AQL v0.9 `GROUP BY`: one row per group; HAVING, ORDER BY, LIMIT and RETURN on
/// the groups; a missing field groups as null; dotted fields nest.
#[tokio::test]
async fn group_by_aggregates_per_group() {
    let adb = Mem::new().await;
    for (i, (url, clicks)) in [("a", 1), ("b", 5), ("a", 3), ("c", 2), ("b", 4), ("a", 2)].iter().enumerate() {
        adb.execute(&format!(r#"STORE INTO EPISODIC (n = {i}, kind = "visit", url = "{url}", clicks = {clicks})"#)).await.unwrap();
    }
    adb.execute(r#"STORE INTO EPISODIC (n = 9, kind = "visit", clicks = 7)"#).await.unwrap();
    let rows = |q: &'static str| {
        let adb = adb.clone();
        async move {
            let r = adb.execute(q).await.unwrap();
            let adb_executor::ResultSet::Aggregation { values } = r.data else { panic!("{q}: not an aggregation") };
            values.as_array().cloned().unwrap_or_else(|| panic!("{q}: {values}"))
        }
    };

    let all = rows(r#"RECALL FROM EPISODIC WHERE kind = "visit" GROUP BY url AGGREGATE COUNT(*) AS n, SUM(clicks) AS clicks ORDER BY clicks DESC"#).await;
    assert_eq!(
        all,
        vec![
            json!({"url": "b", "n": 2, "clicks": 9.0}),
            json!({"url": null, "n": 1, "clicks": 7.0}),
            json!({"url": "a", "n": 3, "clicks": 6.0}),
            json!({"url": "c", "n": 1, "clicks": 2.0}),
        ]
    );
    let top = rows(r#"RECALL FROM EPISODIC WHERE kind = "visit" GROUP BY url AGGREGATE COUNT(*) AS n HAVING n > 1 ORDER BY n DESC LIMIT 1 RETURN url"#).await;
    assert_eq!(top, vec![json!({"url": "a"})]);
    assert!(rows(r#"RECALL FROM EPISODIC WHERE kind = "visit" GROUP BY url AGGREGATE COUNT(*) AS n HAVING n > 5"#).await.is_empty());
    // Two fields, one dotted.
    let two = rows(r#"RECALL FROM EPISODIC WHERE url = "a" GROUP BY url, metadata.scope AGGREGATE MAX(clicks) AS top"#).await;
    assert_eq!(two.len(), 1);
    assert_eq!(two[0]["url"], "a");
    assert!(two[0]["metadata"]["scope"].is_string(), "{}", two[0]);
    assert_eq!(two[0]["top"], 3.0);
    // Without GROUP BY: one object, as before.
    let r = adb.execute(r#"RECALL FROM EPISODIC WHERE kind = "visit" AGGREGATE COUNT(*) AS n"#).await.unwrap();
    let adb_executor::ResultSet::Aggregation { values } = r.data else { panic!() };
    assert_eq!(values, json!({"n": 7}));
}

/// AQL v0.9 `WINDOW` on `RECALL FROM EPISODIC`: over episodes in commit order,
/// before ORDER BY / LIMIT; results newest first as usual.
#[tokio::test]
async fn window_on_episodic_recall() {
    let adb = Mem::new().await;
    for (n, sev) in [(1, 3), (2, 9), (3, 1), (4, 7), (5, 5)] {
        adb.execute(&format!(r#"STORE INTO EPISODIC (pod = "api", n = {n}, sev = {sev})"#)).await.unwrap();
    }
    let ns = |q: &'static str| {
        let adb = adb.clone();
        async move {
            let r = adb.execute(q).await.unwrap();
            r.get_records().unwrap_or_else(|| panic!("{q}")).iter().map(|r| r.data["n"].as_i64().unwrap()).collect::<Vec<_>>()
        }
    };
    assert_eq!(ns(r#"RECALL FROM EPISODIC WHERE pod = "api" WINDOW LAST 2"#).await, [5, 4]);
    assert_eq!(ns(r#"RECALL FROM EPISODIC WHERE pod = "api" WINDOW LAST 1h"#).await, [5, 4, 3, 2, 1]);
    assert_eq!(ns(r#"RECALL FROM EPISODIC WHERE pod = "api" WINDOW SINCE n = 3"#).await, [5, 4, 3]);
    let mut top = ns(r#"RECALL FROM EPISODIC WHERE pod = "api" WINDOW TOP 2 BY sev"#).await;
    top.sort();
    assert_eq!(top, [2, 4]);
    // The window first, then ORDER BY and LIMIT over what it kept.
    assert_eq!(ns(r#"RECALL FROM EPISODIC WHERE pod = "api" WINDOW LAST 3 ORDER BY sev DESC LIMIT 1"#).await, [4]);
    // And before aggregates.
    let r = adb.execute(r#"RECALL FROM EPISODIC WHERE pod = "api" WINDOW LAST 3 AGGREGATE COUNT(*) AS c, MAX(sev) AS top"#).await.unwrap();
    let adb_executor::ResultSet::Aggregation { values } = r.data else { panic!() };
    assert_eq!(values, json!({"c": 3, "top": 7.0}));
}

/// AQL v0.9: `field = null` in UPDATE removes the field, on superseding types
/// (SEMANTIC) and patched ones (EPISODIC, WORKING).
#[tokio::test]
async fn update_null_removes_a_field() {
    let adb = Mem::new().await;
    adb.execute(r#"STORE INTO SEMANTIC (key = "s1", concept = "c", note = "old", n = 1)"#).await.unwrap();
    adb.execute(r#"STORE INTO EPISODIC (ep = "e1", note = "old", n = 1)"#).await.unwrap();
    adb.execute(r#"STORE INTO WORKING (key = "w1", note = "old", n = 1)"#).await.unwrap();
    for (memory, field, id) in [("SEMANTIC", "concept", "c"), ("EPISODIC", "ep", "e1"), ("WORKING", "key", "w1")] {
        let r = adb.execute(&format!(r#"UPDATE INTO {memory} WHERE {field} = "{id}" (note = null, n = 2)"#)).await.unwrap();
        assert!(r.success, "{memory}: {r:?}");
        let r = adb.execute(&format!(r#"RECALL FROM {memory} WHERE {field} = "{id}""#)).await.unwrap();
        let recs = r.get_records().unwrap();
        assert_eq!(recs.len(), 1, "{memory}");
        let data = &recs[0].data;
        assert!(data.get("note").is_none(), "{memory}: note still there: {data}");
        assert_eq!(data["n"], 2, "{memory}: {data}");
        // A removed field no longer matches.
        let r = adb.execute(&format!(r#"RECALL FROM {memory} WHERE note = "old""#)).await.unwrap();
        assert!(r.get_records().unwrap().is_empty(), "{memory}");
    }
}

// ============================================================================
// LINK tests (B16: Link validation)
// ============================================================================

#[tokio::test]
async fn test_link_creates_link_when_records_exist() {
    let adb = Mem::new().await;

    // Store source record in PROCEDURAL
    adb.store(
        MemoryType::Procedural,
        "oom-fix",
        json!({
            "pattern_id": "oom-fix",
            "pattern": "OOMKilled",
            "steps": ["Check memory", "Increase limit"]
        }),
    ).await.unwrap();

    // Store target record in EPISODIC
    adb.store(
        MemoryType::Episodic,
        "inc-001",
        json!({
            "incident_id": "inc-001",
            "incident_type": "OOM",
            "pod": "api-server"
        }),
    ).await.unwrap();

    let executor = adb.clone();

    // LINK statement should succeed
    let result = executor.execute(
        r#"LINK FROM PROCEDURAL WHERE pattern_id = "oom-fix" TO EPISODIC WHERE incident_id = "inc-001" TYPE "applied_to" WEIGHT 0.95"#
    ).await.unwrap();

    assert!(result.success, "LINK should succeed when both records exist");

    // Verify link was created
    let links = adb.get_links_from(MemoryType::Procedural, "oom-fix", Some("applied_to")).await.unwrap();
    assert_eq!(links.len(), 1, "One link should be created");
    assert_eq!(links[0].to_id, "inc-001");
    assert!((links[0].weight - 0.95).abs() < 0.01);
}

#[tokio::test]
async fn test_link_fails_when_source_not_found() {
    let adb = Mem::new().await;

    // Only store target record, no source
    adb.store(
        MemoryType::Episodic,
        "inc-001",
        json!({
            "incident_id": "inc-001",
            "incident_type": "OOM"
        }),
    ).await.unwrap();

    let executor = adb.clone();

    // LINK statement should fail - no source records
    let result = executor.execute(
        r#"LINK FROM PROCEDURAL WHERE pattern_id = "nonexistent" TO EPISODIC WHERE incident_id = "inc-001" TYPE "applied_to""#
    ).await;

    assert!(result.is_err(), "LINK should fail when source records don't exist");
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("No source records found"), "Error should mention source records: {}", err_msg);
}

#[tokio::test]
async fn test_link_fails_when_target_not_found() {
    let adb = Mem::new().await;

    // Only store source record, no target
    adb.store(
        MemoryType::Procedural,
        "oom-fix",
        json!({
            "pattern_id": "oom-fix",
            "pattern": "OOMKilled"
        }),
    ).await.unwrap();

    let executor = adb.clone();

    // LINK statement should fail - no target records
    let result = executor.execute(
        r#"LINK FROM PROCEDURAL WHERE pattern_id = "oom-fix" TO EPISODIC WHERE incident_id = "nonexistent" TYPE "applied_to""#
    ).await;

    assert!(result.is_err(), "LINK should fail when target records don't exist");
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("No target records found"), "Error should mention target records: {}", err_msg);
}

// ============================================================================
// PIPELINE variable binding tests (B29)
// ============================================================================

#[tokio::test]
async fn test_pipeline_variable_binding() {
    // B29: Test that {var} in later stages binds to values from earlier stages
    let adb = Mem::new().await;

    // Store a record in WORKING with a "target_pod" field
    adb.store(
        MemoryType::Working,
        "task-1",
        json!({
            "name": "Find incidents",
            "target_pod": "payments"
        }),
    ).await.unwrap();

    // Store records in EPISODIC with different "pod" values
    adb.store(
        MemoryType::Episodic,
        "inc-001",
        json!({
            "pod": "payments",
            "severity": "critical"
        }),
    ).await.unwrap();

    adb.store(
        MemoryType::Episodic,
        "inc-002",
        json!({
            "pod": "auth",
            "severity": "warning"
        }),
    ).await.unwrap();

    let executor = adb.clone();

    // Pipeline: First stage gets target_pod from WORKING,
    // second stage uses {target_pod} to filter EPISODIC
    let result = executor.execute(
        r#"PIPELINE var_test TIMEOUT 50ms RECALL FROM WORKING WHERE name = "Find incidents" | RECALL FROM EPISODIC WHERE pod = {target_pod}"#
    ).await.unwrap();

    assert!(result.success, "Pipeline should succeed");

    // Check pipeline results
    if let adb_executor::ResultSet::Pipeline { steps } = result.data {
        assert_eq!(steps.len(), 2, "Should have 2 pipeline steps");

        // First step should return task-1
        assert!(steps[0].success);
        let first_records = steps[0].get_records().unwrap();
        assert_eq!(first_records.len(), 1);
        assert_eq!(first_records[0].get_str("target_pod"), Some("payments"));

        // Second step should return only inc-001 (pod = "payments")
        assert!(steps[1].success);
        let second_records = steps[1].get_records().unwrap();
        assert_eq!(second_records.len(), 1, "Variable binding should filter to only 'payments' pod");
        assert_eq!(second_records[0].id, "inc-001");
        assert_eq!(second_records[0].get_str("pod"), Some("payments"));
    } else {
        panic!("Expected Pipeline result");
    }
}

/// L7 / F-L2: a procedure keeps payload fields beyond its node fields, they match
/// WHERE and KEY, and NAMESPACE separates procedures.
#[tokio::test]
async fn procedures_keep_their_payload_and_namespace() {
    let adb = Mem::new().await;
    adb.execute(r#"STORE INTO PROCEDURAL (key = "deploy", pattern = "deploy a service", steps = "build\npush", owner = "ops", priority = 2) NAMESPACE "team-a""#)
        .await
        .unwrap();
    adb.execute(r#"STORE INTO PROCEDURAL (key = "rollback", pattern = "roll back a service", owner = "sre") NAMESPACE "team-b""#)
        .await
        .unwrap();

    let ids = |r: QueryResult| r.get_records().unwrap().iter().map(|r| r.id.clone()).collect::<Vec<_>>();
    let r = adb.execute(r#"RECALL FROM PROCEDURAL WHERE owner = "ops""#).await.unwrap();
    let recs = r.get_records().unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].id, "deploy");
    assert_eq!(recs[0].data["owner"], "ops");
    assert_eq!(recs[0].data["priority"], 2);
    assert_eq!(recs[0].data["steps"], json!(["build", "push"]), "node fields stay shaped");

    assert_eq!(ids(adb.execute(r#"LOOKUP FROM PROCEDURAL KEY owner = "sre""#).await.unwrap()), vec!["rollback"]);
    assert_eq!(ids(adb.execute(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "team-b""#).await.unwrap()), vec!["rollback"]);
    assert_eq!(ids(adb.execute(r#"RECALL FROM PROCEDURAL WHERE priority > 1 NAMESPACE "team-a""#).await.unwrap()), vec!["deploy"]);
}

/// W9 (spec §12): with the budget enforced, a pipeline that runs out of TIMEOUT
/// returns the stages that finished, flagged `timed_out`; without it, it runs on.
#[tokio::test]
async fn pipeline_timeout_is_a_hard_budget_when_enforced() {
    let adb = Mem::new().await;
    for i in 0..200 {
        adb.store(MemoryType::Semantic, &format!("f{i}"), json!({"topic": format!("t{}", i % 5)})).await.unwrap();
    }
    let q = r#"PIPELINE slow TIMEOUT 1ms RECALL FROM SEMANTIC WHERE topic = "t1" | RECALL FROM SEMANTIC WHERE topic = "t2" | RECALL FROM SEMANTIC WHERE topic = "t3""#;
    let x = adb.0.executor();
    let strict = adb_executor::Executor::with_io(x.writes().clone(), x.reads().clone()).with_enforced_timeout(true);
    let r = strict.execute(q).await.unwrap();
    assert_eq!(r.metadata.timed_out, Some(true));
    let adb_executor::ResultSet::Pipeline { steps } = &r.data else { panic!("pipeline result") };
    assert!(steps.len() < 3, "cut short: {} stages ran", steps.len());

    let loose = adb_executor::Executor::with_io(x.writes().clone(), x.reads().clone()).with_enforced_timeout(false);
    let r = loose.execute(q).await.unwrap();
    assert_eq!(r.metadata.timed_out, None);
    let adb_executor::ResultSet::Pipeline { steps } = &r.data else { panic!("pipeline result") };
    assert_eq!(steps.len(), 3);
}

/// S7 (W9): `PATTERN "text"` and `PATTERN $p` with `$p` bound both match
/// procedures by their pattern text; the score is `metadata.confidence`.
#[tokio::test]
async fn pattern_takes_a_literal_or_a_bound_variable() {
    let adb = Mem::new().await;
    adb.execute(r#"STORE INTO PROCEDURAL (key = "oom", pattern = "pod OOMKilled restart loop")"#).await.unwrap();
    adb.execute(r#"STORE INTO PROCEDURAL (key = "disk", pattern = "node disk pressure eviction")"#).await.unwrap();
    let ids = |r: QueryResult| r.get_records().unwrap().iter().map(|r| r.id.clone()).collect::<Vec<_>>();

    let r = adb.execute(r#"LOOKUP FROM PROCEDURAL PATTERN "pod OOMKilled restart" THRESHOLD 0.5"#).await.unwrap();
    // The overlap is the match's confidence (F-L2): 3 of 4 words.
    assert_eq!(r.get_records().unwrap()[0].metadata.confidence, Some(0.75));
    assert_eq!(ids(r), vec!["oom"]);

    let vars = std::collections::HashMap::from([("p".to_string(), json!("node disk pressure"))]);
    let r = adb.0.executor().execute_with_vars("LOOKUP FROM PROCEDURAL PATTERN $p THRESHOLD 0.5", &vars).await.unwrap();
    assert_eq!(ids(r), vec!["disk"]);
}

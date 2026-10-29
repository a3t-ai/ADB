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
/// procedures by their pattern text.
#[tokio::test]
async fn pattern_takes_a_literal_or_a_bound_variable() {
    let adb = Mem::new().await;
    adb.execute(r#"STORE INTO PROCEDURAL (key = "oom", pattern = "pod OOMKilled restart loop")"#).await.unwrap();
    adb.execute(r#"STORE INTO PROCEDURAL (key = "disk", pattern = "node disk pressure eviction")"#).await.unwrap();
    let ids = |r: QueryResult| r.get_records().unwrap().iter().map(|r| r.id.clone()).collect::<Vec<_>>();

    let r = adb.execute(r#"LOOKUP FROM PROCEDURAL PATTERN "pod OOMKilled restart" THRESHOLD 0.5"#).await.unwrap();
    assert_eq!(ids(r), vec!["oom"]);

    let vars = std::collections::HashMap::from([("p".to_string(), json!("node disk pressure"))]);
    let r = adb.0.executor().execute_with_vars("LOOKUP FROM PROCEDURAL PATTERN $p THRESHOLD 0.5", &vars).await.unwrap();
    assert_eq!(ids(r), vec!["disk"]);
}

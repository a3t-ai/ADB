//! The branch API (A3T's review) on branch namespaces: a branch is a base row and a
//! point tag at the current log position; it survives a restart; names, parents and
//! drops are checked; memory reads are unchanged by it. Also B7: sealed layers.

use adb_executor::{Engine, LocalEngine};

#[tokio::test]
async fn a_branch_is_one_record_at_the_log_position_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (offset_before, recall_before) = {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for i in 0..5 {
            engine.query(&format!(r#"STORE INTO SEMANTIC (concept = "c{i}", fact = "f{i}") NAMESPACE "crm""#), None).await.unwrap();
        }
        let before = engine.stats().await.unwrap();
        let recall = engine.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm""#, None).await.unwrap();
        let b = engine.branch_create("crm", "try-new-recall", None, Some("dev@a3t")).await.unwrap();
        assert_eq!(b.parent, "main");
        assert_eq!(b.namespace, "crm");
        assert!(b.point_offset >= 5, "the point is the log position at creation: {b:?}");
        // No memory row was added or changed by the branch.
        assert_eq!(engine.stats().await.unwrap(), before);
        // (The records; the answer's memory point moves with the log, DB-7.)
        assert_eq!(engine.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm""#, None).await.unwrap()["data"], recall["data"]);
        // A child of the branch; checks.
        let c = engine.branch_create("crm", "child", Some("try-new-recall"), None).await.unwrap();
        assert_eq!(c.parent, "try-new-recall");
        assert!(c.point_ts >= b.point_ts, "a branch of a branch is cut later, on its parent branch's own log");
        assert!(engine.branch_create("crm", "try-new-recall", None, None).await.is_err(), "names are unique per namespace");
        assert!(engine.branch_create("crm", "x", Some("nope"), None).await.is_err(), "the parent must exist");
        assert!(engine.branch_create("crm", "main", None, None).await.is_err());
        assert!(engine.branch_drop("crm", "try-new-recall").await.is_err(), "a branch with children cannot be dropped");
        // The same name in another namespace is another branch.
        engine.branch_create("billing", "try-new-recall", None, None).await.unwrap();
        (b.point_offset, recall)
    };
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let crm = engine.branch_list(Some("crm")).await.unwrap();
    assert_eq!(crm.iter().map(|b| b.name.as_str()).collect::<Vec<_>>(), ["child", "try-new-recall"]);
    assert_eq!(crm.iter().find(|b| b.name == "try-new-recall").unwrap().point_offset, offset_before);
    assert_eq!(engine.branch_list(None).await.unwrap().len(), 3);
    assert_eq!(engine.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm""#, None).await.unwrap()["data"], recall_before["data"]);
    assert!(engine.branch_drop("crm", "child").await.unwrap());
    assert!(engine.branch_drop("crm", "try-new-recall").await.unwrap());
    assert!(!engine.branch_drop("crm", "try-new-recall").await.unwrap(), "already gone");
    drop(engine);
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(engine.branch_list(Some("crm")).await.unwrap().len(), 0, "drops survive a restart");
    assert_eq!(engine.branch_list(Some("billing")).await.unwrap().len(), 1);
}

/// The values of `field` in a RECALL answer, sorted.
fn values(answer: &serde_json::Value, field: &str) -> Vec<String> {
    let recs = answer.pointer("/data/records").or_else(|| answer.pointer("/data/data/records")).and_then(|r| r.as_array()).cloned().unwrap_or_default();
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer(&format!("/data/{field}")).and_then(|x| x.as_str()).map(String::from)).collect();
    v.sort();
    v
}

/// B7 (spec §3.9, the compliance example): the trading agent inherits the compliance
/// agent's rules as a sealed layer — pinned at a tag, then mounted; it can't shadow
/// them; it never sees compliance episodes; FORGET on the layer reaches it.
#[tokio::test]
async fn a_sealed_layer_pinned_then_mounted() {
    use adb_executor::branches::{Layer, LayerMode};
    use adb_core::MemoryType;
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let q = |aql: String| {
        let e = &e;
        async move { e.query(&aql, None).await }
    };
    let fact = |ns: &'static str| {
        let e = &e;
        async move { values(&e.query(&format!(r#"RECALL FROM SEMANTIC WHERE fact != "" NAMESPACE "{ns}" LIMIT 100"#), None).await.unwrap(), "fact") }
    };
    q(r#"STORE INTO SEMANTIC (key = "rule:max-trade", fact = "limit 1M") NAMESPACE "compliance""#.into()).await.unwrap();
    q(r#"STORE INTO PROCEDURAL (key = "check_trade", pattern = "check a trade", steps = "limits") NAMESPACE "compliance""#.into()).await.unwrap();
    q(r#"STORE INTO EPISODIC (key = "audit-1", event = "reviewed desk 4") NAMESPACE "compliance""#.into()).await.unwrap();
    e.tag_create("compliance", "v1", None).await.unwrap();
    q(r#"UPDATE INTO SEMANTIC WHERE key = "rule:max-trade" (fact = "limit 2M", content = "trade limit is 2M") NAMESPACE "compliance""#.into()).await.unwrap();
    q(r#"STORE INTO SEMANTIC (key = "desk", fact = "desk 4", content = "desk 4 trades bonds") NAMESPACE "trading""#.into()).await.unwrap();

    let layer = |mode: LayerMode| Layer {
        space: "compliance".into(),
        mode,
        tag: (mode == LayerMode::Pin).then(|| "v1".into()),
        memories: vec![MemoryType::Semantic, MemoryType::Procedural],
        sealed: true,
    };
    assert!(e.layers_set("trading", vec![Layer { memories: vec![MemoryType::Episodic], ..layer(LayerMode::Mount) }]).await.is_err(), "episodes never cross");
    assert!(e.layers_set("trading", vec![Layer { tag: Some("nope".into()), ..layer(LayerMode::Pin) }]).await.is_err());
    e.layers_set("trading", vec![layer(LayerMode::Pin)]).await.unwrap();

    assert_eq!(fact("trading").await, ["desk 4", "limit 1M"], "pinned at v1");
    assert_eq!(values(&q(r#"RECALL FROM SEMANTIC WHERE key = "rule:max-trade" NAMESPACE "trading""#.into()).await.unwrap(), "fact"), ["limit 1M"]);
    let procs = q(r#"RECALL FROM PROCEDURAL NAMESPACE "trading""#.into()).await.unwrap();
    assert_eq!(procs.pointer("/data/count").and_then(|c| c.as_u64()), Some(1), "the inherited procedure: {procs}");
    let eps = q(r#"RECALL FROM EPISODIC NAMESPACE "trading""#.into()).await.unwrap();
    assert_eq!(eps.pointer("/data/count").and_then(|c| c.as_u64()), Some(0), "no compliance episodes");
    assert!(q(r#"STORE INTO SEMANTIC (key = "rule:max-trade", fact = "limit 9M") NAMESPACE "trading""#.into()).await.is_err(), "sealed: can't shadow");
    assert_eq!(fact("compliance").await, ["limit 2M"], "the layer's own space is unchanged");

    e.layers_set("trading", vec![layer(LayerMode::Mount)]).await.unwrap();
    assert_eq!(fact("trading").await, ["desk 4", "limit 2M"], "mounted: current");
    assert!(e.tag_drop("compliance", "v1").await.is_ok(), "no pin on it any more");
    // LIKE (the agent's recall) ranks the merged records too.
    let vars: std::collections::HashMap<String, serde_json::Value> = [("q".to_string(), serde_json::json!("trade limit"))].into();
    let like = e.query(r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "trading" LIMIT 5"#, Some(&vars)).await.unwrap();
    let found = values(&like, "fact");
    assert!(found.contains(&"limit 2M".to_string()) && found.contains(&"desk 4".to_string()), "the layer's and its own: {like}");

    q(r#"FORGET FROM SEMANTIC WHERE key = "rule:max-trade" NAMESPACE "compliance""#.into()).await.unwrap();
    assert_eq!(fact("trading").await, ["desk 4"], "FORGET on the layer reaches the mount");

    drop(e);
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(e.layers_get("trading").await.unwrap().len(), 1, "layers survive a restart");
}

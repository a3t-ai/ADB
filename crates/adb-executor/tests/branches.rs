//! Memory branches, B1 (`.ai/BRANCHING.md`): a branch is one control record at the
//! current log position; it survives a restart; names, parents and drops are checked;
//! memory reads are unchanged by it.

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

#[tokio::test]
#[ignore = "AQL v0.13: the BRANCH modifier is being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn the_branch_modifier_parses_through_to_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    // B1 carries BRANCH through the planner; reading on a branch is B2.
    let r = engine.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "main""#, None).await;
    assert!(r.is_ok(), "{r:?}");
}

async fn main(e: &LocalEngine) -> Vec<String> {
    values(&e.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm" LIMIT 100"#, None).await.unwrap(), "fact")
}

async fn on(e: &LocalEngine, b: &str) -> Vec<String> {
    values(&e.query(&format!(r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "{b}" LIMIT 100"#), None).await.unwrap(), "fact")
}

/// The values of `field` in a RECALL answer, sorted.
fn values(answer: &serde_json::Value, field: &str) -> Vec<String> {
    let recs = answer.pointer("/data/records").or_else(|| answer.pointer("/data/data/records")).and_then(|r| r.as_array()).cloned().unwrap_or_default();
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer(&format!("/data/{field}")).and_then(|x| x.as_str()).map(String::from)).collect();
    v.sort();
    v
}

/// B2 + B3, isolated engines: a branch reads its parent as of the point plus its own
/// writes; its writes go to its own engine — the org's log and table do not move.
#[tokio::test]
#[ignore = "AQL v0.13: the BRANCH modifier is being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn a_branch_is_its_own_engine_and_the_org_never_sees_its_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for (k, v) in [("a", "a1"), ("b", "b1"), ("c", "c1")] {
        e.query(&format!(r#"STORE INTO SEMANTIC (key = "{k}", fact = "{v}") NAMESPACE "crm""#), None).await.unwrap();
    }
    e.branch_create("crm", "try", None, None).await.unwrap();
    e.query(r#"STORE INTO SEMANTIC (key = "d", fact = "d1") NAMESPACE "crm""#, None).await.unwrap();
    e.query(r#"UPDATE INTO SEMANTIC WHERE key = "a" (fact = "a2") NAMESPACE "crm""#, None).await.unwrap();

    // The org's log and stats before the branch writes.
    let org_stats = e.stats().await.unwrap();
    let org_offset = e.branch_list(Some("crm")).await.unwrap(); // touch: a read, no write
    let _ = org_offset;
    let wal_before = std::fs::read_dir(dir.path().join("p0")).unwrap().map(|f| f.unwrap().metadata().unwrap().len()).sum::<u64>();

    e.query(r#"STORE INTO SEMANTIC (key = "e", fact = "e1") NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    e.query(r#"UPDATE INTO SEMANTIC WHERE key = "b" (fact = "b-try") NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    e.query(r#"FORGET FROM SEMANTIC WHERE key = "c" NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();

    assert_eq!(e.stats().await.unwrap(), org_stats, "the org's table got no branch rows");
    let wal_after = std::fs::read_dir(dir.path().join("p0")).unwrap().map(|f| f.unwrap().metadata().unwrap().len()).sum::<u64>();
    assert_eq!(wal_after, wal_before, "the org's WAL got no branch records");
    assert!(dir.path().join("branches").join("crm__try").join("p0").exists(), "the branch has its own WAL");

    assert_eq!(main(&e).await, ["a2", "b1", "c1", "d1"]);
    assert_eq!(on(&e, "try").await, ["a1", "b-try", "e1"], "a at the point, b overridden, c deleted, e its own, no d");

    assert!(e.query(r#"STORE INTO SEMANTIC (key = "a", fact = "x") NAMESPACE "crm" BRANCH "try""#, None).await.is_err(), "key already seen on the branch");
    e.query(r#"UPDATE INTO SEMANTIC WHERE key = "b" (fact = "b-try2") NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    assert_eq!(on(&e, "try").await, ["a1", "b-try2", "e1"]);

    // A branch of the branch.
    e.branch_create("crm", "deeper", Some("try"), None).await.unwrap();
    e.query(r#"STORE INTO SEMANTIC (key = "f", fact = "f1") NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    e.query(r#"UPDATE INTO SEMANTIC WHERE key = "e" (fact = "e-deeper") NAMESPACE "crm" BRANCH "deeper""#, None).await.unwrap();
    assert_eq!(on(&e, "deeper").await, ["a1", "b-try2", "e-deeper"], "no f: try wrote it after deeper's point");
    assert_eq!(on(&e, "try").await, ["a1", "b-try2", "e1", "f1"]);

    let l = e.query(r#"RECALL FROM SEMANTIC WHERE key = "b" NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    assert_eq!(values(&l, "fact"), ["b-try2"]);
    assert!(e.query(r#"RECALL FROM SEMANTIC BRANCH "try""#, None).await.is_err(), "BRANCH needs NAMESPACE");
    assert!(e.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "nope""#, None).await.is_err());

    // The same after a restart (branch engines reopen from their own directories).
    let (m, t, d) = (main(&e).await, on(&e, "try").await, on(&e, "deeper").await);
    drop(e);
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!((main(&e).await, on(&e, "try").await, on(&e, "deeper").await), (m, t, d));
}

/// Every file under `dir` that contains `needle`.
fn files_with(dir: &std::path::Path, needle: &[u8]) -> Vec<String> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() { walk(&p, out) } else { out.push(p) }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    files.into_iter().filter(|f| std::fs::read(f).map(|b| b.windows(needle.len()).any(|w| w == needle)).unwrap_or(false)).map(|f| f.display().to_string()).collect()
}

/// B4: FORGET on main reaches every branch (its view and its own copies; on disk after
/// the next compaction);
/// dropping a branch deletes its engine, and a new branch of the same name starts clean.
#[tokio::test]
#[ignore = "AQL v0.13: the BRANCH modifier is being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn forget_on_main_reaches_branches_and_drop_deletes_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for (k, v) in [("a", "a1"), ("p", "PII-on-main")] {
        e.query(&format!(r#"STORE INTO SEMANTIC (key = "{k}", fact = "{v}") NAMESPACE "crm""#), None).await.unwrap();
    }
    e.branch_create("crm", "try", None, None).await.unwrap();
    e.branch_create("crm", "deeper", Some("try"), None).await.unwrap();
    e.query(r#"UPDATE INTO SEMANTIC WHERE key = "p" (fact = "PII-copied-on-try") NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    e.query(r#"UPDATE INTO SEMANTIC WHERE key = "p" (fact = "PII-copied-on-deeper") NAMESPACE "crm" BRANCH "deeper""#, None).await.unwrap();
    assert_eq!(on(&e, "try").await, ["PII-copied-on-try", "a1"]);
    let branches = dir.path().join("branches");
    assert!(!files_with(&branches, b"PII-copied-on-try").is_empty(), "on disk before (else this test proves nothing)");

    e.query(r#"FORGET FROM SEMANTIC WHERE key = "p" NAMESPACE "crm""#, None).await.unwrap();
    assert_eq!(main(&e).await, ["a1"]);
    assert_eq!(on(&e, "try").await, ["a1"], "gone from the branch's view and its own copy");
    assert_eq!(on(&e, "deeper").await, ["a1"]);
    // FORGET only hides (user, 7 Nov); compaction takes the bytes off main and the branches.
    e.compact().await.unwrap();
    for needle in [&b"PII-on-main"[..], b"PII-copied-on-try", b"PII-copied-on-deeper"] {
        assert!(files_with(dir.path(), needle).is_empty(), "{} still on disk: {:?}", String::from_utf8_lossy(needle), files_with(dir.path(), needle));
    }

    // Drop: the engine's directory goes; the same name later is a new, empty branch.
    e.query(r#"STORE INTO SEMANTIC (key = "t", fact = "only-on-deeper") NAMESPACE "crm" BRANCH "deeper""#, None).await.unwrap();
    assert!(e.branch_drop("crm", "deeper").await.unwrap());
    assert!(!branches.join("crm__deeper").exists(), "its engine's data is deleted");
    assert!(e.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "deeper""#, None).await.is_err());
    e.branch_create("crm", "deeper", Some("try"), None).await.unwrap();
    assert_eq!(on(&e, "deeper").await, ["a1"], "no only-on-deeper: a new branch");
    drop(e);
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(on(&e, "deeper").await, ["a1"]);
}

/// B5: promote a branch as a new agent — the new namespace holds the branch's view
/// (SEMANTIC, PROCEDURAL, TOOLS) as fresh records; EPISODIC stays; the original agent
/// and the branch are unchanged; a clone copies main as it is now.
#[tokio::test]
#[ignore = "AQL v0.13: the BRANCH modifier is being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn promote_a_branch_as_a_new_agent_and_clone_main() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let q = |aql: String| {
        let e = &e;
        async move { e.query(&aql, None).await.unwrap() }
    };
    for (k, v) in [("a", "a1"), ("b", "b1"), ("c", "c1")] {
        q(format!(r#"STORE INTO SEMANTIC (key = "{k}", fact = "{v}") NAMESPACE "crm""#)).await;
    }
    q(r#"STORE INTO SEMANTIC FACT (subject = "user:alice", predicate = "plan", value = "pro") NAMESPACE "crm""#.into()).await;
    q(r#"STORE INTO PROCEDURAL (key = "deploy", pattern = "deploy a service", steps = "build") NAMESPACE "crm""#.into()).await;
    q(r#"STORE INTO EPISODIC (key = "ep1", event = "talked to alice") NAMESPACE "crm""#.into()).await;
    e.branch_create("crm", "try", None, None).await.unwrap();
    q(r#"UPDATE INTO SEMANTIC WHERE key = "b" (fact = "b-try") NAMESPACE "crm" BRANCH "try""#.into()).await;
    q(r#"FORGET FROM SEMANTIC WHERE key = "c" NAMESPACE "crm" BRANCH "try""#.into()).await;
    q(r#"STORE INTO SEMANTIC (key = "e", fact = "e1") NAMESPACE "crm" BRANCH "try""#.into()).await;
    // Main moves on after the point: not in the promoted agent.
    q(r#"STORE INTO SEMANTIC (key = "d", fact = "d1") NAMESPACE "crm""#.into()).await;

    let p = e.branch_promote("crm", "try", "crm-v2", true).await.unwrap();
    assert_eq!(p["namespace"], "crm-v2");
    let in_ns = |ns: &'static str| {
        let e = &e;
        async move { values(&e.query(&format!(r#"RECALL FROM SEMANTIC WHERE fact != "" NAMESPACE "{ns}" LIMIT 100"#), None).await.unwrap(), "fact") }
    };
    assert_eq!(in_ns("crm-v2").await, ["a1", "b-try", "e1"], "the branch's view");
    assert_eq!(main(&e).await, ["a1", "b1", "c1", "d1"], "the original agent unchanged");
    assert_eq!(on(&e, "try").await, ["a1", "b-try", "e1"], "the branch unchanged");
    // The new agent is a normal space: keyed lookups, its own fact and entity, its procedure.
    assert_eq!(values(&q(r#"RECALL FROM SEMANTIC WHERE key = "b" NAMESPACE "crm-v2""#.into()).await, "fact"), ["b-try"]);
    assert_eq!(values(&q(r#"RECALL FROM SEMANTIC WHERE subject = "user:alice" NAMESPACE "crm-v2""#.into()).await, "value"), ["pro"]);
    assert_eq!(values(&q(r#"RECALL FROM SEMANTIC WHERE key = "user:alice" NAMESPACE "crm-v2""#.into()).await, "key"), ["user:alice"]);
    assert_eq!(q(r#"RECALL FROM PROCEDURAL NAMESPACE "crm-v2""#.into()).await.pointer("/data/count").and_then(|c| c.as_u64()), Some(1));
    // D1: episodes stay with the original agent; the new one reads them through a history
    // layer (not a copy), beside its own origin record.
    let eps = q(r#"RECALL FROM EPISODIC NAMESPACE "crm-v2""#.into()).await;
    assert_eq!(eps.pointer("/data/count").and_then(|c| c.as_u64()), Some(2), "{eps}");
    assert_eq!(values(&eps, "event").iter().filter(|e| *e == "promoted_from").count(), 1, "{eps}");
    // Writes to the new agent leave the original alone.
    q(r#"UPDATE INTO SEMANTIC WHERE key = "a" (fact = "a-v2") NAMESPACE "crm-v2""#.into()).await;
    assert_eq!(main(&e).await, ["a1", "b1", "c1", "d1"]);
    assert!(e.branch_promote("crm", "try", "crm-v2", true).await.is_err(), "into a new namespace only");
    assert!(e.branch_promote("crm", "try", "crm", true).await.is_err());

    let c = e.space_clone("crm", "crm-copy").await.unwrap();
    assert!(c["records"].as_u64().unwrap() >= 6, "{c}");
    assert_eq!(in_ns("crm-copy").await, ["a1", "b1", "c1", "d1"]);

    drop(e);
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = e.query(r#"RECALL FROM SEMANTIC WHERE fact != "" NAMESPACE "crm-v2" LIMIT 100"#, None).await.unwrap();
    assert_eq!(values(&r, "fact"), ["a-v2", "b-try", "e1"], "after a restart");
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

/// PATTERN and LIKE read a branch's view: its own records and its parent's at the
/// branch point, ranked as on main; main never sees the branch's.
#[tokio::test]
#[ignore = "AQL v0.13: the BRANCH modifier is being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn pattern_and_like_read_a_branch() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    e.query(r#"STORE INTO PROCEDURAL (pattern = "pod OOMKilled restart", steps = "raise the memory limit") NAMESPACE "ops""#, None).await.unwrap();
    e.query(r#"STORE INTO SEMANTIC (key = "k1", content = "the deploy failed on memory") NAMESPACE "ops""#, None).await.unwrap();
    e.branch_create("ops", "b1", None, None).await.unwrap();
    e.query(r#"STORE INTO PROCEDURAL (pattern = "pod OOMKilled loop", steps = "find the leak") NAMESPACE "ops" BRANCH "b1""#, None).await.unwrap();
    e.query(r#"STORE INTO SEMANTIC (key = "k2", content = "memory leak found in the cache") NAMESPACE "ops" BRANCH "b1""#, None).await.unwrap();

    let pattern = |branch: &'static str| {
        let e = &e;
        async move {
            let aql = format!(r#"RECALL FROM PROCEDURAL LIKE "pod OOMKilled restart" CONFIDENCE 0.4 NAMESPACE "ops"{branch}"#);
            let r = e.query(&aql, None).await.unwrap_or_else(|x| panic!("{aql}: {x}"));
            let mut s = values(&r, "pattern");
            s.sort();
            s
        }
    };
    assert_eq!(pattern(r#" BRANCH "b1""#).await, ["pod OOMKilled loop", "pod OOMKilled restart"], "the branch's and the inherited one");
    assert_eq!(pattern("").await, ["pod OOMKilled restart"], "main: its own only");

    let vars: std::collections::HashMap<String, serde_json::Value> = [("q".to_string(), serde_json::json!("memory leak"))].into();
    let like = e.query(r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "ops" BRANCH "b1" LIMIT 5"#, Some(&vars)).await.unwrap();
    let mut found = values(&like, "content");
    found.sort();
    assert_eq!(found, ["memory leak found in the cache", "the deploy failed on memory"], "{like}");
    let like = e.query(r#"RECALL FROM SEMANTIC LIKE $q NAMESPACE "ops" LIMIT 5"#, Some(&vars)).await.unwrap();
    assert_eq!(values(&like, "content"), ["the deploy failed on memory"], "main doesn't see the branch: {like}");
}

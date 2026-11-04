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
        let recall = engine.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm""#, None).await.unwrap();
        let b = engine.branch_create("crm", "try-new-recall", None, Some("dev@a3t")).await.unwrap();
        assert_eq!(b.parent, "main");
        assert_eq!(b.namespace, "crm");
        assert!(b.point_offset >= 5, "the point is the log position at creation: {b:?}");
        // No memory row was added or changed by the branch.
        assert_eq!(engine.stats().await.unwrap(), before);
        assert_eq!(engine.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm""#, None).await.unwrap(), recall);
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
    assert_eq!(engine.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm""#, None).await.unwrap(), recall_before);
    assert!(engine.branch_drop("crm", "child").await.unwrap());
    assert!(engine.branch_drop("crm", "try-new-recall").await.unwrap());
    assert!(!engine.branch_drop("crm", "try-new-recall").await.unwrap(), "already gone");
    drop(engine);
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(engine.branch_list(Some("crm")).await.unwrap().len(), 0, "drops survive a restart");
    assert_eq!(engine.branch_list(Some("billing")).await.unwrap().len(), 1);
}

#[tokio::test]
async fn the_branch_modifier_parses_through_to_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    // B1 carries BRANCH through the planner; reading on a branch is B2.
    let r = engine.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "main""#, None).await;
    assert!(r.is_ok(), "{r:?}");
}

async fn main(e: &LocalEngine) -> Vec<String> {
    values(&e.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" LIMIT 100"#, None).await.unwrap(), "fact")
}

async fn on(e: &LocalEngine, b: &str) -> Vec<String> {
    values(&e.query(&format!(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "{b}" LIMIT 100"#), None).await.unwrap(), "fact")
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

    let l = e.query(r#"LOOKUP FROM SEMANTIC WHERE key = "b" NAMESPACE "crm" BRANCH "try""#, None).await.unwrap();
    assert_eq!(values(&l, "fact"), ["b-try2"]);
    assert!(e.query(r#"RECALL FROM SEMANTIC ALL BRANCH "try""#, None).await.is_err(), "BRANCH needs NAMESPACE");
    assert!(e.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "nope""#, None).await.is_err());

    // The same after a restart (branch engines reopen from their own directories).
    let (m, t, d) = (main(&e).await, on(&e, "try").await, on(&e, "deeper").await);
    drop(e);
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!((main(&e).await, on(&e, "try").await, on(&e, "deeper").await), (m, t, d));
}

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
        assert!(c.point_offset > b.point_offset);
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

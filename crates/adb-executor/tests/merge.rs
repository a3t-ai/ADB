//! Phase 2 ADB (`.ai/MERGE.md`): memory points on answers (DB-7), the branch origin
//! record (DB-20), the three-way diff (DB-10), and kind 2 merge-back with its guards and
//! revert (DB-19).

use adb_core::MemoryType;
use adb_executor::merge::Resolution;
use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

fn values(answer: &Value, field: &str) -> Vec<String> {
    let recs = answer.pointer("/data/records").or_else(|| answer.pointer("/data/data/records")).and_then(|r| r.as_array()).cloned().unwrap_or_default();
    // PROCEDURAL keeps `steps` as a list: its first step stands for the record here.
    let text = |x: &Value| x.as_str().map(String::from).or_else(|| x.get(0).and_then(|y| y.as_str()).map(String::from));
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer(&format!("/data/{field}")).and_then(text)).collect();
    v.sort();
    v
}

async fn procs(e: &LocalEngine) -> Vec<String> {
    values(&e.query(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "crm" LIMIT 100"#, None).await.unwrap(), "steps")
}

async fn q(e: &LocalEngine, aql: &str) -> Value {
    e.query(aql, None).await.unwrap_or_else(|err| panic!("{aql}: {err}"))
}

#[tokio::test]
async fn memory_points_origin_conflicts_merge_and_revert() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for k in ["a", "b", "c", "d"] {
        q(&e, &format!(r#"STORE INTO PROCEDURAL (key = "{k}", steps = "{k}1") NAMESPACE "crm""#)).await;
    }

    // DB-7: a write answers with its own record's point; a read with the last commit.
    let w = q(&e, r#"STORE INTO PROCEDURAL (key = "x", steps = "x1") NAMESPACE "crm""#).await;
    let p = w.pointer("/metadata/point").cloned().unwrap_or_else(|| panic!("no point: {w}"));
    let r = q(&e, r#"RECALL FROM PROCEDURAL ALL NAMESPACE "crm" LIMIT 100"#).await;
    assert_eq!(r.pointer("/metadata/point/offset"), p.get("offset"), "a read after the write includes it: {r}");
    let at = p["at"].as_str().unwrap();
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "x" (steps = "x2") NAMESPACE "crm""#).await;
    let then = q(&e, &format!(r#"RECALL FROM PROCEDURAL ALL NAMESPACE "crm" AS OF "{at}" LIMIT 100"#)).await;
    assert!(values(&then, "steps").contains(&"x1".to_string()), "AS OF the point reads it as it was: {then}");

    // A branch; DB-20: its first record says where it came from.
    e.branch_create("crm", "try", None, Some("dev@a3t")).await.unwrap();
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "a" (steps = "a-branch") NAMESPACE "crm" BRANCH "try""#).await;
    let origin = q(&e, r#"RECALL FROM EPISODIC ALL NAMESPACE "crm" BRANCH "try" LIMIT 10"#).await;
    assert_eq!(values(&origin, "event"), ["branch_created"], "{origin}");
    assert_eq!(values(&origin, "created_by"), ["dev@a3t"]);
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "b" (steps = "b-branch") NAMESPACE "crm" BRANCH "try""#).await;
    q(&e, r#"FORGET FROM PROCEDURAL WHERE key = "d" NAMESPACE "crm" BRANCH "try""#).await;
    q(&e, r#"STORE INTO PROCEDURAL (key = "e", steps = "e-branch") NAMESPACE "crm" BRANCH "try""#).await;
    // Main moves too.
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "b" (steps = "b-main") NAMESPACE "crm""#).await;
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "c" (steps = "c-main") NAMESPACE "crm""#).await;

    // DB-10
    let d = e.branch_conflicts("crm", "try", &[MemoryType::Procedural]).await.unwrap();
    let clean: Vec<(String, String)> = d["clean"].as_array().unwrap().iter().map(|c| (c["key"].as_str().unwrap().to_string(), c["change"].as_str().unwrap().to_string())).collect();
    assert_eq!(clean, [("a".into(), "change".into()), ("d".into(), "forget".into()), ("e".into(), "add".into())], "{d}");
    let conflicts = d["conflicts"].as_array().unwrap();
    assert_eq!(conflicts.len(), 1, "{d}");
    assert_eq!((conflicts[0]["key"].clone(), conflicts[0]["type"].clone()), (json!("b"), json!("edit_edit")));
    // Payloads are the stored rows (`steps` as written; reads turn it into a list).
    let step = |v: &Value| v.as_str().map(String::from).or_else(|| v.get(0).and_then(|x| x.as_str()).map(String::from));
    assert_eq!((step(&conflicts[0]["main"]["steps"]), step(&conflicts[0]["branch"]["steps"])), (Some("b-main".into()), Some("b-branch".into())));
    let lineage_b = conflicts[0]["lineage"].as_str().unwrap().to_string();
    let main_offset = d["main"]["offset"].as_u64().unwrap();

    // DB-19 guards: an unresolved conflict; main moved after main_offset.
    let err = e.branch_merge("crm", "try", &[], main_offset, vec![], Some("dev")).await.unwrap_err();
    assert!(err.starts_with("merge_conflicts:"), "{err}");
    let take_branch = vec![Resolution { lineage: lineage_b.clone(), memory: MemoryType::Procedural, take: "branch".into(), value: None }];
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "b" (steps = "b-main2") NAMESPACE "crm""#).await;
    let err = e.branch_merge("crm", "try", &[], main_offset, take_branch.clone(), Some("dev")).await.unwrap_err();
    assert!(err.starts_with("merge_stale:"), "{err}");

    // Read again, resolve, merge.
    let d = e.branch_conflicts("crm", "try", &[MemoryType::Procedural]).await.unwrap();
    let m = e.branch_merge("crm", "try", &[], d["main"]["offset"].as_u64().unwrap(), take_branch, Some("dev")).await.unwrap();
    assert_eq!((m["changed"].clone(), m["added"].clone(), m["ended"].clone()), (json!(2), json!(1), json!(1)), "{m}");
    assert_eq!(procs(&e).await, ["a-branch", "b-branch", "c-main", "e-branch", "x2"], "main after the merge");
    // The branch is unchanged by the merge.
    let on_branch = values(&q(&e, r#"RECALL FROM PROCEDURAL ALL NAMESPACE "crm" BRANCH "try" LIMIT 100"#).await, "steps");
    assert_eq!(on_branch, ["a-branch", "b-branch", "c1", "e-branch", "x2"]);

    // Revert: main back as it was before the merge.
    let merge_id = m["merge_id"].as_str().unwrap().to_string();
    let r = e.merge_revert("crm", &merge_id, Some("dev"), false).await.unwrap();
    assert_eq!(r["reverted"], json!(4), "{r}");
    assert_eq!(procs(&e).await, ["a1", "b-main2", "c-main", "d1", "x2"], "main after the revert");
    let err = e.merge_revert("crm", &merge_id, None, false).await.unwrap_err();
    assert!(err.starts_with("merge_not_found:"), "{err}");
    let err = e.merge_revert("crm", "nope", None, false).await.unwrap_err();
    assert!(err.starts_with("merge_not_found:"), "{err}");

    // A revert after main changed a merged record: refused, or the rest with force_skip.
    let d = e.branch_conflicts("crm", "try", &[MemoryType::Procedural]).await.unwrap();
    let res: Vec<Resolution> = d["conflicts"].as_array().unwrap().iter().map(|c| Resolution {
        lineage: c["lineage"].as_str().unwrap().into(),
        memory: serde_json::from_value(c["memory"].clone()).unwrap(),
        take: "branch".into(),
        value: None,
    }).collect();
    let m2 = e.branch_merge("crm", "try", &[], d["main"]["offset"].as_u64().unwrap(), res, None).await.unwrap();
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "a" (steps = "a-later") NAMESPACE "crm""#).await;
    let err = e.merge_revert("crm", m2["merge_id"].as_str().unwrap(), None, false).await.unwrap_err();
    assert!(err.starts_with("revert_conflicts:"), "{err}");
    let r = e.merge_revert("crm", m2["merge_id"].as_str().unwrap(), None, true).await.unwrap();
    assert_eq!(r["skipped"].as_array().unwrap().len(), 1, "{r}");
    assert!(procs(&e).await.contains(&"a-later".to_string()), "the record changed since is left as it is");
}

//! Branch review, the API beside AQL (user, 18 Nov): A3T's merge requests on namespace
//! branches — list, conflicts, merge with a choice per record, revert, rebase, promote.

use adb_core::MemoryType;
use adb_executor::merge::Resolution;
use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

async fn q(e: &LocalEngine, aql: &str) -> Value {
    e.query(aql, None).await.unwrap_or_else(|x| panic!("{aql}: {x}"))
}

async fn steps(e: &LocalEngine, ns: &str) -> Vec<String> {
    let r = q(e, &format!(r#"RECALL FROM PROCEDURAL NAMESPACE "{ns}""#)).await;
    let mut v: Vec<String> = r["data"]["records"].as_array().unwrap().iter().map(|x| format!("{}={}", x["data"]["key"].as_str().unwrap_or("?"), x["data"]["steps"].as_str().map(String::from).or_else(|| x["data"]["steps"].as_array().map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(","))).unwrap_or_else(|| "?".into()))).collect();
    v.sort();
    v
}

#[tokio::test]
async fn review_merge_revert_rebase_promote() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for (k, s) in [("p1", "a"), ("p2", "b"), ("p3", "c")] {
        q(&e, &format!(r#"STORE INTO PROCEDURAL (key = "{k}", pattern = "{k}", steps = "{s}") NAMESPACE "agt""#)).await;
    }
    let b = e.branch_create("agt", "try", None, None).await.unwrap();
    assert_eq!((b.namespace.as_str(), b.name.as_str(), b.parent.as_str()), ("agt", "try", "main"));
    assert_eq!(e.branch_list(Some("agt")).await.unwrap().iter().map(|b| b.name.clone()).collect::<Vec<_>>(), ["try"]);

    // The branch changes p1 and p2, adds p4; main changes p2 too (a conflict).
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "p1" (steps = "a2") NAMESPACE "agt@try""#).await;
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "p2" (steps = "b-branch") NAMESPACE "agt@try""#).await;
    q(&e, r#"STORE INTO PROCEDURAL (key = "p4", pattern = "p4", steps = "d") NAMESPACE "agt@try""#).await;
    q(&e, r#"UPDATE INTO PROCEDURAL WHERE key = "p2" (steps = "b-main") NAMESPACE "agt""#).await;

    let c = e.branch_conflicts("agt", "try", &[MemoryType::Procedural]).await.unwrap();
    assert_eq!(c["clean"].as_array().unwrap().len(), 2, "p1 change, p4 add: {c}");
    assert_eq!(c["conflicts"].as_array().unwrap().len(), 1, "{c}");
    let lineage = c["conflicts"][0]["lineage"].as_str().unwrap().to_string();
    assert_eq!(c["conflicts"][0]["type"], "edit_edit");
    let off = c["main"]["offset"].as_u64().unwrap();

    // Unresolved: refused. Resolved as an edit: merged.
    assert!(e.branch_merge("agt", "try", &[], off, vec![], None).await.unwrap_err().contains("merge_conflicts"));
    let edit = Resolution { lineage: lineage.clone(), memory: MemoryType::Procedural, take: "edit".into(), value: Some(json!({ "key": "p2", "pattern": "p2", "steps": "b-both" })) };
    let m = e.branch_merge("agt", "try", &[], off, vec![edit], Some("alice")).await.unwrap();
    assert_eq!((m["changed"].as_u64(), m["added"].as_u64()), (Some(2), Some(1)), "{m}");
    assert_eq!(steps(&e, "agt").await, ["p1=a2", "p2=b-both", "p3=c", "p4=d"]);

    // Revert puts main back as it was.
    let id = m["merge_id"].as_str().unwrap();
    let r = e.merge_revert("agt", id, Some("alice"), false).await.unwrap();
    assert_eq!(r["reverted"], 3, "{r}");
    assert_eq!(steps(&e, "agt").await, ["p1=a", "p2=b-main", "p3=c"]);
    assert!(e.merge_revert("agt", id, None, false).await.unwrap_err().contains("already reverted"));

    // Rebase. Main changed p1 too since the point (the merge, then the revert), so p1
    // and p2 conflict: keep the branch's p1, take main's p2 (the branch's goes).
    let c = e.branch_conflicts("agt", "try", &[]).await.unwrap();
    let off = c["main"]["offset"].as_u64().unwrap();
    let mut keys: Vec<String> = c["conflicts"].as_array().unwrap().iter().map(|x| x["lineage"].as_str().unwrap().to_string()).collect();
    keys.sort();
    assert_eq!(keys, ["p1", "p2"], "{c}");
    let take = |l: &str, t: &str| Resolution { lineage: l.into(), memory: MemoryType::Procedural, take: t.into(), value: None };
    let rb = e.branch_rebase("agt", "try", off, vec![take("p1", "branch"), take(&lineage, "main")]).await.unwrap();
    assert_eq!((rb["dropped"].as_u64(), rb["kept"].as_u64()), (Some(1), Some(1)), "{rb}");
    assert_eq!(steps(&e, "agt@try").await, ["p1=a2", "p2=b-main", "p3=c", "p4=d"]);

    // Promote: the branch as it reads, into a new namespace.
    let p = e.branch_promote("agt", "try", "agt2", false).await.unwrap();
    assert_eq!(p["records"], 4, "{p}");
    assert_eq!(steps(&e, "agt2").await, ["p1=a2", "p2=b-main", "p3=c", "p4=d"]);

    assert!(e.branch_drop("agt", "try").await.unwrap());
    assert!(e.branch_list(Some("agt")).await.unwrap().is_empty());
}

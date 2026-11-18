//! AQL v0.13 BRANCH / FORK / MERGE on namespaces (`.ai/BRANCH_NAMESPACES.md`, spec
//! §5): a branch is a namespace that reads through to its base at a point; a fork is
//! a copy; MERGE moves one side's changes to the other.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(e: &LocalEngine, aql: &str) -> Value {
    e.query(aql, None).await.unwrap_or_else(|x| panic!("{aql}: {x}"))
}

async fn err(e: &LocalEngine, aql: &str) -> String {
    match e.query(aql, None).await {
        Ok(v) => panic!("{aql} should fail: {v}"),
        Err(x) => x,
    }
}

/// The `field` of each record a read returns, sorted.
async fn vals(e: &LocalEngine, aql: &str, field: &str) -> Vec<String> {
    let r = q(e, aql).await;
    let mut v: Vec<String> = r["data"]["records"]
        .as_array()
        .unwrap_or_else(|| panic!("{aql}: {r}"))
        .iter()
        .filter_map(|x| x["data"][field].as_str().map(String::from))
        .collect();
    v.sort();
    v
}

async fn roles(e: &LocalEngine, ns: &str) -> Vec<String> {
    vals(e, &format!(r#"RECALL FROM SEMANTIC NAMESPACE "{ns}""#), "role").await
}

async fn engine() -> (tempfile::TempDir, LocalEngine) {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    (dir, e)
}

#[tokio::test]
async fn a_branch_reads_through_and_writes_stay_on_it() {
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO SEMANTIC (key = "ada", role = "engineer") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO SEMANTIC (key = "bob", role = "sales") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO EPISODIC (key = "e1", role = "called") NAMESPACE "crm""#).await;
    q(&e, r#"BRANCH FROM "crm" INTO "crm-try""#).await;
    assert_eq!(roles(&e, "crm-try").await, ["engineer", "sales"], "the base's records, through");
    assert_eq!(vals(&e, r#"RECALL FROM EPISODIC NAMESPACE "crm-try""#, "role").await, ["called"], "every memory type");
    assert_eq!(vals(&e, r#"RECALL FROM SEMANTIC WHERE key = "ada" NAMESPACE "crm-try""#, "role").await, ["engineer"], "a key lookup");

    // Writes on the branch: an override, a new record, a forget — the base unchanged.
    let u = q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CTO") NAMESPACE "crm-try""#).await;
    assert_eq!(u["data"]["count"], 1, "{u}");
    q(&e, r#"STORE INTO SEMANTIC (key = "cy", role = "intern") NAMESPACE "crm-try""#).await;
    let f = q(&e, r#"FORGET FROM SEMANTIC WHERE key = "bob" NAMESPACE "crm-try""#).await;
    assert_eq!(f["data"]["count"], 1, "{f}");
    assert_eq!(roles(&e, "crm-try").await, ["CTO", "intern"]);
    assert_eq!(roles(&e, "crm").await, ["engineer", "sales"], "the base doesn't see the branch");
    // A key the branch inherits can't be stored again.
    assert!(err(&e, r#"STORE INTO SEMANTIC (key = "ada", role = "x") NAMESPACE "crm-try""#).await.contains("exists"));

    // Base writes after the point aren't seen by the branch.
    q(&e, r#"STORE INTO SEMANTIC (key = "dan", role = "ops") NAMESPACE "crm""#).await;
    assert_eq!(roles(&e, "crm-try").await, ["CTO", "intern"]);

    // A branch of the branch reads through both.
    q(&e, r#"BRANCH FROM "crm-try" INTO "crm-try2""#).await;
    assert_eq!(roles(&e, "crm-try2").await, ["CTO", "intern"]);

    // BRANCH / FORK refuse an existing or a missing namespace.
    assert!(err(&e, r#"BRANCH FROM "crm" INTO "crm-try""#).await.contains("exists"));
    assert!(err(&e, r#"BRANCH FROM "nope" INTO "x""#).await.contains("no namespace"));
}

#[tokio::test]
async fn as_of_and_fork() {
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO SEMANTIC (key = "ada", role = "engineer") NAMESPACE "crm""#).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let t = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CTO") NAMESPACE "crm""#).await;
    q(&e, &format!(r#"BRANCH FROM "crm" INTO "crm-then" AS OF "{t}""#)).await;
    assert_eq!(roles(&e, "crm-then").await, ["engineer"], "the base as it was then");

    let f = q(&e, r#"FORK FROM "crm" INTO "crm-copy""#).await;
    assert_eq!(f["data"]["records"], 1, "{f}");
    assert_eq!(roles(&e, "crm-copy").await, ["CTO"]);
    // A fork holds its own copy: later base changes don't reach it.
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CEO") NAMESPACE "crm""#).await;
    assert_eq!(roles(&e, "crm-copy").await, ["CTO"]);
}

#[tokio::test]
async fn merge_both_ways_and_conflicts() {
    let (_d, e) = engine().await;
    for (k, r) in [("ada", "engineer"), ("bob", "sales"), ("cy", "intern")] {
        q(&e, &format!(r#"STORE INTO SEMANTIC (key = "{k}", role = "{r}") NAMESPACE "crm""#)).await;
    }
    q(&e, r#"BRANCH FROM "crm" INTO "crm-try""#).await;
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CTO") NAMESPACE "crm-try""#).await;
    q(&e, r#"FORGET FROM SEMANTIC WHERE key = "cy" NAMESPACE "crm-try""#).await;
    q(&e, r#"STORE INTO SEMANTIC (key = "dan", role = "ops") NAMESPACE "crm-try""#).await;
    // Meanwhile on the base: another key changes (no conflict).
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "bob" (role = "head of sales") NAMESPACE "crm""#).await;

    let m = q(&e, r#"MERGE FROM "crm-try" INTO "crm""#).await;
    assert_eq!(m["data"]["merged"], 3, "{m}");
    assert_eq!(roles(&e, "crm").await, ["CTO", "head of sales", "ops"]);
    // Parted now: the branch sees the base's latest, and merging again moves nothing.
    assert_eq!(roles(&e, "crm-try").await, ["CTO", "head of sales", "ops"]);
    assert_eq!(q(&e, r#"MERGE FROM "crm-try" INTO "crm""#).await["data"]["merged"], 0);

    // A conflict: both change ada.
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CEO") NAMESPACE "crm""#).await;
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "ada" (role = "CFO") NAMESPACE "crm-try""#).await;
    let x = err(&e, r#"MERGE FROM "crm-try" INTO "crm""#).await;
    assert!(x.contains("merge_conflict") && x.contains("ada"), "{x}");
    assert_eq!(roles(&e, "crm").await, ["CEO", "head of sales", "ops"], "a refused merge writes nothing");
    // The base into the branch, the base winning: the branch's own ada goes.
    q(&e, r#"MERGE FROM "crm" INTO "crm-try" PREFER SOURCE"#).await;
    assert_eq!(roles(&e, "crm-try").await, ["CEO", "head of sales", "ops"]);

    // PREFER TARGET keeps the target's.
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "bob" (role = "VP") NAMESPACE "crm-try""#).await;
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "bob" (role = "SVP") NAMESPACE "crm""#).await;
    q(&e, r#"MERGE FROM "crm-try" INTO "crm" PREFER TARGET"#).await;
    assert_eq!(roles(&e, "crm").await, ["CEO", "SVP", "ops"]);

    // Two namespaces with no common base.
    q(&e, r#"STORE INTO SEMANTIC (key = "z", role = "z") NAMESPACE "other""#).await;
    assert!(err(&e, r#"MERGE FROM "other" INTO "crm""#).await.contains("no common base"));
}

#[tokio::test]
async fn dropping_namespaces_with_branches() {
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO SEMANTIC (key = "ada", role = "engineer") NAMESPACE "crm""#).await;
    q(&e, r#"BRANCH FROM "crm" INTO "crm-try""#).await;
    q(&e, r#"FORK FROM "crm" INTO "crm-copy""#).await;
    let x = err(&e, r#"FORGET FROM ALL NAMESPACE "crm""#).await;
    assert!(x.contains("has branches") && x.contains("crm-try"), "{x}");
    q(&e, r#"FORGET FROM ALL NAMESPACE "crm-try""#).await;
    assert!(roles(&e, "crm-try").await.is_empty(), "the branch is gone");
    q(&e, r#"FORGET FROM ALL NAMESPACE "crm""#).await;
    assert!(roles(&e, "crm").await.is_empty());
    assert_eq!(roles(&e, "crm-copy").await, ["engineer"], "a fork keeps its copy");
    // The dropped branch's name can be used again.
    q(&e, r#"STORE INTO SEMANTIC (key = "new", role = "fresh") NAMESPACE "crm-try""#).await;
    assert_eq!(roles(&e, "crm-try").await, ["fresh"]);
}

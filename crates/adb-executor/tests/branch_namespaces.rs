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
    assert!(x.contains("has branches of its own") && x.contains("crm-try"), "{x}");
    q(&e, r#"FORGET FROM ALL NAMESPACE "crm-try""#).await;
    assert!(roles(&e, "crm-try").await.is_empty(), "the branch is gone");
    q(&e, r#"FORGET FROM ALL NAMESPACE "crm""#).await;
    assert!(roles(&e, "crm").await.is_empty());
    assert_eq!(roles(&e, "crm-copy").await, ["engineer"], "a fork keeps its copy");
    // The dropped branch's name can be used again.
    q(&e, r#"STORE INTO SEMANTIC (key = "new", role = "fresh") NAMESPACE "crm-try""#).await;
    assert_eq!(roles(&e, "crm-try").await, ["fresh"]);
}

/// FORGET is never missed (user, 13 Nov): a record forgotten on the base is gone from
/// its branches too, though they read the base as it was at their point.
#[tokio::test]
async fn forget_on_the_base_reaches_its_branches() {
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO SEMANTIC (key = "ada", role = "engineer") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO SEMANTIC (key = "bob", role = "sales") NAMESPACE "crm""#).await;
    q(&e, r#"BRANCH FROM "crm" INTO "crm-try""#).await;
    q(&e, r#"BRANCH FROM "crm-try" INTO "crm-try2""#).await;
    q(&e, r#"FORGET FROM SEMANTIC WHERE key = "bob" NAMESPACE "crm""#).await;
    assert_eq!(roles(&e, "crm-try").await, ["engineer"], "the branch");
    assert_eq!(roles(&e, "crm-try2").await, ["engineer"], "and the branch of the branch");
    assert!(vals(&e, r#"RECALL FROM SEMANTIC WHERE key = "bob" NAMESPACE "crm-try""#, "role").await.is_empty(), "a key lookup too");
}

/// LIKE (ranked recall) and PROCEDURAL matching read a branch's view: its own records
/// and its base's at the point.
#[tokio::test]
async fn like_reads_a_branch() {
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO SEMANTIC (key = "tea", content = "ada likes green tea") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO PROCEDURAL (key = "restart", pattern = "pod OOMKilled restart", steps = "raise the memory limit") NAMESPACE "crm""#).await;
    q(&e, r#"BRANCH FROM "crm" INTO "crm-try""#).await;
    q(&e, r#"STORE INTO SEMANTIC (key = "coffee", content = "bob likes black coffee") NAMESPACE "crm-try""#).await;
    q(&e, r#"STORE INTO PROCEDURAL (key = "loop", pattern = "pod OOMKilled loop", steps = "find the leak") NAMESPACE "crm-try""#).await;
    let like = q(&e, r#"RECALL FROM SEMANTIC LIKE "who likes drinks" NAMESPACE "crm-try" LIMIT 10"#).await.to_string();
    assert!(like.contains("green tea") && like.contains("black coffee"), "inherited and own: {like}");
    let base = q(&e, r#"RECALL FROM SEMANTIC LIKE "who likes drinks" NAMESPACE "crm" LIMIT 10"#).await.to_string();
    assert!(!base.contains("black coffee"), "the base doesn't see it: {base}");
    let p = |ns: &'static str| {
        let e = &e;
        async move { vals(e, &format!(r#"RECALL FROM PROCEDURAL LIKE "pod OOMKilled restart" CONFIDENCE 0.4 NAMESPACE "{ns}""#), "pattern").await }
    };
    assert_eq!(p("crm-try").await, ["pod OOMKilled loop", "pod OOMKilled restart"], "the branch's and the inherited one");
    assert_eq!(p("crm").await, ["pod OOMKilled restart"], "the base: its own only");
}

/// A link made on a branch is the branch's: the base doesn't have it, UNLINK there
/// finds it once. Links the base had at the point are seen through.
#[tokio::test]
async fn a_link_on_a_branch_stays_on_it() {
    let (_d, e) = engine().await;
    for k in ["a", "b", "c"] {
        q(&e, &format!(r#"STORE INTO SEMANTIC (key = "{k}", role = "{k}") NAMESPACE "p""#)).await;
    }
    q(&e, r#"LINK FROM SEMANTIC WHERE key = "a" INTO SEMANTIC WHERE key = "c" TYPE "old" NAMESPACE "p""#).await;
    q(&e, r#"BRANCH FROM "p" INTO "p-b1""#).await;
    let l = q(&e, r#"LINK FROM SEMANTIC WHERE key = "a" INTO SEMANTIC WHERE key = "b" TYPE "rel" NAMESPACE "p-b1""#).await;
    assert_eq!(l["data"]["count"], 1, "{l}");
    let links = |ns: &'static str| {
        let e = &e;
        async move {
            let r = q(e, &format!(r#"RECALL FROM SEMANTIC WHERE key = "a" WITH LINKS NAMESPACE "{ns}""#)).await;
            let mut t: Vec<String> = r["data"]["records"][0]["data"]["links"].as_array().map(|a| a.iter().filter_map(|l| l["type"].as_str().or(l["link_type"].as_str()).map(String::from)).collect()).unwrap_or_default();
            t.sort();
            t
        }
    };
    assert_eq!(links("p").await, ["old"], "not on the base");
    let u = q(&e, r#"UNLINK FROM SEMANTIC WHERE key = "a" INTO SEMANTIC WHERE key = "b" NAMESPACE "p-b1""#).await;
    assert_eq!(u["data"]["count"], 1, "the link is on the branch: {u}");
    let u = q(&e, r#"UNLINK FROM SEMANTIC WHERE key = "a" INTO SEMANTIC WHERE key = "b" NAMESPACE "p-b1""#).await;
    assert_eq!(u["data"]["count"], 0, "and gone: {u}");
    assert_eq!(links("p").await, ["old"]);
}

/// BRANCH … AS OF reads every memory type as it was then: episodes and tools too.
#[tokio::test]
async fn a_branch_as_of_reads_episodes_and_tools_as_they_were() {
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO EPISODIC (key = "ep", content = "first telling") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO TOOLS (key = "t", name = "lookup", description = "old description", schema = "{}") NAMESPACE "crm""#).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let t = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    q(&e, r#"UPDATE INTO EPISODIC WHERE key = "ep" (content = "second telling") NAMESPACE "crm""#).await;
    q(&e, r#"UPDATE INTO TOOLS WHERE key = "t" (description = "new description") NAMESPACE "crm""#).await;
    q(&e, &format!(r#"BRANCH FROM "crm" INTO "crm-past" AS OF "{t}""#)).await;
    assert_eq!(vals(&e, r#"RECALL FROM EPISODIC NAMESPACE "crm-past""#, "content").await, ["first telling"]);
    assert_eq!(vals(&e, r#"RECALL FROM TOOLS NAMESPACE "crm-past""#, "description").await, ["old description"]);
    assert_eq!(vals(&e, r#"RECALL FROM EPISODIC NAMESPACE "crm""#, "content").await, ["second telling"]);
}

/// The branch API (A3T's review): stats and records of a branch are its namespace's.
#[tokio::test]
async fn the_branch_api_reads_the_branch_namespace() {
    use adb_core::MemoryType;
    let (_d, e) = engine().await;
    q(&e, r#"STORE INTO SEMANTIC (key = "ada", role = "engineer") NAMESPACE "crm""#).await;
    q(&e, r#"STORE INTO EPISODIC (key = "e1", role = "called") NAMESPACE "crm""#).await;
    let b = e.branch_create("crm", "try", None, Some("dev@a3t")).await.unwrap();
    assert_eq!((b.namespace.as_str(), b.name.as_str(), b.parent.as_str()), ("crm", "try", "main"));
    q(&e, r#"STORE INTO SEMANTIC (key = "cy", role = "intern") NAMESPACE "crm@try""#).await;
    let s = e.space_stats("crm", Some("try"), std::time::Duration::ZERO).await.unwrap();
    assert_eq!(s["tiers"]["SEMANTIC"]["count"], 2, "inherited and own: {s}");
    assert_eq!(s["tiers"]["EPISODIC"]["count"], 1, "{s}");
    let main = e.space_stats("crm", None, std::time::Duration::ZERO).await.unwrap();
    assert_eq!(main["tiers"]["SEMANTIC"]["count"], 1, "{main}");
    let r = e.space_records("crm", MemoryType::Semantic, Some("try"), None, 10).await.unwrap();
    assert_eq!(r["records"].as_array().unwrap().len(), 2, "{r}");
    // A branch of a namespace with no records yet (a new agent's space).
    e.branch_create("fresh", "try", None, None).await.unwrap();
    assert_eq!(e.branch_list(Some("fresh")).await.unwrap().len(), 1);
    // A past point is AQL's BRANCH … AS OF; the API cuts one now.
    assert!(e.branch_drop("crm", "try").await.unwrap());
    assert!(!e.branch_drop("crm", "try").await.unwrap(), "already gone");
}

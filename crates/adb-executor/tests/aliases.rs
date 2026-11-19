//! AQL v0.13 aliases (spec §4): a record's `aliases` are other keys for it. A read by
//! an alias, a LINK naming one, and links stored to or from one all reach the record.
//! Merging two entities is `UPDATE … (aliases = [...])` then FORGET the duplicate.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(e: &LocalEngine, aql: &str) -> Value {
    e.query(aql, None).await.unwrap_or_else(|x| panic!("{aql}: {x}"))
}

fn keys(v: &Value) -> Vec<String> {
    let mut k: Vec<String> = v["data"]["records"].as_array().unwrap().iter().filter_map(|r| r["data"]["key"].as_str().map(String::from)).collect();
    k.sort();
    k
}

fn link_types(v: &Value) -> Vec<String> {
    let mut t: Vec<String> =
        v["data"]["records"][0]["data"]["links"].as_array().map(|a| a.iter().filter_map(|l| l["type"].as_str().or(l["link_type"].as_str()).map(String::from)).collect()).unwrap_or_default();
    t.sort();
    t
}

#[tokio::test]
async fn aliases_reach_the_record() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for k in ["person:ada", "person:ada-l", "org:acme", "topic:maths"] {
            q(&e, &format!(r#"STORE INTO SEMANTIC (key = "{k}", name = "{k}") NAMESPACE "crm""#)).await;
        }
        q(&e, r#"LINK FROM SEMANTIC WHERE key = "person:ada-l" INTO SEMANTIC WHERE key = "org:acme" TYPE "works_at" NAMESPACE "crm""#).await;
        // The merge: Ada is also known as ada-l; the duplicate goes.
        q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "person:ada" (aliases = ["person:ada-l"]) NAMESPACE "crm""#).await;
        let f = q(&e, r#"FORGET FROM SEMANTIC WHERE key = "person:ada-l" NAMESPACE "crm""#).await;
        assert_eq!(f["data"]["count"], 1, "the duplicate's own record: {f}");
        assert_eq!(keys(&q(&e, r#"RECALL FROM SEMANTIC WHERE key = "person:ada-l" NAMESPACE "crm""#).await), ["person:ada"], "a read by the alias");
        // A LINK naming the alias is the record's link; the one stored from the alias too.
        q(&e, r#"LINK FROM SEMANTIC WHERE key = "person:ada-l" INTO SEMANTIC WHERE key = "topic:maths" TYPE "likes" NAMESPACE "crm""#).await;
        let r = q(&e, r#"RECALL FROM SEMANTIC WHERE key = "person:ada" WITH LINKS NAMESPACE "crm""#).await;
        assert_eq!(link_types(&r), ["likes", "works_at"], "{r}");
        // Another namespace's key of the same name is its own.
        q(&e, r#"STORE INTO SEMANTIC (key = "person:ada-l", name = "other") NAMESPACE "kb""#).await;
        assert_eq!(keys(&q(&e, r#"RECALL FROM SEMANTIC WHERE key = "person:ada-l" NAMESPACE "kb""#).await), ["person:ada-l"]);
    }
    // After a restart.
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(keys(&q(&e, r#"RECALL FROM SEMANTIC WHERE key = "person:ada-l" NAMESPACE "crm""#).await), ["person:ada"]);
    // An alias removed: the key no longer reaches the record.
    q(&e, r#"UPDATE INTO SEMANTIC WHERE key = "person:ada" (aliases = []) NAMESPACE "crm""#).await;
    assert!(keys(&q(&e, r#"RECALL FROM SEMANTIC WHERE key = "person:ada-l" NAMESPACE "crm""#).await).is_empty());
}

/// AQL v0.13: a RECALL may read several namespaces: NAMESPACE ("a", "b").
#[tokio::test]
async fn a_read_across_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&e, r#"STORE INTO SEMANTIC (key = "a1", n = 1) NAMESPACE "a""#).await;
    q(&e, r#"STORE INTO SEMANTIC (key = "b1", n = 2) NAMESPACE "b""#).await;
    q(&e, r#"STORE INTO SEMANTIC (key = "c1", n = 3) NAMESPACE "c""#).await;
    let r = q(&e, r#"RECALL FROM SEMANTIC NAMESPACE ("a", "b") ORDER BY n DESC"#).await;
    assert_eq!(r["data"]["records"].as_array().unwrap().iter().map(|x| x["data"]["key"].as_str().unwrap()).collect::<Vec<_>>(), ["b1", "a1"], "{r}");
    assert_eq!(q(&e, r#"RECALL FROM SEMANTIC NAMESPACE ("a", "b", "c") LIMIT 2"#).await["data"]["count"], 2);
    assert!(e.query(r#"RECALL FROM SEMANTIC NAMESPACE ("a", "b") AGGREGATE COUNT(*) AS n"#, None).await.is_err());
}

//! E-L12: keys are unique per (scope, memory type) (D3 S4). A record in a named
//! scope has the id `<scope>~<key>`; `KEY id` / `WHERE id` with `IN SCOPE` take the
//! bare key; payload fields stay bare. (AQL v0.13: `WHERE key = "x"` alone is an
//! exact lookup in the default scope; `key IN ("x")` filters across scopes.) Facts and their entities are per scope, so
//! one subject's facts in two scopes don't replace each other, and forgetting an
//! entity in one scope leaves the other's.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use adb_gateway::FactQuery;
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn records(v: &Value) -> Vec<Value> {
    v["data"]["records"].as_array().cloned().unwrap_or_default()
}

fn ids(v: &Value) -> Vec<String> {
    let mut i: Vec<String> = records(v).iter().filter_map(|r| r["id"].as_str().map(String::from)).collect();
    i.sort();
    i
}

fn objects(v: &Value) -> Vec<String> {
    let mut o: Vec<String> = records(v).iter().filter_map(|r| r["data"]["object"].as_str().map(String::from)).collect();
    o.sort();
    o
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_are_unique_per_scope() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        q(&engine, r#"STORE INTO SEMANTIC (key = "x", n = 0)"#).await;
        q(&engine, r#"STORE INTO SEMANTIC (key = "x", n = 1) IN SCOPE "team:a""#).await;
        q(&engine, r#"STORE INTO SEMANTIC (key = "x", n = 2) IN SCOPE "team:b""#).await;
        for aql in [r#"STORE INTO SEMANTIC (key = "x", n = 3) IN SCOPE "team:a""#, r#"STORE INTO SEMANTIC (key = "x", n = 3)"#] {
            let e = engine.query(aql, None).await.unwrap_err();
            assert!(e.contains("key_exists") || e.contains("exists"), "{aql}: {e}");
        }

        assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key IN ("x")"#).await), ["team:a~x", "team:b~x", "x"]);
        assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key = "x" IN SCOPE "team:a""#).await), ["team:a~x"]);
        let b = records(&q(&engine, r#"RECALL FROM SEMANTIC IN SCOPE "team:b" WHERE id = "x""#).await);
        assert_eq!((b.len(), b[0]["data"]["n"].as_i64(), b[0]["data"]["key"].as_str()), (1, Some(2), Some("x")));
        // The full id works in its scope; without IN SCOPE a bare key is the default scope's.
        assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC IN SCOPE "team:b" WHERE id = "team:b~x""#).await), ["team:b~x"]);
        assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC WHERE id = "x""#).await), ["x"]);
        assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC WHERE id = "x" IN SCOPE "team:a""#).await), ["team:a~x"]);

        let u = q(&engine, r#"UPDATE INTO SEMANTIC WHERE id = "x" (n = 10) IN SCOPE "team:a""#).await;
        assert_eq!(u["data"]["count"], 1, "{u}");
        let n: Vec<i64> = records(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key IN ("x")"#).await).iter().filter_map(|r| r["data"]["n"].as_i64()).collect();
        let mut n = n;
        n.sort();
        assert_eq!(n, [0, 2, 10]);

        for (aql, error) in [
            (r#"STORE INTO SEMANTIC (key = "a~b", n = 1)"#, "can't contain '~'"),
            (r#"STORE INTO SEMANTIC (key = "team:b~y", n = 1) IN SCOPE "team:a""#, "can't contain '~'"),
        ] {
            let e = engine.query(aql, None).await.unwrap_err();
            assert!(e.contains(error), "{aql}: {e}");
        }
    }
    // After a restart the keys are still per scope.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert!(engine.query(r#"STORE INTO SEMANTIC (key = "x", n = 4) IN SCOPE "team:b""#, None).await.is_err());
    q(&engine, r#"STORE INTO SEMANTIC (key = "x", n = 5) IN SCOPE "team:c""#).await;
    assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key IN ("x")"#).await), ["team:a~x", "team:b~x", "team:c~x", "x"]);
}

/// Facts per scope (the fact layer: FACT statements, the fact feed). AQL v0.13 has
/// no FACT: relationships are LINKs, values fields.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "AQL v0.13: FACT is being replaced (relationships are LINKs, values are fields)"]
async fn facts_are_per_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    {
        // Facts per scope: ada's employer differs by scope, and neither replaces the other.
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme") IN SCOPE "team:a""#).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:globex") IN SCOPE "team:b""#).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:bob", predicate = "knows", object = "person:ada") IN SCOPE "team:a""#).await;
        assert_eq!(objects(&q(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" IN SCOPE "team:a""#).await), ["org:acme"]);
        assert_eq!(objects(&q(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#).await), ["org:acme", "org:globex"]);
        // The ends are the scope's entities.
        assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC IN SCOPE "team:b" WHERE id = "person:ada""#).await), ["team:b~person:ada"]);
        let reached = q(&engine, r#"RECALL FROM SEMANTIC IN SCOPE "team:a" WHERE id = "person:bob" FOLLOW LINKS"#).await;
        assert!(ids(&reached).contains(&"team:a~person:ada".to_string()), "{reached}");

        // The feed: bare subjects, scoped ids, one fact per scope.
        let gw = engine.gateway().unwrap();
        let mut seen = Vec::new();
        for _ in 0..50 {
            let b = gw.facts(&FactQuery { max: 100, ..Default::default() }).await.unwrap();
            seen = b.changes.iter().filter_map(|c| c.fact.as_ref().map(|f| (c.scope.clone(), c.id.clone(), f.subject.clone(), f.object.clone().unwrap_or_default()))).collect();
            if seen.len() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        seen.sort();
        assert_eq!(seen[..2], [
            ("team:a".into(), "team:a~fact:person:ada|works_at".into(), "person:ada".into(), "org:acme".into()),
            ("team:a".into(), "team:a~fact:person:bob|knows".into(), "person:bob".into(), "person:ada".into()),
        ]);

        // Forgetting team:a's ada forgets team:a's facts about her only (GR-13 per scope).
        let f = q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "person:ada" IN SCOPE "team:a""#).await;
        assert_eq!(f["data"]["count"], 3, "the entity and its two facts: {f}");
        assert_eq!(objects(&q(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#).await), ["org:globex"]);

    }
}

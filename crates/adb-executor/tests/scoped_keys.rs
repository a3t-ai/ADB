//! E-L12: keys are unique per (scope, memory type) (D3 S4). A record in a named
//! scope has the id `<scope>~<key>`; `KEY id` / `WHERE id` with `IN SCOPE` take the
//! bare key; payload fields stay bare. (AQL v0.13: `WHERE key = "x"` alone is an
//! exact lookup in the default scope; `key IN ("x")` filters across scopes.)
//! Forgetting a key in one scope leaves the other's.


use adb_executor::{Engine, LocalEngine};
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

/// FORGET in one scope leaves the same key in another (GR-13 per scope; AQL v0.13 has no
/// FACT, so this is about records).
#[tokio::test(flavor = "multi_thread")]
async fn forget_is_per_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC (key = "person:ada", employer = "acme") IN SCOPE "team:a""#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "person:ada", employer = "globex") IN SCOPE "team:b""#).await;
    let f = q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "person:ada" IN SCOPE "team:a""#).await;
    assert_eq!(f["data"]["count"], 1, "{f}");
    assert_eq!(ids(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key IN ("person:ada")"#).await), ["team:b~person:ada"]);
}

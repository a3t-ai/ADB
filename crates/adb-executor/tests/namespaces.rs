//! NS (schema v5, `.ai/NS_IDS.md`): keys are unique per namespace — two agents hold
//! the same key apart; a record's fields stay in its own namespace; a key names a
//! record of the statement's NAMESPACE (none: a record without one).

use adb_executor::{Engine, LocalEngine};

fn facts(answer: &serde_json::Value, field: &str) -> Vec<String> {
    let recs = answer.pointer("/data/records").or_else(|| answer.pointer("/data/data/records")).and_then(|r| r.as_array()).cloned().unwrap_or_default();
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer(&format!("/data/{field}")).and_then(|x| x.as_str()).map(String::from)).collect();
    v.sort();
    v
}

#[tokio::test]
async fn two_agents_hold_the_same_key_apart() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let q = |aql: String| {
        let e = &e;
        async move { e.query(&aql, None).await }
    };
    for (ns, v) in [("acme--a", "tea"), ("acme--b", "coffee")] {
        q(format!(r#"STORE INTO SEMANTIC (key = "pref", fact = "{v}") NAMESPACE "{ns}""#)).await.unwrap();
        q(format!(r#"STORE INTO SEMANTIC (key = "user:alice", drinks = "{v}") NAMESPACE "{ns}""#)).await.unwrap();
    }
    assert!(q(r#"STORE INTO SEMANTIC (key = "pref", fact = "x") NAMESPACE "acme--a""#.into()).await.is_err(), "still unique within a namespace");
    for (ns, v) in [("acme--a", "tea"), ("acme--b", "coffee")] {
        let r = q(format!(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "{ns}""#)).await.unwrap();
        assert_eq!(facts(&r, "fact"), [v], "{ns}");
        let f = q(format!(r#"RECALL FROM SEMANTIC WHERE key = "user:alice" NAMESPACE "{ns}""#)).await.unwrap();
        assert_eq!(facts(&f, "drinks"), [v], "{ns}: its own value");
        let stub = q(format!(r#"RECALL FROM SEMANTIC WHERE key = "user:alice" NAMESPACE "{ns}""#)).await.unwrap();
        assert_eq!(facts(&stub, "key"), ["user:alice"], "{ns}: its own entity stub");
    }
    // AQL v0.13: a RECALL without NAMESPACE reads every namespace (an exact key lookup
    // is within the statement's namespace): both agents' records of the key.
    let mut both = facts(&q(r#"RECALL FROM SEMANTIC WHERE key = "pref""#.into()).await.unwrap(), "fact");
    both.sort();
    assert_eq!(both, ["coffee", "tea"]);
    // FORGET in one namespace leaves the other.
    q(r#"FORGET FROM SEMANTIC WHERE id = "pref" NAMESPACE "acme--a""#.into()).await.unwrap();
    assert_eq!(facts(&q(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--b""#.into()).await.unwrap(), "fact"), ["coffee"]);
    // UPDATE by key in a namespace.
    q(r#"UPDATE INTO SEMANTIC WHERE id = "pref" (fact = "latte") NAMESPACE "acme--b""#.into()).await.unwrap();
    assert_eq!(facts(&q(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--b""#.into()).await.unwrap(), "fact"), ["latte"]);
    // The record's id carries the namespace; its key stays bare.
    let r = q(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--b""#.into()).await.unwrap();
    let s = r.to_string();
    assert!(s.contains(r#""id":"acme--b|~pref""#) && s.contains(r#""key":"pref""#), "{s}");
    assert!(q(r#"STORE INTO SEMANTIC (key = "k", fact = "f") NAMESPACE "a|b""#.into()).await.is_err(), "'|' is reserved");
    // UPDATE / FORGET by a payload field stay in the statement's namespace (the same
    // key is in both).
    q(r#"STORE INTO SEMANTIC (key = "pref", fact = "chai") NAMESPACE "acme--a""#.into()).await.unwrap();
    q(r#"UPDATE INTO SEMANTIC WHERE key = "pref" (fact = "masala") NAMESPACE "acme--a""#.into()).await.unwrap();
    assert_eq!(facts(&q(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--b""#.into()).await.unwrap(), "fact"), ["latte"], "B untouched by A's UPDATE");
    q(r#"FORGET FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--a""#.into()).await.unwrap();
    assert_eq!(facts(&q(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--b""#.into()).await.unwrap(), "fact"), ["latte"], "B untouched by A's FORGET");
    // Procedures with a pattern_id: same id in two namespaces, updated apart.
    for ns in ["acme--a", "acme--b"] {
        q(format!(r#"STORE INTO PROCEDURAL (pattern_id = "deploy", pattern = "deploy {ns}", steps = "build") NAMESPACE "{ns}""#)).await.unwrap();
    }
    q(r#"UPDATE INTO PROCEDURAL WHERE pattern_id = "deploy" (pattern = "deploy v2") NAMESPACE "acme--a""#.into()).await.unwrap();
    let b = q(r#"RECALL FROM PROCEDURAL NAMESPACE "acme--b""#.into()).await.unwrap().to_string();
    let a = q(r#"RECALL FROM PROCEDURAL NAMESPACE "acme--a""#.into()).await.unwrap().to_string();
    assert!(a.contains("deploy v2") && b.contains("deploy acme--b") && !b.contains("deploy v2"), "{a}\n{b}");
    // A restart keeps it all (ids hydrate from the table).
    drop(e);
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = e.query(r#"RECALL FROM SEMANTIC WHERE key = "pref" NAMESPACE "acme--b""#, None).await.unwrap();
    assert_eq!(facts(&r, "fact"), ["latte"]);
    assert!(e.query(r#"STORE INTO SEMANTIC (key = "pref", fact = "again") NAMESPACE "acme--a""#, None).await.is_ok(), "A's was forgotten: the key is free there");
    assert!(e.query(r#"STORE INTO SEMANTIC (key = "pref", fact = "again") NAMESPACE "acme--b""#, None).await.is_err(), "B's is still there");
}

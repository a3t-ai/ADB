//! GR-13: forgetting an entity forgets the facts about it (as subject or object),
//! every version, in the same log record; other entities and facts stay.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use adb_gateway::FactQuery;
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn count(v: &Value) -> usize {
    v["data"]["records"].as_array().map(|r| r.len()).unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "AQL v0.13: FACT is being replaced (relationships are LINKs, values are fields)"]
async fn forgetting_an_entity_forgets_its_facts() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:globex")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "age", value = 36)"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:bob", predicate = "knows", object = "person:ada")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "org:acme", predicate = "located_in", object = "city:york")"#).await;

    let r = q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "person:ada""#).await;
    assert_eq!(r["data"]["count"], 4, "the entity and its three facts: {r}");

    for aql in [
        r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#,
        r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" WITH HISTORY"#,
        r#"RECALL FROM SEMANTIC WHERE object = "person:ada""#,
        r#"RECALL FROM SEMANTIC WHERE key = "person:ada""#,
    ] {
        assert_eq!(count(&q(&engine, aql).await), 0, "{aql}");
    }
    // The other ends stay, and facts not about ada.
    for key in ["person:bob", "org:acme", "org:globex", "city:york"] {
        assert_eq!(count(&q(&engine, &format!(r#"RECALL FROM SEMANTIC WHERE key = "{key}""#)).await), 1, "{key}");
    }
    assert_eq!(count(&q(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "org:acme""#).await), 1);
    // A link walk from bob no longer reaches ada.
    let linked = q(&engine, r#"RECALL FROM SEMANTIC WHERE id = "person:bob" FOLLOW LINKS"#).await;
    assert!(!linked.to_string().contains("person:ada"), "{linked}");

    // The fact feed's copy agrees.
    let gw = engine.gateway().unwrap();
    let mut ids = Vec::new();
    for _ in 0..50 {
        let b = gw.facts(&FactQuery { max: 100, ..Default::default() }).await.unwrap();
        ids = b.changes.iter().map(|c| c.id.clone()).collect();
        if ids.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ids, ["fact:org:acme|located_in"]);

    // Forgetting a fact leaves its entities.
    q(&engine, r#"FORGET FROM SEMANTIC WHERE predicate = "located_in""#).await;
    assert_eq!(count(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key = "org:acme""#).await), 1);
}

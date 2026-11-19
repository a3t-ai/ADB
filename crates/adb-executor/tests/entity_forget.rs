//! GR-13 in AQL v0.13: forgetting a record forgets the links that touch it (either
//! end), every version; the records at their other ends, and links not about it, stay.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn count(v: &Value) -> usize {
    v["data"]["records"].as_array().map(|r| r.len()).unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn forgetting_a_record_forgets_its_links() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for (key, name) in [("person:ada", "Ada"), ("person:bob", "Bob"), ("org:acme", "Acme"), ("org:globex", "Globex"), ("city:york", "York")] {
        q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "{key}", name = "{name}") NAMESPACE "crm""#)).await;
    }
    // Ada's age is a field of hers (v0.13: values are fields, relationships links).
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE key = "person:ada" (age = 36) NAMESPACE "crm""#).await;
    let link = |from: &str, to: &str, t: &str| format!(r#"LINK FROM SEMANTIC WHERE key = "{from}" INTO SEMANTIC WHERE key = "{to}" TYPE "{t}" NAMESPACE "crm""#);
    q(&engine, &link("person:ada", "org:acme", "works_at")).await;
    q(&engine, &link("person:ada", "org:globex", "works_at")).await;
    q(&engine, &link("person:bob", "person:ada", "knows")).await;
    q(&engine, &link("org:acme", "city:york", "located_in")).await;

    let r = q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "person:ada" NAMESPACE "crm""#).await;
    assert_eq!(r["data"]["count"], 1, "{r}");

    for aql in [
        r#"RECALL FROM SEMANTIC WHERE key = "person:ada" NAMESPACE "crm""#,
        r#"RECALL FROM SEMANTIC WHERE key = "person:ada" NAMESPACE "crm" WITH HISTORY"#,
    ] {
        assert_eq!(count(&q(&engine, aql).await), 0, "{aql}");
    }
    // The other ends stay.
    for key in ["person:bob", "org:acme", "org:globex", "city:york"] {
        assert_eq!(count(&q(&engine, &format!(r#"RECALL FROM SEMANTIC WHERE key = "{key}" NAMESPACE "crm""#)).await), 1, "{key}");
    }
    // No link reaches ada any more, from either side; acme's own link stays.
    let bob = q(&engine, r#"RECALL FROM SEMANTIC WHERE key = "person:bob" NAMESPACE "crm" FOLLOW LINKS"#).await;
    assert!(!bob.to_string().contains("person:ada"), "{bob}");
    let acme = q(&engine, r#"RECALL FROM SEMANTIC WHERE key = "org:acme" NAMESPACE "crm" WITH LINKS"#).await;
    assert!(!acme.to_string().contains("person:ada"), "{acme}");
    assert!(acme.to_string().contains("located_in"), "{acme}");

    // Forgetting a link (UNLINK) leaves its records.
    q(&engine, r#"UNLINK FROM SEMANTIC WHERE key = "org:acme" INTO SEMANTIC WHERE key = "city:york" TYPE "located_in" NAMESPACE "crm""#).await;
    assert_eq!(count(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key = "org:acme" NAMESPACE "crm""#).await), 1);
    assert_eq!(count(&q(&engine, r#"RECALL FROM SEMANTIC WHERE key = "city:york" NAMESPACE "crm""#).await), 1);
}

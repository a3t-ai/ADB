//! GR-12: MERGE ENTITY moves a duplicate's facts and links to the survivor (the
//! newer of two facts for one key is current, the older history), forgets the
//! duplicate, and makes its key an alias: facts stored with it, and LOOKUP KEY of
//! it, reach the survivor — also after a restart.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn records(v: &Value) -> Vec<Value> {
    v["data"]["records"].as_array().cloned().unwrap_or_default()
}

/// The facts' objects or values, sorted.
async fn objects(engine: &LocalEngine, aql: &str) -> Vec<String> {
    let mut o: Vec<String> = records(&q(engine, aql).await)
        .iter()
        .map(|r| r["data"]["object"].as_str().map(String::from).unwrap_or_else(|| r["data"]["value"].to_string()))
        .collect();
    o.sort();
    o
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "AQL v0.13: MERGE ENTITY is being replaced (aliases: UPDATE … (aliases = [...]) + FORGET)"]
async fn merge_moves_facts_and_links_to_the_survivor() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        q(&engine, r#"STORE INTO SEMANTIC (key = "person:ada", name = "Ada")"#).await;
        // The duplicate's lives_in is older than the survivor's: the survivor's stays.
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada-lovelace", predicate = "lives_in", object = "city:paris")"#).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "lives_in", object = "city:london")"#).await;
        // The duplicate's works_at is newer: it becomes current, acme history.
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme")"#).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada-lovelace", predicate = "works_at", object = "org:globex")"#).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT MULTI (subject = "person:ada-lovelace", predicate = "wrote", object = "note:g")"#).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:bob", predicate = "knows", object = "person:ada-lovelace")"#).await;
        q(&engine, r#"STORE INTO SEMANTIC (key = "org:x", name = "X")"#).await;
        q(&engine, r#"LINK FROM SEMANTIC WHERE key = "person:ada-lovelace" INTO SEMANTIC WHERE key = "org:x" TYPE "funded_by""#).await;

        let r = q(&engine, r#"MERGE ENTITY "person:ada-lovelace" INTO "person:ada""#).await;

        assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#).await, ["city:london", "note:g", "org:globex"]);
        assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AND predicate = "works_at" WITH HISTORY"#).await, ["org:acme", "org:globex"]);
        assert_eq!(records(&q(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada-lovelace""#).await).len(), 0);
        assert_eq!(records(&q(&engine, r#"RECALL FROM SEMANTIC WHERE object = "person:ada-lovelace""#).await).len(), 0);
        assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE predicate = "knows""#).await, ["person:ada"]);
        let reached = q(&engine, r#"RECALL FROM SEMANTIC WHERE id = "person:ada" FOLLOW LINKS"#).await;
        let ids: Vec<String> = records(&reached).iter().filter_map(|r| r["id"].as_str().map(String::from)).collect();
        assert!(ids.contains(&"org:x".to_string()) && ids.contains(&"org:globex".to_string()), "{ids:?}");

        assert_eq!(r["data"]["count"], 4, "works_at, wrote, knows and the link: {r}");
        // The duplicate's key is an alias now.
        let found = records(&q(&engine, r#"RECALL FROM SEMANTIC WHERE id = "person:ada-lovelace""#).await);
        assert_eq!(found.len(), 1);
        assert_eq!((found[0]["id"].as_str(), found[0]["data"]["name"].as_str()), (Some("person:ada"), Some("Ada")));
        assert_eq!(found[0]["data"]["aliases"], serde_json::json!(["person:ada-lovelace"]));
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada-lovelace", predicate = "age", value = 36)"#).await;
        assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AND predicate = "age""#).await, ["36"]);

        for (aql, error) in [
            (r#"STORE INTO SEMANTIC (key = "person:ada-lovelace", name = "again")"#, "was merged into person:ada"),
            (r#"MERGE ENTITY "person:ada-lovelace" INTO "person:bob""#, "already merged"),
            (r#"MERGE ENTITY "person:nobody" INTO "person:ada""#, "no entity person:nobody"),
            (r#"MERGE ENTITY "person:ada" INTO "person:ada-lovelace""#, "is person:ada"),
        ] {
            let e = engine.query(aql, None).await.unwrap_err();
            assert!(e.contains(error), "{aql}: {e}");
        }
    }
    // Aliases come back from the table.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada-lovelace", predicate = "plays", object = "game:chess")"#).await;
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AND predicate = "plays""#).await, ["game:chess"]);
    // A chain: merging the survivor on moves its aliases along.
    q(&engine, r#"STORE INTO SEMANTIC (key = "person:countess", name = "Countess")"#).await;
    q(&engine, r#"MERGE ENTITY "person:ada" INTO "person:countess""#).await;
    let found = records(&q(&engine, r#"RECALL FROM SEMANTIC WHERE id = "person:ada-lovelace""#).await);
    assert_eq!(found[0]["id"], "person:countess");
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:countess" AND predicate = "works_at""#).await, ["org:globex"]);
}

//! AQL v0.12/v0.13: LINK, UNLINK and the topic statements name a NAMESPACE.
//! A link stays within its namespace (both ends there); a namespace's topics are its
//! own (`<ns>/<topic>`), and its subscriptions are ended only within it.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn count(v: &Value) -> u64 {
    v["data"]["count"].as_u64().unwrap_or_else(|| panic!("a count: {v}"))
}

#[tokio::test]
async fn a_link_with_a_namespace_stays_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for ns in ["p", "q"] {
        q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "a", n = 1) NAMESPACE "{ns}""#)).await;
        q(&engine, &format!(r#"STORE INTO EPISODIC (key = "b", n = 2) NAMESPACE "{ns}""#)).await;
    }
    let l = q(&engine, r#"LINK FROM SEMANTIC WHERE key = "a" INTO EPISODIC WHERE key = "b" TYPE "rel" WEIGHT 0.5 NAMESPACE "p""#).await;
    assert_eq!(count(&l), 1, "only p's records: {l}");
    let with = |ns: &'static str| {
        let e = &engine;
        async move {
            let r = q(e, &format!(r#"RECALL FROM SEMANTIC WHERE key = "a" WITH LINKS NAMESPACE "{ns}""#)).await;
            r["data"]["records"][0]["data"]["links"].as_array().map_or(0, |a| a.len())
        }
    };
    assert_eq!((with("p").await, with("q").await), (1, 0));
    // LINK on a link that exists sets its weight (create or update): p's is updated,
    // q gets its own.
    let u = q(&engine, r#"LINK FROM SEMANTIC WHERE key = "a" INTO EPISODIC WHERE key = "b" TYPE "rel" WEIGHT 0.9 NAMESPACE "p""#).await;
    assert_eq!(count(&u), 1, "{u}");
    assert_eq!((with("p").await, with("q").await), (1, 0), "updated, not a second link");
    let u = q(&engine, r#"LINK FROM SEMANTIC WHERE key = "a" INTO EPISODIC WHERE key = "b" TYPE "rel" WEIGHT 0.9 NAMESPACE "q""#).await;
    assert_eq!(count(&u), 1, "{u}");
    let d = q(&engine, r#"UNLINK FROM SEMANTIC WHERE key = "a" INTO EPISODIC WHERE key = "b" NAMESPACE "p""#).await;
    assert_eq!(count(&d), 1, "{d}");
    assert_eq!((with("p").await, with("q").await), (0, 1), "q's link stays");
    // A LINK naming a namespace without the record: refused, as with none at all.
    let e = engine.query(r#"LINK FROM SEMANTIC WHERE key = "a" INTO EPISODIC WHERE key = "b" TYPE "rel" NAMESPACE "r""#, None).await.unwrap_err();
    assert!(e.contains("No source records"), "{e}");
}

#[tokio::test]
async fn a_namespace_has_its_own_topics_and_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();
    let sub = |aql: &'static str| {
        let e = &engine;
        async move { q(e, aql).await["data"]["subscription"].as_str().unwrap().to_string() }
    };
    let p = sub(r#"SUBSCRIBE TOPIC "orders" NAMESPACE "p""#).await;
    let org = sub(r#"SUBSCRIBE TOPIC "orders""#).await;
    let pub_p = q(&engine, r#"PUBLISH TOPIC "orders" (n = 1) NAMESPACE "p""#).await;
    assert_eq!(pub_p["data"]["topic"], "p/orders", "{pub_p}");
    q(&engine, r#"PUBLISH TOPIC "orders" (n = 2) NAMESPACE "q""#).await;
    q(&engine, r#"PUBLISH TOPIC "orders" (n = 3)"#).await;
    let ns_of = |b: &adb_gateway::Batch| -> String { serde_json::to_string(&b.deliveries).unwrap() };
    let got_p = gw.next(&p, None, 10, Duration::from_millis(300)).await.unwrap();
    let got_org = gw.next(&org, None, 10, Duration::from_millis(300)).await.unwrap();
    assert_eq!(got_p.deliveries.len(), 1, "p's subscription sees p's message only: {:?}", ns_of(&got_p));
    assert_eq!(got_org.deliveries.len(), 1, "the org-wide topic sees its own message only: {:?}", ns_of(&got_org));

    // Ended only within the namespace. (AQL v0.13 has no ACK statement: acks are the
    // server's API.)
    let u = q(&engine, &format!(r#"UNSUBSCRIBE "{p}" NAMESPACE "q""#)).await;
    assert_eq!(u["data"]["unsubscribed"], false, "{u}");
    let u = q(&engine, &format!(r#"UNSUBSCRIBE "{p}" NAMESPACE "p""#)).await;
    assert_eq!(u["data"]["unsubscribed"], true, "{u}");
}

/// AQL v0.13: USE names the namespace once for the statements after it; a statement's
/// own NAMESPACE wins; a failing statement stops the script, named by position.
#[tokio::test]
async fn use_and_scripts() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = q(&engine, r#"USE "p"; STORE INTO SEMANTIC (key = "a", n = 1); STORE INTO SEMANTIC (key = "b", n = 2) NAMESPACE "q"; RECALL FROM SEMANTIC WHERE key = "a""#).await;
    assert_eq!(r["data"]["count"], 1, "the last statement's answer: {r}");
    let n = |ns: &'static str| {
        let e = &engine;
        async move { q(e, &format!(r#"RECALL FROM SEMANTIC NAMESPACE "{ns}""#)).await["data"]["count"].as_u64().unwrap() }
    };
    assert_eq!((n("p").await, n("q").await), (1, 1));
    let e = engine.query(r#"USE "p"; STORE INTO SEMANTIC (key = "c", n = 1); STORE INTO SEMANTIC (key = "c", n = 2)"#, None).await.unwrap_err();
    assert!(e.starts_with("statement 3:"), "{e}");
    assert_eq!(n("p").await, 2, "the statements before it stay done");
    assert!(engine.query(r#"USE "p""#, None).await.is_ok());
}

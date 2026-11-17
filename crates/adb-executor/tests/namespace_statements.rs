//! AQL v0.12: LINK, UPDATE LINK, UNLINK and the topic statements name a NAMESPACE.
//! A link stays within its namespace (both ends there); a namespace's topics are its
//! own (`<ns>/<topic>`), and its subscriptions are acked or ended only within it.

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
    let l = q(&engine, r#"LINK FROM SEMANTIC WHERE key = "a" TO EPISODIC WHERE key = "b" TYPE "rel" WEIGHT 0.5 NAMESPACE "p""#).await;
    assert_eq!(count(&l), 1, "only p's records: {l}");
    let with = |ns: &'static str| {
        let e = &engine;
        async move {
            let r = q(e, &format!(r#"RECALL FROM SEMANTIC WHERE key = "a" WITH LINKS ALL NAMESPACE "{ns}""#)).await;
            r["data"]["records"][0]["data"]["links"].as_array().map_or(0, |a| a.len())
        }
    };
    assert_eq!((with("p").await, with("q").await), (1, 0));
    let u = q(&engine, r#"UPDATE LINK FROM SEMANTIC WHERE key = "a" TO EPISODIC WHERE key = "b" TYPE "rel" WEIGHT 0.9 NAMESPACE "q""#).await;
    assert_eq!(count(&u), 0, "q has no link: {u}");
    let u = q(&engine, r#"UPDATE LINK FROM SEMANTIC WHERE key = "a" TO EPISODIC WHERE key = "b" TYPE "rel" WEIGHT 0.9 NAMESPACE "p""#).await;
    assert_eq!(count(&u), 1, "{u}");
    let d = q(&engine, r#"UNLINK FROM SEMANTIC WHERE key = "a" TO EPISODIC WHERE key = "b" NAMESPACE "p""#).await;
    assert_eq!(count(&d), 1, "{d}");
    assert_eq!(with("p").await, 0);
    // A LINK naming a namespace without the record: refused, as with none at all.
    let e = engine.query(r#"LINK FROM SEMANTIC WHERE key = "a" TO EPISODIC WHERE key = "b" TYPE "rel" NAMESPACE "r""#, None).await.unwrap_err();
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

    // Acked and ended only within the namespace.
    let off = got_p.next;
    let e = engine.query(&format!(r#"ACK "{p}" OFFSET {off} NAMESPACE "q""#), None).await.unwrap_err();
    assert!(e.to_lowercase().contains("not found") || e.contains("subscription"), "{e}");
    q(&engine, &format!(r#"ACK "{p}" OFFSET {off} NAMESPACE "p""#)).await;
    let e = engine.query(&format!(r#"ACK "{org}" OFFSET 1 NAMESPACE "p""#), None).await.unwrap_err();
    assert!(!e.is_empty());
    let u = q(&engine, &format!(r#"UNSUBSCRIBE "{p}" NAMESPACE "q""#)).await;
    assert_eq!(u["data"]["unsubscribed"], false, "{u}");
    let u = q(&engine, &format!(r#"UNSUBSCRIBE "{p}" NAMESPACE "p""#)).await;
    assert_eq!(u["data"]["unsubscribed"], true, "{u}");
    // A scope subscription can't name a namespace (yet).
    assert!(engine.query(r#"SUBSCRIBE SCOPE "agent:1" NAMESPACE "p""#, None).await.is_err());
}

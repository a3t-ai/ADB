//! Stage E: subscriptions on one node (E_SUBSCRIPTIONS.md) through the engine:
//! AQL statements to register, publish and ack; the gateway for deliveries.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use adb_gateway::{Batch, Gateway};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

async fn subscribe(engine: &LocalEngine, aql: &str) -> String {
    let r = q(engine, aql).await;
    assert_eq!(r["data"]["type"], "subscription", "{r}");
    r["data"]["subscription"].as_str().unwrap().to_string()
}

/// Everything delivered to `sub` from its cursor up to the applied tip.
async fn drain(gw: &Gateway, sub: &str, from: Option<u64>) -> Batch {
    let mut all = Batch::default();
    let mut from = from;
    loop {
        let b = gw.next(sub, from, 3, Duration::from_millis(300)).await.unwrap();
        all.tip = b.tip;
        all.next = b.next;
        let empty = b.deliveries.is_empty();
        all.deliveries.extend(b.deliveries);
        if empty {
            return all;
        }
        from = Some(b.next);
    }
}

fn offsets(b: &Batch) -> Vec<u64> {
    b.deliveries.iter().map(|d| d.offset).collect()
}

#[tokio::test]
async fn two_topic_subscribers_see_the_same_sequence_and_resume_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (s1, s2, published, acked);
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        let gw = engine.gateway().expect("durable engines have a gateway");
        s1 = subscribe(&engine, r#"SUBSCRIBE TOPIC "orders""#).await;
        s2 = subscribe(&engine, r#"SUBSCRIBE TOPIC "orders""#).await;
        let mut p = Vec::new();
        for i in 0..5 {
            let r = q(&engine, &format!(r#"PUBLISH TOPIC "orders" (order_id = "o-{i}", status = "paid")"#)).await;
            p.push(r["data"]["offset"].as_u64().unwrap());
            // Interleaved writes that aren't topic messages: never delivered on a topic.
            q(&engine, &format!(r#"STORE INTO WORKING (key = "w{i}", step = {i})"#)).await;
            q(&engine, &format!(r#"PUBLISH TOPIC "other" (i = {i})"#)).await;
        }
        published = p;
        let a = drain(&gw, &s1, None).await;
        let b = drain(&gw, &s2, None).await;
        assert_eq!(offsets(&a), published, "subscriber 1 sees exactly the topic's messages, in order");
        assert_eq!(a.deliveries, b.deliveries, "both subscribers see identical deliveries");
        let c = &a.deliveries[0].changes[0];
        assert_eq!((c.op.as_str(), c.topic.as_deref()), ("publish", Some("orders")));
        assert_eq!(c.value.as_ref().unwrap()["order_id"], "o-0");
        // Subscriber 1 acks the third message.
        acked = published[2];
        let r = q(&engine, &format!(r#"ACK "{s1}" OFFSET {acked}"#)).await;
        assert_eq!(r["data"]["cursor"], acked);
        // Topic messages are not memory records.
        let ep = q(&engine, "RECALL FROM EPISODIC ALL").await;
        assert_eq!(ep["data"]["count"], 0, "{ep}");
        let stats = engine.stats().await.unwrap();
        assert_eq!(stats["episodic"], 0, "{stats}");
        assert_eq!(stats["working"], 5, "{stats}");
    }
    // Restart: registrations and cursors come back from the table.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();
    assert_eq!(gw.registry().len(), 2);
    assert_eq!(offsets(&drain(&gw, &s1, None).await), published[3..].to_vec(), "subscriber 1 resumes after its ack");
    assert_eq!(offsets(&drain(&gw, &s2, None).await), published, "subscriber 2 never acked: everything again");
    // A new message reaches both, live.
    let waiter = {
        let gw = gw.clone();
        let s2 = s2.clone();
        tokio::spawn(async move { gw.next(&s2, Some(published[4] + 1), 10, Duration::from_secs(5)).await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let r = q(&engine, r#"PUBLISH TOPIC "orders" (order_id = "o-5")"#).await;
    let live = waiter.await.unwrap();
    assert_eq!(offsets(&live), vec![r["data"]["offset"].as_u64().unwrap()], "a waiting subscriber gets the new message");
}

#[tokio::test]
async fn scope_subscription_sees_record_changes_with_filters() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();
    let all = subscribe(&engine, r#"SUBSCRIBE SCOPE "tenant:default""#).await;
    let facts = subscribe(&engine, r#"SUBSCRIBE SCOPE "tenant:default" KINDS entity WHERE concept = "acme""#).await;
    let paid = subscribe(&engine, r#"SUBSCRIBE TOPIC "orders" WHERE status = "paid""#).await;
    for aql in [
        r#"STORE INTO SEMANTIC (key = "acme", concept = "acme", content = "renews 1 Nov")"#,
        r#"STORE INTO SEMANTIC (key = "beta", concept = "beta")"#,
        r#"STORE INTO EPISODIC (key = "e1", bid_id = "e1", won = false)"#,
        r#"UPDATE INTO EPISODIC WHERE bid_id = "e1" (won = true)"#,
        r#"UPDATE INTO SEMANTIC WHERE concept = "acme" (content = "renews 15 Nov")"#,
        r#"LINK FROM SEMANTIC WHERE concept = "acme" TO EPISODIC WHERE bid_id = "e1" TYPE "evidence" WEIGHT 0.8"#,
        r#"FORGET FROM EPISODIC WHERE bid_id = "e1""#,
        r#"PUBLISH TOPIC "orders" (status = "paid", n = 1)"#,
        r#"PUBLISH TOPIC "orders" (status = "open", n = 2)"#,
    ] {
        q(&engine, aql).await;
    }
    let ops = |b: &Batch| b.deliveries.iter().flat_map(|d| d.changes.iter().map(|c| format!("{}:{}", c.op, c.id))).collect::<Vec<_>>();
    let a = drain(&gw, &all, None).await;
    assert_eq!(
        ops(&a),
        ["put:acme", "put:beta", "put:e1", "patch:e1", "supersede:acme", &format!("link:{}", a.deliveries[5].changes[0].id), "forget:e1"],
        "every record change in the scope, no topic messages"
    );
    assert_eq!(a.deliveries[4].changes[0].version, Some(2));
    let f = drain(&gw, &facts, None).await;
    assert_eq!(ops(&f), ["put:acme", "supersede:acme"], "KINDS entity WHERE concept = acme");
    let p = drain(&gw, &paid, None).await;
    assert_eq!(p.deliveries.len(), 1, "WHERE status = paid on the topic");
    assert_eq!(p.deliveries[0].changes[0].value.as_ref().unwrap()["n"], 1);

    // FROM OFFSET replays from that offset; UNSUBSCRIBE ends the subscription.
    let from = a.deliveries[3].offset;
    let replay = subscribe(&engine, &format!(r#"SUBSCRIBE SCOPE "tenant:default" FROM OFFSET {from}"#)).await;
    assert_eq!(offsets(&drain(&gw, &replay, None).await), offsets(&a)[3..].to_vec());
    let r = q(&engine, &format!(r#"UNSUBSCRIBE "{replay}""#)).await;
    assert_eq!(r["data"]["unsubscribed"], true);
    assert!(gw.next(&replay, None, 10, Duration::ZERO).await.is_err(), "an unsubscribed id is unknown");
}

#[tokio::test]
async fn subscriptions_need_a_durable_engine() {
    let engine = LocalEngine::default();
    let e = engine.query(r#"SUBSCRIBE TOPIC "t""#, None).await.unwrap_err();
    assert!(e.contains("durable engine"), "{e}");
    assert!(engine.gateway().is_none());
}

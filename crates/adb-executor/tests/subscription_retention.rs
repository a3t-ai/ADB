//! W9: log retention and subscriptions. A subscriber whose position is no longer in
//! the log gets a `reset` with the current rows of its target, then deliveries
//! (spec §4.4, reconciliation note 2); a subscription nobody reads is dropped (E-L3).
//! Own test binary: it sets `ADB_LOG_RETAIN` / `ADB_SUB_IDLE` for the whole process.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine, Writes};
use adb_gateway::{SubSpec, Target};
use serde_json::Value;

/// The tests set process-wide settings: one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

async fn subscribe(engine: &LocalEngine, aql: &str) -> String {
    q(engine, aql).await["data"]["subscription"].as_str().unwrap().to_string()
}

/// A subscription through the engine's API (scope subscriptions, a start offset and
/// acks are the server's API in AQL v0.13, not statements).
async fn subscribe_api(engine: &LocalEngine, target: Target, from: Option<u64>) -> String {
    engine.io().0.subscribe(SubSpec { target, kinds: Vec::new(), filter: None }, from).await.unwrap().0
}

/// Enough writes for the log to be trimmed past every subscriber (Lance makes them
/// durable about once a second; the log is trimmed every 256 records).
async fn flood(engine: &LocalEngine) {
    for round in 0..3 {
        for i in 0..300 {
            q(engine, &format!(r#"STORE INTO WORKING (key = "fill-{round}-{i}", i = {i})"#)).await;
        }
        tokio::time::sleep(Duration::from_millis(1300)).await;
    }
}

#[tokio::test]
async fn a_subscriber_behind_the_log_gets_a_reset_with_current_state() {
    let _serial = SERIAL.lock().await;
    std::env::set_var("ADB_LOG_RETAIN", "10");
    // Small segments: the log is purged a closed segment at a time.
    std::env::set_var("ADB_WAL_SEGMENT_BYTES", "16384");
    std::env::remove_var("ADB_SUB_IDLE");
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();

    let scope_sub = subscribe_api(&engine, Target::Scope("team:r".into()), None).await;
    let topic_sub = subscribe(&engine, r#"SUBSCRIBE TOPIC "news""#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "kept", v = 1) IN SCOPE "team:r""#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "gone", v = 1) IN SCOPE "team:r""#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "elsewhere", v = 1) IN SCOPE "team:other""#).await;
    q(&engine, r#"PUBLISH TOPIC "news" (headline = "one")"#).await;
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE key = "kept" (v = 2) IN SCOPE "team:r""#).await;
    q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "gone" IN SCOPE "team:r""#).await;
    flood(&engine).await;

    // The scope subscriber: one reset with the current state — `kept` at v 2, no
    // `gone`, nothing from another scope.
    let b = gw.next(&scope_sub, None, 10, Duration::from_millis(300)).await.unwrap();
    let reset = b.reset.expect("reset past the log");
    assert!(b.deliveries.is_empty());
    assert_eq!(b.next, reset.offset + 1);
    let ids: Vec<&str> = reset.changes.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, vec!["team:r~kept"], "{:?}", reset.changes);
    assert_eq!(reset.changes[0].value.as_ref().unwrap()["v"], 2);
    assert_eq!(reset.changes[0].op, "put");

    // After the reset, changes arrive as deliveries again.
    q(&engine, r#"STORE INTO SEMANTIC (key = "later", v = 1) IN SCOPE "team:r""#).await;
    let b = gw.next(&scope_sub, Some(b.next), 10, Duration::from_secs(2)).await.unwrap();
    assert!(b.reset.is_none());
    let later: Vec<&str> = b.deliveries.iter().flat_map(|d| d.changes.iter().map(|c| c.id.as_str())).collect();
    assert!(later.contains(&"team:r~later"), "{later:?}");

    // The topic subscriber: the retained message.
    let b = gw.next(&topic_sub, None, 10, Duration::from_millis(300)).await.unwrap();
    let reset = b.reset.expect("reset past the log");
    assert_eq!(reset.changes.len(), 1);
    assert_eq!(reset.changes[0].op, "publish");
    assert_eq!(reset.changes[0].value.as_ref().unwrap()["headline"], "one");
    assert!(reset.changes[0].offset.is_some_and(|o| o > 1));

    // A subscription from an offset past retention registers and starts with a reset.
    let old = subscribe_api(&engine, Target::Topic("news".into()), Some(1)).await;
    let b = gw.next(&old, None, 10, Duration::from_millis(300)).await.unwrap();
    assert_eq!(b.reset.expect("reset").changes.len(), 1);
}

#[tokio::test]
async fn an_idle_subscription_is_dropped_and_a_read_one_kept() {
    let _serial = SERIAL.lock().await;
    std::env::set_var("ADB_SUB_IDLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();
    let idle = subscribe(&engine, r#"SUBSCRIBE TOPIC "quiet""#).await;
    let read = subscribe(&engine, r#"SUBSCRIBE TOPIC "busy""#).await;
    let acked = subscribe(&engine, r#"SUBSCRIBE TOPIC "busy""#).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    gw.next(&read, None, 10, Duration::ZERO).await.unwrap();
    let p = q(&engine, r#"PUBLISH TOPIC "busy" (n = 1)"#).await["data"]["offset"].as_u64().unwrap();
    engine.io().0.ack(&acked, p).await.unwrap();

    assert_eq!(engine.drop_idle_subscriptions().await.unwrap(), vec![idle.clone()]);
    assert!(gw.registry().get(&idle).is_none());
    assert!(gw.registry().get(&read).is_some() && gw.registry().get(&acked).is_some());
    // Dropped for good: committed as an UNSUBSCRIBE.
    let r = q(&engine, &format!(r#"UNSUBSCRIBE "{idle}""#)).await;
    assert_eq!(r["data"]["unsubscribed"], false);
}

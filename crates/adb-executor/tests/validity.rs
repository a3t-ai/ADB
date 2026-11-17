//! AQL v0.10 (GR-14, L9): fact validity (`valid_from` / `valid_to`), history reads
//! (`AS OF`, `VALID AT`, `WITH HISTORY`) and the validity timer's `begin` / `end`.

use std::collections::HashMap;
use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use adb_gateway::{Batch, Gateway};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn records(v: &Value) -> Vec<Value> {
    v["data"]["records"].as_array().cloned().unwrap_or_default()
}

/// The facts' objects or values, in result order.
async fn objects(engine: &LocalEngine, aql: &str) -> Vec<String> {
    records(&q(engine, aql).await)
        .iter()
        .map(|r| r["data"]["object"].as_str().map(String::from).unwrap_or_else(|| r["data"]["value"].to_string()))
        .collect()
}

fn later(d: Duration) -> String {
    (chrono::Utc::now() + chrono::Duration::from_std(d).unwrap()).to_rfc3339()
}

/// A fact holds only within its range: the current view, FOLLOW LINKS and keys
/// treat one outside it as not there, `VALID AT` and `WITH HISTORY` still find it,
/// FORGET removes it.
#[tokio::test]
async fn facts_hold_within_their_validity() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "worked_at", object = "org:old", valid_from = "2010-01-01", valid_to = "2020-01-01")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme", valid_from = "2020-01-01")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "moves_to", object = "city:york", valid_from = "2999-01-01")"#).await;

    // Current: only the fact that holds now.
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#).await, ["org:acme"]);
    let r = q(&engine, r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS ALL"#).await;
    let reached: Vec<String> = records(&r).iter().filter_map(|x| x["id"].as_str().map(String::from)).collect();
    assert_eq!(reached, ["org:acme"], "{r}");
    // Stored in one form.
    let r = q(&engine, r#"RECALL FROM SEMANTIC WHERE predicate = "works_at""#).await;
    assert_eq!(records(&r)[0]["data"]["valid_from"], "2020-01-01T00:00:00Z", "{r}");

    // VALID AT: what held then, as known now.
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" VALID AT "2015-06-01""#).await, ["org:old"]);
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" VALID AT "3000-01-01T00:00:00+02:00""#).await, ["org:acme", "city:york"]);
    // WITH HISTORY: every version, whatever its validity.
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" WITH HISTORY"#).await.len(), 3);

    // The key of a lapsed fact is still taken: storing it again supersedes it.
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "worked_at", object = "org:older", valid_to = "2010-01-01")"#).await;
    let r = q(&engine, r#"RECALL FROM SEMANTIC WHERE predicate = "worked_at" WITH HISTORY"#).await;
    let versions: Vec<u64> = records(&r).iter().map(|x| x["metadata"]["version"].as_u64().unwrap()).collect();
    assert_eq!(versions, [1, 2], "{r}");
    assert!(records(&r)[0]["data"]["_replaced_at"].is_string() && records(&r)[1]["data"]["_replaced_at"].is_null(), "{r}");

    // UPDATE moves a bound (and checks it).
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE predicate = "works_at" (valid_to = "2021-01-01")"#).await;
    assert!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#).await.is_empty());
    assert!(engine.query(r#"UPDATE INTO SEMANTIC WHERE predicate = "moves_to" (valid_to = "2000-01-01")"#, None).await.is_ok(), "not current: matches nothing");

    // FORGET reaches facts outside their validity.
    let r = q(&engine, r#"FORGET FROM SEMANTIC WHERE subject = "person:ada""#).await;
    assert_eq!(r["data"]["count"].as_u64(), Some(3), "{r}");
    assert!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" WITH HISTORY"#).await.is_empty());

    // Bad bounds are refused.
    for bad in [
        r#"STORE INTO SEMANTIC FACT (subject = "a", predicate = "p", value = 1, valid_from = "soon")"#,
        r#"STORE INTO SEMANTIC FACT (subject = "a", predicate = "p", value = 1, valid_from = "2020-01-02", valid_to = "2020-01-01")"#,
        r#"STORE INTO SEMANTIC FACT (subject = "a", predicate = "p", value = 1, valid_to = 5)"#,
    ] {
        assert!(engine.query(bad, None).await.is_err(), "{bad} should be refused");
    }
}

/// `AS OF`: each record as it was then (a supersede, a later STORE); `$t` and `AGO`;
/// PROCEDURAL too. History reads refuse LIKE and links.
#[tokio::test]
async fn as_of_reads_past_versions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme")"#).await;
    q(&engine, r#"STORE INTO PROCEDURAL (pattern_id = "deploy", pattern = "deploy app", steps = ["build"])"#).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t1 = chrono::Utc::now().to_rfc3339();
    tokio::time::sleep(Duration::from_millis(20)).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:globex")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:bob", predicate = "works_at", object = "org:acme")"#).await;
    q(&engine, r#"UPDATE INTO PROCEDURAL WHERE pattern_id = "deploy" (steps = ["build", "ship"])"#).await;

    let at = |aql: &str| aql.replace("T1", &t1);
    assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE predicate = "works_at""#).await, ["org:globex", "org:acme"]);
    assert_eq!(objects(&engine, &at(r#"RECALL FROM SEMANTIC WHERE predicate = "works_at" AS OF "T1""#)).await, ["org:acme"]);
    let vars: HashMap<String, Value> = [("t".to_string(), Value::String(t1.clone()))].into();
    let r = engine.query(r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AS OF $t"#, Some(&vars)).await.unwrap();
    assert_eq!(records(&r).len(), 1, "{r}");
    assert_eq!(records(&r)[0]["data"]["object"], "org:acme");
    assert!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE predicate = "works_at" AS OF 1h AGO"#).await.is_empty());
    // Entities are records too: the stubs existed at T1 (ada, acme), bob didn't.
    let r = q(&engine, &at(r#"RECALL FROM SEMANTIC WHERE name = "person:bob" AS OF "T1""#)).await;
    assert!(records(&r).is_empty(), "{r}");

    let r = q(&engine, &at(r#"LOOKUP FROM PROCEDURAL KEY pattern_id = "deploy" AS OF "T1""#)).await;
    assert_eq!(records(&r)[0]["metadata"]["version"], 1, "{r}");
    let r = q(&engine, r#"LOOKUP FROM PROCEDURAL KEY pattern_id = "deploy" WITH HISTORY"#).await;
    assert_eq!(records(&r).len(), 2, "{r}");
    // AS OF bounds WITH HISTORY to the versions recorded by then.
    let r = q(&engine, &at(r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" WITH HISTORY AS OF "T1""#)).await;
    assert_eq!(records(&r).len(), 1, "{r}");
    assert!(records(&r)[0]["data"]["_replaced_at"].is_null(), "not replaced by T1: {r}");
    // Aggregates count versions.
    let r = q(&engine, r#"RECALL FROM SEMANTIC WHERE predicate = "works_at" WITH HISTORY AGGREGATE COUNT(*) AS n"#).await;
    assert_eq!(r["data"]["values"]["n"], 3, "{r}");

    for bad in [
        r#"RECALL FROM SEMANTIC LIKE $q AS OF 1h AGO"#,
        r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AS OF 1h AGO FOLLOW LINKS ALL"#,
        r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AS OF "yesterday""#,
        r#"RECALL FROM SEMANTIC WHERE subject = "person:ada" AS OF $unbound"#,
        r#"RECALL FROM WORKING WHERE a = 1 AS OF 1h AGO"#,
    ] {
        assert!(engine.query(bad, None).await.is_err(), "{bad} should be refused");
    }
}

async fn drain(gw: &Gateway, sub: &str) -> Batch {
    let mut all = Batch::default();
    let mut from = None;
    loop {
        let b = gw.next(sub, from, 3, Duration::from_millis(300)).await.unwrap();
        let empty = b.deliveries.is_empty();
        all.deliveries.extend(b.deliveries);
        if empty {
            return all;
        }
        from = Some(b.next);
    }
}

/// The validity timer (W6-L6): a bound passing commits `begin` / `end` for the fact,
/// once, also for a bound that passed while ADB was down; a superseded fact's old
/// bound fires nothing.
#[tokio::test]
async fn validity_bounds_are_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let ops = |b: &Batch| b.deliveries.iter().flat_map(|d| d.changes.iter().map(|c| format!("{}:{}", c.op, c.id))).collect::<Vec<_>>();
    let sub;
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        let gw = engine.gateway().unwrap();
        let r = q(&engine, r#"SUBSCRIBE SCOPE "tenant:default""#).await;
        sub = r["data"]["subscription"].as_str().unwrap().to_string();
        let (soon, bit_later) = (later(Duration::from_millis(800)), later(Duration::from_millis(1200)));
        q(&engine, &format!(r#"STORE INTO SEMANTIC FACT (subject = "s", predicate = "ends", value = 1, valid_to = "{soon}")"#)).await;
        q(&engine, &format!(r#"STORE INTO SEMANTIC FACT (subject = "s", predicate = "begins", value = 1, valid_from = "{bit_later}")"#)).await;
        q(&engine, &format!(r#"STORE INTO SEMANTIC FACT (subject = "s", predicate = "moved", value = 1, valid_to = "{soon}")"#)).await;
        q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "s", predicate = "moved", value = 2)"#).await;
        assert_eq!(objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "s""#).await, ["1", "2"]);
        let mut seen = Vec::new();
        for _ in 0..40 {
            seen = ops(&drain(&gw, &sub).await);
            if seen.iter().any(|o| o == "begin:fact:s|begins") && seen.iter().any(|o| o == "end:fact:s|ends") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(seen.iter().any(|o| o == "end:fact:s|ends") && seen.iter().any(|o| o == "begin:fact:s|begins"), "{seen:?}");
        assert!(!seen.iter().any(|o| o.ends_with("fact:s|moved") && (o.starts_with("end") || o.starts_with("begin"))), "{seen:?}");
        let mut now = objects(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "s""#).await;
        now.sort();
        assert_eq!(now, ["1", "2"], "ends gone, begins there");
        // A bound that passes while ADB is down.
        let soon = later(Duration::from_millis(600));
        q(&engine, &format!(r#"STORE INTO SEMANTIC FACT (subject = "s", predicate = "offline", value = 1, valid_to = "{soon}")"#)).await;
    }
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();
    let mut seen = Vec::new();
    for _ in 0..20 {
        seen = ops(&drain(&gw, &sub).await);
        if seen.iter().any(|o| o == "end:fact:s|offline") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(seen.iter().filter(|o| *o == "end:fact:s|offline").count(), 1, "{seen:?}");
    assert_eq!(seen.iter().filter(|o| *o == "end:fact:s|ends").count(), 1, "not repeated after a restart: {seen:?}");
}

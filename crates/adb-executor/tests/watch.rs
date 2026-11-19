//! MVP W6: continuous queries (`W6_WATCH.md`) through the engine — `SUBSCRIBE RECALL …` (was WATCH) returns a
//! snapshot, the watcher's frames keep it equal to re-running the query.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use serde_json::{json, Value};

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

/// `SUBSCRIBE RECALL …`: (cq_id, snapshot offset, result).
async fn watch(engine: &LocalEngine, aql: &str) -> (String, u64, Value) {
    let r = q(engine, aql).await;
    let d = &r["data"];
    assert_eq!(d["type"], "snapshot", "{r}");
    (d["cq_id"].as_str().unwrap().to_string(), d["offset"].as_u64().unwrap(), d["result"].clone())
}

/// Every frame after `from`, once the watcher has caught up with every write (and
/// none arrive for `quiet`).
async fn drain(engine: &LocalEngine, id: &str, mut from: u64, quiet: Duration) -> (Vec<Value>, u64) {
    let w = engine.watcher().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while w.pending() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut out = Vec::new();
    loop {
        let frames = w.next(id, Some(from), 100, quiet).await.unwrap();
        if frames.is_empty() {
            return (out, from);
        }
        for f in frames {
            from = f.offset;
            out.push(serde_json::to_value(&f).unwrap());
        }
    }
}

fn comparable(mut r: Value) -> Value {
    if let Some(m) = r.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        m.remove("accessed_at");
    }
    r
}

fn key(r: &Value) -> (String, String) {
    (r["memory_type"].to_string(), r["id"].as_str().unwrap_or_default().to_string())
}

/// A client: apply frames to a copy of the result's records.
fn apply(records: &mut Vec<Value>, frame: &Value) {
    match frame["type"].as_str().unwrap() {
        "reset" | "snapshot" => *records = frame["result"]["data"]["records"].as_array().cloned().unwrap_or_default(),
        "diff" => {
            if let Some(r) = frame.get("result") {
                *records = r["data"]["records"].as_array().cloned().unwrap_or_default();
                return;
            }
            let removed: Vec<(String, String)> = frame["removed"].as_array().unwrap().iter().map(key).collect();
            records.retain(|r| !removed.contains(&key(r)));
            for c in frame["changed"].as_array().unwrap() {
                if let Some(r) = records.iter_mut().find(|r| key(r) == key(c)) {
                    *r = c.clone();
                }
            }
            records.extend(frame["added"].as_array().unwrap().iter().cloned());
            if let Some(order) = frame["order"].as_array() {
                let pos = |r: &Value| order.iter().position(|o| o == &r["id"]).unwrap_or(usize::MAX);
                records.sort_by_key(pos);
            }
        }
        other => panic!("frame type {other}"),
    }
}

fn records_of(result: &Value) -> Vec<Value> {
    result["data"]["records"].as_array().cloned().unwrap_or_default()
}

/// Records as compared with a fresh run: without `accessed_at`, by id (unordered
/// queries come in storage order; ties of ordered ones too).
fn same_set(records: Vec<Value>) -> Vec<Value> {
    let mut v: Vec<Value> = records.into_iter().map(comparable).collect();
    v.sort_by_key(|r| key(r));
    v
}

#[tokio::test]
async fn watch_sends_diffs_as_the_result_changes() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC (key = "b1", topic = "billing", content = "invoices go out on the 1st")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "o1", topic = "ops", content = "deploys on tuesday")"#).await;
    let watched = r#"RECALL FROM SEMANTIC WHERE topic = "billing""#;
    let (id, offset, snap) = watch(&engine, &format!("SUBSCRIBE {watched}")).await;
    assert_eq!(records_of(&snap).len(), 1, "{snap}");

    q(&engine, r#"STORE INTO SEMANTIC (key = "b2", topic = "billing", content = "late fee is 2%")"#).await;
    let (frames, at) = drain(&engine, &id, offset, Duration::from_millis(400)).await;
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0]["type"], "diff");
    assert_eq!(frames[0]["added"][0]["id"], "b2");
    assert!(frames[0]["offset"].as_u64().unwrap() > offset);

    // Another topic: re-run (same memory type), no change, no frame.
    q(&engine, r#"STORE INTO SEMANTIC (key = "o2", topic = "ops", content = "on-call rota")"#).await;
    let (frames, at) = drain(&engine, &id, at, Duration::from_millis(400)).await;
    assert!(frames.is_empty(), "{frames:?}");

    q(&engine, r#"UPDATE INTO SEMANTIC WHERE key = "b2" (content = "late fee is 3%")"#).await;
    let (frames, at) = drain(&engine, &id, at, Duration::from_millis(400)).await;
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0]["changed"][0]["data"]["content"], "late fee is 3%");

    q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "b1""#).await;
    let (frames, at) = drain(&engine, &id, at, Duration::from_millis(400)).await;
    let removed: Vec<&Value> = frames.iter().flat_map(|f| f["removed"].as_array().unwrap().iter()).collect();
    assert_eq!(removed, [&json!({"memory_type": "SEMANTIC", "id": "b1"})], "{frames:?}");

    // Resume: from the snapshot, every frame since; from an unknown offset, a reset.
    let w = engine.watcher().unwrap();
    let all = w.next(&id, Some(offset), 100, Duration::ZERO).await.unwrap();
    assert_eq!(all.last().unwrap().offset, at);
    let reset = w.next(&id, Some(offset + 1), 100, Duration::ZERO).await.unwrap();
    assert_eq!((reset.len(), reset[0].kind), (1, "reset"));
    let fresh = q(&engine, watched).await;
    let mut client = records_of(&snap);
    for f in &all {
        apply(&mut client, &serde_json::to_value(f).unwrap());
    }
    assert_eq!(same_set(client), same_set(records_of(&fresh)));

    let r = q(&engine, &format!(r#"UNSUBSCRIBE "{id}""#)).await;
    assert_eq!(r["data"]["unsubscribed"], true, "{r}");
    assert!(w.next(&id, None, 1, Duration::ZERO).await.is_err());
    // Refusals: a write; FROM OFFSET and DEBOUNCE are not AQL (a live query starts
    // now; how often it re-runs is a server setting).
    assert!(engine.query(r#"SUBSCRIBE STORE INTO SEMANTIC (key = "x")"#, None).await.is_err());
    assert!(engine.query("SUBSCRIBE RECALL FROM SEMANTIC FROM OFFSET 3", None).await.is_err());
    assert!(engine.query("SUBSCRIBE RECALL FROM SEMANTIC DEBOUNCE 5s", None).await.is_err());
}

/// The gate: random writes against several watches; a client applying the frames
/// to the snapshot ends with the query re-run.
#[tokio::test]
async fn frame_stream_equals_rerunning_the_query() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let queries = [
        r#"RECALL FROM SEMANTIC WHERE topic = "t1""#,
        r#"RECALL FROM SEMANTIC IN SCOPE "team:a""#,
        r#"RECALL FROM SEMANTIC ORDER BY n DESC LIMIT 3"#,
        r#"RECALL FROM EPISODIC"#,
        r#"RECALL FROM SEMANTIC WHERE topic = "t2" WITH LINKS"#,
    ];
    let mut watches = Vec::new();
    for qy in &queries {
        let (id, off, snap) = watch(&engine, &format!("SUBSCRIBE {qy}")).await;
        watches.push((id, off, records_of(&snap)));
    }
    // A deterministic pseudo-random sequence of writes.
    let mut seed: u64 = 0x2545F4914F6CDD1D;
    let mut rnd = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut live: Vec<String> = Vec::new();
    for i in 0..60 {
        let topic = ["t1", "t2", "t3"][rnd(3) as usize];
        let scope = if rnd(2) == 0 { r#" IN SCOPE "team:a""# } else { "" };
        match rnd(6) {
            0 | 1 | 2 => {
                let k = format!("k{i}");
                q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "{k}", topic = "{topic}", n = {}, content = "fact {i}"){scope}"#, rnd(100))).await;
                live.push(k);
            }
            3 if !live.is_empty() => {
                let k = &live[rnd(live.len() as u64) as usize];
                q(&engine, &format!(r#"UPDATE INTO SEMANTIC WHERE key = "{k}" (topic = "{topic}", n = {})"#, rnd(100))).await;
            }
            4 if !live.is_empty() => {
                let k = live.remove(rnd(live.len() as u64) as usize);
                q(&engine, &format!(r#"FORGET FROM SEMANTIC WHERE key = "{k}""#)).await;
            }
            5 if live.len() >= 2 => {
                let a = &live[rnd(live.len() as u64) as usize];
                q(&engine, &format!(r#"STORE INTO EPISODIC (key = "e{i}", content = "event {i}")"#)).await;
                q(&engine, &format!(r#"LINK FROM SEMANTIC WHERE key = "{a}" INTO EPISODIC WHERE key = "e{i}" TYPE "about""#)).await;
            }
            _ => {}
        }
        if i % 7 == 0 {
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    }
    for ((id, off, mut client), qy) in watches.into_iter().zip(queries) {
        let (frames, _) = drain(&engine, &id, off, Duration::from_millis(500)).await;
        for f in &frames {
            apply(&mut client, f);
        }
        let fresh = records_of(&q(&engine, qy).await);
        if qy.contains("ORDER BY n DESC") {
            let n: Vec<i64> = client.iter().map(|r| r["data"]["n"].as_i64().unwrap()).collect();
            assert!(n.windows(2).all(|w| w[0] >= w[1]), "{qy}: client order {n:?}");
        }
        assert_eq!(same_set(client), same_set(fresh), "{qy}: {} frames", frames.len());
    }
}

/// Watches survive a restart; a client resuming then gets a `reset`.
#[tokio::test]
async fn watches_survive_restart_and_resume_with_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (id, offset) = {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        q(&engine, r#"STORE INTO SEMANTIC (key = "a", topic = "x")"#).await;
        let (id, offset, _) = watch(&engine, r#"SUBSCRIBE RECALL FROM SEMANTIC WHERE topic = "x""#).await;
        (id, offset)
    };
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let w = engine.watcher().unwrap();
    assert!(w.contains(&id));
    q(&engine, r#"STORE INTO SEMANTIC (key = "b", topic = "x")"#).await;
    let frames = w.next(&id, Some(offset), 10, Duration::from_secs(2)).await.unwrap();
    assert_eq!(frames[0].kind, "reset", "{frames:?}");
    // The reset may predate b's re-run: follow on until the result has both.
    let (rest, _) = drain(&engine, &id, frames[0].offset, Duration::from_millis(400)).await;
    let mut client = Vec::new();
    apply(&mut client, &serde_json::to_value(&frames[0]).unwrap());
    for f in &rest {
        apply(&mut client, f);
    }
    let mut ids: Vec<&str> = client.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, ["a", "b"]);
}

/// FORGET: buffered frames no longer hold the forgotten record's content.
#[tokio::test]
async fn forget_scrubs_buffered_frames() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let (id, offset, _) = watch(&engine, r#"SUBSCRIBE RECALL FROM SEMANTIC"#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "card", content = "SECRET-4111")"#).await;
    let (frames, _) = drain(&engine, &id, offset, Duration::from_millis(400)).await;
    assert!(serde_json::to_string(&frames).unwrap().contains("SECRET-4111"));
    q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "card""#).await;
    drain(&engine, &id, offset, Duration::from_millis(400)).await;
    let w = engine.watcher().unwrap();
    let all = w.next(&id, Some(offset), 100, Duration::ZERO).await.unwrap();
    let text = serde_json::to_string(&all).unwrap();
    assert!(!text.contains("SECRET-4111"), "{text}");
    assert!(text.contains(r#""removed":[{"id":"card","memory_type":"SEMANTIC"}]"#) || text.contains(r#""removed":[{"memory_type":"SEMANTIC","id":"card"}]"#), "{text}");
}

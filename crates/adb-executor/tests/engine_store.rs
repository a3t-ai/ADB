//! FlowR's engine store on ADB (stage H-2): FlowR's conformance suite against
//! an ADB engine space; a restart (with and without a snapshot) gives the same
//! state; an evicted journal reads back from its EPISODIC history; run history
//! is invisible to agents' AQL; retention removes it.

use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use flowr_storage::engine::*;
use serde_json::{json, Value};

const LEASE: Duration = Duration::from_secs(30);

fn new_run(ns: &str) -> NewRun {
    NewRun {
        run_id: uuid::Uuid::now_v7(),
        namespace: ns.into(),
        flow: "f".into(),
        version: "1".into(),
        def_hash: "h".into(),
        parent_run_id: None,
        durability: Durability::Durable,
        debug: false,
        breakpoints: None,
        principal: json!({ "id": "u" }),
        trigger: "api".into(),
        input: json!({ "x": 1 }),
        deadline_at: None,
        dedupe_key: None,
    }
}

fn step(key: &str, n: u64) -> Record {
    Record::StepCompleted {
        step_key: key.into(),
        attempt: 0,
        output: json!({ "n": n }),
        context: Value::Null,
        class: StepClass::Pure,
        duration_us: 1000,
        directive: String::new(),
        input: Value::Null,
        at: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_on_adb_spaces() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let n = std::sync::atomic::AtomicUsize::new(0);
    // Each check gets a fresh space: spaces share nothing.
    flowr_storage::conformance::run_all(|| {
        let s = engine.engine_store(&format!("c{}", n.fetch_add(1, std::sync::atomic::Ordering::SeqCst))).unwrap();
        // A space's first use creates its journal partition (Lance): done here,
        // so the suite's timing checks see an operation's own latency.
        async move {
            s.pending(DEFAULT_POOL).await.unwrap();
            s
        }
    })
    .await;
}

/// Runs, steps, a wait, an approval and an event on space `s`; what a restart
/// must keep.
async fn work(engine: &LocalEngine) -> (RunId, RunId) {
    let s = engine.engine_store("s").unwrap();
    let a = new_run("p");
    let a_id = a.run_id;
    let t = NewTask::start(&a);
    s.start(a, t).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (lease, _, epoch) = s.claim_runs(DEFAULT_POOL, "w", 1, LEASE).await.unwrap().remove(0);
    s.commit_step(a_id, epoch, StepCommit { records: vec![step("/a", 1), step("/b", 2)], checkpoint: None, outbox: vec![] }).await.unwrap();
    s.open_wait(a_id, epoch, "/w", json!({ "mode": "any", "filters": [{ "type": "go" }] })).await.unwrap();
    s.release(a_id, epoch, RunPhase::Waiting, RunOutcome::default(), &lease).await.unwrap();
    s.deliver_event("p", json!({ "id": "e1", "type": "go" })).await.unwrap();

    let b = new_run("p");
    let b_id = b.run_id;
    let t = NewTask::start(&b);
    s.start(b, t).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let claimed = s.claim_runs(DEFAULT_POOL, "w", 5, LEASE).await.unwrap();
    let (lease, _, epoch) = claimed.into_iter().find(|(_, t, _)| t.run_id == b_id).unwrap();
    s.append(b_id, epoch, &[step("/x", 9)]).await.unwrap();
    s.open_approval(b_id, epoch, "r1", json!({ "tool": "deploy" })).await.unwrap();
    s.release(b_id, epoch, RunPhase::Completed, RunOutcome { output: Some(json!({ "ok": true })), error: None }, &lease).await.unwrap();
    (a_id, b_id)
}

async fn observe(engine: &LocalEngine, runs: &[RunId]) -> Value {
    let s = engine.engine_store("s").unwrap();
    let mut out = Vec::new();
    for r in runs {
        let mut row = serde_json::to_value(s.get(*r).await.unwrap()).unwrap();
        row.as_object_mut().unwrap().remove("updated_at");
        out.push(json!({ "run": row, "journal": s.read_from(*r, 0).await.unwrap() }));
    }
    out.push(json!({ "pending": s.pending(DEFAULT_POOL).await.unwrap(), "approvals": s.approvals().await.unwrap() }));
    Value::Array(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_every_space_with_and_without_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (runs, before) = {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        let (a, b) = work(&engine).await;
        (vec![a, b], observe(&engine, &[a, b]).await)
    };
    // From the log alone.
    let after = {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        let after = observe(&engine, &runs).await;
        // Snapshot, then more work on top of it.
        engine.engine_maintain(true).await.unwrap();
        let s = engine.engine_store("s").unwrap();
        s.deliver_event("p", json!({ "id": "e2", "type": "none" })).await.unwrap();
        after
    };
    assert_eq!(after, before, "replayed from the log");
    // From the snapshot and the log above it.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(observe(&engine, &runs).await, before, "snapshot + log");
    let s = engine.engine_store("s").unwrap();
    assert!(s.deliver_event("p", json!({ "id": "e2", "type": "none" })).await.unwrap().is_empty(), "event ids survive too");
    // The dead worker's runs are owned for one more lease period, then claimable.
    assert!(s.orphaned().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_journal_reads_back_from_episodic() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let (_, b) = work(&engine).await;
    let s = engine.engine_store("s").unwrap();
    let in_memory = s.read_from(b, 0).await.unwrap();
    assert_eq!(in_memory.len(), 2, "a step and the approval");
    s.evict_journal(b).await.unwrap();
    assert_eq!(s.read_from(b, 0).await.unwrap(), in_memory, "the same records, from EPISODIC");
    assert_eq!(s.read_from(b, 1).await.unwrap(), in_memory[1..].to_vec());
    // Still after a restart.
    drop(s);
    drop(engine);
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(engine.engine_store("s").unwrap().read_from(b, 0).await.unwrap(), in_memory);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_history_is_not_agent_memory_and_retention_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    engine.query(r#"STORE INTO EPISODIC (note = "an agent's own memory")"#, None).await.unwrap();
    let (_, b) = work(&engine).await;
    let recall = engine.query("RECALL FROM EPISODIC LIMIT 100", None).await.unwrap();
    let text = recall.to_string();
    assert!(text.contains("an agent's own memory"));
    assert!(!text.contains("/x") && !text.contains(&b.to_string()), "no run history in agent memory: {text}");

    let s = engine.engine_store("s").unwrap();
    s.evict_journal(b).await.unwrap();
    assert_eq!(s.read_from(b, 0).await.unwrap().len(), 2);
    let gone = s.purge_finished(chrono::Utc::now() + chrono::Duration::seconds(1)).await.unwrap();
    assert_eq!(gone, 1, "the finished run");
    assert!(s.get(b).await.unwrap().is_none());
    assert!(s.read_from(b, 0).await.unwrap().is_empty(), "its episodes are gone");
}

#[tokio::test(flavor = "multi_thread")]
async fn operation_latency() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let s = engine.engine_store("lat").unwrap();
    let t = std::time::Instant::now();
    s.pending(DEFAULT_POOL).await.unwrap();
    let first = t.elapsed();
    let mut starts = Vec::new();
    for _ in 0..20 {
        let r = new_run("p");
        let task = NewTask::start(&r);
        let t = std::time::Instant::now();
        s.start(r, task).await.unwrap();
        starts.push(t.elapsed());
    }
    starts.sort();
    eprintln!("LATENCY first use {first:?}; start p50 {:?} max {:?}", starts[10], starts[19]);
}

/// ADB trims its log every 256 records below what its consumers hold durably;
/// the engine's floor keeps every record above its snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn the_log_is_trimmed_only_below_the_engine_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let count = |e: &LocalEngine| {
        let s = e.engine_store("t").unwrap();
        async move { s.pending("q").await.unwrap() }
    };
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        let s = engine.engine_store("t").unwrap();
        for i in 0..700 {
            s.enqueue(NewTask { pool: "q".into(), dedupe_key: Some(format!("k{i}")), ..NewTask::resume(uuid::Uuid::now_v7(), "p", None, chrono::Utc::now()) }).await.unwrap();
            // Memory writes too, so the main table's floor moves on.
            if i % 50 == 0 {
                engine.query(&format!(r#"STORE INTO EPISODIC (n = {i})"#), None).await.unwrap();
            }
        }
        assert_eq!(count(&engine).await, 700);
    }
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        assert_eq!(count(&engine).await, 700, "no snapshot yet: every record kept and replayed");
        engine.engine_maintain(true).await.unwrap();
        let s = engine.engine_store("t").unwrap();
        for i in 700..1000 {
            s.enqueue(NewTask { pool: "q".into(), dedupe_key: Some(format!("k{i}")), ..NewTask::resume(uuid::Uuid::now_v7(), "p", None, chrono::Utc::now()) }).await.unwrap();
        }
    }
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(count(&engine).await, 1000, "snapshot + the records above it");
}

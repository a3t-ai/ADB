//! PF-13: writes prepared outside the sequencer (PF-1) and compaction without it
//! (PF-7) under concurrency. 16 writers STORE, UPDATE and FORGET their own keys and
//! all UPDATE four shared keys, each setting its own field there (a lost update drops
//! a field), while compaction runs in a loop. Every acknowledged write must be
//! visible exactly once, forgotten keys gone, the same after a reopen.

use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};

const WRITERS: usize = 16;
const OWN: usize = 12;
const SHARED: usize = 4;
const ROUNDS: usize = 6;

async fn q(engine: &LocalEngine, aql: &str) -> serde_json::Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

async fn lookup(engine: &LocalEngine, key: &str) -> Vec<serde_json::Value> {
    let r = q(engine, &format!(r#"RECALL FROM SEMANTIC WHERE id = "{key}""#)).await;
    r["data"]["records"].as_array().cloned().unwrap_or_default()
}

/// What every writer was told committed: own keys' last value, forgotten keys, and its
/// last value on each shared key.
async fn check(engine: &LocalEngine, last: &[Vec<Option<usize>>], forgotten: &[Vec<bool>], shared: &[Vec<usize>]) {
    for w in 0..WRITERS {
        for k in 0..OWN {
            let rows = lookup(engine, &format!("w{w}k{k}")).await;
            if forgotten[w][k] {
                assert!(rows.is_empty(), "w{w}k{k} was forgotten: {rows:?}");
            } else if let Some(v) = last[w][k] {
                assert_eq!(rows.len(), 1, "w{w}k{k}: {rows:?}");
                assert_eq!(rows[0]["data"]["v"], serde_json::json!(v), "w{w}k{k}: {rows:?}");
            }
        }
    }
    for s in 0..SHARED {
        let rows = lookup(engine, &format!("shared{s}")).await;
        assert_eq!(rows.len(), 1, "shared{s}: {rows:?}");
        for w in 0..WRITERS {
            assert_eq!(rows[0]["data"][format!("w{w}")], serde_json::json!(shared[w][s]), "shared{s} lost writer {w}'s update: {rows:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_prepared_writes_and_online_compaction_lose_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let engine = Arc::new(engine);
    for s in 0..SHARED {
        q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "shared{s}", topic = "shared")"#)).await;
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let compactor = {
        let (engine, stop) = (Arc::clone(&engine), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut n = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                engine.compact().await.unwrap();
                n += 1;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            n
        })
    };
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move {
                let mut last = vec![None; OWN];
                let mut forgotten = vec![false; OWN];
                let mut shared = vec![0usize; SHARED];
                for round in 1..=ROUNDS {
                    for k in 0..OWN {
                        let key = format!("w{w}k{k}");
                        if round == 1 {
                            q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "{key}", topic = "t{k}", v = 1)"#)).await;
                            last[k] = Some(1);
                        } else if forgotten[k] {
                            continue;
                        } else if round == ROUNDS && k % 3 == 0 {
                            q(&engine, &format!(r#"FORGET FROM SEMANTIC WHERE key = "{key}""#)).await;
                            forgotten[k] = true;
                        } else {
                            q(&engine, &format!(r#"UPDATE INTO SEMANTIC WHERE key = "{key}" (v = {round})"#)).await;
                            last[k] = Some(round);
                        }
                    }
                    for (s, v) in shared.iter_mut().enumerate() {
                        q(&engine, &format!(r#"UPDATE INTO SEMANTIC WHERE key = "shared{s}" (w{w} = {round})"#)).await;
                        *v = round;
                    }
                }
                (last, forgotten, shared)
            })
        })
        .collect();
    let mut last = Vec::new();
    let mut forgotten = Vec::new();
    let mut shared = Vec::new();
    for h in writers {
        let (l, f, s) = h.await.unwrap();
        last.push(l);
        forgotten.push(f);
        shared.push(s);
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let compactions = compactor.await.unwrap();
    assert!(compactions >= 2, "compaction ran while writing ({compactions} times)");
    check(&engine, &last, &forgotten, &shared).await;
    // And after a reopen: replay from the log ends in the same state.
    drop(engine);
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    check(&engine, &last, &forgotten, &shared).await;
}

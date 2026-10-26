//! Stage C: a durable engine rebuilds exactly the same state from its log.

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

/// Writes covering every memory type, UPDATE, FORGET and LINK.
const WRITES: &[&str] = &[
    r#"STORE INTO WORKING (task = "t1", step = 1) TTL 1h"#,
    r#"STORE INTO WORKING (task = "t2", step = 2)"#,
    r#"STORE INTO SEMANTIC (key = "acme", concept = "acme", content = "Acme renews on 1 Nov", active = true)"#,
    r#"STORE INTO EPISODIC (bid_id = "e1", campaign = "summer", won = false, cpm_paid = 2.5)"#,
    r#"STORE INTO EPISODIC (bid_id = "e2", campaign = "summer", won = false, cpm_paid = 1.0)"#,
    r#"STORE INTO PROCEDURAL (pattern_id = "oom-fix", pattern = "oom", steps = "restart the pod", confidence = 0.6)"#,
    r#"STORE INTO TOOLS (tool_id = "search", name = "search", description = "web search", ranking = 0.7)"#,
    r#"UPDATE INTO EPISODIC WHERE bid_id = "e1" (won = true, brand_lift = 0.04)"#,
    r#"UPDATE INTO WORKING WHERE task = "t1" (step = 5)"#,
    r#"UPDATE INTO PROCEDURAL WHERE pattern = "oom" (confidence = 0.9)"#,
    r#"FORGET FROM WORKING WHERE task = "t2""#,
    r#"FORGET FROM EPISODIC WHERE bid_id = "e2""#,
    r#"LINK FROM SEMANTIC WHERE concept = "acme" TO EPISODIC WHERE bid_id = "e1" TYPE "evidence" WEIGHT 0.8"#,
    r#"LINK FROM PROCEDURAL WHERE pattern = "oom" TO EPISODIC WHERE bid_id = "e1" TYPE "applied_to" WEIGHT 0.95"#,
];

/// Reads whose results must be identical before and after a restart.
const READS: &[&str] = &[
    r#"SCAN FROM WORKING WINDOW LAST 10"#,
    r#"RECALL FROM SEMANTIC WHERE active = true RETURN concept, content"#,
    r#"RECALL FROM EPISODIC WHERE campaign = "summer""#,
    r#"LOOKUP FROM PROCEDURAL WHERE pattern = "oom" WITH LINKS ALL"#,
    r#"LOAD FROM TOOLS WHERE ranking > 0.5 ORDER BY ranking DESC"#,
    r#"RECALL FROM SEMANTIC WHERE concept = "acme" FOLLOW LINKS TYPE "evidence""#,
];

/// A query result without timing fields.
fn stable(mut v: Value) -> Value {
    if let Some(m) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        m.remove("execution_time_ms");
    }
    v
}

async fn snapshot(engine: &LocalEngine) -> Vec<Value> {
    let mut out = Vec::new();
    for q in READS {
        out.push(stable(engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"))));
    }
    out.push(engine.stats().await.unwrap());
    out
}

#[tokio::test]
async fn restart_rebuilds_identical_state() {
    let dir = tempfile::tempdir().unwrap();

    let before = {
        let (engine, stats) = LocalEngine::durable(dir.path()).await.unwrap();
        assert_eq!(stats.records, 0);
        for q in WRITES {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        snapshot(&engine).await
    };

    // Sanity: the writes did what they say.
    let stats = &before[READS.len()];
    assert_eq!(stats["working"], 1, "{stats}");
    assert_eq!(stats["episodic"], 1, "{stats}");
    let working = before[0].to_string();
    assert!(working.contains(r#""step":5"#), "{working}");

    let (engine, replayed) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(replayed.records, WRITES.len() as u64, "one log record per write");
    let after = snapshot(&engine).await;
    for (i, (b, a)) in before.iter().zip(&after).enumerate() {
        assert_eq!(b, a, "read {i} ({}) differs after restart", READS.get(i).unwrap_or(&"stats"));
    }

    // New writes continue after the replayed ones, and survive another restart.
    engine.query(r#"STORE INTO WORKING (task = "t3", step = 1)"#, None).await.unwrap();
    drop(engine);
    let (engine, replayed) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(replayed.records, WRITES.len() as u64 + 1);
    assert_eq!(engine.stats().await.unwrap()["working"], 2);
}

#[tokio::test]
async fn ttl_measured_from_commit_time_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        engine.query(r#"STORE INTO WORKING (ttl_key = "short") TTL 300ms"#, None).await.unwrap();
        engine.query(r#"STORE INTO WORKING (ttl_key = "long") TTL 1h"#, None).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    // Replay must not restart the short TTL: it expired while ADB was down.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = engine.query(r#"SCAN FROM WORKING WINDOW LAST 10"#, None).await.unwrap().to_string();
    assert!(r.contains("long") && !r.contains("short"), "{r}");
}

#[tokio::test]
async fn reads_and_failed_writes_are_not_logged() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        engine.query(r#"RECALL FROM EPISODIC WHERE campaign = "none""#, None).await.unwrap();
        // An UPDATE / FORGET that matches nothing writes no log record.
        engine.query(r#"UPDATE INTO EPISODIC WHERE bid_id = "ghost" (won = false)"#, None).await.unwrap();
        engine.query(r#"FORGET FROM WORKING WHERE task = "ghost""#, None).await.unwrap();
        assert!(engine.query("STORE INTO", None).await.is_err());
    }
    let (_, replayed) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(replayed.records, 0);
}

/// Stage D2: the partition's Lance table, brought up to the log's end, as engine
/// startup does it (open the store, catch up from the log).
async fn lance_view(dir: &std::path::Path) -> (u64, Vec<adb_store::StoredRow>) {
    use adb_core::log::{CommitTs, PartitionId};
    let store = adb_store::PartitionStore::open(&adb_store::StoreConfig::new(dir.join("store")), PartitionId(0)).await.unwrap();
    let wal = adb_log::LocalWal::open(PartitionId(0), adb_log::WalConfig::new(dir.join("p0"))).unwrap();
    adb_executor::writes::catch_up(&store, &wal).await.unwrap();
    let applied = store.applied_offset().await.unwrap().0;
    let now = CommitTs::from_datetime(chrono::Utc::now());
    let rows = store.current(None, now).await.unwrap();
    store.close().await.unwrap();
    (applied, rows)
}

fn summary(rows: &[adb_store::StoredRow]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = rows
        .iter()
        .map(|r| (format!("{:?}/{:?}", r.data.memory, r.data.kind), r.data.value.get("bid_id").or(r.data.value.get("task")).or(r.data.value.get("concept")).or(r.data.value.get("pattern")).or(r.data.value.get("tool_id")).or(r.data.link_type.as_ref().map(|_| &r.data.value)).map(|v| v.to_string()).unwrap_or_default()))
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn lance_table_mirrors_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in WRITES {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
    }
    let (applied, rows) = lance_view(dir.path()).await;
    assert_eq!(applied, WRITES.len() as u64, "every log record applied");
    let s = summary(&rows);
    // working t1 (t2 forgotten), semantic acme, episodic e1 (e2 forgotten),
    // procedural oom-fix, tools search, two edges.
    assert_eq!(rows.len(), 7, "{s:?}");
    let e1 = rows.iter().find(|r| r.data.value.get("bid_id").and_then(|v| v.as_str()) == Some("e1")).unwrap();
    assert_eq!(e1.data.value["won"], true, "UPDATE applied in Lance");
    assert_eq!(e1.data.value["brand_lift"], 0.04);
    let t1 = rows.iter().find(|r| r.data.value.get("task").and_then(|v| v.as_str()) == Some("t1")).unwrap();
    assert_eq!(t1.data.value["step"], 5);
    assert!(t1.data.ttl_at.is_some(), "TTL carried into Lance");
    assert_eq!(rows.iter().filter(|r| r.data.link_type.is_some()).count(), 2, "{s:?}");
}

#[tokio::test]
async fn lance_catches_up_from_the_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in WRITES {
            engine.query(q, None).await.unwrap();
        }
    }
    let (_, before) = lance_view(dir.path()).await;
    // Lose the Lance table entirely: the log rebuilds it on the next open.
    std::fs::remove_dir_all(dir.path().join("store")).unwrap();
    {
        let (_engine, stats) = LocalEngine::durable(dir.path()).await.unwrap();
        assert_eq!(stats.store_caught_up, WRITES.len() as u64);
    }
    let (applied, after) = lance_view(dir.path()).await;
    assert_eq!(applied, WRITES.len() as u64);
    assert_eq!(summary(&before), summary(&after));
}

/// Stage D3: SEMANTIC / EPISODIC reads from Lance answer exactly like the backends.
#[tokio::test]
async fn lance_reads_match_backend_reads() {
    const EXTRA: &[&str] = &[
        r#"STORE INTO SEMANTIC (concept = "beta", content = "Beta churned", active = false, avg_cpm = 1.5)"#,
        r#"STORE INTO SEMANTIC (concept = "gamma", content = "Gamma upsell", active = true, avg_cpm = 3.2)"#,
        r#"STORE INTO EPISODIC (bid_id = "e3", campaign = "winter", won = true, cpm_paid = 4.0)"#,
        r#"STORE INTO EPISODIC (bid_id = "e4", campaign = "summer", won = false, cpm_paid = 0.5)"#,
        r#"UPDATE INTO SEMANTIC WHERE concept = "gamma" (avg_cpm = 3.6)"#,
        r#"STORE INTO PROCEDURAL (pattern_id = "disk-fix", pattern = "disk full node pressure", steps = "prune build dirs", severity = "high", confidence = 0.7)"#,
        r#"STORE INTO PROCEDURAL (pattern_id = "oom-2", pattern = "OOMKilled container memory", steps = "raise the limit", confidence = 0.4)"#,
        r#"UPDATE INTO PROCEDURAL WHERE pattern_id = "disk-fix" (confidence = 0.8)"#,
    ];
    const QUERIES: &[&str] = &[
        r#"RECALL FROM SEMANTIC WHERE active = true"#,
        r#"RECALL FROM SEMANTIC WHERE avg_cpm > 2 RETURN concept, avg_cpm"#,
        r#"LOOKUP FROM SEMANTIC KEY concept = "gamma""#,
        r#"LOOKUP FROM SEMANTIC KEY id = "acme""#,
        r#"RECALL FROM SEMANTIC ALL ORDER BY concept ASC"#,
        r#"RECALL FROM EPISODIC WHERE campaign = "summer""#,
        r#"RECALL FROM EPISODIC WHERE cpm_paid > 1 ORDER BY cpm_paid DESC LIMIT 2"#,
        r#"RECALL FROM EPISODIC ALL"#,
        r#"RECALL FROM EPISODIC WHERE won = true RETURN bid_id, cpm_paid"#,
        r#"RECALL FROM EPISODIC WHERE campaign = "summer" AGGREGATE COUNT(*) AS n, AVG(cpm_paid) AS avg RETURN n, avg"#,
        r#"RECALL FROM SEMANTIC WHERE concept = "acme" FOLLOW LINKS TYPE "evidence""#,
        r#"LOOKUP FROM SEMANTIC KEY concept = "acme" WITH LINKS ALL"#,
        r#"RECALL FROM ALL WHERE campaign = "summer""#,
        r#"LOOKUP FROM PROCEDURAL ALL ORDER BY confidence DESC"#,
        r#"LOOKUP FROM PROCEDURAL WHERE severity = "medium""#,
        r#"LOOKUP FROM PROCEDURAL WHERE confidence > 0.5 RETURN pattern_id, confidence, version"#,
        r#"LOOKUP FROM PROCEDURAL KEY id = "disk-fix""#,
        r#"LOOKUP FROM PROCEDURAL WHERE pattern = "oom" FOLLOW LINKS TYPE "applied_to""#,
    ];
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in WRITES.iter().chain(EXTRA) {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
    }
    // Oracle: the same log replayed into the in-memory backends, no table (D4.4a:
    // a durable engine keeps SEMANTIC / EPISODIC / PROCEDURAL only in Lance).
    let run = |lance: bool| {
        let path = dir.path().to_path_buf();
        async move {
            let engine = match lance {
                true => LocalEngine::durable(&path).await.unwrap().0,
                false => LocalEngine::replay_only(&path).await.unwrap().0,
            };
            let mut out = Vec::new();
            for q in QUERIES {
                out.push(stable(engine.query(q, None).await.unwrap_or_else(|e| panic!("lance={lance} {q}: {e}"))));
            }
            out.push(engine.stats().await.unwrap());
            out
        }
    };
    let backends = run(false).await;
    let lance = run(true).await;
    // SEMANTIC keeps records in a HashMap, so without ORDER BY the order is
    // unspecified (it differs between any two backends): compare those as sets.
    let as_set = |mut v: Value, q: &str| {
        if !q.contains("ORDER BY") {
            if let Some(records) = v.pointer_mut("/data/records").and_then(|r| r.as_array_mut()) {
                records.sort_by_key(|r| r["id"].as_str().unwrap_or_default().to_string());
            }
        }
        v
    };
    for (i, q) in QUERIES.iter().chain(&["stats"]).enumerate() {
        assert_eq!(as_set(lance[i].clone(), q), as_set(backends[i].clone(), q), "Lance and backend reads differ for: {q}");
    }
    // The queries returned something (not two matching empty answers).
    assert!(backends[0].to_string().contains("acme"), "{}", backends[0]);
}

/// Stage D3.5: the log is trimmed below what Lance holds durably, and a restart
/// still has everything (rebuilt from Lance plus the log's tail).
#[tokio::test]
async fn log_is_purged_below_lances_durable_offset() {
    use std::sync::Arc;
    use adb_core::log::{Epoch, LogOffset, PartitionId};
    use adb_executor::{DurableWrites, Writes};
    use adb_log::{Log, LocalWal, WalConfig};
    use adb_sequencer::Sequencer;
    use adb_store::{PartitionStore, StoreConfig};

    let dir = tempfile::tempdir().unwrap();
    let wal_cfg = WalConfig { dir: dir.path().join("p0"), segment_bytes: 4096 };
    let store_cfg = StoreConfig {
        root: dir.path().join("store"),
        embedding_dim: 4,
        memtable_rows: 100,
        wal_flush: std::time::Duration::from_millis(20),
    };
    let open = || async {
        let adb = Arc::new(adb_backends::Adb::new());
        let seq = Sequencer::new(LocalWal::open(PartitionId(0), wal_cfg.clone()).unwrap(), Epoch(1));
        let store = Arc::new(PartitionStore::open(&store_cfg, PartitionId(0)).await.unwrap());
        let (w, stats) = DurableWrites::open(Arc::clone(&adb), seq, Some(store)).await.unwrap();
        (adb, w, stats)
    };
    let n = 700u64;
    let segments_at_most;
    {
        let (_adb, w, _) = open().await;
        for i in 1..=n {
            Writes::store(&w, adb_core::MemoryType::Episodic, &format!("e{i}"), serde_json::json!({"bid_id": format!("e{i}"), "i": i}), adb_core::Scope::Private, None, None, None)
                .await
                .unwrap();
            if i % 50 == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(60)).await; // let the MemWAL flush
            }
        }
        segments_at_most = std::fs::read_dir(dir.path().join("p0")).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".wal")).count();
    }
    // Purged: the first records are gone from the log, and only a few segments are left.
    let wal = LocalWal::open(PartitionId(0), wal_cfg.clone()).unwrap();
    assert!(matches!(wal.read(LogOffset(1), 1).await, Err(adb_log::LogError::Purged { .. })), "log still holds offset 1");
    assert_eq!(wal.last_offset(), LogOffset(n));
    let total_bytes: u64 = std::fs::read_dir(dir.path().join("p0")).unwrap().map(|e| e.unwrap().metadata().unwrap().len()).sum();
    drop(wal);
    assert!(segments_at_most < 40, "{segments_at_most} segments left ({total_bytes} bytes)");

    // Restart: every record is there (in Lance, its only copy), and new writes
    // continue the log.
    let (_adb, w, stats) = open().await;
    assert_eq!(stats.records, n);
    let now = adb_core::log::CommitTs::from_datetime(chrono::Utc::now());
    assert_eq!(w.store().unwrap().count_current(adb_core::log::Memory::Episodic, now).await.unwrap(), n);
    Writes::store(&w, adb_core::MemoryType::Episodic, "after", serde_json::json!({"bid_id": "after"}), adb_core::Scope::Private, None, None, None)
        .await
        .unwrap();
    assert_eq!(w.durable_offset().await, LogOffset(n + 1));
}

/// Stage D3.7: Lance is written by a background task, and a read served from Lance
/// still sees every write that returned before it.
#[tokio::test]
async fn reads_see_their_own_writes_with_the_background_applier() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for i in 0..200 {
        engine
            .query(&format!(r#"STORE INTO SEMANTIC (concept = "c{i}", n = {i})"#), None)
            .await
            .unwrap();
        let r = engine.query(&format!(r#"RECALL FROM SEMANTIC WHERE concept = "c{i}""#), None).await.unwrap();
        assert_eq!(r["data"]["count"], 1, "write {i} not visible to the next read: {r}");
        if i % 20 == 0 {
            engine.query(&format!(r#"UPDATE INTO SEMANTIC WHERE concept = "c{i}" (n = -1)"#), None).await.unwrap();
            let r = engine.query(&format!(r#"RECALL FROM SEMANTIC WHERE concept = "c{i}" RETURN n"#), None).await.unwrap();
            assert_eq!(r["data"]["records"][0]["data"]["n"], -1, "update {i} not visible: {r}");
        }
    }
}

/// The same key in two memory types is two records, in memory and in Lance. Before
/// schema v3 they were one Lance row: the restart lost WORKING's.
#[tokio::test]
async fn same_key_in_two_memory_types_is_two_records() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        engine.query(r#"STORE INTO WORKING (key = "x", note = "working")"#, None).await.unwrap();
        engine.query(r#"STORE INTO SEMANTIC (key = "x", concept = "x", note = "semantic")"#, None).await.unwrap();
        engine.query(r#"STORE INTO EPISODIC (key = "x", note = "episodic")"#, None).await.unwrap();
    }
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for (m, note) in [("WORKING", "working"), ("SEMANTIC", "semantic"), ("EPISODIC", "episodic")] {
        let r = engine.query(&format!("RECALL FROM {m} ALL"), None).await.unwrap().to_string();
        assert!(r.contains(&format!(r#""note":"{note}""#)), "{m} lost its record after a restart: {r}");
    }
}

/// Links are read from Lance's edge rows (D4.1): forgetting either end removes the
/// link at once (D2 §5), live and after a restart, and links of the same key in
/// another memory type are untouched.
#[tokio::test]
async fn forgetting_a_record_removes_its_links() {
    let dir = tempfile::tempdir().unwrap();
    let links = |r: Value| r.to_string().matches(r#""link_type""#).count();
    let with_links = r#"RECALL FROM SEMANTIC WHERE concept = "acme" WITH LINKS ALL"#;
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in [
            r#"STORE INTO SEMANTIC (key = "acme", concept = "acme")"#,
            r#"STORE INTO EPISODIC (key = "e1", bid_id = "e1")"#,
            r#"STORE INTO EPISODIC (key = "e2", bid_id = "e2")"#,
            r#"STORE INTO WORKING (key = "e1", note = "same key, other type")"#,
            r#"LINK FROM SEMANTIC WHERE concept = "acme" TO EPISODIC WHERE bid_id = "e1" TYPE "evidence" WEIGHT 0.8"#,
            r#"LINK FROM SEMANTIC WHERE concept = "acme" TO EPISODIC WHERE bid_id = "e2" TYPE "evidence" WEIGHT 0.5"#,
        ] {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        assert_eq!(links(engine.query(with_links, None).await.unwrap()), 2);
        engine.query(r#"FORGET FROM WORKING WHERE note = "same key, other type""#, None).await.unwrap();
        assert_eq!(links(engine.query(with_links, None).await.unwrap()), 2, "forgetting WORKING e1 leaves EPISODIC e1's link");
        engine.query(r#"FORGET FROM EPISODIC WHERE bid_id = "e1""#, None).await.unwrap();
        let r = engine.query(with_links, None).await.unwrap();
        assert_eq!(links(r.clone()), 1, "the forgotten episode's link is gone: {r}");
        let follow = engine.query(r#"RECALL FROM SEMANTIC WHERE concept = "acme" FOLLOW LINKS TYPE "evidence""#, None).await.unwrap();
        assert!(follow.to_string().contains(r#""bid_id":"e2""#) && !follow.to_string().contains(r#""bid_id":"e1""#), "{follow}");
    }
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(links(engine.query(with_links, None).await.unwrap()), 1, "same after a restart");
}

/// Every version of the store's rows (history included), engine closed; caught up
/// from the log first, as the background applier may not have finished.
async fn lance_history(dir: &std::path::Path) -> Vec<adb_store::StoredRow> {
    use adb_core::log::PartitionId;
    let store = adb_store::PartitionStore::open(&adb_store::StoreConfig::new(dir.join("store")), PartitionId(0)).await.unwrap();
    let wal = adb_log::LocalWal::open(PartitionId(0), adb_log::WalConfig::new(dir.join("p0"))).unwrap();
    adb_executor::writes::catch_up(&store, &wal).await.unwrap();
    drop(wal);
    let rows = store.scan(None).await.unwrap();
    store.close().await.unwrap();
    rows
}

/// D4.3: UPDATE on SEMANTIC / PROCEDURAL supersedes (D2 §2). History stays in
/// Lance, reads see the current version only, links made to the first version still
/// reach the record, and FORGET removes every version.
#[tokio::test]
async fn update_supersedes_facts_and_procedures() {
    let dir = tempfile::tempdir().unwrap();
    let fact = r#"LOOKUP FROM SEMANTIC KEY id = "acme""#;
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for q in [
            r#"STORE INTO SEMANTIC (key = "acme", concept = "acme", content = "Acme renews on 1 Nov", ttl_note = "none")"#,
            r#"STORE INTO EPISODIC (key = "e1", bid_id = "e1")"#,
            r#"LINK FROM SEMANTIC WHERE concept = "acme" TO EPISODIC WHERE bid_id = "e1" TYPE "evidence" WEIGHT 0.8"#,
            r#"LINK FROM EPISODIC WHERE bid_id = "e1" TO SEMANTIC WHERE concept = "acme" TYPE "about" WEIGHT 0.5"#,
            r#"UPDATE INTO SEMANTIC WHERE concept = "acme" (content = "Acme renews on 15 Nov")"#,
            r#"UPDATE INTO SEMANTIC WHERE concept = "acme" (content = "Acme renews on 20 Nov")"#,
            r#"STORE INTO PROCEDURAL (pattern_id = "p1", pattern = "disk full", steps = "prune", confidence = 0.5)"#,
            r#"UPDATE INTO PROCEDURAL WHERE pattern_id = "p1" (confidence = 0.9)"#,
        ] {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        let r = engine.query(fact, None).await.unwrap().to_string();
        assert!(r.contains("20 Nov") && !r.contains("15 Nov") && r.contains(r#""version":3"#), "current version only: {r}");
        let r = engine.query(r#"RECALL FROM SEMANTIC ALL"#, None).await.unwrap();
        assert_eq!(r["data"]["count"], 1, "one record, not one per version: {r}");
        let r = engine.query(r#"RECALL FROM SEMANTIC WHERE concept = "acme" FOLLOW LINKS TYPE "evidence""#, None).await.unwrap().to_string();
        assert!(r.contains(r#""bid_id":"e1""#), "links from the record survive a supersede: {r}");
        let r = engine.query(r#"RECALL FROM EPISODIC WHERE bid_id = "e1" FOLLOW LINKS TYPE "about""#, None).await.unwrap().to_string();
        assert!(r.contains("20 Nov"), "links to the record reach its current version: {r}");
        let r = engine.query(r#"LOOKUP FROM PROCEDURAL KEY id = "p1""#, None).await.unwrap().to_string();
        assert!(r.contains(r#""confidence":0.9"#) && r.contains(r#""version":2"#), "{r}");
    }
    // History in Lance: three versions of acme, v1 and v2 closed and chained.
    let rows = lance_history(dir.path()).await;
    let mut acme: Vec<_> = rows.iter().filter(|r| r.lineage_id == "semantic:acme").collect();
    acme.sort_by_key(|r| r.version);
    assert_eq!(acme.iter().map(|r| r.version).collect::<Vec<_>>(), vec![1, 2, 3]);
    assert_eq!(acme[0].superseded_by.as_deref(), Some(acme[1].id.as_str()));
    assert_eq!(acme[1].superseded_by.as_deref(), Some(acme[2].id.as_str()));
    assert!(acme[2].superseded_by.is_none() && acme[0].valid_to.is_some());
    assert_eq!(acme[0].data.text.as_deref(), Some("Acme renews on 1 Nov"), "old versions keep their values");

    // After a restart: same current view; then FORGET takes every version.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = engine.query(fact, None).await.unwrap().to_string();
    assert!(r.contains("20 Nov") && r.contains(r#""version":3"#), "{r}");
    let n = engine.query(r#"FORGET FROM SEMANTIC WHERE concept = "acme""#, None).await.unwrap();
    assert_eq!(n["data"]["affected"].as_u64().or(n["data"]["count"].as_u64()).or(n["data"]["affected_count"].as_u64()), Some(1), "one lineage: {n}");
    drop(engine);
    let rows = lance_history(dir.path()).await;
    assert!(rows.iter().filter(|r| r.lineage_id == "semantic:acme").all(|r| r.tombstone), "every version tombstoned");
}

/// D2 T5 (D4.4a): FORGET matches every version. A value that survives only in a
/// superseded version still finds the record, and the whole lineage goes.
#[tokio::test]
async fn forget_matches_values_only_in_old_versions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    for q in [
        r#"STORE INTO SEMANTIC (key = "u1", concept = "user", email = "old@example.com")"#,
        r#"UPDATE INTO SEMANTIC WHERE concept = "user" (email = "new@example.com")"#,
        r#"STORE INTO SEMANTIC (key = "u2", concept = "other", email = "keep@example.com")"#,
        r#"STORE INTO PROCEDURAL (pattern_id = "p1", pattern = "rotate token", steps = "call ops", severity = "high")"#,
        r#"UPDATE INTO PROCEDURAL WHERE pattern_id = "p1" (severity = "low")"#,
    ] {
        engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    let current = engine.query(r#"RECALL FROM SEMANTIC WHERE email = "old@example.com""#, None).await.unwrap();
    assert_eq!(current["data"]["count"], 0, "the old value isn't current: {current}");
    let n = engine.query(r#"FORGET FROM SEMANTIC WHERE email = "old@example.com""#, None).await.unwrap();
    assert!(n.to_string().contains("1"), "{n}");
    let left = engine.query(r#"RECALL FROM SEMANTIC ALL"#, None).await.unwrap().to_string();
    assert!(!left.contains("new@example.com") && left.contains("keep@example.com"), "the whole lineage is gone, the other record stays: {left}");
    engine.query(r#"FORGET FROM PROCEDURAL WHERE severity = "high""#, None).await.unwrap();
    let r = engine.query(r#"LOOKUP FROM PROCEDURAL ALL"#, None).await.unwrap();
    assert_eq!(r["data"]["count"], 0, "{r}");
    drop(engine);
    let rows = lance_history(dir.path()).await;
    assert!(rows.iter().filter(|r| r.lineage_id == "semantic:u1" || r.lineage_id == "procedural:p1").all(|r| r.tombstone));
}

/// Keys are unique among current SEMANTIC / EPISODIC / PROCEDURAL records (D3 S4):
/// a second STORE of a key in use fails with key_exists, live and after a restart,
/// on the durable and the in-memory engine; FORGET frees the key; WORKING and
/// TOOLS keep replace-by-key; STOREs without a key never collide.
#[tokio::test]
async fn store_refuses_a_key_in_use() {
    async fn seed(engine: &LocalEngine) {
        for q in [
            r#"STORE INTO SEMANTIC (key = "k1", content = "one")"#,
            r#"STORE INTO EPISODIC (key = "k1", outcome = "won")"#,
            r#"STORE INTO PROCEDURAL (pattern_id = "p1", pattern = "oom", steps = "a")"#,
            r#"STORE INTO WORKING (key = "w", step = 1)"#,
            r#"STORE INTO WORKING (key = "w", step = 2)"#,
            r#"STORE INTO TOOLS (tool_id = "t", name = "t", description = "one")"#,
            r#"STORE INTO TOOLS (tool_id = "t", name = "t", description = "two")"#,
            r#"STORE INTO SEMANTIC (content = "no key")"#,
            r#"STORE INTO SEMANTIC (content = "no key")"#,
        ] {
            engine.query(q, None).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
    }
    async fn check(engine: &LocalEngine, label: &str) {
        for (m, again) in [
            ("SEMANTIC", r#"(key = "k1", content = "two")"#),
            ("EPISODIC", r#"(key = "k1", outcome = "lost")"#),
            ("PROCEDURAL", r#"(pattern_id = "p1", pattern = "oom", steps = "b")"#),
        ] {
            let msg = format!("{:?}", engine.query(&format!("STORE INTO {m} {again}"), None).await);
            assert!(msg.contains("key_exists"), "{label}: {m} second STORE should fail with key_exists: {msg}");
        }
        let r = engine.query(r#"LOOKUP FROM SEMANTIC KEY id = "k1""#, None).await.unwrap().to_string();
        assert!(r.contains(r#""content":"one""#), "{label}: the first record is untouched: {r}");
    }
    let dir = tempfile::tempdir().unwrap();
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        seed(&engine).await;
        check(&engine, "durable").await;
    }
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    check(&engine, "after restart").await;
    // FORGET frees the key.
    engine.query(r#"FORGET FROM SEMANTIC WHERE key = "k1""#, None).await.unwrap();
    engine.query(r#"STORE INTO SEMANTIC (key = "k1", content = "three")"#, None).await.unwrap();

    let mem = LocalEngine::new(std::sync::Arc::new(adb_backends::Adb::new()));
    seed(&mem).await;
    check(&mem, "in-memory").await;
}

/// `ORDER BY created_at` sorts on the record's own commit time when the payload
/// has no such field (the MCP guide's recency query), live and after a restart.
#[tokio::test]
async fn order_by_created_at_uses_the_records_time() {
    let dir = tempfile::tempdir().unwrap();
    let ids = |v: Value| -> Vec<String> { v["data"]["records"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap().to_string()).collect() };
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        for k in ["o1", "o2", "o3"] {
            engine.query(&format!(r#"STORE INTO SEMANTIC (key = "{k}", content = "renews {k}")"#), None).await.unwrap();
            engine.query(&format!(r#"STORE INTO EPISODIC (key = "{k}", content = "event {k}")"#), None).await.unwrap();
        }
        for m in ["SEMANTIC", "EPISODIC"] {
            let r = engine.query(&format!("RECALL FROM {m} ALL ORDER BY created_at DESC"), None).await.unwrap();
            assert_eq!(ids(r), ["o3", "o2", "o1"], "{m} newest first");
        }
        let r = engine.query(r#"RECALL FROM SEMANTIC WHERE content CONTAINS "renews" ORDER BY created_at ASC LIMIT 2"#, None).await.unwrap();
        assert_eq!(ids(r), ["o1", "o2"]);
    }
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let r = engine.query("RECALL FROM SEMANTIC ALL ORDER BY created_at DESC", None).await.unwrap();
    assert_eq!(ids(r), ["o3", "o2", "o1"], "after a restart");
}

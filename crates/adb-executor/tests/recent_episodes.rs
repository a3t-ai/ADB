//! Newest-first episodes from the recent-episodes index answer exactly as the full
//! read does: same records, same order — through forgets, an expired episode, a
//! namespace with fewer than k, a LIMIT past what the index keeps, and a restart
//! (the index rebuilt from the table).

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(e: &LocalEngine, aql: &str) -> Value {
    e.query(aql, None).await.unwrap_or_else(|x| panic!("{aql}: {x}"))
}

fn ids(v: &Value) -> Vec<String> {
    v["data"]["records"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap().to_string()).collect()
}

/// The index's answer (no WHERE) against the full read (a WHERE every episode matches).
async fn same(e: &LocalEngine, ns: &str, k: usize) -> Vec<String> {
    let fast = q(e, &format!(r#"RECALL FROM EPISODIC LIMIT {k} NAMESPACE "{ns}""#)).await;
    let full = q(e, &format!(r#"RECALL FROM EPISODIC WHERE n >= 0 LIMIT {k} NAMESPACE "{ns}""#)).await;
    assert_eq!(ids(&fast), ids(&full), "{ns} LIMIT {k}");
    assert_eq!(fast["data"]["records"], full["data"]["records"], "{ns} LIMIT {k}: the same records");
    ids(&fast)
}

async fn index_ready(e: &LocalEngine) {
    let store = e.local_spaces().and_then(|s| s.store).expect("a table");
    for _ in 0..200 {
        if store.recent_episodes_ready() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("the recent-episodes index wasn't built");
}

#[tokio::test(flavor = "multi_thread")]
async fn newest_episodes_from_the_index_match_the_full_read() {
    std::env::set_var("ADB_EMBED", "off");
    let dir = tempfile::tempdir().unwrap();
    {
        let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
        index_ready(&e).await;
        for i in 0..300 {
            q(&e, &format!(r#"STORE INTO EPISODIC (key = "a{i}", n = {i}, content = "event {i}") NAMESPACE "a""#)).await;
        }
        for i in 0..5 {
            q(&e, &format!(r#"STORE INTO EPISODIC (key = "b{i}", n = {i}) NAMESPACE "b""#)).await;
        }
        let top = same(&e, "a", 10).await;
        assert_eq!(top.len(), 10);
        assert!(top[0].ends_with("a299"), "newest first: {top:?}");
        same(&e, "a", 1).await;
        same(&e, "a", 256).await;
        // Past what the index keeps (256): the full read answers.
        assert_eq!(same(&e, "a", 300).await.len(), 300);
        assert_eq!(same(&e, "b", 10).await.len(), 5, "fewer than k");
        assert!(same(&e, "none", 10).await.is_empty());

        // The newest forgotten: the next ones move up.
        for i in [299, 298, 290] {
            q(&e, &format!(r#"FORGET FROM EPISODIC WHERE key = "a{i}" NAMESPACE "a""#)).await;
        }
        let after = same(&e, "a", 10).await;
        assert!(after[0].ends_with("a297"), "{after:?}");
        same(&e, "a", 256).await;

        // An episode that expires: gone from both.
        q(&e, r#"STORE INTO EPISODIC (key = "brief", n = 1000) NAMESPACE "a" TTL 1s"#).await;
        assert!(same(&e, "a", 3).await[0].ends_with("brief"));
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert!(!same(&e, "a", 3).await[0].ends_with("brief"));
    }
    // Reopened: the index is built from the table and answers the same.
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    index_ready(&e).await;
    let store = e.local_spaces().and_then(|s| s.store).unwrap();
    let now = adb_core::log::CommitTs::from_datetime(adb_core::time::now());
    assert!(store.recent_episodes("a", 10, now).await.unwrap().is_some(), "the index answers after a restart");
    same(&e, "a", 10).await;
    same(&e, "b", 10).await;
}

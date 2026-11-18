//! W9 (L4): writes that depend on what they read are checked when they commit.
//! `UPDATE … WHERE … AND metadata.version = n` is a compare-and-set: its WHERE is
//! evaluated under the sequencer after every earlier write is applied, so of many
//! concurrent updates from the same version exactly one wins. LINK checks that both
//! ends exist at commit time.
#![recursion_limit = "256"]

use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

async fn compare_and_set_once(memory: &str) {
    let engine = Arc::new(LocalEngine::temporary().await.unwrap());
    q(&engine, &format!(r#"STORE INTO {memory} (key = "counter", n = 0)"#)).await;
    let v = q(&engine, &format!(r#"RECALL FROM {memory} WHERE id = "counter""#)).await;
    let version = v["data"]["records"][0]["metadata"]["version"].as_u64().unwrap();

    let tasks: Vec<_> = (0..20)
        .map(|i| {
            let e = engine.clone();
            let memory = memory.to_string();
            tokio::spawn(async move {
                let r = q(&e, &format!(r#"UPDATE INTO {memory} WHERE key = "counter" AND metadata.version = {version} (n = {i})"#)).await;
                r["data"]["count"].as_u64().unwrap()
            })
        })
        .collect();
    let mut won = 0;
    for t in tasks {
        won += t.await.unwrap();
    }
    assert_eq!(won, 1, "{memory}: exactly one update from version {version} wins");
    let v = q(&engine, &format!(r#"RECALL FROM {memory} WHERE id = "counter""#)).await;
    assert_eq!(v["data"]["records"][0]["metadata"]["version"].as_u64().unwrap(), version + 1);
}

#[tokio::test]
async fn update_where_version_is_a_compare_and_set() {
    compare_and_set_once("SEMANTIC").await;
    compare_and_set_once("PROCEDURAL").await;
}

#[tokio::test]
async fn a_link_to_a_forgotten_record_is_refused() {
    let engine = Arc::new(LocalEngine::temporary().await.unwrap());
    q(&engine, r#"STORE INTO SEMANTIC (key = "a", c = "a")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC (key = "b", c = "b")"#).await;
    // The executor finds both records, then the target is forgotten before the
    // link commits: the write path itself must refuse.
    q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "b""#).await;
    let w = engine.executor().writes().clone();
    let e = w.link(adb_core::MemoryType::Semantic, "a", adb_core::MemoryType::Semantic, "b", "rel", None).await.unwrap_err();
    assert!(e.to_string().contains("target record"), "{e}");
    let e = w.link(adb_core::MemoryType::Semantic, "nope", adb_core::MemoryType::Semantic, "a", "rel", None).await.unwrap_err();
    assert!(e.to_string().contains("source record"), "{e}");
    w.link(adb_core::MemoryType::Semantic, "a", adb_core::MemoryType::Semantic, "a", "self", None).await.unwrap();
}

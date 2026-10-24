//! Writes by id (stage C): resolving a predicate and applying to ids matches the
//! predicate forms, and applying under a fixed clock is deterministic.

use std::time::Duration;

use adb_backends::Adb;
use adb_core::{time, MemoryType, Predicate, Scope};
use chrono::{DateTime, Utc};
use serde_json::json;

fn t(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

#[tokio::test]
async fn targets_then_ids_equals_update_and_forget() {
    let adb = Adb::new();
    for (k, c) in [("a", "summer"), ("b", "summer"), ("c", "winter")] {
        adb.store(MemoryType::Episodic, k, json!({"bid_id": k, "campaign": c})).await.unwrap();
    }
    let summer = Predicate::where_eq("campaign", "summer");

    let mut ids = adb.update_targets(MemoryType::Episodic, &summer).await.unwrap();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
    assert_eq!(adb.update_ids(MemoryType::Episodic, &ids, json!({"won": true})).await.unwrap(), 2);

    let won = adb.forget_targets(MemoryType::Episodic, &Predicate::where_eq("won", true)).await.unwrap();
    assert_eq!(won.len(), 2);
    assert_eq!(adb.forget_ids(MemoryType::Episodic, &won).await.unwrap(), 2);
    assert_eq!(adb.count(MemoryType::Episodic).await, 1);
    // Applying again to ids that are gone changes nothing.
    assert_eq!(adb.forget_ids(MemoryType::Episodic, &won).await.unwrap(), 0);
    assert_eq!(adb.update_ids(MemoryType::Episodic, &won, json!({"x": 1})).await.unwrap(), 0);
}

#[tokio::test]
async fn procedural_forget_keeps_the_index_right() {
    // petgraph moves its last node into a removed slot; the id map must follow.
    let adb = Adb::new();
    for id in ["p1", "p2", "p3"] {
        adb.store(MemoryType::Procedural, id, json!({"pattern_id": id, "pattern": format!("pattern {id}")}))
            .await
            .unwrap();
    }
    assert_eq!(adb.forget_ids(MemoryType::Procedural, &["p1".to_string()]).await.unwrap(), 1);

    // p3 now sits in p1's old slot: updating it by id must hit p3, not something else.
    assert_eq!(
        adb.update_ids(MemoryType::Procedural, &["p3".to_string()], json!({"pattern": "renamed p3"})).await.unwrap(),
        1
    );
    let p3 = adb.update_targets(MemoryType::Procedural, &Predicate::where_eq("pattern", "renamed p3")).await.unwrap();
    assert_eq!(p3, vec!["p3"]);
    let p2 = adb.update_targets(MemoryType::Procedural, &Predicate::where_eq("pattern", "pattern p2")).await.unwrap();
    assert_eq!(p2, vec!["p2"]);
    assert_eq!(adb.count(MemoryType::Procedural).await, 2);
}

#[tokio::test]
async fn working_update_by_id_follows_the_fixed_clock() {
    let adb = Adb::new();
    let t0 = t("2026-10-24T10:00:00Z");
    time::at(t0, async {
        adb.store_with_options(MemoryType::Working, "r1", json!({"race_key": "r1"}), Scope::Private, None, Some(Duration::from_millis(500)))
            .await
            .unwrap();
    })
    .await;
    let ids = vec!["r1".to_string()];

    // Replayed at its commit time (inside the TTL), the update applies...
    let inside = time::at(t0 + chrono::Duration::milliseconds(100), adb.update_ids(MemoryType::Working, &ids, json!({"status": "done"})));
    assert_eq!(inside.await.unwrap(), 1);
    // ...and at a time after expiry it doesn't, whatever the wall clock says.
    let after = time::at(t0 + chrono::Duration::seconds(1), adb.update_ids(MemoryType::Working, &ids, json!({"status": "late"})));
    assert_eq!(after.await.unwrap(), 0);
}

#[tokio::test]
async fn store_under_a_fixed_clock_is_reproducible() {
    let at = t("2026-10-24T10:00:00Z");
    let mut runs = Vec::new();
    for _ in 0..2 {
        let adb = Adb::new();
        let rec = time::at(at, adb.store(MemoryType::Semantic, "acme", json!({"concept": "acme", "content": "renews 1 Nov"})))
            .await
            .unwrap();
        runs.push((rec.id, rec.metadata.created_at, rec.data));
    }
    assert_eq!(runs[0], runs[1]);
    assert_eq!(runs[0].1, at);
}

#[test]
fn store_id_follows_the_backend_rules() {
    assert_eq!(Adb::store_id(MemoryType::Tools, "k", &json!({"tool_id": "search"})), "search");
    assert_eq!(Adb::store_id(MemoryType::Procedural, "k", &json!({"pattern_id": "oom-fix"})), "oom-fix");
    assert_eq!(Adb::store_id(MemoryType::Semantic, "k", &json!({"pattern_id": "ignored"})), "k");
    assert_eq!(Adb::store_id(MemoryType::Tools, "k", &json!({})), "k");
}

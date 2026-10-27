//! Stage G: platform tables — `call:lancedb` semantics, persistence, and recovery
//! when a crash lands between a Lance commit and `_applied.json`.

use adb_core::log::{CommitTs, Effect, Epoch, LogOffset, LogRecord, PartitionId};
use adb_platform::{batches_to_json, PlatformStore};
use serde_json::{json, Value};

fn rec(offset: u64, effects: Vec<Effect>) -> LogRecord {
    LogRecord {
        partition: PartitionId(0),
        offset: LogOffset(offset),
        epoch: Epoch(1),
        commit_ts: CommitTs(1_761_300_000_000_000 + offset as i64),
        txn_id: None,
        effects,
    }
}

fn rows(v: Value) -> Vec<serde_json::Map<String, Value>> {
    v.as_array().unwrap().iter().map(|r| r.as_object().unwrap().clone()).collect()
}

fn insert(table: &str, v: Value) -> Effect {
    Effect::TableInsert { table: table.into(), rows: rows(v) }
}

async fn select(s: &PlatformStore, table: &str, filter: Option<&str>) -> Vec<Value> {
    let mut r = batches_to_json(&s.query(table, filter, 1000).await.unwrap());
    r.sort_by_key(|v| v["id"].as_str().unwrap_or_default().to_string());
    r
}

fn ids(rows: &[Value]) -> Vec<&str> {
    rows.iter().map(|r| r["id"].as_str().unwrap()).collect()
}

/// The A3T flows' shapes: create_table, insert, query by `org_id = "…"` (double
/// quotes, as the flows write it) and by id, update with SQL literals, delete.
#[tokio::test]
async fn call_lancedb_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    s.apply(&rec(1, vec![Effect::TableCreate { table: "template_registry".into(), columns: vec!["id".into(), "org_id".into(), "name".into(), "status".into()] }])).await.unwrap();
    s.apply(&rec(2, vec![insert("template_registry", json!([
        {"id": "t1", "org_id": "org-default", "name": "Support", "status": "active"},
        {"id": "t2", "org_id": "org-default", "name": "Ops", "status": "draft"},
        {"id": "t3", "org_id": "org-other", "name": "Sales's", "status": "active"},
    ]))])).await.unwrap();
    assert_eq!(ids(&select(&s, "template_registry", Some(r#"org_id = "org-default""#)).await), ["t1", "t2"], "double-quoted values, as A3T writes them");
    assert_eq!(ids(&select(&s, "template_registry", Some("id = 't3' AND org_id = 'org-other'")).await), ["t3"]);
    assert_eq!(select(&s, "template_registry", None).await.len(), 3);
    // Update: values are SQL expressions (call:lancedb quotes strings, '' escapes).
    s.apply(&rec(3, vec![Effect::TableUpdate { table: "template_registry".into(), filter: "id = 't2'".into(), values: vec![("status".into(), "'active'".into()), ("name".into(), "'Ops''s'".into())] }])).await.unwrap();
    let t2 = &select(&s, "template_registry", Some("id = 't2'")).await[0];
    assert_eq!((t2["status"].as_str(), t2["name"].as_str()), (Some("active"), Some("Ops's")));
    s.apply(&rec(4, vec![Effect::TableDelete { table: "template_registry".into(), filter: "id = 't1'".into() }])).await.unwrap();
    assert_eq!(ids(&select(&s, "template_registry", None).await), ["t2", "t3"]);
    // Insert into a missing table creates it from the first row; list_tables.
    s.apply(&rec(5, vec![insert("audit_log", json!([{"id": "a1", "event": "login", "n": 3}]))])).await.unwrap();
    assert_eq!(select(&s, "audit_log", None).await[0]["n"], "3", "non-strings stored as their JSON text");
    assert_eq!(s.list_tables().unwrap(), ["audit_log", "template_registry"]);
    // Errors: a missing table, a bad name.
    assert!(s.query("nope", None, 10).await.is_err());
    assert!(s.validate(&Effect::TableDelete { table: "nope".into(), filter: "id = 'x'".into() }).await.is_err());
    assert!(s.validate(&Effect::TableInsert { table: "../etc".into(), rows: vec![] }).await.is_err());
    assert!(s.validate(&insert("template_registry", json!([{"id": "t9", "colour": "teal"}]))).await.is_err(), "unknown column");
    assert!(s.validate(&Effect::TableUpdate { table: "template_registry".into(), filter: "id = 't2'".into(), values: vec![("colour".into(), "'x'".into())] }).await.is_err());
    assert!(s.validate(&Effect::TableDelete { table: "template_registry".into(), filter: "id = = 'x'".into() }).await.is_err(), "bad filter");
    // A row without some columns is fine: they are null.
    s.apply(&rec(6, vec![insert("template_registry", json!([{"id": "t4", "org_id": "org-default"}]))])).await.unwrap();
    assert_eq!(select(&s, "template_registry", Some("id = 't4'")).await[0].get("name"), None);
    s.apply(&rec(7, vec![Effect::TableDelete { table: "template_registry".into(), filter: "id = 't4'".into() }])).await.unwrap();
    // Replace: new columns and rows in one step; an empty replace keeps the columns.
    s.apply(&rec(8, vec![Effect::TableReplace {
        table: "settings".into(),
        columns: vec!["key".into(), "value".into(), "scope".into()],
        rows: rows(json!([{"key": "theme", "value": "dark"}, {"key": "lang", "value": "en", "scope": "org"}])),
    }])).await.unwrap();
    s.apply(&rec(9, vec![Effect::TableReplace {
        table: "settings".into(),
        columns: vec!["key".into(), "value".into()],
        rows: rows(json!([{"key": "theme", "value": "light"}])),
    }])).await.unwrap();
    let st = batches_to_json(&s.query("settings", None, 10).await.unwrap());
    assert_eq!(st, vec![json!({"key": "theme", "value": "light"})], "replaced, columns included");
    assert!(s.schema("settings").await.unwrap().field_with_name("scope").is_err());
    assert!(s.validate(&Effect::TableReplace { table: "settings".into(), columns: vec!["key".into()], rows: rows(json!([{"key": "a", "x": "1"}])) }).await.is_err());
    assert_eq!(s.applied_offset().await, 9);

    // Reopen: everything is there; applied records are skipped.
    drop(s);
    let s = PlatformStore::open(dir.path()).await.unwrap();
    assert_eq!(s.applied_offset().await, 9);
    assert_eq!(ids(&select(&s, "template_registry", None).await), ["t2", "t3"]);
    assert!(!s.apply(&rec(2, vec![insert("template_registry", json!([{"id": "dup"}]))])).await.unwrap());
}

/// A crash after a record's Lance commits but before `_applied.json` was rewritten:
/// replaying that record must not apply it again, and must not let a replayed
/// update or delete touch rows inserted after it.
#[tokio::test]
async fn replay_after_a_crash_between_commit_and_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log = vec![
        rec(1, vec![
            Effect::TableCreate { table: "agents".into(), columns: vec!["id".into(), "org_id".into(), "status".into()] },
            insert("agents", json!([{"id": "a1", "org_id": "o", "status": "new"}])),
        ]),
        rec(2, vec![Effect::TableUpdate { table: "agents".into(), filter: "org_id = 'o'".into(), values: vec![("status".into(), "'ready'".into())] }]),
        rec(3, vec![insert("agents", json!([{"id": "a2", "org_id": "o", "status": "new"}]))]),
        rec(4, vec![
            insert("events", json!([{"id": "e1", "agent": "a1"}])),
            insert("events", json!([{"id": "e2", "agent": "a2"}])),
        ]),
        rec(5, vec![Effect::TableDelete { table: "agents".into(), filter: "id = 'a1'".into() }]),
    ];
    let want = {
        let s = PlatformStore::open(dir.path().join("reference")).await.unwrap();
        for r in &log {
            s.apply(r).await.unwrap();
        }
        (select(&s, "agents", None).await, select(&s, "events", None).await)
    };
    assert_eq!(ids(&want.0), ["a2"]);
    assert_eq!(want.0[0]["status"], "new", "a2 was inserted after the update");

    // For every crash point k: all of records 1..=k committed in Lance, but the
    // checkpoint still says k-1. Reopen and replay the whole log.
    for k in 1..=log.len() {
        let d = dir.path().join(format!("crash-{k}"));
        let checkpoint;
        {
            let s = PlatformStore::open(&d).await.unwrap();
            for r in &log[..k - 1] {
                s.apply(r).await.unwrap();
            }
            checkpoint = std::fs::read(d.join("_applied.json")).ok();
            s.apply(&log[k - 1]).await.unwrap();
        }
        match checkpoint {
            Some(c) => std::fs::write(d.join("_applied.json"), c).unwrap(),
            None => std::fs::remove_file(d.join("_applied.json")).unwrap(),
        }
        let s = PlatformStore::open(&d).await.unwrap();
        for r in &log {
            s.apply(r).await.unwrap();
        }
        let got = (select(&s, "agents", None).await, select(&s, "events", None).await);
        assert_eq!(got, want, "crash after record {k}'s commit");
    }
}

/// Every file under `dir` that contains `needle`.
fn files_with(dir: &std::path::Path, needle: &[u8]) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if std::fs::read(&p).unwrap().windows(needle.len()).any(|w| w == needle) {
                out.push(p);
            }
        }
    }
    out
}

/// W5-L4: compaction removes deleted and updated-away rows from disk (all old
/// versions too), keeps the live ones, and later writes still apply — also after a
/// compaction a crash interrupted.
#[tokio::test]
async fn compaction_erases_deleted_rows() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    s.apply(&rec(1, vec![insert("users", json!([
        {"id": "u1", "email": "a@x", "hash": "secret-hash-one"},
        {"id": "u2", "email": "b@x", "hash": "keep-hash-two"},
    ]))])).await.unwrap();
    s.apply(&rec(2, vec![insert("users", json!([{"id": "u3", "email": "c@x", "hash": "old-hash-three"}]))])).await.unwrap();
    s.apply(&rec(3, vec![Effect::TableUpdate { table: "users".into(), filter: "id = 'u3'".into(), values: vec![("hash".into(), "'new-hash-three'".into())] }])).await.unwrap();
    s.apply(&rec(4, vec![Effect::TableDelete { table: "users".into(), filter: "id = 'u1'".into() }])).await.unwrap();
    assert!(!files_with(dir.path(), b"secret-hash-one").is_empty(), "a delete is logical before compaction");

    let st = s.compact().await.unwrap();
    assert_eq!(st.tables, 1);
    assert!(st.old_versions > 0, "{st:?}");
    assert_eq!(files_with(dir.path(), b"secret-hash-one"), Vec::<std::path::PathBuf>::new(), "deleted row erased");
    assert_eq!(files_with(dir.path(), b"old-hash-three"), Vec::<std::path::PathBuf>::new(), "updated-away value erased");
    let live = select(&s, "users", None).await;
    assert_eq!(ids(&live), ["u2", "u3"]);
    assert_eq!(live[1]["hash"], "new-hash-three");

    // Writes after it apply (compaction's versions aren't mistaken for effects).
    s.apply(&rec(5, vec![insert("users", json!([{"id": "u4", "email": "d@x", "hash": "h4"}]))])).await.unwrap();
    s.apply(&rec(6, vec![Effect::TableDelete { table: "users".into(), filter: "id = 'u2'".into() }])).await.unwrap();
    assert_eq!(ids(&select(&s, "users", None).await), ["u3", "u4"]);

    // A crash during a compaction: `_applied.json` still names the table, Lance has
    // moved on. Reopen settles it, and the next write applies.
    s.compact().await.unwrap();
    drop(s);
    let path = dir.path().join("_applied.json");
    let mut a: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let v = a["versions"]["users"].as_u64().unwrap();
    a["versions"]["users"] = json!(v - 1);
    a["compacting"] = json!("users");
    std::fs::write(&path, serde_json::to_vec(&a).unwrap()).unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    s.apply(&rec(7, vec![insert("users", json!([{"id": "u5", "email": "e@x", "hash": "h5"}]))])).await.unwrap();
    assert_eq!(ids(&select(&s, "users", None).await), ["u3", "u4", "u5"]);
}

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
    // checkpoint still says k-1. Reopen and replay the whole log. A record that went
    // through the memtable (PF-5) reached Lance in a flush, whose marker was written
    // before its commit: the crash leaves that marker too.
    for k in 1..=log.len() {
        let d = dir.path().join(format!("crash-{k}"));
        let checkpoint;
        let mut marker = None;
        {
            let s = PlatformStore::open(&d).await.unwrap();
            for r in &log[..k - 1] {
                s.apply(r).await.unwrap();
            }
            s.flush().await.unwrap();
            checkpoint = std::fs::read(d.join("_applied.json")).ok();
            let mut bases = Vec::new();
            for t in log[k - 1].effects.iter().filter_map(adb_platform::table_of) {
                if let Some(v) = s.version(t).await {
                    bases.push(json!([t, v]));
                }
            }
            s.apply(&log[k - 1]).await.unwrap();
            if s.pending_rows() > 0 {
                marker = Some(json!({ "to": k, "tables": bases }));
            }
            s.flush().await.unwrap();
        }
        match checkpoint {
            Some(c) => {
                let mut c: Value = serde_json::from_slice(&c).unwrap();
                if let Some(m) = marker {
                    c["flush"] = m;
                }
                std::fs::write(d.join("_applied.json"), serde_json::to_vec(&c).unwrap()).unwrap()
            }
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

/// CA-8: one upsert replaces the rows with the same key and inserts the rest, in one
/// version; a key repeated in one upsert keeps its last row; a missing table is created.
#[tokio::test]
async fn upsert_replaces_by_key_in_one_version() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    let upsert = |v: Value| Effect::TableUpsert { table: "state".into(), key: vec!["key".into()], rows: rows(v) };
    // Creates the table.
    s.apply(&rec(1, vec![upsert(json!([{ "id": "1", "key": "a", "value": "1" }, { "id": "2", "key": "b", "value": "2" }]))])).await.unwrap();
    let v1 = s.version("state").await.unwrap();
    // Replaces b, adds c; c twice: the last row wins.
    s.apply(&rec(2, vec![upsert(json!([{ "id": "2", "key": "b", "value": "20" }, { "id": "3", "key": "c", "value": "3" }, { "id": "3", "key": "c", "value": "30" }]))])).await.unwrap();
    // It waits in memory (PF-5) and reaches Lance with the next flush.
    s.flush().await.unwrap();
    assert_eq!(s.version("state").await.unwrap(), v1 + 1, "one version for the upsert");
    let all = select(&s, "state", None).await;
    assert_eq!(ids(&all), ["1", "2", "3"]);
    let by_key: std::collections::HashMap<&str, &str> = all.iter().map(|r| (r["key"].as_str().unwrap(), r["value"].as_str().unwrap())).collect();
    assert_eq!(by_key["a"], "1");
    assert_eq!(by_key["b"], "20");
    assert_eq!(by_key["c"], "30");
    // Validation: rows need their key column.
    assert!(s.validate(&Effect::TableUpsert { table: "state".into(), key: vec!["key".into()], rows: rows(json!([{ "id": "9" }])) }).await.is_err());
}

/// DB-6: a transaction is one record of several effects across tables, checked
/// together; a table created earlier in it counts as existing; after a schema change
/// a later update of that table in the same transaction is refused. Adding columns
/// keeps old rows (null) and survives a reopen.
#[tokio::test]
async fn transactions_and_added_columns() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    let tx = vec![
        Effect::TableCreate { table: "agents".into(), columns: vec!["id".into(), "name".into()] },
        insert("agents", json!([{"id": "a1", "name": "Bot"}])),
        insert("agent_versions", json!([{"id": "v1", "agent": "a1"}])),
    ];
    s.validate_all(&tx).await.unwrap();
    s.apply(&rec(1, tx)).await.unwrap();
    assert_eq!(ids(&select(&s, "agents", None).await), ["a1"]);
    assert_eq!(ids(&select(&s, "agent_versions", None).await), ["v1"]);

    let bad = vec![
        Effect::TableAddColumns { table: "agents".into(), columns: vec!["status".into()] },
        Effect::TableUpdate { table: "agents".into(), filter: "id = 'a1'".into(), values: vec![("status".into(), "'live'".into())] },
    ];
    let e = s.validate_all(&bad).await.unwrap_err().to_string();
    assert!(e.contains("changed shape earlier in this transaction"), "{e}");
    assert!(s.validate_all(&[Effect::TableAddColumns { table: "agents".into(), columns: vec!["1x".into()] }]).await.is_err());
    assert!(s.validate_all(&[Effect::TableAddColumns { table: "nope".into(), columns: vec!["x".into()] }]).await.is_err());

    let add = vec![Effect::TableAddColumns { table: "agents".into(), columns: vec!["status".into(), "name".into()] }];
    s.validate_all(&add).await.unwrap();
    s.apply(&rec(2, add.clone())).await.unwrap();
    // Applying the same columns again (a replayed record past a crash) adds nothing.
    s.apply(&rec(3, add)).await.unwrap();
    let r = select(&s, "agents", None).await;
    assert_eq!(r[0]["name"], json!("Bot"));
    assert!(r[0].get("status").map_or(true, |v| v.is_null()), "old rows read null: {:?}", r[0]);
    let up = Effect::TableUpdate { table: "agents".into(), filter: "id = 'a1'".into(), values: vec![("status".into(), "'live'".into())] };
    s.validate(&up).await.unwrap();
    s.apply(&rec(4, vec![up, insert("agents", json!([{"id": "a2", "name": "Two", "status": "draft"}]))])).await.unwrap();
    drop(s);
    let s = PlatformStore::open(dir.path()).await.unwrap();
    let r = select(&s, "agents", None).await;
    assert_eq!(r.iter().map(|r| r["status"].as_str().unwrap_or("")).collect::<Vec<_>>(), ["live", "draft"]);
}

/// DB-13: an append-only table takes inserts and purges by age; update, delete,
/// upsert and replace are refused, also inside a transaction and after a reopen.
#[tokio::test]
async fn append_only_tables() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    let tx = vec![
        Effect::TableCreate { table: "a3t_errors".into(), columns: vec!["id".into(), "time".into(), "class".into()] },
        Effect::TableAppendOnly { table: "a3t_errors".into() },
        insert("a3t_errors", json!([{"id": "e1", "time": "2026-10-01T00:00:00Z", "class": "timeout"}])),
    ];
    s.validate_all(&tx).await.unwrap();
    s.apply(&rec(1, tx)).await.unwrap();
    s.apply(&rec(2, vec![insert("a3t_errors", json!([{"id": "e2", "time": "2026-11-05T00:00:00Z", "class": "denied"}]))])).await.unwrap();
    for e in [
        Effect::TableUpdate { table: "a3t_errors".into(), filter: "id = 'e1'".into(), values: vec![("class".into(), "'x'".into())] },
        Effect::TableDelete { table: "a3t_errors".into(), filter: "id = 'e1'".into() },
        Effect::TableUpsert { table: "a3t_errors".into(), key: vec!["id".into()], rows: rows(json!([{"id": "e1", "class": "x"}])) },
        Effect::TableReplace { table: "a3t_errors".into(), columns: vec!["id".into()], rows: vec![] },
    ] {
        let err = s.validate(&e).await.unwrap_err().to_string();
        assert!(err.contains("append-only"), "{err}");
    }
    let purge = Effect::TablePurge { table: "a3t_errors".into(), column: "time".into(), before: "2026-11-01T00:00:00Z".into() };
    s.validate(&purge).await.unwrap();
    s.apply(&rec(3, vec![purge])).await.unwrap();
    assert_eq!(ids(&select(&s, "a3t_errors", None).await), ["e2"]);
    drop(s);
    let s = PlatformStore::open(dir.path()).await.unwrap();
    assert!(s.validate(&Effect::TableDelete { table: "a3t_errors".into(), filter: "id = 'e2'".into() }).await.is_err(), "still append-only after a reopen");
    assert!(s.validate(&Effect::TablePurge { table: "a3t_errors".into(), column: "nope".into(), before: "x".into() }).await.is_err());
}

/// PF-5: upserts into an existing table wait in memory and reach Lance in one commit
/// per flush; a write of another kind flushes first; nothing applies twice after a
/// reopen.
#[tokio::test]
async fn upserts_wait_in_memory_and_flush_as_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    let up = |id: &str, v: &str| Effect::TableUpsert { table: "flow_state".into(), key: vec!["id".into()], rows: rows(json!([{"id": id, "v": v}])) };
    s.apply(&rec(1, vec![up("a", "0")])).await.unwrap(); // creates the table (direct)
    let before = s.version("flow_state").await.unwrap();
    let batch = vec![
        rec(2, vec![up("a", "1")]),
        rec(3, vec![]), // a memory record (no platform effect)
        rec(4, vec![up("b", "1")]),
        rec(5, vec![up("a", "2"), up("c", "1")]),
        rec(6, vec![insert("other", json!([{"id": "x"}]))]), // new table: flushes first
        rec(7, vec![up("b", "2")]),
    ];
    let results: Vec<bool> = s.apply_batch(&batch).await.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(results, [true, false, true, true, true, true]);
    // Records 2–5 went to Lance as one commit when record 6 came; 7 waits in memory.
    assert_eq!(s.version("flow_state").await.unwrap(), before + 1);
    assert_eq!(s.pending_rows(), 1);
    let got = |rows: Vec<Value>| rows.iter().map(|r| (r["id"].as_str().unwrap().to_string(), r["v"].as_str().unwrap().to_string())).collect::<Vec<_>>();
    let want = vec![("a".to_string(), "2".to_string()), ("b".into(), "2".into()), ("c".into(), "1".into())];
    assert_eq!(got(select(&s, "flow_state", None).await), want, "reads see memory over Lance");
    assert_eq!(s.applied_offset().await, 6, "the log may be purged only below what Lance holds");
    s.flush().await.unwrap();
    assert_eq!(s.version("flow_state").await.unwrap(), before + 2);
    assert_eq!(s.applied_offset().await, 7);
    drop(s);
    let s = PlatformStore::open(dir.path()).await.unwrap();
    assert_eq!(s.applied_offset().await, 7);
    assert!(s.apply_batch(&batch).await.into_iter().all(|r| !r.unwrap()), "nothing applies twice");
    assert_eq!(got(select(&s, "flow_state", None).await), want);
}

/// PF-5: a read over memory and Lance filters both the way Lance does (A3T's double
/// quoted values), shows each key once (memory's version), keeps the limit, and an
/// update by filter sees what waited in memory.
#[tokio::test]
async fn reads_merge_memory_over_lance() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    let up = |id: &str, org: &str, v: &str| Effect::TableUpsert { table: "t".into(), key: vec!["id".into()], rows: rows(json!([{"id": id, "org": org, "v": v}])) };
    s.apply(&rec(1, vec![Effect::TableCreate { table: "t".into(), columns: vec!["id".into(), "org".into(), "v".into()] }])).await.unwrap();
    s.apply(&rec(2, vec![up("a", "o1", "1"), up("b", "o1", "1"), up("c", "o2", "1")])).await.unwrap();
    s.flush().await.unwrap();
    // In memory: a changes org, b changes value, d is new.
    s.apply(&rec(3, vec![up("a", "o2", "2"), up("b", "o1", "2"), up("d", "o1", "1")])).await.unwrap();
    assert!(s.pending_rows() > 0);
    let o1 = select(&s, "t", Some(r#"org = "o1""#)).await;
    assert_eq!(o1.iter().map(|r| (r["id"].as_str().unwrap(), r["v"].as_str().unwrap())).collect::<Vec<_>>(), [("b", "2"), ("d", "1")]);
    assert_eq!(select(&s, "t", None).await.len(), 4, "each key once");
    assert_eq!(batches_to_json(&s.query("t", None, 2).await.unwrap()).len(), 2, "limit kept");
    // An update by filter flushes first, so it changes the rows memory held too.
    s.apply(&rec(4, vec![Effect::TableUpdate { table: "t".into(), filter: "org = 'o1'".into(), values: vec![("v".into(), "'9'".into())] }])).await.unwrap();
    assert_eq!(s.pending_rows(), 0);
    let o1 = select(&s, "t", Some("org = 'o1'")).await;
    assert!(o1.iter().all(|r| r["v"] == "9") && o1.len() == 2, "{o1:?}");
}

/// PF-5: a crash after one table's flush commit and before the other's: on open the
/// replay puts back only what did not reach Lance (inserts not doubled).
#[tokio::test]
async fn a_crash_mid_flush_neither_loses_nor_doubles_rows() {
    let dir = tempfile::tempdir().unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    s.apply(&rec(1, vec![Effect::TableCreate { table: "a".into(), columns: vec!["id".into()] }])).await.unwrap();
    s.apply(&rec(2, vec![Effect::TableCreate { table: "b".into(), columns: vec!["id".into()] }])).await.unwrap();
    let base_a = s.version("a").await.unwrap();
    let base_b = s.version("b").await.unwrap();
    let r3 = rec(3, vec![insert("a", json!([{"id": "a1"}]))]);
    let r4 = rec(4, vec![insert("b", json!([{"id": "b1"}]))]);
    // Only a's rows reach Lance (b's commit "did not happen").
    s.apply(&r3).await.unwrap();
    s.flush().await.unwrap();
    drop(s);
    // The state a crash between the two commits leaves: the marker, the old offset.
    let path = dir.path().join("_applied.json");
    let mut applied: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    applied["offset"] = json!(2);
    applied.as_object_mut().unwrap().remove("flushed");
    applied["flush"] = json!({ "to": 4, "tables": [["a", base_a], ["b", base_b]] });
    std::fs::write(&path, serde_json::to_vec(&applied).unwrap()).unwrap();
    let s = PlatformStore::open(dir.path()).await.unwrap();
    assert_eq!(s.applied_offset().await, 2, "b's rows are not in Lance: the log replays from 3");
    s.apply_batch(&[r3, r4]).await.into_iter().for_each(|r| { r.unwrap(); });
    s.flush().await.unwrap();
    assert_eq!(ids(&select(&s, "a", None).await), ["a1"], "a's insert not doubled");
    assert_eq!(ids(&select(&s, "b", None).await), ["b1"], "b's insert not lost");
}

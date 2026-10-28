//! D3.6: AQL predicates pushed into the Lance scan return a superset of what the
//! backends' query code matches, on every layer (memtable, flushed generations), and
//! never fail a scan on payloads of mixed JSON types.

use std::time::Duration;

use adb_core::log::{
    CommitTs, Effect, Epoch, Kind, LogOffset, LogRecord, Memory, NewRow, PartitionId, Provenance, RecordId, RowData,
    Visibility,
};
use adb_core::{evaluate_conditions_on_record, Condition, LogicalOp, MemoryRecord, MemoryType, Metadata, Operator, Predicate, Scope, Value};
use adb_store::{predicate_filter, PartitionStore, StoreConfig};
use serde_json::json;

const T0: i64 = 1_761_300_000_000_000;

fn put(offset: u64, id: &str, value: serde_json::Value) -> LogRecord {
    let d = RowData {
        tenant_id: "default".into(),
        scope: "tenant:default".into(),
        visibility: Visibility::Private,
        namespace: None,
        memory: Memory::Episodic,
        kind: Kind::Episode,
        key: None,
        subject_id: None,
        predicate: None,
        object_id: None,
        link_type: None,
        weight: None,
        text: None,
        value,
        sealed: None,
        embedding: None,
        ttl_at: None,
        provenance: Provenance::default(),
    };
    LogRecord {
        partition: PartitionId(0),
        offset: LogOffset(offset),
        epoch: Epoch(1),
        commit_ts: CommitTs(T0 + offset as i64 * 1_000),
        txn_id: None,
        effects: vec![Effect::PutEpisode(NewRow { id: RecordId(id.into()), lineage_id: RecordId(id.into()), version: 1, data: d })],
    }
}

fn payloads() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        ("p1", json!({"campaign": "summer", "cpm": 3.2, "won": true, "n": 7})),
        ("p2", json!({"campaign": "winter", "cpm": 1.0, "won": false, "n": 2})),
        ("p3", json!({"campaign": "summer", "cpm": 0.1, "won": false, "n": -4})),
        ("p4", json!({"campaign": 7, "cpm": "high", "won": "yes", "n": 7.0})),
        ("p5", json!({"other": true})),
        ("p6", json!({"campaign": {"nested": "summer"}, "cpm": [1, 2, 3], "won": null, "n": "7"})),
        ("p7", json!({"campaign": "autumn", "cpm": 123456.789, "n": 9007199254740993_i64, "a.b": "dot"})),
        ("p8", json!({"campaign": "summer", "cpm": 1e-7, "won": true, "n": 3})),
        ("p9", json!("not an object")),
        ("p10", json!({"campaign": "spring", "cpm": 2.5, "n": 0, "data": {"x": 1}})),
    ]
}

fn c(field: &str, op: Operator, v: impl Into<Value>) -> Condition {
    Condition::new(field, op, v)
}

fn predicates() -> Vec<Predicate> {
    use Operator::*;
    let w = |conditions: Vec<Condition>| Predicate::Where { conditions };
    vec![
        Predicate::All,
        w(vec![c("campaign", Eq, "summer")]),
        w(vec![c("cpm", Gt, 1.0)]),
        w(vec![c("cpm", Gte, 1_i64)]),
        w(vec![c("cpm", Lt, 0.5)]),
        w(vec![c("cpm", Lte, 3.2)]),
        w(vec![c("cpm", Eq, 123456.789)]),
        w(vec![c("cpm", Eq, 1e-7)]),
        w(vec![c("n", Eq, 7_i64)]),
        w(vec![c("n", Gt, 9007199254740992_i64)]),
        w(vec![c("n", Gte, -4_i64)]),
        w(vec![c("won", Eq, true)]),
        w(vec![c("won", Eq, false)]),
        w(vec![c("campaign", Ne, "summer")]),
        w(vec![c("campaign", Gt, "spring")]),
        w(vec![c("campaign", In, Value::Array(vec!["summer".into(), "autumn".into()]))]),
        w(vec![c("campaign", In, Value::Array(vec![]))]),
        w(vec![c("campaign", Eq, "summer"), c("cpm", Gt, 1.0)]),
        w(vec![c("campaign", Eq, "winter"), c("cpm", Gt, 1.0).with_logical_op(LogicalOp::Or)]),
        // Left to right: (summer OR winter) AND won = true.
        w(vec![
            c("campaign", Eq, "summer"),
            c("campaign", Eq, "winter").with_logical_op(LogicalOp::Or),
            c("won", Eq, true).with_logical_op(LogicalOp::And),
        ]),
        // An untranslatable OR arm makes the whole OR TRUE.
        w(vec![c("campaign", Eq, "winter"), c("cpm", Ne, 1.0).with_logical_op(LogicalOp::Or)]),
        w(vec![
            c("won", Eq, true),
            Condition::Group {
                conditions: vec![c("n", Eq, 3_i64), c("cpm", Gt, 3.0).with_logical_op(LogicalOp::Or)],
                logical_op: Some(LogicalOp::And),
            },
        ]),
        w(vec![c("id", Eq, "p3")]),
        w(vec![c("id", Gt, "p5")]),
        w(vec![c("data.campaign", Eq, "summer")]),
        w(vec![c("data.data.x", Eq, 1_i64)]),
        w(vec![c("a.b", Eq, "dot")]),
        w(vec![c("metadata.version", Eq, 1_i64)]),
        w(vec![c("campaign", Eq, Value::Variable("v".into()))]),
        w(vec![c("campaign", Contains, "umm")]),
        Predicate::key("id", "p2"),
        Predicate::key("key", "p9"),
        Predicate::key("campaign", "summer"),
    ]
}

fn record(id: &str, data: &serde_json::Value) -> MemoryRecord {
    let mut m = Metadata::new(Scope::Private);
    m.version = 1;
    MemoryRecord { id: id.into(), memory_type: MemoryType::Episodic, data: data.clone(), metadata: m }
}

/// The backends' match (WHERE / KEY id on the record as they hold it).
fn backend_matches(p: &Predicate, r: &MemoryRecord) -> bool {
    match p {
        Predicate::All => true,
        Predicate::Where { conditions } => evaluate_conditions_on_record(r, conditions),
        Predicate::Key { field, value: Value::String(s) } if field == "id" || field == "key" => &r.id == s,
        Predicate::Key { .. } => true,
        _ => true,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pushdown_is_a_superset_on_every_layer() {
    adb_store::configure_lance_runtime();
    let dir = tempfile::tempdir().unwrap();
    // 3-row memtables: the 10 rows span several flushed generations and the memtable.
    let cfg = StoreConfig { root: dir.path().to_path_buf(), embedding_dim: 4, memtable_rows: 3, wal_flush: Duration::from_millis(20), embedding_model: None };
    let store = PartitionStore::open(&cfg, PartitionId(0)).await.unwrap();
    let rows = payloads();
    for (i, (id, v)) in rows.iter().enumerate() {
        store.apply(&put(i as u64 + 1, id, v.clone())).await.unwrap();
    }
    // p1 is rewritten in the memtable: summer → winter. A filter applied before the
    // LSM picks the newest version would return its old row.
    let p1_new = json!({"campaign": "winter", "cpm": 0.2, "won": false, "n": 1});
    store.apply(&put(rows.len() as u64 + 1, "p1", p1_new.clone())).await.unwrap();
    let current: Vec<(String, serde_json::Value)> = rows
        .iter()
        .map(|(id, v)| (id.to_string(), if *id == "p1" { p1_new.clone() } else { v.clone() }))
        .collect();

    let now = CommitTs(T0 + 1_000_000_000);
    let mut pushed = 0;
    for p in predicates() {
        let filter = predicate_filter(Memory::Episodic, &p);
        pushed += usize::from(filter.is_some());
        let got: std::collections::BTreeSet<String> = store
            .current_expr(filter.clone(), now)
            .await
            .unwrap_or_else(|e| panic!("{p:?}: {e}"))
            .into_iter()
            .map(|r| r.key().to_string())
            .collect();
        let want: std::collections::BTreeSet<String> =
            current.iter().filter(|(id, v)| backend_matches(&p, &record(id, v))).map(|(id, _)| id.clone()).collect();
        assert!(want.is_subset(&got), "{p:?}: backend matches {want:?}, pushdown returned only {got:?}");
        if filter.is_some() && !matches!(&p, Predicate::Where { conditions } if conditions.iter().any(|c| matches!(c, Condition::Simple { operator: Operator::Ne, .. }))) {
            // Pushed-down filters must actually narrow: at most the matches plus the
            // rows whose loosely typed fields also qualify.
            assert!(got.len() < current.len() || want.len() == current.len(), "{p:?}: no narrowing, got {got:?}");
        }
    }
    assert!(pushed >= 20, "only {pushed} predicates were pushed down");

    // Exact results where types are clean.
    let ids = |v: Vec<adb_store::StoredRow>| v.into_iter().map(|r| r.key().to_string()).collect::<std::collections::BTreeSet<_>>();
    let summer = predicate_filter(Memory::Episodic, &Predicate::where_eq("campaign", "summer"));
    assert_eq!(ids(store.current_expr(summer, now).await.unwrap()), ["p3", "p8"].map(String::from).into());
    let n7 = predicate_filter(Memory::Episodic, &Predicate::where_eq("n", 7_i64));
    // p4 (7.0) and p6 ("7") are the loose extras; the backend drops them.
    assert_eq!(ids(store.current_expr(n7, now).await.unwrap()), ["p4", "p6"].map(String::from).into());
}

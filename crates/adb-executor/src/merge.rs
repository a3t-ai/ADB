//! Three-way diff of a branch against its parent (DB-10, `.ai/MERGE.md`): what the
//! branch changed since its point, what main changed since, and where both changed the
//! same record (a conflict). Pure over rows, so the rules are tested without an engine.
//!
//! - **base**: the parent as of the branch point;
//! - **main**: the parent now;
//! - **view**: the branch as it reads now (its own rows over base).
//!
//! A branch change is a lineage whose row in the view isn't base's (changed or added),
//! or a base lineage missing from the view (forgotten on the branch). FORGET on main
//! erases a record from base, main and every branch, so it never shows here.

use std::collections::HashMap;

use adb_core::MemoryType;
use adb_store::StoredRow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A clean branch change: applies without asking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub memory: MemoryType,
    pub lineage: String,
    pub key: Option<String>,
    /// `add`, `change` or `forget`.
    pub change: String,
    /// The branch's value (null for `forget`).
    pub branch: Value,
}

/// Both sides changed one record since the point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conflict {
    pub memory: MemoryType,
    pub lineage: String,
    pub key: Option<String>,
    /// `edit_edit`, `add_add` or `edit_forget`.
    #[serde(rename = "type")]
    pub kind: String,
    pub base: Value,
    pub main: Value,
    pub branch: Value,
}

/// How a conflict (or a clean change) is settled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resolution {
    pub lineage: String,
    pub memory: MemoryType,
    /// `branch`, `main` or `edit`.
    pub take: String,
    #[serde(default)]
    pub value: Option<Value>,
}

/// A record's payload as the diff compares it (the branch's delete marker aside).
pub fn payload(r: &StoredRow) -> Value {
    let mut v = r.data.value.clone();
    if let Some(m) = v.as_object_mut() {
        m.remove("_branch_deleted");
    }
    v
}

fn key_of(r: &StoredRow) -> Option<String> {
    r.data.key.clone().or_else(|| r.data.value.get("key").and_then(|k| k.as_str()).map(String::from))
}

/// The diff of one memory type. Rows are current rows (one per lineage).
pub fn diff(memory: MemoryType, base: &[StoredRow], main: &[StoredRow], view: &[StoredRow]) -> (Vec<Change>, Vec<Conflict>) {
    let by = |rows: &[StoredRow]| rows.iter().map(|r| (r.lineage_id.clone(), r.clone())).collect::<HashMap<_, _>>();
    let (base, main, view) = (by(base), by(main), by(view));
    let mut lineages: Vec<&String> = base.keys().chain(view.keys()).collect();
    lineages.sort();
    lineages.dedup();
    let (mut clean, mut conflicts) = (Vec::new(), Vec::new());
    for l in lineages {
        let (b, m, v) = (base.get(l), main.get(l), view.get(l));
        // What the branch did.
        let branch = match (b, v) {
            (Some(b), Some(v)) if b.id == v.id => continue, // untouched
            (None, None) => continue,
            (Some(_), None) => None,     // forgotten on the branch
            (_, Some(v)) => Some(v),      // added or changed
        };
        let key = v.or(b).and_then(key_of);
        let main_changed = match (b, m) {
            (Some(b), Some(m)) => b.id != m.id,
            (Some(_), None) => continue, // erased on main (FORGET): already gone everywhere
            (None, Some(_)) => true,     // main added the same key
            (None, None) => false,
        };
        let change = match (b, branch) {
            (None, Some(_)) => "add",
            (Some(_), Some(_)) => "change",
            (_, None) => "forget",
        };
        if !main_changed {
            clean.push(Change { memory, lineage: l.clone(), key, change: change.into(), branch: branch.map(payload).unwrap_or(Value::Null) });
            continue;
        }
        let m = m.expect("main changed means a main row");
        let kind = match (b, branch) {
            (_, Some(v)) if payload(v) == payload(m) => {
                continue; // both sides made the same change
            }
            (None, Some(_)) => "add_add",
            (Some(_), Some(_)) => "edit_edit",
            (_, None) => "edit_forget",
        };
        conflicts.push(Conflict {
            memory,
            lineage: l.clone(),
            key,
            kind: kind.into(),
            base: b.map(payload).unwrap_or(Value::Null),
            main: payload(m),
            branch: branch.map(payload).unwrap_or(Value::Null),
        });
    }
    (clean, conflicts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_core::log::{CommitTs, Kind, Memory, RowData, Visibility};
    use serde_json::json;

    fn row(lineage: &str, id: &str, value: Value) -> StoredRow {
        StoredRow {
            id: id.into(),
            lineage_id: lineage.into(),
            data: RowData {
                tenant_id: "t".into(),
                scope: "s".into(),
                visibility: Visibility::Private,
                namespace: Some("crm".into()),
                memory: Memory::Procedural,
                kind: Kind::Entity,
                key: value.get("key").and_then(|k| k.as_str()).map(String::from),
                subject_id: None,
                predicate: None,
                object_id: None,
                link_type: None,
                weight: None,
                text: None,
                value,
                embedding: None,
                ttl_at: None,
                sealed: None,
                provenance: Default::default(),
            },
            commit_ts: CommitTs(1),
            valid_from: CommitTs(1),
            valid_to: None,
            superseded_by: None,
            version: 1,
            tombstone: false,
            log_offset: 1,
            epoch: 1,
        }
    }

    #[test]
    fn the_conflict_rules() {
        let p = |k: &str, v: &str| json!({ "key": k, "steps": v });
        let base = vec![
            row("crm|~a", "a1", p("a", "1")), // branch changes, main not → clean change
            row("crm|~b", "b1", p("b", "1")), // both change differently → edit_edit
            row("crm|~c", "c1", p("c", "1")), // both change the same → nothing
            row("crm|~d", "d1", p("d", "1")), // branch forgets, main not → clean forget
            row("crm|~e", "e1", p("e", "1")), // branch forgets, main changes → edit_forget
            row("crm|~f", "f1", p("f", "1")), // main changes only → nothing to merge
            row("crm|~g", "g1", p("g", "1")), // erased on main (FORGET) → nothing
        ];
        let main = vec![
            row("crm|~a", "a1", p("a", "1")),
            row("crm|~b", "b2", p("b", "main")),
            row("crm|~c", "c2", p("c", "same")),
            row("crm|~d", "d1", p("d", "1")),
            row("crm|~e", "e2", p("e", "main")),
            row("crm|~f", "f2", p("f", "main")),
            row("crm|~h", "h2", p("h", "main")), // added on both, differently → add_add
            row("crm|~i", "i2", p("i", "x")),    // added on both, the same → nothing
        ];
        let view = vec![
            row("crm|~a", "a9", p("a", "branch")),
            row("crm|~b", "b9", p("b", "branch")),
            row("crm|~c", "c9", p("c", "same")),
            row("crm|~f", "f1", p("f", "1")),
            row("crm|~h", "h9", p("h", "branch")),
            row("crm|~i", "i9", p("i", "x")),
            row("crm|~j", "j9", p("j", "new")), // added on the branch only → clean add
        ];
        let (clean, conflicts) = diff(MemoryType::Procedural, &base, &main, &view);
        let c: Vec<(String, String)> = clean.iter().map(|c| (c.lineage.clone(), c.change.clone())).collect();
        assert_eq!(c, [("crm|~a".into(), "change".into()), ("crm|~d".into(), "forget".into()), ("crm|~j".into(), "add".into())]);
        let k: Vec<(String, String)> = conflicts.iter().map(|c| (c.lineage.clone(), c.kind.clone())).collect();
        assert_eq!(k, [("crm|~b".into(), "edit_edit".into()), ("crm|~e".into(), "edit_forget".into()), ("crm|~h".into(), "add_add".into())]);
        let b = &conflicts[0];
        assert_eq!((b.base["steps"].clone(), b.main["steps"].clone(), b.branch["steps"].clone()), (json!("1"), json!("main"), json!("branch")));
        assert_eq!(b.key.as_deref(), Some("b"));
        assert_eq!(conflicts[1].branch, Value::Null);
        assert_eq!(conflicts[2].base, Value::Null);
    }
}

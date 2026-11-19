//! Branch review, the API beside AQL (user, 18 Nov): what A3T's merge requests use —
//! list a namespace's branches, the three-way diff (clean changes and conflicts), merge
//! with a choice per record, rebase, revert a merge, promote a branch into a new
//! namespace. Branch `name` of namespace `ns` is the namespace `ns@name`
//! (`.ai/BRANCH_NAMESPACES.md`); everything runs over [`NsOps`], on one engine or a
//! cluster's router. AQL's MERGE is the all-or-nothing form of the same merge.

use std::collections::HashMap;

use adb_core::log::{CommitTs, Memory};
use adb_core::MemoryType;
use adb_store::StoredRow;
use serde_json::{json, Value};

use crate::branches::{Base, BaseKind, Branch, MAIN};
use crate::merge::{Conflict, Change, Resolution};
use crate::namespaces::{tag_name, NsOps};
use crate::writes::MergeWrite;

/// The namespace of branch `name` of `ns` (`main`: `ns` itself).
pub fn branch_ns(ns: &str, name: &str) -> String {
    match name {
        "" | MAIN => ns.to_string(),
        n => format!("{ns}@{n}"),
    }
}

fn now() -> CommitTs {
    CommitTs::from_datetime(adb_core::time::now())
}

fn bare(id: &str) -> String {
    adb_store::without_namespace(adb_store::key_of(id)).to_string()
}

/// Rows keyed by their key without namespace, with that key as lineage (and as id for
/// a first version, whose id is its lineage) so the two sides line up for the diff.
fn by_key(rows: Vec<StoredRow>) -> (Vec<StoredRow>, HashMap<String, StoredRow>) {
    let mut normal = Vec::with_capacity(rows.len());
    let mut original = HashMap::new();
    for r in rows {
        let k = bare(&r.lineage_id);
        original.insert(k.clone(), r.clone());
        let mut n = r;
        if bare(&n.id) == k {
            n.id = k.clone();
        }
        n.lineage_id = k;
        normal.push(n);
    }
    (normal, original)
}

fn branch_row(ns: &str, name: &str, parent: &str, b: &Base) -> Branch {
    Branch {
        id: Branch::id_of(ns, name),
        namespace: ns.to_string(),
        name: name.to_string(),
        parent: parent.to_string(),
        point_offset: b.point_offset,
        point_ts: b.point_ts,
        created_by: None,
    }
}

/// The branches of `ns` (and theirs), from the tags that hold their points.
pub async fn list(ops: &dyn NsOps, ns: &str) -> Result<Vec<Branch>, String> {
    let mut out = Vec::new();
    let mut todo = vec![(ns.to_string(), MAIN.to_string())];
    while let Some((space, parent)) = todo.pop() {
        for t in ops.tags(&space).await? {
            let Some(child) = t.name.strip_prefix("branch.") else { continue };
            let Some(name) = child.strip_prefix(&format!("{ns}@")) else { continue };
            if let Some(b) = ops.base_of(child).await?.filter(|b| b.kind == BaseKind::Branch) {
                out.push(branch_row(ns, name, &parent, &b));
                if out.len() < 1000 {
                    todo.push((child.to_string(), name.to_string()));
                }
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Branch `name` of `ns`, from main or from branch `parent`.
pub async fn create(ops: &dyn NsOps, ns: &str, name: &str, parent: Option<&str>) -> Result<Branch, String> {
    crate::branches::check_name(name)?;
    let parent = parent.filter(|p| !p.is_empty()).unwrap_or(MAIN);
    if ops.base_of(&branch_ns(ns, name)).await?.is_some() || ops.exists(&branch_ns(ns, name)).await? {
        return Err(format!("namespace {ns:?} already has a branch {name:?}"));
    }
    if parent != MAIN && ops.base_of(&branch_ns(ns, parent)).await?.is_none() {
        return Err(format!("no branch {parent:?} in namespace {ns:?} to branch from"));
    }
    crate::namespaces::cut(ops, &branch_ns(ns, parent), &branch_ns(ns, name), None, BaseKind::Branch).await?;
    let b = ops.base_of(&branch_ns(ns, name)).await?.ok_or("the branch has no base")?;
    Ok(branch_row(ns, name, parent, &b))
}

/// The three states of a branch's review: its parent at the point, now, and the branch.
pub struct ThreeWay {
    pub parent: String,
    pub base: Base,
    pub main_offset: u64,
    pub main_ts: CommitTs,
    pub clean: Vec<Change>,
    pub conflicts: Vec<Conflict>,
    /// The parent's and the branch's current rows, by key (as stored).
    pub main: HashMap<String, StoredRow>,
    pub view: HashMap<String, StoredRow>,
}

pub async fn three_way(ops: &dyn NsOps, ns: &str, name: &str, memories: &[MemoryType]) -> Result<ThreeWay, String> {
    let child = branch_ns(ns, name);
    let base = ops.base_of(&child).await?.filter(|b| b.kind == BaseKind::Branch).ok_or_else(|| format!("branch_not_found: no branch {name:?} in namespace {ns:?}"))?;
    let t = now();
    let (mut clean, mut conflicts, mut main_all, mut view_all) = (Vec::new(), Vec::new(), HashMap::new(), HashMap::new());
    let mut main_offset = 0;
    for m in memories {
        let mem: Memory = (*m).into();
        let (then, _) = by_key(ops.rows_at(mem, &base.from, base.point()).await?);
        let (main, main_orig) = by_key(ops.rows_at(mem, &base.from, t).await?);
        let (view, view_orig) = by_key(ops.rows_at(mem, &child, t).await?);
        main_offset = main_offset.max(main.iter().map(|r| r.log_offset).max().unwrap_or(0));
        let (c, x) = crate::merge::diff(*m, &then, &main, &view);
        clean.extend(c);
        conflicts.extend(x);
        main_all.extend(main_orig);
        view_all.extend(view_orig);
    }
    Ok(ThreeWay { parent: base.from.clone(), base, main_offset, main_ts: t, clean, conflicts, main: main_all, view: view_all })
}

pub async fn conflicts(ops: &dyn NsOps, ns: &str, name: &str, memories: &[MemoryType]) -> Result<Value, String> {
    let all = [MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools];
    let w = three_way(ops, ns, name, if memories.is_empty() { &all } else { memories }).await?;
    Ok(json!({
        "base": { "offset": 0, "ts": w.base.point_ts },
        "main": { "offset": w.main_offset, "ts": w.main_ts.0 },
        "clean": w.clean,
        "conflicts": w.conflicts,
    }))
}

/// `template` with `value` as its payload (its text from `content`; embedded again).
fn with_value(template: &StoredRow, value: Value) -> StoredRow {
    let mut r = template.clone();
    r.data.text = value.get("content").and_then(|c| c.as_str()).map(String::from);
    r.data.value = value;
    r.data.embedding = None;
    r
}

fn resolutions_for<'a>(w: &ThreeWay, resolutions: &'a [Resolution]) -> Result<HashMap<&'a str, &'a Resolution>, String> {
    let res: HashMap<&str, &Resolution> = resolutions.iter().map(|r| (r.lineage.as_str(), r)).collect();
    let open: Vec<&str> = w.conflicts.iter().filter(|c| !res.contains_key(c.lineage.as_str())).map(|c| c.lineage.as_str()).collect();
    if !open.is_empty() {
        return Err(format!("merge_conflicts: {} conflicts have no resolution: {}", open.len(), open.join(", ")));
    }
    Ok(res)
}

fn stale(w: &ThreeWay, lineage: &str, main_offset: u64) -> Result<(), String> {
    match w.main.get(lineage) {
        Some(m) if m.log_offset > main_offset => Err(format!("merge_stale: main changed {lineage} at offset {} after {main_offset}: read the conflicts again", m.log_offset)),
        _ => Ok(()),
    }
}

/// Merge branch `name` into its parent (kind 2): its clean changes and each conflict as
/// resolved (`branch`, `main` or `edit`), in one write, recorded for [`revert`].
pub async fn merge(ops: &dyn NsOps, ns: &str, name: &str, memories: &[MemoryType], main_offset: u64, resolutions: Vec<Resolution>, by: Option<&str>) -> Result<Value, String> {
    let w = three_way(ops, ns, name, if memories.is_empty() { &[MemoryType::Procedural] } else { memories }).await?;
    let res = resolutions_for(&w, &resolutions)?;
    let settled = w
        .clean
        .iter()
        .map(|c| (c.memory, c.lineage.clone(), (c.change != "forget").then(|| c.branch.clone())))
        .chain(w.conflicts.iter().map(|c| (c.memory, c.lineage.clone(), (!c.branch.is_null()).then(|| c.branch.clone()))));
    let (mut writes, mut entries) = (Vec::new(), Vec::new());
    let (mut changed, mut added, mut ended) = (0, 0, 0);
    for (memory, lineage, branch_value) in settled {
        let value = match res.get(lineage.as_str()) {
            Some(r) if r.take == "main" => continue,
            Some(r) if r.take == "edit" => Some(r.value.clone().ok_or_else(|| format!("resolution for {lineage}: an edit needs a value"))?),
            Some(r) if r.take != "branch" => return Err(format!("resolution for {lineage}: take is branch, main or edit, not {:?}", r.take)),
            _ => branch_value,
        };
        stale(&w, &lineage, main_offset)?;
        let current = w.main.get(&lineage).cloned();
        let template = w.view.get(&lineage).or(current.as_ref()).cloned();
        let source = match (&value, template) {
            (Some(v), Some(t)) => Some(with_value(&t, v.clone())),
            (Some(_), None) => return Err(format!("resolution for {lineage}: no record to write")),
            (None, _) => None,
        };
        match (&current, &source) {
            (Some(_), Some(_)) => changed += 1,
            (None, Some(_)) => added += 1,
            (Some(_), None) => ended += 1,
            (None, None) => continue,
        }
        entries.push(json!({ "memory": memory, "key": lineage, "before": current, "after": value }));
        writes.push(MergeWrite { memory: memory.into(), key: lineage, current, source });
    }
    ops.merge_apply(&w.parent, writes).await?;
    let merge_id = format!("{name}-{}", adb_core::time::now().timestamp_micros());
    let record = json!({ "kind": "merge", "branch": name, "by": by, "at": adb_core::time::now().to_rfc3339(), "entries": entries });
    ops.record_put(&w.parent, &format!("merge:{}/{merge_id}", w.parent), record).await?;
    Ok(json!({ "merge_id": merge_id, "offset": w.main_offset, "changed": changed, "added": added, "ended": ended }))
}

/// Move branch `name` to its parent now (kind 1): each conflict as resolved — `branch`
/// keeps the branch's record, `main` drops it (the parent's shows through), `edit`
/// writes the value on the branch — then the point moves.
pub async fn rebase(ops: &dyn NsOps, ns: &str, name: &str, main_offset: u64, resolutions: Vec<Resolution>) -> Result<Value, String> {
    let w = three_way(ops, ns, name, &[MemoryType::Semantic, MemoryType::Procedural, MemoryType::Tools]).await?;
    let res = resolutions_for(&w, &resolutions)?;
    let child = branch_ns(ns, name);
    let (mut kept, mut dropped, mut edited) = (0, 0, 0);
    let mut writes = Vec::new();
    for c in &w.conflicts {
        stale(&w, &c.lineage, main_offset)?;
        let own = w.view.get(&c.lineage).filter(|r| r.data.namespace.as_deref() == Some(child.as_str())).cloned();
        match res[c.lineage.as_str()].take.as_str() {
            "branch" => kept += 1,
            "main" => {
                writes.push(MergeWrite { memory: c.memory.into(), key: c.lineage.clone(), current: own, source: None });
                dropped += 1;
            }
            "edit" => {
                let r = res[c.lineage.as_str()];
                let value = r.value.clone().ok_or_else(|| format!("resolution for {}: an edit needs a value", c.lineage))?;
                let template = w.main.get(&c.lineage).or(w.view.get(&c.lineage)).ok_or_else(|| format!("resolution for {}: no row to edit", c.lineage))?;
                writes.push(MergeWrite { memory: c.memory.into(), key: c.lineage.clone(), current: own, source: Some(with_value(template, value)) });
                edited += 1;
            }
            other => return Err(format!("resolution for {}: take is branch, main or edit, not {other:?}", c.lineage)),
        }
    }
    ops.merge_apply(&child, writes).await?;
    let tag = ops.tag_drop(&w.base.from, &w.base.tag).await.and(ops.tag_at(&w.base.from, &w.base.tag, None).await)?;
    ops.base_set(&child, Some(Base { point_ts: tag.point_ts, point_offset: tag.point_offset, ..w.base.clone() })).await?;
    Ok(json!({ "point": { "offset": 0, "ts": tag.point_ts }, "kept": kept, "dropped": dropped, "edited": edited }))
}

/// Undo merge `merge_id` of `ns`: each record it wrote back as it was, unless it changed
/// since (then refused, unless `force_skip` skips those).
pub async fn revert(ops: &dyn NsOps, ns: &str, merge_id: &str, by: Option<&str>, force_skip: bool) -> Result<Value, String> {
    let id = format!("merge:{ns}/{merge_id}");
    let merge = ops.records(ns, &id).await?.into_iter().find(|r| r["id"] == json!(id)).ok_or_else(|| format!("merge_not_found: no merge {merge_id:?} in namespace {ns:?}"))?;
    let revert_id = format!("revert:{ns}/{merge_id}");
    if !ops.records(ns, &revert_id).await?.is_empty() {
        return Err(format!("merge_not_found: merge {merge_id:?} was already reverted"));
    }
    let t = now();
    let mut current: HashMap<(Memory, String), StoredRow> = HashMap::new();
    let (mut writes, mut moved) = (Vec::new(), Vec::new());
    for e in merge["entries"].as_array().cloned().unwrap_or_default() {
        let memory: MemoryType = serde_json::from_value(e["memory"].clone()).map_err(|x| x.to_string())?;
        let mem: Memory = memory.into();
        if !current.keys().any(|(m, _)| *m == mem) {
            for r in ops.rows_at(mem, ns, t).await? {
                current.insert((mem, bare(&r.lineage_id)), r);
            }
        }
        let key = e["key"].as_str().unwrap_or_default().to_string();
        let cur = current.get(&(mem, key.clone())).cloned();
        let after = if e["after"].is_null() { None } else { Some(e["after"].clone()) };
        if cur.as_ref().map(|r| &r.data.value) != after.as_ref() {
            moved.push(key);
            continue;
        }
        let before: Option<StoredRow> = serde_json::from_value(e["before"].clone()).ok();
        writes.push(MergeWrite { memory: mem, key, current: cur, source: before });
    }
    if !moved.is_empty() && !force_skip {
        return Err(format!("merge_moved: {} record(s) changed since the merge: {} (revert with force_skip to skip them)", moved.len(), moved.join(", ")));
    }
    let reverted = writes.len();
    ops.merge_apply(ns, writes).await?;
    ops.record_put(ns, &revert_id, json!({ "kind": "revert", "of": merge_id, "by": by, "at": adb_core::time::now().to_rfc3339() })).await?;
    Ok(json!({ "reverted": reverted, "skipped": moved, "offset": 0 }))
}

/// Copy branch `name` of `ns` (as it reads now) into the new namespace `into`.
pub async fn promote(ops: &dyn NsOps, ns: &str, name: &str, into: &str) -> Result<Value, String> {
    if into == ns {
        return Err(format!("promote into a new namespace, not {into:?} itself"));
    }
    let child = branch_ns(ns, name);
    if ops.base_of(&child).await?.is_none() {
        return Err(format!("branch_not_found: no branch {name:?} in namespace {ns:?}"));
    }
    if ops.exists(into).await? {
        return Err(format!("target_not_empty: namespace {into:?} exists"));
    }
    let t = now();
    let mut rows = Vec::new();
    for m in [Memory::Semantic, Memory::Episodic, Memory::Procedural, Memory::Tools] {
        rows.extend(ops.rows_at(m, &child, t).await?);
    }
    let n = ops.materialise(into, rows).await?;
    Ok(json!({ "namespace": into, "records": n, "from": { "namespace": ns, "branch": name } }))
}

/// The tag that holds a branch's point (for callers building their own).
pub fn branch_tag(ns: &str, name: &str) -> String {
    tag_name(&branch_ns(ns, name), BaseKind::Branch)
}

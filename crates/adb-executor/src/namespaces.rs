//! AQL v0.13 BRANCH / FORK / MERGE and dropping a namespace (`.ai/BRANCH_NAMESPACES.md`),
//! over [`NsOps`]: one engine does them locally; a cluster's router sends each to the
//! partition that holds the namespace concerned.
//!
//! - BRANCH: a base row on the new namespace (`base:<ns>`, kind branch) and, on the
//!   source, a tag that holds the point (`branch.<ns>`; compaction keeps what it reads).
//! - FORK: the source's records at the point copied into the new namespace, and a base
//!   row of kind fork (for MERGE) with its tag (`fork.<ns>`).
//! - MERGE: both sides against their common base at the point (see [`merge`]).
//! - Dropping a namespace (`FORGET FROM ALL NAMESPACE`): refused while it has branches
//!   (its `branch.*` tags); a branch or fork being dropped leaves its base.

use async_trait::async_trait;
use serde_json::{json, Value};

use adb_core::log::{CommitTs, Memory};
use adb_store::StoredRow;

use crate::branches::{Base, BaseKind, Tag};
use crate::writes::MergeWrite;

const MEMORIES: [Memory; 5] = [Memory::Semantic, Memory::Episodic, Memory::Procedural, Memory::Tools, Memory::Working];

/// What BRANCH / FORK / MERGE need of the namespaces they touch.
#[async_trait]
pub trait NsOps: Send + Sync {
    /// Records of any memory type, or a base.
    async fn exists(&self, namespace: &str) -> Result<bool, String>;
    /// A tag on `namespace` at `at` (now when `None`).
    async fn tag_at(&self, namespace: &str, name: &str, at: Option<CommitTs>) -> Result<Tag, String>;
    async fn tag_drop(&self, namespace: &str, name: &str) -> Result<bool, String>;
    async fn tags(&self, namespace: &str) -> Result<Vec<Tag>, String>;
    async fn base_of(&self, namespace: &str) -> Result<Option<Base>, String>;
    /// Record `base` on its namespace, or drop `namespace`'s.
    async fn base_set(&self, namespace: &str, base: Option<Base>) -> Result<(), String>;
    /// Copy `rows` into the new namespace `namespace`.
    async fn materialise(&self, namespace: &str, rows: Vec<StoredRow>) -> Result<u64, String>;
    async fn merge_apply(&self, namespace: &str, writes: Vec<MergeWrite>) -> Result<u64, String>;
    /// `namespace` as it was at `at` (a branch through its bases).
    async fn rows_at(&self, memory: Memory, namespace: &str, at: CommitTs) -> Result<Vec<StoredRow>, String>;
}

/// The tag on a namespace's source that holds its point.
pub fn tag_name(namespace: &str, kind: BaseKind) -> String {
    let safe: String = namespace.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).take(56).collect();
    match kind {
        BaseKind::Branch => format!("branch.{safe}"),
        BaseKind::Fork => format!("fork.{safe}"),
    }
}

fn now() -> CommitTs {
    CommitTs::from_datetime(adb_core::time::now())
}

/// BRANCH (or FORK, `kind`) FROM `from` INTO `into` at `at` (now when `None`).
pub async fn branch(ops: &dyn NsOps, from: &str, into: &str, at: Option<CommitTs>, kind: BaseKind) -> Result<Value, String> {
    if !adb_store::valid_namespace(into) || into.contains(adb_cluster::routing::BRANCH_SEP) {
        return Err(format!("namespace {into:?}: can't be empty or contain '|', '~' or '@'"));
    }
    if !ops.exists(from).await? {
        return Err(format!("no namespace {from:?} to branch or fork from"));
    }
    if ops.exists(into).await? {
        return Err(format!("namespace {into:?} exists: BRANCH and FORK make a new one"));
    }
    let tag = ops.tag_at(from, &tag_name(into, kind), at).await?;
    let point = CommitTs(tag.point_ts);
    let mut answer = json!({ "namespace": into, "from": from, "point": { "ts": tag.point_ts, "at": point.to_datetime().to_rfc3339() } });
    if kind == BaseKind::Fork {
        let mut rows = Vec::new();
        for m in MEMORIES {
            rows.extend(ops.rows_at(m, from, point).await?);
        }
        answer["records"] = json!(ops.materialise(into, rows).await?);
    }
    let base = Base { id: Base::id_of(into), namespace: into.into(), from: from.into(), kind, tag: tag.name, point_ts: tag.point_ts };
    ops.base_set(into, Some(base)).await?;
    Ok(answer)
}

/// MERGE FROM `from` INTO `into` (AQL v0.13 §5). The two parted at a point: one is the
/// other's base. Per key, a side *changed* it when its record now differs from the
/// base's at that point. `from`'s changes reach `into` in one write; a key both changed
/// differently is a conflict — refused (listed) unless `prefer_source` says which side
/// wins. Into a branch from its base the branch's point moves (the base's changes show
/// through), and where the base wins a conflict the branch's own record goes.
/// Afterwards the two have parted now.
pub async fn merge(ops: &dyn NsOps, from: &str, into: &str, prefer_source: Option<bool>) -> Result<Value, String> {
    let (child, base, rebase) = match (ops.base_of(from).await?, ops.base_of(into).await?) {
        (Some(b), _) if b.from == into => (b, into, false),
        (_, Some(b)) if b.from == from => (b, from, true),
        _ => return Err(format!("MERGE: {from:?} and {into:?} have no common base (one must be branched or forked from the other)")),
    };
    let point = child.point();
    let t = now();
    let bare = |r: &StoredRow| adb_store::without_namespace(adb_store::key_of(&r.lineage_id)).to_string();
    let same = |a: Option<&StoredRow>, b: Option<&StoredRow>| match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.data.value == b.data.value && a.data.link_type == b.data.link_type && a.data.weight == b.data.weight,
        _ => false,
    };
    let mut writes = Vec::new();
    let mut conflicts = Vec::new();
    for m in MEMORIES {
        let by_key = |rows: Vec<StoredRow>| rows.into_iter().map(|r| (bare(&r), r)).collect::<std::collections::BTreeMap<_, _>>();
        let then = by_key(ops.rows_at(m, base, point).await?);
        let src = by_key(ops.rows_at(m, from, t).await?);
        let dst = by_key(ops.rows_at(m, into, t).await?);
        let keys: std::collections::BTreeSet<&String> = then.keys().chain(src.keys()).chain(dst.keys()).collect();
        for k in keys {
            let (was, s, d) = (then.get(k), src.get(k), dst.get(k));
            if same(s, was) {
                continue;
            }
            let conflict = !same(d, was) && !same(s, d);
            if conflict {
                match prefer_source {
                    None => {
                        conflicts.push(format!("{m:?} {k}"));
                        continue;
                    }
                    Some(false) => continue,
                    Some(true) => {}
                }
            }
            // The target's own record of the key (a branch's inherited ones aren't).
            let own = d.filter(|r| r.data.namespace.as_deref() == Some(into)).cloned();
            if rebase && child.kind == BaseKind::Branch {
                // Into a branch from its base: the base shows through once the point
                // moves; a conflict the base wins drops the branch's own record.
                if conflict && own.is_some() {
                    writes.push(MergeWrite { memory: m, key: k.clone(), current: own, source: None });
                }
                continue;
            }
            if s.is_some() || own.is_some() {
                writes.push(MergeWrite { memory: m, key: k.clone(), current: own, source: s.cloned() });
            }
        }
    }
    if !conflicts.is_empty() {
        return Err(format!("merge_conflict: {} record(s) changed on both sides: {} (MERGE … PREFER SOURCE or PREFER TARGET)", conflicts.len(), conflicts.join(", ")));
    }
    let merged = ops.merge_apply(into, writes).await?;
    // They have parted now: the child's point moves (its tag on the base too).
    ops.tag_drop(&child.from, &child.tag).await?;
    let tag = ops.tag_at(&child.from, &child.tag, None).await?;
    ops.base_set(&child.namespace, Some(Base { point_ts: tag.point_ts, ..child.clone() })).await?;
    Ok(json!({ "from": from, "into": into, "merged": merged, "point": { "ts": tag.point_ts } }))
}

/// Before `namespace` is dropped: refused while namespaces are branched from it (they
/// read through to it; forks hold their own copy); a branch or fork being dropped
/// leaves its base (its point's tag on the source goes too); its forks' tags go.
pub async fn before_drop(ops: &dyn NsOps, namespace: &str) -> Result<(), String> {
    let tags = ops.tags(namespace).await?;
    let branches: Vec<&str> = tags.iter().filter_map(|t| t.name.strip_prefix("branch.")).collect();
    if !branches.is_empty() {
        return Err(format!("namespace {namespace:?} has branches: {} (drop or merge them first)", branches.join(", ")));
    }
    if let Some(b) = ops.base_of(namespace).await? {
        ops.base_set(namespace, None).await?;
        ops.tag_drop(&b.from, &b.tag).await?;
    }
    for t in tags.iter().filter(|t| t.name.starts_with("fork.")) {
        ops.tag_drop(namespace, &t.name).await?;
    }
    Ok(())
}

/// [`NsOps`] on one engine (every namespace here, or this partition's own).
pub struct LocalNs<'a>(pub &'a crate::LocalEngine);

impl LocalNs<'_> {
    fn d(&self) -> Result<&std::sync::Arc<crate::writes::DurableWrites<crate::engine::EngineLog>>, String> {
        self.0.durable_writes().ok_or_else(|| "BRANCH / FORK / MERGE need a durable engine (start ADB with --data-dir)".to_string())
    }
    fn spaces(&self) -> Result<crate::spaces::LocalSpaces, String> {
        self.0.local_spaces().ok_or_else(|| "BRANCH / FORK / MERGE need a durable engine".to_string())
    }
}

#[async_trait]
impl NsOps for LocalNs<'_> {
    async fn exists(&self, namespace: &str) -> Result<bool, String> {
        let spaces = self.spaces()?;
        if spaces.registry.base_of(namespace).is_some() {
            return Ok(true);
        }
        let view = crate::spaces::SpaceView::new(spaces);
        let t = now();
        for m in MEMORIES {
            if view.versions(m, namespace).await.map_err(|e| e.to_string())?.iter().any(|r| r.is_current_at(t, t)) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    async fn tag_at(&self, namespace: &str, name: &str, at: Option<CommitTs>) -> Result<Tag, String> {
        self.d()?.tag_create_at(namespace, name, at).await.map_err(|e| e.to_string())
    }
    async fn tag_drop(&self, namespace: &str, name: &str) -> Result<bool, String> {
        self.d()?.tag_drop(namespace, name).await.map_err(|e| e.to_string())
    }
    async fn tags(&self, namespace: &str) -> Result<Vec<Tag>, String> {
        Ok(self.spaces()?.registry.tags(Some(namespace)))
    }
    async fn base_of(&self, namespace: &str) -> Result<Option<Base>, String> {
        Ok(self.spaces()?.registry.base_of(namespace))
    }
    async fn base_set(&self, namespace: &str, base: Option<Base>) -> Result<(), String> {
        self.d()?.base_set(namespace, base.as_ref()).await.map_err(|e| e.to_string())
    }
    async fn materialise(&self, namespace: &str, rows: Vec<StoredRow>) -> Result<u64, String> {
        self.d()?.materialise(namespace, rows).await.map_err(|e| e.to_string())
    }
    async fn merge_apply(&self, namespace: &str, writes: Vec<MergeWrite>) -> Result<u64, String> {
        self.d()?.merge_apply(namespace, writes).await.map_err(|e| e.to_string())
    }
    async fn rows_at(&self, memory: Memory, namespace: &str, at: CommitTs) -> Result<Vec<StoredRow>, String> {
        // Bases in other partitions are read through this engine's spaces (the router's
        // on a cluster node).
        self.d()?.space_view().rows_at(memory, namespace, at, now()).await.map_err(|e| e.to_string())
    }
}

/// A partition's half of [`NsOps`] for a router (remote ops `ns_*`).
pub async fn dispatch(e: &crate::LocalEngine, op: &str, a: &Value) -> Option<Result<Value, String>> {
    let ns = LocalNs(e);
    let s = |k: &str| a.get(k).and_then(|v| v.as_str()).map(String::from).ok_or_else(|| format!("argument {k}"));
    fn arg<T: serde::de::DeserializeOwned>(a: &Value, k: &str) -> Result<T, String> {
        serde_json::from_value(a.get(k).cloned().unwrap_or(Value::Null)).map_err(|x| format!("argument {k}: {x}"))
    }
    let val = |v: Result<Value, String>| v;
    Some(match op {
        "ns_exists" => match s("namespace") {
            Ok(n) => ns.exists(&n).await.map(Value::Bool),
            Err(x) => Err(x),
        },
        "ns_tag_at" => match (s("namespace"), s("name"), arg(a, "at")) {
            (Ok(n), Ok(name), Ok(at)) => ns.tag_at(&n, &name, at).await.and_then(|t| serde_json::to_value(t).map_err(|e| e.to_string())),
            (a, b, c) => Err(a.err().or(b.err()).or(c.err()).unwrap_or_default()),
        },
        "ns_tag_drop" => match (s("namespace"), s("name")) {
            (Ok(n), Ok(name)) => ns.tag_drop(&n, &name).await.map(Value::Bool),
            (a, b) => Err(a.err().or(b.err()).unwrap_or_default()),
        },
        "ns_tags" => match s("namespace") {
            Ok(n) => ns.tags(&n).await.and_then(|t| serde_json::to_value(t).map_err(|e| e.to_string())),
            Err(x) => Err(x),
        },
        "ns_base_set" => match (s("namespace"), arg(a, "base")) {
            (Ok(n), Ok(b)) => ns.base_set(&n, b).await.map(|_| Value::Null),
            (a, b) => Err(a.err().or(b.err()).unwrap_or_default()),
        },
        "ns_materialise" => match (s("namespace"), arg(a, "rows")) {
            (Ok(n), Ok(rows)) => ns.materialise(&n, rows).await.map(|n| json!(n)),
            (a, b) => Err(a.err().or(b.err()).unwrap_or_default()),
        },
        "ns_merge_apply" => match (s("namespace"), arg(a, "writes")) {
            (Ok(n), Ok(w)) => ns.merge_apply(&n, w).await.map(|n| json!(n)),
            (a, b) => Err(a.err().or(b.err()).unwrap_or_default()),
        },
        _ => return None,
    })
    .map(val)
}

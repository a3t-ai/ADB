//! Memory branches (agents as repos, `.ai/BRANCHING.md` B1): which branches exist.
//!
//! A branch is a `branch` control row (`Effect::Register`, like subscriptions and
//! watches): its memory space (a namespace), its parent (`main` or another branch of
//! the same space) and its branch point — the log offset and commit time it was cut
//! at. Creating one copies nothing; reads on it (B2) see the parent as of the point
//! plus the branch's own writes (B3). Dropping one is an `Unregister`.

use std::collections::BTreeMap;

use adb_core::log::{CommitTs, Effect, Kind, LogRecord};
use adb_store::PartitionStore;
use std::sync::RwLock;
use serde::{Deserialize, Serialize};

/// The implicit branch every space has.
pub const MAIN: &str = "main";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Branch {
    /// `br:<namespace>/<name>`.
    pub id: String,
    pub namespace: String,
    pub name: String,
    /// `main` or the name of another branch of the same namespace.
    pub parent: String,
    /// The last log offset the branch sees from its parent.
    pub point_offset: u64,
    /// Commit time of that record: the parent is read `AS OF` it.
    pub point_ts: i64,
    #[serde(default)]
    pub created_by: Option<String>,
}

impl Branch {
    pub fn id_of(namespace: &str, name: &str) -> String {
        format!("br:{namespace}/{name}")
    }

    pub fn point(&self) -> CommitTs {
        CommitTs(self.point_ts)
    }
}

/// A named point of a memory space (B7, spec §3.8): `tag:<namespace>/<name>`. A pinned
/// layer reads its space `AS OF` a tag; compaction keeps what a tag points at.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    pub id: String,
    pub namespace: String,
    pub name: String,
    pub point_offset: u64,
    pub point_ts: i64,
    #[serde(default)]
    pub created_by: Option<String>,
}

impl Tag {
    pub fn id_of(namespace: &str, name: &str) -> String {
        format!("tag:{namespace}/{name}")
    }
}

/// AQL v0.13 BRANCH / FORK (`.ai/BRANCH_NAMESPACES.md`): namespace `namespace` was
/// made from `from` as it was at `point_ts`. A branch reads through to it there (its own
/// records first, by key); a fork holds a copy and only remembers where it came from
/// (for MERGE). The point is held by the tag `tag` on `from`, which keeps the versions
/// it reads through compaction, wherever `from` lives. `base:<namespace>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Base {
    pub id: String,
    pub namespace: String,
    pub from: String,
    pub kind: BaseKind,
    pub tag: String,
    pub point_ts: i64,
    /// The source's log offset at the point (0 when branched AS OF a time).
    #[serde(default)]
    pub point_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseKind {
    Branch,
    Fork,
}

impl Base {
    pub fn id_of(namespace: &str) -> String {
        format!("base:{namespace}")
    }

    /// The tag on `from` that holds the point of `namespace`'s base.
    pub fn tag_of(namespace: &str) -> String {
        let safe: String = namespace.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).collect();
        format!("base.{}", safe.chars().take(58).collect::<String>())
    }

    pub fn point(&self) -> CommitTs {
        CommitTs(self.point_ts)
    }
}

/// How a layer is read (B7, spec §3.9): its space now (`mount`) or at a tag (`pin`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerMode {
    Mount,
    Pin,
    /// D1 (DB-11): the episodes of the agent this one was promoted from, read-only, as
    /// of a tag. EPISODIC only, never sealed, never written.
    History,
}

/// One inherited layer of a memory space.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Layer {
    /// The layer's namespace (another agent's memory space).
    pub space: String,
    pub mode: LayerMode,
    /// For `pin`: the tag of `space` read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// The memory types the layer carries (SEMANTIC, PROCEDURAL, TOOLS; never EPISODIC
    /// or WORKING).
    pub memories: Vec<adb_core::MemoryType>,
    /// Writes that would shadow the layer's records are refused.
    #[serde(default)]
    pub sealed: bool,
}

/// A space's layers: `layers:<namespace>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Layers {
    pub id: String,
    pub namespace: String,
    pub layers: Vec<Layer>,
}

impl Layers {
    pub fn id_of(namespace: &str) -> String {
        format!("layers:{namespace}")
    }
}

/// DB-12: one reader of a memory space: who inherits it, how, and at which tag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dependent {
    pub namespace: String,
    pub mode: LayerMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    pub sealed: bool,
    pub memories: Vec<adb_core::MemoryType>,
}

/// DB-4: a storage limit on the namespaces matching `pattern` together (an exact namespace,
/// or a prefix ending in `*`, such as an org's `acme--*`): `quota:<pattern>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quota {
    pub id: String,
    pub pattern: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
    /// DB-8: how many days of replaced versions these namespaces keep (default 90).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_days: Option<u64>,
}

impl Quota {
    pub fn id_of(pattern: &str) -> String {
        format!("quota:{pattern}")
    }

    pub fn covers(&self, namespace: &str) -> bool {
        match self.pattern.strip_suffix('*') {
            Some(prefix) => namespace.starts_with(prefix),
            None => self.pattern == namespace,
        }
    }
}

/// DB-9: a past point of a space: a tag, or an offset from a memory point (DB-7). `ts`
/// goes with the offset once the log no longer holds that record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointRef {
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub ts: Option<i64>,
}

/// Branch names: letters, digits, `-`, `_`, `.`, 1–64 characters; `main` is taken.
pub fn check_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty() && name.len() <= 64 && name.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c));
    if !ok {
        return Err(format!("branch name {name:?}: use 1–64 letters, digits, '-', '_' or '.'"));
    }
    if name == MAIN {
        return Err("\"main\" is every space's own line, not a branch".into());
    }
    Ok(())
}

/// The branches of every space, from the table's current `branch` rows and then the
/// log, as subscriptions are.
#[derive(Default)]
pub struct Branches {
    /// B7: tags and layers, control rows of kind `branch` (by id prefix).
    tags: RwLock<BTreeMap<String, Tag>>,
    layers: RwLock<BTreeMap<String, Layers>>,
    /// DB-4: storage limits.
    quotas: RwLock<BTreeMap<String, Quota>>,
    /// AQL v0.13: the namespaces made by BRANCH / FORK, by `base:<namespace>`.
    bases: RwLock<BTreeMap<String, Base>>,
}

/// What a `branch`-kind control row holds, by its id's prefix.
fn control(value: &serde_json::Value) -> Option<Control> {
    let id = value.get("id")?.as_str()?;
    if id.starts_with("tag:") {
        serde_json::from_value(value.clone()).ok().map(Control::Tag)
    } else if id.starts_with("layers:") {
        serde_json::from_value(value.clone()).ok().map(Control::Layers)
    } else if id.starts_with("quota:") {
        serde_json::from_value(value.clone()).ok().map(Control::Quota)
    } else if id.starts_with("base:") {
        serde_json::from_value(value.clone()).ok().map(Control::Base)
    } else {
        // `br:` rows of the branch engines AQL v0.13 replaced: left in the table, unread
        // (a branch is now a namespace with a `base:` row).
        None
    }
}

enum Control {
    Tag(Tag),
    Layers(Layers),
    Quota(Quota),
    Base(Base),
}

impl Branches {
    pub async fn load(store: &PartitionStore, now: CommitTs) -> Result<Self, String> {
        let rows = store.current(Some("kind = 'branch'"), now).await.map_err(|e| e.to_string())?;
        let me = Self::default();
        for r in &rows {
            me.put(control(&r.data.value));
        }
        Ok(me)
    }

    fn put(&self, c: Option<Control>) {
        match c {
            Some(Control::Tag(t)) => {
                self.tags.write().unwrap().insert(t.id.clone(), t);
            }
            Some(Control::Layers(l)) => {
                self.layers.write().unwrap().insert(l.id.clone(), l);
            }
            Some(Control::Quota(q)) => {
                self.quotas.write().unwrap().insert(q.id.clone(), q);
            }
            Some(Control::Base(b)) => {
                self.bases.write().unwrap().insert(b.id.clone(), b);
            }
            None => {}
        }
    }

    /// Follow a committed record's branch registrations (called in log order).
    pub fn apply(&self, record: &LogRecord) {
        for effect in &record.effects {
            match effect {
                Effect::Register(n) if n.data.kind == Kind::Branch => self.put(control(&n.data.value)),
                Effect::Unregister { id } => {
                    self.tags.write().unwrap().remove(&id.0);
                    self.layers.write().unwrap().remove(&id.0);
                    self.quotas.write().unwrap().remove(&id.0);
                    self.bases.write().unwrap().remove(&id.0);
                }
                _ => {}
            }
        }
    }

    /// The base of a namespace made by BRANCH or FORK.
    pub fn base_of(&self, namespace: &str) -> Option<Base> {
        self.bases.read().unwrap().get(&Base::id_of(namespace)).cloned()
    }

    /// The namespaces this engine holds that were branched or forked from `from`.
    pub fn based_on(&self, from: &str) -> Vec<Base> {
        self.bases.read().unwrap().values().filter(|b| b.from == from).cloned().collect()
    }

    /// DB-4: every quota.
    pub fn quotas(&self) -> Vec<Quota> {
        self.quotas.read().unwrap().values().cloned().collect()
    }

    pub fn tag(&self, namespace: &str, name: &str) -> Option<Tag> {
        self.tags.read().unwrap().get(&Tag::id_of(namespace, name)).cloned()
    }

    pub fn tags(&self, namespace: Option<&str>) -> Vec<Tag> {
        self.tags.read().unwrap().values().filter(|t| namespace.is_none_or(|ns| t.namespace == ns)).cloned().collect()
    }

    /// The layers of `namespace` (empty when it has none).
    pub fn layers_of(&self, namespace: &str) -> Vec<Layer> {
        self.layers.read().unwrap().get(&Layers::id_of(namespace)).map(|l| l.layers.clone()).unwrap_or_default()
    }

    /// Whether any space lists `namespace` as a layer.
    pub fn layered_by(&self, namespace: &str) -> Vec<String> {
        self.layers.read().unwrap().values().filter(|l| l.layers.iter().any(|x| x.space == namespace)).map(|l| l.namespace.clone()).collect()
    }

    /// DB-12: every space reading `space` as a layer, one row per layer.
    pub fn dependents(&self, space: &str) -> Vec<Dependent> {
        let layers = self.layers.read().unwrap();
        let mut out = Vec::new();
        for l in layers.values() {
            for x in l.layers.iter().filter(|x| x.space == space) {
                out.push(Dependent { namespace: l.namespace.clone(), mode: x.mode, tag: x.tag.clone(), sealed: x.sealed, memories: x.memories.clone() });
            }
        }
        out
    }

    /// Compaction pins (B4): per namespace, its oldest tag's point. A branch's or
    /// fork's point is a tag on its source (`branch.<ns>`), and pinned layers read tags.
    pub fn pins(&self) -> std::collections::HashMap<String, CommitTs> {
        let mut out = std::collections::HashMap::<String, CommitTs>::new();
        for t in self.tags.read().unwrap().values() {
            let e = out.entry(t.namespace.clone()).or_insert(CommitTs(t.point_ts));
            *e = (*e).min(CommitTs(t.point_ts));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(check_name("try-new-recall").is_ok());
        assert!(check_name("main").is_err());
        assert!(check_name("").is_err());
        assert!(check_name("a b").is_err());
        assert!(check_name(&"x".repeat(65)).is_err());
    }

    #[test]
    fn tags_pin_and_old_branch_rows_are_left_unread() {
        let b = Branches::default();
        // A `br:` row of the branch engines (before AQL v0.13), as the table may hold it.
        let old = serde_json::json!({
            "id": "br:crm/try", "namespace": "crm", "name": "try", "parent": "main",
            "point_offset": 10, "point_ts": 10, "placement": "http://pod:3001",
            "pending_forgets": [{ "memory": "semantic", "lineages": ["semantic:crm|a"] }],
        });
        assert!(control(&old).is_none(), "left unread");
        assert!(serde_json::from_value::<Branch>(old).is_ok(), "and still decodes as a Branch");
        b.put(control(&serde_json::json!({
            "id": Tag::id_of("compliance", "v1"), "namespace": "compliance", "name": "v1",
            "point_offset": 5, "point_ts": 5,
        })));
        b.put(control(&serde_json::json!({
            "id": Tag::id_of("compliance", "v0"), "namespace": "compliance", "name": "v0",
            "point_offset": 3, "point_ts": 3,
        })));
        assert_eq!(b.pins().get("compliance"), Some(&CommitTs(3)), "the oldest tag");
        assert!(b.pins().get("crm").is_none());
    }
}

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
    /// B6: the branch engine runs in its own ADB process at this Flight URL (`None`:
    /// hosted in the org's process, `<data dir>/branches/`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<String>,
    /// B6: FORGETs on main the branch's pod hasn't applied yet (it was unreachable,
    /// e.g. hibernating): it pulls them when it starts, before it serves.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_forgets: Vec<PendingForget>,
}

/// Lineages FORGET removed on main, for a branch to erase from its own engine (B6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingForget {
    pub memory: adb_core::MemoryType,
    pub lineages: Vec<String>,
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
    by_id: RwLock<BTreeMap<String, Branch>>,
    /// B7: tags and layers, the other control rows of kind `branch` (by id prefix).
    tags: RwLock<BTreeMap<String, Tag>>,
    layers: RwLock<BTreeMap<String, Layers>>,
}

/// What a `branch`-kind control row holds, by its id's prefix.
fn control(value: &serde_json::Value) -> Option<Control> {
    let id = value.get("id")?.as_str()?;
    if id.starts_with("tag:") {
        serde_json::from_value(value.clone()).ok().map(Control::Tag)
    } else if id.starts_with("layers:") {
        serde_json::from_value(value.clone()).ok().map(Control::Layers)
    } else {
        serde_json::from_value(value.clone()).ok().map(Control::Branch)
    }
}

enum Control {
    Branch(Branch),
    Tag(Tag),
    Layers(Layers),
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
            Some(Control::Branch(b)) => {
                self.by_id.write().unwrap().insert(b.id.clone(), b);
            }
            Some(Control::Tag(t)) => {
                self.tags.write().unwrap().insert(t.id.clone(), t);
            }
            Some(Control::Layers(l)) => {
                self.layers.write().unwrap().insert(l.id.clone(), l);
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
                    self.by_id.write().unwrap().remove(&id.0);
                    self.tags.write().unwrap().remove(&id.0);
                    self.layers.write().unwrap().remove(&id.0);
                }
                _ => {}
            }
        }
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

    pub fn get(&self, namespace: &str, name: &str) -> Option<Branch> {
        self.by_id.read().unwrap().get(&Branch::id_of(namespace, name)).cloned()
    }

    /// The branches of `namespace` (all of them when `None`), by id.
    pub fn list(&self, namespace: Option<&str>) -> Vec<Branch> {
        self.by_id.read().unwrap().values().filter(|b| namespace.is_none_or(|ns| b.namespace == ns)).cloned().collect()
    }

    /// `name` and its ancestors up to `main`, nearest first (B2 walks them).
    pub fn chain(&self, namespace: &str, name: &str) -> Result<Vec<Branch>, String> {
        let by_id = self.by_id.read().unwrap();
        let mut out = Vec::new();
        let mut cur = name.to_string();
        while cur != MAIN {
            let b = by_id.get(&Branch::id_of(namespace, &cur)).ok_or_else(|| format!("no branch {cur:?} in namespace {namespace:?}"))?;
            if out.len() > 64 {
                return Err(format!("branch {name:?}: its parent chain is longer than 64"));
            }
            out.push(b.clone());
            cur = b.parent.clone();
        }
        Ok(out)
    }

    /// Whether any branch of `namespace` has `name` as its parent.
    pub fn has_children(&self, namespace: &str, name: &str) -> bool {
        self.by_id.read().unwrap().values().any(|b| b.namespace == namespace && b.parent == name)
    }

    /// The oldest branch point of `namespace`'s branches: compaction keeps the history
    /// they read (B4).
    pub fn oldest_point(&self, namespace: &str) -> Option<CommitTs> {
        self.by_id.read().unwrap().values().filter(|b| b.namespace == namespace).map(Branch::point).min()
    }

    /// Compaction pins (B4) of the engine that is `of` — `None` for the org (whose
    /// readers are the branches cut from `main`), or a branch `(namespace, name)`
    /// (read by its own child branches): per namespace, the oldest point read.
    pub fn pins(&self, of: Option<(&str, &str)>) -> std::collections::HashMap<String, CommitTs> {
        let mut out = std::collections::HashMap::<String, CommitTs>::new();
        for b in self.by_id.read().unwrap().values() {
            let reads_it = match of {
                None => b.parent == MAIN,
                Some((ns, name)) => b.namespace == ns && b.parent == name,
            };
            if reads_it {
                let e = out.entry(b.namespace.clone()).or_insert(b.point());
                *e = (*e).min(b.point());
            }
        }
        // Tags are points of the org's spaces (pinned layers read them).
        if of.is_none() {
            for t in self.tags.read().unwrap().values() {
                let e = out.entry(t.namespace.clone()).or_insert(CommitTs(t.point_ts));
                *e = (*e).min(CommitTs(t.point_ts));
            }
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
    fn chains_walk_to_main() {
        let b = Branches::default();
        let mk = |name: &str, parent: &str, off: u64| Branch {
            id: Branch::id_of("crm", name),
            namespace: "crm".into(),
            name: name.into(),
            parent: parent.into(),
            point_offset: off,
            point_ts: off as i64,
            created_by: None,
            placement: None,
            pending_forgets: Vec::new(),
        };
        {
            let mut m = b.by_id.write().unwrap();
            for x in [mk("a", "main", 10), mk("b", "a", 20)] {
                m.insert(x.id.clone(), x);
            }
        }
        let c = b.chain("crm", "b").unwrap();
        assert_eq!(c.iter().map(|x| x.name.as_str()).collect::<Vec<_>>(), ["b", "a"]);
        assert!(b.chain("crm", "nope").is_err());
        assert!(b.has_children("crm", "a"));
        assert_eq!(b.oldest_point("crm"), Some(CommitTs(10)));
        assert_eq!(b.pins(None).get("crm"), Some(&CommitTs(10)));
        assert_eq!(b.pins(Some(("crm", "a"))).get("crm"), Some(&CommitTs(20)));
        assert!(b.pins(Some(("crm", "b"))).is_empty());
        // A tag pins its space on the org (pinned layers read it), not on a branch.
        b.tags.write().unwrap().insert(Tag::id_of("compliance", "v1"), Tag {
            id: Tag::id_of("compliance", "v1"),
            namespace: "compliance".into(),
            name: "v1".into(),
            point_offset: 5,
            point_ts: 5,
            created_by: None,
        });
        assert_eq!(b.pins(None).get("compliance"), Some(&CommitTs(5)));
        assert!(b.pins(Some(("crm", "a"))).get("compliance").is_none());
    }
}

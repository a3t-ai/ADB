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
}

impl Branches {
    pub async fn load(store: &PartitionStore, now: CommitTs) -> Result<Self, String> {
        let rows = store.current(Some("kind = 'branch'"), now).await.map_err(|e| e.to_string())?;
        let by_id = rows
            .iter()
            .filter_map(|r| serde_json::from_value::<Branch>(r.data.value.clone()).ok())
            .map(|b| (b.id.clone(), b))
            .collect();
        Ok(Self { by_id: RwLock::new(by_id) })
    }

    /// Follow a committed record's branch registrations (called in log order).
    pub fn apply(&self, record: &LogRecord) {
        let mut by_id = self.by_id.write().unwrap();
        for effect in &record.effects {
            match effect {
                Effect::Register(n) if n.data.kind == Kind::Branch => {
                    if let Ok(b) = serde_json::from_value::<Branch>(n.data.value.clone()) {
                        by_id.insert(b.id.clone(), b);
                    }
                }
                Effect::Unregister { id } => {
                    by_id.remove(&id.0);
                }
                _ => {}
            }
        }
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
    }
}

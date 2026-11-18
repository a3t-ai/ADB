//! Scoped access (DB-3): a token limited to named namespaces, read or read and write, so
//! one org's token can't touch another's memory and a read-only token can't write.
//!
//! `ADB_SCOPED_TOKENS` is a JSON list:
//! `[{"name": "acme", "token": "…", "write": ["acme--*"], "read": ["shared-*"]}]`. A pattern
//! is an exact namespace, or a prefix ending in `*`; `*` alone is every namespace. `write`
//! includes read.
//!
//! Default refuse: a scoped token opens only what can be shown to stay in its namespaces.
//! A statement must name its `NAMESPACE`; an action must name its space. Platform tables,
//! the engine store, MCP and the admin endpoints aren't open to it.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct Scope {
    pub name: String,
    pub token: String,
    #[serde(default)]
    pub read: Vec<String>,
    #[serde(default)]
    pub write: Vec<String>,
}

/// The scoped tokens from `ADB_SCOPED_TOKENS`. A malformed setting is an error at start,
/// not a silently open or silently empty list.
pub fn scopes_from_env() -> Vec<Arc<Scope>> {
    let Some(raw) = std::env::var("ADB_SCOPED_TOKENS").ok().filter(|t| !t.trim().is_empty()) else { return Vec::new() };
    let scopes: Vec<Scope> = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("ADB_SCOPED_TOKENS is not a JSON list of scoped tokens: {e}"));
    scopes.into_iter().filter(|s| !s.token.is_empty()).map(Arc::new).collect()
}

fn matches(pattern: &str, namespace: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => namespace.starts_with(prefix),
        None => pattern == namespace,
    }
}

/// How an action reaches a space.
enum Reach {
    Read(&'static str),
    Write(&'static str),
    /// Reads `namespace`, writes `into`.
    Copy,
    /// Writes `namespace`, reads each layer's space.
    Layers,
}

fn reach_of(action: &str) -> Option<Reach> {
    Some(match action {
        "branch_list" | "tag_list" | "layers_get" | "branch_conflicts" | "space_stats" | "space_records" | "space_changes" | "space_usage" => Reach::Read("namespace"),
        "layers_dependents" => Reach::Read("space"),
        "branch_create" | "branch_drop" | "branch_merge" | "branch_rebase" | "merge_revert" | "tag_create" | "tag_drop" => Reach::Write("namespace"),
        "branch_promote" | "space_clone" => Reach::Copy,
        "layers_set" => Reach::Layers,
        _ => return None,
    })
}

impl Scope {
    pub fn may(&self, namespace: &str, write: bool) -> bool {
        self.write.iter().any(|p| matches(p, namespace)) || (!write && self.read.iter().any(|p| matches(p, namespace)))
    }

    fn check(&self, namespace: &str, write: bool) -> Result<(), String> {
        if self.may(namespace, write) {
            Ok(())
        } else {
            Err(format!("{} namespace {namespace:?} is not open to the {} token", if write { "writing" } else { "reading" }, self.name))
        }
    }

    /// Every stage of a statement must name a namespace this token opens.
    pub fn check_aql(&self, aql: &str) -> Result<(), String> {
        let touches = adb_executor::namespaces::statement_namespaces(aql).map_err(|e| format!("{e} is not open to the {} token", self.name))?;
        for (namespace, write) in touches {
            match namespace {
                Some(ns) => self.check(&ns, write)?,
                None => return Err(format!("a statement without NAMESPACE is not open to the {} token", self.name)),
            }
        }
        Ok(())
    }

    /// Step 4b, on an engine that runs statements within a scope (the partition
    /// router): the namespaces this token reaches for `aql` — its write patterns if
    /// any stage writes, else its read and write patterns. A stage naming a namespace
    /// must be open to it as before; a stage naming none runs inside the patterns.
    /// Kinds that belong to no namespace (topics, subscriptions, MERGE ENTITY, WATCH
    /// without NAMESPACE) stay closed to a scoped token.
    pub fn reach_for(&self, aql: &str) -> Result<adb_cluster::routing::Reach, String> {
        // Per stage (and per namespace a stage reads or writes): its NAMESPACE and
        // whether it writes. Kinds that name no namespace stay closed.
        let touched = adb_executor::namespaces::statement_namespaces(aql).map_err(|what| match what.as_str() {
            "a statement that doesn't parse" => format!("a statement that doesn't parse is not open to the {} token", self.name),
            _ => format!("this kind of statement is not open to the {} token", self.name),
        })?;
        let mut writes = false;
        for (ns, w) in touched {
            if let Some(ns) = ns.as_deref() {
                self.check(ns, w)?;
            }
            writes |= w;
        }
        let patterns: Vec<String> = if writes { self.write.clone() } else { self.write.iter().chain(&self.read).cloned().collect() };
        Ok(adb_cluster::routing::Reach::Only(patterns))
    }

    /// A Flight action, by its body.
    pub fn check_action(&self, action: &str, body: &Value) -> Result<(), String> {
        if action == "aql" {
            return self.check_aql(body.get("query").and_then(|q| q.as_str()).unwrap_or_default());
        }
        // Every namespace's branches: for a token that reads them all.
        if action == "branch_list" && body.get("namespace").is_none() && self.read.iter().chain(&self.write).any(|p| p == "*") {
            return Ok(());
        }
        let field = |f: &str| body.get(f).and_then(|v| v.as_str()).map(String::from).ok_or_else(|| format!("{action} without {f} is not open to the {} token", self.name));
        match reach_of(action) {
            Some(Reach::Read(f)) => self.check(&field(f)?, false),
            Some(Reach::Write(f)) => self.check(&field(f)?, true),
            Some(Reach::Copy) => {
                self.check(&field("namespace")?, false)?;
                self.check(&field("into")?, true)
            }
            Some(Reach::Layers) => {
                self.check(&field("namespace")?, true)?;
                for l in body.get("layers").and_then(|l| l.as_array()).cloned().unwrap_or_default() {
                    self.check(l.get("space").and_then(|s| s.as_str()).unwrap_or_default(), false)?;
                }
                Ok(())
            }
            None => Err(format!("{action} is not open to the {} token", self.name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn acme() -> Scope {
        Scope { name: "acme".into(), token: "t".into(), read: vec!["shared".into()], write: vec!["acme--*".into()] }
    }

    #[test]
    fn patterns_and_read_write() {
        let s = acme();
        assert!(s.may("acme--bot", true) && s.may("acme--bot", false));
        assert!(s.may("shared", false) && !s.may("shared", true));
        assert!(!s.may("globex--bot", false) && !s.may("acme", false) && !s.may("shared2", false));
        let all = Scope { name: "np".into(), token: "t".into(), read: vec!["*".into()], write: vec![] };
        assert!(all.may("anything", false) && !all.may("anything", true));
    }

    #[test]
    fn statements_must_name_an_open_namespace() {
        let s = acme();
        assert!(s.check_aql(r#"STORE INTO SEMANTIC (key = "k") NAMESPACE "acme--bot""#).is_ok());
        assert!(s.check_aql(r#"RECALL FROM SEMANTIC NAMESPACE "shared""#).is_ok());
        for refused in [
            r#"STORE INTO SEMANTIC (key = "k") NAMESPACE "shared""#,
            r#"RECALL FROM SEMANTIC NAMESPACE "globex--bot""#,
            r#"FORGET FROM SEMANTIC WHERE key = "k" NAMESPACE "globex--bot""#,
            r#"RECALL FROM SEMANTIC"#,
            r#"SUBSCRIBE TOPIC "news""#,
            r#"not aql at all"#,
        ] {
            let e = s.check_aql(refused).unwrap_err();
            assert_eq!(adb_core::codes::code_of(&e), "forbidden", "{refused}: {e}");
        }
    }

    #[test]
    fn actions_stay_in_their_spaces() {
        let s = acme();
        assert!(s.check_action("branch_create", &json!({ "namespace": "acme--bot" })).is_ok());
        assert!(s.check_action("branch_create", &json!({ "namespace": "shared" })).is_err());
        assert!(s.check_action("space_clone", &json!({ "namespace": "shared", "into": "acme--new" })).is_ok());
        assert!(s.check_action("space_clone", &json!({ "namespace": "globex--a", "into": "acme--new" })).is_err());
        assert!(s.check_action("space_clone", &json!({ "namespace": "shared", "into": "globex--a" })).is_err());
        assert!(s.check_action("layers_set", &json!({ "namespace": "acme--bot", "layers": [{ "space": "globex--a" }] })).is_err());
        assert!(s.check_action("branch_list", &json!({})).is_err(), "every namespace");
        for closed in ["insert", "compact", "replace_table", "list_tables", "table_changes"] {
            assert!(s.check_action(closed, &json!({ "namespace": "acme--bot" })).is_err(), "{closed}");
        }
    }
}

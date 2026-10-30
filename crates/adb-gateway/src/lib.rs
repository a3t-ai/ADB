//! Subscriptions on one node (DELIVERY_PLAN stage E, `E_SUBSCRIPTIONS.md`).
//!
//! - [`Registry`] — the live subscriptions and their acked cursors. Registrations are
//!   committed log records (`Register`, `UpdateSubCursor`, `Unregister`); the registry
//!   follows them as they commit and is reloaded from the table's `sub` rows on open.
//! - [`deliver`] — the matcher: which changes of one log record a subscription sees.
//! - [`Gateway`] — subscriber reads. Each subscriber tails the *applied* log from its
//!   own position: it reads records up to the apply barrier (so every delivery is
//!   readable when it arrives, spec §7), waits for the barrier to move, and reads on.
//!   Live delivery and catch-up are one code path, so every subscriber of a topic sees
//!   the same offsets in the same order, and a slow subscriber only slows itself.
//!
//! A delivery is built from the record's effects alone, plus the scope and kind of
//! records a `Patch` / `Forget` / `Expire` names (they never change for a lineage),
//! so reading the same offset twice gives the same delivery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use adb_core::log::{CommitTs, Effect, Kind, LogOffset, LogRecord, Memory};
use adb_core::{evaluate_conditions, Condition};
use adb_log::{Log, LogError};
use adb_store::{key_of, row_id, sql_str, PartitionStore, StoredRow, RECORD_KINDS};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// What a subscription follows (spec §4.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Scope(String),
    Topic(String),
}

/// A subscription as registered: the `value` of its `sub` row, without the cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubSpec {
    pub target: Target,
    /// Row kinds to deliver; empty = all (topic events only with `KINDS event`, for a
    /// scope subscription).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<Kind>,
    /// AQL conditions on a change's value (E6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Vec<Condition>>,
}

/// A live subscription.
#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: String,
    pub spec: SubSpec,
    /// The last acknowledged offset; deliveries resume after it.
    pub cursor: u64,
    /// When it was last registered, read or acked here (since this process opened:
    /// a restart starts every subscription's idle clock again).
    pub seen: std::time::Instant,
}

/// The `value` of a `sub` row (D3: `{target, filter, …, cursor}`).
pub fn sub_value(spec: &SubSpec, cursor: u64) -> serde_json::Value {
    let mut v = serde_json::to_value(spec).unwrap_or_default();
    if let Some(m) = v.as_object_mut() {
        m.insert("cursor".into(), cursor.into());
    }
    v
}

fn from_value(id: &str, v: &serde_json::Value) -> Option<Subscription> {
    let spec: SubSpec = serde_json::from_value(v.clone()).ok()?;
    let cursor = v.get("cursor").and_then(|c| c.as_u64()).unwrap_or(0);
    Some(Subscription { id: id.to_string(), spec, cursor, seen: std::time::Instant::now() })
}

/// The partition's live subscriptions.
#[derive(Debug, Default)]
pub struct Registry {
    subs: RwLock<HashMap<String, Subscription>>,
}

impl Registry {
    /// The subscriptions in the table's current `sub` rows.
    pub async fn load(store: &PartitionStore, now: CommitTs) -> Result<Self, String> {
        let rows = store.current(Some("kind = 'sub'"), now).await.map_err(|e| e.to_string())?;
        let subs = rows
            .iter()
            .filter_map(|r| from_value(&r.id, &r.data.value))
            .map(|s| (s.id.clone(), s))
            .collect();
        Ok(Self { subs: RwLock::new(subs) })
    }

    /// Follow a committed record's registration effects (called in log order).
    pub fn apply(&self, record: &LogRecord) {
        let mut subs = self.subs.write();
        for effect in &record.effects {
            match effect {
                Effect::Register(n) if n.data.kind == Kind::Sub => {
                    if let Some(s) = from_value(&n.id.0, &n.data.value) {
                        subs.insert(s.id.clone(), s);
                    }
                }
                Effect::UpdateSubCursor { sub_id, cursor } => {
                    if let Some(s) = subs.get_mut(&sub_id.0) {
                        s.cursor = s.cursor.max(cursor.0);
                        s.seen = std::time::Instant::now();
                    }
                }
                Effect::Unregister { id } => {
                    subs.remove(&id.0);
                }
                _ => {}
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<Subscription> {
        self.subs.read().get(id).cloned()
    }

    pub fn len(&self) -> usize {
        self.subs.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The oldest acked cursor: the log must keep everything after it (E3).
    pub fn min_cursor(&self) -> Option<u64> {
        self.subs.read().values().map(|s| s.cursor).min()
    }

    /// A subscriber read `id`: its idle clock starts again (W9).
    pub fn touch(&self, id: &str) {
        if let Some(s) = self.subs.write().get_mut(id) {
            s.seen = std::time::Instant::now();
        }
    }

    /// Subscriptions nobody has read or acked for `idle` (W9, E-L3).
    pub fn idle(&self, idle: Duration) -> Vec<String> {
        self.subs.read().values().filter(|s| s.seen.elapsed() >= idle).map(|s| s.id.clone()).collect()
    }
}

/// One delivery: the changes of one log record a subscription matched (E5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    pub partition: u32,
    pub offset: u64,
    pub commit_ts: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_id: Option<String>,
    pub changes: Vec<Change>,
}

/// One changed record, row, link or topic message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    /// `put`, `patch`, `supersede`, `forget`, `expire`, `link` or `publish`.
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
    pub kind: Kind,
    /// The record key (its lineage), the link id or the message id.
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    pub scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// The new value (`put` / `supersede` / `publish`), the patched fields (`patch`),
    /// the link (`link`); absent for `forget` / `expire`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// In a `reset`: the offset that wrote this row (a delivery has its own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}

/// Scope and kind of record `key` of `memory`, from any of its versions; and the
/// current rows a subscription target covers (its `reset`, W9).
#[async_trait::async_trait]
pub trait Lineages: Send + Sync {
    async fn lineage(&self, memory: Memory, key: &str) -> Option<(String, Kind)>;
    /// Current rows of a scope; a topic's retained messages after offset `after`.
    async fn current_rows(&self, target: &Target, after: u64) -> Result<Vec<StoredRow>, String>;
}

#[async_trait::async_trait]
impl Lineages for PartitionStore {
    async fn lineage(&self, memory: Memory, key: &str) -> Option<(String, Kind)> {
        // Tombstoned and superseded versions too: a forget is applied by now. Edges
        // too: UNLINK forgets an edge's own lineage (AQL v0.7).
        let filter = format!("lineage_id = {} AND ({RECORD_KINDS} OR kind = 'edge')", sql_str(&row_id(memory, key)));
        self.scan(Some(&filter)).await.ok()?.into_iter().next().map(|r| (r.data.scope, r.data.kind))
    }

    async fn current_rows(&self, target: &Target, after: u64) -> Result<Vec<StoredRow>, String> {
        let filter = match target {
            Target::Scope(s) => format!("scope = {} AND ({RECORD_KINDS} OR kind IN ('edge', 'event'))", sql_str(s)),
            // Messages never change: the ones after the subscriber's position are
            // exactly what it missed (those still retained).
            Target::Topic(t) => format!("kind = 'event' AND subject_id = {} AND log_offset > {after}", sql_str(t)),
        };
        self.current(Some(&filter), CommitTs::from_datetime(adb_core::time::now())).await.map_err(|e| e.to_string())
    }
}

/// A current row as the change that would have created it (a `reset`'s content),
/// with the unqualified ids live deliveries carry.
fn row_change(r: &StoredRow) -> Change {
    let d = &r.data;
    let bare = |s: &Option<String>| s.as_deref().map(|s| key_of(s).to_string());
    match d.kind {
        Kind::Edge => Change {
            op: "link".into(),
            memory: Some(d.memory),
            kind: Kind::Edge,
            id: key_of(&r.id).to_string(),
            version: None,
            scope: d.scope.clone(),
            topic: None,
            value: Some(serde_json::json!({
                "from": bare(&d.subject_id),
                "to": bare(&d.object_id),
                "to_memory": d.value.get("to_memory"),
                "link_type": d.link_type,
                "weight": d.weight,
            })),
            offset: Some(r.log_offset),
        },
        Kind::Event => Change {
            op: "publish".into(),
            memory: None,
            kind: Kind::Event,
            id: key_of(&r.id).to_string(),
            version: None,
            scope: d.scope.clone(),
            topic: d.subject_id.clone(),
            value: Some(d.value.clone()),
            offset: Some(r.log_offset),
        },
        kind => Change {
            op: "put".into(),
            memory: Some(d.memory),
            kind,
            id: r.key().to_string(),
            version: Some(r.version),
            scope: d.scope.clone(),
            topic: None,
            value: Some(d.value.clone()),
            offset: Some(r.log_offset),
        },
    }
}

/// The changes of `record` that `spec` matches, or `None`.
pub async fn deliver(spec: &SubSpec, record: &LogRecord, lineages: &dyn Lineages) -> Option<Delivery> {
    let mut changes = Vec::new();
    for effect in &record.effects {
        let change = match effect {
            Effect::PutEntity(n) | Effect::PutFact(n) | Effect::PutEpisode(n) | Effect::PutTool(n) => Change {
                op: "put".into(),
                memory: Some(n.data.memory),
                kind: n.data.kind,
                id: n.lineage_id.0.clone(),
                version: Some(n.version),
                scope: n.data.scope.clone(),
                topic: None,
                value: Some(n.data.value.clone()),
                offset: None,
            },
            Effect::Supersede { new: n, .. } => Change {
                op: "supersede".into(),
                memory: Some(n.data.memory),
                kind: n.data.kind,
                id: n.lineage_id.0.clone(),
                version: Some(n.version),
                scope: n.data.scope.clone(),
                topic: None,
                value: Some(n.data.value.clone()),
                offset: None,
            },
            Effect::PutEdge(n) => Change {
                op: "link".into(),
                memory: Some(n.data.memory),
                kind: Kind::Edge,
                id: n.id.0.clone(),
                version: None,
                scope: n.data.scope.clone(),
                topic: None,
                value: Some(serde_json::json!({
                    "from": n.data.subject_id,
                    "to": n.data.object_id,
                    "to_memory": n.data.value.get("to_memory"),
                    "link_type": n.data.link_type,
                    "weight": n.data.weight,
                })),
                offset: None,
            },
            Effect::PublishEvent(n) => Change {
                op: "publish".into(),
                memory: None,
                kind: Kind::Event,
                id: n.id.0.clone(),
                version: None,
                scope: n.data.scope.clone(),
                topic: n.data.subject_id.clone(),
                value: Some(n.data.value.clone()),
                offset: None,
            },
            Effect::Patch { memory, id, fields, .. } => {
                let Some((scope, kind)) = lineages.lineage(*memory, &id.0).await else { continue };
                Change {
                    op: "patch".into(),
                    memory: Some(*memory),
                    kind,
                    id: id.0.clone(),
                    version: None,
                    scope,
                    topic: None,
                    value: Some(serde_json::Value::Object(fields.clone())),
                    offset: None,
                }
            }
            Effect::Forget { memory, lineage_id } | Effect::Expire { memory, lineage_id } => {
                let Some((scope, kind)) = lineages.lineage(*memory, &lineage_id.0).await else { continue };
                Change {
                    op: match (effect, kind) {
                        (Effect::Forget { .. }, Kind::Edge) => "unlink",
                        (Effect::Forget { .. }, _) => "forget",
                        _ => "expire",
                    }
                    .into(),
                    memory: Some(*memory),
                    kind,
                    id: lineage_id.0.clone(),
                    version: None,
                    scope,
                    topic: None,
                    value: None,
                    offset: None,
                }
            }
            // A fact began or stopped holding (AQL v0.10): `begin` / `end`.
            Effect::Validity { memory, lineage_id, holds } => {
                let Some((scope, kind)) = lineages.lineage(*memory, &lineage_id.0).await else { continue };
                Change {
                    op: if *holds { "begin" } else { "end" }.into(),
                    memory: Some(*memory),
                    kind,
                    id: lineage_id.0.clone(),
                    version: None,
                    scope,
                    topic: None,
                    value: None,
                    offset: None,
                }
            }
            // A vector attached to a row is not a change of its content.
            Effect::Embed { .. } => continue,
            // Registrations and cursors are not changes anyone subscribes to.
            Effect::Register(_) | Effect::UpdateSubCursor { .. } | Effect::Unregister { .. } => continue,
            // Platform tables (stage G) aren't memory: no subscription sees them.
            Effect::TableCreate { .. } | Effect::TableInsert { .. } | Effect::TableUpdate { .. } | Effect::TableDelete { .. } | Effect::TableReplace { .. } => continue,
        };
        if matches(spec, &change) {
            changes.push(change);
        }
    }
    (!changes.is_empty()).then(|| Delivery {
        partition: u32::from(record.partition.0),
        offset: record.offset.0,
        commit_ts: record.commit_ts.to_datetime().to_rfc3339(),
        txn_id: record.txn_id.clone(),
        changes,
    })
}

fn matches(spec: &SubSpec, c: &Change) -> bool {
    let target = match &spec.target {
        Target::Topic(t) => c.kind == Kind::Event && c.topic.as_deref() == Some(t),
        // A scope subscription sees topic messages only when it asks for them.
        Target::Scope(s) => &c.scope == s && (c.kind != Kind::Event || spec.kinds.contains(&Kind::Event)),
    };
    let kind = spec.kinds.is_empty() || spec.kinds.contains(&c.kind);
    // Conditions apply to values; patches, forgets and links have none to test.
    let filter = match (&spec.filter, c.op.as_str(), &c.value) {
        (Some(conds), "put" | "supersede" | "publish", Some(v)) => evaluate_conditions(v, conds),
        _ => true,
    };
    target && kind && filter
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("unknown subscription {0}")]
    UnknownSubscription(String),
    #[error("log: {0}")]
    Log(String),
}

/// Deliveries for one read, and where the next read starts.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Batch {
    pub deliveries: Vec<Delivery>,
    /// The first offset not yet read: pass it as `from` next time (offsets that
    /// matched nothing are skipped, not re-read).
    pub next: u64,
    /// The applied tip when the read returned.
    pub tip: u64,
    /// Set when the subscriber's position is no longer in the log (W9, spec §4.4
    /// and reconciliation note 2): the current state replaces the missed changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset: Option<Reset>,
}

/// Catch-up from the table, as of `offset`. For a scope: its current rows — the
/// subscriber drops what it holds for the scope and takes these. For a topic: the
/// retained messages after the subscriber's position, in order (each change carries
/// its own `offset`). Changes after `offset` follow as deliveries (some may repeat
/// what the reset already shows: at-least-once).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reset {
    pub partition: u32,
    pub offset: u64,
    pub changes: Vec<Change>,
}

/// Subscriber reads over one partition (in-process gateway, stage E).
pub struct Gateway {
    log: Arc<dyn Log>,
    lineages: Arc<dyn Lineages>,
    registry: Arc<Registry>,
    applied: watch::Receiver<u64>,
}

/// Records read from the log per call.
const READ_BATCH: usize = 256;

impl Gateway {
    pub fn new(log: Arc<dyn Log>, lineages: Arc<dyn Lineages>, registry: Arc<Registry>, applied: watch::Receiver<u64>) -> Self {
        Self { log, lineages, registry, applied }
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Up to `max` deliveries for `sub` from offset `from` (default: after its acked
    /// cursor), waiting up to `wait` for the first one.
    pub async fn next(&self, sub: &str, from: Option<u64>, max: usize, wait: Duration) -> Result<Batch, GatewayError> {
        let s = self.registry.get(sub).ok_or_else(|| GatewayError::UnknownSubscription(sub.to_string()))?;
        self.registry.touch(sub);
        let mut next = from.unwrap_or(s.cursor + 1).max(1);
        let deadline = tokio::time::Instant::now() + wait;
        let mut applied = self.applied.clone();
        loop {
            let tip = *applied.borrow_and_update();
            let mut out = Vec::new();
            'read: while next <= tip && out.len() < max {
                let n = ((tip - next + 1) as usize).min(READ_BATCH);
                let records = match self.log.read(LogOffset(next), n).await {
                    Ok(r) => r,
                    Err(LogError::Purged { .. }) => {
                        // Past log retention: catch up from the table (W9).
                        let rows = self.lineages.current_rows(&s.spec.target, next - 1).await.map_err(GatewayError::Log)?;
                        let mut changes: Vec<Change> = rows.iter().map(row_change).filter(|c| matches(&s.spec, c)).collect();
                        changes.sort_by_key(|c| c.offset);
                        let reset = Reset { partition: u32::from(self.log.partition().0), offset: tip, changes };
                        return Ok(Batch { deliveries: Vec::new(), next: tip + 1, tip, reset: Some(reset) });
                    }
                    Err(e) => return Err(GatewayError::Log(e.to_string())),
                };
                if records.is_empty() {
                    break;
                }
                for r in &records {
                    next = r.offset.0 + 1;
                    if let Some(d) = deliver(&s.spec, r, self.lineages.as_ref()).await {
                        out.push(d);
                        if out.len() >= max {
                            break 'read;
                        }
                    }
                }
            }
            if !out.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok(Batch { deliveries: out, next, tip, reset: None });
            }
            // Nothing yet: wait for apply to move on, or the deadline.
            if tokio::time::timeout_at(deadline, applied.changed()).await.is_err() {
                let tip = *applied.borrow();
                return Ok(Batch { deliveries: Vec::new(), next, tip, reset: None });
            }
        }
    }
}

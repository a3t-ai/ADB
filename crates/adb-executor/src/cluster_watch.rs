//! A cluster's watches across partitions: `WATCH` with no `NAMESPACE` (or several)
//! on a cluster (SUBSCRIPTIONS_CQ_SPEC §6).
//!
//! The watch is registered on the system partition (its spec carries the call's
//! scope, `across`) and runs on that partition's owner, in a watcher of its own
//! ([`adb_cq::Watcher`]: frames, resuming, idle expiry as on one engine). Its query
//! re-runs across the partitions, within that scope, as the statement would.
//!
//! What tells it a re-run is due: for each partition holding data, one long poll to
//! the partition's owner — "what in your log after offset N can change these
//! watches?" ([`adb_cq::Watcher::touched_since`]). Nothing is registered on the
//! partitions, so nothing is left behind when a watch or its owner goes. A record
//! forgotten on any partition is scrubbed from the buffered frames, as on one engine.
//!
//! Its id is `c.<id>`: any node sends `UNWATCH` and frame reads for it to the system
//! partition's owner. Frame offsets count the changes seen, from the time the watcher
//! started; a reader resuming at an offset from an earlier owner gets a `reset`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use adb_cluster::map::Partition;
use adb_cluster::routing::Reach;
use adb_cq::{Touched, WatchSpec, Watcher};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::task::JoinHandle;

use crate::partitioned::PartitionedEngine;

/// The id prefix of a cluster's watch.
pub const PREFIX: &str = "c.";

/// How long one partition poll waits for a change.
const POLL_WAIT: Duration = Duration::from_secs(5);

/// The watches across partitions on the system partition's owner.
pub struct ClusterWatches {
    pub(crate) watcher: Arc<Watcher>,
    /// The watcher's "applied tip": raised on every change a partition reports, so
    /// each frame gets a new offset.
    clock: tokio::sync::watch::Sender<u64>,
    /// The watches, as the partitions are asked about them.
    specs: std::sync::Mutex<HashMap<String, WatchSpec>>,
    /// Raised when the watches change: the polls start again with the new set.
    changed: tokio::sync::watch::Sender<u64>,
    pumps: std::sync::Mutex<HashMap<Partition, JoinHandle<()>>>,
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// The system partition's epoch this was started for.
    pub(crate) epoch: u64,
    alive: std::sync::atomic::AtomicBool,
}

impl Drop for ClusterWatches {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Runner {
    node: Weak<PartitionedEngine>,
}

#[async_trait]
impl adb_cq::Runner for Runner {
    async fn run(&self, spec: &WatchSpec) -> Result<Value, String> {
        let node = self.node.upgrade().ok_or("the node is stopping")?;
        let reach: Reach = serde_json::from_value(spec.across.clone().ok_or("not a watch across partitions")?).map_err(|e| e.to_string())?;
        node.run_across(&spec.statement, &spec.variables, &reach).await
    }

    async fn unwatch(&self, cq_id: &str) -> Result<(), String> {
        let node = self.node.upgrade().ok_or("the node is stopping")?;
        node.system_op("cq_unregister", json!({ "id": cq_id })).await.map(|_| ())
    }
}

impl ClusterWatches {
    /// Start on the system partition's owner: load the registered watches, take their
    /// snapshots, poll the partitions.
    pub(crate) async fn start(node: &Arc<PartitionedEngine>, epoch: u64) -> Result<Arc<Self>, String> {
        let start = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
        let (clock, applied) = tokio::sync::watch::channel(start);
        let log: Arc<dyn adb_log::Log> = Arc::new(adb_log::MemLog::new(adb_core::log::PartitionId(0)));
        let watcher = Watcher::new(log, applied, Arc::new(Runner { node: Arc::downgrade(node) }), start);
        let cw = Arc::new(Self {
            watcher: Arc::clone(&watcher),
            clock,
            specs: Default::default(),
            changed: tokio::sync::watch::channel(0).0,
            pumps: Default::default(),
            tasks: Default::default(),
            epoch,
            alive: std::sync::atomic::AtomicBool::new(true),
        });
        let registered: Vec<(String, WatchSpec)> = serde_json::from_value(node.system_op("cq_across", json!({})).await?).map_err(|e| e.to_string())?;
        for (id, spec) in registered {
            if let Err(e) = cw.add(&id, spec).await {
                tracing::warn!(cq = %id, error = %e, "watch across partitions not restored");
            }
        }
        cw.tasks.lock().unwrap().push(Watcher::spawn(&watcher));
        // The partitions holding data, as the catalog grows.
        let (weak_node, weak_cw) = (Arc::downgrade(node), Arc::downgrade(&cw));
        cw.tasks.lock().unwrap().push(tokio::spawn(async move {
            loop {
                let (Some(node), Some(cw)) = (weak_node.upgrade(), weak_cw.upgrade()) else { return };
                if !cw.alive() || node.system_epoch_here() != Some(cw.epoch) {
                    cw.stop();
                    return;
                }
                if !cw.specs.lock().unwrap().is_empty() {
                    match node.data_partitions().await {
                        Ok(parts) => {
                            let mut pumps = cw.pumps.lock().unwrap();
                            for p in parts {
                                pumps.entry(p).or_insert_with(|| tokio::spawn(pump(Arc::downgrade(&node), Arc::downgrade(&cw), p)));
                            }
                        }
                        Err(e) => tracing::debug!(error = %e, "watches across partitions: catalog not read"),
                    }
                }
                drop((node, cw));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }));
        Ok(cw)
    }

    pub(crate) fn alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Stop polling (this node no longer owns the system partition).
    pub(crate) fn stop(&self) {
        self.alive.store(false, std::sync::atomic::Ordering::SeqCst);
        for (_, h) in self.pumps.lock().unwrap().drain() {
            h.abort();
        }
        for h in self.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }

    /// Add a registered watch and take its snapshot.
    pub(crate) async fn add(&self, id: &str, spec: WatchSpec) -> Result<Value, String> {
        let frame = self.watcher.add(id, spec.clone()).await.map_err(|e| e.to_string())?;
        self.specs.lock().unwrap().insert(id.to_string(), spec);
        self.changed.send_modify(|n| *n += 1);
        let mut v = serde_json::to_value(frame).map_err(|e| e.to_string())?;
        name_frames(&mut v);
        Ok(v)
    }

    pub(crate) fn remove(&self, id: &str) -> bool {
        self.specs.lock().unwrap().remove(id);
        self.changed.send_modify(|n| *n += 1);
        self.watcher.remove(id)
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.watcher.contains(id)
    }

    pub(crate) async fn frames(&self, id: &str, from: Option<u64>, max: usize, wait: Duration) -> Result<Value, String> {
        let frames = self.watcher.next(id, from, max, wait).await.map_err(|e| e.to_string().replace(id, &format!("{PREFIX}{id}")))?;
        let mut v = serde_json::to_value(frames).map_err(|e| e.to_string())?;
        name_frames(&mut v);
        Ok(v)
    }

    /// A partition reported changes: record them and mark the watches due.
    fn report(&self, t: Touched) {
        for (memory, key) in &t.forgotten {
            self.watcher.forget(*memory, key);
        }
        if !t.touched.is_empty() {
            self.clock.send_modify(|c| *c += 1);
            self.watcher.poke(&t.touched);
        }
    }

    /// Every watch re-runs once (a partition's poll started: what came before it is
    /// not known).
    fn poke_all(&self) {
        let ids: Vec<String> = self.specs.lock().unwrap().keys().cloned().collect();
        self.report(Touched { touched: ids, ..Default::default() });
    }
}

/// A cluster watch's ids, as callers see them (`c.<id>`).
pub(crate) fn name_frames(v: &mut Value) {
    match v {
        Value::Object(m) => {
            if let Some(Value::String(id)) = m.get_mut("cq_id") {
                if !id.starts_with(PREFIX) {
                    *id = format!("{PREFIX}{id}");
                }
            }
            m.values_mut().for_each(name_frames);
        }
        Value::Array(a) => a.iter_mut().for_each(name_frames),
        _ => {}
    }
}

/// One partition's polls, for as long as the watches run.
async fn pump(node: Weak<PartitionedEngine>, cw: Weak<ClusterWatches>, p: Partition) {
    let mut after: Option<u64> = None;
    loop {
        let (Some(n), Some(c)) = (node.upgrade(), cw.upgrade()) else { return };
        if !c.alive() {
            return;
        }
        let mut changed = c.changed.subscribe();
        changed.borrow_and_update();
        let watches: Vec<(String, WatchSpec)> = c.specs.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        if watches.is_empty() {
            drop((n, c));
            let _ = tokio::time::timeout(Duration::from_secs(5), changed.changed()).await;
            continue;
        }
        let args = json!({ "after": after, "watches": watches, "wait_ms": if after.is_some() { POLL_WAIT.as_millis() as u64 } else { 0 } });
        let h = n.handle(p).await;
        // Nothing held through the poll: the node (and its watches) can stop meanwhile.
        drop((n, c));
        let call = async { h?.op_as::<Touched>("cq_touched", args).await };
        let r = tokio::select! {
            r = call => Some(r),
            // The watches changed: ask again with the new set (from the same offset).
            _ = changed.changed() => None,
        };
        let Some(c) = cw.upgrade() else { return };
        match r {
            None => {}
            Some(Ok(t)) => {
                if after.is_none() {
                    after = Some(t.tip);
                    c.poke_all();
                } else {
                    after = Some(t.tip);
                    c.report(t);
                }
            }
            Some(Err(e)) => {
                tracing::debug!(partition = p, error = %e, "watches across partitions: partition poll failed; retrying");
                drop(c);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

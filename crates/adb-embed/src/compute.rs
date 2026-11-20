//! Compute nodes (19 Nov 2026): processes that run no database, only work the
//! database nodes hand them. Today one function, `embed` (text → vector); more later.
//!
//! A compute node (`adb serve-compute`) announces itself to every database node's
//! control endpoint every [`ANNOUNCE_EVERY`]. A database node keeps who announced
//! within [`TTL`] in this registry and sends its embedding requests to the compute
//! node with the fewest of its requests in flight. One that fails rests for
//! [`REST`]. `ADB_COMPUTE_URL` (comma-separated) names compute nodes up front as
//! well; they never expire.
//!
//! The database nodes then load no model: with `ADB_EMBED=compute`, a database node
//! embeds only through compute nodes. With none up, a write still commits (vectors
//! follow later, `ADB_EMBED_DEFER`), and a `LIKE` read is refused until one is up.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// How often a compute node announces itself.
pub const ANNOUNCE_EVERY: Duration = Duration::from_secs(2);
/// A compute node not heard from for this long is dropped.
pub const TTL: Duration = Duration::from_secs(8);
/// How long a compute node that failed a request is left out.
pub const REST: Duration = Duration::from_secs(5);

/// What a compute node says about itself (`POST /control/compute`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Announce {
    /// Where database nodes reach it (`http://host:3003`).
    pub url: String,
    /// The functions it runs: `embed` today.
    pub functions: Vec<String>,
    /// Its embedding model and dimension (a database node uses only a matching one).
    pub model: String,
    pub dimension: usize,
    /// Its embedding workers.
    pub workers: usize,
    /// Requests it is running or has queued now.
    pub in_flight: usize,
}

/// One compute node as a database node sees it.
struct Node {
    announce: Option<Announce>,
    /// `None`: named in `ADB_COMPUTE_URL`, never expires.
    seen: Option<Instant>,
    /// This database node's requests to it in flight.
    in_flight: Arc<AtomicUsize>,
    served: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    rest_until: Option<Instant>,
}

#[derive(Default)]
struct Registry {
    nodes: BTreeMap<String, Node>,
}

fn registry() -> &'static Mutex<Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(|| {
        let mut r = Registry::default();
        for url in static_urls() {
            r.nodes.insert(url, Node::new(None, None));
        }
        Mutex::new(r)
    })
}

impl Node {
    fn new(announce: Option<Announce>, seen: Option<Instant>) -> Self {
        Self { announce, seen, in_flight: Default::default(), served: Default::default(), failed: Default::default(), rest_until: None }
    }
    fn live(&self, now: Instant) -> bool {
        self.seen.is_none_or(|s| now.duration_since(s) < TTL)
    }
}

/// `ADB_COMPUTE_URL`: compute nodes named up front.
pub fn static_urls() -> Vec<String> {
    std::env::var("ADB_COMPUTE_URL")
        .unwrap_or_default()
        .split(',')
        .map(|u| u.trim().trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty())
        .collect()
}

/// Whether this database node embeds through compute nodes (`ADB_EMBED=compute`, or
/// `ADB_COMPUTE_URL` set).
pub fn enabled() -> bool {
    std::env::var("ADB_EMBED").is_ok_and(|v| v.eq_ignore_ascii_case("compute")) || !static_urls().is_empty()
}

/// A compute node announced itself.
pub fn heard(a: Announce) {
    let url = a.url.trim_end_matches('/').to_string();
    let mut r = registry().lock().unwrap();
    let now = Instant::now();
    match r.nodes.get_mut(&url) {
        Some(n) => {
            if n.seen.is_some() {
                n.seen = Some(now);
            }
            n.announce = Some(Announce { url: url.clone(), ..a });
        }
        None => {
            tracing::info!(url = %url, functions = ?a.functions, workers = a.workers, "compute node joined");
            r.nodes.insert(url.clone(), Node::new(Some(Announce { url, ..a }), Some(now)));
        }
    }
}

/// A compute node chosen for one request: counted in flight until dropped.
pub struct Lease {
    pub url: String,
    in_flight: Arc<AtomicUsize>,
    served: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    ok: bool,
}

impl Lease {
    /// The request succeeded.
    pub fn done(mut self) {
        self.ok = true;
        self.served.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        if !self.ok {
            self.failed.fetch_add(1, Ordering::Relaxed);
            if let Some(n) = registry().lock().unwrap().nodes.get_mut(&self.url) {
                n.rest_until = Some(Instant::now() + REST);
            }
        }
    }
}

/// The live compute node running `model` with the fewest of this node's requests in
/// flight, skipping `tried`; `None` when there is none.
pub fn pick(model: &str, dimension: usize, tried: &[String]) -> Option<Lease> {
    let mut r = registry().lock().unwrap();
    let now = Instant::now();
    r.nodes.retain(|url, n| {
        let keep = n.live(now);
        if !keep {
            tracing::info!(url = %url, "compute node left (not heard from)");
        }
        keep
    });
    let n = r
        .nodes
        .iter()
        .filter(|(url, n)| !tried.contains(url) && n.rest_until.is_none_or(|t| t <= now))
        // An announced node must run the same model; a named one is taken on trust
        // (the dimension check on its answer catches a wrong one).
        .filter(|(_, n)| n.announce.as_ref().is_none_or(|a| a.model == model && a.dimension == dimension && a.functions.iter().any(|f| f == "embed")))
        .min_by_key(|(_, n)| n.in_flight.load(Ordering::Relaxed))?;
    let (url, n) = n;
    n.in_flight.fetch_add(1, Ordering::Relaxed);
    Some(Lease { url: url.clone(), in_flight: Arc::clone(&n.in_flight), served: Arc::clone(&n.served), failed: Arc::clone(&n.failed), ok: false })
}

/// How many compute nodes are live now.
pub fn live() -> usize {
    let r = registry().lock().unwrap();
    let now = Instant::now();
    r.nodes.values().filter(|n| n.live(now)).count()
}

/// The compute nodes as this database node sees them (`GET /control/compute`, the
/// cluster's status).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub url: String,
    pub role: String,
    pub functions: Vec<String>,
    pub model: Option<String>,
    pub workers: Option<usize>,
    /// Requests the compute node reported running or queued.
    pub load: Option<usize>,
    /// This database node's requests to it now, served and failed so far.
    pub in_flight: usize,
    pub served: u64,
    pub failed: u64,
    /// Seconds since it announced itself (`None`: named in `ADB_COMPUTE_URL`).
    pub seen_secs_ago: Option<f64>,
    pub resting: bool,
}

pub fn status() -> Vec<Status> {
    let r = registry().lock().unwrap();
    let now = Instant::now();
    r.nodes
        .iter()
        .filter(|(_, n)| n.live(now))
        .map(|(url, n)| Status {
            url: url.clone(),
            role: "compute".into(),
            functions: n.announce.as_ref().map_or_else(|| vec!["embed".into()], |a| a.functions.clone()),
            model: n.announce.as_ref().map(|a| a.model.clone()),
            workers: n.announce.as_ref().map(|a| a.workers),
            load: n.announce.as_ref().map(|a| a.in_flight),
            in_flight: n.in_flight.load(Ordering::Relaxed),
            served: n.served.load(Ordering::Relaxed),
            failed: n.failed.load(Ordering::Relaxed),
            seen_secs_ago: n.seen.map(|s| now.duration_since(s).as_secs_f64()),
            resting: n.rest_until.is_some_and(|t| t > now),
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn clear() {
    registry().lock().unwrap().nodes.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(url: &str, model: &str) -> Announce {
        Announce { url: url.into(), functions: vec!["embed".into()], model: model.into(), dimension: 384, workers: 4, in_flight: 0 }
    }

    /// One test: the registry is process-wide.
    #[tokio::test(flavor = "multi_thread")]
    async fn picks_the_least_busy_matching_node_and_rests_a_failed_one() {
        clear();
        assert!(pick("m", 384, &[]).is_none(), "no compute node yet");
        heard(a("http://c1:3003/", "m"));
        heard(a("http://c2:3003", "m"));
        heard(a("http://c3:3003", "other-model"));
        assert_eq!(live(), 3);
        let first = pick("m", 384, &[]).unwrap();
        let second = pick("m", 384, &[]).unwrap();
        assert_ne!(first.url, second.url, "the second request goes to the idle node");
        assert!(![&first.url, &second.url].contains(&&"http://c3:3003".to_string()), "another model is never picked");
        let failed = first.url.clone();
        drop(first); // not done(): failed
        second.done();
        let next = pick("m", 384, &[]).unwrap();
        assert_ne!(next.url, failed, "a failed node rests");
        next.done();
        let s = status();
        assert_eq!(s.iter().find(|s| s.url == failed).unwrap().failed, 1);
        assert!(s.iter().find(|s| s.url == failed).unwrap().resting);
        assert!(pick("m", 384, &["http://c1:3003".into(), "http://c2:3003".into()]).is_none(), "tried ones are skipped");
        clear();

        // A database node's embedder sends its texts to a compute node that announced
        // itself, and to another when one fails; with none up, it says so.
        use axum::{routing::post, Json, Router};
        let app = Router::new().route(
            "/embeddings",
            post(|Json(body): Json<serde_json::Value>| async move {
                let n = body["input"].as_array().unwrap().len();
                let data: Vec<_> = (0..n).map(|i| serde_json::json!({ "index": i, "embedding": vec![i as f32 + 1.0; crate::DIM] })).collect();
                Json(serde_json::json!({ "data": data }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        std::env::set_var("ADB_EMBED", "compute");
        let e = Arc::new(crate::Embedder::compute_from_env().expect("ADB_EMBED=compute"));
        assert!(e.on_compute());
        let none = e.embed(vec!["a".into()]).await.unwrap_err();
        assert!(none.contains("no compute node is up"), "{none}");
        heard(Announce { url: "http://127.0.0.1:1".into(), ..a("x", crate::MODEL) }); // nothing listens there
        heard(Announce { url: live_url.clone(), ..a("x", crate::MODEL) });
        for _ in 0..3 {
            let v = e.embed(vec!["a".into(), "b".into()]).await.unwrap();
            assert_eq!(v.len(), 2);
            assert_eq!(v[0].len(), crate::DIM);
        }
        let s = status();
        assert!(s.iter().find(|s| s.url == live_url).unwrap().served >= 3);
        std::env::remove_var("ADB_EMBED");
        clear();
    }
}

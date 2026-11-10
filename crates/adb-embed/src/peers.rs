//! Embedding across a replica set (C-17b, AWS F-20): the leader prepares every write,
//! and embedding is most of a SEMANTIC / EPISODIC write's CPU. With peers set
//! ([`set_peers`], the other nodes' Raft endpoints), each local-model request goes
//! to whichever node — this one or a peer — has the fewest requests in flight; a
//! peer embeds it on its own model ([`serve`], `POST /cluster/embed`). A peer that
//! fails is left out for [`PEER_REST`] and the request runs here. Every node loads
//! the same model (the image's, or the same `embedding.json`): a peer refuses a
//! request naming another model or dimension.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::Embedder;

/// How long a peer that failed is left out.
pub const PEER_REST: Duration = Duration::from_secs(10);

struct Peer {
    url: String,
    in_flight: AtomicUsize,
    down_until: Mutex<Option<Instant>>,
}

struct Peers {
    http: reqwest::Client,
    peers: Vec<Peer>,
}

static PEERS: OnceLock<Peers> = OnceLock::new();
/// Requests this node's own model is running or has queued.
pub(crate) static LOCAL_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// The embedder this node lends its peers.
static SERVING: RwLock<Option<Arc<Embedder>>> = RwLock::new(None);

/// The other nodes' Raft endpoints (`http://host:3002`); set once, at start.
/// `ADB_EMBED_PEERS=0` keeps every embedding on the node that needs it.
pub fn set_peers(urls: Vec<String>) {
    if urls.is_empty() || std::env::var("ADB_EMBED_PEERS").is_ok_and(|v| v == "0") {
        return;
    }
    let http = match reqwest::Client::builder().connect_timeout(Duration::from_millis(300)).timeout(Duration::from_secs(10)).build() {
        Ok(h) => h,
        Err(e) => return tracing::warn!(error = %e, "embedding peers not used"),
    };
    let peers = urls.into_iter().map(|u| Peer { url: format!("{}/cluster/embed", u.trim_end_matches('/')), in_flight: AtomicUsize::new(0), down_until: Mutex::new(None) }).collect();
    if PEERS.set(Peers { http, peers }).is_ok() {
        tracing::info!("embedding shared with the replica set's other nodes (ADB_EMBED_PEERS=0 turns it off)");
    }
}

/// Lend `embedder` to the peers (the node's current one; replaced when settings change).
pub fn serve_with(embedder: Option<Arc<Embedder>>) {
    *SERVING.write().unwrap() = embedder;
}

#[derive(Serialize, Deserialize)]
pub struct EmbedRequest {
    pub model: String,
    pub dimension: usize,
    pub texts: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct EmbedAnswer {
    pub vectors: Vec<Vec<f32>>,
}

/// A peer's request, on this node's own model (never passed on).
pub async fn serve(req: EmbedRequest) -> Result<EmbedAnswer, String> {
    let e = SERVING.read().unwrap().clone().ok_or("this node has no embedding model")?;
    if e.model() != req.model || e.dimension() != req.dimension {
        return Err(format!("this node embeds with {} ({} dimensions), not {} ({})", e.model(), e.dimension(), req.model, req.dimension));
    }
    let n = req.texts.len();
    let vectors = e.embed_here(req.texts).await?;
    tracing::debug!(texts = n, "embedded for a peer");
    Ok(EmbedAnswer { vectors })
}

/// The peer with the fewest requests in flight, if it has fewer than this node.
fn pick() -> Option<(&'static Peers, &'static Peer)> {
    let ps = PEERS.get()?;
    let now = Instant::now();
    let local = LOCAL_IN_FLIGHT.load(Ordering::Relaxed);
    ps.peers
        .iter()
        .filter(|p| p.down_until.lock().unwrap().is_none_or(|t| t <= now))
        .min_by_key(|p| p.in_flight.load(Ordering::Relaxed))
        .filter(|p| p.in_flight.load(Ordering::Relaxed) < local)
        .map(|p| (ps, p))
}

/// `texts` embedded by a less busy peer, or `None` (none is, or it failed: the
/// caller embeds them here).
pub(crate) async fn try_peer(e: &Embedder, texts: &[String]) -> Option<Vec<Vec<f32>>> {
    let (ps, peer) = pick()?;
    peer.in_flight.fetch_add(1, Ordering::Relaxed);
    let body = EmbedRequest { model: e.model().to_string(), dimension: e.dimension(), texts: texts.to_vec() };
    let r = async {
        let r = ps.http.post(&peer.url).json(&body).send().await.map_err(|e| e.to_string())?;
        if !r.status().is_success() {
            return Err(r.text().await.unwrap_or_default());
        }
        r.json::<EmbedAnswer>().await.map_err(|e| e.to_string())
    }
    .await;
    peer.in_flight.fetch_sub(1, Ordering::Relaxed);
    match r {
        Ok(a) if a.vectors.len() == texts.len() && a.vectors.iter().all(|v| v.len() == e.dimension()) => Some(a.vectors),
        Ok(_) => {
            tracing::warn!(peer = %peer.url, "embedding peer answered the wrong shape; left out for 10 s");
            *peer.down_until.lock().unwrap() = Some(Instant::now() + PEER_REST);
            None
        }
        Err(err) => {
            tracing::warn!(peer = %peer.url, error = %err, "embedding peer failed; left out for 10 s");
            *peer.down_until.lock().unwrap() = Some(Instant::now() + PEER_REST);
            None
        }
    }
}

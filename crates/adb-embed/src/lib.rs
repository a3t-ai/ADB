//! ADB's text embeddings (MVP W4, `.ai/W4_RECALL.md`; providers W9.10).
//!
//! One model per deployment. By default **BAAI/bge-small-en-v1.5**, 384 dimensions,
//! run on ONNX Runtime on the CPU; or (W9.10) any OpenAI-compatible `/embeddings`
//! endpoint, chosen in ADB's settings ([`EmbedConfig`], `<data dir>/embedding.json`).
//! ADB embeds the `content` of SEMANTIC / EPISODIC / PROCEDURAL records before
//! logging them (the vector travels in the log as f16, ARCHITECTURE §9.1) and the
//! text of `LIKE $q` queries.
//!
//! The local model is a directory with `model.onnx`, `tokenizer.json`, `config.json`,
//! `special_tokens_map.json` and `tokenizer_config.json` (Hugging Face
//! `Xenova/bge-small-en-v1.5`, `onnx/model.onnx`), baked into the image at
//! [`DEFAULT_MODEL_DIR`]. `ADB_EMBED_MODEL_DIR` points elsewhere; `ADB_EMBED=off`
//! turns embedding off (records are then found by filters and text only).

pub mod peers;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fastembed::{InitOptionsUserDefined, Pooling, TextEmbedding, TokenizerFiles, UserDefinedEmbeddingModel};
use half::f16;
use serde::{Deserialize, Serialize};

/// Dimensions of the model's vectors (the Lance `embedding` column, D3 S5).
pub const DIM: usize = 384;

/// Where the image keeps the model.
pub const DEFAULT_MODEL_DIR: &str = "/models/bge-small-en-v1.5";

/// The model's name, recorded with the table (`_meta`) so a different model is noticed.
pub const MODEL: &str = "bge-small-en-v1.5";

/// Tokens embedded per text; longer `content` is truncated (bge's limit).
const MAX_TOKENS: usize = 512;

/// Where the embeddings come from (W9.10): ADB's settings, kept in the data
/// directory as `embedding.json`. Changing the model or the dimension clears the
/// table's vectors at the next start, and the backfill embeds everything again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbedConfig {
    pub provider: Provider,
    /// The model's name: recorded with the table, so a change is noticed.
    pub model: String,
    /// The vectors' length (the table's `embedding` column).
    pub dimension: usize,
    /// `openai`: the API base, e.g. `https://api.openai.com/v1` (ADB posts to
    /// `<url>/embeddings`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// `openai`: the environment variable holding the API key (from a Kubernetes
    /// Secret). ADB never stores the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// `local`: the model directory (default `ADB_EMBED_MODEL_DIR`, then the image's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_dir: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    /// The model on ADB's CPU (ONNX Runtime).
    Local,
    /// An OpenAI-compatible `/embeddings` endpoint (OpenAI, Azure, Ollama, vLLM, TEI…).
    Openai,
}

/// The settings file in a data directory.
pub const SETTINGS_FILE: &str = "embedding.json";

impl Default for EmbedConfig {
    fn default() -> Self {
        Self { provider: Provider::Local, model: MODEL.into(), dimension: DIM, url: None, api_key_env: None, model_dir: None }
    }
}

impl EmbedConfig {
    /// The settings saved in `data_dir`, if any.
    pub fn load(data_dir: &Path) -> Result<Option<Self>, String> {
        let p = data_dir.join(SETTINGS_FILE);
        match std::fs::read(&p) {
            Ok(b) => serde_json::from_slice(&b).map(Some).map_err(|e| format!("{}: {e}", p.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{}: {e}", p.display())),
        }
    }

    /// Save to `data_dir` atomically (temp file, fsync, rename).
    pub fn save(&self, data_dir: &Path) -> Result<(), String> {
        use std::io::Write as _;
        let p = data_dir.join(SETTINGS_FILE);
        let tmp = p.with_extension("json.tmp");
        let io = |e: std::io::Error| format!("{}: {e}", p.display());
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(&serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?).map_err(io)?;
        f.sync_all().map_err(io)?;
        std::fs::rename(&tmp, &p).map_err(io)?;
        if let Ok(d) = std::fs::File::open(data_dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    /// Settings that can't work, before anything is tried.
    pub fn check(&self) -> Result<(), String> {
        if self.model.trim().is_empty() {
            return Err("model: required".into());
        }
        if !(1..=8192).contains(&self.dimension) {
            return Err(format!("dimension {}: expected 1-8192", self.dimension));
        }
        if self.provider == Provider::Openai {
            let url = self.url.as_deref().unwrap_or("");
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err("url: an http(s) API base is required for the openai provider".into());
            }
            if let Some(var) = &self.api_key_env {
                if std::env::var(var).map_or(true, |v| v.is_empty()) {
                    return Err(format!("api_key_env: {var} is not set in ADB's environment (add it from a Secret)"));
                }
            }
        }
        Ok(())
    }
}

enum Backend {
    Local(LocalModel),
    /// An embedding pool (`ADB_EMBED_URL`, PF-49): `adb serve-embed` processes on other
    /// machines, taken in turn; the database nodes then run no model.
    Pool { client: reqwest::Client, urls: Vec<String>, next: std::sync::atomic::AtomicUsize },
    Http { client: reqwest::Client, url: String, key: Option<String> },
}

/// Texts and where their vectors go (and when the job was queued).
type Job = (Vec<String>, tokio::sync::oneshot::Sender<Result<Vec<Vec<f32>>, String>>, std::time::Instant);

/// Texts the local model runs in one call: concurrent writers' texts share a call
/// (C-17), which costs about what one text alone does.
const LOCAL_BATCH: usize = 64;

/// The local model, run by `ADB_EMBED_WORKERS` threads (`adb-embed-N`), each with its
/// own session, taking jobs from one queue: a worker takes every request queued (up
/// to [`LOCAL_BATCH`] texts) and runs them in one call. Behind a lock, concurrent
/// STOREs embedded one text at a time: ~100 writes/s on 2 vCPUs (AWS F-19). With one
/// worker, its thread (tokenising, the session's own steps) capped STORE at ~600/s on
/// 8 vCPUs while the other cores idled (AWS r7).
struct LocalModel {
    lanes: Arc<Lanes>,
}

/// The two queues of jobs: foreground (a writer or a reader waiting), background
/// (vectors that follow a commit: deferred embeddings, the backfill). A worker takes
/// foreground jobs whenever any wait, and never mixes background texts into their
/// call: a foreground request waits for at most the background call in progress, not
/// for the backlog (Hetzner p15: SEMANTIC writes waited 2.3 s behind a million
/// deferred episodes).
struct Lanes {
    queues: std::sync::Mutex<[std::collections::VecDeque<Job>; 2]>,
    ready: std::sync::Condvar,
    closed: std::sync::atomic::AtomicBool,
}

const FOREGROUND: usize = 0;
const BACKGROUND: usize = 1;

impl LocalModel {
    fn start(models: Vec<TextEmbedding>) -> Result<Self, String> {
        let lanes = Arc::new(Lanes { queues: Default::default(), ready: std::sync::Condvar::new(), closed: Default::default() });
        for (i, mut model) in models.into_iter().enumerate() {
            let lanes = Arc::clone(&lanes);
            std::thread::Builder::new()
                .name(format!("adb-embed-{i}"))
                .spawn(move || loop {
                    // Ends when the embedder is dropped and nothing is left queued.
                    let jobs = {
                        let mut q = lanes.queues.lock().unwrap();
                        loop {
                            let lane = match (q[FOREGROUND].is_empty(), q[BACKGROUND].is_empty()) {
                                (false, _) => FOREGROUND,
                                (true, false) => BACKGROUND,
                                (true, true) => {
                                    if lanes.closed.load(std::sync::atomic::Ordering::SeqCst) {
                                        return;
                                    }
                                    q = lanes.ready.wait(q).unwrap();
                                    continue;
                                }
                            };
                            let mut n = 0;
                            let mut jobs = Vec::new();
                            while n < LOCAL_BATCH {
                                match q[lane].pop_front() {
                                    Some(j) => {
                                        n += j.0.len();
                                        jobs.push(j);
                                    }
                                    None => break,
                                }
                            }
                            break jobs;
                        }
                    };
                    for j in &jobs {
                        adb_core::steps::add("embed.queue wait", j.2.elapsed());
                    }
                    adb_core::steps::value("embed.texts per model call", jobs.iter().map(|j| j.0.len() as u64).sum());
                    let _t = adb_core::steps::timer("embed.model call");
                    run(&mut model, jobs);
                })
                .map_err(|e| format!("embedding thread: {e}"))?;
        }
        Ok(Self { lanes })
    }

    fn submit(&self, texts: Vec<String>, background: bool) -> Result<tokio::sync::oneshot::Receiver<Result<Vec<Vec<f32>>, String>>, String> {
        let (done, wait) = tokio::sync::oneshot::channel();
        if self.lanes.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("embedding thread stopped".to_string());
        }
        self.lanes.queues.lock().unwrap()[if background { BACKGROUND } else { FOREGROUND }].push_back((texts, done, std::time::Instant::now()));
        self.lanes.ready.notify_one();
        Ok(wait)
    }
}

impl Drop for LocalModel {
    fn drop(&mut self) {
        self.lanes.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.lanes.ready.notify_all();
    }
}

/// Embed the texts of `jobs` in one call and answer each job.
fn run(model: &mut TextEmbedding, jobs: Vec<Job>) {
    let texts: Vec<&String> = jobs.iter().flat_map(|(t, _, _)| t.iter()).collect();
    let out = match texts.is_empty() {
        true => Ok(Vec::new()),
        false => model.embed(&texts, Some(LOCAL_BATCH)).map_err(|e| format!("embedding: {e}")),
    };
    match out {
        Ok(mut out) => {
            out.iter_mut().for_each(|v| normalize(v));
            let mut out = out.into_iter();
            for (t, done, _) in jobs {
                let _ = done.send(Ok(out.by_ref().take(t.len()).collect()));
            }
        }
        Err(e) => {
            for (_, done, _) in jobs {
                let _ = done.send(Err(e.clone()));
            }
        }
    }
}

/// The embedding model, loaded once. A local model runs on its own thread, batching
/// concurrent requests; call [`Embedder::embed`].
pub struct Embedder {
    backend: Backend,
    model: String,
    dim: usize,
}

impl Embedder {
    /// Load the local model files in `dir`, running inference on `threads` CPU threads.
    pub fn load(dir: &Path, threads: usize) -> Result<Self, String> {
        let read = |f: &str| std::fs::read(dir.join(f)).map_err(|e| format!("embedding model: {}: {e}", dir.join(f).display()));
        let mut models = Vec::new();
        for _ in 0..workers() {
            let tokenizer = TokenizerFiles {
                tokenizer_file: read("tokenizer.json")?,
                config_file: read("config.json")?,
                special_tokens_map_file: read("special_tokens_map.json")?,
                tokenizer_config_file: read("tokenizer_config.json")?,
            };
            // bge models embed the [CLS] token (fastembed's default for this model).
            let model = UserDefinedEmbeddingModel::new(read("model.onnx")?, tokenizer).with_pooling(Pooling::Cls);
            let options = InitOptionsUserDefined::new().with_max_length(MAX_TOKENS).with_intra_threads(threads.max(1));
            models.push(TextEmbedding::try_new_from_user_defined(model, options).map_err(|e| format!("embedding model: {e}"))?);
        }
        Ok(Self { backend: Backend::Local(LocalModel::start(models)?), model: MODEL.into(), dim: DIM })
    }

    /// The embedder `cfg` describes (W9.10). A local model is loaded now; an
    /// endpoint isn't called until the first embedding ([`Embedder::verify`] does).
    pub fn from_config(cfg: &EmbedConfig) -> Result<Self, String> {
        cfg.check()?;
        match cfg.provider {
            Provider::Local => {
                if let Some(mut pool) = Self::pool_from_env() {
                    pool.model = cfg.model.clone();
                    pool.dim = cfg.dimension;
                    return Ok(pool);
                }
                let dir = cfg
                    .model_dir
                    .clone()
                    .or_else(|| std::env::var("ADB_EMBED_MODEL_DIR").ok())
                    .unwrap_or_else(|| DEFAULT_MODEL_DIR.into());
                let mut e = Self::load(Path::new(&dir), threads())?;
                e.model = cfg.model.clone();
                e.dim = cfg.dimension;
                Ok(e)
            }
            Provider::Openai => {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .map_err(|e| format!("embedding endpoint: {e}"))?;
                let url = cfg.url.clone().unwrap_or_default().trim_end_matches('/').to_string();
                let key = cfg.api_key_env.as_ref().and_then(|v| std::env::var(v).ok());
                Ok(Self { backend: Backend::Http { client, url, key }, model: cfg.model.clone(), dim: cfg.dimension })
            }
        }
    }

    /// Embed a sample and check the vector's length is the configured dimension.
    pub async fn verify(self: &Arc<Self>) -> Result<(), String> {
        let v = self.embed_here(vec!["ADB embedding check".into()]).await?;
        match v.first().map(Vec::len) {
            Some(n) if n == self.dim => Ok(()),
            Some(n) => Err(format!("the model returns {n} dimensions; the settings say {}", self.dim)),
            None => Err("the model returned no vector".into()),
        }
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn dimension(&self) -> usize {
        self.dim
    }

    /// The process's embedder from the environment, loaded once ([`Embedder::from_env`]).
    pub fn shared() -> Option<Arc<Self>> {
        static SHARED: std::sync::OnceLock<Option<Arc<Embedder>>> = std::sync::OnceLock::new();
        SHARED.get_or_init(Self::from_env).clone()
    }

    /// The embedding pool of `ADB_EMBED_URL` (comma-separated `adb serve-embed` URLs),
    /// if set: the same model and dimension as the local one, run elsewhere (PF-49).
    pub fn pool_from_env() -> Option<Self> {
        let urls: Vec<String> = std::env::var("ADB_EMBED_URL").ok()?.split(',').map(|u| u.trim().trim_end_matches('/').to_string()).filter(|u| !u.is_empty()).collect();
        if urls.is_empty() {
            return None;
        }
        let client = reqwest::Client::builder().connect_timeout(std::time::Duration::from_millis(500)).timeout(std::time::Duration::from_secs(30)).build().ok()?;
        tracing::info!(pool = ?urls, model = MODEL, "embeddings from the embedding pool (ADB_EMBED_URL); no model on this node");
        Some(Self { backend: Backend::Pool { client, urls, next: Default::default() }, model: MODEL.into(), dim: DIM })
    }

    /// Whether `ADB_EMBED=off` turns embedding off.
    pub fn disabled() -> bool {
        std::env::var("ADB_EMBED").is_ok_and(|v| v.eq_ignore_ascii_case("off"))
    }

    /// The embedder this process should use without settings: `None` when
    /// `ADB_EMBED=off`, or when the model directory is missing (logged; ADB then
    /// runs without vectors).
    pub fn from_env() -> Option<Arc<Self>> {
        if Self::disabled() {
            tracing::info!("embeddings off (ADB_EMBED=off)");
            return None;
        }
        if let Some(pool) = Self::pool_from_env() {
            return Some(Arc::new(pool));
        }
        let dir: PathBuf = std::env::var("ADB_EMBED_MODEL_DIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from(DEFAULT_MODEL_DIR));
        if !dir.join("model.onnx").exists() {
            tracing::warn!(dir = %dir.display(), "no embedding model: records get no vectors and LIKE finds nothing (set ADB_EMBED_MODEL_DIR)");
            return None;
        }
        let threads = threads();
        let t = std::time::Instant::now();
        match Self::load(&dir, threads) {
            Ok(e) => {
                tracing::info!(model = MODEL, dir = %dir.display(), threads, workers = workers(), took = ?t.elapsed(), "embedding model loaded");
                Some(Arc::new(e))
            }
            Err(e) => {
                tracing::error!(error = %e, "embedding model failed to load; running without vectors");
                None
            }
        }
    }

    /// Embed `texts` on this node's own model (never a peer's); an endpoint's as
    /// [`Embedder::embed`].
    pub async fn embed_here(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        self.embed_here_as(texts, false).await
    }

    /// [`Self::embed_here`] in the background lane: after every foreground request.
    pub async fn embed_here_background(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        self.embed_here_as(texts, true).await
    }

    async fn embed_here_as(&self, texts: Vec<String>, background: bool) -> Result<Vec<Vec<f32>>, String> {
        match &self.backend {
            Backend::Local(model) => {
                peers::LOCAL_IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let r = match model.submit(texts, background) {
                    Ok(wait) => wait.await.map_err(|_| "embedding thread stopped".to_string()).and_then(|r| r),
                    Err(e) => Err(e),
                };
                peers::LOCAL_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                r
            }
            Backend::Pool { client, urls, next } => {
                let t = std::time::Instant::now();
                let mut out = Vec::with_capacity(texts.len());
                for chunk in texts.chunks(HTTP_BATCH) {
                    let mut last = String::new();
                    let mut done = false;
                    for _ in 0..urls.len() {
                        let url = &urls[next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % urls.len()];
                        match http_embed(client, url, None, &self.model, chunk, background).await {
                            Ok(v) => {
                                out.extend(v);
                                done = true;
                                break;
                            }
                            Err(e) => last = e,
                        }
                    }
                    if !done {
                        return Err(format!("embedding pool: {last}"));
                    }
                }
                adb_core::steps::add(if background { "embed.pool round trip (background)" } else { "embed.pool round trip" }, t.elapsed());
                Ok(out)
            }
            Backend::Http { client, url, key } => {
                let mut out = Vec::with_capacity(texts.len());
                for chunk in texts.chunks(HTTP_BATCH) {
                    out.extend(http_embed(client, url, key.as_deref(), &self.model, chunk, false).await?);
                }
                Ok(out)
            }
        }
    }

    /// Embed `texts` with the local model, waiting on this thread (unit-length vectors).
    /// Not from async code: use [`Embedder::embed`].
    pub fn embed_blocking(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let Backend::Local(model) = &self.backend else { return Err("embed_blocking: not a local model".into()) };
        model.submit(texts.to_vec(), false)?.blocking_recv().map_err(|_| "embedding thread stopped".to_string())?
    }

    /// Embed `texts`: unit-length vectors of the configured dimension.
    pub async fn embed(self: &Arc<Self>, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        let out = self.embed_raw(texts, false).await?;
        self.check_dim(out)
    }

    /// [`Self::embed`] for vectors nobody is waiting on (deferred embeddings, the
    /// backfill): the background lane, after every foreground request.
    pub async fn embed_background(self: &Arc<Self>, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        let out = self.embed_raw(texts, true).await?;
        self.check_dim(out)
    }

    fn check_dim(&self, out: Vec<Vec<f32>>) -> Result<Vec<Vec<f32>>, String> {
        if let Some(v) = out.iter().find(|v| v.len() != self.dim) {
            return Err(format!("embedding: the model returned {} dimensions, not {}", v.len(), self.dim));
        }
        Ok(out)
    }

    async fn embed_raw(self: &Arc<Self>, texts: Vec<String>, background: bool) -> Result<Vec<Vec<f32>>, String> {
        if let (Backend::Local(_), false) = (&self.backend, background) {
            // A less busy node of the replica set embeds them, when there is one.
            let t = std::time::Instant::now();
            if let Some(v) = peers::try_peer(self, &texts).await {
                adb_core::steps::add("embed.on a peer (round trip)", t.elapsed());
                return Ok(v);
            }
            let t = std::time::Instant::now();
            let r = self.embed_here_as(texts, false).await;
            adb_core::steps::add("embed.here (queue + model)", t.elapsed());
            return r;
        }
        self.embed_here_as(texts, background).await
    }
}

/// Texts per request to an endpoint.
const HTTP_BATCH: usize = 64;

/// `POST <url>/embeddings` (the OpenAI wire format), vectors in input order.
/// The header `adb serve-embed` reads: `background` queues the request behind every
/// foreground one.
pub const PRIORITY_HEADER: &str = "x-adb-embed-priority";

async fn http_embed(client: &reqwest::Client, url: &str, key: Option<&str>, model: &str, texts: &[String], background: bool) -> Result<Vec<Vec<f32>>, String> {
    #[derive(Deserialize)]
    struct Item {
        #[serde(default)]
        index: usize,
        embedding: Vec<f32>,
    }
    #[derive(Deserialize)]
    struct Resp {
        data: Vec<Item>,
    }
    let mut req = client.post(format!("{url}/embeddings")).json(&serde_json::json!({ "model": model, "input": texts }));
    if background {
        req = req.header(PRIORITY_HEADER, "background");
    }
    if let Some(k) = key {
        req = req.bearer_auth(k);
    }
    let resp = req.send().await.map_err(|e| format!("embedding endpoint: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("embedding endpoint: HTTP {status}: {}", body.chars().take(300).collect::<String>()));
    }
    let mut r: Resp = resp.json().await.map_err(|e| format!("embedding endpoint: unexpected response: {e}"))?;
    if r.data.len() != texts.len() {
        return Err(format!("embedding endpoint: {} vectors for {} texts", r.data.len(), texts.len()));
    }
    r.data.sort_by_key(|i| i.index);
    Ok(r.data.into_iter().map(|mut i| {
        normalize(&mut i.embedding);
        i.embedding
    }).collect())
}

/// ONNX threads per worker (`ADB_EMBED_THREADS`, default 2).
fn threads() -> usize {
    std::env::var("ADB_EMBED_THREADS").ok().and_then(|t| t.parse().ok()).unwrap_or(2)
}

/// Embedding workers, each its own session (`ADB_EMBED_WORKERS`; default one per 4
/// CPUs, 1 to 4). Each holds a copy of the model (~130 MB for bge-small).
pub fn workers() -> usize {
    std::env::var("ADB_EMBED_WORKERS")
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get() / 4).clamp(1, 4))
        .max(1)
}

fn normalize(v: &mut [f32]) {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        v.iter_mut().for_each(|x| *x /= n);
    }
}

/// A vector as the log stores it (S9: f16 halves the log; ≤ 0.5 % recall@10).
pub fn to_f16(v: &[f32]) -> Vec<f16> {
    v.iter().map(|x| f16::from_f32(*x)).collect()
}

/// Cosine similarity of two vectors (1 = same direction).
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Needs the model: `ADB_EMBED_MODEL_DIR` (tests/fetch-model.sh puts it in
    /// $BUILD_DIR/models/bge-small-en-v1.5). Skipped without it.
    fn model() -> Option<Embedder> {
        let dir = PathBuf::from(std::env::var("ADB_EMBED_MODEL_DIR").ok()?);
        Some(Embedder::load(&dir, 2).expect("model loads"))
    }

    #[test]
    fn related_texts_are_closer_than_unrelated_ones() {
        let Some(e) = model() else { return eprintln!("no ADB_EMBED_MODEL_DIR; skipped") };
        let v = e
            .embed_blocking(&[
                "Acme's contract renews on 1 November".into(),
                "When is the Acme renewal date?".into(),
                "The payments pod ran out of memory".into(),
            ])
            .unwrap();
        assert_eq!(v.len(), 3);
        assert!(v.iter().all(|x| x.len() == DIM));
        assert!((v[0].iter().map(|x| x * x).sum::<f32>() - 1.0).abs() < 1e-3, "unit length");
        let (related, unrelated) = (cosine(&v[0], &v[1]), cosine(&v[0], &v[2]));
        assert!(related > unrelated + 0.1, "related {related} vs unrelated {unrelated}");
    }

    /// An OpenAI-compatible endpoint: vectors come back in input order, unit length,
    /// with the key as a bearer token; a wrong dimension is caught by `verify`.
    #[tokio::test(flavor = "multi_thread")]
    async fn openai_compatible_endpoint() {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route(
            "/v1/embeddings",
            post(|headers: axum::http::HeaderMap, Json(body): Json<serde_json::Value>| async move {
                assert_eq!(headers.get("authorization").unwrap(), "Bearer sk-test");
                let n = body["input"].as_array().unwrap().len();
                // Reversed order, as the API allows: `index` says where each goes.
                let data: Vec<_> = (0..n).rev().map(|i| serde_json::json!({ "index": i, "embedding": [i as f32 + 1.0, 0.0, 0.0, 0.0] })).collect();
                Json(serde_json::json!({ "data": data }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        std::env::set_var("ADB_TEST_EMBED_KEY", "sk-test");
        let cfg = EmbedConfig {
            provider: Provider::Openai,
            model: "test-model".into(),
            dimension: 4,
            url: Some(format!("http://{addr}/v1/")),
            api_key_env: Some("ADB_TEST_EMBED_KEY".into()),
            model_dir: None,
        };
        let e = Arc::new(Embedder::from_config(&cfg).unwrap());
        e.verify().await.unwrap();
        let v = e.embed(vec!["a".into(), "b".into(), "c".into()]).await.unwrap();
        assert_eq!(v.iter().map(|x| x[0]).collect::<Vec<_>>(), vec![1.0, 1.0, 1.0], "unit length");
        assert_eq!(v.len(), 3);

        let wrong = Arc::new(Embedder::from_config(&EmbedConfig { dimension: 8, ..cfg.clone() }).unwrap());
        assert!(wrong.verify().await.unwrap_err().contains("4 dimensions"));
        let missing = EmbedConfig { api_key_env: Some("ADB_TEST_NO_SUCH_KEY".into()), ..cfg };
        assert!(Embedder::from_config(&missing).err().unwrap().contains("not set"));
    }

    #[test]
    fn settings_round_trip() {
        let dir = tempfile_dir();
        assert_eq!(EmbedConfig::load(&dir).unwrap(), None);
        let c = EmbedConfig { provider: Provider::Openai, model: "m".into(), dimension: 1536, url: Some("https://x/v1".into()), api_key_env: None, model_dir: None };
        c.save(&dir).unwrap();
        assert_eq!(EmbedConfig::load(&dir).unwrap(), Some(c));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn tempfile_dir() -> PathBuf {
        let d = std::env::temp_dir().join(format!("adb-embed-test-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn f16_keeps_similarity() {
        let a = vec![0.1f32, -0.5, 0.25, 0.8];
        let back: Vec<f32> = to_f16(&a).iter().map(|x| x.to_f32()).collect();
        assert!(cosine(&a, &back) > 0.9999);
    }
}

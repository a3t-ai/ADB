//! ADB's text embeddings (MVP W4, `.ai/W4_RECALL.md`; providers W9.10; one live
//! setting 20 Nov 2026).
//!
//! One embedding setting per deployment ([`EmbedConfig`]), changed at runtime with
//! `PUT /admin/embedding` and applied without a restart ([`Embedder::reconfigure`]):
//!
//! - `local` (default): **BAAI/bge-small-en-v1.5**, 384 dimensions, on ONNX Runtime
//!   on the CPU; no network. What local dev and CE run with no setting at all.
//! - `openai`: OpenAI's `/v1/embeddings`; the setting names the model, the dimension
//!   (asked of the API) and the environment variable holding the key.
//! - `custom`: any OpenAI-compatible `/embeddings` endpoint at the setting's `url`
//!   (Azure, Ollama, vLLM, TEI, an in-house service).
//!
//! and `run_on`: `node` (in this process) or `compute` (on compute nodes, [`compute`],
//! which run the same provider and write the vector files). ADB never stores an API
//! key: the setting names the variable, filled from a Kubernetes Secret.
//!
//! The local model is a directory with `model.onnx`, `tokenizer.json`, `config.json`,
//! `special_tokens_map.json` and `tokenizer_config.json` (Hugging Face
//! `Xenova/bge-small-en-v1.5`, `onnx/model.onnx`), baked into the image at
//! [`DEFAULT_MODEL_DIR`]. `ADB_EMBED_MODEL_DIR` points elsewhere; `ADB_EMBED=off`
//! turns embedding off (records are then found by filters and text only).

pub mod compute;
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

/// The embedding setting (W9.10; `local` / `openai` / `custom` and `run_on`, 20 Nov
/// 2026). Kept as `embedding.json` by the system partition (a cluster) or the data
/// directory (one node). Changing the model or the dimension makes every vector
/// again, in the background.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbedConfig {
    pub provider: Provider,
    /// The model's name (also the name vector files are kept under).
    pub model: String,
    /// The vectors' length.
    pub dimension: usize,
    /// `custom`: the API base, e.g. `http://ollama:11434/v1` (ADB posts to
    /// `<url>/embeddings`). `openai`: optional, default [`OPENAI_URL`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// `openai` (required) / `custom` (optional): the environment variable holding the
    /// API key, from a Kubernetes Secret. ADB never stores the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// `local`: the model directory (default `ADB_EMBED_MODEL_DIR`, then the image's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_dir: Option<String>,
    /// `custom`: ask the endpoint for `dimension`-long vectors (its `dimensions`
    /// field). `openai` always asks.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub request_dimension: bool,
    /// Where the embedding runs: this process, or compute nodes.
    #[serde(default, skip_serializing_if = "RunOn::is_node")]
    pub run_on: RunOn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    /// The model on ADB's CPU (ONNX Runtime).
    Local,
    /// OpenAI's API.
    Openai,
    /// Any OpenAI-compatible `/embeddings` endpoint.
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunOn {
    /// In this process (database node, CE, local dev).
    #[default]
    Node,
    /// On compute nodes (`adb serve-compute`), which write the vector files.
    Compute,
}

impl RunOn {
    fn is_node(&self) -> bool {
        *self == RunOn::Node
    }
}

/// OpenAI's API base.
pub const OPENAI_URL: &str = "https://api.openai.com/v1";

/// The settings file in a data directory.
pub const SETTINGS_FILE: &str = "embedding.json";

impl Default for EmbedConfig {
    fn default() -> Self {
        Self { provider: Provider::Local, model: MODEL.into(), dimension: DIM, url: None, api_key_env: None, model_dir: None, request_dimension: false, run_on: RunOn::Node }
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

    /// Settings that can't work, before anything is tried. The key's variable is
    /// checked where the embedding runs ([`Embedder::from_config`]).
    pub fn check(&self) -> Result<(), String> {
        if self.model.trim().is_empty() {
            return Err("model: required".into());
        }
        if !(1..=8192).contains(&self.dimension) {
            return Err(format!("dimension {}: expected 1-8192", self.dimension));
        }
        let http = |u: &str| u.starts_with("http://") || u.starts_with("https://");
        match self.provider {
            Provider::Local => {}
            Provider::Openai => {
                if self.url.as_deref().is_some_and(|u| !http(u)) {
                    return Err("url: an http(s) API base".into());
                }
                if self.api_key_env.as_deref().is_none_or(|v| v.trim().is_empty()) {
                    return Err("api_key_env: the variable holding the OpenAI key is required".into());
                }
            }
            Provider::Custom => {
                if !self.url.as_deref().is_some_and(http) {
                    return Err("url: an http(s) API base is required for the custom provider".into());
                }
            }
        }
        Ok(())
    }

    /// The setting a compute node runs: the same provider, in its own process.
    pub fn on_node(&self) -> Self {
        Self { run_on: RunOn::Node, ..self.clone() }
    }

    /// The endpoint, its key's variable, and whether to ask for the dimension.
    fn endpoint(&self) -> Option<(String, Option<&str>, bool)> {
        match self.provider {
            Provider::Local => None,
            Provider::Openai => Some((self.url.clone().unwrap_or_else(|| OPENAI_URL.into()), self.api_key_env.as_deref(), true)),
            Provider::Custom => Some((self.url.clone().unwrap_or_default(), self.api_key_env.as_deref(), self.request_dimension)),
        }
    }
}

enum Backend {
    Local(Arc<LocalModel>),
    /// Compute nodes ([`compute`]): `adb serve-compute` processes on other machines,
    /// the least busy first; this node then runs no model.
    Compute { client: reqwest::Client },
    Http { client: reqwest::Client, url: String, key: Option<String>, dimensions: Option<usize> },
    /// The setting can't run here (no local model files): embedding fails until the
    /// setting changes (logged once).
    Unavailable(String),
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

/// The local model's files in `dir`, loaded once per process and directory (a
/// setting switched back to `local` doesn't load them again).
fn local_model(dir: &Path) -> Result<Arc<LocalModel>, String> {
    static LOADED: std::sync::Mutex<Vec<(PathBuf, Arc<LocalModel>)>> = std::sync::Mutex::new(Vec::new());
    let mut loaded = LOADED.lock().unwrap();
    if let Some((_, m)) = loaded.iter().find(|(d, _)| d == dir) {
        return Ok(Arc::clone(m));
    }
    if !dir.join("model.onnx").exists() {
        return Err(format!("no embedding model in {} (set ADB_EMBED_MODEL_DIR)", dir.display()));
    }
    let threads = threads();
    let t = std::time::Instant::now();
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
    let m = Arc::new(LocalModel::start(models)?);
    tracing::info!(model = MODEL, dir = %dir.display(), threads, workers = workers(), took = ?t.elapsed(), "embedding model loaded");
    loaded.push((dir.to_path_buf(), Arc::clone(&m)));
    Ok(m)
}

/// What an [`Embedder`] runs now.
struct Active {
    cfg: EmbedConfig,
    backend: Backend,
}

impl Active {
    fn build(cfg: &EmbedConfig) -> Result<Self, String> {
        cfg.check()?;
        let backend = match (cfg.run_on, cfg.endpoint()) {
            (RunOn::Compute, _) => Backend::Compute {
                client: reqwest::Client::builder().connect_timeout(std::time::Duration::from_millis(500)).timeout(std::time::Duration::from_secs(30)).build().map_err(|e| e.to_string())?,
            },
            (RunOn::Node, None) => {
                let dir = cfg.model_dir.clone().or_else(|| std::env::var("ADB_EMBED_MODEL_DIR").ok()).unwrap_or_else(|| DEFAULT_MODEL_DIR.into());
                match local_model(Path::new(&dir)) {
                    Ok(m) => Backend::Local(m),
                    Err(e) => Backend::Unavailable(e),
                }
            }
            (RunOn::Node, Some((url, key_env, ask))) => {
                let key = match key_env {
                    Some(v) => Some(std::env::var(v).ok().filter(|k| !k.is_empty()).ok_or_else(|| format!("api_key_env: {v} is not set in ADB's environment (add it from a Secret)"))?),
                    None => None,
                };
                let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(30)).build().map_err(|e| format!("embedding endpoint: {e}"))?;
                Backend::Http { client, url: url.trim_end_matches('/').to_string(), key, dimensions: ask.then_some(cfg.dimension) }
            }
        };
        Ok(Self { cfg: cfg.clone(), backend })
    }
}

/// The embeddings of this process: one per process ([`Embedder::process`]), shared by
/// every partition, reconfigured in place when the setting changes
/// ([`Embedder::reconfigure`]). A local model runs on its own threads, batching
/// concurrent requests; call [`Embedder::embed`].
pub struct Embedder {
    active: std::sync::RwLock<Arc<Active>>,
}

impl Embedder {
    /// An embedder for `cfg`. A local model is loaded now (once per process); an
    /// endpoint isn't called until the first embedding ([`Embedder::verify`] does).
    pub fn from_config(cfg: &EmbedConfig) -> Result<Self, String> {
        let a = Active::build(cfg)?;
        if let Backend::Unavailable(e) = &a.backend {
            return Err(e.clone());
        }
        Ok(Self { active: std::sync::RwLock::new(Arc::new(a)) })
    }

    /// The process's embedder (default setting until [`Self::reconfigure`]): `None`
    /// only with `ADB_EMBED=off`. Without the local model files it exists but fails to
    /// embed (logged) until a setting that can run here is applied.
    pub fn process() -> Option<Arc<Self>> {
        static PROCESS: std::sync::OnceLock<Option<Arc<Embedder>>> = std::sync::OnceLock::new();
        PROCESS
            .get_or_init(|| {
                if Self::disabled() {
                    tracing::info!("embeddings off (ADB_EMBED=off)");
                    return None;
                }
                let a = Active::build(&EmbedConfig::default()).expect("the default setting is valid");
                if let Backend::Unavailable(e) = &a.backend {
                    tracing::warn!(error = %e, "no local embedding model: records get no vectors and LIKE ranks by words until an embedding setting that can run is applied");
                }
                Some(Arc::new(Self { active: std::sync::RwLock::new(Arc::new(a)) }))
            })
            .clone()
    }

    fn active(&self) -> Arc<Active> {
        Arc::clone(&self.active.read().unwrap())
    }

    /// Apply `cfg` in place (every holder of this embedder sees it). An endpoint is
    /// tried with one sample first; a setting that fails leaves the old one. Returns
    /// whether the model or dimension changed (the vectors are then made again).
    pub async fn reconfigure(&self, cfg: &EmbedConfig) -> Result<bool, String> {
        if self.active().cfg == *cfg {
            return Ok(false);
        }
        let next = Arc::new(Active::build(cfg)?);
        if let Backend::Unavailable(e) = &next.backend {
            return Err(e.clone());
        }
        if !matches!(next.backend, Backend::Compute { .. }) {
            let v = Self::embed_with(&next, vec!["ADB embedding check".into()], false).await?;
            if v.first().map(Vec::len) != Some(cfg.dimension) {
                return Err(format!("the model returns {} dimensions; the setting says {}", v.first().map_or(0, Vec::len), cfg.dimension));
            }
        }
        let prev = std::mem::replace(&mut *self.active.write().unwrap(), next);
        let changed = prev.cfg.model != cfg.model || prev.cfg.dimension != cfg.dimension;
        tracing::info!(provider = ?cfg.provider, model = %cfg.model, dimension = cfg.dimension, run_on = ?cfg.run_on, reembed = changed, "embedding setting applied");
        Ok(changed)
    }

    /// The setting in force.
    pub fn config(&self) -> EmbedConfig {
        self.active().cfg.clone()
    }

    /// Embed a sample and check the vector's length is the configured dimension.
    pub async fn verify(self: &Arc<Self>) -> Result<(), String> {
        let a = self.active();
        let v = Self::embed_with(&a, vec!["ADB embedding check".into()], false).await?;
        match v.first().map(Vec::len) {
            Some(n) if n == a.cfg.dimension => Ok(()),
            Some(n) => Err(format!("the model returns {n} dimensions; the settings say {}", a.cfg.dimension)),
            None => Err("the model returned no vector".into()),
        }
    }

    pub fn model(&self) -> String {
        self.active().cfg.model.clone()
    }

    pub fn dimension(&self) -> usize {
        self.active().cfg.dimension
    }

    /// Whether this embedder runs on compute nodes.
    pub fn on_compute(&self) -> bool {
        matches!(self.active().backend, Backend::Compute { .. })
    }

    /// Whether it embeds through an endpoint (OpenAI, custom).
    pub fn on_endpoint(&self) -> bool {
        matches!(self.active().backend, Backend::Http { .. })
    }

    /// Whether `ADB_EMBED=off` turns embedding off.
    pub fn disabled() -> bool {
        std::env::var("ADB_EMBED").is_ok_and(|v| v.eq_ignore_ascii_case("off"))
    }

    /// Embed `texts` on this node's own model (never a peer's); an endpoint's as
    /// [`Embedder::embed`].
    pub async fn embed_here(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        Self::embed_with(&self.active(), texts, false).await
    }

    /// [`Self::embed_here`] in the background lane: after every foreground request.
    pub async fn embed_here_background(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        Self::embed_with(&self.active(), texts, true).await
    }

    async fn embed_with(a: &Active, texts: Vec<String>, background: bool) -> Result<Vec<Vec<f32>>, String> {
        let (model, dim) = (a.cfg.model.as_str(), a.cfg.dimension);
        match &a.backend {
            Backend::Unavailable(e) => Err(e.clone()),
            Backend::Local(m) => {
                peers::LOCAL_IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let r = match m.submit(texts, background) {
                    Ok(wait) => wait.await.map_err(|_| "embedding thread stopped".to_string()).and_then(|r| r),
                    Err(e) => Err(e),
                };
                peers::LOCAL_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                r
            }
            Backend::Compute { client } => {
                let t = std::time::Instant::now();
                let mut out = Vec::with_capacity(texts.len());
                for chunk in texts.chunks(HTTP_BATCH) {
                    let mut tried: Vec<String> = Vec::new();
                    let mut last = String::from("no compute node is up");
                    loop {
                        let Some(lease) = compute::pick(model, dim, &tried) else {
                            return Err(format!("compute nodes: {last}"));
                        };
                        match http_embed(client, &lease.url, None, model, None, chunk, background).await {
                            Ok(v) => {
                                lease.done();
                                out.extend(v);
                                break;
                            }
                            Err(e) => {
                                tracing::debug!(url = %lease.url, error = %e, "compute node failed an embedding; trying another");
                                tried.push(lease.url.clone());
                                last = e;
                            }
                        }
                    }
                }
                adb_core::steps::add(if background { "compute.embed round trip (background)" } else { "compute.embed round trip" }, t.elapsed());
                Ok(out)
            }
            Backend::Http { client, url, key, dimensions } => {
                let t = std::time::Instant::now();
                let mut out = Vec::with_capacity(texts.len());
                for chunk in texts.chunks(ENDPOINT_BATCH) {
                    out.extend(http_embed(client, url, key.as_deref(), model, *dimensions, chunk, false).await?);
                }
                adb_core::steps::add("endpoint.embed round trip", t.elapsed());
                Ok(out)
            }
        }
    }

    /// Have a compute node embed `items` (row id, text) and write the vectors as one
    /// vector file in `location` (`adb_store::vectors`); returns the file's URI. The
    /// vectors don't come back. `None`: this embedder doesn't use compute nodes (the
    /// caller embeds and writes the file itself).
    pub async fn embed_to_file(&self, location: &str, items: &[(String, String)]) -> Option<Result<String, String>> {
        let a = self.active();
        let Backend::Compute { client } = &a.backend else { return None };
        let (model, dim) = (a.cfg.model.as_str(), a.cfg.dimension);
        let t = std::time::Instant::now();
        let body = serde_json::json!({ "location": location, "model": model, "dimension": dim, "items": items });
        let mut tried: Vec<String> = Vec::new();
        let mut last = String::from("no compute node is up");
        let r = loop {
            let Some(lease) = compute::pick(model, dim, &tried) else {
                break Err(format!("compute nodes: {last}"));
            };
            let sent = client.post(format!("{}/vectors", lease.url.trim_end_matches('/'))).header(PRIORITY_HEADER, "background").json(&body).send().await;
            let answer = match sent {
                Ok(r) if r.status().is_success() => r.json::<serde_json::Value>().await.map_err(|e| e.to_string()),
                Ok(r) => Err(format!("{}: {}", r.status(), r.text().await.unwrap_or_default())),
                Err(e) => Err(e.to_string()),
            };
            match answer.and_then(|a| a.get("uri").and_then(|u| u.as_str()).map(String::from).ok_or_else(|| "no uri in the answer".to_string())) {
                Ok(uri) => {
                    lease.done();
                    break Ok(uri);
                }
                Err(e) => {
                    tracing::debug!(url = %lease.url, error = %e, "compute node failed a vector file; trying another");
                    tried.push(lease.url.clone());
                    last = e;
                }
            }
        };
        adb_core::steps::add("compute.vectors file round trip", t.elapsed());
        Some(r)
    }

    /// Embed `texts` with the local model, waiting on this thread (unit-length vectors).
    /// Not from async code: use [`Embedder::embed`].
    pub fn embed_blocking(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let a = self.active();
        let Backend::Local(model) = &a.backend else { return Err("embed_blocking: not a local model".into()) };
        model.submit(texts.to_vec(), false)?.blocking_recv().map_err(|_| "embedding thread stopped".to_string())?
    }

    /// Embed `texts`: unit-length vectors of the configured dimension.
    pub async fn embed(self: &Arc<Self>, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        self.embed_raw(texts, false).await
    }

    /// [`Self::embed`] for vectors nobody is waiting on (deferred embeddings, the
    /// backfill): the background lane, after every foreground request.
    pub async fn embed_background(self: &Arc<Self>, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        self.embed_raw(texts, true).await
    }

    async fn embed_raw(self: &Arc<Self>, texts: Vec<String>, background: bool) -> Result<Vec<Vec<f32>>, String> {
        let a = self.active();
        let out = if let (Backend::Local(_), false) = (&a.backend, background) {
            // A less busy node of the replica set embeds them, when there is one.
            let t = std::time::Instant::now();
            if let Some(v) = peers::try_peer(self, &texts).await {
                adb_core::steps::add("embed.on a peer (round trip)", t.elapsed());
                v
            } else {
                let t = std::time::Instant::now();
                let r = Self::embed_with(&a, texts, false).await;
                adb_core::steps::add("embed.here (queue + model)", t.elapsed());
                r?
            }
        } else {
            Self::embed_with(&a, texts, background).await?
        };
        if let Some(v) = out.iter().find(|v| v.len() != a.cfg.dimension) {
            return Err(format!("embedding: the model returned {} dimensions, not {}", v.len(), a.cfg.dimension));
        }
        Ok(out)
    }
}

/// Texts per request to a compute node.
const HTTP_BATCH: usize = 64;
/// Texts per request to an embedding endpoint (OpenAI takes up to 2048): one request
/// per batch counts once against a requests-per-minute limit.
const ENDPOINT_BATCH: usize = 512;

/// `POST <url>/embeddings` (the OpenAI wire format), vectors in input order.
/// The header `adb serve-compute` reads: `background` queues the request behind every
/// foreground one.
pub const PRIORITY_HEADER: &str = "x-adb-embed-priority";

async fn http_embed(client: &reqwest::Client, url: &str, key: Option<&str>, model: &str, dimensions: Option<usize>, texts: &[String], background: bool) -> Result<Vec<Vec<f32>>, String> {
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
    let mut body = serde_json::json!({ "model": model, "input": texts });
    if let Some(d) = dimensions {
        body["dimensions"] = d.into();
    }
    let mut req = client.post(format!("{url}/embeddings")).json(&body);
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

/// Requests this process's own model is running or has queued.
pub fn local_in_flight() -> usize {
    peers::LOCAL_IN_FLIGHT.load(std::sync::atomic::Ordering::Relaxed)
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

/// Texts per model call when a batch is embedded in the background ([`embed_in_calls`]).
pub const MODEL_CALL: usize = 32;

/// Embed `texts` in the background, [`MODEL_CALL`] at a time, the calls in parallel:
/// a foreground request waits little behind any of them on a worker.
pub async fn embed_in_calls(embedder: &Arc<Embedder>, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
    // An endpoint takes the batch in one request (it runs no worker of ours).
    if embedder.on_endpoint() {
        return embedder.embed_background(texts).await;
    }
    let calls = texts.chunks(MODEL_CALL).map(|c| embedder.embed_background(c.to_vec()));
    let mut out = Vec::with_capacity(texts.len());
    for r in futures::future::join_all(calls).await {
        out.extend(r?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Needs the model: `ADB_EMBED_MODEL_DIR` (tests/fetch-model.sh puts it in
    /// $BUILD_DIR/models/bge-small-en-v1.5). Skipped without it.
    fn model() -> Option<Embedder> {
        let dir = std::env::var("ADB_EMBED_MODEL_DIR").ok()?;
        Some(Embedder::from_config(&EmbedConfig { model_dir: Some(dir), ..Default::default() }).expect("model loads"))
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
    /// with the key as a bearer token and the `dimensions` asked for; a wrong dimension
    /// is caught by `verify`.
    #[tokio::test(flavor = "multi_thread")]
    async fn openai_compatible_endpoint() {
        use axum::{routing::post, Json, Router};
        static ASKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        let app = Router::new().route(
            "/v1/embeddings",
            post(|headers: axum::http::HeaderMap, Json(body): Json<serde_json::Value>| async move {
                assert_eq!(headers.get("authorization").unwrap(), "Bearer sk-test");
                // Asked for the configured length (`request_dimension`), or not at all.
                assert!(body.get("dimensions").is_none_or(|d| *d == 4), "{body}");
                if body.get("dimensions").is_some() {
                    ASKED.store(true, std::sync::atomic::Ordering::SeqCst);
                }
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
            provider: Provider::Custom,
            model: "test-model".into(),
            dimension: 4,
            url: Some(format!("http://{addr}/v1/")),
            api_key_env: Some("ADB_TEST_EMBED_KEY".into()),
            model_dir: None,
            request_dimension: true,
            run_on: RunOn::Node,
        };
        let e = Arc::new(Embedder::from_config(&cfg).unwrap());
        e.verify().await.unwrap();
        let v = e.embed(vec!["a".into(), "b".into(), "c".into()]).await.unwrap();
        assert_eq!(v.iter().map(|x| x[0]).collect::<Vec<_>>(), vec![1.0, 1.0, 1.0], "unit length");
        assert_eq!(v.len(), 3);
        assert!(ASKED.load(std::sync::atomic::Ordering::SeqCst), "the dimension was asked for");

        let wrong = Arc::new(Embedder::from_config(&EmbedConfig { dimension: 8, request_dimension: false, ..cfg.clone() }).unwrap());
        assert!(wrong.verify().await.unwrap_err().contains("4 dimensions"));
        let missing = EmbedConfig { api_key_env: Some("ADB_TEST_NO_SUCH_KEY".into()), ..cfg.clone() };
        assert!(Embedder::from_config(&missing).err().unwrap().contains("not set"));

        // Changed in place: a setting that can't work leaves the one in force; one
        // that works replaces it for every holder, and says when vectors must be made again.
        assert!(e.reconfigure(&EmbedConfig { dimension: 8, request_dimension: false, ..cfg.clone() }).await.unwrap_err().contains("4 dimensions"));
        assert_eq!(e.config(), cfg);
        assert!(!e.reconfigure(&cfg).await.unwrap(), "the same setting changes nothing");
        let renamed = EmbedConfig { model: "test-model-2".into(), ..cfg.clone() };
        assert!(e.reconfigure(&renamed).await.unwrap());
        assert_eq!((e.model(), e.dimension()), ("test-model-2".to_string(), 4));
        assert_eq!(e.embed(vec!["x".into()]).await.unwrap().len(), 1);
    }

    #[test]
    fn settings_round_trip() {
        let dir = tempfile_dir();
        assert_eq!(EmbedConfig::load(&dir).unwrap(), None);
        let c = EmbedConfig { provider: Provider::Custom, model: "m".into(), dimension: 1536, url: Some("https://x/v1".into()), api_key_env: None, model_dir: None, request_dimension: true, run_on: RunOn::Compute };
        c.save(&dir).unwrap();
        assert_eq!(EmbedConfig::load(&dir).unwrap(), Some(c));
        std::fs::remove_dir_all(&dir).unwrap();
        // The default is the local model, in this process, and needs no fields.
        let d: EmbedConfig = serde_json::from_str(r#"{"provider":"local","model":"bge-small-en-v1.5","dimension":384}"#).unwrap();
        assert_eq!(d, EmbedConfig::default());
        assert_eq!(serde_json::to_value(&d).unwrap(), serde_json::json!({"provider":"local","model":"bge-small-en-v1.5","dimension":384}));
    }

    /// Each provider's required fields; `openai` has OpenAI's URL and always asks for
    /// the dimension, `custom` asks only when told to.
    #[test]
    fn providers_check_their_fields() {
        let openai = EmbedConfig { provider: Provider::Openai, model: "text-embedding-3-small".into(), dimension: 384, ..Default::default() };
        assert!(openai.check().unwrap_err().contains("api_key_env"));
        let openai = EmbedConfig { api_key_env: Some("OPENAI_API_KEY".into()), ..openai };
        openai.check().unwrap();
        assert_eq!(openai.endpoint(), Some((OPENAI_URL.to_string(), Some("OPENAI_API_KEY"), true)));
        let custom = EmbedConfig { provider: Provider::Custom, model: "nomic-embed-text".into(), dimension: 768, ..Default::default() };
        assert!(custom.check().unwrap_err().contains("url"));
        let custom = EmbedConfig { url: Some("http://ollama:11434/v1".into()), ..custom };
        custom.check().unwrap();
        assert_eq!(custom.endpoint(), Some(("http://ollama:11434/v1".to_string(), None, false)));
        assert_eq!(EmbedConfig { run_on: RunOn::Compute, ..custom.clone() }.on_node(), custom);
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

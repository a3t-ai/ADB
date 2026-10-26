//! ADB's local text embeddings (MVP W4, `.ai/W4_RECALL.md`).
//!
//! One model per deployment: **BAAI/bge-small-en-v1.5**, 384 dimensions, run on
//! ONNX Runtime on the CPU. ADB embeds the `content` of SEMANTIC / EPISODIC /
//! PROCEDURAL records before logging them (the vector travels in the log as f16,
//! ARCHITECTURE §9.1) and the text of `LIKE $q` queries. Nothing leaves the process.
//!
//! The model is a directory with `model.onnx`, `tokenizer.json`, `config.json`,
//! `special_tokens_map.json` and `tokenizer_config.json` (Hugging Face
//! `Xenova/bge-small-en-v1.5`, `onnx/model.onnx`), baked into the image at
//! [`DEFAULT_MODEL_DIR`]. `ADB_EMBED_MODEL_DIR` points elsewhere; `ADB_EMBED=off`
//! turns embedding off (records are then found by filters and text only).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fastembed::{InitOptionsUserDefined, Pooling, TextEmbedding, TokenizerFiles, UserDefinedEmbeddingModel};
use half::f16;

/// Dimensions of the model's vectors (the Lance `embedding` column, D3 S5).
pub const DIM: usize = 384;

/// Where the image keeps the model.
pub const DEFAULT_MODEL_DIR: &str = "/models/bge-small-en-v1.5";

/// The model's name, recorded with the table (`_meta`) so a different model is noticed.
pub const MODEL: &str = "bge-small-en-v1.5";

/// Tokens embedded per text; longer `content` is truncated (bge's limit).
const MAX_TOKENS: usize = 512;

/// The embedding model, loaded once. `embed` is CPU work: call it from a blocking
/// task ([`Embedder::embed_blocking`]) or through [`Embedder::embed`].
pub struct Embedder {
    model: Mutex<TextEmbedding>,
}

impl Embedder {
    /// Load the model files in `dir`, running inference on `threads` CPU threads.
    pub fn load(dir: &Path, threads: usize) -> Result<Self, String> {
        let read = |f: &str| std::fs::read(dir.join(f)).map_err(|e| format!("embedding model: {}: {e}", dir.join(f).display()));
        let tokenizer = TokenizerFiles {
            tokenizer_file: read("tokenizer.json")?,
            config_file: read("config.json")?,
            special_tokens_map_file: read("special_tokens_map.json")?,
            tokenizer_config_file: read("tokenizer_config.json")?,
        };
        // bge models embed the [CLS] token (fastembed's default for this model).
        let model = UserDefinedEmbeddingModel::new(read("model.onnx")?, tokenizer).with_pooling(Pooling::Cls);
        let options = InitOptionsUserDefined::new().with_max_length(MAX_TOKENS).with_intra_threads(threads.max(1));
        let model = TextEmbedding::try_new_from_user_defined(model, options).map_err(|e| format!("embedding model: {e}"))?;
        Ok(Self { model: Mutex::new(model) })
    }

    /// The embedder this process should use: `None` when `ADB_EMBED=off`, or when
    /// the model directory is missing (logged; ADB then runs without vectors).
    pub fn from_env() -> Option<Arc<Self>> {
        if std::env::var("ADB_EMBED").is_ok_and(|v| v.eq_ignore_ascii_case("off")) {
            tracing::info!("embeddings off (ADB_EMBED=off)");
            return None;
        }
        let dir: PathBuf = std::env::var("ADB_EMBED_MODEL_DIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from(DEFAULT_MODEL_DIR));
        if !dir.join("model.onnx").exists() {
            tracing::warn!(dir = %dir.display(), "no embedding model: records get no vectors and LIKE finds nothing (set ADB_EMBED_MODEL_DIR)");
            return None;
        }
        let threads = std::env::var("ADB_EMBED_THREADS").ok().and_then(|t| t.parse().ok()).unwrap_or(2);
        let t = std::time::Instant::now();
        match Self::load(&dir, threads) {
            Ok(e) => {
                tracing::info!(model = MODEL, dir = %dir.display(), threads, took = ?t.elapsed(), "embedding model loaded");
                Some(Arc::new(e))
            }
            Err(e) => {
                tracing::error!(error = %e, "embedding model failed to load; running without vectors");
                None
            }
        }
    }

    /// Embed `texts` (unit-length vectors of [`DIM`]), on this thread.
    pub fn embed_blocking(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let mut model = self.model.lock().map_err(|_| "embedding model poisoned".to_string())?;
        let mut out = model.embed(texts, Some(32)).map_err(|e| format!("embedding: {e}"))?;
        out.iter_mut().for_each(|v| normalize(v));
        Ok(out)
    }

    /// Embed `texts` on Tokio's blocking pool.
    pub async fn embed(self: &Arc<Self>, texts: Vec<String>) -> Result<Vec<Vec<f32>>, String> {
        let me = Arc::clone(self);
        tokio::task::spawn_blocking(move || me.embed_blocking(&texts)).await.map_err(|e| format!("embedding task: {e}"))?
    }
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

    #[test]
    fn f16_keeps_similarity() {
        let a = vec![0.1f32, -0.5, 0.25, 0.8];
        let back: Vec<f32> = to_f16(&a).iter().map(|x| x.to_f32()).collect();
        assert!(cosine(&a, &back) > 0.9999);
    }
}

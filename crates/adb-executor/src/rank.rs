//! Relevance ranking for `RECALL … LIKE $q` (MVP W4, `.ai/W4_RECALL.md` T10).
//!
//! Candidates are the current rows that pass the statement's filters (memory type,
//! `IN SCOPE`, `NAMESPACE`, TTL). Two lists are ranked over them — cosine similarity
//! of the stored vectors to the query vector, and BM25 of the query text over the
//! rows' `text` (stop words dropped, light stemming) — and fused by weighted
//! reciprocal rank (k = 60; meaning counts 5× words), so neither score needs
//! calibrating against the other. Ties go to the higher similarity; a mild recency
//! factor favours newer rows. Brute force: fine below ~10k candidates (S8); an ANN / inverted index
//! for bigger partitions is W4-L2.

use std::collections::HashMap;

use adb_core::log::CommitTs;

/// RRF's constant (Cormack et al.): dampens the weight of the very top ranks.
const RRF_K: f32 = 60.0;
/// Recency half-life: a row this old weighs [`RECENCY_WEIGHT`] / 2 less.
const HALF_LIFE_DAYS: f32 = 30.0;
/// How much recency can move a score (the rest is relevance).
const RECENCY_WEIGHT: f32 = 0.2;
/// Weights of the two lists in the fusion: meaning first, words to back it up
/// (exact names and codes; the only list when rows have no vector yet). Set on the
/// evaluation set (tests/recall-eval, 26 Oct): 0.1–0.2 matched vector-only
/// (MRR 0.97, recall@5 1.00); 0.6 dropped MRR to 0.91; linear score fusion was no
/// better at its best and worse above α = 0.1.
const VECTOR_WEIGHT: f32 = 1.0;
const TEXT_WEIGHT: f32 = 0.2;
const BM25_K1: f32 = 1.2;
const BM25_B: f32 = 0.75;

/// One candidate: its vector (if embedded), its text, and when it was written.
pub struct Candidate<'a> {
    pub embedding: Option<&'a [f32]>,
    pub text: Option<&'a str>,
    pub at: CommitTs,
}

/// A ranked candidate: its index, the fused score and its cosine similarity.
#[derive(Debug, Clone, PartialEq)]
pub struct Ranked {
    pub index: usize,
    pub score: f32,
    pub similarity: Option<f32>,
}

/// Rank `candidates` against `query_vec` and / or `query_text`, best first.
/// `min_similarity` (MIN_CONFIDENCE) drops candidates whose cosine similarity is
/// below it (and those without a vector). Candidates in neither list are dropped.
pub fn rank(candidates: &[Candidate<'_>], query_vec: Option<&[f32]>, query_text: Option<&str>, min_similarity: Option<f32>, now: CommitTs) -> Vec<Ranked> {
    let similarity: Vec<Option<f32>> = candidates
        .iter()
        .map(|c| match (query_vec, c.embedding) {
            (Some(q), Some(e)) => Some(cosine(q, e)),
            _ => None,
        })
        .collect();
    let vector_rank = ranks(&similarity.iter().map(|s| s.filter(|s| *s > 0.0)).collect::<Vec<_>>());
    let text_scores = match query_text {
        Some(q) => bm25(q, &candidates.iter().map(|c| c.text.unwrap_or("")).collect::<Vec<_>>()),
        None => vec![None; candidates.len()],
    };
    let text_rank = ranks(&text_scores);
    let mut out: Vec<Ranked> = (0..candidates.len())
        .filter(|&i| min_similarity.is_none_or(|m| similarity[i].is_some_and(|s| s >= m)))
        .filter_map(|i| {
            let (vw, tw) = weights();
            let rrf = vector_rank[i].map(|r| vw / (RRF_K + r as f32)).unwrap_or(0.0)
                + text_rank[i].map(|r| tw / (RRF_K + r as f32)).unwrap_or(0.0);
            (rrf > 0.0).then(|| {
                let age_days = ((now.0 - candidates[i].at.0).max(0) as f32) / 86_400_000_000.0;
                let recency = 0.5f32.powf(age_days / HALF_LIFE_DAYS);
                Ranked { index: i, score: rrf * (1.0 - RECENCY_WEIGHT + RECENCY_WEIGHT * recency), similarity: similarity[i] }
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(b.similarity.unwrap_or(-1.0).total_cmp(&a.similarity.unwrap_or(-1.0)))
            .then(a.index.cmp(&b.index))
    });
    out
}

/// The fusion weights: hybrid, or one list alone for the evaluation set
/// (`ADB_RECALL_MODE=vector | text`, tests/recall-eval).
fn weights() -> (f32, f32) {
    static MODE: std::sync::OnceLock<(f32, f32)> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("ADB_RECALL_MODE").as_deref() {
        Ok("vector") => (1.0, 0.0),
        Ok("text") => (0.0, 1.0),
        _ => (VECTOR_WEIGHT, TEXT_WEIGHT),
    })
}

/// 1-based ranks of the `Some` scores, highest first; `None` stays unranked.
fn ranks(scores: &[Option<f32>]) -> Vec<Option<usize>> {
    let mut order: Vec<(usize, f32)> = scores.iter().enumerate().filter_map(|(i, s)| s.map(|s| (i, s))).collect();
    order.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut out = vec![None; scores.len()];
    for (rank, (i, _)) in order.into_iter().enumerate() {
        out[i] = Some(rank + 1);
    }
    out
}

/// Words that say nothing about a record's subject.
const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can", "do", "does", "for", "from", "has", "have",
    "how", "i", "in", "is", "it", "its", "me", "my", "of", "on", "or", "our", "so", "that", "the", "their", "them",
    "there", "these", "they", "this", "to", "up", "was", "we", "were", "what", "when", "where", "which", "who", "why",
    "will", "with", "you", "your",
];

/// Lower-cased words, stop words dropped, common endings stripped ("renewal",
/// "renews", "renewed" → "renew"; "Acme's" → "acme").
fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .map(|t| t.to_lowercase())
        .filter(|t| t.len() > 1 && !STOP_WORDS.contains(&t.as_str()))
        .map(|t| stem(&t))
        .collect()
}

fn stem(word: &str) -> String {
    for suffix in ["ations", "ation", "ings", "ing", "als", "al", "edly", "ed", "ies", "es", "ly", "s"] {
        if let Some(stem) = word.strip_suffix(suffix) {
            if stem.chars().count() >= 3 {
                return if suffix == "ies" { format!("{stem}y") } else { stem.to_string() };
            }
        }
    }
    word.to_string()
}

/// BM25 of `query` over `docs` (scored within this candidate set); `None` where no
/// query term occurs.
fn bm25(query: &str, docs: &[&str]) -> Vec<Option<f32>> {
    let q: Vec<String> = {
        let mut t = tokens(query);
        t.sort();
        t.dedup();
        t
    };
    let docs: Vec<Vec<String>> = docs.iter().map(|d| tokens(d)).collect();
    let n = docs.len() as f32;
    let avg = (docs.iter().map(|d| d.len()).sum::<usize>() as f32 / n.max(1.0)).max(1.0);
    let mut df: HashMap<&str, f32> = HashMap::new();
    for d in &docs {
        let mut seen: Vec<&str> = d.iter().map(String::as_str).filter(|t| q.iter().any(|w| w == t)).collect();
        seen.sort();
        seen.dedup();
        for t in seen {
            *df.entry(t).or_default() += 1.0;
        }
    }
    docs.iter()
        .map(|d| {
            let len = d.len() as f32;
            let mut score = 0.0;
            for w in &q {
                let tf = d.iter().filter(|t| *t == w).count() as f32;
                if tf == 0.0 {
                    continue;
                }
                let df = df.get(w.as_str()).copied().unwrap_or(0.0);
                let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
                score += idf * tf * (BM25_K1 + 1.0) / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * len / avg));
            }
            (score > 0.0).then_some(score)
        })
        .collect()
}

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
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

    const NOW: CommitTs = CommitTs(1_800_000_000_000_000);

    fn c<'a>(e: Option<&'a [f32]>, t: &'a str) -> Candidate<'a> {
        Candidate { embedding: e, text: Some(t), at: NOW }
    }

    #[test]
    fn vector_and_text_fuse() {
        let (a, b, x) = ([1.0, 0.0], [0.8, 0.6], [0.0, 1.0]);
        let cands = [c(Some(&a), "acme renewal date"), c(Some(&b), "beta renewal"), c(Some(&x), "payments pod oom")];
        let r = rank(&cands, Some(&[1.0, 0.0]), Some("acme renewal"), None, NOW);
        // The orthogonal one shares no words either: not a match at all.
        assert_eq!(r.iter().map(|r| r.index).collect::<Vec<_>>(), [0, 1], "{r:?}");
        assert!((r[0].similarity.unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn text_alone_ranks_rows_without_vectors() {
        let cands = [c(None, "nothing here"), c(None, "the acme contract renews")];
        let r = rank(&cands, None, Some("acme"), None, NOW);
        assert_eq!(r.iter().map(|r| r.index).collect::<Vec<_>>(), [1], "no term, no rank");
    }

    #[test]
    fn min_confidence_cuts_on_cosine() {
        let (a, b) = ([1.0, 0.0], [0.6, 0.8]);
        let cands = [c(Some(&a), ""), c(Some(&b), ""), c(None, "")];
        let r = rank(&cands, Some(&[1.0, 0.0]), None, Some(0.7), NOW);
        assert_eq!(r.iter().map(|r| r.index).collect::<Vec<_>>(), [0]);
    }

    #[test]
    fn stemming_and_stop_words() {
        assert_eq!(tokens("When is the Acme renewal?"), ["acme", "renew"]);
        assert_eq!(tokens("Acme's contract renews"), ["acme", "contract", "renew"]);
        assert_eq!(stem("policies"), "policy");
        assert_eq!(stem("bus"), "bus", "too short to strip");
    }

    #[test]
    fn recency_breaks_ties_only() {
        let a = [1.0, 0.0];
        let old = Candidate { embedding: Some(&a), text: None, at: CommitTs(NOW.0 - 90 * 86_400_000_000) };
        let new = Candidate { embedding: Some(&a), text: None, at: NOW };
        let r = rank(&[old, new], Some(&[1.0, 0.0]), None, None, NOW);
        assert_eq!(r[0].index, 1, "same similarity: the newer first");
    }
}

# W4 — Relevant recall: local embeddings, vector + full-text, hybrid ranking

> MVP plan W4 (M2). **Decided 26 Oct (user):** ADB embeds text itself with a small
> local CPU model (bge-small class, 384 dims), no external calls. This note settles
> how. The query surface follows the AQL spec (decisions below).

## Goal

`RECALL` can rank by meaning, and agents get the facts relevant to the conversation,
not the newest five (L1, F-L1). Done when an evaluation set shows hybrid recall beats
both vector-only and text-only, and the A3T agent answers from a fact it stored weeks
of messages ago.

## Technical choices (implementation, not product)

| # | Choice | Why |
|---|---|---|
| T1 | **Model `BAAI/bge-small-en-v1.5`**, 384 dims, ONNX (fp32), run with **ONNX Runtime** through `fastembed` 7 | Best small English retrieval model of its size; ONNX Runtime is 2–4× faster on CPU than pure-Rust candle. The runtime library and the model are fetched at build time (both hosts reachable from the pod) |
| T2 | **Model baked into the ADB image** (`/models/bge-small-en-v1.5`, ~130 MB); `ADB_EMBED_MODEL_DIR` overrides, `ADB_EMBED=off` disables | No network at runtime; one model per deployment (D3 S5) |
| T3 | **What is embedded: `content`** (D3 S3), for SEMANTIC, EPISODIC, PROCEDURAL (PROCEDURAL: `pattern` + `steps` when there is no `content`). Truncated at 512 tokens | The field `remember` and agent memory already write |
| T4 | **Embedded before the sequencer**, outside its lock (a blocking pool, 2 ORT threads); the vector travels **in the log record as f16** and lands in Lance as f32 (ARCHITECTURE §9.1, S9) | Replay never re-embeds; embedding doesn't serialise writes. Adds ~5–15 ms per STORE in release |
| T5 | **`UPDATE` that changes `content` re-embeds**; others keep the vector | Supersede copies the row (D2) |
| T6 | **Schema v4: `embedding` FixedSizeList(Float32, 384)**; tables at 768 (all null today) are rewritten at open | S5 said 768 as a starting point; the model sets it |
| T7 | **Backfill**: at open, a background task embeds current rows that have `content` but no vector and commits them as log effects (`Embed {lineage, vector}`), in batches | Existing memory (vc-dev) becomes searchable without a re-import |
| T8 | **Vector search: brute force below ~10k vectors, IVF_RQ + refine above** (ARCHITECTURE §7, S8), built by a background job when the threshold is crossed and refreshed with compaction (W5) | S8 numbers |
| T9 | **Full text: Lance inverted (BM25) index on `text`**, created with the table, optimised in the background | `content CONTAINS` stays a substring filter |
| T10 | **Hybrid ranking: reciprocal-rank fusion (k = 60)** of the vector and BM25 lists, then a recency factor (half-life 30 days on `valid_from`) and +link weight when following links; weights fixed by the eval set | RRF needs no score calibration between the two lists |
| T11 | **Eval set**: ~150 synthetic facts / episodes over 5 topics and ~40 queries with labelled answers (paraphrases, keyword-only, recency-sensitive), in `tests/recall-eval/`; metric recall@5 and MRR; a gate in `check-all.sh` | Decides T10's weights and stops regressions |
| T12 | **Filters still apply**: `WHERE`, `IN SCOPE`, `NAMESPACE`, TTL, current view — ranking happens inside the filtered set (prefilter) | Same semantics as today |

## Decisions (26 Oct, user: "the AQL spec has the answers")

1. **AQL: `RECALL FROM SEMANTIC LIKE $q`**, where `$q` holds *text* (ADB embeds it) or
   a vector. No grammar change. Per AQL v0.5 §6 / §14, `LIKE` is the similarity
   predicate: results come **ranked by similarity**, cut by **`LIMIT n`** and
   **`MIN_CONFIDENCE x`**; an explicit `ORDER BY` overrides the order. ADB's
   similarity is the hybrid rank (T10); `MIN_CONFIDENCE` applies to the cosine
   similarity of the vectors (0–1), reported on each record as its confidence.
2. **MCP `recall` mirrors the spec**: `like` (text), `min_confidence`, `limit` →
   `RECALL FROM <type> LIKE $q MIN_CONFIDENCE x LIMIT n`. `contains` (an ADB extension,
   substring) is unchanged.
3. **Agent prompt (F-L1)**: the memory phase runs `RECALL FROM SEMANTIC LIKE $message
   [MIN_CONFIDENCE c] LIMIT n NAMESPACE …`, with `n` and `c` from the agent's memory
   configuration (`memory_config.semantic.{limit, min_confidence}`, default 5 and none;
   the same keys for the other tiers' limits). No hard-coded mix.

## Steps

1. Embedder crate (`adb-embed`): load, embed batch, f16 codec; tests on known
   similarities. Image build fetches the model.
2. Schema v4 + migration; `Embed` effect; STORE / UPDATE embed outside the lock.
3. Backfill task.
4. Vector + FTS search in `LanceReads` (brute force, then index job); hybrid ranker.
5. AQL `LIKE $text` with `LIMIT` / `MIN_CONFIDENCE`, MCP `recall` `like`, guide.
6. Eval set + gate; tune T10.
7. FlowR agent memory: `LIKE $message LIMIT n` from `memory_config` (F-L1); A3T tier editor gets `limit` / `min_confidence`; A3T check.
8. Release image, vc-dev, user test.

## Risks

- **Image size** +~150 MB (model + ONNX Runtime). Acceptable; quantized model (~35 MB)
  is the fallback.
- **CPU on the node** (4 cores shared): embedding a large backfill competes with
  queries — the backfill runs at low concurrency.
- **English only, one size** (bge-small-en, 384). Pluggable providers of any size,
  chosen in ADB settings, are W4-L1 in `limitations.md` (user, 26 Oct: later).

## As built (27 Oct)

| Step | State |
|---|---|
| 1 Embedder | `adb-embed`: fastembed 7 / ONNX Runtime (static), model from `ADB_EMBED_MODEL_DIR` (image: `/models/bge-small-en-v1.5`, baked by `deploy.sh`; `tests/fetch-model.sh`) |
| 2 Schema / write path | 384-dim column; tables of another dimension rewritten at open (backup kept); STORE / UPDATE embed before the sequencer; `Embed` log effect |
| 3 Backfill | Maintenance ticker, 64 rows per tick while there is work, then once a minute |
| 4 Search | Brute force over the filtered current rows (W4-L2); cosine + BM25, weighted RRF (text 0.2), recency ×0.8–1.0 |
| 5 Query surface | `RECALL … LIKE $q` (text or vector) `MIN_CONFIDENCE` `LIMIT`, `metadata.confidence`; MCP `recall` `like` / `min_confidence` |
| 6 Eval | `tests/recall-eval.mjs` in `check-all.sh`: hybrid recall@1 0.95, recall@5 1.00, MRR 0.97 (text only 0.64 / 0.93 / 0.75) |
| 7 Agents | FlowR memory phase: `LIKE $message`, `memory_config.semantic.{limit, min_confidence}`; A3T tier editor fields; FlowR integration checks the fact reaches the prompt past newer ones |
| 8 Release | pending |

Limits: W4-L1…L6 in `limitations.md`. ADB's memory limit raised to 2 GiB.

## W9.10 (28 Oct): providers

User decisions: local model **or** any OpenAI-compatible `/embeddings` endpoint; the
API key lives in ADB's environment (`api_key_env` names the variable, set from a
Secret); changing model or dimension **clears the vectors and re-embeds** in the
background. Settings: `<data dir>/embedding.json` (`adb_embed::EmbedConfig`), read at
start; `GET/PUT /admin/embedding` (PUT embeds a sample, checks the size, saves and
restarts ADB). The model's name is kept in `_meta` (`embedding_model`); at open a
different one clears the vectors (a different size rewrites the table, as T6).

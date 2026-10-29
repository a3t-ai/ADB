# P1 — Graph core

> **Date:** 29 Oct 2026 · @Sriram · phase 1 of `GOALS.md`. Decisions D1–D3 chosen by
> the user (all as recommended); D4–D7 are defaults that follow from them.

## Decisions

| # | Question | Decision |
|---|---|---|
| D1 | What `FOLLOW LINKS … DEPTH n` returns | **Every record reached within 1..n hops**, each once at its shortest hop, ordered by hop then path weight (product of link weights along the path). Each result carries `_hop` and `_path_weight`. Breadth-first, cycle-safe. DEPTH defaults to 1, capped at 5; at most 1,000 records per traversal. |
| D2 | Entities and facts | The kinds D3 reserved (S7). **Entities** are SEMANTIC `entity` rows with a stable key such as `person:ada-lovelace`. **Facts** are SEMANTIC `fact` rows: `subject_id`, `predicate`, an object that is an entity key or a literal value, `confidence`, `source`. A new fact for the same (subject, predicate) supersedes the old one unless the predicate is declared multi-valued. A fact whose object is an entity is also an edge, so `FOLLOW LINKS` walks it. AQL v0.8 `STORE … FACT`; MCP `remember` with `subject` + `predicate`. |
| D3 | Agents and the graph (GR-6) | After hybrid recall, FlowR's memory phase pulls up to 5 one-hop neighbours of the top results (link weight ≥ 0.5) into the prompt as *related*, and link weight is a ranking signal. Per-agent setting, on by default. |
| D4 | Direction (GR-2) | `FOLLOW LINKS … OUTBOUND` (default) / `INBOUND` / `BOTH`; same on `WITH LINKS`. Edges already store their target (`object_id`). |
| D5 | Conditions (GR-3) | `MIN_WEIGHT w` on `FOLLOW` / `WITH LINKS`; `TYPE "t"`, `TYPE IN ("a", "b")` or `ALL`. |
| D6 | Cache (W10-L2) | Edge buckets in the hot cache, indexed by subject and by object, kept whenever the source memory type is kept. |
| D7 | `LOOKUP … KEY … FOLLOW LINKS` (L6 / GR-7) | Returns the linked records, as `RECALL` does. |

## Steps

| Step | What | Gate |
|---|---|---|
| 1.1 | Multi-hop traversal (D1) for RECALL / LOOKUP / REFLECT; LOOKUP FOLLOW fix (D7) | executor tests: chain, cycle, diamond, depth cap, node cap |
| 1.2 | AQL v0.8: direction, `MIN_WEIGHT`, `TYPE IN`, `ALL` (D4, D5) | AQL conformance + ADB tests |
| 1.3 | Edge buckets in the hot cache (D6) | shadow-mode differential, cache tests |
| 1.4 | Entities and facts (D2): `STORE … FACT`, supersede by (subject, predicate), multi-valued predicates, fact edges, MCP `remember` | executor + MCP tests, crash test with facts |
| 1.5 | FlowR recall with links (D3) | FlowR integration test |
| 1.6 | Gate: graph conformance suite, A3T check (agent recalls a linked fact) | user's A3T pass |

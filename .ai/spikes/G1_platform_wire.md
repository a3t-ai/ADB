# Spike G1 — platform-table wire: HTTP + JSON vs Arrow Flight

> **Date:** 26 Oct 2026 · code: [`g1-wire/`](g1-wire/) · for stage G decision G5
> (`G_PLATFORM_TABLES.md`).

**Setup:** one process, both servers serving the same in-memory 1,000-row table
(`template_registry` shape: 10 short UTF-8 columns + an 8 KB `spec_yaml`), loopback,
keep-alive (reqwest / tonic channel reused). The times are what FlowR would see:
the Flight client converts Arrow batches to JSON rows (`batches_to_json`) and builds
insert batches from JSON records (`records_to_batch`), as `call:lancedb` does today.
Release build, code-server pod (4 CPUs). Two runs (500 and 1,000 iterations):

| Call | JSON p50 / p99 | Flight p50 / p99 | Flight vs JSON |
|---|---|---|---|
| lookup 1 row (id + org) | 0.24–0.29 / 0.50–0.61 ms | 0.42–0.51 / 0.82–0.88 ms | **+0.2 ms** (slower) |
| list 100 rows × 8 KB | 4.3–5.7 / 8.5–8.7 ms | 2.8–3.3 / 4.5–4.7 ms | **1.5–1.7× faster** |
| insert 1,000 small rows | 4.8–8.0 / 10.4–10.5 ms | 1.5–2.3 / 3.5–3.7 ms | **3.2–3.5× faster** |

**Reading:**
- A 1-row call costs Flight about 0.2 ms more: gRPC/HTTP/2 framing plus the schema
  message each response carries. In absolute terms that is a fifth of a millisecond
  per call, against flow steps that take milliseconds and LLM calls that take seconds.
- As soon as rows are bigger or more numerous, Flight wins clearly (no JSON
  encode/parse of large strings; Arrow columns cross as buffers), and its p99 is
  half of JSON's.
- Over the network (not loopback) both add the same round trip, so the relative gap
  on small calls shrinks further.

**Recommendation:** by G5's rule this is the mixed case (Flight slower on the
smallest calls). Even so, **Flight only** is the better trade: one client and one
server surface in FlowR and ADB instead of two, the large-result and bulk wins, the
non-HTTP wire asked for, and a 0.2 ms cost on single-row calls that no A3T flow will
notice. JSON over HTTP stays available through ADB's existing `/query` for AQL.

**Decision (user, 26 Oct):** `call:adb` (FlowR's native ADB calls — platform tables and AQL) is always Flight; ADB over MCP is always JSON.

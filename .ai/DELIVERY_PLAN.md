# ADB — Delivery plan

> **Date:** 9 Oct 2026 · @Sriram · tracks `ARCHITECTURE.md` v1.4 (§18) and
> `RISK_BURNDOWN.md`. Durations are rough, for one engineer working with Claude Code;
> they are for ordering, not commitments.
> **Product:** ADB is part of A3T, SaaS-only, plus a local dev build. Focus: a working,
> usable product; pricing later.

## Principles

- **Each milestone ends with something usable**, not just a layer. M1's local dev
  build is a real product for FlowR developers.
- **The single-node build exercises the whole write path** (§13): the same sequencer,
  applier and schema run in the cluster; only the log implementation changes.
- **Correctness gates before features.** Crash, stale-writer and replay tests from the
  spikes become CI tests in M1; the fault harness (R10) gates the cluster in M3.
- **Settled decisions are not reopened** (option A MemWAL pinned at Lance 12.0.0,
  openraft 0.10.0-alpha.36, "Raft is the WAL", ADB-driven seals, IVF_RQ, one platform
  embedding model, supersede-by-default, business support licence).

## Milestones at a glance

| # | Milestone | Ends with | Needs | Rough size |
|---|---|---|---|---|
| **M0** | Foundations | One Arrow / Lance version, new crate skeletons, CI, D2/D3 written | — | 1–2 weeks |
| **M1** | Single node (local dev build) | Durable, crash-safe ADB on one machine; AQL conformance passes; subscriptions; FlowR uses it | M0 | 5–7 weeks |
| **M2** | Recall and memory lifecycle | Fast hybrid recall, local cache, compaction, physical forget, continuous queries v1 | M1 | 4–5 weeks |
| **M3** | Replication | 3-node replica sets with failover, archive, fault-harness gate | M1 (`adb-log` trait), **3 servers** | 5–7 weeks |
| **M4** | SaaS control plane | Routing, leases, hibernate-by-default with lazy wake, gateway tier, tenancy, A3T hooks | M3 | 4–6 weeks |
| **M5** | Scale-out | Many replica sets, rebalancing, bucket spreading, CQ v1.1 / cross-partition CQ | M4 | ongoing |

M2 can overlap the end of M1, and M3 can start as soon as M1's log trait is stable,
**once the servers are available** — order them early.

---

## M0 — Foundations (1–2 weeks)

| Task | Detail | Closes |
|---|---|---|
| Dependency upgrade — ✅ *24 Oct (K5 closed)* | One Arrow 58 / DataFusion / object_store across the workspace; Lance `=12.0.0`; Rust stable; `protoc` in Dockerfile and docs | K5 |
| AQL from one source | ✅ Done 9 Oct: ADB depends on the AQL repo's `aql-parser` (git), local override in `.cargo/config.toml`; push AQL so Docker and fresh clones build | K9 |
| Crate skeletons — ✅ *24 Oct* | `adb-log`, `adb-sequencer`, `adb-store` (and empty `adb-cluster`, `adb-cq`, `adb-gateway`); move `clawdb`'s Lance code toward `adb-store` | §16 |
| Core types — ✅ *24 Oct* | In `adb-core`: log record, typed effects (`PutEntity`, `PutFact`, `PutEdge`, `PutEpisode`, `SupersedeFact`, `Delete`, `Forget`, `RegisterSub`, …), offsets, epochs, partition id | §9, §11.7 |
| Schema write-up (D3) — ✅ *24 Oct* | Concrete Arrow schema per `kind` (incl. `event`, `sub`, `cq`, `_meta`), nullability, plaintext vs encrypted `value` fields, one platform embedding model and dimension, re-embedding procedure | D3 |
| Temporal write-up (D2) — ✅ *24 Oct* | Supersede-by-default for SEMANTIC / PROCEDURAL, overwrite for WORKING / TOOLS; `valid_from` / `valid_to` / `superseded_by`; TTL; how `UPDATE`, `FORGET`, `WINDOW` map | D2 |
| CI — ⏸ *deferred 24 Oct (user): run the checks by hand until the repos are pushed* | Build + tests on stable, `cargo deny` licences, AQL conformance runner against ADB, spike crash tests (S1, S2, R11, R12) ported as integration tests | B4, K3 |
| Lance bug — ⏸ *parked 24 Oct (user): doesn't affect dev; ADB-driven seals work around it* | File the MemWAL replay report (`lancebug.md`) | K7 |

**Exit:** workspace builds on stable with one Arrow version; D2 and D3 agreed; CI green.

## M1 — Single node / local dev build (5–7 weeks)

**Goal:** a durable, crash-safe ADB on one machine that FlowR developers use daily.

| Area | Build | Spike basis |
|---|---|---|
| `adb-log` | Log trait; local WAL as ADB's own segment files + offset index; WAL writer thread (`pwrite` + `fdatasync`, `IOFlushed`-style callbacks); retention below the durable offset | R2, K6, R12 |
| `adb-sequencer` | Per-partition state on a core's executor (§8); validation (existence, uniqueness, references, OCC read-sets); pending overlay; group commit (2–5 ms); `txn_id` dedupe; `WORKING` / `TOOLS` hot tier (logged; `volatile` documented) | §8, §9 |
| `adb-store` | One Lance table + one MemWAL shard per partition: `durable_write(false)`, no periodic WAL flush, `maintained_indexes` = `id` + `subject_id`, **ADB-driven seals** (row/byte limit or 30 s), one `put` per batch with `_meta`; recovery from `max(Raft durable offset, _meta)`; durable offset via `durable_batch_count`; WAL and orphan-generation GC | G1, R6, R11, R12, S11 |
| Schema + temporal | D3 schema and D2 supersede semantics in the applier and reads | D2, D3 |
| Links | Persisted `edge` rows; `FOLLOW LINKS` over `subject_id` / `object_id` | K1 |
| AQL execution | `aql-planner` + `adb-executor` on the new engine; all v0.5 statements; `IN SCOPE` placement | K3 |
| Subscriptions (single node) | `SUBSCRIBE` / `UNSUBSCRIBE` / `ACK` / `PUBLISH`; matcher after the apply `put`; durable registrations and cursors; in-process gateway | §11.7 |
| Surfaces | `adb-server` (gRPC + MCP) and `adb-cli` on the new engine; local dev packaging | §13 |
| FlowR | Wire `call:a3t:agent` memory recall / store; FlowR cross-instance notifications on topics | K2, D8 |
| Measure | Per-scope write ceiling with `id` + `subject_id` and the full sequencer (§6.2) | G1 |

**Exit criteria**
- [ ] AQL conformance 116/116 (+ the new v0.6 subscription tests) against ADB.
- [ ] 1,000 random kills mid-apply: 0 inconsistent tables, 0 lost acknowledged writes.
- [ ] Stale-owner, replay and durable-offset tests pass (ported R11/R12 harness).
- [ ] Links survive restart.
- [ ] Two subscribers on one topic see identical offset sequences across restarts.
- [ ] FlowR runs an agent with memory recall/store against the local dev build.

## M2 — Recall and memory lifecycle (4–5 weeks)

| Area | Build | Basis |
|---|---|---|
| Indexes | IVF_RQ + refine above ~10k vectors per partition, brute force below; FTS; built off the hot path | S8 |
| Hybrid recall | Vector + FTS + recency + link weight, RRF across scopes; evaluation set | D4 |
| Local cache | Memory + NVMe cache for Lance data, indexes and manifests, on a disk separate from the log; re-run S4 warm | S4, §10.1 |
| Compaction | Fold generations into the base table (`merge_insert` + `mark_sstables_as_compacted`, S2 guard); index maintenance policy | D5, R12 |
| Forget | Tombstones → generation compaction → MemWAL GC → delete + compact + `cleanup_old_versions`; crypto-shred log keys; forget SLA measured | S11, §14.3 |
| Continuous queries v1 | Classifier and `WATCH` subset; static watch sets; incremental filter / current-facts / count classes; timer wheel | §11.7 |

**Exit:** recall p99 within budget with a warm cache; forget leaves nothing recoverable
(byte-scan test from S11); CQ results equal a fresh query at every offset (property
test).

## M3 — Replication (5–7 weeks, needs 3 servers)

| Area | Build | Basis |
|---|---|---|
| openraft log | `=0.10.0-alpha.36` behind `adb-log`; segment-file `RaftLogStorage`; auto-purge off; election 150–300 ms, heartbeat 30 ms, pre-vote | R1, R2, R6, R7 |
| Ownership | Claims / epochs per partition; owner → leader forwarding of group-commit batches | R5, §11.4 |
| Snapshots | The partition map as snapshot; new nodes join under new ids; range-read RPC to peers | R7 |
| Membership | Learner → voter → remove, with leadership transfer before removing a leader | R8 |
| Archive and purge | Archiver; purge below the minimum durable offset after archiving; laggard exception | R9, R12 |
| Measure on servers | Commit latency (R3), leader throughput (R4), forwarding (R5), end-to-end failover and false elections (R6) | R3–R6 |
| **Fault harness (gate)** | Network partitions, clock skew, owner/leader kills, disk-full, slow fsync; linearizability checker; runs on every replication change | R10 |

**Exit:** R10 passes; owner failover p95 < 5 s and leader election < 1 s on the servers;
0 lost acknowledged writes.

## M4 — SaaS control plane (4–6 weeks)

| Area | Build | Basis |
|---|---|---|
| Control plane | Routing map + overrides, leases, owner assignment, bucket assignment | §8, §6 |
| Hibernation | **Hibernated by default**, always-on on request; partition hibernation on an always-on pool; lazy wake (claim, open, replay, rebuild, load subscriptions, prewarm); scheduled wakes for temporal boundaries | §14.1 |
| Gateway tier | Client streams, buffers, acks, catch-up, resume across failover | §11.7 |
| Tenancy | Shared and dedicated tiers, per-tenant credentials and scope checks, SSE-C keys per partition (per tenant in dedicated) | §14 |
| A3T integration | Agent liveness and listening time events for A3T billing (an agent is alive while its trigger is active) | §14.1 |

## M5 — Scale-out (ongoing)

More replica sets and cross-set handoff, load-based ownership balancing, bucket
spreading for the wake budget, hot-scope defences, CQ v1.1 (traversal re-run, top-k,
hybrid) and cross-partition CQs on a watcher tier, the object-store commit tier if
O1–O2 justify it.

---

## M0–M1 execution stages, tested with A3T at each stage

A3T reaches agent memory today through **ClawDB's server** (AQL repo,
`clawdb-server`), configured in `flowr-system/a3t/backend/resources/a3t-resources.yaml`
as `http://clawdb:3000` (REST) and `/mcp/sse` (MCP). ADB therefore ships a
**drop-in server with the same contract**, so switching A3T from ClawDB to ADB is one
URL, and every stage below is tested through the same path A3T uses.

**Server contract (kept from `clawdb-server`):**

| Endpoint | Shape |
|---|---|
| `GET /health` | liveness |
| `POST /query` | `{"query": "<AQL>", "variables"?: {…}}` → `{success, data, error}` (FlowR's `ClawDbMemory` adapter) |
| `GET /stats` | counts per memory type |
| `POST /mcp`, `GET /mcp/sse` + `POST /mcp/message` | MCP (streamable HTTP and legacy SSE) |
| gRPC `:50051` | AQL `proto/` from the AQL repo |

| Stage | Build | Test gate (incl. A3T) |
|---|---|---|
| **A. Drop-in server** (on today's engine) — ☑ *9 Oct: built; 152 Rust tests, AQL conformance 116/116 over HTTP, MCP smoke test passes on stdio / HTTP / SSE. 23 Oct: FlowR-CE → ADB integration test 18/18 (MCP and `call:adb`); ADB (`ghcr.io/srirammails/adb:2026-10-23`, `deploy/k8s/adb.yaml`) and FlowR EE `2026-10-23-ee` on vc-dev; **A3T check on the hosted single node passed** (user). In-cluster latency, keep-alive: `/query` 0.16 ms, MCP 0.21 ms median* | `adb-server` implements the contract above; MCP tools below; `adb serve` (stdio MCP) and `adb serve --http` | Rust tests; AQL conformance runner pointed at ADB's `/query`; Claude Code via stdio MCP; **A3T: point the `clawdb` resource at ADB and run an A3T chat agent with memory** |
| **B. M0 foundations** | K5 dependency upgrade, crate skeletons, `adb-core` log/effect types, D2/D3 written, CI | Everything from stage A still green |
| **C. Log + sequencer** — ☑ *25 Oct: A3T check passed (chat agent stores and reads WORKING memory through ADB's MCP tools on vc-dev; ADB pod restart keeps it). 24 Oct: local WAL (segments, CRC frames, group commit, torn-tail recovery, directory lock); durable engine (`adb --data-dir`: writes resolved to effects at their commit time, logged, then applied; replayed on start; one write-path clock); SIGTERM handled; 182 Rust tests, AQL 116/116 durable, restart reproduces identical reads, SIGKILL rounds with 0 acknowledged writes lost, FlowR integration 22/22 incl. ADB crash mid-conversation. Open: sequencer validation (key uniqueness D3 S4, OCC), pending overlay so concurrent writes share a sync, timer-committed `Expire` (carried into D). Getting there needed FlowR fixes: remote `list_mcp_tools`, `/mcp/sse` → `/mcp/message`, bare names in `a3t`, self-port check, per-tool MCP dispatch, chat pages sending `mcp_servers`, template config as defaults (flowr-system `adb-integration`)* | `adb-log` local WAL; `adb-sequencer` (validation, overlay, group commit, WORKING / TOOLS hot tier) | Crash tests (kill mid-write, restart, nothing acknowledged lost); **A3T: WORKING memory survives an ADB restart mid-conversation** |
| **D. Store (option A)** — ◐ *25 Oct: D1 `adb-store` (Lance MemWAL applier, D2 rules) and D2 dual write (log → memory + Lance; Lance catches up from the log on open) done: 187 tests, AQL 116/116 durable with 0 Lance apply failures, 30 SIGKILL rounds 0 lost. Sync Lance apply cuts debug-build write throughput ~3x (4.6k vs 15.5k acked writes in 30 rounds): move to an applier task in D3. K10 closed (Lance sort spilling bypassed). D3 step 1 done: SEMANTIC and EPISODIC read from Lance (`LanceReads`, backend query code over Lance's current rows; identical to backend reads on 13 queries), AQL 116/116 and FlowR 23/23 on Lance reads, 30 SIGKILL rounds 0 lost. D3.2 done: no full log replay on start — Lance catches up from the log's tail, then the in-memory tier (all types + links) is rebuilt from Lance's current rows; reads identical after restart; 30 SIGKILL rounds 0 lost. D3.5 done: log purged every 256 records below Lance's durable offset (MemWAL durable batch count → log offset, the R11/R12 rule); restart rebuilds from Lance + log tail — the Lance table is now the only copy of older data. D3.7 done: Lance applied by a background task, Lance reads wait for every record committed before them (read-your-writes); crash-run writes 10.7k vs 4–6k sync (15.5k before Lance). D3.6 done: AQL WHERE / KEY id pushed into the Lance scan (`adb_store::predicate_filter`; `value` is JSONB, schema v2; ADB's own `adb_json_scalar` UDF because Lance's json_get_* error on object/array fields) as a superset the backend re-checks — =, <, <=, >, >=, IN on scalars, id, data.x; not !=, CONTAINS, metadata.*, nested paths; FlowR 23/23, 30 SIGKILL rounds 0 lost (12.4k acked). D3 complete; PROCEDURAL native and links from edges move into D4 with removing `adb-backends`* | `adb-store` MemWAL applier, D3 schema, D2 supersede, persisted links; executor moved off `adb-backends` (then deleted) | Conformance 116/116 on the new engine; ported R11/R12 crash tests; **A3T: agent recalls facts and links across restarts; superseded facts hidden** |
| **E. Subscriptions** | `SUBSCRIBE` / `PUBLISH` / `ACK` with in-process gateway; MCP notifications | Two subscribers see identical sequences; **A3T: two FlowR instances receive the same topic events** |
| **F. FlowR wiring** | Port `call:clawdb` handler to `swf_runtime::TaskHandler`; `ClawDbMemory` adapter against ADB; local dev packaging | **A3T end to end: a flow with `call:a3t:agent` stores and recalls memory through ADB** |
| **G. Platform tables** (after D; can run alongside E/F) | Second table family on the log/sequencer/store (ARCHITECTURE §15.1): `create_table` / `insert` / `query` / `update` / `delete` / `list_tables` with `call:lancedb` semantics; Arrow Flight surface; FlowR `adb` platform-table backend chosen by resource type (CE / local dev keep embedded LanceDB). Starts with a wire spike: HTTP + JSON vs gRPC + protobuf vs Flight on A3T-shaped queries | **A3T on hosted ADB: the A3T flows (agents, canvases, templates, audit) run unchanged against ADB platform tables; data survives ADB restarts** |

### MCP server for testing with an LLM or the A3T chat agent

One MCP server (`adb-mcp`), three transports: **stdio** (`adb serve`, for Claude Code /
Claude Desktop), **streamable HTTP** (`POST /mcp`) and **SSE** (`/mcp/sse`, what A3T
uses today).

| Tool | Purpose | Notes |
|---|---|---|
| `aql_query` | Run any AQL statement | ClawDB-compatible; the main test tool |
| `store_memory`, `recall_memory`, `get_stats` | ClawDB's existing tools | Kept so A3T agents work unchanged |
| `remember`, `recall`, `forget`, `link` | High-level verbs an LLM uses without writing AQL | Map to `STORE` / `RECALL` / `FORGET` / `LINK` with `IN SCOPE` |
| `subscribe`, `publish` | Stage E | Deliveries as MCP notifications |

**Resources / prompts for the LLM:** the AQL v0.5 + v0.6 grammar and a short "how to
use memory" guide as MCP resources, so an LLM writes valid AQL; a prompt that walks
through store → recall → link → forget as a smoke test.

**Test scripts:** a scripted MCP client (JSON-RPC over stdio and HTTP) that replays a
fixed session and checks responses — runs in CI; the same session is then run by hand
through Claude Code and through an A3T chat agent.

## First two weeks (M0 start)

1. Push the AQL repo (v0.6 grammar, ethnum fix) so ADB's git dependency resolves
   everywhere; commit ADB's switch to it.
2. Dependency upgrade (K5): one Arrow / DataFusion version, Lance 12, stable Rust;
   `protoc` in the Dockerfile.
3. `adb-core` log record and effect types; empty `adb-log`, `adb-sequencer`,
   `adb-store` crates with traits and tests.
4. D3 schema and D2 temporal write-ups — review together before M1 code depends on them.
5. CI: build, tests, `cargo deny`, AQL conformance runner pointed at ADB, ported spike
   crash tests.
6. File the Lance bug (K7).

## Dependencies and risks to watch

| Item | Affects | Mitigation |
|---|---|---|
| **Three servers** (PLP NVMe, one location) | M3 | Order during M1 |
| Lance MemWAL is new; bug K7 | M1–M2 | Pinned `=12.0.0`, ADB-driven seals, conformance + crash tests on every upgrade |
| openraft alpha churn | M3 | Pinned exact version behind `adb-log`'s trait |
| Object-storage latency | M2 | Local cache is mandatory (§10.1) |
| ADB's `clawdb` / `clawdb-wasm` duplicate the AQL repo's | M0–M1 | Fold `clawdb`'s Lance code into `adb-store`; keep one playground (in the AQL repo) |

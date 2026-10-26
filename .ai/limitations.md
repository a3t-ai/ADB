# Known limitations — left out on purpose, to revisit

What the implementation deliberately skipped or worked around through stage D4
(25 Oct 2026). Each entry says what is missing, what it affects, and where it
should be picked up. Items fixed later move to the bottom with the commit.

## Correctness gaps

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| L1 | **No embeddings are ever stored.** AQL `STORE` has no embedding input and the durable path writes `embedding: None`; only `SemanticBackend::store_with_embedding` (unused by AQL) keeps one, in memory. | `RECALL … LIKE $vec` finds nothing; vector recall is not implemented end to end. | M2 hybrid recall (risk D4): embedding on STORE, Lance vector index. |
| L4 | **No optimistic concurrency** on UPDATE (`version` in `Patch` is always 0; supersede reads the current version under the sequencer lock instead). | Fine with one writer per partition; breaks with concurrent writers or replicas. | M3 (replication) / sequencer validation. |
| L5 | **Records that shared a key across memory types before schema v3 were already one Lance row.** The v2→v3 migration keeps the survivor under the type it was last written as; the overwritten one can't be recovered. | Only data written before 25 Oct. vc-dev had one record and migrated cleanly. | None — note for anyone migrating an old data dir. |
| L6 | **`LOOKUP … KEY … FOLLOW LINKS` returns the source record**, not the linked ones. `RECALL … WHERE … FOLLOW LINKS` works. | AQL users must use the `RECALL` form. | Executor LOOKUP path; with the M2 query rework. |
| L7 | **PROCEDURAL keeps only its node fields** (`pattern_id`, `pattern`, `steps`, `severity`, `confidence`, `source`, counts, `variables`, `metadata`). Other payload fields are dropped by the backend (in the response and in matching), though Lance stores the full payload. | `WHERE owner = …` never matches a procedure. | M2, when the backend's query code is replaced. |
| L8 | **`created_at` of an updated SEMANTIC / PROCEDURAL record is its current version's commit time** (D3 §5 maps it to `commit_ts`); `accessed_at` is the last write, not the last read (D3 drops read tracking). | Callers expecting first-creation time see the last update. Lineage creation time is in the history rows if needed. | Revisit if A3T needs first-creation time (add a column or read v1). |
| L9 | **History is not readable through AQL** (no `AS OF` / `WITH HISTORY`), by design in D2 v1. The rows are kept. | Audit / time travel only by reading the table directly. | AQL v0.6 proposal (D2 §8). |
| L11 | **Lance on local disk is not power-safe by itself** (risk K8): its local writer writes data files without fsync, and its local store has fsync off. ADB now `syncfs`es the store before every log purge, so purged records are on disk — but a power failure between purges can still leave Lance's newest version referring to unsynced data, and opening a damaged table has no recovery path (rebuild from the log only works while the log is complete). | Process crashes are safe (tested); power loss or a node reboot could need manual repair. Object storage is unaffected. | Ask Lance for fsync in its local writer (with the K7 report), or always run the store on object storage; add "open the last good version" recovery. |
| L10 | **`FORGET` is logical only.** Tombstones hide rows at once, but rows stay in Lance until compaction, MemWAL WAL entries aren't GC'd, old Lance versions aren't cleaned, and log payloads aren't crypto-shredded (D2 §5 steps 3–4). | Erasure requests aren't physically complete yet. | M2 (compaction, physical forget, §14.3 crypto-shred). |

## Performance shortcuts (fine for dev-sized data)

| # | Shortcut | Effect | Revisit |
|---|---|---|---|
| P1 | **Every Lance read builds a throwaway backend** from the matching rows and runs the old backend query code over it (`reads::View`). | CPU and allocation per read grow with the rows a filter can't exclude. | M2: queries as DataFusion plans over Arrow batches; then delete `adb-backends` (agreed 25 Oct). |
| P2 | **Filter pushdown covers only** `=`, `<`, `<=`, `>`, `>=`, `IN` on top-level scalar fields, `id`, `data.x` and `KEY id`. Not pushed: `!=`, `CONTAINS`, `STARTS_WITH`, `ENDS_WITH`, `metadata.*`, nested paths, variables, nulls, other `KEY` fields. | Those predicates load every current row of the memory type. | M2 query rework. |
| P3 | **No pushdown at all for PROCEDURAL**: its conditions see the backend's node with defaults the payload may lack. | Each procedural read loads all procedures. | With L7 / M2. |
| P4 | **`FOLLOW LINKS` / `WITH LINKS` do one Lance scan per record and one lookup per link** (N+1). | Slow on records with many links. | M2: one join over edge rows. |
| P5 | **Stats count by loading every current row** (`count_current`). | `stats` cost grows with data. | Keep counts in `_meta`, or a Lance count query. |
| P6 | **`FORGET` on SEMANTIC / PROCEDURAL scans all superseded versions** of the type to honour D2 T5; PROCEDURAL builds one view per old version. | Forget cost grows with history size. | With compaction (history pruned) or a pushed-down history filter. |
| P7 | **Only `id` has a scalar index**; filters on `lineage_id`, `subject_id`, `kind`, `memory` scan. | KEY lookups by lineage and edge reads scan the table. | Add BTree / bitmap indexes maintained by the MemWAL (D5). |
| P8 | **Writes hold the sequencer while their record is synced**, so a write waits for its own `fdatasync`; the WAL still group-commits what queues behind the lock. Measured 26 Oct (debug build, local disk): ~2.6 ms per write sequential, ~1,000 writes/s with 16 concurrent writers. | Fine for MVP traffic. | Release the sequencer before the sync with a pending overlay (later writes see in-flight ones), if a workload needs more. |
| P10 | **Startup scans every current row twice** (hydrate + subscription registry) after Lance and the WAL open: 6.9 s with 4,000 episodes in the debug build, most of it opening the store. | Slow restarts of big partitions (the crash test takes ~95 s for 20 restarts). | Compaction (W5) and a persisted live-key / registry snapshot. |
| P9 | **Release builds take ~40 min from clean** (fat LTO, 4 jobs in the pod). | Slow deploy loop after a cache wipe. | Thin LTO for dev images, or a build node. |

## Scope cut for now

| # | Item | Notes | Revisit |
|---|---|---|---|
| S1 | **One partition** (`PartitionId(0)`); `FORGET FROM ALL` / `UPDATE ALL` commit one transaction per memory type. | Not atomic across types. | M3 placement / multi-partition txns. |
| S2 | **`adb-backends` not deleted**; it still provides the WORKING / TOOLS hot tier and the query code for views. | Agreed 25 Oct. | M2. |
| S3 | ~~Lance R11/R12 crash tests not ported~~ — ported 25 Oct as `adb-store/tests/crash_recovery.rs` (SIGKILL mid-apply → table = model at `_meta`; `_meta` ≥ every claimed durable offset; fenced writer can't change the table). Still single node, local disk, no power-loss test. | — | M3 adds the Raft / multi-node variants (R6, R10). |
| S4 | **Lance MemWAL bug parked** (K7, `lancebug.md`): generations sealed during WAL replay lack `_pk_index`, breaking LSM reads. ADB-driven seals avoid it. | Must stay on ADB-driven seals until fixed upstream. | File the report; re-check on each Lance upgrade. |
| S5 | **CI deferred** (user, 24 Oct): build, workspace tests, AQL conformance, FlowR integration and the crash test run by hand before commits. | Easy to miss a regression. | Before M1 ends. |
| S6 | **`LocalEngine::replay_only` (test oracle) needs the complete log**; the log is purged below Lance's durable offset every 256 records. | Only usable in tests with < 256 records. | Fine as is; noted so nobody uses it on real data. |
| S7 | **`PATTERN` needs a bound variable** (`PATTERN $var`); a string literal doesn't parse. | Pattern recall isn't covered by the differential test. | AQL grammar / test harness with variables. |
| S8 | **Lance logs at INFO by default**, noisy at startup. | Log volume. | Set `RUST_LOG=info,lance=warn` in the manifest. |

## Subscriptions (stage E)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| E-L1 | **Each subscriber reads the log itself** (tails the applied log) instead of a matcher at apply fanning out. | Cost grows with subscribers × writes; fine for tens of subscribers on one node. | M4 cluster gateway (spec §6 matcher at apply). |
| E-L2 | **No table catch-up / `reset`**: past log retention a subscription gets `cursor_expired` and must re-subscribe. | A subscriber away for > 100k records loses its place. | Spec §4.4 + reconciliation note 2. |
| E-L3 | **An abandoned subscription pins the log** (purge keeps everything after the oldest cursor, up to the 100k-record cap). | Up to 100k extra log records kept per partition. | Subscription idle TTL / auto-expiry. |
| E-L4 | **Every `ACK` is a log record.** | Log volume with many subscribers acking often. | Batch cursors (one effect, many subscriptions) or gateway-held cursors (spec open question). |
| E-L5 | **No authorization at registration or delivery** (ADB has no credentials yet). | Anyone who can reach ADB can subscribe to any topic or scope. | §14.4 auth, with the SaaS control plane. |
| E-L7 | **`WHERE` on a subscription tests values of puts / supersedes / publishes only**; patches, forgets and links pass if scope and kind match. | A filtered scope subscription can see patches of records outside its filter. | With CQ watch sets (M2). |
| E-L8 | **A patch / forget delivery looks up the record's scope and kind in Lance**; if compaction ever removes all its rows, the change is skipped. | Only matters once compaction exists (D5 / M2). | Carry scope in `Patch` / `Forget` effects. |
| E-L9 | **No MCP server push** (`notifications/*`); MCP clients long-poll `next_events`. FlowR has no topic *trigger* yet. | Agents poll; flows can't start on a message. | FlowR wiring (stage F). |
| E-L10 | **Topic messages' TTL is a read filter only**: the expiry timer (L3, fixed) commits `Expire` for memory records, not for `event` rows. | Subscribers aren't told a message expired. | Expire events with compaction (W5). |
| E-L12 | **Keys are unique per memory type, not per (scope, memory)** as D3 S4 says: row ids are `<memory>:<key>`, so the same key in two scopes is `key_exists`. Named scopes don't apply to WORKING / TOOLS; reads without `IN SCOPE` see every scope (D3 S6's default-to-the-agent's-scope isn't applied). | Use scope-distinct keys. | Scope in the row id (schema v4) with tenancy (M4). |
| E-L11 | **The expiry timer scans for due rows every second** (a Lance scan with `ttl_at IS NOT NULL`). | Cost grows with TTL'd rows awaiting expiry. | A timer wheel fed by STORE … TTL and seeded at open. |

## FlowR wiring (stage F)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| F-L1 | **SEMANTIC recall is "newest 5 facts in the namespace"**, not relevance. | A busy namespace pushes older relevant facts out of the prompt. | M2 hybrid recall (vector + FTS). |
| F-L2 | **Procedures have no namespace** (ADB's procedural backend ignores it) and are matched in FlowR (token overlap), since `PATTERN` needs a bound variable. | Every agent with PROCEDURAL reads sees every procedure. | With L7 / M2. |
| F-L3 | **The agent's own turns are stored as WORKING (user) / EPISODIC (assistant)**, but nothing summarises them into facts. | Long-term memory grows only through the `remember` tool. | Reflection / summarisation (M2 lifecycle). |
| F-L4 | **`adb_topic` triggers can only be created through the API / `create_trigger`** — A3T's Triggers page lists webhook and cron. | No UI to set one up yet. | A3T UI. |
| F-L5 | **`adb_topic` delivers to workflow targets only** (FlowR has no agent inbox). | Point the trigger at an agent's flow. | — |
| F-L6 | **Topic trigger flows run without a caller** (`principal: None`, the runtime service). | Cedar policies for those runs apply to the service principal. | Give triggers an owner principal (as agents have). |
| F-L7 | `a3t/backend/resources/a3t-resources.yaml` is gitignored; its ADB URLs were updated to `http://adb:3000` locally only. | A fresh A3T install from another checkout still points at `clawdb`. | Commit a template without secrets. |

## Platform tables (stage G)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| G-L1 | **ADB Flight is unauthenticated** (no handshake / token). | Anything that can reach port 3001 can read and write A3T's platform tables. Fine inside the cluster network; not for exposure. | With ADB auth (M4 tenancy). |
| G-L2 | **Every platform column is UTF-8**, as in A3T's embedded LanceDB; no vector search (`call:lancedb` `search` is unsupported on both backends). | Same as today. | When A3T needs typed / vector platform columns. |
| G-L3 | **Platform writes are synchronous and serialised with memory writes** (one sequencer). | Fine for registry traffic; a heavy writer would queue behind memory writes. | Separate partition for platform tables (M3 placement). |
| G-L4 | **No compaction of platform datasets**: every write is a Lance version. | Many small versions over time. | `compact_files` / `cleanup_old_versions` with D5. |
| G-L5 | **Switching a running install to ADB is manual**: `import-lancedb` then edit `a3t/a3t-platform-db` (`backend: adb`) and restart FlowR. Writes between the import and the restart go to the embedded copy. | Plan a short write freeze for the switch. | An import mode that follows FlowR writes, or a switch flow. |
| G-L6 | **FlowR's own state**: the registry (workflows, resources) and `agent:*` state (configs, sessions) are on ADB with `FLOWR_REGISTRY_ADB` (W2c); executions, queue, checkpoints, settings and non-runtime triggers stay local. | Execution history is per FlowR. | When FlowR clusters. |
| G-L7 | `call:rotate`'s knowledge-graph database (`a3t-kg.lance`) stays embedded. | — | When needed. |

## Runtimes on ADB (MVP W2)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| W2-L1 | **Shared-registry writes aren't transactions**: a state `set` and a workflow `register` are delete + insert (two log records); two FlowRs writing one key race, last write wins; `incr` isn't atomic. | A reader can briefly miss a key mid-write. Fine for one console plus runtimes that own their keys. | An ADB upsert action (one effect). |
| W2-L2 | **Reads go to ADB every time** (no cache): each `resources().get` / `workflows().get` is a Flight `do_get` with a filter (full scan of a small table). | A few ms per lookup in-cluster. | Cache with invalidation from a `table` subscription. |
| W2-L3 | **Replies carry the run output as JSON text** (`output_json`): ADB topic messages hold AQL scalars, so the MCP `publish` tool stringifies nested values. | Consumers `fromjson` it. | Object / list literals in AQL `PUBLISH`. |
| W2-L4 | **One runtime pod per deployment**: the subscription is per deployment (kept in ADB under `runtime:<ns>/<name>:triggers`), so two replicas would both read it and run each request twice. | Isolated canvases have one Sandbox pod, so fine today. | Consumer groups (competing consumers) on a subscription. |
| W2-L5 | **Shared placement** (`mode: shared`, a `Runtime` pool) isn't routed: only isolated canvases get a runtime that takes work from their topic. | Shared canvases still run on the console. | Pool runtimes subscribing to each placed canvas's topic. |
| W2-L6 | **Chat isn't relayed**: the console's chat runs agents in the console; deployed canvases take work only from their topic (`a3t/canvas-dispatch`, or any `PUBLISH` to `canvas.<ns>.<name>`). No token streaming from a runtime. | Chat with a deployed agent still runs in the console. | Chat relay over topics (request topic + reply topic with partial tokens). |
| W2-L7 | **A runtime's run history is local to its pod** (executions stay local, decision W2c); the reply and ADB memory are what survive a pod. | Runs on a deleted pod are only visible through replies / memory. | With G-L6. |
| W2-L8 | **Switching the console back off the shared registry** (unset `FLOWR_REGISTRY_ADB`) returns it to its local registry as of the switch; changes made since live only in ADB. | Copy back by hand if needed. | A switch-back that copies `flowr_*` tables to local. |
| W2-L10 | **Agent Sandbox doesn't roll a pod when its Sandbox template changes** (new image, env): the old pod keeps running (or failing) until it is deleted. | After changing the runtime image / `A3T_RUNTIME_ENV_*` / a Runtime CR's image, delete the `cv-*` / `rt-*` pods. | Controller stamps a template hash and replaces the pod (or Sandbox) when it changes. |
| W2-L9 | **Runtime pods' inbound API**: they run FlowR EE with the console's policy hook / OIDC settings via the controller's `A3T_RUNTIME_ENV_*`, but nothing calls them; a runtime's `/message` endpoint can still write the shared registry for a caller with a valid token. | Same trust as the console. | A runtime mode that serves only `/health`. |

## Recall (W4)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| W4-L1 | **One embedding model, fixed: bge-small-en-v1.5, 384 dims** (baked into the image; `ADB_EMBED_MODEL_DIR` swaps the files, not the size). The table's `embedding` column has one fixed dimension. | No larger / multilingual / hosted models (e.g. 768 / 1024 / 1536 dims, OpenAI-compatible endpoints). | **User, 26 Oct:** choose the embedding provider and its size in **ADB settings** (A3T Settings → ADB): local model or provider endpoint, dimension recorded in `_meta`; changing it re-embeds into a new column / table in the background. Not needed for the MVP. |

## Outside ADB (FlowR / A3T), found while testing

| # | Item | Notes |
|---|---|---|
| F1 | `a3t/a3t-chat-engine` (stored flow) falls back to LLM `a3t/austra`, which doesn't exist (`default/Austra` does). | Chat with an agent that has no LLM fails. User to edit. |
| F2 | Agents / templates saved with `enabled_tools: []` before the 25 Oct UI fix (`4c988de`) deny every tool. | Re-save each affected agent (turn "Restrict tools" off or pick tools). |
| F3 | When an agent has no tool list, the chat engine passes `enabled_tools: null` into `system/agent-engine`'s config; whether that overrides a list set on the template wasn't checked. | Check if template-level restrictions seem not to apply. |
| F4 | `a3t-chat-engine` hard-codes `mcp_servers` `[a3t/agent-flows]` as its fallback. | Should be `${ .mcp_servers // [...] }`. |

## Fixed (moved here when done)

| Was | Fixed in |
|---|---|
| Same key in two memory types was one Lance row (data loss on restart) | `d4f0573` (schema v3) |
| Links to a forgotten record lingered until restart | `310a3d1` (D4.1) |
| Procedural `ORDER BY` / `RETURN` ignored | `18f7a25` (D4.2) |
| `UPDATE` overwrote facts in place (no history) | `4003984` (D4.3) |
| `FORGET` missed values only in old versions (D2 T5) | `b367ac4` (D4.4a) |
| L2: STORE with a key in use replaced the record | W3 (26 Oct): `key_exists` for SEMANTIC / EPISODIC / PROCEDURAL; MCP `remember` updates |
| E-L6: every row had scope `tenant:default` | W3 (26 Oct): `IN SCOPE` on STORE / RECALL / LOOKUP / UPDATE / FORGET, MCP `scope` |
| L3: TTL expiry was a read filter only | W3 (26 Oct): expiry timer commits `Expire`, delivered as `expire` |

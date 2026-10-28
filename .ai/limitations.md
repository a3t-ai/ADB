# Known limitations — left out on purpose, to revisit

What the implementation deliberately skipped or worked around through stage D4
(25 Oct 2026). Each entry says what is missing, what it affects, and where it
should be picked up. Items fixed later move to the bottom with the commit.

## Correctness gaps

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| L5 | **Records that shared a key across memory types before schema v3 were already one Lance row.** The v2→v3 migration keeps the survivor under the type it was last written as; the overwritten one can't be recovered. | Only data written before 25 Oct. vc-dev had one record and migrated cleanly. | None — note for anyone migrating an old data dir. |
| L6 | **`LOOKUP … KEY … FOLLOW LINKS` returns the source record**, not the linked ones. `RECALL … WHERE … FOLLOW LINKS` works. | AQL users must use the `RECALL` form. | Executor LOOKUP path; with the M2 query rework. |
| L8 | **`created_at` of an updated SEMANTIC / PROCEDURAL record is its current version's commit time** (D3 §5 maps it to `commit_ts`); `accessed_at` is the last write, not the last read (D3 drops read tracking). | Callers expecting first-creation time see the last update. Lineage creation time is in the history rows if needed. | Revisit if A3T needs first-creation time (add a column or read v1). |
| L9 | **History is not readable through AQL** (no `AS OF` / `WITH HISTORY`), by design in D2 v1. The rows are kept. | Audit / time travel only by reading the table directly. | AQL v0.6 proposal (D2 §8). |
| L11 | **Lance on local disk is not power-safe by itself** (risk K8): its local writer writes data files without fsync. ADB `syncfs`es the store before every log purge and (W9.8) before any compaction deletes the old table or old platform versions, so nothing the log no longer holds depends on unsynced files. Still open: a power cut can leave Lance's *newest* MemWAL / manifest state referring to unsynced files, and opening a damaged table has no fallback (the log only covers what came after the last sync). | Process crashes are safe (tested); power loss or a node reboot could need manual repair of the newest state. Object storage is unaffected. | Open recovery: roll the MemWAL back to the state synced at the last purge and replay the log from there (needs Lance MemWAL internals); or object storage / replicas (M3). |

## Performance shortcuts (fine for dev-sized data)

**Deferred (user, 28 Oct 2026):** no performance work until the feature matrix is
complete; then start with W7-L5, W7-L1 and P11.

Where reads stand (release, 10k facts in the partition, median ms; ADB via HTTP
`tests/bench-reads.mjs` after a compaction, embedded LanceDB 0.40 in process after
`optimize()`, Postgres 18.4 over localhost TCP with typed columns and B-tree indexes):

| Read | ADB | LanceDB | LanceDB + scalar indexes | Postgres | Postgres, JSONB payload |
|---|---|---|---|---|---|
| LOOKUP KEY | 9.8 | 7.0 | 3.8 | 0.35 | 0.24 |
| WHERE field = (pushed down) | 43 | 7.0 | 5.4 | 0.41 | 0.33 |
| WHERE field != | 99 | 8.5 | 3.8 | 0.24 | 0.30 |
| ORDER BY LIMIT 10 | 104 | 12.8 | 12.5 | 0.33 | 8.7 |
| WITH LINKS (50 records) | 42 | 13.2 | 10.4 | 1.2 | 3.8 |
| FOLLOW LINKS | 83 | 38.9 | 31.0 | 0.91 | 0.87 |
| SCAN WORKING WINDOW | 19 | 9.9 | 5.9 | 0.27 | 0.32 |
| AGGREGATE COUNT | 36 | 3.5 | 1.5 | 0.25 | 2.7 |

The near-LanceDB reads (key, WORKING, FOLLOW) pay for the LSM merge, the current
view and HTTP; the 6–12× ones filter, sort or count on payload fields (W7-L5, W7-L2).

| # | Shortcut | Effect | Revisit |
|---|---|---|---|
| W7-L1 | **Every read scans the partition's LSM**: no index serves `memory`, `kind`, `lineage_id` or payload filters in the memtable / generations, so a read costs about the same whatever it selects (debug, 10k facts: ~150 ms for a key lookup, ~220 ms for a pushed-down WHERE, ~650 ms for a `!=`; release figures in `W7_QUERY_ENGINE.md`). | Read latency grows with the partition, not the result. | Scalar indexes on `memory` / `lineage_id` used by the LSM scan (Lance), or one table per memory type. |
| W7-L2 | **ORDER BY / LIMIT run after decoding** the candidates (no TopK in the scan). | `RECALL … ALL ORDER BY x LIMIT 10` decodes the whole memory type. | Sort keys as columns (or a UDF) and a TopK plan. |
| W7-L3 | **WORKING is read from the table like the rest** (the in-memory hot tier is gone): `SCAN FROM WORKING` costs a table scan — release, 10k facts in the partition: ~0.5 ms → 19 ms after a compaction, ~60 ms before. | Scratchpad reads are slower than in memory. | A WORKING cache, if agents need it. |
| W7-L5 | **A payload `WHERE` parses every row's JSON in the scan** (`adb_json_scalar`), whatever it selects: ~40 ms at 10k facts in release. | Payload filters cost the same on small and large results. | Frequently filtered fields as columns, or a JSON index. |
| W7-L6 | **Reads are fastest just after a compaction**: between compactions, small MemWAL batches and generations add ~2× (LOOKUP 24 vs 10 ms at 10k facts). Compaction runs after max(2,000, rows/4) writes. | Latency saw-tooths with the write rate. | Merge memtable batches in Lance; incremental compaction. |
| P11 | **No cached index or page cache of ADB's own**: every read is a Lance scan, so even a key lookup costs milliseconds where Postgres answers from B-tree pages in memory in ~0.3 ms. The gap widens with data size, since scans grow with the partition and indexed lookups don't. | Reads ~30–300× slower than Postgres at 10k rows. | After W7-L5 / W7-L1: scalar indexes on the hot columns, then an in-memory index or row cache per partition if agents need sub-millisecond reads. |
| P2 | **Filter pushdown covers only** `=`, `<`, `<=`, `>`, `>=`, `IN` on top-level scalar fields, `id`, `data.x` and `KEY id`; every condition is then checked exactly on the decoded rows (W7). Not pushed: `!=`, `CONTAINS`, `STARTS_WITH`, `ENDS_WITH`, `metadata.*`, nested paths, variables, nulls, other `KEY` fields. | Those predicates decode every current row of the memory type (without vectors). | More operators in `adb_store::filter`. |
| P3 | **No payload pushdown for PROCEDURAL and TOOLS**: their conditions see the shaped record (defaults the payload may lack). | Each read of them decodes all their rows. | Push down fields the shaping keeps as stored. |
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
| S3 | ~~Lance R11/R12 crash tests not ported~~ — ported 25 Oct as `adb-store/tests/crash_recovery.rs` (SIGKILL mid-apply → table = model at `_meta`; `_meta` ≥ every claimed durable offset; fenced writer can't change the table). Still single node, local disk, no power-loss test. | — | M3 adds the Raft / multi-node variants (R6, R10). |
| S4 | **Lance MemWAL bug parked** (K7, `lancebug.md`): generations sealed during WAL replay lack `_pk_index`, breaking LSM reads. ADB-driven seals avoid it. | Must stay on ADB-driven seals until fixed upstream. | File the report; re-check on each Lance upgrade. |
| S5 | **CI deferred** (user, 24 Oct): build, workspace tests, AQL conformance, FlowR integration and the crash test run by hand before commits. | Easy to miss a regression. | Before M1 ends. |
| S6 | **`LocalEngine::replay_only` (test oracle) needs the complete log**; the log is purged below Lance's durable offset every 256 records. | Only usable in tests with < 256 records. | Fine as is; noted so nobody uses it on real data. |
| S8 | **Lance logs at INFO by default**, noisy at startup. | Log volume. | Set `RUST_LOG=info,lance=warn` in the manifest. |

## Subscriptions (stage E)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| E-L1 | **Each subscriber reads the log itself** (tails the applied log) instead of a matcher at apply fanning out. | Cost grows with subscribers × writes; fine for tens of subscribers on one node. | M4 cluster gateway (spec §6 matcher at apply). |
| E-L4 | **Every `ACK` is a log record.** | Log volume with many subscribers acking often. | Batch cursors (one effect, many subscriptions) or gateway-held cursors (spec open question). |
| E-L5 | **No authorization at registration or delivery** (ADB has no credentials yet). | Anyone who can reach ADB can subscribe to any topic or scope. | §14.4 auth, with the SaaS control plane. |
| E-L7 | **`WHERE` on a subscription tests values of puts / supersedes / publishes only**; patches, forgets and links pass if scope and kind match. | A filtered scope subscription can see patches of records outside its filter. | With CQ watch sets (M2). |
| E-L8 | **A patch / forget delivery looks up the record's scope and kind in Lance**; if compaction ever removes all its rows, the change is skipped. | Only matters once compaction exists (D5 / M2). | Carry scope in `Patch` / `Forget` effects. |
| E-L9 | **No MCP server push** (`notifications/*`); MCP clients long-poll `next_events`. FlowR has no topic *trigger* yet. | Agents poll; flows can't start on a message. | FlowR wiring (stage F). |
| E-L10 | **Topic messages' TTL is a read filter only**: the expiry timer (L3, fixed) commits `Expire` for memory records, not for `event` rows. | Subscribers aren't told a message expired. | Expire events with compaction (W5). |
| E-L12 | **Keys are unique per memory type, not per (scope, memory)** as D3 S4 says: row ids are `<memory>:<key>`, so the same key in two scopes is `key_exists`. Named scopes don't apply to WORKING / TOOLS; reads without `IN SCOPE` see every scope (D3 S6's default-to-the-agent's-scope isn't applied). | Use scope-distinct keys. | Scope in the row id (schema v4) with tenancy (M4). |

## FlowR wiring (stage F)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| F-L2 | **FlowR matches procedures itself** (token overlap). ADB now takes `PATTERN "text"` and a bound `PATTERN $p` (W9.3); FlowR doesn't use it yet. | FlowR does its own matching. | Switch FlowR's procedure lookup to `PATTERN`. |
| F-L3 | **The agent's own turns are stored as WORKING (user) / EPISODIC (assistant)**, but nothing summarises them into facts. | Long-term memory grows only through the `remember` tool. | Reflection / summarisation (M2 lifecycle). |
| F-L4 | **`adb_topic` triggers can only be created through the API / `create_trigger`** — A3T's Triggers page lists webhook and cron. | No UI to set one up yet. | A3T UI. |
| F-L5 | **`adb_topic` delivers to workflow targets only** (FlowR has no agent inbox). | Point the trigger at an agent's flow. | — |
| F-L6 | **Topic trigger flows run without a caller** (`principal: None`, the runtime service). | Cedar policies for those runs apply to the service principal. | Give triggers an owner principal (as agents have). |
| F-L7 | `a3t/backend/resources/a3t-resources.yaml` is gitignored; its ADB URLs were updated to `http://adb:3000` locally only. | A fresh A3T install from another checkout still points at `clawdb`. | Commit a template without secrets. |

## Platform tables (stage G)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| G-L2 | **Every platform column is UTF-8**, as in A3T's embedded LanceDB; no vector search (`call:lancedb` `search` is unsupported on both backends). | Same as today. | When A3T needs typed / vector platform columns. |
| G-L3 | **Platform writes are synchronous and serialised with memory writes** (one sequencer). | Fine for registry traffic; a heavy writer would queue behind memory writes. | Separate partition for platform tables (M3 placement). |
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
| W2-L11 | **Concurrent reconciles of one changed Sandbox can recreate it 2–3 times** in the first second (each sees the old hash before the first apply lands). | A few extra seconds of runtime restart once per template change; settles (18 recreations for 5 sandboxes on 27 Oct, then none). | Serialise per object, or re-read after the delete. |
| W2-L9 | **Runtime pods' inbound API**: they run FlowR EE with the console's policy hook / OIDC settings via the controller's `A3T_RUNTIME_ENV_*`, but nothing calls them; a runtime's `/message` endpoint can still write the shared registry for a caller with a valid token. | Same trust as the console. | A runtime mode that serves only `/health`. |

## AQL / Graph (links)

What links give today (26 Oct): typed, weighted, directed edges between records of any
memory type; `LINK`, `UPDATE LINK`, `UNLINK` (v0.7), versioned, forgotten with their
records, delivered to subscribers; one-hop reads `WITH LINKS ALL | TYPE`, `FOLLOW LINKS
TYPE`, `REFLECT … FOLLOW LINKS`. A light property graph, not a graph database.

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| GR-1 | **`FOLLOW LINKS … DEPTH n` is parsed but ignored**: ADB always stops at one hop (a→b→c→d, DEPTH 1–3 all return `b`). A bug against the grammar. | No multi-hop traversal. | Breadth-first, cycle-safe, capped (e.g. 5), each record once. Small, no grammar change. |
| GR-2 | **Outgoing links only**: no "who links *to* this record". | Reverse questions need the link stored the other way. | AQL addition, e.g. `FOLLOW LINKS TYPE "t" INBOUND`; edge targets are already stored (`object_id`). |
| GR-3 | **No conditions on links** beyond `TYPE`: no `weight > x`, no order by weight, no edge properties. | Can't prefer strong links. | AQL addition: `MIN_WEIGHT w` on `FOLLOW` / `WITH LINKS`, results by link weight. |
| GR-4 | **No paths / shortest path / link counts or aggregates over links.** | No graph analytics. | Only if a use case needs it (post-MVP). |
| GR-6 | **Agents don't use links**: FlowR's memory phase never follows them into the prompt. | Links help AQL users, not chat agents. | Link weight as a ranking signal in hybrid recall (W4 T10), related facts pulled in. |
| GR-7 | **`LOOKUP … KEY … FOLLOW LINKS` returns the source record** (L6). | Use `RECALL … WHERE … FOLLOW LINKS`. | With GR-1. |
| GR-8 | **The WASM playground's built `pkg/` predates v0.6 / v0.7** (source updated; no wasm-pack here). | The browser playground rejects the new statements. | `wasm-pack build crates/clawdb-wasm` where the toolchain is. |

## Recall (W4)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| W4-L2 | **Relevance recall scans its candidates** (brute force): every `LIKE` loads the current rows passing the filters, with their vectors, and ranks them in Rust; no ANN (IVF_RQ) or inverted (BM25) index yet. | Fine below ~10k rows per memory type and filter (S8); slower above. | Lance IVF_RQ + inverted index past the threshold, with the W7 query engine. |
| W4-L3 | **Word matching is English-only and has no synonyms**: Snowball English stemming and a short stop list (W9.11; a longer stop list scored worse — "down", "out", "not" matter in agent memory); BM25 statistics are computed over the candidate set per query. | Words only back up meaning (weight 0.2). | Per-language analyzers with the inverted index (W4-L2). |
| W4-L4 | **The evaluation set is small and synthetic** (93 facts, 42 queries, English); the weights were set on it. | Real agent memory may rank differently. | Grow it from real (anonymised) A3T queries. |
| W4-L6 | **`MIN_CONFIDENCE` is cosine similarity**, which for bge-small sits around 0.5–0.65 even for unrelated text; useful thresholds are ~0.7+. | A low threshold filters little. | Calibrate per model (W4-L1). |

## Lifecycle (W5)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| W5-L1 | **FORGET rewrites the whole table before it returns** (immediate erasure, user 27 Oct): reads and writes wait for it. Measured (debug): 0.33 s at 1k row versions, 1.4 s at 10k; release several times faster. Concurrent FORGETs share one rewrite. | FORGET latency, and a pause for everyone, grow with the table. Fine for MVP sizes. | Crypto-shredding (per-lineage keys; FORGET deletes a key) or in-place generation merges with Lance support, before large tenants. |
| W5-L2 | **A forgotten record leaves a payload-free stub** (id, memory, kind, scope — no value, text or vector) so subscribers get its `forget` and replay finds the tombstone; the log keeps its `Forget` record. Stubs are never pruned. | The record's **key / id** stays as an identifier. Choose keys without personal data. | Prune stubs older than the log's retention. |
| W5-L3 | **Only FORGET erases at once.** Expired rows, unlinked edges and topic messages go at the next compaction (thresholds, nightly 03:00 UTC); their payloads stay in the log until its normal purge. | TTL / unlink are housekeeping, not erasure. | — |
| W5-L7 | **A platform DELETE leaves disk at the next compaction, not at once** (nightly 03:00 UTC, after 256 versions of a table, or the admin `compact`); the deleted row's values stay in the log until its normal purge (as W5-L3). | Data deleted in A3T (a user, a key) is on disk for up to a day. | Erase on platform DELETE for tables marked sensitive, like FORGET. |
| W5-L5 | **`FORGET FROM ALL` compacts once per memory type that matched.** | Slower than one compaction. | One compaction per statement. |
| W5-L6 | **Compaction doesn't change the size threshold logic for tombstones**: thresholds are MemWAL generations > 16 only (tombstone share isn't measured). | Many expirations wait for the nightly run. | Count tombstones in `_meta`. |

## Continuous queries (W6)

| # | Limitation | Effect | Fix |
|---|---|---|---|
| W6-L1 | **Every write to a watched memory type (and scope) re-runs the watch** after its debounce: the watch set ignores `WHERE`, and nothing is maintained incrementally. | One query per watch per debounce window while its memory type is written; many watches on a busy type cost CPU. | `WHERE` pre-filter on put values; incremental filter / count classes with the query engine (W7). |
| W6-L2 | **Frames are buffered in memory** (last 1,000 per watch). | After an ADB restart, or past the buffer, a resuming client gets a `reset` (full result), not the diffs. | Fine for MVP; persist frames only if clients need it. |
| W6-L3 | **A frame's `offset` is a lower bound under concurrent writes**: the result holds every record up to it, and may hold a few just after (they then cause an empty or smaller next diff). | Exact at quiet times; converges under load. | Reads at an exact offset (snapshot reads, W7). |
| W6-L4 | **FORGET and watches**: frames buffered in ADB are scrubbed when the watcher reaches the `Forget` (milliseconds); clients that already received a record keep their copy until the `removed` diff. | Never on disk; clients must apply `removed`. | — |
| W6-L5 | **No per-tenant limits, metering or per-watch permissions**: anyone with the ADB token can watch anything. | Fine on one trusted node. | Tenancy limits with the cluster gateway (M4). |
| W6-L6 | **No timer wheel**: results change on committed records only. TTL expiry is committed (1 s timer, W3), so TTL'd rows leave results; there is no user-set `valid_to` yet that would need a timer. | — | When user-set validity ranges arrive. |
| W6-L7 | **Single partition**; `FROM OFFSET` on a new `WATCH` is refused (resume by id). Conformance `19_watch.yaml` covers registration; diffs are covered by ADB tests. | — | Cross-partition watches (spec v2). |

## Spec completion (W9)

| # | Limitation | Effect | Fix |
|---|---|---|---|
| W9-L1 | **`PIPELINE … TIMEOUT` is enforced only with `ADB_ENFORCE_TIMEOUT=1`** (user, 28 Oct): reads take 10–100 ms, so tight budgets would return partial results. When on, each stage gets what is left of the budget; the stage that runs out is dropped (its own partial rows aren't kept), later stages are skipped, and the result carries `timed_out: true`. `TIMEOUT` on a single statement is ignored. | Budgets aren't honoured by default. | Turn it on by default with the performance work. |
| W9-L2 | **A scope subscription's `reset` carries the scope's whole current state** (every current row, after its filter) in one batch, and says nothing about which rows were deleted while the subscriber was away — it replaces what the subscriber holds. (A topic's reset is exact: the retained messages after the subscriber's position.) | A large scope gives a large first batch after a long absence. | Page the reset (`next` cursor over the table) if scopes get big. |
| W9-L3 | **A subscription's idle clock is in memory**: a restart of ADB starts every subscription's 7 days (`ADB_SUB_IDLE`) again. | An abandoned subscription survives longer if ADB restarts often. | Persist the last read with the cursor, if it matters. |
| W9-L4 | **Embedding settings apply by restarting ADB** (it stops itself after saving; Kubernetes starts it again, a local `adb serve` must be restarted by hand), and a new model or size clears every vector at once: recall by meaning (`LIKE`) is partial until the backfill re-embeds (64 records a second; progress in `GET /admin/embedding`). | A few seconds down, then degraded `LIKE` for a large store. | Side-by-side re-embedding into a second column, if it matters (user chose clear-and-re-embed, 28 Oct). |

## Hot cache (W10)

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| W10-L1 | **`LIKE` is not served from the cache**: vectors aren't kept, so recall by meaning reads Lance. | EPISODIC / SEMANTIC `LIKE` costs what it did. | Keep vectors per rule (`vectors: true`, 1.5 KB a row at 384 dims) and rank in memory. |
| W10-L2 | **Links aren't cached**: `WITH LINKS`, `FOLLOW LINKS` and LINK checks read edges from Lance. | Graph reads stay at Lance latency (parked with graph). | Edge buckets keyed by subject, with graph (GR-*). |
| W10-L3 | **Reads that need every row use the cache only when the bucket is complete**; from an incomplete one only EPISODIC newest-first `LIMIT k` (no ORDER BY) and WORKING `WINDOW LAST n` / `LAST <duration>`. A read without `IN SCOPE` needs every scope's bucket complete. | A `last: n` rule speeds up recent-first reads, not WHERE / ORDER BY / AGGREGATE over the whole memory type. | `all` (or a large `bytes`) for memory types read that way; per-field ordered indexes in buckets if needed. |
| W10-L4 | **A rules change rebuilds the cache with applies and reads held** (one scan of the kept memory types under the store's exclusive lock). | A pause of a scan's length (ms to seconds) on `PUT /admin/cache`. | Build beside the old cache and replay the applies in between. |
| W10-L5 | **Sizes are estimates** (payload JSON + ids + 256 B a row), and the cache is per process. | The pod's memory limit must leave room for `max_bytes` plus overhead. | Measure real heap per row; size `max_bytes` from the pod limit. |
| W10-L6 | **Warm-up at open scans the kept memory types** (after the catch-up, before serving). | Adds to slow restarts of big partitions (P10). | Snapshot the cache with the store, or warm lazily. |
| W10-L8 | **A read copies its bucket's rows out of the cache** before the exact predicate runs, so a read over a large complete bucket costs ~O(rows) (debug, 10k SEMANTIC kept: WHERE 145 ms, ORDER BY LIMIT 440 ms vs 1.8 ms for a key). | `keep: all` on a big memory type speeds key and newest reads, much less WHERE / ORDER BY. | Evaluate the predicate on the cached rows in place (no clone); per-field ordered indexes in buckets. |
| W10-L7 | **Scalar indexes didn't help** (spike, W10 doc): Lance reads that miss the cache still cost 10–100 ms at 10k rows (W7-L1, P7, P11 stand for misses). | Cache misses and SEMANTIC without a rule are as slow as before. | Fewer, larger LSM sources (merge generations), one table per memory type, or SEMANTIC rules. |

## Query engine (W7)

Read-speed items W7-L1, L2, L3, L5 and L6 are under Performance shortcuts.

| # | Limitation | Effect | Fix |
|---|---|---|---|
| W7-L4 | **ADB without `--data-dir` writes to a temporary directory** (removed when it stops) instead of memory. | Needs disk; same engine as durable. | — |

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
| Links couldn't be changed or removed (LINK again added a parallel link) | 26 Oct: AQL v0.7 `UPDATE LINK` / `UNLINK`, re-LINK updates the weight; MCP `unlink`; subscribers get `supersede` / `unlink` of kind `edge` |
| `ORDER BY created_at` ignored on SEMANTIC (sorted on payload fields only) | `9bcec26` |
| W2-L10: Sandbox pods kept their old template | flowr `a3t-controller`: template hash annotation; a changed hash recreates the Sandbox |
| L10: FORGET was logical only | W5 (27 Oct): FORGET compacts before it returns — table rewritten without the record (old table, versions and MemWAL files deleted), its payload redacted from the log (offsets and subscribers kept); the spike and `forget_erases_from_disk_before_it_returns` check no file holds it |
| L1: no embeddings; `RECALL … LIKE` found nothing | W4 (`d82f05d`): local bge-small embeddings on write, in the log; backfill; hybrid ranking; MCP `recall` `like` |
| F-L1: agents got the newest 5 facts | W4: `RECALL … LIKE $message` with `memory_config.semantic.{limit, min_confidence}` |
| L2: STORE with a key in use replaced the record | W3 (26 Oct): `key_exists` for SEMANTIC / EPISODIC / PROCEDURAL; MCP `remember` updates |
| E-L6: every row had scope `tenant:default` | W3 (26 Oct): `IN SCOPE` on STORE / RECALL / LOOKUP / UPDATE / FORGET, MCP `scope` |
| L3: TTL expiry was a read filter only | W3 (26 Oct): expiry timer commits `Expire`, delivered as `expire` |
| G-L4 / W5-L4: platform datasets never compacted (DELETE logical, a version per write) | W5-L4 (27 Oct): `PlatformStore::compact` (`compact_files` materialising deletions + `cleanup_old_versions`), nightly, after 256 versions, and in the admin `compact` |
| P1: a throwaway backend per read | W7 (`97e04ee`): AQL reads as one DataFusion scan of an LSM snapshot, semantics once in `adb_executor::query` |
| P4 / GR-5: N+1 link reads | W7: `WITH LINKS` / `FOLLOW LINKS` read all edges of a result in one scan, targets in one scan per memory type |
| S2: `adb-backends` not deleted | W7 (`c7846d4`): deleted; the table is the only copy and read path |
| G-L1: Flight unauthenticated | W3: one bearer token (`ADB_AUTH_TOKEN`) on HTTP (all but `/health`) and Flight; per-tenant credentials and scope checks stay with E-L5 (M4) |
| L7 / W4-L5: PROCEDURAL kept only its node fields | W9.1: extra payload fields kept and matched (`WHERE`, `KEY` on any field); `LIKE` returns shaped procedures since W7 |
| S7: `PATTERN` needed a variable (and ADB matched the variable's name) | W9.3: `PATTERN "text"` (AQL v0.7 grammar) and `$p` bound from request / pipeline variables |
| v0.6 conformance suite missing | W9.4: AQL `tests/suites/18_subscriptions.yaml`, `19_watch.yaml`, `20_in_scope.yaml` (42 tests); ADB 182/182. `IN SCOPE` on WORKING / TOOLS now says so (it said "needs a durable engine") |
| E-L2: no table catch-up past log retention (`cursor_expired`) | W9.5: the gateway answers with a `reset` (current rows of the target, as of the applied tip) and continues with deliveries; `SUBSCRIBE … FROM OFFSET` past retention registers and starts with it; SSE `event: reset`, MCP `next_events.reset`; retention cap `ADB_LOG_RETAIN` (default 100,000) |
| E-L3: an abandoned subscription pins the log | W9.6: subscriptions nobody reads or acks for 7 days (`ADB_SUB_IDLE`, seconds) are unsubscribed (checked each minute); FlowR's `adb_topic` triggers subscribe again when theirs is gone |
| L4: no optimistic concurrency; references checked outside the sequencer | W9.7: `UPDATE … WHERE … AND metadata.version = n` is a compare-and-set (WHERE evaluated under the sequencer after every earlier write is applied; 20 concurrent updaters → exactly one wins, `tests/concurrency.rs`); LINK checks both ends under the sequencer; PROCEDURAL conditions see `metadata.*`. M3 must keep the WHERE at the partition owner |
| W4-L1: one fixed embedding model | W9.10: local model or an OpenAI-compatible endpoint, model and dimension in `<data dir>/embedding.json`, set from A3T Settings → ADB Embeddings (through FlowR `/registry/adb-embedding`); the API key comes from ADB's environment (`api_key_env`, a Secret) |
| E-L11: the expiry timer scanned for due rows every second | W9.12: TTL timers in memory (fed by every committed write with a TTL, seeded from the table at open); a tick with nothing due costs nothing; each due record is checked against the table before its `Expire` commits |

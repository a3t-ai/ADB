# Known limitations — left out on purpose, to revisit

What the implementation deliberately skipped or worked around (started at stage D4,
25 Oct 2026; last pass 3 Nov). Grouped by area. Each entry says what is missing, what
it affects, and its plan, tagged:

- **MVP**: closed in the first MVP run (below);
- **Decide**: needs a decision from the user first;
- **Later**: after the first MVP run;
- **Note**: by design or no action; kept so nobody trips on it.

IDs are stable (other docs cite them); fixed items move to the bottom with the commit.

## First MVP run (30 Oct 2026, user)

**In:** the graph complete, every memory feature complete, then a Raft cluster (M3).
**Out:** edge ADB (phase 2), scope / edge caching, stitching across partitions,
central KG. KBLaM is the use case ADB is designed for, not built in this run: facts,
entities, provenance and validity should suit a triple feed to the model later.

| Area | Open in the run |
|---|---|
| Memory / AQL | — (done 30 Oct) |
| Graph | GR-4 paths (decide) |
| Cluster | Built 3 Nov: 3-node Raft replica set on vc-dev (CL-1). Left: CL-5 fault harness (the M3 gate), CL-6 measure on 3 servers, CA-24 group commit, CA-25 snapshots, CA-26 membership, CA-27 write idempotency key, CA-28 erasure on followers, L11 power-safe store, CL-3 owner checks under failover, CL-7 leader-label routing, S1 partitions (decide), S5 CI |

Gates that aren't limitations (W8 A3T pass, P1 step 1.6 — the user's A3T check of the
graph on vc-dev, still open on 3 Nov) are in `MVP_PLAN.md` and `P1_GRAPH.md`.

**Order proposed 3 Nov** (what is left in ADB; S9 push done): CL-7 leader-label routing →
CL-5 fault harness → CA-24 group commit → CA-25 snapshots + CA-26 membership →
P1 step 1.6 (user) → SEC-1 → CL-8 hibernation; then the post-MVP phases (edge ADB,
stitching, central KG + KBLaM, M5).

## Memory and AQL

| # | Limitation | Effect | Plan |
|---|---|---|---|
| E-L12-a | **No default scope from the caller** (D3 S6 says a statement without `IN SCOPE` is in the agent's scope, `agent:{id}`): it is `tenant:default`, reads without `IN SCOPE` see every scope, and a bare key without `IN SCOPE` names the default scope's record. Named scopes don't apply to WORKING / TOOLS. `MERGE ENTITY` works on the default scope's entities only. Keys can't contain `~`. | Agents that want isolation say `IN SCOPE`; FlowR doesn't yet (it isolates by `NAMESPACE`). | **Later**: the caller's agent identity on the query; FlowR on scopes if namespaces stop being enough. |
| F-L3-a | **Reflection is per exchange, by the agent's LLM, opt-in** (`memory_config.semantic.reflect`): one extra LLM call per turn (in the background); it sees the exchange and the 4 messages before it, not the whole session; facts are single-valued (a newer `lives_in` replaces the older, kept as history), with the model's keys (no entity resolution beyond reusing the recalled keys; duplicates are `MERGE`d by hand, GR-12-a). Facts about the user are `user:<owner>` with `owner`, and recall drops other owners' facts — but other ADB readers (MCP `recall`, AQL) see every owner's. No consolidation across sessions or agents. Not in A3T's Memory tab yet. | Costs a model call per turn when on. | **Later**: batch reflection (every N turns / session end), owner as a scope (E-L12-a), the A3T toggle. |
| AQL-2 | **No `STREAM BATCH` or `MERGE`** (v0.5 §22 open questions 4, 5). | Long histories are paged with `LIMIT`; cross-memory joins are a `PIPELINE`. | **Note**: not planned; nothing needs them. |
| L9-a | **History reads cover SEMANTIC and PROCEDURAL only, without `LIKE`, `PATTERN` or links** (AQL v0.10): EPISODIC, WORKING and TOOLS change in place, so they have no versions; links aren't read as of a time. Replaced versions are kept 90 days (`HISTORY`); forgotten and expired records have none. History reads scan the memory type's versions in Lance (not the hot cache). | No time travel over episodes or the graph's shape. | **Later**: links `AS OF` if the graph needs it; retention per scope with D5. |
| W9-L1 | **`PIPELINE … TIMEOUT` is enforced only with `ADB_ENFORCE_TIMEOUT=1`** (user, 28 Oct): reads take 10–100 ms, so tight budgets would return partial results. When on, each stage gets what is left of the budget; the stage that runs out is dropped (its own partial rows aren't kept), later stages are skipped, and the result carries `timed_out: true`. `TIMEOUT` on a single statement is ignored. | Budgets aren't honoured by default. | **Later**: on by default with the performance work. |
| L8 | **`created_at` of an updated SEMANTIC / PROCEDURAL record is its current version's commit time** (D3 §5 maps it to `commit_ts`); `accessed_at` is the last write, not the last read (D3 drops read tracking). | Callers expecting first-creation time see the last update; it is in the history rows. | **Note**; revisit if A3T needs first-creation time (readable with `WITH HISTORY`, AQL v0.10). |

## Graph

What links give today: typed, weighted, directed links between records of any memory
type; `LINK`, `UPDATE LINK`, `UNLINK` (v0.7), versioned, forgotten with their records,
delivered to subscribers; multi-hop, inbound / both, `MIN_WEIGHT`, `TYPE IN` / `ALL`
(AQL v0.8); entities and facts, where a fact with an object is a link (P1, 29 Oct,
`P1_GRAPH.md`). A property graph for agent memory, not a graph database.

| # | Limitation | Effect | Plan |
|---|---|---|---|
| GR-17 | **A fact stored with a future `valid_from` replaces the current one at once** (one current version per (subject, predicate)): until it begins, neither holds. `UPDATE` can't reach a fact outside its validity (it matches the current view). | Scheduling "price 12 from 1 Nov" hides today's price. | **Note**: store the change when it takes effect, or use `MULTI`; revisit (keep the replaced version current until the new one begins) if agents need scheduled changes. |
| GR-12-a | **Aliases resolve on writes and `LOOKUP KEY` only**: `RECALL … WHERE subject = alias` matches nothing; `UPDATE` of an entity's `aliases` field takes effect at the next restart; a moved fact keeps no earlier versions; no MCP tool for MERGE. Entity resolution is by hand (`MERGE`), not automatic. | Agents must merge duplicates themselves (or a later resolver does). | Later: alias-aware reads if agents need them; across agents' scopes: stitching. |
| GR-15-a | **The fact feed holds no log position**: a consumer isn't a subscription, so the log is purged past it as usual and its next read is a full snapshot; a remove names an id the consumer may not hold (a namespace filter isn't applied to removes). | A slow consumer re-reads every fact. | Later: register feed consumers like subscriptions (cursor in the log's retention). |
| GR-4 | **No paths / shortest path / link counts or aggregates over links.** | No graph analytics. | **Decide**: in the MVP or not. |
| GR-9 | **Properties beyond type and weight** (e.g. provenance) aren't readable on `LINK` links. Facts carry `source` and any other fields. | Plain links can't say where they came from. | **Note**: use facts for knowledge with provenance. |
| GR-11 | **Fact links can't be changed with `UPDATE LINK` / `UNLINK`** (P1.4): the fact row is the link. | Change the fact: store a new one (supersedes) or FORGET it. | **Note**: by design. |
| GR-16 | **Links don't cross partitions**: a link's target is a record in the same partition (one partition today, S1). | Fine on one partition. | **Later**: stitching (target by global id). |

## Durability and storage

| # | Limitation | Effect | Plan |
|---|---|---|---|
| L11 | **Lance on local disk is not power-safe by itself** (risk K8): its local writer writes data files without fsync. ADB `syncfs`es the store before every log purge and (W9.8) before any compaction deletes the old table or old platform versions, so nothing the log no longer holds depends on unsynced files. Still open: a power cut can leave Lance's *newest* MemWAL / manifest state referring to unsynced files, and opening a damaged table has no fallback (the log only covers what came after the last sync). | Process crashes are safe (tested); power loss or a node reboot could need manual repair of the newest state. Object storage is unaffected. | **MVP** with M3 (user, W9.8): rebuild a damaged replica from its peers, or roll the MemWAL back to the last synced state and replay. |
| S4 | **Lance MemWAL bug parked** (K7, `lancebug.md`): generations sealed during WAL replay lack `_pk_index`, breaking LSM reads. ADB-driven seals avoid it. | Must stay on ADB-driven seals until fixed upstream. | **Later**: file the report; re-check on each Lance upgrade. |
| W5-L1 | **FORGET rewrites the whole table before it returns** (immediate erasure, user 27 Oct): reads and writes wait for it. Measured (debug): 0.33 s at 1k row versions, 1.4 s at 10k; release several times faster. Concurrent FORGETs share one rewrite. | FORGET latency, and a pause for everyone, grow with the table. Fine for MVP sizes. | **Later**: crypto-shredding (per-lineage keys; FORGET deletes a key) or in-place generation merges, before large tenants. |
| W5-L2 | **A forgotten record leaves a payload-free stub** (id, memory, kind, scope — no value, text or vector) so subscribers get its `forget` and replay finds the tombstone; the log keeps its `Forget` record. Stubs are never pruned. | The record's **key / id** stays as an identifier. Choose keys without personal data. | **Later**: prune stubs older than the log's retention. |
| W5-L3 | **Only FORGET erases at once.** Expired rows, unlinked links and topic messages go at the next compaction (thresholds, nightly 03:00 UTC); their payloads stay in the log until its normal purge. | TTL / unlink are housekeeping, not erasure. | **Note**. |
| W5-L7 | **A platform DELETE leaves disk at the next compaction, not at once** (nightly, after 256 versions of a table, or the admin `compact`); the deleted row's values stay in the log until its normal purge. | Data deleted in A3T (a user, a key) is on disk for up to a day. | **Later**: erase on DELETE for tables marked sensitive, like FORGET. |
| W5-L5 | **`FORGET FROM ALL` compacts once per memory type that matched.** | Slower than one compaction. | **Later**: one compaction per statement. |
| W5-L6 | **Compaction thresholds count MemWAL generations only** (> 16); the share of tombstones isn't measured. | Many expirations wait for the nightly run. | **Later**: count tombstones in `_meta`. |
| L5 | **Records that shared a key across memory types before schema v3 were already one Lance row.** The v2→v3 migration keeps the survivor under the type it was last written as. | Only data written before 25 Oct; vc-dev migrated cleanly. | **Note** for anyone migrating an old data dir. |
| S6 | **`LocalEngine::replay_only` (test oracle) needs the complete log**; the log is purged below Lance's durable offset every 256 records. | Only usable in tests with < 256 records. | **Note**: not for real data. |
| W7-L4 | **ADB without `--data-dir` writes to a temporary directory** (removed when it stops) instead of memory. | Needs disk; same engine as durable. | **Note**. |

## Cluster (M3) and partitions

| # | Limitation | Effect | Plan |
|---|---|---|---|
| CL-1 | **Replication — built (3 Nov), not yet gated.** openraft `=0.10.0-alpha.36` behind `adb-log` (`crates/adb-raft`, `.ai/M3_RAFT.md`): 3 voters, the leader runs the engine on the Raft log, followers apply it (`Replica`) and hand it over on taking the lead; Pre-Vote, election 500–1000 ms; a leader stopped with SIGTERM hands the lead over first. Tests: `tests/cluster-raft.mjs` (kills, rejoin blackouts, 0 lost writes), `cluster-a3t.mjs`, `cluster-gateway.mjs --raft`, `cluster-wake.mjs`. Open pieces are tracked as CA-22..28 in flowr-system `.ai/a3t-adb-cluster-limitations.md` and CL-5..CL-8 below. | A replica set survives a node loss; the gates below are not passed yet. | **MVP**: CL-5, CL-6, CA-24..28. |
| CL-2 | **Leader routing is by readiness** (CA-22): one Kubernetes Service in front; `/ready` passes on the leader only, so followers show unready and a StatefulSet RollingUpdate stalls (`deploy/k8s/adb-roll.sh` instead). Followers do not forward. | Rancher shows two of three nodes unready; a leader change reaches clients after the 1 s probe. | **MVP**: CL-7. |
| CL-3 | **Checks that must run at the owner** (key uniqueness, `UPDATE … WHERE metadata.version = n` compare-and-set, LINK end checks, fact supersede) run in the leader's one sequencer. Not yet tested across a failover. | Correct by construction while one sequencer writes; unproven under failover. | **MVP**: cover them in the fault harness (CL-5). |
| CL-4 | **State held only in the process** (subscription registry and cursors in flight, watch buffers, TTL timers, live-key index, hot cache, idle clocks): a new leader rebuilds it when it opens the engine (from the follower's warm parts, 3 Nov); FlowR resubscribes within a second; notices published during the switch can be missed (CA-20), workers fall back to polling. | A short gap in wake-ups after a failover. | **MVP**: subscribers resume by offset (no gap). |
| S1 | **One partition** (`PartitionId(0)`); `FORGET FROM ALL` / `UPDATE ALL` commit one transaction per memory type. | Not atomic across types. | **Decide**: the first cluster as one replica set with one partition (recommended; no cross-partition work) or several. |
| CL-5 | **No fault harness (R10, the M3 gate)**: the tests kill, restart and black out nodes, but nothing injects network partitions, clock skew, disk-full or slow fsync, and no linearizability checker runs on the results. | Raft bugs that only those faults expose are unseen. | **MVP**: build it and run it on every replication change. |
| CL-6 | **Measured on one host only**: vc-dev is one node (`cartize-dev`) and the local tests run on one pod; the M3 targets (leader election < 1 s, owner failover p95 < 5 s, commit latency R3, leader throughput R4) need 3 separate servers. Today: election 0.5–1 s, failover 2–3 s end to end, 0 lost writes. | Numbers may change with real network latency and disks. | **MVP**: measure on 3 servers. |
| CL-7 | **Leader routing by label, not readiness** (proposed 3 Nov): readiness = healthy caught-up member (all three Ready, plain rolling updates); the leader labels its own pod `a3t.io/raft-role=leader` and the `adb` Service selects on it. Needs a Role letting ADB patch its pod. | Replaces CL-2's readiness trick. | **MVP**. |
| CL-8 | **Hibernation (M4) only measured**: waking a stopped 3-node set takes 0.5–5.5 s to serve (release, 5,000 records; the first wake after writes is the slow one, likely replaying unflushed writes — not confirmed); one node alone never serves. No lazy wake on first request, no Sandbox-based ADB trio, no scheduled wakes. The core ADB (what A3T runs on) must stay always-on: everything that could wake it depends on it. | Per-tenant / per-agent ADB hibernation not available yet. | **Later** (M4): ADB trios as hibernating agent-sandbox Sandboxes woken by the edge; confirm and cut the first-wake replay. |
| G-L3 | **Platform writes are serialised with memory writes** (one sequencer). | Fine for registry traffic; a heavy writer would queue behind memory writes. | **Later**: a separate partition for platform tables. |
| W6-L7 | **Watches are single partition**; `FROM OFFSET` on a new `WATCH` is refused (resume by id). Conformance `19_watch.yaml` covers registration; diffs are covered by ADB tests. | — | **Later**: cross-partition watches. |
| E-L1 | **Each subscriber reads the log itself** (tails the applied log) instead of a matcher at apply fanning out. | Cost grows with subscribers × writes; fine for tens of subscribers on one node. | **Later**: M4 gateway (matcher at apply). |

## Subscriptions and watches

| # | Limitation | Effect | Plan |
|---|---|---|---|
| E-L4 | **Every `ACK` is a log record.** | Log volume with many subscribers acking often. | **Later**: batched or gateway-held cursors. |
| E-L7 | **`WHERE` on a subscription tests values of puts / supersedes / publishes only**; patches, forgets and links pass if scope and kind match. | A filtered scope subscription can see patches of records outside its filter. | **Later**: with watch sets that honour `WHERE` (W6-L1). |
| E-L8 | **A patch / forget delivery looks up the record's scope and kind in Lance**; if compaction removed all its rows, the change is skipped. Forgotten records keep a stub (W5-L2), so this needs a record compaction dropped entirely. | Rare. | **Later**: carry scope in `Patch` / `Forget` effects. |
| E-L9 | **No MCP server push** (`notifications/*`); MCP clients long-poll `next_events`. (FlowR's `adb_topic` trigger starts flows on a message.) | MCP agents poll. | **Later**: MCP notifications when clients support them. |
| E-L10 | **Topic messages' TTL is a read filter only**: the expiry timer commits `Expire` for memory records, not for `event` rows. | Subscribers aren't told a message expired. | **Later**. |
| W9-L2 | **A scope subscription's `reset` carries the scope's whole current state** in one batch and says nothing about rows deleted while the subscriber was away — it replaces what the subscriber holds. (A topic's reset is exact.) | A large scope gives a large first batch after a long absence. | **Later**: page the reset if scopes get big. |
| W9-L3 | **A subscription's idle clock is in memory**: a restart starts every subscription's 7 days (`ADB_SUB_IDLE`) again. | An abandoned subscription survives longer if ADB restarts often. | **Later** (see CL-4). |
| W6-L1 | **Every write to a watched memory type (and scope) re-runs the watch** after its debounce: the watch set ignores `WHERE`, and nothing is maintained incrementally. | One query per watch per debounce window while its type is written. | **Later**: `WHERE` pre-filter; incremental filter / count classes. |
| W6-L2 | **Frames are buffered in memory** (last 1,000 per watch). | After a restart, or past the buffer, a resuming client gets a `reset`, not the diffs. | **Note** for the MVP (see CL-4). |
| W6-L3 | **A frame's `offset` is a lower bound under concurrent writes**: the result may hold a few records just after it. | Exact at quiet times; converges under load. | **Later**: reads at an exact offset. |
| W6-L4 | **FORGET and watches**: frames buffered in ADB are scrubbed when the watcher reaches the `Forget`; clients that already received a record keep it until the `removed` diff. | Never on disk; clients must apply `removed`. | **Note**. |

## Recall

| # | Limitation | Effect | Plan |
|---|---|---|---|
| W4-L2 | **Relevance recall scans its candidates** (brute force): no ANN (IVF_RQ) or inverted (BM25) index. | Fine below ~10k rows per memory type and filter; slower above. | **Later** (performance): Lance IVF_RQ + inverted index past the threshold. |
| W4-L3 | **Word matching is English-only and has no synonyms**: Snowball English stemming and a short stop list (W9.11); BM25 statistics computed over the candidates per query. | Words only back up meaning (weight 0.2). | **Later**: per-language analyzers with the inverted index. |
| W4-L4 | **The evaluation set is small and synthetic** (93 facts, 42 queries, English); the weights were set on it. | Real agent memory may rank differently. | **Later**: grow it from real (anonymised) A3T queries. |
| W4-L6 | **`MIN_CONFIDENCE` is cosine similarity**, which for bge-small sits around 0.5–0.65 even for unrelated text; useful thresholds are ~0.7+. | A low threshold filters little. | **Later**: calibrate per model. |
| W9-L4 | **Embedding settings apply by restarting ADB**, and a new model or size clears every vector at once: `LIKE` is partial until the backfill re-embeds (64 records a second; progress in `GET /admin/embedding`). | A few seconds down, then degraded `LIKE` for a large store. | **Note** (user chose clear-and-re-embed, 28 Oct). |

## Performance and the hot cache (deferred)

Performance work is deferred until the feature matrix is complete (user, 28 Oct), and
scope / edge caching is out of the first MVP run (user, 30 Oct). Everything in this
section is **Later**.

Where reads stand (release, 10k facts, median ms, `tests/bench-reads.mjs` over HTTP,
29 Oct; LanceDB / Postgres from the W7 comparison, 28 Oct: embedded LanceDB 0.40 after
`optimize()`, Postgres 18.4 over localhost TCP with typed columns and B-tree indexes):

| Read | ADB, Lance (cache miss) | ADB, from the cache (W10b) | LanceDB + scalar indexes | Postgres |
|---|---|---|---|---|
| LOOKUP KEY | 25 | 1.2 | 3.8 | 0.35 |
| WHERE field = (10k candidates) | 39 | 8.2 | 5.4 | 0.41 |
| WHERE field != | 154 | 11 | 3.8 | 0.24 |
| ORDER BY LIMIT 10 | 180 | 17 | 12.5 | 0.33 |
| AGGREGATE COUNT | 43 | 12 | 1.5 | 0.25 |
| LIKE (words, 10k) | 143 | 44 | — | — |
| SCAN WORKING WINDOW | 44 | 1.0 | 5.9 | 0.27 |
| EPISODIC newest 10 | 53 | 0.9 | — | — |
| WITH LINKS (50 records) | 61 | 50 (links from Lance; before P1.3) | 10.4 | 1.2 |
| FOLLOW LINKS | 112 | 54 (links from Lance; before P1.3) | 31.0 | 0.91 |

Lance figures are between compactions (about half right after one, W7-L6). From the
cache, the remaining cost is HTTP + AQL (~0.8 ms) plus one pass over the bucket.

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| W7-L1 | **A cache miss scans the partition's LSM**: no index serves `memory`, `kind`, `lineage_id` or payload filters in the memtable / generations. Scalar indexes didn't help (W10-L7). | Misses grow with the partition, not the result. | Fewer, larger LSM sources; one table per memory type; cache rules. |
| W10-L7 | **Scalar indexes didn't help** (W10 spike): misses still cost 25–180 ms at 10k rows in release. | Cache misses are as slow as before. | As W7-L1. |
| W7-L2 | **ORDER BY / LIMIT on Lance run after decoding** the candidates (no TopK in the scan). | A miss with `ORDER BY x LIMIT 10` decodes the whole memory type. | Sort keys as columns and a TopK plan. |
| W7-L5 | **A payload `WHERE` on Lance parses every row's JSON in the scan** (`adb_json_scalar`): ~40 ms at 10k facts. | Payload filters on a miss cost the same on small and large results. | Frequently filtered fields as typed columns. |
| W7-L6 | **Lance reads are fastest just after a compaction**: small MemWAL batches and generations add ~2× between compactions (after max(2,000, rows/4) writes). | Miss latency saw-tooths with the write rate. | Merge memtable batches; incremental compaction. |
| P2 | **Filter pushdown covers only** `=`, `<`, `<=`, `>`, `>=`, `IN` on top-level scalar fields, `id`, `data.x` and `KEY id`; every condition is then checked exactly on the decoded rows. Not pushed: `!=`, `CONTAINS`, `STARTS_WITH`, `ENDS_WITH`, `metadata.*`, nested paths, variables, nulls, other `KEY` fields. | Those predicates decode every current row of the memory type. | More operators in `adb_store::filter`. |
| P3 | **No payload pushdown for PROCEDURAL and TOOLS**: their conditions see the shaped record. | Each read of them decodes all their rows. | Push down fields the shaping keeps as stored. |
| P5 | **Stats count by loading every current row** (`count_current`). | `stats` cost grows with data. | Counts in `_meta`, or a Lance count query. |
| P6 | **`FORGET` on SEMANTIC / PROCEDURAL scans all superseded versions** of the type (D2 T5). | Forget cost grows with history size. | A pushed-down history filter. |
| P8 | **Writes hold the sequencer while their record is synced**; the WAL still group-commits what queues behind the lock. ~2.6 ms per write sequential, ~1,000 writes/s with 16 writers (debug, 26 Oct). | Fine for MVP traffic. | Pending overlay, if a workload needs more. |
| P10 | **Startup scans every current row twice** (hydrate + subscription registry): 6.9 s with 4,000 episodes (debug). | Slow restarts of big partitions. | A persisted live-key / registry snapshot. |
| W10-L3 | **Reads that need every row use the cache only when the bucket is complete**; from an incomplete one only EPISODIC newest-first `LIMIT k` and WORKING `WINDOW`. A read without `IN SCOPE` needs every scope's bucket complete. | A `last: n` rule speeds up recent-first reads only. | `all` (or a large `bytes`) for types read that way. |
| W10-L4 | **A rules change rebuilds the cache with applies and reads held.** | A pause of one scan on `PUT /admin/cache`. | Build beside the old cache. |
| W10-L5 | **Cache sizes are estimates** (payload JSON + ids + 256 B a row); the cache is per process. | The pod limit must leave room for `max_bytes` plus overhead. | Measure real heap per row. |
| W10-L6 | **Cache warm-up at open scans the kept memory types.** | Adds to P10. | Snapshot the cache, or warm lazily. |
| W10-L10 | **Reads over a kept bucket are one pass over it**: 8–17 ms at 10k rows, 44 ms for `LIKE`. | Linear past ~10k rows per bucket. | Per-field maps in buckets; cached BM25 statistics; ANN (W4-L2). |
| W10-L11 | **Vectors make cached rows ~3× larger** (768 B a row at 384 dims). | A `keep: all` SEMANTIC bucket of 100k facts is ~110 MB. | `"vectors": false` when `LIKE` isn't used on cached types. |

## Platform tables, FlowR and runtimes

| # | Limitation | Effect | Plan |
|---|---|---|---|
| G-L5 | **Switching a running install to ADB is manual**: `import-lancedb`, then `a3t/a3t-platform-db` `backend: adb`, then restart FlowR. Writes between the import and the restart go to the embedded copy. | Plan a short write freeze. | **Note** for the W8 switch; a follow-the-writes import later. |
| G-L2 | **Every platform column is UTF-8**, as in A3T's embedded LanceDB; no vector search. | Same as today. | **Later**: when A3T needs typed / vector columns. |
| G-L6 | **FlowR's own state**: registry and `agent:*` state are on ADB (W2c); executions, queue, checkpoints, settings and non-runtime triggers stay local. | Execution history is per FlowR. | **Later**: when FlowR clusters. |
| G-L7 | `call:rotate`'s knowledge-graph database (`a3t-kg.lance`) stays embedded. | — | **Later**: with the central KG. |
| G-L8 | **SQL console has no memory cap**: bounded by its row limit (≤ 10,000) and a 30 s timeout. | Superadmin-only, a deliberate risk. | **Later**: a DataFusion memory pool. |
| G-L9 | **SQL console results reach FlowR as text**; nested values as their display form. | Fine for reading. | **Later**: typed JSON if an export is wanted. |
| W2-L1 | **Shared-registry writes aren't transactions**: a state `set` and a workflow `register` are delete + insert; two FlowRs writing one key race; `incr` isn't atomic. | Fine for one console plus runtimes that own their keys. | **Later**: an ADB upsert action. |
| W2-L2 | **Registry reads go to ADB every time** (a Flight `do_get` per lookup). | A few ms per lookup in-cluster. | **Later**: cache invalidated by a `table` subscription. |
| W2-L3 | **Replies carry the run output as JSON text** (`output_json`): topic messages hold AQL scalars. | Consumers `fromjson` it. | **Later**: object / list literals in `PUBLISH`. |
| W2-L4 | **One runtime pod per deployment**: two replicas would both read the subscription and run each request twice. | Isolated canvases have one pod, so fine today. | **Later**: consumer groups. |
| W2-L5 | **Shared placement** (`mode: shared`, a `Runtime` pool) isn't routed. | Shared canvases still run on the console. | **Later**. |
| W2-L6 | **Chat isn't relayed** to deployed canvases; no token streaming from a runtime. | Chat with a deployed agent runs in the console. | **Later**: chat relay over topics. |
| W2-L7 | **A runtime's run history is local to its pod.** | Runs on a deleted pod are visible only through replies / memory. | **Later**, with G-L6. |
| W2-L8 | **Switching the console off the shared registry** returns it to its local registry as of the switch. | Copy back by hand if needed. | **Later**. |
| W2-L11 | **Concurrent reconciles of one changed Sandbox can recreate it 2–3 times** in the first second. | A few extra seconds of restart per template change. | **Later**: serialise per object. |
| F-L4 | **`adb_topic` triggers can only be created through the API / `create_trigger`**. | No A3T UI for them. | **Later**: A3T Triggers page. |
| F-L5 | **`adb_topic` delivers to workflow targets only** (no agent inbox). | Point the trigger at an agent's flow. | **Note**. |
| F-L6 | **Topic trigger flows run without a caller** (`principal: None`). | Cedar policies apply to the service principal. | **Later**: trigger owner principal. |
| F-L7 | `a3t/backend/resources/a3t-resources.yaml` is gitignored; its ADB URLs were changed locally only. | A fresh install still points at `clawdb`. | **Later**: commit a template without secrets. |

## Security and tenancy (M4)

| # | Limitation | Effect | Plan |
|---|---|---|---|
| G-L10 | **ADB has one admin token** (`ADB_AUTH_TOKEN`): anything holding it (FlowR, the CLI) can read, write and run SQL; the superadmin check is in FlowR. | One trust level. | **Later**: per-caller tokens / roles (M4). |
| SEC-1 | **Pool tokens scope the engine only** (CA-40, 3 Nov): `ADB_ENGINE_POOL_TOKENS` limits a pool's token to claiming and following its own pool, but registry and platform-table calls use the one admin token (G-L10), which every FlowR pod holds. | A compromised runtime can still read and write the registry and platform tables. | **MVP**: per-pool scopes for registry and platform actions. |
| E-L5 | **No authorization at subscription registration or delivery** beyond the token. | Any token holder can subscribe to any topic or scope. | **Later**: §14.4 with M4. |
| W6-L5 | **No per-tenant limits, metering or per-watch permissions.** | Fine on one trusted node. | **Later**: M4. |
| W2-L9 | **Runtime pods serve FlowR's full API** though nothing calls them; `/message` can write the shared registry for a caller with a valid token. | Same trust as the console. | **Later**: a runtime mode that serves only `/health`. |

## Tooling

| # | Limitation | Effect | Plan |
|---|---|---|---|
| S5 | **No CI** (user, 24 Oct): `tests/check-all.sh` is run by hand before commits and images. | Easy to miss a regression. | **MVP**, before M3: the fault harness runs on every replication change. Needs the branches pushed. |
| S9 | **The working branch was not on GitHub** — fixed 3 Nov: `m0-k5-deps` pushed to `srirammails/ADB` (push URL SSH). | — | Done; move ADB to `a3t-io/adb` with the repo reorganisation. |
| P9 | **Release builds take ~40 min from clean** (fat LTO, 4 jobs in the pod). | Slow deploy loop after a cache wipe. | **Later**: thin LTO for dev images, or a build node. |
| S8 | **Lance logs at INFO by default**; every read logs a `plan_run` line. | Log volume. | **Later**: `RUST_LOG=info,lance=warn` in the manifest. |
| GR-8 | **The WASM playground's built `pkg/` predates v0.6** (source updated; no wasm-pack here). | The browser playground rejects newer statements. | **Later**: `wasm-pack build crates/clawdb-wasm` where the toolchain is. |
| GR-10 | **The playground ignores direction and `MIN_WEIGHT`** (it parses v0.8). | Browser results differ from ADB for v0.8 link reads. | **Later**, with GR-8. |

## Deferred phases — open questions (29 Oct)

Edge ADB and the central KG are out of the first MVP run (user, 30 Oct). Each question
must be settled before its phase is designed.

| # | Question | Blocks | Recommendation so far |
|---|---|---|---|
| GQ-1 | **Can an edge serve a read that is a little behind?** | Edge ADB | Yes for other agents' writes to shared scopes; an agent's own writes always wait for their commit offset. |
| GQ-2 | **Where is query text embedded for `LIKE` on an edge?** The model is ~130 MB in every runtime container. | Edge ADB | Embed on the core (~1 ms round trip); local as an option. |
| GQ-3 | **Who produces the KBLaM key / value vectors?** ADB (needs the agentLM / harnessLM encoder and adapters), or the model side from ADB's triple feed (GR-15). | Central KG | — |
| GQ-4 | **Where are agentLM / harnessLM specified?** Nothing in the workspace describes them. | Central KG | — |
| GQ-5 | **The edge role isn't in ARCHITECTURE.md**; §13 still says "no embedded production profile" (7 Oct). | Edge ADB | Design §8 / §13 / §15 when the phase starts. |

## Outside ADB (FlowR / A3T), found while testing

| # | Item | Notes |
|---|---|---|
| F1 | `a3t/a3t-chat-engine` (stored flow) falls back to LLM `a3t/austra`, which doesn't exist (`default/Austra` does). | Chat with an agent that has no LLM fails. User to edit. |
| F2 | Agents / templates saved with `enabled_tools: []` before the 25 Oct UI fix (`4c988de`) deny every tool. | Re-save each affected agent. |
| F3 | When an agent has no tool list, the chat engine passes `enabled_tools: null` into `system/agent-engine`; whether that overrides a template's list wasn't checked. | Check if template restrictions seem not to apply. |
| F4 | `a3t-chat-engine` hard-codes `mcp_servers` `[a3t/agent-flows]` as its fallback. | Should be `${ .mcp_servers // [...] }`. |
| F5 | FlowR on vc-dev rejects the registry's Cedar policies at start (policy12–21 use `flow:deploy`, `flow:terminate`, `runtime:hibernate`, … which the schema doesn't define) and falls back to the embedded base schema (`registry_policies=0`). | Add the actions to the AESOP schema or fix the policies (FlowR). |

## Fixed (moved here when done)

| Was | Fixed in |
|---|---|
| GR-14: facts had no validity range; W6-L6: no timer for `valid_to`; L9: no history reads | AQL v0.10 (30 Oct): `valid_from` / `valid_to` on `STORE … FACT` (and `UPDATE`), facts outside their range leave the current view (links too), the validity timer commits `begin` / `end` (also for bounds passed while down); `AS OF`, `VALID AT`, `WITH HISTORY` on RECALL / LOOKUP; MCP `remember` takes the bounds; suite 27 |
| GR-15: no triple feed | Fact feed over Arrow Flight (30 Oct): `do_get` ticket `{"feed": "facts", "from"?, "scope"?, "namespace"?, "max"?, "wait_ms"?}`; a snapshot of the current facts, then `add` / `remove` rows (supersede = remove + add; forget, expire, `valid_to` = remove; `valid_from` = add) in log order, with subject, predicate, object / value, confidence, source, version, bounds; `next` / `tip` / `snapshot` in the schema metadata; a `from` past log retention gives a snapshot again (GR-15-a). Found on the way: `UPDATE` of a fact doubled the memory prefix on its subject / object ids, so links from an updated fact broke; fixed |
| F-L3: nothing turned the agent's turns into facts | FlowR (30 Oct): with `memory_config.semantic.reflect`, after each turn (in the background) the agent's LLM extracts the exchange's lasting facts as JSON; each is a `STORE INTO SEMANTIC FACT … NAMESPACE` with `source = "reflection"`, `owner`, `session_id`, confidence; `user` is `user:<owner>`; recall leaves out other owners' facts. Rest: F-L3-a |
| F-L2: FlowR matched procedures itself | FlowR (30 Oct) asks ADB: `RECALL FROM PROCEDURAL PATTERN $m THRESHOLD 0.2 LIMIT 1`; ADB returns the overlap score in `metadata.confidence` (as LIKE's similarity), FlowR keeps its answer (≥ 0.5) / suggest thresholds; no more reading 500 procedures per turn |
| E-L12: keys unique per memory type, not per (scope, memory); GR-13-a: `FORGET … IN SCOPE` left other scopes' facts about the entity | Schema v4 (30 Oct): a record in a named scope has the id `<scope>~<key>` (the default scope's ids are the keys, so existing data keeps them); `KEY id` / `WHERE id` with `IN SCOPE` take the bare key; facts and the entities at their ends are per scope, so one subject and predicate has a current fact per scope and a forget cascades within its scope (that scope's entity is the only one its facts point at); the fact feed gives bare subjects. On open, rows an older ADB wrote in a named scope are rewritten to scoped ids after the log's tail (`_meta` v4 once done). AQL v0.10 §5, suite 29. Rest: E-L12-a |
| GR-13: forgetting an entity left facts pointing at it | 30 Oct: `FORGET FROM SEMANTIC` of an entity also forgets every fact about it (subject or object, every version) in the same log record, so subscribers, the fact feed, the cache and erasure see them go; the count includes them. LINK edges were already removed with the record. Scope caveat (GR-13-a): fixed with E-L12 |
| GR-12: no entity resolution | AQL v0.10 `MERGE ENTITY dup INTO survivor` (30 Oct, user's choices: newer fact wins, the older kept as history; aliases resolve on write): the duplicate's facts and links move, it is forgotten, its key becomes an alias in the survivor's `aliases` (rebuilt at open); facts stored with an alias and `LOOKUP KEY` of one reach the survivor; suite 28. Limits: GR-12-a |
| AQL-1: no `GROUP BY` | AQL v0.9 §1 (AQL `9c4ee2c`, ADB `7a08843`, 30 Oct): one row per group, `HAVING` / `ORDER BY` / `LIMIT` / `RETURN` on groups; parser enforces `HAVING` and `GROUP BY` need `AGGREGATE`; suite 24. The runner now checks `values` (two stale T13 expectations fixed) |
| AQL-3: `WINDOW` accepted on any statement, ignored outside `SCAN` | AQL v0.9 §2 (AQL `a0511ef`, ADB `e76fef2`): valid on `SCAN FROM WORKING` and `RECALL FROM EPISODIC` only (parse error elsewhere); on EPISODIC over episodes in commit order, before `ORDER BY` / `LIMIT` / `AGGREGATE` / `LIKE`; suite 25 |
| AQL-4: v0.5 §14 listed `WEIGHT` / `THRESHOLD` / `FROM` modifiers the grammar never had | AQL v0.9 §4: withdrawn (user: as recommended); v0.5 §14 points there |
| AQL-5: `UPDATE` couldn't remove a field | AQL v0.9 §3: `field = null` in `UPDATE` removes it (`adb_core::merge_fields`, both the supersede and patch paths); suite 26 |
| GR-1, GR-7 / L6: DEPTH ignored; LOOKUP … FOLLOW LINKS returned the source | P1.1 (`0f9ff6a`, 29 Oct): every record within n hops by hop then path weight, `_hop` / `_path_weight`, cycle-safe, capped at 5 hops / 1,000 records; LOOKUP and REFLECT the same |
| GR-2, GR-3: outbound links only, no conditions on links | P1.2 (29 Oct, AQL v0.8 `fa6d0d2`): `INBOUND` / `BOTH`, `MIN_WEIGHT`, `TYPE IN` / `ALL` on FOLLOW and WITH LINKS; REFLECT honours FOLLOW's type |
| W10-L2: links weren't cached | P1.3 (`cbdf06b`): every current link (edges and fact links) in the hot cache by both ends |
| No entities or facts (subject–predicate–object) | P1.4 (`4241735`, AQL `b03451b`): `STORE INTO SEMANTIC FACT [MULTI]`, one current fact per (subject, predicate), entity stubs, a fact with an object is a link; MCP `remember` with subject / predicate |
| GR-6: agents didn't use links | P1.5 (flowr `ff3e349`, 29 Oct): related facts about recalled entities and linked records in the prompt (weight ≥ 0.5, up to 5), fact confidence in the rank; `memory_config.semantic.{related, related_min_weight}` |
| S3: Lance R11/R12 crash tests not ported | 25 Oct: `adb-store/tests/crash_recovery.rs` (SIGKILL mid-apply → table = model at `_meta`; `_meta` ≥ every claimed durable offset; fenced writer can't change the table). Multi-node variants: CL-1 |
| W7-L3: WORKING read from the table (no in-memory tier) | W10 (`40f1949`): the hot cache keeps WORKING (10 MB per scope by default); `SCAN … WINDOW` 44 → 1.0 ms in release |
| P11: no in-memory index or row cache of ADB's own | W10 / W10b: per-node hot cache per memory type and scope, rules in `cache.json` / `PUT /admin/cache` / A3T Settings; key reads ~1 ms, scans of a kept memory type 8–17 ms at 10k. Misses: W7-L1 |
| P7: only `id` has a scalar index | W10 spike: BTrees on `lineage_id`, `memory`, `scope`, `subject_id`, `commit_ts` made no difference (W10-L7); the cache serves key lookups instead |
| W10-L1: `LIKE` not served from the cache (no vectors kept) | W10b (29 Oct): the cache keeps vectors (default on, 2 B a dimension); `LIKE` over a complete bucket ranks in memory (143 → 44 ms) |
| W10-L8: a read copied its bucket's rows out of the cache | W10b (29 Oct): rows shared with the cache, copied only when returned (WHERE 45 → 8 ms, ORDER BY LIMIT 97 → 17 ms) |
| W10-L9: SEMANTIC not cached by default | 29 Oct: default rule keeps the newest 64 MB of facts per scope; a saved `cache.json` keeps its own rules |
| Same key in two memory types was one Lance row (data loss on restart) | `d4f0573` (schema v3) |
| Links to a forgotten record lingered until restart | `310a3d1` (D4.1) |
| Procedural `ORDER BY` / `RETURN` ignored | `18f7a25` (D4.2) |
| `UPDATE` overwrote facts in place (no history) | `4003984` (D4.3) |
| `FORGET` missed values only in old versions (D2 T5) | `b367ac4` (D4.4a) |
| Links couldn't be changed or removed (LINK again added a parallel link) | 26 Oct: AQL v0.7 `UPDATE LINK` / `UNLINK`, re-LINK updates the weight; MCP `unlink`; subscribers get `supersede` / `unlink` of kind `edge` |
| `ORDER BY created_at` ignored on SEMANTIC | `9bcec26` |
| W2-L10: Sandbox pods kept their old template | flowr `a3t-controller`: template hash annotation; a changed hash recreates the Sandbox |
| L10: FORGET was logical only | W5 (27 Oct): FORGET compacts before it returns — table rewritten without the record, its payload redacted from the log; `forget_erases_from_disk_before_it_returns` checks no file holds it |
| L1: no embeddings; `RECALL … LIKE` found nothing | W4 (`d82f05d`): local bge-small embeddings on write, in the log; backfill; hybrid ranking; MCP `recall` `like` |
| F-L1: agents got the newest 5 facts | W4: `RECALL … LIKE $message` with `memory_config.semantic.{limit, min_confidence}` |
| L2: STORE with a key in use replaced the record | W3 (26 Oct): `key_exists` for SEMANTIC / EPISODIC / PROCEDURAL; MCP `remember` updates |
| E-L6: every row had scope `tenant:default` | W3 (26 Oct): `IN SCOPE` on STORE / RECALL / LOOKUP / UPDATE / FORGET, MCP `scope` |
| L3: TTL expiry was a read filter only | W3 (26 Oct): expiry timer commits `Expire`, delivered as `expire` |
| G-L4 / W5-L4: platform datasets never compacted | W5-L4 (27 Oct): `PlatformStore::compact`, nightly, after 256 versions, and in the admin `compact` |
| P1: a throwaway backend per read | W7 (`97e04ee`): AQL reads as one DataFusion scan of an LSM snapshot |
| P4 / GR-5: N+1 link reads | W7: `WITH LINKS` / `FOLLOW LINKS` read all links of a result in one scan, targets in one scan per memory type |
| S2: `adb-backends` not deleted | W7 (`c7846d4`): deleted; the table is the only copy and read path |
| G-L1: Flight unauthenticated | W3: one bearer token (`ADB_AUTH_TOKEN`) on HTTP (all but `/health`) and Flight; finer roles: G-L10, E-L5 |
| L7 / W4-L5: PROCEDURAL kept only its node fields | W9.1: extra payload fields kept and matched (`WHERE`, `KEY` on any field) |
| S7: `PATTERN` needed a variable | W9.3: `PATTERN "text"` (AQL v0.7 grammar) and bound `$p` |
| v0.6 conformance suite missing | W9.4: AQL suites 18–20 (42 tests); now 216/216 with suites 21–23 |
| E-L2: no table catch-up past log retention (`cursor_expired`) | W9.5: `reset` with the current rows, then deliveries; `ADB_LOG_RETAIN` (default 100,000) |
| E-L3: an abandoned subscription pins the log | W9.6: subscriptions idle for 7 days (`ADB_SUB_IDLE`) are dropped; FlowR's `adb_topic` triggers subscribe again |
| L4: no optimistic concurrency; references checked outside the sequencer | W9.7: `UPDATE … WHERE … AND metadata.version = n` is a compare-and-set under the sequencer (20 concurrent updaters → one wins); LINK checks both ends under the sequencer. In a cluster: CL-3 |
| W4-L1: one fixed embedding model | W9.10: local model or an OpenAI-compatible endpoint, set from A3T Settings → ADB Embeddings; API key from ADB's environment |
| E-L11: the expiry timer scanned for due rows every second | W9.12: TTL timers in memory, seeded from the table at open |

# W10 — Indexes and a per-node hot cache (28 Oct 2026)

Answers the read-latency limitations W7-L1, W7-L3, W7-L6, P7 and P11. The user
asked for indexes scoped as *scope → memory type → index*, kept as a local copy on
each node, with configurable residency (e.g. the last 30 EPISODIC records in memory,
the most recent 10 MB of WORKING).

## As built (28 Oct)

- **L1 dropped after the spike.** Lance 12's MemWAL maintains BTree, FTS and HNSW
  indexes (not bitmap); flushed generations get them and the base table's scanner
  uses them, the memtable only for a filter that is a single comparison. Built with
  BTrees on `lineage_id`, `memory`, `scope`, `subject_id`, `commit_ts` (all
  maintained): bench reads at 10k facts unchanged (debug: LOOKUP 63 → 72 ms, the
  rest within noise) — as the W7 spike found (≤ 10 %). The time is per-source scan
  overhead in the LSM (one scan per generation and memtable, block lists, plan),
  not row selection. Reverted.
- **L2 built** — `adb-store/src/hot.rs`, read path in `adb-executor/src/query.rs`:
  - fed from `PartitionStore::apply_many` (the rows each put wrote, under the store
    lock); `set_cache` fills a new cache from the table under the exclusive lock,
    at open and on a rules change (no restart);
  - buckets `(memory, scope)`, each with a *floor* (newest commit time evicted):
    rows after it are all there; no floor = complete;
  - reads (`read`, `scan`, `load`, `targets`, `by_keys`) take their candidates from
    the cache, then run exactly as on Lance's rows. Complete buckets answer
    anything; incomplete ones answer EPISODIC newest-first `LIMIT k` (no ORDER BY)
    and WORKING `WINDOW LAST n` / `LAST <duration>` when every returned row is newer
    than the floor. Otherwise the read goes to Lance (counted as a miss);
  - `ADB_CACHE=on|off|shadow` (shadow: every cached answer is also read from Lance;
    a difference fails the read and is counted);
  - rules in `<data dir>/cache.json`, `GET/PUT /admin/cache`; the expiry tick
    sweeps rows past their window / TTL;
  - defaults: WORKING 10 MB per scope, TOOLS and PROCEDURAL all, EPISODIC last 200
    per scope, SEMANTIC none (64 MB per scope from 29 Oct, W10-L9); 256 MB cap.
- **Tests:** `adb-store/tests/hot.rs` (random puts / supersedes / patches / forgets /
  expiries over 3 memory types × 3 scopes under tight rules, cache vs table after
  every 5th record, across a compaction, a sweep, a rebuild and a reopen); unit tests
  for the rules; the gates in shadow mode.

- **Measured** (`tests/bench-reads.mjs --cache off|default|all`, debug build,
  10k SEMANTIC facts, 200 EPISODIC, 200 WORKING, median ms):

  | Read | off | default rules | keep all |
  |---|---|---|---|
  | SEMANTIC LOOKUP KEY | 72 | 68 (not kept) | 1.8 |
  | WORKING SCAN WINDOW LAST 10 | 122 | 5.7 | 4.2 |
  | WORKING KEY id | 72 | 1.3 | 1.2 |
  | EPISODIC newest 10 | 130 | 5.9 | 5.4 |
  | SEMANTIC WHERE (10k rows) | 326 | 248 | 145 (W10-L8) |
  | WITH / FOLLOW LINKS | 250 / 497 | — | 232 / 371 (edges from Lance, W10-L2) |

- **W10b (29 Oct): reads in place, `LIKE` from the cache.** Rows and their decoded
  records are shared with the cache (W10-L8); vectors are kept (`"vectors": true`,
  default) so `LIKE` ranks complete buckets in memory (W10-L1); clearing vectors on
  a model change drops the cache's in the same swap. Release, 10k facts, median ms:

  | Read | Lance (cache off) | W10 keep all | W10b keep all |
  |---|---|---|---|
  | SEMANTIC LOOKUP KEY | 25 | 0.8 | 1.2 |
  | SEMANTIC WHERE (10k rows) | 39 | 45 | 8.2 |
  | WHERE != | 154 | 63 | 11 |
  | ORDER BY n DESC LIMIT 10 | 180 | 97 | 17 |
  | AGGREGATE COUNT | 43 | 45 | 12 |
  | LIKE (words, 10k) | 143 | 135 | 44 |
  | WORKING SCAN WINDOW / EPISODIC newest 10 | 44 / 53 | 2.3 / 2.9 | 1.0 / 0.9 |
  | WITH / FOLLOW LINKS (edges from Lance, W10-L2) | 61 / 112 | 84 / 99 | 50 / 54 |

## Two layers (proposal)

| Layer | Where | Serves | Fixes |
|---|---|---|---|
| **L1 — Lance scalar indexes** | On disk, in the partition's table; kept current in memtables by the MemWAL (`maintained_indexes`) | Every read that misses the cache: key lookups, scope / type selection, time order | W7-L1, P7 (reads cost what they select, not the partition size) |
| **L2 — Hot cache** | In memory, on every node that applies the log (the owner today, every replica in M3) | Reads the cache can answer completely (rules below) | W7-L3, P11, most of W7-L6 |

L1 comes first: it makes every cache miss and every cold start cheap, and the cache
warms from it.

### L1 — indexes

- BTree on `lineage_id` (KEY lookups, edges, UPDATE / FORGET targets).
- Bitmap on `memory`, `kind`, `scope` (low cardinality; scope → type selection).
- BTree on `commit_ts` (newest-first reads: `ORDER BY time DESC LIMIT k`, windows).
- All listed in `maintained_indexes` so memtables and sealed generations use them.
- **Payload fields** (W7-L5): the payload is one JSON column, so `WHERE status = …`
  has no column to index and parses every row. Config names the fields to promote
  per memory type (like a Postgres expression index on `data->>'status'`); ADB
  writes them as typed columns at apply and indexes them; filters on other fields
  keep parsing JSON.
- **Spike first (1 day):** confirm Lance 12's MemWAL maintains bitmap and multiple
  BTree indexes in memtables and that the LSM scanner uses them for `memory = … AND
  scope = …`. If not, fall back to BTree on a composite `scope_key = scope|memory`
  column, or one table per memory type.

### L2 — the hot cache

- **Built from the log, not from reads.** The applier hands every record to the cache
  right after Lance applies it, in log order. So the cache is exactly the table's
  state at the applied offset; reads keep the same barrier (read-your-writes), and
  there is no invalidation problem. A replica (M3) builds its own copy from the same
  log — the "local copy on each node".
- **Keyed `scope → memory type → entries`**, each entry the current row (decoded
  JSON + metadata, no vector unless the rule says so), plus per-bucket indexes:
  by lineage id (hash map) and by commit time (ordered).
  WORKING and TOOLS have no named scopes: their bucket key is the `namespace`
  (the agent / session), or `*` without one.
- **Rules (config):** first match wins; scope is an exact name or a glob.

  ```json
  {
    "max_bytes": "512MB",
    "rules": [
      { "memory": "working",  "scope": "*",            "keep": { "bytes": "10MB" } },
      { "memory": "episodic", "scope": "team:billing", "keep": { "last": 200 } },
      { "memory": "episodic", "scope": "*",            "keep": { "last": 30 } },
      { "memory": "tools",    "scope": "*",            "keep": "all" },
      { "memory": "semantic", "scope": "*",            "keep": "none" }
    ]
  }
  ```

  `keep`: `{last: n}` (newest n per bucket), `{bytes: x}` (newest that fit),
  `{within: "24h"}` (time window), `all`, `none`. `max_bytes` is the node-wide cap;
  past it the coldest buckets shrink first and the cache reports it.
- **Where it lives:** `<data dir>/cache.json` (like `embedding.json`), `GET/PUT
  /admin/cache`, an A3T Settings page. Changing rules rebuilds the cache in the
  background from L1 — no restart, reads fall back to Lance meanwhile.

### When a read uses the cache (completeness rules)

The cache answers only when it provably holds every row the read can return;
otherwise the read goes to Lance (L1). Never a partial answer.

| Read | Served from cache when |
|---|---|
| LOOKUP / KEY | hit → cache. Miss → Lance, unless the bucket is complete (`all`, or under its budget) |
| Newest-first `LIMIT k` (with or without WHERE) | the newest cached rows contain k matches — they are a prefix of time order, so they are the top k |
| SCAN WORKING / a window | the bucket covers the whole window (complete, or `within` ≥ window) |
| WHERE without order, AGGREGATE, LIKE | only on a complete bucket |
| Unscoped reads over a memory type | only if every bucket of that type is complete |
| History (`AS OF`, superseded versions) | never (cache holds current rows only) |

### Restart and cluster

- On open: Lance catch-up, then warm each rule from L1 (`scope = … AND memory = …
  ORDER BY commit_ts DESC LIMIT n`), then follow the applier. Reads go to Lance
  until a bucket is warm.
- M3: every replica applies the log, so every replica has its own cache; follower
  reads at their applied offset can use it the same way.

## Correctness gate

Shadow mode (as in W7): every read answered from the cache also runs on Lance and
fails on a difference. The AQL conformance suites, the crash tests and the recall
eval run in shadow mode until the cache has run clean on vc-dev.

## Steps

1. Spike: MemWAL-maintained bitmap / BTree indexes in Lance 12 (above).
2. L1 indexes + rewrite on compaction; `bench-reads.mjs` before / after.
3. Cache core: buckets, rules, byte accounting, applier hook, completeness checks.
4. Config: `cache.json`, `/admin/cache`, metrics (hits, misses, fallbacks, bytes per
   rule); A3T Settings page.
5. Warm-up on open; rebuild on rule change.
6. Shadow gate, release, roll; measurements recorded in `limitations.md`.

## Expected

Release, 10k facts: cache hits ~0.1–0.5 ms over HTTP (vs 10–100 ms now); misses with
L1 in the LanceDB-with-indexes range (1.5–30 ms) instead of scanning.

## W10b — memory budget, configurable and tunable (feature, recorded 4 Nov 2026)

**Why (user, 4 Nov):** on vc-dev the ADB leader holds **1,326 MiB** at idle against a
2 Gi pod limit (followers 560–585 MiB). The hot cache is capped at 256 MB, so most of
it is outside the cache's budget and invisible: an OOM kill would be a failover. User:
"ADB cache management should be configurable / tunable" — kept as a feature.

**What holds memory today, and what can be set:**

| Holder | Set today | Missing |
|---|---|---|
| Hot cache (L2) | `max_bytes` (256 MB), rules per `(memory, scope)`, vectors / edges on or off; live via `PUT /admin/cache`, `cache.json`, `ADB_CACHE_RULES` | its size is not reported per bucket outside `/admin/cache` |
| Lance (MemWAL memtables, sealed generations, index and metadata caches, read buffers) | `memtable_rows` (store config) | a byte budget; Lance's own cache sizes are left at their defaults |
| FlowR engine spaces (run rows, journals held in memory until archived) | — | a cap per space and node-wide; eviction of finished runs' journals to EPISODIC |
| Platform tables, live-key index, aliases, subscription and watch state, TTL timers | — | measured sizes |
| Raft (log store buffers, in-flight entries, the follower's warm parts) | `ADB_RAFT_LOG_KEEP`, snapshot interval | measured size |
| Embedder (model weights, when `ADB_EMBED_MODEL_DIR` is set) | the model choice | reported size |

**Feature:**

1. **One process budget.** `ADB_MEMORY_BUDGET` (bytes or `%` of the cgroup limit,
   *(default)* 60 % of the container's memory limit, read from
   `/sys/fs/cgroup/memory.max`; no limit → 1 GiB), split into shares per holder
   (*(default)* hot cache 40 %, Lance caches 30 %, engine spaces 20 %, the rest 10 %),
   each overridable (`ADB_MEMORY_SHARES` or `memory.json`).
2. **Every holder accounts its bytes** and gives memory back when over its share:
   the hot cache evicts by its rules (as now), Lance caches are sized from their share
   at open, engine spaces archive finished runs' journals to EPISODIC and drop them,
   the rest are measured and reported (and warned about when they grow past their
   share).
3. **Observable.** `/stats` and `GET /admin/memory`: budget, share and bytes per
   holder, cgroup usage and limit, evictions per holder; a summary line in the log
   every minute when any holder is over 80 % of its share.
4. **Tunable at runtime.** `PUT /admin/memory` changes the budget and shares without
   a restart, as `PUT /admin/cache` already does for the cache rules; changes are
   logged and kept in `memory.json`.
5. **Kubernetes-aware defaults.** The budget follows the pod's limit, so raising the
   limit in the StatefulSet raises ADB's working set with no other change; the
   readiness / leader logic is unaffected.
6. **Followers.** A follower's warm parts (table, platform tables, engine spaces it
   hands over on taking the lead) count against the same budget, so a follower that
   becomes leader does not jump past it.

**Gate:** a load (`tests/perf-a3t.mjs` plus a recall-heavy mix) with the budget set
below the working set keeps resident memory under the cgroup limit, answers stay equal
to `ADB_CACHE=shadow`, and `/admin/memory` adds up to within 10 % of the process RSS.

Tracked as **W10-L12** in `limitations.md`.


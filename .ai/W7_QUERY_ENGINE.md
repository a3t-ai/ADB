# W7 — Query engine: AQL reads as one scan plan, no in-memory backends

> MVP plan W7 (M2). Closes P1 (a throwaway backend per read), P2 (narrow pushdown),
> P3 (no pushdown for PROCEDURAL), P4 / GR-5 (N+1 link reads), S2 (`adb-backends`
> not deleted). **Decided 28 Oct (user):** where the old backends differ from the
> AQL spec, the spec and its conformance suites win (differences listed below);
> ANN / full-text indexes come after the MVP (W4-L2 stays).

## Today

A read of SEMANTIC / EPISODIC / PROCEDURAL scans the Lance LSM (with a superset
filter pushed down), decodes every candidate row *with its 384-float vector*, builds
a fresh backend (EPISODIC: a DataFusion `MemTable`; PROCEDURAL: a graph, re-running
each `store`) and runs that backend's query code. WORKING / TOOLS are read from the
in-memory backends, kept in step with the log. Links: one scan per record, one
lookup per link.

## Design

```
AQL step ──► ReadQuery { memory, predicate, modifiers, now }
               │
               ▼  adb-store: PartitionStore::query — under the store's read lock
             DataFusion plan over a snapshot TableProvider of the LSM
             (base table + flushed generations + memtables, newest per id):
               current view (not tombstoned / superseded / closed / expired)
               ∧ memory, record kinds, IN SCOPE, NAMESPACE, visibility
               ∧ pushed-down payload filter (superset; more operators — P2)
               projection: no `embedding` / `sealed` unless LIKE needs them
               │  Arrow batches
               ▼  adb_executor::query: AQL semantics once, for every memory type
             decode → records (PROCEDURAL: payload shaped as a procedure node)
             → exact WHERE / KEY / PATTERN (adb-core conditions)
             → WINDOW (SCAN) → ORDER BY (one rule) → LIMIT → aggregates / HAVING
             → links: one edge scan for all results, one target scan (P4)
             → RETURN
```

- **One snapshot per read**: the plan runs while the store's read lock is held, so a
  compaction can't delete files under it, and every row comes from one LSM snapshot.
- **The same path for all five memory types**: WORKING and TOOLS rows are already
  in Lance (applied from the log like the rest); their reads move there too, and the
  in-memory hot tier goes.
- **UPDATE / FORGET targets** use the same query (FORGET over history included).
- **`adb-backends` is deleted.** An ADB without `--data-dir` runs the same durable
  engine on a temporary directory, removed when it stops. The test oracle
  `replay_only` (log replayed into the backends) becomes "the log replayed into a
  fresh table" — still an independent path (log → apply), not the live table.
- **Shadow mode while switching** (`ADB_QUERY=shadow`): every read runs both paths
  and fails on a difference; the conformance gate runs in shadow mode until the old
  path is deleted.

## Semantics (spec wins; differences from the old backends)

| Area | New engine | Old backends |
|---|---|---|
| Default order | EPISODIC newest first; others oldest first (then id) | SEMANTIC / TOOLS: hash-map order (varied between reads) |
| ORDER BY | One rule for every type: numbers by value, strings by text; missing values last ascending, first descending (as SQL) | Same rule, three copies |
| WINDOW | SCAN FROM WORKING only (AQL v0.5 §WINDOW); `RECALL … WINDOW` refused | EPISODIC accepted `WINDOW` too |
| PROCEDURAL conditions | On the procedure node (defaults filled); no payload pushdown (defaults) | Same |
| `RETURN` on TOOLS RECALL / LOOKUP and on SCAN | Applied (spec) | Ignored: every field came back |
| WORKING `WINDOW LAST n` | The newest *n* (oldest first) | *n* in hash-map order |
| NAMESPACE on PROCEDURAL / TOOLS | Filters by the stored namespace | Never matched (their records had none) |
| WORKING `accessed_at` | The record's last write | The read time |

Anything else the shadow run finds is either fixed to the spec (and added here) or
matches already.

## Steps

1. `adb-store`: snapshot `TableProvider`, `PartitionStore::query`, projected decode.
2. `adb_executor::query` (a module, not a crate: it shares the executor's errors,
   ranker and record mapping): reads for RECALL / LOOKUP on every memory type (predicates,
   modifiers, PROCEDURAL shaping, PATTERN), SCAN windows, LOAD ranking, aggregates,
   links in batches, UPDATE / FORGET targets. `ADB_QUERY=plan | view | shadow`.
3. Executor on `adb-query`; conformance + all gates in shadow mode.
4. Delete `adb-backends` (hot tier, `DirectReads`, in-memory engine → temp dir,
   `replay_only` → fresh-table replay).
5. Measurements (read latency, rows decoded, bytes read) before / after; release.

## Gate

No in-memory views per read; AQL conformance and every gate green; shadow run
without differences before the old path goes; read latency not worse on the
vc-dev data and the 10k-row spike.

## Progress

- **28 Oct, steps 1–3**: `PartitionStore::query` (snapshot `LsmTable` provider; the
  LSM plan runs as one leaf, `LsmExec` — DataFusion's `EnforceDistribution` can't
  merge its sources' statistics); `adb_executor::query` (reads, SCAN, LOAD, targets,
  aggregates, batched links, PROCEDURAL / TOOLS shaping, PATTERN); `ADB_QUERY` =
  `plan` (default) / `view` / `shadow`; LIKE candidates through it (vectors on).
  Shadow run of the AQL conformance suites: 140 / 140 pass, one difference (TOOLS
  `RETURN`, table above).

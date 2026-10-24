# D2 — Temporal model

> **Status:** agreed 24 Oct 2026 (decisions T1–T8 as recommended) · @Sriram
> **Closes:** `RISK_BURNDOWN.md` D2. *Amended 24 Oct: expiry commits an `Expire` effect
> (§4), matching `SUBSCRIPTIONS_CQ_SPEC.md`.*
> **Builds on:** the 9 Oct decision (facts supersede by default; WORKING and TOOLS
> overwrite; transaction time from the log; "as of" queries deferred),
> `ARCHITECTURE.md` §5 (AQL mapping), §7 (table layout), §9 (write path), §11.7
> (timers), §14.1 (scheduled wakes), §14.3 (forget). Column types are D3's job; this
> document fixes what the temporal columns mean and how each AQL statement moves them.

## Decisions (agreed 24 Oct)

The rest of the document follows from these.

| # | Question | Decision |
|---|---|---|
| T1 | How is a record's identity kept across versions? | Every version is a new row with its own ULID `id`; a stable `lineage_id` (the first version's `id`) names the record. Edges and `superseded_by` point at ids; reads and `affected_count` count lineages. |
| T2 | Where does time come from? | One clock: the partition sequencer's **commit timestamp**, written into the log record. Apply and replay never read the wall clock, so a rebuilt table is identical. |
| T3 | Do agents set valid time? | Not in v1. `valid_from` / `valid_to` are set by the engine (commit times). Agent-supplied valid time (e.g. "true from 1 Nov") is a v0.6 proposal (§8). |
| T4 | What does `UPDATE` do on EPISODIC? | **Annotate in place**: fields merge into the same row (outcomes that arrive later, conformance T06-01). No new version; the log keeps the before-image. |
| T5 | Does `FORGET` remove history? | Yes. `FORGET` matches **every version** of a record, current or superseded, and removes the whole lineage, its edges and its log payloads. Forgetting a value that only survives in an old version must work. |
| T6 | Does `UPDATE` extend a TTL? | No. A new version inherits the absolute `ttl_at`; only `STORE … TTL` sets one. |
| T7 | Which time does `WINDOW LAST <duration>` use? | **Last write** (`valid_from` of the current row). Today it uses creation time; for WORKING "recent" should include items just updated. Small behaviour change; no conformance test depends on it. |
| T8 | Automatic decay? | None in v1. Expiry is only TTL; recency enters recall ranking in D4. AQL v0.5 leaves "how to forget" to ADB, so this can change without a grammar change. |

## 1. Time columns

| Column | Meaning | Set by |
|---|---|---|
| `commit_ts` | Transaction time of the write that produced this row | Sequencer (T2) |
| `log_offset` | Position of that write in the partition log | Sequencer |
| `valid_from` | When this version became current = its `commit_ts` | Engine (T3) |
| `valid_to` | When it stopped being current: the superseding version's `commit_ts`; null while current | Engine |
| `superseded_by` | `id` of the next version; null while current | Engine |
| `lineage_id` | Stable identity of the record across versions (T1) | Engine |
| `version` | 1 for the first version, +1 per supersede (or per in-place update for WORKING / TOOLS / EPISODIC) | Engine |
| `ttl_at` | Absolute expiry, `commit_ts + ttl` of the `STORE`; null = no expiry | Engine (T6) |

Transaction-time history is free: every change is a log record with `commit_ts` and
`log_offset`. Nothing else is needed for audit, replay or a later "as of" feature.

**Clock rule (T2).** On one partition `commit_ts` is strictly increasing:
`max(wall clock, previous commit_ts + 1 µs)`. Under replication (M3) the owner assigns
it and it travels in the replicated log record, so followers and replays agree. TTL
expiry and `valid_to` comparisons at read time use the reader's clock against these
stored values; that is the only place wall-clock time enters.

## 2. Per memory type

| Memory | `STORE` | `UPDATE` | History |
|---|---|---|---|
| **SEMANTIC** | New lineage, version 1 | **Supersede**: for each matched current row, write a new version (fields merged), set the old row's `valid_to` and `superseded_by` | Old versions kept, hidden from current reads |
| **PROCEDURAL** | New lineage, version 1 | **Supersede** (as SEMANTIC). `version` increments, so conformance T06-06 (`version >= 2`) holds | Kept |
| **EPISODIC** | New lineage, append-only | **Annotate in place** (T4): merge fields into the same row, `version` +1 | Log only |
| **WORKING** | New lineage, TTL usual | **Overwrite in place**, `version` +1; expired rows don't match (T06-07: `affected_count = 0`) | Log only (none if the scope is `volatile`) |
| **TOOLS** | New lineage | **Overwrite in place** (scores and rankings change often) | Log only |

**Why supersede only SEMANTIC and PROCEDURAL.** Facts and procedures are what an agent
learns; knowing what it believed before, and when that changed, is the temporal value
ADB offers (provenance, audits, "why did the agent do that"). WORKING and TOOLS change
on every reasoning step, so versions would be noise. Episodes are already history:
annotating one with its outcome is not a new belief.

**Merge rule.** `UPDATE … (a = 1, b = 2)` sets `a` and `b` and keeps every other field
of the matched row. A field cannot be removed by `UPDATE`; that needs `FORGET` plus
`STORE`, or a later AQL addition.

**Atomicity.** A supersede is one log record carrying `[SupersedeFact(old), PutFact(new)]`
and applies in one MemWAL `put` (§9.3), so no reader ever sees two current versions or
none. Subscribers see one change at one offset.

## 3. Reads: the current view

Every read sees the **current view** unless a future modifier asks for history. A row
is current when all hold:

1. `superseded_by` is null, and `valid_to` is null or later than now;
2. `ttl_at` is null or later than now;
3. it carries no tombstone (§5).

- `RECALL`, `LOOKUP`, `SCAN`, `LOAD`, `AGGREGATE`, `FOLLOW LINKS` and `WITH LINKS` all
  read the current view. Counts and `LIMIT` apply after the filter, so a fact
  updated five times counts once.
- `ORDER BY` on a time field uses `valid_from` (last write), except EPISODIC, where
  event order is `commit_ts` of the `STORE` (annotations don't reorder events).
- **Edges** resolve through lineages: a `LINK` made to version 1 of a fact still
  reaches the fact after it is superseded. `FOLLOW LINKS` follows edges to the current
  version of each target lineage.
- **History is not readable through AQL in v1** ("as of" deferred). The rows exist, so
  a later `AS OF <time>` or `WITH HISTORY` modifier needs no data migration.

## 4. TTL

- `STORE … TTL 30s` sets `ttl_at = commit_ts + 30s` (T6: `UPDATE` keeps it). TTL is
  allowed on every memory type, as in AQL v0.5; it is the norm for WORKING.
- **Reads** hide expired rows at once (lazy check against `ttl_at`), so expiry is exact
  without a background job (conformance T05-06: gone after 500 ms).
- **Physical removal:** WORKING's in-memory map sweeps expired entries; Lance rows are
  dropped by compaction (D5). Expired data is removed by the same path as `FORGET`
  but on the compaction schedule, not the forget SLA, and log payloads are not
  crypto-shredded (TTL is housekeeping, not an erasure request). The `Expire`
  effect below marks the row for that compaction.
- **Expiry is also logged**, after the fact: when the owner's timer wheel reaches a
  `ttl_at`, it commits an `Expire(lineage)` effect through the log, so every
  subscriber and continuous query sees the expiry at one offset
  (`SUBSCRIPTIONS_CQ_SPEC.md`, temporal events). Reads don't wait for it: they hide
  the row from `ttl_at` on. A hibernated partition with a pending `ttl_at` registers a
  scheduled wake (§14.1), so the `Expire` lands when it wakes. Replay applies the
  logged `Expire` like any effect, so tables stay identical. `volatile` WORKING
  entries expire in memory only.

## 5. FORGET

`FORGET FROM <memory> WHERE …` is the erasure path (§14.3):

1. **Match** across all versions (T5): a lineage is forgotten if any of its versions
   matches the predicate. Forgetting an email address that an old fact version still
   holds removes that lineage.
2. **Remove**: one log record carries `Forget(lineage)` for each match. Apply writes
   tombstones for every version, so the current view drops them at once, and removes
   the lineage's edges (both directions).
3. **Erase**: compaction deletes the rows, MemWAL GC drops their WAL entries, and
   `cleanup_old_versions` removes old Lance versions (verified in S11); the log
   payloads are crypto-shredded under their keys (§14.3).
4. `affected_count` counts lineages (conformance: one stored record → 1; a repeat → 0;
   `FORGET FROM ALL WHERE` over two types → 2).

## 6. Mapping AQL statements to log effects

These are the effect types B4 adds to `adb-core`.

| Statement | Effects in one log record |
|---|---|
| `STORE INTO SEMANTIC / PROCEDURAL` | `PutFact` (or `PutEntity`) with new `lineage_id`, `version = 1`, `ttl_at` |
| `STORE INTO EPISODIC` | `PutEpisode` |
| `STORE INTO WORKING / TOOLS` | `PutEntity` / `PutTool` |
| `UPDATE INTO SEMANTIC / PROCEDURAL` | per matched lineage: `SupersedeFact(old_id, new_id, commit_ts)` + `PutFact(new version)` |
| `UPDATE INTO EPISODIC / WORKING / TOOLS` | per match: `PatchRecord(id, fields, version + 1)` |
| `FORGET` | per matched lineage: `Forget(lineage_id)` (tombstones all versions and edges) |
| `LINK` | `PutEdge` between lineage ids |
| TTL expiry (owner timer) | `Expire(lineage_id)`; reads already hide the row from `ttl_at` |

`UPDATE` validates against the current view at its read offset (OCC read set, §9): if
the matched lineage was superseded or forgotten before commit, the transaction
retries or fails, never writing a second current version.

## 7. Platform tables (stage G)

Platform tables (`ARCHITECTURE.md` §15.1) have **no temporal semantics**: `update`
overwrites in place, `delete` removes the row, no lineages, no TTL. `commit_ts` and
`log_offset` are still recorded, so audit and replay work the same way.

## 8. Deferred (v0.6 proposals for the AQL repo)

- `AS OF <time>` / `WITH HISTORY` on reads (data already kept).
- Agent-supplied valid time: reserved `valid_from` / `valid_to` payload fields on
  `STORE` for facts with known validity ("price valid until 1 Nov"); `valid_to` in
  the future then drives timers as above (T3).
- `WINDOW` on EPISODIC (AQL §22 open question 2), mapping to event `commit_ts`.
- Removing a field in `UPDATE`.

## 9. Conformance impact

All 116 v0.5 tests keep passing under this model: T06-01 (episodic annotate),
T06-06 (version increments), T06-07 (expired WORKING not updated), T05-06 (TTL
expiry exact), and the FORGET counts in §07. New tests to add with stage D:
superseded facts hidden from `RECALL`; edges survive a supersede; `FORGET` of a value
held only in an old version; replay produces identical `valid_from` / `ttl_at`.

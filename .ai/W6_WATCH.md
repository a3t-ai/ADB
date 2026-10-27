# W6 — Continuous queries: `WATCH` v1

> MVP plan W6 (M2). Spec: `SUBSCRIPTIONS_CQ_SPEC.md` §5, §7; AQL v0.6 §3 (grammar
> done). Builds on stage E (subscribers tail the applied log). **Decided 27 Oct
> (user: as recommended).**

## Decisions (27 Oct)

1. **Re-run on match.** A committed write that hits a watch's *static watch set*
   (memory type, `IN SCOPE`; every memory type when the query follows links) marks it
   dirty; after `DEBOUNCE` (default 100 ms, 50–500 ms) the query is re-run and the
   diff against the last result is sent. Exact for every query shape (filters,
   current facts, counts, links, `LIKE` top-k): the result after a diff *is* the query
   re-run. Incremental maintenance per class comes with the query engine (W7).
2. **Durable, resume or reset.** `WATCH` commits a registration (`Register`, kind
   `cq`: the statement, its variables, the debounce), so watches survive restarts. A
   client resumes with the offset of the last frame it applied: it gets the frames
   after it while they are buffered (the last 1,000 per watch, in memory), otherwise
   one `reset` frame with the full current result. After an ADB restart every resume
   is a `reset`.
3. **Idle watches are dropped**: one nobody has read for 24 h (`ADB_WATCH_IDLE`,
   seconds) is unwatched (committed, logged).

## Surface

| Where | What |
|---|---|
| AQL | `WATCH <RECALL / LOOKUP / SCAN / LOAD …> [DEBOUNCE 200ms] [FROM NOW]` → `{cq_id, offset, result}` (data type `subscription`). `UNWATCH "cq-id"`. `FROM OFFSET` is refused: a new watch starts now (resume an existing one by id). |
| HTTP | `GET /watches/{id}/events?from=<offset>` — SSE, one frame per event (`event: diff` / `reset`, `id: <offset>`), `Last-Event-ID` resumes; heartbeat when idle. |
| MCP | `watch` (`query`, `variables`, `debounce_ms`) → snapshot; `watch_changes` (`cq_id`, `from`, `wait_ms`) → frames; `unwatch`. |

## Frames

```
{cq_id, type: "snapshot" | "reset", offset, result}           -- full result (the query's wire result)
{cq_id, type: "diff", offset, added: [record], removed: [{id, memory_type}],
 changed: [record], order?: [id]}                             -- record results
{cq_id, type: "diff", offset, result}                         -- other results (counts, aggregates)
```

`offset`: the applied log offset the result was computed at (every record up to it
is reflected); frames of one watch have increasing offsets. Records compare by
`memory_type` + `id`; `changed` = same record, different content or version
(`accessed_at` ignored). `order` is sent when the order of the ids changed (ranked /
ordered queries).

## How (single node)

- `adb-cq`: `WatchSpec`, the watch set and its matcher, the differ, and the
  `Watcher`: one task per partition tails the applied log like a subscriber, marks
  matching watches dirty, re-runs them after their debounce through a `Runner` (the
  executor), keeps each watch's last result and frame buffer, and wakes readers.
- Registration: run the query once (errors refused before anything is committed),
  commit `Register`, then take the snapshot at the applied tip; records after the tip
  are matched. A record the watcher passed before the watch was added makes it dirty
  (no gap between snapshot and tail).
- FORGET: frames buffered in memory that hold the forgotten record are scrubbed
  when the watcher reaches the `Forget`; the next diff removes it from results.
- A log purged past the watcher (it fell behind) re-runs every watch.

## Gate

- Property test: random writes / updates / forgets / links against several watches;
  applying the frame stream to the snapshot equals re-running the query once the
  watcher has caught up (per frame only at quiet times: W6-L3).
- Resume within the buffer, `reset` past it and after restart; idle expiry; FORGET
  scrub; AQL conformance and all gates still pass.

## As built (27 Oct)

- `adb-cq`: `WatchSpec`, `WatchSet` (memory types, `IN SCOPE`, `LIKE`, ordered),
  `diff` (records by `memory_type` + `id`; `order` only for `ORDER BY` / `LIKE`
  queries — others come in storage order), `Watcher` (match, debounce, re-run,
  frame buffer, resume / reset, FORGET scrub, idle drop, `pending`).
- `LocalEngine`: `WATCH` / `UNWATCH` handled by the engine (a `cq` row committed
  through `DurableWrites::register_watch`; the query is run once before anything is
  committed); the watcher re-runs through its own `Executor`
  (`execute_statement`); watches reloaded at open.
- HTTP `GET /watches/{id}/events`, MCP `watch` / `watch_changes` / `unwatch`, guide.
- Tests: `adb-executor/tests/watch.rs` (diff lifecycle, random-write gate over 5
  watches incl. scope / ORDER BY LIMIT / links, restart → reset, FORGET scrub),
  `adb-cq` unit tests (diff, watch sets, idle), MCP and SSE tests.

Limits: W6-L1…L7 in `limitations.md`.

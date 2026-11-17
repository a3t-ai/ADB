# Metrics and logs (OB-1 to OB-10)

Status: started 9 Nov 2026. Tasks and status: the "Metrics and logs" section of the
core-features doc. Why: on 8 Nov a console query showed 95 ms and nothing on vc-dev
could say where the time went — ADB had no metrics and no per-query log in prod.

## Outputs

| Output | Where | On in prod |
|---|---|---|
| Metrics, Prometheus text | `GET /metrics` on the HTTP port (token unless `ADB_METRICS_OPEN=1`) | always |
| Metrics, JSON with p50 / p90 / p99 | `GET /admin/stats/metrics` (token) | always |
| Logs at `info` | stdout | always (prod default `info,lance=warn`) |
| Logs at `debug`, per target | stdout, `RUST_LOG=info,adb::query=debug` | while investigating |

Rules: labels come from fixed sets (an unknown action or statement is `other`; HTTP
routes are their templates), so a client can't grow the series. Logs carry no payload
values, AQL literals or tokens: a statement is logged as its fingerprint
(`adb_core::metrics::fingerprint`: quoted strings and numbers become `?`).

## Metrics (prefix `adb_`)

Built (OB-1 to OB-3, `crates/adb-core/src/metrics.rs`):

| Name | Type | Labels | Recorded in |
|---|---|---|---|
| `requests_total` | counter | transport (http, flight), op, status (ok, error, cancelled) | HTTP middleware (`adb-server/src/lib.rs`), Flight `do_action` / `do_get` / `do_put` |
| `request_duration_seconds` | histogram | transport, op | same |
| `requests_in_flight` | gauge | transport | same |
| `connections_open`, `connections_total` | gauge, counter | transport (flight) | Flight listener (each accepted stream wrapped) |
| `auth_failures_total` | counter | transport, reason (no_token, bad_token, scope) | `adb-server/src/auth.rs` |
| `queries_total` | counter | statement, memory, status | `LocalEngine::query` (all transports; a branch engine's statements are counted by its parent) |
| `query_duration_seconds` | histogram | statement, memory | same |
| `query_rows` | histogram | statement | same (rows a read returned) |
| `slow_queries_total` | counter | statement | same, over `ADB_SLOW_QUERY_MS` (default 250) |

HTTP ops are route templates (`/query`, `/mcp`, `/subscriptions/:id/events`, …); Flight
ops are `get`, `put` and `action:<name>` (`action:aql`, `action:engine`, …).

Planned: OB-4 write path (sequencer lock wait / hold, submit-to-commit, applied lag per
target, platform groups / memtable / flushes, engine ops), OB-5 Raft (role, term,
elections, indexes, replication lag, catch-up), OB-6 storage (memtables, generations,
compaction, erasure backlog and oldest tombstone, log size and purge floor with its
holder, disk free, fsync), OB-7 subscriptions / embeddings / process (cursor lag, embed
queue, allocator, RSS, files, tasks); HTTP connection counts (axum 0.7's `serve` takes
a plain listener: needs a hyper-util accept loop).

## Logs

`info` (prod): startup (version, settings without secrets, open time), role changes,
elections, takeover and hand-over times, compaction / flush / snapshot / erasure
summaries, **slow queries** (`adb::query`: statement, memory, ms, rows, fingerprint —
built), errors with codes, auth failures (rate-limited), a 60 s summary line, allocator
every 5 min (`adb::alloc`, exists).

`debug`, one line per operation:

| Target | Line |
|---|---|
| `adb::query` | each statement: statement, memory, ms, rows, ok, fingerprint (built) |
| `adb::write` | memory / platform write phases (exists) |
| `adb::engine` | engine op and write phases (exists) |
| `adb_store::scan` | each filtered LSM scan (exists) |
| `adb_store::memtable` | seals (exists) |
| `adb::request`, `adb::raft`, `adb::sub` | planned |

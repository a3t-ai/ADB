# Stage G — platform tables: decisions

> **Status:** agreed 26 Oct 2026 (user: as recommended; fallback = configuration, no auto-failover) · **Builds on:** ARCHITECTURE
> §15.1 (platform tables, decided 24 Oct), the 26 Oct product decision — **ADB is an
> optional upgrade for FlowR EE**: CE and EE default to embedded LanceDB; an EE
> install switches to ADB when configured, and the LanceDB path stays.

## What A3T stores today

A3T's platform database (`a3t-platform.lance`, `A3T_DB_PATH`, resource
`a3t/a3t-platform-db`) holds ~20 small registry tables: `template_registry`, `users`,
`agent_configs`, `resource_registry`, `canvas_specs`, `settings`, `audit_log`,
`canvas_events`, `agent_events`, `roles`, `permissions`, `orgs`, `api_keys`, … Every
column is a UTF-8 string. It is reached four ways, all opening LanceDB directly:

| Door | Operations |
|---|---|
| `call:lancedb` (A3T's CRUD flows: 15 create_table, 21 query, 16 insert, 7 update, 7 delete, list_tables, 1 search) | create_table, insert (creates the table from the first record), query (SQL filter + limit), update (filter + values), delete (filter), list_tables |
| `query_platform_table` (FlowR's MCP agent tools) | query |
| `integrity.rs` (delete dependency checks) | query |
| EE auth (`auth.rs`, `auth_routes.rs`) | query / insert on `users` |

Out of scope: FlowR's own state (`clawdflow.lance`: workflows, resources, executions,
queue — a later step, ARCHITECTURE §15 "the rest of FlowR's state"), and `call:rotate`'s
knowledge-graph database (`a3t-kg.lance`, embeddings).

## Decisions

| # | Question | Recommendation |
|---|---|---|
| G1 | **One switch for every door** | A `PlatformDb` trait in FlowR (the six operations) with two implementations: **embedded LanceDB** (today's code, unchanged) and **ADB**. All four doors go through it, so login, the console, the CRUD flows and integrity checks always see the same data. |
| G2 | **How a deployment picks ADB** | The `a3t/a3t-platform-db` resource: `type_ref: lancedb` + `db_path` = embedded (default, CE and EE); `config.backend: "adb"` + `config.url` = ADB (EE only; CE builds ignore it and stay embedded). Changing it needs a FlowR restart. |
| G3 | **What "fallback to LanceDB" means** | **Configuration, not automatic failover.** If ADB isn't configured, FlowR uses embedded LanceDB. If ADB is configured but unreachable, platform calls **fail** (retried with backoff) — they don't silently switch to a local LanceDB, which would split the data into two diverging copies (writes during the outage lost on the way back). |
| G4 | **ADB's side** | A second table family on the same engine (§15.1): each platform table is its own Lance dataset under the data dir (`platform/<table>.lance`), UTF-8 columns as today. Writes are **log effects** (`TableCreate`, `TableInsert{rows}`, `TableUpdate{filter, values}`, `TableDelete{filter}`), committed through the sequencer and applied in order, so replication (M3) gets them for free; each table remembers the last offset it applied (catch-up on open, like the memory table). Applied **synchronously** before the write returns (platform writes are rare and the next read must see them). Reads go straight to the dataset with the same Lance SQL filters, so `org_id = "…"` behaves exactly as in embedded LanceDB. |
| G5 | **Wire** (you asked for non-HTTP) | **Arrow Flight** (gRPC) as §15.1 says: `do_get` (query → Arrow batches), `do_put` (insert batches), `do_action` (create_table / update / delete / list_tables). Start with a **one-day spike**: HTTP + JSON vs Flight on A3T-shaped calls (a 1-row lookup by id + org, a 100-row list, a 1,000-row insert) in the cluster. If Flight is not slower on small calls, build Flight only; if it is, Flight for bulk and JSON for the rest. ADB keeps HTTP for AQL and MCP. |
| G5 decided | *26 Oct, after spike G1 (`spikes/G1_platform_wire.md`)* | **Every native FlowR → ADB call (`call:adb`) is Arrow Flight; ADB over MCP stays JSON** (user). Platform tables: `do_get` (query → Arrow batches), `do_put` (insert batches), `do_action` (`create_table` / `update` / `delete` / `list_tables`). AQL through `call:adb`: `do_action("aql")`, request and result bodies the same JSON as `POST /query` (AQL results are nested records, not flat tables). MCP (`/mcp`, `/mcp/sse`, JSON-RPC) unchanged. HTTP `/query` stays for tools and tests. Spike: Flight +0.2 ms on 1-row calls, 1.6× faster on 100-row lists, 3.3× on 1,000-row inserts. |
| G6 | **Moving existing data** | `adb import-lancedb <a3t-platform.lance>` (ADB CLI): copies every table into ADB platform tables through the log, once, before switching the resource. Re-runnable (`--replace`). Needed for vc-dev, which has live data. |
| G7 | **Order of work** | (1) wire spike, (2) ADB platform tables + log effects + tests, (3) Flight (or JSON) surface, (4) FlowR `PlatformDb` trait + ADB client, all four doors, (5) import tool, (6) gate. |

## Gate (stage G exit)

- ADB: platform-table ops give the same results as embedded LanceDB on the same
  inputs (differential test over A3T's own filters), survive SIGKILL / restart, and
  replay from the log.
- FlowR: the A3T CRUD flows, login, MCP agent tools and integrity checks pass against
  ADB with no flow changes; switching back to embedded LanceDB still works.
- **A3T on vc-dev:** after `import-lancedb` and switching the resource, A3T runs
  unchanged on ADB platform tables (agents, canvases, templates, audit, login), and
  the data survives ADB restarts.

## SQL console (29 Oct 2026, user)

**Decision (user):** flows and agents use AQL only. SQL is a **superadmin debug
view** of everything in ADB's Lance, over **Arrow Flight only**, with an AQL / SQL
pill in A3T's Query Console (`/db-console`).

- **ADB:** `do_get` ticket `{"sql", "limit"?}` (`adb_executor::sql`). One DataFusion
  session per query over a snapshot: `memory_raw` (the memory table as stored),
  `memory` (the same rows, `value` as JSON text via `adb_json`, `sealed` /
  `embedding` as booleans), `memory_current` (current rows only, no `_meta`) and
  every platform table by name; `information_schema` on. Reads wait for committed
  writes, as AQL reads. DDL, DML and statements refused; default 1,000 rows, at most
  10,000; 30 s timeout. Not on HTTP or MCP.
- **CLI:** `adb sql "<query>" [--flight URL] [--limit n]` (uses `ADB_AUTH_TOKEN`).
- **FlowR:** `POST /registry/adb-sql` `{url, sql, limit?}` — a signed-in human with
  the `SuperAdmin` role only (403 for other roles, agents and services); values come
  back as text.
- **A3T UI:** Query Console, ADB connection, superadmin: AQL / SQL pill; columns
  that look like secrets (`password`, `hash`, `token`, …) hidden behind "Show
  secrets".
- **Tests:** `adb-server/tests/flight.rs` `sql_over_memory_and_platform_tables`
  (platform table filter / group / order, the three memory tables read straight
  after writes, a join, `information_schema`, limit, writes refused); FlowR
  `registry::tests::adb_sql_is_for_the_superadmin_only`.

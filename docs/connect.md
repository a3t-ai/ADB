# Connecting to ADB

Every way into a running node, with a working example for each. Start a node first (see the [README](../README.md#quick-start)).

| Protocol | Where | For | Auth |
|---|---|---|---|
| [HTTP + AQL](#http-and-aql) | `POST http://<host>:3000/query` | Any AQL statement, JSON in and out | Bearer token |
| [Live updates (SSE)](#live-updates-sse) | `GET /subscriptions/:id/events`, `GET /watches/:id/events` | Topic messages; a query's result kept current | Bearer token |
| [MCP](#mcp) | `http://<host>:3000/mcp` (also `/mcp/sse`), or `adb serve` on stdio | Agents and AI tools (Claude Code, Claude Desktop, others) | Bearer token (HTTP) |
| [Arrow Flight](#arrow-flight) | `grpc://<host>:3001` | Bulk reads and writes as Arrow, SQL, AQL | Bearer token |
| [CLI](#cli) | `adb sql`, `adb query` | One statement from a shell | `ADB_AUTH_TOKEN` |
| [Health and metrics](#health-and-metrics) | `/health`, `/metrics`, `/admin/stats/metrics` | Probes and monitoring | `/health` open |

With a token set, every call sends `Authorization: Bearer <token>`. Only `/health`
(and `/ui`) answer without it.

## HTTP and AQL

```bash
curl -s http://localhost:3000/query \
  -H "Authorization: Bearer $ADB_AUTH_TOKEN" -H 'Content-Type: application/json' \
  -d '{"query": "STORE INTO SEMANTIC (key = \"acme-renewal\", content = \"Acme renews on 1 Nov\", account = \"acme\") NAMESPACE \"crm\""}'

curl -s http://localhost:3000/query \
  -H "Authorization: Bearer $ADB_AUTH_TOKEN" -H 'Content-Type: application/json' \
  -d '{"query": "RECALL FROM SEMANTIC WHERE account = $a NAMESPACE \"crm\" LIMIT 10", "variables": {"a": "acme"}}'
```

The answer is `{success, data, error?, code?, execution_time_ms}`; for a read,
`data.data.records` holds the rows. `NAMESPACE` keeps one app's or tenant's memory
apart from another's. Without it, statements use the `default` namespace.

ADB also answers ClawDB's API, so a ClawDB client (or an A3T `clawdb` resource) can
point at `http://<host>:3000` unchanged.

## Live updates (SSE)

**Topics.** `SUBSCRIBE TOPIC` returns a subscription id; read its messages as
server-sent events:

```bash
curl -s http://localhost:3000/query -H "Authorization: Bearer $ADB_AUTH_TOKEN" -H 'Content-Type: application/json' \
  -d '{"query": "SUBSCRIBE TOPIC \"orders\" NAMESPACE \"shop\""}'          # → data.data.subscription
curl -N http://localhost:3000/subscriptions/<subscription>/events -H "Authorization: Bearer $ADB_AUTH_TOKEN"
# elsewhere:
curl -s http://localhost:3000/query -H "Authorization: Bearer $ADB_AUTH_TOKEN" -H 'Content-Type: application/json' \
  -d '{"query": "PUBLISH TOPIC \"orders\" (id = \"o-1\", status = \"paid\") NAMESPACE \"shop\""}'
```

**A query kept current.** `SUBSCRIBE RECALL …` returns the result now and a `cq_id`;
`/watches/<cq_id>/events` then sends a `reset` (the whole result) and a `diff` for
each change.

Both streams number their events: send `Last-Event-ID` (or `?from=<offset>` for
watches) when reconnecting to carry on where you stopped.

## MCP

ADB is an MCP server with memory tools: `remember`, `recall`, `forget`, `link`,
`unlink`, `aql_query`, `watch`/`watch_changes`/`unwatch`, `publish`/`subscribe`/
`next_events`/`ack`/`unsubscribe`, `get_stats` (and ClawDB's `store_memory`,
`recall_memory`). It also offers a memory guide (`adb://guide/memory`), the AQL
grammar (`adb://aql/grammar`) and a `memory_smoke_test` prompt.

```bash
# Claude Code, ADB as a local process (no network, no token)
claude mcp add adb -- /path/to/adb serve --data-dir ~/.adb

# Claude Code, a running ADB over HTTP
claude mcp add --transport http adb http://localhost:3000/mcp \
  --header "Authorization: Bearer $ADB_AUTH_TOKEN"
```

Other clients take the same two forms: a command (`adb serve --data-dir <dir>`) for
stdio, or the URL `http://<host>:3000/mcp` with the `Authorization` header. Clients
that only speak the older SSE transport use `http://<host>:3000/mcp/sse`.

## Arrow Flight

For moving many rows, or reading results as Arrow tables (pandas, Polars, DuckDB).
Requests are JSON in a Flight ticket or action:

```python
import json, pyarrow as pa, pyarrow.flight as fl

c = fl.connect("grpc://localhost:3001")
opts = fl.FlightCallOptions(headers=[(b"authorization", f"Bearer {TOKEN}".encode())])

# AQL
r = list(c.do_action(fl.Action("aql", json.dumps({"query": 'RECALL FROM SEMANTIC NAMESPACE "crm" LIMIT 5'}).encode()), opts))
print(json.loads(r[0].body.to_pybytes()))

# read-only SQL over memory, answered as Arrow
t = c.do_get(fl.Ticket(json.dumps({"sql": "select id, namespace from memory_current limit 100"})), opts).read_all()

# your own table: create it, write Arrow batches, read them back
c.do_action(fl.Action("create_table", json.dumps({"table": "notes", "schema": {"id": "string", "text": "string"}}).encode()), opts)
w, _ = c.do_put(fl.FlightDescriptor.for_path("notes"), pa.schema([("id", pa.string()), ("text", pa.string())]), opts)
w.write_table(pa.table({"id": ["n1"], "text": ["hello"]})); w.close()
print(c.do_get(fl.Ticket(json.dumps({"table": "notes"})), opts).read_all().to_pylist())
```

| Call | Request | Answer |
|---|---|---|
| `do_get` | `{"sql", "limit"?}` | Read-only SQL over `memory`, `memory_current`, `memory_raw` and your tables (at most 10,000 rows) |
| `do_get` | `{"table", "filter"?, "limit"?}` | A table's rows (`filter` is a SQL condition) |
| `do_put` | descriptor path `[table]` (or `[table, "replace"]`), Arrow batches | Rows added (or the table replaced) |
| `do_action("aql")` | `{"query", "variables"?}` | Same answer as `POST /query` |
| `do_action(...)` | `list_tables`, `create_table`, `update`, `delete`, `compact` | See [`flight.rs`](../crates/adb-server/src/flight.rs) |

Flight is not Flight SQL: tools that connect through a Flight SQL / JDBC driver
(DBeaver, BI tools) can't use it yet.

## CLI

```bash
# read-only SQL against a running node, over Flight
ADB_AUTH_TOKEN=... adb sql "select namespace, count(*) from memory_current group by namespace" --flight grpc://localhost:3001

# one AQL statement straight on a data directory (only while no node runs on it)
adb --data-dir ./adb-data query 'RECALL FROM SEMANTIC NAMESPACE "crm" LIMIT 5'
```

For a running node, send AQL with `POST /query` (or the console's Query tab).

## Health and metrics

| Endpoint | |
|---|---|
| `GET /health` | `{status, version}`; always open (liveness probes) |
| `GET /ready` | Cluster nodes: 200 once the node holds its control-plane lease |
| `GET /stats` | Records per memory type |
| `GET /metrics` | Prometheus text; set `ADB_METRICS_OPEN=1` to let a scraper read it without the token |
| `GET /admin/stats/metrics` | The same metrics as JSON, histograms with p50/p90/p99 |
| `GET /admin/console` | This node: version, uptime, auth, storage, endpoints, cluster map (console builds) |

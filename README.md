# ADB — Agent Database

Memory for AI agents, queried in **AQL** (Agent Query Language). Part of A3T, offered
as a service; this repo also builds the local dev server.

- Architecture: [`.ai/ARCHITECTURE.md`](.ai/ARCHITECTURE.md) · delivery plan:
  [`.ai/DELIVERY_PLAN.md`](.ai/DELIVERY_PLAN.md) · AQL spec and parser: the
  [AQL repo](https://github.com/a3t-ai/AQL)
- One binary: a single node for local development, or a cluster over object storage
  (S3). Data is kept on disk with `--data-dir`; without it ADB uses a temporary
  directory and forgets everything when it stops.

## Install

Prebuilt binaries are not published yet: build from source or with Docker.

**From source.** Needs stable Rust and `protoc` (`apt install protobuf-compiler`).

```bash
git clone https://github.com/a3t-ai/ADB && cd ADB
cargo build --release -p adb-cli --features console   # binary: target/release/adb
```

`--features console` adds the [ADB Console](#adb-console) web UI. Leave it out for a
binary without any UI.

**Docker.**

```bash
docker build -t adb --build-arg ADB_FEATURES=console .
docker run -p 3000:3000 -p 3001:3001 -v adb-data:/data adb \
  adb serve-http --port 3000 --data-dir /data
```

ADB uses the AQL parser from the AQL repo. To develop against a local AQL checkout
next to this one (`../AQL`), copy `.cargo/config.toml.example` to `.cargo/config.toml`.

## Start a node

```bash
export ADB_AUTH_TOKEN=$(openssl rand -hex 24)   # optional; without it ADB is open
adb serve-http --data-dir ./adb-data
```

| Flag | Default | |
|---|---|---|
| `--data-dir <dir>` (or `ADB_DATA_DIR`) | a temporary directory | Where data is kept. Without it, data is lost when ADB stops |
| `--host <ip>` | `0.0.0.0` | Address to listen on (`127.0.0.1` keeps ADB local) |
| `--port <n>` | `3000` | HTTP: AQL, MCP, live updates, admin, the console |
| `--flight-port <n>` | `3001` | Arrow Flight; `0` turns it off |

`adb serve` (no `--http`) instead runs MCP on stdin/stdout for a single local agent,
with no network port: see [MCP](#mcp).

## ADB Console

![ADB Console: overview of a 3-node cluster, with every connection point](docs/images/console-overview.png)

Open **`http://localhost:3000/ui`** in a browser (the node's address and HTTP port).
If the node has a token, paste it into the token field and press **Connect**; the
page keeps it for the browser tab only.

| Tab | What you get |
|---|---|
| Overview | Health, mode, uptime, auth, storage; every connection point of this node; copy-ready snippets for curl, an MCP client, Python (Arrow Flight) and the CLI |
| Query | AQL editor (Ctrl/⌘+Enter runs), `$variables` as JSON, history, result as a table or JSON |
| Settings | The embedding setting (provider, model, where it runs), hot-cache rules, compact now |
| Cluster | Nodes, which node owns and replicates each partition, compute nodes |
| Metrics | Latency percentiles (p50/p90/p99) and counters, with a filter and auto-refresh |

| Query | Cluster |
|---|---|
| ![AQL editor with a variable and the result as a table](docs/images/console-query.png) | ![Nodes and the owner of each of 64 partitions](docs/images/console-cluster.png) |
| **Settings** | **Metrics** |
| ![Embedding setting and hot cache rules](docs/images/console-settings.png) | ![Query latencies p50/p90/p99 and counters](docs/images/console-metrics.png) |

The console is a build feature (`console`), off by default: a binary built without
it serves no `/ui`. The page itself is open, as it holds no data; everything it shows
is read with the token, like any other client. To reach a node on another machine
privately, forward its port: `ssh -L 3000:localhost:3000 <host>`.

## Connecting

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

### HTTP and AQL

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

### Live updates (SSE)

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

### MCP

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

### Arrow Flight

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
| `do_action(...)` | `list_tables`, `create_table`, `update`, `delete`, `compact` | See [`flight.rs`](crates/adb-server/src/flight.rs) |

Flight is not Flight SQL: tools that connect through a Flight SQL / JDBC driver
(DBeaver, BI tools) can't use it yet.

### CLI

```bash
# read-only SQL against a running node, over Flight
ADB_AUTH_TOKEN=... adb sql "select namespace, count(*) from memory_current group by namespace" --flight grpc://localhost:3001

# one AQL statement straight on a data directory (only while no node runs on it)
adb --data-dir ./adb-data query 'RECALL FROM SEMANTIC NAMESPACE "crm" LIMIT 5'
```

For a running node, send AQL with `POST /query` (or the console's Query tab).

### Health and metrics

| Endpoint | |
|---|---|
| `GET /health` | `{status, version}`; always open (liveness probes) |
| `GET /ready` | Cluster nodes: 200 once the node holds its control-plane lease |
| `GET /stats` | Records per memory type |
| `GET /metrics` | Prometheus text; set `ADB_METRICS_OPEN=1` to let a scraper read it without the token |
| `GET /admin/stats/metrics` | The same metrics as JSON, histograms with p50/p90/p99 |
| `GET /admin/console` | This node: version, uptime, auth, storage, endpoints, cluster map (console builds) |

## Configuration

Flags set ports and the data directory; everything else is an environment variable
or a live setting changed over HTTP (or in the console's Settings tab).

### Access

| Variable | |
|---|---|
| `ADB_AUTH_TOKEN` | The token for every endpoint except `/health` and `/ui`. Unset: ADB is open (local use only) |
| `ADB_SCOPED_TOKENS` | Tokens limited to namespaces, JSON: `[{"name": "acme", "token": "…", "write": ["acme-*"], "read": ["shared-*"]}]`. They may only run `/query` statements that name a `NAMESPACE` they cover |
| `ADB_METRICS_OPEN` | `1`: `/metrics` needs no token |

### Storage

| Setting | |
|---|---|
| `--data-dir <dir>` | Where a node keeps its data |
| `ADB_COLD_URI` | Object storage for the log and checkpoints: `s3://bucket/prefix`, or `file:///path` for tests. Required for a cluster |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT` | Credentials and endpoint for `s3://` (any S3-compatible store) |

### Embeddings (similarity search)

ADB turns text into vectors in the background. Pick the provider at any time; the
change is applied at once and the vectors are rebuilt in the background when the
model changes:

```bash
curl -s -X PUT http://localhost:3000/admin/embedding -H "Authorization: Bearer $ADB_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"provider": "openai", "model": "text-embedding-3-small", "dimension": 1536, "api_key_env": "OPENAI_API_KEY"}'
```

| Field | |
|---|---|
| `provider` | `local` (built-in model, the default), `openai`, or `custom` (any endpoint with the OpenAI embeddings API; set `url`) |
| `model`, `dimension` | The model and its vector size. Local default: `bge-small-en-v1.5`, 384 |
| `api_key_env` | The name of the environment variable holding the API key. ADB never stores the key itself |
| `request_dimension` | `true`: ask the endpoint for `dimension` (models that can shorten vectors) |
| `run_on` | `node` (default) or `compute`: hand the work to compute nodes (`adb serve-compute`, joined with `ADB_COMPUTE_JOIN=http://<node>:3000`) |

The local model is read from `/models/bge-small-en-v1.5`, or `ADB_EMBED_MODEL_DIR`;
`tests/fetch-model.sh <dir>` downloads it. `ADB_EMBED=off` turns embedding off.

### Hot cache

`GET`/`PUT /admin/cache` with `{max_bytes, rules: [{memory, scope?, keep}]}`: which
memory stays in RAM. Applied at once.

### Cluster

A cluster is several nodes sharing object storage. Each node runs `adb serve-http`
with:

| Variable | |
|---|---|
| `ADB_NODE_ID` | This node's number (`1`, `2`, …; `from-hostname` takes it from a StatefulSet pod name) |
| `ADB_CONTROL_PEERS` | The control-plane voters: `1=http://adb-0:3002,2=http://adb-1:3002,3=http://adb-2:3002` |
| `ADB_CONTROL_PORT` | This node's control-plane port (default `3002`; between nodes only) |
| `ADB_ADVERTISE`, `ADB_ADVERTISE_FLIGHT` | The HTTP and Flight addresses other nodes reach this one at |
| `ADB_PARTITIONS` | Memory partitions, fixed for the cluster's life (default 64) |
| `ADB_COLD_URI` | Required |

Kubernetes manifests: [`deploy/k8s`](deploy/k8s). Clients connect to any node: each
forwards what it does not own.

## Memory types

| Type | For |
|---|---|
| `WORKING` | The current task (short-lived) |
| `EPISODIC` | Events, conversations, incidents |
| `SEMANTIC` | Facts and concepts |
| `PROCEDURAL` | Steps, fixes, patterns |
| `TOOLS` | Tools an agent can use, with rankings |

## AQL examples

```sql
STORE INTO SEMANTIC (key = "acme-renewal", content = "Acme renews on 1 Nov", account = "acme")
RECALL FROM SEMANTIC WHERE account = "acme" LIMIT 10
RECALL FROM EPISODIC WHERE content CONTAINS "outage" ORDER BY severity DESC LIMIT 5
LINK FROM PROCEDURAL WHERE key = "oom-fix" TO EPISODIC WHERE key = "inc-001" TYPE "applied_to" WEIGHT 0.9
FORGET FROM WORKING WHERE task = "draft"
PIPELINE triage TIMEOUT 80ms RECALL FROM WORKING WHERE task = "t1" | RECALL FROM EPISODIC WHERE pod = {pod}
```

## Tests

```bash
cargo test --workspace                                        # unit + HTTP + MCP tests
node ../AQL/tests/run-tests.mjs --url http://localhost:3000   # AQL conformance (start ADB empty)
node tests/mcp-smoke.mjs --stdio target/release/adb           # MCP session: also --http / --sse <url>
node tests/flowr-integration.mjs --adb target/release/adb --flowr <flowr-ce binary>   # FlowR → ADB
docker compose -f deploy/docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test
```

The FlowR test runs A3T's `a3t-test-resource` flow in a real FlowR-CE against ADB through an
`mcp` resource set up like A3T's `agent-memory-mcp`. Building FlowR-CE needs `libssl-dev`,
`pkg-config` and `libclang-dev`.

## Licence

ADB is licensed under the [Elastic License 2.0](LICENSE) (ELv2). You may use, copy, modify
and distribute it, including in production and in your own products, except that you may not
provide ADB to others as a hosted or managed service, circumvent licence-key functionality, or
remove licensing notices. See [NOTICE](NOTICE) for third-party code: `third_party/lance` is a
modified copy of Lance under the Apache License 2.0.

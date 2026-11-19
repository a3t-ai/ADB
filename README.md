# ADB — Agent Database

Memory for AI agents, queried in **AQL** (Agent Query Language). Part of A3T, offered
as a service; this repo also builds the local dev server.

- Architecture: [`.ai/ARCHITECTURE.md`](.ai/ARCHITECTURE.md) · delivery plan:
  [`.ai/DELIVERY_PLAN.md`](.ai/DELIVERY_PLAN.md) · AQL spec and parser: the
  [AQL repo](https://github.com/a3t-ai/AQL)
- Status: **delivery stage A** — the drop-in server and MCP interface run on the
  in-memory engine (data is not persisted yet; the durable engine is stages C–D).

## Memory types

| Type | For |
|---|---|
| `WORKING` | The current task (short-lived) |
| `EPISODIC` | Events, conversations, incidents |
| `SEMANTIC` | Facts and concepts |
| `PROCEDURAL` | Steps, fixes, patterns |
| `TOOLS` | Tools an agent can use, with rankings |

## Build

Requires stable Rust and `protoc` (`apt install protobuf-compiler`).

```bash
cargo build --release          # binary: target/release/adb
cargo test --workspace
```

ADB uses the AQL parser from the AQL repo. To develop against a local AQL checkout
next to this one (`../AQL`), copy `.cargo/config.toml.example` to `.cargo/config.toml`.

## Run

```bash
adb serve                         # MCP on stdio (Claude Code, Claude Desktop)
adb serve --http --port 3000      # REST + MCP over HTTP / SSE (A3T)
adb query 'RECALL FROM EPISODIC WHERE pod = "payments" LIMIT 5'
```

**Claude Code:** `claude mcp add adb -- /path/to/adb serve`

**A3T:** ADB serves the same API as ClawDB's server, so an A3T `clawdb` resource can
point at ADB unchanged (`url: http://<adb>:3000`, MCP at `/mcp/sse`).

## API

| Endpoint | |
|---|---|
| `GET /health` | `{status, version}` |
| `POST /query` | `{"query": "<AQL>", "variables"?: {…}}` → `{success, data, error, execution_time_ms}`; `data` is the query result (`{success, data: {type, records, count}, metadata}`) |
| `GET /stats` | Record counts per memory type |
| `POST /mcp` | MCP, streamable HTTP |
| `GET /mcp/sse` + `POST /mcp/message?sessionId=…` | MCP over SSE |

**MCP tools:** `aql_query`, `store_memory`, `recall_memory`, `get_stats` (ClawDB's) and
`remember`, `recall`, `forget`, `link` (no AQL needed). **Resources:** a memory guide
(`adb://guide/memory`) and the AQL grammar (`adb://aql/grammar`). **Prompt:**
`memory_smoke_test`.

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

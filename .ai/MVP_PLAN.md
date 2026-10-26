# ADB MVP plan — single instance, through M2

> **Decided 26 Oct 2026 (user):** the MVP is everything through M2 on **one ADB
> instance**; Raft (M3) and the control plane (M4) come after. **Target:** A3T (its
> platform database), the FlowR runtimes and the agents all connect to that one ADB,
> on both layers — MCP (JSON: agents' tools) and the database layer (Arrow Flight:
> `call:adb`, platform tables) — and execute AQL.

## Where things stand (26 Oct)

| Piece | State |
|---|---|
| Memory (AQL, Lance store, links, supersede, forget) | Built (stage D), on vc-dev |
| Subscriptions (topics, scope feeds, SSE + MCP) | Built (stage E), on vc-dev |
| Agent memory in `call:a3t:agent`, `adb_topic` trigger | Built (stage F), on vc-dev |
| Platform tables on ADB, Flight, Settings switch | Built (stage G), rolling out |
| **FlowR runtimes** (pods the a3t-controller deploys) | **Built (W2), rolling out** — see W2 below. Was: not connected. The runtime pod gets `A3T_FLOW` / `A3T_SESSION_MODE`, but nothing in FlowR reads them: a runtime starts empty (no flow, no resources, no ADB). The controller image (`a3t-controller:dev`) was hand-loaded on the node and lost when the node ran out of disk; there is no build target for it. |
| Embeddings / vector recall | Missing (L1): `RECALL … LIKE` finds nothing; agents get "newest 5 facts" |

## Workstreams, in order

| # | Workstream | What | Done when |
|---|---|---|---|
| W0 | **Finish stage G on vc-dev** | Roll ADB (Flight port on the Service) then FlowR; `adb-selftest` on LanceDB; Settings → Switch to ADB; `adb-selftest` again | 15/15 both times; A3T works on ADB |
| W1 | **Safety net** | Push branches (user: no GitHub credentials in the pod); one `tests/check-all.sh` that runs every gate (Rust, AQL conformance, FlowR integration, crash, subscription crash, topic trigger, platform differential), run before each commit / image | One command, all green |
| W2 | **Runtimes on ADB** | (a) `deploy.sh` target for the controller (`flowr-controller`), rebuild + push, runtimes come back. (b) A runtime boots from ADB: it reads its canvas / flow and the resources it needs from A3T's platform tables on ADB, gets the ADB resources (`agent-memory` for `call:adb` over Flight, `agent-memory-mcp` for MCP), and runs the flow. (c) FlowR's own state for runtimes (executions, sessions, triggers) on ADB, so a runtime is stateless and restartable — the "second table family" for FlowR state (ARCHITECTURE §15, G-L6), scoped to what runtimes need. | A canvas deployed from A3T starts a runtime that runs its agent with memory in ADB; killing the runtime pod loses nothing |
| W3 | **M1 hardening** | Key uniqueness on STORE (L2); TTL expiry committed by a timer, delivered to subscribers (L3, E-L10); named scopes on STORE (E-L6); concurrent writes sharing one fsync (P8); Flight auth token (G-L1) | Limitations closed, tests for each |
| W4 | **M2: recall** | Embeddings on STORE (L1) — decision: who embeds (ADB calls an embedding provider through a configured resource, or the caller passes the vector); Lance vector index (IVF above ~10k rows, brute force below); full-text index; hybrid ranking (vector + text + recency + link weight, RRF) with an evaluation set; agent memory (F-L1) switches from "newest" to relevance | `RECALL … LIKE` / text recall ranked; agents get relevant facts |
| W5 | **M2: lifecycle** | Compaction (fold generations, platform datasets — G-L4); physical forget (compaction + `cleanup_old_versions`, L10); retention | FORGET physically complete; tables stay compact |
| W6 | **M2: continuous queries** | `WATCH` v1 (static watch sets, filter / current-facts / count classes, timer wheel) | Diff stream equals re-running the query |
| W7 | **M2: query engine** | Queries as DataFusion plans over Arrow; remove `adb-backends` (S2, P1–P4) | No in-memory views per read |
| W8 | **MVP gate** | Everything on vc-dev single node: A3T platform DB on ADB, runtimes + agents on ADB over MCP and Flight, AQL conformance, crash tests, self-test flow | The user's A3T pass |

W0–W2 make the MVP's *connectivity* real; W3 hardens it; W4–W7 are M2. W4 first
within M2: it is the gap agents notice.

## Decisions (26 Oct, user: as recommended)

- **W2b — runtimes run through ADB.** A runtime pod boots with ADB's URL and its
  canvas / deployment id, reads its flow and resources from ADB, gets its work from a
  per-deployment ADB topic that the console publishes to (the `adb_topic` trigger), and
  keeps memory and results in ADB. Runtimes are stateless and share one ADB.
- **W2c — FlowR's registry (workflows, resources) and agent sessions move to ADB**
  (platform tables); executions and the queue stay local to each FlowR.
- **W4 — ADB embeds text itself with a small local CPU model** (bge-small class, 384
  dims), no external calls; an OpenAI-compatible endpoint can be configured later.

## W2 as built (26 Oct)

- **Shared registry (W2c).** `FLOWR_REGISTRY_ADB=<ADB http url>` (optional
  `FLOWR_REGISTRY_ADB_FLIGHT`) puts a FlowR's workflows, resource types, resources and
  `agent:*` state (agent configs, chat sessions, session index) on ADB platform tables
  `flowr_workflows`, `flowr_resource_types`, `flowr_resources`, `flowr_state` (same
  columns as the local LanceDB). A console seeds an empty shared registry from its
  local one on first boot; a runtime never seeds. Code:
  `flowr-ce/src/shared_registry.rs`, `init_storage`. EE only (`adb-platform`).
- **Runtime boot (W2b).** The controller gives each isolated canvas's Sandbox pod
  `A3T_CANVAS=<ns>/<name>`, `A3T_FLOW`, and its own `A3T_RUNTIME_ENV_<NAME>` variables
  as `<NAME>` (so `A3T_RUNTIME_ENV_FLOWR_REGISTRY_ADB=…` reaches every runtime);
  `A3T_RUNTIME_IMAGE` is the default runtime image. At boot a runtime creates (or
  restores) trigger `runtime-<ns>-<name>`: `adb_topic` on `canvas.<ns>.<name>` →
  `A3T_FLOW`, through the ADB resource `A3T_ADB_RESOURCE` (default
  `a3t/agent-memory`). Its `_triggers` key lives in ADB as
  `runtime:<ns>/<name>:triggers`, so a new pod resumes the acked subscription.
- **Requests and replies.** Work is a message on `canvas.<ns>.<name>`; with
  `_reply_topic` set, the runtime publishes `{request_id, status, output_json, error,
  instance_id, canvas}` there when the run ends. `a3t/canvas-dispatch` does a round
  trip from the console; `a3t/runtime-echo` is a flow to deploy for it.
- **Gate:** `tests/runtime-e2e.mjs` (in `check-all.sh`): shared registry, topic work,
  replies, memory, SIGKILL + new empty pod resumes, A3T's dispatch flow — 12 checks.
- **Limits:** W2-L1…L9 in `limitations.md`.

### vc-dev configuration

| Where | Setting |
|---|---|
| `deploy/flowr` (console) | `FLOWR_REGISTRY_ADB=http://adb.a3t.svc.cluster.local:3000` |
| `deploy/a3t-controller` | image `ghcr.io/srirammails/a3t-controller:<date>` (`deploy.sh release controller`); `A3T_RUNTIME_IMAGE=ghcr.io/srirammails/flowr:<ee tag>`; `A3T_RUNTIME_ENV_FLOWR_REGISTRY_ADB`, `A3T_RUNTIME_ENV_FLOWR_SECURITY_POLICY_HOOK=cedar`, `A3T_RUNTIME_ENV_FLOWR_SECURITY_AUTHN=oidc`, `A3T_RUNTIME_ENV_FLOWR_SECURITY_OIDC_ISSUER` |

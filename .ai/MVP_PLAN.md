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
| **FlowR runtimes** (pods the a3t-controller deploys) | **Not connected.** The runtime pod gets `A3T_FLOW` / `A3T_SESSION_MODE`, but nothing in FlowR reads them: a runtime starts empty (no flow, no resources, no ADB). The controller image (`a3t-controller:dev`) was hand-loaded on the node and lost when the node ran out of disk; there is no build target for it. |
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

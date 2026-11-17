# Stage F — FlowR wiring: decisions

> **Status:** agreed 25 Oct 2026 (user: as recommended) · **Scope:** DELIVERY_PLAN stage F
> and the M1 FlowR row: `call:a3t:agent` stores and recalls memory through ADB;
> FlowR cross-instance notifications on topics; local dev packaging. FlowR branch
> `adb-integration`.

## What already exists (from stages A–E)

- `call:adb` (alias `call:clawdb`) — FlowR's AQL task handler on `swf_runtime::TaskHandler`,
  against an `adb` resource. Done in stage A.
- MCP: agents discover and call ADB's tools (`remember`, `recall`, `subscribe`, …)
  through the `agent-memory-mcp` resource; per-tool dispatch to the right server.
- `ClawDbMemory` (ZeroClaw `Memory` trait over `/query`) — written, unused (only
  behind the `zeroclaw-agent` feature, which A3T doesn't build). No change.

**The gap:** `call:a3t:agent` (the Rust agent engine A3T runs) has memory phases —
`recall_memory`, `procedural_check`, `store_memory`, configured by `memory_resource`
and per-tier `memory_config.{working,episodic,semantic,procedural}.{read,write}` — but
they are stubs ("ClawDB integration pending"). An agent only uses ADB when the LLM
decides to call a tool.

## Decisions

| # | Question | Recommendation |
|---|---|---|
| F1 | **What the memory phases do** | With `memory_resource` set (an `adb` / `clawdb` resource), each turn: **recall** WORKING and EPISODIC for this `session_id` (newest 20 each), SEMANTIC facts in the agent's `memory_namespace` (newest 5 — relevance ranking needs M2 hybrid recall), and PROCEDURAL by `PATTERN` match on the message (best 1); **store** the user turn in WORKING (TTL = the session TTL) and the reply in EPISODIC (with tool calls and iterations). AQL over `POST /query`, the same client as `call:adb`. A memory error never fails the turn (logged, turn continues). |
| F2 | **Use of recalled memory** — the YAML engine recalled but never used it | **Add it to the system prompt** as a bounded "What you remember" section (≤ 3,000 characters, newest first, facts before episodes), so recall actually changes the answer. WORKING / EPISODIC of the same session are already in the session history, so only SEMANTIC (and PROCEDURAL, below) are injected. |
| F3 | **Defaults per tier** | WORKING read/write on, EPISODIC read/write on, SEMANTIC **read on, write off** (facts are written deliberately, by the `remember` tool), PROCEDURAL read off. Matches the YAML engine except SEMANTIC read (off there, but nothing used it). |
| F4 | **Procedural short-circuit** (a matching procedure answers without the LLM) | Keep as designed, **off by default** (`memory_config.procedural.read = true` turns it on). When on and not short-circuiting below the match threshold (0.5), the best procedure goes into the prompt as a suggested approach instead. |
| F5 | **Topic trigger** (cross-instance notifications) | A new FlowR trigger type **`adb_topic`** (`resource`, `topic`, optional `filter`): on enable, FlowR subscribes (subscription id kept in the trigger, so a restart resumes), long-polls `next_events`, starts the attached flow / agent per message, and **acks after the flow is started** (at-least-once). Each FlowR instance with the trigger holds **its own subscription**, so every instance sees every message (broadcast — spec §2 non-goal: no competing consumers). |
| F6 | **Local dev packaging** | Add an `adb` service (`ghcr.io/srirammails/adb`, `--data-dir` volume) to `deployment/docker-compose.yml` and a seeded `adb` memory resource pointing at it; CE keeps embedded LanceDB for its own data (stage G is hosted only). |
| F7 | **Reflection** (F-L3, 30 Oct) — turns into facts | `memory_config.semantic.reflect = true` (off by default, it costs an LLM call per turn): after the turn, in the background, the agent's LLM gets the exchange (and the 4 messages before it, and the recalled entity keys) and answers JSON facts; FlowR stores them as `STORE INTO SEMANTIC FACT` with `source = "reflection"`, `owner`, `session_id`. The user is the entity `user:<owner>`; recall leaves out other owners' facts (a namespace is shared by an agent's users). Procedures (F4) are matched by ADB's `PATTERN $m` since F-L2. |

## Exit (stage F gate)

- FlowR test: a `call:a3t:agent` flow with `memory_resource` stores both turns in
  ADB, and a second session recalls a fact remembered in the first (stub LLM, no
  tool call), including after an ADB restart.
- Topic trigger test: two FlowR instances with the trigger each run the flow once per
  published message, in the same order, across a FlowR restart.
- **A3T:** an agent with `memory_resource` set answers from a fact told in an earlier
  chat session.

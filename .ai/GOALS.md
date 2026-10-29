# ADB — End goals and priority order

> **Date:** 29 Oct 2026 · @Sriram · set by the user. This file records **what ADB is
> for** and **the order of work after the MVP**. It does not change
> `ARCHITECTURE.md` yet: the direction below is to be designed there when its phase
> starts. Open questions are in `limitations.md` (GQ-*).

## End goals

1. **Agent memory, close to the agent.**
   - **Working memory** for agents (WORKING: TTL, sub-ms reads).
   - **A semantic cache local to the agent**, reading from LanceDB: an agent's scoped
     memory (SEMANTIC, EPISODIC, WORKING, …) is held next to the FlowR runtime that
     runs it.
   - **A distributed graph**, stitched together from each agent's local SEMANTIC,
     WORKING and EPISODIC memory.
2. **A central knowledge graph for agentLM / harnessLM** — a variation of KBLaM
   (knowledge triples encoded as key / value vectors that the model reads through
   attention, instead of a retrieval step).

Every phase below is judged against these two goals. Work that doesn't move them
(e.g. more cache-miss tuning) waits unless a real workload needs it.

## Direction (29 Oct, user) — not yet in ARCHITECTURE.md

- **"Local to the agent" means placement in a clustered ADB**, not ADB code running
  inside the agent's process. In the real world, **ADB can be a process in the same
  container as the FlowR runtime** (the *edge* ADB) and keep that runtime's scoped
  memory close to it. A core ADB cluster (leader-based, replicated) keeps the data
  durable behind it.
- This reverses, for the edge role only, ARCHITECTURE §13's 7 Oct decision "no
  embedded production profile". To be designed in ARCHITECTURE §8 / §13 / §15 when
  phase 3 starts.
- Working sketch (from the 29 Oct discussion; to be confirmed in design):
  - same binary, two roles: **core** (Raft voters, owners, Lance on object storage)
    and **edge** (one per runtime container; not a Raft member);
  - the edge warms its scopes from the Lance tables, then follows each scope's
    subscription feed from that offset; the W10 hot-cache code fills from the feed;
  - reads of the agent's scopes are local; writes are forwarded to the core owner and
    acknowledged after commit; read-your-writes by commit offset;
  - shared scopes (team / central) are followed by every edge that needs them;
  - the control plane (M4) decides which scopes each edge holds and moves them with
    the agent.

## Where things stand against the goals (29 Oct)

| Goal | Built | Missing |
|---|---|---|
| Working memory | WORKING with TTL, durable, ~1 ms from the cache | — |
| Semantic cache near the agent | Hot cache on the ADB node, filled from the log (W10 / W10b); hybrid recall (W4) | Edge ADB in the runtime container; SEMANTIC not cached by default (W10-L9) |
| Distributed graph | Typed, weighted, versioned links across memory types; one-hop reads | Multi-hop (GR-1), inbound (GR-2), link conditions (GR-3), cached links (W10-L2), links in agent recall (GR-6); shared entities; cross-partition links and traversal |
| Central KG / KBLaM | Nothing in ADB; FlowR's `call:rotate` (RotatE embeddings, embedded `a3t-kg.lance`, G-L7) | Triple model, consolidation from agent memories with provenance, KBLaM key / value vectors, versioned feed to the model, invalidation on supersede / forget |

## Priority order

| # | Phase | What | Goal | Needs servers |
|---|---|---|---|---|
| 0 | **MVP gate** (W8) | User's A3T pass on vc-dev; roll W10b; SEMANTIC cache default (W10-L9) | both (foundation) | No |
| 1 | **Graph core** | Multi-hop `FOLLOW LINKS … DEPTH n` (GR-1, L6 / GR-7), inbound links (GR-2), weight conditions and order (GR-3), edge buckets in the cache (W10-L2), link weight and linked facts in agent recall (GR-6); **shared entities**: stable entity ids and facts as entity–relation–value, the model both goals build on | 1, 2 | No |
| 2 | **Edge ADB** | `adb` edge role in the FlowR runtime container: warm from Lance, follow scope feeds, local cache and reads, writes forwarded to the core; runtime pods start it next to FlowR | 1 | No (single core node) |
| 3 | **Stitching** | Cross-partition links (target by global id), cross-partition traversal and recall (RRF), consolidation job (`REFLECT`-based) merging agents' facts and entities into team / central scopes with entity resolution | 1 | No (one node owns every partition; designed for many) |
| 4 | **Central KG + KBLaM** | Central KG scope fed by phase 3, with provenance and validity; `call:rotate` onto ADB (G-L7); KBLaM key / value vectors per triple, versioned by encoder / adapter, streamed over Arrow Flight, invalidated on supersede / forget | 2 | No (storage side); model side per GQ-3 / GQ-4 |
| 5 | **M3 — Replication** | Core Raft replica sets (DELIVERY_PLAN M3) | 1 (durability behind edges) | Yes, 3 servers |
| 6 | **M4 — Control plane** | DELIVERY_PLAN M4, plus scope placement onto edges and moving scopes with agents | 1 | Yes |

Deferred unless a workload needs it: cache-miss tuning (W7-L1, W7-L5, W7-L6) and the
remaining cache work (W10-L4, W10-L10).

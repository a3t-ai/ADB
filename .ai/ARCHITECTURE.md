# ADB (Agent Database) — Architecture

> **Version:** 1.4 — subscriptions and continuous queries ([`SUBSCRIPTIONS_CQ_SPEC.md`](SUBSCRIPTIONS_CQ_SPEC.md), §11.7) on top of v1.3 (spike results to 7 Oct and the architecture review). Changes listed in §20.
> **Date:** 9 Oct 2026 · @Sriram
> **Language:** AQL v0.5 (spec and reference implementation in the AQL repo)
> **Supersedes:** `archive/ARCHITECTURE_v0.1_in_memory.md` (in-memory engine),
> `archive/adb_lance_persistence.md` v0.2 (cache + write-behind draft), the 5 Oct cluster
> draft, and the sidecar stack in AQL spec §23.
> **Relates to:** `CLAWDB_REQUIREMENTS.md` — this document is its target (3), "Full
> ADB"; targets (1) ClawDB and (2) the WASM playground map onto §13's profiles.

---

## 1. Overview

ADB is an agent-memory database and part of the **A3T platform**. It is offered
**as a hosted service only (SaaS, on Hetzner)**, plus a **local dev build** (single
node) for development. Agents talk AQL (`STORE`, `RECALL`, `LINK`, `FORGET`,
`REFLECT`, …); storage is an internal detail.

A cluster is a fixed set of **4,096 global partitions**. Each partition is a state
machine with **one writer**, fed by a **replicated log**, materialized into **one
Lance table on object storage**, and able to **hibernate** to files plus a log
offset. Lance's columnar base serves vectors, full-text, scalar filters and
analytics; small writes are absorbed by an in-memory layer in front of it.

Commits have two modes behind one log trait: **Raft** (about 1 ms, replicated on
local NVMe, for the low-latency and enterprise tier) and **object-store commit**
(10–100 ms, stateless nodes, a candidate for a cheap shared tier).

The same engine runs as the cluster (production), as a single-node local dev build
(ClawDB), and optionally as the WASM playground for demos (§13).

## 2. How we got here: what carries over

| Earlier design | What it got right | What changes in v1.0 |
|---|---|---|
| **v0.1 in-memory engine** (`adb-backends`: DashMap, petgraph, usearch, DataFusion; DashMap link store) | Latency tiers per memory type; the AQL query engine (pest parser, planner/router, executor, MCP, embedder trait) | **No persistence**: everything, including links, is lost on restart. The query engine carries over unchanged. The in-memory backends become the **hot tier and memtable** (§8.2 option B) instead of the store. |
| **v0.2 Lance persistence draft** (cache per memory type, read/write strategies, write-behind, session loading) | Lance as the store; caches in front; embedded vs serverless vs hybrid deployments | **Write-behind acknowledges a write before it is durable**, so a crash loses acknowledged writes. v1.0 is **log-first**: every write is durable in the log before the ack, and materialization into Lance happens behind it, safely. Read strategies become consistency modes (§10); session loading becomes wake + index prewarm (§14.1); agent config moves to the control plane. |
| **ClawDB** (`clawdb` crate: single-node, direct LanceDB, one table per memory type) | Working single-node persistence; 116/116 conformance | Links are kept in an in-memory `Vec<Link>` and lost on restart; tables split per memory type, so one transaction can't apply atomically. v1.0 uses one table per partition and persisted edges. |
| **AQL spec §23 sidecar** (ADB process next to FlowR in each agent container) | Zero network hops for the agent loop | Survives as the **embedded profile** (§13). |

## 3. Goals and non-goals

**Goals**
- Per-agent memory economics: storage is per tenant; compute is pooled and scales
  with *active partitions*.
- Correct multi-record transactions (entities, facts, edges, episodes, vectors)
  inside a partition.
- Automatic placement: developers declare scopes and affinity keys, never shards.
- Permissively licensed dependencies (Apache-2.0 / MIT crates) so ADB can be run as
  a hosted service without upstream licence limits.
- Verifiable forget, temporal facts and provenance designed in from day one.
- Keep v0.1's latency tiers for the hot path: `WORKING` < 1 ms, `TOOLS` < 2 ms reads.

**Non-goals**
- General-purpose distributed SQL or a public query language beyond AQL.
- Cross-partition ACID by default (sagas/outbox first; 2PC only if a real use case
  appears).
- Postgres-scale relational workloads. The hosted control plane uses its own
  Postgres. FlowR's control-plane data is out of scope for v1 (§15).

## 4. Design principles

1. **The log is the source of truth.** Every table and index is a materialized view
   that can be rebuilt from it.
2. **One sequencer per partition; many clients.** Ten billing agents are ten
   clients of one sequencer, not ten writers.
3. **The partition is the unit** of transaction, placement, recovery and
   hibernation.
4. **The partition count is fixed** and never multiplied by tenants or scopes.
5. **Durable before acknowledged.** No write is acknowledged before it is in the log
   (replaces v0.2's write-behind).
6. **Storage-level fencing:** a stale writer must be unable to damage the table even
   if every other safeguard fails.

## 5. Mapping AQL onto the engine

AQL's five memory types and `LINK` map onto one wide table. `memory` keeps AQL's
`FROM`/`INTO` target; `kind` is the engine's record shape.

| AQL | `memory` | `kind` | Persistence | Served from |
|---|---|---|---|---|
| `WORKING` | working | `entity` | **Log + in-memory map**, TTL; reaches Lance only in a snapshot. Scope policy may mark it `volatile` (not logged; v0.2's `LocalOnly`) | Sequencer memory (< 1 ms) |
| `TOOLS` | tools | `tool` | Log → Lance; hot set cached in memory | Memory cache (< 2 ms), then table |
| `PROCEDURAL` | procedural | `entity` / `fact` | Log → Lance | Table (point / scalar) |
| `SEMANTIC` | semantic | `fact` / `entity` (+ embedding) | Log → Lance | Vector + FTS + scalar |
| `EPISODIC` | episodic | `episode` (+ embedding) | Log → Lance, append-only | Time filter + vector + FTS |
| `LINK` | (source's) | `edge` | Log → Lance | BTree on `subject_id` / `object_id` |
| — | — | `meta` | `_meta` control row | — |

> **Fix carried in.** Both existing implementations lose the ontology on restart:
> `adb-backends`' `LinkStore` is a DashMap, and `clawdb` keeps links in a
> `Vec<Link>`. Links ("learning is links") become persisted `edge` rows, fixed in the
> single-node milestone.

## 6. Data placement

Every write maps to one partition:

```
partition = routing_override(tenant, key)                 -- control-plane exceptions
          ?? hash(tenant_id, affinity_key or scope_id) mod 4096
```

| Concept | What it is | Defined by | Cardinality |
|---|---|---|---|
| Tenant | A hosted customer | Control plane | Thousands+ |
| Scope | Memory boundary: `agent:{id}`, `team:{name}`, `account:{id}` | Developer, at agent creation | Millions |
| Affinity key | Schema field that decides placement (e.g. `account_id`) | Developer, in AQL schema | Per record type |
| Partition | Unit of transaction, placement, recovery, hibernation | ADB (fixed) | 4,096 |
| Replica set | 3 nodes replicating a group of partitions through **one Raft group** | Control plane | Tens to hundreds |
| Partition owner | The node in the replica set that sequences and applies a partition | Control plane (lease + log claim) | One per partition |

- The tenant ID is always part of the hash input, the key prefix and the encryption
  key.
- Data that must change atomically shares an affinity key, so it lands in one
  partition.
- An agent's private scope (`agent:{id}`) is created automatically; shared scopes
  are declared. (v0.2's "shared semantic/procedural across agents" is a shared
  scope.)
- **Hot-scope pinning** is an entry in the control plane's routing-override table
  (versioned with the routing map that SDKs cache), not a change to the hash.
- **Cross-partition reads** (an agent recalling its private scope plus a team scope
  on another partition) are fanned out by the SDK/router and merged (reciprocal-rank
  fusion for hybrid recall). Writes stay single-partition.

### 6.1 Scaling and rebalancing (one Raft group per replica set)

- Partitions are spread across the three owners of their replica set. **Ownership
  moves by `claim(partition, epoch+1)`** and copies no data; a **leader change moves no
  partitions** (§8).
- **Capacity grows by adding replica sets**, not by adding a fourth node to a set.
  Moving a partition to a new set is the cross-set handoff: claim, open, replay (§12).
- A set always runs three voters (§11.6). Dropping to two is **degraded mode**, not a
  scaling step.
- **Every partition has an owner while its set is live.** Only hibernation (§14.1)
  creates an unowned partition.

### 6.2 Hot scopes: one scope, one sequencer

- A scope maps to exactly one partition, and so to one sequencer, which never uses
  more than one core (§8). **A single scope's write ceiling is one sequencer's
  throughput.** Measured so far: a MemWAL put costs 0.13–0.16 ms with only the `id`
  index maintained (G1/S6) — roughly 6k puts/s of apply capacity before sequencer
  validation — and each put carries a whole group-commit batch, so a scope's
  transaction rate can be higher than its put rate. To confirm with `id` + `subject_id`
  maintained together and the full sequencer in the path. (The 200 puts/s in the
  spikes was the offered load, not a ceiling.)
- **Storage never limits the number of scopes.** 100k scopes is ~24 per partition;
  scopes are rows filtered by `scope`.
- Defences, cheapest first: key scopes finely (affinity keys); move the hot partition
  to a quieter core or set; **split the scope** (`scope#0..N`), giving up its single
  total order; serve hot reads from cache.
- **The limit, stated:** a scope cannot have both unlimited write scale and a single
  total order.

## 7. Table layout

One Lance table per partition, `s3://{bucket}/p{partition}/memory.lance`. One table
per partition keeps commits, recovery and hibernation independent, and lets a whole
committed batch apply as one atomic MemWAL `put` (§9.3). (ClawDB's
table-per-memory-type layout can't do that.)

| Column | Type | Used by |
|---|---|---|
| `id` | ULID string, unenforced primary key | All |
| `memory` | enum: working, tools, procedural, semantic, episodic | AQL `FROM`/`INTO` |
| `kind` | enum: entity, fact, edge, episode, tool, meta; `event` (topic message), `sub` (subscription + cursor), `cq` (continuous-query registration) — §11.7 | All |
| `tenant_id`, `scope` | string | Isolation, filters, forget |
| `subject_id`, `predicate`, `object_id` | string | Facts and edges |
| `link_type`, `weight` | string, float | Edges (`LINK TYPE … WEIGHT …`) |
| `value` | JSON / struct (non-searched fields encrypted, §14.3) | Entities, facts, procedures |
| `text` | string | Episodes, facts (full-text) |
| `embedding` | fixed-size float vector | Episodes, facts, entities |
| `valid_from`, `valid_to`, `superseded_by` | timestamp, ULID | Temporal model |
| `ttl_at` | timestamp, nullable | Expiry |
| `provenance` | struct (agent, source, txn_id) | Audit, recall ranking |
| `log_offset`, `epoch` | u64 | Idempotent replay, fencing |

- **Indexes:** BTree on `id`, `subject_id`, `object_id`, `valid_from`; bitmap on
  `memory` and `kind`; FTS on `text`; vector on `embedding` as **IVF_RQ, queried
  with `refine`** (S8: recall@10 0.97–1.00; build 40 s at 500k × 768, 65 s at
  500k × 1536). Embeddings are f32 in the table, f16 in the log (S9).
- **Small partitions:** below a row threshold (**~10k vector rows**, S8: brute force
  23 ms at 10k × 768, 146 ms at 50k) recall is a brute-force scan; ANN indexes are
  built only above it, so 4,096 tables don't each carry a near-empty index.
- **Control row:** `id = "_meta"`, `kind = meta`, carrying `applied_offset` and
  `epoch`, rewritten in every applied batch.

## 8. Cluster topology

```
                ┌────────────── Control plane (+ Postgres) ──────────────┐
                │ partition → replica set · owners · leases · overrides  │
                └───────────────▲───────────────────────▲────────────────┘
                     map / redirect               place / lease / wake
   SDK ──txn──▶ ┌──────────────── Replica set (3 nodes, 1 Raft group) ─────────┐
                │ Node A (Raft leader)    Node B             Node C           │
                │  owns p1,p4,…           owns p2,p5,…       owns p3,p6,…     │
                │  sequencers per core    sequencers         sequencers       │
                │        ▲  batches forwarded to the leader ──┴──────┘        │
                │  openraft: append + replicate; WAL thread per disk (each)   │
                │  appliers on each owner ─▶ Lance tables on object storage   │
                └─────────────────────────────────────────────────────────────┘
```

**One Raft group per replica set, not per partition.** Three nodes host many
partitions under one group, avoiding 4,096 groups' worth of heartbeats and
elections; heartbeats are batched per node pair.

**Owner and Raft leader are separate roles.**
- The control plane spreads **partition ownership** across the three nodes (lease
  plus a log claim, §11.4), so sequencing and applying are spread too.
- Every batch is proposed through the group's **single leader**. An owner that is not
  the leader forwards its batch (one in-rack hop); the leader appends and replicates;
  on commit the owner acks its clients and applies to Lance.
- A **leader change does not move partitions**: owners keep them; only the
  forwarding target changes.
- The leader carries all of the set's replication traffic and log appends. Group
  commit across partitions amortizes that. If a set's leader saturates, splitting
  the set into several groups (multi-Raft) is the scaling lever, gated by the
  benchmark in §19; it is not part of v1.

| Component | Runs where | Responsibility |
|---|---|---|
| SDK | Agent process | Hash to partition, cache routing map (with overrides), idempotency keys, fan-out reads |
| Control plane | Separate service + Postgres | Partition → replica set map, owners, leases, overrides, hibernation, tenant tiers, bucket assignment, agent config. Never on the write path |
| Partition sequencer | The partition's owner, on that partition's core | Validation, pending overlay, group commit, `WORKING` / `TOOLS` hot tier |
| openraft | One group per replica set | Replicate batches, elect leaders, membership |
| WAL writer | One OS thread per disk, outside tokio | `pwrite` + `fdatasync` of the Raft log |
| Applier | The partition's owner | Apply committed batches to Lance (§9.3) |
| Archiver | Per node | Ship sealed log segments to object storage |
| Maintenance | Per partition, background | Flush, compaction, index build, old-version cleanup |
| Matcher | The partition's owner, sequencer's core, after each apply `put` | Test effects against subscriptions and continuous queries; update CQ results; temporal timers (§11.7) |
| Gateway | Always-on stateless tier | Agent connections (gRPC stream, MCP, WebSocket), delivery buffers, acks, catch-up, routing to owners (§11.7) |
| Watcher tier (v2) | Always-on tier | Cross-partition continuous queries (§11.7) |

**Cores:** partitions are assigned to cores (many per core) and each core runs a
single-threaded executor for its sequencers, Dragonfly-style. A partition is never
touched by two cores.

**The sequencer is state, not a thread.** A sequencer is a partition's single-writer
state — epoch, pending overlay, group-commit batch, `WORKING` / `TOOLS` hot tier, the
MemWAL shard writer — run as a task on one core's executor. A core runs many of them;
a sequencer never has a thread of its own. Nothing "starts a sequencer": what
activates is the **partition** — it gets an owner, an epoch and loaded state (§14.1),
and the sequencer is the code that runs over that state. A partition therefore never
uses more than one core (§6.2).

## 9. Write path

A transaction commits when **one log record holding all its effects** is fsynced on a
majority of the replica set (locally in the single-node profiles; by a conditional
PUT in object-store commit mode). There is one commit
point, so there is no 2PC across engines.

1. The SDK computes the partition and sends the transaction to the partition owner,
   with an idempotency key and, for read-modify-write, its read-set versions.
2. The sequencer validates against the table plus the **pending overlay** (appended
   but not yet applied): existence, uniqueness, references, OCC versions.
3. Accepted transactions join the current group-commit batch (2–5 ms window).
4. The batch is proposed to the replica set's Raft group as one entry (forwarded to
   the leader if the owner is not the leader); each record carries
   `epoch`, `txn_id` and typed effects, e.g.
   `[PutEntity, PutFact×3, PutEdge×2, SupersedeFact]`.
5. On quorum commit the leader acks each client with its **commit offset**.
6. The applier materializes committed batches (§9.3).

### 9.1 Rules

- Constraints (uniqueness, references) are ADB code, enforced only in the sequencer;
  Lance's primary key is unenforced.
- Index maintenance is the storage layer's job; ADB never writes index rows.
- Idempotency: `txn_id` dedupe in the sequencer; replay is safe because apply
  upserts by `id` and is offset-guarded (§9.3).
- Interactive transactions are replaced by OCC: read with versions, submit writes
  plus read-set, reject on mismatch.
- Append-only events (observations, call logs) skip read validation.
- Large imports are chunked with a commit marker record, or bypass the memtable as a
  bulk add (v0.2's `WriteAround`).
- Embeddings travel in the log record (stored as f16 in the log to halve its size),
  so rebuilding a table never requires re-embedding.
- `WORKING` writes are logged (so they survive failover) unless the scope marks
  working memory `volatile`; either way they live in the sequencer's in-memory map
  with TTL.

### 9.2 The memtable: two candidate implementations (decided by the spike)

Small writes need an in-memory layer in front of Lance's columnar base.

| | **A. Lance MemWAL (LSM write path)** | **B. ADB-owned memtable** |
|---|---|---|
| What | Lance buffers writes in memtables and flushes L0 generations | The applier keeps applied-but-unflushed rows in memory and writes Lance in larger `merge_insert` batches (seconds) |
| Built from | Lance | **`adb-backends`' in-memory engines**: DashMap rows, usearch for fresh vectors, petgraph for fresh edges, DataFusion/Arrow for fresh episodes |
| WAL | Raft log **and** MemWAL (double write, unless MemWAL durability can be relaxed) | Raft log only |
| Fresh-row reads | Lance merges memtable, L0 and base, using `maintained_indexes` | ADB merges memtable results (small) with Lance results |
| Maturity | **Shipped in Lance 12.0.0** (spike B2): `initialize_mem_wal` / `mem_wal_writer` → `ShardWriter` with per-shard epoch fencing, relaxable durability (`durable_write(false)`), maintained BTree / HNSW (f32) / FTS indexes, `LsmScanner` over memtable + flushed generations + base. Compaction into the base is caller-driven. New in 2026, so expect churn | Long-stable Lance APIs plus code ADB already has |

**Decision (6 Oct 2026, Gate 1): option A — Lance MemWAL**, pinned at Lance
`=12.0.0` ([`spikes/G1_memtable_A_vs_B.md`](spikes/G1_memtable_A_vs_B.md)):
- One MemWAL shard per partition table; shard writer epoch = partition epoch.
- `durable_write(false)`: the Raft log is the WAL, so MemWAL's own WAL flush is lazy
  and losing it only lengthens replay. **No periodic MemWAL WAL flush**
  (`max_wal_flush_interval = None`, R6): MemWAL writes a WAL entry only when ADB seals,
  so a takeover replays nothing from object storage (0.4–0.55 s on fsn1, versus
  ~75 ms per WAL entry replayed); the unsealed tail is replayed from the local Raft
  log. ADB seals on its row/byte limit **or every 30 s** if the memtable is non-empty,
  which bounds both the tail replay and the Raft log the partition pins.
- **ADB garbage-collects MemWAL** (S11): Lance never deletes WAL files or flushed
  generations. After generations are folded into the base table, `adb-store` deletes
  the compacted generation folders and the WAL files below the shard's
  `replay_after_wal_entry_position`.
- `maintained_indexes` = `id` and `subject_id` BTrees only. Maintaining HNSW + FTS in
  the memtable cost 13 ms per put (80 puts/s); with `id` alone a put costs 0.13–0.16 ms
  and vector/FTS over fresh rows fall back to brute force on the memtable. **Still to
  measure: `id` + `subject_id` maintained together** (the spike measured `id` alone).
- **Memtable cap for recall:** brute-force vector search over unindexed memtable rows
  must stay inside the recall budget, so the seal limit is set by recall latency as well
  as by memory — at most ~10k vector rows per memtable (S8: brute force 23 ms at
  10k × 768, 146 ms at 50k).
- **Vector indexes are built off the hot path** (maintenance, never in a write or
  read): IVF_RQ takes 4.6 s at 50k vectors and 40–65 s at 500k (S8).
- Reads go through `LsmScanner` (memtable + flushed generations + base) with an
  SSTable cache; fresh-row point lookups measured 1.6 ms.
- ADB compacts generations into the base table
  (`merge_insert(..).mark_sstables_as_compacted(..)`) and runs index maintenance (D5).

**B is not built.** It stays a documented fallback behind `adb-store`'s storage trait;
conformance tests target the trait so a future B is checked by the same suite. Revisit
B only if a Lance release breaks the MemWAL API, MemWAL loses or corrupts data, or put
cost regresses. If B is ever built, inserts use `append` (255 ms commits), not
`merge_insert` (~1 s and table-sized reads). SlateDB stays a last resort.

### 9.3 Apply, with storage-level fencing

With option A (§9.2) the applier writes through the partition's MemWAL shard writer
(spike R11, [`spikes/R11_raft_memwal_recovery.md`](spikes/R11_raft_memwal_recovery.md)):

- **One committed log batch = one `ShardWriter::put`** holding its rows **and** the
  `_meta` row (`log_offset` = the batch's offset). One put is one WAL entry, so rows
  and `_meta` persist together or not at all.
- **Recovery:** the new owner opens the shard writer (claims the shard epoch = the
  partition epoch, drops a fence sentinel at the WAL tip, replays the WAL), reads
  `_meta` through `LsmScanner`, and replays the Raft log from `_meta + 1`. The
  non-durable MemWAL tail that was lost is simply replayed from Raft. To avoid a cold
  lookup across generations (2 s for 20 on fsn1, R6), the replay start is
  `max(durable offset in the Raft state machine, _meta in the replayed memtable)`;
  re-applying an already-applied batch is harmless.
- **Stale owners are fenced by MemWAL:** the sentinel makes a predecessor's next WAL
  write collide (create-only); its entries from before the sentinel are replayed by
  the successor; its memtable flushes fail at the manifest commit. Tested: 300 local
  + 100 Hetzner takeovers with the old owner still writing — 0 inconsistencies,
  `_meta` never regressed.
- **ADB drives memtable seals** (workaround for a Lance 12.0.0 bug where
  generations sealed *during* WAL replay are committed without their `_pk_index`
  sidecar, breaking all LSM reads): Lance's automatic seal limits are set far above
  anything replay can reach; `adb-store` calls `force_seal_active()` at its own
  row/byte limit. Drop when fixed upstream.
- **Durable offset** for Raft log truncation: the `_meta` offset covered by a
  completed seal (generation flush) or WAL flush — never the in-memory value. How to
  observe it cheaply under `durable_write(false)` is part of R12.

**Commits into the base table** (ADB folding generations in, §9.2) keep the S2 guard:

- **Offset guard:** an applier commits a batch only if the table's
  `_meta.applied_offset` equals the offset it expects, and never applies an offset
  at or below the stored one. Without it, a stale applier that lost ownership
  mid-flight could re-apply an older batch after a newer one and revert rows,
  because upsert-by-id has no ordering of its own.
  - A read check on `_meta` **is not enough** (spike S2): on a commit conflict Lance
    re-runs the merge against the latest version, so a stale batch lands on newer
    data. Every apply therefore uses **`conflict_retries(0)`**. Each batch rewrites
    `_meta`, so two applies always conflict and the commit is a CAS on the version
    the applier read. On rejection the applier re-reads `_meta` and decides again.
  - Defence in depth: rows merge with
    **`UpdateIf("source.log_offset > target.log_offset")`**, so each row only moves
    forward. Deletes are tombstone rows carrying `log_offset` until compaction.
  - Maintenance commits (compaction, index build, cleanup) can reject an apply too;
    the applier retries at the ADB level (measured in S3).
- Records whose epoch is older than the partition's latest claim are dropped.
- On losing ownership the applier stops and closes its writers.
- **Durable offset:** recovery and log truncation use the `applied_offset` that is
  durable in Lance (base or flushed data), never the in-memory value.

## 10. Read path and routing

- The SDK caches the routing map (with overrides); a stale route gets a redirect and
  a map refresh.
- Agents never see partitions: they call `remember()`, `recall()`, `supersede()`,
  `forget()`, or send AQL directly (MCP and gRPC as today).
- Agent logic can run colocated on the partition owner (actor per partition), making
  recall a local call.

| Query | Served from |
|---|---|
| `WORKING` | Sequencer's in-memory map |
| `TOOLS` (load / ranking) | Hot cache, then table |
| Point lookup, entity, current facts | Primary-key BTree (+ memtable) |
| Graph traversal (`FOLLOW LINKS … DEPTH 1–3`) | BTree on `subject_id` / `object_id`, `kind = edge` |
| Semantic / vector recall | Vector index (or brute force below threshold), prefiltered by `tenant_id`, `scope`, `memory` |
| Full-text | FTS on `text` |
| Hybrid recall | Vector + FTS + filters in one query, then rerank |
| `AGGREGATE` / `REFLECT` analytics | DataFusion over the partition's Lance table |
| Cross-scope recall | Fan-out per partition, reciprocal-rank fusion |

**Consistency modes** (replacing v0.2's read strategies). Reads are served by the
partition owner, which serves only while its lease is valid and its claim is the
latest; otherwise it redirects.
- **Read-your-writes:** wait until the applied offset reaches the commit offset, or,
  on the owner, read through the pending overlay with no wait.
- **Bounded staleness** (default): whatever is applied.
- **Indexed-up-to:** indexes that don't cover fresh rows report "indexed up to
  offset N".

### 10.1 Local cache (mandatory)

Cold object-store reads measured **55–100 ms median with tails near 1 s** (S4,
Hetzner fsn1) — far outside the 5 ms point-read and 25 ms recall targets. Every owner
node therefore runs a **hybrid memory + NVMe cache** in front of Lance's data files,
index files, manifests and generation datasets:

- on a **disk or partition separate from the Raft log**, so cache I/O adds no fsync
  jitter (§11.3);
- warmed on partition wake (§14.1) and kept for partitions the node owns;
- also caches manifests and index metadata, which cuts the ~14 requests per table
  open (S7, §14.1);
- object-store reads behind it reuse connections and use hedged requests for the
  multi-second tails (storage baseline: TLS setup alone is 20–30 ms).

S4 must be re-run with this cache warm to set the real read numbers. The cache also
weakens the "stateless nodes" claim of the object-store commit tier (§11.5): nodes
there still need warm local disks to serve reads fast.

## 11. Replication and commit

The log is embedded: openraft runs in-process, one Raft group per replica set, with a
dedicated WAL writer thread per disk. **Iggy and Redpanda are not on the commit
path.** A **log trait** has three implementations: local WAL (embedded and
single-node), openraft (cluster, Raft tier) and object-store log (cluster, object-store
tier). The sequencer and applier don't change between them.

### 11.1 Commit point

A transaction commits when its batch is **fsynced on a majority of the replica
set**. Everything after that is materialization.

| Layer | Where | Durable? | Role |
|---|---|---|---|
| Raft log | Local NVMe, replicated to a quorum | Yes | Commit point; client acked here |
| Pending overlay | Sequencer memory | No | Validation of in-flight transactions; rebuilt from the log |
| Memtable | Owner's memory | No | Applied state, readable (option A: Lance memtable; option B: ADB memtable) |
| Lance MemWAL (option A only) | Object storage | Yes, but redundant | Lets Lance recover its memtable without ADB |
| Lance L0 / base | Object storage | Yes | Checkpointed state; basis for log truncation |

### 11.2 Commit protocol

- The leader replicates to followers **in parallel with its own local fsync**;
  commit = any majority has fsynced (in a 3-node set, possibly the two followers).
  From source (6 Oct): **openraft 0.10.0-alpha does this; 0.9.25 does not**. 0.9's
  `RaftCore` waits for the leader's fsync before replicating, so commit ≈ leader
  fsync + RTT + follower fsync. R3 measures both before pinning.
- Group commit: one `fdatasync` per 2–5 ms batch, shared by every partition whose
  batch is in it.
- Clients are acked at commit; visibility comes at apply (read-your-writes waits for
  `applied_offset`, §10).
- **No diskless (no-fsync) mode.** A node restarting with an empty log but a vote can
  elect a leader missing committed entries. If such a mode is ever used, restarted
  nodes rejoin as non-voting learners until caught up.
- MemWAL (option A) is a second durability layer for the same data, so its flushing
  can be lazy; losing unflushed Lance state only lengthens replay.
- **openraft is pinned at `=0.10.0-alpha.36`** (R1, 7 Oct 2026) behind a thin adapter
  in `adb-log`; upgrades are deliberate. Log storage sits behind `RaftLogStorage` as
  ADB's own segment files plus offset indexes (`raft-engine` is unmaintained, K6).
- The WAL writer receives batches over a channel, does `pwrite` + `fdatasync`
  (io_uring only if profiling shows syscall cost matters) and acks after fsync.

### 11.3 Node storage and retention

- **Power-loss-protected enterprise NVMe** on every replica-set node, followers
  included; local disks, not network volumes (on Hetzner: dedicated servers).
- A **dedicated disk or partition for the log**, so compaction and cache I/O don't
  add fsync jitter.
- Size: a few GB — only entries above the durable Lance offset plus archive lag.
- **Retention:** truncate the log only below the **durable** `_meta.applied_offset`
  (read after a flush/checkpoint, never the applier's in-memory counter), taking the
  minimum across all partitions in the group, and only after those segments are
  archived.
- **Laggard exception:** a partition whose applier is stuck beyond a threshold must
  not pin the whole group's log. Once segments are archived, local truncation may
  pass it, and that partition replays from the archive when it recovers.
- Sealed segments archived to object storage are the second durability tier.

### 11.4 Epochs and fencing

- Each partition has an **epoch**. A new owner appends
  `claim(partition, epoch=N+1, node)` through Raft; the first claim in log order wins.
- Every data record carries its writer's epoch; appliers drop records older than the
  latest claim.
- Failover is triggered by a lease (Kubernetes Lease or control-plane lease); safety
  comes from the log claim, not the lease.
- On losing ownership the applier stops and closes its Lance writers. Whether
  MemWAL itself fences a second writer is to be verified; ADB does not depend on it.
- The **offset guard** (§9.3) protects the table even if a stale applier slips past
  the epoch check.

### 11.5 Alternative commit mode: object-store commit (cheap shared tier)

- No Raft: the partition owner group-commits each batch to object storage (MemWAL or
  a log object) with conditional-write fencing, and acks after the PUT succeeds.
- **Create-only fencing (spike B1):** on Hetzner, `If-None-Match: *` create is atomic
  but `If-Match` replace fails spuriously. Each batch is therefore a new immutable
  object (`log/p{partition}/{offset:020}`) claimed by creating it; leases are
  `lease/p{partition}/{epoch}` objects; nothing is ever updated in place.
- Nodes become stateless: no NVMe, no replica sets, no consensus code — the simplest
  scale-to-zero.
- Costs: commits take 10–100 ms instead of about 1 ms; request limits cap frequency
  (at Hetzner's 750 req/s per bucket and a commit every 100 ms, about 75 active
  partitions per bucket) unless several partitions share one log object; durability
  is the provider's.
- Both modes sit behind the log trait; Raft serves the low-latency and enterprise
  tier. The mode is chosen per tier (§19).

### 11.6 Raft operations

What the spec left implicit, decided here:

- **State machine and snapshots.** The replicated state machine is the log plus, per
  partition, `(owner, epoch, durable applied_offset, Lance version)`. A Raft snapshot
  is that small map, a pointer; table data never travels between nodes (R7: 174 bytes
  for three partitions). A lagging or new follower installs the snapshot, then streams
  the log tail. It has **no log before the snapshot point**, so when it becomes owner of
  a partition whose durable offset is older, it reads that range from a peer's log
  (`adb-log` range-read RPC) or from the archive. openraft's automatic purge is off
  (`max_in_snapshot_log_to_keep = u64::MAX`); ADB purges per §11.3.
- **Who applies.** Only a partition's owner materializes it into Lance; other members
  hold the log, so each Lance table has exactly one writer.
- **Reads.** The owner serves reads of applied state while its lease is valid and its
  claim is current. Linearizable reads, when requested, confirm ownership with a
  ReadIndex round through the leader. Followers serve nothing directly.
- **Membership.** Replacing a node: add it as a learner, let it catch up from snapshot
  plus tail, promote it (joint consensus), remove the old node. **To replace the
  leader, transfer leadership to another voter first** (R8: removing it directly stalls
  commits ~1 s for an election; with a transfer, 12–76 ms). **A node with a lost or
  empty disk always returns under a new node id** (R7: openraft does not notice a
  same-id node that lost its log, and a voter without its log/vote is unsafe). The control plane
  drives every step; a replica set never runs with fewer than three voters
  deliberately.
- **Bootstrap.** The control plane creates a replica set, initializes the group with
  three voters, and assigns initial owners by committing claims; the routing map
  publishes owners, and SDKs follow redirects.
- **Timing.** Heartbeat and election timeouts are set for in-region networks:
  **heartbeat 30 ms, election 150–300 ms, pre-vote on** (R6: an unplanned leader crash
  costs 465–565 ms; the earlier 50 / 300–600 ms took 0.9–1.4 s). Heartbeats are batched
  per node pair. To be confirmed on the servers for false elections under load. After an election the control plane may transfer leadership
  back to keep the leader's load predictable.
- **Testing.** Jepsen-style fault injection (network partitions, clock skew, leader
  and owner kills, disk-full, slow fsync) is a gate for the cluster milestone, on
  top of the single-node fault tests.

### 11.7 Subscriptions and continuous queries

Full spec: [`SUBSCRIPTIONS_CQ_SPEC.md`](SUBSCRIPTIONS_CQ_SPEC.md) (7 Oct 2026, adopted
9 Oct with the adjustments below). Both features are fed from the **committed** log
and are never on the commit path; agents never poll tables.

| | Subscription (standard) | Continuous query (premium) |
|---|---|---|
| Agent asks | "Tell me when anything here changes" | "Tell me when this query's answer changes" |
| API | `SUBSCRIBE SCOPE \| TOPIC … [KINDS …] [FROM OFFSET n \| FROM NOW]`, `PUBLISH TOPIC`, `ACK`, `UNSUBSCRIBE` | `WATCH <AQL query> [DEBOUNCE …]` → snapshot at offset N, then diffs; `UNWATCH` |
| Engine work per write | Filter by scope/topic, forward the committed effects | Match against watch sets; update the result incrementally or re-run (debounced) |
| Cost | Near zero (a cursor on the log) | Per-query state and evaluation, metered |

**Placement.** A topic hashes like a scope (`hash(tenant, topic) mod 4096`), so a scope
or topic lives in one partition and all its subscribers share one order. Each
subscriber has its **own cursor** (broadcast, not competing consumers). Topic messages
are ordinary committed writes (`kind = event`) with a per-topic TTL.

**Execution.**
- The **matcher** runs on the partition owner, on the sequencer's core, **right after
  the apply `put`** (§9.3) — the moment a change becomes readable — against a
  tenant-first index of subscriptions and continuous queries (static watch sets
  percolator-style; read-sets for graph traversals). It has a time budget per batch;
  work over budget goes to a per-partition evaluation task. The matcher never blocks
  apply.
- An always-on **gateway** tier holds agent connections (gRPC stream, MCP,
  WebSocket), per-subscriber buffers and acks, catch-up reads, and routing to current
  owners. A slow subscriber is switched to catch-up from its cursor; it never slows
  the sequencer.
- Cross-partition continuous queries wait for a **watcher tier** (v2).

**Semantics.** Total order within a partition, none across partitions; delivery is
**at-least-once**, tagged with the producing offset (clients drop offsets ≤ their last
ack); deliveries are emitted at **apply**, so a pushed change is already readable on
the owner. A continuous query registers by snapshot at offset N, then tails from N+1 —
no missed or duplicated changes. The v1 `WATCH` subset is: filters / point lookups,
current facts (with `valid_to` timers), count / sum / min / max, graph traversal
(re-run on read-set hit), top-k vector recall (k+m buffer) and hybrid recall
(re-run); anything else is rejected at registration.

**Durability.** Registrations and cursors are rows in the partition table (`kind =
sub | cq`) written through the log (`RegisterSub`, `UpdateSubCursor`, `RegisterCq`, …),
so they inherit replication, hibernation, encryption and forget. Continuous-query
results are not persisted; they are re-snapshotted on wake or failover.

**Adjustments from the spikes** (recorded in the spec):
- The matcher hook is after the MemWAL `put`, not after a `merge_insert` (option A).
- **Catch-up past log retention is a reset, not a replay:** a `log_offset > cursor`
  table scan sees only each row's current version (intermediate updates collapsed,
  deleted / forgotten rows absent), so it is delivered as a `reset` with current state.
  Exact change replay is promised only within log + archive retention.
- **Subscriptions never keep a partition awake** (this replaces the v1.3 rule that
  they block hibernation) — see §14.1 for scheduled wakes.
- `UpdateSubCursor` costs log volume per ack batch; cursors are batched across
  subscribers in one effect (open: gateway-held cursors with checkpoints).

Exports to analytics and webhooks are ordinary subscribers; Iggy or Redis/Dragonfly
Streams may sit behind the gateway for lossy fan-out.

## 12. Failure, recovery and rebalancing

Recovering or moving a partition is one operation: claim the epoch, open state from
object storage, replay the log tail. No data is copied between nodes. (v0.2's
"cross-VM migration" is this operation.)

**Recovery sequence**
1. The owner's lease expires; the control plane picks a new owner within the replica
   set.
2. The new owner appends `claim(partition, epoch=N+1)` through Raft.
3. It opens the partition's Lance table and MemWAL shard writer. With no periodic
   MemWAL WAL flush (§9.2) there is (almost) nothing to replay from object storage
   (R6: 0.4–0.55 s on fsn1).
4. It takes the replay start from `max(durable offset in the Raft state machine,
   _meta in the replayed memtable)` (§9.3) and replays committed records after it from
   the local log, a peer's log or the archive (R7); re-applying an already-applied
   batch is harmless.
5. It rebuilds the pending overlay and the `WORKING` / `TOOLS` hot tier from
   committed-but-unapplied records, then serves.

| Failure | Effect | Handling |
|---|---|---|
| Raft leader crashes | Commits pause for every partition in the set until election | Election (sub-second); owners keep their partitions and forward to the new leader |
| Partition owner crashes | That partition is unavailable until a new claim | Recovery sequence; RTO = lease timeout + table open + tail replay |
| Crash mid-apply | Rows and `_meta` land together or not at all | Replay from durable `applied_offset` |
| Zombie writer after a partition | Its records and commits are ignored | Epoch fencing in appliers, offset guard in the commit, writers closed on lease loss |
| Minority of replica set lost | None for commits | Raft continues; replace the node (§11.6) |
| Majority of replica set lost | Writes stop for its partitions | Restore from object storage + archived log segments; possible loss of the unarchived tail |
| Object storage slow or throttled | Flush and compaction lag; reads hit memtables and cache | Raft log stays durable locally; backpressure on compaction |

**Rebalancing** (one Raft group per replica set, §6.1)
- Within a set, the control plane moves partition **ownership** by load (hot
  partitions, core and node CPU, memory) with `claim(partition, epoch+1)`; data stays in
  object storage. A **leader change moves no partitions**; leadership is moved
  separately (transfer, §11.6).
- **Capacity grows by adding replica sets.** Moving a partition to another set is the
  same claim-open-replay sequence in the target set; a set never grows to a fourth
  node.
- Seal hot partitions often (§9.2) to keep tail replay, and therefore failover, short.
- Every partition always has an owner while its set is live; only hibernation
  (§14.1) leaves a partition unowned.

## 13. Deployment profiles (one engine)

**Product decision (7 Oct 2026):** ADB ships as part of A3T, **SaaS only**. The only
build that runs outside the service is the **local dev** single node. There is no
embedded production profile and no BYOC.

| Profile | Log | Storage | `CLAWDB_REQUIREMENTS` target | Use |
|---|---|---|---|---|
| **Cluster — Raft tier** | openraft replica sets on NVMe | Object storage (Hetzner) | (3) Full ADB | **Production SaaS**: shared and dedicated tiers |
| **Cluster — object-store tier** | Object-store log, conditional writes | Object storage | (3) Full ADB | Candidate cheap shared tier; only if O1–O3 justify it |
| **Local dev (single node)** | Local WAL | Local directory | (1) ClawDB | `adb-server` (gRPC + MCP) for developers; not for production data |
| **Playground** (optional) | None | In-memory | (2) WASM playground | Browser demo; parser + in-memory engine, no persistence |

Local dev and the cluster run the same sequencer, applier and table schema; only the
log implementation and placement change (a single node owns all 4,096 partitions,
most of them hibernated), so the dev build exercises the whole write path. Dev data
on local disk is not power-safe (K8) — acceptable for a dev build.

## 14. Scale-to-zero, tenancy and security

### 14.1 Hibernation, honestly scoped

An idle partition is files plus a log offset. **Partitions are shared**, though:
hashing spreads thousands of tenants over 4,096 partitions, so a shared-tier
partition sleeps only when all of its tenants are idle. So:

- Per tenant, the cost is **storage plus a share of pooled compute**; an idle tenant
  adds nothing beyond its storage.
- Partition hibernation pays off at low global load, overnight, and in the
  **dedicated tier**, where a tenant's own partitions sleep with it.
- The advantage over Neon (no per-tenant process to rehydrate) stands, at partition
  granularity.

#### Operating modes: hibernated (default) or always-on

Both modes are offered (decided 9 Oct 2026):
- **Hibernated — the default.** Unless a customer asks otherwise, ADB and FlowR
  hibernate when idle, to keep A3T's cost down. A3T bills per agent and listening time;
  for billing, an agent is alive while its trigger is active.
- **Always-on — on request.** The customer pays for the full run time and may run as
  many agents as it wants; nothing hibernates.

Pricing is not the focus at this stage; the goal is a working, usable product. The
mode only decides whether the mechanisms below are applied.

#### Two levels of hibernation

**1. Partition hibernation.** The partition has no owner and is just files plus its
durable offset. Nodes stay up. It wakes lazily on its first request.

- **Hibernate:** stop writes, drain the batch and the applier, seal so the durable
  offset equals the commit tip, close the shard writer, release ownership, drop caches.
  **Subscriptions and continuous queries do not keep a partition awake** (§11.7): a
  hibernated partition receives no writes, so nothing it holds can change. Their
  registrations and cursors are already committed rows; continuous-query results are
  rebuilt on wake. The gateway keeps agents' connections open and heartbeats.
- **Wake** = claim + table open + replay + rebuild — never "start a sequencer" (§8):
  1. **Claim the epoch** (one Raft entry).
  2. **Open the Lance table and MemWAL shard writer:** manifests and index metadata —
     ~14 object-store requests (S7), the dominant cost; served from the local cache
     where possible (§10.1).
  3. **Read the durable offset** (Raft state machine, §9.3) and **replay the tail**,
     which is empty after a clean hibernate.
  4. **Rebuild** the pending overlay and the `WORKING` / `TOOLS` hot tier. `WORKING`
     lives in sequencer memory and is reconstructable only because it is logged. **A
     scope that marks `WORKING` as `volatile` loses it on hibernate.**
  5. **Load subscriptions and continuous queries:** the subscription index and CQ
     registrations; re-snapshot each CQ at the current offset. The matcher is live
     before the first new batch applies (CQ clients get a `reset` only if their last
     delivered offset is older than the re-snapshot).
  6. **Prewarm** the hot indexes and the local cache (v0.2's session loading).
- **What wakes a partition:** a write or read for it, or a **scheduled wake**.
  Subscribers alone never wake a partition. Results that change with the clock
  (`valid_to` passing, TTL expiry, relative time windows) are driven by the owner's
  timer wheel; a hibernated partition with a pending boundary registers a scheduled
  wake with the control plane at its earliest boundary. Partitions with no CQs or
  TTLs never schedule wakes.

**2. Node / replica-set hibernation.** When a whole replica set is idle for a
configurable period (e.g. 1–2 h):
- seal every partition so each durable offset equals the commit tip; archive the log
  tail; store the Raft snapshot (the small owner / epoch / offset map) in object
  storage; then release the nodes.
- **Node wake:** a request arrives for a partition in a hibernated set. The control
  plane starts nodes, **bootstraps a fresh group from the archived snapshot** (with new
  node ids — R7: a node without its disk never rejoins under its old id), and the nodes
  come up owning no partitions. The requested partition wakes first; the others wake
  lazily. **Never wake all 4,096 at boot.**

**Invariant:** a live request never hits an unowned partition and fails. Waking the
partition is the first step of serving that request.

**Open conflict (§19), deferred:** §11.3 requires power-loss-protected NVMe on
dedicated servers, and Hetzner dedicated servers are billed monthly, so releasing them
saves nothing. Options: hourly-billed servers for hibernating sets (relying on the
empty-tail invariant at hibernate time), a small always-on pool that hibernates only
partitions, or the object-store commit tier (§11.5) for tiers that need true
scale-to-zero. Deferred while pricing is not the focus: hibernated mode starts as
**partition hibernation on an always-on pool**, which needs no new infrastructure.

#### Wake throughput budget

Hetzner limits requests to 750/s per bucket; one open + first query costs ~14
requests, and S7 reached ~45 opens/s per bucket in practice. So:

```
opens/s ≈ Σ over buckets (750 − steady-state req/s) ÷ 14      (S7: ~45 per bucket)
```

With partitions spread over 20 buckets that is ~900 opens/s, so waking every
partition takes ~5 s. Lazy wake makes that the worst case, not the normal case.
Caching manifests and index metadata locally (§10.1) cuts the 14 requests per open.

**Object-storage write budget:** with the Raft log as the WAL, MemWAL writes only at
seals (§9.2): ~0.8 writes/s per busy partition (S6), less when idle. Partitions are
spread across buckets to stay inside per-bucket limits.

### 14.2 Tenancy tiers

| | Shared tier | Dedicated tier |
|---|---|---|
| Partitions | Global pool, tenant prefix | Own partition set |
| Nodes | Shared | Optionally dedicated |
| Storage | Shared buckets, tenant prefixes | Own bucket or prefix |
| Keys | Per-tenant data keys | Customer-managed keys |
| Noisy neighbours | Per-tenant rate limits at the sequencer | Not applicable |

### 14.3 Encryption and verifiable forget

Encrypting `text` and constraint fields before the log would break full-text search
and sequencer constraints, which need plaintext. So:

- **Log records and archived segments** are encrypted with the tenant's (or scope's)
  data key. Destroying the key crypto-shreds them, including backups, where physical
  deletion is impractical.
- **Lance rows** keep searchable fields in plaintext (`text`, constraint fields,
  `embedding`) under server-side encryption; non-searched payload fields inside
  `value` stay encrypted with the tenant key. Hetzner supports **SSE-C only** (no
  SSE-S3, no bucket default), one key per store, i.e. per partition table (S12). So
  at-rest keys are **per partition** in the shared tier and **per tenant** only in the
  dedicated tier, where a tenant owns its partitions.
- **Forget** (AQL `FORGET`) = destroy the key where the whole scope goes; delete the
  rows; compact; run old-version cleanup with a **short version-retention window**,
  so old Lance versions and flushed data are physically gone within a stated forget
  SLA. Verified in S11, with one addition: forget also needs MemWAL tombstones →
  generation compaction → **MemWAL GC** (§9.2), because WAL files and generations
  keep the plaintext otherwise. Old index files keep vectors until the version cleanup.
- Embeddings are treated as sensitive data.

### 14.4 Credentials, authorization, audit

- `create_agent(scopes=[…])` issues a credential limited to those scopes; the
  sequencer checks scope on every request (v0.2's namespace isolation).
- Delegation (agent acting for a user) is a ReBAC shape: start with scope checks;
  revisit a dedicated engine (InferaDB-style ReBAC or AESOP) when customers need
  fine-grained sharing.
- Every committed record carries tenant, scope, agent and `txn_id`; hash-chaining
  log records gives a tamper-evident trail.

## 15. Relationship to FlowR

- FlowR uses ADB for **agent memory** as a service (ADB is SaaS-only, §13), against
  the local dev build during development. Today FlowR's `call:a3t:agent` skips memory
  recall/store ("ClawDB integration pending"); wiring it is a FlowR task against the
  local dev build.
- FlowR's own control-plane data (templates, agents, canvases, deployments) and run
  state are **not** in the v1 memory schema. Clustering FlowR needs a shared queue,
  shared state and pub/sub for live sessions. **Pub/sub is answered by ADB
  subscriptions (§11.7):** each FlowR instance subscribes to the scopes or topics it
  follows and sees every change in one order, with no FlowR-to-FlowR coordination.
  Open: whether FlowR's run-state notifications use topics directly. Whether the rest of FlowR's state
  becomes a second ADB table family on the same log/sequencer engine is an open
  decision (§19), taken after the single-node milestone.

## 16. Crate plan

| Crate | Status | Role in v1.0 |
|---|---|---|
| `aql-parser`, `aql-planner`, `adb-executor` | Keep | AQL front end and operation tree (unchanged contract) |
| `adb-core` | Keep, extend | Types; add log record, effects, offsets, epochs |
| `adb-mcp`, `adb-server` | Keep | MCP and gRPC/HTTP surfaces |
| `adb-backends` | Repurpose | In-memory engines become the hot tier and memtable option B; the `LinkStore` gains persistence via edges |
| `clawdb` | Fold in | Its Lance code seeds the applier; its table-per-memory-type layout and in-memory links are replaced |
| `clawdb-wasm` | Keep | Playground profile |
| **`adb-log`** (new) | New | Log trait; local WAL, openraft and object-store implementations; WAL writer thread; archiver |
| **`adb-sequencer`** (new) | New | Validation, overlay, group commit, hot tier |
| **`adb-store`** (new) | New | Storage trait; Lance MemWAL writer + `LsmScanner` reads, generation compaction, index maintenance, hot-tier cache; option B and SlateDB are fallbacks behind the trait |
| **`adb-cluster`** (new) | New | Control-plane client, routing map, leases, hibernation, scheduled wakes, gateway routing table |
| **`adb-cq`** (new) | New | Continuous-query result state, update strategies, read-set capture, debounce (§11.7) |
| **`adb-gateway`** (new) | New | Client streams, delivery buffers, acks, catch-up, routing to owners (§11.7) |

## 17. Spike (about 2 weeks, single node): go/no-go on memtable option A

- [x] **0. Find a released Lance crate that exposes the MemWAL writer API**, and pin
      it. → Lance 12.0.0 (spike B2).
- [ ] `merge_insert` p99 for 1–50-row batches (A: LSM path; B: plain).
- [ ] Point-read and `subject_id` scan p99 across memtable, flushed data and base.
- [x] Atomicity of one commit (rows + `_meta`) across a process kill. → 1,000 kills on
      Hetzner fsn1, 0 inconsistencies.
- [x] Offset guard: two appliers racing on one table never revert a row. → only with
      `conflict_retries(0)` + `UpdateIf` (§9.3).
- [ ] Which indexes `maintained_indexes` covers (vector, FTS, scalar) — A only.
- [ ] PUTs per active table per second, and whether MemWAL durability can be relaxed
      — A only.
- [ ] Open / close / reopen cost per table, at 4,096 tables per bucket set.
- [x] Conditional writes (`If-None-Match` / `If-Match`) on Hetzner object storage —
      **blocker** for Lance commits. → create-only works (spike B1).

## 18. Delivery path

1. **Spike** (§17) → choose memtable A or B.
2. **Single node (local dev build):** sequencer, local WAL, applier, memory schema,
   **persisted links**, `WORKING` / `TOOLS` hot tier; the AQL conformance suite keeps
   passing (116/116 today).
   **Subscriptions** ship here: `SUBSCRIBE` / `PUBLISH` / `ACK`, matcher in apply,
   durable registrations and cursors, in-process gateway (§11.7).
3. **Recall:** vector / FTS / scalar indexes, brute-force threshold, hybrid ranking.
   **Continuous queries v1** follow (they depend on the recall indexes): classifier,
   static watch sets, incremental classes, timer wheel, scheduled wakes; v1.1 adds
   traversal re-run, top-k buffer, hybrid re-run, metering.
4. **Replication:** openraft replica set (one group), owner forwarding, epochs and
   claims, offset guard, archiver, recovery, membership, Jepsen-style tests; the
   object-store commit mode alongside.
5. **Control plane:** routing map + overrides, leases, hibernation and wake, tiers.
6. **Multi-node:** ownership balancing, cross-set handoff, bucket spreading; the
   gateway tier with resume across failover; cross-partition continuous queries on a
   watcher tier (v2).

**Day-one rules**
- Every mutation goes through the ADB log; the table is a materialized view of it.
- AQL is the only public contract; Lance and the log sit behind traits.
- ADB-owned ULIDs for every record.
- Log payloads encrypted with tenant keys (§14.3).
- Conformance and fault-injection tests from the first milestone: crash mid-apply,
  stale applier, replay, forget.

## 19. Open questions and risks

Worked through, with experiments, exit criteria and fallbacks, in
[`RISK_BURNDOWN.md`](RISK_BURNDOWN.md).

| Item | Type | Why it matters | Next step |
|---|---|---|---|
| MemWAL writer API availability | ☑ Resolved (B2) | Shipped in Lance 12.0.0, with epoch fencing | Benchmark A vs B on Hetzner (S3, S4, S6) |
| Atomic commit with `_meta` | ☑ Resolved (S1, R11) | Crash-safe apply depends on it | 1,000 kills on fsn1 + 400 MemWAL kills, 0 inconsistent |
| Offset guard inside a Lance commit | ☑ Resolved (S2) | A read check alone lets Lance's retries revert rows | `conflict_retries(0)` + `UpdateIf` on `log_offset` (§9.3) |
| Object-store conditional writes (Hetzner) | ☑ Resolved (B1) | Create-only is atomic (Lance's commit path works); `If-Match` replace is broken | Design rule: create-only objects; open issue B1a (three gateways reject the key) |
| openraft 0.9 vs 0.10 | ☑ Decided (R1) | API churn vs features | Pinned `=0.10.0-alpha.36` — only line with parallel leader fsync |
| One Raft group per replica set | Decision | One leader carries all of the set's appends and replication | Benchmark leader throughput; multi-Raft only if it saturates |
| Commit mode per tier | Decision | Raft (~1 ms, NVMe replica sets) vs object-store commit (10–100 ms, stateless nodes) | Choose per tier; both behind the log trait |
| openraft async leader flush | ☑ Resolved (R2) | Tested on 0.10: +20 ms on the leader's fsync leaves commits unchanged | — |
| Replicated group-commit latency | ◐ Indicative | Sets the `remember()` budget | One box: p50 0.45 ms (1 client), ~21.9k commits/s; confirm on three servers (R3/R4) |
| Partition count (4,096) | Decision | Hard to change later | Confirm against target node count and largest tenant |
| Hetzner 750 req/s per bucket | Risk | Caps writes, reads and table opens per bucket (S7: ~45 opens/s) | Many buckets per cluster; writes only at seals (§9.2); cache manifests (§10.1) |
| Encryption vs search (§14.3) | Decision | Plaintext needed for FTS/constraints | Split rule adopted; S12: SSE-C per table only → per-tenant at-rest keys in the dedicated tier |
| Cross-partition operations | Decision | Shared scopes on other partitions | Sagas/outbox first; 2PC only on demand |
| FlowR state as an ADB table family | Decision | Basis for FlowR cluster mode | Decide after single-node milestone |
| **ADB licence** | ☑ Decided (9 Oct) | SaaS-only; the dev build ships under the **same business support licence as FlowR** | Add the LICENSE text once FlowR's exists (FlowR's `Cargo.toml` still says Apache-2.0; no LICENSE file in either repo); align `Cargo.toml` and README; drop "ADB-CE" |
| **Node hibernation vs PLP dedicated servers** | Risk, deferred | Dedicated servers are billed monthly, so releasing them saves nothing; a woken set is a fresh bootstrap (§14.1) | Start hibernated mode as partition hibernation on an always-on pool (§14.1); revisit with pricing
| **Wake throughput per bucket** | Risk | ~14 requests per open, ~45 opens/s per bucket (S7); a mass wake is bucket-bound | Spread over ~20 buckets (~900 opens/s, all partitions in ~5 s); lazy wake; local manifest cache (§14.1) |
| **Temporal results on hibernated partitions** | Risk | `valid_to` / TTL boundaries change results with no write; without a scheduled wake they go stale silently (§14.1) | Timer wheel + scheduled wakes in the control plane; gate test on wake tolerance |
| **Continuous-query matcher budget** | Unknown | Up to 2,000 CQs per partition must not slow apply | Benchmark matcher cost per batch; demote slow CQs to periodic re-run |
| **Subscription catch-up beyond log retention** | Behaviour to document | A table scan sees only current row versions, so old cursors get a `reset`, not a replay (§11.7) | Publish the replay-retention promise per tier |
| **Cursor write volume** | Unknown | `UpdateSubCursor` per ack batch adds log records | Batch cursors across subscribers; decide on gateway-held cursors |
| **Hot-scope single-sequencer ceiling** | Risk | One scope = one partition = one core; no single scope can scale writes past one sequencer (§6.2) | Measure the ceiling with `id` + `subject_id`; fine keys, move or split hot scopes |
| **`volatile` WORKING lost on hibernate** | Behaviour to document | Unlogged working memory cannot be rebuilt on wake (§14.1) | State it in the AQL docs; default scopes log WORKING |
| Crowded memory market | Market | Differentiation | Lead with cost model, temporal/provenance memory, verifiable forget |

## 20. Changes from the 5 Oct drafts

1. **Raft topology:** one Raft group per replica set, as in the spec. Partition
   owners sequence and apply; batches are forwarded to the group leader; a leader
   change does not move partitions. Multi-Raft is a scaling lever behind a benchmark
   (§8). (v1.0 had proposed 8–16 groups per set; reverted in v1.1.)
2. **Storage-level fencing:** offset-guarded apply prevents a stale applier from
   reverting rows (§9.3).
3. **Durable offset:** recovery and truncation use the offset durable in Lance
   (§9.3).
4. **Log retention:** the spec's rule (below the durable offset, minimum across the
   group, after archiving) plus a laggard exception so one stuck partition can't pin
   the group's log (§11.3).
5. **Memtable:** Lance MemWAL is option A, contingent on an API missing from Lance
   2.0.0; an ADB-owned memtable built from `adb-backends` is the planned fallback;
   SlateDB is last resort (§9.2).
6. **Encryption:** split rule — log/archives encrypted, searchable Lance columns
   plaintext under per-prefix SSE, physical deletion with a short retention window
   (§14.3).
7. **Hibernation claims:** scoped to partitions shared across tenants (§14.1).
8. **AQL mapping:** explicit `memory` column; `WORKING` is log-plus-memory (or
   volatile); links persisted as edges (§5).
9. **Cores:** many partitions per core, never two cores per partition (§8).
10. **Routing:** hot-scope pinning via a versioned override table; cross-scope recall
    is fan-out plus fusion (§6).
11. **Profiles:** playground / embedded / single-node / cluster share one engine,
    mapped to `CLAWDB_REQUIREMENTS` targets (§13).
12. **Small partitions:** brute-force recall below a row threshold (§7).
13. **Log size:** embeddings stored as f16 in the log (§9.1).
14. **Reconciled with v0.1 / v0.2 / ClawDB:** log-first replaces write-behind;
    in-memory backends become the hot tier; crate plan (§2, §16).
15. **Commit design adopted from the updated spec (v1.1):** majority-fsync commit
    point and layer table, parallel leader fsync, no diskless mode, PLP NVMe with a
    dedicated log disk, and object-store commit as a second log mode (§11.1–11.5).
16. **Raft operations added (v1.1):** snapshots as pointers, owner-only apply and
    reads (ReadIndex for linearizable reads), learner-based membership changes,
    bootstrap, timing, Jepsen-style testing (§11.6).
17. **Spike results (v1.2, 6 Oct):** MemWAL writer found in Lance 12.0.0 (§9.2);
    offset guard redefined as `conflict_retries(0)` + per-row `UpdateIf` after the
    read-check version failed the race test (§9.3); parallel leader fsync needs
    openraft 0.10 (§11.2).
18. **Architecture review (v1.3, 7 Oct):** the sequencer is per-partition state on a
    core's executor, never a thread (§8); scaling by adding replica sets and moving
    ownership by claim (§6.1); the hot-scope single-sequencer limit (§6.2); the local
    memory + NVMe cache is mandatory (§10.1); change feed and continuous queries
    (§11.7); two-level hibernation with an explicit wake sequence, the never-unowned
    invariant and a wake budget (§14.1); SaaS-only profiles (§13); new risks (§19).
19. **Subscriptions and continuous queries (v1.4, 9 Oct):** adopted the spec as §11.7 —
    owner-local matcher after the apply `put`, always-on gateway, snapshot-then-tail
    CQs, the v1 `WATCH` subset; registrations and cursors as table rows (`kind = sub |
    cq | event`, §7); subscriptions no longer block hibernation, scheduled wakes cover
    temporal boundaries (§14.1); new components and crates (§8, §16); delivery
    milestones (§18); new risks (§19).

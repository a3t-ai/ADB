# ADB (Agent Database) — Architecture

> **Version:** 1.0 (final for build)
> **Date:** 5 Oct 2026 · @Sriram
> **Language:** AQL v0.5 (spec and reference implementation in the AQL repo)
> **Supersedes:** `ARCHITECTURE_v0.1_in_memory.md` (in-memory engine),
> `adb_lance_persistence.md` v0.2 (cache + write-behind draft), the 5 Oct cluster
> draft, and the sidecar stack in AQL spec §23.
> **Relates to:** `CLAWDB_REQUIREMENTS.md` — this document is its target (3), "Full
> ADB"; targets (1) ClawDB and (2) the WASM playground map onto §13's profiles.

---

## 1. Overview

ADB is an agent-memory database, offered as a hosted service and as an embeddable
engine. Agents talk AQL (`STORE`, `RECALL`, `LINK`, `FORGET`, `REFLECT`, …);
storage is an internal detail.

A cluster is a fixed set of **4,096 global partitions**. Each partition is a state
machine with **one writer**, fed by a **replicated log**, materialized into **one
Lance table on object storage**, and able to **hibernate** to files plus a log
offset. Lance's columnar base serves vectors, full-text, scalar filters and
analytics; small writes are absorbed by an in-memory layer in front of it.

The same engine runs as the WASM playground, embedded inside FlowR or an agent
process, as a single-node server (ClawDB), and as a cluster (§13).

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
| Raft group | Replicates a fixed subset of partitions | ADB (fixed per replica set) | 8–16 per replica set |
| Replica set | 3 nodes hosting several Raft groups | Control plane | Tens to hundreds |

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

## 7. Table layout

One Lance table per partition, `s3://{bucket}/p{partition}/memory.lance`. One table
per partition keeps commits, recovery and hibernation independent, and lets a whole
committed batch apply as one atomic `merge_insert`. (ClawDB's table-per-memory-type
layout can't do that.)

| Column | Type | Used by |
|---|---|---|
| `id` | ULID string, unenforced primary key | All |
| `memory` | enum: working, tools, procedural, semantic, episodic | AQL `FROM`/`INTO` |
| `kind` | enum: entity, fact, edge, episode, tool, meta | All |
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
  `memory` and `kind`; FTS on `text`; vector on `embedding`.
- **Small partitions:** below a row threshold (initially ~50k vector rows) recall is
  a brute-force scan; ANN indexes are built only above it, so 4,096 tables don't
  each carry a near-empty index.
- **Control row:** `id = "_meta"`, `kind = meta`, carrying `applied_offset` and
  `epoch`, rewritten in every applied batch.

## 8. Cluster topology

```
                ┌────────────── Control plane (+ Postgres) ──────────────┐
                │ routing map · overrides · leases · hibernation · tiers │
                └───────────────▲───────────────────────▲────────────────┘
                     map / redirect               place / wake
   SDK ──txn──▶ ┌─────────────── Replica set (3 nodes) ───────────────┐
                │ Node A: leads groups 1,4,7   Node B: 2,5,8   C: 3,6  │
                │  per core: sequencers for that core's partitions     │
                │  openraft (one instance per group) ─ WAL thread/disk │
                │  appliers ─▶ Lance tables on object storage          │
                └─────────────────────────────────────────────────────┘
```

**Who sequences a partition.** With one Raft group per replica set, only the group
leader can propose, so every partition in the set would be sequenced by one node,
and rebalancing inside the set would move everything at once. Instead:

- Each replica set runs **8–16 Raft groups**; a partition belongs to exactly one
  group (fixed).
- **The group leader is the sequencer** for all of its partitions. Leaders are
  spread across the three nodes, so load is balanced.
- **Rebalancing inside a replica set** is a Raft leadership transfer of a group.
  Moving partitions **between replica sets** is the rarer handoff in §12.
- Heartbeats are batched per node pair across groups.

| Component | Runs where | Responsibility |
|---|---|---|
| SDK | Agent process | Hash to partition, cache routing map (with overrides), idempotency keys, fan-out reads |
| Control plane | Separate service + Postgres | Routing map, overrides, leases, hibernation, tenant tiers, bucket assignment, agent config |
| Sequencer | Group leader, per partition, on that partition's core | Validation, pending overlay, group commit, `WORKING` / `TOOLS` hot tier |
| openraft | One instance per group | Replicate batches, elect leaders |
| WAL writer | One OS thread per disk, outside tokio | `pwrite` + `fdatasync` of Raft log segments |
| Applier | Per partition | Apply committed batches to Lance (§9.3) |
| Archiver | Per node | Ship sealed log segments to object storage |
| Maintenance | Per partition, background | Flush, compaction, index build, old-version cleanup |

**Cores:** partitions are assigned to cores (many per core) and each core runs a
single-threaded executor for its sequencers, Dragonfly-style. A partition is never
touched by two cores.

## 9. Write path

A transaction commits when **one log record holding all its effects** is replicated
to a quorum (or fsynced locally in the single-node profiles). There is one commit
point, so there is no 2PC across engines.

1. The SDK computes the partition and sends the transaction to the group leader,
   with an idempotency key and, for read-modify-write, its read-set versions.
2. The sequencer validates against the table plus the **pending overlay** (appended
   but not yet applied): existence, uniqueness, references, OCC versions.
3. Accepted transactions join the current group-commit batch (2–5 ms window).
4. The batch is proposed to the group as one Raft entry; each record carries
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
| Maturity | Format and index metadata exist in Lance 2.0.0 (regions, region manifests, flushed/merged generations, `maintained_indexes`; vector memtables inherit PQ/SQ params). **The writer-side API named in the draft (`LsmWriteSpec`, `flush_lsm`, `close_lsm_writers`, `writerConfigDefaults`, `use_lsm`) is not in the 2.0.0 crate** | Long-stable Lance APIs plus code ADB already has |

**Decision:** spike A first; **B is the planned fallback**. The sequencer's overlay
and the existing in-memory backends already are most of B, and the Raft log already
is its WAL, so B removes the double-WAL and MemWAL-maturity risks. SlateDB stays
behind the storage trait only as a last resort.

### 9.3 Apply, with storage-level fencing

Each applied batch is one atomic Lance commit containing its rows **and** the
`_meta` row.

- **Offset guard:** an applier commits a batch only if the table's
  `_meta.applied_offset` equals the offset it expects, and never applies an offset
  at or below the stored one, enforced inside the commit (Lance's optimistic
  concurrency plus a read check on `_meta`). Without it, a stale applier that lost
  leadership mid-flight could re-apply an older batch after a newer one and revert
  rows, because upsert-by-id has no ordering of its own.
- Records whose epoch is older than the partition's latest claim are dropped.
- On losing leadership the applier stops and closes its writers.
- **Durable offset:** recovery and log truncation use the `applied_offset` that is
  durable in Lance (base or flushed data), never the in-memory value.

## 10. Read path and routing

- The SDK caches the routing map (with overrides); a stale route gets a redirect and
  a map refresh.
- Agents never see partitions: they call `remember()`, `recall()`, `supersede()`,
  `forget()`, or send AQL directly (MCP and gRPC as today).
- Agent logic can run colocated on the sequencer node (actor per partition), making
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

**Consistency modes** (replacing v0.2's read strategies):
- **Read-your-writes:** wait until the applied offset reaches the commit offset, or,
  on the sequencer node, read through the pending overlay with no wait.
- **Bounded staleness** (default): whatever is applied.
- **Indexed-up-to:** indexes that don't cover fresh rows report "indexed up to
  offset N".

## 11. Replication and log

- **Log trait** with two implementations: a local WAL (embedded and single-node
  profiles) and openraft (cluster). The sequencer and applier don't change.
- **openraft** runs in-process, one instance per Raft group (§8). Pin the version
  deliberately (0.9 stable vs 0.10 alpha) and prototype on one.
- **Log storage** behind openraft's `RaftLogStorage`: `raft-engine`, or segment
  files with offset indexes.
- **WAL writer** per disk, outside tokio: batches over a channel, `pwrite` +
  `fdatasync`, ack after fsync (io_uring only if profiling demands it).
- **Iggy and Redpanda are not on the commit path.** Outbound change feeds
  (analytics, webhooks, waiting agents) may use Iggy, or Redis/Dragonfly Streams for
  lossy fan-out, as consumers of the committed log.

**Epochs and fencing**
- Inside a group, Raft's term fences proposals: only the current leader commits.
- A partition's `epoch` changes when it moves between replica sets:
  `claim(partition, epoch=N+1, group)` is committed in the new group's log, and the
  first claim wins.
- Every data record carries its writer's epoch, and the offset guard (§9.3) protects
  the table regardless.
- Leases trigger failover; safety comes from Raft and the log claim, not the lease.

**Retention.** Sealed segments are **archived to object storage first**; the local
log is then truncated freely. A lagging or hibernated partition replays from the
archive, so one stuck partition never pins the group's local log.

## 12. Failure, recovery and rebalancing

Recovering or moving a partition is one operation: become leader (or claim the
epoch), open state from object storage, replay the log tail. No data is copied
between nodes. (v0.2's "cross-VM migration" is this operation.)

**Recovery**
1. Raft elects a new group leader (or the control plane hands the partition to
   another replica set and it commits a claim).
2. Open the partition's Lance table; read the durable `_meta.applied_offset`.
3. Replay committed records after it, from the local log or the archive
   (upserts + offset guard make overlap harmless).
4. Rebuild the pending overlay and the `WORKING` / `TOOLS` hot tier from committed
   records, then serve.

| Failure | Effect | Handling |
|---|---|---|
| Group leader crashes | Its partitions pause until election | Raft election (sub-second) + tail replay |
| Crash mid-apply | Rows and `_meta` land together or not at all | Replay from durable `applied_offset` |
| Stale applier after losing leadership | Its commits are rejected | Offset guard in the commit |
| Minority of replica set lost | None for commits | Replace the node |
| Majority of replica set lost | Writes stop for its partitions | Restore from object storage + archived segments; possible loss of the unarchived tail |
| Object storage slow or throttled | Flush and compaction lag | Raft log stays durable locally; backpressure on compaction |

**Rebalancing:** leadership transfer inside a set; partition handoff between sets
for hot spots; snapshot hot partitions more often to keep failover short.

## 13. Deployment profiles (one engine)

| Profile | Log | Storage | `CLAWDB_REQUIREMENTS` target | Use |
|---|---|---|---|---|
| **Playground** | None | In-memory | (2) WASM playground | Browser demo; parser + in-memory engine, no persistence |
| **Embedded** | Local WAL file | Local directory or object storage | (1) | Inside FlowR or an agent process; replaces the §23 sidecar; v0.2 "embedded" |
| **Single-node server** | Local WAL | Local or object storage | (1) ClawDB | `adb-server` (gRPC + MCP); dev, Claude Desktop, small installs; v0.2 "hybrid" via archived segments |
| **Cluster** | openraft replica sets | Object storage | (3) Full ADB | Hosted service, dedicated tier; v0.2 "serverless" |

Embedded, single-node and cluster run the same sequencer, applier and table schema;
only the log implementation and placement change (a single node owns all 4,096
partitions, most of them hibernated). The single-node build exercises the whole
write path.

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

**Hibernate:** stop writes, drain batch and applier, flush so the durable
`applied_offset` is current, close writers, release, drop caches.
**Wake:** assign, open the table, read `_meta`, replay the (usually empty) tail,
prewarm hot indexes and the `TOOLS` cache (v0.2's session loading), serve. Cold
start scales with the unflushed tail and index warm-up, not total data.

**Object-storage budget:** with the log as the WAL, flushing is a checkpoint (e.g.
4,096 partitions flushing every 60 s ≈ 70 PUT/s, plus compaction). Partitions are
spread across buckets to stay inside per-bucket limits (e.g. Hetzner 750 req/s).

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
  `embedding`) under bucket server-side encryption per tenant prefix; non-searched
  payload fields inside `value` stay encrypted with the tenant key.
- **Forget** (AQL `FORGET`) = destroy the key where the whole scope goes; delete the
  rows; compact; run old-version cleanup with a **short version-retention window**,
  so old Lance versions and flushed data are physically gone within a stated forget
  SLA.
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

- FlowR uses ADB for **agent memory** in every profile: embedded in its process, or
  as a service. Today FlowR's `call:a3t:agent` skips memory recall/store ("ClawDB
  integration pending"); wiring it is a FlowR task against the single-node profile.
- FlowR's own control-plane data (templates, agents, canvases, deployments) and run
  state are **not** in the v1 memory schema. Clustering FlowR needs a shared queue,
  shared state and pub/sub for live sessions. Whether that state becomes a second
  ADB table family on the same log/sequencer engine is an open decision (§19), taken
  after the single-node milestone.

## 16. Crate plan

| Crate | Status | Role in v1.0 |
|---|---|---|
| `aql-parser`, `aql-planner`, `adb-executor` | Keep | AQL front end and operation tree (unchanged contract) |
| `adb-core` | Keep, extend | Types; add log record, effects, offsets, epochs |
| `adb-mcp`, `adb-server` | Keep | MCP and gRPC/HTTP surfaces |
| `adb-backends` | Repurpose | In-memory engines become the hot tier and memtable option B; the `LinkStore` gains persistence via edges |
| `clawdb` | Fold in | Its Lance code seeds the applier; its table-per-memory-type layout and in-memory links are replaced |
| `clawdb-wasm` | Keep | Playground profile |
| **`adb-log`** (new) | New | Log trait; local WAL; openraft implementation |
| **`adb-sequencer`** (new) | New | Validation, overlay, group commit, hot tier |
| **`adb-store`** (new) | New | Storage trait; Lance applier, offset guard, maintenance (SlateDB fallback behind it) |
| **`adb-cluster`** (new) | New | Control-plane client, routing map, leases, hibernation |

## 17. Spike (about 2 weeks, single node): go/no-go on memtable option A

- [ ] **0. Find a released Lance crate that exposes the MemWAL writer API**, and pin
      it. If none does, go straight to option B.
- [ ] `merge_insert` p99 for 1–50-row batches (A: LSM path; B: plain).
- [ ] Point-read and `subject_id` scan p99 across memtable, flushed data and base.
- [ ] Atomicity of one commit (rows + `_meta`) across a process kill.
- [ ] Offset guard: two appliers racing on one table never revert a row.
- [ ] Which indexes `maintained_indexes` covers (vector, FTS, scalar) — A only.
- [ ] PUTs per active table per second, and whether MemWAL durability can be relaxed
      — A only.
- [ ] Open / close / reopen cost per table, at 4,096 tables per bucket set.
- [ ] Conditional writes (`If-None-Match` / `If-Match`) on Hetzner object storage —
      **blocker** for Lance commits.

## 18. Delivery path

1. **Spike** (§17) → choose memtable A or B.
2. **Single node / embedded:** sequencer, local WAL, applier, memory schema,
   **persisted links**, `WORKING` / `TOOLS` hot tier; the AQL conformance suite keeps
   passing (116/116 today).
3. **Recall:** vector / FTS / scalar indexes, brute-force threshold, hybrid ranking.
4. **Replication:** openraft groups, epochs, offset guard, archiver, recovery.
5. **Control plane:** routing map + overrides, leases, hibernation and wake, tiers.
6. **Multi-node:** leadership balancing, cross-set handoff, bucket spreading.

**Day-one rules**
- Every mutation goes through the ADB log; the table is a materialized view of it.
- AQL is the only public contract; Lance and the log sit behind traits.
- ADB-owned ULIDs for every record.
- Log payloads encrypted with tenant keys (§14.3).
- Conformance and fault-injection tests from the first milestone: crash mid-apply,
  stale applier, replay, forget.

## 19. Open questions and risks

| Item | Type | Why it matters | Next step |
|---|---|---|---|
| MemWAL writer API availability | **Risk** | Not in Lance 2.0.0; option A depends on it | Spike step 0; fall back to B |
| Atomic commit with `_meta` | Unknown | Crash-safe apply depends on it | Kill mid-apply in the spike |
| Offset guard inside a Lance commit | Unknown | Prevents stale-applier reverts | Race test in the spike |
| Object-store conditional writes (Hetzner) | **Blocker** | Lance commits depend on compare-and-swap | Verify first |
| openraft 0.9 vs 0.10 | Decision | API churn vs features | Prototype on one, pin it |
| Raft groups per replica set (8–16) | Decision | Balance vs heartbeat cost | Benchmark leadership spread |
| Replicated group-commit latency | Unknown | Sets the `remember()` budget | 3 nodes, 1–4 KB txns, 2–5 ms window, fsync on |
| Partition count (4,096) | Decision | Hard to change later | Confirm against target node count and largest tenant |
| Hetzner 750 req/s per bucket | Risk | Caps active partitions per bucket | Bucket per partition group; checkpoint-style flushing |
| Encryption vs search (§14.3) | Decision | Plaintext needed for FTS/constraints | Adopt split rule; confirm SSE per prefix |
| Cross-partition operations | Decision | Shared scopes on other partitions | Sagas/outbox first; 2PC only on demand |
| FlowR state as an ADB table family | Decision | Basis for FlowR cluster mode | Decide after single-node milestone |
| **ADB licence** | Decision | README says ELv2; workspace `Cargo.toml` says Apache-2.0; no LICENSE file. ELv2 suits a hosted product (it stops others offering ADB as a managed service), but it conflicts with the "ADB-CE" open-source naming used in FlowR | Pick one, add LICENSE, align `Cargo.toml` and README |
| Crowded memory market | Market | Differentiation | Lead with cost model, temporal/provenance memory, verifiable forget |

## 20. Changes from the 5 Oct cluster draft

1. **Who sequences a partition:** several Raft groups per replica set; the group
   leader is the sequencer; rebalancing is leadership transfer (§8).
2. **Storage-level fencing:** offset-guarded apply prevents a stale applier from
   reverting rows (§9.3).
3. **Durable offset:** recovery and truncation use the offset durable in Lance
   (§9.3).
4. **Log retention:** archive sealed segments first, then truncate locally (§11).
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

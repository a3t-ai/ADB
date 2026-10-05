# ADB (Agent Database) — Architecture

> **Version:** 1.1 (final for build) — adds the Raft commit design from the updated 5 Oct spec
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

Commits have two modes behind one log trait: **Raft** (about 1 ms, replicated on
local NVMe, for the low-latency and enterprise tier) and **object-store commit**
(10–100 ms, stateless nodes, a candidate for a cheap shared tier).

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

**Cores:** partitions are assigned to cores (many per core) and each core runs a
single-threaded executor for its sequencers, Dragonfly-style. A partition is never
touched by two cores.

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
  ownership mid-flight could re-apply an older batch after a newer one and revert
  rows, because upsert-by-id has no ordering of its own.
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
  Verify that the pinned openraft version reports the leader's flush asynchronously.
- Group commit: one `fdatasync` per 2–5 ms batch, shared by every partition whose
  batch is in it.
- Clients are acked at commit; visibility comes at apply (read-your-writes waits for
  `applied_offset`, §10).
- **No diskless (no-fsync) mode.** A node restarting with an empty log but a vote can
  elect a leader missing committed entries. If such a mode is ever used, restarted
  nodes rejoin as non-voting learners until caught up.
- MemWAL (option A) is a second durability layer for the same data, so its flushing
  can be lazy; losing unflushed Lance state only lengthens replay.
- openraft is version-pinned deliberately (0.9 stable vs 0.10 alpha); log storage
  sits behind `RaftLogStorage` (`raft-engine`, or segment files plus offset indexes).
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
  is that small map, a pointer; table data never travels between nodes. A lagging or
  new follower installs the snapshot, then streams the log tail.
- **Who applies.** Only a partition's owner materializes it into Lance; other members
  hold the log, so each Lance table has exactly one writer.
- **Reads.** The owner serves reads of applied state while its lease is valid and its
  claim is current. Linearizable reads, when requested, confirm ownership with a
  ReadIndex round through the leader. Followers serve nothing directly.
- **Membership.** Replacing a node: add it as a learner, let it catch up from snapshot
  plus tail, promote it (joint consensus), remove the old node. The control plane
  drives every step; a replica set never runs with fewer than three voters
  deliberately.
- **Bootstrap.** The control plane creates a replica set, initializes the group with
  three voters, and assigns initial owners by committing claims; the routing map
  publishes owners, and SDKs follow redirects.
- **Timing.** Heartbeat and election timeouts are set for in-region networks
  (heartbeat ≈ 50 ms, election 300–600 ms as the starting point), with heartbeats
  batched per node pair. After an election the control plane may transfer leadership
  back to keep the leader's load predictable.
- **Testing.** Jepsen-style fault injection (network partitions, clock skew, leader
  and owner kills, disk-full, slow fsync) is a gate for the cluster milestone, on
  top of the single-node fault tests.

### 11.7 Outbound streams

Change feeds to analytics, webhooks or waiting agents may use Iggy (or
Redis/Dragonfly Streams for lossy fan-out). They consume the committed log; they are
never part of commit.

## 12. Failure, recovery and rebalancing

Recovering or moving a partition is one operation: claim the epoch, open state from
object storage, replay the log tail. No data is copied between nodes. (v0.2's
"cross-VM migration" is this operation.)

**Recovery sequence**
1. The owner's lease expires; the control plane picks a new owner within the replica
   set.
2. The new owner appends `claim(partition, epoch=N+1)` through Raft.
3. It opens the partition's Lance table (option A: MemWAL replays its own WAL into
   memtables).
4. It reads the durable `_meta.applied_offset`, then replays committed records after
   it from the local log or the archive (upserts plus the offset guard make overlap
   harmless).
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

**Rebalancing**
- The control plane moves partition ownership by load (hot partitions, node CPU,
  memory); data stays in object storage.
- Snapshot hot partitions more often to keep replay, and therefore failover, short.
- Moving a partition to another replica set is the same claim-open-replay sequence in
  the target set.

## 13. Deployment profiles (one engine)

| Profile | Log | Storage | `CLAWDB_REQUIREMENTS` target | Use |
|---|---|---|---|---|
| **Playground** | None | In-memory | (2) WASM playground | Browser demo; parser + in-memory engine, no persistence |
| **Embedded** | Local WAL file | Local directory or object storage | (1) | Inside FlowR or an agent process; replaces the §23 sidecar; v0.2 "embedded" |
| **Single-node server** | Local WAL | Local or object storage | (1) ClawDB | `adb-server` (gRPC + MCP); dev, Claude Desktop, small installs; v0.2 "hybrid" via archived segments |
| **Cluster — Raft tier** | openraft replica sets on NVMe | Object storage | (3) Full ADB | Low-latency and enterprise tier, dedicated tier; v0.2 "serverless" |
| **Cluster — object-store tier** | Object-store log, conditional writes | Object storage | (3) Full ADB | Cheap shared tier; stateless nodes, 10–100 ms commits |

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
| **`adb-log`** (new) | New | Log trait; local WAL, openraft and object-store implementations; WAL writer thread; archiver |
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
4. **Replication:** openraft replica set (one group), owner forwarding, epochs and
   claims, offset guard, archiver, recovery, membership, Jepsen-style tests; the
   object-store commit mode alongside.
5. **Control plane:** routing map + overrides, leases, hibernation and wake, tiers.
6. **Multi-node:** ownership balancing, cross-set handoff, bucket spreading.

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
| MemWAL writer API availability | **Risk** | Not in Lance 2.0.0; option A depends on it | Spike step 0; fall back to B |
| Atomic commit with `_meta` | Unknown | Crash-safe apply depends on it | Kill mid-apply in the spike |
| Offset guard inside a Lance commit | Unknown | Prevents stale-applier reverts | Race test in the spike |
| Object-store conditional writes (Hetzner) | ☑ Resolved (B1) | Create-only is atomic (Lance's commit path works); `If-Match` replace is broken | Design rule: create-only objects; open issue B1a (three gateways reject the key) |
| openraft 0.9 vs 0.10 | Decision | API churn vs features | Prototype on one, pin it |
| One Raft group per replica set | Decision | One leader carries all of the set's appends and replication | Benchmark leader throughput; multi-Raft only if it saturates |
| Commit mode per tier | Decision | Raft (~1 ms, NVMe replica sets) vs object-store commit (10–100 ms, stateless nodes) | Choose per tier; both behind the log trait |
| openraft async leader flush | Unknown | Parallel leader fsync needs append completion reported asynchronously | Verify on the pinned version |
| Replicated group-commit latency | Unknown | Sets the `remember()` budget | 3 nodes, 1–4 KB txns, 2–5 ms window, fsync on |
| Partition count (4,096) | Decision | Hard to change later | Confirm against target node count and largest tenant |
| Hetzner 750 req/s per bucket | Risk | Caps active partitions per bucket | Bucket per partition group; checkpoint-style flushing |
| Encryption vs search (§14.3) | Decision | Plaintext needed for FTS/constraints | Adopt split rule; confirm SSE per prefix |
| Cross-partition operations | Decision | Shared scopes on other partitions | Sagas/outbox first; 2PC only on demand |
| FlowR state as an ADB table family | Decision | Basis for FlowR cluster mode | Decide after single-node milestone |
| **ADB licence** | Decision | README says ELv2; workspace `Cargo.toml` says Apache-2.0; no LICENSE file. ELv2 suits a hosted product (it stops others offering ADB as a managed service), but it conflicts with the "ADB-CE" open-source naming used in FlowR | Pick one, add LICENSE, align `Cargo.toml` and README |
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

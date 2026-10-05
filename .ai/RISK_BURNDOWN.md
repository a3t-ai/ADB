# ADB — Known Unknowns and Risks: Burn-down

> **Date:** 5 Oct 2026 · tracks `ARCHITECTURE.md` v1.1 (§17 spike, §19 risks)
> **Rule:** work top to bottom. Each gate can change the architecture, so nothing in
> a later gate is built until the gates before it are closed. Targets marked
> *(proposed)* are starting points — confirm or adjust them before the gate runs.

Status key: ☐ open · ◐ in progress · ☑ closed (record the result and date) · ✕ failed (record the fallback taken)

---

## Gate 0 — Blockers (days, before any build)

These can each invalidate a core decision on their own, and they are cheap.

| ID | Unknown / risk | How to resolve | Done when | If it fails | Unblocks |
|---|---|---|---|---|---|
| ☐ B1 | **Conditional writes on Hetzner object storage** (`If-None-Match` / `If-Match`). Lance commits and the object-store commit mode depend on compare-and-swap. | Script: two writers race to create/replace the same key with conditional headers; check that exactly one wins, every time, under load. | Conditional create and replace are both atomic on Hetzner. | Use a commit coordinator (a small CAS service or Postgres row lock) for Lance commits, or move buckets to a provider that supports it. Re-cost §14.1. | Every gate |
| ☐ B2 | **Lance MemWAL writer API exists in a released crate.** Lance 2.0.0 has only the MemWAL format/index metadata; the writer calls in the spec (`LsmWriteSpec`, `flush_lsm`, `close_lsm_writers`, `writerConfigDefaults`, `use_lsm`) are not in it. | Check the latest Lance/LanceDB releases and changelogs; build a 20-line program that opens an LSM writer and writes one row. | A released, pinnable version exposes the writer path. | **Memtable option B** (ADB-owned memtable). Skip G1-A items. | G1 (option A items) |
| ☐ B3 | **Licence.** README says ELv2, workspace `Cargo.toml` says Apache-2.0, no LICENSE file; FlowR calls it "ADB-CE". | Decide; add LICENSE; align `Cargo.toml`, README and FlowR naming. If there is a CE/EE split, decide which crates are open. | One licence (or a documented split) in the repo. | — | Public releases, hosted offering |
| ☐ B4 | **Dependency licences** for the "all-permissive stack" goal (openraft, raft-engine, Lance, DataFusion, usearch, tokio, …). | `cargo deny check licenses` with an allow-list of Apache-2.0 / MIT / BSD. | No copyleft or source-available crate in the dependency tree. | Replace the offending crate. | Hosted offering |

---

## Gate 1 — Storage spike, single node (~2 weeks)

Decides memtable option A vs B and proves the apply path. Run on the target object
storage (B1 must be closed).

| ID | Unknown / risk | How to resolve | Done when *(proposed targets)* | If it fails |
|---|---|---|---|---|
| ☐ S1 | **Atomic commit of rows + `_meta`** across a crash. | Kill the process at random points during apply (1,000 runs); after restart, check rows and `_meta.applied_offset` always agree. | Zero runs where they disagree. | Make `_meta` a separate idempotent step keyed by offset; re-design recovery (§9.3). |
| ☐ S2 | **Offset guard inside a Lance commit** (stale applier can't revert rows). | Two appliers race on one table with interleaved offsets; check no row ever goes back to an older value. | Zero reverts in 10k races. | Serialize applies through a per-table lock object (conditional PUT) before commit. |
| ☐ S3 | **Write latency**: `merge_insert` for 1–50-row batches — option A (LSM path) and option B (plain, batched every N s). | Benchmark both. | Apply lag p99 < 100 ms at 200 batches/s per partition. | Larger batches / longer flush interval (option B), accept more replay on recovery. |
| ☐ S4 | **Read latency across fresh and flushed data**: point read, `subject_id` scan, vector recall, FTS. | Benchmark with fresh rows in the memtable plus flushed data plus base. | Point read p99 < 5 ms; graph hop p99 < 10 ms; semantic recall p99 < 25 ms (v0.1 targets), warm cache. | Hot-tier cache in front; revisit index choice (S8). |
| ☐ S5 | **Which indexes `maintained_indexes` covers** (vector, FTS, scalar) — option A only. | Write rows, query before flush, compare with brute force. | Fresh rows visible to every index ADB relies on. | Merge memtable results in ADB (as option B does) for the uncovered index types. |
| ☐ S6 | **Object-storage request rate**: PUTs per active table per second; whether MemWAL durability can be relaxed (option A). | Measure PUTs under S3's load; try relaxed flush settings. | ≤ 0.1 PUT/s per idle-active partition plus checkpoint flushes; fits 750 req/s per bucket at the planned partitions-per-bucket. | More buckets per partition group; longer checkpoint interval. |
| ☐ S7 | **Table open / close / reopen cost** (hibernation), at 4,096 tables per bucket set. | Open, query, close all 4,096 tables in a loop; measure, including manifest reads. | Open-to-first-query p95 < 300 ms; no per-bucket throttling. | Fewer, larger tables (identity-sharded table per partition group, §19). |
| ☐ S8 | **Vector index at partition scale**: brute-force threshold, IVF-PQ vs IVF-RQ vs HNSW build time and recall. | Benchmark 10k / 50k / 500k vectors per partition, 768 and 1536 dims. | A threshold and an index type with recall@10 ≥ 0.95 and rebuild < 60 s at the largest planned partition. | Larger partitions get a dedicated index cadence; tune threshold. |
| ☐ S9 | **f16 embeddings in the log**: recall impact after rebuild. | Rebuild a table from an f16 log; compare recall with f32. | Recall@10 drop < 0.5 %. | Keep f32 in the log; re-cost log size. |
| ☐ S10 | **Two writers on one table** (MemWAL behaviour) — defence in depth. | Open two LSM writers on one table and write. | Behaviour documented (fenced, conflict, or corrupt). ADB does not depend on it either way. | Rely on epochs + offset guard (already the plan). |
| ☐ S11 | **Forget is physical**: delete + compact + old-version cleanup removes data from object storage within the SLA. | Forget a scope; list objects and versions; scan for its plaintext and vectors. | Nothing recoverable after the cleanup window; SLA number set (e.g. 24 h). | Shorter version retention; a forced-rewrite path for forget. |
| ☐ S12 | **Per-prefix server-side encryption on Hetzner** (encryption split, §14.3). | Check SSE/SSE-C support and per-prefix keys. | Per-tenant-prefix encryption at rest confirmed. | Encrypt searchable columns at the application layer with search-compatible schemes, or dedicated buckets per tenant for the sensitive tier. |

**Gate 1 decision:** memtable A or B, recorded in `ARCHITECTURE.md` §9.2 with the numbers.

---

## Gate 2 — Replication spike (3 nodes, ~2–3 weeks)

Run on the target hardware class: power-loss-protected NVMe, dedicated log disk.

| ID | Unknown / risk | How to resolve | Done when *(proposed targets)* | If it fails |
|---|---|---|---|---|
| ☐ R1 | **openraft version**: 0.9 stable vs 0.10 alpha. | Prototype the replica set on one; check API stability, membership, snapshot API. | Version pinned, with reasons recorded. | Use the other line. |
| ☐ R2 | **Async leader flush** (leader fsync in parallel with replication) on the pinned version. | Read the storage API; test that commit can complete on two followers before the leader's fsync. | Supported, or a documented workaround. | Accept serial leader fsync; re-measure R3. |
| ☐ R3 | **Replicated group-commit latency** — the `remember()` budget. | 3 nodes, 1–4 KB transactions, 2–5 ms window, fsync on. | Commit p99 < 10 ms; `remember()` end-to-end p99 < 15 ms. | Shorter window, faster disks, or a smaller replica set per region. |
| ☐ R4 | **Single-leader throughput** — one Raft group per replica set carries every append. | Ramp active partitions per set until latency or CPU breaks. | ≥ 20k txn/s per set *(proposed)*, and the number of active partitions per set that sustains it. | Multi-Raft (several groups per set) — the planned scaling lever. |
| ☐ R5 | **Owner → leader forwarding overhead.** | Measure added latency for owners that are not the leader. | < 0.5 ms p99 in-region. | Prefer placing hot partitions on the leader node; leader-affinity rebalancing. |
| ☐ R6 | **Failover RTO** for an owner (lease timeout + table open + tail replay) and for a leader (election). | Kill owners and leaders under load. | Owner failover p95 < 5 s; leader election < 1 s; no lost acknowledged writes. | Shorter lease; snapshot hot partitions more often. |
| ☐ R7 | **Snapshot as pointer** works with openraft's snapshot interface. | Implement install-snapshot as the per-partition map; bring a wiped follower back. | Follower rejoins from snapshot + tail without copying table data. | Stream archived segments instead of a snapshot for catch-up. |
| ☐ R8 | **Membership change** (learner → voter → remove old) under load. | Replace a node during sustained writes. | No commit stall > 1 s; no lost writes. | Schedule replacements only in low-load windows. |
| ☐ R9 | **Retention and archive** — laggard exception. | Stall one partition's applier; confirm the log still truncates after archiving and the partition replays from the archive. | Local log stays within its size budget (a few GB). | Pause commits for the stuck partition only. |
| ☐ R10 | **Correctness under faults** — Jepsen-style harness: network partitions, clock skew, leader/owner kills, disk-full, slow fsync. | Build the harness; run on every replication change. | Linearizability checker passes for single-partition transactions; no lost or duplicated acknowledged writes. | Fix before the cluster milestone; this is a gate, not a report. |

---

## Gate 3 — Object-store commit mode (~1 week, can run in parallel with Gate 2)

| ID | Unknown / risk | How to resolve | Done when *(proposed targets)* | If it fails |
|---|---|---|---|---|
| ☐ O1 | **Commit latency and rate** on Hetzner with conditional-write fencing. | Group-commit batches to a log object per partition; measure. | Commit p99 < 150 ms; fencing never admits two writers. | Drop the mode; shared tier runs on Raft at higher density. |
| ☐ O2 | **Active partitions per bucket** under 750 req/s, including multi-partition log objects. | Ramp partitions; try shared log objects. | A documented density (e.g. ≥ 200 active partitions per bucket with shared log objects). | More buckets per tier, or Raft only. |
| ☐ O3 | **Tier economics**: cost per active and idle tenant, Raft tier vs object-store tier. | Model with the G1–G3 numbers. | Decision recorded per tier (§19 "commit mode per tier"). | — |

---

## Gate 4 — Design decisions (paper, close before the code that needs them)

| ID | Decision | Needed by | How to close |
|---|---|---|---|
| ☐ D1 | **Partition count (4,096)** — hard to change later. | Before cluster milestone | Size against target node count, partitions per core, largest tenant, and S7 / R4 results. |
| ☐ D2 | **Temporal model**: valid-time vs transaction-time, supersession, decay, TTL. | Single-node milestone | Write the schema per `kind`; map AQL `UPDATE`, `FORGET`, `WINDOW`, `TTL`. |
| ☐ D3 | **Schema per kind**, nullable columns, embedding dimension per tenant model. | Single-node milestone | Concrete Arrow schema + migration rules when a tenant changes embedding model. |
| ☐ D4 | **Hybrid recall ranking** (vector + FTS + recency + link weight). | Recall milestone | Ranking formula + evaluation set. |
| ☐ D5 | **Compaction / optimize / index-rebuild policy** per partition. | Recall milestone | Policy from S3/S6/S8 numbers. |
| ☐ D6 | **Cross-partition operations** for shared scopes (sagas / outbox; 2PC only on demand). | Before shared scopes ship | Pick the AQL operations that may span partitions and their saga shape. |
| ☐ D7 | **Encryption split** (log/archives encrypted; searchable columns plaintext under per-prefix SSE). | Single-node milestone | Confirm with S12; document the threat model. |
| ☐ D8 | **FlowR state as an ADB table family** (basis for FlowR cluster mode). | After single-node milestone | Map FlowR's storage interface onto a second schema; decide yes/no. |
| ☐ D9 | **AQL changes** the engine needs (scope declaration, affinity keys in schema, `volatile` working memory, consistency hints). | Single-node milestone | Propose for AQL v0.6 in the AQL repo. |

---

## Known gaps (not unknowns — scheduled work)

| ID | Gap | Where it's fixed |
|---|---|---|
| ☐ K1 | **Links are lost on restart** in both implementations (`adb-backends` DashMap, `clawdb` `Vec<Link>`). | Single-node milestone: persisted `edge` rows. |
| ☐ K2 | FlowR's `call:a3t:agent` skips memory recall/store ("ClawDB integration pending"). | FlowR task against the single-node profile. |
| ☐ K3 | Conformance suite must keep passing (116/116 today) through the engine change. | Every milestone. |
| ☐ K4 | Migration path from current `clawdb` / `adb-backends` data to the partitioned table. | Single-node milestone. |

---

## Gate 5 — Product and market

| ID | Risk | How to resolve |
|---|---|---|
| ☐ M1 | **Crowded memory layer.** | Positioning against current memory products, led by cost model (O3), temporal/provenance memory (D2) and verifiable forget (S11). |
| ☐ M2 | **Per-tenant cost claim** holds only with pooled compute (§14.1). | Publish the cost model from O3 with honest wording. |

---

## Summary: what each gate can change

| Gate | Can change |
|---|---|
| 0 | Object-storage provider, memtable option, licensing |
| 1 | Memtable A vs B, table-per-partition layout, encryption split, forget SLA |
| 2 | Raft topology (one group vs multi-Raft), hardware, latency budget |
| 3 | Whether a cheap object-store tier exists |
| 4 | Schema, partition count, cross-partition semantics, FlowR scope |
| 5 | Positioning and pricing |

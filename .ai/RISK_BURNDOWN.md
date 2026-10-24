# ADB — Known Unknowns and Risks: Burn-down

> **Date:** 5 Oct 2026, updated 6 Oct 2026 · tracks `ARCHITECTURE.md` v1.1 (§17 spike, §19 risks)
> **Rule:** work top to bottom. Each gate can change the architecture, so nothing in
> a later gate is built until the gates before it are closed. Targets marked
> *(proposed)* are starting points — confirm or adjust them before the gate runs.

Status key: ☐ open · ◐ in progress · ☑ closed (record the result and date) · ✕ failed (record the fallback taken)

---

## Gate 0 — Blockers (days, before any build)

These can each invalidate a core decision on their own, and they are cheap.

| ID | Unknown / risk | How to resolve | Done when | If it fails | Unblocks |
|---|---|---|---|---|---|
| ☑ B1 | **Conditional writes on Hetzner object storage.** *Closed 5 Oct 2026 — see [`spikes/B1_hetzner_conditional_writes.md`](spikes/B1_hetzner_conditional_writes.md).* | Raw S3 race (100 × 16) and `object_store` race (50 × 16). | **Create-only (`If-None-Match: *`) is atomic: 150/150 rounds, one winner, 0 violations — Lance's commit path works.** `If-Match` replace is broken (spurious 412 with quoted ETags) → design rule: create-only objects everywhere. | — | Every gate |
| ☑ B1a | **Three Hetzner gateways rejected the access key** (~15 % of requests got 403). *Closed 5 Oct 2026: re-check ~1 h later, 120/120 requests OK across all gateways — key propagation delay.* | Re-ran the gateway check. | 0 × 403. | — | G1 benchmarks |
| ☑ B1d | **fsn1 bucket `adb2` (ADB spike bucket from 6 Oct).** Create-only race re-run: 100/100, 0 violations. But 2 of 20 gateways answer `404 NoSuchBucket` for every request (new-bucket propagation, like B1a). | Re-probe until every gateway answers 200. *Closed 6 Oct 2026: cleared within ~10 min (0/150 at 10:02).* | 0 × 404 over 150 requests. | Hetzner ticket with the gateway IDs. | Every S-item run on Hetzner |
| — B1c | **Rotate the spike access key** (shared in a chat session). *9 Oct: not needed while in dev mode (user's call); rotate before production.* | Create a new key, update env files, delete the old key. | Old key revoked. | — | — |
| ☑ B2 | **Lance MemWAL writer API exists in a released crate.** *Closed 6 Oct 2026 — see [`spikes/B2_S1_S2_lance12_memwal_and_apply.md`](spikes/B2_S1_S2_lance12_memwal_and_apply.md): **Lance 12.0.0** ships it as `DatasetMemWalExt::initialize_mem_wal` / `mem_wal_writer` → `ShardWriter` (`put`, `delete`, `force_seal_active`, `close`), read via `LsmScanner`. The spec's names don't exist.* | Check the latest Lance/LanceDB releases and changelogs; build a 20-line program that opens an LSM writer and writes one row. | A released, pinnable version exposes the writer path. | **Memtable option B** (ADB-owned memtable). Skip G1-A items. | G1 (option A items) |
| ☑ B3 | **Licence.** README says ELv2, workspace `Cargo.toml` says Apache-2.0, no LICENSE file; FlowR calls it "ADB-CE". *7 Oct: **ADB is SaaS-only (part of A3T) plus a local dev build** — the service is proprietary; only the dev build needs a licence: ELv2 or a proprietary dev licence (both stop others hosting it). Drop the "ADB-CE" naming. Remaining: pick the dev-build licence, add LICENSE, align `Cargo.toml` and README.* *Closed 9 Oct: **same business support licence as FlowR.** LICENSE text to add once FlowR's exists (FlowR's `Cargo.toml` says Apache-2.0, no LICENSE file).* | Decide; add LICENSE; align `Cargo.toml`, README and FlowR naming. If there is a CE/EE split, decide which crates are open. | One licence (or a documented split) in the repo. | — | Public releases, hosted offering |
| ☑ B4 | **Dependency licences** for the "all-permissive stack" goal (openraft, raft-engine, Lance, DataFusion, usearch, tokio, …). | `cargo deny check licenses` with an allow-list of Apache-2.0 / MIT / BSD. | No copyleft or source-available crate in the dependency tree. *Closed 6 Oct 2026 (desk, `cargo metadata`, 800 crates): none. Only 3 MPL-2.0 crates (`colored`, `option-ext`, `smartstring`), file-level, used unmodified — allow with a comment. Add `cargo deny` to CI.* | Replace the offending crate. | Hosted offering |

---

## Gate 1 — Storage spike, single node (~2 weeks)

Decides memtable option A vs B and proves the apply path. Run on the target object
storage (B1 must be closed).

| ID | Unknown / risk | How to resolve | Done when *(proposed targets)* | If it fails |
|---|---|---|---|---|
| ☑ S1 | **Atomic commit of rows + `_meta`** across a crash. *Closed 6 Oct 2026: fsn1, 1,000 SIGKILLs over a 30–3,030 ms window, 3,191 batches committed, 0 disagreements, 0 open failures (local: 500 kills, 0).* | Kill the process at random points during apply (1,000 runs); after restart, check rows and `_meta.applied_offset` always agree. | Zero runs where they disagree. | Make `_meta` a separate idempotent step keyed by offset; re-design recovery (§9.3). |
| ☑ S2 | **Offset guard inside a Lance commit** (stale applier can't revert rows). *6 Oct 2026 — [`spikes/B2_S1_S2_lance12_memwal_and_apply.md`](spikes/B2_S1_S2_lance12_memwal_and_apply.md): **§9.3's guard as written fails** — Lance's default conflict retries re-run a stale `merge_insert` on newer data and commit it (`_meta` 5 → 1, rows reverted). **Fix, verified (0 reverts across every version):** `conflict_retries(0)` (commit becomes a CAS on `_meta`) **plus** `WhenMatched::UpdateIf("source.log_offset > target.log_offset")`; deletes as tombstone rows. Re-verified on fsn1 (`plain`: 15 bad versions; `noretry`/`guarded`: 0). New unknown: apply rejections under maintenance commits (→ S3/D5).* | Two appliers race on one table with interleaved offsets; check no row ever goes back to an older value. | Zero reverts in 10k races. | Serialize applies through a per-table lock object (conditional PUT) before commit. |
| ☑ S3 | **Write latency**: `merge_insert` for 1–50-row batches — option A (LSM path) and option B (plain, batched every N s). *First fsn1 data, 6 Oct: one `merge_insert` commit p50 306 ms / p99 3.2 s (≈ 3 commits/s per table, so option B must batch); durable 1-row MemWAL put p50 21 ms / p99 284 ms.* *Measured 6 Oct ([`spikes/G1_memtable_A_vs_B.md`](spikes/G1_memtable_A_vs_B.md)): B `append` commit p50 255 ms / lag p50 1.0 s; B `merge_insert` p50 975 ms and 37 MiB read per commit; A put 0.16 ms (id index only) vs 12.9 ms (all 4 indexes → 80 puts/s).* | Benchmark both. | Apply lag p99 < 100 ms at 200 batches/s per partition. | Larger batches / longer flush interval (option B), accept more replay on recovery. |
| ✕ S4 | **Read latency across fresh and flushed data**: point read, `subject_id` scan, vector recall, FTS. *6 Oct ([`spikes/G1_memtable_A_vs_B.md`](spikes/G1_memtable_A_vs_B.md)): fresh rows 1.6 ms (A memtable); anything on object storage 55–100 ms p50, ~1 s p99 → targets missed. **Fallback taken: local hot-tier cache in front of Lance data is required.** B with unindexed fragments: 1.7 s and 135 MiB per query.* | Benchmark with fresh rows in the memtable plus flushed data plus base. | Point read p99 < 5 ms; graph hop p99 < 10 ms; semantic recall p99 < 25 ms (v0.1 targets), warm cache. | Hot-tier cache in front; revisit index choice (S8). |
| ☑ S5 | **Which indexes `maintained_indexes` covers** *(desk, 6 Oct: BTree, HNSW (f32 only), FTS; no bitmap — fresh-row recall vs brute force still to measure)* (vector, FTS, scalar) — option A only. *Measured 6 Oct: just-written rows visible to point, scan, vector and FTS queries via `LsmScanner` (30/30); vector/FTS over the memtable work without maintained indexes (brute force).* | Write rows, query before flush, compare with brute force. | Fresh rows visible to every index ADB relies on. | Merge memtable results in ADB (as option B does) for the uncovered index types. |
| ☑ S6 | **Object-storage request rate**: PUTs per active table per second; whether MemWAL durability can be relaxed (option A). *(It can: `ShardWriterConfig::durable_write(false)` + `max_wal_flush_interval` — PUT rate still to measure.)* *6 Oct: ~2–3 PUT/s per busy partition at 1 s flushes (both options). Longer WAL flush interval (Raft is the WAL) still to measure.* *Closed 6 Oct ([`spikes/G1b_S6_S8_S9_S11.md`](spikes/G1b_S6_S8_S9_S11.md)): WAL flush 30 s → 0.8 writes/s per busy partition (floor = generation flushes), ~0.03/s lightly used.* | Measure PUTs under S3's load; try relaxed flush settings. | ≤ 0.1 PUT/s per idle-active partition plus checkpoint flushes; fits 750 req/s per bucket at the planned partitions-per-bucket. | More buckets per partition group; longer checkpoint interval. |
| ✕ S7 | **Table open / close / reopen cost** (hibernation), at 4,096 tables per bucket set. *6 Oct (256 tables): ~14 requests per open + query; tops out at ~45 tables/s ≈ 620 req/s (bucket limit); p50 304 ms, p90 398 ms at concurrency 16. Fallbacks: many buckets (D1), keep hot partitions open, local manifest/index-metadata cache.* | Open, query, close all 4,096 tables in a loop; measure, including manifest reads. | Open-to-first-query p95 < 300 ms; no per-bucket throttling. | Fewer, larger tables (identity-sharded table per partition group, §19). |
| ☑ S8 | **Vector index at partition scale**: brute-force threshold, IVF-PQ vs IVF-RQ vs HNSW build time and recall. *Closed 6 Oct: **IVF_RQ + refine** — build 40 s (500k×768) / 65 s (500k×1536, borderline), recall@10 0.97–1.00. Brute-force threshold ≈ 10k vectors (23 ms), not 50k.* | Benchmark 10k / 50k / 500k vectors per partition, 768 and 1536 dims. | A threshold and an index type with recall@10 ≥ 0.95 and rebuild < 60 s at the largest planned partition. | Larger partitions get a dedicated index cadence; tune threshold. |
| ☑ S9 | **f16 embeddings in the log**: recall impact after rebuild. *Closed 6 Oct: f16 recall@10 0.995–1.000 (≤ 0.5 % drop). Keep f16 in the log.* | Rebuild a table from an f16 log; compare recall with f32. | Recall@10 drop < 0.5 %. | Keep f32 in the log; re-cost log size. |
| ☑ S10 | **Two writers on one table** (MemWAL behaviour) — defence in depth. *Closed 6 Oct 2026: **fenced.** Writer 2 takes epoch N+1 and replays writer 1's durable rows; writer 1's next `put` fails `Writer fenced (peer claimed epoch)`.* | Open two LSM writers on one table and write. | Behaviour documented (fenced, conflict, or corrupt). ADB does not depend on it either way. | Rely on epochs + offset guard (already the plan). |
| ✕ S11 | **Forget is physical**: delete + compact + old-version cleanup removes data from object storage within the SLA. *6 Oct: base table forgets physically (delete + compact + `cleanup_old_versions(0)`); **MemWAL WAL files and generations are never deleted by Lance** → fallback taken: ADB garbage-collects MemWAL after generation compaction.* | Forget a scope; list objects and versions; scan for its plaintext and vectors. | Nothing recoverable after the cleanup window; SLA number set (e.g. 24 h). | Shorter version retention; a forced-rewrite path for forget. |
| ☑ S12 | **Per-prefix server-side encryption on Hetzner** (encryption split, §14.3). *Closed 6 Oct: SSE-S3 not supported (400); **SSE-C supported and enforced**, one key per table via `aws_sse_customer_key_base64`. Shared partitions ⇒ per-partition keys, not per-tenant; per-tenant at-rest keys only in the dedicated tier.* | Check SSE/SSE-C support and per-prefix keys. | Per-tenant-prefix encryption at rest confirmed. | Encrypt searchable columns at the application layer with search-compatible schemes, or dedicated buckets per tenant for the sensitive tier. |

**Gate 1 decision:** memtable A or B, recorded in `ARCHITECTURE.md` §9.2 with the numbers.

**Decided (6 Oct 2026): option A** — MemWAL with `maintained_indexes` = `id` + `subject_id` BTrees only, `durable_write(false)`; fallback B with `append` + aggressive index upkeep. See [`spikes/G1_memtable_A_vs_B.md`](spikes/G1_memtable_A_vs_B.md). S8, S9, S11, S6 closed 6 Oct ([`spikes/G1b_S6_S8_S9_S11.md`](spikes/G1b_S6_S8_S9_S11.md)). **Gate 1 closed**; S4/S7 misses carried into the design (hot-tier cache, bucket spreading).

---

## Gate 2 — Replication spike (3 nodes, ~2–3 weeks)

Run on the target hardware class: power-loss-protected NVMe, dedicated log disk.

| ID | Unknown / risk | How to resolve | Done when *(proposed targets)* | If it fails |
|---|---|---|---|---|
| ☑ R1 | **openraft version**: 0.9 stable vs 0.10 alpha. *Closed 7 Oct ([`spikes/R1_R2_openraft.md`](spikes/R1_R2_openraft.md)): **pinned `openraft = "=0.10.0-alpha.36"`** — the only line with parallel leader fsync (R2). Behind a thin adapter in `adb-log`; upgrade deliberately. Fallback 0.9 if the alpha line breaks.* | Prototype the replica set on one; check API stability, membership, snapshot API. | Version pinned, with reasons recorded. | Use the other line. |
| ☑ R2 | **Async leader flush** (leader fsync in parallel with replication) on the pinned version. *(desk, 6 Oct, from source: **0.9.25 — no**, `RaftCore` awaits the leader's fsync before replicating; **0.10.0-alpha.36 — yes**, append is submitted without waiting and the leader counts toward quorum on its flush notification.)* *Closed 7 Oct by test: +20 ms on the leader's fsync only → commit p50 0.98 ms (baseline 1.06 ms); both followers slow → 21.8 ms.* | Read the storage API; test that commit can complete on two followers before the leader's fsync. | Supported, or a documented workaround. | Accept serial leader fsync; re-measure R3. |
| ☐ R3 | **Replicated group-commit latency** — the `remember()` budget. | 3 nodes, 1–4 KB transactions, 2–5 ms window, fsync on. | Commit p99 < 10 ms; `remember()` end-to-end p99 < 15 ms. | Shorter window, faster disks, or a smaller replica set per region. |
| ☐ R4 | **Single-leader throughput** — one Raft group per replica set carries every append. | Ramp active partitions per set until latency or CPU breaks. | ≥ 20k txn/s per set *(proposed)*, and the number of active partitions per set that sustains it. | Multi-Raft (several groups per set) — the planned scaling lever. |
| ◐ R5 | **Owner → leader forwarding overhead.** *7 Oct ([`spikes/R5_owner_forwarding.md`](spikes/R5_owner_forwarding.md)): +~45 µs p50 over TCP loopback (490–508 µs vs 441–464 µs direct), within noise at 16 clients; batching 8 txns/forward → 1.7–2.5× throughput. In-rack number on servers.* | Measure added latency for owners that are not the leader. | < 0.5 ms p99 in-region. | Prefer placing hot partitions on the leader node; leader-affinity rebalancing. |
| ◐ R6 | **Failover RTO** *7 Oct ([`spikes/R6_failover.md`](spikes/R6_failover.md)): owner takeover **0.4–0.55 s** on fsn1 with no periodic MemWAL WAL flush (was ~75 ms per WAL entry replayed, 3.5–13 s); leader election **465–565 ms** at 150–300 ms / 30 ms heartbeat (0.9–1.4 s at 300–600 / 50); 0 lost writes in 15 crashes. End-to-end with leases and false-election check on servers.* | Kill owners and leaders under load. | Owner failover p95 < 5 s; leader election < 1 s; no lost acknowledged writes. | Shorter lease; snapshot hot partitions more often. |
| ☑ R7 | **Snapshot as pointer** works with openraft's snapshot interface. *Closed 7 Oct ([`spikes/R7_snapshot_pointer.md`](spikes/R7_snapshot_pointer.md)): 174-byte snapshot; replacement node caught up in < 1 ms with an identical map. Rules: wiped node always returns as a new id; openraft auto-purge off, ADB purges at min durable applied; a snapshot-joined node replays lagging partitions from a peer's log or the archive.* | Implement install-snapshot as the per-partition map; bring a wiped follower back. **Then make it owner of a partition whose `applied_offset` is below the snapshot index** (desk finding, 6 Oct: its local log has no entries ≤ the snapshot, so replay must come from the archive; openraft's purge must respect §11.3). | Follower rejoins from snapshot + tail without copying table data, and serves as owner after replaying from the archive. | Stream archived segments instead of a snapshot for catch-up. |
| ☑ R8 | **Membership change** (learner → voter → remove old) under load. *Closed 7 Oct (functional, one box — [`spikes/R8_membership_under_load.md`](spikes/R8_membership_under_load.md)): 0 acknowledged writes lost replacing a follower and the leader. Removing the leader directly stalls ~1 s (election); **transferring leadership first: 12–76 ms**. Re-check on servers.* | Replace a node during sustained writes. | No commit stall > 1 s; no lost writes. | Schedule replacements only in low-load windows. |
| ☐ R9 | **Retention and archive** — laggard exception. | Stall one partition's applier; confirm the log still truncates after archiving and the partition replays from the archive. | Local log stays within its size budget (a few GB). | Pause commits for the stuck partition only. |
| ☐ R10 | **Correctness under faults** — Jepsen-style harness: network partitions, clock skew, leader/owner kills, disk-full, slow fsync. | Build the harness; run on every replication change. | Linearizability checker passes for single-partition transactions; no lost or duplicated acknowledged writes. | Fix before the cluster milestone; this is a gate, not a report. |
| ☑ R11 | **Raft + MemWAL recovery offset and stale-owner safety** (option A). *6–7 Oct ([`spikes/R11_raft_memwal_recovery.md`](spikes/R11_raft_memwal_recovery.md)): design = one `put` per log batch with `_meta`; recover `_meta` via `LsmScanner`, replay Raft from `_meta + 1`. Local: 300 kills + 300 takeovers, 0 inconsistent. fsn1: 100 takeovers + 100 kills, 0 inconsistent. Takeover on fsn1 p50 3.5 s / max 32.7 s → R6. **Found a Lance 12.0.0 bug** (replay-sealed generations lack `_pk_index`) → workaround: ADB-driven seals.* | Crash + zombie tests on target storage. | 0 inconsistent states, `_meta` never regresses. | Persist the applied offset outside MemWAL (e.g. in the Raft state machine) and replay more. |
| ◐ R12 | **Log truncation vs MemWAL GC vs archive** *7 Oct ([`spikes/R12_purge_gc_archive.md`](spikes/R12_purge_gc_archive.md)): durable offset = `durable_batch_count` mapped to log offsets — 0 claim violations in 600 kills (local + fsn1); WAL GC below `replay_after_wal_entry_position` safe (200 kills), 24 files left vs 3,813; orphan generations need GC. Generation compaction/GC, archive and purge wiring: build phase.*: openraft purge must not pass the durable `_meta` offset (min across partitions) except for archived segments; MemWAL GC (S11) must not delete WAL entries a replay still needs; how to observe the durable offset under `durable_write(false)`. | Implement purge policy + MemWAL GC; crash during each step; check replay always finds its entries. | No replay ever misses an entry; local log within budget. | Keep more log locally; archive before purge only. |

---

## Gate 3 — Object-store commit mode (~1 week, can run in parallel with Gate 2)

| ID | Unknown / risk | How to resolve | Done when *(proposed targets)* | If it fails |
|---|---|---|---|---|
| ☐ O1 | **Commit latency and rate** on Hetzner with conditional-write fencing. | Group-commit batches to a log object per partition; measure. | Commit p99 < 150 ms; fencing never admits two writers. | Drop the mode; shared tier runs on Raft at higher density. |
| ☐ O2 | **Active partitions per bucket** under 750 req/s, including multi-partition log objects. | Ramp partitions; try shared log objects. | A documented density (e.g. ≥ 200 active partitions per bucket with shared log objects). | More buckets per tier, or Raft only. |
| ☐ O3 | **Tier economics**: cost per active and idle tenant, Raft tier vs object-store tier. *Deferred 9 Oct: pricing is not the focus; product first.* | Model with the G1–G3 numbers. | Decision recorded per tier (§19 "commit mode per tier"). | — |

---

## Gate 4 — Design decisions (paper, close before the code that needs them)

| ID | Decision | Needed by | How to close |
|---|---|---|---|
| ☐ D1 | **Partition count (4,096)** — hard to change later. | Before cluster milestone | Size against target node count, partitions per core, largest tenant, and S7 / R4 results. |
| ☑ D2 | **Temporal model**: valid-time vs transaction-time, supersession, decay, TTL. *9 Oct decision: **facts supersede by default** — in SEMANTIC and PROCEDURAL, `UPDATE` keeps the old version (`superseded_by`, hidden from current reads); WORKING and TOOLS overwrite. Transaction time is free (log offset + commit time). "As of" queries deferred. Closed 24 Oct: [`D2_TEMPORAL.md`](D2_TEMPORAL.md) agreed (lineages, one commit clock, annotate-in-place episodes, FORGET across history, TTL as read filter + timers).* | Single-node milestone | Write the schema per `kind`; map AQL `UPDATE`, `FORGET`, `WINDOW`, `TTL`. |
| ☑ D3 | **Schema per kind**, nullable columns, embedding dimension per tenant model. *9 Oct decision: **one platform embedding model**, chosen by ADB / A3T (one vector dimension per table; tables are shared by tenants). Re-embedding on a model change rebuilds tables from the logged text. Closed 24 Oct: [`D3_SCHEMA.md`](D3_SCHEMA.md) agreed (ULID ids, plaintext `value` + declared `sealed` fields, `content` embedded, unique `key`, dimension 768, platform-table layout; re-embedding reads the table's `text`, since the log is purged below the durable offset).* | Single-node milestone | Concrete Arrow schema + migration rules when a tenant changes embedding model. |
| ☐ D4 | **Hybrid recall ranking** (vector + FTS + recency + link weight). | Recall milestone | Ranking formula + evaluation set. |
| ☐ D5 | **Compaction / optimize / index-rebuild policy** per partition. | Recall milestone | Policy from S3/S6/S8 numbers. |
| ☐ D6 | **Cross-partition operations** for shared scopes (sagas / outbox; 2PC only on demand). | Before shared scopes ship | Pick the AQL operations that may span partitions and their saga shape. |
| ☐ D7 | **Encryption split** (log/archives encrypted; searchable columns plaintext under per-prefix SSE). *24 Oct (D3 S2): all of `value` is searchable plaintext; only fields a scope declares sealed are encrypted in the table.* | Single-node milestone | Confirm with S12; document the threat model. |
| ☐ D8 | **FlowR state as an ADB table family** (basis for FlowR cluster mode). | After single-node milestone | Map FlowR's storage interface onto a second schema; decide yes/no. |
| ☑ D9 | **AQL changes** the engine needs (scope declaration, affinity keys in schema, `volatile` working memory, consistency hints). *Accepted 9 Oct: AQL v0.6 draft adds `SUBSCRIBE` / `UNSUBSCRIBE` / `ACK` / `PUBLISH` / `WATCH` / `UNWATCH` and `IN SCOPE "…"` — grammar, AST and 11 parser tests in the AQL repo (`spec/AQL_SPEC_v0.6_additions.md`); all 132 v0.5 conformance/seed queries parse to identical ASTs. Still open: affinity keys in schema, `volatile` WORKING, consistency hints.* | Single-node milestone | Propose for AQL v0.6 in the AQL repo. |

---

## Known gaps (not unknowns — scheduled work)

| ID | Gap | Where it's fixed |
|---|---|---|
| ☐ K1 | **Links are lost on restart** in both implementations (`adb-backends` DashMap, `clawdb` `Vec<Link>`). | Single-node milestone: persisted `edge` rows. |
| ☐ K2 | FlowR's `call:a3t:agent` skips memory recall/store ("ClawDB integration pending"). | FlowR task against the single-node profile. |
| ☐ K3 | Conformance suite must keep passing (116/116 today) through the engine change. | Every milestone. |
| ☐ K4 | Migration path from current `clawdb` / `adb-backends` data to the partitioned table. | Single-node milestone. |
| ☑ K5 | **Dependency upgrade to Lance 12 / Arrow 58.** The workspace carries Arrow 51 + 53 and DataFusion 37 + 44; `clawdb` is on lancedb 0.15 (Lance 0.22). Lance 12 needs Arrow 58, object_store 0.14, Rust ≥ 1.91 (workspace says 1.75) and `protoc` at build time. *Closed 24 Oct 2026: workspace on Arrow 58 / DataFusion 54 / `lance =12.0.0` (new `adb-store` crate), Rust ≥ 1.91, `chrono` unpinned (the `=0.4.38` pin was for Arrow 51). One Arrow and DataFusion version; only `object_store` 0.13 + 0.14, both from Lance. 153 Rust tests, AQL 116/116, FlowR integration 18/18.* | Before the single-node milestone; one Arrow version across the workspace. |
| ☐ K6 | **`raft-engine` is stale** (last release Apr 2024). | R1: prefer ADB's own segment files behind `RaftLogStorage`. |
| ☐ K7 | **Report the Lance MemWAL replay bug upstream** (generations sealed during WAL replay are committed without `_pk_index`; issue text ready in [`lancebug.md`](lancebug.md), standalone repro `spikes/b2/memwal/src/bin/lancebug.rs`). Drop ADB's seal workaround once fixed. | Before the single-node milestone; re-test on every Lance upgrade. |
| ☐ K8 | **Local-disk durability:** Lance's local store is `LocalFileSystem::new()` with fsync **off** — commits and MemWAL entries on local disk can be lost on power failure. Embedded / single-node profiles must pass an fsync-enabled store (`LocalFileSystem::with_fsync(true)` via `ObjectStoreParams.object_store`) or never purge the local WAL on Lance's durability. Needs a power-loss test (dm-flakey or a VM). *7 Oct: **low priority** — no embedded production profile; only the local dev build uses local disk, where losing data on a power cut is acceptable.* | Single-node milestone. |
| ◐ K9 | **ADB vendors its own copy of `aql-parser`**, already two changes behind the AQL repo (v0.5 `RECALL` without predicate, and v0.6). *9 Oct: ADB now depends on the AQL repo's `aql-parser` (git, `main`); vendored copy removed; local `.cargo/config.toml` (gitignored, example committed) patches it to `../AQL`. Workspace builds and all 142 tests pass on stable (4 stale tests fixed to match AQL v0.5 §19). **Remaining:** push the AQL repo so Docker and fresh clones resolve v0.6.* | Single-node milestone: depend on the AQL repo's `aql-parser` (path or git) so the grammar has one source. |

---

## Gate 5 — Product and market

| ID | Risk | How to resolve |
|---|---|---|
| ☐ M1 | **Crowded memory layer.** *Deferred 9 Oct: pricing is not the focus; product first.* | Positioning against current memory products, led by cost model (O3), temporal/provenance memory (D2) and verifiable forget (S11). |
| ☐ M2 | **Per-tenant cost claim** holds only with pooled compute (§14.1). *Deferred 9 Oct: pricing is not the focus; product first.* | Publish the cost model from O3 with honest wording. |

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

# Gate 1 — memtable option A (Lance MemWAL) vs B (ADB memtable + Lance appends)

> **Date:** 6 Oct 2026 · Hetzner fsn1, bucket `adb2` · Lance 12.0.0 · code:
> [`b2/memwal/src/bin/gate1.rs`](b2/memwal/src/bin/gate1.rs) · tracks S3, S4, S5, S6, S7, S12
> **Load:** 200 batches/s × 5 rows per partition (the S3 target), ADB-shaped rows
> (id, kind, subject_id, 20-word text, 768-dim f32 embedding, log_offset). Base table
> 50k rows with BTree(id), BTree(subject_id), FTS(text), IVF_PQ(embedding).
> Request counts are from Lance's `IOTracker`.

## Decision (confirmed 6 Oct 2026): option A

**Option A — Lance MemWAL — with a narrow `maintained_indexes` set: `id` and
`subject_id` BTrees only, not the vector or FTS index.** One MemWAL shard per partition
table, `durable_write(false)` (the Raft log is the WAL), epoch = partition epoch.

Why:
- **Fresh data is fast and needs no ADB code:** just-written rows are found in
  **1.6 ms** through `LsmScanner`, which also merges vector and FTS results across the
  memtable, flushed generations and base. Option B has to build that merge itself.
- **With only `id` maintained, puts cost 0.16 ms** and a shard keeps up with
  200 puts/s. Maintaining the vector (HNSW) + FTS indexes in the memtable costs
  **13 ms per put → 80 puts/s**, short of the target — that's the one thing to avoid.
  Without them, vector search over fresh rows still works (29 ms; brute force on the
  memtable).
- **Fencing comes for free** (S10), and flushed generations are already indexed, so
  there is no window where reads hit large unindexed data — the failure mode that made
  option B's reads 30× slower (below).
- **Fallback stays B**, with the append path (not `merge_insert`) and an aggressive
  index-maintenance cadence.

Risks of A: the API is new (2026) and will churn — pin `=12.0.0` and keep it behind
`adb-store`'s trait; compaction of generations into the base table is ADB's job
(`merge_insert(..).mark_sstables_as_compacted(..)`), which pays the merge cost measured
below; `subject_id` BTree put cost not yet measured separately (expected small).

## Results

### Writes (S3, S6)

| Path | Commit / put | Throughput | Apply lag (→ object storage) | Requests |
|---|---|---|---|---|
| **B** `merge_insert` every 1 s | commit p50 975 ms, p99 2.3 s | keeps up (back-to-back) | p50 2.0 s, p99 4.2 s | 25 reads/s (**603 MiB / 30 s**), 1.8 writes/s |
| **B** `merge_insert` every 5 s | commit p50 1.09 s | keeps up | p50 4.1 s, p99 7.3 s | 16 reads/s, 0.3 writes/s |
| **B** `append` every 1 s | **commit p50 255 ms**, p99 2.2 s | keeps up | **p50 1.0 s**, p99 3.7 s | 0.7 reads/s, 2.2 writes/s |
| **A**, all 4 indexes maintained | put p50 **12.9 ms**, p99 19 ms | **80 puts/s — misses 200** | WAL flush every 1 s (non-durable) | 2.5 writes/s (~3 MiB/s) |
| **A**, `id` only | put p50 **0.16 ms**, p99 0.33 ms | **200 puts/s ✓** | WAL flush every 1 s | 2.9 writes/s |

- `merge_insert` joins against the table to find matches, so its cost grows with the
  table (37 MiB read per commit at 110k rows). **Inserts of new ids must use
  `append`;** `merge_insert` only for real updates. (An append can't rewrite `_meta`,
  so with B the offset guard would move into commit metadata — another reason for A.)
- **S6:** either option needs ~2–3 PUT/s per busy partition at 1 s flushes. With Raft
  as the WAL, A's WAL flush interval can be much longer (tens of seconds) since loss
  only lengthens replay — that is the lever for the 750 req/s per-bucket budget.

### Reads (S4, S5) — p50 / p99, 30 queries each

| Query | **A** (base + flushed + memtable, SSTable cache) | **B** base table, before index maintenance | **B** after compaction + index update |
|---|---|---|---|
| Point lookup, just-written row | **1.6 ms** / 2.2 ms | — (served by ADB's memtable) | — |
| Point lookup, base row | 70–82 ms / ~1 s | **1.67 s** / 2.1 s | 55 ms / 151 ms |
| `subject_id` scan | 164–178 ms / 0.4–0.8 s | 1.67 s / 4.1 s | 98 ms / 332 ms |
| Vector top-10 | 8.6 ms (HNSW maintained) · 29 ms (id-only) | 1.75 s / 3.8 s | 97 ms / 620 ms |
| FTS top-10 | 212–275 ms / 0.7–0.8 s | 455 ms / 4.1 s | 164 ms / 320 ms |
| Read volume | ~0.45 MiB/query | **~135 MiB/query** | ~0.5 MiB/query |

- **S5:** just-written rows were visible to every query type through `LsmScanner`
  (30/30 point lookups); vector and FTS work over the memtable with or without
  maintained indexes (brute force without).
- **Unindexed data is the trap for B:** 60k appended rows in 39 small fragments made
  every query scan them in full — 30× slower and 16 GB read for 120 queries.
  `compact_files` (4 s) + `optimize_indices` fixed it, but **the index update took
  230 s, ~99k read requests and ~1 GB for 60k new rows**. Index upkeep (D5) is the
  biggest ongoing cost in either option; A defers it to generation compaction.
- **Nothing that touches object storage meets the S4 targets** (point p99 < 5 ms,
  recall p99 < 25 ms): base reads are 55–100 ms p50 with ~1 s tails. **A local hot-tier
  cache (NVMe/RAM) in front of Lance data files is required**, whichever option wins —
  S4's fallback becomes part of the design.

### Open / close (S7) — 256 tables of 200 rows, open → point query → close

| Concurrency | Tables/s | Requests/s | p50 | p90 | p99 |
|---|---|---|---|---|---|
| 16 | 44 | 610 | 304 ms | 398 ms | 1.0 s |
| 32 | 27 | 371 | 392 ms | 5.4 s | 5.6 s |
| 64 | 45 | 630 | 431 ms | 604 ms | 3.1 s |
| 128 | 26 | 358 | 1.27 s | 5.8 s | 6.1 s |

- ~14 requests per open + first query; **throughput tops out at ~45 tables/s ≈ 620
  req/s — the 750 req/s bucket limit**; more concurrency only adds tail latency
  (Lance's throttle backs off; 0 errors).
- **Misses the target** (p95 < 300 ms): 16 concurrent opens give p90 398 ms.
- Waking all 4,096 partitions of one bucket would take ~90 s. Mitigations: spread
  partitions over many buckets (D1), keep hot partitions open, cache manifests and
  index metadata on local disk so a re-open costs fewer requests.

### Encryption (S12)

| Test | Result |
|---|---|
| SSE-S3 (`x-amz-server-side-encryption: AES256`) | **400 InvalidArgument — not supported** |
| Bucket default encryption | none configured (`ServerSideEncryptionConfigurationNotFoundError`) |
| SSE-C (customer key per request) | **supported and enforced**; GET without the key is refused |

`object_store` (and so Lance) supports SSE-C through `aws_sse_customer_key_base64`,
**one key per store, i.e. per table**. Partitions are shared by tenants in the shared
tier, so SSE-C gives **per-partition keys, not per-tenant keys**. Per-tenant
encryption at rest of searchable columns is only possible in the **dedicated tier**
(own partitions). §14.3 needs that correction; crypto-shredding by tenant still works
for the log and archives (application-level encryption).

### Other numbers

- IVF_PQ build on 50k × 768 vectors (223 partitions): **72 s** alone, 125 s with two
  builds sharing 12 cores — over the S8 target (rebuild < 60 s at the largest partition).
- 50k-row base write: ~3 s (150 MB).

## Gate 1 status after this run

| Item | Status |
|---|---|
| S3 write latency | ☑ measured (table above) |
| S4 read latency | ✕ targets missed on object storage → hot-tier cache required |
| S5 index coverage | ☑ fresh rows visible to all query types |
| S6 request rate | ◐ ~2–3 PUT/s per busy partition at 1 s flush; longer WAL flush to test |
| S7 open/close | ✕ p95 target missed; bucket request limit is the ceiling |
| S12 encryption | ☑ answered: SSE-C only, per table |
| S8, S9, S11 | ☐ not run (vector index at scale, f16 recall, physical forget) |

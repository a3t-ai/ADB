# Spikes B2, S1, S2, S10 (+ desk checks B4, S5, R1, R2, R7)

> **Date:** 6 Oct 2026 · tracks `RISK_BURNDOWN.md` · code in [`b2/memwal/`](b2/memwal/)
> **Environment:** local disk (overlay FS) **and Hetzner fsn1, bucket `adb2`** (same
> day, after B1d cleared), Lance **12.0.0**, Rust 1.97.1, 12 cores.

## Verdicts

| ID | Question | Verdict |
|---|---|---|
| **B2** | Is the MemWAL writer API in a released crate? | ☑ **Yes — Lance 12.0.0** (crates.io, 17 Sep 2026). Names differ from the draft spec. |
| **S10** | Two writers on one shard | ☑ **Fenced by epoch** (local and S3). The second writer takes epoch N+1 and replays the first writer's durable rows; the first writer's next `put` fails `Writer fenced (peer claimed epoch)`. |
| **S1** | Rows + `_meta` atomic across a crash (plain commit path) | ☑ **Holds, locally and on fsn1.** fsn1: 1,000 SIGKILLs (30–3,030 ms window), 3,191 batches committed, 832 runs killed after ≥ 1 commit — **0 inconsistent tables, 0 open failures**. Local: 500 kills, 0. |
| **S2** | Offset guard: a stale applier can't revert rows | ✕ **The guard as written in §9.3 fails** — locally and on fsn1. Lance's default conflict retries re-run a stale `merge_insert` on top of newer data and commit it. ☑ Two working guards found, both verified on fsn1 (below). |
| **S3 (first data)** | Apply latency on fsn1 | ◐ **One `merge_insert` commit: p50 306 ms, p90 492 ms, p99 3.2 s** (local: 7 ms). Durable 1-row MemWAL put: p50 21 ms, p99 284 ms. |
| **S5** | Index types MemWAL keeps fresh | ☑ (desk) **BTree, HNSW, FTS.** No bitmap. HNSW needs `FixedSizeList<Float32>`. |
| **B4** | Dependency licences | ☑ **Pass.** 800 crates in the target stack; no GPL/AGPL/BUSL/SSPL. Three MPL-2.0 crates, used unmodified. |
| **R1/R2** | openraft version; async leader flush | ◐ (desk) **Only 0.10 alpha does parallel leader fsync.** 0.9.25 waits for the leader's own fsync before replicating. |
| **R7** | Snapshot as pointer | ◐ (desk) Fits openraft's opaque snapshot data, but **puts the archive on the failover path** (below). |

## B2 — Lance 12.0.0 MemWAL writer

The draft spec's names (`LsmWriteSpec`, `flush_lsm`, `close_lsm_writers`, `use_lsm`)
don't exist; the shipped API is:

| Spec name | Lance 12.0.0 |
|---|---|
| enable LSM on a table | `dataset.initialize_mem_wal().unsharded() / .bucket_sharding(col, n) / .identity_sharding(col).maintained_indexes([..]).execute()` (`DatasetMemWalExt`) |
| `writerConfigDefaults` | `InitializeMemWalBuilder::writer_config_defaults(ShardWriterConfig)` |
| open an LSM writer | `dataset.mem_wal_writer(shard_id, ShardWriterConfig)` → `ShardWriter` |
| write | `ShardWriter::put(batches)`, `delete(keys)` (tombstones), `put_no_wait` |
| `flush_lsm` | `force_seal_active()` → `SealFence`, `wait_for_flush_drain()` |
| `close_lsm_writers` | `ShardWriter::close()` / `abort()` |
| read fresh + flushed + base | `LsmScanner::new(base, shard_snapshots, pk_cols)` (scan, point lookup, `nearest`, `full_text_search`) |
| merge generations into base | caller-driven: `merge_insert(..).mark_sstables_as_compacted(..)`, atomic with the data commit |

Run output ([`b2/memwal/src/main.rs`](b2/memwal/src/main.rs)):

```
[2] writer1 epoch=1 memtable rows=20
[3] writer2 epoch=2 (writer1 epoch=1); writer2 memtable rows after open=20
[3] writer1 put after takeover: rejected: Writer fenced (peer claimed epoch): local epoch 1 < stored epoch 2
[5] manifest after flush: … writer_epoch: 2, current_generation: 2, sstables: [generation 1, 30 rows]
[6] 200 x 1-row durable put (5 ms WAL flush interval, local disk): p50=3.9ms p99=5.8ms
```

The latency is bounded by the 5 ms flush tick on local disk; it says nothing about
object storage (S3/S6 still open).

Findings that matter for option A:
- **Durability is relaxable:** `durable_write(false)` + `max_wal_flush_interval`. That
  is the switch S6 asked about — with the Raft log as the WAL, MemWAL can run
  non-durable and lazy.
- **Requires an unenforced primary key** (`lance-schema:unenforced-primary-key`) —
  ADB's `id` column already is one.
- **MemWAL has its own epoch fencing** per shard. In option A, ADB's partition epoch
  can map 1:1 onto the shard writer epoch (one shard per partition table).
- **Option A changes the apply model.** Rows go to the shard WAL/memtable, not to a
  base-table commit, so §9.3's "rows + `_meta` in one Lance commit" becomes "rows +
  `_meta` in one `put`" (one WAL entry). Compaction into the base is ADB's job.
- Flushed generations are separate Lance datasets (`{uuid}_gen_N`) until compacted —
  adds object count per partition (feeds S6/S7).

## S2 — the offset guard

Test ([`b2/memwal/src/bin/s1s2.rs`](b2/memwal/src/bin/s1s2.rs)): each batch k upserts
20 rows with `value = log_offset = k` plus `_meta`; every table version is compared
with the model of applying `0..=_meta` in order.

**Deterministic case** — zombie holds a table handle from before offsets 1–5, then
re-applies offset 1:

| Mode | Zombie's commit | Table after |
|---|---|---|
| `plain` — `UpdateAll`, Lance default `conflict_retries` (= §9.3 as written) | **COMMITTED** after 2 attempts | **Corrupt:** `_meta` 5 → 1, batch-1 rows reverted |
| `noretry` — `UpdateAll`, `conflict_retries(0)` | rejected ("Too many concurrent writers") | correct |
| `guarded` — `UpdateIf("source.log_offset > target.log_offset")` | committed as a no-op | correct |

**Race** — owner applies 1..300 while a zombie re-applies random old offsets from views
1–4 versions stale; every version checked:

| Mode | Zombie commits | Versions ≠ model | Final |
|---|---|---|---|
| `plain` | 3 ok / 1 rejected | **9** | **corrupt** (`_meta`=213, rows at 300) |
| `noretry` | 0 ok / 318 rejected | 0 | correct |
| `guarded` | 306 ok (no-ops) | 0 | correct |

**Why:** on a commit conflict Lance re-executes the merge against the latest version.
The ADB-side "read `_meta` then commit" check isn't re-run, so the stale batch lands
on top of newer data. The reverts hit both `_meta` and the data rows.

**Recommended guard (both, belt and braces):**
1. `conflict_retries(0)` on every apply. Every batch rewrites `_meta`, so any two
   applies conflict on that row, and the commit becomes a CAS on the version the
   applier read. On rejection the applier re-reads `_meta` and decides again
   (apply, skip, or stop because it lost ownership).
2. `WhenMatched::UpdateIf("source.log_offset > target.log_offset")` so each row is
   monotonic even if (1) is ever loosened. Limitation: an insert of a row that a later
   batch *deleted* is not guarded, so deletes must be tombstone rows (with
   `log_offset`) until compaction — the same approach as MemWAL's `_tombstone`.

New unknown this creates: with `conflict_retries(0)`, **maintenance commits**
(compaction, index build, old-version cleanup) may also reject concurrent applies.
Measure the apply rejection rate under maintenance (add to S3 / D5).

## S1 — crash atomicity (plain commit path)

A child process applies batches in a loop; the parent SIGKILLs it after 30–330 ms (fsn1: 30–3,030 ms) and
checks the table, then restarts it from `_meta + 1`.

| Mode | Kills | Batches applied | Inconsistent | Open failures |
|---|---|---|---|---|
| `guarded` | 300 | 7,352 | 0 | 0 |
| `noretry` | 200 | 5,735 | 0 | 0 |
| `noretry` on **fsn1** | 1,000 | 3,191 | 0 | 0 |

Local disk and S3 use different commit handlers (rename vs conditional PUT); both pass.

## S5 — MemWAL index coverage (desk)

`MemIndexKind::ALL = [BTree, Hnsw, Fts]` (`mem_wal/index.rs`). For ADB's schema:
BTree on `id` / `subject_id` / `object_id` / `valid_from` ✓; FTS on `text` ✓; vector
on `embedding` ✓ only as `FixedSizeList<Float32>` (keep f32 in the table; f16 only in
the log); bitmap on `memory` / `kind` ✗ — unneeded on small memtables (scan). Still
to measure: fresh-row recall vs brute force.

## B4 — licences

`cargo metadata` over Lance 12, lancedb 0.39, openraft 0.10-alpha, raft-engine 0.4.2,
usearch 2.26, object_store 0.14, tonic, petgraph, dashmap, pest: **800 crates**, all
permissive except:

| Crate | Licence | Pulled in by |
|---|---|---|
| `colored` 3.1 | MPL-2.0 | `lance-io` → opendal → HuggingFace backend (`hf-xet`) |
| `option-ext` 0.2 | MPL-2.0 | `dirs` (via `lance-index`) |
| `smartstring` 1.0 | MPL-2.0+ | `raft-engine` → `rhai` |

MPL-2.0 is file-level copyleft: fine unmodified, for distribution and hosting. Allow it
in `deny.toml` with a comment. `cargo-deny` is not installed here; add it to CI.

## R1 / R2 — openraft (desk, from source)

| | 0.9.25 (stable, Jul 2026) | 0.10.0-alpha.36 (Sep 2026) |
|---|---|---|
| `RaftLogStorage::append` | returns before fsync; callback on flush | same |
| Leader's own append | `RaftCore` **awaits the flush callback** before running the next command, so `Replicate` waits for the leader's fsync (`core/raft_core.rs` `append_to_log`) | **submits and returns**; `Replicate` needs only "submitted"; leader progress updates on flush notification (`run_append_entries`, `handle_local_io`) |
| Parallel leader fsync (§11.2) | **No** — commit ≈ leader fsync + RTT + follower fsync | **Yes** — commit = any 2 of 3 fsynced |

So R1 is a trade-off: 0.9 stable means serial leader fsync, and 0.10 alpha means the
commit protocol in §11.2 but an API still changing (36 alphas). With PLP NVMe
(fsync ≈ 50–200 µs) the serial cost may be small. R3 measures it on both versions
before pinning.

Also: `raft-engine` 0.4.2 was last released in **Apr 2024**. Prefer ADB's own segment
files behind `RaftLogStorage`, or treat raft-engine as unmaintained.

## R7 — snapshots as pointers (desk)

openraft treats snapshot data as opaque bytes, so the small per-partition map fits.
**Gap:** a follower that installs a snapshot at log index N receives no entries ≤ N.
If it later becomes owner of a partition whose durable `applied_offset` < N, the
entries it must replay are not in its local log — they come from the **archive**.
So archived segments are on the failover path for any node that joined by snapshot,
and openraft's log purge must not run ahead of `min(durable applied_offset)` except
for archived segments (§11.3's rule must also govern openraft's purge). Test this in
R7/R9.

## Hetzner fsn1 results (`s3://adb2/spikes/…`)

| Test | Result |
|---|---|
| B2 / S10 program | Same behaviour as local: epoch fencing, replay of durable rows, seal + flush to an SSTable |
| Durable 1-row MemWAL `put` × 200 | **p50 21 ms, p90 97 ms, p99 284 ms, max 540 ms** |
| S2 stale case | `plain` **commits and reverts** `_meta` 5 → 1; `noretry` rejected; `guarded` no-op — identical to local |
| S2 race, 100 rounds | `plain`: 5 zombie commits, **15 bad versions**; `noretry`: 92 rejected, 0 bad; `guarded`: 123 no-op commits, 0 bad |
| Sequential `merge_insert` apply (21 rows) × 100 | **p50 306 ms, p90 492 ms, p99 3.23 s** (local disk: p50 7 ms) |

What this changes:
- **Durable MemWAL can't be the commit point** for the low-latency tier (p99 284 ms
  per put). That matches the design — Raft commits, MemWAL runs `durable_write(false)`.
  For the object-store commit tier (O1 target p99 < 150 ms) it's an early warning:
  one PUT per write misses it, so commits must be group-batched.
- **One partition table takes ~3 sequential Lance commits per second.** Option B
  must group many log batches into one `merge_insert` (seconds), and read-your-writes
  must read through the overlay/memtable rather than wait for `applied_offset`
  (~0.3–3 s). The S3 target "200 batches/s per partition" is reachable only that way.
- The first S1 run on S3 (1,000 kills, 30–330 ms window) found 0 inconsistencies but
  committed only 5 batches in total: the window was shorter than start-up + one apply,
  so it barely tested kills near a commit. Re-run with a 30–3,030 ms window:
  **1,000 kills, 3,191 batches, 832 runs with ≥ 1 commit, 0 inconsistent, 0 open
  failures.** S1 closed.

## Still to run on Hetzner

- S3 properly (batch sizes 1–50 × flush intervals, option A vs B), S4, S6, S7, S12.

## How to re-run

```bash
cd .ai/spikes/b2/memwal
export PROTOC=/path/to/protoc            # lance-encoding needs protoc ≥ 3
cargo build --release                    # ~10 min cold
./target/release/b2-memwal /tmp/b2
./target/release/s1s2 stale /tmp/s2 plain|noretry|guarded
./target/release/s1s2 race  /tmp/s2r guarded 300
./target/release/s1s2 crash /tmp/s1 noretry 200
./target/release/s1s2 applybench /tmp/ab noretry 100

# On Hetzner: source an env file (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
# AWS_REGION=fsn1, AWS_ENDPOINT=https://fsn1.your-objectstorage.com), then use
# s3://adb2/... URIs. Widen the kill window on object storage:
CRASH_MAX_MS=3000 ./target/release/s1s2 crash s3://adb2/spikes/s1 noretry 1000
```

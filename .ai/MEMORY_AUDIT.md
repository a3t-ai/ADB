# Memory audit: where ADB and FlowR can hold memory

Static audit, 6 Nov 2026. ADB `m0-k5-deps` at 7dc01f2, flowr-system `adb-integration` at 506ab7e. No code was changed for this audit.

## How to read this

Each finding says how far it has been checked:

- **Read:** confirmed by reading the code. The code does this, but how much memory it costs in production is not measured.
- **Measured:** a test or vc-dev reading backs it.
- **Suspected:** the pattern is there, but part of the path has not been traced.

Nothing here is a proven cause of the vc-dev OOMs. Static reading shows what *can* hold memory. Only a heap profile can rank the findings by how much they actually hold.

## Would static analysis help?

Only partly. Standard tools do not find this kind of leak:

| Tool | What it finds | Use here |
|---|---|---|
| Rust compiler | Use-after-free, double free | Already enforced, says nothing about growth |
| Clippy | Code style and some bugs | Can't tell that a map never shrinks |
| Miri, LeakSanitizer | Memory nothing points to any more | Our growth is still reachable, so they report nothing |

The scan found no `Box::leak`, `mem::forget`, `ManuallyDrop`, `into_raw` or `Rc` in about 71k lines (229 files, tests excluded). Classic leaks are therefore unlikely.

The risk in Rust is **logical leaks**: memory still owned by something that never lets it go. So this audit went through every long-lived collection (64), static (41) and channel (21) by hand, plus the large one-off allocations.

## Summary

| ID | Finding | Type | Severity | Status |
|---|---|---|---|---|
| A1 | Finished runs stay in RAM for 30 days, on every node | Grows with use | **Critical** | Read, cost measured |
| A2 | `identities` and `checkpoints` not freed when a journal is evicted | Grows with use | **High** | Read |
| B1 | Memtables seal by row count only; Lance limits are above the pod limit | Bound too high | **High** | Read |
| C1 | Snapshot serialises the whole core in one buffer, up to every 60 s | Repeated peak | **High** | Read, size measured |
| C2 | FORGET compaction loads the whole table into RAM | Peak | **High** | Read |
| A3 | `InputManager.completed` has no cap | Grows with use | Medium | Read |
| A4 | Subscription topics are never removed | Grows with use | Medium | Read |
| D1-D6 | Unbounded queues with no backpressure | Grows if a consumer is slow | Medium | Read |
| B2 | Lance caches default to 1 GiB + 6 GiB per dataset session | Bound too high | Medium | Suspected |
| B3 | Raft log held in RAM for up to 50,000 entries | Bound by count, not bytes | **High** (raised by the heap profile) | Measured |
| A5 | `PutLedger.offsets` trimmed only when `durable_offset` is called | Grows with use | Low | Suspected |
| A6 | `ExpressionEngine.cache` has no cap | Grows with use | Low | Suspected |
| A7 | `Sequencer.seen` never shrinks | Dormant | Low | Read |

## A. Live memory that grows with use

### A1. Finished runs stay in RAM for 30 days

- **Where:** `flowr-system/shared/flowr-storage/src/engine_core.rs:105` (`EngineCore`), removed only by `purge_finished` at `:770`. Retention comes from `FLOWR_RUN_RETENTION_DAYS` in `flowr-ce/crates/flowr-ce/src/engine_worker.rs:130`, default 30.
- **Why it grows:** each run adds to `runs`, `tasks`, `identities`, `checkpoints` and `evicted`. Evicting the journal after 300 s frees only the journal. The limit is time, not size, and each ADB node holds its own copy.
- **Cost:** 60 to 100 KB per run on all nodes (local probe). Not yet split into what survives journal eviction, so the sums below are upper estimates.

| Traffic | Runs in 30 days | Memory at 80 KB per run |
|---|---|---|
| 1 run a minute | 43,200 | about 3.3 GiB |
| 1 run every 10 s | 259,200 | about 20 GiB |

- **Effect:** the 4 GiB limit is reached at roughly 50,000 finished runs. Raising the limit only delays this.
- **Fix:** keep only active runs in RAM. Finished runs move to a table and are read on demand. Add a cap on count as well as age.

### A2. Eviction frees the journal but not what belongs to it

- **Where:** `engine_core.rs:240`. `Change::JournalEvicted` removes `journal` and adds to `evicted`. `identities` and `checkpoints` are only removed when the run is purged (`:200` onward). Checkpoints are appended at `:274`.
- **Why it matters:** `identities` holds one entry per journal record, and `checkpoints` holds a full byte snapshot per checkpoint. So a part of each journal stays for 30 days.
- **Fix:** clear both on eviction. First check that nothing reads them after a run ends, such as a duplicate check or a rewind.
- **Why first:** smallest change in this list that reduces A1 directly.

### A3. `InputManager.completed` has no cap

- **Where:** `flowr-ce/crates/flowr-ce-agents/src/inputs.rs:90`, pushed at `:134`, `:141`, `:187`.
- **Why it grows:** every answered human-input request stays in the Vec until restart. Each one holds its question and answer, so size depends on the payload.
- **Fix:** ring buffer, or drop on a time limit.

### A4. Subscription topics are never removed

- **Where:** `ADB/crates/adb-engine/src/lib.rs:109` (`subscribe_on`) and `:516` (`notify`). Same pattern in `flowr-system/shared/flowr-storage/src/engine_memory.rs:177`.
- **Why it grows:** dead senders are removed only when that topic next fires. A `run:<id>` topic never fires again after its run ends, so its key, its dead sender and its Vec stay forever. Retention doesn't touch them, so this is the one finding with no bound at all until restart.
- **Cost:** small per run, about a few hundred bytes (estimate).
- **Fix:** remove a topic when its last receiver drops, or sweep on a timer.

### A5. `PutLedger.offsets`

- **Where:** `ADB/crates/adb-store/src/store.rs:1043` pushes on every apply. Trimmed in `durable_offset` (`:1173`). Reset when compaction reopens the store.
- **Not traced:** how often `durable_offset` is called in production. If rarely, 8 bytes per apply accumulate. Low either way.

### A6. `ExpressionEngine.cache`

- **Where:** `flowr-system/shared/flowr-expr/src/engine.rs:20`, inserted at `:205`, keyed by expression text.
- **Risk:** low while expressions come from flow definitions, because the keys are then limited. It grows if expression text is built from run data. Many call sites create a short-lived engine, but `FlowContext` holds a longer-lived one (`shared/flowr-core/src/context.rs:37`).
- **Not traced:** how long each `FlowContext` lives, and whether any expression text contains run data.
- **Fix:** LRU with a cap.

### A7. `Sequencer.seen`: dormant today

- **Where:** `ADB/crates/adb-sequencer/src/lib.rs:67`, inserted at `:140` and `:188`, never shrinks.
- **Status:** the production callers found in the grep pass `txn_id: None`; `Some` appears only in the crate's tests. Nothing grows today.
- **Risk:** it becomes a real leak when client transaction ids are wired in (CA-27). Replay recalls a window of 20,000, but live inserts have no such limit.
- **Fix:** apply the same window to live inserts.

## B. Bounded, but the bound does not fit the pod

### B1. Memtables: no byte limit that protects the pod

- **Where:** `ADB/crates/adb-store/src/store.rs:707-710`, sealing at `:1045-1047`.

| Setting | ADB | Lance default |
|---|---|---|
| `max_memtable_size` | 4 GiB | 256 MiB |
| `max_unflushed_memtable_bytes` (backpressure) | 8 GiB | 1 GiB |
| `max_memtable_batches` | 1,000,000 | 8,000 |
| `max_memtable_rows` | 10,000,000 | 100,000 |

- **Why it matters:** ADB seals by its own limit, `memtable_rows` (default 10,000), and it counts rows only. Memory therefore depends on row size: 10,000 rows of 100 KB is 1 GiB in one memtable. Sealing queues a flush and does not wait for it, and backpressure starts at 8 GiB against a 4 GiB pod. If flushing falls behind, the pod hits OOM before any write slows down.
- **Also:** each memtable allocates a batch slot table sized by `max_memtable_batches` (`lance-14.0.2 .../memtable/batch_store.rs:351`). About 72 MB by my estimate of the slot size, mostly untouched and so not resident. Worth confirming, since each FORGET opens two writers.
- **Not measured:** actual memtable sizes on vc-dev. `writer.memory()` reports them, but ADB doesn't publish it.
- **Fix:** seal on `active_bytes` as well (for example 64 MiB), and set backpressure well under the pod limit. The code comment says these values came from spike R11, so re-run its case when lowering them.

### B2. Lance caches

- **Where:** `store.rs:691`. `Dataset::open` uses the default session, with limits of `DEFAULT_METADATA_CACHE_SIZE` 1 GiB and `DEFAULT_INDEX_CACHE_SIZE` 6 GiB. `SsTableCache::new(64)` at `:713` can add more.
- **Not measured:** how full they get. Earlier suspicion of them as the lead was not supported by evidence. The problem here is that the limits exceed the pod.
- **Fix:** one shared session with limits sized to the pod.

### B3. Raft log in RAM

- **Where:** `ADB/crates/adb-raft/src/store.rs:35`. In memory until a snapshot every `ADB_RAFT_SNAPSHOT_EVERY` (50,000), keeping `ADB_RAFT_LOG_KEEP` (20,000).
- **Risk:** limited by entry count, so memory depends on entry size. The 8 MB on-disk figure used earlier to rule this out was misleading, because in RAM entries are parsed `serde_json::Value` trees. See the heap profile below.

Also counted in the total: the hot cache, 256 MiB by default (`adb-store/src/hot.rs:210`). It works as designed.

## C. Large one-off allocations

These do not leak by themselves. Each peak lands on one worker thread, and glibc keeps that thread's arena at the high-water mark. This is what `malloc_trim` hides.

### C1. Snapshot serialised into one buffer

- **Where:** `ADB/crates/adb-engine/src/lib.rs:467`, `serde_json::to_vec(&snap)`, due every `SNAPSHOT_EVERY` (10,000) records or `SNAPSHOT_AFTER` (60 s).
- **Why it matters:** the whole core is turned into one buffer, on every node, about every minute under traffic. It was 42 MB on vc-dev and grows with A1. Serialising also grows the buffer by doubling, so the peak is larger than the final size.
- **Fix:** `serde_json::to_writer` to a buffered file. Reducing A1 shrinks this too.

### C2. FORGET compaction loads the whole table

- **Where:** `ADB/crates/adb-store/src/store.rs:475` (`rewrite_online_with`), reached from `compact_now` (`adb-executor/src/writes.rs:509`, `:517`).
- **Why it matters:** `scan_in` reads every row, including all versions and vectors, into a Vec. The rows are then filtered, encoded and put into a new memtable, so up to three copies exist at once, and all of this happens under the write lock.
- **Checked:** the writers are released. `close(self)` consumes them. The rest of the old and new `Inner`, including each `base` dataset, lives until the function returns.
- **Fix:** stream the rewrite in chunks, so the peak depends on chunk size and not table size. Also stop compacting the whole table on each FORGET.

### Others

- `rewrite_closed` in `adb-log/src/wal.rs` decoded whole log segments. Fixed in 7b1306d.
- `evictable()` (`adb-engine/src/lib.rs:414`) clones up to 10,000 run rows per pass. Low.

## D. Unbounded queues

A queue only grows if its consumer falls behind. None of these has backpressure, so that memory has no limit while it lasts.

| ID | Where | Consumer that can fall behind |
|---|---|---|
| D1 | `adb-executor/src/apply.rs:72` (`Applier`) | Applying to Lance. Holds whole log records |
| D2 | `adb-engine/src/lib.rs:135` (`JournalJob`) | Journal writes, with retries |
| D3 | `adb-server/src/lib.rs:362` (`Sessions` SSE) | A slow client. Removed on drop, but grows while connected |
| D4 | `flowr-ce/.../engine_adb.rs:160`, `engine_remote.rs:249` (`subscribe`) | Run wake-ups. The tasks exit when the receiver drops |
| D5 | `flowr-ce/.../engine_remote.rs:217`, `grants_store.rs:33` | Remote and store writes |
| D6 | `flowr-ce/.../token_stream.rs:94` | Publishing tokens in cluster mode |

Fix: bounded channels, starting with D1 and D2, where the data is large and the consumer is disk. A full queue must slow the writer down and not drop data.

## Checked, no problem found

- `DurableWrites.forgotten`: taken with `mem::take`.
- `grants` STORE: idle sweep after 12 h.
- Audit BUFFER: capped at 1000.
- `running()`: `RunGuard` removes on drop.
- `engine_remote` `live()`: keyed by pool.
- `Registry.subs`: dropped when idle.
- `Watcher.frames`: last 1000.
- `stats_cache`: capped at 4096.
- `AgentHandle.history`: trimmed to `max_history` (`flowr-ce-agents/src/manager.rs:262-267`).
- `token_stream` sessions: capped with oldest-first eviction, ring buffer per session.
- `JournalHook` maps: one hook per run attempt, so they are dropped with it.
- ADB's `spawn_expiry` and the CQ watcher: hold `Weak`, so they exit with their owner.

## What this audit does not cover

- **Dependencies:** Lance, DataFusion, tonic and fastembed internals, except where noted.
- **`Arc` cycles:** not fully traced. Two `Weak` uses exist, and no cycle turned up in the code I read.
- **FlowR spawned tasks:** only those above. ADB has 7 spawn sites. `adb-cli/src/cluster.rs:56` and `adb-executor/src/engine.rs:973` were not read.
- **Other FlowR statics:** `grants_store.rs:23` (`pending`) and the `ops.rs` ledger were read only in part.

## What the evidence says about the vc-dev OOMs

| Observation | What it supports |
|---|---|
| Old build: 3 of 6 FORGETs jumped the leader by 336 to 463 MiB | FORGET triggers a large peak |
| r1 build, no trim: 42 FORGETs, largest jump 98 MiB | The `rewrite_closed` fix helped, or the jumps depended on log state. **C2 was not changed in r1, so C2 alone did not cause those jumps at this table size** |
| About 8 MiB drift per FORGET on r1 | Something still grows. Not identified |
| Local traffic with no FORGET: 190 to 730 MiB in 8 minutes | Fits A1 + A2 + C1. Not proven |
| Trim frees 120 to 190 MiB each minute | Part of the heap is arena retention, not live data. Trim treats the symptom |

The likely picture is two separate problems: live data growing with runs (A1, A2), and peaks that glibc keeps (C1, C2). This is an inference and has not been tested as a whole.

## Heap profile (6 Nov, local)

`tests/perf-a3t.mjs --n 1500 --c 8 --secs 120` on the release `adb` (r1 code, trim off), 3 nodes, with a sampling `LD_PRELOAD` profiler written for this. All the ADB nodes and FlowR had stopped by about 300 s in both profiled runs, and the test driver then hung, so its results were lost. Why is not known. No Raft snapshot was reached in the first run.

Sampled live MiB on a follower (other follower within a few MiB; `lance batch slots` is address space, mostly not resident):

| Owner | 60 s | 120 s | 180 s | 240 s | 300 s | Shape |
|---|---|---|---|---|---|---|
| Raft log entries | 32 | 68 | 93 | 131 | 181 | Straight line, never falls |
| Engine journal memtable | 71 | 110 | 75 | 226 | 104 | Saw-tooth |
| Lance batch slots | 137 | 68 | 68 | 68 | 68 | Fixed |
| Data table memtable | 4 | 16 | 27 | 34 | 48 | Rising, seals later |
| EngineCore | 4 | 9 | 16 | 21 | 28 | Straight line |
| Histogram tables | 25 | 25 | 25 | 25 | 25 | Fixed |

Leader at 300 s: data table memtable 240, Raft entries 175, journal memtable 119, batch slots 138, EngineCore 27.

Follower at 300 s, from glibc: RssAnon 707 = in use 346 + free but retained 266 + about 95 other. Free retained reached about 230 MiB by 120 s, then moved between 120 and 300 with no clear rise.

What changes in the ranking:

- **B3 is the largest steady growth,** about 0.6 MiB/s on every node. This corrects the earlier "Raft log ruled out".
- **B1 is confirmed as large:** memtables reach 226 MiB on a follower and 240 MiB on the leader.
- **A1 and A2 are real but smaller than ranked here,** 28 MiB after 300 s. The first estimate of 60 to 100 KB per run mixed all of the above. A1 stays a long-term limit, not the near-term driver.
- **Arena retention is a large share but looks bounded in this run.** This is what trim hides.

Not shown by this profile: C1 and C2, because they are short peaks and not live data. The workload has no FORGET.

Second run, same workload with `ADB_RAFT_SNAPSHOT_EVERY=2000 ADB_RAFT_LOG_KEEP=500`: Raft entries about 10 MiB at 300 s against 181, and follower RssAnon about 535 MiB against 707. This proves B3 drives the steady growth. Those settings aren't the fix: a follower that falls behind the kept log needs a copy of a peer's data, so the amount kept should stay large.

Not proven: that the leader's larger data memtable comes from `encode` builders holding spare capacity on one-row puts.

## Suggested order

1. **B3:** in the working tree: `adb-raft/src/store.rs` keeps an index and a small tail in RAM and reads the rest through mmap. Needs cluster tests and its own before/after profile.
2. **A2:** clear on eviction.
3. **C1:** stream the snapshot.
4. **B1:** seal by bytes, set limits under the pod limit, and publish `writer.memory()` as a metric. Without that metric the next investigation is blind too.
5. **A4, A3:** remove dead topics and cap completed inputs.
6. **C2:** streaming rewrite.
7. **D1, D2:** bounded channels.
8. **A1:** finished runs out of RAM. This is the real cure for growth with use, and the largest change.
9. **Regression guard:** a test that runs N flows and fails if live heap per run exceeds a budget. Without this, new growth stays invisible until the next OOM.

After these, `ADB_MALLOC_TRIM_SECS` should be unnecessary. Remove it and confirm heap stays flat.

## Heap profile after the fixes (7 Nov, local)

Same workload (`tests/perf-a3t.mjs --n 1500 --c 8 --secs 120`), debug build, jemalloc with its own profiler (`MALLOC_CONF=prof:true,...`). Every phase completed. Live MiB by owner:

| Owner | Leader | Followers | Shape |
|---|---|---|---|
| Data table memtable | 7 to 71 | 15 to **110, 123** | Saw-tooth on the leader; rises the whole run on followers |
| Raft log (incl. 26 to 29 MiB of static histogram tables) | 39 | 96 | Flat |
| Leader's Raft tail (entries as JSON) | 58 | n/a | Flat |
| Engine journal memtable | 16 to 83 | 17 to 105 | Saw-tooth |
| Lance manifest building (`Transaction::build_manifest`) | 2 to 55 | 1 to 61 | Spikes at commits, higher later |
| EngineCore | 34 (peak 43) | 37 (peak 45) | Now falls as journals are evicted |

RssAnon at the end: 432, 467, 466 MiB, against about 285 and 355 MiB live: jemalloc and profiler overhead not yet broken down.

What that profile led to (7 Nov, all measured on the same workload):

| Finding | Cause | Fix | Result |
|---|---|---|---|
| Followers' data memtable rose all run | One or two rows per put; each batch costs about 24 KiB of arrays and small allocations that Lance doesn't count, so the 64 MiB seal never fired | Seal counts `batch_count * 24 KiB` on top of Lance's figure | 22 to 28 MiB, saw-tooth, instead of 110 to 123 |
| Raft tail 58 MiB on the leader, most of 96 on followers | The 8 MiB tail held parsed JSON | Tail kept encoded, decoded on read | Raft log 39 to 46 MiB on every node (26 to 29 of it openraft's histogram tables) |
| Lance manifest spikes growing on followers | Followers never compacted anything | `Replica::maintain`: the leader's thresholds, once a minute; the data table keeps all history (windows and pins are the leader's) | Generations no longer pile up; forgotten payloads leave a follower's table |
| Follower spikes to 671 to 681 MiB | Compaction (C2) read the whole table, decoded and re-encoded it in memory; the engine journal holds 30 days | Compaction streams the LSM scan into the new table's base, 10,000 rows at a time | Follower peak 255 MiB live; RssAnon max 267 |

RssAnon over 1,470 s now: leader 107 to 305 (max 344), followers 101 to 259 (max 267). Start of this work, glibc: 816, 787, 785 at 630 s.

## Open: erasure on followers' logs

After a FORGET the leader rewrites its own WAL without the forgotten payload (`rewrite_closed`). Followers don't, and the Raft `entries` file keeps up to `ADB_RAFT_LOG_KEEP` (20,000) entries holding it. Follower compaction now erases it from their tables, not from their logs. Not a memory issue: an erasure correctness one.

## 7 Nov: jemalloc's own counters, no profiler (step 4)

Same run (`perf-a3t --n 1500 --c 8 --secs 120`, debug), `ADB_ALLOC_STATS_SECS=30`, no `MALLOC_CONF`.
At the end (about 1,410 s), MiB:

| | RssAnon | resident | active | allocated (live) | retained (address space only) |
|---|---|---|---|---|---|
| Leader | 272 | 240 | 222 | 170 | 509 |
| Follower | 224 | 193 | 182 | 161 | 535 |
| Follower | 222 | 186 | 178 | 158 | 521 |

- Live data (`allocated`) is flat after the first ~600 s: followers 115-130 MiB, leader 130-150 MiB,
  with short peaks (up to 187 / 234) during compactions and the write phase. No steady growth.
- The profiler cost about 50 MiB per node (RssAnon 305/255/259 with it, 272/224/222 without).
- Gap between RssAnon and live data, leader: about 50 MiB of page fragmentation (`active - allocated`,
  higher on the leader than on followers at about 20), about 18 MiB of freed pages waiting for decay
  (`resident - active`), and about 30 MiB outside jemalloc (thread stacks, binary data).
- `retained` (~500 MiB) is virtual only: returned to the system, costs no RAM.
- Throughput unchanged: 7.8 flows/s, 5.6 writes/s (p50 1.25 s, debug).

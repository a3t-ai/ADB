# Write path rework (PF-1 to PF-12)

Status: design, 7 Nov 2026. Tasks and their status: the Performance section of the
core-features doc. No compatibility with old data or old builds is kept (user, 7 Nov).

## Why

One sequencer lock (`DurableWrites::seq`) serializes every write in the database and
is held across work that has nothing to do with ordering:

| Held across | Cost (release, 3 nodes, 7 Nov) |
|---|---|
| Raft commit (`append_record` awaits `client_write`) | 5–9 ms |
| UPDATE's wait for the applier, then its read of the current version | up to ~700 ms with a payload scan |
| Platform-table Lance `merge_insert` + commit | 34–63 ms |
| FORGET's compaction (removed in the fast step) | 0.6–2 s |

Result: STORE caps at ~280/s for every memory type, UPDATE at 12/s, platform writes at
68/s (after group commit), and a slow write of one kind stalls all others.

## Invariants kept

1. A write is acknowledged only after Raft has committed it.
2. One total order per partition: offsets, subscription cursors and replay as today.
3. Read-your-writes on every table type: a read issued after an acknowledged write sees it.
4. No read on any node returns a forgotten record from the FORGET's commit on.
5. Crash recovery: replay of the committed log into every target.
6. The replay oracle (`LocalEngine::replay_only`) equals the live engine.

## Design

### PF-1 Ordering-only sequencer

Split each write into **prepare** (no lock) and **sequence** (lock, microseconds).

- *Prepare*: parse, resolve targets, read current versions, build effects, embed.
  Runs against a read snapshot at offset `N` (the applied mark it waited for). Records
  the keys it read and the keys it will write, with `N`.
- *Sequence*: under the lock, check each key in the write set: `last_write[key] <= N`
  (an in-memory map of the last committed-or-pending offset per key; bounded by
  dropping entries older than the oldest in-flight snapshot). On a conflict, release
  and redo prepare (bounded retries, then a `conflict` error with its code). On success
  assign the offset and commit time, set `last_write[key] = offset` for the write set,
  update the in-memory views that later prepares need (live keys, aliases, quotas), and
  submit the record to Raft. Release the lock **before** awaiting the commit.
- Writes whose decision depends on more than their own keys (uniqueness of a new key,
  quotas, MERGE) put those keys or a namespace-level pseudo key into the read set.
- Losing the lead: the engine is dropped and the new leader rebuilds from the committed
  log, so pending in-memory state needs no rollback.
- Failure of a submitted record (rare: Raft refused): the engine stops serving writes
  and rebuilds from the log, as on a lead change.

### PF-2 Pipelined Raft

`client_write` calls are issued without waiting for the previous one; openraft batches
them into AppendEntries. Acknowledgement order to clients may differ from offsets; the
log order is the truth. Group commit for platform writes stays as a batching layer.

### PF-3 One log

The openraft entries file becomes the WAL: entry index = ADB offset, segmented files
(finished segments can be rewritten or dropped), one fsync per batch. Readers (appliers,
subscribers, followers' replicas) read up to the committed index only. Retention keeps
the larger of Raft's need and `ADB_LOG_RETAIN`. A follower past the retained log needs a
data snapshot (DB-16). Single-node mode uses the same log without Raft.

### PF-4 Appliers for every target

One ordered applier per target, each publishing `applied(offset)`:
memory LSM (exists), platform tables (one per table), engine spaces, subscriptions /
WATCH. A write is acknowledged when committed **and** applied by the targets it touches;
reads wait for the applied mark of their own target only (the existing `ApplyBarrier`,
per target). No apply runs under the sequencer.

### PF-5 Platform tables on the memtable path

Each platform table gets a primary key (its upsert key, or a generated `_id`), a Lance
MemWAL memtable and the LSM read path memory tables use. `UPDATE … SET col = expr WHERE`
and `DELETE WHERE` are resolved in prepare against the merged view (memtable + Lance)
into per-key effects, so the log holds row-level effects and the applier never
evaluates a filter. Interim (fast step): BTree index on upsert keys at platform compaction.

**As built (8 Nov, `crates/adb-platform/src/mem.rs`):** ADB's own memtable rather than a
Lance MemWAL per table (platform tables have no fixed primary key, and their schemas
grow). Upserts on a key and inserts into an existing table are applied to memory and
acknowledged after the Raft commit; the platform applier flushes memory to Lance, one
commit per table, every 200 ms or 4,096 rows. Reads (`query`) evaluate the filter on
memory with Lance's own planner (A3T's double-quoted values) and merge it over Lance,
memory's version of a key winning; the SQL console and compaction flush first. Updates
and deletes by filter, creates, replaces, purges and new columns flush first and apply
to Lance directly, so they see every earlier write (no filter is resolved early).
The platform's applied offset (the log's purge floor, leader and followers) moves only
with a flush. A flush marker (each table's version before) and per-table flushed
offsets make a crash mid-flush lose and double nothing; a table whose Lance version is
not the recorded one takes the direct path, whose version count skips what landed.

### PF-6 FORGET (done in the fast step)

A tombstone committed through Raft; bytes leave at normal compaction (due at most one
hour after a FORGET). See memory note `adb-forget-semantics`.

### PF-7 Maintenance off the lock

Compaction reads a snapshot at offset `N` and writes the new table without holding the
sequencer; then, under a short lock, it applies the records after `N` to the new table
(or replays them through the applier) and swaps it in. Log redaction of forgotten
payloads (`rewrite_closed`, `LogStore::redact`) needs no write lock (closed segments).

### PF-16 Compaction in place (8 Nov)

PF-7 took compaction off the locks but still copied the whole table each time, so its
cost grew with the table. A fold (`crates/adb-store/src/store_fold.rs`) works on what
changed:

1. Seal the active memtable and wait for its SSTable: every applied row is in an SSTable.
2. Read the SSTables (newest version per id) and, from the base table only, the rows
   whose history window or TTL ran out (`superseded_by IS NOT NULL AND valid_to <
   horizon`, `ttl_at <= now`).
3. Apply the compaction rule: dropped ids are deleted from the base, the rest upserted by
   `id` (Lance `merge_insert`).
4. Rewrite only the base fragments that need it: those that held a tombstoned or expired
   row (FORGET's bytes leave the disk), those with over 10% deleted rows, and runs of 8
   small fragments of one size tier (8k / 64k / 512k live rows).
5. Under the store's write lock (milliseconds): swap the base in, drop the folded
   SSTables from the shard manifest (through the writer's own manifest store), delete
   their directories, the MemWAL WAL entries they cover and the base's old versions.

No log replay and no compaction hold on the log are needed (the PF-7 copy is gone).
Full rewrites remain only for migrations (embedding dimension, vector clearing, id
schemes).

### PF-17 Engine store writes pipelined (8 Nov)

FlowR's engine store (runs, leases, journal) wrote through `engine_exec`, which held the
sequencer and the engine-host lock across the Raft commit: every engine write in the
system ran one after another. Measured on perf-a3t (605 flows, 8 callers): each engine
write waited 20.7 ms (p50) for the lock and took 3.1 ms to commit; about 10 writes per
flow capped durable flows at ~26/s whatever the number of callers.

Now, as PF-1: under the locks only the core's change and the submission; the commit is
awaited after them. A committer task, in log order, writes each committed record's
journal episodes and delivers its notices, then answers the caller. Engine reads (and
operations that change nothing) wait until every engine record submitted before them
has committed, so no read sees a change that may not commit. A space takes no
snapshot while one of its records is in flight (the snapshot names an offset). A
commit failure poisons the engine and restarts it, as in PF-1.

### PF-18 One engine write to start a run (planned)

A run pays `start`, `claim_runs` and its first journal `append` as three Raft commits
before its first step. Merge them into one engine write when the caller runs the flow
itself.

### PF-8 to PF-10 Read path

- PF-8: point lookups consult the memtable's key index first (new version or
  tombstone answers at once); only absent keys read Lance files.
- PF-9: the applier gathers records into larger memtable batches
  (`ADB_APPLY_GATHER_MS`); the log keeps every record until Lance has flushed it.
- PF-10: merged reads keep the newest version per key in one pass, no sort.

### PF-12 Partitions (later)

`LogRecord.partition` exists. Each partition (org or shard) gets its own sequencer,
appliers and later its own Raft group; cross-partition atomic writes are not allowed.

## Tests

- Existing: crash-durability, engine-crash, subscriptions-crash, cluster-raft,
  replay oracle, all executor and platform tests.
- PF-13 concurrency checker: N writers on overlapping keys with UPDATE / STORE / FORGET;
  every acknowledged write is visible exactly once, versions increase per key, no lost
  update; FORGET under load stalls no other writer.
- PF-14 benchmarks: `tests/perf-memory.mjs`, `tests/perf-a3t.mjs --ee`, against the
  7 Nov baseline (core-features doc, Performance).

## Order

Fast step (PF-6, PF-9 gathering, PF-11, PF-5 index) → PF-1 with PF-13 → PF-2 → PF-4 with
PF-5 → PF-3 → PF-7 → PF-8, PF-10 → PF-12 when load needs it.

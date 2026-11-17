# Spike R12 — durable offset, MemWAL GC and Raft log purge

> **Date:** 7 Oct 2026 · Lance 12.0.0 · code: [`b2/memwal/src/bin/r11.rs`](b2/memwal/src/bin/r11.rs)
> (`crash` mode with `CLAIMS_FILE`, `WAL_GC`) · local disk and Hetzner fsn1 (`adb2`)
> **Question:** which offsets may ADB call durable (and let Raft purge below), which
> MemWAL files may it delete, and in what order — without a recovery ever missing an
> entry?

## 1. Observing the durable offset under `durable_write(false)` — ☑

`ShardWriter::memtable_stats().durable_batch_count` is the writer-global count of
WAL-durable batches. ADB writes one batch per log entry, so it keeps
`offsets[batch_number]` and reads its **durable offset = offset of the newest durable
batch**. That is the value ADB reports to Raft (an `Applied` record) and the only value
that may bound log purge. Batches present after replay at open are durable by
definition.

**Safety test:** the applier appends every claimed durable offset to an fsynced file;
after each SIGKILL the next owner's recovered `_meta` must be ≥ the highest claim.

| Where | Kills | Claims up to | Violations | Inconsistent tables |
|---|---|---|---|---|
| local disk | 300 | 57,526 | **0** | 0 |
| local disk, with WAL GC (below) | 200 | 15,955 | **0** | 0 |
| Hetzner fsn1 | 100 | 384 | **0** | 0 |

The fsn1 run had thin coverage: takeover took p50 6.6 s there, so most kill windows
ended before the child applied much (394 batches in total).

**Caveat — local disk is not power-safe.** A process kill keeps the OS page cache, so
the local runs cannot catch a missing fsync. Lance builds its local store with
`object_store::LocalFileSystem::new()`, whose **`fsync` is off by default**: Lance
data, manifests and MemWAL WAL entries on local disk can be lost on power failure even
after a commit returned. For the embedded and single-node profiles ADB must either pass
Lance a store built with `LocalFileSystem::with_fsync(true)` (through
`ObjectStoreParams.object_store`) or never purge its local WAL based on Lance's
durability. Object storage is unaffected (a completed PUT is durable). → K8.

## 2. MemWAL WAL garbage collection — ☑

After each ADB-driven seal: wait for the `SealFence`, read the shard manifest's
`replay_after_wal_entry_position`, delete WAL entries at positions ≤ it
(`mem_wal::util::wal_entry_filename(pos)` under `_mem_wal/<shard>/wal/`).

- 200 kills while GC was running: **0 inconsistent, 0 claim violations**.
- WAL files left at the end: **24** (the same run without GC left **3,813**).
- **Orphan generations:** 826 generation folders on disk, 796 in the manifest — 30
  folders from flushes cut off by a kill or a fence. GC must also delete generation
  folders the manifest does not reference, after a grace period longer than the
  longest flush.
- Shard manifest versions (`_mem_wal/<shard>/manifest/*.binpb`) also accumulate; keep
  the latest few.

## 3. Ordering rules (design; generation compaction not yet built)

For each partition, from newest to oldest data:

1. **MemWAL WAL entries** may be deleted once a sealed generation covers them
   (position ≤ `replay_after_wal_entry_position`) — tested above.
2. **Generations** may be deleted once ADB has folded them into the base table
   (`merge_insert(..).mark_sstables_as_compacted(..)`, with the S2 guard) **and** no
   reader can still be using them (grace period), **and** they are no longer listed in
   the snapshot ADB builds for `LsmScanner` (compaction state lives in the base table's
   MemWAL index, not the shard manifest). Not yet built — the compaction job belongs to
   `adb-store`.
3. **Raft log entries** of a replica set may be purged only below the minimum, over
   its partitions, of each partition's **claimed durable offset** (§1) — and only
   after the segment is archived. openraft's automatic purge is off (R7). A node that
   joined by snapshot fetches older ranges from a peer or the archive (R7).

## Recovery time (input to R6)

Takeover on fsn1 measured p50 3.5 s (R11, 1 s WAL flush, frequent seals) to 6.6 s
(here, 0.5 s WAL flush, few seals). The cost is replaying one object per WAL entry
since the last seal. Bounding the unsealed WAL (seal on time as well as size) bounds
recovery time; R6 must set that limit.

## Status

| Part | Status |
|---|---|
| Durable offset observable and safe to purge below | ☑ (local + fsn1) |
| WAL GC safe under crashes | ☑ (local) |
| Orphan generation cleanup | design rule (found) |
| Generation compaction + GC, archive, Raft purge wiring | ☐ build phase (`adb-store`, `adb-log`) |
| Local-disk power safety | ☐ K8 |

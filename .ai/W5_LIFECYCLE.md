# W5 — Lifecycle: compaction, physical forget, retention

> MVP plan W5 (M2). Closes L10 (FORGET is logical only), G-L4 (platform datasets
> never compacted), P10 (slow restarts), and E-L10 (topic messages never expire).
> Status: **agreed 27 Oct** (user: decisions as recommended).

## What grows today

| Where | What accumulates | Effect |
|---|---|---|
| Memory table (Lance + MemWAL) | Every sealed memtable is an SSTable *generation*; nothing merges them. Tombstoned (forgotten, expired, unlinked) rows, superseded versions and old `_meta` rows stay. | Reads and startup scan more each day (P10: 6.9 s for 4k rows in debug); forgotten data stays on disk (L10). |
| Lance versions | Every commit is a version; none are cleaned. | Disk use; forgotten data recoverable from old versions. |
| Topic messages (`event` rows) | TTL is a read filter; rows stay. | Same as above. |
| Platform tables | One Lance version per write (G-L4). | Disk use, slower scans. |
| Log | Already purged below Lance's durable offset (D3.5). | — |

**Lance 12's MemWAL has no compactor.** Its source expects "an external compactor"
to merge SSTable generations into the base table and drain the shard manifest. ADB
must provide it.

## Approach: rewrite compaction (recommended)

ADB already has a crash-safe table rewrite (W4, the 768 → 384 migration): read every
row newest-per-id through the LSM, write a fresh table beside the old one, close it,
swap the directories. W5 reuses it as the compactor:

1. **Take the sequencer** (writes wait; reads continue on the old table).
2. **Read** every row version through the LSM scanner.
3. **Drop** what may go: tombstoned rows (forgotten, expired, unlinked) and the edges
   of forgotten records; superseded versions older than the **history retention**
   (decision 1); expired topic messages; all `_meta` rows but the newest.
4. **Write** the survivors into a fresh table (base dataset with the `id` index,
   MemWAL initialised, no generations), close it, **swap** directories, reopen the
   writer, release the sequencer. `open` already finishes an interrupted swap.
5. **Delete the old table** (not kept as a backup: that's what makes FORGET physical),
   then `syncfs`. The log is already purged below the durable offset; a forgotten
   record's payload can still be in log segments above it until the next purge —
   compaction forces a purge after the swap.
6. **Platform tables**: `compact_files` + `cleanup_old_versions` per dataset (plain
   Lance datasets, no MemWAL), in the same window.

When: when generations > 16, or tombstones > 20 % of rows, or daily at a quiet hour
(decision 2); and on demand (`adb compact`, an admin Flight action).

Cost: writes pause for the rewrite — about a second per 10k rows on local disk
(estimate; measured in the spike). Fine for single-node MVP sizes; an online
compactor (merge generations without stopping writes) is the follow-up, needed before
large tenants.

Alternative considered — **merge generations in place** (write SSTable contents into
the base table with `merge_insert`, then edit the MemWAL shard manifest to drop them):
no write pause, but it edits Lance-internal manifest state that 12.0 doesn't expose as
API and that the replay bug (S4) already showed to be fragile. Later, with Lance
support.

## Spike (first, half a day)

Measure the rewrite on 10k / 100k rows (pause length, disk freed), confirm a forgotten
value is unreadable afterwards (table files, old versions, log segments), and that a
SIGKILL at every step recovers (the crash harness from D/R11).

## Decisions (27 Oct, user: as recommended)

1. History retention **90 days**. 2. Compaction on **thresholds + nightly 03:00** + on demand. 3. FORGET **gone from disk immediately** (user, 27 Oct, changed from 24 h): FORGET returns only after the forgotten rows are physically removed from the table, old versions and the log.

### The questions as asked

1. **History retention** — how long superseded versions (UPDATE history) are kept:
   (a) 90 days, then compacted away *(recommended)*; (b) forever; (c) a number per
   deployment. Forgotten / expired data is always removed at the next compaction.
2. **When compaction runs** — (a) on thresholds plus nightly at 03:00 *(recommended)*;
   (b) thresholds only; (c) on demand only.
3. **Erasure guarantee for FORGET** — (a) gone from disk at the next compaction, at
   most 24 h *(recommended)*; (b) immediately (compact on every FORGET — slow writes).

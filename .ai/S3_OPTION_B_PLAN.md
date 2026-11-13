# S3 option B: one copy on object storage, Raft as the WAL (plan, 13 Nov 2026)

Branch: `s3-option-b` (from `m0-k5-deps` at `7e240b4`).

## 1. Decision

ADB is a simple self-hosted agent database with small, fixed-size disks. Object
storage (Hetzner, or any S3) holds the data. The cluster keeps **one copy** of each
partition's data, on object storage, plus a replicated Raft log of the recent tail.

| Rejected | Why |
|---|---|
| MinIO in front | One more system to run; self-hosting gets harder |
| S3 mounted as NFS or FUSE | Same latency as S3; no append, atomic rename or conditional write, so Lance commits are unsafe |
| S3 as backup only | Local disks must hold all data |
| Fully leaderless ring (Cassandra) | Last write wins, and FORGET could be missed by a read; S3 already does the storage half of a ring |
| **Option A**: each node keeps a full copy and tiers it to its own S3 prefix | 3× object storage; a new node needs a full copy; extra nodes add no write capacity. Tech debt. |

**Option B is what [ARCHITECTURE.md](ARCHITECTURE.md) §6, §8 and §11 already
describe:**
- partitions with one owner each;
- ownership moves by `claim(partition, epoch+1)` through Raft;
- only the owner applies to Lance; the other members hold the log.

What was built so far is the single-partition shortcut: every node applies
everything to its own local Lance table. This plan builds the designed version, with
three storage decisions made on 13 Nov:

1. **The local root holds manifests and hot files; object storage is a second Lance
   base.** The architecture's `s3://{bucket}/p{n}/memory.lance` root would make every
   Lance commit an S3 PUT. On Hetzner that means 10–100 ms, 750 req/s per bucket, and
   unreliable `If-Match` (§11.5). A local root keeps commits local.
2. **Nothing is physically deleted except by a manual purge.**
   - Superseded files, old manifests and forgotten rows stay on object storage.
   - Every FORGET is recorded in an append-only tombstone log.
   - A purge physically erases data across both tiers, all versions and backups, and
     is logged too.
   - This replaces "physical removal at normal compaction" and the background
     old-version cleanup in §8's Maintenance row.
3. **The Raft log is the only durable WAL.** An entry is kept until the data it
   produced is in a Lance version whose files are all on object storage (the
   **archive watermark**).

## 2. Today (measured from the code, 13 Nov)

| Log | Where | Durable | Trimmed |
|---|---|---|---|
| openraft log | `<dir>/raft/entries` (`adb-raft/store.rs`) | fsync, grouped | after a Raft snapshot, every 50k entries; keeps 20k (`ADB_RAFT_SNAPSHOT_EVERY`, `ADB_RAFT_LOG_KEEP`) |
| ADB WAL | `adb-log/wal.rs` segments; the state machine *is* this WAL (`sm.rs`) | `fdatasync`, grouped | `replica.purge()`, below Lance's on-disk offset, keeping `ADB_LOG_RETAIN` for subscribers |
| Lance MemWAL WAL | `adb-store/store.rs` | no (`durable_write(false)`), flushed every 1 s | by Lance |

- Every write is fsynced **twice** per node, and written a third time without fsync.
- A Raft snapshot is a pointer, so a node behind the trimmed log needs a copy of a
  peer's data directory (`sm.rs`).
- Every node applies every write (`replica.rs`).

## 3. Target

```
client ─▶ gateway ─▶ partition owner (lease + epoch claim)
                        │ batch
                        ▼
                    Raft log (1 group per replica set; followers hold the log only)
                        │ committed
                        ▼
                    owner: memtable ─▶ Lance flush to LOCAL root (manifests, hot files)
                        │                        │
                        │            archive job: copy new root files + manifest
                        │            to object storage; tier cold fragments into the
                        │            object-storage base (compaction with a target base)
                        ▼                        ▼
                    reads: memtable + local root + fixed disk cache over object storage
```

### 3.1 What object storage holds, per partition

```
{bucket}/{cluster}/p{n}/
  mirror/      copy of the local root: data/ _versions/ _deletions/ _indices/ _transactions/
               (root-relative paths identical, so the mirror opens as a Lance dataset)
  cold/data/   the second Lance base: fragments tiered out of the root
```

- **The mirror** makes the latest archived version openable from object storage
  alone.
- **The cold base** is where old fragments move so the local root stays small.
- Both are append-only: nothing is overwritten or deleted, except by a purge.

### 3.2 Archive watermark and log retention

1. **Archived version:** a version is archived when every file it references is on
   object storage (in the mirror or the cold base) and its manifest is in
   `mirror/_versions/`.
2. **Watermark:** the ADB offset folded into the newest archived version, from
   `_meta.applied_offset` in that version.
3. **Log retention:** the Raft log keeps entries above the minimum watermark across the
   group's partitions, and at least the subscriber window. This replaces "durable on
   local disk" in §11.3. The laggard exception stays.
4. **Back-pressure:** if object storage is down, archiving stops and the log grows. At
   `ADB_LOG_MAX_GB`, writes are throttled.

### 3.3 Takeover, a new node, or a node behind the log

1. Claim the partition: `claim(p, epoch+1)` through Raft (§11.4).
2. Open the latest archived version from `mirror/` as a **shallow clone** into the
   local root. The new root's manifest points at the mirror and cold files through base
   paths, so nothing is copied.
3. Replay the Raft log from that version's watermark.
4. Serve. The cache starts cold.

This replaces "copy a peer's data directory" (§2).

### 3.4 One log

- Fold the ADB WAL into the Raft log. ADB offsets map to Raft entries; blank and
  membership entries are skipped, as now.
- Subscribers read the retained window from the Raft log store.
- One fsync per batch instead of two.
- The Lance MemWAL WAL stays non-durable, as now, or is switched off if Lance allows
  it.

### 3.5 Tombstone log

Table `_tombstones`: one row per FORGET.

| Column | Meaning |
|---|---|
| `offset`, `commit_ts` | log position and time |
| `partition`, `namespace`, `scope`, `memory` | what was hidden |
| `predicate` | the key or WHERE |
| `rows` | rows hidden |
| `actor`, `key_id`, `reason` | who asked, and why |
| `lance_version` | the version the tombstone took effect in |

- Written by the applier in the same batch as the tombstone.
- Never compacted away; archived like everything else.
- Queryable with AQL; exportable.
- A purge writes a `purge` row: who, what, and which files were removed.

### 3.6 Fixed local cache

- An `object_store_wrapper` (Lance hook) for the cold base and the mirror.
- A disk LRU of whole files or ranges, size `ADB_CACHE_GB`.
- Pinned in the cache, if they fit: indexes, then the `WORKING`, `SEMANTIC` and
  `PROCEDURAL` fragments; `EPISODIC` is evicted first.

## 4. What Lance gives and what we patch (third_party/lance, 14.0.2)

| Need | Lance 14 | Action |
|---|---|---|
| Files under several bases | `Manifest.base_paths`, `base_id` on data, deletion and index files | use |
| Choose where new files go | `WriteParams.target_bases` | use |
| Credentials per base | `DatasetBuilder::with_base_store_params` | use |
| Base-aware reads | `fragment.rs` opens each file through its own base | use |
| Cache hook | `ObjectStoreParams.object_store_wrapper` | use |
| **Compaction into a target base** | missing: compaction builds its own `WriteParams` and always writes to the root | **patch** (P1) |
| Cleanup never deletes outside the root | that is how it behaves (non-root bases are treated as borrowed) | wanted; also stop root cleanup from deleting un-archived files (P2) |
| MemWAL | root store only | fine with a local root |
| Shallow clone of an archived version | in the format (`transaction.proto`: clones reference data through `base_paths`) | spike (S2) |

## 5. Phases

### Phase 0: storage pieces, single node (no Raft changes; testable with a local "cold" directory)

- **P1. Lance patch:** compaction into a target base. Add
  `CompactionOptions.target_bases`, passed through to the rewrite's `WriteParams`.
  Test: compact root fragments into a second local base; the data reads the same; the
  root files become unreferenced; the files in the second base are referenced.
- **P2. Cleanup guard:** root cleanup must not delete a file the archive has not
  copied yet. This is an ADB policy around `cleanup_old_versions`. Simplest form: ADB
  calls cleanup only up to the archived version, and only evicts local copies that the
  mirror holds.
- **S1. Spike:** open the mirror (a local copy of the root) as its own dataset; read
  and point-look-up across mirror and cold.
- **S2. Spike:** shallow-clone an archived version into a new local root, append, and
  read across all three bases.
- **T1. adb-store:**
  - open the table with an optional cold base (`ADB_COLD_URI` plus storage options;
    `file://` in tests);
  - an archive job (copy root files and the manifest to the mirror);
  - a tier job (P1, picking fragments by age and memory type);
  - report the archive watermark.
- **T2. Tombstone log table** (§3.5).

### Phase 1: ADB cluster on option B (3 nodes, one replica set)

- **One log** (§3.4).
- **Owner-only apply:** followers hold the log and do not apply. Ownership comes from
  claims, spread across the three nodes.
- **Log retention at the archive watermark** (§3.2), with back-pressure.
- **Takeover by shallow clone plus replay** (§3.3), replacing the
  data-directory-copy limitation.
- **Cache** (§3.6).
- **Tests:** a kill-the-owner test; a "node behind the log" test; Hetzner Object
  Storage end to end.

### Phase 2: many partitions per replica set

- Partition routing by hash, owners spread across nodes, gateway routing (§6, §8).
- Benchmarks: the 3-node performance suite, today's single partition against 30
  partitions, with CPU on all nodes.

### Phase 3: more replica sets, and a control plane for placement (§6.1)

## 6. Open points

- **Hetzner request limits:** 750 req/s per bucket. Archiving and tiering batch files;
  one bucket per cluster at first, more if needed.
- **Purge:** design, so erasure is proven across both tiers and backups. Needed before
  hosted personal data.
- **MemWAL WAL off:** check whether Lance supports disabling it cleanly.
- **Shallow clone cost** with many versions; spike S2 decides.

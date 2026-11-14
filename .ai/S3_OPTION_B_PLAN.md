# S3 option B: one copy on object storage (plan, 13 Nov 2026; data path revised 14 Nov)

> **14 Nov revision (user decision):** Raft no longer carries the data. Each partition has an **owner** and **one in-memory WAL replica**; the log goes to object storage as small **WAL segments** every second; a small Raft group holds only the ownership map. See §3.0. §3.2–§3.4 below describe the 13 Nov design and are kept for history where §3.0 does not replace them.

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

### 3.0 Data path (14 Nov): owner + in-memory replica, WAL segments in S3, Raft for ownership only

Why: per-partition Raft groups mean heartbeats and elections per partition and three
copies of every write; the user wants the least replication traffic and the fastest
ack. Consensus is needed only for *who owns what*; the data itself is replicated
primary–backup (as PacificA, Kafka ISR + controller).

```
write ─▶ gateway ─▶ owner of p (lease, epoch)
                      │ memory + local WAL (restart cache only)
                      ├──────────▶ replica of p: memory only (spill to NVMe past 100 MB)
                      │                 │ ack
                      ◀─────────────────┘
                      ack to client
                      │
                      ├─ every ADB_WAL_SEGMENT_MS (1 s) or ADB_WAL_SEGMENT_BYTES (8 MB):
                      │     WAL segment ─▶ {cold}/p{n}/wal/{first offset}   (immutable)
                      │     then tell the replica: drop up to that offset
                      │
                      └─ checkpoint (B: ADB_ARCHIVE_MIN_BYTES / ADB_ARCHIVE_MAX_AGE_SECS):
                            large Lance files ─▶ S3; then delete the WAL segments it covers
```

- **Ack:** after the replica holds the write in memory. No fsync and no S3 call on the
  ack path: one network round trip.
- **The replica holds only what object storage lacks** (about a second of writes).
  It never needs the older log: everything older is in S3.
- **Back-pressure (user, 14 Nov):** past `ADB_REPLICA_MEM_BYTES` (100 MB) the replica
  spills to local NVMe (append-only spill files, group fsync) instead of blocking; the
  ack still waits only for the replica's write (memory, or the spill file once
  spilling). Past `ADB_REPLICA_SPILL_MAX_BYTES` (disk cap) writes wait. When S3
  recovers the backlog is uploaded oldest first and the spill files are removed.
- **Why S3 can't take the small chunks into the large file:** no append on S3 or
  Hetzner; a server-side copy-merge (UploadPartCopy) rewrites the whole object per
  chunk and needs 5 MB parts; parts of an unfinished multipart upload can't be read;
  and a Lance file's footer is at its end. Hence two kinds of objects: small WAL
  segments (short-lived) and large Lance files (kept).
- **Owner failure:** the control plane sees the owner's node lease lapse, raises the
  partition's epoch and promotes the replica. The new owner first uploads its memory
  and spill tail as a WAL segment (now S3 holds every acked write), then
  shallow-clones the last checkpoint and replays the WAL segments after its
  watermark (steps 2a–2c), and serves. A new replica is picked; it starts empty.
- **Fencing:** every write needs the replica's ack, and a replica refuses an older
  epoch; an owner cut off by the network cannot complete a write.
- **Loss window:** writes not yet in a WAL segment (~1 s, or the spill backlog) if the
  owner and the replica die together. `ADB_WAL_REPLICAS` (default 1) can be raised to
  2 for a third copy (same mechanism, one more ack).
- **Control plane:** one small Raft group on the same nodes (the existing openraft
  code) holding `partition → (owner, replica, epoch)` and node leases. Its traffic is
  node heartbeats every few seconds and a few entries per failover; writes never
  pass through it.
- **Nodes are S3 + memory:** the owner's local WAL is only a restart cache; adding or
  replacing a node copies no log.
- **WAL segments after a checkpoint** are pure duplicates (every row, tombstone and
  tombstone-log entry is in the Lance files) and are deleted, so a partition holds
  about one checkpoint interval of segments (confirmed by the user, 14 Nov).
- Tuning variables: `ADB_WAL_SEGMENT_MS` (1000), `ADB_WAL_SEGMENT_BYTES` (8 MB),
  `ADB_REPLICA_MEM_BYTES` (100 MB), `ADB_REPLICA_SPILL_MAX_BYTES`, `ADB_WAL_REPLICAS` (1).

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

**Revised 14 Nov (§3.0)** — the order now:
1. Done: archive the node (2a), restore a node from S3 (2b, local), large-file
   checkpoints (2c).
2. WAL segments to object storage (owner side), replay from segments on restore,
   segment deletion after a checkpoint.
3. Control plane: the ownership map and node leases on openraft (metadata only).
4. Owner → replica shipping: in-memory replica, epoch checks, ack from memory, spill
   to NVMe past 100 MB, drop on segment upload.
5. Failover: promote the replica (upload tail, restore, serve), pick a new replica.
6. Tests: kill the owner under load; S3 outage (spill and catch-up); Hetzner end to end.

The 13 Nov list below (Raft as the WAL) is superseded; the Raft data-path work in
progress is parked on branch `raft-datapath-wip`.

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

## 5b. Status (13 Nov 2026)

| Step | Status | Commits |
|---|---|---|
| P1 Lance: compaction into a target base | done; tests local + Hetzner `adb-1` | `dc2dc62`, `1a358c3` |
| S1 open the mirror from object storage alone | done on `adb-1` (open 144–216 ms, read across mirror + cold ~200 ms) | `1a358c3` |
| S2 takeover by shallow clone | done on `adb-1` (clone 28 ms, no data copied); needed two Lance fixes: the clone was written through the source's store, and the source manifest was read through the target's | `f3ff2c3` |
| T1 adb-store: cold base, lineage, archive, watermark, cleanup guard | done; tests local + `adb-1` | `2f9cae0` |
| T1 tier job (local → cold within a byte budget) | done; local data 742 KB → 315 KB in the test | `b683759` |
| T2 tombstone log (`Kind::TombLog`, `tombstone_log()`) | done; one row per FORGET/expiry in the same put, never in memory views, kept by compaction, archived | (this commit) |
| P2 cleanup guard | done as part of T1 (`cleanup_local_versions`: only versions older than the archived one) | `2f9cae0` |
| Phase 1 step 1: log retention at the archive watermark, archive timer (`ADB_ARCHIVE_EVERY_SECS`, default 30 s), `CURRENT` pointer | done; leader only (followers keep their own purge) | `8b072a5` |
| No erasure with object storage: the fold keeps forgotten payloads (archived), archive runs before log redaction | done | `912f95d` |
| Phase 1 step 2a: the whole node archived — platform tables and engine store beside the memory table (`{cold}/platform/{lineage}`, `{cold}/engine/{lineage}`, `{cold}/NODE`); log floor = the lowest point any of them still needs | done; test `cold_node.rs` (log trimmed to 394 with watermark 400) | `746f27f` |
| Phase 1 step 2b: rebuild a lagging or new node from object storage | done in code; restore tested locally (`restore_node.rs`), 3-node test `tests/cluster-restore.mjs` | `d5688c1` + (this commit) |
| Phase 1 step 2c: large files only — archive = checkpoint (14 Nov, user decision "B") | done; tests `tier.rs` (4 folds → 1 data file + 1 manifest on the mirror; 77 → 34 objects for the same run), `durable.rs` (policy not due → nothing archived) | (this commit) |
| §3.0 step 2: WAL segments to object storage (`{cold}/p0/wal/{first}-{last}.seg`, every `ADB_WAL_SEGMENT_MS`, ≤ `ADB_WAL_SEGMENT_BYTES`), `POINT` at each checkpoint, covered segments deleted, restore from object storage alone (`restore_from_object_storage`) | done; test `restore_s3.rs` local + Hetzner `adb-1` (restore 41 ms local, 1.3 s from `adb-1`; every read matches) | (this commit) |

How step 2b works:
- After an archive tick whose floor moved, the leader logs `Effect::Archived {cold, floor, node}` (`node`: the memory table's `CURRENT`, the platform and engine lineages). Every replica sees it.
- Each node's Raft state machine takes its snapshot at the entry holding `floor` (found by binary search in openraft's log) and carries the point as the snapshot data, so openraft trims its log only to what the archive holds. A membership change after the point waits for the next point.
- A node behind the trimmed log gets that snapshot: it writes `raft/restore`, moves `applied` to the snapshot, stops applying, and exits 76. On the next start, before opening anything, `restore::restore_node` shallow-clones the memory table at the archived version (new lineage), copies the platform tables (each set back to the version `_applied.json` records) and the engine, and starts an empty WAL at `floor + 1`. Raft then sends every entry after the point; each store skips what it holds.
- Limits: an empty node 1 (lowest id) initialises a new group at start (existing rule, not new); `branches/` is not archived.

How step 2c works (why: per-fold archiving made ~11 objects per fold, up to ~95k per busy dataset per day, against Hetzner's 50 M objects per bucket):
- S3 objects can't be appended to, so new data starts as small local files and reaches object storage only in large ones. The log (three copies) holds what object storage lacks meanwhile.
- **When** (`ArchivePolicy`, tuning variables): `ADB_ARCHIVE_MIN_BYTES` (default 512 MiB) of folded data not yet archived, or `ADB_ARCHIVE_MAX_AGE_SECS` (default 1800) since the last archive with anything new in the log. `ADB_ARCHIVE_EVERY_SECS` (default 30) is only how often that is checked. An idle node archives nothing (its own `Archived` record doesn't count as new).
- **Merge first**: the fragments object storage holds no file of are compacted into one file per `min_bytes` (memory table: `PartitionStore::archive`; platform tables: `PlatformStore::merge_unarchived`, under its write lock like its compaction).
- **Checkpoint only**: an archive copies the files the current version uses (data, deletions, indexes, its transaction and its manifest, then the hint); versions between two archives never reach object storage. Platform tables are archived at the versions `_applied.json` names, under the platform's file lock.
- After the checkpoint, local versions older than it are removed (with the small fragments the merge replaced). Folds no longer archive.
- Expected per dataset: about `data ÷ min_bytes` data files plus one manifest (and its index files) per checkpoint; at most 48 checkpoints a day with the defaults.
- Open: the fold can still rewrite archived fragments (size tiers) into new ones, which are archived again; indexes are archived at every checkpoint (4 files each in the test).

Measured on Hetzner `nbg1` from the code-server pod (curl, a TLS handshake per request, so pessimistic): PUT 4 KB 42 ms, 1 MB 98 ms, 16 MB 911 ms (~18 MB/s per stream); GET 1 MB 98 ms, 16 MB 347 ms (~46 MB/s); 64 KB range 35 ms; 32 parallel range reads 187 req/s, no 429s.

## 6. Open points

- **Who asked (tombstone log `actor`, `reason`).** ADB has no caller identity on the write path today (`Provenance::default()`; one shared service token). FlowR would send the caller (e.g. `x-adb-actor`), ADB carry it in the log record. Needs approval (touches FlowR and ADB).
- **The fold rewrites cold fragments back into the root** when they hold tombstoned rows or fall in a size tier; the tier job moves them out again and the old cold file stays (never deleted). Correct, but it costs object-storage space; restrict the fold to local fragments, or give it a target base.
- **Tier policy** is "oldest fragments by id"; per-memory-type priority (keep `WORKING`/`SEMANTIC`/`PROCEDURAL` local, tier `EPISODIC` first) needs fragments split by memory type, or by time, to mean anything.
- **Lance clone test** `test_deep_clone_repairs_legacy_schema_without_activation` fails in our copy because the crates.io package ships no fixture data; not ours.

- **Hetzner request limits:** 750 req/s per bucket. Archiving and tiering batch files;
  one bucket per cluster at first, more if needed.
- **Purge:** design, so erasure is proven across both tiers and backups. Needed before
  hosted personal data.
- **MemWAL WAL off:** check whether Lance supports disabling it cleanly.
- **Shallow clone cost** with many versions; spike S2 decides.

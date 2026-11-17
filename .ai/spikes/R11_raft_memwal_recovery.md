# Spike R11 — recovery offset and stale-writer safety with Lance MemWAL

> **Date:** 6–7 Oct 2026 · Lance 12.0.0 · code: [`b2/memwal/src/bin/r11.rs`](b2/memwal/src/bin/r11.rs)
> **Question:** with option A (MemWAL, non-durable puts, the Raft log as the real WAL),
> how does a new owner know where to replay the Raft log from, and can a stale owner
> damage the table?

## Design (proposed, verified below)

- The applier turns each committed log batch k into **one `ShardWriter::put`**
  holding its rows **plus the `_meta` row** (`id = "_meta"`, `log_offset = k`). One
  put is one WAL entry, so rows and `_meta` persist together or not at all.
- **Recovery:** the new owner opens the shard writer (claims epoch N+1, drops a fence
  sentinel at the WAL tip, replays the WAL), reads `_meta` through `LsmScanner`
  (memtable + generations + base, newest per primary key), and **replays the Raft log
  from `_meta + 1`**. Whatever the old owner had not flushed is simply replayed.
- **Stale owners:** Lance fences them. The successor's sentinel makes the
  predecessor's next WAL write collide (create-only), entries that landed before the
  sentinel are replayed by the successor, and a fenced writer's memtable flushes fail
  at the manifest commit. ADB's partition epoch = the shard writer epoch.
- **ADB drives memtable seals** (workaround, below): Lance's automatic seal limits
  are set very high, and `adb-store` calls `force_seal_active()` at its own row/byte
  limit.

## Results

| Test | Where | Result |
|---|---|---|
| **crash** — SIGKILL a child applier (50–1,550 ms window), next owner recovers and checks every row against `model(_meta)` | local | **300 kills, 0 inconsistent**, 60,916 batches |
| **zombie** — 300 takeovers; old owner keeps writing during and after (half in log order, half random old offsets once fenced); state checked at takeover and after | local | **0 inconsistent, 0 `_meta` regressions**; ~40k fenced puts went into the zombie's own memtable and never became visible; ~73k rejected |
| **zombie**, Hetzner fsn1 (100 takeovers, WAL flush 100 ms) | `adb2` | **0 inconsistent, 0 `_meta` regressions**; ~486k fenced puts rejected, ~6.8k went into the zombie's own memtable and never became visible |
| **crash**, Hetzner fsn1 (100 kills, 50–3,050 ms window, WAL flush 1 s, seal every ~10 batches) | `adb2` | **0 inconsistent**, `_meta` never regressed; takeover p50 3.5 s, p90 7.9 s, max 32.7 s |

## Bug found in Lance 12.0.0 — replay flushes generations without `_pk_index`

**Symptom:** after a takeover, every LSM read of the shard fails with
`NotFound: …/<hash>_gen_N/_pk_index/page_lookup.lance`; the shard manifest lists a
generation folder that has data, BTree index and bloom filter but no `_pk_index`.

**Cause** (`dataset/mem_wal/write.rs`, `replay_memtable_from_wal`): during WAL replay
batches are inserted with `insert_batches_only` (no in-memory index update). When the
replayed data reaches a seal threshold mid-replay, `flush_replayed_memtable` flushes
that memtable; the flusher builds the `_pk_index` sidecar from the memtable's
**in-memory PK index**, which is empty, so `create_pk_index` returns early and the
generation is committed without its sidecar. The comment there assumes sealed replay
memtables "needed no in-memory index build". Only the final active memtable gets its
indexes rebuilt after replay.

**Deterministic repro** (`r11 repro <dir> durable` with `REPRO_BATCHES=50`): writer 1
(100k-row memtable) puts 1,050 rows durably and aborts; writer 2 (400-row memtable)
opens → replay seals twice → generations 1 and 2 have no `_pk_index` → LSM read fails.

**When it hits ADB:** any failover where the WAL tail to replay exceeds one memtable's
seal limits — e.g. the old owner's memtable flushes were lagging (frozen memtables
awaiting flush are replayed too), or limits differ between writers. It happened in
both the crash and zombie tests with identical configs.

**Workaround (adopted):** set `max_memtable_rows`, `max_memtable_size`,
`max_memtable_batches` and `max_unflushed_memtable_bytes` far above anything replay
can reach, and seal from ADB with `force_seal_active()` at ADB's own limit. Replay then
never seals mid-replay; the post-replay index rebuild covers everything, and ADB's
next seal writes a complete sidecar. Verified: repro passes; crash and zombie runs
above use this mode. Constraint: Lance requires `max_unflushed_memtable_bytes` ≥
`max_memtable_size` + index reservation (e.g. 4 GiB / 8 GiB).

**Upstream:** report to Lance with the repro; the fix is to build the PK sidecar from
the replayed batches (or update the in-memory PK index during replay). Re-test on each
Lance upgrade; drop the workaround once fixed.

## Recovery time (input to R6)

Takeover on fsn1 = open table + claim epoch + fence sentinel + WAL replay + read state:
**p50 3.5 s, p90 7.9 s, max 32.7 s**. That is above R6's owner-failover target (p95 < 5 s).
It overstates ADB slightly (the test reads the whole table; ADB reads one `_meta` row),
but the main cost is structural: **replay reads one object per WAL entry since the
last generation**. A first attempt with a 100 ms WAL flush and rare seals made each
takeover slower than the last (it was stopped after 32 min, 0 inconsistencies so far).
Levers for R6: seal often enough to bound the WAL tail, a longer WAL interval (fewer,
larger entries — ADB's 30 s), the local cache, and checking whether replay reads
entries in parallel.

## Notes

- **Partial sidecars would be worse than missing ones** (stale rows could win
  dedup). The live path builds the sidecar from the in-memory PK index maintained by
  the asynchronous `index_applier`; the tests saw no partial sidecar, but this is
  worth a targeted test when the upstream fix lands.
- Lost tail after a crash is expected and harmless: with `durable_write(false)` the
  last ≤ WAL-flush-interval of applied batches may be lost and are replayed from Raft.
- The first zombie run reported 149 "inconsistent" rounds — a test bug: the old owner
  wrote random old offsets while it was still the legitimate owner, which Lance
  correctly persisted. Fixed by switching to random offsets only after the successor
  has opened.

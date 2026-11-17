# Spike R6 — failover time (owner takeover, leader election)

> **Date:** 7 Oct 2026 · code: [`b2/memwal/src/bin/r6owner.rs`](b2/memwal/src/bin/r6owner.rs) (fsn1),
> [`r1/raftproto/`](r1/raftproto/) (`raftproto r6 <dir> <emin> <emax> <heartbeat>`)
> **Targets:** owner failover p95 < 5 s, leader election < 1 s, no lost acknowledged writes.

## Verdicts

| | Result |
|---|---|
| **Owner takeover** | ☑ **0.38–0.54 s** on fsn1 with the "Raft is the WAL" setting (below), versus 3.5–13 s when MemWAL flushes its own WAL periodically. Estimated owner failover ≈ lease TTL + ~0.5 s + local tail replay → **≈ 3 s with a 2 s lease**. |
| **Leader election** | ☑ with retuned timeouts: **465–565 ms at 150–300 ms / 30 ms heartbeat**; the §11.6 default (300–600 / 50) took 911–1,403 ms and missed 1 s in 3 of 5 runs. |
| **Lost acknowledged writes** | **0** in 15 leader crashes. |
| On real hardware | ☐ end-to-end failover (lease + claim + takeover) and false-election rate under load — R10 / Gate 2 phase 2. |

## Owner takeover — where the time goes (fsn1)

Writer 1 seals G generations, writes N more batches as N separate WAL entries, dies;
the new owner times each step (cold caches).

| G | N | Open table | Open writer + WAL replay | `_meta` lookup | **Total** |
|---|---|---|---|---|---|
| 0 | 0 | 91 ms | 394 ms | 275 ms | **0.76 s** |
| 0 | 10 | 107 ms | 1 s | 1 ms | **1 s** |
| 0 | 40 | 142 ms | 3 s | 1 ms | **3 s** |
| 0 | 160 | 166 ms | 12 s | 1 ms | **13 s** |
| 20 | 0 | 104 ms | 285 ms | **2 s** | **3 s** |
| 20 | 40 | 112 ms | 3 s | 1 ms | 3 s |

- **WAL replay is strictly sequential** (`replay_memtable_from_wal` reads one entry,
  then the next): **~75 ms per WAL entry** on fsn1. This is what made R11's
  takeovers 3.5–33 s.
- **A cold `_meta` lookup across flushed generations is slow** (2 s over 20
  generations — each generation's dataset and PK index are opened). Avoidable: the
  Raft state machine already holds each partition's durable offset (`Applied`
  records, R12); recovery takes `max(Raft's durable offset, _meta in the replayed
  memtable)` and replays from there. Re-applying an already-applied batch is harmless
  (the log is deterministic; newest-per-PK picks identical content).

### "Raft is the WAL" — no periodic MemWAL WAL flush

`max_wal_flush_interval = None` and `durable_write(false)`: MemWAL writes a WAL entry
**only when ADB seals** (sealing requires the memtable to be in the WAL). 500 puts
with 4 seals wrote **4 WAL entries**; none appeared in the following 2 s. Takeover:

| Run | Open table + writer | WAL entries replayed |
|---|---|---|
| 1 | 378 ms | 0 |
| 2 | 544 ms | 0 |
| 3 | 488 ms | 0 |

The unsealed tail (offsets 401–500 here) is lost from MemWAL by design and replayed
from the local Raft log (~0.16 ms per put, S6 numbers). The durable offset is now the
last seal (`durable_batch_count` advances only at seals), so the Raft log keeps more —
bounded by the seal cadence.

**Seal policy** (bounds recovery time): seal when the memtable reaches its row/byte
limit **or** every T seconds if it holds anything (e.g. T = 30 s). Tail replay is then
≤ T seconds of writes from local NVMe.

## Leader election — unplanned crash under load

3 nodes, 32 clients, the leader is shut down without stepping down; "gap" = longest
time with no commit completing.

| Election timeout / heartbeat | Elected after (5 runs) | Longest gap | < 1 s |
|---|---|---|---|
| 300–600 ms / 50 ms (§11.6 starting point) | 911, 936, 1000, 1163, 1403 ms | ≈ same | 2 / 5 |
| **150–300 ms / 30 ms** | **465, 488, 503, 510, 555 ms** | ≈ same | **5 / 5** |
| 100–200 ms / 20 ms | 315, 337, 370, 392, 466 ms | ≈ same | 5 / 5 |

Election takes ~2–3 × the minimum timeout (detection + vote + the new leader's first
commit + clients finding it). **Proposed: 150–300 ms, heartbeat 30 ms**, with
openraft's pre-vote on; confirm on the servers that it causes no false elections under
load, slow fsync or long pauses (R10).

## Owner failover budget (estimate, to verify end to end)

| Step | Time |
|---|---|
| Lease expiry detected (TTL 2 s, renew every 0.5 s) | ≤ 2 s |
| Claim committed through Raft | ~1–3 ms (R3) |
| Open table + writer, no WAL replay | 0.4–0.55 s |
| Replay the unsealed tail from the local Raft log | ≤ T s of writes × 0.16 ms/batch |
| If the owner's node was also the Raft leader | + 0.5 s election |
| **Total** | **≈ 3 s** (p95 target 5 s) |

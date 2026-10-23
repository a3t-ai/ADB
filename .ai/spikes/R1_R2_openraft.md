# Spikes R1 / R2 — openraft version and parallel leader fsync

> **Date:** 7 Oct 2026 · code: [`r1/raftproto/`](r1/raftproto/) · openraft **0.10.0-alpha.36**
> **Setup:** 3 nodes in one process on the dev box (12 cores, 2 × Samsung PM863 SATA,
> RAID1 — all three logs share that disk), in-process network. Each node's log store
> makes entries readable on `append` and hands them to a dedicated WAL thread that
> group-writes + `fdatasync`s, then fires openraft's `IOFlushed` callback — the async
> contract ADB's WAL writer will use.

## Verdicts

| ID | Verdict |
|---|---|
| **R2** | ☑ **openraft 0.10 commits on the followers while the leader's fsync is still running.** A 20 ms extra delay on the leader's fsync alone leaves commit latency unchanged (p50 0.98 ms vs 1.06 ms). In 0.9.25 the leader waits for its own fsync before replicating (source, `RaftCore::append_to_log`). |
| **R1** | ◐ **Proposed: pin `openraft = "=0.10.0-alpha.36"`** behind a thin adapter in `adb-log`. It is the only line that implements §11.2's commit protocol; the storage-v2 and network-v2 traits used here are what the 0.10 line converges on. Risk: alpha API churn (36 alphas) — pin exactly, upgrade deliberately. Fallback: 0.9 with serial leader fsync, if R3 on real hardware shows the extra fsync (~0.1–0.3 ms on PLP NVMe) fits the budget. |

## R2 — leader fsync in parallel with replication

16 clients, 1 KiB writes, 8 s each. The extra delay is added inside the WAL thread
after `fdatasync`, before the callbacks fire — i.e. a slow disk.

| Extra 20 ms fsync delay on… | Commits/s | p50 | p90 | p99 | Leader fsyncs |
|---|---|---|---|---|---|
| nothing (baseline) | 14,216 | 1.06 ms | 1.29 ms | 2.87 ms | 14,096 |
| **the leader only** | 14,011 | **0.98 ms** | 1.48 ms | 3.27 ms | **347** |
| one follower | 13,699 | 0.98 ms | 1.61 ms | 3.63 ms | 14,602 |
| both followers (control) | 643 | 21.8 ms | 41.4 ms | 43.4 ms | 653 |

The slow leader completed only 347 fsyncs while 112k entries committed — commits did
not wait for it. With both followers slow there is no fast quorum, and latency jumps
by the injected delay, as it must.

## Early look at R3 / R4 (indicative only)

Same box, no network delay, 1 KiB writes:

| Clients | Commits/s | p50 | p90 | p99 |
|---|---|---|---|---|
| 1 | 2,300 | 0.45 ms | 0.59 ms | 0.82 ms |
| 16 | 14,216 | 1.06 ms | 1.29 ms | 2.87 ms |
| 64 | 21,884 | 2.70 ms | 3.67 ms | 8.37 ms |
| 256 | 20,906 | 11.8 ms | 16.9 ms | 22.6 ms |

- One leader saturates around **~21k commits/s** here (R4 target ≥ 20k/s) with all
  three nodes on one disk and 12 cores, and entries serialized as JSON. Real numbers
  need three servers (R3/R4).
- The latency curve shows group commit working: at 64 clients the leader fsyncs ~7k
  times for ~175k commits.
- Not usable from this run: a simulated 100 µs network hop (tokio timers have 1 ms
  resolution, so every hop slept ≥ 1 ms), and 4 KiB payloads (JSON turns bytes into
  number lists). ADB's log will use a binary encoding.

## What the prototype already provides for later spikes

- The `IOFlushed`-after-fsync WAL thread is the shape of ADB's WAL writer (§11.2).
- An in-process router with per-node fault knobs (extra fsync delay, network delay,
  node removal) — the base for R5–R8 and the R10 fault harness.
- Not yet: durable vote storage (votes are in memory), log recovery on restart,
  snapshots carrying the partition map (R7). These come with R6–R8.

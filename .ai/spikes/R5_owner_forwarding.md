# Spike R5 — owner → leader forwarding overhead

> **Date:** 7 Oct 2026 · code: [`r1/raftproto/`](r1/raftproto/) (`raftproto r5 <dir> <secs>`)
> · openraft 0.10.0-alpha.36, 3 nodes in one process, local SSD
> **Target:** forwarding adds < 0.5 ms p99 in-region.

## Setup

openraft rejects `client_write` on a follower (`ForwardToLeader`), so ADB forwards
itself. The leader exposes a forwarding endpoint over **real TCP on loopback**
(u32-length-prefixed JSON `Vec<Req>`, `TCP_NODELAY`, one persistent connection per
owner task); it proposes the received transactions and replies once all are committed.
Compared with an owner that *is* the leader (direct in-process `client_write`). 1 KiB
transactions, 8 s per case, two runs.

## Results

| Path | Clients | Txns/s | p50 | p90 | p99 |
|---|---|---|---|---|---|
| direct | 1 | 2,206 – 2,326 | 441 – 464 µs | 575 – 598 µs | 823 – 935 µs |
| **forwarded** | 1 | 1,917 – 2,057 | **490 – 508 µs** | 633 – 680 µs | 870 µs – 1.02 ms |
| direct | 16 | 11,688 – 12,353 | 1.11 – 1.15 ms | 1.74 – 1.94 ms | 3.7 – 4.4 ms |
| **forwarded** | 16 | 12,392 – 12,954 | 1.13 – 1.16 ms | 1.40 – 1.66 ms | 3.8 – 4.0 ms |
| forwarded, 8 txns / request | 16 | **20,862 – 30,491** | 3.9 – 4.7 ms | 5.0 – 7.8 ms | 11 – 13 ms |

## Findings

- **Forwarding costs ~45 µs at p50** (one client) for the extra hop, TCP and
  serialization; under load (16 clients) it is within noise. The in-rack network adds
  its round trip (typically 0.05–0.15 ms on a datacenter LAN) — expected well inside
  the 0.5 ms p99 target; to confirm on the servers.
- **Owner-side batching pays off:** forwarding 8 transactions per request gave
  1.7–2.5× the throughput, at the price of per-request latency (the request waits for
  all 8). Here each transaction was still its own Raft entry; ADB forwards a
  group-commit batch as **one** log record (§9 step 4), which should raise the leader's
  ceiling further (R4).
- No leader-affinity placement is needed for latency; it remains an R4 lever only if
  the leader saturates on CPU.

## Design notes for `adb-log`

- Forwarding RPC on the same connection pool as Raft traffic, `TCP_NODELAY`, binary
  encoding (the JSON here inflates payloads).
- The owner sends its group-commit batch (2–5 ms window) as one request; the leader
  acks with the commit offset, which the owner returns to every client in the batch.
- On `ForwardToLeader` / leader change the owner re-resolves the leader and resends
  the batch; batches carry `txn_id`s, so a resend after an ambiguous failure is
  deduplicated by the sequencer (§9.1).

# Storage read/write baseline — dev box vs Hetzner object storage

> **Date:** 6 Oct 2026 · scripts: scratch `bench/localbench.py`, `bench/s3bench.py`, `bench/conn.sh`
> (Python + curl; no fio on the box). Inputs to S3 / S4 / O1.

## Local

| | Local SSD (`/tmp`, 2 × Samsung PM863 SATA, RAID1) | `/home/coder` (CIFS storage box) |
|---|---|---|
| 4 KB append + `fdatasync` (WAL-style) | **p50 0.28 ms**, p99 0.72 ms | p50 0.64 ms, p99 1.29 ms, max 15 ms |
| Sequential write + `fdatasync` | 305 MiB/s (2 GiB) | 103 MiB/s (512 MiB) |
| Sequential read (O_DIRECT) | 496 MiB/s | 88 MiB/s |
| Random 4 KB read, QD1 (O_DIRECT) | p50 0.13 ms, p99 0.19 ms (≈ 7,450 IOPS) | p50 0.56 ms, p99 1.29 ms (≈ 1,790 IOPS) |

## Hetzner object storage (fsn1, bucket `adb2`)

New TLS connection per request (curl); connection setup is ~17–29 ms of each number.
TCP connect ≈ 0.3 ms, so the box is in-region.

| Operation | p50 | p90 | p99 |
|---|---|---|---|
| HEAD, keep-alive connection | **8 ms** | — | max 55 ms |
| HEAD 4 KB | 34 ms | 38 ms | 58 ms |
| PUT 4 KB | 65 ms | 135 ms | 2,489 ms |
| PUT 4 KB `If-None-Match: *` (Lance commit primitive) | 50 ms | 103 ms | 151 ms |
| GET 4 KB | 107 ms | 174 ms | 1,170 ms |
| LIST prefix (100 keys) | 46 ms | 87 ms | 124 ms |
| PUT 1 MiB | 84 ms | 148 ms | 158 ms |
| GET 1 MiB | 79 ms | 105 ms | 120 ms |

| Throughput (64 MiB objects) | 1 stream | 8 parallel |
|---|---|---|
| PUT | 62 MiB/s | 95 MiB/s aggregate |
| GET | 98 MiB/s | 109 MiB/s aggregate |

## What it means for ADB

- **Local WAL fsync is fine:** 0.28 ms p50 on PLP SATA SSDs, inside the Raft budget
  (§11.2). NVMe should be lower; R3 still runs on the target hardware.
- **Object storage is ~200× slower per op than local disk** (8–100 ms vs 0.1–0.3 ms)
  and has **multi-second tails** on PUT/GET (p99 1.2–2.5 s). This is why one Lance
  commit takes ~300 ms (several sequential requests) and why nothing latency-sensitive
  may wait on object storage: commits on the log, reads from the memtable / hot cache.
- **Tails need hedging:** reads behind recall (S4) should use request hedging / short
  timeouts with retry; the client must reuse connections (TLS setup ≈ 20–30 ms).
- **Throughput tops out near 100 MiB/s** even with 8 streams, though the NIC reports
  10 Gb/s. Hibernation wake, index rebuilds and partition moves are bounded by this:
  e.g. a 1 GiB partition takes ≥ 10 s to read. Check whether more streams scale
  (S7, D1 sizing).
- **Don't benchmark on `/home/coder`:** it's a network mount, 3–5× slower than local
  SSD with 15 ms fsync outliers. Build output and spike data belong on local disk.

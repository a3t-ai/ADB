# ADB + A3T on AWS: tests, findings and change log

Started 10 Nov 2026. **Nothing here has been applied**: the user asked for full tests and a
report from every perspective first, then a plan, then changes.

## Environment

All `m7i-flex.large` (2 vCPU, 8 GB, 8 GB gp3 EBS root), Amazon Linux 2023, eu-north-1c.
Images as on vc-dev: `adb:2026-11-09-obs1`, `flowr:2026-11-09-ee-obs1`, run with Docker
(`--network host`; the release binary needs glibc 2.39, AL2023 has 2.34).

| Role | Public | Private |
|---|---|---|
| ADB node 1 (leader at start) | 16.16.78.154 | 172.31.4.217 |
| ADB node 2 | 16.170.133.198 | 172.31.7.184 |
| ADB node 3 | 13.51.254.63 | 172.31.15.144 |
| FlowR 1 (console, EE) | 16.192.141.156 | 172.31.0.188 |
| FlowR 2 (console, EE) | 13.51.254.135 | 172.31.3.23 |

FlowR: engine store, registry and platform tables on the ADB leader (Flight), Cedar policy
hook, local JWT auth. ADB: data in `/data/adb`, its own tokens. Client: `tests/perf-remote.mjs`.

## Measured so far (10 Nov)

| Fact | Value |
|---|---|
| One `fsync` of 4 KB (dd oflag=dsync) | EBS 2.0 ms; code-server pod 0.29 ms |
| FlowR host → ADB leader, private network | 0.67 ms per HTTP request |
| code-server pod → AWS | ~55 ms per HTTP request (not usable for latency) |

ADB alone (client on FlowR 2, FlowRs stopped):

| Operation | c=8 | c=32 | c=64 |
|---|---|---|---|
| LOOKUP KEY | 3,333/s p50 1.9 ms | 3,373/s | 3,333/s |
| RECALL WHERE … LIMIT 10 | 963/s p50 8.1 ms | 965/s | 1,032/s |
| STORE (new key) | 88/s p50 90 ms | 86/s | 84/s |
| UPDATE | 79/s | 86/s | 86/s |
| Mixed 70/25/5 | 249/s | 243/s | 237/s |

Whole system through both FlowRs (client on the pod, +55 ms per call):

| Operation | c=16 | c=64 | c=128 |
|---|---|---|---|
| get_state | 521/s | 1,970/s | 2,286/s |
| set_state | 240/s | 350/s | 389/s |
| flow read3 (durable) | 47/s | 58/s | 57/s |

Load at the end: ADB leader 3.0 (of 2 vCPU), followers ~1.1, FlowRs 0.2–0.6.

## Findings

F-1. **Writes are capped by serial disk syncs, not CPU.** Each write waits for about six
syncs one after another (≈12 ms on EBS → ~86 writes/s at any concurrency; ≈1.7 ms on the
pod → the flat ~585/s seen there at c=8 and c=32):
- Raft log append on the leader and a follower (grouped: several appends share a sync);
- `save_committed` (`adb-raft/src/store.rs`): the `committed` file rewritten atomically
  (tmp + sync + rename + directory sync) on every commit; openraft treats it as optional;
- the state machine's `apply` (`adb-raft/src/sm.rs`) awaits each WAL append's sync before
  the next entry, so the WAL's group commit never groups;
- `save()` of the `applied` file after every apply batch: two more syncs.

F-2. **Read load alone can depose the leader.** Full ADB suite, 14:04 onwards: in the
filtered-RECALL phase (each ≈12 ms of server CPU), the leader's CPU went to 200% (both
vCPUs) for ~30 s from 14:08:07; it could not send Raft heartbeats in time ("failed to send a
heartbeat … error sending request"), node 3's election timeout (0.5–1 s) expired at
14:08:34, node 1 stepped down at 14:08:37 and exited by design (`lost the lead`), back as a
follower in ~1 s. All three nodes stayed at the same applied index (68,285). Causes: Raft's
heartbeats and RPC share the runtime and CPU with query execution; nothing bounds
concurrent queries; the election timeout is tight for a 2-vCPU host. Clients pinned to the
old leader then failed (the test driver did; it now follows the leader, as FlowR does not:
FlowR is pointed at one ADB address on AWS).

F-3. **Lance logs "Index __lance_mem_wal has no fields" at ERROR every 10–20 s** on the
release image (seen on node 3), from index maintenance during compaction. Harmless, but an
ERROR every few seconds hides real ones. The 9 Nov change limited `optimize_indices` to the
table's own indexes in the fold; another path still reaches the MemWAL system index.

F-2 again in the second suite (14:37 on): the leader moved twice — node 1 → node 3 before
the run, node 3 → node 2 at RECALL WHERE with 64 clients (clients found it in 2.4 s; 64
requests failed).

F-4. **Reads wait behind writes.** Mixed 70/25/5 at 32 clients: a LOOKUP by key took 161 ms
on the server (≈0 ms alone), a filtered RECALL 172 ms (8 ms alone). Reads inherit the write
queue — most likely the read-your-writes barrier (`ApplyBarrier`) waiting for applies that
are slow because of F-1. Mixed throughput: 89 / 173 / 149 / 184 ops/s at 1 / 8 / 32 / 64.

F-5. **Analytic reads use about one core.** RECALL … ORDER BY n LIMIT 10: 10 ms server time
alone, ~105/s at any concurrency (2 cores would allow ~200). AGGREGATE COUNT: ~180/s.
Filtered RECALL: 5.5–8.3 ms per query, ~237/s (scans payload fields; no index).

F-6. **Keyed reads differ by memory type.** LOOKUP SEMANTIC and RECALL WORKING by key are
answered at ~0 ms server time (hot cache), ~2,800–3,100/s (bounded by HTTP and the 2-vCPU
leader); RECALL EPISODIC by key costs 1.9 ms on the server (~700/s): not cached.

F-7. **Write cost by type** (1 client → saturated): STORE SEMANTIC 45 → ~98/s, EPISODIC
48 → ~118/s, WORKING 84 → ~191/s; UPDATE SEMANTIC 42 → ~95/s, UPDATE WORKING 74 → 356/s;
FORGET 35/s at 1 client (28 ms). Single-client latency 11–28 ms where the same work on the
pod's disk takes 2–5 ms: the serial syncs of F-1.

Resilience (14:38–14:42, 16 writers, `tests/aws-resilience.mjs` + `docker kill` / `start`):

| | Leader killed (node 2) | Follower killed (node 2) |
|---|---|---|
| Acknowledged / lost | 7,384 / **0** | 8,070 / **0** |
| Errors | 16 (one per writer, at the takeover) | 0 |
| Longest gap with no acknowledged write | **4,170 ms** | 54 ms |
| Restarted node caught up | 42 s after its start (down 30 s) | 42 s after its start (down 30 s) |

F-8. **Takeover 4.2 s on AWS** (1.7–2.5 s on the pod): election timeout plus the new
leader opening its engine on 2 vCPU. Within the 5 s budget, with little margin.

F-9. **A restarted node takes ~42 s to catch up** after 30 s down (~2,700 records): the
follower applies the WAL one entry at a time with a sync each (F-1) and its Lance tables on
2 vCPU. A rolling restart of 3 nodes waits that long between nodes.

A3T through both FlowRs (14:43–14:58; server = FlowR's own tool time):

| Operation | c=1 (client in AWS) | c=8 (in AWS) | c=16 / 64 / 128 (pod; server time) |
|---|---|---|---|
| get_state | 406/s, p50 2.4 ms [1.5] | 1,534/s, 4.9 ms | 504 / 939 / 888 /s [2.6 / 34 / 112 ms] |
| list_flows | 522/s, 1.9 ms [1.1] | 2,452/s, 2.8 ms | 520 / 2,014 / 2,186 /s [1.3–1.6 ms] |
| set_state | 72/s, 13.3 ms [12.6] | 250/s, 32 ms | 241 / 354 / 387 /s |
| flow noop (one set step, durable) | 10/s, 104 ms | 30/s, 265 ms | 33 / 39 / 37 /s |
| flow read3 (durable) | 14/s, 75 ms | 44/s, 185 ms | 47 / 59 / 57 /s |
| flow read3 (transient) | 102/s, 9.7 ms | 250/s, 32 ms | 317 / 345 / 334 /s |

F-10. **Durable flows are the slowest path**: ~5 engine-store writes per run (start, claim,
journal appends, release; PF-18), each paying F-1's serial syncs → 75–104 ms for a short
flow and ~40–60 flows/s at most. Transient flows skip them: 9.7 ms, ~345/s.

F-11. **get_state slows under load while list_flows does not**: server time 2.6 → 112 ms
from 16 to 128 clients (list_flows stays ~1.5 ms). Both are FlowR reads; the difference is
the ADB platform table: state is flushed from the platform memtable every 200 ms into
small Lance fragments, and its upsert key is indexed only at platform compaction, so each
get_state's filtered scan grows with the table's fragments.

F-12. **FlowR's own cost is small**: list_flows 1.1–1.6 ms of server time at any load; the
FlowR hosts stayed at load 0.2–0.6. The system's limits are on the ADB side.

FORGET (each key once): 25/s at 1 client (31.5 ms; server 37.7 ms), 43/s at 8, 41/s at 32.

Endurance (14:52–15:12, 16 clients): STORE SEMANTIC 10 min at a steady 83/s (49,745 rows,
no errors); then mixed 70/25/5 for 10 min: **27 ops/s**, p90 1.4 s — filtered RECALL 1,154 ms
on the server (8 ms when the table was small). At the end: 90,252 SEMANTIC rows, 425 MB in
the leader's `/data/adb`, COUNT(*) 583 ms. Leader memory 270–660 MiB (no growth trend);
followers up to 636 MiB; FlowR hosts idle.

F-13. **Filtered reads grow with the table.** A filter on a payload field (`topic`) has no
index: every query scans every row. 5–8 ms at a few thousand rows, ~0.6–1.2 s at 90k on
2 vCPU — and it holds the CPU the writes and Raft need (F-2, F-4). Keyed reads stay fast.

F-14. **Resources are not the limit for writes; CPU is the limit for analytic reads.**
Write phases: leader CPU 8–60%, the disk syncs set the pace. Analytic and filtered reads:
200% (both vCPUs). Memory per ADB node 0.3–0.7 GB of 7.6 GB; disk ~2–5 KB per row incl.
history and indexes.

F-15. **FORGET is the slowest write** (≈40/s at most; 31.5 ms alone): the tombstone write
plus the lineage lookup, on F-1's serial syncs.

## Report (10 Nov)

1. **Infrastructure.** EBS gp3: 2.0 ms per synced 4 KB write (7× the pod's local disk),
   250 MB/s sequential; VPC round trips 0.3–0.7 ms. Durable-write speed on EBS is set by
   sync latency, not bandwidth.
2. **ADB writes** (F-1, F-7, F-15): ~45–85/s for one client, ~95–120/s (SEMANTIC,
   EPISODIC) and ~190–360/s (WORKING) at any concurrency; FORGET ~40/s. Flat with
   concurrency because each write waits for ~6 serial syncs that no batching shares.
3. **ADB reads** (F-5, F-6, F-13): keyed reads 2,800–3,500/s (hot cache, ~0 ms server);
   EPISODIC by key 700/s (not cached); filtered / ordered / aggregate reads CPU-bound on
   about one core and growing with table size (1 s at 90k rows).
4. **Mixed load** (F-4): reads wait behind the write queue — 150–180 ops/s on a small table,
   27 ops/s at 90k rows.
5. **A3T end to end** (F-10–F-12): FlowR adds ~1 ms; get_state 2.4 ms, set_state 13 ms,
   durable flows 75–104 ms and ≤60/s (engine writes on the slow write path), transient
   flows 9.7 ms and ~345/s. get_state slows as the state table's fragments grow (F-11).
6. **Stability** (F-2): CPU-saturating reads deposed the leader three times in two hours;
   no data was lost or diverged, but clients pinned to a node (FlowR on AWS) break.
7. **Resilience** (F-8, F-9): leader kill → 4.2 s without writes, 0 of 7,384 lost; follower
   kill → no effect; a restarted node needs ~42 s to catch up.
8. **Resources** (F-14): memory 0.3–0.7 GB per ADB node, no growth; FlowR ~50 MB.
   Noise: Lance's index ERROR every 10–20 s (F-3).

## Plan (proposed; awaiting approval)

| Phase | Changes | Goal | Retest |
|---|---|---|---|
| 1. Write path | C-1, C-2, C-3, C-15 | writes scale with concurrency on EBS; catch-up and durable flows faster | ADB suite, A3T, resilience |
| 2. Stability | C-4, C-5, C-6, C-8 | no leader loss under read load; FlowR survives a failover | saturation sweep, leader kill with FlowR running |
| 3. Reads | C-10, C-14, C-12, then C-7 / C-11 (payload-field indexes, parallel scans) | reads independent of the write queue and of table size | mixed + endurance at 100k rows |
| 4. Fewer writes per flow | C-13 (PF-18), then PF-3 (one log) | durable flows at a few commits each | A3T flows |
| 5. Noise | C-9 | clean ERROR level | logs |

Infrastructure option to weigh alongside phase 1: instance-store NVMe (e.g. `m7gd`, `i4i`)
or io2 Block Express sync in ~0.1–0.3 ms, but phase 1 is needed on any disk.

## Round 1 applied (10 Nov, after the user's go-ahead)

| Id | Status |
|---|---|
| C-1 | Done: `LocalWal::enqueue` queues without waiting; the state machine queues the whole apply batch, waits once, then answers (`adb-log/src/wal.rs`, `adb-raft/src/sm.rs`) |
| C-2 | Done: `applied` written at most once a second, always on a membership change and before a snapshot |
| C-3 | Done: `committed` written at most once a second (`adb-raft/src/store.rs`) |
| C-15 | Done by C-1 (followers apply through the same state machine) |
| C-4 | Done: Raft core, replication and RPC server on their own runtime (`ADB_RAFT_THREADS`, default 2; `adb-cli/src/cluster.rs`) |
| C-5 | Done: concurrent reads bounded (`ADB_MAX_READS`, default 2 × CPUs) at `LocalEngine::query` |
| C-6 | Already configurable: `ADB_RAFT_HEARTBEAT_MS`, `ADB_RAFT_ELECTION_MIN_MS` / `_MAX_MS`; no code change |
| C-8 | Done in FlowR: `FLOWR_ENGINE_ADB_URL`, `FLOWR_REGISTRY_ADB(_FLIGHT)` and ADB resources take every node, comma separated; calls move to the next node when one does not serve them; subscriptions reconnect to the current one |
| C-14 | Done: the upsert key's index follows the flushed fragments every 10 flushes, under the compaction marker (`adb-platform/src/mem.rs`) |
| C-9 | Not changed: Lance's `load_scalar_index` logs ERROR for any field-less index except its fragment-reuse one — an upstream bug to report; hiding the message in ADB would mask a symptom |
| C-12 | Not changed: the default cache rule keeps the newest 200 EPISODIC records per scope by design; configurable (`cache.json`, `ADB_CACHE_RULES`) |
| C-7, C-10, C-11, C-13, PF-3 | Next round |

Tests on the round-1 code: adb-raft, adb-log, durable, platform, engine_store,
replica_compact, replica_migrate, write_path_concurrency, flight — pass; cluster-raft (worst
failover 2.6 s, 0 of 30,628 lost), crash-durability (20 kills, 0 of 12,637 lost),
subscriptions-crash, engine-crash — pass; FlowR engine tests 43/43.

## Proposed changes (log)

| Id | Change | Why | Risk / test |
|---|---|---|---|
| C-1 | `apply`: append the whole batch to the WAL, sync once | one sync per batch instead of per entry | crash-durability, engine-crash, cluster-raft |
| C-2 | `applied` file: sync only when membership changes | re-applying after a crash is already a no-op (`at <= WAL last` skipped) | restart and snapshot tests |
| C-3 | `committed`: stop syncing it per commit (Raft log stays the truth) | openraft: optional; restart re-applies up to the quorum's commit | restart tests; reads after restart |

| C-4 | Raft (core, heartbeats, RPC server) on its own runtime / threads | query load must never starve heartbeats (F-2) | read saturation test: no leader change |
| C-5 | Bound concurrent queries (admission control, queue beyond the bound) | CPU saturation degrades latency, not leadership (F-2) | saturation sweep |
| C-6 | Election timeout and heartbeat interval sized for the host (configurable) | 0.5–1 s is tight on 2 vCPU under load (F-2) | failover time stays within budget |
| C-7 | Cheaper filtered RECALL (payload-field filters scan; PF-8 / PF-10) | 12 ms CPU for a LIMIT 10 query caps reads at ~160/s per core (F-2) | read sweep |
| C-8 | FlowR follows the ADB leader (a list of ADB nodes, or a leader-aware endpoint) instead of one fixed address | outside Kubernetes no Service routes to the leader (F-2) | failover with FlowR running |
| C-9 | Find the other path to the MemWAL index's optimize (F-3) | ERROR noise | logs clean |

| C-10 | Reads wait only for the applies they need (their keys / memory type), not the whole queue | reads inherit write latency (F-4) | mixed sweep: read latency stays at its alone value |
| C-11 | Parallel scans for analytic reads (DataFusion partitions across cores) | ORDER BY / AGGREGATE use one core (F-5) | analytic sweep |
| C-12 | Hot cache for EPISODIC keyed reads (rules per memory type) | 1.9 ms vs ~0 ms for other types (F-6) | keyed read sweep |

| C-13 | Fewer engine writes per run (PF-18: start + claim + first append as one; end + release as one) | durable flows pay ~5 serial commits (F-10) | durable flow sweep |
| C-14 | Platform tables: index the upsert key as data is flushed (not only at compaction) and merge small flushed fragments | get_state slows as fragments pile up (F-11) | get_state under write load |
| C-15 | Follower catch-up applies in batches (same as C-1 on the follower) | 42 s catch-up (F-9) | restart catch-up time |

More findings and changes are added below as the full tests run.

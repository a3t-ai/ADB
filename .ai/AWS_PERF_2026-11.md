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

## Round 1 retest on AWS (10 Nov, 17:46–18:32; adb:2026-11-10-r1, flowr:2026-11-10-ee-r1)

Comparable with the baseline (same tests and settings; writes and keyed reads do not
depend on table size). Embeddings are on in both (the image loads bge-small-en-v1.5).

| Measure | Baseline | Round 1 |
|---|---|---|
| STORE WORKING, 1 client | 84/s, 11.9 ms | **189/s, 5.2 ms** |
| STORE WORKING, saturated | ~191/s | **~515/s** |
| UPDATE WORKING, 64 clients | 356/s | **1,035/s** |
| STORE SEMANTIC, 1 client | 45/s, 22.3 ms | 64/s, 15.4 ms |
| STORE SEMANTIC / EPISODIC, saturated | ~98 / ~118/s | ~98 / ~121/s (unchanged: F-19) |
| FORGET EPISODIC | 25–43/s | 28–29/s |
| Keyed reads (SEMANTIC, WORKING) | ~3,000/s | ~3,000/s |
| A3T set_state, 1 / 8 / 128 clients | 72 / 250 / 387 /s | **140 / 675 / 902 /s** |
| A3T get_state, 128 clients | 888/s (server 112 ms) | **2,236/s (server 1.7 ms)** |
| Durable flow noop, 1 client / 128 clients | 104 ms / 37/s | **49.6 ms / 153/s** |
| Durable flow read3, 1 client / 128 clients | 75 ms / 57/s | **36.9 ms / 184/s** |
| Transient flow read3, 1 / 128 clients | 9.7 ms / 334/s | 7.9 ms / 522/s |
| Leader change under 64 clients of filtered RECALL | yes (twice) | **none in 60 s** (C-4) |
| Leader kill: acknowledged writes lost | 0 of 7,384 | 0 of 6,726 |
| Leader kill: longest gap with no write | 4,170 ms | **7,090 ms** (F-16) |

Not comparable: filtered / ordered / aggregate reads and the mixed load — the SEMANTIC
table grew from ~5–10k rows (baseline suite) to ~160k (after the first endurance run), and
those reads scan every row (F-13): filtered RECALL now 108 ms alone, 6.5 s at 64 clients.

New findings:

F-9 (corrected). **Catch-up after a restart is ~1.6 s, not 42 s.** The restarted node logged
`replica caught up` 1.6 s after its start. The resilience script began comparing indexes
only after the 90 s writer run ended, so its "42 s" was the rest of the run — a test bug,
in both runs.

F-16. **Takeover grows with the data.** Kill at 18:09:13; election won at 18:09:16.2; the new
leader's engine open took 5.1 s — hot cache 82k rows / 78 MB in 1.45 s, live-key hydration
161k rows in 1.2 s, engine spaces 1.3 s — serving at 18:09:21.3. The follower holds the data
but not the hot cache or the live keys.

F-17. **The read bound (C-5) blocks cheap reads behind scans.** Mixed load at 32 clients:
a LOOKUP waited 1,277 ms on the server (≈0 ms alone) — the few read permits were held by
filtered RECALLs taking 1.6 s each.

F-18. **FlowR fails fast while there is no leader.** Leader kill with FlowR under set_state
load: FlowR moved to the new leader (C-8 works: 315/s over the 90 s), but answered 3,761
errors during the ~7 s with no leader — every node refused, so the call gave up at once
instead of waiting for an election.

F-19. **SEMANTIC and EPISODIC writes are now limited by embedding, not syncs.** WORKING (no
embedding) went 191 → ~515/s; SEMANTIC and EPISODIC stayed at ~100–120/s with the leader's
two main threads at 80–93% (one embedding per STORE / UPDATE with content). Followers ~2%.

F-20. **One node does all the work.** Leader CPU 196% during the runs, followers ~2%: every
query and every write's preparation (incl. embedding) runs on the leader; followers only
replicate.

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

| C-16 | Reads on followers (wait for the leader's commit index, or bounded staleness by choice) | one node's CPU serves every read (F-20) | read sweep across 3 nodes |
| C-17 | Embedding on its own thread pool, outside the write path; optionally a separate embedding service | SEMANTIC / EPISODIC writes capped at ~100/s by embedding (F-19) | STORE SEMANTIC sweep |
| C-18 | Partitions per org or namespace (PF-12) led by different nodes | writes on one node (F-20) | multi-org write sweep |
| C-19 | Keep the hot cache and live keys warm on followers | takeover rebuilds them: 5.1 s at 160k rows (F-16) | leader kill at 160k+ rows |
| C-20 | Read permits for scans only (keyed reads never queue behind them) | cheap reads blocked (F-17) | mixed sweep |
| C-21 | FlowR waits (bounded, e.g. 10 s) for a leader when no node serves a call | errors during an election (F-18) | leader kill with FlowR load: no errors |

More findings and changes are added below as the full tests run.

## Round 2 applied (10 Nov)

| Change | What was done |
|---|---|
| C-20 | Done: only scans (RECALL / SCAN without KEY) take a read permit; LOOKUP and keyed reads never queue behind them (`adb-executor/src/engine.rs` `scans`) |
| C-21 | Done in FlowR: when no node serves a call (not the leader, connection refused) FlowR keeps trying every node for up to 10 s (Flight calls) / 15 s (engine store) instead of failing at once (`handlers/call_adb.rs`, `engine_adb.rs`) |
| C-19 | Done, three parts. (1) Followers keep the hot cache: `Replica::open` fills it from the same settings, every apply maintains it, and the takeover keeps it instead of rescanning. (2) Live-key hydration reads only the key columns (`PartitionStore::live_keys`, projected scan) instead of decoding every payload and embedding. (3) The engine replay at takeover starts where the follower's engine host got to (`Warm::engine_next`); `EngineHost::replay` skips records a space already applied. This also fixes a bug: the warm host re-applied every change since its snapshot at takeover |
| C-17 | Done: the local model runs on one thread (`adb-embed`) that takes every queued request's texts into one model call (up to 64). Before, the model sat behind a lock and concurrent STOREs embedded one text at a time. Writes still wait for their own vector, so a SIMILAR read right after a STORE finds it. An endpoint (`embedding.json` provider `openai`) remains the way to take embedding off the ADB nodes |
| C-16 | Done (F-20): followers serve reads. A follower answers RECALL / LOOKUP / SCAN (HTTP `/query`, Flight `aql`) and Flight table / SQL reads from its own copy, after asking the leader for its ReadIndex (`/raft/read_index`; concurrent reads share one ask) and waiting until its state machine and tables have applied it, so every node gives read-your-writes. Every other HTTP request is passed to the leader (SSE included), so any node takes any request; Flight writes on a follower answer `unavailable: this ADB node is not the leader`. Followers follow the branch registry and entity aliases from the log so their reads match the leader's; their compaction runs beside the applies so reads don't wait for it. `ADB_FOLLOWER_READS=0` turns it off. FlowR sends reads to the nodes in turn and writes to the leader (`FLOWR_ADB_SPREAD_READS=0` turns it off) |
| C-17b | Done (F-20): embedding spread over the replica set. Each local-model request goes to the node with the fewest requests in flight (this one or a peer's `POST /cluster/embed` on its Raft port); a failing peer is left out for 10 s. `ADB_EMBED_PEERS=0` turns it off |
| C-18 | Not in this round: partitions led by different nodes (write sequencing on several leaders) is a multi-Raft change |

## Round 2 retest on AWS (10–11 Nov, 23:13–00:03; adb:2026-11-10-r2, flowr:2026-11-10-ee-r2)

Same tests as round 1, plus stage F: reads sent round-robin to all three nodes vs to the leader only.
Stages A–E send reads to all three nodes (`--nodes`), writes to the leader. Results:
`scratchpad/round3/`; per-node CPU from the samplers (2 vCPU each, 200% = a full node).

| Measure | Round 1 | Round 2 |
|---|---|---|
| STORE SEMANTIC, 1 client | 64/s, 15.4 ms | 60/s, 16.4 ms |
| STORE SEMANTIC, 64 clients | 99/s, p50 633 ms | **638/s, p50 100 ms** |
| STORE EPISODIC, 64 clients | ~121/s | **889/s, p50 71 ms** |
| STORE SEMANTIC, 10 min at 16 clients | 91/s, p50 175 ms | **410/s, p50 38 ms** (246k writes) |
| UPDATE SEMANTIC, 64 clients | 87/s, p50 742 ms | 127/s, p50 501 ms |
| LOOKUP KEY, 64 clients | 3,005/s, p50 18 ms | 1,773/s, p50 38 ms (worse: F-21) |
| Filtered RECALL, 64 clients | 14/s, p50 4.5 s | 22/s, p50 2.8 s |
| FORGET EPISODIC, 64 clients | 29/s | 27/s (unchanged, not in this round) |
| Mixed 70/25/5, 16 clients, 10 min | 20/s, p50 858 ms, p90 1.4 s | 23/s, p50 120 ms, p90 3.8 s (F-22) |
| Leader kill: acknowledged writes lost | 0 of 6,726 | 0 of 25,565 |
| Leader kill: longest gap with no write | 7,090 ms | 5,568 ms |
| FlowR errors during a leader kill (set_state, 16 clients) | 3,761 | **16** (F-23) |
| A3T get_state / list_flows, 128 clients | ~2,236/s | 2,232 / 2,229/s |
| A3T set_state, 128 clients | 902/s | 856/s |
| Durable flow noop / read3, 128 clients | 153 / 184/s | 134 / 155/s |
| Transient flow read3, 128 clients | 522/s | **1,219/s** |

Stage F, 64 clients, 30 s each:

| Phase | Reads on 3 nodes | Reads on the leader only |
|---|---|---|
| STORE SEMANTIC | 633/s | 631/s |
| LOOKUP KEY | 1,841/s, p50 37 ms | **2,882/s, p50 19 ms** |
| RECALL WHERE topic | **9/s, p50 6.1 s** | 3/s, p50 22.8 s |
| CPU (avg) leader / follower / follower | 137% / 138% / 136% | 140% / 29% / 32% |

CPU (avg %) per stage, leader 16.16.78.154 first: A 100/114/95 · D 184/166/179 ·
E STORE 188/184/183 · E mixed 198/140/146. F-20 is fixed: the followers now do as much
work as the leader (in round 1 they sat at ~2%).

New findings:

F-21. **Point reads on a follower are slower than on the leader.** A LOOKUP takes 0.1 ms
on the server, but a follower first asks the leader for its ReadIndex and waits until it
has applied it: one round trip plus the apply lag, ~20 ms at 64 clients. For point reads
the leader-only path is 1.6× faster; for scans the follower path is 3× faster.

F-22. **Mixed load: scans still set the tail.** p50 fell 858 → 120 ms (C-20: LOOKUPs no
longer queue behind scans), but p90 rose to 3.8 s. Filtered RECALL takes 1.8 s on the
server at ~250k rows (it scans every row: F-13 / C-7), and LOOKUPs on followers took
45 ms on the server (waiting for applies while the node was busy with scans).

F-23. **16 FlowR errors are left during a leader kill:** "transport error", i.e. calls that
were in flight to the killed node. C-21 retries a call no node accepted, not one cut off
mid-way (a write cut off may or may not have been applied).

F-24. **Five ADB tests fail; they fail on the round-1 code too** (found while testing round 2):
`space_flight` (a FORGET missing from the space feed), `branch_pod` (the pod's copy of a
forgotten record is not erased), `subscription_retention` (no reset for a subscriber behind
the log), `scoped_keys` (RECALL WHERE key = "x" finds only the default scope, not team:a /
team:b), `integration::pipeline_timeout_is_a_hard_budget_when_enforced` (timing: the
optimised build finishes inside the 1 ms budget). The first four look like one earlier
regression around FORGET or how records in a scope are filtered; not yet investigated.

| Id | Change | Why | Risk / test |
|---|---|---|---|
| C-22 | Point reads: skip the ReadIndex ask when the follower knows it is current (leader lease / last heartbeat's commit index), or send point reads to the leader and scans to followers | F-21 | stage F: LOOKUP on 3 nodes ≥ leader only |
| C-23 | FlowR: retry idempotent calls (reads, set_state upserts) cut off by a transport error on another node | F-23 | leader kill with FlowR: 0 errors |
| C-24 | Find and fix the regression behind the four failing tests; make the timeout test independent of build speed | F-24 | the five tests |

## Rolled to vc-dev (11 Nov, 00:13 UTC; adb:2026-11-10-r2, flowr:2026-11-10-ee-r2)

Rolled followers first, leader last, data kept; the leader handed over (adb-1 → adb-2, term 33) and
the new leader's engine opened in 1.5 s (35,658 records). All three nodes at index 9,165, no restarts.
The same Flight SQL read on each node gave identical answers (followers wait for the leader's ReadIndex);
20 reads per node all succeeded. No errors in the ADB, console, gateway or rt-worker logs.
Found: vc-dev FlowR still reaches ADB through the leader-only Service (PF-33); `count(*)` over the
memory tables fails in SQL on old builds too (PF-34); gateway and rt-worker try run retention with a
pool token and are refused, one WARN per start (PF-35).

## vc-dev perf, A3T end to end (11 Nov, 00:25–00:44 UTC; adb:2026-11-10-r2, flowr:2026-11-10-ee-r2, PF-33 on)

`perf-remote.mjs a3t --flowr http://flowr.a3t.svc.cluster.local:3002` (the Service over the 2 console pods) with the
user's superadmin token (`A3T_TOKEN_FILE`), 15 s per phase. vc-dev = one 12-core node shared with everything else;
ADB on local disk (hostPath), not EBS. Flows in namespace `bench` run on rt-worker (the router sends only a3t-* /
system-* to the console), so every flow call includes the hop to the runtime pod. No errors.

| Phase | c=1 | c=64 | AWS round 2, c=64 |
|---|---|---|---|
| get_state | 234/s, 3.9 ms | 1,677/s, p50 37 ms | 1,984/s |
| list_flows | 277/s, 3.1 ms | 1,666/s | 1,940/s |
| set_state | 341/s, 2.4 ms | **1,782/s**, p50 35 ms | 794/s |
| flow noop (durable) | 35/s, 25 ms | **234/s**, p50 238 ms | 115/s |
| flow read3 (durable) | 31/s, 28 ms | **245/s** | 141/s |
| flow read3 (transient) | 32/s, 26 ms | 317/s, p99 1.2 s | **1,048/s** |

CPU (avg over the run, cgroup counters): ADB leader adb-2 137% (peak 230%), followers adb-0 102%, adb-1 90%
(peaks 172% / 158%); consoles 33% + 29%, rt-worker 31%, gateways idle. ADB memory ≤ 1.4 GiB per pod.

Writes and durable flows are 2× AWS (local disk vs EBS gp3 syncs). Transient flows are 3× slower than AWS and no
faster than durable ones: on vc-dev they go through the runtime dispatch (rt-worker), on AWS they ran on the
FlowR that took the call — the ~25 ms floor at c=1 is that hop, not ADB. ADB-only phases not run on vc-dev
(they write ~20k test records into the shared memory scope).

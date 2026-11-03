# M3 — Raft for ADB (MVP shape)

3 Nov 2026 · for the A3T cluster (flowr-system `.ai/A3T_CLUSTER_ARCHITECTURE.md` stage 3).
Limitations found on the way go to flowr-system `.ai/a3t-adb-cluster-limitations.md`
(CA-2x) and, for ADB-only ones, to `limitations.md`.

## Shape

One replica set of 3 nodes, one Raft group, one partition (S1: one replica set, one
partition — no cross-partition work). openraft `=0.10.0-alpha.36` (R1), election
150–300 ms, heartbeat 30 ms (R6).

**The Raft leader is the only active node.** It runs everything a single ADB runs
today: the sequencer, the appliers, subscriptions, watches, TTL timers, the hot cache,
FlowR's engine spaces, the platform tables. Followers are hot standbys.

```
            client (FlowR, MCP, CLI) ── any node (one Service)
                         │
         follower ── forwards HTTP / Flight ──▶ leader
                                                  │ sequencer
                                                  ▼
                                   RaftLog::append(record)  (adb_log::Log)
                                                  │ client_write, wait for commit
                       ┌──────────────── openraft ─────────────────┐
                       ▼                      ▼                    ▼
                state machine          state machine         state machine
                (leader)               (follower)            (follower)
                   │ append to node's      │                    │
                   ▼ LocalWal (p0)          ▼                    ▼
                engine reads the WAL    replica applier: catch_up(store, WAL),
                as on one node          platform and engine catch-up, purge
```

- **Raft log ≠ ADB log.** openraft keeps its own log (entries, votes) in
  `<dir>/raft`. Its state machine's only job is to append each committed ADB
  `LogRecord` to the node's `LocalWal` (`<dir>/p0`) at the record's own offset. So
  every reader of the WAL — catch-up, subscriptions, the gateway, the watcher — works
  unchanged, and the ADB offset stays contiguous (Raft's blank and membership entries
  do not appear in it).
- **The leader's sequencer appends through [`RaftLog`]**, an `adb_log::Log` whose
  `append` is `raft.client_write(record)` and returns once the record is committed
  and applied to the local WAL; `read` and `last_offset` delegate to the `LocalWal`.
- **Followers apply as they go**: a replica loop runs the same catch-up the open path
  runs (`catch_up(store, wal)`, platform, engine spaces) and the same WAL purge, so
  a follower's store is current and its WAL stays short.
- **Failover is an open.** On becoming leader a node opens the engine the way a
  restarted single node does (catch-up of the tail, hydrate, subscriptions, timers,
  watches, engine spaces) — CL-4 is handled by code that already runs on every
  restart. On losing leadership it closes the engine and returns to replica mode.
- **Routing (CL-2).** Every node serves HTTP and Flight. A follower forwards each
  request (and subscription stream) to the leader's address. Clients keep one URL
  (a Kubernetes Service over the 3 pods). On a leader change a client sees a short
  error burst and retries (FlowR's Flight client already retries `Unavailable`).

## Not in the MVP (recorded as limitations)

- Owner ≠ leader (§8): the owner is the leader.
- Group commit through Raft: the sequencer appends one record at a time.
- Raft log purge and snapshots: the Raft log is kept; a node joining or lagging past
  it needs a copy of a peer's data directory.
- Membership changes at runtime: the 3 voters are fixed at bootstrap.
- Follower reads: all reads go to the leader.
- Transaction-id dedupe across a failover: the sequencer's `committed(txn_id)` map is
  in memory.

## Configuration

| Env | Meaning |
|---|---|
| `ADB_NODE_ID` | This node's id (1, 2, 3) |
| `ADB_PEERS` | `1=http://adb-0.adb:3000,2=http://adb-1.adb:3000,3=http://adb-2.adb:3000` (HTTP base of each node; Raft RPCs at `/raft/*`) |
| `ADB_PEER_FLIGHT` | Same ids → Flight URLs, for forwarding Flight calls |
| unset | Single node, as today |

## Gate

- 3 local nodes: writes through any node; kill the leader under load; a new leader
  within 1 s; no acknowledged write lost; FlowR runs (stage-1 test pointed at the
  cluster) complete through a leader kill.
- Then vc-dev: StatefulSet `adb-0..2`, the A3T end-to-end tests.

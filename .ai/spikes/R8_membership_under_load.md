# Spike R8 — replacing nodes under load

> **Date:** 7 Oct 2026 · code: [`r1/raftproto/`](r1/raftproto/) (`raftproto r8 <dir> plain|transfer`)
> · openraft 0.10.0-alpha.36, 3 nodes in one process, local SSD

## Test

32 clients write continuously (256-byte payloads, one partition each, sequential
offsets; on error they retry the same offset against the current leader). At t = 3 s a
follower is replaced: a fresh node joins as a learner, catches up, and a membership
change swaps it in. At t = 6 s **the leader** is replaced the same way. At t = 9 s
writes stop and every final voter's partition map is checked against the last
acknowledged offset of every client.

Two modes for the leader replacement:
- **plain** — the old leader is removed directly; it steps down and the rest elect;
- **transfer** — `trigger().transfer_leader(other_voter)` first, then remove the old one.

## Results (two runs each)

| | plain | transfer |
|---|---|---|
| Follower replacement (learner → voter) | 4–24 ms | 21 ms |
| Leader replacement | 5–15 ms to commit the change | 9–10 ms (+ 2.4 ms transfer) |
| **Longest commit stall** | **956 ms, 973 ms, 1.03 s** | **76 ms, 12 ms** |
| Worst 100 ms window | 0 commits | 546 / 2,547 commits (median 3,100) |
| Commits in 9 s | ~248k | 281k–287k |
| Client retries | 32 | 20–32 |
| **Acknowledged writes missing on any final voter** | **0** (of 96 checks) | **0** (of 96 checks) |

## Findings → rules

1. **No acknowledged write was lost** in any membership change, including removing
   the leader. ☑ R8's correctness criterion.
2. **Never remove the current leader directly.** The stall equals an election (≈ one
   election timeout, 300–600 ms here, plus detection) and broke the 1 s limit in one of
   three plain runs. **Rule: transfer leadership to another voter first** — the stall
   drops to 12–76 ms. The control plane's node-replacement procedure (§11.6) does this
   step; leadership transfer is also how §11.6 "transfers leadership back" after an
   election.
3. **Unplanned leader loss** (R6) costs the same ~1 s as the plain case. With
   heartbeat 50 ms and election 300–600 ms it is borderline against R6's < 1 s
   election target; R6 tunes the timeouts on real hardware (lower timeouts trade
   against false elections under load).
4. Same-machine caveat: no real network, all logs on one disk. Re-run on the three
   servers as part of R3–R6.

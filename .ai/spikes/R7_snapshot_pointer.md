# Spike R7 — snapshot as a pointer; rejoining after a lost disk

> **Date:** 7 Oct 2026 · code: [`r1/raftproto/`](r1/raftproto/) (`raftproto r7 <dir> default|guarded`)
> · openraft 0.10.0-alpha.36, 3 nodes in one process

## Setup

The replicated state machine is ADB's partition map only — per partition
`(owner, epoch, last_offset, durable applied offset)` — built from three log record
types: `Data`, `Applied` (an owner reporting its durable `_meta` offset) and `Claim`.
Three partitions owned by nodes 1–3; 600 data batches each; partitions 0 and 1 report
applied = 600, **partition 2's applier is stuck at applied = 50** (it still needs log
index 157 onward). The leader builds a snapshot, then the follower that owns partition 2
loses its disk.

Two purge modes:
- **default** — openraft purges the log right after a snapshot
  (`max_in_snapshot_log_to_keep = 0`);
- **guarded** — openraft never auto-purges; ADB purges only below the lowest durable
  applied offset across partitions (§11.3).

## Results

| | default | guarded |
|---|---|---|
| Snapshot size | **174 bytes** | 174 bytes |
| Leader log after snapshot/purge | empty | starts at 155 (keeps partition 2's tail) |
| Replacement node (learner → voter) catch-up | 0.5 ms, via snapshot | 0.8 ms, via snapshot |
| Its partition map = leader's | yes | yes |
| Its log starts at | 1818 (after the snapshot) | 1818 |
| Entry 157 (needed by partition 2) on the new node | **no** | **no** |
| … on the leader | **no** | yes |
| … on the other follower | yes (never snapshotted) | yes |

## Findings → design rules

1. **The snapshot is a pointer.** 174 bytes for three partitions; table data never
   moves between nodes. ☑ R7's "done when".
2. **A wiped node must never rejoin under its old id.** First attempt: the same id
   came back empty; the leader still believed it held index 1817, kept heartbeating,
   and never re-sent anything — the node sat empty as a learner with the leader's
   vote. (Raft would be unsafe here anyway: a voter that forgot its log and vote can
   help elect a leader missing committed entries.) **Rule: a node with a lost or empty
   disk always returns as a new node id** — add as learner, catch up, change
   membership, remove the old id (§11.6). The control plane must enforce it (node ids
   bound to disk identity).
3. **openraft's default purge loses entries lagging partitions still need.** With
   default settings the leader dropped partition 2's whole tail right after the
   snapshot. **Rule: `max_in_snapshot_log_to_keep = u64::MAX` (no automatic purge);
   ADB calls `trigger().purge_log(upto)` with `upto` = the lowest durable applied
   offset across the group's partitions, after archiving** (§11.3, R12).
4. **A node that joined by snapshot has no log before the snapshot point**, even in
   guarded mode. If it becomes owner of a lagging partition, the entries it must
   replay come from **a peer's log** (guarded purge keeps them on the long-lived
   members) **or the archive**. `adb-log` therefore needs a "read log range from peer"
   RPC, with the archive as the fallback once every member that held the range is
   gone. Owner placement should prefer members that hold the partition's tail.

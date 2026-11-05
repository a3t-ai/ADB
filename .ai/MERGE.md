# Merges, conflicts and memory points (phase 2 ADB: DB-7, DB-10, DB-11, DB-19, DB-20)

Status 5 Nov: DB-7, DB-20, DB-10, DB-19, DB-11 built (`adb-executor/src/merge.rs`,
`tests/merge.rs`, `adb-server/tests/merge_flight.rs`). **The "As built" section at the end is
the contract; where it differs from the design text, it wins.** D7 (kind 1 must rebase) is
still open: the promote gate is on, and flipping it is one default.

5 Nov 2026. Plan: claude.ai doc "A3T core features — ADB and platform plan" (§ Two merges
and their conflicts). Decisions D1 (episodes), D3/D4 (one cluster, non-prod pod), D7–D9
open (proposed below as defaults).

## DB-7 Memory point

Every query answer carries `metadata.point = {offset, ts, at}`: the last log record the
answer includes (a write: its own record; a read: the last commit before it ran — the
answer includes at least that). `at` is the commit time as RFC 3339 with microseconds,
usable as `AS OF "<at>"` to read the same state again (SEMANTIC, PROCEDURAL; other tiers
with DB-8). A branch engine's point is on the branch's own log. No new AQL (`AT` was the
proposal; `AS OF` already takes the time).

## DB-20 Branch origin record

When a branch engine opens with an empty log, its first record is an EPISODIC row in the
branch's namespace: `{event: "branch_created", from_namespace, parent, point_offset,
point_ts, created_by}`. A branch reads its parent's history through its view already.

## DB-10 Three-way diff and conflicts

For branch `name` of `namespace` and a memory type: **base** = the parent as of the branch
point, **main** = the parent now, **branch** = the branch's own rows (values, and
`_branch_deleted` markers). Per lineage:

| Branch | Main since base | Result |
|---|---|---|
| no own row | anything | nothing to merge |
| value, lineage new on both sides (base none, main none) | — | clean add |
| value | main unchanged (same row id as base) | clean change |
| value | main changed to the same value | clean (no-op) |
| value | main changed differently | conflict `edit_edit` |
| value, base none | main added the same key with another value | conflict `add_add` |
| deleted marker | main unchanged | clean forget |
| deleted marker | main changed | conflict `edit_forget` |
| anything | main erased it (FORGET) | nothing: FORGET already reached the branch |

`branch_conflicts {namespace, name, memories?}` → `{base: {offset, ts}, main: {offset, ts},
clean: [...], conflicts: [{memory, lineage, key, type, base, main, branch}]}` (values are
payloads; `key` is the bare caller key when there is one).

## Resolutions

`{lineage, memory, take: "branch" | "main" | "edit", value?}`. Every conflict needs one;
a resolution for a clean change may override it (`main` drops it).

## DB-19 Kind 2: merge back into main

`branch_merge {namespace, name, memories: ["PROCEDURAL"] (default), main_offset,
resolutions, merged_by?}`:

1. Recompute the diff. Any conflict without a resolution → refused (`merge_conflicts`). Any
   affected lineage changed on main after `main_offset` → refused (`merge_stale`). Both answer
   with a sentence; the caller reads `branch_conflicts` again.
2. One log record on the org: for each change, a `Supersede` (new version of the lineage,
   value from the branch or the edit), a new row (`Put*`) for an add, an `Expire` for a
   branch forget (soft end; FORGET stays the only erasure); plus a control row
   `merge:<namespace>/<id>` holding, per lineage, the main row it replaced (or none).
3. Answer `{merge_id, offset, changed, added, ended}`.

`merge_revert {namespace, merge_id}`: for each lineage of the merge whose current main row
is still the merge's → a `Supersede` back to the replaced value (or `Expire` for an add,
a new version from the ended row for an end). Lineages changed since the merge are listed as
conflicts and left (refused unless `force_skip: true`, which reverts the rest).

## DB-11 Kind 1: rebase, then promote as a new agent

`branch_rebase {namespace, name, main_offset, resolutions}`: same checks as merge; then on
the branch's engine: `keep main` → the branch's own rows of that lineage are erased (the
parent's show through); `edit` → a new own row; `take branch` → kept. Then the branch's point
moves to main now (control record), so the view carries every clean main change.

`branch_promote` (built) after a clean rebase: refused while the diff has conflicts
(`merge_conflicts`) unless `{allow_stale: true}`. It writes, in the new namespace:
the view (SEMANTIC, PROCEDURAL, TOOLS) as today; a first EPISODIC record `{event:
"promoted_from", namespace, branch, point}`; and a **history layer** on the new space:
`{space: <old namespace>, mode: "history", memories: ["EPISODIC"]}` (D1: the new agent reads
the old agent's episodes, read-only). History layers are EPISODIC only, never written,
never shadowed; reads merge them after the space's own episodes.

## Codes

`merge_conflicts`, `merge_stale`, `merge_not_found`, `revert_conflicts`.

## As built: the contract for callers

Flight actions (FlowR's REST passes through to them). Refusals are gRPC errors: the message
is a sentence, the code is in `adb-code` metadata (`.ai/ERROR_CODES.md`).

| Action | Request | Answer |
|---|---|---|
| `branch_conflicts` | `{namespace, name, memories?}` (default SEMANTIC, PROCEDURAL, TOOLS) | `{base: {offset, ts}, main: {offset, ts}, clean: [Change], conflicts: [Conflict]}` |
| `branch_merge` | `{namespace, name, memories?` (default PROCEDURAL)`, main_offset, resolutions?, merged_by?}` | `{merge_id, offset, changed, added, ended}` |
| `branch_rebase` | `{namespace, name, main_offset, resolutions?}` | `{point: {offset, ts}, kept, dropped, edited}` |
| `branch_promote` | `{namespace, name, into, allow_stale?}` | `{namespace, records, from: {namespace, branch}}` |
| `merge_revert` | `{namespace, merge_id, reverted_by?, force_skip?}` | `{reverted, skipped: [lineage], offset}` |

- `Change`: `{memory, lineage, key, change: "add" | "change" | "forget", branch}`.
- `Conflict`: `{memory, lineage, key, type: "edit_edit" | "add_add" | "edit_forget", base, main,
  branch}`.
- `Resolution`: `{lineage, memory, take: "branch" | "main" | "edit", value?}`; `value` is
  required for `edit`. `memory` is `"SEMANTIC"`, `"PROCEDURAL"` or `"TOOLS"`.

What callers must know:

- **`lineage` is opaque.** It carries the memory prefix (`procedural:ops|~refund`). Send it back
  unchanged in a resolution; show `key` to people. `key` can be null.
- **`base`, `main` and `branch` are stored payloads**, not RECALL's shaped records: PROCEDURAL
  `steps` is as written (often a string), while RECALL returns it as a list. Render both.
  `base` is null for `add_add`; `branch` is null for `edit_forget`. Ts values are µs.
- **`merge_conflicts` and `merge_stale` answer with a sentence, not a list or a new diff.** On
  either, call `branch_conflicts` again; pass its `main.offset` as `main_offset`.
- `branch_promote` is refused with `merge_conflicts` while main and the branch both changed a
  record, and with `merge_stale` while main changed anything since the point (rebase first,
  or `allow_stale: true`). Branches of another branch skip this check.
- Only branches of `main` can be diffed, merged or rebased (`unsupported`). A branch in its own
  pod can't be rebased (`branch_pod_unsupported`).
- A merge makes changes only to main; the branch stays until dropped. A kind 2 merge's
  `Expire` is a soft end: a merged forget doesn't erase, and FORGET stays the only erasure.
- `merge_revert` of a merge already reverted answers `merge_not_found`.
- Promote is several steps, not one record (copy, tag on the old space, layers, origin record).
  A crash between them leaves the new space without its history layer; retrying is refused
  (`target_not_empty`). Known limit.
- History layers (`mode: "history"`) read the old space's EPISODIC as of tag
  `promoted-<into>`. FORGET in the old space reaches them, since they aren't copies. They come
  back in `layers_get`, so a UI should list them as history, not as inherited knowledge.
- Layers of the old space are copied to the new agent as they are at promote.

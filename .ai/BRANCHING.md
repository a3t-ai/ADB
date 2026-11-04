# Memory branching, layers and tags (agents as repos)

The spec covers A3T, FlowR and ADB together and lives in flowr-system:
`.ai/A3T_AGENTS_AS_REPOS_SPEC.md` (3 Nov 2026, from the design post
blog.a3t.io/agents-as-repos). ADB's part, by section of that spec:

| Spec | ADB work |
|---|---|
| §3.1 | Memory space = `(tenant_id, namespace)` *(default)* |
| §3.2 | `branches` control table; schema v5 `branch` column on every row |
| §3.3 | Overlay reads: branch rows + parent `AS OF` the branch point − overridden lineages; planner rewrite for every statement; AQL `BRANCH` / `SET BRANCH` |
| §3.4 | Writes per memory type (working not branched; episodic own stream; semantic / procedural / tools copy-on-write) |
| §3.5 | `Effect::Patch` becomes versioned while a branch or pin covers the space |
| §3.6 | Retention pins in `compaction_row` (per space); FORGET erases across branches |
| §3.7 | Branch-aware `LIKE` recall, hot cache and live-key index |
| §3.8 | Tags, diff, rebase, promote (procedural / tools) |
| §3.9 | Layers: mount / pin / fork, sealed lineages, precedence, cross-tenant grants |
| §3.10 | Memory point `{space, branch, offset, layers}` on reads, for audit |

Why a row-level overlay and not Lance-level forks: ADB's history is per row
(`valid_from` / `valid_to` / `superseded_by`, `AS OF`), the table is shared by every
tenant of the partition, and compaction deletes old Lance versions (W5).

## Design (revised 4 Nov 2026, user)

**A branch is isolated from the production agent.** Each branch is its own ADB engine:
its own WAL, its own sequencer, its own small Lance table (the *delta*). The org's
engine only logs the branch's control records (created at point X, dropped) and answers
reads of the parent's rows `AS OF` the point. Test and dev traffic never reaches the
org's sequencer, WAL, table or compaction. No Iggy: the branch log is ADB's WAL.

- **Placement**, same code: *hosted* — branch engines live in the org's ADB process,
  under `<data dir>/branches/<namespace>__<name>`, parent reads in-process; *own pod* —
  a separate ADB process (a hibernating Sandbox with its own volume), parent reads over
  Flight.
- **Reads on a branch** = the overlay: the branch's own rows (current now), then the
  parent's rows `AS OF` the branch point (the parent being the org, or another branch
  that answers the same way recursively); the nearest level wins per lineage; a
  `_branch_deleted` row hides a lineage.
- **Writes on a branch** go to the branch engine only: a new record in its table; a
  change to a parent record = an override row in the branch table with the parent's
  lineage; a delete = a marker row; a keyed STORE of a key the overlay already has is
  refused (UPDATE it).
- **Merge = promote as a new agent**: materialise the branch's view (SEMANTIC,
  PROCEDURAL, TOOLS) into a new namespace in production through the org's sequencer.
  The original agent is untouched. No merge into the parent.
- **Fork / clone** = the same materialisation without a branch.
- **Durability**: a hosted branch's engine is on the org leader's disk, not
  Raft-replicated: after an ADB failover a hosted branch is unavailable until moved or
  restored (dev and test data). Branches that must survive failovers run in their own pod.

## Build slices (revised 4 Nov 2026)

| Slice | Scope | Done when |
|---|---|---|
| B1 | Branch control records on the org log (`branch` rows), create / list / drop, AQL `BRANCH` | done (d734525) |
| B2 | Branch engines: `ParentSource` (rows of a namespace `AS OF` t: the org engine, or a branch engine recursively); `BranchHost` opening branch engines under `<data dir>/branches/`; a statement with `BRANCH "b"` runs on b's engine | A branch engine reads its parent `AS OF` the point plus its own rows; the org table and log get no branch rows |
| B3 | Branch-engine writes: STORE, UPDATE (override rows), FORGET (markers), key checks against the overlay | The developer test of the post: main and branch views stay apart through restarts, a branch of a branch |
| B4 | Retention pins at the org (compaction keeps what open branches read); FORGET on main logged for branches and applied by a branch engine before it serves; dropping a branch deletes its engine | Compaction keeps a branch's view; a forgotten record is gone from every branch |
| B5 | Promote as a new agent / clone: materialise a view into a new namespace through the org sequencer (batched) | A promoted agent's memory equals the branch view; the original agent unchanged |
| B6 | Own-pod placement: branch engine in its own ADB process, parent reads over Flight, a hibernating Sandbox; routing of `BRANCH` statements | The developer loop on vc-dev with the branch in its own pod |
| B7 | Layers (mount / pin, sealed), FlowR (agent config, branch runs, gateway side-effect guard) | The trading / compliance example |
| later | FOLLOW LINKS / LIKE / WATCH / aggregates on branches | — |

The first B2/B3 build (branch rows in the org table, schema v5 `branch` column) is kept
at tag `b2-in-org-rows` and not used: it left branch traffic on the org.

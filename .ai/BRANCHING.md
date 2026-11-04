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

## Build slices (4 Nov 2026; merging parked by the user)

| Slice | Scope | Done when |
|---|---|---|
| B1 | Schema v5 `branch` column (rows default `main`); `branches` control table (logged); AQL `BRANCH "<name>"` modifier + `branch` on HTTP / Flight requests; `CREATE BRANCH` / `LIST BRANCHES` / `DROP BRANCH` admin actions | A branch can be created at the current offset and listed; existing data reads unchanged |
| B2 | Overlay reads for RECALL / LOOKUP on SEMANTIC, PROCEDURAL, TOOLS, EPISODIC: parent rows current at the branch point (`is_current_at`), branch rows current now, minus overridden lineages; chains walk parents | Conformance reads on a fresh branch equal the parent's at the point, after 1,000 parent writes |
| B3 | Writes on a branch: STORE (new lineage, `branch` set), UPDATE of a parent lineage → override row (`overrides`), FORGET → branch tombstone; parent rows never touched | Branch writes invisible on `main`; `main` writes after the point invisible on the branch |
| B4 | Retention pins: compaction keeps versions an open branch or tag needs; FORGET on `main` erases the lineage on every branch | Compaction after 10k writes keeps the branch's view; a forgotten record is gone from every branch |
| B5 | Layers: an agent space lists `{space, mode: mount | pin(tag), memories, sealed}`; reads union the layers' SEMANTIC / PROCEDURAL / TOOLS (episodic, working never); writes that touch or shadow a sealed lineage are refused; precedence = layer order | The trading / compliance example of the post as a test |
| B6 | FlowR: `agent_configs.memory_branch` + `layers`; agent memory passes `BRANCH` and layers; branch runs: the gateway refuses outbound calls from a non-`main` branch unless the branch policy allows them; MCP tools `memory_branch_create | list | diff | rebase | tag` | The developer loop (branch, run, diff, rebase) end to end against a real A3T agent |
| later | FOLLOW LINKS / LIKE / WATCH / aggregates on branches, branch index, flattening, merge (parked) | — |

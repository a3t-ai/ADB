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

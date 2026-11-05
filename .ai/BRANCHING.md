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
| B2 | Branch engines: `ParentSource` (rows of a namespace `AS OF` t: the org engine, or a branch engine recursively); `BranchHost` opening branch engines under `<data dir>/branches/`; a statement with `BRANCH "b"` runs on b's engine | done (669e80d) |
| B3 | Branch-engine writes: STORE, UPDATE (override rows), FORGET (markers), key checks against the overlay | done (669e80d) |
| B4 | Retention pins (compaction keeps what open branches read); FORGET on main reaches every branch engine; dropping a branch deletes its engine | done (see below) |
| NS | Record ids carry the namespace (schema v5, `.ai/NS_IDS.md`): needed by B5 | done (ns-ids) |
| B5 | Promote as a new agent / clone: materialise a view into a new namespace through the org sequencer (batched) | done (see below) |
| B6 | Own-pod placement: branch engine in its own ADB process, parent reads over Flight, a hibernating Sandbox; routing of `BRANCH` statements | ADB side done (see below); the developer loop on vc-dev waits on the A3T controller (B7) |
| B7 | Layers (mount / pin, sealed), FlowR (agent config, branch runs, gateway side-effect guard) | ADB layers done (see below); FlowR part next |
| later | FOLLOW LINKS / LIKE / WATCH / aggregates on branches | — |

### B4 as built

- **Pins.** `PartitionStore::compact_pinned(history, now, pins)`: per namespace, the
  horizon for superseded versions is never later than the pin, so a version current at
  a branch point stays. The org's pins are the oldest point of each namespace's
  branches cut from `main`; a branch engine's pins are its own child branches' points
  (`Branches::pins`). Forgotten and expired rows still go: a pin keeps history, not
  forgotten data.
- **FORGET on main** tombstones every version of the lineage in the org table (so every
  branch's view of the parent hides it at once), erases it from the org's disk, then
  `BranchHost::forget` has each branch engine erase its own rows of those lineages —
  overrides and edges — from its table, log and history (`forget_lineages`). FORGET
  returns after all of them. Hosted placement only: synchronous and in-process. In
  B6 (own pod) the org must deliver the FORGET to a remote branch, and a branch pod
  that is hibernating applies it before it serves.
- **Drop** deletes the branch engine's directory; a later branch of the same name is new
  and empty.
- Limits: FORGET opens every branch engine of the org (cost grows with the number of
  branches); if one branch fails to erase, FORGET reports an error after main has
  already erased, and that branch keeps its copy (a retried FORGET finds nothing on main
  to target). Drop the branch to erase it; a durable pending-FORGET list per branch
  comes with B6, where a remote branch needs one anyway.

### B5 as built

- `branch_promote(namespace, branch, into)` and `space_clone(namespace, into)` (Engine
  trait; Flight actions `branch_promote {namespace, name, into}`, `space_clone
  {namespace, into}`), on the org's engine only.
- Source: promote reads the branch's view now (its rows, then the parent `AS OF` the
  point; deleted lineages hidden); clone reads main now.
- Copied: SEMANTIC, PROCEDURAL, TOOLS current records, as **new records at version 1**
  (no history, no links). EPISODIC stays with the original agent; WORKING is session state.
- Ids and FACT ends move to the new namespace (`.ai/NS_IDS.md`); entities are written
  before the facts about them; 500 records per log record, through the org's sequencer.
- `into` must be a valid namespace holding no records (checked under the sequencer
  lock), and not the source namespace. The original agent and the branch are unchanged;
  dropping the branch afterwards is the caller's choice.
- Limits: links (`LINK`) are not copied; provenance stays as written (who wrote the
  original record); a large space is copied in one call (no progress, no resume).

### B6 as built (ADB side)

- **Pod mode**: `adb serve-http` with `ADB_BRANCH_OF=<org Flight URL>` and
  `ADB_BRANCH=<namespace>/<name>` (`ADB_BRANCH_TOKEN`, default `ADB_AUTH_TOKEN`) is that
  branch's engine (`adb-cli/src/branch.rs`). It reads its record from the org, opens,
  applies pending FORGETs, then serves HTTP and Flight. Manifest template:
  `deploy/k8s/adb-branch.yaml` (one StatefulSet + volume per branch; hibernate = scale 0).
- **Parent reads over Flight**: `branch_parent_rows {memory, namespace, branch?, at}` on
  the org (main, or a hosted branch) returns the rows current at `at`
  (`FlightParentSource`, `adb-server/src/branch_remote.rs`); `StoredRow` is serde.
- **Placement**: `branch_place {namespace, name, url}` records the pod's Flight URL on
  the branch row. The org then forwards `BRANCH` statements to the pod (`aql` action) and
  reads its view from it for promote. Only a branch with no hosted engine yet, and no
  children, can be placed.
- **FORGET on main** goes to each placed pod (`branch_forget`); a pod that can't be
  reached gets the lineages added to its row's `pending_forgets` (logged on the org);
  the pod pulls them on start (`branch_pending`), erases, and acks
  (`branch_pending_ack`). FORGET returns once main is erased and every pod either
  erased or has it pending.
- Test: `adb-server/tests/branch_pod.rs` (org and pod in one process over Flight:
  forwarding, isolation of the org table, FORGET live and pending across a restart, the
  pod's disk erased, promote of a pod branch).
- Limits: no branch of a pod branch, no moving a hosted branch into a pod; the pod's
  data is deleted by deleting its volume (the org's `branch_drop` only drops the row);
  creating, scaling and routing pods is the A3T controller's job (B7); the org↔pod
  calls use one shared token (`ADB_AUTH_TOKEN`).

### B7 as built (ADB: tags and layers)

- **Tags** (`tag_create {namespace, name}`, `tag_list`, `tag_drop`): `tag:<ns>/<name>` →
  the log position and commit time when tagged. Compaction keeps what a tag points at
  (org pins); a tag a pinned layer reads can't be dropped.
- **Layers** (`layers_set {namespace, layers}`, `layers_get`): `{space, mode: mount | pin,
  tag?, memories: [SEMANTIC | PROCEDURAL | TOOLS], sealed}`. One level deep (a layer has
  no layers; a space used as a layer has none), never EPISODIC or WORKING.
- **Reads** with `NAMESPACE` of a space with layers carrying the memory: sealed layers'
  records first, then the space's own, then unsealed layers' — one record per key, the
  first wins. Mount = the layer now; pin = `AS OF` its tag. A layer record answers under
  the reading space's id; its `metadata.namespace` is the layer's. FORGET on the layer
  reaches every mount and pin at once (it tombstones every version).
- **Sealed**: STORE of a key a sealed layer has, or a FACT with the same subject and
  predicate as one of its facts, is refused in the inheriting space. Its UPDATE / FORGET
  only reach its own records, so a layer's can't be changed from there.
- Test: `a_sealed_layer_pinned_then_mounted` (the compliance example of the spec).
- `LIKE` (the agent's recall) ranks the merged records (`query::rank_rows`).
- Limits: `FOLLOW LINKS`, `WITH LINKS`, `PATTERN` and history reads see the space's own
  records only (no layers yet; FlowR's recall also follows links from its facts); no
  cross-tenant layers (one tenant per ADB today); a
  procedure / tool `KEY pattern_id` lookup through a layer doesn't match (ids differ).

The first B2/B3 build (branch rows in the org table, schema v5 `branch` column) is kept
at tag `b2-in-org-rows` and not used: it left branch traffic on the org.

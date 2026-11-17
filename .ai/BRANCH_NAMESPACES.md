# Branches as namespaces (17 Nov 2026)

**Decision (user, 17 Nov):** a memory branch is not its own engine. It is just a
new namespace, `<ns>@<branch>`, on the same infrastructure as every namespace
("keep simple, branch is just a new namespace, all same infra"): placed by the
namespace hash like any other, in whatever partition that is. Partition code
(owner, in-memory WAL replica, WAL segments and checkpoints on S3, takeover, epoch
fencing) applies unchanged. (A dedicated branches partition was discussed and set
aside for now.) Only the branch rules — reads see the parent at the branch point,
UPDATE/FORGET of an inherited record write an override — stay branch-specific.

Replaces: branch engines (`BranchHost`, `LocalEngine::durable_branch`,
`<pdir>/branches/…`), branch pods (B6, `placement`, `BranchRemote`), and commit
3e66286 (branch places on object storage), which those engines made necessary.

## Layout

- `ns@b` is placed by `routing::partition_of` like any namespace. It is not in the
  catalog: reads across every namespace (no NAMESPACE) don't see branch records; a
  branch is reached by naming it.
- `@` is reserved: a caller's namespace can't contain it; `NAMESPACE "ns" BRANCH "b"`
  becomes namespace `ns@b` at the engine's entry (the statement is rewritten:
  `modifiers.namespace = "ns@b"`, `branch = None`) and is routed by that.
- A branch of a branch: `ns@b1@b2`? No — the parent is in the registry row, the
  namespace is always `ns@<name>` (names are unique per namespace).

## Registry (unchanged place)

Branch rows stay in the **parent namespace's partition** (`Effect::Register`, kind
branch): create, list, drop, rebase point, pins for compaction (B4), tags and layers
(B7) all keep working there. The branch namespace's partition reads a branch's row (parent,
point) from the parent's partition (local, or the `branch_get` op) and caches it;
rebase/drop invalidate.

## Rules per namespace

`DurableWrites.parent` / `LanceReads.parent` (engine-wide `ParentView`) become
`parent_of(namespace)`: `Some(view)` for `ns@b` — source = the parent namespace's
rows as of the point (`StoreSource` when the parent is in this engine; a remote
`rows_as_of` on a cluster; a branch of a branch reads its parent branch's view).

- Rows of `ns@b` have ids `ns@b|<scope>~<key>`. The overlay matches the branch's and
  the parent's rows by the id **without its namespace** (`without_namespace`), not
  by shared ids. An override of an inherited record is written in `ns@b` with the
  same bare key; `_branch_deleted` the same.
- Answers on a branch show the parent's namespace (`ns`) and ids as the branch engine
  did (callers don't see `@`).
- FORGET on main of `ns|…k` reaches `ns@*|…k` in each branch namespace's partition (op), as B4.
- Drop: the branch's rows are forgotten (tombstones, logged), the row unregistered.
- Promote / clone / merge (DB-19) / rebase (DB-11) / conflicts read `ns@b` and `ns`.

## Steps

1. Revert 3e66286 (done, 250a9cb). `@` reserved for branches (`BRANCH_SEP`); branch
   namespaces kept out of the catalog.
2. Statement rewrite at entry (single node and cluster): BRANCH → `ns@b`.
3. Per-namespace parent in writes and reads; overlay by namespace-less id.
4. Registry lookups from the branch namespace's partition (local / op), FORGET-on-main fan-out,
   drop, promote, rebase, merge.
5. Remove branch engines, pods, `BranchHost`; tests: branches.rs, merge.rs,
   namespace_statements (branch links), a cluster test: a branch survives a takeover
   of its partition (rebuilt from S3 like any namespace).

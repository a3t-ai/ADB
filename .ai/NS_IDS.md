# NS — record ids carry the namespace (schema v5)

Decided 4 Nov 2026 (user: "Option 1"). Needed by branching B5 (promote a branch as a new
agent in a new namespace, `.ai/BRANCHING.md`), and fixes a collision that exists today.

## Problem

A record id is the caller's key, unique per (memory type, scope) (D3 S4, schema v4). The
namespace is only a column. FlowR keys an agent's memory by namespace
(`<project>--<agent>`) and every agent writes keys like `user:<owner>`, so:

- two agents' records with the same key are one record: the second keyed STORE is refused
  (`key exists`), and a FACT's entity stub is shared — a FACT in agent B about `user:alice`
  points at the stub agent A created;
- a promoted agent (same keys, new namespace) can't be written beside the original.

## Design

Keys are unique per **(namespace, scope, memory type)**. A record in a namespace has the id

```
<namespace>|<scope part>~<key>        e.g.  acme--support-bot|~user:alice
                                            acme--support-bot|team:x~user:alice
```

- `|` (`NAMESPACE_SEP`) is reserved: namespaces can't contain `|` or `~` (checked on
  write). Scope names can't contain `|` already (`IN SCOPE` charset).
- A record without a namespace keeps today's id (`user:alice`, `team:x~user:alice`).
- Parsing: an id is namespaced when the part before its first `~` contains `|`. Keys
  can't contain `~`, so `fact:a|p|b` (no `~`) is never mistaken for one.
  `unscoped(id)` (the bare key: after the first `~`) works unchanged.
- Helpers in `adb-store` `rows.rs`: `space_id(namespace, scope, key)`, `namespaced(ns, id)`
  (adds a namespace to a scoped id; idempotent), `namespace_of(id)`, `without_namespace(id)`.

### Callers

- **AQL: no change.** `NAMESPACE "…"` already exists; FlowR already sends it.
- Answers: the record's `id` carries the namespace, as it carries a named scope today
  (`acme--support-bot|~user:alice`); the payload's `key` stays bare (`user:alice`) —
  FlowR reads `data.key`.
  PROCEDURAL and TOOLS records with a `pattern_id` / `tool_id` show that (bare) as their
  id, as today; without one, the key with its namespace (`team-a|~deploy`).
- Writes: STORE, FACT (its key and its ends) build ids in the statement's namespace. A
  FACT's ends are entities **of its own namespace** (stubs created there when missing).
- Reads and UPDATE / FORGET: `KEY "k"`, `WHERE id = "k"` are qualified with the
  statement's `NAMESPACE` and `IN SCOPE` (executor, as `IN SCOPE` is today).
  **Behaviour change:** without `NAMESPACE`, a key names a record that has no namespace.
- UPDATE / FORGET by a payload field (`WHERE key = …`) touch only the statement's
  `NAMESPACE` (without one: every namespace, as reads). Before NS this didn't matter —
  a key was in one namespace — so the writes ignored `NAMESPACE`; now they apply it.
- LINK / UPDATE LINK / UNLINK use the matched records' ids: unchanged (ids are full).
- MERGE takes no `NAMESPACE` (AQL), so it reaches only records without a namespace
  (NS-L1); the executor refuses one if a planner ever passes it.
- Branch engines (B2–B4): their rows are in the branch's namespace, so ids line up with
  the parent's (overrides keep the parent's lineage).

## Migration (schema v4 → v5)

At open, after the log's tail is applied (as v4 `scope_row_ids`), one online rewrite
`namespace_row_ids`:

1. Every record row with a namespace: `lineage_id` (and `id` of a lineage's first
   version) → `space_id(namespace, scope, key)`. Later versions' ids are ULIDs and stay.
2. FACT rows: `subject_id` / `object_id` → the end key in **the fact's namespace**. An end
   that has no entity there gets a stub entity (`{key, name}`) in that namespace — the
   split of today's shared stubs.
3. LINK edge rows: ends follow their records' new ids.
4. `_meta.schema_version` = 5. New stubs are written in the same rewrite (crash-safe: all
   or nothing).

Old log records below the applied offset are not applied again. Raft followers run the
same rewrite on their own table on open (deterministic). On vc-dev this is an ADB roll
(user OK needed).

## Limits

- NS-L1: MERGE can't reach records in a namespace (no `NAMESPACE` on MERGE; its fact
  re-keying builds keys from ends' ids — both to do together).
- NS-L2: a link between records of two namespaces is allowed (ids are full) but nothing
  reads across namespaces on purpose.
- NS-L3: an entity that had real content in agent A and only facts in agent B gets a
  stub in B; its content stays with A.

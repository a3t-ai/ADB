# Known limitations — left out on purpose, to revisit

What the implementation deliberately skipped or worked around through stage D4
(25 Oct 2026). Each entry says what is missing, what it affects, and where it
should be picked up. Items fixed later move to the bottom with the commit.

## Correctness gaps

| # | Limitation | Effect | Revisit |
|---|---|---|---|
| L1 | **No embeddings are ever stored.** AQL `STORE` has no embedding input and the durable path writes `embedding: None`; only `SemanticBackend::store_with_embedding` (unused by AQL) keeps one, in memory. | `RECALL … LIKE $vec` finds nothing; vector recall is not implemented end to end. | M2 hybrid recall (risk D4): embedding on STORE, Lance vector index. |
| L2 | **Key uniqueness not enforced** (D3 S4). A second `STORE` with an existing key replaces the record. | MCP `remember` with an existing key silently overwrites. Needs a decision on the MCP behaviour before changing. | Carried C item, before the D gate. |
| L3 | **TTL expiry is a read filter only.** No timer commits the `Expire` effect (D2 §4). | Expired rows are hidden correctly but never logged, so subscribers / CQs won't see expiry events; rows stay until forget. | Carried C item; needed by stage E. |
| L4 | **No optimistic concurrency** on UPDATE (`version` in `Patch` is always 0; supersede reads the current version under the sequencer lock instead). | Fine with one writer per partition; breaks with concurrent writers or replicas. | M3 (replication) / sequencer validation. |
| L5 | **Records that shared a key across memory types before schema v3 were already one Lance row.** The v2→v3 migration keeps the survivor under the type it was last written as; the overwritten one can't be recovered. | Only data written before 25 Oct. vc-dev had one record and migrated cleanly. | None — note for anyone migrating an old data dir. |
| L6 | **`LOOKUP … KEY … FOLLOW LINKS` returns the source record**, not the linked ones. `RECALL … WHERE … FOLLOW LINKS` works. | AQL users must use the `RECALL` form. | Executor LOOKUP path; with the M2 query rework. |
| L7 | **PROCEDURAL keeps only its node fields** (`pattern_id`, `pattern`, `steps`, `severity`, `confidence`, `source`, counts, `variables`, `metadata`). Other payload fields are dropped by the backend (in the response and in matching), though Lance stores the full payload. | `WHERE owner = …` never matches a procedure. | M2, when the backend's query code is replaced. |
| L8 | **`created_at` of an updated SEMANTIC / PROCEDURAL record is its current version's commit time** (D3 §5 maps it to `commit_ts`); `accessed_at` is the last write, not the last read (D3 drops read tracking). | Callers expecting first-creation time see the last update. Lineage creation time is in the history rows if needed. | Revisit if A3T needs first-creation time (add a column or read v1). |
| L9 | **History is not readable through AQL** (no `AS OF` / `WITH HISTORY`), by design in D2 v1. The rows are kept. | Audit / time travel only by reading the table directly. | AQL v0.6 proposal (D2 §8). |
| L11 | **Lance on local disk is not power-safe by itself** (risk K8): its local writer writes data files without fsync, and its local store has fsync off. ADB now `syncfs`es the store before every log purge, so purged records are on disk — but a power failure between purges can still leave Lance's newest version referring to unsynced data, and opening a damaged table has no recovery path (rebuild from the log only works while the log is complete). | Process crashes are safe (tested); power loss or a node reboot could need manual repair. Object storage is unaffected. | Ask Lance for fsync in its local writer (with the K7 report), or always run the store on object storage; add "open the last good version" recovery. |
| L10 | **`FORGET` is logical only.** Tombstones hide rows at once, but rows stay in Lance until compaction, MemWAL WAL entries aren't GC'd, old Lance versions aren't cleaned, and log payloads aren't crypto-shredded (D2 §5 steps 3–4). | Erasure requests aren't physically complete yet. | M2 (compaction, physical forget, §14.3 crypto-shred). |

## Performance shortcuts (fine for dev-sized data)

| # | Shortcut | Effect | Revisit |
|---|---|---|---|
| P1 | **Every Lance read builds a throwaway backend** from the matching rows and runs the old backend query code over it (`reads::View`). | CPU and allocation per read grow with the rows a filter can't exclude. | M2: queries as DataFusion plans over Arrow batches; then delete `adb-backends` (agreed 25 Oct). |
| P2 | **Filter pushdown covers only** `=`, `<`, `<=`, `>`, `>=`, `IN` on top-level scalar fields, `id`, `data.x` and `KEY id`. Not pushed: `!=`, `CONTAINS`, `STARTS_WITH`, `ENDS_WITH`, `metadata.*`, nested paths, variables, nulls, other `KEY` fields. | Those predicates load every current row of the memory type. | M2 query rework. |
| P3 | **No pushdown at all for PROCEDURAL**: its conditions see the backend's node with defaults the payload may lack. | Each procedural read loads all procedures. | With L7 / M2. |
| P4 | **`FOLLOW LINKS` / `WITH LINKS` do one Lance scan per record and one lookup per link** (N+1). | Slow on records with many links. | M2: one join over edge rows. |
| P5 | **Stats count by loading every current row** (`count_current`). | `stats` cost grows with data. | Keep counts in `_meta`, or a Lance count query. |
| P6 | **`FORGET` on SEMANTIC / PROCEDURAL scans all superseded versions** of the type to honour D2 T5; PROCEDURAL builds one view per old version. | Forget cost grows with history size. | With compaction (history pruned) or a pushed-down history filter. |
| P7 | **Only `id` has a scalar index**; filters on `lineage_id`, `subject_id`, `kind`, `memory` scan. | KEY lookups by lineage and edge reads scan the table. | Add BTree / bitmap indexes maintained by the MemWAL (D5). |
| P8 | **Concurrent writes don't share a disk sync** (no pending overlay); every write waits for its own group commit. | Write latency under concurrency. | Carried C item. |
| P9 | **Release builds take ~40 min from clean** (fat LTO, 4 jobs in the pod). | Slow deploy loop after a cache wipe. | Thin LTO for dev images, or a build node. |

## Scope cut for now

| # | Item | Notes | Revisit |
|---|---|---|---|
| S1 | **One partition** (`PartitionId(0)`); `FORGET FROM ALL` / `UPDATE ALL` commit one transaction per memory type. | Not atomic across types. | M3 placement / multi-partition txns. |
| S2 | **`adb-backends` not deleted**; it still provides the WORKING / TOOLS hot tier and the query code for views. | Agreed 25 Oct. | M2. |
| S3 | **Lance R11/R12 crash tests not ported** into the suite (MemWAL durability under crash, from the spikes). | Our own crash test (SIGKILL rounds) passes; Lance-level cases aren't regression-tested. | Before the D gate. |
| S4 | **Lance MemWAL bug parked** (K7, `lancebug.md`): generations sealed during WAL replay lack `_pk_index`, breaking LSM reads. ADB-driven seals avoid it. | Must stay on ADB-driven seals until fixed upstream. | File the report; re-check on each Lance upgrade. |
| S5 | **CI deferred** (user, 24 Oct): build, workspace tests, AQL conformance, FlowR integration and the crash test run by hand before commits. | Easy to miss a regression. | Before M1 ends. |
| S6 | **`LocalEngine::replay_only` (test oracle) needs the complete log**; the log is purged below Lance's durable offset every 256 records. | Only usable in tests with < 256 records. | Fine as is; noted so nobody uses it on real data. |
| S7 | **`PATTERN` needs a bound variable** (`PATTERN $var`); a string literal doesn't parse. | Pattern recall isn't covered by the differential test. | AQL grammar / test harness with variables. |
| S8 | **Lance logs at INFO by default**, noisy at startup. | Log volume. | Set `RUST_LOG=info,lance=warn` in the manifest. |

## Outside ADB (FlowR / A3T), found while testing

| # | Item | Notes |
|---|---|---|
| F1 | `a3t/a3t-chat-engine` (stored flow) falls back to LLM `a3t/austra`, which doesn't exist (`default/Austra` does). | Chat with an agent that has no LLM fails. User to edit. |
| F2 | Agents / templates saved with `enabled_tools: []` before the 25 Oct UI fix (`4c988de`) deny every tool. | Re-save each affected agent (turn "Restrict tools" off or pick tools). |
| F3 | When an agent has no tool list, the chat engine passes `enabled_tools: null` into `system/agent-engine`'s config; whether that overrides a list set on the template wasn't checked. | Check if template-level restrictions seem not to apply. |
| F4 | `a3t-chat-engine` hard-codes `mcp_servers` `[a3t/agent-flows]` as its fallback. | Should be `${ .mcp_servers // [...] }`. |

## Fixed (moved here when done)

| Was | Fixed in |
|---|---|
| Same key in two memory types was one Lance row (data loss on restart) | `d4f0573` (schema v3) |
| Links to a forgotten record lingered until restart | `310a3d1` (D4.1) |
| Procedural `ORDER BY` / `RETURN` ignored | `18f7a25` (D4.2) |
| `UPDATE` overwrote facts in place (no history) | `4003984` (D4.3) |
| `FORGET` missed values only in old versions (D2 T5) | `b367ac4` (D4.4a) |

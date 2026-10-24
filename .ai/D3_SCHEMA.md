# D3 — Schema per kind

> **Status:** agreed 24 Oct 2026 (decisions S1–S8 as recommended) · @Sriram
> **Closes:** `RISK_BURNDOWN.md` D3.
> **Builds on:** `ARCHITECTURE.md` §5 (AQL mapping), §7 (one wide table per partition),
> §9.1 (embeddings in the log), §11.7 (`event` / `sub` / `cq` rows), §14.3 (encryption
> split), §15.1 (platform tables); [`D2_TEMPORAL.md`](D2_TEMPORAL.md) (time columns);
> the 9 Oct decision on one platform embedding model; spikes S8 (vector index), S9
> (f16 in the log), S12 (SSE-C per table).

## Decisions (agreed 24 Oct)

The rest of the document follows from these.

| # | Question | Decision |
|---|---|---|
| S1 | Record id format | **ULID as a 26-char string** (`Utf8`). Sorts by time, readable in logs and AQL results, filters as plain SQL. Today's ids are UUID v4 strings; clients treat ids as opaque, so the switch is invisible. Binary ULIDs are a later size optimisation. |
| S2 | Which payload fields are searchable? | **All of `value` is plaintext in Lance** (under the partition's SSE-C key), because AQL can filter on any field. Fields a scope declares **sealed** go into `sealed`, encrypted with the tenant key and not filterable. The log is encrypted with the tenant key as before. This refines §14.3, which assumed ADB knows which fields are "non-searched"; it can't without a declaration. Input to D7. |
| S3 | Which field is embedded and full-text indexed? | The reserved payload field **`content`** (MCP `remember` already uses it). It is copied to `text`; rows without it get no embedding and no FTS, and are found by exact and scalar predicates. |
| S4 | Stable keys | The reserved payload field **`key`** goes to the `key` column, **unique among current rows** of (tenant, scope, memory). `STORE` with a key already in use fails with `key_exists` (use `UPDATE`); after `FORGET` the key is free again. Enforced by the sequencer (uniqueness validation, §9). |
| S5 | Embedding dimension | **One dimension per deployment**, set with the platform model and recorded in `_meta`; **768** as the starting point (S8 numbers, half the size of 1,536). The model itself is an A3T / ADB choice outside this document. |
| S6 | Default named scope | When a statement has no `IN SCOPE`, the scope is the caller's agent, `agent:{agent_id}`, or `tenant:default` for callers without an agent identity. Placement hashes `(tenant_id, scope)` (§6). |
| S7 | Facts vs entities | AQL payloads are flat records, so AQL writes to SEMANTIC and PROCEDURAL produce **`entity`** rows. **`fact`** rows (subject, predicate, object) come from the SDK / MCP triple calls (`remember` with a subject and predicate) and supersede by `(subject_id, predicate)`. Both follow D2. |
| S8 | Platform tables (stage G) | **One Lance table per (partition, platform table)**, `p{n}/t_{name}.lance`, with the schema given at `create_table` plus ADB system columns prefixed `_`. Same log and sequencer as the partition's memory table. Schema changes in v1: add nullable columns only. |

## 1. The memory table

One Lance table per partition (`p{n}/memory.lance`, §7). All kinds share it; a column
a kind doesn't use is null. Timestamps are `Timestamp(Microsecond, "UTC")`.

| Column | Arrow type | Null | Meaning |
|---|---|---|---|
| `id` | Utf8 (ULID) | no | Row id; a new one per version (D2 T1) |
| `lineage_id` | Utf8 (ULID) | no | Stable record identity = first version's `id` |
| `tenant_id` | Utf8 | no | Isolation; every read filters on it |
| `scope` | Utf8 | no | Named scope (`agent:42`, `team:billing`; S6); placement key |
| `visibility` | Dictionary(Int8, Utf8): `private`, `shared`, `cluster` | no | AQL v0.5 `SCOPE` modifier |
| `namespace` | Utf8 | yes | AQL `NAMESPACE` modifier |
| `memory` | Dictionary: `working`, `tools`, `procedural`, `semantic`, `episodic` | no | AQL `FROM` / `INTO` target |
| `kind` | Dictionary: `entity`, `fact`, `edge`, `episode`, `tool`, `event`, `sub`, `cq`, `meta` | no | Row shape |
| `key` | Utf8 | yes | Stable key (S4) |
| `subject_id`, `predicate`, `object_id` | Utf8 | yes | Facts and edges (edges point at lineage ids) |
| `link_type` | Utf8 | yes | Edge type (`LINK … TYPE`) |
| `weight` | Float32 | yes | Edge weight (`LINK … WEIGHT`) |
| `value` | Utf8 (JSON object) | yes | The payload, plaintext (S2) |
| `sealed` | LargeBinary | yes | Sealed fields, encrypted with the tenant key (S2) |
| `text` | Utf8 | yes | Copy of `content` for FTS and embedding (S3) |
| `embedding` | FixedSizeList(Float32, D) | yes | f32 in the table, f16 in the log (S9); D from `_meta` (S5) |
| `commit_ts`, `valid_from` | Timestamp | no | D2 §1 |
| `valid_to` | Timestamp | yes | D2 §1 |
| `superseded_by` | Utf8 | yes | D2 §1 |
| `version` | UInt32 | no | D2 §1 |
| `ttl_at` | Timestamp | yes | D2 §4 |
| `tombstone` | Boolean | no | Forgotten or expired, until compaction removes the row (D2 §4–5) |
| `provenance` | Struct(`agent_id` Utf8, `source` Utf8, `txn_id` Utf8) | yes | Who wrote it and in which transaction |
| `log_offset` | UInt64 | no | Offset of the write; the offset guard (§9.3) |
| `epoch` | UInt64 | no | Owner epoch (fencing, §11.4) |

### Columns used per kind

● required · ○ optional · — always null.

| Column | entity | fact | edge | episode | tool | event | sub | cq | meta |
|---|---|---|---|---|---|---|---|---|---|
| `key` | ○ | ○ | — | ○ | ● (tool name) | — | — | — | — |
| `subject_id` / `predicate` | — | ● | ● (source) | — | — | ● (topic) | ● (target) | — | — |
| `object_id` | — | ○ | ● (target) | — | — | — | — | — | — |
| `link_type` / `weight` | — | — | ● / ○ | — | — | — | — | — | — |
| `value` | ● | ○ | ○ | ● | ● | ● (message) | ● | ● | ● |
| `text` / `embedding` | ○ | ○ | — | ○ | ○ | — | — | — | — |
| `valid_to` / `superseded_by` | ○ | ○ | — | — | — | — | — | — | — |
| `ttl_at` | ○ | ○ | ○ | ○ | ○ | ○ (topic TTL) | — | — | — |

- **`edge`:** `subject_id` = source lineage, `object_id` = target lineage,
  `predicate` = `"link"`, `link_type` and `weight` from `LINK`. Memory = the source's.
- **`event`:** a `PUBLISH`ed topic message; `subject_id` = topic.
- **`sub`:** `subject_id` = subscription id; `value` = `{target, filter, subscriber,
  cursor}`. The cursor moves with `UpdateSubCursor` effects, batched (§11.7).
- **`cq`:** `value` = `{query, plan_hash, class, watch_set, subscriber, last_offset}`.
- **`meta`:** one row per table, `id = "_meta"`; `value` = `{applied_offset,
  durable_batch_count, epoch, schema_version, embedding_model, embedding_dim}`.

### Reserved payload fields

`content` (S3), `key` (S4) and, from D2 §8, a later `valid_from` / `valid_to`. Any
field whose name starts with `_` is reserved for ADB and rejected in `STORE` and
`UPDATE`. Every other field is free and lives in `value`.

### Filtering on payload fields

`WHERE campaign = "summer" AND cpm_paid > 2` filters on fields inside `value`. The
executor pushes down the column predicates (`tenant_id`, `scope`, `memory`, `kind`,
current-view columns from D2 §3) to Lance, then evaluates the payload predicates over
the decoded JSON. Promoting hot payload fields to real columns, or a JSON index, is a
stage D measurement, not a v1 schema change.

## 2. Indexes

As decided in §7 and G1, with the D2 columns added:

| Index | On | Built |
|---|---|---|
| BTree | `id`, `subject_id` | MemWAL `maintained_indexes` (in the memtable too; G1) |
| BTree | `lineage_id`, `key`, `object_id`, `valid_from` | Off the hot path on flushed data; the memtable is scanned |
| Bitmap | `memory`, `kind`, `tombstone` | Off the hot path |
| FTS | `text` | Off the hot path |
| Vector | `embedding`, IVF_RQ + `refine` | Only above ~10k vector rows per partition; brute force below (S8) |

## 3. The embedding model

- **One platform model** (9 Oct), so every table has one dimension and tenants share
  tables. `_meta` records `embedding_model` and `embedding_dim`.
- **Where:** ADB's server embeds `content` **before** the transaction reaches the
  sequencer, so the partition's core never waits on a model call. The vector travels
  in the log record as f16 (S9); replay never re-embeds.
- **Changing the model** (re-embedding): per partition, in the background, read the
  current rows' `text` from the table (the log is purged below the durable offset, so
  the table, not the log, is the source), embed with the new model, and write the new
  column as a new table version; writes during the rebuild are embedded with both
  models. When a partition is done, `_meta` switches to the new model in one commit.
  A hibernated partition re-embeds on its next wake, before it serves vector recall.

## 4. Schema versions

`_meta.schema_version` starts at 1. Changes in v1 only **add nullable columns**
(Lance `add_columns`, no rewrite), applied per partition on open. Removing or
retyping a column is a table rebuild, done the same way as a re-embedding.

## 5. Platform tables (stage G)

For `call:lancedb`'s six operations (`ARCHITECTURE.md` §15.1):

- **Placement:** `hash(tenant_id, table_name)` picks the partition (S8), so one
  platform table's writes are ordered on one log and sequencer.
- **Schema:** the Arrow schema from `create_table`, plus system columns `_id` (ULID),
  `_commit_ts`, `_log_offset`, `_epoch`. Names starting with `_` are rejected in the
  user schema.
- **No temporal semantics** (D2 §7): `update` overwrites, `delete` removes; no
  lineages, TTL or embeddings. `query` takes the same SQL filter as LanceDB's
  `only_if`.
- **Encryption:** as memory tables: plaintext columns under the partition's SSE-C
  key, log encrypted with the tenant key.

## 6. Changes from today's engine

| Today (`adb-backends`) | D3 |
|---|---|
| UUID v4 ids | ULIDs (S1) |
| `Metadata { created_at, accessed_at, scope, namespace, ttl, version }` | `commit_ts` / `valid_from`, `visibility`, `namespace`, `ttl_at`, `version` columns; `accessed_at` is dropped (a read would become a write; recency for ranking comes from `commit_ts` and D4) |
| One table per memory type (ClawDB) | One table per partition, all kinds |
| Links in memory | `edge` rows |

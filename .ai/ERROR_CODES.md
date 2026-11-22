# ADB error codes (DB-18)

Every refusal ADB answers carries a stable code beside its sentence. The sentence may be
reworded; a code keeps its meaning. Source: `crates/adb-core/src/codes.rs` (`code_of`,
with one test row per refusal message).

Where the code appears:

| Path | Where |
|---|---|
| `POST /query` and the admin endpoints | `{success: false, code, error}` |
| Flight `aql` action | `{success: false, code, error}` |
| Any other Flight action | gRPC status metadata `adb-code` (the message stays the sentence) |
| Auth refusal (HTTP) | `{success: false, code: "unauthorized", error}` |

| Code | Meaning | Usual fix |
|---|---|---|
| `parse_error` | The AQL doesn't parse | Fix the statement |
| `invalid_memory_type` | The verb doesn't take that memory type | Use an allowed type |
| `missing_predicate` | FORGET / UPDATE without WHERE | Add WHERE |
| `missing_payload` | A write without data | Add the fields |
| `invalid_predicate` | A predicate the memory type can't evaluate | Change the predicate |
| `invalid_value` | A variable, date, duration, size or rule value is wrong or missing | Pass a valid value |
| `timeout` | The statement ran past its budget | Narrow it or raise TIMEOUT |
| `not_found` | The record or resource doesn't exist | — |
| `key_exists` | A keyed STORE on a key that has a current record | UPDATE it, or FORGET it first |
| `key_on_branch` | A keyed STORE on a branch for a key the branch already sees | UPDATE it on the branch |
| `merged_record` | A write to an entity that was merged into another | Write to the survivor |
| `merge_invalid` | `MERGE FROM … INTO …` can't apply (the two have no common base) | — |
| `link_target_missing` | LINK to a record that doesn't exist (any more) | — |
| `namespace_invalid` | Empty namespace, or one containing `\|` or `~` | Rename |
| `branch_name_invalid` | Branch name not 1–64 of letters, digits, `-`, `_`, `.`, or `main` | Rename |
| `branch_exists` | The namespace already has a branch of that name | Pick another name |
| `branch_not_found` | No such branch (or parent) in the namespace | — |
| `branch_has_children` | Drop of a branch with branches of its own | Drop the children first |
| `target_not_empty` | Promote or clone into a namespace that holds records | Use a new namespace |
| `promote_into_self` | Promote into the branch's own namespace | Use a new namespace |
| `tag_exists` | The namespace already has a tag of that name | Pick another name |
| `tag_not_found` | No such tag | — |
| `tag_pinned` | Drop of a tag a layer pins | Change that space's layers first |
| `layer_invalid` | A layer that is nested, itself, of a wrong memory type, a bad space, or a pin without a tag | Fix the layer |
| `sealed_key` | A write that would shadow a sealed layer's key | Don't; change the layer |
| `durable_required` | The feature needs `--data-dir` | Start ADB with a data dir |
| `not_ready` | The system partition (platform tables, engine store, table feed) is still opening on its node, e.g. after a takeover; gRPC `UNAVAILABLE` | Retry later |
| `forbidden` | The token doesn't open this operation (pool or engine token) | Use the right token |
| `unauthorized` | No or bad token | Send `Authorization: Bearer` |
| `unsupported` | Not supported (yet), or an unknown action | — |
| `internal` | Anything else: a fault, not the caller's mistake | Report it |

Merges and tables:

| Code | Meaning | Usual fix |
|---|---|---|
| `merge_conflicts` | A merge has conflicts without a resolution | Resolve each (`branch_conflicts` lists them) |
| `merge_stale` | Main changed an affected record after `main_offset` | Read the conflicts again |
| `quota_exceeded` | A new record would pass a storage quota on its namespace (DB-4) | Free space with FORGET, or raise the quota |
| `point_too_old` | The asked point is past the 90 days of versions kept, or its offset left the log with no `ts` given | Use a tag, or a newer point |
| `point_invalid` | `at` names neither a tag nor an offset, or an offset past the log's end, or asks for a point on a branch of a branch | Fix the request |
| `merge_not_found` | No such merge in the namespace, or it was already reverted | — |
| `revert_conflicts` | Records changed since the merge | `force_skip` reverts the rest |
| `append_only` | Update, delete, upsert or replace of an append-only table | Insert; purge by age |
| `transaction_invalid` | A transaction that can't be checked (empty, unknown op, change after a schema change) | Split it |
| `table_not_found` | No such platform table | Create it |
| `column_not_found` | No such column | `add_columns` |

Removed 18 Nov 2026 with the branch engines (AQL v0.13, branches are namespaces):
`branch_needs_namespace`, `branch_wrong_namespace`, `branch_pod_unsupported`,
`wrong_engine` — nothing answers with them any more.

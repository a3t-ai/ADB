//! Stable error codes (DB-18): every refusal ADB answers carries a code beside its
//! sentence, so callers (FlowR, the console) can offer the right fix without parsing
//! the words. The sentence may be reworded; a code never changes meaning.
//!
//! Most refusals are built as strings deep in the executor, so the code is derived
//! from the message here, in one place. Every message ADB can refuse with has a row in
//! the test below: a new refusal without a code shows up as `internal` there.

/// The codes, as documented in `.ai/ERROR_CODES.md`.
pub const CODES: &[&str] = &[
    "parse_error",
    "invalid_memory_type",
    "missing_predicate",
    "missing_payload",
    "invalid_predicate",
    "invalid_value",
    "timeout",
    "not_found",
    "key_exists",
    "key_on_branch",
    "merged_record",
    "merge_invalid",
    "link_target_missing",
    "namespace_invalid",
    "branch_name_invalid",
    "branch_exists",
    "branch_not_found",
    "branch_has_children",
    "target_not_empty",
    "promote_into_self",
    "tag_exists",
    "tag_not_found",
    "tag_pinned",
    "layer_invalid",
    "sealed_key",
    "merge_conflicts",
    "merge_stale",
    "quota_exceeded",
    "point_too_old",
    "point_invalid",
    "merge_not_found",
    "revert_conflicts",
    "append_only",
    "transaction_invalid",
    "table_not_found",
    "column_not_found",
    "durable_required",
    "forbidden",
    "unauthorized",
    "unsupported",
    "internal",
];

/// The stable code for a refusal message (an `AdbError`'s text or an engine's string).
pub fn code_of(message: &str) -> &'static str {
    let m = message;
    let has = |s: &str| m.contains(s);
    // Order matters: the more specific patterns first.
    if has("unauthorized") {
        "unauthorized"
    } else if has(" pool's token") || has(" is the console's (the engine token)") || has(" is not open to the ") || m.starts_with("forbidden:") || has("Query execution error: forbidden:") {
        "forbidden"
    } else if has("quota_exceeded:") {
        "quota_exceeded"
    } else if has("point_too_old:") {
        "point_too_old"
    } else if has("point_invalid:") {
        "point_invalid"
    } else if has("belongs to the sealed layer") {
        "sealed_key"
    } else if has("is pinned by") {
        "tag_pinned"
    } else if has("already has a tag") {
        "tag_exists"
    } else if has(" has no tag ") || has("needs a tag") {
        if has("pinning") { "layer_invalid" } else { "tag_not_found" }
    } else if has("layers are one level deep") || has("can't be a layer of itself") || has("a layer carries") || has("layer space ") || has("a history layer of") {
        "layer_invalid"
    } else if has("already has a branch") {
        "branch_exists"
    } else if has("branch name ") && has("use 1–64") {
        "branch_name_invalid"
    } else if has("has branches of its own: drop them first") {
        "branch_has_children"
    } else if has("no branch ") {
        "branch_not_found"
    } else if has("exists on this branch (from it or its parent)") {
        "key_on_branch"
    } else if has("already holds") && has("records: promote into a new one") {
        "target_not_empty"
    } else if has("promote into a new namespace, not") {
        "promote_into_self"
    } else if has("can't be empty or contain '|' or '~'") {
        "namespace_invalid"
    } else if has("key_exists:") {
        "key_exists"
    } else if has(" was merged into ") && has("store into") {
        "merged_record"
    } else if has("MERGE:") {
        "merge_invalid"
    } else if has("LINK: the ") && has("doesn't exist") {
        "link_target_missing"
    } else if has("merge_conflicts:") {
        "merge_conflicts"
    } else if has("merge_stale:") {
        "merge_stale"
    } else if has("merge_not_found:") {
        "merge_not_found"
    } else if has("revert_conflicts:") {
        "revert_conflicts"
    } else if has("is append-only: rows can't be changed") {
        "append_only"
    } else if has("changed shape earlier in this transaction") || has("a transaction needs at least one operation") || has("a transaction holds platform-table effects only") || has("unknown table operation") {
        "transaction_invalid"
    } else if has("no platform table") {
        "table_not_found"
    } else if has(" has no column ") || has("is not among the columns of") {
        "column_not_found"
    } else if has("column name ") || has("add columns to ") || has("purge ") && has("name the time") {
        "invalid_value"
    } else if has("needs a durable engine") || has("need a durable engine") || has("need a data directory") || has("no durable log") || has("no table to compact") {
        "durable_required"
    } else if has("requires a WHERE") || has("WHERE predicate") {
        "missing_predicate"
    } else if has("Parse error") || has("Invalid syntax") || has("Parse Error") || has("parse error") {
        "parse_error"
    } else if has("Invalid memory type") {
        "invalid_memory_type"
    } else if has("Missing predicate") || has("requires a WHERE") || has("WHERE predicate") {
        "missing_predicate"
    } else if has("Missing payload") {
        "missing_payload"
    } else if has("Invalid predicate") {
        "invalid_predicate"
    } else if has("the variable has no value") || has("LIKE $") || has("not a date") || has("unit is ") || has("rule for ") || has("a vector is a list of numbers") {
        "invalid_value"
    } else if has("timed out") {
        "timeout"
    } else if has("Record not found") || has("not found") {
        "not_found"
    } else if has("not supported") || has("isn't supported") {
        "unsupported"
    } else {
        "internal"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One row per refusal ADB can answer with (as the executor, store, platform
    /// and server words them today, values filled in).
    const CASES: &[(&str, &str)] = &[
        ("unauthorized: ADB needs Authorization: Bearer <token>", "unauthorized"),
        ("purge_finished is the console's (the engine token), not the worker pool's", "forbidden"),
        ("table_read is not open to the gateway pool's token", "forbidden"),
        ("the worker pool's token claims from worker only, not default", "forbidden"),
        ("the worker pool's token follows tasks:worker only, not run:1", "forbidden"),
        ("rule:max belongs to the sealed layer \"compliance\" of \"trading\": it can't be written or shadowed here", "sealed_key"),
        ("tag \"v1\" of \"compliance\" is pinned by \"trading\": change its layers first", "tag_pinned"),
        ("namespace \"crm\" already has a tag \"v1\"", "tag_exists"),
        ("\"compliance\" has no tag \"v9\"", "tag_not_found"),
        ("pinning \"compliance\" needs a tag", "layer_invalid"),
        ("\"compliance\" has layers of its own: layers are one level deep (B7)", "layer_invalid"),
        ("\"trading\" is a layer of another space: layers are one level deep (B7)", "layer_invalid"),
        ("\"trading\" can't be a layer of itself", "layer_invalid"),
        ("a layer carries SEMANTIC, PROCEDURAL or TOOLS, not Episodic", "layer_invalid"),
        ("layer space \"a|b\": not a valid namespace", "layer_invalid"),
        ("namespace \"crm\" already has a branch \"try\"", "branch_exists"),
        ("branch name \"bad name\": use 1–64 letters, digits, '-', '_' or '.'", "branch_name_invalid"),
        ("branch \"try\" has branches of its own: drop them first", "branch_has_children"),
        ("no branch \"try\" in namespace \"crm\"", "branch_not_found"),
        ("no branch \"main2\" in namespace \"crm\" to branch from", "branch_not_found"),
        ("Query execution error: crm|~a exists on this branch (from it or its parent): UPDATE it to change it on the branch", "key_on_branch"),
        ("Query execution error: namespace \"crm2\" already holds 3 records: promote into a new one", "target_not_empty"),
        ("promote into a new namespace, not \"crm\" itself", "promote_into_self"),
        ("Query execution error: NAMESPACE \"a|b\": a namespace can't be empty or contain '|' or '~'", "namespace_invalid"),
        ("namespace \"a~b\": can't be empty or contain '|' or '~'", "namespace_invalid"),
        ("key_exists: SEMANTIC already has a record with key 'user:alice' (UPDATE changes it; FORGET frees the key)", "key_exists"),
        ("Query execution error: e1 was merged into e2: store into e2", "merged_record"),
        ("Query execution error: MERGE: no entity alice", "merge_invalid"),
        ("Query execution error: MERGE: a was already merged into b", "merge_invalid"),
        ("Query execution error: LINK: the target record Semantic \"x\" doesn't exist (any more)", "link_target_missing"),
        ("memory branches need a durable engine (start ADB with --data-dir)", "durable_required"),
        ("tags need a durable engine (start ADB with --data-dir)", "durable_required"),
        ("embedding settings need a data directory (start ADB with --data-dir)", "durable_required"),
        ("no table to compact", "durable_required"),
        ("Parse error: expected verb at position 0", "parse_error"),
        ("Invalid syntax: unexpected token", "parse_error"),
        ("Invalid memory type 'EPISODIC' for verb 'MERGE'. Allowed: [\"SEMANTIC\"]", "invalid_memory_type"),
        ("Missing predicate: FORGET requires a WHERE clause", "missing_predicate"),
        ("Parse error: Invalid statement: FORGET requires a WHERE predicate (it never applies to a whole memory type)", "missing_predicate"),
        ("Missing payload: STORE requires data", "missing_payload"),
        ("Invalid predicate for SEMANTIC: bad field", "invalid_predicate"),
        ("Query execution error: LIKE $q: the variable has no value (pass it in `variables`)", "invalid_value"),
        ("Query execution error: LIKE $q: a vector is a list of numbers", "invalid_value"),
        ("Query execution error: AS OF: not a date (\"x\")", "invalid_value"),
        ("duration \"5y\": unit is s, m, h or d", "invalid_value"),
        ("size \"5TB\": unit is B, KB, MB or GB", "invalid_value"),
        ("Operation timed out after 100ms", "timeout"),
        ("Record not found: SEMANTIC/x", "not_found"),
        ("Operation 'x' not supported on this backend", "unsupported"),
        ("Query execution error: table a3t_errors is append-only: rows can't be changed or deleted (purge by age only)", "append_only"),
        ("Query execution error: table t changed shape earlier in this transaction: update or delete its rows in a transaction of their own", "transaction_invalid"),
        ("Query execution error: a transaction needs at least one operation", "transaction_invalid"),
        ("Query execution error: no platform table agents", "table_not_found"),
        ("Query execution error: lance: table agents has no column colour", "column_not_found"),
        ("Query execution error: lance: column name \"1x\": letters, digits and _, starting with a letter or _", "invalid_value"),
        ("merge_conflicts: 2 conflicts have no resolution: a, b", "merge_conflicts"),
        ("Query execution error: merge_stale: main changed 1 since the conflicts were read: a", "merge_stale"),
        ("Query execution error: point_too_old: offset 3 is older than the 90 days kept", "point_too_old"),
        ("point_invalid: offset 99 is past the log's end (12)", "point_invalid"),
        ("Query execution error: quota_exceeded: \"acme--*\" holds 3 records, its limit is 3", "quota_exceeded"),
        ("merge_not_found: no merge \"m1\" in namespace \"crm\"", "merge_not_found"),
        ("revert_conflicts: 1 records changed since the merge: a (force_skip reverts the rest)", "revert_conflicts"),
        ("Query execution error: a history layer of \"bot\" carries EPISODIC only and isn't sealed", "layer_invalid"),
        ("Internal error: store: disk full", "internal"),
    ];

    #[test]
    fn every_refusal_has_its_code() {
        for (msg, want) in CASES {
            assert_eq!(code_of(msg), *want, "{msg}");
            assert!(CODES.contains(want), "{want} is not in CODES");
        }
    }

    #[test]
    fn every_code_is_reachable() {
        for code in CODES {
            assert!(CASES.iter().any(|(_, c)| c == code) || *code == "unsupported", "no case answers {code}");
        }
    }
}

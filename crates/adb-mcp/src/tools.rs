//! MCP tool definitions and the AQL each tool builds.
//!
//! `aql_query`, `store_memory`, `recall_memory` and `get_stats` match ClawDB's MCP
//! server exactly, so A3T agents configured for ClawDB work unchanged. `remember`,
//! `recall`, `forget` and `link` let an LLM use memory without writing AQL.

use serde_json::{json, Map, Value};

const MEMORY_TYPES: [&str; 5] = ["WORKING", "EPISODIC", "PROCEDURAL", "SEMANTIC", "TOOLS"];

/// `tools/list` result.
pub fn tool_list() -> Value {
    let mt = json!({ "type": "string", "enum": MEMORY_TYPES });
    json!({ "tools": [
        {
            "name": "aql_query",
            "description": "Execute an AQL (Agent Query Language) query against ADB. Use this for all memory operations including STORE, RECALL, SCAN, LOOKUP, LOAD, FORGET, UPDATE, LINK, REFLECT, and PIPELINE. See the resource adb://guide/memory.",
            "inputSchema": { "type": "object", "properties": {
                "query": { "type": "string", "description": "The AQL query to execute" },
                "variables": { "type": "object", "description": "Values for {name} placeholders in the query (optional)" }
            }, "required": ["query"] }
        },
        {
            "name": "store_memory",
            "description": "Store data in agent memory. Shortcut for STORE INTO queries: a `key` already in use in SEMANTIC, EPISODIC or PROCEDURAL fails with key_exists (use UPDATE, or `remember`).",
            "inputSchema": { "type": "object", "properties": {
                "memory_type": with_desc(&mt, "Type of memory to store in"),
                "data": { "type": "object", "description": "Key-value pairs to store" },
                "scope": { "type": "string", "enum": ["private", "shared"], "description": "Visibility scope (optional)" },
                "namespace": { "type": "string", "description": "Namespace for isolation (optional)" }
            }, "required": ["memory_type", "data"] }
        },
        {
            "name": "recall_memory",
            "description": "Recall data from agent memory. Shortcut for RECALL FROM queries.",
            "inputSchema": { "type": "object", "properties": {
                "memory_type": with_desc(&mt, "Type of memory to recall from"),
                "filter": { "type": "object", "description": "Field conditions to filter by (optional)" },
                "limit": { "type": "integer", "description": "Maximum number of records to return (optional)" },
                "order_by": { "type": "string", "description": "Field to order by (optional)" },
                "order_desc": { "type": "boolean", "description": "Order descending (optional, default false)" }
            }, "required": ["memory_type"] }
        },
        {
            "name": "get_stats",
            "description": "Get ADB statistics including record counts per memory type.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "remember",
            "description": "Remember something. Stores `content` (plus optional fields) in memory. Default memory type SEMANTIC (facts); use EPISODIC for events, PROCEDURAL for how-tos, WORKING for the current task. With `subject` + `predicate` (+ `object` or `value`) it is a fact in the knowledge graph.",
            "inputSchema": { "type": "object", "properties": {
                "scope": { "type": "string", "description": "Named scope, e.g. \"team:billing\" or \"agent:42\" (optional; SEMANTIC, EPISODIC, PROCEDURAL). Without it, recall and forget cover every scope." },
                "content": { "type": "string", "description": "What to remember, in plain words" },
                "memory_type": with_desc(&mt, "Where to store it (default SEMANTIC)"),
                "key": { "type": "string", "description": "A stable key, so you can update, link or forget it later (optional). Remembering a key again updates that memory (a new version)." },
                "fields": { "type": "object", "description": "Extra structured fields, e.g. {\"account\": \"acct-9\"} (optional)" },
                "namespace": { "type": "string", "description": "Namespace for isolation (optional)" },
                "subject": { "type": "string", "description": "Make it a fact about this entity, e.g. \"person:ada\" (with predicate; SEMANTIC). A new fact for the same subject and predicate replaces the old one." },
                "predicate": { "type": "string", "description": "The fact's relation, e.g. \"works_at\" (with subject)" },
                "object": { "type": "string", "description": "The entity the fact points at, e.g. \"org:acme\" — a link you can follow (or give value)" },
                "value": { "description": "The fact's literal value, e.g. 1815 (or give object)" },
                "confidence": { "type": "number", "description": "How sure, 0-1 (the link's weight; default 1)" },
                "multi": { "type": "boolean", "description": "The predicate holds several values (e.g. knows): keep one fact per object" },
                "valid_from": { "type": "string", "description": "The fact holds from this date or time, e.g. \"2026-11-01\" (optional; before it, recall doesn't return it)" },
                "valid_to": { "type": "string", "description": "The fact holds until this date or time (optional; from then on, recall doesn't return it)" }
            }, "required": ["content"] }
        },
        {
            "name": "recall",
            "description": "Recall memories. With `like`: the memories most relevant to that text (by meaning and words), each with a `confidence` (0-1); cut by `min_confidence` and `limit`. Without it: `contains` matches the content, `filter` matches fields; newest first.",
            "inputSchema": { "type": "object", "properties": {
                "scope": { "type": "string", "description": "Named scope, e.g. \"team:billing\" or \"agent:42\" (optional; SEMANTIC, EPISODIC, PROCEDURAL). Without it, recall and forget cover every scope." },
                "memory_type": with_desc(&mt, "Where to look (default SEMANTIC)"),
                "like": { "type": "string", "description": "Rank by relevance to this text, e.g. the user's question (optional; not with contains / filter)" },
                "min_confidence": { "type": "number", "description": "With `like`: drop memories less similar than this (0-1, optional)" },
                "namespace": { "type": "string", "description": "Namespace (optional)" },
                "contains": { "type": "string", "description": "Text the content must contain (optional)" },
                "filter": { "type": "object", "description": "Field = value conditions (optional)" },
                "limit": { "type": "integer", "description": "Maximum results (default 10)" }
            } }
        },
        {
            "name": "forget",
            "description": "Forget memories that match a key or field conditions.",
            "inputSchema": { "type": "object", "properties": {
                "scope": { "type": "string", "description": "Named scope, e.g. \"team:billing\" or \"agent:42\" (optional; SEMANTIC, EPISODIC, PROCEDURAL). Without it, recall and forget cover every scope." },
                "memory_type": with_desc(&mt, "Where to forget from"),
                "key": { "type": "string", "description": "Forget the record with this key (optional)" },
                "filter": { "type": "object", "description": "Field = value conditions (optional)" }
            }, "required": ["memory_type"] }
        },
        {
            "name": "link",
            "description": "Link two memories, e.g. a fix (PROCEDURAL) applied to an incident (EPISODIC). Both are found by their key. Linking the same two memories with the same link_type again sets that link's weight.",
            "inputSchema": { "type": "object", "properties": {
                "from_type": with_desc(&mt, "Memory type of the source"),
                "from_key": { "type": "string" },
                "to_type": with_desc(&mt, "Memory type of the target"),
                "to_key": { "type": "string" },
                "link_type": { "type": "string", "description": "Relationship, e.g. \"applied_to\"" },
                "weight": { "type": "number", "description": "Strength 0–1 (optional)" }
            }, "required": ["from_type", "from_key", "to_type", "to_key", "link_type"] }
        },
        {
            "name": "unlink",
            "description": "Remove the link between two memories (only links of `link_type`, if given); both memories stay.",
            "inputSchema": { "type": "object", "properties": {
                "from_type": with_desc(&mt, "Memory type of the source"),
                "from_key": { "type": "string" },
                "to_type": with_desc(&mt, "Memory type of the target"),
                "to_key": { "type": "string" },
                "link_type": { "type": "string", "description": "Only this relationship (optional; default: every link between them)" }
            }, "required": ["from_type", "from_key", "to_type", "to_key"] }
        },
        {
            "name": "subscribe",
            "description": "Follow a topic (messages sent with `publish`) or a scope (every memory change in it). Returns a subscription id; read with `next_events`. Every subscriber of a topic sees the same messages in the same order.",
            "inputSchema": { "type": "object", "properties": {
                "topic": { "type": "string", "description": "Topic name (give this or scope)" },
                "scope": { "type": "string", "description": "Scope, e.g. \"tenant:default\" for every memory change (give this or topic)" },
                "kinds": { "type": "array", "items": { "type": "string", "enum": ["entity", "fact", "edge", "episode", "tool", "event"] }, "description": "Only these row kinds (optional)" },
                "filter": { "type": "object", "description": "Field = value conditions on each change's value (optional)" },
                "from_offset": { "type": "integer", "description": "Replay from this log offset (optional; default: from now)" }
            } }
        },
        {
            "name": "publish",
            "description": "Send a message to a topic. Every subscriber of the topic receives it.",
            "inputSchema": { "type": "object", "properties": {
                "topic": { "type": "string" },
                "fields": { "type": "object", "description": "The message, e.g. {\"order_id\": \"o-1\", \"status\": \"paid\"}" },
                "ttl": { "type": "string", "description": "How long to keep it, e.g. \"1h\", \"7d\" (optional, default 7d)" }
            }, "required": ["topic", "fields"] }
        },
        {
            "name": "next_events",
            "description": "Read a subscription's next deliveries, waiting up to `wait_seconds` for one. Pass the returned `next` as `from` on the following call, and `ack` what you have handled so a restart resumes after it.",
            "inputSchema": { "type": "object", "properties": {
                "subscription": { "type": "string" },
                "from": { "type": "integer", "description": "First offset to read (optional; default: after the last ack)" },
                "max": { "type": "integer", "description": "Most deliveries to return (default 20)" },
                "wait_seconds": { "type": "integer", "description": "How long to wait for a delivery (default 10, at most 30)" }
            }, "required": ["subscription"] }
        },
        {
            "name": "ack",
            "description": "Mark a subscription's deliveries up to `offset` as handled.",
            "inputSchema": { "type": "object", "properties": {
                "subscription": { "type": "string" },
                "offset": { "type": "integer" }
            }, "required": ["subscription", "offset"] }
        },
        {
            "name": "watch",
            "description": "Keep a query's result current: returns its result now (`{cq_id, offset, result}`); read what changes with `watch_changes`. Takes RECALL, LOOKUP, SCAN or LOAD.",
            "inputSchema": { "type": "object", "properties": {
                "query": { "type": "string", "description": "The AQL read, e.g. RECALL FROM SEMANTIC WHERE topic = \"billing\" LIMIT 20" },
                "variables": { "type": "object", "description": "Values for $name variables in the query (optional)" },
                "debounce_ms": { "type": "integer", "description": "Wait this long after a change before re-running, 50-500 (default 100)" }
            }, "required": ["query"] }
        },
        {
            "name": "watch_changes",
            "description": "A watch's next frames, waiting up to `wait_seconds`: `diff` (added / removed / changed records) or `reset` (the whole result). Pass the returned `from` on the next call; start with the `offset` `watch` returned.",
            "inputSchema": { "type": "object", "properties": {
                "cq_id": { "type": "string" },
                "from": { "type": "integer", "description": "Offset of the last frame you applied (without it: a reset)" },
                "max": { "type": "integer", "description": "Most frames to return (default 20)" },
                "wait_seconds": { "type": "integer", "description": "How long to wait for a frame (default 10, at most 30)" }
            }, "required": ["cq_id"] }
        },
        {
            "name": "unwatch",
            "description": "Stop a watch. Watches nobody reads for 24 h stop by themselves.",
            "inputSchema": { "type": "object", "properties": {
                "cq_id": { "type": "string" }
            }, "required": ["cq_id"] }
        },
        {
            "name": "unsubscribe",
            "description": "Stop a subscription.",
            "inputSchema": { "type": "object", "properties": {
                "subscription": { "type": "string" }
            }, "required": ["subscription"] }
        }
    ]})
}

fn with_desc(schema: &Value, desc: &str) -> Value {
    let mut s = schema.clone();
    s["description"] = json!(desc);
    s
}

/// An AQL literal for a JSON value.
pub fn literal(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote(s),
        Value::Array(a) => format!("[{}]", a.iter().map(literal).collect::<Vec<_>>().join(", ")),
        Value::Object(_) => quote(&v.to_string()),
    }
}

/// A double-quoted AQL string.
pub fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A memory type argument, validated and upper-cased.
pub fn memory_type(args: &Value, name: &str, default: Option<&str>) -> Result<String, String> {
    let raw = match args.get(name).and_then(Value::as_str).or(default) {
        Some(m) => m.to_ascii_uppercase(),
        None => return Err(format!("Missing required: {name}")),
    };
    if MEMORY_TYPES.contains(&raw.as_str()) {
        Ok(raw)
    } else {
        Err(format!("Invalid {name}: {raw} (expected one of {})", MEMORY_TYPES.join(", ")))
    }
}

/// A field name, restricted to AQL field paths so arguments can't inject syntax.
pub fn field(name: &str) -> Result<&str, String> {
    let ok = !name.is_empty()
        && name.split('.').all(|part| {
            let mut c = part.chars();
            matches!(c.next(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_')
                && c.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        });
    if ok {
        Ok(name)
    } else {
        Err(format!("Invalid field name: {name:?}"))
    }
}

/// `(a = 1, b = "x")` from a JSON object.
pub fn payload(fields: &Map<String, Value>) -> Result<String, String> {
    let parts = fields
        .iter()
        .map(|(k, v)| Ok(format!("{} = {}", field(k)?, literal(v))))
        .collect::<Result<Vec<_>, String>>()?;
    Ok(format!("({})", parts.join(", ")))
}

/// `WHERE a = 1 AND b = "x"` from a JSON object (empty string for no conditions).
pub fn where_eq(filter: &Map<String, Value>) -> Result<String, String> {
    if filter.is_empty() {
        return Ok(String::new());
    }
    let parts = filter
        .iter()
        .map(|(k, v)| Ok(format!("{} = {}", field(k)?, literal(v))))
        .collect::<Result<Vec<_>, String>>()?;
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_escape() {
        assert_eq!(literal(&json!("a \"b\" \\c")), r#""a \"b\" \\c""#);
        assert_eq!(literal(&json!(3)), "3");
        assert_eq!(literal(&json!([1, "x"])), r#"[1, "x"]"#);
    }

    #[test]
    fn field_names_cannot_inject() {
        assert!(field("account").is_ok());
        assert!(field("meta.pod").is_ok());
        assert!(field("x = 1 OR y").is_err());
        assert!(field("").is_err());
        assert!(field("a\"").is_err());
    }

    #[test]
    fn memory_type_validated() {
        assert_eq!(memory_type(&json!({"t": "semantic"}), "t", None).unwrap(), "SEMANTIC");
        assert!(memory_type(&json!({"t": "ALL; FORGET"}), "t", None).is_err());
        assert_eq!(memory_type(&json!({}), "t", Some("EPISODIC")).unwrap(), "EPISODIC");
    }
}

/// `watch`: `WATCH <query> [DEBOUNCE <n>ms]` (MVP W6).
pub fn watch(args: &Value) -> Result<String, String> {
    let query = args.get("query").and_then(Value::as_str).ok_or("Missing required: query")?.trim();
    let query = query.strip_prefix("WATCH ").or_else(|| query.strip_prefix("watch ")).unwrap_or(query);
    let mut aql = format!("WATCH {query}");
    if let Some(ms) = args.get("debounce_ms").and_then(Value::as_u64) {
        aql.push_str(&format!(" DEBOUNCE {ms}ms"));
    }
    Ok(aql)
}

/// `unwatch`: `UNWATCH "<cq_id>"`.
pub fn unwatch(args: &Value) -> Result<String, String> {
    let id = args.get("cq_id").and_then(Value::as_str).ok_or("Missing required: cq_id")?;
    Ok(format!("UNWATCH {}", quote(id)))
}

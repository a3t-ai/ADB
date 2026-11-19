# Using ADB memory

ADB is an agent-memory database. You talk to it in **AQL** (Agent Query Language) or
through the shortcut tools.

## Memory types

| Type | Use it for |
|---|---|
| `WORKING` | The current task: scratch state, short-lived (use `TTL`) |
| `EPISODIC` | What happened: events, conversations, incidents, with time |
| `SEMANTIC` | What is true: facts and concepts |
| `PROCEDURAL` | How to do things: steps, fixes, patterns |
| `TOOLS` | Tools you can use, with categories and rankings |

## Tools

- `remember` / `recall` / `forget` / `link` — the easy way: no AQL needed.
- `store_memory` / `recall_memory` / `get_stats` — structured shortcuts.
- `aql_query` — anything else, in AQL.

## AQL (v0.13) in a few lines

```sql
STORE INTO SEMANTIC (key = "acct-9-renewal", content = "Renewal due 1 Nov", account = "acct-9")
RECALL FROM SEMANTIC WHERE account = "acct-9" ORDER BY created_at DESC LIMIT 5
RECALL FROM EPISODIC WHERE content CONTAINS "outage" LIMIT 10
RECALL FROM SEMANTIC LIKE $q CONFIDENCE 0.6 LIMIT 5      -- $q = "when does acme renew?" (variables)
LINK FROM PROCEDURAL WHERE key = "oom-fix" INTO EPISODIC WHERE key = "inc-001" TYPE "applied_to" WEIGHT 0.9
UPDATE INTO SEMANTIC WHERE key = "acct-9-renewal" (content = "Renewal due 15 Nov")
FORGET FROM WORKING WHERE task = "draft"
```

Rules of thumb:
- Reads use `FROM`, writes use `INTO`.
- Every read is `RECALL`: `WHERE key = "k"` is an exact lookup, `LIKE "text"` (or
  `LIKE $q`) a similarity search with an optional `CONFIDENCE 0.7` minimum, no
  `WHERE` reads everything (within `LIMIT`). `RECALL FROM WORKING WINDOW LAST 10`
  reads the session's newest state; `RECALL FROM PROCEDURAL LIKE "pod OOMKilled"`
  the best-matching procedures.
- Strings are double-quoted; field names are bare (`a.b` for nested fields).
- `PIPELINE p TIMEOUT 80ms RECALL … | RECALL …` runs stages in order; `{name}` uses
  a value from an earlier stage; the last stage may be a `STORE` or `UPDATE`.
- Store a `key` field on records you will want to update, link or forget later.
  A key is unique per memory type: `STORE` with a key in use fails with
  `key_exists` (use `UPDATE`, or the `remember` tool, which updates it).
- Links: `LINK FROM … WHERE … INTO … WHERE … TYPE "t" [WEIGHT 0.9]` creates a link,
  or sets the weight of the one that exists; to change a link's type, `UNLINK` it and
  `LINK` again. `UNLINK FROM … INTO … [TYPE "t"]` removes links (records stay).
  `WITH LINKS [TYPE "t" | TYPE ("a", "b")] [WEIGHT 0.5]` and
  `FOLLOW LINKS [TYPE …] [WEIGHT …] [DEPTH n]` read links both ways.
- A relationship is a link, a value is a field: "Ada works at Acme" is a `LINK …
  TYPE "works_at"`; her age is `UPDATE INTO SEMANTIC WHERE key = "person:ada" (age = 36)`.
- Two keys for one thing: `UPDATE INTO SEMANTIC WHERE key = "person:ada"
  (aliases = ["person:ada-lovelace"])`, then `FORGET` the duplicate.
- `IN SCOPE "team:billing"` (or `"agent:42"`) puts a SEMANTIC / EPISODIC /
  PROCEDURAL record in a named scope, or limits a read, update or forget to it:
  `RECALL FROM SEMANTIC IN SCOPE "team:billing" WHERE account = "acct-9"`.
  Without it, reads see every scope. The tools take it as `scope`.

## Topics and subscriptions

Agents (and FlowR instances) can tell each other things through topics. Every
subscriber of a topic sees the same messages in the same order.

- `subscribe` with `topic` (or `scope` = `"tenant:default"` for every memory change) → a subscription id.
- `publish` with `topic` and `fields` sends a message (kept 7 days unless `ttl`).
- `next_events` with the subscription id waits for and returns deliveries; pass its `next` back as `from`.
  If it carries `reset`, you were away longer than the log keeps. For a topic,
  `reset.changes` are the messages you missed (still retained), in order; for a
  scope, they are its current state: replace what you hold for it. Then go on.
- `ack` the last offset you handled, so after a restart you continue from there.
- `unsubscribe` when done.

In AQL: `SUBSCRIBE TOPIC "orders" [WHERE status = "paid"]`,
`PUBLISH TOPIC "orders" (order_id = "o-1")`, `UNSUBSCRIBE "<id>"`. Acks, start
offsets and scope subscriptions are the tools above (the server's API), not AQL.

## Watching a query

To keep a result current instead of re-asking:

- `watch` with an AQL `RECALL` → `{cq_id, offset, result}`.
- `watch_changes` with `cq_id` and `from` = that `offset` waits for frames: `diff`
  (`added`, `removed`, `changed` records; `order` for ordered queries) or `reset`
  (the whole `result`). Pass the returned `from` back next time.
- `unwatch` when done; a watch nobody reads for 24 h stops by itself.

In AQL: `SUBSCRIBE RECALL FROM SEMANTIC WHERE topic = "billing"`, `UNSUBSCRIBE "<cq_id>"`.

The full grammar is the resource `adb://aql/grammar`.

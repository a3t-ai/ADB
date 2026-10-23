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

## AQL in five lines

```sql
STORE INTO SEMANTIC (key = "acct-9-renewal", content = "Renewal due 1 Nov", account = "acct-9")
RECALL FROM SEMANTIC WHERE account = "acct-9" ORDER BY created_at DESC LIMIT 5
RECALL FROM EPISODIC WHERE content CONTAINS "outage" LIMIT 10
LINK FROM PROCEDURAL WHERE key = "oom-fix" TO EPISODIC WHERE key = "inc-001" TYPE "applied_to" WEIGHT 0.9
FORGET FROM WORKING WHERE task = "draft"
```

Rules of thumb:
- Reads use `FROM`, writes use `INTO`.
- Strings are double-quoted; field names are bare (`a.b` for nested fields).
- `LOOKUP` needs a key or pattern and is not valid on `WORKING` (use `SCAN` or `RECALL`).
- `PIPELINE` needs a `TIMEOUT`: `PIPELINE p TIMEOUT 80ms RECALL … | RECALL …`.
- Store a `key` field on records you will want to update, link or forget later.

The full grammar is the resource `adb://aql/grammar`.

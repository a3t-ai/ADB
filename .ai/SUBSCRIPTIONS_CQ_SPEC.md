# ADB — Subscriptions and Continuous Queries Spec

Oct 7, 2026 · @Sriram

> **Status in the architecture (9 Oct 2026):** adopted as `ARCHITECTURE.md` §11.7
> (v1.4), with four adjustments recorded there — see "Reconciliation with the
> architecture" at the end of this file.

## 1. Overview

ADB adds two ways for agents to learn about changes without polling: **subscriptions** (push every committed change in a scope or topic) and **continuous queries** (push changes to the result of an AQL query). Both are fed from the committed log; neither is on the commit path.

|  | Subscription | Continuous query |
| --- | --- | --- |
| Agent asks | "Tell me when anything here changes" | "Tell me when this query's answer changes" |
| Keyed on | Scope or topic (one partition) | AQL query with `WATCH` |
| Engine work per write | Filter by scope/topic, forward | Match against watch sets, update result, emit diff |
| Cost | Near zero (cursor on the log) | Per-query state and evaluation |
| Tier | Standard | Premium |

This spec extends `ARCHITECTURE.md` v1.2: it replaces §11.7 (outbound streams) for agent-facing feeds and answers the pub/sub part of §15's open item for clustered FlowR. Exports to analytics and webhooks stay on §11.7.

## 2. Goals and non-goals

**Goals**

- Several independent FlowR instances see the same changes in the same order, with no FlowR-to-FlowR coordination.
- No table polling: change detection follows the committed log.
- Exact resume after disconnect, crash or hibernation, using log offsets.
- Hibernation stays the default: a subscription or continuous query does not keep an idle partition awake.
- The standard feature costs near nothing per subscriber; the premium feature pays for its own evaluation.

**Non-goals (v1)**

- Competing consumers / work queues (one of N workers takes each message).
- Exactly-once delivery. Delivery is at-least-once with offsets; clients deduplicate.
- A global order across partitions.
- Full incremental view maintenance (Materialize / differential-dataflow class) or arbitrary AQL under `WATCH`.
- Cross-partition continuous queries (v2, §6).

## 3. Concepts and terms

| Term | Meaning |
| --- | --- |
| Effect | One typed change in a committed batch: `PutEntity`, `PutFact`, `PutEdge`, `PutEpisode`, `SupersedeFact`, `Delete`, `Forget` |
| Offset | Position of a committed batch in a partition's log order; the unit of ordering, resume and dedup |
| Topic | A named channel, hashed like a scope: `hash(tenant_id, topic_id) mod 4096`, so one topic lives in one partition |
| Subscription | A durable registration: (tenant, partition, scope or topic, filter, cursor) |
| Cursor | The last offset a subscriber has acknowledged; one per subscriber, so readers never consume each other's messages |
| Continuous query (CQ) | A registered AQL query with `WATCH`, its current result, watch set and update strategy |
| Watch set | What a CQ depends on: static predicates from the plan, or the read-set captured during execution |
| Matcher | Code in the apply step that checks each effect against registrations |
| Gateway | The always-on tier holding agent connections (gRPC stream / MCP / WebSocket) and buffering deliveries |
| Diff | A CQ update: rows added, removed or changed, tagged with the offset that produced it |

## 4. Subscriptions (change feed)

A subscription is a cursor on one partition's committed log, filtered to a scope or topic. Every subscriber sees every matching change, in log order (broadcast).

### 4.1 API

```
SUBSCRIBE SCOPE "team:billing"            [KINDS fact, edge] [FROM OFFSET n | FROM NOW]
SUBSCRIBE TOPIC "orders.events"           [FROM OFFSET n | FROM NOW]
PUBLISH  TOPIC "orders.events" { ... }    -- an ordinary committed write of kind = event
ACK      <subscription_id> OFFSET n
UNSUBSCRIBE <subscription_id>
```

SDK equivalents: `subscribe(scope|topic, filter, from)`, `publish(topic, payload)`, `ack(sub, offset)`. Each delivery carries `(partition, offset, txn_id, effects[])`.

### 4.2 Semantics

- **Placement:** a topic or scope hashes to exactly one partition, so all subscribers share one order. A topic that must span partitions is two topics.
- **Broadcast:** each subscriber has its own cursor; acknowledging advances only that cursor.
- **Filter:** scope or topic, plus optional `kind` / `memory` / `predicate`. Filters use plaintext columns only (§9).
- **Payload:** the committed effects (new row values), not a re-read of the table.
- **Publish:** a topic message is a normal write (`kind = event`), committed through the sequencer and retained like any other row (TTL configurable per topic).
- **Start point:** `FROM NOW` = the commit tip at registration; `FROM OFFSET n` replays from n if still retained, else returns `cursor_expired` with the oldest available offset.

### 4.3 Flow

1. Agent registers via the gateway; the registration is committed to the partition's log (`kind = sub` row), so it survives restarts and hibernation.
2. On each apply, the owner's matcher checks the batch's effects against that partition's subscription index (keyed by scope/topic).
3. Matches go to the gateway holding the subscriber's connection; the gateway pushes and tracks acks.
4. Acked cursors are written back in batches (`UpdateSubCursor` effect), not per message.

### 4.4 Catch-up

A subscriber behind the live tail is served from the log (local, then archive) or, past log retention, from the table by `log_offset > cursor` scan. The gateway switches to live delivery once the reader reaches the tip.

## 5. Continuous queries

A continuous query is planned once, snapshotted at an offset, then kept current by matching each committed effect against its watch set and applying the update strategy its query class allows.

### 5.1 API

```
WATCH RECALL FROM SEMANTIC IN SCOPE "agent:42" WHERE predicate = "owns" LIMIT 20
      [DEBOUNCE 100ms] [FROM NOW | FROM OFFSET n]
-> { cq_id, offset: N, result: [...] }            -- snapshot
-> { cq_id, offset: M, added: [...], removed: [...], changed: [...] }   -- diffs
UNWATCH <cq_id>
```

### 5.2 Registration

1. Parse and plan as a normal AQL query.
2. **Classify** the plan (§5.4). Reject if outside the supported subset, with the reason.
3. **Extract the watch set** (§5.3).
4. Execute at the partition's applied offset N; return the result tagged N.
5. Commit the registration (`kind = cq` row: query text, plan hash, class, watch set, last offset, subscriber) so it survives restart and hibernation.
6. From N+1, every applied effect goes through the matcher.

Snapshot at N, then tail from N+1, is what guarantees no missed and no duplicated changes.

### 5.3 Watch sets

| Form | Built from | Used for | Kept in |
| --- | --- | --- | --- |
| Static | Plan predicates: tenant, scope, `memory`, `kind`, `predicate`, `subject_id`, time range | Filters, lookups, aggregates, top-k recall | Inverted index of CQs keyed by (scope, kind, predicate), percolator-style |
| Dynamic (read-set) | Key ranges actually read during execution | Graph traversals | Per-CQ range set, re-captured on every re-run |

An effect is tested only against CQs whose index keys it hits, so matcher cost scales with relevant CQs, not total CQs. A traversal's read-set changes as its result changes: a new edge can extend the frontier.

### 5.4 Query classes and update strategies

| Class | Example | Strategy on a matching effect | v1 |
| --- | --- | --- | --- |
| Filter / point lookup | Facts for `subject_id = X` | **Incremental**: add, remove or replace the row in the result; `SupersedeFact` = remove old + add new | Yes |
| Current facts (temporal) | Facts valid now | Incremental, plus timer on `valid_to` (§8) | Yes |
| Count / sum | Count of open tasks | Incremental | Yes |
| Min / max | Latest episode time | Incremental; on removal of the extreme, re-run | Yes |
| Graph traversal | `FOLLOW LINKS ... DEPTH 1-3` | **Re-run** on read-set intersection, debounced | Yes |
| Vector / semantic top-k | `RECALL ... LIMIT k` | **Hybrid**: score the new row against the query vector, insert if above the k-th; keep a k+m buffer; re-query when the buffer empties | Yes |
| Hybrid recall (vector + FTS + RRF) | Default `recall()` | Re-run, debounced | Yes, premium-limited |
| `REFLECT` / `AGGREGATE` analytics | Group-by over a scope | Periodic re-run | No |
| Cross-partition | Agent scope + team scope | Watcher tier merge (§6) | No (v2) |

The classifier picks one strategy at registration. Anything unclassified is rejected; that rejection list is the published "WATCH subset" of AQL.

### 5.5 Debounce and coalescing

Re-run classes coalesce matching effects for a debounce window (default 100 ms, 50-500 ms allowed) and run once. Incremental classes emit per batch, never per effect: one diff per committed batch at most.

## 6. Execution placement

Matching runs on the partition owner inside the apply step; delivery runs on an always-on gateway; cross-partition queries wait for a watcher tier in v2.

*(Diagram in the original doc: single-partition path · owner-local matcher, always-on gateway.)*

Writes go straight to the owner as today. Registrations (`SUBSCRIBE`, `WATCH`) enter through the gateway and are committed through the same sequencer, so they are ordered with the data they watch.

| Component | Runs where | Responsibility | Always on |
| --- | --- | --- | --- |
| Matcher | Partition owner, same core as the sequencer, after `merge_insert` | Test effects against subscription and CQ indexes; update CQ results; fire temporal timers | No (lives and hibernates with the partition) |
| CQ evaluation task | Partition owner, same core, off the apply path | Re-runs, top-k backfill, work over the matcher's per-batch budget | No |
| Gateway | Separate stateless tier (or a role on workers, §12) | Client connections, per-subscriber buffers, acks, catch-up reads, routing to current owners | Yes |
| Control plane | Existing | Routing map for gateways; scheduled wakes | Yes |
| Watcher tier (v2) | Separate tier | Cross-partition CQs: fan-in of partition feeds, merge, per-partition offset vectors | Yes |

Why the owner: it is the only node that applies the partition, so it sees each effect exactly once, in order, at the moment it becomes readable. A separate tier tailing the log would re-read every batch and lag apply.

## 7. Consistency, ordering and delivery

Every delivery is tagged with the partition offset that produced it; offsets are the only ordering, resume and dedup mechanism.

- **Order:** total within a partition (log order). Two FlowR instances on one topic or scope see identical sequences. No order across partitions.
- **Visibility:** deliveries are emitted at **apply**, not commit, so a pushed change is already readable on the owner (read-your-writes holds for anyone reacting to it).
- **Delivery:** at-least-once. Duplicates occur only after reconnect or owner failover; clients drop offsets ≤ their last acked offset.
- **Gaps:** a client seeing offset jump from N to M receives every change between N and M that matched its filter; offsets that did not match are simply absent. The gateway sends a heartbeat with the current tip so idle clients can advance their cursor.
- **Resume:** reconnect with `FROM OFFSET last_acked`. Subscriptions replay from log or table (§4.4). CQs replay effects if within log retention, else re-snapshot and send a `reset` frame with a full result.
- **Backpressure:** each subscriber has a bounded gateway buffer (default 10,000 deliveries or 64 MiB). On overflow the gateway drops the live stream for that subscriber and switches it to catch-up from its cursor. A slow subscriber never slows the sequencer or apply.
- **CQ result versions:** each diff carries `(offset, result_version)`; a client applying diffs out of order can detect it and request a snapshot.

## 8. Hibernation, wake and temporal boundaries

Subscriptions and CQs do not keep a partition awake: a hibernated partition receives no writes, so nothing it holds can change. Only the gateway stays up.

**Hibernate:** registrations are already committed rows (`kind = sub`, `kind = cq`), and cursors are flushed with the rest of the partition. CQ results are not persisted; they are rebuilt on wake. The gateway keeps the agent's connection open and sends heartbeats.

**Wake:** a write for the partition triggers the normal lazy wake (`ARCHITECTURE` §14.1). After the table opens, the owner loads the subscription index and CQ registrations, re-snapshots each CQ at the current offset, and the matcher is live before the first new batch applies. CQ clients receive a `reset` frame only if their last delivered offset is older than the re-snapshot; otherwise the first diff follows normally.

**Time-driven changes:** results that depend on the clock change without any write. These need explicit timers:

| Source | Example | Handling |
| --- | --- | --- |
| `valid_to` passes | "Facts valid now" | Owner timer wheel keyed by the earliest boundary per CQ; fires as a synthetic `Expire` effect |
| TTL expiry | `WORKING` entries, topic message TTL | Same timer wheel; expiry commits a `Delete` through the log so all subscribers see it |
| Relative time windows | Episodes in the last 24 h | Timer on the oldest row's exit time |

A hibernated partition with a pending boundary registers a **scheduled wake** with the control plane at `min(next_boundary)`. Without it, "current" results go stale silently. Partitions with no CQs or TTLs never schedule wakes.

**Cost of a wake caused by a subscriber:** none. Subscribers alone never wake a partition; only writes and scheduled boundaries do.

## 9. Security, tenancy and limits

A subscriber can only see what its credential could read; the matcher runs inside the owner with the same scope checks as reads.

- **Authorization:** registration checks the credential's scopes (`ARCHITECTURE` §14.4). The check is re-applied at delivery, so revoking a credential stops its streams within one batch.
- **Tenant isolation:** the subscription and CQ indexes are keyed by tenant first; an effect is never tested against another tenant's registrations.
- **Encryption:** `WATCH` and subscription filters may reference plaintext columns only (`tenant_id`, `scope`, `memory`, `kind`, `predicate`, `subject_id`, `object_id`, time columns, `text`, `embedding`). Fields inside the encrypted `value` can be delivered (decrypted with the tenant key at the gateway) but not filtered on.
- **Forget:** a `Forget` effect is delivered as a removal; CQs drop the rows; gateway buffers purge undelivered payloads for the forgotten scope.
- **Audit:** registrations, unregistrations and credential revocations are committed records (tamper-evident with the log).

**Limits (defaults, per tenant tier)**

| Limit | Standard | Premium |
| --- | --- | --- |
| Subscriptions per tenant | 1,000 | 10,000 |
| CQs per tenant | 0 | 500 |
| CQs per partition (all tenants) | — | 2,000 |
| CQ result size | — | 1,000 rows |
| Traversal depth under `WATCH` | — | 3 |
| Debounce minimum | — | 50 ms |
| Gateway buffer per subscriber | 10,000 / 64 MiB | 10,000 / 64 MiB |

**Pricing basis:** subscriptions are included. CQs bill per registered query-hour plus evaluation units (re-runs and vector scorings), metered at the matcher.

## 10. Failure and recovery

All durable state (registrations, cursors) is in the partition's log and table, so recovery is the normal partition recovery plus a re-snapshot of CQs.

| Failure | Effect | Handling |
| --- | --- | --- |
| Partition owner crashes | Deliveries for that partition pause | New owner recovers the partition (`ARCHITECTURE` §12), loads registrations, re-snapshots CQs; gateway resumes each client from its last acked offset; duplicates possible |
| Gateway node crashes | Clients disconnect | Clients reconnect to another gateway with `FROM OFFSET last_acked`; no state lost (cursors are in the partition) |
| Raft leader election | Commits pause sub-second | No action; deliveries follow apply |
| Slow or stalled subscriber | Its buffer fills | Switched to catch-up from cursor (§7); others unaffected |
| CQ evaluation too slow | Matcher lag grows | Per-partition CQ budget; over budget, the CQ is demoted to periodic re-run and the client is notified |
| Cursor older than log retention | Log replay impossible | Subscriptions: table scan by `log_offset`. CQs: re-snapshot + `reset` |
| Partition moved to another node or set | Same as owner crash, planned | Leadership/ownership handoff first, then gateway re-routes |

The matcher must never block apply. It runs after the batch's `merge_insert` succeeds, on the same core, with a time budget per batch; work over budget is queued to a per-partition evaluation task.

## 11. Data model and crates

Registrations and cursors are rows in the partition's existing Lance table, so they inherit replication, hibernation, encryption and forget with no new storage.

| `kind` | Key columns | Payload (`value`) |
| --- | --- | --- |
| `sub` | `id`, `tenant_id`, `scope` or topic, `subject_id` = subscriber | Filter, cursor offset, gateway route, created/expires |
| `cq` | `id`, `tenant_id`, `scope`, `subject_id` = subscriber | AQL text, plan hash, class, static watch set, debounce, last offset, next temporal boundary |
| `event` | `id`, `tenant_id`, `scope` = topic | Published message; TTL per topic |

New log effects: `RegisterSub`, `UpdateSubCursor`, `UnregisterSub`, `RegisterCq`, `UnregisterCq`, `Expire`.

| Crate | Change |
| --- | --- |
| `aql-parser`, `aql-planner` | `SUBSCRIBE`, `PUBLISH`, `WATCH`, `UNWATCH`; plan classifier; watch-set extraction |
| `adb-sequencer` | Matcher in the apply step; subscription index; CQ index; timer wheel |
| `adb-cq` (new) | Result state per CQ, update strategies, read-set capture, debounce |
| `adb-gateway` (new) | Client streams (gRPC, MCP, WebSocket), buffers, acks, catch-up, routing to owners |
| `adb-cluster` | Scheduled wakes; gateway routing table |
| `adb-core` | New effects and `kind` values |

## 12. Delivery plan, tests and open questions

Ship subscriptions with the single-node milestone; continuous queries follow recall indexes, since top-k CQs depend on them.

1. **Subscriptions, single node:** `SUBSCRIBE` / `PUBLISH` / `ACK`, matcher in apply, durable registrations and cursors, in-process gateway. FlowR wires its cross-instance notifications to it.
2. **Subscriptions, cluster:** gateway tier, routing by partition owner, catch-up from archive, resume across failover.
3. **CQ v1:** classifier, static watch sets, incremental filter / current-facts / count classes, timer wheel, scheduled wakes.
4. **CQ v1.1:** traversal re-run with read-sets, top-k with k+m buffer, hybrid re-run, debounce, metering.
5. **CQ v2:** cross-partition CQs on a watcher tier (fan-in of partition feeds, per-partition offset vectors).

**Tests (gates)**

- [ ] Two subscribers on one topic receive identical offset sequences under owner kills and leader elections.
- [ ] No missed or duplicated diffs across snapshot-then-tail at registration (property test against re-running the query at every offset).
- [ ] CQ result after N random batches equals a fresh execution of the query at the same offset (all v1 classes).
- [ ] Hibernate → write → wake delivers the write to a connected subscriber; no wake caused by subscribers alone.
- [ ] A `valid_to` boundary on a hibernated partition fires within the scheduled-wake tolerance.
- [ ] Slow subscriber does not raise apply latency (p99 unchanged at 10,000 buffered deliveries).
- [ ] Matcher overhead per batch with 2,000 CQs on a partition stays under the apply budget.
- [ ] `Forget` removes the scope from CQ results and gateway buffers.

**Open questions**

- Gateway as its own tier, or a role on every ADB worker with client-side routing? (Affects hop count vs operational simplicity.)
- Should ack-driven `UpdateSubCursor` writes go through the sequencer (durable, costs log volume) or be stored at the gateway with periodic checkpoint?
- Topic retention: per-topic TTL only, or also max messages / bytes?
- Matcher budget per batch and the demotion rule for slow CQs: needs a benchmark.
- Top-k buffer size m: fixed (e.g. k) or adaptive from observed removal rate?
- Is `REFLECT` under `WATCH` worth supporting as a scheduled query rather than a CQ?
- Can FlowR's run-state change notifications (§15 of `ARCHITECTURE`) use topics directly, or do they need their own table family?

## Reconciliation with the architecture (added 9 Oct 2026)

Adopted into `ARCHITECTURE.md` v1.4 with these adjustments, from the spike results:

1. **Apply is a MemWAL `put`, not `merge_insert`** (option A, §9.2–9.3). The matcher
   runs after the batch's `put` returns — the moment the rows are readable on the
   owner. Wherever this spec says "after `merge_insert`", read "after the apply `put`".
2. **Catch-up from the table is not a change log.** Past log retention, a
   `log_offset > cursor` scan returns each row's *current* version only: intermediate
   updates are collapsed, and deleted or forgotten rows are absent. Table catch-up is
   therefore delivered as a **`reset` with current state** (like a CQ re-snapshot),
   not as a replay of individual changes. Exact change replay is guaranteed only
   within log (+ archive) retention, which sets the retention promise per tier.
3. **Subscriptions do not block hibernation.** This spec supersedes the v1.3 rule
   ("a partition with an active subscription or continuous query doesn't hibernate"):
   subscribers never keep a partition awake or wake it; writes and scheduled temporal
   boundaries do.
4. **Cursor writes cost log volume.** `UpdateSubCursor` through the sequencer adds a
   committed record per ack batch per subscriber; with 1,000 subscriptions per tenant
   this needs batching across subscribers (one effect carrying many cursors) — tied to
   the spec's open question on gateway-held cursors.

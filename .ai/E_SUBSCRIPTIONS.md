# Stage E — subscriptions on a single node: decisions

> **Status:** agreed 25 Oct 2026 (user: as recommended) · **Builds on:**
> `SUBSCRIPTIONS_CQ_SPEC.md` (§4 subscriptions, §7 delivery; adopted as
> ARCHITECTURE §11.7 with the 9 Oct reconciliation), D2_TEMPORAL, D3_SCHEMA (`event`
> and `sub` rows) · **Scope:** DELIVERY_PLAN stage E = spec §12 step 1: `SUBSCRIBE` /
> `UNSUBSCRIBE` / `ACK` / `PUBLISH`, matcher at apply, durable registrations and
> cursors, in-process gateway. No continuous queries (`WATCH`, M2), no cluster
> gateway (M4).

The AQL v0.6 parser already parses all four statements; ADB's planner rejects them.
These are the choices the spec leaves open, each with a recommendation.

## Decisions

| # | Question | Recommendation |
|---|---|---|
| E1 | **Delivery transport** | **HTTP SSE**: `GET /subscriptions/{id}/events?from=<offset>`, one SSE event per delivery with `id: <offset>`, so a reconnect with `Last-Event-ID` resumes exactly. Plus **MCP tools** (`subscribe`, `publish`, `ack`, `next_events` — a long-poll returning deliveries after the cursor, with a timeout). Push over MCP (`notifications/*`) later: FlowR's MCP client doesn't consume server notifications today. |
| E2 | **Acks** — through the log or held by the gateway? | **Through the log**: each `ACK` statement commits one `UpdateSubCursor` record, so cursors survive restarts with nothing extra to persist. Clients ack in batches (every N deliveries or every second), not per delivery. Revisit with many subscribers (one effect carrying many cursors). |
| E3 | **Log retention vs the D3.5 purge** | The purge floor becomes **min(Lance durable offset, oldest live cursor)**, capped so the log never keeps more than **100,000 records** behind the tip (setting). A subscription that falls behind the cap gets `cursor_expired` with the oldest available offset; it re-subscribes `FROM NOW`, or `FROM OFFSET` within retention. **W9 (28 Oct):** past the cap the subscriber gets the spec's table catch-up instead — a `reset` with the current state, then deliveries (`cursor_expired` is gone). |
| E4 | **Where deliveries come from** | **Live:** the background applier, after each record's Lance put returns, hands the record and the rows it wrote to an in-process hub (a broadcast channel); a delivery is therefore readable when it arrives (spec §7). **Catch-up:** read the partition log from the cursor; switch to live at the tip, dropping offsets already sent. A subscriber that lags past the hub's buffer falls back to catch-up — it never slows apply. |
| E4′ | *Built (25 Oct) as a refinement of E4* | **Subscribers tail the applied log** instead of a broadcast hub: each reads records up to the apply barrier from its own position and waits on the barrier. Same semantics (delivered = readable; a slow subscriber never slows apply), one code path for live and catch-up, so sequences are identical by construction. Each subscriber reads the log itself — fine on one node; the matcher-at-apply fan-out of the spec returns with the cluster gateway (M4). Deliveries are built from the record's effects only (plus the lineage's scope / kind for patch / forget), so re-reading an offset gives the same delivery. |
| E5 | **What a delivery contains** | `{partition, offset, commit_ts, txn_id, changes[]}`, one change per matching effect: `op` (`put` / `patch` / `supersede` / `forget` / `expire` / `publish`), `memory`, `kind`, `id` (the record key), `version`, `scope`, and the row's new `value` (for `forget` / `expire`, the id only). Built from the rows the store wrote, so patches and forgets carry their scope without a lookup. Offsets that match nothing are skipped; SSE comments carry a heartbeat with the tip every 15 s. |
| E6 | **Matching** | `SCOPE "s"`: changes whose row `scope` is `s`. `TOPIC "t"`: `event` rows with `subject_id = t`. `KINDS`: the row kind. `WHERE`: AQL conditions evaluated on the row's value (no encryption yet, so the spec's "plaintext columns only" rule is moot until §14.3). Every row today has scope `tenant:default` — named scopes (`IN SCOPE …`) aren't wired into STORE yet, so `SUBSCRIBE SCOPE "tenant:default"` means "everything"; named scopes follow when STORE gets them. |
| E7 | **Registrations** | `SUBSCRIBE` commits a `Register` effect (`kind = sub` row: `subject_id` = subscription id, `value = {target, kinds, filter, cursor}`) and returns `{subscription, start_offset}`; the id is a ULID from the sequencer. `UNSUBSCRIBE` commits `Unregister`. On open, live subscriptions are loaded from the table. `FROM NOW` = the commit tip at registration. |
| E8 | **Publish** | `PUBLISH TOPIC "t" (…) [TTL d]` commits `PublishEvent` (`kind = event`, `subject_id = t`, `ttl_at` from TTL; default TTL 7 days). Returns `{id, offset}`. Event rows aren't part of any memory type's reads (WORKING … EPISODIC never see `event` / `sub` rows). |
| E9 | **Engines without a log** (`LocalEngine::new`, in-memory) | Subscription statements fail with a clear "needs a durable engine (`--data-dir`)". |
| E10 | **A3T exit check** ("two FlowR instances receive the same topic events") | Two A3T chat agents (or two FlowR instances) on the `agent-memory-mcp` server: both `subscribe` to a topic, one `publish`es, both `next_events` see the same offsets in the same order, including after an ADB restart (`FROM OFFSET` last acked). A FlowR-native topic *trigger* (start a flow per event) belongs to stage F (FlowR wiring). |

## Not in stage E

Continuous queries / `WATCH` (M2); cluster gateway, routing and failover (M4);
table catch-up as `reset` (E3); named scopes on STORE (E6); per-topic retention
beyond TTL; credential checks at delivery (no auth in ADB yet); MCP server push.

## Gate (stage E exit)

- Two subscribers on one topic receive identical offset sequences, across ADB
  SIGKILLs and restarts, resuming from their acked cursors (automated test, plus
  the crash harness extended with subscribers).
- No delivery missed or duplicated across catch-up → live switch-over (property
  test against reading the log directly).
- A slow subscriber doesn't slow writes (bounded hub buffer; apply latency unchanged).
- AQL conformance 116/116 and FlowR integration still pass; **A3T check E10**.

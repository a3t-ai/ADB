# Phase 3 ADB: DB-14, DB-12, DB-9 (built 6 Nov; see "As built" at the end)

6 Nov 2026. Same pattern as phases 1 and 2: Flight actions, stable codes, FlowR passes through.

## DB-12 Layer dependents (S)

`layers_dependents {space}` → `{dependents: [{namespace, mode, tag, sealed, memories}]}`, from the
registry (`Branches::layered_by`), which already holds it in memory. No new record type.
History layers are included with `mode: "history"`, so a caller can tell them apart. Answers VT-6:
"who inherits this space, at which tag".

## DB-9 Branch or clone at a point or tag (M)

`branch_create {…, at?}` and `space_clone {…, at?}`, with `at = {tag}` or `{offset}`. `writes.rs`
already has `branch_create_at`.

- **Not a bare time.** A branch records both offset and time, and a memory point (DB-7) already
  gives the offset. The time is read from the log record; a purged offset is refused.
- **New code `point_too_old`:** versions are kept 90 days or while pinned. Asking for older
  would silently read a partial past, so it's refused unless a tag pins it.
- **Honest limit (L9-a, until DB-8):** only SEMANTIC and PROCEDURAL have versions. EPISODIC and
  TOOLS change in place, so at a past point they read as they are now, plus or minus. A replay
  (AC-9) that depends on them isn't exact until DB-8. The answer should carry
  `exact: false` when such rows are involved, not hide it.
- **Pins:** a past-point branch pins from its point at creation; check that compaction can't have
  removed versions between the check and the pin (do both under the sequencer lock).
- **Clone** uses `materialise_from` with `rows_as_of(at)`. Same exclusions as today.

Question: should a branch at a past point be mergeable? A diff against a far-past base shows
everything main did since as main changes. Proposed: allowed; rebase handles it; nothing special.

## DB-14 Space stats, paging, feed (M)

**Stats.** `space_stats {namespace, branch?}` → per tier `{count, bytes, last_write_ts}`. `bytes`
is the payload's size, not disk (per-record disk isn't knowable). Scans current rows, which grows
with the space (W7-L1), so the Memory tab shouldn't poll it: a short cache (~10 s) per namespace,
stated in the answer. `branch_list {stats: true}` reuses it for BR-3, opt-in because N branches
means N scans.

**Paging.** `space_records {namespace, memory, branch?, after?, limit?}` → `{records, next}`, newest
first by `(commit_ts, lineage)`; `next` is an opaque cursor. New writes during paging don't shift
pages. It's still a scan per page today; paging bounds the answer, not the work. A new action
instead of AQL, so no grammar change.

**Feed.** `space_changes {namespace, branch?, memories?, from?, max?, wait_ms?}` → `{changes, reset,
next, tip}`, the same long-poll and reset as `table_changes` (DB-5). From `Put*`, `Supersede`,
`Patch`, `Forget`, `Expire`. `Forget` and `Expire` carry no namespace field, so it comes from the
lineage (`namespace_of`).

- **Redaction:** FORGET rewrites the log, so a later read returns no erased payload. A `forget`
  change names the key only (W5-L2's limit: keys remain).
- **Layers:** the feed is the space's own writes; a mounted layer's changes aren't included
  (W6-L7). Stated, not hidden.
- **Scan cost:** every read walks the whole log from `from`, across all namespaces, to filter
  one. Fine for a Memory tab at low write rates. Many open tabs would multiply it, so FlowR
  should hold one follower per namespace rather than one per viewer.

## Order

The schedule says DB-14, DB-12, DB-9. Building DB-12 first is worth considering: it's the
smallest, but only VT-6 needs it, while DB-14 serves UI-1, UI-5 and the merge summary. So the
schedule's order stands. DB-9 last: AC-9 needs it, and its exactness depends on DB-8.

## New codes

`point_too_old`.

## As built (6 Nov)

Tests: `crates/adb-server/tests/space_flight.rs`, `crates/adb-executor/tests/point_retention.rs`.

- **`at = {tag} | {offset, ts?}`.** The design said a purged offset is refused. The log only
  drops whole 64 MB segments, but a real point can still leave it, and a memory point carries
  its time anyway, so `ts` is accepted once the log no longer holds the offset. An offset the
  log still holds uses the log's time and ignores `ts`. Without `ts`, or past 90 days:
  `point_too_old`.
- **`point_invalid`** is a second new code: `at` with neither tag nor offset, an offset past
  the log's end, or `at` with a parent branch. A past point is cut from main only.
- **`exact`:** answered on `branch_create` and `space_clone` with `at`. False when the space has
  EPISODIC or TOOLS rows at that point. It says whether such rows exist, not whether they changed.
- **Stats cache:** 10 s by default, `max_age_ms` overrides, `age_ms` in the answer. Scans rows on
  a miss, as designed.
- **Paging:** an updated record moves to the top, so a reader paging through can miss it on later
  pages. New records never shift pages.
- **Feed:** a branch in its own pod is refused here (follow its feed on the pod). Links and topics
  aren't in the feed.
- **Not tested:** on a Raft cluster, or `space_stats` / `space_records` on a pod branch.

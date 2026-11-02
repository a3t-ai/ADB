# Stage H — FlowR's engine store on ADB: design

> **Status:** agreed 2 Nov 2026 (user: as recommended; the journal's history is **episodic memory**; **one ADB, polished, rather than more components** — no Iggy in the engine)  · **Builds on:** ARCHITECTURE §9 (write
> path), §11 (replication), §15 ("the rest of FlowR's state"); FlowR's
> `.ai/FLOWR_QUEUE_CHECKPOINT_SPEC.md` (the engine contract) and
> `.ai/FLOWR_ENGINE_LIMITATIONS.md` (L23, L35). · **Product decision (user, 2 Nov):**
> **ADB is FlowR's only engine store, for CE and EE** (revised 2 Nov: ADB will also have
> a CE edition). CE runs ADB **embedded** in the FlowR process; EE talks to an ADB
> server over Flight. The SQLite store is kept on FlowR's branch `engine-sqlite`
> (07b16e6) and removed from `adb-integration` once the ADB store passes (end of H-4).
> This replaces the spec's plan of Postgres for EE (spec §8, open question 7).

## What the engine store is

Everything FlowR needs to run flows durably, behind four traits in
`flowr-storage::engine` (`TaskQueue`, `RunStore`, `Journal`, `Notifier`):

| Part | What it holds | How it changes |
|---|---|---|
| Tasks | Start / resume / timer tasks per pool, with visibility time, lease and dedupe key | Enqueue, claim (lease), heartbeat, ack, nack |
| Runs | One row per run: phase, owner, **epoch**, requests (cancel, suspend), timestamps | Start, claim (new epoch), renew, release, requests |
| Journal | Per run, ordered records: steps, timers, waits, approvals, effects | Append, fenced by the run's epoch; records with the same identity are skipped |
| Waits | Open `listen` waits and approval questions | Opened by a run, closed by an event, an answer or a timeout |
| Events seen | Event ids delivered per project | Kept until the retention purge |
| Outbox, checkpoints | Rows for a drain; interpreter snapshots (not used yet) | Appended with a step |

Every operation is **atomic across parts**. For example, a claim leases a task and
takes ownership of its run under a new epoch in one step, and an event closes waits,
appends to several journals and enqueues resumes in one step. A write from a worker
whose epoch is stale is refused (`Fenced`). Today two stores pass the same conformance
suite (`flowr_storage::conformance`, 20 checks): the in-memory reference and SQLite
(CE, and EE's console). The SQLite store serves runtime pods through the console's
gateway (port 3003), so EE has one console and the console is a single point of
failure (L23, L35).

ADB's platform tables can't hold this: they offer insert, update and delete by filter,
with no transaction across rows and no compare-and-set. ADB's **sequencer and log**
can, because every write already goes through one sequencer, is validated there, and
commits as one log record (§9). The engine becomes a third table family on that path.

## Decisions

| # | Question | Recommendation |
|---|---|---|
| H1 | **One store, two ways to reach it** | **ADB only.** CE: ADB **embedded** in the FlowR process (ADB's engine and memory crates as a library, data in `<data>/adb`), so CE stays one process with no setup, and FlowR's tests run it in-process. EE: the console and runtimes reach an **ADB server** over Flight (H6), selected by the ADB engine resource. Both run the same `EngineCore` on the same ADB write path, so there is one implementation. **No SQLite fallback** (revised 2 Nov): the SQLite store lives on the branch `engine-sqlite` and leaves `adb-integration` when H-4's gate passes. An EE console whose ADB is unreachable retries with backoff and its runs wait; it never falls back to a local store. |
| H2 | **One implementation of the rules** | Move the in-memory store's logic into a pure **`EngineCore`** in `flowr-storage` (no I/O, no clock, no random ids: `now` and new ids are passed in). The memory store becomes `EngineCore` behind a mutex, and **ADB embeds the same `EngineCore`** (path dependency on `flowr-system/shared/flowr-storage`, which pulls only serde, uuid, chrono and async-trait). The rules are written once; the conformance suite checks the ADB wiring, not a second copy of the semantics. SQLite keeps its own SQL implementation, and the suite keeps it in step. |
| H3 | **What goes in the log** | **What changed, not the request**, as ADB does for memory writes (§9: validate in the sequencer, log typed effects). The sequencer runs the operation on `EngineCore` (`exec(op, now, ids) → (reply, changes)`) and logs the changes as one record: `Effect::Engine { space, changes }`, with `changes` opaque bytes to `adb-core` (JSON of `flowr-storage` change types). It applies them, then replies. Replay, catch-up and followers only ever `apply(change)`, so they never re-run decisions and never depend on the clock. Each change is small and explicit: `TaskEnqueued`, `TaskLeased`, `TaskAcked`, `RunStarted`, `RunOwned{epoch}`, `RunReleased`, `JournalAppended{run, seq, record}`, `WaitOpened`, `WaitClosed`, `EventSeen`, and so on. |
| H4 | **Heartbeats** | **Not logged.** A `heartbeat` or `renew` extends a lease in the leader's memory only, and is answered from there. After an ADB restart or a leader change, every lease and ownership gets one fresh lease period from the moment the engine opened. Safety doesn't depend on lease times: a worker that lost its run is still refused by the epoch check on its next write. The cost is liveness only, and only after an ADB restart: a run whose worker died just before it waits one more lease period. This removes most of the log volume (each live run renews every few seconds). |
| H5 | **Where state lives** | **Hot state in memory** inside the sequencer: tasks, runs, waits, events seen, outbox, and the journals of live runs. This is what operations are decided against, and it is small. **The journal's history is EPISODIC memory**: every journal record is also written, in the same log record as the engine change, as an episode (`PutEpisode`). Its fields: tenant `flowr:<space>`, namespace the FlowR project, scope the run id, key the sequence number. Its value is `{seq, epoch, at, kind, step_key, record}`; its text is a one-line summary ("step /charge completed in 120 ms"); it has **no embedding and no TTL**. So a run's history is one more kind of memory, readable with AQL and later by agents (KBLaM direction, H15), with no second store. A finished run's journal leaves the core a few minutes after it ends, and is read back from EPISODIC (scope = run, ordered by key). **Snapshots:** the hot state is written to `engine/<space>/snapshot` every 10,000 records or 60 s, whichever comes first. On open: load the snapshot, then replay the log after it. The log floor includes the snapshot's offset. |
| H6 | **Wire** | **Arrow Flight**, like every native FlowR → ADB call (G5). `do_action("engine")` with `{space, op, args}`, using exactly the op names and argument shapes of FlowR's gateway today (`enqueue`, `claim_runs`, `commit_step`, `deliver_event`, …), and errors that keep their kind (`fenced`, `lease_lost`, `not_found`, `conflict`). The FlowR client is the gateway client with a different transport. Notifications go over `do_get` with ticket `{"engine_subscribe": topic}`, a stream of JSON notices, reconnected by the client and best effort as today (claimers also poll). |
| H7 | **Who talks to ADB** | **The console and runtime pods, directly.** Runtimes get the same ADB engine resource (the controller already passes `A3T_ADB_RESOURCE` to runtimes; it also passes the engine token, H8), so a console restart no longer interrupts runs on runtimes (closes L35). The console's engine gateway (3003) stays for the SQLite fallback, and FlowR keeps sending chat and agent stream chunks to the console over the gateway's `/stream` either way. |
| H8 | **Access** | Engine operations need an ADB token with the **`engine` scope**, held by FlowR consoles and runtimes only (a new secret, `adb-engine`). Tokens used for agent memory (`adb-auth`) can't read or write the engine. Runs carry principals and journals carry step inputs and outputs, so this is not memory-level data. |
| H9 | **Several FlowR installs on one ADB** | Each FlowR installation uses its own **space** (`config.space`, default `default`), with its own core, snapshot and journal dataset. Spaces share nothing, so one ADB can serve vc-dev and a staging console without mixing runs. |
| H10 | **Live pools** | The router sends runs only to pools with a live worker; today the gateway sees the claims. With ADB, the core records each pool's last claim time (in memory, not logged) and answers a new op, `live_pools`. It's added to the `TaskQueue` contract with a default, so SQLite answers it too, and the console's runtime view reads it from the store instead of the gateway. |
| H11 | **Moving existing runs** | `adb import-flowr-engine <flowr.db> [--space s]` (ADB CLI) copies tasks, runs, journals, waits and events seen from a FlowR SQLite file into a space, once, with the console stopped (vc-dev, and any CE user upgrading). Re-runnable (`--replace`). No export: there is no SQLite to go back to (the branch keeps the code). |
| H12 | **Not in this stage** | Raft. Stage H works on one ADB node, and replication comes with ADB M3 for free, because the engine is only log effects plus `apply`. More than one console replica also waits: chat and agent streams live in the console's memory (`token_stream`), so replicas need those streams over ADB topics first (a separate step). |
| H13 | **Notifications and live views** | The `Notifier` (task available on a pool, run changed) is ADB's subscription transport: a Flight stream fed from the engine's in-memory broadcast. Notices are not logged: they are hints, and claimers also poll. The same stream carries a run's new journal records, so the console's run views stream instead of polling (closes L25 once the UI uses it). |
| H14 | **No extra components** | One ADB, polished (user). Iggy, Postgres and Dragonfly are not part of the engine. A high-volume export of lifecycle and metering events (L5's outbox) can be added later as an optional sink, if a customer needs one. |
| H15 | **Who may read run history** | At first only the engine scope (H8): the `flowr:*` tenants are invisible to agent tokens, and AQL `FORGET`, `UPDATE`, TTL, consolidation and compaction leave them alone. Removal is retention only: the engine's purge physically deletes a finished run's episodes. Letting agents recall run history (their own runs, or a project's) is a later, explicit grant. |
| H16 | **One Arrow / Lance stack, the latest** | Both repos on the **same, newest compatible versions** (user, 2 Nov): **Lance / lance-index =14.0.2, LanceDB 0.41.0, Arrow (and arrow-flight, -ipc, -cast) 58.4.1, DataFusion 54.1.0**, checked on crates.io 2 Nov 2026. Arrow 60 and DataFusion 55 are out, but Lance 14.0.2 and LanceDB 0.41 require Arrow ^58 and DataFusion ^54, so 58/54 is the newest set that fits together. Today ADB is on Lance 12 (Arrow 58.4.0, DataFusion 54.1) and FlowR on LanceDB 0.26 (Arrow 57, DataFusion 51). Bumped together in H-0, then kept in step: a check fails the build when the two lockfiles disagree on these crates. |

## How an operation runs

1. FlowR calls `do_action("engine", {space, op, args})`.
2. The server takes the sequencer lock (the same lock platform and memory writes
   take), runs `EngineCore::exec(op, now, fresh ids)` on the space's core, and gets
   the reply and the changes.
3. If there are no changes (a read, a refused write, a duplicate), it replies at once.
   Otherwise it appends one log record (`Effect::Engine{space, changes}` plus a
   `PutEpisode` per new journal record, fsynced), applies the changes to the core,
   lets the memory applier write the episodes, and wakes subscribers.
4. It replies with the result, or with the error kind (`Fenced` and the others).

Reads (`get`, `list`, `read_from` for a live run, `pending`, `approvals`) are answered
from the core without the log. `read_from` for a finished, evicted run reads its episodes from EPISODIC.
`list` with filters over history (run lists, newest first, by project, flow or status)
reads the in-memory run table. Run rows are small and are purged by retention
(30 days), so they stay in memory. The journal's episodes are the only large data.

## Performance target

The SQLite numbers from FlowR's bench (`engine_sqlite::tests::bench`) are the bar:
start + claim + release per run, step commit p50 and p99, and steps per second with 8
concurrent runs. **The ADB store, measured from FlowR over Flight in the cluster, must
be within 2× of SQLite on all of them.** Each write is one fsynced log append, like
SQLite with `synchronous=FULL`, plus about 0.2 ms of Flight. If it isn't within 2×,
first group-commit concurrent operations in one log record (§9's 2–5 ms window);
don't drop durability.

## Gate (stage H exit)

- **Episodic:** a finished run's journal reads back from EPISODIC identically (same records, same order) to what the core held, and AQL over the `flowr:<space>` tenant sees it; agent tokens don't; `FORGET` can't touch it; retention removes it.
- **Contract:** `flowr_storage::conformance` passes against the ADB store over Flight
  (all checks, including fencing, waits, approvals and event dedupe), and against the
  refactored memory store and SQLite.
- **FlowR:** the engine test suites (`engine_runs`, `engine_resume` crash tests,
  `engine_debug`, `engine_events`, `engine_event_triggers`, `engine_agent`,
  `engine_approval`, `run_tools`) pass with the console and workers on the ADB store.
- **ADB crash:** SIGKILL ADB in the middle of runs (steps, a timer, a listener, an
  approval). After restart every run continues from its journal, none is lost or runs
  twice, and the snapshot plus log replay give the same state as before.
- **Performance:** within 2× of SQLite (above).
- **vc-dev:** after `import-flowr-engine` and switching the console and runtimes to
  the ADB resource, the `a3t-test` flows pass (listen and emit, timeout, event
  trigger, durable approval), a run executes on `rt-worker`, and restarting the
  console pod doesn't interrupt a run on `rt-worker`.
- **CE:** a fresh CE install runs flows durably on the embedded ADB with no setup, and
  survives a restart mid-run.

## Implementation plan

Each step ends green on its own tests and is committed; nothing reaches vc-dev before
H-6, which needs your OK to roll. Sizes are rough working days.

### H-0 — one stack, latest versions (both repos, ~2 days)

> **Done 2 Nov 2026** — ADB `cdc9058`, FlowR `743be77`. Every ADB test and the full `check-all` gate pass, and so do FlowR's suites; data written by Lance 12 (ADB memory and platform tables) and by LanceDB 0.26 (FlowR) opens and reads the same. Found on the way: Lance 14 reads `"…"` in SQL filters as a column, so FlowR's `PlatformDb` converts A3T's double-quoted values (`flowr_storage::filter::compat`); and a canvas sandbox's runs were routed to a pool it doesn't claim (fixed; the runtime e2e passes). EE's `flowr/Cargo.lock` is updated in the working tree, not committed (user's staged file).

1. **ADB:** Lance and lance-index `=12.0.0` → `=14.0.2`; Arrow to 58.4.1 (lockfile).
   Fix API changes (MemWAL applier, merge-insert, compaction, scalar indexes, the
   `LANCE_BYPASS_SPILLING` workaround K10, which may no longer be needed). Exit: ADB's
   tests, AQL conformance, `gate --quick` and the crash tests pass; the 1,000-kill
   crash run is repeated before vc-dev.
2. **FlowR** (`flowr-ce` workspace and EE's `flowr-server`): LanceDB 0.26 → 0.41.0,
   every Arrow crate 57 → 58.4.1 (including the `arrow-flight` client that talks to
   ADB). Fix API changes in the LanceDB storage, `call:lancedb`, the platform DB and
   the registry. Exit: FlowR's full test suites pass; existing `clawdflow.lance` and
   `a3t-platform.lance` data open and read unchanged (tested on a copy of vc-dev's
   data shape).
3. A version check (script in both repos' test runs) that the two lockfiles resolve
   these crates to the same versions.
4. Build size and time recorded against the 100 GB budget.

### H-1 — `EngineCore` in FlowR (`flowr-storage`, ~2 days, no behaviour change)

> **Done 2 Nov 2026** — FlowR `caa9078`. As planned, with these choices: changes are whole rows (task / run / wait set or deleted, journal entries appended, event seen, checkpoint, outbox), emitted through `apply`, the only mutation; typed methods on the core rather than an `Op` enum (the wire format stays the gateway's op JSON, mapped in H-3); a run remembers its last pool, used for resumes when the host has no router (ADB). The console's router still reads live pools from the gateway; switching it to `TaskQueue::live_pools` comes with H-4.

1. `engine::core`: `Op` (one variant per store method, serde names = the gateway's op
   names), `Reply`, `Change` (each with `durable()`: false only for lease extensions,
   H4), `Notice` (topic + message), and `EngineCore` holding today's in-memory state.
   `exec(&mut self, op, Ctx { now, new_id })` validates first, then emits changes and
   applies each as it emits it, and returns `(reply, changes, notices)`. A refused
   operation emits nothing. `apply(&mut self, &Change)` is the only code that mutates.
2. The journal held by the core can be **evicted** per run (`Op::Evict`); `read_from`
   on an evicted run answers `Reply::Archived`, and the host reads its archive.
3. `MemoryStore` = `Mutex<EngineCore>` + the notice fan-out (same behaviour, half
   the code).
4. `TaskQueue::live_pools(within)` (H10) with a SQLite implementation (last claim per
   pool, in memory), and the console's router and runtime view reading it instead of
   the gateway's map.
5. `EngineCore` is `Serialize`/`Deserialize` (snapshots, H5).
6. Tests: conformance on memory and SQLite; a **replay test** (random operation
   sequences: applying the logged durable changes to an empty core gives the same
   state, ignoring lease extensions); a determinism test (the same ops with the same
   `now`/ids give the same changes).

### H-2 — the engine in ADB (`adb-engine`, new crate, ~4 days)

1. Path dependency on `flowr-storage`; `Effect::Engine { space, changes }` in
   `adb-core` (changes as JSON bytes, opaque to the core crates).
2. `DurableWrites::engine(space, op)`: under the sequencer lock, `exec`; if there are
   durable changes, one log record `[Engine{…}, PutEpisode(journal row)…]` (fsynced),
   then apply: the engine applier updates the space's core, the memory applier writes
   the episodes to Lance as usual. Lease extensions apply without a record.
3. Journal episodes as in H5 (no embedding, no TTL); a `flowr:` tenant guard:
   `FORGET` / `UPDATE` refused, TTL / consolidation / compaction skip them, agent
   tokens can't see them (H15); `PurgeRunHistory { tenant, runs }` effect for
   retention (physical delete, as W5's immediate FORGET).
4. Archive reads: `read_from` on an evicted run → EPISODIC scan (tenant, scope = run,
   key > after, ordered). Finished runs are evicted 5 minutes after their end.
5. Snapshots and open: per space, every 10,000 records or 60 s; open loads the newest
   snapshot and replays `Engine` effects after it; all leases get one fresh period
   (H4); the log floor includes the snapshot offset.
6. Spaces created on first use; `adb engine stats` (runs live, journals in memory,
   snapshot age) for operations.
7. Tests: conformance through an in-process adapter; snapshot + replay equality;
   archive read equality; tenant guard; retention purge.

### H-3 — Flight surface (`adb-server`, ~1 day)

`do_action("engine")` `{space, op, args}` → the reply JSON or a typed error;
`do_get` ticket `{"engine_subscribe": {space, topic}}` → a stream of notices and,
for `run:<id>`, new journal records (H13); the `engine` token scope and secret
(H8). Tests: conformance over Flight in `adb-server/tests`.

### H-4 — FlowR on ADB, embedded and remote (~3 days)

1. **Embedded (CE, tests):** `init_storage` opens ADB's durable engine in
   `<data>/adb` in-process and uses it as the engine store, with no network hop.
2. **Remote (EE):** `AdbEngineStore`, the gateway client's op encoding over FlowR's
   existing Flight client (the one `call:adb` and the platform tables use); errors keep
   their kind. Selected by the ADB engine resource on the console and, through the
   controller's runtime env, on runtimes, which then talk to ADB directly (H7).
3. Run views: `get_run` / journal streaming from the subscription (H13), UI moves off
   polling (L25).
4. Tests: every FlowR engine suite on the embedded ADB (the default in tests), and
   once against a local ADB server over Flight (`FLOWR_TEST_ADB=<url>`).
5. **Remove SQLite** from `adb-integration` (engine store, gateway's SQLite path,
   the `rusqlite` dependency) in one commit once 1–4 are green. The branch
   `engine-sqlite` keeps it.

### H-5 — moving runs (`adb` CLI, ~1 day)

`adb import-flowr-engine <flowr.db> [--space]`: tasks, runs, journals (to the core and
to EPISODIC), waits, events seen; re-runnable (`--replace`); a test that imports a
SQLite file written by the `engine-sqlite` build and reads every run back identically.

### H-6 — crash, bench, vc-dev (~2 days)

1. Crash: SIGKILL ADB during runs (steps, timer, listener, approval); restart; all
   continue; nothing lost or repeated (a `tests/engine-crash.mjs` beside ADB's other
   crash tests).
2. Bench against SQLite (target: within 2×); group commit if needed.
3. **vc-dev (your OK to roll):** ADB image with the engine; FlowR image with the
   client; secret `adb-engine`; stop the console, `import-flowr-engine`, start it
   with the ADB resource; controller env for runtimes; run the gate (a3t-test flows,
   a run on `rt-worker`, console restart during a run on `rt-worker`).

L2 (subflow journals) and L5 (outbox drain) don't depend on the store and can go in
between. Both must still pass the conformance suite on every store.

## Risks

| Risk | Handling |
|---|---|
| Two copies of the rules drift (SQLite vs core) | Conformance suite on every store in every change (as today); H2 removes the third copy. |
| Journal volume in EPISODIC (every step of every run) | No embeddings (the cost of an episode is a Lance row); pure steps already arrive batched; retention purges; the bench includes Lance write volume. If episodes slow agent recall, the `flowr:` tenant moves to its own partition. |
| Memory growth in the sequencer | Only live runs' journals are in memory; finished runs leave after a few minutes; run rows are purged by retention. Add a size metric and a cap on journals kept in memory (spill to Lance early). |
| One sequencer lock for memory, platform and engine writes | Engine operations are short (microseconds of `exec`, plus the fsync shared with group commit). The bench decides whether engine writes need their own partition and log. |
| Lease reset after an ADB restart (H4) | It only delays dead-worker recovery by one lease period after a restart. The epoch fencing that keeps it safe is already tested by the conformance suite and the crash tests. |
| Two Arrow / Lance stacks in one binary (H16) | H-0 aligns versions first; if no LanceDB release fits, FlowR's LanceDB uses move onto embedded ADB platform tables. |
| CE gets heavier (ADB's engine linked in) | FlowR already links Lance, Arrow and DataFusion; after H-0 they are shared. Record binary size and start time in H-4. |
| Cross-repo dependency (ADB → `flowr-storage`, FlowR → ADB crates) | A path dependency between the sibling repos (`../flowr-system/shared/flowr-storage`). `flowr-storage` stays free of FlowR's runtime: the CI check builds it alone. |

# Stage H — FlowR's engine store on ADB: design

> **Status:** draft for agreement (2 Nov 2026) · **Builds on:** ARCHITECTURE §9 (write
> path), §11 (replication), §15 ("the rest of FlowR's state"); FlowR's
> `.ai/FLOWR_QUEUE_CHECKPOINT_SPEC.md` (the engine contract) and
> `.ai/FLOWR_ENGINE_LIMITATIONS.md` (L23, L35). · **Product decision (user, 2 Nov):**
> for FlowR **EE, ADB is the default engine store and SQLite the fallback**; CE keeps
> SQLite. This replaces the spec's plan of Postgres for EE (spec §8, open question 7).

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
| H1 | **Default and fallback** | EE uses ADB when the console is given an ADB engine resource (`FLOWR_ENGINE_ADB=<ns/name>` or `config.engine: true` on the ADB resource); otherwise it uses SQLite (`<data>/flowr.db`), exactly as today. **Fallback is configuration, not automatic failover**, as for platform tables (G3): if ADB is configured but unreachable, store calls retry with backoff and runs wait. They never switch to a local SQLite file, which would split runs, leases and journals into two diverging copies. CE builds ignore the setting. |
| H2 | **One implementation of the rules** | Move the in-memory store's logic into a pure **`EngineCore`** in `flowr-storage` (no I/O, no clock, no random ids: `now` and new ids are passed in). The memory store becomes `EngineCore` behind a mutex, and **ADB embeds the same `EngineCore`** (path dependency on `flowr-system/shared/flowr-storage`, which pulls only serde, uuid, chrono and async-trait). The rules are written once; the conformance suite checks the ADB wiring, not a second copy of the semantics. SQLite keeps its own SQL implementation, and the suite keeps it in step. |
| H3 | **What goes in the log** | **What changed, not the request**, as ADB does for memory writes (§9: validate in the sequencer, log typed effects). The sequencer runs the operation on `EngineCore` (`exec(op, now, ids) → (reply, changes)`) and logs the changes as one record: `Effect::Engine { space, changes }`, with `changes` opaque bytes to `adb-core` (JSON of `flowr-storage` change types). It applies them, then replies. Replay, catch-up and followers only ever `apply(change)`, so they never re-run decisions and never depend on the clock. Each change is small and explicit: `TaskEnqueued`, `TaskLeased`, `TaskAcked`, `RunStarted`, `RunOwned{epoch}`, `RunReleased`, `JournalAppended{run, seq, record}`, `WaitOpened`, `WaitClosed`, `EventSeen`, and so on. |
| H4 | **Heartbeats** | **Not logged.** A `heartbeat` or `renew` extends a lease in the leader's memory only, and is answered from there. After an ADB restart or a leader change, every lease and ownership gets one fresh lease period from the moment the engine opened. Safety doesn't depend on lease times: a worker that lost its run is still refused by the epoch check on its next write. The cost is liveness only, and only after an ADB restart: a run whose worker died just before it waits one more lease period. This removes most of the log volume (each live run renews every few seconds). |
| H5 | **Where state lives** | **Hot state in memory** inside the sequencer: tasks, runs, waits, events seen, outbox, and the journals of live runs. This is what operations are decided against, and it is small. **History in Lance**: the journal of every run is appended to `engine/<space>/journal.lance` (run, seq, epoch, at, kind, step key, body) in batches by the applier, the way memory rows reach Lance. A finished run's journal leaves memory a few minutes after it ends, and is then read from Lance. **Snapshots:** the hot state (without journal bodies already in Lance) is written to `engine/<space>/snapshot` every 10,000 records or 60 s, whichever comes first. On open: load the snapshot, then replay the log after it. The log may be purged below `min(snapshot offset, journal flushed offset) + 1`, the same rule ADB already uses for its other consumers. |
| H6 | **Wire** | **Arrow Flight**, like every native FlowR → ADB call (G5). `do_action("engine")` with `{space, op, args}`, using exactly the op names and argument shapes of FlowR's gateway today (`enqueue`, `claim_runs`, `commit_step`, `deliver_event`, …), and errors that keep their kind (`fenced`, `lease_lost`, `not_found`, `conflict`). The FlowR client is the gateway client with a different transport. Notifications go over `do_get` with ticket `{"engine_subscribe": topic}`, a stream of JSON notices, reconnected by the client and best effort as today (claimers also poll). |
| H7 | **Who talks to ADB** | **The console and runtime pods, directly.** Runtimes get the same ADB engine resource (the controller already passes `A3T_ADB_RESOURCE` to runtimes; it also passes the engine token, H8), so a console restart no longer interrupts runs on runtimes (closes L35). The console's engine gateway (3003) stays for the SQLite fallback, and FlowR keeps sending chat and agent stream chunks to the console over the gateway's `/stream` either way. |
| H8 | **Access** | Engine operations need an ADB token with the **`engine` scope**, held by FlowR consoles and runtimes only (a new secret, `adb-engine`). Tokens used for agent memory (`adb-auth`) can't read or write the engine. Runs carry principals and journals carry step inputs and outputs, so this is not memory-level data. |
| H9 | **Several FlowR installs on one ADB** | Each FlowR installation uses its own **space** (`config.space`, default `default`), with its own core, snapshot and journal dataset. Spaces share nothing, so one ADB can serve vc-dev and a staging console without mixing runs. |
| H10 | **Live pools** | The router sends runs only to pools with a live worker; today the gateway sees the claims. With ADB, the core records each pool's last claim time (in memory, not logged) and answers a new op, `live_pools`. It's added to the `TaskQueue` contract with a default, so SQLite answers it too, and the console's runtime view reads it from the store instead of the gateway. |
| H11 | **Moving existing runs** | `adb import-flowr-engine <flowr.db> [--space s]` (ADB CLI) copies tasks, runs, journals, waits and events seen from FlowR's SQLite file into the space through the log, once, with the console stopped. Then the console is started with the ADB resource. Re-runnable (`--replace`). vc-dev has live runs and a waiting test listener, so this is needed. Going back to SQLite is the reverse (`adb export-flowr-engine`), for the fallback to stay real. |
| H12 | **Not in this stage** | Raft. Stage H works on one ADB node, and replication comes with ADB M3 for free, because the engine is only log effects plus `apply`. More than one console replica also waits: chat and agent streams live in the console's memory (`token_stream`), so replicas need those streams over ADB topics first (a separate step). |

## How an operation runs

1. FlowR calls `do_action("engine", {space, op, args})`.
2. The server takes the sequencer lock (the same lock platform and memory writes
   take), runs `EngineCore::exec(op, now, fresh ids)` on the space's core, and gets
   the reply and the changes.
3. If there are no changes (a read, a refused write, a duplicate), it replies at once.
   Otherwise it appends one log record (`Effect::Engine{space, changes}`, fsynced),
   applies the changes to the core, hands journal appends to the Lance applier and
   wakes subscribers.
4. It replies with the result, or with the error kind (`Fenced` and the others).

Reads (`get`, `list`, `read_from` for a live run, `pending`, `approvals`) are answered
from the core without the log. `read_from` for a finished run reads `journal.lance`.
`list` with filters over history (run lists, newest first, by project, flow or status)
reads the in-memory run table. Run rows are small and are purged by retention
(30 days), so they stay in memory. The Lance journal is the only large data.

## Performance target

The SQLite numbers from FlowR's bench (`engine_sqlite::tests::bench`) are the bar:
start + claim + release per run, step commit p50 and p99, and steps per second with 8
concurrent runs. **The ADB store, measured from FlowR over Flight in the cluster, must
be within 2× of SQLite on all of them.** Each write is one fsynced log append, like
SQLite with `synchronous=FULL`, plus about 0.2 ms of Flight. If it isn't within 2×,
first group-commit concurrent operations in one log record (§9's 2–5 ms window);
don't drop durability.

## Gate (stage H exit)

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
  console pod doesn't interrupt a run on `rt-worker`. Switching back to SQLite (export
  and unset) still works.

## Order of work

| Step | Where | What |
|---|---|---|
| H-1 | FlowR `flowr-storage` | `EngineCore` + change types; the memory store rebuilt on it; `live_pools` in the contract; conformance green for memory and SQLite. No behaviour change. |
| H-2 | ADB `adb-engine` (new crate) | Spaces, `Effect::Engine`, exec under the sequencer lock, apply, journal applier to Lance, snapshots, open (snapshot + replay), log floor; in-process conformance run. |
| H-3 | ADB `adb-server` | Flight `do_action("engine")`, `engine_subscribe` stream, `engine` token scope. |
| H-4 | FlowR EE | `AdbEngineStore` (Flight client with the gateway's op encoding), the store switch (H1), runtimes direct (H7), live pools from the store (H10). |
| H-5 | ADB CLI | `import-flowr-engine` / `export-flowr-engine`. |
| H-6 | Both | Crash tests, bench, then roll to vc-dev behind the switch and run the gate there. |

L2 (subflow journals) and L5 (outbox drain) don't depend on the store and can go in
between. Both must still pass the conformance suite on all three stores.

## Risks

| Risk | Handling |
|---|---|
| Two copies of the rules drift (SQLite vs core) | Conformance suite on every store in every change (as today); H2 removes the third copy. |
| Memory growth in the sequencer | Only live runs' journals are in memory; finished runs leave after a few minutes; run rows are purged by retention. Add a size metric and a cap on journals kept in memory (spill to Lance early). |
| One sequencer lock for memory, platform and engine writes | Engine operations are short (microseconds of `exec`, plus the fsync shared with group commit). The bench decides whether engine writes need their own partition and log. |
| Lease reset after an ADB restart (H4) | It only delays dead-worker recovery by one lease period after a restart. The epoch fencing that keeps it safe is already tested by the conformance suite and the crash tests. |
| Cross-repo dependency (ADB → `flowr-storage`) | A path dependency between the sibling repos (`../flowr-system/shared/flowr-storage`). `flowr-storage` stays free of FlowR's runtime: the CI check builds it alone. |

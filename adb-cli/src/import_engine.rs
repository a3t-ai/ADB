//! `adb import-flowr-engine` (stage H-5, `.ai/H_ENGINE_STORE.md` H11): a FlowR
//! SQLite engine store (`flowr.db`, the schema on FlowR's branch
//! `engine-sqlite`) into an ADB engine space, over Flight with the engine token
//! (`ADB_ENGINE_TOKEN`). The rows become engine changes, logged by ADB like any
//! operation's: each run with its journal and checkpoints, then the tasks, the
//! open waits and the event ids delivered. A space that already holds runs is
//! refused (importing twice would repeat their journals).

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use flowr_storage::engine::{JournalEntry, OutboxRow, RunRow, Task};
use flowr_storage::engine_core::{Change, RunState, TaskRow, WaitRow};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

/// Journal entries per batch (Flight messages stay well under 64 MB).
const ENTRIES_PER_BATCH: usize = 500;
const ROWS_PER_BATCH: usize = 500;

fn time(n: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(n)
}

fn untag<T: DeserializeOwned>(s: &str) -> Result<T, String> {
    serde_json::from_value(Value::String(s.to_string())).map_err(|e| format!("{s}: {e}"))
}

fn json_of(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or(Value::Null)
}

fn table_exists(c: &Connection, name: &str) -> Result<bool, String> {
    c.query_row("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1", [name], |_| Ok(()))
        .optional()
        .map(|r| r.is_some())
        .map_err(|e| e.to_string())
}

/// What a `flowr.db` holds, counted.
#[derive(Debug, Default, PartialEq)]
pub struct Counts {
    pub runs: usize,
    pub entries: usize,
    pub tasks: usize,
    pub waits: usize,
    pub events: usize,
    pub checkpoints: usize,
    pub outbox: usize,
}

/// The engine changes that rebuild `db`'s state, in batches to send in order.
pub fn changes(db: &Path) -> Result<(Vec<Vec<Change>>, Counts), String> {
    let c = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| format!("open {}: {e}", db.display()))?;
    let e = |e: rusqlite::Error| e.to_string();
    let mut counts = Counts::default();
    let mut batches: Vec<Vec<Change>> = Vec::new();

    // Each run's pool: its tasks' (resumes go there without a router).
    let mut pools: HashMap<String, String> = HashMap::new();
    {
        let mut st = c.prepare("SELECT run_id, pool FROM tasks").map_err(e)?;
        for r in st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))).map_err(e)? {
            let (run, pool) = r.map_err(e)?;
            pools.insert(run, pool);
        }
    }

    // Runs, each with its journal and checkpoints.
    let mut st = c
        .prepare(
            "SELECT run_id, namespace, flow, version, def_hash, parent_run_id, durability, debug, breakpoints, principal, \
             trigger_kind, input, output, error, phase, current_step, owner, epoch, owner_lease_until, cancel_requested, \
             suspend_requested, resume_count, last_seq, deadline_at, dedupe_key, start_task_id, created_at, started_at, \
             completed_at, updated_at FROM runs ORDER BY created_at",
        )
        .map_err(e)?;
    let rows = st
        .query_map([], |r| {
            Ok((
                (r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?),
                (r.get::<_, Option<String>>(5)?, r.get::<_, String>(6)?, r.get::<_, i64>(7)?, r.get::<_, Option<String>>(8)?, r.get::<_, String>(9)?),
                (r.get::<_, String>(10)?, r.get::<_, String>(11)?, r.get::<_, Option<String>>(12)?, r.get::<_, Option<String>>(13)?, r.get::<_, String>(14)?),
                (r.get::<_, Option<String>>(15)?, r.get::<_, Option<String>>(16)?, r.get::<_, i64>(17)?, r.get::<_, Option<i64>>(18)?, r.get::<_, i64>(19)?),
                (r.get::<_, i64>(20)?, r.get::<_, i64>(21)?, r.get::<_, i64>(22)?, r.get::<_, Option<i64>>(23)?, r.get::<_, Option<String>>(24)?),
                (r.get::<_, Option<String>>(25)?, r.get::<_, i64>(26)?, r.get::<_, Option<i64>>(27)?, r.get::<_, Option<i64>>(28)?, r.get::<_, i64>(29)?),
            ))
        })
        .map_err(e)?;
    for row in rows {
        let ((run_id, namespace, flow, version, def_hash), (parent, durability, debug, breakpoints, principal), (trigger, input, output, error, phase), (current_step, owner, epoch, owner_until, cancel), (suspend, resume_count, last_seq, deadline, dedupe), (start_task, created, started, completed, updated)) =
            row.map_err(e)?;
        let id: uuid::Uuid = run_id.parse().map_err(|x| format!("run id {run_id}: {x}"))?;
        let state = RunState {
            row: RunRow {
                run_id: id,
                namespace,
                flow,
                version,
                def_hash,
                parent_run_id: parent.and_then(|p| p.parse().ok()),
                durability: untag(&durability)?,
                debug: debug != 0,
                breakpoints: breakpoints.map(|b| json_of(&b)),
                principal: json_of(&principal),
                trigger,
                input: json_of(&input),
                output: output.map(|o| json_of(&o)),
                error,
                phase: untag(&phase)?,
                current_step,
                owner,
                epoch: epoch as u64,
                cancel_requested: cancel != 0,
                suspend_requested: suspend != 0,
                resume_count: resume_count as u32,
                last_seq: last_seq as u64,
                deadline_at: deadline.map(time),
                created_at: time(created),
                started_at: started.map(time),
                completed_at: completed.map(time),
                updated_at: time(updated),
            },
            owner_until: owner_until.map(time),
            pool: pools.get(&run_id).cloned().unwrap_or_else(|| "default".into()),
            dedupe_key: dedupe,
            start_task: start_task.and_then(|t| t.parse().ok()).unwrap_or_default(),
        };
        let mut batch = vec![Change::Run { id, state: Some(state) }];
        counts.runs += 1;
        let mut js = c.prepare("SELECT seq, epoch, at, body FROM journal WHERE run_id = ?1 ORDER BY seq").map_err(e)?;
        let entries: Vec<JournalEntry> = js
            .query_map([&run_id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?, r.get::<_, String>(3)?)))
            .map_err(e)?
            .map(|r| {
                let (seq, epoch, at, body) = r.map_err(e)?;
                Ok(JournalEntry { seq: seq as u64, epoch: epoch as u64, at: time(at), record: serde_json::from_str(&body).map_err(|x| format!("run {run_id} seq {seq}: {x}"))? })
            })
            .collect::<Result<_, String>>()?;
        counts.entries += entries.len();
        let mut chunks = entries.chunks(ENTRIES_PER_BATCH);
        if let Some(first) = chunks.next() {
            batch.push(Change::Journal { run: id, entries: first.to_vec() });
        }
        let rest: Vec<Vec<Change>> = chunks.map(|ch| vec![Change::Journal { run: id, entries: ch.to_vec() }]).collect();
        if table_exists(&c, "checkpoints")? {
            let mut cs = c.prepare("SELECT seq, snapshot, at FROM checkpoints WHERE run_id = ?1 ORDER BY seq").map_err(e)?;
            for r in cs.query_map([&run_id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, i64>(2)?))).map_err(e)? {
                let (seq, snapshot, at) = r.map_err(e)?;
                batch.push(Change::Checkpoint { run: id, seq: seq as u64, snapshot, at: time(at) });
                counts.checkpoints += 1;
            }
        }
        batches.push(batch);
        batches.extend(rest);
    }

    // Tasks, in their claim order.
    let mut tasks = Vec::new();
    let mut ts = c
        .prepare("SELECT task_id, pool, kind, run_id, namespace, payload, dedupe_key, visible_at, lease_owner, lease_token, lease_until, attempts FROM tasks ORDER BY visible_at")
        .map_err(e)?;
    let mut order = 0u64;
    for r in ts
        .query_map([], |r| {
            Ok((
                (r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?),
                (r.get::<_, Option<String>>(6)?, r.get::<_, i64>(7)?, r.get::<_, Option<String>>(8)?, r.get::<_, i64>(9)?, r.get::<_, Option<i64>>(10)?, r.get::<_, i64>(11)?),
            ))
        })
        .map_err(e)?
    {
        let ((task_id, pool, kind, run_id, namespace, payload), (dedupe, visible, lease_owner, lease_token, lease_until, attempts)) = r.map_err(e)?;
        order += 1;
        let id: uuid::Uuid = task_id.parse().map_err(|x| format!("task id {task_id}: {x}"))?;
        tasks.push(Change::Task {
            id,
            row: Some(TaskRow {
                task: Task { task_id: id, pool, kind: untag(&kind)?, run_id: run_id.parse().map_err(|x| format!("task {task_id}: {x}"))?, namespace, payload: json_of(&payload), attempts: attempts as u32 },
                order,
                dedupe_key: dedupe,
                visible_at: time(visible),
                lease_owner,
                lease_token: lease_token as u64,
                lease_until: lease_until.map(time),
            }),
        });
    }
    counts.tasks = tasks.len();
    batches.extend(tasks.chunks(ROWS_PER_BATCH).map(|c| c.to_vec()));

    // Open waits (from ee27) and event ids delivered.
    if table_exists(&c, "waits")? {
        let mut ws = c.prepare("SELECT run_id, wait_id, kind, namespace, correlation, received FROM waits ORDER BY created_at").map_err(e)?;
        let mut waits = Vec::new();
        for r in ws
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?)))
            .map_err(e)?
        {
            let (run_id, wait_id, kind, namespace, correlation, received) = r.map_err(e)?;
            let run: uuid::Uuid = run_id.parse().map_err(|x| format!("wait {wait_id}: {x}"))?;
            let received = match json_of(&received) {
                Value::Array(a) => a,
                _ => Vec::new(),
            };
            waits.push(Change::Wait { run, wait_id: wait_id.clone(), row: Some(WaitRow { kind, wait_id, run, namespace, correlation: json_of(&correlation), received }) });
        }
        counts.waits = waits.len();
        batches.extend(waits.chunks(ROWS_PER_BATCH).map(|c| c.to_vec()));
    }
    if table_exists(&c, "events_seen")? {
        let mut es = c.prepare("SELECT namespace, id, at FROM events_seen").map_err(e)?;
        let mut events = Vec::new();
        for r in es.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))).map_err(e)? {
            let (ns, id, at) = r.map_err(e)?;
            events.push(Change::EventSeen { key: format!("{ns}\n{id}"), at: time(at) });
        }
        counts.events = events.len();
        batches.extend(events.chunks(ROWS_PER_BATCH).map(|c| c.to_vec()));
    }
    if table_exists(&c, "outbox")? {
        let mut os = c.prepare("SELECT id, topic, body FROM outbox ORDER BY id").map_err(e)?;
        let mut rows = Vec::new();
        for r in os.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).map_err(e)? {
            let (id, topic, body) = r.map_err(e)?;
            rows.push(Change::Outbox { row: OutboxRow { id: id as u64, topic, body: json_of(&body) } });
        }
        counts.outbox = rows.len();
        batches.extend(rows.chunks(ROWS_PER_BATCH).map(|c| c.to_vec()));
    }
    Ok((batches, counts))
}

/// A Flight connection to one ADB with its engine token.
struct Flight {
    client: arrow_flight::flight_service_client::FlightServiceClient<tonic::transport::Channel>,
    bearer: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl Flight {
    async fn connect(url: &str, token_var: &str) -> Result<Self, String> {
        let channel = tonic::transport::Channel::from_shared(url.to_string()).map_err(|e| e.to_string())?.connect().await.map_err(|e| format!("connect {url}: {e}"))?;
        let bearer = std::env::var(token_var)
            .ok()
            .filter(|t| !t.is_empty())
            .map(|t| format!("Bearer {t}").parse().map_err(|_| format!("{token_var} is not a valid header value")))
            .transpose()?;
        let client = arrow_flight::flight_service_client::FlightServiceClient::new(channel).max_encoding_message_size(64 << 20).max_decoding_message_size(64 << 20);
        Ok(Self { client, bearer })
    }

    async fn action(&mut self, kind: &str, body: &Value) -> Result<Value, String> {
        use futures::StreamExt;
        let mut req = tonic::Request::new(arrow_flight::Action { r#type: kind.into(), body: body.to_string().into() });
        if let Some(b) = &self.bearer {
            req.metadata_mut().insert("authorization", b.clone());
        }
        let mut s = self.client.do_action(req).await.map_err(|e| e.message().to_string())?.into_inner();
        let r = s.next().await.ok_or("no answer")?.map_err(|e| e.message().to_string())?;
        serde_json::from_slice(&r.body).map_err(|e| format!("answer: {e}"))
    }

    async fn restore(&mut self, space: &str, changes: &[Change], first: bool) -> Result<(), String> {
        self.action("engine_restore", &json!({ "space": space, "changes": changes, "first": first })).await.map(|_| ())
    }
}

/// Send `db` into `space` of the ADB at `flight`.
pub async fn import(db: &Path, flight: &str, space: &str) -> Result<(), String> {
    let (batches, counts) = changes(db)?;
    let mut to = Flight::connect(flight, "ADB_ENGINE_TOKEN").await?;
    let total = batches.len();
    for (i, batch) in batches.into_iter().enumerate() {
        to.restore(space, &batch, i == 0).await.map_err(|e| format!("batch {}/{total}: {e}", i + 1))?;
    }
    println!(
        "imported into space {space}: {} runs ({} journal entries, {} checkpoints), {} tasks, {} waits, {} event ids, {} outbox rows",
        counts.runs, counts.entries, counts.checkpoints, counts.tasks, counts.waits, counts.events, counts.outbox
    );
    Ok(())
}

/// The bytes of changes sent in one `engine_restore` (Flight messages stay well
/// under 64 MB).
const SNAPSHOT_BATCH_BYTES: usize = 8 << 20;
/// Evicted journals read at once from the old ADB.
const JOURNAL_READS: usize = 32;

/// An ADB engine space's snapshot file (`<data>/engine/<space>/snapshot.json`).
#[derive(serde::Deserialize)]
struct Snapshot {
    offset: u64,
    core: flowr_storage::engine_core::EngineCore,
}

/// Move an engine space from another ADB's snapshot (`snapshot.json`, taken
/// with the log replayed: the ADB stopped, or opened offline) into `space` of
/// the ADB at `flight`. With `journals_from` (that ADB, serving its data
/// offline; token `ADB_ENGINE_TOKEN_FROM`), each finished run's evicted journal
/// is read back and goes along, so its history is kept; without it, finished
/// runs keep their rows and lose their step history.
pub async fn import_snapshot(snapshot: &Path, flight: &str, space: &str, journals_from: Option<&str>) -> Result<(), String> {
    use futures::StreamExt;
    let file = std::fs::File::open(snapshot).map_err(|e| format!("open {}: {e}", snapshot.display()))?;
    let snap: Snapshot = serde_json::from_reader(std::io::BufReader::with_capacity(1 << 20, file)).map_err(|e| format!("{}: {e}", snapshot.display()))?;
    let changes = snap.core.to_changes();
    drop(snap.core);
    let runs: HashMap<_, _> = changes.iter().filter_map(|c| match c {
        Change::Run { id, state: Some(s) } => Some((*id, s.clone())),
        _ => None,
    }).collect();
    let mut to = Flight::connect(flight, "ADB_ENGINE_TOKEN").await?;
    let mut from = match journals_from {
        Some(u) => Some(Flight::connect(u, "ADB_ENGINE_TOKEN_FROM").await?),
        None => None,
    };
    let (mut batch, mut bytes, mut first, mut sent) = (Vec::new(), 0usize, true, 0usize);
    let (mut entries_moved, mut journals_read, mut journals_missing) = (0usize, 0usize, 0usize);
    // Sends `batch` once it is big enough (or `force`).
    macro_rules! flush {
        ($force:expr) => {
            if !batch.is_empty() && ($force || bytes >= SNAPSHOT_BATCH_BYTES) {
                to.restore(space, &batch, first).await.map_err(|e| format!("after {sent} changes: {e}"))?;
                sent += batch.len();
                first = false;
                batch.clear();
                bytes = 0;
            }
        };
    }
    let mut evicted = Vec::new();
    for c in changes {
        if let Change::JournalEvicted { run } = c {
            evicted.push(run);
            continue;
        }
        bytes += serde_json::to_vec(&c).map(|v| v.len()).unwrap_or(0);
        batch.push(c);
        flush!(false);
    }
    // The finished runs, each with its journal read back (when asked) and its
    // final state again (entries move the row), then marked evicted.
    for chunk in evicted.chunks(JOURNAL_READS * 8) {
        let journals: Vec<(flowr_storage::engine::RunId, Vec<JournalEntry>)> = match from.as_mut() {
            None => chunk.iter().map(|r| (*r, Vec::new())).collect(),
            Some(f) => {
                let mut out = Vec::with_capacity(chunk.len());
                for part in chunk.chunks(JOURNAL_READS) {
                    let reads = part.iter().map(|run| {
                        let mut f = Flight { client: f.client.clone(), bearer: f.bearer.clone() };
                        let run = *run;
                        async move {
                            let v = f.action("engine", &json!({ "space": space, "op": "read_from", "args": { "run": run, "after_seq": 0 } })).await?;
                            if let Some(e) = v.get("err").or_else(|| v.get("error")) {
                                return Err(format!("reading run {run}'s journal: {e}"));
                            }
                            let entries = match v.get("ok") {
                                Some(Value::Null) | None => Vec::new(),
                                Some(e) => serde_json::from_value::<Vec<JournalEntry>>(e.clone()).map_err(|e| format!("run {run}: {e}"))?,
                            };
                            Ok::<_, String>((run, entries))
                        }
                    });
                    for r in futures::stream::iter(reads).buffered(JOURNAL_READS).collect::<Vec<_>>().await {
                        out.push(r?);
                    }
                }
                out
            }
        };
        for (run, entries) in journals {
            if from.is_some() {
                if entries.is_empty() { journals_missing += 1 } else { journals_read += 1 }
            }
            if !entries.is_empty() {
                entries_moved += entries.len();
                let j = Change::Journal { run, entries };
                bytes += serde_json::to_vec(&j).map(|v| v.len()).unwrap_or(0);
                batch.push(j);
                if let Some(s) = runs.get(&run) {
                    batch.push(Change::Run { id: run, state: Some(s.clone()) });
                }
            }
            batch.push(Change::JournalEvicted { run });
            bytes += 64;
            flush!(false);
        }
    }
    flush!(true);
    println!(
        "imported into space {space} (snapshot at offset {}): {} runs, {} changes; evicted journals: {} read back ({} entries), {} without entries{}",
        snap.offset,
        runs.len(),
        sent,
        journals_read,
        entries_moved,
        journals_missing,
        if from.is_none() { " (not read: no --journals-from)" } else { "" }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use adb_executor::{Engine, LocalEngine};
    use flowr_storage::engine::*;

    /// The tables of FlowR's SQLite engine store (branch `engine-sqlite`).
    const SCHEMA: &str = "
CREATE TABLE tasks (task_id TEXT PRIMARY KEY, pool TEXT NOT NULL, kind TEXT NOT NULL, run_id TEXT NOT NULL, namespace TEXT NOT NULL,
  payload TEXT NOT NULL, dedupe_key TEXT UNIQUE, visible_at INTEGER NOT NULL, lease_owner TEXT, lease_token INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER, attempts INTEGER NOT NULL DEFAULT 0);
CREATE TABLE runs (run_id TEXT PRIMARY KEY, namespace TEXT NOT NULL, flow TEXT NOT NULL, version TEXT NOT NULL, def_hash TEXT NOT NULL,
  parent_run_id TEXT, durability TEXT NOT NULL, debug INTEGER NOT NULL DEFAULT 0, breakpoints TEXT, principal TEXT NOT NULL,
  trigger_kind TEXT NOT NULL, input TEXT NOT NULL, output TEXT, error TEXT, phase TEXT NOT NULL, current_step TEXT, owner TEXT,
  epoch INTEGER NOT NULL DEFAULT 0, owner_lease_until INTEGER, cancel_requested INTEGER NOT NULL DEFAULT 0,
  suspend_requested INTEGER NOT NULL DEFAULT 0, resume_count INTEGER NOT NULL DEFAULT 0, last_seq INTEGER NOT NULL DEFAULT 0,
  deadline_at INTEGER, dedupe_key TEXT UNIQUE, start_task_id TEXT, created_at INTEGER NOT NULL, started_at INTEGER,
  completed_at INTEGER, updated_at INTEGER NOT NULL);
CREATE TABLE journal (run_id TEXT NOT NULL, seq INTEGER NOT NULL, ident TEXT, step_key TEXT, kind TEXT NOT NULL, epoch INTEGER NOT NULL,
  body TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (run_id, seq));
CREATE TABLE checkpoints (run_id TEXT NOT NULL, seq INTEGER NOT NULL, snapshot BLOB NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (run_id, seq));
CREATE TABLE waits (run_id TEXT NOT NULL, wait_id TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'event', namespace TEXT NOT NULL,
  correlation TEXT NOT NULL, received TEXT NOT NULL DEFAULT '[]', created_at INTEGER NOT NULL, PRIMARY KEY (run_id, wait_id));
CREATE TABLE events_seen (namespace TEXT NOT NULL, id TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (namespace, id));
CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL);
";

    #[tokio::test(flavor = "multi_thread")]
    async fn a_flowr_db_imports_into_an_engine_space() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("flowr.db");
        let (done, waiting, task) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        {
            let c = Connection::open(&db).unwrap();
            c.execute_batch(SCHEMA).unwrap();
            let run = |id: &uuid::Uuid, phase: &str, last_seq: i64, completed: Option<i64>| {
                c.execute(
                    "INSERT INTO runs(run_id, namespace, flow, version, def_hash, durability, principal, trigger_kind, input, phase, epoch, last_seq, created_at, completed_at, updated_at, output)
                     VALUES (?1, 'p', 'f', '1', 'h', 'durable', '{\"id\":\"u\"}', 'api', '{\"x\":1}', ?2, 1, ?3, ?4, ?5, ?4, '{\"ok\":true}')",
                    rusqlite::params![id.to_string(), phase, last_seq, now, completed],
                )
                .unwrap();
            };
            run(&done, "completed", 2, Some(now));
            run(&waiting, "waiting", 1, None);
            let step = |run: &uuid::Uuid, seq: i64, key: &str| {
                let rec = Record::StepCompleted { step_key: key.into(), attempt: 0, output: json!(seq), context: Value::Null, class: StepClass::Pure, duration_us: 5, directive: String::new(), input: Value::Null, at: None };
                c.execute("INSERT INTO journal(run_id, seq, kind, epoch, body, at) VALUES (?1, ?2, 'step_completed', 1, ?3, ?4)", rusqlite::params![run.to_string(), seq, serde_json::to_string(&rec).unwrap(), now]).unwrap();
            };
            step(&done, 1, "/a");
            step(&done, 2, "/b");
            step(&waiting, 1, "/a");
            c.execute(
                "INSERT INTO tasks(task_id, pool, kind, run_id, namespace, payload, dedupe_key, visible_at) VALUES (?1, 'default', 'resume_run', ?2, 'p', '{}', ?3, ?4)",
                rusqlite::params![task.to_string(), waiting.to_string(), format!("{waiting}:timer:1"), now + 3_600_000_000_000i64],
            )
            .unwrap();
            c.execute("INSERT INTO waits(run_id, wait_id, namespace, correlation, created_at) VALUES (?1, '/w', 'p', '{\"mode\":\"any\",\"filters\":[{\"type\":\"go\"}]}', ?2)", rusqlite::params![waiting.to_string(), now]).unwrap();
            c.execute("INSERT INTO events_seen(namespace, id, at) VALUES ('p', 'e1', ?1)", [now]).unwrap();
        }
        let (batches, counts) = changes(&db).unwrap();
        assert_eq!(counts, Counts { runs: 2, entries: 3, tasks: 1, waits: 1, events: 1, checkpoints: 0, outbox: 0 });

        let (engine, _) = LocalEngine::durable(dir.path().join("adb")).await.unwrap();
        for (i, b) in batches.iter().enumerate() {
            let b: Vec<Value> = b.iter().map(|c| serde_json::to_value(c).unwrap()).collect();
            engine.engine_restore("s", b, i == 0).await.unwrap();
        }
        let s = engine.engine_store("s").unwrap();
        let r = s.get(done).await.unwrap().unwrap();
        assert_eq!((r.phase, r.last_seq, r.output.clone()), (RunPhase::Completed, 2, Some(json!({ "ok": true }))));
        assert_eq!(s.read_from(done, 0).await.unwrap().len(), 2);
        assert_eq!(s.get(waiting).await.unwrap().unwrap().phase, RunPhase::Waiting);
        assert_eq!(s.pending(DEFAULT_POOL).await.unwrap(), 1, "the timer's wake-up");
        assert!(s.deliver_event("p", json!({ "id": "e1", "type": "go" })).await.unwrap().is_empty(), "an event id already delivered");
        assert_eq!(s.deliver_event("p", json!({ "id": "e2", "type": "go" })).await.unwrap(), vec![waiting], "the open wait");
        // Importing again into the same space is refused.
        let again: Vec<Value> = batches[0].iter().map(|c| serde_json::to_value(c).unwrap()).collect();
        assert!(engine.engine_restore("s", again, true).await.unwrap_err().contains("already holds"));
        // And it all survives a restart.
        drop(s);
        drop(engine);
        let (engine, _) = LocalEngine::durable(dir.path().join("adb")).await.unwrap();
        assert_eq!(engine.engine_store("s").unwrap().read_from(done, 0).await.unwrap().len(), 2);
    }
}

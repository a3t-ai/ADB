//! ADB over Arrow Flight (stage G, `G_PLATFORM_TABLES.md` G5): FlowR's native
//! `call:adb` wire. Platform tables move as Arrow batches; AQL rides `do_action`
//! with the same JSON as `POST /query`. MCP stays JSON over HTTP.
//!
//! | Call | Request | Answer |
//! |---|---|---|
//! | `do_get` | ticket `{"table", "filter"?, "limit"?}` (Lance SQL filter, default limit 100) | the matching rows as Arrow batches |
//! | `do_get` | ticket `{"sql", "limit"?}`: read-only SQL over memory and platform tables (superadmin debug view, `adb_executor::sql`; default limit 1,000, max 10,000) | the result as Arrow batches |
//! | `do_get` | ticket `{"feed": "facts", "from"?, "scope"?, "namespace"?, "max"?, "wait_ms"?}`: the fact feed (GR-15, `adb_gateway::facts`); no `from`, or one the log no longer holds, gives a snapshot | fact rows as Arrow batches ([`fact_schema`]); schema metadata `next` (the next `from`), `tip` and `snapshot` (`"true"`: replace the copy) |
//! | `do_put` | descriptor path `[table]`, Arrow batches | one `PutResult`, metadata `{"inserted", "offset"}` |
//! | `do_put` | descriptor path `[table, "replace"]`, Arrow batches | the table replaced (its columns = the batches'), `{"inserted", "offset"}` |
//! | `do_action("replace_table")` | `{"table", "columns"}` | the table replaced by an empty one, `{"replaced": true, "offset"}` |
//! | `do_action("create_table")` | `{"table", "columns"?: [..] \| "schema"?: {col: type}}` | `{"created", "exists", "table"}` |
//! | `do_action("update")` | `{"table", "filter", "values": {col: value}}` | `{"updated": true, "offset"}` |
//! | `do_action("delete")` | `{"table", "filter"}` | `{"deleted": true, "offset"}` |
//! | `do_action("compact")` | `{}` | `{"total", "kept", "bytes_before", "bytes_after", "took_ms", "platform": {…}}` (W5, W5-L4) |
//! | `do_action("list_tables")` | `{}` | `["table", …]` |
//! | `do_action("aql")` | `{"query", "variables"?}` | the `POST /query` result (`{success, data, …}`) |
//! | `do_action("engine")` | `{"space", "op", "args"}`: FlowR's engine store (stage H), one op per store method (`flowr_storage::engine_wire`, the same as FlowR's gateway) | `{"ok": result}` or `{"err": {"kind", "id"?, "message"}}` (`fenced`, `lease_lost`, `not_found`, `conflict`, `backend`) |
//! | `do_action("engine_subscribe")` | `{"space", "topic"}` (`tasks:<pool>`, `run:<id>`) | a stream: one result per notice (JSON), until the client goes |
//! | `do_action("engine_restore")` | `{"space", "changes": [..], "first"?}`: another store's state as engine changes (`adb import-flowr-engine`, H-5); `first` refuses a space that holds runs | `{"restored": n}` |
//!
//! Engine actions need the engine token (`ADB_ENGINE_TOKEN`) when ADB has tokens;
//! the memory token (`ADB_AUTH_TOKEN`) doesn't open them, and the engine token
//! opens nothing else (H8).
//!
//! Errors are gRPC statuses: `invalid_argument` for a bad request or write,
//! `not_found` for a missing table, `failed_precondition` without `--data-dir`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use adb_core::log::Effect;
use adb_executor::Engine;
use arrow_array::builder::{Float32Builder, StringBuilder, UInt32Builder, UInt64Builder};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use tonic::{Request, Response, Status, Streaming};

/// Largest Flight message either way (a big list or a bulk insert).
const MAX_MESSAGE: usize = 64 << 20;

/// Serve Flight on `addr` until the process ends.
pub async fn serve_flight(addr: SocketAddr, engine: Arc<dyn Engine>) -> std::io::Result<()> {
    serve_flight_with_tokens(addr, engine, crate::auth::token_from_env(), crate::auth::engine_token_from_env()).await
}

/// [`serve_flight`] with an explicit memory token (`None`: open) and no engine token.
pub async fn serve_flight_with_token(addr: SocketAddr, engine: Arc<dyn Engine>, token: Option<Arc<str>>) -> std::io::Result<()> {
    serve_flight_with_tokens(addr, engine, token, None).await
}

/// [`serve_flight`] with explicit tokens (both `None`: open).
pub async fn serve_flight_with_tokens(addr: SocketAddr, engine: Arc<dyn Engine>, token: Option<Arc<str>>, engine_token: Option<Arc<str>>) -> std::io::Result<()> {
    tracing::info!(auth = token.is_some(), engine_auth = engine_token.is_some(), "ADB Flight on grpc://{addr}");
    let svc = FlightServiceServer::new(AdbFlight { engine })
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    let pools = crate::auth::pool_tokens_from_env();
    if !pools.is_empty() {
        tracing::info!(pools = ?pools.iter().map(|(p, _)| p.to_string()).collect::<Vec<_>>(), "engine pool tokens");
    }
    let svc = tonic::service::interceptor::InterceptedService::new(svc, crate::auth::FlightAuth { memory: token, engine: engine_token, pools, scopes: crate::scope::scopes_from_env() });
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))
}

pub struct AdbFlight {
    engine: Arc<dyn Engine>,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

use crate::auth::Access;

fn access<T>(req: &Request<T>) -> Access {
    req.extensions().get::<Access>().copied().unwrap_or(Access::Open)
}

/// Memory, platform tables and AQL: not with the engine token (H8).
fn not_engine_only<T>(req: &Request<T>) -> Result<(), Status> {
    match access(req) {
        Access::Engine => Err(Status::permission_denied("the engine token opens only FlowR's engine store")),
        _ => Ok(()),
    }
}

/// Tables are not open to a scoped token (DB-3).
fn not_scoped<T>(req: &Request<T>) -> Result<(), Status> {
    match req.extensions().get::<Arc<crate::scope::Scope>>() {
        Some(s) => Err(Status::permission_denied(format!("platform tables are not open to the {} token", s.name))),
        None => Ok(()),
    }
}

fn platform_err(e: adb_platform::PlatformError) -> Status {
    match e {
        adb_platform::PlatformError::NoTable(t) => Status::not_found(format!("no platform table {t}")),
        other => Status::invalid_argument(other.to_string()),
    }
}

fn body(bytes: &[u8]) -> Result<Value, Status> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(bytes).map_err(|e| Status::invalid_argument(format!("request body is not JSON: {e}")))
}

fn str_field<'a>(v: &'a Value, name: &str) -> Result<&'a str, Status> {
    v.get(name).and_then(|x| x.as_str()).ok_or_else(|| Status::invalid_argument(format!("missing \"{name}\"")))
}

/// `call:lancedb`'s update values: strings become quoted SQL literals ('' escapes),
/// null becomes NULL, anything else its JSON text.
fn sql_literal(v: &Value) -> String {
    match v {
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

/// Arrow batches (any string-ish columns) as rows of field → string value.
fn batches_to_rows(batches: &[RecordBatch]) -> Vec<serde_json::Map<String, Value>> {
    let mut rows = Vec::new();
    for b in batches {
        let cols: Vec<(String, Arc<dyn Array>)> = b
            .schema()
            .fields()
            .iter()
            .zip(b.columns())
            .map(|(f, c)| {
                let c = arrow_cast::cast(c, &arrow_schema::DataType::Utf8).unwrap_or_else(|_| Arc::clone(c));
                (f.name().clone(), c)
            })
            .collect();
        for r in 0..b.num_rows() {
            let mut o = serde_json::Map::new();
            for (name, col) in &cols {
                if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    if a.is_valid(r) {
                        o.insert(name.clone(), Value::String(a.value(r).to_string()));
                    }
                }
            }
            rows.push(o);
        }
    }
    rows
}

/// Fact rows per Arrow batch.
const FACT_BATCH: usize = 4096;
/// Default changes per fact-feed read.
const FACT_MAX: usize = 10_000;

/// The fact feed's columns; `next`, `tip` and `snapshot` go in the metadata. A
/// `remove` sets `op` through `scope`; the rest is null.
pub fn fact_schema(next: u64, tip: u64, snapshot: bool) -> SchemaRef {
    let s = |n: &str, null: bool| Field::new(n, DataType::Utf8, null);
    let fields = vec![
        s("op", false),
        Field::new("offset", DataType::UInt64, false),
        s("commit_ts", false),
        s("id", false),
        Field::new("version", DataType::UInt32, true),
        s("scope", false),
        s("namespace", true),
        s("subject", true),
        s("predicate", true),
        s("object", true),
        // JSON text.
        s("value", true),
        Field::new("confidence", DataType::Float32, true),
        s("source", true),
        s("agent", true),
        s("valid_from", true),
        s("valid_to", true),
        // The whole payload, JSON text.
        s("data", true),
    ];
    let meta = HashMap::from([
        ("next".to_string(), next.to_string()),
        ("tip".to_string(), tip.to_string()),
        ("snapshot".to_string(), snapshot.to_string()),
    ]);
    Arc::new(Schema::new_with_metadata(fields, meta))
}

fn fact_batch(schema: &SchemaRef, changes: &[adb_gateway::FactChange]) -> Result<RecordBatch, Status> {
    let n = changes.len();
    let strings = || StringBuilder::with_capacity(n, n * 16);
    let (mut op, mut ts, mut id, mut scope) = (strings(), strings(), strings(), strings());
    let (mut ns, mut subject, mut predicate, mut object, mut value) = (strings(), strings(), strings(), strings(), strings());
    let (mut source, mut agent, mut from, mut to, mut data) = (strings(), strings(), strings(), strings(), strings());
    let (mut offset, mut version, mut confidence) = (UInt64Builder::with_capacity(n), UInt32Builder::with_capacity(n), Float32Builder::with_capacity(n));
    for c in changes {
        op.append_value(c.op);
        offset.append_value(c.offset);
        ts.append_value(c.commit_ts.to_datetime().to_rfc3339());
        id.append_value(&c.id);
        version.append_option(c.version);
        scope.append_value(&c.scope);
        let f = c.fact.as_ref();
        ns.append_option(f.and_then(|f| f.namespace.as_deref()));
        subject.append_option(f.map(|f| f.subject.as_str()));
        predicate.append_option(f.map(|f| f.predicate.as_str()));
        object.append_option(f.and_then(|f| f.object.as_deref()));
        value.append_option(f.and_then(|f| f.value.as_ref()).map(|v| v.to_string()));
        confidence.append_option(f.map(|f| f.confidence));
        source.append_option(f.and_then(|f| f.source.as_deref()));
        agent.append_option(f.and_then(|f| f.agent.as_deref()));
        from.append_option(f.and_then(|f| f.valid_from.as_deref()));
        to.append_option(f.and_then(|f| f.valid_to.as_deref()));
        data.append_option(f.map(|f| f.data.to_string()));
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(op.finish()),
        Arc::new(offset.finish()),
        Arc::new(ts.finish()),
        Arc::new(id.finish()),
        Arc::new(version.finish()),
        Arc::new(scope.finish()),
        Arc::new(ns.finish()),
        Arc::new(subject.finish()),
        Arc::new(predicate.finish()),
        Arc::new(object.finish()),
        Arc::new(value.finish()),
        Arc::new(confidence.finish()),
        Arc::new(source.finish()),
        Arc::new(agent.finish()),
        Arc::new(from.finish()),
        Arc::new(to.finish()),
        Arc::new(data.finish()),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| Status::internal(e.to_string()))
}

impl AdbFlight {
    /// The fact feed (GR-15): one read, as Arrow batches.
    async fn fact_feed(&self, q: &Value) -> Result<BoxStream<FlightData>, Status> {
        let gateway = self.engine.gateway().ok_or_else(|| Status::failed_precondition("the fact feed needs a durable engine (start ADB with --data-dir)"))?;
        let s = |k: &str| q.get(k).and_then(|v| v.as_str()).map(String::from);
        let query = adb_gateway::FactQuery {
            from: q.get("from").and_then(|v| v.as_u64()),
            scope: s("scope"),
            namespace: s("namespace"),
            max: q.get("max").and_then(|v| v.as_u64()).map(|m| m as usize).unwrap_or(FACT_MAX).max(1),
            wait: std::time::Duration::from_millis(q.get("wait_ms").and_then(|v| v.as_u64()).unwrap_or(0).min(60_000)),
        };
        let b = gateway.facts(&query).await.map_err(|e| Status::internal(e.to_string()))?;
        let schema = fact_schema(b.next, b.tip, b.snapshot);
        let batches = b.changes.chunks(FACT_BATCH).map(|c| fact_batch(&schema, c)).collect::<Result<Vec<_>, _>>()?;
        tracing::debug!(rows = b.changes.len(), snapshot = b.snapshot, next = b.next, "fact feed read");
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Box::pin(stream))
    }

    fn flowr_engine(&self, b: &Value) -> Result<Arc<dyn flowr_storage::engine::EngineStore>, Status> {
        let space = str_field(b, "space")?;
        self.engine
            .flowr_engine(space)
            .ok_or_else(|| Status::failed_precondition("FlowR's engine store needs a durable ADB (start it with --data-dir)"))
    }

    /// One engine store op, in the gateway's envelope (stage H).
    async fn engine_op(&self, b: &Value) -> Result<Value, Status> {
        let store = self.flowr_engine(b)?;
        let op = str_field(b, "op")?;
        let args = b.get("args").cloned().unwrap_or(Value::Null);
        let t0 = std::time::Instant::now();
        let r = flowr_storage::engine_wire::dispatch(store.as_ref(), op, &args).await;
        // One line per engine store call (`RUST_LOG=adb::engine=debug`).
        tracing::debug!(target: "adb::engine", op, ok = r.is_ok(), us = t0.elapsed().as_micros() as u64, "engine op");
        Ok(match r {
            Ok(v) => json!({ "ok": v }),
            Err(e) => json!({ "err": flowr_storage::engine_wire::err_json(&e) }),
        })
    }

    /// Engine notices on a topic, as a stream of results.
    fn engine_subscribe(&self, b: &Value) -> Result<BoxStream<arrow_flight::Result>, Status> {
        let store = self.flowr_engine(b)?;
        let topic = str_field(b, "topic")?;
        let notices = store.subscribe(topic).map(|v| Ok(arrow_flight::Result { body: v.to_string().into() }));
        Ok(Box::pin(notices))
    }

    fn platform(&self) -> Result<Arc<adb_platform::PlatformStore>, Status> {
        self.engine
            .platform()
            .ok_or_else(|| Status::failed_precondition("platform tables need a durable engine (start ADB with --data-dir)"))
    }

    async fn write_many(&self, effects: Vec<Effect>) -> Result<u64, Status> {
        self.engine.platform_write_many(effects).await.map_err(|e| {
            if e.starts_with("no platform table") { Status::not_found(e) } else { Status::invalid_argument(e) }
        })
    }

    async fn write(&self, effect: Effect) -> Result<u64, Status> {
        self.engine.platform_write(effect).await.map_err(|e| {
            if e.starts_with("no platform table") { Status::not_found(e) } else { Status::invalid_argument(e) }
        })
    }

    async fn action(&self, kind: &str, b: Value) -> Result<Value, Status> {
        match kind {
            "list_tables" => Ok(json!(self.platform()?.list_tables().map_err(platform_err)?)),
            "create_table" => {
                let table = str_field(&b, "table")?.to_string();
                let p = self.platform()?;
                if p.list_tables().map_err(platform_err)?.contains(&table) {
                    return Ok(json!({ "created": false, "exists": true, "table": table }));
                }
                // `call:lancedb` takes a `schema` object (every column UTF-8); a plain
                // `columns` list is accepted too.
                let mut columns: Vec<String> = match (b.get("columns").and_then(|c| c.as_array()), b.get("schema").and_then(|s| s.as_object())) {
                    (Some(c), _) => c.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
                    (None, Some(s)) => s.keys().cloned().collect(),
                    _ => Vec::new(),
                };
                columns.dedup();
                let create = Effect::TableCreate { table: table.clone(), columns };
                // DB-13: `append_only: true` marks it in the same record.
                let offset = match b.get("append_only").and_then(|v| v.as_bool()).unwrap_or(false) {
                    true => self.write_many(vec![create, Effect::TableAppendOnly { table: table.clone() }]).await?,
                    false => self.write(create).await?,
                };
                Ok(json!({ "created": true, "exists": false, "table": table, "offset": offset }))
            }
            "update" => {
                let table = str_field(&b, "table")?.to_string();
                let filter = str_field(&b, "filter")?.to_string();
                let values = b
                    .get("values")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| Status::invalid_argument("update needs a \"values\" object"))?
                    .iter()
                    .map(|(k, v)| (k.clone(), sql_literal(v)))
                    .collect();
                let offset = self.write(Effect::TableUpdate { table, filter, values }).await?;
                Ok(json!({ "updated": true, "offset": offset }))
            }
            "replace_table" => {
                let table = str_field(&b, "table")?.to_string();
                let columns: Vec<String> = b
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .map(|c| c.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                let offset = self.write(Effect::TableReplace { table, columns, rows: Vec::new() }).await?;
                Ok(json!({ "replaced": true, "offset": offset }))
            }
            "delete" => {
                let table = str_field(&b, "table")?.to_string();
                let filter = str_field(&b, "filter")?.to_string();
                let offset = self.write(Effect::TableDelete { table, filter }).await?;
                Ok(json!({ "deleted": true, "offset": offset }))
            }
            // DB-5: platform-table changes after `from` (long poll up to `wait_ms`):
            // `{from?, tables?, max?, wait_ms?}` → `{changes, reset, next, tip}`.
            "table_changes" => {
                let gateway = self.engine.gateway().ok_or_else(|| Status::failed_precondition("the table feed needs a durable engine (start ADB with --data-dir)"))?;
                let q = adb_gateway::tables::TableQuery {
                    from: b.get("from").and_then(|v| v.as_u64()),
                    tables: string_list(b.get("tables")),
                    max: b.get("max").and_then(|v| v.as_u64()).map(|m| m as usize).unwrap_or(1000),
                    wait: std::time::Duration::from_millis(b.get("wait_ms").and_then(|v| v.as_u64()).unwrap_or(0).min(60_000)),
                };
                let t = gateway.table_changes(&q).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(json!({ "changes": t.changes, "reset": t.reset, "next": t.next, "tip": t.tip }))
            }
            // DB-10 / DB-19 (`.ai/MERGE.md`): three-way diff, merge back, revert.
            "branch_conflicts" | "branch_merge" | "branch_rebase" | "merge_revert" => {
                let ns = str_field(&b, "namespace")?;
                let memories: Vec<adb_core::MemoryType> = serde_json::from_value(b.get("memories").cloned().unwrap_or(json!([])))
                    .map_err(|e| Status::invalid_argument(format!("memories: {e}")))?;
                let r = match kind {
                    "branch_conflicts" => self.engine.branch_conflicts(ns, str_field(&b, "name")?, &memories).await,
                    "branch_merge" => {
                        let main_offset = b.get("main_offset").and_then(|v| v.as_u64()).ok_or_else(|| Status::invalid_argument("branch_merge needs main_offset (main.offset from branch_conflicts)"))?;
                        let resolutions: Vec<adb_executor::merge::Resolution> = serde_json::from_value(b.get("resolutions").cloned().unwrap_or(json!([])))
                            .map_err(|e| Status::invalid_argument(format!("resolutions: {e}")))?;
                        let by = b.get("merged_by").and_then(|v| v.as_str());
                        self.engine.branch_merge(ns, str_field(&b, "name")?, &memories, main_offset, resolutions, by).await
                    }
                    "branch_rebase" => {
                        let main_offset = b.get("main_offset").and_then(|v| v.as_u64()).ok_or_else(|| Status::invalid_argument("branch_rebase needs main_offset (main.offset from branch_conflicts)"))?;
                        let resolutions: Vec<adb_executor::merge::Resolution> = serde_json::from_value(b.get("resolutions").cloned().unwrap_or(json!([])))
                            .map_err(|e| Status::invalid_argument(format!("resolutions: {e}")))?;
                        self.engine.branch_rebase(ns, str_field(&b, "name")?, main_offset, resolutions).await
                    }
                    _ => {
                        let by = b.get("reverted_by").and_then(|v| v.as_str());
                        let force = b.get("force_skip").and_then(|v| v.as_bool()).unwrap_or(false);
                        self.engine.merge_revert(ns, str_field(&b, "merge_id")?, by, force).await
                    }
                };
                r.map_err(merge_status)
            }
            // DB-6: several operations as one transaction (one log record, applied in
            // order): `{ops: [{op: "insert" | "upsert" | "update" | "delete" |
            // "create_table" | "add_columns" | "append_only" | "purge", …that action's
            // fields}]}`.
            "transaction" => {
                let ops = b.get("ops").and_then(|o| o.as_array()).ok_or_else(|| Status::invalid_argument("transaction needs ops: [{op, …}]"))?;
                let effects = ops
                    .iter()
                    .map(|o| table_effect(o.get("op").and_then(|v| v.as_str()).unwrap_or_default(), o))
                    .collect::<Result<Vec<_>, _>>()?;
                let n = effects.len();
                let offset = self.write_many(effects).await?;
                Ok(json!({ "committed": true, "ops": n, "offset": offset }))
            }
            // DB-6: new UTF-8 columns on an existing table (old rows read null).
            "add_columns" | "append_only" | "purge" => {
                let offset = self.write(table_effect(kind, &b)?).await?;
                Ok(json!({ "done": true, "offset": offset }))
            }
            // Memory branches (B1, agents as repos): cut one at the current log
            // position (nothing copied), list, drop.
            "branch_create" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                let parent = b.get("parent").and_then(|p| p.as_str());
                let by = b.get("created_by").and_then(|p| p.as_str());
                if let Some(at) = b.get("at") {
                    if parent.is_some_and(|p| !p.is_empty() && p != adb_executor::branches::MAIN) {
                        return Err(Status::invalid_argument("point_invalid: a branch at a past point is cut from main"));
                    }
                    return self.engine.branch_create_past(ns, name, by, &point_ref(at)?).await.map_err(Status::invalid_argument);
                }
                let br = self.engine.branch_create(ns, name, parent, by).await.map_err(Status::invalid_argument)?;
                Ok(serde_json::to_value(br).unwrap_or_default())
            }
            "branch_list" => {
                let ns = b.get("namespace").and_then(|n| n.as_str());
                let branches = self.engine.branch_list(ns).await.map_err(Status::internal)?;
                // DB-14: opt-in, as N branches mean N scans.
                if b.get("stats").and_then(|v| v.as_bool()).unwrap_or(false) {
                    let mut out = Vec::with_capacity(branches.len());
                    for br in &branches {
                        let mut v = serde_json::to_value(br).unwrap_or_default();
                        v["stats"] = self.engine.space_stats(&br.namespace, Some(&br.name), STATS_MAX_AGE).await.map_err(Status::internal)?;
                        out.push(v);
                    }
                    return Ok(json!({ "branches": out }));
                }
                Ok(json!({ "branches": branches }))
            }
            "branch_drop" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                Ok(json!({ "dropped": self.engine.branch_drop(ns, name).await.map_err(Status::invalid_argument)? }))
            }
            // B5: a branch promoted as a new agent; a space cloned (main, now).
            "branch_promote" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                let into = str_field(&b, "into")?;
                self.engine
                    .branch_promote(ns, name, into, b.get("allow_stale").and_then(|v| v.as_bool()).unwrap_or(false))
                    .await
                    .map_err(merge_status)
            }
            "space_clone" => {
                let ns = str_field(&b, "namespace")?;
                let into = str_field(&b, "into")?;
                // DB-17: `{procedures_only: true, tools?}`, for a copy to another org.
                if b.get("procedures_only").and_then(|v| v.as_bool()).unwrap_or(false) {
                    if b.get("at").is_some() {
                        return Err(Status::invalid_argument("point_invalid: a procedures-only copy is of main now"));
                    }
                    return self.engine.space_copy_procedures(ns, into, b.get("tools").and_then(|v| v.as_bool()).unwrap_or(false)).await.map_err(Status::invalid_argument);
                }
                if let Some(at) = b.get("at") {
                    return self.engine.space_clone_at(ns, into, &point_ref(at)?).await.map_err(Status::invalid_argument);
                }
                self.engine.space_clone(ns, into).await.map_err(Status::invalid_argument)
            }
            // DB-14: stats, paging and the feed of one memory space.
            "space_stats" => {
                let max_age = b.get("max_age_ms").and_then(|v| v.as_u64()).map(std::time::Duration::from_millis).unwrap_or(STATS_MAX_AGE);
                self.engine.space_stats(str_field(&b, "namespace")?, b.get("branch").and_then(|v| v.as_str()), max_age).await.map_err(Status::invalid_argument)
            }
            "space_records" => {
                let memory: adb_core::MemoryType = serde_json::from_value(b.get("memory").cloned().unwrap_or_default())
                    .map_err(|e| Status::invalid_argument(format!("memory: {e}")))?;
                let limit = b.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
                self.engine
                    .space_records(str_field(&b, "namespace")?, memory, b.get("branch").and_then(|v| v.as_str()), b.get("after").and_then(|v| v.as_str()), limit)
                    .await
                    .map_err(Status::invalid_argument)
            }
            "space_changes" => {
                let ns = str_field(&b, "namespace")?;
                let gateway = self.engine.space_gateway(ns, b.get("branch").and_then(|v| v.as_str())).await.map_err(Status::invalid_argument)?;
                let q = adb_gateway::tables::SpaceQuery {
                    namespace: ns.to_string(),
                    from: b.get("from").and_then(|v| v.as_u64()),
                    memories: string_list(b.get("memories")),
                    max: b.get("max").and_then(|v| v.as_u64()).map(|m| m as usize).unwrap_or(1000),
                    wait: std::time::Duration::from_millis(b.get("wait_ms").and_then(|v| v.as_u64()).unwrap_or(0).min(60_000)),
                };
                let t = gateway.space_changes(&q).await.map_err(|e| Status::internal(e.to_string()))?;
                Ok(json!({ "changes": t.changes, "reset": t.reset, "next": t.next, "tip": t.tip }))
            }
            // DB-4: storage quotas and usage.
            "quota_set" => {
                let q = self.engine.quota_set(str_field(&b, "pattern")?, b.get("max_bytes").and_then(|v| v.as_u64()), b.get("max_records").and_then(|v| v.as_u64()), b.get("history_days").and_then(|v| v.as_u64())).await.map_err(Status::invalid_argument)?;
                Ok(serde_json::to_value(q).unwrap_or_default())
            }
            "quota_drop" => Ok(json!({ "dropped": self.engine.quota_drop(str_field(&b, "pattern")?).await.map_err(Status::invalid_argument)? })),
            "quota_list" => Ok(json!({ "quotas": self.engine.quota_list().await.map_err(Status::internal)? })),
            "space_usage" => self.engine.space_usage(b.get("namespace").and_then(|v| v.as_str())).await.map_err(Status::internal),
            // DB-12: who reads a space as a layer.
            "layers_dependents" => {
                let space = str_field(&b, "space")?;
                Ok(json!({ "dependents": self.engine.layers_dependents(space).await.map_err(Status::internal)? }))
            }
            // B6: branches in their own pod. The org: a parent's rows AS OF a time,
            // placing a branch in a pod, its pending FORGETs. The pod: erase lineages.
            // B7: tags (named points of a space) and layers (inherited spaces).
            "tag_create" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                let by = b.get("created_by").and_then(|p| p.as_str());
                Ok(serde_json::to_value(self.engine.tag_create(ns, name, by).await.map_err(Status::invalid_argument)?).unwrap_or_default())
            }
            "tag_list" => {
                let ns = b.get("namespace").and_then(|n| n.as_str());
                Ok(json!({ "tags": self.engine.tag_list(ns).await.map_err(Status::internal)? }))
            }
            "tag_drop" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                Ok(json!({ "dropped": self.engine.tag_drop(ns, name).await.map_err(Status::invalid_argument)? }))
            }
            "layers_set" => {
                let ns = str_field(&b, "namespace")?;
                let layers: Vec<adb_executor::branches::Layer> = serde_json::from_value(b.get("layers").cloned().unwrap_or(json!([])))
                    .map_err(|e| Status::invalid_argument(format!("layers: {e}")))?;
                Ok(serde_json::to_value(self.engine.layers_set(ns, layers).await.map_err(Status::invalid_argument)?).unwrap_or_default())
            }
            "layers_get" => {
                let ns = str_field(&b, "namespace")?;
                Ok(json!({ "layers": self.engine.layers_get(ns).await.map_err(Status::internal)? }))
            }
            "branch_parent_rows" => {
                let memory: adb_core::MemoryType = serde_json::from_value(b.get("memory").cloned().unwrap_or_default())
                    .map_err(|e| Status::invalid_argument(format!("memory: {e}")))?;
                let ns = str_field(&b, "namespace")?;
                let branch = b.get("branch").and_then(|v| v.as_str());
                let at = b.get("at").and_then(|v| v.as_i64()).ok_or_else(|| Status::invalid_argument("missing \"at\" (commit time, µs)"))?;
                let rows = self.engine.parent_rows(memory, ns, branch, at).await.map_err(Status::failed_precondition)?;
                Ok(json!({ "rows": rows }))
            }
            "branch_place" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                let url = str_field(&b, "url")?;
                let br = self.engine.branch_place(ns, name, url).await.map_err(Status::invalid_argument)?;
                Ok(serde_json::to_value(br).unwrap_or_default())
            }
            "branch_pending" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                Ok(json!({ "forgets": self.engine.branch_pending(ns, name).await.map_err(Status::internal)? }))
            }
            "branch_pending_ack" => {
                let ns = str_field(&b, "namespace")?;
                let name = str_field(&b, "name")?;
                let n = b.get("count").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                self.engine.branch_pending_ack(ns, name, n).await.map_err(Status::internal)?;
                Ok(json!({ "acked": n }))
            }
            "branch_forget" => {
                let memory: adb_core::MemoryType = serde_json::from_value(b.get("memory").cloned().unwrap_or_default())
                    .map_err(|e| Status::invalid_argument(format!("memory: {e}")))?;
                let lineages: Vec<String> = serde_json::from_value(b.get("lineages").cloned().unwrap_or_default())
                    .map_err(|e| Status::invalid_argument(format!("lineages: {e}")))?;
                let n = self.engine.forget_lineages(memory, &lineages).await.map_err(Status::failed_precondition)?;
                Ok(json!({ "erased": n }))
            }
            // CA-8 / CA-24: one write for an insert-or-replace by key (FlowR's state
            // and registry writes). `{table, key: [cols], rows: [{…}]}`; values are
            // kept as text, as the platform tables store them.
            "upsert" => {
                let table = str_field(&b, "table")?.to_string();
                let key: Vec<String> = b
                    .get("key")
                    .and_then(|k| k.as_array())
                    .map(|a| a.iter().filter_map(|c| c.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                let rows: Vec<serde_json::Map<String, Value>> = b
                    .get("rows")
                    .and_then(|r| r.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|r| r.as_object())
                            .map(|r| r.iter().map(|(k, v)| (k.clone(), match v { Value::String(_) | Value::Null => v.clone(), other => Value::String(other.to_string()) })).collect())
                            .collect()
                    })
                    .unwrap_or_default();
                if rows.is_empty() {
                    return Ok(json!({ "upserted": 0 }));
                }
                let n = rows.len();
                let offset = self.write(Effect::TableUpsert { table, key, rows }).await?;
                Ok(json!({ "upserted": n, "offset": offset }))
            }
            // W5: compact now (admin; Flight is behind the token when ADB has one).
            "compact" => self.engine.compact().await.map_err(Status::failed_precondition),
            "aql" => {
                let query = str_field(&b, "query")?;
                let vars: Option<HashMap<String, Value>> =
                    b.get("variables").and_then(|v| v.as_object()).map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                // Same answer shape as POST /query.
                Ok(match self.engine.query(query, vars.as_ref()).await {
                    Ok(data) => json!({ "success": true, "data": data }),
                    Err(e) => json!({ "success": false, "code": adb_core::codes::code_of(&e), "error": e }),
                })
            }
            other => Err(Status::unimplemented(format!("unknown action {other}"))),
        }
    }

    /// `do_action`'s work; refusals get their stable code added by [`with_code`].
    async fn do_action_inner(&self, req: Request<Action>) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let access = access(&req);
        let pool = req.extensions().get::<crate::auth::EnginePool>().cloned();
        let scope = req.extensions().get::<Arc<crate::scope::Scope>>().cloned();
        let a = req.into_inner();
        if a.r#type.starts_with("engine") {
            if access == Access::Memory || access == Access::Scoped {
                return Err(Status::permission_denied("FlowR's engine store needs the engine token (ADB_ENGINE_TOKEN)"));
            }
            let b = body(&a.body)?;
            if let Some(crate::auth::EnginePool(p)) = &pool {
                crate::auth::pool_allows(p, &a.r#type, &b).map_err(Status::permission_denied)?;
            }
            if a.r#type == "engine_subscribe" {
                return Ok(Response::new(self.engine_subscribe(&b)?));
            }
            if a.r#type == "engine_restore" {
                let space = str_field(&b, "space")?;
                let changes = b.get("changes").and_then(|c| c.as_array()).cloned().ok_or_else(|| Status::invalid_argument("engine_restore needs changes"))?;
                let first = b.get("first").and_then(|f| f.as_bool()).unwrap_or(false);
                let n = self.engine.engine_restore(space, changes, first).await.map_err(Status::failed_precondition)?;
                let r = arrow_flight::Result { body: json!({ "restored": n }).to_string().into() };
                return Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))));
            }
            if a.r#type != "engine" {
                return Err(Status::invalid_argument(format!("unknown action {}", a.r#type)));
            }
            let r = arrow_flight::Result { body: self.engine_op(&b).await?.to_string().into() };
            return Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))));
        }
        if access == Access::Engine {
            return Err(Status::permission_denied("the engine token opens only FlowR's engine store"));
        }
        let b = body(&a.body)?;
        if let Some(scope) = &scope {
            scope.check_action(&a.r#type, &b).map_err(Status::permission_denied)?;
        }
        let answer = self.action(&a.r#type, b).await?;
        let r = arrow_flight::Result { body: answer.to_string().into() };
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))))
    }
}

/// A merge refusal as its gRPC status (the code goes in `adb-code`, [`with_code`]).
fn merge_status(e: String) -> Status {
    match e.split(':').next().unwrap_or("") {
        "merge_conflicts" | "merge_stale" | "revert_conflicts" => Status::failed_precondition(e),
        "merge_not_found" => Status::not_found(e),
        _ => Status::invalid_argument(e),
    }
}

/// How long a space's stats are kept unless the caller says otherwise (DB-14).
const STATS_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(10);

/// DB-9: `at = {tag} | {offset, ts?}`.
fn point_ref(at: &Value) -> Result<adb_executor::branches::PointRef, Status> {
    serde_json::from_value(at.clone()).map_err(|e| Status::invalid_argument(format!("point_invalid: at: {e}")))
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(|c| c.as_array()).map(|a| a.iter().filter_map(|c| c.as_str().map(String::from)).collect()).unwrap_or_default()
}

/// Rows as the platform tables store them: every value text (null stays null).
fn table_rows(b: &Value) -> Vec<serde_json::Map<String, Value>> {
    b.get("rows")
        .and_then(|r| r.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|r| r.as_object())
                .map(|r| r.iter().map(|(k, v)| (k.clone(), match v { Value::String(_) | Value::Null => v.clone(), other => Value::String(other.to_string()) })).collect())
                .collect()
        })
        .unwrap_or_default()
}

/// One platform-table operation as its log effect (the single actions and the steps
/// of a `transaction`).
fn table_effect(op: &str, b: &Value) -> Result<Effect, Status> {
    let table = str_field(b, "table")?.to_string();
    Ok(match op {
        "create_table" => {
            let mut columns = string_list(b.get("columns"));
            if columns.is_empty() {
                columns = b.get("schema").and_then(|s| s.as_object()).map(|s| s.keys().cloned().collect()).unwrap_or_default();
            }
            Effect::TableCreate { table, columns }
        }
        "insert" => Effect::TableInsert { table, rows: table_rows(b) },
        "upsert" => Effect::TableUpsert { table, key: string_list(b.get("key")), rows: table_rows(b) },
        "update" => {
            let filter = str_field(b, "filter")?.to_string();
            let values = b
                .get("values")
                .and_then(|v| v.as_object())
                .ok_or_else(|| Status::invalid_argument("update needs a \"values\" object"))?
                .iter()
                .map(|(k, v)| (k.clone(), sql_literal(v)))
                .collect();
            Effect::TableUpdate { table, filter, values }
        }
        "delete" => Effect::TableDelete { table, filter: str_field(b, "filter")?.to_string() },
        "add_columns" => Effect::TableAddColumns { table, columns: string_list(b.get("columns")) },
        "append_only" => Effect::TableAppendOnly { table },
        "purge" => Effect::TablePurge { table, column: str_field(b, "column")?.to_string(), before: str_field(b, "before")?.to_string() },
        other => return Err(Status::invalid_argument(format!("unknown table operation {other:?}"))),
    })
}

/// A refusal with its stable code (DB-18) in the `adb-code` metadata: from the
/// message, else from the gRPC status (permission denied, unauthenticated).
fn with_code(mut s: Status) -> Status {
    let code = match adb_core::codes::code_of(s.message()) {
        "internal" => match s.code() {
            tonic::Code::PermissionDenied => "forbidden",
            tonic::Code::Unauthenticated => "unauthorized",
            tonic::Code::NotFound => "not_found",
            tonic::Code::Unimplemented => "unsupported",
            _ => "internal",
        },
        c => c,
    };
    if let Ok(v) = code.parse() {
        s.metadata_mut().insert("adb-code", v);
    }
    s
}

#[tonic::async_trait]
impl FlightService for AdbFlight {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        not_engine_only(&req)?;
        let scope = req.extensions().get::<Arc<crate::scope::Scope>>().cloned();
        let q = body(&req.into_inner().ticket)?;
        // DB-3: a scoped token follows its own namespace's fact feed, and reads no table.
        if let Some(scope) = &scope {
            let feed = q.get("feed").and_then(|f| f.as_str()) == Some("facts");
            let ns = q.get("namespace").and_then(|n| n.as_str()).unwrap_or_default();
            if !feed || !scope.may(ns, false) {
                return Err(Status::permission_denied(format!("this read is not open to the {} token", scope.name)));
            }
        }
        match q.get("feed").and_then(|f| f.as_str()) {
            Some("facts") => return Ok(Response::new(self.fact_feed(&q).await?)),
            Some(other) => return Err(Status::invalid_argument(format!("unknown feed {other} (the feed is \"facts\")"))),
            None => {}
        }
        if let Some(sql) = q.get("sql").and_then(|s| s.as_str()) {
            let limit = q.get("limit").and_then(|l| l.as_u64()).map(|l| l as usize);
            let r = self.engine.sql(sql, limit).await.map_err(Status::invalid_argument)?;
            tracing::info!(rows = r.batches.iter().map(|b| b.num_rows()).sum::<usize>(), "SQL console query");
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(r.schema)
                .build(futures::stream::iter(r.batches.into_iter().map(Ok)))
                .map_err(|e| Status::internal(e.to_string()));
            return Ok(Response::new(Box::pin(stream)));
        }
        let table = str_field(&q, "table")?;
        let filter = q.get("filter").and_then(|f| f.as_str());
        let limit = q.get("limit").and_then(|l| l.as_u64()).unwrap_or(100) as usize;
        let p = self.platform()?;
        let schema = p.schema(table).await.map_err(platform_err)?;
        let batches = p.query(table, filter, limit).await.map_err(platform_err)?;
        // The schema goes first even with no rows, so the client always gets one.
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_put(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        not_engine_only(&req)?;
        not_scoped(&req)?;
        let mut stream = req.into_inner().peekable();
        // The table comes in the first message's descriptor; `[table, "replace"]`
        // replaces the table with these rows instead of appending them;
        // `[table, "upsert", key…]` replaces the rows with the same key columns.
        let (table, replace, upsert_key) = {
            let first = Pin::new(&mut stream).peek().await.ok_or_else(|| Status::invalid_argument("empty do_put"))?;
            let first = first.as_ref().map_err(|e| Status::invalid_argument(e.to_string()))?;
            let path = first.flight_descriptor.as_ref().map(|d| d.path.clone()).unwrap_or_default();
            let table = path.first().cloned().ok_or_else(|| Status::invalid_argument("do_put needs a descriptor path [table]"))?;
            let mode = path.get(1).map(String::as_str);
            let key: Vec<String> = if mode == Some("upsert") { path[2..].to_vec() } else { Vec::new() };
            if mode == Some("upsert") && key.is_empty() {
                return Err(Status::invalid_argument("an upsert needs key columns: [table, \"upsert\", key…]"));
            }
            (table, mode == Some("replace"), key)
        };
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        )
        .try_collect()
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let rows = batches_to_rows(&batches);
        let n = rows.len();
        let offset = if replace {
            let columns: Vec<String> = batches
                .first()
                .map(|b| b.schema().fields().iter().map(|f| f.name().clone()).collect())
                .unwrap_or_default();
            self.write(Effect::TableReplace { table, columns, rows }).await?
        } else if n == 0 {
            0
        } else if !upsert_key.is_empty() {
            self.write(Effect::TableUpsert { table, key: upsert_key, rows }).await?
        } else {
            self.write(Effect::TableInsert { table, rows }).await?
        };
        let r = PutResult { app_metadata: json!({ "inserted": n, "offset": offset }).to_string().into() };
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))))
    }

    async fn do_action(&self, req: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        self.do_action_inner(req).await.map_err(with_code)
    }



    async fn list_actions(&self, _: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            ("create_table", "Create a platform table: {table, columns? | schema?}"),
            ("update", "Set values on matching rows: {table, filter, values}"),
            ("delete", "Delete matching rows: {table, filter}"),
            ("transaction", "Several table operations as one record (DB-6): {ops: [{op, table, …}]}; op = insert | upsert | update | delete | create_table | add_columns | append_only | purge"),
            ("branch_conflicts", "Three-way diff of a branch against main (DB-10): {namespace, name, memories?} → {base, main, clean, conflicts}"),
            ("branch_merge", "Merge a branch back into main (DB-19, kind 2): {namespace, name, memories? (PROCEDURAL), main_offset, resolutions, merged_by?} → {merge_id, offset, changed, added, ended}"),
            ("branch_rebase", "Move a branch to main now (DB-11, kind 1): {namespace, name, main_offset, resolutions} → {point, kept, dropped, edited}"),
            ("merge_revert", "Undo a merge (DB-19): {namespace, merge_id, reverted_by?, force_skip?} → {reverted, skipped, offset}"),
            ("table_changes", "Platform-table change feed (DB-5): {from?, tables?, max?, wait_ms?} → {changes, reset, next, tip}; reset: reload, then follow from next"),
            ("space_stats", "Per memory type counts of a space (DB-14): {namespace, branch?, max_age_ms?} → {tiers: {type: {count, bytes, last_write_ts}}, age_ms}"),
            ("space_records", "A page of a space's current rows, newest first (DB-14): {namespace, memory, branch?, after?, limit?} → {records, next}"),
            ("space_changes", "A space's change feed (DB-14): {namespace, branch?, memories?, from?, max?, wait_ms?} → {changes, reset, next, tip}"),
            ("quota_set", "Storage limit (DB-4) and history window (DB-8): {pattern, max_bytes?, max_records?, history_days?}; a pattern is a namespace, or a prefix ending in *"),
            ("quota_drop", "Remove a quota: {pattern}"),
            ("quota_list", "The quotas: {quotas}"),
            ("space_usage", "Records and payload bytes per namespace (DB-4): {namespace?} → {namespaces}"),
            ("layers_dependents", "Who reads a space as a layer (DB-12): {space} → {dependents: [{namespace, mode, tag?, sealed, memories}]}"),
            ("add_columns", "Add UTF-8 columns to a table (DB-6): {table, columns}"),
            ("append_only", "Make a table append-only (DB-13): {table}; also create_table {append_only: true}"),
            ("purge", "Retention (DB-13): delete rows whose {column} is before {before}: {table, column, before}"),
            ("compact", "Compact the memory table and platform tables now: drop forgotten / expired / deleted data and old history (W5)"),
            ("list_tables", "Platform table names"),
            ("replace_table", "Replace a table with an empty one: {table, columns}"),
            ("aql", "Run AQL: {query, variables?} → the POST /query result"),
            ("engine", "FlowR's engine store: {space, op, args} → {ok} | {err} (engine token)"),
            ("engine_subscribe", "FlowR engine notices: {space, topic} → a stream of JSON notices (engine token)"),
            ("engine_restore", "Load FlowR engine state: {space, changes, first?} → {restored} (engine token; adb import-flowr-engine)"),
        ]
        .map(|(t, d)| Ok(ActionType { r#type: t.into(), description: d.into() }));
        Ok(Response::new(Box::pin(futures::stream::iter(actions))))
    }

    async fn handshake(&self, _: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("no handshake: ADB Flight is unauthenticated inside the cluster"))
    }
    async fn list_flights(&self, _: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("use do_action(list_tables)"))
    }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("use do_get with a JSON ticket"))
    }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("not supported"))
    }
    async fn get_schema(&self, req: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        not_scoped(&req)?;
        let table = req.into_inner().path.first().cloned().ok_or_else(|| Status::invalid_argument("descriptor path [table]"))?;
        let schema = self.platform()?.schema(&table).await.map_err(platform_err)?;
        let opts = arrow_ipc::writer::IpcWriteOptions::default();
        let r: SchemaResult = arrow_flight::SchemaAsIpc::new(&schema, &opts).try_into().map_err(|e: arrow_schema::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(r))
    }
    async fn do_exchange(&self, _: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("not supported"))
    }
}

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
    let svc = tonic::service::interceptor::InterceptedService::new(svc, crate::auth::FlightAuth { memory: token, engine: engine_token, pools });
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
        Ok(match flowr_storage::engine_wire::dispatch(store.as_ref(), op, &args).await {
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
                let offset = self.write(Effect::TableCreate { table: table.clone(), columns }).await?;
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
            // W5: compact now (admin; Flight is behind the token when ADB has one).
            "compact" => self.engine.compact().await.map_err(Status::failed_precondition),
            "aql" => {
                let query = str_field(&b, "query")?;
                let vars: Option<HashMap<String, Value>> =
                    b.get("variables").and_then(|v| v.as_object()).map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                // Same answer shape as POST /query.
                Ok(match self.engine.query(query, vars.as_ref()).await {
                    Ok(data) => json!({ "success": true, "data": data }),
                    Err(e) => json!({ "success": false, "error": e }),
                })
            }
            other => Err(Status::unimplemented(format!("unknown action {other}"))),
        }
    }
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
        let q = body(&req.into_inner().ticket)?;
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
        let mut stream = req.into_inner().peekable();
        // The table comes in the first message's descriptor; `[table, "replace"]`
        // replaces the table with these rows instead of appending them.
        let (table, replace) = {
            let first = Pin::new(&mut stream).peek().await.ok_or_else(|| Status::invalid_argument("empty do_put"))?;
            let first = first.as_ref().map_err(|e| Status::invalid_argument(e.to_string()))?;
            let path = first.flight_descriptor.as_ref().map(|d| d.path.clone()).unwrap_or_default();
            let table = path.first().cloned().ok_or_else(|| Status::invalid_argument("do_put needs a descriptor path [table]"))?;
            (table, path.get(1).map(|p| p == "replace").unwrap_or(false))
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
        } else {
            self.write(Effect::TableInsert { table, rows }).await?
        };
        let r = PutResult { app_metadata: json!({ "inserted": n, "offset": offset }).to_string().into() };
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))))
    }

    async fn do_action(&self, req: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        let access = access(&req);
        let pool = req.extensions().get::<crate::auth::EnginePool>().cloned();
        let a = req.into_inner();
        if a.r#type.starts_with("engine") {
            if access == Access::Memory {
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
        let answer = self.action(&a.r#type, body(&a.body)?).await?;
        let r = arrow_flight::Result { body: answer.to_string().into() };
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))))
    }

    async fn list_actions(&self, _: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            ("create_table", "Create a platform table: {table, columns? | schema?}"),
            ("update", "Set values on matching rows: {table, filter, values}"),
            ("delete", "Delete matching rows: {table, filter}"),
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

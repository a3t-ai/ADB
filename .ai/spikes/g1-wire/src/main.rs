//! Spike G1 (stage G, G5): HTTP + JSON vs Arrow Flight for A3T platform-table calls.
//!
//! Both servers serve the same in-memory table, so only the wire differs. The
//! client measures what FlowR would see end to end, including turning Flight's Arrow
//! batches into the JSON rows A3T's flows consume (`batches_to_json`), and building
//! batches from JSON records for inserts (`records_to_batch`), as FlowR's
//! `call:lancedb` handler does today.
//!
//! Calls (A3T-shaped):
//! - lookup: 1 row by id + org (template / agent detail) out of 1,000;
//! - list:   100 rows of `template_registry` shape (each with an 8 KB YAML spec);
//! - insert: 1,000 small rows in one call.
//!
//!   cargo run --release -- [iterations]

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::{Stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use tonic::{Request, Response, Status, Streaming};

const COLS: [&str; 10] = ["id", "org_id", "name", "kind", "version", "status", "owner", "created_at", "updated_at", "description"];

fn schema(with_spec: bool) -> SchemaRef {
    let mut f: Vec<Field> = COLS.iter().map(|c| Field::new(*c, DataType::Utf8, true)).collect();
    if with_spec {
        f.push(Field::new("spec_yaml", DataType::Utf8, true));
    }
    Arc::new(Schema::new(f))
}

fn table(rows: usize, spec_bytes: usize) -> RecordBatch {
    let spec = "x".repeat(spec_bytes);
    let mut cols: Vec<Arc<dyn Array>> = COLS
        .iter()
        .map(|c| {
            let v: Vec<String> = (0..rows)
                .map(|i| match *c {
                    "id" => format!("tpl-{i:06}"),
                    "org_id" => "org-default".to_string(),
                    _ => format!("{c}-value-{i:06}-padding-text"),
                })
                .collect();
            Arc::new(StringArray::from(v)) as Arc<dyn Array>
        })
        .collect();
    cols.push(Arc::new(StringArray::from(vec![spec.as_str(); rows])));
    RecordBatch::try_new(schema(true), cols).unwrap()
}

/// FlowR's `batches_to_json` (call_lancedb.rs).
fn batches_to_json(batches: &[RecordBatch]) -> Vec<Value> {
    let mut rows = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let mut o = serde_json::Map::new();
            for (i, f) in b.schema().fields().iter().enumerate() {
                if let Some(a) = b.column(i).as_any().downcast_ref::<StringArray>() {
                    if a.is_valid(r) {
                        o.insert(f.name().clone(), json!(a.value(r)));
                    }
                }
            }
            rows.push(Value::Object(o));
        }
    }
    rows
}

/// FlowR's `records_to_batch` with an inferred all-Utf8 schema.
fn records_to_batch(records: &[Value]) -> RecordBatch {
    let keys: Vec<String> = records[0].as_object().unwrap().keys().cloned().collect();
    let schema = Arc::new(Schema::new(keys.iter().map(|k| Field::new(k, DataType::Utf8, true)).collect::<Vec<_>>()));
    let cols = keys
        .iter()
        .map(|k| {
            let v: Vec<Option<String>> = records
                .iter()
                .map(|r| r.get(k).map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string())))
                .collect();
            Arc::new(StringArray::from(v)) as Arc<dyn Array>
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

/// The server's query: rows matching `id` (or the first `limit`), as one batch.
fn select(t: &RecordBatch, id: Option<&str>, limit: usize) -> RecordBatch {
    let ids = t.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let idx: Vec<u32> = (0..t.num_rows())
        .filter(|&i| id.is_none_or(|want| ids.value(i) == want))
        .take(limit)
        .map(|i| i as u32)
        .collect();
    let take = arrow_array::UInt32Array::from(idx);
    let cols = t.columns().iter().map(|c| arrow_select_take(c, &take)).collect();
    RecordBatch::try_new(t.schema(), cols).unwrap()
}

fn arrow_select_take(c: &Arc<dyn Array>, idx: &arrow_array::UInt32Array) -> Arc<dyn Array> {
    let s = c.as_any().downcast_ref::<StringArray>().unwrap();
    Arc::new(StringArray::from_iter(idx.values().iter().map(|&i| Some(s.value(i as usize)))))
}

// ---------------------------------------------------------------- HTTP + JSON server

async fn json_server(t: Arc<RecordBatch>, addr: SocketAddr) {
    use axum::routing::post;
    use axum::{extract::State, Json, Router};
    async fn query(State(t): State<Arc<RecordBatch>>, Json(b): Json<Value>) -> Json<Value> {
        let id = b.get("id").and_then(|v| v.as_str());
        let limit = b.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
        Json(json!(batches_to_json(&[select(&t, id, limit)])))
    }
    async fn insert(Json(b): Json<Value>) -> Json<Value> {
        let records = b["records"].as_array().cloned().unwrap_or_default();
        let batch = records_to_batch(&records);
        Json(json!({ "inserted": batch.num_rows() }))
    }
    let app = Router::new().route("/query", post(query)).route("/insert", post(insert)).with_state(t);
    let l = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(l, app).await.unwrap();
}

// ---------------------------------------------------------------- Arrow Flight server

struct Flight {
    t: Arc<RecordBatch>,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for Flight {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let q: Value = serde_json::from_slice(&req.into_inner().ticket).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let id = q.get("id").and_then(|v| v.as_str());
        let limit = q.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
        let batch = select(&self.t, id, limit);
        let stream = FlightDataEncoderBuilder::new()
            .build(futures::stream::iter(vec![Ok(batch)]))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_put(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        let stream = req.into_inner().map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)));
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(stream)
            .try_collect()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        let r = PutResult { app_metadata: json!({ "inserted": n }).to_string().into() };
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(r)]))))
    }

    async fn handshake(&self, _: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn list_flights(&self, _: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn do_action(&self, _: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn list_actions(&self, _: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn do_exchange(&self, _: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(""))
    }
}

// ---------------------------------------------------------------- client

fn report(name: &str, mut t: Vec<Duration>) {
    t.sort();
    let p = |q: f64| t[((t.len() as f64 * q) as usize).min(t.len() - 1)].as_secs_f64() * 1e3;
    let mean = t.iter().map(|d| d.as_secs_f64()).sum::<f64>() / t.len() as f64 * 1e3;
    println!("{name:<26} p50 {:>8.3} ms   p99 {:>8.3} ms   mean {:>8.3} ms", p(0.5), p(0.99), mean);
}

#[tokio::main]
async fn main() {
    let iters: usize = std::env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(500);
    let t = Arc::new(table(1_000, 8 * 1024));
    let (ja, fa): (SocketAddr, SocketAddr) = ("127.0.0.1:39301".parse().unwrap(), "127.0.0.1:39302".parse().unwrap());
    tokio::spawn(json_server(Arc::clone(&t), ja));
    let svc = FlightServiceServer::new(Flight { t: Arc::clone(&t) }).max_decoding_message_size(64 << 20).max_encoding_message_size(64 << 20);
    tokio::spawn(tonic::transport::Server::builder().add_service(svc).serve(fa));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let http = reqwest::Client::new();
    let channel = tonic::transport::Channel::from_static("http://127.0.0.1:39302").connect().await.unwrap();
    let mut flight = arrow_flight::flight_service_client::FlightServiceClient::new(channel)
        .max_decoding_message_size(64 << 20)
        .max_encoding_message_size(64 << 20);

    let small_records: Vec<Value> = (0..1_000)
        .map(|i| json!({ "id": format!("ev-{i}"), "org_id": "org-default", "kind": "audit", "message": format!("event {i} happened"), "at": "2026-10-26T10:00:00Z" }))
        .collect();

    println!("{iters} iterations each; loopback, keep-alive; times include JSON ↔ Arrow on the client as FlowR needs it\n");
    for (name, id, limit) in [("lookup 1 row", Some("tpl-000500"), 1usize), ("list 100 rows × 8 KB", None, 100)] {
        let body = json!({ "table": "template_registry", "id": id, "limit": limit });
        let mut tj = Vec::new();
        let mut tf = Vec::new();
        for i in 0..iters + 20 {
            let s = Instant::now();
            let rows: Value = http.post(format!("http://{ja}/query")).json(&body).send().await.unwrap().json().await.unwrap();
            assert_eq!(rows.as_array().unwrap().len(), limit);
            if i >= 20 { tj.push(s.elapsed()); }

            let s = Instant::now();
            let resp = flight.do_get(Ticket { ticket: body.to_string().into() }).await.unwrap().into_inner();
            let stream = resp.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)));
            let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(stream).try_collect().await.unwrap();
            let rows = batches_to_json(&batches);
            assert_eq!(rows.len(), limit);
            if i >= 20 { tf.push(s.elapsed()); }
        }
        report(&format!("{name} · JSON"), tj);
        report(&format!("{name} · Flight"), tf);
    }
    {
        let mut tj = Vec::new();
        let mut tf = Vec::new();
        let body = json!({ "table": "audit_log", "records": small_records });
        for i in 0..(iters / 5).max(20) + 5 {
            let s = Instant::now();
            let r: Value = http.post(format!("http://{ja}/insert")).json(&body).send().await.unwrap().json().await.unwrap();
            assert_eq!(r["inserted"], 1_000);
            if i >= 5 { tj.push(s.elapsed()); }

            let s = Instant::now();
            let batch = records_to_batch(&small_records);
            let data = FlightDataEncoderBuilder::new()
                .with_flight_descriptor(Some(FlightDescriptor::new_path(vec!["audit_log".into()])))
                .build(futures::stream::iter(vec![Ok(batch)]))
                .map(|r| r.unwrap());
            let mut results = flight.do_put(data).await.unwrap().into_inner();
            let r = results.next().await.unwrap().unwrap();
            let v: Value = serde_json::from_slice(&r.app_metadata).unwrap();
            assert_eq!(v["inserted"], 1_000);
            if i >= 5 { tf.push(s.elapsed()); }
        }
        report("insert 1,000 rows · JSON", tj);
        report("insert 1,000 rows · Flight", tf);
    }
}

//! `adb import-lancedb` (stage G, G6): copy an embedded LanceDB directory — A3T's
//! `a3t-platform.lance` — into a running ADB's platform tables over Arrow Flight.
//! Each `<table>.lance` dataset becomes a platform table (columns as UTF-8, as A3T
//! writes them), inserted in chunks through ADB's log like any other write.

use std::path::Path;
use std::sync::Arc;

use adb_store::lance::dataset::Dataset;
use arrow_array::{Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightDescriptor};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use serde_json::{json, Value};
use tonic::transport::Channel;

const CHUNK: usize = 500;

type Client = FlightServiceClient<tonic::service::interceptor::InterceptedService<Channel, Bearer>>;

/// `authorization: Bearer $ADB_AUTH_TOKEN` on every call, when the target ADB has
/// a token (W3).
#[derive(Clone)]
struct Bearer(Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

async fn action(c: &mut Client, kind: &str, body: Value) -> Result<Value, String> {
    let mut s = c
        .do_action(Action { r#type: kind.into(), body: body.to_string().into() })
        .await
        .map_err(|e| format!("{kind}: {}", e.message()))?
        .into_inner();
    let r = s.next().await.ok_or(format!("{kind}: no answer"))?.map_err(|e| format!("{kind}: {}", e.message()))?;
    serde_json::from_slice(&r.body).map_err(|e| format!("{kind}: {e}"))
}

/// `batch` with every column cast to UTF-8 (A3T's tables already are).
fn utf8(batch: &RecordBatch) -> Result<RecordBatch, String> {
    let fields: Vec<Field> = batch.schema().fields().iter().map(|f| Field::new(f.name(), DataType::Utf8, true)).collect();
    let cols: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .map(|c| arrow_cast::cast(c, &DataType::Utf8).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).map_err(|e| e.to_string())
}

/// A Flight client for `flight`, with `ADB_AUTH_TOKEN` when it is set.
async fn connect(flight: &str) -> Result<Client, String> {
    let channel = Channel::from_shared(flight.to_string()).map_err(|e| e.to_string())?.connect().await.map_err(|e| format!("connect {flight}: {e}"))?;
    let bearer = std::env::var("ADB_AUTH_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .map(|t| format!("Bearer {t}").parse().map_err(|_| "ADB_AUTH_TOKEN is not a valid header value".to_string()))
        .transpose()?;
    Ok(FlightServiceClient::with_interceptor(channel, Bearer(bearer))
        .max_encoding_message_size(64 << 20)
        .max_decoding_message_size(64 << 20))
}

/// `adb sql`: read-only SQL over a running ADB's memory and platform tables (the
/// superadmin's debug view), printed as JSON rows with every value as text.
pub async fn sql(query: &str, flight: &str, limit: Option<usize>) -> Result<(), String> {
    let mut c = connect(flight).await?;
    let mut ticket = json!({ "sql": query });
    if let Some(l) = limit {
        ticket["limit"] = json!(l);
    }
    let s = c
        .do_get(arrow_flight::Ticket { ticket: ticket.to_string().into() })
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let batches: Vec<RecordBatch> = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        s.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
    )
    .try_collect()
    .await
    .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for b in &batches {
        let cols: Vec<(String, Arc<dyn Array>)> = b
            .schema()
            .fields()
            .iter()
            .zip(b.columns())
            .map(|(f, c)| Ok((f.name().clone(), arrow_cast::cast(c, &DataType::Utf8).map_err(|e| format!("{}: {e}", f.name()))?)))
            .collect::<Result<_, String>>()?;
        for r in 0..b.num_rows() {
            let mut o = serde_json::Map::new();
            for (name, col) in &cols {
                let a = col.as_any().downcast_ref::<arrow_array::StringArray>().expect("cast to Utf8");
                o.insert(name.clone(), if a.is_valid(r) { Value::String(a.value(r).to_string()) } else { Value::Null });
            }
            rows.push(Value::Object(o));
        }
    }
    println!("{}", serde_json::to_string_pretty(&rows).map_err(|e| e.to_string())?);
    Ok(())
}

pub async fn import(path: &Path, flight: &str, replace: bool) -> Result<(), String> {
    let mut c = connect(flight).await?;
    let existing: Vec<String> = serde_json::from_value(action(&mut c, "list_tables", json!({})).await?).unwrap_or_default();
    let mut tables: Vec<String> = std::fs::read_dir(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_suffix(".lance")).map(String::from))
        .collect();
    tables.sort();
    let mut total = 0usize;
    for table in &tables {
        let ds = Dataset::open(&path.join(format!("{table}.lance")).to_string_lossy()).await.map_err(|e| format!("{table}: {e}"))?;
        let columns: Vec<String> = ds.schema().fields.iter().map(|f| f.name.clone()).collect();
        if existing.contains(table) {
            if !replace {
                println!("  {table}: already in ADB, skipped (--replace to overwrite)");
                continue;
            }
            action(&mut c, "delete", json!({ "table": table, "filter": "true" })).await?;
        } else {
            action(&mut c, "create_table", json!({ "table": table, "columns": columns })).await?;
        }
        let batches: Vec<RecordBatch> = ds.scan().try_into_stream().await.map_err(|e| e.to_string())?.try_collect().await.map_err(|e| e.to_string())?;
        let mut rows = 0;
        for b in &batches {
            let b = utf8(b)?;
            let mut start = 0;
            while start < b.num_rows() {
                let len = CHUNK.min(b.num_rows() - start);
                let chunk = b.slice(start, len);
                let data = FlightDataEncoderBuilder::new()
                    .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![table.clone()])))
                    .build(futures::stream::iter(vec![Ok(chunk)]))
                    .filter_map(|r| async move { r.ok() });
                let mut res = c.do_put(data).await.map_err(|e| format!("{table}: {}", e.message()))?.into_inner();
                res.next().await.ok_or(format!("{table}: no answer"))?.map_err(|e| format!("{table}: {}", e.message()))?;
                rows += len;
                start += len;
            }
        }
        total += rows;
        println!("  {table}: {rows} rows");
    }
    println!("imported {} tables, {total} rows into {flight}", tables.len());
    Ok(())
}

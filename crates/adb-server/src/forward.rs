//! Step 4b: Flight calls for the system partition (platform tables, FlowR's engine
//! store, SQL) on a node that doesn't own it, sent on to the node that does — with
//! the caller's metadata (its token, read options), so the owner checks it as if the
//! caller had come straight there. A forwarded call carries `x-adb-forwarded`: a node
//! asked one that it can't serve either (its map is stale) refuses instead of sending
//! it on again.

use std::collections::HashMap;
use std::sync::Mutex;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor, PutResult, SchemaResult, Ticket};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

/// The marker on a call one node sent on to another.
pub const FORWARDED: &str = "x-adb-forwarded";

/// Actions served by the system partition's node.
pub const SYSTEM_ACTIONS: &[&str] = &[
    "list_tables", "create_table", "insert", "upsert", "update", "replace_table", "delete", "table_changes", "transaction", "add_columns", "append_only", "purge", "engine",
    "engine_subscribe", "engine_restore",
];

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Channels to the other nodes' Flight endpoints, kept open.
#[derive(Default)]
pub struct Forwarder {
    channels: Mutex<HashMap<String, Channel>>,
}

impl Forwarder {
    fn client(&self, url: &str) -> Result<FlightServiceClient<Channel>, Status> {
        let mut c = self.channels.lock().unwrap();
        let ch = match c.get(url) {
            Some(ch) => ch.clone(),
            None => {
                let ch = Channel::from_shared(url.to_string()).map_err(|e| Status::internal(format!("{url}: {e}")))?.connect_timeout(std::time::Duration::from_secs(3)).connect_lazy();
                c.insert(url.to_string(), ch.clone());
                ch
            }
        };
        Ok(FlightServiceClient::new(ch).max_decoding_message_size(256 << 20).max_encoding_message_size(256 << 20))
    }

    /// `req`'s message with its metadata, marked as forwarded.
    fn carry<T>(req: Request<T>) -> Result<Request<T>, Status> {
        if req.metadata().contains_key(FORWARDED) {
            return Err(Status::unavailable("not_owner: this node doesn't serve the system partition either (its map is stale); retry"));
        }
        let (mut meta, _, msg) = req.into_parts();
        meta.insert(FORWARDED, "1".parse().expect("a valid header value"));
        let mut r = Request::new(msg);
        *r.metadata_mut() = meta;
        Ok(r)
    }

    pub async fn do_action(&self, url: &str, req: Request<Action>) -> Result<Response<BoxStream<arrow_flight::Result>>, Status> {
        let r = self.client(url)?.do_action(Self::carry(req)?).await?;
        Ok(Response::new(Box::pin(r.into_inner())))
    }

    pub async fn do_get(&self, url: &str, req: Request<Ticket>) -> Result<Response<BoxStream<FlightData>>, Status> {
        let r = self.client(url)?.do_get(Self::carry(req)?).await?;
        Ok(Response::new(Box::pin(r.into_inner())))
    }

    pub async fn do_put(&self, url: &str, req: Request<Streaming<FlightData>>) -> Result<Response<BoxStream<PutResult>>, Status> {
        let req = Self::carry(req)?;
        let (meta, _, incoming) = req.into_parts();
        // The caller's messages as they arrive; a broken stream ends the put there.
        let mut out = Request::new(incoming.scan((), |_, r| futures::future::ready(r.ok())));
        *out.metadata_mut() = meta;
        let r = self.client(url)?.do_put(out).await?;
        Ok(Response::new(Box::pin(r.into_inner())))
    }

    pub async fn get_schema(&self, url: &str, req: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        self.client(url)?.get_schema(Self::carry(req)?).await
    }
}

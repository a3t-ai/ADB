//! ADB HTTP server — a drop-in replacement for ClawDB's server (`clawdb-server`).
//!
//! A3T reaches agent memory through these endpoints today, so they keep ClawDB's
//! paths and wire format:
//!
//! | Endpoint | Purpose |
//! |---|---|
//! | `GET /health` | `{status, version}` |
//! | `POST /query` | `{"query": AQL, "variables"?: {…}}` → `{success, data, error, execution_time_ms}` |
//! | `GET /stats` | `{success, data: {working: n, …}}` |
//! | `POST /mcp` | MCP over streamable HTTP (JSON responses) |
//! | `GET /mcp/sse`, `POST /mcp/message?sessionId=…` | MCP over HTTP+SSE (no `sessionId`: reply in the body) |
//! | `GET /subscriptions/{id}/events?from=N` | A subscription's deliveries as SSE (stage E): `event: delivery`, `id: <offset>`; reconnect with `Last-Event-ID` to resume; `event: heartbeat` with the tip when idle |
//! | `GET /watches/{id}/events?from=N` | A continuous query's frames as SSE (W6): `event: diff` / `reset`, `id: <offset>`; `from` / `Last-Event-ID` = the last frame applied |
//!
//! With `ADB_AUTH_TOKEN` set, every endpoint but `/health` needs
//! `Authorization: Bearer <token>` ([`auth`]).

pub mod auth;
pub mod flight;
pub use flight::{serve_flight, serve_flight_with_token};

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adb_executor::Engine;
use adb_mcp::protocol::{JsonRpcRequest, JsonRpcResponse};
use adb_mcp::McpServer;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};

/// Shared state: the engine and the MCP handler over it.
#[derive(Clone)]
pub struct AppState {
    engine: Arc<dyn Engine>,
    mcp: McpServer,
    sessions: Sessions,
}

impl AppState {
    pub fn new(engine: Arc<dyn Engine>) -> Self {
        Self { mcp: McpServer::new(Arc::clone(&engine)), engine, sessions: Sessions::default() }
    }
}

/// The full router, with the token from `ADB_AUTH_TOKEN` if set.
pub fn router(engine: Arc<dyn Engine>) -> Router {
    router_with_token(engine, auth::token_from_env())
}

/// The full router; with `token`, every endpoint but `/health` requires it.
pub fn router_with_token(engine: Arc<dyn Engine>, token: Option<Arc<str>>) -> Router {
    let mut r = routes(engine);
    if let Some(t) = token {
        r = r.layer(axum::middleware::from_fn(move |req, next| auth::require_token(Arc::clone(&t), req, next)));
    }
    r.layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
}

fn routes(engine: Arc<dyn Engine>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/query", post(query))
        .route("/stats", get(stats))
        .route("/admin/embedding", get(embedding).put(set_embedding))
        .route("/mcp", post(mcp_post).get(mcp_get))
        .route("/mcp/sse", get(mcp_sse))
        .route("/mcp/message", post(mcp_message))
        .route("/subscriptions/:id/events", get(subscription_events))
        .route("/watches/:id/events", get(watch_events))
        .with_state(AppState::new(engine))
}

/// Bind and serve until the process ends.
pub async fn serve(addr: SocketAddr, engine: Arc<dyn Engine>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(auth = auth::token_from_env().is_some(), "ADB listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router(engine)).await
}

// ---------------------------------------------------------------- subscriptions

#[derive(Deserialize)]
struct EventsParams {
    from: Option<u64>,
}

/// `GET /subscriptions/{id}/events` — deliveries as SSE (E1). Each delivery is an
/// `event: delivery` with `id: <offset>`, so `Last-Event-ID` resumes after the last one
/// received; idle periods send `event: heartbeat` with `{tip, next}`. A subscriber
/// whose position left the log gets `event: reset` (the current rows, `id` = the
/// offset they are as of) and then deliveries after it (W9). An error is sent as
/// `event: error` and ends the stream.
async fn subscription_events(
    State(s): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Query(p): Query<EventsParams>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(gw) = s.engine.gateway() else {
        return (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "error": "subscriptions need a durable engine (start ADB with --data-dir)" })))
            .into_response();
    };
    if gw.registry().get(&id).is_none() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": format!("unknown subscription {id}") }))).into_response();
    }
    let resume = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|last| last + 1);
    let from = p.from.or(resume);
    use futures::StreamExt as _;
    let stream = futures::stream::unfold(Some(from), move |state| {
        let gw = gw.clone();
        let id = id.clone();
        async move {
            let from = state?;
            let events: Vec<Result<Event, Infallible>> = match gw.next(&id, from, 100, Duration::from_secs(15)).await {
                Ok(b) if b.deliveries.is_empty() && b.reset.is_none() => {
                    let hb = serde_json::json!({ "tip": b.tip, "next": b.next });
                    return Some((futures::stream::iter(vec![Ok(Event::default().event("heartbeat").data(hb.to_string()))]), Some(Some(b.next))));
                }
                Ok(b) => {
                    let next = b.next;
                    let reset = b.reset.iter().map(|r| Ok(Event::default().event("reset").id(r.offset.to_string()).data(serde_json::to_string(r).unwrap_or_default())));
                    let evs: Vec<Result<Event, Infallible>> = reset
                        .chain(b.deliveries.iter().map(|d| {
                            Ok(Event::default().event("delivery").id(d.offset.to_string()).data(serde_json::to_string(d).unwrap_or_default()))
                        }))
                        .collect();
                    return Some((futures::stream::iter(evs), Some(Some(next))));
                }
                Err(e) => vec![Ok(Event::default().event("error").data(serde_json::json!({ "error": e.to_string() }).to_string()))],
            };
            Some((futures::stream::iter(events), None))
        }
    })
    .flatten();
    Sse::new(stream).keep_alive(KeepAlive::default()).into_response()
}

// ---------------------------------------------------------------- watches (W6)

/// `GET /watches/{id}/events?from=<offset>` — a continuous query's frames as SSE:
/// `event: diff` / `reset`, `id: <offset>`. `from` (or `Last-Event-ID`) is the offset
/// of the last frame the client applied (the `WATCH` snapshot's at first); without
/// one, or past the buffer, the stream starts with a `reset` (W6 decision 2).
async fn watch_events(
    State(s): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Query(p): Query<EventsParams>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(w) = s.engine.watcher() else {
        return (StatusCode::NOT_IMPLEMENTED, Json(serde_json::json!({ "error": "watches need a durable engine (start ADB with --data-dir)" })))
            .into_response();
    };
    if !w.contains(&id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": format!("unknown watch {id}") }))).into_response();
    }
    let last = headers.get("last-event-id").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
    let from = p.from.or(last);
    use futures::StreamExt as _;
    let stream = futures::stream::unfold(Some(from), move |state| {
        let w = w.clone();
        let id = id.clone();
        async move {
            let from = state?;
            match w.next(&id, from, 100, Duration::from_secs(15)).await {
                Ok(frames) if frames.is_empty() => {
                    let hb = serde_json::json!({ "offset": from });
                    Some((futures::stream::iter(vec![Ok::<_, Infallible>(Event::default().event("heartbeat").data(hb.to_string()))]), Some(from)))
                }
                Ok(frames) => {
                    let next = frames.last().map(|f| f.offset);
                    let evs: Vec<Result<Event, Infallible>> = frames
                        .iter()
                        .map(|f| Ok(Event::default().event(f.kind).id(f.offset.to_string()).data(serde_json::to_string(f).unwrap_or_default())))
                        .collect();
                    Some((futures::stream::iter(evs), Some(next)))
                }
                Err(e) => Some((
                    futures::stream::iter(vec![Ok(Event::default().event("error").data(serde_json::json!({ "error": e.to_string() }).to_string()))]),
                    None,
                )),
            }
        }
    })
    .flatten();
    Sse::new(stream).keep_alive(KeepAlive::default()).into_response()
}

// ---------------------------------------------------------------- REST

#[derive(Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok", version: env!("CARGO_PKG_VERSION") })
}

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    #[serde(default)]
    variables: Option<HashMap<String, Value>>,
}

#[derive(Serialize)]
struct QueryResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_time_ms: Option<u64>,
}

/// `POST /query` — ClawDB's shape: `data` is the whole wire `QueryResult`.
async fn query(State(s): State<AppState>, Json(req): Json<QueryRequest>) -> (StatusCode, Json<QueryResponse>) {
    let start = Instant::now();
    let result = s.engine.query(&req.query, req.variables.as_ref()).await;
    let ms = Some(start.elapsed().as_millis() as u64);
    match result {
        Ok(wire) => {
            let success = wire.get("success").and_then(Value::as_bool).unwrap_or(true);
            let error = wire.get("error").and_then(Value::as_str).map(String::from);
            (StatusCode::OK, Json(QueryResponse { success, data: Some(wire), error, execution_time_ms: ms }))
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(QueryResponse { success: false, data: None, error: Some(e), execution_time_ms: ms }),
        ),
    }
}

/// `GET /stats` — `{success, data: {working: n, episodic: n, …}}`.
/// `GET /admin/embedding` — the embedding settings and progress (W9.10).
async fn embedding(State(s): State<AppState>) -> (StatusCode, Json<QueryResponse>) {
    respond(s.engine.embedding().await, StatusCode::INTERNAL_SERVER_ERROR)
}

/// `PUT /admin/embedding` — `{provider, model, dimension, url?, api_key_env?}`:
/// checked with a sample embedding, saved, applied by restarting ADB (W9.10).
async fn set_embedding(State(s): State<AppState>, Json(body): Json<Value>) -> (StatusCode, Json<QueryResponse>) {
    respond(s.engine.set_embedding(body).await, StatusCode::BAD_REQUEST)
}

fn respond(r: Result<Value, String>, on_error: StatusCode) -> (StatusCode, Json<QueryResponse>) {
    match r {
        Ok(data) => (StatusCode::OK, Json(QueryResponse { success: true, data: Some(data), error: None, execution_time_ms: None })),
        Err(e) => (on_error, Json(QueryResponse { success: false, data: None, error: Some(e), execution_time_ms: None })),
    }
}

async fn stats(State(s): State<AppState>) -> (StatusCode, Json<QueryResponse>) {
    match s.engine.stats().await {
        Ok(data) => (
            StatusCode::OK,
            Json(QueryResponse { success: true, data: Some(data), error: None, execution_time_ms: None }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(QueryResponse { success: false, data: None, error: Some(e), execution_time_ms: None }),
        ),
    }
}

// ---------------------------------------------------------------- MCP: streamable HTTP

/// `POST /mcp` — one JSON-RPC message (or a batch) in, JSON out; notifications → 202.
async fn mcp_post(State(s): State<AppState>, Json(body): Json<Value>) -> Response {
    match body {
        Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                if let Some(r) = handle_value(&s.mcp, item).await {
                    out.push(r);
                }
            }
            if out.is_empty() {
                StatusCode::ACCEPTED.into_response()
            } else {
                Json(out).into_response()
            }
        }
        item => match handle_value(&s.mcp, item).await {
            Some(r) => Json(r).into_response(),
            None => StatusCode::ACCEPTED.into_response(),
        },
    }
}

/// `GET /mcp` — no server-initiated stream on the streamable transport (yet).
async fn mcp_get() -> StatusCode {
    StatusCode::METHOD_NOT_ALLOWED
}

async fn handle_value(mcp: &McpServer, v: Value) -> Option<JsonRpcResponse> {
    match serde_json::from_value::<JsonRpcRequest>(v) {
        Ok(req) => mcp.handle(req).await,
        Err(e) => Some(JsonRpcResponse::error(None, -32600, format!("Invalid request: {e}"))),
    }
}

// ---------------------------------------------------------------- MCP: HTTP+SSE

/// Open SSE sessions: session id → sender of serialized JSON-RPC responses.
#[derive(Clone, Default)]
struct Sessions(Arc<Mutex<HashMap<String, mpsc::UnboundedSender<String>>>>);

impl Sessions {
    fn open(&self) -> (String, mpsc::UnboundedReceiver<String>) {
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.lock().unwrap().insert(id.clone(), tx);
        (id, rx)
    }
    fn send(&self, id: &str, msg: String) -> bool {
        self.0.lock().unwrap().get(id).map(|tx| tx.send(msg).is_ok()).unwrap_or(false)
    }
    fn close(&self, id: &str) {
        self.0.lock().unwrap().remove(id);
    }
}

/// Removes the session when the SSE stream is dropped (client disconnected).
struct SessionGuard {
    sessions: Sessions,
    id: String,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.sessions.close(&self.id);
        tracing::debug!("MCP SSE session closed: {}", self.id);
    }
}

/// `GET /mcp/sse` — first event `endpoint` = where to POST messages; then one
/// `message` event per JSON-RPC response.
async fn mcp_sse(State(s): State<AppState>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (id, rx) = s.sessions.open();
    tracing::info!("MCP SSE session opened: {id}");
    let endpoint = format!("/mcp/message?sessionId={id}");
    let guard = SessionGuard { sessions: s.sessions.clone(), id };
    let stream = futures::stream::unfold((Some(endpoint), rx, guard), |(endpoint, mut rx, guard)| async move {
        if let Some(e) = endpoint {
            return Some((Ok(Event::default().event("endpoint").data(e)), (None, rx, guard)));
        }
        let msg = rx.recv().await?;
        Some((Ok(Event::default().event("message").data(msg)), (None, rx, guard)))
    });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

#[derive(Deserialize)]
struct MessageQuery {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
}

/// `POST /mcp/message?sessionId=…` — 202, the response goes out on the session's SSE stream.
/// Without `sessionId` it answers in the body like `POST /mcp`: FlowR's `call:mcp` maps a
/// `/mcp/sse` resource URL to `/mcp/message` and posts there without opening a stream.
async fn mcp_message(State(s): State<AppState>, Query(q): Query<MessageQuery>, Json(body): Json<Value>) -> Response {
    let Some(session_id) = q.session_id else {
        return mcp_post(State(s), Json(body)).await;
    };
    if !s.sessions.0.lock().unwrap().contains_key(&session_id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Some(resp) = handle_value(&s.mcp, body).await {
        let text = serde_json::to_string(&resp).unwrap_or_default();
        if !s.sessions.send(&session_id, text) {
            return StatusCode::NOT_FOUND.into_response();
        }
    }
    StatusCode::ACCEPTED.into_response()
}

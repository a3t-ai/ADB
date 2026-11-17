//! Stage H-3: FlowR's engine store over Arrow Flight — the conformance suite
//! through `do_action("engine")` with FlowR's own wire client, notices over
//! `engine_subscribe`, and the engine token's scope (H8).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use async_trait::async_trait;
use flowr_storage::engine::*;
use flowr_storage::engine_wire::{unwrap, Client, Transport};
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::{json, Value};
use tonic::transport::Channel;

const MEMORY: &str = "mem-token";
const ENGINE: &str = "engine-token";

async fn start(dir: &std::path::Path, tokens: bool) -> SocketAddr {
    let (engine, _) = LocalEngine::durable(dir).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (m, e) = if tokens { (Some(Arc::from(MEMORY)), Some(Arc::from(ENGINE))) } else { (None, None) };
    tokio::spawn(async move {
        adb_server::serve_flight_with_tokens(addr, engine, m, e).await.unwrap();
    });
    for _ in 0..100 {
        if Channel::from_shared(format!("http://{addr}")).unwrap().connect().await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Flight server did not start");
}

async fn client(addr: SocketAddr) -> FlightServiceClient<Channel> {
    FlightServiceClient::new(Channel::from_shared(format!("http://{addr}")).unwrap().connect().await.unwrap())
}

fn with_token<T>(body: T, token: Option<&str>) -> tonic::Request<T> {
    let mut r = tonic::Request::new(body);
    if let Some(t) = token {
        r.metadata_mut().insert("authorization", format!("Bearer {t}").parse().unwrap());
    }
    r
}

/// FlowR's engine wire over ADB's Flight (what FlowR's ADB client does in H-4).
struct FlightTransport {
    client: FlightServiceClient<Channel>,
    space: String,
    token: Option<String>,
}

#[async_trait]
impl Transport for FlightTransport {
    async fn call(&self, op: &str, args: Value) -> StoreResult<Value> {
        let body = json!({ "space": self.space, "op": op, "args": args }).to_string();
        let req = with_token(Action { r#type: "engine".into(), body: body.into() }, self.token.as_deref());
        let mut s = self.client.clone().do_action(req).await.map_err(|e| StoreError::Backend(e.to_string()))?.into_inner();
        let r = s.next().await.ok_or_else(|| StoreError::Backend("no answer".into()))?.map_err(|e| StoreError::Backend(e.to_string()))?;
        unwrap(serde_json::from_slice(&r.body).map_err(|e| StoreError::Backend(e.to_string()))?)
    }

    fn subscribe(&self, topic: &str) -> BoxStream<'static, Value> {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let (mut client, token) = (self.client.clone(), self.token.clone());
        let body = json!({ "space": self.space, "topic": topic }).to_string();
        tokio::spawn(async move {
            let req = with_token(Action { r#type: "engine_subscribe".into(), body: body.into() }, token.as_deref());
            if let Ok(r) = client.do_action(req).await {
                let mut s = r.into_inner();
                while let Some(Ok(n)) = s.next().await {
                    if tx.unbounded_send(serde_json::from_slice(&n.body).unwrap_or(Value::Null)).is_err() {
                        return;
                    }
                }
            }
        });
        rx.boxed()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_over_flight() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path(), true).await;
    let c = client(addr).await;
    let n = std::sync::atomic::AtomicUsize::new(0);
    flowr_storage::conformance::run_all(|| {
        let store = Client::new(FlightTransport {
            client: c.clone(),
            space: format!("f{}", n.fetch_add(1, std::sync::atomic::Ordering::SeqCst)),
            token: Some(ENGINE.into()),
        });
        async move {
            store.pending(DEFAULT_POOL).await.unwrap();
            store
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_engine_token_opens_only_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path(), true).await;
    let mut c = client(addr).await;
    let engine_call = |token: Option<&'static str>| {
        with_token(Action { r#type: "engine".into(), body: json!({ "space": "s", "op": "pending", "args": { "pool": "default" } }).to_string().into() }, token)
    };
    // The engine token: the engine, nothing else.
    assert!(c.do_action(engine_call(Some(ENGINE))).await.is_ok());
    let aql = with_token(Action { r#type: "aql".into(), body: json!({ "query": "RECALL FROM SEMANTIC LIMIT 1" }).to_string().into() }, Some(ENGINE));
    assert_eq!(c.do_action(aql).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    let get = with_token(Ticket { ticket: json!({ "table": "users" }).to_string().into() }, Some(ENGINE));
    assert_eq!(c.do_get(get).await.err().map(|e| e.code()), Some(tonic::Code::PermissionDenied));
    // The memory token: not the engine.
    assert_eq!(c.do_action(engine_call(Some(MEMORY))).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    let aql = with_token(Action { r#type: "aql".into(), body: json!({ "query": "RECALL FROM SEMANTIC LIMIT 1" }).to_string().into() }, Some(MEMORY));
    assert!(c.do_action(aql).await.is_ok());
    // No token, or a wrong one: nothing.
    assert_eq!(c.do_action(engine_call(None)).await.unwrap_err().code(), tonic::Code::Unauthenticated);
    assert_eq!(c.do_action(engine_call(Some("nope"))).await.unwrap_err().code(), tonic::Code::Unauthenticated);
}

#[tokio::test(flavor = "multi_thread")]
async fn notices_and_typed_errors_over_flight() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path(), false).await;
    let store = Client::new(FlightTransport { client: client(addr).await, space: "n".into(), token: None });
    let mut tasks = store.subscribe(&tasks_topic("q"));
    tokio::time::sleep(Duration::from_millis(200)).await;
    store.enqueue(NewTask { pool: "q".into(), ..NewTask::resume(uuid::Uuid::now_v7(), "p", None, chrono::Utc::now()) }).await.unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(5), tasks.next()).await.expect("a notice").unwrap();
    assert!(notice.get("enqueued").is_some(), "{notice}");
    // A fenced writer is told it is fenced, across Flight.
    let run = NewRun {
        run_id: uuid::Uuid::now_v7(), namespace: "p".into(), flow: "f".into(), version: "1".into(), def_hash: "h".into(),
        parent_run_id: None, durability: Durability::Durable, debug: false, breakpoints: None, principal: json!({}),
        trigger: "api".into(), input: json!({}), deadline_at: None, dedupe_key: None,
    };
    let id = run.run_id;
    let t = NewTask::start(&run);
    store.start(run, t).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (_, _, epoch) = store.claim_runs(DEFAULT_POOL, "w", 1, Duration::from_secs(30)).await.unwrap().remove(0);
    assert!(matches!(store.append(id, epoch + 1, &[]).await, Err(StoreError::Fenced(r)) if r == id));
}

/// H-6 bench over Flight (EE: FlowR → ADB server), the same measures as the
/// embedded one. `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_over_flight() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start(dir.path(), true).await;
    let s = Arc::new(Client::new(FlightTransport { client: client(addr).await, space: "bench".into(), token: Some(ENGINE.into()) }));
    let lease = Duration::from_secs(30);
    let new_run = || NewRun {
        run_id: uuid::Uuid::now_v7(), namespace: "p".into(), flow: "f".into(), version: "1".into(), def_hash: "h".into(),
        parent_run_id: None, durability: Durability::Durable, debug: false, breakpoints: None, principal: json!({}),
        trigger: "api".into(), input: json!({}), deadline_at: None, dedupe_key: None,
    };
    let step = |i: u64| Record::StepCompleted { step_key: format!("/s{i}"), attempt: 0, output: json!(i), context: Value::Null, class: StepClass::Pure, duration_us: 1, directive: String::new(), input: Value::Null, at: None };
    s.pending(DEFAULT_POOL).await.unwrap();
    let t = std::time::Instant::now();
    for _ in 0..500 {
        s.pending(DEFAULT_POOL).await.unwrap();
    }
    println!("BENCH(flight) a read-only call (pending): {:?}", t.elapsed() / 500);
    let n = 300;
    let t = std::time::Instant::now();
    for _ in 0..n {
        let run = new_run();
        let task = NewTask { visible_at: chrono::Utc::now() - chrono::Duration::seconds(1), ..NewTask::start(&run) };
        s.start(run, task).await.unwrap();
        let (l, task, epoch) = s.claim_runs(DEFAULT_POOL, "w", 1, lease).await.unwrap().remove(0);
        s.release(task.run_id, epoch, RunPhase::Completed, RunOutcome::default(), &l).await.unwrap();
    }
    let per_run = t.elapsed() / n;
    let run = new_run();
    let id = run.run_id;
    let task = NewTask { visible_at: chrono::Utc::now() - chrono::Duration::seconds(1), ..NewTask::start(&run) };
    s.start(run, task).await.unwrap();
    let (_, _, epoch) = s.claim_runs(DEFAULT_POOL, "w", 1, lease).await.unwrap().remove(0);
    let mut lat = Vec::new();
    for i in 0..1000 {
        let t = std::time::Instant::now();
        s.commit_step(id, epoch, StepCommit { records: vec![step(i)], checkpoint: None, outbox: vec![] }).await.unwrap();
        lat.push(t.elapsed());
    }
    lat.sort();
    let t = std::time::Instant::now();
    let mut hs = Vec::new();
    for _ in 0..8 {
        let s = Arc::clone(&s);
        let run = new_run();
        let id = run.run_id;
        let task = NewTask { pool: format!("b-{id}"), visible_at: chrono::Utc::now() - chrono::Duration::seconds(1), ..NewTask::start(&run) };
        let pool = task.pool.clone();
        s.start(run, task).await.unwrap();
        hs.push(tokio::spawn(async move {
            let (_, _, epoch) = s.claim_runs(&pool, "w", 1, lease).await.unwrap().remove(0);
            for i in 0..200 {
                s.commit_step(id, epoch, StepCommit { records: vec![step(i)], checkpoint: None, outbox: vec![] }).await.unwrap();
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    println!("BENCH(flight) run start+claim+release: {per_run:?}/run ({:.0} runs/s)", 1.0 / per_run.as_secs_f64());
    println!("BENCH(flight) step commit p50 {:?} p99 {:?}", lat[500], lat[990]);
    println!("BENCH(flight) 8 concurrent runs: {:.0} steps/s", 1600.0 / t.elapsed().as_secs_f64());
}

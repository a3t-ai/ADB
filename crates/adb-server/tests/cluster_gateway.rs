//! S3 option B step 4b: three nodes with their real transports — HTTP (`/query`,
//! `/internal/*`) and Arrow Flight — in one process, the partitions spread over them.
//! Any node answers AQL over either transport; platform tables reach the node owning
//! the system partition from any node (Flight forwarded, with the caller's token); a
//! scoped token runs statements without NAMESPACE inside its namespaces; the internal
//! routes need the cluster's token.
//!
//! Own test binary: it sets `ADB_SCOPED_TOKENS` and turns object storage off.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use adb_cluster::map::{Assignment, ControlCmd, NodeId, Partition, PartitionMap};
use adb_executor::partitioned::PartitionedEngine;
use adb_executor::remote::Placement;
use adb_executor::Engine;
use adb_server::branch_remote::flight_action;
use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use serde_json::{json, Value};
use tonic::transport::Channel;

const TOKEN: &str = "cluster-token";
const SCOPED: &str = "a3t-token";
const PARTITIONS: Partition = 8;

struct Static {
    me: NodeId,
    map: RwLock<Arc<PartitionMap>>,
}

impl Placement for Static {
    fn me(&self) -> NodeId {
        self.me
    }
    fn map(&self) -> Arc<PartitionMap> {
        Arc::clone(&self.map.read().unwrap())
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Node {
    http: String,
    flight: String,
    _dir: tempfile::TempDir,
}

async fn query(http: &reqwest::Client, node: &str, token: &str, aql: &str) -> (u16, Value) {
    let r = http.post(format!("{node}/query")).bearer_auth(token).json(&json!({ "query": aql })).send().await.unwrap();
    let status = r.status().as_u16();
    (status, r.json().await.unwrap())
}

fn records(v: &Value) -> Vec<Value> {
    v.pointer("/data/data/records").and_then(|r| r.as_array()).cloned().unwrap_or_default().iter().map(|r| r["data"].clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn any_node_over_http_and_flight() {
    std::env::set_var("ADB_S3", "off");
    // Correctness, not the catalog lag: every statement sees the current catalog.
    std::env::set_var("ADB_CATALOG_TTL_MS", "0");
    std::env::set_var("ADB_SCOPED_TOKENS", json!([{ "name": "a3t", "token": SCOPED, "write": ["a3t-shared--*"] }]).to_string());
    // The map: partition p owned by node p % 3 + 1; every node's two endpoints.
    let ports: Vec<(u16, u16)> = (0..3).map(|_| (free_port(), free_port())).collect();
    let mut map = PartitionMap::default();
    map.apply(&ControlCmd::Init { partitions: PARTITIONS }).unwrap();
    for (i, (h, f)) in ports.iter().enumerate() {
        map.apply(&ControlCmd::Join { node: i as u64 + 1, addr: format!("http://127.0.0.1:{h}"), flight: Some(format!("http://127.0.0.1:{f}")) }).unwrap();
    }
    for p in map.all_partitions() {
        let owner = u64::from(p) % 3 + 1;
        map.apply(&ControlCmd::Assign { partition: p, assignment: Assignment { owner, replica: Some(owner % 3 + 1), epoch: 1 } }).unwrap();
    }
    let peers = Arc::new(adb_server::internal::HttpPeers::new(Some(Arc::from(TOKEN))));
    let mut nodes = Vec::new();
    let mut engines = Vec::new();
    for (i, (h, f)) in ports.iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let place = Arc::new(Static { me: i as u64 + 1, map: RwLock::new(Arc::new(map.clone())) });
        let node = PartitionedEngine::open_cluster(dir.path(), place as Arc<dyn Placement>, Arc::clone(&peers) as _).await.unwrap();
        let app = adb_server::cluster_router(Arc::clone(&node), Some(Arc::from(TOKEN)));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", *h)).await.unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let engine: Arc<dyn Engine> = Arc::new(Arc::clone(&node));
        let fport = *f;
        tokio::spawn(async move {
            let _ = adb_server::serve_flight_with_tokens(SocketAddr::from(([127, 0, 0, 1], fport)), engine, Some(Arc::from(TOKEN)), None).await;
        });
        nodes.push(Node { http: format!("http://127.0.0.1:{h}"), flight: format!("http://127.0.0.1:{f}"), _dir: dir });
        engines.push(node);
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let http = reqwest::Client::new();
    let sys_owner = (u64::from(PARTITIONS) % 3) as usize; // index of the node owning the system partition

    // AQL over HTTP: written through node 1, read through nodes 2 and 3.
    let ns = ["a3t-shared--bot", "a3t-shared--ops", "demo--y"];
    for (i, n) in ns.iter().enumerate() {
        let (s, v) = query(&http, &nodes[0].http, TOKEN, &format!(r#"STORE INTO SEMANTIC (key = "k", n = {i}, owner = "{n}") NAMESPACE "{n}""#)).await;
        assert_eq!(s, 200, "{v}");
    }
    for node in &nodes[1..] {
        let (_, v) = query(&http, &node.http, TOKEN, r#"RECALL FROM SEMANTIC ALL ORDER BY n ASC"#).await;
        assert_eq!(records(&v).iter().map(|r| r["owner"].clone()).collect::<Vec<_>>(), ns.iter().map(|n| json!(n)).collect::<Vec<_>>(), "{v}");
    }
    // AQL over Flight through another node.
    let r = flight_action(&nodes[2].flight, Some(TOKEN), "aql", json!({ "query": r#"RECALL FROM SEMANTIC ALL NAMESPACE "demo--y""# })).await.unwrap();
    assert_eq!(r["data"]["data"]["count"], 1, "{r}");

    // Platform tables from a node that doesn't own the system partition: forwarded.
    let other = (sys_owner + 1) % 3;
    assert!(engines[other].system_flight().is_some() && engines[sys_owner].system_flight().is_none());
    flight_action(&nodes[other].flight, Some(TOKEN), "transaction", json!({ "ops": [{ "op": "insert", "table": "users", "rows": [{ "id": "u1", "name": "Ada" }] }] })).await.unwrap();
    let tables = flight_action(&nodes[other].flight, Some(TOKEN), "list_tables", json!({})).await.unwrap();
    assert!(tables.to_string().contains("users"), "{tables}");
    let ch = Channel::from_shared(nodes[(other + 1) % 3].flight.clone()).unwrap().connect().await.unwrap();
    let mut c = FlightServiceClient::new(ch);
    let mut req = tonic::Request::new(Ticket { ticket: json!({ "table": "users" }).to_string().into() });
    req.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let s = c.do_get(req).await.unwrap().into_inner();
    let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(s.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)))).try_collect().await.unwrap();
    assert_eq!(adb_platform::batches_to_json(&batches)[0]["name"], "Ada", "read through a third node");
    // The forwarded call keeps the caller's token: without one it is refused.
    assert!(flight_action(&nodes[other].flight, None, "list_tables", json!({})).await.is_err());

    // A scoped token: no NAMESPACE → inside a3t-shared--*; another namespace → 403.
    let (s, v) = query(&http, &nodes[1].http, SCOPED, r#"RECALL FROM SEMANTIC ALL ORDER BY n ASC"#).await;
    assert_eq!(s, 200, "{v}");
    let owners: Vec<Value> = records(&v).iter().map(|r| r["owner"].clone()).collect();
    assert_eq!(owners, vec![json!("a3t-shared--bot"), json!("a3t-shared--ops")], "{v}");
    let (s, v) = query(&http, &nodes[1].http, SCOPED, r#"RECALL FROM SEMANTIC ALL NAMESPACE "demo--y""#).await;
    assert_eq!((s, v["code"].clone()), (403, json!("forbidden")), "{v}");
    let (s, v) = query(&http, &nodes[1].http, SCOPED, r#"FORGET FROM SEMANTIC WHERE key = "k""#).await;
    assert_eq!(s, 200, "{v}");
    let (_, v) = query(&http, &nodes[0].http, TOKEN, r#"RECALL FROM SEMANTIC ALL"#).await;
    assert_eq!(records(&v).iter().map(|r| r["owner"].clone()).collect::<Vec<_>>(), vec![json!("demo--y")], "the scoped FORGET stayed in scope: {v}");
    let (s, _) = query(&http, &nodes[1].http, SCOPED, r#"SUBSCRIBE TOPIC "news""#).await;
    assert_eq!(s, 403, "topics are no namespace's");

    // The internal routes need the cluster's token.
    let r = http.post(format!("{}/internal/query", nodes[0].http)).json(&json!({ "aql": "RECALL FROM SEMANTIC ALL", "reach": "All" })).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 401);
    let r = http.post(format!("{}/internal/query", nodes[0].http)).bearer_auth(SCOPED).json(&json!({ "aql": "RECALL FROM SEMANTIC ALL", "reach": "All" })).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 403, "a scoped token reaches /query only");
}

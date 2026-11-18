//! DB-2: one pod serves the branches of many agents, reads their parents from the prod
//! ADB over Flight with a read-only token (DB-3), keeps prod's log untouched, and applies
//! FORGETs it missed when it restarts. Own test binary: it sets `ADB_SCOPED_TOKENS`.

use std::net::SocketAddr;
use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use adb_server::branch_remote::{flight_action, FlightRemote};
use adb_server::non_prod::{NonProdConfig, NonProdPod};
use serde_json::{json, Value};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

const ROOT: &str = "root-token";
const NP: &str = "np-token";

async fn aql(url: &str, q: &str) -> Value {
    let r = flight_action(url, Some(ROOT), "aql", json!({ "query": q })).await.unwrap_or_else(|e| panic!("{q}: {e}"));
    assert_eq!(r["success"], json!(true), "{q}: {r}");
    r
}

async fn start_pod(dir: &std::path::Path, org: &str, own: &str, port: u16) -> tokio::task::JoinHandle<()> {
    let config = NonProdConfig { org: org.into(), own_url: own.into(), token: Some(NP.into()) };
    let pod: Arc<dyn Engine> = Arc::new(NonProdPod::open(dir, config).await.unwrap());
    let task = tokio::spawn(async move {
        let _ = adb_server::serve_flight_with_tokens(SocketAddr::from(([127, 0, 0, 1], port)), pod, Some(Arc::from(ROOT)), None).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    task
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "AQL v0.13: the BRANCH modifier and branch pods are being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn one_pod_hosts_the_branches_of_many_agents() {
    std::env::set_var("ADB_SCOPED_TOKENS", json!([{ "name": "non-prod", "token": NP, "read": ["*"] }]).to_string());
    let (org_dir, pod_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (org_port, pod_port) = (free_port(), free_port());
    let (org, pod) = (format!("http://127.0.0.1:{org_port}"), format!("http://127.0.0.1:{pod_port}"));
    let (local, _) = LocalEngine::durable(org_dir.path()).await.unwrap();
    local.set_branch_remote(Arc::new(FlightRemote { token: Some(ROOT.into()) }));
    let local: Arc<dyn Engine> = Arc::new(local);
    let e = Arc::clone(&local);
    tokio::spawn(async move {
        let _ = adb_server::serve_flight_with_tokens(SocketAddr::from(([127, 0, 0, 1], org_port)), e, Some(Arc::from(ROOT)), None).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let act = |kind: &'static str, body: Value| {
        let org = org.clone();
        async move { flight_action(&org, Some(ROOT), kind, body).await }
    };

    // Two agents' memory on prod, a branch of each, placed on the pod.
    aql(&org, r#"STORE INTO SEMANTIC (key = "fact", content = "crm original") NAMESPACE "crm""#).await;
    aql(&org, r#"STORE INTO SEMANTIC (key = "forget-me", content = "crm erase these words") NAMESPACE "crm""#).await;
    aql(&org, r#"STORE INTO SEMANTIC (key = "fact", content = "ops original") NAMESPACE "ops""#).await;
    for ns in ["crm", "ops"] {
        act("branch_create", json!({ "namespace": ns, "name": "dev" })).await.unwrap();
        act("branch_place", json!({ "namespace": ns, "name": "dev", "url": pod })).await.unwrap();
    }
    act("branch_create", json!({ "namespace": "crm", "name": "elsewhere" })).await.unwrap();
    let mut pod_task = start_pod(pod_dir.path(), &org, &pod, pod_port).await;
    let before = act("space_changes", json!({ "namespace": "crm" })).await.unwrap()["tip"].clone();

    // Each branch reads its own parent through the pod, and writes stay on the branch.
    let read = aql(&pod, r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "dev""#).await.to_string();
    assert!(read.contains("crm original") && !read.contains("ops original"), "{read}");
    let read = aql(&pod, r#"RECALL FROM SEMANTIC NAMESPACE "ops" BRANCH "dev""#).await.to_string();
    assert!(read.contains("ops original") && !read.contains("crm original"), "{read}");
    aql(&pod, r#"STORE INTO SEMANTIC (key = "new", content = "crm branch only") NAMESPACE "crm" BRANCH "dev""#).await;
    aql(&pod, r#"STORE INTO SEMANTIC (key = "new", content = "ops branch only") NAMESPACE "ops" BRANCH "dev""#).await;
    let read = aql(&pod, r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "dev""#).await.to_string();
    assert!(read.contains("crm branch only") && !read.contains("ops branch only"), "{read}");

    // Prod's log and main never moved, and prod's own forwarding reaches the same data.
    assert_eq!(act("space_changes", json!({ "namespace": "crm" })).await.unwrap()["tip"], before, "the prod log moved");
    assert!(!aql(&org, r#"RECALL FROM SEMANTIC NAMESPACE "crm""#).await.to_string().contains("branch only"));
    let via_prod = aql(&org, r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "dev""#).await.to_string();
    assert!(via_prod.contains("crm branch only"), "{via_prod}");
    let stats = act("space_stats", json!({ "namespace": "crm", "branch": "dev", "max_age_ms": 0 })).await.unwrap();
    assert_eq!(stats["tiers"]["SEMANTIC"]["count"], json!(3), "{stats}");

    // No main here, and a branch not placed here is refused.
    for q in [r#"STORE INTO SEMANTIC (key = "x", content = "y") NAMESPACE "crm""#, r#"RECALL FROM SEMANTIC NAMESPACE "crm""#, r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "elsewhere""#, r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "nope""#] {
        let r = flight_action(&pod, Some(ROOT), "aql", json!({ "query": q })).await.unwrap();
        assert_eq!(r["success"], json!(false), "{q}: {r}");
    }

    // FORGET on main reaches the live pod.
    aql(&pod, r#"UPDATE INTO SEMANTIC WHERE key = "fact" (content = "crm edited on the branch") NAMESPACE "crm" BRANCH "dev""#).await;
    aql(&org, r#"FORGET FROM SEMANTIC WHERE key = "forget-me" NAMESPACE "crm""#).await;
    assert!(!aql(&pod, r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "dev""#).await.to_string().contains("erase these words"));

    // Down, then back: its writes are still there, and a FORGET it missed is applied before it serves.
    pod_task.abort();
    let _ = pod_task.await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    aql(&org, r#"FORGET FROM SEMANTIC WHERE key = "fact" NAMESPACE "ops""#).await;
    let pending = act("branch_pending", json!({ "namespace": "ops", "name": "dev" })).await.unwrap();
    assert_eq!(pending["forgets"].as_array().map(|f| f.len()), Some(1), "{pending}");
    pod_task = start_pod(pod_dir.path(), &org, &pod, pod_port).await;
    let back = aql(&pod, r#"RECALL FROM SEMANTIC NAMESPACE "ops" BRANCH "dev""#).await.to_string();
    assert!(back.contains("ops branch only") && !back.contains("ops original"), "{back}");
    let acked = act("branch_pending", json!({ "namespace": "ops", "name": "dev" })).await.unwrap();
    assert_eq!(acked["forgets"], json!([]), "{acked}");

    // The pod's token on prod can't write it.
    let denied = flight_action(&org, Some(NP), "aql", json!({ "query": r#"STORE INTO SEMANTIC (key = "x", content = "y") NAMESPACE "crm""# })).await;
    assert!(denied.is_err(), "{denied:?}");

    // Promote a branch hosted on the pod as a new agent: prod reads the pod's view.
    let promoted = act("branch_promote", json!({ "namespace": "crm", "name": "dev", "into": "crm2", "allow_stale": true })).await.unwrap();
    assert_eq!(promoted["namespace"], json!("crm2"), "{promoted}");
    let copy = aql(&org, r#"RECALL FROM SEMANTIC NAMESPACE "crm2""#).await.to_string();
    assert!(copy.contains("crm branch only") && copy.contains("crm edited on the branch"), "{copy}");
    pod_task.abort();
}

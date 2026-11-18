//! B6 (`.ai/BRANCHING.md`): a branch in its own ADB process. The org forwards `BRANCH`
//! statements to the pod over Flight; the pod reads its parent over Flight `AS OF` the
//! point; FORGET on main reaches the pod, or waits (pending) while it is down and is
//! applied when it starts; a pod branch can be promoted as a new agent.

use std::sync::Arc;

use adb_executor::branch_engine::ParentView;
use adb_executor::{Engine, LocalEngine};
use adb_server::branch_remote::{flight_action, FlightParentSource, FlightRemote};
use serde_json::json;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn facts(answer: &serde_json::Value) -> Vec<String> {
    let recs = answer.pointer("/data/records").or_else(|| answer.pointer("/data/data/records")).and_then(|r| r.as_array()).cloned().unwrap_or_default();
    let mut v: Vec<String> = recs.iter().filter_map(|r| r.pointer("/data/fact").and_then(|x| x.as_str()).map(String::from)).collect();
    v.sort();
    v
}

fn events(answer: &serde_json::Value) -> Vec<String> {
    let recs = answer.pointer("/data/records").or_else(|| answer.pointer("/data/data/records")).and_then(|r| r.as_array()).cloned().unwrap_or_default();
    recs.iter().filter_map(|r| r.pointer("/data/event").and_then(|x| x.as_str()).map(String::from)).collect()
}

/// The log offset an answer's memory point names (DB-7).
fn offset(answer: &serde_json::Value) -> u64 {
    answer.pointer("/metadata/point/offset").and_then(|o| o.as_u64()).unwrap_or_else(|| panic!("no point: {answer}"))
}

fn files_with(dir: &std::path::Path, needle: &[u8]) -> usize {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() { walk(&p, out) } else { out.push(p) }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    files.iter().filter(|f| std::fs::read(f).map(|b| b.windows(needle.len()).any(|w| w == needle)).unwrap_or(false)).count()
}

/// Start a branch pod on `dir`: open, apply pending FORGETs, serve Flight.
async fn start_pod(dir: &std::path::Path, org: &str, port: u16) -> (Arc<dyn Engine>, tokio::task::JoinHandle<()>) {
    let list = flight_action(org, None, "branch_list", json!({ "namespace": "crm" })).await.unwrap();
    let b: Vec<adb_executor::branches::Branch> = serde_json::from_value(list["branches"].clone()).unwrap();
    let b = b.into_iter().find(|b| b.name == "pod-try").unwrap();
    let source = Arc::new(FlightParentSource { url: org.into(), token: None, branch: None });
    let view = ParentView { source, namespace: "crm".into(), branch: "pod-try".into(), point: b.point().into() };
    let (engine, _) = LocalEngine::durable_branch(dir, view).await.unwrap();
    // As `adb-cli`'s pod mode does (DB-20): a new pod's first record names its origin.
    adb_executor::branch_engine::write_origin(&engine, &b).await.unwrap();
    let pending = flight_action(org, None, "branch_pending", json!({ "namespace": "crm", "name": "pod-try" })).await.unwrap();
    let forgets: Vec<adb_executor::branches::PendingForget> = serde_json::from_value(pending["forgets"].clone()).unwrap();
    for f in &forgets {
        engine.forget_lineages(f.memory, &f.lineages).await.unwrap();
    }
    flight_action(org, None, "branch_pending_ack", json!({ "namespace": "crm", "name": "pod-try", "count": forgets.len() })).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let e = Arc::clone(&engine);
    let task = tokio::spawn(async move {
        let _ = adb_server::serve_flight(std::net::SocketAddr::from(([127, 0, 0, 1], port)), e).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    (engine, task)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "AQL v0.13: the BRANCH modifier and branch pods are being replaced by branch namespaces (see .ai/BRANCH_NAMESPACES.md)"]
async fn a_branch_in_its_own_pod() {
    let (org_dir, pod_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (org_port, pod_port) = (free_port(), free_port());
    let (org_url, pod_url) = (format!("http://127.0.0.1:{org_port}"), format!("http://127.0.0.1:{pod_port}"));
    let (local, _) = LocalEngine::durable(org_dir.path()).await.unwrap();
    local.set_branch_remote(Arc::new(FlightRemote { token: None }));
    let org: Arc<dyn Engine> = Arc::new(local);
    let o = Arc::clone(&org);
    tokio::spawn(async move {
        let _ = adb_server::serve_flight(std::net::SocketAddr::from(([127, 0, 0, 1], org_port)), o).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let q = |aql: &'static str| {
        let org = Arc::clone(&org);
        async move { org.query(aql, None).await }
    };
    for aql in [
        r#"STORE INTO SEMANTIC (key = "a", fact = "a1") NAMESPACE "crm""#,
        r#"STORE INTO SEMANTIC (key = "p", fact = "PII-main-p") NAMESPACE "crm""#,
        r#"STORE INTO SEMANTIC (key = "s", fact = "PII-main-s") NAMESPACE "crm""#,
    ] {
        q(aql).await.unwrap();
    }
    org.branch_create("crm", "pod-try", None, None).await.unwrap();
    q(r#"STORE INTO SEMANTIC (key = "late", fact = "late1") NAMESPACE "crm""#).await.unwrap();

    let (pod, task) = start_pod(pod_dir.path(), &org_url, pod_port).await;
    let placed = flight_action(&org_url, None, "branch_place", json!({ "namespace": "crm", "name": "pod-try", "url": pod_url })).await.unwrap();
    assert_eq!(placed["placement"], pod_url.as_str());

    // Statements on the branch run in the pod; the org's table doesn't move.
    let org_stats = org.stats().await.unwrap();
    let org_at = offset(&q(r#"RECALL FROM SEMANTIC NAMESPACE "crm" LIMIT 1"#).await.unwrap());
    q(r#"UPDATE INTO SEMANTIC WHERE key = "p" (fact = "PII-pod-copy") NAMESPACE "crm" BRANCH "pod-try""#).await.unwrap();
    q(r#"STORE INTO SEMANTIC (key = "e", fact = "e1") NAMESPACE "crm" BRANCH "pod-try""#).await.unwrap();
    assert_eq!(org.stats().await.unwrap(), org_stats, "branch writes stay in the pod");
    assert_eq!(offset(&q(r#"RECALL FROM SEMANTIC NAMESPACE "crm" LIMIT 1"#).await.unwrap()), org_at, "DB-1: nothing reached the org's log");
    // DB-20: the pod's first record says where it came from.
    let origin = q(r#"RECALL FROM EPISODIC NAMESPACE "crm" BRANCH "pod-try" LIMIT 10"#).await.unwrap();
    assert_eq!(events(&origin), ["branch_created"], "{origin}");
    let on = || async { facts(&q(r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "pod-try" LIMIT 100"#).await.unwrap()) };
    assert_eq!(on().await, ["PII-main-s", "PII-pod-copy", "a1", "e1"], "parent AS OF the point (no late1) + its own");
    assert_eq!(facts(&pod.query(r#"RECALL FROM SEMANTIC NAMESPACE "crm" BRANCH "pod-try" LIMIT 100"#, None).await.unwrap()), on().await);

    // FORGET on main reaches the running pod: no read there returns its copy from the
    // FORGET's commit on, and the pod's next compaction takes it off the disk
    // (FORGET semantics since 7 Nov: a tombstone; physical removal at compaction).
    q(r#"FORGET FROM SEMANTIC WHERE key = "p" NAMESPACE "crm""#).await.unwrap();
    assert_eq!(on().await, ["PII-main-s", "a1", "e1"]);
    pod.compact().await.unwrap();
    assert_eq!(files_with(pod_dir.path(), b"PII-pod-copy"), 0, "the pod's copy is erased at its compaction");

    // The pod copies `s`, then goes down (hibernates): a FORGET on main waits for it.
    q(r#"UPDATE INTO SEMANTIC WHERE key = "s" (fact = "PII-pod-s") NAMESPACE "crm" BRANCH "pod-try""#).await.unwrap();
    assert_eq!(on().await, ["PII-pod-s", "a1", "e1"]);
    task.abort();
    drop(pod);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    q(r#"FORGET FROM SEMANTIC WHERE key = "s" NAMESPACE "crm""#).await.unwrap();
    let pending = flight_action(&org_url, None, "branch_pending", json!({ "namespace": "crm", "name": "pod-try" })).await.unwrap();
    assert_eq!(pending["forgets"].as_array().map(|a| a.len()), Some(1), "{pending}");
    // It starts again: the pending FORGET is applied before it serves, then cleared.
    let (pod, _task) = start_pod(pod_dir.path(), &org_url, pod_port).await;
    assert_eq!(on().await, ["a1", "e1"]);
    pod.compact().await.unwrap();
    assert_eq!(files_with(pod_dir.path(), b"PII-pod-s"), 0, "erased from the pod's disk at its compaction");
    let pending = flight_action(&org_url, None, "branch_pending", json!({ "namespace": "crm", "name": "pod-try" })).await.unwrap();
    assert_eq!(pending["forgets"].as_array().map(|a| a.len()), Some(0));

    // Promote the pod branch as a new agent.
    let p = org.branch_promote("crm", "pod-try", "crm-v2", true).await.unwrap();
    assert_eq!(p["records"], 2, "{p}");
    assert_eq!(facts(&q(r#"RECALL FROM SEMANTIC NAMESPACE "crm-v2" LIMIT 100"#).await.unwrap()), ["a1", "e1"]);
    assert!(org.branch_create("crm", "child", Some("pod-try"), None).await.is_err(), "no hosted branch on a pod branch (yet)");
}

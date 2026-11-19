//! DB-12, DB-14 and DB-9 over Flight, as FlowR calls them: who reads a layer, stats,
//! paging and the feed of a space, and a branch or clone at a past point.

use std::sync::Arc;

use adb_executor::{Engine, LocalEngine};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use serde_json::{json, Value};

async fn action(url: &str, kind: &str, body: Value) -> Result<Value, (String, String)> {
    let ch = tonic::transport::Channel::from_shared(url.to_string()).unwrap().connect().await.unwrap();
    let mut c = FlightServiceClient::new(ch);
    match c.do_action(Action { r#type: kind.into(), body: body.to_string().into() }).await {
        Ok(r) => {
            use futures::StreamExt;
            Ok(serde_json::from_slice(&r.into_inner().next().await.unwrap().unwrap().body).unwrap())
        }
        Err(s) => Err((s.metadata().get("adb-code").and_then(|v| v.to_str().ok()).unwrap_or("").to_string(), s.message().to_string())),
    }
}

async fn serve(dir: &std::path::Path) -> String {
    let (engine, _) = LocalEngine::durable(dir).await.unwrap();
    let engine: Arc<dyn Engine> = Arc::new(engine);
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = adb_server::serve_flight(std::net::SocketAddr::from(([127, 0, 0, 1], port)), engine).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    format!("http://127.0.0.1:{port}")
}

async fn aql(f: &str, q: &str) -> Value {
    let r = action(f, "aql", json!({ "query": q })).await.unwrap();
    assert_eq!(r["success"], json!(true), "{q}: {r}");
    r
}

/// Keys in a `RECALL`'s answer.
fn holds(answer: &Value, word: &str) -> bool {
    answer.to_string().contains(word)
}

#[tokio::test(flavor = "multi_thread")]
async fn layer_dependents_list_each_reader() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    aql(&f, r#"STORE INTO PROCEDURAL (key = "rule", steps = "v1") NAMESPACE "compliance""#).await;
    action(&f, "tag_create", json!({ "namespace": "compliance", "name": "v1" })).await.unwrap();
    let layer = |mode: &str, tag: Option<&str>, sealed: bool| json!({ "space": "compliance", "mode": mode, "tag": tag, "memories": ["PROCEDURAL"], "sealed": sealed });
    action(&f, "layers_set", json!({ "namespace": "trading", "layers": [layer("pin", Some("v1"), true)] })).await.unwrap();
    action(&f, "layers_set", json!({ "namespace": "support", "layers": [layer("mount", None, false)] })).await.unwrap();

    let d = action(&f, "layers_dependents", json!({ "space": "compliance" })).await.unwrap();
    let rows = d["dependents"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{d}");
    let trading = rows.iter().find(|r| r["namespace"] == json!("trading")).unwrap();
    assert_eq!((trading["mode"].clone(), trading["tag"].clone(), trading["sealed"].clone()), (json!("pin"), json!("v1"), json!(true)), "{d}");
    let support = rows.iter().find(|r| r["namespace"] == json!("support")).unwrap();
    assert_eq!((support["mode"].clone(), support["sealed"].clone()), (json!("mount"), json!(false)), "{d}");
    assert!(support.get("tag").is_none(), "{d}");

    let none = action(&f, "layers_dependents", json!({ "space": "trading" })).await.unwrap();
    assert_eq!(none["dependents"], json!([]));
}

#[tokio::test(flavor = "multi_thread")]
async fn stats_count_each_tier_of_one_space_and_its_branch() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "a", content = "first") NAMESPACE "crm""#).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "b", content = "second") NAMESPACE "crm""#).await;
    aql(&f, r#"STORE INTO PROCEDURAL (key = "p", steps = "s") NAMESPACE "crm""#).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "other", content = "not this space") NAMESPACE "billing""#).await;
    // An update is one record, not two.
    aql(&f, r#"UPDATE INTO SEMANTIC WHERE key = "a" (content = "first, edited") NAMESPACE "crm""#).await;

    let s = action(&f, "space_stats", json!({ "namespace": "crm", "max_age_ms": 0 })).await.unwrap();
    assert_eq!(s["tiers"]["SEMANTIC"]["count"], json!(2), "{s}");
    assert_eq!(s["tiers"]["PROCEDURAL"]["count"], json!(1), "{s}");
    assert_eq!(s["tiers"]["TOOLS"]["count"], json!(0), "{s}");
    assert!(s["tiers"]["SEMANTIC"]["bytes"].as_u64().unwrap() > 0, "{s}");
    assert!(s["tiers"]["SEMANTIC"]["last_write_ts"].as_i64().unwrap() > 0, "{s}");
    assert!(s["tiers"]["TOOLS"]["last_write_ts"].is_null(), "{s}");

    // Kept for the asked age: a new write doesn't show until it passes.
    aql(&f, r#"STORE INTO SEMANTIC (key = "c", content = "third") NAMESPACE "crm""#).await;
    let kept = action(&f, "space_stats", json!({ "namespace": "crm", "max_age_ms": 60000 })).await.unwrap();
    assert_eq!(kept["tiers"]["SEMANTIC"]["count"], json!(2), "{kept}");
    let fresh = action(&f, "space_stats", json!({ "namespace": "crm", "max_age_ms": 0 })).await.unwrap();
    assert_eq!(fresh["tiers"]["SEMANTIC"]["count"], json!(3), "{fresh}");
}

#[tokio::test(flavor = "multi_thread")]
async fn records_page_newest_first_and_new_writes_do_not_shift_pages() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    for i in 0..5 {
        aql(&f, &format!(r#"STORE INTO SEMANTIC (key = "k{i}", content = "c{i}") NAMESPACE "crm""#)).await;
    }
    let page = |after: Value| {
        let f = f.clone();
        async move { action(&f, "space_records", json!({ "namespace": "crm", "memory": "SEMANTIC", "limit": 2, "after": after })).await.unwrap() }
    };
    let keys = |p: &Value| p["records"].as_array().unwrap().iter().map(|r| r["key"].as_str().unwrap().to_string()).collect::<Vec<_>>();
    let p1 = page(Value::Null).await;
    assert_eq!(keys(&p1), ["k4", "k3"], "{p1}");
    assert_eq!(p1["records"][0]["value"]["content"], json!("c4"), "{p1}");
    // A write between pages lands before the cursor.
    aql(&f, r#"STORE INTO SEMANTIC (key = "new", content = "n") NAMESPACE "crm""#).await;
    let p2 = page(p1["next"].clone()).await;
    assert_eq!(keys(&p2), ["k2", "k1"], "{p2}");
    let p3 = page(p2["next"].clone()).await;
    assert_eq!(keys(&p3), ["k0"], "{p3}");
    assert!(p3["next"].is_null(), "{p3}");
    let (_, msg) = action(&f, "space_records", json!({ "namespace": "crm", "memory": "SEMANTIC", "after": "not a cursor" })).await.unwrap_err();
    assert!(msg.contains("after"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn feed_follows_one_space_and_never_returns_an_erased_payload() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "before", content = "x") NAMESPACE "crm""#).await;

    // No offset: reset, then follow from next.
    let first = action(&f, "space_changes", json!({ "namespace": "crm" })).await.unwrap();
    assert_eq!(first["reset"], json!(true), "{first}");
    let from = first["next"].clone();

    aql(&f, r#"STORE INTO SEMANTIC (key = "fact", content = "secret words") NAMESPACE "crm""#).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "elsewhere", content = "y") NAMESPACE "billing""#).await;
    aql(&f, r#"STORE INTO EPISODIC (key = "ep", content = "talked") NAMESPACE "crm""#).await;
    aql(&f, r#"UPDATE INTO EPISODIC WHERE key = "ep" (content = "talked more") NAMESPACE "crm""#).await;
    aql(&f, r#"UPDATE INTO SEMANTIC WHERE key = "fact" (content = "secret words, edited") NAMESPACE "crm""#).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "gone", content = "erase these words") NAMESPACE "crm""#).await;
    aql(&f, r#"FORGET FROM SEMANTIC WHERE key = "gone" NAMESPACE "crm""#).await;

    // A feed answers with what is applied when it is read: follow it (as a consumer
    // does, from `next`) until the FORGET is in.
    let mut gathered: Vec<serde_json::Value> = Vec::new();
    let mut at = from.clone();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let page = action(&f, "space_changes", json!({ "namespace": "crm", "from": at, "wait_ms": 500 })).await.unwrap();
        assert_eq!(page["reset"], json!(false), "{page}");
        gathered.extend(page["changes"].as_array().cloned().unwrap_or_default());
        at = page["next"].clone();
        if gathered.iter().any(|c| c["op"] == "forget") || std::time::Instant::now() >= until {
            break;
        }
    }
    let all = json!({ "changes": gathered, "next": at });
    let changes = all["changes"].as_array().unwrap();
    let got: Vec<(String, String, String)> = changes
        .iter()
        .map(|c| (c["op"].as_str().unwrap().into(), c["memory"].as_str().unwrap().to_ascii_uppercase(), c["key"].as_str().unwrap_or_default().to_string()))
        .collect();
    let want = |op: &str, m: &str, k: &str| (op.to_string(), m.to_string(), k.to_string());
    for w in [
        want("put", "SEMANTIC", "fact"),
        want("put", "EPISODIC", "ep"),
        want("update", "EPISODIC", "ep"),
        want("update", "SEMANTIC", "fact"),
        want("forget", "SEMANTIC", "gone"),
    ] {
        assert!(got.contains(&w), "missing {w:?} in {got:?}");
    }
    assert!(got.iter().all(|(_, _, k)| k != "elsewhere"), "another space's write: {got:?}");
    assert!(!all.to_string().contains("erase these words"), "an erased payload in the feed: {all}");
    assert!(changes.windows(2).all(|w| w[0]["offset"].as_u64() <= w[1]["offset"].as_u64()), "{all}");

    // One memory type only; a waiting read wakes on the next write.
    let only = action(&f, "space_changes", json!({ "namespace": "crm", "from": from, "memories": ["EPISODIC"] })).await.unwrap();
    assert!(only["changes"].as_array().unwrap().iter().all(|c| c["memory"].as_str().unwrap().eq_ignore_ascii_case("EPISODIC")), "{only}");
    assert_eq!(only["changes"].as_array().unwrap().len(), 2, "{only}");

    let next = all["next"].clone();
    let g = f.clone();
    let wait = tokio::spawn(async move { action(&g, "space_changes", json!({ "namespace": "crm", "from": next, "wait_ms": 10000 })).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let t = std::time::Instant::now();
    aql(&f, r#"STORE INTO SEMANTIC (key = "late", content = "z") NAMESPACE "crm""#).await;
    let woke = wait.await.unwrap();
    assert_eq!(woke["changes"][0]["key"], json!("late"), "{woke}");
    assert!(t.elapsed() < std::time::Duration::from_secs(2), "reached the feed in {:?}", t.elapsed());
}

/// DB-17: a procedures-only copy carries no episodes or facts.
#[tokio::test(flavor = "multi_thread")]
async fn a_procedures_only_copy_carries_nothing_about_clients() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    aql(&f, r#"STORE INTO PROCEDURAL (key = "refund", steps = "check, then pay") NAMESPACE "acme--bot""#).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "client", content = "client secret fact") NAMESPACE "acme--bot""#).await;
    aql(&f, r#"STORE INTO EPISODIC (key = "talk", content = "client said something") NAMESPACE "acme--bot""#).await;
    aql(&f, r#"STORE INTO WORKING (key = "w", content = "client working note") NAMESPACE "acme--bot""#).await;
    aql(&f, r#"STORE INTO TOOLS (key = "t", name = "lookup", description = "a tool", schema = "{}") NAMESPACE "acme--bot""#).await;

    let c = action(&f, "space_clone", json!({ "namespace": "acme--bot", "into": "globex--bot", "procedures_only": true })).await.unwrap();
    assert_eq!(c["records"], json!(1), "{c}");
    assert!(holds(&aql(&f, r#"RECALL FROM PROCEDURAL NAMESPACE "globex--bot""#).await, "check, then pay"));
    let stats = action(&f, "space_stats", json!({ "namespace": "globex--bot", "max_age_ms": 0 })).await.unwrap();
    for (tier, count) in [("PROCEDURAL", 1), ("SEMANTIC", 0), ("EPISODIC", 0), ("WORKING", 0), ("TOOLS", 0)] {
        assert_eq!(stats["tiers"][tier]["count"], json!(count), "{tier}: {stats}");
    }
    let with = action(&f, "space_clone", json!({ "namespace": "acme--bot", "into": "initech--bot", "procedures_only": true, "tools": true })).await.unwrap();
    assert_eq!(with["records"], json!(2), "{with}");
    assert_eq!(action(&f, "space_clone", json!({ "namespace": "acme--bot", "into": "x", "procedures_only": true, "at": { "tag": "v" } })).await.unwrap_err().0, "point_invalid");
}

/// A branch over Flight, as A3T reviews one: `branch: "try"` reads the namespace
/// `crm@try` (its own records and `crm`'s at the point) for stats, records and the
/// feed; a clone at a tag reads what the tag held.
#[tokio::test(flavor = "multi_thread")]
async fn a_branch_and_a_clone_at_a_tag_over_flight() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "a", content = "first") NAMESPACE "crm""#).await;
    action(&f, "tag_create", json!({ "namespace": "crm", "name": "v1" })).await.unwrap();
    let b = action(&f, "branch_create", json!({ "namespace": "crm", "name": "try" })).await.unwrap();
    assert_eq!(b["parent"], json!("main"), "{b}");
    let (_, msg) = action(&f, "branch_create", json!({ "namespace": "crm", "name": "past", "at": { "tag": "v1" } })).await.unwrap_err();
    assert!(msg.contains("AS OF"), "{msg}");

    let from = action(&f, "space_changes", json!({ "namespace": "crm", "branch": "try" })).await.unwrap()["next"].clone();
    aql(&f, r#"STORE INTO SEMANTIC (key = "b", content = "on the branch") NAMESPACE "crm@try""#).await;
    aql(&f, r#"UPDATE INTO SEMANTIC WHERE key = "a" (content = "first, edited on main") NAMESPACE "crm""#).await;

    let s = action(&f, "space_stats", json!({ "namespace": "crm", "branch": "try", "max_age_ms": 0 })).await.unwrap();
    assert_eq!(s["tiers"]["SEMANTIC"]["count"], json!(2), "{s}");
    let r = action(&f, "space_records", json!({ "namespace": "crm", "branch": "try", "memory": "SEMANTIC", "limit": 10 })).await.unwrap();
    assert!(holds(&r, "on the branch") && holds(&r, "\"first\"") && !holds(&r, "edited on main"), "the base at the point: {r}");
    let mut seen = Vec::new();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut at = from;
    while seen.is_empty() && std::time::Instant::now() < until {
        let page = action(&f, "space_changes", json!({ "namespace": "crm", "branch": "try", "from": at, "wait_ms": 500 })).await.unwrap();
        seen.extend(page["changes"].as_array().cloned().unwrap_or_default());
        at = page["next"].clone();
    }
    assert_eq!(seen.len(), 1, "the branch's own write only: {seen:?}");
    assert_eq!(seen[0]["key"], json!("b"), "{seen:?}");

    // A clone at the tag: what main held then.
    let c = action(&f, "space_clone", json!({ "namespace": "crm", "into": "crm-v1", "at": { "tag": "v1" } })).await.unwrap();
    assert_eq!(c["records"], json!(1), "{c}");
    let then = aql(&f, r#"RECALL FROM SEMANTIC NAMESPACE "crm-v1""#).await;
    assert!(holds(&then, "\"first\"") && !holds(&then, "edited"), "{then}");
}

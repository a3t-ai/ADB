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

    // A branch counts its own view; opt-in on branch_list.
    action(&f, "branch_create", json!({ "namespace": "crm", "name": "try" })).await.unwrap();
    aql(&f, r#"STORE INTO SEMANTIC (key = "d", content = "only on the branch") NAMESPACE "crm" BRANCH "try""#).await;
    let b = action(&f, "space_stats", json!({ "namespace": "crm", "branch": "try", "max_age_ms": 0 })).await.unwrap();
    assert_eq!(b["tiers"]["SEMANTIC"]["count"], json!(4), "{b}");
    let main = action(&f, "space_stats", json!({ "namespace": "crm", "max_age_ms": 0 })).await.unwrap();
    assert_eq!(main["tiers"]["SEMANTIC"]["count"], json!(3), "{main}");
    let plain = action(&f, "branch_list", json!({ "namespace": "crm" })).await.unwrap();
    assert!(plain["branches"][0].get("stats").is_none(), "{plain}");
    let with = action(&f, "branch_list", json!({ "namespace": "crm", "stats": true })).await.unwrap();
    assert_eq!(with["branches"][0]["stats"]["tiers"]["SEMANTIC"]["count"], json!(4), "{with}");
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

    let all = action(&f, "space_changes", json!({ "namespace": "crm", "from": from, "wait_ms": 2000 })).await.unwrap();
    assert_eq!(all["reset"], json!(false), "{all}");
    let changes = all["changes"].as_array().unwrap();
    let got: Vec<(String, String, String)> = changes
        .iter()
        .map(|c| (c["op"].as_str().unwrap().into(), c["memory"].as_str().unwrap().to_ascii_uppercase(), c["key"].as_str().unwrap_or_default().to_string()))
        .collect();
    let want = |op: &str, m: &str, k: &str| (op.to_string(), m.to_string(), k.to_string());
    for w in [
        want("put", "SEMANTIC", "fact"),
        want("put", "EPISODIC", "ep"),
        want("patch", "EPISODIC", "ep"),
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

    // A branch's writes are on its own feed, not main's.
    action(&f, "branch_create", json!({ "namespace": "crm", "name": "try" })).await.unwrap();
    let main_from = woke["next"].clone();
    let br = action(&f, "space_changes", json!({ "namespace": "crm", "branch": "try" })).await.unwrap();
    aql(&f, r#"STORE INTO SEMANTIC (key = "branched", content = "b") NAMESPACE "crm" BRANCH "try""#).await;
    let on_branch = action(&f, "space_changes", json!({ "namespace": "crm", "branch": "try", "from": br["next"], "wait_ms": 2000 })).await.unwrap();
    assert!(holds(&on_branch["changes"], "branched"), "{on_branch}");
    let on_main = action(&f, "space_changes", json!({ "namespace": "crm", "from": main_from })).await.unwrap();
    assert!(!holds(&on_main["changes"], "branched"), "{on_main}");
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_and_clone_at_a_tag_or_offset_read_what_that_point_held() {
    let dir = tempfile::tempdir().unwrap();
    let f = serve(dir.path()).await;
    aql(&f, r#"STORE INTO SEMANTIC (key = "fact", content = "original") NAMESPACE "crm""#).await;
    action(&f, "tag_create", json!({ "namespace": "crm", "name": "v1" })).await.unwrap();
    let point = aql(&f, r#"STORE INTO PROCEDURAL (key = "proc", steps = "first") NAMESPACE "crm""#).await;
    let offset = point["data"]["metadata"]["point"]["offset"].as_u64().unwrap_or_else(|| panic!("no point in {point}"));
    aql(&f, r#"UPDATE INTO SEMANTIC WHERE key = "fact" (content = "changed later") NAMESPACE "crm""#).await;
    aql(&f, r#"UPDATE INTO PROCEDURAL WHERE key = "proc" (steps = "second") NAMESPACE "crm""#).await;

    // At a tag: what v1 held. It came before the procedure existed.
    let b = action(&f, "branch_create", json!({ "namespace": "crm", "name": "at-tag", "at": { "tag": "v1" } })).await.unwrap();
    assert_eq!(b["exact"], json!(true), "{b}");
    let read = aql(&f, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" BRANCH "at-tag" LIMIT 10"#).await;
    assert!(holds(&read, "original") && !holds(&read, "changed later"), "{read}");
    let procs = aql(&f, r#"RECALL FROM PROCEDURAL ALL NAMESPACE "crm" BRANCH "at-tag" LIMIT 10"#).await;
    assert!(!holds(&procs, "first") && !holds(&procs, "second"), "{procs}");

    // At an offset: the memory point of an answer.
    action(&f, "branch_create", json!({ "namespace": "crm", "name": "at-off", "at": { "offset": offset } })).await.unwrap();
    let procs = aql(&f, r#"RECALL FROM PROCEDURAL ALL NAMESPACE "crm" BRANCH "at-off" LIMIT 10"#).await;
    assert!(holds(&procs, "first") && !holds(&procs, "second"), "{procs}");

    // The branch writes on top of the past, main is untouched.
    aql(&f, r#"STORE INTO SEMANTIC (key = "new", content = "from the past") NAMESPACE "crm" BRANCH "at-off""#).await;
    let main = aql(&f, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm" LIMIT 10"#).await;
    assert!(holds(&main, "changed later") && !holds(&main, "from the past"), "{main}");

    // A clone at the tag.
    let c = action(&f, "space_clone", json!({ "namespace": "crm", "into": "crm-v1", "at": { "tag": "v1" } })).await.unwrap();
    assert_eq!(c["records"], json!(1), "{c}");
    assert!(c["from"]["point"]["offset"].is_u64(), "{c}");
    let cloned = aql(&f, r#"RECALL FROM SEMANTIC ALL NAMESPACE "crm-v1" LIMIT 10"#).await;
    assert!(holds(&cloned, "original") && !holds(&cloned, "changed later"), "{cloned}");

    // EPISODIC changes in place, so a past point isn't exact once the space holds some.
    aql(&f, r#"STORE INTO EPISODIC (key = "ep", content = "talked") NAMESPACE "crm""#).await;
    let later = aql(&f, r#"STORE INTO SEMANTIC (key = "z", content = "z") NAMESPACE "crm""#).await;
    let at = later["data"]["metadata"]["point"]["offset"].as_u64().unwrap();
    let inexact = action(&f, "branch_create", json!({ "namespace": "crm", "name": "inexact", "at": { "offset": at } })).await.unwrap();
    assert_eq!(inexact["exact"], json!(false), "{inexact}");

    // Refusals, with their codes.
    let refused = |body: Value| {
        let f = f.clone();
        async move { action(&f, "branch_create", body).await.unwrap_err().0 }
    };
    assert_eq!(refused(json!({ "namespace": "crm", "name": "x1", "at": { "tag": "v9" } })).await, "tag_not_found");
    assert_eq!(refused(json!({ "namespace": "crm", "name": "x2", "at": {} })).await, "point_invalid");
    assert_eq!(refused(json!({ "namespace": "crm", "name": "x3", "at": { "offset": 99999999 } })).await, "point_invalid");
    assert_eq!(refused(json!({ "namespace": "crm", "name": "x4", "parent": "at-tag", "at": { "tag": "v1" } })).await, "point_invalid");
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
    assert!(holds(&aql(&f, r#"RECALL FROM PROCEDURAL ALL NAMESPACE "globex--bot""#).await, "check, then pay"));
    let stats = action(&f, "space_stats", json!({ "namespace": "globex--bot", "max_age_ms": 0 })).await.unwrap();
    for (tier, count) in [("PROCEDURAL", 1), ("SEMANTIC", 0), ("EPISODIC", 0), ("WORKING", 0), ("TOOLS", 0)] {
        assert_eq!(stats["tiers"][tier]["count"], json!(count), "{tier}: {stats}");
    }
    let with = action(&f, "space_clone", json!({ "namespace": "acme--bot", "into": "initech--bot", "procedures_only": true, "tools": true })).await.unwrap();
    assert_eq!(with["records"], json!(2), "{with}");
    assert_eq!(action(&f, "space_clone", json!({ "namespace": "acme--bot", "into": "x", "procedures_only": true, "at": { "tag": "v" } })).await.unwrap_err().0, "point_invalid");
}

//! GR-15: the fact feed — a snapshot of the current facts, then their changes in
//! log order (`add` / `remove`), as a consumer keeping a copy reads them.

use std::collections::BTreeMap;
use std::time::Duration;

use adb_executor::{Engine, LocalEngine};
use adb_gateway::{FactBatch, FactQuery, Gateway};
use serde_json::Value;

async fn q(engine: &LocalEngine, aql: &str) -> Value {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"))
}

fn later(d: Duration) -> String {
    (chrono::Utc::now() + chrono::Duration::from_std(d).unwrap()).to_rfc3339()
}

fn query(from: Option<u64>) -> FactQuery {
    FactQuery { from, max: 1000, ..Default::default() }
}

fn ops(b: &FactBatch) -> Vec<String> {
    b.changes.iter().map(|c| format!("{}:{}:{}", c.op, c.id, c.version.map(|v| v.to_string()).unwrap_or_default())).collect()
}

/// Reads from `from` until `n` changes have arrived (writes are applied in the
/// background): their ops, and the next position.
async fn read_n(gw: &Gateway, from: Option<u64>, n: usize) -> (Vec<String>, u64) {
    let (mut out, mut from) = (Vec::new(), from);
    for _ in 0..50 {
        let b = gw.facts(&FactQuery { wait: Duration::from_millis(100), ..query(from) }).await.unwrap();
        assert!(!b.snapshot);
        out.extend(ops(&b));
        from = Some(b.next);
        if out.len() >= n {
            break;
        }
    }
    (out, from.unwrap())
}

/// A consumer's copy: id → (version, object or value).
#[derive(Default)]
struct Copy {
    facts: BTreeMap<String, (u32, String)>,
    from: Option<u64>,
}

impl Copy {
    async fn read(&mut self, gw: &Gateway) -> FactBatch {
        let b = gw.facts(&query(self.from)).await.unwrap();
        if b.snapshot {
            self.facts.clear();
        }
        for c in &b.changes {
            match (c.op, &c.fact) {
                ("add", Some(f)) => {
                    let shown = f.object.clone().unwrap_or_else(|| f.value.as_ref().map(|v| v.to_string()).unwrap_or_default());
                    self.facts.insert(c.id.clone(), (c.version.unwrap(), shown));
                }
                ("remove", _) => {
                    self.facts.remove(&c.id);
                }
                other => panic!("unexpected change {other:?}"),
            }
        }
        self.from = Some(b.next);
        b
    }

    fn shown(&self) -> Vec<String> {
        self.facts.iter().map(|(k, (v, s))| format!("{k}@{v}={s}")).collect()
    }
}

/// The copy a consumer keeps matches `RECALL FROM SEMANTIC`, through stores,
/// supersedes, updates, forgets, TTL and validity bounds; reading an offset twice
/// gives the same rows; a reader from a purged offset gets a snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn fact_feed_follows_the_current_view() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    let gw = engine.gateway().unwrap();
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:acme", confidence = 0.9, source = "hr")"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "age", value = 36)"#).await;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "worked_at", object = "org:old", valid_to = "2020-01-01")"#).await;
    // Entities and other memory types are not facts.
    q(&engine, r#"STORE INTO WORKING (k = "w")"#).await;

    // A snapshot is as of the applied tip: wait until it covers the writes.
    let mut copy = Copy::default();
    let mut b = copy.read(&gw).await;
    for _ in 0..50 {
        if copy.facts.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        copy = Copy::default();
        b = copy.read(&gw).await;
    }
    assert!(b.snapshot, "no `from`: a snapshot");
    assert_eq!(copy.shown(), ["fact:person:ada|age@1=36", "fact:person:ada|works_at@1=org:acme"]);
    let acme = b.changes.iter().find(|c| c.id == "fact:person:ada|works_at").unwrap().fact.clone().unwrap();
    assert_eq!((acme.subject.as_str(), acme.predicate.as_str(), acme.confidence), ("person:ada", "works_at", 0.9));
    assert_eq!(acme.data["source"], "hr");
    assert_eq!(b.next, b.tip + 1);

    // Changes after the snapshot's tip may repeat what it showed; then nothing new.
    let shown = copy.shown();
    copy.read(&gw).await;
    assert_eq!(copy.shown(), shown, "repeats change nothing");
    let quiet = gw.facts(&FactQuery { wait: Duration::from_millis(100), ..query(copy.from) }).await.unwrap();
    assert!(!quiet.snapshot && quiet.changes.is_empty());

    let (soon, bit_later) = (later(Duration::from_secs(3)), later(Duration::from_millis(3500)));
    let from = copy.from;
    q(&engine, r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "works_at", object = "org:globex")"#).await;
    q(&engine, r#"FORGET FROM SEMANTIC WHERE predicate = "age""#).await;
    q(&engine, r#"UPDATE INTO SEMANTIC WHERE predicate = "works_at" (confidence_note = "checked")"#).await;
    q(&engine, &format!(r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "visits", object = "city:york", valid_to = "{soon}")"#)).await;
    q(&engine, &format!(r#"STORE INTO SEMANTIC FACT (subject = "person:ada", predicate = "moves_to", object = "city:paris", valid_from = "{bit_later}")"#)).await;
    let (seen, _) = read_n(&gw, from, 6).await;
    assert_eq!(
        seen,
        [
            "remove:fact:person:ada|works_at:1",
            "add:fact:person:ada|works_at:2",
            "remove:fact:person:ada|age:",
            "remove:fact:person:ada|works_at:2",
            "add:fact:person:ada|works_at:3",
            "add:fact:person:ada|visits:1",
        ],
        "moves_to holds later: not added yet"
    );
    assert_eq!(read_n(&gw, from, 6).await.0, seen, "the same offset gives the same rows");
    copy.read(&gw).await;
    assert_eq!(copy.shown(), ["fact:person:ada|visits@1=city:york", "fact:person:ada|works_at@3=org:globex"]);
    // An updated fact still links its ends.
    let linked = q(&engine, r#"LOOKUP FROM SEMANTIC KEY id = "person:ada" FOLLOW LINKS ALL"#).await;
    let ids: Vec<&str> = linked["data"]["records"].as_array().unwrap().iter().filter_map(|r| r["id"].as_str()).collect();
    assert!(ids.contains(&"org:globex"), "{ids:?}");

    // The bounds pass: visits leaves, moves_to enters.
    for _ in 0..60 {
        copy.read(&gw).await;
        if copy.facts.contains_key("fact:person:ada|moves_to") && !copy.facts.contains_key("fact:person:ada|visits") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(copy.shown(), ["fact:person:ada|moves_to@1=city:paris", "fact:person:ada|works_at@3=org:globex"]);

    // The copy is what RECALL shows; a fresh snapshot agrees.
    let recall = q(&engine, r#"RECALL FROM SEMANTIC WHERE subject = "person:ada""#).await;
    let mut objects: Vec<&str> = recall["data"]["records"].as_array().unwrap().iter().map(|r| r["data"]["object"].as_str().unwrap()).collect();
    objects.sort();
    assert_eq!(objects, ["city:paris", "org:globex"]);
    let mut fresh = Copy::default();
    fresh.read(&gw).await;
    assert_eq!(fresh.shown(), copy.shown());

    // A scope filter.
    let none = gw.facts(&FactQuery { scope: Some("tenant:other".into()), ..query(None) }).await.unwrap();
    assert!(none.snapshot && none.changes.is_empty());
}

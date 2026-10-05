//! B1: conditional writes on Hetzner through object_store (the client Lance uses).
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use std::{collections::BTreeMap, env, sync::Arc};

#[tokio::main]
async fn main() {
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(env::var("AWS_ENDPOINT").unwrap())
            .with_region(env::var("AWS_REGION").unwrap())
            .with_bucket_name(env::var("ADB_BUCKET").unwrap())
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
            .with_virtual_hosted_style_request(false)
            .build()
            .expect("build store"),
    );
    let rounds: usize = env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let writers: usize = env::var("W").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
    let base = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

    let (mut ok, mut multi, mut none, mut mismatch) = (0, 0, 0, 0);
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for r in 0..rounds {
        let path = Path::from(format!("b1-test/os-race/k-{base}-{r}"));
        let futs = (0..writers).map(|w| {
            let s = store.clone();
            let p = path.clone();
            async move {
                let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
                (w, s.put_opts(&p, PutPayload::from(format!("writer-{w}")), opts).await)
            }
        });
        let res = futures::future::join_all(futs).await;
        let mut winners = vec![];
        for (w, r) in res {
            let k = match &r {
                Ok(_) => { winners.push(w); "Ok".to_string() }
                Err(object_store::Error::AlreadyExists { .. }) => "AlreadyExists".to_string(),
                Err(object_store::Error::Precondition { .. }) => "Precondition".to_string(),
                Err(e) => format!("Other({})", e.to_string().chars().take(60).collect::<String>()),
            };
            let key = if k.starts_with("Other") { if k.contains("403") || k.contains("Access") { "Other(403 AccessDenied)".to_string() } else { k } } else { k };
            *kinds.entry(key).or_default() += 1;
        }
        match winners.len() {
            0 => none += 1,
            1 => {
                let mut body = String::new();
                for _ in 0..5 {
                    if let Ok(g) = store.get(&path).await { body = String::from_utf8(g.bytes().await.unwrap().to_vec()).unwrap(); break; }
                }
                if body == format!("writer-{}", winners[0]) { ok += 1 } else { mismatch += 1; println!("round {r}: winner {} stored {body:?}", winners[0]) }
            }
            n => { multi += 1; println!("round {r}: {n} winners: {winners:?}") }
        }
    }
    println!("PutMode::Create race: rounds={rounds} writers={writers}");
    for (k, n) in &kinds { println!("  {k:<26} {n}"); }
    println!("  exactly one winner + correct body: {ok}");
    println!("  >1 winner (VIOLATION): {multi}   wrong body (VIOLATION): {mismatch}   no winner (all errors): {none}");

    // PutMode::Update (If-Match) — Lance's commit does not use it; document the behaviour.
    let p = Path::from(format!("b1-test/os-update/u-{base}"));
    let mut tally: BTreeMap<String, usize> = BTreeMap::new();
    for i in 0..20 {
        let put = loop { if let Ok(r) = store.put(&p, PutPayload::from(format!("v{i}"))).await { break r; } };
        let upd = UpdateVersion { e_tag: put.e_tag.clone(), version: put.version.clone() };
        let r = store.put_opts(&p, PutPayload::from(format!("v{i}-updated")), PutOptions { mode: PutMode::Update(upd), ..Default::default() }).await;
        let k = match r { Ok(_) => "Ok".into(), Err(object_store::Error::Precondition { .. }) => "Precondition (412)".into(), Err(e) => format!("Other({})", e.to_string().chars().take(50).collect::<String>()) };
        *tally.entry(k).or_default() += 1;
    }
    println!("PutMode::Update with the e_tag the server just returned (should always succeed):");
    for (k, n) in &tally { println!("  {k:<26} {n}"); }
}

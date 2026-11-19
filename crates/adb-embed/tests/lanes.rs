//! The model's two lanes: a foreground request is served before a background backlog
//! queued ahead of it (deferred vectors must not hold up a writer or reader waiting).
//! Needs the model (`ADB_EMBED_MODEL_DIR`); skipped without it.

use std::sync::Arc;
use std::time::Instant;

#[tokio::test(flavor = "multi_thread")]
async fn a_foreground_request_passes_the_background_backlog() {
    let Ok(dir) = std::env::var("ADB_EMBED_MODEL_DIR") else {
        eprintln!("no ADB_EMBED_MODEL_DIR; skipped");
        return;
    };
    std::env::set_var("ADB_EMBED_WORKERS", "1");
    let e = Arc::new(adb_embed::Embedder::load(std::path::Path::new(&dir), 1).unwrap());
    let texts = |n: usize, tag: &str| (0..n).map(|i| format!("{tag} background text number {i} about invoices and renewals")).collect::<Vec<_>>();
    // A backlog of 30 background calls of 32 texts each on one worker.
    let t0 = Instant::now();
    let mut backlog = Vec::new();
    for b in 0..30 {
        let (e, t) = (Arc::clone(&e), texts(32, &format!("b{b}")));
        backlog.push(tokio::spawn(async move {
            e.embed_background(t).await.unwrap();
            t0.elapsed()
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let fg = e.embed(vec!["When is the Acme renewal?".into()]).await.unwrap();
    let fg_done = t0.elapsed();
    assert_eq!(fg.len(), 1);
    let mut after = 0;
    for b in backlog {
        if b.await.unwrap() > fg_done {
            after += 1;
        }
    }
    assert!(after >= 25, "the foreground request waited for the backlog: only {after} of 30 background calls finished after it");
}

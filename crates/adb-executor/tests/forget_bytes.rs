//! M2 exit criterion (W9.9, spike S11): after FORGET nothing of the record is
//! recoverable from disk — no file under the data directory (log, table, MemWAL,
//! versions) contains its bytes, also after a restart.

use std::path::Path;

use adb_executor::{Engine, LocalEngine};

async fn q(engine: &LocalEngine, aql: &str) {
    engine.query(aql, None).await.unwrap_or_else(|e| panic!("{aql}: {e}"));
}

/// Files under `dir` that contain `needle`.
fn holders(dir: &Path, needle: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(bytes) = std::fs::read(&p) {
                if bytes.windows(needle.len()).any(|w| w == needle.as_bytes()) {
                    out.push(p.strip_prefix(dir).unwrap().display().to_string());
                }
            }
        }
    }
    out
}

#[tokio::test]
async fn a_forgotten_record_leaves_no_bytes_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    // Markers in a first version, an update (a second version) and a patched
    // WORKING record; LINKs to and from the fact.
    let (v1, v2, w) = ("W9SECRETv1-7f3a91c2", "W9SECRETv2-0b44de17", "W9SECRETwk-5e8a2b90");
    {
        let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
        q(&engine, &format!(r#"STORE INTO SEMANTIC (key = "secret", content = "{v1}")"#)).await;
        q(&engine, r#"STORE INTO SEMANTIC (key = "other", content = "nothing to hide")"#).await;
        q(&engine, &format!(r#"UPDATE INTO SEMANTIC WHERE key = "secret" (content = "{v2}")"#)).await;
        q(&engine, r#"LINK FROM SEMANTIC WHERE key = "secret" TO SEMANTIC WHERE key = "other" TYPE "rel""#).await;
        q(&engine, &format!(r#"STORE INTO WORKING (key = "scratch", note = "{w}")"#)).await;
        for i in 0..300 {
            q(&engine, &format!(r#"STORE INTO EPISODIC (key = "e{i}", content = "filler {i}")"#)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        // The scan finds the markers while they exist (it would prove nothing otherwise).
        for m in [v1, v2, w] {
            assert!(!holders(dir.path(), m).is_empty(), "{m} should be on disk before FORGET");
        }

        q(&engine, r#"FORGET FROM SEMANTIC WHERE key = "secret""#).await;
        q(&engine, r#"FORGET FROM WORKING WHERE key = "scratch""#).await;
        for m in [v1, v2, w] {
            assert_eq!(holders(dir.path(), m), Vec::<String>::new(), "{m} after FORGET");
        }
    }
    // And after a restart (replay, rebuild): still nowhere.
    let (engine, _) = LocalEngine::durable(dir.path()).await.unwrap();
    q(&engine, r#"STORE INTO SEMANTIC (key = "after", content = "x")"#).await;
    drop(engine);
    for m in [v1, v2, w] {
        assert_eq!(holders(dir.path(), m), Vec::<String>::new(), "{m} after restart");
    }
}

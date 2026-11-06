//! DB-4: writes over a namespace's or an org's storage quota are refused with a code; usage
//! per namespace is reported; quotas survive a restart.

use adb_executor::{Engine, LocalEngine};

async fn store(e: &LocalEngine, ns: &str, key: &str) -> Result<serde_json::Value, String> {
    e.query(&format!(r#"STORE INTO SEMANTIC (key = "{key}", content = "some words") NAMESPACE "{ns}""#), None).await
}

#[tokio::test(flavor = "multi_thread")]
async fn quotas_hold_writes_and_usage_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
        store(&e, "acme--a", "before").await.unwrap();

        // A namespace: at its limit, the next is refused with the code; others are untouched.
        e.quota_set("acme--a", None, Some(2)).await.unwrap();
        store(&e, "acme--a", "second").await.unwrap();
        let refused = store(&e, "acme--a", "third").await.unwrap_err();
        assert_eq!(adb_core::codes::code_of(&refused), "quota_exceeded", "{refused}");
        store(&e, "globex--a", "elsewhere").await.unwrap();

        // Reads and FORGET still work, and FORGET frees room.
        assert!(e.query(r#"RECALL FROM SEMANTIC ALL NAMESPACE "acme--a""#, None).await.unwrap().to_string().contains("second"));
        e.query(r#"FORGET FROM SEMANTIC WHERE key = "second" NAMESPACE "acme--a""#, None).await.unwrap();
        store(&e, "acme--a", "third").await.unwrap();

        // An org: every namespace under its prefix together.
        e.quota_set("org--*", None, Some(3)).await.unwrap();
        store(&e, "org--a", "1").await.unwrap();
        store(&e, "org--b", "2").await.unwrap();
        store(&e, "org--c", "3").await.unwrap();
        let refused = store(&e, "org--a", "4").await.unwrap_err();
        assert_eq!(adb_core::codes::code_of(&refused), "quota_exceeded", "{refused}");

        // Bytes.
        e.quota_set("big", Some(200), None).await.unwrap();
        store(&e, "big", "fits").await.unwrap();
        let long = "x".repeat(500);
        let refused = e.query(&format!(r#"STORE INTO SEMANTIC (key = "long", content = "{long}") NAMESPACE "big""#), None).await.unwrap_err();
        assert_eq!(adb_core::codes::code_of(&refused), "quota_exceeded", "{refused}");

        // A flood of writes holds at the limit.
        e.quota_set("flood", None, Some(5)).await.unwrap();
        let mut ok = 0;
        for i in 0..20 {
            if store(&e, "flood", &format!("k{i}")).await.is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 5);

        // Usage per namespace.
        let usage = e.space_usage(None).await.unwrap();
        assert_eq!(usage["namespaces"]["flood"]["records"], serde_json::json!(5), "{usage}");
        assert_eq!(usage["namespaces"]["acme--a"]["records"], serde_json::json!(2), "{usage}");
        assert!(usage["namespaces"]["flood"]["bytes"].as_u64().unwrap() > 0, "{usage}");
        let one = e.space_usage(Some("big")).await.unwrap();
        assert_eq!(one["namespaces"].as_object().unwrap().len(), 1, "{one}");
    }

    // After a restart the quota holds, counted from the table.
    let (e, _) = LocalEngine::durable(dir.path()).await.unwrap();
    assert_eq!(e.quota_list().await.unwrap().len(), 4);
    let refused = store(&e, "flood", "after").await.unwrap_err();
    assert_eq!(adb_core::codes::code_of(&refused), "quota_exceeded", "{refused}");

    // Dropped, it no longer holds.
    assert!(e.quota_drop("flood").await.unwrap());
    store(&e, "flood", "after").await.unwrap();
    assert!(!e.quota_drop("flood").await.unwrap());

    // A branch's writes aren't the org's storage.
    e.branch_create("acme--a", "dev", None, None).await.unwrap();
    for i in 0..3 {
        e.query(&format!(r#"STORE INTO SEMANTIC (key = "b{i}", content = "x") NAMESPACE "acme--a" BRANCH "dev""#), None).await.unwrap();
    }
}

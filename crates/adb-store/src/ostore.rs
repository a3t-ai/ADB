//! One object-storage client per location, for the whole process.
//!
//! Lance's `ObjectStore::from_uri` builds a new client each call (a new registry): on
//! S3 that is the certificate store parsed again and a new TLS connection for every
//! segment shipped, listed or read — a node spent its CPU in TLS setup (Hetzner,
//! 15 Nov). Here each location's client is built once and shared; its connections are
//! reused.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// A store and the path of the location in it.
pub type Located = (std::sync::Arc<lance::io::ObjectStore>, object_store::path::Path);

/// The store for `uri` (built on first use) and `uri`'s path in it.
pub async fn located(uri: &str) -> lance::Result<Located> {
    static CACHE: OnceLock<Mutex<HashMap<String, Located>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(l) = cache.lock().unwrap().get(uri) {
        return Ok(l.clone());
    }
    let l = lance::io::ObjectStore::from_uri(uri).await?;
    cache.lock().unwrap().entry(uri.to_string()).or_insert_with(|| l.clone());
    Ok(l)
}

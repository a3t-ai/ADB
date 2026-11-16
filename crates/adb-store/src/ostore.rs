//! One object-storage client per location, for the whole process.
//!
//! Lance's `ObjectStore::from_uri` builds a new client each call (a new registry): on
//! S3 that is the certificate store parsed again and a new TLS connection for every
//! segment shipped, listed or read — a node spent its CPU in TLS setup (Hetzner,
//! 15 Nov). Here each location's client is built once and shared; its connections are
//! reused.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use object_store::ObjectStoreExt;

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

/// The object `name` directly under `uri`, or `None` when there is none.
pub async fn get_opt(uri: &str, name: &str) -> lance::Result<Option<Vec<u8>>> {
    let (store, base) = located(uri).await?;
    match store.inner.get(&base.join(name)).await {
        Ok(r) => Ok(Some(r.bytes().await.map_err(lance::Error::from)?.to_vec())),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Write `bytes` as `name` under `uri` unless an object of that name is there. A
/// store without conditional writes gets a plain write after a look (the caller reads
/// the object back: the last writer of a race wins, and both see the same).
pub async fn put_if_absent(uri: &str, name: &str, bytes: Vec<u8>) -> lance::Result<()> {
    let (store, base) = located(uri).await?;
    let path = base.join(name);
    let opts = object_store::PutOptions { mode: object_store::PutMode::Create, ..Default::default() };
    match store.inner.put_opts(&path, bytes.clone().into(), opts).await {
        Ok(_) | Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => Ok(()),
        Err(e) => {
            tracing::debug!(error = %e, "conditional write refused; writing after a look");
            if get_opt(uri, name).await?.is_none() {
                store.inner.put(&path, bytes.into()).await.map_err(lance::Error::from)?;
            }
            Ok(())
        }
    }
}

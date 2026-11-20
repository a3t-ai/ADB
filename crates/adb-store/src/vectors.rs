//! Vector files (20 Nov 2026): a partition's vectors live beside its table, not in it.
//!
//! Whoever makes the vectors — a compute node, or the database node's own model when
//! it has no compute node (CE, local dev) — writes them as one file per batch:
//!
//! ```text
//! {cold}/p{partition}/vectors/{millis}-{uuid}.vec    with object storage
//! {root}/p{partition}/vectors/{millis}-{uuid}.vec    without
//! ```
//!
//! and the owner commits a small `Vectors { uri }` log record naming the file. Every
//! node that applies the record (owner and replicas) loads the file into its
//! [`VectorMap`]; a node that opens the partition lists the folder. A vector never
//! changes a row of the table: before this, each one arriving made the owner look
//! the record up and rewrite its whole row, then replicate it (Hetzner p15b/p17:
//! reads fell 60% while vectors caught up).
//!
//! `LIKE` ranks the current rows with the vectors the map holds for them; a row
//! whose vector hasn't arrived yet ranks on its words. Vectors of rows that were
//! superseded or forgotten stay in the files and are never ranked (only current rows
//! are); a purge (not built yet) must remove them from the files too.
//!
//! File layout: `ADBV1\n`, model (u16 length + UTF-8), dimension (u32), count (u32),
//! then per vector its row id (u16 length + UTF-8) and `dimension` f16 values; all
//! integers little-endian.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures::StreamExt;
use half::f16;
use object_store::ObjectStoreExt;

use adb_core::log::PartitionId;

const MAGIC: &[u8] = b"ADBV1\n";
const EXT: &str = ".vec";
/// Files a partition may gather before its owner merges them into one.
pub const MERGE_AT: usize = 64;

pub type Entry = (String, Vec<f16>);

/// The folder of `partition`'s vector files.
pub fn location(cold: Option<&str>, root: &std::path::Path, partition: PartitionId) -> String {
    match cold {
        Some(c) => format!("{}/p{}/vectors", c.trim_end_matches('/'), partition.0),
        None => root.join(format!("p{}", partition.0)).join("vectors").to_string_lossy().into_owned(),
    }
}

/// A new file name: sorts by the time it was made.
pub fn new_name() -> String {
    let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    format!("{ms:013}-{}{EXT}", uuid::Uuid::new_v4().simple())
}

pub fn encode(model: &str, dim: usize, entries: &[Entry]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(MAGIC.len() + 12 + model.len() + entries.len() * (dim * 2 + 48));
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&u16::try_from(model.len()).map_err(|_| "model name too long")?.to_le_bytes());
    out.extend_from_slice(model.as_bytes());
    out.extend_from_slice(&(dim as u32).to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, v) in entries {
        if v.len() != dim {
            return Err(format!("vector of {id}: {} values, not {dim}", v.len()));
        }
        out.extend_from_slice(&u16::try_from(id.len()).map_err(|_| format!("row id too long: {id}"))?.to_le_bytes());
        out.extend_from_slice(id.as_bytes());
        for x in v {
            out.extend_from_slice(&x.to_bits().to_le_bytes());
        }
    }
    Ok(out)
}

/// A file's model, dimension and vectors.
pub fn decode(bytes: &[u8]) -> Result<(String, usize, Vec<Entry>), String> {
    struct R<'a>(&'a [u8]);
    impl<'a> R<'a> {
        fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
            if self.0.len() < n {
                return Err("vector file cut short".into());
            }
            let (a, b) = self.0.split_at(n);
            self.0 = b;
            Ok(a)
        }
        fn u16(&mut self) -> Result<usize, String> {
            Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as usize)
        }
        fn u32(&mut self) -> Result<usize, String> {
            Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
        }
        fn str(&mut self) -> Result<String, String> {
            let n = self.u16()?;
            String::from_utf8(self.take(n)?.to_vec()).map_err(|e| e.to_string())
        }
    }
    let mut r = R(bytes);
    if r.take(MAGIC.len())? != MAGIC {
        return Err("not a vector file".into());
    }
    let model = r.str()?;
    let dim = r.u32()?;
    let count = r.u32()?;
    let mut entries = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        let id = r.str()?;
        let raw = r.take(dim * 2)?;
        entries.push((id, raw.chunks_exact(2).map(|c| f16::from_bits(u16::from_le_bytes([c[0], c[1]]))).collect()));
    }
    Ok((model, dim, entries))
}

fn split(uri: &str) -> (&str, &str) {
    uri.rsplit_once('/').unwrap_or(("", uri))
}

/// Write `entries` as a new file in `location`; returns the file's URI.
pub async fn write(location: &str, model: &str, dim: usize, entries: &[Entry]) -> Result<String, String> {
    let bytes = encode(model, dim, entries)?;
    let name = new_name();
    let (store, base) = crate::ostore::located(location).await.map_err(|e| e.to_string())?;
    let _t = adb_core::steps::timer("vectors.file write");
    store.inner.put(&base.join(name.as_str()), bytes.into()).await.map_err(|e| format!("writing {location}/{name}: {e}"))?;
    Ok(format!("{location}/{name}"))
}

async fn read(uri: &str) -> Result<Option<Vec<u8>>, String> {
    let (dir, name) = split(uri);
    crate::ostore::get_opt(dir, name).await.map_err(|e| format!("reading {uri}: {e}"))
}

/// A partition's vectors, by row id, as its vector files hold them.
pub struct VectorMap {
    location: String,
    /// The model whose vectors rank, and their length; files of another model are
    /// skipped. Changed with the embedding setting ([`Self::follow`]).
    model: RwLock<(Option<String>, usize)>,
    vectors: RwLock<HashMap<String, Arc<[f16]>>>,
    /// Files loaded, by name.
    files: Mutex<BTreeSet<String>>,
    /// Files being loaded now.
    loading: AtomicUsize,
    /// How many times the model changed ([`Self::follow`]).
    switches: AtomicUsize,
}

impl VectorMap {
    pub fn new(location: String, model: Option<String>, dim: usize) -> Self {
        Self { location, model: RwLock::new((model, dim)), vectors: Default::default(), files: Default::default(), loading: AtomicUsize::new(0), switches: AtomicUsize::new(0) }
    }

    /// How many times the model changed: whoever acts on a change compares it.
    pub fn switches(&self) -> usize {
        self.switches.load(Ordering::Relaxed)
    }

    /// The model (and length) whose vectors this map holds.
    pub fn model(&self) -> (Option<String>, usize) {
        self.model.read().unwrap().clone()
    }

    /// The embedding setting now names `model` with `dim`-long vectors: when that's
    /// another model, the map drops what it holds and loads that model's files again,
    /// in the background (the backfill embeds what has none). Returns whether it switched.
    pub fn follow(self: &Arc<Self>, model: &str, dim: usize) -> bool {
        {
            let mut m = self.model.write().unwrap();
            if m.0.as_deref() == Some(model) && m.1 == dim {
                return false;
            }
            *m = (Some(model.to_string()), dim);
        }
        self.vectors.write().unwrap().clear();
        self.files.lock().unwrap().clear();
        self.switches.fetch_add(1, Ordering::Relaxed);
        tracing::info!(location = %self.location, model, dim, "embedding model changed: vector files loaded again");
        let me = Arc::clone(self);
        me.loading.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(e) = me.load_all().await {
                tracing::warn!(error = %e, "vector files not loaded after the model changed");
            }
            me.loading.fetch_sub(1, Ordering::Relaxed);
        });
        true
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn get(&self, row_id: &str) -> Option<Arc<[f16]>> {
        self.vectors.read().unwrap().get(row_id).cloned()
    }

    pub fn contains(&self, row_id: &str) -> bool {
        self.vectors.read().unwrap().contains_key(row_id)
    }

    /// Drop `row_id`'s vector (its text changed; the new one comes in a later file).
    pub fn remove(&self, row_id: &str) {
        self.vectors.write().unwrap().remove(row_id);
    }

    pub fn len(&self) -> usize {
        self.vectors.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Files being loaded now ([`Self::load_soon`]).
    pub fn loading(&self) -> usize {
        self.loading.load(Ordering::Relaxed)
    }

    pub fn files(&self) -> usize {
        self.files.lock().unwrap().len()
    }

    fn accept(&self, uri: &str, model: &str, dim: usize, entries: Vec<Entry>) {
        let (want, want_dim) = self.model();
        if want.as_deref().is_some_and(|m| m != model) || dim != want_dim {
            tracing::info!(%uri, model, dim, "vector file of another model or dimension: skipped (the backfill embeds again)");
            return;
        }
        let mut v = self.vectors.write().unwrap();
        for (id, x) in entries {
            v.insert(id, x.into());
        }
    }

    /// Vectors this node made and wrote itself (no read back).
    pub fn insert(&self, uri: &str, model: &str, entries: Vec<Entry>) {
        let name = split(uri).1.to_string();
        let dim = entries.first().map_or(self.model().1, |(_, v)| v.len());
        self.accept(uri, model, dim, entries);
        self.files.lock().unwrap().insert(name);
    }

    /// Load the file at `uri` once. A file gone (merged into a newer one) is skipped:
    /// the merged file holds its vectors.
    pub async fn load(&self, uri: &str) -> Result<(), String> {
        let name = split(uri).1.to_string();
        if self.files.lock().unwrap().contains(&name) {
            return Ok(());
        }
        let _t = adb_core::steps::timer("vectors.file load");
        let Some(bytes) = read(uri).await? else {
            tracing::debug!(%uri, "vector file gone (merged)");
            return Ok(());
        };
        let (model, dim, entries) = decode(&bytes).map_err(|e| format!("{uri}: {e}"))?;
        self.accept(uri, &model, dim, entries);
        self.files.lock().unwrap().insert(name);
        Ok(())
    }

    /// Load `uri` on its own task (an apply puts its rows meanwhile).
    pub fn load_soon(self: &Arc<Self>, uri: String) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        me.loading.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(e) = me.load(&uri).await {
                tracing::warn!(error = %e, "vector file not loaded; LIKE ranks those records on their words");
            }
            me.loading.fetch_sub(1, Ordering::Relaxed);
        })
    }

    async fn names(&self) -> Result<Vec<String>, String> {
        let (store, base) = crate::ostore::located(&self.location).await.map_err(|e| e.to_string())?;
        let mut names: Vec<String> = Vec::new();
        let mut list = store.inner.list(Some(&base));
        while let Some(m) = list.next().await {
            match m {
                Ok(m) => {
                    if let Some(n) = m.location.filename().filter(|n| n.ends_with(EXT)) {
                        names.push(n.to_string());
                    }
                }
                Err(object_store::Error::NotFound { .. }) => break,
                Err(e) => return Err(format!("listing {}: {e}", self.location)),
            }
        }
        names.sort();
        Ok(names)
    }

    /// Load every file in the folder, oldest first (a partition being opened).
    pub async fn load_all(&self) -> Result<usize, String> {
        let names = self.names().await?;
        async fn fetch(uri: String) -> (String, Result<Option<Vec<u8>>, String>) {
            let r = read(&uri).await;
            (uri, r)
        }
        let uris: Vec<String> = names.iter().map(|n| format!("{}/{n}", self.location)).collect();
        // Read in parallel, apply in name (time) order.
        let read: Vec<(String, Result<Option<Vec<u8>>, String>)> = futures::stream::iter(uris.into_iter().map(fetch)).buffered(16).collect().await;
        for ((uri, r), name) in read.into_iter().zip(&names) {
            if let Some(bytes) = r? {
                let (model, dim, entries) = decode(&bytes).map_err(|e| format!("{uri}: {e}"))?;
                self.accept(&uri, &model, dim, entries);
            }
            self.files.lock().unwrap().insert(name.clone());
        }
        Ok(names.len())
    }

    /// With [`MERGE_AT`] files or more, write the map as one file and delete the files
    /// it replaces (the owner's housekeeping). A file another writer adds meanwhile
    /// stays. Returns how many files were merged.
    pub async fn merge(&self) -> Result<usize, String> {
        let (Some(model), dim) = self.model() else { return Ok(0) };
        let merged: Vec<String> = self.files.lock().unwrap().iter().cloned().collect();
        if merged.len() < MERGE_AT {
            return Ok(0);
        }
        let entries: Vec<Entry> = self.vectors.read().unwrap().iter().map(|(k, v)| (k.clone(), v.to_vec())).collect();
        let _t = adb_core::steps::timer("vectors.merge");
        let uri = write(&self.location, &model, dim, &entries).await?;
        self.files.lock().unwrap().insert(split(&uri).1.to_string());
        let (store, base) = crate::ostore::located(&self.location).await.map_err(|e| e.to_string())?;
        for n in &merged {
            match store.inner.delete(&base.clone().join(n.as_str())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => tracing::warn!(error = %e, file = %n, "merged vector file not deleted"),
            }
            self.files.lock().unwrap().remove(n);
        }
        tracing::info!(files = merged.len(), vectors = entries.len(), "vector files merged");
        Ok(merged.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(x: f32, dim: usize) -> Vec<f16> {
        (0..dim).map(|i| f16::from_f32(x + i as f32)).collect()
    }

    #[test]
    fn a_file_round_trips() {
        let e = vec![("semantic:a".to_string(), v(1.0, 4)), ("episodic:b|~x".to_string(), v(-2.5, 4))];
        let (m, d, back) = decode(&encode("bge", 4, &e).unwrap()).unwrap();
        assert_eq!((m.as_str(), d), ("bge", 4));
        assert_eq!(back, e);
        assert!(decode(&encode("bge", 4, &e).unwrap()[..20]).is_err());
        assert!(encode("bge", 3, &e).is_err());
    }

    #[tokio::test]
    async fn files_load_merge_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let loc = location(None, dir.path(), PartitionId(7));
        let map = VectorMap::new(loc.clone(), Some("bge".into()), 4);
        let mut uris = Vec::new();
        for i in 0..MERGE_AT {
            uris.push(write(&loc, "bge", 4, &[(format!("semantic:k{i}"), v(i as f32, 4))]).await.unwrap());
        }
        // Another model's file is skipped.
        let other = write(&loc, "other", 4, &[("semantic:z".into(), v(9.0, 4))]).await.unwrap();
        for u in uris.iter().chain([&other]) {
            map.load(u).await.unwrap();
        }
        assert_eq!(map.len(), MERGE_AT);
        assert!(!map.contains("semantic:z"));
        // Newer file wins for the same id.
        let newer = write(&loc, "bge", 4, &[("semantic:k0".into(), v(100.0, 4))]).await.unwrap();
        map.load(&newer).await.unwrap();
        assert_eq!(map.get("semantic:k0").unwrap()[0].to_f32(), 100.0);

        assert_eq!(map.merge().await.unwrap(), MERGE_AT + 2);
        assert_eq!(map.files(), 1);
        // A file named in an old log record and merged away is skipped.
        map.load(&uris[3]).await.unwrap();

        let fresh = Arc::new(VectorMap::new(loc.clone(), Some("bge".into()), 4));
        assert_eq!(fresh.load_all().await.unwrap(), 1);
        assert_eq!(fresh.len(), MERGE_AT);
        assert_eq!(fresh.get("semantic:k0").unwrap()[0].to_f32(), 100.0);

        // Another model: its files rank; this one's wait for a switch back.
        write(&loc, "other", 4, &[("semantic:z".into(), v(9.0, 4))]).await.unwrap();
        assert!(fresh.follow("other", 4));
        while fresh.loading() > 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!((fresh.len(), fresh.contains("semantic:z")), (1, true));
        assert!(!fresh.follow("other", 4), "the same model again changes nothing");
    }
}

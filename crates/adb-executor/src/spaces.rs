//! Where another memory space's rows, tags and layers are read (B7 layers; S3
//! option B step 3).
//!
//! A space with layers reads other spaces (`a3t-docs`, `a3t-usage`, another agent).
//! On one node every space is in the one table and registry. In a cluster a layer
//! space usually lives in another partition: the partition router sets a
//! [`Spaces`] source on every partition engine ([`SpaceView::set_remote`]) that reads
//! each space from its own partition. The statement's own namespace is always local.
//!
//! The view holds the source weakly: the router owns the partition engines, and
//! they must not keep it alive.

use std::sync::{Arc, OnceLock, Weak};

use adb_core::log::Memory;
use adb_core::{AdbError, AdbResult};
use adb_store::{PartitionStore, StoredRow};
use async_trait::async_trait;

use crate::apply::ApplyBarrier;
use crate::branches::{Branches, Layer, Quota, Tag};

/// Other spaces, wherever they live.
#[async_trait]
pub trait Spaces: Send + Sync {
    /// Every version of `space`'s rows of `memory`, once its partition has applied
    /// what it committed.
    async fn versions(&self, memory: Memory, space: &str) -> AdbResult<Vec<StoredRow>>;
    async fn tag(&self, space: &str, name: &str) -> AdbResult<Option<Tag>>;
    async fn layers_of(&self, space: &str) -> AdbResult<Vec<Layer>>;
    /// AQL v0.13: what `space` was branched or forked from, if it was.
    async fn base_of(&self, space: &str) -> AdbResult<Option<crate::branches::Base>>;
    /// Every space (in any partition) reading `space` as a layer.
    async fn layered_by(&self, space: &str) -> AdbResult<Vec<String>>;
    /// The org's quotas (DB-4): in a cluster, kept once in the system partition.
    async fn quotas(&self) -> AdbResult<Vec<Quota>>;
    /// (records, payload bytes) of the namespaces `quota` covers in every partition
    /// but `except` — current usage, not reserved: parallel writes on other
    /// partitions can pass a pattern quota by about one write each (user, 14 Nov).
    async fn usage_elsewhere(&self, quota: &Quota, except: adb_core::log::PartitionId) -> AdbResult<(u64, u64)>;
}

/// One partition's own table and registry as a [`Spaces`] (also what a router asks
/// of each partition).
pub struct LocalSpaces {
    /// `None`: an engine without a table (no rows to read).
    pub store: Option<Arc<PartitionStore>>,
    pub barrier: Option<Arc<ApplyBarrier>>,
    pub registry: Arc<Branches>,
}

#[async_trait]
impl Spaces for LocalSpaces {
    async fn versions(&self, memory: Memory, space: &str) -> AdbResult<Vec<StoredRow>> {
        let Some(store) = &self.store else { return Ok(Vec::new()) };
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        store.versions(memory, Some(&format!("namespace = {}", adb_store::sql_str(space)))).await.map_err(|e| AdbError::Internal(format!("store: {e}")))
    }

    async fn tag(&self, space: &str, name: &str) -> AdbResult<Option<Tag>> {
        Ok(self.registry.tag(space, name))
    }

    async fn layers_of(&self, space: &str) -> AdbResult<Vec<Layer>> {
        Ok(self.registry.layers_of(space))
    }

    async fn base_of(&self, space: &str) -> AdbResult<Option<crate::branches::Base>> {
        Ok(self.registry.base_of(space))
    }

    async fn layered_by(&self, space: &str) -> AdbResult<Vec<String>> {
        Ok(self.registry.layered_by(space))
    }

    async fn quotas(&self) -> AdbResult<Vec<Quota>> {
        Ok(self.registry.quotas())
    }

    /// One partition: there is nowhere else.
    async fn usage_elsewhere(&self, _quota: &Quota, _except: adb_core::log::PartitionId) -> AdbResult<(u64, u64)> {
        Ok((0, 0))
    }
}

/// What a partition engine reads other spaces through: the router's source once set
/// (and alive), else its own table and registry.
#[derive(Clone)]
pub struct SpaceView {
    local: Arc<LocalSpaces>,
    remote: Arc<OnceLock<Weak<dyn Spaces>>>,
}

impl SpaceView {
    pub fn new(local: LocalSpaces) -> Self {
        Self { local: Arc::new(local), remote: Arc::new(OnceLock::new()) }
    }

    /// A view that reads every space through `spaces` (a router's own, while it lives).
    pub fn routed(spaces: &Arc<dyn Spaces>) -> Self {
        let v = Self::new(LocalSpaces { store: None, barrier: None, registry: Arc::new(Branches::default()) });
        v.set_remote(Arc::downgrade(spaces));
        v
    }

    /// Read other spaces through `spaces` from now on (set once, by the router).
    pub fn set_remote(&self, spaces: Weak<dyn Spaces>) {
        let _ = self.remote.set(spaces);
    }

    fn source(&self) -> Arc<dyn Spaces> {
        match self.remote.get().and_then(Weak::upgrade) {
            Some(r) => r,
            None => Arc::clone(&self.local) as Arc<dyn Spaces>,
        }
    }

    pub async fn versions(&self, memory: Memory, space: &str) -> AdbResult<Vec<StoredRow>> {
        self.source().versions(memory, space).await
    }

    pub async fn tag(&self, space: &str, name: &str) -> AdbResult<Option<Tag>> {
        self.source().tag(space, name).await
    }

    pub async fn layers_of(&self, space: &str) -> AdbResult<Vec<Layer>> {
        self.source().layers_of(space).await
    }

    pub async fn base_of(&self, space: &str) -> AdbResult<Option<crate::branches::Base>> {
        self.source().base_of(space).await
    }

    /// AQL v0.13: `space` as it was at `at` — its own records then, and, for a branch,
    /// its base's as they were at the branch point (or `at`, if earlier), one per key,
    /// with this space's ids. Forgotten-on-the-branch records (`_branch_deleted`) hide
    /// the base's. `in_scope` narrows to one named scope.
    pub async fn rows_at(&self, memory: adb_core::log::Memory, space: &str, at: adb_core::log::CommitTs, now: adb_core::log::CommitTs) -> AdbResult<Vec<StoredRow>> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        let mut level = space.to_string();
        let mut point = at;
        // Walk the chain of bases: own rows first, then each base's at its point.
        for _ in 0..16 {
            for mut r in self.versions(memory, &level).await? {
                if !r.is_current_at(point, now) {
                    continue;
                }
                let bare = adb_store::without_namespace(adb_store::key_of(&r.lineage_id)).to_string();
                if !seen.insert(bare.clone()) {
                    continue;
                }
                if crate::namespaces::branch_deleted(&r) {
                    continue;
                }
                if level != space {
                    let here = adb_store::row_id(memory, &adb_store::namespaced(Some(space), &bare));
                    if r.id == r.lineage_id {
                        r.id = here.clone();
                    }
                    r.lineage_id = here;
                }
                out.push(r);
            }
            match self.base_of(&level).await? {
                Some(b) if b.kind == crate::branches::BaseKind::Branch => {
                    point = point.min(b.point());
                    level = b.from;
                }
                _ => return Ok(out),
            }
        }
        Err(AdbError::Internal(format!("namespace {space:?}: its bases go more than 16 deep")))
    }

    pub async fn layered_by(&self, space: &str) -> AdbResult<Vec<String>> {
        self.source().layered_by(space).await
    }

    pub async fn quotas(&self) -> AdbResult<Vec<Quota>> {
        self.source().quotas().await
    }

    pub async fn usage_elsewhere(&self, quota: &Quota, except: adb_core::log::PartitionId) -> AdbResult<(u64, u64)> {
        self.source().usage_elsewhere(quota, except).await
    }

    /// The commit time a layer is read at: its tag's point for a pin or history, `None`
    /// (now) for a mount — and for a pin whose tag is gone.
    pub async fn layer_point(&self, l: &Layer) -> AdbResult<Option<adb_core::log::CommitTs>> {
        Ok(match l.mode {
            crate::branches::LayerMode::Mount => None,
            crate::branches::LayerMode::Pin | crate::branches::LayerMode::History => match &l.tag {
                Some(t) => self.tag(&l.space, t).await?.map(|t| adb_core::log::CommitTs(t.point_ts)),
                None => None,
            },
        })
    }
}

//! Where the executor's reads come from (DELIVERY_PLAN.md stage D3).
//!
//! - [`DirectReads`] — the in-memory backends (`adb-backends`).
//! - [`LanceReads`] — SEMANTIC and EPISODIC records, and every link, from the
//!   partition's Lance table (its current view, D2_TEMPORAL.md §3); other memory
//!   types from the backends (WORKING and TOOLS are the in-memory hot tier;
//!   PROCEDURAL moves later). Links are the table's edge rows (stage D4.1), so a
//!   forgotten record's links are gone at once, as D2 §5 says, not only after a
//!   restart.
//!
//! `LanceReads` rebuilds the records from the table and answers with the backend's
//! own query code (a throwaway backend filled with `insert_record`), so predicates,
//! ordering and modifiers behave exactly as before. The predicate is also pushed into
//! the Lance scan where it can be (`adb_store::predicate_filter`, stage D3.6), so a
//! read loads only a superset of the matching rows instead of the whole memory type.

use std::sync::Arc;
use std::time::Duration;

use adb_backends::{Adb, Backend, EpisodicBackend, SemanticBackend};
use adb_core::log::{CommitTs, Memory};
use adb_core::{time, AdbError, AdbResult, Link, MemoryRecord, MemoryType, Metadata, Modifiers, Predicate};
use adb_store::{predicate_filter, PartitionStore, StoredRow};
use datafusion::prelude::{col, lit};
use async_trait::async_trait;

use crate::apply::ApplyBarrier;

/// The executor's read operations.
#[async_trait]
pub trait Reads: Send + Sync {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>>;

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>>;

    async fn lookup(&self, memory: MemoryType, predicate: &Predicate) -> AdbResult<Vec<MemoryRecord>> {
        self.lookup_with_modifiers(memory, predicate, &Modifiers::default()).await
    }

    /// Links leaving record `from_id` (`WITH LINKS`, `FOLLOW LINKS`), oldest first.
    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>>;
}

/// Reads from the in-memory backends.
pub struct DirectReads(pub Arc<Adb>);

#[async_trait]
impl Reads for DirectReads {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        self.0.lookup_with_modifiers(memory, predicate, modifiers).await
    }

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        self.0.recall_with_modifiers(memory, predicate, modifiers).await
    }

    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>> {
        self.0.get_links_from(from_type, from_id, link_type).await
    }
}

/// A stored row as the backends hold it.
pub fn to_record(r: &StoredRow) -> MemoryRecord {
    let mut m = Metadata::new(r.data.visibility.into());
    m.created_at = r.commit_ts.to_datetime();
    m.accessed_at = r.valid_from.to_datetime();
    m.namespace = r.data.namespace.clone();
    m.ttl = r.data.ttl_at.map(|t| Duration::from_micros((t.0 - r.commit_ts.0).max(0) as u64));
    m.version = u64::from(r.version);
    MemoryRecord { id: r.key().to_string(), memory_type: r.data.memory.into(), data: r.data.value.clone(), metadata: m }
}

/// An edge row as a link: ends back to their record ids, `created_at` its commit.
pub fn to_link(r: &StoredRow) -> Option<Link> {
    let d = &r.data;
    let to: Memory = d.value.get("to_memory").cloned().and_then(|v| serde_json::from_value(v).ok())?;
    Some(Link {
        id: r.key().to_string(),
        from_type: d.memory.into(),
        from_id: adb_store::key_of(d.subject_id.as_deref()?).to_string(),
        to_type: to.into(),
        to_id: adb_store::key_of(d.object_id.as_deref()?).to_string(),
        link_type: d.link_type.clone().unwrap_or_default(),
        weight: d.weight.unwrap_or(1.0),
        created_at: r.commit_ts.to_datetime(),
        updated_at: r.valid_from.to_datetime(),
    })
}

/// SEMANTIC and EPISODIC records and every link from Lance, the rest from the
/// backends.
pub struct LanceReads {
    adb: Arc<Adb>,
    store: Arc<PartitionStore>,
    /// Waits for the background applier, so reads see every write that returned
    /// before them (stage D3.7). `None` = the table is written synchronously.
    barrier: Option<Arc<ApplyBarrier>>,
}

enum View {
    Semantic(SemanticBackend),
    Episodic(EpisodicBackend),
}

impl LanceReads {
    pub fn new(adb: Arc<Adb>, store: Arc<PartitionStore>, barrier: Option<Arc<ApplyBarrier>>) -> Self {
        Self { adb, store, barrier }
    }

    /// The memory types served from Lance.
    pub fn serves(memory: MemoryType) -> bool {
        matches!(memory, MemoryType::Semantic | MemoryType::Episodic)
    }

    /// A throwaway backend holding the memory type's current rows that may match
    /// `predicate`.
    async fn view(&self, memory: MemoryType, predicate: &Predicate) -> AdbResult<View> {
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        let m: Memory = memory.into();
        let mut filter = col("memory").eq(lit(m.tag())).and(col("kind").not_eq(lit("edge")));
        if let Some(p) = predicate_filter(m, predicate) {
            filter = filter.and(p);
        }
        let now = CommitTs::from_datetime(time::now());
        let rows = self
            .store
            .current_expr(Some(filter), now)
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        Ok(match memory {
            MemoryType::Semantic => {
                let b = SemanticBackend::new()?;
                rows.iter().for_each(|r| b.insert_record(to_record(r)));
                View::Semantic(b)
            }
            _ => {
                let b = EpisodicBackend::new();
                rows.iter().for_each(|r| b.insert_record(to_record(r)));
                View::Episodic(b)
            }
        })
    }
}

#[async_trait]
impl Reads for LanceReads {
    async fn lookup_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        if !Self::serves(memory) {
            return self.adb.lookup_with_modifiers(memory, predicate, modifiers).await;
        }
        match self.view(memory, predicate).await? {
            View::Semantic(b) => b.lookup(predicate, modifiers).await,
            View::Episodic(b) => b.lookup(predicate, modifiers).await,
        }
    }

    async fn links_from(&self, from_type: MemoryType, from_id: &str, link_type: Option<&str>) -> AdbResult<Vec<Link>> {
        if let Some(b) = &self.barrier {
            b.wait_for_committed().await;
        }
        let now = CommitTs::from_datetime(time::now());
        let rows = self
            .store
            .edges_from(from_type.into(), from_id, link_type, now)
            .await
            .map_err(|e| AdbError::Internal(format!("store: {e}")))?;
        Ok(rows.iter().filter_map(to_link).collect())
    }

    async fn recall_with_modifiers(&self, memory: MemoryType, predicate: &Predicate, modifiers: &Modifiers) -> AdbResult<Vec<MemoryRecord>> {
        if !Self::serves(memory) {
            return self.adb.recall_with_modifiers(memory, predicate, modifiers).await;
        }
        match self.view(memory, predicate).await? {
            View::Semantic(b) => b.recall(predicate, modifiers).await,
            View::Episodic(b) => b.recall(predicate, modifiers).await,
        }
    }
}

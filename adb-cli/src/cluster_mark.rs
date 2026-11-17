//! Which cluster a node's disk and its object storage belong to.
//!
//! Object storage holds the cluster's mark (`{ADB_COLD_URI}/cluster.json`: an id, the
//! partition count); the first voter writes it when there is none. Every node keeps
//! a copy on its disk (`{dir}/cluster.json`) and, before it starts Raft or opens a
//! partition, checks the two agree. A disk from another incarnation of the cluster (a
//! volume kept while object storage was emptied, or a node pointed at another
//! cluster's storage) would bring that incarnation's control log and WAL tail into
//! this one: such a node refuses to start and says why.
//!
//! A cluster started again from object storage alone (every disk new) takes the mark
//! it finds: that is a restore.

use std::path::Path;

use adb_cluster::map::Partition;
use serde::{Deserialize, Serialize};

const NAME: &str = "cluster.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    pub id: String,
    pub partitions: Partition,
    /// When the first voter wrote it (RFC 3339).
    pub created: String,
}

fn parse(bytes: &[u8], what: &str) -> Result<Mark, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("{what}: not a cluster mark: {e}"))
}

fn read_local(dir: &Path) -> Result<Option<Mark>, String> {
    let path = dir.join(NAME);
    match std::fs::read(&path) {
        Ok(b) => parse(&b, &path.display().to_string()).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

fn write_local(dir: &Path, mark: &Mark) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let tmp = dir.join(format!("{NAME}.tmp"));
    let bytes = serde_json::to_vec_pretty(mark).map_err(|e| e.to_string())?;
    let f = || -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, dir.join(NAME))?;
        std::fs::File::open(dir)?.sync_all()
    };
    f().map_err(|e| format!("{}: {e}", dir.join(NAME).display()))
}

async fn read_store(cold: &str) -> Result<Option<Mark>, String> {
    let bytes = adb_store::ostore::get_opt(cold, NAME).await.map_err(|e| format!("object storage ({cold}/{NAME}): {e}"))?;
    bytes.map(|b| parse(&b, &format!("{cold}/{NAME}"))).transpose()
}

/// The cluster this node belongs to: the mark on object storage, checked against the
/// copy on `dir` and `partitions` (`ADB_PARTITIONS`), and kept on `dir`. `first`: this
/// node is the lowest voter, which writes the mark when object storage has none.
///
/// `None`: object storage has no mark yet and this node does not write it. The node
/// starts unmarked and takes the mark at its next start: a cluster upgraded node by
/// node, the first voter last, keeps its quorum (waiting here took the others down).
pub async fn settle(dir: &Path, cold: &str, first: bool, partitions: Partition) -> Result<Option<Mark>, String> {
    let local = read_local(dir)?;
    let mark = loop {
        match (&local, read_store(cold).await?) {
            (Some(l), Some(s)) if l.id == s.id => break s,
            (Some(l), Some(s)) => {
                return Err(format!(
                    "this node's disk ({}) belongs to ADB cluster {}, but object storage ({cold}) holds cluster {}: clear this node's disk (its volume) to join {}, or point ADB_COLD_URI back at {}'s storage",
                    dir.display(),
                    l.id,
                    s.id,
                    s.id,
                    l.id
                ))
            }
            (Some(l), None) => {
                return Err(format!(
                    "this node's disk ({}) belongs to ADB cluster {}, but object storage ({cold}) holds no cluster: it was emptied, or ADB_COLD_URI points elsewhere. Point it back at {}'s storage, or clear every node's disk to start a new cluster",
                    dir.display(),
                    l.id,
                    l.id
                ))
            }
            (None, Some(s)) => break s,
            (None, None) if first => {
                let new = Mark { id: uuid::Uuid::new_v4().to_string(), partitions, created: chrono::Utc::now().to_rfc3339() };
                adb_store::ostore::put_if_absent(cold, NAME, serde_json::to_vec_pretty(&new).map_err(|e| e.to_string())?).await.map_err(|e| format!("object storage ({cold}/{NAME}): {e}"))?;
                // Read back: a racing writer's mark, if it won, is the cluster's.
                continue;
            }
            (None, None) => {
                tracing::warn!(%cold, "object storage holds no cluster mark yet (the first voter writes it): starting unmarked");
                return Ok(None);
            }
        }
    };
    if mark.partitions != partitions {
        return Err(format!("ADB cluster {} has {} partitions (fixed for its life), but ADB_PARTITIONS asks {partitions}", mark.id, mark.partitions));
    }
    if local.is_none() {
        write_local(dir, &mark)?;
        tracing::info!(cluster = %mark.id, "this node joins the cluster object storage names");
    }
    Ok(Some(mark))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cold(d: &tempfile::TempDir) -> String {
        format!("file://{}", d.path().display())
    }

    #[tokio::test]
    async fn the_first_voter_writes_the_mark_and_the_others_take_it() {
        let store = tempfile::tempdir().unwrap();
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        // Before the first voter: unmarked, nothing kept.
        assert_eq!(settle(b.path(), &cold(&store), false, 8).await.unwrap(), None);
        assert!(!b.path().join(NAME).exists());
        let m1 = settle(a.path(), &cold(&store), true, 8).await.unwrap();
        let m2 = settle(b.path(), &cold(&store), false, 8).await.unwrap();
        assert!(m1.is_some());
        assert_eq!(m1, m2);
        // Started again: the same.
        assert_eq!(settle(a.path(), &cold(&store), true, 8).await.unwrap(), m1);
    }

    #[tokio::test]
    async fn a_disk_of_another_incarnation_is_refused() {
        let (old, new) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let disk = tempfile::tempdir().unwrap();
        settle(disk.path(), &cold(&old), true, 8).await.unwrap();
        // A new cluster on other storage; this disk is pointed at it.
        settle(tempfile::tempdir().unwrap().path(), &cold(&new), true, 8).await.unwrap();
        let e = settle(disk.path(), &cold(&new), false, 8).await.unwrap_err();
        assert!(e.contains("belongs to ADB cluster") && e.contains("clear this node's disk"), "{e}");
        // Object storage emptied under it.
        let empty = tempfile::tempdir().unwrap();
        let e = settle(disk.path(), &cold(&empty), true, 8).await.unwrap_err();
        assert!(e.contains("holds no cluster"), "{e}");
    }

    #[tokio::test]
    async fn new_disks_on_marked_storage_restore_and_the_partition_count_is_fixed() {
        let store = tempfile::tempdir().unwrap();
        let m = settle(tempfile::tempdir().unwrap().path(), &cold(&store), true, 8).await.unwrap();
        // Every disk new (a restore): the mark is taken, whoever starts first.
        assert_eq!(settle(tempfile::tempdir().unwrap().path(), &cold(&store), true, 8).await.unwrap(), m);
        let e = settle(tempfile::tempdir().unwrap().path(), &cold(&store), true, 16).await.unwrap_err();
        assert!(e.contains("partitions"), "{e}");
    }
}

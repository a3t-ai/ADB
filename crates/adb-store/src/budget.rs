//! A node's compaction budget (15 Nov): compactions ran whenever a table came due, on
//! the threads serving requests — 34 folds in 10 s on one node under load (Hetzner
//! round 5), every request slower. Now:
//!
//! - **At most `ADB_COMPACTIONS` folds at once** on the node (default 2), whatever
//!   table they belong to; a fold waits for a permit before it starts its work.
//! - **Replica copies take one of their own first** (`ADB_REPLICA_COMPACTIONS`,
//!   default 1): a cache of the owner never holds every permit, so owners go first.
//! - **On their own threads, at a lower priority**: [`run`] puts a compaction on a
//!   small runtime (`ADB_COMPACTION_THREADS`, default 2, nice 10), so what it spawns
//!   doesn't take the request threads. Lance's CPU pool is shared with queries; the
//!   permits bound how much of it compactions use.

use std::future::Future;
use std::sync::OnceLock;

use tokio::sync::{Semaphore, SemaphorePermit};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).filter(|&n: &usize| n > 0).unwrap_or(default)
}

fn permits() -> &'static Semaphore {
    static P: OnceLock<Semaphore> = OnceLock::new();
    P.get_or_init(|| Semaphore::new(env_usize("ADB_COMPACTIONS", 2)))
}

fn replica_permits() -> &'static Semaphore {
    static P: OnceLock<Semaphore> = OnceLock::new();
    P.get_or_init(|| Semaphore::new(env_usize("ADB_REPLICA_COMPACTIONS", 1)))
}

/// Held for a fold's whole run.
pub struct Permit {
    _node: SemaphorePermit<'static>,
    _replica: Option<SemaphorePermit<'static>>,
}

/// Wait for the right to fold: a replica copy's own permit first, then the node's.
pub async fn acquire(replica: bool) -> Permit {
    let t = std::time::Instant::now();
    let replica = match replica {
        true => Some(replica_permits().acquire().await.expect("never closed")),
        false => None,
    };
    let node = permits().acquire().await.expect("never closed");
    adb_core::steps::add("compaction budget wait", t.elapsed());
    Permit { _node: node, _replica: replica }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(env_usize("ADB_COMPACTION_THREADS", 2))
            .thread_name("adb-compact")
            .on_thread_start(|| adb_core::disk::lower_thread_priority(10))
            .enable_all()
            .build()
            .expect("the compaction runtime")
    })
}

/// Run `work` (a compaction) on the compaction runtime and wait for it here.
pub async fn run<F>(work: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match runtime().spawn(work).await {
        Ok(v) => v,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn at_most_the_budget_at_once_and_one_replica_copy() {
        // Defaults: 2 folds on the node, 1 of them a replica copy's.
        let (now, max, replicas_now, replicas_max) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut tasks = Vec::new();
        for k in 0..12 {
            let (now, max, rn, rm) = (Arc::clone(&now), Arc::clone(&max), Arc::clone(&replicas_now), Arc::clone(&replicas_max));
            tasks.push(tokio::spawn(run(async move {
                let replica = k % 2 == 0;
                let _p = acquire(replica).await;
                max.fetch_max(now.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                if replica {
                    rm.fetch_max(rn.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                if replica {
                    rn.fetch_sub(1, Ordering::SeqCst);
                }
                now.fetch_sub(1, Ordering::SeqCst);
            })));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(max.load(Ordering::SeqCst), 2, "the node's budget");
        assert_eq!(replicas_max.load(Ordering::SeqCst), 1, "one replica copy at a time");
    }
}

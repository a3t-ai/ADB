//! jemalloc as the allocator. glibc keeps what a thread frees in that thread's arena and
//! rarely returns it, so a peak on any worker stays in the process. jemalloc returns unused
//! pages from a background thread, so nothing has to trim on a timer.
//!
//! - `background_thread`: purging runs on its own thread, not only inside allocations.
//! - `dirty_decay_ms` / `muzzy_decay_ms`: how soon unused pages go back to the system.
//! - `narenas`: fixed, because the default follows the node's CPUs, not the pod's limit.
//!
//! `MALLOC_CONF` overrides these at run time, for example to turn on heap profiling
//! (`prof:true,prof_prefix:/tmp/heap`).

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Read by jemalloc at startup. Must end in a NUL.
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8; 70] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0,narenas:8\0";

/// jemalloc's own counters, in bytes. `allocated` is what the program holds; `active`
/// adds the rest of the pages those allocations sit on; `resident` is what jemalloc
/// has in RAM, its own metadata included; `retained` is address space kept but already
/// returned to the system, so it costs no RAM.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub metadata: usize,
    pub retained: usize,
}

fn read(name: &[u8]) -> usize {
    let mut v: usize = 0;
    let mut len = std::mem::size_of::<usize>();
    // SAFETY: `name` ends in a NUL, and these keys hold a size_t.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(name.as_ptr() as *const _, &mut v as *mut usize as *mut _, &mut len, std::ptr::null_mut(), 0)
    };
    if rc == 0 { v } else { 0 }
}

/// The counters as of now. jemalloc caches them until its epoch is advanced.
pub fn stats() -> Stats {
    let mut epoch: u64 = 1;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: "epoch" takes and returns a uint64_t.
    unsafe {
        tikv_jemalloc_sys::mallctl(b"epoch\0".as_ptr() as *const _, &mut epoch as *mut u64 as *mut _, &mut len, &mut epoch as *mut u64 as *mut _, len);
    }
    Stats {
        allocated: read(b"stats.allocated\0"),
        active: read(b"stats.active\0"),
        resident: read(b"stats.resident\0"),
        metadata: read(b"stats.metadata\0"),
        retained: read(b"stats.retained\0"),
    }
}

/// `ADB_ALLOC_STATS_SECS` (default 60; 0 turns the log line off).
pub fn report_interval() -> u64 {
    std::env::var("ADB_ALLOC_STATS_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60)
}

/// Logs the counters at info on target `adb::alloc`, so heap can be watched in a pod's logs.
pub fn report_every(secs: u64) {
    if secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        loop {
            tick.tick().await;
            let s = stats();
            let mib = |b: usize| b >> 20;
            tracing::info!(target: "adb::alloc", allocated_mib = mib(s.allocated), active_mib = mib(s.active), resident_mib = mib(s.resident), metadata_mib = mib(s.metadata), retained_mib = mib(s.retained), "allocator");
        }
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn counters_follow_a_large_allocation() {
        let before = super::stats().allocated;
        let held = vec![1u8; 64 << 20];
        let during = super::stats().allocated;
        assert!(during >= before + (60 << 20), "before {before}, during {during}");
        drop(held);
        let s = super::stats();
        assert!(s.resident >= s.allocated && s.active >= s.allocated);
    }
}

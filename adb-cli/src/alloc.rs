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

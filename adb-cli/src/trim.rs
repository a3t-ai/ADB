//! Hand free heap back to the system on a timer (leader memory, CA-53). glibc keeps what a
//! thread freed in that thread's arena, so concurrent large answers leave each arena at its
//! high-water mark for good. `ADB_MALLOC_TRIM_SECS` sets the period; 0 turns it off.

use std::time::{Duration, Instant};

extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

/// Heap in use by this process (`RssAnon`), in MiB.
fn anon_mib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("RssAnon")).and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()))
        .unwrap_or(0)
        / 1024
}

pub fn start() {
    let secs: u64 = std::env::var("ADB_MALLOC_TRIM_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    if secs == 0 {
        return;
    }
    std::thread::Builder::new()
        .name("malloc-trim".into())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_secs(secs));
            let (before, t0) = (anon_mib(), Instant::now());
            // SAFETY: glibc's own call, safe from any thread at any time.
            unsafe { malloc_trim(0) };
            tracing::info!(before_mib = before, after_mib = anon_mib(), took_ms = t0.elapsed().as_millis() as u64, "malloc_trim");
        })
        .expect("starting the trim thread");
}

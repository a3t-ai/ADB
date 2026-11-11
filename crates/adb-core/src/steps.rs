//! Where a write's time goes, step by step (`ADB_STEP_STATS=1`): each step adds its
//! duration to a counter, and every 10 s one log line per step gives count, mean and
//! max, then the counters reset. Off by default; on, it costs a lock per step.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Default, Clone, Copy)]
struct Acc {
    n: u64,
    sum_us: u64,
    max_us: u64,
}

struct Stats {
    steps: Mutex<BTreeMap<&'static str, Acc>>,
    values: Mutex<BTreeMap<&'static str, Acc>>,
}

fn stats() -> Option<&'static Stats> {
    static S: OnceLock<Option<Stats>> = OnceLock::new();
    S.get_or_init(|| {
        let on = std::env::var("ADB_STEP_STATS").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("on"));
        if !on {
            return None;
        }
        // Reported from a thread of its own, so a stalled runtime still reports.
        std::thread::Builder::new()
            .name("adb-step-stats".into())
            .spawn(|| loop {
                std::thread::sleep(Duration::from_secs(10));
                report();
            })
            .ok()?;
        tracing::info!("write step timings on (ADB_STEP_STATS): one line per step every 10 s");
        Some(Stats { steps: Mutex::new(BTreeMap::new()), values: Mutex::new(BTreeMap::new()) })
    })
    .as_ref()
}

/// Whether step timings are kept.
pub fn on() -> bool {
    stats().is_some()
}

/// Add `d` to `step`.
pub fn add(step: &'static str, d: Duration) {
    let Some(s) = stats() else { return };
    let us = d.as_micros() as u64;
    let mut m = s.steps.lock().unwrap();
    let a = m.entry(step).or_default();
    a.n += 1;
    a.sum_us += us;
    a.max_us = a.max_us.max(us);
}

/// A size or count (batch sizes, queue depths): mean and max per 10 s.
pub fn value(name: &'static str, v: u64) {
    let Some(s) = stats() else { return };
    let mut m = s.values.lock().unwrap();
    let a = m.entry(name).or_default();
    a.n += 1;
    a.sum_us += v;
    a.max_us = a.max_us.max(v);
}

/// Times a scope into `step` when dropped.
pub struct Timer(&'static str, Instant);

impl Drop for Timer {
    fn drop(&mut self) {
        add(self.0, self.1.elapsed());
    }
}

/// Start a [`Timer`] for `step`.
pub fn timer(step: &'static str) -> Timer {
    Timer(step, Instant::now())
}

/// How late the tokio runtime wakes a 20 ms sleep: a busy or blocked runtime shows
/// here before anywhere else. Call once from inside the runtime.
pub fn spawn_lag_probe() {
    if !on() {
        return;
    }
    tokio::spawn(async {
        loop {
            let t0 = Instant::now();
            tokio::time::sleep(Duration::from_millis(20)).await;
            add("runtime.lag (20 ms sleep overshoot)", t0.elapsed().saturating_sub(Duration::from_millis(20)));
        }
    });
    let m = tokio::runtime::Handle::current().metrics();
    std::thread::Builder::new()
        .name("adb-rt-stats".into())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
            value("runtime.workers", m.num_workers() as u64);
            value("runtime.alive tasks", m.num_alive_tasks() as u64);
            value("runtime.global queue depth", m.global_queue_depth() as u64);
        })
        .ok();
}

/// Time since `t0` into `step`, and a new start for the next step.
pub fn lap(step: &'static str, t0: &mut Instant) {
    if on() {
        let now = Instant::now();
        add(step, now - *t0);
        *t0 = now;
    }
}

fn report() {
    let Some(s) = stats() else { return };
    let taken = std::mem::take(&mut *s.steps.lock().unwrap());
    for (step, a) in taken {
        let mean_ms = a.sum_us as f64 / a.n.max(1) as f64 / 1000.0;
        // busy = how many requests were in this step on average (sum / 10 s).
        let busy = a.sum_us as f64 / 10_000_000.0;
        tracing::info!(target: "adb::steps", step, n = a.n, per_s = a.n / 10, mean_ms = format!("{mean_ms:.2}"), max_ms = format!("{:.1}", a.max_us as f64 / 1000.0), in_step = format!("{busy:.1}"), "step");
    }
    let values = std::mem::take(&mut *s.values.lock().unwrap());
    for (name, a) in values {
        tracing::info!(target: "adb::steps", value = name, n = a.n, mean = format!("{:.1}", a.sum_us as f64 / a.n.max(1) as f64), max = a.max_us, "value");
    }
}

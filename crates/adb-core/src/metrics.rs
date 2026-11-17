//! Process-wide metrics (OB-1, `.ai/OBSERVABILITY.md`): one Prometheus registry, served
//! as text at `GET /metrics` and as JSON (with percentiles) at `GET /admin/stats/metrics`.
//!
//! Labels are bounded: transports, operations and statements come from fixed sets
//! (anything else is `other`), never from user input. Query text is logged only as a
//! fingerprint, literals replaced by `?`.

use std::sync::OnceLock;
use std::time::Duration;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

/// Latency buckets, seconds: 0.5 ms to 10 s.
const SECONDS: &[f64] = &[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
/// Row-count buckets.
const ROWS: &[f64] = &[0.0, 1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0];

pub struct Metrics {
    pub registry: Registry,
    /// Requests by transport (http, flight), operation and status (ok, error).
    pub requests: IntCounterVec,
    pub request_seconds: HistogramVec,
    pub requests_in_flight: IntGaugeVec,
    /// Connections by transport: open now, and accepted since start.
    pub connections_open: IntGaugeVec,
    pub connections: IntCounterVec,
    /// Refused credentials by transport and reason.
    pub auth_failures: IntCounterVec,
    /// AQL statements by statement, memory type and status.
    pub queries: IntCounterVec,
    pub query_seconds: HistogramVec,
    pub query_rows: HistogramVec,
    pub slow_queries: IntCounterVec,
}

fn counter(r: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("metric");
    r.register(Box::new(c.clone())).expect("registered once");
    c
}

fn gauge(r: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("metric");
    r.register(Box::new(g.clone())).expect("registered once");
    g
}

fn histogram(r: &Registry, name: &str, help: &str, labels: &[&str], buckets: &[f64]) -> HistogramVec {
    let h = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets.to_vec()), labels).expect("metric");
    r.register(Box::new(h.clone())).expect("registered once");
    h
}

/// The process's metrics.
pub fn get() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(|| {
        let r = Registry::new_custom(Some("adb".into()), None).expect("registry");
        Metrics {
            requests: counter(&r, "requests_total", "Requests by transport, operation and status", &["transport", "op", "status"]),
            request_seconds: histogram(&r, "request_duration_seconds", "Request duration by transport and operation", &["transport", "op"], SECONDS),
            requests_in_flight: gauge(&r, "requests_in_flight", "Requests being served, by transport", &["transport"]),
            connections_open: gauge(&r, "connections_open", "Open connections by transport", &["transport"]),
            connections: counter(&r, "connections_total", "Connections accepted since start, by transport", &["transport"]),
            auth_failures: counter(&r, "auth_failures_total", "Refused credentials by transport and reason", &["transport", "reason"]),
            queries: counter(&r, "queries_total", "AQL statements by statement, memory type and status", &["statement", "memory", "status"]),
            query_seconds: histogram(&r, "query_duration_seconds", "AQL statement duration by statement and memory type", &["statement", "memory"], SECONDS),
            query_rows: histogram(&r, "query_rows", "Rows an AQL read returned, by statement", &["statement"], ROWS),
            slow_queries: counter(&r, "slow_queries_total", "AQL statements slower than ADB_SLOW_QUERY_MS, by statement", &["statement"]),
            registry: r,
        }
    })
}

/// Every metric in Prometheus' text format.
pub fn render() -> String {
    let mut out = Vec::new();
    let _ = TextEncoder::new().encode(&get().registry.gather(), &mut out);
    String::from_utf8(out).unwrap_or_default()
}

/// Every metric as JSON: counters and gauges by labels; histograms with count, sum,
/// mean and p50 / p90 / p99 estimated from their buckets (in the metric's unit).
pub fn snapshot() -> serde_json::Value {
    use prometheus::proto::MetricType;
    let mut out = serde_json::Map::new();
    for family in get().registry.gather() {
        let mut series = Vec::new();
        for m in family.get_metric() {
            let labels: serde_json::Map<String, serde_json::Value> =
                m.get_label().iter().map(|l| (l.get_name().to_string(), l.get_value().into())).collect();
            let value = match family.get_field_type() {
                MetricType::COUNTER => serde_json::json!(m.get_counter().get_value()),
                MetricType::GAUGE => serde_json::json!(m.get_gauge().get_value()),
                MetricType::HISTOGRAM => {
                    let h = m.get_histogram();
                    let count = h.get_sample_count();
                    let buckets: Vec<(f64, u64)> = h.get_bucket().iter().map(|b| (b.get_upper_bound(), b.get_cumulative_count())).collect();
                    let q = |p: f64| quantile(&buckets, count, p);
                    serde_json::json!({
                        "count": count,
                        "sum": h.get_sample_sum(),
                        "mean": if count > 0 { h.get_sample_sum() / count as f64 } else { 0.0 },
                        "p50": q(0.5), "p90": q(0.9), "p99": q(0.99),
                    })
                }
                _ => continue,
            };
            series.push(serde_json::json!({ "labels": labels, "value": value }));
        }
        out.insert(family.get_name().to_string(), serde_json::Value::Array(series));
    }
    serde_json::Value::Object(out)
}

/// The `p` quantile from cumulative buckets, linearly within the bucket it falls in.
fn quantile(buckets: &[(f64, u64)], count: u64, p: f64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    let rank = p * count as f64;
    let (mut lower, mut below) = (0.0, 0u64);
    for &(upper, cum) in buckets {
        if cum as f64 >= rank {
            let inside = (cum - below) as f64;
            let frac = if inside > 0.0 { (rank - below as f64) / inside } else { 1.0 };
            return lower + (upper - lower) * frac.clamp(0.0, 1.0);
        }
        lower = upper;
        below = cum;
    }
    lower
}

/// A request in flight on `transport`; on drop it counts as done (with its status
/// unless [`InFlight::done`] gave one: a dropped request counts as `cancelled`).
pub struct InFlight {
    transport: &'static str,
    op: String,
    started: std::time::Instant,
    status: Option<&'static str>,
}

impl InFlight {
    pub fn start(transport: &'static str, op: impl Into<String>) -> Self {
        get().requests_in_flight.with_label_values(&[transport]).inc();
        Self { transport, op: op.into(), started: std::time::Instant::now(), status: None }
    }

    pub fn done(mut self, ok: bool) {
        self.status = Some(if ok { "ok" } else { "error" });
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let m = get();
        m.requests_in_flight.with_label_values(&[self.transport]).dec();
        let status = self.status.unwrap_or("cancelled");
        m.requests.with_label_values(&[self.transport, &self.op, status]).inc();
        m.request_seconds.with_label_values(&[self.transport, &self.op]).observe(self.started.elapsed().as_secs_f64());
    }
}

/// An open connection on `transport`, counted while it lives.
pub struct Connection(&'static str);

impl Connection {
    pub fn open(transport: &'static str) -> Self {
        let m = get();
        m.connections.with_label_values(&[transport]).inc();
        m.connections_open.with_label_values(&[transport]).inc();
        Self(transport)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        get().connections_open.with_label_values(&[self.0]).dec();
    }
}

const STATEMENTS: &[&str] = &[
    "RECALL", "LOOKUP", "SCAN", "STORE", "UPDATE", "FORGET", "LINK", "UNLINK", "MERGE", "WATCH", "UNWATCH", "SUBSCRIBE",
    "UNSUBSCRIBE", "PUBLISH", "ACK", "PIPELINE", "REFLECT", "LOAD", "BEGIN", "COMMIT", "ROLLBACK",
];
const MEMORIES: &[&str] = &["WORKING", "EPISODIC", "SEMANTIC", "PROCEDURAL", "TOOLS"];

/// The statement keyword and memory type of `aql`, from fixed sets (`other`, `-`).
pub fn statement_of(aql: &str) -> (&'static str, &'static str) {
    let mut words = aql.split_whitespace().filter(|w| !w.starts_with("--"));
    let first = words.next().unwrap_or("").to_ascii_uppercase();
    let statement = STATEMENTS.iter().find(|s| **s == first).copied().unwrap_or("other");
    let mut memory = "-";
    let mut prev = String::new();
    for w in aql.split_whitespace() {
        let up = w.trim_matches(|c: char| !c.is_ascii_alphanumeric()).to_ascii_uppercase();
        if prev == "FROM" || prev == "INTO" {
            if let Some(m) = MEMORIES.iter().find(|m| **m == up) {
                memory = m;
                break;
            }
        }
        prev = up;
    }
    (statement, memory)
}

/// `aql` with every literal replaced by `?` (strings in single or double quotes,
/// numbers), whitespace collapsed, at most 300 characters: safe to log.
pub fn fingerprint(aql: &str) -> String {
    let mut out = String::with_capacity(aql.len().min(300));
    let mut chars = aql.chars().peekable();
    let mut prev_word = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' => {
                // Skip to the closing quote (a doubled quote stays inside).
                while let Some(d) = chars.next() {
                    if d == c {
                        if chars.peek() == Some(&c) {
                            chars.next();
                            continue;
                        }
                        break;
                    }
                }
                out.push('?');
                prev_word = false;
            }
            d if d.is_ascii_digit() && !prev_word => {
                while chars.peek().is_some_and(|x| x.is_ascii_digit() || *x == '.') {
                    chars.next();
                }
                out.push('?');
                prev_word = false;
            }
            w if w.is_whitespace() => {
                if !out.ends_with(' ') && !out.is_empty() {
                    out.push(' ');
                }
                prev_word = false;
            }
            other => {
                out.push(other);
                prev_word = other.is_alphanumeric() || other == '_';
            }
        }
        if out.len() >= 300 {
            out.push('…');
            break;
        }
    }
    out.trim().to_string()
}

/// Slow-query threshold (`ADB_SLOW_QUERY_MS`, default 250 ms).
pub fn slow_query() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| Duration::from_millis(std::env::var("ADB_SLOW_QUERY_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(250)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_and_memory_types_come_from_fixed_sets() {
        assert_eq!(statement_of("SCAN FROM WORKING limit 10 ORDER BY today desc"), ("SCAN", "WORKING"));
        assert_eq!(statement_of("store into semantic (key = \"k\")"), ("STORE", "SEMANTIC"));
        assert_eq!(statement_of("DROP TABLE x"), ("other", "-"));
        assert_eq!(statement_of("RECALL FROM \"; DROP\""), ("RECALL", "-"));
    }

    #[test]
    fn fingerprints_carry_no_literals() {
        assert_eq!(fingerprint(r#"STORE INTO WORKING (name = "bob", score = 82, active = true)"#), "STORE INTO WORKING (name = ?, score = ?, active = true)");
        assert_eq!(fingerprint("RECALL FROM SEMANTIC WHERE key = 'it''s' LIMIT 10"), "RECALL FROM SEMANTIC WHERE key = ? LIMIT ?");
        assert_eq!(fingerprint("LOOKUP k2  FROM\nEPISODIC"), "LOOKUP k2 FROM EPISODIC");
    }

    #[test]
    fn quantiles_interpolate_within_buckets() {
        let b = [(1.0, 50), (2.0, 100)];
        assert_eq!(quantile(&b, 100, 0.5), 1.0);
        assert!((quantile(&b, 100, 0.75) - 1.5).abs() < 1e-9);
    }
}

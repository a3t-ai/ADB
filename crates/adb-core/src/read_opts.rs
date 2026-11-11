//! How fresh a follower read must be (PF-46), set per request by the HTTP and Flight
//! handlers and read by the follower's engine.
//!
//! - **applied** (default, `ADB_FOLLOWER_CONSISTENCY=applied`): whatever this follower
//!   has applied, and at least the client's read-your-writes token if it sent one
//!   (`x-adb-after: <offset>`, the `metadata.point.offset` its last write returned).
//! - **strict** (`x-adb-read: strict`, or `ADB_FOLLOWER_CONSISTENCY=strict`): the
//!   leader's read index first (linearizable), as before PF-46.
//!
//! The leader always answers from its own state; these only change follower reads.

/// The headers (HTTP) and metadata keys (Flight) a client sets.
pub const AFTER_HEADER: &str = "x-adb-after";
pub const READ_HEADER: &str = "x-adb-read";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadOpts {
    /// Wait until this follower has applied this log offset (read-your-writes).
    pub after: Option<u64>,
    /// Ask the leader for its read index first (linearizable).
    pub strict: bool,
}

impl ReadOpts {
    /// From a header lookup: `x-adb-after` and `x-adb-read`.
    pub fn from_lookup<'a>(get: impl Fn(&str) -> Option<&'a str>) -> Self {
        Self {
            after: get(AFTER_HEADER).and_then(|v| v.trim().parse().ok()),
            strict: get(READ_HEADER).is_some_and(|v| v.trim().eq_ignore_ascii_case("strict")),
        }
    }

    /// Whether this read needs the leader's read index (the request, or the node's
    /// default `ADB_FOLLOWER_CONSISTENCY=strict`).
    pub fn strict_here(&self) -> bool {
        self.strict || default_strict()
    }
}

fn default_strict() -> bool {
    static S: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *S.get_or_init(|| std::env::var("ADB_FOLLOWER_CONSISTENCY").is_ok_and(|v| v.trim().eq_ignore_ascii_case("strict")))
}

tokio::task_local! {
    static READ_OPTS: ReadOpts;
}

/// The current request's options (defaults outside a request).
pub fn current() -> ReadOpts {
    READ_OPTS.try_with(|o| *o).unwrap_or_default()
}

/// Run `f` with `opts` as the request's options.
pub async fn scope<F: std::future::Future>(opts: ReadOpts, f: F) -> F::Output {
    READ_OPTS.scope(opts, f).await
}

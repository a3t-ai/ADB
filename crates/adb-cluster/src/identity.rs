//! Which cluster a node belongs to, on the wire. A node that knows its cluster's id
//! (the cluster mark, `adb-cli` `cluster_mark`) sends it on every call to another
//! node (`x-adb-cluster`), and refuses a call carrying another cluster's id: a node
//! pointed at the wrong peers (a copied config, a reused address) can't take part in
//! another cluster's Raft group, replication or queries.
//!
//! A call without the header is still answered: nodes of a version before this one
//! don't send it, and a node-by-node upgrade must keep its quorum.

use std::sync::OnceLock;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// The header a node's calls to other nodes carry.
pub const HEADER: &str = "x-adb-cluster";

static ID: OnceLock<String> = OnceLock::new();

/// This node's cluster, once its mark is settled (set once per process).
pub fn set(id: &str) {
    let _ = ID.set(id.to_string());
}

/// This node's cluster, if it knows it.
pub fn get() -> Option<&'static str> {
    ID.get().map(String::as_str)
}

/// `req` with this node's cluster id, if it knows it.
pub fn tag(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match get() {
        Some(id) => req.header(HEADER, id),
        None => req,
    }
}

/// Whether a call claiming cluster `theirs` may be answered by a node of `ours`.
pub fn admits(ours: Option<&str>, theirs: Option<&str>) -> Result<(), String> {
    match (ours, theirs) {
        (Some(o), Some(t)) if o != t => Err(format!("wrong_cluster: this node is in cluster {o}; the call came from cluster {t}")),
        _ => Ok(()),
    }
}

/// Axum middleware for the routes other nodes call: refuses (409 `wrong_cluster`) a
/// call from another cluster.
pub async fn guard(req: Request, next: Next) -> Response {
    let theirs = req.headers().get(HEADER).and_then(|v| v.to_str().ok());
    if let Err(e) = admits(get(), theirs) {
        tracing::warn!(error = %e, path = %req.uri().path(), "refused a call from another cluster");
        return (StatusCode::CONFLICT, e).into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::admits;

    #[test]
    fn only_another_clusters_id_is_refused() {
        assert!(admits(Some("a"), Some("a")).is_ok());
        assert!(admits(Some("a"), None).is_ok(), "an older node sends none");
        assert!(admits(None, Some("b")).is_ok(), "a node not marked yet knows no id");
        assert!(admits(None, None).is_ok());
        let e = admits(Some("a"), Some("b")).unwrap_err();
        assert!(e.starts_with("wrong_cluster"), "{e}");
    }
}

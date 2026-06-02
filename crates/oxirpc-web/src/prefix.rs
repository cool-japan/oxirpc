//! Tower layer that strips a URI path prefix from incoming gRPC-Web requests.
//!
//! Use [`GrpcWebPrefixLayer`] when you need to expose a gRPC-Web service on a
//! sub-path (e.g., `/api/…`) but the underlying tonic handler expects the bare
//! gRPC path (`/mypackage.MyService/Method`).
//!
//! # Example
//!
//! ```rust,no_run
//! use oxirpc_web::GrpcWebPrefixLayer;
//!
//! // Strips "/api" prefix:
//! // /api/mypackage.MyService/Method → /mypackage.MyService/Method
//! let layer = GrpcWebPrefixLayer::new("/api");
//! ```
//!
//! # HTTP/1.1 requirement
//!
//! The server **must** have `accept_http1(true)` set — gRPC-Web requests from
//! browsers arrive over HTTP/1.1 by default.

use std::task::{Context, Poll};

use http::{Request, Uri};
use tower::{Layer, Service};

// ─── Layer ───────────────────────────────────────────────────────────────────

/// Tower [`Layer`] that strips a path prefix from incoming requests.
///
/// Requests whose URI path begins with the configured prefix have that prefix
/// removed before being forwarded to the inner service. Requests that do *not*
/// start with the prefix are forwarded unchanged.
///
/// The match is boundary-safe: `/api` is stripped from `/api/foo` but **not**
/// from `/apifoo` (the remainder must be empty or start with `/`).
#[derive(Debug, Clone)]
pub struct GrpcWebPrefixLayer {
    prefix: String,
}

impl GrpcWebPrefixLayer {
    /// Create a prefix-stripping layer.
    ///
    /// The `prefix` should start with `/` (e.g., `"/api"` or `"/grpc-web"`).
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Return the configured prefix string.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl<S> Layer<S> for GrpcWebPrefixLayer {
    type Service = GrpcWebPrefixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcWebPrefixService {
            inner,
            prefix: self.prefix.clone(),
        }
    }
}

// ─── Service ─────────────────────────────────────────────────────────────────

/// Tower [`Service`] that strips a path prefix from incoming requests.
///
/// Produced by [`GrpcWebPrefixLayer`].
#[derive(Debug, Clone)]
pub struct GrpcWebPrefixService<S> {
    inner: S,
    prefix: String,
}

impl<S, B> Service<Request<B>> for GrpcWebPrefixService<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let path = req.uri().path();

        // Boundary-safe prefix match: remainder must be empty or start with '/'.
        if let Some(remainder) = path.strip_prefix(self.prefix.as_str()) {
            if remainder.is_empty() || remainder.starts_with('/') {
                let new_path = if remainder.is_empty() { "/" } else { remainder };
                if let Some(new_uri) = rebuild_uri(req.uri(), new_path) {
                    *req.uri_mut() = new_uri;
                }
            }
        }

        self.inner.call(req)
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Rebuild a [`Uri`] with a new path, preserving scheme, authority, and query.
///
/// Returns `None` if the resulting URI would be invalid.
fn rebuild_uri(original: &Uri, new_path: &str) -> Option<Uri> {
    let path_and_query_str = match original.query() {
        Some(q) => format!("{}?{}", new_path, q),
        None => new_path.to_owned(),
    };
    let pq: http::uri::PathAndQuery = path_and_query_str.parse().ok()?;
    let mut parts = original.clone().into_parts();
    parts.path_and_query = Some(pq);
    Uri::from_parts(parts).ok()
}

//! CORS policy for the gRPC-Web bridge (Pure Rust, no dependencies).
//!
//! Browsers issue a CORS preflight (`OPTIONS`) before a gRPC-Web call. This
//! module models the policy and produces the response header set, so a future
//! native bridge (or any HTTP layer) can answer preflights and decorate
//! responses without pulling in a CORS middleware crate.
//!
//! # Example
//!
//! ```rust
//! use oxirpc_web::cors::CorsPolicy;
//!
//! let policy = CorsPolicy::new()
//!     .allow_origin("https://app.example.com")
//!     .allow_method("POST")
//!     .allow_header("x-grpc-web")
//!     .max_age_secs(3600);
//!
//! let headers = policy.preflight_headers("https://app.example.com");
//! assert!(headers.iter().any(|(k, _)| k == "access-control-allow-origin"));
//! ```

/// A configurable CORS policy.
#[derive(Debug, Clone)]
pub struct CorsPolicy {
    allowed_origins: Origins,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    exposed_headers: Vec<String>,
    allow_credentials: bool,
    max_age_secs: Option<u64>,
}

/// The set of origins a policy permits.
#[derive(Debug, Clone)]
enum Origins {
    /// Any origin (`*`). Incompatible with credentials.
    Any,
    /// An explicit allow-list (case-sensitive exact match).
    List(Vec<String>),
}

impl Default for CorsPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CorsPolicy {
    /// Create a policy that, by default, allows the standard gRPC-Web method and
    /// headers but **no** origins (you must add at least one, or call
    /// [`CorsPolicy::allow_any_origin`]).
    pub fn new() -> Self {
        Self {
            allowed_origins: Origins::List(Vec::new()),
            allowed_methods: vec!["POST".to_owned(), "OPTIONS".to_owned()],
            allowed_headers: default_grpc_web_headers(),
            exposed_headers: vec!["grpc-status".to_owned(), "grpc-message".to_owned()],
            allow_credentials: false,
            max_age_secs: None,
        }
    }

    /// Allow any origin (`Access-Control-Allow-Origin: *`).
    ///
    /// Note: per the CORS spec, `*` cannot be combined with credentials; calling
    /// [`CorsPolicy::allow_credentials`] afterwards will cause
    /// [`CorsPolicy::preflight_headers`] to echo the request origin instead.
    pub fn allow_any_origin(mut self) -> Self {
        self.allowed_origins = Origins::Any;
        self
    }

    /// Add an explicit allowed origin (exact match, e.g. `https://app.example.com`).
    pub fn allow_origin(mut self, origin: impl Into<String>) -> Self {
        match &mut self.allowed_origins {
            Origins::List(list) => list.push(origin.into()),
            Origins::Any => {
                self.allowed_origins = Origins::List(vec![origin.into()]);
            }
        }
        self
    }

    /// Replace the allowed HTTP methods.
    pub fn allow_methods<I, S>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_methods = methods.into_iter().map(Into::into).collect();
        self
    }

    /// Add a single allowed HTTP method.
    pub fn allow_method(mut self, method: impl Into<String>) -> Self {
        self.allowed_methods.push(method.into());
        self
    }

    /// Add a single allowed request header.
    pub fn allow_header(mut self, header: impl Into<String>) -> Self {
        self.allowed_headers.push(header.into());
        self
    }

    /// Add a response header to expose to the browser.
    pub fn expose_header(mut self, header: impl Into<String>) -> Self {
        self.exposed_headers.push(header.into());
        self
    }

    /// Allow credentials (cookies / authorization) on cross-origin requests.
    pub fn allow_credentials(mut self, enable: bool) -> Self {
        self.allow_credentials = enable;
        self
    }

    /// Set the preflight cache lifetime in seconds.
    pub fn max_age_secs(mut self, secs: u64) -> Self {
        self.max_age_secs = Some(secs);
        self
    }

    /// Whether `origin` is permitted by this policy.
    pub fn is_origin_allowed(&self, origin: &str) -> bool {
        match &self.allowed_origins {
            Origins::Any => true,
            Origins::List(list) => list.iter().any(|o| o == origin),
        }
    }

    /// The value to emit for `Access-Control-Allow-Origin` given a request
    /// `origin`, or [`None`] if the origin is not allowed.
    fn allow_origin_value(&self, origin: &str) -> Option<String> {
        match &self.allowed_origins {
            Origins::Any => {
                if self.allow_credentials {
                    // `*` is illegal with credentials — echo the concrete origin.
                    Some(origin.to_owned())
                } else {
                    Some("*".to_owned())
                }
            }
            Origins::List(list) => list
                .iter()
                .find(|o| o.as_str() == origin)
                .map(|o| o.to_owned()),
        }
    }

    /// Build the response headers for a CORS **preflight** (`OPTIONS`) for the
    /// given request `origin`.
    ///
    /// Returns an empty vector if the origin is not allowed (the caller should
    /// then respond without CORS headers, causing the browser to block).
    pub fn preflight_headers(&self, origin: &str) -> Vec<(String, String)> {
        let Some(allow_origin) = self.allow_origin_value(origin) else {
            return Vec::new();
        };
        let mut headers = vec![
            ("access-control-allow-origin".to_owned(), allow_origin),
            (
                "access-control-allow-methods".to_owned(),
                self.allowed_methods.join(", "),
            ),
            (
                "access-control-allow-headers".to_owned(),
                self.allowed_headers.join(", "),
            ),
        ];
        if let Some(age) = self.max_age_secs {
            headers.push(("access-control-max-age".to_owned(), age.to_string()));
        }
        if self.allow_credentials {
            headers.push((
                "access-control-allow-credentials".to_owned(),
                "true".to_owned(),
            ));
        }
        headers
    }

    /// Build the CORS headers to attach to an actual (non-preflight) response.
    pub fn response_headers(&self, origin: &str) -> Vec<(String, String)> {
        let Some(allow_origin) = self.allow_origin_value(origin) else {
            return Vec::new();
        };
        let mut headers = vec![("access-control-allow-origin".to_owned(), allow_origin)];
        if !self.exposed_headers.is_empty() {
            headers.push((
                "access-control-expose-headers".to_owned(),
                self.exposed_headers.join(", "),
            ));
        }
        if self.allow_credentials {
            headers.push((
                "access-control-allow-credentials".to_owned(),
                "true".to_owned(),
            ));
        }
        headers
    }
}

/// The headers a gRPC-Web client commonly sends, allowed by default.
fn default_grpc_web_headers() -> Vec<String> {
    [
        "content-type",
        "x-grpc-web",
        "x-user-agent",
        "grpc-timeout",
        "authorization",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

// ─── Tower Layer + Service ────────────────────────────────────────────────────

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::{HeaderValue, Method, Request, Response};

/// Tower [`Layer`](tower::Layer) that injects CORS headers using a [`CorsPolicy`].
///
/// Handles `OPTIONS` preflight requests (returns `204 No Content` with CORS
/// headers) and injects `Access-Control-*` headers on all non-preflight
/// responses.
///
/// # Example
///
/// ```rust,no_run
/// use oxirpc_web::cors::{CorsLayer, CorsPolicy};
///
/// let layer = CorsLayer::new(CorsPolicy::new().allow_any_origin());
/// ```
#[derive(Clone, Debug)]
pub struct CorsLayer {
    policy: CorsPolicy,
}

impl CorsLayer {
    /// Create a new [`CorsLayer`] with the given [`CorsPolicy`].
    pub fn new(policy: CorsPolicy) -> Self {
        Self { policy }
    }
}

impl<S> tower::Layer<S> for CorsLayer {
    type Service = CorsService<S>;

    fn layer(&self, inner: S) -> CorsService<S> {
        CorsService {
            inner,
            policy: self.policy.clone(),
        }
    }
}

/// Tower service that applies CORS headers from a [`CorsPolicy`] to an inner
/// service.
///
/// Preflight (`OPTIONS`) requests are answered with `204 No Content` and the
/// appropriate `Access-Control-*` headers.  All other requests are forwarded to
/// the inner service; CORS headers are appended to the response.
#[derive(Clone, Debug)]
pub struct CorsService<S> {
    inner: S,
    policy: CorsPolicy,
}

impl<S, ReqBody, ResBody> tower::Service<Request<ReqBody>> for CorsService<S>
where
    S: tower::Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let origin: Option<String> = req
            .headers()
            .get("origin")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        // ── OPTIONS preflight ────────────────────────────────────────────────
        if req.method() == Method::OPTIONS {
            let policy = self.policy.clone();
            return Box::pin(async move {
                let mut builder = Response::builder().status(204);
                if let Some(ref orig) = origin {
                    for (k, v) in policy.preflight_headers(orig) {
                        if let Ok(val) = HeaderValue::from_str(&v) {
                            builder = builder.header(k, val);
                        }
                    }
                }
                // `ResBody: Default` gives us a zero-allocation empty body for the
                // preflight path (e.g. `tonic::Body::empty()` or `()` in tests).
                match builder.body(ResBody::default()) {
                    Ok(resp) => Ok(resp),
                    Err(_) => Ok(Response::new(ResBody::default())),
                }
            });
        }

        // ── Normal request — forward, then inject CORS headers ───────────────
        let policy = self.policy.clone();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let mut response = fut.await?;
            if let Some(ref orig) = origin {
                if policy.is_origin_allowed(orig) {
                    let headers = response.headers_mut();
                    for (k, v) in policy.response_headers(orig) {
                        if let Ok(name) = http::header::HeaderName::from_bytes(k.as_bytes()) {
                            if let Ok(val) = HeaderValue::from_str(&v) {
                                headers.insert(name, val);
                            }
                        }
                    }
                }
            }
            Ok(response)
        })
    }
}

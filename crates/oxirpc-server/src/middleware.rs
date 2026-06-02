//! Tower middleware layers for the OxiRPC server.
//!
//! This module provides two ready-to-use Tower [`Layer`]
//! implementations:
//!
//! - [`MethodInterceptorLayer`] — dispatches per-path interceptor closures before
//!   forwarding to the inner service.
//! - [`IpRateLimiterLayer`] — token-bucket rate limiting keyed by client IP
//!   address.
//!
//! # Example — method interceptor
//!
//! ```rust,no_run
//! use oxirpc_server::middleware::MethodInterceptorLayer;
//! use tonic::Status;
//!
//! let layer = MethodInterceptorLayer::builder()
//!     .route("/greeter.Greeter/SayHello", |_path, headers| {
//!         if headers.get("authorization").is_some() {
//!             Ok(())
//!         } else {
//!             Err(Status::unauthenticated("missing token"))
//!         }
//!     })
//!     .build();
//! ```
//!
//! # Example — IP rate limiter
//!
//! ```rust,no_run
//! use oxirpc_server::middleware::IpRateLimiterLayer;
//!
//! // Allow 100 req/sec with a burst of 20.
//! let layer = IpRateLimiterLayer::new(100.0, 20);
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use http::{HeaderMap, Request, Response};
use tonic::Status;
use tower::{Layer, Service};

// ─── Type aliases ─────────────────────────────────────────────────────────────

/// Boxed interceptor function type.
type InterceptFn = Arc<dyn Fn(&str, &HeaderMap) -> Result<(), Status> + Send + Sync>;

// ─── MethodInterceptorLayer ───────────────────────────────────────────────────

/// Tower [`Layer`] that dispatches per-method interceptor closures.
///
/// Path matching is an exact string comparison on `req.uri().path()`
/// (e.g. `/greeter.Greeter/SayHello`).  Unmatched paths are forwarded to the
/// inner service unchanged.
///
/// Build via [`MethodInterceptorLayer::builder`]:
///
/// ```rust
/// use oxirpc_server::middleware::MethodInterceptorLayer;
/// use tonic::Status;
///
/// let layer = MethodInterceptorLayer::builder()
///     .route("/svc.Svc/Method", |_path, _headers| Ok(()))
///     .build();
/// ```
#[derive(Clone)]
pub struct MethodInterceptorLayer {
    rules: Arc<Vec<(String, InterceptFn)>>,
}

impl MethodInterceptorLayer {
    /// Return a [`MethodInterceptorBuilder`] for constructing this layer.
    pub fn builder() -> MethodInterceptorBuilder {
        MethodInterceptorBuilder::new()
    }
}

impl<S> Layer<S> for MethodInterceptorLayer {
    type Service = MethodInterceptorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MethodInterceptorService {
            inner,
            rules: Arc::clone(&self.rules),
        }
    }
}

// ─── MethodInterceptorBuilder ─────────────────────────────────────────────────

/// Builder for [`MethodInterceptorLayer`].
pub struct MethodInterceptorBuilder {
    rules: Vec<(String, InterceptFn)>,
}

impl MethodInterceptorBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Register an interceptor closure for the given exact URI path.
    ///
    /// The closure receives `(path: &str, headers: &HeaderMap)` and should
    /// return `Ok(())` to allow the request through or `Err(Status)` to reject it.
    pub fn route(
        mut self,
        path: impl Into<String>,
        f: impl Fn(&str, &HeaderMap) -> Result<(), Status> + Send + Sync + 'static,
    ) -> Self {
        self.rules.push((path.into(), Arc::new(f)));
        self
    }

    /// Consume the builder and return a [`MethodInterceptorLayer`].
    pub fn build(self) -> MethodInterceptorLayer {
        MethodInterceptorLayer {
            rules: Arc::new(self.rules),
        }
    }
}

impl Default for MethodInterceptorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ─── MethodInterceptorService ─────────────────────────────────────────────────

/// Tower [`Service`] produced by [`MethodInterceptorLayer`].
#[derive(Clone)]
pub struct MethodInterceptorService<S> {
    inner: S,
    rules: Arc<Vec<(String, InterceptFn)>>,
}

impl<S, B> Service<Request<B>> for MethodInterceptorService<S>
where
    S: Service<Request<B>, Response = Response<tonic::body::Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    B: Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path().to_owned();
        let headers = req.headers().clone();

        // Check for a matching rule.
        for (rule_path, intercept_fn) in self.rules.iter() {
            if path == *rule_path {
                if let Err(status) = intercept_fn(&path, &headers) {
                    let rejection: Response<tonic::body::Body> =
                        status.into_http::<tonic::body::Body>();
                    return Box::pin(async move { Ok(rejection) });
                }
                // Rule matched and passed — stop checking, forward.
                break;
            }
        }

        Box::pin(self.inner.call(req))
    }
}

// ─── IpRateLimiterLayer ───────────────────────────────────────────────────────

/// Tower [`Layer`] implementing per-IP token-bucket rate limiting.
///
/// Reads the client IP from (in priority order):
///
/// 1. The first value in the `x-forwarded-for` header.
/// 2. The `x-real-ip` header.
/// 3. Falls back to `127.0.0.1`.
///
/// When a bucket is exhausted, the request is rejected with
/// [`tonic::Code::ResourceExhausted`].
///
/// ```rust
/// use oxirpc_server::middleware::IpRateLimiterLayer;
///
/// // 10 req/sec with a burst of 5.
/// let layer = IpRateLimiterLayer::new(10.0, 5);
/// ```
#[derive(Clone, Debug)]
pub struct IpRateLimiterLayer {
    max_per_sec: f64,
    burst: u64,
}

impl IpRateLimiterLayer {
    /// Create a new rate limiter layer.
    ///
    /// * `max_per_sec` — steady-state refill rate (tokens per second).
    /// * `burst` — maximum burst capacity (bucket size).
    pub fn new(max_per_sec: f64, burst: u64) -> Self {
        Self { max_per_sec, burst }
    }
}

impl<S> Layer<S> for IpRateLimiterLayer {
    type Service = IpRateLimiterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IpRateLimiterService {
            inner,
            max_per_sec: self.max_per_sec,
            burst: self.burst,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

// ─── Token bucket ─────────────────────────────────────────────────────────────

struct TokenBucket {
    tokens: f64,
    capacity: f64,
    rate: f64,
    last_refill: Instant,
    last_used: Instant,
}

impl TokenBucket {
    fn new(rate: f64, burst: u64) -> Self {
        let capacity = burst as f64;
        let now = Instant::now();
        Self {
            tokens: capacity,
            capacity,
            rate,
            last_refill: now,
            last_used: now,
        }
    }

    /// Attempt to consume one token.  Returns `true` if a token was available.
    fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.last_used = now;
            true
        } else {
            false
        }
    }
}

// ─── IpRateLimiterService ─────────────────────────────────────────────────────

/// Tower [`Service`] produced by [`IpRateLimiterLayer`].
#[derive(Clone)]
pub struct IpRateLimiterService<S> {
    inner: S,
    max_per_sec: f64,
    burst: u64,
    buckets: Arc<Mutex<HashMap<IpAddr, TokenBucket>>>,
}

/// Extract a best-effort client IP from request headers.
fn extract_ip(headers: &HeaderMap) -> IpAddr {
    // 1. x-forwarded-for: take the first comma-separated entry.
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        let first = xff.split(',').next().unwrap_or("").trim();
        if let Ok(addr) = first.parse::<IpAddr>() {
            return addr;
        }
    }

    // 2. x-real-ip header.
    if let Some(xri) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if let Ok(addr) = xri.trim().parse::<IpAddr>() {
            return addr;
        }
    }

    // 3. Fallback.
    IpAddr::from([127, 0, 0, 1])
}

impl<S, B> Service<Request<B>> for IpRateLimiterService<S>
where
    S: Service<Request<B>, Response = Response<tonic::body::Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    B: Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let ip = extract_ip(req.headers());
        let max_per_sec = self.max_per_sec;
        let burst = self.burst;

        let allowed = match self.buckets.lock() {
            Err(_) => {
                // Lock poisoned — fail open to avoid a hard outage.
                true
            }
            Ok(mut buckets) => {
                let bucket = buckets
                    .entry(ip)
                    .or_insert_with(|| TokenBucket::new(max_per_sec, burst));
                let result = bucket.try_acquire();

                // Periodic cleanup: if the map grows too large, evict fully-refilled
                // buckets that haven't been used recently.
                if buckets.len() > 1000 {
                    let idle_threshold_secs = 10.0 / max_per_sec.max(f64::EPSILON);
                    let now = Instant::now();
                    buckets.retain(|_, b| {
                        !(b.tokens >= b.capacity
                            && now.duration_since(b.last_used).as_secs_f64() > idle_threshold_secs)
                    });
                }

                result
            }
        };

        if !allowed {
            let rejection: Response<tonic::body::Body> =
                Status::resource_exhausted("rate limit exceeded").into_http::<tonic::body::Body>();
            return Box::pin(async move { Ok(rejection) });
        }

        Box::pin(self.inner.call(req))
    }
}

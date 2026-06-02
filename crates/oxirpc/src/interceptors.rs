//! Ready-to-use gRPC interceptor patterns for OxiRPC.
//!
//! This module provides composable [`tonic::service::Interceptor`]
//! implementations covering the most common cross-cutting concerns:
//!
//! - [`crate::interceptors::BearerAuthInterceptor`] — validates `Authorization: Bearer <token>`.
//! - [`crate::interceptors::TracingInterceptor`] — attaches a monotonic `x-request-id` to every RPC.
//! - [`crate::interceptors::DeadlineInterceptor`] — injects a default `grpc-timeout` (millisecond precision, back-compat).
//! - [`crate::interceptors::RateLimitInterceptor`] — token-bucket rate limiter; thread-safe via `Arc<Mutex<f64>>`.
//! - [`crate::interceptors::LoggingInterceptor`] — callback-based logger; sink receives `&Request<()>`.
//! - [`crate::interceptors::MetricsInterceptor`] — atomic counters (total + per-path) with `snapshot()`.
//! - [`crate::interceptors::TimeoutInterceptor`] — injects `grpc-timeout` using the precise `format_grpc_timeout` formatter.
//! - [`crate::interceptors::InterceptorChain`] — composes any number of interceptors in order, short-circuiting on first `Err`.
//!
//! All single-concern interceptors implement [`Clone`] so they can be passed to
//! `tonic::transport::Server::builder().layer(InterceptorLayer::new(…))` or
//! used with `tonic::service::interceptor(…)`.
//!
//! # Example — bearer auth on a server
//!
//! ```rust,no_run
//! use oxirpc::interceptors::BearerAuthInterceptor;
//! use tonic::service::interceptor;
//!
//! let auth = BearerAuthInterceptor::new("super-secret");
//! // Then wrap your service:
//! // let svc = interceptor(auth)(your_grpc_service);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Type alias for the logging sink closure stored inside [`LoggingInterceptor`].
type LogSink = Arc<dyn Fn(&Request<()>) + Send + Sync>;

/// Type alias for a single boxed interceptor step used by [`InterceptorChain`].
type ChainStep = Box<dyn FnMut(Request<()>) -> Result<Request<()>, Status> + Send>;

// ---------------------------------------------------------------------------
// BearerAuthInterceptor
// ---------------------------------------------------------------------------

/// A gRPC interceptor that enforces HTTP Bearer-token authentication.
///
/// Every incoming request must carry the header:
/// ```text
/// authorization: Bearer <token>
/// ```
/// Requests with a missing or incorrect token are rejected with
/// [`tonic::Code::Unauthenticated`].
///
/// # Example
///
/// ```rust
/// use oxirpc::interceptors::BearerAuthInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::Request;
///
/// let mut interceptor = BearerAuthInterceptor::new("mysecret");
///
/// // A request without the header is rejected.
/// let req: Request<()> = Request::new(());
/// assert!(interceptor.call(req).is_err());
///
/// // A request with the correct token passes through.
/// let mut req: Request<()> = Request::new(());
/// req.metadata_mut()
///     .insert("authorization", "Bearer mysecret".parse().unwrap());
/// assert!(interceptor.call(req).is_ok());
/// ```
#[derive(Clone, Debug)]
pub struct BearerAuthInterceptor {
    expected_token: String,
}

impl BearerAuthInterceptor {
    /// Create a new interceptor that accepts the given `token`.
    ///
    /// `token` should be the raw bearer value (without the `Bearer ` prefix).
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            expected_token: token.into(),
        }
    }
}

impl Interceptor for BearerAuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let auth = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        let expected = format!("Bearer {}", self.expected_token);
        match auth {
            Some(v) if v == expected => Ok(request),
            _ => Err(Status::unauthenticated(
                "missing or invalid authorization token",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// TracingInterceptor
// ---------------------------------------------------------------------------

/// A gRPC interceptor that attaches a monotonically-increasing `x-request-id`
/// metadata header to every outgoing request.
///
/// The counter is process-wide and uses [`Ordering::Relaxed`] — sequential
/// per-connection ordering is not guaranteed, but each RPC gets a unique ID.
///
/// # Example
///
/// ```rust
/// use oxirpc::interceptors::TracingInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::Request;
///
/// let mut interceptor = TracingInterceptor;
/// let req: Request<()> = Request::new(());
/// let req = interceptor.call(req).unwrap();
/// let id_header = req.metadata().get("x-request-id");
/// assert!(id_header.is_some());
/// ```
#[derive(Clone, Debug, Default)]
pub struct TracingInterceptor;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Interceptor for TracingInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id_str = id.to_string();
        let header_val = id_str
            .parse()
            .map_err(|_| Status::internal("request-id header encoding error"))?;
        request.metadata_mut().insert("x-request-id", header_val);
        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// DeadlineInterceptor
// ---------------------------------------------------------------------------

/// A gRPC interceptor that injects a default `grpc-timeout` header when the
/// caller has not already set one.
///
/// gRPC timeout format: `<value><unit>` where unit is one of:
/// `H` (hours), `M` (minutes), `S` (seconds), `m` (milliseconds),
/// `u` (microseconds), `n` (nanoseconds).
///
/// This interceptor injects a millisecond-precision timeout derived from the
/// configured [`Duration`].
///
/// # Example
///
/// ```rust
/// use oxirpc::interceptors::DeadlineInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::Request;
/// use std::time::Duration;
///
/// let mut interceptor = DeadlineInterceptor::new(Duration::from_secs(5));
/// let req: Request<()> = Request::new(());
/// let req = interceptor.call(req).unwrap();
/// let timeout = req.metadata().get("grpc-timeout");
/// assert!(timeout.is_some());
/// ```
#[derive(Clone, Debug)]
pub struct DeadlineInterceptor {
    default_timeout: Duration,
}

impl DeadlineInterceptor {
    /// Create a new interceptor with `default_timeout` as the fallback timeout.
    pub fn new(default_timeout: Duration) -> Self {
        Self { default_timeout }
    }
}

impl Interceptor for DeadlineInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        // Only inject if the caller hasn't already set a deadline.
        if request.metadata().get("grpc-timeout").is_none() {
            // gRPC timeout: value in milliseconds + 'm' suffix.
            let millis = self.default_timeout.as_millis();
            let header_val = format!("{millis}m");
            if let Ok(v) = header_val.parse() {
                request.metadata_mut().insert("grpc-timeout", v);
            }
        }
        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// RateLimitInterceptor
// ---------------------------------------------------------------------------

/// A gRPC interceptor implementing a thread-safe token-bucket rate limiter.
///
/// Tokens are refilled continuously based on elapsed wall time. If no token is
/// available when a request arrives, the request is rejected immediately with
/// [`tonic::Code::ResourceExhausted`]. The interceptor can be cheaply cloned
/// because both token state and refill timestamp are stored behind `Arc<Mutex<_>>`.
///
/// # Example
///
/// ```rust
/// use oxirpc::interceptors::RateLimitInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::{Code, Request};
///
/// // capacity=2, 1 token/sec refill
/// let mut limiter = RateLimitInterceptor::new(2, 1.0);
/// assert!(limiter.call(Request::new(())).is_ok());
/// assert!(limiter.call(Request::new(())).is_ok());
/// // Bucket empty — third request rejected.
/// let err = limiter.call(Request::new(())).unwrap_err();
/// assert_eq!(err.code(), Code::ResourceExhausted);
/// ```
#[derive(Clone, Debug)]
pub struct RateLimitInterceptor {
    capacity: f64,
    tokens: Arc<Mutex<f64>>,
    refill_per_sec: f64,
    last_refill: Arc<Mutex<std::time::Instant>>,
}

impl RateLimitInterceptor {
    /// Create a new token-bucket limiter with the given `capacity` and
    /// `refill_per_sec` rate. The bucket starts full.
    pub fn new(capacity: u64, refill_per_sec: f64) -> Self {
        Self {
            capacity: capacity as f64,
            tokens: Arc::new(Mutex::new(capacity as f64)),
            refill_per_sec,
            last_refill: Arc::new(Mutex::new(std::time::Instant::now())),
        }
    }
}

impl Interceptor for RateLimitInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let now = std::time::Instant::now();

        let mut last = self
            .last_refill
            .lock()
            .map_err(|_| Status::internal("rate limiter lock poisoned"))?;
        let elapsed = now.duration_since(*last).as_secs_f64();
        *last = now;
        drop(last);

        let mut tokens = self
            .tokens
            .lock()
            .map_err(|_| Status::internal("rate limiter lock poisoned"))?;
        *tokens = (*tokens + elapsed * self.refill_per_sec).min(self.capacity);

        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(request)
        } else {
            Err(Status::resource_exhausted("rate limit exceeded"))
        }
    }
}

// ---------------------------------------------------------------------------
// LoggingInterceptor
// ---------------------------------------------------------------------------

/// A gRPC interceptor that invokes a user-supplied callback on every request.
///
/// The `sink` closure receives a shared reference to the incoming `Request<()>`
/// before the request is forwarded. Use [`LoggingInterceptor::noop`] when no
/// logging is desired (e.g., in tests that exercise other interceptors in a chain).
///
/// The interceptor is `Clone` because the sink is stored behind `Arc`.
///
/// # Example
///
/// ```rust
/// use std::sync::{Arc, Mutex};
/// use oxirpc::interceptors::LoggingInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::Request;
///
/// let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
/// let log_clone = log.clone();
/// let mut interceptor = LoggingInterceptor::new(move |req| {
///     log_clone.lock().unwrap().push(format!("called: {:?}", req.metadata()));
/// });
/// interceptor.call(Request::new(())).unwrap();
/// assert_eq!(log.lock().unwrap().len(), 1);
/// ```
#[derive(Clone)]
pub struct LoggingInterceptor {
    sink: LogSink,
}

impl LoggingInterceptor {
    /// Create a new interceptor with the provided `sink` callback.
    pub fn new(sink: impl Fn(&Request<()>) + Send + Sync + 'static) -> Self {
        Self {
            sink: Arc::new(sink),
        }
    }

    /// Create a no-op interceptor that silently passes every request through.
    pub fn noop() -> Self {
        Self::new(|_| {})
    }
}

impl std::fmt::Debug for LoggingInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggingInterceptor").finish_non_exhaustive()
    }
}

impl Interceptor for LoggingInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        (self.sink)(&request);
        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// MetricsInterceptor
// ---------------------------------------------------------------------------

/// A snapshot of counters captured by [`MetricsInterceptor`] at a single point
/// in time.
pub struct MetricsSnapshot {
    /// Total number of requests observed since the interceptor was created.
    pub total: u64,
    /// Per-path request counts keyed by the value of the `x-grpc-method`
    /// metadata header, or `"unknown"` when that header is absent.
    pub by_path: HashMap<String, u64>,
}

/// A gRPC interceptor that tracks request counters with zero allocation on the
/// fast path for the total count.
///
/// The interceptor is `Clone`; all clones share the same underlying counters.
///
/// # Example
///
/// ```rust
/// use oxirpc::interceptors::MetricsInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::Request;
///
/// let mut interceptor = MetricsInterceptor::new();
/// let mut req: Request<()> = Request::new(());
/// req.metadata_mut().insert("x-grpc-method", "/svc/Method".parse().unwrap());
/// interceptor.call(req).unwrap();
/// let snap = interceptor.snapshot();
/// assert_eq!(snap.total, 1);
/// assert_eq!(snap.by_path["/svc/Method"], 1);
/// ```
#[derive(Clone, Debug)]
pub struct MetricsInterceptor {
    total: Arc<AtomicU64>,
    by_path: Arc<Mutex<HashMap<String, u64>>>,
}

impl MetricsInterceptor {
    /// Create a new interceptor with all counters initialised to zero.
    pub fn new() -> Self {
        Self {
            total: Arc::new(AtomicU64::new(0)),
            by_path: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Capture a consistent snapshot of the current counter state.
    ///
    /// The `total` value is read first with `SeqCst` ordering; the per-path map
    /// is then cloned under the same lock used for writes, so the snapshot is
    /// internally consistent at the moment of the call.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let total = self.total.load(Ordering::SeqCst);
        let by_path = self.by_path.lock().map(|g| g.clone()).unwrap_or_default();
        MetricsSnapshot { total, by_path }
    }
}

impl Default for MetricsInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl Interceptor for MetricsInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.total.fetch_add(1, Ordering::SeqCst);

        let path = request
            .metadata()
            .get("x-grpc-method")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_owned();

        if let Ok(mut map) = self.by_path.lock() {
            *map.entry(path).or_insert(0) += 1;
        }

        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// TimeoutInterceptor
// ---------------------------------------------------------------------------

/// A gRPC interceptor that injects a `grpc-timeout` header using the
/// standards-compliant [`oxirpc_core::timeout::format_grpc_timeout`] formatter.
///
/// Unlike [`DeadlineInterceptor`] (which hard-codes millisecond encoding),
/// this interceptor picks the coarsest exact unit that fits the duration — e.g.,
/// `5S` for five seconds instead of `5000m`. It is otherwise identical in
/// behaviour: the header is only injected when absent.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
/// use oxirpc::interceptors::TimeoutInterceptor;
/// use tonic::service::Interceptor;
/// use tonic::Request;
///
/// let mut interceptor = TimeoutInterceptor::new(Duration::from_secs(5));
/// let req: Request<()> = Request::new(());
/// let req = interceptor.call(req).unwrap();
/// let hdr = req.metadata().get("grpc-timeout").and_then(|v| v.to_str().ok()).unwrap();
/// assert_eq!(hdr, "5S");
/// ```
#[derive(Clone, Debug)]
pub struct TimeoutInterceptor {
    /// The fallback timeout injected when no `grpc-timeout` header is present.
    pub default_timeout: Duration,
}

impl TimeoutInterceptor {
    /// Create a new interceptor with the given `default_timeout`.
    pub fn new(default_timeout: Duration) -> Self {
        Self { default_timeout }
    }
}

impl Interceptor for TimeoutInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if request.metadata().get("grpc-timeout").is_none() {
            let formatted = oxirpc_core::timeout::format_grpc_timeout(self.default_timeout);
            let header_val = formatted
                .parse()
                .map_err(|_| Status::internal("grpc-timeout header encoding error"))?;
            request.metadata_mut().insert("grpc-timeout", header_val);
        }
        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// InterceptorChain
// ---------------------------------------------------------------------------

/// A composable chain of [`tonic::service::Interceptor`]s executed in
/// insertion order.
///
/// The chain short-circuits on the first `Err` — subsequent steps are not
/// called. Build the chain with [`InterceptorChain::push`]:
///
/// ```rust
/// use std::time::Duration;
/// use oxirpc::interceptors::{InterceptorChain, TracingInterceptor, TimeoutInterceptor};
/// use tonic::service::Interceptor;
/// use tonic::Request;
///
/// let mut chain = InterceptorChain::new()
///     .push(TracingInterceptor)
///     .push(TimeoutInterceptor::new(Duration::from_secs(10)));
///
/// let req: Request<()> = Request::new(());
/// let req = chain.call(req).unwrap();
/// assert!(req.metadata().get("x-request-id").is_some());
/// assert!(req.metadata().get("grpc-timeout").is_some());
/// ```
pub struct InterceptorChain {
    steps: Vec<ChainStep>,
}

impl InterceptorChain {
    /// Create an empty chain.
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Append `interceptor` as the next step in the chain.
    pub fn push<I>(mut self, mut interceptor: I) -> Self
    where
        I: tonic::service::Interceptor + Send + 'static,
    {
        self.steps.push(Box::new(move |req| interceptor.call(req)));
        self
    }
}

impl Default for InterceptorChain {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InterceptorChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InterceptorChain")
            .field("steps", &self.steps.len())
            .finish()
    }
}

impl Interceptor for InterceptorChain {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.steps
            .iter_mut()
            .try_fold(request, |req, step| step(req))
    }
}

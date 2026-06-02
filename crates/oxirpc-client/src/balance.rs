//! Client-side load balancing: endpoint resolution and balancer policies.
//!
//! # Overview
//!
//! - [`Endpoint`] — a URI + weight pair describing one backend replica.
//! - [`Resolver`] / [`DynResolver`] — traits for discovering live endpoints.
//! - [`StaticResolver`] — fixed list; useful for tests and static deployments.
//! - [`DnsResolver`] — resolves a hostname via `tokio::net::lookup_host`.
//! - [`LoadBalancer`] — synchronous, object-safe pick policy.
//! - [`PickFirst`] — always choose the first endpoint.
//! - [`RoundRobin`] — cycle through endpoints using an atomic counter.
//! - [`Weighted`] — deterministic weighted selection without a `rand` dep.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// ── Endpoint ─────────────────────────────────────────────────────────────────

/// A single backend endpoint with an optional load-balancing weight.
///
/// Endpoints whose weight is `0` are still included in the list but are
/// never selected by the [`Weighted`] balancer (they receive zero slots).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// The base URI of the backend (e.g. `http://10.0.0.1:50051`).
    pub uri: http::Uri,
    /// Relative weight for [`Weighted`] selection.  Default is `1`.
    pub weight: u32,
}

impl Endpoint {
    /// Create an endpoint with the default weight of `1`.
    pub fn new(uri: http::Uri) -> Self {
        Self { uri, weight: 1 }
    }

    /// Override the selection weight (builder-style).
    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }
}

// ── ResolveError ─────────────────────────────────────────────────────────────

/// Errors that can occur during endpoint resolution.
#[derive(Debug)]
pub enum ResolveError {
    /// The resulting URI could not be constructed from the resolved address.
    InvalidUri(String),
    /// An I/O error occurred during name resolution (e.g. DNS lookup failed).
    Io(std::io::Error),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::InvalidUri(msg) => write!(f, "invalid URI: {msg}"),
            ResolveError::Io(e) => write!(f, "I/O error during resolution: {e}"),
        }
    }
}

impl std::error::Error for ResolveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ResolveError::Io(e) => Some(e),
            ResolveError::InvalidUri(_) => None,
        }
    }
}

// ── Resolver trait ───────────────────────────────────────────────────────────

/// An async-capable endpoint resolver.
///
/// Implementors discover the set of live backends for a service name.
/// Because `async fn` in traits is stable from Rust 1.75+ and this workspace
/// requires ≥ 1.89, the returned `Future` is unnamed; use [`DynResolver`] for
/// heap-allocated (object-safe) dispatch.
pub trait Resolver: Send + Sync {
    /// Resolve and return the current list of live endpoints.
    fn resolve(&self) -> impl Future<Output = Result<Vec<Endpoint>, ResolveError>> + Send;
}

// ── DynResolver (object-safe wrapper) ────────────────────────────────────────

/// Object-safe variant of [`Resolver`] that boxes the returned future.
///
/// Every type implementing [`Resolver`] automatically implements `DynResolver`
/// via the blanket impl below.
pub trait DynResolver: Send + Sync {
    /// Resolve endpoints, returning a boxed future.
    fn resolve_boxed<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Endpoint>, ResolveError>> + Send + 'a>>;
}

impl<T: Resolver> DynResolver for T {
    fn resolve_boxed<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Endpoint>, ResolveError>> + Send + 'a>> {
        Box::pin(self.resolve())
    }
}

// ── StaticResolver ───────────────────────────────────────────────────────────

/// A resolver that always returns a fixed list of endpoints.
///
/// Useful for unit tests and static deployments where the backend addresses
/// are known at construction time.
#[derive(Clone, Debug)]
pub struct StaticResolver(pub Vec<Endpoint>);

impl StaticResolver {
    /// Create a new static resolver from the given endpoint list.
    pub fn new(endpoints: Vec<Endpoint>) -> Self {
        Self(endpoints)
    }
}

impl Resolver for StaticResolver {
    async fn resolve(&self) -> Result<Vec<Endpoint>, ResolveError> {
        Ok(self.0.clone())
    }
}

// ── DnsResolver ──────────────────────────────────────────────────────────────

/// A resolver that performs a DNS lookup using [`tokio::net::lookup_host`].
///
/// Each resolved `SocketAddr` is converted into an `http` scheme URI.
/// All endpoints receive weight `1`.
///
/// # Note
///
/// Tests that exercise `DnsResolver::resolve` must be gated with `#[ignore]`
/// because they require an active network and a resolvable hostname.
#[derive(Clone, Debug)]
pub struct DnsResolver {
    /// The hostname to resolve.
    pub host: String,
    /// The port to attach to each resolved address.
    pub port: u16,
}

impl DnsResolver {
    /// Create a new DNS resolver for `host:port`.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl Resolver for DnsResolver {
    async fn resolve(&self) -> Result<Vec<Endpoint>, ResolveError> {
        let target = format!("{}:{}", self.host, self.port);
        let addrs = tokio::net::lookup_host(target)
            .await
            .map_err(ResolveError::Io)?;

        addrs
            .map(|addr| {
                let authority = addr.to_string();
                let uri = http::Uri::builder()
                    .scheme("http")
                    .authority(authority.as_str())
                    .path_and_query("/")
                    .build()
                    .map_err(|e| ResolveError::InvalidUri(e.to_string()))?;
                Ok(Endpoint::new(uri))
            })
            .collect()
    }
}

// ── LoadBalancer trait ────────────────────────────────────────────────────────

/// A synchronous, object-safe policy that picks one endpoint from a slice.
///
/// The balancer receives the current endpoint list on every call; it may
/// maintain internal state (e.g. an atomic counter) but must be thread-safe.
pub trait LoadBalancer: Send + Sync {
    /// Return the index of the chosen endpoint, or `None` if the slice is
    /// empty.
    fn pick(&self, endpoints: &[Endpoint]) -> Option<usize>;
}

// ── PickFirst ─────────────────────────────────────────────────────────────────

/// Always selects the first endpoint in the list.
///
/// This matches the default behaviour of most gRPC stubs and is the simplest
/// possible balancer.
pub struct PickFirst;

impl LoadBalancer for PickFirst {
    fn pick(&self, endpoints: &[Endpoint]) -> Option<usize> {
        if endpoints.is_empty() {
            None
        } else {
            Some(0)
        }
    }
}

// ── RoundRobin ───────────────────────────────────────────────────────────────

/// Distributes requests evenly across all endpoints using a lock-free counter.
///
/// Each call to [`pick`](LoadBalancer::pick) atomically increments an internal
/// index and returns `index % len`, ensuring a strict cycle through the list.
pub struct RoundRobin {
    idx: AtomicUsize,
}

impl RoundRobin {
    /// Create a new round-robin balancer starting at index `0`.
    pub fn new() -> Self {
        Self {
            idx: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalancer for RoundRobin {
    fn pick(&self, endpoints: &[Endpoint]) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        let i = self.idx.fetch_add(1, Ordering::Relaxed);
        Some(i % endpoints.len())
    }
}

// ── Weighted ─────────────────────────────────────────────────────────────────

/// Deterministic weighted selection — no external `rand` dependency.
///
/// Each endpoint receives a share of requests proportional to its `weight`.
/// The algorithm increments a monotonic counter on every call and maps the
/// counter modulo the total weight onto an endpoint slot, so the distribution
/// is perfectly uniform over any window equal to the total weight.
///
/// Endpoints with weight `0` never receive traffic.  If *all* endpoints have
/// weight `0`, the first endpoint is returned as a safe fallback.
pub struct Weighted {
    counter: AtomicU64,
}

impl Weighted {
    /// Create a new weighted balancer.
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl Default for Weighted {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalancer for Weighted {
    fn pick(&self, endpoints: &[Endpoint]) -> Option<usize> {
        if endpoints.is_empty() {
            return None;
        }
        let total: u64 = endpoints.iter().map(|e| e.weight as u64).sum();
        if total == 0 {
            return Some(0);
        }
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut slot = c % total;
        for (i, ep) in endpoints.iter().enumerate() {
            if slot < ep.weight as u64 {
                return Some(i);
            }
            slot -= ep.weight as u64;
        }
        // Unreachable given correct arithmetic, but return last as fallback.
        Some(endpoints.len() - 1)
    }
}

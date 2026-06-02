//! Push-based xDS resolver implementation.
//!
//! [`XdsResolver`] implements the [`Resolver`] trait and is fed by an external
//! control plane via the paired [`XdsWatcher`]. The ADS streaming client that
//! drives updates from a real xDS server is deferred to Round 7.
//!
//! # Example
//!
//! ```rust
//! use oxirpc_client::xds::{xds_resolver, ClusterLoadAssignment, HealthStatus,
//!     LbEndpoint, LocalityLbEndpoints, SocketAddress};
//! use oxirpc_client::balance::Resolver;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let (resolver, watcher) = xds_resolver();
//!
//! // Push a CLA update from the control plane (or a test harness).
//! let cla = ClusterLoadAssignment {
//!     cluster_name: "my-service".into(),
//!     endpoints: vec![LocalityLbEndpoints {
//!         lb_endpoints: vec![LbEndpoint {
//!             address: SocketAddress { address: "10.0.0.1".into(), port: 50051 },
//!             health_status: HealthStatus::Healthy,
//!             load_balancing_weight: 1,
//!         }],
//!         load_balancing_weight: 1,
//!     }],
//! };
//! watcher.update(&cla);
//!
//! let endpoints = resolver.resolve().await.unwrap();
//! assert_eq!(endpoints.len(), 1);
//! # }
//! ```

use tokio::sync::watch;

use crate::balance::{Endpoint, ResolveError, Resolver};
use crate::xds::types::ClusterLoadAssignment;

// ── XdsResolver ──────────────────────────────────────────────────────────────

/// A push-based resolver fed by an external control plane.
///
/// Obtain a paired `(XdsResolver, XdsWatcher)` via [`xds_resolver`].
/// The ADS streaming client that drives updates from a real xDS server is
/// deferred to Round 7.
///
/// `XdsResolver::resolve` returns a snapshot of the most recently pushed
/// endpoint list without blocking; it is always immediately available.
pub struct XdsResolver {
    receiver: watch::Receiver<Vec<Endpoint>>,
}

// ── XdsWatcher ───────────────────────────────────────────────────────────────

/// The write side of the xDS resolver channel.
///
/// Call [`XdsWatcher::update`] whenever the control plane delivers a new
/// [`ClusterLoadAssignment`], or [`XdsWatcher::update_endpoints`] for raw
/// endpoint lists (useful in tests).
pub struct XdsWatcher {
    sender: watch::Sender<Vec<Endpoint>>,
}

// ── Constructor ───────────────────────────────────────────────────────────────

/// Create a paired [`XdsResolver`] and [`XdsWatcher`].
///
/// The resolver is initialised with an empty endpoint list. The watcher is
/// used to push updates from the control plane (or a test harness).
pub fn xds_resolver() -> (XdsResolver, XdsWatcher) {
    let (sender, receiver) = watch::channel(Vec::new());
    (XdsResolver { receiver }, XdsWatcher { sender })
}

// ── Resolver impl ─────────────────────────────────────────────────────────────

impl Resolver for XdsResolver {
    fn resolve(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<Endpoint>, ResolveError>> + Send {
        let endpoints = self.receiver.borrow().clone();
        async move { Ok(endpoints) }
    }
}

// ── XdsWatcher methods ────────────────────────────────────────────────────────

impl XdsWatcher {
    /// Push a new endpoint snapshot derived from a [`ClusterLoadAssignment`].
    ///
    /// Only `HealthStatus::Healthy` and `HealthStatus::Unknown` endpoints
    /// from the CLA are forwarded to the resolver; unhealthy and draining
    /// backends are filtered out.
    pub fn update(&self, cla: &ClusterLoadAssignment) {
        let endpoints = cla.to_endpoints();
        // Ignore send error — it means no resolver is listening, which is fine.
        let _ = self.sender.send(endpoints);
    }

    /// Push a raw endpoint list (for testing or manual control).
    ///
    /// This bypasses `ClusterLoadAssignment` conversion; all provided endpoints
    /// are forwarded directly.
    pub fn update_endpoints(&self, endpoints: Vec<Endpoint>) {
        let _ = self.sender.send(endpoints);
    }
}

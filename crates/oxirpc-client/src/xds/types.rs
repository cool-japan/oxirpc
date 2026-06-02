//! xDS endpoint type definitions mirroring Envoy API v3.
//!
//! These types represent a subset of the Envoy EDS (Endpoint Discovery Service)
//! data model sufficient for the `XdsResolver`. Full ADS streaming is deferred
//! to Round 7.

use crate::balance::Endpoint;

// ── HealthStatus ─────────────────────────────────────────────────────────────

/// Health status of an xDS endpoint, mirroring `envoy.config.core.v3.HealthStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Status not known. Treated as healthy for routing purposes (Envoy default).
    Unknown,
    /// Endpoint is healthy and should receive traffic.
    Healthy,
    /// Endpoint is unhealthy and should not receive traffic.
    Unhealthy,
    /// Endpoint is draining (graceful shutdown) and should not receive new traffic.
    Draining,
}

// ── SocketAddress ─────────────────────────────────────────────────────────────

/// An Envoy-style socket address composed of a host and port.
#[derive(Debug, Clone)]
pub struct SocketAddress {
    /// Hostname or IP address string (e.g. `"10.0.0.1"` or `"backend.svc"`).
    pub address: String,
    /// TCP port number.
    pub port: u32,
}

// ── LbEndpoint ───────────────────────────────────────────────────────────────

/// An individual load-balanced endpoint, corresponding to
/// `envoy.config.endpoint.v3.LbEndpoint`.
#[derive(Debug, Clone)]
pub struct LbEndpoint {
    /// The network address of this endpoint.
    pub address: SocketAddress,
    /// Current health status of this endpoint.
    pub health_status: HealthStatus,
    /// Relative load-balancing weight for this endpoint.
    ///
    /// A value of `0` is treated as weight `1` when converting to [`Endpoint`].
    pub load_balancing_weight: u32,
}

// ── LocalityLbEndpoints ───────────────────────────────────────────────────────

/// A locality-grouped set of lb endpoints, corresponding to
/// `envoy.config.endpoint.v3.LocalityLbEndpoints`.
#[derive(Debug, Clone)]
pub struct LocalityLbEndpoints {
    /// The individual load-balanced endpoints in this locality.
    pub lb_endpoints: Vec<LbEndpoint>,
    /// Load-balancing weight applied to the entire locality group.
    pub load_balancing_weight: u32,
}

// ── ClusterLoadAssignment ─────────────────────────────────────────────────────

/// Top-level EDS cluster load assignment, mirroring
/// `envoy.config.endpoint.v3.ClusterLoadAssignment`.
#[derive(Debug, Clone)]
pub struct ClusterLoadAssignment {
    /// The name of the cluster this assignment belongs to.
    pub cluster_name: String,
    /// Locality-grouped endpoint sets comprising this cluster.
    pub endpoints: Vec<LocalityLbEndpoints>,
}

impl ClusterLoadAssignment {
    /// Convert this CLA to a list of [`Endpoint`]s, filtering out unhealthy backends.
    ///
    /// Only [`HealthStatus::Healthy`] and [`HealthStatus::Unknown`] endpoints are
    /// included (Envoy treats `Unknown` as healthy). Each endpoint's weight is set
    /// from [`LbEndpoint::load_balancing_weight`], defaulting to `1` if `0`.
    ///
    /// Endpoints whose `address` cannot be parsed into a valid `http::Uri` are
    /// silently dropped.
    pub fn to_endpoints(&self) -> Vec<Endpoint> {
        self.endpoints
            .iter()
            .flat_map(|locality| locality.lb_endpoints.iter())
            .filter(|ep| {
                ep.health_status == HealthStatus::Healthy
                    || ep.health_status == HealthStatus::Unknown
            })
            .filter_map(|ep| {
                let uri_str = format!("http://{}:{}", ep.address.address, ep.address.port);
                uri_str.parse::<http::Uri>().ok().map(|uri| {
                    let weight = if ep.load_balancing_weight == 0 {
                        1
                    } else {
                        ep.load_balancing_weight
                    };
                    Endpoint::new(uri).with_weight(weight)
                })
            })
            .collect()
    }
}

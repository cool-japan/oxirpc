//! Hand-authored prost types for the Envoy xDS API v3.
//!
//! This module provides minimal [`prost::Message`]-derived structs for the
//! subset of Envoy proto types needed by the ADS streaming client.  Field
//! numbers match the canonical Envoy proto definitions exactly — correctness
//! here is critical for wire compatibility.
//!
//! # Proto sources
//! - `envoy/service/discovery/v3/discovery.proto`
//! - `envoy/config/endpoint/v3/endpoint_components.proto`
//! - `envoy/config/cluster/v3/cluster.proto`
//! - `google/protobuf/any.proto`
//! - `google/rpc/status.proto`

// ── Node ─────────────────────────────────────────────────────────────────────

/// Envoy node identity, sent in every `DiscoveryRequest`.
///
/// Maps to `envoy.config.core.v3.Node`, fields 1, 2, 6, 8.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Node {
    /// Unique identifier for the node.
    #[prost(string, tag = "1")]
    pub id: String,

    /// Cluster/deployment group this node belongs to.
    #[prost(string, tag = "2")]
    pub cluster: String,

    /// User-agent name (e.g. `"oxirpc"`).
    #[prost(string, tag = "6")]
    pub user_agent_name: String,

    /// User-agent version string.
    #[prost(string, tag = "8")]
    pub user_agent_version: String,
}

// ── DiscoveryRequest ─────────────────────────────────────────────────────────

/// Sent by the client to request or acknowledge resources.
///
/// Maps to `envoy.service.discovery.v3.DiscoveryRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct DiscoveryRequest {
    /// Version info from the most recent accepted `DiscoveryResponse`.
    /// Empty string on initial request.
    #[prost(string, tag = "1")]
    pub version_info: String,

    /// Node identity.
    #[prost(message, optional, tag = "2")]
    pub node: Option<Node>,

    /// Resource names this client is subscribed to.
    #[prost(string, repeated, tag = "3")]
    pub resource_names: Vec<String>,

    /// The type URL (e.g. `CDS_TYPE_URL` or `EDS_TYPE_URL`).
    #[prost(string, tag = "4")]
    pub type_url: String,

    /// Nonce from the `DiscoveryResponse` being ACKed or NACKed.
    /// Empty string on initial request.
    #[prost(string, tag = "5")]
    pub response_nonce: String,

    /// Set on NACK to describe the decoding error.  Absent on ACK.
    #[prost(message, optional, tag = "6")]
    pub error_detail: Option<GoogleRpcStatus>,
}

// ── DiscoveryResponse ─────────────────────────────────────────────────────────

/// Sent by the server to push resource updates.
///
/// Maps to `envoy.service.discovery.v3.DiscoveryResponse`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct DiscoveryResponse {
    /// Version identifier for the enclosed resources.
    #[prost(string, tag = "1")]
    pub version_info: String,

    /// The encoded resources (each is a `google.protobuf.Any`).
    #[prost(message, repeated, tag = "2")]
    pub resources: Vec<GoogleAny>,

    /// Type URL shared by all resources in this response.
    #[prost(string, tag = "4")]
    pub type_url: String,

    /// Nonce used by the client in the next ACK/NACK.
    #[prost(string, tag = "5")]
    pub nonce: String,
}

// ── GoogleAny ─────────────────────────────────────────────────────────────────

/// A minimal `google.protobuf.Any`.
///
/// Field numbers 1 and 2 match the canonical `google/protobuf/any.proto`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GoogleAny {
    /// Fully-qualified type URL (e.g. `type.googleapis.com/envoy.config.cluster.v3.Cluster`).
    #[prost(string, tag = "1")]
    pub type_url: String,

    /// Protobuf-encoded payload bytes.
    #[prost(bytes = "vec", tag = "2")]
    pub value: Vec<u8>,
}

// ── GoogleRpcStatus ───────────────────────────────────────────────────────────

/// A minimal `google.rpc.Status` used in NACK `error_detail`.
///
/// Field numbers 1 and 2 match `google/rpc/status.proto`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GoogleRpcStatus {
    /// gRPC status code (see `google.rpc.Code`).
    #[prost(int32, tag = "1")]
    pub code: i32,

    /// Human-readable error message.
    #[prost(string, tag = "2")]
    pub message: String,
}

// ── Cluster ───────────────────────────────────────────────────────────────────

/// Minimal Cluster definition from `envoy.config.cluster.v3.Cluster`.
///
/// Only fields required to extract the EDS service name are decoded.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Cluster {
    /// The cluster name.
    #[prost(string, tag = "1")]
    pub name: String,

    /// EDS cluster configuration (present when `type == EDS`).
    #[prost(message, optional, tag = "3")]
    pub eds_cluster_config: Option<EdsClusterConfig>,
}

// ── EdsClusterConfig ─────────────────────────────────────────────────────────

/// The `eds_cluster_config` embedded in a `Cluster`.
///
/// Field 2 is `service_name` in
/// `envoy.config.cluster.v3.Cluster.EdsClusterConfig`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct EdsClusterConfig {
    /// Override service name for EDS lookups.  Empty means use cluster name.
    #[prost(string, tag = "2")]
    pub service_name: String,
}

// ── ClusterLoadAssignment ─────────────────────────────────────────────────────

/// Encoded EDS resource: `envoy.config.endpoint.v3.ClusterLoadAssignment`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ClusterLoadAssignment {
    /// Name of the cluster this assignment belongs to.
    #[prost(string, tag = "1")]
    pub cluster_name: String,

    /// Locality-grouped sets of endpoints.
    #[prost(message, repeated, tag = "2")]
    pub endpoints: Vec<LocalityLbEndpoints>,
}

// ── LocalityLbEndpoints ───────────────────────────────────────────────────────

/// A locality-grouped set of load-balanced endpoints.
///
/// Maps to `envoy.config.endpoint.v3.LocalityLbEndpoints`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct LocalityLbEndpoints {
    /// Individual load-balanced endpoints in this locality.
    #[prost(message, repeated, tag = "1")]
    pub lb_endpoints: Vec<LbEndpoint>,

    /// Optional weight for this entire locality group.
    #[prost(message, optional, tag = "6")]
    pub load_balancing_weight: Option<UInt32Value>,
}

// ── LbEndpoint ────────────────────────────────────────────────────────────────

/// A single load-balanced endpoint.
///
/// Maps to `envoy.config.endpoint.v3.LbEndpoint`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct LbEndpoint {
    /// Network address of this endpoint.
    #[prost(message, optional, tag = "1")]
    pub endpoint: Option<Endpoint>,

    /// Health status (maps to `envoy.config.core.v3.HealthStatus`).
    #[prost(int32, tag = "2")]
    pub health_status: i32,

    /// Per-endpoint load-balancing weight.
    #[prost(message, optional, tag = "4")]
    pub load_balancing_weight: Option<UInt32Value>,
}

// ── Endpoint ──────────────────────────────────────────────────────────────────

/// Network address wrapper.
///
/// Maps to `envoy.config.endpoint.v3.Endpoint`, field 1 = address.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Endpoint {
    /// The socket address of this endpoint.
    #[prost(message, optional, tag = "1")]
    pub address: Option<Address>,
}

// ── Address ───────────────────────────────────────────────────────────────────

/// An Envoy `core.v3.Address`, field 1 = `socket_address`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Address {
    /// The socket (TCP/UDP) address.
    #[prost(message, optional, tag = "1")]
    pub socket_address: Option<SocketAddress>,
}

// ── SocketAddress ─────────────────────────────────────────────────────────────

/// A TCP/UDP socket address (`core.v3.SocketAddress`).
///
/// Field 1 = address, 3 = port_value.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SocketAddress {
    /// IP address or hostname string.
    #[prost(string, tag = "1")]
    pub address: String,

    /// TCP/UDP port number (field 3, `oneof port_specifier` = `port_value`).
    #[prost(uint32, tag = "3")]
    pub port_value: u32,
}

// ── UInt32Value ───────────────────────────────────────────────────────────────

/// `google.protobuf.UInt32Value` — a nullable uint32 wrapper.
#[derive(Clone, PartialEq, prost::Message)]
pub struct UInt32Value {
    /// The wrapped value.
    #[prost(uint32, tag = "1")]
    pub value: u32,
}

// ── Type URL constants ────────────────────────────────────────────────────────

/// Type URL for `envoy.config.cluster.v3.Cluster`.
pub const CDS_TYPE_URL: &str = "type.googleapis.com/envoy.config.cluster.v3.Cluster";

/// Type URL for `envoy.config.endpoint.v3.ClusterLoadAssignment`.
pub const EDS_TYPE_URL: &str = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";

// ── Conversion: proto CLA → xds::types::CLA ──────────────────────────────────

impl From<ClusterLoadAssignment> for crate::xds::types::ClusterLoadAssignment {
    fn from(proto: ClusterLoadAssignment) -> Self {
        let endpoints = proto
            .endpoints
            .into_iter()
            .map(|locality| {
                let lb_weight = locality.load_balancing_weight.map(|w| w.value).unwrap_or(1);
                let lb_endpoints = locality
                    .lb_endpoints
                    .into_iter()
                    .filter_map(convert_lb_endpoint)
                    .collect();
                crate::xds::types::LocalityLbEndpoints {
                    lb_endpoints,
                    load_balancing_weight: lb_weight,
                }
            })
            .collect();

        crate::xds::types::ClusterLoadAssignment {
            cluster_name: proto.cluster_name,
            endpoints,
        }
    }
}

/// Convert a proto `LbEndpoint` to the xds types variant.
///
/// Returns `None` if the address chain is missing.
fn convert_lb_endpoint(ep: LbEndpoint) -> Option<crate::xds::types::LbEndpoint> {
    let sock = ep
        .endpoint
        .as_ref()
        .and_then(|e| e.address.as_ref())
        .and_then(|a| a.socket_address.as_ref())?;

    let health_status = match ep.health_status {
        1 => crate::xds::types::HealthStatus::Healthy,
        2 => crate::xds::types::HealthStatus::Unhealthy,
        3 => crate::xds::types::HealthStatus::Draining,
        _ => crate::xds::types::HealthStatus::Unknown,
    };

    let weight = ep.load_balancing_weight.map(|w| w.value).unwrap_or(0);

    Some(crate::xds::types::LbEndpoint {
        address: crate::xds::types::SocketAddress {
            address: sock.address.clone(),
            port: sock.port_value,
        },
        health_status,
        load_balancing_weight: weight,
    })
}

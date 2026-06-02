//! Native gRPC health checking protocol types.
//!
//! These are hand-written prost types for `grpc.health.v1`, avoiding the
//! need to run protoc or generate from a `.proto` file.
//!
//! ## Wire format
//!
//! These types match the binary proto encoding of the official
//! `grpc.health.v1.Health` service proto exactly.  They are suitable for
//! embedding in a custom health server that speaks the standard gRPC health
//! checking protocol.
//!
//! ## Example
//!
//! ```rust
//! use oxirpc_health::proto::{HealthCheckRequest, HealthCheckResponse, ServingStatusProto};
//! use prost::Message;
//!
//! let req = HealthCheckRequest { service: "my.Service".to_owned() };
//! let bytes = req.encode_to_vec();
//! let decoded = HealthCheckRequest::decode(bytes.as_slice()).expect("valid proto bytes");
//! assert_eq!(decoded.service, "my.Service");
//!
//! let resp = HealthCheckResponse::serving();
//! assert_eq!(resp.status, ServingStatusProto::Serving as i32);
//! ```

use prost::Message;

/// Request for the `Health.Check` and `Health.Watch` RPCs.
///
/// Corresponds to `grpc.health.v1.HealthCheckRequest` in the proto definition.
#[derive(Clone, PartialEq, Message)]
pub struct HealthCheckRequest {
    /// Service name to check.
    ///
    /// Pass an empty string to query overall server health
    /// (the gRPC health checking convention for the unnamed / aggregate service).
    #[prost(string, tag = "1")]
    pub service: String,
}

/// Response from the `Health.Check` and `Health.Watch` RPCs.
///
/// Corresponds to `grpc.health.v1.HealthCheckResponse` in the proto definition.
#[derive(Clone, PartialEq, Message)]
pub struct HealthCheckResponse {
    /// The health status of the service, encoded as the `ServingStatusProto` enum value.
    #[prost(enumeration = "ServingStatusProto", tag = "1")]
    pub status: i32,
}

/// Serving status values for the `grpc.health.v1` proto.
///
/// Corresponds to `grpc.health.v1.HealthCheckResponse.ServingStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum ServingStatusProto {
    /// The server does not have a status to report.
    Unknown = 0,
    /// The server is SERVING and is ready to take requests.
    Serving = 1,
    /// The server is NOT_SERVING and not ready to take requests.
    NotServing = 2,
    /// The service name is not registered in the server.
    ServiceUnknown = 3,
}

impl HealthCheckResponse {
    /// Construct a `SERVING` response.
    pub fn serving() -> Self {
        Self {
            status: ServingStatusProto::Serving as i32,
        }
    }

    /// Construct a `NOT_SERVING` response.
    pub fn not_serving() -> Self {
        Self {
            status: ServingStatusProto::NotServing as i32,
        }
    }

    /// Return the `ServingStatusProto` variant for the current status value.
    ///
    /// Returns `None` when `status` contains an unrecognised discriminant.
    pub fn serving_status(&self) -> Option<ServingStatusProto> {
        ServingStatusProto::try_from(self.status).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn request_roundtrip_empty_service() {
        let req = HealthCheckRequest {
            service: String::new(),
        };
        let bytes = req.encode_to_vec();
        // Empty string field → empty proto bytes (default value not written)
        let decoded = HealthCheckRequest::decode(bytes.as_slice()).expect("decode failed");
        assert_eq!(decoded.service, "");
    }

    #[test]
    fn request_roundtrip_named_service() {
        let req = HealthCheckRequest {
            service: "grpc.health.v1.Health".to_owned(),
        };
        let bytes = req.encode_to_vec();
        let decoded = HealthCheckRequest::decode(bytes.as_slice()).expect("decode failed");
        assert_eq!(decoded.service, "grpc.health.v1.Health");
    }

    #[test]
    fn response_serving_status_code() {
        let resp = HealthCheckResponse::serving();
        assert_eq!(resp.status, ServingStatusProto::Serving as i32);
        assert_eq!(resp.serving_status(), Some(ServingStatusProto::Serving));
    }

    #[test]
    fn response_not_serving_status_code() {
        let resp = HealthCheckResponse::not_serving();
        assert_eq!(resp.status, ServingStatusProto::NotServing as i32);
        assert_eq!(resp.serving_status(), Some(ServingStatusProto::NotServing));
    }

    #[test]
    fn response_roundtrip() {
        let resp = HealthCheckResponse::serving();
        let bytes = resp.encode_to_vec();
        let decoded = HealthCheckResponse::decode(bytes.as_slice()).expect("decode failed");
        assert_eq!(decoded.status, ServingStatusProto::Serving as i32);
    }

    #[test]
    fn unknown_discriminant_returns_none() {
        let resp = HealthCheckResponse { status: 99 };
        assert_eq!(resp.serving_status(), None);
    }
}

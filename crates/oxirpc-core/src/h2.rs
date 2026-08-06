//! gRPC-over-HTTP/2 wire format constants and header helpers.
//!
//! > **Deprecated shim**: header constants and types have moved to
//! > [`crate::wire::header`] and [`crate::wire::trailer`]. The items below are
//! > preserved for one release of backward compatibility.
//!
//! gRPC is a thin layer over HTTP/2:
//! - Requests use POST, path `/{package}.{Service}/{Method}`, content-type `application/grpc[+proto]`
//! - Responses carry `grpc-status` and `grpc-message` in HTTP/2 trailers
//! - Payloads use the 5-byte length-prefixed framing from [`crate::grpc`]
//!
//! This module provides constants and helpers for constructing the correct
//! HTTP/2 header sets that a native gRPC transport must produce.
//!
//! # Phase 2 Foundation
//!
//! This module is the foundation for the native gRPC-over-HTTP/2 transport
//! (Phase 2). It does NOT implement H2 transport (that is Phase 3); it
//! provides the header names, constants, and helper types that a future
//! native transport will use.

/// HTTP/2 pseudo-header `:method` value for gRPC — always POST.
pub const GRPC_METHOD: &str = "POST";
/// Content-type for binary protobuf encoding.
pub const GRPC_CONTENT_TYPE: &str = "application/grpc+proto";
/// Content-type prefix for any gRPC variant.
pub const GRPC_CONTENT_TYPE_PREFIX: &str = "application/grpc";

/// HTTP/2 header name for gRPC compression encoding.
pub const GRPC_ENCODING: &str = "grpc-encoding";
/// HTTP/2 header name for gRPC accepted compression encodings.
pub const GRPC_ACCEPT_ENCODING: &str = "grpc-accept-encoding";
/// HTTP/2 trailer name for the gRPC status code.
pub const GRPC_STATUS: &str = "grpc-status";
/// HTTP/2 trailer name for the gRPC error message.
pub const GRPC_MESSAGE: &str = "grpc-message";
/// HTTP/2 header name for gRPC deadline/timeout.
pub const GRPC_TIMEOUT: &str = "grpc-timeout";
/// User-agent prefix for OxiRPC clients.
pub const GRPC_USER_AGENT_PREFIX: &str = "oxirpc/";

/// Encapsulates the mandatory HTTP/2 headers for a gRPC request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRequestHeaders {
    /// HTTP/2 `:scheme` (http or https)
    pub scheme: String,
    /// HTTP/2 `:authority` (host:port)
    pub authority: String,
    /// HTTP/2 `:path` (`/{package}.{Service}/{Method}`)
    pub path: String,
    /// `grpc-encoding` header value, if compression is used
    pub encoding: Option<String>,
    /// `grpc-timeout` header value, if a deadline is set
    pub timeout: Option<String>,
    /// User-defined metadata headers
    pub metadata: Vec<(String, Vec<u8>)>,
}

impl GrpcRequestHeaders {
    /// Construct a minimal gRPC request header set.
    pub fn new(
        scheme: impl Into<String>,
        authority: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            scheme: scheme.into(),
            authority: authority.into(),
            path: path.into(),
            encoding: None,
            timeout: None,
            metadata: Vec::new(),
        }
    }

    /// Returns `true` if the path has the format `/{service}/{method}` —
    /// i.e. starts with exactly one `/` and contains a `/` separator
    /// after the leading slash, but not immediately after it.
    ///
    /// Valid:   `/foo/bar`
    /// Invalid: `foo/bar`, `/foo`, `//foo/bar`
    pub fn is_valid_path(&self) -> bool {
        let p = self.path.as_str();
        // Must start with exactly one '/'.
        if !p.starts_with('/') {
            return false;
        }
        let after_slash = &p[1..];
        // Must not be empty after the leading '/', and must not start with '/'.
        if after_slash.is_empty() || after_slash.starts_with('/') {
            return false;
        }
        // Must contain at least one more '/' to separate service from method.
        after_slash.contains('/')
    }
}

/// Encapsulates the HTTP/2 response initial metadata for a gRPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcResponseHeaders {
    /// HTTP/2 `:status` (always `"200"` for gRPC — errors are in trailers)
    pub http_status: u16,
    /// Content-type (`application/grpc` or `application/grpc+proto`)
    pub content_type: String,
    /// Compression encoding of the response, if any
    pub encoding: Option<String>,
}

impl GrpcResponseHeaders {
    /// Construct a standard gRPC response header (`200` + `application/grpc+proto`).
    pub fn new() -> Self {
        Self {
            http_status: 200,
            content_type: GRPC_CONTENT_TYPE.to_owned(),
            encoding: None,
        }
    }
}

impl Default for GrpcResponseHeaders {
    fn default() -> Self {
        Self::new()
    }
}

/// gRPC status code (mirrored from the gRPC spec, 0–16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum GrpcStatusCode {
    /// Not an error; returned on success.
    Ok = 0,
    /// The operation was cancelled, typically by the caller.
    Cancelled = 1,
    /// Unknown error.
    Unknown = 2,
    /// Client specified an invalid argument.
    InvalidArgument = 3,
    /// Deadline expired before operation could complete.
    DeadlineExceeded = 4,
    /// Some requested entity was not found.
    NotFound = 5,
    /// The entity that a client attempted to create already exists.
    AlreadyExists = 6,
    /// The caller does not have permission to execute the specified operation.
    PermissionDenied = 7,
    /// Some resource has been exhausted.
    ResourceExhausted = 8,
    /// Operation was rejected because the system is not in a state required.
    FailedPrecondition = 9,
    /// The operation was aborted.
    Aborted = 10,
    /// Operation was attempted past the valid range.
    OutOfRange = 11,
    /// Operation is not implemented or not supported.
    Unimplemented = 12,
    /// Internal errors.
    Internal = 13,
    /// The service is currently unavailable.
    Unavailable = 14,
    /// Unrecoverable data loss or corruption.
    DataLoss = 15,
    /// The request does not have valid authentication credentials.
    Unauthenticated = 16,
}

impl GrpcStatusCode {
    /// Parse from a numeric string (as carried in the `grpc-status` trailer).
    pub fn from_str_value(s: &str) -> Option<Self> {
        let n: u32 = s.trim().parse().ok()?;
        Self::from_u32(n)
    }

    /// Parse from a `u32`.
    pub fn from_u32(n: u32) -> Option<Self> {
        match n {
            0 => Some(Self::Ok),
            1 => Some(Self::Cancelled),
            2 => Some(Self::Unknown),
            3 => Some(Self::InvalidArgument),
            4 => Some(Self::DeadlineExceeded),
            5 => Some(Self::NotFound),
            6 => Some(Self::AlreadyExists),
            7 => Some(Self::PermissionDenied),
            8 => Some(Self::ResourceExhausted),
            9 => Some(Self::FailedPrecondition),
            10 => Some(Self::Aborted),
            11 => Some(Self::OutOfRange),
            12 => Some(Self::Unimplemented),
            13 => Some(Self::Internal),
            14 => Some(Self::Unavailable),
            15 => Some(Self::DataLoss),
            16 => Some(Self::Unauthenticated),
            _ => None,
        }
    }

    /// The numeric wire value as a string, suitable for the `grpc-status` trailer.
    ///
    /// Returns the decimal representation of the status code as defined in the
    /// gRPC spec (e.g. `"0"` for `Ok`, `"5"` for `NotFound`).
    pub fn as_wire_value(&self) -> &'static str {
        match self {
            Self::Ok => "0",
            Self::Cancelled => "1",
            Self::Unknown => "2",
            Self::InvalidArgument => "3",
            Self::DeadlineExceeded => "4",
            Self::NotFound => "5",
            Self::AlreadyExists => "6",
            Self::PermissionDenied => "7",
            Self::ResourceExhausted => "8",
            Self::FailedPrecondition => "9",
            Self::Aborted => "10",
            Self::OutOfRange => "11",
            Self::Unimplemented => "12",
            Self::Internal => "13",
            Self::Unavailable => "14",
            Self::DataLoss => "15",
            Self::Unauthenticated => "16",
        }
    }
}

use crate::status::StatusCode;

impl From<GrpcStatusCode> for StatusCode {
    fn from(code: GrpcStatusCode) -> StatusCode {
        StatusCode::from_i32_lossy(code as i32)
    }
}

impl From<StatusCode> for GrpcStatusCode {
    fn from(code: StatusCode) -> GrpcStatusCode {
        GrpcStatusCode::from_u32(code as i32 as u32).unwrap_or(GrpcStatusCode::Unknown)
    }
}

/// Check whether a content-type value is a gRPC content-type.
///
/// Returns `true` for `"application/grpc"`, `"application/grpc+proto"`, and
/// any value starting with `"application/grpc"` (per gRPC spec section on
/// content-type negotiation). Returns `false` for `"application/json"` and
/// other non-gRPC types.
///
/// This delegates to [`crate::wire::header::is_grpc_content_type`], the
/// canonical implementation used by the native H2/H3 transports to actually
/// validate requests and responses. Kept here (rather than removed outright)
/// as part of this module's documented one-release backward-compatibility
/// shim — see the module docs.
pub fn is_grpc_content_type(ct: &str) -> bool {
    crate::wire::header::is_grpc_content_type(ct)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── GrpcStatusCode ──────────────────────────────────────────────────────

    #[test]
    fn status_code_u32_round_trip_all_17() {
        let all = [
            GrpcStatusCode::Ok,
            GrpcStatusCode::Cancelled,
            GrpcStatusCode::Unknown,
            GrpcStatusCode::InvalidArgument,
            GrpcStatusCode::DeadlineExceeded,
            GrpcStatusCode::NotFound,
            GrpcStatusCode::AlreadyExists,
            GrpcStatusCode::PermissionDenied,
            GrpcStatusCode::ResourceExhausted,
            GrpcStatusCode::FailedPrecondition,
            GrpcStatusCode::Aborted,
            GrpcStatusCode::OutOfRange,
            GrpcStatusCode::Unimplemented,
            GrpcStatusCode::Internal,
            GrpcStatusCode::Unavailable,
            GrpcStatusCode::DataLoss,
            GrpcStatusCode::Unauthenticated,
        ];
        assert_eq!(all.len(), 17);
        for code in all {
            let n = code as u32;
            assert_eq!(
                GrpcStatusCode::from_u32(n),
                Some(code),
                "from_u32({n}) should yield {code:?}"
            );
        }
    }

    #[test]
    fn status_code_from_u32_out_of_range_returns_none() {
        assert!(GrpcStatusCode::from_u32(17).is_none());
        assert!(GrpcStatusCode::from_u32(u32::MAX).is_none());
    }

    #[test]
    fn status_code_from_str_value_parses_valid_range() {
        for n in 0u32..=16 {
            let s = n.to_string();
            assert!(
                GrpcStatusCode::from_str_value(&s).is_some(),
                "from_str_value(\"{s}\") should succeed"
            );
        }
    }

    #[test]
    fn status_code_from_str_value_rejects_invalid() {
        assert!(GrpcStatusCode::from_str_value("17").is_none());
        assert!(GrpcStatusCode::from_str_value("-1").is_none());
        assert!(GrpcStatusCode::from_str_value("abc").is_none());
        assert!(GrpcStatusCode::from_str_value("").is_none());
    }

    #[test]
    fn status_code_as_wire_value_matches_numeric() {
        for n in 0u32..=16 {
            let code = GrpcStatusCode::from_u32(n).unwrap();
            let wire = code.as_wire_value();
            let parsed: u32 = wire.parse().expect("wire value must be numeric");
            assert_eq!(parsed, n, "as_wire_value mismatch for {code:?}");
        }
    }

    // ─── is_grpc_content_type ─────────────────────────────────────────────────

    #[test]
    fn is_grpc_content_type_recognises_exact_types() {
        assert!(is_grpc_content_type("application/grpc"));
        assert!(is_grpc_content_type("application/grpc+proto"));
        assert!(is_grpc_content_type("application/grpc+json"));
    }

    #[test]
    fn is_grpc_content_type_rejects_non_grpc() {
        assert!(!is_grpc_content_type("application/json"));
        assert!(!is_grpc_content_type("text/plain"));
        assert!(!is_grpc_content_type(""));
    }

    // ─── GrpcRequestHeaders::is_valid_path ────────────────────────────────────

    #[test]
    fn is_valid_path_accepts_canonical_grpc_path() {
        let h = GrpcRequestHeaders::new("https", "localhost:50051", "/foo.Bar/Baz");
        assert!(h.is_valid_path(), "/foo.Bar/Baz should be valid");
    }

    #[test]
    fn is_valid_path_rejects_no_leading_slash() {
        let h = GrpcRequestHeaders::new("https", "localhost", "foo/bar");
        assert!(!h.is_valid_path(), "no leading slash should be invalid");
    }

    #[test]
    fn is_valid_path_rejects_single_segment() {
        let h = GrpcRequestHeaders::new("https", "localhost", "/foo");
        assert!(
            !h.is_valid_path(),
            "/foo (single segment) should be invalid"
        );
    }

    #[test]
    fn is_valid_path_rejects_double_leading_slash() {
        let h = GrpcRequestHeaders::new("https", "localhost", "//foo/bar");
        assert!(!h.is_valid_path(), "//foo/bar should be invalid");
    }

    // ─── GrpcResponseHeaders ─────────────────────────────────────────────────

    #[test]
    fn response_headers_default_has_200_and_grpc_proto() {
        let h = GrpcResponseHeaders::default();
        assert_eq!(h.http_status, 200);
        assert_eq!(h.content_type, GRPC_CONTENT_TYPE);
        assert!(h.encoding.is_none());
    }

    #[test]
    fn response_headers_new_equals_default() {
        assert_eq!(GrpcResponseHeaders::new(), GrpcResponseHeaders::default());
    }

    // ─── GrpcRequestHeaders builder ─────────────────────────────────────────

    #[test]
    fn request_headers_new_fields_initialised() {
        let h = GrpcRequestHeaders::new("http", "example.com:80", "/pkg.Svc/Method");
        assert_eq!(h.scheme, "http");
        assert_eq!(h.authority, "example.com:80");
        assert_eq!(h.path, "/pkg.Svc/Method");
        assert!(h.encoding.is_none());
        assert!(h.timeout.is_none());
        assert!(h.metadata.is_empty());
    }
}

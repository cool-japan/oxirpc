//! HTTP/2 headers for gRPC requests and responses.
//!
//! gRPC maps onto HTTP/2 by setting specific pseudo-headers (`:method`,
//! `:scheme`, `:authority`, `:path`) and regular headers (`content-type`,
//! `te`, `grpc-encoding`, `grpc-accept-encoding`, `grpc-timeout`,
//! `user-agent`). This module provides builders and parsers for those headers.

use http::{HeaderMap, HeaderName, HeaderValue};

use crate::{
    encoding::CompressionEncoding,
    metadata::{base64_decode, base64_encode, Metadata},
    wire::WireError,
};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Bare `application/grpc` content-type.
pub const CONTENT_TYPE_GRPC: &str = "application/grpc";
/// Protobuf variant of the gRPC content-type.
pub const CONTENT_TYPE_GRPC_PROTO: &str = "application/grpc+proto";
/// JSON variant of the gRPC content-type.
pub const CONTENT_TYPE_GRPC_JSON: &str = "application/grpc+json";
/// The HTTP/2 `te` header value required by gRPC clients.
pub const TE_TRAILERS: &str = "trailers";
/// Encodings advertised in `grpc-accept-encoding` when no override is given.
/// Matches the CompressionEncoding enum: identity, gzip, zstd.
pub const DEFAULT_ACCEPT_ENCODING: &str = "identity,gzip,zstd";

/// Returns `true` if `v` starts with `"application/grpc"` (case-sensitive,
/// per the gRPC spec content-type match rule).
pub fn is_grpc_content_type(v: &str) -> bool {
    v.starts_with(CONTENT_TYPE_GRPC)
}

// ─── PseudoHeaders ────────────────────────────────────────────────────────────

/// HTTP/2 pseudo-headers for a gRPC request.
///
/// These cannot be inserted into `http::HeaderMap` directly (they're not valid
/// header names in the `http` crate), so they are returned separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PseudoHeaders {
    /// Always `"POST"` for gRPC.
    pub method: &'static str,
    /// `"http"` or `"https"`.
    pub scheme: String,
    /// Host and optional port, e.g. `"example.com:50051"`.
    pub authority: String,
    /// RPC path: `"/{package}.{Service}/{Method}"`.
    pub path: String,
}

// ─── BuiltRequestHeaders ─────────────────────────────────────────────────────

/// The result of [`build_request_headers`].
#[derive(Debug)]
pub struct BuiltRequestHeaders {
    /// HTTP/2 pseudo-headers (`:method`, `:scheme`, `:authority`, `:path`).
    pub pseudo: PseudoHeaders,
    /// Regular HTTP headers ready for transmission.
    pub headers: HeaderMap,
}

// ─── RequestHeaderSpec ────────────────────────────────────────────────────────

/// Parameters for building gRPC request headers.
pub struct RequestHeaderSpec<'a> {
    /// `"http"` or `"https"`.
    pub scheme: &'a str,
    /// Host and optional port.
    pub authority: &'a str,
    /// RPC path (`/{package}.{Service}/{Method}`).
    pub path: &'a str,
    /// Content-type to use; defaults to [`CONTENT_TYPE_GRPC_PROTO`] if empty.
    pub content_type: &'a str,
    /// The encoding to advertise in `grpc-encoding`, if any.
    pub encoding: Option<CompressionEncoding>,
    /// Encodings to advertise in `grpc-accept-encoding`.
    /// An empty slice suppresses that header entirely.
    pub accept_encoding: &'a [CompressionEncoding],
    /// Request deadline carried as `grpc-timeout`.
    pub timeout: Option<std::time::Duration>,
    /// `user-agent` value, if any.
    pub user_agent: Option<&'a str>,
    /// Custom metadata headers.
    pub metadata: &'a Metadata,
}

/// Validate the gRPC RPC path (`/{pkg}.Service/Method`).
///
/// The path must start with exactly one `/` and must contain at least one
/// further `/` separating the service from the method.
fn validate_path(path: &str) -> Result<(), WireError> {
    if !path.starts_with('/') {
        return Err(WireError::BadHeaderValue(format!(
            "gRPC path must start with '/': {path:?}"
        )));
    }
    let after = &path[1..];
    if after.is_empty() || after.starts_with('/') || !after.contains('/') {
        return Err(WireError::BadHeaderValue(format!(
            "gRPC path must be /<service>/<method>: {path:?}"
        )));
    }
    Ok(())
}

/// Build the HTTP headers for a gRPC request.
///
/// Returns a [`BuiltRequestHeaders`] containing the pseudo-headers (which the
/// caller must apply to the HTTP/2 `HEADERS` frame) and the regular headers
/// (which can be inserted directly via an `http::Request` builder).
///
/// # Errors
///
/// - [`WireError::BadHeaderValue`] — if the path is malformed.
/// - [`WireError::BadTimeoutHeader`] — if a timeout duration cannot be encoded.
/// - [`WireError::BadHeaderValue`] — if a metadata key or value is not a legal
///   HTTP header name / value.
pub fn build_request_headers(
    spec: &RequestHeaderSpec<'_>,
) -> Result<BuiltRequestHeaders, WireError> {
    validate_path(spec.path)?;

    let mut headers = HeaderMap::new();

    // content-type
    let ct = if spec.content_type.is_empty() {
        CONTENT_TYPE_GRPC_PROTO
    } else {
        spec.content_type
    };
    headers.insert(http::header::CONTENT_TYPE, hv(ct)?);

    // te: trailers (required by gRPC spec)
    headers.insert(http::header::TE, hv(TE_TRAILERS)?);

    // user-agent (optional)
    if let Some(ua) = spec.user_agent {
        headers.insert(http::header::USER_AGENT, hv(ua)?);
    }

    // grpc-encoding
    if let Some(enc) = spec.encoding {
        if enc.is_compressing() {
            headers.insert(hn("grpc-encoding")?, hv(enc.as_str())?);
        }
    }

    // grpc-accept-encoding
    if !spec.accept_encoding.is_empty() {
        let accept: Vec<&str> = spec.accept_encoding.iter().map(|e| e.as_str()).collect();
        headers.insert(hn("grpc-accept-encoding")?, hv(&accept.join(","))?);
    }

    // grpc-timeout
    if let Some(dur) = spec.timeout {
        use crate::timeout::format_grpc_timeout;
        let ts = format_grpc_timeout(dur);
        headers.insert(
            hn("grpc-timeout")?,
            hv(&ts).map_err(|_| WireError::BadTimeoutHeader(ts.clone()))?,
        );
    }

    // Custom metadata
    for (key, value_bytes) in spec.metadata.iter() {
        let header_name = hn(key)?;
        let header_val = if Metadata::is_binary_key(key) {
            // binary keys: base64-encode the raw bytes
            let encoded = base64_encode(value_bytes);
            HeaderValue::from_str(&encoded).map_err(|e| WireError::BadHeaderValue(e.to_string()))?
        } else {
            let s = std::str::from_utf8(value_bytes)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            hv(s)?
        };
        headers.append(header_name, header_val);
    }

    let pseudo = PseudoHeaders {
        method: "POST",
        scheme: spec.scheme.to_owned(),
        authority: spec.authority.to_owned(),
        path: spec.path.to_owned(),
    };

    Ok(BuiltRequestHeaders { pseudo, headers })
}

// ─── ParsedRequest ───────────────────────────────────────────────────────────

/// Parsed fields from an incoming gRPC request's HTTP headers.
///
/// Note: `path` is populated only when the caller supplies it via the `path`
/// argument to [`parse_request_headers`]. HTTP/2 pseudo-headers (`:path`,
/// `:authority`) are not carried in `HeaderMap` — the H2 layer must extract
/// them from the `HEADERS` frame and supply them separately.
#[derive(Debug, Default)]
pub struct ParsedRequest {
    /// RPC path (`:path` pseudo-header), populated when supplied by the caller.
    pub path: String,
    /// `:authority` pseudo-header.
    pub authority: Option<String>,
    /// `content-type` value.
    pub content_type: Option<String>,
    /// Negotiated encoding from `grpc-encoding`.
    pub encoding: Option<CompressionEncoding>,
    /// Accepted encodings from `grpc-accept-encoding`.
    pub accept_encoding: Vec<CompressionEncoding>,
    /// Timeout from `grpc-timeout`.
    pub timeout: Option<std::time::Duration>,
    /// Other headers folded into metadata.
    pub metadata: Metadata,
}

/// Parse the HTTP headers of an incoming gRPC request into a [`ParsedRequest`].
///
/// `path` must be supplied by the caller from the HTTP/2 `:path` pseudo-header
/// (extracted by the h2 transport layer from the `HEADERS` frame). It is written
/// directly into [`ParsedRequest::path`]; passing `""` leaves the field empty.
///
/// Only regular headers (not pseudo-headers) are inspected in `headers`. The
/// `host` header is used as a proxy for `:authority` when present.
pub fn parse_request_headers(headers: &HeaderMap, path: &str) -> Result<ParsedRequest, WireError> {
    let mut parsed = ParsedRequest {
        path: path.to_owned(),
        ..ParsedRequest::default()
    };

    // authority: check "host" header (populated by h2 layer on server side)
    if let Some(host) = headers.get("host") {
        parsed.authority = Some(
            host.to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?
                .to_owned(),
        );
    }

    // content-type
    if let Some(ct) = headers.get(http::header::CONTENT_TYPE) {
        parsed.content_type = Some(
            ct.to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?
                .to_owned(),
        );
    }

    // grpc-encoding
    if let Some(enc_hv) = headers.get("grpc-encoding") {
        let enc_str = enc_hv
            .to_str()
            .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        parsed.encoding = CompressionEncoding::from_str_opt(enc_str);
    }

    // grpc-accept-encoding
    if let Some(ae_hv) = headers.get("grpc-accept-encoding") {
        let ae_str = ae_hv
            .to_str()
            .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        parsed.accept_encoding = ae_str
            .split(',')
            .filter_map(CompressionEncoding::from_str_opt)
            .collect();
    }

    // grpc-timeout
    if let Some(to_hv) = headers.get("grpc-timeout") {
        let to_str = to_hv
            .to_str()
            .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        let dur = crate::timeout::parse_grpc_timeout(to_str)
            .map_err(|e| WireError::BadTimeoutHeader(e.to_string()))?;
        parsed.timeout = Some(dur);
    }

    // Everything else goes into metadata, respecting -bin convention.
    let reserved: std::collections::HashSet<&str> = [
        "content-type",
        "te",
        "user-agent",
        "grpc-encoding",
        "grpc-accept-encoding",
        "grpc-timeout",
        "host",
    ]
    .iter()
    .copied()
    .collect();

    for (name, value) in headers.iter() {
        let key = name.as_str();
        if reserved.contains(key) {
            continue;
        }
        if Metadata::is_binary_key(key) {
            let b64 = value
                .to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            let bytes = base64_decode(b64)
                .ok_or_else(|| WireError::BadHeaderValue(format!("bad base64 in {key}")))?;
            parsed
                .metadata
                .insert_bin(key, &bytes)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        } else {
            let s = value
                .to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            parsed
                .metadata
                .insert(key, s)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        }
    }

    Ok(parsed)
}

// ─── Response headers ────────────────────────────────────────────────────────

/// Parameters for building gRPC response initial headers.
pub struct ResponseHeaderSpec<'a> {
    /// `content-type` value.
    pub content_type: &'a str,
    /// Encoding advertised in `grpc-encoding`.
    pub encoding: Option<CompressionEncoding>,
    /// Trailing metadata to include in initial headers (non-trailer).
    pub metadata: &'a Metadata,
}

/// Build the initial HTTP headers for a gRPC response (HTTP status 200).
pub fn build_response_headers(spec: &ResponseHeaderSpec<'_>) -> Result<HeaderMap, WireError> {
    let mut headers = HeaderMap::new();

    let ct = if spec.content_type.is_empty() {
        CONTENT_TYPE_GRPC_PROTO
    } else {
        spec.content_type
    };
    headers.insert(http::header::CONTENT_TYPE, hv(ct)?);

    if let Some(enc) = spec.encoding {
        if enc.is_compressing() {
            headers.insert(hn("grpc-encoding")?, hv(enc.as_str())?);
        }
    }

    for (key, value_bytes) in spec.metadata.iter() {
        let header_name = hn(key)?;
        let header_val = if Metadata::is_binary_key(key) {
            let encoded = base64_encode(value_bytes);
            HeaderValue::from_str(&encoded).map_err(|e| WireError::BadHeaderValue(e.to_string()))?
        } else {
            let s = std::str::from_utf8(value_bytes)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            hv(s)?
        };
        headers.append(header_name, header_val);
    }

    Ok(headers)
}

// ─── ParsedResponse ───────────────────────────────────────────────────────────

/// Parsed fields from a gRPC response's initial HTTP headers.
#[derive(Debug, Default)]
pub struct ParsedResponse {
    /// HTTP status code (should be 200 for gRPC).
    pub http_status: u16,
    /// `content-type` value.
    pub content_type: Option<String>,
    /// Negotiated encoding.
    pub encoding: Option<CompressionEncoding>,
    /// Other headers as metadata.
    pub metadata: Metadata,
}

/// Parse the initial headers of a gRPC response.
pub fn parse_response_headers(headers: &HeaderMap) -> Result<ParsedResponse, WireError> {
    let mut parsed = ParsedResponse::default();

    if let Some(ct) = headers.get(http::header::CONTENT_TYPE) {
        parsed.content_type = Some(
            ct.to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?
                .to_owned(),
        );
    }

    if let Some(enc_hv) = headers.get("grpc-encoding") {
        let enc_str = enc_hv
            .to_str()
            .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        parsed.encoding = CompressionEncoding::from_str_opt(enc_str);
    }

    let reserved: std::collections::HashSet<&str> =
        ["content-type", "grpc-encoding"].iter().copied().collect();

    for (name, value) in headers.iter() {
        let key = name.as_str();
        if reserved.contains(key) {
            continue;
        }
        if Metadata::is_binary_key(key) {
            let b64 = value
                .to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            let bytes = base64_decode(b64)
                .ok_or_else(|| WireError::BadHeaderValue(format!("bad base64 in {key}")))?;
            parsed
                .metadata
                .insert_bin(key, &bytes)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        } else {
            let s = value
                .to_str()
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
            parsed
                .metadata
                .insert(key, s)
                .map_err(|e| WireError::BadHeaderValue(e.to_string()))?;
        }
    }

    Ok(parsed)
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/// Convert a `&str` to a [`HeaderName`].
fn hn(s: &str) -> Result<HeaderName, WireError> {
    HeaderName::from_bytes(s.as_bytes()).map_err(|e| WireError::BadHeaderValue(e.to_string()))
}

/// Convert a `&str` to a [`HeaderValue`].
fn hv(s: &str) -> Result<HeaderValue, WireError> {
    HeaderValue::from_str(s).map_err(|e| WireError::BadHeaderValue(e.to_string()))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encoding::CompressionEncoding, metadata::Metadata};
    use std::time::Duration;

    fn default_spec<'a>(path: &'a str, metadata: &'a Metadata) -> RequestHeaderSpec<'a> {
        RequestHeaderSpec {
            scheme: "https",
            authority: "localhost:50051",
            path,
            content_type: "",
            encoding: None,
            accept_encoding: &[],
            timeout: None,
            user_agent: None,
            metadata,
        }
    }

    #[test]
    fn request_build_sets_content_type_and_te_trailers() {
        let md = Metadata::new();
        let spec = default_spec("/foo.Bar/Baz", &md);
        let built = build_request_headers(&spec).unwrap();
        assert_eq!(
            built.headers.get(http::header::CONTENT_TYPE).unwrap(),
            CONTENT_TYPE_GRPC_PROTO
        );
        assert_eq!(built.headers.get(http::header::TE).unwrap(), TE_TRAILERS);
        assert_eq!(built.pseudo.method, "POST");
        assert_eq!(built.pseudo.path, "/foo.Bar/Baz");
    }

    #[test]
    fn request_build_includes_grpc_timeout_when_set() {
        let md = Metadata::new();
        let spec = RequestHeaderSpec {
            timeout: Some(Duration::from_secs(5)),
            ..default_spec("/foo.Bar/Baz", &md)
        };
        let built = build_request_headers(&spec).unwrap();
        let to = built
            .headers
            .get("grpc-timeout")
            .expect("grpc-timeout missing");
        assert_eq!(to.to_str().unwrap(), "5S");
    }

    #[test]
    fn request_build_accept_encoding_comma_joined() {
        let md = Metadata::new();
        let accept = [CompressionEncoding::Gzip, CompressionEncoding::Zstd];
        let spec = RequestHeaderSpec {
            accept_encoding: &accept,
            ..default_spec("/foo.Bar/Baz", &md)
        };
        let built = build_request_headers(&spec).unwrap();
        let ae = built
            .headers
            .get("grpc-accept-encoding")
            .expect("grpc-accept-encoding missing");
        assert_eq!(ae.to_str().unwrap(), "gzip,zstd");
    }

    #[test]
    fn request_build_path_validation_rejects_missing_slash() {
        let md = Metadata::new();
        let spec = default_spec("no_leading_slash", &md);
        let err = build_request_headers(&spec).unwrap_err();
        assert!(matches!(err, WireError::BadHeaderValue(_)));
    }

    #[test]
    fn request_build_binary_metadata_base64_encoded() {
        let mut md = Metadata::new();
        md.insert_bin("token-bin", b"\x00\x01\x02").unwrap();
        let spec = default_spec("/foo.Bar/Baz", &md);
        let built = build_request_headers(&spec).unwrap();
        // Must have "token-bin" header with base64 value
        let v = built.headers.get("token-bin").expect("token-bin missing");
        // Decode it back and verify
        let decoded = base64_decode(v.to_str().unwrap()).expect("base64 decode failed");
        assert_eq!(decoded, b"\x00\x01\x02");
    }

    #[test]
    fn parse_request_headers_round_trip() {
        let mut md = Metadata::new();
        md.insert("x-trace-id", "abc123").unwrap();
        let accept = [CompressionEncoding::Gzip];
        let spec = RequestHeaderSpec {
            scheme: "https",
            authority: "localhost:50051",
            path: "/pkg.Svc/Method",
            content_type: CONTENT_TYPE_GRPC_PROTO,
            encoding: Some(CompressionEncoding::Gzip),
            accept_encoding: &accept,
            timeout: Some(Duration::from_millis(500)),
            user_agent: Some("test-client/1.0"),
            metadata: &md,
        };
        let built = build_request_headers(&spec).unwrap();
        let parsed = parse_request_headers(&built.headers, "/pkg.Svc/Method").unwrap();
        assert_eq!(parsed.path, "/pkg.Svc/Method");
        assert_eq!(
            parsed.content_type.as_deref(),
            Some(CONTENT_TYPE_GRPC_PROTO)
        );
        assert_eq!(parsed.encoding, Some(CompressionEncoding::Gzip));
        assert_eq!(parsed.accept_encoding, vec![CompressionEncoding::Gzip]);
        assert_eq!(parsed.timeout, Some(Duration::from_millis(500)));
        assert_eq!(parsed.metadata.get("x-trace-id"), Some("abc123"));
    }

    #[test]
    fn response_build_defaults_to_grpc_proto() {
        let md = Metadata::new();
        let spec = ResponseHeaderSpec {
            content_type: "",
            encoding: None,
            metadata: &md,
        };
        let headers = build_response_headers(&spec).unwrap();
        assert_eq!(
            headers.get(http::header::CONTENT_TYPE).unwrap(),
            CONTENT_TYPE_GRPC_PROTO
        );
    }

    #[test]
    fn user_metadata_preserved_in_response_headers() {
        let mut md = Metadata::new();
        md.insert("x-request-id", "req-42").unwrap();
        let spec = ResponseHeaderSpec {
            content_type: CONTENT_TYPE_GRPC_PROTO,
            encoding: None,
            metadata: &md,
        };
        let headers = build_response_headers(&spec).unwrap();
        let v = headers.get("x-request-id").expect("x-request-id missing");
        assert_eq!(v.to_str().unwrap(), "req-42");
    }
}

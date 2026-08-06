//! Response `content-type` validation for native gRPC calls.
//!
//! Per the gRPC-over-HTTP/2 spec, a response whose `content-type` does not
//! begin with `application/grpc` is not a gRPC response at all — e.g. a
//! reverse-proxy error page, a load-balancer health page, or a plain HTTP
//! endpoint accidentally reachable at the same address. Feeding such a body
//! into the gRPC frame decoder produces a confusing "invalid gRPC frame"
//! error; this module lets both the H2 and H3 client transports fail fast
//! with a clear, typed error instead.

use http::{HeaderMap, StatusCode};
use oxirpc_core::wire::header::is_grpc_content_type;
use oxirpc_core::OxiRpcError;

/// Validate that `headers` (the response's initial headers) carry a gRPC
/// `content-type`.
///
/// # Errors
///
/// Returns [`OxiRpcError::Transport`] naming the actual `content-type` value
/// (or noting its absence) and the HTTP status, when the header is missing or
/// does not satisfy [`is_grpc_content_type`].
pub(crate) fn validate_grpc_response_content_type(
    headers: &HeaderMap,
    http_status: StatusCode,
) -> Result<(), OxiRpcError> {
    let content_type = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if content_type.is_some_and(is_grpc_content_type) {
        return Ok(());
    }
    let message = match content_type {
        Some(ct) => format!(
            "server response has non-gRPC content-type {ct:?} (http status {})",
            http_status.as_u16()
        ),
        None => format!(
            "server response is missing a content-type header (http status {})",
            http_status.as_u16()
        ),
    };
    Err(OxiRpcError::Transport(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bare_grpc_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/grpc".parse().expect("header value"),
        );
        assert!(validate_grpc_response_content_type(&headers, StatusCode::OK).is_ok());
    }

    #[test]
    fn accepts_grpc_proto_and_json_variants() {
        for ct in ["application/grpc+proto", "application/grpc+json"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                ct.parse().expect("header value"),
            );
            assert!(
                validate_grpc_response_content_type(&headers, StatusCode::OK).is_ok(),
                "{ct:?} must be accepted"
            );
        }
    }

    #[test]
    fn rejects_missing_content_type() {
        let headers = HeaderMap::new();
        let err = validate_grpc_response_content_type(&headers, StatusCode::OK)
            .expect_err("missing content-type must be rejected");
        match err {
            OxiRpcError::Transport(msg) => {
                assert!(msg.contains("missing"), "got: {msg}");
            }
            other => panic!("expected Transport error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_html_content_type_and_reports_status() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "text/html".parse().expect("header value"),
        );
        let err = validate_grpc_response_content_type(&headers, StatusCode::BAD_GATEWAY)
            .expect_err("text/html must be rejected");
        match err {
            OxiRpcError::Transport(msg) => {
                assert!(msg.contains("text/html"), "got: {msg}");
                assert!(msg.contains("502"), "got: {msg}");
            }
            other => panic!("expected Transport error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_json_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/json".parse().expect("header value"),
        );
        assert!(validate_grpc_response_content_type(&headers, StatusCode::OK).is_err());
    }
}

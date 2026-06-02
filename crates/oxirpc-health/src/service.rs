//! Native `grpc.health.v1.Health` tower `Service` implementation.
//!
//! [`NativeHealthService`] is a [`tower::Service`] that dispatches to:
//!
//! - `/grpc.health.v1.Health/Check` — unary RPC
//! - `/grpc.health.v1.Health/Watch` — server-streaming RPC
//!
//! All other paths return an HTTP 200 with a `grpc-status: 12` (UNIMPLEMENTED)
//! trailer, conforming to the gRPC wire format.
//!
//! # Compression
//!
//! The service performs gRPC compression negotiation:
//!
//! - Reads the client's `grpc-accept-encoding` header to negotiate response compression.
//! - Reads the client's `grpc-encoding` header to know how the request body is compressed.
//! - Uses the `*_with_encoding` wire helpers for all request decoding and response encoding.
//! - Sets `grpc-encoding` in the response headers when sending compressed responses.
//! - Advertises `grpc-accept-encoding` in responses to tell clients what the server supports.
//!
//! Enable compression features downstream: `oxirpc-health/gzip` or `oxirpc-health/zstd`.

use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Request;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt as _;
use tonic::server::NamedService;

use oxirpc_core::encoding::CompressionEncoding;
use oxirpc_core::wire::{
    server::{
        encode_grpc_message_with_encoding, error_response_body, grpc_response_headers,
        ok_grpc_trailers, read_unary_request_with_encoding, streaming_response_body,
        unary_response_body_compressed,
    },
    NativeBody,
};
use oxirpc_core::OxiRpcError;
use oxirpc_core::ServerCompressionPrefs;

use crate::proto::{HealthCheckRequest, HealthCheckResponse, ServingStatusProto};
use crate::state::HealthState;

// ─────────────────────────────────────────────────────────────────────────────
// NativeHealthService
// ─────────────────────────────────────────────────────────────────────────────

/// A native `grpc.health.v1.Health` service backed by a shared [`HealthState`].
///
/// Implements `NamedService` (required by tonic router) and `tower::Service`.
#[derive(Debug, Clone)]
pub struct NativeHealthService {
    state: Arc<HealthState>,
}

impl NativeHealthService {
    /// Create a new [`NativeHealthService`] sharing the given [`HealthState`].
    pub fn new(state: Arc<HealthState>) -> Self {
        Self { state }
    }
}

impl Default for NativeHealthService {
    /// Create a [`NativeHealthService`] backed by a fresh, empty [`HealthState`].
    ///
    /// All services will initially report `SERVICE_UNKNOWN`.  Use
    /// [`HealthState::set`] on the shared state to update individual services.
    fn default() -> Self {
        Self::new(HealthState::new())
    }
}

impl NamedService for NativeHealthService {
    const NAME: &'static str = "grpc.health.v1.Health";
}

// ─────────────────────────────────────────────────────────────────────────────
// tower::Service impl
// ─────────────────────────────────────────────────────────────────────────────

impl<B> tower::Service<Request<B>> for NativeHealthService
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<OxiRpcError>,
{
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let state = Arc::clone(&self.state);
        let path = req.uri().path().to_owned();

        Box::pin(async move {
            let resp = match path.as_str() {
                "/grpc.health.v1.Health/Check" => handle_check(state, req).await,
                "/grpc.health.v1.Health/Watch" => handle_watch(state, req).await,
                // gRPC status 12 = UNIMPLEMENTED
                _ => grpc_error(12, "unknown method"),
            };
            Ok(resp)
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Check (unary)
// ─────────────────────────────────────────────────────────────────────────────

/// Handler for the unary `Check` RPC.
async fn handle_check<B>(state: Arc<HealthState>, req: Request<B>) -> http::Response<NativeBody>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<OxiRpcError>,
{
    // Extract server prefs from extensions *before* consuming the body.
    let server_prefs = req
        .extensions()
        .get::<Arc<ServerCompressionPrefs>>()
        .map(Arc::as_ref);
    // Negotiate compression from headers *before* consuming the body.
    let response_enc = negotiate_response_encoding(req.headers(), server_prefs);
    let request_enc = parse_request_encoding(req.headers());

    let check_req = match read_unary_request_with_encoding::<HealthCheckRequest, _>(
        req.into_body(),
        request_enc,
    )
    .await
    {
        Ok(r) => r,
        // gRPC status 13 = INTERNAL
        Err(e) => return grpc_error(13, &e.to_string()),
    };

    match state.get_status(&check_req.service).await {
        Some(status) => {
            let proto_val = serving_status_to_response_i32(status);
            let resp_msg = HealthCheckResponse { status: proto_val };
            // `unary_response_body_compressed` fast-paths to the uncompressed path
            // when `response_enc == Identity`, so this is always safe to call.
            match unary_response_body_compressed(&resp_msg, response_enc) {
                Ok(native_body) => {
                    let mut response = http::Response::new(native_body);
                    *response.status_mut() = http::StatusCode::OK;
                    let response_headers = grpc_response_headers();
                    for (k, v) in &response_headers {
                        response.headers_mut().insert(k.clone(), v.clone());
                    }
                    // Advertise what this server supports.
                    add_accept_encoding_header(response.headers_mut());
                    // Tell the client how the response payload is encoded.
                    if response_enc.is_compressing() {
                        if let Ok(v) = http::HeaderValue::from_str(response_enc.as_str()) {
                            response.headers_mut().insert("grpc-encoding", v);
                        }
                    }
                    response
                }
                // gRPC status 13 = INTERNAL
                Err(e) => grpc_error(13, &e.to_string()),
            }
        }
        // gRPC status 5 = NOT_FOUND
        None => grpc_error(5, &format!("service '{}' not found", check_req.service)),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Watch (server-streaming)
// ─────────────────────────────────────────────────────────────────────────────

/// Handler for the server-streaming `Watch` RPC.
async fn handle_watch<B>(state: Arc<HealthState>, req: Request<B>) -> http::Response<NativeBody>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<OxiRpcError>,
{
    // Extract server prefs from extensions *before* consuming the body.
    let server_prefs = req
        .extensions()
        .get::<Arc<ServerCompressionPrefs>>()
        .map(Arc::as_ref);
    // Negotiate compression from headers *before* consuming the body.
    let response_enc = negotiate_response_encoding(req.headers(), server_prefs);
    let request_enc = parse_request_encoding(req.headers());

    let watch_req = match read_unary_request_with_encoding::<HealthCheckRequest, _>(
        req.into_body(),
        request_enc,
    )
    .await
    {
        Ok(r) => r,
        // gRPC status 13 = INTERNAL
        Err(e) => return grpc_error(13, &e.to_string()),
    };

    let rx = state.watcher(&watch_req.service).await;
    let (sender, native_body) = streaming_response_body();

    tokio::spawn(async move {
        let mut stream = WatchStream::new(rx);

        while let Some(status_i32) = stream.next().await {
            let resp_msg = HealthCheckResponse { status: status_i32 };
            // `encode_grpc_message_with_encoding` fast-paths to uncompressed when
            // `response_enc == Identity`.
            let frame = match encode_grpc_message_with_encoding(&resp_msg, response_enc) {
                Ok(f) => f,
                Err(e) => {
                    sender
                        .send_error(oxirpc_core::OxiRpcError::Proto(e.to_string()))
                        .await;
                    return;
                }
            };
            if sender.send_data(frame).await.is_err() {
                return;
            }
        }

        let _ = sender.send_trailers(ok_grpc_trailers()).await;
    });

    let mut response = http::Response::new(native_body);
    *response.status_mut() = http::StatusCode::OK;
    let response_headers = grpc_response_headers();
    for (k, v) in &response_headers {
        response.headers_mut().insert(k.clone(), v.clone());
    }
    // Advertise what this server supports.
    add_accept_encoding_header(response.headers_mut());
    // Tell the client how the stream payload will be encoded.
    if response_enc.is_compressing() {
        if let Ok(v) = http::HeaderValue::from_str(response_enc.as_str()) {
            response.headers_mut().insert("grpc-encoding", v);
        }
    }
    response
}

// ─────────────────────────────────────────────────────────────────────────────
// Compression helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Return the slice of [`CompressionEncoding`] values this build supports,
/// in preference order (gzip before zstd).
///
/// All feature-gating is concentrated here so callers need no `#[cfg]`.
fn supported_encodings() -> &'static [CompressionEncoding] {
    #[cfg(all(feature = "gzip", feature = "zstd"))]
    {
        &[CompressionEncoding::Gzip, CompressionEncoding::Zstd]
    }
    #[cfg(all(feature = "gzip", not(feature = "zstd")))]
    {
        &[CompressionEncoding::Gzip]
    }
    #[cfg(all(feature = "zstd", not(feature = "gzip")))]
    {
        &[CompressionEncoding::Zstd]
    }
    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    {
        &[]
    }
}

/// Build the `grpc-accept-encoding` advertisement string for this build
/// (e.g. `"gzip,identity"` or `"identity"` when no compression is compiled in).
fn server_accept_encoding_value() -> &'static str {
    #[cfg(all(feature = "gzip", feature = "zstd"))]
    {
        "gzip,zstd,identity"
    }
    #[cfg(all(feature = "gzip", not(feature = "zstd")))]
    {
        "gzip,identity"
    }
    #[cfg(all(feature = "zstd", not(feature = "gzip")))]
    {
        "zstd,identity"
    }
    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    {
        "identity"
    }
}

/// Negotiate the compression encoding for a response based on the client's
/// `grpc-accept-encoding` request header and the server's preferences.
///
/// When `server_prefs` is `Some` and its `send` slice is non-empty, that slice
/// is used in preference to the compiled-feature defaults.  Returns
/// [`CompressionEncoding::Identity`] if no compressing encoding is mutually
/// supported, or if no compression features are compiled in.
fn negotiate_response_encoding(
    headers: &http::HeaderMap,
    server_prefs: Option<&ServerCompressionPrefs>,
) -> CompressionEncoding {
    let accept = headers
        .get("grpc-accept-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let effective: &[CompressionEncoding] = match server_prefs {
        Some(p) if !p.send.is_empty() => &p.send,
        _ => supported_encodings(),
    };
    if effective.is_empty() {
        return CompressionEncoding::Identity;
    }
    CompressionEncoding::negotiate(accept, effective)
}

/// Parse the `grpc-encoding` request header to determine how the request body
/// is compressed.
///
/// Returns [`CompressionEncoding::Identity`] if the header is absent or unknown.
fn parse_request_encoding(headers: &http::HeaderMap) -> CompressionEncoding {
    headers
        .get("grpc-encoding")
        .and_then(|v| v.to_str().ok())
        .and_then(CompressionEncoding::from_str_opt)
        .unwrap_or(CompressionEncoding::Identity)
}

/// Insert the `grpc-accept-encoding` advertisement header into the given map.
fn add_accept_encoding_header(headers: &mut http::HeaderMap) {
    let val = server_accept_encoding_value();
    if let Ok(v) = http::HeaderValue::from_str(val) {
        headers.insert("grpc-accept-encoding", v);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Convert a `tonic_health::ServingStatus` to a proto `i32` for the response.
fn serving_status_to_response_i32(status: tonic_health::ServingStatus) -> i32 {
    match status {
        tonic_health::ServingStatus::Unknown => ServingStatusProto::Unknown as i32,
        tonic_health::ServingStatus::Serving => ServingStatusProto::Serving as i32,
        tonic_health::ServingStatus::NotServing => ServingStatusProto::NotServing as i32,
    }
}

/// Build a gRPC error response with HTTP 200 status (gRPC-over-H2 spec requirement).
///
/// The gRPC status code and message are carried in the response trailers,
/// not in the HTTP status line.
fn grpc_error(code: u32, message: &str) -> http::Response<NativeBody> {
    let mut resp = http::Response::new(error_response_body(code, message));
    *resp.status_mut() = http::StatusCode::OK;
    let headers = grpc_response_headers();
    for (k, v) in &headers {
        resp.headers_mut().insert(k.clone(), v.clone());
    }
    resp
}

//! Native gRPC-Web Tower middleware ([`NativeGrpcWebLayer`] / [`NativeGrpcWebService`]).
//!
//! This module provides a Pure-Rust, zero-unwrap Tower [`Layer`] + [`Service`]
//! pair that translates inbound gRPC-Web HTTP/1.1 requests into native gRPC
//! HTTP/2 requests for tonic services, and translates the gRPC responses back
//! to gRPC-Web responses (including the embedded trailer frame).
//!
//! It complements (and can eventually replace) the [`tonic_web::GrpcWebLayer`]
//! re-export while remaining fully interoperable.
//!
//! ## Pass-through behaviour
//!
//! Requests whose `Content-Type` header is **not** one of the four gRPC-Web
//! variants, and `OPTIONS` preflight requests, are passed through directly to
//! the inner service with the body converted to a [`tonic::body::Body`].
//!
//! ## Usage
//!
//! ```rust,no_run
//! use oxirpc_web::native::{native_grpc_web_layer, NativeGrpcWebLayer};
//! use tower::Layer;
//!
//! // Wrap a tonic service with the native translation layer.
//! let layer = native_grpc_web_layer();
//! // server.layer(layer).add_service(my_svc).serve(addr).await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Method, Request, Response};
use tonic::body::Body as TonicBody;
use tower::{Layer, Service};

use crate::translate::{translate_request, translate_response, GrpcWebContentType};

// ─── Layer ───────────────────────────────────────────────────────────────────

/// Tower [`Layer`] that translates inbound gRPC-Web HTTP/1.1 requests into
/// native gRPC HTTP/2 requests and translates gRPC responses back to gRPC-Web.
///
/// Non-gRPC-Web requests (including `OPTIONS` preflights) are passed through
/// unchanged.
///
/// Combine with [`crate::cors::CorsLayer`] for a fully-featured gRPC-Web
/// endpoint:
///
/// ```rust,no_run
/// use oxirpc_web::{
///     native::native_grpc_web_layer,
///     cors::{CorsLayer, CorsPolicy},
/// };
/// use tower_layer::Stack;
///
/// let layer = Stack::new(
///     native_grpc_web_layer(),
///     CorsLayer::new(CorsPolicy::new().allow_any_origin()),
/// );
/// ```
#[derive(Clone, Debug, Default)]
pub struct NativeGrpcWebLayer;

impl NativeGrpcWebLayer {
    /// Create a new [`NativeGrpcWebLayer`].
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for NativeGrpcWebLayer {
    type Service = NativeGrpcWebService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NativeGrpcWebService { inner }
    }
}

// ─── Service ─────────────────────────────────────────────────────────────────

/// Tower [`Service`] that performs native gRPC-Web ↔ gRPC translation.
///
/// Produced by [`NativeGrpcWebLayer`].
#[derive(Clone)]
pub struct NativeGrpcWebService<S> {
    inner: S,
}

impl<S> NativeGrpcWebService<S> {
    /// Create a [`NativeGrpcWebService`] wrapping `inner`.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ReqBody, RespBody> Service<Request<ReqBody>> for NativeGrpcWebService<S>
where
    S: Service<Request<TonicBody>, Response = Response<RespBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: http_body::Body<Data = Bytes> + Send + 'static,
    ReqBody::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + std::fmt::Display + Send + 'static,
    RespBody: http_body::Body<Data = Bytes> + Send + 'static,
    RespBody::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + std::fmt::Display + Send + 'static,
{
    type Response = Response<TonicBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // Detect gRPC-Web content-type.
        let content_type_opt: Option<GrpcWebContentType> = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(GrpcWebContentType::from_header);

        // Pass through OPTIONS (CORS preflight) and non-gRPC-Web requests.
        let Some(ct) = content_type_opt else {
            let mut inner = self.inner.clone();
            let tonic_req = req.map(TonicBody::new);
            return Box::pin(async move {
                let resp = inner.call(tonic_req).await?;
                // Re-wrap the response body into TonicBody so the output type is consistent.
                Ok(resp.map(TonicBody::new))
            });
        };

        if req.method() == Method::OPTIONS {
            let mut inner = self.inner.clone();
            let tonic_req = req.map(TonicBody::new);
            return Box::pin(async move {
                let resp = inner.call(tonic_req).await?;
                Ok(resp.map(TonicBody::new))
            });
        }

        // Confirmed gRPC-Web request — perform translation.
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Translate the gRPC-Web request to a native gRPC request.
            let grpc_req = match translate_request(req, ct).await {
                Ok(r) => r,
                Err(e) => {
                    // Return a 400 Bad Request on translation failure.
                    let body = TonicBody::new(http_body_util::Full::new(Bytes::from(format!(
                        "grpc-web translate error: {e}"
                    ))));
                    let resp = Response::builder()
                        .status(400)
                        .body(body)
                        .unwrap_or_else(|_| {
                            Response::new(TonicBody::new(http_body_util::Full::new(Bytes::new())))
                        });
                    return Ok(resp);
                }
            };

            // Call the inner gRPC service.
            let grpc_resp = inner.call(grpc_req).await?;

            // Translate the gRPC response back to gRPC-Web.
            Ok(translate_response(grpc_resp, ct).await)
        })
    }
}

// ─── Convenience constructors ─────────────────────────────────────────────────

/// Create a new [`NativeGrpcWebLayer`].
///
/// Convenience alias for [`NativeGrpcWebLayer::new`].
pub fn native_grpc_web_layer() -> NativeGrpcWebLayer {
    NativeGrpcWebLayer::new()
}

/// Wrap a tonic service directly with native gRPC-Web translation.
///
/// Equivalent to `NativeGrpcWebLayer::new().layer(inner)`.
pub fn native_grpc_web<S>(inner: S) -> NativeGrpcWebService<S> {
    NativeGrpcWebLayer::new().layer(inner)
}

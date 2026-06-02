//! Tower middleware that injects [`ServerCompressionPrefs`] into request extensions
//! and enforces the server's inbound compression acceptance policy.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::Request;
use oxirpc_core::wire::body::NativeBody;
use oxirpc_core::wire::server::{error_response_body, grpc_response_headers};
use oxirpc_core::{
    encoding::{accept_encoding_header_value, is_request_encoding_acceptable, CompressionEncoding},
    ServerCompressionPrefs,
};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

// ── Body conversion helpers ──────────────────────────────────────────────────

/// Convert a `Response<NativeBody>` to `Response<tonic::body::Body>` for use on
/// the tonic-Routes native path (routes return `tonic::body::Body`, not `NativeBody`).
pub(crate) fn native_body_to_tonic(
    resp: http::Response<NativeBody>,
) -> http::Response<tonic::body::Body> {
    resp.map(tonic::body::Body::new)
}

/// Identity converter for the registry path (already `Response<NativeBody>`).
pub(crate) fn native_body_identity(resp: http::Response<NativeBody>) -> http::Response<NativeBody> {
    resp
}

// ── Layer ────────────────────────────────────────────────────────────────────

/// Tower layer that injects [`ServerCompressionPrefs`] into each request's extensions
/// and enforces the server's inbound compression acceptance policy before dispatch.
///
/// Applied at the top of the native service stack so all services (health, reflect,
/// user-registered) see the server operator's compression preferences, and requests
/// with an unaccepted `grpc-encoding` are short-circuited with gRPC status 12
/// (UNIMPLEMENTED) + `grpc-accept-encoding` before the body is read.
///
/// The `C` parameter is a converter from `Response<NativeBody>` to the inner
/// service's response body type `RespB`.  Use [`CompressionPrefsLayer::for_native`]
/// for services returning `Response<NativeBody>` (registry path) or
/// [`CompressionPrefsLayer::for_routes`] for services returning
/// `Response<tonic::body::Body>` (tonic-Routes path).
#[derive(Clone)]
pub(crate) struct CompressionPrefsLayer<C> {
    prefs: Arc<ServerCompressionPrefs>,
    converter: C,
}

impl CompressionPrefsLayer<fn(http::Response<NativeBody>) -> http::Response<NativeBody>> {
    /// Create a layer for the native-registry path (services returning `Response<NativeBody>`).
    pub(crate) fn new(prefs: ServerCompressionPrefs) -> Self {
        Self {
            prefs: Arc::new(prefs),
            converter: native_body_identity,
        }
    }
}

impl CompressionPrefsLayer<fn(http::Response<NativeBody>) -> http::Response<tonic::body::Body>> {
    /// Create a layer for the tonic-Routes native path (services returning
    /// `Response<tonic::body::Body>`).
    pub(crate) fn for_routes(prefs: ServerCompressionPrefs) -> Self {
        Self {
            prefs: Arc::new(prefs),
            converter: native_body_to_tonic,
        }
    }
}

impl<C, S, RespB> Layer<S> for CompressionPrefsLayer<C>
where
    C: Fn(http::Response<NativeBody>) -> http::Response<RespB> + Clone + Send + Sync + 'static,
{
    type Service = CompressionPrefsService<S, C, RespB>;

    fn layer(&self, inner: S) -> Self::Service {
        CompressionPrefsService {
            inner,
            prefs: Arc::clone(&self.prefs),
            converter: self.converter.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ── Service ──────────────────────────────────────────────────────────────────

/// Tower service wrapper that validates inbound compression and injects compression
/// prefs into every request before forwarding to the inner service.
pub(crate) struct CompressionPrefsService<S, C, RespB> {
    inner: S,
    prefs: Arc<ServerCompressionPrefs>,
    converter: C,
    _phantom: std::marker::PhantomData<fn() -> RespB>,
}

impl<S: Clone, C: Clone, RespB> Clone for CompressionPrefsService<S, C, RespB> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            prefs: Arc::clone(&self.prefs),
            converter: self.converter.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, B, C, RespB> Service<Request<B>> for CompressionPrefsService<S, C, RespB>
where
    S: Service<Request<B>, Response = http::Response<RespB>>,
    S::Future: Send,
    C: Fn(http::Response<NativeBody>) -> http::Response<RespB> + Clone + Send + 'static,
    RespB: 'static,
{
    type Response = http::Response<RespB>;
    type Error = S::Error;
    type Future = CompressionValidationFuture<S::Future, C, RespB>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        // Parse the inbound grpc-encoding header.
        // None means the header was absent (treated as identity) OR contained an unknown token.
        let grpc_enc_opt: Option<CompressionEncoding> = req
            .headers()
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok())
            .and_then(CompressionEncoding::from_str_opt);

        // Absent header → identity → always acceptable. Only validate if header was present.
        let should_validate = req.headers().contains_key("grpc-encoding");

        if should_validate && !is_request_encoding_acceptable(grpc_enc_opt, &self.prefs.accept) {
            // Reject with status 12 (UNIMPLEMENTED) + grpc-accept-encoding header.
            let accept_val = accept_encoding_header_value(&self.prefs.accept);
            let body = error_response_body(
                12,
                &format!("unsupported compression encoding; server accepts: {accept_val}"),
            );
            let mut native_resp = http::Response::new(body);
            *native_resp.status_mut() = http::StatusCode::OK;
            let hdrs = grpc_response_headers();
            for (k, v) in &hdrs {
                native_resp.headers_mut().insert(k.clone(), v.clone());
            }
            if let Ok(v) = http::HeaderValue::from_str(&accept_val) {
                native_resp.headers_mut().insert("grpc-accept-encoding", v);
            }
            let converted = (self.converter)(native_resp);
            return CompressionValidationFuture::Rejected {
                response: Some(converted),
            };
        }

        // Inject prefs for downstream response-encoding negotiation.
        req.extensions_mut().insert(Arc::clone(&self.prefs));
        CompressionValidationFuture::Forwarded {
            inner: self.inner.call(req),
            _phantom: std::marker::PhantomData,
        }
    }
}

pin_project! {
    /// Future type for [`CompressionPrefsService`].
    ///
    /// `Rejected` returns an immediate status-12 response without calling the inner service.
    /// `Forwarded` polls the inner service's future directly (zero overhead on the hot path).
    #[project = CompressionValidationFutureProj]
    pub(crate) enum CompressionValidationFuture<F, C, RespB> {
        Rejected { response: Option<http::Response<RespB>> },
        Forwarded { #[pin] inner: F, _phantom: std::marker::PhantomData<(C, RespB)> },
    }
}

impl<F, C, RespB, E> std::future::Future for CompressionValidationFuture<F, C, RespB>
where
    F: std::future::Future<Output = Result<http::Response<RespB>, E>>,
{
    type Output = Result<http::Response<RespB>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            CompressionValidationFutureProj::Rejected { response } => Poll::Ready(Ok(response
                .take()
                .expect("CompressionValidationFuture polled after completion"))),
            CompressionValidationFutureProj::Forwarded { inner, .. } => inner.poll(cx),
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    use tower::ServiceExt;

    /// A mock inner service that always returns HTTP 200 with an empty NativeBody
    /// and a successful gRPC trailer (grpc-status: 0).
    fn mock_ok_service() -> impl tower::Service<
        http::Request<NativeBody>,
        Response = http::Response<NativeBody>,
        Error = Infallible,
        Future = std::future::Ready<Result<http::Response<NativeBody>, Infallible>>,
    > {
        tower::service_fn(|_req: http::Request<NativeBody>| {
            use oxirpc_core::wire::server::ok_grpc_trailers;
            let body = NativeBody::empty().with_trailers(ok_grpc_trailers());
            let resp = http::Response::builder()
                .status(http::StatusCode::OK)
                .body(body)
                .expect("mock response");
            std::future::ready(Ok::<_, Infallible>(resp))
        })
    }

    fn make_prefs_layer_identity_only(
    ) -> CompressionPrefsLayer<fn(http::Response<NativeBody>) -> http::Response<NativeBody>> {
        CompressionPrefsLayer::new(oxirpc_core::ServerCompressionPrefs {
            send: vec![],
            accept: vec![CompressionEncoding::Identity],
        })
    }

    fn make_prefs_layer_default(
    ) -> CompressionPrefsLayer<fn(http::Response<NativeBody>) -> http::Response<NativeBody>> {
        CompressionPrefsLayer::new(oxirpc_core::ServerCompressionPrefs {
            send: vec![],
            accept: vec![], // empty → use decodable_encodings() defaults
        })
    }

    /// Requests without grpc-encoding are always forwarded (treated as identity).
    #[tokio::test]
    async fn no_grpc_encoding_header_forwarded() {
        let layer = make_prefs_layer_identity_only();
        let svc = layer.layer(mock_ok_service());
        let req = http::Request::builder()
            .uri("/test.Service/Method")
            .body(NativeBody::empty())
            .expect("request");
        let resp = svc.oneshot(req).await.expect("call");
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    /// Requests with grpc-encoding: identity are always forwarded.
    #[tokio::test]
    async fn identity_encoding_header_always_forwarded() {
        let layer = make_prefs_layer_identity_only();
        let svc = layer.layer(mock_ok_service());
        let req = http::Request::builder()
            .uri("/test.Service/Method")
            .header("grpc-encoding", "identity")
            .body(NativeBody::empty())
            .expect("request");
        let resp = svc.oneshot(req).await.expect("call");
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    /// Requests with an unknown token in grpc-encoding are rejected with grpc-status 12.
    #[tokio::test]
    async fn unknown_encoding_header_rejected_with_status_12() {
        use http_body_util::BodyExt as _;

        let layer = make_prefs_layer_default();
        let svc = layer.layer(mock_ok_service());
        let req = http::Request::builder()
            .uri("/test.Service/Method")
            .header("grpc-encoding", "totally-made-up-encoding")
            .body(NativeBody::empty())
            .expect("request");
        let resp = svc.oneshot(req).await.expect("call");
        assert_eq!(resp.status(), http::StatusCode::OK);
        // Must carry grpc-accept-encoding header.
        assert!(
            resp.headers().contains_key("grpc-accept-encoding"),
            "response must carry grpc-accept-encoding"
        );
        // Drain body and check trailers for grpc-status: 12.
        let (_parts, body) = resp.into_parts();
        let collected = body.collect().await.expect("collect");
        let trailers = collected.trailers().cloned().unwrap_or_default();
        let grpc_status = trailers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("missing");
        assert_eq!(
            grpc_status, "12",
            "unknown encoding must yield grpc-status: 12 (UNIMPLEMENTED), got: {grpc_status}"
        );
    }

    /// Requests with grpc-encoding: gzip rejected when accept=[Identity].
    #[tokio::test]
    async fn inbound_unaccepted_encoding_returns_status_12() {
        use http_body_util::BodyExt as _;

        let layer = make_prefs_layer_identity_only();
        let svc = layer.layer(mock_ok_service());
        let req = http::Request::builder()
            .uri("/test.Service/Method")
            .header("grpc-encoding", "gzip")
            .body(NativeBody::empty())
            .expect("request");
        let resp = svc.oneshot(req).await.expect("call");
        assert_eq!(
            resp.status(),
            http::StatusCode::OK,
            "gRPC rejections must use HTTP 200"
        );
        // Must carry grpc-accept-encoding header.
        let accept_hdr = resp
            .headers()
            .get("grpc-accept-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            !accept_hdr.is_empty(),
            "response must carry grpc-accept-encoding on rejection"
        );
        assert!(
            accept_hdr.contains("identity"),
            "grpc-accept-encoding must include identity, got: {accept_hdr}"
        );
        // Drain body and check trailers for grpc-status: 12.
        let (_parts, body) = resp.into_parts();
        let collected = body.collect().await.expect("collect");
        let trailers = collected.trailers().cloned().unwrap_or_default();
        let grpc_status = trailers
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("missing");
        assert_eq!(
            grpc_status, "12",
            "unaccepted gzip encoding must yield grpc-status: 12 (UNIMPLEMENTED), got: {grpc_status}"
        );
    }
}

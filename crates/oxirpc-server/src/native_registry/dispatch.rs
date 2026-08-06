//! [`RegistryService`] — a `tower::Service` that dispatches by service prefix.
//!
//! Path format: `/<service_name>/<method_name>`.  The dispatcher extracts the
//! service name by stripping the leading `/` then taking the part before the
//! next `/`.
//!
//! Before any of that, the request's `content-type` header is validated: per
//! the gRPC-over-HTTP/2 spec, a request whose `content-type` does not begin
//! with `application/grpc` is rejected with HTTP 415 (Unsupported Media
//! Type) rather than being routed to a service — see the internal
//! `reject_non_grpc_content_type` helper. This keeps a plain HTTP client, a
//! health-check probe, or a misconfigured reverse proxy from being fed
//! straight into the gRPC frame decoder.
//!
//! Dispatch result:
//! 1. Service found  →  clone its `BoxedNativeService` and call it.
//! 2. Not found, fallback set  →  call the fallback.
//! 3. Not found, no fallback  →  HTTP 200 + `grpc-status: 12`
//!    (`UNIMPLEMENTED`, "unknown service") using native wire helpers.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use oxirpc_core::interceptor::AsyncInterceptor;
use oxirpc_core::message::Request as IRequest;
use oxirpc_core::metadata::Metadata;
use oxirpc_core::wire::{
    header::is_grpc_content_type,
    server::{error_response_body, grpc_response_headers},
    NativeBody,
};
use tower::Service;

use super::service_set::BoxedNativeService;

// ─── Shared service slot ──────────────────────────────────────────────────────

/// A thread-safe, cloneable slot for a single `BoxedNativeService`.
///
/// `std::sync::Mutex<T>` is `Sync` whenever `T: Send`, making
/// `Arc<Mutex<BoxedNativeService<B>>>` both `Send` and `Sync`.
/// The lock is held only for the duration of cloning the inner service —
/// never across an `.await`.
type ServiceSlot<B> = Arc<Mutex<BoxedNativeService<B>>>;

// ─── Inner ────────────────────────────────────────────────────────────────────

/// The immutable snapshot of the registry, shared via `Arc`.
///
/// Each service is stored in a `ServiceSlot<B>` so the `HashMap` is
/// `Send + Sync` regardless of `B`'s `Sync`ness.
struct Inner<B> {
    services: HashMap<&'static str, ServiceSlot<B>>,
    fallback: Option<ServiceSlot<B>>,
    async_interceptor: Option<Arc<dyn AsyncInterceptor>>,
}

// SAFETY analysis (no unsafe needed):
// - `Mutex<BoxedNativeService<B>>` is `Sync` when `BoxedNativeService<B>: Send`.
// - `BoxedNativeService<B>` = `BoxCloneService<Request<B>, Response<NativeBody>, Infallible>`.
// - `BoxCloneService<T,U,E>` is `Send` when `T,U,E: Send`, which is satisfied
//   by our where-clause `B: Send`.
// Therefore `Inner<B>: Send + Sync` without any unsafe.

// ─── RegistryService ─────────────────────────────────────────────────────────

/// A `Clone`-able `tower::Service` that routes by gRPC service-name prefix.
///
/// Obtained via [`super::registry::NativeServiceRegistry::into_service`].
/// Each clone shares the same inner [`Arc`] — cheap and lock-free at the
/// Arc level; individual service lookups require a brief non-async mutex lock.
pub struct RegistryService<B = NativeBody> {
    inner: Arc<Inner<B>>,
}

impl<B> Clone for RegistryService<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<B> RegistryService<B> {
    pub(super) fn new(
        services: HashMap<&'static str, BoxedNativeService<B>>,
        fallback: Option<BoxedNativeService<B>>,
        async_interceptor: Option<Arc<dyn AsyncInterceptor>>,
    ) -> Self {
        let slotted: HashMap<&'static str, ServiceSlot<B>> = services
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        let fallback_slot = fallback.map(|f| Arc::new(Mutex::new(f)));
        Self {
            inner: Arc::new(Inner {
                services: slotted,
                fallback: fallback_slot,
                async_interceptor,
            }),
        }
    }
}

// ─── Service impl ─────────────────────────────────────────────────────────────

impl<B> Service<Request<B>> for RegistryService<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<NativeBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        // Reject non-gRPC requests before doing any dispatch work (route
        // lookup, interceptor invocation, or handing the body to the frame
        // decoder). Per the gRPC-over-HTTP/2 spec: "If Content-Type does not
        // begin with 'application/grpc', gRPC servers SHOULD respond with
        // HTTP status of 415 (Unsupported Media Type)." A plain HTTP client,
        // a health-check probe, or a misconfigured reverse proxy must never
        // reach the frame decoder or a service handler.
        if let Some(rejection) = reject_non_grpc_content_type(req.headers()) {
            return Box::pin(async move { Ok(rejection) });
        }

        let path = req.uri().path();

        // Parse: strip leading '/', then split on '/' to get service name.
        // "/pkg.Svc/Method" → service_name = "pkg.Svc"
        // "/nomethod"       → no '/' after strip → unimplemented
        let service_name: Option<String> = {
            let stripped = path.strip_prefix('/').unwrap_or(path);
            stripped.split_once('/').map(|(svc, _)| svc.to_owned())
        };

        // Clone the matching boxed service (or fallback) *synchronously* before
        // the async block, holding the Mutex for the minimum time — not across
        // any await point.
        let resolved: Option<BoxedNativeService<B>> = service_name.as_deref().and_then(|name| {
            self.inner
                .services
                .get(name)
                .and_then(|slot| slot.lock().ok().map(|guard| guard.clone()))
        });

        let fallback_clone: Option<BoxedNativeService<B>> = if resolved.is_none() {
            self.inner
                .fallback
                .as_ref()
                .and_then(|slot| slot.lock().ok().map(|guard| guard.clone()))
        } else {
            None
        };

        let interceptor = self.inner.async_interceptor.clone();

        Box::pin(async move {
            // Path had no slash after stripping the leading '/' → unimplemented.
            if service_name.is_none() {
                return Ok(unimplemented_response());
            }

            let mut req = req;
            if let Some(ref interceptor) = interceptor {
                let irequest = irequest_from_headers(req.headers());
                match interceptor.intercept_async(irequest).await {
                    Ok(updated) => apply_metadata_to_headers(&updated, req.headers_mut()),
                    Err(status) => return Ok(status_response(&status)),
                }
            }

            if let Some(mut boxed) = resolved {
                return boxed.call(req).await;
            }

            if let Some(mut fb) = fallback_clone {
                return fb.call(req).await;
            }

            // No match, no fallback → UNIMPLEMENTED.
            Ok(unimplemented_response())
        })
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Validate the incoming request's `content-type` header against the gRPC
/// wire format.
///
/// Returns `Some(response)` — an HTTP 415 rejection built by
/// [`unsupported_media_type_response`] — when the header is missing or does
/// not satisfy [`is_grpc_content_type`]; returns `None` when the request
/// should proceed to normal dispatch.
fn reject_non_grpc_content_type(headers: &HeaderMap) -> Option<Response<NativeBody>> {
    let content_type = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if content_type.is_some_and(is_grpc_content_type) {
        return None;
    }
    Some(unsupported_media_type_response(content_type))
}

/// Build a plain-text HTTP 415 response rejecting a non-gRPC `content-type`.
///
/// Per the gRPC-over-HTTP/2 spec: "This will prevent other HTTP/2 clients
/// from interpreting a gRPC error response, which uses status 200 (OK), as
/// successful." The body is plain text (not gRPC-framed) and carries no
/// `grpc-status` trailer — unlike [`unimplemented_response`] — because the
/// peer has not demonstrated that it speaks gRPC at all.
fn unsupported_media_type_response(actual: Option<&str>) -> Response<NativeBody> {
    let message = match actual {
        Some(ct) => format!(
            "invalid gRPC request content-type {ct:?}; expected 'application/grpc' \
             or a variant such as 'application/grpc+proto'"
        ),
        None => "missing content-type header; gRPC requests require \
                  'content-type: application/grpc[+proto|+json]'"
            .to_owned(),
    };
    let mut resp = Response::new(NativeBody::once(Bytes::from(message)));
    *resp.status_mut() = http::StatusCode::UNSUPPORTED_MEDIA_TYPE;
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

/// Build a gRPC UNIMPLEMENTED (status code 12) response using native wire helpers.
///
/// Returns HTTP 200 with `grpc-status: 12` in the response trailers, as required
/// by the gRPC-over-HTTP/2 spec.
fn unimplemented_response() -> Response<NativeBody> {
    // gRPC status 12 = UNIMPLEMENTED
    let body = error_response_body(12, "unknown service");
    let mut resp = Response::new(body);
    *resp.status_mut() = http::StatusCode::OK;
    let headers = grpc_response_headers();
    for (k, v) in &headers {
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp
}

/// Build a metadata-only `Request<()>` from incoming request headers.
fn irequest_from_headers(headers: &HeaderMap) -> IRequest<()> {
    let mut req = IRequest::new(());
    let md = req.metadata_mut();
    for (name, value) in headers.iter() {
        let key = name.as_str();
        if Metadata::is_binary_key(key) {
            if let Ok(s) = value.to_str() {
                if let Ok(raw) = Metadata::decode_wire_bin(s) {
                    let _ = md.insert_bin(key, &raw);
                }
            }
        } else if let Ok(s) = value.to_str() {
            let _ = md.insert(key, s);
        }
    }
    req
}

/// Merge the (possibly mutated) metadata of `req` back into `headers`.
fn apply_metadata_to_headers(req: &IRequest<()>, headers: &mut HeaderMap) {
    for (key, value) in req.metadata().to_wire() {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(key.as_bytes()),
            http::HeaderValue::from_str(&value),
        ) {
            headers.remove(&name);
            headers.append(name, val);
        }
    }
}

/// Build a gRPC error response from a `Status`, mirroring `unimplemented_response`.
fn status_response(status: &oxirpc_core::rpc::Status) -> Response<NativeBody> {
    let code = status.code.as_i32().max(0) as u32;
    let body = error_response_body(code, &status.message);
    let mut resp = Response::new(body);
    *resp.status_mut() = http::StatusCode::OK;
    let headers = grpc_response_headers();
    for (k, v) in &headers {
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── reject_non_grpc_content_type ─────────────────────────────────────────

    #[test]
    fn accepts_bare_and_variant_grpc_content_types() {
        for ct in [
            "application/grpc",
            "application/grpc+proto",
            "application/grpc+json",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                ct.parse().expect("header value"),
            );
            assert!(
                reject_non_grpc_content_type(&headers).is_none(),
                "{ct:?} must be accepted, not rejected"
            );
        }
    }

    #[test]
    fn rejects_missing_content_type_header() {
        let headers = HeaderMap::new();
        let resp =
            reject_non_grpc_content_type(&headers).expect("missing content-type must be rejected");
        assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn rejects_plain_http_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "text/html".parse().expect("header value"),
        );
        let resp = reject_non_grpc_content_type(&headers).expect("text/html must be rejected");
        assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        // The peer never demonstrated it speaks gRPC — no grpc-status trailer.
        assert!(
            !resp.headers().contains_key("grpc-status"),
            "a 415 rejection must not carry a grpc-status header"
        );
    }

    #[test]
    fn rejects_json_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/json".parse().expect("header value"),
        );
        assert!(reject_non_grpc_content_type(&headers).is_some());
    }

    #[tokio::test]
    async fn unsupported_media_type_response_body_names_the_actual_content_type() {
        use http_body_util::BodyExt as _;

        let resp = unsupported_media_type_response(Some("text/html"));
        assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let collected = resp.into_body().collect().await.expect("collect body");
        let text = String::from_utf8_lossy(&collected.to_bytes()).into_owned();
        assert!(
            text.contains("text/html"),
            "rejection message must name the actual content-type, got: {text}"
        );
    }

    #[tokio::test]
    async fn unsupported_media_type_response_body_explains_missing_header() {
        use http_body_util::BodyExt as _;

        let resp = unsupported_media_type_response(None);
        assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let collected = resp.into_body().collect().await.expect("collect body");
        let text = String::from_utf8_lossy(&collected.to_bytes()).into_owned();
        assert!(
            text.contains("missing"),
            "rejection message must explain the header is missing, got: {text}"
        );
    }

    // ─── RegistryService::call end-to-end (via the tower Service impl) ────────

    #[tokio::test]
    async fn call_rejects_request_with_wrong_content_type_before_dispatch() {
        use std::convert::Infallible;
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::task::{Context, Poll};
        use tonic::server::NamedService;

        /// A mock service that records whether it was ever called.
        #[derive(Clone)]
        struct RecordingService(Arc<AtomicBool>);

        impl NamedService for RecordingService {
            const NAME: &'static str = "mock.Service";
        }

        impl Service<Request<tonic::body::Body>> for RecordingService {
            type Response = Response<NativeBody>;
            type Error = Infallible;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
                self.0.store(true, Ordering::SeqCst);
                Box::pin(async move { Ok(Response::new(NativeBody::empty())) })
            }
        }

        let called = Arc::new(AtomicBool::new(false));
        let mut services: HashMap<&'static str, ServiceSlot<tonic::body::Body>> = HashMap::new();
        services.insert(
            RecordingService::NAME,
            Arc::new(Mutex::new(BoxedNativeService::new(RecordingService(
                Arc::clone(&called),
            )))),
        );
        let mut svc = RegistryService {
            inner: Arc::new(Inner {
                services,
                fallback: None,
                async_interceptor: None,
            }),
        };

        let req = Request::builder()
            .uri("/mock.Service/Method")
            .header("content-type", "text/plain")
            .body(tonic::body::Body::default())
            .expect("request");

        let resp = svc.call(req).await.expect("infallible");
        assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(
            !called.load(Ordering::SeqCst),
            "a non-gRPC request must never reach the service handler"
        );
    }
}

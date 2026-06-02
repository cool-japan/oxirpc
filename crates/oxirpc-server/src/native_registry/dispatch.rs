//! [`RegistryService`] — a `tower::Service` that dispatches by service prefix.
//!
//! Path format: `/<service_name>/<method_name>`.  The dispatcher extracts the
//! service name by stripping the leading `/` then taking the part before the
//! next `/`.
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
use http::{Request, Response};
use oxirpc_core::wire::{
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

        Box::pin(async move {
            // Path had no slash after stripping the leading '/' → unimplemented.
            if service_name.is_none() {
                return Ok(unimplemented_response());
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

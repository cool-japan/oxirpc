//! Integration tests for the `OxiNamedService` trait and related additions.
//!
//! Covers:
//! 1. Direct implementation of `OxiNamedService` (no tonic dependency).
//! 2. Blanket impl: a type implementing `tonic::server::NamedService` automatically
//!    satisfies `OxiNamedService`.
//! 3. `NativeServiceRegistry::add_service` accepts any `OxiNamedService` implementor.
//! 4. Compile-time doc-test anchor for `health_service()` (guarded by feature flag).

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::{Request, Response};
use oxirpc_core::wire::{
    server::{error_response_body, grpc_response_headers},
    NativeBody,
};
use oxirpc_server::OxiNamedService;
use tower::Service;

// ─── Test 1: Direct OxiNamedService implementation ───────────────────────────

/// A type that implements `OxiNamedService` directly — no tonic dependency.
struct DirectService;

impl OxiNamedService for DirectService {
    const NAME: &'static str = "direct.v1.DirectService";
}

#[test]
fn oxi_named_service_trait_can_be_implemented_directly() {
    assert_eq!(DirectService::NAME, "direct.v1.DirectService");
}

// ─── Test 2: Blanket impl covers tonic::server::NamedService ─────────────────

/// A type that implements `tonic::server::NamedService` but *not* `OxiNamedService`
/// explicitly — the blanket impl must bridge the gap.
#[derive(Clone)]
struct TonicStyleService;

impl tonic::server::NamedService for TonicStyleService {
    const NAME: &'static str = "tonic.v1.TonicStyleService";
}

impl Service<Request<tonic::body::Body>> for TonicStyleService {
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
        Box::pin(async { Ok(tonic::Status::ok("ok").into_http()) })
    }
}

// ─── Native response helper ───────────────────────────────────────────────────

/// Build an HTTP 200 + grpc-status: 0 (OK) native response used by CallRecorder.
fn ok_native_response() -> Response<NativeBody> {
    let body = error_response_body(0, "");
    let mut resp = Response::new(body);
    *resp.status_mut() = http::StatusCode::OK;
    let headers = grpc_response_headers();
    for (k, v) in &headers {
        resp.headers_mut().append(k.clone(), v.clone());
    }
    resp
}

/// Helper that requires only `OxiNamedService`.
fn get_name<S: OxiNamedService>() -> &'static str {
    S::NAME
}

#[test]
fn blanket_impl_covers_tonic_named_service() {
    // `TonicStyleService` only impl `tonic::server::NamedService`; the blanket
    // impl means it also satisfies `OxiNamedService`.
    assert_eq!(
        get_name::<TonicStyleService>(),
        "tonic.v1.TonicStyleService"
    );
}

// ─── Test 3: NativeServiceRegistry accepts OxiNamedService ───────────────────
//
// `NativeServiceRegistry::add_service` now uses `OxiNamedService` as its name
// bound.  We verify that it dispatches correctly when called with a tonic service
// (which satisfies the bound via the blanket impl).

#[cfg(feature = "native")]
#[tokio::test]
async fn native_service_registry_accepts_oxi_named_service() {
    use oxirpc_server::native_registry::NativeServiceRegistry;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    /// Minimal service that records whether it was called.
    #[derive(Clone)]
    struct CallRecorder {
        called: Arc<AtomicBool>,
    }

    impl tonic::server::NamedService for CallRecorder {
        const NAME: &'static str = "test.v1.CallRecorder";
    }

    impl Service<Request<tonic::body::Body>> for CallRecorder {
        type Response = Response<NativeBody>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
            self.called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(ok_native_response()) })
        }
    }

    let called_flag = Arc::new(AtomicBool::new(false));
    let recorder = CallRecorder {
        called: Arc::clone(&called_flag),
    };

    let registry = NativeServiceRegistry::new().add_service(recorder);
    let mut dispatch = registry.into_service();

    let req = Request::builder()
        .uri("/test.v1.CallRecorder/Ping")
        .body(tonic::body::Body::default())
        .expect("request build");
    dispatch.call(req).await.expect("infallible call");

    assert!(
        called_flag.load(Ordering::SeqCst),
        "registry did not dispatch to the CallRecorder service"
    );
}

// ─── Test 4: health_service() compile anchor (feature = "health") ─────────────

/// Compile-time check: `ServerBuilder::health_service()` is callable and returns
/// `ServeReady`.  Requires the `health` feature on `oxirpc-server`.
///
/// ```rust,no_run
/// # #[cfg(feature = "health")]
/// # async fn run() -> Result<(), oxirpc_server::OxiRpcError> {
/// let _ready = oxirpc_server::ServerBuilder::new().health_service();
/// # Ok(()) }
/// ```
#[cfg(feature = "health")]
#[test]
fn health_service_returns_serve_ready() {
    // `health_service()` → `ServeReady` — we just need this to compile and
    // type-check; no network I/O happens in this unit test.
    let _ready: oxirpc_server::ServeReady = oxirpc_server::ServerBuilder::new().health_service();
}

//! Integration tests for `NativeServiceRegistry` and `RegistryService`.
//!
//! Covers dispatch correctness (service found / not found / fallback),
//! path-parsing edge cases, `names()` iterator, cloneability, and an end-to-end
//! round-trip using the native H2 transport.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use http::{Request, Response};
use http_body_util::BodyExt;
use oxirpc_core::wire::{
    server::{error_response_body, grpc_response_headers},
    NativeBody,
};
use tonic::server::NamedService;
use tower::Service;

use oxirpc_server::native_registry::NativeServiceRegistry;

// ─── Minimal mock service ─────────────────────────────────────────────────────

/// A test stub that always responds with `grpc-status: 0` (OK).
#[derive(Clone)]
struct MockService {
    call_count: Arc<AtomicUsize>,
}

impl MockService {
    fn new(_name: &'static str) -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl NamedService for MockService {
    const NAME: &'static str = "mock.Service";
}

impl Service<Request<tonic::body::Body>> for MockService {
    type Response = Response<NativeBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
        let count = Arc::clone(&self.call_count);
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(ok_native_response())
        })
    }
}

/// A second mock service with a distinct NAME.
#[derive(Clone)]
struct MockServiceB {
    call_count: Arc<AtomicUsize>,
}

impl MockServiceB {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl NamedService for MockServiceB {
    const NAME: &'static str = "mock.ServiceB";
}

impl Service<Request<tonic::body::Body>> for MockServiceB {
    type Response = Response<NativeBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
        let count = Arc::clone(&self.call_count);
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(ok_native_response())
        })
    }
}

// ─── Native response helpers ──────────────────────────────────────────────────

/// Build an HTTP 200 + grpc-status: 0 (OK) response with `NativeBody`.
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

// ─── Helper: call RegistryService directly ───────────────────────────────────

async fn call_path(
    svc: &mut oxirpc_server::RegistryService<tonic::body::Body>,
    path: &str,
) -> Response<NativeBody> {
    // A valid gRPC content-type is required since `RegistryService::call`
    // rejects anything else with HTTP 415 before it ever reaches routing —
    // these tests exercise routing/dispatch, not content-type validation.
    let req = Request::builder()
        .uri(path)
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::default())
        .expect("request");
    svc.call(req).await.expect("infallible")
}

/// Collect the body trailers and read `grpc-status` from them.
///
/// The native gRPC wire format places status codes in HTTP/2 trailing headers
/// (sent after the data frames), not in the initial response headers.
async fn grpc_status_from_response(resp: Response<NativeBody>) -> Option<i32> {
    let collected = resp.into_body().collect().await.ok()?;
    let trailers = collected.trailers().cloned().unwrap_or_default();
    trailers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
}

// ─── 1. empty_registry_returns_unimplemented ──────────────────────────────────

#[tokio::test]
async fn empty_registry_returns_unimplemented() {
    let registry = NativeServiceRegistry::new();
    let mut svc = registry.into_service();

    let resp = call_path(&mut svc, "/some.Service/Method").await;
    // gRPC UNIMPLEMENTED == 12 (in body trailers per native gRPC wire format)
    assert_eq!(
        grpc_status_from_response(resp).await,
        Some(12),
        "expected grpc-status: 12 (UNIMPLEMENTED)"
    );
}

// ─── 2. single_service_dispatches_correctly ───────────────────────────────────

#[tokio::test]
async fn single_service_dispatches_correctly() {
    let mock = MockService::new("mock.Service");
    let counter = Arc::clone(&mock.call_count);

    let registry = NativeServiceRegistry::new().add_service(mock);
    let mut svc = registry.into_service();

    call_path(&mut svc, "/mock.Service/SomeMethod").await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

// ─── 3. unknown_service_returns_unimplemented ─────────────────────────────────

#[tokio::test]
async fn unknown_service_returns_unimplemented() {
    let mock = MockService::new("mock.Service");
    let registry = NativeServiceRegistry::new().add_service(mock);
    let mut svc = registry.into_service();

    let resp = call_path(&mut svc, "/wrong.Service/Method").await;
    assert_eq!(
        grpc_status_from_response(resp).await,
        Some(12),
        "unknown service must return grpc-status: 12"
    );
}

// ─── 4. multi_service_dispatches_independently ────────────────────────────────

#[tokio::test]
async fn multi_service_dispatches_independently() {
    let mock_a = MockService::new("mock.Service");
    let mock_b = MockServiceB::new();
    let count_a = Arc::clone(&mock_a.call_count);
    let count_b = Arc::clone(&mock_b.call_count);

    let registry = NativeServiceRegistry::new()
        .add_service(mock_a)
        .add_service(mock_b);
    let mut svc = registry.into_service();

    call_path(&mut svc, "/mock.Service/MethodA").await;
    call_path(&mut svc, "/mock.ServiceB/MethodB").await;
    call_path(&mut svc, "/mock.ServiceB/MethodB").await;

    assert_eq!(count_a.load(Ordering::SeqCst), 1, "ServiceA called once");
    assert_eq!(count_b.load(Ordering::SeqCst), 2, "ServiceB called twice");
}

// ─── 5. path_parsing_rejects_no_slash ────────────────────────────────────────

#[tokio::test]
async fn path_parsing_rejects_no_slash() {
    // "/noservice" — stripped = "noservice", no '/' → unimplemented
    let mock = MockService::new("noservice");
    let registry = NativeServiceRegistry::new().add_service(mock);
    let mut svc = registry.into_service();

    let resp = call_path(&mut svc, "/noservice").await;
    assert_eq!(
        grpc_status_from_response(resp).await,
        Some(12),
        "path with no method slash must return UNIMPLEMENTED"
    );
}

// ─── 5b. wrong_content_type_returns_415_without_dispatching ──────────────────

/// Regression test: a request whose `content-type` does not begin with
/// `application/grpc` must be rejected with HTTP 415 *before* it reaches any
/// registered service — not silently routed and fed into the frame decoder.
#[tokio::test]
async fn wrong_content_type_returns_415_without_dispatching() {
    let mock = MockService::new("mock.Service");
    let counter = Arc::clone(&mock.call_count);

    let registry = NativeServiceRegistry::new().add_service(mock);
    let mut svc = registry.into_service();

    let req = Request::builder()
        .uri("/mock.Service/SomeMethod")
        .header("content-type", "text/html")
        .body(tonic::body::Body::default())
        .expect("request");
    let resp = svc.call(req).await.expect("infallible");

    assert_eq!(
        resp.status(),
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "non-gRPC content-type must yield HTTP 415"
    );
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "the registered service must never be called for a non-gRPC request"
    );
}

/// Same as above, but with no `content-type` header at all (a bare plain-HTTP
/// client, or a health-check probe that never sets one).
#[tokio::test]
async fn missing_content_type_returns_415_without_dispatching() {
    let mock = MockService::new("mock.Service");
    let counter = Arc::clone(&mock.call_count);

    let registry = NativeServiceRegistry::new().add_service(mock);
    let mut svc = registry.into_service();

    let req = Request::builder()
        .uri("/mock.Service/SomeMethod")
        .body(tonic::body::Body::default())
        .expect("request");
    let resp = svc.call(req).await.expect("infallible");

    assert_eq!(resp.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

// ─── 6. name_iter_returns_all_registered ─────────────────────────────────────

#[test]
fn name_iter_returns_all_registered() {
    let registry = NativeServiceRegistry::new()
        .add_service(MockService::new("mock.Service"))
        .add_service(MockServiceB::new());

    let mut names: Vec<&'static str> = registry.names().collect();
    names.sort_unstable();

    assert_eq!(names, ["mock.Service", "mock.ServiceB"]);
}

// ─── 7. registry_service_is_cloneable ────────────────────────────────────────

#[tokio::test]
async fn registry_service_is_cloneable() {
    let mock = MockService::new("mock.Service");
    let counter = Arc::clone(&mock.call_count);

    let registry = NativeServiceRegistry::new().add_service(mock);
    let svc = registry.into_service();

    // Clone and call both; they share the same underlying counter.
    let mut svc1 = svc.clone();
    let mut svc2 = svc.clone();

    call_path(&mut svc1, "/mock.Service/Method1").await;
    call_path(&mut svc2, "/mock.Service/Method2").await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "both clones share the service"
    );
}

// ─── 8. serve_native_registry_e2e ────────────────────────────────────────────

#[cfg(feature = "native")]
#[tokio::test(flavor = "multi_thread")]
async fn serve_native_registry_e2e() {
    use oxirpc_health::{HealthBuilder, ServingStatus};
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    // Build a health service and register it.
    let (health_svc, mut health_handle) = HealthBuilder::new()
        .register("e2e.TestService", ServingStatus::Serving)
        .build_native()
        .await;
    health_handle.set_serving("e2e.TestService").await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let registry = NativeServiceRegistry::new().add_service(health_svc);

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        // We add a dummy service to produce a ServeReady; the actual dispatch
        // goes through the registry, not the tonic router.
        let dummy = MockService::new("mock.Service");
        oxirpc_server::ServerBuilder::new()
            .add_native_service(dummy)
            .serve_native_registry_with_listener_shutdown(listener, registry, async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    // Give the server a moment to be ready.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("channel")
        .connect()
        .await
        .expect("connect");

    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "e2e.TestService".to_string(),
        })
        .await
        .expect("health check");
    // 1 = SERVING
    assert_eq!(resp.into_inner().status, 1);

    tx.send(()).ok();
    server.await.ok();
}

// ─── 9. registry_accepts_native_body_request ─────────────────────────────────

/// Verify the registry handles a `NativeBody`-typed request when the registry
/// is typed with `NativeBody` as the request body parameter.
#[tokio::test]
async fn registry_accepts_native_body_request() {
    use http::Request;
    use oxirpc_core::wire::NativeBody;
    use std::sync::atomic::AtomicBool;
    use tower::ServiceExt;

    /// A minimal native-body service — accepts `Request<NativeBody>` and
    /// returns `Response<NativeBody>`.
    #[derive(Clone)]
    struct NativeMock {
        called: Arc<AtomicBool>,
    }

    impl tonic::server::NamedService for NativeMock {
        const NAME: &'static str = "native.Test";
    }

    impl Service<Request<NativeBody>> for NativeMock {
        type Response = Response<NativeBody>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<NativeBody>) -> Self::Future {
            self.called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(ok_native_response()) })
        }
    }

    let called_flag = Arc::new(AtomicBool::new(false));
    let mock = NativeMock {
        called: Arc::clone(&called_flag),
    };

    // NativeBody-typed registry.
    let registry: NativeServiceRegistry<NativeBody> =
        NativeServiceRegistry::new().add_service(mock);
    let mut svc = registry.into_service();

    let body = NativeBody::empty();
    let request = Request::builder()
        .method("POST")
        .uri("/native.Test/SomeMethod")
        .header("content-type", "application/grpc")
        .body(body)
        .expect("build request");

    let response = svc
        .ready()
        .await
        .expect("ready")
        .call(request)
        .await
        .expect("call");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert!(
        called_flag.load(Ordering::SeqCst),
        "NativeMock was not called"
    );

    let collected = response.into_body().collect().await.expect("collect");
    let trailers = collected.trailers().cloned().unwrap_or_default();
    let status = trailers
        .get("grpc-status")
        .map(|v| v.to_str().unwrap_or("-1"));
    assert!(
        status.is_some(),
        "grpc-status trailer must be present in registry response"
    );
}

// ─── async interceptor: inject + abort ────────────────────────────────────────

#[tokio::test]
async fn async_interceptor_injects_header_server() {
    use oxirpc_core::interceptor::AsyncInterceptor;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    // A service that records whether it saw header `x-server-injected`.
    #[derive(Clone)]
    struct HeaderSpy {
        saw: Arc<AtomicBool>,
    }
    impl NamedService for HeaderSpy {
        const NAME: &'static str = "spy.Service";
    }
    impl Service<Request<tonic::body::Body>> for HeaderSpy {
        type Response = Response<NativeBody>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: Request<tonic::body::Body>) -> Self::Future {
            let saw = Arc::clone(&self.saw);
            let present = req
                .headers()
                .get("x-server-injected")
                .map(|v| v == "yes")
                .unwrap_or(false);
            Box::pin(async move {
                saw.store(present, Ordering::SeqCst);
                Ok(ok_native_response())
            })
        }
    }

    let saw = Arc::new(AtomicBool::new(false));
    let spy = HeaderSpy {
        saw: Arc::clone(&saw),
    };

    let interceptor = |mut req: oxirpc_core::message::Request<()>| async move {
        req.metadata_mut()
            .insert("x-server-injected", "yes")
            .unwrap();
        Ok::<_, oxirpc_core::rpc::Status>(req)
    };

    let registry = NativeServiceRegistry::new()
        .add_service(spy)
        .with_async_interceptor(Arc::new(interceptor) as Arc<dyn AsyncInterceptor>);
    let mut svc = registry.into_service();

    let req = Request::builder()
        .uri("/spy.Service/Method")
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::default())
        .expect("request");
    let resp = svc.call(req).await.expect("infallible");
    assert_eq!(resp.status(), http::StatusCode::OK);
    assert!(
        saw.load(Ordering::SeqCst),
        "service must see the injected header"
    );
}

#[tokio::test]
async fn async_interceptor_abort_returns_status_server() {
    use oxirpc_core::interceptor::AsyncInterceptor;
    use oxirpc_core::status::StatusCode;
    use std::sync::Arc;

    let mock = MockService::new("mock.Service");
    let interceptor = |_req: oxirpc_core::message::Request<()>| async move {
        Err::<oxirpc_core::message::Request<()>, _>(oxirpc_core::rpc::Status::new(
            StatusCode::PermissionDenied,
            "denied",
        ))
    };
    let registry = NativeServiceRegistry::new()
        .add_service(mock)
        .with_async_interceptor(Arc::new(interceptor) as Arc<dyn AsyncInterceptor>);
    let mut svc = registry.into_service();

    let resp = call_path(&mut svc, "/mock.Service/Method").await;
    // Aborted by interceptor → PermissionDenied == 7 in trailers.
    assert_eq!(
        grpc_status_from_response(resp).await,
        Some(7),
        "interceptor abort must yield grpc-status 7"
    );
}

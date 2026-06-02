//! Tests for Tower middleware layers: MethodInterceptorLayer, IpRateLimiterLayer,
//! and Display for ServerBuilder.

use http::{Request, Response};
use oxirpc_server::{IpRateLimiterLayer, MethodInterceptorLayer, ServerBuilder};
use tonic::Status;
use tower::{Layer, Service, ServiceExt};

// ─── Helper: mock inner service ───────────────────────────────────────────────

fn ok_response() -> Response<tonic::body::Body> {
    Response::builder()
        .status(200)
        .body(tonic::body::Body::empty())
        .expect("response builder")
}

/// Build a simple inner service that always returns a 200 OK.
fn echo_service() -> impl Service<
    Request<tonic::body::Body>,
    Response = Response<tonic::body::Body>,
    Error = std::convert::Infallible,
    Future = std::future::Ready<Result<Response<tonic::body::Body>, std::convert::Infallible>>,
> + Clone {
    tower::service_fn(|_req: Request<tonic::body::Body>| {
        std::future::ready(Ok::<_, std::convert::Infallible>(ok_response()))
    })
}

// ─── MethodInterceptorLayer tests ─────────────────────────────────────────────

/// A rule that always passes should let the request through and return 200.
#[tokio::test]
async fn method_interceptor_allows_allowed_path() {
    let layer = MethodInterceptorLayer::builder()
        .route("/svc.Svc/Method", |_path, _headers| Ok(()))
        .build();

    let inner = echo_service();
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .uri("/svc.Svc/Method")
        .body(tonic::body::Body::empty())
        .expect("request builder");

    let resp = svc
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");
    assert_eq!(resp.status(), 200);
}

/// A rule returning Err(Unauthenticated) should produce a non-200 response with
/// the grpc-status trailer set.
#[tokio::test]
async fn method_interceptor_blocks_on_rejection() {
    let layer = MethodInterceptorLayer::builder()
        .route("/svc.Svc/Secure", |_path, _headers| {
            Err(Status::unauthenticated("no token"))
        })
        .build();

    let inner = echo_service();
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .uri("/svc.Svc/Secure")
        .body(tonic::body::Body::empty())
        .expect("request builder");

    let resp = svc
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");

    // tonic encodes gRPC status in the `grpc-status` header.
    // Code::Unauthenticated == 16.
    let grpc_status = resp
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        grpc_status, "16",
        "expected grpc-status 16 (Unauthenticated)"
    );
}

/// Requests to unregistered paths must pass through to the inner service.
#[tokio::test]
async fn method_interceptor_passes_through_unmatched() {
    let layer = MethodInterceptorLayer::builder()
        .route("/svc.Svc/KnownMethod", |_path, _headers| {
            Err(Status::internal("should not be called"))
        })
        .build();

    let inner = echo_service();
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .uri("/svc.Svc/UnknownMethod")
        .body(tonic::body::Body::empty())
        .expect("request builder");

    let resp = svc
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");
    // Should reach the inner service and get 200.
    assert_eq!(resp.status(), 200);
}

// ─── IpRateLimiterLayer tests ─────────────────────────────────────────────────

/// The first request with burst=1 must succeed (bucket starts full).
#[tokio::test]
async fn ip_rate_limiter_allows_first_request() {
    let layer = IpRateLimiterLayer::new(1.0, 1);
    let inner = echo_service();
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .uri("/svc.Svc/Method")
        .header("x-forwarded-for", "10.0.0.1")
        .body(tonic::body::Body::empty())
        .expect("request builder");

    let resp = svc
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");
    assert_eq!(resp.status(), 200, "first request should pass");
}

/// With burst=1, a second immediate request from the same IP must be rate-limited.
#[tokio::test]
async fn ip_rate_limiter_blocks_after_burst() {
    let layer = IpRateLimiterLayer::new(0.1, 1); // very slow refill, burst of 1
    let inner = echo_service();
    // Share the same service instance so the bucket state is preserved.
    let mut svc = layer.layer(inner);

    // First request: allowed.
    let req1 = Request::builder()
        .uri("/svc.Svc/Method")
        .header("x-forwarded-for", "10.0.0.2")
        .body(tonic::body::Body::empty())
        .expect("request 1");
    let resp1 = svc
        .ready()
        .await
        .expect("ready")
        .call(req1)
        .await
        .expect("call 1");
    assert_eq!(resp1.status(), 200, "first request should pass");

    // Second request immediately after: bucket empty, should be rate-limited.
    let req2 = Request::builder()
        .uri("/svc.Svc/Method")
        .header("x-forwarded-for", "10.0.0.2")
        .body(tonic::body::Body::empty())
        .expect("request 2");
    let resp2 = svc
        .ready()
        .await
        .expect("ready")
        .call(req2)
        .await
        .expect("call 2");

    // gRPC ResourceExhausted == code 8.
    let grpc_status = resp2
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        grpc_status, "8",
        "second request should get ResourceExhausted (8)"
    );
}

// ─── IpRateLimiterLayer: multiple IPs are budgeted independently ──────────────

/// Two distinct IPs must not share a token bucket: each gets its own burst.
#[tokio::test]
async fn ip_rate_limiter_different_ips_are_independent() {
    // burst=1, very slow refill so no tokens are replenished during the test.
    let layer = IpRateLimiterLayer::new(0.001, 1);
    let inner = echo_service();
    let mut svc = layer.layer(inner);

    // IP A: first request — should be allowed.
    let req_a1 = Request::builder()
        .uri("/svc.Svc/Method")
        .header("x-forwarded-for", "10.1.0.1")
        .body(tonic::body::Body::empty())
        .expect("req_a1");
    let resp_a1 = svc
        .ready()
        .await
        .expect("ready")
        .call(req_a1)
        .await
        .expect("call");
    assert_eq!(resp_a1.status(), 200, "IP A first request should pass");

    // IP B: first request from a *different* IP — should also be allowed.
    let req_b1 = Request::builder()
        .uri("/svc.Svc/Method")
        .header("x-forwarded-for", "10.2.0.1")
        .body(tonic::body::Body::empty())
        .expect("req_b1");
    let resp_b1 = svc
        .ready()
        .await
        .expect("ready")
        .call(req_b1)
        .await
        .expect("call");
    assert_eq!(
        resp_b1.status(),
        200,
        "IP B first request should also pass independently"
    );

    // IP A: second request — now exhausted, should be rate-limited.
    let req_a2 = Request::builder()
        .uri("/svc.Svc/Method")
        .header("x-forwarded-for", "10.1.0.1")
        .body(tonic::body::Body::empty())
        .expect("req_a2");
    let resp_a2 = svc
        .ready()
        .await
        .expect("ready")
        .call(req_a2)
        .await
        .expect("call");
    let grpc_status = resp_a2
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        grpc_status, "8",
        "IP A second request should be ResourceExhausted"
    );
}

/// A request carrying no IP headers falls back to a default key; it should
/// still be processed (allowed or rate-limited) without panic.
#[tokio::test]
async fn ip_rate_limiter_no_ip_header_does_not_panic() {
    let layer = IpRateLimiterLayer::new(1.0, 5);
    let inner = echo_service();
    let mut svc = layer.layer(inner);

    let req = Request::builder()
        .uri("/svc.Svc/Method")
        // No x-forwarded-for or x-real-ip header.
        .body(tonic::body::Body::empty())
        .expect("req");
    // Must not panic — status is implementation-defined (200 or 429/8).
    let _resp = svc
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");
}

// ─── MethodInterceptorBuilder default construction ────────────────────────────

/// `MethodInterceptorBuilder::default()` should construct a layer without panic.
#[test]
fn method_interceptor_builder_default_constructs() {
    use oxirpc_server::MethodInterceptorBuilder;
    let layer = MethodInterceptorBuilder::default().build();
    let _ = layer; // Just verify it builds.
}

/// A layer with no registered routes must pass every request through unchanged.
#[tokio::test]
async fn method_interceptor_empty_layer_passes_all() {
    use oxirpc_server::MethodInterceptorBuilder;
    let layer = MethodInterceptorBuilder::default().build();
    let inner = echo_service();
    let mut svc = layer.layer(inner);

    for path in ["/svc.A/Foo", "/svc.B/Bar", "/health/Check"] {
        let req = Request::builder()
            .uri(path)
            .body(tonic::body::Body::empty())
            .expect("request builder");
        let resp = svc
            .ready()
            .await
            .expect("ready")
            .call(req)
            .await
            .expect("call");
        assert_eq!(
            resp.status(),
            200,
            "path {path} should pass through empty interceptor"
        );
    }
}

/// Multiple routes can be registered; each is checked independently.
#[tokio::test]
async fn method_interceptor_multiple_routes() {
    let layer = MethodInterceptorLayer::builder()
        .route("/svc.Svc/Public", |_path, _headers| Ok(()))
        .route("/svc.Svc/Private", |_path, _headers| {
            Err(Status::permission_denied("restricted"))
        })
        .build();

    let inner = echo_service();
    let mut svc = layer.layer(inner);

    // Public should pass.
    let req_pub = Request::builder()
        .uri("/svc.Svc/Public")
        .body(tonic::body::Body::empty())
        .expect("pub req");
    let resp_pub = svc
        .ready()
        .await
        .expect("ready")
        .call(req_pub)
        .await
        .expect("call");
    assert_eq!(resp_pub.status(), 200, "/Public should be allowed");

    // Private should be rejected.
    let req_priv = Request::builder()
        .uri("/svc.Svc/Private")
        .body(tonic::body::Body::empty())
        .expect("priv req");
    let resp_priv = svc
        .ready()
        .await
        .expect("ready")
        .call(req_priv)
        .await
        .expect("call");
    // Code::PermissionDenied == 7.
    let grpc_status = resp_priv
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        grpc_status, "7",
        "expected grpc-status 7 (PermissionDenied) for /Private"
    );
}

// ─── Display for ServerBuilder ────────────────────────────────────────────────

#[test]
fn server_builder_display_default() {
    let s = ServerBuilder::default().to_string();
    assert!(
        s.starts_with("ServerBuilder("),
        "Display should start with 'ServerBuilder(', got: {s}"
    );
    assert!(
        s.contains("accept_http1="),
        "missing accept_http1 field: {s}"
    );
    assert!(s.contains("timeout="), "missing timeout field: {s}");
    assert!(
        s.contains("max_concurrent_streams="),
        "missing max_concurrent_streams field: {s}"
    );
    assert!(s.contains("tcp_nodelay="), "missing tcp_nodelay field: {s}");
    assert!(
        s.contains("accept_encoding="),
        "missing accept_encoding field: {s}"
    );
}

#[test]
fn server_builder_display_with_values() {
    use std::time::Duration;

    let s = ServerBuilder::new()
        .accept_http1(true)
        .timeout(Duration::from_millis(5000))
        .max_concurrent_streams(200)
        .tcp_nodelay(true)
        .to_string();

    // The Display format includes tls=false when the `tls` feature is enabled
    // (Slice 3 added TLS support). Accept both formats.
    assert!(
        s == "ServerBuilder(accept_http1=true, timeout=5000ms, max_concurrent_streams=200, tcp_nodelay=true, accept_encoding=[])"
            || s == "ServerBuilder(accept_http1=true, timeout=5000ms, max_concurrent_streams=200, tcp_nodelay=true, accept_encoding=[], tls=false)",
        "unexpected Display output: {s}"
    );
}

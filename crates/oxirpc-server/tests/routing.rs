//! Integration tests for [`oxirpc_server::routing::MethodRouter`].

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::{Request, Response};
use tower::{Layer, Service, ServiceExt};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn build_req(path: &str) -> Request<tonic::body::Body> {
    Request::builder()
        .uri(path)
        .body(tonic::body::Body::empty())
        .expect("request builder")
}

// ─── EchoService ─────────────────────────────────────────────────────────────

/// A minimal service that echoes its tag in an `x-echo` header.
#[derive(Clone)]
struct EchoService {
    msg: &'static str,
}

impl Service<Request<tonic::body::Body>> for EchoService {
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
        let msg = self.msg;
        Box::pin(async move {
            let resp = Response::builder()
                .status(200)
                .header("x-echo", msg)
                .body(tonic::body::Body::empty())
                .expect("response builder");
            Ok(resp)
        })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Requests matching a registered path must be dispatched to the exact handler.
#[tokio::test]
async fn router_dispatches_to_exact_match() {
    // We need to import MethodRouter – Slice 3 adds `pub use routing::MethodRouter`
    // to lib.rs. Until then we access via the module path.
    use oxirpc_server::MethodRouter;

    let mut router = MethodRouter::new()
        .route("/svc/Foo", EchoService { msg: "a" })
        .route("/svc/Bar", EchoService { msg: "b" });

    let req = build_req("/svc/Foo");
    let resp = router
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(
        resp.headers().get("x-echo").and_then(|v| v.to_str().ok()),
        Some("a"),
        "expected handler 'a' for /svc/Foo"
    );
}

/// When no route matches but a fallback is registered, the fallback handles it.
#[tokio::test]
async fn router_falls_back_when_no_match() {
    use oxirpc_server::MethodRouter;

    let mut router = MethodRouter::new()
        .route("/svc/Known", EchoService { msg: "known" })
        .fallback(EchoService { msg: "fb" });

    let req = build_req("/svc/Unknown");
    let resp = router
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(
        resp.headers().get("x-echo").and_then(|v| v.to_str().ok()),
        Some("fb"),
        "expected fallback handler for /svc/Unknown"
    );
}

/// When no route matches and there is no fallback, the router returns an
/// UNIMPLEMENTED response (grpc-status: 12).
#[tokio::test]
async fn router_returns_unimplemented_without_fallback() {
    use oxirpc_server::MethodRouter;

    let mut router = MethodRouter::new().route("/svc/Known", EchoService { msg: "known" });

    let req = build_req("/svc/Unknown");
    let resp = router
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");

    // grpc-status: 12 (UNIMPLEMENTED) should be present in the response headers.
    let grpc_status = resp
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    assert_eq!(
        grpc_status.as_deref(),
        Some("12"),
        "expected grpc-status: 12 (UNIMPLEMENTED), got: {:?}",
        grpc_status
    );
}

/// Cloning a router yields an independent but identically-configured copy.
/// Both clones must be able to dispatch independently without interfering.
#[tokio::test]
async fn router_clones_correctly_for_concurrent_use() {
    use oxirpc_server::MethodRouter;

    let original = MethodRouter::new()
        .route("/svc/Ping", EchoService { msg: "pong" })
        .fallback(EchoService { msg: "fallback" });

    let mut clone_a = original.clone();
    let mut clone_b = original.clone();

    // Both clones should dispatch to the same handler.
    let resp_a = clone_a
        .ready()
        .await
        .expect("ready")
        .call(build_req("/svc/Ping"))
        .await
        .expect("call");

    let resp_b = clone_b
        .ready()
        .await
        .expect("ready")
        .call(build_req("/svc/Ping"))
        .await
        .expect("call");

    assert_eq!(
        resp_a.headers().get("x-echo").and_then(|v| v.to_str().ok()),
        Some("pong"),
        "clone_a should dispatch to pong handler"
    );
    assert_eq!(
        resp_b.headers().get("x-echo").and_then(|v| v.to_str().ok()),
        Some("pong"),
        "clone_b should dispatch to pong handler"
    );
}

/// `MethodRouter` can be wrapped in a `MethodInterceptorLayer`; an allowing
/// interceptor must let the request through to the router's registered handler.
#[tokio::test]
async fn router_chains_with_interceptor_layer() {
    use oxirpc_server::{MethodInterceptorLayer, MethodRouter};

    // Build a router with a simple echo handler.
    let router = MethodRouter::new().route("/svc/Hello", EchoService { msg: "hello" });

    // Wrap router in an interceptor that always allows the request.
    let layer = MethodInterceptorLayer::builder()
        .route("/svc/Hello", |_path, _headers| Ok(()))
        .build();

    let mut svc = layer.layer(router);

    let req = build_req("/svc/Hello");
    let resp = svc
        .ready()
        .await
        .expect("ready")
        .call(req)
        .await
        .expect("call");

    assert_eq!(
        resp.headers().get("x-echo").and_then(|v| v.to_str().ok()),
        Some("hello"),
        "interceptor should pass through to router handler"
    );
}

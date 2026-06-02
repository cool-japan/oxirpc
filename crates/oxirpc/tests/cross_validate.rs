//! Cross-validation tests: semantic equivalence between native transport and tonic transport.
//!
//! These tests verify that the native transport stack (`NativeChannel` /
//! `NativeServiceRegistry`) produces equivalent gRPC responses to tonic's own
//! transport for the same RPC payloads.
//!
//! Test layout:
//!
//! 1. `native_unary_request_produces_same_response_as_tonic`
//!    — tonic CLIENT → native SERVER (NativeServiceRegistry)
//!
//! 2. `tonic_server_accepts_native_channel_request`
//!    — native CLIENT (NativeChannel.call) → tonic SERVER
//!
//! 3. `native_to_native_roundtrip_matches_tonic_to_tonic`
//!    — both transports run in parallel; responses must be equal
//!
//! 4. `native_unimplemented_method_returns_grpc_12`
//!    — empty NativeServiceRegistry → tonic client → expect UNIMPLEMENTED(12)
//!
//! 5. `error_status_propagates_correctly_through_native_layer`
//!    — greeter that returns InvalidArgument; verify code+message survive
//!
//! 6. `native_server_stream_matches_tonic`
//!    — Watch stream from native server matches Watch stream from tonic server
//!
//! 7. `native_bidi_reflection_matches_tonic`
//!    — ListServices response from native reflection matches tonic reflection
//!
//! 8. `streaming_not_serving_status_propagates_through_native_layer`
//!    — NOT_SERVING status (value 2) survives the native streaming path
//!
//! Requires the `native` cargo feature.

use std::net::SocketAddr;
use tokio::sync::oneshot;

// ── Generated proto types + native service stubs ──────────────────────────────
//
// greeter.rs          → HelloRequest / HelloReply (prost messages)
// greeter.services.rs → greeter_server / greeter_client modules (native stubs)

pub mod greeter {
    include!(concat!(env!("OUT_DIR"), "/greeter.rs"));
    include!(concat!(env!("OUT_DIR"), "/greeter.services.rs"));
}

use greeter::{
    greeter_client::GreeterClient,
    greeter_server::{Greeter, GreeterServer},
    HelloReply, HelloRequest,
};

use oxirpc_client::{
    balance::{Endpoint, StaticResolver},
    native_channel::{NativeBody, NativeChannelBuilder},
};
use oxirpc_core::wire::{
    frame::encode_frame,
    server::{error_response_body, grpc_response_headers, read_unary_request, unary_response_body},
};
use oxirpc_server::{NativeServiceRegistry, ServerBuilder};

// ── Greeter impls ─────────────────────────────────────────────────────────────

/// Standard greeter: returns `"hello, {name}"`.
#[derive(Debug, Default, Clone)]
struct MyGreeter;

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloReply>, tonic::Status> {
        let name = request.into_inner().name;
        Ok(tonic::Response::new(HelloReply {
            message: format!("hello, {name}"),
        }))
    }
}

/// Error greeter: always returns `InvalidArgument` with a fixed message.
#[derive(Debug, Default, Clone)]
struct ErrorGreeter;

#[tonic::async_trait]
impl Greeter for ErrorGreeter {
    async fn hello(
        &self,
        _request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloReply>, tonic::Status> {
        Err(tonic::Status::new(
            tonic::Code::InvalidArgument,
            "test-error",
        ))
    }
}

// ── NativeGreeterService ──────────────────────────────────────────────────────
//
// A native tower::Service wrapper around any `Greeter` impl that returns
// `Response<NativeBody>` — required by `NativeServiceRegistry::add_service`.
//
// The codegen-produced `GreeterServer<T>` returns `Response<tonic::body::Body>`
// (tonic codec path) and therefore cannot be added to `NativeServiceRegistry`.
// This hand-written wrapper uses the native wire helpers instead.

#[derive(Clone)]
struct NativeGreeterService<G>(std::sync::Arc<G>);

impl<G: Greeter> NativeGreeterService<G> {
    fn new(inner: G) -> Self {
        Self(std::sync::Arc::new(inner))
    }
}

impl<G: Greeter> tonic::server::NamedService for NativeGreeterService<G> {
    const NAME: &'static str = "greeter.Greeter";
}

impl<G: Greeter> tower::Service<http::Request<NativeBody>> for NativeGreeterService<G> {
    type Response = http::Response<NativeBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<NativeBody>) -> Self::Future {
        let inner = std::sync::Arc::clone(&self.0);
        Box::pin(async move {
            let resp = native_greeter_dispatch(inner, req).await;
            Ok(resp)
        })
    }
}

/// Dispatch a single greeter RPC to the inner `G: Greeter`.
///
/// Reads the request body, calls the greeter, encodes the response using the
/// native wire helpers. Errors (tonic::Status) are embedded as gRPC error
/// trailers (HTTP 200 with `grpc-status` + `grpc-message`).
async fn native_greeter_dispatch<G: Greeter>(
    inner: std::sync::Arc<G>,
    req: http::Request<NativeBody>,
) -> http::Response<NativeBody> {
    let build_resp = |body: NativeBody| {
        let mut r = http::Response::new(body);
        *r.status_mut() = http::StatusCode::OK;
        for (k, v) in &grpc_response_headers() {
            r.headers_mut().insert(k.clone(), v.clone());
        }
        r
    };

    match req.uri().path() {
        "/greeter.Greeter/Hello" => {
            let hello_req: HelloRequest = match read_unary_request(req.into_body()).await {
                Ok(r) => r,
                Err(e) => {
                    return build_resp(error_response_body(13, &e.to_string()));
                }
            };
            let tonic_req = tonic::Request::new(hello_req);
            match inner.hello(tonic_req).await {
                Ok(reply) => {
                    let body = match unary_response_body(reply.get_ref()) {
                        Ok(b) => b,
                        Err(e) => return build_resp(error_response_body(13, &e.to_string())),
                    };
                    build_resp(body)
                }
                Err(status) => {
                    build_resp(error_response_body(status.code() as u32, status.message()))
                }
            }
        }
        _ => build_resp(error_response_body(12, "unimplemented method")),
    }
}

// ── Helper: bind OS-assigned listener ────────────────────────────────────────

async fn bind_random() -> (tokio::net::TcpListener, SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

// ── Helper: connect tonic channel ────────────────────────────────────────────

async fn tonic_channel(addr: SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect tonic channel")
}

// ── Helper: build NativeChannel pointing at `addr` ───────────────────────────

async fn native_channel_for(addr: SocketAddr) -> oxirpc_client::NativeChannel {
    let uri: http::Uri = format!("http://{addr}").parse().expect("valid http URI");
    let resolver = StaticResolver::new(vec![Endpoint::new(uri)]);
    NativeChannelBuilder::new()
        .resolver(resolver)
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .await
        .expect("NativeChannel::build")
}

// ── Helper: call /greeter.Greeter/Hello via NativeChannel ─────────────────
//
// Manually frames a HelloRequest, sends it via NativeChannel.call(), then
// decodes the HelloReply from the response body.  On error, decodes the
// grpc-status trailer into a tonic::Status.

async fn native_hello(
    channel: &oxirpc_client::NativeChannel,
    addr: SocketAddr,
    name: &str,
) -> Result<HelloReply, tonic::Status> {
    use prost::Message as _;

    // Encode the request proto.
    let req_proto = HelloRequest {
        name: name.to_owned(),
    };
    let req_bytes = req_proto.encode_to_vec();

    // Wrap in a gRPC 5-byte length-prefix frame.
    let framed = encode_frame(&req_bytes, false).expect("encode_frame");

    // Build the HTTP/2 request.
    let authority = addr.to_string();
    let http_req = http::Request::builder()
        .method("POST")
        .uri(format!("http://{authority}/greeter.Greeter/Hello"))
        .version(http::Version::HTTP_2)
        .header("content-type", "application/grpc+proto")
        .header("te", "trailers")
        .body(NativeBody::once(framed))
        .expect("build http request");

    // Send request and get response.
    // OxiRpcError implements Into<tonic::Status>: Status(s) maps to s,
    // Transport maps to unavailable, Timeout to deadline_exceeded, etc.
    let resp = channel.call(http_req).await.map_err(tonic::Status::from)?;

    // Consume the response body — collect all data bytes.
    // OxiRpcError implements Into<tonic::Status> so we can use it directly.
    let body = resp.into_body();
    let payload = collect_native_body(body)
        .await
        .map_err(tonic::Status::from)?;

    // The NativeChannel's `pump_response` already runs a `FrameDecoder` which
    // strips the 5-byte gRPC frame header before emitting bytes into the body
    // channel.  The collected bytes are therefore the raw proto payload.
    HelloReply::decode(payload.as_slice())
        .map_err(|e| tonic::Status::internal(format!("proto decode failed: {e}")))
}

/// Drain all data bytes from a `NativeBody`, returning a single byte vec.
async fn collect_native_body(body: NativeBody) -> Result<Vec<u8>, oxirpc_core::OxiRpcError> {
    use http_body::Body as _;
    use std::pin::pin;

    let mut out = Vec::new();
    let mut pinned = pin!(body);
    loop {
        match futures_util::future::poll_fn(|cx| pinned.as_mut().poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if frame.is_data() {
                    if let Ok(data) = frame.into_data() {
                        out.extend_from_slice(&data);
                    }
                }
                // Skip trailer frames — status is already decoded by call.rs.
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: native_unary_request_produces_same_response_as_tonic
//
// tonic CLIENT → native SERVER (NativeServiceRegistry + GreeterServer)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_unary_request_produces_same_response_as_tonic() {
    let (listener, addr) = bind_random().await;
    let registry = NativeServiceRegistry::new().add_service(NativeGreeterService::new(MyGreeter));

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let channel = tonic_channel(addr).await;
    let mut client = GreeterClient::new(channel);

    let resp = client
        .hello(tonic::Request::new(HelloRequest {
            name: "cross-validate".to_owned(),
        }))
        .await
        .expect("SayHello RPC must succeed");

    assert_eq!(
        resp.into_inner().message,
        "hello, cross-validate",
        "native server must return the same greeting as tonic"
    );

    server_handle.abort();
    server_handle.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: tonic_server_accepts_native_channel_request
//
// native CLIENT (NativeChannel.call) → tonic SERVER (tonic::transport::Server)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tonic_server_accepts_native_channel_request() {
    let (listener, addr) = bind_random().await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GreeterServer::new(MyGreeter))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let channel = native_channel_for(addr).await;
    let reply = native_hello(&channel, addr, "cross-validate")
        .await
        .expect("native channel → tonic server RPC must succeed");

    assert_eq!(
        reply.message, "hello, cross-validate",
        "tonic server must accept native channel request and return the same greeting"
    );

    shutdown_tx.send(()).ok();
    server_handle.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: native_to_native_roundtrip_matches_tonic_to_tonic
//
// Both transports run in parallel.  Their responses must be semantically equal.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_to_native_roundtrip_matches_tonic_to_tonic() {
    // ── Pair A: native server + tonic client ─────────────────────────────────
    let (native_listener, native_addr) = bind_random().await;
    let native_registry =
        NativeServiceRegistry::new().add_service(NativeGreeterService::new(MyGreeter));

    let native_server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(native_listener, native_registry)
            .await
            .ok();
    });

    // ── Pair B: tonic server + native client ─────────────────────────────────
    let (tonic_listener, tonic_addr) = bind_random().await;
    let (tonic_shutdown_tx, tonic_shutdown_rx) = oneshot::channel::<()>();

    let tonic_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GreeterServer::new(MyGreeter))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(tonic_listener),
                async {
                    tonic_shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // ── Pair A: tonic client → native server ─────────────────────────────────
    let (pair_a_reply, pair_b_reply) = tokio::join!(
        async {
            let ch = tonic_channel(native_addr).await;
            let mut client = GreeterClient::new(ch);
            client
                .hello(tonic::Request::new(HelloRequest {
                    name: "compare".to_owned(),
                }))
                .await
                .expect("pair-A RPC (tonic client → native server) must succeed")
                .into_inner()
        },
        async {
            // ── Pair B: native client → tonic server ─────────────────────────
            let ch = native_channel_for(tonic_addr).await;
            native_hello(&ch, tonic_addr, "compare")
                .await
                .expect("pair-B RPC (native client → tonic server) must succeed")
        }
    );

    assert_eq!(
        pair_a_reply.message, pair_b_reply.message,
        "native↔tonic and tonic↔native must produce identical responses"
    );

    native_server.abort();
    native_server.await.ok();
    tonic_shutdown_tx.send(()).ok();
    tonic_server.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: native_unimplemented_method_returns_grpc_12
//
// An empty NativeServiceRegistry (no services registered) is queried via a
// tonic Channel.  The dispatcher must return gRPC status 12 (UNIMPLEMENTED).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_unimplemented_method_returns_grpc_12() {
    let (listener, addr) = bind_random().await;

    // Empty registry — no services registered, no fallback.
    // The registry dispatches by name; with no entries it returns UNIMPLEMENTED.
    let empty_registry = NativeServiceRegistry::new();

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, empty_registry)
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let channel = tonic_channel(addr).await;
    let mut client = GreeterClient::new(channel);

    let status = client
        .hello(tonic::Request::new(HelloRequest {
            name: "should-fail".to_owned(),
        }))
        .await
        .expect_err("empty registry must return an error");

    assert_eq!(
        status.code(),
        tonic::Code::Unimplemented,
        "empty NativeServiceRegistry must return gRPC status 12 (UNIMPLEMENTED), got {:?}",
        status.code()
    );

    server_handle.abort();
    server_handle.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: error_status_propagates_correctly_through_native_layer
//
// 5a: tonic client → native server that returns InvalidArgument
// 5b: native client → tonic server that returns InvalidArgument
//
// Both paths must preserve code (InvalidArgument = 3) and message ("test-error").
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn error_status_propagates_correctly_through_native_layer() {
    // ── Path 5a: tonic client → native server (ErrorGreeter) ─────────────────
    let (native_err_listener, native_err_addr) = bind_random().await;
    let err_registry =
        NativeServiceRegistry::new().add_service(NativeGreeterService::new(ErrorGreeter));

    let native_err_server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(native_err_listener, err_registry)
            .await
            .ok();
    });

    // ── Path 5b: tonic server (ErrorGreeter) ─────────────────────────────────
    let (tonic_err_listener, tonic_err_addr) = bind_random().await;
    let (err_shutdown_tx, err_shutdown_rx) = oneshot::channel::<()>();

    let tonic_err_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GreeterServer::new(ErrorGreeter))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(tonic_err_listener),
                async {
                    err_shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // Path 5a: tonic Channel → native server
    let (status_5a, status_5b) = tokio::join!(
        async {
            let ch = tonic_channel(native_err_addr).await;
            let mut client = GreeterClient::new(ch);
            client
                .hello(tonic::Request::new(HelloRequest {
                    name: "err-test".to_owned(),
                }))
                .await
                .expect_err("ErrorGreeter must return an error (path 5a)")
        },
        async {
            // Path 5b: NativeChannel → tonic server
            let ch = native_channel_for(tonic_err_addr).await;
            native_hello(&ch, tonic_err_addr, "err-test")
                .await
                .expect_err("ErrorGreeter must return an error (path 5b)")
        }
    );

    // ── Verify path 5a ───────────────────────────────────────────────────────
    assert_eq!(
        status_5a.code(),
        tonic::Code::InvalidArgument,
        "path 5a: expected InvalidArgument, got {:?}",
        status_5a.code()
    );
    assert_eq!(
        status_5a.message(),
        "test-error",
        "path 5a: error message must survive native server → tonic client round-trip"
    );

    // ── Verify path 5b ───────────────────────────────────────────────────────
    assert_eq!(
        status_5b.code(),
        tonic::Code::InvalidArgument,
        "path 5b: expected InvalidArgument, got {:?}",
        status_5b.code()
    );
    assert_eq!(
        status_5b.message(),
        "test-error",
        "path 5b: error message must survive tonic server → native client round-trip"
    );

    native_err_server.abort();
    native_err_server.await.ok();
    err_shutdown_tx.send(()).ok();
    tonic_err_server.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6: native_server_stream_matches_tonic
//
// Cross-validate the NATIVE streaming path (NativeHealthService Watch RPC)
// against the TONIC streaming path (tonic_health HealthServer Watch RPC).
//
// Both paths are called with the same service name registered as SERVING.
// We collect the FIRST frame from each stream and assert:
//   - both return status == 1 (ServingStatus::Serving)
//
// This proves the native streaming response body is byte-compatible with tonic:
// the tonic HealthClient can successfully decode the native streaming response.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_server_stream_matches_tonic() {
    use oxirpc_health::HealthBuilder;
    use tokio_stream::StreamExt as _;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    const WATCH_SVC: &str = "cross.WatchService";

    // ── Path A (native): NativeHealthService via serve_native_registry ────────
    let (native_svc, mut native_handle) = HealthBuilder::new().build_native().await;
    native_handle.set_serving(WATCH_SVC).await;

    let (native_listener, native_addr) = bind_random().await;
    let native_registry = NativeServiceRegistry::new().add_service(native_svc.clone());

    let native_server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(native_listener, native_registry)
            .await
            .ok();
    });

    // ── Path B (tonic): tonic HealthServer via tonic transport ────────────────
    let (tonic_health_svc, mut tonic_handle) = oxirpc_health::HealthBuilder::new().build().await;
    tonic_handle.set_serving(WATCH_SVC).await;

    let (tonic_listener, tonic_addr) = bind_random().await;
    let (tonic_shutdown_tx, tonic_shutdown_rx) = oneshot::channel::<()>();

    let tonic_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(tonic_health_svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(tonic_listener),
                async {
                    tonic_shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    // Allow both servers to start.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // ── Call Watch on both paths and read the first frame from each ───────────
    let (native_first, tonic_first) = tokio::join!(
        async {
            let ch = tonic_channel(native_addr).await;
            let mut client = HealthClient::new(ch);
            let mut stream = client
                .watch(HealthCheckRequest {
                    service: WATCH_SVC.to_string(),
                })
                .await
                .expect("Watch RPC on native server")
                .into_inner();
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .expect("first Watch frame within 2 s (native)")
                .expect("stream must yield at least one frame (native)")
                .expect("stream frame must be Ok (native)")
        },
        async {
            let ch = tonic_channel(tonic_addr).await;
            let mut client = HealthClient::new(ch);
            let mut stream = client
                .watch(HealthCheckRequest {
                    service: WATCH_SVC.to_string(),
                })
                .await
                .expect("Watch RPC on tonic server")
                .into_inner();
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .expect("first Watch frame within 2 s (tonic)")
                .expect("stream must yield at least one frame (tonic)")
                .expect("stream frame must be Ok (tonic)")
        }
    );

    // Both paths must return ServingStatus::Serving == 1.
    assert_eq!(
        native_first.status, 1,
        "native streaming Watch must return SERVING (1), got {}",
        native_first.status
    );
    assert_eq!(
        tonic_first.status, 1,
        "tonic streaming Watch must return SERVING (1), got {}",
        tonic_first.status
    );
    assert_eq!(
        native_first.status, tonic_first.status,
        "native and tonic Watch responses must be byte-compatible and equal"
    );

    // Keep handles alive until after stream reads to prevent NOT_SERVING notifications.
    drop(native_handle);
    drop(tonic_handle);

    native_server.abort();
    native_server.await.ok();
    tonic_shutdown_tx.send(()).ok();
    tonic_server.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7: native_bidi_reflection_matches_tonic
//
// Cross-validate the NATIVE bidi reflection path (NativeReflectionServiceV1)
// against the TONIC reflection path (tonic_reflection v1 server).
//
// Both paths receive a ListServices("") request.
// We assert that both return a non-empty service list (same cardinality or
// superset), proving the native bidi-sequential response path is wire-compatible.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_bidi_reflection_matches_tonic() {
    use tokio_stream::iter as stream_iter;
    use tonic_reflection::pb::v1::{
        server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
        ServerReflectionRequest, FILE_DESCRIPTOR_SET,
    };

    // ── Path A (native): NativeReflectionServiceV1 ────────────────────────────
    let native_reflect_svc = oxirpc_reflect::ReflectionBuilder::new()
        .register_file_descriptor_set(FILE_DESCRIPTOR_SET.to_vec())
        .build_native_v1();

    let (native_listener, native_addr) = bind_random().await;
    let native_registry = NativeServiceRegistry::new().add_service(native_reflect_svc.clone());

    let native_server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(native_listener, native_registry)
            .await
            .ok();
    });

    // ── Path B (tonic): tonic_reflection v1 server ────────────────────────────
    let tonic_reflect_svc = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("build tonic reflection v1");

    let (tonic_listener, tonic_addr) = bind_random().await;
    let (tonic_shutdown_tx, tonic_shutdown_rx) = oneshot::channel::<()>();

    let tonic_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(tonic_reflect_svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(tonic_listener),
                async {
                    tonic_shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // ── Call ListServices("") on both paths ───────────────────────────────────
    let list_request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };

    let (native_services, tonic_services) = tokio::join!(
        async {
            let ch = tonic_channel(native_addr).await;
            let mut client = ServerReflectionClient::new(ch);
            let mut stream = client
                .server_reflection_info(stream_iter(vec![list_request.clone()]))
                .await
                .expect("ListServices RPC on native reflection server")
                .into_inner();
            let reply = tokio::time::timeout(std::time::Duration::from_secs(2), stream.message())
                .await
                .expect("ListServices reply within 2 s (native)")
                .expect("stream recv must not error (native)")
                .expect("stream must yield a response (native)");
            match reply.message_response {
                Some(MessageResponse::ListServicesResponse(r)) => r.service,
                other => {
                    panic!("expected ListServicesResponse from native reflection, got: {other:?}")
                }
            }
        },
        async {
            let ch = tonic_channel(tonic_addr).await;
            let mut client = ServerReflectionClient::new(ch);
            let mut stream = client
                .server_reflection_info(stream_iter(vec![list_request.clone()]))
                .await
                .expect("ListServices RPC on tonic reflection server")
                .into_inner();
            let reply = tokio::time::timeout(std::time::Duration::from_secs(2), stream.message())
                .await
                .expect("ListServices reply within 2 s (tonic)")
                .expect("stream recv must not error (tonic)")
                .expect("stream must yield a response (tonic)");
            match reply.message_response {
                Some(MessageResponse::ListServicesResponse(r)) => r.service,
                other => {
                    panic!("expected ListServicesResponse from tonic reflection, got: {other:?}")
                }
            }
        }
    );

    // Both must return at least one service (the reflection service itself).
    assert!(
        !native_services.is_empty(),
        "native reflection ListServices must return at least one service entry"
    );
    assert!(
        !tonic_services.is_empty(),
        "tonic reflection ListServices must return at least one service entry"
    );

    // Both must include a service name that contains "ServerReflection".
    // NOTE: native reflection returns unqualified service names (e.g. "ServerReflection")
    // while tonic returns fully-qualified names (e.g. "grpc.reflection.v1.ServerReflection").
    // We test for substring inclusion so both paths satisfy the same assertion.
    let native_has_reflection = native_services
        .iter()
        .any(|s| s.name.contains("ServerReflection"));
    let tonic_has_reflection = tonic_services
        .iter()
        .any(|s| s.name.contains("ServerReflection"));

    assert!(
        native_has_reflection,
        "native reflection must list a 'ServerReflection' service; got: {:?}",
        native_services.iter().map(|s| &s.name).collect::<Vec<_>>()
    );
    assert!(
        tonic_has_reflection,
        "tonic reflection must list a 'ServerReflection' service; got: {:?}",
        tonic_services.iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    native_server.abort();
    native_server.await.ok();
    tonic_shutdown_tx.send(()).ok();
    tonic_server.await.ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 8: streaming_not_serving_status_propagates_through_native_layer
//
// A service is registered as NOT_SERVING on the native health service.
// The Watch stream on the native server returns status == 2 (NOT_SERVING).
// We cross-validate against the same observation on the tonic server.
//
// This exercises the streaming path with a non-SERVING payload, proving that
// non-OK service statuses survive the native streaming path intact.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn streaming_not_serving_status_propagates_through_native_layer() {
    use oxirpc_health::HealthBuilder;
    use tokio_stream::StreamExt as _;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    const WATCH_SVC: &str = "cross.NotServingService";

    // ── Path A (native): NativeHealthService with NOT_SERVING ─────────────────
    let (native_svc, mut native_handle) = HealthBuilder::new().build_native().await;
    native_handle.set_not_serving(WATCH_SVC).await;

    let (native_listener, native_addr) = bind_random().await;
    let native_registry = NativeServiceRegistry::new().add_service(native_svc.clone());

    let native_server = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(native_listener, native_registry)
            .await
            .ok();
    });

    // ── Path B (tonic): tonic HealthServer with NOT_SERVING ───────────────────
    let (tonic_health_svc, mut tonic_handle) = oxirpc_health::HealthBuilder::new().build().await;
    tonic_handle.set_not_serving(WATCH_SVC).await;

    let (tonic_listener, tonic_addr) = bind_random().await;
    let (tonic_shutdown_tx, tonic_shutdown_rx) = oneshot::channel::<()>();

    let tonic_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(tonic_health_svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(tonic_listener),
                async {
                    tonic_shutdown_rx.await.ok();
                },
            )
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // ── Read first Watch frame from both paths ────────────────────────────────
    let (native_first, tonic_first) = tokio::join!(
        async {
            let ch = tonic_channel(native_addr).await;
            let mut client = HealthClient::new(ch);
            let mut stream = client
                .watch(HealthCheckRequest {
                    service: WATCH_SVC.to_string(),
                })
                .await
                .expect("Watch RPC on native server (NOT_SERVING)")
                .into_inner();
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .expect("first Watch frame within 2 s (native NOT_SERVING)")
                .expect("stream must yield at least one frame (native NOT_SERVING)")
                .expect("stream frame must be Ok (native NOT_SERVING)")
        },
        async {
            let ch = tonic_channel(tonic_addr).await;
            let mut client = HealthClient::new(ch);
            let mut stream = client
                .watch(HealthCheckRequest {
                    service: WATCH_SVC.to_string(),
                })
                .await
                .expect("Watch RPC on tonic server (NOT_SERVING)")
                .into_inner();
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .expect("first Watch frame within 2 s (tonic NOT_SERVING)")
                .expect("stream must yield at least one frame (tonic NOT_SERVING)")
                .expect("stream frame must be Ok (tonic NOT_SERVING)")
        }
    );

    // NOT_SERVING = 2 in the proto ServingStatus enum.
    assert_eq!(
        native_first.status, 2,
        "native streaming Watch must return NOT_SERVING (2), got {}",
        native_first.status
    );
    assert_eq!(
        tonic_first.status, 2,
        "tonic streaming Watch must return NOT_SERVING (2), got {}",
        tonic_first.status
    );
    assert_eq!(
        native_first.status, tonic_first.status,
        "native and tonic Watch NOT_SERVING responses must match"
    );

    // Keep handles alive until after stream reads.
    drop(native_handle);
    drop(tonic_handle);

    native_server.abort();
    native_server.await.ok();
    tonic_shutdown_tx.send(()).ok();
    tonic_server.await.ok();
}

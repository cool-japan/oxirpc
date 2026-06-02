//! Integration tests for [`oxirpc_web::transport::GrpcWebServer`].
//!
//! Tests cover:
//! 1. HTTP/1.1 gRPC-Web unary roundtrip (binary frame).
//! 2. HTTP/2 prior-knowledge connection still served.
//! 3. CORS preflight over HTTP/1.1 with a `CorsPolicy`.
//! 4. Non-gRPC-Web requests pass through to the inner service.
//! 5. Graceful shutdown with an immediately-ready future returns `Ok(())`.
//! 6. Server binds and accepts TCP connections without crashing.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt as _, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use oxirpc_web::{
    codec::{encode_frame, Frame},
    cors::CorsPolicy,
    transport::GrpcWebServer,
};
use tokio::net::TcpStream;
use tonic::body::Body as TonicBody;

// ─── Minimal echo service ────────────────────────────────────────────────────

/// A minimal tower service that echoes back the body of any request it receives
/// as a 200 response with `content-type: application/grpc-web+proto`.
///
/// Accepts `Request<ReqBody>` for any `ReqBody` that is an `http_body::Body`.
#[derive(Clone, Debug)]
struct EchoService;

impl<ReqBody> tower::Service<Request<ReqBody>> for EchoService
where
    ReqBody: http_body::Body<Data = Bytes> + Send + 'static,
    ReqBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<TonicBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        Box::pin(async move {
            // Collect the request body; ignore errors — return empty on failure.
            let body_bytes = req
                .into_body()
                .collect()
                .await
                .map(|collected| collected.to_bytes())
                .unwrap_or_default();

            // Build a gRPC-Web binary data frame from the body.
            let frame = Frame::data(body_bytes.to_vec());
            let frame_bytes =
                encode_frame(&frame, oxirpc_core::encoding::CompressionEncoding::Identity)
                    .unwrap_or_default();

            // Append a minimal gRPC trailers frame: grpc-status: 0
            let trailer_frame = Frame::trailers(&[("grpc-status", "0")]);
            let trailer_bytes = encode_frame(
                &trailer_frame,
                oxirpc_core::encoding::CompressionEncoding::Identity,
            )
            .unwrap_or_default();

            let mut resp_body_bytes = frame_bytes;
            resp_body_bytes.extend_from_slice(&trailer_bytes);

            let resp_body = TonicBody::new(Full::new(Bytes::from(resp_body_bytes)));

            let resp = Response::builder()
                .status(200)
                .header("content-type", "application/grpc-web+proto")
                .body(resp_body)
                .unwrap_or_else(|_| Response::new(TonicBody::default()));
            Ok(resp)
        })
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Bind a `GrpcWebServer` on a random port and return its local address.
///
/// The server runs in a background task and shuts down when `shutdown_rx` is
/// dropped (by notifying through the watch channel).
async fn spawn_server(with_cors: bool) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // Bind a temporary listener to get a free port, then close it so the
    // GrpcWebServer can rebind.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let server = if with_cors {
        GrpcWebServer::new(EchoService).cors(CorsPolicy::new().allow_any_origin())
    } else {
        GrpcWebServer::new(EchoService)
    };

    tokio::spawn(async move {
        let shutdown = async move {
            let _ = rx.await;
        };
        let _ = server.serve_with_shutdown(addr, shutdown).await;
    });

    // Give the server time to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, tx)
}

/// Manually build a gRPC-Web binary request body for the given `payload`.
fn make_grpc_web_body(payload: &[u8]) -> Bytes {
    let frame = Frame::data(payload.to_vec());
    let bytes = encode_frame(&frame, oxirpc_core::encoding::CompressionEncoding::Identity)
        .expect("encode frame");
    Bytes::from(bytes)
}

// ─── test 1: HTTP/1.1 unary roundtrip ────────────────────────────────────────

#[tokio::test]
async fn http1_unary_grpc_web_roundtrip() {
    let (addr, _shutdown) = spawn_server(false).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let payload = b"hello grpc-web";
    let body = make_grpc_web_body(payload);

    let req = Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/test.Echo/Echo"))
        .header("content-type", "application/grpc-web+proto")
        .body(Full::new(body))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    // The response should contain at least a 5-byte gRPC-Web frame header.
    assert!(
        resp_bytes.len() >= 5,
        "expected at least 5 bytes in gRPC-Web response, got {}",
        resp_bytes.len()
    );
    // First byte should be 0x00 (uncompressed data frame) or 0x80 (trailer).
    let first_byte = resp_bytes[0];
    assert!(
        first_byte == 0x00 || first_byte == 0x80,
        "unexpected frame flag byte: 0x{:02x}",
        first_byte
    );
}

// ─── test 2: HTTP/2 prior-knowledge connection ────────────────────────────────

#[tokio::test]
async fn http2_prior_knowledge_still_served() {
    use hyper::client::conn::http2;
    use hyper_util::rt::TokioExecutor;

    let (addr, _shutdown) = spawn_server(false).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = http2::handshake(TokioExecutor::new(), io).await.unwrap();
    tokio::spawn(conn);

    let payload = b"h2 test";
    let body = make_grpc_web_body(payload);

    let req = Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/test.Echo/Echo"))
        .header("content-type", "application/grpc-web+proto")
        .body(Full::new(body))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(
        resp_bytes.len() >= 5,
        "H2 response too short: {} bytes",
        resp_bytes.len()
    );
}

// ─── test 3: CORS preflight over HTTP/1.1 ────────────────────────────────────

#[tokio::test]
async fn cors_preflight_over_http1() {
    let (addr, _shutdown) = spawn_server(true).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = Request::builder()
        .method("OPTIONS")
        .uri(format!("http://{addr}/test.Echo/Echo"))
        .header("origin", "https://app.example.com")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type,x-grpc-web")
        .body(Full::new(Bytes::new()))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    // CorsService returns 204 for OPTIONS preflights (cors.rs line 303).
    assert_eq!(resp.status(), 204);

    let headers = resp.headers();
    let acao = headers
        .get("access-control-allow-origin")
        .and_then(|v| v.to_str().ok());
    assert_eq!(acao, Some("*"), "expected CORS allow-origin header");
}

// ─── test 4: non-gRPC-Web passthrough ────────────────────────────────────────

#[tokio::test]
async fn non_grpc_web_passthrough() {
    let (addr, _shutdown) = spawn_server(false).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    // Send a plain POST with application/json — not a gRPC-Web content-type.
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/anything"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(b"{\"key\":\"value\"}".as_ref())))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    // The EchoService returns 200 for all requests regardless of content-type.
    // NativeGrpcWebLayer passes through non-gRPC-Web requests unchanged.
    assert_eq!(resp.status(), 200);
}

// ─── test 5: graceful shutdown returns Ok(()) ────────────────────────────────

#[tokio::test]
async fn graceful_shutdown_drains() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let server = GrpcWebServer::new(EchoService);

    // Immediately-ready shutdown future.
    let result = server
        .serve_with_shutdown(addr, std::future::ready(()))
        .await;

    assert!(
        result.is_ok(),
        "serve_with_shutdown should return Ok(()): {:?}",
        result
    );
}

// ─── test 6: server binds and accepts connections ────────────────────────────

#[tokio::test]
async fn server_binds_and_accepts_connections() {
    let (addr, _shutdown) = spawn_server(false).await;

    // Connecting should succeed without error.
    let stream = TcpStream::connect(addr).await;
    assert!(
        stream.is_ok(),
        "should be able to connect to server: {:?}",
        stream.err()
    );

    // Verify local/peer addresses look sane.
    let stream = stream.unwrap();
    assert_eq!(
        stream.peer_addr().unwrap().port(),
        addr.port(),
        "connected to wrong port"
    );
}

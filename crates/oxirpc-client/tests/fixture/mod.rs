//! Self-contained gRPC server fixture for integration tests.
//!
//! Uses `tonic::server::Grpc` with `tonic_prost::ProstCodec` directly — no
//! codegen required.  All fixtures bind `127.0.0.1:0` (random port).
//!
//! Fixtures:
//! - [`spawn_fixture`] — basic plaintext Pinger server
//! - [`spawn_flaky_fixture`] — returns UNAVAILABLE for first N calls
//! - [`spawn_slow_fixture`] — sleeps before each Ping response

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::Server;

// ── Proto messages ─────────────────────────────────────────────────────────────

/// Empty request message for the Ping/Stream/Sink/Echo RPCs.
#[derive(Clone, prost::Message)]
pub struct Empty {}

/// Pong response message.
#[derive(Clone, prost::Message)]
pub struct Pong {
    /// The payload string echoed or set by the server.
    #[prost(string, tag = "1")]
    pub msg: String,
}

// ── PingerService ──────────────────────────────────────────────────────────────

/// Shared state threaded through all Pinger variants.
#[derive(Clone, Default)]
pub struct PingerState {
    /// Total call count (incremented atomically on each Ping RPC).
    pub call_count: Arc<AtomicUsize>,
    /// Return UNAVAILABLE for the first `fail_first_n` calls.
    pub fail_first_n: usize,
    /// Sleep this many milliseconds before each Ping response.
    pub delay_ms: u64,
}

/// A minimal gRPC service that dispatches the four Pinger RPCs.
///
/// Implements `tower::Service<http::Request<tonic::body::Body>>` so it can be
/// plugged directly into `tonic::transport::Server::add_service`.
#[derive(Clone)]
pub struct PingerService {
    state: Arc<PingerState>,
}

impl PingerService {
    fn new(state: PingerState) -> Self {
        Self {
            state: Arc::new(state),
        }
    }
}

// tower::Service ---------------------------------------------------------------

type BoxFuture<T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;

impl tower::Service<http::Request<tonic::body::Body>> for PingerService {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let state = self.state.clone();
        Box::pin(async move {
            let path = req.uri().path().to_owned();
            let resp = match path.as_str() {
                "/fixture.Pinger/Ping" => handle_ping(req, state).await,
                "/fixture.Pinger/Stream" => handle_stream(req, state).await,
                "/fixture.Pinger/Sink" => handle_sink(req, state).await,
                "/fixture.Pinger/Echo" => handle_echo(req, state).await,
                _ => {
                    let status = tonic::Status::unimplemented(format!("unknown path: {path}"));
                    status.into_http()
                }
            };
            Ok(resp)
        })
    }
}

impl tonic::server::NamedService for PingerService {
    const NAME: &'static str = "fixture.Pinger";
}

// ── RPC handlers ──────────────────────────────────────────────────────────────

/// Codec type alias for the tests (Pong decoded from Empty, or Empty decoded — we
/// reuse ProstCodec<Pong, Empty> = encode Pong, decode Empty).
type PingCodec = tonic_prost::ProstCodec<Pong, Empty>;

/// Codec for server-streaming: encodes Pong, decodes Empty.
type StreamCodec = tonic_prost::ProstCodec<Pong, Empty>;

/// Codec for client-streaming: encodes Pong, decodes Empty (body is Empty, response is Pong).
type SinkCodec = tonic_prost::ProstCodec<Pong, Empty>;

/// Codec for bidi-streaming: encodes Pong, decodes Pong.
type EchoCodec = tonic_prost::ProstCodec<Pong, Pong>;

async fn handle_ping(
    req: http::Request<tonic::body::Body>,
    state: Arc<PingerState>,
) -> http::Response<tonic::body::Body> {
    let call_n = state.call_count.fetch_add(1, Ordering::SeqCst);

    // Fail first N calls with UNAVAILABLE.
    if call_n < state.fail_first_n {
        return tonic::Status::unavailable("flaky server").into_http();
    }

    // Optional delay for timeout tests.
    if state.delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(state.delay_ms)).await;
    }

    let mut grpc = tonic::server::Grpc::new(PingCodec::default());

    struct PingHandler;
    impl tower::Service<tonic::Request<Empty>> for PingHandler {
        type Response = tonic::Response<Pong>;
        type Error = tonic::Status;
        type Future = BoxFuture<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: tonic::Request<Empty>) -> Self::Future {
            Box::pin(async {
                Ok(tonic::Response::new(Pong {
                    msg: "pong".to_owned(),
                }))
            })
        }
    }

    grpc.unary(PingHandler, req).await
}

async fn handle_stream(
    req: http::Request<tonic::body::Body>,
    _state: Arc<PingerState>,
) -> http::Response<tonic::body::Body> {
    let mut grpc = tonic::server::Grpc::new(StreamCodec::default());

    struct StreamHandler;

    type PongStream = Pin<Box<dyn Stream<Item = Result<Pong, tonic::Status>> + Send + 'static>>;

    impl tower::Service<tonic::Request<Empty>> for StreamHandler {
        type Response = tonic::Response<PongStream>;
        type Error = tonic::Status;
        type Future = BoxFuture<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: tonic::Request<Empty>) -> Self::Future {
            Box::pin(async {
                let msgs = vec![
                    Ok(Pong {
                        msg: "pong-0".to_owned(),
                    }),
                    Ok(Pong {
                        msg: "pong-1".to_owned(),
                    }),
                    Ok(Pong {
                        msg: "pong-2".to_owned(),
                    }),
                ];
                let stream: PongStream = Box::pin(tokio_stream::iter(msgs));
                Ok(tonic::Response::new(stream))
            })
        }
    }

    grpc.server_streaming(StreamHandler, req).await
}

async fn handle_sink(
    req: http::Request<tonic::body::Body>,
    _state: Arc<PingerState>,
) -> http::Response<tonic::body::Body> {
    let mut grpc = tonic::server::Grpc::new(SinkCodec::default());

    struct SinkHandler;
    impl tower::Service<tonic::Request<tonic::Streaming<Empty>>> for SinkHandler {
        type Response = tonic::Response<Pong>;
        type Error = tonic::Status;
        type Future = BoxFuture<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, mut req: tonic::Request<tonic::Streaming<Empty>>) -> Self::Future {
            Box::pin(async move {
                // Drain all incoming messages.
                while req.get_mut().next().await.is_some() {}
                Ok(tonic::Response::new(Pong {
                    msg: "sink-ok".to_owned(),
                }))
            })
        }
    }

    grpc.client_streaming(SinkHandler, req).await
}

async fn handle_echo(
    req: http::Request<tonic::body::Body>,
    _state: Arc<PingerState>,
) -> http::Response<tonic::body::Body> {
    let mut grpc = tonic::server::Grpc::new(EchoCodec::default());

    struct EchoHandler;

    type EchoPongStream = Pin<Box<dyn Stream<Item = Result<Pong, tonic::Status>> + Send + 'static>>;

    impl tower::Service<tonic::Request<tonic::Streaming<Pong>>> for EchoHandler {
        type Response = tonic::Response<EchoPongStream>;
        type Error = tonic::Status;
        type Future = BoxFuture<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: tonic::Request<tonic::Streaming<Pong>>) -> Self::Future {
            Box::pin(async move {
                let stream = req.into_inner();
                let echo: EchoPongStream =
                    Box::pin(stream.map(|item| item.map(|p| Pong { msg: p.msg })));
                Ok(tonic::Response::new(echo))
            })
        }
    }

    grpc.streaming(EchoHandler, req).await
}

// ── Fixture launchers ─────────────────────────────────────────────────────────

/// Spawn a basic plaintext Pinger server on a random port.
///
/// Returns `(addr, shutdown_tx, join_handle)`.
/// Drop or send to `shutdown_tx` to stop the server.
pub async fn spawn_fixture() -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let state = PingerState::default();
    spawn_pinger_server(state).await
}

/// Spawn a Pinger server that returns UNAVAILABLE for the first `fail_n` calls.
pub async fn spawn_flaky_fixture(
    fail_n: usize,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let state = PingerState {
        fail_first_n: fail_n,
        ..Default::default()
    };
    spawn_pinger_server(state).await
}

/// Spawn a Pinger server that sleeps `delay_ms` before responding to Ping.
pub async fn spawn_slow_fixture(
    delay_ms: u64,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let state = PingerState {
        delay_ms,
        ..Default::default()
    };
    spawn_pinger_server(state).await
}

/// Spawn a Pinger server that shares the provided `PingerState` (call-count tracking).
pub async fn spawn_fixture_with_state(
    state: PingerState,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    spawn_pinger_server(state).await
}

// ── Internal helper ───────────────────────────────────────────────────────────

async fn spawn_pinger_server(
    state: PingerState,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let svc = PingerService::new(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move { rx.await.unwrap_or(()) },
            )
            .await;
    });

    (addr, tx, handle)
}

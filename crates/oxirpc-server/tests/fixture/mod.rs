//! Self-contained fixtures for oxirpc-server integration tests.
//!
//! Provides two hand-rolled gRPC services — `PingerService` and `PongerService`
//! — that can be registered with a `ServerBuilder` without pulling in any
//! external service crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::Duration;

use tonic::server::{NamedService, UnaryService};

// ---------------------------------------------------------------------------
// Proto message types
// ---------------------------------------------------------------------------

/// An empty request / response message.
#[derive(Clone, prost::Message)]
pub struct Empty {}

/// Response from PingerService.
#[derive(Clone, prost::Message)]
pub struct Pong {
    #[prost(string, tag = "1")]
    pub msg: String,
}

/// Request for PingerService — carries an arbitrary binary payload.
#[derive(Clone, prost::Message)]
pub struct PingRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// PingerService
// ---------------------------------------------------------------------------

/// A simple gRPC service with three routes:
///
/// * `/fixture.Pinger/Ping`     — immediate unary (returns `Pong { msg: "pong" }`)
/// * `/fixture.Pinger/PingSlow` — delayed by `delay_ms` milliseconds
/// * `/fixture.Pinger/PingBig`  — returns `Pong { msg: "pong" }` regardless of payload size
pub struct PingerService {
    /// Number of calls received across all routes.
    pub call_count: Arc<AtomicUsize>,
    /// Artificial delay injected by `PingSlow`.
    pub delay_ms: u64,
    /// Optional max-decode size (bytes) applied to the `Grpc` server handler.
    pub max_decode_bytes: Option<usize>,
}

impl PingerService {
    /// Construct a new service with the given counter and delay.
    pub fn new(call_count: Arc<AtomicUsize>, delay_ms: u64) -> Self {
        Self {
            call_count,
            delay_ms,
            max_decode_bytes: None,
        }
    }

    /// Construct with an explicit max-decode-bytes limit.
    pub fn with_limit(
        call_count: Arc<AtomicUsize>,
        delay_ms: u64,
        max_decode_bytes: usize,
    ) -> Self {
        Self {
            call_count,
            delay_ms,
            max_decode_bytes: Some(max_decode_bytes),
        }
    }
}

impl NamedService for PingerService {
    const NAME: &'static str = "fixture.Pinger";
}

impl Clone for PingerService {
    fn clone(&self) -> Self {
        Self {
            call_count: Arc::clone(&self.call_count),
            delay_ms: self.delay_ms,
            max_decode_bytes: self.max_decode_bytes,
        }
    }
}

// ---- unimplemented helper --------------------------------------------------

fn unimplemented_response() -> http::Response<tonic::body::Body> {
    tonic::Status::unimplemented("route not found").into_http()
}

// ---- UnaryService handlers -------------------------------------------------

struct PingHandler {
    call_count: Arc<AtomicUsize>,
}

impl UnaryService<PingRequest> for PingHandler {
    type Response = Pong;
    type Future =
        Pin<Box<dyn Future<Output = Result<tonic::Response<Pong>, tonic::Status>> + Send>>;

    fn call(&mut self, _req: tonic::Request<PingRequest>) -> Self::Future {
        let count = Arc::clone(&self.call_count);
        Box::pin(async move {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(tonic::Response::new(Pong {
                msg: "pong".to_string(),
            }))
        })
    }
}

struct PingSlowHandler {
    call_count: Arc<AtomicUsize>,
    delay_ms: u64,
}

impl UnaryService<PingRequest> for PingSlowHandler {
    type Response = Pong;
    type Future =
        Pin<Box<dyn Future<Output = Result<tonic::Response<Pong>, tonic::Status>> + Send>>;

    fn call(&mut self, _req: tonic::Request<PingRequest>) -> Self::Future {
        let count = Arc::clone(&self.call_count);
        let delay = self.delay_ms;
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            count.fetch_add(1, Ordering::Relaxed);
            Ok(tonic::Response::new(Pong {
                msg: "pong-slow".to_string(),
            }))
        })
    }
}

struct PingBigHandler {
    call_count: Arc<AtomicUsize>,
}

impl UnaryService<PingRequest> for PingBigHandler {
    type Response = Pong;
    type Future =
        Pin<Box<dyn Future<Output = Result<tonic::Response<Pong>, tonic::Status>> + Send>>;

    fn call(&mut self, _req: tonic::Request<PingRequest>) -> Self::Future {
        let count = Arc::clone(&self.call_count);
        Box::pin(async move {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(tonic::Response::new(Pong {
                msg: "pong-big".to_string(),
            }))
        })
    }
}

// ---- tower::Service impl ---------------------------------------------------

impl tower::Service<http::Request<tonic::body::Body>> for PingerService {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let path = req.uri().path().to_owned();
        let call_count = Arc::clone(&self.call_count);
        let delay_ms = self.delay_ms;
        let max_decode = self.max_decode_bytes;

        Box::pin(async move {
            match path.as_str() {
                "/fixture.Pinger/Ping" => {
                    // Server codec: Encode=Pong (response), Decode=PingRequest (incoming).
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        Pong,
                        PingRequest,
                    >::default());
                    if let Some(limit) = max_decode {
                        grpc = grpc.max_decoding_message_size(limit);
                    }
                    let handler = PingHandler { call_count };
                    Ok(grpc.unary(handler, req).await)
                }
                "/fixture.Pinger/PingSlow" => {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        Pong,
                        PingRequest,
                    >::default());
                    if let Some(limit) = max_decode {
                        grpc = grpc.max_decoding_message_size(limit);
                    }
                    let handler = PingSlowHandler {
                        call_count,
                        delay_ms,
                    };
                    Ok(grpc.unary(handler, req).await)
                }
                "/fixture.Pinger/PingBig" => {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        Pong,
                        PingRequest,
                    >::default());
                    if let Some(limit) = max_decode {
                        grpc = grpc.max_decoding_message_size(limit);
                    }
                    let handler = PingBigHandler { call_count };
                    Ok(grpc.unary(handler, req).await)
                }
                _ => Ok(unimplemented_response()),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// PongerService
// ---------------------------------------------------------------------------

/// A simple gRPC service with one route:
///
/// * `/fixture.Ponger/Pong` — returns `Pong { msg: "ponger-ok" }`
#[derive(Clone)]
pub struct PongerService;

impl NamedService for PongerService {
    const NAME: &'static str = "fixture.Ponger";
}

struct PongHandler;

impl UnaryService<Empty> for PongHandler {
    type Response = Pong;
    type Future =
        Pin<Box<dyn Future<Output = Result<tonic::Response<Pong>, tonic::Status>> + Send>>;

    fn call(&mut self, _req: tonic::Request<Empty>) -> Self::Future {
        Box::pin(async move {
            Ok(tonic::Response::new(Pong {
                msg: "ponger-ok".to_string(),
            }))
        })
    }
}

impl tower::Service<http::Request<tonic::body::Body>> for PongerService {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let path = req.uri().path().to_owned();
        Box::pin(async move {
            match path.as_str() {
                "/fixture.Ponger/Pong" => {
                    // Server codec: Encode=Pong (response), Decode=Empty (incoming).
                    let mut grpc =
                        tonic::server::Grpc::new(tonic_prost::ProstCodec::<Pong, Empty>::default());
                    let handler = PongHandler;
                    Ok(grpc.unary(handler, req).await)
                }
                _ => Ok(unimplemented_response()),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Spawn helpers
// ---------------------------------------------------------------------------

/// Bind to `127.0.0.1:0`, wire up Pinger + Ponger on the given builder,
/// and spawn `serve_with_listener_shutdown` in a background task.
///
/// Returns `(addr, shutdown_tx, join_handle)`.
pub async fn spawn_with_builder(
    builder: oxirpc_server::ServerBuilder,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let pinger = PingerService::new(Arc::new(AtomicUsize::new(0)), 0);
    let ponger = PongerService;
    spawn_with_builder_and_services(builder, pinger, ponger).await
}

/// Internal helper: wire given pinger + ponger onto builder.
async fn spawn_with_builder_and_services(
    builder: oxirpc_server::ServerBuilder,
    pinger: PingerService,
    ponger: PongerService,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        builder
            .add_service(pinger)
            .add_service(ponger)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });
    (addr, tx, handle)
}

/// Spawn a default server with a zero-delay PingerService and PongerService.
pub async fn spawn_fixture() -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    spawn_with_builder(oxirpc_server::ServerBuilder::new()).await
}

/// Spawn a server where PingSlow introduces the given delay (milliseconds).
pub async fn spawn_slow_fixture(
    delay_ms: u64,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let pinger = PingerService::new(Arc::new(AtomicUsize::new(0)), delay_ms);
    let ponger = PongerService;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let builder = oxirpc_server::ServerBuilder::new();
    let handle = tokio::spawn(async move {
        builder
            .add_service(pinger)
            .add_service(ponger)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });
    (addr, tx, handle)
}

/// Spawn a server where Pinger's `Grpc` handler caps decoding to `max_decode_bytes`.
pub async fn spawn_limited_fixture(
    max_decode_bytes: usize,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let pinger = PingerService::with_limit(Arc::new(AtomicUsize::new(0)), 0, max_decode_bytes);
    let ponger = PongerService;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let builder = oxirpc_server::ServerBuilder::new();
    let handle = tokio::spawn(async move {
        builder
            .add_service(pinger)
            .add_service(ponger)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });
    (addr, tx, handle)
}

// ---------------------------------------------------------------------------
// Client-side helpers
// ---------------------------------------------------------------------------

/// Build a `tonic::transport::Channel` connected to `addr`.
pub async fn connect(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect")
}

/// Issue a unary `/fixture.Pinger/Ping` call and return the response.
pub async fn ping(
    channel: tonic::transport::Channel,
    payload: Vec<u8>,
) -> Result<tonic::Response<Pong>, tonic::Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("grpc ready");
    grpc.unary(
        tonic::Request::new(PingRequest { payload }),
        "/fixture.Pinger/Ping"
            .parse::<http::uri::PathAndQuery>()
            .expect("path"),
        tonic_prost::ProstCodec::<PingRequest, Pong>::default(),
    )
    .await
}

/// Issue a unary `/fixture.Pinger/PingSlow` call and return the response.
pub async fn ping_slow(
    channel: tonic::transport::Channel,
    payload: Vec<u8>,
) -> Result<tonic::Response<Pong>, tonic::Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("grpc ready");
    grpc.unary(
        tonic::Request::new(PingRequest { payload }),
        "/fixture.Pinger/PingSlow"
            .parse::<http::uri::PathAndQuery>()
            .expect("path"),
        tonic_prost::ProstCodec::<PingRequest, Pong>::default(),
    )
    .await
}

/// Issue a unary `/fixture.Ponger/Pong` call and return the response.
pub async fn pong(
    channel: tonic::transport::Channel,
) -> Result<tonic::Response<Pong>, tonic::Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("grpc ready");
    grpc.unary(
        tonic::Request::new(Empty {}),
        "/fixture.Ponger/Pong"
            .parse::<http::uri::PathAndQuery>()
            .expect("path"),
        tonic_prost::ProstCodec::<Empty, Pong>::default(),
    )
    .await
}

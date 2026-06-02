//! Benchmarks: RPC throughput, server startup time, and graceful shutdown latency.
//!
//! All benchmarks use a loopback connection (127.0.0.1:0) with a hand-rolled
//! `PingerService` fixture so no external proto generation is required.
//!
//! # Benchmarks
//!
//! - `bench_server_startup_time` — wall time from `spawn_bench_server` through
//!   the first successful Ping RPC (bind + serve + first response).
//! - `bench_single_rpc_latency` — server spun up once outside the loop; measures
//!   the round-trip latency of a single unary Ping call over loopback.
//! - `bench_graceful_shutdown_latency` — spawns a server, fires a shutdown signal,
//!   measures time from signal to `JoinHandle` completion.

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use tonic::server::{NamedService, UnaryService};

// ---------------------------------------------------------------------------
// Minimal in-process fixture (mirrors tests/fixture/mod.rs without depending on it)
// ---------------------------------------------------------------------------

#[derive(Clone, prost::Message)]
struct Pong {
    #[prost(string, tag = "1")]
    msg: String,
}

#[derive(Clone, prost::Message)]
struct PingRequest {
    #[prost(bytes = "vec", tag = "1")]
    payload: Vec<u8>,
}

#[derive(Clone)]
struct PingerService {
    call_count: Arc<AtomicUsize>,
}

impl PingerService {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl NamedService for PingerService {
    const NAME: &'static str = "bench.Pinger";
}

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

        Box::pin(async move {
            if path == "/bench.Pinger/Ping" {
                let mut grpc = tonic::server::Grpc::new(
                    tonic_prost::ProstCodec::<Pong, PingRequest>::default(),
                );
                let handler = PingHandler { call_count };
                Ok(grpc.unary(handler, req).await)
            } else {
                Ok(tonic::Status::unimplemented("route not found").into_http())
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tokio runtime for benchmarks
// ---------------------------------------------------------------------------

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ---------------------------------------------------------------------------
// Helpers: spawn server, connect channel, issue Ping
// ---------------------------------------------------------------------------

async fn spawn_bench_server() -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let svc = PingerService::new();
    let handle = tokio::spawn(async move {
        oxirpc_server::ServerBuilder::new()
            .add_service(svc)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });
    (addr, tx, handle)
}

async fn connect(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect")
}

async fn ping(channel: tonic::transport::Channel) -> Result<tonic::Response<Pong>, tonic::Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("grpc ready");
    grpc.unary(
        tonic::Request::new(PingRequest { payload: vec![] }),
        "/bench.Pinger/Ping"
            .parse::<http::uri::PathAndQuery>()
            .expect("path"),
        tonic_prost::ProstCodec::<PingRequest, Pong>::default(),
    )
    .await
}

// ---------------------------------------------------------------------------
// 1. Server startup time
//    Measures: spawn server → first successful RPC response (loopback).
// ---------------------------------------------------------------------------

fn bench_server_startup_time(c: &mut Criterion) {
    let rt = make_runtime();

    c.bench_function("server_startup_time", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (addr, tx, handle) = spawn_bench_server().await;
                let channel = connect(addr).await;
                let resp = ping(channel).await.expect("ping");
                assert_eq!(resp.into_inner().msg, "pong");
                tx.send(()).ok();
                handle.await.ok();
            });
        });
    });
}

// ---------------------------------------------------------------------------
// 2. Single RPC latency
//    Server is started once outside the loop; only the unary call is timed.
// ---------------------------------------------------------------------------

fn bench_single_rpc_latency(c: &mut Criterion) {
    let rt = make_runtime();

    // Spin up a persistent server for the duration of the benchmark.
    let (addr, shutdown_tx, server_handle) = rt.block_on(spawn_bench_server());

    c.bench_function("single_rpc_latency", |b| {
        b.iter(|| {
            rt.block_on(async {
                // A fresh channel each iteration; includes TCP connect overhead,
                // which is realistic for loopback single-connection latency.
                let channel = connect(addr).await;
                let resp = ping(channel).await.expect("ping");
                assert_eq!(resp.into_inner().msg, "pong");
            });
        });
    });

    rt.block_on(async {
        shutdown_tx.send(()).ok();
        server_handle.await.ok();
    });
}

// ---------------------------------------------------------------------------
// 3. Graceful shutdown latency
//    Measures time from shutdown signal send to server task completion.
// ---------------------------------------------------------------------------

fn bench_graceful_shutdown_latency(c: &mut Criterion) {
    let rt = make_runtime();

    c.bench_function("graceful_shutdown_latency", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (addr, tx, handle) = spawn_bench_server().await;

                // Prime the server with one request to confirm it is ready.
                let channel = connect(addr).await;
                ping(channel).await.expect("pre-shutdown ping");

                // Measure: send shutdown → task done.
                let t0 = tokio::time::Instant::now();
                tx.send(()).ok();
                handle.await.ok();
                let elapsed = t0.elapsed();

                // Sanity upper bound — not a hard SLA, just a smoke test.
                assert!(
                    elapsed < Duration::from_secs(2),
                    "shutdown took too long: {elapsed:?}"
                );
            });
        });
    });
}

// ---------------------------------------------------------------------------
// Criterion registration
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_server_startup_time,
    bench_single_rpc_latency,
    bench_graceful_shutdown_latency,
);
criterion_main!(benches);

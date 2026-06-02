//! Tests for concurrent stream handling via `ServerBuilder::max_concurrent_streams`.
//!
//! Verifies that 50+ concurrent gRPC calls all complete correctly when the
//! server is configured with an adequate `max_concurrent_streams` value.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::Duration;

use http::{Request, Response};
use tonic::server::{NamedService, UnaryService};
use tower::Service;

// ─── Inline proto types (avoid fixture dependency) ───────────────────────────

#[derive(Clone, prost::Message)]
struct PingPayload {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

#[derive(Clone, prost::Message)]
struct PongMsg {
    #[prost(string, tag = "1")]
    msg: String,
}

// ─── Inline PingerService ─────────────────────────────────────────────────────

#[derive(Clone)]
struct InlinePinger {
    call_count: Arc<AtomicUsize>,
}

impl InlinePinger {
    fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl NamedService for InlinePinger {
    const NAME: &'static str = "concurrent.Pinger";
}

struct PingHandler {
    call_count: Arc<AtomicUsize>,
}

impl UnaryService<PingPayload> for PingHandler {
    type Response = PongMsg;
    type Future =
        Pin<Box<dyn Future<Output = Result<tonic::Response<PongMsg>, tonic::Status>> + Send>>;

    fn call(&mut self, _req: tonic::Request<PingPayload>) -> Self::Future {
        let count = Arc::clone(&self.call_count);
        Box::pin(async move {
            count.fetch_add(1, Ordering::Relaxed);
            Ok(tonic::Response::new(PongMsg {
                msg: "pong".to_string(),
            }))
        })
    }
}

impl Service<Request<tonic::body::Body>> for InlinePinger {
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<tonic::body::Body>) -> Self::Future {
        let call_count = Arc::clone(&self.call_count);
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(
                tonic_prost::ProstCodec::<PongMsg, PingPayload>::default(),
            );
            let handler = PingHandler { call_count };
            Ok(grpc.unary(handler, req).await)
        })
    }
}

// ─── Client-side helpers ─────────────────────────────────────────────────────

async fn connect(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect")
}

async fn ping(
    channel: tonic::transport::Channel,
) -> Result<tonic::Response<PongMsg>, tonic::Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("grpc ready");
    grpc.unary(
        tonic::Request::new(PingPayload { data: vec![] }),
        "/concurrent.Pinger/SomeMethod"
            .parse::<http::uri::PathAndQuery>()
            .expect("path"),
        tonic_prost::ProstCodec::<PingPayload, PongMsg>::default(),
    )
    .await
}

// ─── Test ────────────────────────────────────────────────────────────────────

/// Spawn 50 concurrent `Ping` calls against a server configured with a high
/// `max_concurrent_streams` limit.  All calls must succeed.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_stream_limit_enforced() {
    const N_CONCURRENT: usize = 50;
    const MAX_STREAMS: u32 = 200;

    let pinger = InlinePinger::new();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        oxirpc_server::ServerBuilder::new()
            .max_concurrent_streams(MAX_STREAMS)
            .add_service(pinger)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    // Issue N_CONCURRENT calls in parallel.
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(N_CONCURRENT);

    for _ in 0..N_CONCURRENT {
        let channel = connect(addr).await;
        let count = Arc::clone(&success_count);
        tasks.push(tokio::spawn(async move {
            let result = ping(channel).await;
            if result.is_ok() {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Wait for all tasks with a generous timeout.
    let deadline = tokio::time::timeout(Duration::from_secs(15), async {
        for task in tasks {
            task.await.ok();
        }
    });
    deadline.await.expect("concurrent calls timed out");

    let succeeded = success_count.load(Ordering::SeqCst);
    assert_eq!(
        succeeded, N_CONCURRENT,
        "{succeeded}/{N_CONCURRENT} concurrent calls succeeded (expected all)"
    );

    tx.send(()).ok();
    server.await.ok();
}

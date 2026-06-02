//! Tests for ServerBuilder config methods and the listener-based serve path.
//!
//! Uses the gRPC health service as a concrete mountable service so we can drive
//! a real client round-trip and confirm `serve_with_listener_shutdown` works
//! and that the OS-assigned port (bind to `:0`) is observable before serving.

use std::time::Duration;

use oxirpc_health::{HealthBuilder, ServingStatus};
use oxirpc_server::ServerBuilder;
use tokio::sync::oneshot;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

#[test]
fn builder_config_methods_chain() {
    // Pure builder-config smoke test — must compile and not panic.
    let _ready = ServerBuilder::new()
        .concurrency_limit_per_connection(128)
        .max_concurrent_streams(256)
        .timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(60))
        .http2_keepalive_interval(Duration::from_secs(20));
    // (no add_service: just exercising the builder configuration surface)
}

#[tokio::test]
async fn serve_with_listener_round_trip() {
    let (health_svc, mut handle) = HealthBuilder::new()
        .register("test.Svc", ServingStatus::Serving)
        .build_native()
        .await;
    handle.set_serving("test.Svc").await;

    // Bind to an OS-assigned port and learn the address *before* serving.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let (tx, rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        ServerBuilder::new()
            .tcp_nodelay(true)
            .add_native_service(health_svc)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    // Connect a client to the discovered address and issue a health Check.
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("channel")
        .connect()
        .await
        .expect("connect");
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "test.Svc".to_string(),
        })
        .await
        .expect("check rpc");
    // 1 == SERVING in the health proto enum.
    assert_eq!(resp.into_inner().status, 1);

    tx.send(()).ok();
    server.await.ok();
}

#[cfg(feature = "health")]
#[tokio::test]
async fn add_native_service_serves_health_roundtrip() {
    let (health_svc, _handle) = HealthBuilder::new()
        .register("roundtrip.Svc", ServingStatus::Serving)
        .build_native()
        .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let (tx, rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        ServerBuilder::new()
            .add_native_service(health_svc)
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    // Give server a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("channel")
        .connect()
        .await
        .expect("connect");
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "roundtrip.Svc".to_string(),
        })
        .await
        .expect("check rpc");
    // 1 = SERVING
    assert_eq!(resp.into_inner().status, 1);

    tx.send(()).ok();
}

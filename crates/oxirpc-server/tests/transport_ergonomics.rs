//! Tests for transport ergonomics (Slice 4): new `ServerBuilder` knobs and
//! multi-service `ServeReady::add_service`. Extended in Slice 3 with HTTP/2
//! adaptive window, pending/local error reset streams, TCP keepalive interval/
//! retries, and Unix domain socket serving.

use std::time::Duration;

use oxirpc_health::{HealthBuilder, ServingStatus};
use oxirpc_server::ServerBuilder;

// ---------------------------------------------------------------------------
// Builder-chain smoke tests — compile + no-panic; no actual I/O.
// ---------------------------------------------------------------------------

#[test]
fn accept_http1_chain() {
    let _b = ServerBuilder::new().accept_http1(true);
    let _b2 = ServerBuilder::new().accept_http1(false);
}

#[test]
fn transport_knobs_chain() {
    // Verify every new transport knob compiles and chains without panic.
    let _b = ServerBuilder::new()
        .accept_http1(false)
        .concurrency_limit_per_connection(64)
        .max_concurrent_streams(128)
        .timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(30))
        .http2_keepalive_interval(Duration::from_secs(5))
        .http2_keepalive_timeout(Duration::from_secs(20))
        .initial_stream_window_size(1 << 17) // 128 KiB
        .initial_connection_window_size(1 << 20) // 1 MiB
        .max_frame_size(1 << 15) // 32 KiB
        .http2_max_header_list_size(8_192)
        .load_shed(false)
        .max_connection_age(Duration::from_secs(600))
        .max_connection_age_grace(Duration::from_secs(30));
}

// ---------------------------------------------------------------------------
// Multi-service: ServeReady::add_service chains (compile-level + runtime).
//
// We register two genuinely distinct services — health (grpc.health.v1.Health)
// and reflection (grpc.reflection.v1.ServerReflection) — so there is no route
// collision.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_service_add_service_chains() {
    // Health service.
    let (health_svc, mut health_handle) = HealthBuilder::new()
        .register("test.Svc", ServingStatus::Serving)
        .build_native()
        .await;
    health_handle.set_serving("test.Svc").await;

    // Reflection service (different gRPC service name, no collision).
    let reflection_svc = tonic_reflection::server::Builder::configure()
        .build_v1()
        .expect("build reflection service");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // Chain two distinct services into the same ServeReady.
    let serve_ready = ServerBuilder::new()
        .accept_http1(false)
        .add_native_service(health_svc)
        .add_service(reflection_svc); // multi-service chaining

    let server_handle = tokio::spawn(async move {
        serve_ready
            .serve_with_listener_shutdown(listener, async {
                rx.await.ok();
            })
            .await
            .ok();
    });

    // Verify the health service responds (proves the full stack is live).
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("channel")
        .connect()
        .await
        .expect("connect");
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let resp = client
        .check(tonic_health::pb::HealthCheckRequest {
            service: "test.Svc".to_string(),
        })
        .await
        .expect("health check rpc");
    // Status 1 == SERVING in the health proto.
    assert_eq!(resp.into_inner().status, 1);

    tx.send(()).ok();
    server_handle.await.ok();
}

// ---------------------------------------------------------------------------
// Slice 3: new HTTP/2 knobs and Unix domain socket serving.
// ---------------------------------------------------------------------------

#[test]
fn new_http2_knobs_compile() {
    let _ = ServerBuilder::new()
        .http2_adaptive_window(Some(true))
        .http2_max_pending_accept_reset_streams(Some(100))
        .http2_max_local_error_reset_streams(Some(100))
        .tcp_keepalive_interval(Some(Duration::from_secs(60)))
        .tcp_keepalive_retries(Some(3));
}

#[cfg(unix)]
#[tokio::test]
async fn serve_unix_binds_to_temp_path() {
    use std::env;
    let dir = env::temp_dir();
    let path = dir.join(format!("oxirpc-test-{}.sock", std::process::id()));
    // Clean up any leftover socket.
    let _ = std::fs::remove_file(&path);
    // Just verify it compiles + binds (immediately shut down).
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
    let path_clone = path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let _ = signal_tx.send(());
    });
    // We need a real service to call serve_unix_with_shutdown. Use a health service.
    let (svc, _handle) = HealthBuilder::new().build_native().await;
    let result = ServerBuilder::new()
        .add_native_service(svc)
        .serve_unix_with_shutdown(&path_clone, async {
            let _ = signal_rx.await;
        })
        .await;
    let _ = std::fs::remove_file(&path);
    assert!(result.is_ok(), "unix serve failed: {result:?}");
}

// ---------------------------------------------------------------------------
// reflection_service convenience (requires `reflect` feature)
// ---------------------------------------------------------------------------

/// Smoke test: `ServerBuilder::reflection_service` accepts a valid FDS and
/// returns a `ServeReady` ready to be bound.
#[cfg(feature = "reflect")]
#[test]
fn reflection_service_produces_serve_ready() {
    use prost::Message as _;
    use prost_types::{FileDescriptorProto, FileDescriptorSet, ServiceDescriptorProto};

    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".to_owned()),
            package: Some("test".to_owned()),
            service: vec![ServiceDescriptorProto {
                name: Some("TestService".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let fds_bytes = bytes::Bytes::from(fds.encode_to_vec());

    let result = ServerBuilder::new().reflection_service(fds_bytes);
    assert!(
        result.is_ok(),
        "reflection_service should succeed with valid FDS bytes"
    );
}

/// Error path: `ServerBuilder::reflection_service` rejects malformed bytes.
#[cfg(feature = "reflect")]
#[test]
fn reflection_service_rejects_invalid_fds() {
    let bad_bytes = bytes::Bytes::from_static(b"not-proto-binary");
    let result = ServerBuilder::new().reflection_service(bad_bytes);
    assert!(
        result.is_err(),
        "reflection_service should fail with invalid FDS bytes"
    );
}

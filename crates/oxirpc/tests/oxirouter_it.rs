//! gRPC health-check round-trip integration test.
//!
//! Exercises the full server → client round-trip using oxirpc's own building
//! blocks:
//!
//! 1. `oxirpc_health::HealthBuilder` builds a `NativeHealthService`.
//! 2. `oxirpc_server::ServerBuilder` + `NativeServiceRegistry` bind an
//!    OS-assigned port and serve the health service via the native H2 transport.
//! 3. A tonic `Channel` connects to that port and calls `grpc.health.v1.Health/Check`.
//! 4. The test asserts the response is `SERVING` (status == 1) and tears down
//!    the server cleanly.
//!
//! Requires the `native` and `health` cargo features.

use oxirpc_health::HealthBuilder;
use oxirpc_server::{NativeServiceRegistry, ServerBuilder};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Bind a `tokio::net::TcpListener` on an OS-assigned port and return the
/// `(listener, addr)` pair.
async fn bind_random() -> (tokio::net::TcpListener, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

/// Open a plaintext tonic `Channel` to `addr`.
async fn connect_channel(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid endpoint URI")
        .connect()
        .await
        .expect("connect channel")
}

// ── Test: health_check_round_trip ─────────────────────────────────────────────
//
// Spins up a NativeHealthService on an ephemeral localhost port, issues a
// gRPC Health/Check RPC, asserts SERVING, then tears the server down.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn health_check_round_trip() {
    // 1. Build a NativeHealthService and mark the root service as SERVING.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    // 2. Bind an OS-assigned port to avoid races between parallel test runs.
    let (listener, addr) = bind_random().await;

    // 3. Register the health service and spawn the native H2 server.
    let registry = NativeServiceRegistry::new().add_service(native_svc);
    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    // Give the server a moment to begin accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // 4. Connect a tonic channel and call Health/Check.
    let channel = connect_channel(addr).await;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await
        .expect("Health/Check RPC must succeed")
        .into_inner();

    // ServingStatus::Serving = 1 in the proto enum.
    assert_eq!(resp.status, 1, "expected SERVING (1), got {}", resp.status);

    // 5. Tear down: abort the server task and wait for it to finish.
    server_handle.abort();
    server_handle.await.ok();
}

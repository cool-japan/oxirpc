//! Native↔native end-to-end tests.
//!
//! Exercises the `NativeServiceRegistry` → `serve_native_registry_*` path
//! with `NativeHealthService` as the service under test.  Client side uses
//! tonic's transport `Channel` (the native client channel — Round 7 work —
//! is not yet wired to tonic's codec layer, so a tonic channel provides the
//! gRPC-over-h2 framing while the *server* runs fully natively).
//!
//! Requires the `native` and `health` cargo features.

use oxirpc_health::HealthBuilder;
use oxirpc_server::{NativeServiceRegistry, ServerBuilder};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Bind an OS-assigned listener and return the `(listener, addr)` pair.
async fn bind_random() -> (tokio::net::TcpListener, std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");
    (listener, addr)
}

/// Connect a tonic channel to `addr` (plaintext).
async fn connect_channel(addr: std::net::SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("valid URI")
        .connect()
        .await
        .expect("connect channel")
}

// ── Test 1: native_unary_e2e_zero_tonic_dataplane ────────────────────────────
//
// Spin up a NativeServiceRegistry server on the native H2 transport, connect
// via a plain tonic Channel, call Health/Check, assert SERVING.
//
// The server-side data plane is entirely native (`NativeServiceRegistry` +
// `serve_native_registry_with_listener`).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_unary_e2e_zero_tonic_dataplane() {
    // 1. Build NativeHealthService via HealthBuilder.
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    // 2. Bind OS-assigned port.
    let (listener, addr) = bind_random().await;

    // 3. Build a NativeServiceRegistry with the health service.
    //    In Phase 4.1, `serve_native_registry_*` is available directly on
    //    `ServerBuilder` — no call to `add_service` needed.
    let registry = NativeServiceRegistry::new().add_service(native_svc);

    // 4. Spawn the native registry server.
    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    // Give the server a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // 5. Connect tonic channel and invoke Health/Check.
    let channel = connect_channel(addr).await;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await
        .expect("Health/Check RPC")
        .into_inner();

    // ServingStatus::Serving = 1.
    assert_eq!(resp.status, 1, "expected SERVING (1), got {}", resp.status);

    server_handle.abort();
    server_handle.await.ok();
}

// ── Test 2: native_registry_graceful_shutdown ─────────────────────────────────
//
// Verify that `serve_native_registry_with_listener_shutdown` exits cleanly
// when the shutdown signal fires.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_registry_graceful_shutdown() {
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("").await;

    let (listener, addr) = bind_random().await;
    let registry = NativeServiceRegistry::new().add_service(native_svc);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener_shutdown(listener, registry, async move {
                shutdown_rx.await.ok();
            })
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Verify it's up.
    let channel = connect_channel(addr).await;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: "".to_string(),
        })
        .await
        .expect("Health/Check before shutdown")
        .into_inner();
    assert_eq!(resp.status, 1, "should be SERVING before shutdown");

    // Trigger shutdown.
    shutdown_tx.send(()).ok();

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), server_handle).await;
    assert!(
        result.is_ok(),
        "server task should exit within 2 s after shutdown signal"
    );
}

// ── Test 3: native_server_stream_e2e ─────────────────────────────────────────
//
// Verify that the server-streaming (Watch) path works through the native
// registry transport.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_server_stream_e2e() {
    let (native_svc, mut handle) = HealthBuilder::new().build_native().await;
    handle.set_serving("watch.Target").await;

    let (listener, addr) = bind_random().await;
    let registry = NativeServiceRegistry::new().add_service(native_svc);

    let server_handle = tokio::spawn(async move {
        ServerBuilder::new()
            .serve_native_registry_with_listener(listener, registry)
            .await
            .ok();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Open a Watch stream and read the first status update.
    let channel = connect_channel(addr).await;
    let mut client = HealthClient::new(channel);
    let mut stream = client
        .watch(HealthCheckRequest {
            service: "watch.Target".to_string(),
        })
        .await
        .expect("Watch RPC")
        .into_inner();

    use tokio_stream::StreamExt as _;
    let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("first Watch message within 1 s")
        .expect("stream did not end immediately")
        .expect("Watch stream item");

    // The service was set to SERVING.
    assert_eq!(
        first.status, 1,
        "expected SERVING (1), got {}",
        first.status
    );

    server_handle.abort();
    server_handle.await.ok();
}
